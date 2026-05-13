"""Offline entry point for regenerating border fixture seeds."""

import argparse
import csv
import logging
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Mapping

import psycopg
from psycopg import sql

from pg_nearest_city.datasets.types import BoundarySource
from pg_nearest_city.db.settings import DBConnSettings


COUNTRY_TABLE = "country"
ORACLE_TABLE = "border_country_oracle"
ORACLE_SCHEMA_PREFIX = "tmp_border_oracle"
SEAM_RADIUS_M = 20_000
PAIR_RADIUS_M = 10_000
MAX_PAIRS_PER_COUNTRY_PAIR = 5
MIN_SEAM_LENGTH_M = 1_000
NEAREST_CANDIDATES_PER_CITY = 32
PROBE_RING_DISTANCES_M = (100, 500, 1000)
PROBE_STATUSES = ("ok", "ambiguous", "unplaceable")
BORDER_PROBE_FIXTURE_PATH = Path("tests/fixtures/border_country_probes.csv")

# Cheap geometry prefilters before exact geography distance checks.
SEAM_BBOX_DEG = 0.25
PAIR_BBOX_DEG = 0.15

PROBE_CSV_HEADER = [
    "pair",
    "source_country",
    "expected_alpha2",
    "expected_alpha3",
    "expected_country_name",
    "neighbor_country",
    "source_city",
    "source_lat",
    "source_lon",
    "neighbor_city",
    "neighbor_lat",
    "neighbor_lon",
    "seam_lat",
    "seam_lon",
    "ring_distance_m",
    "probe_lat",
    "probe_lon",
    "status",
]

CountryPair = tuple[str, str]


@dataclass(frozen=True, slots=True)
class CountryOracleRef:
    """Reference to the disposable corrected pre-simplification oracle table."""

    schema: str
    table: str = ORACLE_TABLE

    @property
    def qualified_name(self) -> str:
        """Return a human-readable qualified table name."""
        return f"{self.schema}.{self.table}"


@dataclass(frozen=True, slots=True)
class BorderFixtureRequest:
    """Input bundle for one offline border fixture generator run."""

    db: DBConnSettings
    cache_dir: Path
    boundary_source: BoundarySource
    country_pairs: frozenset[CountryPair]
    max_pairs_per_country_pair: int
    ring_distances_m: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ProbeRow:
    """Named seam-relative probe row produced by the generator."""

    pair: str
    source_country: str
    expected_alpha2: str
    expected_alpha3: str
    expected_country_name: str
    neighbor_country: str
    source_city: str
    source_lat: object
    source_lon: object
    neighbor_city: str
    neighbor_lat: object
    neighbor_lon: object
    seam_lat: object
    seam_lon: object
    ring_distance_m: int
    probe_lat: object
    probe_lon: object
    status: str

    def as_csv_row(self) -> tuple:
        """Return the row in committed `PROBE_CSV_HEADER` order."""
        return (
            self.pair,
            self.source_country,
            self.expected_alpha2,
            self.expected_alpha3,
            self.expected_country_name,
            self.neighbor_country,
            self.source_city,
            self.source_lat,
            self.source_lon,
            self.neighbor_city,
            self.neighbor_lat,
            self.neighbor_lon,
            self.seam_lat,
            self.seam_lon,
            self.ring_distance_m,
            self.probe_lat,
            self.probe_lon,
            self.status,
        )


@dataclass(frozen=True, slots=True)
class PairSummary:
    """Per-country-pair discovery and probe-status summary for reporting."""

    pair: str
    discovered_rows: int
    status_counts: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class BorderFixtureResult:
    """Output bundle from one offline border fixture generator run."""

    probe_rows: list[ProbeRow]
    pair_summaries: list[PairSummary]
    missing_required_pairs: list[str]
    missing_ok_pairs: list[str]
    discovered_pair_count: int
    requested_country_pairs: frozenset[CountryPair]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse border fixture generator arguments."""
    parser = argparse.ArgumentParser(
        description="Regenerate offline border fixture seeds from runtime seams."
    )
    group_db = parser.add_argument_group("group_db")
    group_db.add_argument("--db-host", help="Database host")
    group_db.add_argument("--db-port", type=int, help="Database port")
    group_db.add_argument("--db-name", help="Database name")
    group_db.add_argument("--db-user", help="Database username")
    group_db.add_argument("--db-password", help="Database password")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/data/cache"),
        help="Directory containing cached source datasets and dataset_registry.json",
    )
    parser.add_argument(
        "--boundary-source",
        choices=[s.value for s in BoundarySource],
        default=BoundarySource.NATURAL_EARTH.value,
        help="Boundary source to use when rebuilding the pre-simplification oracle",
    )
    parser.add_argument(
        "--probes-output",
        type=Path,
        default=BORDER_PROBE_FIXTURE_PATH,
        help=(
            "CSV path for the canonical generated seam-relative probe fixture "
            f"(default: {BORDER_PROBE_FIXTURE_PATH})"
        ),
    )
    parser.add_argument(
        "--max-pairs-per-country-pair",
        type=int,
        default=MAX_PAIRS_PER_COUNTRY_PAIR,
        help="Maximum discovered seed rows to keep per country pair",
    )
    parser.add_argument(
        "--country-pair",
        action="append",
        default=[],
        help=(
            "Country pair like IT/SI. Restricts discovery to the given pairs; "
            "omit to discover all adjacent country pairs."
        ),
    )
    return parser.parse_args(argv)


def _quote_gdal_pg_value(value: object) -> str:
    """Quote a value for a GDAL PostgreSQL keyword connection string."""
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def gdal_pg_conn_string_for_schema(
    settings: DBConnSettings, schema: str
) -> str:
    """Build a GDAL PostgreSQL connection string scoped to a scratch schema."""
    parts = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.name,
        "user": settings.user,
        "password": settings.password,
        "active_schema": schema,
    }
    return " ".join(
        f"{key}={_quote_gdal_pg_value(value)}" for key, value in parts.items()
    )


def make_oracle_schema_name(prefix: str = ORACLE_SCHEMA_PREFIX) -> str:
    """Return a process-scoped scratch schema name for oracle reconstruction."""
    return f"{prefix}_{os.getpid()}"


def _set_search_path(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
    conn.commit()


def _create_oracle_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.commit()


def _drop_oracle_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
    conn.commit()


def _ensure_runtime_geocoding_available(conn: psycopg.Connection) -> None:
    """Fail early if the loader-derived geocoding table is absent."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.geocoding') AS table_name")
        row = cur.fetchone()
        if row["table_name"] is None:
            raise RuntimeError(
                "Missing required runtime geocoding table public.geocoding. "
                "Load the database before rebuilding the border fixture oracle."
            )


def _create_oracle_working_tables(conn: psycopg.Connection) -> None:
    """Create only the loader scratch tables needed to build country_init."""
    from pg_nearest_city.db.tables import (
        CountryInit,
        TmpIso3166,
        TmpNonIsoAdm0,
        TmpNonIsoGid0Parent,
        TmpPromotedAdm1ToGid0,
    )

    for table in (
        CountryInit,
        TmpIso3166,
        TmpNonIsoAdm0,
        TmpNonIsoGid0Parent,
        TmpPromotedAdm1ToGid0,
    ):
        with conn.cursor() as cur:
            cur.execute(table.create_sql())
    conn.commit()


def _create_oracle_indices(conn: psycopg.Connection) -> None:
    """Create only data-load scratch indexes inside the oracle schema."""
    from pg_nearest_city.db.tables import get_tables

    with conn.cursor() as cur:
        for table in get_tables(filters=[{"is_for_data_load": True}]):
            for index in table.get_indices():
                if index.is_post_load:
                    cur.execute(index.index_def)
    conn.commit()


def _apply_boundary_source_corrections(
    conn: psycopg.Connection, boundary_source: BoundarySource
) -> None:
    """Apply boundary-table corrections without mutating runtime geocoding rows."""
    from pg_nearest_city.db.corrections import (
        GADM_DATA_CORRECTIONS,
        NE_DATA_CORRECTIONS,
    )
    from pg_nearest_city.db.data_cleanup import make_queries

    rows = (
        GADM_DATA_CORRECTIONS
        if boundary_source == BoundarySource.GADM
        else NE_DATA_CORRECTIONS
    )
    with conn.cursor() as cur:
        for correction, query in zip(rows, make_queries(rows), strict=False):
            cur.execute(query)
            if cur.rowcount > correction.result_limit:
                conn.rollback()
                raise RuntimeError(
                    f"Correction affected too many rows: {correction.description} "
                    f"expected <= {correction.result_limit}, got {cur.rowcount}"
                )
    conn.commit()


def _materialize_oracle_table(conn: psycopg.Connection) -> None:
    """Copy corrected pre-simplification country_init geometry into oracle table."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(ORACLE_TABLE))
        )
        cur.execute(
            sql.SQL(
                "CREATE TABLE {} AS "
                "SELECT alpha2, alpha3, name, geom FROM country_init"
            ).format(sql.Identifier(ORACLE_TABLE))
        )
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} (alpha2)").format(
                sql.Identifier(f"{ORACLE_TABLE}_alpha2_idx"),
                sql.Identifier(ORACLE_TABLE),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} USING GIST (geom)").format(
                sql.Identifier(f"{ORACLE_TABLE}_geom_idx"),
                sql.Identifier(ORACLE_TABLE),
            )
        )
        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(ORACLE_TABLE)))
    conn.commit()


def rebuild_corrected_country_oracle(
    *,
    conn_settings: DBConnSettings,
    cache_dir: Path,
    boundary_source: BoundarySource,
    schema: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[CountryOracleRef, psycopg.Connection]:
    """Rebuild the corrected pre-simplification country oracle in scratch state.

    The loader pipeline is reused for source import, ISO sovereignty merging,
    GeoBoundaries/Overpass corrections, and overlap resolution, but the flow
    stops before `SIMPLIFY_COUNTRY` and never writes to the shipped runtime
    `public.country` table.  A scratch schema is used because GDAL imports need
    cross-connection visibility; callers must drop it when done.
    """
    from pg_nearest_city.db.data_import import Config, DataLoader

    log = logger or logging.getLogger("border_fixture_oracle")
    oracle_schema = schema or make_oracle_schema_name()
    loader = DataLoader(
        config=Config(
            db_conn_settings=conn_settings,
            cache_dir=cache_dir,
            cache_files=True,
            boundary_source=boundary_source,
        ),
        logger=log,
    )
    conn = loader.conn

    try:
        _create_oracle_schema(conn, oracle_schema)
        _set_search_path(conn, oracle_schema)
        loader.config.db_conn_str = gdal_pg_conn_string_for_schema(
            conn_settings, oracle_schema
        )

        _ensure_runtime_geocoding_available(conn)
        _create_oracle_working_tables(conn)
        loader._create_temp_reference_tables(conn)
        loader._import_country_boundaries(conn)
        _create_oracle_indices(conn)
        _apply_boundary_source_corrections(conn, boundary_source)
        loader._merge_countries_to_iso_defs(conn)
        loader._update_country_geometries(conn)
        loader._import_geoboundary_corrections(conn)
        _materialize_oracle_table(conn)
    except Exception:
        _drop_oracle_schema(conn, oracle_schema)
        conn.close()
        raise

    ref = CountryOracleRef(schema=oracle_schema)
    log.info("Rebuilt corrected country oracle at %s", ref.qualified_name)
    return ref, conn


@contextmanager
def corrected_country_oracle(
    *,
    conn_settings: DBConnSettings,
    cache_dir: Path,
    boundary_source: BoundarySource,
    logger: logging.Logger | None = None,
) -> Iterator[CountryOracleRef]:
    """Context manager that rebuilds and then explicitly drops oracle scratch state."""
    ref: CountryOracleRef | None = None
    conn: psycopg.Connection | None = None
    try:
        ref, conn = rebuild_corrected_country_oracle(
            conn_settings=conn_settings,
            cache_dir=cache_dir,
            boundary_source=boundary_source,
            logger=logger,
        )
        yield ref
    finally:
        if ref is not None and conn is not None:
            _drop_oracle_schema(conn, ref.schema)
            conn.close()


class BorderFixtureGenerator:
    """Single entry point that owns the full border fixture generation flow.

    The generator owns the corrected-country oracle rebuild, the scratch-schema
    lifecycle, border-pair discovery, and seam-probe generation.  Discovered
    border pairs remain an internal step used to feed probe generation and are
    not part of the public surface.
    """

    def __init__(
        self,
        request: BorderFixtureRequest,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        if request.max_pairs_per_country_pair < 1:
            raise ValueError("max_pairs_per_country_pair must be >= 1")
        self._request = request
        self._logger = logger or logging.getLogger("border_fixture_generator")

    def run(self) -> BorderFixtureResult:
        """Run the full sequence and return the named result bundle."""
        req = self._request
        country_pairs = set(req.country_pairs)
        with corrected_country_oracle(
            conn_settings=req.db,
            cache_dir=req.cache_dir,
            boundary_source=req.boundary_source,
            logger=self._logger,
        ) as oracle:
            self._logger.info(
                "rebuilt corrected country oracle at %s", oracle.qualified_name
            )
            with (
                psycopg.connect(req.db.conn_string) as conn,
                conn.cursor() as cur,
            ):
                pair_rows = self._discover_border_city_pairs(
                    cur,
                    country_pairs=country_pairs or None,
                    max_pairs_per_country_pair=req.max_pairs_per_country_pair,
                )
                raw_probe_rows = self._generate_seam_probe_rows(
                    cur,
                    oracle=oracle,
                    ring_distances_m=req.ring_distances_m,
                )

        probe_rows = [_probe_row_from_tuple(row) for row in raw_probe_rows]
        return _build_result(
            requested_country_pairs=frozenset(req.country_pairs),
            pair_rows=pair_rows,
            probe_rows=probe_rows,
        )

    def _discover_border_city_pairs(
        self,
        cur: psycopg.Cursor,
        *,
        country_pairs: set[CountryPair] | None = None,
        max_pairs_per_country_pair: int = MAX_PAIRS_PER_COUNTRY_PAIR,
    ) -> list[tuple]:
        """Discover nearby cross-border city pairs using runtime-country seams."""
        if max_pairs_per_country_pair < 1:
            raise ValueError("max_pairs_per_country_pair must be >= 1")

        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_normalized_countries ON COMMIT DROP AS
            SELECT
                alpha2,
                MIN(name) AS name,
                ST_Multi(ST_UnaryUnion(ST_Collect(geom))) AS geom
            FROM {COUNTRY_TABLE}
            GROUP BY alpha2
            """
        )
        # tmp_normalized_countries kept materialized: self-joined in the seam build
        # below and the GIST/alpha2 indexes materially help that self-join.
        cur.execute(
            """
            CREATE INDEX tmp_normalized_countries_alpha2_idx
            ON tmp_normalized_countries (alpha2)
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_normalized_countries_geom_idx
            ON tmp_normalized_countries USING GIST (geom)
            """
        )
        cur.execute("ANALYZE tmp_normalized_countries")

        # Inline the optional country-pair restriction as a VALUES list so the
        # filter rides along with the seam build rather than a separate temp
        # table.  Pairs come from `normalize_country_pair`, which already
        # validates each side as a 2-char alpha-2 code.
        pair_join_sql: sql.Composable = sql.SQL("")
        if country_pairs:
            pair_values = sql.SQL(", ").join(
                sql.SQL("({}, {})").format(sql.Literal(a), sql.Literal(b))
                for a, b in sorted(country_pairs)
            )
            pair_join_sql = sql.SQL(
                " JOIN (VALUES {values}) AS p(country_a, country_b)"
                " ON p.country_a = a.alpha2 AND p.country_b = b.alpha2"
            ).format(values=pair_values)

        cur.execute(
            sql.SQL(
                """
                CREATE TEMP TABLE tmp_valid_seams ON COMMIT DROP AS
                WITH seams AS (
                    SELECT
                        a.alpha2 AS country_a,
                        b.alpha2 AS country_b,
                        a.name AS country_name_a,
                        b.name AS country_name_b,
                        ST_LineMerge(
                            ST_UnaryUnion(
                                ST_CollectionExtract(
                                    ST_Intersection(ST_Boundary(a.geom), ST_Boundary(b.geom)),
                                    2
                                )
                            )
                        ) AS seam_geom
                    FROM tmp_normalized_countries a
                    JOIN tmp_normalized_countries b
                      ON a.alpha2 < b.alpha2
                     AND a.geom && b.geom
                     AND ST_Intersects(a.geom, b.geom)
                    {pair_join}
                )
                SELECT
                    *,
                    ST_Length(seam_geom::geography) AS seam_length_m
                FROM seams
                WHERE seam_geom IS NOT NULL
                  AND NOT ST_IsEmpty(seam_geom)
                  AND ST_Length(seam_geom::geography) >= {min_seam_length}
                """
            ).format(
                pair_join=pair_join_sql,
                min_seam_length=sql.Literal(MIN_SEAM_LENGTH_M),
            )
        )
        # tmp_valid_seams kept materialized: read again later by
        # _generate_seam_probe_rows on the same connection, so the seam build
        # cost is amortized across both stages.
        cur.execute(
            """
            CREATE INDEX tmp_valid_seams_pair_idx
            ON tmp_valid_seams (country_a, country_b)
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_valid_seams_geom_idx
            ON tmp_valid_seams USING GIST (seam_geom)
            """
        )
        cur.execute("ANALYZE tmp_valid_seams")

        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_seam_cities_a ON COMMIT DROP AS
            SELECT
                s.country_a,
                s.country_b,
                s.country_name_a,
                s.country_name_b,
                s.seam_length_m,
                g.city,
                g.lat,
                g.lon,
                g.geom
            FROM tmp_valid_seams s
            JOIN geocoding g
              ON g.country = s.country_a
             AND g.geom && ST_Expand(s.seam_geom, {SEAM_BBOX_DEG})
             AND ST_DWithin(g.geom::geography, s.seam_geom::geography, {SEAM_RADIUS_M})
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_seam_cities_a_pair_idx
            ON tmp_seam_cities_a (country_a, country_b)
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_seam_cities_a_geom_idx
            ON tmp_seam_cities_a USING GIST (geom)
            """
        )
        cur.execute("ANALYZE tmp_seam_cities_a")

        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_seam_cities_b ON COMMIT DROP AS
            SELECT
                s.country_a,
                s.country_b,
                g.city,
                g.lat,
                g.lon,
                g.geom
            FROM tmp_valid_seams s
            JOIN geocoding g
              ON g.country = s.country_b
             AND g.geom && ST_Expand(s.seam_geom, {SEAM_BBOX_DEG})
             AND ST_DWithin(g.geom::geography, s.seam_geom::geography, {SEAM_RADIUS_M})
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_seam_cities_b_pair_idx
            ON tmp_seam_cities_b (country_a, country_b)
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_seam_cities_b_geom_idx
            ON tmp_seam_cities_b USING GIST (geom)
            """
        )
        cur.execute("ANALYZE tmp_seam_cities_b")

        cur.execute("DROP TABLE IF EXISTS tmp_discovered_border_city_pairs")
        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_discovered_border_city_pairs ON COMMIT DROP AS
            WITH candidate_pairs AS (
                SELECT DISTINCT
                    a.country_a,
                    a.country_b,
                    a.country_name_a,
                    a.country_name_b,
                    a.seam_length_m,
                    a.city AS city_a,
                    b.city AS city_b,
                    a.lat AS lat_a,
                    a.lon AS lon_a,
                    b.lat AS lat_b,
                    b.lon AS lon_b,
                    a.geom AS geom_a,
                    b.geom AS geom_b,
                    ST_Distance(a.geom::geography, b.geom::geography) AS distance_m
                FROM tmp_seam_cities_a a
                JOIN LATERAL (
                    SELECT
                        b.city,
                        b.lat,
                        b.lon,
                        b.geom
                    FROM tmp_seam_cities_b b
                    WHERE b.country_a = a.country_a
                      AND b.country_b = a.country_b
                      AND b.geom && ST_Expand(a.geom, {PAIR_BBOX_DEG})
                      AND ST_DWithin(a.geom::geography, b.geom::geography, {PAIR_RADIUS_M})
                    ORDER BY b.geom <-> a.geom
                    LIMIT {NEAREST_CANDIDATES_PER_CITY}
                ) b ON TRUE
            ),
            ranked_pairs AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY country_a, country_b
                        ORDER BY distance_m, city_a, city_b
                    ) AS rn
                FROM candidate_pairs
            )
            SELECT
                country_a || '/' || country_b AS pair,
                country_a,
                country_b,
                country_name_a,
                country_name_b,
                city_a,
                city_b,
                lat_a,
                lon_a,
                lat_b,
                lon_b,
                geom_a,
                geom_b,
                distance_m,
                seam_length_m
            FROM ranked_pairs
            WHERE rn <= {max_pairs_per_country_pair}
            """
        )
        cur.execute(
            """
            CREATE INDEX tmp_discovered_border_city_pairs_pair_idx
            ON tmp_discovered_border_city_pairs (country_a, country_b)
            """
        )
        cur.execute("ANALYZE tmp_discovered_border_city_pairs")

        cur.execute(
            """
            SELECT
                pair,
                country_a,
                country_b,
                country_name_a,
                country_name_b,
                city_a,
                city_b,
                lat_a,
                lon_a,
                lat_b,
                lon_b,
                ROUND(distance_m::numeric, 1) AS distance_m,
                ROUND(seam_length_m::numeric, 1) AS seam_length_m
            FROM tmp_discovered_border_city_pairs
            ORDER BY country_a, country_b, distance_m, city_a, city_b
            """
        )
        return cur.fetchall()

    def _generate_seam_probe_rows(
        self,
        cur: psycopg.Cursor,
        *,
        oracle: CountryOracleRef,
        ring_distances_m: Iterable[int] = PROBE_RING_DISTANCES_M,
    ) -> list[tuple]:
        """Generate seam-relative inward probe rows from discovered seed pairs.

        `_discover_border_city_pairs` must have been called first on the same
        connection so the temporary seam and selected-pair tables exist.
        """
        distances = sorted({int(distance) for distance in ring_distances_m})
        if not distances or distances[0] <= 0:
            raise ValueError("ring_distances_m must contain positive distances")

        # Inline the ring distances as a VALUES list rather than a scratch
        # table.  The CROSS JOIN consumes them exactly once and the final
        # SELECT orders by ring_distance_m, so row order is independent of the
        # VALUES order.
        ring_distance_values = sql.SQL(", ").join(
            sql.SQL("({})").format(sql.Literal(distance)) for distance in distances
        )

        cur.execute(
            sql.SQL(
                """
                WITH source_seeds AS (
                    SELECT
                        ROW_NUMBER() OVER (
                            ORDER BY pair, distance_m, city_a, city_b, country_a
                        ) * 2 - 1 AS seed_id,
                        pair,
                        country_a AS source_country,
                        country_b AS neighbor_country,
                        city_a AS source_city,
                        lat_a AS source_lat,
                        lon_a AS source_lon,
                        city_b AS neighbor_city,
                        lat_b AS neighbor_lat,
                        lon_b AS neighbor_lon,
                        geom_a AS source_geom,
                        geom_b AS neighbor_geom
                    FROM tmp_discovered_border_city_pairs
                    UNION ALL
                    SELECT
                        ROW_NUMBER() OVER (
                            ORDER BY pair, distance_m, city_a, city_b, country_b
                        ) * 2 AS seed_id,
                        pair,
                        country_b AS source_country,
                        country_a AS neighbor_country,
                        city_b AS source_city,
                        lat_b AS source_lat,
                        lon_b AS source_lon,
                        city_a AS neighbor_city,
                        lat_a AS neighbor_lat,
                        lon_a AS neighbor_lon,
                        geom_b AS source_geom,
                        geom_a AS neighbor_geom
                    FROM tmp_discovered_border_city_pairs
                ),
                seam_anchors AS (
                    SELECT
                        seed.*,
                        ST_ClosestPoint(seam.seam_geom, seed.source_geom) AS seam_geom,
                        ST_Azimuth(
                            ST_ClosestPoint(seam.seam_geom, seed.source_geom),
                            seed.source_geom
                        ) AS source_normal
                    FROM source_seeds seed
                    JOIN tmp_valid_seams seam
                      ON seam.country_a = LEAST(seed.source_country, seed.neighbor_country)
                     AND seam.country_b = GREATEST(
                         seed.source_country,
                         seed.neighbor_country
                     )
                ),
                candidates AS (
                    SELECT
                        anchor.*,
                        distances.distance_m AS ring_distance_m,
                        direction.direction_index,
                        CASE
                            WHEN anchor.source_normal IS NULL THEN NULL
                            ELSE ST_Project(
                                anchor.seam_geom::geography,
                                distances.distance_m::double precision,
                                anchor.source_normal + direction.bearing_delta
                            )::geometry
                        END AS candidate_geom
                    FROM seam_anchors anchor
                    CROSS JOIN (VALUES {ring_distances}) AS distances(distance_m)
                    CROSS JOIN (VALUES (0, 0.0), (1, pi())) AS direction(
                        direction_index, bearing_delta
                    )
                ),
                classified_candidates AS (
                    SELECT
                        candidate.*,
                        EXISTS (
                            SELECT 1
                            FROM {oracle} source_country
                            WHERE source_country.alpha2 = candidate.source_country
                              AND candidate.candidate_geom IS NOT NULL
                              AND ST_Covers(source_country.geom, candidate.candidate_geom)
                        ) AS covers_source
                    FROM candidates candidate
                ),
                classified_rows AS (
                    SELECT
                        candidate.seed_id,
                        candidate.pair,
                        candidate.source_country,
                        candidate.neighbor_country,
                        candidate.source_city,
                        candidate.source_lat,
                        candidate.source_lon,
                        candidate.neighbor_city,
                        candidate.neighbor_lat,
                        candidate.neighbor_lon,
                        candidate.seam_geom,
                        candidate.ring_distance_m,
                        COUNT(*) FILTER (
                            WHERE candidate.covers_source
                        ) AS source_cover_count,
                        (ARRAY_AGG(
                            candidate.candidate_geom
                            ORDER BY candidate.direction_index
                        ) FILTER (WHERE candidate.covers_source))[1] AS accepted_probe_geom
                    FROM classified_candidates candidate
                    GROUP BY
                        candidate.seed_id,
                        candidate.pair,
                        candidate.source_country,
                        candidate.neighbor_country,
                        candidate.source_city,
                        candidate.source_lat,
                        candidate.source_lon,
                        candidate.neighbor_city,
                        candidate.neighbor_lat,
                        candidate.neighbor_lon,
                        candidate.seam_geom,
                        candidate.ring_distance_m
                )
                SELECT
                    row.pair,
                    row.source_country,
                    oracle_country.alpha2 AS expected_alpha2,
                    oracle_country.alpha3 AS expected_alpha3,
                    oracle_country.name AS expected_country_name,
                    row.neighbor_country,
                    row.source_city,
                    ROUND(row.source_lat::numeric, 7) AS source_lat,
                    ROUND(row.source_lon::numeric, 7) AS source_lon,
                    row.neighbor_city,
                    ROUND(row.neighbor_lat::numeric, 7) AS neighbor_lat,
                    ROUND(row.neighbor_lon::numeric, 7) AS neighbor_lon,
                    ROUND(ST_Y(row.seam_geom)::numeric, 7) AS seam_lat,
                    ROUND(ST_X(row.seam_geom)::numeric, 7) AS seam_lon,
                    row.ring_distance_m,
                    CASE
                        WHEN row.source_cover_count = 1 THEN
                            ROUND(ST_Y(row.accepted_probe_geom)::numeric, 7)
                        ELSE NULL
                    END AS probe_lat,
                    CASE
                        WHEN row.source_cover_count = 1 THEN
                            ROUND(ST_X(row.accepted_probe_geom)::numeric, 7)
                        ELSE NULL
                    END AS probe_lon,
                    CASE
                        WHEN row.source_cover_count = 1 THEN 'ok'
                        WHEN row.source_cover_count > 1 THEN 'ambiguous'
                        ELSE 'unplaceable'
                    END AS status
                FROM classified_rows row
                JOIN {oracle} oracle_country
                  ON oracle_country.alpha2 = row.source_country
                ORDER BY
                    row.pair,
                    row.source_country,
                    row.source_city,
                    row.source_lat,
                    row.source_lon,
                    row.neighbor_country,
                    row.neighbor_city,
                    row.neighbor_lat,
                    row.neighbor_lon,
                    ST_Y(row.seam_geom),
                    ST_X(row.seam_geom),
                    row.ring_distance_m
                """
            ).format(
                oracle=sql.Identifier(oracle.schema, oracle.table),
                ring_distances=ring_distance_values,
            )
        )
        return cur.fetchall()


def _probe_row_from_tuple(row: tuple) -> ProbeRow:
    """Adapt a raw cursor tuple from `generate_seam_probe_rows` into `ProbeRow`."""
    return ProbeRow(
        pair=row[0],
        source_country=row[1],
        expected_alpha2=row[2],
        expected_alpha3=row[3],
        expected_country_name=row[4],
        neighbor_country=row[5],
        source_city=row[6],
        source_lat=row[7],
        source_lon=row[8],
        neighbor_city=row[9],
        neighbor_lat=row[10],
        neighbor_lon=row[11],
        seam_lat=row[12],
        seam_lon=row[13],
        ring_distance_m=row[14],
        probe_lat=row[15],
        probe_lon=row[16],
        status=row[17],
    )


def _build_result(
    *,
    requested_country_pairs: frozenset[CountryPair],
    pair_rows: list[tuple],
    probe_rows: list[ProbeRow],
) -> BorderFixtureResult:
    """Derive named summary data from the internal pair/probe row sequences."""
    discovered_counts: Counter[str] = Counter(str(row[0]) for row in pair_rows)
    status_counts_by_pair: dict[str, Counter[str]] = {}
    for probe in probe_rows:
        status_counts_by_pair.setdefault(probe.pair, Counter())[probe.status] += 1

    if requested_country_pairs:
        summary_keys = sorted(
            format_country_pair(pair) for pair in requested_country_pairs
        )
    else:
        summary_keys = sorted(discovered_counts)

    pair_summaries = [
        PairSummary(
            pair=pair,
            discovered_rows=discovered_counts.get(pair, 0),
            status_counts=dict(status_counts_by_pair.get(pair, Counter())),
        )
        for pair in summary_keys
    ]

    if requested_country_pairs:
        missing_required_pairs = [
            pair for pair in summary_keys if discovered_counts.get(pair, 0) == 0
        ]
        missing_ok_pairs = [
            pair
            for pair in summary_keys
            if status_counts_by_pair.get(pair, Counter())["ok"] == 0
        ]
    else:
        missing_required_pairs = []
        missing_ok_pairs = []

    return BorderFixtureResult(
        probe_rows=probe_rows,
        pair_summaries=pair_summaries,
        missing_required_pairs=missing_required_pairs,
        missing_ok_pairs=missing_ok_pairs,
        discovered_pair_count=len(pair_rows),
        requested_country_pairs=requested_country_pairs,
    )


def normalize_country_pair(raw: str) -> CountryPair:
    """Parse a country-pair CLI value into sorted alpha-2 codes."""
    parts = [p.strip().upper() for p in raw.split("/", 1)]
    if len(parts) != 2 or not all(len(p) == 2 for p in parts):
        raise ValueError(f"Invalid country pair {raw!r}; expected like IT/SI")
    a, b = sorted(parts)
    return a, b


def normalize_country_pairs(raw_pairs: Iterable[str]) -> set[CountryPair]:
    """Parse multiple country-pair CLI values."""
    return {normalize_country_pair(raw) for raw in raw_pairs}


def format_country_pair(pair: CountryPair) -> str:
    """Format a normalized country pair for display."""
    return f"{pair[0]}/{pair[1]}"


def format_probe_status_summary(status_counts: Mapping[str, int]) -> str:
    """Format status counts in stable status order for generator summaries."""
    return ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in PROBE_STATUSES
    )


def write_probe_rows(output: Path, rows: list[tuple]) -> None:
    """Write generated seam-relative probe rows to a CSV file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(PROBE_CSV_HEADER)
        writer.writerows(rows)


def _print_result_summary(result: BorderFixtureResult) -> None:
    """Print operator-facing summary derived from the generator's named result."""
    if not result.requested_country_pairs:
        for summary in result.pair_summaries:
            print(f"{summary.pair}: {summary.discovered_rows} discovered city pairs")
        print(
            f"total: {len(result.pair_summaries)} country pairs, "
            f"{result.discovered_pair_count} city pairs"
        )
    for summary in result.pair_summaries:
        print(
            f"{summary.pair}: {summary.discovered_rows} discovered city pairs, "
            f"{format_probe_status_summary(summary.status_counts)}"
        )
    print(f"total: {len(result.probe_rows)} probe rows")


def main() -> None:
    """Run the offline border fixture seed generator."""
    args = parse_args()
    if args.max_pairs_per_country_pair < 1:
        raise SystemExit("--max-pairs-per-country-pair must be >= 1")

    try:
        country_pairs = normalize_country_pairs(args.country_pair)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if country_pairs:
        scope_label = f"{len(country_pairs)} country pairs (override)"
    else:
        scope_label = "all adjacent country pairs (global)"
    print(f"discovering border-city pairs for {scope_label}")

    conn_settings = DBConnSettings().with_overrides(
        **{
            k.lstrip("db_"): v
            for k, v in args._get_kwargs()
            if k.startswith("db_") and v
        }
    )

    request = BorderFixtureRequest(
        db=conn_settings,
        cache_dir=args.cache_dir,
        boundary_source=BoundarySource(args.boundary_source),
        country_pairs=frozenset(country_pairs),
        max_pairs_per_country_pair=args.max_pairs_per_country_pair,
        ring_distances_m=PROBE_RING_DISTANCES_M,
    )
    try:
        result = BorderFixtureGenerator(request).run()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    _print_result_summary(result)

    if result.missing_required_pairs:
        raise SystemExit(
            "no discovered city pairs for required country pairs: "
            + ", ".join(result.missing_required_pairs)
        )
    if result.missing_ok_pairs:
        raise SystemExit(
            "no usable seam probe rows for required country pairs: "
            + ", ".join(result.missing_ok_pairs)
        )

    write_probe_rows(
        args.probes_output, [row.as_csv_row() for row in result.probe_rows]
    )
    print(f"wrote {len(result.probe_rows)} probes to {args.probes_output}")


if __name__ == "__main__":
    main()
