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

CSV_HEADER = [
    "pair",
    "country_a",
    "country_b",
    "country_name_a",
    "country_name_b",
    "city_a",
    "city_b",
    "lat_a",
    "lon_a",
    "lat_b",
    "lon_b",
    "distance_m",
    "seam_length_m",
]

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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse border fixture generator arguments."""
    parser = argparse.ArgumentParser(
        description="Regenerate offline border fixture seeds from runtime seams."
    )
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
        "--pairs-output",
        "--pair-output",
        "--output",
        dest="pairs_output",
        type=Path,
        help="Optional CSV path for inspecting discovered border-city pairs",
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


def count_rows_by_pair(rows: list[tuple]) -> Counter[str]:
    """Count discovered rows by CSV pair key."""
    return Counter(str(row[0]) for row in rows)


def count_probe_rows_by_pair_and_status(rows: list[tuple]) -> dict[str, Counter[str]]:
    """Count generated probe rows by pair and auditable row status."""
    counts: dict[str, Counter[str]] = {}
    for row in rows:
        pair = str(row[0])
        status = str(row[-1])
        counts.setdefault(pair, Counter())[status] += 1
    return counts


def pairs_without_ok_probes(
    country_pairs: set[CountryPair], probe_rows: list[tuple]
) -> list[str]:
    """Return formatted country pairs with no usable generated probe rows."""
    counts = count_probe_rows_by_pair_and_status(probe_rows)
    return [
        pair
        for pair in sorted(format_country_pair(pair) for pair in country_pairs)
        if counts.get(pair, Counter())["ok"] == 0
    ]


def print_pair_summary(country_pairs: set[CountryPair], rows: list[tuple]) -> None:
    """Print a short deterministic discovery summary."""
    counts = count_rows_by_pair(rows)
    for pair in sorted(format_country_pair(pair) for pair in country_pairs):
        print(f"{pair}: {counts[pair]} discovered city pairs")


def print_discovered_pair_summary(rows: list[tuple]) -> None:
    """Print per-pair counts using only what discovery produced."""
    counts = count_rows_by_pair(rows)
    for pair in sorted(counts):
        print(f"{pair}: {counts[pair]} discovered city pairs")
    print(f"total: {len(counts)} country pairs, {len(rows)} city pairs")


def format_probe_status_summary(status_counts: Mapping[str, int]) -> str:
    """Format status counts in stable status order for generator summaries."""
    return ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in PROBE_STATUSES
    )


def print_probe_summary(
    pairs: Iterable[str], pair_rows: list[tuple], probe_rows: list[tuple]
) -> None:
    """Print per-pair discovery and probe-status counts."""
    discovered_counts = count_rows_by_pair(pair_rows)
    status_counts = count_probe_rows_by_pair_and_status(probe_rows)
    for pair in sorted(pairs):
        print(
            f"{pair}: {discovered_counts[pair]} discovered city pairs, "
            f"{format_probe_status_summary(status_counts.get(pair, Counter()))}"
        )
    print(f"total: {len(probe_rows)} probe rows")


def pairs_without_rows(country_pairs: set[CountryPair], rows: list[tuple]) -> list[str]:
    """Return formatted country pairs with no discovered seed rows."""
    counts = count_rows_by_pair(rows)
    return [
        pair
        for pair in sorted(format_country_pair(pair) for pair in country_pairs)
        if counts[pair] == 0
    ]


def discover_border_city_pairs(
    cur: psycopg.Cursor,
    *,
    country_pairs: set[CountryPair] | None = None,
    max_pairs_per_country_pair: int = MAX_PAIRS_PER_COUNTRY_PAIR,
) -> list[tuple]:
    """Discover nearby cross-border city pairs using runtime-country seams."""
    if max_pairs_per_country_pair < 1:
        raise ValueError("max_pairs_per_country_pair must be >= 1")

    if country_pairs:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_target_pairs (
                country_a CHAR(2) NOT NULL,
                country_b CHAR(2) NOT NULL,
                PRIMARY KEY (country_a, country_b)
            ) ON COMMIT DROP
            """
        )
        cur.executemany(
            """
            INSERT INTO tmp_target_pairs (country_a, country_b)
            VALUES (%s, %s)
            """,
            sorted(country_pairs),
        )

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

    pair_join = ""
    if country_pairs:
        pair_join = """
        JOIN tmp_target_pairs p
          ON p.country_a = a.alpha2
         AND p.country_b = b.alpha2
        """

    cur.execute(
        f"""
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
          AND ST_Length(seam_geom::geography) >= {MIN_SEAM_LENGTH_M}
        """
    )
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


def generate_seam_probe_rows(
    cur: psycopg.Cursor,
    *,
    oracle: CountryOracleRef,
    ring_distances_m: Iterable[int] = PROBE_RING_DISTANCES_M,
) -> list[tuple]:
    """Generate seam-relative inward probe rows from discovered seed pairs.

    `discover_border_city_pairs` must have been called first on the same
    connection so the temporary seam and selected-pair tables exist.
    """
    distances = sorted({int(distance) for distance in ring_distances_m})
    if not distances or distances[0] <= 0:
        raise ValueError("ring_distances_m must contain positive distances")

    cur.execute("DROP TABLE IF EXISTS tmp_probe_ring_distances")
    cur.execute(
        """
        CREATE TEMP TABLE tmp_probe_ring_distances (
            distance_m integer PRIMARY KEY
        ) ON COMMIT DROP
        """
    )
    cur.executemany(
        "INSERT INTO tmp_probe_ring_distances (distance_m) VALUES (%s)",
        [(distance,) for distance in distances],
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
                CROSS JOIN tmp_probe_ring_distances distances
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
        ).format(oracle=sql.Identifier(oracle.schema, oracle.table))
    )
    return cur.fetchall()


def write_pair_rows(output: Path, rows: list[tuple]) -> None:
    """Write discovered border-city pairs to a CSV file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(CSV_HEADER)
        writer.writerows(rows)


def write_probe_rows(output: Path, rows: list[tuple]) -> None:
    """Write generated seam-relative probe rows to a CSV file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(PROBE_CSV_HEADER)
        writer.writerows(rows)


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

    conn_settings = DBConnSettings()
    boundary_source = BoundarySource(args.boundary_source)
    probe_rows: list[tuple] = []
    try:
        with corrected_country_oracle(
            conn_settings=conn_settings,
            cache_dir=args.cache_dir,
            boundary_source=boundary_source,
        ) as oracle:
            print(f"rebuilt corrected country oracle at {oracle.qualified_name}")
            with (
                psycopg.connect(conn_settings.conn_string) as conn,
                conn.cursor() as cur,
            ):
                rows = discover_border_city_pairs(
                    cur,
                    country_pairs=country_pairs or None,
                    max_pairs_per_country_pair=args.max_pairs_per_country_pair,
                )
                probe_rows = generate_seam_probe_rows(cur, oracle=oracle)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    if country_pairs:
        summary_pairs = [format_country_pair(pair) for pair in country_pairs]
        print_probe_summary(summary_pairs, rows, probe_rows)
        missing_pairs = pairs_without_rows(country_pairs, rows)
        if missing_pairs:
            raise SystemExit(
                "no discovered city pairs for required country pairs: "
                + ", ".join(missing_pairs)
            )
        missing_ok_pairs = pairs_without_ok_probes(country_pairs, probe_rows)
        if missing_ok_pairs:
            raise SystemExit(
                "no usable seam probe rows for required country pairs: "
                + ", ".join(missing_ok_pairs)
            )
    else:
        print_discovered_pair_summary(rows)
        print_probe_summary(count_rows_by_pair(rows).keys(), rows, probe_rows)

    if args.pairs_output is not None:
        write_pair_rows(args.pairs_output, rows)
        print(f"wrote {len(rows)} pairs to {args.pairs_output}")
    else:
        print(f"discovered {len(rows)} pairs; pass --pairs-output to write CSV")

    write_probe_rows(args.probes_output, probe_rows)
    print(f"wrote {len(probe_rows)} probes to {args.probes_output}")


if __name__ == "__main__":
    main()
