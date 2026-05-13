"""Offline entry point for regenerating border fixture seeds."""

import argparse
import csv
import logging
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

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

    cur.execute(
        f"""
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
            ROUND(distance_m::numeric, 1) AS distance_m,
            ROUND(seam_length_m::numeric, 1) AS seam_length_m
        FROM ranked_pairs
        WHERE rn <= {max_pairs_per_country_pair}
        ORDER BY country_a, country_b, distance_m
        """
    )
    return cur.fetchall()


def write_pair_rows(output: Path, rows: list[tuple]) -> None:
    """Write discovered border-city pairs to a CSV file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(CSV_HEADER)
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
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    if country_pairs:
        print_pair_summary(country_pairs, rows)
        missing_pairs = pairs_without_rows(country_pairs, rows)
        if missing_pairs:
            raise SystemExit(
                "no discovered city pairs for required country pairs: "
                + ", ".join(missing_pairs)
            )
    else:
        print_discovered_pair_summary(rows)

    if args.pairs_output is not None:
        write_pair_rows(args.pairs_output, rows)
        print(f"wrote {len(rows)} pairs to {args.pairs_output}")
    else:
        print(f"discovered {len(rows)} pairs; pass --pairs-output to write CSV")


if __name__ == "__main__":
    main()
