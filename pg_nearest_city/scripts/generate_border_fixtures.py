"""Offline entry point for regenerating border fixture seeds."""

import argparse
import csv
from collections import Counter
from pathlib import Path
from typing import Iterable

import psycopg

from pg_nearest_city.db.settings import DBConnSettings


COUNTRY_TABLE = "country"
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse border fixture generator arguments."""
    parser = argparse.ArgumentParser(
        description="Regenerate offline border fixture seeds from runtime seams."
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

    with psycopg.connect(DBConnSettings().conn_string) as conn, conn.cursor() as cur:
        rows = discover_border_city_pairs(
            cur,
            country_pairs=country_pairs or None,
            max_pairs_per_country_pair=args.max_pairs_per_country_pair,
        )

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
