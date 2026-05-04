"""Discover nearby cross-border city pairs for manual fixture selection."""

import argparse
import csv
from pathlib import Path

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--country-pair",
        action="append",
        default=[],
        help="Optional filter like IT/SI; may be passed multiple times",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Parse --country-pair filters into normalized (alpha2_a, alpha2_b) tuples.
    pair_filters: set[tuple[str, str]] = set()
    for raw in args.country_pair:
        parts = [p.strip().upper() for p in raw.split("/", 1)]
        if len(parts) != 2 or not all(len(p) == 2 for p in parts):
            raise SystemExit(f"Invalid country pair {raw!r}; expected like IT/SI")
        pair_filters.add(tuple(sorted(parts)))

    with psycopg.connect(DBConnSettings().conn_string) as conn, conn.cursor() as cur:
        if pair_filters:
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
                sorted(pair_filters),
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
        if pair_filters:
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
            WHERE rn <= {MAX_PAIRS_PER_COUNTRY_PAIR}
            ORDER BY country_a, country_b, distance_m
            """
        )
        rows = cur.fetchall()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(CSV_HEADER)
        writer.writerows(rows)

    print(f"wrote {len(rows)} pairs to {args.output}")


if __name__ == "__main__":
    main()
