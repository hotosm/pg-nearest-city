"""DB queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from psycopg import sql

if TYPE_CHECKING:
    from pg_nearest_city.datasets.sources import PromotedTerritory


def create_database(db_name: str) -> sql.Composed:
    """CREATEs a database."""
    parts = [
        sql.SQL("CREATE DATABASE "),
        sql.Identifier(db_name),
    ]

    return sql.Composed(parts)


def drop_database(db_name: str) -> sql.Composed:
    """DROPs a database."""
    parts = [
        sql.SQL("DROP DATABASE IF EXISTS "),
        sql.Identifier(db_name),
    ]

    return sql.Composed(parts)


def drop_table(table_name: str) -> sql.Composed:
    """DROPs a table."""
    parts = [
        sql.SQL("DROP TABLE IF EXISTS "),
        sql.Identifier(table_name),
    ]

    return sql.Composed(parts)


# Simplify country_init geometries, then subdivide into smaller polygons
# for faster point-in-polygon lookups.  Three tolerance tiers:
#   - Countries sharing a border: 0.00001 (preserve border accuracy)
#   - Small isolated countries/islands (< 25 000 km²): 0.00001
#   - Large isolated countries: 0.001
# ST_Subdivide splits each MultiPolygon into pieces of at most 256
# vertices so GiST bounding boxes are tight.
SIMPLIFY_COUNTRY: str = """
        INSERT INTO country (alpha2, alpha3, name, geom)
        WITH neighbors AS (
          SELECT DISTINCT ci_1.alpha2
          FROM country_init ci_1
          JOIN country_init ci_2 ON ci_1.alpha2 != ci_2.alpha2
            AND ST_Intersects(ci_1.geom, ci_2.geom)
        ),
        simplified AS (
          SELECT
            ci.alpha2,
            ci.alpha3,
            ci.name,
            ST_MakeValid(ST_SimplifyVW(
              ci.geom,
              CASE
                WHEN n.alpha2 IS NOT NULL THEN 0.00001
                WHEN ST_Area(ci.geom::geography) < 2.5e10 THEN 0.00001
                ELSE 0.001
              END
            )) AS geom
          FROM country_init ci
          LEFT JOIN neighbors n ON ci.alpha2 = n.alpha2
        )
        SELECT alpha2, alpha3, name, ST_Subdivide(geom, 256) AS geom
        FROM simplified
"""

# COPY the ISO 3166-1 reference CSV into tmp_iso3166.
INSERT_ISO3166: str = sql.Composed(
    [
        sql.SQL("COPY "),
        sql.Identifier("tmp_iso3166"),
        sql.SQL(" FROM STDIN WITH (FORMAT CSV, HEADER)"),
    ]
).as_string()

# Find ADM0 entries whose alpha3 (gid0) is not in ISO 3166-1, excluding
# Namibia ('NA', which is a valid ISO code that looks like a sentinel) and
# the Caspian Sea placeholder ('XCA').
INSERT_NON_ISO_ADM0: str = """
        INSERT INTO tmp_noniso_adm0
            (gid0, name, geom)
        SELECT a.gid0, a.country, a.geom
        FROM tmp_country_bounds_adm0 a
        WHERE NOT EXISTS (
            SELECT 1 FROM tmp_iso3166 i
            WHERE a.gid0 = i.alpha3
        ) AND
        a.gid0 NOT IN ('NA', 'XCA')
    """

# Map synthetic GADM parent codes (Z01-Z09) back to real ISO alpha3 codes
# by joining through the ADM1 country name to the ADM0 table.
INSERT_NON_ISO_GID0_Z01_Z09_PARENTS: str = """
    INSERT INTO tmp_noniso_gid0_parent (gid0, parent_gid0)
    SELECT
      a1.parent_gid0   AS gid0,
      a0.gid0          AS parent_gid0
    FROM (
      SELECT DISTINCT parent_gid0, country
      FROM tmp_country_bounds_adm1
      WHERE parent_gid0 ~ '^Z[0-9]{2}$'
    ) a1
    JOIN tmp_country_bounds_adm0 a0 ON a0.country = a1.country
    JOIN tmp_iso3166 i ON i.alpha3 = a0.gid0
    """

# Assign remaining non-ISO territories to their parent ISO country using
# spatial heuristics: first by containment (ST_CoveredBy, smallest parent
# wins), then by longest shared border for adjacent territories (>1km).
INSERT_NON_ISO_GID0_BORDER_HEURISTICS: str = """
    WITH iso_parent AS (
      SELECT t.gid0, t.geom
      FROM tmp_country_bounds_adm0 t
      JOIN tmp_iso3166 i ON t.gid0 = i.alpha3
    ),
    covered AS (
      SELECT DISTINCT ON (c.gid0)
        c.gid0,
        p.gid0 AS parent_gid0
      FROM tmp_noniso_adm0 c
      JOIN iso_parent p ON
        c.geom && p.geom AND
        ST_CoveredBy(c.geom, p.geom)
      ORDER BY c.gid0, ST_Area(p.geom::geography) ASC
    ),
    adj AS (
      SELECT DISTINCT ON (c.gid0)
        c.gid0,
        p.gid0 AS parent_gid0,
        ST_Length(ST_Intersection(
            ST_Boundary(c.geom),
            ST_Boundary(p.geom)
        )::geography) AS shared_border_m
      FROM tmp_noniso_adm0 c
      JOIN iso_parent p ON
        c.geom && p.geom AND
        ST_Touches(c.geom, p.geom)
      ORDER BY c.gid0, shared_border_m DESC
    )
    INSERT INTO tmp_noniso_gid0_parent (gid0, parent_gid0)
    SELECT gid0, parent_gid0 FROM covered
    UNION ALL
    SELECT a.gid0, a.parent_gid0
    FROM adj a
    WHERE NOT EXISTS (
        SELECT 1 FROM covered c
        WHERE a.gid0 = c.gid0
    ) AND
    a.shared_border_m > 1000
    ON CONFLICT (gid0) DO NOTHING
    """

# Register ADM1 regions as standalone alpha3 entries so they are
# promoted from their parent country's ADM1 subdivisions to independent
# country rows.  The actual values are boundary-source-specific and
# provided by GADM_PROMOTED_TERRITORIES / NE_PROMOTED_TERRITORIES.
INSERT_PROMOTED_ADM1_TO_GID0: str = """
    INSERT INTO tmp_promoted_adm1_to_gid0
        (gid0, gid1, name, parent_gid0)
    VALUES {}
      """


def insert_promoted_adm1_to_gid0(
    territories: "Sequence[PromotedTerritory]",
) -> str | None:
    """Build INSERT for promoting ADM1 regions to GID0-level entities.

    Returns None when *territories* is empty (no promotions needed).
    """
    if not territories:
        return None
    values = ", ".join(
        sql.SQL("({}, {}, {}, {})")
        .format(
            sql.Literal(t.alpha3),
            sql.Literal(t.gid1),
            sql.Literal(t.name),
            sql.Literal(t.parent_alpha3),
        )
        .as_string()
        for t in territories
    )
    return INSERT_PROMOTED_ADM1_TO_GID0.format(values)


# Copy promoted ADM1 geometries into the ADM0 table so they
# participate in the country boundary pipeline as top-level entries.
INSERT_PROMOTED_ADM1_TO_ADM0: str = """
    INSERT INTO tmp_country_bounds_adm0 (gid0, country, geom)
    SELECT p.gid0, p.name, a1.geom
    FROM tmp_promoted_adm1_to_gid0 p
    JOIN tmp_country_bounds_adm1 a1 ON a1.gid1 = p.gid1
    """

# Upsert ADM0 geometries into country_init, keyed by ISO alpha2.
# Multiple ADM0 rows can map to the same alpha3 (e.g. NE lists France
# and Clipperton Island separately but both resolve to FRA), so we
# union their geometries before upserting.
INSERT_COUNTRY_INIT_GEOM_ADM1: str = """
        INSERT INTO country_init
            (alpha2, alpha3, name, geom)
        SELECT
            i.alpha2,
            t.gid0,
            (ARRAY_AGG(t.country ORDER BY ST_Area(t.geom::geography) DESC))[1],
            ST_Multi(ST_UnaryUnion(ST_Collect(t.geom)))
        FROM tmp_country_bounds_adm0 t
        JOIN tmp_iso3166 i
            ON t.gid0 = i.alpha3
        GROUP BY i.alpha2, t.gid0
        ON CONFLICT (alpha2) DO UPDATE SET
            geom = EXCLUDED.geom,
            name = EXCLUDED.name
    """

# Fast path for GADM: each gid0 has exactly one row in
# tmp_country_bounds_adm0 and maps 1:1 to an ISO alpha3, so the
# GROUP BY / ST_Collect / ST_UnaryUnion / ST_Area aggregation in
# INSERT_COUNTRY_INIT_GEOM_ADM1 is unnecessary.  Geometries are
# already MULTIPOLYGON from ogr2ogr PROMOTE_TO_MULTI.
INSERT_COUNTRY_INIT_GEOM_ADM1_GADM: str = """
        INSERT INTO country_init
            (alpha2, alpha3, name, geom)
        SELECT i.alpha2, t.gid0, t.country, t.geom
        FROM tmp_country_bounds_adm0 t
        JOIN tmp_iso3166 i
            ON t.gid0 = i.alpha3
        ON CONFLICT (alpha2) DO UPDATE SET
            geom = EXCLUDED.geom,
            name = EXCLUDED.name
    """

# Subtract promoted ADM1 geometries from their parent country's
# ADM0 polygon so the parent no longer claims that territory.
UPDATE_TMP_ADM0_FROM_NONISO_DEFS: str = """
        WITH cut AS (
          SELECT
            p.parent_gid0,
            ST_UnaryUnion(ST_Collect(a0.geom)) AS cut_geom
          FROM tmp_promoted_adm1_to_gid0 p
          JOIN tmp_country_bounds_adm0 a0 ON a0.gid0 = p.gid0
          GROUP BY p.parent_gid0
        )
        UPDATE tmp_country_bounds_adm0 parent
        SET geom = ST_Multi(ST_Difference(parent.geom, cut.cut_geom))
            ::geometry(MultiPolygon,4326)
        FROM cut
        WHERE parent.gid0 = cut.parent_gid0
    """

# Merge non-ISO territories back into their parent ISO country's geometry
# (e.g. fold disputed or unrecognised regions into the parent polygon).
UPDATE_TMP_ADM0_FROM_ISO_DEFS: str = """
        WITH addin AS (
          SELECT
            m.parent_gid0,
            ST_UnaryUnion(ST_Collect(child.geom)) AS add_geom
          FROM tmp_noniso_gid0_parent m
          JOIN tmp_country_bounds_adm0 child ON child.gid0 = m.gid0
          GROUP BY m.parent_gid0
        )
        UPDATE tmp_country_bounds_adm0 parent
        SET geom = ST_Multi(
                    ST_UnaryUnion(ST_Collect(ARRAY[parent.geom, addin.add_geom]))
                  )::geometry(MultiPolygon,4326)
        FROM addin
        WHERE parent.gid0 = addin.parent_gid0
    """

# Upsert GeoBoundaries correction polygons into country_init, replacing
# the upstream geometry for countries where it was inaccurate.
UPDATE_COUNTRY_FROM_GEOBOUNDARIES: str = """
    INSERT INTO country_init (alpha2, alpha3, name, geom)
    SELECT i.alpha2, s.shapegroup, s.shapename, s.wkb_geometry
    FROM tmp_country_staging s
    JOIN tmp_iso3166 i ON i.alpha3 = s.shapegroup
    ON CONFLICT (alpha2) DO UPDATE SET
        geom = EXCLUDED.geom
    """

# Overwrite country_init geometries with the (now cleaned) ADM0 polygons.
UPDATE_COUNTRY_INIT_GEOM_ADM0: str = """
        UPDATE country_init ci
        SET geom = t.geom
        FROM tmp_country_bounds_adm0 t
        WHERE ci.alpha3 = t.gid0
    """

# Fix overlapping country polygons after all corrections are applied.
# For each pair of overlapping countries, uses geocoding city presence to
# decide ownership: exclave territory (small country has cities, large doesn't)
# is subtracted from the large country; overshooting borders (small country
# has no cities in the overlap) are trimmed from the small country.
RESOLVE_COUNTRY_OVERLAPS: str = """
    WITH areas AS MATERIALIZED (
        SELECT alpha2, geom, ST_Area(geom::geography) AS area_m2
        FROM country_init
    ),
    candidate_pairs AS MATERIALIZED (
        SELECT
            a.alpha2 AS large_alpha2,
            b.alpha2 AS small_alpha2,
            b.geom   AS small_geom,
            ST_Intersection(a.geom, b.geom) AS overlap_geom
        FROM areas a
        JOIN areas b
            ON a.alpha2 != b.alpha2
            AND a.geom && b.geom
            AND a.area_m2 > b.area_m2
            AND ST_Area(ST_Intersection(a.geom, b.geom)::geography) > 1000000
    ),
    classified AS MATERIALIZED (
        SELECT
            cp.large_alpha2,
            cp.small_alpha2,
            cp.small_geom,
            cp.overlap_geom,
            EXISTS (
                SELECT 1
                FROM geocoding g
                WHERE g.country = cp.large_alpha2
                  AND cp.overlap_geom && g.geom
                  AND ST_Covers(cp.overlap_geom, g.geom)
            ) AS large_has_cities,
            EXISTS (
                SELECT 1
                FROM geocoding g
                WHERE g.country = cp.small_alpha2
                  AND cp.overlap_geom && g.geom
                  AND ST_Covers(cp.overlap_geom, g.geom)
            ) AS small_has_cities
        FROM candidate_pairs cp
    ),
    -- Exclave: the small country has geocoding cities in the overlap but the
    -- large country does not.  The overlap is legitimately the small country's
    -- territory (an exclave inside the larger country's polygon).  Subtract
    -- the small country's full geometry from the large country so the exclave
    -- is no longer double-claimed.
    exclave_subtractions AS (
        SELECT large_alpha2 AS alpha2, ST_Union(small_geom) AS to_subtract
        FROM classified
        WHERE small_has_cities AND NOT large_has_cities
        GROUP BY large_alpha2
    ),
    -- Overshoot / uninhabited border: the small country's polygon extends into
    -- the large country's territory (small has no cities in the overlap).  This
    -- covers both the explicit overshoot case (large has cities there) and the
    -- uninhabited-border case (neither country has cities in the overlap, e.g.
    -- a coarse source polygon that bleeds across a mountain border).  In both
    -- situations, trim only the intersection out of the small country's polygon.
    overshoot_subtractions AS (
        SELECT small_alpha2 AS alpha2, ST_Union(overlap_geom) AS to_subtract
        FROM classified
        WHERE NOT small_has_cities
        GROUP BY small_alpha2
    ),
    all_subtractions AS (
        SELECT alpha2, to_subtract FROM exclave_subtractions
        UNION ALL
        SELECT alpha2, to_subtract FROM overshoot_subtractions
    ),
    merged AS (
        SELECT alpha2, ST_Union(to_subtract) AS to_subtract
        FROM all_subtractions
        GROUP BY alpha2
    )
    UPDATE country_init ci
    SET geom = ST_Multi(
                   ST_Difference(ci.geom, m.to_subtract)
               )::geometry(MultiPolygon, 4326)
    FROM merged m
    WHERE ci.alpha2 = m.alpha2
    """


def select_adm0(
    file_layer: str,
    alpha3_column: str,
    adm0_name_column: str,
    country_filter: set[str] | None = None,
    exclude_alpha3: list[str] | None = None,
) -> str:
    """Query for ADM0 (country-level) boundaries."""
    parts: list[sql.Composable] = [
        sql.SQL("SELECT "),
        sql.Identifier(alpha3_column),
        sql.SQL(" AS gid0, "),
        sql.Identifier(adm0_name_column),
        sql.SQL(" AS country FROM "),
        sql.Identifier(file_layer),
    ]

    conditions: list[sql.Composable] = []
    if country_filter:
        conditions.append(
            sql.Composed(
                [
                    sql.Identifier(alpha3_column),
                    sql.SQL(" IN "),
                    sql.SQL("(")
                    + sql.SQL(", ").join(sql.Literal(v) for v in sorted(country_filter))
                    + sql.SQL(")"),
                ]
            )
        )
    if exclude_alpha3:
        conditions.append(
            sql.Composed(
                [
                    sql.Identifier(alpha3_column),
                    sql.SQL(" NOT IN "),
                    sql.SQL("(")
                    + sql.SQL(", ").join(sql.Literal(v) for v in exclude_alpha3)
                    + sql.SQL(")"),
                ]
            )
        )
    if conditions:
        parts.append(sql.SQL(" WHERE "))
        parts.append(sql.SQL(" AND ").join(conditions))

    return sql.Composed(parts).as_string()


def select_adm1(
    file_layer: str,
    alpha3_column: str,
    adm0_name_column: str,
    adm1_column: str,
    adm1_name_column: str,
    exclude_alpha3: list[str] | None = None,
) -> str:
    """Query for ADM1 (first-level administrative divisions)."""
    parts: list[sql.Composable] = [
        sql.SQL("SELECT "),
        sql.Identifier(alpha3_column),
        sql.SQL(" AS parent_gid0, "),
        sql.Identifier(adm1_column),
        sql.SQL(" AS gid1, "),
        sql.Identifier(adm0_name_column),
        sql.SQL(" AS country, "),
        sql.Identifier(adm1_name_column),
        sql.SQL(" AS name_1 FROM "),
        sql.Identifier(file_layer),
    ]

    if exclude_alpha3:
        parts.append(sql.SQL(" WHERE "))
        parts.append(
            sql.Composed(
                [
                    sql.Identifier(alpha3_column),
                    sql.SQL(" NOT IN "),
                    sql.SQL("(")
                    + sql.SQL(", ").join(sql.Literal(v) for v in exclude_alpha3)
                    + sql.SQL(")"),
                ]
            )
        )

    return sql.Composed(parts).as_string()


def upsert_overpass_boundary(tmp_tbl: str) -> sql.Composed:
    """Upsert an Overpass boundary from a temporary ogr2ogr table into country_init."""
    return sql.SQL(
        "INSERT INTO {dst} ({alpha2}, {alpha3}, {name}, {geom})"
        " SELECT %s, %s, %s,"
        " ST_Multi(ST_MakeValid(ST_UnaryUnion(ST_Collect({wkb}))))"
        " FROM {src}"
        " WHERE {boundary} = 'administrative'"
        " ON CONFLICT ({alpha2}) DO UPDATE SET"
        " {geom} = EXCLUDED.{geom}"
    ).format(
        dst=sql.Identifier("country_init"),
        alpha2=sql.Identifier("alpha2"),
        alpha3=sql.Identifier("alpha3"),
        name=sql.Identifier("name"),
        geom=sql.Identifier("geom"),
        wkb=sql.Identifier("wkb_geometry"),
        src=sql.Identifier(tmp_tbl),
        boundary=sql.Identifier("boundary"),
    )


def vacuum_analyze(full: bool) -> str:
    """VACUUM [FULL] and ANALYZE tables."""
    parts = [
        sql.SQL("VACUUM (ANALYZE"),
    ]
    if full:
        parts.append(sql.SQL(", FULL"))

    parts.append(sql.SQL(")"))

    return sql.Composed(parts).as_string()
