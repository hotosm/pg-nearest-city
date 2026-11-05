"""DB queries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg import sql

if TYPE_CHECKING:
    pass


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


# Simplify, snap to a 0.0001 degree grid (about 11m) so the file compresses
# well, then subdivide for fast point lookups. Small countries keep more detail.
SIMPLIFY_COUNTRY: str = """
        INSERT INTO country (alpha2, alpha3, name, geom)
        WITH simplified AS (
          SELECT
            alpha2,
            alpha3,
            name,
            ST_CollectionExtract(ST_ReducePrecision(ST_MakeValid(ST_SimplifyVW(
              geom,
              CASE WHEN ST_Area(geom::geography) < 1e9 THEN 1e-7 ELSE 3e-5 END
            )), 0.0001), 3) AS geom
          FROM country_init
        )
        SELECT alpha2, alpha3, name, ST_Subdivide(geom, 256) AS geom
        FROM simplified
        WHERE NOT ST_IsEmpty(geom)
"""

SELECT_CGAZ: str = "SELECT shapeGroup AS alpha3 FROM globalADM0"

COPY_COUNTRY_NAMES: str = (
    "COPY tmp_country_names (alpha2, alpha3, name) FROM STDIN WITH (FORMAT CSV)"
)

COPY_BOUNDARY_OVERRIDES: str = (
    "COPY tmp_boundary_overrides (alpha2, geom) FROM STDIN WITH (FORMAT CSV)"
)

# Overrides replace CGAZ wherever they overlap. CGAZ disputed zones have
# numeric codes, so the names join drops them and only overrides remain there.
# Subdividing first takes this from hours to minutes on the largest countries.
BUILD_COUNTRY_INIT: str = """
    INSERT INTO country_init (alpha2, alpha3, name, geom)
    WITH parts AS (
        SELECT
            n.alpha2,
            COALESCE(ST_Difference(piece.geom, kept.geom), piece.geom) AS geom
        FROM tmp_cgaz c
        JOIN tmp_country_names n ON n.alpha3 = c.alpha3
        CROSS JOIN LATERAL ST_Subdivide(
            ST_MakeValid(ST_SetSRID(c.geom, 4326)), 1024
        ) AS piece(geom)
        LEFT JOIN LATERAL (
            SELECT ST_Union(o.geom) AS geom
            FROM tmp_boundary_overrides o
            WHERE o.geom && piece.geom
        ) kept ON TRUE
        UNION ALL
        SELECT alpha2, geom FROM tmp_boundary_overrides
    )
    SELECT
        n.alpha2,
        n.alpha3,
        n.name,
        ST_Multi(ST_CollectionExtract(ST_UnaryUnion(ST_Collect(p.geom)), 3))
    FROM parts p
    JOIN tmp_country_names n ON n.alpha2 = p.alpha2
    GROUP BY n.alpha2, n.alpha3, n.name
"""

DELETE_UNFILTERED_COUNTRIES: str = "DELETE FROM country_init WHERE NOT alpha2 = ANY(%s)"


def vacuum_analyze(full: bool) -> str:
    """VACUUM [FULL] and ANALYZE tables."""
    parts = [
        sql.SQL("VACUUM (ANALYZE"),
    ]
    if full:
        parts.append(sql.SQL(", FULL"))

    parts.append(sql.SQL(")"))

    return sql.Composed(parts).as_string()
