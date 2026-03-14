"""DB queries."""

from psycopg import sql


def drop_table(table_name: str) -> sql.Composed:
    """DROPs a table."""
    parts = [
        sql.SQL("DROP TABLE IF EXISTS "),
        sql.Identifier(table_name),
    ]

    return sql.Composed(parts)


SIMPLIFY_COUNTRY: str = """
        INSERT INTO country
        WITH neighbors AS (
          SELECT DISTINCT ci_1.alpha2
          FROM country_init ci_1
          JOIN country_init ci_2 ON ci_1.alpha2 != ci_2.alpha2
            AND ST_Intersects(ci_1.geom, ci_2.geom)
        )
        SELECT
          ci.alpha2,
          ci.alpha3,
          ci.name,
          ST_SimplifyVW(
            ci.geom,
            CASE WHEN n.alpha2 IS NOT NULL THEN 0.00001 ELSE 0.001 END
          ) AS geom
        FROM country_init ci
        LEFT JOIN neighbors n ON ci.alpha2 = n.alpha2
        ON CONFLICT (alpha2) DO UPDATE SET
            geom = EXCLUDED.geom,
            name = EXCLUDED.name
"""

INSERT_ISO3166: str = sql.Composed(
    [
        sql.SQL("COPY "),
        sql.Identifier("tmp_iso3166"),
        sql.SQL(" FROM STDIN WITH (FORMAT CSV, HEADER)"),
    ]
).as_string()

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

INSERT_PROMOTED_ADM1_TO_GID0: str = """
    INSERT INTO tmp_promoted_adm1_to_gid0
        (gid0, gid1, name, parent_gid0)
    VALUES
        ('HKG', 'CHN.HKG', 'Hong Kong', 'CHN'),
        ('MAC', 'CHN.MAC', 'Macau', 'CHN')
      """

INSERT_PROMOTED_ADM1_TO_ADM0: str = """
    INSERT INTO tmp_country_bounds_adm0 (gid0, country, geom)
    SELECT p.gid0, p.name, a1.geom
    FROM tmp_promoted_adm1_to_gid0 p
    JOIN tmp_country_bounds_adm1 a1 ON a1.gid1 = p.gid1
    """

INSERT_COUNTRY_INIT_GEOM_ADM1: str = """
        INSERT INTO country_init
            (alpha2, alpha3, name, geom)
        SELECT
            i.alpha2, t.gid0, t.country, t.geom
        FROM tmp_country_bounds_adm0 t
        JOIN tmp_iso3166 i
            ON t.gid0 = i.alpha3
        ON CONFLICT (alpha2) DO UPDATE SET
            geom = EXCLUDED.geom,
            name = EXCLUDED.name
    """

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

UPDATE_COUNTRY_FROM_GEOBOUNDARIES: str = """
    UPDATE country_init ci
    SET geom = s.wkb_geometry
    FROM tmp_country_staging s
    WHERE ci.alpha3 = s.shapegroup
    """

UPDATE_COUNTRY_INIT_GEOM_ADM0: str = """
        UPDATE country_init ci
        SET geom = t.geom
        FROM tmp_country_bounds_adm0 t
        WHERE ci.alpha3 = t.gid0
    """


def select_adm0(
    file_layer: str,
    alpha3_column: str,
    adm0_name_column: str,
    country_filter: str | None = None,
) -> str:
    """Query for ADM0 (country-level) boundaries."""
    parts: list[sql.Composable] = [
        sql.SQL("SELECT "),
        sql.Identifier(alpha3_column),
        sql.SQL(" AS gid0, "),
        sql.Identifier(adm0_name_column),
        sql.SQL(" AS country, "),
        sql.Identifier("geom"),
        sql.SQL(" FROM "),
        sql.Identifier(file_layer),
    ]

    if country_filter:
        parts.extend(
            [
                sql.SQL(" WHERE "),
                sql.Identifier(alpha3_column),
                sql.SQL(" = "),
                sql.Literal(country_filter),
            ]
        )

    return sql.Composed(parts).as_string()


def select_adm1(
    file_layer: str,
    alpha3_column: str,
    adm0_name_column: str,
    adm1_column: str,
    adm1_name_column: str,
) -> str:
    """Query for ADM1 (first-level administrative divisions)."""
    parts = [
        sql.SQL("SELECT "),
        sql.Identifier(alpha3_column),
        sql.SQL(" AS parent_gid0, "),
        sql.Identifier(adm1_column),
        sql.SQL(" AS gid1, "),
        sql.Identifier(adm0_name_column),
        sql.SQL(" AS country, "),
        sql.Identifier(adm1_name_column),
        sql.SQL(" AS name_1, "),
        sql.Identifier("geom"),
        sql.SQL(" FROM "),
        sql.Identifier(file_layer),
    ]

    return sql.Composed(parts).as_string()


def vacuum_analyze(full: bool) -> str:
    """VACUUM [FULL] and ANALYZE tables."""
    parts = [
        sql.SQL("VACUUM (ANALYZE"),
    ]
    if full:
        parts.append(sql.SQL(", FULL"))

    parts.append(sql.SQL(")"))

    return sql.Composed(parts).as_string()
