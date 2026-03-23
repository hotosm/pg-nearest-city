"""Tests for pg_nearest_city.db.queries."""

from pg_nearest_city.db import queries
from pg_nearest_city.datasets.sources import (
    GADM_PROMOTED_TERRITORIES,
    NE_PROMOTED_TERRITORIES,
    PromotedTerritory,
)


class TestDropTable:
    def test_drop_table_sql(self):
        result = queries.drop_table("my_table").as_string()
        assert result == 'DROP TABLE IF EXISTS "my_table"'


class TestSelectAdm0:
    def test_basic_select(self):
        sql = queries.select_adm0(
            file_layer="countries",
            alpha3_column="ADM0_A3",
            adm0_name_column="NAME",
        )
        assert '"ADM0_A3" AS gid0' in sql
        assert '"NAME" AS country' in sql
        assert '"countries"' in sql
        assert "WHERE" not in sql

    def test_with_country_filter(self):
        sql = queries.select_adm0(
            file_layer="countries",
            alpha3_column="ADM0_A3",
            adm0_name_column="NAME",
            country_filter={"ITA"},
        )
        assert "WHERE" in sql
        assert "'ITA'" in sql

    def test_with_multi_country_filter(self):
        sql = queries.select_adm0(
            file_layer="countries",
            alpha3_column="ADM0_A3",
            adm0_name_column="NAME",
            country_filter={"ITA", "VAT", "SMR"},
        )
        assert "WHERE" in sql
        assert "IN" in sql
        assert "'ITA'" in sql
        assert "'VAT'" in sql
        assert "'SMR'" in sql

    def test_with_exclusion(self):
        sql = queries.select_adm0(
            file_layer="countries",
            alpha3_column="ADM0_A3",
            adm0_name_column="NAME",
            exclude_alpha3=["XCA", "NA"],
        )
        assert "NOT IN" in sql
        assert "'XCA'" in sql
        assert "'NA'" in sql

    def test_with_both_filter_and_exclusion(self):
        sql = queries.select_adm0(
            file_layer="countries",
            alpha3_column="ADM0_A3",
            adm0_name_column="NAME",
            country_filter={"FRA"},
            exclude_alpha3=["XCA"],
        )
        assert "WHERE" in sql
        assert "AND" in sql


class TestSelectAdm1:
    def test_basic_select(self):
        sql = queries.select_adm1(
            file_layer="provinces",
            alpha3_column="adm0_a3",
            adm0_name_column="admin",
            adm1_column="adm1_code",
            adm1_name_column="name",
        )
        assert '"adm0_a3" AS parent_gid0' in sql
        assert '"adm1_code" AS gid1' in sql
        assert "WHERE" not in sql

    def test_with_exclusion(self):
        sql = queries.select_adm1(
            file_layer="provinces",
            alpha3_column="adm0_a3",
            adm0_name_column="admin",
            adm1_column="adm1_code",
            adm1_name_column="name",
            exclude_alpha3=["FRA"],
        )
        assert "NOT IN" in sql
        assert "'FRA'" in sql


class TestInsertPromotedAdm1:
    def test_returns_none_for_empty(self):
        assert queries.insert_promoted_adm1_to_gid0([]) is None

    def test_gadm_territories(self):
        sql = queries.insert_promoted_adm1_to_gid0(GADM_PROMOTED_TERRITORIES)
        assert sql is not None
        assert "'HKG'" in sql
        assert "'CHN.HKG'" in sql
        assert "'MAC'" in sql

    def test_ne_territories(self):
        sql = queries.insert_promoted_adm1_to_gid0(NE_PROMOTED_TERRITORIES)
        assert sql is not None
        assert "'GLP'" in sql
        assert "'FRA-4603'" in sql
        assert "'GUF'" in sql

    def test_single_territory(self):
        t = PromotedTerritory(
            alpha3="TST", gid1="PAR.TST", name="Test", parent_alpha3="PAR"
        )
        sql = queries.insert_promoted_adm1_to_gid0([t])
        assert sql is not None
        assert "VALUES" in sql
        assert "'TST'" in sql


class TestVacuumAnalyze:
    def test_without_full(self):
        sql = queries.vacuum_analyze(full=False)
        assert "VACUUM" in sql
        assert "ANALYZE" in sql
        assert "FULL" not in sql

    def test_with_full(self):
        sql = queries.vacuum_analyze(full=True)
        assert "FULL" in sql


class TestSqlConstants:
    """Verify key SQL constants are non-empty strings."""

    def test_simplify_country(self):
        assert "ST_Subdivide" in queries.SIMPLIFY_COUNTRY

    def test_insert_iso3166(self):
        assert "COPY" in queries.INSERT_ISO3166

    def test_insert_country_init_geom_adm1(self):
        assert "ST_UnaryUnion" in queries.INSERT_COUNTRY_INIT_GEOM_ADM1

    def test_insert_country_init_geom_adm1_gadm(self):
        sql = queries.INSERT_COUNTRY_INIT_GEOM_ADM1_GADM
        assert "ST_UnaryUnion" not in sql
        assert "ON CONFLICT" in sql

    def test_resolve_country_overlaps(self):
        assert "exclave" in queries.RESOLVE_COUNTRY_OVERLAPS.lower()

    def test_update_country_from_geoboundaries(self):
        assert "tmp_country_staging" in queries.UPDATE_COUNTRY_FROM_GEOBOUNDARIES
