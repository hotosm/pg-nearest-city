"""Tests for pg_nearest_city.db.queries."""

from pg_nearest_city.db import queries


class TestDropTable:
    def test_drop_table_sql(self):
        result = queries.drop_table("my_table").as_string()
        assert result == 'DROP TABLE IF EXISTS "my_table"'


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
