"""Tests for pg_nearest_city.db.corrections data definitions."""

import pytest

from pg_nearest_city.db.corrections import (
    BOUNDARY_CORRECTIONS,
    DATA_CORRECTIONS,
    GADM_DATA_CORRECTIONS,
    NE_BOUNDARY_CORRECTIONS,
)
from pg_nearest_city.db.data_cleanup import make_queries


@pytest.mark.integration
class TestDataCorrections:
    def test_common_corrections_produce_valid_sql(self, test_db_conn_string):
        """All DATA_CORRECTIONS generate valid psycopg.sql objects."""
        import psycopg
        import psycopg.sql

        conn = psycopg.Connection.connect(test_db_conn_string)
        queries = make_queries(DATA_CORRECTIONS)
        assert len(queries) == len(DATA_CORRECTIONS)
        for q in queries:
            assert isinstance(q, psycopg.sql.Composed)
            sql_str = q.as_string(conn)
            assert "UPDATE" in sql_str or "DELETE" in sql_str or "INSERT" in sql_str
        conn.close()

    def test_gadm_corrections_produce_valid_sql(self, test_db_conn_string):
        import psycopg

        conn = psycopg.Connection.connect(test_db_conn_string)
        queries = make_queries(GADM_DATA_CORRECTIONS)
        assert len(queries) == len(GADM_DATA_CORRECTIONS)
        for q in queries:
            sql_str = q.as_string(conn)
            assert "UPDATE" in sql_str
        conn.close()


class TestBoundaryCorrections:
    def test_common_boundary_corrections_defined(self):
        assert len(BOUNDARY_CORRECTIONS) > 0
        isos = {t.iso for t in BOUNDARY_CORRECTIONS}
        assert "BEL" in isos
        assert "ITA" in isos

    def test_ne_boundary_corrections_defined(self):
        assert len(NE_BOUNDARY_CORRECTIONS) > 0
        isos = {t.iso for t in NE_BOUNDARY_CORRECTIONS}
        assert "GIB" in isos
        assert "USA" in isos
