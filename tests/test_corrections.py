"""Tests for pg_nearest_city.db.corrections data definitions."""

import pytest

from pg_nearest_city.db.corrections import DATA_CORRECTIONS
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

    def test_no_kosovo_to_serbia_remap_in_common_corrections(self):
        for row in DATA_CORRECTIONS:
            assert not (
                row.tbl_name == "geocoding"
                and row.col_val == "RS"
                and any(p.col_val == "XK" for p in row.predicate_cols)
            )
