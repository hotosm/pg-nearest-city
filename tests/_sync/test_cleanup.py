from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import psycopg
import pytest
from pg_nearest_city.db.data_cleanup import (ROWS_TO_CLEAN, SQL_CLEAN_BASE_DEL,
                                             SQL_CLEAN_BASE_UPD,
                                             SQL_CLEAN_PREDICATES, Comment,
                                             PredicateData, make_queries)

if TYPE_CHECKING:
    from pg_nearest_city.db.data_cleanup import (_DML, _Comment,
                                                 _PredicateComparison)


@pytest.fixture()
def test_db(test_db_conn_string):
    """Provide a clean database connection for each test."""
    conn = psycopg.Connection.connect(test_db_conn_string)

    yield conn

    conn.close()


class TestMakeQueries:
    """Test cases for make_queries function."""

    @patch("pg_nearest_city.db.tables.get_all_table_classes")
    def test_make_queries_update_single_predicate(self, mock_get_tables, test_db):
        """Test make_queries with single UPDATE operation."""
        mock_table = Mock()
        mock_table.name = "geocoding"
        mock_get_tables.return_value = [mock_table]

        predicate = PredicateData(
            col_name="city", comparison=_PredicateComparison.EQUAL, col_val="Old City"
        )

        row_data = RowData(
            col_name="city",
            col_val="New City",
            comment=_Comment.SPELLING,
            dml=_DML.UPDATE,
            predicate_cols=[predicate],
            tbl_name="geocoding",
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """UPDATE "geocoding" SET "city" = 'New City' WHERE """
            """"city" = 'Old City'"""
        )

    @patch("pg_nearest_city.db.tables.get_all_table_classes")
    def test_make_queries_update_multiple_predicates(self, mock_get_tables, test_db):
        """Test make_queries with single UPDATE operation."""
        mock_table = Mock()
        mock_table.name = "geocoding"
        mock_get_tables.return_value = [mock_table]

        predicates = [
            PredicateData(
                col_name="city",
                comparison=_PredicateComparison.EQUAL,
                col_val="Old City",
            ),
            PredicateData(
                col_name="created_at",
                comparison=_PredicateComparison.BETWEEN,
                col_val="1995-05-23",
                col_val_2="2038-01-01",
            ),
        ]

        row_data = RowData(
            col_name="city",
            col_val="New City",
            comment=_Comment.SPELLING,
            dml=_DML.UPDATE,
            predicate_cols=predicates,
            tbl_name="geocoding",
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """UPDATE "geocoding" SET "city" = 'New City' WHERE """
            """"city" = 'Old City' AND "created_at" BETWEEN """
            """'1995-05-23' AND '2038-01-01'"""
        )

    @patch("pg_nearest_city.db.tables.get_all_table_classes")
    def test_make_queries_delete_operation(self, mock_get_tables, test_db):
        """Test make_queries with DELETE operation."""
        mock_table = Mock()
        mock_table.name = "geocoding"
        mock_get_tables.return_value = [mock_table]

        predicate = PredicateData(
            col_name="city",
            comparison=_PredicateComparison.EQUAL,
            col_val="Dallas",
        )

        row_data = RowData(
            comment=_Comment.ERRATUM,
            dml=_DML.DELETE,
            predicate_cols=[predicate],
            tbl_name="geocoding",
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == """DELETE FROM "geocoding" WHERE "city" = 'Dallas'"""

    @patch("pg_nearest_city.db.tables.get_all_table_classes")
    def test_make_queries_insert_raises_not_implemented(self, mock_get_tables):
        """Test make_queries raises NotImplemented for INSERT operations."""
        mock_table = Mock()
        mock_table.name = "geocoding"
        mock_get_tables.return_value = [mock_table]

        with pytest.raises(NotImplementedError):
            row_data = RowData(
                col_name="city",
                col_val="New City",
                comment=_Comment.MISSING,
                dml=_DML.INSERT,
                tbl_name="geocoding",
            )
            make_queries([row_data])
