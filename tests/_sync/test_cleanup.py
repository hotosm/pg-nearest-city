import psycopg
import psycopg.sql
import pytest

from pg_nearest_city.db.data_cleanup import (
    DeleteData,
    InsertData,
    PredicateData,
    UpdateData,
    _ConflictAction,
    _CorrectionType,
    _PredicateComparison,
    make_queries,
)


@pytest.fixture()
def test_db(test_db_conn_string):
    """Provide a clean database connection for each test."""
    conn = psycopg.Connection.connect(test_db_conn_string)

    yield conn

    conn.close()


class TestMakeQueries:
    """Test cases for make_queries function."""

    def test_make_queries_update_single_predicate(self, test_db):
        """Test make_queries with single UPDATE operation."""
        predicate = PredicateData(
            col_name="city", comparison=_PredicateComparison.EQUAL, col_val="Old City"
        )

        row_data = UpdateData(
            col_name="city",
            col_val="New City",
            correction_type=_CorrectionType.SPELLING,
            description="Test spelling correction",
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

    def test_make_queries_update_multiple_predicates(self, test_db):
        """Test make_queries with multiple predicates on UPDATE operation."""
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

        row_data = UpdateData(
            col_name="city",
            col_val="New City",
            correction_type=_CorrectionType.SPELLING,
            description="Test spelling correction",
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

    def test_make_queries_delete_operation(self, test_db):
        """Test make_queries with DELETE operation."""
        predicate = PredicateData(
            col_name="city",
            comparison=_PredicateComparison.EQUAL,
            col_val="Dallas",
        )

        row_data = DeleteData(
            correction_type=_CorrectionType.ERRATUM,
            description="Test erratum deletion",
            predicate_cols=[predicate],
            tbl_name="geocoding",
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == """DELETE FROM "geocoding" WHERE "city" = 'Dallas'"""

    def test_make_queries_insert_plain(self, test_db):
        """Test make_queries with plain INSERT operation."""
        row_data = InsertData(
            correction_type=_CorrectionType.MISSING,
            description="Test plain insert",
            tbl_name="geocoding",
            col_vals={"city": "Campione", "country": "IT"},
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """INSERT INTO "geocoding" ("city", "country") VALUES ('Campione', 'IT')"""
        )

    def test_make_queries_insert_where_not_exists(self, test_db):
        """Test make_queries with INSERT ... WHERE NOT EXISTS."""
        row_data = InsertData(
            correction_type=_CorrectionType.MISSING,
            description="Test idempotent insert",
            tbl_name="geocoding",
            col_vals={"city": "Campione", "country": "IT"},
            existence_check=[
                PredicateData(
                    col_name="city",
                    comparison=_PredicateComparison.EQUAL,
                    col_val="Campione",
                ),
                PredicateData(
                    col_name="country",
                    comparison=_PredicateComparison.EQUAL,
                    col_val="IT",
                ),
            ],
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """INSERT INTO "geocoding" ("city", "country") SELECT 'Campione', 'IT'"""
            """ WHERE NOT EXISTS (SELECT 1 FROM "geocoding" WHERE """
            """"city" = 'Campione' AND "country" = 'IT')"""
        )

    def test_make_queries_insert_upsert_do_nothing(self, test_db):
        """Test make_queries with INSERT ... ON CONFLICT DO NOTHING."""
        row_data = InsertData(
            correction_type=_CorrectionType.MISSING,
            description="Test upsert do nothing",
            tbl_name="geocoding",
            col_vals={"city": "Campione", "country": "IT"},
            conflict_cols=["city", "country"],
            conflict_action=_ConflictAction.NOTHING,
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """INSERT INTO "geocoding" ("city", "country") VALUES ('Campione', 'IT') """
            """ON CONFLICT ("city", "country") DO NOTHING"""
        )

    def test_make_queries_insert_upsert_do_update(self, test_db):
        """Test make_queries with INSERT ... ON CONFLICT DO UPDATE SET."""
        row_data = InsertData(
            correction_type=_CorrectionType.MISSING,
            description="Test upsert do update",
            tbl_name="geocoding",
            col_vals={"city": "Campione", "country": "IT", "population": 2000},
            conflict_cols=["city", "country"],
            conflict_action=_ConflictAction.UPDATE,
        )

        queries = make_queries([row_data])

        assert len(queries) == 1
        assert isinstance(queries[0], psycopg.sql.Composed)

        query_str = queries[0].as_string(test_db)
        assert query_str == (
            """INSERT INTO "geocoding" ("city", "country", "population") """
            """VALUES ('Campione', 'IT', 2000) """
            """ON CONFLICT ("city", "country") DO UPDATE SET """
            """"population" = EXCLUDED."population\""""
        )
