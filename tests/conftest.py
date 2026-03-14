"""Test fixtures."""

import psycopg
import pytest

from pg_nearest_city import DbConfig


@pytest.fixture()
def test_db_conn_string():
    """Get the database connection string for the test db."""
    # Use connection params from env
    return DbConfig().get_connection_string()


@pytest.fixture()
def fresh_db_conn_string():
    """Create a temporary empty database and return its connection string.

    The database is dropped after the test completes.
    """
    db_config = DbConfig()
    fresh_name = "pg_nearest_city_test_fresh"
    admin_conn_string = db_config.get_connection_string()

    conn = psycopg.Connection.connect(admin_conn_string, autocommit=True)
    conn.execute(f"DROP DATABASE IF EXISTS {fresh_name}")
    conn.execute(f"CREATE DATABASE {fresh_name}")
    conn.close()

    fresh_config = DbConfig(dbname=fresh_name)
    yield fresh_config.get_connection_string()

    conn = psycopg.Connection.connect(admin_conn_string, autocommit=True)
    conn.execute(f"DROP DATABASE IF EXISTS {fresh_name}")
    conn.close()
