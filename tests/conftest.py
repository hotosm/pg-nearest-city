"""Test fixtures."""

import pytest

from pg_nearest_city import DbConfig


@pytest.fixture()
def test_db_conn_string():
    """Get the database connection string for the test db."""
    # Use connection params from env
    return DbConfig().get_connection_string()
