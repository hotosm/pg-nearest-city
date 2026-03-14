"""Test sync geocoder initialization and querying."""

import os

import psycopg
import pytest

from pg_nearest_city import DbConfig, Location, NearestCity, geo_test_cases


# NOTE we define the fixture here and not in conftest.py to allow
# async --> sync conversion to take place
@pytest.fixture()
def test_db(test_db_conn_string):
    """Provide a database connection for each test."""
    conn = psycopg.Connection.connect(test_db_conn_string)

    yield conn

    conn.close()


@pytest.fixture()
def fresh_db(fresh_db_conn_string):
    """Provide a connection to a temporary empty database."""
    conn = psycopg.Connection.connect(fresh_db_conn_string)

    yield conn

    conn.close()


def test_db_conn_missng_vars():
    """Check db connection error raised on missing vars."""
    original_user = os.getenv("PGNEAREST_DB_USER")
    original_pass = os.getenv("PGNEAREST_DB_PASSWORD")

    os.environ["PGNEAREST_DB_USER"] = ""
    os.environ["PGNEAREST_DB_PASSWORD"] = ""

    with pytest.raises(ValueError):
        DbConfig()

    # Re-set env vars, so following tests dont fail
    os.environ["PGNEAREST_DB_USER"] = original_user or ""
    os.environ["PGNEAREST_DB_PASSWORD"] = original_pass or ""


def test_db_conn_vars_from_env():
    """Check db connection variables are passed through."""
    db_conf = DbConfig()
    assert db_conf.host == os.getenv("PGNEAREST_DB_HOST", "localhost")
    assert db_conf.user == os.getenv("PGNEAREST_DB_USER")
    assert db_conf.password == os.getenv("PGNEAREST_DB_PASSWORD")
    assert db_conf.dbname == os.getenv("PGNEAREST_DB_NAME", "postgres")
    assert db_conf.port == 5432


def test_full_initialization_query():
    """Test database initialization and basic query."""
    with NearestCity() as geocoder:
        location = geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


def test_init_without_context_manager():
    """Should raise an error if not used in with block."""
    with pytest.raises(RuntimeError):
        geocoder = NearestCity()
        geocoder.query(40.7128, -74.0060)


def test_check_initialization_fresh_database(fresh_db):
    """Test initialization check on a fresh database with no tables."""
    geocoder = NearestCity(fresh_db)

    with fresh_db.cursor() as cur:
        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert not status.has_country_table or not status.has_geocoding_table


def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    with NearestCity(test_db) as geocoder:
        pass

    with test_db.cursor() as cur:
        status = geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_geocoding_data
    assert status.has_country_data


def test_init_db_at_startup_then_query(test_db):
    """Web servers have a startup lifecycle that could do the initialisation."""
    with NearestCity(test_db) as geocoder:
        pass  # do nothing, initialisation is complete here

    with NearestCity() as geocoder:
        location = geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


def test_invalid_coordinates(test_db):
    """Test that invalid coordinates are properly handled."""
    with NearestCity(test_db) as geocoder:
        with pytest.raises(ValueError):
            geocoder.query(91, 0)  # Invalid latitude

        with pytest.raises(ValueError):
            geocoder.query(0, 181)  # Invalid longitude


@pytest.mark.parametrize("case", geo_test_cases)
def test_cities_close_country_boundaries(case):
    with NearestCity() as geocoder:
        location = geocoder.query(lon=case.lon, lat=case.lat)
        assert location is not None
        assert isinstance(location, Location)
        assert location.city == case.expected_city
        assert location.country == case.expected_country
