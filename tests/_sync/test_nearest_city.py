"""Test async geocoder initialization and data file loading."""

import os

import pytest

import psycopg

from pg_nearest_city import NearestCity, Location, DbConfig


# NOTE we define the fixture here and not in conftest.py to allow
# async --> sync conversion to take place
@pytest.fixture()
def test_db(test_db_conn_string):
    """Provide a clean database connection for each test."""
    # Create a single connection for the test
    conn = psycopg.Connection.connect(test_db_conn_string)

    # Clean up any existing state
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
    conn.commit()

    yield conn

    conn.close()


def test_db_conn_missng_vars():
    """Check db connection error raised on missing vars."""
    os.environ["PGNEAREST_DB_USER"] = ""
    os.environ["PGNEAREST_DB_PASSWORD"] = ""

    with pytest.raises(ValueError):
        DbConfig()


def test_db_conn_vars_from_env():
    """Check db connection variables are passed through."""
    db_conf = DbConfig()
    assert db_conf.host == os.getenv("PGNEAREST_DB_HOST")
    assert db_conf.user == os.getenv("PGNEAREST_DB_USER")
    assert db_conf.password == os.getenv("PGNEAREST_DB_PASSWORD")
    assert db_conf.dbname == os.getenv("PGNEAREST_DB_NAME")
    assert db_conf.port == 5432


def test_full_initialization_query():
    """Test completet database initialization and basic query through connect method."""
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


def test_check_initialization_fresh_database(test_db):
    """Test initialization check on a fresh database with no tables."""
    geocoder = NearestCity(test_db)

    with test_db.cursor() as cur:
        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert not status.has_table


def test_check_initialization_incomplete_table(test_db):
    """Test initialization check with a table that's missing columns."""
    geocoder = NearestCity(test_db)

    with test_db.cursor() as cur:
        cur.execute("""
            CREATE TABLE pg_nearest_city_geocoding (
                city varchar,
                country varchar
            );
        """)
        test_db.commit()

        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert not status.has_valid_structure


def test_check_initialization_empty_table(test_db):
    """Test initialization check with properly structured but empty table."""
    geocoder = NearestCity(test_db)

    with test_db.cursor() as cur:
        geocoder._create_geocoding_table(cur)
        test_db.commit()

        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert status.has_valid_structure
    assert not status.has_data


def test_check_initialization_missing_voronoi(test_db):
    """Test initialization check when Voronoi polygons are missing."""
    geocoder = NearestCity(test_db)

    with test_db.cursor() as cur:
        geocoder._create_geocoding_table(cur)
        geocoder._import_cities(cur)
        test_db.commit()

        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert not status.has_complete_voronoi


def test_check_initialization_missing_index(test_db):
    """Test initialization check when spatial index is missing."""
    geocoder = NearestCity(test_db)

    with test_db.cursor() as cur:
        geocoder._create_geocoding_table(cur)
        geocoder._import_cities(cur)
        geocoder._import_voronoi_polygons(cur)
        test_db.commit()

        status = geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert status.has_complete_voronoi
    assert not status.has_spatial_index


def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    with NearestCity(test_db) as geocoder:
        geocoder.initialize()

    with test_db.cursor() as cur:
        status = geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_complete_voronoi
    assert status.has_data


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
        geocoder.initialize()

        with pytest.raises(ValueError):
            geocoder.query(91, 0)  # Invalid latitude

        with pytest.raises(ValueError):
            geocoder.query(0, 181)  # Invalid longitude
