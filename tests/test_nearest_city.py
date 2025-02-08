"""Test async geocoder initialization and data file loading."""

import os

import psycopg
import pytest
import pytest

from pg_nearest_city.nearest_city import NearestCity
from pg_nearest_city.base_nearest_city import DbConfig, Location


def get_test_config():
    """Get database configuration from environment variables or defaults."""
    return DbConfig(
        dbname=os.getenv("PGNEAREST_TEST_DB", "cities"),
        user=os.getenv("PGNEAREST_TEST_USER", "cities"),
        password=os.getenv("PGNEAREST_TEST_PASSWORD", "dummycipassword"),
        host=os.getenv("PGNEAREST_TEST_HOST", "db"),
        port=int(os.getenv("PGNEAREST_TEST_PORT", "5432")),
    )


@pytest.fixture()
def test_db():
    """Provide a clean database connection for each test."""
    config = get_test_config()

    # Create a single connection for the test
    conn = psycopg.Connection.connect(config.get_connection_string())

    # Clean up any existing state
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
    conn.commit()

    yield conn

    conn.close()


def test_full_initialization_connect():
    """Test completet database initialization and basic query through connect method."""
    with NearestCity.connect(get_test_config()) as geocoder:
        location = geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


def test_full_initialization(test_db):
    """Test complete database initialization and basic query."""
    geocoder = NearestCity(test_db)
    geocoder.initialize()

    # Test with New York coordinates
    location = geocoder.query(40.7128, -74.0060)
    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


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
    geocoder = NearestCity(test_db)
    geocoder.initialize()

    with test_db.cursor() as cur:
        status = geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_complete_voronoi
    assert status.has_data


def test_invalid_coordinates(test_db):
    """Test that invalid coordinates are properly handled."""
    geocoder = NearestCity(test_db)
    geocoder.initialize()

    with pytest.raises(ValueError):
        geocoder.query(91, 0)  # Invalid latitude

    with pytest.raises(ValueError):
        geocoder.query(0, 181)  # Invalid longitude
