"""Test geocoder initialization and data file loading."""

import os
import psycopg
import pytest
from pg_nearest_city.nearest_city import NearestCity, DbConfig


def get_test_config():
    """Get database configuration from environment variables or defaults."""
    return DbConfig(
        dbname=os.getenv("PGNEAREST_TEST_DB", "gisdb"),
        user=os.getenv("PGNEAREST_TEST_USER", "postgres"),
        password=os.getenv("PGNEAREST_TEST_PASSWORD", "postgres"),
        host=os.getenv("PGNEAREST_TEST_HOST", "localhost"),
        port=int(os.getenv("PGNEAREST_TEST_PORT", "5432")),
    )


def test_full_initialization():
    """Test full database initialization process."""

    config = get_test_config()
    geocoder = NearestCity(config)
    geocoder.initialize()

    location = geocoder.query(40.7128, -74.0060)
    assert location is not None
    assert location.city == "New York City"

    geocoder.close()


def test_external_connection():
    """Test using an external connection."""
    with psycopg.connect(get_test_config().get_connection_string()) as external_conn:
        geocoder = NearestCity(external_conn)
        location = geocoder.query(40.7128, -74.0060)
        assert location is not None


@pytest.fixture
def test_db():
    """Provide a clean database for each test."""
    config = get_test_config()

    # Connect and clean up any existing state
    with psycopg.connect(config.get_connection_string()) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
        conn.commit()

    return config


def test_check_initialization_fresh_database(test_db):
    """Test initialization check on a fresh database with no tables."""
    geocoder = NearestCity(test_db)
    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == False
    assert "Table does not exist" in status["details"]


def test_check_initialization_incomplete_table(test_db):
    """Test initialization check with a table that's missing columns."""
    geocoder = NearestCity(test_db)

    # Create table with missing columns
    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE pg_nearest_city_geocoding (
                    city varchar,
                    country varchar
                );
            """)
            conn.commit()

            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == True
    assert "Table structure is invalid" in status["details"]


def test_check_initialization_empty_table(test_db):
    """Test initialization check with properly structured but empty table."""
    geocoder = NearestCity(test_db)

    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            geocoder._create_geocoding_table(cur)
            conn.commit()

            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == False
    assert "No city data present" in status["details"]


def test_check_initialization_wrong_city_count(test_db):
    """Test initialization check with wrong number of cities."""
    geocoder = NearestCity(test_db)

    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            # Create table and insert just one test city
            geocoder._create_geocoding_table(cur)
            cur.execute("""
                INSERT INTO pg_nearest_city_geocoding (city, country, lat, lon)
                VALUES ('Test City', 'Test Country', 0, 0);
            """)
            conn.commit()

            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == True
    assert "City count mismatch" in status["details"]
    assert str(geocoder.EXPECTED_CITY_COUNT) in status["details"]


def test_check_initialization_missing_voronoi(test_db):
    """Test initialization check when Voronoi polygons are missing."""
    geocoder = NearestCity(test_db)

    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            # Set up table and import cities only
            geocoder._create_geocoding_table(cur)
            geocoder._import_cities(cur)
            conn.commit()

            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == True
    assert "Missing Voronoi polygons" in status["details"]


def test_check_initialization_missing_index(test_db):
    """Test initialization check when spatial index is missing."""
    geocoder = NearestCity(test_db)

    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            # Do full initialization except for the spatial index
            geocoder._create_geocoding_table(cur)
            geocoder._import_cities(cur)
            geocoder._import_voronoi_polygons(cur)
            conn.commit()

            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == False
    assert status["needs_repair"] == True
    assert "Missing spatial index" in status["details"]


def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    geocoder = NearestCity(test_db)

    # Perform full initialization
    geocoder.initialize()

    with psycopg.connect(test_db.get_connection_string()) as conn:
        with conn.cursor() as cur:
            status = geocoder._check_initialization_status(cur)

    assert status["is_initialized"] == True
    assert status["needs_repair"] == False
    assert "Database properly initialized" in status["details"]


def test_check_initialization_with_external_connection(test_db):
    """Test initialization check using an external connection."""
    with psycopg.connect(test_db.get_connection_string()) as conn:
        geocoder = NearestCity(conn)

        with conn.cursor() as cur:
            status = geocoder._check_initialization_status(cur)

        assert status["is_initialized"] == False
        assert status["needs_repair"] == False
        assert "Table does not exist" in status["details"]

        # Now initialize and check again
        geocoder.initialize()

        with conn.cursor() as cur:
            status = geocoder._check_initialization_status(cur)

        assert status["is_initialized"] == True
        assert status["needs_repair"] == False
        assert "Database properly initialized" in status["details"]
