"""Test geocoder initialization and data file loading."""

import os
import psycopg
from pg_nearest_city.reverse_geocoder import ReverseGeocoder, DbConfig

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
    geocoder = ReverseGeocoder(config)
    geocoder.initialize()

    location = geocoder.reverse_geocode(40.7128, -74.0060)
    assert location is not None
    assert location.city == "New York City"

    geocoder.close()

def test_external_connection():
    """Test using an external connection."""
    with psycopg.connect(
        get_test_config().get_connection_string()
    ) as external_conn:
        geocoder = ReverseGeocoder(external_conn)
        location = geocoder.reverse_geocode(40.7128, -74.0060)
        assert location is not None

    
