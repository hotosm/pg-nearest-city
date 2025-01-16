"""Test geocoder initialization and data file loading."""

import os
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


def test_geocoder_initialization():
    """Test that geocoder can initialize and find its data files."""
    config = get_test_config()
    geocoder = ReverseGeocoder(config)

    assert geocoder.cities_file.exists(), "Cities file should exist"
    assert geocoder.voronoi_file.exists(), "Voronoi file should exist"
