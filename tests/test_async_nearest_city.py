"""Test async geocoder initialization and data file loading."""

import os

import psycopg
import pytest
import pytest_asyncio

from pg_nearest_city.async_nearest_city import AsyncNearestCity
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


@pytest_asyncio.fixture()
async def test_db():
    """Provide a clean database connection for each test."""
    config = get_test_config()

    # Create a single connection for the test
    conn = await psycopg.AsyncConnection.connect(config.get_connection_string())

    # Clean up any existing state
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
    await conn.commit()

    yield conn

    await conn.close()


async def test_full_initialization_connect():
    """Test completet database initialization and basic query through connect method."""
    async with AsyncNearestCity.connect(get_test_config()) as geocoder:
        location = await geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


async def test_full_initialization(test_db):
    """Test complete database initialization and basic query."""
    geocoder = AsyncNearestCity(test_db)
    await geocoder.initialize()

    # Test with New York coordinates
    location = await geocoder.query(40.7128, -74.0060)
    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


async def test_check_initialization_fresh_database(test_db):
    """Test initialization check on a fresh database with no tables."""
    geocoder = AsyncNearestCity(test_db)
    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert not status.has_table


async def test_check_initialization_incomplete_table(test_db):
    """Test initialization check with a table that's missing columns."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await cur.execute("""
            CREATE TABLE pg_nearest_city_geocoding (
                city varchar,
                country varchar
            );
        """)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert not status.has_valid_structure


async def test_check_initialization_empty_table(test_db):
    """Test initialization check with properly structured but empty table."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._create_geocoding_table(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert status.has_valid_structure
    assert not status.has_data


async def test_check_initialization_missing_voronoi(test_db):
    """Test initialization check when Voronoi polygons are missing."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._create_geocoding_table(cur)
        await geocoder._import_cities(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert not status.has_complete_voronoi


async def test_check_initialization_missing_index(test_db):
    """Test initialization check when spatial index is missing."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._create_geocoding_table(cur)
        await geocoder._import_cities(cur)
        await geocoder._import_voronoi_polygons(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert status.has_complete_voronoi
    assert not status.has_spatial_index


async def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    geocoder = AsyncNearestCity(test_db)
    await geocoder.initialize()

    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_complete_voronoi
    assert status.has_data


async def test_invalid_coordinates(test_db):
    """Test that invalid coordinates are properly handled."""
    geocoder = AsyncNearestCity(test_db)
    await geocoder.initialize()

    with pytest.raises(ValueError):
        await geocoder.query(91, 0)  # Invalid latitude

    with pytest.raises(ValueError):
        await geocoder.query(0, 181)  # Invalid longitude
