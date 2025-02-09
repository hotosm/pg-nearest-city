"""Test async geocoder initialization and data file loading."""

import os

import pytest
import pytest_asyncio
import psycopg

from pg_nearest_city import AsyncNearestCity, Location, DbConfig


# NOTE we define the fixture here and not in conftest.py to allow
# async --> sync conversion to take place
@pytest_asyncio.fixture()
async def test_db(test_db_conn_string):
    """Provide a clean database connection for each test."""
    # Create a single connection for the test
    conn = await psycopg.AsyncConnection.connect(test_db_conn_string)

    # Clean up any existing state
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
    await conn.commit()

    yield conn

    await conn.close()


async def test_db_conn_missng_vars():
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


async def test_db_conn_vars_from_env():
    """Check db connection variables are passed through."""
    db_conf = DbConfig()
    assert db_conf.host == os.getenv("PGNEAREST_DB_HOST")
    assert db_conf.user == os.getenv("PGNEAREST_DB_USER")
    assert db_conf.password == os.getenv("PGNEAREST_DB_PASSWORD")
    assert db_conf.dbname == os.getenv("PGNEAREST_DB_NAME")
    assert db_conf.port == 5432


async def test_full_initialization_query():
    """Test completet database initialization and basic query through connect method."""
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


async def test_init_without_context_manager():
    """Should raise an error if not used in with block."""
    with pytest.raises(RuntimeError):
        geocoder = AsyncNearestCity()
        await geocoder.query(40.7128, -74.0060)


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
    async with AsyncNearestCity(test_db) as geocoder:
        await geocoder.initialize()

    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_complete_voronoi
    assert status.has_data


async def test_init_db_at_startup_then_query(test_db):
    """Web servers have a startup lifecycle that could do the initialisation."""
    async with AsyncNearestCity(test_db) as geocoder:
        pass  # do nothing, initialisation is complete here

    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


async def test_invalid_coordinates(test_db):
    """Test that invalid coordinates are properly handled."""
    async with AsyncNearestCity(test_db) as geocoder:
        await geocoder.initialize()

        with pytest.raises(ValueError):
            await geocoder.query(91, 0)  # Invalid latitude

        with pytest.raises(ValueError):
            await geocoder.query(0, 181)  # Invalid longitude
