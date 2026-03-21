"""Test async geocoder initialization and querying."""

import os

import psycopg
import pytest
import pytest_asyncio

from pg_nearest_city import AsyncNearestCity, DbConfig, Location, geo_test_cases


# NOTE we define the fixture here and not in conftest.py to allow
# async --> sync conversion to take place
@pytest_asyncio.fixture()
async def test_db(test_db_conn_string):
    """Provide a database connection for each test."""
    conn = await psycopg.AsyncConnection.connect(test_db_conn_string)

    yield conn

    await conn.close()


@pytest_asyncio.fixture()
async def fresh_db(fresh_db_conn_string):
    """Provide a connection to a temporary empty database."""
    conn = await psycopg.AsyncConnection.connect(fresh_db_conn_string)

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
    assert db_conf.host == os.getenv("PGNEAREST_DB_HOST", "localhost")
    assert db_conf.user == os.getenv("PGNEAREST_DB_USER")
    assert db_conf.password == os.getenv("PGNEAREST_DB_PASSWORD")
    assert db_conf.dbname == os.getenv("PGNEAREST_DB_NAME", "postgres")
    assert db_conf.port == 5432


@pytest.mark.integration
async def test_full_initialization_query():
    """Test database initialization and basic query."""
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(-74.0060, 40.7128)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


async def test_init_without_context_manager():
    """Should raise an error if not used in with block."""
    with pytest.raises(RuntimeError):
        geocoder = AsyncNearestCity()
        await geocoder.query(-74.0060, 40.7128)


@pytest.mark.integration
async def test_check_initialization_fresh_database(fresh_db):
    """Test initialization check on a fresh database with no tables."""
    geocoder = AsyncNearestCity(fresh_db)

    async with fresh_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert not status.has_country_table or not status.has_geocoding_table


@pytest.mark.integration
async def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    async with AsyncNearestCity(test_db) as geocoder:
        pass

    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_geocoding_data
    assert status.has_country_data


@pytest.mark.integration
async def test_init_db_at_startup_then_query(test_db):
    """Web servers have a startup lifecycle that could do the initialisation."""
    async with AsyncNearestCity(test_db) as geocoder:
        pass  # do nothing, initialisation is complete here

    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(-74.0060, 40.7128)

    assert location is not None
    assert location.city == "New York City"
    assert isinstance(location, Location)


@pytest.mark.integration
async def test_invalid_coordinates(test_db):
    """Test that invalid coordinates are properly handled."""
    async with AsyncNearestCity(test_db) as geocoder:
        with pytest.raises(ValueError):
            await geocoder.query(0, 91)  # Invalid latitude

        with pytest.raises(ValueError):
            await geocoder.query(181, 0)  # Invalid longitude


@pytest.mark.integration
@pytest.mark.parametrize("case", geo_test_cases)
async def test_cities_close_country_boundaries(case):
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(lon=case.lon, lat=case.lat)
        assert isinstance(location, Location)
        assert location.city == case.expected_city
        assert location.country == case.expected_country
