"""Test async geocoder initialization and data file loading."""

import os

import psycopg
import pytest
import pytest_asyncio
from pg_nearest_city import AsyncNearestCity, DbConfig, Location, geo_test_cases


# NOTE we define the fixture here and not in conftest.py to allow
# async --> sync conversion to take place
@pytest_asyncio.fixture()
async def test_db(test_db_conn_string):
    """Provide a clean database connection for each test.

    Drops the geocoding table and clears country boundaries so each test
    starts from a known state.  The country metadata rows are left in place
    since they come from the bundled ISO CSV and are idempotent.
    """
    conn = await psycopg.AsyncConnection.connect(test_db_conn_string)

    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS geocoding CASCADE;")
        await cur.execute("UPDATE country SET geom = NULL")
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
    """Test complete database initialization and basic query through connect method."""
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(40.7128, -74.0060)

    assert location is not None
    assert location.city == "New York City"
    assert location.country == "US"
    assert location.country_alpha3 == "USA"
    assert location.country_name == "United States of America"
    assert isinstance(location, Location)


async def test_init_without_context_manager():
    """Should raise an error if not used in with block."""
    with pytest.raises(RuntimeError):
        geocoder = AsyncNearestCity()
        await geocoder.query(40.7128, -74.0060)


async def test_check_initialization_fresh_database(test_db):
    """Test initialization check on a database with no geocoding table."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert not status.has_table


async def test_check_initialization_incomplete_table(test_db):
    """Test initialization check with a table that's missing columns."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE geocoding (
                city text,
                country char(2)
            );
        """
        )
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert not status.has_valid_structure


async def test_check_initialization_empty_table(test_db):
    """Test initialization check with properly structured but empty table."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._setup_tables(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_table
    assert status.has_valid_structure
    assert not status.has_data


async def test_check_initialization_missing_country_boundaries(test_db):
    """Test initialization check when country boundary geometries are missing."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._setup_extensions(cur)
        await geocoder._setup_tables(cur)
        await geocoder._import_country_metadata(cur)
        await geocoder._import_cities(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert not status.has_country_boundaries


async def test_check_initialization_missing_index(test_db):
    """Test initialization check when spatial index is missing."""
    geocoder = AsyncNearestCity(test_db)

    async with test_db.cursor() as cur:
        await geocoder._setup_extensions(cur)
        await geocoder._setup_tables(cur)
        await geocoder._import_country_metadata(cur)
        await geocoder._import_country_boundaries(cur)
        await geocoder._import_cities(cur)
        await test_db.commit()

        status = await geocoder._check_initialization_status(cur)

    assert not status.is_fully_initialized
    assert status.has_data
    assert status.has_country_boundaries
    assert not status.has_spatial_index


async def test_check_initialization_complete(test_db):
    """Test initialization check with a properly initialized database."""
    async with AsyncNearestCity(test_db) as geocoder:
        await geocoder.initialize()

    async with test_db.cursor() as cur:
        status = await geocoder._check_initialization_status(cur)

    assert status.is_fully_initialized
    assert status.has_spatial_index
    assert status.has_country_boundaries
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


@pytest.mark.parametrize("case", geo_test_cases)
async def test_cities_close_country_boundaries(case):
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(lon=case.lon, lat=case.lat)
        assert location is not None
        assert isinstance(location, Location)
        assert location.city == case.expected_city
        assert location.country == case.expected_country


async def test_offshore_border_point_returns_none():
    """Point near Sint Maarten coast should return no country/city if outside all borders."""
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(lon=-63.11852, lat=18.03783)
        assert location is None
