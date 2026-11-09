"""Test async geocoder initialization and querying."""

import os
from unittest.mock import AsyncMock

import psycopg
import pytest
import pytest_asyncio

from pg_nearest_city import AsyncNearestCity, DbConfig, Location, geo_test_cases
from tests.fixtures import (
    BORDER_PROBE_IDS,
    BORDER_PROBES,
    BorderProbe,
    load_nominatim_countries,
)


# Nominatim reports sovereign states for some territories, so never 100%.
MIN_NOMINATIM_MATCH = 0.9

# Probes come from unsimplified boundaries, so seams can shift by this much.
BORDER_TOLERANCE_M = 100


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


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def border_geocoder():
    async with AsyncNearestCity() as geocoder:
        yield geocoder


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
    assert location.lat == pytest.approx(40.71, abs=0.1)
    assert location.lon == pytest.approx(-74.01, abs=0.1)
    assert isinstance(location, Location)
    assert location.country_alpha3 == "USA"
    assert location.country_name is not None


async def test_init_without_context_manager():
    """Should raise an error if not used in with block."""
    with pytest.raises(RuntimeError):
        geocoder = AsyncNearestCity()
        await geocoder.query(-74.0060, 40.7128)


async def test_owned_connection_closed_when_initialization_fails(monkeypatch):
    """An owned connection must not leak if __aenter__ fails."""
    conn = AsyncMock()
    monkeypatch.setattr(
        AsyncNearestCity, "get_connection", AsyncMock(return_value=conn)
    )
    monkeypatch.setattr(
        AsyncNearestCity, "initialize", AsyncMock(side_effect=RuntimeError)
    )

    with pytest.raises(RuntimeError):
        async with AsyncNearestCity():
            pass

    conn.close.assert_called_once()


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
    assert location.country_alpha3 == "USA"
    assert location.country_name is not None


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
async def test_cities_close_country_boundaries(case, loaded_countries):
    if loaded_countries is not None and case.expected_country not in loaded_countries:
        pytest.skip(f"{case.expected_country} not loaded")
    async with AsyncNearestCity() as geocoder:
        location = await geocoder.query(lon=case.lon, lat=case.lat)
        assert isinstance(location, Location)
        assert location.city == case.expected_city
        assert location.country == case.expected_country


@pytest.mark.integration
async def test_point_in_gap_between_countries_resolves(test_db):
    async with AsyncNearestCity(test_db) as geocoder:
        location = await geocoder.query(lon=6.9274, lat=46.0147)

    assert location is not None
    assert location.country in {"CH", "FR", "IT"}


@pytest.mark.integration
async def test_inland_city_on_long_polygon_edge_resolves(test_db):
    async with AsyncNearestCity(test_db) as geocoder:
        location = await geocoder.query(lon=-83.78906, lat=31.17991)

    assert location is not None
    assert location.city == "Moultrie"


@pytest.mark.integration
async def test_offshore_point_uses_nearest_polygon(test_db):
    async with AsyncNearestCity(test_db) as geocoder:
        location = await geocoder.query(lon=14.633234, lat=67.488647)

    assert location is not None
    assert location.country == "NO"


@pytest.mark.integration
async def test_enclave_wins_over_surrounding_country(test_db):
    async with AsyncNearestCity(test_db) as geocoder:
        location = await geocoder.query(lon=12.4534, lat=41.9029)

    assert location is not None
    assert location.country == "VA"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("lon", "lat", "country"),
    [
        (-66.1, 18.45, "PR"),
        (144.75, 13.47, "GU"),
        (114.17, 22.32, "HK"),
        (-63.24971, 17.62652, "BQ"),
        (-6.80501, 61.77103, "FO"),
        (-8.71, 70.93, "SJ"),
    ],
)
async def test_territory_coast_is_not_assigned_to_parent(test_db, lon, lat, country):
    async with AsyncNearestCity(test_db) as geocoder:
        location = await geocoder.query(lon=lon, lat=lat)

    assert location is not None
    assert location.country == country


@pytest.mark.integration
async def test_countries_agree_with_nominatim(test_db):
    points = load_nominatim_countries()
    async with AsyncNearestCity(test_db) as geocoder:
        locations = [await geocoder.query(lon, lat) for lon, lat, _ in points]

    matches = [
        location is not None and location.country == country
        for location, (_, _, country) in zip(locations, points, strict=True)
    ]
    assert sum(matches) / len(matches) >= MIN_NOMINATIM_MATCH


@pytest.mark.integration
@pytest.mark.parametrize("probe", BORDER_PROBES, ids=BORDER_PROBE_IDS)
async def test_border_fixture_country_assignment(
    probe: BorderProbe, loaded_countries, border_geocoder
):
    if loaded_countries is not None and probe.expected_alpha2 not in loaded_countries:
        pytest.skip(f"{probe.expected_alpha2} not loaded")
    location = await border_geocoder.query(lon=probe.probe_lon, lat=probe.probe_lat)
    assert location is not None
    assert isinstance(location, Location)
    if location.country == probe.expected_alpha2:
        return
    assert location.country in probe.pair.split("/")
    async with border_geocoder.connection.cursor() as cur:
        await cur.execute(
            """
            SELECT 1 FROM country
            WHERE alpha2 = %s AND ST_DWithin(
              geom::geography,
              ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
              %s
            )
            """,
            (
                probe.expected_alpha2,
                probe.probe_lon,
                probe.probe_lat,
                BORDER_TOLERANCE_M,
            ),
        )
        assert await cur.fetchone() is not None
