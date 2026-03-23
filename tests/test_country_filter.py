"""Tests for single-country database loads."""

from random import getrandbits

import psycopg
import pytest

from pg_nearest_city import DbConfig, Location, NearestCity
from pg_nearest_city.base_nearest_city import geo_test_cases
from pg_nearest_city.datasets.sources import (
    get_required_countries,
)
from pg_nearest_city.db.queries import create_database, drop_database

_ITALY_CASES = [c for c in geo_test_cases if c.expected_country == "IT"]
_NON_ITALY_CASES = [c for c in geo_test_cases if c.expected_country != "IT"]


@pytest.fixture(scope="module")
def italy_db():
    """Bootstrap a fresh DB loaded with only Italy and its enclaves."""
    db_config = DbConfig()
    db_name = f"pg_nearest_city_test_italy_{getrandbits(32)}"
    admin_conn_str = db_config.get_connection_string()

    conn = psycopg.Connection.connect(admin_conn_str, autocommit=True)
    conn.execute(drop_database(db_name=db_name))
    conn.execute(create_database(db_name=db_name))
    conn.close()

    italy_config = DbConfig(dbname=db_name)

    # Bootstrap from bundled data (loads everything).
    # Entering the context manager triggers initialize(), which auto-imports
    # from the bundled CSV files into the empty database.
    with NearestCity(italy_config):
        pass

    # Keep only Italy + enclave dependencies (VA, SM)
    required_a3 = get_required_countries("ITA")
    # Map alpha-3 to alpha-2 for filtering (bundled data uses alpha-2)
    # ITA→IT, VAT→VA, SMR→SM
    a3_to_a2 = {"ITA": "IT", "VAT": "VA", "SMR": "SM"}
    required_a2 = {a3_to_a2.get(a3, a3) for a3 in required_a3}

    conn = psycopg.Connection.connect(italy_config.get_connection_string())
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM geocoding WHERE country != ALL(%s)", (list(required_a2),)
        )
        cur.execute("DELETE FROM country WHERE alpha2 != ALL(%s)", (list(required_a2),))
    conn.commit()
    conn.close()

    yield italy_config

    # Teardown
    conn = psycopg.Connection.connect(admin_conn_str, autocommit=True)
    conn.execute(drop_database(db_name=db_name))
    conn.close()


class TestEnclaveDependencies:
    """Verify the enclave dependency graph resolution."""

    def test_italy_includes_vatican_and_san_marino(self):
        result = get_required_countries("ITA")
        assert result == {"ITA", "VAT", "SMR"}

    def test_switzerland_transitive(self):
        result = get_required_countries("CHE")
        assert "CHE" in result
        assert "ITA" in result  # Campione
        assert "DEU" in result  # Büsingen
        assert "VAT" in result  # transitive via ITA
        assert "SMR" in result  # transitive via ITA

    def test_bidirectional_nl_be(self):
        result_nl = get_required_countries("NLD")
        result_be = get_required_countries("BEL")
        assert result_nl == {"NLD", "BEL"}
        assert result_be == {"BEL", "NLD"}

    def test_no_dependencies(self):
        result = get_required_countries("JPN")
        assert result == {"JPN"}


@pytest.mark.integration
class TestSingleCountryLoad:
    """Verify behavior when only Italy (+ enclaves) is loaded."""

    def test_only_expected_countries_loaded(self, italy_db):
        """Verify the fixture filtered to Italy + its enclaves only."""
        conn = psycopg.Connection.connect(italy_db.get_connection_string())
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT alpha2 FROM country")
            countries = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT DISTINCT country FROM geocoding")
            geocoding_countries = {row[0] for row in cur.fetchall()}
        conn.close()
        assert countries == {"IT", "VA", "SM"}
        assert geocoding_countries == {"IT", "VA", "SM"}

    @pytest.mark.parametrize("case", _ITALY_CASES)
    def test_italy_cases_resolve(self, case, italy_db):
        """Points in Italy should resolve to the expected city."""
        with NearestCity(italy_db) as geocoder:
            location = geocoder.query(lon=case.lon, lat=case.lat)
        assert location is not None
        assert isinstance(location, Location)
        assert location.city == case.expected_city
        assert location.country == "IT"

    @pytest.mark.parametrize("case", _NON_ITALY_CASES)
    def test_non_italy_cases_no_false_positive(self, case, italy_db):
        """Non-Italian points should not return their expected non-Italian result.

        Points far from Italy return None (no country polygon covers them).
        Points in enclaves (Vatican, San Marino) should resolve to those
        countries since their boundaries are loaded as enclave dependencies.
        Points near Italy's border may fall within Italy's polygon due to
        boundary imprecision — that's acceptable for a filtered load.
        """
        with NearestCity(italy_db) as geocoder:
            location = geocoder.query(lon=case.lon, lat=case.lat)
        if location is not None:
            # Must be one of the loaded countries
            assert location.country in {"IT", "VA", "SM"}
