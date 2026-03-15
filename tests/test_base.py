"""Tests for pg_nearest_city.base_nearest_city."""

import pytest

from pg_nearest_city.base_nearest_city import (
    BaseNearestCity,
    InitializationStatus,
)


class TestInitializationStatus:
    def test_default_not_initialized(self):
        status = InitializationStatus()
        assert not status.is_fully_initialized

    def test_fully_initialized(self):
        status = InitializationStatus()
        status.has_country_table = True
        status.has_geocoding_table = True
        status.has_country_data = True
        status.has_geocoding_data = True
        status.has_spatial_index = True
        assert status.is_fully_initialized

    def test_missing_components_partial(self):
        status = InitializationStatus()
        status.has_country_table = True
        status.has_geocoding_table = True
        missing = status.get_missing_components()
        assert "country table" not in missing
        assert "geocoding table" not in missing
        assert len(missing) == 3


class TestFindDataFile:
    def test_finds_gz_file(self, tmp_path):
        f = tmp_path / "country.csv.gz"
        f.write_bytes(b"data")
        result = BaseNearestCity._find_data_file(tmp_path, "country.csv")
        assert result == f

    def test_prefers_zst_over_gz(self, tmp_path):
        """zst appears first in COMPRESSION_EXTENSIONS."""
        (tmp_path / "country.csv.zst").write_bytes(b"zst")
        (tmp_path / "country.csv.gz").write_bytes(b"gz")
        result = BaseNearestCity._find_data_file(tmp_path, "country.csv")
        assert result is not None
        assert result.suffix == ".zst"

    def test_returns_none_when_missing(self, tmp_path):
        result = BaseNearestCity._find_data_file(tmp_path, "country.csv")
        assert result is None


class TestFindDataPath:
    def test_finds_via_constructor_path(self, tmp_path):
        (tmp_path / "country.csv.gz").write_bytes(b"c")
        (tmp_path / "geocoding.csv.gz").write_bytes(b"g")
        result = BaseNearestCity._find_data_path(str(tmp_path))
        assert result == str(tmp_path)

    def test_finds_via_env_var(self, tmp_path, monkeypatch):
        (tmp_path / "country.csv.gz").write_bytes(b"c")
        (tmp_path / "geocoding.csv.gz").write_bytes(b"g")
        monkeypatch.setenv("PGNEAREST_DATA_PATH", str(tmp_path))
        result = BaseNearestCity._find_data_path()
        assert result == str(tmp_path)

    def test_returns_none_when_no_data(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PGNEAREST_DATA_PATH", str(tmp_path))
        monkeypatch.setattr(
            BaseNearestCity, "DATA_FALLBACK_PATH", str(tmp_path / "nope")
        )
        result = BaseNearestCity._find_data_path()
        assert result is None

    def test_constructor_path_takes_priority(self, tmp_path, monkeypatch):
        d1 = tmp_path / "first"
        d2 = tmp_path / "second"
        d1.mkdir()
        d2.mkdir()
        for d in (d1, d2):
            (d / "country.csv.gz").write_bytes(b"c")
            (d / "geocoding.csv.gz").write_bytes(b"g")
        monkeypatch.setenv("PGNEAREST_DATA_PATH", str(d2))
        result = BaseNearestCity._find_data_path(str(d1))
        assert result == str(d1)


class TestBootstrapSql:
    def test_bootstrap_sql_creates_extensions_and_tables(self):
        raw = BaseNearestCity._get_bootstrap_sql()
        stmts = [s.decode() for s in raw]
        assert any("postgis" in s.lower() for s in stmts)
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in stmts)
        # Country should be created before Geocoding
        create_stmts = [s for s in stmts if "CREATE TABLE" in s]
        country_idx = next(
            i for i, s in enumerate(create_stmts) if "country" in s.lower()
        )
        geocoding_idx = next(
            i for i, s in enumerate(create_stmts) if "geocoding" in s.lower()
        )
        assert country_idx < geocoding_idx

    def test_bootstrap_sql_drops_in_reverse_order(self):
        raw = BaseNearestCity._get_bootstrap_sql()
        stmts = [s.decode() for s in raw]
        drop_stmts = [s for s in stmts if "DROP TABLE" in s]
        # Geocoding (depends on Country) should be dropped first
        geocoding_idx = next(
            i for i, s in enumerate(drop_stmts) if "geocoding" in s.lower()
        )
        country_idx = next(
            i for i, s in enumerate(drop_stmts) if "country" in s.lower()
        )
        assert geocoding_idx < country_idx

    def test_bootstrap_index_sql(self):
        raw = BaseNearestCity._get_bootstrap_index_sql()
        stmts = [s.decode() for s in raw]
        assert len(stmts) >= 2
        assert all("CREATE INDEX" in s for s in stmts)


class TestSyncQueryOceanPoint:
    """Test that querying a point in the ocean returns None."""

    def test_ocean_point_returns_none(self):
        from pg_nearest_city import NearestCity

        with NearestCity() as geocoder:
            # Middle of the Pacific
            result = geocoder.query(lat=0.0, lon=-160.0)
        assert result is None


class TestAsyncQueryOceanPoint:
    """Test that querying a point in the ocean returns None (async)."""

    @pytest.mark.asyncio
    async def test_ocean_point_returns_none(self):
        from pg_nearest_city import AsyncNearestCity

        async with AsyncNearestCity() as geocoder:
            result = await geocoder.query(lat=0.0, lon=-160.0)
        assert result is None
