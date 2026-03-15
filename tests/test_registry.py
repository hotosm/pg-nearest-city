"""Tests for pg_nearest_city.datasets.registry."""

from pg_nearest_city.datasets.registry import DatasetEntry, DatasetRegistry, GeoSource


class TestDatasetEntry:
    def test_from_dict(self):
        d = {
            "filehash": "abc123",
            "filepath": "/tmp/test.zip",
            "geosource": "GEONAMES",
            "url": "https://example.com/test.zip",
        }
        entry = DatasetEntry.from_dict(d)
        assert entry.filehash == "abc123"
        assert entry.geosource == GeoSource.GEONAMES


class TestDatasetRegistry:
    def test_in_memory_registry(self):
        reg = DatasetRegistry(persist=False)
        reg.register(
            geosource=GeoSource.GEONAMES,
            filename="cities500.zip",
            filehash="deadbeef",
            filepath="/tmp/cities500.zip",
            url="https://example.com/cities500.zip",
        )
        assert reg.get("cities500.zip") is not None
        assert reg.get("nonexistent") is None

    def test_get_by_source(self):
        reg = DatasetRegistry(persist=False)
        reg.register(
            geosource=GeoSource.GEONAMES,
            filename="a.zip",
            filehash="h1",
            filepath="/tmp/a.zip",
            url="https://example.com/a.zip",
        )
        reg.register(
            geosource=GeoSource.GADM,
            filename="b.zip",
            filehash="h2",
            filepath="/tmp/b.zip",
            url="https://example.com/b.zip",
        )
        assert reg.get_by_source(GeoSource.GEONAMES) == ["a.zip"]
        assert reg.get_by_source(GeoSource.GADM) == ["b.zip"]
        assert reg.get_by_source(GeoSource.NATURAL_EARTH) == []

    def test_list_all(self):
        reg = DatasetRegistry(persist=False)
        reg.register(
            geosource=GeoSource.GEONAMES,
            filename="x.zip",
            filehash="h",
            filepath="x.zip",
            url="u",
        )
        entries = reg.list_all()
        assert len(entries) == 1
        assert entries[0][0] == "x.zip"

    def test_is_cached_false_when_file_missing(self, tmp_path):
        reg = DatasetRegistry(persist=False)
        reg.register(
            geosource=GeoSource.GEONAMES,
            filename="missing.zip",
            filehash="h",
            filepath="missing.zip",
            url="u",
        )
        assert reg.is_cached("missing.zip", tmp_path) is False

    def test_is_cached_true_when_file_exists(self, tmp_path):
        reg = DatasetRegistry(persist=False)
        reg.register(
            geosource=GeoSource.GEONAMES,
            filename="found.zip",
            filehash="h",
            filepath="found.zip",
            url="u",
        )
        (tmp_path / "found.zip").write_bytes(b"data")
        assert reg.is_cached("found.zip", tmp_path) is True

    def test_get_cached_path_none_for_unknown(self, tmp_path):
        reg = DatasetRegistry(persist=False)
        assert reg.get_cached_path("nope", tmp_path) is None

    def test_persistent_save_load(self, tmp_path):
        reg_path = tmp_path / "registry.json"
        reg = DatasetRegistry(registry_path=reg_path, persist=True)
        reg.register(
            geosource=GeoSource.GADM,
            filename="gadm.zip",
            filehash="abc",
            filepath="gadm.zip",
            url="https://example.com/gadm.zip",
        )
        assert reg_path.exists()

        # Load from file
        reg2 = DatasetRegistry(registry_path=reg_path, persist=True)
        entry = reg2.get("gadm.zip")
        assert entry is not None
        assert entry.filehash == "abc"

    def test_load_missing_file(self, tmp_path):
        reg = DatasetRegistry(registry_path=tmp_path / "nofile.json", persist=True)
        assert len(reg.entries) == 0

    def test_load_corrupt_file(self, tmp_path):
        reg_path = tmp_path / "bad.json"
        reg_path.write_text("not json{{{")
        reg = DatasetRegistry(registry_path=reg_path, persist=True)
        assert len(reg.entries) == 0

    def test_make_sha256_hash(self, tmp_path):
        reg = DatasetRegistry(persist=False)
        f = tmp_path / "data.bin"
        f.write_bytes(b"hello world")
        h = reg.make_sha256_hash(f)
        assert h is not None
        assert len(h) == 64  # hex sha256

    def test_make_sha256_hash_missing_file(self, tmp_path):
        reg = DatasetRegistry(persist=False)
        h = reg.make_sha256_hash(tmp_path / "nonexistent")
        assert h is None
