"""Tests for pg_nearest_city.utils."""

import re

import pytest

from pg_nearest_city.utils import (
    COMPRESSION_EXTENSIONS,
    _attr_names_for,
    _eval_simple,
    _get_value,
    _is_sequence_iterable,
    _match_obj,
    filter_items,
    get_all_subclasses,
    open_compressed,
    preferred_compression_ext,
)


# ---------------------------------------------------------------------------
# get_all_subclasses
# ---------------------------------------------------------------------------
class TestGetAllSubclasses:
    def test_finds_subclasses_in_same_module(self):
        """get_all_subclasses finds subclasses defined in the base's module."""
        from pg_nearest_city.db.tables import BaseTable

        subs = get_all_subclasses(BaseTable)
        names = {c.__name__ for c in subs}
        assert "Country" in names
        assert "Geocoding" in names
        assert "CountryInit" in names

    def test_returns_empty_for_orphan_class(self):
        """A class with no module returns an empty list."""

        class Orphan:
            pass

        Orphan.__module__ = "__nosuchmodule__"
        # get_all_subclasses uses inspect.getmodule which may return None
        result = get_all_subclasses(Orphan)
        assert result == []


# ---------------------------------------------------------------------------
# _attr_names_for
# ---------------------------------------------------------------------------
class TestAttrNamesFor:
    def test_dict(self):
        assert _attr_names_for({"a": 1, "b": 2}) == {"a", "b"}

    def test_object_with_dict(self):
        class Obj:
            x = 1

        # vars on a class returns its own __dict__ keys
        assert "x" in _attr_names_for(Obj)

    def test_slots_string(self):
        class S:
            __slots__ = "name"

        # vars() works on the class itself, so slots are found via that path
        result = _attr_names_for(S)
        assert "name" in result

    def test_slots_tuple(self):
        class S:
            __slots__ = ("a", "b")

        result = _attr_names_for(S)
        assert "a" in result
        assert "b" in result

    def test_no_attrs(self):
        # int has no meaningful attrs for our purposes
        assert isinstance(_attr_names_for(42), set)


# ---------------------------------------------------------------------------
# _get_value
# ---------------------------------------------------------------------------
class TestGetValue:
    def test_from_dict(self):
        assert _get_value({"x": 10}, "x") == 10

    def test_from_object(self):
        class Obj:
            x = 10

        assert _get_value(Obj, "x") == 10

    def test_missing_key(self):
        from pg_nearest_city.utils import _MISSING

        assert _get_value({}, "nope") is _MISSING


# ---------------------------------------------------------------------------
# _is_sequence_iterable
# ---------------------------------------------------------------------------
class TestIsSequenceIterable:
    def test_list(self):
        assert _is_sequence_iterable([1, 2]) is True

    def test_set(self):
        assert _is_sequence_iterable({1, 2}) is True

    def test_string(self):
        assert _is_sequence_iterable("abc") is False

    def test_bytes(self):
        assert _is_sequence_iterable(b"abc") is False

    def test_dict(self):
        assert _is_sequence_iterable({"a": 1}) is False


# ---------------------------------------------------------------------------
# _eval_simple
# ---------------------------------------------------------------------------
class TestEvalSimple:
    def test_equality(self):
        assert _eval_simple(5, 5) is True
        assert _eval_simple(5, 6) is False

    def test_callable(self):
        assert _eval_simple(10, lambda x: x > 5) is True
        assert _eval_simple(3, lambda x: x > 5) is False

    def test_membership_list(self):
        assert _eval_simple("a", ["a", "b"]) is True
        assert _eval_simple("c", ["a", "b"]) is False

    def test_membership_set(self):
        assert _eval_simple(1, {1, 2, 3}) is True

    def test_regex_string(self):
        assert _eval_simple("hello world", ("regex", r"world$")) is True
        assert _eval_simple("hello", ("regex", r"world$")) is False

    def test_regex_compiled(self):
        pat = re.compile(r"\d+")
        assert _eval_simple("abc123", ("regex", pat)) is True
        assert _eval_simple("abc", ("regex", pat)) is False

    def test_regex_none_value(self):
        assert _eval_simple(None, ("regex", ".*")) is True


# ---------------------------------------------------------------------------
# _match_obj / filter_items
# ---------------------------------------------------------------------------
class TestFilterItems:
    def test_no_filters_returns_all(self):
        items = [1, 2, 3]
        assert filter_items(items) == items
        assert filter_items(items, []) == items

    def test_equality_filter(self):
        from dataclasses import dataclass

        @dataclass
        class Item:
            name: str
            active: bool

        items = [Item("a", True), Item("b", False), Item("c", True)]
        result = filter_items(items, [{"active": True}])
        assert len(result) == 2
        assert all(i.active for i in result)

    def test_or_semantics(self):
        """Multiple filter dicts are OR'ed."""
        items = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}]
        result = filter_items(items, [{"x": 1}, {"x": 3}])
        assert len(result) == 2

    def test_nested_dict_filter(self):
        """Dict condition recurses into nested attributes."""
        from dataclasses import dataclass

        @dataclass
        class Inner:
            val: int

        @dataclass
        class Outer:
            items: list

        o1 = Outer(items=[Inner(1), Inner(2)])
        o2 = Outer(items=[Inner(3)])
        result = filter_items([o1, o2], [{"items": {"val": 2}}])
        assert result == [o1]

    def test_disjoint_keys_no_match(self):
        """If filter keys don't overlap with object attrs, no match."""
        assert _match_obj({"a": 1}, {"z": 1}) is False

    def test_missing_value_no_match(self):
        """If filter key exists as attr name but value is MISSING, no match."""
        from pg_nearest_city.utils import _MISSING

        assert _match_obj({"a": _MISSING}, {"a": 1}) is False


# ---------------------------------------------------------------------------
# Compression helpers
# ---------------------------------------------------------------------------
class TestCompression:
    def test_compression_extensions_tuple(self):
        assert isinstance(COMPRESSION_EXTENSIONS, tuple)
        assert ".gz" in COMPRESSION_EXTENSIONS
        assert ".zst" in COMPRESSION_EXTENSIONS

    def test_preferred_compression_ext_returns_string(self):
        ext = preferred_compression_ext()
        assert ext.startswith(".")
        assert ext in COMPRESSION_EXTENSIONS

    def test_open_compressed_gzip_roundtrip(self, tmp_path):
        path = tmp_path / "test.gz"
        data = b"hello compressed world"
        with open_compressed(path, "wb") as fh:
            fh.write(data)
        with open_compressed(path, "rb") as fh:
            assert fh.read() == data

    def test_open_compressed_bz2_roundtrip(self, tmp_path):
        path = tmp_path / "test.bz2"
        data = b"bz2 test data"
        with open_compressed(path, "wb") as fh:
            fh.write(data)
        with open_compressed(path, "rb") as fh:
            assert fh.read() == data

    def test_open_compressed_xz_roundtrip(self, tmp_path):
        path = tmp_path / "test.xz"
        data = b"xz test data"
        with open_compressed(path, "wb") as fh:
            fh.write(data)
        with open_compressed(path, "rb") as fh:
            assert fh.read() == data

    def test_open_compressed_zstd_roundtrip(self, tmp_path):
        """Test zstd roundtrip if CLI is available."""
        from pg_nearest_city.utils import _HAS_ZSTD_CLI, _zstd_open

        if not _zstd_open and not _HAS_ZSTD_CLI:
            pytest.skip("zstd not available")  # type: ignore[invalid-argument-type]
        path = tmp_path / "test.zst"
        data = b"zstd test data"
        with open_compressed(path, "wb") as fh:
            fh.write(data)
        with open_compressed(path, "rb") as fh:
            assert fh.read() == data

    def test_open_compressed_zstd_unavailable_raises(self, tmp_path, monkeypatch):
        """When zstd is completely unavailable, open_compressed raises."""
        import pg_nearest_city.utils as utils_mod

        monkeypatch.setattr(utils_mod, "_zstd_open", None)
        monkeypatch.setattr(utils_mod, "_HAS_ZSTD_CLI", False)
        path = tmp_path / "test.zst"
        with pytest.raises(ImportError, match="zstd is not available"):
            with open_compressed(path, "rb") as _fh:
                pass

    def test_open_compressed_xz_unavailable_raises(self, tmp_path, monkeypatch):
        import pg_nearest_city.utils as utils_mod

        monkeypatch.setattr(utils_mod, "_HAS_LZMA", False)
        path = tmp_path / "test.xz"
        with pytest.raises(ImportError, match="lzma module is not available"):
            with open_compressed(path, "rb") as _fh:
                pass

    def test_open_compressed_bz2_unavailable_raises(self, tmp_path, monkeypatch):
        import pg_nearest_city.utils as utils_mod

        monkeypatch.setattr(utils_mod, "_HAS_BZ2", False)
        path = tmp_path / "test.bz2"
        with pytest.raises(ImportError, match="bz2 module is not available"):
            with open_compressed(path, "rb") as _fh:
                pass

    def test_preferred_ext_fallback_chain(self, monkeypatch):
        """Test the fallback chain in preferred_compression_ext."""
        import pg_nearest_city.utils as utils_mod

        # With everything disabled, should fall back to .gz
        monkeypatch.setattr(utils_mod, "_zstd_open", None)
        monkeypatch.setattr(utils_mod, "_HAS_ZSTD_CLI", False)
        monkeypatch.setattr(utils_mod, "_HAS_LZMA", False)
        monkeypatch.setattr(utils_mod, "_HAS_BZ2", False)
        assert preferred_compression_ext() == ".gz"

        # With only bz2
        monkeypatch.setattr(utils_mod, "_HAS_BZ2", True)
        assert preferred_compression_ext() == ".bz2"

        # With lzma
        monkeypatch.setattr(utils_mod, "_HAS_LZMA", True)
        assert preferred_compression_ext() == ".xz"

        # With zstd CLI
        monkeypatch.setattr(utils_mod, "_HAS_ZSTD_CLI", True)
        assert preferred_compression_ext() == ".zst"
