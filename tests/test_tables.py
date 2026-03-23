"""Tests for pg_nearest_city.db.tables."""

import warnings

import pytest

from pg_nearest_city.db.tables import (
    BaseTable,
    Country,
    CountryInit,
    Geocoding,
    Index,
    IndexType,
    TmpCountryBoundsAdm0,
    TmpIso3166,
    TmpNonIsoAdm0,
    TmpNonIsoGid0Parent,
    TmpPromotedAdm1ToGid0,
    create_dependency_graph,
    get_tables,
    get_tables_in_creation_order,
)


class TestIndex:
    def test_btree_index(self):
        idx = Index(tbl_name="foo", col_names=["bar"], index_type=IndexType.BTREE)
        assert idx.name == "foo_bar_idx"
        assert "BTREE" in idx.index_def
        assert "CREATE INDEX IF NOT EXISTS" in idx.index_def

    def test_gist_index(self):
        idx = Index(tbl_name="foo", col_names=["geom"], index_type=IndexType.GIST)
        assert idx.name == "foo_geom_idx"
        assert "GIST" in idx.index_def

    def test_multi_column_index(self):
        idx = Index(tbl_name="t", col_names=["a", "b"])
        assert idx.name == "t_a_b_idx"
        assert "a, b" in idx.index_def

    def test_unique_index_suffix(self):
        idx = Index(tbl_name="t", col_names=["x"], is_unique=True)
        assert idx.name == "t_x_unq"

    def test_partial_index_suffix(self):
        idx = Index(
            tbl_name="t",
            col_names=["x"],
            partial_def="WHERE active = true",
        )
        assert idx.name == "t_x_pidx"
        assert "WHERE active = true" in idx.index_def


class TestBaseTable:
    def test_attrs_returns_classvar_names(self):
        attrs = BaseTable.attrs()
        assert "name" in attrs
        assert "is_temp" in attrs
        assert "depends_on" in attrs

    def test_create_sql_externally_defined_raises(self):
        assert TmpCountryBoundsAdm0.is_externally_defined is True
        with pytest.raises(NotImplementedError):
            TmpCountryBoundsAdm0.create_sql()

    def test_create_sql_base_returns_empty(self):
        assert BaseTable.create_sql() == ""


class TestTableSubclasses:
    def test_country_create_sql(self):
        sql = Country.create_sql()
        assert "CREATE TABLE country" in sql
        assert "alpha2 CHAR(2)" in sql
        assert "Polygon" in sql

    def test_country_indices(self):
        indices = Country.get_indices()
        assert len(indices) == 2
        types = {i.index_type for i in indices}
        assert IndexType.GIST in types
        assert IndexType.BTREE in types

    def test_geocoding_create_sql(self):
        sql = Geocoding.create_sql()
        assert "CREATE TABLE geocoding" in sql
        assert "ST_MakePoint" in sql

    def test_geocoding_depends_on_country(self):
        assert Country in Geocoding.depends_on

    def test_geocoding_indices(self):
        indices = Geocoding.get_indices()
        assert len(indices) == 1
        assert indices[0].index_type == IndexType.GIST

    def test_country_init_create_sql(self):
        sql = CountryInit.create_sql()
        assert "MultiPolygon" in sql
        assert "country_init" in sql

    def test_country_init_is_temp(self):
        assert CountryInit.is_temp is True
        assert CountryInit.is_unlogged is True

    def test_tmp_iso3166_create_sql(self):
        sql = TmpIso3166.create_sql()
        assert "tmp_iso3166" in sql
        assert "alpha3 CHAR(3)" in sql

    def test_tmp_noniso_adm0_create_sql(self):
        sql = TmpNonIsoAdm0.create_sql()
        assert "tmp_noniso_adm0" in sql
        assert "MultiPolygon" in sql

    def test_tmp_noniso_gid0_parent_create_sql(self):
        sql = TmpNonIsoGid0Parent.create_sql()
        assert "parent_gid0" in sql

    def test_tmp_promoted_adm1_create_sql(self):
        sql = TmpPromotedAdm1ToGid0.create_sql()
        assert "gid1" in sql


class TestGetTables:
    def test_unfiltered_returns_all(self):
        tables = get_tables()
        assert len(tables) >= 8
        names = {t.name for t in tables}
        assert "country" in names
        assert "geocoding" in names

    def test_filter_temp_tables(self):
        temp_tables = get_tables(filters=[{"is_temp": True}])
        assert all(t.is_temp for t in temp_tables)
        assert len(temp_tables) >= 4

    def test_filter_non_temp_tables(self):
        perm_tables = get_tables(filters=[{"is_temp": False}])
        names = {t.name for t in perm_tables}
        assert "country" in names
        assert "geocoding" in names
        assert not any(t.is_temp for t in perm_tables)


class TestDependencyGraph:
    def test_creation_order_country_before_geocoding(self):
        order = get_tables_in_creation_order()
        names = [t.name for t in order]
        assert names.index("country") < names.index("geocoding")

    def test_dependency_graph_warns_on_missing_deps(self):
        """Filtering out Country while keeping Geocoding warns about missing dep."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            create_dependency_graph(filters=[{"name": ["geocoding"]}])
            assert len(w) == 1
            assert "filtered-out" in str(w[0].message)
