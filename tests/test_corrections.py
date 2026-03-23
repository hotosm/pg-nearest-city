"""Tests for pg_nearest_city.db.corrections data definitions."""

import pytest

from pg_nearest_city.db.corrections import (
    BOUNDARY_CORRECTIONS,
    DATA_CORRECTIONS,
    GADM_DATA_CORRECTIONS,
    NE_BOUNDARY_CORRECTIONS,
    NE_DATA_CORRECTIONS,
)
from pg_nearest_city.db.data_cleanup import make_queries


@pytest.mark.integration
class TestDataCorrections:
    def test_common_corrections_produce_valid_sql(self, test_db_conn_string):
        """All DATA_CORRECTIONS generate valid psycopg.sql objects."""
        import psycopg
        import psycopg.sql

        conn = psycopg.Connection.connect(test_db_conn_string)
        queries = make_queries(DATA_CORRECTIONS)
        assert len(queries) == len(DATA_CORRECTIONS)
        for q in queries:
            assert isinstance(q, psycopg.sql.Composed)
            sql_str = q.as_string(conn)
            assert "UPDATE" in sql_str or "DELETE" in sql_str or "INSERT" in sql_str
        conn.close()

    def test_gadm_corrections_produce_valid_sql(self, test_db_conn_string):
        import psycopg

        conn = psycopg.Connection.connect(test_db_conn_string)
        queries = make_queries(GADM_DATA_CORRECTIONS)
        assert len(queries) == len(GADM_DATA_CORRECTIONS)
        for q in queries:
            sql_str = q.as_string(conn)
            assert "UPDATE" in sql_str
        conn.close()


class TestNEAlpha3Remaps:
    """NE uses non-ISO alpha3 codes for some ISO entities; verify remaps."""

    NE_REMAPS = {
        "ALD": "ALA",  # Åland
        "KOS": "XKX",  # Kosovo
        "PSX": "PSE",  # Palestine
        "SAH": "ESH",  # Western Sahara
        "SDS": "SSD",  # South Sudan
    }

    @pytest.mark.parametrize("ne_code,iso_code", NE_REMAPS.items())
    def test_ne_remap_exists(self, ne_code, iso_code):
        match = [
            r
            for r in NE_DATA_CORRECTIONS
            if any(p.col_val == ne_code for p in r.predicate_cols)
        ]
        assert len(match) == 1
        assert match[0].col_val == iso_code
        assert match[0].tbl_name == "tmp_country_bounds_adm0"

    def test_no_kosovo_to_serbia_remap_in_common_corrections(self):
        for row in DATA_CORRECTIONS:
            assert not (
                row.tbl_name == "geocoding"
                and row.col_val == "RS"
                and any(p.col_val == "XK" for p in row.predicate_cols)
            )

    def test_gadm_remaps_xko_to_xkx(self):
        kosovo = [
            r
            for r in GADM_DATA_CORRECTIONS
            if any(p.col_val == "XKO" for p in r.predicate_cols)
        ]
        assert len(kosovo) == 1
        assert kosovo[0].col_val == "XKX"
        assert kosovo[0].tbl_name == "tmp_country_bounds_adm0"


class TestBoundaryCorrections:
    def test_common_boundary_corrections_defined(self):
        assert len(BOUNDARY_CORRECTIONS) > 0
        isos = {t.iso for t in BOUNDARY_CORRECTIONS}
        assert "BEL" in isos
        assert "ITA" in isos

    def test_ne_boundary_corrections_defined(self):
        assert len(NE_BOUNDARY_CORRECTIONS) > 0
        isos = {t.iso for t in NE_BOUNDARY_CORRECTIONS}
        assert "GIB" in isos
        assert "USA" in isos
