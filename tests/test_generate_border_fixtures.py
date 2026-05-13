from pathlib import Path

import pytest

from pg_nearest_city.scripts.generate_border_fixtures import (
    CountryOracleRef,
    count_probe_rows_by_pair_and_status,
    count_rows_by_pair,
    format_probe_status_summary,
    gdal_pg_conn_string_for_schema,
    make_oracle_schema_name,
    normalize_country_pair,
    normalize_country_pairs,
    pairs_without_ok_probes,
    pairs_without_rows,
    parse_args,
)
from pg_nearest_city.db.settings import DBConnSettings


def test_normalize_country_pair_uppercases_and_sorts():
    assert normalize_country_pair("us/mx") == ("MX", "US")
    assert normalize_country_pair("SI / IT") == ("IT", "SI")


def test_normalize_country_pair_rejects_invalid_shape():
    with pytest.raises(ValueError, match="expected like IT/SI"):
        normalize_country_pair("USA/MX")


def test_normalize_country_pairs_parses_multiple_values():
    assert normalize_country_pairs(["us/mx", "SI / IT"]) == {
        ("MX", "US"),
        ("IT", "SI"),
    }


def test_normalize_country_pairs_returns_empty_set_without_input():
    assert normalize_country_pairs([]) == set()


def test_parse_args_defaults_country_pair_to_empty_for_global_discovery():
    args = parse_args([])

    assert args.country_pair == []
    assert args.pairs_output is None
    assert args.probes_output is None
    assert args.cache_dir == Path("/data/cache")
    assert args.boundary_source == "naturalearth"


def test_parse_args_accepts_pair_output_alias():
    args = parse_args(["--pair-output", "tmp/pairs.csv", "--country-pair", "FR/MC"])

    assert args.pairs_output == Path("tmp/pairs.csv")
    assert args.country_pair == ["FR/MC"]


def test_count_rows_by_pair_counts_discovered_pair_keys():
    rows = [
        ("FR/MC", "FR", "MC"),
        ("FR/MC", "FR", "MC"),
        ("BD/IN", "BD", "IN"),
    ]

    assert count_rows_by_pair(rows) == {"FR/MC": 2, "BD/IN": 1}


def test_pairs_without_rows_reports_required_pairs_with_no_discoveries():
    country_pairs = {("BD", "IN"), ("FR", "MC"), ("RS", "XK")}
    rows = [
        ("FR/MC", "FR", "MC"),
        ("BD/IN", "BD", "IN"),
    ]

    assert pairs_without_rows(country_pairs, rows) == ["RS/XK"]


def test_count_probe_rows_by_pair_and_status_counts_statuses():
    rows = [
        ("FR/MC", "FR", "ok"),
        ("FR/MC", "MC", "ambiguous"),
        ("FR/MC", "MC", "ambiguous"),
        ("BD/IN", "BD", "unplaceable"),
    ]

    assert count_probe_rows_by_pair_and_status(rows) == {
        "FR/MC": {"ok": 1, "ambiguous": 2},
        "BD/IN": {"unplaceable": 1},
    }


def test_pairs_without_ok_probes_reports_required_pairs_without_usable_rows():
    country_pairs = {("BD", "IN"), ("FR", "MC"), ("RS", "XK")}
    rows = [
        ("FR/MC", "FR", "ok"),
        ("BD/IN", "BD", "unplaceable"),
    ]

    assert pairs_without_ok_probes(country_pairs, rows) == ["BD/IN", "RS/XK"]


def test_format_probe_status_summary_uses_stable_status_order():
    assert (
        format_probe_status_summary({"unplaceable": 2, "ok": 1})
        == "ok=1, ambiguous=0, unplaceable=2"
    )


def test_country_oracle_ref_formats_qualified_name():
    ref = CountryOracleRef(schema="tmp_border_oracle_test")

    assert ref.qualified_name == "tmp_border_oracle_test.border_country_oracle"


def test_make_oracle_schema_name_uses_prefix_and_pid(monkeypatch):
    monkeypatch.setattr(
        "pg_nearest_city.scripts.generate_border_fixtures.os.getpid", lambda: 123
    )

    assert make_oracle_schema_name("tmp_test") == "tmp_test_123"


def test_gdal_pg_conn_string_for_schema_includes_active_schema():
    settings = DBConnSettings(
        host="db.example.test",
        port=5433,
        name="nearest",
        user="loader",
        password="secret'quote",
    )

    conn_string = gdal_pg_conn_string_for_schema(settings, "tmp_oracle")

    assert "host='db.example.test'" in conn_string
    assert "port='5433'" in conn_string
    assert "dbname='nearest'" in conn_string
    assert "user='loader'" in conn_string
    assert "password='secret\\'quote'" in conn_string
    assert "active_schema='tmp_oracle'" in conn_string
