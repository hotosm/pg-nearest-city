from pathlib import Path

import pytest

from pg_nearest_city.scripts.generate_border_fixtures import (
    BORDER_PROBE_FIXTURE_PATH,
    BorderFixtureGenerator,
    BorderFixtureResult,
    CountryOracleRef,
    PROBE_CSV_HEADER,
    PairSummary,
    ProbeRow,
    gdal_pg_conn_string_for_schema,
    main,
    make_oracle_schema_name,
    normalize_country_pair,
    normalize_country_pairs,
    parse_args,
    write_probe_rows,
)
from pg_nearest_city.db.settings import DBConnSettings


def _make_probe_row() -> ProbeRow:
    return ProbeRow(
        pair="FR/MC",
        source_country="MC",
        expected_alpha2="MC",
        expected_alpha3="MCO",
        expected_country_name="Monaco",
        neighbor_country="FR",
        source_city="Monaco",
        source_lat="43.7333000",
        source_lon="7.4167000",
        neighbor_city="Beausoleil",
        neighbor_lat="43.7436000",
        neighbor_lon="7.4238000",
        seam_lat="43.7381000",
        seam_lon="7.4210000",
        ring_distance_m=100,
        probe_lat="43.7375000",
        probe_lon="7.4205000",
        status="ok",
    )


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
    assert args.probes_output == BORDER_PROBE_FIXTURE_PATH
    assert args.cache_dir == Path("/data/cache")
    assert args.boundary_source == "naturalearth"


def test_parse_args_accepts_probe_output_override():
    args = parse_args(["--probes-output", "tmp/probes.csv"])

    assert args.probes_output == Path("tmp/probes.csv")


def test_probe_csv_header_is_the_committed_fixture_contract():
    assert PROBE_CSV_HEADER == [
        "pair",
        "source_country",
        "expected_alpha2",
        "expected_alpha3",
        "expected_country_name",
        "neighbor_country",
        "source_city",
        "source_lat",
        "source_lon",
        "neighbor_city",
        "neighbor_lat",
        "neighbor_lon",
        "seam_lat",
        "seam_lon",
        "ring_distance_m",
        "probe_lat",
        "probe_lon",
        "status",
    ]
    assert "id" not in PROBE_CSV_HEADER


def test_probe_row_as_csv_row_matches_header_order():
    row = _make_probe_row()

    csv_row = row.as_csv_row()

    assert len(csv_row) == len(PROBE_CSV_HEADER)
    assert dict(zip(PROBE_CSV_HEADER, csv_row)) == {
        "pair": "FR/MC",
        "source_country": "MC",
        "expected_alpha2": "MC",
        "expected_alpha3": "MCO",
        "expected_country_name": "Monaco",
        "neighbor_country": "FR",
        "source_city": "Monaco",
        "source_lat": "43.7333000",
        "source_lon": "7.4167000",
        "neighbor_city": "Beausoleil",
        "neighbor_lat": "43.7436000",
        "neighbor_lon": "7.4238000",
        "seam_lat": "43.7381000",
        "seam_lon": "7.4210000",
        "ring_distance_m": 100,
        "probe_lat": "43.7375000",
        "probe_lon": "7.4205000",
        "status": "ok",
    }


def test_write_probe_rows_uses_fixed_header_and_lf_line_endings(tmp_path):
    output = tmp_path / "fixtures" / "border_country_probes.csv"
    rows = [_make_probe_row().as_csv_row()]

    write_probe_rows(output, rows)

    assert output.read_text(encoding="utf-8") == (
        ",".join(PROBE_CSV_HEADER)
        + "\nFR/MC,MC,MC,MCO,Monaco,FR,Monaco,43.7333000,7.4167000,"
        "Beausoleil,43.7436000,7.4238000,43.7381000,7.4210000,100,"
        "43.7375000,7.4205000,ok\n"
    )


def test_main_fails_loudly_for_missing_required_pairs(monkeypatch):
    result = BorderFixtureResult(
        probe_rows=[],
        pair_summaries=[
            PairSummary(
                pair="FR/MC",
                discovered_rows=0,
                status_counts={"ok": 0, "ambiguous": 0, "unplaceable": 0},
            )
        ],
        missing_required_pairs=["FR/MC"],
        missing_ok_pairs=[],
        discovered_pair_count=0,
        requested_country_pairs=frozenset({("FR", "MC")}),
    )
    monkeypatch.setattr(BorderFixtureGenerator, "run", lambda self: result)
    monkeypatch.setattr(
        "sys.argv", ["generate_border_fixtures.py", "--country-pair", "FR/MC"]
    )

    with pytest.raises(
        SystemExit,
        match="no discovered city pairs for required country pairs: FR/MC",
    ):
        main()


def test_main_fails_loudly_for_missing_usable_probes(monkeypatch):
    result = BorderFixtureResult(
        probe_rows=[],
        pair_summaries=[
            PairSummary(
                pair="FR/MC",
                discovered_rows=1,
                status_counts={"ok": 0, "ambiguous": 1, "unplaceable": 0},
            )
        ],
        missing_required_pairs=[],
        missing_ok_pairs=["FR/MC"],
        discovered_pair_count=1,
        requested_country_pairs=frozenset({("FR", "MC")}),
    )
    monkeypatch.setattr(BorderFixtureGenerator, "run", lambda self: result)
    monkeypatch.setattr(
        "sys.argv", ["generate_border_fixtures.py", "--country-pair", "FR/MC"]
    )

    with pytest.raises(
        SystemExit,
        match="no usable seam probe rows for required country pairs: FR/MC",
    ):
        main()


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
