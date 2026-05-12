from pathlib import Path

import pytest

from pg_nearest_city.scripts.generate_border_fixtures import (
    count_rows_by_pair,
    normalize_country_pair,
    normalize_country_pairs,
    pairs_without_rows,
    parse_args,
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
    assert args.pairs_output is None


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
