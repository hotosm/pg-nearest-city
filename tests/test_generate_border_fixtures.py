from pathlib import Path

import pytest

from pg_nearest_city.scripts.generate_border_fixtures import (
    DEFAULT_COUNTRY_PAIRS,
    count_rows_by_pair,
    parse_args,
    select_country_pairs,
)


def test_select_country_pairs_uses_default_allowlist_without_overrides():
    assert select_country_pairs([]) == set(DEFAULT_COUNTRY_PAIRS)


def test_select_country_pairs_replaces_default_allowlist_with_overrides():
    selected = select_country_pairs(["us/mx", "SI / IT"])

    assert selected == {("MX", "US"), ("IT", "SI")}
    assert not selected.intersection(DEFAULT_COUNTRY_PAIRS)


def test_select_country_pairs_rejects_invalid_pair_shape():
    with pytest.raises(ValueError, match="expected like IT/SI"):
        select_country_pairs(["USA/MX"])


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
