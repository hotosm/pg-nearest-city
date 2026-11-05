"""Fixes for upstream city data."""

from pg_nearest_city.db.data_cleanup import (
    PredicateData,
    RowData,
    UpdateData,
    _CorrectionType,
    _PredicateComparison,
)

# --- Data corrections (spelling, erratum, etc.) ---
DATA_CORRECTIONS: list[RowData] = [
    UpdateData(
        correction_type=_CorrectionType.SPELLING,
        description="Change spelling of Simson Bay Village to Simpson Bay Village",
        tbl_name="geocoding",
        col_name="city",
        col_val="Simpson Bay Village",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="city",
                comparison=_PredicateComparison.EQUAL,
                col_val="Simson Bay Village",
            ),
            PredicateData(
                col_name="country",
                comparison=_PredicateComparison.EQUAL,
                col_val="SX",
            ),
        ],
    ),
    UpdateData(
        correction_type=_CorrectionType.SPELLING,
        description="Change spelling of St. Michiel to Sint Michiel",
        tbl_name="geocoding",
        col_name="city",
        col_val="Sint Michiel",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="city",
                comparison=_PredicateComparison.EQUAL,
                col_val="St. Michiel",
            ),
            PredicateData(
                col_name="country",
                comparison=_PredicateComparison.EQUAL,
                col_val="CW",
            ),
        ],
    ),
    UpdateData(
        correction_type=_CorrectionType.SPELLING,
        description="Change spelling of Büsingen to Büsingen am Hochrhein",
        tbl_name="geocoding",
        col_name="city",
        col_val="Büsingen am Hochrhein",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="city",
                comparison=_PredicateComparison.EQUAL,
                col_val="Büsingen",
            ),
            PredicateData(
                col_name="country",
                comparison=_PredicateComparison.EQUAL,
                col_val="DE",
            ),
        ],
    ),
    UpdateData(
        correction_type=_CorrectionType.SPELLING,
        description="Change spelling of Pante Makasar to Pante Macassar",
        tbl_name="geocoding",
        col_name="city",
        col_val="Pante Macassar",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="city",
                comparison=_PredicateComparison.EQUAL,
                col_val="Pante Makasar",
            ),
            PredicateData(
                col_name="country",
                comparison=_PredicateComparison.EQUAL,
                col_val="TL",
            ),
        ],
    ),
]
