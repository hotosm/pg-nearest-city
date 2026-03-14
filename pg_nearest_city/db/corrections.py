"""Single source of truth for all upstream data fixes.

Data corrections (spelling, erratum) and boundary corrections
(GeoBoundaries targets) are defined here as Python instances.
SQL pipeline logic stays in queries.py.
"""

from __future__ import annotations

from pg_nearest_city.datasets.geoboundaries import GeoBoundaryTarget
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
    UpdateData(
        correction_type=_CorrectionType.ERRATUM,
        description="Update Macau's alpha3 column to MAC",
        tbl_name="tmp_country_bounds_adm1",
        col_name="parent_gid0",
        col_val="MAC",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="gid1",
                comparison=_PredicateComparison.EQUAL,
                col_val="CHN.MAC",
            ),
            PredicateData(
                col_name="name_1",
                comparison=_PredicateComparison.EQUAL,
                col_val="Macau",
            ),
        ],
    ),
    UpdateData(
        correction_type=_CorrectionType.ERRATUM,
        description="Update Hong Kong's alpha3 column to HKG",
        tbl_name="tmp_country_bounds_adm1",
        col_name="parent_gid0",
        col_val="HKG",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="gid1",
                comparison=_PredicateComparison.EQUAL,
                col_val="CHN.HKG",
            ),
            PredicateData(
                col_name="name_1",
                comparison=_PredicateComparison.EQUAL,
                col_val="Hong Kong",
            ),
        ],
    ),
    UpdateData(
        correction_type=_CorrectionType.ERRATUM,
        description="Update Kosovo's alpha2 column to RS (Serbia)",
        tbl_name="geocoding",
        col_name="country",
        col_val="RS",
        result_limit=100,
        predicate_cols=[
            PredicateData(
                col_name="country",
                comparison=_PredicateComparison.EQUAL,
                col_val="XK",
            ),
        ],
    ),
]

# --- Boundary corrections (GeoBoundaries targets) ---
# HK/MO are handled by GADM ADM1 promotion (higher resolution than GeoBoundaries).
# Other targets replace GADM ADM0 boundaries with more accurate GeoBoundaries data
# for countries where GADM has known issues (enclaves, border accuracy, etc.).
BOUNDARY_CORRECTIONS: list[GeoBoundaryTarget] = [
    GeoBoundaryTarget(iso="BEL", adm_level="ADM0"),
    GeoBoundaryTarget(iso="CHE", adm_level="ADM0"),
    GeoBoundaryTarget(iso="IND", adm_level="ADM0"),
    GeoBoundaryTarget(iso="ITA", adm_level="ADM0"),
    GeoBoundaryTarget(iso="LUX", adm_level="ADM0"),
    GeoBoundaryTarget(iso="NLD", adm_level="ADM0"),
    GeoBoundaryTarget(iso="NPL", adm_level="ADM0"),
    GeoBoundaryTarget(iso="SVN", adm_level="ADM0"),
]
