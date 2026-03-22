"""Single source of truth for all upstream data fixes.

Data corrections (spelling, erratum), boundary corrections
(GeoBoundaries targets), and Overpass boundary targets are
defined here as Python instances.
SQL pipeline logic stays in queries.py.
"""

from __future__ import annotations

from pg_nearest_city.datasets.geoboundaries import GeoBoundaryTarget
from pg_nearest_city.datasets.types import (
    OverpassBoundaryTarget,
    OverpassItemType,
    OverpassQL,
    OverpassQueryBuilder,
    OverpassType,
)
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

# GADM-specific: reparent HK/MO ADM1 entries from CHN to their own alpha3,
# so the promotion pipeline treats them as standalone countries.
# Also remap Kosovo's GADM code (XKO) to the user-assigned ISO code (XKX).
GADM_DATA_CORRECTIONS: list[RowData] = [
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
        description="Remap Kosovo's GADM code (XKO) to ISO user-assigned code (XKX)",
        tbl_name="tmp_country_bounds_adm0",
        col_name="gid0",
        col_val="XKX",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="gid0",
                comparison=_PredicateComparison.EQUAL,
                col_val="XKO",
            ),
        ],
    ),
]

# NE-specific: remap Kosovo's Natural Earth code (KOS) to the user-assigned
# ISO code (XKX) so it matches the ISO 3166-1 CSV and isn't absorbed.
NE_DATA_CORRECTIONS: list[RowData] = [
    UpdateData(
        correction_type=_CorrectionType.ERRATUM,
        description="Remap Kosovo's NE code (KOS) to ISO user-assigned code (XKX)",
        tbl_name="tmp_country_bounds_adm0",
        col_name="gid0",
        col_val="XKX",
        result_limit=1,
        predicate_cols=[
            PredicateData(
                col_name="gid0",
                comparison=_PredicateComparison.EQUAL,
                col_val="KOS",
            ),
        ],
    ),
]

BOUNDARY_CORRECTIONS: list[GeoBoundaryTarget] = [
    GeoBoundaryTarget(iso="BEL", adm_level="ADM0"),
    GeoBoundaryTarget(iso="CHE", adm_level="ADM0"),
    GeoBoundaryTarget(iso="ITA", adm_level="ADM0"),
    GeoBoundaryTarget(iso="LUX", adm_level="ADM0"),
    GeoBoundaryTarget(
        iso="MCO", adm_level="ADM0"
    ),  # MC - Monaco (polygon overshoots into adjacent FR territory)
    GeoBoundaryTarget(iso="NLD", adm_level="ADM0"),
    GeoBoundaryTarget(iso="SVN", adm_level="ADM0"),
]

NE_BOUNDARY_CORRECTIONS: list[GeoBoundaryTarget] = [
    # Countries where NE geometry is imprecise or missing key sub-territories.
    GeoBoundaryTarget(
        iso="CYP", adm_level="ADM0"
    ),  # CY - Cyprus (NE geometry excludes northern buffer zone)
    # IND handled via Overpass (see OVERPASS_BOUNDARY_TARGETS below).
    GeoBoundaryTarget(
        iso="TLS", adm_level="ADM0"
    ),  # TL - Timor-Leste (NE omits Oecusse exclave)
    # Small exclaves and islands absent from or poorly modelled in NE.
    GeoBoundaryTarget(
        iso="FRA", adm_level="ADM0"
    ),  # NE polygon doesn't cover area near Monaco border
    GeoBoundaryTarget(iso="DEU", adm_level="ADM0"),  # Büsingen exclave
    GeoBoundaryTarget(iso="ESP", adm_level="ADM0"),  # Llívia exclave
    GeoBoundaryTarget(iso="GBR", adm_level="ADM0"),  # island coverage (e.g. Islay)
    GeoBoundaryTarget(iso="GIB", adm_level="ADM0"),  # absent from NE admin0
    GeoBoundaryTarget(
        iso="MYT", adm_level="ADM0"
    ),  # NE ADM1 polygon too coarse for Mayotte
    GeoBoundaryTarget(iso="USA", adm_level="ADM0"),  # Point Roberts, Niagara border
]


# ---------------------------------------------------------------------------
# Overpass boundary targets
# Territories absent from Natural Earth / GeoBoundaries, or where those
# sources are too imprecise and OSM provides a better polygon.
# Only applied when using Natural Earth (skipped for GADM).
# ---------------------------------------------------------------------------
def _make_iso_boundary_query(iso2: str) -> OverpassQL:
    """Build an Overpass QL query for a country boundary by ISO 3166-1 alpha-2 code."""
    return OverpassQL(
        item_type=[OverpassItemType.DEFAULT_INPUT, OverpassItemType.RECURSE_DOWN_REL],
        out_fmt="xml",
        out_stmt="body",
        queries=[
            OverpassQueryBuilder(OverpassType.REL)
            .iso3166(iso2)
            .boundary("administrative")
            .build()
        ],
    )


OVERPASS_BOUNDARY_TARGETS: list[OverpassBoundaryTarget] = [
    OverpassBoundaryTarget(
        alpha2="IN",
        alpha3="IND",
        name="India",
        query=_make_iso_boundary_query("IN"),
    ),  # NE polygon has border overlaps with NP, MM, BT, BD, PK
    OverpassBoundaryTarget(
        alpha2="SX",
        alpha3="SXM",
        name="Sint Maarten",
        query=_make_iso_boundary_query("SX"),
    ),  # absent from both NE admin0 and GeoBoundaries
]
