"""Dataset definitions and pre-configured URLConfig instances."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .types import (
    EPSGCode,
    GeonamesColumn,
)
from .url_config import URLConfig


@dataclass(frozen=True, slots=True)
class CityDataset:
    """Represents a dataset for cities from GeoNames.

    alpha2_column: The column containing the ISO-3166-1 alpha-2 country code.
    alpha3_column: The column containing the ISO-3166-1 alpha-3 country code.
    name_column: The column containing the city name.
    lat_column: The column containing the latitude of the city.
    lon_column: The column containing the longitude of the city.
    epsg_code: The code of the CRS used for lat/lon.
    desired_order: The desired order of the columns, if any.
    header: A header defining the position of all columns in the input file.
    """

    alpha2_column: str | GeonamesColumn = ""
    alpha3_column: str | GeonamesColumn = ""
    name_column: str | GeonamesColumn = ""
    lat_column: str | GeonamesColumn = ""
    lon_column: str | GeonamesColumn = ""
    epsg_code: int | EPSGCode = EPSGCode.WGS_84
    desired_order: list = field(default_factory=list)
    header: list[str] = field(default_factory=list)

    def __post_init__(self):
        if self.desired_order:
            object.__setattr__(
                self, "desired_order", [getattr(self, x) for x in self.desired_order]
            )
        else:
            object.__setattr__(
                self,
                "desired_order",
                [v for k, v in asdict(self).items() if v and k.endswith("_column")],
            )

        object.__setattr__(
            self, "header", [x.lower() for x in GeonamesColumn.__members__.keys()]
        )


@dataclass(frozen=True, slots=True)
class PromotedTerritory:
    """An ADM1 region to promote to a standalone ADM0-level country.

    alpha3: ISO 3166-1 alpha-3 code for the promoted territory.
    gid1: Key matching the gid1 column in tmp_country_bounds_adm1.
    name: Country name.
    parent_alpha3: Alpha-3 of the parent country whose polygon will be trimmed.
    """

    alpha3: str
    gid1: str
    name: str
    parent_alpha3: str


@dataclass(frozen=True, slots=True)
class CountryDataset:
    """Represents a dataset for countries.

    alpha3_column: The column containing the ISO-3166-1 alpha-3 country code.
    adm0_name_column: The column containing the country name.
    adm1_column: The column containing the first administrative level code.
    adm1_name_column: The column containing the first administrative level name.
    """

    alpha3_column: str = ""
    adm0_name_column: str = ""
    adm1_column: str = ""
    adm1_name_column: str = ""
    desired_order: list = field(default_factory=list)

    def __post_init__(self):
        if self.desired_order:
            object.__setattr__(
                self, "desired_order", [getattr(self, x) for x in self.desired_order]
            )
        else:
            object.__setattr__(
                self,
                "desired_order",
                [v for k, v in asdict(self).items() if v and k.endswith("_column")],
            )


# ---------------------------------------------------------------------------
# GeoNames
# ---------------------------------------------------------------------------
GEONAMES_URLCONFIG = URLConfig(
    url="https://download.geonames.org/export/dump/cities500.zip"
)

GEONAMES_DATASET = CityDataset(
    alpha2_column=GeonamesColumn.COUNTRY_CODE,
    name_column=GeonamesColumn.NAME,
    lon_column=GeonamesColumn.LONGITUDE,
    lat_column=GeonamesColumn.LATITUDE,
    desired_order=["name_column", "alpha2_column", "lat_column", "lon_column"],
)

# ---------------------------------------------------------------------------
# GADM (baseline country boundaries)
# ---------------------------------------------------------------------------
GADM_URLCONFIG = URLConfig(
    url="https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-levels.zip",
)

GADM_0_DATASET = CountryDataset(alpha3_column="GID_0", adm0_name_column="COUNTRY")

GADM_1_DATASET = CountryDataset(
    alpha3_column="GID_0",
    adm1_column="GID_1",
    adm0_name_column="COUNTRY",
    adm1_name_column="NAME_1",
)

# ---------------------------------------------------------------------------
# Natural Earth (country boundaries; alternative to GADM)
# ---------------------------------------------------------------------------
NE_COUNTRIES_URLCONFIG = URLConfig(
    url="https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
)

NE_ADM1_URLCONFIG = URLConfig(
    url="https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
)

NE_COUNTRIES_DATASET = CountryDataset(
    alpha3_column="ADM0_A3",
    adm0_name_column="NAME",
)

NE_ADM1_DATASET = CountryDataset(
    alpha3_column="adm0_a3",
    adm1_column="adm1_code",
    adm0_name_column="admin",
    adm1_name_column="name",
)

# ---------------------------------------------------------------------------
# ADM1-to-ADM0 promotions
# Regions that exist only in the ADM1 layer of a given boundary source but
# should be treated as standalone countries.  The gid1 values are
# source-specific (GADM uses "CHN.HKG", NE uses "FRA-2000", etc.).
# ---------------------------------------------------------------------------
GADM_PROMOTED_TERRITORIES: list[PromotedTerritory] = [
    PromotedTerritory(
        alpha3="HKG", gid1="CHN.HKG", name="Hong Kong", parent_alpha3="CHN"
    ),
    PromotedTerritory(alpha3="MAC", gid1="CHN.MAC", name="Macau", parent_alpha3="CHN"),
]

NE_PROMOTED_TERRITORIES: list[PromotedTerritory] = [
    PromotedTerritory(
        alpha3="GLP", gid1="FRA-4603", name="Guadeloupe", parent_alpha3="FRA"
    ),
    PromotedTerritory(
        alpha3="GUF", gid1="FRA-2000", name="French Guiana", parent_alpha3="FRA"
    ),
    PromotedTerritory(
        alpha3="MTQ", gid1="FRA-1442", name="Martinique", parent_alpha3="FRA"
    ),
    PromotedTerritory(
        alpha3="MYT", gid1="FRA-4602", name="Mayotte", parent_alpha3="FRA"
    ),
    PromotedTerritory(
        alpha3="REU", gid1="FRA-4601", name="Réunion", parent_alpha3="FRA"
    ),
]

# ---------------------------------------------------------------------------
# Enclave dependencies
# When loading a single country, these additional countries must also be
# loaded so that RESOLVE_COUNTRY_OVERLAPS can correctly subtract enclave
# geometries.  Keys and values are ISO 3166-1 alpha-3 codes.
# The resolver walks the graph transitively (e.g. CHE → ITA → VAT, SMR).
# ---------------------------------------------------------------------------
ENCLAVE_DEPENDENCIES: dict[str, list[str]] = {
    "ITA": ["VAT", "SMR"],
    "FRA": ["MCO"],
    "ZAF": ["LSO", "SWZ"],
    "NLD": ["BEL"],
    "BEL": ["NLD"],
    "CHE": ["ITA", "DEU"],  # Campione d'Italia, Büsingen am Hochrhein
    "ESP": ["GIB", "AND"],
}


def get_required_countries(target: str) -> set[str]:
    """Find all countries needed for correct boundary resolution.

    Walks ENCLAVE_DEPENDENCIES transitively so that nested enclaves
    (e.g. CHE → ITA → VAT, SMR) are included.

    Args:
        target: ISO 3166-1 alpha-3 code of the requested country.

    Returns:
        Set of alpha-3 codes including the target and all dependencies.
    """
    required: set[str] = set()
    stack = [target]
    while stack:
        country = stack.pop()
        if country in required:
            continue
        required.add(country)
        stack.extend(ENCLAVE_DEPENDENCIES.get(country, []))
    return required


# ---------------------------------------------------------------------------
# Overpass
# Used as a fallback boundary source for territories absent from both
# Natural Earth and GeoBoundaries.
# ---------------------------------------------------------------------------
OVERPASS_URLCONFIG = URLConfig(
    url="https://overpass.private.coffee/api/interpreter"
)
