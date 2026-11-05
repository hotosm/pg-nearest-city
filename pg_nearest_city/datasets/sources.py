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
# geoBoundaries CGAZ (one gap-free global file of country boundaries)
# ---------------------------------------------------------------------------
CGAZ_URLCONFIG = URLConfig(
    url="https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.gpkg"
)

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
