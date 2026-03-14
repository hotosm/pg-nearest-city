"""Dataset definitions and pre-configured URLConfig instances."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .types import EPSGCode, GeonamesColumn
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
# Overpass
# Overpass support is available for targeted boundary queries; currently
# using GeoBoundaries for corrections.
# ---------------------------------------------------------------------------
OVERPASS_URLCONFIG = URLConfig(
    url="https://maps.mail.ru/osm/tools/overpass/api/interpreter"
)
