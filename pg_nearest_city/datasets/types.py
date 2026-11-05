"""Small classes / types and related functions for URLs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, cast


class CompressionAlgorithm(str, Enum):
    """Compression for exported data. AUTO prefers xz, then bz2, then gzip."""

    AUTO = "auto"
    GZIP = "gzip"
    BZ2 = "bz2"
    XZ = "xz"
    ZSTD = "zstd"

    @property
    def extension(self) -> str:
        """Return the file extension for this algorithm.

        Raises ValueError if called on AUTO (must be resolved first).
        """
        _map = {
            self.GZIP: ".gz",
            self.BZ2: ".bz2",
            self.XZ: ".xz",
            self.ZSTD: ".zst",
        }
        if self not in _map:
            raise ValueError(
                f"Cannot get extension for {self.value!r}; resolve AUTO first"
            )
        return _map[self]


class DownloadOutcome(str, Enum):
    """Outcome states for a download attempt."""

    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"
    LOCAL_FRESH = "LOCAL_FRESH"
    NOT_MODIFIED = "NOT_MODIFIED"


class EPSGCode(int, Enum):
    """Supported EPSG coordinate reference system codes."""

    WGS_84 = 4326


class GeonamesColumn(int, Enum):
    """Column indices for the GeoNames tab-separated data file."""

    GEONAME_ID = 0  # int id of record in geonames DB
    NAME = 1  # name of geographical point [UTF-8]
    ASCII_NAME = 2  # name of geographical point [ASCII]
    ALT_NAMES = 3  # comma-separated alt. names for geographical point [ASCII]
    LATITUDE = 4  # latitude in decimal degrees [WGS 84]
    LONGITUDE = 5  # longitude in decimal degrees [WGS 84]
    FEATURE_CLASS = 6  # geonames feature class
    FEATURE_CODE = 7  # geonames feature code
    COUNTRY_CODE = 8  # ISO-3166 alpha2 country code
    ALT_COUNTRY_CODES = 9  # comma-separated alt. ISO-3166 alpha2 country codes
    ADMIN1_CODE = 10  # 1st administrative division code
    ADMIN2_CODE = 11  # 2nd administrative division code
    ADMIN3_CODE = 12  # 3rd administrative division code
    ADMIN4_CODE = 13  # 4th administrative division code
    POPULATION = 14  # population of geographical point [int64_t]
    ELEVATION = 15  # elevation of geographical point [meters]
    DEM = 16  # digital elevation model {SRTM3, GTOPO30}
    TIMEZONE = 17  # IANA timezone id
    MODIFICATION_DATE = 18  # date of last modification [yyyy-MM-dd]


class HTTPCode(int, Enum):
    """Standard HTTP status codes."""

    OK = 200
    PARTIAL_CONTENT = 206
    MOVED_PERM = 301
    FOUND = 302
    NOT_MODIFIED = 304
    REDIRECT_TEMP = 307
    REDIRECT_PERM = 308
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    GONE = 410
    LENGTH_REQUIRED = 411
    RANGE_NOT_SATISFIABLE = 416
    TOO_MANY_REQUESTS = 429
    SERVER_ERR = 500
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504


@dataclass(frozen=True, slots=True)
class Header:
    """An HTTP header key-value pair."""

    key: str
    value: str

    def as_kv(self) -> tuple[str, str]:
        """Return the header as a (key, value) tuple."""
        return (self.key, self.value)


class ZipFileStatus(str, Enum):
    """Status codes for ZIP file inspection."""

    BAD_FILE = "BAD_FILE"
    MISSING = "MISSING"
    NO_FILE_NAMES = "NO_FILE_NAMES"
    OK = "OK"


def headers_to_mapping(
    headers: Mapping[str, str] | Iterable[Header] | None,
) -> dict[str, str]:
    """Normalize various header inputs into a plain dict[str, str]."""
    if headers is None:
        return {}
    if isinstance(headers, Mapping):
        return cast(dict[str, str], {k: v for k, v in headers.items()})

    return {h.key: h.value for h in headers}
