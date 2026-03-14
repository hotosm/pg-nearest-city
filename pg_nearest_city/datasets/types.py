"""Small classes / types and related functions for URLs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Iterable, Literal, Mapping, cast


class DownloadOutcome(str, Enum):
    """Outcome states for a download attempt."""

    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"
    LOCAL_FRESH = "LOCAL_FRESH"
    NOT_MODIFIED = "NOT_MODIFIED"


class EPSGCode(int, Enum):
    """Supported EPSG coordinate reference system codes."""

    WGS_84 = 4326


class GeoBoundaryRelease(str, Enum):
    """GeoBoundaries release tier identifiers."""

    GB_OPEN = "gbOpen"
    GB_HUMANITARIAN = "gbHumanitarian"
    GB_AUTHORITATIVE = "gbAuthoritative"

    def __str__(self):
        return self.value


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


class OverpassQueryMem(int, Enum):
    """Memory limit options for Overpass API queries."""

    DEFAULT = 2**29  # 512 MiB
    HIGH = 2**30  # 1 GiB


class OverpassFilterType(str, Enum):
    """Tag keys used for Overpass QL filter expressions."""

    ADMIN_LEVEL = "admin_level"
    BOUNDARY = "boundary"
    ISO3166_1 = "ISO3166-1"
    NAME = "name"
    PLACE = "place"


class OverpassFilter(str, Enum):
    """Comparison operators for Overpass QL filter expressions."""

    CASE_INSENSITIVE = "i"
    EQUAL = "="
    EXISTS = ""
    NOT_EQUAL = "!="
    NOT_EXISTS = "!"
    RE = "~"


class OverpassItemType(str, Enum):
    """Item recursion statements for Overpass QL output."""

    DEFAULT_INPUT = "._"
    RECURSE_DOWN = ">"
    RECURSE_DOWN_REL = ">>"
    RECURSE_UP = "<"
    RECURSE_UP_REL = "<<"


class OverpassType(str, Enum):
    """OSM element types for Overpass QL queries."""

    AREA = "area"
    DERIVED = "derived"
    NODE = "node"
    NODE_REL = "nr"
    NODE_WAY = "nw"
    NODE_WAY_REL = "nwr"
    REL = "rel"
    WAY = "way"


@dataclass(frozen=True, slots=True)
class OverpassQuery:
    """A single Overpass QL filter expression."""

    filter: OverpassFilter
    filter_type: OverpassFilterType
    query_rhs: str

    def __str__(self):
        return f'["{self.filter_type.value}"{self.filter.value}"{self.query_rhs}"]'


class OverpassQueryBuilder:
    """Builder for creating chains of OverpassQuery filters."""

    def __init__(self, osm_type: OverpassType):
        self.osm_type = osm_type
        self.queries: list[OverpassQuery] = []

    def filter(
        self,
        filter_type: OverpassFilterType,
        value: str,
        filter_op: OverpassFilter = OverpassFilter.EQUAL,
    ) -> OverpassQueryBuilder:
        """Add a filter to the query chain.

        Args:
            filter_type: The type of filter (name, boundary, place, etc.)
            value: The value to filter by
            filter_op: The filter operation (default: EQUAL)

        Returns:
            OverpassQueryBuilder for method chaining
        """
        self.queries.append(
            OverpassQuery(filter=filter_op, filter_type=filter_type, query_rhs=value)
        )
        return self

    def admin_level(self, level: int | str) -> OverpassQueryBuilder:
        """Filter by admin_level tag."""
        return self.filter(OverpassFilterType.ADMIN_LEVEL, str(level))

    def iso3166(self, code: str, regex: bool = False) -> OverpassQueryBuilder:
        """Filter by ISO3166-1 country code."""
        op = OverpassFilter.RE if regex else OverpassFilter.EQUAL
        return self.filter(OverpassFilterType.ISO3166_1, code, op)

    def name(self, value: str, regex: bool = False) -> OverpassQueryBuilder:
        """Filter by name tag."""
        op = OverpassFilter.RE if regex else OverpassFilter.EQUAL
        return self.filter(OverpassFilterType.NAME, value, op)

    def boundary(self, value: str) -> OverpassQueryBuilder:
        """Filter by boundary tag."""
        return self.filter(OverpassFilterType.BOUNDARY, value)

    def place(self, value: str) -> OverpassQueryBuilder:
        """Filter by place tag."""
        return self.filter(OverpassFilterType.PLACE, value)

    def name_regex(self, pattern: str) -> OverpassQueryBuilder:
        """Filter by name tag using a regex pattern."""
        return self.name(pattern, regex=True)

    def build(self) -> tuple[OverpassType, list[OverpassQuery]]:
        """Return the (OverpassType, queries) tuple for use in OverpassQL."""
        if not self.queries:
            raise ValueError("Query builder must have at least one filter")
        return (self.osm_type, self.queries.copy())


@dataclass(frozen=True, slots=True)
class OverpassQL:
    """A class to more easily create Overpass QL queries.

    See https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL.
    """

    item_type: list[OverpassItemType]
    queries: list[tuple[OverpassType, list[OverpassQuery]]]
    filename: str = field(init=False, repr=False)
    out_fmt: Literal["csv", "json", "xml"] = "xml"
    out_stmt: Literal["geom", "meta", "body", "skel", "ids", "tags", "count"] = "geom"
    timeout: int = 180
    maxsize: OverpassQueryMem = OverpassQueryMem.DEFAULT

    def __post_init__(self):
        """Validate the OverpassQL configuration."""
        if not self.item_type:
            raise ValueError("item_type must contain at least one item")

        if not all(isinstance(item, OverpassItemType) for item in self.item_type):
            raise TypeError("All items in item_type must be OverpassItemType enums")

        if not self.queries:
            raise ValueError("queries must contain at least one query")

        for i, query_tuple in enumerate(self.queries):
            if not isinstance(query_tuple, tuple) or len(query_tuple) != 2:
                raise TypeError(
                    f"Query at index {i} must be a tuple of "
                    f"(OverpassType, list[OverpassQuery])"
                )

            osm_type, query_list = query_tuple

            if not isinstance(osm_type, OverpassType):
                raise TypeError(
                    f"Query at index {i}: first element must be OverpassType, "
                    f"got {type(osm_type)}"
                )

            if not isinstance(query_list, list):
                raise TypeError(
                    f"Query at index {i}: second element must be a list, "
                    f"got {type(query_list)}"
                )

            if not query_list:
                raise ValueError(
                    f"Query at index {i}: query list must contain at least one "
                    f"OverpassQuery"
                )

            if not all(isinstance(q, OverpassQuery) for q in query_list):
                raise TypeError(
                    f"Query at index {i}: all items in query list must be "
                    f"OverpassQuery instances"
                )

        if self.timeout <= 0:
            raise ValueError(f"timeout must be positive, got {self.timeout}")

        if self.maxsize <= 0:
            raise ValueError(f"maxsize must be positive, got {self.maxsize}")

        query_hash = sha256(self.make_data_query().encode("utf-8")).hexdigest()

        object.__setattr__(
            self,
            "filename",
            f"overpass_{query_hash[:16]}",
        )

    @property
    def filetype(self) -> str:
        """Return the file extension for the output format."""
        return "osm" if self.out_fmt in {"json", "xml"} else self.out_fmt

    def _make_settings(self) -> str:
        """Generate a settings header like [out:xml][timeout:180][maxsize:536870912]."""
        return (
            f"[out:{self.out_fmt}]"
            f"[timeout:{self.timeout}]"
            f"[maxsize:{self.maxsize.value}]"
        )

    def _make_item(self) -> str:
        """Generate the item recursion statement like (._;>;)."""
        items = ";".join(x.value for x in self.item_type)
        return f"({items};)"

    def _make_item_conditionally(self) -> str:
        """Generate the item recursion statement if type is not geom."""
        if self.out_stmt != "geom":
            return f"{self._make_item()};\n"
        return ""

    def _make_queries(self) -> str:
        """Generate the query statements."""
        query_lines = []
        for osm_type, query_list in self.queries:
            filters = "".join(str(q) for q in query_list)
            query_lines.append(f"  {osm_type.value}{filters}")
        return ";\n".join(query_lines)

    def make_data_query(self) -> str:
        """Build the complete Overpass QL query string."""
        return (
            f"{self._make_settings()};\n"
            "(\n"
            f"{self._make_queries()};\n"
            ");\n"
            f"{self._make_item_conditionally()}"
            f"out {self.out_stmt};"
        )


def headers_to_mapping(
    headers: Mapping[str, str] | Iterable[Header] | None,
) -> dict[str, str]:
    """Normalize various header inputs into a plain dict[str, str]."""
    if headers is None:
        return {}
    if isinstance(headers, Mapping):
        return cast(dict[str, str], {k: v for k, v in headers.items()})

    return {h.key: h.value for h in headers}
