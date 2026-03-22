"""Base code used in async and sync implementation alike."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from psycopg import sql

from pg_nearest_city.utils import COMPRESSION_EXTENSIONS


@dataclass
class DbConfig:
    """Database config for Postgres.

    Allows overriding values via constructor parameters, fallback to env vars.


    Args:
        dbname(str): Database name.
        user(str): Database user.
        password(str): Database password.
        host(str): Database host.
        port(str): Database port.
    """

    dbname: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None

    def __post_init__(self):
        """Ensures env variables are read at runtime, not at class definition."""
        self.dbname = self.dbname or os.getenv("PGNEAREST_DB_NAME", "postgres")
        self.user = self.user or os.getenv("PGNEAREST_DB_USER")
        self.password = self.password or os.getenv("PGNEAREST_DB_PASSWORD")
        self.host = self.host or os.getenv("PGNEAREST_DB_HOST", "localhost")
        self.port = self.port or int(os.getenv("PGNEAREST_DB_PORT", "5432"))

        # Raise error if any required field is missing
        missing_fields = [
            field for field in ["dbname", "user"] if not getattr(self, field)
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required database config fields: {', '.join(missing_fields)}"
            )
        if not self.password:
            logging.getLogger("pg_nearest_city").debug(
                "No database password configured; connecting without a password"
            )

    def get_connection_string(self) -> str:
        """Connection string that psycopg accepts."""
        parts = [f"dbname={self.dbname}", f"user={self.user}"]
        if self.password:
            parts.append(f"password={self.password}")
        parts.extend([f"host={self.host}", f"port={self.port}"])
        return " ".join(parts)


@dataclass
class Location:
    """A location object for JSON serialisation.

    Args:
        city(str): The city.
        country(str): The country.
        lat(str): Latitude.
        lon(str): Longitude.
    """

    city: str
    country: str
    lat: float
    lon: float


@dataclass
class InitializationStatus:
    """Initialization stages as bools."""

    def __init__(self):
        """Initialize the stages as not started."""
        self.has_country_table: bool = False
        self.has_geocoding_table: bool = False
        self.has_country_data: bool = False
        self.has_geocoding_data: bool = False
        self.has_spatial_index: bool = False

    @property
    def is_fully_initialized(self) -> bool:
        """Check if the database is fully initialized and ready for use."""
        return (
            self.has_country_table
            and self.has_geocoding_table
            and self.has_country_data
            and self.has_geocoding_data
            and self.has_spatial_index
        )

    def get_missing_components(self) -> list[str]:
        """Return a list of components that are not properly initialized."""
        missing = []
        if not self.has_country_table:
            missing.append("country table")
        if not self.has_geocoding_table:
            missing.append("geocoding table")
        if not self.has_country_data:
            missing.append("country boundary data")
        if not self.has_geocoding_data:
            missing.append("geocoding city data")
        if not self.has_spatial_index:
            missing.append("spatial index")
        return missing


class BaseNearestCity:
    """Base class to inherit for sync and async versions."""

    # Data file stems (compression extension varies)
    COUNTRY_DATA_STEM = "country.csv"
    GEOCODING_DATA_STEM = "geocoding.csv"

    # Default data directory locations for auto-import
    DATA_ENV_VAR = "PGNEAREST_DATA_PATH"
    DATA_PACKAGE_PATH = str(Path(__file__).parent / "data")
    DATA_FALLBACK_PATH = "/data/pg_nearest_city"

    # COPY statements for importing data
    COPY_COUNTRY_FROM = (
        "COPY country(alpha2, alpha3, name, geom) FROM STDIN WITH (FORMAT CSV)"
    )
    COPY_GEOCODING_FROM = (
        "COPY geocoding(city, country, lat, lon) FROM STDIN WITH (FORMAT CSV)"
    )

    @staticmethod
    def validate_coordinates(lon: float, lat: float) -> Optional[Location]:
        """Check coord in expected EPSG:4326 ranges."""
        if not -90 <= lat <= 90:
            raise ValueError(f"Latitude {lat} is outside valid range [-90, 90]")
        if not -180 <= lon <= 180:
            raise ValueError(f"Longitude {lon} is outside valid range [-180, 180]")

    @staticmethod
    def _get_tables_existence_query() -> sql.SQL:
        """Check if country and geocoding tables exist."""
        return sql.SQL(
            """
            SELECT
                EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'country'
                ) AS has_country,
                EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'geocoding'
                ) AS has_geocoding;
            """
        )

    @staticmethod
    def _get_country_structure_query() -> sql.SQL:
        """Get the fields from the country table."""
        return sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'country'
        """
        )

    @staticmethod
    def _get_geocoding_structure_query() -> sql.SQL:
        """Get the fields from the geocoding table."""
        return sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'geocoding'
        """
        )

    @staticmethod
    def _get_data_completeness_query() -> sql.SQL:
        """Check data was loaded into both tables."""
        return sql.SQL(
            """
            SELECT
                (SELECT COUNT(*) FROM country WHERE geom IS NOT NULL) AS country_count,
                (SELECT COUNT(*) FROM geocoding) AS geocoding_count;
        """
        )

    @staticmethod
    def _get_spatial_index_check_query() -> sql.SQL:
        """Check spatial indices exist on both tables."""
        return sql.SQL(
            """
            SELECT
                EXISTS (
                    SELECT FROM pg_indexes
                    WHERE tablename = 'country'
                    AND indexdef LIKE '%gist%'
                ) AS has_country_idx,
                EXISTS (
                    SELECT FROM pg_indexes
                    WHERE tablename = 'geocoding'
                    AND indexdef LIKE '%gist%'
                ) AS has_geocoding_idx;
            """
        )

    @staticmethod
    def _get_reverse_geocoding_query(lon: float, lat: float):
        """The query to do the reverse geocode!"""
        return sql.SQL(
            """
            WITH query_point AS (
              SELECT ST_SetSRID(
                ST_MakePoint({}, {}), 4326) AS geom
            )
            SELECT g.city, g.country, g.lon, g.lat
            FROM query_point qp
            JOIN country c ON ST_Covers(c.geom, qp.geom)
            CROSS JOIN LATERAL (
              SELECT city, country, lon, lat, geom
              FROM geocoding
              WHERE country = c.alpha2
              ORDER BY geom <-> qp.geom
              LIMIT 1
            ) g
            """
        ).format(sql.Literal(lon), sql.Literal(lat))

    @staticmethod
    def _find_data_file(directory: Path, stem: str) -> Path | None:
        """Find a data file in *directory* with any supported compression extension."""
        for ext in COMPRESSION_EXTENSIONS:
            path = directory / f"{stem}{ext}"
            if path.is_file():
                return path
        return None

    @staticmethod
    def _find_data_path(constructor_path: str | None = None) -> str | None:
        """Find a data directory containing exporteded CSV files."""
        candidates: list[str] = []
        if constructor_path:
            candidates.append(constructor_path)
        env_path = os.getenv(BaseNearestCity.DATA_ENV_VAR)
        if env_path:
            candidates.append(env_path)
        candidates.append(BaseNearestCity.DATA_PACKAGE_PATH)
        candidates.append(BaseNearestCity.DATA_FALLBACK_PATH)

        for path_str in candidates:
            d = Path(path_str)
            if not d.is_dir():
                continue
            country = BaseNearestCity._find_data_file(
                d, BaseNearestCity.COUNTRY_DATA_STEM
            )
            geocoding = BaseNearestCity._find_data_file(
                d, BaseNearestCity.GEOCODING_DATA_STEM
            )
            if country and geocoding:
                return path_str
        return None

    @staticmethod
    def _get_bootstrap_sql() -> list[bytes]:
        """Return SQL statements that create the permanent tables from scratch.

        Returns bytes because psycopg's sql.SQL requires LiteralString,
        but these are built dynamically from table definitions. bytes
        is accepted by cur.execute() without the LiteralString constraint.
        """
        from pg_nearest_city.db.tables import get_tables_in_creation_order

        tables = get_tables_in_creation_order(filters=[{"is_temp": False}])

        stmts: list[bytes] = [
            b"CREATE EXTENSION IF NOT EXISTS postgis",
            b"CREATE EXTENSION IF NOT EXISTS btree_gist",
        ]
        for table_cls in reversed(tables):
            stmts.append(
                sql.SQL("DROP TABLE IF EXISTS {tbl}")
                .format(tbl=sql.Identifier(table_cls.name))
                .as_string(None)
                .encode()
            )
        for table_cls in tables:
            stmts.append(
                table_cls.create_sql()
                .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                .encode()
            )
        return stmts

    @staticmethod
    def _get_bootstrap_index_sql() -> list[bytes]:
        """Return SQL statements that create bootstrap indices."""
        from pg_nearest_city.db.tables import get_tables_in_creation_order

        return [
            idx.index_def.encode()
            for t in get_tables_in_creation_order(filters=[{"is_temp": False}])
            for idx in t.get_indices()
        ]


@dataclass
class GeoTestCase:
    """Class representing points with their expected values.

    Test cases should meet one or more of the following critiera:
        1. Near country borders
        2. Islands
        3. Enclaves or exclaves
        4. Split cities
        5. Overseas territories

    All longitude / latitudes are in EPSG 4326.

    lon: longitude
    lat: latitude
    expected city: name of city expected, exactly as stored in the DB
    expected country: ISO 3166-1 alpha2 code of the country expected
    """

    lon: float
    lat: float
    expected_city: str
    expected_country: str


# Points near land borders where correct country assignment depends on
# boundary precision.  Includes tripoints, twin cities, and split cities.
_border_region_cases: list[GeoTestCase] = [
    GeoTestCase(7.397405, 43.750402, "La Turbie", "FR"),  # FR/MC border
    GeoTestCase(-79.0647, 43.0896, "Niagara Falls", "US"),  # US/CA border
    GeoTestCase(-117.1221, 32.5422, "Imperial Beach", "US"),  # US/MX border
    GeoTestCase(-5.3525, 36.1658, "La Línea de la Concepción", "ES"),  # ES/GI border
    GeoTestCase(7.6194, 47.5948, "Weil am Rhein", "DE"),  # DE/CH/FR tripoint
    GeoTestCase(10.2640, 47.1274, "St Anton am Arlberg", "AT"),  # AT/CH border
    GeoTestCase(-6.3390, 54.1751, "Newry", "GB"),  # GB/IE border
    GeoTestCase(88.190070, 26.888170, "Mirik", "IN"),  # IN/NP border
    GeoTestCase(87.26525, 26.39905, "Jogbani", "IN"),  # IN/NP border
    GeoTestCase(91.45407, 23.25178, "Belonia", "IN"),  # IN/BD border
    GeoTestCase(75.02787, 32.03733, "Derā Nānak", "IN"),  # IN/PK border (Punjab)
    GeoTestCase(94.30073, 24.25080, "Moreh", "IN"),  # IN/MM border
    GeoTestCase(89.37558, 26.84766, "Jaigaon", "IN"),  # IN/BT border
    GeoTestCase(
        -72.099238, 45.00505, "Derby Line", "US"
    ),  # US/CA, border through buildings
    GeoTestCase(13.6242, 45.9564, "Gorizia", "IT"),  # IT/SI split city
    GeoTestCase(13.6458, 45.9558, "Nova Gorica", "SI"),  # IT/SI split city
    GeoTestCase(24.1336, 65.8353, "Haparanda", "SE"),  # SE/FI twin cities
    GeoTestCase(24.1775, 65.8494, "Tornio", "FI"),  # SE/FI twin cities
    GeoTestCase(
        -6.271183, 55.687669, "Bowmore", "GB"
    ),  # Islay, tests island boundary coverage
    GeoTestCase(114.06932, 22.50166, "San Tin", "HK"),  # HK/CN border
    GeoTestCase(114.0683, 22.54554, "Shenzhen", "CN"),  # CN/HK border
    GeoTestCase(20.86667, 42.88333, "Mitrovicë", "XK"),  # XK/RS border (split city)
    GeoTestCase(21.58333, 42.84306, "Medveđa", "RS"),  # RS/XK border
]

# Enclaves, exclaves, and non-contiguous territories only accessible through
# or entirely surrounded by another country.
_enclave_exclave_cases: list[GeoTestCase] = [
    GeoTestCase(
        4.929431, 51.445820, "Baarle-Nassau", "NL"
    ),  # NL/BE interleaved enclaves
    GeoTestCase(4.931858, 51.440628, "Baarle-Hertog", "BE"),  # BE exclave in NL
    GeoTestCase(8.6903, 47.6961, "Büsingen am Hochrhein", "DE"),  # DE exclave in CH
    GeoTestCase(8.9719, 45.9686, "Campione", "IT"),  # IT exclave in CH
    GeoTestCase(1.9842, 42.4742, "Llívia", "ES"),  # ES exclave in FR
    GeoTestCase(
        10.4472, 47.5761, "Jungholz", "AT"
    ),  # AT quasi-exclave, single-point connection
    GeoTestCase(
        10.17197, 47.35127, "Mittelberg", "AT"
    ),  # Kleinwalsertal, road-accessible only via DE
    GeoTestCase(20.5072, 54.7104, "Kaliningrad", "RU"),  # RU exclave between PL and LT
    GeoTestCase(
        -123.0586, 48.9884, "Point Roberts", "US"
    ),  # US exclave, accessible only via CA
    GeoTestCase(-5.3088, 35.8867, "Ceuta", "ES"),  # ES autonomous city in North Africa
    GeoTestCase(
        -2.9383, 35.2923, "Melilla", "ES"
    ),  # ES autonomous city in North Africa
    GeoTestCase(124.3667, -9.2, "Pante Macassar", "TL"),  # Oecusse, TL exclave in ID
]

# Very small sovereign states where boundary geometry precision is critical.
_microstate_cases: list[GeoTestCase] = [
    GeoTestCase(12.4534, 41.9033, "Vatican City", "VA"),
    GeoTestCase(12.44639, 43.93667, "San Marino", "SM"),
    GeoTestCase(7.42145, 43.73718, "Monaco", "MC"),
    GeoTestCase(9.5167, 47.1333, "Vaduz", "LI"),
    GeoTestCase(6.13268, 49.60982, "Luxembourg", "LU"),
    GeoTestCase(1.5218, 42.5075, "Andorra la Vella", "AD"),
    GeoTestCase(-5.35257, 36.14474, "Gibraltar", "GI"),
]

# Islands divided between or shared by multiple sovereign states.
_split_island_cases: list[GeoTestCase] = [
    GeoTestCase(-63.0822, 18.0731, "Agrément", "MF"),  # Saint-Martin (FR side)
    GeoTestCase(
        -63.11852, 18.03783, "Simpson Bay Village", "SX"
    ),  # Sint Maarten (NL side)
    GeoTestCase(-63.0458, 18.0255, "Philipsburg", "SX"),  # Sint Maarten (NL side)
    GeoTestCase(33.3642, 35.1856, "Nicosia", "CY"),  # Cyprus / Northern Cyprus
    GeoTestCase(
        114.94006, 4.89035, "Bandar Seri Begawan", "BN"
    ),  # Borneo tripoint (MY/BN/ID)
]

# Overseas territories, dependencies, and special administrative regions
# that must be recognised as distinct from their parent state.
_overseas_territory_cases: list[GeoTestCase] = [
    GeoTestCase(-52.3261, 4.9224, "Cayenne", "GF"),  # French Guiana
    GeoTestCase(-61.026394, 14.613134, "Le Lamentin", "MQ"),  # Martinique
    GeoTestCase(-61.5339, 16.2411, "Pointe-à-Pitre", "GP"),  # Guadeloupe
    GeoTestCase(-68.9900, 12.1696, "Sint Michiel Liber", "CW"),  # Curaçao
    GeoTestCase(-70.02703, 12.52398, "Oranjestad", "AW"),  # Aruba
    GeoTestCase(-57.8517, -51.6975, "Stanley", "FK"),  # Falkland Islands
    GeoTestCase(55.478017, -21.297475, "Saint-Pierre", "RE"),  # Réunion
    GeoTestCase(114.17469, 22.27832, "Hong Kong", "HK"),  # HK SAR
    GeoTestCase(113.5439, 22.1987, "Macau", "MO"),  # MO SAR
    GeoTestCase(45.22878, -12.78234, "Mamoudzou", "YT"),  # Mayotte
]

geo_test_cases: list[GeoTestCase] = (
    _border_region_cases
    + _enclave_exclave_cases
    + _microstate_cases
    + _split_island_cases
    + _overseas_territory_cases
)
