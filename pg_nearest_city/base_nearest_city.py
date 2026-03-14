"""Base code used in async and sync implementation alike."""

import logging
import os
from dataclasses import dataclass
from typing import Optional

from psycopg import sql


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

    # Default dump file locations for auto-import
    DUMP_ENV_VAR = "PGNEAREST_DUMP_PATH"
    DUMP_FALLBACK_PATH = "/data/pg_nearest_city.dump"

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
    def _find_dump_path(constructor_path: str | None = None) -> str | None:
        """Find a dump file for auto-import."""
        candidates = []
        if constructor_path:
            candidates.append(constructor_path)
        env_path = os.getenv(BaseNearestCity.DUMP_ENV_VAR)
        if env_path:
            candidates.append(env_path)
        candidates.append(BaseNearestCity.DUMP_FALLBACK_PATH)

        for path in candidates:
            if os.path.isfile(path):
                return path
        return None


@dataclass
class GeoTestCase:
    """Class representing points with their expected values.

    The given points lie either close to country borders,
    are islands, or both (St. Martin / Sint Marteen).

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


geo_test_cases: list[GeoTestCase] = [
    # === EXISTING ===
    GeoTestCase(7.397405, 43.750402, "La Turbie", "FR"),
    GeoTestCase(-79.0647, 43.0896, "Niagara Falls", "US"),
    GeoTestCase(-117.1221, 32.5422, "Imperial Beach", "US"),
    GeoTestCase(-5.3525, 36.1658, "La Línea de la Concepción", "ES"),
    GeoTestCase(12.4534, 41.9033, "Vatican City", "VA"),
    GeoTestCase(-63.0822, 18.0731, "Agrément", "MF"),
    GeoTestCase(-63.11852, 18.03783, "Simpson Bay Village", "SX"),
    GeoTestCase(-63.0458, 18.0255, "Philipsburg", "SX"),
    GeoTestCase(7.6194, 47.5948, "Weil am Rhein", "DE"),
    GeoTestCase(10.2640, 47.1274, "St Anton am Arlberg", "AT"),
    GeoTestCase(4.929431, 51.445820, "Baarle-Nassau", "NL"),
    GeoTestCase(-6.3390, 54.1751, "Newry", "GB"),
    GeoTestCase(55.478017, -21.297475, "Saint-Pierre", "RE"),
    GeoTestCase(-6.271183, 55.687669, "Bowmore", "GB"),
    GeoTestCase(88.136284, 26.934422, "Mirik", "IN"),
    GeoTestCase(114.060691, 22.512898, "San Tin", "HK"),
    # === ENCLAVES / EXCLAVES ===
    # Baarle-Hertog (Belgian exclave in Netherlands)
    GeoTestCase(4.931858, 51.440628, "Baarle-Hertog", "BE"),
    # Büsingen (German exclave in Switzerland)
    GeoTestCase(8.6903, 47.6961, "Büsingen am Hochrhein", "DE"),
    # Campione d'Italia (Italian exclave in Switzerland)
    GeoTestCase(8.9719, 45.9686, "Campione", "IT"),
    # Llívia (Spanish exclave in France)
    GeoTestCase(1.9794, 42.4664, "Llívia", "ES"),
    # Jungholz (Austrian quasi-exclave, connected only at a point)
    GeoTestCase(10.4472, 47.5761, "Jungholz", "AT"),
    # Kleinwalsertal (Austrian, road-accessible only via Germany)
    GeoTestCase(10.17197, 47.35127, "Mittelberg", "AT"),
    # === MICROSTATES ===
    GeoTestCase(12.44639, 43.93667, "San Marino", "SM"),
    GeoTestCase(7.42145, 43.73718, "Monaco", "MC"),
    GeoTestCase(9.5167, 47.1333, "Vaduz", "LI"),
    GeoTestCase(6.13268, 49.60982, "Luxembourg", "LU"),
    GeoTestCase(1.5218, 42.5075, "Andorra la Vella", "AD"),
    # === DISPUTED / COMPLEX BORDERS ===
    # Gibraltar (UK overseas territory)
    GeoTestCase(-5.35257, 36.14474, "Gibraltar", "GI"),
    # Ceuta (Spanish city in North Africa)
    GeoTestCase(-5.3088, 35.8867, "Ceuta", "ES"),
    # Melilla (Spanish city in North Africa)
    GeoTestCase(-2.9383, 35.2923, "Melilla", "ES"),
    # Kaliningrad (Russian exclave)
    GeoTestCase(20.5072, 54.7104, "Kaliningrad", "RU"),
    # === ISLANDS WITH SPLIT SOVEREIGNTY ===
    # Timor-Leste / Indonesia (Oecusse exclave)
    GeoTestCase(124.3667, -9.2, "Pante Macassar", "TL"),
    # Cyprus / Northern Cyprus
    GeoTestCase(33.3642, 35.1856, "Nicosia", "CY"),
    # Borneo tripoint area (MY/BN/ID)
    GeoTestCase(114.94006, 4.89035, "Bandar Seri Begawan", "BN"),
    # === OVERSEAS TERRITORIES ===
    # French Guiana
    GeoTestCase(-52.3261, 4.9224, "Cayenne", "GF"),
    # Martinique
    GeoTestCase(-61.026394, 14.613134, "Le Lamentin", "MQ"),
    # Guadeloupe
    GeoTestCase(-61.5339, 16.2411, "Pointe-à-Pitre", "GP"),
    # Curaçao
    GeoTestCase(-68.9900, 12.1696, "Sint Michiel Liber", "CW"),
    # Aruba
    GeoTestCase(-70.02703, 12.52398, "Oranjestad", "AW"),
    # Falkland Islands
    GeoTestCase(-57.8517, -51.6975, "Stanley", "FK"),
    # === TRICKY BORDER REGIONS ===
    # Point Roberts (US exclave accessible only via Canada)
    GeoTestCase(-123.0586, 48.9884, "Point Roberts", "US"),
    # Stanstead/Derby Line (US/Canada border runs through buildings)
    GeoTestCase(-72.099238, 45.005667, "Derby Line", "US"),
    # Gorizia/Nova Gorica (IT/SI, city split by border)
    GeoTestCase(13.6242, 45.9564, "Gorizia", "IT"),
    GeoTestCase(13.6458, 45.9558, "Nova Gorica", "SI"),
    # Haparanda/Tornio (SE/FI twin cities)
    GeoTestCase(24.1336, 65.8353, "Haparanda", "SE"),
    GeoTestCase(24.1775, 65.8494, "Tornio", "FI"),
    # === HONG KONG / MACAU / CHINA ===
    GeoTestCase(114.17469, 22.27832, "Hong Kong", "HK"),
    GeoTestCase(113.5439, 22.1987, "Macau", "MO"),
    GeoTestCase(114.0683, 22.54554, "Shenzhen", "CN"),
]
