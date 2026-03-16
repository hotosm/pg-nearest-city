"""Base code used in async and sync implementation alike."""

import os
from dataclasses import dataclass, field
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
        self.dbname = self.dbname or os.getenv("PGNEAREST_DB_NAME")
        self.user = self.user or os.getenv("PGNEAREST_DB_USER")
        self.password = self.password or os.getenv("PGNEAREST_DB_PASSWORD")
        self.host = self.host or os.getenv("PGNEAREST_DB_HOST", "db")
        self.port = self.port or int(os.getenv("PGNEAREST_DB_PORT", "5432"))

        # Raise error if any required field is missing
        missing_fields = [
            field
            for field in ["dbname", "user", "password"]
            if not getattr(self, field)
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required database config fields: {', '.join(missing_fields)}"
            )

    def get_connection_string(self) -> str:
        """Connection string that psycopg accepts."""
        return (
            f"dbname={self.dbname} user={self.user} password={self.password} "
            f"host={self.host} port={self.port}"
        )


@dataclass
class Location:
    """A location object for JSON serialisation.

    Args:
        city(str): The city.
        country(str): The two-letter ISO 3166-1 alpha-2 country code.
        country_alpha3(str): The three-letter ISO 3166-1 alpha-3 country code.
        country_name(str): The full country name.
        lat(str): Latitude.
        lon(str): Longitude.
    """

    city: str
    country: str
    lat: float
    lon: float
    country_alpha3: Optional[str] = field(default=None)
    country_name: Optional[str] = field(default=None)


@dataclass
class InitializationStatus:
    """Initialization stages as bools."""

    def __init__(self):
        """Initialize the stages as not started."""
        self.has_table: bool = False
        self.has_valid_structure: bool = False
        self.has_data: bool = False
        self.has_country_boundaries: bool = False
        self.has_spatial_index: bool = False

    @property
    def is_fully_initialized(self) -> bool:
        """Check if the database is fully initialized and ready for use."""
        return (
            self.has_table
            and self.has_valid_structure
            and self.has_data
            and self.has_country_boundaries
            and self.has_spatial_index
        )

    def get_missing_components(self) -> list[str]:
        """Return a list of components that are not properly initialized."""
        missing = []
        if not self.has_table:
            missing.append("database table")
        if not self.has_valid_structure:
            missing.append("valid table structure")
        if not self.has_data:
            missing.append("city data")
        if not self.has_country_boundaries:
            missing.append("country boundary geometries")
        if not self.has_spatial_index:
            missing.append("spatial index")
        return missing


class BaseNearestCity:
    """Base class to inherit for sync and async versions."""

    @staticmethod
    def validate_coordinates(lon: float, lat: float) -> Optional[Location]:
        """Check coord in expected EPSG:4326 ranges."""
        if not -90 <= lat <= 90:
            raise ValueError(f"Latitude {lat} is outside valid range [-90, 90]")
        if not -180 <= lon <= 180:
            raise ValueError(f"Longitude {lon} is outside valid range [-180, 180]")

    @staticmethod
    def _get_tableexistence_query() -> sql.SQL:
        """Check if the geocoding table exists via SQL."""
        return sql.SQL(
            """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'geocoding'
                );
            """
        )

    @staticmethod
    def _get_table_structure_query() -> sql.SQL:
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
        """Check data was loaded into correct structure."""
        return sql.SQL(
            """
            SELECT
                (SELECT COUNT(*) FROM geocoding) AS total_cities,
                (SELECT COUNT(*) FROM country WHERE geom IS NOT NULL) AS countries_with_boundaries
            """
        )

    @staticmethod
    def _get_spatial_index_check_query() -> sql.SQL:
        """Check index was created correctly."""
        return sql.SQL(
            """
            SELECT (
                SELECT COUNT(*) FROM pg_indexes
                WHERE tablename = 'geocoding'
                AND indexname IN (
                    'geocoding_geom_idx',
                    'geocoding_country_geom_gist_idx'
                )
            ) = 2;
        """
        )

    @staticmethod
    def _get_reverse_geocoding_query(lon: float, lat: float):
        """The query to do the reverse geocode!

        Uses the && bounding-box operator as a fast pre-filter before the
        precise ST_Contains check, then orders by spherical distance to the
        query point to return the nearest city within the matched country.
        """
        return sql.SQL(
            """
            WITH query_point AS (
              SELECT ST_SetSRID(
                ST_MakePoint({}, {}), 4326) AS geom
            )
            SELECT g.city, c.alpha2, c.alpha3, g.lon, g.lat, c.name
            FROM query_point qp
            JOIN country c ON c.geom && qp.geom
              AND ST_Contains(c.geom, qp.geom)
            JOIN geocoding g ON c.alpha2 = g.country
            ORDER BY g.geom <-> qp.geom
            LIMIT 1
            """
        ).format(sql.Literal(lon), sql.Literal(lat))


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
    GeoTestCase(7.397405, 43.750402, "La Turbie", "FR"),
    GeoTestCase(-79.0647, 43.0896, "Niagara Falls", "US"),
    GeoTestCase(-117.1221, 32.5422, "Imperial Beach", "US"),
    GeoTestCase(-5.3525, 36.1658, "La Línea de la Concepción", "ES"),
    GeoTestCase(12.4534, 41.9033, "Vatican City", "VA"),
    GeoTestCase(-63.0822, 18.0731, "Agrément", "MF"),
    GeoTestCase(-63.0458, 18.0255, "Philipsburg", "SX"),
    GeoTestCase(7.6194, 47.5948, "Weil am Rhein", "DE"),
    GeoTestCase(10.2640, 47.1274, "St Anton am Arlberg", "AT"),
    # Use a point clearly inside the NL enclave to avoid borderline ambiguity.
    GeoTestCase(4.92917, 51.4475, "Baarle-Nassau", "NL"),
    GeoTestCase(-6.3390, 54.1751, "Newry", "GB"),
    GeoTestCase(55.478017, -21.297475, "Saint-Pierre", "RE"),
    GeoTestCase(-6.271183, 55.687669, "Bowmore", "GB"),
    # Very close to India / Nepal border
    GeoTestCase(88.158681, 26.895101, "Mirik", "IN"),
    # GADM ADM_1 exception territories promoted into country boundaries.
    GeoTestCase(121.52639, 25.05306, "Taipei", "TW"),
    GeoTestCase(114.060691, 22.512898, "San Tin", "HK"),
    GeoTestCase(113.54611, 22.20056, "Macau", "MO"),
    GeoTestCase(34.46672, 31.50161, "Gaza", "PS"),
    GeoTestCase(-51.72157, 64.18347, "Nuuk", "GL"),
    GeoTestCase(-6.77164, 62.00973, "Tórshavn", "FO"),
    GeoTestCase(19.93481, 60.09726, "Mariehamn", "AX"),
]
