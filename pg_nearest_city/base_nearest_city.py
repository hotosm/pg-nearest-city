"""Base code used in async and sync implementation alike."""

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
        self.has_table: bool = False
        self.has_valid_structure: bool = False
        self.has_data: bool = False
        self.has_complete_voronoi: bool = False
        self.has_spatial_index: bool = False

    @property
    def is_fully_initialized(self) -> bool:
        """Check if the database is fully initialized and ready for use."""
        return (
            self.has_table
            and self.has_valid_structure
            and self.has_data
            and self.has_complete_voronoi
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
        if not self.has_complete_voronoi:
            missing.append("complete Voronoi polygons")
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
        """Check if a table exists via SQL."""
        return sql.SQL(
            """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'pg_nearest_city_geocoding'
                );
            """
        )

    @staticmethod
    def _get_table_structure_query() -> sql.SQL:
        """Get the fields from the pg_nearest_city_geocoding table."""
        return sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'pg_nearest_city_geocoding'
        """
        )

    @staticmethod
    def _get_data_completeness_query() -> sql.SQL:
        """Check data was loaded into correct structure."""
        return sql.SQL(
            """
            SELECT
                COUNT(*) as total_cities,
                COUNT(*) FILTER (WHERE voronoi IS NOT NULL) as cities_with_voronoi
            FROM pg_nearest_city_geocoding;
        """
        )

    @staticmethod
    def _get_spatial_index_check_query() -> sql.SQL:
        """Check index was created correctly."""
        return sql.SQL(
            """
            SELECT EXISTS (
                SELECT FROM pg_indexes
                WHERE tablename = 'pg_nearest_city_geocoding'
                AND indexname = 'geocoding_voronoi_idx'
            );
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
            JOIN country c ON c.geom && qp.geom
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
    expected coutnry: ISO 3166-1 alpha2 code of the country expected

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
    GeoTestCase(-63.0822, 18.0731, "Marigot", "MF"),
    GeoTestCase(-63.11852, 18.03783, "Simpson Bay Village", "SX"),
    GeoTestCase(-63.0458, 18.0255, "Philipsburg", "SX"),
    GeoTestCase(7.6194, 47.5948, "Weil am Rhein", "DE"),
    GeoTestCase(10.2640, 47.1274, "St Anton am Arlberg", "AT"),
    GeoTestCase(4.9312, 51.4478, "Baarle-Nassau", "NL"),
    GeoTestCase(-6.3390, 54.1751, "Newry", "GB"),
    GeoTestCase(55.478017, -21.297475, "Saint-Pierre", "RE"),
    GeoTestCase(-6.271183, 55.687669, "Bowmore", "GB"),
    GeoTestCase(88.136284, 26.934422, "Mirik", "IN"),
]
