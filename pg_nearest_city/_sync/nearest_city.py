"""Main logic."""

import gzip
import importlib.resources
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg
from psycopg import Cursor

from pg_nearest_city.base_nearest_city import (
    BaseNearestCity,
    DbConfig,
    InitializationStatus,
    Location,
)

logger = logging.getLogger("pg_nearest_city")


class NearestCity:
    """Reverse geocoding to the nearest city over 1000 population."""

    @classmethod
    @contextmanager
    def connect(
        cls,
        db: Optional[psycopg.Connection | DbConfig] = None,
    ):
        """Managed NearestCity instance with automatic initialization and cleanup.

        Args:
            db: Either a DbConfig for a new connection or an existing
                psycopg Connection. If neither are passed, attempts to extract
                variables from system environment, else uses defaults.
        """
        is_external_connection = isinstance(db, psycopg.Connection)
        is_db_config = isinstance(db, DbConfig)

        conn: psycopg.Connection

        if is_external_connection:
            conn = db
        elif is_db_config:
            conn = psycopg.Connection.connect(db.get_connection_string())
        else:
            # Fallback to env var extraction, or defaults for testing
            conn = psycopg.Connection.connect(
                DbConfig().get_connection_string()
            )

        geocoder = cls(conn)

        try:
            geocoder.initialize()
            yield geocoder
        finally:
            if not is_external_connection:
                conn.close()

    def __init__(
        self,
        connection: psycopg.Connection,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize reverse geocoder with an existing Connection.

        Args:
            db: An existing psycopg Connection
            connection: psycopg.Connection
            logger: Optional custom logger. If not provided, uses package logger
        """
        # Allow users to provide their own logger while having a sensible default
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self.connection = connection

        with importlib.resources.path(
            "pg_nearest_city.data", "cities_1000_simple.txt.gz"
        ) as cities_path:
            self.cities_file = cities_path
        with importlib.resources.path(
            "pg_nearest_city.data", "voronois.wkb.gz"
        ) as voronoi_path:
            self.voronoi_file = voronoi_path

    def initialize(self) -> None:
        """Initialize the geocoding database with validation checks."""
        try:
            with self.connection.cursor() as cur:
                self._logger.info("Starting database initialization check")
                status = self._check_initialization_status(cur)

                if status.is_fully_initialized:
                    self._logger.info("Database already properly initialized")
                    return

                if status.has_table and not status.is_fully_initialized:
                    missing = status.get_missing_components()
                    self._logger.warning(
                        "Database needs repair. Missing components: %s",
                        ", ".join(missing),
                    )
                    self._logger.info("Reinitializing from scratch")
                    cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")

                self._logger.info("Creating geocoding table")
                self._create_geocoding_table(cur)

                self._logger.info("Importing city data")
                self._import_cities(cur)

                self._logger.info("Processing Voronoi polygons")
                self._import_voronoi_polygons(cur)

                self._logger.info("Creating spatial index")
                self._create_spatial_index(cur)

                self.connection.commit()

                self._logger.debug("Verifying final initialization state")
                final_status = self._check_initialization_status(cur)
                if not final_status.is_fully_initialized:
                    missing = final_status.get_missing_components()
                    self._logger.error(
                        "Initialization failed final validation. Missing: %s",
                        ", ".join(missing),
                    )
                    raise RuntimeError(
                        "Initialization failed final validation. "
                        f"Missing components: {', '.join(missing)}"
                    )

                self._logger.info("Initialization complete and verified")

        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}") from e

    def query(self, lat: float, lon: float) -> Optional[Location]:
        """Find the nearest city to the given coordinates using Voronoi regions.

        Args:
            lat: Latitude in degrees (-90 to 90)
            lon: Longitude in degrees (-180 to 180)

        Returns:
            Location object if a matching city is found, None otherwise

        Raises:
            ValueError: If coordinates are out of valid ranges
            RuntimeError: If database query fails
        """
        # Validate coordinate ranges
        BaseNearestCity.validate_coordinates(lon, lat)

        try:
            with self.connection.cursor() as cur:
                cur.execute(
                    BaseNearestCity._get_reverse_geocoding_query(lon, lat)
                )
                result = cur.fetchone()

                if not result:
                    return None

                return Location(
                    city=result[0],
                    country=result[1],
                    lat=float(result[2]),
                    lon=float(result[3]),
                )
        except Exception as e:
            self._logger.error(f"Reverse geocoding failed: {str(e)}")
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}") from e

    def _check_initialization_status(
        self, cur: psycopg.Cursor
    ) -> InitializationStatus:
        """Check the status and integrity of the geocoding database.

        Performs essential validation checks to ensure the database is
        properly initialized and contains valid data.
        """
        status = InitializationStatus()

        # Check table existence
        cur.execute(BaseNearestCity._get_tableexistence_query())
        table_exists = cur.fetchone()
        status.has_table = bool(table_exists and table_exists[0])

        # If table doesn't exist, we can't check other properties
        if not status.has_table:
            return status

        # Check table structure
        cur.execute(BaseNearestCity._get_table_structure_query())
        columns = {col: dtype for col, dtype in cur.fetchall()}
        expected_columns = {
            "city": "character varying",
            "country": "character varying",
            "lat": "numeric",
            "lon": "numeric",
            "geom": "geometry",
            "voronoi": "geometry",
        }
        status.has_valid_structure = all(col in columns for col in expected_columns)
        # If table doesn't have valid structure, we can't check other properties
        if not status.has_valid_structure:
            return status

        # Check data completeness
        cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = cur.fetchone()
        total_cities, cities_with_voronoi = counts

        status.has_data = total_cities > 0
        status.has_complete_voronoi = cities_with_voronoi == total_cities

        # Check spatial index
        cur.execute(BaseNearestCity._get_spatial_index_check_query())
        has_index = cur.fetchone()
        status.has_spatial_index = bool(has_index and has_index[0])

        return status

    def _import_cities(self, cur: Cursor):
        if not self.cities_file.exists():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")

        """Import city data using COPY protocol."""
        with cur.copy(
            "COPY pg_nearest_city_geocoding(city, country, lat, lon) FROM STDIN"
        ) as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    copy.write(data)
                    copied_bytes += len(data)
                self._logger.info(f"Imported {copied_bytes:,} bytes of city data")

    def _create_geocoding_table(self, cur: Cursor):
        """Create the main table."""
        cur.execute("""
            CREATE TABLE pg_nearest_city_geocoding (
                city varchar,
                country varchar,
                lat decimal,
                lon decimal,
                geom geometry(Point,4326)
                  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326))
                  STORED,
                voronoi geometry(Polygon,4326)
            );
        """)

    def _import_voronoi_polygons(self, cur: Cursor):
        """Import and integrate Voronoi polygons into the main table."""
        if not self.voronoi_file.exists():
            raise FileNotFoundError(f"Voronoi file not found: {self.voronoi_file}")

        # First create temporary table for the import
        cur.execute("""
            CREATE TEMP TABLE voronoi_import (
                city text,
                country text,
                wkb bytea
            );
        """)

        # Import the binary WKB data
        with cur.copy(
            "COPY voronoi_import (city, country, wkb) FROM STDIN"
        ) as copy:
            with gzip.open(self.voronoi_file, "rb") as f:
                while data := f.read(8192):
                    copy.write(data)

        # Update main table with Voronoi geometries
        cur.execute("""
            UPDATE pg_nearest_city_geocoding g
            SET voronoi = ST_GeomFromWKB(v.wkb, 4326)
            FROM voronoi_import v
            WHERE g.city = v.city
            AND g.country = v.country;
        """)

        # Clean up temporary table
        cur.execute("DROP TABLE voronoi_import;")

    def _create_spatial_index(self, cur: Cursor):
        """Create a spatial index on the Voronoi polygons for efficient queries."""
        cur.execute("""
            CREATE INDEX geocoding_voronoi_idx
            ON pg_nearest_city_geocoding
            USING GIST (voronoi);
        """)
