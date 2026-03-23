"""Main logic."""

import logging
from pathlib import Path
from textwrap import dedent, fill
from typing import Optional

import psycopg

from pg_nearest_city.base_nearest_city import (
    BaseNearestCity,
    DbConfig,
    InitializationStatus,
    Location,
)
from pg_nearest_city.utils import open_compressed

logger = logging.getLogger("pg_nearest_city")


class NearestCity:
    """Reverse geocoding to the nearest city over 1000 population."""

    connection: psycopg.Connection

    def __init__(
        self,
        db: psycopg.Connection | DbConfig | None = None,
        logger: Optional[logging.Logger] = None,
        data_path: str | None = None,
    ):
        """Initialize reverse geocoder.

        Args:
            db: An existing psycopg Connection or DbConfig
            logger: Optional custom logger.
            data_path: Optional path to directory containing exported CSV data files.
        """
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self._db = db
        self._data_path = data_path
        self._is_external_connection = False
        self._is_initialized = False

    def __enter__(self):
        """Open the context manager."""
        self.connection = self.get_connection(self._db)
        self.initialize()
        self._is_initialized = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the context manager."""
        if self.connection and not self._is_external_connection:
            self.connection.close()
        self._is_initialized = False

    def get_connection(
        self,
        db: Optional[psycopg.Connection | DbConfig] = None,
    ) -> psycopg.Connection:
        """Determine the database connection to use."""
        if isinstance(db, psycopg.Connection):
            self._is_external_connection = True
            return db
        self._is_external_connection = False
        if isinstance(db, DbConfig):
            return psycopg.Connection.connect(db.get_connection_string())
        return psycopg.Connection.connect(
            DbConfig().get_connection_string(),
        )

    def initialize(self) -> None:
        """Initialize the geocoding database with validation checks.

        Checks for country and geocoding tables. If not present,
        attempts auto-import from exported CSV data files.

        Uses a PostgreSQL advisory lock to prevent concurrent bootstrap races
        when multiple processes start up simultaneously.
        """
        if not getattr(self, "connection", None):
            self._inform_user_if_not_context_manager()

        # "pnc\0" as a 32-bit integer advisory lock ID
        _advisory_lock_id = 0x706E6300

        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (_advisory_lock_id,))

            self._logger.info("Starting database initialization check")
            with self.connection.cursor() as cur:
                status = self._check_initialization_status(cur)

            if status.is_fully_initialized:
                self._logger.info("Database already properly initialized")
                return

            missing = status.get_missing_components()

            # Attempt auto-import from data directory
            data_path = BaseNearestCity._find_data_path(self._data_path)
            if data_path:
                self._logger.warning(
                    "Database not ready (missing: %s); importing from: %s",
                    ", ".join(missing),
                    data_path,
                )
                self._import_from_data(data_path)

                # Re-check after import
                with self.connection.cursor() as cur:
                    status = self._check_initialization_status(cur)
                if status.is_fully_initialized:
                    self._logger.warning("Database initialized from data files")
                    return

            raise RuntimeError(
                "Database is not initialized and no data files found. "
                "Run the bootstrap pipeline first (pgnearest-load), "
                f"or provide a data directory via {BaseNearestCity.DATA_ENV_VAR} "
                "env var or data_path constructor arg."
            )

        except RuntimeError:
            raise
        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}") from e
        finally:
            with self.connection.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (_advisory_lock_id,))

    def _import_from_data(self, data_path: str) -> None:
        """Bootstrap the database from exported CSV data files."""
        data_dir = Path(data_path)
        country_file = BaseNearestCity._find_data_file(
            data_dir, BaseNearestCity.COUNTRY_DATA_STEM
        )
        geocoding_file = BaseNearestCity._find_data_file(
            data_dir, BaseNearestCity.GEOCODING_DATA_STEM
        )
        if not country_file or not geocoding_file:
            raise RuntimeError(f"Data files not found in {data_path}")

        with self.connection.cursor() as cur:
            # Create schema
            for stmt in BaseNearestCity._get_bootstrap_sql():
                cur.execute(stmt)
            self.connection.commit()

            # Import country data (alpha2, alpha3, name, geom as hex WKB)
            self._logger.info("Importing country data from %s", country_file.name)
            with open_compressed(country_file) as fh:
                with cur.copy(BaseNearestCity.COPY_COUNTRY_FROM) as copy:
                    while data := fh.read(8192):
                        copy.write(data)
            self.connection.commit()

            # Import geocoding data (city, country, lat, lon)
            self._logger.info("Importing geocoding data from %s", geocoding_file.name)
            with open_compressed(geocoding_file) as fh:
                with cur.copy(BaseNearestCity.COPY_GEOCODING_FROM) as copy:
                    while data := fh.read(8192):
                        copy.write(data)
            self.connection.commit()

            # Create indices
            self._logger.info("Creating indices")
            for stmt in BaseNearestCity._get_bootstrap_index_sql():
                cur.execute(stmt)
            self.connection.commit()

    def _inform_user_if_not_context_manager(self):
        """Raise an error if the context manager was not used."""
        if not self._is_initialized:
            raise RuntimeError(
                fill(
                    dedent("""
                NearestCity must be used within 'with' context.\n
                    For example:\n
                    with NearestCity() as geocoder:\n
                        details = geocoder.query(lon, lat)
            """)
                )
            )

    def query(self, lon: float, lat: float) -> Optional[Location]:
        """Find the nearest city to the given coordinates.

        Uses ST_Covers + lateral join on country and geocoding tables.
        Coordinates use (lon, lat) order, matching PostGIS convention
        where longitude is the X axis and latitude is Y.

        Args:
            lon: Longitude in degrees (-180 to 180)
            lat: Latitude in degrees (-90 to 90)

        Returns:
            Location object if a matching city is found, None otherwise

        Raises:
            ValueError: If coordinates are out of valid ranges
            RuntimeError: If database query fails
        """
        self._inform_user_if_not_context_manager()

        BaseNearestCity.validate_coordinates(lon, lat)

        try:
            with self.connection.cursor() as cur:
                cur.execute(
                    BaseNearestCity._get_reverse_geocoding_query(lon, lat),
                )
                result = cur.fetchone()

                if not result:
                    return None

                return Location(
                    city=result[0],
                    country=result[1],
                    lat=float(result[2]),
                    lon=float(result[3]),
                    country_alpha3=result[4],
                    country_name=result[5],
                )
        except Exception as e:
            self._logger.error(f"Reverse geocoding failed: {str(e)}")
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}") from e

    def _check_initialization_status(
        self,
        cur: psycopg.Cursor,
    ) -> InitializationStatus:
        """Check the status and integrity of the geocoding database."""
        status = InitializationStatus()

        # Check table existence
        cur.execute(BaseNearestCity._get_tables_existence_query())
        result = cur.fetchone()
        status.has_country_table = bool(result and result[0])
        status.has_geocoding_table = bool(result and result[1])

        if not (status.has_country_table and status.has_geocoding_table):
            return status

        # Check data completeness
        cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = cur.fetchone()
        if counts is not None:
            status.has_country_data = counts[0] > 0
            status.has_geocoding_data = counts[1] > 0

        # Check spatial indices
        cur.execute(BaseNearestCity._get_spatial_index_check_query())
        idx_result = cur.fetchone()
        status.has_spatial_index = bool(idx_result and idx_result[0] and idx_result[1])

        return status
