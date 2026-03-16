"""Main logic."""

import gzip
import logging
from typing import Optional
from importlib import resources
from textwrap import dedent, fill

import psycopg

from pg_nearest_city.base_nearest_city import (
    BaseNearestCity,
    DbConfig,
    InitializationStatus,
    Location,
)
from pg_nearest_city.db.data_cleanup import ROWS_TO_CLEAN, make_queries
from pg_nearest_city.db.tables import get_tables_in_creation_order

logger = logging.getLogger("pg_nearest_city")

# Arbitrary lock ID used for pg_advisory_lock to serialise concurrent inits.
_ADVISORY_LOCK_ID = 0x706E6300  # "pnc\x00"


class NearestCity:
    """Reverse geocoding to the nearest city over 1000 population."""

    def __init__(
        self,
        db: psycopg.Connection | DbConfig | None = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize reverse geocoder with an existing Connection.

        Args:
            db: An existing psycopg Connection
            connection: psycopg.Connection
            logger: Optional custom logger. If not provided, uses package logger.
        """
        # Allow users to provide their own logger while having a sensible default
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self._db = db
        self.connection: psycopg.Connection = None
        self._is_external_connection = False
        self._is_initialized = False

        self.cities_file = resources.files("pg_nearest_city.data").joinpath(
            "cities_500_simple.txt.gz"
        )
        self.country_boundaries_file = resources.files("pg_nearest_city.data").joinpath(
            "country_boundaries.wkb.gz"
        )
        self.iso_file = resources.files("pg_nearest_city.db").joinpath(
            "iso-3166-1.csv.gz"
        )

    def __enter__(self):
        """Open the context manager."""
        self.connection = self.get_connection(self._db)
        # Create the relevant tables and validate
        self.initialize()
        self._is_initialized = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the context manager."""
        if self.connection and not self._is_external_connection:
            self.connection.close()
        self._initialized = False

    def get_connection(
        self,
        db: Optional[psycopg.Connection | DbConfig] = None,
    ) -> psycopg.Connection:
        """Determine the database connection to use."""
        self._is_external_connection = isinstance(db, psycopg.Connection)
        is_db_config = isinstance(db, DbConfig)

        if self._is_external_connection:
            return db
        elif is_db_config:
            return psycopg.Connection.connect(db.get_connection_string())
        else:
            # Fallback to env var extraction, or defaults for testing
            return psycopg.Connection.connect(
                DbConfig().get_connection_string(),
            )

    def initialize(self) -> None:
        """Initialize the geocoding database with validation checks.

        Uses a PostgreSQL advisory lock to prevent race conditions when multiple
        callers attempt to initialize concurrently. The first caller performs the
        initialization; subsequent callers wait, then skip once complete.
        """
        if not self.connection:
            self._inform_user_if_not_context_manager()

        try:
            with self.connection.cursor() as cur:
                self._logger.info("Starting database initialization check")

                # Acquire advisory lock to serialise concurrent initializations.
                cur.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_ID,))
                try:
                    # Re-check status after acquiring the lock in case another
                    # caller already completed initialization while we waited.
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
                        cur.execute("TRUNCATE geocoding")
                        cur.execute("UPDATE country SET geom = NULL")

                    self._logger.info("Setting up PostGIS extension")
                    self._setup_extensions(cur)

                    self._logger.info("Creating tables")
                    self._setup_tables(cur)

                    self._logger.info("Importing country metadata")
                    self._import_country_metadata(cur)

                    self._logger.info("Importing country boundaries")
                    self._import_country_boundaries(cur)

                    self._logger.info("Importing city data")
                    self._import_cities(cur)

                    self._logger.info("Creating spatial indices")
                    self._create_spatial_indices(cur)

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
                finally:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_ID,))

        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}") from e

    def _inform_user_if_not_context_manager(self):
        """Raise an error if the context manager was not used."""
        if not self._is_initialized:
            raise RuntimeError(
                fill(
                    dedent("""
                NearestCity must be used within 'with' context.\n
                    For example:\n
                    with NearestCity() as geocoder:\n
                        details = geocoder.query(lat, lon)
            """)
                )
            )

    def query(self, lat: float, lon: float) -> Optional[Location]:
        """Find the nearest city to the given coordinates.

        Uses country boundary polygons to constrain the search to the correct
        country before finding the nearest city by distance.

        Args:
            lat: Latitude in degrees (-90 to 90)
            lon: Longitude in degrees (-180 to 180)

        Returns:
            Location object if a matching city is found, None otherwise

        Raises:
            ValueError: If coordinates are out of valid ranges
            RuntimeError: If database query fails
        """
        # Throw an error if not used in 'with' block
        self._inform_user_if_not_context_manager()

        # Validate coordinate ranges
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
                    country_alpha3=result[2],
                    lat=float(result[4]),
                    lon=float(result[3]),
                    country_name=result[5],
                )
        except Exception as e:
            self._logger.error(f"Reverse geocoding failed: {str(e)}")
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}") from e

    def _check_initialization_status(
        self,
        cur: psycopg.Cursor,
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
            "city": "text",
            "country": "character",
            "lat": "numeric",
            "lon": "numeric",
            "geom": "geometry",
        }
        status.has_valid_structure = all(col in columns for col in expected_columns)
        # If table doesn't have valid structure, we can't check other properties
        if not status.has_valid_structure:
            return status

        # Check data completeness
        cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = cur.fetchone()
        total_cities, countries_with_boundaries = counts

        status.has_data = total_cities > 0
        status.has_country_boundaries = countries_with_boundaries > 0

        # Check spatial index
        cur.execute(BaseNearestCity._get_spatial_index_check_query())
        has_index = cur.fetchone()
        status.has_spatial_index = bool(has_index and has_index[0])

        return status

    def _setup_extensions(self, cur: psycopg.Cursor):
        """Ensure required PostgreSQL extensions are available."""
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    def _setup_tables(self, cur: psycopg.Cursor):
        """Create country and geocoding tables if they don't exist."""
        for table in get_tables_in_creation_order():
            if table.safe_ops:
                cur.execute(
                    table.sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                )
            else:
                cur.execute(table.sql)

    def _import_country_metadata(self, cur: psycopg.Cursor):
        """Import ISO 3166-1 country codes and names into the country table.

        The bundled CSV has four columns (alpha2, alpha3, numeric, name).
        The country table only stores alpha2, alpha3, and name.
        """
        if not self.iso_file.is_file():
            raise FileNotFoundError(f"ISO country file not found: {self.iso_file}")

        # Temp table matches the CSV format (including numeric) so COPY succeeds.
        cur.execute("""
            CREATE TEMP TABLE country_iso_tmp (
                alpha2 CHAR(2),
                alpha3 CHAR(3),
                numeric CHAR(3),
                name TEXT
            ) ON COMMIT DROP
        """)

        with gzip.open(self.iso_file, "r") as f:
            # Skip the header line
            f.readline()
            with cur.copy("COPY country_iso_tmp FROM STDIN WITH (FORMAT CSV)") as copy:
                while data := f.read(8192):
                    copy.write(data)

        cur.execute("""
            INSERT INTO country (alpha2, alpha3, name)
            SELECT alpha2, alpha3, name FROM country_iso_tmp
            ORDER BY alpha2
            ON CONFLICT DO NOTHING
        """)

    def _import_country_boundaries(self, cur: psycopg.Cursor):
        """Import country boundary geometries from the bundled WKB file."""
        if not self.country_boundaries_file.is_file():
            raise FileNotFoundError(
                f"Country boundaries file not found: {self.country_boundaries_file}"
            )

        cur.execute("""
            CREATE TEMP TABLE country_boundaries_tmp (
                alpha2 CHAR(2),
                wkb BYTEA
            ) ON COMMIT DROP
        """)

        with cur.copy("COPY country_boundaries_tmp (alpha2, wkb) FROM STDIN") as copy:
            with gzip.open(self.country_boundaries_file, "rb") as f:
                while data := f.read(8192):
                    copy.write(data)

        cur.execute("""
            UPDATE country c
            SET geom = ST_GeomFromWKB(t.wkb, 4326)
            FROM country_boundaries_tmp t
            WHERE c.alpha2 = t.alpha2
        """)

    def _import_cities(self, cur: psycopg.Cursor):
        """Import city data using COPY protocol."""
        if not self.cities_file.is_file():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")

        with cur.copy("COPY geocoding(city, country, lat, lon) FROM STDIN") as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    copy.write(data)
                    copied_bytes += len(data)
                self._logger.info(f"Imported {copied_bytes:,} bytes of city data")

        # Apply targeted cleanup fixes for known source-data issues.
        for query in make_queries(ROWS_TO_CLEAN):
            cur.execute(query)

    def _create_spatial_indices(self, cur: psycopg.Cursor):
        """Create spatial indices for efficient geocoding queries."""
        cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_geom_idx
            ON geocoding
            USING GIST (geom)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_country_idx
            ON geocoding (country)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_country_geom_gist_idx
            ON geocoding
            USING GIST (country, geom)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS country_geom_idx
            ON country
            USING GIST (geom)
        """)
