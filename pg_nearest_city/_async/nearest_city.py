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


class AsyncNearestCity:
    """Reverse geocoding to the nearest city over 1000 population."""

    def __init__(
        self,
        db: psycopg.AsyncConnection | DbConfig | None = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize reverse geocoder with an existing AsyncConnection.

        Args:
            db: An existing psycopg AsyncConnection
            connection: psycopg.AsyncConnection
            logger: Optional custom logger. If not provided, uses package logger.
        """
        # Allow users to provide their own logger while having a sensible default
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self._db = db
        self.connection: psycopg.AsyncConnection = None
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

    async def __aenter__(self):
        """Open the context manager."""
        self.connection = await self.get_connection(self._db)
        # Create the relevant tables and validate
        await self.initialize()
        self._is_initialized = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close the context manager."""
        if self.connection and not self._is_external_connection:
            await self.connection.close()
        self._initialized = False

    async def get_connection(
        self,
        db: Optional[psycopg.AsyncConnection | DbConfig] = None,
    ) -> psycopg.AsyncConnection:
        """Determine the database connection to use."""
        self._is_external_connection = isinstance(db, psycopg.AsyncConnection)
        is_db_config = isinstance(db, DbConfig)

        if self._is_external_connection:
            return db
        elif is_db_config:
            return await psycopg.AsyncConnection.connect(db.get_connection_string())
        else:
            # Fallback to env var extraction, or defaults for testing
            return await psycopg.AsyncConnection.connect(
                DbConfig().get_connection_string(),
            )

    async def initialize(self) -> None:
        """Initialize the geocoding database with validation checks.

        Uses a PostgreSQL advisory lock to prevent race conditions when multiple
        callers attempt to initialize concurrently. The first caller performs the
        initialization; subsequent callers wait, then skip once complete.
        """
        if not self.connection:
            self._inform_user_if_not_context_manager()

        try:
            async with self.connection.cursor() as cur:
                self._logger.info("Starting database initialization check")

                # Acquire advisory lock to serialise concurrent initializations.
                await cur.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_ID,))
                try:
                    # Re-check status after acquiring the lock in case another
                    # caller already completed initialization while we waited.
                    status = await self._check_initialization_status(cur)

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
                        await cur.execute("TRUNCATE geocoding")
                        await cur.execute("UPDATE country SET geom = NULL")

                    self._logger.info("Setting up PostGIS extension")
                    await self._setup_extensions(cur)

                    self._logger.info("Creating tables")
                    await self._setup_tables(cur)

                    self._logger.info("Importing country metadata")
                    await self._import_country_metadata(cur)

                    self._logger.info("Importing country boundaries")
                    await self._import_country_boundaries(cur)

                    self._logger.info("Importing city data")
                    await self._import_cities(cur)

                    self._logger.info("Creating spatial indices")
                    await self._create_spatial_indices(cur)

                    await self.connection.commit()

                    self._logger.debug("Verifying final initialization state")
                    final_status = await self._check_initialization_status(cur)
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
                    await cur.execute(
                        "SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_ID,)
                    )

        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}") from e

    def _inform_user_if_not_context_manager(self):
        """Raise an error if the context manager was not used."""
        if not self._is_initialized:
            raise RuntimeError(
                fill(
                    dedent("""
                AsyncNearestCity must be used within 'async with' context.\n
                    For example:\n
                    async with AsyncNearestCity() as geocoder:\n
                        details = geocoder.query(lat, lon)
            """)
                )
            )

    async def query(self, lat: float, lon: float) -> Optional[Location]:
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
            async with self.connection.cursor() as cur:
                await cur.execute(
                    BaseNearestCity._get_reverse_geocoding_query(lon, lat),
                )
                result = await cur.fetchone()

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

    async def _check_initialization_status(
        self,
        cur: psycopg.AsyncCursor,
    ) -> InitializationStatus:
        """Check the status and integrity of the geocoding database.

        Performs essential validation checks to ensure the database is
        properly initialized and contains valid data.
        """
        status = InitializationStatus()

        # Check table existence
        await cur.execute(BaseNearestCity._get_tableexistence_query())
        table_exists = await cur.fetchone()
        status.has_table = bool(table_exists and table_exists[0])

        # If table doesn't exist, we can't check other properties
        if not status.has_table:
            return status

        # Check table structure
        await cur.execute(BaseNearestCity._get_table_structure_query())
        columns = {col: dtype for col, dtype in await cur.fetchall()}
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
        await cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = await cur.fetchone()
        total_cities, countries_with_boundaries = counts

        status.has_data = total_cities > 0
        status.has_country_boundaries = countries_with_boundaries > 0

        # Check spatial index
        await cur.execute(BaseNearestCity._get_spatial_index_check_query())
        has_index = await cur.fetchone()
        status.has_spatial_index = bool(has_index and has_index[0])

        return status

    async def _setup_extensions(self, cur: psycopg.AsyncCursor):
        """Ensure required PostgreSQL extensions are available."""
        await cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    async def _setup_tables(self, cur: psycopg.AsyncCursor):
        """Create country and geocoding tables if they don't exist."""
        for table in get_tables_in_creation_order():
            if table.safe_ops:
                await cur.execute(
                    table.sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                )
            else:
                await cur.execute(table.sql)

    async def _import_country_metadata(self, cur: psycopg.AsyncCursor):
        """Import ISO 3166-1 country codes and names into the country table.

        The bundled CSV has four columns (alpha2, alpha3, numeric, name).
        The country table only stores alpha2, alpha3, and name.
        """
        if not self.iso_file.is_file():
            raise FileNotFoundError(f"ISO country file not found: {self.iso_file}")

        # Temp table matches the CSV format (including numeric) so COPY succeeds.
        await cur.execute("""
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
            async with cur.copy(
                "COPY country_iso_tmp FROM STDIN WITH (FORMAT CSV)"
            ) as copy:
                while data := f.read(8192):
                    await copy.write(data)

        await cur.execute("""
            INSERT INTO country (alpha2, alpha3, name)
            SELECT alpha2, alpha3, name FROM country_iso_tmp
            ORDER BY alpha2
            ON CONFLICT DO NOTHING
        """)

    async def _import_country_boundaries(self, cur: psycopg.AsyncCursor):
        """Import country boundary geometries from the bundled WKB file."""
        if not self.country_boundaries_file.is_file():
            raise FileNotFoundError(
                f"Country boundaries file not found: {self.country_boundaries_file}"
            )

        await cur.execute("""
            CREATE TEMP TABLE country_boundaries_tmp (
                alpha2 CHAR(2),
                wkb BYTEA
            ) ON COMMIT DROP
        """)

        async with cur.copy(
            "COPY country_boundaries_tmp (alpha2, wkb) FROM STDIN"
        ) as copy:
            with gzip.open(self.country_boundaries_file, "rb") as f:
                while data := f.read(8192):
                    await copy.write(data)

        await cur.execute("""
            UPDATE country c
            SET geom = ST_GeomFromWKB(t.wkb, 4326)
            FROM country_boundaries_tmp t
            WHERE c.alpha2 = t.alpha2
        """)

    async def _import_cities(self, cur: psycopg.AsyncCursor):
        """Import city data using COPY protocol."""
        if not self.cities_file.is_file():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")

        async with cur.copy(
            "COPY geocoding(city, country, lat, lon) FROM STDIN"
        ) as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    await copy.write(data)
                    copied_bytes += len(data)
                self._logger.info(f"Imported {copied_bytes:,} bytes of city data")

        # Apply targeted cleanup fixes for known source-data issues.
        for query in make_queries(ROWS_TO_CLEAN):
            await cur.execute(query)

    async def _create_spatial_indices(self, cur: psycopg.AsyncCursor):
        """Create spatial indices for efficient geocoding queries."""
        await cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_geom_idx
            ON geocoding
            USING GIST (geom)
        """)
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_country_idx
            ON geocoding (country)
        """)
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS geocoding_country_geom_gist_idx
            ON geocoding
            USING GIST (country, geom)
        """)
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS country_geom_idx
            ON country
            USING GIST (geom)
        """)
