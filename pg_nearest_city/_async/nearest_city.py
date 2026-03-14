"""Main logic."""

import logging
import shutil
import subprocess
from textwrap import dedent, fill
from typing import Optional

import psycopg

from pg_nearest_city.base_nearest_city import (
    BaseNearestCity,
    DbConfig,
    InitializationStatus,
    Location,
)

logger = logging.getLogger("pg_nearest_city")


class AsyncNearestCity:
    """Reverse geocoding to the nearest city over 1000 population."""

    connection: psycopg.AsyncConnection

    def __init__(
        self,
        db: psycopg.AsyncConnection | DbConfig | None = None,
        logger: Optional[logging.Logger] = None,
        dump_path: str | None = None,
    ):
        """Initialize reverse geocoder.

        Args:
            db: An existing psycopg AsyncConnection or DbConfig
            logger: Optional custom logger.
            dump_path: Optional path to a pg_dump file for auto-import.
        """
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self._db = db
        self._dump_path = dump_path
        self._is_external_connection = False
        self._is_initialized = False

    async def __aenter__(self):
        """Open the context manager."""
        self.connection = await self.get_connection(self._db)
        await self.initialize()
        self._is_initialized = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close the context manager."""
        if self.connection and not self._is_external_connection:
            await self.connection.close()
        self._is_initialized = False

    async def get_connection(
        self,
        db: Optional[psycopg.AsyncConnection | DbConfig] = None,
    ) -> psycopg.AsyncConnection:
        """Determine the database connection to use."""
        if isinstance(db, psycopg.AsyncConnection):
            self._is_external_connection = True
            return db
        self._is_external_connection = False
        if isinstance(db, DbConfig):
            return await psycopg.AsyncConnection.connect(db.get_connection_string())
        return await psycopg.AsyncConnection.connect(
            DbConfig().get_connection_string(),
        )

    async def initialize(self) -> None:
        """Initialize the geocoding database with validation checks.

        Checks for country and geocoding tables. If not present,
        attempts auto-import from a dump file.
        """
        if not getattr(self, "connection", None):
            self._inform_user_if_not_context_manager()

        try:
            async with self.connection.cursor() as cur:
                self._logger.info("Starting database initialization check")
                status = await self._check_initialization_status(cur)

                if status.is_fully_initialized:
                    self._logger.info("Database already properly initialized")
                    return

                missing = status.get_missing_components()

                # Attempt auto-import from dump
                dump_path = BaseNearestCity._find_dump_path(self._dump_path)
                if dump_path:
                    self._logger.warning(
                        "Database not ready (missing: %s); importing from dump: %s",
                        ", ".join(missing),
                        dump_path,
                    )
                    self._import_from_dump(dump_path)

                    # Re-check after import
                    status = await self._check_initialization_status(cur)
                    if status.is_fully_initialized:
                        self._logger.warning("Database initialized from dump file")
                        return

                raise RuntimeError(
                    "Database is not initialized and no dump file found. "
                    "Run the bootstrap pipeline first "
                    "(python -m pg_nearest_city.scripts.load_database), "
                    f"or provide a dump file via {BaseNearestCity.DUMP_ENV_VAR} "
                    "env var or dump_path constructor arg."
                )

        except RuntimeError:
            raise
        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}") from e

    def _import_from_dump(self, dump_path: str) -> None:
        """Import database from a pg_dump file using pg_restore."""
        if not shutil.which("pg_restore"):
            raise RuntimeError(
                "pg_restore not found - please install PostgreSQL client tools"
            )

        conn_info = self.connection.info
        cmd = [
            "pg_restore",
            "--no-owner",
            "--no-privileges",
            f"--host={conn_info.host}",
            f"--port={conn_info.port}",
            f"--username={conn_info.user}",
            f"--dbname={conn_info.dbname}",
            dump_path,
        ]
        env = None
        if conn_info.password:
            import os

            env = {**os.environ, "PGPASSWORD": conn_info.password}

        try:
            subprocess.run(cmd, check=True, env=env)
        except subprocess.CalledProcessError as e:
            self._logger.error(f"pg_restore failed: {e}")
            raise RuntimeError(f"Failed to restore from dump: {e}") from e

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

        Uses ST_Covers + lateral join on country and geocoding tables.

        Args:
            lat: Latitude in degrees (-90 to 90)
            lon: Longitude in degrees (-180 to 180)

        Returns:
            Location object if a matching city is found, None otherwise

        Raises:
            ValueError: If coordinates are out of valid ranges
            RuntimeError: If database query fails
        """
        self._inform_user_if_not_context_manager()

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
                    lat=float(result[2]),
                    lon=float(result[3]),
                )
        except Exception as e:
            self._logger.error(f"Reverse geocoding failed: {str(e)}")
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}") from e

    async def _check_initialization_status(
        self,
        cur: psycopg.AsyncCursor,
    ) -> InitializationStatus:
        """Check the status and integrity of the geocoding database."""
        status = InitializationStatus()

        # Check table existence
        await cur.execute(BaseNearestCity._get_tables_existence_query())
        result = await cur.fetchone()
        status.has_country_table = bool(result and result[0])
        status.has_geocoding_table = bool(result and result[1])

        if not (status.has_country_table and status.has_geocoding_table):
            return status

        # Check data completeness
        await cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = await cur.fetchone()
        if counts is not None:
            status.has_country_data = counts[0] > 0
            status.has_geocoding_data = counts[1] > 0

        # Check spatial indices
        await cur.execute(BaseNearestCity._get_spatial_index_check_query())
        idx_result = await cur.fetchone()
        status.has_spatial_index = bool(idx_result and idx_result[0] and idx_result[1])

        return status
