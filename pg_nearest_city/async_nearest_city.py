import importlib.resources
import psycopg
import gzip
from psycopg import AsyncCursor

from typing import Optional
from pg_nearest_city.base_nearest_city import BaseNearestCity
from pg_nearest_city.base_nearest_city import Location
from pg_nearest_city.base_nearest_city import InitializationStatus

import logging
from typing import Optional

logger = logging.getLogger("pg_nearest_city")

class AsyncNearestCity:
    def __init__(self, db: psycopg.AsyncConnection, logger: Optional[logging.Logger] = None):
        """Initialize reverse geocoder with an existing AsyncConnection
        
        Args:
            db: An existing psycopg AsyncConnection
            logger: Optional custom logger. If not provided, uses package logger
        """
        self.db = db
        # Allow users to provide their own logger while having a sensible default
        self._logger = logger or logging.getLogger("pg_nearest_city")
        self._connection = None

        with importlib.resources.path(
            "pg_nearest_city.data", "cities_1000_simple.txt.gz"
        ) as cities_path:
            self.cities_file = cities_path
        with importlib.resources.path(
            "pg_nearest_city.data", "voronois.wkb.gz"
        ) as voronoi_path:
            self.voronoi_file = voronoi_path

    async def Initialize(self) -> None:
        """Initialize the geocoding database with validation checks."""
        try:
            conn = self._get_connection()
            async with conn.cursor() as cur:
                self._logger.info("Starting database initialization check")
                status = await self._check_initialization_status(cur)
                
                if status.is_fully_initialized:
                    self._logger.info("Database already properly initialized")
                    return
                
                if status.has_table and not status.is_fully_initialized:
                    missing = status.get_missing_components()
                    self._logger.warning(
                        "Database needs repair. Missing components: %s", 
                        ", ".join(missing)
                    )
                    self._logger.info("Reinitializing from scratch")
                    await cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")
                
                self._logger.info("Creating geocoding table")
                await self._create_geocoding_table(cur)
                
                self._logger.info("Importing city data")
                await self._import_cities(cur)
                
                self._logger.info("Processing Voronoi polygons")
                await self._import_voronoi_polygons(cur)
                
                self._logger.info("Creating spatial index")
                await self._create_spatial_index(cur)
                
                await conn.commit()
                
                self._logger.debug("Verifying final initialization state")
                final_status = await self._check_initialization_status(cur)
                if not final_status.is_fully_initialized:
                    missing = final_status.get_missing_components()
                    self._logger.error(
                        "Initialization failed final validation. Missing: %s",
                        ", ".join(missing)
                    )
                    raise RuntimeError(
                        f"Initialization failed final validation. Missing components: {', '.join(missing)}"
                    )
                
                self._logger.info("Initialization complete and verified")
                
        except Exception as e:
            self._logger.error("Database initialization failed: %s", str(e))
            raise RuntimeError(f"Database initialization failed: {str(e)}")

    def _get_connection(self) -> psycopg.AsyncConnection:
        """Get or create database connection."""
        return self.db

    async def query(self, lat: float, lon: float) -> Optional[Location]:
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
            conn = self._get_connection()
            async with conn.cursor() as cur:
                await cur.execute(
                    BaseNearestCity._get_reverse_geocoding_query(lon, lat)
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
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}")


    async def _check_initialization_status(self, cur: psycopg.AsyncCursor) -> InitializationStatus:
        """Check the status and integrity of the geocoding database.

        Performs essential validation checks to ensure the database is properly initialized
        and contains valid data.
        """
        status = InitializationStatus()
        
        # Check table existence
        await cur.execute(BaseNearestCity._get_table_existance_query())
        table_exists = await cur.fetchone()
        status.has_table = bool(table_exists and table_exists[0])
        
        # If table doesn't exist, we can't check other properties
        if not status.has_table:
            return status
            
        # Check table structure
        await cur.execute(BaseNearestCity._get_table_structure_query())
        columns = {col: dtype for col, dtype in await cur.fetchall()}
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
        await cur.execute(BaseNearestCity._get_data_completeness_query())
        counts = await cur.fetchone()
        total_cities, cities_with_voronoi = counts
        
        status.has_data = total_cities > 0
        status.has_complete_voronoi = cities_with_voronoi == total_cities
        
        # Check spatial index
        await cur.execute(BaseNearestCity._get_spatial_index_check_query())
        has_index = await cur.fetchone()
        status.has_spatial_index = bool(has_index and has_index[0])
        
        return status

    async def _import_cities(self, cur: AsyncCursor):
        if not self.cities_file.exists():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")

        """Import city data using COPY protocol."""
        async with cur.copy(
            "COPY pg_nearest_city_geocoding(city, country, lat, lon) FROM STDIN"
        ) as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    await copy.write(data)
                    copied_bytes += len(data)
                self._logger.info(f"Imported {copied_bytes:,} bytes of city data")

    async def _create_geocoding_table(self, cur: AsyncCursor):
        """Create the main table"""
        await cur.execute("""
            CREATE TABLE pg_nearest_city_geocoding (
                city varchar,
                country varchar,
                lat decimal,
                lon decimal,
                geom geometry(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
                voronoi geometry(Polygon,4326)
            );
        """)

    async def _import_voronoi_polygons(self, cur: AsyncCursor):
        """Import and integrate Voronoi polygons into the main table."""

        if not self.voronoi_file.exists():
            raise FileNotFoundError(f"Voronoi file not found: {self.voronoi_file}")

        # First create temporary table for the import
        await cur.execute("""
            CREATE TEMP TABLE voronoi_import (
                city text,
                country text,
                wkb bytea
            );
        """)

        # Import the binary WKB data
        async with cur.copy(
            "COPY voronoi_import (city, country, wkb) FROM STDIN"
        ) as copy:
            with gzip.open(self.voronoi_file, "rb") as f:
                while data := f.read(8192):
                    await copy.write(data)

        # Update main table with Voronoi geometries
        await cur.execute("""
            UPDATE pg_nearest_city_geocoding g
            SET voronoi = ST_GeomFromWKB(v.wkb, 4326)
            FROM voronoi_import v
            WHERE g.city = v.city
            AND g.country = v.country;
        """)

        # Clean up temporary table
        await cur.execute("DROP TABLE voronoi_import;")

    async def _create_spatial_index(self, cur: AsyncCursor):
        """Create a spatial index on the Voronoi polygons for efficient queries."""
        await cur.execute("""
            CREATE INDEX geocoding_voronoi_idx 
            ON pg_nearest_city_geocoding 
            USING GIST (voronoi);
        """)
