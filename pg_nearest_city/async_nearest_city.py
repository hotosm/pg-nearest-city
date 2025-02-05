import importlib.resources
import psycopg
import gzip
from psycopg import AsyncCursor  

from typing import Optional
from pg_nearest_city.base_nearest_city import BaseNearestCity
from pg_nearest_city.base_nearest_city import Location

class AsyncNearestCity:
    def __init__(self, db: psycopg.AsyncConnection):
        """Initialize reverse geocoder with database configuration or existing connection.

        Args:
            db: An existing psycopg AsyncConnection
        """

        self.db = db
        self._connection = None
        
        with importlib.resources.path(
            "pg_nearest_city.data", "cities_1000_simple.txt.gz"
        ) as cities_path:
            self.cities_file = cities_path
        with importlib.resources.path(
            "pg_nearest_city.data", "voronois.wkb.gz"
        ) as voronoi_path:
            self.voronoi_file = voronoi_path

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
                await cur.execute(BaseNearestCity._get_reverse_geocoding_query(lon, lat))
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
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}")

    async def Initialize(self) -> None:
        """Initialize the geocoding database with validation checks.

        Performs necessary initialization steps based on current database state.
        Will attempt repair if the database is in an invalid state.
        """
        try:
            conn = self._get_connection()
            async with conn.cursor() as cur:
                status = await self._check_initialization_status(cur)

                if status["is_initialized"]:
                    print("Database already properly initialized.")
                    return

                if status["needs_repair"]:
                    print(f"Database needs repair: {status['details']}")
                    print("Reinitializing from scratch...")
                    await cur.execute("DROP TABLE IF EXISTS pg_nearest_city_geocoding;")

                print("Creating geocoding table...")
                await self._create_geocoding_table(cur)

                print("Importing city data...")
                await self._import_cities(cur)

                print("Processing Voronoi polygons...")
                await self._import_voronoi_polygons(cur)

                print("Creating spatial index...")
                await self._create_spatial_index(cur)

                await conn.commit()

                # Verify initialization
                final_status = await self._check_initialization_status(cur)
                if not final_status["is_initialized"]:
                    raise RuntimeError(
                        f"Initialization failed final validation: {final_status['details']}"
                    )

                print("Initialization complete and verified!")

        except Exception as e:
            raise RuntimeError(f"Database initialization failed: {str(e)}")

    async def _check_initialization_status(self, cur: psycopg.AsyncCursor) -> dict:
        """Check the status and integrity of the geocoding database.

        Performs essential validation checks to ensure the database is properly initialized
        and contains valid data.
        """
        # Check table existence
        await cur.execute(BaseNearestCity._get_table_existance_query())
        table_exists = await cur.fetchone()

        if not table_exists[0]:
            return {
                "is_initialized": False,
                "needs_repair": False,
                "details": "Table does not exist",
            }

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

        if not all(col in columns for col in expected_columns):
            return {
                "is_initialized": False,
                "needs_repair": True,
                "details": "Table structure is invalid",
            }

        # Check data completeness
        await cur.execute(BaseNearestCity._get_data_completeness_query())

        counts = await cur.fetchone()
        total_cities, cities_with_voronoi = counts

        if total_cities == 0:
            return {
                "is_initialized": False,
                "needs_repair": False,
                "details": "No city data present",
            }

        if total_cities != self.EXPECTED_CITY_COUNT:
            return {
                "is_initialized": False,
                "needs_repair": True,
                "details": f"City count mismatch: expected {self.EXPECTED_CITY_COUNT}, found {total_cities}",
            }

        if cities_with_voronoi != total_cities:
            return {
                "is_initialized": False,
                "needs_repair": True,
                "details": "Missing Voronoi polygons",
            }

        # Check spatial index
        await cur.execute(BaseNearestCity._get_spatial_index_check_query())
        has_index = (await cur.fetchone())[0]

        if not has_index:
            return {
                "is_initialized": False,
                "needs_repair": True,
                "details": "Missing spatial index",
            }

        # Everything looks good
        return {
            "is_initialized": True,
            "needs_repair": False,
            "details": "Database properly initialized",
        }

    async def _import_cities(self,cur: AsyncCursor):
        if not self.cities_file.exists():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")

        """Import city data using COPY protocol."""
        async with cur.copy("COPY pg_nearest_city_geocoding(city, country, lat, lon) FROM STDIN") as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    await copy.write(data)
                    copied_bytes += len(data)
                print(f"Imported {copied_bytes:,} bytes of city data")

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
        async with cur.copy("COPY voronoi_import (city, country, wkb) FROM STDIN") as copy:
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

