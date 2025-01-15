import psycopg
import gzip

from dataclasses import dataclass
from typing import Optional
import importlib.resources


from psycopg import sql


@dataclass
class DbConfig:
    dbname: str
    user: str
    password: str
    host: str = "localhost"
    port: int = 5432

    def get_connection_string(self) -> str:
        return f"dbname={self.dbname} user={self.user} password={self.password} host={self.host} port={self.port}"


@dataclass
class Location:
    city: str
    country: str
    lat: float
    lon: float


class ReverseGeocoder:
    def __init__(self, config: DbConfig):
        """Initialize reverse geocoder with database configuration and data directory.

        Args:
            config: Database connection configuration
            data_dir: Directory containing required data files
        """
        self.conn_string = config.get_connection_string()

        with importlib.resources.path(
            "pg_nearest_city.data", "cities_1000_simple.txt.gz"
        ) as cities_path:
            self.cities_file = cities_path
        with importlib.resources.path(
            "pg_nearest_city.data", "voronois.wkb.gz"
        ) as voronoi_path:
            self.voronoi_file = voronoi_path

        if not self.cities_file.exists():
            raise FileNotFoundError(f"Cities file not found: {self.cities_file}")
        if not self.voronoi_file.exists():
            raise FileNotFoundError(f"Voronoi file not found: {self.voronoi_file}")

    def _get_connection(self):
        """Create and return a database connection."""
        try:
            return psycopg.connect(self.conn_string)
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database: {str(e)}")

    def reverse_geocode(self, lat: float, lon: float) -> Optional[Location]:
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
        if not -90 <= lat <= 90:
            raise ValueError(f"Latitude {lat} is outside valid range [-90, 90]")
        if not -180 <= lon <= 180:
            raise ValueError(f"Longitude {lon} is outside valid range [-180, 180]")

        query = sql.SQL("""
            SELECT city, country, lat, lon 
            FROM geocoding 
            WHERE ST_Contains(voronoi, ST_SetSRID(ST_MakePoint({}, {}), 4326))
            LIMIT 1
        """).format(sql.Literal(lon), sql.Literal(lat))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
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
            raise RuntimeError(f"Reverse geocoding failed: {str(e)}")

    def initialize(self) -> None:
        """Initialize the geocoding database with cities and Voronoi regions."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    print("Creating geocoding table...")
                    self._create_geocoding_table(cur)

                    print("Importing city data...")
                    self._import_cities(cur)

                    print("Processing Voronoi polygons...")
                    self._import_voronoi_polygons(cur)

                    print("Creating spatial index...")
                    self._create_spatial_index(cur)

                    conn.commit()
                    print("Initialization complete!")
        except Exception as e:
            raise RuntimeError(f"Database initialization failed: {str(e)}")

    def _import_cities(self, cur):
        """Import city data using COPY protocol."""
        with cur.copy("COPY geocoding(city, country, lat, lon) FROM STDIN") as copy:
            with gzip.open(self.cities_file, "r") as f:
                copied_bytes = 0
                while data := f.read(8192):
                    copy.write(data)
                    copied_bytes += len(data)
                print(f"Imported {copied_bytes:,} bytes of city data")

    def _create_geocoding_table(self, cur):
        """Create the main table"""
        cur.execute("""
            CREATE TABLE geocoding (
                city varchar,
                country varchar,
                lat decimal,
                lon decimal,
                geom geometry(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
                voronoi geometry(Polygon,4326)
            );
        """)

    def _import_voronoi_polygons(self, cur):
        """Import and integrate Voronoi polygons into the main table."""
        # First create temporary table for the import
        cur.execute("""
            CREATE TEMP TABLE voronoi_import (
                city text,
                country text,
                wkb bytea
            );
        """)

        # Import the binary WKB data
        with cur.copy("COPY voronoi_import (city, country, wkb) FROM STDIN") as copy:
            with gzip.open(self.voronoi_file, "rb") as f:
                while data := f.read(8192):
                    copy.write(data)

        # Update main table with Voronoi geometries
        cur.execute("""
            UPDATE geocoding g
            SET voronoi = ST_GeomFromWKB(v.wkb, 4326)
            FROM voronoi_import v
            WHERE g.city = v.city
            AND g.country = v.country;
        """)

        # Clean up temporary table
        cur.execute("DROP TABLE voronoi_import;")

    def _create_spatial_index(self, cur):
        """Create a spatial index on the Voronoi polygons for efficient queries."""
        cur.execute("""
            CREATE INDEX geocoding_voronoi_idx 
            ON geocoding 
            USING GIST (voronoi);
        """)
