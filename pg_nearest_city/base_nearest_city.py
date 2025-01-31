from dataclasses import dataclass
from typing import Optional

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


class BaseNearestCity:

    @staticmethod
    def validate_coordinates(lon: float, lat: float) -> Optional[Location]:
        # Validate coordinate ranges
        if not -90 <= lat <= 90:
            raise ValueError(f"Latitude {lat} is outside valid range [-90, 90]")
        if not -180 <= lon <= 180:
            raise ValueError(f"Longitude {lon} is outside valid range [-180, 180]")


    @staticmethod
    def _get_table_existance_query() -> sql.SQL:
        return sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'pg_nearest_city_geocoding'
                );
            """)

    @staticmethod
    def _get_table_structure_query() -> sql.SQL:
        return sql.SQL("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'pg_nearest_city_geocoding'
        """)

    @staticmethod
    def _get_data_completeness_query() -> sql.SQL:
        return sql.SQL("""
                    SELECT 
                        COUNT(*) as total_cities,
                        COUNT(*) FILTER (WHERE voronoi IS NOT NULL) as cities_with_voronoi
                    FROM pg_nearest_city_geocoding;
                """)

    @staticmethod
    def _get_spatial_index_check_query() -> sql.SQL:
        return sql.SQL("""
            SELECT EXISTS (
                SELECT FROM pg_indexes 
                WHERE tablename = 'pg_nearest_city_geocoding' 
                AND indexname = 'geocoding_voronoi_idx'
            );
        """)
