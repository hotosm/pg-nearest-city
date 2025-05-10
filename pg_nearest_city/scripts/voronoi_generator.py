"""Generates the voronois.wkb file for pg-nearest-city.

This script downloads GeoNames data, processes it through PostGIS to compute
Voronoi polygons, and exports them as WKB for use with the pg-nearest-city package.

"""

import argparse
import atexit
import csv
import gzip
import logging
import os
import shutil
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg
from psycopg.rows import dict_row


@dataclass
class Config:
    """Configuration parameters for the Voronoi generator."""

    # Database connection from environment variables or defaults
    db_name: str = os.environ.get("PGNEAREST_DB_NAME", "postgres")
    db_user: str = os.environ.get("PGNEAREST_DB_USER", "postgres")
    db_password: str = os.environ.get("PGNEAREST_DB_PASSWORD", "postgres")
    db_host: str = os.environ.get("PGNEAREST_DB_HOST", "localhost")
    db_port: int = int(os.environ.get("PGNEAREST_DB_PORT", "5432"))

    # Data sources
    geonames_url: str = "http://download.geonames.org/export/dump/cities1000.zip"
    _zip_path: str = ""

    # Output configuration
    output_dir: Path = Path("/data/output")  # Default output directory
    compress_output: bool = True

    # Processing options
    country_filter: Optional[str] = None  # Optional filter for testing (e.g., "IT")

    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def ensure_output_directories(self):
        """Ensure all output directories exist."""
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)


class VoronoiGenerator:
    """Generates Voronoi WKB files from GeoNames data."""

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        """Initialise class."""
        self.config = config
        self.logger = logger or logging.getLogger("voronoi_generator")
        self.temp_dir = None

    def run_pipeline(self):
        """Execute the full data pipeline."""
        # Create temp directory and register cleanup
        self.temp_dir = Path(tempfile.mkdtemp(prefix="voronoi_generator_"))
        self.logger.info(f"Using temporary directory: {self.temp_dir}")

        # Register cleanup function to ensure temp directory is removed
        atexit.register(self._cleanup_temp_dir)

        try:
            # Ensure output directories exist
            self.config.ensure_output_directories()

            if not self.config._zip_path or not Path(self.config._zip_path).is_file():
                self._download_data()
                self.config._zip_path = ""
            self._extract_data()
            self._clean_data()

            # Connect to database
            with psycopg.connect(
                self.config.get_connection_string(), row_factory=dict_row
            ) as conn:
                # Run each stage with the same connection
                self._setup_database(conn)
                self._import_data(conn)
                self._create_spatial_index(conn)
                self._compute_voronoi(conn)
                self._export_wkb(conn)

            # Verify output files
            self._verify_output_files()

            self.logger.info("Pipeline completed successfully.")

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            # Cleanup is also handled by atexit, but we do it here as well
            # for good measure
            self._cleanup_temp_dir()

    def _cleanup_temp_dir(self):
        """Clean up temporary directory."""
        if self.temp_dir and self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            except (FileNotFoundError, OSError, PermissionError) as e:
                self.logger.warning(f"Failed to clean up temporary directory: {e}")

    def _setup_database(self, conn):
        """Set up the database schema and extensions."""
        self.logger.info("Setting up database schema")
        with conn.cursor() as cur:
            try:
                # Ensure PostGIS extension is installed
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

                # Drop table if exists to ensure clean state
                cur.execute("DROP TABLE IF EXISTS geocoding;")

                # Create geocoding table
                cur.execute("""
                    CREATE TABLE geocoding (
                        city varchar,
                        country varchar,
                        lat decimal,
                        lon decimal,
                        geom geometry(Point,4326) GENERATED ALWAYS AS (
                          ST_SetSRID(ST_MakePoint(lon, lat), 4326)
                        ) STORED,
                        voronoi geometry(Polygon,4326)
                    );
                """)
                conn.commit()
                self.logger.info("Database schema setup complete")
            except psycopg.errors.UndefinedFile as e:
                # Handle specific errors related to extensions
                self.logger.error(f"PostgreSQL extension error: {e}")
                self.logger.error(
                    "Make sure PostGIS is installed in your PostgreSQL instance"
                )
                raise
            except Exception as e:
                self.logger.error(f"Database setup error: {e}")
                raise

    def _download_data(self):
        """Download GeoNames data."""
        self.logger.info(f"Downloading data from {self.config.geonames_url}")

        zip_path = self.temp_dir / "cities1000.zip"

        try:
            urllib.request.urlretrieve(self.config.geonames_url, zip_path)
        except urllib.error.URLError as e:
            self.logger.error(f"Failed to download data: {e}")
            raise

    def _extract_data(self):
        """Extract GeoNames data."""
        zip_path = self.temp_dir / "cities1000.zip"

        if self.config._zip_path:
            shutil.copy2(self.config._zip_path, zip_path)
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.temp_dir)
        except (FileNotFoundError, PermissionError, zipfile.BadZipFile) as e:
            self.logger.error(f"Failed to extract zip file: {e}")
            raise

    def _clean_data(self):
        """Clean GeoNames data to the simplified format."""
        self.logger.info("Cleaning data to simplified format")

        raw_file = self.temp_dir / "cities1000.txt"
        clean_file = self.temp_dir / "cities_clean.txt"

        # This is the file format expected by the package
        simplified_file = self.temp_dir / "cities_1000_simple.txt"
        simplified_gz = self.temp_dir / "cities_1000_simple.txt.gz"

        # Output path for the package
        output_cities_gz = self.config.output_dir / "cities_1000_simple.txt.gz"

        try:
            with open(raw_file, "r", newline="") as f:
                tsv_raw = [x for x in csv.reader(f, delimiter="\t", escapechar="\\")]
        except csv.Error as e:
            self.logger.error(f"Failed to import data for cleaning: {e}")
            raise
        try:
            # Extract columns 2 (city), 9 (country), 5 (lat), 6 (lon)
            with open(clean_file, "w", newline="") as f:
                writer = csv.writer(
                    f,
                    delimiter="\t",
                    lineterminator="\n",
                    quoting=csv.QUOTE_NONE,
                    escapechar="\\",
                    doublequote=True,
                )
                for row in tsv_raw:
                    writer.writerow([row[1], row[8], row[4], row[5]])
        except csv.Error as e:
            self.logger.error(f"Failed to clean data: {e}")
            raise

        # Generate the simplified version in the exact format needed by the package
        try:
            # Copy to the specific filename expected by the package
            shutil.copy(clean_file, simplified_file)

            # Compress the simplified file
            with open(simplified_file, "rb") as f_in:
                with gzip.open(simplified_gz, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Save to the output directory (always, regardless of other settings)
            shutil.copy(simplified_gz, output_cities_gz)
            self.logger.info(f"Saved cities data for package: {output_cities_gz}")

            # Verify the file was created
            if not output_cities_gz.exists():
                self.logger.error(
                    f"Failed to save cities data: {output_cities_gz} does not exist"
                )
                raise FileNotFoundError(
                    f"Failed to save cities data: {output_cities_gz}"
                )

        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to create simplified data: {e}")
            raise

        self.logger.info(f"Data cleaned and saved to {clean_file}")
        return clean_file

    def _import_data(self, conn):
        """Import the cleaned data into PostgreSQL."""
        clean_file = self.temp_dir / "cities_clean.txt"
        self.logger.info(f"Importing data from {clean_file}")

        if not clean_file.exists():
            self.logger.error(f"Clean data file not found: {clean_file}")
            raise FileNotFoundError(f"Clean data file not found: {clean_file}")

        with conn.cursor() as cur:
            copy_stmt = [
                "COPY geocoding(city, country, lat, lon) FROM STDIN DELIMITER E'\\t'"
            ]
            # Apply country filter if specified
            if self.config.country_filter:
                self.logger.info(f"Filtering for country: {self.config.country_filter}")
                copy_stmt.append("WHERE country = %s")
            try:
                # Use COPY for efficient import
                with open(clean_file, "r") as f:
                    if self.config.country_filter:
                        with cur.copy(
                            " ".join(copy_stmt),
                            (self.config.country_filter,),
                        ) as copy:
                            for line in f:
                                copy.write(line)
                    else:
                        with cur.copy(" ".join(copy_stmt)) as copy:
                            for line in f:
                                copy.write(line)
                conn.commit()

                # Log record count
                cur.execute("SELECT COUNT(*) as count FROM geocoding")
                result = cur.fetchone()
                count = result["count"]
                self.logger.info(f"Imported {count} records")

                if count == 0:
                    self.logger.warning(
                        "No records were imported! Check your data source and filters."
                    )
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to import data: {e}")
                raise

    def _create_spatial_index(self, conn):
        """Create spatial index for efficient processing."""
        self.logger.info("Creating spatial index on geometry")
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "CREATE INDEX geocoding_geom_idx ON geocoding USING GIST(geom);"
                )
                conn.commit()
                self.logger.info("Spatial index created")
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to create spatial index: {e}")
                raise

    def _compute_voronoi(self, conn):
        """Compute Voronoi polygons using PostGIS."""
        self.logger.info("Computing Voronoi polygons")
        with conn.cursor() as cur:
            try:
                # First create a temporary table with all Voronoi polygons
                cur.execute("""
                    CREATE TEMP TABLE voronoi_temp AS
                    SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))).geom
                    FROM geocoding;
                """)

                # Update the main table by matching points to their containing polygons
                cur.execute("""
                    UPDATE geocoding g SET voronoi = v.geom
                    FROM voronoi_temp v
                    WHERE ST_Contains(v.geom, g.geom);
                """)

                conn.commit()

                # Verify results
                cur.execute(
                    """
                    SELECT COUNT(*) as with_voronoi
                    FROM geocoding WHERE voronoi IS NOT NULL
                    """
                )
                with_voronoi = cur.fetchone()["with_voronoi"]

                cur.execute("SELECT COUNT(*) as total FROM geocoding")
                total = cur.fetchone()["total"]

                self.logger.info(f"Voronoi polygons computed: {with_voronoi}/{total}")

                if with_voronoi < total:
                    self.logger.warning(
                        f"{total - with_voronoi} records did not get Voronoi polygons"
                    )

                if with_voronoi == 0:
                    self.logger.error("No Voronoi polygons were generated!")
                    raise Exception("Failed to generate any Voronoi polygons")

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to compute Voronoi polygons: {e}")
                raise

    def _export_wkb(self, conn):
        """Export the Voronoi polygons to WKB format."""
        # Output path for voronois.wkb.gz
        export_path = (
            self.config.output_dir / "voronois.wkb.gz"
            if self.config.compress_output
            else self.config.output_dir / "voronois.wkb"
        )

        self.logger.info(f"Exporting WKB to {export_path}")

        # Create a temporary file for export
        temp_wkb = self.temp_dir / "voronois.wkb"

        try:
            with conn.cursor() as cur:
                # Export using copy command
                with open(temp_wkb, "wb") as f:
                    with cur.copy(
                        """
                        COPY (SELECT city, country, ST_AsBinary(voronoi)
                        FROM geocoding WHERE voronoi IS NOT NULL) TO STDOUT
                        """
                    ) as copy:
                        for data in copy:
                            f.write(data)

                # Check if any data was exported
                if temp_wkb.stat().st_size == 0:
                    self.logger.error("No data was exported! WKB file is empty.")
                    raise ValueError("Export resulted in empty WKB file")

            # Compress if needed and save to output directory
            if self.config.compress_output:
                with open(temp_wkb, "rb") as f_in:
                    with gzip.open(export_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self.logger.info(f"Compressed WKB saved to {export_path}")
            else:
                shutil.copy(temp_wkb, export_path)
                self.logger.info(f"WKB saved to {export_path}")

            # Verify the output file exists and has content
            if not export_path.exists():
                self.logger.error(f"Output file was not created: {export_path}")
                raise FileNotFoundError(f"Output file was not created: {export_path}")

            if export_path.stat().st_size == 0:
                self.logger.error(f"Output file is empty: {export_path}")
                raise ValueError(f"Output file is empty: {export_path}")

        except Exception as e:
            self.logger.error(f"Failed to export WKB: {e}")
            raise

    def _verify_output_files(self):
        """Verify that all required output files exist."""
        cities_file = self.config.output_dir / "cities_1000_simple.txt.gz"
        voronoi_file = (
            self.config.output_dir / "voronois.wkb.gz"
            if self.config.compress_output
            else self.config.output_dir / "voronois.wkb"
        )

        files_exist = True

        if not cities_file.exists():
            self.logger.error(f"Required output file missing: {cities_file}")
            files_exist = False
        else:
            self.logger.info(f"Verified output file: {cities_file}")

        if not voronoi_file.exists():
            self.logger.error(f"Required output file missing: {voronoi_file}")
            files_exist = False
        else:
            self.logger.info(f"Verified output file: {voronoi_file}")

        if not files_exist:
            raise FileNotFoundError("One or more required output files are missing")

        self.logger.info("All required output files have been created successfully:")
        self.logger.info(f"  - {cities_file}")
        self.logger.info(f"  - {voronoi_file}")


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger("voronoi_generator")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Voronoi WKB file from GeoNames data"
    )
    group_db = parser.add_argument_group("group_db")
    group_db.add_argument("--db-host", help="Database host")
    group_db.add_argument("--db-port", type=int, help="Database port")
    group_db.add_argument("--db-name", help="Database name")
    group_db.add_argument("--db-user", help="Database username")
    group_db.add_argument("--db-password", help="Database password")

    parser.add_argument("--country", help="Filter to specific country code (e.g. IT)")
    parser.add_argument(
        "--no-compress", action="store_true", help="Don't compress output"
    )
    parser.add_argument(
        "--output-dir", default="/data/output", help="Directory for output files"
    )
    parser.add_argument(
        "--zip-path", help="Path to existing cities1000.zip (avoids re-downloading)"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger = setup_logging()

    # Create config with consistent Path objects
    config = Config(
        output_dir=Path(args.output_dir),
        compress_output=not args.no_compress,
        country_filter=args.country,
    )

    # Override config with command line args if provided
    config.db_host = args.db_host or config.db_host
    config.db_port = args.db_port or config.db_port
    config.db_name = args.db_name or config.db_name
    config.db_user = args.db_user or config.db_user
    config.db_password = args.db_password or config.db_password
    config._zip_path = args.zip_path or config._zip_path

    generator = VoronoiGenerator(config, logger)

    try:
        generator.run_pipeline()
        logger.info("Generation complete!")

        # Print summary info
        logger.info("\nOutput files created:")
        logger.info(f"  - {config.output_dir}/cities_1000_simple.txt.gz")
        logger.info(
            f"  - {config.output_dir}/voronois.wkb"
            f"{'.gz' if config.compress_output else ''}"
        )
        logger.info("\nThese files are ready for use with the pg-nearest-city package.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise SystemExit(1) from e
