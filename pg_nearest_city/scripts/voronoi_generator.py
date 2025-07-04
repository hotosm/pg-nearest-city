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
import subprocess
import tempfile
import urllib.request
import zipfile
from collections import ChainMap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import psycopg
from psycopg.rows import dict_row

from pg_nearest_city.db.tables import get_tables_in_creation_order


@dataclass
class Header:
    key: str
    value: str

    # dataclass.asdict builds using attributes as keys; we just need a key:value pair
    def _to_dict(self):
        return {self.key: self.value}


@dataclass
class URLConfig:
    domain: str
    path: str
    scheme: str = "https"
    slug: str = field(init=False)
    zip_path: str = ""
    headers: list[Header] = field(default_factory=list)
    _headers: dict[str, str] = field(default_factory=dict)
    url: str = field(init=False)

    def __post_init__(self):
        self._headers = dict(ChainMap(*[header._to_dict() for header in self.headers]))
        self.domain = self.domain.rstrip("/")
        self.path = self.path.lstrip("/")
        self.slug = self.path.rsplit("/", maxsplit=1)[-1]
        self.url = f"{self.scheme}://{self.domain}/{self.path}"


@dataclass
class Config:
    """Configuration parameters for the Voronoi generator."""

    # Database connection from environment variables or defaults
    db_name: str = os.environ.get("PGNEAREST_DB_NAME", "postgres")
    db_user: str = os.environ.get("PGNEAREST_DB_USER", "postgres")
    db_password: str = os.environ.get("PGNEAREST_DB_PASSWORD", "postgres")
    db_host: str = os.environ.get("PGNEAREST_DB_HOST", "localhost")
    db_port: int = int(os.environ.get("PGNEAREST_DB_PORT", "5432"))

    # Output configuration
    output_dir: Path = Path("/data/output")  # Default output directory
    compress_output: bool = True

    # Processing options
    country_filter: Optional[str] = None  # Optional filter for testing (e.g., "IT")

    # Data sources
    country_boundaries: URLConfig = field(init=False)
    geonames: URLConfig = field(init=False)

    def __post_init__(self) -> None:
        self.geonames: URLConfig = URLConfig(
            domain="download.geonames.org",
            path="export/dump/cities1000.zip",
            zip_path="cities1000.zip",
        )
        self.country_boundaries: URLConfig = URLConfig(
            domain="www.naturalearthdata.com",
            # This path is not a typo, it really has http//www... after the domain
            path="http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip",
            zip_path="countries10m.zip",
            headers=[
                Header(key="Referer", value="https://www.naturalearthdata.com/"),
                Header(key="User-Agent", value="curl/8.7.1"),
            ],
        )

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
        self.temp_dir: Path | None = None

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

            for url_config in (self.config.geonames, self.config.country_boundaries):
                if not url_config.zip_path or not Path(url_config.zip_path).is_file():
                    self._download_data(url_config)
                self._extract_data(url_config)
            self._clean_geonames()

            # Connect to database
            with psycopg.connect(
                self.config.get_connection_string(), row_factory=dict_row
            ) as conn:
                # Run each stage with the same connection
                self._setup_database(conn)
                self._setup_country_table(conn)
                self._import_geonames(conn)
                self._import_country_boundaries(conn)
                self._create_country_index(conn)
                self._create_spatial_indices(conn)
                self._compute_voronoi(conn)
                self._export_wkb(conn)

            # Verify output files
            self._verify_output_files()

            self.logger.info("Pipeline completed successfully.")

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
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
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                for table in get_tables_in_creation_order():
                    if table.drop_first:
                        cur.execute(
                            f"DROP TABLE {'IF EXISTS' if table.safe_ops else ''} "
                            f"{table.name}"
                        )
                    if table.safe_ops:
                        cur.execute(
                            table.sql.replace(
                                "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
                            )
                        )
                    else:
                        cur.execute(table.sql)
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

    def _setup_country_table(self, conn):
        """Import data to the country lookup table, ignoring duplicates."""
        self.logger.info("Importing data to `country`")

        with conn.cursor() as cur:
            copy_stmt = "COPY country_tmp FROM STDIN WITH (FORMAT CSV, HEADER)"
            prep_stmt = [
                [
                    "CREATE TEMP TABLE country_tmp",
                    "ON COMMIT DROP",
                    "AS SELECT alpha2, alpha3, numeric, name",
                    "FROM country WITH NO DATA",
                ],
                [
                    "INSERT INTO country (alpha2, alpha3, numeric, name)",
                    "SELECT *",
                    "FROM country_tmp",
                    "ORDER BY alpha2",
                    "ON CONFLICT",
                    "DO NOTHING",
                ],
            ]
            iso_path = (
                Path(__file__).resolve().parent.parent.joinpath("db/iso-3166-1.csv.gz")
            )
            try:
                cur.execute(" ".join(prep_stmt[0]))
                with gzip.open(iso_path, "r") as f:
                    with cur.copy(copy_stmt) as copy:
                        for line in f:
                            copy.write(line)
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to import data: {e}")
                raise
            cur.execute(" ".join(prep_stmt[1]))
            conn.commit()

    def _download_data(self, url_config: URLConfig):
        """Download data from a given URL."""
        self.logger.info(f"Downloading data from {url_config.domain}")

        assert isinstance(self.temp_dir, Path)
        zip_path = self.temp_dir / url_config.zip_path

        try:
            _request = urllib.request.Request(
                url_config.url, headers=url_config._headers
            )
            with urllib.request.urlopen(_request) as resp:
                with open(zip_path, "wb") as f:
                    shutil.copyfileobj(resp, f)
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            self.logger.error(f"Failed to download data: {e}")
            raise
        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to save data: {e}")
            raise

    def _extract_data(self, url_config: URLConfig):
        """Extract data from a given zip file."""
        assert isinstance(self.temp_dir, Path)
        zip_path = self.temp_dir / url_config.zip_path
        shutil.copy2(url_config.zip_path, zip_path)

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.temp_dir)
        except (FileNotFoundError, PermissionError, zipfile.BadZipFile) as e:
            self.logger.error(f"Failed to extract zip file: {e}")
            raise

    def _clean_geonames(self):
        """Clean GeoNames data to simplified format."""
        self.logger.info("Cleaning GeoNames data to simplified format")

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

    def _import_country_boundaries(self, conn) -> None:
        """Import the country boundaries into PostgreSQL."""
        self.logger.info("Importing NaturalEarth country boundaries data")

        assert isinstance(self.temp_dir, Path)
        if not shutil.which("ogr2ogr"):
            raise RuntimeError("Couldn't find ogr2ogr - please install it")
        _shpfile_path = Path(self.config.country_boundaries.slug)
        ogr_cmd: list[str] = [
            "ogr2ogr",
            "-nln",
            "tmp_country_bounds",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-lco",
            "PRECISION=NO",
            "-f",
            "PostgreSQL",
            "-sql",
            f"SELECT iso_a2_eh AS alpha2, iso_a3_eh AS alpha3, iso_n3_eh AS numeric FROM {_shpfile_path.stem}",
            f"PG:{self.config.get_connection_string()}",
            f"{self.temp_dir / _shpfile_path.with_suffix('.shp')}",
        ]

        update_sql: str = """
            UPDATE country
            SET geom = t.geom
            FROM tmp_country_bounds t
            WHERE country.alpha2 = t.alpha2
        """
        drop_sql: str = "DROP TABLE tmp_country_bounds"

        try:
            subprocess.run(ogr_cmd, check=True)
        except subprocess.CalledProcessError:
            self.logger.error(
                f"Failed to extract country boundaries from {_shpfile_path.with_suffix('.shp')}"
            )
            raise

        try:
            with conn.cursor() as cur:
                cur.execute(update_sql)
                # cur.execute(drop_sql)
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update country with geom: {e}")
            raise

    def _import_geonames(self, conn):
        """Import the cleaned GeoNames data into PostgreSQL."""
        clean_file = self.temp_dir / "cities_clean.txt"
        self.logger.info(f"Importing GeoNames data from {clean_file}")

        if not clean_file.exists():
            self.logger.error(f"Clean data file not found: {clean_file}")
            raise FileNotFoundError(f"Clean data file not found: {clean_file}")

        copy_stmt = [
            "COPY geocoding(city, country, lat, lon) FROM STDIN DELIMITER E'\\t'"
        ]
        with conn.cursor() as cur:
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

    def _create_spatial_indices(self, conn):
        """Create spatial indices for efficient processing."""
        self.logger.info("Creating spatial index on geometry")
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS geocoding_geom_idx ON geocoding USING GIST(geom)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS country_geom_idx ON country USING GIST(geom)"
                )
                conn.commit()
                self.logger.info("Spatial indices created")
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to create spatial indices: {e}")
                raise

    def _create_country_index(self, conn):
        """Create index on geocoding.country for FK."""
        self.logger.info("Creating B+tree index on country")
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS geocoding_country_idx ON geocoding (country)"
                )
                conn.commit()
                self.logger.info("country index created")
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to create index on country: {e}")
                raise

    def _compute_voronoi(self, conn):
        """Compute Voronoi polygons using PostGIS."""
        self.logger.info("Computing Voronoi polygons")
        with conn.cursor() as cur:
            try:
                # First create a temporary table with all Voronoi polygons
                cur.execute(
                    """
                    CREATE TEMP TABLE voronoi_temp ON COMMIT DROP AS
                    SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))).geom
                    FROM geocoding
                """
                )

                # Update the main table by matching points to their containing polygons
                cur.execute(
                    """
                    UPDATE geocoding g SET voronoi = v.geom
                    FROM voronoi_temp v
                    WHERE ST_Contains(v.geom, g.geom)
                """
                )

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
        "--zip-path-countries",
        help="Path to existing countries10m.zip (avoids re-downloading)",
    )
    parser.add_argument(
        "--zip-path-cities",
        help="Path to existing cities1000.zip (avoids re-downloading)",
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
