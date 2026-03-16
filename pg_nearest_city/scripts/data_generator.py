"""Generates data files for pg-nearest-city.

This script downloads GeoNames city data and GADM country boundary data, imports
them into PostGIS, and exports two files for bundling with the package:

- cities_<N>_simple.txt.gz  - city points (city, country, lat, lon)
- country_boundaries.wkb.gz - country boundary polygons keyed by ISO alpha-2

GADM 4.1 data (~500 MB) is downloaded automatically from UC Davis on first run
and cached in ./cache/ for subsequent runs.
"""

import argparse
import csv
import gzip
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.request
import zipfile
from collections import ChainMap
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pg_nearest_city
import psycopg
from osgeo import gdal
from pg_nearest_city.db.data_cleanup import (
    POST_COUNTRY_UPDATE_ROWS,
    PRE_COUNTRY_UPDATE_ROWS,
    make_queries,
)
from pg_nearest_city.db.tables import get_tables_in_creation_order
from psycopg.rows import dict_row


@dataclass
class Header:
    """Class to create a k:v pair instead of dataclasses' asdict method."""

    key: str
    value: str

    def _to_dict(self):
        return {self.key: self.value}


@dataclass
class URLConfig:
    """Class representing a data file.

    domain: the domain portion of the URL (i.e. after http[s]:// and before the first /)
    path: the path portion of the URL (i.e. everything after the domain)
    alpha3_column: the name of the column in the file with an ISO 3166-1 alpha-3 code
    scheme: the scheme portion of the URL (i.e. http, https)
    slug: the last portion of the URL (automatically generated)
    zip_name: the name of the file once downloaded
    headers: a list of Headers to be passed in (e.g. Referer, User-Agent)

    """

    domain: str
    path: str
    alpha3_column: str = ""
    scheme: str = "https"
    slug: str = field(init=False)
    zip_name: str = ""
    headers: list[Header] = field(default_factory=list)
    _headers: dict[str, str] = field(default_factory=dict)
    url: str = field(init=False)

    def __post_init__(self):
        """Creates necessary portions of dataclass from supplied values."""
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

    cur_dt: datetime = datetime.now()

    # Input directory - downloads and extracted source files are always stored
    # here so they survive container restarts.
    input_dir: Path = Path("/data/input")

    # Output configuration
    output_dir: Path = Path("/data/output")
    compress_output: bool = True

    # Processing options
    country_filter: Optional[str] = None  # Optional filter for testing (e.g., "IT")
    simplify_tolerance: float = 0.01  # ST_SimplifyPreserveTopology tolerance in degrees

    # Data sources
    country_boundaries: URLConfig = field(init=False)
    geonames: URLConfig = field(init=False)

    def __post_init__(self) -> None:
        """Creates URLConfig objects for Config."""
        self.geonames: URLConfig = URLConfig(
            domain="download.geonames.org",
            path="export/dump/cities500.zip",
            zip_name="cities500.zip",
        )
        # GADM 4.1 six-level GeoPackage - contains ADM_0 through ADM_5 layers.
        # ADM_0 (country boundaries) is extracted by the pipeline automatically.
        self.country_boundaries: URLConfig = URLConfig(
            alpha3_column="GID_0",
            domain="geodata.ucdavis.edu",
            path="gadm/gadm4.1/gadm_410-levels.zip",
            zip_name="gadm_410-levels.zip",
        )

    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def ensure_directories(self):
        """Ensure all directories exist."""
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


class VoronoiGenerator:
    """Generates geocoding data files (city points + country boundaries) from GeoNames and GADM."""

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        """Initialise class."""
        self.config = config
        self.logger = logger or logging.getLogger("data_generator")
        self.temp_dir: Path | None = None

    def run_pipeline(self):
        """Execute the full data pipeline."""
        # Create temp directory - cleaned up only on success so that a failed
        # run can be inspected and re-started without re-downloading large files.
        self.temp_dir = Path(tempfile.mkdtemp(prefix="data_generator_"))
        self.logger.info(f"Using temporary directory: {self.temp_dir}")

        try:
            # Ensure directories exist
            self.config.ensure_directories()

            # Download and extract GeoNames city data and GADM country boundaries.
            for url_config in (self.config.geonames, self.config.country_boundaries):
                zip_path = self.config.input_dir / url_config.zip_name
                if zip_path.is_file():
                    if not self._is_valid_zip(zip_path):
                        self.logger.warning(
                            f"{url_config.zip_name} exists but is not a valid zip "
                            "(possibly a partial download) - re-downloading"
                        )
                        zip_path.unlink()
                        self._download_data(url_config)
                    else:
                        self._check_cached_file_mtime(url_config)
                else:
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
                self._export_country_boundaries(conn)

            # Can't perform VACUUM inside of a transaction
            with psycopg.connect(
                self.config.get_connection_string(), autocommit=True
            ) as conn:
                self._vacuum_full_and_analyze_db(conn)

            # Verify output files
            self._verify_output_files()

            self.logger.info("Pipeline completed successfully.")

            # Only clean up temp dir on success - on failure it is preserved so
            # the next run can resume without re-downloading.
            self._cleanup_temp_dir()

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.logger.info(
                f"Temporary directory preserved for inspection: {self.temp_dir}"
            )
            raise

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
                cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
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

        # The ISO CSV has four columns (alpha2, alpha3, numeric, name).
        # Use a temp table that matches the CSV so COPY succeeds, then insert
        # only the columns that exist in the country table.
        with conn.cursor() as cur:
            copy_stmt = "COPY country_tmp FROM STDIN WITH (FORMAT CSV, HEADER)"
            iso_path = (
                Path(pg_nearest_city.__file__)
                .resolve()
                .parent.joinpath("db/iso-3166-1.csv.gz")
            )
            try:
                cur.execute(
                    "CREATE TEMP TABLE country_tmp "
                    "(alpha2 CHAR(2), alpha3 CHAR(3), numeric CHAR(3), name TEXT) "
                    "ON COMMIT DROP"
                )
                with gzip.open(iso_path, "r") as f:
                    with cur.copy(copy_stmt) as copy:
                        for line in f:
                            copy.write(line)
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to import data: {e}")
                raise
            cur.execute(
                "INSERT INTO country (alpha2, alpha3, name) "
                "SELECT alpha2, alpha3, name FROM country_tmp "
                "ORDER BY alpha2 ON CONFLICT DO NOTHING"
            )
            conn.commit()

    def _is_valid_zip(self, path: Path) -> bool:
        """Return True if path is a readable, non-empty zip file."""
        try:
            with zipfile.ZipFile(path, "r") as zf:
                return len(zf.namelist()) > 0
        except (zipfile.BadZipFile, OSError):
            return False

    def _download_data(self, url_config: URLConfig):
        """Download data from a given URL."""
        self.logger.info(f"Downloading data from {url_config.domain}")

        zip_name = self.config.input_dir / url_config.zip_name

        try:
            _request = urllib.request.Request(
                url_config.url, headers=url_config._headers
            )
            with urllib.request.urlopen(_request) as resp:
                total_size = resp.headers.get("Content-Length")
                total_bytes = (
                    int(total_size) if total_size and total_size.isdigit() else 0
                )
                if total_bytes:
                    self.logger.info(
                        "Expected download size for %s: %.1f MB",
                        url_config.zip_name,
                        total_bytes / (1024 * 1024),
                    )
                else:
                    self.logger.info(
                        "Server did not provide total size for %s; showing bytes downloaded only",
                        url_config.zip_name,
                    )

                chunk_size = 1024 * 1024  # 1 MB chunks
                downloaded = 0
                start_time = time.monotonic()
                last_log = start_time
                last_percent = -1
                with open(zip_name, "wb") as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.monotonic()
                        elapsed = max(now - start_time, 1e-9)
                        # Log at most every ~2 seconds, or each +5% increment.
                        should_log = now - last_log >= 2
                        if total_bytes:
                            percent = int((downloaded * 100) / total_bytes)
                            if percent >= last_percent + 5:
                                should_log = True
                        else:
                            percent = -1

                        if should_log:
                            speed_mbps = (downloaded / (1024 * 1024)) / elapsed
                            if total_bytes:
                                self.logger.info(
                                    "Download progress %s: %d%% (%.1f/%.1f MB) at %.2f MB/s",
                                    url_config.zip_name,
                                    percent,
                                    downloaded / (1024 * 1024),
                                    total_bytes / (1024 * 1024),
                                    speed_mbps,
                                )
                                last_percent = percent
                            else:
                                self.logger.info(
                                    "Download progress %s: %.1f MB at %.2f MB/s",
                                    url_config.zip_name,
                                    downloaded / (1024 * 1024),
                                    speed_mbps,
                                )
                            last_log = now

                total_elapsed = max(time.monotonic() - start_time, 1e-9)
                avg_speed_mbps = (downloaded / (1024 * 1024)) / total_elapsed
                self.logger.info(
                    "Finished downloading %s: %.1f MB in %.1fs (avg %.2f MB/s)",
                    url_config.zip_name,
                    downloaded / (1024 * 1024),
                    total_elapsed,
                    avg_speed_mbps,
                )
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            self.logger.error(f"Failed to download data: {e}")
            raise
        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to save data: {e}")
            raise

    def _check_cached_file_mtime(self, url_config: URLConfig):
        """Check modification time of cached file to determine freshness."""
        zip_name = self.config.input_dir / url_config.zip_name
        assert zip_name.is_file()
        if datetime.fromtimestamp(
            zip_name.stat().st_mtime
        ) < self.config.cur_dt - timedelta(weeks=1):
            self.logger.warning(f"{url_config.zip_name} is more than one week old")
            if (
                _download_file := input(f"Download {url_config.zip_name} again (y/n)? ")
            ).lower() == "y":
                self._download_data(url_config)
                return
            self.logger.info(f"User declined to re-download {url_config.zip_name}")

    def _extract_data(self, url_config: URLConfig):
        """Extract data from a zip file into the source directory.

        Skips extraction if the first file in the zip already exists in the
        source directory - this avoids re-extracting large archives (e.g. the
        2.5 GB GADM GeoPackage) on re-runs after a pipeline failure.
        """
        zip_path = self.config.input_dir / url_config.zip_name
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                members = zip_ref.namelist()
                if members and (self.config.input_dir / members[0]).exists():
                    self.logger.info(
                        f"Skipping extraction of {url_config.zip_name} "
                        f"- {members[0]} already exists in {self.config.input_dir}"
                    )
                    return
                self.logger.info(f"Extracting {url_config.zip_name}")
                zip_ref.extractall(self.config.input_dir)
        except (FileNotFoundError, PermissionError, zipfile.BadZipFile) as e:
            self.logger.error(f"Failed to extract zip file: {e}")
            raise

    def _clean_geonames(self):
        """Clean GeoNames data to simplified format."""
        self.logger.info("Cleaning GeoNames data to simplified format")

        raw_file = self.config.input_dir / Path(
            self.config.geonames.zip_name
        ).with_suffix(".txt")
        clean_file = self.temp_dir / "cities_clean.txt"

        # This is the file format expected by the package
        _simplified_file = Path(
            "_".join(re.split(r"(\d+)", Path(raw_file).stem)) + "simple"
        )
        simplified_file = self.temp_dir / _simplified_file.with_suffix(".txt")
        simplified_gz = self.temp_dir / _simplified_file.with_suffix(".txt.gz")

        # Output path for the package
        output_cities_gz = self.config.output_dir / _simplified_file.with_suffix(
            ".txt.gz"
        )
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
        """Import country boundaries from the GADM 4.1 GeoPackage into PostgreSQL.

        The GADM zip contains a GeoPackage with layers ADM_0 through ADM_5.
        Both ADM_0 and a filtered subset of ADM_1 are imported as temporary tables
        via the GDAL Python API (gdal.VectorTranslate).

        Pipeline order within this method:
          1. Import ADM_0 → tmp_country_bounds_adm0
          2. Import ADM_1 (TWN, HKG, MAC, PSE, GRL, FRO, ALA) → tmp_country_bounds_adm1
          3. PRE_COUNTRY_UPDATE_ROWS cleanups (e.g. Kosovo alpha3 fix on adm0)
          4. UPDATE country.geom from tmp_country_bounds_adm0; drop adm0
          5. POST_COUNTRY_UPDATE_ROWS cleanups (e.g. populate territories missing from
             ADM_0, subtract self-governing territories from their parent country)
          6. Drop tmp_country_bounds_adm1
        """
        self.logger.info("Importing country boundaries data")

        # The zip extracts a GeoPackage; find it in the source directory.
        gpkg_files = list(self.config.input_dir.glob("*.gpkg"))
        if not gpkg_files:
            raise FileNotFoundError(
                f"No GeoPackage (.gpkg) found in {self.config.input_dir} after extraction. "
                "Expected the GADM 4.1 levels zip to contain a .gpkg file."
            )
        gpkg_path = gpkg_files[0]
        self.logger.info(f"Using GeoPackage: {gpkg_path.name}")

        alpha3_col = self.config.country_boundaries.alpha3_column  # GID_0

        gdal.UseExceptions()
        gdal.SetConfigOption("PG_USE_COPY", "YES")
        try:
            # Step 1 - import ADM_0 (country-level boundaries).
            # Use layers+selectFields (not SQLStatement) so GDAL includes geometry
            # automatically.  SQLStatement with the GeoPackage native SQL dialect
            # does not include geometry unless explicitly selected, and selecting it
            # as an attribute column bypasses PostGIS type registration.
            result = gdal.VectorTranslate(
                f"PG:{self.config.get_connection_string()}",
                str(gpkg_path),
                format="PostgreSQL",
                layers=["ADM_0"],
                selectFields=[alpha3_col],
                layerName="tmp_country_bounds_adm0",
                geometryType="PROMOTE_TO_MULTI",
                layerCreationOptions=["GEOMETRY_NAME=geom", "PRECISION=NO"],
                accessMode="overwrite",
            )
            if result is None:
                raise RuntimeError(
                    f"GDAL VectorTranslate failed importing ADM_0 from "
                    f"{gpkg_path.name}: {gdal.GetLastErrorMsg()}"
                )
            result = None  # release dataset

            # Step 2 - import ADM_1 for territories absent from or overlapping ADM_0.
            #
            # TWN: GADM 4.1 includes Taiwan's territory inside China's ADM_0 polygon.
            #      The union of TWN's ADM_1 features is subtracted from China's geometry
            #      in POST_COUNTRY_UPDATE_ROWS to produce a clean boundary.
            # HKG/MAC: Hong Kong and Macao SARs are also inside China's ADM_0 polygon
            #           and have no geometry in ADM_0 themselves.
            # PSE: Palestine has no geometry in ADM_0; its territory appears in ADM_1.
            # GRL/FRO: Greenland and Faroe Islands are self-governing territories within
            #          Denmark; their geometry may not appear in ADM_0 and Denmark's
            #          ADM_0 polygon likely encompasses their area.
            # ALA: Åland Islands are autonomous within Finland and absent from ADM_0.
            #
            # NOTE:
            # In ADM_1, HKG/MAC commonly have GID_0=CHN and must be selected by GID_1.
            adm1_territories = ("TWN", "HKG", "MAC", "PSE", "GRL", "FRO", "ALA")
            adm1_where = (
                f"{alpha3_col} IN {adm1_territories} "
                "OR GID_1 LIKE 'HKG.%' "
                "OR GID_1 LIKE 'MAC.%'"
            )
            result = gdal.VectorTranslate(
                f"PG:{self.config.get_connection_string()}",
                str(gpkg_path),
                format="PostgreSQL",
                layers=["ADM_1"],
                selectFields=[alpha3_col, "GID_1"],
                where=adm1_where,
                layerName="tmp_country_bounds_adm1",
                geometryType="PROMOTE_TO_MULTI",
                layerCreationOptions=["GEOMETRY_NAME=geom", "PRECISION=NO"],
                accessMode="overwrite",
            )
            if result is None:
                raise RuntimeError(
                    f"GDAL VectorTranslate failed importing ADM_1 "
                    f"({', '.join(adm1_territories)}) from "
                    f"{gpkg_path.name}: {gdal.GetLastErrorMsg()}"
                )
            result = None  # release dataset
        except Exception as e:
            self.logger.error(
                f"GDAL failed importing boundaries from {gpkg_path.name}: {e}"
            )
            raise
        finally:
            gdal.SetConfigOption("PG_USE_COPY", None)

        # GDAL may name the geometry column differently depending on version/driver
        # (e.g. wkb_geometry, the_geom).  Normalise to 'geom' so that all subsequent
        # SQL and cleanup queries can use a consistent column name.
        self._normalize_tmp_geom_column(conn, "tmp_country_bounds_adm0")
        self._normalize_tmp_geom_column(conn, "tmp_country_bounds_adm1")
        self._normalize_adm1_alpha3(conn)

        # Step 3 - pre-update cleanups (runs before country.geom is populated).
        self._run_cleanup_rows(conn, PRE_COUNTRY_UPDATE_ROWS)

        # Step 4 — copy geometries from ADM_0 into the country table, then drop adm0.
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE country SET geom = t.geom "
                    "FROM tmp_country_bounds_adm0 t "
                    "WHERE country.alpha3 = t.alpha3"
                )
                updated = cur.rowcount
                self.logger.info(f"Updated geom for {updated} countries")
                cur.execute("DROP TABLE IF EXISTS tmp_country_bounds_adm0")
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update country with geom: {e}")
            raise

        # Step 5 — promote selected ADM_1 territories into country.geom.
        #
        # Some territories (e.g. HKG/MAC/PSE) are missing from ADM_0 in GADM 4.1.
        # Merge each territory's ADM_1 parts and write them into country.geom by alpha3.
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "WITH adm1_union AS ("
                    "  SELECT alpha3, ST_Union(geom) AS geom "
                    "  FROM tmp_country_bounds_adm1 "
                    "  GROUP BY alpha3"
                    ") "
                    "UPDATE country c "
                    "SET geom = a.geom "
                    "FROM adm1_union a "
                    "WHERE c.alpha3 = a.alpha3"
                )
                updated = cur.rowcount
                self.logger.info(
                    "Updated geom from ADM_1 territories for %d countries", updated
                )
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update country with ADM_1 geom: {e}")
            raise

        # Step 6 — post-update cleanups (country.geom now populated; adm1 still exists).
        self._run_cleanup_rows(conn, POST_COUNTRY_UPDATE_ROWS)

        # Step 7 — drop the ADM_1 temp table.
        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS tmp_country_bounds_adm1")
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to drop tmp_country_bounds_adm1: {e}")
            raise

    def _normalize_tmp_geom_column(self, conn, table_name: str) -> None:
        """Normalise column names on a GDAL-created temp table.

        Ensures:
        - the geometry column is named 'geom' (GDAL uses various defaults)
        - the alpha-3 attribute column is named 'alpha3' (GDAL preserves the
          source field name, e.g. 'GID_0')
        """
        alpha3_col = self.config.country_boundaries.alpha3_column  # e.g. GID_0
        with conn.cursor() as cur:
            # --- geometry column ---
            cur.execute(
                "SELECT f_geometry_column FROM geometry_columns "
                "WHERE f_table_name = %s LIMIT 1",
                (table_name,),
            )
            row = cur.fetchone()
            if not row:
                # Fallback: look in information_schema for any geometry-typed column
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND udt_name = 'geometry' LIMIT 1",
                    (table_name,),
                )
                row = cur.fetchone()
            if not row:
                raise RuntimeError(
                    f"No geometry column found for table {table_name!r} - "
                    "GDAL import may have failed silently"
                )
            actual_geom_col = (
                row["f_geometry_column"]
                if "f_geometry_column" in row
                else row["column_name"]
            )
            if actual_geom_col != "geom":
                self.logger.info(
                    f"Renaming geometry column {actual_geom_col!r} → 'geom' in {table_name}"
                )
                cur.execute(
                    f'ALTER TABLE "{table_name}" RENAME COLUMN "{actual_geom_col}" TO geom'
                )

            # --- alpha3 attribute column ---
            # PostgreSQL stores unquoted identifiers in lowercase, so compare
            # case-insensitively and use the actual stored name for ALTER TABLE.
            if alpha3_col.lower() != "alpha3":
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND LOWER(column_name) = LOWER(%s) LIMIT 1",
                    (table_name, alpha3_col),
                )
                row = cur.fetchone()
                if row:
                    actual_alpha3_col = row["column_name"]
                    self.logger.info(
                        f"Renaming attribute column {actual_alpha3_col!r} → 'alpha3' in {table_name}"
                    )
                    cur.execute(
                        f'ALTER TABLE "{table_name}" RENAME COLUMN "{actual_alpha3_col}" TO alpha3'
                    )

            conn.commit()

    def _normalize_adm1_alpha3(self, conn) -> None:
        """Normalize tmp_country_bounds_adm1 alpha3 values for nested territories."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'tmp_country_bounds_adm1' "
                "AND LOWER(column_name) = 'gid_1' LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                self.logger.warning(
                    "tmp_country_bounds_adm1 has no GID_1 column; skipping alpha3 remap"
                )
                conn.commit()
                return

            gid1_col = row["column_name"]
            cur.execute(
                f"""
                UPDATE tmp_country_bounds_adm1
                SET alpha3 = CASE
                    WHEN "{gid1_col}" LIKE 'HKG.%' THEN 'HKG'
                    WHEN "{gid1_col}" LIKE 'MAC.%' THEN 'MAC'
                    WHEN "{gid1_col}" LIKE 'PSE.%' THEN 'PSE'
                    WHEN "{gid1_col}" LIKE 'TWN.%' THEN 'TWN'
                    WHEN "{gid1_col}" LIKE 'GRL.%' THEN 'GRL'
                    WHEN "{gid1_col}" LIKE 'FRO.%' THEN 'FRO'
                    WHEN "{gid1_col}" LIKE 'ALA.%' THEN 'ALA'
                    ELSE alpha3
                END
                """
            )
            self.logger.info(
                "Normalized alpha3 for ADM_1 territories (rows touched: %d)",
                cur.rowcount,
            )
            conn.commit()

    def _run_cleanup_rows(self, conn, rows):
        """Execute a list of RowData cleanup queries, checking affected row counts."""
        if not rows:
            return
        query_data = zip(rows, make_queries(rows), strict=False)
        with conn.cursor() as cur:
            for query_info, query in query_data:
                try:
                    cur.execute(query)
                    if cur.rowcount == query_info.result_limit:
                        conn.commit()
                        continue
                    elif cur.rowcount > query_info.result_limit:
                        conn.rollback()
                        raise RuntimeError(
                            f"Cleanup query affected {cur.rowcount} rows, "
                            f"expected at most {query_info.result_limit} - "
                            "tighten predicates and try again"
                        )
                    elif not cur.rowcount:
                        self.logger.warning(
                            f"Expected {query_info.result_limit} affected rows, "
                            "got 0 affected rows"
                        )
                        conn.commit()
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Failed to update data: {e}")
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
        self.logger.info("Creating spatial indices on geometry columns")
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS geocoding_country_geom_gist_idx "
                    "ON geocoding USING GIST (country, geom)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS country_geom_idx "
                    "ON country USING GIST(geom)"
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
                    "CREATE INDEX IF NOT EXISTS geocoding_country_idx "
                    "ON geocoding (country)"
                )
                conn.commit()
                self.logger.info("B+tree index created on country")
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to create index on country: {e}")
                raise

    def _export_country_boundaries(self, conn):
        """Export country boundary geometries as a WKB file for bundling with the package.

        The output file (country_boundaries.wkb.gz) is tab-separated with columns:
        alpha2, wkb_binary - one row per country.  This file is loaded by the
        runtime initialization to populate country.geom without requiring an
        external download at query time.
        """
        export_path = (
            self.config.output_dir / "country_boundaries.wkb.gz"
            if self.config.compress_output
            else self.config.output_dir / "country_boundaries.wkb"
        )

        self.logger.info(f"Exporting country boundaries to {export_path}")

        temp_wkb = self.temp_dir / "country_boundaries.wkb"

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM country WHERE geom IS NOT NULL")
                count = cur.fetchone()["count"]
                if count == 0:
                    raise ValueError(
                        "No country boundary geometries found - "
                        "check that GADM data was imported successfully."
                    )
                self.logger.info(
                    f"Exporting boundaries for {count} countries "
                    f"(simplify tolerance: {self.config.simplify_tolerance}°)"
                )

                with open(temp_wkb, "wb") as f:
                    with cur.copy(
                        "COPY (SELECT alpha2, ST_AsBinary("
                        "ST_SimplifyPreserveTopology(geom, %s)) "
                        "FROM country WHERE geom IS NOT NULL) TO STDOUT",
                        (self.config.simplify_tolerance,),
                    ) as copy:
                        for data in copy:
                            f.write(data)

                if temp_wkb.stat().st_size == 0:
                    raise ValueError("Export resulted in empty country boundaries file")

            if self.config.compress_output:
                with open(temp_wkb, "rb") as f_in:
                    with gzip.open(export_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self.logger.info(
                    f"Compressed country boundaries saved to {export_path}"
                )
            else:
                shutil.copy(temp_wkb, export_path)
                self.logger.info(f"Country boundaries saved to {export_path}")

            if not export_path.exists() or export_path.stat().st_size == 0:
                raise FileNotFoundError(
                    f"Country boundaries output file missing or empty: {export_path}"
                )

        except Exception as e:
            self.logger.error(f"Failed to export country boundaries: {e}")
            raise

    def _verify_output_files(self):
        """Verify that all required output files exist."""
        # Derive the cities filename from the geonames zip name (e.g. cities500 -> cities_500_simple.txt.gz)
        match = re.match(r"([a-z]+)([0-9]+)", self.config.geonames.zip_name, re.I)
        if match:
            cities_filename = f"{'_'.join(match.groups())}_simple.txt.gz"
        else:
            cities_filename = "cities_simple.txt.gz"
        cities_file = self.config.output_dir / cities_filename

        boundaries_file = (
            self.config.output_dir / "country_boundaries.wkb.gz"
            if self.config.compress_output
            else self.config.output_dir / "country_boundaries.wkb"
        )

        files_exist = True

        if not cities_file.exists():
            self.logger.error(f"Required output file missing: {cities_file}")
            files_exist = False
        else:
            self.logger.info(f"Verified output file: {cities_file}")

        if not boundaries_file.exists():
            self.logger.error(f"Required output file missing: {boundaries_file}")
            files_exist = False
        else:
            self.logger.info(f"Verified output file: {boundaries_file}")

        if not files_exist:
            raise FileNotFoundError("One or more required output files are missing")

        self.logger.info("All required output files have been created successfully:")
        self.logger.info(f"  - {cities_file}")
        self.logger.info(f"  - {boundaries_file}")

    def _vacuum_full_and_analyze_db(self, conn):
        """Perform VACUUM (ANALYZE, FULL) on tables to cleanup dead tuples."""
        self.logger.info("Performing VACUUM (ANALYZE, FULL) on geocoding tables")
        with conn.cursor() as cur:
            try:
                cur.execute("VACUUM (ANALYZE, FULL) geocoding")
                cur.execute("VACUUM (ANALYZE, FULL) country")
                self.logger.info("Tables vacuumed")
            except Exception as e:
                self.logger.error(f"Failed to vacuum tables: {e}")
                raise


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger("data_generator")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate geocoding data files from GeoNames and GADM"
    )
    group_db = parser.add_argument_group("group_db")
    group_db.add_argument("--db-host", help="Database host")
    group_db.add_argument("--db-port", type=int, help="Database port")
    group_db.add_argument("--db-name", help="Database name")
    group_db.add_argument("--db-user", help="Database username")
    group_db.add_argument("--db-password", help="Database password")

    parser.add_argument(
        "--input-dir",
        default="/data/input",
        help="Directory where downloaded source files are stored (persisted across runs)",
    )
    parser.add_argument("--country", help="Filter to specific country code (e.g. IT)")
    parser.add_argument(
        "--no-compress", action="store_true", help="Don't compress output"
    )
    parser.add_argument(
        "--output-dir", default="/data/output", help="Directory for output files"
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.01,
        help="ST_SimplifyPreserveTopology tolerance in degrees for country boundary export (default: 0.01 ≈ 1km)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger = setup_logging()

    # Create config with consistent Path objects
    config = Config(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        compress_output=not args.no_compress,
        country_filter=args.country,
        simplify_tolerance=args.simplify_tolerance,
    )

    # Override config with command line args if provided
    config.db_host = args.db_host or config.db_host
    config.db_port = args.db_port or config.db_port
    config.db_name = args.db_name or config.db_name
    config.db_user = args.db_user or config.db_user
    config.db_password = args.db_password or config.db_password

    generator = VoronoiGenerator(config, logger)
    geonames_output_match = re.match(
        r"([a-z]+)([0-9]+)", config.geonames.zip_name, re.I
    )
    if geonames_output_match:
        geonames_output = f"{'_'.join(geonames_output_match.groups())}_simple.txt.gz"
    else:
        logger.warning(
            "Failed to match filename for geonames simple output - "
            "check output directory for file like "
            f"'{Path(config.geonames.zip_name).stem}'"
        )
        geonames_output = "?"
    try:
        generator.run_pipeline()
        logger.info("Generation complete!")

        ext = ".gz" if config.compress_output else ""
        logger.info("\nOutput files created:")
        logger.info(f"  - {config.output_dir}/{geonames_output}")
        logger.info(f"  - {config.output_dir}/country_boundaries.wkb{ext}")
        logger.info("\nThese files are ready for use with the pg-nearest-city package.")
        logger.info(
            "Copy them to pg_nearest_city/data/ and pg_nearest_city/data/ respectively."
        )
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise Exception from e
