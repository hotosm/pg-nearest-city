"""Data import."""

from __future__ import annotations

import atexit
import csv
import itertools
import logging
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from osgeo import gdal

import psycopg
from psycopg.rows import DictRow, dict_row

from pg_nearest_city.datasets.registry import DatasetRegistry, GeoSource
from pg_nearest_city.datasets.sources import CGAZ_URLCONFIG, GEONAMES_URLCONFIG
from pg_nearest_city.datasets.types import CompressionAlgorithm, DownloadOutcome
from pg_nearest_city.db import queries
from pg_nearest_city.db.corrections import DATA_CORRECTIONS
from pg_nearest_city.db.data_cleanup import make_queries
from pg_nearest_city.db.settings import DBConnSettings
from pg_nearest_city.db.tables import (
    IndexType,
    alter_post_tables,
    get_tables,
    setup_database,
)
from pg_nearest_city.scripts.dataset_fetcher import (
    extract_dataset,
    fetch_and_extract_dataset,
)

if TYPE_CHECKING:
    from pg_nearest_city.db.settings import DBConfigSetting

gdal.UseExceptions()


@dataclass
class Config:
    """Configuration for the data loading pipeline."""

    # DB configuration
    db_conn_settings: DBConnSettings = DBConnSettings()
    db_config_settings: list[DBConfigSetting] = field(default_factory=list)
    db_config_settings_initial: list[DBConfigSetting] = field(default_factory=list)

    # Cache configuration
    cache_dir: Path = Path("/data/cache")
    cache_files: bool = True
    cur_dt: datetime = datetime.now()

    # Output configuration
    compression: CompressionAlgorithm = CompressionAlgorithm.AUTO
    output_dir: Path = Path("/data/output")
    update_package: bool = False

    # Processing options
    country_filter: str | None = None  # Alpha-2 code from CLI (e.g., "IT")

    # Temporary table names
    tmp_tables: list[str] = field(default_factory=list)

    # Miscellaneous config
    country_names_path = Path(__file__).parent / "country_names.csv"
    boundary_overrides_path = Path(__file__).parent / "boundary_overrides.csv.xz"
    temp_dir = Path(tempfile.mkdtemp(prefix="data_loader_"))

    def _load_iso_mapping(self) -> tuple[dict[str, str], dict[str, str]]:
        """Return alpha-2 to alpha-3 and alpha-3 to alpha-2 lookups."""
        with self.country_names_path.open(newline="", encoding="utf-8") as fh:
            codes = [(row[0], row[1]) for row in csv.reader(fh)]
        return dict(codes), {alpha3: alpha2 for alpha2, alpha3 in codes}

    def __post_init__(self):
        self.db_conn_str = self.db_conn_settings.conn_string

        # Resolve country filter: alpha-2 → alpha-3 set with enclave deps
        self.country_filter_alpha3: set[str] | None = None
        self.country_filter_alpha2: set[str] | None = None
        if self.country_filter:
            a2_to_a3, a3_to_a2 = self._load_iso_mapping()
            target_a3 = a2_to_a3.get(self.country_filter.upper())
            if not target_a3:
                raise ValueError(
                    f"Unknown country code: {self.country_filter!r}. "
                    "Use an ISO 3166-1 alpha-2 code (e.g. IT, FR, NL)."
                )
            from pg_nearest_city.datasets.sources import get_required_countries

            required_a3 = get_required_countries(target_a3)
            self.country_filter_alpha3 = required_a3
            self.country_filter_alpha2 = {
                a3_to_a2[a3] for a3 in required_a3 if a3 in a3_to_a2
            }


class DataLoader:
    """Orchestrates fetching, processing, and importing datasets into the database."""

    def __init__(self, config: Config, logger: logging.Logger | None = None):
        self.config = config
        self.conn = cast(
            psycopg.Connection[DictRow],
            psycopg.connect(self.config.db_conn_str, row_factory=dict_row),  # type: ignore[invalid-argument-type]
        )
        self.logger = logger or logging.getLogger("data_loader")

        if self.config.cache_files:
            registry_path = self.config.cache_dir / "dataset_registry.json"
            self.registry = DatasetRegistry(registry_path, logger=self.logger)
        else:
            self.logger.info("Caching disabled; downloads will use temp directory")
            self.config.cache_dir = self.config.temp_dir
            self.registry = DatasetRegistry(
                logger=self.logger,
                persist=False,
            )

        atexit.register(self._cleanup_temp_dir)

    def _setup_db(self, conn) -> None:
        setup_database(conn)

    def _cleanup_temp_dir(self):
        """Clean up temporary directory."""
        if self.config.temp_dir and self.config.temp_dir.exists():
            try:
                shutil.rmtree(self.config.temp_dir)
                self.logger.info(
                    f"Cleaned up temporary directory: {self.config.temp_dir}"
                )
            except (FileNotFoundError, OSError, PermissionError) as e:
                self.logger.warning(f"Failed to clean up temporary directory: {e}")

    def _alter_db_params(self, conn) -> None:
        """Alter DB parameters for faster work while loading."""
        for setting in self.config.db_config_settings:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SET {setting.name}={setting.value}")
                    if not conn.autocommit:
                        conn.commit()
            except Exception as e:
                if not conn.autocommit:
                    conn.rollback()
                self.logger.error(f"Failed to set {setting.name}={setting.value}: {e}")

    def _alter_post_tables(self, conn) -> None:
        alter_post_tables(conn)

    def _alter_table_params(self, conn, init: bool) -> None:
        """Alter table parameters for faster loading, and then durability when done."""
        update_tables_sql: str = "ALTER TABLE {} SET {}"
        # NOTE: order matters - a logged table can't reference an unlogged table
        tables = ["geocoding", "country"]
        for table, param in itertools.product(
            tables if init else reversed(tables),
            (
                "UNLOGGED" if init else "LOGGED",
                f"(autovacuum_enabled={'false' if init else 'true'})",
            ),
        ):
            try:
                with conn.cursor() as cur:
                    self.logger.info(f"Altering table {table} - SET {param}")
                    cur.execute(update_tables_sql.format(table, param))
                if not conn.autocommit:
                    conn.commit()
            except Exception as e:
                if not conn.autocommit:
                    conn.rollback()
                self.logger.error(f"Failed to alter table {table} - SET {param}: {e}")

    def _download_geonames(self) -> None:
        """Download GeoNames cities dataset."""
        self.logger.info("Downloading GeoNames dataset")
        result = fetch_and_extract_dataset(
            url_config=GEONAMES_URLCONFIG,
            geosource=GeoSource.GEONAMES,
            cache_dir=self.config.cache_dir,
            temp_dir=self.config.temp_dir,
            registry=self.registry,
            logger=self.logger,
        )
        if result == DownloadOutcome.FAILED:
            raise RuntimeError("Failed to download GeoNames dataset")

    def _download_boundaries(self) -> None:
        """Download the CGAZ country boundaries."""
        self.logger.info("Downloading CGAZ dataset")
        result = fetch_and_extract_dataset(
            url_config=CGAZ_URLCONFIG,
            geosource=GeoSource.CGAZ,
            cache_dir=self.config.cache_dir,
            temp_dir=self.config.temp_dir,
            registry=self.registry,
            logger=self.logger,
        )
        if result == DownloadOutcome.FAILED:
            raise RuntimeError("Failed to download CGAZ dataset")

    def _clean_all_tables(self) -> None:
        """Drop ALL project tables (permanent and temp) for --clean recovery."""
        self.logger.info("Dropping all project tables")
        with self.conn.cursor() as cur:
            for table in get_tables():
                try:
                    cur.execute(queries.drop_table(table_name=table.name))
                except Exception as e:
                    self.logger.warning(f"Failed to drop {table.name}: {e}")
        self.conn.commit()

    def _resolve_compression(self) -> CompressionAlgorithm:
        """Resolve AUTO compression to a concrete algorithm.

        AUTO always prefers xz for best compression ratio. The export step
        runs infrequently, so the speed tradeoff is acceptable. Falls back
        through the availability chain if lzma is unavailable.
        """
        from pg_nearest_city.utils import _HAS_BZ2, _HAS_LZMA

        algo = self.config.compression
        if algo != CompressionAlgorithm.AUTO:
            return algo

        if _HAS_LZMA:
            return CompressionAlgorithm.XZ
        if _HAS_BZ2:
            return CompressionAlgorithm.BZ2
        return CompressionAlgorithm.GZIP

    def _export_data(self) -> None:
        """Export country and geocoding tables as compressed CSV files.

        Country geometries are written as hex-encoded WKB by PostgreSQL's
        COPY, so the files round-trip losslessly through COPY FROM.
        """
        from pg_nearest_city.base_nearest_city import BaseNearestCity
        from pg_nearest_city.utils import open_compressed

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        algo = self._resolve_compression()
        ext = algo.extension
        self.logger.info(f"Using {algo.value} compression")

        country_path = (
            self.config.output_dir / f"{BaseNearestCity.COUNTRY_DATA_STEM}{ext}"
        )
        geocoding_path = (
            self.config.output_dir / f"{BaseNearestCity.GEOCODING_DATA_STEM}{ext}"
        )

        self.logger.info(f"Exporting country data to {country_path}")
        with self.conn.cursor() as cur:
            with open_compressed(country_path, "wb") as fh:
                with cur.copy(
                    "COPY country(alpha2, alpha3, name, geom) "
                    "TO STDOUT WITH (FORMAT CSV)"
                ) as copy:
                    for data in copy:
                        fh.write(data)

        self.logger.info(f"Exporting geocoding data to {geocoding_path}")
        with self.conn.cursor() as cur:
            with open_compressed(geocoding_path, "wb") as fh:
                with cur.copy(
                    "COPY geocoding(city, country, lat, lon) "
                    "TO STDOUT WITH (FORMAT CSV)"
                ) as copy:
                    for data in copy:
                        fh.write(data)

        self.conn.commit()
        self.logger.info(f"Data exported to {self.config.output_dir}")

        pkg_data_dir = Path(BaseNearestCity.DATA_PACKAGE_PATH)
        exported = [p for p in [country_path, geocoding_path] if p.exists()]
        if self.config.update_package:
            pkg_data_dir.mkdir(parents=True, exist_ok=True)
            for src in exported:
                dest = pkg_data_dir / src.name
                shutil.copy2(src, dest)
                self.logger.info(f"Copied {src.name} -> {dest}")
        elif self.config.output_dir.resolve() != pkg_data_dir.resolve():
            self.logger.info(
                f"To update bundled package data, re-run with --update-package "
                f"or copy files manually to {pkg_data_dir}"
            )

    def _clean_geonames(self):
        """Clean GeoNames data to simplified format."""
        from pg_nearest_city.utils import open_compressed

        self.logger.info("Cleaning GeoNames data to simplified format")

        geonames_filename = self.registry.get_by_source(source=GeoSource.GEONAMES)
        if not geonames_filename:
            raise RuntimeError(
                "Missing required boundary files in registry. "
                "Run dataset_fetcher.py first to download the data."
            )
        cached_geonames_path = self.registry.get_cached_path(
            filename=geonames_filename[0],
            cache_dir=self.config.cache_dir,
        )
        if not cached_geonames_path:
            raise RuntimeError(
                "Missing required boundary files in registry. "
                "Run dataset_fetcher.py first to download the data."
            )

        if not cached_geonames_path.exists():
            raise RuntimeError(
                "Registry has entries but files are missing from cache. "
                "Run dataset_fetcher.py first to download the data."
            )
        if not extract_dataset(
            src_file=cached_geonames_path, dst_dir=self.config.temp_dir
        ):
            raise RuntimeError("Failed to extract dataset")

        raw_file = self.config.temp_dir / Path(GEONAMES_URLCONFIG.slug).with_suffix(
            ".txt"
        )
        clean_file = self.config.temp_dir / "cities_clean.txt"

        # This is the file format expected by the package
        _simplified_file = Path(
            "_".join(re.split(r"(\d+)", Path(raw_file).stem)) + "simple"
        )
        simplified_file = self.config.temp_dir / _simplified_file.with_suffix(".txt")
        algo = self._resolve_compression()
        comp_ext = algo.extension
        simplified_compressed = self.config.temp_dir / _simplified_file.with_suffix(
            f".txt{comp_ext}"
        )

        # Output path for the package
        output_cities_compressed = (
            self.config.output_dir / _simplified_file.with_suffix(f".txt{comp_ext}")
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

            # Compress the simplified file using the resolved algorithm
            with open(simplified_file, "rb") as f_in:
                with open_compressed(simplified_compressed, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Save to the output directory (always, regardless of other settings)
            shutil.copy(simplified_compressed, output_cities_compressed)
            self.logger.info(
                f"Saved cities data ({algo.value}): {output_cities_compressed}"
            )

            # Verify the file was created
            if not output_cities_compressed.exists():
                self.logger.error(
                    f"Failed to save cities data: "
                    f"{output_cities_compressed} does not exist"
                )
                raise FileNotFoundError(
                    f"Failed to save cities data: {output_cities_compressed}"
                )

        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to create simplified data: {e}")
            raise

        self.logger.info(f"Data cleaned and saved to {clean_file}")
        return clean_file

    def _drop_temp_tables(self, conn) -> None:
        """Drop tables used for import which are no longer needed."""
        self.logger.info("Dropping temporary tables used for import")
        try:
            with conn.cursor() as cur:
                for table in get_tables(filters=[{"is_temp": True}]):
                    cur.execute(queries.drop_table(table_name=table.name))
        except Exception as e:
            self.logger.error(f"Failed to DROP TABLE {table.name}: {e}")
            raise

    def _import_geonames(self, conn) -> None:
        """Import the cleaned GeoNames data into PostgreSQL."""
        clean_file = self.config.temp_dir / "cities_clean.txt"
        self.logger.info(f"Importing GeoNames data from {clean_file}")

        if not clean_file.exists():
            self.logger.error(f"Clean data file not found: {clean_file}")
            raise FileNotFoundError(f"Clean data file not found: {clean_file}")

        copy_stmt = (
            "COPY geocoding(city, country, lat, lon) FROM STDIN DELIMITER E'\\t'"
        )
        alpha2_filter = self.config.country_filter_alpha2
        with conn.cursor() as cur:
            if alpha2_filter:
                self.logger.info(
                    f"Filtering GeoNames for countries: {sorted(alpha2_filter)}"
                )
            try:
                with open(clean_file, "r") as f:
                    with cur.copy(copy_stmt) as copy:
                        for line in f:
                            if alpha2_filter:
                                # TSV: city\tcountry\tlat\tlon
                                parts = line.split("\t", 3)
                                if len(parts) < 2 or parts[1] not in alpha2_filter:
                                    continue
                            copy.write(line)
                # Log record count
                cur.execute("SELECT COUNT(*) as count FROM geocoding")
                result = cur.fetchone()
                count = result["count"]
                self.logger.info(f"Imported {count} records")

                if count == 0:
                    self.logger.warning(
                        "No records were imported! Check your data source and filters."
                    )
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to import data: {e}")
                raise

    def _run_ogr_boundary_import(
        self, table_name: str, src_path: Path, sql_query: str
    ) -> None:
        """Load one boundary layer into a PostgreSQL staging table via GDAL."""
        gdal.SetConfigOption("PG_USE_COPY", "YES")
        try:
            result = gdal.VectorTranslate(
                f"PG:{self.config.db_conn_str}",
                str(src_path),
                format="PostgreSQL",
                SQLStatement=sql_query,
                SQLDialect="OGRSQL",
                layerName=table_name,
                geometryType="PROMOTE_TO_MULTI",
                layerCreationOptions=["GEOMETRY_NAME=geom", "SPATIAL_INDEX=NONE"],
            )
            if result is None:
                raise RuntimeError(
                    f"GDAL VectorTranslate failed for {table_name} "
                    f"from {src_path.name}: {gdal.GetLastErrorMsg()}"
                )
            result = None  # release dataset
        except Exception:
            self.logger.error(
                f"Failed to import boundaries into {table_name} from {src_path.name}"
            )
            raise
        finally:
            gdal.SetConfigOption("PG_USE_COPY", None)

    def _import_boundaries(self, conn) -> None:
        """Build country_init from CGAZ and the committed boundary overrides."""
        from pg_nearest_city.utils import open_compressed

        self.logger.info("Importing country boundaries")
        self._run_ogr_boundary_import(
            "tmp_cgaz",
            self.config.cache_dir / CGAZ_URLCONFIG.filename,
            queries.SELECT_CGAZ,
        )
        alpha2_filter = self.config.country_filter_alpha2
        try:
            with conn.cursor() as cur:
                with self.config.country_names_path.open("rb") as fh:
                    with cur.copy(queries.COPY_COUNTRY_NAMES) as copy:
                        copy.write(fh.read())
                with open_compressed(self.config.boundary_overrides_path) as fh:
                    with cur.copy(queries.COPY_BOUNDARY_OVERRIDES) as copy:
                        copy.write(fh.read())
                cur.execute(queries.BUILD_COUNTRY_INIT)
                if alpha2_filter:
                    cur.execute(
                        queries.DELETE_UNFILTERED_COUNTRIES, [sorted(alpha2_filter)]
                    )
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to build country boundaries: {e}")
            raise

    def _cleanup_datasets(self, conn):
        """Fix known issues with datasets."""
        self.logger.info("Cleaning up imported datasets")
        rows = DATA_CORRECTIONS
        query_data = zip(rows, make_queries(rows), strict=False)
        with conn.cursor() as cur:
            for query_info, query in query_data:
                try:
                    self.logger.info(query_info.description)
                    cur.execute(query)
                    if cur.rowcount == query_info.result_limit:
                        conn.commit()
                    elif cur.rowcount > query_info.result_limit:
                        self.logger.error(
                            f"Expected {query_info.result_limit} affected rows, "
                            f"got {cur.rowcount} affected rows - "
                            "tighten predicates and try again"
                        )
                        conn.rollback()
                        return
                    elif not cur.rowcount:
                        self.logger.warning(
                            f"Expected {query_info.result_limit} affected rows, "
                            "got 0 affected rows"
                        )
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Failed to update data: {e}")
                    raise

    def _create_spatial_indices(self, conn):
        """Create spatial indices for efficient processing."""
        self.logger.info("Creating spatial indices on geometry columns")
        with conn.cursor() as cur:
            for table in get_tables(
                filters=[
                    {
                        "is_for_data_load": False,
                        "indices": {"is_post_load": True, "index_type": "GIST"},
                    },
                    {
                        "is_for_data_load": True,
                        "is_temp": True,
                        "indices": {"is_post_load": True, "index_type": "GIST"},
                    },
                ]
            ):
                for index in table.get_indices():
                    if index.index_type != IndexType.GIST:
                        continue
                    try:
                        cur.execute(index.index_def)
                        conn.commit()
                        self.logger.info(
                            f"Spatial index {index.name} created on {table.name}"
                        )
                    except Exception as e:
                        conn.rollback()
                        self.logger.error(
                            f"Failed to create spatial index {index.name} "
                            f"on {table.name}: {e}"
                        )
                        raise

    def _create_btree_indices(self, conn):
        """Create B+tree indices."""
        self.logger.info("Creating B+tree indices")
        with conn.cursor() as cur:
            for table in get_tables(
                filters=[
                    {
                        "is_for_data_load": False,
                        "indices": {"is_post_load": True, "index_type": "BTREE"},
                    },
                    {
                        "is_externally_defined": True,
                        "is_for_data_load": True,
                        "indices": {"is_post_load": True, "index_type": "BTREE"},
                    },
                ]
            ):
                for index in table.get_indices():
                    if index.index_type != IndexType.BTREE:
                        continue
                    try:
                        cur.execute(index.index_def)
                        conn.commit()
                        self.logger.info(
                            f"B+tree index {index.name} created on {table.name}"
                        )
                    except Exception as e:
                        conn.rollback()
                        self.logger.error(
                            f"Failed to create B+tree index {index.name} "
                            f"on {table.name}: {e}"
                        )
                        raise

    def _simplify_country_table(self, conn) -> None:
        """Simplify the topology of the country table to reduce its size."""
        self.logger.info("Simplifying country topology - this may take some time")
        try:
            with conn.cursor() as cur:
                cur.execute(queries.SIMPLIFY_COUNTRY)
                conn.commit()
                self.logger.info("Country table geometry simplified")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to simplify country table geometry: {e}")
            raise

    def _vacuum_and_analyze_db(self, conn, full: bool):
        """Perform VACUUM (ANALYZE[, FULL]) on tables to cleanup dead tuples."""
        self.logger.info(
            f"Performing VACUUM (ANALYZE{', FULL' if full else ''}) on tables"
        )
        was_autocommit = conn.autocommit
        if not was_autocommit:
            conn.set_autocommit(True)
        with conn.cursor() as cur:
            try:
                cur.execute(queries.vacuum_analyze(full=full))
                self.logger.info("VACUUM complete")
            except Exception as e:
                self.logger.error(f"Failed to VACUUM tables: {e}")
                raise
            finally:
                if not was_autocommit:
                    conn.set_autocommit(False)
