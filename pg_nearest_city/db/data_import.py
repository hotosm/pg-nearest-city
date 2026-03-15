"""Data import."""

from __future__ import annotations

import atexit
import csv
import gzip
import itertools
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import psycopg
from psycopg.rows import DictRow, dict_row

from pg_nearest_city.datasets.geoboundaries import GeoBoundaryConfig
from pg_nearest_city.datasets.registry import DatasetRegistry, GeoSource
from pg_nearest_city.datasets.sources import (
    GADM_0_DATASET,
    GADM_1_DATASET,
    GADM_PROMOTED_TERRITORIES,
    GADM_URLCONFIG,
    GEONAMES_URLCONFIG,
    NE_ADM1_DATASET,
    NE_ADM1_URLCONFIG,
    NE_COUNTRIES_DATASET,
    NE_COUNTRIES_URLCONFIG,
    NE_PROMOTED_TERRITORIES,
    OVERPASS_URLCONFIG,
)
from pg_nearest_city.datasets.url_config import URLConfig
from pg_nearest_city.datasets.types import (
    BoundarySource,
    CompressionAlgorithm,
    DownloadOutcome,
    GeoBoundaryRelease,
)
from pg_nearest_city.db import queries
from pg_nearest_city.db.corrections import (
    BOUNDARY_CORRECTIONS,
    DATA_CORRECTIONS,
    GADM_DATA_CORRECTIONS,
    NE_BOUNDARY_CORRECTIONS,
    OVERPASS_BOUNDARY_TARGETS,
)
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
    fetch_geoboundaries,
)

if TYPE_CHECKING:
    from pg_nearest_city.db.settings import DBConfigSetting


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

    # Processing options
    boundary_source: BoundarySource = BoundarySource.GADM
    country_filter: str | None = None  # Optional filter for testing (e.g., "IT")

    # Temporary table names
    tmp_tables: list[str] = field(default_factory=list)

    # Miscellaneous config
    iso3166_path = Path(__file__).parent / "iso-3166-1.csv.gz"
    temp_dir = Path(tempfile.mkdtemp(prefix="data_loader_"))

    def __post_init__(self):
        self.db_conn_str = self.db_conn_settings.conn_string


@dataclass
class Prerequisite:
    """A CLI binary required by the pipeline."""

    binary_name: str
    pkg_name_apt: str
    pkg_name_yum: str
    pkg_name_zyp: str
    pkg_name_brew: str
    pkg_name_ports: str


class DataLoader:
    """Orchestrates fetching, processing, and importing datasets into the database."""

    def __init__(self, config: Config, logger: logging.Logger | None = None):
        self.config = config
        self.conn = cast(
            psycopg.Connection[DictRow],
            psycopg.connect(self.config.db_conn_str, row_factory=dict_row),  # type: ignore[invalid-argument-type]
        )
        self.logger = logger or logging.getLogger("data_loader")

        all_targets = list(BOUNDARY_CORRECTIONS)
        if config.boundary_source == BoundarySource.NATURAL_EARTH:
            all_targets.extend(NE_BOUNDARY_CORRECTIONS)
        self.geoboundaries_config = GeoBoundaryConfig(
            release=GeoBoundaryRelease.GB_OPEN,
            targets=tuple(all_targets),
        )

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

    def _check_prerequisites(self, conn) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchall()
        except Exception as e:
            self.logger.error(f"Failed to reach DB: {e}")

        for prog in ["ogr2ogr"]:
            if not shutil.which(prog):
                raise RuntimeError(f"{prog!r} not found - please install it")

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

    def _download_gadm(self) -> None:
        """Download GADM country boundaries dataset."""
        self.logger.info("Downloading GADM dataset")
        result = fetch_and_extract_dataset(
            url_config=GADM_URLCONFIG,
            geosource=GeoSource.GADM,
            cache_dir=self.config.cache_dir,
            temp_dir=self.config.temp_dir,
            registry=self.registry,
            logger=self.logger,
        )
        if result == DownloadOutcome.FAILED:
            raise RuntimeError("Failed to download GADM dataset")

    def _download_naturalearth(self) -> None:
        """Download Natural Earth country boundaries datasets."""
        self.logger.info("Downloading Natural Earth datasets")
        for url_config in (NE_COUNTRIES_URLCONFIG, NE_ADM1_URLCONFIG):
            result = fetch_and_extract_dataset(
                url_config=url_config,
                geosource=GeoSource.NATURAL_EARTH,
                cache_dir=self.config.cache_dir,
                temp_dir=self.config.temp_dir,
                registry=self.registry,
                logger=self.logger,
            )
            if result == DownloadOutcome.FAILED:
                raise RuntimeError(
                    f"Failed to download Natural Earth dataset: {url_config.filename}"
                )

    def _download_boundaries(self) -> None:
        """Download country boundary dataset from the configured source."""
        if self.config.boundary_source == BoundarySource.GADM:
            self._download_gadm()
        else:
            self._download_naturalearth()

    def _download_geoboundaries(self) -> None:
        """Download GeoBoundary correction datasets."""
        self.logger.info("Downloading GeoBoundary datasets")
        results = fetch_geoboundaries(
            gb_config=self.geoboundaries_config,
            cache_dir=self.config.cache_dir / "geoboundaries",
            registry=self.registry,
            logger=self.logger,
        )
        failed = [k for k, (o, _) in results.items() if o == DownloadOutcome.FAILED]
        if failed:
            self.logger.error(f"Failed to download geoboundaries: {', '.join(failed)}")

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

        AUTO selection is based on benchmark data (see COMPRESSION.md):
          - GADM (80 MB): zstd for speed (3.38x in 0.11s vs xz 5.14x in 64s).
          - NaturalEarth (22 MB): xz for ratio (3.72x in 14s); data is small
            enough that the time penalty is acceptable.
        Falls back through the availability chain if the preferred compressor
        is unavailable.
        """
        from pg_nearest_city.utils import _HAS_BZ2, _HAS_LZMA, _HAS_ZSTD_CLI, _zstd_open

        algo = self.config.compression
        if algo != CompressionAlgorithm.AUTO:
            return algo

        if self.config.boundary_source == BoundarySource.GADM:
            if _zstd_open is not None or _HAS_ZSTD_CLI:
                return CompressionAlgorithm.ZSTD
            return CompressionAlgorithm.GZIP
        else:
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

    def _create_temp_reference_tables(self, conn) -> None:
        """Create temporary tables used for import."""
        self.logger.info("Creating temporary tables used for import")
        if not self.config.iso3166_path.exists():
            self.logger.error(f"ISO3166-1 file not found: {self.config.iso3166_path}")
            raise FileNotFoundError(
                f"ISO3166-1 file not found: {self.config.iso3166_path}"
            )

        if self.config.boundary_source == BoundarySource.GADM:
            promoted = GADM_PROMOTED_TERRITORIES
        else:
            promoted = NE_PROMOTED_TERRITORIES

        try:
            with conn.cursor() as cur:
                with gzip.open(self.config.iso3166_path, "rb") as f:
                    with cur.copy(queries.INSERT_ISO3166) as copy:
                        for line in f:
                            copy.write(line)
                promote_sql = queries.insert_promoted_adm1_to_gid0(promoted)
                if promote_sql:
                    cur.execute(promote_sql)
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to import data: {e}")
            raise

    def _import_geonames(self, conn) -> None:
        """Import the cleaned GeoNames data into PostgreSQL."""
        clean_file = self.config.temp_dir / "cities_clean.txt"
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

    def _get_gadm_paths(self) -> tuple[Path, Path]:
        """Resolve GADM source path (single .gpkg serves both ADM0 and ADM1 layers)."""
        gadm_filenames = self.registry.get_by_source(source=GeoSource.GADM)
        if not gadm_filenames:
            raise RuntimeError(
                "Missing GADM files in registry. "
                "Run dataset_fetcher.py first to download the data."
            )
        cached = self.registry.get_cached_path(gadm_filenames[0], self.config.cache_dir)
        if not cached or not cached.exists():
            raise RuntimeError(
                "Registry has entries but GADM files are missing from cache. "
                "Run dataset_fetcher.py first to download the data."
            )
        if not extract_dataset(src_file=cached, dst_dir=self.config.temp_dir):
            raise RuntimeError("Failed to extract GADM dataset")
        gpkg = (Path(self.config.temp_dir) / gadm_filenames[0]).with_suffix(".gpkg")
        return gpkg, gpkg

    def _get_ne_boundary_paths(self) -> tuple[Path, Path]:
        """Resolve Natural Earth source paths (.shp for ADM0 and ADM1)."""
        ne_filenames = self.registry.get_by_source(source=GeoSource.NATURAL_EARTH)
        if not ne_filenames:
            raise RuntimeError(
                "Missing Natural Earth files in registry. "
                "Run dataset_fetcher.py first to download the data."
            )
        for filename in ne_filenames:
            cached = self.registry.get_cached_path(filename, self.config.cache_dir)
            if not cached or not cached.exists():
                raise RuntimeError(
                    f"Registry has entries but {filename} is missing from cache. "
                    "Run dataset_fetcher.py first to download the data."
                )
            if not extract_dataset(src_file=cached, dst_dir=self.config.temp_dir):
                raise RuntimeError(
                    f"Failed to extract Natural Earth dataset: {filename}"
                )
        adm0_path = self.config.temp_dir / "ne_10m_admin_0_countries.shp"
        adm1_path = self.config.temp_dir / "ne_10m_admin_1_states_provinces.shp"
        for path in (adm0_path, adm1_path):
            if not path.exists():
                raise RuntimeError(
                    f"Extraction succeeded but {path.name} not found in temp dir."
                )
        return adm0_path, adm1_path

    def _run_ogr_boundary_import(
        self, table_name: str, src_path: Path, sql: str
    ) -> None:
        """Run ogr2ogr to load one boundary layer into a PostgreSQL staging table."""
        cmd: list[str] = [
            "ogr2ogr",
            "-nln",
            table_name,
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-lco",
            "SPATIAL_INDEX=NONE",
            "--config",
            "PG_USE_COPY=YES",
            "-f",
            "PostgreSQL",
            f"PG:{self.config.db_conn_str}",
            str(src_path),
            "-dialect",
            "OGRSQL",
            "-sql",
            sql,
        ]
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            self.logger.error(
                f"Failed to import boundaries into {table_name} from {src_path.name}"
            )
            raise

    def _import_country_boundaries(self, conn) -> None:
        """Import country boundaries into PostgreSQL from the configured source."""
        source = self.config.boundary_source
        self.logger.info(f"Importing country boundaries from {source.value}")

        assert isinstance(self.config.temp_dir, Path)

        if source == BoundarySource.GADM:
            adm0_path, adm1_path = self._get_gadm_paths()
            adm0_layer, adm1_layer = "ADM_0", "ADM_1"
            adm0_cfg, adm1_cfg = GADM_0_DATASET, GADM_1_DATASET
            exclude = None
        else:
            adm0_path, adm1_path = self._get_ne_boundary_paths()
            adm0_layer = "ne_10m_admin_0_countries"
            adm1_layer = "ne_10m_admin_1_states_provinces"
            adm0_cfg, adm1_cfg = NE_COUNTRIES_DATASET, NE_ADM1_DATASET
            exclude = None

        sql_adm0 = queries.select_adm0(
            file_layer=adm0_layer,
            alpha3_column=adm0_cfg.alpha3_column,
            adm0_name_column=adm0_cfg.adm0_name_column,
            country_filter=self.config.country_filter,
            exclude_alpha3=exclude,
        )
        sql_adm1 = queries.select_adm1(
            file_layer=adm1_layer,
            alpha3_column=adm1_cfg.alpha3_column,
            adm0_name_column=adm1_cfg.adm0_name_column,
            adm1_column=adm1_cfg.adm1_column,
            adm1_name_column=adm1_cfg.adm1_name_column,
            exclude_alpha3=exclude,
        )

        self._run_ogr_boundary_import("tmp_country_bounds_adm0", adm0_path, sql_adm0)
        self._run_ogr_boundary_import("tmp_country_bounds_adm1", adm1_path, sql_adm1)

    def _import_geoboundary_corrections(self, conn) -> None:
        """Import geoboundary GeoJSON files as boundary corrections.

        For each cached geoboundary GeoJSON:
          1. ogr2ogr it into tmp_country_staging
          2. Run the update query to merge corrections into country

        Alpha-3 overrides for SARs/territories are already baked into the
        cached GeoJSON files during the fetch phase (see GeoBoundaryConfig
        _filter_geojson), so no post-import SQL fixup is needed.
        """
        self.logger.info("Importing geoboundary corrections")

        # Only import files that are in the current geoboundaries config.
        # The registry may contain files from previous runs with different
        # BOUNDARY_CORRECTIONS - loading stale entries would overwrite correct polygons.
        valid_filenames = {
            f"geoBoundaries_{t.iso}_{t.adm_level}.geojson"
            for t in self.geoboundaries_config.targets
        }
        gb_files = [
            f
            for f in self.registry.get_by_source(source=GeoSource.GEOBOUNDARIES)
            if (e := self.registry.get(f)) and e.filepath in valid_filenames
        ]
        if not gb_files:
            self.logger.warning(
                "No geoboundary files in registry; skipping corrections. "
                "Run dataset_fetcher.py first to download geoboundary data."
            )
            return

        imported_count = 0
        for gb_key in gb_files:
            entry = self.registry.get(gb_key)
            if not entry:
                continue

            geojson_path = self.config.cache_dir / "geoboundaries" / entry.filepath
            if not geojson_path.exists():
                self.logger.error(
                    f"Registry entry {gb_key} points to {geojson_path} "
                    "but file is missing; skipping"
                )
                continue

            self.logger.info(f"Importing geoboundary: {geojson_path.name}")

            # ogr2ogr: GeoJSON → tmp_country_staging
            # -append: add to existing table (multiple files may be loaded)
            # -nlt:    promote to multi so geometry type matches country table
            ogr_cmd: list[str] = [
                "ogr2ogr",
                "-nln",
                "tmp_country_staging",
                "-append",
                "-nlt",
                "PROMOTE_TO_MULTI",
                "--config",
                "OGR_GEOJSON_MAX_OBJ_SIZE",
                "0",
                "-f",
                "PostgreSQL",
                f"PG:{self.config.db_conn_str}",
                str(geojson_path.resolve()),
            ]

            try:
                subprocess.run(ogr_cmd, check=True)
                imported_count += 1
            except subprocess.CalledProcessError:
                self.logger.error(f"ogr2ogr failed for {geojson_path.name}; skipping")
                continue

        if imported_count == 0:
            self.logger.warning("No geoboundary files were imported")
            return

        self.logger.info(
            f"Imported {imported_count} geoboundary file(s) into tmp_country_staging"
        )

        # Run the update query to merge staging data into country table
        try:
            with conn.cursor() as cur:
                cur.execute(queries.UPDATE_COUNTRY_FROM_GEOBOUNDARIES)
                conn.commit()
                self.logger.info("Geoboundary corrections applied to country table")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update country boundaries: {e}")
            raise

        self._import_overpass_boundary_corrections(conn)
        self._resolve_country_overlaps(conn)

    def _resolve_country_overlaps(self, conn) -> None:
        """Remove exclave areas from surrounding country polygons via ST_Difference.

        After all boundary corrections are applied, any country whose polygon
        was not updated to exclude a neighbour's exclave will overlap with that
        exclave's polygon.  For each such pair we subtract the smaller country's
        geometry from the larger one, leaving each exclave's territory exclusively
        assigned to the correct country.
        """
        self.logger.info(
            "Resolving overlapping country boundaries - this may take some time"
        )
        try:
            with conn.cursor() as cur:
                cur.execute(queries.RESOLVE_COUNTRY_OVERLAPS)
                affected = cur.rowcount
                conn.commit()
                self.logger.info(
                    f"Resolved overlapping boundaries for {affected} country/countries"
                )
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to resolve country overlaps: {e}")
            raise

    def _import_overpass_boundary_corrections(self, conn) -> None:
        """Fetch boundaries from Overpass for territories absent from GeoBoundaries.

        For each target:
          1. POST the OverpassQL query and cache the OSM XML response.
          2. ogr2ogr the OSM multipolygons layer into a transient temp table.
          3. Upsert the unioned boundary geometry directly into country_init.
          4. Drop the transient temp table.

        Skipped when using GADM as the boundary source, since GADM already
        includes all Overpass target territories.
        """
        if self.config.boundary_source == BoundarySource.GADM:
            self.logger.info(
                "Skipping Overpass boundary corrections (not needed for GADM)"
            )
            return

        overpass_cache_dir = self.config.cache_dir / "overpass"
        overpass_cache_dir.mkdir(parents=True, exist_ok=True)

        from pg_nearest_city.datasets.types import OverpassBoundaryTarget

        reg_keys = OverpassBoundaryTarget.make_registry_keys(
            OVERPASS_BOUNDARY_TARGETS
        )

        for target, reg_key in zip(OVERPASS_BOUNDARY_TARGETS, reg_keys, strict=True):
            query_str = target.query.make_data_query()
            url_config = URLConfig(
                url=OVERPASS_URLCONFIG.url,
                data=f"data={query_str}",
                target_filename=f"{reg_key}.osm",
                timeout=180.0,
            )
            osm_path = overpass_cache_dir / url_config.filename

            self.logger.info(f"Fetching Overpass boundary for {target.alpha3}")
            outcome = url_config.ensure_cached(
                cache_dir=overpass_cache_dir,
                local_freshness=timedelta(days=7),
                timeout_get=180.0,
                logger=self.logger,
            )
            if outcome == DownloadOutcome.FAILED:
                self.logger.warning(
                    f"Failed to fetch Overpass boundary for {target.alpha3}; skipping"
                )
                continue

            tmp_tbl = f"tmp_overpass_{target.alpha3.lower()}"
            ogr_cmd: list[str] = [
                "ogr2ogr",
                "-nln",
                tmp_tbl,
                "-overwrite",
                "-f",
                "PostgreSQL",
                f"PG:{self.config.db_conn_str}",
                str(osm_path),
                "multipolygons",
            ]
            try:
                subprocess.run(ogr_cmd, check=True)
            except subprocess.CalledProcessError:
                self.logger.warning(
                    f"ogr2ogr failed for Overpass boundary {target.alpha3}; skipping"
                )
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        queries.upsert_overpass_boundary(tmp_tbl),
                        (target.alpha2, target.alpha3, target.name),
                    )
                    conn.commit()
                    self.logger.info(
                        f"Upserted Overpass boundary for {target.alpha3} "
                        "into country_init"
                    )
                self.registry.register(
                    geosource=GeoSource.OVERPASS,
                    filename=reg_key,
                    filehash=target.query_hash,
                    filepath=url_config.filename,
                    url=OVERPASS_URLCONFIG.url,
                )
                with conn.cursor() as cur:
                    cur.execute(queries.drop_table(tmp_tbl))
                    conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.warning(
                    f"Failed to insert Overpass boundary for {target.alpha3}: {e}"
                )
                continue

    def _cleanup_datasets(self, conn):
        """Fix known issues with datasets."""
        self.logger.info("Cleaning up imported datasets")
        rows = list(DATA_CORRECTIONS)
        if self.config.boundary_source == BoundarySource.GADM:
            rows.extend(GADM_DATA_CORRECTIONS)
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

    def _merge_countries_to_iso_defs(self, conn) -> None:
        """Ensure all country boundaries respect ISO3166-1 definitions."""
        try:
            with conn.cursor() as cur:
                self.logger.info("Finding countries not defined by ISO3166-1")
                cur.execute(queries.INSERT_NON_ISO_ADM0)
                self.logger.info(
                    "Finding countries to be split as defined by ISO3166-1"
                )
                cur.execute(queries.INSERT_NON_ISO_GID0_Z01_Z09_PARENTS)
                cur.execute(queries.INSERT_NON_ISO_GID0_BORDER_HEURISTICS)
                cur.execute(queries.UPDATE_TMP_ADM0_FROM_ISO_DEFS)
                cur.execute(queries.INSERT_PROMOTED_ADM1_TO_ADM0)
                cur.execute(queries.UPDATE_TMP_ADM0_FROM_NONISO_DEFS)
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to merge countries: {e}")
            raise

    def _update_country_geometries(self, conn) -> None:
        """Update geometry columns."""
        try:
            with conn.cursor() as cur:
                self.logger.info("Updating country boundaries")
                cur.execute(queries.UPDATE_COUNTRY_INIT_GEOM_ADM0)
                if not self.config.country_filter:
                    if self.config.boundary_source == BoundarySource.GADM:
                        cur.execute(queries.INSERT_COUNTRY_INIT_GEOM_ADM1_GADM)
                    else:
                        cur.execute(queries.INSERT_COUNTRY_INIT_GEOM_ADM1)
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update country with geom: {e}")
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
