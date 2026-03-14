"""Fetches datasets for use."""

from __future__ import annotations

import logging
import zipfile
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from pg_nearest_city.datasets.geoboundaries import GeoBoundaryConfig
from pg_nearest_city.datasets.registry import DatasetRegistry, GeoSource
from pg_nearest_city.datasets.types import DownloadOutcome, ZipFileStatus
from pg_nearest_city.scripts.helpers import make_file_link

if TYPE_CHECKING:
    from pg_nearest_city.datasets import URLConfig


def get_info_from_zip(zip_file: Path) -> tuple[ZipFileStatus, list[zipfile.ZipInfo]]:
    """Inspect the cached ZIP to determine the information of the geom file[s]."""
    if not zip_file.exists():
        return ZipFileStatus.MISSING, []
    try:
        with zipfile.ZipFile(zip_file, "r") as zf:
            return ZipFileStatus.OK, zf.infolist()
    except zipfile.BadZipFile:
        return ZipFileStatus.BAD_FILE, []

    return ZipFileStatus.NO_FILE_NAMES, []


def extract_dataset(
    src_file: Path,
    dst_dir: Path,
    logger: logging.Logger | None = None,
    use_hardlink: bool = True,
) -> bool:
    """Extract a given dataset."""
    logger = logger or logging.getLogger("dataset_extractor")
    dst_file = dst_dir / src_file.name
    if not zipfile.is_zipfile(src_file):
        if _link_err := make_file_link(
            src=src_file,
            dst=dst_file,
            is_hardlink=use_hardlink,
            overwrite_existing=True,
        ):
            if _link_err[-1].link_type == "copy":
                logger.error(
                    f"Failed to link or copy {src_file} to {dst_dir}: "
                    f"{_link_err[-1].link_err}"
                )
                raise _link_err[-1].link_err
            fallback = "symlink" if use_hardlink else "hardlink"
            logger.warning(
                f"Failed to {_link_err[0].link_type} {src_file} to {dst_dir}, "
                f"using {fallback}"
            )
        logger.info(f"Loaded {src_file} to {dst_dir}")
        return True

    _zip_status, _zip_info = get_info_from_zip(zip_file=src_file)
    if _zip_status is ZipFileStatus.OK:
        for _zip_file_info in _zip_info:
            _uncomp_file_path = (dst_dir / _zip_file_info.filename).resolve()
            if (
                not _uncomp_file_path.exists()
                or _uncomp_file_path.stat().st_size != _zip_file_info.file_size
            ):
                break
        else:
            logger.info(
                f"All files in {src_file} already existed in {dst_dir}"
                " - skipping extraction"
            )
            return True
    try:
        with zipfile.ZipFile(src_file, "r") as zf:
            zf.extractall(dst_dir)
            logger.info(f"Extracted {src_file} to {dst_dir}")
            return True
    except (FileNotFoundError, PermissionError, zipfile.BadZipFile) as e:
        logger.error(f"Failed to extract {src_file}: {e}")
        return False


def fetch_and_extract_dataset(
    url_config: URLConfig,
    geosource: GeoSource,
    cache_dir: Path,
    temp_dir: Path,
    registry: DatasetRegistry,
    logger: logging.Logger | None = None,
    use_hardlink: bool = True,
) -> DownloadOutcome:
    """Fetch and extract (if compressed) a single dataset.

    Returns True on success, False on failure.
    """
    logger = logger or logging.getLogger("dataset_fetcher")

    result = url_config.ensure_cached(
        cache_dir=cache_dir,
        local_freshness=timedelta(days=7),
        logger=logger,
        timeout_get=url_config.timeout,
    )

    if result == DownloadOutcome.FAILED:
        logger.error(f"Failed to fetch {url_config.filename} from {url_config.url}")

    if not (file_path := cache_dir / url_config.filename).exists():
        logger.error(f"File not found: {file_path}")

    if _skipped_download := result in {
        DownloadOutcome.LOCAL_FRESH,
        DownloadOutcome.NOT_MODIFIED,
    }:
        logger.info(f"Dataset {url_config.filename} was cached - skipping download")

    if result != DownloadOutcome.DOWNLOADED:
        return result

    file_hash = registry.make_sha256_hash(file_path=file_path)
    assert isinstance(file_hash, str)

    if existing_entry := registry.get(url_config.filename):
        if existing_entry.filehash != file_hash:
            if _skipped_download:
                logger.warning(
                    f"Recorded filehash for {url_config.filename} did not match "
                    "computed - file may be corrupt"
                )
    registry.register(
        filehash=file_hash,
        filename=url_config.filename,
        filepath=file_path,
        geosource=geosource,
        url=url_config.url,
    )

    return (
        DownloadOutcome.DOWNLOADED
        if extract_dataset(src_file=file_path, dst_dir=temp_dir)
        else DownloadOutcome.FAILED
    )


def fetch_geoboundaries(
    gb_config: GeoBoundaryConfig,
    cache_dir: Path,
    registry: DatasetRegistry,
    logger: logging.Logger | None = None,
) -> dict[str, tuple[DownloadOutcome, Path | None]]:
    """Fetch all configured geoboundary targets.

    Unlike fetch_and_extract_dataset, geoboundaries are GeoJSON files
    that don't need extraction.  The two-step API resolve → download
    flow is handled internally by GeoBoundaryConfig.

    Returns a dict keyed by registry key with (outcome, path) tuples.
    """
    logger = logger or logging.getLogger("dataset_fetcher")
    logger.info("Fetching geoboundary datasets")

    results = gb_config.ensure_cached(
        cache_dir=cache_dir,
        registry=registry,
        logger=logger,
    )

    failed = [k for k, (o, _) in results.items() if o == DownloadOutcome.FAILED]
    if failed:
        logger.error(f"Failed to fetch geoboundaries: {', '.join(failed)}")

    return results
