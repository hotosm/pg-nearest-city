"""GeoBoundaries download support.

Two-step flow: resolve the geoboundaries.org API to get the current
download URL, then fetch the GeoJSON and track it in the registry.

Supports selective extraction: when a target specifies ``extract_shapes``,
only those features are imported and their ``shapeGroup`` is overridden
to the correct ISO alpha-3 code.  This handles SARs like Hong Kong and
Macau which appear inside the CHN/ADM1 dataset with shapeGroup=CHN.
"""

from __future__ import annotations

import json
import logging
import ssl
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .registry import DatasetRegistry, GeoSource
from .types import DownloadOutcome, GeoBoundaryRelease
from .url_config import URLConfig


@dataclass(frozen=True, slots=True)
class GeoAPIResponse:
    """Parsed metadata from a geoboundaries API response."""

    boundary_iso: str
    boundary_type: str
    boundary_canonical: str
    boundary_version: str
    build_date: str
    gj_download_url: str
    download_url: str
    source_data_update_date: str
    api_url: str

    @classmethod
    def from_json(cls, data: dict, api_url: str) -> GeoAPIResponse:
        """Construct a GeoAPIResponse from a raw API response dict."""
        return cls(
            boundary_iso=data.get("boundaryISO", ""),
            boundary_type=data.get("boundaryType", ""),
            boundary_canonical=data.get("boundaryCanonical", ""),
            boundary_version=data.get("boundaryName", ""),
            build_date=data.get("buildDate", ""),
            gj_download_url=data.get("gjDownloadURL", ""),
            download_url=data.get("downloadURL", ""),
            source_data_update_date=data.get("sourceDataUpdateDate", ""),
            api_url=api_url,
        )


@dataclass(frozen=True, slots=True)
class ShapeExtract:
    """One feature to extract from a downloaded GeoJSON.

    Maps a shapeName value found in the GeoJSON to the alpha-3 code that
    should replace shapeGroup after import.  Used to cherry-pick specific
    features from a parent country's ADM1 dataset.

    shape_name:      Value of the ``shapeName`` property to match.
    alpha3_override: The alpha-3 code to write into ``shapeGroup``.
    """

    shape_name: str
    alpha3_override: str


@dataclass(frozen=True, slots=True)
class GeoBoundaryTarget:
    """A single geoboundary to fetch.

    iso:       ISO 3166-1 alpha-3 code for the API request.
    adm_level: ADM0, ADM1, etc.
    extract_shapes:
               If non-empty, only features whose `shapeName` matches
               one of these entries are imported.  Each match gets its
               `shapeGroup` rewritten to the corresponding alpha3_override.
               If empty, all features are imported as-is (full-country mode).
    """

    iso: str
    adm_level: str = "ADM0"
    extract_shapes: tuple[ShapeExtract, ...] = ()

    @property
    def is_selective(self) -> bool:
        """True if only a subset of features should be imported."""
        return len(self.extract_shapes) > 0

    @property
    def shape_name_to_alpha3(self) -> dict[str, str]:
        """Lookup: shapeName → desired alpha-3 code."""
        return {s.shape_name: s.alpha3_override for s in self.extract_shapes}


@dataclass(frozen=True, slots=True)
class GeoBoundaryConfig:
    """Manages fetching geoboundary GeoJSON files via the two-step API flow.

    release: gbOpen, gbHumanitarian, or gbAuthoritative.
    targets: Which boundaries to download.
    base_url: API base URL.
    timeout_api: Timeout in seconds for the metadata API call.
    timeout_download: Timeout in seconds for the actual GeoJSON download.
    """

    release: GeoBoundaryRelease = GeoBoundaryRelease.GB_OPEN
    targets: tuple[GeoBoundaryTarget, ...] = ()
    base_url: str = "https://www.geoboundaries.org/api/current"
    timeout_api: float = 10.0
    timeout_download: float = 30.0

    @staticmethod
    def _registry_key(release: str, iso: str, adm_level: str) -> str:
        return f"geoboundaries_{release}_{iso}_{adm_level}".lower()

    def _api_url(self, target: GeoBoundaryTarget) -> str:
        return f"{self.base_url}/{self.release}/{target.iso}/{target.adm_level}/"

    def resolve(
        self,
        target: GeoBoundaryTarget,
        logger: logging.Logger | None = None,
    ) -> GeoAPIResponse:
        """Hit the geoboundaries API for a single target and return metadata."""
        log = logger or logging.getLogger("geoboundary")
        api_url = self._api_url(target)
        log.info(f"Resolving geoboundary: {api_url}")

        ctx = ssl.create_default_context()
        req = urllib.request.Request(
            api_url,
            method="GET",
            headers={"User-Agent": "python-urlconfig/1.0"},
        )
        with closing(
            urllib.request.urlopen(req, timeout=self.timeout_api, context=ctx)
        ) as resp:
            body = json.loads(resp.read().decode("utf-8"))

        return GeoAPIResponse.from_json(body, api_url)

    @staticmethod
    def _filter_geojson(
        src_path: Path,
        dst_path: Path,
        target: GeoBoundaryTarget,
        logger: logging.Logger,
    ) -> int:
        """Read a GeoJSON FeatureCollection, filter and rewrite features to dst_path.

        Keeps only matching features and rewrites shapeGroup. Returns the number
        of features written.
        """
        with open(src_path, "r", encoding="utf-8") as f:
            geojson = json.load(f)

        all_features = geojson.get("features", [])
        total_count = len(all_features)
        lookup = target.shape_name_to_alpha3
        kept: list[dict] = []

        for feature in all_features:
            props = feature.get("properties", {})
            shape_name = props.get("shapeName", "")
            if "Special Administrative Region" in shape_name:
                sar = shape_name.partition("Special Administrative Region")
                shape_name = sar[0].strip()
            if shape_name in lookup:
                props["shapeGroup"] = lookup[shape_name]
                kept.append(feature)

        if not kept:
            logger.warning(
                f"No features matched extract_shapes for "
                f"{target.iso}/{target.adm_level}. "
                f"Expected: {list(lookup.keys())}"
            )
            return 0

        geojson["features"] = kept
        with open(dst_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, separators=(",", ":"))

        kept_names = {p.get("properties", {}).get("shapeName") for p in kept}
        mapped = ", ".join(
            f"{s.shape_name} → {s.alpha3_override}"
            for s in target.extract_shapes
            if s.shape_name in kept_names
        )
        logger.info(
            f"Filtered {src_path.name}: kept {len(kept)} of {total_count} features"
            f" ({mapped})"
        )
        return len(kept)

    def _ensure_single(
        self,
        target: GeoBoundaryTarget,
        cache_dir: Path,
        registry: DatasetRegistry,
        logger: logging.Logger,
    ) -> tuple[DownloadOutcome, Path | None]:
        """Download one geoboundary if changed, or return cached path.

        For selective targets, the cached file is the *filtered* GeoJSON
        (not the full parent download).
        """
        reg_key = self._registry_key(self.release, target.iso, target.adm_level)

        # Always resolve API to detect version changes
        try:
            api = self.resolve(target, logger=logger)
        except Exception as e:
            logger.error(
                f"API resolution failed for {target.iso}/{target.adm_level}: {e}"
            )
            existing = registry.get(reg_key)
            if existing:
                cached = cache_dir / existing.filepath
                if cached.exists():
                    logger.info("API unreachable; keeping cached file")
                    return DownloadOutcome.LOCAL_FRESH, cached
            return DownloadOutcome.FAILED, None

        dl_url = api.gj_download_url
        if not dl_url:
            logger.error(
                f"No gjDownloadURL in API response for {target.iso}/{target.adm_level}"
            )
            return DownloadOutcome.FAILED, None

        existing = registry.get(reg_key)
        if existing and existing.url == dl_url:
            cached = cache_dir / existing.filepath
            if cached.exists():
                logger.info(
                    f"Cache hit: {reg_key} (version={api.boundary_version}, "
                    f"build={api.build_date})"
                )
                return DownloadOutcome.LOCAL_FRESH, cached

        target_filename = f"geoBoundaries_{target.iso}_{target.adm_level}"

        url_cfg = URLConfig(
            url=dl_url,
            timeout=self.timeout_download,
            target_filename=target_filename,
            target_filetype="geojson",
        )

        cache_dir.mkdir(parents=True, exist_ok=True)

        if target.is_selective:
            raw_filename = f".raw_{url_cfg.filename}"
            raw_dest = cache_dir / raw_filename
            final_dest = cache_dir / url_cfg.filename

            outcome, meta = url_cfg._http_get_conditional(
                dest_path=raw_dest,
                verify_tls=True,
                timeout=self.timeout_download,
                logger=logger,
            )

            if outcome != DownloadOutcome.DOWNLOADED:
                logger.error(
                    f"Download failed for {reg_key}: {meta.get('err', 'unknown')}"
                )
                raw_dest.unlink(missing_ok=True)
                return DownloadOutcome.FAILED, None

            n_kept = self._filter_geojson(raw_dest, final_dest, target, logger)
            raw_dest.unlink(missing_ok=True)

            if n_kept == 0:
                final_dest.unlink(missing_ok=True)
                return DownloadOutcome.FAILED, None

            dest = final_dest
        else:
            dest = cache_dir / url_cfg.filename

            outcome, meta = url_cfg._http_get_conditional(
                dest_path=dest,
                verify_tls=True,
                timeout=self.timeout_download,
                logger=logger,
            )

            if outcome != DownloadOutcome.DOWNLOADED:
                logger.error(
                    f"Download failed for {reg_key}: {meta.get('err', 'unknown')}"
                )
                return DownloadOutcome.FAILED, None

        filehash = registry.make_sha256_hash(dest)
        registry.register(
            geosource=GeoSource.GEOBOUNDARIES,
            filename=reg_key,
            filehash=filehash or "",
            filepath=url_cfg.filename,
            url=dl_url,
        )
        logger.info(
            f"{'Filtered and cached' if target.is_selective else 'Downloaded'} "
            f"{url_cfg.filename} (version={api.boundary_version}, "
            f"build={api.build_date})"
        )
        return DownloadOutcome.DOWNLOADED, dest

    def ensure_cached(
        self,
        cache_dir: Path,
        registry: DatasetRegistry,
        logger: logging.Logger | None = None,
    ) -> dict[str, tuple[DownloadOutcome, Path | None]]:
        """Download all configured targets.  Returns a dict keyed by registry key."""
        log = logger or logging.getLogger("geoboundary")
        results: dict[str, tuple[DownloadOutcome, Path | None]] = {}

        for target in self.targets:
            key = self._registry_key(self.release, target.iso, target.adm_level)
            outcome, path = self._ensure_single(target, cache_dir, registry, log)
            results[key] = (outcome, path)

        downloaded = sum(
            1 for o, _ in results.values() if o == DownloadOutcome.DOWNLOADED
        )
        cached = sum(1 for o, _ in results.values() if o == DownloadOutcome.LOCAL_FRESH)
        failed = sum(1 for o, _ in results.values() if o == DownloadOutcome.FAILED)
        log.info(
            f"GeoBoundaries: {downloaded} downloaded, {cached} cached, {failed} failed "
            f"(of {len(self.targets)} targets)"
        )
        return results
