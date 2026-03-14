"""Simplified dataset registry that works with URLConfig."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path

TEN_MIB: int = 10_485_760


class GeoSource(str, Enum):
    """Enumeration of supported geospatial data sources."""

    GADM = "GADM"
    GEOBOUNDARIES = "GEOBOUNDARIES"
    GEONAMES = "GEONAMES"
    OVERPASS = "OVERPASS"


@dataclass
class DatasetEntry:
    """Registry entry linking a URLConfig to its purpose."""

    filehash: str
    filepath: str
    geosource: GeoSource
    url: str

    @classmethod
    def from_dict(cls, data: dict) -> DatasetEntry:
        """Construct a DatasetEntry from a plain dict."""
        return cls(
            filehash=data["filehash"],
            filepath=data["filepath"],
            geosource=GeoSource(data["geosource"]),
            url=data["url"],
        )


class DatasetRegistry:
    """Simple registry that maps GeoSource types to their URLConfigs."""

    def __init__(
        self, registry_path: Path | None = None, logger: logging.Logger | None = None
    ):
        self.logger = logger or logging.getLogger("dataset_registry")
        self.registry_path = registry_path or Path("dataset_registry.json")
        self.entries: dict[str, DatasetEntry] = {}
        self._load()

    def _load(self) -> None:
        """Load registry from file."""
        if not self.registry_path.exists():
            self.logger.warning(f"No registry file found at {self.registry_path}")
            return

        try:
            with open(self.registry_path, "r") as f:
                data = json.load(f)
                self.entries = {
                    key: DatasetEntry.from_dict(value) for key, value in data.items()
                }
            self.logger.info(f"Loaded {len(self.entries)} dataset entries")
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.error(f"Failed to load registry: {e}")
            self.entries = {}

    def _save(self) -> None:
        """Save registry to file."""
        try:
            with open(self.registry_path, "w") as f:
                data = {key: asdict(value) for key, value in self.entries.items()}
                json.dump(data, f, indent=2)
                f.seek(0, 2)
                f.write("\n")
            self.logger.debug(f"Saved {len(self.entries)} dataset entries")
        except OSError as e:
            self.logger.error(f"Failed to save registry: {e}")

    def register(
        self,
        geosource: GeoSource,
        filename: str,
        filehash: str,
        filepath: str | Path,
        url: str,
        config: dict[str, str] | None = None,
    ) -> None:
        """Register a dataset."""
        entry = DatasetEntry(
            filehash=filehash,
            filepath=str(filepath),
            geosource=geosource,
            url=url,
        )
        self.entries[filename] = entry
        self._save()
        self.logger.info(f"Registered {geosource.value}: {filename}")

    def get(self, filename: str) -> DatasetEntry | None:
        """Get entry for a specific GeoSource."""
        return self.entries.get(filename)

    def get_by_source(self, source: GeoSource) -> list:
        """Get the filename[s] for a dataset by GeoSource."""
        return [k for k, v in self.entries.items() if v.geosource == source]

    def list_all(self) -> list[tuple[str, dict]]:
        """List all registered datasets."""
        return [(k, asdict(v)) for k, v in self.entries.items()]

    def get_cached_path(self, filename: str, cache_dir: Path) -> Path | None:
        """Get the expected cache path for a dataset."""
        entry = self.entries.get(filename)
        if entry:
            return cache_dir / filename
        return None

    def is_cached(self, filename: str, cache_dir: Path) -> bool:
        """Check if a dataset is cached."""
        path = self.get_cached_path(filename, cache_dir)
        return path.exists() if path else False

    def make_sha256_hash(
        self, file_path: Path, chunk_size: int = TEN_MIB
    ) -> str | None:
        """Create a SHA256 hash for a file."""
        m = sha256()
        if not file_path.exists():
            self.logger.error(f"{file_path} not found, unable to create hash")
            return None
        with open(file_path, mode="rb") as f:
            while chunk := f.read(TEN_MIB):
                m.update(chunk)
        return m.hexdigest()
