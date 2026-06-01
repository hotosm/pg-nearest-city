"""Loader for the committed border-country probe fixture CSV.

Shared by the sync and async runtime tests so both consume identical rows.
"""

import csv
from dataclasses import dataclass
from pathlib import Path

FIXTURE_PATH = Path(__file__).parent / "border_country_probes.csv"


@dataclass(frozen=True)
class BorderProbe:
    """A single seam-relative probe row from the fixture."""

    pair: str
    expected_alpha2: str
    expected_alpha3: str
    source_country: str
    source_city: str
    ring_distance_m: int
    probe_lat: float
    probe_lon: float

    @property
    def label(self) -> str:
        """Stable, readable pytest parametrize id."""
        return (
            f"{self.pair}|{self.source_country}|"
            f"{self.source_city}|{self.ring_distance_m}m"
        )


def load_ok_border_probes() -> list[BorderProbe]:
    """Read fixture CSV and return only rows whose status is `ok`."""
    rows: list[BorderProbe] = []
    with FIXTURE_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["status"] != "ok":
                continue
            rows.append(
                BorderProbe(
                    pair=row["pair"],
                    expected_alpha2=row["expected_alpha2"],
                    expected_alpha3=row["expected_alpha3"],
                    source_country=row["source_country"],
                    source_city=row["source_city"],
                    ring_distance_m=int(row["ring_distance_m"]),
                    probe_lat=float(row["probe_lat"]),
                    probe_lon=float(row["probe_lon"]),
                )
            )
    return rows


BORDER_PROBES: list[BorderProbe] = load_ok_border_probes()
BORDER_PROBE_IDS: list[str] = [p.label for p in BORDER_PROBES]
