"""Border probe fixture, shared by the sync and async tests."""

import csv
from dataclasses import dataclass
from pathlib import Path

FIXTURE_PATH = Path(__file__).parent / "border_country_probes.csv"
NOMINATIM_FIXTURE_PATH = Path(__file__).parent / "nominatim_countries.csv"


@dataclass(frozen=True)
class BorderProbe:
    pair: str
    expected_alpha2: str
    expected_alpha3: str
    source_country: str
    source_city: str
    neighbor_city: str
    ring_distance_m: int
    probe_lat: float
    probe_lon: float

    @property
    def label(self) -> str:
        return (
            f"{self.pair}|{self.source_country}|"
            f"{self.source_city}->{self.neighbor_city}|{self.ring_distance_m}m"
        )


def load_ok_border_probes() -> list[BorderProbe]:
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
                    neighbor_city=row["neighbor_city"],
                    ring_distance_m=int(row["ring_distance_m"]),
                    probe_lat=float(row["probe_lat"]),
                    probe_lon=float(row["probe_lon"]),
                )
            )
    return rows


def load_nominatim_countries() -> list[tuple[float, float, str]]:
    with NOMINATIM_FIXTURE_PATH.open(newline="", encoding="utf-8") as f:
        return [
            (float(row["lon"]), float(row["lat"]), row["country"])
            for row in csv.DictReader(f)
        ]


BORDER_PROBES: list[BorderProbe] = load_ok_border_probes()
BORDER_PROBE_IDS: list[str] = [p.label for p in BORDER_PROBES]
