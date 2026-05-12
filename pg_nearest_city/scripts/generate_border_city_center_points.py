"""Generate city-center point fixtures from nearby cross-border city pairs."""

import argparse
import csv
from collections import OrderedDict
from pathlib import Path


INPUT_HEADER = [
    "pair",
    "country_a",
    "country_b",
    "country_name_a",
    "country_name_b",
    "city_a",
    "city_b",
    "lat_a",
    "lon_a",
    "lat_b",
    "lon_b",
    "distance_m",
    "seam_length_m",
]

OUTPUT_HEADER = [
    "point_id",
    "country",
    "country_name",
    "city",
    "lat",
    "lon",
    "example_pair",
    "source_pair_count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--keep-duplicates",
        action="store_true",
        help=(
            "Emit one row per city occurrence in the pair CSV instead of collapsing "
            "exact duplicate city centers"
        ),
    )
    return parser.parse_args()


def build_point_rows(
    pair_rows: list[dict[str, str]], keep_duplicates: bool = False
) -> list[dict[str, str | int]]:
    """Convert border-pair rows into city-center point rows."""
    if keep_duplicates:
        output_rows: list[dict[str, str | int]] = []
        for row in pair_rows:
            output_rows.extend(_expand_pair_row(row))
        return _with_point_ids(output_rows)

    points: OrderedDict[tuple[str, str, str, str], dict[str, str | int]] = OrderedDict()
    for row in pair_rows:
        for point in _expand_pair_row(row):
            key = (
                str(point["country"]),
                str(point["city"]),
                str(point["lat"]),
                str(point["lon"]),
            )
            existing = points.get(key)
            if existing is None:
                points[key] = point
                continue
            existing["source_pair_count"] = int(existing["source_pair_count"]) + 1

    return _with_point_ids(list(points.values()))


def _expand_pair_row(row: dict[str, str]) -> list[dict[str, str | int]]:
    return [
        {
            "country": row["country_a"],
            "country_name": row["country_name_a"],
            "city": row["city_a"],
            "lat": row["lat_a"],
            "lon": row["lon_a"],
            "example_pair": row["pair"],
            "source_pair_count": 1,
        },
        {
            "country": row["country_b"],
            "country_name": row["country_name_b"],
            "city": row["city_b"],
            "lat": row["lat_b"],
            "lon": row["lon_b"],
            "example_pair": row["pair"],
            "source_pair_count": 1,
        },
    ]


def _with_point_ids(
    rows: list[dict[str, str | int]],
) -> list[dict[str, str | int]]:
    for index, row in enumerate(rows, start=1):
        row["point_id"] = f"border-city-center-{index:05d}"
    return rows


def main() -> None:
    args = parse_args()

    with args.input.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != INPUT_HEADER:
            raise SystemExit(
                "Unexpected input header; expected world-border-city-pairs.csv shape"
            )
        pair_rows = list(reader)

    point_rows = build_point_rows(
        pair_rows=pair_rows,
        keep_duplicates=args.keep_duplicates,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_HEADER, lineterminator="\n")
        writer.writeheader()
        writer.writerows(point_rows)

    print(f"wrote {len(point_rows)} points to {args.output}")


if __name__ == "__main__":
    main()
