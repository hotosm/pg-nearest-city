"""Entry point for the pgnearest-load CLI command."""

import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pg_nearest_city.datasets.types import BoundarySource, CompressionAlgorithm
from pg_nearest_city.db.data_import import Config, DataLoader
from pg_nearest_city.db.settings import DBConnSettings


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger("data_loader")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    group_db = parser.add_argument_group("group_db")
    group_db.add_argument("--db-host", help="Database host")
    group_db.add_argument("--db-port", type=int, help="Database port")
    group_db.add_argument("--db-name", help="Database name")
    group_db.add_argument("--db-user", help="Database username")
    group_db.add_argument("--db-password", help="Database password")

    parser.add_argument(
        "--cache-dir", default="/data/cache", help="Directory to cache downloaded files"
    )
    parser.add_argument(
        "--country",
        help="Filter to specific ISO 3166-1 alpha-2 country code (e.g. IT). "
        "Enclave dependencies are loaded automatically.",
    )
    parser.add_argument(
        "--boundary-source",
        choices=[s.value for s in BoundarySource],
        default=BoundarySource.NATURAL_EARTH.value,
        help="Country boundary data source (default: NaturalEarth)",
    )
    parser.add_argument(
        "--compression",
        choices=[c.value for c in CompressionAlgorithm],
        default=CompressionAlgorithm.AUTO.value,
        help="Compression for exported data (default: auto)",
    )
    parser.add_argument(
        "--no-cache", action="store_true", help="Don't cache downloaded files"
    )
    parser.add_argument(
        "--output-dir", default="/data/output", help="Directory for output files"
    )

    step_group = parser.add_mutually_exclusive_group()
    step_group.add_argument(
        "--skip-steps",
        help="Comma-separated step prefixes to skip (e.g. 01,03)",
    )
    step_group.add_argument(
        "--only-steps",
        help="Comma-separated step prefixes to run exclusively (e.g. 05,06)",
    )
    parser.add_argument(
        "--list-steps",
        action="store_true",
        help="Print available step names and exit",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Drop ALL project tables before starting (recovery from partial runs)",
    )

    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class Step:
    """A named pipeline step with its callable and keyword arguments."""

    name: str
    fn: Callable[..., None]
    kwargs: dict[str, object]
    pass_conn: bool = True


def _parse_step_prefixes(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    return {s.strip() for s in raw.split(",") if s.strip()}


def _should_run(step: Step, skip: set[str] | None, only: set[str] | None) -> bool:
    prefix = step.name.split("_", 1)[0]
    if only is not None:
        return prefix in only
    if skip is not None:
        return prefix not in skip
    return True


STEP_NAMES: list[str] = [
    "00_check_prerequisites",
    "01_setup_db",
    "02_alter_db_params",
    "03_download_geonames",
    "04_download_boundaries",
    "05_download_geoboundaries",
    "06_clean_geonames",
    "07_set_tables_unlogged",
    "08_create_ref_tables",
    "09_import_geonames",
    "10_import_boundaries",
    "11_create_btree_indices",
    "12_apply_corrections",
    "13_create_spatial_indices",
    "14_merge_iso_countries",
    "15_update_geometries",
    "16_import_geoboundaries",
    "17_simplify_geometry",
    "18_vacuum_analyze",
    "19_drop_temp_tables",
    "20_set_tables_logged",
    "21_add_constraints",
    "22_export_data",
]


def main() -> None:
    """Run the database loading pipeline."""
    args = parse_args()

    if args.list_steps:
        for name in STEP_NAMES:
            print(name)
        sys.exit(0)

    logger = setup_logging()

    conn_settings = DBConnSettings().with_overrides(
        **{
            k.lstrip("db_"): v
            for k, v in args._get_kwargs()
            if k.startswith("db_") and v
        }
    )

    dataloader = DataLoader(
        config=Config(
            db_conn_settings=conn_settings,
            boundary_source=BoundarySource(args.boundary_source),
            cache_dir=Path(args.cache_dir),
            cache_files=not args.no_cache,
            compression=CompressionAlgorithm(args.compression),
            output_dir=Path(args.output_dir),
            country_filter=args.country,
        ),
        logger=logger,
    )

    load_steps: list[Step] = [
        Step("00_prerequisites", dataloader._check_prerequisites, {}),
        Step("01_setup_db", dataloader._setup_db, {}),
        Step("02_alter_db_params", dataloader._alter_db_params, {}),
        Step(
            "03_download_geonames", dataloader._download_geonames, {}, pass_conn=False
        ),
        Step(
            "04_download_boundaries",
            dataloader._download_boundaries,
            {},
            pass_conn=False,
        ),
        Step(
            "05_download_geoboundaries",
            dataloader._download_geoboundaries,
            {},
            pass_conn=False,
        ),
        Step("06_clean_geonames", dataloader._clean_geonames, {}, pass_conn=False),
        Step("07_set_tables_unlogged", dataloader._alter_table_params, {"init": True}),
        Step("08_create_ref_tables", dataloader._create_temp_reference_tables, {}),
        Step("09_import_geonames", dataloader._import_geonames, {}),
        Step("10_import_boundaries", dataloader._import_country_boundaries, {}),
        Step("11_create_btree_indices", dataloader._create_btree_indices, {}),
        Step("12_apply_corrections", dataloader._cleanup_datasets, {}),
        Step("13_create_spatial_indices", dataloader._create_spatial_indices, {}),
        Step("14_merge_iso_countries", dataloader._merge_countries_to_iso_defs, {}),
        Step("15_update_geometries", dataloader._update_country_geometries, {}),
        Step("16_import_geoboundaries", dataloader._import_geoboundary_corrections, {}),
        Step("17_simplify_geometry", dataloader._simplify_country_table, {}),
        Step("18_vacuum_analyze", dataloader._vacuum_and_analyze_db, {"full": True}),
        Step("19_drop_temp_tables", dataloader._drop_temp_tables, {}),
        Step("20_set_tables_logged", dataloader._alter_table_params, {"init": False}),
        Step("21_add_constraints", dataloader._alter_post_tables, {}),
        Step("22_export_data", dataloader._export_data, {}, pass_conn=False),
    ]

    if args.clean:
        logger.info("--clean: dropping all project tables")
        dataloader._clean_all_tables()

    skip = _parse_step_prefixes(args.skip_steps)
    only = _parse_step_prefixes(args.only_steps)

    for step in load_steps:
        if not _should_run(step, skip, only):
            logger.info(f"Skipping step: {step.name}")
            continue
        logger.info(f"Running step: {step.name}")
        if step.pass_conn:
            step.fn(**step.kwargs | {"conn": dataloader.conn})
        else:
            step.fn(**step.kwargs)


if __name__ == "__main__":
    main()
