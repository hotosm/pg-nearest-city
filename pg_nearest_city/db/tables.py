"""DB models and raw import data."""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from enum import Enum
from graphlib import TopologicalSorter
from typing import Any, ClassVar

from psycopg import sql
from psycopg.errors import UndefinedFile

from pg_nearest_city.utils import filter_items, get_all_subclasses


class IndexType(str, Enum):
    """Supported PostgreSQL index access methods."""

    BTREE = "BTREE"
    GIST = "GIST"


@dataclass(slots=True)
class Index:
    """Represents a PostgreSQL index definition."""

    tbl_name: str
    col_names: list[str] = field(default_factory=list)
    index_type: IndexType = IndexType.BTREE
    is_post_load: bool = True
    is_unique: bool = False
    partial_def: str | None = None

    _col_list: str = field(init=False, repr=False)
    _idx_cols: str = field(init=False, repr=False)
    _suffix: str = field(init=False, repr=False)
    _name: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._col_list = ", ".join(self.col_names)
        self._idx_cols = "_".join(self.col_names)
        self._suffix = (
            "pidx" if self.partial_def else ("unq" if self.is_unique else "idx")
        )
        self._name = f"{self.tbl_name}_{self._idx_cols}_{self._suffix}"

    @property
    def name(self) -> str:
        """Return the auto-generated index name."""
        return self._name

    @property
    def index_def(self) -> str:
        """Return the CREATE INDEX SQL statement."""
        return (
            f"CREATE INDEX IF NOT EXISTS {self._name} "
            f"ON {self.tbl_name} USING {self.index_type.value} "
            f"({self._col_list}) {self.partial_def or ''}"
        ).strip()


class BaseTable:
    """Base table class."""

    name: ClassVar[str]
    indices: ClassVar[tuple[Index, ...]]
    alters: ClassVar[tuple[str, ...]] = ()
    analyze: ClassVar[bool] = False
    depends_on: ClassVar[tuple[type["BaseTable"], ...]] = ()
    drop_first: ClassVar[bool] = False
    is_externally_defined: ClassVar[bool] = False
    is_for_data_load: ClassVar[bool] = False
    is_post_alter: ClassVar[bool] = False
    is_temp: ClassVar[bool] = False
    is_unlogged: ClassVar[bool] = False
    safe_ops: ClassVar[bool] = True

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.alters = cls.get_alters()
        cls.indices = cls.get_indices()

    @classmethod
    def attrs(cls) -> list[str]:
        """Return the names of all ClassVar attributes on BaseTable."""
        return list(BaseTable.__annotations__.keys())

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        if cls.is_externally_defined:
            raise NotImplementedError(
                f"{cls.__name__}.create_sql() must be implemented"
            )
        return ""

    @classmethod
    def get_alters(cls) -> tuple[str, ...]:
        """Return ALTER TABLE constraint clauses to apply after creation."""
        return ()

    @classmethod
    def get_indices(cls) -> tuple[Index, ...]:
        """Return index definitions for this table."""
        return ()


class Country(BaseTable):
    """Country table class - lookup table."""

    name = "country"

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        return f"""
        CREATE TABLE {cls.name} (
            id     INT GENERATED ALWAYS AS IDENTITY NOT NULL,
            alpha2 CHAR(2) NOT NULL,
            alpha3 CHAR(3) NOT NULL,
            name   TEXT NOT NULL,
            geom   GEOMETRY(Polygon,4326) DEFAULT NULL,
            CONSTRAINT {cls.name}_pkey PRIMARY KEY (id),
            CONSTRAINT {cls.name}_name_len_chk CHECK (
                char_length(name) <= 126
            )
        )
        """

    @classmethod
    def get_indices(cls) -> tuple[Index, ...]:
        """Return index definitions for this table."""
        return (
            Index(tbl_name=cls.name, col_names=["geom"], index_type=IndexType.GIST),
            Index(tbl_name=cls.name, col_names=["alpha2"], index_type=IndexType.BTREE),
        )


class CountryInit(BaseTable):
    """CountryInit table class - used for initial data load.

    Stores one MultiPolygon per country before subdivision into the
    final country table.
    """

    name = "country_init"
    drop_first = True
    is_for_data_load = True
    is_temp = True
    is_unlogged = True

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        return f"""
        CREATE TABLE {cls.name} (
            alpha2 CHAR(2) NOT NULL,
            alpha3 CHAR(3) NOT NULL,
            name   TEXT NOT NULL,
            geom   GEOMETRY(MultiPolygon,4326) DEFAULT NULL,
            CONSTRAINT {cls.name}_pkey PRIMARY KEY (alpha2),
            CONSTRAINT {cls.name}_alpha3_unq UNIQUE (alpha3),
            CONSTRAINT {cls.name}_name_len_chk CHECK (
                char_length(name) <= 126
            )
        )
        """

    @classmethod
    def get_indices(cls) -> tuple[Index, ...]:
        """Return index definitions for this table."""
        return (
            Index(tbl_name=cls.name, col_names=["geom"], index_type=IndexType.GIST),
        )


class Geocoding(BaseTable):
    """Geocoding table class - main table.

    Note:
       The 'country' column name is retained (vs. 'country_code')
       despite being an ISO3166-alpha2 code for backwards compatibility.
       It may be migrated in the future.
    """

    name = "geocoding"
    depends_on = (Country,)
    drop_first = True
    is_post_alter = True

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        return f"""
        CREATE TABLE {cls.name} (
            id      INT GENERATED ALWAYS AS IDENTITY NOT NULL,
            city    TEXT NOT NULL,
            country CHAR(2) NOT NULL,
            lat     DECIMAL NOT NULL,
            lon     DECIMAL NOT NULL,
            geom    GEOMETRY(Point,4326) GENERATED ALWAYS AS (
              ST_SetSRID(ST_MakePoint(lon, lat), 4326)
            ) STORED,
            CONSTRAINT {cls.name}_pkey PRIMARY KEY (id),
            CONSTRAINT {cls.name}_city_len_chk CHECK (
                char_length(city) <= 126
            )
        )
        """

    @classmethod
    def get_alters(cls) -> tuple[str, ...]:
        """Return ALTER TABLE constraint clauses for this table."""
        return ()

    @classmethod
    def get_indices(cls) -> tuple[Index, ...]:
        """Return index definitions for this table."""
        return (
            Index(
                tbl_name=cls.name,
                col_names=["country", "geom"],
                index_type=IndexType.GIST,
            ),
        )


class TmpCgaz(BaseTable):
    """CGAZ country polygons, created by GDAL on import."""

    name = "tmp_cgaz"
    drop_first = True
    is_externally_defined = True
    is_for_data_load = True
    is_temp = True
    is_unlogged = True


class TmpCountryNames(BaseTable):
    """Country codes and names, loaded from country_names.csv."""

    name = "tmp_country_names"
    drop_first = True
    is_for_data_load = True
    is_temp = True
    is_unlogged = True

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        return f"""
        CREATE TABLE {cls.name} (
          alpha2 CHAR(2) PRIMARY KEY,
          alpha3 CHAR(3) NOT NULL UNIQUE,
          name TEXT NOT NULL
        )
        """


class TmpBoundaryOverrides(BaseTable):
    """Shapes that replace CGAZ, loaded from boundary_overrides.csv.xz."""

    name = "tmp_boundary_overrides"
    drop_first = True
    is_for_data_load = True
    is_temp = True
    is_unlogged = True

    @classmethod
    def create_sql(cls) -> str:
        """Return the CREATE TABLE SQL for this table."""
        return f"""
        CREATE TABLE {cls.name} (
          alpha2 CHAR(2) NOT NULL,
          geom GEOMETRY(MultiPolygon,4326) NOT NULL
        )
        """


def get_tables(filters: list[dict[str, Any]] | None = None) -> list[type[BaseTable]]:
    """Return all BaseTable subclasses, optionally filtered."""
    return filter_items(get_all_subclasses(BaseTable), filters or [])


def create_dependency_graph(
    filters: list[dict[str, Any]] | None = None,
) -> dict[type[BaseTable], set[type[BaseTable]]]:
    """Create a dependency graph for TopologicalSorter.

    If filters remove a table that another table depends_on, emit a warning
    and drop the missing dependency edges.
    """
    table_classes = get_tables(filters=filters)
    table_set = set(table_classes)

    graph: dict[type[BaseTable], set[type[BaseTable]]] = {}
    missing_deps: dict[type[BaseTable], set[type[BaseTable]]] = {}

    for table_cls in table_classes:
        deps = set(table_cls.depends_on)
        missing = deps - table_set
        if missing:
            missing_deps[table_cls] = missing
        graph[table_cls] = deps & table_set

    if missing_deps:
        lines: list[str] = [
            "Illegal filtered state: one or more included tables depend on"
            " tables that were filtered out."
        ]
        for tbl in sorted(missing_deps, key=lambda c: c.__name__):
            tbl_deps = ", ".join(sorted(d.__name__ for d in missing_deps[tbl]))
            lines.append(f"  - {tbl.__name__} depends on filtered-out: {tbl_deps}")
        warnings.warn("\n".join(lines), category=RuntimeWarning, stacklevel=2)

    return graph


def get_tables_in_creation_order(
    filters: list[dict[str, Any]] | None = None,
) -> list[type[BaseTable]]:
    """Get tables in the order they should be created."""
    graph = create_dependency_graph(filters=filters)
    sorter = TopologicalSorter(graph)
    return list(sorter.static_order())


def alter_post_tables(conn, logger: logging.Logger | None = None) -> None:
    """Alter tables after all data is loaded."""
    logger = logger or logging.getLogger("alter_post_tables")
    logger.info("Altering tables after data load")

    with conn.cursor() as cur:
        try:
            for table in get_tables(filters=[{"is_post_alter": True}]):
                for alter in table.alters:
                    cur.execute(f"ALTER TABLE {table.name} ADD {alter}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to alter table {table.name}: {e}")
            raise


def setup_database(conn, logger: logging.Logger | None = None) -> None:
    """Set up the database schema and extensions."""
    logger = logger or logging.getLogger("setup_database")
    logger.info("Setting up database schema")

    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        except UndefinedFile as e:
            logger.error(f"PostgreSQL extension error: {e}")
            logger.error("Ensure PostGIS is installed in your PostgreSQL instance")
            raise

        try:
            for table in get_tables_in_creation_order():
                if table.drop_first:
                    exists_clause = (
                        sql.SQL(" IF EXISTS") if table.safe_ops else sql.SQL("")
                    )
                    cur.execute(
                        sql.SQL("DROP TABLE{} {}").format(
                            exists_clause,
                            sql.Identifier(table.name),
                        )
                    )

                if table.is_externally_defined:
                    _sql = "SELECT 1"
                else:
                    _sql = table.create_sql()

                if table.safe_ops:
                    _sql = _sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                if table.is_unlogged:
                    _sql = _sql.replace("CREATE TABLE", "CREATE UNLOGGED TABLE")

                cur.execute(_sql)

                for alter in table.alters:
                    if not table.is_post_alter:
                        cur.execute(f"ALTER TABLE {table.name} ADD {alter}")

                for idx in table.get_indices():
                    if not idx.is_post_load:
                        cur.execute(idx.index_def)

                if table.analyze:
                    cur.execute(f"ANALYZE {table.name}")

            conn.commit()
            logger.info("Database schema setup complete")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database setup error: {e}")
            raise
