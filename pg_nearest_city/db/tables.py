from __future__ import annotations

import inspect
from graphlib import TopologicalSorter
from typing import ClassVar, Optional, Type


class BaseTable:
    name: ClassVar[str]
    depends_on: ClassVar[Optional[Type[BaseTable]]]
    drop_first: ClassVar[bool]
    safe_ops: ClassVar[bool]
    sql: ClassVar[str]


class Country(BaseTable):
    name: ClassVar[str] = "country"
    depends_on: ClassVar[Optional[Type[BaseTable]]] = None
    drop_first: ClassVar[bool] = False
    safe_ops: ClassVar[bool] = True
    sql: ClassVar[str] = """
    CREATE TABLE country (
        alpha2 CHAR(2) NOT NULL,
        alpha3 CHAR(3) NOT NULL,
        numeric CHAR(3) NOT NULL,
        name TEXT NOT NULL,
        CONSTRAINT country_pkey PRIMARY KEY (alpha2),
        CONSTRAINT country_alpha3_unq UNIQUE (alpha3),
        CONSTRAINT country_numeric_unq UNIQUE (numeric),
        CONSTRAINT country_name_len_chk CHECK (
            char_length(name) <= 126
        )
    )
    """


class Geocoding(BaseTable):
    name: ClassVar[str] = "geocoding"
    depends_on: ClassVar[Optional[Type[BaseTable]]] = Country
    drop_first: ClassVar[bool] = True
    safe_ops: ClassVar[bool] = True
    sql: ClassVar[str] = """
    CREATE TABLE geocoding (
        id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
        city TEXT NOT NULL,
        country_code CHAR(2) NOT NULL,
        lat DECIMAL NOT NULL,
        lon DECIMAL NOT NULL,
        geom GEOMETRY(Point,4326) GENERATED ALWAYS AS (
          ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        ) STORED,
        voronoi GEOMETRY(Polygon,4326),
        CONSTRAINT geocoding_pkey PRIMARY KEY (id),
        CONSTRAINT geocoding_city_len_chk CHECK (
            char_length(city) <= 126
        ),
        CONSTRAINT geocoding_country_code_fkey
            FOREIGN KEY (country_code)
            REFERENCES country (alpha2)
            ON UPDATE RESTRICT
            ON DELETE RESTRICT
    )
    """


def get_all_table_classes() -> list[Type[BaseTable]]:
    """Get all subclasses of BaseTable defined in the module."""
    return [
        cls
        for _, cls in inspect.getmembers(
            inspect.getmodule(BaseTable),
            lambda c: (
                inspect.isclass(c) and issubclass(c, BaseTable) and c is not BaseTable
            ),
        )
    ]


def create_dependency_graph() -> dict[Type[BaseTable], set[Type[BaseTable]]]:
    """Create a dependency graph for TopologicalSorter."""
    graph = {}
    table_classes = get_all_table_classes()

    for table_cls in table_classes:
        dependencies = set()
        if table_cls.depends_on is not None:
            dependencies.add(table_cls.depends_on)
        graph[table_cls] = dependencies

    return graph


def get_tables_in_creation_order() -> list[Type[BaseTable]]:
    """Get tables in the order they should be created."""
    graph = create_dependency_graph()
    sorter = TopologicalSorter(graph)

    return list(sorter.static_order())
