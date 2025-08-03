"""Data with known issues to be cleaned."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from pg_nearest_city.db.tables import get_all_table_classes
from psycopg import sql


class _Comment(Enum):
    COORDINATES = "COORDINATES"
    ERRATUM = "ERRATUM"
    MISSING = "MISSING"
    SPELLING = "SPELLING"


class _DML(Enum):
    DELETE = "DELETE"
    INSERT = "INSERT"  # not yet implemented
    UPDATE = "UPDATE"


class _PredicateComparison(Enum):
    BETWEEN = "BETWEEN"
    BETWEEN_SYM = "BETWEEN SYMMETRIC"
    DISTINCT = "IS DISTINCT FROM"
    EQUAL = "="
    FALSE = "FALSE"
    GT = ">"
    GTE = ">="
    IN = "IN"
    LT = "<"
    LTE = "<="
    NOT_BETWEEN = "NOT BETWEEN"
    NOT_BETWEEN_SYM = "NOT BETWEEN SYMMETRIC"
    NOT_DISTINCT = "IS NOT DISTINCT FROM"
    NOT_EQ = "<>"
    NOT_FALSE = "IS NOT FALSE"
    NOT_IN = "NOT IN"
    NOT_NULL = "IS NOT NULL"
    NOT_TRUE = "IS NOT TRUE"
    NOT_UNK = "IS NOT UNKNOWN"
    NULL = "IS NULL"
    TRUE = "IS TRUE"
    UNK = "IS UNKNOWN"

    @property
    def requires_two_values(self) -> bool:
        """Return True if this comparison requires two values (e.g., BETWEEN)."""
        return self in {
            _PredicateComparison.BETWEEN,
            _PredicateComparison.BETWEEN_SYM,
            _PredicateComparison.NOT_BETWEEN,
            _PredicateComparison.NOT_BETWEEN_SYM,
        }

    @property
    def requires_column_comparison(self) -> bool:
        """Return True if this comparison can compare two columns."""
        return self in {
            _PredicateComparison.DISTINCT,
            _PredicateComparison.NOT_DISTINCT,
        }

    @property
    def requires_no_values(self) -> bool:
        """Return True if this comparison requires no values (e.g., IS NULL)."""
        return self in {
            _PredicateComparison.FALSE,
            _PredicateComparison.NOT_FALSE,
            _PredicateComparison.NULL,
            _PredicateComparison.NOT_NULL,
            _PredicateComparison.TRUE,
            _PredicateComparison.NOT_TRUE,
            _PredicateComparison.UNK,
            _PredicateComparison.NOT_UNK,
        }

    @property
    def supports_lists(self) -> bool:
        """Return True if this comparison supports list values (e.g., IN)."""
        return self in {
            _PredicateComparison.IN,
            _PredicateComparison.NOT_IN,
        }


@dataclass
class PredicateData:
    """Class representing a predicate for a query.

    col_name: name of column being used as a predicate
    comparison: type of comparison for col_name
    col_val: value to compare column against
    col_val_2: additional value to compare column against (for BETWEEN, etc.)
    col_name_2: second column name for column-to-column comparisons

    """

    col_name: str
    comparison: _PredicateComparison
    col_name_2: str | None = None
    col_val: float | int | str | list | None = None
    col_val_2: float | int | str | None = None

    def __post_init__(self) -> None:
        """Validate predicate data based on comparison type."""
        if self.comparison.requires_two_values:
            if self.col_val_2 is None:
                raise ValueError(f"Must provide col_val_2 for {self.comparison.value}")

        elif self.comparison.requires_no_values:
            if (
                self.col_val is not None
                or self.col_val_2 is not None
                or self.col_name_2 is not None
            ):
                raise ValueError(
                    f"Must not provide any values for {self.comparison.value}"
                )

        elif self.comparison.supports_lists:
            if self.col_val is None:
                raise ValueError(
                    "Must provide col_val (list or single value) "
                    f"for {self.comparison.value}"
                )

        elif self.comparison.requires_column_comparison:
            if self.col_val is None and self.col_name_2 is None:
                raise ValueError(
                    "Must provide either col_val or col_name_2 "
                    f"for {self.comparison.value}"
                )

        else:
            if self.col_val is None:
                raise ValueError(f"Must provide col_val for {self.comparison.value}")


@dataclass
class RowData:
    """Class representing a partial row transformation.

    comment: type of correction (purely for observation)
    dml: type of _DML
    tbl_name: table name containing the target column
    col_name: column name to transform.
    col_val: value to assign to the specified column
    predicate_cols: one or more PredicateData instances
    result_limit: maximum number of rows that to be affected (0 = no limit)

    """

    comment: _Comment
    dml: _DML
    tbl_name: str
    col_name: str | None = None
    col_val: float | int | str | None = None
    predicate_cols: list[PredicateData] = field(default_factory=list)
    result_limit: int = 1

    def __post_init__(self):
        """Performs validation of various supplied values."""
        if self.tbl_name not in [x.name for x in get_all_table_classes()]:
            raise ValueError(f"Table {self.tbl_name} does not exist")

        if self.dml is _DML.INSERT:
            raise NotImplementedError("INSERT operations not yet implemented")

        if self.dml is not _DML.DELETE and (
            self.col_name is None or self.col_val is None
        ):
            raise ValueError(
                f"Column name and value must be provided for {self.dml.value}"
            )

        if not self.predicate_cols:
            raise ValueError("At least one predicate must be provided")

        if self.result_limit < 0:
            raise ValueError("result_limit must be non-negative")


def format_predicate(predicate: PredicateData) -> sql.Composed:
    """Format a single predicate into SQL."""
    col_name = sql.Identifier(predicate.col_name)
    comparison = sql.SQL(predicate.comparison.value)

    if predicate.comparison.requires_no_values:
        return sql.SQL("{col_name} {comparison}").format(
            col_name=col_name, comparison=comparison
        )

    elif predicate.comparison.requires_two_values:
        return sql.SQL("{col_name} {comparison} {val1} AND {val2}").format(
            col_name=col_name,
            comparison=comparison,
            val1=sql.Literal(predicate.col_val),
            val2=sql.Literal(predicate.col_val_2),
        )

    elif predicate.comparison.supports_lists and isinstance(predicate.col_val, list):
        values = sql.SQL("({})").format(
            sql.SQL(", ").join(sql.Literal(val) for val in predicate.col_val)
        )
        return sql.SQL("{col_name} {comparison} {values}").format(
            col_name=col_name, comparison=comparison, values=values
        )

    elif predicate.comparison.requires_column_comparison and predicate.col_name_2:
        return sql.SQL("{col_name} {comparison} {col_name_2}").format(
            col_name=col_name,
            comparison=comparison,
            col_name_2=sql.Identifier(predicate.col_name_2),
        )

    else:
        return sql.SQL("{col_name} {comparison} {col_val}").format(
            col_name=col_name,
            comparison=comparison,
            col_val=sql.Literal(predicate.col_val),
        )


PC = _PredicateComparison
PD = PredicateData
ROWS_TO_CLEAN: list[RowData] = [
    RowData(
        comment=_Comment.SPELLING,
        col_name="city",
        col_val="Simpson Bay Village",
        dml=_DML.UPDATE,
        predicate_cols=[
            PD(
                col_name="city",
                col_val="Simson Bay Village",
                comparison=PC.EQUAL,
            ),
            PD(
                col_name="country",
                col_val="SX",
                comparison=PC.EQUAL,
            ),
        ],
        tbl_name="geocoding",
        result_limit=1,
    ),
]

SQL_CLEAN_BASE_DEL: sql.SQL = sql.SQL("DELETE FROM {tbl_name} WHERE ")
SQL_CLEAN_BASE_UPD: sql.SQL = sql.SQL(
    "UPDATE {tbl_name} SET {col_name} = {col_val} WHERE "
)
SQL_CLEAN_PREDICATES: sql.SQL = sql.SQL("{col_name} {comparison} {col_val}")


def make_queries(rows: list[RowData]) -> list[sql.Composed]:
    """Generate SQL queries from row data specifications."""
    queries: list[sql.Composed] = []

    for row in rows:
        predicate_clauses = [
            format_predicate(predicate) for predicate in row.predicate_cols
        ]
        predicates = sql.SQL(" AND ").join(predicate_clauses)

        if row.dml is _DML.UPDATE:
            base_query = SQL_CLEAN_BASE_UPD.format(
                tbl_name=sql.Identifier(row.tbl_name),
                col_name=sql.Identifier(row.col_name),
                col_val=sql.Literal(row.col_val),
            )
        elif row.dml is _DML.DELETE:
            base_query = SQL_CLEAN_BASE_DEL.format(
                tbl_name=sql.Identifier(row.tbl_name),
            )
        elif row.dml is _DML.INSERT:
            raise NotImplementedError("INSERT operations not yet implemented")
        else:
            raise ValueError(f"Unsupported _DML operation: {row.dml}")

        full_query = base_query + predicates

        queries.append(full_query)

    return queries
