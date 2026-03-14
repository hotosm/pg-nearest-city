"""Data with known issues to be cleaned."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from psycopg import sql


class _CorrectionType(str, Enum):
    COORDINATES = "COORDINATES"
    ERRATUM = "ERRATUM"
    MISSING = "MISSING"
    SPELLING = "SPELLING"


class _ConflictAction(str, Enum):
    NOTHING = "NOTHING"
    UPDATE = "UPDATE"


class _PredicateComparison(str, Enum):
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
    NOT_EQUAL = "<>"
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


def _validate_predicates(predicate_cols: list[PredicateData]) -> None:
    if not predicate_cols:
        raise ValueError("At least one predicate must be provided")
    if any(not isinstance(p, PredicateData) for p in predicate_cols):
        bad = {type(p).__name__ for p in predicate_cols}
        raise TypeError(f"predicate_cols must be PredicateData; got {bad}")


@dataclass
class UpdateData:
    """An UPDATE correction: sets col_name = col_val where predicate_cols match."""

    correction_type: _CorrectionType
    description: str
    tbl_name: str
    col_name: str
    col_val: float | int | str
    predicate_cols: list[PredicateData] = field(default_factory=list)
    result_limit: int = 1
    is_post_load: bool = True

    def __post_init__(self) -> None:
        _validate_predicates(self.predicate_cols)
        if self.result_limit < 0:
            raise ValueError("result_limit must be non-negative")


@dataclass
class DeleteData:
    """A DELETE correction: removes rows where predicate_cols match."""

    correction_type: _CorrectionType
    description: str
    tbl_name: str
    predicate_cols: list[PredicateData] = field(default_factory=list)
    result_limit: int = 1
    is_post_load: bool = True

    def __post_init__(self) -> None:
        _validate_predicates(self.predicate_cols)
        if self.result_limit < 0:
            raise ValueError("result_limit must be non-negative")


@dataclass
class InsertData:
    """An INSERT correction.

    col_vals: mapping of column name → value to insert.

    Conflict resolution (mutually exclusive):
      - conflict_cols + conflict_action: ON CONFLICT (...) DO NOTHING / DO UPDATE SET
      - existence_check: INSERT ... SELECT ... WHERE NOT EXISTS
        (SELECT 1 FROM ... WHERE ...)
    """

    correction_type: _CorrectionType
    description: str
    tbl_name: str
    col_vals: dict[str, float | int | str]
    conflict_cols: list[str] = field(default_factory=list)
    conflict_action: _ConflictAction | None = None
    existence_check: list[PredicateData] = field(default_factory=list)
    result_limit: int = 1
    is_post_load: bool = True

    def __post_init__(self) -> None:
        if not self.col_vals:
            raise ValueError("col_vals must not be empty")
        if self.conflict_cols and self.existence_check:
            raise ValueError(
                "conflict_cols/conflict_action and existence_check "
                "are mutually exclusive"
            )
        if bool(self.conflict_cols) != (self.conflict_action is not None):
            raise ValueError(
                "conflict_cols and conflict_action must both be provided or neither"
            )
        if self.result_limit < 0:
            raise ValueError("result_limit must be non-negative")
        if self.existence_check:
            if any(not isinstance(p, PredicateData) for p in self.existence_check):
                bad = {type(p).__name__ for p in self.existence_check}
                raise TypeError(f"existence_check must be PredicateData; got {bad}")


RowData = UpdateData | DeleteData | InsertData


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


SQL_CLEAN_BASE_DEL: sql.SQL = sql.SQL("DELETE FROM {tbl_name} WHERE ")
SQL_CLEAN_BASE_UPD: sql.SQL = sql.SQL(
    "UPDATE {tbl_name} SET {col_name} = {col_val} WHERE "
)


def _make_insert_query(row: InsertData) -> sql.Composed:
    col_names = sql.SQL(", ").join(sql.Identifier(k) for k in row.col_vals)
    col_values = sql.SQL(", ").join(sql.Literal(v) for v in row.col_vals.values())

    if row.existence_check:
        predicates = sql.SQL(" AND ").join(
            format_predicate(p) for p in row.existence_check
        )
        insert_part = sql.SQL(
            "INSERT INTO {tbl_name} ({col_names}) SELECT {col_vals}"
        ).format(
            tbl_name=sql.Identifier(row.tbl_name),
            col_names=col_names,
            col_vals=col_values,
        )
        not_exists_part = sql.SQL(
            " WHERE NOT EXISTS (SELECT 1 FROM {tbl_name} WHERE {predicates})"
        ).format(
            tbl_name=sql.Identifier(row.tbl_name),
            predicates=predicates,
        )
        return insert_part + not_exists_part

    elif row.conflict_cols:
        conflict_targets = sql.SQL(", ").join(
            sql.Identifier(c) for c in row.conflict_cols
        )
        if row.conflict_action is _ConflictAction.UPDATE:
            update_cols = [k for k in row.col_vals if k not in row.conflict_cols]
            set_clause = sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in update_cols
            )
            return sql.SQL(
                "INSERT INTO {tbl_name} ({col_names}) VALUES ({col_vals}) "
                "ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
            ).format(
                tbl_name=sql.Identifier(row.tbl_name),
                col_names=col_names,
                col_vals=col_values,
                conflict_cols=conflict_targets,
                set_clause=set_clause,
            )
        else:  # NOTHING
            return sql.SQL(
                "INSERT INTO {tbl_name} ({col_names}) VALUES ({col_vals}) "
                "ON CONFLICT ({conflict_cols}) DO NOTHING"
            ).format(
                tbl_name=sql.Identifier(row.tbl_name),
                col_names=col_names,
                col_vals=col_values,
                conflict_cols=conflict_targets,
            )

    else:
        return sql.SQL(
            "INSERT INTO {tbl_name} ({col_names}) VALUES ({col_vals})"
        ).format(
            tbl_name=sql.Identifier(row.tbl_name),
            col_names=col_names,
            col_vals=col_values,
        )


def make_queries(rows: list[RowData]) -> list[sql.Composed]:
    """Generate SQL queries from row data specifications."""
    queries: list[sql.Composed] = []

    for row in rows:
        if isinstance(row, InsertData):
            queries.append(_make_insert_query(row))
            continue

        predicate_clauses = [format_predicate(p) for p in row.predicate_cols]
        predicates = sql.SQL(" AND ").join(predicate_clauses)

        if isinstance(row, UpdateData):
            base_query = SQL_CLEAN_BASE_UPD.format(
                tbl_name=sql.Identifier(row.tbl_name),
                col_name=sql.Identifier(row.col_name),
                col_val=sql.Literal(row.col_val),
            )
        else:  # DeleteData
            base_query = SQL_CLEAN_BASE_DEL.format(
                tbl_name=sql.Identifier(row.tbl_name),
            )

        queries.append(base_query + predicates)

    return queries
