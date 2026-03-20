from __future__ import annotations

import pandas as pd
import pytest

from src.core.models import ColumnSchema, DType, Schema
from src.core.validator import validate_against_schema as validate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schema(*cols: ColumnSchema, allow_extra: bool = False) -> Schema:
    return Schema(columns=list(cols), allow_extra_cols=allow_extra)


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------

def test_valid_dataframe_passes() -> None:
    schema = _schema(
        ColumnSchema(name="age", dtype=DType.INTEGER),
        ColumnSchema(name="name", dtype=DType.STRING),
    )
    df = pd.DataFrame({"age": [25, 30], "name": ["Alice", "Bob"]})

    report = validate(df, schema)

    assert report.is_valid is True
    assert report.violations == []


def test_missing_column_detected() -> None:
    schema = _schema(
        ColumnSchema(name="age", dtype=DType.INTEGER),
        ColumnSchema(name="name", dtype=DType.STRING),
    )
    df = pd.DataFrame({"age": [25, 30]})  # "name" fehlt

    report = validate(df, schema)

    assert report.is_valid is False
    assert len(report.violations) == 1
    assert report.violations[0].column == "name"
    assert report.violations[0].rule == "missing_column"


# ---------------------------------------------------------------------------
# Missing columns
# ---------------------------------------------------------------------------

def test_multiple_missing_columns_detected() -> None:
    schema = _schema(
        ColumnSchema(name="age", dtype=DType.INTEGER),
        ColumnSchema(name="name", dtype=DType.STRING),
        ColumnSchema(name="score", dtype=DType.FLOAT),
    )
    df = pd.DataFrame({"age": [25]})

    report = validate(df, schema)

    missing_cols = {v.column for v in report.violations if v.rule == "missing_column"}
    assert missing_cols == {"name", "score"}


# ---------------------------------------------------------------------------
# Extra columns
# ---------------------------------------------------------------------------

def test_extra_column_detected() -> None:
    schema = _schema(ColumnSchema(name="age", dtype=DType.INTEGER))
    df = pd.DataFrame({"age": [25], "unexpected": ["x"]})

    report = validate(df, schema)

    assert report.is_valid is False
    extra = [v for v in report.violations if v.rule == "extra_column"]
    assert len(extra) == 1
    assert extra[0].column == "unexpected"


def test_extra_column_allowed_when_schema_permits() -> None:
    schema = _schema(
        ColumnSchema(name="age", dtype=DType.INTEGER),
        allow_extra=True,
    )
    df = pd.DataFrame({"age": [25], "bonus": ["x"]})

    report = validate(df, schema)

    assert report.is_valid is True


# ---------------------------------------------------------------------------
# Nullable
# ---------------------------------------------------------------------------

def test_null_in_non_nullable_column_detected() -> None:
    schema = _schema(ColumnSchema(name="age", dtype=DType.INTEGER, nullable=False))
    df = pd.DataFrame({"age": [25, None]})

    report = validate(df, schema)

    assert report.is_valid is False
    nullable_viols = [v for v in report.violations if v.rule == "nullable"]
    assert len(nullable_viols) == 1
    assert 1 in nullable_viols[0].row_indices


def test_null_in_nullable_column_passes() -> None:
    schema = _schema(ColumnSchema(name="age", dtype=DType.INTEGER, nullable=True))
    df = pd.DataFrame({"age": [25, None]})

    report = validate(df, schema)

    nullable_viols = [v for v in report.violations if v.rule == "nullable"]
    assert nullable_viols == []


# ---------------------------------------------------------------------------
# DType — parametrized
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "dtype, values, bad_indices",
    [
        (DType.INTEGER, [10, "thirty"], [1]),
        (DType.STRING,  [1, 2],        [0, 1]),
        (DType.BOOLEAN, [True, "yes"], [1]),
    ],
    ids=["integer", "string", "boolean"],
)
def test_dtype_violation(
    dtype: DType, values: list[object], bad_indices: list[int]
) -> None:
    schema = _schema(ColumnSchema(name="col", dtype=dtype))
    df = pd.DataFrame({"col": values})

    report = validate(df, schema)

    dtype_viols = [v for v in report.violations if v.rule == "dtype"]
    assert len(dtype_viols) == 1
    assert dtype_viols[0].row_indices == bad_indices


# ---------------------------------------------------------------------------
# Range — parametrized
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "min_val, max_val, values, rule, bad_indices",
    [
        (18,   None, [15, 20],   "min_value", [0]),
        (None, 100,  [80, 150],  "max_value", [1]),
    ],
    ids=["below_min", "above_max"],
)
def test_range_violation(
    min_val: float | None,
    max_val: float | None,
    values: list[int],
    rule: str,
    bad_indices: list[int],
) -> None:
    col = ColumnSchema(
        name="age", dtype=DType.INTEGER, min_value=min_val, max_value=max_val
    )
    schema = _schema(col)
    df = pd.DataFrame({"age": values})

    report = validate(df, schema)

    range_viols = [v for v in report.violations if v.rule == rule]
    assert len(range_viols) == 1
    assert range_viols[0].row_indices == bad_indices


@pytest.mark.parametrize(
    "min_val, max_val, value",
    [
        (18,  None, 18),   # exakt am Minimum
        (None, 100, 100),  # exakt am Maximum
    ],
    ids=["at_min_boundary", "at_max_boundary"],
)
def test_range_boundary_passes(
    min_val: float | None, max_val: float | None, value: int
) -> None:
    col = ColumnSchema(
        name="age", dtype=DType.INTEGER, min_value=min_val, max_value=max_val
    )
    schema = _schema(col)
    df = pd.DataFrame({"age": [value]})

    report = validate(df, schema)

    range_viols = [v for v in report.violations if v.rule in ("min_value", "max_value")]
    assert range_viols == []


# ---------------------------------------------------------------------------
# Allowed values
# ---------------------------------------------------------------------------

def test_allowed_values_violation_detected() -> None:
    allowed = ["active", "inactive"]
    schema = _schema(
        ColumnSchema(name="status", dtype=DType.STRING, allowed_values=allowed)
    )
    df = pd.DataFrame({"status": ["active", "unknown"]})

    report = validate(df, schema)

    av_viols = [v for v in report.violations if v.rule == "allowed_values"]
    assert len(av_viols) == 1
    assert 1 in av_viols[0].row_indices


def test_allowed_values_passes() -> None:
    allowed = ["active", "inactive"]
    schema = _schema(
        ColumnSchema(name="status", dtype=DType.STRING, allowed_values=allowed)
    )
    df = pd.DataFrame({"status": ["active", "inactive", "active"]})

    report = validate(df, schema)

    av_viols = [v for v in report.violations if v.rule == "allowed_values"]
    assert av_viols == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_dataframe_passes() -> None:
    schema = _schema(
        ColumnSchema(name="age", dtype=DType.INTEGER),
        ColumnSchema(name="name", dtype=DType.STRING),
    )
    df = pd.DataFrame({
        "age": pd.Series([], dtype=int),
        "name": pd.Series([], dtype=str),
    })

    report = validate(df, schema)

    assert report.is_valid is True
    assert report.violations == []
