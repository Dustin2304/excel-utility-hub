from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.core.merger import merge_sources
from src.core.models import (
    ColumnSchema,
    ConflictStrategy,
    DType,
    ExcelSource,
    MergeReport,
    Schema,
)


@pytest.fixture
def simple_schema() -> Schema:
    return Schema(
        columns=[
            ColumnSchema("customer_id", DType.INTEGER, nullable=False),
            ColumnSchema("revenue",     DType.FLOAT,   nullable=False),
        ],
        allow_extra_cols=True,
    )


def make_excel(tmp_path: Path, filename: str, data: dict[str, Any]) -> Path:
    path = tmp_path / filename
    pd.DataFrame(data).to_excel(path, index=False)
    return path


def test_merge_two_compatible_sources(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(
        tmp_path, "s1.xlsx", {"customer_id": [1, 2], "revenue": [100.5, 200.5]}
    )
    p2 = make_excel(
        tmp_path, "s2.xlsx", {"customer_id": [3, 4], "revenue": [300.5, 400.5]}
    )
    sources = [
        ExcelSource(path=p1, expected_schema=simple_schema),
        ExcelSource(path=p2, expected_schema=simple_schema),
    ]
    report = merge_sources(sources, join_key="customer_id")
    assert isinstance(report, MergeReport)
    assert report.sources_loaded == 2
    assert report.rows_total == 4
    assert report.conflicts_resolved == 0


def test_column_mapping_applied(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(tmp_path, "s1.xlsx", {"customer_id": [1], "revenue": [100.5]})
    p2 = make_excel(tmp_path, "s2.xlsx", {"Kundennummer": [2], "Umsatz": [200.5]})
    sources = [
        ExcelSource(path=p1, expected_schema=simple_schema),
        ExcelSource(
            path=p2,
            expected_schema=simple_schema,
            column_mapping={"Kundennummer": "customer_id", "Umsatz": "revenue"},
        ),
    ]
    report = merge_sources(sources, join_key="customer_id")
    assert report.sources_loaded == 2
    assert report.rows_total == 2


def test_last_wins_resolves_conflict(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(tmp_path, "s1.xlsx", {"customer_id": [1], "revenue": [100.5]})
    p2 = make_excel(tmp_path, "s2.xlsx", {"customer_id": [1], "revenue": [999.5]})
    sources = [
        ExcelSource(path=p1, expected_schema=simple_schema),
        ExcelSource(path=p2, expected_schema=simple_schema),
    ]
    report = merge_sources(
        sources, join_key="customer_id", conflict_resolution=ConflictStrategy.LAST_WINS
    )
    assert report.conflicts_resolved == 1
    assert report.rows_total == 1


def test_first_wins_resolves_conflict(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(
        tmp_path, "s1.xlsx", {"customer_id": [1, 2], "revenue": [100.5, 200.5]}
    )
    p2 = make_excel(
        tmp_path, "s2.xlsx", {"customer_id": [2, 3], "revenue": [999.5, 300.5]}
    )
    sources = [
        ExcelSource(path=p1, expected_schema=simple_schema),
        ExcelSource(path=p2, expected_schema=simple_schema),
    ]
    report = merge_sources(
        sources, join_key="customer_id", conflict_resolution=ConflictStrategy.FIRST_WINS
    )
    assert report.conflicts_resolved == 1
    assert report.rows_total == 3


def test_raise_strategy_on_conflict(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(tmp_path, "s1.xlsx", {"customer_id": [1], "revenue": [100.5]})
    p2 = make_excel(tmp_path, "s2.xlsx", {"customer_id": [1], "revenue": [999.5]})
    sources = [
        ExcelSource(path=p1, expected_schema=simple_schema),
        ExcelSource(path=p2, expected_schema=simple_schema),
    ]
    with pytest.raises(ValueError, match="Conflict"):
        merge_sources(
            sources, join_key="customer_id", conflict_resolution=ConflictStrategy.RAISE
        )


def test_empty_sources_list() -> None:
    report = merge_sources([], join_key="customer_id")
    assert report.sources_loaded == 0
    assert report.rows_total == 0
    assert report.conflicts_resolved == 0


def test_schema_violation_raises(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(tmp_path, "bad.xlsx", {"wrong_col": [1], "revenue": [100.5]})
    sources = [ExcelSource(path=p1, expected_schema=simple_schema)]
    with pytest.raises(ValueError, match="failed validation"):
        merge_sources(sources, join_key="customer_id")


def test_to_dict_structure(tmp_path: Path, simple_schema: Schema) -> None:
    p1 = make_excel(tmp_path, "s1.xlsx", {"customer_id": [1], "revenue": [100.5]})
    report = merge_sources(
        [ExcelSource(path=p1, expected_schema=simple_schema)], join_key="customer_id"
    )
    d = report.to_dict()
    assert set(d.keys()) == {
        "sources_loaded",
        "rows_total",
        "conflicts_resolved",
        "strategy_used",
    }
    assert isinstance(d["strategy_used"], str)
