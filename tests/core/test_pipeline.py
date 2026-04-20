from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.core.models import (
    ColumnSchema,
    DType,
    DuplicateStrategy,
    ExcelSource,
    OutlierMethod,
    Schema,
)
from src.core.pipeline import (
    CleaningConfig,
    MergeConfig,
    OutlierConfig,
    PipelineConfig,
    PipelineReport,
    run_pipeline,
)


@pytest.fixture
def schema() -> Schema:
    return Schema(
        columns=[
            ColumnSchema("customer_id", DType.INTEGER, nullable=False),
            ColumnSchema("revenue",     DType.FLOAT,   nullable=False),
        ],
        allow_extra_cols=True,
    )


def make_excel(tmp_path: Path, name: str, data: dict[str, Any]) -> Path:
    path = tmp_path / name
    pd.DataFrame(data).to_excel(path, index=False)
    return path


def test_minimal_pipeline_returns_report(tmp_path: Path, schema: Schema) -> None:
    p1 = make_excel(
        tmp_path,
        "s.xlsx",
        {"customer_id": [1, 2, 3], "revenue": [10.5, 20.5, 30.5]},
    )
    config = PipelineConfig(
        sources=[ExcelSource(path=p1, expected_schema=schema)],
        merge=MergeConfig(join_key="customer_id"),
        outliers=OutlierConfig(enabled=False),
    )
    report = run_pipeline(config)
    assert isinstance(report, PipelineReport)
    assert report.sources_loaded == 1
    assert report.rows_after_merge == 3
    assert report.rows_after_dedup == 3
    assert report.outliers_flagged == 0


def test_dedup_reduces_rows(tmp_path: Path, schema: Schema) -> None:
    p1 = make_excel(
        tmp_path,
        "s1.xlsx",
        {"customer_id": [1, 2], "revenue": [10.5, 20.5]},
    )
    p2 = make_excel(
        tmp_path,
        "s2.xlsx",
        {"customer_id": [3, 4], "revenue": [30.5, 40.5]},
    )
    config = PipelineConfig(
        sources=[
            ExcelSource(path=p1, expected_schema=schema),
            ExcelSource(path=p2, expected_schema=schema),
        ],
        merge=MergeConfig(join_key="customer_id"),
        cleaning=CleaningConfig(
            duplicate_strategy=DuplicateStrategy.DROP_LAST,
            duplicate_subset=["customer_id"],
        ),
        outliers=OutlierConfig(enabled=False),
    )
    report = run_pipeline(config)
    assert report.rows_after_merge == 4
    assert report.rows_after_dedup == 4


def test_outlier_detection_flags_rows(tmp_path: Path, schema: Schema) -> None:
    rng = np.random.default_rng(42)
    revenue = rng.normal(50, 5, 50).tolist() + [9999.5, -9999.5]
    customer_ids = list(range(1, len(revenue) + 1))
    p1 = make_excel(
        tmp_path, "s.xlsx", {"customer_id": customer_ids, "revenue": revenue}
    )
    config = PipelineConfig(
        sources=[ExcelSource(path=p1, expected_schema=schema)],
        merge=MergeConfig(join_key="customer_id"),
        outliers=OutlierConfig(
            enabled=True,
            method=OutlierMethod.IQR,
            columns=["revenue"],
        ),
    )
    report = run_pipeline(config)
    assert report.outliers_flagged > 0


def test_outliers_disabled(tmp_path: Path, schema: Schema) -> None:
    p1 = make_excel(
        tmp_path,
        "s.xlsx",
        {"customer_id": [1, 2], "revenue": [10.5, 99999.5]},
    )
    config = PipelineConfig(
        sources=[ExcelSource(path=p1, expected_schema=schema)],
        merge=MergeConfig(join_key="customer_id"),
        outliers=OutlierConfig(enabled=False),
    )
    report = run_pipeline(config)
    assert report.outliers_flagged == 0


def test_to_dict_structure(tmp_path: Path, schema: Schema) -> None:
    p1 = make_excel(
        tmp_path, "s.xlsx", {"customer_id": [1], "revenue": [10.5]}
    )
    config = PipelineConfig(
        sources=[ExcelSource(path=p1, expected_schema=schema)],
        merge=MergeConfig(join_key="customer_id"),
        outliers=OutlierConfig(enabled=False),
    )
    d = run_pipeline(config).to_dict()
    assert set(d.keys()) == {
        "sources_loaded",
        "rows_after_merge",
        "rows_after_dedup",
        "outliers_flagged",
    }
