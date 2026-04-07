from __future__ import annotations

import pandas as pd
import pytest

from src.core.cleaner import handle_duplicates
from src.core.models import DedupReport, DuplicateStrategy


@pytest.fixture
def df_with_dupes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name":  ["Alice", "Bob", "Alice", "Charlie", "Bob"],
            "score": [85.0, 90.0, 85.0, 73.5, 90.0],
            "dept":  ["HR", "IT", "HR", "Finance", "IT"],
        }
    )


@pytest.fixture
def df_no_dupes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name":  ["Alice", "Bob", "Charlie"],
            "score": [85.0, 90.0, 73.5],
        }
    )


@pytest.fixture
def df_all_dupes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name":  ["Alice", "Alice", "Alice"],
            "score": [85.0, 85.0, 85.0],
        }
    )


@pytest.mark.parametrize("strategy", list(DuplicateStrategy))
def test_all_strategies_return_dedup_report(
    df_with_dupes: pd.DataFrame, strategy: DuplicateStrategy
) -> None:
    report = handle_duplicates(df_with_dupes, subset=["name"], strategy=strategy)
    assert isinstance(report, DedupReport)
    assert report.strategy_used == strategy


def test_drop_first_removes_correct_rows(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes, subset=["name"], strategy=DuplicateStrategy.DROP_FIRST
    )
    assert report.duplicates_found == 2
    assert report.rows_affected == 2


def test_drop_last_removes_correct_rows(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes, subset=["name"], strategy=DuplicateStrategy.DROP_LAST
    )
    assert report.duplicates_found == 2
    assert report.rows_affected == 2


def test_flag_strategy_counts_all_flagged_rows(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes, subset=["name"], strategy=DuplicateStrategy.FLAG
    )
    assert report.duplicates_found == 2
    assert report.rows_affected == 4


def test_merge_aggregate_detects_duplicates(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes, subset=["name"], strategy=DuplicateStrategy.MERGE_AGGREGATE
    )
    assert report.duplicates_found == 2
    assert report.rows_affected == 2


def test_no_duplicates_returns_zero(df_no_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_no_dupes, subset=["name"], strategy=DuplicateStrategy.DROP_LAST
    )
    assert report.duplicates_found == 0
    assert report.rows_affected == 0


def test_all_rows_identical(df_all_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_all_dupes, subset=["name"], strategy=DuplicateStrategy.DROP_FIRST
    )
    assert report.duplicates_found > 0
    assert report.rows_affected == 2


def test_empty_dataframe() -> None:
    df = pd.DataFrame({"name": [], "score": []})
    report = handle_duplicates(
        df, subset=["name"], strategy=DuplicateStrategy.DROP_FIRST
    )
    assert report.duplicates_found == 0
    assert report.rows_affected == 0


def test_multi_column_subset(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes,
        subset=["name", "score"],
        strategy=DuplicateStrategy.DROP_FIRST,
    )
    assert isinstance(report, DedupReport)
    assert report.duplicates_found == 2


def test_to_dict_structure(df_with_dupes: pd.DataFrame) -> None:
    report = handle_duplicates(
        df_with_dupes, subset=["name"], strategy=DuplicateStrategy.DROP_FIRST
    )
    d = report.to_dict()
    assert set(d.keys()) == {"duplicates_found", "rows_affected", "strategy_used"}
    assert isinstance(d["strategy_used"], str)
    assert d["strategy_used"] == "drop_first"
