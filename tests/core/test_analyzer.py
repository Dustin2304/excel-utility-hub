from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.core.analyzer import detect_outliers
from src.core.models import OutlierMethod, OutlierReport


@pytest.fixture
def df_with_outliers() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    normal = rng.normal(50, 5, 100).tolist()
    outliers = [200.0, -100.0]
    return pd.DataFrame({"value": normal + outliers})


@pytest.fixture
def df_clean() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({"value": rng.normal(50, 5, 100).tolist()})


@pytest.mark.parametrize("method", list(OutlierMethod))
def test_all_methods_return_outlier_report(
    df_with_outliers: pd.DataFrame, method: OutlierMethod
) -> None:
    report = detect_outliers(df_with_outliers, columns=["value"], method=method)
    assert isinstance(report, OutlierReport)
    assert report.method_used == method


@pytest.mark.parametrize("method", list(OutlierMethod))
def test_obvious_outliers_detected(
    df_with_outliers: pd.DataFrame, method: OutlierMethod
) -> None:
    report = detect_outliers(df_with_outliers, columns=["value"], method=method)
    assert 100 in report.outlier_indices or 101 in report.outlier_indices


def test_clean_data_has_few_outliers(df_clean: pd.DataFrame) -> None:
    report = detect_outliers(
        df_clean, columns=["value"], method=OutlierMethod.ZSCORE
    )
    assert len(report.outlier_indices) < 5


def test_columns_checked_recorded(df_with_outliers: pd.DataFrame) -> None:
    report = detect_outliers(df_with_outliers, columns=["value"])
    assert "value" in report.columns_checked


def test_to_dict_structure(df_with_outliers: pd.DataFrame) -> None:
    report = detect_outliers(df_with_outliers, columns=["value"])
    d = report.to_dict()
    assert set(d.keys()) == {
        "outlier_indices",
        "method_used",
        "contamination_rate",
        "columns_checked",
    }
    assert isinstance(d["method_used"], str)


def test_empty_dataframe() -> None:
    df = pd.DataFrame({"value": pd.Series([], dtype=float)})
    report = detect_outliers(df, columns=["value"], method=OutlierMethod.IQR)
    assert report.outlier_indices == []


def test_multiple_columns(df_with_outliers: pd.DataFrame) -> None:
    df = df_with_outliers.copy()
    df["value2"] = df["value"] * 2
    report = detect_outliers(df, columns=["value", "value2"])
    assert "value" in report.columns_checked
    assert "value2" in report.columns_checked
