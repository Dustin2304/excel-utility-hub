"""Outlier detection — pure logic, no I/O.

Three methods are available:

- ``IQR``: classical interquartile-range fence (Q1 - 1.5*IQR, Q3 + 1.5*IQR).
  Fast, dependency-free, works well for normally distributed data.
- ``ZSCORE``: absolute z-score with a fixed threshold of 3.0.
  Easy to explain, sensitive to extreme values.
- ``ISOLATION_FOREST``: scikit-learn's IsolationForest. ML-based, works for
  high-dimensional data without normality assumptions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from src.core.models import OutlierMethod, OutlierReport

_ZSCORE_THRESHOLD = 3.0
_IQR_FENCE = 1.5


def detect_outliers(
    df: pd.DataFrame,
    columns: list[str],
    method: OutlierMethod = OutlierMethod.ISOLATION_FOREST,
    contamination: float = 0.05,
) -> OutlierReport:
    """Detect outliers in the specified numeric columns.

    Args:
        df: Input DataFrame.
        columns: Numeric columns to check.
        method: Detection method to use.
        contamination: Expected proportion of outliers in [0.0, 0.5].
            Only used by ``ISOLATION_FOREST`` and as report metadata.

    Returns:
        An :class:`OutlierReport` with the row indices flagged as outliers.
    """
    if df.empty:
        return OutlierReport(
            outlier_indices=[],
            method_used=method,
            contamination_rate=contamination,
            columns_checked=list(columns),
        )

    data = df[columns]

    if method is OutlierMethod.ISOLATION_FOREST:
        indices = _detect_isolation_forest(data, contamination)
    elif method is OutlierMethod.ZSCORE:
        indices = _detect_zscore(data)
    elif method is OutlierMethod.IQR:
        indices = _detect_iqr(data)
    else:
        raise ValueError(f"Unsupported outlier method: {method}")

    return OutlierReport(
        outlier_indices=indices,
        method_used=method,
        contamination_rate=contamination,
        columns_checked=list(columns),
    )


def _detect_isolation_forest(data: pd.DataFrame, contamination: float) -> list[int]:
    filled = data.fillna(data.median(numeric_only=True))
    clf = IsolationForest(contamination=contamination, random_state=42)
    predictions = clf.fit_predict(filled)
    return [int(i) for i in data.index[predictions == -1].tolist()]


def _detect_zscore(data: pd.DataFrame) -> list[int]:
    std = data.std(ddof=0).replace(0, np.nan)
    z_scores = ((data - data.mean()) / std).abs()
    flagged = z_scores.fillna(0.0) > _ZSCORE_THRESHOLD
    outlier_mask = flagged.any(axis=1).to_numpy()
    return [int(i) for i in data.index[outlier_mask].tolist()]


def _detect_iqr(data: pd.DataFrame) -> list[int]:
    q1 = data.quantile(0.25)
    q3 = data.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - _IQR_FENCE * iqr
    upper = q3 + _IQR_FENCE * iqr
    outlier_mask = ((data < lower) | (data > upper)).any(axis=1)
    return [int(i) for i in data.index[outlier_mask.to_numpy()].tolist()]
