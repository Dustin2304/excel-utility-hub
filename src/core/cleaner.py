"""Duplicate handling for DataFrames — pure logic, no I/O."""

from __future__ import annotations

import pandas as pd

from src.core.models import DedupReport, DuplicateStrategy


def handle_duplicates(
    df: pd.DataFrame,
    subset: list[str],
    strategy: DuplicateStrategy,
) -> DedupReport:
    """Detect and handle duplicate rows according to a configurable strategy.

    Args:
        df: Input DataFrame. Never modified in place.
        subset: Columns to use for duplicate detection.
        strategy: How to handle duplicates.

    Returns:
        A :class:`DedupReport` describing what was found and how it was handled.
    """
    _, report = dedupe(df, subset=subset, strategy=strategy)
    return report


def dedupe(
    df: pd.DataFrame,
    subset: list[str],
    strategy: DuplicateStrategy,
) -> tuple[pd.DataFrame, DedupReport]:
    """Apply a duplicate strategy and return both the DataFrame and a report.

    Args:
        df: Input DataFrame. Never modified in place.
        subset: Columns to use for duplicate detection.
        strategy: How to handle duplicates.

    Returns:
        Tuple of ``(cleaned DataFrame, DedupReport)``. For ``FLAG`` the
        DataFrame gains an ``is_duplicate`` boolean column.
    """
    if df.empty:
        return df, DedupReport(
            duplicates_found=0, rows_affected=0, strategy_used=strategy
        )

    duplicate_mask = df.duplicated(subset=subset, keep=False)
    flagged_count = int(duplicate_mask.sum())
    duplicates_found = flagged_count // 2

    if duplicates_found == 0:
        return df, DedupReport(
            duplicates_found=0, rows_affected=0, strategy_used=strategy
        )

    if strategy is DuplicateStrategy.FLAG:
        result = df.copy()
        result["is_duplicate"] = duplicate_mask
        return result, DedupReport(
            duplicates_found=duplicates_found,
            rows_affected=flagged_count,
            strategy_used=strategy,
        )

    result = _apply_strategy(df, subset, strategy)
    rows_affected = len(df) - len(result)
    return result, DedupReport(
        duplicates_found=duplicates_found,
        rows_affected=rows_affected,
        strategy_used=strategy,
    )


def _apply_strategy(
    df: pd.DataFrame,
    subset: list[str],
    strategy: DuplicateStrategy,
) -> pd.DataFrame:
    """Return a deduplicated copy of ``df`` according to ``strategy``."""
    if strategy is DuplicateStrategy.DROP_FIRST:
        return df.drop_duplicates(subset=subset, keep="last")
    if strategy is DuplicateStrategy.DROP_LAST:
        return df.drop_duplicates(subset=subset, keep="first")
    if strategy is DuplicateStrategy.MERGE_AGGREGATE:
        return _merge_aggregate(df, subset)
    raise ValueError(f"Unsupported strategy: {strategy}")


def _merge_aggregate(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Aggregate duplicate groups: sum numeric columns, take first for others."""
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    agg: dict[str, str] = {}
    for col in df.columns:
        if col in subset:
            continue
        agg[col] = "sum" if col in numeric_cols else "first"
    return df.groupby(subset, as_index=False).agg(agg)
