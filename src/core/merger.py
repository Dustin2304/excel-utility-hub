"""Multi-source Excel merger.

This module is the only place in ``src/core/`` that performs file I/O —
``pd.read_excel`` is unavoidable here and explicitly allowed by the
architecture rules.
"""

from __future__ import annotations

import pandas as pd

from src.core.models import (
    ConflictStrategy,
    ExcelSource,
    MergeReport,
)
from src.core.validator import validate_against_schema


def merge_sources(
    sources: list[ExcelSource],
    join_key: str,
    conflict_resolution: ConflictStrategy = ConflictStrategy.LAST_WINS,
) -> MergeReport:
    """Load, validate, and merge multiple Excel sources into one DataFrame.

    Args:
        sources: List of Excel source definitions.
        join_key: Column used as the unique identifier across sources.
        conflict_resolution: How to handle rows that share the same join_key
            across sources.

    Returns:
        A :class:`MergeReport` describing the merge.

    Raises:
        ValueError: If a source fails schema validation, or if conflicts
            are detected and ``conflict_resolution`` is ``RAISE``.
    """
    if not sources:
        return MergeReport(
            sources_loaded=0,
            rows_total=0,
            conflicts_resolved=0,
            strategy_used=conflict_resolution,
        )

    frames = [_load_and_validate(source) for source in sources]
    merged, conflicts_resolved = _resolve_conflicts(
        frames, join_key, conflict_resolution
    )
    return MergeReport(
        sources_loaded=len(sources),
        rows_total=len(merged),
        conflicts_resolved=conflicts_resolved,
        strategy_used=conflict_resolution,
    )


def _load_and_validate(source: ExcelSource) -> pd.DataFrame:
    """Read an Excel source, apply column mapping, and validate the schema."""
    df = pd.read_excel(source.path)
    if source.column_mapping:
        df = df.rename(columns=source.column_mapping)
    report = validate_against_schema(df, source.expected_schema)
    if not report.is_valid:
        raise ValueError(
            f"Source {source.path} failed validation: {report.summary}"
        )
    return df


def _resolve_conflicts(
    frames: list[pd.DataFrame],
    join_key: str,
    strategy: ConflictStrategy,
) -> tuple[pd.DataFrame, int]:
    """Combine frames according to the selected conflict strategy."""
    if strategy is ConflictStrategy.FIRST_WINS:
        return _first_wins(frames, join_key)
    if strategy is ConflictStrategy.LAST_WINS:
        return _last_wins(frames, join_key)
    if strategy is ConflictStrategy.RAISE:
        return _raise_on_conflict(frames, join_key)
    raise ValueError(f"Unsupported conflict strategy: {strategy}")


def _first_wins(
    frames: list[pd.DataFrame], join_key: str
) -> tuple[pd.DataFrame, int]:
    merged = frames[0]
    conflicts = 0
    for frame in frames[1:]:
        existing_keys = merged[join_key]
        mask_new = ~frame[join_key].isin(existing_keys)
        conflicts += int((~mask_new).sum())
        merged = pd.concat([merged, frame[mask_new]], ignore_index=True)
    return merged, conflicts


def _last_wins(
    frames: list[pd.DataFrame], join_key: str
) -> tuple[pd.DataFrame, int]:
    combined = pd.concat(frames, ignore_index=True)
    duplicate_mask = combined.duplicated(subset=[join_key], keep=False)
    conflicts = int(duplicate_mask.sum()) // 2
    merged = combined.drop_duplicates(subset=[join_key], keep="last").reset_index(
        drop=True
    )
    return merged, conflicts


def _raise_on_conflict(
    frames: list[pd.DataFrame], join_key: str
) -> tuple[pd.DataFrame, int]:
    combined = pd.concat(frames, ignore_index=True)
    duplicates = combined[combined.duplicated(subset=[join_key], keep=False)]
    if not duplicates.empty:
        keys = duplicates[join_key].unique().tolist()
        raise ValueError(f"Conflict detected for keys: {keys}")
    return combined, 0
