"""Pipeline orchestration: validate → merge → deduplicate → outliers.

``run_pipeline`` itself performs no I/O. File reading happens exclusively in
``src.core.merger``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.analyzer import detect_outliers
from src.core.cleaner import dedupe
from src.core.merger import load_and_merge_sources
from src.core.models import (
    ConflictStrategy,
    DuplicateStrategy,
    ExcelSource,
    OutlierMethod,
)

__all__ = [
    "CleaningConfig",
    "MergeConfig",
    "OutlierConfig",
    "PipelineConfig",
    "PipelineReport",
    "run_pipeline",
]


@dataclass
class OutlierConfig:
    enabled:       bool          = True
    method:        OutlierMethod = OutlierMethod.ISOLATION_FOREST
    contamination: float         = 0.05
    columns:       list[str]     = field(default_factory=list)


@dataclass
class CleaningConfig:
    duplicate_strategy: DuplicateStrategy = DuplicateStrategy.DROP_LAST
    duplicate_subset:   list[str]         = field(default_factory=list)


@dataclass
class MergeConfig:
    join_key:            str              = "id"
    conflict_resolution: ConflictStrategy = ConflictStrategy.LAST_WINS


@dataclass
class PipelineConfig:
    sources:     list[ExcelSource]
    merge:       MergeConfig    = field(default_factory=MergeConfig)
    cleaning:    CleaningConfig = field(default_factory=CleaningConfig)
    outliers:    OutlierConfig  = field(default_factory=OutlierConfig)
    output_path: Path           = field(
        default_factory=lambda: Path("output/merged_clean.xlsx")
    )


@dataclass
class PipelineReport:
    sources_loaded:     int
    rows_after_merge:   int
    rows_after_dedup:   int
    outliers_flagged:   int
    violations_found:   int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources_loaded":   self.sources_loaded,
            "rows_after_merge": self.rows_after_merge,
            "rows_after_dedup": self.rows_after_dedup,
            "outliers_flagged": self.outliers_flagged,
            "violations_found": self.violations_found,
        }


def run_pipeline(config: PipelineConfig) -> PipelineReport:
    """Orchestrate the full data pipeline.

    Stages:
        1. Load + validate + merge all sources via the merger.
        2. Deduplicate the merged DataFrame.
        3. Detect outliers on the deduplicated DataFrame, if enabled.

    Args:
        config: Full pipeline configuration.

    Returns:
        A :class:`PipelineReport` summarising every stage.
    """
    merged, merge_report = load_and_merge_sources(
        config.sources,
        join_key=config.merge.join_key,
        conflict_resolution=config.merge.conflict_resolution,
    )
    rows_after_merge = merge_report.rows_total

    deduped = _run_dedup(merged, config.cleaning, config.merge.join_key)
    rows_after_dedup = len(deduped)

    outliers_flagged = _run_outlier_detection(deduped, config.outliers)

    return PipelineReport(
        sources_loaded=merge_report.sources_loaded,
        rows_after_merge=rows_after_merge,
        rows_after_dedup=rows_after_dedup,
        outliers_flagged=outliers_flagged,
        violations_found=0,
    )


def _run_dedup(
    df: pd.DataFrame,
    cleaning: CleaningConfig,
    join_key: str,
) -> pd.DataFrame:
    """Apply the duplicate strategy and return the resulting DataFrame."""
    if df.empty:
        return df
    subset = cleaning.duplicate_subset or [join_key]
    cleaned, _ = dedupe(df, subset=subset, strategy=cleaning.duplicate_strategy)
    return cleaned


def _run_outlier_detection(df: pd.DataFrame, cfg: OutlierConfig) -> int:
    """Run outlier detection if enabled and return the flagged count."""
    if not cfg.enabled or df.empty:
        return 0
    columns = cfg.columns or df.select_dtypes(include="number").columns.tolist()
    if not columns:
        return 0
    report = detect_outliers(
        df,
        columns=columns,
        method=cfg.method,
        contamination=cfg.contamination,
    )
    return len(report.outlier_indices)
