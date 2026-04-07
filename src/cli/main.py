"""CLI adapter for the Excel Utility Hub pipeline.

This module is the only adapter touching the outside world: it parses CLI
arguments, loads a YAML configuration, runs the pipeline, and prints the
result. It contains no data transformation logic.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

from src.core.models import (
    ColumnSchema,
    ConflictStrategy,
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


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        description="Excel Utility Hub — run a data pipeline from a YAML config.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the pipeline YAML configuration file.",
    )
    args = parser.parse_args(argv)

    if not args.config.exists():
        print(f"Error: config file not found: {args.config}", file=sys.stderr)
        return 1

    try:
        config = _load_config(args.config)
        report = run_pipeline(config)
    except (ValueError, KeyError, yaml.YAMLError) as exc:
        print(f"Pipeline error: {exc}", file=sys.stderr)
        return 1

    _print_report(report)
    return 0


def _load_config(path: Path) -> PipelineConfig:
    """Parse a YAML file into a :class:`PipelineConfig`."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping.")

    sources = [_build_source(s) for s in raw.get("sources", [])]
    merge_cfg = _build_merge_config(raw.get("merge", {}))
    cleaning_cfg = _build_cleaning_config(raw.get("cleaning", {}))
    outliers_cfg = _build_outlier_config(raw.get("outliers", {}))
    output_path = Path(raw.get("output", {}).get("path", "output/merged_clean.xlsx"))

    return PipelineConfig(
        sources=sources,
        merge=merge_cfg,
        cleaning=cleaning_cfg,
        outliers=outliers_cfg,
        output_path=output_path,
    )


def _build_source(raw: dict[str, Any]) -> ExcelSource:
    schema = _build_schema(raw["schema"])
    return ExcelSource(
        path=Path(raw["path"]),
        expected_schema=schema,
        column_mapping=dict(raw.get("column_mapping", {})),
    )


def _build_schema(raw: dict[str, Any]) -> Schema:
    columns = [_build_column(c) for c in raw.get("columns", [])]
    return Schema(
        columns=columns,
        allow_extra_cols=bool(raw.get("allow_extra_cols", False)),
    )


def _build_column(raw: dict[str, Any]) -> ColumnSchema:
    return ColumnSchema(
        name=str(raw["name"]),
        dtype=DType(raw["dtype"]),
        nullable=bool(raw.get("nullable", False)),
        min_value=raw.get("min_value"),
        max_value=raw.get("max_value"),
        allowed_values=list(raw.get("allowed_values", []) or []),
    )


def _build_merge_config(raw: dict[str, Any]) -> MergeConfig:
    return MergeConfig(
        join_key=str(raw.get("join_key", "id")),
        conflict_resolution=ConflictStrategy(
            raw.get("conflict_resolution", ConflictStrategy.LAST_WINS.value)
        ),
    )


def _build_cleaning_config(raw: dict[str, Any]) -> CleaningConfig:
    return CleaningConfig(
        duplicate_strategy=DuplicateStrategy(
            raw.get("duplicate_strategy", DuplicateStrategy.DROP_LAST.value)
        ),
        duplicate_subset=list(raw.get("duplicate_subset", []) or []),
    )


def _build_outlier_config(raw: dict[str, Any]) -> OutlierConfig:
    return OutlierConfig(
        enabled=bool(raw.get("enabled", True)),
        method=OutlierMethod(
            raw.get("method", OutlierMethod.ISOLATION_FOREST.value)
        ),
        contamination=float(raw.get("contamination", 0.05)),
        columns=list(raw.get("columns", []) or []),
    )


def _print_report(report: PipelineReport) -> None:
    """Print the pipeline report as a small human-readable summary."""
    print("Pipeline finished successfully.")
    print(json.dumps(report.to_dict(), indent=2))


if __name__ == "__main__":
    sys.exit(main())
