from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class DType(str, Enum):
    INTEGER = "int"
    FLOAT   = "float"
    STRING  = "str"
    BOOLEAN = "bool"
    DATE    = "date"

@dataclass
class ColumnSchema:
    name:           str
    dtype:          DType
    nullable:       bool        = False
    min_value:      float | None = None
    max_value:      float | None = None
    allowed_values: list[Any]   = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.min_value is not None and self.max_value is not None:
            if self.min_value > self.max_value:
                raise ValueError(
                    f"min_value ({self.min_value}) > max_value ({self.max_value})"
                )

@dataclass
class Schema:
    columns:          list[ColumnSchema]
    allow_extra_cols: bool = False

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

@dataclass
class Violation:
    column:      str
    rule:        str
    message:     str
    row_indices: list[int] = field(default_factory=list)

@dataclass
class ValidationReport:
    is_valid:   bool
    violations: list[Violation] = field(default_factory=list)

    @property
    def summary(self) -> str:
        if self.is_valid:
            return "Validation passed."
        return (
            f"Validation failed with {len(self.violations)} violation(s): "
            + ", ".join(v.rule for v in self.violations)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid":   self.is_valid,
            "summary":    self.summary,
            "violations": [
                {
                    "column":      v.column,
                    "rule":        v.rule,
                    "message":     v.message,
                    "row_indices": v.row_indices,
                }
                for v in self.violations
            ],
        }


class DuplicateStrategy(str, Enum):
    DROP_FIRST      = "drop_first"
    DROP_LAST       = "drop_last"
    FLAG            = "flag"
    MERGE_AGGREGATE = "merge_aggregate"


@dataclass
class DedupReport:
    duplicates_found: int
    rows_affected:    int
    strategy_used:    DuplicateStrategy

    def to_dict(self) -> dict[str, Any]:
        return {
            "duplicates_found": self.duplicates_found,
            "rows_affected":    self.rows_affected,
            "strategy_used":    self.strategy_used.value,
        }


class ConflictStrategy(str, Enum):
    LAST_WINS  = "last_wins"
    FIRST_WINS = "first_wins"
    RAISE      = "raise"


@dataclass
class ExcelSource:
    path:            Path
    expected_schema: Schema
    column_mapping:  dict[str, str] = field(default_factory=dict)


@dataclass
class MergeReport:
    sources_loaded:     int
    rows_total:         int
    conflicts_resolved: int
    strategy_used:      ConflictStrategy

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources_loaded":     self.sources_loaded,
            "rows_total":         self.rows_total,
            "conflicts_resolved": self.conflicts_resolved,
            "strategy_used":      self.strategy_used.value,
        }


class OutlierMethod(str, Enum):
    ISOLATION_FOREST = "isolation_forest"
    ZSCORE           = "zscore"
    IQR              = "iqr"


@dataclass
class OutlierReport:
    outlier_indices:    list[int]
    method_used:        OutlierMethod
    contamination_rate: float
    columns_checked:    list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "outlier_indices":    self.outlier_indices,
            "method_used":        self.method_used.value,
            "contamination_rate": self.contamination_rate,
            "columns_checked":    self.columns_checked,
        }
