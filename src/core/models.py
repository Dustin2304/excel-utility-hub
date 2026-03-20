from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
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

def to_dict(self: "ValidationReport") -> dict[str, Any]:        return {
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

