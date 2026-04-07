# Excel Utility Hub — Claude Code Master Context

## Project

Python toolkit for Excel data pipelines. Validates DataFrames against
defined schemas, handles duplicates, merges multiple Excel sources,
and detects outliers via ML. Portfolio project demonstrating software
architecture, testing discipline, and pragmatic ML integration.

Repo: https://github.com/Dustin2304/excel-utility-hub

---

## Non-Negotiable Architecture Rules

- `src/core/` contains pure logic only. No I/O, no side effects,
  no print statements, no file reads. Every function takes data in
  and returns a result — nothing else.
- `src/cli/` is the only adapter that touches the outside world.
  It reads arguments, calls core/, formats output. No logic here.
- `src/dashboard/` does not exist yet. Do not create it.
- Test data is always constructed as pd.DataFrame inline in the test.
  Never read from external files in unit tests. Use tmp_path fixture
  only in merger tests where file I/O is unavoidable.
- No hardcoded paths anywhere. Configuration flows through
  PipelineConfig only.
- Dataclasses for all data structures. Never return raw dicts.
- Enums instead of magic strings. Always.
- Full type hints on every function and method including return types.
- `field(default_factory=list)` for all mutable dataclass defaults.
- Private helper functions prefixed with `_`.
- No function longer than 40 lines. Split if needed.
- Never suppress mypy errors with `# type: ignore` without a comment.
- No `except: pass` or bare `except Exception` without logging.

---

## Tech Stack

- Python 3.11
- pandas >= 2.0
- scikit-learn >= 1.3 (Isolation Forest only)
- pytest + pytest-cov
- ruff (linting + import sorting)
- mypy strict (type checking)
- GitHub Actions (CI)
- Docker

---

## Quality Gate — Run Before Every Commit
```bash
ruff check src/ tests/
mypy src/
pytest tests/ --cov=src --cov-report=term-missing
```

All three must pass clean. No commit with any failure.

---

## Conventional Commits — Always
feat(core):    new functionality in src/core/
feat(cli):     new functionality in src/cli/
fix(core):     bug fix
test(core):    tests added or changed
refactor(core): restructure without behavior change
chore(ci):     tooling, config, workflow changes
docs:          README, CLAUDE.md, docstrings

One commit per completed logical unit. If you need "and" in the
message, it's two commits.

---

## Testing Philosophy

- Write failing tests before implementing (TDD). Always.
- `@pytest.fixture` for reusable DataFrames and schemas.
- `@pytest.mark.parametrize` for strategy enums and boundary values.
- Test edge cases first: empty DataFrame, all nulls, boundary values,
  all-duplicate input, empty source list.
- Coverage target: >80% enforced in CI via `--cov-fail-under=80`.
- Every public function needs at least: happy path, one edge case,
  to_dict() structure test.

---

## Current State of the Codebase

### Completed and committed — do not recreate

**`src/core/models.py`** — all data structures:
- `DType` (str, Enum): INTEGER, FLOAT, STRING, BOOLEAN, DATE
- `ColumnSchema` (dataclass): name, dtype, nullable, min_value,
  max_value, allowed_values — with __post_init__ guard
- `Schema` (dataclass): columns, allow_extra_cols, column_names property
- `Violation` (dataclass): column, rule, message, row_indices
- `ValidationReport` (dataclass): is_valid, violations, summary
  property, to_dict()
- `DuplicateStrategy` (str, Enum): DROP_FIRST, DROP_LAST, FLAG,
  MERGE_AGGREGATE
- `DedupReport` (dataclass): duplicates_found, rows_affected,
  strategy_used, to_dict()

**`src/core/validator.py`** — Case A, complete:
- `validate_against_schema(df, schema) -> ValidationReport`
- `_validate_column(series, col_schema) -> list[Violation]`
- Rules: missing columns, extra columns, nullable, dtype, min/max
  range, allowed values — all with row_indices

**`tests/core/test_validator.py`** — 17 tests, 94% coverage:
- score_schema and valid_df fixtures
- Happy path, missing column, extra column (rejected + allowed),
  nullable (fail + pass), parametrized range (5 values),
  parametrized allowed_values (5 values), row_indices verification,
  empty DataFrame, all nulls, to_dict structure

**CI:** `.github/workflows/ci.yml` — ruff → mypy → pytest on every
push and PR to main. Currently green.

**README.md:** Complete with CI badge, problem statement, features,
mermaid data flow diagram, architecture explanation, tech stack table,
installation, usage example, running tests, project status table,
license.

---

## What Needs to Be Built — In This Order

### Case B — Duplicate Handler

**`src/core/models.py`** — append only, do not touch existing code:
```python
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
    def to_dict(self: "DedupReport") -> dict[str, Any]: ...
```

**`src/core/cleaner.py`** — new file:
- `handle_duplicates(df, subset, strategy) -> DedupReport`
- DROP_FIRST/DROP_LAST: pd.DataFrame.drop_duplicates(keep=...)
- FLAG: add is_duplicate column, do not delete rows
- MERGE_AGGREGATE: groupby(subset).agg(first for str, sum for numeric)
- Always return DedupReport, never modify df in place

**`tests/core/test_cleaner.py`** — new file:
- Fixtures: df_with_dupes (5 rows, 2 pairs), df_no_dupes, df_all_dupes
- @pytest.mark.parametrize over all 4 strategies: returns DedupReport
- DROP_FIRST: correct rows_affected
- DROP_LAST: correct rows_affected
- FLAG: is_duplicate column exists, rows_affected = flagged count
- MERGE_AGGREGATE: numeric columns summed
- No duplicates → duplicates_found=0, rows_affected=0
- All rows identical → duplicates_found > 0
- Empty DataFrame → no error
- Multi-column subset → works correctly
- to_dict() structure: all keys present, strategy_used is str

Commit sequence:
feat(core): add DuplicateStrategy enum and DedupReport model
test(core): add failing tests for duplicate handler
feat(core): implement duplicate handler with configurable strategy
test(core): add parametrized duplicate handler tests

---

### Case C — Multi-Source Merger

**`src/core/models.py`** — append only:
```python
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
    def to_dict(self: "MergeReport") -> dict[str, Any]: ...
```

**`src/core/merger.py`** — new file:
- `merge_sources(sources, join_key, conflict_resolution) -> MergeReport`
- For each source: pd.read_excel → apply column_mapping → call
  validate_against_schema() → raise ValueError if invalid
- LAST_WINS: concat all → drop_duplicates(keep="last"), count conflicts
- FIRST_WINS: iterate sources, only add rows with new join_key values
- RAISE: concat → check duplicated(join_key) → raise ValueError with
  conflicting key list if any found
- Empty sources list → MergeReport with all zeros, no error

**`tests/core/test_merger.py`** — new file, use tmp_path fixture:
- Helper function make_excel(tmp_path, filename, data) -> Path
- Two compatible sources → rows_total = sum, conflicts_resolved = 0
- Column mapping applied → schema passes after rename
- LAST_WINS conflict → conflicts_resolved = 1, rows_total = 1
- FIRST_WINS conflict → first value kept
- RAISE on conflict → pytest.raises(ValueError, match="Conflict")
- Empty sources list → sources_loaded=0, rows_total=0
- Schema violation → pytest.raises(ValueError, match="failed validation")
- to_dict() structure

Commit sequence:
feat(core): add ExcelSource, ConflictStrategy and MergeReport models
test(core): add failing tests for multi-source merger
feat(core): implement multi-source merger with conflict resolution
test(core): add merger tests including conflict resolution edge cases

---

### Case D — Outlier Detection

**`src/core/models.py`** — append only:
```python
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
    def to_dict(self: "OutlierReport") -> dict[str, Any]: ...
```

**`src/core/analyzer.py`** — new file:
- `detect_outliers(df, columns, method, contamination=0.05) -> OutlierReport`
- Dispatch via match/case on OutlierMethod
- `_detect_isolation_forest(data, contamination)`: IsolationForest
  from sklearn, random_state=42, fillna(median) before fit
- `_detect_zscore(data, contamination)`: threshold=3.0, abs z-score,
  any column exceeds threshold → outlier
- `_detect_iqr(data)`: Q1/Q3/IQR per column, 1.5*IQR fence,
  any column outside fence → outlier
- All three return list[int] of row indices
- Empty DataFrame → return empty list, no error

When to use which method (must be in docstring AND README):
- IQR: fast, no sklearn, normally distributed data
- ZSCORE: classic, sensitive to extremes, easy to explain
- ISOLATION_FOREST: ML-based, high-dimensional data, no normality
  assumption required

**`tests/core/test_analyzer.py`** — new file:
- Fixtures: df_with_outliers (np.random.seed(42), 100 normal +
  [200.0, -100.0] at indices 100, 101), df_clean (100 normal only)
- @pytest.mark.parametrize over all 3 methods: returns OutlierReport
- @pytest.mark.parametrize over all 3 methods: index 100 or 101
  in outlier_indices (obvious outliers detected)
- Clean data: fewer than 5 outliers flagged (ZSCORE)
- columns_checked contains the specified column
- to_dict() structure
- Empty DataFrame → outlier_indices == []
- Multiple columns → all listed in columns_checked

Commit sequence:
feat(core): add OutlierMethod enum and OutlierReport model
test(core): add failing tests for outlier detection
feat(core): implement outlier detection with three methods
test(core): add outlier detection tests with synthetic data

---

### Pipeline Runner

**`src/core/pipeline.py`** — new file:
- Dataclasses: OutlierConfig, CleaningConfig, MergeConfig,
  PipelineConfig, PipelineReport
- `run_pipeline(config: PipelineConfig) -> PipelineReport`
- Stage 1: for each source → validate → deduplicate
- Stage 2: merge_sources() with config.merge settings
- Stage 3: if outliers.enabled → detect_outliers() on merged result
- PipelineReport: sources_loaded, rows_after_dedup, rows_after_merge,
  outliers_flagged, violations_found — with to_dict()
- No I/O in run_pipeline() itself. pd.read_excel only in merger.

**`docs/example_config.yaml`** — complete example showing all options.

**`tests/core/test_pipeline.py`** — new file, use tmp_path:
- Minimal pipeline with one source → returns PipelineReport
- violations_found increments when source has schema issues
- outliers_flagged > 0 when data has obvious outliers
- outliers disabled via config → outliers_flagged == 0

Commit sequence:
feat(core): add PipelineConfig and PipelineReport models
feat(core): implement pipeline runner orchestrating all cases
test(core): add pipeline integration tests

---

### CLI Adapter

**`src/cli/main.py`** — new file:
- argparse with --config Path argument
- Validate config file exists → sys.exit(1) with message if not
- Load YAML → build PipelineConfig → call run_pipeline()
- Print PipelineReport as formatted summary to stdout
- Print errors to stderr, sys.exit(1) on failure
- No logic. No data transformation. Only: parse → call → format.

Commit:
feat(cli): add CLI adapter for pipeline runner

---

### Final README Update

After all cases are complete, update README.md:
- Features section: list all 4 cases with bullet points
- Project Status table: all cases marked Done
- Usage section: add CLI example alongside Python API example
- Keep all other sections unchanged

Commit:
docs: update README with completed cases and CLI usage

---

## src/core/__init__.py — Public API Exports

Keep this file updated as cases are completed:
```python
from src.core.models import (
    ColumnSchema, DType, Schema, Violation, ValidationReport,
    DuplicateStrategy, DedupReport,
    ConflictStrategy, ExcelSource, MergeReport,
    OutlierMethod, OutlierReport,
)
from src.core.validator import validate_against_schema
from src.core.cleaner import handle_duplicates
from src.core.merger import merge_sources
from src.core.analyzer import detect_outliers
from src.core.pipeline import PipelineConfig, PipelineReport, run_pipeline

__all__ = [
    "ColumnSchema", "DType", "Schema", "Violation", "ValidationReport",
    "DuplicateStrategy", "DedupReport",
    "ConflictStrategy", "ExcelSource", "MergeReport",
    "OutlierMethod", "OutlierReport",
    "validate_against_schema",
    "handle_duplicates",
    "merge_sources",
    "detect_outliers",
    "PipelineConfig", "PipelineReport", "run_pipeline",
]
```

---

## Hard Rules — Never Violate

- Never recreate or overwrite completed files listed above
- Never skip the quality gate before committing
- Never use `# type: ignore` without a comment explaining why
- Never write a test that reads from a hardcoded file path
- Never put logic in src/cli/main.py
- Never put I/O in src/core/ except merger.py where it is unavoidable
- Never use bare `except:` or `except Exception:` without logging
- Never return a raw dict where a dataclass is expected
- Never use a magic string where an Enum exists