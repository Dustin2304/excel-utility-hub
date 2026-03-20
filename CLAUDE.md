# Excel Utility Hub – Claude Code Context

## Project Overview

Python toolkit for Excel data pipelines. Validates DataFrames against
defined schemas, handles duplicates, merges multiple Excel sources, and
detects outliers via ML. Designed as a portfolio project demonstrating
software architecture, testing discipline, and pragmatic ML integration.

---

## Architecture Rules

- `src/core/` contains pure logic only. No I/O, no side effects, no
  print statements. Every function is testable without touching the
  filesystem.
- `src/cli/` is the only external adapter. It reads arguments, calls
  core/, and formats output. Nothing else.
- `src/dashboard/` is a future adapter. Do not create it until core/
  is complete.
- Test data is always created as pd.DataFrame inline in the test.
  Never read from external files in tests.
- No hardcoded paths anywhere. All configuration flows through
  PipelineConfig.

---

## Tech Stack

- Python 3.11
- pandas >= 2.0
- scikit-learn >= 1.3
- pytest + pytest-cov
- ruff (linting)
- mypy strict (type checking)
- GitHub Actions (CI)
- Docker

---

## Code Conventions

- Full type hints on every function and method, including return types
- Dataclasses for all data structures — never return raw dicts
- Enums instead of magic strings
- `field(default_factory=list)` for mutable dataclass defaults
- Private helper functions prefixed with `_`
- No function longer than 40 lines — split if needed
- Conventional Commits: feat/fix/test/docs/chore/refactor + scope

---

## Hard Rules — Never Violate

- No `except: pass` or bare `except Exception` without logging
- No `Any` as return type except in `to_dict()` methods
- No unused imports
- No code that only works locally (absolute paths, hardcoded usernames)
- Never suppress mypy errors with `# type: ignore` without a comment
  explaining why

---

## Testing Philosophy

- Write failing tests before implementing (TDD)
- `@pytest.fixture` for reusable DataFrames and schemas
- `@pytest.mark.parametrize` for range checks and enum coverage
- Test edge cases first: empty DataFrame, all nulls, boundary values
- Coverage target: >80% enforced in CI via `--cov-fail-under=80`

---

## Before Every Commit
```bash
ruff check src/ tests/
mypy src/
pytest tests/ --cov=src --cov-report=term-missing
```

All three must pass clean. No commit with a red pipeline.

---

## Commit Reference
```
feat(core): <what was added>
fix(core): <what was corrected>
test(core): <what tests were added>
refactor(core): <what was restructured>
chore(ci): <tooling or config change>
docs: <documentation update>
```