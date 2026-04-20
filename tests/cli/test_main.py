from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.cli.main import main


def _write_excel(path: Path, data: dict[str, list[object]]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(data).to_excel(writer, index=False)


def _minimal_config(excel_path: Path) -> str:
    return f"""
sources:
  - path: {excel_path.as_posix()}
    schema:
      allow_extra_cols: true
      columns:
        - name: customer_id
          dtype: int
          nullable: false
        - name: revenue
          dtype: float
          nullable: false

merge:
  join_key: customer_id
  conflict_resolution: last_wins

cleaning:
  duplicate_strategy: drop_last
  duplicate_subset:
    - customer_id

outliers:
  enabled: false
"""


def test_cli_runs_pipeline_successfully(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    excel_path = tmp_path / "sales.xlsx"
    _write_excel(
        excel_path,
        {"customer_id": [1, 2, 3], "revenue": [10.5, 20.5, 30.5]},
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(_minimal_config(excel_path), encoding="utf-8")

    exit_code = main(["--config", str(config_path)])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Pipeline finished successfully." in captured.out
    assert "sources_loaded" in captured.out


def test_cli_missing_config_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "does_not_exist.yaml"

    exit_code = main(["--config", str(missing)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "not found" in captured.err


def test_cli_invalid_config_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("- just a list, not a mapping\n", encoding="utf-8")

    exit_code = main(["--config", str(bad_config)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Pipeline error" in captured.err


def test_cli_schema_violation_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    excel_path = tmp_path / "bad.xlsx"
    _write_excel(excel_path, {"wrong_col": [1], "revenue": [10.5]})
    config_path = tmp_path / "config.yaml"
    config_path.write_text(_minimal_config(excel_path), encoding="utf-8")

    exit_code = main(["--config", str(config_path)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Pipeline error" in captured.err
