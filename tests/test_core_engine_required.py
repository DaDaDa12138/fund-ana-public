from __future__ import annotations

from pathlib import Path
import sqlite3
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend import standalone_runner
from backend.core_runner import CoreStepFailedError


def _complete_core(root: Path) -> Path:
    (root / "bond_fund_screening" / "tasks").mkdir(parents=True)
    (root / "main.py").touch()
    (root / "bond_fund_screening" / "tasks" / "daily_screening_master.py").touch()
    return root


@pytest.mark.parametrize(
    "layout", ["unset", "absent", "file", "empty", "missing_main", "missing_screening", "entry_directory"]
)
def test_incomplete_core_cannot_start_collection_or_emit_snapshot_exports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, layout: str,
) -> None:
    core_root = tmp_path / "core"
    if layout == "unset":
        core_root = None
    elif layout == "file":
        core_root.touch()
    elif layout == "empty":
        core_root.mkdir()
    elif layout in {"missing_main", "missing_screening", "entry_directory"}:
        _complete_core(core_root)
        entry = core_root / (
            "bond_fund_screening/tasks/daily_screening_master.py"
            if layout == "missing_screening" else "main.py"
        )
        entry.unlink()
        if layout == "entry_directory":
            entry.mkdir()

    core = Mock(side_effect=AssertionError("incomplete core was started"))
    database = Mock(side_effect=AssertionError("snapshot database was opened"))
    child = Mock(side_effect=AssertionError("fallback subprocess was started"))
    monkeypatch.setattr(standalone_runner, "run_core_pipeline", core)
    monkeypatch.setattr(sqlite3, "connect", database)
    monkeypatch.setattr(subprocess, "Popen", child)
    output = tmp_path / "exports"
    settings = SimpleNamespace(core_root=core_root, core_output_dir=output)

    with pytest.raises(FileNotFoundError, match="core_engine_unavailable"):
        standalone_runner.run_project_pipeline(settings, {}, lambda line: None)

    core.assert_not_called()
    database.assert_not_called()
    child.assert_not_called()
    assert not output.exists()


@pytest.mark.parametrize("options", [
    {"run_collection": True, "run_screening": True},
    {"run_collection": True, "run_screening": False},
    {"run_collection": False, "run_screening": True},
    {"run_collection": False, "run_screening": False},
    {"reuse_existing_outputs": True},
    {"skip_risk_calculation": True},
])
def test_pipeline_options_cannot_bypass_required_core(
    tmp_path: Path, options: dict,
) -> None:
    settings = SimpleNamespace(core_root=tmp_path / "absent", core_output_dir=tmp_path / "exports")
    with pytest.raises(FileNotFoundError, match="core_engine_unavailable"):
        standalone_runner.run_project_pipeline(settings, options, lambda line: None)
    assert not settings.core_output_dir.exists()


@pytest.mark.parametrize("options", [
    {},
    {"run_collection": True, "run_screening": True},
    {"run_collection": False, "run_screening": False},
    {"reuse_existing_outputs": True, "workflow_run_id": "contract-run"},
])
def test_complete_core_preserves_options_and_missing_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, options: dict,
) -> None:
    settings = SimpleNamespace(core_root=_complete_core(tmp_path / "core"))
    original_options = dict(options)
    artifacts = {"core_output_dir": str(tmp_path / "output"), "long_csv": None, "short_csv": None}
    core = Mock(return_value=artifacts)
    monkeypatch.setattr(standalone_runner, "run_core_pipeline", core)
    log = Mock()

    result = standalone_runner.run_project_pipeline(settings, options, log)

    assert result is artifacts
    assert result["long_csv"] is None
    assert result["short_csv"] is None
    assert options == original_options
    core.assert_called_once_with(settings, options, log)
    assert core.call_args.args[1] is options


@pytest.mark.parametrize("error", [
    FileNotFoundError("core input unavailable"),
    CoreStepFailedError(stage="risk", return_code=2, meta_path="private-meta.json"),
])
def test_complete_core_failure_is_not_replaced_by_snapshot_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception,
) -> None:
    settings = SimpleNamespace(core_root=_complete_core(tmp_path / "core"))
    core = Mock(side_effect=error)
    monkeypatch.setattr(standalone_runner, "run_core_pipeline", core)

    with pytest.raises(type(error)) as caught:
        standalone_runner.run_project_pipeline(settings, {}, lambda line: None)

    assert caught.value is error
    core.assert_called_once()
