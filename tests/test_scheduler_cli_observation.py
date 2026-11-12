"""The observer finishing must not turn a running scheduler task into a failure."""
from __future__ import annotations
from dataclasses import replace
import json
from pathlib import Path
import sys
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.config import load_settings
from scripts import run_scheduler as cli


@pytest.fixture
def settings(tmp_path):
    path = tmp_path / "scheduler.json"
    path.write_text(json.dumps({"manual_run_history": [{"request_id": "known", "job_id": "daily_refresh", "status": "running",
                                                       "requested_at": "2026-09-07T18:00:00"}]}))
    workflow = tmp_path / "workflow.json"
    workflow.write_text(json.dumps({"run_id": "unrelated_old_run", "status": "succeeded", "started_at": "2026-09-07T13:00:00"}))
    return replace(load_settings(), scheduler_state_file=path, state_file=workflow)


def test_query_is_read_only_and_does_not_attribute_old_workflow(settings):
    before = settings.scheduler_state_file.read_bytes()
    result = cli._query_existing_request(settings, "known")
    assert result["status"] == "running"
    assert result["task_terminal"] is False and result["new_task_submitted"] is False
    assert "workflow_run_id" not in result
    assert cli._result_exit_code(result) == 2
    assert settings.scheduler_state_file.read_bytes() == before


def test_unknown_request_does_not_resubmit(settings):
    result = cli._query_existing_request(settings, "missing")
    assert result["status"] == "not_found"
    assert result["task_terminal"] is None and result["new_task_submitted"] is False
    assert cli._result_exit_code(result) == 1


def test_query_cli_bypasses_schema_migration_and_scheduler_start(settings, monkeypatch, capsys):
    from backend import config, schema_bootstrap, scheduler
    monkeypatch.setattr(config, "load_settings", lambda *args: settings)
    monkeypatch.setattr(schema_bootstrap, "ensure_database_schema", lambda *args: pytest.fail("query attempted migration"))
    monkeypatch.setattr(scheduler, "ProjectScheduler", lambda *args, **kwargs: pytest.fail("query started a scheduler"))
    monkeypatch.setattr(sys, "argv", ["run_scheduler.py", "--request-id", "known"])
    assert cli.main() == 2
    assert json.loads(capsys.readouterr().out)["request_id"] == "known"


def test_terminal_query_is_terminal_and_forbidden_is_not_success(settings):
    payload = json.loads(settings.scheduler_state_file.read_text())
    payload["manual_run_history"][0]["status"] = "failed"
    payload["manual_run_history"][0]["error"] = "real fixture failure"
    settings.scheduler_state_file.write_text(json.dumps(payload))
    result = cli._query_existing_request(settings, "known")
    assert result["task_terminal"] is True and result["background_continues"] is False
    assert cli._result_exit_code(result) == 1
    assert cli._result_exit_code({"status": "forbidden"}) == 1


@pytest.mark.parametrize("workflow_start", ["2026-09-07T13:00:00", "2026-09-07T22:00:00"])
def test_terminal_query_does_not_attach_an_unrelated_workflow(settings, workflow_start):
    payload = json.loads(settings.scheduler_state_file.read_text())
    payload["manual_run_history"][0]["status"] = "success"
    settings.scheduler_state_file.write_text(json.dumps(payload))
    settings.state_file.write_text(json.dumps({"run_id": "unrelated", "status": "succeeded", "started_at": workflow_start}))
    result = cli._query_existing_request(settings, "known")
    assert result["status"] == "success" and result["task_terminal"] is True
    assert "workflow_run_id" not in result and "publication_result" not in result


def test_unknown_state_and_cancelled_request_are_not_success(settings):
    payload = json.loads(settings.scheduler_state_file.read_text())
    payload["manual_run_history"][0]["status"] = "new_unknown_status"
    settings.scheduler_state_file.write_text(json.dumps(payload))
    result = cli._query_existing_request(settings, "known")
    assert result["task_terminal"] is None and result["background_continues"] is None
    assert cli._result_exit_code(result) == 1
    assert cli._result_exit_code({"status": "cancelled"}) == 1


def test_explicit_nested_workflow_id_survives_a_later_workflow(settings, monkeypatch):
    payload = json.loads(settings.scheduler_state_file.read_text())
    payload["manual_run_history"][0].update(status="success", result={"workflow_run_id": "actual_run"})
    settings.scheduler_state_file.write_text(json.dumps(payload))
    monkeypatch.setattr(cli, "_build_publication_result", lambda _, run_id: {"status": "published", "run_id": run_id})
    result = cli._query_existing_request(settings, "known")
    assert result["workflow_run_id"] == "actual_run"
    assert result["publication_result"]["run_id"] == "actual_run"


def test_screening_utils_import_order_does_not_eagerly_reenter_database_connection():
    command = """
import sys
from pathlib import Path
root = Path.cwd()
sys.path.insert(0, str(root / 'core'))
sys.path.insert(0, str(root / 'core/bond_fund_screening'))
import backend.config
import backend.forward_validation
import core.utils.core_csv_storage
import core.nav_window
assert Path(core.utils.core_csv_storage.__file__).resolve() == root / "core/utils/core_csv_storage.py"
assert Path(core.nav_window.__file__).resolve() == root / "core/bond_fund_screening/core/nav_window.py"
from utils import chunks
assert list(chunks([1, 2, 3], 2)) == [[1, 2], [3]]
"""
    result = subprocess.run([sys.executable, "-c", command], cwd=ROOT, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
