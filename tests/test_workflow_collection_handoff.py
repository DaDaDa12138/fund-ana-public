"""Exercise the real CLI and pipe reader with frozen collection terminal events."""
from __future__ import annotations

import gzip
import json
from pathlib import Path
import select
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend import scheduler as scheduler_module
from backend.collection_log_summary import classify_collection_log_severity
from backend.workflow import WorkflowManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = json.loads(gzip.decompress(
    (PROJECT_ROOT / "tests/fixtures/nested_period_collection_20260912.json.gz").read_bytes()
))
PHASES = [item["source_evidence"] for item in FIXTURE["phases"]]

# Replace external producers and publication only. The actual CLI argument
# parser, WorkflowManager, state lock/log, stdout, and scheduler reader run.
CHILD = r'''
import json
from pathlib import Path
import runpy
import sys
import time
from types import SimpleNamespace

root, evidence_path, state_path, behavior, script, *flags = sys.argv[1:]
sys.path.insert(0, root)
sys.path.insert(0, str(Path(root) / "core"))
from backend import workflow

settings = SimpleNamespace(state_file=Path(state_path),
    tiantian_base_url="https://disabled.invalid/tiantian",
    autostock_base_url="https://disabled.invalid/autostock")
workflow.load_settings = lambda: settings
workflow.ensure_database_schema = lambda settings: None
workflow.load_latest_canonical_report_payload = lambda settings: None
workflow.load_publication_registry = lambda settings: {}
evidence = json.loads(Path(evidence_path).read_text())

def pipeline(settings, options, log):
    log("isolated pipeline started")
    if evidence is not None:
        log("api_source_evidence=" + json.dumps(evidence, ensure_ascii=False))
    if behavior == "barrier":
        release = Path(state_path).with_suffix(".release")
        deadline = time.monotonic() + 10
        while not release.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("isolated event was not flushed before report")
            time.sleep(0.01)
    for number in range(410):
        log(f"isolated bounded-state progress {number}")
    if behavior == "failure":
        raise RuntimeError("isolated failure after collection")
    return {}

def report(**kwargs):
    if behavior == "tail":
        for number in range(250):
            print(f"isolated downstream stdout {number}", flush=True)
    return {"run_id": kwargs["run_id"], "counts": {"final_count": None},
            "artifacts": {"report_markdown": "isolated/no-business-report.md"}}

workflow.run_project_pipeline = pipeline
workflow.build_report_payload = report
workflow.build_lightweight_report_payload = report
workflow.WorkflowManager._publish_formal_report = lambda self, **kwargs: ({}, None)
workflow.WorkflowManager._run_annual_return_gate_shadow_audit = lambda *args, **kwargs: None
workflow.WorkflowManager._run_frozen_baseline_audit = lambda *args, **kwargs: None
sys.argv = [script, *flags]
runpy.run_path(script, run_name="__main__")
'''


def child_command(tmp_path: Path, evidence: dict | None, *, behavior: str, flags: list[str]) -> list[str]:
    harness = tmp_path / "workflow_child.py"
    harness.write_text(CHILD)
    evidence_path = tmp_path / "terminal.json"
    evidence_path.write_text(json.dumps(evidence, ensure_ascii=False))
    return [sys.executable, str(harness), str(PROJECT_ROOT), str(evidence_path),
            str(tmp_path / "workflow_state.json"), behavior,
            str(PROJECT_ROOT / "scripts/run_bond_workflow.py"), *flags]


@pytest.mark.parametrize("compact", [False, True])
def test_cli_without_stream_flag_preserves_json_only_output(tmp_path: Path, compact: bool) -> None:
    command = child_command(tmp_path, PHASES[1], behavior="normal",
                            flags=["--compact-json"] if compact else [])
    result = subprocess.run(command, capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["counts"]["final_count"] is None
    assert "api_source_evidence=" not in result.stdout
    if compact:
        assert len(result.stdout.splitlines()) == 1


def test_cli_flushes_real_terminal_before_report_and_state_log_eviction(tmp_path: Path) -> None:
    command = child_command(tmp_path, PHASES[1], behavior="barrier",
                            flags=["--compact-json", "--stream-collection-evidence"])
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout is not None
        assert select.select([process.stdout], [], [], 8)[0], "terminal evidence was buffered"
        line = process.stdout.readline().rstrip()
        assert line.startswith("api_source_evidence=")
        assert json.loads(line.split("=", 1)[1]) == PHASES[1]
        assert process.poll() is None  # Report is still waiting at the barrier.
        (tmp_path / "workflow_state.release").touch()
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stderr
        assert json.loads(stdout)["counts"]["final_count"] is None
        state = json.loads((tmp_path / "workflow_state.json").read_text())
        assert state["status"] == "succeeded"
        assert all("api_source_evidence=" not in item for item in state["logs"])
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()


def test_failed_child_still_hands_off_terminal_without_printing_success_report(tmp_path: Path) -> None:
    command = child_command(tmp_path, PHASES[1], behavior="failure",
                            flags=["--compact-json", "--stream-collection-evidence"])
    result = subprocess.run(command, capture_output=True, text=True, timeout=25)
    assert result.returncode != 0
    assert "isolated failure after collection" in result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0].split("=", 1)[1]) == PHASES[1]
    state = json.loads((tmp_path / "workflow_state.json").read_text())
    assert state["status"] == "failed" and state["current_run_report"] is None


@pytest.mark.parametrize("initial,nested,expected_status", [
    (0, 1, "degraded"),
    (0, 0, "success"),
    (0, None, "success"),
    (None, None, "success"),
])
def test_scheduler_receives_last_real_phase_over_cli_pipe_after_log_tail_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    initial: int | None, nested: int | None, expected_status: str,
) -> None:
    scheduler = scheduler_module.ProjectScheduler.__new__(scheduler_module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        project_root=PROJECT_ROOT, database_path=tmp_path / "unused.db",
        latest_report_file=tmp_path / "latest.json", reports_dir=tmp_path / "reports",
        effective_nav_coverage_ratio=0.9, effective_nav_weekday_only=True,
        run_collection_by_default=True, run_screening_by_default=True,
    )
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    full_log: list[str] = []
    scheduler._log = full_log.append
    scheduler._build_host_runtime_snapshot = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._build_data_quality_audits_compat = lambda **kwargs: {}
    scheduler._daily_refresh_lineage_rescreen_required = lambda **kwargs: {"required": False}
    scheduler._build_maintenance_data_change_summary = lambda **kwargs: {"changed": False}
    snapshot = SimpleNamespace(candidate_date="2026-09-11", signature="isolated",
                               to_dict=lambda: {"signature": "isolated"})
    monkeypatch.setattr(scheduler_module, "load_effective_nav_snapshot", lambda *args, **kwargs: snapshot)
    monkeypatch.setattr(scheduler_module, "load_latest_screened_db_snapshot", lambda *args: {})
    monkeypatch.setattr(scheduler_module, "screening_snapshot_changed", lambda *args: (True, "isolated_check"))
    monkeypatch.setattr(scheduler_module, "run_autostock_nav_tail_fill", Mock(return_value={}))

    def initial_collection(settings, options, log):
        if initial is not None:
            log("api_source_evidence=" + json.dumps(PHASES[initial]))
        return {}

    monkeypatch.setattr(scheduler_module, "run_project_pipeline", initial_collection)
    original_popen = subprocess.Popen
    commands: list[list[str]] = []

    def run_actual_cli(cmd, **kwargs):
        commands.append(list(cmd))
        command = child_command(tmp_path, PHASES[nested] if nested is not None else None,
                                behavior="tail", flags=cmd[2:])
        return original_popen(command, **kwargs)

    monkeypatch.setattr(scheduler_module.subprocess, "Popen", run_actual_cli)
    result = scheduler._execute_scheduled_refresh(
        task_name="isolated_nested_collection", refresh_window="daily",
        nav_history_sync_enabled=False, nav_history_sync_limit=1,
        long_term_history_enabled=False, long_term_history_limit=1,
    )
    assert len(commands) == 1 and "--stream-collection-evidence" in commands[0]
    assert result["status"] == expected_status
    assert result["screening_triggered"] is True
    assert all("api_source_evidence=" not in line for line in result["logs_tail"])
    expected = PHASES[nested] if nested is not None else PHASES[initial] if initial is not None else None
    assert result.get("period_return_collection") == expected
    if nested == 1:
        assert result["degraded"] is True
        assert result["warning_reason"] == "period_return_date_lag_quarantined"
        assert result["period_return_collection"]["pending_count"] == 226
        assert result["period_return_collection"]["quarantined_date_lag_fcodes"] == ["016314"]
    assert sum("api_source_evidence=" in line for line in full_log) == int(initial is not None) + int(nested is not None)


def test_forwarding_failure_preserves_original_event_and_is_not_silent(tmp_path: Path) -> None:
    settings = SimpleNamespace(state_file=tmp_path / "state.json")
    callback = Mock(side_effect=BrokenPipeError("isolated closed scheduler pipe"))
    manager = WorkflowManager(settings, collection_event_callback=callback)
    line = "api_source_evidence=" + json.dumps(PHASES[1])
    try:
        manager.store.start("isolated", {})
        with pytest.raises(BrokenPipeError, match="closed scheduler pipe"):
            manager._append_pipeline_log(line)
        assert manager.store.snapshot()["logs"][-1] == line
        assert classify_collection_log_severity([line])["degraded"] is True
    finally:
        manager.store.close()
