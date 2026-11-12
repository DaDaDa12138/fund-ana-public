"""Audit I/O failure must remain visible independently of business outcomes."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend import scheduler as scheduler_module
from backend.scheduler import ProjectScheduler
from backend.scheduler_diagnostics import (
    build_automation_autonomy_summary,
    build_scheduler_chain_audit,
    build_scheduler_status_payload,
    copy_task_artifact_fields,
    summarize_scheduler_run_for_api,
)
from backend.task_runtime import SqliteTaskRecorder
from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
from scripts.audit_data_truthfulness import _scheduler_task_audit_outcomes


TASK = "scheduled_snapshot_watchdog_refresh"


@pytest.fixture
def scheduler(tmp_path, monkeypatch):
    instance = ProjectScheduler.__new__(ProjectScheduler)
    instance.settings = SimpleNamespace(reports_dir=tmp_path / "reports")
    snapshots, logs = [], []
    instance.state_store = SimpleNamespace(
        finish_task=lambda name, result: snapshots.append(deepcopy(result)),
        stage_task_result=lambda name, result: snapshots.append(deepcopy(result)), set_jobs=Mock(),
    )
    instance.task_recorder = SqliteTaskRecorder(tmp_path / "tasks.db")
    instance._serialize_jobs = lambda: []
    instance._refresh_runtime_status_summary = lambda: None
    instance.logger = logs.append
    instance._log = logs.append
    monkeypatch.setattr(scheduler_module, "load_publication_registry", lambda settings: {})
    return instance, snapshots, logs


@pytest.mark.parametrize("failure", ["io_error", "no_artifact"])
def test_audit_failure_does_not_turn_skipped_business_work_into_fake_audit_success(scheduler, failure):
    instance, snapshots, logs = scheduler
    task = instance.task_recorder.start_task(TASK)
    def write(**kwargs):
        if failure == "io_error":
            raise OSError("audit output unavailable")
        return {}
    instance._write_task_run_audit = write
    result = instance._finalize_task(task_info=task, task_name=TASK, result={
        "status": "skipped", "skip_reason": "effective_nav_snapshot_already_screened",
        "started_at": task["started_at"],
    })
    assert result["status"] == "skipped"
    assert result["task_run_audit_status"] == "failed"
    assert result["task_run_audit_error"]
    assert any("[task-audit-failed]" in message for message in logs)
    assert snapshots[-1]["task_run_audit_error"] == result["task_run_audit_error"]
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        status, extra = conn.execute("SELECT status,extra_data FROM task_history WHERE id=?", (task["id"],)).fetchone()
    assert status == "skipped"
    assert json.loads(extra)["task_run_audit_status"] == "failed"


def test_sql_status_uses_final_publication_review_outcome(scheduler, tmp_path, monkeypatch):
    instance, snapshots, _ = scheduler
    task = instance.task_recorder.start_task(TASK)
    artifact = tmp_path / "audit.json"
    artifact.write_text('{}')
    instance._write_task_run_audit = lambda **kwargs: {"artifact_path": str(artifact)}
    monkeypatch.setattr(scheduler_module, "_build_publication_result_for_workflow_run", lambda *args: {"status": "blocked"})
    monkeypatch.setattr(scheduler_module, "_apply_publication_result_to_task_status",
                        lambda result, publication: result.update(status="degraded"))
    result = instance._finalize_task(task_info=task, task_name=TASK, result={
        "status": "success", "workflow_run_id": "synthetic_io_run", "started_at": task["started_at"],
    })
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        status, extra = conn.execute("SELECT status,extra_data FROM task_history WHERE id=?", (task["id"],)).fetchone()
        current_status = conn.execute("SELECT status FROM task_status WHERE task_name=?", (TASK,)).fetchone()[0]
    assert status == current_status == result["status"] == snapshots[-1]["status"] == "degraded"
    assert json.loads(extra)["status"] == "degraded"
    assert json.loads(extra)["task_run_audit_status"] == "written"
    final_audit = json.loads(artifact.read_text())
    assert final_audit["task"]["status"] == "degraded"
    assert final_audit["scheduler"]["task_run_summary"]["status"] == "degraded"
    assert final_audit["scheduler"]["task_closure"]["status"] == "degraded"
    assert final_audit["task_result"]["task_run_audit"]["status"] == "degraded"


def test_api_summary_preserves_legacy_failure_and_new_success_does_not_inherit_it():
    failed = {"status": "skipped", "task_run_audit_error": "real recorded I/O error"}
    summary = summarize_scheduler_run_for_api(failed)
    assert summary["task_run_audit_status"] == "failed"
    assert summary["task_run_audit_error"] == failed["task_run_audit_error"]
    fields = copy_task_artifact_fields({"task_run_audit_status": "written", "task_run_audit_error": None}, failed)
    assert fields["task_run_audit_status"] == "written"
    assert fields["task_run_audit_error"] is None
    assert "task_run_audit_status" not in copy_task_artifact_fields({"status": "success"})


def test_chain_and_lightweight_status_surface_audit_failure_even_when_nav_aligned():
    failed = {"status": "skipped", "skip_reason": "effective_nav_snapshot_already_screened",
              "task_run_audit_error": "recorded audit error"}
    snapshot = {"candidate_date": "2026-09-04", "signature": "same"}
    overview = {"snapshot_watchdog_run": failed, "effective_nav_snapshot": snapshot,
                "latest_screened_snapshot": snapshot}
    code = "snapshot_watchdog_task_audit_failed"
    assert code in {issue["code"] for issue in build_scheduler_chain_audit(overview)["open_issues"]}
    autonomy_inputs = []
    def autonomy(summary, chain):
        autonomy_inputs.append((summary, chain))
        return {}
    payload = build_scheduler_status_payload(
        {"last_runs": {TASK: failed}}, include_detail=False,
        build_scheduler_overview_summary=lambda state: {},
        build_scheduler_overview_api_summary_fn=lambda overview: overview,
        build_scheduler_chain_audit=build_scheduler_chain_audit,
        build_automation_autonomy_summary=autonomy,
    )
    assert payload["snapshot_watchdog_run"]["task_run_audit_status"] == "failed"
    assert code in {issue["code"] for issue in autonomy_inputs[0][1]["open_issues"]}


def test_truthfulness_reads_original_failure_without_changing_history(tmp_path):
    path = tmp_path / "scheduler.json"
    path.write_text(json.dumps({"last_runs": {TASK: {"status": "skipped", "task_run_audit_error": "I/O failed"}}}))
    before = path.read_bytes()
    result = _scheduler_task_audit_outcomes(SimpleNamespace(scheduler_state_file=path))
    assert result["warnings"] == [f"{TASK}:task_run_audit_failed"]
    assert result["tasks"][TASK]["business_status"] == "skipped"
    assert path.read_bytes() == before


def test_compacted_task_history_keeps_audit_outcome(tmp_path):
    result = {"status": "skipped", "task_run_audit_status": "failed",
              "task_run_audit_error": "recorded write error", "logs_tail": ["I/O fixture log"] * 200}
    compact = compact_task_result_for_history_if_needed(
        result, task_name=TASK, reports_dir=tmp_path / "reports", force=True, write_sidecar=False,
    )
    assert compact["task_history_extra_data_compacted"] is True
    assert compact["task_run_audit_status"] == "failed"
    assert compact["task_run_audit_error"] == result["task_run_audit_error"]


@pytest.mark.parametrize("source,large", [("daily", True), ("interval", False), ("interval", True)])
def test_finalize_keeps_real_automation_provenance_in_state_sql_and_audit(scheduler, tmp_path, source, large):
    instance, snapshots, _ = scheduler
    fixture = json.loads((Path(__file__).parent / "fixtures/scheduler_automation_provenance_20260908.json").read_text())
    provenance = fixture[source]["fields"]
    task = instance.task_recorder.start_task(TASK)
    artifact = tmp_path / "task_audit.json"

    def write(**kwargs):
        artifact.write_text(json.dumps({"task_result": kwargs["result"]}))
        return {"artifact_path": str(artifact)}

    instance._write_task_run_audit = write
    payload = {"status": "skipped", "started_at": task["started_at"], **deepcopy(provenance)}
    if large:
        payload["test_transport_padding"] = "x" * 300_000
    result = instance._finalize_task(task_info=task, task_name=TASK, result=payload)
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        extra = json.loads(conn.execute("SELECT extra_data FROM task_history WHERE id=?", (task["id"],)).fetchone()[0])
    original = json.loads(artifact.read_text())["task_result"]
    for record in [result, original, extra, *snapshots]:
        assert record["trigger_source"] == provenance["trigger_source"]
        assert record["archive_shadow_observation"] == provenance["archive_shadow_observation"]
    assert bool(extra.get("task_history_extra_data_compacted")) == large
    assert len(json.dumps(extra)) < 20_000
    assert fixture[source]["fields"] == provenance


def test_compaction_preserves_failed_observation_without_inventing_completion(tmp_path):
    observation = {"status": "failed", "error": "isolated I/O failure", "timed_out": True,
                   "consecutive_matched_windows": 0, "observation_requirement_met": False,
                   "production_reader_changed": False, "source_retirement_authorized": False}
    payload = {"status": "skipped", "trigger_source": "interval", "archive_shadow_observation": observation}
    compact = compact_task_result_for_history_if_needed(payload, task_name=TASK, force=True, write_sidecar=False)
    assert compact["archive_shadow_observation"] == observation
    compact["archive_shadow_observation"]["status"] = "changed_by_test"
    assert observation["status"] == "failed"
    legacy = compact_task_result_for_history_if_needed({"status": "success"}, task_name=TASK, force=True, write_sidecar=False)
    assert "trigger_source" not in legacy
    assert "archive_shadow_observation" not in legacy


@pytest.mark.parametrize("large", [False, True])
def test_storage_failure_is_preserved_in_sql_state_and_task_audit(scheduler, tmp_path, large):
    instance, snapshots, _ = scheduler
    task = instance.task_recorder.start_task(TASK)
    artifact = tmp_path / "storage_task_audit.json"

    def write(**kwargs):
        artifact.write_text(json.dumps({"task_result": kwargs["result"]}))
        return {"artifact_path": str(artifact)}

    instance._write_task_run_audit = write
    observation = {"status": "failed", "timed_out": True, "allocated_bytes": None,
                   "error": "isolated observation timeout", "observation_id": "storage_fixture"}
    payload = {"status": "degraded", "started_at": task["started_at"], "storage_observation": observation}
    if large:
        payload["test_transport_padding"] = "x" * 300_000
    result = instance._finalize_task(task_info=task, task_name=TASK, result=payload)
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        status, extra = conn.execute("SELECT status,extra_data FROM task_history WHERE id=?", (task["id"],)).fetchone()
    assert status == "degraded"
    for record in (result, json.loads(extra), json.loads(artifact.read_text())["task_result"], *snapshots):
        assert record["status"] == "degraded" and record["storage_observation"] == observation
    assert summarize_scheduler_run_for_api(result)["storage_observation"] == observation
    assert bool(json.loads(extra).get("task_history_extra_data_compacted")) == large


def test_autonomy_exposes_audit_warning_and_real_next_job_time():
    next_run = "2026-09-07T22:00:00+08:00"
    overview = {"snapshot_watchdog_run": {"status": "skipped", "task_run_audit_error": "recorded I/O error"},
                "jobs": [{"id": "snapshot_watchdog", "next_run_time": next_run}]}
    result = build_automation_autonomy_summary(
        overview, build_scheduler_chain_audit(overview), manual_operations_enabled=False,
        load_database_overview_fn=lambda payload: {},
    )
    code = "snapshot_watchdog_task_audit_failed"
    assert code in result["warning_issue_codes"]
    assert code not in result["resolved_warning_issue_codes"]
    item = next(item for item in result["warning_items"] if item["code"] == code)
    assert item["next_automatic_action_at"] == next_run
