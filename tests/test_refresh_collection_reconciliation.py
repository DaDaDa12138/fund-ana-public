"""Reconcile actual frozen collection bytes; mutations below are isolated faults."""
from __future__ import annotations

import base64
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

import pytest

from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
from scripts import audit_data_truthfulness as audit

FIXTURE_PATH = Path(__file__).parent / "fixtures/refresh_collection_phases_20260912.json.gz"
FIXTURE_SHA = "e8c15d9dbd35d26d4040ab8c2cc080ec250bf9a34a5f617adf594c79044996f5"
COLUMNS = ("id", "task_name", "status", "started_at", "completed_at", "duration",
           "records_processed", "records_failed", "error_message", "extra_data")


@pytest.fixture
def recorded_task(tmp_path):
    raw = FIXTURE_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == FIXTURE_SHA
    fixture = json.loads(gzip.decompress(raw))
    for name, record in fixture["files"].items():
        content = base64.b64decode(record["base64"])
        assert len(content) == record["length"] and hashlib.sha256(content).hexdigest() == record["sha256"]
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    db = tmp_path / "task_fixture.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE task_history (id INTEGER PRIMARY KEY, task_name TEXT,
            status TEXT, started_at TEXT, completed_at TEXT, duration INTEGER,
            records_processed INTEGER, records_failed INTEGER, error_message TEXT, extra_data TEXT)""")
    row = deepcopy(fixture["task_row"])
    extra = json.loads(row["extra_data"])
    resolve = lambda value: tmp_path / str(value).removeprefix("/app/")
    task_audit_path = resolve(extra["task_run_audit_path"])
    task_audit = json.loads(gzip.decompress(task_audit_path.read_bytes()))
    run_id = extra["screening_run_id"]
    index_path = tmp_path / "reports/runs" / run_id / "logs/subprocess/subprocess_index.json"
    index = json.loads(index_path.read_bytes())
    period_item = next(item for item in index["items"] if item["task_name"] == "collect-period-returns")
    stdout = resolve(period_item["stdout_path_relative"])
    summary = json.loads(stdout.read_text().split("api_source_evidence=", 1)[1].splitlines()[0])
    settings = SimpleNamespace(project_root=tmp_path, database_path=db, reports_dir=tmp_path / "reports",
        scheduler_state_file=tmp_path / "backend/data/scheduler_state.json")
    state = json.loads(settings.scheduler_state_file.read_bytes())

    def save(*, full_audit=True):
        row["extra_data"] = json.dumps(extra, ensure_ascii=False)
        with sqlite3.connect(db) as conn:
            conn.execute("INSERT OR REPLACE INTO task_history VALUES (?,?,?,?,?,?,?,?,?,?)",
                         tuple(row.get(key) for key in COLUMNS))
        if full_audit:
            task_audit_path.write_bytes(gzip.compress(json.dumps(task_audit, ensure_ascii=False).encode(), mtime=0))
            settings.scheduler_state_file.write_text(json.dumps(state, ensure_ascii=False))

    # Preserve the actual task-audit bytes in the original-record case.
    save(full_audit=False)
    return SimpleNamespace(settings=settings, row=row, extra=extra, task_audit=task_audit,
        task_audit_path=task_audit_path, index=index, index_path=index_path, period_item=period_item,
        summary=summary, state=state, task_log=resolve(extra["task_log_path"]),
        save=save, resolve=resolve, run_id=run_id)


def check(task):
    return audit._find_scheduled_refresh_task_truth(task.settings, run_id=task.run_id,
                                                   final_count=task.extra["screening_final_count"])


def save_index(task):
    index = task.index
    failed = [item["task_name"] for item in index["items"] if item["success"] is not True]
    index.update(subprocess_count=len(index["items"]), success_count=len(index["items"]) - len(failed),
                 failed_count=len(failed), failed_task_names=failed)
    task.index_path.write_text(json.dumps(index, ensure_ascii=False))


@pytest.fixture
def forwarded_task(recorded_task):
    """Model successful delivery of the same second event, with no new fund data."""
    task = recorded_task
    fields = {"status": "degraded", "degraded": True, "degraded_stage": "collect-period-returns",
              "warning_reason": "period_return_date_lag_quarantined"}
    task.row["status"] = fields["status"]
    task.task_audit["task"].update(status=fields["status"])
    task.task_audit["task_result"].update(**fields, period_return_collection=deepcopy(task.summary))
    for value in task.task_audit["scheduler"].values():
        if isinstance(value, dict) and value.get("task_name") == task.row["task_name"]:
            value.update(fields)
    compact = compact_task_result_for_history_if_needed(deepcopy(task.task_audit["task_result"]),
        task_name=task.row["task_name"], task_info={"id": task.row["id"]}, force=True, write_sidecar=False)
    task.extra.clear()
    task.extra.update(compact)
    for value in (task.state["last_runs"][task.row["task_name"]], task.state["task_closures"]["daily_refresh"]):
        value.update(fields)
        if "task_history_full_payload" in value:
            value["task_history_full_payload"] = deepcopy(compact["task_history_full_payload"])
    with task.task_log.open("a") as stream:
        stream.write("\napi_source_evidence=" + json.dumps(task.summary, ensure_ascii=False) + "\n")
    task.save()
    return task


def test_original_success_and_stale_pending_are_reported_without_rewriting(recorded_task):
    task = recorded_task
    protected = {path: hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in (task.task_audit_path, task.task_log, task.settings.scheduler_state_file)}
    before = task.row["extra_data"]
    result, blockers, _ = check(task)
    assert json.loads(json.dumps(result))["row"]["id"] == task.row["id"]
    reconciliation = result["collection_reconciliation"]
    assert reconciliation["source_evidence_verified"] is True
    assert reconciliation["source_phase_count"] == reconciliation["current_task_phase_count"] == 2
    assert [phase["pending_count"] for phase in reconciliation["phases"]] == [425, 226]
    assert reconciliation["recorded"]["status"] == result["row"]["status"] == "success"
    assert reconciliation["derived"]["status"] == "degraded"
    assert reconciliation["derived"]["pending_count"] == 226
    assert reconciliation["derived"]["blocking_count"] == 0
    assert reconciliation["parent_payload_binding"]["sql_full_payload_verified"] is True
    assert reconciliation["parent_payload_binding"]["verified"] is True  # Empty legacy stage vs compact null.
    assert reconciliation["parent_log_phase_count"] == 0
    assert reconciliation["parent_log_bound_phase_count"] == 1
    assert reconciliation["parent_matches_derived"] is False
    assert "scheduled_refresh_collection_parent_log_phase_mismatch" in blockers
    assert any("parent_mismatch:task_result:period_return_collection" in item for item in blockers)
    assert any("parent_mismatch:state.last_runs." in item for item in blockers)
    assert result["collection_degradation"] is None
    with sqlite3.connect(task.settings.database_path) as conn:
        assert conn.execute("SELECT extra_data FROM task_history WHERE id = ?", (task.row["id"],)).fetchone()[0] == before
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest for path, digest in protected.items())


def test_archived_parent_log_preserves_original_reconciliation_and_source_sha(recorded_task, monkeypatch):
    from backend import task_log_compression as compression
    from backend.task_audit_storage import read_original_artifact_bytes

    task = recorded_task
    original = task.task_log.read_bytes()
    before = check(task)
    # Age is simulated only in this isolated fixture. Recorded source bytes,
    # task statuses and business values remain exactly as originally captured.
    monkeypatch.setattr(compression, "time", SimpleNamespace(
        time=lambda: time.time() + 90 * 86400, monotonic=time.monotonic))
    plan = compression.plan_task_log_compression(task.settings.project_root)
    assert plan["candidate_count"] == 1, plan
    result = compression.compress_task_log(task.settings.project_root, plan["candidates"][0])
    assert result["status"] == "compressed", result
    after = check(task)
    assert after == before
    assert after[0]["collection_reconciliation"]["parent_log_complete_sha256"] == hashlib.sha256(original).hexdigest()
    assert read_original_artifact_bytes(task.task_log) == original


def test_healthy_first_phase_and_truthful_degraded_second_phase_pass(forwarded_task):
    result, blockers, warnings = check(forwarded_task)
    assert json.loads(json.dumps(result))["collection_reconciliation"]["derived"]["pending_count"] == 226
    assert not blockers
    reconciliation = result["collection_reconciliation"]
    assert reconciliation["parent_matches_derived"] is True
    assert reconciliation["parent_log_phase_count"] == 1
    assert reconciliation["parent_log_bound_phase_count"] == 2
    assert result["row"]["status"] == "degraded"
    assert result["collection_degradation"]["pending_count"] == 226
    assert result["collection_degradation"]["failed_count"] == 1
    assert "scheduled_refresh_collection_degraded:period_return_date_lag_quarantined" in warnings


@pytest.mark.parametrize("damage", [
    "response_bytes", "stdout_truncated", "meta_missing", "meta_status", "meta_path", "index_missing",
    "missing_planned_phase", "failed_child", "timeout", "timeout_reason", "run_id", "duplicate_phase",
    "time_before_task", "time_after_task", "index_zero_count", "index_false_plan",
])
def test_incomplete_or_conflicting_sources_cannot_explain_a_degraded_parent(forwarded_task, damage):
    task = forwarded_task
    item = task.period_item
    meta_path = task.resolve(item["meta_path_relative"])
    meta = json.loads(meta_path.read_bytes())
    if damage == "response_bytes":
        task.resolve(task.summary["response_artifact"]).write_bytes(b"isolated corrupted source")
    elif damage == "stdout_truncated":
        task.resolve(item["stdout_path_relative"]).write_text("isolated incomplete child log\n")
    elif damage == "meta_missing":
        meta_path.unlink()
    elif damage == "meta_status":
        meta["success"] = False
        meta_path.write_text(json.dumps(meta))
    elif damage == "meta_path":
        meta["stdout_path"] = item["stderr_path"]
        meta_path.write_text(json.dumps(meta))
    elif damage == "index_missing":
        task.index_path.unlink()
    elif damage == "missing_planned_phase":
        task.index["items"].remove(item)
        save_index(task)
    elif damage in {"failed_child", "timeout", "timeout_reason"}:
        fields = {"success": False, "return_code": 1} if damage == "failed_child" else (
            {"success": False, "return_code": -9, "timed_out": True, "timeout_reason": "wall_timeout"}
            if damage == "timeout" else {"timeout_reason": "wall_timeout"})
        item.update(fields)
        meta.update(fields)
        meta_path.write_text(json.dumps(meta))
        save_index(task)
    elif damage == "run_id":
        task.index["run_id"] = "unrelated-run"
        save_index(task)
    elif damage == "duplicate_phase":
        task.index["items"].append(deepcopy(item))
        save_index(task)
    elif damage in {"time_before_task", "time_after_task"}:
        stamp = "2026-09-11T08:08:41" if damage == "time_before_task" else "2026-09-13T08:08:41"
        item["started_at"] = meta["started_at"] = stamp
        meta_path.write_text(json.dumps(meta))
        save_index(task)
    elif damage == "index_zero_count":
        task.index["subprocess_count"] = 0
        task.index_path.write_text(json.dumps(task.index))
    elif damage == "index_false_plan":
        task.index["period_return_collection_planned"] = False
        task.index_path.write_text(json.dumps(task.index))
    result, blockers, _ = check(task)
    assert blockers, damage
    assert result["collection_degradation"] is None
    assert result["collection_reconciliation"]["derived"] is None


@pytest.mark.parametrize("damage", ["sha", "length", "path", "task_id", "result_status", "sql_flag", "latest_pending", "state", "state_sha"])
def test_recorded_views_and_compact_identity_are_independently_checked(forwarded_task, damage):
    task = forwarded_task
    if damage == "sha":
        task.extra["task_history_full_payload"]["full_payload_sha256"] = "0" * 64
    elif damage == "length":
        task.extra["task_history_full_payload"]["full_payload_bytes"] += 1
    elif damage == "path":
        task.extra["task_history_full_payload"]["sidecar_project_path"] = task.summary["response_artifact"]
    elif damage == "task_id":
        task.task_audit["task"]["task_history_id"] += 1
    elif damage == "result_status":
        task.task_audit["task_result"]["status"] = "success"
    elif damage == "sql_flag":
        task.extra.pop("task_history_extra_data_compacted")
    elif damage == "latest_pending":
        task.task_audit["task_result"]["period_return_collection"]["pending_count"] = 425
    elif damage == "state":
        task.state["last_runs"][task.row["task_name"]].update(status="success", degraded=False)
    elif damage == "state_sha":
        task.state["last_runs"][task.row["task_name"]]["task_history_full_payload"]["full_payload_sha256"] = "0" * 64
    task.save()
    result, blockers, _ = check(task)
    assert blockers and result["collection_degradation"] is None
    assert result["collection_reconciliation"]["derived"]["pending_count"] == 226
    assert result["collection_reconciliation"]["parent_matches_derived"] is False


def test_full_log_scan_does_not_lose_initial_index_and_events_outside_ui_tail(forwarded_task):
    with forwarded_task.task_log.open("a") as stream:
        stream.write("isolated later diagnostic line\n" * 180_000)
    result, blockers, _ = check(forwarded_task)
    assert not blockers
    assert result["collection_reconciliation"]["parent_log_bound_phase_count"] == 2


def test_physically_truncated_parent_log_remains_unproven(forwarded_task):
    forwarded_task.task_log.write_text("isolated missing original collection index\n")
    result, blockers, _ = check(forwarded_task)
    assert "scheduled_refresh_collection_subprocess_index_missing" in blockers
    assert result["collection_reconciliation"]["source_evidence_verified"] is False
    assert result["collection_reconciliation"]["derived"] is None


def test_source_phase_order_is_derived_from_times_not_index_item_order(forwarded_task):
    forwarded_task.index["items"].reverse()
    save_index(forwarded_task)
    result, blockers, _ = check(forwarded_task)
    assert not blockers
    assert [phase["pending_count"] for phase in result["collection_reconciliation"]["phases"]] == [425, 226]


def test_successful_sql_row_does_not_bypass_full_payload_identity(recorded_task):
    recorded_task.extra["task_history_full_payload"]["full_payload_sha256"] = "0" * 64
    recorded_task.save(full_audit=False)
    result, blockers, _ = check(recorded_task)
    assert "scheduled_refresh_collection_sql_full_payload_identity_mismatch" in blockers
    assert result["collection_reconciliation"]["parent_payload_binding"]["verified"] is False
    assert result["row"]["status"] == "success"


def test_parent_workflow_failure_is_not_reclassified_as_only_collection_degradation(forwarded_task):
    task = forwarded_task
    task.task_audit["workflow_summary"]["status"] = "failed"
    task.save()
    _, blockers, _ = check(task)
    assert "scheduled_refresh_workflow_summary_not_success" in blockers


def test_unknown_current_collection_does_not_borrow_an_older_workflow_index(forwarded_task):
    task = forwarded_task
    task.task_audit["workflow_summary"]["run_id"] = "unrelated-run"
    task.task_audit["workflow_summary"].pop("options")
    task.index_path.unlink()
    task.save()
    result, blockers, _ = check(task)
    assert "scheduled_refresh_task_run_audit_run_id_mismatch" in blockers
    assert result["collection_reconciliation"]["derived"] is None


def test_newer_state_is_not_compared_as_this_historical_task(forwarded_task):
    task = forwarded_task
    for value in (task.state["last_runs"][task.row["task_name"]], task.state["task_closures"]["daily_refresh"]):
        value.update(started_at="2026-09-13T07:30:00", status="success", screening_run_id="later-run")
    task.save()
    result, blockers, _ = check(task)
    assert not blockers
    assert not any(view["source"].startswith("state.") for view in result["collection_reconciliation"]["parent_views"])


@pytest.mark.parametrize("source_still_running", [False, True])
def test_prior_collection_has_its_own_owner_and_does_not_invent_a_new_observation(recorded_task, source_still_running):
    task = recorded_task
    earlier = dict(task.row)
    # The collection succeeded even if later workflow work failed. A separate
    # rescreen may use those collection inputs, but cannot claim their counts.
    earlier["status"] = "failed"
    if source_still_running:
        earlier["completed_at"] = "2026-09-12 09:10:00"
    with sqlite3.connect(task.settings.database_path) as conn:
        conn.execute("INSERT OR REPLACE INTO task_history VALUES (?,?,?,?,?,?,?,?,?,?)",
                     tuple(earlier.get(key) for key in COLUMNS))
    task.run_id = "isolated-later-rescreen"
    task.row.update(id=12624, status="success", started_at="2026-09-12 09:00:00", completed_at="2026-09-12 09:05:00")
    log = task.settings.project_root / "rescreen.log"
    log.write_text("isolated report-only rescreen; no current collection\n")
    audit_path = task.settings.project_root / "rescreen.json"
    extra = {"status": "success", "collection_skipped": True, "screening_reason": "maintenance_changed_rescreen",
             "screening_run_id": task.run_id, "screening_final_count": 260, "screening_triggered": True,
             "task_log_path": str(log), "task_run_audit_path": str(audit_path)}
    audit_path.write_text(json.dumps({"task": {"task_history_id": task.row["id"], "task_name": task.row["task_name"], "status": "success"},
        "task_result": extra, "workflow_summary": {"run_id": task.run_id, "status": "succeeded",
            "options": {"run_collection": False, "run_screening": False}}}))
    task.extra.clear()
    task.extra.update(extra)
    task.save(full_audit=False)
    result, blockers, _ = check(task)
    if source_still_running:
        assert "scheduled_refresh_prior_same_day_collection_evidence_missing" in blockers
    else:
        assert not blockers
        reconciliation = result["collection_reconciliation"]
        assert reconciliation["current_task_phase_count"] == 0
        assert reconciliation["source_phase_count"] == 1
        assert reconciliation["phases"][0]["source_task_history_id"] == 12623
        assert reconciliation["derived"] is None
        assert result["row"]["status"] == "success"
