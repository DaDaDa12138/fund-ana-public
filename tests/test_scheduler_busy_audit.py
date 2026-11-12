"""Isolated scheduler contention; no fund data or real automatic run is created."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.scheduler import ProjectScheduler
from backend.scheduler_state import SchedulerStateStore
from backend.task_runtime import SqliteTaskRecorder
from backend.scheduler_diagnostics import build_scheduler_chain_audit, build_scheduler_status_payload
from scripts.audit_data_truthfulness import _scheduler_task_audit_outcomes


ACTIVE = "scheduled_daily_bond_refresh"
WATCHDOG = "scheduled_snapshot_watchdog_refresh"


def sql_rows(path: Path, table: str) -> list[tuple]:
    assert table in {"task_status", "task_history"}
    with sqlite3.connect(path) as conn:
        return conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()


@pytest.fixture
def busy_scheduler(tmp_path: Path):
    instance = ProjectScheduler.__new__(ProjectScheduler)
    instance.settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
                                         scheduler_state_file=tmp_path / "scheduler.json")
    instance.state_store = SchedulerStateStore(instance.settings.scheduler_state_file)
    instance.state_store.finish_task("fixture_previous_task", {"status": "degraded", "error": "retained fixture error"})
    instance.state_store.start_task(ACTIVE)
    instance.task_recorder = SqliteTaskRecorder(tmp_path / "tasks.db")
    instance.active_task_info = instance.task_recorder.start_task(ACTIVE)
    instance._manual_run_lock = threading.Lock()
    instance._manual_run_lock.acquire()
    instance._ensure_runtime_host_allowed = lambda: None
    instance._host_context = lambda: "isolated_contention_test"
    instance.logger = Mock()
    instance._task_log_context = threading.local()
    active_log = tmp_path / "active.log"
    active_log.write_text("existing active log\n")
    instance._task_log_context.payload = {"path": active_log, "task_name": ACTIVE}
    instance._write_task_run_audit = Mock(side_effect=AssertionError("must not bind active workflow to skipped attempt"))
    instance._finalize_task = Mock(side_effect=AssertionError("must not finish active task"))
    instance._enter_task_log_context = Mock(side_effect=AssertionError("must not replace active log context"))
    yield instance
    instance._manual_run_lock.release()


@pytest.mark.parametrize("task_name,trigger_source", [(WATCHDOG, "interval"), (ACTIVE, "cron")])
def test_busy_attempt_is_persisted_without_finishing_the_running_task(busy_scheduler, task_name, trigger_source):
    instance = busy_scheduler
    state_before = instance.state_store.snapshot()
    statuses_before = sql_rows(instance.task_recorder.db_path, "task_status")
    active_before = sql_rows(instance.task_recorder.db_path, "task_history")[0]
    log_context = instance._task_log_context.payload
    runner = Mock(side_effect=AssertionError("business runner must not execute"))

    result = instance._run_task(task_name, runner, trigger_source=trigger_source)

    assert result.get("trigger_source") == trigger_source
    assert result["status"] == "skipped" and result["skip_reason"] == "scheduler_busy"
    assert result["task_run_audit_status"] == result["task_history_write_status"] == "written"
    assert result["scheduler_state_write_status"] == "written"
    state_after = instance.state_store.snapshot()
    assert state_after["current_task"] == state_before["current_task"]
    assert state_after["last_runs"] == state_before["last_runs"]
    assert state_after["last_error"] == state_before["last_error"]
    assert state_after["recent_busy_skips"][0] == result
    assert instance._manual_run_lock.locked()
    assert instance._task_log_context.payload is log_context
    assert log_context["path"].read_text() == "existing active log\n"
    assert sql_rows(instance.task_recorder.db_path, "task_status") == statuses_before
    history = sql_rows(instance.task_recorder.db_path, "task_history")
    assert history[0] == active_before and len(history) == 2
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        status, extra = conn.execute("SELECT status,extra_data FROM task_history WHERE id=?", (result["task_history_id"],)).fetchone()
    assert status == "skipped" and json.loads(extra) == result
    artifact = json.loads(Path(result["task_run_audit_path"]).read_text())
    assert artifact["artifact_type"] == "scheduler_busy_skip"
    assert artifact["task"]["task_history_id"] == result["task_history_id"]
    assert artifact["task_result"]["trigger_source"] == trigger_source
    assert artifact["task_result"]["busy_task"] == state_before["current_task"]
    assert artifact["business_runner_executed"] is False
    assert "workflow_run_id" not in artifact["task_result"]
    assert Path(result["task_log_path"]).is_file()
    runner.assert_not_called()
    instance._write_task_run_audit.assert_not_called()
    instance._finalize_task.assert_not_called()


def test_concurrent_skips_have_distinct_ids_artifacts_and_do_not_run_shadow(busy_scheduler, monkeypatch):
    from backend import screening_archive_shadow, storage_observation
    observer = Mock(side_effect=AssertionError("busy attempts cannot count observation windows"))
    monkeypatch.setattr(screening_archive_shadow, "observe_due_archive_shadow", observer)
    storage_observer = Mock(side_effect=AssertionError("busy attempts cannot scan storage"))
    monkeypatch.setattr(storage_observation, "observe_due_storage_usage", storage_observer)
    instance = busy_scheduler
    before = instance.state_store.snapshot()["current_task"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: instance._run_task(WATCHDOG, Mock(), trigger_source="interval"), range(8)))
    assert len({row["attempt_id"] for row in results}) == 8
    assert len({row["task_history_id"] for row in results}) == 8
    assert len({row["task_run_audit_path"] for row in results}) == 8
    assert len(instance.state_store.snapshot()["recent_busy_skips"]) == 8
    assert instance.state_store.snapshot()["current_task"] == before
    assert len(sql_rows(instance.task_recorder.db_path, "task_status")) == 1
    assert all(row["archive_shadow_observation"] == {"status": "skipped", "reason": "scheduler_busy"} for row in results)
    assert all(row["storage_observation"] == {"status": "skipped", "reason": "scheduler_busy"} for row in results)
    observer.assert_not_called()
    storage_observer.assert_not_called()


def test_state_history_is_bounded_and_preserves_audited_details(busy_scheduler):
    instance = busy_scheduler
    result = instance._run_task(WATCHDOG, Mock(), trigger_source="interval")
    for index in range(25):
        item = {**deepcopy(result), "attempt_id": f"isolated_history_boundary_{index}"}
        instance.state_store.record_busy_skip(item)
    history = instance.state_store.snapshot()["recent_busy_skips"]
    assert len(history) == 20
    assert history[0]["attempt_id"] == "isolated_history_boundary_24"
    assert history[-1]["attempt_id"] == "isolated_history_boundary_5"
    assert instance.state_store.snapshot()["current_task"]["task_name"] == ACTIVE


@pytest.mark.parametrize("failure", ["audit", "sql_start", "sql_finish", "state", "log"])
def test_persistence_failure_remains_visible_without_running_business_work(busy_scheduler, monkeypatch, failure):
    from backend import scheduler_busy_audit
    instance = busy_scheduler
    before = instance.state_store.snapshot()["current_task"]

    def fail(*args, **kwargs):
        raise OSError(f"isolated {failure} write failure")

    if failure == "audit":
        monkeypatch.setattr(scheduler_busy_audit, "write_audit_payload", fail)
    elif failure == "sql_start":
        monkeypatch.setattr(instance.task_recorder, "record_skipped_attempt", fail)
    elif failure == "sql_finish":
        monkeypatch.setattr(instance.task_recorder, "finish_task", fail)
    elif failure == "state":
        monkeypatch.setattr(instance.state_store, "record_busy_skip", fail)
    else:
        (instance.settings.project_root / "logs").write_text("isolated path collision")
    result = instance._run_task(WATCHDOG, Mock(side_effect=AssertionError("must skip")), trigger_source="interval")
    field = {"audit": "task_run_audit_status", "state": "scheduler_state_write_status", "log": "task_log_write_status"}.get(failure, "task_history_write_status")
    assert result["status"] == "skipped" and result[field] == "failed"
    assert instance.state_store.snapshot()["current_task"] == before
    if failure != "state":
        assert instance.state_store.snapshot()["recent_busy_skips"][0][field] == "failed"
        audit = _scheduler_task_audit_outcomes(instance.settings)
        assert any("busy_skip" in warning for warning in audit["warnings"])
    if failure == "state":
        with sqlite3.connect(instance.task_recorder.db_path) as conn:
            extra = conn.execute("SELECT extra_data FROM task_history WHERE id=?", (result["task_history_id"],)).fetchone()[0]
        assert json.loads(extra)[field] == "failed"
    assert any("failed" in str(call) for call in instance.logger.call_args_list)


def test_skip_persistence_does_not_restore_a_task_that_finished_during_io(busy_scheduler, monkeypatch):
    from backend import scheduler_busy_audit
    instance = busy_scheduler
    write = scheduler_busy_audit.write_audit_payload
    completed = {"status": "success", "finished_at": "isolated_completion"}

    def owner_finishes(path, payload):
        instance.state_store.finish_task(ACTIVE, completed)
        instance.task_recorder.finish_task(instance.active_task_info, status="success", extra_data=completed)
        return write(path, payload)

    monkeypatch.setattr(scheduler_busy_audit, "write_audit_payload", owner_finishes)
    result = instance._run_task(ACTIVE, Mock(), trigger_source="cron")
    state = instance.state_store.snapshot()
    assert state["current_task"] is None
    assert state["last_runs"][ACTIVE] == completed
    assert result["busy_task"]["task_name"] == ACTIVE
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        assert conn.execute("SELECT status FROM task_status WHERE task_name=?", (ACTIVE,)).fetchone()[0] == "success"


def test_missing_legacy_persistence_outcome_is_not_reported_as_success():
    from backend.scheduler_diagnostics import summarize_busy_skips
    result = summarize_busy_skips([{"status": "skipped", "skip_reason": "scheduler_busy"}])
    assert result["recent_count"] == result["pending_persistence_count"] == 1
    assert result["latest_started_at"] is None


def test_busy_attempt_does_not_satisfy_latest_completed_execution(busy_scheduler):
    instance = busy_scheduler
    instance.settings.database_path = instance.task_recorder.db_path
    prior = instance.task_recorder.start_task(ACTIVE)
    instance.task_recorder.finish_task(prior, status="success", extra_data={"status": "success"})
    prior_at = "2026-09-07 20:00:00"
    with sqlite3.connect(instance.task_recorder.db_path) as conn:
        conn.execute("UPDATE task_history SET started_at=?,completed_at=? WHERE id=?", (prior_at, prior_at, prior["id"]))
    instance._run_task(ACTIVE, Mock(), trigger_source="cron")
    assert instance._latest_completed_run_at(ACTIVE) == datetime.fromisoformat(prior_at)


def test_busy_weekly_attempt_does_not_hide_the_active_write_window(busy_scheduler):
    from scripts.audit_data_truthfulness import _weekly_maintenance_live_write_window
    instance = busy_scheduler
    instance.settings.database_path = instance.task_recorder.db_path
    weekly = "scheduled_weekly_bond_maintenance"
    instance.state_store.mark_started(scheduler_instance_id="isolated_weekly_owner")
    instance.state_store.start_task(weekly)
    task = instance.task_recorder.start_task(weekly)
    assert _weekly_maintenance_live_write_window(instance.settings)["active"] is True
    instance._run_task(weekly, Mock(), trigger_source="cron")
    after = _weekly_maintenance_live_write_window(instance.settings)
    assert after["active"] is True
    assert after["matched_row"]["id"] == task["id"]


def test_execution_history_filter_preserves_unknown_legacy_metadata(tmp_path: Path):
    from backend.task_runtime import SCHEDULER_EXECUTION_ROW_SQL
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE task_history (id INTEGER PRIMARY KEY, status TEXT, extra_data TEXT)")
        conn.executemany("INSERT INTO task_history VALUES (?, 'skipped', ?)", [
            (1, None), (2, "invalid JSON"), (3, "{}"),
            (4, '{"skip_reason":"effective_nav_snapshot_already_screened"}'),
            (5, '{"skip_reason":"scheduler_busy"}'),
        ])
        ids = [row[0] for row in conn.execute(f"SELECT id FROM task_history WHERE {SCHEDULER_EXECUTION_ROW_SQL} ORDER BY id")]
    assert ids == [1, 2, 3, 4]


def test_busy_attempt_preserves_failed_schedule_slot_and_open_failure(busy_scheduler):
    from backend.scheduler_diagnostics import build_recent_scheduler_failure_recovery_summary
    from backend.scheduler_status_support import load_task_schedule_slot_runs

    instance = busy_scheduler
    instance.settings.database_path = instance.task_recorder.db_path
    task = instance.task_recorder.start_task(ACTIVE)
    instance.task_recorder.finish_task(task, status="failed", error_message="isolated failure")
    now = datetime.now()

    def read_slot():
        return load_task_schedule_slot_runs(
            instance.task_recorder.db_path, instance.settings, "daily_refresh", ACTIVE,
            parse_datetime=datetime.fromisoformat, limit_rows=1,
        )

    def read_failures():
        return build_recent_scheduler_failure_recovery_summary(
            instance.settings, max_runs_per_task=1, now=now,
        )

    slot_before, failures_before = read_slot(), read_failures()
    assert slot_before[0]["_history_id"] == task["id"]
    assert failures_before["summary"]["open_failure_sequence_total"] == 1
    instance._run_task(ACTIVE, Mock(), trigger_source="cron")
    assert read_slot() == slot_before
    assert read_failures() == failures_before


def test_busy_weekly_attempt_preserves_warning_truth(busy_scheduler):
    from scripts.audit_data_truthfulness import _find_weekly_maintenance_warning_truth

    instance = busy_scheduler
    instance.settings.database_path = instance.task_recorder.db_path
    weekly = "scheduled_weekly_bond_maintenance"
    task = instance.task_recorder.start_task(weekly)
    instance.task_recorder.finish_task(task, status="degraded", extra_data={
        "status": "degraded", "degraded": True, "warning_reason": "isolated_collection_failure",
    })
    before = _find_weekly_maintenance_warning_truth(instance.settings)
    assert before[0]["matched"] is True
    instance._run_task(weekly, Mock(), trigger_source="cron")
    assert _find_weekly_maintenance_warning_truth(instance.settings) == before


@pytest.mark.parametrize("reader", ["scheduler", "truthfulness"])
def test_busy_history_cannot_evict_daily_lineage_from_bounded_search(busy_scheduler, reader):
    from scripts.audit_data_truthfulness import _find_scheduled_refresh_task_truth

    instance = busy_scheduler
    instance.settings.database_path = instance.task_recorder.db_path
    run_id = "isolated_lineage_fixture"
    task = instance.task_recorder.start_task(ACTIVE)
    instance.task_recorder.finish_task(task, status="success", extra_data={
        "status": "success", "screening_run_id": run_id, "screening_triggered": True,
    })

    def read():
        if reader == "scheduler":
            return instance._latest_report_scheduled_daily_lineage_evidence(run_id)
        return _find_scheduled_refresh_task_truth(instance.settings, run_id=run_id, final_count=0)

    before = read()
    if reader == "scheduler":
        assert before["task_history_id"] == task["id"]
    else:
        assert before[0]["matched"] is True
    for _ in range(201):
        instance.task_recorder.record_skipped_attempt(ACTIVE, result={
            "status": "skipped", "skip_reason": "scheduler_busy",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        })
    assert read() == before


@pytest.mark.parametrize("include_detail", [False, True])
def test_scheduler_api_exposes_skips_and_failures_without_replacing_active_state(busy_scheduler, include_detail):
    instance = busy_scheduler
    result = instance._run_task(WATCHDOG, Mock(), trigger_source="interval")
    state = instance.state_store.snapshot()
    state["recent_busy_skips"][0]["task_run_audit_status"] = "failed"
    state["recent_busy_skips"][0]["task_run_audit_error"] = "isolated audit error"
    payload = build_scheduler_status_payload(
        state, include_detail=include_detail,
        build_scheduler_overview_summary=lambda item: deepcopy(item),
        build_scheduler_overview_api_summary_fn=lambda item: deepcopy(item),
        build_scheduler_chain_audit=build_scheduler_chain_audit,
        build_automation_autonomy_summary=lambda *args: {},
    )
    assert payload["current_task"] == state["current_task"]
    assert payload["busy_skip_summary"]["recent_count"] == 1
    assert payload["busy_skip_summary"]["persistence_failure_count"] == 1
    assert "scheduler_busy_skip_persistence_failed" in {row["code"] for row in payload["scheduler_chain_audit"]["open_issues"]}
    if include_detail:
        assert payload["recent_busy_skips"][0]["attempt_id"] == result["attempt_id"]
    else:
        assert "recent_busy_skips" not in payload


@pytest.mark.parametrize("endpoint", ["health", "status"])
def test_web_compact_payloads_preserve_busy_summary_and_warning(busy_scheduler, monkeypatch, endpoint):
    import backend.app as web

    instance = busy_scheduler
    instance._run_task(WATCHDOG, Mock(), trigger_source="interval")
    state = instance.state_store.snapshot()
    state["recent_busy_skips"][0]["task_run_audit_status"] = "failed"
    state["recent_busy_skips"][0]["task_run_audit_error"] = "isolated artifact write failure"
    # Keep the real web payload assemblers and chain audit, isolate unrelated business reads.
    for name in ("_build_scheduler_overview_summary", "_load_latest_report_cached",
                 "_load_latest_report_snapshot_cached", "_load_default_risk_lag_summary",
                 "_load_risk_sync_summary", "_load_risk_coverage_gap_summary",
                 "_load_current_final_selection_governance_queue_summary_cached",
                 "_scheduler_summary_common_file_cache_basis", "_build_automation_autonomy_summary"):
        monkeypatch.setattr(web, name, lambda *args, **kwargs: {})
    monkeypatch.setattr(web, "_resolve_lightweight_scheduler_runtime_digest_sources", lambda *args, **kwargs: ({}, {}))
    monkeypatch.setattr(web, "_load_scheduler_summary_fragment_cached", lambda name, basis, builder: (builder(), {}))
    monkeypatch.setattr(web, "_attach_lightweight_status_source_contract", lambda payload, **kwargs: payload)
    monkeypatch.setattr(web.dashboard_payloads, "_build_nav_risk_governance_summary", lambda *args, **kwargs: {})
    builder = web._build_health_scheduler_payload if endpoint == "health" else web._build_scheduler_status_summary_payload
    payload = builder(state)
    assert payload["busy_skip_summary"]["recent_count"] == 1
    assert payload["busy_skip_summary"]["persistence_failure_count"] == 1
    assert payload["current_task"] == state["current_task"]
    assert "recent_busy_skips" not in payload
    assert "scheduler_busy_skip_persistence_failed" in {row["code"] for row in payload["scheduler_chain_audit"]["open_issues"]}
    header = web._build_health_scheduler_status_summary(payload)
    assert header["busy_skip_summary"] == payload["busy_skip_summary"]


def test_overview_api_projection_preserves_busy_attempt_summary():
    from backend.scheduler_diagnostics import build_scheduler_overview_api_summary, summarize_busy_skips

    summary = summarize_busy_skips([{"status": "skipped", "skip_reason": "scheduler_busy"}])
    assert build_scheduler_overview_api_summary({"busy_skip_summary": summary})["busy_skip_summary"] == summary
