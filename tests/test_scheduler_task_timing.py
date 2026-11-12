"""Completion must include observation/audit work; fixtures contain no fund data."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend import scheduler as scheduler_module
from backend import scheduler_state, task_runtime
from backend.scheduler import ProjectScheduler
from backend.scheduler_diagnostics import (
    build_task_run_artifacts_summary,
    summarize_scheduler_run_for_api,
)
from backend.scheduler_state import SchedulerStateStore
from backend.scheduler_status_support import build_task_closure_summary, summarize_task_closure_for_state
from backend.task_runtime import SqliteTaskRecorder
from backend.task_audit_storage import read_task_audit


TASK = "scheduled_snapshot_watchdog_refresh"


@pytest.fixture
def timed_scheduler(tmp_path, monkeypatch):
    clock = SimpleNamespace(wall=datetime(2026, 9, 10, 23, 59, 58), elapsed=0.0)

    def advance(seconds):
        clock.wall += timedelta(seconds=seconds)
        clock.elapsed += seconds

    class ClockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock.wall if tz is None else clock.wall.astimezone(tz)

    for module in (scheduler_module, scheduler_state, task_runtime):
        monkeypatch.setattr(module, "datetime", ClockDateTime)
    try:
        timing_module = importlib.import_module("backend.scheduler_task_timing")
    except ModuleNotFoundError:
        pass  # Run the same behavior assertions against the old implementation.
    else:
        monkeypatch.setattr(timing_module, "datetime", ClockDateTime)
        monkeypatch.setattr(timing_module, "monotonic", lambda: clock.elapsed)

    instance = ProjectScheduler.__new__(ProjectScheduler)
    instance.settings = SimpleNamespace(
        project_root=tmp_path, reports_dir=tmp_path / "reports",
        database_path=tmp_path / "tasks.db", scheduler_state_file=tmp_path / "scheduler.json",
        scheduler_timezone="Asia/Shanghai", effective_nav_coverage_ratio=0.95,
        effective_nav_weekday_only=True,
    )
    instance.state_store = SchedulerStateStore(instance.settings.scheduler_state_file)
    instance.task_recorder = SqliteTaskRecorder(instance.settings.database_path)
    instance._manual_run_lock = threading.Lock()
    instance._startup_recovery_context = threading.local()
    instance._task_log_context = threading.local()
    instance._task_log_file_lock = threading.Lock()
    instance._ensure_runtime_host_allowed = lambda: None
    instance._write_heartbeat_file = lambda: None
    instance._enqueue_post_task_catch_up_if_needed = lambda **kwargs: None
    instance._host_context = lambda: "isolated_timing_test"
    instance._serialize_jobs = lambda: []
    instance._workflow_summary_for_task_audit = lambda: {}
    instance.logger = lambda message: None
    events = {}
    monkeypatch.setattr(scheduler_module, "load_publication_registry", lambda settings: {})
    monkeypatch.setattr(scheduler_module, "load_scheduler_status", lambda *args, **kwargs: instance.state_store.snapshot())
    monkeypatch.setattr(scheduler_module, "load_effective_nav_snapshot", lambda *args, **kwargs: SimpleNamespace(to_dict=lambda: {}))

    def status_payload(state, publication=None):
        latest = deepcopy(state.get("last_runs", {}).get(TASK, {}))
        return {"snapshot_watchdog_run": latest,
                "task_closures": {"snapshot_watchdog": build_task_closure_summary(
                    task_key="snapshot_watchdog", task_name=TASK, result=latest)}}

    instance._build_scheduler_status_payload_for_task_audit = status_payload
    original_audit = instance._write_task_run_audit

    def audit(**kwargs):
        events["owner_during_audit"] = instance.state_store.snapshot().get("current_task")
        advance(5)
        if events.get("audit_fails"):
            events["audit_end"] = clock.wall
            raise OSError("isolated audit write failure")
        result = original_audit(**kwargs)
        events["audit_end"] = clock.wall
        return result

    def refresh():
        events["owner_during_refresh"] = instance.state_store.snapshot().get("current_task")
        advance(7)
        events["refresh_end"] = clock.wall
        if events.get("refresh_fails"):
            raise OSError("isolated summary write failure")

    instance._write_task_run_audit = audit
    instance._refresh_runtime_status_summary = refresh
    original_connect = instance.task_recorder._connect
    calls = []

    def connect():
        if calls:
            advance(2)  # A slow final DB open/lock must not be hidden by an early timestamp.
        calls.append(True)
        conn = original_connect()
        events["database_opened_at"] = clock.wall
        return conn

    instance.task_recorder._connect = connect
    from backend import screening_archive_shadow, storage_observation

    def archive(*args, **kwargs):
        advance(3)
        events["archive_end"] = clock.wall
        return {"status": "skipped", "reason": "isolated_not_due"}

    def storage(*args, **kwargs):
        advance(4)
        events["storage_end"] = clock.wall
        if events.get("storage_fails"):
            raise TimeoutError("isolated observation timeout")
        return {"status": "skipped", "reason": "isolated_not_due"}

    monkeypatch.setattr(screening_archive_shadow, "observe_due_archive_shadow", archive)
    monkeypatch.setattr(storage_observation, "observe_due_storage_usage", storage)
    return instance, clock, advance, events


def execute(fixture, status="skipped", *, large=False):
    instance, clock, advance, events = fixture

    def runner():
        started = clock.wall.isoformat()
        advance(2)
        events["business_end"] = clock.wall
        if status == "raises":
            raise RuntimeError("isolated runner failure")
        result = {"task_name": TASK, "status": status, "started_at": started,
                  "finished_at": clock.wall.isoformat(), "business_marker": "preserved"}
        if large:
            result["test_transport_padding"] = "x" * 300_000
        return result

    return instance._run_task(TASK, runner, trigger_source="interval")


@pytest.mark.parametrize("status", ["success", "skipped", "degraded", "failed", "raises"])
def test_task_completion_includes_observations_audit_refresh_and_database_wait(timed_scheduler, status):
    instance, clock, _, events = timed_scheduler
    result = execute(timed_scheduler, status)
    finished = datetime.fromisoformat(result["finished_at"])
    assert finished >= max(events[name] for name in (
        "business_end", "archive_end", "storage_end", "audit_end", "refresh_end", "database_opened_at"))
    assert datetime.fromisoformat(result["business_finished_at"]) == events["business_end"]
    assert result["status"] == ("failed" if status == "raises" else status)
    assert result["task_timing"]["elapsed_seconds"] == pytest.approx(clock.elapsed)
    assert not instance._manual_run_lock.locked()
    assert instance.state_store.snapshot()["current_task"] is None


def test_active_task_ownership_is_retained_through_audit_and_summary(timed_scheduler):
    _, _, _, events = timed_scheduler
    execute(timed_scheduler)
    assert events["owner_during_audit"]["task_name"] == TASK
    assert events["owner_during_refresh"]["task_name"] == TASK


@pytest.mark.parametrize("large", [False, True])
def test_final_sql_state_audit_closure_and_api_share_completion(timed_scheduler, large):
    instance, _, _, _ = timed_scheduler
    result = execute(timed_scheduler, "success", large=large)
    with sqlite3.connect(instance.settings.database_path) as conn:
        status, completed, duration, raw = conn.execute(
            "SELECT status,completed_at,duration,extra_data FROM task_history WHERE task_name=?", (TASK,)).fetchone()
        current_completed = conn.execute("SELECT completed_at FROM task_status WHERE task_name=?", (TASK,)).fetchone()[0]
    extra = json.loads(raw)
    state = instance.state_store.snapshot()
    assert result["task_run_audit_path"].endswith(".json.gz")
    audit = read_task_audit(result["task_run_audit_path"])
    closure = build_task_closure_summary(task_key="snapshot_watchdog", task_name=TASK, result=extra)
    artifact_summary = build_task_run_artifacts_summary(state)["task_map"]["snapshot_watchdog"]
    for record in (extra, state["last_runs"][TASK], audit["task"], audit["task_result"],
                   audit["scheduler"]["task_run_summary"], audit["scheduler"]["task_closure"],
                   summarize_task_closure_for_state(closure), summarize_scheduler_run_for_api(extra), artifact_summary):
        assert record["finished_at"] == result["finished_at"]
        assert record["business_finished_at"] == result["business_finished_at"]
        assert record["task_timing"] == result["task_timing"]
        assert record["status"] == status == "success"
    assert datetime.fromisoformat(completed) == datetime.fromisoformat(result["finished_at"])
    assert completed == current_completed
    assert duration == int(result["task_timing"]["elapsed_seconds"])
    assert bool(extra.get("task_history_extra_data_compacted")) == large
    if large:
        raw_full = json.dumps(audit["task_result"], ensure_ascii=False, separators=(",", ":")).encode()
        assert extra["task_history_full_payload"]["full_payload_sha256"] == hashlib.sha256(raw_full).hexdigest()
    assert len(raw) < 25_000
    assert "finished_at=" + result["finished_at"] in Path(result["task_log_path"]).read_text()


@pytest.mark.parametrize("failure", ["storage_fails", "audit_fails", "refresh_fails"])
def test_slow_failed_finalization_keeps_business_status_and_actual_end(timed_scheduler, failure):
    instance, _, _, events = timed_scheduler
    events[failure] = True
    result = execute(timed_scheduler)
    assert datetime.fromisoformat(result["finished_at"]) >= events["refresh_end"]
    assert result["status"] == "skipped"
    if failure == "storage_fails":
        assert result["storage_observation"]["status"] == "failed"
    elif failure == "audit_fails":
        assert result["task_run_audit_status"] == "failed"
    else:
        assert result["runtime_status_summary_refresh_error"] == "isolated summary write failure"
    with sqlite3.connect(instance.settings.database_path) as conn:
        extra = json.loads(conn.execute("SELECT extra_data FROM task_history").fetchone()[0])
    assert extra["finished_at"] == result["finished_at"]
    assert extra.get("runtime_status_summary_refresh_error") == result.get("runtime_status_summary_refresh_error")


def test_legacy_api_does_not_invent_phase_times_or_zero_durations():
    raw = {"status": "skipped", "finished_at": "2026-09-09T01:00:00"}
    original = deepcopy(raw)
    summary = summarize_scheduler_run_for_api(raw)
    assert "business_finished_at" not in summary
    assert "task_timing" not in summary
    assert raw == original


def test_unreported_business_times_remain_null_after_real_completion(timed_scheduler):
    instance, _, _, _ = timed_scheduler
    result = instance._run_task(TASK, lambda: {"status": "skipped"}, trigger_source="interval")
    assert result["business_started_at"] is None
    assert result["business_finished_at"] is None
    assert result["finished_at"]
    assert result["task_timing"]["phases"]["business"]["elapsed_seconds"] == 0
    from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
    compact = compact_task_result_for_history_if_needed(result, task_name=TASK, force=True, write_sidecar=False)
    assert compact["business_finished_at"] is None
    assert summarize_scheduler_run_for_api(compact)["business_finished_at"] is None


def test_monotonic_duration_survives_wall_clock_correction(timed_scheduler):
    instance, clock, advance, _ = timed_scheduler

    def runner():
        start = clock.wall.isoformat()
        advance(2)
        clock.wall -= timedelta(hours=1)
        return {"status": "skipped", "started_at": start, "finished_at": clock.wall.isoformat()}

    result = instance._run_task(TASK, runner, trigger_source="interval")
    assert result["task_timing"]["elapsed_seconds"] == pytest.approx(clock.elapsed)
    assert result["task_timing"]["wall_elapsed_seconds"] == pytest.approx(clock.elapsed - 3600)
    with sqlite3.connect(instance.settings.database_path) as conn:
        duration = conn.execute("SELECT duration FROM task_history").fetchone()[0]
    assert duration == int(clock.elapsed) > 0


def test_offset_start_time_and_unknown_total_are_not_fabricated(timed_scheduler):
    instance, clock, _, _ = timed_scheduler
    task = instance.task_recorder.start_task(TASK)
    task["started_at"] = clock.wall.astimezone(timezone.utc).isoformat()
    with sqlite3.connect(instance.settings.database_path) as conn:
        conn.execute("UPDATE task_history SET started_at=? WHERE id=?", (task["started_at"], task["id"]))
    result = instance._finalize_task(task_info=task, task_name=TASK, result={"status": "skipped"})
    assert result["task_timing"]["elapsed_seconds"] is None
    assert result["task_timing"]["wall_elapsed_seconds"] is None
    with sqlite3.connect(instance.settings.database_path) as conn:
        duration = conn.execute("SELECT duration FROM task_history").fetchone()[0]
    assert duration == 14  # Five audit seconds, seven summary seconds, two DB-open seconds.


def test_seal_failure_retains_original_audit_and_records_failure_without_retiming_sql(timed_scheduler, monkeypatch):
    instance, _, advance, _ = timed_scheduler
    original_bytes = []

    def fail(path, result):
        original_bytes.append(Path(path).read_bytes())
        advance(6)
        raise OSError("isolated completion projection failure")

    monkeypatch.setattr(scheduler_module, "seal_task_run_audit", fail)
    result = execute(timed_scheduler, large=True)
    assert result["task_run_audit_status"] == "failed"
    assert "completion_seal_failed" in result["task_run_audit_error"]
    assert Path(result["task_run_audit_path"]).read_bytes() == original_bytes[0]
    with sqlite3.connect(instance.settings.database_path) as conn:
        completed, raw = conn.execute("SELECT completed_at,extra_data FROM task_history").fetchone()
    assert datetime.fromisoformat(completed) == datetime.fromisoformat(result["finished_at"])
    extra = json.loads(raw)
    assert extra["task_run_audit_status"] == "failed"
    assert extra["task_history_full_payload"]["source"] == "scheduler_finalize"
    assert instance.state_store.snapshot()["last_runs"][TASK]["task_run_audit_status"] == "failed"
    assert "[task-recorded]" in Path(result["task_log_path"]).read_text()


def test_atomic_audit_seal_failure_does_not_destroy_prepared_evidence(timed_scheduler, monkeypatch):
    from backend import task_audit_storage
    instance, _, _, _ = timed_scheduler
    original_replace = task_audit_storage.os.replace

    def replace(source, target):
        if "scheduler_task_runs" in str(target):
            raise OSError("isolated rename interruption")
        return original_replace(source, target)

    monkeypatch.setattr(task_audit_storage.os, "replace", replace)
    result = execute(timed_scheduler)
    assert result["task_run_audit_status"] == "failed"
    audit_path = Path(result["task_run_audit_path"])
    audit = read_task_audit(audit_path)
    assert audit["persistence_stage"] == "audit_collected_before_completion"
    assert audit["task"]["finished_at"] is None
    assert list(audit_path.parent.glob(".*.tmp")) == []
