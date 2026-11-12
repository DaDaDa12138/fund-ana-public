from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.scheduler import ProjectScheduler, load_scheduler_status
from backend.scheduler_state import SchedulerStateStore


def test_scheduler_heartbeat_refreshes_next_schedule_during_a_long_task(tmp_path) -> None:
    store = SchedulerStateStore(tmp_path / "scheduler.json")
    store.mark_started(scheduler_instance_id="test-owner")
    store.start_task("fixture-long-task")
    store.set_jobs([{"id": "snapshot_watchdog", "next_run_time": "2026-09-07T18:20:00+08:00"}])
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.state_store = store
    scheduler.settings = SimpleNamespace(scheduler_heartbeat_interval_seconds=60)
    stopped = threading.Event()
    scheduler.stop_event = SimpleNamespace(is_set=stopped.is_set, wait=lambda seconds: stopped.set())
    scheduler._should_self_retire = lambda: False
    scheduler._write_heartbeat_file = lambda: None
    scheduler._state_store_runtime_api_config_kwargs = lambda: {"scheduler_instance_id": "test-owner"}
    scheduler.scheduler = SimpleNamespace(get_jobs=lambda: [SimpleNamespace(
        id="snapshot_watchdog", name="snapshot_watchdog", trigger="interval[0:15:00]",
        next_run_time=datetime.fromisoformat("2026-09-07T20:50:00+08:00"))])
    scheduler._heartbeat_loop()
    result = store.snapshot()
    assert result["jobs"][0]["next_run_time"] == "2026-09-07T20:50:00+08:00"
    assert result["current_task"]["task_name"] == "fixture-long-task"


def test_retired_scheduler_heartbeat_cannot_replace_new_owners_jobs(tmp_path) -> None:
    store = SchedulerStateStore(tmp_path / "scheduler.json")
    store.mark_started(scheduler_instance_id="new-owner")
    jobs = [{"id": "fixture", "next_run_time": "2026-09-08T07:30:00+08:00"}]
    store.set_jobs(jobs)
    before = store.snapshot()
    store.heartbeat(scheduler_instance_id="old-owner", jobs=[])
    after = store.snapshot()
    assert after["jobs"] == jobs
    assert after["last_heartbeat_at"] == before["last_heartbeat_at"]


def test_scheduler_resolves_startup_catch_up_jobs() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
    )

    scheduler._last_run_at = lambda task_name, require_satisfied=False: {  # type: ignore[method-assign]
        "scheduled_daily_bond_refresh": datetime.fromisoformat("2026-04-09T06:15:00"),
        "scheduled_weekly_bond_maintenance": datetime.fromisoformat("2026-04-05T03:30:00"),
    }.get(task_name)

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-10T16:30:00"),
    )

    assert due == ["daily_refresh"]

    scheduler._last_run_at = lambda task_name, require_satisfied=False: {  # type: ignore[method-assign]
        "scheduled_daily_bond_refresh": datetime.fromisoformat("2026-04-09T06:15:00"),
        "scheduled_weekly_bond_maintenance": datetime.fromisoformat("2026-04-05T03:30:00"),
    }.get(task_name)
    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T10:00:00"),
    )
    assert due == ["weekly_maintenance", "daily_refresh"]

    scheduler._last_run_at = lambda task_name, require_satisfied=False: None  # type: ignore[method-assign]
    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T10:00:00"),
    )
    assert due == ["weekly_maintenance", "daily_refresh"]

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T02:00:00"),
    )
    assert due == []

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T07:00:00"),
    )
    assert due == ["weekly_maintenance"]

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T23:00:00"),
    )
    assert due == ["weekly_maintenance", "daily_refresh"]

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-12T23:00:00") + timedelta(days=1),
    )
    assert due == ["weekly_maintenance", "daily_refresh"]


def test_scheduler_startup_catch_up_adds_daily_refresh_when_snapshot_already_advanced() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=23,
        scheduler_daily_minute=59,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=23,
        scheduler_weekly_minute=59,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: datetime.fromisoformat("2026-04-10T08:00:00")  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(snapshot=lambda: {"last_runs": {}})

    snapshot = SimpleNamespace(ready=True, candidate_date="2026-04-09", signature="sig-new")
    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-08", "signature": "sig-old"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
    ):
        due = ProjectScheduler._resolve_startup_catch_up_jobs(
            scheduler,
            datetime.fromisoformat("2026-04-10T16:00:00"),
        )

    assert due == ["intraday_refresh"]


def test_scheduler_startup_catch_up_retries_failed_daily_refresh_after_restart() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=6,
        scheduler_daily_minute=10,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": {
                "scheduled_weekly_bond_maintenance": {
                    "status": "success",
                    "started_at": "2026-04-06T03:05:00",
                    "finished_at": "2026-04-06T03:20:00",
                },
                "scheduled_daily_bond_refresh": {
                    "status": "failed",
                    "started_at": "2026-04-11T12:31:48",
                    "finished_at": "2026-04-11T12:36:01",
                    "error": "boom",
                },
            }
        }
    )

    with (
        patch(
            "backend.scheduler.load_effective_nav_snapshot",
            return_value=SimpleNamespace(ready=True, candidate_date="2026-04-09", signature="sig-new"),
        ),
        patch(
            "backend.scheduler.load_latest_screened_db_snapshot",
            return_value={"candidate_date": "2026-04-09", "signature": "sig-old"},
        ),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(False, "effective_nav_date_already_screened")),
    ):
        due = ProjectScheduler._resolve_startup_catch_up_jobs(
            scheduler,
            datetime.fromisoformat("2026-04-11T12:49:00"),
        )

    assert due == ["daily_refresh"]


def test_scheduler_startup_catch_up_skips_already_processed_same_snapshot() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=23,
        scheduler_daily_minute=59,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=23,
        scheduler_weekly_minute=59,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: datetime.fromisoformat("2026-04-10T08:00:00")  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": {
                "scheduled_daily_bond_refresh": {
                    "status": "success",
                    "screening_triggered": True,
                    "after_effective_nav_snapshot": {
                        "candidate_date": "2026-04-09",
                        "signature": "sig-new",
                    },
                }
            }
        }
    )

    snapshot = SimpleNamespace(ready=True, candidate_date="2026-04-09", signature="sig-new")
    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-08", "signature": "sig-old"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
    ):
        due = ProjectScheduler._resolve_startup_catch_up_jobs(
            scheduler,
            datetime.fromisoformat("2026-04-10T16:00:00"),
        )

    assert due == ["intraday_refresh"]


def test_scheduler_startup_catch_up_skips_same_snapshot_using_last_screening_run_report(
    tmp_path: Path,
) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    runs_dir = tmp_path / "runs"
    run_id = "20260411_125904_sync"
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / f"bond_analysis_report_{run_id}.json").write_text(
        json.dumps(
            {
                "database_freshness": {
                    "effective_nav_snapshot": {
                        "candidate_date": "2026-04-10",
                        "signature": "sig-new",
                        "ready": True,
                    }
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=23,
        scheduler_daily_minute=59,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=23,
        scheduler_weekly_minute=59,
        database_path=tmp_path / "unused.db",
        latest_report_file=tmp_path / "latest.json",
        runs_dir=runs_dir,
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: datetime.fromisoformat("2026-04-10T08:00:00")  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": {
                "scheduled_daily_bond_refresh": {
                    "status": "success",
                    "screening_triggered": True,
                    "screening_run_id": run_id,
                    "after_effective_nav_snapshot": {
                        "candidate_date": "2026-04-10",
                        "signature": "sig-old",
                    },
                }
            }
        }
    )

    snapshot = SimpleNamespace(ready=True, candidate_date="2026-04-10", signature="sig-new")
    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-09", "signature": "sig-old"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
    ):
        due = ProjectScheduler._resolve_startup_catch_up_jobs(
            scheduler,
            datetime.fromisoformat("2026-04-11T16:00:00"),
        )

    assert due == ["intraday_refresh"]


def test_scheduler_startup_catch_up_retries_same_snapshot_when_previous_run_never_triggered_screening() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=23,
        scheduler_daily_minute=59,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=23,
        scheduler_weekly_minute=59,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: datetime.fromisoformat("2026-04-10T08:00:00")  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": {
                "scheduled_daily_bond_refresh": {
                    "status": "degraded",
                    "screening_triggered": False,
                    "skip_reason": "workflow_busy_after_collection_state_only",
                    "after_effective_nav_snapshot": {
                        "candidate_date": "2026-04-10",
                        "signature": "sig-new",
                    },
                }
            }
        }
    )

    snapshot = SimpleNamespace(ready=True, candidate_date="2026-04-10", signature="sig-new")
    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-09", "signature": "sig-old"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
    ):
        due = ProjectScheduler._resolve_startup_catch_up_jobs(
            scheduler,
            datetime.fromisoformat("2026-04-11T16:00:00"),
        )

    assert due == ["intraday_refresh"]


def _blocked_snapshot_last_runs(
    *,
    candidate_date: str = "2026-07-06",
    signature: str = "sig-blocked",
    finished_at: str = "2026-07-07T15:30:00",
) -> dict[str, object]:
    return {
        "scheduled_intraday_bond_refresh": {
            "status": "degraded",
            "started_at": "2026-07-07T15:20:00",
            "finished_at": finished_at,
            "screening_triggered": True,
            "screening_run_id": "20260707_152000_sync",
            "after_effective_nav_snapshot": {
                "candidate_date": candidate_date,
                "signature": signature,
            },
            "publication_result": {
                "status": "blocked",
                "run_id": "20260707_152000_sync",
            },
        }
    }


def test_blocked_snapshot_retry_cooldown_expires_and_does_not_match_new_signature() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_blocked_snapshot_retry_cooldown_minutes=120,
    )
    state_payload = {"last_runs": _blocked_snapshot_last_runs()}

    active = scheduler._blocked_snapshot_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-blocked"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T16:00:00"),
    )
    assert active["active"] is True
    assert active["skip_reason"] == "blocked_snapshot_retry_cooldown"
    assert active["last_attempted_at"] == "2026-07-07T15:30:00"
    assert active["next_retry_at"] == "2026-07-07T17:30:00"
    assert active["remaining_seconds"] == 90 * 60

    expired = scheduler._blocked_snapshot_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-blocked"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T17:31:00"),
    )
    assert expired["active"] is False
    assert expired["skip_reason"] is None
    assert expired["next_retry_at"] == "2026-07-07T17:30:00"

    new_signature = scheduler._blocked_snapshot_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-new"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T16:00:00"),
    )
    assert new_signature["active"] is False
    assert new_signature["next_retry_at"] is None


def test_blocked_snapshot_retry_cooldown_survives_watchdog_skip_state_overwrite() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_blocked_snapshot_retry_cooldown_minutes=120,
    )
    state_payload = {
        "last_runs": {
            "scheduled_snapshot_watchdog_refresh": {
                "status": "skipped",
                "skip_reason": "blocked_snapshot_retry_cooldown",
                "candidate_date": "2026-07-06",
                "signature": "sig-blocked",
                "last_attempted_at": "2026-07-07T15:30:00",
                "next_retry_at": "2026-07-07T17:30:00",
                "blocked_task_name": "scheduled_snapshot_watchdog_refresh",
                "blocked_screening_run_id": "20260707_152000_sync",
            }
        }
    }

    cooldown = scheduler._blocked_snapshot_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-blocked"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T16:15:00"),
    )

    assert cooldown["active"] is True
    assert cooldown["last_attempted_at"] == "2026-07-07T15:30:00"
    assert cooldown["next_retry_at"] == "2026-07-07T17:30:00"
    assert cooldown["remaining_seconds"] == 75 * 60
    assert cooldown["screening_run_id"] == "20260707_152000_sync"


def test_failed_screening_retry_cooldown_expires_and_is_signature_scoped() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_failed_screening_retry_cooldown_minutes=120,
    )
    state_payload = {
        "last_runs": {
            "scheduled_daily_bond_refresh": {
                "status": "failed",
                "finished_at": "2026-07-07T15:30:00",
                "failure_signature": "workflow_subprocess_failed",
                "error": "workflow_subprocess_failed exit=1",
                "failed_effective_nav_snapshot": {
                    "candidate_date": "2026-07-06",
                    "signature": "sig-failed",
                },
            }
        }
    }
    active = scheduler._failed_screening_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-failed"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T16:00:00"),
    )
    assert active["active"] is True
    assert active["skip_reason"] == "failed_screening_retry_cooldown"
    assert active["failure_signature"] == "workflow_subprocess_failed"
    assert active["remaining_seconds"] == 90 * 60

    expired = scheduler._failed_screening_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-failed"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T17:31:00"),
    )
    assert expired["active"] is False

    new_signature = scheduler._failed_screening_retry_cooldown(
        SimpleNamespace(candidate_date="2026-07-06", signature="sig-new"),
        state_payload=state_payload,
        now=datetime.fromisoformat("2026-07-07T16:00:00"),
    )
    assert new_signature["active"] is False
    assert new_signature["next_retry_at"] is None


def test_scheduler_startup_catch_up_spaces_blocked_same_snapshot_retries() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=23,
        scheduler_daily_minute=59,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=23,
        scheduler_weekly_minute=59,
        scheduler_blocked_snapshot_retry_cooldown_minutes=120,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: datetime.fromisoformat(  # type: ignore[method-assign]
        "2026-07-07T15:30:00"
    )
    scheduler._load_last_processed_screening_snapshot = lambda _run_id: {}  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {"last_runs": _blocked_snapshot_last_runs()}
    )

    current_snapshot = SimpleNamespace(
        ready=True,
        candidate_date="2026-07-06",
        signature="sig-blocked",
    )
    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=current_snapshot),
        patch(
            "backend.scheduler.load_latest_screened_db_snapshot",
            return_value={"candidate_date": "2026-07-05", "signature": "sig-old"},
        ),
        patch(
            "backend.scheduler.screening_snapshot_changed",
            return_value=(True, "effective_nav_snapshot_advanced_without_collection"),
        ),
        patch("backend.scheduler.load_publication_registry", return_value={}),
    ):
        during_cooldown = scheduler._resolve_startup_catch_up_jobs(
            datetime.fromisoformat("2026-07-07T16:00:00")
        )
        after_cooldown = scheduler._resolve_startup_catch_up_jobs(
            datetime.fromisoformat("2026-07-07T17:31:00")
        )

    assert during_cooldown == []
    assert after_cooldown == ["intraday_refresh"]


def test_snapshot_watchdog_reports_blocked_snapshot_retry_cooldown() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_blocked_snapshot_retry_cooldown_minutes=120,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        scheduler_intraday_nav_history_sync_enabled=True,
        scheduler_intraday_nav_history_sync_limit=0,
        scheduler_intraday_long_term_history_enabled=False,
        scheduler_intraday_long_term_history_limit=0,
        scheduler_intraday_long_term_history_refresh_stale=False,
    )
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._load_last_processed_screening_snapshot = lambda _run_id: {}  # type: ignore[method-assign]
    finished_at = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="seconds")
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": _blocked_snapshot_last_runs(finished_at=finished_at)
        }
    )
    current_snapshot = SimpleNamespace(
        ready=True,
        candidate_date="2026-07-06",
        signature="sig-blocked",
    )

    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=current_snapshot),
        patch(
            "backend.scheduler.load_latest_screened_db_snapshot",
            return_value={"candidate_date": "2026-07-05", "signature": "sig-old"},
        ),
        patch(
            "backend.scheduler.screening_snapshot_changed",
            return_value=(True, "effective_nav_snapshot_advanced_without_collection"),
        ),
        patch("backend.scheduler.load_publication_registry", return_value={}),
        patch.object(ProjectScheduler, "_execute_scheduled_refresh") as execute_refresh,
    ):
        result = scheduler._execute_snapshot_watchdog()

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "blocked_snapshot_retry_cooldown"
    assert result["candidate_date"] == "2026-07-06"
    assert result["signature"] == "sig-blocked"
    assert result["next_retry_at"]
    assert result["remaining_seconds"] > 0
    execute_refresh.assert_not_called()


def test_post_task_catch_up_does_not_enqueue_during_blocked_snapshot_cooldown() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=0,
        scheduler_daily_minute=0,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=0,
        scheduler_intraday_minute=1,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=0,
        scheduler_weekly_minute=0,
        scheduler_blocked_snapshot_retry_cooldown_minutes=120,
        database_path=Path("/tmp/unused.db"),
        latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
    )
    scheduler.stop_event = threading.Event()
    scheduler._manual_run_lock = threading.Lock()
    scheduler._post_task_catch_up_lock = threading.Lock()
    scheduler._log = lambda _message: None  # type: ignore[method-assign]
    scheduler._load_last_processed_screening_snapshot = lambda _run_id: {}  # type: ignore[method-assign]
    finished_at = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="seconds")
    state_payload = {
        "current_task": None,
        "pending_manual_run_requests": [],
        "last_runs": _blocked_snapshot_last_runs(
            candidate_date="2026-07-09",
            finished_at=finished_at,
        ),
    }
    enqueue_request = Mock(return_value={"accepted": True})
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: state_payload,
        enqueue_unique_manual_run_request=enqueue_request,
    )
    current_snapshot = SimpleNamespace(
        ready=True,
        candidate_date="2026-07-09",
        signature="sig-blocked",
    )

    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=current_snapshot),
        patch(
            "backend.scheduler.load_latest_screened_db_snapshot",
            return_value={"candidate_date": "2026-07-08", "signature": "sig-old"},
        ),
        patch(
            "backend.scheduler.screening_snapshot_changed",
            return_value=(True, "effective_nav_snapshot_advanced_without_collection"),
        ),
        patch("backend.scheduler.load_publication_registry", return_value={}),
    ):
        scheduler._enqueue_post_task_catch_up_if_needed(
            completed_task_name="scheduled_intraday_bond_refresh"
        )

    enqueue_request.assert_not_called()


def test_scheduler_singleton_lease_blocks_second_pid_namespace_instance(
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        scheduler_lock_file=tmp_path / "scheduler.lease",
        scheduler_pid_file=tmp_path / "scheduler.pid",
        scheduler_state_file=tmp_path / "scheduler_state.json",
        scheduler_heartbeat_file=tmp_path / "scheduler_heartbeat.json",
        scheduler_host_mode="external_process",
    )
    first = ProjectScheduler.__new__(ProjectScheduler)
    first.settings = settings
    first.scheduler_instance_id = "instance-a"
    first._scheduler_lease_handle = None
    first._scheduler_lease_path = None
    second = ProjectScheduler.__new__(ProjectScheduler)
    second.settings = settings
    second.scheduler_instance_id = "instance-b"
    second._scheduler_lease_handle = None
    second._scheduler_lease_path = None

    with patch("backend.scheduler.os.getpid", return_value=1):
        first._ensure_single_instance()
        try:
            with pytest.raises(RuntimeError, match="scheduler_singleton_lease_unavailable"):
                second._ensure_single_instance()
            metadata = json.loads(settings.scheduler_lock_file.read_text(encoding="utf-8"))
            assert metadata["scheduler_instance_id"] == "instance-a"
            assert metadata["pid"] == 1
        finally:
            first._cleanup_pid_file()
            first._release_single_instance()


def test_scheduler_runtime_evidence_ignores_namespace_local_pid_without_instance_heartbeat(
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        scheduler_lock_file=tmp_path / "scheduler.lease",
        scheduler_pid_file=tmp_path / "scheduler.pid",
        scheduler_state_file=tmp_path / "scheduler_state.json",
        scheduler_heartbeat_file=tmp_path / "scheduler_heartbeat.json",
        scheduler_host_mode="external_process",
        scheduler_heartbeat_interval_seconds=60,
        scheduler_heartbeat_stale_after_seconds=180,
    )
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = settings
    scheduler.state_store = SchedulerStateStore(settings.scheduler_state_file)
    scheduler.scheduler_instance_id = "instance-live"
    scheduler._scheduler_lease_handle = None
    scheduler._scheduler_lease_path = None

    with patch("backend.scheduler.os.getpid", return_value=1):
        scheduler._ensure_single_instance()
        try:
            scheduler._mark_state_started()
            scheduler._write_heartbeat_file()
            with patch.object(ProjectScheduler, "_pid_is_alive", return_value=True):
                live = ProjectScheduler._scheduler_runtime_evidence(
                    settings,
                    state_payload=scheduler.state_store.snapshot(),
                )
                assert live["alive"] is True
                assert live["pid_alive_diagnostic"] is True

                heartbeat = json.loads(settings.scheduler_heartbeat_file.read_text(encoding="utf-8"))
                heartbeat["scheduler_instance_id"] = "instance-from-other-namespace"
                settings.scheduler_heartbeat_file.write_text(
                    json.dumps(heartbeat, ensure_ascii=False),
                    encoding="utf-8",
                )
                mismatched = ProjectScheduler._scheduler_runtime_evidence(
                    settings,
                    state_payload=scheduler.state_store.snapshot(),
                )
            assert mismatched["alive"] is False
            assert mismatched["pid_alive_diagnostic"] is True
            assert "scheduler_instance_mismatch" in mismatched["reason_codes"]
        finally:
            scheduler.state_store.mark_stopped(scheduler_instance_id="instance-live")
            scheduler._cleanup_pid_file()
            scheduler._release_single_instance()


def test_load_scheduler_status_does_not_recover_task_history_from_foreign_pid_namespace(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    database_path = tmp_path / "bond_funds.db"
    state_file.write_text(
        json.dumps(
            {
                "scheduler_running": True,
                "pid": 1,
                "scheduler_instance_id": "stale-instance",
                "host_context": "external_scheduler_process",
                "last_heartbeat_at": "2026-07-10T15:10:00",
                "current_task": {
                    "task_name": "scheduled_intraday_bond_refresh",
                    "started_at": "2026-07-10T15:11:00",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    heartbeat_file.write_text(
        json.dumps(
            {
                "scheduler_running": True,
                "pid": 1,
                "scheduler_instance_id": "stale-instance",
                "host_context": "external_scheduler_process",
                "heartbeat_at": "2026-07-10T15:10:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    conn = sqlite3.connect(str(database_path))
    try:
        conn.execute(
            """
            CREATE TABLE task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                duration INTEGER,
                records_processed INTEGER,
                records_inserted INTEGER,
                records_updated INTEGER,
                records_failed INTEGER,
                error_message TEXT,
                error_traceback TEXT,
                extra_data TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO task_history (task_name, status, started_at) VALUES (?, ?, ?)",
            ("scheduled_intraday_bond_refresh", "running", "2026-07-10T15:11:00"),
        )
        conn.commit()
    finally:
        conn.close()

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_lock_file=tmp_path / "scheduler.lease",
        scheduler_pid_file=tmp_path / "scheduler.pid",
        scheduler_enabled=True,
        scheduler_host_mode="external_process",
        scheduler_heartbeat_interval_seconds=60,
        scheduler_heartbeat_stale_after_seconds=180,
        database_path=database_path,
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_intraday_refresh_enabled=False,
        scheduler_snapshot_watchdog_enabled=False,
    )
    snapshot = SimpleNamespace(
        candidate_date="2026-07-09",
        signature="sig-current",
        ready=True,
        to_dict=lambda: {"candidate_date": "2026-07-09", "signature": "sig-current", "ready": True},
    )
    with (
        patch.object(ProjectScheduler, "_pid_is_alive", return_value=True),
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={}),
        patch.object(ProjectScheduler, "_resolve_startup_catch_up_jobs", return_value=[]),
        patch("backend.scheduler._load_task_schedule_slot_runs", return_value=[]),
    ):
        payload = load_scheduler_status(settings)

    persisted_state = json.loads(state_file.read_text(encoding="utf-8"))
    conn = sqlite3.connect(str(database_path))
    try:
        task_status = conn.execute("SELECT status FROM task_history WHERE id = 1").fetchone()[0]
    finally:
        conn.close()
    assert payload["scheduler_running"] is False
    assert payload["scheduler_state_claimed_running"] is True
    assert payload["scheduler_recovery_deferred"] is True
    assert payload["scheduler_recovery_boundary"] == "exclusive_scheduler_lease_owner_only"
    assert payload["scheduler_runtime_evidence"]["pid_alive_diagnostic"] is True
    assert persisted_state["scheduler_running"] is True
    assert persisted_state["pid"] == 1
    assert persisted_state["current_task"]["task_name"] == "scheduled_intraday_bond_refresh"
    assert task_status == "running"


def test_scheduler_lease_owner_defers_task_recovery_while_workflow_lease_is_active(
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        scheduler_lock_file=tmp_path / "scheduler.lease",
        scheduler_pid_file=tmp_path / "scheduler.pid",
        scheduler_state_file=tmp_path / "scheduler_state.json",
        scheduler_heartbeat_file=tmp_path / "scheduler_heartbeat.json",
        scheduler_host_mode="external_process",
    )
    state_store = SchedulerStateStore(settings.scheduler_state_file)
    state_store._persist(
        {
            **state_store._default_state(),
            "scheduler_running": True,
            "pid": 1,
            "scheduler_instance_id": "dead-scheduler-instance",
            "host_context": "external_scheduler_process",
            "current_task": {
                "task_name": "scheduled_intraday_bond_refresh",
                "started_at": "2026-07-10T15:11:00",
            },
        }
    )
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = settings
    scheduler.state_store = state_store
    scheduler.scheduler_instance_id = "replacement-instance"
    scheduler._scheduler_lease_handle = None
    scheduler._scheduler_lease_path = None
    scheduler.workflow_manager = SimpleNamespace(
        store=SimpleNamespace(has_active_lock=lambda: True)
    )
    scheduler.task_recorder = Mock()
    scheduler._ensure_single_instance()
    try:
        recovery = scheduler._recover_state_if_stale()
    finally:
        scheduler._cleanup_pid_file()
        scheduler._release_single_instance()

    assert recovery["scheduler_recovered"] is True
    assert recovery["recovery_deferred"] is True
    assert recovery["recovery_reason"] == "active_workflow_lease_detected"
    scheduler.task_recorder.recover_stale_tasks.assert_not_called()


def test_post_task_catch_up_does_not_requeue_workflow_busy_skip() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=0,
        scheduler_daily_minute=0,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=0,
        scheduler_intraday_minute=1,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=0,
        scheduler_weekly_minute=0,
        scheduler_workflow_busy_retry_cooldown_seconds=300,
    )
    scheduler.stop_event = threading.Event()
    scheduler._manual_run_lock = threading.Lock()
    scheduler._post_task_catch_up_lock = threading.Lock()
    scheduler._log = lambda _message: None  # type: ignore[method-assign]
    busy_at = (datetime.now() - timedelta(seconds=10)).isoformat(timespec="seconds")
    state_payload = {
        "current_task": None,
        "pending_manual_run_requests": [],
        "last_runs": {
            "scheduled_intraday_bond_refresh": {
                "status": "skipped",
                "skip_reason": "workflow_busy_locked",
                "screening_triggered": False,
                "started_at": busy_at,
                "finished_at": busy_at,
            }
        },
    }
    enqueue_request = Mock(return_value={"accepted": True})
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: state_payload,
        enqueue_unique_manual_run_request=enqueue_request,
    )

    scheduler._enqueue_post_task_catch_up_if_needed(
        completed_task_name="scheduled_intraday_bond_refresh"
    )

    cooldown = scheduler._workflow_busy_retry_cooldown(state_payload=state_payload)
    assert cooldown["active"] is True
    assert cooldown["skip_reason"] == "workflow_busy_retry_cooldown"
    assert cooldown["workflow_busy_skip_reason"] == "workflow_busy_locked"
    enqueue_request.assert_not_called()


def test_scheduler_startup_catch_up_skips_intraday_when_daily_already_satisfied() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
    )
    scheduler._last_run_at = lambda task_name, require_satisfied=False: {  # type: ignore[method-assign]
        "scheduled_daily_bond_refresh": datetime.fromisoformat("2026-04-10T08:30:00"),
        "scheduled_intraday_bond_refresh": None,
        "scheduled_weekly_bond_maintenance": datetime.fromisoformat("2026-04-05T03:30:00"),
    }.get(task_name)

    due = ProjectScheduler._resolve_startup_catch_up_jobs(
        scheduler,
        datetime.fromisoformat("2026-04-10T12:00:00"),
    )

    assert due == []


def test_scheduler_last_refresh_processed_current_snapshot_accepts_intraday_result() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace()
    scheduler._load_last_processed_screening_snapshot = lambda run_id: {}  # type: ignore[method-assign]

    result = ProjectScheduler._last_refresh_processed_current_snapshot(
        scheduler,
        SimpleNamespace(candidate_date="2026-04-10", signature="sig-new"),
        state_payload={
            "last_runs": {
                "scheduled_intraday_bond_refresh": {
                    "status": "success",
                    "screening_triggered": True,
                    "after_effective_nav_snapshot": {
                        "candidate_date": "2026-04-10",
                        "signature": "sig-new",
                    },
                }
            }
        },
    )

    assert result is True


def test_last_run_at_falls_back_to_latest_completed_history_when_last_run_workflow_busy(tmp_path: Path) -> None:
    db_path = tmp_path / "bond_funds.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                extra_data TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO task_history (task_name, status, started_at, completed_at, extra_data)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "scheduled_daily_bond_refresh",
                "success",
                "2026-04-23T07:30:00",
                "2026-04-23T07:50:13",
                "{}",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(database_path=db_path)
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {
            "last_runs": {
                "scheduled_daily_bond_refresh": {
                    "status": "skipped",
                    "skip_reason": "workflow_busy_state_only",
                    "started_at": "2026-04-23T10:21:02",
                    "finished_at": "2026-04-23T10:21:02",
                }
            }
        }
    )

    result = ProjectScheduler._last_run_at(
        scheduler,
        "scheduled_daily_bond_refresh",
        require_satisfied=True,
    )

    assert result == datetime.fromisoformat("2026-04-23T07:50:13")


def test_run_once_supports_intraday_refresh() -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler._recover_state_if_stale = lambda: {}  # type: ignore[method-assign]
    scheduler._log_recovery_summary = lambda recovery: None  # type: ignore[method-assign]
    scheduler._ensure_single_instance = lambda: None  # type: ignore[method-assign]
    scheduler._start_heartbeat = lambda: None  # type: ignore[method-assign]
    scheduler._stop_heartbeat = lambda: None  # type: ignore[method-assign]
    scheduler._cleanup_pid_file = lambda: None  # type: ignore[method-assign]
    scheduler.state_store = SimpleNamespace(mark_started=lambda: None, mark_stopped=lambda: None)
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._run_task = lambda task_name, runner: {"task_name": task_name, **runner()}  # type: ignore[method-assign]

    result = ProjectScheduler.run_once(scheduler, "intraday_refresh")

    assert result["task_name"] == "scheduled_intraday_bond_refresh"
    assert result["status"] == "success"


def test_request_manual_run_accepts_and_records_request(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    state_store = SchedulerStateStore(tmp_path / "manual-run-state.json")
    scheduler.state_store = state_store
    scheduler._manual_run_lock = threading.Lock()
    scheduler._run_task = lambda task_name, runner: {"task_name": task_name, **runner()}  # type: ignore[method-assign]
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success", "started_at": "2026-04-14T21:00:00", "finished_at": "2026-04-14T21:00:02"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]

    result = ProjectScheduler.request_manual_run(scheduler, "daily_refresh", requested_by="test")

    assert result["status"] == "accepted"
    deadline = datetime.now() + timedelta(seconds=2)
    snapshot = {}
    while datetime.now() < deadline:
        snapshot = state_store.snapshot()
        history = snapshot.get("manual_run_history") or []
        if history and history[0].get("status") == "success":
            break
        time.sleep(0.05)
    history = snapshot.get("manual_run_history") or []
    assert history
    assert history[0]["job_id"] == "daily_refresh"
    assert history[0]["requested_by"] == "test"
    assert history[0]["status"] == "success"


def test_request_manual_run_records_workflow_and_publication_tracking_fields(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    state_store = SchedulerStateStore(tmp_path / "manual-run-tracking-state.json")
    scheduler.state_store = state_store
    scheduler._manual_run_lock = threading.Lock()
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]
    scheduler._run_task = lambda task_name, runner: {  # type: ignore[method-assign]
        **runner(),
        "task_name": task_name,
        "status": "degraded",
        "started_at": "2026-04-14T21:00:00",
        "finished_at": "2026-04-14T21:00:02",
        "workflow_run_id": "20260414_210000_sync",
        "status_detail": "publication_blocked_unpublished",
        "warning_reason": "formal_publication_blocked_unpublished",
        "degraded_stage": "formal_publication",
        "publication_result": {
            "status": "blocked",
            "primary_blocker_code": "risk_coverage_insufficient",
        },
    }

    result = ProjectScheduler.request_manual_run(scheduler, "daily_refresh", requested_by="test")

    assert result["status"] == "accepted"
    deadline = datetime.now() + timedelta(seconds=2)
    snapshot = {}
    while datetime.now() < deadline:
        snapshot = state_store.snapshot()
        history = snapshot.get("manual_run_history") or []
        if history and history[0].get("status") == "degraded":
            break
        time.sleep(0.05)
    history = snapshot.get("manual_run_history") or []
    assert history
    assert history[0]["workflow_run_id"] == "20260414_210000_sync"
    assert history[0]["status"] == "degraded"
    assert history[0]["status_detail"] == "publication_blocked_unpublished"
    assert history[0]["result"]["warning_reason"] == "formal_publication_blocked_unpublished"
    assert history[0]["result"]["degraded_stage"] == "formal_publication"
    assert history[0]["publication_result"]["status"] == "blocked"
    assert history[0]["publication_result"]["primary_blocker_code"] == "risk_coverage_insufficient"


def test_finalize_task_uses_explicit_workflow_and_publication_tracking_fields(tmp_path: Path, monkeypatch) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        publication_registry_file=tmp_path / "publication_registry.json",
    )
    scheduler.state_store = SimpleNamespace(
        finish_task=Mock(),
        stage_task_result=Mock(),
        set_jobs=Mock(),
    )
    scheduler.task_recorder = SimpleNamespace(finish_task=Mock(
        side_effect=lambda task_info, **kwargs: kwargs["completion_result_factory"](datetime.now())))
    monkeypatch.setattr("backend.scheduler.seal_task_run_audit", lambda *args: None)
    scheduler._serialize_jobs = lambda: []  # type: ignore[method-assign]
    scheduler._write_task_run_audit = lambda **kwargs: {  # type: ignore[method-assign]
        "artifact_path": "/tmp/task_audit.json",
        "workflow_run_id": None,
    }

    registry = {
        "current_published_run_id": "20260414_205500_sync",
        "automation_layers": {
            "latest_successful_is_currently_published": False,
            "latest_successful_run": {
                "run_id": "20260414_210000_sync",
                "publishable": False,
                "publish_hold": False,
                "publish_hold_reason_codes": [],
                "primary_blocker_code": "risk_coverage_insufficient",
                "primary_blocker_label": "风险覆盖不足",
                "primary_blocker_detail": "0.72 < 0.80",
                "publish_governance_summary": {"status": "blocked"},
                "publish_blockers": [
                    {
                        "code": "risk_coverage_insufficient",
                        "severity": "blocking",
                        "blocking": True,
                    }
                ],
            },
        },
    }

    result = {
        "task_name": "scheduled_intraday_bond_refresh",
        "status": "success",
        "screening_triggered": True,
        "screening_run_id": "20260414_210000_sync",
        "started_at": "2026-04-14T21:00:00",
        "finished_at": "2026-04-14T21:00:02",
    }

    with patch("backend.scheduler.load_publication_registry", return_value=registry):
        finalized = ProjectScheduler._finalize_task(
            scheduler,
            task_info={"id": 1, "started_at": "2026-04-14T21:00:00"},
            task_name="scheduled_intraday_bond_refresh",
            result=result,
        )

    assert finalized["workflow_run_id"] == "20260414_210000_sync"
    assert finalized["task_run_audit_path"] == "/tmp/task_audit.json"
    assert finalized["publication_result"]["status"] == "blocked"
    assert finalized["publication_result"]["primary_blocker_code"] == "risk_coverage_insufficient"
    assert finalized["status"] == "degraded"
    assert finalized["warning_reason"] == "formal_publication_blocked_unpublished"
    assert finalized["degraded_stage"] == "formal_publication"
    assert finalized["status_detail"] == "publication_blocked_unpublished"


def test_finalize_task_does_not_backfill_stale_workflow_tracking_for_skipped_run(tmp_path: Path, monkeypatch) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        publication_registry_file=tmp_path / "publication_registry.json",
    )
    scheduler.state_store = SimpleNamespace(
        finish_task=Mock(),
        stage_task_result=Mock(),
        set_jobs=Mock(),
    )
    scheduler.task_recorder = SimpleNamespace(finish_task=Mock(
        side_effect=lambda task_info, **kwargs: kwargs["completion_result_factory"](datetime.now())))
    monkeypatch.setattr("backend.scheduler.seal_task_run_audit", lambda *args: None)
    scheduler._serialize_jobs = lambda: []  # type: ignore[method-assign]
    scheduler._write_task_run_audit = lambda **kwargs: {  # type: ignore[method-assign]
        "artifact_path": "/tmp/task_audit.json",
        "workflow_run_id": "20260414_210000_sync",
    }

    registry = {
        "current_published_run_id": "20260414_210000_sync",
        "automation_layers": {
            "latest_successful_is_currently_published": True,
            "latest_successful_run": {
                "run_id": "20260414_210000_sync",
                "publishable": True,
                "publish_hold": False,
                "publish_governance_summary": {"status": "publishable"},
            },
        },
    }

    result = {
        "task_name": "scheduled_intraday_bond_refresh",
        "status": "skipped",
        "skip_reason": "effective_nav_snapshot_already_screened",
        "screening_triggered": False,
        "maintenance_rescreen_triggered": False,
        "report_refresh_triggered": False,
        "initial_screening_triggered": False,
        "started_at": "2026-04-14T21:00:00",
        "finished_at": "2026-04-14T21:00:02",
    }

    with patch("backend.scheduler.load_publication_registry", return_value=registry):
        finalized = ProjectScheduler._finalize_task(
            scheduler,
            task_info={"id": 1, "started_at": "2026-04-14T21:00:00"},
            task_name="scheduled_intraday_bond_refresh",
            result=result,
        )

    assert "workflow_run_id" not in finalized
    assert "publication_result" not in finalized
    assert "status_detail" not in finalized
    assert finalized["task_run_audit_path"] == "/tmp/task_audit.json"


def test_scheduler_state_store_clears_stale_workflow_tracking_for_skipped_manual_run(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    store = SchedulerStateStore(state_file)
    store._persist(
        {
            **store._default_state(),
            "manual_run_history": [
                {
                    "request_id": "req-skipped",
                    "job_id": "intraday_refresh",
                    "status": "skipped",
                    "workflow_run_id": "20260512_114909_sync",
                    "status_detail": "success_and_published",
                    "publication_result": {
                        "status": "published",
                        "run_id": "20260512_114909_sync",
                    },
                    "result": {
                        "status": "skipped",
                        "screening_triggered": False,
                        "report_refresh_triggered": False,
                        "skip_reason": "effective_nav_snapshot_already_screened",
                    },
                }
            ],
        }
    )

    snapshot = store.snapshot()
    history = snapshot.get("manual_run_history") or []

    assert len(history) == 1
    assert history[0]["request_id"] == "req-skipped"
    assert "workflow_run_id" not in history[0]
    assert "status_detail" not in history[0]
    assert "publication_result" not in history[0]


def test_scheduler_state_store_normalizes_legacy_publish_hold_as_blocked(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    store = SchedulerStateStore(state_file)
    legacy_publication_result = {
        "status": "publish_hold",
        "run_id": "20260624_135756_sync",
        "current_published_run_id": "20260624_081226_sync",
        "latest_successful_is_currently_published": False,
        "publishable": False,
        "publish_hold": True,
        "publish_hold_reason_codes": ["publish_hold_by_contraction_guard"],
        "primary_blocker_code": "publish_hold_by_contraction_guard",
        "primary_blocker_label": "发布暂缓",
        "publish_governance_status": "held",
        "publish_governance_summary": {
            "publishable": False,
            "publish_blocked": True,
            "blocking_total": 0,
            "warning_total": 1,
            "highest_severity": "warning",
            "status": "held",
            "primary_blocker_code": "publish_hold_by_contraction_guard",
            "primary_blocker_label": "发布暂缓",
        },
        "publish_blockers": [
            {
                "code": "publish_hold_by_contraction_guard",
                "label": "发布暂缓",
                "severity": "warning",
                "blocking": False,
            }
        ],
    }
    store._persist(
        {
            **store._default_state(),
            "manual_run_history": [
                {
                    "request_id": "req-hold",
                    "job_id": "intraday_refresh",
                    "status": "success",
                    "status_detail": "success_but_publish_hold",
                    "publication_result": legacy_publication_result,
                    "result": {"status": "success"},
                }
            ],
            "last_runs": {
                "scheduled_intraday_bond_refresh": {
                    "task_name": "scheduled_intraday_bond_refresh",
                    "status": "success",
                    "status_detail": "success_but_publish_hold",
                    "publication_result": legacy_publication_result,
                }
            },
        }
    )

    snapshot = store.snapshot()
    history = snapshot.get("manual_run_history") or []
    last_run = (snapshot.get("last_runs") or {})["scheduled_intraday_bond_refresh"]

    assert history[0]["status"] == "degraded"
    assert history[0]["status_detail"] == "publication_blocked_unpublished"
    assert history[0]["warning_reason"] == "formal_publication_blocked_unpublished"
    assert history[0]["degraded_stage"] == "formal_publication"
    assert history[0]["result"]["status"] == "degraded"
    assert history[0]["publication_result"]["status"] == "blocked"
    assert history[0]["publication_result"]["publish_hold"] is False
    assert history[0]["publication_result"]["legacy_publish_hold"] is True
    assert history[0]["publication_result"]["publish_blockers"][0]["severity"] == "blocking"
    assert history[0]["publication_result"]["publish_blockers"][0]["blocking"] is True
    assert history[0]["publication_result"]["publish_governance_summary"]["status"] == "blocked"
    assert history[0]["publication_result"]["publish_governance_summary"]["blocking_total"] == 1
    assert last_run["status"] == "degraded"
    assert last_run["status_detail"] == "publication_blocked_unpublished"
    assert last_run["publication_result"]["status"] == "blocked"


def test_request_manual_run_rejects_when_scheduler_busy(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    state_store = SchedulerStateStore(tmp_path / "manual-run-busy-state.json")
    state_store._persist(
        {
            **state_store._default_state(),
            "current_task": {
                "task_name": "scheduled_daily_bond_refresh",
                "started_at": "2026-04-14T21:00:00",
            },
        }
    )
    scheduler.state_store = state_store
    scheduler._manual_run_lock = threading.Lock()
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]

    result = ProjectScheduler.request_manual_run(scheduler, "intraday_refresh", requested_by="test")

    assert result["status"] == "rejected"
    assert result["error"] == "scheduler_busy"
    history = state_store.snapshot().get("manual_run_history") or []
    assert history
    assert history[0]["status"] == "rejected"
    assert history[0]["job_id"] == "intraday_refresh"


def test_enqueue_manual_run_request_accepts_when_external_scheduler_idle(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.state_store = SchedulerStateStore(tmp_path / "manual-run-queue-state.json")
    scheduler._manual_run_lock = threading.Lock()
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]

    result = ProjectScheduler.enqueue_manual_run_request(scheduler, "intraday_refresh", requested_by="scheduler_cli")

    assert result["status"] == "accepted"
    assert result["execution_mode"] == "external_scheduler_queue"
    snapshot = scheduler.state_store.snapshot()
    pending = snapshot.get("pending_manual_run_requests") or []
    assert pending
    assert pending[0]["request_id"] == result["request_id"]
    assert pending[0]["job_id"] == "intraday_refresh"


def test_enqueue_manual_run_request_rejects_duplicate_pending_job(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.state_store = SchedulerStateStore(tmp_path / "manual-run-queue-duplicate-state.json")
    scheduler._manual_run_lock = threading.Lock()
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]

    first = ProjectScheduler.enqueue_manual_run_request(scheduler, "intraday_refresh", requested_by="scheduler_cli")
    second = ProjectScheduler.enqueue_manual_run_request(scheduler, "intraday_refresh", requested_by="api")

    assert first["status"] == "accepted"
    assert second["status"] == "rejected"
    assert second["error"] == "scheduler_request_already_pending"
    snapshot = scheduler.state_store.snapshot()
    pending = snapshot.get("pending_manual_run_requests") or []
    assert len(pending) == 1
    assert pending[0]["request_id"] == first["request_id"]


def test_enqueue_manual_run_request_accepts_only_one_duplicate_job_under_concurrency(tmp_path: Path) -> None:
    state_file = tmp_path / "manual-run-queue-concurrent-state.json"
    results: list[dict[str, object]] = []

    def _worker() -> None:
        scheduler = ProjectScheduler.__new__(ProjectScheduler)
        scheduler.state_store = SchedulerStateStore(state_file)
        scheduler._manual_run_lock = threading.Lock()
        scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
        scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"}  # type: ignore[method-assign]
        scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]
        results.append(ProjectScheduler.enqueue_manual_run_request(scheduler, "daily_refresh", requested_by="scheduler_cli"))

    threads = [threading.Thread(target=_worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    accepted = [item for item in results if item.get("status") == "accepted"]
    rejected = [item for item in results if item.get("status") == "rejected"]

    assert len(accepted) == 1
    assert len(rejected) == 5
    assert {item.get("error") for item in rejected} == {"scheduler_request_already_pending"}

    snapshot = SchedulerStateStore(state_file).snapshot()
    pending = snapshot.get("pending_manual_run_requests") or []
    assert len(pending) == 1
    assert pending[0]["request_id"] == accepted[0]["request_id"]


def test_drain_pending_manual_run_requests_executes_same_request_id(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    state_store = SchedulerStateStore(tmp_path / "manual-run-drain-state.json")
    scheduler.state_store = state_store
    scheduler.logger = lambda message: None  # type: ignore[attr-defined]
    scheduler._manual_run_lock = threading.Lock()
    scheduler._run_task = lambda task_name, runner, **kwargs: {"task_name": task_name, **runner()}  # type: ignore[method-assign]
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success", "started_at": "2026-04-21T14:00:00", "finished_at": "2026-04-21T14:00:02"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success", "started_at": "2026-04-21T14:00:00", "finished_at": "2026-04-21T14:00:02"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]
    request = ProjectScheduler.enqueue_manual_run_request(scheduler, "intraday_refresh", requested_by="scheduler_cli")

    ProjectScheduler._drain_pending_manual_run_requests_once(scheduler)

    deadline = datetime.now() + timedelta(seconds=2)
    snapshot = {}
    while datetime.now() < deadline:
        snapshot = state_store.snapshot()
        history = snapshot.get("manual_run_history") or []
        if history and history[0].get("request_id") == request["request_id"] and history[0].get("status") == "success":
            break
        time.sleep(0.05)

    snapshot = state_store.snapshot()
    history = snapshot.get("manual_run_history") or []
    assert history
    assert history[0]["request_id"] == request["request_id"]
    assert history[0]["job_id"] == "intraday_refresh"
    assert history[0]["requested_by"] == "scheduler_cli"
    assert history[0]["status"] == "success"
    assert snapshot.get("pending_manual_run_requests") == []


def test_drain_pending_manual_run_requests_marks_request_running_before_completion(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    state_store = SchedulerStateStore(tmp_path / "manual-run-drain-running-state.json")
    scheduler.state_store = state_store
    scheduler.logger = lambda message: None  # type: ignore[attr-defined]
    scheduler._manual_run_lock = threading.Lock()
    running_seen = threading.Event()
    release_runner = threading.Event()

    def _run_task(task_name, runner, **kwargs):
        deadline = datetime.now() + timedelta(seconds=2)
        while datetime.now() < deadline:
            history = state_store.snapshot().get("manual_run_history") or []
            if history and history[0].get("status") == "running":
                running_seen.set()
                break
            time.sleep(0.01)
        release_runner.wait(timeout=2)
        return {"task_name": task_name, **runner()}

    scheduler._run_task = _run_task  # type: ignore[method-assign]
    scheduler._execute_daily_refresh = lambda: {"task_name": "scheduled_daily_bond_refresh", "status": "success"}  # type: ignore[method-assign]
    scheduler._execute_intraday_refresh = lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success", "started_at": "2026-04-21T14:00:00", "finished_at": "2026-04-21T14:00:02"}  # type: ignore[method-assign]
    scheduler._execute_weekly_maintenance = lambda: {"task_name": "scheduled_weekly_bond_maintenance", "status": "success"}  # type: ignore[method-assign]

    request = ProjectScheduler.enqueue_manual_run_request(scheduler, "intraday_refresh", requested_by="scheduler_cli")

    ProjectScheduler._drain_pending_manual_run_requests_once(scheduler)

    assert running_seen.wait(timeout=2) is True
    running_history = state_store.snapshot().get("manual_run_history") or []
    assert running_history[0]["request_id"] == request["request_id"]
    assert running_history[0]["status"] == "running"

    release_runner.set()
    deadline = datetime.now() + timedelta(seconds=2)
    while datetime.now() < deadline:
        history = state_store.snapshot().get("manual_run_history") or []
        if history and history[0].get("status") == "success":
            break
        time.sleep(0.05)

    history = state_store.snapshot().get("manual_run_history") or []
    assert history[0]["status"] == "success"


def test_run_regression_smoke_returns_environment_error_when_pytest_is_unavailable(tmp_path: Path) -> None:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(project_root=tmp_path)
    scheduler._project_root = lambda: tmp_path  # type: ignore[method-assign]
    scheduler._python_bin = lambda: str(tmp_path / ".venv" / "bin" / "python")  # type: ignore[method-assign]
    scheduler._python_has_module = lambda python_bin, module_name: False  # type: ignore[method-assign]

    summary = ProjectScheduler._run_regression_smoke(
        scheduler,
        groups=["scheduler_guard", "dashboard_payload"],
    )

    assert summary["status"] == "degraded"
    assert summary["overall_status"] == "failed"
    assert summary["environment_error"] == "missing_pytest_dependency"
    assert summary["failed_group_total"] == 2
    assert summary["group_results"][0]["failure_reason"] == "missing_pytest_dependency"


def test_run_task_skips_when_scheduler_execution_lock_is_busy(tmp_path: Path) -> None:
    from backend.task_runtime import SqliteTaskRecorder

    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")
    scheduler.state_store = SchedulerStateStore(tmp_path / "manual-run-global-lock-state.json")
    scheduler.task_recorder = SqliteTaskRecorder(tmp_path / "tasks.db")
    scheduler.logger = Mock()
    scheduler._ensure_runtime_host_allowed = lambda: None
    scheduler._manual_run_lock = threading.Lock()
    scheduler._manual_run_lock.acquire()

    try:
        result = ProjectScheduler._run_task(
            scheduler,
            "scheduled_intraday_bond_refresh",
            lambda: {"task_name": "scheduled_intraday_bond_refresh", "status": "success"},
        )
    finally:
        scheduler._manual_run_lock.release()

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "scheduler_busy"
    assert result["task_run_audit_status"] == "written"
    assert scheduler.state_store.snapshot()["recent_busy_skips"][0] == result


def test_scheduler_state_snapshot_backfills_latest_runs_alias_without_persisting_duplicate(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    state_file.write_text(
        json.dumps(
            {
                "scheduler_running": False,
                "last_runs": {
                    "scheduled_daily_bond_refresh": {
                        "status": "success",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    state_store = SchedulerStateStore(state_file)
    snapshot = state_store.snapshot()
    raw_payload = json.loads(state_file.read_text(encoding="utf-8"))

    assert snapshot["latest_runs"] == snapshot["last_runs"]
    assert "latest_runs" not in raw_payload


def test_scheduler_state_runtime_summary_persists_lightweight_runtime_digests(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    state_store = SchedulerStateStore(state_file)

    state_store.set_runtime_status_summary(
        {
            "automation_host_mode": "external_scheduler_process",
            "host_context": "external_scheduler_process",
            "configured_host_context": "external_scheduler_process",
            "host_context_mismatch": False,
            "effective_nav_snapshot": {"candidate_date": "2026-05-12", "ready": True},
            "latest_screened_snapshot": {"candidate_date": "2026-05-12", "ready": True},
            "task_closures": {"daily_refresh": {"status": "success"}},
            "child_task_idle_streaks": {"weekly_maintenance": {"regression_smoke": {"idle_streak": 1}}},
            "nav_freshness": {"stale": False},
            "maintenance_summary": {"long_term_rank_success_total": 160},
            "schedule_stability_summary": {
                "summary": {"task_count": 3, "late_slot_total": 1},
                "tasks": [{"task_key": "weekly_maintenance", "late_slot_count": 1}],
            },
            "automation_closure_digest": {
                "summary": {"task_count": 4},
                "alerts": [{"code": "a1"}],
                "ordered_tasks": [{"task_key": "daily_refresh"}],
            },
            "startup_catch_up_due_jobs": [],
            "startup_catch_up_pending": False,
            "screening_backlog_due": False,
            "screening_backlog_reason": None,
            "configured_job_ids": ["daily_refresh", "weekly_maintenance"],
            "missing_job_ids": [],
            "unexpected_job_ids": [],
            "job_drift_detected": False,
        }
    )

    snapshot = state_store.snapshot(include_compat_aliases=False)
    raw_payload = json.loads(state_file.read_text(encoding="utf-8"))

    assert snapshot["maintenance_summary"]["long_term_rank_success_total"] == 160
    assert snapshot["schedule_stability_summary"]["summary"]["late_slot_total"] == 1
    assert snapshot["automation_closure_digest"]["summary"]["task_count"] == 4
    assert snapshot["automation_host_mode"] == "external_scheduler_process"
    assert snapshot["host_context"] == "external_scheduler_process"
    assert snapshot["configured_host_context"] == "external_scheduler_process"
    assert snapshot["host_context_mismatch"] is False
    assert raw_payload["maintenance_summary"]["long_term_rank_success_total"] == 160
    assert raw_payload["schedule_stability_summary"]["tasks"][0]["task_key"] == "weekly_maintenance"
    assert raw_payload["automation_closure_digest"]["alerts"][0]["code"] == "a1"
    assert raw_payload["automation_host_mode"] == "external_scheduler_process"
    assert raw_payload["host_context"] == "external_scheduler_process"


def test_scheduler_state_store_handles_concurrent_manual_run_history_updates_across_instances(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    SchedulerStateStore(state_file)._persist({"scheduler_running": False, "manual_run_history": []})

    errors: list[str] = []

    def _worker(index: int) -> None:
        store = SchedulerStateStore(state_file)
        try:
            store.append_manual_run_entry(
                {
                    "request_id": f"req-{index}",
                    "requested_at": f"2026-04-21T16:25:{index:02d}",
                    "status": "accepted",
                }
            )
        except Exception as exc:  # pragma: no cover - regression guard
            errors.append(str(exc))

    threads = [threading.Thread(target=_worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    snapshot = SchedulerStateStore(state_file).snapshot()
    history = snapshot.get("manual_run_history") or []
    request_ids = {str(item.get("request_id") or "").strip() for item in history}
    assert len(history) == 10
    assert request_ids == {f"req-{index}" for index in range(10)}


def test_load_scheduler_status_separates_catch_up_from_nav_date_age(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    state_file.write_text(json.dumps({"scheduler_running": False}, ensure_ascii=False), encoding="utf-8")
    heartbeat_file.write_text(json.dumps({"scheduler_running": False}, ensure_ascii=False), encoding="utf-8")

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_enabled=True,
        database_path=tmp_path / "unused.db",
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        scheduler_timezone="Asia/Shanghai",
    )

    snapshot = SimpleNamespace(
        candidate_date="2026-04-08",
        signature="sig-new",
        ready=True,
        to_dict=lambda: {"candidate_date": "2026-04-08", "ready": True},
    )

    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-07", "signature": "sig-old"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
        patch.object(ProjectScheduler, "_resolve_startup_catch_up_jobs", return_value=["daily_refresh"]),
        patch("backend.scheduler.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = datetime.fromisoformat("2026-04-10T16:00:00+08:00")
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        payload = load_scheduler_status(settings)

    assert payload["automation_host_mode"] == "embedded_web_scheduler"
    assert payload["startup_catch_up_pending"] is True
    assert payload["startup_catch_up_due_jobs"] == ["daily_refresh"]
    assert payload["screening_backlog_due"] is True
    assert payload["screening_backlog_reason"] == "effective_nav_snapshot_advanced"
    assert payload["nav_freshness"]["effective_candidate_date"] == "2026-04-08"
    assert payload["nav_freshness"]["today"] == "2026-04-10"
    assert payload["nav_freshness"]["effective_calendar_age_days"] == 2
    assert payload["nav_freshness"]["last_business_day"] is None
    assert payload["nav_freshness"]["lag_days_vs_last_business_day"] is None
    assert payload["nav_freshness"]["stale"] is None
    assert payload["nav_freshness"]["effective_snapshot_ready"] is True
    assert payload["configured_job_ids"] == [
        "daily_refresh",
        "weekly_maintenance",
        "intraday_refresh",
        "snapshot_watchdog",
    ]
    assert payload["missing_job_ids"] == [
        "daily_refresh",
        "weekly_maintenance",
        "intraday_refresh",
        "snapshot_watchdog",
    ]
    assert payload["job_drift_detected"] is True
    assert payload["task_closures"]["daily_refresh"]["action_category"] == "none"
    assert payload["task_closures"]["daily_refresh"]["no_action_category"] is None
    assert payload["task_closures"]["snapshot_watchdog"]["task_name"] == "scheduled_snapshot_watchdog_refresh"


def test_load_task_schedule_slot_runs_assigns_early_intraday_run_to_previous_slot(tmp_path: Path) -> None:
    from backend.scheduler import _load_task_schedule_slot_runs

    db_path = tmp_path / "bond_funds.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                extra_data TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO task_history (task_name, status, started_at, completed_at, extra_data)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "scheduled_intraday_bond_refresh",
                "success",
                "2026-04-23T08:58:54",
                "2026-04-23T09:09:11",
                json.dumps({"screening_triggered": True}, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    settings = SimpleNamespace(
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
    )

    runs = _load_task_schedule_slot_runs(
        db_path,
        settings,
        "intraday_refresh",
        "scheduled_intraday_bond_refresh",
    )

    assert len(runs) == 1
    assert runs[0]["started_at"] == "2026-04-23T08:58:54"
    assert runs[0]["_slot_expected_at"] == "2026-04-22T13:45:00"


def test_select_task_closure_source_prefers_slot_run_over_workflow_busy_last_run() -> None:
    from backend.scheduler import _select_task_closure_source

    slot_runs = {
        "weekly_maintenance": [
            {
                "task_name": "scheduled_weekly_bond_maintenance",
                "status": "success",
                "started_at": "2026-04-19T03:00:00",
                "finished_at": "2026-04-19T03:32:00",
            }
        ]
    }
    last_runs = {
        "scheduled_weekly_bond_maintenance": {
            "task_name": "scheduled_weekly_bond_maintenance",
            "status": "skipped",
            "skip_reason": "workflow_busy_state_only",
            "started_at": "2026-04-23T10:21:06",
            "finished_at": "2026-04-23T10:21:06",
        }
    }

    selected = _select_task_closure_source(
        task_key="weekly_maintenance",
        task_name="scheduled_weekly_bond_maintenance",
        slot_runs=slot_runs,
        last_runs=last_runs,
    )

    assert selected["status"] == "success"
    assert selected["started_at"] == "2026-04-19T03:00:00"


def test_load_scheduler_status_exposes_systemd_service_runtime(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    state_file.write_text(json.dumps({"scheduler_running": True, "pid": 1234}, ensure_ascii=False), encoding="utf-8")
    heartbeat_file.write_text(json.dumps({"scheduler_running": True, "pid": 1234}, ensure_ascii=False), encoding="utf-8")

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_enabled=True,
        database_path=tmp_path / "unused.db",
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=False,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        scheduler_timezone="Asia/Shanghai",
        scheduler_host_mode="external_process",
        scheduler_snapshot_watchdog_enabled=True,
        scheduler_service_unit_name="fund-ana-scheduler.service",
        web_service_unit_name="fund-ana-web.service",
    )

    snapshot = SimpleNamespace(
        candidate_date="2026-04-08",
        signature="sig-new",
        ready=True,
        to_dict=lambda: {"candidate_date": "2026-04-08", "ready": True},
    )
    scheduler_runtime = {
        "unit_name": "fund-ana-scheduler.service",
        "active_state": "active",
        "sub_state": "running",
        "restart_count": 2,
        "exec_main_start_at": "Tue 2026-04-21 10:09:37 CST",
        "running": True,
    }
    web_runtime = {
        "unit_name": "fund-ana-web.service",
        "active_state": "active",
        "sub_state": "running",
        "restart_count": 1,
        "exec_main_start_at": "Tue 2026-04-21 10:22:56 CST",
        "running": True,
    }

    with (
        patch.object(ProjectScheduler, "_pid_is_alive", return_value=True),
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value=None),
        patch("backend.scheduler._read_systemd_service_runtime", side_effect=[scheduler_runtime, web_runtime]),
    ):
        payload = load_scheduler_status(settings)

    assert payload["scheduler_service_runtime"] == scheduler_runtime
    assert payload["web_service_runtime"] == web_runtime
    assert payload["automation_host_mode"] == "external_scheduler_process"


def test_load_scheduler_status_builds_task_closures_from_schedule_slot_run(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    state_file.write_text(
        json.dumps(
            {
                "scheduler_running": True,
                "pid": 1234,
                "last_runs": {
                    "scheduled_intraday_bond_refresh": {
                        "task_name": "scheduled_intraday_bond_refresh",
                        "status": "success",
                        "started_at": "2026-04-20T21:42:50",
                        "finished_at": "2026-04-20T21:52:49",
                        "screening_triggered": True,
                        "screening_reason": "effective_nav_snapshot_advanced_without_collection",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    heartbeat_file.write_text(json.dumps({"scheduler_running": True, "pid": 1234}, ensure_ascii=False), encoding="utf-8")

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_enabled=True,
        database_path=tmp_path / "unused.db",
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        scheduler_timezone="Asia/Shanghai",
        scheduler_host_mode="external_process",
        scheduler_snapshot_watchdog_enabled=True,
        scheduler_service_unit_name="fund-ana-scheduler.service",
        web_service_unit_name="fund-ana-web.service",
    )

    snapshot = SimpleNamespace(
        candidate_date="2026-04-20",
        signature="sig-new",
        ready=True,
        to_dict=lambda: {"candidate_date": "2026-04-20", "ready": True},
    )
    slot_run = {
        "task_name": "scheduled_intraday_bond_refresh",
        "status": "skipped",
        "started_at": "2026-04-20T13:45:00",
        "finished_at": "2026-04-20T13:49:26",
        "skip_reason": "effective_nav_snapshot_already_screened",
        "screening_triggered": False,
    }

    with (
        patch.object(ProjectScheduler, "_pid_is_alive", return_value=True),
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value=None),
        patch("backend.scheduler._read_systemd_service_runtime", return_value={}),
        patch(
            "backend.scheduler._load_task_schedule_slot_runs",
            side_effect=lambda _db_path, _settings, task_key, _task_name, **_kwargs: [slot_run] if task_key == "intraday_refresh" else [],
        ),
    ):
        payload = load_scheduler_status(settings)

    assert payload["task_closures"]["intraday_refresh"]["started_at"] == "2026-04-20T13:45:00"
    assert payload["task_closures"]["intraday_refresh"]["status"] == "skipped"
    assert payload["task_closures"]["intraday_refresh"]["skip_reason"] == "effective_nav_snapshot_already_screened"


def test_load_scheduler_status_prefers_latest_weekly_run_over_stale_schedule_slot(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    state_file.write_text(
        json.dumps(
            {
                "scheduler_running": True,
                "pid": 1234,
                "last_runs": {
                    "scheduled_weekly_bond_maintenance": {
                        "task_name": "scheduled_weekly_bond_maintenance",
                        "status": "success",
                        "started_at": "2026-04-22T16:06:33",
                        "finished_at": "2026-04-22T16:35:56",
                        "report_refresh_triggered": True,
                        "report_refresh_run_id": "20260422_162611_sync",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    heartbeat_file.write_text(json.dumps({"scheduler_running": True, "pid": 1234}, ensure_ascii=False), encoding="utf-8")

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_enabled=True,
        database_path=tmp_path / "unused.db",
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        scheduler_timezone="Asia/Shanghai",
        scheduler_host_mode="external_process",
        scheduler_snapshot_watchdog_enabled=True,
        scheduler_service_unit_name="fund-ana-scheduler.service",
        web_service_unit_name="fund-ana-web.service",
    )

    snapshot = SimpleNamespace(
        candidate_date="2026-04-22",
        signature="sig-new",
        ready=True,
        to_dict=lambda: {"candidate_date": "2026-04-22", "ready": True},
    )
    stale_slot_run = {
        "task_name": "scheduled_weekly_bond_maintenance",
        "status": "success",
        "started_at": "2026-04-19T03:00:00",
        "finished_at": "2026-04-19T03:24:36",
        "report_refresh_triggered": True,
        "report_refresh_run_id": "20260419_031851_sync",
    }

    with (
        patch.object(ProjectScheduler, "_pid_is_alive", return_value=True),
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value=None),
        patch("backend.scheduler._read_systemd_service_runtime", return_value={}),
        patch(
            "backend.scheduler._load_task_schedule_slot_runs",
            side_effect=lambda _db_path, _settings, task_key, _task_name, **_kwargs: [stale_slot_run] if task_key == "weekly_maintenance" else [],
        ),
    ):
        payload = load_scheduler_status(settings)

    weekly = payload["task_closures"]["weekly_maintenance"]
    persisted = json.loads(state_file.read_text(encoding="utf-8"))

    assert weekly["started_at"] == "2026-04-22T16:06:33"
    assert weekly["report_refresh_run_id"] == "20260422_162611_sync"
    assert weekly["scheduled_for"] == "2026-04-19T03:00:00"
    assert weekly["start_delay_seconds"] == 306393
    assert weekly["started_late"] is True
    assert weekly["startup_catch_up_suspected"] is False
    assert persisted["task_closures"]["weekly_maintenance"]["started_at"] == "2026-04-22T16:06:33"
    assert persisted["task_closures"]["weekly_maintenance"]["report_refresh_run_id"] == "20260422_162611_sync"
    assert persisted["task_closures"]["weekly_maintenance"]["scheduled_for"] == "2026-04-19T03:00:00"
    assert persisted["task_closures"]["weekly_maintenance"]["start_delay_seconds"] == 306393
    assert persisted["task_closures"]["weekly_maintenance"]["started_late"] is True
    assert persisted["task_closures"]["weekly_maintenance"]["startup_catch_up_suspected"] is False


def test_load_scheduler_status_backfills_holding_period_no_signal_delta_from_legacy_unknown_field(tmp_path: Path) -> None:
    from backend.config import load_settings

    settings = load_settings(PROJECT_ROOT / "config" / "project.yaml")
    original_state_file = settings.scheduler_state_file
    try:
        state_file = tmp_path / "scheduler_state.json"
        settings.scheduler_state_file = state_file
        state_file.write_text(
            json.dumps(
                {
                    "scheduler_running": True,
                    "pid": 123456,
                    "jobs": [],
                    "last_runs": {
                        "scheduled_daily_bond_refresh": {
                            "status": "success",
                            "maintenance_data_change_summary": {
                                "changed": True,
                                "holding_period_unknown_delta": -3,
                            },
                            "holding_period_change_summary": {
                                "changed": True,
                                "holding_period_unknown_delta": -3,
                            },
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        payload = load_scheduler_status(settings)
    finally:
        settings.scheduler_state_file = original_state_file

    daily = (payload.get("task_closures") or {}).get("daily_refresh") or {}
    assert daily["maintenance_data_change_summary"]["holding_period_no_signal_delta"] == -3
    assert daily["holding_period_change_summary"]["holding_period_no_signal_delta"] == -3


def test_load_scheduler_status_tracks_child_task_idle_streaks(tmp_path: Path) -> None:
    state_file = tmp_path / "scheduler_state.json"
    heartbeat_file = tmp_path / "scheduler_heartbeat.json"
    state_file.write_text(
        json.dumps(
            {
                "scheduler_running": False,
                "child_task_idle_streaks": {
                    "weekly_maintenance": {
                        "future_outlier_monitor": {
                            "idle_streak": 2,
                            "no_action_category": "idle_by_design",
                        }
                    }
                },
                "last_runs": {
                    "scheduled_weekly_bond_maintenance": {
                        "status": "success",
                        "future_outlier_monitor": {
                            "status": "success",
                            "category": "future_outlier_monitor",
                            "idle_reason": "no_candidates_from_latest_audit",
                            "requested_total": 0,
                            "processed_total": 0,
                            "changed_total": 0,
                        },
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    heartbeat_file.write_text(json.dumps({"scheduler_running": False}, ensure_ascii=False), encoding="utf-8")

    settings = SimpleNamespace(
        scheduler_state_file=state_file,
        scheduler_heartbeat_file=heartbeat_file,
        scheduler_enabled=True,
        database_path=tmp_path / "unused.db",
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        latest_report_file=tmp_path / "latest.json",
        scheduler_daily_hour=7,
        scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True,
        scheduler_intraday_hour=13,
        scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun",
        scheduler_weekly_hour=3,
        scheduler_weekly_minute=0,
        scheduler_timezone="Asia/Shanghai",
        scheduler_snapshot_watchdog_enabled=True,
    )

    snapshot = SimpleNamespace(
        candidate_date="2026-04-08",
        signature="sig-new",
        ready=True,
        to_dict=lambda: {
            "candidate_date": "2026-04-08",
            "signature": "sig-new",
            "ready": True,
            "readiness_reason": "ready",
            "candidate_fund_count": 7100,
            "coverage_ratio_vs_total": 0.95,
            "risk_1y_effective_end_date": "2026-04-08",
            "risk_1y_effective_fund_count": 6900,
            "risk_1y_effective_coverage_ratio_vs_candidate": 0.91,
            "raw_max_latest_nav_date": "2026-04-08",
        },
    )

    with (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=snapshot),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={"candidate_date": "2026-04-08", "signature": "sig-new"}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(False, "effective_nav_snapshot_already_screened")),
        patch.object(ProjectScheduler, "_resolve_startup_catch_up_jobs", return_value=[]),
    ):
        payload = load_scheduler_status(settings)

    weekly = (payload.get("task_closures") or {}).get("weekly_maintenance") or {}
    future_outlier = (weekly.get("child_tasks") or {}).get("future_outlier_monitor") or {}
    persisted = json.loads(state_file.read_text(encoding="utf-8"))

    assert future_outlier["no_action_category"] == "idle_by_design"
    assert future_outlier["idle_streak"] == 3
    assert persisted["child_task_idle_streaks"]["weekly_maintenance"]["future_outlier_monitor"]["idle_streak"] == 3


def test_runtime_log_audit_requires_cross_window_or_live_api_confirmation() -> None:
    from scripts.audit_runtime_logs import build_runtime_log_audit_payload, build_text_source

    single_source = build_text_source(
        "task.log",
        kind="recent_task_logs",
        text="2026-06-25 10:00:00 [task-finish] status=failed\n",
        max_lines=20,
    )
    single_payload = build_runtime_log_audit_payload(
        [single_source],
        live_api_summary={"available": True, "automation_attention": False},
    )

    assert single_payload["passed"] is True
    assert single_payload["confirmed_issue_count"] == 0
    assert single_payload["review_item_count"] == 1
    assert single_payload["review_items"][0]["review_reason"] == "single_window_error_signal_not_enough_to_claim_project_bug"

    current_source = build_text_source(
        "docker",
        kind="current_container_logs",
        text='fund-ana-web | "GET /api/demo HTTP/1.1" 500 12\n',
        max_lines=20,
    )
    historical_source = build_text_source(
        "web_8090.out",
        kind="persistent_log_tail",
        text='127.0.0.1 - - "GET /api/demo HTTP/1.1" 500 -\n',
        max_lines=20,
    )
    multi_window_payload = build_runtime_log_audit_payload(
        [current_source, historical_source],
        live_api_summary={"available": True, "automation_attention": False},
    )

    assert multi_window_payload["passed"] is False
    assert multi_window_payload["confirmed_issue_count"] == 1
    assert multi_window_payload["confirmed_issues"][0]["category"] == "http_5xx"
    assert (
        multi_window_payload["confirmed_issues"][0]["confirmation_basis"]
        == "same_normalized_error_signature_seen_in_multiple_log_windows"
    )
    assert multi_window_payload["confirmed_issues"][0]["signature"] == "http_5xx:GET:/api/demo:500"

    different_path_source = build_text_source(
        "web_8090_different.out",
        kind="persistent_log_tail",
        text='127.0.0.1 - - "GET /api/other HTTP/1.1" 500 -\n',
        max_lines=20,
    )
    different_signature_payload = build_runtime_log_audit_payload(
        [current_source, different_path_source],
        live_api_summary={"available": True, "automation_attention": False},
    )

    assert different_signature_payload["passed"] is True
    assert different_signature_payload["confirmed_issue_count"] == 0
    assert different_signature_payload["review_item_count"] == 2
    assert {
        item["signature"] for item in different_signature_payload["review_items"]
    } == {"http_5xx:GET:/api/demo:500", "http_5xx:GET:/api/other:500"}

    live_confirmed_payload = build_runtime_log_audit_payload(
        [single_source],
        live_api_summary={
            "available": True,
            "automation_attention": True,
            "blocking_issue_codes": ["intraday_refresh_failed"],
        },
    )

    assert live_confirmed_payload["passed"] is True
    assert live_confirmed_payload["confirmed_issue_count"] == 0
    assert live_confirmed_payload["review_item_count"] == 1

    related_source = build_text_source(
        "task_7503_scheduled_intraday_bond_refresh.log",
        kind="recent_task_logs",
        text="2026-07-10 15:11:07 [task-finish] status=failed\n",
        max_lines=20,
    )
    related_live_payload = build_runtime_log_audit_payload(
        [related_source],
        live_api_summary={
            "available": True,
            "automation_attention": True,
            "blocking_issue_codes": ["intraday_refresh_failed"],
        },
    )

    assert related_live_payload["passed"] is False
    assert related_live_payload["confirmed_issue_count"] == 1
    assert related_live_payload["confirmed_issues"][0]["confirmation_basis"] == "runtime_log_error_corroborated_by_live_scheduler_attention"


def test_runtime_log_audit_downgrades_failure_recovered_by_later_same_task_run(tmp_path: Path) -> None:
    from scripts.audit_runtime_logs import build_file_source, build_runtime_log_audit_payload

    failed_log = tmp_path / "task_10_scheduled_daily_bond_refresh.log"
    failed_log.write_text(
        "2026-07-13 07:30:00 [task-start] task_id=10 task_name=scheduled_daily_bond_refresh started_at=2026-07-13T07:30:00\n"
        "2026-07-13 07:47:33 scheduled_daily_bond_refresh: Traceback (most recent call last):\n"
        "2026-07-13 07:47:44 [task-finish] status=failed finished_at=2026-07-13T07:47:33\n",
        encoding="utf-8",
    )
    success_log = tmp_path / "task_11_scheduled_daily_bond_refresh.log"
    success_log.write_text(
        "2026-07-13 07:59:33 [task-start] task_id=11 task_name=scheduled_daily_bond_refresh started_at=2026-07-13T07:59:33\n"
        "2026-07-13 08:20:21 [task-finish] status=success finished_at=2026-07-13T08:20:21\n",
        encoding="utf-8",
    )

    payload = build_runtime_log_audit_payload(
        [
            build_file_source(failed_log, project_root=tmp_path, kind="recent_task_logs", max_lines=50),
            build_file_source(success_log, project_root=tmp_path, kind="recent_task_logs", max_lines=50),
        ],
        live_api_summary={"available": True, "automation_attention": False},
    )

    assert payload["passed"] is True
    assert payload["confirmed_issue_count"] == 0
    assert payload["review_item_count"] == 0
    resolved = [item for item in payload["observations"] if item.get("resolved")]
    assert {item["category"] for item in resolved} == {"python_traceback", "task_failed"}
    assert all(
        item["recovery_evidence"]["recovery_task_id"] == "11"
        for item in resolved
    )


def test_incremental_space_reclaim_requires_incremental_auto_vacuum(tmp_path: Path) -> None:
    import sqlite3

    from backend.database_space_maintenance import run_incremental_space_reclaim

    database_path = tmp_path / "default.db"
    with sqlite3.connect(database_path) as conn:
        conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
        conn.executemany("INSERT INTO sample(value) VALUES (?)", [("x" * 1000,) for _ in range(100)])
        conn.commit()

    result = run_incremental_space_reclaim(database_path, min_freelist_pages=0)

    assert result["status"] == "conversion_required"
    assert result["auto_vacuum"] == 0
    assert result["reclaimed_pages"] == 0


def test_incremental_space_reclaim_loops_until_bounded_page_limit(tmp_path: Path) -> None:
    import sqlite3

    from backend.database_space_maintenance import run_incremental_space_reclaim

    database_path = tmp_path / "incremental.db"
    with sqlite3.connect(database_path) as conn:
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("VACUUM")
        conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, payload BLOB)")
        conn.executemany(
            "INSERT INTO sample(payload) VALUES (?)",
            [(b"x" * 3500,) for _ in range(500)],
        )
        conn.commit()
        conn.execute("DELETE FROM sample WHERE id > 10")
        conn.commit()
        freelist_before = int(conn.execute("PRAGMA freelist_count").fetchone()[0])

    result = run_incremental_space_reclaim(
        database_path,
        max_pages=20,
        min_freelist_pages=1,
    )

    assert freelist_before > 20
    assert result["status"] == "reclaimed"
    assert result["attempted_steps"] == 20
    assert result["reclaimed_pages"] == 20
    assert result["freelist_pages_after"] == result["freelist_pages_before"] - 20
    assert result["readonly_probe"] == "ok"
    assert result["full_quick_check"] == "deferred_to_stopped_service_compaction_window"
    with sqlite3.connect(database_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_runtime_log_audit_normalizes_log_prefixes_for_stable_signatures() -> None:
    from scripts.audit_runtime_logs import build_text_source

    docker_source = build_text_source(
        "docker",
        kind="current_container_logs",
        text=(
            "fund-ana-web        | \x1b[33m2026-06-25 12:19:35 - api.autostock_client - "
            "WARNING - AutoStock fund 批量查询存在返回缺失: requested_unique_total=37 "
            "returned_unique_total=13 missing_count=24\x1b[0m\n"
        ),
        max_lines=20,
    )
    persistent_source = build_text_source(
        "web_8090.out",
        kind="persistent_log_tail",
        text=(
            "2026-06-25 12:59:35 - api.autostock_client - WARNING - "
            "AutoStock fund 批量查询存在返回缺失: requested_unique_total=37 "
            "returned_unique_total=13 missing_count=24\n"
        ),
        max_lines=20,
    )
    different_source = build_text_source(
        "web_8090_different.out",
        kind="persistent_log_tail",
        text=(
            "2026-06-25 13:01:00 - api.autostock_client - WARNING - "
            "AutoStock fund 批量查询存在返回缺失: requested_unique_total=10 "
            "returned_unique_total=9 missing_count=1\n"
        ),
        max_lines=20,
    )

    docker_signature = docker_source["events"][0]["signature"]
    persistent_signature = persistent_source["events"][0]["signature"]
    different_signature = different_source["events"][0]["signature"]

    assert docker_signature == persistent_signature
    assert docker_signature.startswith("generic_warning:autostock fund 批量查询存在返回缺失")
    assert different_signature != docker_signature


def _publication_record(
    run_id: str,
    *,
    publishable: bool,
    formal_publication_eligible: bool | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "run_id": run_id,
        "generated_at": (
            f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}T"
            f"{run_id[9:11]}:{run_id[11:13]}:{run_id[13:15]}"
        ),
        "publishable": publishable,
        "risk_publish_ready": True,
        "publish_blockers": [],
        "counts": {"final_count": 10},
    }
    if formal_publication_eligible is not None:
        record["formal_publication_eligible"] = formal_publication_eligible
    return record


def test_validation_only_record_does_not_replace_latest_successful_formal_run() -> None:
    from backend.publication import _build_automation_layers

    formal = _publication_record(
        "20260711_195604_sync", publishable=True, formal_publication_eligible=True
    )
    validation = _publication_record(
        "20260711_225715_sync", publishable=False, formal_publication_eligible=False
    )
    registry = {
        "canonical_runs": [formal],
        "superseded_runs": [validation],
        "current_published_run_id": formal["run_id"],
    }

    with patch("backend.publication._build_database_snapshot_summary", return_value={}):
        layers = _build_automation_layers(SimpleNamespace(), registry)

    assert layers["latest_successful_run"]["run_id"] == formal["run_id"]
    assert layers["latest_successful_is_currently_published"] is True
    assert layers["latest_successful_is_publishable"] is True


def test_legacy_publication_record_without_eligibility_field_remains_candidate() -> None:
    from backend.publication import _build_automation_layers

    legacy = _publication_record("20260711_210000_sync", publishable=False)
    older = _publication_record(
        "20260711_195604_sync", publishable=True, formal_publication_eligible=True
    )
    registry = {
        "canonical_runs": [older],
        "superseded_runs": [legacy],
        "current_published_run_id": older["run_id"],
    }

    with patch("backend.publication._build_database_snapshot_summary", return_value={}):
        layers = _build_automation_layers(SimpleNamespace(), registry)

    assert layers["latest_successful_run"]["run_id"] == legacy["run_id"]
    assert layers["latest_successful_is_currently_published"] is False


def test_collected_validation_report_is_marked_outside_formal_automation(tmp_path: Path) -> None:
    from backend.publication import _collect_run_records

    runs_dir = tmp_path / "reports" / "runs"
    run_dir = runs_dir / "20260711_225715_sync"
    run_dir.mkdir(parents=True)
    report_path = run_dir / "bond_analysis_report_20260711_225715_sync.json"
    report_path.write_text(
        json.dumps(
            {
                "run_id": "20260711_225715_sync",
                "generated_at": "2026-07-11T22:57:15",
                "counts": {"final_count": 292},
                "publication_control": {
                    "mode": "validation_only",
                    "eligible_for_formal_publication": False,
                    "workflow_options": {"reuse_existing_outputs": True},
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )
    settings = SimpleNamespace(
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=tmp_path / "reports" / "archive" / "superseded_runs",
    )

    records = _collect_run_records(settings)

    assert len(records) == 1
    assert records[0]["publication_mode"] == "validation_only"
    assert records[0]["formal_publication_eligible"] is False
