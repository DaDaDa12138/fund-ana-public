from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import threading
from unittest.mock import Mock, patch

import pytest

from backend.scheduler_collection_retry import period_collection_retry_cooldown


DAILY = "scheduled_daily_bond_refresh"
WATCHDOG = "scheduled_snapshot_watchdog_refresh"
SNAPSHOT = {"candidate_date": "2026-10-15", "signature": "unchanged-input"}
SETTINGS = SimpleNamespace(scheduler_failed_screening_retry_cooldown_minutes=120)


def blocked_entry(**counts):
    return {
        "status": "degraded", "warning_reason": "period_return_collection_incomplete",
        "started_at": "2026-10-16T07:59:00", "finished_at": "2026-10-16T08:00:00",
        "screening_triggered": False, "after_effective_nav_snapshot": dict(SNAPSHOT),
        "period_return_collection": {
            "saved_fund_count": 0, "unselected_count": 0, "blocking_count": 2, **counts,
        },
    }


def cooldown(entry, *, at="2026-10-16T08:00:01", snapshot=None):
    return period_collection_retry_cooldown(
        SETTINGS, SNAPSHOT if snapshot is None else snapshot,
        state_payload={"last_runs": {DAILY: entry}}, now=datetime.fromisoformat(at),
    )


def test_no_progress_stays_degraded_and_spaces_retry_until_boundary():
    entry = blocked_entry()
    before = deepcopy(entry)
    result = cooldown(entry)
    assert result["active"] is True
    assert result["skip_reason"] == "period_collection_retry_cooldown"
    assert result["next_retry_at"] == "2026-10-16T10:00:00"
    assert result["remaining_seconds"] == 7199
    assert result["blocking_count"] == 2
    assert entry == before
    assert cooldown(entry, at="2026-10-16T10:00:00")["active"] is False


@pytest.mark.parametrize("saved,remaining,expected", [(200, 378, False), (200, 157, False), (155, 0, True), (0, 10, True)])
def test_only_verified_progress_with_remaining_batch_can_continue(saved, remaining, expected):
    assert cooldown(blocked_entry(saved_fund_count=saved, unselected_count=remaining))["active"] is expected


@pytest.mark.parametrize("snapshot", [
    {"candidate_date": "2026-10-15", "signature": "new-input"},
    {"candidate_date": "2026-10-16", "signature": "unchanged-input"},
    {"candidate_date": "2026-10-15", "signature": None},
])
def test_changed_or_unknown_snapshot_is_not_suppressed(snapshot):
    assert cooldown(blocked_entry(), snapshot=snapshot)["active"] is False


def test_missing_progress_counts_do_not_grant_immediate_retry():
    entry = blocked_entry()
    entry["period_return_collection"] = {"blocking_count": 2}
    result = cooldown(entry)
    assert result["active"] is True
    assert result["saved_fund_count"] is None
    assert result["unselected_count"] is None


@pytest.mark.parametrize("value", [True, "200", 1.5, None, -1])
def test_invalid_progress_counts_cannot_disable_cooldown(value):
    assert cooldown(blocked_entry(saved_fund_count=value, unselected_count=200))["active"] is True
    assert cooldown(blocked_entry(saved_fund_count=200, unselected_count=value))["active"] is True


@pytest.mark.parametrize("attempt,observed", [
    ("2026-10-16T00:00:00+00:00", "2026-10-16T08:00:01"),
    ("2026-10-16T08:00:00", "2026-10-16T00:00:01+00:00"),
])
def test_retry_boundary_converts_offsets_to_the_same_instant(attempt, observed):
    entry = blocked_entry()
    entry["finished_at"] = attempt
    assert cooldown(entry, at=observed)["remaining_seconds"] == 7199


def test_watchdog_poll_and_restart_preserve_original_retry_boundary():
    initial = cooldown(blocked_entry())
    skip = {
        **initial, "status": "skipped", "finished_at": "2026-10-16T09:45:00",
        "blocked_task_name": DAILY,
        "period_return_collection": {"saved_fund_count": 0, "unselected_count": 0, "blocking_count": 2},
    }
    # Only the persisted skip remains, as after a later watchdog overwrite.
    state = {"last_runs": {WATCHDOG: skip}}
    result = period_collection_retry_cooldown(
        SETTINGS, SimpleNamespace(**SNAPSHOT), state_payload=state,
        now=datetime.fromisoformat("2026-10-16T09:59:59+08:00"),
    )
    assert result["active"] is True
    assert result["remaining_seconds"] == 1
    assert result["next_retry_at"] == "2026-10-16T10:00:00+08:00"
    assert result["task_name"] == DAILY


def test_newer_progress_is_not_hidden_by_older_failed_task():
    entry = blocked_entry(saved_fund_count=200, unselected_count=157)
    older = blocked_entry()
    older["finished_at"] = "2026-10-16T07:45:00"
    result = period_collection_retry_cooldown(
        SETTINGS, SNAPSHOT, state_payload={"last_runs": {DAILY: entry, WATCHDOG: older}},
        now=datetime.fromisoformat("2026-10-16T08:01:00"),
    )
    assert result["active"] is False
    assert result["next_retry_at"] is None


@pytest.mark.parametrize("change", [{"status": "success"}, {"warning_reason": "other_failure"}, {"after_effective_nav_snapshot": {}}])
def test_unrelated_results_do_not_create_cooldown(change):
    assert cooldown({**blocked_entry(), **change})["active"] is False


def scheduler_fixture(entry):
    from backend.scheduler import ProjectScheduler

    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_daily_hour=0, scheduler_daily_minute=0,
        scheduler_intraday_refresh_enabled=True, scheduler_intraday_hour=0, scheduler_intraday_minute=1,
        scheduler_weekly_day_of_week="sun", scheduler_weekly_hour=0, scheduler_weekly_minute=0,
        scheduler_failed_screening_retry_cooldown_minutes=120,
        database_path=Path("/tmp/unused.db"), latest_report_file=Path("/tmp/latest.json"),
        effective_nav_coverage_ratio=0.9, effective_nav_weekday_only=True,
    )
    scheduler.stop_event = threading.Event()
    scheduler._manual_run_lock = threading.Lock()
    scheduler._post_task_catch_up_lock = threading.Lock()
    scheduler._log = lambda message: None
    scheduler._last_run_at = lambda *args, **kwargs: None
    scheduler._load_last_processed_screening_snapshot = lambda run_id: {}
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.state_store = SimpleNamespace(
        snapshot=lambda: {"current_task": None, "last_runs": {DAILY: entry}},
        enqueue_unique_manual_run_request=Mock(return_value={"accepted": True}),
    )
    return scheduler


def snapshot_patches():
    return (
        patch("backend.scheduler.load_effective_nav_snapshot", return_value=SimpleNamespace(ready=True, **SNAPSHOT)),
        patch("backend.scheduler.load_latest_screened_db_snapshot", return_value={}),
        patch("backend.scheduler.screening_snapshot_changed", return_value=(True, "effective_nav_snapshot_advanced")),
        patch("backend.scheduler.load_publication_registry", return_value={}),
    )


def test_startup_retry_waits_for_boundary_and_unblocks_changed_input():
    scheduler = scheduler_fixture(blocked_entry())
    p1, p2, p3, p4 = snapshot_patches()
    with p1, p2, p3, p4:
        during = scheduler._resolve_startup_catch_up_jobs(datetime.fromisoformat("2026-10-16T08:01:00"))
        expired = scheduler._resolve_startup_catch_up_jobs(datetime.fromisoformat("2026-10-16T10:00:00"))
    assert during == ["weekly_maintenance"]
    assert expired == ["weekly_maintenance", "daily_refresh"]


def test_post_task_does_not_immediately_requeue_same_blocked_input():
    entry = blocked_entry()
    entry["finished_at"] = (datetime.now() - timedelta(minutes=10)).isoformat()
    scheduler = scheduler_fixture(entry)
    p1, p2, p3, p4 = snapshot_patches()
    with p1, p2, p3, p4:
        scheduler._enqueue_post_task_catch_up_if_needed(completed_task_name=DAILY)
    scheduler.state_store.enqueue_unique_manual_run_request.assert_not_called()


def test_watchdog_cannot_bypass_collection_cooldown():
    entry = blocked_entry()
    entry["finished_at"] = (datetime.now() - timedelta(minutes=10)).isoformat()
    scheduler = scheduler_fixture(entry)
    scheduler._execute_scheduled_refresh = Mock(side_effect=AssertionError("must not rerun collection"))
    p1, p2, p3, p4 = snapshot_patches()
    with p1, p2, p3, p4:
        result = scheduler._execute_snapshot_watchdog()
    assert result["status"] == "skipped"
    assert result["skip_reason"] == "period_collection_retry_cooldown"
    assert result["blocked_task_name"] == DAILY
    assert result["last_attempted_at"] == entry["finished_at"].split(".")[0]
    assert result["period_return_collection"]["blocking_count"] == 2
    scheduler._execute_scheduled_refresh.assert_not_called()


@pytest.mark.parametrize("source,expected", [("post_task_catch_up", "post_task_catch_up"), ("manual_cli", "manual_cli"), (None, "external_scheduler_queue")])
def test_queue_preserves_trigger_source_in_execution(source, expected):
    scheduler = scheduler_fixture(blocked_entry())
    scheduler.state_store.claim_next_pending_manual_run_request = lambda: {
        "request_id": "request-1", "job_id": "daily_refresh", "trigger_source": source,
    }
    scheduler.state_store.update_manual_run_entry = Mock()
    runner = Mock()
    scheduler._resolve_job_runner = lambda job_id: (DAILY, runner)
    scheduler._run_task = Mock(return_value={"status": "degraded"})
    with patch("backend.scheduler.threading.Thread", side_effect=lambda target, **kwargs: SimpleNamespace(start=target)):
        scheduler._drain_pending_manual_run_requests_once()
    scheduler._run_task.assert_called_once_with(DAILY, runner, trigger_source=expected)
