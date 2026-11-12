"""Bound automatic retries when period-return collection cannot make progress."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo


def period_collection_retry_cooldown(
    settings: Any,
    current_snapshot: Any,
    *,
    state_payload: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Space no-progress retries without marking blocked inputs as processed.

    Reuse the configured failed-screening retry interval. An observed successful
    batch with unselected work can continue immediately; missing progress data
    never grants that exception. Watchdog skips inherit the original attempt,
    so polling and scheduler restarts cannot extend the retry boundary.
    """
    try:
        minutes = max(0, int(getattr(settings, "scheduler_failed_screening_retry_cooldown_minutes", 120) or 0))
    except (TypeError, ValueError):
        minutes = 120
    snapshot = current_snapshot if isinstance(current_snapshot, dict) else {
        "candidate_date": getattr(current_snapshot, "candidate_date", None),
        "signature": getattr(current_snapshot, "signature", None),
    }
    candidate_date = str(snapshot.get("candidate_date") or "").strip()
    signature = str(snapshot.get("signature") or "").strip()
    result: dict[str, Any] = {
        "active": False, "configured_minutes": minutes, "skip_reason": None,
        "candidate_date": candidate_date or None, "signature": signature or None,
        "last_attempted_at": None, "next_retry_at": None, "remaining_seconds": 0,
        "task_name": None, "saved_fund_count": None, "unselected_count": None,
        "blocking_count": None,
    }
    if not minutes or not candidate_date or not signature:
        return result

    current_time = now or datetime.now()
    latest: tuple[datetime, str, dict[str, Any], bool] | None = None
    for task_name in (
        "scheduled_daily_bond_refresh", "scheduled_intraday_bond_refresh",
        "scheduled_snapshot_watchdog_refresh",
    ):
        entry = ((state_payload or {}).get("last_runs") or {}).get(task_name) or {}
        if not isinstance(entry, dict):
            continue
        inherited = entry.get("status") == "skipped" and entry.get("skip_reason") == "period_collection_retry_cooldown"
        if inherited:
            observed_snapshot = entry
            evidence = entry.get("period_return_collection") or {}
            attempted_values = [entry.get("last_attempted_at")]
        elif entry.get("status") == "degraded" and entry.get("warning_reason") == "period_return_collection_incomplete":
            observed_snapshot = entry.get("after_effective_nav_snapshot") or {}
            evidence = entry.get("period_return_collection") or {}
            attempted_values = [entry.get(key) for key in ("finished_at", "completed_at", "started_at")]
        else:
            continue
        if not isinstance(observed_snapshot, dict) or not isinstance(evidence, dict):
            continue
        if (str(observed_snapshot.get("candidate_date") or "").strip() != candidate_date
                or str(observed_snapshot.get("signature") or "").strip() != signature):
            continue
        attempted_at = None
        for value in attempted_values:
            try:
                attempted_at = datetime.fromisoformat(str(value))
                break
            except (TypeError, ValueError):
                continue
        if attempted_at is None:
            continue
        # Persisted naive task times use the configured project timezone.
        # Convert aware times before comparison; dropping their offset would
        # move an actual UTC attempt by eight hours in the default project.
        zone = ZoneInfo(str(getattr(settings, "timezone", "Asia/Shanghai")))
        if attempted_at.tzinfo is None and current_time.tzinfo is not None:
            attempted_at = attempted_at.replace(tzinfo=zone).astimezone(current_time.tzinfo)
        elif attempted_at.tzinfo is not None and current_time.tzinfo is None:
            attempted_at = attempted_at.astimezone(zone).replace(tzinfo=None)
        progress_counts = [evidence.get(key) for key in ("saved_fund_count", "unselected_count")]
        progress_pending = all(type(value) is int and value > 0 for value in progress_counts)
        if latest is None or attempted_at > latest[0]:
            latest = (attempted_at, str(entry.get("blocked_task_name") or task_name), evidence, progress_pending)
    if latest is None:
        return result

    attempted_at, task_name, evidence, progress_pending = latest
    next_retry_at = attempted_at + timedelta(minutes=minutes)
    active = not progress_pending and current_time < next_retry_at
    result.update({
        "active": active,
        "skip_reason": "period_collection_retry_cooldown" if active else None,
        "last_attempted_at": attempted_at.isoformat(timespec="seconds"),
        "next_retry_at": next_retry_at.isoformat(timespec="seconds") if not progress_pending else None,
        "remaining_seconds": max(0, int((next_retry_at - current_time).total_seconds())) if active else 0,
        "task_name": task_name,
        **{key: evidence.get(key) for key in ("saved_fund_count", "unselected_count", "blocking_count")},
    })
    return result
