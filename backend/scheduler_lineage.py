"""Preserve the distinction between a guarded skip and an executed workflow."""
from __future__ import annotations

from datetime import datetime
from typing import Any


def daily_screening_evidence(run: dict[str, Any]) -> dict[str, Any]:
    return {key: run.get(key) for key in (
        "status", "screening_triggered", "skip_reason", "started_at", "finished_at",
        "screening_run_id", "report_refresh_run_id",
    )}


def is_busy_skip_without_screening(evidence: dict[str, Any]) -> bool:
    """Require an explicit terminal skip; missing fields do not prove a skip."""
    if (evidence.get("status") != "skipped"
            or evidence.get("screening_triggered") is not False
            or evidence.get("skip_reason") not in {"workflow_busy_locked", "scheduler_busy"}
            or evidence.get("screening_run_id")
            or evidence.get("report_refresh_run_id")):
        return False
    try:
        started = datetime.fromisoformat(str(evidence["started_at"]))
        finished = datetime.fromisoformat(str(evidence["finished_at"]))
        return finished >= started
    except (KeyError, TypeError, ValueError):
        return False


def is_failed_without_completed_screening(evidence: dict[str, Any]) -> bool:
    """A failed attempt does not establish a completed screening run."""
    if (evidence.get("status") != "failed"
            or evidence.get("screening_triggered") is not False and evidence.get("screening_triggered") is not None
            or evidence.get("screening_run_id") or evidence.get("report_refresh_run_id")):
        return False
    try:
        started = datetime.fromisoformat(str(evidence["started_at"]))
        finished = datetime.fromisoformat(str(evidence["finished_at"]))
        return finished >= started
    except (KeyError, TypeError, ValueError):
        return False


def busy_skip_has_current_publication(run: dict[str, Any], overview: dict[str, Any]) -> bool:
    """A completed guard needs no manual rerun once the real publication is fresh."""
    if not is_busy_skip_without_screening(run):
        return False
    effective = overview.get("effective_nav_snapshot") or {}
    layers = overview.get("publication_layers") or {}
    published = layers.get("current_published_run") or {}
    snapshot = published.get("snapshot") or {}
    candidate_date = effective.get("candidate_date")
    return bool(
        candidate_date and effective.get("ready") is True
        and published.get("run_id") and published.get("published") is True
        and published.get("publishable") is True and not published.get("publish_hold")
        and not published.get("publish_blocked")
        and snapshot.get("candidate_date") == candidate_date
        and snapshot.get("risk_1y_effective_end_date") == candidate_date
    )
