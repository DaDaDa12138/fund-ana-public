#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.scheduler import ProjectScheduler  # noqa: E402


NON_DATABASE_WEEKLY_TASK_OVERRIDES = {
    "scheduler_weekly_nav_history_enabled": False,
    "scheduler_weekly_plain_open_tail_follow_up_enabled": False,
    "scheduler_weekly_future_outlier_monitor_enabled": False,
    "scheduler_weekly_holding_period_edge_audit_enabled": False,
    "scheduler_weekly_nav_history_consistency_audit_enabled": False,
    "scheduler_weekly_long_term_history_enabled": False,
    "scheduler_weekly_regression_smoke_enabled": False,
}


def _disable_non_database_weekly_tasks(settings: Any) -> dict[str, Any]:
    applied: dict[str, Any] = {}
    for attr, value in NON_DATABASE_WEEKLY_TASK_OVERRIDES.items():
        if not hasattr(settings, attr):
            continue
        applied[attr] = getattr(settings, attr)
        setattr(settings, attr, value)
    return applied


def _patch_non_database_runtime_hooks(scheduler: ProjectScheduler) -> None:
    scheduler._build_data_quality_audits_compat = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "data_quality_audit": {
            "status": "skipped",
            "skip_reason": "database_governance_only_runner",
        }
    }
    scheduler._attempt_refresh_risk_free_rate = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "status": "skipped",
        "skip_reason": "database_governance_only_runner",
        "succeeded": True,
    }
    scheduler._build_risk_free_rate_drift_audit = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "status": "skipped",
        "skip_reason": "database_governance_only_runner",
        "alert_triggered": False,
    }


def _status_of(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("status") or "").strip()


def _summary_of(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else {}


def _build_runner_payload(
    *,
    started_at: str,
    finished_at: str,
    result: dict[str, Any],
    disabled_task_original_values: dict[str, Any],
    collection_enabled: bool,
    bond_universe_sync_enabled: bool,
) -> dict[str, Any]:
    governance = result.get("database_governance_health_audit")
    completion = result.get("database_optimization_completion_audit")
    cleanup = result.get("screening_history_cleanup_execution")
    archive = result.get("screening_history_archive_execution")
    archive_rotation = result.get("screening_history_archive_rotation")
    backup_cleanup = result.get("database_backup_cleanup_execution")
    backup_compression = result.get("database_backup_compression_execution")
    nav_archive = result.get("nav_history_archive_preview")
    rank_archive = result.get("rank_history_archive_rolling")
    return {
        "generated_at": finished_at,
        "runner": "database_governance_maintenance",
        "parameters": {
            "collection_enabled": collection_enabled,
            "bond_universe_sync_enabled": bond_universe_sync_enabled,
            "non_database_weekly_tasks_disabled": True,
            "disabled_task_original_values": disabled_task_original_values,
        },
        "summary": {
            "task_status": _status_of(result),
            "degraded": bool(result.get("degraded")),
            "warning_reason": result.get("warning_reason"),
            "degraded_stage": result.get("degraded_stage"),
            "database_governance_status": _status_of(governance),
            "database_governance_blocked_item_count": int(
                _summary_of(governance).get("blocked_item_count") or 0
            ),
            "database_governance_review_item_count": int(
                _summary_of(governance).get("review_item_count") or 0
            ),
            "database_optimization_completion_status": _status_of(completion),
            "database_optimization_completion_ready": bool(
                _summary_of(completion).get("final_completion_ready")
            ),
            "screening_cleanup_status": _status_of(cleanup),
            "screening_cleanup_apply_readiness": str(
                ((cleanup or {}).get("apply_readiness") or {}).get("status") or ""
            ).strip(),
            "screening_cleanup_deleted_total": int(
                _summary_of(cleanup).get("deleted_total") or 0
            ),
            "screening_archive_status": _status_of(archive),
            "screening_archive_rotation_status": _status_of(archive_rotation),
            "nav_archive_status": _status_of(nav_archive),
            "rank_archive_status": _status_of(rank_archive),
            "database_backup_cleanup_status": _status_of(backup_cleanup),
            "database_backup_compression_status": _status_of(backup_compression),
        },
        "started_at": started_at,
        "finished_at": finished_at,
        "result": result,
    }


def _write_runner_artifact(settings: Any, payload: dict[str, Any], timestamp: str | None) -> Path:
    output_dir = Path(settings.reports_dir) / "audit" / "database_governance_health"
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_path = output_dir / f"database_governance_maintenance_runner_{ts}.json"
    artifact_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return artifact_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the weekly database-governance maintenance chain without "
            "external collection or non-database weekly tasks."
        )
    )
    parser.add_argument("--config", default="", help="Project config path.")
    parser.add_argument("--timestamp", default="", help="Artifact timestamp suffix.")
    parser.add_argument(
        "--with-collection",
        action="store_true",
        help="Also run weekly external collection before database governance.",
    )
    parser.add_argument(
        "--with-bond-universe-sync",
        action="store_true",
        help="Also run bond universe sync before database governance.",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    disabled_originals = _disable_non_database_weekly_tasks(settings)
    scheduler = ProjectScheduler(settings=settings, logger=lambda message: print(message, file=sys.stderr))
    _patch_non_database_runtime_hooks(scheduler)

    started_at = datetime.now().isoformat(timespec="seconds")
    result = scheduler._execute_weekly_maintenance(
        collection_enabled=bool(args.with_collection),
        bond_universe_sync_enabled=bool(args.with_bond_universe_sync),
    )
    finished_at = datetime.now().isoformat(timespec="seconds")
    payload = _build_runner_payload(
        started_at=started_at,
        finished_at=finished_at,
        result=result,
        disabled_task_original_values=disabled_originals,
        collection_enabled=bool(args.with_collection),
        bond_universe_sync_enabled=bool(args.with_bond_universe_sync),
    )
    artifact_path = _write_runner_artifact(
        settings,
        payload,
        timestamp=(str(args.timestamp or "").strip() or None),
    )
    output = {
        "status": payload["summary"]["task_status"],
        "artifact_path": str(artifact_path),
        "summary": payload["summary"],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if payload["summary"]["database_governance_status"] == "blocked":
        return 2
    if payload["summary"]["database_optimization_completion_status"] == "blocked":
        return 2
    if payload["summary"]["task_status"] == "failed":
        return 1
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
