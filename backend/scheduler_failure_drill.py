from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from backend.publication_status import build_publication_status_summary
from backend.scheduler import _select_task_closure_source
from backend.scheduler_diagnostics import (
    build_schedule_stability_summary,
    build_scheduler_chain_audit,
    build_scheduler_overview_summary,
    build_scheduler_task_closure_digest,
    build_task_run_artifacts_summary,
)
from scripts.audit_runtime_utils import build_audit_artifact_path


class _DrillProjectScheduler:
    def __init__(self, _settings: Any) -> None:
        pass

    def _build_empty_detail_payload_audit(self) -> dict[str, Any]:
        return {
            "available": False,
            "source": "scheduler_failure_drill",
            "reason": "synthetic_failure_drill",
        }


def _drill_schedule_health(_task_key: str, _run_payload: dict[str, Any], _scheduler_started_at: Any) -> dict[str, Any]:
    return {
        "scheduled_for": None,
        "started_late": False,
        "startup_catch_up_suspected": False,
        "start_delay_seconds": 0,
    }


def _resolve_drill_latest_screened_snapshot(
    run: dict[str, Any] | None,
    _publication: dict[str, Any] | None,
    overview_latest_screened_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    return dict((run or {}).get("latest_screened_snapshot") or overview_latest_screened_snapshot or {})


def _build_drill_task_closure_digest(overview: dict[str, Any] | None) -> dict[str, Any]:
    return build_scheduler_task_closure_digest(
        overview,
        resolve_run_latest_screened_snapshot_fn=_resolve_drill_latest_screened_snapshot,
    )


def _build_drill_schedule_stability_summary(overview: dict[str, Any] | None) -> dict[str, Any]:
    return build_schedule_stability_summary(
        overview,
        build_task_schedule_health_fn=_drill_schedule_health,
    )


def _build_synthetic_failed_scheduler_state() -> dict[str, Any]:
    failed_run = {
        "task_name": "scheduled_daily_bond_refresh",
        "status": "failed",
        "started_at": "2026-04-29T07:30:00",
        "finished_at": "2026-04-29T07:31:05",
        "warning_reason": "workflow_subprocess_failed",
        "action_category": "screening_triggered",
        "screening_triggered": False,
        "report_refresh_triggered": False,
        "logs_tail": [
            "scheduled_daily_bond_refresh started",
            "workflow_subprocess_failed exit=1",
        ],
        "task_log_file": "/app/logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
        "task_log_file_relative": "logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
        "task_log_file_project_path": "logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
        "task_run_audit_json": "/app/reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
        "task_run_audit_json_relative": "reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
        "task_run_audit_json_project_path": "reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
        "task_run_audit": {
            "artifact_path": "/app/reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
            "artifact_path_relative": "reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
            "artifact_project_path": "reports/.audit/scheduler_task_runs/20260429/task_drill_daily_failure.json",
            "task_log_path": "/app/logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
            "task_log_path_relative": "logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
            "task_log_project_path": "logs/tasks/task_drill_scheduled_daily_bond_refresh.log",
            "workflow_run_id": "",
            "status": "failed",
        },
        "before_effective_nav_snapshot": {
            "candidate_date": "2026-04-28",
            "candidate_fund_count": 7304,
            "ready": True,
        },
        "after_effective_nav_snapshot": {
            "candidate_date": "2026-04-28",
            "candidate_fund_count": 7304,
            "ready": True,
        },
        "latest_screened_snapshot": {
            "candidate_date": "2026-04-28",
            "candidate_fund_count": 7304,
            "ready": True,
        },
    }
    return {
        "enabled": True,
        "scheduler_running": True,
        "automation_host_mode": "external_scheduler_process",
        "started_at": "2026-04-29T07:20:00",
        "last_heartbeat_at": "2026-04-29T07:31:10",
        "last_error": None,
        "last_runs": {
            "scheduled_daily_bond_refresh": failed_run,
        },
        "task_schedule_slot_runs": {
            "daily_refresh": [failed_run],
        },
        "task_closures": {
            "daily_refresh": failed_run,
        },
        "jobs": [{"id": "daily_refresh"}],
        "current_task": {},
        "manual_run_history": [],
        "pending_manual_run_requests": [],
        "screening_backlog_due": False,
        "startup_catch_up_pending": False,
        "startup_catch_up_due_jobs": [],
        "configured_job_ids": ["daily_refresh"],
        "missing_job_ids": [],
        "unexpected_job_ids": [],
        "job_drift_detected": False,
        "effective_nav_snapshot": {
            "candidate_date": "2026-04-28",
            "candidate_fund_count": 7304,
            "ready": True,
        },
        "latest_screened_snapshot": {
            "candidate_date": "2026-04-28",
            "candidate_fund_count": 7304,
            "ready": True,
        },
        "nav_freshness": {"raw_latest_nav_date": "2026-04-28"},
    }


def _build_synthetic_publication() -> dict[str, Any]:
    run_id = "20260429_104047_sync"
    return {
        "latest_canonical_run_id": run_id,
        "current_published_run_id": run_id,
        "automation_layers": {
            "latest_successful_run": {
                "run_id": run_id,
                "generated_at": "2026-04-29T10:41:30",
                "publishable": True,
                "published": True,
                "publish_blockers": [],
            },
            "latest_publishable_run": {
                "run_id": run_id,
                "generated_at": "2026-04-29T10:41:30",
            },
            "current_published_run": {
                "run_id": run_id,
                "generated_at": "2026-04-29T10:41:30",
            },
            "latest_successful_is_publishable": True,
            "latest_successful_is_currently_published": True,
            "latest_successful_blockers": [],
        },
    }


def _check(name: str, passed: bool, detail: str, *, severity: str = "blocking") -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "severity": severity,
        "detail": detail,
    }


def build_scheduler_failure_drill_payload(settings: Any | None = None) -> dict[str, Any]:
    settings = settings or SimpleNamespace()
    scheduler_state = _build_synthetic_failed_scheduler_state()
    publication = _build_synthetic_publication()
    overview = build_scheduler_overview_summary(
        scheduler_state,
        settings=settings,
        project_scheduler_cls=_DrillProjectScheduler,
        select_task_closure_source_fn=_select_task_closure_source,
        build_task_schedule_health_fn=_drill_schedule_health,
        resolve_run_latest_screened_snapshot_fn=_resolve_drill_latest_screened_snapshot,
        build_schedule_stability_summary_fn=_build_drill_schedule_stability_summary,
        build_scheduler_task_closure_digest_fn=_build_drill_task_closure_digest,
    )
    chain_audit = build_scheduler_chain_audit(overview)
    artifacts = build_task_run_artifacts_summary(scheduler_state)
    publication_status = build_publication_status_summary(publication)
    closure_digest = overview.get("automation_closure_digest") or {}
    daily_closure = ((closure_digest.get("task_map") or {}).get("daily_refresh") or {})
    daily_artifact = ((artifacts.get("task_map") or {}).get("daily_refresh") or {})
    issue_codes = {
        str(item.get("code") or "")
        for item in chain_audit.get("open_issues") or []
    }
    checks = [
        _check(
            "failed_task_marked_attention",
            daily_closure.get("status") == "failed" and bool(daily_closure.get("attention_needed")),
            "失败任务必须在 automation_closure_digest 中标记 attention_needed。",
        ),
        _check(
            "chain_audit_contains_failure_issue",
            "daily_refresh_failed" in issue_codes,
            "scheduler_chain_audit 必须生成 daily_refresh_failed 问题码。",
        ),
        _check(
            "task_artifacts_exposed",
            bool(daily_artifact.get("task_log_file_project_path"))
            and bool(daily_artifact.get("task_run_audit_json_project_path")),
            "失败任务必须暴露项目相对任务日志和审计 JSON 路径。",
        ),
        _check(
            "publication_status_not_corrupted_by_failed_task",
            publication_status.get("status") == "success_published",
            "合成失败任务不应污染当前已发布版本状态。",
        ),
    ]
    failed_checks = [item for item in checks if not item["passed"] and item["severity"] == "blocking"]
    return {
        "artifact_type": "scheduler_failure_drill",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scenario": "scheduled_daily_failure_with_task_artifacts",
        "passed": not failed_checks,
        "checks": checks,
        "summary": {
            "failed_task_key": "daily_refresh",
            "failed_task_status": daily_closure.get("status"),
            "attention_total": int((closure_digest.get("summary") or {}).get("attention_total") or 0),
            "chain_issue_count": int(chain_audit.get("issue_count") or 0),
            "artifact_task_count": int((artifacts.get("summary") or {}).get("artifact_task_count") or 0),
            "publication_status": publication_status.get("status"),
        },
        "failure_surface": {
            "automation_closure_daily": daily_closure,
            "chain_audit_open_issues": chain_audit.get("open_issues") or [],
            "task_run_artifact_daily": daily_artifact,
            "publication_status_summary": publication_status,
        },
    }


def write_scheduler_failure_drill_report(
    payload: dict[str, Any],
    *,
    reports_dir: Path,
) -> Path:
    output_dir = reports_dir / "audit" / "scheduler_failure_drill"
    output_path = build_audit_artifact_path(output_dir, "scheduler_failure_drill")
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path
