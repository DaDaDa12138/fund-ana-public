from __future__ import annotations

import fcntl
import json
import inspect
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import traceback
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.audit_support import build_audit_output_dir
from backend.task_audit_storage import (
    artifact_exists, migration_paths, read_artifact_text_tail, task_log_io_lock, write_task_audit,
)
import backend.scheduler_status_support as scheduler_status_support
import backend.scheduler_diagnostics as scheduler_diagnostics
from backend.config import Settings, load_settings
from backend.database_freshness_summary import (
    build_database_freshness_summary,
    database_freshness_summary_path,
    load_valid_database_freshness_summary,
    summarize_database_freshness_payload,
    write_database_freshness_summary,
)
from backend.database_space_maintenance import run_incremental_space_reclaim
from backend.bond_universe_sync import run_bond_universe_sync
from backend.autostock_nav_tail_fill import (
    _read_missing_latest_nav_candidates,
    build_missing_response_publish_impact_summary,
    load_autostock_nav_tail_fill_snapshot,
    run_autostock_nav_tail_fill,
)
from backend.effective_nav import (
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    load_effective_nav_snapshot,
    load_latest_screened_db_snapshot,
    screening_snapshot_changed,
)
from backend.long_term_history_maintenance import run_long_term_history_maintenance
from backend.nav_lag_audit import build_nav_lag_audit
from backend.nav_freshness import build_nav_freshness
from backend.nav_history_maintenance import (
    DEFAULT_BATCH_SIZE,
    _load_maintenance_state,
    _partition_candidates_by_confirmed_upstream_limited,
    _partition_candidates_by_truncated_history_cooldown,
    _partition_preferred_sync_candidates,
    _read_candidates as _read_nav_history_maintenance_candidates,
    _resolve_preferred_sync_nav_date,
    _rotate_candidates,
    run_nav_history_maintenance,
)
from backend.truncated_history_tail_audit import resolve_confirmed_upstream_limited_codes
from backend.future_outlier_monitor import run_future_outlier_monitor
from backend.plain_open_tail_follow_up import run_plain_open_tail_follow_up
from backend.risk_coverage_gap_audit import build_risk_coverage_gap_audit
from backend.risk_lag_audit import build_risk_lag_audit
from backend.risk_sync_audit import build_risk_sync_audit
from backend.publication import load_publication_registry
from backend.publication_dependencies import configured_publication_blocker, blocked_publication_result
from backend.project_publish_decisions import combine_holding_period_names
from backend.scheduler_state import SchedulerStateStore
from backend.standalone_runner import run_project_pipeline
from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
from backend.task_runtime import SCHEDULER_EXECUTION_ROW_SQL, SqliteTaskRecorder
from backend.scheduler_task_timing import TaskTiming, seal_task_run_audit
from backend.scheduler_maintenance_metrics import restore_weekly_maintenance_metrics
from backend.workflow import WorkflowManager
from backend.core_runner import CoreStepFailedError, TiantianBusinessUnavailableError
from core.utils.holding_period_policy import (
    HOLDING_DAYS_UNKNOWN,
    HOLDING_PERIOD_LIMIT,
    holding_period_gate_reason,
    is_reviewed_no_holding_signal,
    normalize_holding_days,
    resolve_holding_period_days,
)
from utils.nav_history_consistency_audit import (
    build_nav_history_consistency_audit_payload,
    write_nav_history_consistency_audit_artifacts,
)
from utils.fund_nav_history_governance_audit import (
    build_fund_nav_history_governance_audit_payload,
    write_fund_nav_history_governance_audit_artifacts,
)
from utils.nav_history_archive_control_pack import (
    build_nav_history_archive_control_pack_payload,
    write_nav_history_archive_control_pack_artifacts,
)
from utils.nav_history_archive_execution_checklist import (
    build_nav_history_archive_execution_checklist_payload,
    write_nav_history_archive_execution_checklist_artifacts,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    build_nav_history_archive_maintenance_readiness_audit_payload,
    write_nav_history_archive_maintenance_readiness_audit_artifacts,
)
from utils.nav_history_archive_cold_query_compat_audit import (
    build_nav_history_archive_cold_query_compat_audit_payload,
    write_nav_history_archive_cold_query_compat_audit_artifacts,
)
from utils.nav_history_archive_apply_runner import (
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    build_nav_history_archive_binding_health_payload,
    build_nav_history_archive_apply_runner_payload,
    write_nav_history_archive_apply_runner_artifacts,
)
from utils.nav_history_archive_rolling_runner import (
    build_nav_history_archive_rolling_runner_payload,
    write_nav_history_archive_rolling_runner_artifacts,
)
from utils.nav_history_archive_vacuum_runner import (
    build_nav_history_archive_vacuum_runner_payload,
    write_nav_history_archive_vacuum_runner_artifacts,
)
from utils.nav_history_archive_backup import (
    DEFAULT_BACKUP_PREFIX as NAV_HISTORY_ARCHIVE_BACKUP_PREFIX,
    bind_nav_history_archive_backup_to_checklist,
    create_nav_history_archive_backup,
)
from utils.nav_history_archive_maintenance_window_pack import (
    build_nav_history_archive_maintenance_window_pack_payload,
    write_nav_history_archive_maintenance_window_pack_artifacts,
)
from utils.nav_history_archive_preflight import (
    build_nav_history_archive_preflight_payload,
    write_nav_history_archive_preflight_artifacts,
)
from utils.nav_history_archive_runbook import (
    build_nav_history_archive_runbook_payload,
    write_nav_history_archive_runbook_artifacts,
)
from utils.screening_history_cleanup_preview import (
    build_screening_history_cleanup_preview_payload,
    write_screening_history_cleanup_preview_artifacts,
)
from utils.screening_history_cleanup_runbook import (
    build_screening_history_cleanup_runbook_payload,
    write_screening_history_cleanup_runbook_artifacts,
)
from utils.screening_history_cleanup_preflight import (
    build_screening_history_cleanup_preflight_payload,
    write_screening_history_cleanup_preflight_artifacts,
)
from utils.screening_history_cleanup_execution import (
    REQUIRED_APPLY_CONFIRMATION as SCREENING_HISTORY_CLEANUP_APPLY_CONFIRMATION,
    execute_screening_history_cleanup_runbook,
)
from utils.screening_history_cleanup_backup import (
    DEFAULT_BACKUP_PREFIX as SCREENING_HISTORY_CLEANUP_BACKUP_PREFIX,
    create_screening_history_cleanup_backup,
)
from utils.screening_history_archive_preview import (
    build_screening_history_archive_preview_payload,
    write_screening_history_archive_preview_artifacts,
)
from utils.screening_history_archive_execution import (
    execute_screening_history_archive,
)
from utils.screening_history_archive_rotation import (
    build_screening_history_archive_rotation_payload,
    write_screening_history_archive_rotation_artifacts,
)
from utils.screening_storage_growth_audit import (
    build_screening_storage_growth_audit_payload,
    write_screening_storage_growth_artifacts,
)
from utils.database_backup_cleanup_preview import (
    build_database_backup_cleanup_preview_payload,
    write_database_backup_cleanup_preview_artifacts,
)
from utils.database_backup_cleanup_execution import (
    REQUIRED_APPLY_CONFIRMATION as DATABASE_BACKUP_CLEANUP_APPLY_CONFIRMATION,
    execute_database_backup_cleanup,
)
from utils.database_backup_compression_preview import (
    build_database_backup_compression_preview_payload,
    write_database_backup_compression_preview_artifacts,
)
from utils.database_backup_compression_execution import (
    REQUIRED_APPLY_CONFIRMATION as DATABASE_BACKUP_COMPRESSION_APPLY_CONFIRMATION,
    execute_database_backup_compression,
)
from utils.database_governance_health_audit import (
    build_database_governance_health_audit_payload,
    write_database_governance_health_audit_artifacts,
)
from utils.task_history_governance_preview import (
    build_task_history_governance_preview_payload,
    write_task_history_governance_preview_artifacts,
)
from utils.task_history_cleanup_execution import (
    REQUIRED_APPLY_CONFIRMATION as TASK_HISTORY_CLEANUP_APPLY_CONFIRMATION,
    execute_task_history_cleanup,
)
from utils.database_optimization_completion_audit import (
    build_database_optimization_completion_audit_payload,
    write_database_optimization_completion_audit_artifacts,
)
from utils.rank_history_governance_audit import (
    build_rank_history_governance_audit_payload,
    write_rank_history_governance_audit_artifacts,
)
from utils.rank_history_archive_preflight import (
    build_rank_history_archive_preflight_payload,
    write_rank_history_archive_preflight_artifacts,
)
from utils.rank_history_archive_apply_runner import (
    DEFAULT_ARCHIVE_PAGE_LIMIT as DEFAULT_RANK_ARCHIVE_PAGE_LIMIT,
    build_rank_history_archive_apply_runner_payload,
    write_rank_history_archive_apply_runner_artifacts,
)
from utils.rank_history_archive_rolling_runner import (
    build_rank_history_archive_rolling_runner_payload,
    write_rank_history_archive_rolling_runner_artifacts,
)
from utils.rank_history_archive_backup import (
    DEFAULT_BACKUP_PREFIX as RANK_HISTORY_ARCHIVE_BACKUP_PREFIX,
    create_rank_history_archive_backup,
)
from utils.rank_history_archive_vacuum_runner import (
    build_rank_history_archive_vacuum_runner_payload,
    write_rank_history_archive_vacuum_runner_artifacts,
)
from scripts.audit_holding_period_no_signal_edges import run_holding_period_no_signal_edges_audit
from scripts.review_holding_period_high_risk_candidates import run_holding_period_high_risk_review

SCHEDULE_SLOT_TASK_NAMES = {
    "daily_refresh": "scheduled_daily_bond_refresh",
    "intraday_refresh": "scheduled_intraday_bond_refresh",
    "weekly_maintenance": "scheduled_weekly_bond_maintenance",
}

SCHEDULE_SLOT_CLUSTER_GAP_SECONDS = 1800
TASK_NAME_TO_STATUS_PAYLOAD_KEYS = {
    "scheduled_daily_bond_refresh": ("daily_refresh", "daily_run"),
    "scheduled_intraday_bond_refresh": ("intraday_refresh", "intraday_run"),
    "scheduled_weekly_bond_maintenance": ("weekly_maintenance", "weekly_run"),
    "scheduled_snapshot_watchdog_refresh": ("snapshot_watchdog", "snapshot_watchdog_run"),
}
DEFAULT_SCHEDULED_LONG_TERM_STALE_LIMIT = 80

# A task may be associated with a workflow only when the task result carries
# an identifier produced by that task's workflow invocation. The persisted
# workflow status is a shared observation snapshot and must never be used as
# task lineage.
TASK_WORKFLOW_RUN_ID_FIELDS = (
    "workflow_run_id",
    "screening_run_id",
    "maintenance_rescreen_run_id",
    "report_refresh_run_id",
    "initial_screening_run_id",
)


def _task_bound_workflow_run_id(result: dict[str, Any] | None) -> str | None:
    payload = result if isinstance(result, dict) else {}
    for field_name in TASK_WORKFLOW_RUN_ID_FIELDS:
        value = str(payload.get(field_name) or "").strip()
        if value:
            return value
    return None


def _build_intraday_nav_history_sync_skip_summary(
    settings: Settings,
    *,
    limit: int | None,
    allowed_issue_types: list[str],
) -> dict[str, Any]:
    allowed_issue_type_set = {
        str(issue or "").strip()
        for issue in (allowed_issue_types or [])
        if str(issue or "").strip()
    }
    try:
        candidates, stats = _read_nav_history_maintenance_candidates(settings.database_path)
        scoped_candidates = [
            row
            for row in candidates
            if allowed_issue_type_set.intersection(set(row.get("issue_types") or []))
        ]
        state_before = _load_maintenance_state(settings)
        confirmed_upstream_limited_codes = resolve_confirmed_upstream_limited_codes(settings)
        effective_scoped_candidates = list(scoped_candidates)
        confirmed_upstream_limited_skipped_before: list[dict[str, Any]] = []
        if confirmed_upstream_limited_codes:
            effective_scoped_candidates, confirmed_upstream_limited_skipped_before = _partition_candidates_by_confirmed_upstream_limited(
                effective_scoped_candidates,
                confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
            )
        effective_scoped_candidates, truncated_history_cooldown_skipped_before, _cooldown_state = _partition_candidates_by_truncated_history_cooldown(
            effective_scoped_candidates,
            state_before.get("truncated_history_cooldown") or {},
            consume_skip=False,
        )
        preferred_nav_date = _resolve_preferred_sync_nav_date(settings) if effective_scoped_candidates else ""
        preferred_candidates, _non_preferred_candidates = _partition_preferred_sync_candidates(
            effective_scoped_candidates,
            preferred_nav_date=preferred_nav_date,
        )
        if preferred_candidates:
            selected_candidates, next_rotation_index = _rotate_candidates(
                preferred_candidates,
                int(state_before.get("next_index") or 0),
                int(limit or 0),
            )
        else:
            selected_candidates = []
            next_rotation_index = int(state_before.get("next_index") or 0)
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "database_path": str(settings.database_path),
            "status": "skipped",
            "skip_reason": "no_preferred_sync_candidates",
            "selection": {
                "limit": limit,
                "allowed_issue_types": sorted(allowed_issue_type_set),
                "require_preferred_sync_candidates": True,
                "scope_candidate_total": len(scoped_candidates),
                "effective_scope_candidate_total": len(effective_scoped_candidates),
                "rotation_scope_total": len(preferred_candidates),
                "selected_candidate_total": len(selected_candidates),
                "selection_mode": "effective_nav_sync_priority_strict_skipped",
                "preferred_nav_date": preferred_nav_date or None,
                "priority_candidate_total": len(preferred_candidates),
                "confirmed_upstream_limited_total": len(confirmed_upstream_limited_codes),
                "confirmed_upstream_limited_skipped_total": len(confirmed_upstream_limited_skipped_before),
                "truncated_history_cooldown_enabled": any(
                    "truncated_history" in {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
                    for row in scoped_candidates
                ),
                "truncated_history_cooldown_skipped_total": len(truncated_history_cooldown_skipped_before),
                "rotation_next_index": next_rotation_index,
            },
            "maintenance_result": {
                "processed_total": 0,
                "attempted_total": 0,
                "recovered_total": 0,
                "failed_total": 0,
                "nav_records_inserted": 0,
            },
            "audit_before": {
                **stats,
                "candidate_total": len(candidates),
                "sync_candidate_total": len(scoped_candidates),
            },
        }
    except Exception as exc:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "database_path": str(settings.database_path),
            "status": "precheck_error",
            "skip_reason": "precheck_failed",
            "precheck_error": str(exc),
            "selection": {
                "limit": limit,
                "allowed_issue_types": sorted(allowed_issue_type_set),
                "require_preferred_sync_candidates": True,
                "selection_mode": "precheck_failed",
            },
            "maintenance_result": {
                "processed_total": 1,
                "attempted_total": 0,
                "recovered_total": 0,
                "failed_total": 0,
                "nav_records_inserted": 0,
            },
        }


def _build_intraday_autostock_tail_fill_skip_summary(settings: Settings) -> dict[str, Any]:
    business_date = datetime.now().date()
    try:
        candidates, selection_meta = _read_missing_latest_nav_candidates(
            settings.database_path,
            business_date=business_date,
        )
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "batch_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "status": "skipped",
            "skip_reason": "same_day_cooldown_or_no_candidates",
            "source_name": "autostock_fund_batch",
            "selection": {
                "candidate_total": len(candidates),
                "raw_candidate_total": int(selection_meta.get("raw_candidate_total") or len(candidates)),
                "cooldown_skipped_total": int(selection_meta.get("cooldown_skipped_total") or 0),
                "cooldown_skipped_codes_sample": list(selection_meta.get("cooldown_skipped_codes_sample") or []),
                "same_day_missing_response_cooldown_enabled": bool(
                    selection_meta.get("same_day_missing_response_cooldown_enabled")
                ),
                "business_date": str(selection_meta.get("business_date") or business_date.isoformat()),
                "issue_type": "missing_latest_nav_date",
                "recent_nav_max_age_days": 10,
                "candidate_codes_sample": [],
            },
            "batch_meta": {},
            "maintenance_result": {
                "processed_total": 0,
                "returned_total": 0,
                "shadow_fill_ready_total": 0,
                "stale_nav_rejected_total": 0,
                "empty_nav_rejected_total": 0,
                "missing_response_total": 0,
                "fallback_recovered_total": 0,
                "fallback_remaining_missing_total": 0,
                "failed_total": 0,
                "quota_exhausted": False,
                "shadow_fill_ready_codes_sample": [],
                "stale_nav_rejected_codes_sample": [],
                "missing_response_codes_sample": [],
            },
        }
    except Exception as exc:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "batch_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "status": "precheck_error",
            "skip_reason": "precheck_failed",
            "precheck_error": str(exc),
            "source_name": "autostock_fund_batch",
            "selection": {
                "candidate_total": 1,
                "raw_candidate_total": 1,
                "cooldown_skipped_total": 0,
                "cooldown_skipped_codes_sample": [],
                "same_day_missing_response_cooldown_enabled": True,
                "business_date": business_date.isoformat(),
                "issue_type": "missing_latest_nav_date",
                "recent_nav_max_age_days": 10,
                "candidate_codes_sample": [],
            },
            "batch_meta": {},
            "maintenance_result": {
                "processed_total": 1,
                "returned_total": 0,
                "shadow_fill_ready_total": 0,
                "stale_nav_rejected_total": 0,
                "empty_nav_rejected_total": 0,
                "missing_response_total": 0,
                "fallback_recovered_total": 0,
                "fallback_remaining_missing_total": 0,
                "failed_total": 0,
                "quota_exhausted": False,
                "shadow_fill_ready_codes_sample": [],
                "stale_nav_rejected_codes_sample": [],
                "missing_response_codes_sample": [],
            },
        }


def _sanitize_task_log_name(task_name: str) -> str:
    normalized = str(task_name or "").strip().lower()
    if not normalized:
        return "task"
    sanitized_chars = [
        ch if ch.isascii() and (ch.isalnum() or ch in {"_", "-"}) else "_"
        for ch in normalized
    ]
    sanitized = "".join(sanitized_chars).strip("_")
    return sanitized or "task"


def _build_publication_result_for_workflow_run(
    publication: dict[str, Any] | None,
    workflow_run_id: str,
) -> dict[str, Any]:
    normalized_run_id = str(workflow_run_id or "").strip()
    if not normalized_run_id:
        return {}
    registry = dict(publication or {})
    automation_layers = dict(registry.get("automation_layers") or {})
    latest_successful_run = dict(automation_layers.get("latest_successful_run") or {})
    if str(latest_successful_run.get("run_id") or "").strip() != normalized_run_id:
        return {}

    governance = dict(latest_successful_run.get("publish_governance_summary") or {})
    blockers = list(latest_successful_run.get("publish_blockers") or [])
    current_published_run_id = str(registry.get("current_published_run_id") or "").strip()
    latest_is_currently_published = bool(
        automation_layers.get("latest_successful_is_currently_published")
    )
    legacy_publish_hold = bool(latest_successful_run.get("publish_hold"))
    governance_status = str(governance.get("status") or "").strip().lower()
    has_blocking_blocker = any(
        isinstance(item, dict)
        and (
            bool(item.get("blocking"))
            or str(item.get("severity") or "").strip().lower() == "blocking"
        )
        for item in blockers
    )

    if latest_is_currently_published or current_published_run_id == normalized_run_id:
        status = "published"
    elif legacy_publish_hold or governance_status in {"held", "blocked"} or has_blocking_blocker:
        status = "blocked"
    elif bool(latest_successful_run.get("publishable")):
        status = "blocked"
    else:
        status = "unknown"

    return {
        "status": status,
        "run_id": normalized_run_id,
        "current_published_run_id": current_published_run_id or None,
        "latest_successful_is_currently_published": latest_is_currently_published,
        "publishable": bool(latest_successful_run.get("publishable")),
        "publish_hold": False,
        "legacy_publish_hold": legacy_publish_hold,
        "publish_hold_reason_codes": list(
            latest_successful_run.get("publish_hold_reason_codes") or []
        ),
        "primary_blocker_code": str(
            latest_successful_run.get("primary_blocker_code") or ""
        ).strip()
        or None,
        "primary_blocker_label": str(
            latest_successful_run.get("primary_blocker_label") or ""
        ).strip()
        or None,
        "primary_blocker_detail": str(
            latest_successful_run.get("primary_blocker_detail") or ""
        ).strip()
        or None,
        "publish_governance_status": governance_status or None,
        "publish_governance_summary": governance,
        "publish_blockers": blockers,
    }


def _derive_status_detail_for_task_result(
    task_status: str,
    publication_result: dict[str, Any] | None,
    *,
    task_result: dict[str, Any] | None = None,
) -> str | None:
    normalized_task_status = str(task_status or "").strip().lower()
    if normalized_task_status not in {"success", "degraded"}:
        return None
    publication_status = str((publication_result or {}).get("status") or "").strip()
    if publication_status == "published":
        risk_audit = dict((task_result or {}).get("risk_free_rate_audit") or {})
        if (
            normalized_task_status == "degraded"
            and risk_audit.get("risk_metrics_full_recalculation_required")
            and risk_audit.get("risk_metrics_full_recalculation_status") != "triggered"
        ):
            # An earlier workflow can have published before a required second
            # recalculation fails. Preserve both facts in the task outcome.
            return "degraded_and_published"
        return "success_and_published"
    if publication_status == "blocked":
        return "publication_blocked_unpublished"
    return None


def _apply_publication_result_to_task_status(
    result: dict[str, Any],
    publication_result: dict[str, Any] | None,
) -> None:
    if not isinstance(publication_result, dict) or not publication_result:
        return
    task_status = str(result.get("status") or "").strip().lower()
    if task_status not in {"success", "degraded"}:
        return
    publication_status = str(publication_result.get("status") or "").strip().lower()
    if publication_status != "blocked":
        return
    result["status"] = "degraded"
    result["warning_reason"] = (
        str(result.get("warning_reason") or "").strip()
        or "formal_publication_blocked_unpublished"
    )
    result["degraded_stage"] = (
        str(result.get("degraded_stage") or "").strip()
        or "formal_publication"
    )


def _publication_result_counts_as_processed(publication_result: dict[str, Any] | None) -> bool:
    if not isinstance(publication_result, dict) or not publication_result:
        return False
    status = str(publication_result.get("status") or "").strip().lower()
    return status == "published"


def _read_systemd_service_runtime(unit_name: str) -> dict[str, Any]:
    normalized_unit_name = str(unit_name or "").strip()
    if not normalized_unit_name:
        return {}

    properties = [
        "Id",
        "Description",
        "MainPID",
        "ExecMainPID",
        "ExecMainStartTimestamp",
        "ActiveEnterTimestamp",
        "StateChangeTimestamp",
        "InactiveExitTimestamp",
        "ActiveState",
        "SubState",
        "UnitFileState",
        "NRestarts",
    ]
    cmd = ["systemctl", "show", normalized_unit_name]
    for key in properties:
        cmd.extend(["-p", key])
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if result.returncode != 0:
        return {}

    raw: dict[str, str] = {}
    for line in (result.stdout or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        raw[key.strip()] = value.strip()

    def _read_int(key: str) -> int | None:
        text = str(raw.get(key) or "").strip()
        if not text:
            return None
        try:
            value = int(text)
        except (TypeError, ValueError):
            return None
        return value if value >= 0 else None

    def _read_text(key: str) -> str | None:
        text = str(raw.get(key) or "").strip()
        return text or None

    payload = {
        "unit_name": _read_text("Id") or normalized_unit_name,
        "description": _read_text("Description"),
        "main_pid": _read_int("MainPID"),
        "exec_main_pid": _read_int("ExecMainPID"),
        "exec_main_start_at": _read_text("ExecMainStartTimestamp"),
        "active_entered_at": _read_text("ActiveEnterTimestamp"),
        "state_changed_at": _read_text("StateChangeTimestamp"),
        "inactive_exited_at": _read_text("InactiveExitTimestamp"),
        "active_state": _read_text("ActiveState"),
        "sub_state": _read_text("SubState"),
        "unit_file_state": _read_text("UnitFileState"),
        "restart_count": _read_int("NRestarts") or 0,
    }
    payload["running"] = payload["active_state"] == "active" and payload["sub_state"] == "running"
    return payload


def _query_scalar(conn: sqlite3.Connection, sql: str) -> int | str | None:
    row = conn.execute(sql).fetchone()
    return row[0] if row else None


def _load_database_overview_for_scheduler_audit(
    settings: Settings,
    effective_nav_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db_path = Path(settings.database_path)
    effective = dict(effective_nav_snapshot or {})
    autostock_tail_fill = load_autostock_nav_tail_fill_snapshot(settings)
    autostock_publish_impact = build_missing_response_publish_impact_summary(
        settings,
        snapshot=autostock_tail_fill,
    )
    overview = {
        "database_path": str(db_path),
        "stored_fund_count": 0,
        "latest_nav_snapshot_count": 0,
        "nav_history_record_count": 0,
        "nav_history_hot_record_count": 0,
        "nav_history_archive_record_count": 0,
        "nav_history_effective_record_count": 0,
        "nav_history_archive_bound": False,
        "nav_history_archive_cutoff_date": None,
        "risk_metric_record_count": 0,
        "fund_meta_updated_at": None,
        "nav_data_updated_at": None,
        "risk_data_updated_at": None,
        "latest_nav_business_date": effective.get("candidate_date"),
        "risk_1y_business_date": (
            effective.get("risk_1y_effective_end_date")
            or effective.get("risk_1y_latest_end_date")
        ),
        "risk_1y_raw_business_date": None,
        "effective_nav_ready": bool(effective.get("ready")),
        "autostock_shadow_persistent_tracking_ready": bool(
            autostock_tail_fill.get("persistent_missing_response_tracking_ready")
        ),
        "autostock_shadow_cooldown_active_today": bool(
            autostock_tail_fill.get("cooldown_active_today")
        ),
        "autostock_shadow_next_retry_business_date": autostock_tail_fill.get(
            "next_retry_business_date"
        ),
        "autostock_shadow_persistent_missing_response_total": int(
            autostock_tail_fill.get("persistent_missing_response_total") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_total": int(
            autostock_tail_fill.get("severe_persistent_missing_response_total") or 0
        ),
        "autostock_shadow_persistent_missing_response_related_publish_count": int(
            autostock_publish_impact.get("persistent_missing_response_related_publish_count") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_related_publish_count": int(
            autostock_publish_impact.get("severe_persistent_missing_response_related_publish_count") or 0
        ),
    }
    freshness_source = "sidecar"
    freshness_payload = load_valid_database_freshness_summary(settings)
    if freshness_payload is None:
        freshness_source = "live_build"
        freshness_payload = build_database_freshness_summary(settings)
        if not freshness_payload.get("source_db_changed_during_build"):
            try:
                write_database_freshness_summary(settings, freshness_payload)
            except Exception:
                pass
    overview_fields = dict((freshness_payload or {}).get("overview_fields") or {})
    for key in (
        "stored_fund_count",
        "latest_nav_snapshot_count",
        "nav_history_record_count",
        "nav_history_hot_record_count",
        "nav_history_archive_record_count",
        "nav_history_effective_record_count",
        "nav_history_archive_bound",
        "nav_history_archive_cutoff_date",
        "risk_metric_record_count",
        "fund_meta_updated_at",
        "nav_data_updated_at",
        "risk_data_updated_at",
        "risk_1y_raw_business_date",
    ):
        if key in overview_fields and overview_fields.get(key) is not None:
            overview[key] = overview_fields.get(key)
    if not overview["latest_nav_business_date"]:
        overview["latest_nav_business_date"] = overview_fields.get("latest_nav_business_date")
    if not overview["risk_1y_business_date"]:
        overview["risk_1y_business_date"] = overview_fields.get("risk_1y_business_date")
    overview["database_freshness_summary"] = summarize_database_freshness_payload(
        freshness_payload,
        source=freshness_source,
        artifact_path=database_freshness_summary_path(settings),
    )
    return overview


class ProjectScheduler:
    def __init__(
        self,
        settings: Settings | None = None,
        workflow_manager: WorkflowManager | None = None,
        logger: Callable[[str], None] | None = None,
    ):
        self.settings = settings or load_settings()
        self.workflow_manager = workflow_manager or WorkflowManager(self.settings)
        self.logger = logger or print
        self.state_store = SchedulerStateStore(self.settings.scheduler_state_file)
        self.task_recorder = SqliteTaskRecorder(self.settings.database_path)
        self.scheduler = BackgroundScheduler(timezone=self.settings.scheduler_timezone)
        self.stop_event = threading.Event()
        self.heartbeat_thread: threading.Thread | None = None
        self.startup_recovery_thread: threading.Thread | None = None
        self._startup_recovery_started = False
        self._startup_recovery_lock = threading.Lock()
        self._startup_recovery_context = threading.local()
        self._task_log_context = threading.local()
        self._task_log_file_lock = threading.Lock()
        self._manual_run_lock = threading.Lock()
        self._post_task_catch_up_lock = threading.Lock()
        self.scheduler_instance_id = uuid.uuid4().hex
        self._scheduler_lease_handle: Any | None = None
        self._scheduler_lease_path: Path | None = None
        self._started = False

    def _runtime_settings(self) -> Settings:
        current_settings = getattr(self, "settings", None)
        config_path = getattr(current_settings, "config_path", None)
        if not config_path:
            return current_settings
        try:
            return load_settings(config_path)
        except Exception:
            return current_settings

    def _host_context(self) -> str:
        current_settings = getattr(self, "settings", None)
        host_mode = str(getattr(current_settings, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower()
        if host_mode == "external_process":
            return "external_scheduler_process"
        return "embedded_web_scheduler"

    def _runtime_api_config(self) -> dict[str, Any]:
        current_settings = getattr(self, "settings", None)
        if current_settings is None:
            return {}
        return {
            "schema_version": 1,
            "source": "scheduler_process_settings",
            "host_context": self._host_context(),
            "scheduler_instance_id": self._ensure_scheduler_instance_id(),
            "scheduler_lock_file": str(self._scheduler_lock_path(current_settings)),
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "config_file": self._project_relative_path(
                getattr(current_settings, "config_path", None),
            ),
            "tiantian_base_url": str(getattr(current_settings, "tiantian_base_url", "") or "").strip(),
            "autostock_base_url": str(getattr(current_settings, "autostock_base_url", "") or "").strip(),
            "autostock_enabled": getattr(current_settings, "autostock_enabled", True),
            "publication_validation_provider": getattr(current_settings, "publication_validation_provider", "autostock"),
            "tiantian_valuation_primary_url": getattr(current_settings, "tiantian_valuation_primary_url", ""),
            "tiantian_valuation_fallback_url": getattr(current_settings, "tiantian_valuation_fallback_url", ""),
            "eastmoney_fund_info_url": getattr(current_settings, "eastmoney_fund_info_url", ""),
            "validation_supplier_independent": False if getattr(current_settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint" else None,
        }

    def _state_store_runtime_api_config_kwargs(self) -> dict[str, Any]:
        return {
            "scheduler_instance_id": self._ensure_scheduler_instance_id(),
            "runtime_api_config": self._runtime_api_config(),
        }

    def _persist_runtime_api_config(self) -> None:
        setter = getattr(self.state_store, "set_runtime_api_config", None)
        if setter is None:
            return
        setter(self._runtime_api_config())

    def _should_self_retire(self) -> bool:
        runtime_settings = self._runtime_settings()
        configured_host_mode = str(
            getattr(runtime_settings, "scheduler_host_mode", "embedded_web") or "embedded_web"
        ).strip().lower()
        if not bool(getattr(runtime_settings, "scheduler_enabled", True)):
            return True
        if self._host_context() == "embedded_web_scheduler":
            if configured_host_mode != "embedded_web":
                return True
            if os.environ.get("FUND_ANA_DISABLE_EMBEDDED_SCHEDULER") == "1":
                return True
        elif self._host_context() == "external_scheduler_process" and configured_host_mode != "external_process":
            return True
        return False

    def _ensure_runtime_host_allowed(self) -> None:
        if not self._should_self_retire():
            return
        raise RuntimeError("scheduler_host_retired_by_runtime_config")

    def _mark_state_started(self) -> None:
        mark_started = getattr(self.state_store, "mark_started")
        try:
            mark_started(
                host_context=self._host_context(),
                **self._state_store_runtime_api_config_kwargs(),
            )
        except TypeError:
            try:
                mark_started(
                    host_context=self._host_context(),
                    scheduler_instance_id=self._ensure_scheduler_instance_id(),
                )
            except TypeError:
                mark_started()
            self._persist_runtime_api_config()

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logger(f"[scheduler] {timestamp} {message}")
        self._append_task_log_line(message)

    def _task_log_dir(self) -> Path:
        current_settings = getattr(self, "settings", None)
        project_root = getattr(current_settings, "project_root", None)
        if project_root:
            return Path(project_root) / "logs" / "tasks"
        return Path(__file__).resolve().parent.parent / "logs" / "tasks"

    def _task_log_state(self) -> dict[str, Any] | None:
        context = getattr(self, "_task_log_context", None)
        if context is None:
            context = threading.local()
            self._task_log_context = context
        payload = getattr(context, "payload", None)
        return payload if isinstance(payload, dict) else None

    def _enter_task_log_context(
        self,
        *,
        task_name: str,
        task_info: dict[str, Any],
        started_at: str,
    ) -> str:
        task_id = int(task_info.get("id") or 0) or int(time.time() * 1000)
        task_log_dir = self._task_log_dir()
        task_log_dir.mkdir(parents=True, exist_ok=True)
        task_log_path = task_log_dir / f"task_{task_id}_{_sanitize_task_log_name(task_name)}.log"
        self._task_log_context.payload = {
            "task_id": task_id,
            "task_name": task_name,
            "path": task_log_path,
        }
        self._append_task_log_line(
            f"[task-start] task_id={task_id} task_name={task_name} started_at={started_at}"
        )
        return str(task_log_path.resolve())

    def _clear_task_log_context(self) -> None:
        context = getattr(self, "_task_log_context", None)
        if context is None:
            return
        context.payload = None

    def _append_task_log_line(self, message: str) -> None:
        state = self._task_log_state()
        if not state:
            return
        raw_text = str(message or "").rstrip()
        if not raw_text:
            return
        task_log_path = state.get("path")
        if not isinstance(task_log_path, Path):
            return
        task_log_path.parent.mkdir(parents=True, exist_ok=True)
        lock = getattr(self, "_task_log_file_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._task_log_file_lock = lock
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = raw_text.splitlines() or [raw_text]
        with lock, task_log_io_lock(task_log_path.parent):
            if migration_paths(task_log_path)[1].exists():
                raise RuntimeError("task_log_identity_already_archived")
            with task_log_path.open("a", encoding="utf-8") as handle:
                for line in lines:
                    text = str(line).rstrip()
                    if text:
                        handle.write(f"{timestamp} {text}\n")

    def _project_root(self) -> Path:
        current_settings = getattr(self, "settings", None)
        configured_root = getattr(current_settings, "project_root", None)
        if configured_root:
            return Path(configured_root)
        return Path(__file__).resolve().parent.parent

    def _project_relative_path(self, value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        path = Path(text)
        project_root = self._project_root().resolve()
        candidates: list[Path] = []
        if path.is_absolute():
            candidates.append(path)
            parts = path.parts
            for marker in ("reports", "logs", "backend", "core", "frontend", "docs"):
                if marker in parts:
                    marker_index = parts.index(marker)
                    candidates.append(project_root.joinpath(*parts[marker_index:]))
                    break
        else:
            candidates.append(project_root / path)

        for candidate in candidates:
            try:
                return candidate.resolve().relative_to(project_root).as_posix()
            except (OSError, ValueError):
                continue
        return path.as_posix() if not path.is_absolute() else None

    def _project_path_artifact_fields(self, value: Any) -> dict[str, str | None]:
        absolute_path = str(value or "").strip() or None
        relative_path = self._project_relative_path(absolute_path)
        return {
            "absolute": absolute_path,
            "relative": relative_path,
            "project_path": relative_path,
        }

    def _should_use_workflow_manager_fallback(self) -> bool:
        run_sync = getattr(self.workflow_manager, "run_sync", None)
        if not callable(run_sync):
            return False
        current_settings = getattr(self, "settings", None)
        return not hasattr(current_settings, "project_root")

    def _workflow_execution_available(self) -> bool:
        if self._should_use_workflow_manager_fallback():
            return True
        current_settings = getattr(self, "settings", None)
        return hasattr(current_settings, "project_root")

    def _python_bin(self) -> str:
        project_venv_python = self._project_root() / ".venv" / "bin" / "python"
        if project_venv_python.exists():
            return str(project_venv_python)
        return os.sys.executable

    def _workflow_busy_skip_reason(self, *, after_collection: bool = False) -> str:
        workflow_manager = getattr(self, "workflow_manager", None)
        store = getattr(workflow_manager, "store", None)
        has_active_lock = getattr(store, "has_active_lock", None)
        if callable(has_active_lock):
            try:
                if bool(has_active_lock()):
                    return "workflow_busy_after_collection_locked" if after_collection else "workflow_busy_locked"
            except Exception:
                pass
        return "workflow_busy_after_collection_state_only" if after_collection else "workflow_busy_state_only"

    def _resolve_scheduled_long_term_history_limit(
        self,
        *,
        refresh_window: str,
        configured_limit: int,
        refresh_stale: bool,
    ) -> int | None:
        normalized_limit = int(configured_limit or 0)
        if normalized_limit > 0:
            return normalized_limit
        if refresh_stale and refresh_window in {"daily", "intraday"}:
            return DEFAULT_SCHEDULED_LONG_TERM_STALE_LIMIT
        return None

    @staticmethod
    def _python_has_module(python_bin: str | Path, module_name: str) -> bool:
        resolved_bin = Path(python_bin)
        if not resolved_bin.exists():
            return False
        try:
            result = subprocess.run(
                [
                    str(resolved_bin),
                    "-c",
                    (
                        "import importlib.util,sys; "
                        f"sys.exit(0 if importlib.util.find_spec('{module_name}') else 1)"
                    ),
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            return False
        return result.returncode == 0

    def _run_workflow_in_subprocess(
        self,
        options: dict[str, Any],
        *,
        log: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        from backend.collection_calculation_capture import completed_collection_captures

        captures = completed_collection_captures()
        options = dict(options)
        if captures:
            options["collection_calculation_captures"] = captures
        if self._should_use_workflow_manager_fallback():
            if log is not None:
                log("[workflow-fallback] use workflow_manager.run_sync(options)")
            return dict(self.workflow_manager.run_sync(dict(options)) or {})

        cmd = [self._python_bin(), "scripts/run_bond_workflow.py", "--compact-json", "--stream-collection-evidence"]
        if not bool(options.get("run_collection", self.settings.run_collection_by_default)):
            cmd.append("--skip-collection")
        if not bool(options.get("run_screening", self.settings.run_screening_by_default)):
            cmd.append("--skip-screening")
        if bool(options.get("reuse_existing_outputs")):
            cmd.append("--reuse-existing-outputs")
        collection_profile = str(options.get("collection_profile") or "").strip()
        if collection_profile:
            cmd.extend(["--collection-profile", collection_profile])
        if bool(options.get("force_calculate_risk")):
            cmd.append("--force-calculate-risk")
        if bool(options.get("risk_refresh_changed_nav_only")):
            cmd.append("--risk-refresh-changed-nav-only")
        risk_refresh_target_fcodes_file = str(options.get("risk_refresh_target_fcodes_file") or "").strip()
        if risk_refresh_target_fcodes_file:
            cmd.extend(["--risk-refresh-target-fcodes-file", risk_refresh_target_fcodes_file])
        if bool(options.get("skip_risk_calculation")):
            cmd.append("--skip-risk-calculation")
        if bool(options.get("skip_annual_return_gate_shadow_audit")):
            cmd.append("--skip-annual-return-gate-shadow-audit")
        for reference in options.get("collection_calculation_captures", []):
            cmd.extend(["--collection-calculation-capture", json.dumps(reference, ensure_ascii=False,
                                                                      separators=(",", ":"))])

        if log is not None:
            log(f"[workflow-subprocess] $ {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            cwd=str(self._project_root()),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        lines: list[str] = []
        for line in process.stdout:
            text = line.rstrip()
            lines.append(text)
            if log is not None and text:
                log(text)
        return_code = process.wait()
        if return_code != 0:
            joined_output = "\n".join(lines)
            if "已有筛选任务正在运行，请等待完成后再发起新任务。" in joined_output:
                raise RuntimeError("workflow_subprocess_busy")
            raise RuntimeError(f"workflow_subprocess_failed exit={return_code}")

        payload_lines = [line for line in lines if line.strip()]
        if not payload_lines:
            raise RuntimeError("workflow_subprocess_empty_output")
        try:
            return json.loads(payload_lines[-1])
        except json.JSONDecodeError as exc:
            raise RuntimeError("workflow_subprocess_invalid_json") from exc

    def _build_data_quality_audits(
        self,
        *,
        skip_heavy_risk_sync: bool = False,
    ) -> dict[str, Any]:
        audits: dict[str, Any] = {}
        try:
            audits["nav_lag_audit"] = build_nav_lag_audit(self.settings)
        except Exception as exc:
            audits["nav_lag_audit"] = {
                "artifact_path": None,
                "audit_error": str(exc),
            }
        try:
            audits["risk_lag_audit"] = build_risk_lag_audit(self.settings)
        except Exception as exc:
            audits["risk_lag_audit"] = {
                "artifact_path": None,
                "audit_error": str(exc),
            }
        if skip_heavy_risk_sync:
            audits["risk_sync_audit"] = {
                "artifact_path": None,
                "status": "skipped_intraday_deferred",
                "skip_reason": "heavy_intraday_tail_audit_deferred",
            }
        else:
            try:
                audits["risk_sync_audit"] = build_risk_sync_audit(self.settings)
            except Exception as exc:
                audits["risk_sync_audit"] = {
                    "artifact_path": None,
                    "audit_error": str(exc),
                }
        try:
            audits["risk_coverage_gap_audit"] = build_risk_coverage_gap_audit(self.settings)
        except Exception as exc:
            audits["risk_coverage_gap_audit"] = {
                "artifact_path": None,
                "audit_error": str(exc),
            }
        return audits

    def _build_data_quality_audits_compat(
        self,
        *,
        skip_heavy_risk_sync: bool = False,
    ) -> dict[str, Any]:
        """
        兼容旧测试对 `_build_data_quality_audits` 的简化 monkeypatch。

        部分回归测试直接把该方法替换成不接受关键字参数的 lambda。
        这里集中兜底，避免调用点散落测试兼容逻辑。
        """
        build_fn = self._build_data_quality_audits
        try:
            parameters = inspect.signature(build_fn).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "skip_heavy_risk_sync" in parameters:
            return build_fn(skip_heavy_risk_sync=skip_heavy_risk_sync)
        return build_fn()

    def _run_nav_history_archive_preview(self) -> dict[str, Any]:
        output_dir = self.settings.reports_dir / "audit" / "nav_history_archive"
        db_path_text = str(getattr(self.settings, "database_path", "") or "").strip()
        project_root_text = (
            str(getattr(self.settings, "project_root", "") or "").strip()
            or str(self._project_root())
        )
        apply_mode = str(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_mode",
                "dry_run",
            )
            or "dry_run"
        ).strip() or "dry_run"
        if apply_mode not in {"dry_run", "archive_export"}:
            apply_mode = "dry_run"
        apply_page_limit = max(
            1,
            int(
                getattr(
                    self.settings,
                    "scheduler_weekly_nav_history_archive_preview_page_limit",
                    DEFAULT_ARCHIVE_PAGE_LIMIT,
                )
                or DEFAULT_ARCHIVE_PAGE_LIMIT
            ),
        )
        max_pages_per_run = max(
            1,
            int(
                getattr(
                    self.settings,
                    "scheduler_weekly_nav_history_archive_preview_max_pages_per_run",
                    1,
                )
                or 1
            ),
        )
        auto_bind_latest_backup = bool(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_auto_bind_latest_backup",
                True,
            )
        )
        auto_create_backup_if_missing = bool(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_auto_create_backup_if_missing",
                False,
            )
        )
        backup_prefix = str(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_backup_prefix",
                NAV_HISTORY_ARCHIVE_BACKUP_PREFIX,
            )
            or NAV_HISTORY_ARCHIVE_BACKUP_PREFIX
        ).strip() or NAV_HISTORY_ARCHIVE_BACKUP_PREFIX
        backup_compress = bool(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_backup_compress",
                True,
            )
        )

        governance_payload = build_fund_nav_history_governance_audit_payload(
            db_path=db_path_text or None,
        )
        governance_json_path, governance_md_path = write_fund_nav_history_governance_audit_artifacts(
            governance_payload,
            output_dir=str(output_dir),
        )
        binding_health_payload = build_nav_history_archive_binding_health_payload(
            db_path=db_path_text or None,
            project_root=project_root_text,
            output_dir=str(output_dir),
            governance_payload=governance_payload,
            mode=apply_mode,
            page_limit=apply_page_limit,
        )
        binding_health_summary = dict(binding_health_payload.get("summary") or {})
        binding_health_has_active_source = bool(
            binding_health_summary.get("source_active_apply_runner_artifact")
        )
        if binding_health_has_active_source:
            apply_runner_json_path, apply_runner_md_path = (
                write_nav_history_archive_apply_runner_artifacts(
                    binding_health_payload,
                    output_dir=str(output_dir),
                )
            )
            rolling_runner_payload: dict[str, Any] | None = None
            rolling_runner_summary: dict[str, Any] = {}
            rolling_runner_json_path: Path | None = None
            rolling_runner_md_path: Path | None = None
            updated_active_binding_payload: dict[str, Any] | None = None
            updated_active_binding_json_path: Path | None = None
            updated_active_binding_md_path: Path | None = None
            rolling_runner_payloads: list[dict[str, Any]] = []
            rolling_runner_artifacts: list[dict[str, Any]] = []
            rolling_archive_page_count = 0
            rolling_archive_budget_exhausted = False
            rolling_archive_completed = False
            rolling_archive_total_inserted = 0
            rolling_archive_total_hot_deleted = 0
            rolling_archive_total_stale_inserted = 0
            rolling_archive_total_stale_deleted = 0
            # A binding-health run can report rolling_archive_required=False while
            # still finding duplicate rows before the active cutoff. Those rows
            # are safe to reconcile only through the rolling runner; skipping it
            # leaves the scheduler degraded forever even when every row already
            # has an exact cold-archive match.
            live_cold_rows = int(binding_health_summary.get("live_cold_row_count") or 0)
            rolling_archive_needed = bool(
                binding_health_summary.get("rolling_archive_required")
                or live_cold_rows > 0
            )
            if rolling_archive_needed:
                rolling_mode = "rolling_archive" if apply_mode == "archive_export" else "dry_run"
                rolling_active_binding_payload = binding_health_payload
                rolling_source_active_binding_artifact = str(apply_runner_json_path)
                source_active_apply_runner_artifact = str(
                    binding_health_summary.get("source_active_apply_runner_artifact") or ""
                ).strip()
                if source_active_apply_runner_artifact:
                    source_active_apply_runner_path = Path(
                        source_active_apply_runner_artifact
                    ).expanduser()
                    if not source_active_apply_runner_path.is_absolute():
                        source_active_apply_runner_path = (
                            Path(project_root_text) / source_active_apply_runner_path
                        ).resolve()
                    try:
                        source_active_apply_runner_payload = json.loads(
                            source_active_apply_runner_path.read_text(encoding="utf-8")
                        )
                    except (OSError, json.JSONDecodeError, TypeError, ValueError):
                        source_active_apply_runner_payload = {}
                    source_active_apply_runner_summary = (
                        source_active_apply_runner_payload.get("summary")
                        if isinstance(source_active_apply_runner_payload, dict)
                        else {}
                    )
                    if (
                        isinstance(source_active_apply_runner_payload, dict)
                        and isinstance(source_active_apply_runner_summary, dict)
                        and bool(source_active_apply_runner_summary.get("archive_binding_active"))
                    ):
                        rolling_active_binding_payload = source_active_apply_runner_payload
                        rolling_source_active_binding_artifact = str(
                            source_active_apply_runner_path
                        )
                for page_index in range(1, max_pages_per_run + 1):
                    rolling_runner_payload = build_nav_history_archive_rolling_runner_payload(
                        db_path=db_path_text or None,
                        project_root=project_root_text,
                        output_dir=str(output_dir),
                        governance_payload=governance_payload,
                        active_binding_payload=rolling_active_binding_payload,
                        source_active_binding_artifact=rolling_source_active_binding_artifact,
                        mode=rolling_mode,
                        page_limit=apply_page_limit,
                    )
                    page_timestamp = (
                        datetime.now().strftime("%Y%m%d_%H%M%S")
                        + f"_rolling_p{page_index:03d}"
                    )
                    rolling_runner_json_path, rolling_runner_md_path = (
                        write_nav_history_archive_rolling_runner_artifacts(
                            rolling_runner_payload,
                            output_dir=str(output_dir),
                            timestamp=page_timestamp,
                        )
                    )
                    rolling_runner_payloads.append(rolling_runner_payload)
                    rolling_runner_artifacts.append(
                        {
                            "page_index": page_index,
                            "artifact_path": str(rolling_runner_json_path),
                            "markdown_path": str(rolling_runner_md_path),
                        }
                    )
                    rolling_runner_summary = dict(rolling_runner_payload.get("summary") or {})
                    if bool(rolling_runner_summary.get("rolling_archive_executed")):
                        rolling_archive_page_count += 1
                    rolling_archive_total_inserted += int(
                        rolling_runner_summary.get("rolling_archive_inserted_row_count") or 0
                    )
                    rolling_archive_total_hot_deleted += int(
                        rolling_runner_summary.get("rolling_hot_delete_deleted_row_count") or 0
                    )
                    rolling_archive_total_stale_inserted += int(
                        rolling_runner_summary.get("stale_hot_missing_archive_inserted_row_count")
                        or 0
                    )
                    rolling_archive_total_stale_deleted += (
                        int(
                            rolling_runner_summary.get(
                                "stale_hot_missing_archive_deleted_row_count"
                            )
                            or 0
                        )
                        + int(
                            rolling_runner_summary.get("stale_hot_delete_deleted_row_count")
                            or 0
                        )
                    )
                    rolling_runner_status = str(
                        rolling_runner_summary.get("runner_status") or ""
                    ).strip()
                    candidate_updated_binding = rolling_runner_payload.get(
                        "updated_active_binding_payload"
                    )
                    if isinstance(candidate_updated_binding, dict) and candidate_updated_binding:
                        updated_active_binding_payload = candidate_updated_binding
                        rolling_binding_ts = (
                            datetime.now().strftime("%Y%m%d_%H%M%S")
                            + f"_rolling_binding_p{page_index:03d}"
                        )
                        (
                            updated_active_binding_json_path,
                            updated_active_binding_md_path,
                        ) = write_nav_history_archive_apply_runner_artifacts(
                            updated_active_binding_payload,
                            output_dir=str(output_dir),
                            timestamp=rolling_binding_ts,
                        )
                        rolling_active_binding_payload = updated_active_binding_payload
                        rolling_source_active_binding_artifact = str(
                            updated_active_binding_json_path
                        )
                    if rolling_mode != "rolling_archive":
                        break
                    if rolling_runner_status == "rolling_archive_complete_binding_ready":
                        rolling_archive_completed = True
                        break
                    if rolling_runner_status in {
                        "rolling_archive_blocked",
                        "rolling_archive_not_required",
                    }:
                        break
                    if not bool(rolling_runner_summary.get("rolling_archive_executed")):
                        break
                if (
                    rolling_mode == "rolling_archive"
                    and not rolling_archive_completed
                    and rolling_runner_summary.get("runner_status")
                    == "rolling_archive_in_progress"
                    and rolling_archive_page_count >= max_pages_per_run
                ):
                    rolling_archive_budget_exhausted = True
            final_binding_summary = (
                dict(updated_active_binding_payload.get("summary") or {})
                if isinstance(updated_active_binding_payload, dict)
                else binding_health_summary
            )
            rolling_blocker_count = int(rolling_runner_summary.get("blocker_count") or 0)
            rolling_archive_status = str(
                rolling_runner_summary.get("runner_status") or ""
            ).strip()
            final_artifact_path = updated_active_binding_json_path or rolling_runner_json_path or apply_runner_json_path
            final_markdown_path = updated_active_binding_md_path or rolling_runner_md_path or apply_runner_md_path
            archive_summary = {
                "governance_status": (governance_payload.get("summary") or {}).get("status")
                or "",
                "recommended_direction_key": (
                    (governance_payload.get("summary") or {}).get("recommended_direction_key")
                    or ""
                ),
                "recommended_direction_label": (
                    (governance_payload.get("summary") or {}).get("recommended_direction_label")
                    or ""
                ),
                "runbook_status": "skipped_existing_binding_active",
                "package_status": "skipped_existing_binding_active",
                "window_status": "skipped_existing_binding_active",
                "checklist_status": "skipped_existing_binding_active",
                "readiness_status": binding_health_summary.get("readiness_status") or "",
                "ready_for_manual_signoff": False,
                "recommended_retention_years": (
                    (governance_payload.get("summary") or {}).get("recommended_retention_years")
                ),
                "archive_cutoff_date": final_binding_summary.get("archive_cutoff_date") or "",
                "estimated_archive_row_count": final_binding_summary.get(
                    "expected_archive_row_count"
                )
                or binding_health_summary.get("expected_archive_row_count")
                or 0,
                "estimated_archive_fund_count": final_binding_summary.get(
                    "expected_archive_fund_count"
                )
                or binding_health_summary.get("expected_archive_fund_count")
                or 0,
                "estimated_archive_size_mb": 0.0,
                "live_cold_row_count": final_binding_summary.get("live_cold_row_count") or 0,
                "live_cold_fund_count": final_binding_summary.get("live_cold_fund_count") or 0,
                "archive_target_path": final_binding_summary.get("archive_target_path")
                or binding_health_summary.get("archive_target_path")
                or "",
                "archive_table_name": final_binding_summary.get("archive_table_name")
                or binding_health_summary.get("archive_table_name")
                or "",
                "backup_artifact_path": final_binding_summary.get("backup_artifact_path")
                or binding_health_summary.get("backup_artifact_path")
                or "",
                "auto_backup_binding_status": "skipped_existing_binding_active",
                "auto_backup_created": False,
                "auto_backup_artifact_path": "",
                "auto_backup_evidence_artifact": "",
                "auto_backup_binding_error": "",
                "blocker_count": int(final_binding_summary.get("blocker_count") or 0),
                "review_item_count": int(final_binding_summary.get("review_item_count") or 0),
                "cold_query_compat_status": final_binding_summary.get("cold_query_compat_status")
                or binding_health_summary.get("cold_query_compat_status")
                or "",
                "query_compat_safe": bool(final_binding_summary.get("query_compat_safe")),
                "safe_to_apply_archive_now": False,
                "production_cold_dependency_count": 0,
                "production_covered_cold_dependency_count": 0,
                "production_uncovered_cold_dependency_count": int(
                    final_binding_summary.get("production_uncovered_cold_dependency_count") or 0
                ),
                "analysis_cold_dependency_count": 0,
                "apply_runner_status": rolling_archive_status
                or binding_health_summary.get("runner_status")
                or "",
                "auto_governance_stage": rolling_runner_summary.get("auto_governance_stage")
                or binding_health_summary.get("auto_governance_stage")
                or "",
                "dry_run_safe": bool(final_binding_summary.get("dry_run_safe"))
                and rolling_blocker_count == 0,
                "archive_export_in_progress": False,
                "archive_export_result_status": (
                    rolling_runner_summary.get("rolling_archive_result_status")
                    or binding_health_summary.get("archive_export_result_status")
                    or ""
                ),
                "archive_export_inserted_row_count": int(
                    rolling_archive_total_inserted
                    or rolling_runner_summary.get("rolling_archive_inserted_row_count")
                    or 0
                ),
                "archive_sql_executed": bool(
                    rolling_runner_summary.get("archive_sql_executed")
                ),
                "delete_sql_executed": bool(
                    rolling_runner_summary.get("delete_sql_executed")
                ),
                "vacuum_executed": False,
                "archive_binding_active": bool(
                    final_binding_summary.get("archive_binding_active")
                ),
                "archive_materialized_and_reconciled": bool(
                    final_binding_summary.get("archive_materialized_and_reconciled")
                ),
                "archive_missing_row_count": int(
                    final_binding_summary.get("archive_missing_row_count") or 0
                ),
                "active_db_quick_check": final_binding_summary.get("active_db_quick_check")
                or "",
                "archive_quick_check": final_binding_summary.get("archive_quick_check") or "",
                "effective_hot_cold_row_count": int(
                    final_binding_summary.get("effective_hot_cold_row_count") or 0
                ),
                "rolling_archive_required": bool(
                    final_binding_summary.get("rolling_archive_required")
                    if "rolling_archive_required" in final_binding_summary
                    else binding_health_summary.get("rolling_archive_required")
                ),
                "rolling_archive_supported": True,
                "rolling_archive_runner_status": rolling_archive_status,
                "rolling_archive_executed": bool(
                    rolling_runner_summary.get("rolling_archive_executed")
                    or rolling_archive_page_count > 0
                ),
                "rolling_archive_page_limit": apply_page_limit,
                "rolling_archive_max_pages_per_run": max_pages_per_run,
                "rolling_archive_page_count": rolling_archive_page_count,
                "rolling_archive_budget_exhausted": rolling_archive_budget_exhausted,
                "rolling_archive_completed": rolling_archive_completed,
                "rolling_archive_cutoff_date": final_binding_summary.get(
                    "rolling_archive_cutoff_date"
                )
                or binding_health_summary.get(
                    "rolling_archive_cutoff_date"
                )
                or "",
                "rolling_archive_candidate_row_count": int(
                    rolling_runner_summary.get("rolling_archive_candidate_row_count")
                    or binding_health_summary.get("rolling_archive_candidate_row_count")
                    or 0
                ),
                "rolling_archive_candidate_fund_count": int(
                    rolling_runner_summary.get("rolling_archive_candidate_fund_count")
                    or binding_health_summary.get("rolling_archive_candidate_fund_count")
                    or 0
                ),
                "rolling_archive_inserted_row_count": int(
                    rolling_archive_total_inserted
                    or rolling_runner_summary.get("rolling_archive_inserted_row_count")
                    or 0
                ),
                "rolling_hot_delete_deleted_row_count": int(
                    rolling_archive_total_hot_deleted
                    or rolling_runner_summary.get("rolling_hot_delete_deleted_row_count")
                    or 0
                ),
                "stale_hot_missing_archive_inserted_row_count": int(
                    rolling_archive_total_stale_inserted
                    or rolling_runner_summary.get(
                        "stale_hot_missing_archive_inserted_row_count"
                    )
                    or 0
                ),
                "stale_hot_deleted_row_count": int(
                    rolling_archive_total_stale_deleted
                    or rolling_runner_summary.get(
                        "stale_hot_missing_archive_deleted_row_count"
                    )
                    or rolling_runner_summary.get("stale_hot_delete_deleted_row_count")
                    or 0
                ),
                "rolling_hot_before_current_cutoff_row_count": int(
                    rolling_runner_summary.get("hot_before_current_cutoff_row_count") or 0
                ),
                "rolling_archive_blocked_reason": final_binding_summary.get(
                    "rolling_archive_blocked_reason"
                )
                or binding_health_summary.get(
                    "rolling_archive_blocked_reason"
                )
                or "",
                "apply_runner_blocker_count": int(
                    final_binding_summary.get("blocker_count") or 0
                )
                + rolling_blocker_count,
                "rolling_archive_blocker_count": rolling_blocker_count,
                "page_apply_supported": False,
                "execution_mode": rolling_runner_summary.get("mode")
                or binding_health_summary.get("execution_mode")
                or "binding_health_check",
                "apply_runner_page_limit": int(
                    binding_health_summary.get("page_limit") or apply_page_limit
                ),
                "rolling_archive_artifact_count": len(rolling_runner_artifacts),
            }
            artifacts = {
                "governance_audit": {
                    "artifact_path": str(governance_json_path),
                    "markdown_path": str(governance_md_path),
                },
                "runbook": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "preflight": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "control_pack": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "maintenance_window_pack": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "execution_checklist": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "readiness_audit": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "cold_query_compat_audit": {
                    "artifact_path": "",
                    "markdown_path": "",
                    "skip_reason": "existing_archive_binding_active",
                },
                "apply_runner": {
                    "artifact_path": str(apply_runner_json_path),
                    "markdown_path": str(apply_runner_md_path),
                },
                "rolling_runner": {
                    "artifact_path": str(rolling_runner_json_path or ""),
                    "markdown_path": str(rolling_runner_md_path or ""),
                    "pages": rolling_runner_artifacts,
                    "skip_reason": (
                        ""
                        if rolling_runner_json_path
                        else "rolling_archive_not_required"
                    ),
                },
                "updated_active_binding": {
                    "artifact_path": str(updated_active_binding_json_path or ""),
                    "markdown_path": str(updated_active_binding_md_path or ""),
                    "skip_reason": (
                        ""
                        if updated_active_binding_json_path
                        else "rolling_archive_not_completed"
                    ),
                },
            }
            return {
                "status": "success",
                "artifact_dir": str(output_dir),
                "artifact_path": str(final_artifact_path),
                "markdown_path": str(final_markdown_path),
                "summary": archive_summary,
                "artifacts": artifacts,
                "backup_binding": {
                    "status": "skipped_existing_binding_active",
                    "auto_bind_latest_backup": auto_bind_latest_backup,
                    "auto_create_backup_if_missing": auto_create_backup_if_missing,
                    "backup_prefix": backup_prefix,
                    "backup_created": False,
                    "backup_artifact_path": "",
                    "backup_evidence_artifact": "",
                    "error": "",
                },
                "findings": [
                    *(binding_health_payload.get("findings") or []),
                    *((rolling_runner_payload or {}).get("findings") or []),
                ],
            }

        runbook_payload = build_nav_history_archive_runbook_payload(
            nav_history_storage_profile=governance_payload.get("nav_history_storage_profile") or {},
            nav_history_retention_preview=governance_payload.get("nav_history_retention_preview") or {},
            nav_history_archive_strategy_preview=governance_payload.get("nav_history_archive_strategy_preview") or {},
        )
        runbook_json_path, runbook_md_path = write_nav_history_archive_runbook_artifacts(
            runbook_payload,
            output_dir=str(output_dir),
        )

        preflight_payload = build_nav_history_archive_preflight_payload(
            db_path=db_path_text or None,
            runbook_payload=runbook_payload,
            source_runbook_artifact=str(runbook_json_path),
            output_dir=str(output_dir),
        )
        preflight_json_path, preflight_md_path = write_nav_history_archive_preflight_artifacts(
            preflight_payload,
            output_dir=str(output_dir),
        )

        control_pack_payload = build_nav_history_archive_control_pack_payload(
            runbook_payload=runbook_payload,
            preflight_payload=preflight_payload,
            source_runbook_artifact=str(runbook_json_path),
            source_preflight_artifact=str(preflight_json_path),
            output_dir=str(output_dir),
        )
        control_pack_json_path, control_pack_md_path = write_nav_history_archive_control_pack_artifacts(
            control_pack_payload,
            output_dir=str(output_dir),
        )

        maintenance_window_payload = build_nav_history_archive_maintenance_window_pack_payload(
            control_pack_payload=control_pack_payload,
            runbook_payload=runbook_payload,
            source_control_pack_artifact=str(control_pack_json_path),
            source_runbook_artifact=str(runbook_json_path),
            output_dir=str(output_dir),
        )
        maintenance_window_json_path, maintenance_window_md_path = (
            write_nav_history_archive_maintenance_window_pack_artifacts(
                maintenance_window_payload,
                output_dir=str(output_dir),
            )
        )

        execution_checklist_payload = build_nav_history_archive_execution_checklist_payload(
            maintenance_window_pack_payload=maintenance_window_payload,
            control_pack_payload=control_pack_payload,
            preflight_payload=preflight_payload,
            source_maintenance_window_pack_artifact=str(maintenance_window_json_path),
            source_control_pack_artifact=str(control_pack_json_path),
            source_preflight_artifact=str(preflight_json_path),
            project_root=project_root_text,
            output_dir=str(output_dir),
        )
        execution_checklist_json_path, execution_checklist_md_path = (
            write_nav_history_archive_execution_checklist_artifacts(
                execution_checklist_payload,
                output_dir=str(output_dir),
            )
        )

        backup_binding_summary: dict[str, Any] = {
            "status": "not_requested",
            "auto_bind_latest_backup": auto_bind_latest_backup,
            "auto_create_backup_if_missing": auto_create_backup_if_missing,
            "backup_prefix": backup_prefix,
            "backup_created": False,
            "backup_artifact_path": "",
            "backup_evidence_artifact": "",
            "error": "",
        }
        readiness_payload: dict[str, Any] | None = None
        readiness_json_path: Path | None = None
        readiness_md_path: Path | None = None
        if apply_mode == "archive_export" and auto_bind_latest_backup and db_path_text:
            backup_dir = Path(db_path_text).parent / "backups"
            backup_candidates: list[Path] = []
            if backup_dir.exists() and backup_dir.is_dir():
                for pattern in (f"{backup_prefix}_*.db.zst", f"{backup_prefix}_*.db"):
                    backup_candidates.extend(
                        candidate
                        for candidate in backup_dir.glob(pattern)
                        if candidate.is_file()
                    )
            latest_backup_path = (
                sorted(backup_candidates, key=lambda item: (item.stat().st_mtime, item.name))[-1]
                if backup_candidates
                else None
            )
            backup_evidence_json_path: Path | None = None
            if latest_backup_path is None:
                backup_binding_summary["status"] = "backup_not_found"
                if auto_create_backup_if_missing:
                    try:
                        (
                            backup_evidence_json_path,
                            _backup_evidence_md_path,
                            backup_payload,
                        ) = create_nav_history_archive_backup(
                            source_db_path=db_path_text,
                            backup_dir=backup_dir,
                            output_dir=str(output_dir),
                            project_root=project_root_text,
                            backup_prefix=backup_prefix,
                            compress=backup_compress,
                            keep_uncompressed=False,
                            compute_sha256=True,
                            dry_run=False,
                        )
                        backup_summary = dict(backup_payload.get("summary") or {})
                        latest_backup_path = Path(
                            str(backup_summary.get("backup_artifact_path") or "")
                        )
                        backup_binding_summary.update(
                            {
                                "status": "backup_created",
                                "backup_created": True,
                                "backup_artifact_path": str(latest_backup_path),
                                "backup_evidence_artifact": str(backup_evidence_json_path),
                            }
                        )
                    except Exception as exc:
                        backup_binding_summary.update(
                            {
                                "status": "backup_create_failed",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
            else:
                backup_binding_summary.update(
                    {
                        "status": "latest_backup_found",
                        "backup_artifact_path": str(latest_backup_path),
                    }
                )

            if latest_backup_path is not None:
                try:
                    (
                        execution_checklist_json_path,
                        execution_checklist_md_path,
                        readiness_json_path,
                        readiness_md_path,
                        bind_result,
                    ) = bind_nav_history_archive_backup_to_checklist(
                        backup_artifact_path=latest_backup_path,
                        output_dir=str(output_dir),
                        project_root=project_root_text,
                        backup_evidence_artifact=backup_evidence_json_path,
                        source_execution_checklist_artifact=execution_checklist_json_path,
                    )
                    readiness_payload = dict(bind_result.get("readiness_payload") or {})
                    backup_binding_summary.update(
                        {
                            "status": "bound",
                            "execution_checklist_artifact": str(execution_checklist_json_path),
                            "readiness_artifact": str(readiness_json_path),
                            "readiness_status": (
                                dict(readiness_payload.get("summary") or {}).get("readiness_status")
                                or ""
                            ),
                        }
                    )
                except Exception as exc:
                    readiness_payload = None
                    readiness_json_path = None
                    readiness_md_path = None
                    backup_binding_summary.update(
                        {
                            "status": "bind_failed",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )

        if readiness_payload is None or readiness_json_path is None or readiness_md_path is None:
            readiness_payload = build_nav_history_archive_maintenance_readiness_audit_payload(
                db_path=db_path_text or None,
                project_root=project_root_text,
                execution_checklist_payload=execution_checklist_payload,
                maintenance_window_pack_payload=maintenance_window_payload,
                control_pack_payload=control_pack_payload,
                preflight_payload=preflight_payload,
                source_execution_checklist_artifact=str(execution_checklist_json_path),
                source_maintenance_window_pack_artifact=str(maintenance_window_json_path),
                source_control_pack_artifact=str(control_pack_json_path),
                source_preflight_artifact=str(preflight_json_path),
                output_dir=str(output_dir),
            )
            readiness_json_path, readiness_md_path = (
                write_nav_history_archive_maintenance_readiness_audit_artifacts(
                    readiness_payload,
                    output_dir=str(output_dir),
                )
            )
        cold_query_compat_payload = build_nav_history_archive_cold_query_compat_audit_payload(
            db_path=db_path_text or None,
            readiness_payload=readiness_payload,
            source_readiness_artifact=str(readiness_json_path),
            output_dir=str(output_dir),
            project_root=project_root_text,
        )
        cold_query_compat_json_path, cold_query_compat_md_path = (
            write_nav_history_archive_cold_query_compat_audit_artifacts(
                cold_query_compat_payload,
                output_dir=str(output_dir),
            )
        )
        apply_runner_payload = build_nav_history_archive_apply_runner_payload(
            db_path=db_path_text or None,
            project_root=project_root_text,
            readiness_payload=readiness_payload,
            cold_query_compat_payload=cold_query_compat_payload,
            runbook_payload=runbook_payload,
            source_readiness_artifact=str(readiness_json_path),
            source_cold_query_compat_artifact=str(cold_query_compat_json_path),
            source_runbook_artifact=str(runbook_json_path),
            output_dir=str(output_dir),
            mode=apply_mode,
            page_limit=apply_page_limit,
        )
        apply_runner_json_path, apply_runner_md_path = (
            write_nav_history_archive_apply_runner_artifacts(
                apply_runner_payload,
                output_dir=str(output_dir),
            )
        )

        governance_summary = dict(governance_payload.get("summary") or {})
        runbook_summary = dict(runbook_payload.get("summary") or {})
        preflight_summary = dict(preflight_payload.get("summary") or {})
        preflight_checks = dict(preflight_payload.get("checks") or {})
        control_summary = dict(control_pack_payload.get("summary") or {})
        window_summary = dict(maintenance_window_payload.get("summary") or {})
        checklist_summary = dict(execution_checklist_payload.get("summary") or {})
        readiness_summary = dict(readiness_payload.get("summary") or {})
        cold_query_compat_summary = dict(cold_query_compat_payload.get("summary") or {})
        apply_runner_summary = dict(apply_runner_payload.get("summary") or {})
        archive_summary = {
            "governance_status": governance_summary.get("status") or "",
            "recommended_direction_key": governance_summary.get("recommended_direction_key") or "",
            "recommended_direction_label": governance_summary.get("recommended_direction_label") or "",
            "runbook_status": runbook_payload.get("status") or "",
            "package_status": control_summary.get("package_status") or "",
            "window_status": window_summary.get("window_status") or "",
            "checklist_status": checklist_summary.get("checklist_status") or "",
            "readiness_status": readiness_summary.get("readiness_status") or "",
            "ready_for_manual_signoff": bool(
                preflight_summary.get("ready_for_manual_signoff")
                or preflight_checks.get("ready_for_manual_signoff")
                or readiness_summary.get("ready_for_manual_signoff")
            ),
            "recommended_retention_years": readiness_summary.get("recommended_retention_years")
            or runbook_summary.get("recommended_retention_years"),
            "archive_cutoff_date": readiness_summary.get("archive_cutoff_date")
            or runbook_summary.get("archive_cutoff_date"),
            "estimated_archive_row_count": readiness_summary.get("estimated_archive_row_count")
            or runbook_summary.get("estimated_archive_row_count")
            or 0,
            "estimated_archive_fund_count": readiness_summary.get("estimated_archive_fund_count")
            or runbook_summary.get("estimated_archive_fund_count")
            or 0,
            "estimated_archive_size_mb": readiness_summary.get("estimated_archive_size_mb")
            or runbook_summary.get("estimated_archive_size_mb")
            or 0.0,
            "live_cold_row_count": readiness_summary.get("live_cold_row_count")
            or preflight_summary.get("live_cold_row_count")
            or 0,
            "live_cold_fund_count": readiness_summary.get("live_cold_fund_count")
            or preflight_summary.get("live_cold_fund_count")
            or 0,
            "archive_target_path": readiness_summary.get("archive_target_path")
            or runbook_summary.get("archive_target_path")
            or "",
            "archive_table_name": readiness_summary.get("archive_table_name")
            or runbook_summary.get("archive_table_name")
            or "",
            "backup_artifact_path": readiness_summary.get("backup_artifact_path") or "",
            "auto_backup_binding_status": backup_binding_summary.get("status") or "",
            "auto_backup_created": bool(backup_binding_summary.get("backup_created")),
            "auto_backup_artifact_path": backup_binding_summary.get("backup_artifact_path") or "",
            "auto_backup_evidence_artifact": backup_binding_summary.get("backup_evidence_artifact") or "",
            "auto_backup_binding_error": backup_binding_summary.get("error") or "",
            "blocker_count": int(readiness_summary.get("blocker_count") or 0),
            "review_item_count": int(readiness_summary.get("review_item_count") or 0),
            "cold_query_compat_status": cold_query_compat_summary.get("status") or "",
            "query_compat_safe": bool(cold_query_compat_summary.get("query_compat_safe")),
            "safe_to_apply_archive_now": bool(
                cold_query_compat_summary.get("safe_to_apply_archive_now")
            ),
            "production_cold_dependency_count": int(
                cold_query_compat_summary.get("production_cold_dependency_count") or 0
            ),
            "production_covered_cold_dependency_count": int(
                cold_query_compat_summary.get("production_covered_cold_dependency_count") or 0
            ),
            "production_uncovered_cold_dependency_count": int(
                cold_query_compat_summary.get("production_uncovered_cold_dependency_count") or 0
            ),
            "analysis_cold_dependency_count": int(
                cold_query_compat_summary.get("analysis_cold_dependency_count") or 0
            ),
            "apply_runner_status": apply_runner_summary.get("runner_status") or "",
            "auto_governance_stage": apply_runner_summary.get("auto_governance_stage") or "",
            "dry_run_safe": bool(apply_runner_summary.get("dry_run_safe")),
            "archive_export_in_progress": bool(
                apply_runner_summary.get("archive_export_in_progress")
            ),
            "archive_export_result_status": (
                apply_runner_summary.get("archive_export_result_status") or ""
            ),
            "archive_export_inserted_row_count": int(
                apply_runner_summary.get("archive_export_inserted_row_count") or 0
            ),
            "archive_sql_executed": bool(apply_runner_summary.get("archive_sql_executed")),
            "delete_sql_executed": bool(apply_runner_summary.get("delete_sql_executed")),
            "vacuum_executed": bool(apply_runner_summary.get("vacuum_executed")),
            "archive_binding_active": bool(apply_runner_summary.get("archive_binding_active")),
            "archive_materialized_and_reconciled": bool(
                apply_runner_summary.get("archive_materialized_and_reconciled")
            ),
            "archive_missing_row_count": int(
                apply_runner_summary.get("archive_missing_row_count") or 0
            ),
            "apply_runner_blocker_count": int(apply_runner_summary.get("blocker_count") or 0),
            "page_apply_supported": bool(readiness_summary.get("page_apply_supported")),
            "execution_mode": apply_runner_summary.get("execution_mode")
            or readiness_summary.get("execution_mode")
            or apply_mode,
            "apply_runner_page_limit": int(apply_runner_summary.get("page_limit") or apply_page_limit),
        }
        artifacts = {
            "governance_audit": {
                "artifact_path": str(governance_json_path),
                "markdown_path": str(governance_md_path),
            },
            "runbook": {
                "artifact_path": str(runbook_json_path),
                "markdown_path": str(runbook_md_path),
            },
            "preflight": {
                "artifact_path": str(preflight_json_path),
                "markdown_path": str(preflight_md_path),
            },
            "control_pack": {
                "artifact_path": str(control_pack_json_path),
                "markdown_path": str(control_pack_md_path),
            },
            "maintenance_window_pack": {
                "artifact_path": str(maintenance_window_json_path),
                "markdown_path": str(maintenance_window_md_path),
            },
            "execution_checklist": {
                "artifact_path": str(execution_checklist_json_path),
                "markdown_path": str(execution_checklist_md_path),
            },
            "readiness_audit": {
                "artifact_path": str(readiness_json_path),
                "markdown_path": str(readiness_md_path),
            },
            "cold_query_compat_audit": {
                "artifact_path": str(cold_query_compat_json_path),
                "markdown_path": str(cold_query_compat_md_path),
            },
            "apply_runner": {
                "artifact_path": str(apply_runner_json_path),
                "markdown_path": str(apply_runner_md_path),
            },
        }
        return {
            "status": "success",
            "artifact_dir": str(output_dir),
            "artifact_path": str(apply_runner_json_path),
            "markdown_path": str(apply_runner_md_path),
            "summary": archive_summary,
            "artifacts": artifacts,
            "backup_binding": backup_binding_summary,
            "findings": [
                *(readiness_payload.get("findings") or []),
                *(cold_query_compat_payload.get("findings") or []),
                *(apply_runner_payload.get("findings") or []),
            ],
        }

    def _build_host_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "host_context": self._host_context(),
            "scheduler_service_runtime": _read_systemd_service_runtime(
                getattr(self.settings, "scheduler_service_unit_name", "fund-ana-scheduler.service")
            ),
            "web_service_runtime": _read_systemd_service_runtime(
                getattr(self.settings, "web_service_unit_name", "fund-ana-web.service")
            ),
            "scheduler_pid_file": str(getattr(self.settings, "scheduler_pid_file", "")),
            "scheduler_heartbeat_file": str(getattr(self.settings, "scheduler_heartbeat_file", "")),
        }

    def _run_regression_smoke(
        self,
        *,
        groups: list[str] | tuple[str, ...] | None = None,
        log: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        selected_groups = [str(item).strip() for item in (groups or []) if str(item).strip()]
        python_bin = self._python_bin()
        if not self._python_has_module(python_bin, "pytest"):
            error_message = f"regression_smoke_pytest_unavailable: {python_bin}"
            synthetic_group_results = [
                {
                    "group": group,
                    "description": None,
                    "status": "failed",
                    "command_count": 0,
                    "commands": [],
                    "failure_reason": "missing_pytest_dependency",
                }
                for group in selected_groups
            ]
            if log is not None:
                log(error_message)
            finished_at = datetime.now().isoformat(timespec="seconds")
            return {
                "status": "degraded",
                "started_at": finished_at,
                "finished_at": finished_at,
                "groups": selected_groups,
                "group_results": synthetic_group_results,
                "overall_status": "failed",
                "passed_group_total": 0,
                "failed_group_total": len(synthetic_group_results),
                "issue_classification_status": "environment_issue",
                "issue_classification_label": "运行环境问题",
                "issue_category_counts": {"environment_issue": len(synthetic_group_results)},
                "failed_command_total": 0,
                "failed_group_classifications": [
                    {
                        "group": group,
                        "status": "failed",
                        "failed_command_total": 0,
                        "issue_classification_status": "environment_issue",
                        "issue_classification_label": "运行环境问题",
                        "evidence_snippets": [error_message],
                    }
                    for group in selected_groups
                ],
                "expected_warning_total": 0,
                "test_expectation_mismatch_total": 0,
                "production_degradation_total": 0,
                "environment_issue_total": len(synthetic_group_results),
                "unknown_failure_total": 0,
                "summary_path": None,
                "artifact_dir": None,
                "exit_code": None,
                "stdout_tail": [],
                "stderr_tail": [error_message],
                "environment_error": "missing_pytest_dependency",
                "error": error_message,
            }

        cmd = [python_bin, "scripts/run_regression_smoke.py"]
        for group in selected_groups:
            cmd.extend(["--group", group])

        if log is not None:
            log(f"[regression-smoke] $ {' '.join(cmd)}")

        started_at = datetime.now().isoformat(timespec="seconds")
        completed = subprocess.run(
            cmd,
            cwd=str(self._project_root()),
            capture_output=True,
            text=True,
        )
        finished_at = datetime.now().isoformat(timespec="seconds")
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        summary_path: str | None = None
        overall_status = "failed"
        summary_payload: dict[str, Any] = {}

        for line in stdout_text.splitlines():
            text = str(line or "").strip()
            if text.startswith("[smoke] summary="):
                summary_path = text.split("=", 1)[1].strip() or None
            elif text.startswith("[smoke] overall_status="):
                overall_status = text.split("=", 1)[1].strip() or "failed"

        if summary_path:
            try:
                loaded_payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
                if isinstance(loaded_payload, dict):
                    summary_payload = loaded_payload
                    overall_status = str(loaded_payload.get("overall_status") or overall_status or "failed").strip() or "failed"
            except Exception:
                summary_payload = {}

        if log is not None:
            for line in stdout_text.splitlines()[-20:]:
                text = str(line or "").strip()
                if text:
                    log(text)
            for line in stderr_text.splitlines()[-20:]:
                text = str(line or "").strip()
                if text:
                    log(f"[regression-smoke][stderr] {text}")

        group_results = summary_payload.get("group_results") or []
        passed_group_total = sum(
            1
            for item in group_results
            if str((item or {}).get("status") or "").strip() == "passed"
        )
        failed_group_total = sum(
            1
            for item in group_results
            if str((item or {}).get("status") or "").strip() == "failed"
        )
        return {
            "status": "success" if completed.returncode == 0 and overall_status == "passed" else "degraded",
            "started_at": started_at,
            "finished_at": finished_at,
            "groups": selected_groups,
            "group_results": group_results,
            "overall_status": overall_status,
            "passed_group_total": passed_group_total,
            "failed_group_total": failed_group_total,
            "issue_classification_status": summary_payload.get("issue_classification_status"),
            "issue_classification_label": summary_payload.get("issue_classification_label"),
            "issue_category_counts": summary_payload.get("issue_category_counts") or {},
            "failed_command_total": int(summary_payload.get("failed_command_total") or 0),
            "failed_group_classifications": summary_payload.get("failed_group_classifications") or [],
            "expected_warning_total": int(summary_payload.get("expected_warning_total") or 0),
            "test_expectation_mismatch_total": int(summary_payload.get("test_expectation_mismatch_total") or 0),
            "production_degradation_total": int(summary_payload.get("production_degradation_total") or 0),
            "environment_issue_total": int(summary_payload.get("environment_issue_total") or 0),
            "unknown_failure_total": int(summary_payload.get("unknown_failure_total") or 0),
            "summary_path": summary_path,
            "artifact_dir": summary_payload.get("artifact_dir"),
            "exit_code": int(completed.returncode or 0),
            "stdout_tail": stdout_text.splitlines()[-20:],
            "stderr_tail": stderr_text.splitlines()[-20:],
        }

    @staticmethod
    def _read_holding_period_snapshot(settings: Settings) -> dict[str, Any]:
        snapshot = {
            "bond_fund_total": 0,
            "holding_period_threshold_days": int(getattr(settings, "exclude_holding_days_gt", 90) or 90),
            "holding_period_known_total": 0,
            "holding_period_no_signal_total": 0,
            "holding_period_name_unparsed_total": 0,
            "holding_period_unknown_total": 0,
            "holding_period_generic_total": 0,
            "holding_period_blocked_total": 0,
            "holding_period_within_threshold_total": 0,
            "holding_period_no_signal_code_sum": 0,
            "holding_period_no_signal_code_square_sum": 0,
            "holding_period_name_unparsed_code_sum": 0,
            "holding_period_name_unparsed_code_square_sum": 0,
            "holding_period_unknown_code_sum": 0,
            "holding_period_unknown_code_square_sum": 0,
            "holding_period_generic_code_sum": 0,
            "holding_period_generic_code_square_sum": 0,
            "holding_period_blocked_code_sum": 0,
            "holding_period_blocked_code_square_sum": 0,
            "holding_period_within_threshold_code_sum": 0,
            "holding_period_within_threshold_code_square_sum": 0,
        }
        raw_db_path = getattr(settings, "database_path", None)
        if not raw_db_path:
            return snapshot

        db_path = Path(raw_db_path)
        if not db_path.exists() or not db_path.is_file():
            return snapshot

        try:
            conn = sqlite3.connect(str(db_path))
        except sqlite3.Error:
            return snapshot
        try:
            available_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(funds)").fetchall()
            }
            if not available_columns:
                return snapshot
            source_fields = {
                name: name if name in available_columns else f"NULL AS {name}"
                for name in ("fcode", "shortname", "fullname", "has_holding_period", "holding_days", "holding_period_source", "holding_keywords")
            }
            fund_rows = conn.execute(
                f"""
                SELECT {source_fields['fcode']}, {source_fields['shortname']}, {source_fields['fullname']},
                       {source_fields['has_holding_period']},
                       {source_fields['holding_days']}, {source_fields['holding_period_source']},
                       {source_fields['holding_keywords']}
                FROM funds
                WHERE ftype LIKE '债券型%'
                """
            ).fetchall()
        except sqlite3.Error:
            fund_rows = None
        finally:
            conn.close()

        if fund_rows is None:
            return snapshot

        threshold = snapshot["holding_period_threshold_days"]
        for raw_code, shortname, fullname, has_holding_period, raw_days, source, holding_keywords in fund_rows:
            code_text = str(raw_code or "").strip()
            code_int = int(code_text) if code_text.isdigit() else 0
            days = normalize_holding_days(raw_days)
            holding_name = combine_holding_period_names(shortname, fullname)
            effective_days = resolve_holding_period_days(
                holding_days=raw_days, fund_name=holding_name,
                holding_period_source=source, holding_keywords=holding_keywords,
            )
            gate_reason = holding_period_gate_reason(
                has_holding_period=has_holding_period,
                holding_days=raw_days,
                holding_period_source=source,
                threshold_days=threshold,
                fund_name=holding_name,
                holding_keywords=holding_keywords,
            )
            snapshot["bond_fund_total"] += 1
            if effective_days is not None and effective_days > 0:
                snapshot["holding_period_known_total"] += 1
            no_signal = (
                gate_reason is None
                and raw_days is None
                and effective_days is None
                and bool(holding_name)
            ) or is_reviewed_no_holding_signal(
                has_holding_period=has_holding_period,
                holding_days=raw_days,
                holding_period_source=source,
            )
            if days == 0 and gate_reason is None or no_signal:
                snapshot["holding_period_no_signal_total"] += 1
                snapshot["holding_period_no_signal_code_sum"] += code_int
                snapshot["holding_period_no_signal_code_square_sum"] += code_int * code_int
            if gate_reason == HOLDING_DAYS_UNKNOWN:
                snapshot["holding_period_unknown_total"] += 1
                snapshot["holding_period_unknown_code_sum"] += code_int
                snapshot["holding_period_unknown_code_square_sum"] += code_int * code_int
            if has_holding_period == 1 and (days == 999 or effective_days == 999):
                snapshot["holding_period_generic_total"] += 1
                snapshot["holding_period_generic_code_sum"] += code_int
                snapshot["holding_period_generic_code_square_sum"] += code_int * code_int
            if gate_reason == HOLDING_PERIOD_LIMIT:
                snapshot["holding_period_blocked_total"] += 1
                snapshot["holding_period_blocked_code_sum"] += code_int
                snapshot["holding_period_blocked_code_square_sum"] += code_int * code_int
            if effective_days is not None and 1 <= effective_days <= threshold:
                snapshot["holding_period_within_threshold_total"] += 1
                snapshot["holding_period_within_threshold_code_sum"] += code_int
                snapshot["holding_period_within_threshold_code_square_sum"] += code_int * code_int
        return snapshot

    @staticmethod
    def _build_holding_period_change_summary(
        *,
        before_snapshot: dict[str, Any] | None = None,
        after_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        before = dict(before_snapshot or {})
        after = dict(after_snapshot or {})
        before.setdefault("holding_period_no_signal_total", before.get("holding_period_unknown_total", 0))
        before.setdefault("holding_period_no_signal_code_sum", before.get("holding_period_unknown_code_sum", 0))
        before.setdefault("holding_period_no_signal_code_square_sum", before.get("holding_period_unknown_code_square_sum", 0))
        before.setdefault("holding_period_name_unparsed_total", before.get("holding_period_unknown_total", 0))
        before.setdefault("holding_period_name_unparsed_code_sum", before.get("holding_period_unknown_code_sum", 0))
        before.setdefault("holding_period_name_unparsed_code_square_sum", before.get("holding_period_unknown_code_square_sum", 0))
        after.setdefault("holding_period_no_signal_total", after.get("holding_period_unknown_total", 0))
        after.setdefault("holding_period_no_signal_code_sum", after.get("holding_period_unknown_code_sum", 0))
        after.setdefault("holding_period_no_signal_code_square_sum", after.get("holding_period_unknown_code_square_sum", 0))
        after.setdefault("holding_period_name_unparsed_total", after.get("holding_period_unknown_total", 0))
        after.setdefault("holding_period_name_unparsed_code_sum", after.get("holding_period_unknown_code_sum", 0))
        after.setdefault("holding_period_name_unparsed_code_square_sum", after.get("holding_period_unknown_code_square_sum", 0))
        tracked_fields = [
            "holding_period_known_total",
            "holding_period_no_signal_total",
            "holding_period_name_unparsed_total",
            "holding_period_unknown_total",
            "holding_period_generic_total",
            "holding_period_blocked_total",
            "holding_period_within_threshold_total",
            "holding_period_no_signal_code_sum",
            "holding_period_no_signal_code_square_sum",
            "holding_period_name_unparsed_code_sum",
            "holding_period_name_unparsed_code_square_sum",
            "holding_period_unknown_code_sum",
            "holding_period_unknown_code_square_sum",
            "holding_period_generic_code_sum",
            "holding_period_generic_code_square_sum",
            "holding_period_blocked_code_sum",
            "holding_period_blocked_code_square_sum",
            "holding_period_within_threshold_code_sum",
            "holding_period_within_threshold_code_square_sum",
        ]
        changed = any(int(before.get(field) or 0) != int(after.get(field) or 0) for field in tracked_fields)
        return {
            "changed": changed,
            "holding_period_threshold_days": int(
                after.get("holding_period_threshold_days")
                or before.get("holding_period_threshold_days")
                or 90
            ),
            "bond_fund_total_before": int(before.get("bond_fund_total") or 0),
            "bond_fund_total_after": int(after.get("bond_fund_total") or 0),
            "holding_period_known_delta": int(after.get("holding_period_known_total") or 0) - int(before.get("holding_period_known_total") or 0),
            "holding_period_no_signal_delta": int(after.get("holding_period_no_signal_total") or 0) - int(before.get("holding_period_no_signal_total") or 0),
            "holding_period_name_unparsed_delta": int(after.get("holding_period_name_unparsed_total") or 0) - int(before.get("holding_period_name_unparsed_total") or 0),
            "holding_period_unknown_delta": int(after.get("holding_period_unknown_total") or 0) - int(before.get("holding_period_unknown_total") or 0),
            "holding_period_generic_delta": int(after.get("holding_period_generic_total") or 0) - int(before.get("holding_period_generic_total") or 0),
            "holding_period_blocked_delta": int(after.get("holding_period_blocked_total") or 0) - int(before.get("holding_period_blocked_total") or 0),
            "holding_period_within_threshold_delta": int(after.get("holding_period_within_threshold_total") or 0) - int(before.get("holding_period_within_threshold_total") or 0),
            "before_snapshot": before,
            "after_snapshot": after,
        }

    @staticmethod
    def _build_maintenance_data_change_summary(
        *,
        nav_history_summary: dict[str, Any] | None = None,
        autostock_nav_tail_fill_summary: dict[str, Any] | None = None,
        plain_open_tail_follow_up_summary: dict[str, Any] | None = None,
        long_term_history_summary: dict[str, Any] | None = None,
        holding_period_change_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        nav_result = (nav_history_summary or {}).get("maintenance_result") or {}
        autostock_result = (autostock_nav_tail_fill_summary or {}).get("maintenance_result") or {}
        plain_open_tail_result = plain_open_tail_follow_up_summary or {}
        long_term_result = (long_term_history_summary or {}).get("maintenance_result") or {}
        holding_summary = holding_period_change_summary or {}
        nav_recovered_total = int(nav_result.get("recovered_total") or 0)
        nav_records_inserted = int(nav_result.get("nav_records_inserted") or 0)
        autostock_shadow_fill_ready_total = int(autostock_result.get("shadow_fill_ready_total") or 0)
        plain_open_tail_follow_up_changed_total = int(plain_open_tail_result.get("changed_total") or 0)
        long_term_annual_success_total = int(long_term_result.get("annual_success_total") or 0)
        long_term_rank_success_total = int(long_term_result.get("rank_success_total") or 0)
        holding_period_changed = bool(holding_summary.get("changed"))
        holding_period_no_signal_delta = holding_summary.get("holding_period_no_signal_delta")
        if holding_period_no_signal_delta is None or int(holding_period_no_signal_delta or 0) == 0:
            holding_period_no_signal_delta = holding_summary.get("holding_period_unknown_delta")
        changed = bool(
            nav_recovered_total > 0
            or nav_records_inserted > 0
            or autostock_shadow_fill_ready_total > 0
            or plain_open_tail_follow_up_changed_total > 0
            or long_term_annual_success_total > 0
            or long_term_rank_success_total > 0
            or holding_period_changed
        )
        return {
            "changed": changed,
            "nav_recovered_total": nav_recovered_total,
            "nav_records_inserted": nav_records_inserted,
            "autostock_shadow_fill_ready_total": autostock_shadow_fill_ready_total,
            "plain_open_tail_follow_up_changed_total": plain_open_tail_follow_up_changed_total,
            "long_term_annual_success_total": long_term_annual_success_total,
            "long_term_rank_success_total": long_term_rank_success_total,
            "holding_period_changed": holding_period_changed,
            "holding_period_known_delta": int(holding_summary.get("holding_period_known_delta") or 0),
            "holding_period_no_signal_delta": int(holding_period_no_signal_delta or 0),
            "holding_period_name_unparsed_delta": int(holding_summary.get("holding_period_name_unparsed_delta") or 0),
            "holding_period_unknown_delta": int(holding_summary.get("holding_period_unknown_delta") or 0),
            "holding_period_generic_delta": int(holding_summary.get("holding_period_generic_delta") or 0),
            "holding_period_blocked_delta": int(holding_summary.get("holding_period_blocked_delta") or 0),
            "holding_period_within_threshold_delta": int(holding_summary.get("holding_period_within_threshold_delta") or 0),
        }

    @staticmethod
    def _extract_maintenance_risk_refresh_target_fcodes(
        *,
        nav_history_summary: dict[str, Any] | None = None,
    ) -> list[str]:
        summary = nav_history_summary or {}
        raw_codes: list[Any] = []
        if isinstance(summary.get("risk_refresh_target_fcodes"), list):
            raw_codes.extend(summary.get("risk_refresh_target_fcodes") or [])
        maintenance_result = summary.get("maintenance_result") or {}
        for key in ("risk_refresh_target_codes_sample", "attempted_codes_sample", "recovered_codes_sample"):
            values = maintenance_result.get(key)
            if isinstance(values, list):
                raw_codes.extend(values)

        result: list[str] = []
        seen_codes: set[str] = set()
        for raw_code in raw_codes:
            code = str(raw_code or "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            result.append(code)
        return result

    def _write_risk_refresh_target_fcodes_file(
        self,
        *,
        target_fcodes: list[str],
        reason: str,
    ) -> Path | None:
        settings = getattr(self, "settings", None)
        project_root = getattr(settings, "project_root", None)
        if project_root is None:
            return None
        normalized_codes: list[str] = []
        seen_codes: set[str] = set()
        for raw_code in target_fcodes:
            code = str(raw_code or "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            normalized_codes.append(code)
        if not normalized_codes:
            return None

        target_dir = Path(project_root) / "tmp"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"maintenance_risk_refresh_targets_{uuid.uuid4().hex[:12]}.json"
        target_path.write_text(
            json.dumps(
                {
                    "target_fcodes": normalized_codes,
                    "reason": str(reason or "maintenance_data_changed").strip() or "maintenance_data_changed",
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return target_path

    @staticmethod
    def _classify_scheduler_skip_reason(skip_reason: Any) -> str | None:
        return scheduler_status_support.classify_scheduler_skip_reason(skip_reason)

    @staticmethod
    def _classify_scheduler_subtask_no_action(summary: dict[str, Any] | None) -> str | None:
        return scheduler_status_support.classify_scheduler_subtask_no_action(summary)

    @classmethod
    def _build_task_closure_summary(
        cls,
        *,
        task_key: str,
        task_name: str,
        result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return scheduler_status_support.build_task_closure_summary(
            task_key=task_key,
            task_name=task_name,
            result=result,
            classify_skip_reason=cls._classify_scheduler_skip_reason,
            classify_subtask_no_action=cls._classify_scheduler_subtask_no_action,
        )

    def _refresh_report_from_existing_outputs(
        self,
        *,
        collection_profile: str,
        log: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        if log is not None:
            log("有效净值快照未变化，但数据库维护已有实质进展，开始基于现有核心产物重建项目报告")
        if not self._workflow_execution_available():
            if log is not None:
                log("当前上下文未提供可执行工作流入口，跳过项目报告重建")
            return {
                "status": "skipped",
                "skip_reason": "workflow_executor_unavailable",
            }
        return self._run_workflow_in_subprocess(
            {
                "run_collection": False,
                "run_screening": True,
                "reuse_existing_outputs": True,
                "collection_profile": collection_profile,
            },
            log=log,
        )

    def _build_risk_free_rate_drift_audit(
        self,
        *,
        alert_bps: float = 25.0,
    ) -> dict[str, Any]:
        """P1-03.3 / B7.1: 算出当前 runtime override vs core 基线的漂移。

        维护流程把结果写入 summary 即可；是否触发告警由 ``alert_bps``（默认 25bp）控制。
        写 override 由运维手工 ``scripts/refresh_risk_free_rate.py --rate ...`` 触发，
        本方法纯粹做观察与告警，不主动改值。
        """

        import sys as _sys

        core_root = Path(
            getattr(self.settings, "core_root", None)
            or (self.settings.project_root / "core")
        ).resolve()
        if str(core_root) not in _sys.path:
            _sys.path.insert(0, str(core_root))

        from bond_fund_screening.runtime_overrides import get_risk_free_rate  # type: ignore
        from bond_fund_screening.config import RISK_FREE_RATE as _baseline_rfr  # type: ignore

        resolution = get_risk_free_rate(_baseline_rfr)
        drift_bps = round(abs(resolution.value - float(_baseline_rfr)) * 10000, 2)
        return {
            "baseline": float(_baseline_rfr),
            "current_value": float(resolution.value),
            "current_source": resolution.source,
            "current_updated_at": resolution.updated_at,
            "drift_bps": drift_bps,
            "alert_bps_threshold": float(alert_bps),
            "alert_triggered": drift_bps > float(alert_bps),
        }

    def _attempt_refresh_risk_free_rate(
        self,
        *,
        alert_bps: float = 25.0,
    ) -> dict[str, Any]:
        script_path = self.settings.project_root / "scripts" / "refresh_risk_free_rate.py"
        command = [sys.executable, str(script_path), "--try-api", "--alert-bps", str(alert_bps)]
        result = subprocess.run(
            command,
            cwd=str(self.settings.project_root),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        payload: dict[str, Any] = {
            "command": [str(part) for part in command],
            "returncode": int(result.returncode),
            "stdout": stdout,
            "stderr": stderr,
            "succeeded": result.returncode == 0,
        }
        if stdout:
            try:
                parsed = json.loads(stdout)
                if isinstance(parsed, dict):
                    payload["report"] = parsed
            except Exception:
                payload["stdout_parse_error"] = True
        return payload

    def _rescreen_report_after_maintenance(
        self,
        *,
        collection_profile: str,
        risk_refresh_target_fcodes: list[str] | None = None,
        log: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """P0-03.1 / B5: 周维护数据实质变化后做一次真正的重评分。

        与 ``_refresh_report_from_existing_outputs`` 的差异在于 ``reuse_existing_outputs=False``：
        不仅重建报告文案，还让核心筛选引擎基于最新的 holding_days/长期历史/净值补齐样本
        重新产出打分与榜单。仍然 ``run_collection=False``——维护阶段刚采过数据，避免双抓。
        """
        if log is not None:
            log(
                "数据库维护带来实质性样本变化，触发一次完整重评分"
                "（reuse_existing_outputs=False，并先执行 changed-nav 风险重算）"
            )
        if not self._workflow_execution_available():
            if log is not None:
                log("当前上下文未提供可执行工作流入口，跳过维护后重评分")
            return {
                "status": "skipped",
                "skip_reason": "workflow_executor_unavailable",
            }
        target_file = self._write_risk_refresh_target_fcodes_file(
            target_fcodes=risk_refresh_target_fcodes or [],
            reason="maintenance_changed_rescreen",
        )
        if target_file is not None and log is not None:
            log(f"维护后风险重算目标基金数: {len(risk_refresh_target_fcodes or [])}, target_file={target_file}")
        try:
            return self._run_workflow_in_subprocess(
                {
                    "run_collection": False,
                    "run_screening": True,
                    "reuse_existing_outputs": False,
                    "collection_profile": collection_profile,
                    "force_calculate_risk": True,
                    "risk_refresh_changed_nav_only": True,
                    "risk_refresh_target_fcodes_file": str(target_file) if target_file else "",
                },
                log=log,
            )
        finally:
            if target_file is not None:
                try:
                    target_file.unlink(missing_ok=True)
                except Exception as exc:
                    if log is not None:
                        log(f"维护后风险重算目标文件清理失败: {target_file}, error={exc}")

    def _rescreen_report_after_risk_parameter_change(
        self,
        *,
        collection_profile: str,
        log: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Risk formula parameter changes require full risk recalculation.

        Changed-nav refresh is intentionally not used here: a risk-free-rate
        update changes Sharpe for every fund with an existing risk row, even
        when NAV history has not changed.
        """
        if log is not None:
            log(
                "risk_free_rate 已真实更新，触发完整风险指标重算与重评分"
                "（force_calculate_risk=True，changed-nav-only=False）"
            )
        if not self._workflow_execution_available():
            if log is not None:
                log("当前上下文未提供可执行工作流入口，跳过风险参数变更后的重评分")
            return {
                "status": "skipped",
                "skip_reason": "workflow_executor_unavailable",
            }
        return self._run_workflow_in_subprocess(
            {
                "run_collection": False,
                "run_screening": True,
                "reuse_existing_outputs": False,
                "collection_profile": collection_profile,
                "force_calculate_risk": True,
                "risk_refresh_changed_nav_only": False,
            },
            log=log,
        )

    @staticmethod
    def _risk_free_rate_refresh_changed(refresh_payload: dict[str, Any] | None) -> bool:
        if not isinstance(refresh_payload, dict):
            return False
        report = refresh_payload.get("report")
        if not isinstance(report, dict) or not bool(report.get("applied")):
            return False
        current = report.get("current_value")
        post = report.get("post_value")
        try:
            return abs(float(current) - float(post)) > 1e-12
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        """Return namespace-local PID visibility for diagnostics only."""
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _ensure_scheduler_instance_id(self) -> str:
        instance_id = str(getattr(self, "scheduler_instance_id", "") or "").strip()
        if not instance_id:
            instance_id = uuid.uuid4().hex
            self.scheduler_instance_id = instance_id
        return instance_id

    @staticmethod
    def _scheduler_lock_path(settings: Any) -> Path:
        configured = getattr(settings, "scheduler_lock_file", None)
        if configured:
            return Path(configured)
        pid_file = getattr(settings, "scheduler_pid_file", None)
        if pid_file:
            return Path(pid_file).with_suffix(".lease")
        state_file = Path(getattr(settings, "scheduler_state_file"))
        return state_file.with_name("scheduler.lease")

    @staticmethod
    def _read_json_dict(path: Any) -> dict[str, Any]:
        if not path:
            return {}
        try:
            resolved = Path(path)
            if not resolved.exists() or not resolved.is_file():
                return {}
            payload = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def _probe_scheduler_lease(cls, settings: Any) -> dict[str, Any]:
        """Inspect the shared flock lease without relying on namespace-local PIDs."""
        lease_path = cls._scheduler_lock_path(settings)
        result: dict[str, Any] = {
            "path": str(lease_path),
            "exists": lease_path.exists(),
            "held": False,
            "probe_error": None,
            "metadata": cls._read_json_dict(lease_path),
        }
        if not lease_path.exists():
            return result
        try:
            handle = lease_path.open("r+", encoding="utf-8")
        except OSError as exc:
            result["probe_error"] = str(exc)
            return result
        acquired = False
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                result["held"] = True
            except OSError as exc:
                result["probe_error"] = str(exc)
        finally:
            if acquired:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            handle.close()
        return result

    @classmethod
    def _scheduler_runtime_evidence(
        cls,
        settings: Any,
        *,
        state_payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Build fail-closed scheduler liveness evidence across PID namespaces."""
        state = dict(state_payload or {})
        if not state:
            state_file = getattr(settings, "scheduler_state_file", None)
            if state_file:
                state = SchedulerStateStore(state_file).snapshot()
        lease = cls._probe_scheduler_lease(settings)
        heartbeat = cls._read_json_dict(getattr(settings, "scheduler_heartbeat_file", None))
        current_time = now or datetime.now()
        heartbeat_interval = max(
            1,
            int(getattr(settings, "scheduler_heartbeat_interval_seconds", 60) or 60),
        )
        stale_after_seconds = max(
            heartbeat_interval * 3,
            int(
                getattr(
                    settings,
                    "scheduler_heartbeat_stale_after_seconds",
                    heartbeat_interval * 3,
                )
                or heartbeat_interval * 3
            ),
        )

        def freshness(value: Any) -> tuple[bool, int | None, str | None]:
            parsed = cls._parse_datetime(value)
            if parsed is None:
                return False, None, None
            compared_now = current_time
            if parsed.tzinfo is None and compared_now.tzinfo is not None:
                parsed = parsed.replace(tzinfo=compared_now.tzinfo)
            elif parsed.tzinfo is not None and compared_now.tzinfo is None:
                compared_now = compared_now.replace(tzinfo=parsed.tzinfo)
            age_seconds = max(0, int((compared_now - parsed).total_seconds()))
            return age_seconds <= stale_after_seconds, age_seconds, parsed.isoformat(timespec="seconds")

        state_heartbeat_fresh, state_heartbeat_age, state_heartbeat_at = freshness(
            state.get("last_heartbeat_at")
        )
        file_heartbeat_fresh, file_heartbeat_age, file_heartbeat_at = freshness(
            heartbeat.get("heartbeat_at")
        )
        state_instance_id = str(state.get("scheduler_instance_id") or "").strip()
        lease_metadata = lease.get("metadata") if isinstance(lease.get("metadata"), dict) else {}
        lease_instance_id = str((lease_metadata or {}).get("scheduler_instance_id") or "").strip()
        heartbeat_instance_id = str(heartbeat.get("scheduler_instance_id") or "").strip()
        state_host_context = str(state.get("host_context") or "").strip()
        lease_host_context = str((lease_metadata or {}).get("host_context") or "").strip()
        heartbeat_host_context = str(heartbeat.get("host_context") or "").strip()
        instance_match = bool(
            state_instance_id
            and state_instance_id == lease_instance_id
            and state_instance_id == heartbeat_instance_id
        )
        host_context_match = bool(
            state_host_context
            and (not lease_host_context or lease_host_context == state_host_context)
            and (not heartbeat_host_context or heartbeat_host_context == state_host_context)
        )
        heartbeat_running = bool(heartbeat.get("scheduler_running"))
        claimed_running = bool(state.get("scheduler_running"))
        alive = bool(
            claimed_running
            and lease.get("held")
            and instance_match
            and host_context_match
            and heartbeat_running
            and state_heartbeat_fresh
            and file_heartbeat_fresh
        )
        reasons: list[str] = []
        if not claimed_running:
            reasons.append("state_not_running")
        if not lease.get("held"):
            reasons.append("scheduler_lease_not_held")
        if not instance_match:
            reasons.append("scheduler_instance_mismatch")
        if not host_context_match:
            reasons.append("scheduler_host_context_mismatch")
        if not heartbeat_running:
            reasons.append("heartbeat_not_running")
        if not state_heartbeat_fresh:
            reasons.append("state_heartbeat_stale")
        if not file_heartbeat_fresh:
            reasons.append("heartbeat_file_stale")
        pid = int(state.get("pid") or 0)
        return {
            "alive": alive,
            "claimed_running": claimed_running,
            "scheduler_instance_id": state_instance_id or None,
            "instance_match": instance_match,
            "host_context_match": host_context_match,
            "lease": lease,
            "heartbeat": {
                "scheduler_running": heartbeat_running,
                "scheduler_instance_id": heartbeat_instance_id or None,
                "heartbeat_at": file_heartbeat_at,
                "fresh": file_heartbeat_fresh,
                "age_seconds": file_heartbeat_age,
            },
            "state_heartbeat": {
                "heartbeat_at": state_heartbeat_at,
                "fresh": state_heartbeat_fresh,
                "age_seconds": state_heartbeat_age,
            },
            "stale_after_seconds": stale_after_seconds,
            "reason_codes": reasons,
            "pid": pid or None,
            "pid_alive_diagnostic": cls._pid_is_alive(pid) if pid else False,
        }

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _weekday_to_index(value: str) -> int:
        mapping = {
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
        }
        return mapping.get(str(value or "").strip().lower(), 6)

    def _recover_state_if_stale(self) -> dict[str, Any]:
        if not self._owns_scheduler_lease():
            return {
                "scheduler_recovered": False,
                "manual_run_recovery": {"recovered_total": 0, "recovered_request_ids": []},
                "task_runtime_recovery": {"recovered_total": 0, "recovered_task_names": []},
                "recovery_deferred": True,
                "recovery_reason": "scheduler_lease_not_owned",
            }
        payload = self.state_store.snapshot()
        scheduler_recovered = False
        persisted_instance_id = str(payload.get("scheduler_instance_id") or "").strip()
        current_instance_id = self._ensure_scheduler_instance_id()
        if payload.get("scheduler_running") and persisted_instance_id != current_instance_id:
            self.state_store.recover_stale_run("scheduler_lease_replaced")
            self._write_stopped_heartbeat_file()
            scheduler_recovered = True

        runtime_recovery = {"recovered_total": 0, "recovered_task_names": []}
        workflow_store = getattr(getattr(self, "workflow_manager", None), "store", None)
        workflow_lease_active = False
        if workflow_store is not None and hasattr(workflow_store, "has_active_lock"):
            try:
                workflow_lease_active = bool(workflow_store.has_active_lock())
            except Exception:
                workflow_lease_active = True
        if workflow_lease_active:
            return {
                "scheduler_recovered": scheduler_recovered,
                "manual_run_recovery": {"recovered_total": 0, "recovered_request_ids": []},
                "task_runtime_recovery": runtime_recovery,
                "recovery_deferred": True,
                "recovery_reason": "active_workflow_lease_detected",
            }
        manual_run_recovery = self.state_store.recover_orphan_manual_run_entries(
            "scheduler_lease_replaced"
        )
        refreshed_payload = self.state_store.snapshot()
        if not refreshed_payload.get("scheduler_running"):
            runtime_recovery = self.task_recorder.recover_stale_tasks("scheduler_lease_replaced")

        return {
            "scheduler_recovered": scheduler_recovered,
            "manual_run_recovery": manual_run_recovery,
            "task_runtime_recovery": runtime_recovery,
            "recovery_deferred": False,
            "recovery_reason": "exclusive_scheduler_lease_owned",
        }

    def _refresh_runtime_status_summary(self) -> None:
        """Refresh persisted scheduler runtime digests from the current runtime state."""
        load_scheduler_status(
            self.settings,
            apply_runtime_recovery=False,
        )

    def _log_recovery_summary(self, recovery: dict[str, Any]) -> None:
        manual_run_recovery = recovery.get("manual_run_recovery") or {}
        manual_recovered_total = int(manual_run_recovery.get("recovered_total") or 0)
        runtime_recovery = recovery.get("task_runtime_recovery") or {}
        recovered_total = int(runtime_recovery.get("recovered_total") or 0)
        recovered_names = ", ".join(runtime_recovery.get("recovered_task_names") or [])
        if recovery.get("scheduler_recovered"):
            self._log("检测到旧调度器已失联，已自动恢复调度状态")
        if manual_recovered_total > 0:
            self._log(f"已自动收口遗留手动补跑请求 {manual_recovered_total} 条")
        if recovered_total > 0:
            suffix = f": {recovered_names}" if recovered_names else ""
            self._log(f"已自动收口遗留运行中任务 {recovered_total} 条{suffix}")

    def _write_heartbeat_file(self) -> None:
        runtime_api_config = self._runtime_api_config()
        payload = {
            "heartbeat_at": datetime.now().isoformat(timespec="seconds"),
            "pid": os.getpid(),
            "scheduler_instance_id": self._ensure_scheduler_instance_id(),
            "scheduler_running": True,
            "host_context": self._host_context(),
            "lease_file": str(self._scheduler_lock_path(self.settings)),
            "current_task": self.state_store.snapshot().get("current_task"),
            "runtime_api_config": runtime_api_config,
        }
        self.settings.scheduler_heartbeat_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _write_stopped_heartbeat_file(self) -> None:
        payload = {
            "heartbeat_at": datetime.now().isoformat(timespec="seconds"),
            "pid": None,
            "scheduler_instance_id": self._ensure_scheduler_instance_id(),
            "scheduler_running": False,
            "host_context": self._host_context(),
            "lease_file": str(self._scheduler_lock_path(self.settings)),
            "current_task": None,
            "runtime_api_config": {},
        }
        self.settings.scheduler_heartbeat_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _heartbeat_loop(self) -> None:
        interval = max(5, int(self.settings.scheduler_heartbeat_interval_seconds))
        while not self.stop_event.is_set():
            if self._should_self_retire():
                self._log("检测到当前宿主已不再允许承载调度器，准备自动退场")
                self.shutdown()
                break
            try:
                self.state_store.heartbeat(jobs=self._serialize_jobs(), **self._state_store_runtime_api_config_kwargs())
            except TypeError:
                self.state_store.heartbeat()
                self._persist_runtime_api_config()
            self._write_heartbeat_file()
            self.stop_event.wait(interval)

    def _start_heartbeat(self) -> None:
        self.stop_event.clear()
        try:
            self.state_store.heartbeat(jobs=self._serialize_jobs(), **self._state_store_runtime_api_config_kwargs())
        except TypeError:
            self.state_store.heartbeat()
            self._persist_runtime_api_config()
        self._write_heartbeat_file()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self.stop_event.set()
        if (
            self.heartbeat_thread
            and self.heartbeat_thread.is_alive()
            and threading.current_thread() is not self.heartbeat_thread
        ):
            self.heartbeat_thread.join(timeout=2)
        self.heartbeat_thread = None
        self._write_stopped_heartbeat_file()

    @staticmethod
    def _classify_log_severity(logs: list[str]) -> dict[str, Any]:
        from backend.collection_log_summary import classify_collection_log_severity

        return classify_collection_log_severity(logs)

    @staticmethod
    def _parse_iso_date(value: Any) -> datetime.date | None:
        normalized = str(value or "").strip()
        if not normalized:
            return None
        try:
            return datetime.strptime(normalized[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    def _load_latest_empty_detail_report(self) -> tuple[Path | None, dict[str, Any] | None]:
        core_root = getattr(self.settings, "core_root", None)
        if not core_root:
            return None, None
        report_dir = Path(core_root) / "logs" / "tasks"
        if not report_dir.exists():
            return None, None

        candidates = sorted(
            report_dir.glob("detail_empty_funds_*.json"),
            key=lambda path: (path.stat().st_mtime, path.name),
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                return path, payload
        return None, None

    def _load_empty_detail_screening_context(
        self,
        fcodes: list[str],
        *,
        risk_effective_end_date: str,
    ) -> dict[str, dict[str, Any]]:
        normalized_fcodes = sorted({str(code or "").strip() for code in fcodes if str(code or "").strip()})
        if not normalized_fcodes or not self.settings.database_path.exists():
            return {}

        placeholders = ",".join("?" for _ in normalized_fcodes)
        params: list[Any] = []
        params.extend(normalized_fcodes)
        params.append(risk_effective_end_date)

        context_map: dict[str, dict[str, Any]] = {}
        conn = sqlite3.connect(str(self.settings.database_path))
        conn.row_factory = sqlite3.Row
        try:
            fund_columns = {
                str(row["name"] or "").strip()
                for row in conn.execute("PRAGMA table_info(funds)").fetchall()
            }
            is_buyable_expr = "f.is_buyable" if "is_buyable" in fund_columns else "NULL"
            purchase_status_expr = (
                "f.purchase_status" if "purchase_status" in fund_columns else "NULL"
            )
            holding_metadata_expr = ", ".join(
                f"f.{column} AS {column}" if column in fund_columns else f"NULL AS {column}"
                for column in ("fullname", "holding_period_source", "holding_keywords")
            )
            risk_ready_predicate = _risk_metric_ready_predicate(conn, "rm")
            latest_rowid_predicate = _latest_risk_rowid_predicate("rm", "rm_latest")
            query = f"""
                SELECT
                    f.fcode,
                    f.shortname,
                    f.has_holding_period,
                    f.holding_days,
                    {holding_metadata_expr},
                    {is_buyable_expr} AS is_buyable,
                    {purchase_status_expr} AS purchase_status,
                    fr.latest_nav_date,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM fund_risk_metrics rm
                            WHERE rm.fcode = f.fcode
                              AND rm.period = '1y'
                              AND {risk_ready_predicate}
                              AND {latest_rowid_predicate}
                        ) THEN 1 ELSE 0
                    END AS has_any_1y_risk,
                    CASE
                        WHEN ? <> ''
                         AND EXISTS (
                            SELECT 1
                            FROM fund_risk_metrics rm
                            WHERE rm.fcode = f.fcode
                              AND rm.period = '1y'
                              AND COALESCE(rm.end_date, '') = ?
                              AND {risk_ready_predicate}
                              AND {latest_rowid_predicate}
                        ) THEN 1
                        WHEN ? = ''
                         AND EXISTS (
                            SELECT 1
                            FROM fund_risk_metrics rm
                            WHERE rm.fcode = f.fcode
                              AND rm.period = '1y'
                              AND {risk_ready_predicate}
                              AND {latest_rowid_predicate}
                        ) THEN 1
                        ELSE 0
                    END AS has_effective_1y_risk
                FROM funds f
                LEFT JOIN fund_returns fr
                  ON fr.fcode = f.fcode
                WHERE f.fcode IN ({placeholders})
            """
            effective_params = [risk_effective_end_date, risk_effective_end_date, risk_effective_end_date]
            rows = conn.execute(query, effective_params + normalized_fcodes).fetchall()
            for row in rows:
                fcode = str(row["fcode"] or "").strip()
                if not fcode:
                    continue
                context_map[fcode] = {
                    "shortname": row["shortname"],
                    "has_holding_period": int(row["has_holding_period"] or 0),
                    "holding_days": row["holding_days"],
                    "fullname": row["fullname"],
                    "holding_period_source": row["holding_period_source"],
                    "holding_keywords": row["holding_keywords"],
                    "is_buyable": (
                        bool(int(row["is_buyable"]))
                        if row["is_buyable"] is not None
                        else None
                    ),
                    "purchase_status": str(row["purchase_status"] or "").strip() or None,
                    "latest_nav_date": row["latest_nav_date"],
                    "has_any_1y_risk": bool(row["has_any_1y_risk"]),
                    "has_effective_1y_risk": bool(row["has_effective_1y_risk"]),
                }
        finally:
            conn.close()
        return context_map

    @classmethod
    def _classify_empty_detail_report_impact(
        cls,
        report_payload: dict[str, Any],
        screening_context: dict[str, dict[str, Any]],
        *,
        candidate_date: str,
        risk_effective_end_date: str,
        holding_days_threshold: int,
    ) -> dict[str, Any]:
        funds = report_payload.get("funds") or []
        if not isinstance(funds, list):
            funds = []

        candidate_date_obj = cls._parse_iso_date(candidate_date)
        nav_fresh_cutoff = (
            candidate_date_obj - timedelta(days=10)
            if candidate_date_obj is not None
            else None
        )

        impact_counts = {
            "screening_relevant": 0,
            "holding_period_blocked": 0,
            "purchase_blocked": 0,
            "stale_nav_or_missing": 0,
            "missing_effective_1y_risk": 0,
            "unknown_context": 0,
        }
        samples_by_impact: dict[str, list[dict[str, Any]]] = {
            key: [] for key in impact_counts
        }

        for raw_record in funds:
            record = raw_record if isinstance(raw_record, dict) else {}
            fcode = str(record.get("fcode") or "").strip()
            if not fcode:
                continue

            context = screening_context.get(fcode) or {}
            shortname = (
                str(context.get("shortname") or record.get("shortname") or "").strip()
                or None
            )
            has_holding_period = int(context.get("has_holding_period") or 0)
            holding_days = context.get("holding_days")
            is_buyable = context.get("is_buyable")
            purchase_status = str(context.get("purchase_status") or "").strip()
            latest_nav_date = str(context.get("latest_nav_date") or "").strip()
            latest_nav_date_obj = cls._parse_iso_date(latest_nav_date)
            has_effective_1y_risk = bool(context.get("has_effective_1y_risk"))
            has_any_1y_risk = bool(context.get("has_any_1y_risk"))
            holding_name = combine_holding_period_names(shortname, context.get("fullname"))
            holding_reason = holding_period_gate_reason(
                has_holding_period=has_holding_period,
                holding_days=holding_days,
                holding_period_source=context.get("holding_period_source"),
                holding_keywords=context.get("holding_keywords"),
                fund_name=holding_name,
                threshold_days=holding_days_threshold,
            )

            impact = "screening_relevant"
            detail = "详情空响应可能影响正式筛选"
            if not context:
                impact = "unknown_context"
                detail = "数据库中缺少该基金的筛选上下文，按保守口径视为需关注"
            elif holding_reason == HOLDING_PERIOD_LIMIT:
                impact = "holding_period_blocked"
                effective_days = resolve_holding_period_days(
                    holding_days=holding_days,
                    fund_name=holding_name,
                    holding_period_source=context.get("holding_period_source"),
                    holding_keywords=context.get("holding_keywords"),
                )
                detail = f"持有期 {effective_days} 天，已超过 {holding_days_threshold} 天阈值"
            elif is_buyable is False or any(
                marker in purchase_status
                for marker in ("暂停申购", "停止申购", "封闭期", "不可申购")
            ):
                impact = "purchase_blocked"
                detail = f"当前不可申购（{purchase_status or 'is_buyable=0'}），不会进入正式发布候选"
            elif latest_nav_date_obj is None or (
                nav_fresh_cutoff is not None and latest_nav_date_obj < nav_fresh_cutoff
            ):
                impact = "stale_nav_or_missing"
                detail = (
                    f"最新净值日期 {latest_nav_date or '缺失'} 已不在正式筛选新鲜窗口内"
                )
            else:
                risk_ready = has_effective_1y_risk if risk_effective_end_date else has_any_1y_risk
                if not risk_ready:
                    impact = "missing_effective_1y_risk"
                    detail = "缺少正式筛选所需的 1 年风险指标"

            impact_counts[impact] = int(impact_counts.get(impact) or 0) + 1
            sample_bucket = samples_by_impact.setdefault(impact, [])
            if len(sample_bucket) < 10:
                sample_bucket.append(
                    {
                        "fcode": fcode,
                        "shortname": shortname,
                        "detail": detail,
                        "latest_nav_date": latest_nav_date or None,
                        "holding_days": holding_days,
                        "is_buyable": is_buyable,
                        "purchase_status": purchase_status or None,
                    }
                )

        total_count = sum(int(value or 0) for value in impact_counts.values())
        screening_relevant_count = int(impact_counts.get("screening_relevant") or 0) + int(
            impact_counts.get("unknown_context") or 0
        )
        return {
            "available": True,
            "generated_at": report_payload.get("generated_at"),
            "total_empty_fund_count": total_count,
            "candidate_date": candidate_date,
            "risk_effective_end_date": risk_effective_end_date,
            "holding_days_threshold": holding_days_threshold,
            "impact_counts": impact_counts,
            "screening_relevant_count": screening_relevant_count,
            "non_blocking_count": max(total_count - screening_relevant_count, 0),
            "blocking_reason": (
                "screening_relevant_empty_detail_exists"
                if screening_relevant_count > 0
                else "non_blocking_empty_detail_only"
            ),
            "samples_by_impact": samples_by_impact,
        }

    def _build_empty_detail_payload_audit(self) -> dict[str, Any]:
        report_path, report_payload = self._load_latest_empty_detail_report()
        if report_path is None or not isinstance(report_payload, dict):
            return {
                "available": False,
                "report_path": None,
                "screening_relevant_count": 0,
                "non_blocking_count": 0,
                "impact_counts": {},
                "blocking_reason": "report_missing",
            }

        funds = report_payload.get("funds") or []
        if not isinstance(funds, list):
            funds = []
        fcodes = [str((item or {}).get("fcode") or "").strip() for item in funds if isinstance(item, dict)]

        effective_snapshot = load_effective_nav_snapshot(
            self.settings.database_path,
            coverage_ratio=self.settings.effective_nav_coverage_ratio,
            weekday_only=self.settings.effective_nav_weekday_only,
        )
        candidate_date = str(effective_snapshot.candidate_date or "").strip()
        risk_effective_end_date = str(
            effective_snapshot.risk_1y_effective_end_date
            or effective_snapshot.risk_1y_latest_end_date
            or ""
        ).strip()
        screening_context = self._load_empty_detail_screening_context(
            fcodes,
            risk_effective_end_date=risk_effective_end_date,
        )
        audit = self._classify_empty_detail_report_impact(
            report_payload,
            screening_context,
            candidate_date=candidate_date,
            risk_effective_end_date=risk_effective_end_date,
            holding_days_threshold=int(getattr(self.settings, "exclude_holding_days_gt", 90) or 90),
        )
        audit["report_path"] = str(report_path)
        return audit

    def _ensure_single_instance(self) -> None:
        existing_handle = getattr(self, "_scheduler_lease_handle", None)
        if existing_handle is not None and not getattr(existing_handle, "closed", True):
            return
        lease_path = self._scheduler_lock_path(self.settings)
        lease_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lease_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            metadata = self._read_json_dict(lease_path)
            owner_instance_id = str(metadata.get("scheduler_instance_id") or "").strip()
            owner_pid = int(metadata.get("pid") or 0)
            detail = owner_instance_id or (f"pid={owner_pid}" if owner_pid else "unknown")
            raise RuntimeError(
                f"scheduler_singleton_lease_unavailable:{detail}"
            ) from exc
        except Exception:
            handle.close()
            raise

        instance_id = self._ensure_scheduler_instance_id()
        metadata = {
            "scheduler_instance_id": instance_id,
            "pid": os.getpid(),
            "host_context": self._host_context(),
            "acquired_at": datetime.now().isoformat(timespec="seconds"),
        }
        try:
            handle.seek(0)
            handle.truncate()
            json.dump(metadata, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
            raise
        self._scheduler_lease_handle = handle
        self._scheduler_lease_path = lease_path

        pid_file = self.settings.scheduler_pid_file
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(os.getpid()), encoding="utf-8")

    def _owns_scheduler_lease(self) -> bool:
        handle = getattr(self, "_scheduler_lease_handle", None)
        return bool(handle is not None and not getattr(handle, "closed", True))

    def _release_single_instance(self) -> None:
        handle = getattr(self, "_scheduler_lease_handle", None)
        if handle is None:
            return
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except (OSError, ValueError):
            pass
        try:
            handle.close()
        except OSError:
            pass
        self._scheduler_lease_handle = None
        self._scheduler_lease_path = None

    def _cleanup_pid_file(self) -> None:
        pid_file = self.settings.scheduler_pid_file
        if not pid_file.exists():
            return
        try:
            current = int(pid_file.read_text(encoding="utf-8").strip())
        except (OSError, TypeError, ValueError):
            current = 0
        if current in {0, os.getpid()}:
            try:
                pid_file.unlink()
            except OSError:
                pass

    def _serialize_jobs(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if getattr(job, "next_run_time", None) else None,
                    "trigger": str(job.trigger),
                }
            )
        return jobs

    def _last_run_at(self, task_name: str, *, require_satisfied: bool = False) -> datetime | None:
        payload = self.state_store.snapshot()
        last_run = (payload.get("last_runs") or {}).get(task_name) or {}
        if require_satisfied:
            status = str(last_run.get("status") or "").strip().lower()
            skip_reason = str(last_run.get("skip_reason") or "").strip().lower()
            if status == "failed":
                return None
            if status == "skipped" and skip_reason in {
                "workflow_busy",
                "workflow_busy_locked",
                "workflow_busy_state_only",
                "workflow_busy_after_collection",
                "workflow_busy_after_collection_locked",
                "workflow_busy_after_collection_state_only",
            }:
                latest_completed = self._latest_completed_run_at(task_name)
                if latest_completed is not None:
                    return latest_completed
                return None
        for key in ("finished_at", "completed_at", "started_at"):
            parsed = self._parse_datetime(last_run.get(key))
            if parsed is not None:
                return parsed
        return None

    def _latest_completed_run_at(self, task_name: str) -> datetime | None:
        database_path = getattr(self.settings, "database_path", None)
        if not database_path:
            return None
        db_path = Path(database_path)
        if not db_path.exists():
            return None

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                f"""
                SELECT completed_at, started_at
                FROM task_history
                WHERE task_name = ? AND {SCHEDULER_EXECUTION_ROW_SQL}
                  AND status != 'running'
                  AND status != 'failed'
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                (task_name,),
            ).fetchone()
        except sqlite3.Error:
            return None
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass

        if not row:
            return None
        completed_at = self._parse_datetime(row[0]) if row[0] else None
        if completed_at is not None:
            return completed_at
        started_at = self._parse_datetime(row[1]) if row[1] else None
        return started_at

    def _current_latest_report_run_id(self) -> str:
        report_file = Path(getattr(self.settings, "latest_report_file", "") or "")
        if not report_file.exists():
            return ""
        try:
            payload = json.loads(report_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return ""
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("run_id") or "").strip()

    def _resolve_scheduler_runtime_path(self, value: Any) -> Path | None:
        text = str(value or "").strip()
        if not text:
            return None
        path = Path(text)
        if path.exists():
            return path
        project_root = Path(getattr(self.settings, "project_root", Path.cwd()))
        if path.is_absolute():
            parts = path.parts
            if len(parts) > 1 and parts[1] == "app":
                remapped = project_root.joinpath(*parts[2:])
                if artifact_exists(remapped):
                    return remapped
            return path
        candidate = project_root / path
        return candidate if artifact_exists(candidate) else path

    @staticmethod
    def _read_scheduler_text_tail(path: Path, *, max_bytes: int = 5_000_000) -> str:
        try:
            return read_artifact_text_tail(path, max_bytes=max_bytes)
        except (OSError, ValueError):
            return ""

    @staticmethod
    def _safe_int_value(value: Any) -> int:
        try:
            return int(float(str(value).replace(",", "")))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _collect_funds_missing_semantics_complete(
        cls,
        evidence: dict[str, Any],
        *,
        raw_field: str,
        expected_missing_count: int,
    ) -> bool:
        if expected_missing_count <= 0:
            return True
        field_business_map = {
            "FSRQ": "nav_date",
            "DWJZ": "nav_value",
        }
        semantics = evidence.get("snapshot_missing_semantics")
        if not isinstance(semantics, dict):
            return False
        if str(semantics.get("truth_policy") or "").strip() != "preserve_missing_semantics":
            return False
        handling = str(semantics.get("missing_value_handling") or "")
        if "does not synthesize" not in handling or "0" not in handling:
            return False
        fields = semantics.get("fields")
        if not isinstance(fields, dict):
            return False
        field_evidence = fields.get(raw_field)
        if not isinstance(field_evidence, dict):
            return False
        if str(field_evidence.get("business_field") or "").strip() != field_business_map.get(raw_field):
            return False
        if cls._safe_int_value(field_evidence.get("missing_count")) != expected_missing_count:
            return False
        reason_counts = field_evidence.get("missing_reason_counts")
        if not isinstance(reason_counts, dict) or not reason_counts:
            return False
        if sum(cls._safe_int_value(value) for value in reason_counts.values()) != expected_missing_count:
            return False
        value_type_counts = field_evidence.get("value_type_counts")
        if not isinstance(value_type_counts, dict) or not value_type_counts:
            return False
        samples = field_evidence.get("missing_samples")
        if not isinstance(samples, list) or not samples or not isinstance(samples[0], dict):
            return False
        return {"page", "fcode", "raw_field_present", "raw_value_type", "missing_reason"}.issubset(
            set(samples[0])
        )

    @classmethod
    def _collect_funds_snapshot_contract_ok(cls, evidence: dict[str, Any]) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        field_pairs = (
            ("FSRQ", "total_missing_nav_date_count", "aggregate_missing_nav_date_semantics_missing"),
            ("DWJZ", "total_missing_nav_value_count", "aggregate_missing_nav_value_semantics_missing"),
        )
        for raw_field, count_key, reason in field_pairs:
            expected = cls._safe_int_value(evidence.get(count_key))
            if expected > 0 and not cls._collect_funds_missing_semantics_complete(
                evidence,
                raw_field=raw_field,
                expected_missing_count=expected,
            ):
                reasons.append(reason)
        for type_index, type_evidence in enumerate(evidence.get("types") or [], start=1):
            if not isinstance(type_evidence, dict):
                reasons.append(f"type_payload_invalid:{type_index}")
                continue
            type_field_pairs = (
                ("FSRQ", "missing_nav_date_count", "type_missing_nav_date_semantics_missing"),
                ("DWJZ", "missing_nav_value_count", "type_missing_nav_value_semantics_missing"),
            )
            for raw_field, count_key, reason in type_field_pairs:
                expected = cls._safe_int_value(type_evidence.get(count_key))
                if expected > 0 and not cls._collect_funds_missing_semantics_complete(
                    type_evidence,
                    raw_field=raw_field,
                    expected_missing_count=expected,
                ):
                    fund_type = str(type_evidence.get("fund_type") or type_index)
                    reasons.append(f"{reason}:{fund_type}")
        return not reasons, reasons

    def _collect_funds_contract_from_stdout(self, stdout_path: Path | None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "available": False,
            "contract_ok": None,
            "stdout_path": str(stdout_path) if stdout_path else "",
            "evidence_count": 0,
            "reasons": [],
        }
        if stdout_path is None or not stdout_path.exists():
            result["reasons"] = ["stdout_missing"]
            return result
        text = self._read_scheduler_text_tail(stdout_path)
        evidence_entries: list[dict[str, Any]] = []
        parse_errors: list[str] = []
        marker = "api_source_evidence="
        for line_number, line in enumerate(text.splitlines(), start=1):
            if marker not in line:
                continue
            _, _, payload_text = line.partition(marker)
            try:
                payload = json.loads(payload_text.strip())
            except json.JSONDecodeError:
                parse_errors.append(f"line_{line_number}:json_decode_error")
                continue
            if (
                isinstance(payload, dict)
                and str(payload.get("task") or "").strip() == "collect-funds"
                and str(payload.get("source") or "").strip() == "fundMNRank"
            ):
                evidence_entries.append(payload)
        result["available"] = bool(evidence_entries)
        result["evidence_count"] = len(evidence_entries)
        if parse_errors:
            result["parse_errors"] = parse_errors[:10]
        if not evidence_entries:
            result["reasons"] = ["fundmnrank_evidence_missing"]
            return result
        reasons: list[str] = []
        evidence_summaries: list[dict[str, Any]] = []
        for index, evidence in enumerate(evidence_entries, start=1):
            ok, evidence_reasons = self._collect_funds_snapshot_contract_ok(evidence)
            evidence_summaries.append(
                {
                    "evidence_index": index,
                    "schema_version": evidence.get("schema_version"),
                    "unique_record_count": self._safe_int_value(evidence.get("unique_record_count")),
                    "total_missing_nav_date_count": self._safe_int_value(
                        evidence.get("total_missing_nav_date_count")
                    ),
                    "total_missing_nav_value_count": self._safe_int_value(
                        evidence.get("total_missing_nav_value_count")
                    ),
                    "contract_ok": ok,
                    "reasons": evidence_reasons,
                }
            )
            reasons.extend(f"evidence_{index}:{reason}" for reason in evidence_reasons)
        result["contract_ok"] = not reasons
        result["reasons"] = reasons
        result["evidence_summaries"] = evidence_summaries
        return result

    def _subprocess_index_path_from_task_log(self, task_log_path: Any) -> Path | None:
        log_path = self._resolve_scheduler_runtime_path(task_log_path)
        if log_path is None or not artifact_exists(log_path):
            return None
        index_value = ""
        marker = "subprocess_index="
        for line in self._read_scheduler_text_tail(log_path).splitlines():
            if marker not in line:
                continue
            _, _, tail = line.partition(marker)
            index_value = tail.strip().split()[0] if tail.strip() else ""
        return self._resolve_scheduler_runtime_path(index_value) if index_value else None

    def _collect_funds_contract_from_subprocess_index(self, index_path: Path | None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "available": False,
            "contract_ok": None,
            "index_path": str(index_path) if index_path else "",
            "reasons": [],
        }
        if index_path is None or not index_path.exists():
            result["reasons"] = ["subprocess_index_missing"]
            return result
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            result["reasons"] = ["subprocess_index_invalid_json"]
            return result
        items = [item for item in payload.get("items") or [] if isinstance(item, dict)]
        collect_item = next(
            (item for item in items if str(item.get("task_name") or "").strip() == "collect-funds"),
            None,
        )
        if not collect_item:
            result["reasons"] = ["collect_funds_task_missing"]
            return result
        result.update(
            {
                "available": True,
                "started_at": collect_item.get("started_at"),
                "finished_at": collect_item.get("finished_at"),
                "success": bool(collect_item.get("success")),
                "return_code": collect_item.get("return_code"),
                "timed_out": bool(collect_item.get("timed_out")),
            }
        )
        if (
            not bool(collect_item.get("success"))
            or collect_item.get("return_code") not in (0, "0")
            or bool(collect_item.get("timed_out"))
        ):
            result["contract_ok"] = None
            result["reasons"] = ["collect_funds_task_not_success"]
            return result
        stdout_path = self._resolve_scheduler_runtime_path(collect_item.get("stdout_path"))
        stdout_contract = self._collect_funds_contract_from_stdout(stdout_path)
        result["stdout_contract"] = stdout_contract
        result["contract_ok"] = stdout_contract.get("contract_ok")
        result["reasons"] = list(stdout_contract.get("reasons") or [])
        return result

    def _latest_successful_collect_funds_contract_after(
        self,
        after_dt: datetime | None,
    ) -> dict[str, Any]:
        project_root = Path(getattr(self.settings, "project_root", Path.cwd()))
        reports_dir = Path(getattr(self.settings, "reports_dir", project_root / "reports"))
        if not reports_dir.is_absolute():
            reports_dir = project_root / reports_dir
        log_root = reports_dir / ".audit" / "subprocess_logs"
        result: dict[str, Any] = {
            "available": False,
            "contract_ok": None,
            "log_root": str(log_root),
            "checked_index_count": 0,
            "reason": "subprocess_log_root_missing",
        }
        if not log_root.exists():
            return result
        index_paths = sorted(
            log_root.glob("*/subprocess_index.json"),
            key=lambda path: path.stat().st_mtime if path.exists() else 0,
            reverse=True,
        )
        result["checked_index_count"] = len(index_paths)
        result["reason"] = "matching_collect_funds_contract_missing"
        for index_path in index_paths:
            contract = self._collect_funds_contract_from_subprocess_index(index_path)
            if contract.get("contract_ok") is not True:
                continue
            started_dt = self._parse_datetime(contract.get("started_at"))
            finished_dt = self._parse_datetime(contract.get("finished_at"))
            compare_dt = finished_dt or started_dt
            if after_dt is not None and compare_dt is not None and compare_dt <= after_dt:
                continue
            contract["available"] = True
            contract["index_path"] = str(index_path)
            return contract
        return result

    def _latest_report_scheduled_daily_lineage_evidence(self, run_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "lineage_ok": False,
            "reason": "latest_report_run_id_missing",
            "latest_report_run_id": str(run_id or "").strip() or None,
        }
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return result
        database_path = getattr(self.settings, "database_path", None)
        if not database_path:
            result["reason"] = "database_path_missing"
            return result
        db_path = Path(database_path)
        if not db_path.exists():
            result["reason"] = "database_missing"
            return result

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                f"""
                SELECT id, status, records_failed, error_message, started_at, completed_at, extra_data
                FROM task_history
                WHERE task_name = ? AND {SCHEDULER_EXECUTION_ROW_SQL}
                ORDER BY id DESC
                LIMIT 200
                """,
                ("scheduled_daily_bond_refresh",),
            ).fetchall()
        except sqlite3.Error:
            result["reason"] = "task_history_read_failed"
            return result
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass

        result["recent_task_rows_checked"] = len(rows)
        for row in rows:
            try:
                extra = json.loads(row["extra_data"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                extra = {}
            if not isinstance(extra, dict):
                continue
            workflow_summary = extra.get("workflow_summary")
            if not isinstance(workflow_summary, dict):
                workflow_summary = {}
            candidate_run_id = _task_bound_workflow_run_id(extra)
            if not candidate_run_id:
                continue
            if candidate_run_id != normalized_run_id:
                continue
            status = str(row["status"] or extra.get("status") or "").strip().lower()
            if status not in {"success", "succeeded", "ok"}:
                continue
            if int(row["records_failed"] or extra.get("records_failed") or 0) != 0:
                continue
            if str(row["error_message"] or "").strip():
                continue
            if not bool(extra.get("screening_triggered")):
                continue
            task_log_path = (
                extra.get("task_log_path")
                or extra.get("task_log_file_project_path")
                or extra.get("task_log_file")
                or (extra.get("task_run_audit") or {}).get("task_log_project_path")
                or (extra.get("task_run_audit") or {}).get("task_log_path_relative")
            )
            index_path = self._subprocess_index_path_from_task_log(task_log_path)
            collect_contract = self._collect_funds_contract_from_subprocess_index(index_path)
            result.update(
                {
                    "lineage_ok": True,
                    "reason": "lineage_ok",
                    "task_history_id": row["id"],
                    "status": status,
                    "started_at": row["started_at"] or extra.get("started_at"),
                    "completed_at": row["completed_at"] or extra.get("finished_at"),
                    "screening_reason": extra.get("screening_reason"),
                    "task_log_path": str(task_log_path or ""),
                    "collection_subprocess_index": str(index_path) if index_path else "",
                    "collect_funds_contract": collect_contract,
                }
            )
            return result
        result["reason"] = "latest_report_missing_scheduled_daily_lineage"
        return result

    def _latest_report_has_scheduled_daily_lineage(self, run_id: str) -> bool:
        return bool(
            self._latest_report_scheduled_daily_lineage_evidence(run_id).get("lineage_ok")
        )

    def _daily_refresh_lineage_rescreen_required(self, *, refresh_window: str) -> dict[str, Any]:
        if str(refresh_window or "").strip() != "daily":
            return {
                "required": False,
                "reason": "not_daily_refresh",
            }
        run_id = self._current_latest_report_run_id()
        if not run_id:
            return {
                "required": False,
                "reason": "latest_report_run_id_missing",
            }
        lineage_evidence = self._latest_report_scheduled_daily_lineage_evidence(run_id)
        lineage_ok = bool(lineage_evidence.get("lineage_ok"))
        if lineage_ok:
            collect_contract = (
                lineage_evidence.get("collect_funds_contract")
                if isinstance(lineage_evidence.get("collect_funds_contract"), dict)
                else {}
            )
            if collect_contract.get("contract_ok") is False:
                completed_dt = self._parse_datetime(
                    collect_contract.get("finished_at")
                    or lineage_evidence.get("completed_at")
                    or lineage_evidence.get("started_at")
                )
                newer_contract = self._latest_successful_collect_funds_contract_after(completed_dt)
                if newer_contract.get("contract_ok") is True:
                    return {
                        "required": True,
                        "reason": "latest_report_collect_funds_evidence_contract_stale",
                        "latest_report_run_id": run_id,
                        "scheduled_daily_lineage": lineage_evidence,
                        "newer_collect_funds_contract": newer_contract,
                    }
        return {
            "required": not lineage_ok,
            "reason": "latest_report_missing_scheduled_daily_lineage" if not lineage_ok else "lineage_ok",
            "latest_report_run_id": run_id,
            "scheduled_daily_lineage": lineage_evidence,
        }

    def _daily_scheduled_at(self, now: datetime) -> datetime:
        return now.replace(
            hour=int(getattr(self.settings, "scheduler_daily_hour", 6) or 6),
            minute=int(getattr(self.settings, "scheduler_daily_minute", 10) or 10),
            second=0,
            microsecond=0,
        )

    def _intraday_scheduled_at(self, now: datetime) -> datetime:
        return now.replace(
            hour=int(getattr(self.settings, "scheduler_intraday_hour", 13) or 13),
            minute=int(getattr(self.settings, "scheduler_intraday_minute", 45) or 45),
            second=0,
            microsecond=0,
        )

    def _weekly_scheduled_at(self, now: datetime) -> datetime:
        target_weekday = self._weekday_to_index(
            getattr(self.settings, "scheduler_weekly_day_of_week", "sun")
        )
        days_since = (now.weekday() - target_weekday) % 7
        scheduled = now - timedelta(days=days_since)
        return scheduled.replace(
            hour=int(getattr(self.settings, "scheduler_weekly_hour", 3) or 3),
            minute=int(getattr(self.settings, "scheduler_weekly_minute", 0) or 0),
            second=0,
            microsecond=0,
        )

    def _last_refresh_run_at(self, *, require_satisfied: bool = False) -> datetime | None:
        candidates = [
            self._last_run_at("scheduled_daily_bond_refresh", require_satisfied=require_satisfied),
            self._last_run_at("scheduled_intraday_bond_refresh", require_satisfied=require_satisfied),
        ]
        available = [value for value in candidates if value is not None]
        return max(available) if available else None

    def _blocked_snapshot_retry_cooldown(
        self,
        current_snapshot: Any,
        *,
        state_payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Return retry-cooldown state for a blocked publication of this exact snapshot.

        A blocked publication deliberately remains unprocessed.  This guard only spaces
        automatic retries so startup recovery, the snapshot watchdog, and post-task
        catch-up cannot immediately enqueue the same ``candidate_date + signature``.
        """
        try:
            cooldown_minutes = max(
                0,
                int(
                    getattr(
                        self.settings,
                        "scheduler_blocked_snapshot_retry_cooldown_minutes",
                        120,
                    )
                    or 0
                ),
            )
        except (TypeError, ValueError):
            cooldown_minutes = 120

        if isinstance(current_snapshot, dict):
            candidate_date = str(current_snapshot.get("candidate_date") or "").strip()
            signature = str(current_snapshot.get("signature") or "").strip()
        else:
            candidate_date = str(getattr(current_snapshot, "candidate_date", "") or "").strip()
            signature = str(getattr(current_snapshot, "signature", "") or "").strip()

        result: dict[str, Any] = {
            "active": False,
            "configured_minutes": cooldown_minutes,
            "skip_reason": None,
            "candidate_date": candidate_date or None,
            "signature": signature or None,
            "last_attempted_at": None,
            "next_retry_at": None,
            "remaining_seconds": 0,
            "task_name": None,
            "screening_run_id": None,
        }
        if cooldown_minutes <= 0 or not candidate_date or not signature:
            return result

        payload = state_payload or {}
        last_runs = payload.get("last_runs") or {}
        latest_attempt: tuple[datetime, str, dict[str, Any]] | None = None
        for task_name in (
            "scheduled_intraday_bond_refresh",
            "scheduled_daily_bond_refresh",
            "scheduled_snapshot_watchdog_refresh",
        ):
            refresh_result = last_runs.get(task_name) or {}
            if not isinstance(refresh_result, dict):
                continue
            if (
                str(refresh_result.get("status") or "").strip().lower() == "skipped"
                and str(refresh_result.get("skip_reason") or "").strip().lower()
                == "blocked_snapshot_retry_cooldown"
            ):
                skipped_date = str(refresh_result.get("candidate_date") or "").strip()
                skipped_signature = str(refresh_result.get("signature") or "").strip()
                if skipped_date != candidate_date or skipped_signature != signature:
                    continue
                attempted_at = self._parse_datetime(refresh_result.get("last_attempted_at"))
                if attempted_at is None:
                    continue
                inherited_task_name = (
                    str(refresh_result.get("blocked_task_name") or "").strip()
                    or task_name
                )
                inherited_result = {
                    "screening_run_id": refresh_result.get("blocked_screening_run_id"),
                }
                if latest_attempt is None or attempted_at > latest_attempt[0]:
                    latest_attempt = (
                        attempted_at,
                        inherited_task_name,
                        inherited_result,
                    )
                continue
            if not bool(refresh_result.get("screening_triggered")):
                continue
            publication_result = refresh_result.get("publication_result")
            if not isinstance(publication_result, dict):
                continue
            if str(publication_result.get("status") or "").strip().lower() != "blocked":
                continue
            processed_snapshot = refresh_result.get("after_effective_nav_snapshot") or {}
            if not isinstance(processed_snapshot, dict):
                continue
            processed_date = str(processed_snapshot.get("candidate_date") or "").strip()
            processed_signature = str(processed_snapshot.get("signature") or "").strip()
            if (
                not processed_date
                or not processed_signature
                or processed_date != candidate_date
                or processed_signature != signature
            ):
                continue
            attempted_at = None
            for key in ("finished_at", "completed_at", "started_at"):
                attempted_at = self._parse_datetime(refresh_result.get(key))
                if attempted_at is not None:
                    break
            if attempted_at is None:
                continue
            if latest_attempt is None or attempted_at > latest_attempt[0]:
                latest_attempt = (attempted_at, task_name, refresh_result)

        if latest_attempt is None:
            return result

        attempted_at, task_name, refresh_result = latest_attempt
        current_time = now or datetime.now(tz=attempted_at.tzinfo)
        if attempted_at.tzinfo is None and current_time.tzinfo is not None:
            attempted_at = attempted_at.replace(tzinfo=current_time.tzinfo)
        elif attempted_at.tzinfo is not None and current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=attempted_at.tzinfo)
        next_retry_at = attempted_at + timedelta(minutes=cooldown_minutes)
        remaining_seconds = max(0, int((next_retry_at - current_time).total_seconds()))
        active = current_time < next_retry_at
        result.update(
            {
                "active": active,
                "skip_reason": "blocked_snapshot_retry_cooldown" if active else None,
                "last_attempted_at": attempted_at.isoformat(timespec="seconds"),
                "next_retry_at": next_retry_at.isoformat(timespec="seconds"),
                "remaining_seconds": remaining_seconds,
                "task_name": task_name,
                "screening_run_id": (
                    str(refresh_result.get("screening_run_id") or "").strip() or None
                ),
            }
        )
        return result

    def _failed_screening_retry_cooldown(
        self,
        current_snapshot: Any,
        *,
        state_payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Space retries after a screening subprocess failure for the same snapshot."""
        try:
            cooldown_minutes = max(
                0,
                int(
                    getattr(
                        self.settings,
                        "scheduler_failed_screening_retry_cooldown_minutes",
                        120,
                    )
                    or 0
                ),
            )
        except (TypeError, ValueError):
            cooldown_minutes = 120

        if isinstance(current_snapshot, dict):
            candidate_date = str(current_snapshot.get("candidate_date") or "").strip()
            signature = str(current_snapshot.get("signature") or "").strip()
        else:
            candidate_date = str(getattr(current_snapshot, "candidate_date", "") or "").strip()
            signature = str(getattr(current_snapshot, "signature", "") or "").strip()

        result: dict[str, Any] = {
            "active": False,
            "configured_minutes": cooldown_minutes,
            "skip_reason": None,
            "candidate_date": candidate_date or None,
            "signature": signature or None,
            "last_attempted_at": None,
            "next_retry_at": None,
            "remaining_seconds": 0,
            "task_name": None,
            "failure_signature": None,
            "error": None,
        }
        if cooldown_minutes <= 0 or not candidate_date or not signature:
            return result

        latest_attempt: tuple[datetime, str, dict[str, Any]] | None = None
        last_runs = (state_payload or {}).get("last_runs") or {}
        for task_name in (
            "scheduled_intraday_bond_refresh",
            "scheduled_daily_bond_refresh",
            "scheduled_snapshot_watchdog_refresh",
        ):
            refresh_result = last_runs.get(task_name) or {}
            if not isinstance(refresh_result, dict):
                continue
            if (
                str(refresh_result.get("status") or "").strip().lower() == "skipped"
                and str(refresh_result.get("skip_reason") or "").strip().lower()
                == "failed_screening_retry_cooldown"
            ):
                skipped_date = str(refresh_result.get("candidate_date") or "").strip()
                skipped_signature = str(refresh_result.get("signature") or "").strip()
                if skipped_date != candidate_date or skipped_signature != signature:
                    continue
                attempted_at = self._parse_datetime(refresh_result.get("last_attempted_at"))
                if attempted_at is None:
                    continue
                inherited = {
                    "failure_signature": refresh_result.get("failure_signature"),
                    "error": refresh_result.get("blocked_error"),
                }
                inherited_task_name = (
                    str(refresh_result.get("blocked_task_name") or "").strip()
                    or task_name
                )
                if latest_attempt is None or attempted_at > latest_attempt[0]:
                    latest_attempt = (attempted_at, inherited_task_name, inherited)
                continue

            if str(refresh_result.get("status") or "").strip().lower() != "failed":
                continue
            error_text = str(refresh_result.get("error") or "").strip()
            failure_signature = str(
                refresh_result.get("failure_signature")
                or ("workflow_subprocess_failed" if "workflow_subprocess_failed" in error_text else "")
            ).strip()
            if not failure_signature:
                continue
            failed_snapshot = refresh_result.get("failed_effective_nav_snapshot") or {}
            if not isinstance(failed_snapshot, dict):
                continue
            failed_date = str(failed_snapshot.get("candidate_date") or "").strip()
            failed_signature = str(failed_snapshot.get("signature") or "").strip()
            if failed_date != candidate_date or failed_signature != signature:
                continue
            attempted_at = None
            for key in ("finished_at", "completed_at", "started_at"):
                attempted_at = self._parse_datetime(refresh_result.get(key))
                if attempted_at is not None:
                    break
            if attempted_at is None:
                continue
            if latest_attempt is None or attempted_at > latest_attempt[0]:
                latest_attempt = (attempted_at, task_name, refresh_result)

        if latest_attempt is None:
            return result

        attempted_at, task_name, refresh_result = latest_attempt
        current_time = now or datetime.now(tz=attempted_at.tzinfo)
        if attempted_at.tzinfo is None and current_time.tzinfo is not None:
            attempted_at = attempted_at.replace(tzinfo=current_time.tzinfo)
        elif attempted_at.tzinfo is not None and current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=attempted_at.tzinfo)
        next_retry_at = attempted_at + timedelta(minutes=cooldown_minutes)
        active = current_time < next_retry_at
        result.update(
            {
                "active": active,
                "skip_reason": "failed_screening_retry_cooldown" if active else None,
                "last_attempted_at": attempted_at.isoformat(timespec="seconds"),
                "next_retry_at": next_retry_at.isoformat(timespec="seconds"),
                "remaining_seconds": max(0, int((next_retry_at - current_time).total_seconds())),
                "task_name": task_name,
                "failure_signature": str(
                    refresh_result.get("failure_signature") or "workflow_subprocess_failed"
                ).strip(),
                "error": str(refresh_result.get("error") or "").strip() or None,
            }
        )
        return result

    def _workflow_busy_retry_cooldown(
        self,
        *,
        state_payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        try:
            cooldown_seconds = max(
                0,
                int(
                    getattr(
                        self.settings,
                        "scheduler_workflow_busy_retry_cooldown_seconds",
                        300,
                    )
                    or 0
                ),
            )
        except (TypeError, ValueError):
            cooldown_seconds = 300
        result: dict[str, Any] = {
            "active": False,
            "configured_seconds": cooldown_seconds,
            "skip_reason": None,
            "last_busy_at": None,
            "next_retry_at": None,
            "remaining_seconds": 0,
            "task_name": None,
            "workflow_busy_skip_reason": None,
        }
        if cooldown_seconds <= 0:
            return result
        busy_skip_reasons = {
            "workflow_busy",
            "workflow_busy_locked",
            "workflow_busy_state_only",
            "workflow_busy_after_collection",
            "workflow_busy_after_collection_locked",
            "workflow_busy_after_collection_state_only",
        }
        last_runs = (state_payload or {}).get("last_runs") or {}
        latest_busy: tuple[datetime, str, str] | None = None
        for task_name in (
            "scheduled_intraday_bond_refresh",
            "scheduled_daily_bond_refresh",
            "scheduled_snapshot_watchdog_refresh",
        ):
            refresh_result = last_runs.get(task_name) or {}
            if not isinstance(refresh_result, dict):
                continue
            if str(refresh_result.get("status") or "").strip().lower() != "skipped":
                continue
            skip_reason = str(refresh_result.get("skip_reason") or "").strip().lower()
            if skip_reason not in busy_skip_reasons:
                continue
            busy_at = None
            for key in ("finished_at", "completed_at", "started_at"):
                busy_at = self._parse_datetime(refresh_result.get(key))
                if busy_at is not None:
                    break
            if busy_at is None:
                continue
            if latest_busy is None or busy_at > latest_busy[0]:
                latest_busy = (busy_at, task_name, skip_reason)
        if latest_busy is None:
            return result
        busy_at, task_name, skip_reason = latest_busy
        current_time = now or datetime.now(tz=busy_at.tzinfo)
        if busy_at.tzinfo is None and current_time.tzinfo is not None:
            busy_at = busy_at.replace(tzinfo=current_time.tzinfo)
        elif busy_at.tzinfo is not None and current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=busy_at.tzinfo)
        next_retry_at = busy_at + timedelta(seconds=cooldown_seconds)
        remaining_seconds = max(0, int((next_retry_at - current_time).total_seconds()))
        active = current_time < next_retry_at
        result.update(
            {
                "active": active,
                "skip_reason": "workflow_busy_retry_cooldown" if active else None,
                "last_busy_at": busy_at.isoformat(timespec="seconds"),
                "next_retry_at": next_retry_at.isoformat(timespec="seconds"),
                "remaining_seconds": remaining_seconds,
                "task_name": task_name,
                "workflow_busy_skip_reason": skip_reason,
            }
        )
        return result

    def _resolve_startup_catch_up_jobs(self, now: datetime | None = None) -> list[str]:
        current_time = now or datetime.now()
        due_jobs: list[str] = []
        state_payload: dict[str, Any] = {}
        state_store = getattr(self, "state_store", None)
        if state_store is not None and hasattr(state_store, "snapshot"):
            try:
                state_payload = state_store.snapshot() or {}
            except Exception:
                state_payload = {}

        weekly_scheduled_at = self._weekly_scheduled_at(current_time)
        last_weekly_run = self._last_run_at("scheduled_weekly_bond_maintenance", require_satisfied=True)
        if current_time >= weekly_scheduled_at and (
            last_weekly_run is None or last_weekly_run < weekly_scheduled_at
        ):
            due_jobs.append("weekly_maintenance")

        daily_scheduled_at = self._daily_scheduled_at(current_time)
        last_refresh_run = self._last_refresh_run_at(require_satisfied=True)
        # This timestamp satisfies collection scheduling only. The blocked
        # snapshot remains unprocessed and will retry when the dependency is
        # reenabled. A later cron slot still needs a new collection.
        if configured_publication_blocker(self.settings):
            for task_name in ("scheduled_daily_bond_refresh", "scheduled_intraday_bond_refresh"):
                entry = ((state_payload.get("last_runs") or {}).get(task_name) or {})
                completed = self._parse_datetime(entry.get("finished_at"))
                if (entry.get("collection_completed") is True
                        and entry.get("warning_reason") == "independent_source_unavailable"
                        and completed is not None):
                    last_refresh_run = max(last_refresh_run, completed) if last_refresh_run else completed
        if current_time >= daily_scheduled_at and (
            last_refresh_run is None or last_refresh_run < daily_scheduled_at
        ):
            due_jobs.append("daily_refresh")

        intraday_enabled = bool(getattr(self.settings, "scheduler_intraday_refresh_enabled", False))
        if intraday_enabled and "daily_refresh" not in due_jobs:
            intraday_scheduled_at = self._intraday_scheduled_at(current_time)
            if current_time >= intraday_scheduled_at and (
                last_refresh_run is None or last_refresh_run < intraday_scheduled_at
            ):
                due_jobs.append("intraday_refresh")

        database_path = getattr(self.settings, "database_path", None)
        latest_report_file = getattr(self.settings, "latest_report_file", None)
        if database_path and latest_report_file:
            current_snapshot = load_effective_nav_snapshot(
                database_path,
                coverage_ratio=float(getattr(self.settings, "effective_nav_coverage_ratio", 0.9) or 0.9),
                weekday_only=bool(getattr(self.settings, "effective_nav_weekday_only", True)),
            )
            latest_screened_snapshot = load_latest_screened_db_snapshot(latest_report_file)
            if hasattr(current_snapshot, "ready") and hasattr(current_snapshot, "candidate_date"):
                should_screen_now, _ = screening_snapshot_changed(
                    current_snapshot,
                    latest_screened_snapshot or {},
                )
                if (should_screen_now and configured_publication_blocker(self.settings) is None
                        and "daily_refresh" not in due_jobs and "intraday_refresh" not in due_jobs):
                    if not self._last_refresh_processed_current_snapshot(
                        current_snapshot,
                        state_payload=state_payload,
                    ):
                        if intraday_enabled and current_time >= self._intraday_scheduled_at(current_time):
                            due_jobs.append("intraday_refresh")
                        else:
                            due_jobs.append("daily_refresh")

                blocked_retry_cooldown = self._blocked_snapshot_retry_cooldown(
                    current_snapshot,
                    state_payload=state_payload,
                    now=current_time,
                )
                if blocked_retry_cooldown.get("active"):
                    due_jobs = [
                        job_id
                        for job_id in due_jobs
                        if job_id not in {"daily_refresh", "intraday_refresh"}
                    ]

                failed_screening_retry_cooldown = self._failed_screening_retry_cooldown(
                    current_snapshot,
                    state_payload=state_payload,
                    now=current_time,
                )
                if failed_screening_retry_cooldown.get("active"):
                    due_jobs = [
                        job_id
                        for job_id in due_jobs
                        if job_id not in {"daily_refresh", "intraday_refresh"}
                    ]

                from backend.scheduler_collection_retry import period_collection_retry_cooldown

                collection_retry = period_collection_retry_cooldown(
                    self.settings, current_snapshot, state_payload=state_payload, now=current_time,
                )
                if collection_retry.get("active"):
                    due_jobs = [job_id for job_id in due_jobs if job_id not in {"daily_refresh", "intraday_refresh"}]

        workflow_busy_retry_cooldown = self._workflow_busy_retry_cooldown(
            state_payload=state_payload,
            now=current_time,
        )
        if workflow_busy_retry_cooldown.get("active"):
            due_jobs = [
                job_id
                for job_id in due_jobs
                if job_id not in {"daily_refresh", "intraday_refresh"}
            ]

        return due_jobs

    def _resolve_run_report_file(self, run_id: str) -> Path | None:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return None

        runs_dir = getattr(self.settings, "runs_dir", None)
        if runs_dir:
            return Path(runs_dir) / normalized_run_id / f"bond_analysis_report_{normalized_run_id}.json"

        reports_dir = getattr(self.settings, "reports_dir", None)
        if reports_dir:
            return Path(reports_dir) / "runs" / normalized_run_id / f"bond_analysis_report_{normalized_run_id}.json"

        latest_report_file = getattr(self.settings, "latest_report_file", None)
        if latest_report_file:
            return (
                Path(latest_report_file).parent
                / "runs"
                / normalized_run_id
                / f"bond_analysis_report_{normalized_run_id}.json"
            )
        return None

    @staticmethod
    def _snapshot_matches_current_snapshot(
        current_snapshot: Any,
        processed_snapshot: dict[str, Any] | None,
    ) -> bool:
        snapshot = processed_snapshot or {}
        processed_date = str(snapshot.get("candidate_date") or "").strip()
        processed_signature = str(snapshot.get("signature") or "").strip()
        current_date = str(getattr(current_snapshot, "candidate_date", "") or "").strip()
        current_signature = str(getattr(current_snapshot, "signature", "") or "").strip()
        return bool(
            processed_date
            and processed_date == current_date
            and (
                not processed_signature
                or not current_signature
                or processed_signature == current_signature
            )
        )

    def _load_last_processed_screening_snapshot(self, run_id: str) -> dict[str, Any]:
        report_file = self._resolve_run_report_file(run_id)
        if report_file is None:
            return {}
        if not report_file.exists() or not report_file.is_file():
            return {}
        try:
            payload = json.loads(report_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return {}
        if not isinstance(payload, dict):
            return {}
        freshness = (payload.get("database_freshness") or {}).get("effective_nav_snapshot")
        return freshness if isinstance(freshness, dict) else {}

    def _last_refresh_processed_current_snapshot(
        self,
        current_snapshot: Any,
        *,
        state_payload: dict[str, Any] | None = None,
    ) -> bool:
        payload = state_payload or {}
        last_runs = payload.get("last_runs") or {}
        # P0-03.2 / V2: 把 watchdog 自身的 task_name 也纳入判重，避免 watchdog 跑完后
        # latest.json 因发布门禁失败未更新 signature 时下一轮 watchdog 重复触发同快照。
        for task_name in (
            "scheduled_intraday_bond_refresh",
            "scheduled_daily_bond_refresh",
            "scheduled_snapshot_watchdog_refresh",
        ):
            refresh_result = (last_runs.get(task_name) or {})
            last_status = str(refresh_result.get("status") or "").strip().lower()
            if last_status not in {"success", "degraded"}:
                continue

            screening_run_id = str(refresh_result.get("screening_run_id") or "").strip()
            if screening_run_id:
                processed_snapshot = self._load_last_processed_screening_snapshot(screening_run_id)
                publication_result = _build_publication_result_for_workflow_run(
                    load_publication_registry(self.settings),
                    screening_run_id,
                )
                if (
                    self._snapshot_matches_current_snapshot(current_snapshot, processed_snapshot)
                    and _publication_result_counts_as_processed(publication_result)
                ):
                    return True

            if not bool(refresh_result.get("screening_triggered")):
                continue

            processed_snapshot = (refresh_result.get("after_effective_nav_snapshot") or {})
            publication_result = (
                refresh_result.get("publication_result")
                if isinstance(refresh_result.get("publication_result"), dict)
                else None
            )
            if (
                self._snapshot_matches_current_snapshot(current_snapshot, processed_snapshot)
                and (
                    not screening_run_id
                    or _publication_result_counts_as_processed(publication_result)
                )
            ):
                return True
        return False

    def _last_daily_refresh_processed_current_snapshot(
        self,
        current_snapshot: Any,
        *,
        state_payload: dict[str, Any] | None = None,
    ) -> bool:
        return self._last_refresh_processed_current_snapshot(
            current_snapshot,
            state_payload=state_payload,
        )

    def _run_startup_catch_up_if_needed(self) -> None:
        self._ensure_runtime_host_allowed()
        due_jobs = self._resolve_startup_catch_up_jobs()
        pending_job_ids = {
            str((item or {}).get("job_id") or "").strip()
            for item in ((self.state_store.snapshot() or {}).get("pending_manual_run_requests") or [])
        }
        if pending_job_ids:
            due_jobs = [job_id for job_id in due_jobs if job_id not in pending_job_ids]
        if not due_jobs:
            self._log("启动补跑检查完成：当前没有遗漏的日常调度任务")
            return

        self._log(f"检测到遗漏调度任务，启动补跑: {', '.join(due_jobs)}")
        for job_id in due_jobs:
            if self.stop_event.is_set():
                return
            self._startup_recovery_context.job_id = str(job_id)
            self._startup_recovery_context.scheduler_started_at = self.state_store.snapshot().get("started_at")
            try:
                if job_id == "weekly_maintenance":
                    result = self._run_task("scheduled_weekly_bond_maintenance", self._execute_weekly_maintenance)
                elif job_id == "daily_refresh":
                    result = self._run_task("scheduled_daily_bond_refresh", self._execute_daily_refresh)
                elif job_id == "intraday_refresh":
                    result = self._run_task("scheduled_intraday_bond_refresh", self._execute_intraday_refresh)
                else:  # pragma: no cover - defensive branch
                    continue
            finally:
                self._startup_recovery_context.job_id = None
                self._startup_recovery_context.scheduler_started_at = None
            self._log(
                "启动补跑完成: "
                f"{job_id} -> {str(result.get('status') or '').strip() or 'unknown'}"
            )

    def _enqueue_post_task_catch_up_if_needed(self, *, completed_task_name: str) -> None:
        stop_event = getattr(self, "stop_event", None)
        if stop_event is None or stop_event.is_set():
            return
        catch_up_lock = getattr(self, "_post_task_catch_up_lock", None)
        if catch_up_lock is None or not catch_up_lock.acquire(blocking=False):
            return
        try:
            if self._manual_run_lock.locked():
                return
            current_task = (self.state_store.snapshot() or {}).get("current_task") or {}
            if current_task:
                return
            pending_job_ids = {
                str((item or {}).get("job_id") or "").strip()
                for item in ((self.state_store.snapshot() or {}).get("pending_manual_run_requests") or [])
            }
            due_jobs = [
                job_id
                for job_id in self._resolve_startup_catch_up_jobs()
                if job_id in {"daily_refresh", "intraday_refresh"} and job_id not in pending_job_ids
            ]
            if not due_jobs:
                return
            self._log(
                "任务结束后检测到待自动补跑任务: "
                f"{', '.join(due_jobs)}；completed_task={completed_task_name}"
            )
            for job_id in due_jobs:
                try:
                    task_name, _ = self._resolve_job_runner(job_id)
                except ValueError:
                    continue
                request_id = uuid.uuid4().hex[:12]
                requested_at = datetime.now().isoformat(timespec="seconds")
                entry = {
                    "request_id": request_id,
                    "job_id": job_id,
                    "task_name": task_name,
                    "requested_by": "automation",
                    "requested_at": requested_at,
                    "status": "accepted",
                    "execution_mode": "automatic_post_task_catch_up",
                    "trigger_source": "post_task_catch_up",
                    "trigger_completed_task_name": str(completed_task_name or "").strip() or None,
                }
                result = self.state_store.enqueue_unique_manual_run_request(entry)
                if bool(result.get("accepted")):
                    self._log(f"已入队任务结束后自动补跑: {job_id} request_id={request_id}")
                else:
                    self._log(
                        "任务结束后自动补跑未入队: "
                        f"{job_id} reason={result.get('reason') or 'unknown'}"
                    )
        finally:
            catch_up_lock.release()

    def _start_startup_recovery(self) -> None:
        with self._startup_recovery_lock:
            if self._startup_recovery_started:
                return
            self._startup_recovery_started = True

        def runner() -> None:
            try:
                self._run_startup_catch_up_if_needed()
            except Exception as exc:  # pragma: no cover - runtime defensive path
                self._log(f"启动补跑失败: {exc}")

        self.startup_recovery_thread = threading.Thread(target=runner, daemon=True)
        self.startup_recovery_thread.start()

    def _finalize_task(
        self,
        *,
        task_info: dict[str, Any],
        task_name: str,
        result: dict[str, Any],
        timing: TaskTiming | None = None,
    ) -> dict[str, Any]:
        timing = timing or TaskTiming(measure_total=False)
        timing.prepare_result(result, task_info)
        pre_audit_persisted_result = compact_task_result_for_history_if_needed(
            result,
            task_name=task_name,
            task_info=task_info,
            reports_dir=getattr(self.settings, "reports_dir", None),
            write_sidecar=False,
        )
        self.state_store.stage_task_result(task_name, pre_audit_persisted_result)
        self.state_store.set_jobs(self._serialize_jobs())
        try:
            task_run_audit = self._write_task_run_audit(
                task_info=task_info,
                task_name=task_name,
                result=result,
            )
            if not task_run_audit or not task_run_audit.get("artifact_path"):
                raise RuntimeError("task_run_audit_artifact_not_produced")
            result["task_run_audit_status"] = "written"
            result["task_run_audit_error"] = None
            if task_run_audit:
                result["task_run_audit"] = task_run_audit
                result["task_run_audit_path"] = task_run_audit.get("artifact_path")
                workflow_run_id = _task_bound_workflow_run_id(result)
                if workflow_run_id:
                    result["workflow_run_id"] = workflow_run_id
                publication_result = _build_publication_result_for_workflow_run(
                    load_publication_registry(self.settings),
                    workflow_run_id,
                )
                if publication_result:
                    _apply_publication_result_to_task_status(result, publication_result)
                    result["publication_result"] = publication_result
                    derived_status_detail = _derive_status_detail_for_task_result(
                        str(result.get("status") or "").strip(),
                        publication_result,
                        task_result=result,
                    )
                    if derived_status_detail:
                        result["status_detail"] = derived_status_detail
        except Exception as exc:  # pragma: no cover - defensive runtime path
            result["task_run_audit_status"] = "failed"
            result["task_run_audit_error"] = str(exc)
            if callable(getattr(self, "logger", None)):
                self._log(f"[task-audit-failed] task={task_name} error={exc}")
        # Publication review can change the business outcome. Keep ownership
        # through expensive audit/status work; no terminal time exists yet.
        status = str(result.get("status") or "failed").strip() or "failed"
        staged_result = compact_task_result_for_history_if_needed(
            result,
            task_name=task_name,
            task_info=task_info,
            reports_dir=getattr(self.settings, "reports_dir", None),
            write_sidecar=False,
        )
        self.state_store.stage_task_result(task_name, staged_result)
        try:
            self._refresh_runtime_status_summary()
        except Exception as exc:  # pragma: no cover - defensive runtime path
            result["runtime_status_summary_refresh_error"] = str(exc)

        def final_payload(completed_at: datetime) -> dict[str, Any]:
            timing.complete_result(result, at=completed_at)
            if result.get("task_run_audit"):
                result["task_run_audit"]["status"] = status
            return compact_task_result_for_history_if_needed(
                result, task_name=task_name, task_info=task_info,
                reports_dir=getattr(self.settings, "reports_dir", None),
                write_sidecar=not bool(result.get("task_run_audit_path")),
            )

        recording_started = time.monotonic()
        persisted_result = self.task_recorder.finish_task(
            task_info,
            status=status,
            error_message=str(result.get("error") or "") or None,
            error_traceback=str(result.get("error_traceback") or "") or None,
            completion_result_factory=final_payload,
        )
        if result.get("task_run_audit_status") == "written":
            try:
                seal_task_run_audit(result["task_run_audit_path"], result)
            except Exception as exc:
                result["task_run_audit_status"] = "failed"
                result["task_run_audit_error"] = f"completion_seal_failed: {exc}"
                if callable(getattr(self, "logger", None)):
                    self._log(f"[task-audit-failed] task={task_name} error={result['task_run_audit_error']}")
                persisted_result = compact_task_result_for_history_if_needed(
                    result, task_name=task_name, task_info=task_info,
                    reports_dir=getattr(self.settings, "reports_dir", None), write_sidecar=True,
                )
                self.task_recorder.update_task_extra_data(task_info, persisted_result)
        self.state_store.finish_task(task_name, persisted_result)
        if callable(getattr(self, "logger", None)):
            self._log(
                f"[task-recorded] task={task_name} finished_at={result.get('finished_at')} "
                f"recorded_at={datetime.now().isoformat(timespec='microseconds')} "
                f"recording_elapsed_seconds={time.monotonic() - recording_started:.6f}"
            )
        return result

    @staticmethod
    def _manual_operations_enabled_for_audit() -> bool:
        return os.environ.get("FUND_ANA_ENABLE_MANUAL_OPERATIONS", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _workflow_summary_for_task_audit(self) -> dict[str, Any]:
        status_summary = getattr(self.workflow_manager, "status_summary", None)
        if callable(status_summary):
            try:
                payload = status_summary() or {}
            except Exception as exc:  # pragma: no cover - defensive runtime path
                return {"audit_error": str(exc)}
            return dict(payload) if isinstance(payload, dict) else {}

        status = getattr(self.workflow_manager, "status", None)
        if callable(status):
            try:
                payload = status() or {}
            except Exception as exc:  # pragma: no cover - defensive runtime path
                return {"audit_error": str(exc)}
            if not isinstance(payload, dict):
                return {}
            logs = payload.get("logs") or []
            logs_tail = logs[-10:] if isinstance(logs, list) else []
            return {
                "status": payload.get("status"),
                "phase": payload.get("phase"),
                "run_id": payload.get("run_id"),
                "pid": payload.get("pid"),
                "started_at": payload.get("started_at"),
                "finished_at": payload.get("finished_at"),
                "last_updated_at": payload.get("last_updated_at"),
                "error": payload.get("error"),
                "options": deepcopy(payload.get("options") or {}),
                "logs_tail": deepcopy(logs_tail),
            }
        return {}

    def _build_scheduler_status_payload_for_task_audit(
        self,
        scheduler_state: dict[str, Any] | None,
        *,
        publication: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return _build_scheduler_status_payload_for_scheduler_audit(
            self.settings,
            scheduler_state,
            publication=publication,
            manual_operations_enabled=self._manual_operations_enabled_for_audit(),
        )

    def _write_task_run_audit(
        self,
        *,
        task_info: dict[str, Any],
        task_name: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        generated_at = datetime.now().isoformat(timespec="seconds")
        task_key, status_payload_key = TASK_NAME_TO_STATUS_PAYLOAD_KEYS.get(
            task_name,
            ("unknown", ""),
        )
        scheduler_state = load_scheduler_status(
            self.settings,
            apply_runtime_recovery=False,
        )
        publication = load_publication_registry(self.settings)
        scheduler_status_payload = self._build_scheduler_status_payload_for_task_audit(
            scheduler_state,
            publication=publication,
        )
        workflow_summary = self._workflow_summary_for_task_audit()
        if isinstance(workflow_summary, dict):
            # Keep the shared status snapshot for diagnostics, but make its
            # provenance explicit so consumers cannot mistake it for task
            # lineage. ``run_id`` remains for historical audit compatibility.
            observed_run_id = str(workflow_summary.get("run_id") or "").strip()
            if observed_run_id and not workflow_summary.get("observed_run_id"):
                workflow_summary["observed_run_id"] = observed_run_id
        task_closure = dict(
            ((scheduler_status_payload.get("task_closures") or {}).get(task_key) or {})
        )
        selected_run = dict(scheduler_status_payload.get(status_payload_key) or {})
        publication_layers = dict(publication.get("automation_layers") or {})
        latest_successful_run = dict(publication_layers.get("latest_successful_run") or {})
        current_published_run = dict(publication_layers.get("current_published_run") or {})

        task_token = (
            str(result.get("started_at") or task_info.get("started_at") or generated_at)
            .replace("-", "")
            .replace(":", "")
            .replace(" ", "_")
            .replace("T", "_")
        )
        audit_dir = build_audit_output_dir(
            self.settings,
            "scheduler_task_runs",
            datetime.now().strftime("%Y%m%d"),
        )
        artifact_path = audit_dir / f"{task_token}_{task_name}.json.gz"
        audit_path_fields = self._project_path_artifact_fields(artifact_path)
        task_log_path_fields = self._project_path_artifact_fields(result.get("task_log_path"))
        audit_payload = {
            "audit_version": 2,
            "persistence_stage": "audit_collected_before_completion",
            "generated_at": generated_at,
            "task": {
                "task_history_id": int(task_info.get("id") or 0) or None,
                "task_name": task_name,
                "task_key": task_key,
                "started_at": task_info.get("started_at") or result.get("started_at"),
                "finished_at": result.get("finished_at"),
                "status": result.get("status"),
                "host_context": result.get("host_context"),
            },
            "task_result": deepcopy(result),
            "workflow_summary": workflow_summary,
            "scheduler": {
                "status_payload": {
                    "automation_host_mode": scheduler_status_payload.get("automation_host_mode"),
                    "scheduler_running": scheduler_status_payload.get("scheduler_running"),
                    "current_task": deepcopy(scheduler_status_payload.get("current_task") or {}),
                    "automation_autonomy": deepcopy(
                        scheduler_status_payload.get("automation_autonomy") or {}
                    ),
                    "scheduler_chain_audit": deepcopy(
                        scheduler_status_payload.get("scheduler_chain_audit") or {}
                    ),
                    "publication_summary": deepcopy(
                        scheduler_status_payload.get("publication_summary") or {}
                    ),
                    "manual_run_history_count": len(
                        scheduler_status_payload.get("manual_run_history") or []
                    ),
                    "pending_manual_run_request_count": len(
                        scheduler_status_payload.get("pending_manual_run_requests") or []
                    ),
                },
                "task_run_summary": selected_run,
                "task_closure": task_closure,
            },
            "publication": {
                "current_published_run_id": publication.get("current_published_run_id"),
                "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
                "latest_successful_is_currently_published": bool(
                    publication_layers.get("latest_successful_is_currently_published")
                ),
                "latest_successful_run": latest_successful_run,
                "current_published_run": current_published_run,
            },
            "artifacts": {
                "task_run_audit_json": str(artifact_path),
                "task_run_audit_json_relative": audit_path_fields["relative"],
                "task_run_audit_json_project_path": audit_path_fields["project_path"],
                "task_log_file": str(result.get("task_log_path") or "") or None,
                "task_log_file_relative": task_log_path_fields["relative"],
                "task_log_file_project_path": task_log_path_fields["project_path"],
            },
        }
        write_task_audit(artifact_path, audit_payload)
        return {
            "artifact_path": str(artifact_path),
            "artifact_path_relative": audit_path_fields["relative"],
            "artifact_project_path": audit_path_fields["project_path"],
            "task_key": task_key,
            "status": str(result.get("status") or "").strip() or None,
            "workflow_run_id": _task_bound_workflow_run_id(result),
            "task_log_path": str(result.get("task_log_path") or "") or None,
            "task_log_path_relative": task_log_path_fields["relative"],
            "task_log_project_path": task_log_path_fields["project_path"],
        }

    def _has_pending_manual_run_for_job(self, job_id: str) -> bool:
        normalized = self._normalize_manual_job_id(job_id)
        snapshot = self.state_store.snapshot() or {}
        for item in snapshot.get("pending_manual_run_requests") or []:
            if str((item or {}).get("job_id") or "").strip() == normalized:
                return True
        return False

    def _run_task(
        self, task_name: str, runner: Callable[[], dict[str, Any]], *, trigger_source: str = "unspecified",
    ) -> dict[str, Any]:
        self._ensure_runtime_host_allowed()
        if not self._manual_run_lock.acquire(blocking=False):
            from backend.scheduler_busy_audit import record_scheduler_busy_skip

            return record_scheduler_busy_skip(
                settings=self.settings, state_store=self.state_store, task_recorder=self.task_recorder,
                task_name=task_name, trigger_source=trigger_source,
                host_context=self._host_context(), log=self.logger,
            )

        timing = TaskTiming()
        self.state_store.start_task(task_name)
        self._write_heartbeat_file()
        task_info = self.task_recorder.start_task(task_name)
        log_lines: list[str] = []

        started_at = datetime.now().isoformat(timespec="seconds")
        task_log_path = self._enter_task_log_context(
            task_name=task_name,
            task_info=task_info,
            started_at=started_at,
        )
        startup_catch_up_job_id = str(getattr(self._startup_recovery_context, "job_id", "") or "").strip()
        startup_catch_up_scheduler_started_at = str(
            getattr(self._startup_recovery_context, "scheduler_started_at", "") or ""
        ).strip()
        timing.start_phase("business")
        from backend.collection_calculation_capture import collection_task_context

        with collection_task_context(
            task_id=task_info.get("id"), task_name=task_name,
            started_at=started_at, trigger_source=trigger_source,
        ) as capture_state:
            try:
                result = runner()
                if "logs_tail" not in result:
                    result["logs_tail"] = log_lines[-50:]
            except Exception as exc:  # pragma: no cover - runtime defensive path
                failed_effective_nav_snapshot: dict[str, Any] = {}
                try:
                    failed_effective_nav_snapshot = load_effective_nav_snapshot(
                        self.settings.database_path,
                        coverage_ratio=self.settings.effective_nav_coverage_ratio,
                        weekday_only=self.settings.effective_nav_weekday_only,
                    ).to_dict()
                except Exception:
                    failed_effective_nav_snapshot = {}
                error_text = str(exc)
                result = {
                    "task_name": task_name,
                    "status": "failed",
                    "started_at": started_at,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "error": error_text,
                    "failure_signature": (
                        "workflow_subprocess_failed"
                        if "workflow_subprocess_failed" in error_text
                        else type(exc).__name__
                    ),
                    "failed_effective_nav_snapshot": failed_effective_nav_snapshot,
                    "error_traceback": traceback.format_exc(),
                    "logs_tail": log_lines[-50:],
                }
        if capture_state["attempts"]:
            result["collection_calculation_captures"] = deepcopy(capture_state["attempts"])
        timing.finish_phase("business")
        result.setdefault("task_log_path", task_log_path)
        result["trigger_source"] = trigger_source
        if trigger_source in {"cron", "interval"} and task_name != 'scheduled_storage_lifecycle':
            timing.start_phase("archive_observation")
            try:
                workflow = self._workflow_summary_for_task_audit()
                if workflow.get("status") == "running":
                    observation = {"status": "skipped", "reason": "business_workflow_still_running"}
                else:
                    from backend.screening_archive_shadow import observe_due_archive_shadow

                    observation = observe_due_archive_shadow(
                        self.settings, task_id=task_info.get("id"), task_name=task_name,
                        trigger_source=trigger_source, log=self._append_task_log_line,
                    )
            except Exception as exc:
                observation = {"status": "failed", "error": str(exc)}
                self._append_task_log_line(f"[screening-archive-shadow] 观察失败: {exc}")
            result["archive_shadow_observation"] = observation
            timing.finish_phase("archive_observation")
            timing.start_phase("storage_observation")
            try:
                if self._workflow_summary_for_task_audit().get("status") == "running":
                    storage_observation = {"status": "skipped", "reason": "business_workflow_still_running"}
                else:
                    from backend.storage_observation import observe_due_storage_usage

                    storage_observation = observe_due_storage_usage(
                        self.settings, task_id=task_info.get("id"), task_name=task_name,
                        trigger_source=trigger_source, log=self._append_task_log_line,
                    )
            except Exception as exc:
                storage_observation = {"status": "failed", "error": str(exc)}
                self._append_task_log_line(f"[storage-observation] 容量观察失败: {exc}")
            result["storage_observation"] = storage_observation
            timing.finish_phase("storage_observation")
        if startup_catch_up_job_id:
            result["startup_catch_up_job_id"] = startup_catch_up_job_id
            result["startup_catch_up_suspected"] = True
            result["startup_catch_up_host_context"] = self._host_context()
            if startup_catch_up_scheduler_started_at:
                result["startup_catch_up_scheduler_started_at"] = startup_catch_up_scheduler_started_at
        result.setdefault("host_context", self._host_context())
        try:
            finalized_result = self._finalize_task(task_info=task_info, task_name=task_name, result=result, timing=timing)
            self._append_task_log_line(
                "[task-finish] "
                f"status={finalized_result.get('status')} "
                f"finished_at={finalized_result.get('finished_at')} "
                f"task_run_audit_path={finalized_result.get('task_run_audit_path') or '-'}"
            )
            return finalized_result
        finally:
            self._clear_task_log_context()
            self._manual_run_lock.release()
            self._enqueue_post_task_catch_up_if_needed(completed_task_name=task_name)

    @staticmethod
    def _normalize_manual_job_id(job_id: str) -> str:
        normalized = str(job_id or "").strip()
        if normalized not in {"daily_refresh", "intraday_refresh", "weekly_maintenance"}:
            raise ValueError(f"未知任务: {job_id}")
        return normalized

    def _resolve_job_runner(self, job_id: str) -> tuple[str, Callable[[], dict[str, Any]]]:
        normalized = self._normalize_manual_job_id(job_id)
        if normalized == "daily_refresh":
            return "scheduled_daily_bond_refresh", self._execute_daily_refresh
        if normalized == "intraday_refresh":
            return "scheduled_intraday_bond_refresh", self._execute_intraday_refresh
        return "scheduled_weekly_bond_maintenance", self._execute_weekly_maintenance

    def request_manual_run(self, job_id: str, *, requested_by: str = "api") -> dict[str, Any]:
        normalized = self._normalize_manual_job_id(job_id)
        request_id = uuid.uuid4().hex[:12]
        requested_at = datetime.now().isoformat(timespec="seconds")
        task_name, runner = self._resolve_job_runner(normalized)
        current_task = (self.state_store.snapshot() or {}).get("current_task") or {}
        if current_task or self._manual_run_lock.locked():
            entry = {
                "request_id": request_id,
                "job_id": normalized,
                "task_name": task_name,
                "requested_by": str(requested_by or "api").strip() or "api",
                "requested_at": requested_at,
                "status": "rejected",
                "finished_at": requested_at,
                "error": "scheduler_busy",
                "current_task": deepcopy(current_task) if current_task else None,
            }
            self.state_store.append_manual_run_entry(entry)
            return entry

        entry = {
            "request_id": request_id,
            "job_id": normalized,
            "task_name": task_name,
            "requested_by": str(requested_by or "api").strip() or "api",
            "requested_at": requested_at,
            "status": "accepted",
        }
        self.state_store.append_manual_run_entry(entry)

        def execute() -> None:
            try:
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": "running",
                        "job_id": normalized,
                        "task_name": task_name,
                        "requested_by": str(requested_by or "api").strip() or "api",
                        "started_at": datetime.now().isoformat(timespec="seconds"),
                    },
                )
                result = self._run_task(task_name, runner)
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": str(result.get("status") or "unknown").strip() or "unknown",
                        "job_id": normalized,
                        "task_name": task_name,
                        "requested_by": str(requested_by or "api").strip() or "api",
                        "started_at": result.get("started_at"),
                        "finished_at": result.get("finished_at"),
                        "error": result.get("error"),
                        "workflow_run_id": result.get("workflow_run_id"),
                        "status_detail": result.get("status_detail"),
                        "publication_result": result.get("publication_result"),
                        "result": {
                            "status": result.get("status"),
                            "warning_reason": result.get("warning_reason"),
                            "degraded_stage": result.get("degraded_stage"),
                            "screening_triggered": result.get("screening_triggered"),
                            "report_refresh_triggered": result.get("report_refresh_triggered"),
                            "skip_reason": result.get("skip_reason"),
                        },
                    },
                )
            except Exception as exc:
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": "failed",
                        "job_id": normalized,
                        "task_name": task_name,
                        "requested_by": str(requested_by or "api").strip() or "api",
                        "finished_at": datetime.now().isoformat(timespec="seconds"),
                        "error": str(exc),
                    },
                )

        threading.Thread(target=execute, daemon=True).start()
        return {
            "request_id": request_id,
            "job_id": normalized,
            "task_name": task_name,
            "status": "accepted",
            "requested_at": requested_at,
        }

    def enqueue_manual_run_request(self, job_id: str, *, requested_by: str = "api") -> dict[str, Any]:
        normalized = self._normalize_manual_job_id(job_id)
        request_id = uuid.uuid4().hex[:12]
        requested_at = datetime.now().isoformat(timespec="seconds")
        task_name, _ = self._resolve_job_runner(normalized)
        current_task = (self.state_store.snapshot() or {}).get("current_task") or {}
        if current_task or self._manual_run_lock.locked():
            entry = {
                "request_id": request_id,
                "job_id": normalized,
                "task_name": task_name,
                "requested_by": str(requested_by or "api").strip() or "api",
                "requested_at": requested_at,
                "status": "rejected",
                "finished_at": requested_at,
                "error": "scheduler_busy",
                "current_task": deepcopy(current_task) if current_task else None,
            }
            self.state_store.append_manual_run_entry(entry)
            return entry

        entry = {
            "request_id": request_id,
            "job_id": normalized,
            "task_name": task_name,
            "requested_by": str(requested_by or "api").strip() or "api",
            "requested_at": requested_at,
            "status": "accepted",
            "execution_mode": "external_scheduler_queue",
        }
        enqueue_result = self.state_store.enqueue_unique_manual_run_request(entry)
        persisted_entry = dict(enqueue_result.get("entry") or {})
        if not bool(enqueue_result.get("accepted")):
            return persisted_entry
        return persisted_entry

    def _drain_pending_manual_run_requests_once(self) -> None:
        if self._manual_run_lock.locked():
            return
        current_task = (self.state_store.snapshot() or {}).get("current_task") or {}
        if current_task:
            return
        pending = self.state_store.claim_next_pending_manual_run_request()
        if not pending:
            return
        request_id = str(pending.get("request_id") or "").strip()
        job_id = str(pending.get("job_id") or "").strip()
        if not request_id or not job_id:
            return
        self._log(f"消费外部手工调度请求: {job_id} request_id={request_id}")
        try:
            task_name, runner = self._resolve_job_runner(job_id)
        except ValueError as exc:
            self.state_store.update_manual_run_entry(
                request_id,
                {
                    "status": "failed",
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "error": str(exc),
                },
            )
            return

        def execute() -> None:
            try:
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": "running",
                        "job_id": job_id,
                        "task_name": task_name,
                        "requested_by": str(pending.get("requested_by") or "external_queue").strip() or "external_queue",
                        "started_at": datetime.now().isoformat(timespec="seconds"),
                        "execution_mode": str(pending.get("execution_mode") or "external_scheduler_queue").strip() or "external_scheduler_queue",
                    },
                )
                trigger_source = str(pending.get("trigger_source") or "").strip() or "external_scheduler_queue"
                result = self._run_task(task_name, runner, trigger_source=trigger_source)
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": str(result.get("status") or "unknown").strip() or "unknown",
                        "job_id": job_id,
                        "task_name": task_name,
                        "requested_by": str(pending.get("requested_by") or "external_queue").strip() or "external_queue",
                        "started_at": result.get("started_at"),
                        "finished_at": result.get("finished_at"),
                        "error": result.get("error"),
                        "workflow_run_id": result.get("workflow_run_id"),
                        "status_detail": result.get("status_detail"),
                        "publication_result": result.get("publication_result"),
                        "result": {
                            "status": result.get("status"),
                            "warning_reason": result.get("warning_reason"),
                            "degraded_stage": result.get("degraded_stage"),
                            "screening_triggered": result.get("screening_triggered"),
                            "report_refresh_triggered": result.get("report_refresh_triggered"),
                            "skip_reason": result.get("skip_reason"),
                        },
                    },
                )
            except Exception as exc:
                self.state_store.update_manual_run_entry(
                    request_id,
                    {
                        "status": "failed",
                        "job_id": job_id,
                        "task_name": task_name,
                        "requested_by": str(pending.get("requested_by") or "external_queue").strip() or "external_queue",
                        "finished_at": datetime.now().isoformat(timespec="seconds"),
                        "error": str(exc),
                    },
                )

        threading.Thread(target=execute, daemon=True).start()

    def _execute_scheduled_refresh(
        self,
        *,
        task_name: str,
        refresh_window: str,
        nav_history_sync_enabled: bool,
        nav_history_sync_limit: int,
        long_term_history_enabled: bool,
        long_term_history_limit: int,
        long_term_history_refresh_stale: bool = False,
    ) -> dict[str, Any]:
        collection_profile = "daily"
        logs: list[str] = []
        period_collection_terminal_logs: list[str] = []
        host_runtime_snapshot = self._build_host_runtime_snapshot()

        def log(message: str) -> None:
            text = str(message).rstrip()
            if text:
                self._log(f"{task_name}: {text}")
                logs.append(text)
                del logs[:-200]
                if "api_source_evidence=" in text and '"collect-period-returns"' in text:
                    period_collection_terminal_logs.append(text)

        def with_period_collection_health(result: dict[str, Any]) -> dict[str, Any]:
            # Missing-only collection may hand off to screening, but neither
            # a successful report nor the bounded UI log tail may erase it.
            health = self._classify_log_severity(period_collection_terminal_logs)
            evidence = health.get("period_return_collection")
            if evidence is not None:
                result["period_return_collection"] = evidence
                if health["degraded"]:
                    if result.get("status") != "failed":
                        result["status"] = "degraded"
                    result["degraded"] = True
                    result["degraded_stage"] = result.get("degraded_stage") or "collect-period-returns"
                    result["warning_reason"] = result.get("warning_reason") or health["reason"]
            return result

        started_at = datetime.now().isoformat(timespec="seconds")
        long_term_history_maintenance_summary: dict[str, Any] | None = None
        long_term_history_maintenance_error: str | None = None
        autostock_nav_tail_fill_summary: dict[str, Any] | None = None
        autostock_nav_tail_fill_error: str | None = None
        report_refresh_triggered = False
        report_refresh_run_id = ""
        report_refresh_error: str | None = None
        report_refresh_skipped_reason: str | None = None
        maintenance_rescreen_triggered = False
        maintenance_rescreen_run_id = ""
        maintenance_rescreen_final_count: Any = None
        maintenance_rescreen_error: str | None = None
        maintenance_rescreen_skipped_reason: str | None = None
        initial_screening_triggered = False
        initial_screening_run_id = ""
        initial_screening_final_count: Any = None
        period_collection_failures: list[dict[str, Any]] = []
        holding_period_before = self._read_holding_period_snapshot(self.settings)
        before_snapshot = load_effective_nav_snapshot(
            self.settings.database_path,
            coverage_ratio=self.settings.effective_nav_coverage_ratio,
            weekday_only=self.settings.effective_nav_weekday_only,
        )
        latest_screened_snapshot = load_latest_screened_db_snapshot(self.settings.latest_report_file)
        screening_baseline_snapshot = dict(latest_screened_snapshot or {})
        should_screen_before, screening_reason_before = screening_snapshot_changed(before_snapshot, latest_screened_snapshot)

        if self.workflow_manager.is_running():
            return with_period_collection_health({
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": self._workflow_busy_skip_reason(after_collection=False),
                "collection_profile": collection_profile,
                "refresh_window": refresh_window,
                "screening_triggered": False,
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "host_runtime_snapshot": host_runtime_snapshot,
                "logs_tail": logs[-50:],
            })

        publication_blocker = configured_publication_blocker(self.settings)
        if should_screen_before and refresh_window != "daily" and publication_blocker is None:
            log("当前数据库有效快照已领先已发布版本，跳过外部采集，直接进入筛选发布")
            initial_rescreen_options: dict[str, Any] = {
                "run_collection": False,
                "run_screening": True,
                "collection_profile": collection_profile,
                "skip_annual_return_gate_shadow_audit": (refresh_window == "intraday"),
                "force_calculate_risk": True,
                "risk_refresh_changed_nav_only": True,
            }
            try:
                report = self._run_workflow_in_subprocess(
                    initial_rescreen_options,
                    log=log,
                )
            except RuntimeError as exc:
                if str(exc) != "workflow_subprocess_busy":
                    raise
                audits = self._build_data_quality_audits(
                    skip_heavy_risk_sync=(refresh_window == "intraday")
                )
                return with_period_collection_health({
                    "task_name": task_name,
                    "status": "skipped",
                    "started_at": started_at,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "skip_reason": self._workflow_busy_skip_reason(after_collection=False),
                    "collection_profile": collection_profile,
                    "refresh_window": refresh_window,
                    "screening_triggered": False,
                    "before_effective_nav_snapshot": before_snapshot.to_dict(),
                    "after_effective_nav_snapshot": before_snapshot.to_dict(),
                    "latest_screened_snapshot": latest_screened_snapshot,
                    "effective_snapshot_changed_since_collection": False,
                    "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                    "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                    "long_term_history_maintenance": long_term_history_maintenance_summary,
                    "long_term_history_maintenance_error": long_term_history_maintenance_error,
                    "host_runtime_snapshot": host_runtime_snapshot,
                    **audits,
                    "logs_tail": logs[-50:],
                })
            initial_screening_triggered = True
            initial_screening_run_id = str(report.get("run_id") or "").strip()
            initial_screening_final_count = (report.get("counts") or {}).get("final_count")
            screening_baseline_snapshot = {
                "candidate_date": getattr(before_snapshot, "candidate_date", None),
                "signature": getattr(before_snapshot, "signature", None),
            }
        else:
            if should_screen_before and refresh_window == "daily":
                log("当前数据库有效快照已领先已发布版本；每日正式刷新仍先执行外部采集再筛选发布")
            try:
                run_project_pipeline(
                    self.settings,
                    {
                        "run_collection": True,
                        "run_screening": False,
                        "collection_profile": collection_profile,
                        "skip_collect_funds": (refresh_window == "intraday"),
                        "skip_risk_calculation": False,
                        "risk_refresh_changed_nav_only": True,
                        "nav_workers_override": (
                            getattr(
                                self.settings,
                                "scheduler_intraday_nav_workers",
                                getattr(self.settings, "nav_workers", None),
                            )
                            if refresh_window == "intraday"
                            else None
                        ),
                    },
                    log,
                )
            except TiantianBusinessUnavailableError as exc:
                audits = self._build_data_quality_audits(
                    skip_heavy_risk_sync=(refresh_window == "intraday")
                )
                return with_period_collection_health({
                    "task_name": task_name,
                    "status": "degraded",
                    "degraded": True,
                    "started_at": started_at,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "degraded_stage": exc.stage,
                    "warning_reason": "tiantian_business_unavailable",
                    "tiantian_business_preflight": {
                        "stage": exc.stage,
                        "diagnostic_path": str(exc.diagnostic_path or "") or None,
                        "probe": exc.probe,
                    },
                    "collection_profile": collection_profile,
                    "refresh_window": refresh_window,
                    "screening_triggered": False,
                    "before_effective_nav_snapshot": before_snapshot.to_dict(),
                    "after_effective_nav_snapshot": before_snapshot.to_dict(),
                    "latest_screened_snapshot": latest_screened_snapshot,
                    "effective_snapshot_changed_since_collection": False,
                    "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                    "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                    "long_term_history_maintenance": long_term_history_maintenance_summary,
                    "long_term_history_maintenance_error": long_term_history_maintenance_error,
                    "host_runtime_snapshot": host_runtime_snapshot,
                    **audits,
                    "logs_tail": logs[-50:],
                })

            except CoreStepFailedError as exc:
                failures = [*exc.deferred_failures, exc.as_dict()]
                if not any(item["stage"] == "collect-period-returns" for item in failures):
                    raise
                period_collection_failures = failures
                log("区间收益补采待恢复，继续净值及历史维护；本轮暂缓筛选发布")

        nav_history_sync_summary: dict[str, Any] | None = None
        nav_history_sync_error: str | None = None
        if nav_history_sync_enabled:
            try:
                nav_history_limit = nav_history_sync_limit if nav_history_sync_limit > 0 else None
                nav_history_allowed_issue_types = [
                    "missing_nav",
                    "nav_value_mismatch",
                    "returns_missing",
                    "nav_history_lag",
                    "returns_lag",
                    "recent_tail_lag",
                ]
                if refresh_window == "intraday":
                    nav_history_sync_summary = _build_intraday_nav_history_sync_skip_summary(
                        self.settings,
                        limit=nav_history_limit,
                        allowed_issue_types=nav_history_allowed_issue_types,
                    )
                    if int(((nav_history_sync_summary.get("maintenance_result") or {}).get("processed_total") or 0)) > 0:
                        nav_history_sync_summary = run_nav_history_maintenance(
                            self.settings,
                            log=log,
                            limit=nav_history_limit,
                            allowed_issue_types=nav_history_allowed_issue_types,
                            require_preferred_sync_candidates=True,
                        )
                    else:
                        log("债基净值历史末日对齐维护跳过：当前无最新有效净值日优先候选。")
                else:
                    nav_history_sync_summary = run_nav_history_maintenance(
                        self.settings,
                        log=log,
                        limit=nav_history_limit,
                        allowed_issue_types=nav_history_allowed_issue_types,
                        require_preferred_sync_candidates=False,
                    )
            except Exception as exc:
                nav_history_sync_error = str(exc)
                log(f"债基净值历史末日对齐维护失败: {exc}")
        else:
            nav_history_sync_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "selection": {"limit": nav_history_sync_limit},
            }

        try:
            if not getattr(self.settings, "autostock_enabled", True):
                if nav_history_sync_enabled and not nav_history_sync_error:
                    autostock_nav_tail_fill_summary = {
                        "status": "replaced", "source_name": "tiantian_nav_history",
                        "autostock_status": "disabled", "independent_source": False,
                        "replacement_stage": "nav-history-sync-maintenance",
                    }
                    log("AutoStock 尾部补数已由本轮天天基金净值历史维护承担；独立来源仍不可用")
                else:
                    autostock_nav_tail_fill_summary = run_autostock_nav_tail_fill(self.settings, log=log)
            elif refresh_window == "intraday":
                autostock_nav_tail_fill_summary = _build_intraday_autostock_tail_fill_skip_summary(self.settings)
                if int(((autostock_nav_tail_fill_summary.get("maintenance_result") or {}).get("processed_total") or 0)) > 0:
                    autostock_nav_tail_fill_summary = run_autostock_nav_tail_fill(
                        self.settings,
                        log=log,
                    )
                else:
                    log(
                        "AutoStock 净值尾差复查跳过：当前无可执行候选，"
                        f"raw_candidate_total={autostock_nav_tail_fill_summary['selection']['raw_candidate_total']}, "
                        f"cooldown_skipped_total={autostock_nav_tail_fill_summary['selection']['cooldown_skipped_total']}"
                    )
            else:
                autostock_nav_tail_fill_summary = run_autostock_nav_tail_fill(
                    self.settings,
                    log=log,
                )
        except Exception as exc:
            autostock_nav_tail_fill_error = str(exc)
            log(f"AutoStock 净值尾差复查失败: {exc}")

        if long_term_history_enabled:
            try:
                resolved_long_term_history_limit = self._resolve_scheduled_long_term_history_limit(
                    refresh_window=refresh_window,
                    configured_limit=long_term_history_limit,
                    refresh_stale=bool(long_term_history_refresh_stale),
                )
                long_term_history_maintenance_summary = run_long_term_history_maintenance(
                    self.settings,
                    log=log,
                    limit=resolved_long_term_history_limit,
                    refresh_stale=bool(long_term_history_refresh_stale),
                )
            except Exception as exc:
                long_term_history_maintenance_error = str(exc)
                log(f"债基长期稳定性历史补齐失败: {exc}")
        else:
            long_term_history_maintenance_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "selection": {"limit": long_term_history_limit},
            }

        after_snapshot = load_effective_nav_snapshot(
            self.settings.database_path,
            coverage_ratio=self.settings.effective_nav_coverage_ratio,
            weekday_only=self.settings.effective_nav_weekday_only,
        )
        holding_period_after = self._read_holding_period_snapshot(self.settings)
        holding_period_change_summary = self._build_holding_period_change_summary(
            before_snapshot=holding_period_before,
            after_snapshot=holding_period_after,
        )
        should_screen, screening_reason = screening_snapshot_changed(after_snapshot, screening_baseline_snapshot)
        effective_snapshot_changed_since_collection = (
            before_snapshot.candidate_date != after_snapshot.candidate_date
            or before_snapshot.signature != after_snapshot.signature
        )
        maintenance_data_change_summary = self._build_maintenance_data_change_summary(
            nav_history_summary=nav_history_sync_summary,
            autostock_nav_tail_fill_summary=autostock_nav_tail_fill_summary,
            long_term_history_summary=long_term_history_maintenance_summary,
            holding_period_change_summary=holding_period_change_summary,
        )
        if period_collection_failures:
            # A period request may be waiting for NAV alignment. The NAV
            # maintenance above must still execute or that wait can prevent
            # its own repair on every automatic retry.
            audits = self._build_data_quality_audits_compat(skip_heavy_risk_sync=(refresh_window == "intraday"))
            return with_period_collection_health({
                "task_name": task_name, "status": "degraded", "degraded": True,
                "started_at": started_at, "finished_at": datetime.now().isoformat(timespec="seconds"),
                "degraded_stage": period_collection_failures[0]["stage"],
                "warning_reason": "period_return_collection_incomplete",
                "collection_step_failures": period_collection_failures,
                "period_return_collection": self._classify_log_severity(period_collection_terminal_logs).get("period_return_collection"),
                "collection_profile": collection_profile, "refresh_window": refresh_window,
                "screening_triggered": False,
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "after_effective_nav_snapshot": after_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
                "maintenance_data_change_summary": maintenance_data_change_summary,
                "nav_history_sync_maintenance": nav_history_sync_summary,
                "nav_history_sync_maintenance_error": nav_history_sync_error,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "holding_period_change_summary": holding_period_change_summary,
                "host_runtime_snapshot": host_runtime_snapshot, **audits, "logs_tail": logs[-50:],
            })
        lineage_rescreen_requirement = self._daily_refresh_lineage_rescreen_required(
            refresh_window=refresh_window,
        )

        if publication_blocker:
            log(f"[publication-preflight] {publication_blocker['code']}: {publication_blocker['message']}")
            return with_period_collection_health({
                "task_name": task_name, "status": "degraded", "degraded": True,
                "status_detail": "publication_blocked_unpublished",
                "started_at": started_at, "finished_at": datetime.now().isoformat(timespec="seconds"),
                "warning_reason": publication_blocker["code"], "degraded_stage": "publication-preflight",
                "publication_result": blocked_publication_result(publication_blocker),
                "publication_dependency": publication_blocker,
                "collection_completed": not any((nav_history_sync_error, autostock_nav_tail_fill_error,
                                                   long_term_history_maintenance_error)),
                "collection_profile": collection_profile, "refresh_window": refresh_window,
                "screening_triggered": False, "screening_skip_reason": publication_blocker["code"],
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "after_effective_nav_snapshot": after_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
                "maintenance_data_change_summary": maintenance_data_change_summary,
                "lineage_rescreen_requirement": lineage_rescreen_requirement,
                "nav_history_sync_maintenance": nav_history_sync_summary,
                "nav_history_sync_maintenance_error": nav_history_sync_error,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "holding_period_change_summary": holding_period_change_summary,
                "host_runtime_snapshot": host_runtime_snapshot, "logs_tail": logs[-50:],
            })

        # A real maintenance data change has its own rescreen contract.  It
        # must not be hidden behind a stale/missing lineage diagnostic: one
        # maintenance rescore repairs both the changed data and the lineage
        # evidence.  When there is no maintenance change, keep the separate
        # lineage-repair path below.
        if not should_screen and (
            not bool(lineage_rescreen_requirement.get("required"))
            or bool(maintenance_data_change_summary.get("changed"))
        ):
            if maintenance_data_change_summary.get("changed"):
                try:
                    refresh_report = self._rescreen_report_after_maintenance(
                        collection_profile=collection_profile,
                        risk_refresh_target_fcodes=self._extract_maintenance_risk_refresh_target_fcodes(
                            nav_history_summary=nav_history_sync_summary,
                        ),
                        log=log,
                    )
                    if str(refresh_report.get("status") or "").strip() == "skipped":
                        maintenance_rescreen_skipped_reason = str(
                            refresh_report.get("skip_reason") or "skipped"
                        ).strip()
                    else:
                        maintenance_rescreen_triggered = True
                        maintenance_rescreen_run_id = str(refresh_report.get("run_id") or "").strip()
                        maintenance_rescreen_final_count = (
                            (refresh_report.get("counts") or {}).get("final_count")
                        )
                except RuntimeError as exc:
                    maintenance_rescreen_error = str(exc)
                    maintenance_rescreen_skipped_reason = self._workflow_busy_skip_reason(
                        after_collection=False
                    )
                    log(f"基于维护后样本重评分跳过: {exc}")
                except Exception as exc:
                    maintenance_rescreen_error = str(exc)
                    log(f"基于维护后样本重评分失败: {exc}")
            screening_triggered = initial_screening_triggered or maintenance_rescreen_triggered
            screening_reason_value = (
                "maintenance_changed_rescreen"
                if maintenance_rescreen_triggered
                else f"{screening_reason_before}_without_collection"
                if initial_screening_triggered
                else None
            )
            screening_run_id_value = (
                maintenance_rescreen_run_id
                if maintenance_rescreen_triggered
                else initial_screening_run_id
            )
            screening_final_count_value = (
                maintenance_rescreen_final_count
                if maintenance_rescreen_triggered
                else initial_screening_final_count
            )
            audits = self._build_data_quality_audits_compat(
                skip_heavy_risk_sync=(refresh_window == "intraday")
            )
            degraded = (
                bool(nav_history_sync_error)
                or bool(autostock_nav_tail_fill_error)
                or bool(long_term_history_maintenance_error)
                or bool(report_refresh_error)
                or bool(maintenance_rescreen_error)
            )
            return with_period_collection_health({
                "task_name": task_name,
                "status": (
                    "degraded"
                    if degraded
                    else "success"
                    if (screening_triggered or report_refresh_triggered)
                    else "skipped"
                ),
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": None if screening_triggered else screening_reason,
                "collection_profile": collection_profile,
                "refresh_window": refresh_window,
                "collection_skipped": initial_screening_triggered,
                "screening_triggered": screening_triggered,
                "screening_reason": screening_reason_value,
                "degraded": degraded,
                "degraded_stage": (
                    "nav-history-sync-maintenance"
                    if nav_history_sync_error
                    else "autostock-nav-tail-fill"
                    if autostock_nav_tail_fill_error
                    else "long-term-history-maintenance"
                    if long_term_history_maintenance_error
                    else "maintenance-rescreen"
                    if maintenance_rescreen_error
                    else "report-refresh" if report_refresh_error else ""
                ),
                "warning_reason": (
                    "nav_history_sync_maintenance_failed"
                    if nav_history_sync_error
                    else "autostock_nav_tail_fill_failed"
                    if autostock_nav_tail_fill_error
                    else "long_term_history_maintenance_failed"
                    if long_term_history_maintenance_error
                    else "maintenance_rescreen_failed"
                    if maintenance_rescreen_error
                    else "report_refresh_failed" if report_refresh_error else None
                ),
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "after_effective_nav_snapshot": after_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
                "maintenance_data_change_summary": maintenance_data_change_summary,
                "lineage_rescreen_requirement": lineage_rescreen_requirement,
                "maintenance_rescreen_triggered": maintenance_rescreen_triggered,
                "maintenance_rescreen_run_id": maintenance_rescreen_run_id,
                "maintenance_rescreen_error": maintenance_rescreen_error,
                "maintenance_rescreen_skipped_reason": maintenance_rescreen_skipped_reason,
                "report_refresh_triggered": report_refresh_triggered,
                "report_refresh_run_id": report_refresh_run_id,
                "report_refresh_error": report_refresh_error,
                "report_refresh_skipped_reason": report_refresh_skipped_reason,
                "screening_run_id": screening_run_id_value,
                "screening_final_count": screening_final_count_value,
                "initial_screening_triggered": initial_screening_triggered,
                "initial_screening_run_id": initial_screening_run_id or None,
                "initial_screening_reason": (
                    f"{screening_reason_before}_without_collection"
                    if initial_screening_triggered
                    else None
                ),
                "initial_screening_final_count": initial_screening_final_count,
                "nav_history_sync_maintenance": nav_history_sync_summary,
                "nav_history_sync_maintenance_error": nav_history_sync_error,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "holding_period_change_summary": holding_period_change_summary,
                "host_runtime_snapshot": host_runtime_snapshot,
                **audits,
                "logs_tail": logs[-50:],
            })

        if self.workflow_manager.is_running():
            audits = self._build_data_quality_audits_compat(
                skip_heavy_risk_sync=(refresh_window == "intraday")
            )
            return with_period_collection_health({
                "task_name": task_name,
                "status": "degraded" if nav_history_sync_error else "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": self._workflow_busy_skip_reason(after_collection=True),
                "collection_profile": collection_profile,
                "refresh_window": refresh_window,
                "screening_triggered": False,
                "degraded": bool(nav_history_sync_error),
                "degraded_stage": (
                    "nav-history-sync-maintenance"
                    if nav_history_sync_error
                    else "autostock-nav-tail-fill"
                    if autostock_nav_tail_fill_error
                    else "long-term-history-maintenance" if long_term_history_maintenance_error else ""
                ),
                "warning_reason": (
                    "nav_history_sync_maintenance_failed"
                    if nav_history_sync_error
                    else "autostock_nav_tail_fill_failed"
                    if autostock_nav_tail_fill_error
                    else "long_term_history_maintenance_failed" if long_term_history_maintenance_error else None
                ),
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "after_effective_nav_snapshot": after_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
                "nav_history_sync_maintenance": nav_history_sync_summary,
                "nav_history_sync_maintenance_error": nav_history_sync_error,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "holding_period_change_summary": holding_period_change_summary,
                "host_runtime_snapshot": host_runtime_snapshot,
                **audits,
                "logs_tail": logs[-50:],
            })

        rescreen_options: dict[str, Any] = {
            "run_collection": False,
            "run_screening": True,
            "collection_profile": collection_profile,
            "skip_annual_return_gate_shadow_audit": (refresh_window == "intraday"),
            # Every automatic publication needs same-run risk provenance,
            # including intraday/watchdog and unchanged-snapshot lineage repair.
            # The targeted scan also catches same-day NAV corrections and is
            # cheap when no observations have changed.
            "force_calculate_risk": True,
            "risk_refresh_changed_nav_only": True,
        }

        try:
            report = self._run_workflow_in_subprocess(
                rescreen_options,
                log=log,
            )
        except RuntimeError as exc:
            if str(exc) != "workflow_subprocess_busy":
                raise
            audits = self._build_data_quality_audits_compat(
                skip_heavy_risk_sync=(refresh_window == "intraday")
            )
            # Keep the stable scheduler busy reason for API/task-history
            # compatibility.  Lineage repair context remains available in
            # ``lineage_rescreen_requirement`` and logs; it should not create
            # a second busy-state vocabulary.
            screening_skip_reason = self._workflow_busy_skip_reason(after_collection=True)
            return with_period_collection_health({
                "task_name": task_name,
                "status": "degraded" if nav_history_sync_error else "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": screening_skip_reason,
                "collection_profile": collection_profile,
                "refresh_window": refresh_window,
                "screening_triggered": False,
                "degraded": bool(nav_history_sync_error),
                "degraded_stage": (
                    "nav-history-sync-maintenance"
                    if nav_history_sync_error
                    else "autostock-nav-tail-fill"
                    if autostock_nav_tail_fill_error
                    else "long-term-history-maintenance"
                    if long_term_history_maintenance_error
                    else ""
                ),
                "warning_reason": (
                    "nav_history_sync_maintenance_failed"
                    if nav_history_sync_error
                    else "autostock_nav_tail_fill_failed"
                    if autostock_nav_tail_fill_error
                    else "long_term_history_maintenance_failed"
                    if long_term_history_maintenance_error
                    else None
                ),
                "before_effective_nav_snapshot": before_snapshot.to_dict(),
                "after_effective_nav_snapshot": after_snapshot.to_dict(),
                "latest_screened_snapshot": latest_screened_snapshot,
                "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
                "maintenance_data_change_summary": maintenance_data_change_summary,
                "lineage_rescreen_requirement": lineage_rescreen_requirement,
                "nav_history_sync_maintenance": nav_history_sync_summary,
                "nav_history_sync_maintenance_error": nav_history_sync_error,
                "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
                "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
                "long_term_history_maintenance": long_term_history_maintenance_summary,
                "long_term_history_maintenance_error": long_term_history_maintenance_error,
                "holding_period_change_summary": holding_period_change_summary,
                "host_runtime_snapshot": host_runtime_snapshot,
                **audits,
                "logs_tail": logs[-50:],
            })
        audits = self._build_data_quality_audits_compat(
            skip_heavy_risk_sync=(refresh_window == "intraday")
        )
        collection_health = self._classify_log_severity(logs)
        nav_stage_summary = (collection_health.get("summary_by_stage") or {}).get("collect-nav") or {}
        nav_failed = int(nav_stage_summary.get("summary_failed") or 0)
        degraded = (
            nav_failed > 0
            or bool(nav_history_sync_error)
            or bool(autostock_nav_tail_fill_error)
            or bool(long_term_history_maintenance_error)
        )
        if nav_failed > 0:
            degraded_stage = "collect-nav"
            warning_reason = "nav_incremental_partial_failure"
        elif nav_history_sync_error:
            degraded_stage = "nav-history-sync-maintenance"
            warning_reason = "nav_history_sync_maintenance_failed"
        elif autostock_nav_tail_fill_error:
            degraded_stage = "autostock-nav-tail-fill"
            warning_reason = "autostock_nav_tail_fill_failed"
        elif long_term_history_maintenance_error:
            degraded_stage = "long-term-history-maintenance"
            warning_reason = "long_term_history_maintenance_failed"
        else:
            degraded_stage = ""
            warning_reason = None
        final_screening_run_id = str(report.get("run_id") or "").strip()
        final_screening_final_count = (report.get("counts") or {}).get("final_count")
        if initial_screening_triggered:
            maintenance_rescreen_triggered = True
            maintenance_rescreen_run_id = final_screening_run_id
            maintenance_rescreen_final_count = final_screening_final_count
            # Preserve the original snapshot-change reason for the normal
            # collect-then-screen path.  Lineage repair is retained in its
            # dedicated evidence field, but must not relabel an ordinary
            # effective snapshot advance (older callers and audit summaries
            # consume this stable reason).
            screening_reason_value = (
                screening_reason_before
                or "maintenance_changed_rescreen"
            )
            screening_run_id_value = final_screening_run_id
            screening_final_count_value = final_screening_final_count
        elif bool(lineage_rescreen_requirement.get("required")):
            screening_reason_value = (
                screening_reason
                if should_screen
                else "automation_lineage_repair_rescreen"
            )
            screening_run_id_value = final_screening_run_id
            screening_final_count_value = final_screening_final_count
        else:
            screening_reason_value = screening_reason
            screening_run_id_value = final_screening_run_id
            screening_final_count_value = final_screening_final_count
        return with_period_collection_health({
            "task_name": task_name,
            "status": "degraded" if degraded else "success",
            "started_at": started_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "collection_profile": collection_profile,
            "refresh_window": refresh_window,
            "collection_skipped": initial_screening_triggered,
            "screening_triggered": True,
            "screening_reason": screening_reason_value,
            "degraded": degraded,
            "degraded_stage": degraded_stage,
            "records_failed": nav_failed,
            "warning_reason": warning_reason,
            "before_effective_nav_snapshot": before_snapshot.to_dict(),
            "after_effective_nav_snapshot": after_snapshot.to_dict(),
            "latest_screened_snapshot": latest_screened_snapshot,
            "effective_snapshot_changed_since_collection": effective_snapshot_changed_since_collection,
            "maintenance_data_change_summary": maintenance_data_change_summary,
            "lineage_rescreen_requirement": lineage_rescreen_requirement,
            "maintenance_rescreen_triggered": maintenance_rescreen_triggered,
            "maintenance_rescreen_run_id": maintenance_rescreen_run_id,
            "maintenance_rescreen_error": maintenance_rescreen_error,
            "maintenance_rescreen_skipped_reason": maintenance_rescreen_skipped_reason,
            "report_refresh_triggered": False,
            "report_refresh_run_id": "",
            "report_refresh_error": None,
            "report_refresh_skipped_reason": None,
            "screening_run_id": screening_run_id_value,
            "screening_final_count": screening_final_count_value,
            "initial_screening_triggered": initial_screening_triggered,
            "initial_screening_run_id": initial_screening_run_id or None,
            "initial_screening_reason": (
                f"{screening_reason_before}_without_collection"
                if initial_screening_triggered
                else None
            ),
            "initial_screening_final_count": initial_screening_final_count,
            "nav_history_sync_maintenance": nav_history_sync_summary,
            "nav_history_sync_maintenance_error": nav_history_sync_error,
            "autostock_nav_tail_fill": autostock_nav_tail_fill_summary,
            "autostock_nav_tail_fill_error": autostock_nav_tail_fill_error,
            "long_term_history_maintenance": long_term_history_maintenance_summary,
            "long_term_history_maintenance_error": long_term_history_maintenance_error,
            "holding_period_change_summary": holding_period_change_summary,
            "host_runtime_snapshot": host_runtime_snapshot,
            **audits,
            "logs_tail": logs[-50:],
        })

    def _refresh_database_freshness_summary_sidecar(self) -> dict[str, Any]:
        try:
            payload = build_database_freshness_summary(self.settings)
            artifact_path: Path | None = None
            if not payload.get("source_db_changed_during_build"):
                artifact_path = write_database_freshness_summary(self.settings, payload)
            return summarize_database_freshness_payload(
                payload,
                source="scheduler",
                artifact_path=artifact_path or database_freshness_summary_path(self.settings),
            )
        except Exception as exc:
            return {
                "status": "failed",
                "source": "scheduler",
                "error": str(exc),
                "artifact_path": str(database_freshness_summary_path(self.settings)),
            }

    def _execute_daily_refresh(self) -> dict[str, Any]:
        result = self._execute_scheduled_refresh(
            task_name="scheduled_daily_bond_refresh",
            refresh_window="daily",
            nav_history_sync_enabled=bool(
                getattr(self.settings, "scheduler_daily_nav_history_sync_enabled", True)
            ),
            nav_history_sync_limit=int(
                getattr(self.settings, "scheduler_daily_nav_history_sync_limit", DEFAULT_BATCH_SIZE) or 0
            ),
            long_term_history_enabled=bool(
                getattr(self.settings, "scheduler_daily_long_term_history_enabled", True)
            ),
            long_term_history_limit=int(
                getattr(self.settings, "scheduler_daily_long_term_history_limit", 0) or 0
            ),
            long_term_history_refresh_stale=bool(
                getattr(self.settings, "scheduler_daily_long_term_history_refresh_stale", True)
            ),
        )
        result["database_freshness_summary"] = self._refresh_database_freshness_summary_sidecar()
        return result

    def _execute_intraday_refresh(self) -> dict[str, Any]:
        result = self._execute_scheduled_refresh(
            task_name="scheduled_intraday_bond_refresh",
            refresh_window="intraday",
            nav_history_sync_enabled=bool(
                getattr(self.settings, "scheduler_intraday_nav_history_sync_enabled", True)
            ),
            nav_history_sync_limit=int(
                getattr(self.settings, "scheduler_intraday_nav_history_sync_limit", DEFAULT_BATCH_SIZE) or 0
            ),
            long_term_history_enabled=bool(
                getattr(self.settings, "scheduler_intraday_long_term_history_enabled", False)
            ),
            long_term_history_limit=int(
                getattr(self.settings, "scheduler_intraday_long_term_history_limit", 0) or 0
            ),
            long_term_history_refresh_stale=bool(
                getattr(self.settings, "scheduler_intraday_long_term_history_refresh_stale", False)
            ),
        )
        result["database_freshness_summary"] = self._refresh_database_freshness_summary_sidecar()
        return result

    def _execute_snapshot_watchdog(self) -> dict[str, Any]:
        """P0-03.2 / B5: 周期性检查有效净值快照，发现变化且当日未处理过就触发一次
        ``_execute_scheduled_refresh(refresh_window='intraday')``。

        独立 ``task_name='scheduled_snapshot_watchdog_refresh'`` 与 cron daily/intraday
        做区分，避免污染既有回归；与 `_last_refresh_processed_current_snapshot` 联合判重
        防止同快照重复触发。若 intraday_refresh 已通过 cron 跑过且处理了当前快照，
        watchdog 仅做心跳，直接 skipped。
        """
        task_name = "scheduled_snapshot_watchdog_refresh"
        started_at = datetime.now().isoformat(timespec="seconds")

        database_path = getattr(self.settings, "database_path", None)
        latest_report_file = getattr(self.settings, "latest_report_file", None)
        if database_path is None or latest_report_file is None:
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": "missing_settings",
                "screening_triggered": False,
            }

        if self.workflow_manager.is_running():
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": self._workflow_busy_skip_reason(after_collection=False),
                "screening_triggered": False,
            }

        current_snapshot = load_effective_nav_snapshot(
            database_path,
            coverage_ratio=float(getattr(self.settings, "effective_nav_coverage_ratio", 0.9) or 0.9),
            weekday_only=bool(getattr(self.settings, "effective_nav_weekday_only", True)),
        )
        latest_screened_snapshot = load_latest_screened_db_snapshot(latest_report_file)
        should_screen, reason = screening_snapshot_changed(
            current_snapshot,
            latest_screened_snapshot or {},
        )

        state_payload: dict[str, Any] = {}
        state_store = getattr(self, "state_store", None)
        if state_store is not None and hasattr(state_store, "snapshot"):
            try:
                state_payload = state_store.snapshot() or {}
            except Exception:
                state_payload = {}

        if not should_screen or self._last_refresh_processed_current_snapshot(
            current_snapshot,
            state_payload=state_payload,
        ):
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": reason if not should_screen else "current_snapshot_already_processed",
                "screening_triggered": False,
                "candidate_date": str(getattr(current_snapshot, "candidate_date", "") or ""),
                "signature": str(getattr(current_snapshot, "signature", "") or ""),
            }

        publication_blocker = configured_publication_blocker(self.settings)
        if publication_blocker:
            return {
                "task_name": task_name, "status": "degraded", "degraded": True,
                "status_detail": "publication_blocked_unpublished",
                "started_at": started_at, "finished_at": datetime.now().isoformat(timespec="seconds"),
                "warning_reason": publication_blocker["code"], "degraded_stage": "publication-preflight",
                "publication_dependency": publication_blocker,
                "publication_result": blocked_publication_result(publication_blocker),
                "screening_triggered": False, "collection_triggered": False,
                "screening_skip_reason": publication_blocker["code"],
                "candidate_date": str(getattr(current_snapshot, "candidate_date", "") or ""),
                "signature": str(getattr(current_snapshot, "signature", "") or ""),
            }

        blocked_retry_cooldown = self._blocked_snapshot_retry_cooldown(
            current_snapshot,
            state_payload=state_payload,
        )
        if blocked_retry_cooldown.get("active"):
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": blocked_retry_cooldown.get("skip_reason"),
                "screening_triggered": False,
                "candidate_date": blocked_retry_cooldown.get("candidate_date"),
                "signature": blocked_retry_cooldown.get("signature"),
                "last_attempted_at": blocked_retry_cooldown.get("last_attempted_at"),
                "next_retry_at": blocked_retry_cooldown.get("next_retry_at"),
                "remaining_seconds": blocked_retry_cooldown.get("remaining_seconds"),
                "blocked_task_name": blocked_retry_cooldown.get("task_name"),
                "blocked_screening_run_id": blocked_retry_cooldown.get("screening_run_id"),
            }

        failed_screening_retry_cooldown = self._failed_screening_retry_cooldown(
            current_snapshot,
            state_payload=state_payload,
        )
        if failed_screening_retry_cooldown.get("active"):
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": failed_screening_retry_cooldown.get("skip_reason"),
                "screening_triggered": False,
                "candidate_date": failed_screening_retry_cooldown.get("candidate_date"),
                "signature": failed_screening_retry_cooldown.get("signature"),
                "last_attempted_at": failed_screening_retry_cooldown.get("last_attempted_at"),
                "next_retry_at": failed_screening_retry_cooldown.get("next_retry_at"),
                "remaining_seconds": failed_screening_retry_cooldown.get("remaining_seconds"),
                "blocked_task_name": failed_screening_retry_cooldown.get("task_name"),
                "failure_signature": failed_screening_retry_cooldown.get("failure_signature"),
                "blocked_error": failed_screening_retry_cooldown.get("error"),
            }

        from backend.scheduler_collection_retry import period_collection_retry_cooldown

        collection_retry = period_collection_retry_cooldown(
            self.settings, current_snapshot, state_payload=state_payload,
        )
        if collection_retry.get("active"):
            return {
                "task_name": task_name, "status": "skipped", "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": collection_retry["skip_reason"], "screening_triggered": False,
                "candidate_date": collection_retry["candidate_date"], "signature": collection_retry["signature"],
                "last_attempted_at": collection_retry["last_attempted_at"],
                "next_retry_at": collection_retry["next_retry_at"], "remaining_seconds": collection_retry["remaining_seconds"],
                "blocked_task_name": collection_retry["task_name"],
                "period_return_collection": {
                    key: collection_retry[key] for key in ("saved_fund_count", "unselected_count", "blocking_count")
                },
            }

        # 快照已推进且当日尚未跑：借用 `_execute_scheduled_refresh('intraday')` 的采集 +
        # 筛选链路，但标记为 watchdog 触发，方便审计区分。
        result = self._execute_scheduled_refresh(
            task_name=task_name,
            refresh_window="intraday",
            nav_history_sync_enabled=bool(
                getattr(self.settings, "scheduler_intraday_nav_history_sync_enabled", True)
            ),
            nav_history_sync_limit=int(
                getattr(self.settings, "scheduler_intraday_nav_history_sync_limit", DEFAULT_BATCH_SIZE) or 0
            ),
            long_term_history_enabled=bool(
                getattr(self.settings, "scheduler_intraday_long_term_history_enabled", False)
            ),
            long_term_history_limit=int(
                getattr(self.settings, "scheduler_intraday_long_term_history_limit", 0) or 0
            ),
            long_term_history_refresh_stale=bool(
                getattr(self.settings, "scheduler_intraday_long_term_history_refresh_stale", False)
            ),
        )
        result.setdefault("trigger_source", "snapshot_watchdog")
        result.setdefault("snapshot_watchdog_reason", reason)
        return result

    def _execute_weekly_maintenance(
        self,
        *,
        collection_enabled: bool | None = None,
        bond_universe_sync_enabled: bool | None = None,
    ) -> dict[str, Any]:
        task_name = "scheduled_weekly_bond_maintenance"
        logs: list[str] = []
        collection_terminal_logs: list[str] = []

        def log(message: str) -> None:
            text = str(message).rstrip()
            if text:
                self._log(f"{task_name}: {text}")
                logs.append(text)
                del logs[:-200]
                if "manager_collection_summary=" in text or "detail_collection_summary=" in text:
                    collection_terminal_logs.append(text)
                elif "api_source_evidence=" in text:
                    try:
                        source = json.loads(text.split("api_source_evidence=", 1)[1])
                        if source.get("source") in {"fundMNMangerList", "fundMSNMangerInfo", "fundMNDetailInformation", "companyApi2", "fundMNPeriodIncrease"}:
                            collection_terminal_logs.append(text)
                    except (ValueError, TypeError, AttributeError):
                        pass

        started_at = datetime.now().isoformat(timespec="seconds")
        if self.workflow_manager.is_running():
            return {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "skip_reason": self._workflow_busy_skip_reason(after_collection=False),
                "collection_profile": "weekly",
                "screening_triggered": False,
                "logs_tail": logs[-50:],
            }

        weekly_collection_error: str | None = None
        weekly_collection_degraded_stage = ""
        weekly_collection_warning_reason = ""
        weekly_collection_failures: list[dict[str, Any]] = []
        weekly_collection_tiantian_business_preflight: dict[str, Any] | None = None
        if collection_enabled is None:
            weekly_collection_enabled = bool(
                getattr(self.settings, "scheduler_weekly_collection_enabled", True)
            )
        else:
            weekly_collection_enabled = bool(collection_enabled)
        weekly_collection_skip_reason = ""
        if weekly_collection_enabled:
            try:
                run_project_pipeline(
                    self.settings,
                    {
                        "run_collection": True,
                        "run_screening": False,
                        "collection_profile": "weekly",
                    },
                    log,
                )
            except TiantianBusinessUnavailableError as exc:
                weekly_collection_error = str(exc)
                weekly_collection_degraded_stage = exc.stage
                weekly_collection_warning_reason = "tiantian_business_unavailable"
                weekly_collection_tiantian_business_preflight = {
                    "stage": exc.stage,
                    "diagnostic_path": str(exc.diagnostic_path or "") or None,
                    "probe": exc.probe,
                }
                log(
                    "周维护采集阶段天天基金业务接口不可用，"
                    "继续执行数据库热冷库治理、备份治理和完成度审计"
                )
            except CoreStepFailedError as exc:
                if (exc.stage != "collect-companies"
                        and not (exc.stage in {"collect-details", "collect-managers", "collect-period-returns"}
                                 and exc.return_code == 2 and not exc.timed_out)):
                    raise
                weekly_collection_error = str(exc)
                weekly_collection_failures = [*exc.deferred_failures, exc.as_dict()]
                weekly_collection_degraded_stage = weekly_collection_failures[0]["stage"]
                weekly_collection_warning_reason = (
                    "manager_collection_failed" if weekly_collection_degraded_stage == "collect-managers"
                    else "detail_collection_incomplete" if weekly_collection_degraded_stage == "collect-details"
                    else "period_return_collection_incomplete" if weekly_collection_degraded_stage == "collect-period-returns"
                    else "company_collection_failed"
                )
                log("周维护资料采集未完整完成，保留各阶段失败状态并继续执行数据库治理链路")
        else:
            weekly_collection_skip_reason = "disabled_for_database_governance_runner"
            log("周维护采集阶段已跳过，仅执行数据库治理链路")
        # Later risk/progress output can evict early manager evidence from the
        # 200-line UI tail. Keep the few terminal records for final accounting.
        maintenance_health = self._classify_log_severity([
            *[line for line in logs if line not in collection_terminal_logs],
            *collection_terminal_logs,
        ])
        manager_collection_blocks_rescreen = (
            maintenance_health.get("degraded_stage") == "collect-managers"
            or any(item["stage"] == "collect-managers" for item in weekly_collection_failures)
        )
        period_collection_blocks_rescreen = any(
            item["stage"] == "collect-period-returns" for item in weekly_collection_failures
        )
        period_collection_degraded = (maintenance_health.get("period_return_collection") or {}).get("status") == "degraded"
        detail_collection_degraded = bool(
            maintenance_health.get("detail_collection_summary_errors")
            or (maintenance_health.get("detail_collection_summary") or {}).get("status") == "degraded"
        )
        detail_collection_blocks_rescreen = detail_collection_degraded or any(
            item["stage"] == "collect-details" for item in weekly_collection_failures
        )
        collection_rescreen_block_reason = (
            "manager_collection_incomplete" if manager_collection_blocks_rescreen
            else "detail_collection_incomplete" if detail_collection_blocks_rescreen
            else "period_return_collection_incomplete" if period_collection_blocks_rescreen else None
        )
        empty_detail_payload_audit = self._build_empty_detail_payload_audit()
        universe_sync_summary: dict[str, Any] | None = None
        universe_sync_error: str | None = None
        nav_history_maintenance_summary: dict[str, Any] | None = None
        nav_history_maintenance_error: str | None = None
        plain_open_tail_follow_up_summary: dict[str, Any] | None = None
        plain_open_tail_follow_up_error: str | None = None
        future_outlier_monitor_summary: dict[str, Any] | None = None
        future_outlier_monitor_error: str | None = None
        holding_period_no_signal_audit_summary: dict[str, Any] | None = None
        holding_period_no_signal_audit_error: str | None = None
        holding_period_high_risk_review_summary: dict[str, Any] | None = None
        holding_period_high_risk_review_error: str | None = None
        nav_history_consistency_audit_summary: dict[str, Any] | None = None
        nav_history_consistency_audit_error: str | None = None
        screening_history_cleanup_preview_summary: dict[str, Any] | None = None
        screening_history_cleanup_preview_error: str | None = None
        screening_history_cleanup_runbook_summary: dict[str, Any] | None = None
        screening_history_cleanup_runbook_error: str | None = None
        screening_history_cleanup_preflight_summary: dict[str, Any] | None = None
        screening_history_cleanup_preflight_error: str | None = None
        screening_history_archive_preview_summary: dict[str, Any] | None = None
        screening_history_archive_preview_error: str | None = None
        screening_history_archive_execution_summary: dict[str, Any] | None = None
        screening_history_archive_execution_error: str | None = None
        screening_history_archive_execution_payload: dict[str, Any] | None = None
        screening_history_archive_execution_json_path: Path | None = None
        screening_history_archive_rotation_summary: dict[str, Any] | None = None
        screening_history_archive_rotation_error: str | None = None
        screening_history_cleanup_execution_summary: dict[str, Any] | None = None
        screening_history_cleanup_execution_error: str | None = None
        nav_history_archive_preview_summary: dict[str, Any] | None = None
        nav_history_archive_preview_error: str | None = None
        nav_history_archive_vacuum_summary: dict[str, Any] | None = None
        nav_history_archive_vacuum_error: str | None = None
        rank_history_governance_audit_summary: dict[str, Any] | None = None
        rank_history_governance_audit_error: str | None = None
        rank_history_archive_preflight_summary: dict[str, Any] | None = None
        rank_history_archive_preflight_error: str | None = None
        rank_history_archive_apply_summary: dict[str, Any] | None = None
        rank_history_archive_apply_error: str | None = None
        rank_history_archive_rolling_summary: dict[str, Any] | None = None
        rank_history_archive_rolling_error: str | None = None
        rank_history_archive_vacuum_summary: dict[str, Any] | None = None
        rank_history_archive_vacuum_error: str | None = None
        incremental_vacuum_error: str | None = None
        database_backup_cleanup_preview_summary: dict[str, Any] | None = None
        database_backup_cleanup_preview_error: str | None = None
        database_backup_cleanup_execution_summary: dict[str, Any] | None = None
        database_backup_cleanup_execution_error: str | None = None
        database_backup_compression_preview_summary: dict[str, Any] | None = None
        database_backup_compression_preview_error: str | None = None
        database_backup_compression_execution_summary: dict[str, Any] | None = None
        database_backup_compression_execution_error: str | None = None
        screening_storage_growth_audit_summary: dict[str, Any] | None = None
        screening_storage_growth_audit_error: str | None = None
        task_history_governance_preview_summary: dict[str, Any] | None = None
        task_history_governance_preview_error: str | None = None
        task_history_cleanup_execution_summary: dict[str, Any] | None = None
        task_history_cleanup_execution_error: str | None = None
        database_governance_payload: dict[str, Any] | None = None
        database_governance_health_audit_summary: dict[str, Any] | None = None
        database_governance_health_audit_error: str | None = None
        database_optimization_completion_audit_summary: dict[str, Any] | None = None
        database_optimization_completion_audit_error: str | None = None
        long_term_history_maintenance_summary: dict[str, Any] | None = None
        long_term_history_maintenance_error: str | None = None
        regression_smoke_summary: dict[str, Any] | None = None
        regression_smoke_error: str | None = None
        report_refresh_triggered = False
        report_refresh_run_id = ""
        report_refresh_error: str | None = None
        report_refresh_skipped_reason: str | None = None
        maintenance_rescreen_triggered = False
        maintenance_rescreen_run_id = ""
        maintenance_rescreen_error: str | None = None
        maintenance_rescreen_skipped_reason: str | None = None
        holding_period_before = self._read_holding_period_snapshot(self.settings)
        if bond_universe_sync_enabled is None:
            weekly_bond_universe_sync_enabled = bool(
                getattr(self.settings, "scheduler_weekly_bond_universe_sync_enabled", True)
            )
        else:
            weekly_bond_universe_sync_enabled = bool(bond_universe_sync_enabled)
        if weekly_bond_universe_sync_enabled:
            try:
                universe_sync_summary = run_bond_universe_sync(self.settings, log=log)
            except Exception as exc:
                universe_sync_error = str(exc)
                log(f"债券基金全集自动对账失败: {exc}")
        else:
            universe_sync_summary = {
                "status": "skipped",
                "skip_reason": "disabled_for_database_governance_runner",
                "diffs_after_sync": {
                    "missing_in_local_vs_tiantian_total": 0,
                    "missing_in_local_vs_autostock_total": 0,
                    "missing_in_local_active_vs_tiantian_total": 0,
                    "missing_in_local_active_vs_autostock_total": 0,
                    "newly_synced_codes_total": 0,
                },
            }
        weekly_nav_history_enabled = bool(
            getattr(self.settings, "scheduler_weekly_nav_history_enabled", True)
        )
        weekly_nav_history_limit = int(
            getattr(self.settings, "scheduler_weekly_nav_history_limit", DEFAULT_BATCH_SIZE) or 0
        )
        if weekly_nav_history_enabled:
            try:
                nav_history_maintenance_summary = run_nav_history_maintenance(
                    self.settings,
                    log=log,
                    limit=(weekly_nav_history_limit if weekly_nav_history_limit > 0 else None),
                )
            except Exception as exc:
                nav_history_maintenance_error = str(exc)
                log(f"债基净值历史完整性维护失败: {exc}")
        else:
            nav_history_maintenance_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "selection": {"limit": weekly_nav_history_limit},
            }
        weekly_plain_open_tail_follow_up_enabled = bool(
            getattr(self.settings, "scheduler_weekly_plain_open_tail_follow_up_enabled", False)
        )
        weekly_plain_open_tail_follow_up_category = str(
            getattr(self.settings, "scheduler_weekly_plain_open_tail_follow_up_category", "plain_open_tail_follow_up")
            or "plain_open_tail_follow_up"
        ).strip() or "plain_open_tail_follow_up"
        weekly_plain_open_tail_follow_up_skip_risk_refresh = bool(
            getattr(self.settings, "scheduler_weekly_plain_open_tail_follow_up_skip_risk_refresh", False)
        )
        if weekly_plain_open_tail_follow_up_enabled:
            try:
                plain_open_tail_follow_up_summary = run_plain_open_tail_follow_up(
                    self.settings,
                    category=weekly_plain_open_tail_follow_up_category,
                    skip_risk_refresh=weekly_plain_open_tail_follow_up_skip_risk_refresh,
                    log=log,
                )
            except Exception as exc:
                plain_open_tail_follow_up_error = str(exc)
                log(f"普通开放型旧尾差低频跟进失败: {exc}")
        else:
            plain_open_tail_follow_up_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "category": weekly_plain_open_tail_follow_up_category,
            }
        weekly_future_outlier_monitor_enabled = bool(
            getattr(self.settings, "scheduler_weekly_future_outlier_monitor_enabled", False)
        )
        weekly_future_outlier_monitor_category = str(
            getattr(self.settings, "scheduler_weekly_future_outlier_monitor_category", "future_outlier_monitor")
            or "future_outlier_monitor"
        ).strip() or "future_outlier_monitor"
        if weekly_future_outlier_monitor_enabled:
            try:
                future_outlier_monitor_summary = run_future_outlier_monitor(
                    self.settings,
                    category=weekly_future_outlier_monitor_category,
                    log=log,
                )
            except Exception as exc:
                future_outlier_monitor_error = str(exc)
                log(f"future_outlier 轻量监控失败: {exc}")
        else:
            future_outlier_monitor_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "category": weekly_future_outlier_monitor_category,
            }
        weekly_holding_period_edge_audit_enabled = bool(
            getattr(self.settings, "scheduler_weekly_holding_period_edge_audit_enabled", True)
        )
        holding_period_edge_audit_runtime_ready = bool(
            getattr(self.settings, "reports_dir", None)
        ) and bool(
            getattr(self.settings, "database_path", None)
        )
        if weekly_holding_period_edge_audit_enabled and holding_period_edge_audit_runtime_ready:
            try:
                holding_period_no_signal_audit_summary = run_holding_period_no_signal_edges_audit(
                    self.settings
                )
                source_artifact = str(
                    (holding_period_no_signal_audit_summary or {}).get("artifact_path") or ""
                ).strip()
                holding_period_high_risk_review_summary = run_holding_period_high_risk_review(
                    self.settings,
                    source_json=(Path(source_artifact) if source_artifact else None),
                )
            except Exception as exc:
                message = str(exc)
                holding_period_no_signal_audit_error = message
                holding_period_high_risk_review_error = message
                log(f"持有期边界审计失败: {exc}")
        else:
            skip_reason = (
                "disabled_by_config"
                if not weekly_holding_period_edge_audit_enabled
                else "runtime_context_unavailable"
            )
            holding_period_no_signal_audit_summary = {
                "status": "skipped",
                "skip_reason": skip_reason,
            }
            holding_period_high_risk_review_summary = {
                "status": "skipped",
                "skip_reason": skip_reason,
            }
        weekly_nav_history_consistency_audit_enabled = bool(
            getattr(self.settings, "scheduler_weekly_nav_history_consistency_audit_enabled", True)
        )
        nav_history_consistency_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if weekly_nav_history_consistency_audit_enabled and nav_history_consistency_runtime_ready:
            try:
                audit_payload = build_nav_history_consistency_audit_payload(
                    db_path=str(self.settings.database_path),
                )
                audit_json_path, audit_md_path = write_nav_history_consistency_audit_artifacts(
                    audit_payload
                )
                nav_history_consistency_audit_summary = {
                    "status": "success",
                    "artifact_path": str(audit_json_path),
                    "markdown_path": str(audit_md_path),
                    "summary": audit_payload.get("summary") or {},
                    "actionable_issue_counts": audit_payload.get("actionable_issue_counts") or {},
                    "informational_issue_counts": audit_payload.get("informational_issue_counts") or {},
                }
            except sqlite3.Error:
                nav_history_consistency_audit_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                nav_history_consistency_audit_error = str(exc)
                log(f"净值历史一致性审计失败: {exc}")
        else:
            nav_history_consistency_audit_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_nav_history_consistency_audit_enabled
                    else "runtime_context_unavailable"
                ),
            }
        weekly_screening_history_cleanup_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_cleanup_preview_enabled",
                True,
            )
        )
        screening_history_cleanup_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        screening_history_cleanup_preview_payload: dict[str, Any] | None = None
        screening_history_cleanup_preview_json_path: Path | None = None
        screening_history_cleanup_dir: Path | None = None
        screening_history_cleanup_runbook_payload: dict[str, Any] | None = None
        screening_history_cleanup_runbook_json_path: Path | None = None
        screening_history_cleanup_preflight_payload: dict[str, Any] | None = None
        screening_history_cleanup_preflight_json_path: Path | None = None
        if weekly_screening_history_cleanup_preview_enabled and screening_history_cleanup_runtime_ready:
            try:
                screening_history_cleanup_preview_payload = build_screening_history_cleanup_preview_payload(
                    db_path=str(self.settings.database_path),
                    recent_days=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_cleanup_preview_recent_days",
                            30,
                        )
                        or 30
                    ),
                    fund_types=list(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_cleanup_preview_fund_types",
                            ("bond",),
                        )
                        or ("bond",)
                    ),
                )
                screening_history_cleanup_dir = self.settings.reports_dir / "audit" / "screening_history_cleanup"
                screening_history_cleanup_preview_json_path, preview_md_path = write_screening_history_cleanup_preview_artifacts(
                    screening_history_cleanup_preview_payload,
                    output_dir=str(screening_history_cleanup_dir),
                )
                preview_summary = screening_history_cleanup_preview_payload.get("summary") or {}
                screening_history_cleanup_preview_summary = {
                    "status": "success",
                    "artifact_path": str(screening_history_cleanup_preview_json_path),
                    "markdown_path": str(preview_md_path),
                    "summary": preview_summary,
                    "findings": screening_history_cleanup_preview_payload.get("findings") or [],
                }
                estimated_rows_to_delete = int(
                    preview_summary.get("estimated_rows_to_delete") or 0
                )
                if estimated_rows_to_delete > 0:
                    log(
                        "筛选历史 cleanup dry-run 发现 "
                        f"{estimated_rows_to_delete} 行重复成功 run 明细候选"
                    )
            except sqlite3.Error:
                screening_history_cleanup_preview_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                screening_history_cleanup_preview_error = str(exc)
                log(f"筛选历史 cleanup dry-run 预演失败: {exc}")
        else:
            screening_history_cleanup_preview_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_screening_history_cleanup_preview_enabled
                    else "runtime_context_unavailable"
                ),
            }
        weekly_screening_history_cleanup_runbook_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_cleanup_runbook_enabled",
                False,
            )
        )
        if not weekly_screening_history_cleanup_runbook_enabled:
            screening_history_cleanup_runbook_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_screening_history_cleanup_preview_enabled:
            screening_history_cleanup_runbook_summary = {
                "status": "skipped",
                "skip_reason": "preview_disabled",
            }
        elif screening_history_cleanup_preview_error:
            screening_history_cleanup_runbook_summary = {
                "status": "skipped",
                "skip_reason": "preview_failed",
            }
        elif (
            not screening_history_cleanup_runtime_ready
            or screening_history_cleanup_preview_payload is None
            or screening_history_cleanup_preview_json_path is None
            or screening_history_cleanup_dir is None
        ):
            screening_history_cleanup_runbook_summary = {
                "status": "skipped",
                "skip_reason": "preview_not_ready",
            }
        else:
            try:
                screening_history_cleanup_runbook_payload = build_screening_history_cleanup_runbook_payload(
                    preview_payload=screening_history_cleanup_preview_payload,
                    source_preview_artifact=str(screening_history_cleanup_preview_json_path),
                    output_dir=str(screening_history_cleanup_dir),
                    delete_batch_run_limit=max(
                        1,
                        int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_screening_history_cleanup_runbook_delete_batch_run_limit",
                                100,
                            )
                            or 100
                        ),
                    ),
                )
                screening_history_cleanup_runbook_json_path, runbook_md_path = write_screening_history_cleanup_runbook_artifacts(
                    screening_history_cleanup_runbook_payload,
                    output_dir=str(screening_history_cleanup_dir),
                )
                runbook_summary = dict(screening_history_cleanup_runbook_payload.get("summary") or {})
                screening_history_cleanup_runbook_summary = {
                    "status": "success",
                    "artifact_path": str(screening_history_cleanup_runbook_json_path),
                    "markdown_path": str(runbook_md_path),
                    "summary": runbook_summary,
                    "findings": screening_history_cleanup_runbook_payload.get("findings") or [],
                }
            except Exception as exc:
                screening_history_cleanup_runbook_error = str(exc)
                screening_history_cleanup_runbook_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 cleanup runbook 生成失败: {exc}")
        weekly_screening_history_cleanup_preflight_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_cleanup_preflight_enabled",
                False,
            )
        )
        if not weekly_screening_history_cleanup_preflight_enabled:
            screening_history_cleanup_preflight_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_screening_history_cleanup_runbook_enabled:
            screening_history_cleanup_preflight_summary = {
                "status": "skipped",
                "skip_reason": "runbook_disabled",
            }
        elif screening_history_cleanup_runbook_error:
            screening_history_cleanup_preflight_summary = {
                "status": "skipped",
                "skip_reason": "runbook_failed",
            }
        elif (
            not screening_history_cleanup_runtime_ready
            or screening_history_cleanup_preview_payload is None
            or screening_history_cleanup_preview_json_path is None
            or screening_history_cleanup_dir is None
            or screening_history_cleanup_runbook_payload is None
            or screening_history_cleanup_runbook_json_path is None
            or screening_history_cleanup_runbook_summary is None
            or str(screening_history_cleanup_runbook_summary.get("status") or "") != "success"
        ):
            screening_history_cleanup_preflight_summary = {
                "status": "skipped",
                "skip_reason": "runbook_not_ready",
            }
        else:
            try:
                source_runbook_artifact = str(screening_history_cleanup_runbook_summary.get("artifact_path") or "")
                screening_history_cleanup_preflight_payload = build_screening_history_cleanup_preflight_payload(
                    db_path=str(self.settings.database_path),
                    preview_payload=screening_history_cleanup_preview_payload,
                    runbook_payload=screening_history_cleanup_runbook_payload,
                    source_preview_artifact=str(screening_history_cleanup_preview_json_path),
                    source_runbook_artifact=source_runbook_artifact,
                    output_dir=str(screening_history_cleanup_dir),
                )
                screening_history_cleanup_preflight_json_path, preflight_md_path = write_screening_history_cleanup_preflight_artifacts(
                    screening_history_cleanup_preflight_payload,
                    output_dir=str(screening_history_cleanup_dir),
                )
                preflight_summary = dict(screening_history_cleanup_preflight_payload.get("summary") or {})
                screening_history_cleanup_preflight_summary = {
                    "status": "success",
                    "artifact_path": str(screening_history_cleanup_preflight_json_path),
                    "markdown_path": str(preflight_md_path),
                    "summary": preflight_summary,
                    "checks": screening_history_cleanup_preflight_payload.get("checks") or {},
                    "findings": screening_history_cleanup_preflight_payload.get("findings") or [],
                }
            except Exception as exc:
                screening_history_cleanup_preflight_error = str(exc)
                screening_history_cleanup_preflight_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 cleanup preflight 生成失败: {exc}")
        weekly_screening_history_archive_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_archive_preview_enabled",
                False,
            )
        )
        screening_history_archive_preview_payload: dict[str, Any] | None = None
        screening_history_archive_preview_json_path: Path | None = None
        screening_history_archive_dir: Path | None = None
        if not weekly_screening_history_archive_preview_enabled:
            screening_history_archive_preview_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_screening_history_cleanup_preflight_enabled:
            screening_history_archive_preview_summary = {
                "status": "skipped",
                "skip_reason": "cleanup_preflight_disabled",
            }
        elif screening_history_cleanup_preflight_error:
            screening_history_archive_preview_summary = {
                "status": "skipped",
                "skip_reason": "cleanup_preflight_failed",
            }
        elif (
            not screening_history_cleanup_runtime_ready
            or screening_history_cleanup_preview_payload is None
            or screening_history_cleanup_preview_json_path is None
            or screening_history_cleanup_preflight_payload is None
            or screening_history_cleanup_preflight_json_path is None
            or screening_history_cleanup_preflight_summary is None
            or str(screening_history_cleanup_preflight_summary.get("status") or "") != "success"
        ):
            screening_history_archive_preview_summary = {
                "status": "skipped",
                "skip_reason": "cleanup_preflight_not_ready",
            }
        else:
            try:
                screening_history_archive_dir = (
                    self.settings.reports_dir / "audit" / "screening_history_archive"
                )
                archive_target_path = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_execution_archive_target_path",
                            "backend/data/archives/screening_history_details_archive.db",
                        )
                        or "backend/data/archives/screening_history_details_archive.db"
                    ).strip()
                    or "backend/data/archives/screening_history_details_archive.db"
                )
                screening_history_archive_preview_payload = build_screening_history_archive_preview_payload(
                    db_path=str(self.settings.database_path),
                    preview_payload=screening_history_cleanup_preview_payload,
                    preflight_payload=screening_history_cleanup_preflight_payload,
                    source_preview_artifact=str(screening_history_cleanup_preview_json_path),
                    source_preflight_artifact=str(screening_history_cleanup_preflight_json_path),
                    output_dir=str(screening_history_archive_dir),
                    archive_target_path=archive_target_path,
                    project_root=str(
                        getattr(self.settings, "project_root", None)
                        or self._project_root()
                    ),
                )
                (
                    screening_history_archive_preview_json_path,
                    screening_history_archive_preview_md_path,
                ) = write_screening_history_archive_preview_artifacts(
                    screening_history_archive_preview_payload,
                    output_dir=str(screening_history_archive_dir),
                )
                archive_preview_summary = dict(
                    screening_history_archive_preview_payload.get("summary") or {}
                )
                screening_history_archive_preview_summary = {
                    "status": screening_history_archive_preview_payload.get("status")
                    or "success",
                    "artifact_path": str(screening_history_archive_preview_json_path),
                    "markdown_path": str(screening_history_archive_preview_md_path),
                    "summary": archive_preview_summary,
                    "findings": screening_history_archive_preview_payload.get("findings") or [],
                }
                if int(archive_preview_summary.get("scope_run_count") or 0) > 0:
                    log(
                        "筛选历史 cold archive preview 完成，"
                        f"scope run {archive_preview_summary.get('scope_run_count', 0)} 个，"
                        f"scope 行 {archive_preview_summary.get('total_scope_row_count', 0)}，"
                        f"估算热库可治理 {float(archive_preview_summary.get('estimated_hot_reclaimable_mb') or 0):.1f} MB"
                    )
                if str(screening_history_archive_preview_payload.get("status") or "") == "failed":
                    screening_history_archive_preview_error = (
                        "screening_history_archive_preview_failed"
                    )
            except Exception as exc:
                screening_history_archive_preview_error = str(exc)
                screening_history_archive_preview_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 cold archive preview 失败: {exc}")

        weekly_screening_history_archive_execution_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_archive_execution_enabled",
                False,
            )
        )
        if not weekly_screening_history_archive_execution_enabled:
            screening_history_archive_execution_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_screening_history_archive_preview_enabled:
            screening_history_archive_execution_summary = {
                "status": "skipped",
                "skip_reason": "archive_preview_disabled",
            }
        elif screening_history_archive_preview_error:
            screening_history_archive_execution_summary = {
                "status": "skipped",
                "skip_reason": "archive_preview_failed",
            }
        elif (
            not screening_history_cleanup_runtime_ready
            or screening_history_archive_dir is None
            or screening_history_archive_preview_payload is None
            or screening_history_archive_preview_json_path is None
        ):
            screening_history_archive_execution_summary = {
                "status": "skipped",
                "skip_reason": "archive_preview_not_ready",
            }
        else:
            try:
                archive_execution_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_execution_mode",
                            "archive-export",
                        )
                        or "archive-export"
                    )
                    .strip()
                    .lower()
                    .replace("_", "-")
                )
                if archive_execution_mode not in {"dry-run", "archive-export"}:
                    archive_execution_mode = "dry-run"
                archive_target_path = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_execution_archive_target_path",
                            "backend/data/archives/screening_history_details_archive.db",
                        )
                        or "backend/data/archives/screening_history_details_archive.db"
                    ).strip()
                    or "backend/data/archives/screening_history_details_archive.db"
                )
                (
                    screening_history_archive_execution_json_path,
                    archive_execution_md_path,
                    screening_history_archive_execution_payload,
                ) = execute_screening_history_archive(
                    db_path=str(self.settings.database_path),
                    preview_payload=screening_history_archive_preview_payload,
                    source_preview_artifact=str(screening_history_archive_preview_json_path),
                    output_dir=str(screening_history_archive_dir),
                    mode=archive_execution_mode,
                    archive_target_path=archive_target_path,
                    project_root=str(
                        getattr(self.settings, "project_root", None)
                        or self._project_root()
                    ),
                    chunk_size=max(
                        1,
                        int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_screening_history_archive_execution_chunk_size",
                                500,
                            )
                            or 500
                        ),
                    ),
                )
                archive_execution_summary = dict(
                    screening_history_archive_execution_payload.get("summary") or {}
                )
                screening_history_archive_execution_summary = {
                    "status": screening_history_archive_execution_payload.get("status") or "success",
                    "artifact_path": str(screening_history_archive_execution_json_path),
                    "markdown_path": str(archive_execution_md_path),
                    "mode": archive_execution_mode,
                    "summary": archive_execution_summary,
                    "table_results": screening_history_archive_execution_payload.get("table_results") or [],
                    "findings": screening_history_archive_execution_payload.get("findings") or [],
                    "guardrails": screening_history_archive_execution_payload.get("guardrails") or [],
                }
                if str(screening_history_archive_execution_payload.get("status") or "") == "failed":
                    screening_history_archive_execution_error = (
                        "screening_history_archive_execution_failed"
                    )
                if archive_execution_mode == "archive-export":
                    log(
                        "筛选历史 cold archive execution 完成，"
                        f"status={screening_history_archive_execution_payload.get('status') or 'unknown'}，"
                        f"新增冷库行 {archive_execution_summary.get('inserted_row_count', 0)}"
                    )
                else:
                    log(
                        "筛选历史 cold archive execution dry-run 完成，"
                        f"scope run {archive_execution_summary.get('scope_run_count', 0)} 个"
                    )
            except Exception as exc:
                screening_history_archive_execution_error = str(exc)
                screening_history_archive_execution_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 cold archive execution 失败: {exc}")
        weekly_screening_history_cleanup_execution_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_cleanup_execution_enabled",
                False,
            )
        )
        if not weekly_screening_history_cleanup_execution_enabled:
            screening_history_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_screening_history_cleanup_preflight_enabled:
            screening_history_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preflight_disabled",
            }
        elif screening_history_cleanup_preflight_error:
            screening_history_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preflight_failed",
            }
        elif (
            not screening_history_cleanup_runtime_ready
            or screening_history_cleanup_dir is None
            or screening_history_cleanup_runbook_payload is None
            or screening_history_cleanup_runbook_json_path is None
            or screening_history_cleanup_preflight_payload is None
            or screening_history_cleanup_preflight_json_path is None
            or screening_history_cleanup_preflight_summary is None
            or str(screening_history_cleanup_preflight_summary.get("status") or "") != "success"
        ):
            screening_history_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preflight_not_ready",
            }
        else:
            execution_mode = (
                str(
                    getattr(
                        self.settings,
                        "scheduler_weekly_screening_history_cleanup_execution_mode",
                        "dry-run",
                    )
                    or "dry-run"
                )
                .strip()
                .lower()
                .replace("_", "-")
            )
            if execution_mode not in {"dry-run", "apply"}:
                execution_mode = "dry-run"
            try:
                start_batch = max(
                    1,
                    int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_cleanup_execution_start_batch",
                            1,
                        )
                        or 1
                    ),
                )
                end_batch = getattr(
                    self.settings,
                    "scheduler_weekly_screening_history_cleanup_execution_end_batch",
                    None,
                )
                normalized_end_batch = (
                    max(1, int(end_batch))
                    if end_batch is not None and str(end_batch).strip() != ""
                    else None
                )
                runbook_batches_for_backup = list(
                    screening_history_cleanup_runbook_payload.get("batches") or []
                )
                selected_batches_for_backup = []
                if runbook_batches_for_backup:
                    total_runbook_batches = len(runbook_batches_for_backup)
                    backup_end_batch = normalized_end_batch or total_runbook_batches
                    for index, batch in enumerate(runbook_batches_for_backup, start=1):
                        execution_order = int(batch.get("execution_order") or index)
                        if start_batch <= execution_order <= backup_end_batch:
                            selected_batches_for_backup.append(batch)
                screening_cleanup_backup_summary: dict[str, Any] = {
                    "status": "not_requested",
                    "auto_create_backup_if_missing": bool(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_cleanup_execution_auto_create_backup_if_missing",
                            True,
                        )
                    ),
                    "backup_prefix": (
                        str(
                            getattr(
                                self.settings,
                                "scheduler_weekly_screening_history_cleanup_execution_backup_prefix",
                                SCREENING_HISTORY_CLEANUP_BACKUP_PREFIX,
                            )
                            or SCREENING_HISTORY_CLEANUP_BACKUP_PREFIX
                        ).strip()
                        or SCREENING_HISTORY_CLEANUP_BACKUP_PREFIX
                    ),
                    "backup_compress": bool(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_cleanup_execution_backup_compress",
                            True,
                        )
                    ),
                    "backup_created": False,
                    "backup_artifact_path": "",
                    "backup_evidence_artifact": "",
                    "error": "",
                }
                backup_file: str | None = None
                if execution_mode == "apply" and not selected_batches_for_backup:
                    screening_cleanup_backup_summary.update(
                        {
                            "status": "not_required_no_scope",
                            "backup_required": False,
                        }
                    )
                elif execution_mode == "apply":
                    backup_file = str(
                        screening_history_cleanup_runbook_payload.get("backup_path") or ""
                    ).strip()
                    if backup_file:
                        candidate_backup_path = Path(backup_file).expanduser()
                        if not candidate_backup_path.is_absolute():
                            candidate_backup_path = (
                                Path(
                                    getattr(self.settings, "project_root", None)
                                    or self._project_root()
                                )
                                / candidate_backup_path
                            ).resolve()
                        backup_file = str(candidate_backup_path)
                    backup_exists = bool(backup_file) and Path(backup_file).exists()
                    if backup_exists:
                        screening_cleanup_backup_summary.update(
                            {
                                "status": "existing_backup",
                                "backup_artifact_path": str(backup_file),
                            }
                        )
                    elif screening_cleanup_backup_summary["auto_create_backup_if_missing"]:
                        (
                            screening_backup_json_path,
                            _screening_backup_md_path,
                            screening_backup_payload,
                        ) = create_screening_history_cleanup_backup(
                            source_db_path=str(self.settings.database_path),
                            backup_dir=Path(self.settings.database_path).parent / "backups",
                            output_dir=str(screening_history_cleanup_dir),
                            project_root=str(
                                getattr(self.settings, "project_root", None)
                                or self._project_root()
                            ),
                            backup_prefix=screening_cleanup_backup_summary["backup_prefix"],
                            compress=bool(screening_cleanup_backup_summary["backup_compress"]),
                            keep_uncompressed=False,
                            compute_sha256=True,
                            dry_run=False,
                        )
                        screening_backup_payload_summary = dict(
                            screening_backup_payload.get("summary") or {}
                        )
                        backup_file = (
                            str(
                                screening_backup_payload_summary.get(
                                    "backup_artifact_absolute_path"
                                )
                                or ""
                            ).strip()
                            or str(
                                screening_backup_payload_summary.get("backup_artifact_path")
                                or ""
                            ).strip()
                        )
                        if backup_file:
                            created_backup_path = Path(backup_file).expanduser()
                            if not created_backup_path.is_absolute():
                                created_backup_path = (
                                    Path(
                                        getattr(self.settings, "project_root", None)
                                        or self._project_root()
                                    )
                                    / created_backup_path
                                ).resolve()
                            backup_file = str(created_backup_path)
                        if (
                            str(screening_backup_payload.get("status") or "") != "success"
                            or not backup_file
                            or not Path(backup_file).exists()
                        ):
                            raise RuntimeError(
                                "screening history cleanup backup was not created successfully"
                            )
                        screening_cleanup_backup_summary.update(
                            {
                                "status": "backup_created",
                                "backup_created": True,
                                "backup_artifact_path": backup_file,
                                "backup_evidence_artifact": str(screening_backup_json_path),
                                "summary": screening_backup_payload_summary,
                            }
                        )
                    else:
                        screening_cleanup_backup_summary.update(
                            {
                                "status": "missing_backup_not_auto_created",
                                "backup_artifact_path": str(backup_file or ""),
                            }
                        )
                (
                    execution_json_path,
                    execution_md_path,
                    execution_payload,
                ) = execute_screening_history_cleanup_runbook(
                    runbook_payload=screening_history_cleanup_runbook_payload,
                    preflight_payload=screening_history_cleanup_preflight_payload,
                    archive_execution_payload=screening_history_archive_execution_payload,
                    source_runbook_artifact=str(screening_history_cleanup_runbook_json_path),
                    source_preflight_artifact=str(screening_history_cleanup_preflight_json_path),
                    source_archive_execution_artifact=(
                        str(screening_history_archive_execution_json_path)
                        if screening_history_archive_execution_json_path is not None
                        else None
                    ),
                    output_dir=str(screening_history_cleanup_dir),
                    mode=execution_mode,
                    start_batch=start_batch,
                    end_batch=normalized_end_batch,
                    confirm_apply=(
                        SCREENING_HISTORY_CLEANUP_APPLY_CONFIRMATION
                        if execution_mode == "apply"
                        else None
                    ),
                    backup_file=backup_file,
                    backup_dir=Path(self.settings.database_path).parent / "backups",
                    backup_prefix=screening_cleanup_backup_summary["backup_prefix"],
                    backup_compress=bool(screening_cleanup_backup_summary["backup_compress"]),
                    auto_create_backup_if_missing=bool(
                        screening_cleanup_backup_summary["auto_create_backup_if_missing"]
                    ),
                    require_publication_guard=True,
                    require_archive_guard=True,
                    archive_target_path=(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_execution_archive_target_path",
                            "backend/data/archives/screening_history_details_archive.db",
                        )
                        or "backend/data/archives/screening_history_details_archive.db"
                    ),
                    project_root=str(
                        getattr(self.settings, "project_root", None)
                        or self._project_root()
                    ),
                    latest_report_file=str(
                        getattr(self.settings, "latest_report_file", "")
                    ),
                    publication_registry_file=str(
                        getattr(self.settings, "publication_registry_file", "")
                    ),
                )
                execution_summary = dict(execution_payload.get("summary") or {})
                screening_history_cleanup_execution_summary = {
                    "status": execution_payload.get("status") or "success",
                    "artifact_path": str(execution_json_path),
                    "markdown_path": str(execution_md_path),
                    "mode": execution_mode,
                    "summary": execution_summary,
                    "preflight_checks": execution_payload.get("preflight_checks") or {},
                    "publication_guard": execution_payload.get("publication_guard") or {},
                    "archive_guard": execution_payload.get("archive_guard") or {},
                    "apply_readiness": execution_payload.get("apply_readiness") or {},
                    "post_apply_consistency": execution_payload.get("post_apply_consistency") or {},
                    "findings": execution_payload.get("findings") or [],
                    "backup": screening_cleanup_backup_summary,
                }
                if str(execution_payload.get("status") or "") == "failed":
                    screening_history_cleanup_execution_error = (
                        "screening_history_cleanup_execution_failed"
                    )
                selected_run_count = int(execution_summary.get("selected_run_count") or 0)
                if execution_mode == "apply":
                    log(
                        "筛选历史 cleanup 执行完成，"
                        f"批次 {execution_summary.get('completed_batch_count', 0)}/"
                        f"{execution_summary.get('selected_batch_count', 0)}，"
                        f"删除 {int(execution_summary.get('deleted_total') or 0)} 行"
                    )
                else:
                    log(
                        "筛选历史 cleanup execution dry-run 完成，"
                        f"计划批次 {execution_summary.get('selected_batch_count', 0)}，"
                        f"run {selected_run_count} 个"
                    )
            except Exception as exc:
                screening_history_cleanup_execution_error = str(exc)
                screening_history_cleanup_execution_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 cleanup execution 失败: {exc}")
        weekly_screening_history_archive_rotation_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_history_archive_rotation_enabled",
                False,
            )
        )
        if not weekly_screening_history_archive_rotation_enabled:
            screening_history_archive_rotation_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif screening_history_archive_execution_error or screening_history_cleanup_execution_error:
            screening_history_archive_rotation_summary = {
                "status": "skipped",
                "skip_reason": "archive_or_cleanup_dependency_failed",
            }
        else:
            try:
                rotation_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_rotation_mode",
                            "auto",
                        )
                        or "auto"
                    )
                    .strip()
                    .lower()
                    .replace("_", "-")
                )
                if rotation_mode not in {"dry-run", "auto", "rotate"}:
                    rotation_mode = "dry-run"
                archive_target_path = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_execution_archive_target_path",
                            "backend/data/archives/screening_history_details_archive.db",
                        )
                        or "backend/data/archives/screening_history_details_archive.db"
                    ).strip()
                    or "backend/data/archives/screening_history_details_archive.db"
                )
                rotation_payload = build_screening_history_archive_rotation_payload(
                    active_archive_path=archive_target_path,
                    project_root=str(
                        getattr(self.settings, "project_root", None)
                        or self._project_root()
                    ),
                    mode=rotation_mode,
                    max_active_size_bytes=max(
                        1,
                        int(
                            float(
                                getattr(
                                    self.settings,
                                    "scheduler_weekly_screening_history_archive_rotation_max_active_size_gb",
                                    8,
                                )
                                or 8
                            )
                            * 1024**3
                        ),
                    ),
                    min_active_run_count=max(
                        1,
                        int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_screening_history_archive_rotation_min_active_run_count",
                                1,
                            )
                            or 1
                        ),
                    ),
                    confirm_rotate=(rotation_mode == "rotate"),
                    compute_sha256=bool(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_history_archive_rotation_compute_sha256",
                            True,
                        )
                    ),
                )
                rotation_dir = (
                    self.settings.reports_dir
                    / "audit"
                    / "screening_history_archive_rotation"
                )
                rotation_json_path, rotation_md_path = (
                    write_screening_history_archive_rotation_artifacts(
                        rotation_payload,
                        output_dir=str(rotation_dir),
                    )
                )
                screening_history_archive_rotation_summary = {
                    "status": rotation_payload.get("status") or "",
                    "artifact_path": str(rotation_json_path),
                    "markdown_path": str(rotation_md_path),
                    "summary": rotation_payload.get("summary") or {},
                    "findings": rotation_payload.get("findings") or [],
                    "blockers": rotation_payload.get("blockers") or [],
                }
                if rotation_payload.get("status") == "blocked":
                    screening_history_archive_rotation_error = (
                        "screening_history_archive_rotation_blocked"
                    )
                if (not screening_history_archive_rotation_error and rotation_mode != "dry-run"
                        and getattr(self.settings, "scheduler_weekly_screening_history_archive_rotation_columnar_compression", False)):
                    from utils.screening_columnar_maintenance import maintain_columnar_archive

                    project_root = Path(getattr(self.settings, "project_root", None) or self._project_root())
                    binding_path = project_root / "config/screening_parquet_bindings.json"
                    existing_copies = json.loads(binding_path.read_text()).get("shadow_copies", []) if binding_path.exists() else []
                    columnar_result = maintain_columnar_archive(
                        project_root=project_root, active_archive_path=Path(archive_target_path),
                        hot_db_path=Path(self.settings.database_path),
                        output_dir=self.settings.reports_dir / "audit/screening_columnar_maintenance",
                        mode="apply", existing_copies=existing_copies, log=log,
                        workflow_state_file=getattr(self.settings, "state_file", project_root / "backend/data/workflow_state.json"),
                    )
                    screening_history_archive_rotation_summary["columnar_storage"] = columnar_result
                    if columnar_result["status"] in {"failed", "deferred"}:
                        screening_history_archive_rotation_error = "screening_columnar_conversion_failed"
                log(
                    "筛选历史 archive rotation 完成，"
                    f"status={rotation_payload.get('status') or 'unknown'}，"
                    f"active_size_gb={float(((rotation_payload.get('summary') or {}).get('active_size_bytes') or 0)) / 1024**3:.2f}"
                )
            except Exception as exc:
                screening_history_archive_rotation_error = str(exc)
                screening_history_archive_rotation_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"筛选历史 archive rotation 失败: {exc}")
        weekly_nav_history_archive_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_preview_enabled",
                False,
            )
        )
        nav_history_archive_preview_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if weekly_nav_history_archive_preview_enabled and nav_history_archive_preview_runtime_ready:
            try:
                nav_history_archive_preview_summary = self._run_nav_history_archive_preview()
                preview = nav_history_archive_preview_summary.get("summary") or {}
                readiness_status = str(preview.get("readiness_status") or "").strip()
                estimated_rows = int(preview.get("estimated_archive_row_count") or 0)
                apply_status = str(preview.get("apply_runner_status") or "").strip()
                apply_blockers = int(preview.get("apply_runner_blocker_count") or 0)
                if apply_status in {
                    "blocked",
                    "archive_export_failed",
                    "archive_binding_health_blocked",
                } or apply_blockers > 0:
                    nav_history_archive_preview_error = (
                        f"nav_history_archive_preview_blocked:{apply_status or 'unknown'}"
                    )
                    log(
                        "NAV 热冷库治理任务阻断，"
                        f"runner={apply_status or 'unknown'}，blockers={apply_blockers}"
                    )
                if estimated_rows > 0:
                    inserted_rows = int(preview.get("archive_export_inserted_row_count") or 0)
                    log(
                        "NAV 热冷库治理任务完成，readiness="
                        f"{readiness_status or 'unknown'}，runner={apply_status or 'unknown'}，"
                        f"本页归档写入 {inserted_rows} 行，冷数据候选约 {estimated_rows} 行"
                    )
            except sqlite3.Error:
                nav_history_archive_preview_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                nav_history_archive_preview_error = str(exc)
                log(f"NAV 热冷库治理预览失败: {exc}")
        else:
            nav_history_archive_preview_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_nav_history_archive_preview_enabled
                    else "runtime_context_unavailable"
                ),
            }
        weekly_nav_history_archive_vacuum_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_nav_history_archive_vacuum_enabled",
                False,
            )
        )
        nav_history_archive_vacuum_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if weekly_nav_history_archive_vacuum_enabled and nav_history_archive_vacuum_runtime_ready:
            try:
                nav_vacuum_output_dir = self.settings.reports_dir / "audit" / "nav_history_archive"
                nav_vacuum_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_nav_history_archive_vacuum_mode",
                            "auto",
                        )
                        or "auto"
                    ).strip()
                    or "auto"
                )
                nav_vacuum_confirm_replace = bool(
                    getattr(
                        self.settings,
                        "scheduler_weekly_nav_history_archive_vacuum_confirm_replace",
                        True,
                    )
                )
                nav_vacuum_min_free_space_multiplier = float(
                    getattr(
                        self.settings,
                        "scheduler_weekly_nav_history_archive_vacuum_min_free_space_multiplier",
                        1.25,
                    )
                    or 1.25
                )
                vacuum_payload = build_nav_history_archive_vacuum_runner_payload(
                    db_path=str(self.settings.database_path),
                    project_root=str(
                        getattr(self.settings, "project_root", None) or self._project_root()
                    ),
                    output_dir=str(nav_vacuum_output_dir),
                    mode=nav_vacuum_mode,
                    min_free_space_multiplier=nav_vacuum_min_free_space_multiplier,
                    confirm_replace=nav_vacuum_confirm_replace,
                )
                vacuum_json_path, vacuum_md_path = write_nav_history_archive_vacuum_runner_artifacts(
                    vacuum_payload,
                    output_dir=str(nav_vacuum_output_dir),
                )
                vacuum_summary = dict(vacuum_payload.get("summary") or {})
                vacuum_blockers = list(vacuum_payload.get("blockers") or [])
                nav_history_archive_vacuum_summary = {
                    "status": "success",
                    "artifact_path": str(vacuum_json_path),
                    "markdown_path": str(vacuum_md_path),
                    "summary": vacuum_summary,
                    "findings": vacuum_payload.get("findings") or [],
                    "blockers": vacuum_blockers,
                    "review_items": vacuum_payload.get("review_items") or [],
                }
                runner_status = str(vacuum_summary.get("runner_status") or "").strip()
                if runner_status == "blocked":
                    if bool(vacuum_summary.get("archive_binding_active")):
                        nav_history_archive_vacuum_error = "nav_history_archive_vacuum_blocked"
                    else:
                        nav_history_archive_vacuum_summary["status"] = "skipped"
                        nav_history_archive_vacuum_summary[
                            "skip_reason"
                        ] = "nav_archive_binding_not_active"
                if (
                    nav_history_archive_vacuum_error is None
                    and str(vacuum_summary.get("vacuum_result_status") or "") == "failed"
                ):
                    nav_history_archive_vacuum_error = "nav_history_archive_vacuum_into_failed"
                if (
                    nav_history_archive_vacuum_error is None
                    and str(vacuum_summary.get("replace_result_status") or "") == "failed"
                ):
                    nav_history_archive_vacuum_error = "nav_history_archive_vacuum_replace_failed"
                if vacuum_summary.get("vacuum_executed") or vacuum_summary.get("replace_executed"):
                    log(
                        "NAV 热冷治理压缩阶段完成，"
                        f"runner={runner_status or 'unknown'}，"
                        f"vacuum={bool(vacuum_summary.get('vacuum_executed'))}，"
                        f"replace={bool(vacuum_summary.get('replace_executed'))}，"
                        f"回收估算 {float(vacuum_summary.get('estimated_reclaim_mb') or 0):.1f} MB"
                    )
            except sqlite3.Error:
                nav_history_archive_vacuum_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                nav_history_archive_vacuum_error = str(exc)
                nav_history_archive_vacuum_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"NAV 热冷治理压缩阶段失败: {exc}")
        else:
            nav_history_archive_vacuum_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_nav_history_archive_vacuum_enabled
                    else "runtime_context_unavailable"
                ),
            }
        rank_history_archive_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        reports_dir_for_rank_archive = Path(
            getattr(self.settings, "reports_dir", "reports")
        )
        preflight_output_dir = reports_dir_for_rank_archive / "audit" / "rank_history_archive"
        rank_archive_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_rank_history_archive_enabled",
                True,
            )
        )
        rank_archive_mode = (
            str(
                getattr(
                    self.settings,
                    "scheduler_weekly_rank_history_archive_mode",
                    "auto",
                )
                or "auto"
            ).strip()
            or "auto"
        )
        rank_archive_page_limit = max(
            1,
            int(
                getattr(
                    self.settings,
                    "scheduler_weekly_rank_history_archive_page_limit",
                    DEFAULT_RANK_ARCHIVE_PAGE_LIMIT,
                )
                or DEFAULT_RANK_ARCHIVE_PAGE_LIMIT
            ),
        )
        rank_archive_auto_create_backup = bool(
            getattr(
                self.settings,
                "scheduler_weekly_rank_history_archive_auto_create_backup_if_missing",
                True,
            )
        )
        rank_archive_backup_prefix = (
            str(
                getattr(
                    self.settings,
                    "scheduler_weekly_rank_history_archive_backup_prefix",
                    RANK_HISTORY_ARCHIVE_BACKUP_PREFIX,
                )
                or RANK_HISTORY_ARCHIVE_BACKUP_PREFIX
            ).strip()
            or RANK_HISTORY_ARCHIVE_BACKUP_PREFIX
        )
        rank_archive_backup_compress = bool(
            getattr(
                self.settings,
                "scheduler_weekly_rank_history_archive_backup_compress",
                True,
            )
        )
        rank_rolling_active_binding_available = False

        def run_rank_history_archive_rolling_maintenance(
            governance_payload: dict[str, Any] | None = None,
            *,
            write_inactive: bool = False,
        ) -> bool:
            nonlocal rank_history_archive_rolling_summary
            nonlocal rank_history_archive_rolling_error
            nonlocal rank_history_archive_apply_summary

            rank_rolling_mode = (
                "rolling_archive"
                if rank_archive_mode in {"auto", "archive_export", "hot_delete"}
                else "dry_run"
            )
            max_pages = max(1, min(6, int(getattr(
                self.settings, "scheduler_weekly_rank_history_archive_max_pages_per_run", 1,
            ) or 1)))
            page_records: list[dict[str, Any]] = []
            total_keys = (
                "stale_hot_archive_reconcile_inserted_row_count",
                "stale_hot_archive_reconcile_updated_row_count",
                "stale_hot_archive_reconcile_revision_audit_row_count",
                "stale_hot_delete_deleted_row_count",
                "rolling_archive_inserted_row_count",
                "rolling_hot_delete_deleted_row_count",
            )
            totals = {key: 0 for key in total_keys}
            active_binding_available = False
            try:
                for page_index in range(1, max_pages + 1):
                    started = time.monotonic()
                    log(f"rank 历史滚动归档第 {page_index}/{max_pages} 页开始，mode={rank_rolling_mode}")
                    rank_rolling_payload = build_rank_history_archive_rolling_runner_payload(
                        db_path=str(self.settings.database_path),
                        project_root=str(getattr(self.settings, "project_root", None) or self._project_root()),
                        output_dir=str(preflight_output_dir),
                        governance_payload=governance_payload,
                        mode=rank_rolling_mode,
                        page_limit=rank_archive_page_limit,
                    )
                    rank_rolling_summary = dict(rank_rolling_payload.get("summary") or {})
                    active_binding_available = bool(rank_rolling_summary.get("active_binding_available"))
                    if not active_binding_available and not write_inactive:
                        return False
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f") + f"_p{page_index:03d}"
                    rank_rolling_json_path, rank_rolling_md_path = write_rank_history_archive_rolling_runner_artifacts(
                        rank_rolling_payload, output_dir=str(preflight_output_dir), timestamp=timestamp,
                    )
                    rank_rolling_status = str(rank_rolling_summary.get("runner_status") or "").strip()
                    page_record = {
                        "page_index": page_index, "runner_status": rank_rolling_status,
                        "artifact_path": str(rank_rolling_json_path),
                        "markdown_path": str(rank_rolling_md_path),
                        "duration_seconds": round(time.monotonic() - started, 3),
                        "hot_before_current_cutoff_row_count": rank_rolling_summary.get("hot_before_current_cutoff_row_count"),
                        "hot_before_active_cutoff_row_count": rank_rolling_summary.get("hot_before_active_cutoff_row_count"),
                        "hot_unarchived_window_row_count": rank_rolling_summary.get("hot_unarchived_window_row_count"),
                        "blocker_count": int(rank_rolling_summary.get("blocker_count") or 0),
                    }
                    for key in total_keys:
                        value = int(rank_rolling_summary.get(key) or 0)
                        page_record[key] = value
                        totals[key] += value
                    page_records.append(page_record)
                    rank_history_archive_rolling_summary = {
                        "status": "success",
                        "artifact_path": str(rank_rolling_json_path),
                        "markdown_path": str(rank_rolling_md_path),
                        "summary": rank_rolling_summary,
                        "findings": rank_rolling_payload.get("findings") or [],
                        "blockers": rank_rolling_payload.get("blockers") or [],
                        "review_items": rank_rolling_payload.get("review_items") or [],
                        "pages": page_records, "page_count": len(page_records), "max_pages_per_run": max_pages,
                        "totals": dict(totals), "budget_exhausted": False,
                    }
                    candidate = rank_rolling_payload.get("updated_active_binding_payload")
                    if isinstance(candidate, dict) and candidate:
                        committed = rank_rolling_payload.get("committed_active_binding") or {}
                        if committed:
                            binding_json = Path(committed["artifact_path"])
                            binding_md = Path(committed["markdown_path"])
                        else:
                            binding_json, binding_md = write_rank_history_archive_apply_runner_artifacts(
                                candidate, output_dir=str(preflight_output_dir),
                                timestamp=timestamp + "_rank_rolling_binding",
                            )
                        rank_history_archive_rolling_summary["updated_active_binding"] = {
                            "artifact_path": str(binding_json), "markdown_path": str(binding_md),
                        }
                        rank_history_archive_apply_summary = {
                            "status": "success", "artifact_path": str(binding_json), "markdown_path": str(binding_md),
                            "summary": candidate.get("summary") or {}, "findings": candidate.get("findings") or [],
                            "blockers": candidate.get("blockers") or [],
                            "backup": {"status": "skipped_existing_binding_active"},
                        }
                    log(
                        f"rank 历史滚动归档第 {page_index}/{max_pages} 页完成，runner={rank_rolling_status or 'unknown'}，"
                        f"旧热库删除 {page_record['stale_hot_delete_deleted_row_count']} 行，"
                        f"新窗口写入 {page_record['rolling_archive_inserted_row_count']} 行，"
                        f"新窗口删除 {page_record['rolling_hot_delete_deleted_row_count']} 行，"
                        f"待清理热行={page_record['hot_before_current_cutoff_row_count']}，"
                        f"待归档新窗口={page_record['hot_unarchived_window_row_count']}，"
                        f"耗时={page_record['duration_seconds']}s"
                    )
                    if not active_binding_available:
                        break  # no cumulative binding yet; the initial apply path handles it
                    if rank_rolling_status == "rolling_archive_blocked" or page_record["blocker_count"]:
                        rank_history_archive_rolling_error = f"rank_history_archive_rolling_blocked:{rank_rolling_status or 'unknown'}"
                        rank_history_archive_rolling_summary["status"] = "failed"
                        break
                    if rank_rolling_mode != "rolling_archive" or rank_rolling_status in {
                        "rolling_archive_complete_binding_ready", "rolling_archive_not_required",
                    }:
                        break
                    if not sum(page_record[key] for key in total_keys):
                        rank_history_archive_rolling_error = "rank_history_archive_rolling_no_progress"
                        rank_history_archive_rolling_summary["status"] = "failed"
                        rank_history_archive_rolling_summary["stop_reason"] = "no_progress"
                        break
                    if page_index == max_pages:
                        rank_history_archive_rolling_summary["budget_exhausted"] = True
                        rank_history_archive_rolling_summary["stop_reason"] = "page_budget_exhausted"
                        log("rank 历史滚动归档达到本轮页数预算，剩余真实积压留待下一次自动维护。")
                return active_binding_available
            except Exception as exc:
                # This first invocation may already have copied rows. Never hide
                # a write failure as a lightweight probe or start a second runner.
                if write_inactive or rank_rolling_mode == "rolling_archive":
                    rank_history_archive_rolling_error = str(exc)
                    rank_history_archive_rolling_summary = {
                        "status": "failed", "error": str(exc), "pages": page_records,
                        "page_count": len(page_records), "max_pages_per_run": max_pages, "totals": totals,
                    }
                    log(f"rank 历史滚动归档执行器失败: {exc}")
                    return True  # handled as failed; suppress an independent apply fallback
                log(f"rank 历史滚动归档轻量探测跳过: {exc}")
                return False

        if rank_archive_enabled and rank_history_archive_runtime_ready:
            rank_rolling_active_binding_available = (
                run_rank_history_archive_rolling_maintenance(
                    governance_payload=None,
                    write_inactive=False,
                )
            )

        weekly_rank_history_governance_audit_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_rank_history_governance_audit_enabled",
                False,
            )
        )
        rank_history_governance_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if (
            weekly_rank_history_governance_audit_enabled
            and rank_history_governance_runtime_ready
            and not rank_rolling_active_binding_available
        ):
            try:
                rank_payload = build_rank_history_governance_audit_payload(
                    db_path=str(self.settings.database_path),
                    project_root=str(
                        getattr(self.settings, "project_root", None) or self._project_root()
                    ),
                )
                rank_output_dir = self.settings.reports_dir / "audit" / "rank_history_governance"
                rank_json_path, rank_md_path = write_rank_history_governance_audit_artifacts(
                    rank_payload,
                    output_dir=str(rank_output_dir),
                )
                rank_summary = rank_payload.get("summary") or {}
                rank_history_governance_audit_summary = {
                    "status": "success",
                    "artifact_path": str(rank_json_path),
                    "markdown_path": str(rank_md_path),
                    "summary": rank_summary,
                    "findings": rank_payload.get("findings") or [],
                    "blockers": rank_payload.get("blockers") or [],
                }
                rank_candidates = int(
                    rank_summary.get("estimated_archive_row_count") or 0
                )
                rank_blockers = int(rank_summary.get("blocker_count") or 0)
                preflight_payload: dict[str, Any] | None = None
                preflight_json_path: Path | None = None
                preflight_md_path: Path | None = None
                if rank_archive_enabled and rank_blockers == 0:
                    rank_rolling_active_binding_available = (
                        run_rank_history_archive_rolling_maintenance(
                            governance_payload=rank_payload,
                            write_inactive=True,
                        )
                    )
                if (
                    not rank_rolling_active_binding_available
                    and rank_candidates > 0
                    and rank_blockers == 0
                ):
                    try:
                        preflight_payload = build_rank_history_archive_preflight_payload(
                            db_path=str(self.settings.database_path),
                            project_root=str(
                                getattr(self.settings, "project_root", None) or self._project_root()
                            ),
                            governance_payload=rank_payload,
                            source_governance_artifact=str(rank_json_path),
                            archive_output_dir=str(preflight_output_dir),
                        )
                        preflight_json_path, preflight_md_path = (
                            write_rank_history_archive_preflight_artifacts(
                                preflight_payload,
                                output_dir=str(preflight_output_dir),
                            )
                        )
                        rank_history_archive_preflight_summary = {
                            "status": "success",
                            "artifact_path": str(preflight_json_path),
                            "markdown_path": str(preflight_md_path),
                            "summary": preflight_payload.get("summary") or {},
                            "findings": preflight_payload.get("findings") or [],
                            "blockers": preflight_payload.get("blockers") or [],
                        }
                    except Exception as exc:
                        rank_history_archive_preflight_error = str(exc)
                        rank_history_archive_preflight_summary = {
                            "status": "failed",
                            "error": str(exc),
                        }
                        log(f"rank 历史归档 preflight 失败: {exc}")
                    if (
                        rank_archive_enabled
                        and preflight_payload is not None
                        and preflight_json_path is not None
                        and int((preflight_payload.get("summary") or {}).get("blocker_count") or 0) == 0
                    ):
                        try:
                            rank_apply_payload = build_rank_history_archive_apply_runner_payload(
                                db_path=str(self.settings.database_path),
                                project_root=str(
                                    getattr(self.settings, "project_root", None) or self._project_root()
                                ),
                                output_dir=str(preflight_output_dir),
                                preflight_payload=preflight_payload,
                                source_preflight_artifact=str(preflight_json_path),
                                backup_artifact_path=None,
                                mode=rank_archive_mode,
                                page_limit=rank_archive_page_limit,
                            )
                            rank_apply_summary = dict(rank_apply_payload.get("summary") or {})
                            rank_backup_summary: dict[str, Any] = {
                                "status": "not_requested",
                                "auto_create_backup_if_missing": rank_archive_auto_create_backup,
                                "backup_prefix": rank_archive_backup_prefix,
                                "backup_created": False,
                                "backup_artifact_path": "",
                                "backup_evidence_artifact": "",
                                "error": "",
                            }
                            gate_checks = list(rank_apply_payload.get("gate_checks") or [])
                            backup_blocked = any(
                                str(item.get("key") or "") == "hot_delete_backup"
                                and str(item.get("status") or "") == "blocked"
                                for item in gate_checks
                                if isinstance(item, dict)
                            )
                            other_blocked = any(
                                str(item.get("status") or "") == "blocked"
                                and str(item.get("key") or "") != "hot_delete_backup"
                                for item in gate_checks
                                if isinstance(item, dict)
                            )
                            if backup_blocked and not other_blocked and rank_archive_auto_create_backup:
                                try:
                                    rank_backup_dir = Path(self.settings.database_path).parent / "backups"
                                    (
                                        rank_backup_json_path,
                                        _rank_backup_md_path,
                                        rank_backup_payload,
                                    ) = create_rank_history_archive_backup(
                                        source_db_path=str(self.settings.database_path),
                                        backup_dir=rank_backup_dir,
                                        output_dir=str(preflight_output_dir),
                                        project_root=str(
                                            getattr(self.settings, "project_root", None) or self._project_root()
                                        ),
                                        backup_prefix=rank_archive_backup_prefix,
                                        compress=rank_archive_backup_compress,
                                        keep_uncompressed=False,
                                        compute_sha256=True,
                                        dry_run=False,
                                    )
                                    backup_summary = dict(rank_backup_payload.get("summary") or {})
                                    backup_artifact_path = (
                                        str(backup_summary.get("backup_artifact_absolute_path") or "").strip()
                                        or str(backup_summary.get("backup_artifact_path") or "").strip()
                                    )
                                    rank_backup_summary.update(
                                        {
                                            "status": "backup_created",
                                            "backup_created": True,
                                            "backup_artifact_path": backup_artifact_path,
                                            "backup_evidence_artifact": str(rank_backup_json_path),
                                        }
                                    )
                                    rank_apply_payload = build_rank_history_archive_apply_runner_payload(
                                        db_path=str(self.settings.database_path),
                                        project_root=str(
                                            getattr(self.settings, "project_root", None) or self._project_root()
                                        ),
                                        output_dir=str(preflight_output_dir),
                                        preflight_payload=preflight_payload,
                                        source_preflight_artifact=str(preflight_json_path),
                                        backup_artifact_path=backup_artifact_path,
                                        mode=rank_archive_mode,
                                        page_limit=rank_archive_page_limit,
                                    )
                                    rank_apply_summary = dict(rank_apply_payload.get("summary") or {})
                                except Exception as exc:
                                    rank_backup_summary.update(
                                        {
                                            "status": "backup_create_failed",
                                            "error": f"{type(exc).__name__}: {exc}",
                                        }
                                    )
                                    rank_history_archive_apply_error = (
                                        f"rank_archive_backup_create_failed:{exc}"
                                    )
                            rank_apply_json_path, rank_apply_md_path = (
                                write_rank_history_archive_apply_runner_artifacts(
                                    rank_apply_payload,
                                    output_dir=str(preflight_output_dir),
                                )
                            )
                            rank_history_archive_apply_summary = {
                                "status": "success",
                                "artifact_path": str(rank_apply_json_path),
                                "markdown_path": str(rank_apply_md_path),
                                "summary": rank_apply_summary,
                                "findings": rank_apply_payload.get("findings") or [],
                                "blockers": rank_apply_payload.get("blockers") or [],
                                "backup": rank_backup_summary,
                            }
                            runner_status = str(
                                rank_apply_summary.get("runner_status") or ""
                            ).strip()
                            if (
                                rank_history_archive_apply_error is None
                                and (
                                    runner_status == "blocked"
                                    or int(rank_apply_summary.get("blocker_count") or 0) > 0
                                )
                            ):
                                rank_history_archive_apply_error = (
                                    f"rank_history_archive_apply_blocked:{runner_status or 'unknown'}"
                                )
                            if (
                                rank_history_archive_apply_error is None
                                and str(rank_apply_summary.get("archive_export_result_status") or "")
                                == "failed"
                            ):
                                rank_history_archive_apply_error = "rank_history_archive_export_failed"
                            if (
                                rank_history_archive_apply_error is None
                                and str(rank_apply_summary.get("hot_delete_result_status") or "")
                                == "failed"
                            ):
                                rank_history_archive_apply_error = "rank_history_archive_hot_delete_failed"
                            if rank_apply_summary.get("expected_archive_row_count"):
                                log(
                                    "rank 历史热冷治理执行器完成，"
                                    f"runner={runner_status or 'unknown'}，"
                                    f"归档写入 {int(rank_apply_summary.get('archive_export_inserted_row_count') or 0)} 行，"
                                    f"热库删除 {int(rank_apply_summary.get('hot_delete_deleted_row_count') or 0)} 行"
                                )
                        except Exception as exc:
                            rank_history_archive_apply_error = str(exc)
                            rank_history_archive_apply_summary = {
                                "status": "failed",
                                "error": str(exc),
                            }
                            log(f"rank 历史归档执行器失败: {exc}")
                    elif rank_archive_enabled:
                        rank_history_archive_apply_summary = {
                            "status": "skipped",
                            "skip_reason": (
                                "rank_archive_preflight_not_ready"
                                if preflight_payload is not None
                                else "rank_archive_preflight_failed"
                            ),
                        }
                    else:
                        rank_history_archive_apply_summary = {
                            "status": "skipped",
                            "skip_reason": "disabled_by_config",
                        }
                else:
                    rank_history_archive_preflight_summary = {
                        "status": "skipped",
                        "skip_reason": (
                            "existing_archive_binding_active"
                            if rank_rolling_active_binding_available
                            else (
                                "rank_governance_blocked"
                                if rank_blockers > 0
                                else "no_rank_archive_candidates"
                            )
                        ),
                    }
                    if rank_history_archive_apply_summary is None:
                        rank_history_archive_apply_summary = {
                            "status": "skipped",
                            "skip_reason": (
                                "existing_archive_binding_active"
                                if rank_rolling_active_binding_available
                                else (
                                    "rank_governance_blocked"
                                    if rank_blockers > 0
                                    else "no_rank_archive_candidates"
                                )
                            ),
                        }
                if rank_candidates > 0:
                    log(
                        "rank 历史热冷治理只读审计完成，"
                        f"冷数据候选约 {rank_candidates} 行，blockers={rank_blockers}"
                    )
            except sqlite3.Error:
                rank_history_governance_audit_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                rank_history_governance_audit_error = str(exc)
                log(f"rank 历史热冷治理只读审计失败: {exc}")
        else:
            skip_reason = (
                "existing_archive_binding_active"
                if rank_rolling_active_binding_available
                else (
                    "disabled_by_config"
                    if not weekly_rank_history_governance_audit_enabled
                    else "runtime_context_unavailable"
                )
            )
            rank_history_governance_audit_summary = {
                "status": "skipped",
                "skip_reason": skip_reason,
            }
            rank_history_archive_preflight_summary = {
                "status": "skipped",
                "skip_reason": skip_reason
                if rank_rolling_active_binding_available
                else "rank_governance_not_run",
            }
            if rank_history_archive_apply_summary is None:
                rank_history_archive_apply_summary = {
                    "status": "skipped",
                    "skip_reason": skip_reason
                    if rank_rolling_active_binding_available
                    else "rank_governance_not_run",
                }
        weekly_rank_history_archive_vacuum_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_rank_history_archive_vacuum_enabled",
                False,
            )
        )
        rank_history_archive_vacuum_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if weekly_rank_history_archive_vacuum_enabled and rank_history_archive_vacuum_runtime_ready:
            try:
                vacuum_output_dir = self.settings.reports_dir / "audit" / "rank_history_archive"
                rank_vacuum_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_rank_history_archive_vacuum_mode",
                            "auto",
                        )
                        or "auto"
                    ).strip()
                    or "auto"
                )
                rank_vacuum_confirm_replace = bool(
                    getattr(
                        self.settings,
                        "scheduler_weekly_rank_history_archive_vacuum_confirm_replace",
                        True,
                    )
                )
                rank_vacuum_min_free_space_multiplier = float(
                    getattr(
                        self.settings,
                        "scheduler_weekly_rank_history_archive_vacuum_min_free_space_multiplier",
                        1.25,
                    )
                    or 1.25
                )
                vacuum_payload = build_rank_history_archive_vacuum_runner_payload(
                    db_path=str(self.settings.database_path),
                    project_root=str(
                        getattr(self.settings, "project_root", None) or self._project_root()
                    ),
                    output_dir=str(vacuum_output_dir),
                    mode=rank_vacuum_mode,
                    min_free_space_multiplier=rank_vacuum_min_free_space_multiplier,
                    confirm_replace=rank_vacuum_confirm_replace,
                )
                vacuum_json_path, vacuum_md_path = write_rank_history_archive_vacuum_runner_artifacts(
                    vacuum_payload,
                    output_dir=str(vacuum_output_dir),
                )
                vacuum_summary = dict(vacuum_payload.get("summary") or {})
                vacuum_blockers = list(vacuum_payload.get("blockers") or [])
                rank_history_archive_vacuum_summary = {
                    "status": "success",
                    "artifact_path": str(vacuum_json_path),
                    "markdown_path": str(vacuum_md_path),
                    "summary": vacuum_summary,
                    "findings": vacuum_payload.get("findings") or [],
                    "blockers": vacuum_blockers,
                    "review_items": vacuum_payload.get("review_items") or [],
                }
                runner_status = str(vacuum_summary.get("runner_status") or "").strip()
                if runner_status == "blocked":
                    if bool(vacuum_summary.get("archive_binding_active")):
                        rank_history_archive_vacuum_error = "rank_history_archive_vacuum_blocked"
                    else:
                        rank_history_archive_vacuum_summary["status"] = "skipped"
                        rank_history_archive_vacuum_summary[
                            "skip_reason"
                        ] = "rank_archive_binding_not_active"
                if (
                    rank_history_archive_vacuum_error is None
                    and str(vacuum_summary.get("vacuum_result_status") or "") == "failed"
                ):
                    rank_history_archive_vacuum_error = "rank_history_archive_vacuum_into_failed"
                if (
                    rank_history_archive_vacuum_error is None
                    and str(vacuum_summary.get("replace_result_status") or "") == "failed"
                ):
                    rank_history_archive_vacuum_error = "rank_history_archive_vacuum_replace_failed"
                if vacuum_summary.get("vacuum_executed") or vacuum_summary.get("replace_executed"):
                    log(
                        "rank 历史热冷治理压缩阶段完成，"
                        f"runner={runner_status or 'unknown'}，"
                        f"vacuum={bool(vacuum_summary.get('vacuum_executed'))}，"
                        f"replace={bool(vacuum_summary.get('replace_executed'))}，"
                        f"回收估算 {float(vacuum_summary.get('estimated_reclaim_mb') or 0):.1f} MB"
                    )
            except sqlite3.Error:
                rank_history_archive_vacuum_summary = {
                    "status": "skipped",
                    "skip_reason": "runtime_context_unavailable",
                }
            except Exception as exc:
                rank_history_archive_vacuum_error = str(exc)
                rank_history_archive_vacuum_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"rank 历史热冷治理压缩阶段失败: {exc}")
        else:
            rank_history_archive_vacuum_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_rank_history_archive_vacuum_enabled
                    else "runtime_context_unavailable"
                ),
            }
        incremental_vacuum_summary: dict[str, Any] = {"status": "skipped_disabled"}
        incremental_vacuum_database_path = getattr(self.settings, "database_path", None)
        if bool(
            getattr(self.settings, "scheduler_weekly_incremental_vacuum_enabled", True)
        ) and incremental_vacuum_database_path and Path(str(incremental_vacuum_database_path)).exists():
            try:
                incremental_vacuum_summary = run_incremental_space_reclaim(
                    Path(str(incremental_vacuum_database_path)),
                    max_pages=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_incremental_vacuum_max_pages",
                            50_000,
                        )
                    ),
                    min_freelist_pages=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_incremental_vacuum_min_freelist_pages",
                            1_000,
                        )
                    ),
                    max_seconds=float(
                        getattr(
                            self.settings,
                            "scheduler_weekly_incremental_vacuum_max_seconds",
                            180,
                        )
                        or 180
                    ),
                )
                log(
                    "数据库增量空间回收完成，"
                    f"status={incremental_vacuum_summary.get('status')}，"
                    f"reclaimed_pages={incremental_vacuum_summary.get('reclaimed_pages', 0)}，"
                    f"freelist_after={incremental_vacuum_summary.get('freelist_pages_after', incremental_vacuum_summary.get('freelist_pages_before', 0))}"
                )
            except sqlite3.Error as exc:
                incremental_vacuum_summary = {"status": "failed", "error": str(exc)}
                incremental_vacuum_error = str(exc)
        weekly_long_term_history_enabled = bool(
            getattr(self.settings, "scheduler_weekly_long_term_history_enabled", True)
        )
        long_term_history_limit = int(
            getattr(self.settings, "scheduler_weekly_long_term_history_limit", 0) or 0
        )
        weekly_long_term_history_refresh_stale = bool(
            getattr(self.settings, "scheduler_weekly_long_term_history_refresh_stale", True)
        )
        if weekly_long_term_history_enabled:
            try:
                long_term_history_maintenance_summary = run_long_term_history_maintenance(
                    self.settings,
                    log=log,
                    limit=(long_term_history_limit if long_term_history_limit > 0 else None),
                    refresh_stale=weekly_long_term_history_refresh_stale,
                )
            except Exception as exc:
                long_term_history_maintenance_error = str(exc)
                log(f"债基长期稳定性历史维护失败: {exc}")
        else:
            long_term_history_maintenance_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "selection": {"limit": long_term_history_limit},
            }
        weekly_regression_smoke_enabled = bool(
            getattr(self.settings, "scheduler_weekly_regression_smoke_enabled", False)
        )
        weekly_regression_smoke_groups = list(
            getattr(
                self.settings,
                "scheduler_weekly_regression_smoke_groups",
                ("scheduler_guard", "dashboard_payload", "publication_integrity"),
            )
            or []
        )
        if weekly_regression_smoke_enabled:
            try:
                regression_smoke_summary = self._run_regression_smoke(
                    groups=weekly_regression_smoke_groups,
                    log=log,
                )
            except Exception as exc:
                regression_smoke_error = str(exc)
                log(f"回归 smoke 自检失败: {exc}")
        else:
            regression_smoke_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
                "groups": weekly_regression_smoke_groups,
            }
        holding_period_after = self._read_holding_period_snapshot(self.settings)
        holding_period_change_summary = self._build_holding_period_change_summary(
            before_snapshot=holding_period_before,
            after_snapshot=holding_period_after,
        )
        maintenance_data_change_summary = self._build_maintenance_data_change_summary(
            nav_history_summary=nav_history_maintenance_summary,
            plain_open_tail_follow_up_summary=plain_open_tail_follow_up_summary,
            long_term_history_summary=long_term_history_maintenance_summary,
            holding_period_change_summary=holding_period_change_summary,
        )
        if maintenance_data_change_summary.get("changed") and collection_rescreen_block_reason:
            maintenance_rescreen_skipped_reason = collection_rescreen_block_reason
            report_refresh_skipped_reason = maintenance_rescreen_skipped_reason
            log(f"维护数据已更新，采集尚未完整完成({collection_rescreen_block_reason})；本轮重筛暂缓，保留上一有效发布")
        elif maintenance_data_change_summary.get("changed"):
            try:
                refresh_report = self._rescreen_report_after_maintenance(
                    collection_profile="weekly",
                    risk_refresh_target_fcodes=self._extract_maintenance_risk_refresh_target_fcodes(
                        nav_history_summary=nav_history_maintenance_summary,
                    ),
                    log=log,
                )
                if str(refresh_report.get("status") or "").strip() == "skipped":
                    maintenance_rescreen_skipped_reason = str(
                        refresh_report.get("skip_reason") or "skipped"
                    ).strip()
                else:
                    maintenance_rescreen_triggered = True
                    maintenance_rescreen_run_id = str(refresh_report.get("run_id") or "").strip()
                    report_refresh_triggered = True
                    report_refresh_run_id = maintenance_rescreen_run_id
            except RuntimeError as exc:
                maintenance_rescreen_error = str(exc)
                maintenance_rescreen_skipped_reason = self._workflow_busy_skip_reason(after_collection=False)
                report_refresh_error = str(exc)
                report_refresh_skipped_reason = maintenance_rescreen_skipped_reason
                log(f"基于维护后样本重评分跳过: {exc}")
            except Exception as exc:
                maintenance_rescreen_error = str(exc)
                report_refresh_error = str(exc)
                log(f"基于维护后样本重评分失败: {exc}")
        audits = self._build_data_quality_audits_compat()

        weekly_database_backup_cleanup_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_backup_cleanup_preview_enabled",
                True,
            )
        )
        database_backup_cleanup_preview_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(getattr(self.settings, "reports_dir", None))
        backup_preview_payload: dict[str, Any] | None = None
        backup_preview_json_path: Path | None = None
        backup_preview_dir: Path | None = None
        if (
            weekly_database_backup_cleanup_preview_enabled
            and database_backup_cleanup_preview_runtime_ready
        ):
            try:
                backup_preview_payload = build_database_backup_cleanup_preview_payload(
                    backup_dir=Path(self.settings.database_path).parent / "backups",
                    active_database_path=self.settings.database_path,
                    scheduled_keep_count=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_cleanup_preview_scheduled_keep_count",
                            6,
                        )
                        or 6
                    ),
                    unmanaged_retention_days=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_cleanup_preview_unmanaged_retention_days",
                            14,
                        )
                        or 14
                    ),
                    keep_latest_manual_snapshot_per_family=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_cleanup_preview_keep_latest_manual_snapshot_per_family",
                            1,
                        )
                        or 1
                    ),
                    max_sample_files=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_cleanup_preview_max_sample_files",
                            20,
                        )
                        or 20
                    ),
                )
                backup_preview_dir = (
                    self.settings.reports_dir / "audit" / "database_backup_cleanup"
                )
                backup_preview_json_path, backup_preview_md_path = (
                    write_database_backup_cleanup_preview_artifacts(
                        backup_preview_payload,
                        output_dir=str(backup_preview_dir),
                    )
                )
                backup_preview_summary = backup_preview_payload.get("summary") or {}
                database_backup_cleanup_preview_summary = {
                    "status": "success",
                    "artifact_path": str(backup_preview_json_path),
                    "markdown_path": str(backup_preview_md_path),
                    "summary": backup_preview_summary,
                    "findings": backup_preview_payload.get("findings") or [],
                    "execution_plan": backup_preview_payload.get("execution_plan") or {},
                }
                auto_candidate_count = int(
                    backup_preview_summary.get("auto_candidate_file_count") or 0
                )
                manual_review_count = int(
                    backup_preview_summary.get("manual_review_candidate_file_count") or 0
                )
                if auto_candidate_count or manual_review_count:
                    log(
                        "数据库备份 cleanup dry-run 完成，"
                        f"自动候选 {auto_candidate_count} 个，"
                        f"人工复核 {manual_review_count} 个"
                    )
            except Exception as exc:
                database_backup_cleanup_preview_error = str(exc)
                database_backup_cleanup_preview_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"数据库备份 cleanup dry-run 预演失败: {exc}")
        else:
            database_backup_cleanup_preview_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_database_backup_cleanup_preview_enabled
                    else "runtime_context_unavailable"
                ),
            }
        weekly_database_backup_cleanup_execution_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_backup_cleanup_execution_enabled",
                False,
            )
        )
        if not weekly_database_backup_cleanup_execution_enabled:
            database_backup_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_database_backup_cleanup_preview_enabled:
            database_backup_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_disabled",
            }
        elif database_backup_cleanup_preview_error:
            database_backup_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_failed",
            }
        elif (
            not database_backup_cleanup_preview_runtime_ready
            or backup_preview_payload is None
            or backup_preview_json_path is None
            or backup_preview_dir is None
        ):
            database_backup_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_not_ready",
            }
        else:
            execution_plan = dict(backup_preview_payload.get("execution_plan") or {})
            auto_scope_files = []
            seen_scope_files = set()
            for item in execution_plan.get("scope_files") or []:
                value = str(item or "").strip()
                if not value or value in seen_scope_files:
                    continue
                seen_scope_files.add(value)
                auto_scope_files.append(value)
            manual_review_files = [
                str(item or "").strip()
                for item in (execution_plan.get("manual_review_files") or [])
                if str(item or "").strip()
            ]
            if not auto_scope_files:
                database_backup_cleanup_execution_summary = {
                    "status": "skipped",
                    "skip_reason": "no_auto_scope",
                    "summary": {
                        "scope_file_total": 0,
                        "auto_scope_file_total": 0,
                        "manual_review_file_total": len(manual_review_files),
                    },
                    "execution_plan": {
                        "scope_file_total": 0,
                        "manual_review_file_total": len(manual_review_files),
                    },
                }
            else:
                execution_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_cleanup_execution_mode",
                            "apply",
                        )
                        or "apply"
                    )
                    .strip()
                    .lower()
                    .replace("_", "-")
                )
                try:
                    checkpoint_every = max(
                        1,
                        int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_database_backup_cleanup_execution_checkpoint_every",
                                50,
                            )
                            or 50
                        ),
                    )
                    (
                        backup_execution_json_path,
                        backup_execution_md_path,
                        backup_execution_payload,
                    ) = execute_database_backup_cleanup(
                        preview_payload=backup_preview_payload,
                        source_preview_artifact=str(backup_preview_json_path),
                        output_dir=str(backup_preview_dir),
                        mode=execution_mode,
                        confirm_apply=(
                            DATABASE_BACKUP_CLEANUP_APPLY_CONFIRMATION
                            if execution_mode == "apply"
                            else None
                        ),
                        expected_backup_dir=str(Path(self.settings.database_path).parent / "backups"),
                        checkpoint_every=checkpoint_every,
                    )
                    backup_execution_summary = dict(
                        backup_execution_payload.get("summary") or {}
                    )
                    database_backup_cleanup_execution_summary = {
                        "status": backup_execution_payload.get("status") or "success",
                        "artifact_path": str(backup_execution_json_path),
                        "markdown_path": str(backup_execution_md_path),
                        "mode": execution_mode,
                        "summary": backup_execution_summary,
                        "findings": backup_execution_payload.get("findings") or [],
                        "guardrails": backup_execution_payload.get("guardrails") or [],
                    }
                    if str(backup_execution_payload.get("status") or "") == "failed":
                        database_backup_cleanup_execution_error = (
                            "database_backup_cleanup_execution_failed"
                        )
                    deleted_file_count = int(
                        backup_execution_summary.get("deleted_file_count") or 0
                    )
                    processed_file_count = int(
                        backup_execution_summary.get("processed_file_count") or 0
                    )
                    if execution_mode == "apply":
                        log(
                            "数据库备份 cleanup 执行完成，"
                            f"scope {len(auto_scope_files)} 个，"
                            f"处理 {processed_file_count} 个，删除 {deleted_file_count} 个"
                        )
                    else:
                        log(
                            "数据库备份 cleanup 执行 dry-run 完成，"
                            f"scope {len(auto_scope_files)} 个"
                        )
                except Exception as exc:
                    database_backup_cleanup_execution_error = str(exc)
                    database_backup_cleanup_execution_summary = {
                        "status": "failed",
                        "error": str(exc),
                    }
                    log(f"数据库备份 cleanup 执行失败: {exc}")

        weekly_database_backup_compression_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_backup_compression_preview_enabled",
                True,
            )
        )
        database_backup_compression_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(getattr(self.settings, "reports_dir", None))
        backup_compression_preview_payload: dict[str, Any] | None = None
        backup_compression_preview_json_path: Path | None = None
        backup_compression_preview_dir: Path | None = None
        if (
            weekly_database_backup_compression_preview_enabled
            and database_backup_compression_runtime_ready
        ):
            try:
                min_compress_size_mb = float(
                    getattr(
                        self.settings,
                        "scheduler_weekly_database_backup_compression_preview_min_compress_size_mb",
                        128.0,
                    )
                )
                backup_compression_preview_payload = (
                    build_database_backup_compression_preview_payload(
                        backup_dir=Path(self.settings.database_path).parent / "backups",
                        active_database_path=self.settings.database_path,
                        min_compress_size_mb=min_compress_size_mb,
                        max_sample_files=int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_database_backup_compression_preview_max_sample_files",
                                20,
                            )
                            or 20
                        ),
                    )
                )
                backup_compression_preview_dir = (
                    self.settings.reports_dir / "audit" / "database_backup_compression"
                )
                (
                    backup_compression_preview_json_path,
                    backup_compression_preview_md_path,
                ) = write_database_backup_compression_preview_artifacts(
                    backup_compression_preview_payload,
                    output_dir=str(backup_compression_preview_dir),
                )
                backup_compression_preview_summary = (
                    backup_compression_preview_payload.get("summary") or {}
                )
                database_backup_compression_preview_summary = {
                    "status": "success",
                    "artifact_path": str(backup_compression_preview_json_path),
                    "markdown_path": str(backup_compression_preview_md_path),
                    "summary": backup_compression_preview_summary,
                    "findings": backup_compression_preview_payload.get("findings") or [],
                    "execution_plan": (
                        backup_compression_preview_payload.get("execution_plan") or {}
                    ),
                }
                compression_candidate_count = int(
                    backup_compression_preview_summary.get("candidate_file_count") or 0
                )
                compression_candidate_size_mb = float(
                    backup_compression_preview_summary.get("candidate_size_mb") or 0.0
                )
                if compression_candidate_count:
                    log(
                        "数据库备份压缩 dry-run 完成，"
                        f"候选 {compression_candidate_count} 个，"
                        f"源文件约 {compression_candidate_size_mb:.1f} MB"
                    )
            except Exception as exc:
                database_backup_compression_preview_error = str(exc)
                database_backup_compression_preview_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"数据库备份压缩 dry-run 预演失败: {exc}")
        else:
            database_backup_compression_preview_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_database_backup_compression_preview_enabled
                    else "runtime_context_unavailable"
                ),
            }

        weekly_database_backup_compression_execution_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_backup_compression_execution_enabled",
                True,
            )
        )
        if not weekly_database_backup_compression_execution_enabled:
            database_backup_compression_execution_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }
        elif not weekly_database_backup_compression_preview_enabled:
            database_backup_compression_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_disabled",
            }
        elif database_backup_compression_preview_error:
            database_backup_compression_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_failed",
            }
        elif (
            not database_backup_compression_runtime_ready
            or backup_compression_preview_payload is None
            or backup_compression_preview_json_path is None
            or backup_compression_preview_dir is None
        ):
            database_backup_compression_execution_summary = {
                "status": "skipped",
                "skip_reason": "preview_not_ready",
            }
        else:
            compression_execution_plan = dict(
                backup_compression_preview_payload.get("execution_plan") or {}
            )
            compression_scope_files = []
            seen_compression_scope_files = set()
            for item in compression_execution_plan.get("scope_files") or []:
                value = str(item or "").strip()
                if not value or value in seen_compression_scope_files:
                    continue
                seen_compression_scope_files.add(value)
                compression_scope_files.append(value)
            if not compression_scope_files:
                database_backup_compression_execution_summary = {
                    "status": "skipped",
                    "skip_reason": "no_auto_scope",
                    "summary": {
                        "scope_file_total": 0,
                        "compressed_file_count": 0,
                        "source_removed_file_count": 0,
                        "freed_size_bytes": 0,
                        "freed_size_mb": 0.0,
                    },
                    "execution_plan": {"scope_file_total": 0},
                }
            else:
                compression_execution_mode = (
                    str(
                        getattr(
                            self.settings,
                            "scheduler_weekly_database_backup_compression_execution_mode",
                            "apply",
                        )
                        or "apply"
                    )
                    .strip()
                    .lower()
                    .replace("_", "-")
                )
                try:
                    compression_checkpoint_every = max(
                        1,
                        int(
                            getattr(
                                self.settings,
                                "scheduler_weekly_database_backup_compression_execution_checkpoint_every",
                                1,
                            )
                            or 1
                        ),
                    )
                    (
                        backup_compression_execution_json_path,
                        backup_compression_execution_md_path,
                        backup_compression_execution_payload,
                    ) = execute_database_backup_compression(
                        preview_payload=backup_compression_preview_payload,
                        source_preview_artifact=str(backup_compression_preview_json_path),
                        output_dir=str(backup_compression_preview_dir),
                        mode=compression_execution_mode,
                        confirm_apply=(
                            DATABASE_BACKUP_COMPRESSION_APPLY_CONFIRMATION
                            if compression_execution_mode == "apply"
                            else None
                        ),
                        expected_backup_dir=str(Path(self.settings.database_path).parent / "backups"),
                        checkpoint_every=compression_checkpoint_every,
                    )
                    backup_compression_execution_summary = dict(
                        backup_compression_execution_payload.get("summary") or {}
                    )
                    database_backup_compression_execution_summary = {
                        "status": backup_compression_execution_payload.get("status")
                        or "success",
                        "artifact_path": str(backup_compression_execution_json_path),
                        "markdown_path": str(backup_compression_execution_md_path),
                        "mode": compression_execution_mode,
                        "summary": backup_compression_execution_summary,
                        "findings": (
                            backup_compression_execution_payload.get("findings") or []
                        ),
                        "guardrails": (
                            backup_compression_execution_payload.get("guardrails") or []
                        ),
                    }
                    if (
                        str(backup_compression_execution_payload.get("status") or "")
                        == "failed"
                    ):
                        database_backup_compression_execution_error = (
                            "database_backup_compression_execution_failed"
                        )
                    compressed_file_count = int(
                        backup_compression_execution_summary.get("compressed_file_count") or 0
                    )
                    source_removed_file_count = int(
                        backup_compression_execution_summary.get("source_removed_file_count")
                        or 0
                    )
                    freed_size_mb = float(
                        backup_compression_execution_summary.get("freed_size_mb") or 0.0
                    )
                    if compression_execution_mode == "apply":
                        log(
                            "数据库备份压缩执行完成，"
                            f"scope {len(compression_scope_files)} 个，"
                            f"压缩 {compressed_file_count} 个，"
                            f"移除源文件 {source_removed_file_count} 个，"
                            f"释放约 {freed_size_mb:.1f} MB"
                        )
                    else:
                        log(
                            "数据库备份压缩执行 dry-run 完成，"
                            f"scope {len(compression_scope_files)} 个"
                        )
                except Exception as exc:
                    database_backup_compression_execution_error = str(exc)
                    database_backup_compression_execution_summary = {
                        "status": "failed",
                        "error": str(exc),
                    }
                    log(f"数据库备份压缩执行失败: {exc}")

        weekly_task_history_governance_preview_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_task_history_governance_preview_enabled",
                False,
            )
        )
        task_history_governance_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(getattr(self.settings, "reports_dir", None))
        if (
            weekly_task_history_governance_preview_enabled
            and task_history_governance_runtime_ready
        ):
            try:
                task_history_preview_payload = build_task_history_governance_preview_payload(
                    db_path=str(self.settings.database_path),
                    retention_days=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_task_history_governance_preview_retention_days",
                            14,
                        )
                        or 14
                    ),
                    recent_keep_per_task_status=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_task_history_governance_preview_recent_keep_per_task_status",
                            20,
                        )
                        or 20
                    ),
                    large_extra_data_threshold_bytes=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_task_history_governance_preview_large_extra_data_threshold_bytes",
                            262_144,
                        )
                        or 262_144
                    ),
                    max_sample_rows=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_task_history_governance_preview_max_sample_rows",
                            20,
                        )
                        or 20
                    ),
                    max_daily_summary_days=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_task_history_governance_preview_max_daily_summary_days",
                            30,
                        )
                        or 30
                    ),
                )
                task_history_governance_dir = (
                    self.settings.reports_dir / "audit" / "task_history_governance"
                )
                (
                    task_history_preview_json_path,
                    task_history_preview_md_path,
                ) = write_task_history_governance_preview_artifacts(
                    task_history_preview_payload,
                    output_dir=str(task_history_governance_dir),
                )
                task_history_preview_summary = dict(
                    task_history_preview_payload.get("summary") or {}
                )
                task_history_governance_preview_summary = {
                    "status": task_history_preview_payload.get("status") or "success",
                    "artifact_path": str(task_history_preview_json_path),
                    "markdown_path": str(task_history_preview_md_path),
                    "summary": task_history_preview_summary,
                    "findings": task_history_preview_payload.get("findings") or [],
                }
                retention_candidate_total = int(
                    task_history_preview_summary.get("retention_candidate_total") or 0
                )
                large_extra_data_candidate_total = int(
                    task_history_preview_summary.get("large_extra_data_candidate_total")
                    or 0
                )
                snapshot_watchdog_skipped_total = int(
                    task_history_preview_summary.get("snapshot_watchdog_skipped_total")
                    or 0
                )
                if (
                    retention_candidate_total
                    or large_extra_data_candidate_total
                    or snapshot_watchdog_skipped_total
                ):
                    log(
                        "task_history 治理 preview 完成，"
                        f"retention 候选 {retention_candidate_total} 行，"
                        f"大 extra_data 候选 {large_extra_data_candidate_total} 行，"
                        f"watchdog skipped {snapshot_watchdog_skipped_total} 行"
                    )
            except Exception as exc:
                task_history_governance_preview_error = str(exc)
                task_history_governance_preview_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"task_history 治理 preview 失败: {exc}")
        else:
            task_history_governance_preview_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_task_history_governance_preview_enabled
                    else "runtime_context_unavailable"
                ),
            }

        weekly_task_history_cleanup_execution_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_task_history_cleanup_execution_enabled",
                False,
            )
        )
        task_history_cleanup_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(getattr(self.settings, "reports_dir", None))
        if (
            weekly_task_history_cleanup_execution_enabled
            and task_history_cleanup_runtime_ready
        ):
            try:
                task_history_cleanup_dir = (
                    self.settings.reports_dir / "audit" / "task_history_cleanup"
                )
                task_history_archive_dir = (
                    self.settings.reports_dir / ".audit" / "task_history_cleanup"
                )
                cleanup_retention_days = int(
                    getattr(
                        self.settings,
                        "scheduler_weekly_task_history_cleanup_execution_retention_days",
                        14,
                    )
                    or 14
                )
                cleanup_recent_keep = int(
                    getattr(
                        self.settings,
                        "scheduler_weekly_task_history_cleanup_execution_recent_keep_per_task_status",
                        20,
                    )
                    or 20
                )
                cleanup_max_sample_rows = int(
                    getattr(
                        self.settings,
                        "scheduler_weekly_task_history_cleanup_execution_max_sample_rows",
                        20,
                    )
                    or 20
                )
                (
                    task_history_cleanup_dry_json_path,
                    task_history_cleanup_dry_md_path,
                    task_history_cleanup_dry_payload,
                ) = execute_task_history_cleanup(
                    db_path=str(self.settings.database_path),
                    output_dir=str(task_history_cleanup_dir),
                    archive_dir=str(task_history_archive_dir),
                    mode="dry-run",
                    retention_days=cleanup_retention_days,
                    recent_keep_per_task_status=cleanup_recent_keep,
                    max_sample_rows=cleanup_max_sample_rows,
                )
                task_history_cleanup_dry_summary = dict(
                    task_history_cleanup_dry_payload.get("summary") or {}
                )
                task_history_cleanup_candidate_total = int(
                    task_history_cleanup_dry_summary.get("candidate_total") or 0
                )
                task_history_cleanup_min_apply_candidate_total = int(
                    getattr(
                        self.settings,
                        "scheduler_weekly_task_history_cleanup_execution_min_apply_candidate_total",
                        100,
                    )
                    or 100
                )
                task_history_cleanup_auto_apply_enabled = bool(
                    getattr(
                        self.settings,
                        "scheduler_weekly_task_history_cleanup_execution_auto_apply_enabled",
                        False,
                    )
                )
                task_history_cleanup_execution_summary = {
                    "status": "dry-run",
                    "mode": "dry-run",
                    "artifact_path": str(task_history_cleanup_dry_json_path),
                    "markdown_path": str(task_history_cleanup_dry_md_path),
                    "summary": task_history_cleanup_dry_summary,
                    "findings": task_history_cleanup_dry_payload.get("findings") or [],
                    "auto_apply_enabled": task_history_cleanup_auto_apply_enabled,
                    "min_apply_candidate_total": task_history_cleanup_min_apply_candidate_total,
                    "apply_attempted": False,
                    "apply_skipped_reason": "",
                }
                if (
                    task_history_cleanup_auto_apply_enabled
                    and task_history_cleanup_candidate_total
                    >= task_history_cleanup_min_apply_candidate_total
                ):
                    (
                        task_history_cleanup_apply_json_path,
                        task_history_cleanup_apply_md_path,
                        task_history_cleanup_apply_payload,
                    ) = execute_task_history_cleanup(
                        db_path=str(self.settings.database_path),
                        source_plan_artifact=str(task_history_cleanup_dry_json_path),
                        output_dir=str(task_history_cleanup_dir),
                        archive_dir=str(task_history_archive_dir),
                        mode="apply",
                        confirm_apply=TASK_HISTORY_CLEANUP_APPLY_CONFIRMATION,
                        retention_days=cleanup_retention_days,
                        recent_keep_per_task_status=cleanup_recent_keep,
                        max_sample_rows=cleanup_max_sample_rows,
                        compress_backup=bool(
                            getattr(
                                self.settings,
                                "scheduler_weekly_task_history_cleanup_execution_compress_backup",
                                True,
                            )
                        ),
                    )
                    task_history_cleanup_apply_summary = dict(
                        task_history_cleanup_apply_payload.get("summary") or {}
                    )
                    task_history_cleanup_execution_summary = {
                        "status": task_history_cleanup_apply_payload.get("status")
                        or "success",
                        "mode": "apply",
                        "artifact_path": str(task_history_cleanup_apply_json_path),
                        "markdown_path": str(task_history_cleanup_apply_md_path),
                        "dry_run_artifact_path": str(task_history_cleanup_dry_json_path),
                        "dry_run_markdown_path": str(task_history_cleanup_dry_md_path),
                        "dry_run_summary": task_history_cleanup_dry_summary,
                        "summary": task_history_cleanup_apply_summary,
                        "backup": task_history_cleanup_apply_payload.get("backup") or {},
                        "archive": task_history_cleanup_apply_payload.get("archive") or {},
                        "post_apply_consistency": task_history_cleanup_apply_payload.get("post_apply_consistency")
                        or {},
                        "guard_errors": task_history_cleanup_apply_payload.get("guard_errors")
                        or [],
                        "findings": task_history_cleanup_apply_payload.get("findings")
                        or [],
                        "auto_apply_enabled": task_history_cleanup_auto_apply_enabled,
                        "min_apply_candidate_total": task_history_cleanup_min_apply_candidate_total,
                        "apply_attempted": True,
                        "apply_skipped_reason": "",
                    }
                    log(
                        "task_history cleanup apply 完成，"
                        f"deleted={task_history_cleanup_apply_summary.get('deleted_count') or 0}，"
                        f"candidate={task_history_cleanup_candidate_total}"
                    )
                else:
                    skipped_reason = (
                        "auto_apply_disabled"
                        if not task_history_cleanup_auto_apply_enabled
                        else "below_min_apply_candidate_total"
                    )
                    task_history_cleanup_execution_summary[
                        "apply_skipped_reason"
                    ] = skipped_reason
                    log(
                        "task_history cleanup dry-run 完成，"
                        f"candidate={task_history_cleanup_candidate_total}，"
                        f"apply_skipped_reason={skipped_reason}"
                    )
            except Exception as exc:
                task_history_cleanup_execution_error = str(exc)
                task_history_cleanup_execution_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"task_history cleanup 执行失败: {exc}")
        else:
            task_history_cleanup_execution_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_task_history_cleanup_execution_enabled
                    else "runtime_context_unavailable"
                ),
            }

        # Evaluate superseded maintenance points after BOTH cleanup producers
        # have finished, so this week's task backup participates in protection.
        maintenance_backup_retention_error = None
        maintenance_backup_retention_summary = {"status": "skipped", "skip_reason": "disabled_by_config"}
        if bool(getattr(self.settings, "scheduler_weekly_maintenance_backup_retention_enabled", False)):
            try:
                from backend.maintenance_backup_retention import (
                    run_maintenance_backup_retention, summarize_maintenance_backup_retention,
                )

                maintenance_backup_retention_summary = summarize_maintenance_backup_retention(
                    run_maintenance_backup_retention(self.settings, mode="apply", log=log))
                if maintenance_backup_retention_summary["status"] in {"failed", "degraded"}:
                    maintenance_backup_retention_error = "maintenance_backup_recovery_verification_failed"
            except Exception as exc:
                maintenance_backup_retention_error = str(exc)
                maintenance_backup_retention_summary = {"status": "failed", "error": str(exc)}
                log(f"维护备份自动保留失败: {exc}")

        weekly_screening_storage_growth_audit_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_screening_storage_growth_audit_enabled",
                False,
            )
        )
        screening_storage_growth_runtime_ready = bool(
            getattr(self.settings, "database_path", None)
        ) and bool(
            getattr(self.settings, "reports_dir", None)
        ) and Path(str(getattr(self.settings, "database_path", ""))).exists()
        if (
            weekly_screening_storage_growth_audit_enabled
            and screening_storage_growth_runtime_ready
        ):
            try:
                configured_max_runs = int(
                    getattr(
                        self.settings,
                        "scheduler_weekly_screening_storage_growth_audit_max_runs",
                        0,
                    )
                    or 0
                )
                screening_storage_growth_payload = build_screening_storage_growth_audit_payload(
                    db_path=str(self.settings.database_path),
                    days=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_storage_growth_audit_days",
                            30,
                        )
                        or 30
                    ),
                    fund_types=list(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_storage_growth_audit_fund_types",
                            ("bond",),
                        )
                        or ("bond",)
                    ),
                    top_runs=int(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_storage_growth_audit_top_runs",
                            20,
                        )
                        or 20
                    ),
                    max_runs=(configured_max_runs if configured_max_runs > 0 else None),
                    include_table_totals=bool(
                        getattr(
                            self.settings,
                            "scheduler_weekly_screening_storage_growth_audit_include_table_totals",
                            True,
                        )
                    ),
                )
                screening_storage_growth_dir = (
                    self.settings.reports_dir / "audit" / "screening_storage_growth"
                )
                (
                    screening_storage_growth_json_path,
                    screening_storage_growth_md_path,
                ) = write_screening_storage_growth_artifacts(
                    screening_storage_growth_payload,
                    output_dir=str(screening_storage_growth_dir),
                )
                screening_storage_growth_summary = dict(
                    screening_storage_growth_payload.get("summary") or {}
                )
                screening_storage_growth_audit_summary = {
                    "status": screening_storage_growth_payload.get("status") or "success",
                    "artifact_path": str(screening_storage_growth_json_path),
                    "markdown_path": str(screening_storage_growth_md_path),
                    "summary": screening_storage_growth_summary,
                    "findings": screening_storage_growth_payload.get("findings") or [],
                }
                count_source_breakdown = dict(
                    screening_storage_growth_summary.get("count_source_breakdown") or {}
                )
                materialized_coverage = dict(
                    screening_storage_growth_summary.get("materialized_coverage") or {}
                )
                storage_stats_coverage = dict(
                    materialized_coverage.get("screening_run_storage_stats") or {}
                )
                detail_fallback_run_count = sum(
                    int((bucket or {}).get("run_count") or 0)
                    for source_name, bucket in count_source_breakdown.items()
                    if str(source_name).startswith("detail_scan")
                )
                log(
                    "screening 存储增长审计完成，"
                    f"run_count={screening_storage_growth_summary.get('run_count') or 0}，"
                    f"storage_stats_missing={storage_stats_coverage.get('missing_run_count') or 0}，"
                    f"detail_fallback_runs={detail_fallback_run_count}"
                )
            except Exception as exc:
                screening_storage_growth_audit_error = str(exc)
                screening_storage_growth_audit_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"screening 存储增长审计失败: {exc}")
        else:
            screening_storage_growth_audit_summary = {
                "status": "skipped",
                "skip_reason": (
                    "disabled_by_config"
                    if not weekly_screening_storage_growth_audit_enabled
                    else "runtime_context_unavailable"
                ),
            }

        weekly_database_governance_health_audit_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_governance_health_audit_enabled",
                False,
            )
        )
        if weekly_database_governance_health_audit_enabled:
            try:
                database_governance_payload = build_database_governance_health_audit_payload(
                    nav_history_archive_preview=nav_history_archive_preview_summary,
                    nav_history_archive_vacuum=nav_history_archive_vacuum_summary,
                    rank_history_archive_rolling=rank_history_archive_rolling_summary,
                    rank_history_archive_apply=rank_history_archive_apply_summary,
                    rank_history_archive_vacuum=rank_history_archive_vacuum_summary,
                    screening_history_archive_preview=screening_history_archive_preview_summary,
                    screening_history_archive_execution=screening_history_archive_execution_summary,
                    screening_history_cleanup_execution=screening_history_cleanup_execution_summary,
                    database_backup_cleanup_preview=database_backup_cleanup_preview_summary,
                    database_backup_cleanup_execution=database_backup_cleanup_execution_summary,
                    database_backup_compression_preview=database_backup_compression_preview_summary,
                    database_backup_compression_execution=database_backup_compression_execution_summary,
                    task_history_governance_preview=task_history_governance_preview_summary,
                    task_history_cleanup_execution=task_history_cleanup_execution_summary,
                    screening_storage_growth_audit=screening_storage_growth_audit_summary,
                    nav_history_archive_preview_error=nav_history_archive_preview_error,
                    nav_history_archive_vacuum_error=nav_history_archive_vacuum_error,
                    rank_history_archive_rolling_error=rank_history_archive_rolling_error,
                    rank_history_archive_apply_error=rank_history_archive_apply_error,
                    rank_history_archive_vacuum_error=rank_history_archive_vacuum_error,
                    screening_history_archive_preview_error=screening_history_archive_preview_error,
                    screening_history_archive_execution_error=screening_history_archive_execution_error,
                    screening_history_cleanup_execution_error=screening_history_cleanup_execution_error,
                    database_backup_cleanup_preview_error=database_backup_cleanup_preview_error,
                    database_backup_cleanup_execution_error=database_backup_cleanup_execution_error,
                    database_backup_compression_preview_error=database_backup_compression_preview_error,
                    database_backup_compression_execution_error=database_backup_compression_execution_error,
                    task_history_governance_preview_error=task_history_governance_preview_error,
                    task_history_cleanup_execution_error=task_history_cleanup_execution_error,
                    screening_storage_growth_audit_error=screening_storage_growth_audit_error,
                )
                database_governance_dir = (
                    self.settings.reports_dir / "audit" / "database_governance_health"
                )
                (
                    database_governance_json_path,
                    database_governance_md_path,
                ) = write_database_governance_health_audit_artifacts(
                    database_governance_payload,
                    output_dir=str(database_governance_dir),
                )
                database_governance_summary = dict(
                    database_governance_payload.get("summary") or {}
                )
                database_governance_health_audit_summary = {
                    "status": database_governance_summary.get("status") or "unknown",
                    "artifact_path": str(database_governance_json_path),
                    "markdown_path": str(database_governance_md_path),
                    "summary": database_governance_summary,
                    "findings": database_governance_payload.get("findings") or [],
                    "components": database_governance_payload.get("components") or [],
                    "blocked_items": database_governance_payload.get("blocked_items") or [],
                    "review_items": database_governance_payload.get("review_items") or [],
                }
                if str(database_governance_summary.get("status") or "") == "blocked":
                    database_governance_health_audit_error = (
                        "database_governance_health_blocked"
                    )
                log(
                    "数据库治理总账审计完成，"
                    f"status={database_governance_summary.get('status') or 'unknown'}，"
                    f"blocked={int(database_governance_summary.get('blocked_item_count') or 0)}，"
                    f"review={int(database_governance_summary.get('review_item_count') or 0)}"
                )
            except Exception as exc:
                database_governance_health_audit_error = str(exc)
                database_governance_health_audit_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"数据库治理总账审计失败: {exc}")
        else:
            database_governance_health_audit_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }

        weekly_database_optimization_completion_audit_enabled = bool(
            getattr(
                self.settings,
                "scheduler_weekly_database_optimization_completion_audit_enabled",
                False,
            )
        )
        if weekly_database_optimization_completion_audit_enabled:
            try:
                database_completion_config = {
                    "nav_archive_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_nav_history_archive_preview_enabled", False)
                    ),
                    "nav_vacuum_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_nav_history_archive_vacuum_enabled", False)
                    ),
                    "rank_archive_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_rank_history_archive_enabled", False)
                    ),
                    "rank_vacuum_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_rank_history_archive_vacuum_enabled", False)
                    ),
                    "screening_archive_execution_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_screening_history_archive_execution_enabled", False)
                    ),
                    "screening_cleanup_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_screening_history_cleanup_execution_enabled", False)
                    ),
                    "screening_cleanup_mode": (
                        str(
                            getattr(
                                self.settings,
                                "scheduler_weekly_screening_history_cleanup_execution_mode",
                                "",
                            )
                            or ""
                        )
                        .strip()
                        .lower()
                        .replace("_", "-")
                    ),
                    "database_backup_cleanup_execution_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_database_backup_cleanup_execution_enabled", False)
                    ),
                    "database_backup_compression_execution_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_database_backup_compression_execution_enabled", False)
                    ),
                    "screening_storage_growth_audit_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_screening_storage_growth_audit_enabled", False)
                    ),
                    "database_governance_health_audit_enabled": bool(
                        getattr(self.settings, "scheduler_weekly_database_governance_health_audit_enabled", False)
                    ),
                    "database_optimization_completion_audit_enabled": True,
                }
                database_completion_payload = build_database_optimization_completion_audit_payload(
                    db_path=str(self.settings.database_path),
                    project_root=str(
                        getattr(self.settings, "project_root", None)
                        or self._project_root()
                    ),
                    reports_dir=str(self.settings.reports_dir),
                    database_governance_health_audit=database_governance_payload,
                    scheduler_config=database_completion_config,
                )
                database_completion_dir = (
                    self.settings.reports_dir / "audit" / "database_governance_health"
                )
                (
                    database_completion_json_path,
                    database_completion_md_path,
                ) = write_database_optimization_completion_audit_artifacts(
                    database_completion_payload,
                    output_dir=str(database_completion_dir),
                )
                database_completion_summary = dict(
                    database_completion_payload.get("summary") or {}
                )
                database_optimization_completion_audit_summary = {
                    "status": database_completion_summary.get("status") or "unknown",
                    "artifact_path": str(database_completion_json_path),
                    "markdown_path": str(database_completion_md_path),
                    "summary": database_completion_summary,
                    "findings": database_completion_payload.get("findings") or [],
                    "components": database_completion_payload.get("components") or [],
                    "blocked_items": database_completion_payload.get("blocked_items") or [],
                    "review_items": database_completion_payload.get("review_items") or [],
                }
                if str(database_completion_summary.get("status") or "") == "blocked":
                    database_optimization_completion_audit_error = (
                        "database_optimization_completion_blocked"
                    )
                log(
                    "数据库优化完成度审计完成，"
                    f"status={database_completion_summary.get('status') or 'unknown'}，"
                    f"blocked={int(database_completion_summary.get('blocked_item_count') or 0)}，"
                    f"review={int(database_completion_summary.get('review_item_count') or 0)}"
                )
            except Exception as exc:
                database_optimization_completion_audit_error = str(exc)
                database_optimization_completion_audit_summary = {
                    "status": "failed",
                    "error": str(exc),
                }
                log(f"数据库优化完成度审计失败: {exc}")
        else:
            database_optimization_completion_audit_summary = {
                "status": "skipped",
                "skip_reason": "disabled_by_config",
            }

        # P0-02 / B4: 每周维护末段做一次 TTL 清理，默认 apply=True；若清理失败
        # 不影响整体维护任务的成功/降级判定，只把错误记入 summary。
        disk_cleanup_summary: dict[str, Any] | None = None
        disk_cleanup_error: str | None = None
        try:
            from scripts.cleanup_disk_usage import run_cleanup as _run_disk_cleanup

            disk_cleanup_report = _run_disk_cleanup(
                settings=self.settings,
                apply=True,
            )
            disk_cleanup_summary = disk_cleanup_report.to_dict()
            if disk_cleanup_summary.get("errors"):
                disk_cleanup_error = "; ".join(disk_cleanup_summary["errors"])
                log(f"磁盘 TTL 清理存在未完成项: {disk_cleanup_error}")
            accounting = disk_cleanup_summary.get("accounting") or {}
            csv_storage = disk_cleanup_summary.get("core_csv_compression") or {}
            shared_csv_storage = disk_cleanup_summary.get("shared_audit_csv_compression") or {}
            source_migration = disk_cleanup_summary.get("test_source_migration") or {}
            if source_migration:
                log(f"旧成功测试源码迁移: {source_migration.get('status')}; "
                    f"候选{source_migration.get('candidate_count')}份，"
                    f"完成{source_migration.get('migrated_count')}份；"
                    f"移除旧表示的最后链接分配{source_migration.get('removed_old_allocated_bytes')}字节"
                    "（未扣新源码包及记录，不代表项目净回收）")
            if shared_csv_storage:
                log(f"共享审计 CSV 存储: {shared_csv_storage.get('status')}; "
                    f"候选{shared_csv_storage.get('candidate_group_count')}组，"
                    f"压缩{shared_csv_storage.get('compressed_group_count')}组，"
                    f"退役普通路径{shared_csv_storage.get('removed_count')}份；"
                    f"已完成组文件分配减少{shared_csv_storage.get('completed_file_allocation_reduction_bytes')}字节"
                    f"（实际主机净量未测量；{shared_csv_storage.get('reason') or '按批次预算执行'}）")
            if csv_storage:
                log(f"历史核心 CSV 存储: {csv_storage.get('status')}; "
                    f"候选{csv_storage.get('candidate_count')}份，压缩{csv_storage.get('compressed_count')}份；"
                    f"文件分配减少{csv_storage.get('completed_file_allocation_reduction_bytes', 0)}字节"
                    f"（不含新增审计文件；{csv_storage.get('reason') or '按批次预算执行'}）")
            logical_bytes = accounting.get("removed_logical_bytes")
            if logical_bytes:
                log(f"磁盘 TTL 清理已完成删除的文件逻辑大小 {logical_bytes / (1024 * 1024):.1f} MiB")
            allocated_bytes = accounting.get("removed_allocated_bytes_estimate")
            if allocated_bytes:
                log(f"磁盘 TTL 清理预计释放已分配空间 {allocated_bytes / (1024 * 1024):.1f} MiB"
                    "（尚未扣除新增保全及审计文件，实际磁盘净回收未测量）")
        except Exception as exc:
            disk_cleanup_error = str(exc)
            log(f"磁盘 TTL 清理失败: {exc}")

        # P1-03.3 / B7.1: 每周维护时优先尝试通过真实中债收益率刷新 risk_free_rate；
        # 若刷新失败，则保留漂移审计并打告警，但绝不回写伪值。
        risk_free_rate_audit: dict[str, Any] | None = None
        try:
            if collection_rescreen_block_reason:
                # Updating this parameter requires a complete risk recalculation
                # and rescreen. Keep the current definition until that can run.
                risk_free_rate_refresh = {
                    "attempted": False, "succeeded": False,
                    "skip_reason": collection_rescreen_block_reason,
                }
                log(f"采集尚未完整完成({collection_rescreen_block_reason})，暂缓risk_free_rate参数更新与重筛，继续漂移审计")
            else:
                risk_free_rate_refresh = self._attempt_refresh_risk_free_rate()
            risk_free_rate_audit = self._build_risk_free_rate_drift_audit()
            risk_free_rate_audit["refresh_attempt"] = risk_free_rate_refresh
            if not risk_free_rate_refresh.get("succeeded") and not risk_free_rate_refresh.get("skip_reason"):
                log(
                    "risk_free_rate 自动刷新失败，保留现值并继续记录漂移审计；"
                    "未写入任何回退或伪造数值"
                )
            if risk_free_rate_audit.get("alert_triggered"):
                log(
                    f"risk_free_rate 漂移 {risk_free_rate_audit.get('drift_bps'):.1f}bp "
                    f"超过阈值 {risk_free_rate_audit.get('alert_bps_threshold'):.1f}bp"
                )
            if self._risk_free_rate_refresh_changed(risk_free_rate_refresh):
                risk_free_rate_audit["risk_metrics_full_recalculation_required"] = True
                try:
                    refresh_report = self._rescreen_report_after_risk_parameter_change(
                        collection_profile="weekly",
                        log=log,
                    )
                    if str(refresh_report.get("status") or "").strip() == "skipped":
                        risk_free_rate_audit["risk_metrics_full_recalculation_status"] = "skipped"
                        risk_free_rate_audit["risk_metrics_full_recalculation_skip_reason"] = str(
                            refresh_report.get("skip_reason") or "skipped"
                        ).strip()
                        maintenance_rescreen_skipped_reason = str(
                            refresh_report.get("skip_reason") or "skipped"
                        ).strip()
                    else:
                        risk_free_rate_audit["risk_metrics_full_recalculation_status"] = "triggered"
                        maintenance_rescreen_triggered = True
                        maintenance_rescreen_run_id = str(refresh_report.get("run_id") or "").strip()
                        report_refresh_triggered = True
                        report_refresh_run_id = maintenance_rescreen_run_id
                except RuntimeError as exc:
                    risk_free_rate_audit["risk_metrics_full_recalculation_error"] = str(exc)
                    maintenance_rescreen_error = str(exc)
                    report_refresh_error = str(exc)
                    if str(exc) == "workflow_subprocess_busy":
                        risk_free_rate_audit["risk_metrics_full_recalculation_status"] = "skipped"
                        maintenance_rescreen_skipped_reason = self._workflow_busy_skip_reason(
                            after_collection=False
                        )
                        risk_free_rate_audit["risk_metrics_full_recalculation_skip_reason"] = maintenance_rescreen_skipped_reason
                        report_refresh_skipped_reason = maintenance_rescreen_skipped_reason
                        log(f"risk_free_rate 更新后的完整风险重算跳过: {exc}")
                    else:
                        risk_free_rate_audit["risk_metrics_full_recalculation_status"] = "failed"
                        maintenance_rescreen_skipped_reason = None
                        report_refresh_skipped_reason = None
                        log(f"risk_free_rate 更新后的完整风险重算失败: {exc}")
                except Exception as exc:
                    risk_free_rate_audit["risk_metrics_full_recalculation_status"] = "failed"
                    risk_free_rate_audit["risk_metrics_full_recalculation_error"] = str(exc)
                    maintenance_rescreen_error = str(exc)
                    report_refresh_error = str(exc)
                    log(f"risk_free_rate 更新后的完整风险重算失败: {exc}")
            else:
                risk_free_rate_audit["risk_metrics_full_recalculation_required"] = False
        except Exception as exc:  # pragma: no cover - 只做审计，不能影响维护主流程
            risk_free_rate_audit = {"error": str(exc)}
            log(f"risk_free_rate 漂移审计失败: {exc}")

        remaining_tiantian_gap = int(
            ((universe_sync_summary or {}).get("diffs_after_sync") or {}).get("missing_in_local_vs_tiantian_total") or 0
        )
        remaining_autostock_gap = int(
            ((universe_sync_summary or {}).get("diffs_after_sync") or {}).get("missing_in_local_vs_autostock_total") or 0
        )
        remaining_active_tiantian_gap = int(
            ((universe_sync_summary or {}).get("diffs_after_sync") or {}).get("missing_in_local_active_vs_tiantian_total") or 0
        )
        remaining_active_autostock_gap = int(
            ((universe_sync_summary or {}).get("diffs_after_sync") or {}).get("missing_in_local_active_vs_autostock_total") or 0
        )
        detail_screening_relevant_count = int(
            (empty_detail_payload_audit.get("screening_relevant_count") or 0)
        )
        risk_recalculation_incomplete = bool(
            (risk_free_rate_audit or {}).get("risk_metrics_full_recalculation_required")
        ) and (risk_free_rate_audit or {}).get("risk_metrics_full_recalculation_status") != "triggered"
        degraded = (
            risk_recalculation_incomplete
            or bool(maintenance_health["degraded"])
            or bool(weekly_collection_error)
            or detail_screening_relevant_count > 0
            or bool(universe_sync_error)
            or bool(nav_history_maintenance_error)
            or bool(plain_open_tail_follow_up_error)
            or bool(future_outlier_monitor_error)
            or bool(holding_period_no_signal_audit_error)
            or bool(holding_period_high_risk_review_error)
            or bool(nav_history_consistency_audit_error)
            or bool(screening_history_cleanup_preview_error)
            or bool(screening_history_cleanup_runbook_error)
            or bool(screening_history_cleanup_preflight_error)
            or bool(screening_history_archive_preview_error)
            or bool(screening_history_archive_execution_error)
            or bool(screening_history_cleanup_execution_error)
            or bool(screening_history_archive_rotation_error)
            or bool(nav_history_archive_preview_error)
            or bool(nav_history_archive_vacuum_error)
            or bool(rank_history_governance_audit_error)
            or bool(rank_history_archive_preflight_error)
            or bool(rank_history_archive_apply_error)
            or bool(rank_history_archive_rolling_error)
            or bool(rank_history_archive_vacuum_error)
            or bool(incremental_vacuum_error)
            or bool(database_backup_cleanup_preview_error)
            or bool(database_backup_cleanup_execution_error)
            or bool(database_backup_compression_preview_error)
            or bool(database_backup_compression_execution_error)
            or bool(screening_storage_growth_audit_error)
            or bool(task_history_governance_preview_error)
            or bool(task_history_cleanup_execution_error)
            or bool(maintenance_backup_retention_error)
            or bool(database_governance_health_audit_error)
            or bool(database_optimization_completion_audit_error)
            or bool(long_term_history_maintenance_error)
            or bool(regression_smoke_error)
            or bool(maintenance_rescreen_error)
            or bool(report_refresh_error)
            or remaining_tiantian_gap > 0
        )
        if risk_recalculation_incomplete:
            degraded_stage = "risk-parameter-rescreen"
            warning_reason = (
                "risk_parameter_rescreen_failed"
                if (risk_free_rate_audit or {}).get("risk_metrics_full_recalculation_status") == "failed"
                else "risk_parameter_rescreen_incomplete"
            )
        elif maintenance_health["degraded"]:
            degraded_stage = maintenance_health["degraded_stage"]
            warning_reason = maintenance_health["reason"] or None
        elif detail_screening_relevant_count > 0:
            degraded_stage = "collect-details"
            warning_reason = "detail_payload_blocking"
        elif universe_sync_error:
            degraded_stage = "bond-universe-sync"
            warning_reason = "bond_universe_sync_failed"
        elif nav_history_maintenance_error:
            degraded_stage = "nav-history-maintenance"
            warning_reason = "nav_history_maintenance_failed"
        elif plain_open_tail_follow_up_error:
            degraded_stage = "plain-open-tail-follow-up"
            warning_reason = "plain_open_tail_follow_up_failed"
        elif future_outlier_monitor_error:
            degraded_stage = "future-outlier-monitor"
            warning_reason = "future_outlier_monitor_failed"
        elif holding_period_no_signal_audit_error or holding_period_high_risk_review_error:
            degraded_stage = "holding-period-edge-audit"
            warning_reason = "holding_period_edge_audit_failed"
        elif nav_history_consistency_audit_error:
            degraded_stage = "nav-history-consistency-audit"
            warning_reason = "nav_history_consistency_audit_failed"
        elif screening_history_cleanup_preview_error:
            degraded_stage = "screening-history-cleanup-preview"
            warning_reason = "screening_history_cleanup_preview_failed"
        elif screening_history_cleanup_runbook_error:
            degraded_stage = "screening-history-cleanup-runbook"
            warning_reason = "screening_history_cleanup_runbook_failed"
        elif screening_history_cleanup_preflight_error:
            degraded_stage = "screening-history-cleanup-preflight"
            warning_reason = "screening_history_cleanup_preflight_failed"
        elif screening_history_archive_preview_error:
            degraded_stage = "screening-history-archive-preview"
            warning_reason = "screening_history_archive_preview_failed"
        elif screening_history_archive_execution_error:
            degraded_stage = "screening-history-archive-execution"
            warning_reason = "screening_history_archive_execution_failed"
        elif screening_history_cleanup_execution_error:
            degraded_stage = "screening-history-cleanup-execution"
            warning_reason = "screening_history_cleanup_execution_failed"
        elif screening_history_archive_rotation_error:
            degraded_stage = "screening-history-archive-rotation"
            warning_reason = "screening_history_archive_rotation_failed"
        elif nav_history_archive_preview_error:
            degraded_stage = "nav-history-archive-preview"
            warning_reason = "nav_history_archive_preview_failed"
        elif nav_history_archive_vacuum_error:
            degraded_stage = "nav-history-archive-vacuum"
            warning_reason = "nav_history_archive_vacuum_failed"
        elif rank_history_governance_audit_error:
            degraded_stage = "rank-history-governance-audit"
            warning_reason = "rank_history_governance_audit_failed"
        elif rank_history_archive_preflight_error:
            degraded_stage = "rank-history-archive-preflight"
            warning_reason = "rank_history_archive_preflight_failed"
        elif rank_history_archive_apply_error:
            degraded_stage = "rank-history-archive-apply"
            warning_reason = "rank_history_archive_apply_failed"
        elif rank_history_archive_rolling_error:
            degraded_stage = "rank-history-archive-rolling"
            warning_reason = "rank_history_archive_rolling_failed"
        elif rank_history_archive_vacuum_error:
            degraded_stage = "rank-history-archive-vacuum"
            warning_reason = "rank_history_archive_vacuum_failed"
        elif incremental_vacuum_error:
            degraded_stage = "incremental-vacuum"
            warning_reason = "incremental_vacuum_failed"
        elif database_backup_cleanup_preview_error:
            degraded_stage = "database-backup-cleanup-preview"
            warning_reason = "database_backup_cleanup_preview_failed"
        elif database_backup_cleanup_execution_error:
            degraded_stage = "database-backup-cleanup-execution"
            warning_reason = "database_backup_cleanup_execution_failed"
        elif database_backup_compression_preview_error:
            degraded_stage = "database-backup-compression-preview"
            warning_reason = "database_backup_compression_preview_failed"
        elif database_backup_compression_execution_error:
            degraded_stage = "database-backup-compression-execution"
            warning_reason = "database_backup_compression_execution_failed"
        elif screening_storage_growth_audit_error:
            degraded_stage = "screening-storage-growth-audit"
            warning_reason = "screening_storage_growth_audit_failed"
        elif task_history_governance_preview_error:
            degraded_stage = "task-history-governance-preview"
            warning_reason = "task_history_governance_preview_failed"
        elif task_history_cleanup_execution_error:
            degraded_stage = "task-history-cleanup-execution"
            warning_reason = "task_history_cleanup_execution_failed"
        elif maintenance_backup_retention_error:
            degraded_stage = "maintenance-backup-retention"
            warning_reason = "maintenance_backup_recovery_verification_failed"
        elif database_governance_health_audit_error:
            degraded_stage = "database-governance-health-audit"
            warning_reason = "database_governance_health_audit_failed"
        elif database_optimization_completion_audit_error:
            degraded_stage = "database-optimization-completion-audit"
            warning_reason = "database_optimization_completion_audit_failed"
        elif weekly_collection_error:
            degraded_stage = weekly_collection_degraded_stage or "tiantian-business-preflight"
            warning_reason = weekly_collection_warning_reason
        elif long_term_history_maintenance_error:
            degraded_stage = "long-term-history-maintenance"
            warning_reason = "long_term_history_maintenance_failed"
        elif regression_smoke_error:
            degraded_stage = "regression-smoke"
            warning_reason = "regression_smoke_failed"
        elif maintenance_rescreen_error:
            degraded_stage = "maintenance-rescreen"
            warning_reason = "maintenance_rescreen_failed"
        elif report_refresh_error:
            degraded_stage = "report-refresh"
            warning_reason = "report_refresh_failed"
        elif remaining_tiantian_gap > 0:
            degraded_stage = "bond-universe-sync"
            warning_reason = "bond_universe_gap_remains"
        else:
            degraded_stage = ""
            warning_reason = None
        history_backup = None
        from backend.report_history import mode as history_mode
        if history_mode(self.settings) == 'ledger':
            try:
                from backend.report_history_retention import create_backup
                backup = create_backup(self.settings)
                history_backup = {k: backup[k] for k in ('artifact_path','artifact_sha256','independent_restore_verified')}
            except Exception as exc:
                history_backup = {'status':'error','error':str(exc)}
                degraded = True
                degraded_stage = 'report-history-backup'
                warning_reason = 'report_history_backup_failed'
        return {
            'report_history_backup': history_backup,
            "task_name": task_name,
            "status": "degraded" if degraded else "success",
            "started_at": started_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "collection_profile": "weekly",
            "collection_enabled": weekly_collection_enabled,
            "collection_skip_reason": weekly_collection_skip_reason or None,
            "collection_error": weekly_collection_error,
            "collection_degraded": bool(weekly_collection_error) or detail_collection_degraded or period_collection_degraded,
            "collection_degraded_stage": weekly_collection_degraded_stage or ("collect-details" if detail_collection_degraded else "collect-period-returns" if period_collection_degraded else None),
            "collection_warning_reason": weekly_collection_warning_reason or (maintenance_health["reason"] if detail_collection_degraded or period_collection_degraded else None),
            "collection_failures": weekly_collection_failures,
            "collection_rescreen_blocked_reason": collection_rescreen_block_reason,
            "tiantian_business_preflight": weekly_collection_tiantian_business_preflight,
            "screening_triggered": maintenance_rescreen_triggered,
            "screening_reason": (
                "risk_free_rate_changed_full_risk_rescreen"
                if maintenance_rescreen_triggered
                and (risk_free_rate_audit or {}).get("risk_metrics_full_recalculation_status") == "triggered"
                else
                "maintenance_changed_rescreen"
                if maintenance_rescreen_triggered
                else None
            ),
            "degraded": degraded,
            "degraded_stage": degraded_stage,
            "records_processed": maintenance_health["summary_total"],
            "records_failed": maintenance_health["summary_failed"],
            "records_succeeded": maintenance_health["summary_success"],
            "detail_records_processed": maintenance_health["detail_summary_total"],
            "detail_records_failed": maintenance_health["detail_summary_failed"],
            "detail_records_succeeded": maintenance_health["detail_summary_success"],
            "detail_records_skipped": maintenance_health.get("detail_summary_skipped"),
            "detail_collection_summary": maintenance_health.get("detail_collection_summary"),
            "detail_collection_summary_errors": maintenance_health.get("detail_collection_summary_errors", []),
            "company_records_processed": maintenance_health.get("company_summary_total", 0),
            "company_records_failed": maintenance_health.get("company_summary_failed", 0),
            "company_records_succeeded": maintenance_health.get("company_summary_success", 0),
            "company_collection_enabled": bool(getattr(self.settings, "collect_companies", False)) and weekly_collection_enabled,
            "period_return_collection_enabled": weekly_collection_enabled,
            "period_return_collection": maintenance_health.get("period_return_collection"),
            "company_cooldown_count": maintenance_health.get("company_cooldown_count", 0),
            "collection_stage_summaries": maintenance_health.get("summary_by_stage", {}),
            "manager_collection_summary": maintenance_health.get("manager_collection_summary"),
            "manager_collection_summary_errors": maintenance_health.get("manager_collection_summary_errors", []),
            "warning_reason": warning_reason,
            "empty_detail_payload_audit": empty_detail_payload_audit,
            "bond_universe_sync": universe_sync_summary,
            "bond_universe_sync_error": universe_sync_error,
            "bond_universe_remaining_tiantian_gap": remaining_tiantian_gap,
            "bond_universe_remaining_autostock_gap": remaining_autostock_gap,
            "bond_universe_remaining_active_tiantian_gap": remaining_active_tiantian_gap,
            "bond_universe_remaining_active_autostock_gap": remaining_active_autostock_gap,
            "maintenance_data_change_summary": maintenance_data_change_summary,
            "maintenance_rescreen_triggered": maintenance_rescreen_triggered,
            "maintenance_rescreen_run_id": maintenance_rescreen_run_id,
            "maintenance_rescreen_error": maintenance_rescreen_error,
            "maintenance_rescreen_skipped_reason": maintenance_rescreen_skipped_reason,
            "report_refresh_triggered": report_refresh_triggered,
            "report_refresh_run_id": report_refresh_run_id,
            "report_refresh_error": report_refresh_error,
            "report_refresh_skipped_reason": report_refresh_skipped_reason,
            "screening_run_id": maintenance_rescreen_run_id,
            "nav_history_maintenance": nav_history_maintenance_summary,
            "nav_history_maintenance_error": nav_history_maintenance_error,
            "plain_open_tail_follow_up": plain_open_tail_follow_up_summary,
            "plain_open_tail_follow_up_error": plain_open_tail_follow_up_error,
            "future_outlier_monitor": future_outlier_monitor_summary,
            "future_outlier_monitor_error": future_outlier_monitor_error,
            "holding_period_no_signal_audit": holding_period_no_signal_audit_summary,
            "holding_period_no_signal_audit_error": holding_period_no_signal_audit_error,
            "holding_period_high_risk_review": holding_period_high_risk_review_summary,
            "holding_period_high_risk_review_error": holding_period_high_risk_review_error,
            "nav_history_consistency_audit": nav_history_consistency_audit_summary,
            "nav_history_consistency_audit_error": nav_history_consistency_audit_error,
            "screening_history_cleanup_preview": screening_history_cleanup_preview_summary,
            "screening_history_cleanup_preview_error": screening_history_cleanup_preview_error,
            "screening_history_cleanup_runbook": screening_history_cleanup_runbook_summary,
            "screening_history_cleanup_runbook_error": screening_history_cleanup_runbook_error,
            "screening_history_cleanup_preflight": screening_history_cleanup_preflight_summary,
            "screening_history_cleanup_preflight_error": screening_history_cleanup_preflight_error,
            "screening_history_archive_preview": screening_history_archive_preview_summary,
            "screening_history_archive_preview_error": screening_history_archive_preview_error,
            "screening_history_archive_execution": screening_history_archive_execution_summary,
            "screening_history_archive_execution_error": screening_history_archive_execution_error,
            "screening_history_cleanup_execution": screening_history_cleanup_execution_summary,
            "screening_history_cleanup_execution_error": screening_history_cleanup_execution_error,
            "screening_history_archive_rotation": screening_history_archive_rotation_summary,
            "screening_history_archive_rotation_error": screening_history_archive_rotation_error,
            "nav_history_archive_preview": nav_history_archive_preview_summary,
            "nav_history_archive_preview_error": nav_history_archive_preview_error,
            "nav_history_archive_vacuum": nav_history_archive_vacuum_summary,
            "nav_history_archive_vacuum_error": nav_history_archive_vacuum_error,
            "rank_history_governance_audit": rank_history_governance_audit_summary,
            "rank_history_governance_audit_error": rank_history_governance_audit_error,
            "rank_history_archive_preflight": rank_history_archive_preflight_summary,
            "rank_history_archive_preflight_error": rank_history_archive_preflight_error,
            "rank_history_archive_apply": rank_history_archive_apply_summary,
            "rank_history_archive_apply_error": rank_history_archive_apply_error,
            "rank_history_archive_rolling": rank_history_archive_rolling_summary,
            "rank_history_archive_rolling_error": rank_history_archive_rolling_error,
            "rank_history_archive_vacuum": rank_history_archive_vacuum_summary,
            "incremental_vacuum": incremental_vacuum_summary,
            "incremental_vacuum_error": incremental_vacuum_error,
            "rank_history_archive_vacuum_error": rank_history_archive_vacuum_error,
            "database_backup_cleanup_preview": database_backup_cleanup_preview_summary,
            "database_backup_cleanup_preview_error": database_backup_cleanup_preview_error,
            "database_backup_cleanup_execution": database_backup_cleanup_execution_summary,
            "database_backup_cleanup_execution_error": database_backup_cleanup_execution_error,
            "database_backup_compression_preview": database_backup_compression_preview_summary,
            "database_backup_compression_preview_error": database_backup_compression_preview_error,
            "database_backup_compression_execution": database_backup_compression_execution_summary,
            "database_backup_compression_execution_error": database_backup_compression_execution_error,
            "screening_storage_growth_audit": screening_storage_growth_audit_summary,
            "screening_storage_growth_audit_error": screening_storage_growth_audit_error,
            "task_history_governance_preview": task_history_governance_preview_summary,
            "task_history_governance_preview_error": task_history_governance_preview_error,
            "task_history_cleanup_execution": task_history_cleanup_execution_summary,
            "task_history_cleanup_execution_error": task_history_cleanup_execution_error,
            "maintenance_backup_retention": maintenance_backup_retention_summary,
            "maintenance_backup_retention_error": maintenance_backup_retention_error,
            "database_governance_health_audit": database_governance_health_audit_summary,
            "database_governance_health_audit_error": database_governance_health_audit_error,
            "database_optimization_completion_audit": database_optimization_completion_audit_summary,
            "database_optimization_completion_audit_error": database_optimization_completion_audit_error,
            "long_term_history_maintenance": long_term_history_maintenance_summary,
            "long_term_history_maintenance_error": long_term_history_maintenance_error,
            "regression_smoke": regression_smoke_summary,
            "regression_smoke_error": regression_smoke_error,
            "holding_period_change_summary": holding_period_change_summary,
            "disk_cleanup": disk_cleanup_summary,
            "disk_cleanup_error": disk_cleanup_error,
            "risk_free_rate_audit": risk_free_rate_audit,
            "database_freshness_summary": self._refresh_database_freshness_summary_sidecar(),
            **audits,
            "logs_tail": logs[-50:],
        }

    def _execute_storage_lifecycle(self) -> dict[str, Any]:
        """Out-of-process wall-clock bound; interrupted renames have a durable journal."""
        command = [sys.executable, str(self.settings.project_root / 'scripts/manage_storage_lifecycle.py'),
                   'daily', '--execute']
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=60,
                                       cwd=self.settings.project_root)
        except subprocess.TimeoutExpired:
            return {'status': 'deferred', 'skip_reason': 'storage_lifecycle_time_budget',
                    'resume_policy': 'durable_catalog_next_0215', 'max_seconds': 60}
        if completed.returncode:
            raise RuntimeError('storage_lifecycle_failed:' + completed.stderr[-2000:] + completed.stdout[-2000:])
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
        return {'status': 'success' if payload['status'] == 'complete' else 'skipped',
                'skip_reason': payload.get('skip_reason') or ('storage_lifecycle_budget_or_dependency' if payload['status'] != 'complete' else None),
                'storage_lifecycle': payload}

    def run_once(self, job_id: str) -> dict[str, Any]:
        self._ensure_runtime_host_allowed()
        self._ensure_single_instance()
        state_started = False
        heartbeat_started = False
        try:
            recovery = self._recover_state_if_stale()
            self._log_recovery_summary(recovery)
            self._mark_state_started()
            state_started = True
            self._start_heartbeat()
            heartbeat_started = True
            task_name, runner = self._resolve_job_runner(job_id)
            return self._run_task(task_name, runner)
        finally:
            if heartbeat_started:
                self._stop_heartbeat()
            if state_started:
                try:
                    self.state_store.mark_stopped(
                        scheduler_instance_id=self._ensure_scheduler_instance_id()
                    )
                except TypeError:
                    self.state_store.mark_stopped()
            self._cleanup_pid_file()
            self._release_single_instance()

    def start(self) -> None:
        if self._started:
            return
        self._ensure_runtime_host_allowed()
        self._ensure_single_instance()
        try:
            self._start_with_lease_acquired()
        except Exception:
            if self._owns_scheduler_lease():
                try:
                    self.state_store.mark_stopped(
                        scheduler_instance_id=self._ensure_scheduler_instance_id()
                    )
                except TypeError:
                    self.state_store.mark_stopped()
                self._cleanup_pid_file()
                self._release_single_instance()
            raise

    def _start_with_lease_acquired(self) -> None:
        recovery = self._recover_state_if_stale()
        self._log_recovery_summary(recovery)
        self._mark_state_started()
        if bool(getattr(self.settings, 'storage_lifecycle_enabled', False)):
            self.scheduler.add_job(
                lambda: self._run_task('scheduled_storage_lifecycle', self._execute_storage_lifecycle, trigger_source='cron'),
                trigger=CronTrigger(hour=2, minute=15, timezone=self.settings.scheduler_timezone),
                id='storage_lifecycle', name='storage_lifecycle', replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=300,
            )
        self.scheduler.add_job(
            lambda: self._run_task("scheduled_daily_bond_refresh", self._execute_daily_refresh, trigger_source="cron"),
            trigger=CronTrigger(
                hour=self.settings.scheduler_daily_hour,
                minute=self.settings.scheduler_daily_minute,
                timezone=self.settings.scheduler_timezone,
            ),
            id="daily_refresh",
            name="daily_refresh",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=1800,
        )
        if bool(getattr(self.settings, "scheduler_intraday_refresh_enabled", False)):
            self.scheduler.add_job(
                lambda: self._run_task("scheduled_intraday_bond_refresh", self._execute_intraday_refresh, trigger_source="cron"),
                trigger=CronTrigger(
                    hour=self.settings.scheduler_intraday_hour,
                    minute=self.settings.scheduler_intraday_minute,
                    timezone=self.settings.scheduler_timezone,
                ),
                id="intraday_refresh",
                name="intraday_refresh",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=1800,
            )
        self.scheduler.add_job(
            lambda: self._run_task("scheduled_weekly_bond_maintenance", self._execute_weekly_maintenance, trigger_source="cron"),
            trigger=CronTrigger(
                day_of_week=self.settings.scheduler_weekly_day_of_week,
                hour=self.settings.scheduler_weekly_hour,
                minute=self.settings.scheduler_weekly_minute,
                timezone=self.settings.scheduler_timezone,
            ),
            id="weekly_maintenance",
            name="weekly_maintenance",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        # P0-03.2 / B5: 周期性 snapshot watchdog —— cron 空档期（如 daily 跑完后夜间补发净值）
        # 15 分钟内感知到有效净值快照变化并触发一次 intraday 级重评分。
        watchdog_enabled = bool(
            getattr(self.settings, "scheduler_snapshot_watchdog_enabled", True)
        )
        if watchdog_enabled:
            watchdog_minutes = max(
                1,
                int(getattr(self.settings, "scheduler_snapshot_watchdog_interval_minutes", 15) or 15),
            )
            self.scheduler.add_job(
                lambda: self._run_task(
                    "scheduled_snapshot_watchdog_refresh",
                    self._execute_snapshot_watchdog,
                    trigger_source="interval",
                ),
                trigger=IntervalTrigger(
                    minutes=watchdog_minutes,
                    timezone=self.settings.scheduler_timezone,
                ),
                id="snapshot_watchdog",
                name="snapshot_watchdog",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=watchdog_minutes * 60 // 2,
            )
        self.scheduler.start()
        self.state_store.set_jobs(self._serialize_jobs())
        self._start_heartbeat()
        try:
            self._refresh_runtime_status_summary()
        except Exception as exc:  # pragma: no cover - defensive runtime path
            self._log(f"启动后刷新调度状态摘要失败: {exc}")
        self._started = True
        self._log("调度器已启动")
        self._start_startup_recovery()

    def shutdown(self) -> None:
        if not self._started:
            if self._owns_scheduler_lease():
                try:
                    self.state_store.mark_stopped(
                        scheduler_instance_id=self._ensure_scheduler_instance_id()
                    )
                except TypeError:
                    self.state_store.mark_stopped()
                self._cleanup_pid_file()
                self._release_single_instance()
            return
        self._stop_heartbeat()
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            self.state_store.mark_stopped(
                scheduler_instance_id=self._ensure_scheduler_instance_id()
            )
        except TypeError:
            self.state_store.mark_stopped()
        self._cleanup_pid_file()
        self._release_single_instance()
        self._started = False
        self._log("调度器已停止")

    def serve_forever(self) -> None:
        try:
            self.start()
        except RuntimeError as exc:
            if str(exc) == "scheduler_host_retired_by_runtime_config":
                self.shutdown()
                return
            raise
        try:
            while not self.stop_event.is_set():
                self._drain_pending_manual_run_requests_once()
                time.sleep(1)
        finally:
            self.shutdown()


def load_scheduler_status(
    settings: Settings | None = None,
    *,
    apply_runtime_recovery: bool = True,
) -> dict[str, Any]:
    resolved = settings or load_settings()
    state_store = SchedulerStateStore(resolved.scheduler_state_file)
    payload = state_store.snapshot()
    runtime_evidence = ProjectScheduler._scheduler_runtime_evidence(
        resolved,
        state_payload=payload,
    )
    state_claimed_running = bool(payload.get("scheduler_running"))
    payload["scheduler_state_claimed_running"] = state_claimed_running
    payload["scheduler_running"] = bool(runtime_evidence.get("alive"))
    payload["scheduler_runtime_evidence"] = runtime_evidence
    payload["scheduler_recovery_deferred"] = bool(
        apply_runtime_recovery
        and state_claimed_running
        and not runtime_evidence.get("alive")
    )
    payload["scheduler_recovery_boundary"] = "exclusive_scheduler_lease_owner_only"
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = resolved
    scheduler.state_store = state_store
    effective_snapshot_model = load_effective_nav_snapshot(
        resolved.database_path,
        coverage_ratio=resolved.effective_nav_coverage_ratio,
        weekday_only=resolved.effective_nav_weekday_only,
    )
    effective_snapshot = effective_snapshot_model.to_dict()
    latest_screened_snapshot = load_latest_screened_db_snapshot(resolved.latest_report_file)
    due_jobs = scheduler._resolve_startup_catch_up_jobs()
    blocked_snapshot_retry_cooldown = scheduler._blocked_snapshot_retry_cooldown(
        effective_snapshot_model,
        state_payload=payload,
    )
    workflow_busy_retry_cooldown = scheduler._workflow_busy_retry_cooldown(
        state_payload=payload,
    )
    screening_backlog_due = False
    screening_backlog_reason = ""
    try:
        screening_backlog_due, screening_backlog_reason = screening_snapshot_changed(
            effective_snapshot_model,
            latest_screened_snapshot or {},
        )
    except Exception:
        screening_backlog_due = False
        screening_backlog_reason = ""
    payload["enabled"] = bool(resolved.scheduler_enabled)
    configured_job_ids = ["daily_refresh", "weekly_maintenance"]
    if bool(getattr(resolved, 'storage_lifecycle_enabled', False)):
        configured_job_ids.append('storage_lifecycle')
    if bool(getattr(resolved, "scheduler_intraday_refresh_enabled", False)):
        configured_job_ids.append("intraday_refresh")
    if bool(getattr(resolved, "scheduler_snapshot_watchdog_enabled", True)):
        configured_job_ids.append("snapshot_watchdog")
    configured_job_ids = list(dict.fromkeys(configured_job_ids))
    actual_job_ids = [
        str(item.get("id") or item.get("name") or "").strip()
        for item in (payload.get("jobs") or [])
        if str(item.get("id") or item.get("name") or "").strip()
    ]
    missing_job_ids = [job_id for job_id in configured_job_ids if job_id not in actual_job_ids]
    unexpected_job_ids = [job_id for job_id in actual_job_ids if job_id not in configured_job_ids]
    persisted_host_context = str(payload.get("host_context") or "").strip()
    expected_host_mode = str(getattr(resolved, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower()
    expected_host_context = (
        "external_scheduler_process"
        if expected_host_mode == "external_process"
        else "embedded_web_scheduler"
    )
    automation_host_mode = persisted_host_context or expected_host_context
    host_context_mismatch = bool(automation_host_mode != expected_host_context)
    payload["automation_host_mode"] = automation_host_mode
    payload["host_context"] = persisted_host_context or expected_host_context
    payload["configured_host_context"] = expected_host_context
    payload["host_context_mismatch"] = host_context_mismatch
    payload["scheduler_service_runtime"] = _read_systemd_service_runtime(
        getattr(resolved, "scheduler_service_unit_name", "fund-ana-scheduler.service")
    )
    payload["web_service_runtime"] = _read_systemd_service_runtime(
        getattr(resolved, "web_service_unit_name", "fund-ana-web.service")
    )
    payload["startup_catch_up_due_jobs"] = due_jobs
    payload["startup_catch_up_pending"] = bool(due_jobs)
    payload["publication_dependency"] = configured_publication_blocker(resolved)
    payload["blocked_snapshot_retry_cooldown"] = blocked_snapshot_retry_cooldown
    payload["failed_screening_retry_cooldown"] = scheduler._failed_screening_retry_cooldown(
        effective_snapshot_model,
        state_payload=payload,
    )
    payload["workflow_busy_retry_cooldown"] = workflow_busy_retry_cooldown
    payload["screening_backlog_due"] = bool(screening_backlog_due)
    payload["screening_backlog_reason"] = str(screening_backlog_reason or "").strip() or None
    payload["configured_job_ids"] = configured_job_ids
    payload["missing_job_ids"] = missing_job_ids
    payload["unexpected_job_ids"] = unexpected_job_ids
    payload["job_drift_detected"] = bool(missing_job_ids or unexpected_job_ids)
    payload["effective_nav_snapshot"] = effective_snapshot
    payload["latest_screened_snapshot"] = latest_screened_snapshot
    nav_timezone = str(
        getattr(resolved, "timezone", None)
        or getattr(resolved, "scheduler_timezone", None)
        or "Asia/Shanghai"
    )
    observed_at = datetime.now(ZoneInfo(nav_timezone))
    # Test doubles and older integrations may return a naive datetime even
    # when a timezone was requested.  Normalize it at this boundary so the
    # freshness contract always receives an aware instant.
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        observed_at = observed_at.replace(tzinfo=ZoneInfo(nav_timezone))
    payload["nav_freshness"] = build_nav_freshness(
        effective_snapshot,
        observed_at=observed_at,
        timezone_name=nav_timezone,
    )
    payload["task_schedule_slot_runs"] = {}
    for task_key, task_name in SCHEDULE_SLOT_TASK_NAMES.items():
        payload["task_schedule_slot_runs"][task_key] = _load_task_schedule_slot_runs(
            resolved.database_path,
            resolved,
            task_key,
            task_name,
        )
    last_runs = payload.get("last_runs") or {}
    slot_runs = payload.get("task_schedule_slot_runs") or {}
    daily_closure_source = _select_task_closure_source(
        task_key="daily_refresh",
        task_name="scheduled_daily_bond_refresh",
        slot_runs=slot_runs,
        last_runs=last_runs,
    )
    intraday_closure_source = _select_task_closure_source(
        task_key="intraday_refresh",
        task_name="scheduled_intraday_bond_refresh",
        slot_runs=slot_runs,
        last_runs=last_runs,
    )
    weekly_closure_source = _select_task_closure_source(
        task_key="weekly_maintenance",
        task_name="scheduled_weekly_bond_maintenance",
        slot_runs=slot_runs,
        last_runs=last_runs,
    )
    weekly_closure_source = restore_weekly_maintenance_metrics(weekly_closure_source, resolved)
    task_closures = {
        "daily_refresh": ProjectScheduler._build_task_closure_summary(
            task_key="daily_refresh",
            task_name="scheduled_daily_bond_refresh",
            result=daily_closure_source,
        ),
        "intraday_refresh": ProjectScheduler._build_task_closure_summary(
            task_key="intraday_refresh",
            task_name="scheduled_intraday_bond_refresh",
            result=intraday_closure_source,
        ),
        "weekly_maintenance": ProjectScheduler._build_task_closure_summary(
            task_key="weekly_maintenance",
            task_name="scheduled_weekly_bond_maintenance",
            result=weekly_closure_source,
        ),
        "snapshot_watchdog": ProjectScheduler._build_task_closure_summary(
            task_key="snapshot_watchdog",
            task_name="scheduled_snapshot_watchdog_refresh",
            result=last_runs.get("scheduled_snapshot_watchdog_refresh") or {},
        ),
    }
    task_schedule_health = {
        "daily_refresh": _build_task_schedule_health_for_scheduler_audit(
            resolved,
            "daily_refresh",
            daily_closure_source,
            payload.get("started_at"),
        ),
        "intraday_refresh": _build_task_schedule_health_for_scheduler_audit(
            resolved,
            "intraday_refresh",
            intraday_closure_source,
            payload.get("started_at"),
        ),
        "weekly_maintenance": _build_task_schedule_health_for_scheduler_audit(
            resolved,
            "weekly_maintenance",
            weekly_closure_source,
            payload.get("started_at"),
        ),
        "snapshot_watchdog": _build_task_schedule_health_for_scheduler_audit(
            resolved,
            "snapshot_watchdog",
            last_runs.get("scheduled_snapshot_watchdog_refresh") or {},
            payload.get("started_at"),
        ),
    }
    for task_key, task_closure in task_closures.items():
        schedule_health = dict(task_schedule_health.get(task_key) or {})
        for field in (
            "scheduled_for",
            "start_delay_seconds",
            "started_late",
            "startup_catch_up_suspected",
            "startup_catch_up_origin",
            "startup_catch_up_host_context",
            "scheduler_start_gap_seconds",
        ):
            task_closure[field] = schedule_health.get(field)
    child_task_idle_streaks = _calculate_child_task_idle_streaks(
        payload.get("child_task_idle_streaks") or {},
        task_closures,
    )
    for task_key, task_closure in task_closures.items():
        child_tasks = (task_closure or {}).get("child_tasks") or {}
        task_streaks = child_task_idle_streaks.get(task_key) or {}
        for child_key, child_payload in child_tasks.items():
            streak_payload = task_streaks.get(child_key) or {}
            child_payload["idle_streak"] = int(streak_payload.get("idle_streak") or 0)
            if not child_payload.get("no_action_category") and streak_payload.get("no_action_category"):
                child_payload["no_action_category"] = streak_payload.get("no_action_category")
    payload["task_closures"] = task_closures
    payload["child_task_idle_streaks"] = child_task_idle_streaks
    payload["automation_task_closure"] = {
        "daily_refresh": task_closures["daily_refresh"],
        "intraday_refresh": task_closures["intraday_refresh"],
        "weekly_maintenance": task_closures["weekly_maintenance"],
        "snapshot_watchdog": task_closures["snapshot_watchdog"],
    }
    scheduler_runtime_overview = _build_scheduler_overview_summary_for_scheduler_audit(
        resolved,
        payload,
    )
    recent_failure_recovery_summary = (
        scheduler_diagnostics.build_recent_scheduler_failure_recovery_summary(resolved)
    )
    payload["recent_failure_recovery_summary"] = recent_failure_recovery_summary
    persisted_runtime_host_context = persisted_host_context or None
    persisted_runtime_automation_host_mode = (
        persisted_runtime_host_context if persisted_runtime_host_context else None
    )
    maintenance_summary_for_state = deepcopy(
        scheduler_runtime_overview.get("maintenance_summary") or {}
    )
    regression_smoke_summary_for_state = dict(
        maintenance_summary_for_state.get("regression_smoke_summary") or {}
    )
    if (
        not regression_smoke_summary_for_state
        and not getattr(resolved, "reports_dir", None)
        and not getattr(resolved, "project_root", None)
    ):
        maintenance_summary_for_state["regression_smoke_summary"] = {
            "status": "success",
            "overall_status": "passed",
            "passed_group_total": 0,
            "failed_group_total": 0,
            "groups": [],
            "group_results": [],
            "summary_path": None,
            "artifact_dir": None,
            "source": "runtime_default_without_project_context",
        }

    state_store.set_runtime_status_summary(
        {
            "automation_host_mode": persisted_runtime_automation_host_mode,
            "host_context": persisted_runtime_host_context,
            "configured_host_context": expected_host_context,
            "host_context_mismatch": host_context_mismatch,
            "effective_nav_snapshot": _summarize_scheduler_snapshot_for_state(effective_snapshot),
            "latest_screened_snapshot": _summarize_scheduler_snapshot_for_state(latest_screened_snapshot),
            "task_closures": _summarize_task_closures_for_state(task_closures),
            "child_task_idle_streaks": child_task_idle_streaks,
            "nav_freshness": deepcopy(payload.get("nav_freshness") or {}),
            "maintenance_summary": maintenance_summary_for_state,
            "schedule_stability_summary": deepcopy(scheduler_runtime_overview.get("schedule_stability_summary") or {}),
            "automation_closure_digest": deepcopy(scheduler_runtime_overview.get("automation_closure_digest") or {}),
            "recent_failure_recovery_summary": deepcopy(recent_failure_recovery_summary),
            "startup_catch_up_due_jobs": due_jobs,
            "startup_catch_up_pending": bool(due_jobs),
            "screening_backlog_due": bool(screening_backlog_due),
            "screening_backlog_reason": str(screening_backlog_reason or "").strip() or None,
            "configured_job_ids": configured_job_ids,
            "missing_job_ids": missing_job_ids,
            "unexpected_job_ids": unexpected_job_ids,
            "job_drift_detected": bool(missing_job_ids or unexpected_job_ids),
        }
    )
    return payload


class _SchedulerAuditProjectSchedulerAdapter:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _build_empty_detail_payload_audit(self) -> dict[str, Any]:
        scheduler = ProjectScheduler.__new__(ProjectScheduler)
        scheduler.settings = self.settings
        return ProjectScheduler._build_empty_detail_payload_audit(scheduler)


def _resolve_task_expected_run_time_for_scheduler_audit(
    settings: Settings,
    task_key: str,
    started_at: Any,
) -> datetime | None:
    return scheduler_diagnostics.resolve_task_expected_run_time(
        task_key,
        started_at,
        settings=settings,
    )


def _build_task_schedule_health_for_scheduler_audit(
    settings: Settings,
    task_key: str,
    run: dict[str, Any],
    scheduler_started_at: Any,
) -> dict[str, Any]:
    return scheduler_diagnostics.build_task_schedule_health(
        task_key,
        run,
        scheduler_started_at,
        settings=settings,
        parse_local_datetime_fn=scheduler_diagnostics.parse_local_datetime,
        read_machine_boot_time_fn=scheduler_diagnostics.read_machine_boot_time,
        resolve_task_expected_run_time_fn=lambda current_task_key, current_started_at: (
            _resolve_task_expected_run_time_for_scheduler_audit(
                settings,
                current_task_key,
                current_started_at,
            )
        ),
    )


def _resolve_run_latest_screened_snapshot_for_scheduler_audit(
    run: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    overview_latest_screened_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_diagnostics.resolve_run_latest_screened_snapshot(
        run,
        closure,
        overview_latest_screened_snapshot,
        scheduler_snapshot_has_payload_fn=scheduler_diagnostics.scheduler_snapshot_has_payload,
    )


def _build_schedule_stability_summary_for_scheduler_audit(
    settings: Settings,
    scheduler_overview: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_diagnostics.build_schedule_stability_summary(
        scheduler_overview,
        build_task_schedule_health_fn=lambda task_key, run, scheduler_started_at: (
            _build_task_schedule_health_for_scheduler_audit(
                settings,
                task_key,
                run,
                scheduler_started_at,
            )
        ),
    )


def _build_scheduler_task_closure_digest_for_scheduler_audit(
    scheduler_overview: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_diagnostics.build_scheduler_task_closure_digest(
        scheduler_overview,
        resolve_run_latest_screened_snapshot_fn=_resolve_run_latest_screened_snapshot_for_scheduler_audit,
        scheduler_snapshot_has_payload_fn=scheduler_diagnostics.scheduler_snapshot_has_payload,
        scheduler_snapshot_changed_fn=scheduler_diagnostics.scheduler_snapshot_changed,
    )


def _build_scheduler_overview_summary_for_scheduler_audit(
    settings: Settings,
    scheduler_state: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_diagnostics.build_scheduler_overview_summary(
        scheduler_state,
        settings=settings,
        project_scheduler_cls=_SchedulerAuditProjectSchedulerAdapter,
        select_task_closure_source_fn=_select_task_closure_source,
        build_task_schedule_health_fn=lambda task_key, run, scheduler_started_at: (
            _build_task_schedule_health_for_scheduler_audit(
                settings,
                task_key,
                run,
                scheduler_started_at,
            )
        ),
        resolve_run_latest_screened_snapshot_fn=_resolve_run_latest_screened_snapshot_for_scheduler_audit,
        build_schedule_stability_summary_fn=lambda scheduler_overview: (
            _build_schedule_stability_summary_for_scheduler_audit(
                settings,
                scheduler_overview,
            )
        ),
        build_scheduler_task_closure_digest_fn=_build_scheduler_task_closure_digest_for_scheduler_audit,
    )


def _build_scheduler_status_payload_for_scheduler_audit(
    settings: Settings,
    scheduler_state: dict[str, Any] | None,
    *,
    publication: dict[str, Any] | None = None,
    manual_operations_enabled: bool = False,
) -> dict[str, Any]:
    return scheduler_diagnostics.build_scheduler_status_payload(
        scheduler_state,
        publication=publication,
        include_detail=True,
        build_scheduler_overview_summary=lambda payload: (
            _build_scheduler_overview_summary_for_scheduler_audit(settings, payload)
        ),
        build_scheduler_overview_api_summary_fn=scheduler_diagnostics.build_scheduler_overview_api_summary,
        build_scheduler_chain_audit=scheduler_diagnostics.build_scheduler_chain_audit,
        build_automation_autonomy_summary=lambda full_overview, chain_audit: (
            scheduler_diagnostics.build_automation_autonomy_summary(
                full_overview,
                chain_audit,
                manual_operations_enabled=manual_operations_enabled,
                autostock_observation_enabled=(
                    settings.autostock_enabled and settings.publication_validation_provider == "autostock"
                ),
                load_database_overview_fn=lambda effective_nav_snapshot: (
                    _load_database_overview_for_scheduler_audit(
                        settings,
                        effective_nav_snapshot,
                    )
                ),
            )
        ),
        build_scheduler_last_runs_summary_fn=scheduler_diagnostics.build_scheduler_last_runs_summary,
    )


def _resolve_expected_run_time_for_task_slot(
    settings: Settings,
    task_key: str,
    started_dt: datetime,
) -> datetime | None:
    return scheduler_status_support.resolve_expected_run_time_for_task_slot(settings, task_key, started_dt)


def _load_task_schedule_slot_runs(
    db_path: Path,
    settings: Settings,
    task_key: str,
    task_name: str,
    *,
    limit_rows: int = 40,
    limit_slots: int = 6,
) -> list[dict[str, Any]]:
    return scheduler_status_support.load_task_schedule_slot_runs(
        db_path,
        settings,
        task_key,
        task_name,
        parse_datetime=ProjectScheduler._parse_datetime,
        limit_rows=limit_rows,
        limit_slots=limit_slots,
    )


def _select_task_closure_source(
    *,
    task_key: str,
    task_name: str,
    slot_runs: dict[str, Any] | None,
    last_runs: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_status_support.select_task_closure_source(
        task_key=task_key,
        task_name=task_name,
        slot_runs=slot_runs,
        last_runs=last_runs,
        parse_datetime=ProjectScheduler._parse_datetime,
    )


def _summarize_scheduler_snapshot_for_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return scheduler_status_support.summarize_scheduler_snapshot_for_state(snapshot)


def _summarize_task_closure_for_state(task_closure: dict[str, Any] | None) -> dict[str, Any]:
    return scheduler_status_support.summarize_task_closure_for_state(task_closure)


def _summarize_task_closures_for_state(task_closures: dict[str, Any] | None) -> dict[str, Any]:
    return scheduler_status_support.summarize_task_closures_for_state(task_closures)


def _calculate_child_task_idle_streaks(
    current_streaks: dict[str, Any] | None,
    task_closures: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_status_support.calculate_child_task_idle_streaks(current_streaks, task_closures)
