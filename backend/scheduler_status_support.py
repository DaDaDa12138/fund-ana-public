from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from backend.config import Settings
from backend.task_runtime import SCHEDULER_EXECUTION_ROW_SQL
from backend.scheduler_task_timing import copy_task_timing_fields
from backend.scheduler_maintenance_metrics import MAINTENANCE_CHILD_KEYS, maintenance_metrics

SCHEDULE_SLOT_CLUSTER_GAP_SECONDS = 1800


def classify_scheduler_skip_reason(skip_reason: Any) -> str | None:
    reason = str(skip_reason or "").strip().lower()
    if not reason:
        return None
    if reason in {
        "effective_nav_snapshot_already_screened",
        "effective_nav_date_already_screened",
        "effective_nav_date_already_screened_pending_raw_nav_completion",
        "current_snapshot_already_processed",
    }:
        return "already_screened"
    if reason in {
        "effective_nav_not_ready",
        "no_new_inputs",
    }:
        return "no_new_nav"
    if reason in {
        "workflow_busy",
        "workflow_busy_locked",
        "workflow_busy_state_only",
        "workflow_busy_after_collection",
        "workflow_busy_after_collection_locked",
        "workflow_busy_after_collection_state_only",
        "workflow_executor_unavailable",
        "missing_settings",
    }:
        return "automation_guard"
    if reason == "disabled_by_config":
        return "disabled_by_policy"
    return "other"


_NORMAL_AUTOMATION_GUARD_SKIP_REASONS = frozenset(
    {
        "workflow_busy",
        "workflow_busy_locked",
        "workflow_busy_state_only",
        "workflow_busy_after_collection",
        "workflow_busy_after_collection_locked",
        "workflow_busy_after_collection_state_only",
        "scheduler_busy",
    }
)

_BLOCKING_AUTOMATION_GUARD_SKIP_REASONS = frozenset(
    {
        "workflow_executor_unavailable",
        "missing_settings",
        "precheck_failed",
        "runtime_context_unavailable",
        "preview_failed",
        "runbook_failed",
        "cleanup_preflight_failed",
        "archive_preview_failed",
        "preflight_failed",
    }
)

_NORMAL_NO_ACTION_CATEGORIES = frozenset(
    {
        "already_screened",
        "no_new_nav",
        "disabled_by_policy",
        "maintenance_only",
        "backlog_empty",
        "idle_by_design",
        "no_action_after_scan",
    }
)


def classify_scheduler_run_semantic_status(
    *,
    status: Any,
    skip_reason: Any = None,
    no_action_category: Any = None,
    degraded: bool = False,
    classify_skip_reason: Callable[[Any], str | None] = classify_scheduler_skip_reason,
) -> dict[str, Any]:
    normalized_status = str(status or "").strip().lower()
    normalized_skip_reason = str(skip_reason or "").strip().lower()
    normalized_no_action = str(no_action_category or "").strip().lower()
    skip_reason_category = classify_skip_reason(normalized_skip_reason)

    if degraded or normalized_status == "degraded":
        return {
            "run_semantic_status": "degraded",
            "skip_semantic_status": None,
            "skip_reason_category": skip_reason_category,
            "manual_intervention_hint": True,
        }
    if normalized_status in {"failed", "failure", "error"}:
        return {
            "run_semantic_status": "failed",
            "skip_semantic_status": None,
            "skip_reason_category": skip_reason_category,
            "manual_intervention_hint": True,
        }
    if normalized_status in {"success", "ok", "complete", "completed", "passed"}:
        return {
            "run_semantic_status": "success",
            "skip_semantic_status": None,
            "skip_reason_category": skip_reason_category,
            "manual_intervention_hint": False,
        }
    if normalized_status == "running":
        return {
            "run_semantic_status": "running",
            "skip_semantic_status": None,
            "skip_reason_category": skip_reason_category,
            "manual_intervention_hint": False,
        }
    if normalized_status == "skipped":
        skip_semantic_status = "blocked"
        if normalized_no_action in _NORMAL_NO_ACTION_CATEGORIES:
            skip_semantic_status = "normal"
        elif skip_reason_category in {"already_screened", "no_new_nav", "disabled_by_policy"}:
            skip_semantic_status = "normal"
        elif (
            skip_reason_category == "automation_guard"
            and normalized_skip_reason in _NORMAL_AUTOMATION_GUARD_SKIP_REASONS
        ):
            skip_semantic_status = "normal"
        elif normalized_skip_reason in _BLOCKING_AUTOMATION_GUARD_SKIP_REASONS:
            skip_semantic_status = "blocked"
        run_semantic_status = (
            "skipped_normal" if skip_semantic_status == "normal" else "skipped_blocked"
        )
        return {
            "run_semantic_status": run_semantic_status,
            "skip_semantic_status": skip_semantic_status,
            "skip_reason_category": skip_reason_category,
            "manual_intervention_hint": skip_semantic_status == "blocked",
        }
    return {
        "run_semantic_status": normalized_status or "unknown",
        "skip_semantic_status": None,
        "skip_reason_category": skip_reason_category,
        "manual_intervention_hint": False,
    }


def classify_scheduler_subtask_no_action(summary: dict[str, Any] | None) -> str | None:
    payload = dict(summary or {})
    status = str(payload.get("status") or "").strip().lower()
    skip_reason = str(payload.get("skip_reason") or "").strip().lower()
    idle_reason = str(payload.get("idle_reason") or "").strip().lower()
    category = str(payload.get("category") or "").strip().lower()
    maintenance_result = payload.get("maintenance_result") or {}
    group_total = len(payload.get("groups") or [])
    group_results = payload.get("group_results") or []
    processed_group_total = int(payload.get("passed_group_total") or 0) + int(payload.get("failed_group_total") or 0)
    candidate_total = int(
        payload.get("candidate_total")
        or group_total
        or payload.get("selection", {}).get("candidate_total")
        or maintenance_result.get("candidate_total")
        or 0
    )
    processed_total = int(
        payload.get("processed_total")
        or processed_group_total
        or maintenance_result.get("processed_total")
        or 0
    )
    changed_total = int(
        payload.get("changed_total")
        or payload.get("passed_group_total")
        or len([item for item in group_results if str((item or {}).get("status") or "").strip().lower() == "passed"])
        or maintenance_result.get("changed_total")
        or maintenance_result.get("recovered_total")
        or maintenance_result.get("nav_records_inserted")
        or maintenance_result.get("annual_success_total")
        or maintenance_result.get("rank_success_total")
        or maintenance_result.get("shadow_fill_ready_total")
        or 0
    )
    upstream_limited_total = int(
        payload.get("truncated_history_upstream_limited_total")
        or maintenance_result.get("confirmed_upstream_limited_skipped_total")
        or payload.get("selection", {}).get("automatic_scope_excluded_upstream_limited_total")
        or payload.get("selection", {}).get("cooldown_upstream_limited_candidate_total")
        or maintenance_result.get("cooldown_upstream_limited_candidate_total")
        or payload.get("selection", {}).get("cooldown_stale_no_progress_upstream_limited_candidate_total")
        or maintenance_result.get("cooldown_stale_no_progress_upstream_limited_candidate_total")
        or 0
    )
    upstream_limited_selected_total = int(
        payload.get("upstream_limited_selected_total")
        or maintenance_result.get("upstream_limited_selected_total")
        or 0
    )
    pending_recovery_total = int(
        (payload.get("upstream_observation_summary") or {}).get("pending_recovery_total")
        or (maintenance_result.get("upstream_observation_summary") or {}).get("pending_recovery_total")
        or 0
    )
    local_retry_window_pending_total = int(
        ((payload.get("upstream_observation_summary") or {}).get("pending_reason_counts") or {}).get(
            "local_retry_window_pending"
        )
        or ((maintenance_result.get("upstream_observation_summary") or {}).get("pending_reason_counts") or {}).get(
            "local_retry_window_pending"
        )
        or 0
    )

    if status == "skipped" and skip_reason == "disabled_by_config":
        return "disabled_by_policy"
    if idle_reason == "no_candidates_from_latest_audit":
        if category in {"plain_open_tail_follow_up", "future_outlier_monitor"}:
            return "idle_by_design"
        return "backlog_empty"
    if (
        upstream_limited_total > 0
        and processed_total == 0
        and changed_total == 0
        and pending_recovery_total > 0
        and local_retry_window_pending_total > 0
    ):
        return "retry_window_pending"
    if upstream_limited_total > 0 and processed_total == 0 and changed_total == 0:
        return "upstream_limited"
    if upstream_limited_total > 0 and upstream_limited_selected_total >= processed_total and changed_total == 0:
        return "upstream_limited"
    if candidate_total == 0 and processed_total == 0 and changed_total == 0:
        return "backlog_empty"
    if processed_total > 0 and changed_total == 0:
        return "no_action_after_scan"
    return None


def _normalize_long_term_upstream_observation_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    summary = dict(payload or {})
    if not summary:
        return {}
    pending_reason_counts = summary.get("pending_reason_counts") or {}
    pending_recovery_samples = summary.get("pending_recovery_samples") or []
    pending_retry_window_total = int(
        summary.get("pending_retry_window_total")
        or pending_reason_counts.get("local_retry_window_pending")
        or 0
    )
    pending_retry_cooldown_total = int(
        summary.get("pending_retry_cooldown_total")
        or pending_reason_counts.get("local_retry_cooldown_pending")
        or 0
    )
    pending_retry_ready_total = int(
        summary.get("pending_retry_ready_total")
        or pending_reason_counts.get("local_retry_pending")
        or 0
    ) + int(pending_reason_counts.get("local_previous_failure_pending_recheck") or 0)
    fallback_next_retry_values = sorted(
        str((item or {}).get("cooldown_next_retry_after") or "").strip()
        for item in pending_recovery_samples
        if str((item or {}).get("cooldown_next_retry_after") or "").strip()
    )
    normalized = dict(summary)
    normalized["pending_retry_window_total"] = pending_retry_window_total
    normalized["pending_retry_cooldown_total"] = pending_retry_cooldown_total
    normalized["pending_retry_ready_total"] = pending_retry_ready_total
    normalized["pending_next_retry_after_min"] = (
        summary.get("pending_next_retry_after_min")
        or (fallback_next_retry_values[0] if fallback_next_retry_values else None)
    )
    normalized["pending_next_retry_after_max"] = (
        summary.get("pending_next_retry_after_max")
        or (fallback_next_retry_values[-1] if fallback_next_retry_values else None)
    )
    return normalized


def build_task_closure_summary(
    *,
    task_key: str,
    task_name: str,
    result: dict[str, Any] | None,
    classify_skip_reason: Callable[[Any], str | None] = classify_scheduler_skip_reason,
    classify_subtask_no_action: Callable[[dict[str, Any] | None], str | None] = classify_scheduler_subtask_no_action,
) -> dict[str, Any]:
    def _resolve_legacy_no_signal_delta(summary: dict[str, Any]) -> int:
        explicit = summary.get("holding_period_no_signal_delta")
        if explicit is not None:
            explicit_value = int(explicit or 0)
            if explicit_value != 0:
                return explicit_value
        return int(summary.get("holding_period_unknown_delta") or 0)

    payload = dict(result or {})
    task_run_audit = dict(payload.get("task_run_audit") or {})
    task_run_audit_json = (
        str(task_run_audit.get("artifact_path") or payload.get("task_run_audit_path")
            or payload.get("task_run_audit_json") or "").strip()
        or None
    )
    task_run_audit_json_relative = (
        str(
            task_run_audit.get("artifact_path_relative")
            or task_run_audit.get("artifact_project_path")
            or payload.get("task_run_audit_json_relative")
            or payload.get("task_run_audit_json_project_path")
            or ""
        ).strip()
        or None
    )
    task_run_audit_json_project_path = (
        str(
            task_run_audit.get("artifact_project_path")
            or task_run_audit.get("artifact_path_relative")
            or payload.get("task_run_audit_json_project_path")
            or payload.get("task_run_audit_json_relative")
            or ""
        ).strip()
        or None
    )
    task_log_file = (
        str(payload.get("task_log_path") or task_run_audit.get("task_log_path")
            or payload.get("task_log_file") or "").strip()
        or None
    )
    task_log_file_relative = (
        str(
            task_run_audit.get("task_log_path_relative")
            or task_run_audit.get("task_log_project_path")
            or payload.get("task_log_file_relative")
            or payload.get("task_log_file_project_path")
            or ""
        ).strip()
        or None
    )
    task_log_file_project_path = (
        str(
            task_run_audit.get("task_log_project_path")
            or task_run_audit.get("task_log_path_relative")
            or payload.get("task_log_file_project_path")
            or payload.get("task_log_file_relative")
            or ""
        ).strip()
        or None
    )
    status = str(payload.get("status") or "").strip().lower()
    skip_reason = str(payload.get("skip_reason") or "").strip()
    warning_reason = str(payload.get("warning_reason") or "").strip()
    screening_reason = str(payload.get("screening_reason") or "").strip()
    screening_triggered = bool(payload.get("screening_triggered"))
    maintenance_change = dict(payload.get("maintenance_data_change_summary") or {})
    maintenance_change["holding_period_no_signal_delta"] = _resolve_legacy_no_signal_delta(maintenance_change)
    holding_period_change_summary = dict(payload.get("holding_period_change_summary") or {})
    holding_period_change_summary["holding_period_no_signal_delta"] = _resolve_legacy_no_signal_delta(
        holding_period_change_summary
    )
    action_category = "none"
    if screening_triggered:
        if str(screening_reason or "").strip() == "maintenance_changed_rescreen":
            action_category = "maintenance_rescreen_triggered"
        else:
            action_category = "screening_triggered"
    elif bool(payload.get("report_refresh_triggered")):
        action_category = "report_refresh_triggered"
    elif bool(maintenance_change.get("changed")):
        action_category = "data_maintenance_changed_only"
    elif status in {"success", "degraded"}:
        action_category = "scan_only_no_material_change"

    no_action_category = None
    if action_category == "none":
        no_action_category = classify_skip_reason(skip_reason or warning_reason)
    if not no_action_category and action_category in {"scan_only_no_material_change", "data_maintenance_changed_only"}:
        no_action_category = "maintenance_only"

    # History/state may already contain a compact closure. Rebuilding it must
    # not drop its children simply because the original heavy results moved out.
    child_tasks: dict[str, Any] = deepcopy(payload.get("child_tasks") or {})
    subtask_definitions = [
        ("nav_history_sync", payload.get("nav_history_sync_maintenance")),
        ("nav_history_maintenance", payload.get("nav_history_maintenance")),
        ("autostock_nav_tail_fill", payload.get("autostock_nav_tail_fill")),
        ("plain_open_tail_follow_up", payload.get("plain_open_tail_follow_up")),
        ("future_outlier_monitor", payload.get("future_outlier_monitor")),
        ("long_term_history_maintenance", payload.get("long_term_history_maintenance")),
        ("screening_storage_growth_audit", payload.get("screening_storage_growth_audit")),
        ("database_governance_health_audit", payload.get("database_governance_health_audit")),
        ("database_optimization_completion_audit", payload.get("database_optimization_completion_audit")),
        ("regression_smoke", payload.get("regression_smoke")),
    ]
    for child_key, child_payload in subtask_definitions:
        if child_payload is None:
            continue
        child_snapshot = dict(child_payload or {})
        child_maintenance_result = child_snapshot.get("maintenance_result") or {}
        counts = maintenance_metrics(child_snapshot) if child_key in MAINTENANCE_CHILD_KEYS else {}
        classification_input = ({**child_snapshot, **counts,
                                 "maintenance_result": {**child_maintenance_result, **counts}}
                                if counts else child_snapshot)
        child_no_action_category = classify_subtask_no_action(classification_input)
        child_tasks[child_key] = {
            "status": str(child_snapshot.get("status") or "").strip() or None,
            "skip_reason": str(child_snapshot.get("skip_reason") or "").strip() or None,
            "idle_reason": str(child_snapshot.get("idle_reason") or "").strip() or None,
            "no_action_category": child_no_action_category,
            "candidate_total": None if counts else int(
                child_snapshot.get("candidate_total")
                or len(child_snapshot.get("groups") or [])
                or child_snapshot.get("selection", {}).get("candidate_total")
                or child_maintenance_result.get("candidate_total")
                or 0
            ),
            "processed_total": None if counts else int(
                child_snapshot.get("processed_total")
                or (
                    int(child_snapshot.get("passed_group_total") or 0)
                    + int(child_snapshot.get("failed_group_total") or 0)
                )
                or child_maintenance_result.get("processed_total")
                or 0
            ),
            "changed_total": None if counts else int(
                child_snapshot.get("changed_total")
                or child_snapshot.get("passed_group_total")
                or child_maintenance_result.get("changed_total")
                or child_maintenance_result.get("recovered_total")
                or child_maintenance_result.get("nav_records_inserted")
                or child_maintenance_result.get("annual_success_total")
                or child_maintenance_result.get("rank_success_total")
                or child_maintenance_result.get("shadow_fill_ready_total")
                or 0
            ),
            "failed_reason_summary": deepcopy(child_maintenance_result.get("failed_reason_summary") or {}),
            "failed_reason_governance_summary": deepcopy(
                child_maintenance_result.get("failed_reason_governance_summary") or {}
            ),
            "upstream_observation_summary": (
                _normalize_long_term_upstream_observation_summary(
                    child_maintenance_result.get("upstream_observation_summary") or {}
                )
                if child_key == "long_term_history_maintenance"
                else deepcopy(child_maintenance_result.get("upstream_observation_summary") or {})
            ),
            "priority_blocked_summary": deepcopy(
                child_maintenance_result.get("priority_blocked_summary") or {}
            ),
            "governance_queue_consumption_audit": deepcopy(
                child_maintenance_result.get("governance_queue_consumption_audit") or {}
            ),
            "coverage_progress": deepcopy(child_maintenance_result.get("coverage_progress") or {}),
        }
        if child_key in MAINTENANCE_CHILD_KEYS:
            child_tasks[child_key].update(counts)
            if child_tasks[child_key]["processed_total"] is None and child_no_action_category in {
                "backlog_empty", "no_action_after_scan",
            }:
                child_tasks[child_key]["no_action_category"] = None
        if child_key in {
            "screening_storage_growth_audit",
            "database_governance_health_audit",
            "database_optimization_completion_audit",
        }:
            child_summary = dict(child_snapshot.get("summary") or {})
            child_status = str(child_snapshot.get("status") or "").strip().lower()
            component_count = int(
                child_summary.get("component_count")
                or len(child_snapshot.get("components") or [])
                or child_summary.get("run_count")
                or 0
            )
            blocked_item_count = int(child_summary.get("blocked_item_count") or 0)
            if child_status in {"success", "ok", "review", "blocked", "complete"}:
                child_tasks[child_key]["no_action_category"] = None
            child_tasks[child_key].update(
                {
                    "artifact_path": str(child_snapshot.get("artifact_path") or "").strip() or None,
                    "markdown_path": str(child_snapshot.get("markdown_path") or "").strip() or None,
                    "summary": deepcopy(child_snapshot.get("summary") or {}),
                    "blocked_items": deepcopy(child_snapshot.get("blocked_items") or []),
                    "review_items": deepcopy(child_snapshot.get("review_items") or []),
                    "candidate_total": component_count,
                    "processed_total": component_count,
                    "changed_total": blocked_item_count,
                }
            )

    for child_key in MAINTENANCE_CHILD_KEYS:
        if child_key in child_tasks and payload.get(child_key) is None:
            child_tasks[child_key].update(maintenance_metrics(child_tasks[child_key], legacy_summary=True))
            # A policy skip is explicit even when old count fields cannot be
            # verified. Only discard categories inferred from those counters.
            if (child_tasks[child_key]["maintenance_metrics_status"] == "legacy_unverified"
                    and child_tasks[child_key].get("no_action_category") not in {
                        "disabled_by_policy", "idle_by_design",
                    }):
                child_tasks[child_key]["no_action_category"] = None

    empty_detail_payload_audit = dict(payload.get("empty_detail_payload_audit") or {})
    if empty_detail_payload_audit:
        impact_counts = dict(empty_detail_payload_audit.get("impact_counts") or {})
        total_count = int(empty_detail_payload_audit.get("total_empty_fund_count") or 0)
        screening_relevant_count = int(empty_detail_payload_audit.get("screening_relevant_count") or 0)
        non_blocking_count = int(empty_detail_payload_audit.get("non_blocking_count") or 0)
        child_tasks["empty_detail_payload_audit"] = {
            "status": "success" if bool(empty_detail_payload_audit.get("available")) else "skipped",
            "skip_reason": None if bool(empty_detail_payload_audit.get("available")) else "report_missing",
            "idle_reason": None,
            "no_action_category": (
                "backlog_empty"
                if bool(empty_detail_payload_audit.get("available")) and total_count == 0
                else "upstream_limited"
                if bool(empty_detail_payload_audit.get("available"))
                and total_count > 0
                and screening_relevant_count == 0
                else None
            ),
            "candidate_total": total_count,
            "processed_total": total_count,
            "changed_total": screening_relevant_count,
            "impact_counts": impact_counts,
            "screening_relevant_count": screening_relevant_count,
            "non_blocking_count": non_blocking_count,
            "blocking_reason": str(empty_detail_payload_audit.get("blocking_reason") or "").strip() or None,
            "samples_by_impact": deepcopy(empty_detail_payload_audit.get("samples_by_impact") or {}),
            "report_path": str(empty_detail_payload_audit.get("report_path") or "").strip() or None,
        }

    semantic_status = classify_scheduler_run_semantic_status(
        status=status,
        skip_reason=skip_reason,
        no_action_category=no_action_category,
        degraded=bool(payload.get("degraded")) or status == "degraded",
        classify_skip_reason=classify_skip_reason,
    )
    return {
        "task_key": task_key,
        "task_name": task_name,
        "status": status or None,
        **semantic_status,
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        **copy_task_timing_fields(payload),
        "skip_reason": skip_reason or None,
        "screening_reason": screening_reason or None,
        "warning_reason": warning_reason or None,
        "degraded": bool(payload.get("degraded")) or status == "degraded",
        "degraded_stage": str(payload.get("degraded_stage") or "").strip() or None,
        "degradation_summary": deepcopy(payload.get("degradation_summary") or {}),
        "screening_triggered": screening_triggered,
        "maintenance_rescreen_triggered": bool(payload.get("maintenance_rescreen_triggered")),
        "maintenance_rescreen_run_id": str(payload.get("maintenance_rescreen_run_id") or "").strip() or None,
        "report_refresh_triggered": bool(payload.get("report_refresh_triggered")),
        "report_refresh_run_id": str(payload.get("report_refresh_run_id") or "").strip() or None,
        "screening_run_id": str(payload.get("screening_run_id") or "").strip() or None,
        "screening_final_count": payload.get("screening_final_count"),
        "action_category": action_category,
        "no_action_category": no_action_category,
        "before_effective_nav_snapshot": payload.get("before_effective_nav_snapshot") or {},
        "after_effective_nav_snapshot": payload.get("after_effective_nav_snapshot") or {},
        "latest_screened_snapshot": payload.get("latest_screened_snapshot") or {},
        "host_context": payload.get("host_context"),
        "host_runtime_snapshot": payload.get("host_runtime_snapshot") or {},
        "task_run_audit_json": task_run_audit_json,
        "task_run_audit_json_relative": task_run_audit_json_relative,
        "task_run_audit_json_project_path": task_run_audit_json_project_path,
        "task_log_file": task_log_file,
        "task_log_file_relative": task_log_file_relative,
        "task_log_file_project_path": task_log_file_project_path,
        "task_history_id": payload.get("task_history_id"),
        "maintenance_metrics_source": deepcopy(payload.get("maintenance_metrics_source") or {}),
        "effective_snapshot_changed_since_collection": bool(
            payload.get("effective_snapshot_changed_since_collection")
        ),
        "nav_history_consistency_audit": deepcopy(payload.get("nav_history_consistency_audit") or {}),
        "screening_blocks_detail": deepcopy(payload.get("screening_blocks_detail") or {}),
        "screening_blockers": deepcopy(payload.get("screening_blockers") or []),
        "maintenance_data_change_summary": maintenance_change,
        "holding_period_change_summary": holding_period_change_summary,
        "maintenance_change_items": deepcopy(payload.get("maintenance_change_items") or []),
        "child_tasks": child_tasks,
        "child_task_digest": deepcopy(payload.get("child_task_digest") or {}),
        "logs_tail": deepcopy(payload.get("logs_tail") or []),
    }


def resolve_expected_run_time_for_task_slot(
    settings: Settings,
    task_key: str,
    started_dt: datetime,
) -> datetime | None:
    if task_key == "daily_refresh":
        return started_dt.replace(
            hour=int(getattr(settings, "scheduler_daily_hour", 7) or 7),
            minute=int(getattr(settings, "scheduler_daily_minute", 30) or 30),
            second=0,
            microsecond=0,
        )
    if task_key == "intraday_refresh":
        expected = started_dt.replace(
            hour=int(getattr(settings, "scheduler_intraday_hour", 13) or 13),
            minute=int(getattr(settings, "scheduler_intraday_minute", 45) or 45),
            second=0,
            microsecond=0,
        )
        if started_dt < expected:
            expected -= timedelta(days=1)
        return expected
    if task_key == "weekly_maintenance":
        target_weekday = {
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
        }.get(str(getattr(settings, "scheduler_weekly_day_of_week", "sun") or "sun").strip().lower(), 6)
        expected = started_dt.replace(
            hour=int(getattr(settings, "scheduler_weekly_hour", 3) or 3),
            minute=int(getattr(settings, "scheduler_weekly_minute", 0) or 0),
            second=0,
            microsecond=0,
        )
        while expected.weekday() != target_weekday:
            expected -= timedelta(days=1)
        return expected
    return None


def load_task_schedule_slot_runs(
    db_path: Path,
    settings: Settings,
    task_key: str,
    task_name: str,
    *,
    parse_datetime: Callable[[str], datetime | None],
    limit_rows: int = 40,
    limit_slots: int = 6,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT id, task_name, status, started_at, completed_at, extra_data
            FROM task_history
            WHERE task_name = ? AND {SCHEDULER_EXECUTION_ROW_SQL} AND status != 'running'
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (task_name, int(limit_rows)),
        ).fetchall()
    except sqlite3.Error:
        conn.close()
        return []
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            pass

    grouped_runs: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
    expected_slot_dates: dict[str, datetime] = {}
    for row in rows:
        started_text = str(row["started_at"] or "").strip()
        started_dt = parse_datetime(started_text)
        if started_dt is None:
            continue
        expected_dt = resolve_expected_run_time_for_task_slot(settings, task_key, started_dt)
        if expected_dt is None:
            continue
        expected_key = expected_dt.isoformat(timespec="seconds")
        extra_payload: dict[str, Any] = {}
        raw_extra = row["extra_data"]
        if raw_extra:
            try:
                decoded = json.loads(raw_extra)
                if isinstance(decoded, dict):
                    extra_payload = decoded
            except (TypeError, ValueError, json.JSONDecodeError):
                extra_payload = {}
        payload = deepcopy(extra_payload)
        payload.setdefault("task_name", str(row["task_name"] or "").strip() or task_name)
        payload.setdefault("status", str(row["status"] or "").strip() or None)
        payload.setdefault("started_at", started_dt.isoformat(timespec="seconds"))
        finished_text = str(row["completed_at"] or "").strip()
        if finished_text:
            payload.setdefault("finished_at", finished_text.replace(" ", "T"))
        payload["_history_id"] = int(row["id"] or 0)
        payload["_slot_expected_at"] = expected_key
        grouped_runs.setdefault(expected_key, []).append((started_dt, payload))
        expected_slot_dates[expected_key] = expected_dt

    selected_runs: list[tuple[datetime, dict[str, Any]]] = []
    for expected_key, items in grouped_runs.items():
        ordered_items = sorted(items, key=lambda item: item[0])
        selected_cluster: list[tuple[datetime, dict[str, Any]]] = []
        for started_dt, payload in ordered_items:
            if not selected_cluster:
                selected_cluster.append((started_dt, payload))
                continue
            previous_started_dt = selected_cluster[-1][0]
            if int((started_dt - previous_started_dt).total_seconds()) <= SCHEDULE_SLOT_CLUSTER_GAP_SECONDS:
                selected_cluster.append((started_dt, payload))
                continue
            break
        representative = deepcopy(selected_cluster[-1][1])
        representative["_slot_cluster_size"] = len(selected_cluster)
        selected_runs.append((expected_slot_dates[expected_key], representative))

    selected_runs.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in selected_runs[:limit_slots]]


def select_task_closure_source(
    *,
    task_key: str,
    task_name: str,
    slot_runs: dict[str, Any] | None,
    last_runs: dict[str, Any] | None,
    parse_datetime: Callable[[str], datetime | None],
) -> dict[str, Any]:
    def _merge_legacy_task_summary(
        primary: dict[str, Any],
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(primary)
        for summary_key in ("maintenance_data_change_summary", "holding_period_change_summary"):
            primary_summary = dict(merged.get(summary_key) or {})
            fallback_summary = dict(fallback.get(summary_key) or {})
            if not fallback_summary:
                continue
            for key, value in fallback_summary.items():
                if key not in primary_summary or primary_summary.get(key) in (None, "", 0):
                    if value not in (None, ""):
                        primary_summary[key] = deepcopy(value)
            if (
                int(primary_summary.get("holding_period_no_signal_delta") or 0) == 0
                and int(fallback_summary.get("holding_period_unknown_delta") or 0) != 0
            ):
                primary_summary["holding_period_no_signal_delta"] = int(
                    fallback_summary.get("holding_period_unknown_delta") or 0
                )
            if primary_summary:
                merged[summary_key] = primary_summary
        return merged

    slot_source = dict((((slot_runs or {}).get(task_key) or [None])[0]) or {})
    last_source = dict((((last_runs or {}).get(task_name)) or {}))
    if not slot_source:
        return last_source
    if not last_source:
        return slot_source
    last_status = str(last_source.get("status") or "").strip().lower()
    last_skip_reason = str(last_source.get("skip_reason") or "").strip().lower()
    if last_status == "skipped" and last_skip_reason in {
        "workflow_busy",
        "workflow_busy_locked",
        "workflow_busy_state_only",
        "workflow_busy_after_collection",
        "workflow_busy_after_collection_locked",
        "workflow_busy_after_collection_state_only",
    }:
        return _merge_legacy_task_summary(slot_source, last_source)
    if task_key != "weekly_maintenance":
        return _merge_legacy_task_summary(slot_source, last_source)

    slot_started = parse_datetime(str(slot_source.get("started_at") or "").strip())
    last_started = parse_datetime(str(last_source.get("started_at") or "").strip())
    if slot_started is None:
        return last_source
    if last_started is None:
        return _merge_legacy_task_summary(slot_source, last_source)

    if last_started > slot_started:
        return last_source
    return _merge_legacy_task_summary(slot_source, last_source)


def summarize_scheduler_snapshot_for_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(snapshot or {})
    return {
        "candidate_date": str(payload.get("candidate_date") or "").strip() or None,
        "signature": str(payload.get("signature") or "").strip() or None,
        "ready": bool(payload.get("ready")),
        "readiness_reason": str(payload.get("readiness_reason") or "").strip() or None,
        "candidate_fund_count": int(payload.get("candidate_fund_count") or 0),
        "coverage_ratio_vs_total": payload.get("coverage_ratio_vs_total"),
        "risk_1y_effective_end_date": str(payload.get("risk_1y_effective_end_date") or "").strip() or None,
        "risk_1y_effective_fund_count": int(payload.get("risk_1y_effective_fund_count") or 0),
        "risk_1y_effective_coverage_ratio_vs_candidate": payload.get("risk_1y_effective_coverage_ratio_vs_candidate"),
        "raw_max_latest_nav_date": str(payload.get("raw_max_latest_nav_date") or "").strip() or None,
    }


def summarize_task_closure_for_state(task_closure: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(task_closure or {})
    child_task_digest = payload.get("child_task_digest") or {}
    if not isinstance(child_task_digest, dict):
        child_task_digest = {}
    semantic_status = classify_scheduler_run_semantic_status(
        status=payload.get("status"),
        skip_reason=payload.get("skip_reason") or payload.get("warning_reason"),
        no_action_category=payload.get("no_action_category"),
        degraded=bool(payload.get("degraded")) or str(payload.get("status") or "").strip().lower() == "degraded",
    )
    return {
        "task_name": payload.get("task_name"),
        "task_history_id": payload.get("task_history_id"),
        "maintenance_metrics_source": deepcopy(payload.get("maintenance_metrics_source") or {}),
        "status": payload.get("status"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        **copy_task_timing_fields(payload),
        "action_category": payload.get("action_category"),
        "no_action_category": payload.get("no_action_category"),
        "run_semantic_status": payload.get("run_semantic_status") or semantic_status.get("run_semantic_status"),
        "skip_semantic_status": payload.get("skip_semantic_status") or semantic_status.get("skip_semantic_status"),
        "skip_reason_category": payload.get("skip_reason_category") or semantic_status.get("skip_reason_category"),
        "manual_intervention_hint": bool(
            payload.get("manual_intervention_hint")
            if payload.get("manual_intervention_hint") is not None
            else semantic_status.get("manual_intervention_hint")
        ),
        "skip_reason": payload.get("skip_reason"),
        "screening_triggered": bool(payload.get("screening_triggered")),
        "screening_run_id": payload.get("screening_run_id"),
        "screening_final_count": payload.get("screening_final_count"),
        "maintenance_rescreen_triggered": bool(payload.get("maintenance_rescreen_triggered")),
        "maintenance_rescreen_run_id": payload.get("maintenance_rescreen_run_id"),
        "report_refresh_triggered": bool(payload.get("report_refresh_triggered")),
        "report_refresh_run_id": payload.get("report_refresh_run_id"),
        "warning_reason": payload.get("warning_reason"),
        "degraded": bool(payload.get("degraded")) or str(payload.get("status") or "").strip().lower() == "degraded",
        "degraded_stage": payload.get("degraded_stage"),
        "degradation_summary": deepcopy(payload.get("degradation_summary") or {}),
        "attention_needed": bool(payload.get("attention_needed")),
        "host_context": payload.get("host_context"),
        "scheduled_for": payload.get("scheduled_for"),
        "start_delay_seconds": payload.get("start_delay_seconds"),
        "started_late": bool(payload.get("started_late")),
        "startup_catch_up_suspected": bool(payload.get("startup_catch_up_suspected")),
        "startup_catch_up_origin": payload.get("startup_catch_up_origin"),
        "startup_catch_up_host_context": payload.get("startup_catch_up_host_context"),
        "scheduler_start_gap_seconds": payload.get("scheduler_start_gap_seconds"),
        "task_run_audit_json": payload.get("task_run_audit_json"),
        "task_run_audit_json_relative": payload.get("task_run_audit_json_relative"),
        "task_run_audit_json_project_path": payload.get("task_run_audit_json_project_path"),
        "task_log_file": payload.get("task_log_file"),
        "task_log_file_relative": payload.get("task_log_file_relative"),
        "task_log_file_project_path": payload.get("task_log_file_project_path"),
        "maintenance_change_items": deepcopy(payload.get("maintenance_change_items") or []),
        "child_task_counts": deepcopy((child_task_digest.get("counts") or {})),
        "child_tasks": deepcopy(payload.get("child_tasks") or {}),
    }


def summarize_task_closures_for_state(task_closures: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(task_closures or {})
    summarized = {
        str(task_key): summarize_task_closure_for_state(task_closure)
        for task_key, task_closure in payload.items()
        if isinstance(task_key, str)
    }
    weekly_closure = dict(summarized.get("weekly_maintenance") or {})
    weekly_child_tasks = dict(weekly_closure.get("child_tasks") or {})
    long_term_child = dict(weekly_child_tasks.get("long_term_history_maintenance") or {})
    if long_term_child:
        long_term_child = {
            "status": long_term_child.get("status"),
            "skip_reason": long_term_child.get("skip_reason"),
            "idle_reason": long_term_child.get("idle_reason"),
            "no_action_category": long_term_child.get("no_action_category"),
            "candidate_total": long_term_child.get("candidate_total"),
            "processed_total": long_term_child.get("processed_total"),
            "changed_total": long_term_child.get("changed_total"),
            "failed_reason_summary": long_term_child.get("failed_reason_summary") or {},
            "failed_reason_governance_summary": long_term_child.get("failed_reason_governance_summary") or {},
            "upstream_observation_summary": long_term_child.get("upstream_observation_summary") or {},
            "priority_blocked_summary": long_term_child.get("priority_blocked_summary") or {},
            "governance_queue_consumption_audit": long_term_child.get("governance_queue_consumption_audit") or {},
            "coverage_progress": long_term_child.get("coverage_progress") or {},
            "idle_streak": long_term_child.get("idle_streak"),
            **maintenance_metrics(long_term_child, legacy_summary=True),
        }
        weekly_child_tasks["long_term_history_maintenance"] = long_term_child
        weekly_closure["child_tasks"] = weekly_child_tasks
        summarized["weekly_maintenance"] = weekly_closure
    return summarized


def calculate_child_task_idle_streaks(
    current_streaks: dict[str, Any] | None,
    task_closures: dict[str, Any] | None,
) -> dict[str, Any]:
    streaks: dict[str, Any] = deepcopy(current_streaks or {})
    for task_key, task_closure in (task_closures or {}).items():
        if not isinstance(task_key, str):
            continue
        task_streaks = streaks.get(task_key) or {}
        if not isinstance(task_streaks, dict):
            task_streaks = {}
        child_tasks = (task_closure or {}).get("child_tasks") or {}
        next_task_streaks: dict[str, Any] = {}
        for child_key, child_payload in child_tasks.items():
            if not isinstance(child_key, str):
                continue
            no_action_category = str((child_payload or {}).get("no_action_category") or "").strip() or None
            previous = int((task_streaks.get(child_key) or {}).get("idle_streak") or 0)
            if no_action_category in {"backlog_empty", "idle_by_design", "disabled_by_policy"}:
                idle_streak = previous + 1
            else:
                idle_streak = 0
            next_task_streaks[child_key] = {
                "idle_streak": idle_streak,
                "no_action_category": no_action_category,
            }
        if next_task_streaks:
            streaks[task_key] = next_task_streaks
    return streaks
