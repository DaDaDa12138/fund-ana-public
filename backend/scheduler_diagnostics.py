from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable
from urllib.parse import quote

from backend.publication_status import build_publication_status_summary
from backend.weekly_warning_contract import weekly_publication_evidence, weekly_publication_interpretation
from backend.task_runtime import SCHEDULER_EXECUTION_ROW_SQL
from backend.task_audit_storage import read_artifact_text_tail, read_task_audit_bytes
from backend.scheduler_task_timing import copy_task_timing_fields
from backend.scheduler_maintenance_metrics import (
    MAINTENANCE_CHILD_KEYS, maintenance_metrics, optional_count, restore_weekly_maintenance_metrics,
)
from backend.scheduler_lineage import busy_skip_has_current_publication, daily_screening_evidence
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics
from backend.scheduler_status_support import (
    build_task_closure_summary,
    classify_scheduler_run_semantic_status,
    summarize_task_closures_for_state,
)


SCHEDULER_TASK_LABELS = {
    "daily_refresh": "日更调度",
    "intraday_refresh": "盘中补刷",
    "weekly_maintenance": "周维护",
    "snapshot_watchdog": "快照看门狗",
}

SCHEDULER_CHILD_TASK_LABELS = {
    "nav_history_sync": "净值同步巡检",
    "nav_history_maintenance": "净值历史维护",
    "autostock_nav_tail_fill": "AutoStock 尾差回补",
    "plain_open_tail_follow_up": "开放申购尾差跟进",
    "future_outlier_monitor": "未来日期异常监控",
    "long_term_history_maintenance": "长期历史维护",
    "screening_storage_growth_audit": "筛选存储增长审计",
    "database_governance_health_audit": "数据库治理总账审计",
    "database_optimization_completion_audit": "数据库优化完成度审计",
    "regression_smoke": "回归 Smoke 自检",
}

SCHEDULER_MAINTENANCE_CHANGE_LABELS = {
    "nav_records_inserted": "新增净值记录",
    "nav_recovered_total": "补回净值基金",
    "plain_open_tail_follow_up_changed_total": "开放申购尾差修复",
    "autostock_shadow_fill_ready_total": "AutoStock 待补齐样本",
    "long_term_annual_success_total": "长期年收益补齐",
    "long_term_rank_success_total": "长期排名补齐",
}

SCHEDULER_HOLDING_PERIOD_CHANGE_KEYS = [
    "holding_period_blocked_delta",
    "holding_period_generic_delta",
    "holding_period_known_delta",
    "holding_period_no_signal_delta",
    "holding_period_name_unparsed_delta",
    "holding_period_unknown_delta",
    "holding_period_within_threshold_delta",
]

SCHEDULER_CLOSURE_TASK_ORDER = [
    "daily_refresh",
    "intraday_refresh",
    "weekly_maintenance",
    "snapshot_watchdog",
]

SCHEDULER_OVERVIEW_RUN_KEYS = {
    "daily_refresh": "daily_run",
    "intraday_refresh": "intraday_run",
    "weekly_maintenance": "weekly_run",
    "snapshot_watchdog": "snapshot_watchdog_run",
}

DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT = 0
MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT = 50
NAV_HISTORY_ARCHIVE_ROLLING_ARTIFACT_GLOB = "nav_history_archive_rolling_runner_*.json"
NAV_HISTORY_ARCHIVE_RESOLVED_RUNNER_STATUSES = {
    "rolling_archive_not_required",
    "rolling_archive_complete_binding_ready",
}
RANK_HISTORY_ARCHIVE_ROLLING_ARTIFACT_GLOB = "rank_history_archive_rolling_runner_*.json"
RANK_HISTORY_ARCHIVE_RESOLVED_RUNNER_STATUSES = {
    "rolling_archive_in_progress",
    "rolling_archive_complete_binding_ready",
    "rolling_archive_not_required",
}

SCHEDULER_TASK_NAME_BY_KEY = {
    "daily_refresh": "scheduled_daily_bond_refresh",
    "intraday_refresh": "scheduled_intraday_bond_refresh",
    "weekly_maintenance": "scheduled_weekly_bond_maintenance",
    "snapshot_watchdog": "scheduled_snapshot_watchdog_refresh",
}

TASK_WORKFLOW_RUN_ID_FIELDS = (
    "workflow_run_id",
    "screening_run_id",
    "maintenance_rescreen_run_id",
    "report_refresh_run_id",
    "initial_screening_run_id",
)


def _task_bound_workflow_run_id(payload: dict[str, Any] | None) -> str | None:
    source = payload if isinstance(payload, dict) else {}
    for field_name in TASK_WORKFLOW_RUN_ID_FIELDS:
        value = str(source.get(field_name) or "").strip()
        if value:
            return value
    return None


def scheduler_task_key_for_task_name(task_name: Any) -> str | None:
    normalized = str(task_name or "").strip()
    if not normalized:
        return None
    for task_key, scheduler_task_name in SCHEDULER_TASK_NAME_BY_KEY.items():
        if scheduler_task_name == normalized:
            return task_key
    return None

TASK_ARTIFACT_FIELD_NAMES = (
    "task_run_audit_json",
    "task_run_audit_json_relative",
    "task_run_audit_json_project_path",
    "task_log_file",
    "task_log_file_relative",
    "task_log_file_project_path",
)


def copy_task_artifact_fields(
    *sources: dict[str, Any] | None,
) -> dict[str, Any]:
    def _field_candidates(source: dict[str, Any], field_name: str) -> list[Any]:
        task_run_audit = dict(source.get("task_run_audit") or {})
        if field_name == "task_run_audit_json":
            return [
                source.get(field_name),
                source.get("task_run_audit_path"),
                task_run_audit.get("artifact_path"),
            ]
        if field_name == "task_run_audit_json_relative":
            return [
                source.get(field_name),
                task_run_audit.get("artifact_path_relative"),
                task_run_audit.get("artifact_project_path"),
            ]
        if field_name == "task_run_audit_json_project_path":
            return [
                source.get(field_name),
                task_run_audit.get("artifact_project_path"),
                task_run_audit.get("artifact_path_relative"),
            ]
        if field_name == "task_log_file":
            return [
                source.get(field_name),
                source.get("task_log_path"),
                task_run_audit.get("task_log_path"),
            ]
        if field_name == "task_log_file_relative":
            return [
                source.get(field_name),
                task_run_audit.get("task_log_path_relative"),
                task_run_audit.get("task_log_project_path"),
            ]
        if field_name == "task_log_file_project_path":
            return [
                source.get(field_name),
                task_run_audit.get("task_log_project_path"),
                task_run_audit.get("task_log_path_relative"),
            ]
        return [source.get(field_name)]

    artifacts: dict[str, Any] = {}
    for field_name in TASK_ARTIFACT_FIELD_NAMES:
        for source in sources:
            if not isinstance(source, dict):
                continue
            for value in _field_candidates(source, field_name):
                if value not in (None, ""):
                    artifacts[field_name] = value
                    break
            if field_name in artifacts:
                break
    for source in sources:
        if not isinstance(source, dict):
            continue
        error = str(source.get("task_run_audit_error") or "").strip()
        audit_status = source.get("task_run_audit_status")
        if error or audit_status is not None:
            # A newer explicit outcome takes precedence over a closure copied
            # from an earlier run. An absent legacy outcome remains unknown.
            artifacts["task_run_audit_status"] = "failed" if error else audit_status
            artifacts["task_run_audit_error"] = error or None
            break
    return artifacts


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _resolve_project_artifact_path(
    value: Any,
    *,
    project_root: Path | str | None,
) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    root = Path(project_root or Path.cwd()).resolve()
    candidate = Path(text)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    normalized = text.replace("\\", "/")
    for marker in ("/reports/", "reports/", "/logs/", "logs/"):
        marker_index = normalized.find(marker)
        if marker_index < 0:
            continue
        relative = normalized[marker_index + (1 if marker.startswith("/") else 0) :]
        return root / relative
    if not candidate.is_absolute():
        return root / candidate
    return candidate


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(read_task_audit_bytes(path).decode("utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _project_relative_text(path: Path | None, *, project_root: Path | str | None) -> str | None:
    if path is None:
        return None
    root = Path(project_root or Path.cwd()).resolve()
    try:
        return str(path.resolve().relative_to(root))
    except (OSError, ValueError):
        return str(path)


def _read_text_tail(path: Path | None, *, max_bytes: int = 262_144) -> str:
    if path is None:
        return ""
    try:
        return read_artifact_text_tail(path, max_bytes=max_bytes)
    except (OSError, ValueError):
        return ""


def _read_text_head_tail(path: Path | None, *, max_bytes: int = 262_144) -> str:
    if path is None:
        return ""
    try:
        return read_artifact_text_tail(path, max_bytes=max_bytes, include_head=True)
    except (OSError, ValueError):
        return ""


def _parse_embedded_api_source_evidence(text: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    marker = "api_source_evidence="
    for line in str(text or "").splitlines():
        marker_index = line.find(marker)
        if marker_index < 0:
            continue
        raw = line[marker_index + len(marker) :].strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
        elif isinstance(payload, list):
            entries.extend(item for item in payload if isinstance(item, dict))
    return entries


def _resolve_subprocess_artifact_path(
    value: Any,
    *,
    project_root: Path | str | None,
) -> Path | None:
    return _resolve_project_artifact_path(value, project_root=project_root)


def _subprocess_index_path_from_task_log(
    value: Any,
    *,
    project_root: Path | str | None,
) -> Path | None:
    task_log_path = _resolve_project_artifact_path(value, project_root=project_root)
    text = _read_text_head_tail(task_log_path)
    if not text:
        return None
    matches = re.findall(r"subprocess_index=([^\s]+)", text)
    if not matches:
        return None
    return _resolve_project_artifact_path(matches[-1], project_root=project_root)


def _collection_subprocess_health_from_task_log(
    value: Any,
    *,
    project_root: Path | str | None,
) -> dict[str, Any]:
    index_path = _subprocess_index_path_from_task_log(value, project_root=project_root)
    index_payload = _read_json_object(index_path)
    result: dict[str, Any] = {
        "available": bool(index_payload),
        "subprocess_index_path": _project_relative_text(index_path, project_root=project_root),
        "status": "ok",
        "task_count": 0,
        "warning_count": 0,
        "blocker_count": 0,
        "warnings": [],
        "blockers": [],
        "tasks": {},
        "truth_policy": "read_only_subprocess_log_evidence_no_state_mutation",
    }
    if not index_payload:
        result["status"] = "not_available"
        return result

    items = [item for item in index_payload.get("items") or [] if isinstance(item, dict)]
    result["task_count"] = len(items)
    warnings: list[str] = []
    blockers: list[str] = []
    task_summaries: dict[str, Any] = {}
    for item in items:
        task_name = str(item.get("task_name") or "").strip()
        if not task_name:
            continue
        stdout_path = _resolve_subprocess_artifact_path(
            item.get("stdout_path"),
            project_root=project_root,
        )
        stderr_path = _resolve_subprocess_artifact_path(
            item.get("stderr_path"),
            project_root=project_root,
        )
        evidence_entries = _parse_embedded_api_source_evidence(_read_text_tail(stdout_path))
        stderr_text = _read_text_tail(stderr_path)
        blocker_samples = [
            line.strip()
            for line in stderr_text.splitlines()
            if line.strip()
            and not re.search(r"\b(INFO|DEBUG)\b", line, flags=re.IGNORECASE)
        ][:12]
        source_summaries: dict[str, Any] = {}
        task_failed_response_count = 0
        task_empty_response_count = 0
        task_transport_exception_count = 0
        task_save_failed_count = 0
        failure_samples: list[dict[str, Any]] = []
        for evidence in evidence_entries:
            source = str(evidence.get("source") or "").strip() or "unknown"
            failed_response_count = _to_int(evidence.get("failed_response_count"))
            empty_response_count = _to_int(evidence.get("empty_response_count"))
            transport_exception_count = _to_int(evidence.get("transport_exception_count"))
            save_failed_count = _to_int(evidence.get("save_failed_count"))
            task_failed_response_count += failed_response_count
            task_empty_response_count += empty_response_count
            task_transport_exception_count += transport_exception_count
            task_save_failed_count += save_failed_count
            failure_samples.extend(
                sample
                for sample in evidence.get("failure_samples") or []
                if isinstance(sample, dict)
            )
            source_summaries[source] = {
                "source": source,
                "target_count": _to_int(evidence.get("target_count")),
                "request_count": _to_int(evidence.get("request_count")),
                "accepted_response_count": _to_int(evidence.get("accepted_response_count")),
                "failed_response_count": failed_response_count,
                "empty_response_count": empty_response_count,
                "transport_exception_count": transport_exception_count,
                "save_failed_count": save_failed_count,
                "quality_status_counts": evidence.get("quality_status_counts") or {},
                "quality_payload_type_counts": evidence.get("quality_payload_type_counts") or {},
                "stage": evidence.get("stage"),
                "failure_samples": failure_samples[:5],
            }
        task_warning_codes: list[str] = []
        task_blocker_codes: list[str] = []
        if task_failed_response_count > 0:
            task_warning_codes.append("failed_responses_present")
        if task_transport_exception_count > 0:
            task_warning_codes.append("transport_exceptions_present")
        if task_save_failed_count > 0:
            task_blocker_codes.append("save_failures_present")
        if blocker_samples:
            task_warning_codes.append("stderr_nonempty_failure_signals_present")
        if item.get("success") is not True or item.get("return_code") not in (0, "0", None):
            task_blocker_codes.append("subprocess_execution_failed")
        for code in task_warning_codes:
            warnings.append(f"{task_name}:{code}")
        for code in task_blocker_codes:
            blockers.append(f"{task_name}:{code}")
        task_summaries[task_name] = {
            "task_name": task_name,
            "success": bool(item.get("success")),
            "return_code": item.get("return_code"),
            "duration_seconds": item.get("duration_seconds"),
            "failed_response_count": task_failed_response_count,
            "empty_response_count": task_empty_response_count,
            "transport_exception_count": task_transport_exception_count,
            "save_failed_count": task_save_failed_count,
            "stderr_blocker_sample_count": len(blocker_samples),
            "stderr_blocker_samples": blocker_samples,
            "failure_samples": failure_samples[:8],
            "sources": source_summaries,
            "warnings": task_warning_codes,
            "blockers": task_blocker_codes,
        }

    result["tasks"] = task_summaries
    result["warnings"] = warnings
    result["blockers"] = blockers
    result["warning_count"] = len(warnings)
    result["blocker_count"] = len(blockers)
    result["status"] = "needs_review" if blockers else "watch" if warnings else "ok"
    return result


def apply_weekly_collection_subprocess_overlay(
    weekly_run: dict[str, Any] | None,
    *,
    project_root: Path | str | None,
) -> dict[str, Any]:
    payload = dict(weekly_run or {})
    artifacts = copy_task_artifact_fields(payload)
    collection_health = _collection_subprocess_health_from_task_log(
        artifacts.get("task_log_file_project_path")
        or artifacts.get("task_log_file_relative")
        or artifacts.get("task_log_file"),
        project_root=project_root,
    )
    if collection_health.get("available"):
        payload["collection_subprocess_health"] = collection_health
    tasks = collection_health.get("tasks") if isinstance(collection_health.get("tasks"), dict) else {}
    affected_task_set = {
        task_name
        for task_name, task in tasks.items()
        if task_name in {"collect-funds", "collect-details", "collect-managers", "collect-companies", "collect-period-returns"}
        and isinstance(task, dict)
        and (task.get("warnings") or task.get("blockers"))
    }
    affected_tasks = [
        task_name
        for task_name in ("collect-managers", "collect-details", "collect-funds", "collect-companies", "collect-period-returns")
        if task_name in affected_task_set
    ]
    if affected_tasks and str(payload.get("status") or "").strip().lower() == "success":
        primary_task_name = affected_tasks[0]
        primary_task = tasks.get(primary_task_name) if isinstance(tasks.get(primary_task_name), dict) else {}
        sources = primary_task.get("sources") if isinstance(primary_task.get("sources"), dict) else {}
        primary_source = (
            sources.get("fundMNMangerList")
            or sources.get("fundMNDetailInformation")
            or next(iter(sources.values()), {})
            if sources
            else {}
        )
        if not isinstance(primary_source, dict):
            primary_source = {}
        payload.update(
            {
                "status": "degraded",
                "degraded": True,
                "degraded_stage": primary_task_name,
                "warning_reason": "collection_partial_failure",
                "records_failed": _to_int(primary_task.get("failed_response_count")),
                "records_processed": _to_int(primary_source.get("target_count")),
                "records_succeeded": _to_int(primary_source.get("accepted_response_count")),
            }
        )
    return payload


def _latest_nav_history_archive_rolling_artifact(
    *,
    project_root: Path | str | None,
) -> Path | None:
    root = Path(project_root or Path.cwd()).resolve()
    artifact_dir = root / "reports" / "audit" / "nav_history_archive"
    if not artifact_dir.exists():
        return None
    candidates = sorted(
        path for path in artifact_dir.glob(NAV_HISTORY_ARCHIVE_ROLLING_ARTIFACT_GLOB) if path.is_file()
    )
    return candidates[-1] if candidates else None


def _latest_rank_history_archive_rolling_artifact(
    *,
    project_root: Path | str | None,
) -> Path | None:
    root = Path(project_root or Path.cwd()).resolve()
    artifact_dir = root / "reports" / "audit" / "rank_history_archive"
    if not artifact_dir.exists():
        return None
    candidates = [
        path
        for path in artifact_dir.glob(RANK_HISTORY_ARCHIVE_ROLLING_ARTIFACT_GLOB)
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime_ns, path.name))


def _summary_int(summary: dict[str, Any], key: str) -> int:
    return _to_int(summary.get(key))


def _nav_history_archive_preview_resolution_evidence(
    run: dict[str, Any],
    *,
    task_key: str | None,
    degraded_stage: str | None,
    warning_reason: str | None,
    project_root: Path | str | None,
) -> dict[str, Any]:
    if (
        task_key != "weekly_maintenance"
        or str(degraded_stage or "").strip() != "nav-history-archive-preview"
        or str(warning_reason or "").strip() != "nav_history_archive_preview_failed"
    ):
        return {"applicable": False}

    preview = run.get("nav_history_archive_preview") if isinstance(run.get("nav_history_archive_preview"), dict) else {}
    preview_artifacts = preview.get("artifacts") if isinstance(preview.get("artifacts"), dict) else {}
    preview_rolling = (
        preview_artifacts.get("rolling_runner")
        if isinstance(preview_artifacts.get("rolling_runner"), dict)
        else {}
    )
    historical_path_text = (
        preview.get("artifact_path")
        or preview_rolling.get("artifact_path")
        or preview_rolling.get("artifact_project_path")
    )
    historical_path = _resolve_project_artifact_path(
        historical_path_text,
        project_root=project_root,
    )
    historical_payload = _read_json_object(historical_path)
    historical_summary = (
        historical_payload.get("summary")
        if isinstance(historical_payload.get("summary"), dict)
        else preview.get("summary")
        if isinstance(preview.get("summary"), dict)
        else {}
    )

    latest_path = _latest_nav_history_archive_rolling_artifact(project_root=project_root)
    latest_payload = _read_json_object(latest_path)
    latest_summary = (
        latest_payload.get("summary")
        if isinstance(latest_payload.get("summary"), dict)
        else {}
    )

    historical_name = historical_path.name if historical_path is not None else ""
    latest_name = latest_path.name if latest_path is not None else ""
    latest_after_historical = None
    if historical_name and latest_name:
        latest_after_historical = latest_name > historical_name

    latest_runner_status = str(latest_summary.get("runner_status") or "").strip()
    hot_before_active_count = _summary_int(latest_summary, "hot_before_active_cutoff_row_count")
    hot_before_current_count = _summary_int(latest_summary, "hot_before_current_cutoff_row_count")
    stale_missing_key_count = _summary_int(latest_summary, "stale_hot_missing_archive_key_row_count")
    key_conflict_count = _summary_int(latest_summary, "stale_hot_key_conflict_row_count")
    archive_out_of_scope_count = _summary_int(latest_summary, "archive_out_of_scope_current_row_count")
    blocker_count = _summary_int(latest_summary, "blocker_count")
    root_cause_resolved = bool(
        latest_summary
        and latest_runner_status in NAV_HISTORY_ARCHIVE_RESOLVED_RUNNER_STATUSES
        and blocker_count == 0
        and hot_before_active_count == 0
        and hot_before_current_count == 0
        and stale_missing_key_count == 0
        and key_conflict_count == 0
        and archive_out_of_scope_count == 0
        and str(latest_summary.get("active_db_quick_check") or "").strip() == "ok"
        and str(latest_summary.get("archive_quick_check") or "").strip() == "ok"
        and latest_after_historical is not False
    )

    return {
        "applicable": True,
        "available": bool(latest_summary),
        "truth_policy": "read_only_runtime_artifact_evidence_no_state_mutation",
        "historical_failure": {
            "artifact_path": _project_relative_text(historical_path, project_root=project_root),
            "runner_status": historical_summary.get("runner_status"),
            "blocker_count": _summary_int(historical_summary, "blocker_count"),
            "rolling_archive_blocked_reason": historical_summary.get("rolling_archive_blocked_reason"),
            "stale_hot_missing_archive_key_row_count": _summary_int(
                historical_summary,
                "stale_hot_missing_archive_key_row_count",
            ),
            "hot_before_current_cutoff_row_count": _summary_int(
                historical_summary,
                "hot_before_current_cutoff_row_count",
            ),
        },
        "latest_resolution_evidence": {
            "artifact_path": _project_relative_text(latest_path, project_root=project_root),
            "generated_at": latest_payload.get("generated_at"),
            "runner_status": latest_runner_status or None,
            "active_cutoff_date": latest_summary.get("active_cutoff_date"),
            "archive_cutoff_date": latest_summary.get("archive_cutoff_date"),
            "blocker_count": blocker_count,
            "review_item_count": _summary_int(latest_summary, "review_item_count"),
            "hot_before_active_cutoff_row_count": hot_before_active_count,
            "hot_before_current_cutoff_row_count": hot_before_current_count,
            "stale_hot_missing_archive_key_row_count": stale_missing_key_count,
            "stale_hot_key_conflict_row_count": key_conflict_count,
            "archive_out_of_scope_current_row_count": archive_out_of_scope_count,
            "active_db_quick_check": latest_summary.get("active_db_quick_check"),
            "archive_quick_check": latest_summary.get("archive_quick_check"),
        },
        "latest_evidence_after_historical_failure": latest_after_historical,
        "root_cause_resolved": root_cause_resolved,
        "root_cause_status": (
            "resolved_awaiting_next_weekly_maintenance"
            if root_cause_resolved
            else "unresolved_or_resolution_evidence_missing"
        ),
        "current_interpretation": (
            "Historical weekly maintenance remains degraded, but later read-only NAV archive evidence proves "
            "the underlying rolling archive blocker is clear; wait for the next scheduled weekly maintenance "
            "to refresh the historical warning."
            if root_cause_resolved
            else "Historical weekly maintenance degradation still lacks later read-only evidence proving the "
            "NAV archive blocker is clear."
        ),
    }


def _rank_history_archive_rolling_resolution_evidence(
    run: dict[str, Any],
    *,
    task_key: str | None,
    degraded_stage: str | None,
    warning_reason: str | None,
    project_root: Path | str | None,
) -> dict[str, Any]:
    if (
        task_key != "weekly_maintenance"
        or str(degraded_stage or "").strip() != "rank-history-archive-rolling"
        or str(warning_reason or "").strip() != "rank_history_archive_rolling_failed"
    ):
        return {"applicable": False}

    historical = (
        run.get("rank_history_archive_rolling")
        if isinstance(run.get("rank_history_archive_rolling"), dict)
        else {}
    )
    historical_path = _resolve_project_artifact_path(
        historical.get("artifact_path"),
        project_root=project_root,
    )
    historical_payload = _read_json_object(historical_path)
    historical_summary = (
        historical_payload.get("summary")
        if isinstance(historical_payload.get("summary"), dict)
        else historical.get("summary")
        if isinstance(historical.get("summary"), dict)
        else {}
    )

    latest_path = _latest_rank_history_archive_rolling_artifact(project_root=project_root)
    latest_payload = _read_json_object(latest_path)
    latest_summary = (
        latest_payload.get("summary")
        if isinstance(latest_payload.get("summary"), dict)
        else {}
    )
    latest_after_historical = None
    if historical_path is not None and latest_path is not None:
        try:
            latest_after_historical = latest_path.stat().st_mtime_ns > historical_path.stat().st_mtime_ns
        except OSError:
            latest_after_historical = latest_path.name > historical_path.name

    latest_runner_status = str(latest_summary.get("runner_status") or "").strip()
    blocker_count = _summary_int(latest_summary, "blocker_count")
    stale_missing_key_count = _summary_int(
        latest_summary,
        "stale_hot_missing_archive_key_row_count",
    )
    unsafe_conflict_count = _summary_int(
        latest_summary,
        "stale_hot_unsafe_conflict_row_count",
    )
    hot_before_current_count = _summary_int(
        latest_summary,
        "hot_before_current_cutoff_row_count",
    )
    in_progress_has_apply_evidence = bool(
        latest_runner_status != "rolling_archive_in_progress"
        or (
            latest_summary.get("rolling_archive_executed") is True
            and str(latest_summary.get("stale_hot_delete_result_status") or "").strip()
            == "executed"
            and _summary_int(latest_summary, "stale_hot_delete_deleted_row_count") > 0
        )
    )
    root_cause_resolved = bool(
        latest_summary
        and latest_runner_status in RANK_HISTORY_ARCHIVE_RESOLVED_RUNNER_STATUSES
        and blocker_count == 0
        and stale_missing_key_count == 0
        and unsafe_conflict_count == 0
        and str(latest_summary.get("active_db_quick_check") or "").strip() == "ok"
        and str(latest_summary.get("archive_quick_check") or "").strip() == "ok"
        and in_progress_has_apply_evidence
        and latest_after_historical is not False
    )

    return {
        "applicable": True,
        "available": bool(latest_summary),
        "truth_policy": "read_only_runtime_artifact_evidence_no_state_mutation",
        "historical_failure": {
            "artifact_path": _project_relative_text(historical_path, project_root=project_root),
            "runner_status": historical_summary.get("runner_status"),
            "blocker_count": _summary_int(historical_summary, "blocker_count"),
            "stale_hot_exact_delete_supported": historical_summary.get(
                "stale_hot_exact_delete_supported"
            ),
            "stale_hot_missing_archive_key_row_count": _summary_int(
                historical_summary,
                "stale_hot_missing_archive_key_row_count",
            ),
            "stale_hot_key_conflict_row_count": _summary_int(
                historical_summary,
                "stale_hot_key_conflict_row_count",
            ),
        },
        "latest_resolution_evidence": {
            "artifact_path": _project_relative_text(latest_path, project_root=project_root),
            "generated_at": latest_payload.get("generated_at"),
            "runner_status": latest_runner_status or None,
            "blocker_count": blocker_count,
            "review_item_count": _summary_int(latest_summary, "review_item_count"),
            "stale_hot_missing_archive_key_row_count": stale_missing_key_count,
            "stale_hot_unsafe_conflict_row_count": unsafe_conflict_count,
            "stale_hot_archive_reconcile_result_status": latest_summary.get(
                "stale_hot_archive_reconcile_result_status"
            ),
            "stale_hot_delete_result_status": latest_summary.get(
                "stale_hot_delete_result_status"
            ),
            "stale_hot_delete_deleted_row_count": _summary_int(
                latest_summary,
                "stale_hot_delete_deleted_row_count",
            ),
            "hot_before_current_cutoff_row_count": hot_before_current_count,
            "active_db_quick_check": latest_summary.get("active_db_quick_check"),
            "archive_quick_check": latest_summary.get("archive_quick_check"),
        },
        "latest_evidence_after_historical_failure": latest_after_historical,
        "root_cause_resolved": root_cause_resolved,
        "root_cause_status": (
            "resolved_awaiting_next_weekly_maintenance"
            if root_cause_resolved
            else "unresolved_or_resolution_evidence_missing"
        ),
        "current_interpretation": (
            "Historical weekly rank archive failure is resolved by a later bounded page with zero "
            "blockers, zero missing archive keys, zero unsafe conflicts, and healthy databases. "
            f"The remaining {hot_before_current_count} old hot rows are an automatic rolling backlog."
            if root_cause_resolved
            else "Historical weekly rank archive degradation still lacks later read-only evidence "
            "that the failed rolling page can continue safely."
        ),
    }


def _classify_scheduler_degradation_category(
    *,
    task_key: str | None,
    degraded_stage: str | None,
    warning_reason: str | None,
    status: str | None,
) -> str:
    stage = str(degraded_stage or "").strip()
    reason = str(warning_reason or "").strip()
    if stage in {"collect-funds", "collect-managers", "collect-details", "collect-companies", "collect-period-returns", "bond-universe-sync"} or reason in {
        "tiantian_business_unavailable",
        "tiantian_payload_invalid",
        "detail_payload_blocking",
        "bond_universe_sync_failed",
        "bond_universe_gap_remains",
    }:
        return "upstream_api_unavailable"
    if stage in {
        "nav-history-maintenance",
        "plain-open-tail-follow-up",
        "future-outlier-monitor",
        "holding-period-edge-audit",
        "nav-history-consistency-audit",
        "long-term-history-maintenance",
        "regression-smoke",
    }:
        return "local_maintenance_failed"
    if stage in {"maintenance-rescreen", "risk-parameter-rescreen", "report-refresh"}:
        return "workflow_refresh_failed"
    if str(status or "").strip().lower() == "failed":
        return "scheduler_task_failed"
    if task_key == "weekly_maintenance":
        return "weekly_maintenance_degraded"
    return "scheduler_run_degraded"


def build_scheduler_run_degradation_summary(
    run: dict[str, Any] | None,
    *,
    task_key: str | None = None,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    payload = dict(run or {})
    status = str(payload.get("status") or "").strip().lower()
    degraded_stage = _clean_text(payload.get("degraded_stage"))
    warning_reason = _clean_text(payload.get("warning_reason"))
    degraded = bool(payload.get("degraded")) or status in {"degraded", "failed"}
    if not (degraded or degraded_stage or warning_reason):
        return {}

    category = _classify_scheduler_degradation_category(
        task_key=task_key,
        degraded_stage=degraded_stage,
        warning_reason=warning_reason,
        status=status,
    )
    empty_detail_payload_audit = dict(payload.get("empty_detail_payload_audit") or {})
    empty_impact_counts = dict(empty_detail_payload_audit.get("impact_counts") or {})
    maintenance_change = dict(payload.get("maintenance_data_change_summary") or {})
    remaining_tiantian_gap = _to_int(payload.get("bond_universe_remaining_tiantian_gap"))
    remaining_autostock_gap = _to_int(payload.get("bond_universe_remaining_autostock_gap"))
    remaining_active_tiantian_gap = _to_int(payload.get("bond_universe_remaining_active_tiantian_gap"))
    remaining_active_autostock_gap = _to_int(payload.get("bond_universe_remaining_active_autostock_gap"))
    screening_relevant_empty_detail_count = _to_int(
        empty_detail_payload_audit.get("screening_relevant_count")
        or empty_impact_counts.get("screening_relevant")
    )
    non_blocking_empty_detail_count = _to_int(empty_detail_payload_audit.get("non_blocking_count"))
    if non_blocking_empty_detail_count <= 0:
        non_blocking_empty_detail_count = max(
            0,
            _to_int(empty_detail_payload_audit.get("total_empty_fund_count"))
            - screening_relevant_empty_detail_count,
        )

    maintenance_rescreen_error = _clean_text(payload.get("maintenance_rescreen_error"))
    maintenance_rescreen_skipped_reason = _clean_text(payload.get("maintenance_rescreen_skipped_reason"))
    report_refresh_error = _clean_text(payload.get("report_refresh_error"))
    report_refresh_skipped_reason = _clean_text(payload.get("report_refresh_skipped_reason"))
    tiantian_preflight = dict(payload.get("tiantian_business_preflight") or {})
    tiantian_preflight_probe = dict(tiantian_preflight.get("probe") or {})
    tiantian_preflight_status = _clean_text(tiantian_preflight_probe.get("status"))
    tiantian_preflight_message = _clean_text(tiantian_preflight_probe.get("message"))
    workflow_busy_guarded = any(
        reason in {"workflow_busy", "workflow_busy_locked", "workflow_busy_state_only"}
        for reason in (maintenance_rescreen_skipped_reason, report_refresh_skipped_reason)
        if reason
    )

    if status == "failed":
        impact_code = "task_failed_review_required"
    elif screening_relevant_empty_detail_count > 0:
        impact_code = "screening_relevant_detail_gap_auto_retry"
    elif remaining_tiantian_gap > 0 or remaining_active_tiantian_gap > 0:
        impact_code = "universe_gap_watch"
    elif workflow_busy_guarded:
        impact_code = "workflow_busy_guarded_no_manual_required"
    else:
        impact_code = "no_current_publish_impact_detected"

    manual_intervention_required = bool(
        status == "failed"
        or impact_code == "task_failed_review_required"
        or (category == "local_maintenance_failed" and not workflow_busy_guarded)
    )
    auto_recovery_policy = (
        "retry_on_next_schedule"
        if category == "upstream_api_unavailable"
        else "scheduler_guarded_workflow_retry"
        if workflow_busy_guarded
        else "inspect_detail_and_retry_automatically"
        if category in {"local_maintenance_failed", "workflow_refresh_failed"}
        else "monitor_next_run"
    )
    artifacts = copy_task_artifact_fields(payload)
    nav_archive_resolution = _nav_history_archive_preview_resolution_evidence(
        payload,
        task_key=task_key,
        degraded_stage=degraded_stage,
        warning_reason=warning_reason,
        project_root=project_root,
    )
    rank_archive_resolution = _rank_history_archive_rolling_resolution_evidence(
        payload,
        task_key=task_key,
        degraded_stage=degraded_stage,
        warning_reason=warning_reason,
        project_root=project_root,
    )
    maintenance_resolution = (
        nav_archive_resolution
        if nav_archive_resolution.get("applicable")
        else rank_archive_resolution
    )
    root_error = _clean_text(
        payload.get("bond_universe_sync_error")
        or payload.get("error")
        or tiantian_preflight_message
        or maintenance_rescreen_error
        or report_refresh_error
    )
    detail_parts = [
        part
        for part in (
            degraded_stage,
            warning_reason,
            f"error={root_error}" if root_error else None,
            f"tiantian_preflight_status={tiantian_preflight_status}"
            if tiantian_preflight_status
            else None,
            f"screening_relevant_empty_detail={screening_relevant_empty_detail_count}"
            if screening_relevant_empty_detail_count
            else None,
            f"remaining_tiantian_gap={remaining_tiantian_gap}" if remaining_tiantian_gap else None,
            f"remaining_autostock_gap={remaining_autostock_gap}" if remaining_autostock_gap else None,
        )
        if part
    ]
    return {
        "available": True,
        "status": status or None,
        "task_key": task_key,
        "category": category,
        "degraded_stage": degraded_stage,
        "warning_reason": warning_reason,
        "root_error": root_error,
        "impact_code": impact_code,
        "manual_intervention_required": manual_intervention_required,
        "auto_recovery_policy": auto_recovery_policy,
        "blocking_current_publication": bool(
            impact_code == "task_failed_review_required"
        ),
        "workflow_busy_guarded": workflow_busy_guarded,
        "issue_detail": " / ".join(detail_parts) if detail_parts else status or "degraded",
        "records_processed": payload.get("records_processed"),
        "records_succeeded": payload.get("records_succeeded"),
        "records_failed": payload.get("records_failed"),
        "detail_records_processed": payload.get("detail_records_processed"),
        "detail_records_succeeded": payload.get("detail_records_succeeded"),
        "detail_records_failed": payload.get("detail_records_failed"),
        "detail_records_skipped": payload.get("detail_records_skipped"),
        "detail_collection_summary": payload.get("detail_collection_summary"),
        "detail_collection_summary_errors": payload.get("detail_collection_summary_errors"),
        "remaining_tiantian_gap": remaining_tiantian_gap,
        "remaining_autostock_gap": remaining_autostock_gap,
        "remaining_active_tiantian_gap": remaining_active_tiantian_gap,
        "remaining_active_autostock_gap": remaining_active_autostock_gap,
        "empty_detail_screening_relevant_count": screening_relevant_empty_detail_count,
        "empty_detail_non_blocking_count": non_blocking_empty_detail_count,
        "maintenance_changed": bool(maintenance_change.get("changed")),
        "nav_records_inserted": _to_int(maintenance_change.get("nav_records_inserted")),
        "nav_recovered_total": _to_int(maintenance_change.get("nav_recovered_total")),
        "long_term_annual_success_total": _to_int(
            maintenance_change.get("long_term_annual_success_total")
        ),
        "long_term_rank_success_total": _to_int(
            maintenance_change.get("long_term_rank_success_total")
        ),
        "maintenance_rescreen_error": maintenance_rescreen_error,
        "maintenance_rescreen_skipped_reason": maintenance_rescreen_skipped_reason,
        "report_refresh_error": report_refresh_error,
        "report_refresh_skipped_reason": report_refresh_skipped_reason,
        "task_run_audit_json_project_path": artifacts.get("task_run_audit_json_project_path"),
        "task_log_file_project_path": artifacts.get("task_log_file_project_path"),
        "nav_history_archive_resolution_evidence": nav_archive_resolution,
        "rank_history_archive_resolution_evidence": rank_archive_resolution,
        "root_cause_resolved_by_later_evidence": bool(
            maintenance_resolution.get("root_cause_resolved")
        ),
        "current_interpretation": maintenance_resolution.get("current_interpretation")
        if maintenance_resolution.get("applicable")
        else None,
    }


def _apply_current_recovery_to_run_summary(
    run: dict[str, Any] | None,
    *,
    current_task_key: str,
    current_task_name: str | None,
    current_task_started_at: str | None,
) -> dict[str, Any]:
    payload = dict(run or {})
    if not payload:
        return payload
    status = str(payload.get("status") or payload.get("run_semantic_status") or "").strip().lower()
    summary = dict(payload.get("degradation_summary") or {})
    manual_hint = bool(payload.get("manual_intervention_hint"))
    historical_manual_required = summary.get("manual_intervention_required")
    if status not in {"failed", "degraded"} and not manual_hint and historical_manual_required is not True:
        return payload

    payload["current_recovery_in_progress"] = True
    payload["current_recovery_scope"] = "same_task_recovery"
    payload["current_recovery_task_key"] = current_task_key
    payload["current_recovery_task_name"] = current_task_name or SCHEDULER_TASK_NAME_BY_KEY.get(current_task_key)
    payload["current_recovery_started_at"] = current_task_started_at
    payload["historical_status"] = payload.get("status")
    payload["historical_run_semantic_status"] = payload.get("run_semantic_status")
    payload["historical_manual_intervention_hint"] = manual_hint
    payload["manual_intervention_hint"] = False

    if summary:
        if "manual_intervention_required_historical" not in summary:
            summary["manual_intervention_required_historical"] = summary.get("manual_intervention_required")
        if "blocking_current_publication_historical" not in summary:
            summary["blocking_current_publication_historical"] = summary.get("blocking_current_publication")
        summary["manual_intervention_required"] = False
        summary["blocking_current_publication"] = False
        summary["current_recovery_in_progress"] = True
        summary["current_recovery_scope"] = "same_task_recovery"
        summary["current_recovery_task_key"] = current_task_key
        summary["current_recovery_task_name"] = payload["current_recovery_task_name"]
        summary["current_recovery_started_at"] = current_task_started_at
        summary["current_interpretation"] = "historical_issue_is_currently_recovering_automatically"
        payload["degradation_summary"] = summary
    return payload


def apply_current_recovery_overlay_to_scheduler_payload(
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    result = dict(payload or {})
    automation = dict(result.get("automation_autonomy") or {})
    if (
        automation.get("automatic_recovering") is not True
        or automation.get("manual_intervention_required") is True
    ):
        return result

    current_task = dict(result.get("current_task") or {})
    current_task_name = str(
        automation.get("current_task_name")
        or current_task.get("task_name")
        or ""
    ).strip()
    current_task_key = str(automation.get("current_task_key") or "").strip()
    if not current_task_key:
        current_task_key = scheduler_task_key_for_task_name(current_task_name) or ""
    if current_task_key not in SCHEDULER_OVERVIEW_RUN_KEYS:
        return result

    run_field = SCHEDULER_OVERVIEW_RUN_KEYS[current_task_key]
    updated_run = _apply_current_recovery_to_run_summary(
        result.get(run_field) or {},
        current_task_key=current_task_key,
        current_task_name=current_task_name,
        current_task_started_at=str(current_task.get("started_at") or "").strip() or None,
    )
    if updated_run:
        result[run_field] = updated_run

    chain_audit = dict(result.get("scheduler_chain_audit") or {})
    chain_runs = dict(chain_audit.get("runs") or {})
    if chain_runs.get(current_task_key):
        chain_runs[current_task_key] = _apply_current_recovery_to_run_summary(
            chain_runs.get(current_task_key) or {},
            current_task_key=current_task_key,
            current_task_name=current_task_name,
            current_task_started_at=str(current_task.get("started_at") or "").strip() or None,
        )
        chain_audit["runs"] = chain_runs
        result["scheduler_chain_audit"] = chain_audit
    return result


def normalize_scheduler_detail_sample_limit(
    sample_limit: Any,
    *,
    default: int = DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
    max_limit: int = MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT,
) -> int:
    try:
        parsed = int(sample_limit)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(0, min(int(max_limit), parsed))


def reduce_scheduler_detail_samples(
    payload: dict[str, Any] | None,
    *,
    sample_limit: Any = DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
    include_full_samples: bool = False,
) -> dict[str, Any]:
    normalized_limit = normalize_scheduler_detail_sample_limit(sample_limit)
    if include_full_samples:
        full_payload = deepcopy(payload or {})
        full_payload["detail_sample_policy"] = {
            "mode": "full",
            "include_full_samples": True,
            "sample_limit": None,
            "max_sample_limit": MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT,
        }
        return full_payload

    def _reduce(value: Any, *, in_sample_container: bool = False) -> Any:
        if isinstance(value, dict):
            compact: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key_is_sample_container = "sample" in str(raw_key).lower()
                if key_is_sample_container and isinstance(raw_value, (list, dict)) and normalized_limit <= 0:
                    continue
                compact[raw_key] = _reduce(
                    raw_value,
                    in_sample_container=in_sample_container or key_is_sample_container,
                )
            return compact
        if isinstance(value, list):
            items = value
            if in_sample_container:
                items = items[:normalized_limit]
            return [_reduce(item, in_sample_container=False) for item in items]
        return value

    reduced = _reduce(payload or {})
    reduced["detail_sample_policy"] = {
        "mode": "sample_limited" if normalized_limit > 0 else "sample_omitted",
        "include_full_samples": False,
        "sample_limit": normalized_limit,
        "max_sample_limit": MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT,
    }
    return reduced


def build_task_run_artifacts_summary(
    scheduler_state: dict[str, Any] | None,
) -> dict[str, Any]:
    state = dict(scheduler_state or {})
    last_runs = dict(state.get("last_runs") or {})
    task_closures = dict(state.get("task_closures") or {})
    ordered_tasks: list[dict[str, Any]] = []
    task_map: dict[str, Any] = {}

    for task_key in SCHEDULER_CLOSURE_TASK_ORDER:
        task_name = SCHEDULER_TASK_NAME_BY_KEY.get(task_key) or task_key
        latest_run = dict(last_runs.get(task_name) or {})
        task_closure = dict(task_closures.get(task_key) or {})
        artifact_fields = copy_task_artifact_fields(latest_run, task_closure)
        if not artifact_fields:
            continue

        task_payload = {
            "task_key": task_key,
            "task_name": latest_run.get("task_name") or task_closure.get("task_name") or task_name,
            "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
            "status": latest_run.get("status") or task_closure.get("status"),
            "started_at": latest_run.get("started_at") or task_closure.get("started_at"),
            "finished_at": latest_run.get("finished_at") or task_closure.get("finished_at"),
            **copy_task_timing_fields(latest_run, task_closure),
            "workflow_run_id": (
                latest_run.get("workflow_run_id")
                or latest_run.get("screening_run_id")
                or latest_run.get("maintenance_rescreen_run_id")
                or latest_run.get("report_refresh_run_id")
                or latest_run.get("initial_screening_run_id")
                or task_closure.get("screening_run_id")
                or task_closure.get("maintenance_rescreen_run_id")
                or task_closure.get("report_refresh_run_id")
                or task_closure.get("initial_screening_run_id")
            ),
            **artifact_fields,
        }
        ordered_tasks.append(task_payload)
        task_map[task_key] = task_payload

    ordered_tasks.sort(
        key=lambda item: str(item.get("finished_at") or item.get("started_at") or ""),
        reverse=True,
    )
    latest_task = ordered_tasks[0] if ordered_tasks else {}
    return {
        "summary": {
            "task_count": len(ordered_tasks),
            "artifact_task_count": len(ordered_tasks),
            "latest_task_key": latest_task.get("task_key"),
            "latest_task_name": latest_task.get("task_name"),
            "latest_finished_at": latest_task.get("finished_at"),
        },
        "ordered_tasks": ordered_tasks,
        "task_map": task_map,
    }


def build_recent_scheduler_failure_recovery_summary(
    settings: Any,
    *,
    lookback_days: int = 2,
    max_runs_per_task: int = 80,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Summarize recent scheduler audit files into failure/recovery streaks."""

    audit_root = Path(getattr(settings, "reports_dir", Path("reports"))) / ".audit" / "scheduler_task_runs"
    task_name_to_key = {value: key for key, value in SCHEDULER_TASK_NAME_BY_KEY.items()}
    current_time = now or datetime.now()
    lookback_start = current_time - timedelta(days=max(1, int(lookback_days or 1)))

    def _read_audit(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(read_task_audit_bytes(path).decode("utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _project_path(path: Path) -> str:
        project_root = Path(getattr(settings, "project_root", Path.cwd()))
        try:
            return str(path.resolve().relative_to(project_root.resolve()))
        except (OSError, ValueError):
            return str(path)

    def _run_datetime(run: dict[str, Any]) -> datetime | None:
        return (
            parse_local_datetime(run.get("started_at"))
            or parse_local_datetime(run.get("finished_at"))
            or parse_local_datetime(run.get("generated_at"))
        )

    def _logs_tail_lines(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item or "") for item in value]
        if isinstance(value, str):
            return value.splitlines()
        return []

    def _extract_exit_code(text: Any) -> int | None:
        match = re.search(r"\bexit\s*[= ]\s*(\d+)\b", str(text or ""), flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    def _extract_workflow_failure(error_text: Any) -> tuple[str | None, int | None]:
        text = str(error_text or "")
        match = re.search(r"步骤失败:\s*([^(]+?)\s*\(exit\s*=\s*(\d+)\)", text)
        if match:
            stage = re.sub(r"\s+", " ", match.group(1)).strip()
            try:
                return stage or None, int(match.group(2))
            except (TypeError, ValueError):
                return stage or None, None
        return None, _extract_exit_code(text)

    def _extract_failed_subtask(logs_tail: Any) -> dict[str, Any]:
        for raw_line in _logs_tail_lines(logs_tail):
            line = str(raw_line or "")
            if "任务" not in line or ("失败" not in line and "❌" not in line):
                continue
            label_match = re.search(r"(任务\s*\d+\s*:\s*.+?)\s+(?:❌|失败)", line)
            if not label_match:
                continue
            duration_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*秒", line)
            exit_match = re.search(r"秒\s+(\d+)\s*$", line)
            label = re.sub(r"\s+", " ", label_match.group(1)).strip()
            duration_seconds = None
            if duration_match:
                try:
                    duration_seconds = float(duration_match.group(1))
                except (TypeError, ValueError):
                    duration_seconds = None
            exit_code = None
            if exit_match:
                try:
                    exit_code = int(exit_match.group(1))
                except (TypeError, ValueError):
                    exit_code = None
            return {
                "label": label or None,
                "duration_seconds": duration_seconds,
                "exit_code": exit_code,
            }
        return {}

    def _run_payload_from_audit(path: Path, audit: dict[str, Any]) -> dict[str, Any]:
        task = dict(audit.get("task") or {})
        result = dict(audit.get("task_result") or {})
        workflow = dict(audit.get("workflow_summary") or {})
        task_name = str(task.get("task_name") or result.get("task_name") or "").strip()
        task_key = str(task.get("task_key") or "").strip() or task_name_to_key.get(task_name) or task_name
        started_at = task.get("started_at") or result.get("started_at") or workflow.get("started_at")
        finished_at = task.get("finished_at") or result.get("finished_at") or workflow.get("finished_at")
        status = str(task.get("status") or result.get("status") or workflow.get("status") or "").strip().lower()
        if status == "succeeded":
            status = "success"
        error_text = str(
            result.get("error")
            or workflow.get("error")
            or result.get("warning_reason")
            or result.get("skip_reason")
            or ""
        ).strip()
        task_workflow_run_id = _task_bound_workflow_run_id(result)
        screening_run_id = str(
            result.get("screening_run_id")
            or result.get("maintenance_rescreen_run_id")
            or result.get("report_refresh_run_id")
            or result.get("initial_screening_run_id")
            # workflow_summary is a shared observation, not task provenance.
            # An absent task-bound run must remain unknown even if a workflow
            # happened to be visible when the audit was written.
            or ""
        ).strip()
        workflow_run_id = task_workflow_run_id or ""
        workflow_error = str(workflow.get("error") or "").strip()
        workflow_failed_stage, workflow_failure_exit_code = _extract_workflow_failure(workflow_error)
        task_failure_exit_code = _extract_exit_code(result.get("error"))
        failed_subtask = _extract_failed_subtask(workflow.get("logs_tail") or result.get("logs_tail"))
        failed_subtask_exit_code = failed_subtask.get("exit_code")
        failure_exit_code = failed_subtask_exit_code
        if failure_exit_code is None:
            failure_exit_code = workflow_failure_exit_code
        if failure_exit_code is None:
            failure_exit_code = task_failure_exit_code
        return {
            "task_key": task_key,
            "task_name": task_name or SCHEDULER_TASK_NAME_BY_KEY.get(task_key) or task_key,
            "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "generated_at": audit.get("generated_at"),
            **copy_task_timing_fields(result),
            "screening_run_id": screening_run_id or None,
            "workflow_run_id": workflow_run_id or None,
            "screening_reason": result.get("screening_reason"),
            "screening_triggered": bool(result.get("screening_triggered")),
            "skip_reason": result.get("skip_reason"),
            "warning_reason": result.get("warning_reason"),
            "degraded_stage": result.get("degraded_stage"),
            "error": error_text or None,
            "workflow_error": workflow_error or None,
            "workflow_failed_stage": workflow_failed_stage,
            "workflow_failure_exit_code": workflow_failure_exit_code,
            "failed_subtask": failed_subtask.get("label"),
            "failed_subtask_duration_seconds": failed_subtask.get("duration_seconds"),
            "failed_subtask_exit_code": failed_subtask_exit_code,
            "failure_exit_code": failure_exit_code,
            "workflow_options": dict(workflow.get("options") or {}),
            "artifact_project_path": _project_path(path),
            "_sort_dt": (
                parse_local_datetime(started_at)
                or parse_local_datetime(finished_at)
                or parse_local_datetime(audit.get("generated_at"))
            ),
        }

    runs_by_task: dict[str, list[dict[str, Any]]] = {}
    scanned_run_total = 0
    history_source = ""
    lookback_days_by_task: dict[str, int] = {}
    database_path_value = getattr(settings, "database_path", None)
    database_path = Path(database_path_value).expanduser() if database_path_value else None
    if database_path is not None and database_path.exists() and database_path.is_file():
        requested_lookback_days = max(1, int(lookback_days or 1))
        lookback_days_by_task = {
            "daily_refresh": max(14, requested_lookback_days),
            "intraday_refresh": max(14, requested_lookback_days),
            "weekly_maintenance": max(56, requested_lookback_days),
            "snapshot_watchdog": max(2, requested_lookback_days),
        }
        try:
            database_uri = f"file:{quote(str(database_path.resolve()))}?mode=ro"
            with sqlite3.connect(database_uri, uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                task_history_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_history'"
                ).fetchone()
                if task_history_exists:
                    for task_key in SCHEDULER_CLOSURE_TASK_ORDER:
                        task_name = SCHEDULER_TASK_NAME_BY_KEY[task_key]
                        task_lookback_start = current_time - timedelta(
                            days=lookback_days_by_task[task_key]
                        )
                        rows = conn.execute(
                            f"""
                            SELECT id, task_name, status, started_at, completed_at,
                                   error_message, extra_data
                            FROM task_history
                            WHERE task_name = ? AND {SCHEDULER_EXECUTION_ROW_SQL} AND started_at >= ?
                            ORDER BY started_at DESC, id DESC
                            LIMIT ?
                            """,
                            (
                                task_name,
                                task_lookback_start.strftime("%Y-%m-%d %H:%M:%S"),
                                max(1, int(max_runs_per_task or 1)),
                            ),
                        ).fetchall()
                        for row in reversed(rows):
                            try:
                                extra = json.loads(row["extra_data"] or "{}")
                            except (json.JSONDecodeError, TypeError, ValueError):
                                extra = {}
                            if not isinstance(extra, dict):
                                extra = {}
                            status = str(extra.get("status") or row["status"] or "").strip().lower()
                            if status == "succeeded":
                                status = "success"
                            error_text = str(
                                extra.get("error")
                                or row["error_message"]
                                or extra.get("warning_reason")
                                or extra.get("skip_reason")
                                or ""
                            ).strip()
                            workflow_error = str(extra.get("workflow_error") or "").strip()
                            workflow_failed_stage, workflow_failure_exit_code = (
                                _extract_workflow_failure(workflow_error or error_text)
                            )
                            degraded_stage = str(extra.get("degraded_stage") or "").strip()
                            started_at = extra.get("started_at") or row["started_at"]
                            finished_at = extra.get("finished_at") or row["completed_at"]
                            screening_run_id = str(
                                extra.get("screening_run_id")
                                or extra.get("maintenance_rescreen_run_id")
                                or ""
                            ).strip()
                            workflow_run_id = str(extra.get("workflow_run_id") or "").strip()
                            artifact_project_path = str(
                                extra.get("task_run_audit_json_project_path")
                                or extra.get("task_run_audit_path_project_path")
                                or extra.get("task_run_audit_path")
                                or ""
                            ).strip()
                            run_payload = {
                                "task_key": task_key,
                                "task_name": task_name,
                                "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
                                "status": status,
                                "started_at": started_at,
                                "finished_at": finished_at,
                                **copy_task_timing_fields(extra),
                                "screening_run_id": screening_run_id or None,
                                "workflow_run_id": workflow_run_id or None,
                                "screening_reason": extra.get("screening_reason"),
                                "screening_triggered": bool(extra.get("screening_triggered")),
                                "skip_reason": extra.get("skip_reason"),
                                "warning_reason": extra.get("warning_reason"),
                                "degraded_stage": degraded_stage or None,
                                "error": error_text or None,
                                "workflow_error": workflow_error or None,
                                "workflow_failed_stage": workflow_failed_stage or degraded_stage or None,
                                "workflow_failure_exit_code": workflow_failure_exit_code,
                                "failed_subtask": None,
                                "failed_subtask_duration_seconds": None,
                                "failed_subtask_exit_code": None,
                                "failure_exit_code": (
                                    workflow_failure_exit_code
                                    if workflow_failure_exit_code is not None
                                    else _extract_exit_code(error_text)
                                ),
                                "workflow_options": dict(extra.get("workflow_options") or {}),
                                "artifact_project_path": artifact_project_path or None,
                                "task_history_id": int(row["id"] or 0),
                                "_sort_dt": (
                                    parse_local_datetime(started_at)
                                    or parse_local_datetime(finished_at)
                                ),
                            }
                            runs_by_task.setdefault(task_key, []).append(run_payload)
                            scanned_run_total += 1
        except sqlite3.Error:
            runs_by_task = {}
            scanned_run_total = 0

    if runs_by_task:
        history_source = "task_history"
    elif audit_root.exists():
        history_source = "scheduler_task_audit_artifacts"
        cutoff_day = lookback_start.strftime("%Y%m%d")
        for day_dir in sorted(item for item in audit_root.iterdir() if item.is_dir()):
            if day_dir.name.isdigit() and day_dir.name < cutoff_day:
                continue
            for path in sorted([*day_dir.glob("*.json"), *day_dir.glob("*.json.gz")]):
                if path.name.endswith(".json.gz"):
                    original = path.with_suffix("")
                    if original.exists() or original.is_symlink():
                        continue  # An interrupted migration must not duplicate a task.
                    if Path(f"{path}.receipt").exists():
                        path = original  # Preserve the historical artifact identity.
                audit = _read_audit(path)
                if not audit:
                    continue
                run_payload = _run_payload_from_audit(path, audit)
                run_dt = _run_datetime(run_payload)
                if run_dt is not None and run_dt < lookback_start:
                    continue
                task_key = str(run_payload.get("task_key") or "").strip()
                if not task_key:
                    continue
                runs_by_task.setdefault(task_key, []).append(run_payload)
                scanned_run_total += 1
    else:
        return {
            "available": False,
            "reason": "scheduler_task_history_and_audit_root_missing",
            "audit_root": str(audit_root),
            "lookback_days": int(lookback_days or 0),
            "scanned_run_total": 0,
            "tasks": [],
            "task_map": {},
            "summary": {
                "task_count": 0,
                "recovered_sequence_total": 0,
                "open_failure_sequence_total": 0,
                "open_degradation_sequence_total": 0,
                "recent_failure_run_total": 0,
                "recent_degraded_run_total": 0,
                "attention_task_total": 0,
            },
        }

    task_map: dict[str, Any] = {}
    tasks: list[dict[str, Any]] = []
    summary = {
        "task_count": 0,
        "recovered_sequence_total": 0,
        "open_failure_sequence_total": 0,
        "open_degradation_sequence_total": 0,
        "recent_failure_run_total": 0,
        "recent_degraded_run_total": 0,
        "attention_task_total": 0,
    }

    def _counter_items(values: list[Any]) -> list[dict[str, Any]]:
        counter = Counter(value for value in values if value not in (None, ""))
        return [{"value": value, "count": count} for value, count in counter.most_common()]

    def _numeric_values(values: list[Any]) -> list[float]:
        numbers: list[float] = []
        for value in values:
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            numbers.append(number)
        return numbers

    def _failure_sequence_summary(
        failure_runs: list[dict[str, Any]],
        *,
        status: str,
        recovery_run: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not failure_runs:
            return {}
        first_failure = failure_runs[0]
        last_failure = failure_runs[-1]
        failed_run_count = sum(
            1 for run in failure_runs
            if str(run.get("status") or "").strip().lower() == "failed"
        )
        degraded_run_count = sum(
            1 for run in failure_runs
            if str(run.get("status") or "").strip().lower() == "degraded"
        )
        workflow_run_ids = [
            str(run.get("workflow_run_id") or run.get("screening_run_id") or "").strip()
            for run in failure_runs
            if str(run.get("workflow_run_id") or run.get("screening_run_id") or "").strip()
        ]
        failed_subtask_durations = _numeric_values(
            [run.get("failed_subtask_duration_seconds") for run in failure_runs]
        )
        sequence = {
            "status": status,
            "failure_count": len(failure_runs),
            "failure_like_run_count": len(failure_runs),
            "failed_run_count": failed_run_count,
            "degraded_run_count": degraded_run_count,
            "latest_failure_status": str(last_failure.get("status") or "").strip().lower(),
            "first_failed_at": first_failure.get("started_at"),
            "last_failed_at": last_failure.get("finished_at") or last_failure.get("started_at"),
            "failure_workflow_run_ids": workflow_run_ids,
            "latest_failure_workflow_run_id": last_failure.get("workflow_run_id")
            or last_failure.get("screening_run_id"),
            "latest_failure_error": last_failure.get("error"),
            "latest_failure_workflow_error": last_failure.get("workflow_error"),
            "latest_failed_stage": (
                last_failure.get("degraded_stage")
                or last_failure.get("workflow_failed_stage")
            ),
            "latest_failed_subtask": last_failure.get("failed_subtask"),
            "latest_failure_exit_code": last_failure.get("failure_exit_code"),
            "latest_failed_subtask_duration_seconds": last_failure.get("failed_subtask_duration_seconds"),
            "failure_error_counts": _counter_items([run.get("error") for run in failure_runs]),
            "failure_status_counts": _counter_items(
                [str(run.get("status") or "").strip().lower() for run in failure_runs]
            ),
            "workflow_error_counts": _counter_items([run.get("workflow_error") for run in failure_runs]),
            "failed_stage_counts": _counter_items([run.get("workflow_failed_stage") for run in failure_runs]),
            "failed_subtask_counts": _counter_items([run.get("failed_subtask") for run in failure_runs]),
            "failure_exit_code_counts": _counter_items([run.get("failure_exit_code") for run in failure_runs]),
            "latest_failure_artifact_project_path": last_failure.get("artifact_project_path"),
        }
        if failed_subtask_durations:
            sequence.update(
                {
                    "min_failed_subtask_duration_seconds": min(failed_subtask_durations),
                    "max_failed_subtask_duration_seconds": max(failed_subtask_durations),
                    "avg_failed_subtask_duration_seconds": round(
                        sum(failed_subtask_durations) / len(failed_subtask_durations),
                        3,
                    ),
                }
            )
        if recovery_run is not None:
            sequence.update(
                {
                    "recovered_at": recovery_run.get("finished_at") or recovery_run.get("started_at"),
                    "recovery_status": recovery_run.get("status"),
                    "recovery_screening_run_id": recovery_run.get("screening_run_id"),
                    "recovery_workflow_run_id": recovery_run.get("workflow_run_id")
                    or recovery_run.get("screening_run_id"),
                    "recovery_screening_reason": recovery_run.get("screening_reason"),
                    "recovery_workflow_options": recovery_run.get("workflow_options") or {},
                    "recovery_artifact_project_path": recovery_run.get("artifact_project_path"),
                }
            )
        return sequence

    for task_key in SCHEDULER_CLOSURE_TASK_ORDER:
        runs = list(runs_by_task.get(task_key) or [])
        if not runs:
            continue
        runs.sort(key=lambda item: item.get("_sort_dt") or datetime.min)
        runs = runs[-max(1, int(max_runs_per_task or 1)):]
        failure_streak: list[dict[str, Any]] = []
        recovered_sequences: list[dict[str, Any]] = []
        recent_failure_run_total = 0
        recent_degraded_run_total = 0
        for run in runs:
            status = str(run.get("status") or "").strip().lower()
            if status in {"failed", "degraded"}:
                failure_streak.append(run)
                if status == "failed":
                    recent_failure_run_total += 1
                else:
                    recent_degraded_run_total += 1
                continue
            if status == "success" and failure_streak:
                recovered_sequences.append(
                    _failure_sequence_summary(failure_streak, status="recovered", recovery_run=run)
                )
                failure_streak = []
                continue
            if status in {"success", "skipped"}:
                failure_streak = []

        latest_run = runs[-1]
        latest_recovered_sequence = recovered_sequences[-1] if recovered_sequences else {}
        open_failure_sequence = {}
        if failure_streak:
            open_failure_sequence = _failure_sequence_summary(failure_streak, status="open")

        open_sequence_latest_status = str(
            open_failure_sequence.get("latest_failure_status") or ""
        ).strip().lower()
        attention_needed = bool(
            (
                open_failure_sequence
                and (
                    open_sequence_latest_status == "failed"
                    or int(open_failure_sequence.get("failure_count") or 0) >= 2
                )
            )
            or int((latest_recovered_sequence or {}).get("failure_count") or 0) >= 2
        )
        task_summary = {
            "task_key": task_key,
            "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
            "scanned_run_total": len(runs),
            "recent_failure_run_total": recent_failure_run_total,
            "recent_degraded_run_total": recent_degraded_run_total,
            "recovered_sequence_total": len(recovered_sequences),
            "latest_status": latest_run.get("status"),
            "latest_started_at": latest_run.get("started_at"),
            "latest_finished_at": latest_run.get("finished_at"),
            "latest_artifact_project_path": latest_run.get("artifact_project_path"),
            "latest_recovered_sequence": latest_recovered_sequence,
            "open_failure_sequence": open_failure_sequence,
            "attention_needed": attention_needed,
        }
        tasks.append(task_summary)
        task_map[task_key] = task_summary
        summary["task_count"] += 1
        summary["recovered_sequence_total"] += len(recovered_sequences)
        summary["recent_failure_run_total"] += recent_failure_run_total
        summary["recent_degraded_run_total"] += recent_degraded_run_total
        if open_failure_sequence:
            summary["open_failure_sequence_total"] += 1
            if open_sequence_latest_status == "degraded":
                summary["open_degradation_sequence_total"] += 1
        if attention_needed:
            summary["attention_task_total"] += 1

    return {
        "available": True,
        "source": history_source,
        "audit_root": str(audit_root),
        "lookback_days": int(lookback_days or 0),
        "lookback_days_by_task": lookback_days_by_task,
        "scanned_run_total": scanned_run_total,
        "summary": summary,
        "tasks": tasks,
        "task_map": task_map,
    }


SCHEDULER_OFFICIAL_ENTRYPOINT = "scripts/run_scheduler.py"
SCHEDULER_OFFICIAL_MODULE = "backend/scheduler.py"
SCHEDULER_EMBEDDED_FALLBACK = "backend.app.ensure_embedded_scheduler"
REGRESSION_SMOKE_REPORT_ROOT = (
    Path(__file__).resolve().parent.parent / "reports" / "audit" / "regression_smoke"
)


def _resolve_regression_smoke_report_root(settings: Any | None) -> Path | None:
    current = settings
    reports_dir = getattr(current, "reports_dir", None)
    if reports_dir:
        return Path(reports_dir) / "audit" / "regression_smoke"
    project_root = getattr(current, "project_root", None)
    if project_root:
        return Path(project_root) / "reports" / "audit" / "regression_smoke"
    return None


def scheduler_snapshot_signature(snapshot: dict[str, Any] | None) -> tuple[str, str, int, str]:
    payload = dict(snapshot or {})
    return (
        str(payload.get("candidate_date") or "").strip(),
        str(payload.get("signature") or "").strip(),
        int(payload.get("candidate_fund_count") or 0),
        str(payload.get("readiness_reason") or "").strip(),
    )


def scheduler_snapshot_has_payload(snapshot: dict[str, Any] | None) -> bool:
    payload = dict(snapshot or {})
    return bool(
        payload
        and (
            str(payload.get("candidate_date") or "").strip()
            or str(payload.get("signature") or "").strip()
            or int(payload.get("candidate_fund_count") or 0) > 0
            or str(payload.get("readiness_reason") or "").strip()
            or "ready" in payload
        )
    )


def _normalize_regression_smoke_summary(
    raw_summary: dict[str, Any] | None,
    *,
    summary_path: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    payload = dict(raw_summary or {})
    if not payload:
        return {}
    groups = list(payload.get("groups") or payload.get("selected_groups") or [])
    group_results = list(payload.get("group_results") or [])
    passed_group_total = payload.get("passed_group_total")
    failed_group_total = payload.get("failed_group_total")
    if passed_group_total is None:
        passed_group_total = sum(
            1 for item in group_results if str((item or {}).get("status") or "").strip().lower() == "passed"
        )
    if failed_group_total is None:
        failed_group_total = sum(
            1 for item in group_results if str((item or {}).get("status") or "").strip().lower() == "failed"
        )
    overall_status = str(payload.get("overall_status") or payload.get("status") or "").strip() or None
    status = str(payload.get("status") or "").strip() or None
    if not status and overall_status:
        normalized_overall = overall_status.lower()
        if normalized_overall in {"passed", "success"}:
            status = "success"
        elif normalized_overall in {"failed", "error"}:
            status = "failed"
        else:
            status = overall_status
    artifact_dir = str(payload.get("artifact_dir") or "").strip() or None
    resolved_summary_path = (
        str(summary_path or "").strip()
        or str(payload.get("summary_path") or "").strip()
        or (str(Path(artifact_dir) / "summary.json") if artifact_dir else None)
    )
    resolved_source = str(source or payload.get("source") or "").strip() or None
    replay_precision_status = str(payload.get("replay_precision_status") or "").strip() or None
    replay_precision_status_label = str(payload.get("replay_precision_status_label") or "").strip() or None
    auxiliary_scope = str(payload.get("auxiliary_scope") or "").strip() or None
    auxiliary_scope_label = str(payload.get("auxiliary_scope_label") or "").strip() or None
    scope_detail = str(payload.get("scope_detail") or "").strip() or None
    issue_classification_status = str(payload.get("issue_classification_status") or "").strip() or None
    issue_classification_label = str(payload.get("issue_classification_label") or "").strip() or None
    issue_category_counts = dict(payload.get("issue_category_counts") or {})
    failed_group_classifications = list(payload.get("failed_group_classifications") or [])
    if resolved_source == "latest_artifact_fallback":
        replay_precision_status = replay_precision_status or "latest_only"
        replay_precision_status_label = replay_precision_status_label or "仅 latest 审计"
        auxiliary_scope = auxiliary_scope or "latest_audit_only"
        auxiliary_scope_label = auxiliary_scope_label or "当前最新审计"
        scope_detail = (
            scope_detail
            or "当前摘要来自最新 regression smoke 审计产物，不提供历史 run 级别回放。"
        )
    normalized = {
        "status": status,
        "overall_status": overall_status,
        "passed_group_total": int(passed_group_total or 0),
        "failed_group_total": int(failed_group_total or 0),
        "issue_classification_status": issue_classification_status,
        "issue_classification_label": issue_classification_label,
        "issue_category_counts": issue_category_counts,
        "failed_command_total": int(payload.get("failed_command_total") or 0),
        "failed_group_classifications": failed_group_classifications,
        "expected_warning_total": int(payload.get("expected_warning_total") or 0),
        "test_expectation_mismatch_total": int(payload.get("test_expectation_mismatch_total") or 0),
        "production_degradation_total": int(payload.get("production_degradation_total") or 0),
        "environment_issue_total": int(payload.get("environment_issue_total") or 0),
        "unknown_failure_total": int(payload.get("unknown_failure_total") or 0),
        "groups": groups,
        "group_results": group_results,
        "summary_path": resolved_summary_path,
        "artifact_dir": artifact_dir,
        "exit_code": payload.get("exit_code"),
        "generated_at": str(payload.get("generated_at") or "").strip() or None,
        "source": resolved_source,
        "replay_precision_status": replay_precision_status,
        "replay_precision_status_label": replay_precision_status_label,
        "auxiliary_scope": auxiliary_scope,
        "auxiliary_scope_label": auxiliary_scope_label,
        "scope_detail": scope_detail,
    }
    if not any(
        [
            normalized.get("status"),
            normalized.get("overall_status"),
            normalized.get("groups"),
            normalized.get("group_results"),
            normalized.get("summary_path"),
            normalized.get("artifact_dir"),
            normalized.get("generated_at"),
        ]
    ):
        return {}
    return normalized


def _build_regression_smoke_child_task(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = _normalize_regression_smoke_summary(summary)
    processed_total = int(payload.get("passed_group_total") or 0) + int(payload.get("failed_group_total") or 0)
    candidate_total = len(payload.get("groups") or [])
    return {
        "status": payload.get("status"),
        "skip_reason": None,
        "idle_reason": None,
        "no_action_category": None,
        "candidate_total": candidate_total,
        "processed_total": processed_total,
        "changed_total": int(payload.get("passed_group_total") or 0),
        "failed_reason_summary": {},
        "failed_reason_governance_summary": {},
        "upstream_observation_summary": {},
        "priority_blocked_summary": {},
        "coverage_progress": {},
        "overall_status": payload.get("overall_status"),
        "issue_classification_status": payload.get("issue_classification_status"),
        "issue_classification_label": payload.get("issue_classification_label"),
        "issue_category_counts": payload.get("issue_category_counts") or {},
        "failed_command_total": int(payload.get("failed_command_total") or 0),
        "failed_group_classifications": payload.get("failed_group_classifications") or [],
        "expected_warning_total": int(payload.get("expected_warning_total") or 0),
        "test_expectation_mismatch_total": int(payload.get("test_expectation_mismatch_total") or 0),
        "production_degradation_total": int(payload.get("production_degradation_total") or 0),
        "environment_issue_total": int(payload.get("environment_issue_total") or 0),
        "unknown_failure_total": int(payload.get("unknown_failure_total") or 0),
        "summary_path": payload.get("summary_path"),
        "artifact_dir": payload.get("artifact_dir"),
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _summarize_regression_smoke_summary_for_api(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = _normalize_regression_smoke_summary(summary)
    if not payload:
        return {}
    return {
        "status": payload.get("status"),
        "overall_status": payload.get("overall_status"),
        "issue_classification_status": payload.get("issue_classification_status"),
        "issue_classification_label": payload.get("issue_classification_label"),
        "issue_category_counts": payload.get("issue_category_counts") or {},
        "failed_command_total": int(payload.get("failed_command_total") or 0),
        "failed_group_classifications": payload.get("failed_group_classifications") or [],
        "expected_warning_total": int(payload.get("expected_warning_total") or 0),
        "test_expectation_mismatch_total": int(payload.get("test_expectation_mismatch_total") or 0),
        "production_degradation_total": int(payload.get("production_degradation_total") or 0),
        "environment_issue_total": int(payload.get("environment_issue_total") or 0),
        "unknown_failure_total": int(payload.get("unknown_failure_total") or 0),
        "passed_group_total": int(payload.get("passed_group_total") or 0),
        "failed_group_total": int(payload.get("failed_group_total") or 0),
        "groups": list(payload.get("groups") or []),
        "group_results": list(payload.get("group_results") or []),
        "summary_path": payload.get("summary_path"),
        "artifact_dir": payload.get("artifact_dir"),
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _summarize_long_term_upstream_observation_summary_for_api(
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    if not payload:
        return {}
    pending_reason_counts = payload.get("pending_reason_counts") or {}
    pending_recovery_samples = payload.get("pending_recovery_samples") or []
    pending_retry_window_total = int(
        payload.get("pending_retry_window_total")
        or pending_reason_counts.get("local_retry_window_pending")
        or 0
    )
    pending_retry_cooldown_total = int(
        payload.get("pending_retry_cooldown_total")
        or pending_reason_counts.get("local_retry_cooldown_pending")
        or 0
    )
    pending_retry_ready_total = int(
        payload.get("pending_retry_ready_total")
        or pending_reason_counts.get("local_retry_pending")
        or 0
    ) + int(pending_reason_counts.get("local_previous_failure_pending_recheck") or 0)
    fallback_next_retry_values = sorted(
        str((item or {}).get("cooldown_next_retry_after") or "").strip()
        for item in pending_recovery_samples
        if str((item or {}).get("cooldown_next_retry_after") or "").strip()
    )
    return {
        "pending_recovery_total": int(payload.get("pending_recovery_total") or 0),
        "pending_retry_window_total": pending_retry_window_total,
        "pending_retry_cooldown_total": pending_retry_cooldown_total,
        "pending_retry_ready_total": pending_retry_ready_total,
        "pending_next_retry_after_min": (
            payload.get("pending_next_retry_after_min")
            or (fallback_next_retry_values[0] if fallback_next_retry_values else None)
        ),
        "pending_next_retry_after_max": (
            payload.get("pending_next_retry_after_max")
            or (fallback_next_retry_values[-1] if fallback_next_retry_values else None)
        ),
        "upstream_limited_watchlist_total": int(payload.get("upstream_limited_watchlist_total") or 0),
        "stale_upstream_limited_watchlist_total": int(
            payload.get("stale_upstream_limited_watchlist_total") or 0
        ),
        "terminal_upstream_limited_watchlist_total": int(
            payload.get("terminal_upstream_limited_watchlist_total") or 0
        ),
        "scope_filtered_upstream_limited_total": int(
            payload.get("scope_filtered_upstream_limited_total") or 0
        ),
        "upstream_limited_total": int(payload.get("upstream_limited_total") or 0),
        "upstream_limited_empty_payload_total": int(
            payload.get("upstream_limited_empty_payload_total") or 0
        ),
        "pending_reason_counts": payload.get("pending_reason_counts") or {},
        "pending_subtype_counts": payload.get("pending_subtype_counts") or {},
        "upstream_limited_reason_counts": payload.get("upstream_limited_reason_counts") or {},
        "stale_upstream_limited_reason_counts": payload.get("stale_upstream_limited_reason_counts") or {},
        "terminal_upstream_limited_reason_counts": payload.get("terminal_upstream_limited_reason_counts")
        or {},
        "scope_filtered_upstream_limited_reason_counts": (
            payload.get("scope_filtered_upstream_limited_reason_counts") or {}
        ),
        "upstream_limited_subtype_counts": payload.get("upstream_limited_subtype_counts") or {},
        "stale_upstream_limited_subtype_counts": (
            payload.get("stale_upstream_limited_subtype_counts") or {}
        ),
        "terminal_upstream_limited_subtype_counts": (
            payload.get("terminal_upstream_limited_subtype_counts") or {}
        ),
        "scope_filtered_upstream_limited_subtype_counts": (
            payload.get("scope_filtered_upstream_limited_subtype_counts") or {}
        ),
    }


def _normalize_long_term_child_task_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    child = deepcopy(payload or {})
    upstream_observation_summary = child.get("upstream_observation_summary") or {}
    if upstream_observation_summary:
        child["upstream_observation_summary"] = _summarize_long_term_upstream_observation_summary_for_api(
            upstream_observation_summary
        )
    return child


def summarize_scheduler_maintenance_summary_for_api(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(summary or {})
    if not payload:
        return {}
    return {
        "metrics_source": deepcopy(payload.get("metrics_source") or {}),
        "nav_processed_total": optional_count(payload.get("nav_processed_total")),
        "nav_attempted_total": optional_count(payload.get("nav_attempted_total")),
        "nav_recovered_total": optional_count(payload.get("nav_recovered_total")),
        "nav_failed_total": optional_count(payload.get("nav_failed_total")),
        "nav_records_inserted": optional_count(payload.get("nav_records_inserted")),
        "long_term_status": payload.get("long_term_status"),
        "long_term_no_action_category": payload.get("long_term_no_action_category"),
        "long_term_processed_total": optional_count(payload.get("long_term_processed_total")),
        "long_term_changed_total": optional_count(payload.get("long_term_changed_total")),
        "long_term_annual_success_total": optional_count(payload.get("long_term_annual_success_total")),
        "long_term_rank_success_total": optional_count(payload.get("long_term_rank_success_total")),
        "long_term_failed_reason_summary": payload.get("long_term_failed_reason_summary") or {},
        "long_term_failed_reason_governance_summary": (
            payload.get("long_term_failed_reason_governance_summary") or {}
        ),
        "long_term_upstream_observation_summary": (
            _summarize_long_term_upstream_observation_summary_for_api(
                payload.get("long_term_upstream_observation_summary") or {}
            )
        ),
        "long_term_priority_blocked_summary": (
            payload.get("long_term_priority_blocked_summary") or {}
        ),
        "long_term_coverage_progress": payload.get("long_term_coverage_progress") or {},
        "regression_smoke_summary": _summarize_regression_smoke_summary_for_api(
            payload.get("regression_smoke_summary") or {}
        ),
    }


def _load_latest_regression_smoke_summary(
    report_root: Path | None = None,
) -> dict[str, Any]:
    if report_root is None:
        report_root = REGRESSION_SMOKE_REPORT_ROOT
    if not report_root.exists():
        return {}

    try:
        summary_paths = sorted(
            (
                path
                for path in report_root.glob("regression_smoke_*/summary.json")
                if path.is_file()
            ),
            key=lambda path: (path.stat().st_mtime, path.name),
            reverse=True,
        )
    except OSError:
        return {}

    for summary_path in summary_paths:
        try:
            raw_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        normalized = _normalize_regression_smoke_summary(
            raw_summary,
            summary_path=str(summary_path),
            source="latest_artifact_fallback",
        )
        if normalized.get("groups") or normalized.get("group_results") or normalized.get("overall_status"):
            return normalized
    return {}


def build_scheduler_path_governance_summary(
    scheduler_overview: dict[str, Any] | None,
) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    host_context = str(overview.get("host_context") or "").strip()
    automation_host_mode = str(overview.get("automation_host_mode") or "").strip()
    configured_host_context = str(
        overview.get("configured_host_context")
        or host_context
        or automation_host_mode
        or ""
    ).strip()
    scheduler_enabled = bool(overview.get("enabled"))
    scheduler_running = bool(overview.get("scheduler_running"))
    host_context_mismatch = bool(overview.get("host_context_mismatch"))

    official_runtime_host = (
        "external_scheduler_process"
        if configured_host_context == "external_scheduler_process"
        else "embedded_web_scheduler"
    )
    fallback_active = bool(
        configured_host_context == "embedded_web_scheduler"
        or (host_context == "embedded_web_scheduler" and scheduler_running)
    )
    deployment_mode = (
        "external_process"
        if official_runtime_host == "external_scheduler_process"
        else "embedded_web"
    )
    summary_parts = [
        f"官方调度入口为 {SCHEDULER_OFFICIAL_ENTRYPOINT} + {SCHEDULER_OFFICIAL_MODULE}",
        (
            "当前配置要求独立 scheduler 进程常驻。"
            if official_runtime_host == "external_scheduler_process"
            else "当前配置允许 web 内嵌调度兜底。"
        ),
    ]
    if host_context:
        summary_parts.append(f"当前运行宿主为 {host_context}。")
    if host_context_mismatch:
        summary_parts.append("运行宿主与配置不一致。")

    return {
        "available": True,
        "official_entrypoint": SCHEDULER_OFFICIAL_ENTRYPOINT,
        "official_module": SCHEDULER_OFFICIAL_MODULE,
        "embedded_fallback_entrypoint": SCHEDULER_EMBEDDED_FALLBACK,
        "deployment_mode": deployment_mode,
        "official_runtime_host": official_runtime_host,
        "configured_host_context": configured_host_context or None,
        "current_host_context": host_context or None,
        "automation_host_mode": automation_host_mode or None,
        "scheduler_enabled": scheduler_enabled,
        "scheduler_running": scheduler_running,
        "host_context_mismatch": host_context_mismatch,
        "fallback_mode_allowed": configured_host_context == "embedded_web_scheduler",
        "fallback_mode_active": fallback_active,
        "legacy_paths": [
            {
                "path": SCHEDULER_EMBEDDED_FALLBACK,
                "role": "development_fallback_only",
            }
        ],
        "summary": " ".join(summary_parts),
    }


def scheduler_snapshot_changed(
    before_snapshot: dict[str, Any] | None,
    after_snapshot: dict[str, Any] | None,
) -> bool:
    before_has_payload = scheduler_snapshot_has_payload(before_snapshot)
    after_has_payload = scheduler_snapshot_has_payload(after_snapshot)
    if not before_has_payload and not after_has_payload:
        return False
    return scheduler_snapshot_signature(before_snapshot) != scheduler_snapshot_signature(after_snapshot)


def build_scheduler_maintenance_change_items(change_summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    summary = dict(change_summary or {})
    items: list[dict[str, Any]] = []
    for key, label in SCHEDULER_MAINTENANCE_CHANGE_LABELS.items():
        value = int(summary.get(key) or 0)
        if value > 0:
            items.append({"key": key, "label": label, "value": value})
    holding_delta_total = sum(abs(int(summary.get(key) or 0)) for key in SCHEDULER_HOLDING_PERIOD_CHANGE_KEYS)
    if bool(summary.get("holding_period_changed")) and holding_delta_total > 0:
        items.append(
            {
                "key": "holding_period_changed",
                "label": "持有期结构变化",
                "value": holding_delta_total,
            }
        )
    return items


def build_scheduler_child_task_digest(child_tasks: dict[str, Any] | None) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    counts = {
        "total": 0,
        "changed_total": 0,
        "change_unknown_total": 0,
        "activity_total": 0,
        "upstream_limited_total": 0,
        "backlog_empty_total": 0,
        "idle_by_design_total": 0,
        "no_action_after_scan_total": 0,
        "disabled_by_policy_total": 0,
        "attention_total": 0,
    }
    for task_key, raw_payload in (child_tasks or {}).items():
        payload = dict(raw_payload or {})
        changed_total = optional_count(payload.get("changed_total"))
        processed_total = optional_count(payload.get("processed_total"))
        candidate_total = optional_count(payload.get("candidate_total"))
        status = str(payload.get("status") or "").strip() or None
        skip_reason = str(payload.get("skip_reason") or "").strip() or None
        no_action_category = str(payload.get("no_action_category") or "").strip() or None
        attention_needed = bool(
            status in {"failed", "degraded", "blocked"}
            or no_action_category == "upstream_limited"
        )

        if changed_total is not None and changed_total > 0:
            counts["changed_total"] += 1
        if changed_total is None:
            counts["change_unknown_total"] += 1
        if payload.get("activity_observed") is True:
            counts["activity_total"] += 1
        if no_action_category == "upstream_limited":
            counts["upstream_limited_total"] += 1
        if no_action_category == "backlog_empty":
            counts["backlog_empty_total"] += 1
        if no_action_category == "idle_by_design":
            counts["idle_by_design_total"] += 1
        if no_action_category == "no_action_after_scan":
            counts["no_action_after_scan_total"] += 1
        if no_action_category == "disabled_by_policy":
            counts["disabled_by_policy_total"] += 1
        if attention_needed:
            counts["attention_total"] += 1
        counts["total"] += 1

        items.append(
            {
                "task_key": task_key,
                "task_label": SCHEDULER_CHILD_TASK_LABELS.get(task_key) or task_key,
                "status": status,
                "skip_reason": skip_reason,
                "no_action_category": no_action_category,
                "candidate_total": candidate_total,
                "processed_total": processed_total,
                "changed_total": changed_total,
                "activity_observed": payload.get("activity_observed"),
                "idle_streak": int(payload.get("idle_streak") or 0),
                "attention_needed": attention_needed,
            }
        )

    return {
        "counts": counts,
        "items": items,
    }


def parse_local_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def read_machine_boot_time() -> datetime | None:
    try:
        stat_text = Path("/proc/stat").read_text(encoding="utf-8")
    except OSError:
        return None
    for line in stat_text.splitlines():
        if not line.startswith("btime "):
            continue
        try:
            return datetime.fromtimestamp(int(line.split()[1]))
        except (IndexError, TypeError, ValueError, OSError):
            return None
    return None


def resolve_task_expected_run_time(
    task_key: str,
    started_at: Any,
    *,
    settings: Any,
) -> datetime | None:
    started_dt = parse_local_datetime(started_at)
    if started_dt is None:
        return None

    if task_key == "daily_refresh":
        return started_dt.replace(
            hour=int(getattr(settings, "scheduler_daily_hour", 7) or 7),
            minute=int(getattr(settings, "scheduler_daily_minute", 30) or 30),
            second=0,
            microsecond=0,
        )
    if task_key == "intraday_refresh":
        return started_dt.replace(
            hour=int(getattr(settings, "scheduler_intraday_hour", 13) or 13),
            minute=int(getattr(settings, "scheduler_intraday_minute", 45) or 45),
            second=0,
            microsecond=0,
        )
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


def build_task_schedule_health(
    task_key: str,
    run: dict[str, Any],
    scheduler_started_at: Any,
    *,
    settings: Any,
    parse_local_datetime_fn: Callable[[Any], datetime | None] | None = None,
    read_machine_boot_time_fn: Callable[[], datetime | None] | None = None,
    resolve_task_expected_run_time_fn: Callable[[str, Any], datetime | None] | None = None,
) -> dict[str, Any]:
    parse_dt = parse_local_datetime_fn or parse_local_datetime
    read_boot_time = read_machine_boot_time_fn or read_machine_boot_time
    resolve_expected_time = resolve_task_expected_run_time_fn or (
        lambda current_task_key, current_started_at: resolve_task_expected_run_time(
            current_task_key,
            current_started_at,
            settings=settings,
        )
    )

    started_at = str(run.get("started_at") or "").strip()
    started_dt = parse_dt(started_at)
    expected_dt = resolve_expected_time(task_key, started_at)
    persisted_scheduler_started_at = str(run.get("startup_catch_up_scheduler_started_at") or "").strip()
    scheduler_started_dt = parse_dt(persisted_scheduler_started_at or scheduler_started_at)
    machine_boot_dt = read_boot_time()
    startup_catch_up_host_context = str(run.get("startup_catch_up_host_context") or run.get("host_context") or "").strip()

    delay_seconds = 0
    if started_dt is not None and expected_dt is not None:
        delay_seconds = max(0, int((started_dt - expected_dt).total_seconds()))

    raw_scheduler_start_gap_seconds = None
    if started_dt is not None and scheduler_started_dt is not None:
        raw_scheduler_start_gap_seconds = int((started_dt - scheduler_started_dt).total_seconds())

    startup_catch_up_suspected = bool(run.get("startup_catch_up_suspected"))
    if not startup_catch_up_suspected:
        startup_catch_up_suspected = bool(
            delay_seconds > 300
            and raw_scheduler_start_gap_seconds is not None
            and 0 <= raw_scheduler_start_gap_seconds <= 600
        )
    if not startup_catch_up_suspected:
        startup_catch_up_suspected = bool(
            delay_seconds > 300
            and started_dt is not None
            and scheduler_started_dt is not None
            and scheduler_started_dt > started_dt
        )
    scheduler_start_gap_seconds = (
        raw_scheduler_start_gap_seconds
        if raw_scheduler_start_gap_seconds is not None and raw_scheduler_start_gap_seconds >= 0
        else None
    )
    startup_catch_up_origin = None
    if startup_catch_up_suspected:
        if (
            started_dt is not None
            and machine_boot_dt is not None
            and 0 <= int((started_dt - machine_boot_dt).total_seconds()) <= 600
        ):
            startup_catch_up_origin = "machine_boot_catch_up"
        elif startup_catch_up_host_context == "embedded_web_scheduler":
            startup_catch_up_origin = "embedded_web_bootstrap_catch_up"
        elif startup_catch_up_host_context == "external_scheduler_process":
            startup_catch_up_origin = "external_scheduler_restart_catch_up"
        elif (
            started_dt is not None
            and scheduler_started_dt is not None
            and scheduler_started_dt > started_dt
        ):
            startup_catch_up_origin = "prior_host_instance_catch_up"
        else:
            startup_catch_up_origin = "host_restart_catch_up"

    return {
        "scheduled_for": expected_dt.isoformat(timespec="seconds") if expected_dt is not None else None,
        "actual_started_at": started_at or None,
        "start_delay_seconds": delay_seconds,
        "started_late": bool(delay_seconds > 300),
        "startup_catch_up_suspected": startup_catch_up_suspected,
        "startup_catch_up_origin": startup_catch_up_origin,
        "startup_catch_up_host_context": startup_catch_up_host_context or None,
        "scheduler_start_gap_seconds": scheduler_start_gap_seconds,
    }


def resolve_run_latest_screened_snapshot(
    run: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    overview_latest_screened_snapshot: dict[str, Any] | None,
    *,
    scheduler_snapshot_has_payload_fn: Callable[[dict[str, Any] | None], bool] = scheduler_snapshot_has_payload,
) -> dict[str, Any]:
    for candidate in (
        (closure or {}).get("latest_screened_snapshot") or {},
        (run or {}).get("latest_screened_snapshot") or {},
        overview_latest_screened_snapshot or {},
    ):
        if scheduler_snapshot_has_payload_fn(candidate):
            return dict(candidate)
    return {}


def build_schedule_stability_summary(
    scheduler_overview: dict[str, Any] | None,
    *,
    build_task_schedule_health_fn: Callable[[str, dict[str, Any], Any], dict[str, Any]],
) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    slot_runs = overview.get("task_schedule_slot_runs") or {}
    scheduler_started_at = overview.get("started_at")
    task_map: dict[str, Any] = {}
    tasks: list[dict[str, Any]] = []
    summary = {
        "task_count": 0,
        "task_with_history_total": 0,
        "late_slot_task_total": 0,
        "attention_task_total": 0,
        "late_slot_total": 0,
        "scheduler_drift_late_slot_total": 0,
    }

    for task_key in ("daily_refresh", "intraday_refresh", "weekly_maintenance"):
        raw_runs = list((slot_runs.get(task_key) or [])[:6])
        slot_windows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        late_start_cause_counter: Counter[str] = Counter()
        late_slot_count = 0
        scheduler_drift_late_slot_count = 0
        consecutive_late_slots = 0
        consecutive_scheduler_drift_late_slots = 0
        max_delay_seconds = 0

        for raw_run in raw_runs:
            run_payload = dict(raw_run or {})
            schedule_health = build_task_schedule_health_fn(task_key, run_payload, scheduler_started_at)
            slot_windows.append((run_payload, schedule_health))
            if bool(schedule_health.get("started_late")):
                late_slot_count += 1
                max_delay_seconds = max(max_delay_seconds, int(schedule_health.get("start_delay_seconds") or 0))
                late_start_cause = (
                    "startup_catch_up"
                    if bool(schedule_health.get("startup_catch_up_suspected"))
                    else "scheduler_drift"
                )
                late_start_cause_counter[late_start_cause] += 1
                if late_start_cause == "scheduler_drift":
                    scheduler_drift_late_slot_count += 1

        for _, schedule_health in slot_windows:
            if not bool(schedule_health.get("started_late")):
                break
            consecutive_late_slots += 1
        for _, schedule_health in slot_windows:
            if not bool(schedule_health.get("started_late")) or bool(schedule_health.get("startup_catch_up_suspected")):
                break
            consecutive_scheduler_drift_late_slots += 1

        slot_window_size = len(slot_windows)
        latest_run, latest_schedule_health = slot_windows[0] if slot_windows else ({}, {})
        attention_needed = bool(
            slot_window_size >= 3
            and (
                scheduler_drift_late_slot_count >= 2
                or consecutive_scheduler_drift_late_slots >= 2
            )
        )

        task_summary = {
            "task_key": task_key,
            "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
            "slot_window_size": slot_window_size,
            "late_slot_count": late_slot_count,
            "scheduler_drift_late_slot_count": scheduler_drift_late_slot_count,
            "late_slot_ratio": round(late_slot_count / slot_window_size, 4) if slot_window_size > 0 else 0.0,
            "consecutive_late_slots": consecutive_late_slots,
            "consecutive_scheduler_drift_late_slots": consecutive_scheduler_drift_late_slots,
            "max_delay_seconds": max_delay_seconds,
            "latest_slot_started_late": bool(latest_schedule_health.get("started_late")),
            "latest_slot_start_delay_seconds": int(latest_schedule_health.get("start_delay_seconds") or 0),
            "latest_slot_startup_catch_up_origin": latest_schedule_health.get("startup_catch_up_origin"),
            "latest_slot_status": latest_run.get("status"),
            "attention_needed": attention_needed,
            "late_start_cause_breakdown": [
                {"cause": cause, "count": count}
                for cause, count in sorted(late_start_cause_counter.items())
            ],
            "recent_slots": [
                {
                    "started_at": run_payload.get("started_at"),
                    "status": run_payload.get("status"),
                    "scheduled_for": schedule_health.get("scheduled_for"),
                    "start_delay_seconds": int(schedule_health.get("start_delay_seconds") or 0),
                    "started_late": bool(schedule_health.get("started_late")),
                    "startup_catch_up_origin": schedule_health.get("startup_catch_up_origin"),
                }
                for run_payload, schedule_health in slot_windows
            ],
        }
        task_map[task_key] = task_summary
        tasks.append(task_summary)
        summary["task_count"] += 1
        if slot_window_size > 0:
            summary["task_with_history_total"] += 1
        if late_slot_count > 0:
            summary["late_slot_task_total"] += 1
        if attention_needed:
            summary["attention_task_total"] += 1
        summary["late_slot_total"] += late_slot_count
        summary["scheduler_drift_late_slot_total"] += scheduler_drift_late_slot_count

    return {
        "summary": summary,
        "tasks": tasks,
        "task_map": task_map,
    }


def build_scheduler_task_closure_digest(
    scheduler_overview: dict[str, Any] | None,
    *,
    resolve_run_latest_screened_snapshot_fn: Callable[[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None], dict[str, Any]],
    scheduler_snapshot_has_payload_fn: Callable[[dict[str, Any] | None], bool] = scheduler_snapshot_has_payload,
    scheduler_snapshot_changed_fn: Callable[[dict[str, Any] | None, dict[str, Any] | None], bool] = scheduler_snapshot_changed,
) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    overview_latest_screened_snapshot = overview.get("latest_screened_snapshot") or {}
    ordered_tasks: list[dict[str, Any]] = []
    task_map: dict[str, Any] = {}
    summary = {
        "task_count": 0,
        "screening_triggered_total": 0,
        "report_refresh_triggered_total": 0,
        "maintenance_changed_total": 0,
        "snapshot_changed_total": 0,
        "attention_total": 0,
        "upstream_limited_child_task_total": 0,
    }
    alerts: list[dict[str, Any]] = []

    for task_key in SCHEDULER_CLOSURE_TASK_ORDER:
        run = dict(overview.get(SCHEDULER_OVERVIEW_RUN_KEYS.get(task_key, f"{task_key}_run")) or {})
        before_snapshot = run.get("before_effective_nav_snapshot") or {}
        after_snapshot = run.get("after_effective_nav_snapshot") or {}
        latest_screened_snapshot = resolve_run_latest_screened_snapshot_fn(run, None, overview_latest_screened_snapshot)
        maintenance_change_summary = run.get("maintenance_data_change_summary") or {}
        maintenance_change_items = build_scheduler_maintenance_change_items(maintenance_change_summary)
        child_task_digest = build_scheduler_child_task_digest(run.get("child_tasks") or {})
        snapshot_changed_during_task = scheduler_snapshot_changed_fn(before_snapshot, after_snapshot)
        effective_snapshot_ahead_of_screened = bool(
            scheduler_snapshot_has_payload_fn(after_snapshot)
            and scheduler_snapshot_has_payload_fn(latest_screened_snapshot)
            and scheduler_snapshot_changed_fn(after_snapshot, latest_screened_snapshot)
        )
        snapshot_advanced_without_screening = bool(
            effective_snapshot_ahead_of_screened
            and not bool(run.get("screening_triggered"))
            and not bool(run.get("report_refresh_triggered"))
        )
        attention_needed = bool(
            run.get("status") in {"failed", "degraded"}
            or snapshot_advanced_without_screening
            or child_task_digest["counts"]["attention_total"] > 0
            or str(run.get("no_action_category") or "").strip() in {"automation_guard", "upstream_limited"}
        )

        if bool(run.get("screening_triggered")):
            summary["screening_triggered_total"] += 1
        if bool(run.get("report_refresh_triggered")):
            summary["report_refresh_triggered_total"] += 1
        if maintenance_change_items:
            summary["maintenance_changed_total"] += 1
        if snapshot_changed_during_task:
            summary["snapshot_changed_total"] += 1
        if attention_needed:
            summary["attention_total"] += 1
        summary["upstream_limited_child_task_total"] += int(child_task_digest["counts"]["upstream_limited_total"] or 0)
        summary["task_count"] += 1

        task_digest = {
            "task_key": task_key,
            "task_label": SCHEDULER_TASK_LABELS.get(task_key) or task_key,
            "status": run.get("status"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
            **copy_task_timing_fields(run),
            "action_category": run.get("action_category"),
            "no_action_category": run.get("no_action_category"),
            "screening_triggered": bool(run.get("screening_triggered")),
            "maintenance_rescreen_triggered": bool(run.get("maintenance_rescreen_triggered")),
            "screening_run_id": run.get("screening_run_id"),
            "maintenance_rescreen_run_id": run.get("maintenance_rescreen_run_id"),
            "report_refresh_triggered": bool(run.get("report_refresh_triggered")),
            "report_refresh_run_id": run.get("report_refresh_run_id"),
            "screening_reason": run.get("screening_reason"),
            "skip_reason": run.get("skip_reason"),
            "warning_reason": run.get("warning_reason"),
            "before_effective_nav_snapshot": before_snapshot,
            "after_effective_nav_snapshot": after_snapshot,
            "latest_screened_snapshot": latest_screened_snapshot,
            "snapshot_changed_during_task": snapshot_changed_during_task,
            "effective_snapshot_ahead_of_screened": effective_snapshot_ahead_of_screened,
            "snapshot_advanced_without_screening": snapshot_advanced_without_screening,
            "maintenance_change_items": maintenance_change_items,
            "child_task_digest": child_task_digest,
            "attention_needed": attention_needed,
            **copy_task_artifact_fields(run),
        }
        ordered_tasks.append(task_digest)
        task_map[task_key] = task_digest

        if snapshot_advanced_without_screening:
            alerts.append(
                {
                    "code": f"{task_key}_snapshot_advanced_without_screening",
                    "task_key": task_key,
                    "task_label": task_digest["task_label"],
                    "severity": "warning",
                    "label": f"{task_digest['task_label']}完成后快照已推进但未触发重筛",
                    "detail": (
                        f"后快照 {str(after_snapshot.get('candidate_date') or '-').strip() or '-'} "
                        f"与当时已发布快照不一致，需结合 {str(run.get('skip_reason') or run.get('warning_reason') or '').strip() or '运行闭环结果'} 复核。"
                    ),
                }
            )

    return {
        "summary": summary,
        "ordered_tasks": ordered_tasks,
        "task_map": task_map,
        "alerts": alerts,
    }


def build_scheduler_overview_summary(
    scheduler_state: dict[str, Any] | None,
    *,
    settings: Any,
    project_scheduler_cls: Any,
    select_task_closure_source_fn: Callable[..., dict[str, Any]],
    build_task_schedule_health_fn: Callable[[str, dict[str, Any], Any], dict[str, Any]],
    resolve_run_latest_screened_snapshot_fn: Callable[[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None], dict[str, Any]],
    build_schedule_stability_summary_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    build_scheduler_task_closure_digest_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
) -> dict[str, Any]:
    payload = dict(scheduler_state or {})
    project_root = getattr(settings, "project_root", Path.cwd())
    last_runs = payload.get("last_runs") or {}
    slot_runs = payload.get("task_schedule_slot_runs") or {}
    task_closures = payload.get("task_closures") or {}
    summarized_task_closures = summarize_task_closures_for_state(task_closures)
    manual_run_history = payload.get("manual_run_history") or []
    pending_manual_run_requests = payload.get("pending_manual_run_requests") or []
    jobs = payload.get("jobs") or []
    current_task = payload.get("current_task") or {}

    def _runtime_closure(task_key: str, task_name: str, run: dict[str, Any] | None) -> dict[str, Any]:
        if (slot_runs.get(task_key) or []):
            return {}
        payload_run = dict(run or {})
        if not payload_run:
            return {}
        return build_task_closure_summary(
            task_key=task_key,
            task_name=task_name,
            result=payload_run,
        )

    daily_run = ((slot_runs.get("daily_refresh") or [None])[0] or last_runs.get("scheduled_daily_bond_refresh") or {})
    intraday_run = ((slot_runs.get("intraday_refresh") or [None])[0] or last_runs.get("scheduled_intraday_bond_refresh") or {})
    weekly_run = select_task_closure_source_fn(
        task_key="weekly_maintenance",
        task_name="scheduled_weekly_bond_maintenance",
        slot_runs=slot_runs,
        last_runs=last_runs,
    )
    watchdog_run = last_runs.get("scheduled_snapshot_watchdog_refresh") or {}
    daily_closure = task_closures.get("daily_refresh") or {}
    intraday_closure = task_closures.get("intraday_refresh") or {}
    weekly_closure = task_closures.get("weekly_maintenance") or {}
    # Select one task first. Never combine a closure from a different maintenance
    # window with the chosen run or the latest global maintenance attachment.
    weekly_source = restore_weekly_maintenance_metrics(weekly_run or weekly_closure, settings)
    if weekly_source:
        weekly_run = weekly_source
        weekly_closure = build_task_closure_summary(
            task_key="weekly_maintenance", task_name="scheduled_weekly_bond_maintenance", result=weekly_source,
        )
        task_closures = {**task_closures, "weekly_maintenance": weekly_closure}
        summarized_task_closures = summarize_task_closures_for_state(task_closures)
    watchdog_closure = task_closures.get("snapshot_watchdog") or {}
    daily_runtime_closure = _runtime_closure(
        "daily_refresh",
        "scheduled_daily_bond_refresh",
        last_runs.get("scheduled_daily_bond_refresh") or {},
    )
    intraday_runtime_closure = _runtime_closure(
        "intraday_refresh",
        "scheduled_intraday_bond_refresh",
        last_runs.get("scheduled_intraday_bond_refresh") or {},
    )
    daily_job = next((item for item in jobs if (item.get("id") or item.get("name")) == "daily_refresh"), {})
    intraday_job = next((item for item in jobs if (item.get("id") or item.get("name")) == "intraday_refresh"), {})
    weekly_job = next((item for item in jobs if (item.get("id") or item.get("name")) == "weekly_maintenance"), {})
    watchdog_job = next((item for item in jobs if (item.get("id") or item.get("name")) == "snapshot_watchdog"), {})
    weekly_child_tasks = weekly_closure.get("child_tasks") or {}
    nav_history_maintenance = (
        weekly_child_tasks.get("nav_history_maintenance")
        or weekly_run.get("nav_history_maintenance")
        or {}
    )
    nav_history_result = nav_history_maintenance.get("maintenance_result") or nav_history_maintenance or {}
    long_term_history_maintenance = (
        weekly_child_tasks.get("long_term_history_maintenance")
        or weekly_run.get("long_term_history_maintenance")
        or {}
    )
    long_term_history_result = (
        long_term_history_maintenance.get("maintenance_result")
        or long_term_history_maintenance
        or {}
    )
    daily_logs_tail = (daily_run.get("logs_tail") or [])[-6:]
    intraday_logs_tail = (intraday_run.get("logs_tail") or [])[-6:]
    weekly_logs_tail = (weekly_run.get("logs_tail") or [])[-6:]
    watchdog_logs_tail = (watchdog_run.get("logs_tail") or [])[-6:]
    weekly_child_tasks_for_api = dict(weekly_closure.get("child_tasks") or {})
    regression_smoke = _normalize_regression_smoke_summary(
        weekly_run.get("regression_smoke")
        or weekly_closure.get("regression_smoke")
    )
    if not regression_smoke and not weekly_child_tasks_for_api:
        fallback_report_root = _resolve_regression_smoke_report_root(settings)
        if fallback_report_root is not None:
            regression_smoke = _normalize_regression_smoke_summary(
                _load_latest_regression_smoke_summary(fallback_report_root)
            )
    if "long_term_history_maintenance" in weekly_child_tasks_for_api:
        weekly_child_tasks_for_api["long_term_history_maintenance"] = _normalize_long_term_child_task_payload(
            weekly_child_tasks_for_api.get("long_term_history_maintenance") or {}
        )
    if regression_smoke:
        weekly_child_tasks_for_api["regression_smoke"] = _build_regression_smoke_child_task(
            regression_smoke
        )
    empty_detail_payload_audit = project_scheduler_cls(settings)._build_empty_detail_payload_audit()
    scheduler_started_at = payload.get("started_at")
    daily_schedule_health = build_task_schedule_health_fn("daily_refresh", daily_run, scheduler_started_at)
    intraday_schedule_health = build_task_schedule_health_fn("intraday_refresh", intraday_run, scheduler_started_at)
    weekly_schedule_health = build_task_schedule_health_fn("weekly_maintenance", weekly_run, scheduler_started_at)
    weekly_run = apply_weekly_collection_subprocess_overlay(
        weekly_run,
        project_root=project_root,
    )
    weekly_artifacts = copy_task_artifact_fields(weekly_run, weekly_closure)
    weekly_collection_subprocess_health = (
        weekly_run.get("collection_subprocess_health")
        if isinstance(weekly_run.get("collection_subprocess_health"), dict)
        else {}
    )
    overview_latest_screened_snapshot = payload.get("latest_screened_snapshot") or {}
    summary = {
        "enabled": bool(payload.get("enabled")),
        "scheduler_running": bool(payload.get("scheduler_running")),
        "automation_host_mode": payload.get("automation_host_mode"),
        "host_context": payload.get("host_context"),
        "configured_host_context": payload.get("configured_host_context"),
        "host_context_mismatch": bool(payload.get("host_context_mismatch")),
        "scheduler_service_runtime": payload.get("scheduler_service_runtime") or {},
        "web_service_runtime": payload.get("web_service_runtime") or {},
        "started_at": payload.get("started_at"),
        "stopped_at": payload.get("stopped_at"),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "last_error": payload.get("last_error"),
        "screening_backlog_due": bool(payload.get("screening_backlog_due")),
        "screening_backlog_reason": payload.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(payload.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": payload.get("startup_catch_up_due_jobs") or [],
        "blocked_snapshot_retry_cooldown": payload.get("blocked_snapshot_retry_cooldown") or {},
        "failed_screening_retry_cooldown": payload.get("failed_screening_retry_cooldown") or {},
        "workflow_busy_retry_cooldown": payload.get("workflow_busy_retry_cooldown") or {},
        "scheduler_runtime_evidence": payload.get("scheduler_runtime_evidence") or {},
        "scheduler_state_claimed_running": bool(payload.get("scheduler_state_claimed_running")),
        "scheduler_recovery_deferred": bool(payload.get("scheduler_recovery_deferred")),
        "scheduler_recovery_boundary": payload.get("scheduler_recovery_boundary"),
        "configured_job_ids": payload.get("configured_job_ids") or [],
        "missing_job_ids": payload.get("missing_job_ids") or [],
        "unexpected_job_ids": payload.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(payload.get("job_drift_detected")),
        "effective_nav_snapshot": payload.get("effective_nav_snapshot") or {},
        "latest_screened_snapshot": payload.get("latest_screened_snapshot") or {},
        "nav_freshness": payload.get("nav_freshness") or {},
        "jobs": jobs,
        "current_task": current_task,
        "busy_skip_summary": summarize_busy_skips(payload.get("recent_busy_skips")),
        "manual_run_history": manual_run_history[:10] if isinstance(manual_run_history, list) else [],
        "pending_manual_run_requests": (
            pending_manual_run_requests[:10] if isinstance(pending_manual_run_requests, list) else []
        ),
        "task_schedule_slot_runs": slot_runs,
        "task_closures": summarized_task_closures,
        "daily_job": daily_job,
        "intraday_job": intraday_job,
        "weekly_job": weekly_job,
        "watchdog_job": watchdog_job,
        "daily_run": {
            "task_name": daily_run.get("task_name") or daily_closure.get("task_name"),
            "status": daily_run.get("status") or daily_closure.get("status"),
            "started_at": daily_run.get("started_at") or daily_closure.get("started_at"),
            "finished_at": daily_run.get("finished_at") or daily_closure.get("finished_at"),
            **copy_task_timing_fields(daily_run, daily_closure),
            "screening_triggered": bool(
                daily_run.get("screening_triggered")
                if "screening_triggered" in daily_run
                else daily_runtime_closure.get("screening_triggered")
                if daily_runtime_closure
                else daily_closure.get("screening_triggered")
            ),
            "maintenance_rescreen_triggered": bool(
                daily_run.get("maintenance_rescreen_triggered")
                if "maintenance_rescreen_triggered" in daily_run
                else daily_runtime_closure.get("maintenance_rescreen_triggered")
                if daily_runtime_closure
                else daily_closure.get("maintenance_rescreen_triggered")
            ),
            "screening_reason": (
                daily_run.get("screening_reason")
                or daily_runtime_closure.get("screening_reason")
                or daily_closure.get("screening_reason")
            ),
            "skip_reason": (
                daily_run.get("skip_reason")
                or daily_runtime_closure.get("skip_reason")
                or daily_closure.get("skip_reason")
            ),
            "warning_reason": (
                daily_run.get("warning_reason")
                or daily_runtime_closure.get("warning_reason")
                or daily_closure.get("warning_reason")
            ),
            "action_category": (
                daily_runtime_closure.get("action_category")
                if daily_runtime_closure
                else daily_closure.get("action_category")
            ),
            "no_action_category": (
                daily_runtime_closure.get("no_action_category")
                if daily_runtime_closure
                else daily_closure.get("no_action_category")
            ),
            "degraded": bool(daily_run.get("degraded")),
            "degraded_stage": daily_run.get("degraded_stage"),
            "screening_run_id": (
                daily_run.get("screening_run_id")
                or daily_runtime_closure.get("screening_run_id")
                or daily_closure.get("screening_run_id")
            ),
            "maintenance_rescreen_run_id": (
                daily_run.get("maintenance_rescreen_run_id")
                or daily_runtime_closure.get("maintenance_rescreen_run_id")
                or daily_closure.get("maintenance_rescreen_run_id")
            ),
            "screening_final_count": (
                daily_run.get("screening_final_count")
                or daily_runtime_closure.get("screening_final_count")
                or daily_closure.get("screening_final_count")
            ),
            "report_refresh_triggered": bool(
                daily_run.get("report_refresh_triggered")
                if "report_refresh_triggered" in daily_run
                else daily_runtime_closure.get("report_refresh_triggered")
                if daily_runtime_closure
                else daily_closure.get("report_refresh_triggered")
            ),
            "report_refresh_run_id": (
                daily_run.get("report_refresh_run_id")
                or daily_runtime_closure.get("report_refresh_run_id")
                or daily_closure.get("report_refresh_run_id")
            ),
            "before_effective_nav_snapshot": (
                daily_run.get("before_effective_nav_snapshot")
                or daily_runtime_closure.get("before_effective_nav_snapshot")
                or daily_closure.get("before_effective_nav_snapshot")
                or {}
            ),
            "after_effective_nav_snapshot": (
                daily_run.get("after_effective_nav_snapshot")
                or daily_runtime_closure.get("after_effective_nav_snapshot")
                or daily_closure.get("after_effective_nav_snapshot")
                or {}
            ),
            "latest_screened_snapshot": resolve_run_latest_screened_snapshot_fn(
                daily_run,
                daily_closure,
                overview_latest_screened_snapshot,
            ),
            "maintenance_data_change_summary": (
                daily_run.get("maintenance_data_change_summary")
                or daily_runtime_closure.get("maintenance_data_change_summary")
                or daily_closure.get("maintenance_data_change_summary")
                or {}
            ),
            "child_tasks": daily_closure.get("child_tasks") or {},
            "records_failed": daily_run.get("records_failed"),
            "scheduled_for": daily_schedule_health.get("scheduled_for"),
            "start_delay_seconds": daily_schedule_health.get("start_delay_seconds"),
            "started_late": daily_schedule_health.get("started_late"),
            "startup_catch_up_suspected": daily_schedule_health.get("startup_catch_up_suspected"),
            "startup_catch_up_origin": daily_schedule_health.get("startup_catch_up_origin"),
            "startup_catch_up_host_context": daily_schedule_health.get("startup_catch_up_host_context"),
            "scheduler_start_gap_seconds": daily_schedule_health.get("scheduler_start_gap_seconds"),
            "logs_tail": daily_logs_tail,
            **copy_task_artifact_fields(daily_run, daily_closure),
        },
        "intraday_run": {
            "task_name": intraday_run.get("task_name") or intraday_closure.get("task_name"),
            "status": intraday_run.get("status") or intraday_closure.get("status"),
            "started_at": intraday_run.get("started_at") or intraday_closure.get("started_at"),
            "finished_at": intraday_run.get("finished_at") or intraday_closure.get("finished_at"),
            **copy_task_timing_fields(intraday_run, intraday_closure),
            "screening_triggered": bool(
                intraday_run.get("screening_triggered")
                if "screening_triggered" in intraday_run
                else intraday_runtime_closure.get("screening_triggered")
                if intraday_runtime_closure
                else intraday_closure.get("screening_triggered")
            ),
            "maintenance_rescreen_triggered": bool(
                intraday_run.get("maintenance_rescreen_triggered")
                if "maintenance_rescreen_triggered" in intraday_run
                else intraday_runtime_closure.get("maintenance_rescreen_triggered")
                if intraday_runtime_closure
                else intraday_closure.get("maintenance_rescreen_triggered")
            ),
            "screening_reason": (
                intraday_run.get("screening_reason")
                or intraday_runtime_closure.get("screening_reason")
                or intraday_closure.get("screening_reason")
            ),
            "skip_reason": (
                intraday_run.get("skip_reason")
                or intraday_runtime_closure.get("skip_reason")
                or intraday_closure.get("skip_reason")
            ),
            "warning_reason": (
                intraday_run.get("warning_reason")
                or intraday_runtime_closure.get("warning_reason")
                or intraday_closure.get("warning_reason")
            ),
            "action_category": (
                intraday_runtime_closure.get("action_category")
                if intraday_runtime_closure
                else intraday_closure.get("action_category")
            ),
            "no_action_category": (
                intraday_runtime_closure.get("no_action_category")
                if intraday_runtime_closure
                else intraday_closure.get("no_action_category")
            ),
            "degraded": bool(intraday_run.get("degraded")),
            "degraded_stage": intraday_run.get("degraded_stage"),
            "screening_run_id": (
                intraday_run.get("screening_run_id")
                or intraday_runtime_closure.get("screening_run_id")
                or intraday_closure.get("screening_run_id")
            ),
            "maintenance_rescreen_run_id": (
                intraday_run.get("maintenance_rescreen_run_id")
                or intraday_runtime_closure.get("maintenance_rescreen_run_id")
                or intraday_closure.get("maintenance_rescreen_run_id")
            ),
            "screening_final_count": (
                intraday_run.get("screening_final_count")
                or intraday_runtime_closure.get("screening_final_count")
                or intraday_closure.get("screening_final_count")
            ),
            "report_refresh_triggered": bool(
                intraday_run.get("report_refresh_triggered")
                if "report_refresh_triggered" in intraday_run
                else intraday_runtime_closure.get("report_refresh_triggered")
                if intraday_runtime_closure
                else intraday_closure.get("report_refresh_triggered")
            ),
            "report_refresh_run_id": (
                intraday_run.get("report_refresh_run_id")
                or intraday_runtime_closure.get("report_refresh_run_id")
                or intraday_closure.get("report_refresh_run_id")
            ),
            "before_effective_nav_snapshot": (
                intraday_run.get("before_effective_nav_snapshot")
                or intraday_runtime_closure.get("before_effective_nav_snapshot")
                or intraday_closure.get("before_effective_nav_snapshot")
                or {}
            ),
            "after_effective_nav_snapshot": (
                intraday_run.get("after_effective_nav_snapshot")
                or intraday_runtime_closure.get("after_effective_nav_snapshot")
                or intraday_closure.get("after_effective_nav_snapshot")
                or {}
            ),
            "latest_screened_snapshot": resolve_run_latest_screened_snapshot_fn(
                intraday_run,
                intraday_closure,
                overview_latest_screened_snapshot,
            ),
            "maintenance_data_change_summary": (
                intraday_run.get("maintenance_data_change_summary")
                or intraday_runtime_closure.get("maintenance_data_change_summary")
                or intraday_closure.get("maintenance_data_change_summary")
                or {}
            ),
            "child_tasks": intraday_closure.get("child_tasks") or {},
            "records_failed": intraday_run.get("records_failed"),
            "scheduled_for": intraday_schedule_health.get("scheduled_for"),
            "start_delay_seconds": intraday_schedule_health.get("start_delay_seconds"),
            "started_late": intraday_schedule_health.get("started_late"),
            "startup_catch_up_suspected": intraday_schedule_health.get("startup_catch_up_suspected"),
            "startup_catch_up_origin": intraday_schedule_health.get("startup_catch_up_origin"),
            "startup_catch_up_host_context": intraday_schedule_health.get("startup_catch_up_host_context"),
            "scheduler_start_gap_seconds": intraday_schedule_health.get("scheduler_start_gap_seconds"),
            "logs_tail": intraday_logs_tail,
            **copy_task_artifact_fields(intraday_run, intraday_closure),
        },
        "weekly_run": {
            "task_name": weekly_run.get("task_name") or weekly_closure.get("task_name"),
            "task_history_id": weekly_run.get("task_history_id"),
            "maintenance_metrics_source": weekly_run.get("maintenance_metrics_source") or {},
            "status": weekly_run.get("status") or weekly_closure.get("status"),
            "started_at": weekly_run.get("started_at") or weekly_closure.get("started_at"),
            "finished_at": weekly_run.get("finished_at") or weekly_closure.get("finished_at"),
            **copy_task_timing_fields(weekly_run, weekly_closure),
            "screening_triggered": bool(
                weekly_run.get("screening_triggered") if "screening_triggered" in weekly_run else weekly_closure.get("screening_triggered")
            ),
            "maintenance_rescreen_triggered": bool(
                weekly_run.get("maintenance_rescreen_triggered")
                if "maintenance_rescreen_triggered" in weekly_run
                else weekly_closure.get("maintenance_rescreen_triggered")
            ),
            "screening_reason": weekly_run.get("screening_reason") or weekly_closure.get("screening_reason"),
            "skip_reason": weekly_run.get("skip_reason") or weekly_closure.get("skip_reason"),
            "warning_reason": weekly_run.get("warning_reason") or weekly_closure.get("warning_reason"),
            "action_category": weekly_closure.get("action_category"),
            "no_action_category": weekly_closure.get("no_action_category"),
            "degraded": bool(weekly_run.get("degraded")),
            "degraded_stage": weekly_run.get("degraded_stage"),
            "collection_subprocess_health": (
                weekly_run.get("collection_subprocess_health")
                or weekly_collection_subprocess_health
            ),
            "degradation_summary": build_scheduler_run_degradation_summary(
                weekly_run or weekly_closure,
                task_key="weekly_maintenance",
                project_root=project_root,
            ),
            "screening_run_id": weekly_run.get("screening_run_id") or weekly_closure.get("screening_run_id"),
            "maintenance_rescreen_run_id": weekly_run.get("maintenance_rescreen_run_id") or weekly_closure.get("maintenance_rescreen_run_id"),
            "report_refresh_triggered": bool(weekly_closure.get("report_refresh_triggered")),
            "report_refresh_run_id": weekly_closure.get("report_refresh_run_id"),
            "before_effective_nav_snapshot": weekly_closure.get("before_effective_nav_snapshot") or {},
            "after_effective_nav_snapshot": weekly_closure.get("after_effective_nav_snapshot") or {},
            "latest_screened_snapshot": resolve_run_latest_screened_snapshot_fn(
                weekly_run,
                weekly_closure,
                overview_latest_screened_snapshot,
            ),
            "maintenance_data_change_summary": weekly_closure.get("maintenance_data_change_summary") or {},
            "child_tasks": (
                weekly_child_tasks_for_api
                | (
                    {"regression_smoke": _build_regression_smoke_child_task(regression_smoke)}
                    if regression_smoke and "regression_smoke" not in weekly_child_tasks_for_api
                    else {}
                )
            ),
            "records_processed": weekly_run.get("records_processed"),
            "records_succeeded": weekly_run.get("records_succeeded"),
            "records_failed": weekly_run.get("records_failed"),
            "empty_detail_payload_audit": empty_detail_payload_audit,
            "nav_history_consistency_audit": weekly_run.get("nav_history_consistency_audit") or {},
            "nav_history_consistency_audit_error": weekly_run.get("nav_history_consistency_audit_error"),
            "task_history_governance_preview": weekly_run.get("task_history_governance_preview") or {},
            "task_history_governance_preview_error": weekly_run.get("task_history_governance_preview_error"),
            "task_history_cleanup_execution": weekly_run.get("task_history_cleanup_execution") or {},
            "task_history_cleanup_execution_error": weekly_run.get("task_history_cleanup_execution_error"),
            "maintenance_backup_retention": weekly_run.get("maintenance_backup_retention") or {},
            "maintenance_backup_retention_error": weekly_run.get("maintenance_backup_retention_error"),
            "scheduled_for": weekly_schedule_health.get("scheduled_for"),
            "start_delay_seconds": weekly_schedule_health.get("start_delay_seconds"),
            "started_late": weekly_schedule_health.get("started_late"),
            "startup_catch_up_suspected": weekly_schedule_health.get("startup_catch_up_suspected"),
            "startup_catch_up_origin": weekly_schedule_health.get("startup_catch_up_origin"),
            "startup_catch_up_host_context": weekly_schedule_health.get("startup_catch_up_host_context"),
            "scheduler_start_gap_seconds": weekly_schedule_health.get("scheduler_start_gap_seconds"),
            "logs_tail": weekly_logs_tail,
            **weekly_artifacts,
        },
        "snapshot_watchdog_run": {
            "task_name": watchdog_run.get("task_name") or watchdog_closure.get("task_name"),
            "status": watchdog_run.get("status") or watchdog_closure.get("status"),
            "started_at": watchdog_run.get("started_at") or watchdog_closure.get("started_at"),
            "finished_at": watchdog_run.get("finished_at") or watchdog_closure.get("finished_at"),
            **copy_task_timing_fields(watchdog_run, watchdog_closure),
            "screening_triggered": bool(
                watchdog_run.get("screening_triggered") if "screening_triggered" in watchdog_run else watchdog_closure.get("screening_triggered")
            ),
            "screening_reason": watchdog_run.get("screening_reason") or watchdog_closure.get("screening_reason"),
            "skip_reason": watchdog_run.get("skip_reason") or watchdog_closure.get("skip_reason"),
            "warning_reason": watchdog_run.get("warning_reason") or watchdog_closure.get("warning_reason"),
            "action_category": watchdog_closure.get("action_category"),
            "no_action_category": watchdog_closure.get("no_action_category"),
            "screening_run_id": watchdog_run.get("screening_run_id") or watchdog_closure.get("screening_run_id"),
            "screening_final_count": watchdog_run.get("screening_final_count") or watchdog_closure.get("screening_final_count"),
            "before_effective_nav_snapshot": watchdog_closure.get("before_effective_nav_snapshot") or {},
            "after_effective_nav_snapshot": watchdog_closure.get("after_effective_nav_snapshot") or {},
            "latest_screened_snapshot": resolve_run_latest_screened_snapshot_fn(
                watchdog_run,
                watchdog_closure,
                overview_latest_screened_snapshot,
            ),
            "maintenance_data_change_summary": watchdog_closure.get("maintenance_data_change_summary") or {},
            "child_tasks": watchdog_closure.get("child_tasks") or {},
            "logs_tail": watchdog_logs_tail,
            **copy_task_artifact_fields(watchdog_run, watchdog_closure),
        },
        "maintenance_summary": {
            "metrics_source": weekly_run.get("maintenance_metrics_source") or {},
            "nav_processed_total": optional_count(nav_history_result.get("processed_total")),
            "nav_attempted_total": optional_count(nav_history_result.get("attempted_total")),
            "nav_recovered_total": optional_count(nav_history_result.get("recovered_total")),
            "nav_failed_total": optional_count(nav_history_result.get("failed_total")),
            "nav_records_inserted": optional_count(nav_history_result.get("nav_records_inserted")),
            "long_term_status": long_term_history_maintenance.get("status"),
            "long_term_no_action_category": long_term_history_maintenance.get("no_action_category"),
            "long_term_processed_total": optional_count(long_term_history_result.get("processed_total")),
            "long_term_changed_total": optional_count(long_term_history_maintenance.get("changed_total")),
            "long_term_annual_success_total": optional_count(long_term_history_result.get("annual_success_total")),
            "long_term_rank_success_total": optional_count(long_term_history_result.get("rank_success_total")),
            "long_term_failed_reason_summary": long_term_history_result.get("failed_reason_summary") or {},
            "long_term_failed_reason_governance_summary": (
                long_term_history_result.get("failed_reason_governance_summary") or {}
            ),
            "long_term_upstream_observation_summary": (
                long_term_history_result.get("upstream_observation_summary") or {}
            ),
            "long_term_priority_blocked_summary": (
                long_term_history_result.get("priority_blocked_summary") or {}
            ),
            "long_term_coverage_progress": long_term_history_result.get("coverage_progress") or {},
            "regression_smoke_summary": regression_smoke,
        },
        "timing_summary": {
            "daily_refresh": daily_schedule_health,
            "intraday_refresh": intraday_schedule_health,
            "weekly_maintenance": weekly_schedule_health,
        },
    }
    for run_key in ("daily_run", "intraday_run", "weekly_run", "snapshot_watchdog_run"):
        run_payload = summary.get(run_key)
        if not isinstance(run_payload, dict):
            continue
        semantic_status = classify_scheduler_run_semantic_status(
            status=run_payload.get("status"),
            skip_reason=run_payload.get("skip_reason") or run_payload.get("warning_reason"),
            no_action_category=run_payload.get("no_action_category"),
            degraded=bool(run_payload.get("degraded")),
        )
        for field_name, field_value in semantic_status.items():
            run_payload.setdefault(field_name, field_value)
    weekly_payload = summary.get("weekly_run") or {}
    if weekly_payload:
        from backend.collection_count_correction import apply_to_weekly_payload

        weekly_payload = apply_to_weekly_payload(settings, weekly_payload)
        summary["weekly_run"] = weekly_payload
    if weekly_payload.get("status") == "degraded" and not weekly_payload.get("warning_reason"):
        from backend.weekly_warning_correction import apply_historical_warning_correction

        summary["weekly_run"] = apply_historical_warning_correction(settings, weekly_payload)
    summary["schedule_stability_summary"] = build_schedule_stability_summary_fn(summary)
    summary["automation_closure_digest"] = build_scheduler_task_closure_digest_fn(summary)
    return summary


def summarize_busy_skips(history: Any) -> dict[str, Any]:
    entries = [row for row in history[:20] if isinstance(row, dict)] if isinstance(history, list) else []
    status_fields = ("task_run_audit_status", "task_history_write_status", "scheduler_state_write_status", "task_log_write_status")
    error_fields = ("task_run_audit_error", "task_history_write_error", "scheduler_state_write_error", "task_log_write_error")
    return {
        "history_scope": "latest_20_persisted_attempts",
        "recent_count": len(entries),
        "latest_started_at": entries[0].get("started_at") if entries else None,
        "persistence_failure_count": sum(
            any(row.get(field) == "failed" for field in status_fields)
            or any(row.get(field) for field in error_fields) for row in entries
        ),
        "pending_persistence_count": sum(
            any(row.get(field) not in {"written", "failed"} for field in status_fields) for row in entries
        ),
    }


def build_task_audit_failure_issues(scheduler_overview: dict[str, Any] | None) -> list[dict[str, Any]]:
    overview = scheduler_overview or {}
    labels = {"daily_refresh": "日更", "intraday_refresh": "盘中更新",
              "weekly_maintenance": "周维护", "snapshot_watchdog": "快照检查"}
    issues = []
    for task_key, run_key in SCHEDULER_OVERVIEW_RUN_KEYS.items():
        run = overview.get(run_key) or {}
        error = str(run.get("task_run_audit_error") or "").strip()
        if error or run.get("task_run_audit_status") == "failed":
            issues.append({
                "code": f"{task_key}_task_audit_failed",
                "label": f"{labels[task_key]}运行审计失败",
                "detail": error or "任务运行审计未完成，业务执行结果另行保留。",
                "severity": "warning",
                "task_run_audit_status": "failed",
                "business_status": run.get("status"),
            })
    busy = overview.get("busy_skip_summary") or {}
    if busy.get("persistence_failure_count") or busy.get("pending_persistence_count"):
        issues.append({
            "code": "scheduler_busy_skip_persistence_failed",
            "label": "忙时跳过记录未完整保存",
            "detail": "部分跳过记录的数据库、状态、日志或审计保存失败或未完成；正在执行的任务状态单独保留。",
            "severity": "warning",
            "busy_skip_summary": deepcopy(busy),
        })
    return issues


def build_scheduler_chain_audit(scheduler_overview: dict[str, Any] | None) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    issues: list[dict[str, Any]] = build_task_audit_failure_issues(overview)

    def add_issue(code: str, label: str, detail: str, severity: str = "warning") -> None:
        issues.append(
            {
                "code": code,
                "label": label,
                "detail": detail,
                "severity": severity,
            }
        )

    def _recent_failure_focus_text(sequence: dict[str, Any]) -> str:
        stage = str(sequence.get("latest_failed_stage") or "").strip()
        subtask = str(sequence.get("latest_failed_subtask") or "").strip()
        exit_code = sequence.get("latest_failure_exit_code")
        run_id = str(sequence.get("latest_failure_workflow_run_id") or "").strip()
        pieces: list[str] = []
        if stage:
            pieces.append(f"阶段 {stage}")
        if subtask:
            pieces.append(f"子任务 {subtask}")
        if exit_code not in (None, ""):
            pieces.append(f"exit={exit_code}")
        if run_id:
            pieces.append(f"失败 run={run_id}")
        return "；".join(pieces) if pieces else f"错误 {sequence.get('latest_failure_error') or '-'}"

    startup_catch_up_due_jobs = overview.get("startup_catch_up_due_jobs") or []
    missing_job_ids = overview.get("missing_job_ids") or []
    unexpected_job_ids = overview.get("unexpected_job_ids") or []
    effective_nav_snapshot = overview.get("effective_nav_snapshot") or {}
    latest_screened_snapshot = overview.get("latest_screened_snapshot") or {}
    latest_candidate_date = str(latest_screened_snapshot.get("candidate_date") or "").strip()
    latest_signature = str(latest_screened_snapshot.get("signature") or "").strip()
    effective_candidate_date = str(effective_nav_snapshot.get("candidate_date") or "").strip()
    effective_signature = str(effective_nav_snapshot.get("signature") or "").strip()
    current_snapshot_aligned = bool(
        effective_candidate_date
        and latest_candidate_date
        and effective_candidate_date == latest_candidate_date
        and (not effective_signature or not latest_signature or effective_signature == latest_signature)
    )
    if bool(overview.get("startup_catch_up_pending")):
        add_issue(
            "startup_catch_up_pending",
            "存在启动补跑待处理任务",
            ",".join(str(item) for item in startup_catch_up_due_jobs) or "存在待补跑任务",
        )
    if bool(overview.get("job_drift_detected")):
        detail_parts: list[str] = []
        if missing_job_ids:
            detail_parts.append(f"missing={','.join(str(item) for item in missing_job_ids)}")
        if unexpected_job_ids:
            detail_parts.append(f"unexpected={','.join(str(item) for item in unexpected_job_ids)}")
        add_issue(
            "scheduler_job_drift",
            "调度器运行态与当前配置不一致",
            " / ".join(detail_parts) or "job 定义存在漂移",
            severity="error",
        )

    screening_backlog_reason = str(overview.get("screening_backlog_reason") or "").strip()
    if bool(overview.get("screening_backlog_due")):
        add_issue(
            "screening_backlog_due",
            "存在待补筛快照",
            screening_backlog_reason or "数据库有效快照已领先当前发布版本",
        )
    if bool(overview.get("host_context_mismatch")):
        configured_host_context = str(overview.get("configured_host_context") or "").strip()
        actual_host_context = str(overview.get("automation_host_mode") or "").strip()
        add_issue(
            "scheduler_host_context_mismatch",
            "调度宿主与配置不一致",
            f"configured={configured_host_context or 'unknown'} / actual={actual_host_context or 'unknown'}",
            severity="warning",
        )

    closure_digest = overview.get("automation_closure_digest") or {}
    for alert in (closure_digest.get("alerts") or []):
        add_issue(
            str(alert.get("code") or "automation_closure_alert"),
            str(alert.get("label") or "自动化闭环存在待复核事项"),
            str(alert.get("detail") or "请复核最近一次自动化闭环结果"),
            severity=str(alert.get("severity") or "warning"),
        )
    schedule_stability_summary = overview.get("schedule_stability_summary") or {}
    for task_summary in (schedule_stability_summary.get("tasks") or []):
        if not bool((task_summary or {}).get("attention_needed")):
            continue
        task_label = str((task_summary or {}).get("task_label") or (task_summary or {}).get("task_key") or "调度任务")
        consecutive_late_slots = int((task_summary or {}).get("consecutive_late_slots") or 0)
        late_slot_count = int((task_summary or {}).get("late_slot_count") or 0)
        scheduler_drift_late_slot_count = int(
            (task_summary or {}).get("scheduler_drift_late_slot_count")
            or late_slot_count
            or 0
        )
        consecutive_scheduler_drift_late_slots = int(
            (task_summary or {}).get("consecutive_scheduler_drift_late_slots")
            or consecutive_late_slots
            or 0
        )
        slot_window_size = int((task_summary or {}).get("slot_window_size") or 0)
        max_delay_seconds = int((task_summary or {}).get("max_delay_seconds") or 0)
        max_delay_minutes = round(max_delay_seconds / 60, 1) if max_delay_seconds > 0 else 0
        detail = (
            f"最近 {slot_window_size} 个调度槽位中真实调度漂移晚跑 {scheduler_drift_late_slot_count} 次，"
            f"连续真实调度漂移晚跑 {consecutive_scheduler_drift_late_slots} 次，最大延迟 {max_delay_minutes} 分钟"
        )
        add_issue(
            f"{str((task_summary or {}).get('task_key') or 'scheduler_task')}_late_start_stability",
            f"{task_label}准点性不稳定",
            detail,
            severity="warning",
        )

    recent_failure_recovery_summary = overview.get("recent_failure_recovery_summary") or {}
    for task_summary in (recent_failure_recovery_summary.get("tasks") or []):
        if not bool((task_summary or {}).get("attention_needed")):
            continue
        task_key = str((task_summary or {}).get("task_key") or "scheduler_task")
        task_label = str((task_summary or {}).get("task_label") or task_key)
        open_sequence = (task_summary or {}).get("open_failure_sequence") or {}
        recovered_sequence = (task_summary or {}).get("latest_recovered_sequence") or {}
        if open_sequence:
            failure_count = int(open_sequence.get("failure_count") or 0)
            latest_failure_status = str(
                open_sequence.get("latest_failure_status") or "failed"
            ).strip().lower()
            degraded_only = bool(
                int(open_sequence.get("degraded_run_count") or 0) > 0
                and int(open_sequence.get("failed_run_count") or 0) == 0
            )
            add_issue(
                (
                    f"{task_key}_recent_degradation_sequence_open"
                    if degraded_only
                    else f"{task_key}_recent_failure_sequence_open"
                ),
                (
                    f"{task_label}近期连续降级未恢复"
                    if degraded_only
                    else f"{task_label}近期连续失败未恢复"
                ),
                (
                    f"连续{'降级' if degraded_only else '失败/降级'} {failure_count} 次；最近时间 "
                    f"{open_sequence.get('last_failed_at') or '-'}；"
                    f"{_recent_failure_focus_text(open_sequence)}；"
                    f"错误 {open_sequence.get('latest_failure_error') or '-'}"
                ),
                severity="warning" if degraded_only or latest_failure_status == "degraded" else "error",
            )
            continue
        failure_count = int(recovered_sequence.get("failure_count") or 0)
        if failure_count >= 2:
            add_issue(
                f"{task_key}_recent_failure_sequence_recovered",
                f"{task_label}近期连续失败后已自动恢复",
                (
                    f"连续失败 {failure_count} 次，已在 "
                    f"{recovered_sequence.get('recovered_at') or '-'} 自动恢复；"
                    f"{_recent_failure_focus_text(recovered_sequence)}；"
                    f"恢复 run={recovered_sequence.get('recovery_screening_run_id') or '-'}"
                ),
                severity="warning",
            )

    run_definitions = [
        ("daily_refresh", "日常调度", overview.get("daily_run") or {}),
        ("intraday_refresh", "盘中补刷新", overview.get("intraday_run") or {}),
        ("weekly_maintenance", "周维护", overview.get("weekly_run") or {}),
        ("snapshot_watchdog", "快照看门狗", overview.get("snapshot_watchdog_run") or {}),
    ]
    run_summaries: dict[str, Any] = {}
    for run_code, run_label, run in run_definitions:
        status = str(run.get("status") or "").strip()
        warning_reason = str(run.get("warning_reason") or "").strip()
        screening_reason = str(run.get("screening_reason") or "").strip()
        skip_reason = str(run.get("skip_reason") or "").strip()
        degraded_stage = str(run.get("degraded_stage") or "").strip()
        action_category = str(run.get("action_category") or "").strip()
        no_action_category = str(run.get("no_action_category") or "").strip()
        late_start_cause = None
        if bool(run.get("started_late")):
            late_start_cause = "startup_catch_up" if bool(run.get("startup_catch_up_suspected")) else "scheduler_drift"
        suppress_historical_false_positive = False
        suppress_stale_watchdog_issue = bool(
            run_code == "snapshot_watchdog"
            and current_snapshot_aligned
            and status in {"failed", "degraded", "skipped"}
        )
        if run_code == "weekly_maintenance" and status in {"failed", "degraded"}:
            empty_detail_payload_audit = run.get("empty_detail_payload_audit") or {}
            screening_relevant_count = int(empty_detail_payload_audit.get("screening_relevant_count") or 0)
            suppress_historical_false_positive = (
                degraded_stage == "collect-managers"
                and warning_reason == "tiantian_payload_invalid"
                and bool(empty_detail_payload_audit.get("available"))
                and screening_relevant_count == 0
            )
        if status in {"failed", "degraded"}:
            if suppress_stale_watchdog_issue:
                pass
            elif not suppress_historical_false_positive:
                degradation_summary = (
                    run.get("degradation_summary")
                    or build_scheduler_run_degradation_summary(
                        run,
                        task_key=run_code,
                    )
                )
                detail_parts = [
                    part
                    for part in [
                        degradation_summary.get("issue_detail"),
                        (degradation_summary.get("nav_history_archive_resolution_evidence") or {}).get(
                            "root_cause_status"
                        ),
                        degraded_stage,
                        warning_reason,
                        screening_reason,
                        skip_reason,
                    ]
                    if part
                ]
                add_issue(
                    f"{run_code}_{status}",
                    f"{run_label}{'失败' if status == 'failed' else '降级'}",
                    " / ".join(detail_parts) or status,
                    severity="error" if status == "failed" else "warning",
                )
        if (status == "skipped" and no_action_category in {"automation_guard", "other"}
                and not suppress_stale_watchdog_issue
                and not busy_skip_has_current_publication(run, overview)):
            detail_parts = [part for part in [skip_reason, warning_reason, screening_reason] if part]
            add_issue(
                f"{run_code}_skip_guard",
                f"{run_label}未形成有效闭环",
                " / ".join(detail_parts) or "skipped",
                severity="warning",
            )
        if bool(run.get("started_late")) and late_start_cause == "scheduler_drift":
            delay_seconds = int(run.get("start_delay_seconds") or 0)
            delay_minutes = round(delay_seconds / 60, 1) if delay_seconds > 0 else 0
            detail = f"较计划时间延后 {delay_minutes} 分钟"
            add_issue(
                f"{run_code}_late_start",
                f"{run_label}未准点执行",
                detail,
                severity="warning",
            )
        run_summaries[run_code] = {
            "status": status or "",
            **copy_task_artifact_fields(run),
            "screening_triggered": bool(run.get("screening_triggered")),
            "collection_skipped": bool(run.get("collection_skipped")),
            "screening_reason": screening_reason or None,
            "skip_reason": skip_reason or None,
            "warning_reason": warning_reason or None,
            "action_category": action_category or None,
            "no_action_category": no_action_category or None,
            "degraded": bool(run.get("degraded")),
            "degraded_stage": degraded_stage or None,
            "degradation_summary": run.get("degradation_summary")
            or build_scheduler_run_degradation_summary(
                run,
                task_key=run_code,
            ),
            "suppressed_historical_false_positive": suppress_historical_false_positive,
            "scheduled_for": run.get("scheduled_for"),
            "start_delay_seconds": int(run.get("start_delay_seconds") or 0),
            "started_late": bool(run.get("started_late")),
            "startup_catch_up_suspected": bool(run.get("startup_catch_up_suspected")),
            "startup_catch_up_origin": str(run.get("startup_catch_up_origin") or "").strip() or None,
            "late_start_cause": late_start_cause,
        }

    return {
        "aligned": len(issues) == 0,
        "issue_count": len(issues),
        "startup_catch_up_pending": bool(overview.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": startup_catch_up_due_jobs,
        "screening_backlog_due": bool(overview.get("screening_backlog_due")),
        "screening_backlog_reason": screening_backlog_reason or None,
        "runs": run_summaries,
        "schedule_stability_summary": schedule_stability_summary,
        "open_issues": issues,
    }


def build_automation_autonomy_summary(
    scheduler_overview: dict[str, Any] | None,
    scheduler_chain_audit: dict[str, Any] | None,
    *,
    manual_operations_enabled: bool,
    load_database_overview_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    autostock_observation_enabled: bool = True,
) -> dict[str, Any]:
    overview_payload = dict(scheduler_overview or {})
    weekly_run_payload = (
        overview_payload.get("weekly_run")
        if isinstance(overview_payload.get("weekly_run"), dict)
        else {}
    )

    def _dedupe_preserve_order(items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            normalized = str(item or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _scheduler_task_key_for_task_name(task_name: str) -> str | None:
        normalized = str(task_name or "").strip()
        if not normalized:
            return None
        for task_key, scheduler_task_name in SCHEDULER_TASK_NAME_BY_KEY.items():
            if scheduler_task_name == normalized:
                return task_key
        return None

    def _is_current_task_recovery_issue(code: str, current_task_key: str | None) -> bool:
        normalized_code = str(code or "").strip()
        if not normalized_code or not current_task_key:
            return False
        prefix = f"{current_task_key}_"
        if not normalized_code.startswith(prefix):
            return False
        return normalized_code.endswith(
            (
                "_failed",
                "_degraded",
                "_skip_guard",
                "_recent_failure_sequence_open",
                "_recent_degradation_sequence_open",
            )
        )

    def _snapshot_date_and_signature(snapshot: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(snapshot or {})
        return {
            "candidate_date": str(payload.get("candidate_date") or "").strip() or None,
            "signature": str(payload.get("signature") or "").strip() or None,
            "candidate_fund_count": payload.get("candidate_fund_count"),
            "risk_1y_effective_end_date": payload.get("risk_1y_effective_end_date"),
            "risk_1y_effective_fund_count": payload.get("risk_1y_effective_fund_count"),
        }

    def _snapshot_publish_effectively_aligned(
        effective_snapshot: dict[str, Any] | None,
        screened_snapshot: dict[str, Any] | None,
    ) -> bool:
        effective = dict(effective_snapshot or {})
        screened = dict(screened_snapshot or {})
        effective_candidate_date = str(effective.get("candidate_date") or "").strip()
        screened_candidate_date = str(screened.get("candidate_date") or "").strip()
        if not effective_candidate_date or effective_candidate_date != screened_candidate_date:
            return False

        effective_signature = str(effective.get("signature") or "").strip()
        screened_signature = str(screened.get("signature") or "").strip()
        if effective_signature and screened_signature and effective_signature == screened_signature:
            return True

        comparable_keys = [
            "candidate_fund_count",
            "raw_max_latest_nav_date",
            "risk_1y_effective_end_date",
            "risk_1y_effective_fund_count",
            "risk_1y_effective_coverage_ratio_vs_candidate",
            "coverage_ratio_vs_total",
        ]
        comparable_count = 0
        for key in comparable_keys:
            left = effective.get(key)
            right = screened.get(key)
            if left in {None, ""} or right in {None, ""}:
                continue
            comparable_count += 1
            if left != right:
                return False
        return comparable_count > 0

    def _latest_successful_publish_hold_effectively_aligned(
        effective_snapshot: dict[str, Any] | None,
        publication_layers_payload: dict[str, Any] | None,
    ) -> bool:
        return False

    def _formal_processed_run_effectively_aligned(
        effective_snapshot: dict[str, Any] | None,
        publication_layers_payload: dict[str, Any] | None,
    ) -> bool:
        publication_layers = dict(publication_layers_payload or {})
        formal_run = dict(publication_layers.get("latest_formal_processed_run") or {})
        if not formal_run:
            return False

        formal_snapshot = dict(formal_run.get("snapshot") or {})
        if not _snapshot_publish_effectively_aligned(effective_snapshot, formal_snapshot):
            return False

        for blocker in formal_run.get("publish_blockers") or []:
            if not isinstance(blocker, dict):
                continue
            severity = str(blocker.get("severity") or "").strip().lower()
            if bool(blocker.get("blocking")) or severity == "blocking":
                return False
        return bool(
            formal_run.get("publishable")
            or formal_run.get("published")
        )

    def _parse_candidate_date(value: Any):
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None

    def _weekday_lag(start_value: Any, end_value: Any) -> int | None:
        start_date = _parse_candidate_date(start_value)
        end_date = _parse_candidate_date(end_value)
        if start_date is None or end_date is None:
            return None
        if end_date <= start_date:
            return 0
        lag = 0
        cursor = start_date
        while cursor < end_date:
            cursor += timedelta(days=1)
            if cursor.weekday() < 5:
                lag += 1
        return lag

    def _compact_publish_blockers(items: Any) -> list[dict[str, Any]]:
        blockers: list[dict[str, Any]] = []
        if not isinstance(items, list):
            return blockers
        for item in items[:5]:
            if not isinstance(item, dict):
                continue
            blockers.append(
                {
                    "code": str(item.get("code") or "").strip() or None,
                    "label": str(item.get("label") or "").strip() or None,
                    "detail": str(item.get("detail") or "").strip() or None,
                    "severity": str(item.get("severity") or "").strip() or None,
                    "blocking": bool(item.get("blocking")),
                }
            )
        return blockers

    def _build_publish_hold_staleness_summary(
        publication_layers_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        publication_layers = dict(publication_layers_payload or {})
        latest_successful = (
            dict(publication_layers.get("latest_successful_run") or {})
            if isinstance(publication_layers.get("latest_successful_run"), dict)
            else {}
        )
        current_published = (
            dict(publication_layers.get("current_published_run") or {})
            if isinstance(publication_layers.get("current_published_run"), dict)
            else {}
        )
        latest_snapshot = dict(latest_successful.get("snapshot") or {})
        current_snapshot = dict(current_published.get("snapshot") or {})
        latest_date = str(latest_snapshot.get("candidate_date") or "").strip()
        current_date = str(current_snapshot.get("candidate_date") or "").strip()
        latest_run_id = _clean_run_id(latest_successful.get("run_id"))
        current_run_id = _clean_run_id(current_published.get("run_id"))
        publish_blockers = list(
            latest_successful.get("publish_blockers")
            or publication_layers.get("latest_successful_blockers")
            or []
        )
        has_blocking_blocker = any(
            isinstance(item, dict)
            and (
                bool(item.get("blocking"))
                or str(item.get("severity") or "").strip().lower() == "blocking"
            )
            for item in publish_blockers
        )
        weekday_lag = _weekday_lag(current_date, latest_date)
        threshold = 1
        stale = bool(
            latest_successful
            and current_published
            and latest_run_id
            and current_run_id
            and latest_run_id != current_run_id
            and bool(latest_successful.get("publish_hold"))
            and not has_blocking_blocker
            and weekday_lag is not None
            and weekday_lag > threshold
        )

        def _count_delta(key: str) -> int | None:
            latest_value = latest_successful.get(key)
            current_value = current_published.get(key)
            if latest_value in (None, "") or current_value in (None, ""):
                return None
            try:
                return int(latest_value) - int(current_value)
            except (TypeError, ValueError):
                return None

        return {
            "available": bool(latest_successful and current_published),
            "stale": stale,
            "stale_reason": (
                "latest_successful_publish_hold_lag_exceeds_weekday_threshold"
                if stale
                else None
            ),
            "comparison_basis": "current_published_vs_latest_successful_observed_nav_dates",
            "weekday_lag": weekday_lag,
            "weekday_lag_threshold": threshold,
            "lag_calendar": "monday_to_friday_without_holiday_calendar",
            "trading_day_lag": None,
            "upstream_disclosure_overdue": None,
            "legacy_business_day_fields_are_weekday_aliases": True,
            "business_day_lag": weekday_lag,
            "business_day_lag_threshold": threshold,
            "latest_successful_run": {
                "run_id": latest_run_id,
                "generated_at": str(latest_successful.get("generated_at") or "").strip() or None,
                "candidate_date": latest_date or None,
                "final_count": latest_successful.get("final_count"),
                "dynamic_final_count": latest_successful.get("dynamic_final_count"),
                "publish_hold": bool(latest_successful.get("publish_hold")),
                "publish_hold_reason_codes": list(latest_successful.get("publish_hold_reason_codes") or []),
                "primary_blocker_code": str(latest_successful.get("primary_blocker_code") or "").strip() or None,
                "primary_blocker_label": str(latest_successful.get("primary_blocker_label") or "").strip() or None,
                "primary_blocker_detail": str(latest_successful.get("primary_blocker_detail") or "").strip() or None,
            },
            "current_published_run": {
                "run_id": current_run_id,
                "generated_at": str(current_published.get("generated_at") or "").strip() or None,
                "candidate_date": current_date or None,
                "final_count": current_published.get("final_count"),
                "dynamic_final_count": current_published.get("dynamic_final_count"),
            },
            "count_delta_vs_current_published": {
                "final_count": _count_delta("final_count"),
                "dynamic_final_count": _count_delta("dynamic_final_count"),
            },
            "latest_successful_final_selection_contraction": (
                latest_successful.get("final_selection_contraction") or {}
            ),
            "publish_blockers_preview": _compact_publish_blockers(publish_blockers),
            "blocking_blocker_present": has_blocking_blocker,
            "truth_policy": "real_publication_registry_snapshot_candidate_dates_only",
        }

    def _build_manual_run_queue_summary(
        current_task_payload: dict[str, Any] | None,
        pending_requests_payload: Any,
        manual_run_history_payload: Any,
    ) -> dict[str, Any]:
        current_task_dict = dict(current_task_payload or {})
        pending_requests = (
            [dict(item) for item in pending_requests_payload if isinstance(item, dict)]
            if isinstance(pending_requests_payload, list)
            else []
        )
        manual_run_history = (
            [dict(item) for item in manual_run_history_payload if isinstance(item, dict)]
            if isinstance(manual_run_history_payload, list)
            else []
        )
        current_task_name_local = str(current_task_dict.get("task_name") or "").strip()
        active_manual_entry = {}
        for item in manual_run_history:
            status = str(item.get("status") or "").strip().lower()
            task_name = str(item.get("task_name") or "").strip()
            if status in {"accepted", "running", "started"} and task_name == current_task_name_local:
                active_manual_entry = item
                break
        pending_request_ids = {
            str(item.get("request_id") or "").strip()
            for item in pending_requests
            if str(item.get("request_id") or "").strip()
        }

        def _is_entry_effectively_active(entry: dict[str, Any]) -> bool:
            request_id = str(entry.get("request_id") or "").strip()
            status = str(entry.get("status") or "").strip().lower()
            task_name = str(entry.get("task_name") or "").strip()
            if status not in {"accepted", "running", "started"}:
                return False
            if request_id and request_id in pending_request_ids:
                return True
            return bool(current_task_name_local and task_name == current_task_name_local)

        latest_manual_entry = {}
        for item in manual_run_history:
            status = str(item.get("status") or "").strip().lower()
            if status in {"success", "failed", "rejected", "skipped", "degraded", "unknown"}:
                latest_manual_entry = item
                break
            if _is_entry_effectively_active(item):
                latest_manual_entry = item
                break
        current_task_requested_by = str(active_manual_entry.get("requested_by") or "").strip()
        current_task_is_manual = bool(current_task_name_local and current_task_requested_by)
        current_task_origin = (
            "manual_request_queue"
            if current_task_is_manual
            else ("scheduler_automation" if current_task_name_local else "idle")
        )
        pending_requested_by_counts: dict[str, int] = {}
        pending_job_ids: list[str] = []
        for item in pending_requests:
            requested_by = str(item.get("requested_by") or "unknown").strip() or "unknown"
            pending_requested_by_counts[requested_by] = pending_requested_by_counts.get(requested_by, 0) + 1
            job_id = str(item.get("job_id") or "").strip()
            if job_id:
                pending_job_ids.append(job_id)

        latest_manual_status = str(latest_manual_entry.get("status") or "").strip() or None
        latest_manual_error = str(
            latest_manual_entry.get("error")
            or (latest_manual_entry.get("result") or {}).get("warning_reason")
            or ""
        ).strip() or None

        return {
            "available": True,
            "pending_request_count": int(len(pending_requests)),
            "pending_job_ids": pending_job_ids[:6],
            "pending_requested_by_counts": pending_requested_by_counts,
            "current_task_origin": current_task_origin,
            "current_task_requested_by": current_task_requested_by or None,
            "current_task_is_manual": current_task_is_manual,
            "current_task_name": current_task_name_local or None,
            "latest_manual_request_status": latest_manual_status,
            "latest_manual_request_error": latest_manual_error,
            "latest_manual_request_job_id": str(latest_manual_entry.get("job_id") or "").strip() or None,
            "manual_queue_active": bool(pending_requests),
            "summary": (
                f"当前手动补跑排队 {len(pending_requests)} 个；"
                f"当前执行来源 {current_task_origin}。"
            ),
        }

    overview = dict(scheduler_overview or {})
    audit = dict(scheduler_chain_audit or {})
    open_issues = audit.get("open_issues") or []
    database_overview = load_database_overview_fn(overview.get("effective_nav_snapshot") or {})
    effective_nav_snapshot = overview.get("effective_nav_snapshot") or {}
    latest_screened_snapshot = overview.get("latest_screened_snapshot") or {}
    latest_candidate_date = str(latest_screened_snapshot.get("candidate_date") or "").strip()
    latest_signature = str(latest_screened_snapshot.get("signature") or "").strip()
    effective_candidate_date = str(effective_nav_snapshot.get("candidate_date") or "").strip()
    effective_signature = str(effective_nav_snapshot.get("signature") or "").strip()
    publication_layers = overview.get("publication_layers") or {}
    publication_context = overview.get("publication_context") or {}
    latest_successful_run = (
        publication_layers.get("latest_successful_run")
        if isinstance(publication_layers.get("latest_successful_run"), dict)
        else {}
    )
    latest_publishable_run = (
        publication_layers.get("latest_publishable_run")
        if isinstance(publication_layers.get("latest_publishable_run"), dict)
        else {}
    )
    current_published_run = (
        publication_layers.get("current_published_run")
        if isinstance(publication_layers.get("current_published_run"), dict)
        else {}
    )
    daily_run = overview.get("daily_run") if isinstance(overview.get("daily_run"), dict) else {}

    def _clean_run_id(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    latest_canonical_run_id = (
        _clean_run_id((publication_context or {}).get("latest_canonical_run_id"))
        or _clean_run_id(latest_successful_run.get("run_id"))
        or _clean_run_id(latest_publishable_run.get("run_id"))
        or _clean_run_id(current_published_run.get("run_id"))
    )
    current_published_run_id = (
        _clean_run_id((publication_context or {}).get("current_published_run_id"))
        or _clean_run_id(current_published_run.get("run_id"))
    )
    latest_successful_run_id = (
        _clean_run_id((publication_context or {}).get("latest_successful_run_id"))
        or _clean_run_id(latest_successful_run.get("run_id"))
    )
    daily_screening_run_id = (
        _clean_run_id(daily_run.get("screening_run_id"))
        or _clean_run_id(daily_run.get("report_refresh_run_id"))
    )
    lineage_available = bool(
        latest_canonical_run_id
        or current_published_run_id
        or latest_successful_run_id
        or daily_screening_run_id
    )
    lineage_aligned = bool(
        latest_canonical_run_id
        and current_published_run_id
        and daily_screening_run_id
        and latest_canonical_run_id == current_published_run_id == daily_screening_run_id
    )
    current_snapshot_aligned = bool(
        effective_candidate_date
        and latest_candidate_date
        and effective_candidate_date == latest_candidate_date
        and (not effective_signature or not latest_signature or effective_signature == latest_signature)
    )
    publish_snapshot_effectively_aligned = _snapshot_publish_effectively_aligned(
        effective_nav_snapshot,
        latest_screened_snapshot,
    )
    latest_successful_publish_hold_aligned = _latest_successful_publish_hold_effectively_aligned(
        effective_nav_snapshot,
        publication_layers,
    )
    latest_formal_processed_run_aligned = _formal_processed_run_effectively_aligned(
        effective_nav_snapshot,
        publication_layers,
    )
    publish_hold_staleness_summary = _build_publish_hold_staleness_summary(publication_layers)
    formal_publication_stale_publish_hold = bool(publish_hold_staleness_summary.get("stale"))
    latest_successful_is_publishable = bool(
        publication_layers.get("latest_successful_is_publishable")
    ) or bool(latest_successful_run.get("publishable"))
    latest_successful_is_currently_published = bool(
        publication_layers.get("latest_successful_is_currently_published")
    ) or bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_run_id == current_published_run_id
    )
    latest_successful_publish_blockers = list(
        latest_successful_run.get("publish_blockers")
        or publication_layers.get("latest_successful_blockers")
        or []
    )
    latest_successful_has_blocking_publish_blocker = any(
        isinstance(item, dict)
        and (
            bool(item.get("blocking"))
            or str(item.get("severity") or "").strip().lower() == "blocking"
        )
        for item in latest_successful_publish_blockers
    )
    latest_successful_is_legacy_publish_hold = bool(latest_successful_run.get("publish_hold"))
    latest_successful_publishable_not_current = bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_is_publishable
        and not latest_successful_is_currently_published
    )
    latest_successful_legacy_hold_not_current = bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_is_legacy_publish_hold
        and not latest_successful_is_currently_published
    )
    latest_successful_blocked_not_current = bool(
        latest_successful_run_id
        and current_published_run_id
        and not latest_successful_is_currently_published
        and not latest_formal_processed_run_aligned
        and not latest_successful_is_legacy_publish_hold
        and (
            bool(latest_successful_run.get("publish_blocked"))
            or latest_successful_has_blocking_publish_blocker
        )
    )
    current_publication_formal_degradation_resolved = bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_is_currently_published
        and latest_successful_is_publishable
        and not latest_successful_is_legacy_publish_hold
        and not bool(latest_successful_run.get("publish_blocked"))
        and not latest_successful_has_blocking_publish_blocker
    )

    def _formal_publication_degradation_resolved_detail(issue: dict[str, Any]) -> dict[str, Any]:
        return {
            "root_cause_status": "resolved_by_current_formal_publication",
            "root_cause_resolved": True,
            "raw_signal": True,
            "effective_signal": False,
            "current_warning_suppressed": True,
            "suppression_reason": "current_latest_successful_is_formally_published",
            "manual_intervention_required": False,
            "current_interpretation": (
                "历史调度记录曾因正式发布未发布而降级；当前 latest successful 已成为 current published，"
                "且不存在发布阻断，因此该历史信号不再代表当前自动化故障。"
            ),
            "issue_detail": str((issue or {}).get("detail") or "").strip() or None,
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "latest_successful_is_publishable": latest_successful_is_publishable,
            "latest_successful_is_currently_published": latest_successful_is_currently_published,
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
        }

    def _is_resolved_formal_publication_degradation(issue: dict[str, Any]) -> bool:
        if not current_publication_formal_degradation_resolved:
            return False
        code = str((issue or {}).get("code") or "").strip()
        if not code.endswith("_degraded"):
            return False
        detail = str((issue or {}).get("detail") or "").strip()
        return bool(
            "formal_publication_blocked_unpublished" in detail
            or "formal_publication" in detail
            and "publication_blocked_unpublished" in detail
        )

    def _resolved_intraday_failure_by_later_daily_publication(
        issue: dict[str, Any],
    ) -> dict[str, Any] | None:
        code = str((issue or {}).get("code") or "").strip()
        if code != "intraday_refresh_failed":
            return None
        intraday_run = (
            overview.get("intraday_run")
            if isinstance(overview.get("intraday_run"), dict)
            else {}
        )
        recovery_run = daily_run if isinstance(daily_run, dict) else {}
        failed_finished_at = parse_local_datetime(intraday_run.get("finished_at"))
        recovery_finished_at = parse_local_datetime(recovery_run.get("finished_at"))
        recovery_run_id = _clean_run_id(
            recovery_run.get("screening_run_id")
            or recovery_run.get("report_refresh_run_id")
        )
        recovery_is_later = bool(
            failed_finished_at
            and recovery_finished_at
            and recovery_finished_at > failed_finished_at
        )
        lineage_matches_recovery = bool(
            recovery_run_id
            and recovery_run_id == daily_screening_run_id
            and recovery_run_id == latest_canonical_run_id
            and recovery_run_id == current_published_run_id
            and recovery_run_id == latest_successful_run_id
        )
        publication_and_snapshot_aligned = bool(
            current_publication_formal_degradation_resolved
            and lineage_aligned
            and (
                current_snapshot_aligned
                or publish_snapshot_effectively_aligned
                or latest_formal_processed_run_aligned
            )
        )
        if not all(
            [
                str(intraday_run.get("status") or "").strip().lower() == "failed",
                str(recovery_run.get("status") or "").strip().lower() == "success",
                bool(recovery_run.get("screening_triggered")),
                recovery_is_later,
                lineage_matches_recovery,
                publication_and_snapshot_aligned,
            ]
        ):
            return None
        return {
            "root_cause_status": "resolved_by_later_daily_formal_publication",
            "root_cause_resolved": True,
            "raw_signal": True,
            "effective_signal": False,
            "current_warning_suppressed": True,
            "suppression_reason": "later_equivalent_refresh_formally_published",
            "manual_intervention_required": False,
            "current_interpretation": (
                "历史盘中补刷新失败已由时间更晚的日更同能力链路完成采集、风险重算、筛选和正式发布；"
                "当前正式快照与调度血缘一致，因此旧失败不再代表当前自动化阻断。"
            ),
            "failed_task_key": "intraday_refresh",
            "failed_at": intraday_run.get("finished_at"),
            "recovery_task_key": "daily_refresh",
            "recovered_at": recovery_run.get("finished_at"),
            "recovery_run_id": recovery_run_id,
            "daily_run_later_than_failure": recovery_is_later,
            "lineage_aligned": lineage_aligned,
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "truth_policy": "real_scheduler_timestamps_publication_registry_and_snapshot_alignment_only",
        }

    current_published_pointer_missing = bool(
        latest_successful_run_id
        and latest_successful_is_publishable
        and not current_published_run_id
    )
    weekly_degradation_summary = (
        (
            (
                (audit.get("runs") or {}).get("weekly_maintenance")
                if isinstance(audit.get("runs"), dict)
                else {}
            )
            or {}
        ).get("degradation_summary")
        or {}
    )
    weekly_degradation_summary = (
        weekly_degradation_summary
        if isinstance(weekly_degradation_summary, dict)
        else {}
    )
    nav_weekly_resolution = weekly_degradation_summary.get(
        "nav_history_archive_resolution_evidence"
    )
    nav_weekly_resolution = (
        nav_weekly_resolution if isinstance(nav_weekly_resolution, dict) else {}
    )
    rank_weekly_resolution = weekly_degradation_summary.get(
        "rank_history_archive_resolution_evidence"
    )
    rank_weekly_resolution = (
        rank_weekly_resolution if isinstance(rank_weekly_resolution, dict) else {}
    )
    weekly_resolution = (
        nav_weekly_resolution
        if nav_weekly_resolution.get("applicable")
        else rank_weekly_resolution
    )
    weekly_warning_root_cause_resolved = bool(
        weekly_resolution.get("applicable")
        and weekly_resolution.get("root_cause_resolved") is True
    )

    blocking_issue_codes: list[str] = []
    warning_issue_codes: list[str] = []
    resolved_warning_issue_codes: list[str] = []
    resolved_refresh_failure_details: dict[str, dict[str, Any]] = {}
    publication_issue_details: dict[str, Any] = {}
    if latest_successful_publishable_not_current:
        blocking_issue_codes.append("formal_publication_pointer_mismatch")
        publication_issue_details["formal_publication_pointer_mismatch"] = {
            "root_cause_status": "open_publication_pointer_mismatch",
            "root_cause_resolved": False,
            "manual_intervention_required": True,
            "recommended_action": "repair_publication_pointer",
            "current_interpretation": (
                "最新成功运行已通过发布门禁，但 current published 指针仍指向旧版本；"
                "这不再是正常暂缓发布状态，必须自动重建 publication registry 并写回 latest.json。"
            ),
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "latest_successful_is_publishable": True,
            "latest_successful_is_currently_published": False,
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
        }
    if current_published_pointer_missing:
        blocking_issue_codes.append("formal_publication_pointer_missing")
        publication_issue_details["formal_publication_pointer_missing"] = {
            "root_cause_status": "open_publication_pointer_missing",
            "root_cause_resolved": False,
            "manual_intervention_required": True,
            "recommended_action": "rebuild_publication_registry",
            "current_interpretation": (
                "最新成功运行已通过发布门禁，但 publication registry 缺少 current published 指针；"
                "正式发布闭环未成立。"
            ),
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
        }
    if latest_successful_legacy_hold_not_current:
        blocking_issue_codes.append("formal_publication_legacy_publish_hold")
        publication_issue_details["formal_publication_legacy_publish_hold"] = {
            "root_cause_status": "open_legacy_publish_hold",
            "root_cause_resolved": False,
            "manual_intervention_required": True,
            "recommended_action": "auto_rebuild_or_rescreen",
            "current_interpretation": (
                "最新成功运行出现旧版 publish_hold 且未正式发布；"
                "该状态已不再允许作为成功闭环。"
            ),
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "publish_hold_reason_codes": list(latest_successful_run.get("publish_hold_reason_codes") or []),
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
        }
    if latest_successful_blocked_not_current:
        blocking_issue_codes.append("formal_publication_latest_successful_blocked")
        publication_issue_details["formal_publication_latest_successful_blocked"] = {
            "root_cause_status": "open_publication_blocked",
            "root_cause_resolved": False,
            "manual_intervention_required": True,
            "recommended_action": "resolve_publish_blockers",
            "current_interpretation": (
                "最新成功运行未成为正式发布版本，且存在发布阻断；"
                "必须保留当前旧正式榜并暴露阻断原因，不能标记为健康成功。"
            ),
            "latest_successful_run_id": latest_successful_run_id,
            "current_published_run_id": current_published_run_id,
            "latest_successful_is_publishable": latest_successful_is_publishable,
            "publish_blockers": _compact_publish_blockers(latest_successful_publish_blockers),
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
        }
    for issue in open_issues:
        code = str((issue or {}).get("code") or "").strip()
        severity = str((issue or {}).get("severity") or "").strip().lower()
        if not code:
            continue
        if not autostock_observation_enabled and code.startswith("autostock_"):
            resolved_warning_issue_codes.append(code)
            continue
        if code == "weekly_maintenance_degraded" and weekly_warning_root_cause_resolved:
            resolved_warning_issue_codes.append(code)
            continue
        resolved_refresh_failure = _resolved_intraday_failure_by_later_daily_publication(
            issue if isinstance(issue, dict) else {}
        )
        if resolved_refresh_failure:
            resolved_warning_issue_codes.append(code)
            resolved_refresh_failure_details[code] = resolved_refresh_failure
            continue
        if _is_resolved_formal_publication_degradation(issue if isinstance(issue, dict) else {}):
            resolved_warning_issue_codes.append(code)
            continue
        if code == "snapshot_watchdog_skip_guard" and (
            current_snapshot_aligned
            or publish_snapshot_effectively_aligned
            or latest_formal_processed_run_aligned
        ):
            warning_issue_codes.append(code)
            continue
        if code == "screening_backlog_due" and (
            publish_snapshot_effectively_aligned
            or latest_formal_processed_run_aligned
        ):
            warning_issue_codes.append(code)
            continue
        is_blocking = bool(
            severity == "error"
            or code in {
                "startup_catch_up_pending",
                "screening_backlog_due",
                "scheduler_job_drift",
            }
            or code.endswith("_skip_guard")
        )
        if is_blocking:
            blocking_issue_codes.append(code)
        else:
            warning_issue_codes.append(code)

    persistent_missing_total = int(database_overview.get("autostock_shadow_persistent_missing_response_total") or 0)
    severe_persistent_missing_total = int(database_overview.get("autostock_shadow_severe_persistent_missing_response_total") or 0)
    persistent_missing_related_publish_total = int(
        database_overview.get("autostock_shadow_persistent_missing_response_related_publish_count") or 0
    )
    severe_persistent_missing_related_publish_total = int(
        database_overview.get("autostock_shadow_severe_persistent_missing_response_related_publish_count") or 0
    )
    persistent_tracking_ready = bool(database_overview.get("autostock_shadow_persistent_tracking_ready"))
    cooldown_active_today = bool(database_overview.get("autostock_shadow_cooldown_active_today"))
    if autostock_observation_enabled and not persistent_tracking_ready and "autostock_persistent_tracking_not_ready" not in warning_issue_codes:
        warning_issue_codes.append("autostock_persistent_tracking_not_ready")
    if (
        autostock_observation_enabled
        and persistent_missing_total > 0
        and persistent_missing_related_publish_total > 0
        and (
            severe_persistent_missing_related_publish_total > 0
            or not cooldown_active_today
            or not persistent_tracking_ready
        )
        and "autostock_persistent_missing_response" not in warning_issue_codes
    ):
        warning_issue_codes.append("autostock_persistent_missing_response")
    if (
        autostock_observation_enabled
        and severe_persistent_missing_total > 0
        and severe_persistent_missing_related_publish_total > 0
        and "autostock_severe_persistent_missing_response" not in warning_issue_codes
    ):
        warning_issue_codes.append("autostock_severe_persistent_missing_response")

    scheduler_running = bool(overview.get("scheduler_running"))
    host_context_mismatch = bool(overview.get("host_context_mismatch"))
    raw_startup_catch_up_pending = bool(overview.get("startup_catch_up_pending"))
    raw_screening_backlog_due = bool(overview.get("screening_backlog_due"))
    current_task = dict(overview.get("current_task") or {})
    pending_manual_run_requests = overview.get("pending_manual_run_requests") or []
    manual_run_history = overview.get("manual_run_history") or []
    current_task_name = str(current_task.get("task_name") or "").strip()
    automation_recovery_in_progress = bool(scheduler_running and current_task_name)
    manual_run_queue_summary = _build_manual_run_queue_summary(
        current_task,
        pending_manual_run_requests,
        manual_run_history,
    )
    current_task_key = _scheduler_task_key_for_task_name(current_task_name)
    startup_catch_up_pending = raw_startup_catch_up_pending
    screening_backlog_due = bool(
        raw_screening_backlog_due
        and not publish_snapshot_effectively_aligned
        and not latest_formal_processed_run_aligned
    )
    startup_catch_up_publish_aligned = bool(
        raw_startup_catch_up_pending
        and (
            publish_snapshot_effectively_aligned
            or latest_formal_processed_run_aligned
        )
    )
    if startup_catch_up_publish_aligned:
        startup_catch_up_pending = False
        blocking_issue_codes = [
            code for code in blocking_issue_codes if code != "startup_catch_up_pending"
        ]
        warning_issue_codes.append("startup_catch_up_pending")

    current_task_recovery_issue_codes = [
        code
        for code in blocking_issue_codes
        if _is_current_task_recovery_issue(code, current_task_key)
    ]
    current_task_recovery_issue_codes = _dedupe_preserve_order(current_task_recovery_issue_codes)
    recovery_only_blockers = {
        "startup_catch_up_pending",
        "screening_backlog_due",
        *current_task_recovery_issue_codes,
    }
    non_recovery_blocking_issue_codes = [
        code for code in blocking_issue_codes if code not in recovery_only_blockers
    ]
    if automation_recovery_in_progress and not non_recovery_blocking_issue_codes:
        startup_catch_up_pending = False
        screening_backlog_due = False
        warning_issue_codes.extend(blocking_issue_codes)
        blocking_issue_codes = []

    blocking_issue_codes = _dedupe_preserve_order(blocking_issue_codes)
    warning_issue_codes = _dedupe_preserve_order(warning_issue_codes)
    resolved_warning_issue_codes = _dedupe_preserve_order(resolved_warning_issue_codes)
    warning_issue_details: dict[str, Any] = {}
    resolved_warning_issue_details: dict[str, Any] = {}
    resolved_warning_issue_details.update(resolved_refresh_failure_details)
    if raw_screening_backlog_due and not screening_backlog_due and "screening_backlog_due" in warning_issue_codes:
        if latest_formal_processed_run_aligned:
            downgrade_reason = "latest_formal_processed_run_aligned"
            current_interpretation = (
                "有效净值快照已由具备正式血缘的运行处理，但后续诊断运行不可发布；"
                "当前无需人工补跑，保留 warning 用于跟踪发布治理。"
            )
        elif publish_snapshot_effectively_aligned:
            downgrade_reason = "current_published_snapshot_aligned"
            current_interpretation = (
                "有效净值快照与当前发布/已筛选快照等效一致；原始待补筛信号不需要人工处理。"
            )
        else:
            downgrade_reason = "automation_recovery_overlay"
            current_interpretation = (
                "原始待补筛信号已被自动恢复流程降级；当前无需人工介入。"
            )
        latest_successful_snapshot = (
            dict(latest_successful_run.get("snapshot") or {})
            if isinstance(latest_successful_run, dict)
            else {}
        )
        latest_formal_processed_run = (
            dict(publication_layers.get("latest_formal_processed_run") or {})
            if isinstance(publication_layers, dict)
            else {}
        )
        latest_formal_processed_snapshot = dict(latest_formal_processed_run.get("snapshot") or {})
        warning_issue_details["screening_backlog_due"] = {
            "root_cause_status": "downgraded_to_warning",
            "root_cause_resolved": bool(
                publish_snapshot_effectively_aligned
                or latest_formal_processed_run_aligned
            ),
            "raw_signal": True,
            "effective_signal": False,
            "manual_intervention_required": False,
            "downgrade_reason": downgrade_reason,
            "current_interpretation": current_interpretation,
            "screening_backlog_reason": str(overview.get("screening_backlog_reason") or "").strip() or None,
            "effective_nav_snapshot": _snapshot_date_and_signature(effective_nav_snapshot),
            "latest_screened_snapshot": _snapshot_date_and_signature(latest_screened_snapshot),
            "latest_successful_run": {
                "run_id": _clean_run_id(latest_successful_run.get("run_id"))
                if isinstance(latest_successful_run, dict)
                else None,
                "publish_hold": bool(latest_successful_run.get("publish_hold"))
                if isinstance(latest_successful_run, dict)
                else False,
                "snapshot": _snapshot_date_and_signature(latest_successful_snapshot),
            },
            "latest_formal_processed_run": {
                "run_id": _clean_run_id(latest_formal_processed_run.get("run_id")),
                "publishable": bool(latest_formal_processed_run.get("publishable")),
                "publish_hold": bool(latest_formal_processed_run.get("publish_hold")),
                "published": bool(latest_formal_processed_run.get("published")),
                "snapshot": _snapshot_date_and_signature(latest_formal_processed_snapshot),
            },
            "truth_policy": "real_scheduler_state_publication_registry_and_snapshot_alignment_only",
        }
    if (
        raw_startup_catch_up_pending
        and not startup_catch_up_pending
        and "startup_catch_up_pending" in warning_issue_codes
    ):
        if startup_catch_up_publish_aligned:
            downgrade_reason = (
                "latest_formal_processed_run_aligned"
                if latest_formal_processed_run_aligned
                else "current_published_snapshot_aligned"
            )
            current_interpretation = (
                "启动补跑原始信号存在，但发布快照已对齐或最新正式血缘运行已处理当前快照；"
                "当前无需人工补跑。"
            )
        else:
            downgrade_reason = "automation_recovery_overlay"
            current_interpretation = "启动补跑原始信号正在由当前自动任务恢复，当前无需人工介入。"
        warning_issue_details["startup_catch_up_pending"] = {
            "root_cause_status": "downgraded_to_warning",
            "root_cause_resolved": bool(
                startup_catch_up_publish_aligned or automation_recovery_in_progress
            ),
            "raw_signal": True,
            "effective_signal": False,
            "manual_intervention_required": False,
            "downgrade_reason": downgrade_reason,
            "current_interpretation": current_interpretation,
            "startup_catch_up_due_jobs": list(overview.get("startup_catch_up_due_jobs") or [])[:12],
            "current_task_name": current_task_name or None,
            "truth_policy": "real_scheduler_state_publication_registry_and_recovery_overlay_only",
        }
    if (
        (
            "weekly_maintenance_degraded" in warning_issue_codes
            or "weekly_maintenance_degraded" in resolved_warning_issue_codes
        )
        and weekly_resolution.get("applicable")
    ):
        weekly_subprocess_health = (
            weekly_run_payload.get("collection_subprocess_health")
            if isinstance(weekly_run_payload.get("collection_subprocess_health"), dict)
            else {}
        )
        latest_resolution_evidence = dict(weekly_resolution.get("latest_resolution_evidence") or {})
        if weekly_subprocess_health:
            if not str(latest_resolution_evidence.get("artifact_path") or "").strip():
                latest_resolution_evidence["artifact_path"] = weekly_subprocess_health.get(
                    "subprocess_index_path"
                )
            latest_resolution_evidence.setdefault(
                "blocker_count",
                _to_int(weekly_subprocess_health.get("blocker_count")),
            )
            latest_resolution_evidence.setdefault("stale_hot_missing_archive_key_row_count", 0)
            latest_resolution_evidence.setdefault(
                "runner_status",
                "weekly_collection_partial_failure_visible",
            )
            latest_resolution_evidence.setdefault(
                "warning_count",
                _to_int(weekly_subprocess_health.get("warning_count")),
            )
            latest_resolution_evidence.setdefault(
                "affected_tasks",
                [
                    task_name
                    for task_name, task in (weekly_subprocess_health.get("tasks") or {}).items()
                    if isinstance(task, dict) and (task.get("warnings") or task.get("blockers"))
                ]
                if isinstance(weekly_subprocess_health.get("tasks"), dict)
                else [],
            )
        weekly_detail = {
            "root_cause_status": weekly_resolution.get("root_cause_status"),
            "root_cause_resolved": bool(weekly_resolution.get("root_cause_resolved")),
            "current_interpretation": weekly_resolution.get("current_interpretation"),
            "latest_resolution_evidence": latest_resolution_evidence,
            "truth_policy": weekly_resolution.get("truth_policy"),
        }
        if "weekly_maintenance_degraded" in warning_issue_codes:
            warning_issue_details["weekly_maintenance_degraded"] = dict(weekly_detail)
        if "weekly_maintenance_degraded" in resolved_warning_issue_codes:
            resolved_warning_issue_details["weekly_maintenance_degraded"] = {
                **weekly_detail,
                "raw_signal": True,
                "effective_signal": False,
                "current_warning_suppressed": True,
                "suppression_reason": "root_cause_resolved_by_later_read_only_evidence",
                "manual_intervention_required": False,
            }
    if "weekly_maintenance_degraded" in warning_issue_codes and "weekly_maintenance_degraded" not in warning_issue_details:
        weekly_subprocess_health = (
            weekly_run_payload.get("collection_subprocess_health")
            if isinstance(weekly_run_payload.get("collection_subprocess_health"), dict)
            else {}
        )
        weekly_degradation_summary = (
            weekly_run_payload.get("degradation_summary")
            if isinstance(weekly_run_payload.get("degradation_summary"), dict)
            else {}
        )
        weekly_degraded_stage = str(
            weekly_run_payload.get("degraded_stage")
            or weekly_degradation_summary.get("degraded_stage")
            or ""
        ).strip()
        weekly_warning_reason = str(
            weekly_run_payload.get("warning_reason")
            or weekly_degradation_summary.get("warning_reason")
            or weekly_run_payload.get("corrected_warning_reason")
            or ""
        ).strip()
        weekly_degradation_category = str(
            weekly_degradation_summary.get("category") or ""
        ).strip()
        collection_affected_tasks = [
            task_name
            for task_name, task in (weekly_subprocess_health.get("tasks") or {}).items()
            if isinstance(task, dict) and (task.get("warnings") or task.get("blockers"))
        ] if isinstance(weekly_subprocess_health.get("tasks"), dict) else []
        # The persisted primary degradation is evidence even when the parser
        # did not emit an additional warning for that same subprocess.
        if (weekly_degraded_stage in (weekly_subprocess_health.get("tasks") or {})
                and weekly_degraded_stage not in collection_affected_tasks):
            collection_affected_tasks.append(weekly_degraded_stage)
        primary_cause_is_collection = bool(
            weekly_degraded_stage
            in {"collect-funds", "collect-details", "collect-managers", "collect-companies", "collect-period-returns", "bond-universe-sync"}
            or weekly_degradation_category == "upstream_api_unavailable"
        )
        latest_resolution_evidence = {
            "artifact_path": (
                weekly_degradation_summary.get("task_run_audit_json_project_path")
                or weekly_run_payload.get("task_run_audit_json_project_path")
                or weekly_degradation_summary.get("task_log_file_project_path")
                or weekly_run_payload.get("task_log_file_project_path")
            ),
            "runner_status": weekly_warning_reason or weekly_degraded_stage or "weekly_stage_degraded",
            "degraded_stage": weekly_degraded_stage or None,
            "warning_reason": weekly_warning_reason or None,
            "category": weekly_degradation_category or None,
            "impact_code": weekly_degradation_summary.get("impact_code"),
            "root_error": weekly_degradation_summary.get("root_error"),
            "issue_detail": weekly_degradation_summary.get("issue_detail"),
            "primary_cause_is_collection": primary_cause_is_collection,
        }
        historical_correction = weekly_run_payload.get("historical_warning_correction") or {}
        if historical_correction.get("verified"):
            latest_resolution_evidence["historical_warning_correction"] = historical_correction
        if weekly_subprocess_health:
            latest_resolution_evidence["collection_observation"] = {
                "artifact_path": weekly_subprocess_health.get("subprocess_index_path"),
                "blocker_count": _to_int(weekly_subprocess_health.get("blocker_count")),
                "warning_count": _to_int(weekly_subprocess_health.get("warning_count")),
                "affected_tasks": collection_affected_tasks,
                "primary_cause": primary_cause_is_collection,
            }
        weekly_audit_path = _resolve_project_artifact_path(
            weekly_run_payload.get("task_run_audit_json_project_path"),
            project_root=Path(__file__).resolve().parents[1],
        )
        publication_evidence = weekly_publication_evidence(
            weekly_run_payload, _read_json_object(weekly_audit_path),
        )
        latest_resolution_evidence["publication_evidence"] = publication_evidence
        publication_interpretation = weekly_publication_interpretation(publication_evidence)
        if primary_cause_is_collection:
            current_interpretation = (
                f"{publication_interpretation}；主降级阶段为 "
                f"{weekly_degraded_stage or 'upstream-collection'}"
                f"（{weekly_warning_reason or '上游采集未完全成功'}）；"
                "调度器会在下一个采集周期自动重试，缺失业务值保持缺失语义。"
            )
        else:
            secondary_observation = (
                "并存的上游采集告警仅作为次级观察证据，不是本次 degraded 的主因。"
                if collection_affected_tasks
                else ""
            )
            current_interpretation = (
                f"{publication_interpretation}；主降级阶段为 "
                f"{weekly_degraded_stage or 'unknown'}"
                f"（{weekly_warning_reason or '未提供 warning_reason'}）；"
                "该自动治理阶段未闭环，将由下一个周维护窗口重试。"
                f"{secondary_observation}"
            )
        warning_issue_details["weekly_maintenance_degraded"] = {
            "root_cause_status": "unresolved_weekly_stage_failure",
            "root_cause_resolved": False,
            "primary_degraded_stage": weekly_degraded_stage or None,
            "primary_warning_reason": weekly_warning_reason or None,
            "primary_cause_is_collection": primary_cause_is_collection,
            "current_interpretation": current_interpretation,
            "latest_resolution_evidence": latest_resolution_evidence,
            "truth_policy": (
                "persisted_scheduler_stage_and_read_only_runtime_artifact_evidence_no_state_mutation"
            ),
        }
    for issue in open_issues:
        if not isinstance(issue, dict):
            continue
        code = str(issue.get("code") or "").strip()
        if not code or code not in resolved_warning_issue_codes:
            continue
        if code in resolved_warning_issue_details:
            continue
        if not _is_resolved_formal_publication_degradation(issue):
            continue
        resolved_warning_issue_details[code] = _formal_publication_degradation_resolved_detail(issue)
    if formal_publication_stale_publish_hold:
        publication_issue_details.setdefault(
            "formal_publication_stale_publish_hold",
            {
                "root_cause_status": "open_legacy_publish_hold_stale_publication",
                "root_cause_resolved": False,
                "manual_intervention_required": True,
                "recommended_action": "auto_rebuild_or_rescreen",
                "current_interpretation": (
                    "最新运行出现旧版 publish_hold 状态且未成为正式发布；"
                    "该状态不允许作为 warning 或成功闭环，必须按发布阻断处理。"
                ),
                **publish_hold_staleness_summary,
            },
        )
        if "formal_publication_stale_publish_hold" not in blocking_issue_codes:
            blocking_issue_codes.append("formal_publication_stale_publish_hold")
        warning_issue_codes = [
            code for code in warning_issue_codes
            if code != "formal_publication_stale_publish_hold"
        ]
        warning_issue_details.pop("formal_publication_stale_publish_hold", None)
    blocking_issue_codes = _dedupe_preserve_order(blocking_issue_codes)
    warning_issue_codes = _dedupe_preserve_order(warning_issue_codes)

    operator_attention_required = bool(
        host_context_mismatch
        or "scheduler_host_context_mismatch" in warning_issue_codes
        or "scheduler_host_context_mismatch" in blocking_issue_codes
    )
    manual_intervention_required = bool(
        not scheduler_running
        or startup_catch_up_pending
        or screening_backlog_due
        or blocking_issue_codes
    )
    automatic_execution_ready = not manual_intervention_required
    automatic_recovering = bool(
        automation_recovery_in_progress
        and automatic_execution_ready
        and not manual_intervention_required
    )

    issue_by_code = {str(item.get("code") or ""): item for item in open_issues if isinstance(item, dict)}
    jobs_by_id = {str(item.get("id") or ""): item for item in overview_payload.get("jobs") or [] if isinstance(item, dict)}
    warning_items = []
    for code in warning_issue_codes:
        issue = issue_by_code.get(code) or {}
        detail = warning_issue_details.get(code) or {}
        label = str(issue.get("label") or "自动任务待复核")
        next_action_at = None
        if code == "weekly_maintenance_degraded":
            label = "周维护采集不完整" if detail.get("primary_cause_is_collection") else "周维护未完成"
            next_action_at = (overview_payload.get("weekly_job") or jobs_by_id.get("weekly_maintenance") or {}).get("next_run_time")
        elif code.startswith("daily_refresh_"):
            next_action_at = (overview_payload.get("daily_job") or jobs_by_id.get("daily_refresh") or {}).get("next_run_time")
        elif code.startswith("intraday_refresh_"):
            next_action_at = (overview_payload.get("intraday_job") or jobs_by_id.get("intraday_refresh") or {}).get("next_run_time")
        elif code.startswith("snapshot_watchdog_"):
            next_action_at = (overview_payload.get("watchdog_job") or jobs_by_id.get("snapshot_watchdog") or {}).get("next_run_time")
        elif code.startswith("weekly_maintenance_"):
            next_action_at = (overview_payload.get("weekly_job") or jobs_by_id.get("weekly_maintenance") or {}).get("next_run_time")
        warning_items.append({"code": code, "label": label,
                              "detail": detail.get("current_interpretation") or issue.get("detail"),
                              "next_automatic_action_at": next_action_at})

    if manual_intervention_required:
        status = "attention_required"
        summary = "自动闭环未收口，当前仍需人工复核或补跑。"
    elif automatic_recovering:
        status = "automatic_recovering"
        summary = "自动闭环正在恢复或补跑中，当前无需人工介入。"
    elif operator_attention_required or warning_issue_codes:
        status = "automatic_with_warnings"
        labels = list(dict.fromkeys(item["label"] for item in warning_items))
        summary = "自动流程可运行；" + "、".join(labels[:2] or ["存在待复核告警"]) + (f"等 {len(labels)} 项告警。" if len(labels) > 2 else "。")
    else:
        status = "automatic"
        summary = "自动闭环正常，无需人工干预。"

    return {
        "status": status,
        "run_id": latest_canonical_run_id,
        "latest_canonical_run_id": latest_canonical_run_id,
        "current_published_run_id": current_published_run_id,
        "latest_successful_run_id": latest_successful_run_id,
        "daily_screening_run_id": daily_screening_run_id,
        "automation_lineage": {
            "available": lineage_available,
            "source": "publication_registry_and_scheduler_state",
            "truth_policy": "real_publication_registry_and_scheduler_state_only",
            "latest_canonical_run_id": latest_canonical_run_id,
            "current_published_run_id": current_published_run_id,
            "latest_successful_run_id": latest_successful_run_id,
            "daily_screening_run_id": daily_screening_run_id,
            "daily_run_status": daily_run.get("status"),
            "daily_run_finished_at": daily_run.get("finished_at"),
            "daily_screening_evidence": daily_screening_evidence(daily_run),
            "lineage_aligned": lineage_aligned,
        },
        "automatic_execution_ready": automatic_execution_ready,
        "manual_intervention_required": manual_intervention_required,
        "operator_attention_required": operator_attention_required,
        "manual_operations_enabled": bool(manual_operations_enabled),
        "blocking_issue_codes": blocking_issue_codes,
        "warning_issue_codes": warning_issue_codes,
        "warning_items": warning_items,
        "publication_issue_details": publication_issue_details,
        "warning_issue_details": warning_issue_details,
        "resolved_warning_issue_codes": resolved_warning_issue_codes,
        "resolved_warning_issue_details": resolved_warning_issue_details,
        "scheduler_running": scheduler_running,
        "automatic_recovering": automatic_recovering,
        "startup_catch_up_pending": startup_catch_up_pending,
        "screening_backlog_due": screening_backlog_due,
        "startup_catch_up_pending_raw": raw_startup_catch_up_pending,
        "screening_backlog_due_raw": raw_screening_backlog_due,
        "host_context_mismatch": host_context_mismatch,
        "automation_recovery_in_progress": automation_recovery_in_progress,
        "recovery_in_progress_issue_codes": current_task_recovery_issue_codes,
        "current_task_name": current_task_name or None,
        "current_task_key": current_task_key,
        "publish_snapshot_effectively_aligned": publish_snapshot_effectively_aligned,
        "latest_successful_publish_hold_aligned": latest_successful_publish_hold_aligned,
        "latest_formal_processed_run_aligned": latest_formal_processed_run_aligned,
        "latest_successful_is_publishable": latest_successful_is_publishable,
        "latest_successful_is_currently_published": latest_successful_is_currently_published,
        "latest_successful_publishable_not_current": latest_successful_publishable_not_current,
        "formal_publication_stale_publish_hold": formal_publication_stale_publish_hold,
        "formal_publication_staleness": publish_hold_staleness_summary,
        "autostock_observation_enabled": autostock_observation_enabled,
        "autostock_observation_policy": (
            "current_provider_observation" if autostock_observation_enabled
            else "retired_provider_history_only_no_health_or_publication_gate"
        ),
        "autostock_persistent_tracking_ready": persistent_tracking_ready,
        "autostock_persistent_missing_response_total": persistent_missing_total,
        "autostock_severe_persistent_missing_response_total": severe_persistent_missing_total,
        "autostock_persistent_missing_response_related_publish_count": persistent_missing_related_publish_total,
        "autostock_severe_persistent_missing_response_related_publish_count": severe_persistent_missing_related_publish_total,
        "autostock_next_retry_business_date": database_overview.get("autostock_shadow_next_retry_business_date"),
        "manual_run_queue_summary": manual_run_queue_summary,
        "summary": summary,
    }


def summarize_scheduler_snapshot_for_api(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(snapshot or {})
    coverage_semantics = build_risk_coverage_threshold_semantics(
        {
            "coverage_ratio_vs_candidate": payload.get("risk_1y_effective_coverage_ratio_vs_candidate"),
            "minimum_coverage_ratio": payload.get("minimum_coverage_ratio"),
            "publish_minimum_coverage_ratio": payload.get("publish_minimum_coverage_ratio"),
            "observation_coverage_ratio": payload.get("observation_coverage_ratio"),
        },
        coverage_sufficient=(
            bool(payload.get("coverage_sufficient"))
            if payload.get("coverage_sufficient") is not None
            else None
        ),
    )
    return {
        "candidate_date": payload.get("candidate_date"),
        "signature": payload.get("signature"),
        "ready": payload.get("ready"),
        "readiness_reason": payload.get("readiness_reason"),
        "candidate_fund_count": payload.get("candidate_fund_count"),
        "coverage_ratio_vs_total": payload.get("coverage_ratio_vs_total"),
        "raw_max_latest_nav_date": payload.get("raw_max_latest_nav_date"),
        "risk_1y_effective_end_date": payload.get("risk_1y_effective_end_date"),
        "risk_1y_effective_fund_count": payload.get("risk_1y_effective_fund_count"),
        "risk_1y_effective_coverage_ratio_vs_candidate": coverage_semantics["coverage_ratio_vs_candidate"],
        "risk_1y_publish_minimum_coverage_ratio": coverage_semantics["publish_minimum_coverage_ratio"],
        "risk_1y_observation_coverage_ratio": coverage_semantics["observation_coverage_ratio"],
        "risk_1y_coverage_sufficient": coverage_semantics["coverage_sufficient"],
        "risk_1y_coverage_observation_line_met": coverage_semantics["coverage_observation_line_met"],
        "risk_1y_coverage_gate_status": coverage_semantics["coverage_gate_status"],
        "risk_1y_coverage_gate_label": coverage_semantics["coverage_gate_label"],
        "risk_1y_coverage_gate_explanation": coverage_semantics["coverage_gate_explanation"],
    }


def summarize_scheduler_run_for_api(
    run: dict[str, Any] | None,
    *,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    raw_payload = dict(run or {})
    payload = dict(raw_payload)
    task_name = str(payload.get("task_name") or "").strip()
    inferred_task_key = next(
        (key for key, name in SCHEDULER_TASK_NAME_BY_KEY.items() if name == task_name),
        None,
    )
    full_maintenance_result = task_name == "scheduled_weekly_bond_maintenance" and any(
        isinstance(payload.get(key), dict) for key in MAINTENANCE_CHILD_KEYS
    )
    if payload and (full_maintenance_result or (
        payload.get("action_category") is None and payload.get("no_action_category") is None
    )):
        payload = build_task_closure_summary(
            task_key=inferred_task_key or "unknown",
            task_name=task_name or "unknown",
            result=payload,
        )
        if raw_payload:
            for key in (
                "degraded",
                "degraded_stage",
                "records_processed",
                "records_succeeded",
                "records_failed",
                "detail_records_processed",
                "detail_records_succeeded",
                "detail_records_failed",
                "detail_records_skipped",
                "detail_collection_summary",
                "detail_collection_summary_errors",
            ):
                if key in raw_payload and (raw_payload.get(key) not in (None, "") or key.startswith("detail_")):
                    payload[key] = raw_payload.get(key)
            payload["degradation_summary"] = build_scheduler_run_degradation_summary(
                raw_payload,
                task_key=inferred_task_key,
                project_root=project_root,
            )
    nav_history_consistency_audit = payload.get("nav_history_consistency_audit") or {}
    child_tasks = dict(payload.get("child_tasks") or {})
    summarized_nav_history_consistency_audit = {}
    if isinstance(nav_history_consistency_audit, dict) and nav_history_consistency_audit:
        summarized_nav_history_consistency_audit = {
            "status": nav_history_consistency_audit.get("status"),
            "artifact_path": nav_history_consistency_audit.get("artifact_path"),
            "markdown_path": nav_history_consistency_audit.get("markdown_path"),
            "summary": nav_history_consistency_audit.get("summary") or {},
            "actionable_issue_counts": nav_history_consistency_audit.get("actionable_issue_counts") or {},
            "informational_issue_counts": nav_history_consistency_audit.get("informational_issue_counts") or {},
        }
    summarized_child_tasks = {}

    def _drop_heavy_sample_payload(value: Any) -> Any:
        if isinstance(value, dict):
            compact: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key = str(raw_key)
                if "sample" in key.lower() and isinstance(raw_value, (list, dict)):
                    continue
                compact[raw_key] = _drop_heavy_sample_payload(raw_value)
            return compact
        if isinstance(value, list):
            return [_drop_heavy_sample_payload(item) for item in value]
        return value

    task_history_governance_preview = _drop_heavy_sample_payload(
        payload.get("task_history_governance_preview") or {}
    )
    task_history_cleanup_execution = _drop_heavy_sample_payload(
        payload.get("task_history_cleanup_execution") or {}
    )
    screening_storage_growth_audit = _drop_heavy_sample_payload(
        payload.get("screening_storage_growth_audit") or {}
    )
    if isinstance(screening_storage_growth_audit, dict):
        screening_storage_growth_audit.pop("daily_rows", None)
        screening_storage_growth_audit.pop("top_noisy_runs", None)

    for child_key, child_payload in child_tasks.items():
        if not isinstance(child_key, str):
            continue
        child = dict(child_payload or {})
        if not child:
            continue
        if child_key == "regression_smoke":
            summarized_child_tasks[child_key] = {
                "status": child.get("status"),
                "candidate_total": child.get("candidate_total"),
                "processed_total": child.get("processed_total"),
                "changed_total": child.get("changed_total"),
                "overall_status": child.get("overall_status"),
                "issue_classification_status": child.get("issue_classification_status"),
                "issue_classification_label": child.get("issue_classification_label"),
                "issue_category_counts": child.get("issue_category_counts") or {},
                "failed_command_total": child.get("failed_command_total"),
                "failed_group_classifications": child.get("failed_group_classifications") or [],
                "expected_warning_total": child.get("expected_warning_total"),
                "test_expectation_mismatch_total": child.get("test_expectation_mismatch_total"),
                "production_degradation_total": child.get("production_degradation_total"),
                "environment_issue_total": child.get("environment_issue_total"),
                "unknown_failure_total": child.get("unknown_failure_total"),
                "summary_path": child.get("summary_path"),
                "artifact_dir": child.get("artifact_dir"),
                "generated_at": child.get("generated_at"),
                "source": child.get("source"),
                "replay_precision_status": child.get("replay_precision_status"),
                "replay_precision_status_label": child.get("replay_precision_status_label"),
                "auxiliary_scope": child.get("auxiliary_scope"),
                "auxiliary_scope_label": child.get("auxiliary_scope_label"),
                "scope_detail": child.get("scope_detail"),
            }
            continue
        summarized_child_tasks[child_key] = {
            "status": child.get("status"),
            "skip_reason": child.get("skip_reason"),
            "idle_reason": child.get("idle_reason"),
            "no_action_category": child.get("no_action_category"),
            "candidate_total": child.get("candidate_total"),
            "processed_total": child.get("processed_total"),
            "changed_total": child.get("changed_total"),
            "failed_reason_summary": _drop_heavy_sample_payload(
                child.get("failed_reason_summary") or {}
            ),
            "failed_reason_governance_summary": _drop_heavy_sample_payload(
                child.get("failed_reason_governance_summary") or {}
            ),
            "upstream_observation_summary": _drop_heavy_sample_payload(
                child.get("upstream_observation_summary") or {}
            ),
            "priority_blocked_summary": _drop_heavy_sample_payload(
                child.get("priority_blocked_summary") or {}
            ),
            "governance_queue_consumption_audit": _drop_heavy_sample_payload(
                child.get("governance_queue_consumption_audit") or {}
            ),
            "coverage_progress": _drop_heavy_sample_payload(child.get("coverage_progress") or {}),
            "idle_streak": child.get("idle_streak"),
            **(maintenance_metrics(child, legacy_summary=True) if child_key in MAINTENANCE_CHILD_KEYS else {}),
        }
    semantic_status = classify_scheduler_run_semantic_status(
        status=payload.get("status"),
        skip_reason=payload.get("skip_reason") or payload.get("warning_reason"),
        no_action_category=payload.get("no_action_category"),
        degraded=bool(payload.get("degraded")),
    )
    degradation_summary = (
        payload.get("degradation_summary")
        or build_scheduler_run_degradation_summary(
            payload,
            task_key=inferred_task_key,
            project_root=project_root,
        )
    )
    manual_intervention_hint = bool(
        payload.get("manual_intervention_hint")
        if payload.get("manual_intervention_hint") is not None
        else semantic_status.get("manual_intervention_hint")
    )
    if isinstance(degradation_summary, dict) and "manual_intervention_required" in degradation_summary:
        manual_intervention_hint = bool(
            degradation_summary.get("manual_intervention_required")
        )
    return {
        "task_name": payload.get("task_name"),
        "status": payload.get("status"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        **copy_task_timing_fields(raw_payload, payload),
        "task_history_id": payload.get("task_history_id"),
        "maintenance_metrics_source": deepcopy(payload.get("maintenance_metrics_source") or {}),
        "screening_triggered": bool(payload.get("screening_triggered")),
        "maintenance_rescreen_triggered": bool(payload.get("maintenance_rescreen_triggered")),
        "maintenance_rescreen_run_id": payload.get("maintenance_rescreen_run_id"),
        "screening_reason": payload.get("screening_reason"),
        "skip_reason": payload.get("skip_reason"),
        "warning_reason": payload.get("warning_reason"),
        "action_category": payload.get("action_category"),
        "no_action_category": payload.get("no_action_category"),
        "run_semantic_status": payload.get("run_semantic_status") or semantic_status.get("run_semantic_status"),
        "skip_semantic_status": payload.get("skip_semantic_status") or semantic_status.get("skip_semantic_status"),
        "skip_reason_category": payload.get("skip_reason_category") or semantic_status.get("skip_reason_category"),
        "manual_intervention_hint": manual_intervention_hint,
        "degraded": bool(payload.get("degraded")),
        "degraded_stage": payload.get("degraded_stage"),
        "degradation_summary": degradation_summary,
        "screening_run_id": payload.get("screening_run_id"),
        "screening_final_count": payload.get("screening_final_count"),
        "report_refresh_triggered": bool(payload.get("report_refresh_triggered")),
        "report_refresh_run_id": payload.get("report_refresh_run_id"),
        "records_processed": payload.get("records_processed"),
        "records_succeeded": payload.get("records_succeeded"),
        "records_failed": payload.get("records_failed"),
        **({"historical_collection_counts": raw_payload["historical_collection_counts"]}
           if "historical_collection_counts" in raw_payload else {}),
        "detail_records_processed": payload.get("detail_records_processed"),
        "detail_records_succeeded": payload.get("detail_records_succeeded"),
        "detail_records_failed": payload.get("detail_records_failed"),
        "detail_records_skipped": payload.get("detail_records_skipped"),
        "detail_collection_summary": payload.get("detail_collection_summary"),
        "detail_collection_summary_errors": payload.get("detail_collection_summary_errors"),
        "scheduled_for": payload.get("scheduled_for"),
        "start_delay_seconds": payload.get("start_delay_seconds"),
        "started_late": bool(payload.get("started_late")),
        "startup_catch_up_suspected": bool(payload.get("startup_catch_up_suspected")),
        "startup_catch_up_origin": payload.get("startup_catch_up_origin"),
        "scheduler_start_gap_seconds": payload.get("scheduler_start_gap_seconds"),
        "child_tasks": summarized_child_tasks,
        "collection_subprocess_health": _drop_heavy_sample_payload(
            payload.get("collection_subprocess_health") or {}
        ),
        "nav_history_consistency_audit": summarized_nav_history_consistency_audit,
        "nav_history_consistency_audit_error": payload.get("nav_history_consistency_audit_error"),
        "screening_storage_growth_audit": screening_storage_growth_audit,
        "screening_storage_growth_audit_error": payload.get("screening_storage_growth_audit_error"),
        "task_history_governance_preview": task_history_governance_preview,
        "task_history_governance_preview_error": payload.get("task_history_governance_preview_error"),
        "task_history_cleanup_execution": task_history_cleanup_execution,
        "task_history_cleanup_execution_error": payload.get("task_history_cleanup_execution_error"),
        "maintenance_backup_retention": _drop_heavy_sample_payload(raw_payload.get("maintenance_backup_retention") or {}),
        "maintenance_backup_retention_error": raw_payload.get("maintenance_backup_retention_error"),
        **({"storage_observation": deepcopy(raw_payload["storage_observation"])}
           if "storage_observation" in raw_payload else {}),
        **copy_task_artifact_fields(raw_payload, payload),
    }


def build_scheduler_last_runs_summary(scheduler_overview: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    overview = dict(scheduler_overview or {})
    return {
        "scheduled_daily_bond_refresh": dict(overview.get("daily_run") or {}),
        "scheduled_intraday_bond_refresh": dict(overview.get("intraday_run") or {}),
        "scheduled_weekly_bond_maintenance": dict(overview.get("weekly_run") or {}),
        "scheduled_snapshot_watchdog_refresh": dict(overview.get("snapshot_watchdog_run") or {}),
    }


def summarize_scheduler_chain_audit_for_status_summary(
    chain_audit: dict[str, Any] | None,
) -> dict[str, Any]:
    audit = dict(chain_audit or {})
    summarized = {
        "aligned": bool(audit.get("aligned")),
        "issue_count": int(audit.get("issue_count") or 0),
        "open_issues": list(audit.get("open_issues") or [])[:10],
        "screening_backlog_due": bool(audit.get("screening_backlog_due")),
        "screening_backlog_reason": audit.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(audit.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": audit.get("startup_catch_up_due_jobs") or [],
    }
    weekly_run = dict(((audit.get("runs") or {}).get("weekly_maintenance") or {}))
    if weekly_run:
        summarized["runs"] = {
            "weekly_maintenance": {
                "status": weekly_run.get("status"),
                "warning_reason": weekly_run.get("warning_reason"),
                "degraded_stage": weekly_run.get("degraded_stage"),
                "degradation_summary": weekly_run.get("degradation_summary") or {},
                "suppressed_historical_false_positive": bool(
                    weekly_run.get("suppressed_historical_false_positive")
                ),
            }
        }
    return summarized


def summarize_automation_closure_digest_for_status_summary(
    automation_closure_digest: dict[str, Any] | None,
) -> dict[str, Any]:
    digest = dict(automation_closure_digest or {})
    return {
        "summary": digest.get("summary") or {},
        "alerts": list(digest.get("alerts") or [])[:10],
        "ordered_tasks": [],
    }


def summarize_schedule_stability_for_status_summary(
    schedule_stability_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = dict(schedule_stability_summary or {})
    tasks = list(summary.get("tasks") or [])
    return {
        "summary": summary.get("summary") or {},
        "tasks": [
            {
                "task_key": task.get("task_key"),
                "task_label": task.get("task_label"),
                "slot_window_size": task.get("slot_window_size"),
                "late_slot_count": task.get("late_slot_count"),
                "scheduler_drift_late_slot_count": task.get("scheduler_drift_late_slot_count"),
                "consecutive_late_slots": task.get("consecutive_late_slots"),
                "consecutive_scheduler_drift_late_slots": task.get("consecutive_scheduler_drift_late_slots"),
                "max_delay_seconds": task.get("max_delay_seconds"),
                "latest_slot_started_late": task.get("latest_slot_started_late"),
                "latest_slot_start_delay_seconds": task.get("latest_slot_start_delay_seconds"),
                "latest_slot_startup_catch_up_origin": task.get("latest_slot_startup_catch_up_origin"),
                "latest_slot_status": task.get("latest_slot_status"),
                "attention_needed": task.get("attention_needed"),
            }
            for task in tasks
        ],
    }


def summarize_maintenance_summary_for_status_summary(
    maintenance_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = dict(maintenance_summary or {})
    smoke = dict(summary.get("regression_smoke_summary") or {})
    return {
        "metrics_source": deepcopy(summary.get("metrics_source") or {}),
        "nav_processed_total": summary.get("nav_processed_total"),
        "nav_attempted_total": summary.get("nav_attempted_total"),
        "nav_recovered_total": summary.get("nav_recovered_total"),
        "nav_failed_total": summary.get("nav_failed_total"),
        "nav_records_inserted": summary.get("nav_records_inserted"),
        "long_term_processed_total": summary.get("long_term_processed_total"),
        "long_term_changed_total": summary.get("long_term_changed_total"),
        "long_term_annual_success_total": summary.get("long_term_annual_success_total"),
        "long_term_rank_success_total": summary.get("long_term_rank_success_total"),
        "regression_smoke_summary": {
            "overall_status": smoke.get("overall_status"),
            "status": smoke.get("status"),
            "passed_group_total": smoke.get("passed_group_total"),
            "failed_group_total": smoke.get("failed_group_total"),
            "issue_classification_status": smoke.get("issue_classification_status"),
            "issue_classification_label": smoke.get("issue_classification_label"),
            "issue_category_counts": smoke.get("issue_category_counts") or {},
            "failed_command_total": smoke.get("failed_command_total"),
            "failed_group_classifications": smoke.get("failed_group_classifications") or [],
            "expected_warning_total": smoke.get("expected_warning_total"),
            "test_expectation_mismatch_total": smoke.get("test_expectation_mismatch_total"),
            "production_degradation_total": smoke.get("production_degradation_total"),
            "environment_issue_total": smoke.get("environment_issue_total"),
            "unknown_failure_total": smoke.get("unknown_failure_total"),
            "groups": smoke.get("groups") or [],
            "generated_at": smoke.get("generated_at"),
            "summary_path": smoke.get("summary_path"),
            "artifact_dir": smoke.get("artifact_dir"),
            "source": smoke.get("source"),
            "replay_precision_status": smoke.get("replay_precision_status"),
            "replay_precision_status_label": smoke.get("replay_precision_status_label"),
            "auxiliary_scope": smoke.get("auxiliary_scope"),
            "auxiliary_scope_label": smoke.get("auxiliary_scope_label"),
            "scope_detail": smoke.get("scope_detail"),
        },
    }


def summarize_long_term_governance_for_status_summary(
    governance_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = dict(governance_summary or {})
    upstream_observation_summary = {
        key: value
        for key, value in dict(summary.get("upstream_observation_summary") or {}).items()
        if "sample" not in str(key).lower()
    }
    stale_scope_not_ready_summary = {
        key: value
        for key, value in dict(summary.get("stale_scope_not_ready_summary") or {}).items()
        if "sample" not in str(key).lower()
    }
    buyable_active_stale_scope_recoverability_summary = {
        key: value
        for key, value in dict(
            summary.get("buyable_active_stale_scope_recoverability_summary") or {}
        ).items()
        if "sample" not in str(key).lower()
    }
    return {
        "headline": summary.get("headline"),
        "explanation": summary.get("explanation"),
        "blocking_reason": summary.get("blocking_reason"),
        "ready_for_scoring": summary.get("ready_for_scoring"),
        "candidate_fund_count": summary.get("candidate_fund_count"),
        "annual_feature_gap_fund_count": summary.get("annual_feature_gap_fund_count"),
        "annual_feature_ready_coverage_ratio": summary.get("annual_feature_ready_coverage_ratio"),
        "annual_feature_ready_fund_count": summary.get("annual_feature_ready_fund_count"),
        "annual_history_missing_total": summary.get("annual_history_missing_total"),
        "annual_feature_pending_maturity_fund_count": summary.get("annual_feature_pending_maturity_fund_count"),
        "annual_feature_actionable_gap_fund_count": summary.get("annual_feature_actionable_gap_fund_count"),
        "annual_feature_metadata_unknown_fund_count": summary.get("annual_feature_metadata_unknown_fund_count"),
        "annual_feature_actionable_missing_history_fund_count": summary.get(
            "annual_feature_actionable_missing_history_fund_count"
        ),
        "annual_feature_actionable_sparse_year_row_gap_fund_count": summary.get(
            "annual_feature_actionable_sparse_year_row_gap_fund_count"
        ),
        "annual_feature_actionable_null_year_gap_fund_count": summary.get(
            "annual_feature_actionable_null_year_gap_fund_count"
        ),
        "annual_feature_upstream_limited_gap_fund_count": summary.get(
            "annual_feature_upstream_limited_gap_fund_count"
        ),
        "annual_feature_upstream_limited_missing_history_fund_count": summary.get(
            "annual_feature_upstream_limited_missing_history_fund_count"
        ),
        "annual_feature_upstream_limited_sparse_year_row_gap_fund_count": summary.get(
            "annual_feature_upstream_limited_sparse_year_row_gap_fund_count"
        ),
        "annual_feature_upstream_limited_null_year_gap_fund_count": summary.get(
            "annual_feature_upstream_limited_null_year_gap_fund_count"
        ),
        "rank_feature_gap_fund_count": summary.get("rank_feature_gap_fund_count"),
        "rank_feature_ready_coverage_ratio": summary.get("rank_feature_ready_coverage_ratio"),
        "rank_feature_ready_fund_count": summary.get("rank_feature_ready_fund_count"),
        "rank_feature_stale_fund_count": summary.get("rank_feature_stale_fund_count"),
        "rank_history_missing_total": summary.get("rank_history_missing_total"),
        "full_feature_gap_fund_count": summary.get("full_feature_gap_fund_count"),
        "full_feature_ready_coverage_ratio": summary.get("full_feature_ready_coverage_ratio"),
        "full_feature_ready_fund_count": summary.get("full_feature_ready_fund_count"),
        "annual_feature_stale_fund_count": summary.get("annual_feature_stale_fund_count"),
        "full_feature_stale_fund_count": summary.get("full_feature_stale_fund_count"),
        "neutralized_coverage_ratio": summary.get("neutralized_coverage_ratio"),
        "neutralized_fund_count": summary.get("neutralized_fund_count"),
        "await_history_maturity_count": summary.get("await_history_maturity_count"),
        "local_retry_pool_count": summary.get("local_retry_pool_count"),
        "local_retry_ready_count": summary.get("local_retry_ready_count"),
        "local_retry_window_pending_count": summary.get("local_retry_window_pending_count"),
        "local_retry_cooldown_pending_count": summary.get("local_retry_cooldown_pending_count"),
        "priority_blocked_total": summary.get("priority_blocked_total"),
        "priority_blocked_total_raw": summary.get("priority_blocked_total_raw"),
        "priority_blocked_total_excluding_stale_scope_not_ready": summary.get(
            "priority_blocked_total_excluding_stale_scope_not_ready"
        ),
        "buyable_active_metadata_blocked_total": summary.get("buyable_active_metadata_blocked_total"),
        "buyable_active_scope_not_ready_total": summary.get("buyable_active_scope_not_ready_total"),
        "high_priority_scope_not_ready_total": summary.get("high_priority_scope_not_ready_total"),
        "scope_not_ready_total": summary.get("scope_not_ready_total"),
        "stale_scope_not_ready_total": summary.get("stale_scope_not_ready_total"),
        "stale_scope_not_ready_summary": stale_scope_not_ready_summary,
        "priority_scope_not_ready_summary": summary.get("priority_scope_not_ready_summary") or {},
        "buyable_active_stale_scope_recoverability_summary": (
            buyable_active_stale_scope_recoverability_summary
        ),
        "upstream_limited_empty_payload_total": summary.get("upstream_limited_empty_payload_total"),
        "upstream_limited_watchlist_count": summary.get("upstream_limited_watchlist_count"),
        "terminal_upstream_limited_watchlist_count": summary.get("terminal_upstream_limited_watchlist_count"),
        "stale_upstream_limited_watchlist_count": summary.get("stale_upstream_limited_watchlist_count"),
        "scope_filtered_upstream_limited_total": summary.get("scope_filtered_upstream_limited_total"),
        "maintenance_status": summary.get("maintenance_status"),
        "maintenance_processed_total": summary.get("maintenance_processed_total"),
        "maintenance_changed_total": summary.get("maintenance_changed_total"),
        "maintenance_idle_reason": summary.get("maintenance_idle_reason"),
        "maintenance_no_action_category": summary.get("maintenance_no_action_category"),
        "governance_pool_summary": summary.get("governance_pool_summary") or [],
        "coverage_snapshot": summary.get("coverage_snapshot") or {},
        "upstream_observation_summary": upstream_observation_summary,
    }


def build_scheduler_overview_api_summary(scheduler_overview: dict[str, Any] | None) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    automation_closure_digest = dict(overview.get("automation_closure_digest") or {})
    automation_closure_digest.pop("task_map", None)
    return {
        "busy_skip_summary": deepcopy(overview.get("busy_skip_summary") or {}),
        "enabled": bool(overview.get("enabled")),
        "scheduler_running": bool(overview.get("scheduler_running")),
        "automation_host_mode": overview.get("automation_host_mode"),
        "scheduler_service_runtime": overview.get("scheduler_service_runtime") or {},
        "web_service_runtime": overview.get("web_service_runtime") or {},
        "started_at": overview.get("started_at"),
        "stopped_at": overview.get("stopped_at"),
        "last_heartbeat_at": overview.get("last_heartbeat_at"),
        "last_error": overview.get("last_error"),
        "screening_backlog_due": bool(overview.get("screening_backlog_due")),
        "screening_backlog_reason": overview.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(overview.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": overview.get("startup_catch_up_due_jobs") or [],
        "blocked_snapshot_retry_cooldown": overview.get("blocked_snapshot_retry_cooldown") or {},
        "failed_screening_retry_cooldown": overview.get(
            "failed_screening_retry_cooldown"
        )
        or {},
        "workflow_busy_retry_cooldown": overview.get("workflow_busy_retry_cooldown") or {},
        "scheduler_runtime_evidence": overview.get("scheduler_runtime_evidence") or {},
        "scheduler_state_claimed_running": bool(overview.get("scheduler_state_claimed_running")),
        "scheduler_recovery_deferred": bool(overview.get("scheduler_recovery_deferred")),
        "scheduler_recovery_boundary": overview.get("scheduler_recovery_boundary"),
        "configured_job_ids": overview.get("configured_job_ids") or [],
        "missing_job_ids": overview.get("missing_job_ids") or [],
        "unexpected_job_ids": overview.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(overview.get("job_drift_detected")),
        "effective_nav_snapshot": summarize_scheduler_snapshot_for_api(overview.get("effective_nav_snapshot") or {}),
        "latest_screened_snapshot": summarize_scheduler_snapshot_for_api(overview.get("latest_screened_snapshot") or {}),
        "nav_freshness": overview.get("nav_freshness") or {},
        "current_task": overview.get("current_task") or {},
        "daily_job": overview.get("daily_job") or {},
        "intraday_job": overview.get("intraday_job") or {},
        "weekly_job": overview.get("weekly_job") or {},
        "watchdog_job": overview.get("watchdog_job") or {},
        "maintenance_summary": summarize_scheduler_maintenance_summary_for_api(
            overview.get("maintenance_summary") or {}
        ),
        "timing_summary": overview.get("timing_summary") or {},
        "schedule_stability_summary": overview.get("schedule_stability_summary") or {},
        "automation_closure_digest": automation_closure_digest,
        "recent_failure_recovery_summary": overview.get("recent_failure_recovery_summary") or {},
        "scheduler_path_governance": build_scheduler_path_governance_summary(overview),
        "daily_run": summarize_scheduler_run_for_api(overview.get("daily_run") or {}),
        "intraday_run": summarize_scheduler_run_for_api(overview.get("intraday_run") or {}),
        "weekly_run": summarize_scheduler_run_for_api(overview.get("weekly_run") or {}),
        "snapshot_watchdog_run": summarize_scheduler_run_for_api(overview.get("snapshot_watchdog_run") or {}),
    }


def build_scheduler_status_payload(
    scheduler_state: dict[str, Any] | None,
    *,
    publication: dict[str, Any] | None = None,
    include_detail: bool = True,
    build_scheduler_overview_summary: Callable[[dict[str, Any] | None], dict[str, Any]],
    build_scheduler_overview_api_summary_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    build_scheduler_chain_audit: Callable[[dict[str, Any] | None], dict[str, Any]],
    build_automation_autonomy_summary: Callable[[dict[str, Any] | None, dict[str, Any] | None], dict[str, Any]],
    build_scheduler_last_runs_summary_fn: Callable[[dict[str, Any] | None], dict[str, dict[str, Any]]] = build_scheduler_last_runs_summary,
    restore_weekly_maintenance_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = dict(scheduler_state or {})
    busy_skip_summary = summarize_busy_skips(state.get("recent_busy_skips"))
    if not include_detail:
        scheduler_path_governance = build_scheduler_path_governance_summary(state)
        configured_host_context = str(scheduler_path_governance.get("configured_host_context") or "").strip()
        if configured_host_context:
            scheduler_path_governance["deployment_mode"] = (
                "external_process"
                if configured_host_context == "external_scheduler_process"
                else "embedded_web"
            )
            scheduler_path_governance["official_runtime_host"] = configured_host_context
        daily_run = summarize_scheduler_run_for_api(
            ((state.get("last_runs") or {}).get("scheduled_daily_bond_refresh") or {})
        )
        intraday_run = summarize_scheduler_run_for_api(
            ((state.get("last_runs") or {}).get("scheduled_intraday_bond_refresh") or {})
        )
        weekly_source = (state.get("last_runs") or {}).get("scheduled_weekly_bond_maintenance") or {}
        if restore_weekly_maintenance_fn is not None:
            weekly_source = restore_weekly_maintenance_fn(weekly_source)
        weekly_run = summarize_scheduler_run_for_api(weekly_source)
        snapshot_watchdog_run = summarize_scheduler_run_for_api(
            ((state.get("last_runs") or {}).get("scheduled_snapshot_watchdog_refresh") or {})
        )
        lightweight_chain_audit = {
            "aligned": not bool(state.get("screening_backlog_due") or state.get("host_context_mismatch")),
            "issue_count": int(bool(state.get("screening_backlog_due"))) + int(bool(state.get("host_context_mismatch"))),
            "open_issues": [],
            "screening_backlog_due": bool(state.get("screening_backlog_due")),
            "screening_backlog_reason": state.get("screening_backlog_reason"),
            "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
            "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
        }
        if bool(state.get("host_context_mismatch")):
            lightweight_chain_audit["open_issues"].append(
                {
                    "code": "scheduler_host_context_mismatch",
                    "label": "调度宿主口径不一致",
                    "severity": "warning",
                }
            )
        if bool(state.get("screening_backlog_due")):
            lightweight_chain_audit["open_issues"].append(
                {
                    "code": "screening_backlog_due",
                    "label": "存在待补筛快照",
                    "severity": "warning",
                }
            )
        lightweight_overview = {
            "busy_skip_summary": busy_skip_summary,
            "jobs": state.get("jobs") or [],
            "automation_host_mode": state.get("automation_host_mode"),
            "host_context": state.get("host_context"),
            "configured_host_context": state.get("configured_host_context"),
            "host_context_mismatch": bool(state.get("host_context_mismatch")),
            "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
            "web_service_runtime": state.get("web_service_runtime") or {},
            "screening_backlog_due": bool(state.get("screening_backlog_due")),
            "screening_backlog_reason": state.get("screening_backlog_reason"),
            "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
            "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
            "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
            "latest_screened_snapshot": state.get("latest_screened_snapshot") or {},
            "current_task": state.get("current_task") or {},
            "pending_manual_run_requests": state.get("pending_manual_run_requests") or [],
            "manual_run_history": state.get("manual_run_history") or [],
            "daily_run": daily_run,
            "intraday_run": intraday_run,
            "weekly_run": weekly_run,
            "snapshot_watchdog_run": snapshot_watchdog_run,
        }
        if publication is not None:
            lightweight_overview["publication_layers"] = deepcopy(publication.get("automation_layers") or {})
            lightweight_overview["publication_context"] = {
                "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
                "current_published_run_id": publication.get("current_published_run_id"),
                "latest_successful_run_id": publication.get("latest_successful_run_id"),
            }
        lightweight_chain_audit["open_issues"].extend(build_task_audit_failure_issues(lightweight_overview))
        lightweight_chain_audit["issue_count"] = len(lightweight_chain_audit["open_issues"])
        lightweight_chain_audit["aligned"] = not lightweight_chain_audit["open_issues"]
        automation_autonomy = build_automation_autonomy_summary(
            lightweight_overview,
            lightweight_chain_audit,
        )
        return apply_current_recovery_overlay_to_scheduler_payload({
            "busy_skip_summary": busy_skip_summary,
            "state_schema_version": state.get("state_schema_version"),
            "enabled": bool(state.get("enabled")),
            "scheduler_running": bool(state.get("scheduler_running")),
            "automation_host_mode": state.get("automation_host_mode"),
            "host_context": state.get("host_context"),
            "configured_host_context": state.get("configured_host_context"),
            "host_context_mismatch": bool(state.get("host_context_mismatch")),
            "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
            "web_service_runtime": state.get("web_service_runtime") or {},
            "pid": state.get("pid"),
            "started_at": state.get("started_at"),
            "stopped_at": state.get("stopped_at"),
            "last_heartbeat_at": state.get("last_heartbeat_at"),
            "last_error": state.get("last_error"),
            "screening_backlog_due": bool(state.get("screening_backlog_due")),
            "screening_backlog_reason": state.get("screening_backlog_reason"),
            "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
            "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
            "configured_job_ids": state.get("configured_job_ids") or [],
            "missing_job_ids": state.get("missing_job_ids") or [],
            "unexpected_job_ids": state.get("unexpected_job_ids") or [],
            "job_drift_detected": bool(state.get("job_drift_detected")),
            "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
            "latest_screened_snapshot": state.get("latest_screened_snapshot") or {},
            "nav_freshness": state.get("nav_freshness") or {},
            "current_task": state.get("current_task") or {},
            "scheduler_chain_audit": lightweight_chain_audit,
            "automation_autonomy": automation_autonomy,
            "scheduler_path_governance": scheduler_path_governance,
            "task_run_artifacts": {},
            "maintenance_summary": {},
            "schedule_stability_summary": {"summary": {}, "tasks": []},
            "automation_closure_digest": {"summary": {}, "alerts": [], "ordered_tasks": []},
            "daily_run": daily_run,
            "intraday_run": intraday_run,
            "weekly_run": weekly_run,
            "snapshot_watchdog_run": snapshot_watchdog_run,
            "manual_run_history_count": len(state.get("manual_run_history") or []),
            "pending_manual_run_request_count": len(state.get("pending_manual_run_requests") or []),
        })
    full_overview = build_scheduler_overview_summary(state)
    full_overview["busy_skip_summary"] = busy_skip_summary
    if publication is not None:
        full_overview["publication_layers"] = deepcopy(publication.get("automation_layers") or {})
        full_overview["publication_context"] = {
            "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
            "current_published_run_id": publication.get("current_published_run_id"),
            "latest_successful_run_id": publication.get("latest_successful_run_id"),
        }
    overview = build_scheduler_overview_api_summary_fn(full_overview)
    chain_audit = build_scheduler_chain_audit(full_overview)
    automation_autonomy = build_automation_autonomy_summary(
        full_overview,
        chain_audit,
    )
    scheduler_path_governance = dict(
        overview.get("scheduler_path_governance")
        or build_scheduler_path_governance_summary(full_overview)
    )
    if not scheduler_path_governance.get("configured_host_context"):
        scheduler_path_governance["configured_host_context"] = state.get("configured_host_context")
    if not scheduler_path_governance.get("current_host_context"):
        scheduler_path_governance["current_host_context"] = state.get("host_context")
    if not scheduler_path_governance.get("automation_host_mode"):
        scheduler_path_governance["automation_host_mode"] = state.get("automation_host_mode")
    configured_host_context = str(scheduler_path_governance.get("configured_host_context") or "").strip()
    if configured_host_context:
        scheduler_path_governance["deployment_mode"] = (
            "external_process"
            if configured_host_context == "external_scheduler_process"
            else "embedded_web"
        )
        scheduler_path_governance["official_runtime_host"] = configured_host_context
    scheduler_service_runtime = (
        overview.get("scheduler_service_runtime")
        or state.get("scheduler_service_runtime")
        or {}
    )
    web_service_runtime = (
        overview.get("web_service_runtime")
        or state.get("web_service_runtime")
        or {}
    )
    payload = {
        "busy_skip_summary": busy_skip_summary,
        "state_schema_version": state.get("state_schema_version"),
        "enabled": bool(state.get("enabled")),
        "scheduler_running": bool(state.get("scheduler_running")),
        "automation_host_mode": state.get("automation_host_mode"),
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": scheduler_service_runtime,
        "web_service_runtime": web_service_runtime,
        "pid": state.get("pid"),
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "screening_backlog_due": bool(state.get("screening_backlog_due")),
        "screening_backlog_reason": state.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
        "latest_screened_snapshot": state.get("latest_screened_snapshot") or {},
        "nav_freshness": state.get("nav_freshness") or {},
        "current_task": state.get("current_task") or {},
        "scheduler_chain_audit": (
            chain_audit
            if include_detail
            else summarize_scheduler_chain_audit_for_status_summary(chain_audit)
        ),
        "automation_autonomy": automation_autonomy,
        "scheduler_path_governance": scheduler_path_governance,
        "task_run_artifacts": build_task_run_artifacts_summary(state),
        "maintenance_summary": (
            overview.get("maintenance_summary") or {}
            if include_detail
            else summarize_maintenance_summary_for_status_summary(overview.get("maintenance_summary") or {})
        ),
        "schedule_stability_summary": (
            overview.get("schedule_stability_summary") or {}
            if include_detail
            else summarize_schedule_stability_for_status_summary(
                overview.get("schedule_stability_summary") or {}
            )
        ),
        "automation_closure_digest": (
            overview.get("automation_closure_digest") or {}
            if include_detail
            else summarize_automation_closure_digest_for_status_summary(
                overview.get("automation_closure_digest") or {}
            )
        ),
        "recent_failure_recovery_summary": overview.get("recent_failure_recovery_summary") or {},
        "daily_run": overview.get("daily_run") or {},
        "intraday_run": overview.get("intraday_run") or {},
        "weekly_run": overview.get("weekly_run") or {},
        "snapshot_watchdog_run": overview.get("snapshot_watchdog_run") or {},
    }
    if include_detail:
        payload["recent_busy_skips"] = deepcopy((state.get("recent_busy_skips") or [])[:20])
        payload["manual_run_history"] = (state.get("manual_run_history") or [])[:10]
        payload["pending_manual_run_requests"] = (state.get("pending_manual_run_requests") or [])[:10]
        latest_runs = build_scheduler_last_runs_summary_fn(overview)
        payload["last_runs"] = deepcopy(latest_runs)
        payload["latest_runs"] = latest_runs
        payload["jobs"] = state.get("jobs") or []
        payload["task_closures"] = summarize_task_closures_for_state(state.get("task_closures") or {})
        payload["child_task_idle_streaks"] = state.get("child_task_idle_streaks") or {}
    else:
        payload["manual_run_history_count"] = len(state.get("manual_run_history") or [])
        payload["pending_manual_run_request_count"] = len(state.get("pending_manual_run_requests") or [])
        payload["_long_term_maintenance_summary_full"] = overview.get("maintenance_summary") or {}
        payload["_long_term_task_closures_full"] = state.get("task_closures") or {}
    if publication is not None:
        publication_layers = publication.get("automation_layers") or {}
        payload["publication_summary"] = {
            "current_published_run_id": publication.get("current_published_run_id"),
            "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
            "latest_successful_run_id": ((publication_layers.get("latest_successful_run") or {}).get("run_id")),
            "latest_successful_is_currently_published": bool(
                publication_layers.get("latest_successful_is_currently_published")
            ),
            "latest_successful_blockers": list(publication_layers.get("latest_successful_blockers") or []),
            "status_summary": build_publication_status_summary(publication),
        }
    return apply_current_recovery_overlay_to_scheduler_payload(payload)
