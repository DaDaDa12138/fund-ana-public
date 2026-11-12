#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Database hot/cold governance health rollup.

This module is intentionally read-only. It summarizes the maintenance evidence
already produced by weekly jobs and turns scattered database-governance signals
into one auditable status.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from utils.fund_code_alias_audit import load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/database_governance_health"
DATABASE_GOVERNANCE_HEALTH_AUDIT_JSON_PATTERN = "database_governance_health_audit_*.json"


def load_latest_database_governance_health_audit_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        DATABASE_GOVERNANCE_HEALTH_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_success_status(status: Any) -> bool:
    return _normalize_text(status) in {"success", "dry-run", "applied", "skipped"}


def _is_skipped(summary: dict) -> bool:
    return _normalize_text(summary.get("status")) == "skipped"


def _is_disabled(summary: dict) -> bool:
    return _is_skipped(summary) and _normalize_text(summary.get("skip_reason")) == "disabled_by_config"


def _append_item(
    items: list[dict],
    *,
    key: str,
    status: str,
    label: str,
    detail: str,
    severity: str = "info",
    action_hint: str = "",
    evidence: Optional[dict] = None,
) -> None:
    items.append(
        {
            "key": key,
            "status": status,
            "severity": severity,
            "label": label,
            "detail": detail,
            "action_hint": action_hint,
            "evidence": evidence or {},
        }
    )


def _component_status(items: list[dict]) -> str:
    if any(item.get("status") == "blocked" for item in items):
        return "blocked"
    if any(item.get("status") == "review" for item in items):
        return "review"
    return "ok"


def _build_nav_component(
    *,
    nav_history_archive_preview: dict | None,
    nav_history_archive_vacuum: dict | None,
    nav_history_archive_preview_error: str | None,
    nav_history_archive_vacuum_error: str | None,
) -> dict:
    items: list[dict] = []
    preview = _normalize_dict(nav_history_archive_preview)
    preview_summary = _normalize_dict(preview.get("summary"))
    vacuum = _normalize_dict(nav_history_archive_vacuum)
    vacuum_summary = _normalize_dict(vacuum.get("summary"))

    if nav_history_archive_preview_error:
        _append_item(
            items,
            key="nav_archive_preview_error",
            status="blocked",
            label="NAV archive preview failed",
            detail=nav_history_archive_preview_error,
            severity="error",
            action_hint="Fix NAV archive preview before treating hot/cold governance as healthy.",
        )
    elif _is_disabled(preview):
        _append_item(
            items,
            key="nav_archive_preview_disabled",
            status="review",
            label="NAV archive preview disabled",
            detail="Weekly NAV archive preview is disabled by config.",
            severity="warning",
            action_hint="Enable the preview when automatic hot/cold governance is expected.",
        )
    elif preview and _normalize_text(preview.get("status")) and not _is_success_status(preview.get("status")):
        _append_item(
            items,
            key="nav_archive_preview_not_success",
            status="blocked",
            label="NAV archive preview not successful",
            detail=f"status={preview.get('status')}",
            severity="error",
            evidence={"artifact_path": preview.get("artifact_path") or ""},
        )
    else:
        blocker_count = _normalize_int(
            preview_summary.get("apply_runner_blocker_count")
            or preview_summary.get("blocker_count")
            or 0
        )
        active = bool(preview_summary.get("archive_binding_active")) or bool(
            preview_summary.get("active_binding_available")
        )
        runner_status = _normalize_text(
            preview_summary.get("rolling_archive_runner_status")
            or preview_summary.get("apply_runner_status")
            or preview_summary.get("readiness_status")
            or preview_summary.get("runner_status")
            or preview.get("status")
        )
        archive_cutoff_date = (
            preview_summary.get("archive_cutoff_date")
            or preview_summary.get("active_cutoff_date")
            or preview_summary.get("rolling_archive_cutoff_date")
            or ""
        )
        if blocker_count > 0:
            _append_item(
                items,
                key="nav_archive_blockers",
                status="blocked",
                label="NAV archive blockers",
                detail=f"blocker_count={blocker_count}; runner_status={runner_status}",
                severity="error",
                evidence={"artifact_path": preview.get("artifact_path") or ""},
            )
        elif active:
            _append_item(
                items,
                key="nav_archive_binding_active",
                status="ok",
                label="NAV hot/cold binding active",
                detail=f"runner_status={runner_status}; cutoff={archive_cutoff_date}",
                evidence={
                    "artifact_path": preview.get("artifact_path") or "",
                    "rolling_archive_candidate_row_count": _normalize_int(
                        preview_summary.get("rolling_archive_candidate_row_count")
                    ),
                    "rolling_hot_delete_deleted_row_count": _normalize_int(
                        preview_summary.get("rolling_hot_delete_deleted_row_count")
                    ),
                },
            )
        elif preview:
            _append_item(
                items,
                key="nav_archive_binding_not_active",
                status="review",
                label="NAV hot/cold binding not active",
                detail=f"runner_status={runner_status or 'unknown'}",
                severity="warning",
                action_hint="Keep NAV archive preview/runbook evidence under review until active binding is established.",
                evidence={"artifact_path": preview.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="nav_archive_preview_missing",
                status="review",
                label="NAV archive evidence missing",
                detail="No NAV archive preview summary was provided.",
                severity="warning",
            )

    if nav_history_archive_vacuum_error:
        _append_item(
            items,
            key="nav_vacuum_error",
            status="blocked",
            label="NAV vacuum failed",
            detail=nav_history_archive_vacuum_error,
            severity="error",
        )
    elif _is_disabled(vacuum):
        _append_item(
            items,
            key="nav_vacuum_disabled",
            status="review",
            label="NAV vacuum disabled",
            detail="Weekly NAV vacuum governance is disabled by config.",
            severity="warning",
        )
    elif vacuum:
        blocker_count = _normalize_int(vacuum_summary.get("blocker_count") or 0)
        runner_status = _normalize_text(vacuum_summary.get("runner_status"))
        if blocker_count > 0:
            _append_item(
                items,
                key="nav_vacuum_blocked",
                status="blocked",
                label="NAV vacuum blockers",
                detail=f"blocker_count={blocker_count}; runner_status={runner_status}",
                severity="error",
                evidence={"artifact_path": vacuum.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="nav_vacuum_governed",
                status="ok",
                label="NAV vacuum governed",
                detail=(
                    f"runner_status={runner_status}; "
                    f"effective_mode={vacuum_summary.get('effective_mode') or ''}; "
                    f"estimated_reclaim_mb={vacuum_summary.get('estimated_reclaim_mb') or 0}"
                ),
                evidence={"artifact_path": vacuum.get("artifact_path") or ""},
            )
    else:
        _append_item(
            items,
            key="nav_vacuum_missing",
            status="review",
            label="NAV vacuum evidence missing",
            detail="No NAV vacuum summary was provided.",
            severity="warning",
        )

    return {
        "component": "nav_history_hot_cold",
        "status": _component_status(items),
        "items": items,
    }


def _build_rank_component(
    *,
    rank_history_archive_rolling: dict | None,
    rank_history_archive_apply: dict | None,
    rank_history_archive_vacuum: dict | None,
    rank_history_archive_rolling_error: str | None,
    rank_history_archive_apply_error: str | None,
    rank_history_archive_vacuum_error: str | None,
) -> dict:
    items: list[dict] = []
    rolling = _normalize_dict(rank_history_archive_rolling)
    rolling_summary = _normalize_dict(rolling.get("summary"))
    apply = _normalize_dict(rank_history_archive_apply)
    apply_summary = _normalize_dict(apply.get("summary"))
    vacuum = _normalize_dict(rank_history_archive_vacuum)
    vacuum_summary = _normalize_dict(vacuum.get("summary"))

    if rank_history_archive_rolling_error:
        _append_item(
            items,
            key="rank_rolling_error",
            status="blocked",
            label="rank rolling archive failed",
            detail=rank_history_archive_rolling_error,
            severity="error",
        )
    elif _is_disabled(rolling):
        _append_item(
            items,
            key="rank_rolling_disabled",
            status="review",
            label="rank rolling archive disabled",
            detail="Weekly rank rolling governance is disabled by config.",
            severity="warning",
        )
    elif rolling:
        blocker_count = _normalize_int(rolling_summary.get("blocker_count") or 0)
        active = bool(rolling_summary.get("active_binding_available"))
        runner_status = _normalize_text(rolling_summary.get("runner_status"))
        if blocker_count > 0 or runner_status == "rolling_archive_blocked":
            _append_item(
                items,
                key="rank_rolling_blocked",
                status="blocked",
                label="rank rolling archive blocked",
                detail=f"blocker_count={blocker_count}; runner_status={runner_status}",
                severity="error",
                evidence={"artifact_path": rolling.get("artifact_path") or ""},
            )
        elif active:
            _append_item(
                items,
                key="rank_rolling_active",
                status="ok",
                label="rank hot/cold binding active",
                detail=(
                    f"runner_status={runner_status}; "
                    f"candidate_rows={rolling_summary.get('rolling_archive_candidate_row_count') or 0}; "
                    f"deleted_rows={rolling_summary.get('rolling_hot_delete_deleted_row_count') or 0}"
                ),
                evidence={"artifact_path": rolling.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="rank_rolling_no_active_binding",
                status="review",
                label="rank active binding not detected by rolling probe",
                detail=f"runner_status={runner_status or 'unknown'}",
                severity="warning",
                action_hint="Governance/preflight/apply evidence should establish the first active binding.",
                evidence={"artifact_path": rolling.get("artifact_path") or ""},
            )
    else:
        _append_item(
            items,
            key="rank_rolling_missing",
            status="review",
            label="rank rolling evidence missing",
            detail="No rank rolling summary was provided.",
            severity="warning",
        )

    if rank_history_archive_apply_error:
        _append_item(
            items,
            key="rank_apply_error",
            status="blocked",
            label="rank archive apply failed",
            detail=rank_history_archive_apply_error,
            severity="error",
        )
    elif apply and not _is_disabled(apply):
        blocker_count = _normalize_int(apply_summary.get("blocker_count") or 0)
        active = bool(apply_summary.get("archive_binding_active"))
        runner_status = _normalize_text(apply_summary.get("runner_status"))
        if blocker_count > 0 or runner_status == "blocked":
            _append_item(
                items,
                key="rank_apply_blocked",
                status="blocked",
                label="rank archive apply blocked",
                detail=f"blocker_count={blocker_count}; runner_status={runner_status}",
                severity="error",
                evidence={"artifact_path": apply.get("artifact_path") or ""},
            )
        elif active:
            _append_item(
                items,
                key="rank_apply_binding_active",
                status="ok",
                label="rank archive binding active",
                detail=f"runner_status={runner_status}; cutoff={apply_summary.get('archive_cutoff_date') or ''}",
                evidence={"artifact_path": apply.get("artifact_path") or ""},
            )
        elif _is_skipped(apply):
            _append_item(
                items,
                key="rank_apply_skipped",
                status="ok",
                label="rank archive apply skipped",
                detail=f"skip_reason={apply.get('skip_reason') or ''}",
                evidence={"artifact_path": apply.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="rank_apply_review",
                status="review",
                label="rank archive apply requires review",
                detail=f"runner_status={runner_status or 'unknown'}",
                severity="warning",
                evidence={"artifact_path": apply.get("artifact_path") or ""},
            )

    if rank_history_archive_vacuum_error:
        _append_item(
            items,
            key="rank_vacuum_error",
            status="blocked",
            label="rank vacuum failed",
            detail=rank_history_archive_vacuum_error,
            severity="error",
        )
    elif _is_disabled(vacuum):
        _append_item(
            items,
            key="rank_vacuum_disabled",
            status="review",
            label="rank vacuum disabled",
            detail="Weekly rank vacuum governance is disabled by config.",
            severity="warning",
        )
    elif vacuum:
        blocker_count = _normalize_int(vacuum_summary.get("blocker_count") or 0)
        runner_status = _normalize_text(vacuum_summary.get("runner_status"))
        if blocker_count > 0:
            _append_item(
                items,
                key="rank_vacuum_blocked",
                status="blocked",
                label="rank vacuum blockers",
                detail=f"blocker_count={blocker_count}; runner_status={runner_status}",
                severity="error",
                evidence={"artifact_path": vacuum.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="rank_vacuum_governed",
                status="ok",
                label="rank vacuum governed",
                detail=(
                    f"runner_status={runner_status}; "
                    f"effective_mode={vacuum_summary.get('effective_mode') or ''}; "
                    f"estimated_reclaim_mb={vacuum_summary.get('estimated_reclaim_mb') or 0}"
                ),
                evidence={"artifact_path": vacuum.get("artifact_path") or ""},
            )
    else:
        _append_item(
            items,
            key="rank_vacuum_missing",
            status="review",
            label="rank vacuum evidence missing",
            detail="No rank vacuum summary was provided.",
            severity="warning",
        )

    return {
        "component": "rank_history_hot_cold",
        "status": _component_status(items),
        "items": items,
    }


def _build_screening_component(
    *,
    screening_history_archive_preview: dict | None,
    screening_history_archive_execution: dict | None,
    screening_history_cleanup_execution: dict | None,
    screening_history_archive_preview_error: str | None,
    screening_history_archive_execution_error: str | None,
    screening_history_cleanup_execution_error: str | None,
) -> dict:
    items: list[dict] = []
    archive_preview = _normalize_dict(screening_history_archive_preview)
    archive_preview_summary = _normalize_dict(archive_preview.get("summary"))
    archive_execution = _normalize_dict(screening_history_archive_execution)
    archive_execution_summary = _normalize_dict(archive_execution.get("summary"))
    execution = _normalize_dict(screening_history_cleanup_execution)
    execution_summary = _normalize_dict(execution.get("summary"))
    status = _normalize_text(execution.get("status"))
    mode = _normalize_text(execution.get("mode"))
    publication_guard = _normalize_dict(execution.get("publication_guard"))
    archive_guard = _normalize_dict(execution.get("archive_guard"))
    apply_readiness = _normalize_dict(execution.get("apply_readiness"))
    post_consistency = _normalize_dict(execution.get("post_apply_consistency"))

    if screening_history_archive_preview_error:
        _append_item(
            items,
            key="screening_archive_preview_error",
            status="blocked",
            label="screening history archive preview failed",
            detail=screening_history_archive_preview_error,
            severity="error",
        )
    elif _is_disabled(archive_preview):
        _append_item(
            items,
            key="screening_archive_preview_disabled",
            status="review",
            label="screening history archive preview disabled",
            detail="Weekly screening history cold archive preview is disabled by config.",
            severity="warning",
        )
    elif archive_preview:
        preview_status = _normalize_text(archive_preview.get("status"))
        if preview_status == "failed":
            _append_item(
                items,
                key="screening_archive_preview_failed",
                status="blocked",
                label="screening history archive preview failed",
                detail=archive_preview_summary.get("database_error") or f"status={preview_status}",
                severity="error",
                evidence={"artifact_path": archive_preview.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="screening_archive_preview_governed",
                status="ok",
                label="screening history archive preview governed",
                detail=(
                    f"status={preview_status}; "
                    f"scope_runs={archive_preview_summary.get('scope_run_count') or 0}; "
                    f"scope_rows={archive_preview_summary.get('total_scope_row_count') or 0}; "
                    f"estimated_hot_reclaim_mb={archive_preview_summary.get('estimated_hot_reclaimable_mb') or 0}"
                ),
                evidence={"artifact_path": archive_preview.get("artifact_path") or ""},
            )
    else:
        _append_item(
            items,
            key="screening_archive_preview_missing",
            status="review",
            label="screening archive preview evidence missing",
            detail="No screening history archive preview summary was provided.",
            severity="warning",
        )

    if screening_history_archive_execution_error:
        _append_item(
            items,
            key="screening_archive_execution_error",
            status="blocked",
            label="screening history archive execution failed",
            detail=screening_history_archive_execution_error,
            severity="error",
        )
    elif _is_disabled(archive_execution):
        _append_item(
            items,
            key="screening_archive_execution_disabled",
            status="review",
            label="screening history archive execution disabled",
            detail="Weekly screening history cold archive execution is disabled by config.",
            severity="warning",
        )
    elif archive_execution:
        archive_status = _normalize_text(archive_execution.get("status"))
        archive_mode = _normalize_text(archive_execution.get("mode"))
        if archive_status == "failed":
            _append_item(
                items,
                key="screening_archive_execution_failed",
                status="blocked",
                label="screening history archive execution failed",
                detail=archive_execution_summary.get("error") or f"mode={archive_mode}; status={archive_status}",
                severity="error",
                evidence={"artifact_path": archive_execution.get("artifact_path") or ""},
            )
        else:
            _append_item(
                items,
                key="screening_archive_execution_governed",
                status="ok",
                label="screening history archive execution governed",
                detail=(
                    f"mode={archive_mode}; status={archive_status}; "
                    f"scope_runs={archive_execution_summary.get('scope_run_count') or 0}; "
                    f"inserted_rows={archive_execution_summary.get('inserted_row_count') or 0}; "
                    f"archive_quick_check={archive_execution_summary.get('archive_db_quick_check') or ''}"
                ),
                evidence={"artifact_path": archive_execution.get("artifact_path") or ""},
            )
    else:
        _append_item(
            items,
            key="screening_archive_execution_missing",
            status="review",
            label="screening archive execution evidence missing",
            detail="No screening history archive execution summary was provided.",
            severity="warning",
        )

    if screening_history_cleanup_execution_error:
        _append_item(
            items,
            key="screening_cleanup_error",
            status="blocked",
            label="screening history cleanup failed",
            detail=screening_history_cleanup_execution_error,
            severity="error",
        )
    elif _is_disabled(execution):
        _append_item(
            items,
            key="screening_cleanup_disabled",
            status="review",
            label="screening history cleanup disabled",
            detail="Weekly screening history cleanup execution is disabled by config.",
            severity="warning",
        )
    elif status == "failed":
        _append_item(
            items,
            key="screening_cleanup_failed",
            status="blocked",
            label="screening history cleanup failed",
            detail=f"mode={mode}; selected_run_count={execution_summary.get('selected_run_count') or 0}",
            severity="error",
            evidence={"artifact_path": execution.get("artifact_path") or ""},
        )
    elif execution:
        if mode == "apply":
            selected_count = int(execution_summary.get("selected_run_count") or 0)
            allowed_guard_statuses = (
                {"ok", "disabled", "not_required"}
                if selected_count <= 0
                else {"ok", "disabled"}
            )
            guard_ok = _normalize_text(publication_guard.get("status")) in allowed_guard_statuses
            archive_ok = _normalize_text(archive_guard.get("status")) in allowed_guard_statuses
            consistency_ok = _normalize_text(post_consistency.get("status")) == "ok"
            if status == "applied" and guard_ok and archive_ok and consistency_ok:
                item_status = "ok"
                severity = "info"
                detail = (
                    "apply completed as no-scope idempotent run."
                    if selected_count <= 0
                    else "apply completed with publication guard, archive guard and post-apply consistency checks."
                )
            else:
                item_status = "blocked"
                severity = "error"
                detail = (
                    f"apply status={status}; publication_guard={publication_guard.get('status')}; "
                    f"archive_guard={archive_guard.get('status')}; "
                    f"post_apply_consistency={post_consistency.get('status')}"
                )
            _append_item(
                items,
                key="screening_cleanup_apply_guarded",
                status=item_status,
                label="screening cleanup apply governance",
                detail=detail,
                severity=severity,
                evidence={"artifact_path": execution.get("artifact_path") or ""},
            )
        else:
            selected_count = int(
                execution_summary.get("selected_run_count")
                or archive_guard.get("selected_run_count")
                or 0
            )
            archive_status = _normalize_text(archive_guard.get("status"))
            archive_required = bool(archive_guard.get("required"))
            archive_ok = (
                not archive_required
                or archive_status in {"ok", "disabled", "not_required"}
            )
            readiness_status = _normalize_text(apply_readiness.get("status"))
            readiness_ok = (
                not apply_readiness
                or readiness_status in {"ready", "disabled", "not_required", "no_scope"}
            )
            backup_readiness = _normalize_dict(apply_readiness.get("backup_readiness"))
            backup_status = _normalize_text(
                apply_readiness.get("backup_status")
                or backup_readiness.get("status")
                or "not_recorded"
            )
            backup_creation_mode = _normalize_text(
                apply_readiness.get("backup_creation_mode")
                or backup_readiness.get("creation_mode")
            )
            backup_blocker_count = _normalize_int(
                apply_readiness.get("backup_blocker_count")
                or backup_readiness.get("blocker_count")
                or 0
            )
            item_status = "ok" if archive_ok and readiness_ok else "blocked"
            severity = "info" if archive_ok and readiness_ok else "error"
            _append_item(
                items,
                key="screening_cleanup_dry_run",
                status=item_status,
                label="screening cleanup dry-run governed",
                detail=(
                    f"selected_runs={selected_count}; "
                    f"deleted_total={execution_summary.get('deleted_total') or 0}; "
                    f"publication_guard={publication_guard.get('status') or 'not_required'}; "
                    f"archive_guard={archive_guard.get('status') or 'not_recorded'}; "
                    f"apply_readiness={apply_readiness.get('status') or 'not_recorded'}; "
                    f"backup_status={backup_status}; "
                    f"backup_creation_mode={backup_creation_mode}; "
                    f"backup_blocker_count={backup_blocker_count}"
                ),
                severity=severity,
                evidence={
                    "artifact_path": execution.get("artifact_path") or "",
                    "backup_readiness": backup_readiness,
                },
            )
    else:
        _append_item(
            items,
            key="screening_cleanup_missing",
            status="review",
            label="screening cleanup evidence missing",
            detail="No screening history cleanup execution summary was provided.",
            severity="warning",
        )

    return {
        "component": "screening_history_hot_cold_cleanup",
        "status": _component_status(items),
        "items": items,
    }


def _build_backup_component(
    *,
    database_backup_cleanup_preview: dict | None,
    database_backup_cleanup_execution: dict | None,
    database_backup_compression_preview: dict | None,
    database_backup_compression_execution: dict | None,
    database_backup_cleanup_preview_error: str | None,
    database_backup_cleanup_execution_error: str | None,
    database_backup_compression_preview_error: str | None,
    database_backup_compression_execution_error: str | None,
) -> dict:
    items: list[dict] = []
    preview = _normalize_dict(database_backup_cleanup_preview)
    preview_summary = _normalize_dict(preview.get("summary"))
    execution = _normalize_dict(database_backup_cleanup_execution)
    execution_summary = _normalize_dict(execution.get("summary"))
    compression_preview = _normalize_dict(database_backup_compression_preview)
    compression_preview_summary = _normalize_dict(compression_preview.get("summary"))
    compression_execution = _normalize_dict(database_backup_compression_execution)
    compression_execution_summary = _normalize_dict(compression_execution.get("summary"))

    if database_backup_cleanup_preview_error:
        _append_item(
            items,
            key="backup_cleanup_preview_error",
            status="blocked",
            label="database backup cleanup preview failed",
            detail=database_backup_cleanup_preview_error,
            severity="error",
        )
    elif _is_disabled(preview):
        _append_item(
            items,
            key="backup_cleanup_preview_disabled",
            status="review",
            label="database backup cleanup preview disabled",
            detail="Weekly database backup cleanup preview is disabled by config.",
            severity="warning",
        )
    elif preview:
        _append_item(
            items,
            key="backup_cleanup_preview_ok",
            status="ok",
            label="database backup cleanup preview governed",
            detail=(
                f"auto_candidates={preview_summary.get('auto_candidate_file_count') or 0}; "
                f"manual_review={preview_summary.get('manual_review_candidate_file_count') or 0}; "
                f"protected={preview_summary.get('protected_file_count') or 0}"
            ),
            evidence={"artifact_path": preview.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="backup_cleanup_preview_missing",
            status="review",
            label="database backup cleanup preview missing",
            detail="No database backup cleanup preview summary was provided.",
            severity="warning",
        )

    if database_backup_cleanup_execution_error:
        _append_item(
            items,
            key="backup_cleanup_execution_error",
            status="blocked",
            label="database backup cleanup execution failed",
            detail=database_backup_cleanup_execution_error,
            severity="error",
        )
    elif _is_disabled(execution):
        _append_item(
            items,
            key="backup_cleanup_execution_disabled",
            status="review",
            label="database backup cleanup execution disabled",
            detail="Weekly database backup cleanup execution is disabled by config.",
            severity="warning",
        )
    elif execution:
        status = _normalize_text(execution.get("status"))
        if status == "failed":
            item_status = "blocked"
            severity = "error"
        else:
            item_status = "ok"
            severity = "info"
        _append_item(
            items,
            key="backup_cleanup_execution_governed",
            status=item_status,
            label="database backup cleanup execution governed",
            detail=(
                f"status={status}; skip_reason={execution.get('skip_reason') or ''}; "
                f"deleted_files={execution_summary.get('deleted_file_count') or 0}; "
                f"manual_review={execution_summary.get('manual_review_file_total') or 0}"
            ),
            severity=severity,
            evidence={"artifact_path": execution.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="backup_cleanup_execution_missing",
            status="review",
            label="database backup cleanup execution missing",
            detail="No database backup cleanup execution summary was provided.",
            severity="warning",
        )

    if database_backup_compression_preview_error:
        _append_item(
            items,
            key="backup_compression_preview_error",
            status="blocked",
            label="database backup compression preview failed",
            detail=database_backup_compression_preview_error,
            severity="error",
        )
    elif _is_disabled(compression_preview):
        _append_item(
            items,
            key="backup_compression_preview_disabled",
            status="review",
            label="database backup compression preview disabled",
            detail="Weekly database backup compression preview is disabled by config.",
            severity="warning",
            action_hint="Enable backup compression preview when rollback snapshots should be governed automatically.",
        )
    elif compression_preview:
        _append_item(
            items,
            key="backup_compression_preview_ok",
            status="ok",
            label="database backup compression preview governed",
            detail=(
                f"candidates={compression_preview_summary.get('candidate_file_count') or 0}; "
                f"candidate_size_mb={compression_preview_summary.get('candidate_size_mb') or 0}; "
                f"estimated_reclaim_mb={compression_preview_summary.get('estimated_source_reclaim_size_mb') or 0}"
            ),
            evidence={"artifact_path": compression_preview.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="backup_compression_preview_missing",
            status="review",
            label="database backup compression preview missing",
            detail="No database backup compression preview summary was provided.",
            severity="warning",
        )

    if database_backup_compression_execution_error:
        _append_item(
            items,
            key="backup_compression_execution_error",
            status="blocked",
            label="database backup compression execution failed",
            detail=database_backup_compression_execution_error,
            severity="error",
        )
    elif _is_disabled(compression_execution):
        _append_item(
            items,
            key="backup_compression_execution_disabled",
            status="review",
            label="database backup compression execution disabled",
            detail="Weekly database backup compression execution is disabled by config.",
            severity="warning",
            action_hint="Enable execution when large rollback snapshots should be converted to verified .zst automatically.",
        )
    elif compression_execution:
        status = _normalize_text(compression_execution.get("status"))
        if status == "failed":
            item_status = "blocked"
            severity = "error"
        else:
            item_status = "ok"
            severity = "info"
        _append_item(
            items,
            key="backup_compression_execution_governed",
            status=item_status,
            label="database backup compression execution governed",
            detail=(
                f"status={status}; skip_reason={compression_execution.get('skip_reason') or ''}; "
                f"compressed_files={compression_execution_summary.get('compressed_file_count') or 0}; "
                f"source_removed={compression_execution_summary.get('source_removed_file_count') or 0}; "
                f"freed_mb={compression_execution_summary.get('freed_size_mb') or 0}"
            ),
            severity=severity,
            evidence={"artifact_path": compression_execution.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="backup_compression_execution_missing",
            status="review",
            label="database backup compression execution missing",
            detail="No database backup compression execution summary was provided.",
            severity="warning",
        )

    return {
        "component": "database_backup_cleanup",
        "status": _component_status(items),
        "items": items,
    }


def _build_task_history_component(
    *,
    task_history_governance_preview: dict | None,
    task_history_cleanup_execution: dict | None,
    task_history_governance_preview_error: str | None,
    task_history_cleanup_execution_error: str | None,
) -> dict:
    items: list[dict] = []
    preview = _normalize_dict(task_history_governance_preview)
    preview_summary = _normalize_dict(preview.get("summary"))
    cleanup = _normalize_dict(task_history_cleanup_execution)
    cleanup_summary = _normalize_dict(cleanup.get("summary"))
    cleanup_status = _normalize_text(cleanup.get("status"))
    cleanup_candidate_total = _normalize_int(cleanup_summary.get("candidate_total"))
    cleanup_min_apply_candidate_total = _normalize_int(
        cleanup.get("min_apply_candidate_total"),
        default=100,
    )
    cleanup_effectively_controlled = False

    if task_history_cleanup_execution_error:
        _append_item(
            items,
            key="task_history_cleanup_execution_error",
            status="blocked",
            label="task_history cleanup execution failed",
            detail=task_history_cleanup_execution_error,
            severity="error",
            action_hint="Fix task_history cleanup execution before treating task_history retention as governed.",
        )
    elif _is_disabled(cleanup):
        _append_item(
            items,
            key="task_history_cleanup_execution_disabled",
            status="review",
            label="task_history cleanup execution disabled",
            detail="Weekly task_history cleanup execution is disabled by config.",
            severity="warning",
            action_hint="Enable guarded cleanup execution so retention candidates do not grow without bound.",
        )
    elif cleanup:
        post_apply = _normalize_dict(cleanup.get("post_apply_consistency"))
        mode = _normalize_text(cleanup.get("mode"))
        apply_skipped_reason = _normalize_text(cleanup.get("apply_skipped_reason"))
        if cleanup_status in {"failed", "blocked"}:
            item_status = "blocked"
            severity = "error"
        elif (
            mode == "apply"
            and cleanup_status == "success"
            and _normalize_text(post_apply.get("status")) == "ok"
        ):
            item_status = "ok"
            severity = "info"
            cleanup_effectively_controlled = True
        elif (
            mode == "dry-run"
            and apply_skipped_reason == "below_min_apply_candidate_total"
            and cleanup_candidate_total < cleanup_min_apply_candidate_total
        ):
            item_status = "ok"
            severity = "info"
            cleanup_effectively_controlled = True
        elif mode == "dry-run" and cleanup_candidate_total == 0:
            item_status = "ok"
            severity = "info"
            cleanup_effectively_controlled = True
        else:
            item_status = "review"
            severity = "warning"
        _append_item(
            items,
            key="task_history_cleanup_execution",
            status=item_status,
            label="task_history cleanup execution",
            detail=(
                f"status={cleanup_status or 'unknown'}; "
                f"mode={mode or 'unknown'}; "
                f"candidate_total={cleanup_candidate_total}; "
                f"deleted_count={cleanup_summary.get('deleted_count') or 0}; "
                f"min_apply_candidate_total={cleanup_min_apply_candidate_total}; "
                f"apply_skipped_reason={apply_skipped_reason or ''}; "
                f"post_apply_status={post_apply.get('status') or ''}"
            ),
            severity=severity,
            action_hint=(
                "Review cleanup execution guard errors and retention candidates."
                if item_status != "ok"
                else ""
            ),
            evidence={"artifact_path": cleanup.get("artifact_path") or ""},
        )

    if task_history_governance_preview_error:
        _append_item(
            items,
            key="task_history_governance_preview_error",
            status="blocked",
            label="task_history governance preview failed",
            detail=task_history_governance_preview_error,
            severity="error",
            action_hint="Fix task_history governance preview before treating database automation evidence as complete.",
        )
    elif _is_disabled(preview):
        _append_item(
            items,
            key="task_history_governance_preview_disabled",
            status="review",
            label="task_history governance preview disabled",
            detail="Weekly task_history governance preview is disabled by config.",
            severity="warning",
            action_hint="Enable the read-only preview before applying task_history cleanup or sidecarization.",
        )
    elif preview:
        status = _normalize_text(preview.get("status"))
        retention_candidate_total = _normalize_int(preview_summary.get("retention_candidate_total"))
        large_extra_data_candidate_total = _normalize_int(
            preview_summary.get("large_extra_data_candidate_total")
        )
        snapshot_watchdog_skipped_total = _normalize_int(
            preview_summary.get("snapshot_watchdog_skipped_total")
        )
        preview_has_counted_findings = bool(
            retention_candidate_total
            or large_extra_data_candidate_total
            or snapshot_watchdog_skipped_total
        )
        preview_has_uncontrolled_findings = bool(
            large_extra_data_candidate_total
            or (retention_candidate_total and not cleanup_effectively_controlled)
            or (snapshot_watchdog_skipped_total and not cleanup_effectively_controlled)
        )
        if status == "failed":
            item_status = "blocked"
            severity = "error"
        elif (
            status != "success"
            or preview_has_uncontrolled_findings
            or (
                _normalize_text(preview_summary.get("status")) == "review"
                and not preview_has_counted_findings
            )
        ):
            item_status = "review"
            severity = "warning"
        else:
            item_status = "ok"
            severity = "info"
        _append_item(
            items,
            key="task_history_governance_preview",
            status=item_status,
            label="task_history governance preview",
            detail=(
                f"status={status or 'unknown'}; "
                f"total_rows={preview_summary.get('total_rows') or 0}; "
                f"retention_candidates={retention_candidate_total}; "
                f"large_extra_data_candidates={large_extra_data_candidate_total}; "
                f"snapshot_watchdog_skipped={snapshot_watchdog_skipped_total}; "
                f"preview_action={preview_summary.get('preview_action') or 'unknown'}"
            ),
            severity=severity,
            action_hint=(
                "Review task_history retention and sidecarization candidates before enabling guarded execution."
                if item_status == "review"
                else ""
            ),
            evidence={"artifact_path": preview.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="task_history_governance_preview_missing",
            status="review",
            label="task_history governance preview missing",
            detail="No task_history governance preview summary was provided.",
            severity="warning",
            action_hint="Run the weekly preview to keep scheduler/task_history storage growth auditable.",
        )

    return {
        "component": "task_history_governance",
        "status": _component_status(items),
        "items": items,
    }


def _build_screening_storage_growth_component(
    *,
    screening_storage_growth_audit: dict | None,
    screening_storage_growth_audit_error: str | None,
) -> dict:
    items: list[dict] = []
    audit = _normalize_dict(screening_storage_growth_audit)
    summary = _normalize_dict(audit.get("summary"))

    if screening_storage_growth_audit_error:
        _append_item(
            items,
            key="screening_storage_growth_audit_error",
            status="blocked",
            label="screening storage growth audit failed",
            detail=screening_storage_growth_audit_error,
            severity="error",
            action_hint="Fix the read-only screening storage audit before trusting hot-table growth evidence.",
        )
    elif _is_disabled(audit):
        _append_item(
            items,
            key="screening_storage_growth_audit_disabled",
            status="review",
            label="screening storage growth audit disabled",
            detail="Weekly screening storage growth audit is disabled by config.",
            severity="warning",
            action_hint="Enable this read-only audit to keep screening_decisions/screening_picks growth visible.",
        )
    elif audit:
        status = _normalize_text(audit.get("status"))
        coverage = _normalize_dict(summary.get("materialized_coverage"))
        storage_stats = _normalize_dict(coverage.get("screening_run_storage_stats"))
        latest_activity_gap = _normalize_dict(summary.get("latest_activity_gap_analysis"))
        snapshot_coverage = _normalize_dict(summary.get("summary_snapshot_coverage"))
        snapshot_baseline = _normalize_dict(snapshot_coverage.get("baseline"))
        reason_snapshot = _normalize_dict(snapshot_coverage.get("decision_reason_summary"))
        metric_snapshot = _normalize_dict(snapshot_coverage.get("pick_metric_snapshots"))
        count_source_breakdown = _normalize_dict(summary.get("count_source_breakdown"))
        storage_stats_exists = bool(storage_stats.get("exists"))
        storage_stats_missing = _normalize_int(storage_stats.get("missing_run_count"))
        latest_refresh_candidate_count = _normalize_int(
            latest_activity_gap.get("refresh_candidate_count")
        )
        latest_unresolved_count = _normalize_int(latest_activity_gap.get("unresolved_count"))
        snapshot_baseline_missing = _normalize_int(snapshot_baseline.get("baseline_missing_run_count"))
        reason_snapshot_refresh_candidates = _normalize_int(
            reason_snapshot.get("refresh_candidate_count")
        )
        reason_snapshot_stale_extra = _normalize_int(
            reason_snapshot.get("stale_extra_run_count")
        )
        metric_snapshot_refresh_candidates = _normalize_int(
            metric_snapshot.get("refresh_candidate_count")
        )
        metric_snapshot_stale_extra = _normalize_int(
            metric_snapshot.get("stale_extra_run_count")
        )
        snapshot_tables_missing = bool(snapshot_coverage) and (
            not bool(reason_snapshot.get("exists")) or not bool(metric_snapshot.get("exists"))
        )
        detail_fallback_run_count = 0
        for source_name, bucket in count_source_breakdown.items():
            if str(source_name).startswith("detail_scan"):
                detail_fallback_run_count += _normalize_int(_normalize_dict(bucket).get("run_count"))

        if status == "failed":
            item_status = "blocked"
            severity = "error"
        elif (
            (not storage_stats_exists)
            or storage_stats_missing > 0
            or detail_fallback_run_count > 0
            or latest_refresh_candidate_count > 0
            or latest_unresolved_count > 0
            or snapshot_baseline_missing > 0
            or snapshot_tables_missing
            or reason_snapshot_refresh_candidates > 0
            or reason_snapshot_stale_extra > 0
            or metric_snapshot_refresh_candidates > 0
            or metric_snapshot_stale_extra > 0
        ):
            item_status = "review"
            severity = "warning"
        else:
            item_status = "ok"
            severity = "info"

        _append_item(
            items,
            key="screening_storage_growth_audit",
            status=item_status,
            label="screening storage growth audit",
            detail=(
                f"status={status or 'unknown'}; "
                f"run_count={summary.get('run_count') or 0}; "
                f"storage_stats_missing={storage_stats_missing}; "
                f"latest_activity_refresh_candidates={latest_refresh_candidate_count}; "
                f"latest_activity_unresolved={latest_unresolved_count}; "
                f"snapshot_baseline_missing={snapshot_baseline_missing}; "
                f"reason_snapshot_refresh_candidates={reason_snapshot_refresh_candidates}; "
                f"reason_snapshot_stale_extra={reason_snapshot_stale_extra}; "
                f"metric_snapshot_refresh_candidates={metric_snapshot_refresh_candidates}; "
                f"metric_snapshot_stale_extra={metric_snapshot_stale_extra}; "
                f"detail_fallback_runs={detail_fallback_run_count}; "
                f"table_count_source={_normalize_dict(summary.get('table_totals')).get('count_source') or 'unknown'}"
            ),
            severity=severity,
            action_hint=(
                "Refresh run-level summaries or resolve summary snapshot coverage gaps before expanding API/report reuse."
                if item_status == "review"
                else ""
            ),
            evidence={"artifact_path": audit.get("artifact_path") or ""},
        )
    else:
        _append_item(
            items,
            key="screening_storage_growth_audit_missing",
            status="review",
            label="screening storage growth audit missing",
            detail="No screening storage growth audit summary was provided.",
            severity="warning",
            action_hint="Run the weekly audit to keep screening hot-table growth and summary coverage auditable.",
        )

    return {
        "component": "screening_storage_growth",
        "status": _component_status(items),
        "items": items,
    }


def build_database_governance_health_audit_payload(
    *,
    generated_at: str | None = None,
    nav_history_archive_preview: dict | None = None,
    nav_history_archive_vacuum: dict | None = None,
    rank_history_archive_rolling: dict | None = None,
    rank_history_archive_apply: dict | None = None,
    rank_history_archive_vacuum: dict | None = None,
    screening_history_archive_preview: dict | None = None,
    screening_history_archive_execution: dict | None = None,
    screening_history_cleanup_execution: dict | None = None,
    database_backup_cleanup_preview: dict | None = None,
    database_backup_cleanup_execution: dict | None = None,
    database_backup_compression_preview: dict | None = None,
    database_backup_compression_execution: dict | None = None,
    task_history_governance_preview: dict | None = None,
    task_history_cleanup_execution: dict | None = None,
    screening_storage_growth_audit: dict | None = None,
    nav_history_archive_preview_error: str | None = None,
    nav_history_archive_vacuum_error: str | None = None,
    rank_history_archive_rolling_error: str | None = None,
    rank_history_archive_apply_error: str | None = None,
    rank_history_archive_vacuum_error: str | None = None,
    screening_history_archive_preview_error: str | None = None,
    screening_history_archive_execution_error: str | None = None,
    screening_history_cleanup_execution_error: str | None = None,
    database_backup_cleanup_preview_error: str | None = None,
    database_backup_cleanup_execution_error: str | None = None,
    database_backup_compression_preview_error: str | None = None,
    database_backup_compression_execution_error: str | None = None,
    task_history_governance_preview_error: str | None = None,
    task_history_cleanup_execution_error: str | None = None,
    screening_storage_growth_audit_error: str | None = None,
) -> dict:
    components = [
        _build_nav_component(
            nav_history_archive_preview=nav_history_archive_preview,
            nav_history_archive_vacuum=nav_history_archive_vacuum,
            nav_history_archive_preview_error=nav_history_archive_preview_error,
            nav_history_archive_vacuum_error=nav_history_archive_vacuum_error,
        ),
        _build_rank_component(
            rank_history_archive_rolling=rank_history_archive_rolling,
            rank_history_archive_apply=rank_history_archive_apply,
            rank_history_archive_vacuum=rank_history_archive_vacuum,
            rank_history_archive_rolling_error=rank_history_archive_rolling_error,
            rank_history_archive_apply_error=rank_history_archive_apply_error,
            rank_history_archive_vacuum_error=rank_history_archive_vacuum_error,
        ),
        _build_screening_component(
            screening_history_archive_preview=screening_history_archive_preview,
            screening_history_archive_execution=screening_history_archive_execution,
            screening_history_cleanup_execution=screening_history_cleanup_execution,
            screening_history_archive_preview_error=screening_history_archive_preview_error,
            screening_history_archive_execution_error=screening_history_archive_execution_error,
            screening_history_cleanup_execution_error=screening_history_cleanup_execution_error,
        ),
        _build_backup_component(
            database_backup_cleanup_preview=database_backup_cleanup_preview,
            database_backup_cleanup_execution=database_backup_cleanup_execution,
            database_backup_compression_preview=database_backup_compression_preview,
            database_backup_compression_execution=database_backup_compression_execution,
            database_backup_cleanup_preview_error=database_backup_cleanup_preview_error,
            database_backup_cleanup_execution_error=database_backup_cleanup_execution_error,
            database_backup_compression_preview_error=database_backup_compression_preview_error,
            database_backup_compression_execution_error=database_backup_compression_execution_error,
        ),
    ]
    if (
        task_history_governance_preview is not None
        or task_history_cleanup_execution is not None
        or task_history_governance_preview_error
        or task_history_cleanup_execution_error
    ):
        components.append(
            _build_task_history_component(
                task_history_governance_preview=task_history_governance_preview,
                task_history_cleanup_execution=task_history_cleanup_execution,
                task_history_governance_preview_error=task_history_governance_preview_error,
                task_history_cleanup_execution_error=task_history_cleanup_execution_error,
            )
        )
    if screening_storage_growth_audit is not None or screening_storage_growth_audit_error:
        components.append(
            _build_screening_storage_growth_component(
                screening_storage_growth_audit=screening_storage_growth_audit,
                screening_storage_growth_audit_error=screening_storage_growth_audit_error,
            )
        )
    blocked_items = [
        item
        for component in components
        for item in component.get("items", [])
        if item.get("status") == "blocked"
    ]
    review_items = [
        item
        for component in components
        for item in component.get("items", [])
        if item.get("status") == "review"
    ]
    component_status_counts: dict[str, int] = {}
    for component in components:
        status = _normalize_text(component.get("status")) or "unknown"
        component_status_counts[status] = component_status_counts.get(status, 0) + 1

    overall_status = "ok"
    if blocked_items:
        overall_status = "blocked"
    elif review_items:
        overall_status = "review"

    findings = []
    if blocked_items:
        findings.append(
            f"数据库治理总账存在 {len(blocked_items)} 个阻断项，周维护应标记 degraded。"
        )
        findings.extend(str(item.get("detail") or item.get("label") or "") for item in blocked_items[:5])
    elif review_items:
        findings.append(
            f"数据库治理总账存在 {len(review_items)} 个观察项；自动化主链路未阻断，但后续应补齐证据。"
        )
        findings.extend(str(item.get("detail") or item.get("label") or "") for item in review_items[:5])
    else:
        findings.append("数据库热库/冷库、vacuum、筛选历史 archive/cleanup 与备份 cleanup/压缩证据均未发现阻断项。")

    return {
        "generated_at": generated_at or datetime.now().isoformat(),
        "summary": {
            "status": overall_status,
            "component_count": len(components),
            "component_status_counts": component_status_counts,
            "blocked_item_count": len(blocked_items),
            "review_item_count": len(review_items),
            "automatic_governance_ready": overall_status != "blocked",
        },
        "components": components,
        "blocked_items": blocked_items,
        "review_items": review_items,
        "findings": findings,
    }


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def render_database_governance_health_audit_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# database governance health audit",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {summary.get('status')}",
        f"- automatic_governance_ready: {summary.get('automatic_governance_ready')}",
        f"- component_count: {summary.get('component_count', 0)}",
        f"- blocked_item_count: {summary.get('blocked_item_count', 0)}",
        f"- review_item_count: {summary.get('review_item_count', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## components"])
    for component in payload.get("components") or []:
        lines.append(f"- {component.get('component')}: {component.get('status')}")
        for item in component.get("items") or []:
            lines.append(
                "  - "
                + json.dumps(
                    {
                        "key": item.get("key"),
                        "status": item.get("status"),
                        "severity": item.get("severity"),
                        "detail": item.get("detail"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
    return "\n".join(lines).strip() + "\n"


def write_database_governance_health_audit_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: str | None = None,
) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    json_path = artifact_dir / f"database_governance_health_audit_{ts}.json"
    md_path = artifact_dir / f"database_governance_health_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_database_governance_health_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path
