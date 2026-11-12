#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""VACUUM INTO runner for rank hot/cold database maintenance."""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_vacuum_runner import (
    DEFAULT_MIN_FREE_SPACE_MULTIPLIER,
    DEFAULT_ROLLBACK_DIR,
    _core_table_counts,
    _database_metrics,
    _default_candidate_path,
    _default_rollback_path,
    _execute_replace,
    _execute_vacuum_into,
    _inspect_candidate,
    _inspect_post_replace,
    _normalize_dict,
    _normalize_float,
    _normalize_int,
    _normalize_text,
    _now_ts,
    _resolve_output_dir,
    _resolve_path,
    _resolve_project_root,
    _round_mb,
    _stringify_project_path,
    _table_counts,
)
from utils.rank_history_archive_apply_runner import (
    DEFAULT_OUTPUT_DIR,
    RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RANK_HISTORY_ARCHIVE_VACUUM_RUNNER_JSON_PATTERN = (
    "rank_history_archive_vacuum_runner_*.json"
)
ALLOWED_MODES = {"dry_run", "vacuum_into", "replace", "post_replace_audit", "auto"}
DEFAULT_CANDIDATE_DIR = "backend/data/maintenance/rank_history_vacuum"
DEFAULT_ROLLBACK_PREFIX = "bond_funds_pre_rank_vacuum_replace"


def load_latest_rank_history_archive_vacuum_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        RANK_HISTORY_ARCHIVE_VACUUM_RUNNER_JSON_PATTERN,
        output_dir=output_dir,
    )


def _load_latest_active_binding_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Optional[Path], dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    try:
        candidates = sorted(resolved_output_dir.glob(RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN))
    except OSError:
        return None, {}
    for candidate in reversed(candidates):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        summary = _normalize_dict(payload.get("summary")) if isinstance(payload, dict) else {}
        if bool(summary.get("archive_binding_active")) and (
            _normalize_text(summary.get("runner_status")) == "archive_binding_active"
        ):
            return candidate, payload
    return None, {}


def _latest_candidate_path(
    *,
    db_path: Path,
    candidate_dir: Path,
) -> Optional[Path]:
    try:
        candidates = sorted(
            candidate_dir.glob(f"{db_path.stem}_vacuum_candidate_*{db_path.suffix}"),
            key=lambda item: (item.stat().st_mtime, item.name),
        )
    except OSError:
        return None
    return candidates[-1] if candidates else None


def build_rank_history_archive_vacuum_runner_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    mode: str = "dry_run",
    candidate_path: str | Path | None = None,
    candidate_dir: str | Path = DEFAULT_CANDIDATE_DIR,
    rollback_path: str | Path | None = None,
    rollback_dir: str | Path = DEFAULT_ROLLBACK_DIR,
    rollback_prefix: str = DEFAULT_ROLLBACK_PREFIX,
    timestamp: Optional[str] = None,
    min_free_space_multiplier: float = DEFAULT_MIN_FREE_SPACE_MULTIPLIER,
    confirm_replace: bool = False,
    confirm_services_stopped: bool = False,
) -> dict:
    root = _resolve_project_root(project_root)
    ts = timestamp or _now_ts()
    requested_mode = _normalize_text(mode) or "dry_run"
    resolved_output_dir = _resolve_output_dir(output_dir, root)
    source_db_path = _resolve_path(db_path or "backend/data/bond_funds.db", root)
    resolved_candidate_dir = _resolve_path(candidate_dir, root) or (
        root / DEFAULT_CANDIDATE_DIR
    ).resolve()
    resolved_rollback_dir = _resolve_path(rollback_dir, root) or (
        root / DEFAULT_ROLLBACK_DIR
    ).resolve()
    if source_db_path is None:
        source_db_path = (root / "backend/data/bond_funds.db").resolve()

    explicit_candidate_path = _resolve_path(candidate_path, root)
    resolved_candidate_path = explicit_candidate_path
    if resolved_candidate_path is None and requested_mode == "auto":
        latest_candidate = _latest_candidate_path(
            db_path=source_db_path,
            candidate_dir=resolved_candidate_dir,
        )
        if latest_candidate is not None:
            latest_inspection = _inspect_candidate(
                source_db_path=source_db_path,
                candidate_path=latest_candidate,
            )
            if bool(
                latest_inspection.get("exists")
                and latest_inspection.get("counts_match")
                and _normalize_text(latest_inspection.get("quick_check")).lower() == "ok"
            ):
                resolved_candidate_path = latest_candidate
    if resolved_candidate_path is None:
        resolved_candidate_path = _default_candidate_path(
            db_path=source_db_path,
            candidate_dir=resolved_candidate_dir,
            timestamp=ts,
        )

    resolved_rollback_path = _resolve_path(rollback_path, root)
    if resolved_rollback_path is None:
        resolved_rollback_path = _default_rollback_path(
            db_path=source_db_path,
            rollback_dir=resolved_rollback_dir,
            timestamp=ts,
            rollback_prefix=rollback_prefix,
        )

    active_binding_path, active_binding_payload = _load_latest_active_binding_payload(
        output_dir=resolved_output_dir,
        project_root=root,
    )
    active_binding_summary = _normalize_dict(active_binding_payload.get("summary"))
    source_metrics = _database_metrics(source_db_path)
    source_table_counts: dict[str, int] = {}
    if source_metrics.get("exists"):
        source_table_counts = _table_counts(source_db_path)

    initial_candidate_inspection = _inspect_candidate(
        source_db_path=source_db_path,
        candidate_path=resolved_candidate_path,
    )
    initial_candidate_valid = bool(
        initial_candidate_inspection.get("exists")
        and initial_candidate_inspection.get("counts_match")
        and _normalize_text(initial_candidate_inspection.get("quick_check")).lower() == "ok"
    )
    free_probe = resolved_candidate_path.parent
    while not free_probe.exists() and free_probe.parent != free_probe:
        free_probe = free_probe.parent
    disk_usage = shutil.disk_usage(free_probe if free_probe.exists() else root)
    source_size_bytes = _normalize_int(source_metrics.get("size_bytes"))
    required_free_bytes = int(source_size_bytes * max(1.0, float(min_free_space_multiplier)))
    free_space_ok = disk_usage.free >= required_free_bytes
    estimated_reclaim_bytes = _normalize_int(source_metrics.get("freelist_bytes"))
    meaningful_reclaim_bytes = int(source_size_bytes * 0.05)
    reclaim_worth_vacuum = bool(
        source_size_bytes > 0 and estimated_reclaim_bytes >= meaningful_reclaim_bytes
    )
    effective_mode = requested_mode
    if requested_mode == "auto":
        if initial_candidate_valid:
            effective_mode = "replace"
        elif reclaim_worth_vacuum:
            effective_mode = "vacuum_into"
        else:
            effective_mode = "dry_run"

    blockers: list[str] = []
    review_items: list[str] = []
    if requested_mode not in ALLOWED_MODES:
        blockers.append(f"unsupported mode: {requested_mode}")
    if effective_mode not in ALLOWED_MODES - {"auto"}:
        blockers.append(f"unsupported effective mode: {effective_mode}")
    if not bool(source_metrics.get("exists")):
        blockers.append(f"active database missing: {source_db_path}")
    if _normalize_text(source_metrics.get("quick_check")).lower() != "ok":
        blockers.append(f"active database quick_check failed: {source_metrics.get('quick_check')}")
    if not active_binding_summary.get("archive_binding_active"):
        blockers.append("active rank cold binding evidence is required before VACUUM replacement.")
    if _normalize_int(active_binding_summary.get("live_cold_row_count")) != 0:
        blockers.append("active rank binding evidence still reports live cold rows in hot DB.")
    if source_db_path.resolve() == resolved_candidate_path.resolve():
        blockers.append("candidate path must not equal active database path.")
    if source_db_path.resolve() == resolved_rollback_path.resolve():
        blockers.append("rollback path must not equal active database path.")
    if effective_mode in {"vacuum_into", "replace"} and not free_space_ok:
        blockers.append(
            "insufficient free disk space for VACUUM candidate: "
            f"free={disk_usage.free}, required={required_free_bytes}"
        )
    if effective_mode == "vacuum_into" and resolved_candidate_path.exists():
        blockers.append(f"candidate already exists: {resolved_candidate_path}")
    if effective_mode == "replace":
        if not confirm_replace:
            blockers.append("replace mode requires explicit confirm_replace=true.")
        if not confirm_services_stopped:
            blockers.append(
                "replace mode requires explicit confirm_services_stopped=true; "
                "the runner cannot prove web/scheduler containers are offline."
            )
        if not resolved_candidate_path.exists():
            blockers.append(f"candidate database missing: {resolved_candidate_path}")
        if resolved_rollback_path.exists():
            blockers.append(f"rollback target already exists: {resolved_rollback_path}")
    if effective_mode == "post_replace_audit" and not resolved_rollback_path.exists():
        blockers.append(f"rollback database missing: {resolved_rollback_path}")
    if requested_mode == "auto" and not initial_candidate_valid and not reclaim_worth_vacuum:
        review_items.append(
            "auto mode skipped VACUUM candidate because estimated freelist reclaim is below 5% of DB size."
        )
    elif 0 < estimated_reclaim_bytes < int(source_size_bytes * 0.05):
        review_items.append(
            "estimated freelist reclaim is below 5% of DB size; VACUUM benefit is limited."
        )

    vacuum_result = {
        "status": "not_requested" if effective_mode != "vacuum_into" else "not_executed",
        "vacuum_executed": False,
    }
    replace_result = {
        "status": "not_requested" if effective_mode != "replace" else "not_executed",
        "replace_executed": False,
    }

    if effective_mode == "vacuum_into" and not blockers:
        vacuum_result = _execute_vacuum_into(
            source_db_path=source_db_path,
            candidate_path=resolved_candidate_path,
        )
        if vacuum_result.get("status") != "executed":
            blockers.append(
                "VACUUM INTO failed: "
                + _normalize_text(vacuum_result.get("error") or "unknown")
            )

    candidate_inspection = _inspect_candidate(
        source_db_path=source_db_path,
        candidate_path=resolved_candidate_path,
    )
    post_replace_inspection: dict[str, Any] = {}
    candidate_valid = bool(
        candidate_inspection.get("exists")
        and candidate_inspection.get("counts_match")
        and _normalize_text(candidate_inspection.get("quick_check")).lower() == "ok"
    )
    if effective_mode == "replace":
        if not candidate_valid:
            blockers.append("candidate database must pass quick_check and full table-count comparison.")
        if not blockers:
            replace_result = _execute_replace(
                source_db_path=source_db_path,
                candidate_path=resolved_candidate_path,
                rollback_path=resolved_rollback_path,
                timestamp=ts,
            )
            if replace_result.get("status") != "replaced":
                blockers.append(
                    "replace failed: "
                    + _normalize_text(replace_result.get("error") or replace_result.get("status"))
                )
            else:
                replace_result["replace_executed"] = True
                candidate_inspection = _inspect_candidate(
                    source_db_path=source_db_path,
                    candidate_path=None,
                )
                source_metrics = _database_metrics(source_db_path)
                source_table_counts = _table_counts(source_db_path)
    if effective_mode == "post_replace_audit" and not blockers:
        post_replace_inspection = _inspect_post_replace(
            active_db_path=source_db_path,
            rollback_path=resolved_rollback_path,
        )
        if not bool(post_replace_inspection.get("business_counts_match")):
            blockers.append("active and rollback database business table counts do not match.")
        if not bool(post_replace_inspection.get("quick_check_ok")):
            blockers.append("active or rollback quick_check is not ok.")
        if _normalize_int(post_replace_inspection.get("runtime_table_mismatch_count")) > 0:
            review_items.append(
                "volatile runtime table counts changed after service restart; business table counts match."
            )
        source_metrics = _database_metrics(source_db_path)
        source_table_counts = _table_counts(source_db_path)

    final_metrics = _database_metrics(source_db_path)
    if blockers:
        runner_status = "blocked"
        auto_governance_stage = "rank_vacuum_replace_blocked"
    elif effective_mode == "replace" and replace_result.get("status") == "replaced":
        runner_status = "vacuum_replace_complete"
        auto_governance_stage = "rank_vacuum_replace_complete"
    elif effective_mode == "post_replace_audit":
        runner_status = "vacuum_replace_postcheck_ok"
        auto_governance_stage = "rank_vacuum_replace_postcheck_ok"
    elif effective_mode == "vacuum_into" and candidate_valid:
        runner_status = "candidate_ready_for_replace"
        auto_governance_stage = "rank_vacuum_candidate_ready"
    elif candidate_valid:
        runner_status = "candidate_ready_for_replace"
        auto_governance_stage = "rank_vacuum_candidate_ready"
    else:
        runner_status = "dry_run_ready_for_vacuum_into"
        auto_governance_stage = "rank_vacuum_plan_ready"

    size_delta_mb = _normalize_float(candidate_inspection.get("size_delta_mb"))
    if effective_mode == "post_replace_audit" and post_replace_inspection:
        size_delta_mb = _normalize_float(post_replace_inspection.get("size_delta_mb"))

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "effective_mode": effective_mode,
        "source_db_path": _stringify_project_path(source_db_path, root),
        "source_db_absolute_path": str(source_db_path),
        "candidate_path": _stringify_project_path(resolved_candidate_path, root),
        "candidate_absolute_path": str(resolved_candidate_path),
        "rollback_path": _stringify_project_path(resolved_rollback_path, root),
        "rollback_absolute_path": str(resolved_rollback_path),
        "source_size_mb": _normalize_float(source_metrics.get("size_mb")),
        "final_size_mb": _normalize_float(final_metrics.get("size_mb")),
        "candidate_size_mb": _normalize_float(
            _normalize_dict(candidate_inspection.get("candidate_metrics")).get("size_mb")
        ),
        "freelist_count": _normalize_int(source_metrics.get("freelist_count")),
        "freelist_mb": _normalize_float(source_metrics.get("freelist_mb")),
        "estimated_reclaim_mb": _round_mb(estimated_reclaim_bytes),
        "candidate_size_delta_mb": size_delta_mb,
        "candidate_size_reduction_pct": _normalize_float(
            candidate_inspection.get("size_reduction_pct")
        ),
        "post_replace_size_delta_mb": _normalize_float(
            post_replace_inspection.get("size_delta_mb")
        ),
        "post_replace_size_reduction_pct": _normalize_float(
            post_replace_inspection.get("size_reduction_pct")
        ),
        "post_replace_counts_match": bool(post_replace_inspection.get("counts_match")),
        "post_replace_business_counts_match": bool(
            post_replace_inspection.get("business_counts_match")
        ),
        "post_replace_runtime_table_mismatch_count": _normalize_int(
            post_replace_inspection.get("runtime_table_mismatch_count")
        ),
        "post_replace_quick_check_ok": bool(post_replace_inspection.get("quick_check_ok")),
        "rollback_quick_check": post_replace_inspection.get("rollback_quick_check") or "",
        "rollback_size_mb": _normalize_float(
            _normalize_dict(post_replace_inspection.get("rollback_metrics")).get("size_mb")
        ),
        "active_db_quick_check": source_metrics.get("quick_check") or "",
        "candidate_quick_check": candidate_inspection.get("quick_check") or "",
        "candidate_exists": bool(candidate_inspection.get("exists")),
        "candidate_valid": candidate_valid,
        "candidate_counts_match": bool(candidate_inspection.get("counts_match")),
        "vacuum_executed": bool(vacuum_result.get("vacuum_executed")),
        "vacuum_result_status": vacuum_result.get("status") or "",
        "replace_executed": bool(replace_result.get("replace_executed")),
        "replace_result_status": replace_result.get("status") or "",
        "rollback_db_path": replace_result.get("rollback_db_path") or "",
        "active_binding_artifact": _stringify_project_path(active_binding_path, root),
        "archive_binding_active": bool(active_binding_summary.get("archive_binding_active")),
        "archive_cutoff_date": active_binding_summary.get("archive_cutoff_date") or "",
        "live_cold_row_count": _normalize_int(active_binding_summary.get("live_cold_row_count")),
        "exact_matched_hot_cold_row_count": _normalize_int(
            active_binding_summary.get("exact_matched_hot_cold_row_count")
        ),
        "disk_free_mb": _round_mb(disk_usage.free),
        "required_free_mb": _round_mb(required_free_bytes),
        "free_space_ok": free_space_ok,
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "confirm_replace": bool(confirm_replace),
        "confirm_services_stopped": bool(confirm_services_stopped),
    }

    findings = []
    if blockers:
        findings.append(f"rank VACUUM runner blocked with {len(blockers)} blockers.")
        findings.extend(blockers[:5])
    elif runner_status == "vacuum_replace_complete":
        findings.append(
            "VACUUM candidate replaced the active database and rollback DB was retained."
        )
    elif runner_status == "vacuum_replace_postcheck_ok":
        findings.append(
            "Post-replace audit verified active and rollback database counts, quick_check, and size delta."
        )
    elif runner_status == "candidate_ready_for_replace":
        findings.append(
            "VACUUM INTO candidate is verified and ready for the next replace step."
        )
    else:
        findings.append("VACUUM INTO dry-run gates are ready; no candidate or replacement was executed.")
    findings.extend(review_items[:5])

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "summary": summary,
        "blockers": blockers,
        "review_items": review_items,
        "source_metrics": source_metrics,
        "final_metrics": final_metrics,
        "source_table_counts": source_table_counts,
        "source_core_table_counts": _core_table_counts(source_table_counts),
        "candidate_inspection": candidate_inspection,
        "post_replace_inspection": post_replace_inspection,
        "active_binding_summary": active_binding_summary,
        "vacuum_result": vacuum_result,
        "replace_result": replace_result,
        "findings": findings,
        "safety": {
            "vacuum_executed": bool(vacuum_result.get("vacuum_executed")),
            "replace_executed": bool(replace_result.get("replace_executed")),
            "active_db_replaced": bool(replace_result.get("replace_executed")),
            "rollback_retained": bool(replace_result.get("rollback_db_path")),
            "services_stopped_confirmed": bool(confirm_services_stopped),
        },
    }


def render_rank_history_archive_vacuum_runner_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# rank history archive vacuum runner",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- runner_status: {summary.get('runner_status', '')}",
        f"- auto_governance_stage: {summary.get('auto_governance_stage', '')}",
        f"- mode: {summary.get('mode', '')}",
        f"- effective_mode: {summary.get('effective_mode', '')}",
        f"- source_size_mb: {summary.get('source_size_mb', 0)}",
        f"- final_size_mb: {summary.get('final_size_mb', 0)}",
        f"- candidate_size_mb: {summary.get('candidate_size_mb', 0)}",
        f"- freelist_mb: {summary.get('freelist_mb', 0)}",
        f"- candidate_valid: {summary.get('candidate_valid', False)}",
        f"- vacuum_executed: {summary.get('vacuum_executed', False)}",
        f"- replace_executed: {summary.get('replace_executed', False)}",
        f"- rollback_db_path: {summary.get('rollback_db_path', '')}",
        f"- blocker_count: {summary.get('blocker_count', 0)}",
        "",
        "## blockers",
    ]
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## review_items"])
    for item in payload.get("review_items") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_rank_history_archive_vacuum_runner_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or _now_ts()
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"rank_history_archive_vacuum_runner_{ts}.json"
    md_path = artifact_dir / f"rank_history_archive_vacuum_runner_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_rank_history_archive_vacuum_runner_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
