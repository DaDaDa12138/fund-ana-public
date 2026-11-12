#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档人工维护窗口 SOP（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_execution_checklist import (
    load_latest_nav_history_archive_execution_checklist_payload,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    load_latest_nav_history_archive_maintenance_readiness_audit_payload,
)
from utils.nav_history_archive_maintenance_window_pack import (
    load_latest_nav_history_archive_maintenance_window_pack_payload,
)
from utils.nav_history_archive_post_maintenance_evidence import (
    load_latest_nav_history_archive_post_maintenance_evidence_payload,
)
from utils.nav_history_archive_runbook import load_latest_nav_history_archive_runbook_payload


NAV_HISTORY_ARCHIVE_MANUAL_WINDOW_SOP_JSON_PATTERN = (
    "nav_history_archive_manual_window_sop_*.json"
)


def load_latest_nav_history_archive_manual_window_sop_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档人工维护窗口 SOP。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_MANUAL_WINDOW_SOP_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_list(value, limit: int = 0) -> list[str]:
    items = [_normalize_text(item) for item in (value or []) if _normalize_text(item)]
    if limit > 0:
        return items[:limit]
    return items


def _dedupe_preserve_order(items) -> list[str]:
    return list(dict.fromkeys(_normalize_list(items)))


def build_nav_history_archive_manual_window_sop_payload(
    *,
    runbook_payload: Optional[dict] = None,
    maintenance_window_pack_payload: Optional[dict] = None,
    execution_checklist_payload: Optional[dict] = None,
    maintenance_readiness_audit_payload: Optional[dict] = None,
    post_maintenance_evidence_payload: Optional[dict] = None,
    source_runbook_artifact: Optional[str] = None,
    source_maintenance_window_pack_artifact: Optional[str] = None,
    source_execution_checklist_artifact: Optional[str] = None,
    source_maintenance_readiness_audit_artifact: Optional[str] = None,
    source_post_maintenance_evidence_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """聚合 NAV 归档维护窗口执行前后的单页 SOP。"""
    if runbook_payload is None:
        runbook_path, runbook_payload = load_latest_nav_history_archive_runbook_payload(
            output_dir=output_dir,
        )
        source_runbook_artifact = str(runbook_path)
    if maintenance_window_pack_payload is None:
        maintenance_window_pack_path, maintenance_window_pack_payload = (
            load_latest_nav_history_archive_maintenance_window_pack_payload(
                output_dir=output_dir,
            )
        )
        source_maintenance_window_pack_artifact = str(maintenance_window_pack_path)
    if execution_checklist_payload is None:
        execution_checklist_path, execution_checklist_payload = (
            load_latest_nav_history_archive_execution_checklist_payload(
                output_dir=output_dir,
            )
        )
        source_execution_checklist_artifact = str(execution_checklist_path)
    if maintenance_readiness_audit_payload is None:
        readiness_audit_path, maintenance_readiness_audit_payload = (
            load_latest_nav_history_archive_maintenance_readiness_audit_payload(
                output_dir=output_dir,
            )
        )
        source_maintenance_readiness_audit_artifact = str(readiness_audit_path)
    if post_maintenance_evidence_payload is None:
        post_maintenance_evidence_path, post_maintenance_evidence_payload = (
            load_latest_nav_history_archive_post_maintenance_evidence_payload(
                output_dir=output_dir,
            )
        )
        source_post_maintenance_evidence_artifact = str(post_maintenance_evidence_path)

    runbook_payload = dict(runbook_payload or {})
    maintenance_window_pack_payload = dict(maintenance_window_pack_payload or {})
    execution_checklist_payload = dict(execution_checklist_payload or {})
    maintenance_readiness_audit_payload = dict(maintenance_readiness_audit_payload or {})
    post_maintenance_evidence_payload = dict(post_maintenance_evidence_payload or {})

    runbook_summary = _normalize_dict(runbook_payload.get("summary"))
    window_summary = _normalize_dict(maintenance_window_pack_payload.get("summary"))
    execution_summary = _normalize_dict(execution_checklist_payload.get("summary"))
    readiness_summary = _normalize_dict(maintenance_readiness_audit_payload.get("summary"))
    evidence_summary = _normalize_dict(post_maintenance_evidence_payload.get("summary"))

    archive_cutoff_date = _normalize_text(
        readiness_summary.get("archive_cutoff_date")
        or execution_summary.get("archive_cutoff_date")
        or window_summary.get("archive_cutoff_date")
        or runbook_summary.get("archive_cutoff_date")
    )
    recommended_retention_years = _normalize_int(
        runbook_summary.get("recommended_retention_years")
        or window_summary.get("recommended_retention_years")
        or execution_summary.get("recommended_retention_years")
    )
    estimated_archive_row_count = _normalize_int(
        readiness_summary.get("estimated_archive_row_count")
        or execution_summary.get("estimated_archive_row_count")
        or window_summary.get("estimated_archive_row_count")
        or runbook_summary.get("estimated_archive_row_count")
    )
    estimated_archive_fund_count = _normalize_int(
        readiness_summary.get("estimated_archive_fund_count")
        or execution_summary.get("estimated_archive_fund_count")
        or window_summary.get("estimated_archive_fund_count")
        or runbook_summary.get("estimated_archive_fund_count")
    )
    estimated_archive_size_mb = _normalize_float(
        readiness_summary.get("estimated_archive_size_mb")
        or execution_summary.get("estimated_archive_size_mb")
        or window_summary.get("estimated_archive_size_mb")
        or runbook_summary.get("estimated_archive_size_mb")
    )
    archive_target_path = _normalize_text(
        execution_summary.get("archive_target_path")
        or window_summary.get("archive_target_path")
        or runbook_summary.get("archive_target_path")
    )
    archive_table_name = _normalize_text(
        execution_summary.get("archive_table_name")
        or window_summary.get("archive_table_name")
        or runbook_summary.get("archive_table_name")
    )
    readiness_status = _normalize_text(readiness_summary.get("readiness_status"))
    checklist_status = _normalize_text(execution_summary.get("checklist_status"))
    window_status = _normalize_text(window_summary.get("window_status"))
    package_status = _normalize_text(
        readiness_summary.get("package_status")
        or execution_summary.get("package_status")
        or window_summary.get("package_status")
    )
    ready_for_manual_window_execution = bool(
        readiness_summary.get("ready_for_manual_window_execution")
    )
    blocker_count = _normalize_int(readiness_summary.get("blocker_count"))
    review_item_count = _normalize_int(readiness_summary.get("review_item_count"))
    page_apply_supported = False
    execution_mode = _normalize_text(
        readiness_summary.get("execution_mode")
        or execution_summary.get("execution_mode")
        or window_summary.get("execution_mode")
        or evidence_summary.get("execution_mode")
        or "manual_window_only"
    ) or "manual_window_only"

    available = any(
        [
            bool(runbook_payload),
            bool(maintenance_window_pack_payload),
            bool(execution_checklist_payload),
            bool(maintenance_readiness_audit_payload),
            bool(post_maintenance_evidence_payload),
        ]
    )

    action_hint = (
        "页面不提供 apply；应在维护窗口内按该 SOP 顺序人工执行归档、删除、回滚核对与补证。"
    )
    message = "当前 NAV 冷数据归档已收敛为单页维护窗口 SOP，可直接作为人工执行对照表。"
    template_status = "manual_window_sop_ready"
    if not available:
        template_status = "unavailable"
        message = "当前缺少 NAV 归档上游产物，暂无法聚合维护窗口 SOP。"
        action_hint = "先补齐 runbook / maintenance window pack / execution checklist / readiness audit。"
    elif readiness_status == "blocked":
        template_status = "review_required"
        message = "当前 NAV 冷数据归档价值明确，但维护窗口仍有阻断项，SOP 仅可用于问题收敛。"
        action_hint = "先消除 readiness audit 阻断项，再按该 SOP 进入真实维护窗口。"
    elif not ready_for_manual_window_execution:
        template_status = "review_required"
        message = "当前 NAV 冷数据归档维护窗口 SOP 已生成，但仍有待补齐项，暂不应直接执行。"
        action_hint = "先补齐责任人、路径、备份和留痕项，再按该 SOP 进入维护窗口。"

    window_entry_conditions = _dedupe_preserve_order(
        list(maintenance_window_pack_payload.get("pre_window_checks") or [])
        + list(execution_checklist_payload.get("final_gate_checks") or [])
        + list(runbook_payload.get("preflight_checklist") or [])
    )[:8]
    execution_steps_short = _dedupe_preserve_order(
        list(execution_checklist_payload.get("execution_order") or [])
        + list(maintenance_window_pack_payload.get("execution_sequence") or [])
    )[:8]
    hard_stop_conditions = _dedupe_preserve_order(
        list(maintenance_window_pack_payload.get("stop_conditions") or [])
        + list(execution_checklist_payload.get("rollback_triggers") or [])
        + list(maintenance_readiness_audit_payload.get("blockers") or [])
    )[:8]
    rollback_entry_conditions = _dedupe_preserve_order(
        list(execution_checklist_payload.get("rollback_triggers") or [])
        + list(runbook_payload.get("rollback_strategy") or [])
        + list(execution_checklist_payload.get("rollback_prerequisites") or [])
    )[:8]
    required_artifacts = [
        {
            "label": "runbook",
            "artifact_path": source_runbook_artifact or "",
            "generated_at": _normalize_text(runbook_payload.get("generated_at")),
        },
        {
            "label": "maintenance_window_pack",
            "artifact_path": source_maintenance_window_pack_artifact or "",
            "generated_at": _normalize_text(maintenance_window_pack_payload.get("generated_at")),
        },
        {
            "label": "execution_checklist",
            "artifact_path": source_execution_checklist_artifact or "",
            "generated_at": _normalize_text(execution_checklist_payload.get("generated_at")),
        },
        {
            "label": "maintenance_readiness_audit",
            "artifact_path": source_maintenance_readiness_audit_artifact or "",
            "generated_at": _normalize_text(maintenance_readiness_audit_payload.get("generated_at")),
        },
        {
            "label": "post_maintenance_evidence",
            "artifact_path": source_post_maintenance_evidence_artifact or "",
            "generated_at": _normalize_text(post_maintenance_evidence_payload.get("generated_at")),
        },
    ]
    required_signoffs = _dedupe_preserve_order(
        list(post_maintenance_evidence_payload.get("signoff_requirements") or [])
        + [
            "maintenance_owner 确认维护窗口进入与关闭。",
            "db_operator 确认归档与删除实际执行结果。",
            "data_reviewer 确认抽样范围、计数与删后剩余冷数据。",
            "rollback_owner 若触发回滚，需确认恢复完成与一致性复核。",
        ]
    )[:8]
    required_sql_groups = [
        {
            "group_key": "sample_validation_queries",
            "label": "删前抽样核验 SQL",
            "statements": list(runbook_payload.get("sample_validation_queries") or [])[:5],
        },
        {
            "group_key": "archive_sql_preview",
            "label": "归档写入 SQL",
            "statements": list(runbook_payload.get("archive_sql_preview") or [])[:6],
        },
        {
            "group_key": "delete_sql_preview",
            "label": "删除 SQL",
            "statements": list(runbook_payload.get("delete_sql_preview") or [])[:4],
        },
        {
            "group_key": "postcheck_sql_preview",
            "label": "删后核对 SQL",
            "statements": list(runbook_payload.get("postcheck_sql_preview") or [])[:6],
        },
    ]
    evidence_backfill_steps = _dedupe_preserve_order(
        list(post_maintenance_evidence_payload.get("evidence_capture_sequence") or [])
        + list(post_maintenance_evidence_payload.get("required_evidence_items") or [])
    )[:8]

    findings = _dedupe_preserve_order(
        list(maintenance_readiness_audit_payload.get("findings") or [])[:2]
        + list(execution_checklist_payload.get("findings") or [])[:2]
        + list(post_maintenance_evidence_payload.get("findings") or [])[:2]
        + [message]
    )

    summary = {
        "template_status": template_status,
        "readiness_status": readiness_status,
        "checklist_status": checklist_status,
        "window_status": window_status,
        "package_status": package_status,
        "ready_for_manual_window_execution": ready_for_manual_window_execution,
        "recommended_retention_years": recommended_retention_years or None,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": estimated_archive_row_count,
        "estimated_archive_fund_count": estimated_archive_fund_count,
        "estimated_archive_size_mb": estimated_archive_size_mb,
        "archive_target_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "blocker_count": blocker_count,
        "review_item_count": review_item_count,
        "page_apply_supported": page_apply_supported,
        "execution_mode": execution_mode,
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": available,
        "message": message,
        "action_hint": action_hint,
        "source_nav_history_archive_runbook_artifact": source_runbook_artifact or "",
        "source_nav_history_archive_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_nav_history_archive_maintenance_window_pack_artifact": (
            source_maintenance_window_pack_artifact or ""
        ),
        "source_nav_history_archive_maintenance_window_pack_generated_at": (
            maintenance_window_pack_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_execution_checklist_artifact": (
            source_execution_checklist_artifact or ""
        ),
        "source_nav_history_archive_execution_checklist_generated_at": (
            execution_checklist_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_maintenance_readiness_audit_artifact": (
            source_maintenance_readiness_audit_artifact or ""
        ),
        "source_nav_history_archive_maintenance_readiness_audit_generated_at": (
            maintenance_readiness_audit_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_post_maintenance_evidence_artifact": (
            source_post_maintenance_evidence_artifact or ""
        ),
        "source_nav_history_archive_post_maintenance_evidence_generated_at": (
            post_maintenance_evidence_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_runbook_summary": runbook_summary,
        "source_nav_history_archive_maintenance_window_pack_summary": window_summary,
        "source_nav_history_archive_execution_checklist_summary": execution_summary,
        "source_nav_history_archive_maintenance_readiness_audit_summary": readiness_summary,
        "source_nav_history_archive_post_maintenance_evidence_summary": evidence_summary,
        "summary": summary,
        "window_entry_conditions": window_entry_conditions,
        "execution_steps_short": execution_steps_short,
        "hard_stop_conditions": hard_stop_conditions,
        "rollback_entry_conditions": rollback_entry_conditions,
        "required_artifacts": required_artifacts,
        "required_signoffs": required_signoffs,
        "required_sql_groups": required_sql_groups,
        "evidence_backfill_steps": evidence_backfill_steps,
        "findings": findings,
    }


def render_nav_history_archive_manual_window_sop_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档人工维护窗口 SOP Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive manual window sop",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        f"- template_status: {summary.get('template_status', '')}",
        f"- readiness_status: {summary.get('readiness_status', '')}",
        f"- checklist_status: {summary.get('checklist_status', '')}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        "",
        "## window_entry_conditions",
    ]
    for item in payload.get("window_entry_conditions") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_steps_short"])
    for item in payload.get("execution_steps_short") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## hard_stop_conditions"])
    for item in payload.get("hard_stop_conditions") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## rollback_entry_conditions"])
    for item in payload.get("rollback_entry_conditions") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## required_artifacts"])
    for item in payload.get("required_artifacts") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## required_signoffs"])
    for item in payload.get("required_signoffs") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## required_sql_groups"])
    for item in payload.get("required_sql_groups") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## evidence_backfill_steps"])
    for item in payload.get("evidence_backfill_steps") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_manual_window_sop_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档人工维护窗口 SOP JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_manual_window_sop_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_manual_window_sop_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_manual_window_sop_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
