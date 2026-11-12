#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档维护控制包（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_maintenance_defaults import (
    DEFAULT_ARCHIVE_TABLE_NAME,
    build_nav_history_archive_target_path,
    load_nav_history_archive_maintenance_defaults,
    normalize_nav_history_archive_maintenance_defaults,
)
from utils.nav_history_archive_preflight import (
    load_latest_nav_history_archive_preflight_payload,
)
from utils.nav_history_archive_runbook import (
    load_latest_nav_history_archive_runbook_payload,
)


NAV_HISTORY_ARCHIVE_CONTROL_PACK_JSON_PATTERN = "nav_history_archive_control_pack_*.json"


def load_latest_nav_history_archive_control_pack_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档控制包。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_CONTROL_PACK_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


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


def build_nav_history_archive_control_pack_payload(
    *,
    runbook_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    source_runbook_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    maintenance_defaults: Optional[dict] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于最新 runbook / preflight 生成 NAV 归档签字控制包。"""
    maintenance_defaults = normalize_nav_history_archive_maintenance_defaults(
        maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    )
    if runbook_payload is None:
        runbook_path, runbook_payload = load_latest_nav_history_archive_runbook_payload(output_dir=output_dir)
        source_runbook_artifact = str(runbook_path)
    if preflight_payload is None:
        preflight_path, preflight_payload = load_latest_nav_history_archive_preflight_payload(output_dir=output_dir)
        source_preflight_artifact = str(preflight_path)

    runbook_payload = dict(runbook_payload or {})
    preflight_payload = dict(preflight_payload or {})
    runbook_summary = _normalize_dict(runbook_payload.get("summary"))
    preflight_summary = _normalize_dict(preflight_payload.get("summary"))
    preflight_checks = _normalize_dict(preflight_payload.get("checks"))

    ready_for_manual_signoff = bool(
        preflight_checks.get(
            "ready_for_manual_signoff",
            preflight_summary.get("ready_for_manual_signoff"),
        )
    )
    archive_cutoff_date = str(runbook_summary.get("archive_cutoff_date") or "").strip()
    archive_table_name = str(
        runbook_summary.get("archive_table_name")
        or maintenance_defaults.get("archive_table_name")
        or DEFAULT_ARCHIVE_TABLE_NAME
    ).strip() or DEFAULT_ARCHIVE_TABLE_NAME
    archive_target_path = str(runbook_summary.get("archive_target_path") or "").strip() or (
        build_nav_history_archive_target_path(
            archive_cutoff_date,
            maintenance_defaults=maintenance_defaults,
        )
    )
    operator_assignments = dict(maintenance_defaults.get("operator_assignments") or {})

    operator_roles = [
        {
            "role": "maintenance_owner",
            "required": True,
            "assignee": str(operator_assignments.get("maintenance_owner") or "").strip(),
            "responsibility": "确认维护窗口、签字记录、归档目标路径和最终执行顺序。",
        },
        {
            "role": "db_operator",
            "required": True,
            "assignee": str(operator_assignments.get("db_operator") or "").strip(),
            "responsibility": "按 runbook 顺序执行抽样核验、归档 SQL、删除 SQL 与 postcheck SQL。",
        },
        {
            "role": "data_reviewer",
            "required": True,
            "assignee": str(operator_assignments.get("data_reviewer") or "").strip(),
            "responsibility": "复核 cutoff、冷数据范围、归档计数与抽样结果是否一致。",
        },
        {
            "role": "rollback_owner",
            "required": True,
            "assignee": str(operator_assignments.get("rollback_owner") or "").strip(),
            "responsibility": "触发异常时中止窗口并按备份 / 归档回滚预案接管恢复。",
        },
    ]
    signoff_record_template = {
        "archive_cutoff_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
        "backup_ready_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
        "archive_target_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
        "postcheck_plan_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
    }

    package_status = "ready_for_manual_window" if ready_for_manual_signoff else "review_required"
    package_message = (
        "当前 runbook / preflight 已收敛为 NAV 归档维护签字包，可进入人工维护窗口排期。"
        if ready_for_manual_signoff
        else "当前 runbook / preflight 尚未完全收敛，进入维护窗口前仍需先处理漂移或补齐控制项。"
    )

    execution_window_requirements = [
        "仅允许在明确维护窗口内执行，禁止在常规运行时触发归档和删除。",
        "执行前必须确认主库备份、归档目标文件和归档表可读写。",
        "删除动作必须晚于归档计数校验，且必须保留 postcheck 证据。",
        "若任何抽样、归档计数或 postcheck 不一致，应立即停止后续步骤并转入回滚预案。",
        "本控制包只用于人工签字与执行顺序确认，不直接执行任何 SQL。",
    ]
    manual_checklist = list(runbook_payload.get("preflight_checklist") or [])[:5]
    manual_steps = list(runbook_payload.get("execution_sequence") or [])[:5]
    stop_conditions = [
        "preflight 未达到 ready_for_manual_signoff。",
        "抽样核验结果与 runbook 预计归档范围不一致。",
        "归档表计数与主库冷数据计数不一致。",
        "删除后 remaining_cold_rows 非 0 或发现超出 cutoff 的异常删除。",
    ]
    findings = (
        [str(item or "").strip() for item in (preflight_payload.get("findings") or []) if str(item or "").strip()]
        or [package_message]
    )

    summary = {
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "recommended_retention_years": _normalize_int(runbook_summary.get("recommended_retention_years")) or None,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": _normalize_int(runbook_summary.get("estimated_archive_row_count")),
        "estimated_archive_fund_count": _normalize_int(runbook_summary.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": _normalize_float(runbook_summary.get("estimated_archive_size_mb")),
        "archive_target_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "live_cold_row_count": _normalize_int(preflight_summary.get("live_cold_row_count")),
        "live_cold_fund_count": _normalize_int(preflight_summary.get("live_cold_fund_count")),
        "live_min_nav_date": str(preflight_summary.get("live_min_nav_date") or "").strip(),
        "live_max_nav_date": str(preflight_summary.get("live_max_nav_date") or "").strip(),
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(runbook_payload) and bool(preflight_payload),
        "message": package_message,
        "source_nav_history_archive_runbook_artifact": source_runbook_artifact or "",
        "source_nav_history_archive_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_nav_history_archive_preflight_artifact": source_preflight_artifact or "",
        "source_nav_history_archive_preflight_generated_at": preflight_payload.get("generated_at") or "",
        "source_nav_history_archive_runbook_summary": runbook_summary,
        "source_nav_history_archive_preflight_summary": preflight_summary,
        "summary": summary,
        "operator_roles": operator_roles,
        "signoff_record_template": signoff_record_template,
        "execution_window_requirements": execution_window_requirements,
        "manual_checklist": manual_checklist,
        "manual_steps": manual_steps,
        "sample_validation_queries": list(runbook_payload.get("sample_validation_queries") or [])[:3],
        "postcheck_sql_preview": list(runbook_payload.get("postcheck_sql_preview") or [])[:3],
        "stop_conditions": stop_conditions,
        "findings": findings,
    }


def render_nav_history_archive_control_pack_markdown(payload: dict) -> str:
    """渲染 NAV 归档控制包 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive control pack",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- source_runbook_artifact: {payload.get('source_nav_history_archive_runbook_artifact', '')}",
        f"- source_preflight_artifact: {payload.get('source_nav_history_archive_preflight_artifact', '')}",
        f"- package_status: {summary.get('package_status', '')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff', False)}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        "",
        "## execution_window_requirements",
    ]
    for item in payload.get("execution_window_requirements") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## manual_checklist"])
    for item in payload.get("manual_checklist") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## manual_steps"])
    for item in payload.get("manual_steps") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_control_pack_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 归档控制包 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_control_pack_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_control_pack_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_nav_history_archive_control_pack_markdown(payload), encoding="utf-8")
    return json_path, md_path
