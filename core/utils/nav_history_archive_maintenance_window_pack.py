#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档维护窗口包（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_maintenance_defaults import (
    build_nav_history_archive_target_path,
    load_nav_history_archive_maintenance_defaults,
    normalize_nav_history_archive_maintenance_defaults,
)
from utils.nav_history_archive_control_pack import (
    load_latest_nav_history_archive_control_pack_payload,
)
from utils.nav_history_archive_runbook import (
    load_latest_nav_history_archive_runbook_payload,
)


NAV_HISTORY_ARCHIVE_MAINTENANCE_WINDOW_PACK_JSON_PATTERN = (
    "nav_history_archive_maintenance_window_pack_*.json"
)


def load_latest_nav_history_archive_maintenance_window_pack_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档维护窗口包。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_MAINTENANCE_WINDOW_PACK_JSON_PATTERN,
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


def build_nav_history_archive_maintenance_window_pack_payload(
    *,
    control_pack_payload: Optional[dict] = None,
    runbook_payload: Optional[dict] = None,
    source_control_pack_artifact: Optional[str] = None,
    source_runbook_artifact: Optional[str] = None,
    maintenance_defaults: Optional[dict] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于 control pack / runbook 生成 NAV 历史归档维护窗口包。"""
    maintenance_defaults = normalize_nav_history_archive_maintenance_defaults(
        maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    )
    if control_pack_payload is None:
        control_pack_path, control_pack_payload = load_latest_nav_history_archive_control_pack_payload(
            output_dir=output_dir,
        )
        source_control_pack_artifact = str(control_pack_path)
    if runbook_payload is None:
        runbook_path, runbook_payload = load_latest_nav_history_archive_runbook_payload(
            output_dir=output_dir,
        )
        source_runbook_artifact = str(runbook_path)

    control_pack_payload = dict(control_pack_payload or {})
    runbook_payload = dict(runbook_payload or {})
    control_pack_summary = _normalize_dict(control_pack_payload.get("summary"))
    runbook_summary = _normalize_dict(runbook_payload.get("summary"))

    package_status = str(control_pack_summary.get("package_status") or "").strip()
    ready_for_manual_signoff = bool(control_pack_summary.get("ready_for_manual_signoff"))
    archive_cutoff_date = str(control_pack_summary.get("archive_cutoff_date") or "").strip()
    archive_table_name = str(
        control_pack_summary.get("archive_table_name")
        or maintenance_defaults.get("archive_table_name")
        or runbook_summary.get("archive_table_name")
        or ""
    ).strip()
    archive_target_path = str(
        control_pack_summary.get("archive_target_path")
        or runbook_summary.get("archive_target_path")
        or ""
    ).strip() or (
        build_nav_history_archive_target_path(
            archive_cutoff_date,
            maintenance_defaults=maintenance_defaults,
        )
    )
    operator_assignments = dict(maintenance_defaults.get("operator_assignments") or {})

    window_status = "review_required"
    message = "当前 NAV 归档维护窗口包仅供人工排窗与执行留痕使用。"
    action_hint = "页面不提供 apply；应在维护窗口内以该窗口包作为人工执行、核对和留痕模板。"
    if ready_for_manual_signoff and package_status == "ready_for_manual_window":
        window_status = "ready_for_manual_window"
        message = "当前 NAV 归档链路已收敛为维护窗口包，可用于人工排窗、执行和留痕。"
    elif control_pack_payload:
        message = (
            "当前 NAV 归档控制链路已生成，但维护窗口包仍处于复核状态；"
            "进入真实窗口前应先处理未闭合控制项。"
        )
        action_hint = "先对齐 preflight、control pack 和 runbook 来源，再将该窗口包作为单一执行模板。"

    pre_window_checks = [
        "确认维护窗口内已暂停相关净值写入或已接受 live 冷数据统计快照的一致性风险。",
        "确认主库备份、归档目标文件路径和目标表可写，并记录备份文件与归档文件路径。",
        f"确认本次归档 cutoff 固定为 {archive_cutoff_date or '待确认'}，执行中不得临时改动窗口边界。",
        f"确认归档目标为 {archive_target_path or 'backend/data/archives/...'}，归档表为 {archive_table_name or 'fund_nav_history_archive'}。",
        "确认删除动作只在归档计数与抽样复核一致后执行，且删后必须立即补留 postcheck 证据。",
    ]
    delete_gate_checks = [
        "archive_sql_preview 执行后，归档表计数必须与主库冷数据计数一致。",
        "抽样基金的最早/最晚净值日期必须与主库冷数据样本一致。",
        "若归档表缺列、主键冲突或写入中断，不进入 delete_sql_preview。",
        "若执行窗口内发生新写入或统计漂移，停止删除并重新生成 runbook / preflight / control pack。",
    ]
    execution_sequence = [
        "1. 先执行删前核对与 sample_validation_queries，固定本次窗口的冷数据范围。",
        "2. 执行 archive_sql_preview，将 cutoff 前冷数据写入独立归档库。",
        "3. 复核归档表计数、抽样范围和归档文件路径，满足 delete_gate_checks 后再进入删除阶段。",
        "4. 执行 delete_sql_preview 删除主库冷数据，并立即执行 postcheck_sql_preview。",
        "5. 记录维护窗口编号、执行人、复核人、备份路径、归档路径和删后证据，必要时再评估 VACUUM。",
    ]
    post_window_evidence = [
        "维护窗口编号、执行开始/结束时间、执行人与复核人。",
        "主库备份路径、归档文件路径、归档表名和最终归档行数。",
        "删前主库冷数据计数、归档表计数、删后 remaining_cold_rows 结果。",
        "若评估执行 VACUUM，补留 VACUUM 前后库体积与 freelist_count 对比。",
    ]
    stop_conditions = [
        "control pack 未达到 ready_for_manual_window。",
        "归档表计数与主库冷数据计数不一致。",
        "postcheck 返回 remaining_cold_rows 非 0。",
        "无法确认归档文件、备份文件或恢复责任人。",
    ]
    evidence_record_template = {
        "window_id": "",
        "maintenance_owner": str(operator_assignments.get("maintenance_owner") or "").strip(),
        "db_operator": str(operator_assignments.get("db_operator") or "").strip(),
        "data_reviewer": str(operator_assignments.get("data_reviewer") or "").strip(),
        "rollback_owner": str(operator_assignments.get("rollback_owner") or "").strip(),
        "started_at": "",
        "completed_at": "",
        "backup_artifact_path": "",
        "archive_artifact_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "precheck_row_count": 0,
        "archive_row_count": 0,
        "remaining_cold_rows": 0,
        "vacuum_executed": False,
        "evidence_paths": [],
        "notes": "",
    }

    summary = {
        "window_status": window_status,
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "recommended_retention_years": _normalize_int(control_pack_summary.get("recommended_retention_years")) or None,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": _normalize_int(control_pack_summary.get("estimated_archive_row_count")),
        "estimated_archive_fund_count": _normalize_int(control_pack_summary.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": _normalize_float(control_pack_summary.get("estimated_archive_size_mb")),
        "archive_target_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "live_cold_row_count": _normalize_int(control_pack_summary.get("live_cold_row_count")),
        "live_cold_fund_count": _normalize_int(control_pack_summary.get("live_cold_fund_count")),
        "live_min_nav_date": str(control_pack_summary.get("live_min_nav_date") or "").strip(),
        "live_max_nav_date": str(control_pack_summary.get("live_max_nav_date") or "").strip(),
        "page_apply_supported": False,
        "execution_mode": "manual_window_only",
    }

    findings = [
        str(item or "").strip()
        for item in (control_pack_payload.get("findings") or [])
        if str(item or "").strip()
    ]
    if not findings:
        findings = [message]

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(control_pack_payload) and bool(runbook_payload),
        "message": message,
        "action_hint": action_hint,
        "source_nav_history_archive_control_pack_artifact": source_control_pack_artifact or "",
        "source_nav_history_archive_control_pack_generated_at": control_pack_payload.get("generated_at") or "",
        "source_nav_history_archive_runbook_artifact": source_runbook_artifact or "",
        "source_nav_history_archive_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_nav_history_archive_control_pack_summary": control_pack_summary,
        "source_nav_history_archive_runbook_summary": runbook_summary,
        "summary": summary,
        "pre_window_checks": pre_window_checks,
        "delete_gate_checks": delete_gate_checks,
        "execution_sequence": execution_sequence,
        "archive_sql_preview": list(runbook_payload.get("archive_sql_preview") or []),
        "delete_sql_preview": list(runbook_payload.get("delete_sql_preview") or []),
        "postcheck_sql_preview": list(runbook_payload.get("postcheck_sql_preview") or []),
        "sample_validation_queries": list(runbook_payload.get("sample_validation_queries") or []),
        "post_window_evidence": post_window_evidence,
        "stop_conditions": stop_conditions,
        "evidence_record_template": evidence_record_template,
        "findings": findings,
    }


def render_nav_history_archive_maintenance_window_pack_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档维护窗口包 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive maintenance window pack",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        f"- source_control_pack_artifact: {payload.get('source_nav_history_archive_control_pack_artifact', '')}",
        f"- source_runbook_artifact: {payload.get('source_nav_history_archive_runbook_artifact', '')}",
        f"- window_status: {summary.get('window_status', '')}",
        f"- package_status: {summary.get('package_status', '')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff', False)}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        "",
        "## pre_window_checks",
    ]
    for item in payload.get("pre_window_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## delete_gate_checks"])
    for item in payload.get("delete_gate_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_sequence"])
    for item in payload.get("execution_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## sample_validation_queries"])
    for item in payload.get("sample_validation_queries") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## archive_sql_preview"])
    for item in payload.get("archive_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## delete_sql_preview"])
    for item in payload.get("delete_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## postcheck_sql_preview"])
    for item in payload.get("postcheck_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## post_window_evidence"])
    for item in payload.get("post_window_evidence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## stop_conditions"])
    for item in payload.get("stop_conditions") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_maintenance_window_pack_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档维护窗口包 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_maintenance_window_pack_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_maintenance_window_pack_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_maintenance_window_pack_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
