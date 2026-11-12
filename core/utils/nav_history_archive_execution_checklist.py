#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档最终执行清单（只读）。
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
from utils.nav_history_archive_control_pack import (
    load_latest_nav_history_archive_control_pack_payload,
)
from utils.nav_history_archive_maintenance_window_pack import (
    load_latest_nav_history_archive_maintenance_window_pack_payload,
)
from utils.nav_history_archive_preflight import (
    load_latest_nav_history_archive_preflight_payload,
)


NAV_HISTORY_ARCHIVE_EXECUTION_CHECKLIST_JSON_PATTERN = (
    "nav_history_archive_execution_checklist_*.json"
)


def load_latest_nav_history_archive_execution_checklist_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档最终执行清单。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_EXECUTION_CHECKLIST_JSON_PATTERN,
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


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_list(value, limit: int = 0) -> list[str]:
    items = [str(item or "").strip() for item in (value or []) if str(item or "").strip()]
    if limit > 0:
        return items[:limit]
    return items


def _resolve_project_root(
    *,
    project_root: Optional[str],
    preflight_payload: Optional[dict],
) -> tuple[Path, bool]:
    if project_root:
        return Path(project_root).resolve(), True

    preflight_summary = _normalize_dict(preflight_payload)
    db_path_text = _normalize_text(preflight_summary.get("db_path"))
    if not db_path_text:
        return PROJECT_ROOT, False

    db_path = Path(db_path_text)
    if not db_path.is_absolute():
        db_path = (PROJECT_ROOT / db_path).resolve()
    else:
        db_path = db_path.resolve()

    if db_path.parent.name == "data" and db_path.parent.parent.name == "backend":
        inferred_root = db_path.parent.parent.parent.resolve()
    elif db_path.parent.name == "database":
        inferred_root = db_path.parent.parent.resolve()
    else:
        inferred_root = db_path.parent.resolve()

    if inferred_root.exists():
        return inferred_root, True

    project_database_dir = (PROJECT_ROOT / "database").resolve()
    if project_database_dir.exists():
        return PROJECT_ROOT, True

    return inferred_root, False


def _iter_files(path: Optional[Path]) -> list[Path]:
    if not path or not path.exists() or not path.is_dir():
        return []
    return [item for item in path.iterdir() if item.is_file()]


def _latest_file(path: Optional[Path]) -> Optional[Path]:
    files = _iter_files(path)
    if not files:
        return None
    return max(files, key=lambda item: item.stat().st_mtime_ns)


def _stringify_project_path(path: Optional[Path], project_root: Path) -> str:
    if path is None:
        return ""
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def build_nav_history_archive_execution_checklist_payload(
    *,
    maintenance_window_pack_payload: Optional[dict] = None,
    control_pack_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    source_maintenance_window_pack_artifact: Optional[str] = None,
    source_control_pack_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    maintenance_defaults: Optional[dict] = None,
    project_root: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于 maintenance window pack / control pack / preflight 生成最终执行清单。"""
    maintenance_defaults = normalize_nav_history_archive_maintenance_defaults(
        maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    )
    if maintenance_window_pack_payload is None:
        maintenance_window_pack_path, maintenance_window_pack_payload = (
            load_latest_nav_history_archive_maintenance_window_pack_payload(output_dir=output_dir)
        )
        source_maintenance_window_pack_artifact = str(maintenance_window_pack_path)
    if control_pack_payload is None:
        control_pack_path, control_pack_payload = load_latest_nav_history_archive_control_pack_payload(
            output_dir=output_dir,
        )
        source_control_pack_artifact = str(control_pack_path)
    if preflight_payload is None:
        preflight_path, preflight_payload = load_latest_nav_history_archive_preflight_payload(
            output_dir=output_dir,
        )
        source_preflight_artifact = str(preflight_path)

    maintenance_window_pack_payload = dict(maintenance_window_pack_payload or {})
    control_pack_payload = dict(control_pack_payload or {})
    preflight_payload = dict(preflight_payload or {})
    resolved_project_root, can_bind_backup_artifact = _resolve_project_root(
        project_root=project_root,
        preflight_payload=preflight_payload,
    )

    maintenance_summary = _normalize_dict(maintenance_window_pack_payload.get("summary"))
    control_summary = _normalize_dict(control_pack_payload.get("summary"))
    preflight_summary = _normalize_dict(preflight_payload.get("summary"))
    preflight_checks = _normalize_dict(preflight_payload.get("checks"))

    ready_for_manual_signoff = bool(
        control_summary.get(
            "ready_for_manual_signoff",
            preflight_checks.get(
                "ready_for_manual_signoff",
                preflight_summary.get("ready_for_manual_signoff"),
            ),
        )
    )
    package_status = str(control_summary.get("package_status") or "").strip()
    window_status = str(maintenance_summary.get("window_status") or "").strip()
    archive_cutoff_date = str(maintenance_summary.get("archive_cutoff_date") or "").strip()
    archive_target_path = str(
        maintenance_summary.get("archive_target_path")
        or control_summary.get("archive_target_path")
        or ""
    ).strip() or (
        build_nav_history_archive_target_path(
            archive_cutoff_date,
            maintenance_defaults=maintenance_defaults,
        )
    )
    archive_table_name = str(
        maintenance_summary.get("archive_table_name")
        or control_summary.get("archive_table_name")
        or maintenance_defaults.get("archive_table_name")
        or DEFAULT_ARCHIVE_TABLE_NAME
    ).strip() or DEFAULT_ARCHIVE_TABLE_NAME
    operator_assignments = dict(maintenance_defaults.get("operator_assignments") or {})
    vacuum_decision_default = str(
        maintenance_defaults.get("vacuum_decision_default") or ""
    ).strip()
    backup_dir_path = (
        (resolved_project_root / "backend" / "data" / "backups").resolve()
        if can_bind_backup_artifact
        else None
    )
    backup_file_count = len(_iter_files(backup_dir_path))
    latest_backup_path = _latest_file(backup_dir_path)
    latest_backup_path_text = _stringify_project_path(
        latest_backup_path,
        resolved_project_root,
    )

    checklist_status = "review_required"
    message = "当前执行清单仅供维护窗口内人工逐项核对与留痕，不提供页面 apply。"
    action_hint = "应以 execution checklist + maintenance window pack 作为维护窗口内的单一执行模板。"
    if (
        ready_for_manual_signoff
        and package_status == "ready_for_manual_window"
        and window_status == "ready_for_manual_window"
    ):
        checklist_status = "ready_for_manual_execution"
        message = "当前 NAV 归档链路已形成最终执行清单，可直接用于维护窗口内的人工执行、回退核对和留痕。"
        action_hint = (
            "页面不提供 apply；应在维护窗口内按 execution checklist、maintenance window pack、"
            "control pack 的顺序人工执行并补齐证据。"
        )
    elif maintenance_window_pack_payload:
        message = (
            "当前 NAV 归档维护窗口包已生成，但最终执行清单仍处于复核状态；"
            "进入真实维护前应先闭合剩余门禁项。"
        )
        action_hint = "先确认 preflight、control pack 与 maintenance window pack 来源一致，再用该清单做最终签字。"

    final_gate_checks = [
        "确认维护窗口编号、开始/结束时间、执行人、复核人、回退责任人已经预先登记。",
        f"确认本次归档 cutoff 固定为 {archive_cutoff_date or '待确认'}，执行过程中不得临时更改。",
        "确认维护窗口内已暂停相关净值写入，或已明确接受 live 冷数据统计快照与实时写入之间的一致性边界。",
        "确认删前抽样核验、归档计数核验、删除门禁与删后 postcheck 均已有对应执行记录栏位。",
        "确认任何一步若命中 stop conditions 或 rollback triggers，必须立即停止后续 SQL 并转入回退预案。",
    ]
    backup_and_restore_checks = [
        "记录主库备份文件路径、生成时间、文件大小、校验人和恢复责任人。",
        "确认备份文件与归档库文件不在同一临时目录，避免维护窗口内单点损坏。",
        "确认恢复演练命令、恢复时长预估和 rollback owner 已完成预先确认。",
        "确认删前可随时访问最新主库备份，且备份文件在维护窗口内不会被覆盖。",
    ]
    archive_target_checks = [
        f"确认归档目标文件路径为 {archive_target_path or 'backend/data/archives/...'}。",
        f"确认归档表名为 {archive_table_name or 'fund_nav_history_archive'}，且目标库 attach 后可正常建表/插入。",
        "确认 archive_sql_preview 和 postcheck_sql_preview 使用的是同一归档库文件路径。",
        "确认归档库空间、宿主磁盘剩余空间与归档估算体积满足本次窗口要求。",
    ]
    execution_order = [
        "1. 记录维护窗口元信息，冻结 cutoff、备份路径、归档路径与执行责任人。",
        "2. 执行 sample_validation_queries，固定删前冷数据范围并补留抽样证据。",
        "3. 执行 archive_sql_preview，将 cutoff 前冷数据写入独立归档库。",
        "4. 复核归档表计数、抽样基金范围和归档路径，满足删除门禁后才允许进入 delete_sql_preview。",
        "5. 执行 delete_sql_preview 与 postcheck_sql_preview，记录删后 remaining_cold_rows 和异常项。",
        "6. 评估是否执行 VACUUM，并补留删后库体积、freelist_count 与业务恢复确认结果。",
    ]
    rollback_triggers = [
        "归档表计数与主库冷数据计数不一致。",
        "抽样基金最早/最晚净值日期与主库冷数据样本不一致。",
        "delete_sql_preview 执行前未完成归档路径、备份路径或签字人确认。",
        "postcheck 返回 remaining_cold_rows 非 0，或发现 cutoff 之外的数据被误删。",
        "维护窗口内出现新的实时写入、库锁异常或归档目标不可访问。",
    ]
    rollback_prerequisites = [
        "主库备份文件路径、恢复命令与恢复责任人均已登记。",
        "归档库文件路径、归档表名与已写入行数均已记录。",
        "能够明确识别 delete_sql_preview 是否已执行，以及已执行到哪一步。",
        "维护窗口内具备暂停业务写入、回滚后做一致性复核和重新开放业务的责任链。",
    ]
    post_maintenance_measurements = [
        "记录删前/删后主库冷数据行数、归档行数和 remaining_cold_rows。",
        "记录删前/删后主库文件大小、freelist_count、WAL 状态和归档库文件大小。",
        "记录维护窗口完成时间、业务恢复时间以及执行中发现的异常或人工补救动作。",
        "若未立即 VACUUM，记录延后 VACUUM 的原因和计划执行窗口。",
    ]
    vacuum_decision_checklist = [
        "若 freelist_count 与 database_reclaimable_mb 较高，可评估在后续独立窗口执行 VACUUM。",
        "若当前窗口对停机时长敏感，优先完成归档/删除和证据留存，VACUUM 延后处理。",
        "执行 VACUUM 前再次确认备份已完成，且业务方接受额外锁库时长。",
        "无论是否执行 VACUUM，都应补留决策理由与责任人。",
    ]
    signoff_fields = [
        {
            "field": "maintenance_window_metadata_confirmed",
            "required": True,
            "description": "确认维护窗口编号、责任人、执行时间和回退责任链。",
        },
        {
            "field": "backup_restore_ready",
            "required": True,
            "description": "确认主库备份、恢复路径和恢复责任人。",
        },
        {
            "field": "archive_target_ready",
            "required": True,
            "description": "确认归档目标库、归档表与磁盘空间。",
        },
        {
            "field": "delete_gate_confirmed",
            "required": True,
            "description": "确认删除门禁只在归档计数和抽样复核一致后触发。",
        },
        {
            "field": "rollback_plan_confirmed",
            "required": True,
            "description": "确认 rollback triggers、回退步骤与证据记录模板。",
        },
        {
            "field": "vacuum_decision_recorded",
            "required": True,
            "description": "确认是否在本窗口执行 VACUUM，并记录理由。",
        },
    ]
    execution_record_template = {
        "window_id": "",
        "maintenance_owner": str(operator_assignments.get("maintenance_owner") or "").strip(),
        "db_operator": str(operator_assignments.get("db_operator") or "").strip(),
        "data_reviewer": str(operator_assignments.get("data_reviewer") or "").strip(),
        "rollback_owner": str(operator_assignments.get("rollback_owner") or "").strip(),
        "started_at": "",
        "archive_completed_at": "",
        "delete_completed_at": "",
        "completed_at": "",
        "backup_artifact_path": "",
        "archive_artifact_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "archive_row_count": 0,
        "pre_delete_cold_row_count": 0,
        "post_delete_remaining_cold_rows": 0,
        "vacuum_decision": vacuum_decision_default or "pending",
        "vacuum_executed": False,
        "rollback_required": False,
        "rollback_completed": False,
        "evidence_paths": [],
        "notes": "",
    }

    findings = _normalize_list(maintenance_window_pack_payload.get("findings"), limit=3)
    if not findings:
        findings = _normalize_list(control_pack_payload.get("findings"), limit=3)
    if not findings:
        findings = _normalize_list(preflight_payload.get("findings"), limit=3)
    if not findings:
        findings = [message]

    summary = {
        "checklist_status": checklist_status,
        "window_status": window_status,
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "recommended_retention_years": _normalize_int(maintenance_summary.get("recommended_retention_years")) or None,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": _normalize_int(maintenance_summary.get("estimated_archive_row_count")),
        "estimated_archive_fund_count": _normalize_int(maintenance_summary.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": _normalize_float(maintenance_summary.get("estimated_archive_size_mb")),
        "archive_target_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "live_cold_row_count": _normalize_int(maintenance_summary.get("live_cold_row_count")),
        "live_cold_fund_count": _normalize_int(maintenance_summary.get("live_cold_fund_count")),
        "live_min_nav_date": str(maintenance_summary.get("live_min_nav_date") or "").strip(),
        "live_max_nav_date": str(maintenance_summary.get("live_max_nav_date") or "").strip(),
        "backup_dir_path": _stringify_project_path(backup_dir_path, resolved_project_root),
        "backup_file_count": backup_file_count,
        "latest_backup_path": latest_backup_path_text,
        "page_apply_supported": False,
        "execution_mode": "manual_window_only",
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(maintenance_window_pack_payload) and bool(control_pack_payload) and bool(preflight_payload),
        "message": message,
        "action_hint": action_hint,
        "source_nav_history_archive_maintenance_window_pack_artifact": source_maintenance_window_pack_artifact or "",
        "source_nav_history_archive_maintenance_window_pack_generated_at": maintenance_window_pack_payload.get("generated_at") or "",
        "source_nav_history_archive_control_pack_artifact": source_control_pack_artifact or "",
        "source_nav_history_archive_control_pack_generated_at": control_pack_payload.get("generated_at") or "",
        "source_nav_history_archive_preflight_artifact": source_preflight_artifact or "",
        "source_nav_history_archive_preflight_generated_at": preflight_payload.get("generated_at") or "",
        "source_nav_history_archive_maintenance_window_pack_summary": maintenance_summary,
        "source_nav_history_archive_control_pack_summary": control_summary,
        "source_nav_history_archive_preflight_summary": preflight_summary,
        "summary": summary,
        "final_gate_checks": final_gate_checks,
        "backup_and_restore_checks": backup_and_restore_checks,
        "archive_target_checks": archive_target_checks,
        "execution_order": execution_order,
        "rollback_triggers": rollback_triggers,
        "rollback_prerequisites": rollback_prerequisites,
        "post_maintenance_measurements": post_maintenance_measurements,
        "vacuum_decision_checklist": vacuum_decision_checklist,
        "signoff_fields": signoff_fields,
        "execution_record_template": execution_record_template,
        "findings": findings,
    }


def render_nav_history_archive_execution_checklist_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档最终执行清单 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive execution checklist",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        f"- source_maintenance_window_pack_artifact: {payload.get('source_nav_history_archive_maintenance_window_pack_artifact', '')}",
        f"- source_control_pack_artifact: {payload.get('source_nav_history_archive_control_pack_artifact', '')}",
        f"- source_preflight_artifact: {payload.get('source_nav_history_archive_preflight_artifact', '')}",
        f"- checklist_status: {summary.get('checklist_status', '')}",
        f"- window_status: {summary.get('window_status', '')}",
        f"- package_status: {summary.get('package_status', '')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff', False)}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        f"- backup_file_count: {summary.get('backup_file_count', 0)}",
        f"- latest_backup_path: {summary.get('latest_backup_path', '')}",
        "",
        "## final_gate_checks",
    ]
    for item in payload.get("final_gate_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## backup_and_restore_checks"])
    for item in payload.get("backup_and_restore_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## archive_target_checks"])
    for item in payload.get("archive_target_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_order"])
    for item in payload.get("execution_order") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## rollback_triggers"])
    for item in payload.get("rollback_triggers") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## rollback_prerequisites"])
    for item in payload.get("rollback_prerequisites") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## post_maintenance_measurements"])
    for item in payload.get("post_maintenance_measurements") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## vacuum_decision_checklist"])
    for item in payload.get("vacuum_decision_checklist") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## signoff_fields"])
    for item in payload.get("signoff_fields") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## execution_record_template"])
    lines.append(json.dumps(payload.get("execution_record_template") or {}, ensure_ascii=False, indent=2))

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_execution_checklist_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档最终执行清单 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_execution_checklist_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_execution_checklist_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_execution_checklist_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
