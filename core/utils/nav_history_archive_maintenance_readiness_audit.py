#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档维护窗口准备度审计（只读）。
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_control_pack import (
    load_latest_nav_history_archive_control_pack_payload,
)
from utils.nav_history_archive_execution_checklist import (
    load_latest_nav_history_archive_execution_checklist_payload,
)
from utils.nav_history_archive_maintenance_window_pack import (
    load_latest_nav_history_archive_maintenance_window_pack_payload,
)
from utils.nav_history_archive_preflight import (
    load_latest_nav_history_archive_preflight_payload,
)


NAV_HISTORY_ARCHIVE_MAINTENANCE_READINESS_AUDIT_JSON_PATTERN = (
    "nav_history_archive_maintenance_readiness_audit_*.json"
)


def load_latest_nav_history_archive_maintenance_readiness_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档维护窗口准备度审计。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_MAINTENANCE_READINESS_AUDIT_JSON_PATTERN,
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


def _normalize_path(value, project_root: Path) -> Optional[Path]:
    text = _normalize_text(value)
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    else:
        path = path.resolve()
        if not path.exists():
            parts = path.parts
            for marker in ("backend", "reports", "config", "core", "scripts", "frontend"):
                if marker in parts:
                    index = parts.index(marker)
                    return (project_root / Path(*parts[index:])).resolve()
    return path


def _resolve_project_root(project_root: Optional[str]) -> Path:
    if not project_root:
        return PROJECT_ROOT
    return Path(project_root).resolve()


def _resolve_output_dir(output_dir: str, project_root: Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def _artifact_ref_matches(left, right, project_root: Path) -> bool:
    left_path = _normalize_path(left, project_root)
    right_path = _normalize_path(right, project_root)
    if left_path is None or right_path is None:
        return False
    return left_path == right_path


def _artifact_generated_at_matches(left, right) -> bool:
    left_text = _normalize_text(left)
    right_text = _normalize_text(right)
    if not left_text or not right_text:
        return False
    return left_text == right_text


def _is_dir_writable(path: Optional[Path]) -> bool:
    return bool(path and path.exists() and path.is_dir() and os.access(path, os.W_OK | os.X_OK))


def _iter_files(path: Optional[Path]):
    if not path or not path.exists() or not path.is_dir():
        return []
    return [item for item in path.iterdir() if item.is_file()]


def _latest_file(path: Optional[Path]) -> Optional[Path]:
    files = _iter_files(path)
    if not files:
        return None
    return max(files, key=lambda item: item.stat().st_mtime_ns)


def _find_existing_probe_path(path: Optional[Path]) -> Optional[Path]:
    current = path
    while current is not None:
        if current.exists():
            return current
        if current.parent == current:
            return None
        current = current.parent
    return None


def _classify_disk_usage(usage_percent: float) -> str:
    if usage_percent >= 95:
        return "critical"
    if usage_percent >= 85:
        return "warning"
    return "healthy"


def _build_check(key: str, label: str, status: str, detail: str, action_hint: str = "") -> dict:
    return {
        "key": key,
        "label": label,
        "status": status,
        "detail": detail,
        "action_hint": action_hint,
    }


def _build_findings(*, blockers: list[str], review_items: list[str], message: str) -> list[str]:
    findings = []
    if blockers:
        findings.append(
            f"当前维护窗口准备度存在 {len(blockers)} 个阻断项，尚不应进入真实维护窗口。"
        )
        findings.extend(blockers[:4])
    elif review_items:
        findings.append(
            f"当前维护窗口准备度仍有 {len(review_items)} 个待补齐项，进入真实维护前应先完成人工确认。"
        )
        findings.extend(review_items[:4])
    else:
        findings.append(message)
    return findings


def build_nav_history_archive_maintenance_readiness_audit_payload(
    *,
    db_path: Optional[str] = None,
    project_root: Optional[str] = None,
    execution_checklist_payload: Optional[dict] = None,
    maintenance_window_pack_payload: Optional[dict] = None,
    control_pack_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    source_execution_checklist_artifact: Optional[str] = None,
    source_maintenance_window_pack_artifact: Optional[str] = None,
    source_control_pack_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于最新只读产物生成维护窗口准备度审计。"""
    root = _resolve_project_root(project_root)
    resolved_output_dir = _resolve_output_dir(output_dir, root)

    if execution_checklist_payload is None:
        execution_checklist_path, execution_checklist_payload = (
            load_latest_nav_history_archive_execution_checklist_payload(
                output_dir=str(resolved_output_dir)
            )
        )
        source_execution_checklist_artifact = str(execution_checklist_path)
    if maintenance_window_pack_payload is None:
        maintenance_window_pack_path, maintenance_window_pack_payload = (
            load_latest_nav_history_archive_maintenance_window_pack_payload(
                output_dir=str(resolved_output_dir)
            )
        )
        source_maintenance_window_pack_artifact = str(maintenance_window_pack_path)
    if control_pack_payload is None:
        control_pack_path, control_pack_payload = (
            load_latest_nav_history_archive_control_pack_payload(
                output_dir=str(resolved_output_dir)
            )
        )
        source_control_pack_artifact = str(control_pack_path)
    if preflight_payload is None:
        preflight_path, preflight_payload = load_latest_nav_history_archive_preflight_payload(
            output_dir=str(resolved_output_dir)
        )
        source_preflight_artifact = str(preflight_path)

    execution_checklist_payload = dict(execution_checklist_payload or {})
    maintenance_window_pack_payload = dict(maintenance_window_pack_payload or {})
    control_pack_payload = dict(control_pack_payload or {})
    preflight_payload = dict(preflight_payload or {})

    execution_summary = _normalize_dict(execution_checklist_payload.get("summary"))
    maintenance_summary = _normalize_dict(maintenance_window_pack_payload.get("summary"))
    control_summary = _normalize_dict(control_pack_payload.get("summary"))
    preflight_summary = _normalize_dict(preflight_payload.get("summary"))
    preflight_checks = _normalize_dict(preflight_payload.get("checks"))

    checklist_status = _normalize_text(execution_summary.get("checklist_status"))
    window_status = _normalize_text(maintenance_summary.get("window_status"))
    package_status = _normalize_text(
        control_summary.get("package_status") or execution_summary.get("package_status")
    )
    ready_for_manual_signoff = bool(
        execution_summary.get(
            "ready_for_manual_signoff",
            control_summary.get(
                "ready_for_manual_signoff",
                preflight_checks.get(
                    "ready_for_manual_signoff",
                    preflight_summary.get("ready_for_manual_signoff"),
                ),
            ),
        )
    )

    archive_target_path = _normalize_text(
        execution_summary.get("archive_target_path")
        or maintenance_summary.get("archive_target_path")
        or control_summary.get("archive_target_path")
    )
    archive_table_name = _normalize_text(
        execution_summary.get("archive_table_name")
        or maintenance_summary.get("archive_table_name")
        or control_summary.get("archive_table_name")
    )
    archive_target_absolute_path = _normalize_path(archive_target_path, root)
    archive_target_parent_path = (
        archive_target_absolute_path.parent if archive_target_absolute_path is not None else None
    )

    db_path_text = _normalize_text(db_path) or _normalize_text(preflight_payload.get("db_path"))
    if db_path_text:
        active_db_path = _normalize_path(db_path_text, root)
    elif project_root:
        active_db_path = (root / "backend/data/bond_funds.db").resolve()
    else:
        active_db_path = Path(resolve_db_path("database/bond_funds.db")).resolve()
    active_db_exists = bool(active_db_path and active_db_path.exists() and active_db_path.is_file())
    active_db_size_bytes = active_db_path.stat().st_size if active_db_exists else 0
    active_db_parent_exists = bool(active_db_path and active_db_path.parent.exists())

    backup_dir_path = (root / "backend/data/backups").resolve()
    backup_dir_exists = backup_dir_path.exists() and backup_dir_path.is_dir()
    backup_dir_writable = _is_dir_writable(backup_dir_path)
    backup_file_count = len(_iter_files(backup_dir_path))
    latest_backup_path = _latest_file(backup_dir_path)
    latest_backup_size_bytes = latest_backup_path.stat().st_size if latest_backup_path else 0

    audit_dir_path = resolved_output_dir
    audit_dir_exists = audit_dir_path.exists() and audit_dir_path.is_dir()
    audit_dir_writable = _is_dir_writable(audit_dir_path)
    audit_file_count = len(_iter_files(audit_dir_path))

    archive_target_parent_exists = bool(
        archive_target_parent_path and archive_target_parent_path.exists() and archive_target_parent_path.is_dir()
    )
    archive_target_parent_writable = _is_dir_writable(archive_target_parent_path)

    project_disk_usage = shutil.disk_usage(root)
    project_disk_usage_percent = (
        round((project_disk_usage.used / project_disk_usage.total) * 100, 2)
        if project_disk_usage.total
        else 0.0
    )
    project_disk_status = _classify_disk_usage(project_disk_usage_percent)

    archive_probe_path = _find_existing_probe_path(archive_target_parent_path or archive_target_absolute_path)
    archive_disk_usage = shutil.disk_usage(archive_probe_path or root)
    archive_target_disk_free_bytes = int(archive_disk_usage.free)
    archive_target_disk_free_mb = round(archive_target_disk_free_bytes / (1024 * 1024), 2)

    estimated_archive_size_mb = _normalize_float(
        execution_summary.get("estimated_archive_size_mb")
        or maintenance_summary.get("estimated_archive_size_mb")
        or control_summary.get("estimated_archive_size_mb")
    )
    estimated_archive_size_bytes = int(max(estimated_archive_size_mb, 0.0) * 1024 * 1024)
    archive_target_free_ratio = (
        round(archive_target_disk_free_bytes / estimated_archive_size_bytes, 2)
        if estimated_archive_size_bytes > 0
        else None
    )

    operator_roles = list(control_pack_payload.get("operator_roles") or [])
    operator_placeholder_checks = []
    operator_placeholders_complete = bool(operator_roles)
    for item in operator_roles:
        role = _normalize_text(item.get("role"))
        if not role:
            continue
        assignee = _normalize_text(item.get("assignee"))
        required = bool(item.get("required"))
        status = "ok" if assignee or not required else "review_required"
        if required and not assignee:
            operator_placeholders_complete = False
        operator_placeholder_checks.append(
            {
                "role": role,
                "required": required,
                "assignee": assignee,
                "status": status,
                "responsibility": _normalize_text(item.get("responsibility")),
            }
        )

    execution_record_template = _normalize_dict(
        execution_checklist_payload.get("execution_record_template")
    )
    backup_artifact_path = _normalize_text(execution_record_template.get("backup_artifact_path"))
    backup_artifact_absolute_path = _normalize_path(backup_artifact_path, root)
    backup_artifact_exists = bool(
        backup_artifact_absolute_path
        and backup_artifact_absolute_path.exists()
        and backup_artifact_absolute_path.is_file()
    )
    backup_prerequisites_complete = (
        bool(execution_checklist_payload.get("backup_and_restore_checks"))
        and "backup_artifact_path" in execution_record_template
        and bool(execution_record_template.get("archive_artifact_path") or archive_target_path)
        and bool(execution_record_template.get("archive_table_name") or archive_table_name)
        and "rollback_owner" in execution_record_template
    )
    vacuum_decision = _normalize_text(execution_record_template.get("vacuum_decision"))
    vacuum_decision_recorded = vacuum_decision not in {"", "pending"}
    backup_baseline_status = "ok"
    backup_baseline_detail = (
        f"备份目录已有 {backup_file_count} 个文件，execution checklist 已绑定真实备份文件 "
        f"{backup_artifact_absolute_path}。"
    )
    backup_baseline_hint = "进入真实维护窗口前，必须至少绑定一份可访问的真实备份基线。"
    if backup_dir_exists and backup_dir_writable:
        if backup_file_count <= 0:
            backup_baseline_status = "review_required"
            backup_baseline_detail = (
                f"备份目录 {backup_dir_path} 当前没有任何备份文件，尚未形成真实备份基线。"
            )
            backup_baseline_hint = "先生成并落盘主库备份，再更新 execution checklist 中的 backup_artifact_path。"
        elif not backup_artifact_path:
            backup_baseline_status = "review_required"
            backup_baseline_detail = (
                "execution checklist 尚未记录 backup_artifact_path，当前无法把维护窗口绑定到具体备份文件。"
            )
            backup_baseline_hint = "把 execution checklist 的 backup_artifact_path 指向一份已存在的真实备份文件。"
        elif not backup_artifact_exists:
            backup_baseline_status = "review_required"
            backup_baseline_detail = (
                f"execution checklist 记录的备份文件不存在：{backup_artifact_absolute_path}。"
            )
            backup_baseline_hint = "修正 backup_artifact_path，确保它指向备份目录内一份真实存在的备份文件。"

    source_chain_checks = []
    comparison_specs = (
        (
            execution_checklist_payload.get(
                "source_nav_history_archive_maintenance_window_pack_artifact"
            ),
            source_maintenance_window_pack_artifact,
            execution_checklist_payload.get(
                "source_nav_history_archive_maintenance_window_pack_generated_at"
            ),
            maintenance_window_pack_payload.get("generated_at"),
        ),
        (
            execution_checklist_payload.get("source_nav_history_archive_control_pack_artifact"),
            source_control_pack_artifact,
            execution_checklist_payload.get("source_nav_history_archive_control_pack_generated_at"),
            control_pack_payload.get("generated_at"),
        ),
        (
            execution_checklist_payload.get("source_nav_history_archive_preflight_artifact"),
            source_preflight_artifact,
            execution_checklist_payload.get("source_nav_history_archive_preflight_generated_at"),
            preflight_payload.get("generated_at"),
        ),
    )
    for left_path, right_path, left_time, right_time in comparison_specs:
        if _normalize_text(left_path) and _normalize_text(right_path):
            source_chain_checks.append(
                _artifact_ref_matches(left_path, right_path, root)
            )
        if _normalize_text(left_time) and _normalize_text(right_time):
            source_chain_checks.append(
                _artifact_generated_at_matches(left_time, right_time)
            )
    source_chain_consistent = all(source_chain_checks) if source_chain_checks else True

    approval_chain_ready = (
        ready_for_manual_signoff
        and checklist_status == "ready_for_manual_execution"
        and window_status == "ready_for_manual_window"
        and package_status == "ready_for_manual_window"
    )

    archive_target_capacity_status = "ok"
    archive_target_capacity_detail = (
        f"归档目标文件系统剩余 {archive_target_disk_free_mb} MB，"
        f"理论归档体积 {estimated_archive_size_mb:.2f} MB。"
    )
    archive_target_capacity_hint = "当前磁盘空间与理论归档体积匹配。"
    if estimated_archive_size_bytes <= 0:
        archive_target_capacity_status = "review_required"
        archive_target_capacity_detail = "当前未形成有效的理论归档体积估算，无法判断归档目标空间是否充分。"
        archive_target_capacity_hint = "先重新确认 estimated_archive_size_mb，再决定是否进入维护窗口。"
    elif archive_target_disk_free_bytes < estimated_archive_size_bytes:
        archive_target_capacity_status = "blocked"
        archive_target_capacity_hint = "归档目标文件系统剩余空间低于理论归档体积，先扩容或更换归档落点。"
    elif archive_target_disk_free_bytes < int(estimated_archive_size_bytes * 1.5):
        archive_target_capacity_status = "review_required"
        archive_target_capacity_hint = "归档目标剩余空间偏紧，建议先确认安全冗余或调整归档落点。"

    project_disk_status_for_readiness = "ok"
    project_disk_detail = (
        f"项目所在文件系统使用率 {project_disk_usage_percent}% ，"
        f"剩余 {round(project_disk_usage.free / (1024 * 1024), 2)} MB。"
    )
    project_disk_hint = "当前项目文件系统仍有可用空间。"
    if project_disk_status == "critical":
        project_disk_status_for_readiness = "blocked"
        project_disk_hint = "项目所在文件系统已接近打满，当前不应进入真实维护窗口。"
    elif project_disk_status == "warning":
        project_disk_status_for_readiness = "review_required"
        project_disk_hint = "项目所在文件系统已进入高压区，建议先确认本轮维护的空间冗余。"

    readiness_checks = [
        _build_check(
            "approval_chain",
            "审批链路",
            "ok" if approval_chain_ready else "blocked",
            (
                f"checklist={checklist_status or 'missing'} / "
                f"window={window_status or 'missing'} / "
                f"package={package_status or 'missing'} / "
                f"signoff={'yes' if ready_for_manual_signoff else 'no'}"
            ),
            "只有 execution checklist、maintenance window pack、control pack 与 preflight 全部闭合后，才允许进入真实维护窗口。",
        ),
        _build_check(
            "source_chain",
            "来源一致性",
            "ok" if source_chain_consistent else "blocked",
            "execution checklist 与 maintenance window pack / control pack / preflight 的来源路径和时间戳一致。"
            if source_chain_consistent
            else "execution checklist 的来源路径或时间戳与上游产物不一致。",
            "链路漂移时必须重新生成对应只读产物，不要带着旧清单进入维护窗口。",
        ),
        _build_check(
            "primary_database",
            "活动主库",
            "ok" if active_db_exists else "blocked",
            f"活动主库路径 {active_db_path or '—'}，大小 {round(active_db_size_bytes / (1024 * 1024), 2)} MB。"
            if active_db_exists
            else f"活动主库路径 {active_db_path or '—'} 不存在。",
            "先确认当前运行主库路径和文件状态，再评估归档窗口。",
        ),
        _build_check(
            "backup_directory",
            "备份目录",
            "ok" if backup_dir_exists and backup_dir_writable else "blocked",
            f"备份目录 {backup_dir_path}，文件 {backup_file_count} 个。"
            if backup_dir_exists
            else f"备份目录 {backup_dir_path} 不存在。",
            "维护窗口前必须确认备份目录存在且可写，且最新备份不会被覆盖。",
        ),
        _build_check(
            "backup_baseline",
            "真实备份基线",
            backup_baseline_status,
            backup_baseline_detail,
            backup_baseline_hint,
        ),
        _build_check(
            "audit_directory",
            "审计目录",
            "ok" if audit_dir_exists and audit_dir_writable else "blocked",
            f"审计目录 {audit_dir_path}，文件 {audit_file_count} 个。"
            if audit_dir_exists
            else f"审计目录 {audit_dir_path} 不存在。",
            "维护窗口证据需要回写到 audit 目录，目录不可写时不应推进。",
        ),
        _build_check(
            "archive_target_parent",
            "归档目标目录",
            "ok" if archive_target_parent_exists and archive_target_parent_writable else "blocked",
            f"归档目标 {archive_target_absolute_path or '—'}，父目录 {archive_target_parent_path or '—'}。"
            if archive_target_path
            else "当前未解析出归档目标路径。",
            "先确认 archive_target_path 的父目录存在且可写，再进入维护窗口。",
        ),
        _build_check(
            "project_disk_capacity",
            "项目磁盘空间",
            project_disk_status_for_readiness,
            project_disk_detail,
            project_disk_hint,
        ),
        _build_check(
            "archive_target_capacity",
            "归档落点空间",
            archive_target_capacity_status,
            archive_target_capacity_detail,
            archive_target_capacity_hint,
        ),
        _build_check(
            "backup_prerequisites",
            "备份前提模板",
            "ok" if backup_prerequisites_complete else "review_required",
            "execution checklist 已包含 backup_artifact_path、archive_artifact_path、archive_table_name 与 rollback_owner 模板字段。"
            if backup_prerequisites_complete
            else "execution checklist 仍缺少备份/回退前提模板字段或 backup_and_restore_checks。",
            "只读模板应先完整，再让执行人在维护窗口内补录真实备份信息。",
        ),
        _build_check(
            "operator_placeholders",
            "责任人占位",
            "ok" if operator_placeholders_complete else "review_required",
            "maintenance_owner / db_operator / data_reviewer / rollback_owner 已全部明确。"
            if operator_placeholders_complete
            else "仍有必填角色未填写 assignee。",
            "进入真实维护前必须补齐维护负责人、执行人、复核人和回退责任人。",
        ),
        _build_check(
            "vacuum_decision",
            "VACUUM 决策",
            "ok" if vacuum_decision_recorded else "review_required",
            f"当前 vacuum_decision={vacuum_decision or 'pending'}。",
            "无论本窗口是否执行 VACUUM，都应先记录决策与理由。",
        ),
    ]

    blockers: list[str] = []
    review_items: list[str] = []

    if not approval_chain_ready:
        blockers.append("上游审批链路尚未达到 ready 状态，当前不应进入真实维护窗口。")
    if not source_chain_consistent:
        blockers.append("execution checklist 与上游维护产物来源不一致，必须先重新生成并对齐链路。")
    if not active_db_exists:
        blockers.append(f"活动主库文件不存在：{active_db_path or '—'}。")
    if not backup_dir_exists:
        blockers.append(f"备份目录不存在：{backup_dir_path}。")
    elif not backup_dir_writable:
        blockers.append(f"备份目录不可写：{backup_dir_path}。")
    if not audit_dir_exists:
        blockers.append(f"审计目录不存在：{audit_dir_path}。")
    elif not audit_dir_writable:
        blockers.append(f"审计目录不可写：{audit_dir_path}。")
    if not archive_target_path:
        blockers.append("当前 execution checklist 未提供 archive_target_path。")
    elif not archive_target_parent_exists:
        blockers.append(f"归档目标父目录不存在：{archive_target_parent_path or '—'}。")
    elif not archive_target_parent_writable:
        blockers.append(f"归档目标父目录不可写：{archive_target_parent_path or '—'}。")
    if project_disk_status_for_readiness == "blocked":
        blockers.append("项目所在文件系统已接近打满，进入维护窗口存在明显空间风险。")
    if archive_target_capacity_status == "blocked":
        blockers.append("归档目标文件系统剩余空间低于理论归档体积，当前不应执行归档。")

    if backup_baseline_status == "review_required":
        if backup_file_count <= 0:
            review_items.append("备份目录当前没有任何备份文件，尚未形成真实备份基线。")
        elif not backup_artifact_path:
            review_items.append(
                "execution checklist 尚未记录 backup_artifact_path，进入真实维护前应绑定到一份真实备份文件。"
            )
        elif not backup_artifact_exists:
            review_items.append(
                f"execution checklist 记录的 backup_artifact_path 不存在：{backup_artifact_absolute_path}。"
            )
    if project_disk_status_for_readiness == "review_required":
        review_items.append("项目所在文件系统已进入高压区，建议先确认本轮维护的空间冗余。")
    if archive_target_capacity_status == "review_required":
        review_items.append("归档目标剩余空间安全冗余不足，建议先确认容量余量。")
    if not backup_prerequisites_complete:
        review_items.append("execution checklist 的备份/回退模板尚未完全闭合。")
    if not operator_placeholders_complete:
        missing_roles = [
            item["role"]
            for item in operator_placeholder_checks
            if item.get("required") and not item.get("assignee")
        ]
        review_items.append(
            "以下必填责任人仍未补齐 assignee: "
            + "、".join(missing_roles or ["maintenance_owner/db_operator/data_reviewer/rollback_owner"])
            + "。"
        )
    if not vacuum_decision_recorded:
        review_items.append("VACUUM 决策仍为 pending，进入真实维护前应先记录是否执行及原因。")

    readiness_status = "ready_for_manual_window_execution"
    message = "当前维护窗口准备度审计通过，可在人工维护窗口内按 execution checklist 执行。"
    action_hint = "页面仍不提供 apply；真实归档、删除、回退与证据补录都必须在人工维护窗口内执行。"
    if blockers:
        readiness_status = "blocked"
        message = "当前维护窗口准备度未通过，存在阻断项。"
        action_hint = blockers[0]
    elif review_items:
        readiness_status = "review_required"
        message = "当前维护窗口准备度仍有待补齐项。"
        action_hint = review_items[0]

    findings = _build_findings(
        blockers=blockers,
        review_items=review_items,
        message=message,
    )

    summary = {
        "readiness_status": readiness_status,
        "ready_for_manual_window_execution": readiness_status == "ready_for_manual_window_execution",
        "checklist_status": checklist_status,
        "window_status": window_status,
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "recommended_retention_years": _normalize_int(
            execution_summary.get("recommended_retention_years")
            or maintenance_summary.get("recommended_retention_years")
            or control_summary.get("recommended_retention_years")
        ) or None,
        "archive_cutoff_date": _normalize_text(
            execution_summary.get("archive_cutoff_date")
            or maintenance_summary.get("archive_cutoff_date")
            or control_summary.get("archive_cutoff_date")
        ),
        "estimated_archive_row_count": _normalize_int(
            execution_summary.get("estimated_archive_row_count")
            or maintenance_summary.get("estimated_archive_row_count")
            or control_summary.get("estimated_archive_row_count")
        ),
        "estimated_archive_fund_count": _normalize_int(
            execution_summary.get("estimated_archive_fund_count")
            or maintenance_summary.get("estimated_archive_fund_count")
            or control_summary.get("estimated_archive_fund_count")
        ),
        "estimated_archive_size_mb": estimated_archive_size_mb,
        "archive_target_path": archive_target_path,
        "archive_target_absolute_path": str(archive_target_absolute_path or "").strip(),
        "archive_target_parent_path": str(archive_target_parent_path or "").strip(),
        "archive_target_parent_exists": archive_target_parent_exists,
        "archive_target_parent_writable": archive_target_parent_writable,
        "archive_table_name": archive_table_name,
        "active_db_path": str(active_db_path or "").strip(),
        "active_db_exists": active_db_exists,
        "active_db_parent_exists": active_db_parent_exists,
        "active_db_size_mb": round(active_db_size_bytes / (1024 * 1024), 2),
        "backup_dir_path": str(backup_dir_path),
        "backup_dir_exists": backup_dir_exists,
        "backup_dir_writable": backup_dir_writable,
        "backup_file_count": backup_file_count,
        "backup_artifact_path": backup_artifact_path,
        "backup_artifact_absolute_path": str(backup_artifact_absolute_path or "").strip(),
        "backup_artifact_exists": backup_artifact_exists,
        "latest_backup_path": str(latest_backup_path or "").strip(),
        "latest_backup_size_mb": round(latest_backup_size_bytes / (1024 * 1024), 2),
        "audit_dir_path": str(audit_dir_path),
        "audit_dir_exists": audit_dir_exists,
        "audit_dir_writable": audit_dir_writable,
        "audit_file_count": audit_file_count,
        "project_disk_total_mb": round(project_disk_usage.total / (1024 * 1024), 2),
        "project_disk_free_mb": round(project_disk_usage.free / (1024 * 1024), 2),
        "project_disk_usage_percent": project_disk_usage_percent,
        "project_disk_status": project_disk_status,
        "archive_target_fs_probe_path": str(archive_probe_path or "").strip(),
        "archive_target_disk_free_mb": archive_target_disk_free_mb,
        "archive_target_free_ratio": archive_target_free_ratio,
        "backup_prerequisites_complete": backup_prerequisites_complete,
        "operator_placeholders_complete": operator_placeholders_complete,
        "vacuum_decision_recorded": vacuum_decision_recorded,
        "source_chain_consistent": source_chain_consistent,
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "page_apply_supported": False,
        "execution_mode": "manual_window_only",
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": all(
            (
                bool(execution_checklist_payload),
                bool(maintenance_window_pack_payload),
                bool(control_pack_payload),
                bool(preflight_payload),
            )
        ),
        "message": message,
        "action_hint": action_hint,
        "source_nav_history_archive_execution_checklist_artifact": source_execution_checklist_artifact or "",
        "source_nav_history_archive_execution_checklist_generated_at": execution_checklist_payload.get("generated_at") or "",
        "source_nav_history_archive_maintenance_window_pack_artifact": source_maintenance_window_pack_artifact or "",
        "source_nav_history_archive_maintenance_window_pack_generated_at": maintenance_window_pack_payload.get("generated_at") or "",
        "source_nav_history_archive_control_pack_artifact": source_control_pack_artifact or "",
        "source_nav_history_archive_control_pack_generated_at": control_pack_payload.get("generated_at") or "",
        "source_nav_history_archive_preflight_artifact": source_preflight_artifact or "",
        "source_nav_history_archive_preflight_generated_at": preflight_payload.get("generated_at") or "",
        "source_nav_history_archive_execution_checklist_summary": execution_summary,
        "source_nav_history_archive_maintenance_window_pack_summary": maintenance_summary,
        "source_nav_history_archive_control_pack_summary": control_summary,
        "source_nav_history_archive_preflight_summary": preflight_summary,
        "summary": summary,
        "readiness_checks": readiness_checks,
        "operator_placeholder_checks": operator_placeholder_checks,
        "blockers": blockers,
        "review_items": review_items,
        "findings": findings,
    }


def render_nav_history_archive_maintenance_readiness_audit_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档维护窗口准备度审计 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive maintenance readiness audit",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        f"- source_execution_checklist_artifact: {payload.get('source_nav_history_archive_execution_checklist_artifact', '')}",
        f"- source_maintenance_window_pack_artifact: {payload.get('source_nav_history_archive_maintenance_window_pack_artifact', '')}",
        f"- source_control_pack_artifact: {payload.get('source_nav_history_archive_control_pack_artifact', '')}",
        f"- source_preflight_artifact: {payload.get('source_nav_history_archive_preflight_artifact', '')}",
        f"- readiness_status: {summary.get('readiness_status', '')}",
        f"- blocker_count: {summary.get('blocker_count', 0)}",
        f"- review_item_count: {summary.get('review_item_count', 0)}",
        f"- active_db_path: {summary.get('active_db_path', '')}",
        f"- archive_target_absolute_path: {summary.get('archive_target_absolute_path', '')}",
        f"- project_disk_free_mb: {summary.get('project_disk_free_mb', 0.0)}",
        f"- archive_target_disk_free_mb: {summary.get('archive_target_disk_free_mb', 0.0)}",
        "",
        "## readiness_checks",
    ]
    for item in payload.get("readiness_checks") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## operator_placeholder_checks"])
    for item in payload.get("operator_placeholder_checks") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## blockers"])
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## review_items"])
    for item in payload.get("review_items") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_maintenance_readiness_audit_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档维护窗口准备度审计 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_maintenance_readiness_audit_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_maintenance_readiness_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_maintenance_readiness_audit_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
