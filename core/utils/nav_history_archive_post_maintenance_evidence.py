#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档执行后证据回填模板（只读）。
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
from utils.nav_history_archive_maintenance_defaults import (
    DEFAULT_ARCHIVE_TABLE_NAME,
    load_nav_history_archive_maintenance_defaults,
    normalize_nav_history_archive_maintenance_defaults,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    load_latest_nav_history_archive_maintenance_readiness_audit_payload,
)
from utils.nav_history_archive_maintenance_window_pack import (
    load_latest_nav_history_archive_maintenance_window_pack_payload,
)


NAV_HISTORY_ARCHIVE_POST_MAINTENANCE_EVIDENCE_JSON_PATTERN = (
    "nav_history_archive_post_maintenance_evidence_*.json"
)


def load_latest_nav_history_archive_post_maintenance_evidence_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档执行后证据回填模板。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_POST_MAINTENANCE_EVIDENCE_JSON_PATTERN,
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


def _normalize_list(value, limit: int = 0) -> list[str]:
    items = [str(item or "").strip() for item in (value or []) if str(item or "").strip()]
    if limit > 0:
        return items[:limit]
    return items


def _dedupe_preserve_order(items) -> list[str]:
    return list(dict.fromkeys(_normalize_list(items)))


def build_nav_history_archive_post_maintenance_evidence_payload(
    *,
    execution_checklist_payload: Optional[dict] = None,
    maintenance_readiness_audit_payload: Optional[dict] = None,
    maintenance_window_pack_payload: Optional[dict] = None,
    source_execution_checklist_artifact: Optional[str] = None,
    source_maintenance_readiness_audit_artifact: Optional[str] = None,
    source_maintenance_window_pack_artifact: Optional[str] = None,
    maintenance_defaults: Optional[dict] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于 NAV 归档执行清单与准备度审计生成执行后证据回填模板。"""
    maintenance_defaults = normalize_nav_history_archive_maintenance_defaults(
        maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    )
    if execution_checklist_payload is None:
        execution_checklist_path, execution_checklist_payload = (
            load_latest_nav_history_archive_execution_checklist_payload(output_dir=output_dir)
        )
        source_execution_checklist_artifact = str(execution_checklist_path)
    if maintenance_readiness_audit_payload is None:
        readiness_audit_path, maintenance_readiness_audit_payload = (
            load_latest_nav_history_archive_maintenance_readiness_audit_payload(
                output_dir=output_dir
            )
        )
        source_maintenance_readiness_audit_artifact = str(readiness_audit_path)
    if maintenance_window_pack_payload is None:
        maintenance_window_pack_path, maintenance_window_pack_payload = (
            load_latest_nav_history_archive_maintenance_window_pack_payload(output_dir=output_dir)
        )
        source_maintenance_window_pack_artifact = str(maintenance_window_pack_path)

    execution_checklist_payload = dict(execution_checklist_payload or {})
    maintenance_readiness_audit_payload = dict(maintenance_readiness_audit_payload or {})
    maintenance_window_pack_payload = dict(maintenance_window_pack_payload or {})

    execution_summary = _normalize_dict(execution_checklist_payload.get("summary"))
    readiness_summary = _normalize_dict(maintenance_readiness_audit_payload.get("summary"))
    window_summary = _normalize_dict(maintenance_window_pack_payload.get("summary"))
    execution_record_template = _normalize_dict(
        execution_checklist_payload.get("execution_record_template")
    )
    window_evidence_record_template = _normalize_dict(
        maintenance_window_pack_payload.get("evidence_record_template")
    )
    operator_assignments = dict(maintenance_defaults.get("operator_assignments") or {})

    archive_cutoff_date = str(
        readiness_summary.get("archive_cutoff_date")
        or execution_summary.get("archive_cutoff_date")
        or window_summary.get("archive_cutoff_date")
        or ""
    ).strip()
    archive_target_path = str(
        execution_summary.get("archive_target_path")
        or window_summary.get("archive_target_path")
        or execution_record_template.get("archive_artifact_path")
        or window_evidence_record_template.get("archive_artifact_path")
        or ""
    ).strip()
    archive_table_name = str(
        execution_summary.get("archive_table_name")
        or window_summary.get("archive_table_name")
        or execution_record_template.get("archive_table_name")
        or window_evidence_record_template.get("archive_table_name")
        or maintenance_defaults.get("archive_table_name")
        or DEFAULT_ARCHIVE_TABLE_NAME
    ).strip() or DEFAULT_ARCHIVE_TABLE_NAME

    checklist_status = str(execution_summary.get("checklist_status") or "").strip()
    readiness_status = str(readiness_summary.get("readiness_status") or "").strip()
    window_status = str(window_summary.get("window_status") or "").strip()
    package_status = str(
        readiness_summary.get("package_status")
        or execution_summary.get("package_status")
        or window_summary.get("package_status")
        or ""
    ).strip()
    ready_for_manual_window_execution = bool(
        readiness_summary.get("ready_for_manual_window_execution")
    )
    vacuum_decision = str(
        execution_record_template.get("vacuum_decision")
        or maintenance_defaults.get("vacuum_decision_default")
        or "pending"
    ).strip()

    template_status = "observe_only"
    message = "当前 NAV 归档执行后证据模板仅供维护窗口结束后人工补录，不提供页面 apply。"
    action_hint = (
        "页面不提供 apply；真实维护完成后，应按该模板统一回填备份、归档、删除、"
        "postcheck、体积测量与签字证据。"
    )
    if execution_checklist_payload:
        template_status = "evidence_template_ready"
        message = "当前 NAV 归档已形成执行后证据回填模板，可用于维护窗口结束后的统一补证。"
    if (
        checklist_status == "ready_for_manual_execution"
        and window_status == "ready_for_manual_window"
    ):
        template_status = "ready_for_post_maintenance_backfill"
        message = "当前 NAV 归档执行清单已闭合；真实维护完成后，应按该模板统一回填执行证据。"
    if ready_for_manual_window_execution and readiness_status == "ready_for_manual_window_execution":
        template_status = "ready_for_post_maintenance_backfill"
        message = "当前 NAV 归档维护准备度已通过；真实维护窗口执行完成后，应按该模板统一回填证据。"
    elif readiness_status == "blocked":
        template_status = "review_required"
        message = "当前维护准备度仍有阻断项，但执行后证据字段已先行固化，可提前准备补证材料。"

    required_evidence_items = _dedupe_preserve_order(
        [
            "维护窗口编号、执行开始/结束时间、执行人与复核人。",
            "主库备份路径、备份生成时间、备份大小、备份校验人和恢复责任人。",
            "归档库路径、归档表名、归档写入行数、抽样基金列表与抽样核验结论。",
            "删前主库冷数据行数、基金数、归档表计数与删除门禁核验结果。",
            "删后 remaining_cold_rows、remaining_cold_funds、postcheck SQL 输出与异常说明。",
            "主库大小前后、归档库大小、freelist_count 前后、WAL/checkpoint 结果。",
            "VACUUM 是否执行、执行时间、执行前后体积变化与延后原因。",
            "是否触发回滚、回滚完成时间、回滚责任人和补救说明。",
            "执行签字、复核签字、维护负责人签字与最终备注。",
        ]
        + list(maintenance_window_pack_payload.get("post_window_evidence") or [])
        + list(execution_checklist_payload.get("post_maintenance_measurements") or [])
    )
    evidence_capture_sequence = [
        "1. 回填窗口元信息、责任人、开始/结束时间和主库备份证据。",
        "2. 回填 archive_sql_preview 的写入结果，包括归档库路径、归档表名、归档行数和抽样基金。",
        "3. 回填 delete_sql_preview 与 postcheck_sql_preview 的结果，确认 remaining_cold_rows 与 remaining_cold_funds。",
        "4. 回填主库/归档库体积、freelist_count、WAL/checkpoint 结果和 VACUUM 决策。",
        "5. 若发生异常或回滚，回填触发原因、补救动作、回滚完成时间和最终签字。",
    ]
    exception_capture_requirements = [
        "若任一步骤失败，记录失败 SQL/命令、失败时间、影响范围和是否已停止后续步骤。",
        "若删除后发现 cutoff 之外数据受影响，立即记录异常范围并转入回滚预案。",
        "若维护窗口未执行 VACUUM，必须记录延后原因、责任人和后续窗口计划。",
        "若未发生异常，也需明确记录 rollback_required = false 和 exception_summary = none。",
    ]
    signoff_requirements = [
        "db_operator 确认执行事实、关键计数与命令输出。",
        "data_reviewer 确认抽样范围、归档计数、删后剩余冷数据与业务边界。",
        "maintenance_owner 确认维护窗口关闭、业务恢复和最终证据归档完整。",
        "rollback_owner 若发生回滚，需额外签字确认恢复完成与一致性复核结果。",
    ]
    evidence_sections = [
        {
            "section_key": "window_metadata",
            "label": "窗口元信息",
            "required_fields": [
                "window_id",
                "started_at",
                "completed_at",
                "maintenance_owner",
                "db_operator",
                "data_reviewer",
                "rollback_owner",
            ],
        },
        {
            "section_key": "backup_restore",
            "label": "备份与恢复前提",
            "required_fields": [
                "backup_artifact_path",
                "backup_artifact_size_mb",
                "backup_artifact_created_at",
                "backup_validation_owner",
            ],
        },
        {
            "section_key": "archive_insert",
            "label": "归档写入结果",
            "required_fields": [
                "archive_artifact_path",
                "archive_table_name",
                "archive_insert_row_count",
                "archive_row_count",
                "archive_insert_sample_fcodes",
            ],
        },
        {
            "section_key": "delete_postcheck",
            "label": "删除与删后核对",
            "required_fields": [
                "pre_delete_cold_row_count",
                "pre_delete_cold_fund_count",
                "post_delete_remaining_cold_rows",
                "post_delete_remaining_cold_funds",
                "postcheck_verified_by",
            ],
        },
        {
            "section_key": "storage_measurements",
            "label": "体积与空页测量",
            "required_fields": [
                "primary_db_size_before_mb",
                "primary_db_size_after_mb",
                "archive_db_size_mb",
                "freelist_count_before",
                "freelist_count_after",
                "wal_checkpoint_result",
                "vacuum_decision",
                "vacuum_executed",
            ],
        },
        {
            "section_key": "signoff",
            "label": "异常、回滚与签字",
            "required_fields": [
                "rollback_required",
                "rollback_completed",
                "exception_summary",
                "signoff.operator_signed_by",
                "signoff.reviewer_signed_by",
                "signoff.maintenance_owner_signed_by",
            ],
        },
    ]

    evidence_record_template = {
        "window_id": str(
            execution_record_template.get("window_id")
            or window_evidence_record_template.get("window_id")
            or ""
        ).strip(),
        "maintenance_owner": str(
            execution_record_template.get("maintenance_owner")
            or window_evidence_record_template.get("maintenance_owner")
            or operator_assignments.get("maintenance_owner")
            or ""
        ).strip(),
        "db_operator": str(
            execution_record_template.get("db_operator")
            or window_evidence_record_template.get("db_operator")
            or operator_assignments.get("db_operator")
            or ""
        ).strip(),
        "data_reviewer": str(
            execution_record_template.get("data_reviewer")
            or window_evidence_record_template.get("data_reviewer")
            or operator_assignments.get("data_reviewer")
            or ""
        ).strip(),
        "rollback_owner": str(
            execution_record_template.get("rollback_owner")
            or window_evidence_record_template.get("rollback_owner")
            or operator_assignments.get("rollback_owner")
            or ""
        ).strip(),
        "started_at": str(execution_record_template.get("started_at") or "").strip(),
        "archive_started_at": "",
        "archive_completed_at": str(execution_record_template.get("archive_completed_at") or "").strip(),
        "delete_started_at": "",
        "delete_completed_at": str(execution_record_template.get("delete_completed_at") or "").strip(),
        "completed_at": str(
            execution_record_template.get("completed_at")
            or window_evidence_record_template.get("completed_at")
            or ""
        ).strip(),
        "backup_artifact_path": str(execution_record_template.get("backup_artifact_path") or "").strip(),
        "backup_artifact_size_mb": 0.0,
        "backup_artifact_created_at": "",
        "backup_validation_owner": "",
        "archive_artifact_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "archive_insert_row_count": 0,
        "archive_row_count": _normalize_int(
            execution_record_template.get("archive_row_count")
            or window_evidence_record_template.get("archive_row_count")
        ),
        "archive_insert_sample_fcodes": [],
        "pre_delete_cold_row_count": _normalize_int(
            execution_record_template.get("pre_delete_cold_row_count")
            or window_evidence_record_template.get("precheck_row_count")
        ),
        "pre_delete_cold_fund_count": 0,
        "post_delete_remaining_cold_rows": _normalize_int(
            execution_record_template.get("post_delete_remaining_cold_rows")
            or window_evidence_record_template.get("remaining_cold_rows")
        ),
        "post_delete_remaining_cold_funds": 0,
        "postcheck_verified_by": "",
        "primary_db_size_before_mb": 0.0,
        "primary_db_size_after_mb": 0.0,
        "archive_db_size_mb": 0.0,
        "freelist_count_before": 0,
        "freelist_count_after": 0,
        "wal_checkpoint_result": "",
        "vacuum_decision": vacuum_decision or "pending",
        "vacuum_executed": bool(
            execution_record_template.get("vacuum_executed")
            or window_evidence_record_template.get("vacuum_executed")
        ),
        "vacuum_completed_at": "",
        "rollback_required": bool(execution_record_template.get("rollback_required")),
        "rollback_completed": bool(execution_record_template.get("rollback_completed")),
        "exception_summary": "",
        "evidence_paths": list(
            execution_record_template.get("evidence_paths")
            or window_evidence_record_template.get("evidence_paths")
            or []
        ),
        "notes": str(
            execution_record_template.get("notes")
            or window_evidence_record_template.get("notes")
            or ""
        ).strip(),
        "signoff": {
            "operator_signed_by": "",
            "operator_signed_at": "",
            "reviewer_signed_by": "",
            "reviewer_signed_at": "",
            "maintenance_owner_signed_by": "",
            "maintenance_owner_signed_at": "",
            "rollback_owner_signed_by": "",
            "rollback_owner_signed_at": "",
        },
    }

    findings = _dedupe_preserve_order(
        list(maintenance_readiness_audit_payload.get("findings") or [])[:2]
        + list(execution_checklist_payload.get("findings") or [])[:2]
        + [message]
    )

    summary = {
        "template_status": template_status,
        "readiness_status": readiness_status,
        "checklist_status": checklist_status,
        "window_status": window_status,
        "package_status": package_status,
        "ready_for_manual_window_execution": ready_for_manual_window_execution,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": _normalize_int(
            readiness_summary.get("estimated_archive_row_count")
            or execution_summary.get("estimated_archive_row_count")
            or window_summary.get("estimated_archive_row_count")
        ),
        "estimated_archive_fund_count": _normalize_int(
            readiness_summary.get("estimated_archive_fund_count")
            or execution_summary.get("estimated_archive_fund_count")
            or window_summary.get("estimated_archive_fund_count")
        ),
        "estimated_archive_size_mb": _normalize_float(
            readiness_summary.get("estimated_archive_size_mb")
            or execution_summary.get("estimated_archive_size_mb")
            or window_summary.get("estimated_archive_size_mb")
        ),
        "archive_target_path": archive_target_path,
        "archive_table_name": archive_table_name,
        "blocker_count": _normalize_int(readiness_summary.get("blocker_count")),
        "review_item_count": _normalize_int(readiness_summary.get("review_item_count")),
        "page_apply_supported": False,
        "execution_mode": str(
            readiness_summary.get("execution_mode")
            or execution_summary.get("execution_mode")
            or window_summary.get("execution_mode")
            or "manual_window_only"
        ).strip(),
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(execution_checklist_payload or maintenance_window_pack_payload),
        "message": message,
        "action_hint": action_hint,
        "source_nav_history_archive_execution_checklist_artifact": source_execution_checklist_artifact or "",
        "source_nav_history_archive_execution_checklist_generated_at": execution_checklist_payload.get("generated_at") or "",
        "source_nav_history_archive_maintenance_readiness_audit_artifact": (
            source_maintenance_readiness_audit_artifact or ""
        ),
        "source_nav_history_archive_maintenance_readiness_audit_generated_at": (
            maintenance_readiness_audit_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_maintenance_window_pack_artifact": (
            source_maintenance_window_pack_artifact or ""
        ),
        "source_nav_history_archive_maintenance_window_pack_generated_at": (
            maintenance_window_pack_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_execution_checklist_summary": execution_summary,
        "source_nav_history_archive_maintenance_readiness_audit_summary": readiness_summary,
        "source_nav_history_archive_maintenance_window_pack_summary": window_summary,
        "summary": summary,
        "required_evidence_items": required_evidence_items,
        "evidence_capture_sequence": evidence_capture_sequence,
        "exception_capture_requirements": exception_capture_requirements,
        "signoff_requirements": signoff_requirements,
        "evidence_sections": evidence_sections,
        "reference_execution_order": list(execution_checklist_payload.get("execution_order") or []),
        "reference_postcheck_sql_preview": list(
            maintenance_window_pack_payload.get("postcheck_sql_preview") or []
        ),
        "reference_rollback_triggers": list(
            execution_checklist_payload.get("rollback_triggers") or []
        ),
        "evidence_record_template": evidence_record_template,
        "findings": findings,
    }


def render_nav_history_archive_post_maintenance_evidence_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档执行后证据回填模板 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive post maintenance evidence",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        (
            "- source_execution_checklist_artifact: "
            f"{payload.get('source_nav_history_archive_execution_checklist_artifact', '')}"
        ),
        (
            "- source_maintenance_readiness_audit_artifact: "
            f"{payload.get('source_nav_history_archive_maintenance_readiness_audit_artifact', '')}"
        ),
        (
            "- source_maintenance_window_pack_artifact: "
            f"{payload.get('source_nav_history_archive_maintenance_window_pack_artifact', '')}"
        ),
        f"- template_status: {summary.get('template_status', '')}",
        f"- readiness_status: {summary.get('readiness_status', '')}",
        f"- checklist_status: {summary.get('checklist_status', '')}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        "",
        "## required_evidence_items",
    ]
    for item in payload.get("required_evidence_items") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## evidence_capture_sequence"])
    for item in payload.get("evidence_capture_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## exception_capture_requirements"])
    for item in payload.get("exception_capture_requirements") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## signoff_requirements"])
    for item in payload.get("signoff_requirements") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## evidence_sections"])
    for item in payload.get("evidence_sections") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## reference_execution_order"])
    for item in payload.get("reference_execution_order") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## reference_postcheck_sql_preview"])
    for item in payload.get("reference_postcheck_sql_preview") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## reference_rollback_triggers"])
    for item in payload.get("reference_rollback_triggers") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## evidence_record_template"])
    lines.append(json.dumps(payload.get("evidence_record_template") or {}, ensure_ascii=False, indent=2))

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_post_maintenance_evidence_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档执行后证据回填模板 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_post_maintenance_evidence_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_post_maintenance_evidence_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_post_maintenance_evidence_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
