#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库备份 manual review 执行手册与处置记录模板（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent

from utils.database_backup_manual_review_control_pack import (
    load_latest_database_backup_manual_review_control_pack_payload,
)
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


DATABASE_BACKUP_MANUAL_REVIEW_RUNBOOK_JSON_PATTERN = "database_backup_manual_review_runbook_*.json"


def load_latest_database_backup_manual_review_runbook_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新数据库备份 manual review runbook。"""
    return load_latest_json_artifact(
        DATABASE_BACKUP_MANUAL_REVIEW_RUNBOOK_JSON_PATTERN,
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


def build_database_backup_manual_review_runbook_payload(
    *,
    control_pack_payload: Optional[dict] = None,
    source_control_pack_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于最新 manual review control pack 生成只读 runbook。"""
    if control_pack_payload is None:
        control_pack_path, control_pack_payload = load_latest_database_backup_manual_review_control_pack_payload(
            output_dir=output_dir,
        )
        source_control_pack_artifact = str(control_pack_path)

    payload = dict(control_pack_payload or {})
    summary = _normalize_dict(payload.get("summary"))
    file_review_records = [
        dict(item)
        for item in (payload.get("file_review_records") or [])
        if isinstance(item, dict)
    ]
    manual_review_files = [
        str(item or "").strip()
        for item in (payload.get("manual_review_files") or [])
        if str(item or "").strip()
    ]

    package_status = str(summary.get("package_status") or "").strip()
    ready_for_manual_signoff = bool(summary.get("ready_for_manual_signoff"))
    manual_review_candidate_file_count = _normalize_int(summary.get("manual_review_candidate_file_count"))
    manual_review_candidate_size_mb = _normalize_float(summary.get("manual_review_candidate_size_mb"))
    oversized_manual_snapshot_review_file_count = _normalize_int(
        summary.get("oversized_manual_snapshot_review_file_count")
    )
    oversized_manual_snapshot_review_size_mb = _normalize_float(
        summary.get("oversized_manual_snapshot_review_size_mb")
    )
    active_database_size_mb = _normalize_float(summary.get("active_database_size_mb"))
    active_database_path = str(summary.get("active_database_path") or "").strip()
    target_backup_dir = str(summary.get("target_backup_dir") or "").strip()

    runbook_status = "observe_only"
    message = "当前数据库备份 manual review runbook 仅作只读记录。"
    action_hint = "继续保留为只读处置模板即可；如后续再次出现 manual review 文件，再重新签字。"
    if ready_for_manual_signoff and package_status == "ready_for_manual_review":
        runbook_status = "ready_for_manual_review_window"
        message = (
            "当前 manual review 控制包已收敛为人工处置手册，可在维护窗口内完成逐项签字和处置记录。"
        )
        action_hint = "页面不提供删除入口；应以此 runbook 作为人工删前核对、签字记录和删后补证模板。"

    pre_delete_checks = [
        "确认 source control pack 为最新产物，且 manual_review_files 清单与当前备份目录完全一致。",
        "确认维护窗口已结束，不再依赖这些 maintenance snapshot 作为恢复点。",
        "确认当前 active database 体积、manual review 文件体积和 snapshot_to_active_ratio 仍与 control pack 一致。",
        "确认保留链路中仍存在足够的回滚点、计划备份和最近可恢复快照。",
        "若任一快照的用途、来源或删除后影响无法解释，则停止删除并保留该文件。",
    ]
    execution_sequence = [
        "1. 先在维护窗口核对 control pack、当前备份目录和主库大小，不一致则中止。",
        "2. 逐个填写文件处置记录，明确保留/删除结论、责任人、签字时间和备注。",
        "3. 仅对已完成双人复核并签字通过的文件执行人工删除；页面不参与执行。",
        "4. 删除后立即补录删后核对结果，包括目录复查、剩余回滚点和补证路径。",
        "5. 将最终签字 runbook 与 control pack 一并留存在 audit 中，作为维护窗口证据。",
    ]
    post_disposition_evidence = [
        "补留删除前后备份目录清单或截图，确认目标文件已移除且未误删其他快照。",
        "补留维护窗口记录，说明为何删除、谁批准、谁执行、谁复核。",
        "补留当前主库与剩余备份体积摘要，证明删除后仍保留足够恢复点。",
        "若最终决定继续保留，也需记录保留理由、下次复核时间和责任人。",
    ]
    stop_conditions = [
        "control pack 与当前目录清单不一致。",
        "回滚窗口尚未关闭，或仍有未完成维护依赖该快照。",
        "无法确认某个 snapshot 的用途、来源或恢复价值。",
        "删除后将导致最近维护缺少安全回滚点。",
    ]
    disposition_record_template = {
        "window_id": "",
        "maintenance_owner": "",
        "backup_reviewer": "",
        "rollback_owner": "",
        "decision": "pending",
        "decision_reason": "",
        "approved_at": "",
        "executed_at": "",
        "evidence_paths": [],
        "notes": "",
    }
    file_disposition_records = []
    for item in file_review_records:
        file_disposition_records.append(
            {
                "relative_path": str(item.get("relative_path") or "").strip(),
                "backup_label": str(item.get("backup_label") or "").strip(),
                "family_label": str(item.get("family_label") or "").strip(),
                "timestamp": str(item.get("timestamp") or "").strip(),
                "size_mb": _normalize_float(item.get("size_mb")),
                "manual_review_reason": str(item.get("manual_review_reason") or "").strip(),
                "snapshot_to_active_ratio": _normalize_float(item.get("snapshot_to_active_ratio")),
                "recommended_disposition": str(item.get("recommended_disposition") or "").strip(),
                "final_disposition": "",
                "approved_by": [],
                "approved_at": "",
                "executed_by": "",
                "executed_at": "",
                "postcheck_status": "",
                "evidence_paths": [],
                "notes": "",
            }
        )

    summary_payload = {
        "runbook_status": runbook_status,
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "target_backup_dir": target_backup_dir,
        "active_database_path": active_database_path,
        "active_database_size_mb": active_database_size_mb,
        "manual_review_candidate_file_count": manual_review_candidate_file_count,
        "manual_review_candidate_size_mb": manual_review_candidate_size_mb,
        "oversized_manual_snapshot_review_file_count": oversized_manual_snapshot_review_file_count,
        "oversized_manual_snapshot_review_size_mb": oversized_manual_snapshot_review_size_mb,
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "message": message,
        "action_hint": action_hint,
        "source_database_backup_manual_review_control_pack_artifact": source_control_pack_artifact or "",
        "source_control_pack_generated_at": payload.get("generated_at") or "",
        "source_database_backup_manual_review_control_pack_summary": summary,
        "summary": summary_payload,
        "manual_review_files": manual_review_files,
        "file_disposition_records": file_disposition_records,
        "pre_delete_checks": pre_delete_checks,
        "execution_sequence": execution_sequence,
        "post_disposition_evidence": post_disposition_evidence,
        "stop_conditions": stop_conditions,
        "disposition_record_template": disposition_record_template,
        "findings": [
            str(item or "").strip()
            for item in (payload.get("findings") or [])
            if str(item or "").strip()
        ] or [message],
    }


def render_database_backup_manual_review_runbook_markdown(payload: dict) -> str:
    """渲染数据库备份 manual review runbook Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# database backup manual review runbook",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- action_hint: {payload.get('action_hint', '')}",
        (
            "- source_control_pack_artifact: "
            f"{payload.get('source_database_backup_manual_review_control_pack_artifact', '')}"
        ),
        f"- runbook_status: {summary.get('runbook_status', '')}",
        f"- package_status: {summary.get('package_status', '')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff', False)}",
        f"- manual_review_candidate_file_count: {summary.get('manual_review_candidate_file_count', 0)}",
        f"- manual_review_candidate_size_mb: {summary.get('manual_review_candidate_size_mb', 0.0)}",
        "",
        "## pre_delete_checks",
    ]
    for item in payload.get("pre_delete_checks") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_sequence"])
    for item in payload.get("execution_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## post_disposition_evidence"])
    for item in payload.get("post_disposition_evidence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## stop_conditions"])
    for item in payload.get("stop_conditions") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## file_disposition_records"])
    for item in payload.get("file_disposition_records") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    return "\n".join(lines).strip() + "\n"


def write_database_backup_manual_review_runbook_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出数据库备份 manual review runbook JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"database_backup_manual_review_runbook_{ts}.json"
    md_path = artifact_dir / f"database_backup_manual_review_runbook_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_database_backup_manual_review_runbook_markdown(payload), encoding="utf-8")
    return json_path, md_path
