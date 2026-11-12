#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库备份 manual review 控制包（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent

from utils.database_backup_cleanup_preview import (
    load_latest_database_backup_cleanup_preview_payload,
)
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


DATABASE_BACKUP_MANUAL_REVIEW_CONTROL_PACK_JSON_PATTERN = "database_backup_manual_review_control_pack_*.json"


def load_latest_database_backup_manual_review_control_pack_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新数据库备份 manual review 控制包。"""
    return load_latest_json_artifact(
        DATABASE_BACKUP_MANUAL_REVIEW_CONTROL_PACK_JSON_PATTERN,
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


def _build_file_review_records(
    *,
    manual_review_files: list[str],
    candidate_file_samples: list[dict],
) -> list[dict]:
    sample_by_path = {
        str(item.get("relative_path") or "").strip(): dict(item)
        for item in (candidate_file_samples or [])
        if str(item.get("relative_path") or "").strip()
    }
    records = []
    for relative_path in manual_review_files:
        sample = sample_by_path.get(relative_path, {})
        records.append(
            {
                "relative_path": relative_path,
                "backup_label": str(sample.get("backup_label") or "").strip(),
                "family_label": str(sample.get("family_label") or "").strip(),
                "timestamp": str(sample.get("timestamp") or sample.get("modified_at") or "").strip(),
                "size_mb": _normalize_float(sample.get("size_mb")),
                "manual_review_reason": str(sample.get("manual_review_reason") or "").strip(),
                "snapshot_to_active_ratio": _normalize_float(sample.get("snapshot_to_active_ratio")),
                "recommended_disposition": "manual_review_required",
                "decision": "",
                "signed_off": False,
                "notes": "",
            }
        )
    return records


def build_database_backup_manual_review_control_pack_payload(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于最新备份 preview 生成 manual review 控制包。"""
    if preview_payload is None:
        preview_path, preview_payload = load_latest_database_backup_cleanup_preview_payload(output_dir=output_dir)
        source_preview_artifact = str(preview_path)

    preview_payload = dict(preview_payload or {})
    preview_summary = _normalize_dict(preview_payload.get("summary"))
    execution_plan = _normalize_dict(preview_payload.get("execution_plan"))
    candidate_groups = [
        dict(item)
        for item in (preview_payload.get("candidate_groups") or [])
        if isinstance(item, dict) and str(item.get("disposition") or "").strip() == "manual_review_candidate"
    ]
    candidate_file_samples = [
        dict(item)
        for item in (preview_payload.get("candidate_file_samples") or [])
        if isinstance(item, dict)
    ]
    manual_review_files = [
        str(item or "").strip()
        for item in (execution_plan.get("manual_review_files") or [])
        if str(item or "").strip()
    ]

    manual_review_candidate_file_count = _normalize_int(preview_summary.get("manual_review_candidate_file_count"))
    manual_review_candidate_size_mb = _normalize_float(preview_summary.get("manual_review_candidate_size_mb"))
    oversized_manual_snapshot_review_file_count = _normalize_int(
        preview_summary.get("oversized_manual_snapshot_review_file_count")
    )
    oversized_manual_snapshot_review_size_mb = _normalize_float(
        preview_summary.get("oversized_manual_snapshot_review_size_mb")
    )
    active_database_size_mb = _normalize_float(preview_summary.get("active_database_size_mb"))

    ready_for_manual_signoff = manual_review_candidate_file_count > 0
    package_status = "ready_for_manual_review" if ready_for_manual_signoff else "observe_only"
    message = (
        "当前数据库备份 manual review 候选已收敛为单一签字包，应在维护窗口内人工确认是否删除。"
        if ready_for_manual_signoff
        else "当前没有需要人工复核的数据库备份文件，manual review 控制包仅保留为只读记录。"
    )

    findings = [
        str(item or "").strip()
        for item in (preview_payload.get("findings") or [])
        if str(item or "").strip()
    ]
    if not findings:
        findings = [message]

    operator_roles = [
        {
            "role": "maintenance_owner",
            "required": True,
            "assignee": "",
            "responsibility": "确认回滚窗口已结束、删除范围准确，签发最终 manual review 结论。",
        },
        {
            "role": "backup_reviewer",
            "required": True,
            "assignee": "",
            "responsibility": "逐个复核 maintenance snapshot 的来源、体积、保留理由和删除风险。",
        },
        {
            "role": "rollback_owner",
            "required": True,
            "assignee": "",
            "responsibility": "确认恢复点是否仍需保留，并在异常时接管回滚方案。",
        },
    ]
    signoff_record_template = {
        "rollback_window_closed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
        "delete_scope_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
        "latest_snapshot_need_confirmed": {"signed": False, "reviewer": "", "signed_at": "", "notes": ""},
    }
    execution_window_requirements = [
        "页面 apply 仅处理自动候选，manual review 文件不得从页面自动删除。",
        "必须先确认维护窗口已结束，且当前不再依赖这些快照作为恢复点。",
        "删除前应再次核对主库当前大小、快照体积和快照来源时间。",
        "若任一文件的保留理由不清晰，应停止删除并转入继续保留。",
        "本控制包只用于人工签字，不直接执行任何删除。",
    ]
    manual_checklist = [
        "确认 source preview 为最新产物，且 manual_review_files 清单与当前备份目录一致。",
        "确认回滚窗口已结束，且不存在依赖快照恢复的未完成维护。",
        "确认每个 maintenance snapshot 的来源、用途和保留理由。",
        "确认若删除后仍保留足够的回滚点与月备份。",
    ]
    manual_steps = [
        "逐个核对 manual_review_files 的 family、体积、时间戳和 manual_review_reason。",
        "在 signoff_record_template 中记录每个关键签字项是否通过。",
        "仅在维护窗口内人工删除已签字确认的 manual review 文件。",
        "删除后补留 execution checkpoint 或人工处置记录，保留审计证据。",
    ]
    stop_conditions = [
        "存在仍需保留的恢复点或回滚窗口尚未结束。",
        "manual_review_files 与 preview 清单不一致。",
        "任一文件的来源、用途或删除理由无法解释。",
        "删除后可能导致当前维护无法回滚。",
    ]
    file_review_records = _build_file_review_records(
        manual_review_files=manual_review_files,
        candidate_file_samples=candidate_file_samples,
    )

    summary = {
        "package_status": package_status,
        "ready_for_manual_signoff": ready_for_manual_signoff,
        "target_backup_dir": str(
            execution_plan.get("target_dir")
            or preview_payload.get("backup_dir")
            or ""
        ).strip(),
        "active_database_path": str(preview_summary.get("active_database_path") or "").strip(),
        "active_database_size_mb": active_database_size_mb,
        "manual_review_candidate_file_count": manual_review_candidate_file_count,
        "manual_review_candidate_size_mb": manual_review_candidate_size_mb,
        "oversized_manual_snapshot_review_file_count": oversized_manual_snapshot_review_file_count,
        "oversized_manual_snapshot_review_size_mb": oversized_manual_snapshot_review_size_mb,
        "manual_snapshot_count": _normalize_int(preview_summary.get("manual_snapshot_count")),
        "manual_snapshot_size_mb": _normalize_float(preview_summary.get("manual_snapshot_size_mb")),
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "message": message,
        "source_database_backup_cleanup_preview_artifact": source_preview_artifact or "",
        "source_preview_generated_at": preview_payload.get("generated_at") or "",
        "source_database_backup_cleanup_preview_summary": preview_summary,
        "summary": summary,
        "operator_roles": operator_roles,
        "signoff_record_template": signoff_record_template,
        "execution_window_requirements": execution_window_requirements,
        "manual_checklist": manual_checklist,
        "manual_steps": manual_steps,
        "stop_conditions": stop_conditions,
        "manual_review_files": manual_review_files,
        "file_review_records": file_review_records,
        "candidate_groups": candidate_groups[:5],
        "findings": findings,
    }


def render_database_backup_manual_review_control_pack_markdown(payload: dict) -> str:
    """渲染数据库备份 manual review 控制包 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# database backup manual review control pack",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- message: {payload.get('message', '')}",
        f"- source_preview_artifact: {payload.get('source_database_backup_cleanup_preview_artifact', '')}",
        f"- package_status: {summary.get('package_status', '')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff', False)}",
        f"- manual_review_candidate_file_count: {summary.get('manual_review_candidate_file_count', 0)}",
        f"- manual_review_candidate_size_mb: {summary.get('manual_review_candidate_size_mb', 0.0)}",
        f"- oversized_manual_snapshot_review_file_count: {summary.get('oversized_manual_snapshot_review_file_count', 0)}",
        "",
        "## manual_review_files",
    ]
    for item in payload.get("manual_review_files") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## manual_steps"])
    for item in payload.get("manual_steps") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_database_backup_manual_review_control_pack_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出数据库备份 manual review 控制包 JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"database_backup_manual_review_control_pack_{ts}.json"
    md_path = artifact_dir / f"database_backup_manual_review_control_pack_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_database_backup_manual_review_control_pack_markdown(payload), encoding="utf-8")
    return json_path, md_path
