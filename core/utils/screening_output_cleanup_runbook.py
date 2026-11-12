#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选输出清理执行手册（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_output_cleanup_preview import (
    load_latest_screening_output_cleanup_preview_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCREENING_OUTPUT_CLEANUP_RUNBOOK_JSON_PATTERN = "screening_output_cleanup_runbook_*.json"


def load_latest_screening_output_cleanup_runbook_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选输出 cleanup runbook。"""
    return load_latest_json_artifact(
        SCREENING_OUTPUT_CLEANUP_RUNBOOK_JSON_PATTERN,
        output_dir=output_dir,
    )


def _resolve_artifact_path(path_value: str) -> Path:
    path = Path(str(path_value or "").strip()).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def build_screening_output_cleanup_runbook_payload(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """基于最新 preview 构建筛选输出清理执行手册。"""
    if preview_payload is None:
        if source_preview_artifact:
            preview_path = _resolve_artifact_path(source_preview_artifact)
            preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
            source_preview_artifact = str(preview_path)
        else:
            preview_path, preview_payload = load_latest_screening_output_cleanup_preview_payload(output_dir=output_dir)
            source_preview_artifact = str(preview_path)

    payload = dict(preview_payload or {})
    preview_summary = dict(payload.get("summary") or {})
    preview_parameters = dict(payload.get("parameters") or {})
    execution_plan = dict(payload.get("execution_plan") or {})
    findings = list(payload.get("findings") or [])

    scope_files = [
        str(item).strip()
        for item in (execution_plan.get("scope_files") or [])
        if str(item or "").strip()
    ]
    target_dir = str(execution_plan.get("target_dir") or payload.get("output_dir") or "").strip()
    candidate_groups = list(payload.get("candidate_groups") or [])
    top_groups = []
    for item in candidate_groups[:5]:
        top_groups.append({
            "family_key": str(item.get("family_key") or "").strip(),
            "label": str(item.get("label") or item.get("family_key") or "未分类").strip() or "未分类",
            "candidate_file_count": int(item.get("candidate_file_count") or 0),
            "candidate_size_mb": float(item.get("candidate_size_mb") or 0.0),
            "latest_protected_path": str(item.get("latest_protected_path") or "").strip(),
        })

    action_hint = (
        "进入真实删除前，先确认最新 preview 与当前输出目录一致，并留存 runbook / preflight 作为删前签字材料。"
        if scope_files
        else "当前没有待清理候选，继续保留 runbook 作为只读记录即可。"
    )
    message = (
        findings[0]
        if findings
        else f"当前候选文件 {len(scope_files)} 个，可按 runbook 中的目标目录与 guardrails 做人工复核。"
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "status": "ready_for_manual_review" if scope_files else "no_candidates",
        "message": message,
        "action_hint": action_hint,
        "source_screening_output_cleanup_preview_artifact": source_preview_artifact or "",
        "source_screening_output_cleanup_preview_generated_at": payload.get("generated_at") or "",
        "source_screening_output_cleanup_preview_summary": preview_summary,
        "target_output_dir": target_dir,
        "summary": {
            "candidate_file_count": int(preview_summary.get("candidate_file_count") or 0),
            "candidate_family_count": int(preview_summary.get("candidate_family_count") or 0),
            "candidate_size_mb": float(preview_summary.get("candidate_size_mb") or 0.0),
            "retention_days": int(preview_parameters.get("retention_days") or 0),
            "keep_latest_per_family": bool(preview_parameters.get("keep_latest_per_family", True)),
            "scope_file_total": len(scope_files),
            "top_candidate_group_count": len(top_groups),
        },
        "findings": findings,
        "manual_checklist": [
            "确认 source preview 为当前最新产物，且 artifact_path / generated_at 与页面展示一致。",
            "确认 target_output_dir 指向当前运行中的筛选输出目录，而不是历史副本或错误挂载路径。",
            "确认 scope_files 仅包含超过保留窗口的时间戳历史文件，且未包含当前工作簿或稳定输出。",
            "确认 keep_latest_per_family 保护策略与本轮维护预期一致。",
            "确认执行后仍保留每个产物家族至少一个最新文件，满足回溯和对比需求。",
        ],
        "execution_sequence": [
            "1. 先阅读 manual_checklist，确认 preview 与当前目录一致。",
            "2. 在维护窗口内重新抽样核对 top_candidate_groups 与 sample_scope_files。",
            "3. 仅在 preflight ready_for_manual_signoff 后，才生成 execution checkpoint 或执行 apply。",
            "4. 执行后回到概览页复核 checkpoint 与剩余候选是否符合预期。",
        ],
        "guardrails": list(execution_plan.get("guardrails") or []),
        "top_candidate_groups": top_groups,
        "sample_scope_files": scope_files[:20],
    }


def render_screening_output_cleanup_runbook_markdown(payload: dict) -> str:
    """渲染筛选输出 cleanup runbook Markdown。"""
    summary = payload.get("summary") or {}
    lines = [
        "# screening output cleanup runbook",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {payload.get('status')}",
        f"- source_preview_artifact: {payload.get('source_screening_output_cleanup_preview_artifact')}",
        f"- source_preview_generated_at: {payload.get('source_screening_output_cleanup_preview_generated_at')}",
        f"- target_output_dir: {payload.get('target_output_dir')}",
        f"- candidate_file_count: {summary.get('candidate_file_count', 0)}",
        f"- candidate_family_count: {summary.get('candidate_family_count', 0)}",
        f"- candidate_size_mb: {summary.get('candidate_size_mb', 0)}",
        f"- retention_days: {summary.get('retention_days', 0)}",
        f"- keep_latest_per_family: {summary.get('keep_latest_per_family')}",
        "",
        "## manual_checklist",
    ]
    for item in payload.get("manual_checklist") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_sequence"])
    for item in payload.get("execution_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## top_candidate_groups"])
    for item in payload.get("top_candidate_groups") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False, sort_keys=True)}")

    lines.extend(["", "## sample_scope_files"])
    for item in payload.get("sample_scope_files") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_screening_output_cleanup_runbook_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出筛选输出 cleanup runbook JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"screening_output_cleanup_runbook_{ts}.json"
    md_path = artifact_dir / f"screening_output_cleanup_runbook_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_output_cleanup_runbook_markdown(payload), encoding="utf-8")
    return json_path, md_path
