#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选输出清理执行前置核对（只读）。
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
from utils.screening_output_cleanup_runbook import (
    load_latest_screening_output_cleanup_runbook_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCREENING_OUTPUT_CLEANUP_PREFLIGHT_JSON_PATTERN = "screening_output_cleanup_preflight_*.json"


def load_latest_screening_output_cleanup_preflight_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选输出 cleanup preflight。"""
    return load_latest_json_artifact(
        SCREENING_OUTPUT_CLEANUP_PREFLIGHT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _resolve_artifact_path(path_value: str) -> Path:
    path = Path(str(path_value or "").strip()).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def _normalize_relative_paths(items) -> list[str]:
    normalized = []
    seen = set()
    for item in items or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def build_screening_output_cleanup_preflight_payload(
    *,
    preview_payload: Optional[dict] = None,
    runbook_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    source_runbook_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """构建筛选输出 cleanup preflight。"""
    if preview_payload is None:
        if source_preview_artifact:
            preview_path = _resolve_artifact_path(source_preview_artifact)
            preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
            source_preview_artifact = str(preview_path)
        else:
            preview_path, preview_payload = load_latest_screening_output_cleanup_preview_payload(output_dir=output_dir)
            source_preview_artifact = str(preview_path)

    if runbook_payload is None:
        if source_runbook_artifact:
            runbook_path = _resolve_artifact_path(source_runbook_artifact)
            runbook_payload = json.loads(runbook_path.read_text(encoding="utf-8"))
            source_runbook_artifact = str(runbook_path)
        else:
            runbook_path, runbook_payload = load_latest_screening_output_cleanup_runbook_payload(output_dir=output_dir)
            source_runbook_artifact = str(runbook_path)

    preview_payload = dict(preview_payload or {})
    runbook_payload = dict(runbook_payload or {})
    preview_summary = dict(preview_payload.get("summary") or {})
    preview_parameters = dict(preview_payload.get("parameters") or {})
    preview_execution_plan = dict(preview_payload.get("execution_plan") or {})
    runbook_summary = dict(runbook_payload.get("summary") or {})

    target_output_dir = Path(
        str(
            preview_execution_plan.get("target_dir")
            or preview_payload.get("output_dir")
            or runbook_payload.get("target_output_dir")
            or ""
        ).strip()
    ).expanduser()
    if not target_output_dir.is_absolute():
        target_output_dir = (PROJECT_ROOT / target_output_dir).resolve()
    else:
        target_output_dir = target_output_dir.resolve()

    preview_scope_files = _normalize_relative_paths(preview_execution_plan.get("scope_files") or [])
    runbook_scope_files = _normalize_relative_paths(runbook_payload.get("sample_scope_files") or [])

    existing_scope_files = []
    missing_scope_files = []
    escaped_scope_files = []
    for relative_path in preview_scope_files:
        resolved_path = (target_output_dir / relative_path).resolve()
        try:
            resolved_path.relative_to(target_output_dir)
        except ValueError:
            escaped_scope_files.append(relative_path)
            continue
        if resolved_path.exists() and resolved_path.is_file():
            existing_scope_files.append(relative_path)
        else:
            missing_scope_files.append(relative_path)

    checks = {
        "runbook_references_preview_artifact": bool(source_preview_artifact) and (
            str(runbook_payload.get("source_screening_output_cleanup_preview_artifact") or "").strip()
            == str(source_preview_artifact).strip()
        ),
        "runbook_references_preview_generated_at": (
            str(runbook_payload.get("source_screening_output_cleanup_preview_generated_at") or "").strip()
            == str(preview_payload.get("generated_at") or "").strip()
        ),
        "runbook_scope_subset_of_preview_scope": set(runbook_scope_files).issubset(set(preview_scope_files)),
        "target_output_dir_exists": target_output_dir.exists() and target_output_dir.is_dir(),
        "scope_files_within_target_dir": len(escaped_scope_files) == 0,
        "scope_files_still_exist": len(existing_scope_files) == len(preview_scope_files),
        "candidate_count_matches_scope_total": (
            int(preview_summary.get("candidate_file_count") or 0) == len(preview_scope_files)
            and int(runbook_summary.get("scope_file_total") or 0) == len(preview_scope_files)
        ),
    }
    checks["ready_for_manual_signoff"] = all(checks.values())

    findings = []
    if checks["ready_for_manual_signoff"]:
        findings.append("当前输出目录、preview 与 runbook 链路一致，可进入人工签字。")
    else:
        findings.append("当前筛选输出链路未完全对齐，进入真实删除前应先重新生成或复核。")
    if missing_scope_files:
        findings.append(f"preview 中有 {len(missing_scope_files)} 个 scope 文件已不存在，需先确认是否已被人工处理。")
    if escaped_scope_files:
        findings.append(f"检测到 {len(escaped_scope_files)} 个 scope 文件越出目标目录，当前链路应视为阻断。")
    if not checks["runbook_scope_subset_of_preview_scope"]:
        findings.append("runbook 中的样本文件超出了 preview scope，链路不一致。")

    return {
        "generated_at": datetime.now().isoformat(),
        "target_output_dir": str(target_output_dir),
        "source_screening_output_cleanup_preview_artifact": source_preview_artifact or "",
        "source_screening_output_cleanup_runbook_artifact": source_runbook_artifact or "",
        "source_screening_output_cleanup_preview_generated_at": preview_payload.get("generated_at") or "",
        "source_screening_output_cleanup_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "summary": {
            "candidate_file_count": int(preview_summary.get("candidate_file_count") or 0),
            "candidate_family_count": int(preview_summary.get("candidate_family_count") or 0),
            "candidate_size_mb": float(preview_summary.get("candidate_size_mb") or 0.0),
            "retention_days": int(preview_parameters.get("retention_days") or 0),
            "keep_latest_per_family": bool(preview_parameters.get("keep_latest_per_family", True)),
            "scope_file_total": len(preview_scope_files),
            "existing_scope_file_count": len(existing_scope_files),
            "missing_scope_file_count": len(missing_scope_files),
            "escaped_scope_file_count": len(escaped_scope_files),
            "ready_for_manual_signoff": checks["ready_for_manual_signoff"],
        },
        "checks": checks,
        "drift": {
            "missing_scope_files_sample": missing_scope_files[:20],
            "escaped_scope_files_sample": escaped_scope_files[:20],
        },
        "findings": findings,
    }


def render_screening_output_cleanup_preflight_markdown(payload: dict) -> str:
    """渲染筛选输出 cleanup preflight Markdown。"""
    summary = payload.get("summary") or {}
    checks = payload.get("checks") or {}
    drift = payload.get("drift") or {}

    lines = [
        "# screening output cleanup preflight",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- target_output_dir: {payload.get('target_output_dir')}",
        f"- source_preview_artifact: {payload.get('source_screening_output_cleanup_preview_artifact')}",
        f"- source_runbook_artifact: {payload.get('source_screening_output_cleanup_runbook_artifact')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff')}",
        f"- candidate_file_count: {summary.get('candidate_file_count', 0)}",
        f"- scope_file_total: {summary.get('scope_file_total', 0)}",
        f"- existing_scope_file_count: {summary.get('existing_scope_file_count', 0)}",
        f"- missing_scope_file_count: {summary.get('missing_scope_file_count', 0)}",
        "",
        "## checks",
    ]
    for key, value in checks.items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## drift"])
    lines.append(f"- missing_scope_files_sample: {json.dumps(drift.get('missing_scope_files_sample') or [], ensure_ascii=False)}")
    lines.append(f"- escaped_scope_files_sample: {json.dumps(drift.get('escaped_scope_files_sample') or [], ensure_ascii=False)}")
    return "\n".join(lines).strip() + "\n"


def write_screening_output_cleanup_preflight_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出筛选输出 cleanup preflight JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    if not out_dir.is_absolute():
        out_dir = (PROJECT_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"screening_output_cleanup_preflight_{ts}.json"
    md_path = out_dir / f"screening_output_cleanup_preflight_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_output_cleanup_preflight_markdown(payload), encoding="utf-8")
    return json_path, md_path
