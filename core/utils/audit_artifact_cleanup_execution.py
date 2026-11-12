#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit 目录历史产物清理执行器。

默认 dry-run；只有显式 apply 且满足确认串、预演产物、路径边界校验时才会执行。
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.audit_artifact_cleanup_preview import (
    load_latest_audit_artifact_cleanup_preview_payload,
)
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent
AUDIT_ARTIFACT_CLEANUP_EXECUTION_JSON_PATTERN = "audit_artifact_cleanup_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_AUDIT_ARTIFACT_CLEANUP"
DEFAULT_CHECKPOINT_EVERY = 20


def load_latest_audit_artifact_cleanup_execution_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 audit artifact cleanup 执行 checkpoint。"""
    return load_latest_json_artifact(
        AUDIT_ARTIFACT_CLEANUP_EXECUTION_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_mode(mode: str) -> str:
    value = str(mode or "dry-run").strip().lower()
    if value not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    return value


def _resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def _resolve_output_dir(output_dir: str) -> Path:
    artifact_dir = _resolve_project_path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _resolve_output_paths(output_dir: str, timestamp: Optional[str] = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    return (
        artifact_dir / f"audit_artifact_cleanup_execution_{ts}.json",
        artifact_dir / f"audit_artifact_cleanup_execution_{ts}.md",
    )


def _load_preview_payload(
    *,
    preview_payload: Optional[dict],
    source_preview_artifact: Optional[str],
    audit_output_dir: str,
) -> tuple[str, dict]:
    if preview_payload is not None:
        return str(source_preview_artifact or "").strip(), dict(preview_payload or {})

    if source_preview_artifact:
        preview_path = _resolve_project_path(source_preview_artifact)
        payload = json.loads(preview_path.read_text(encoding="utf-8"))
        return str(preview_path), dict(payload or {})

    preview_path, payload = load_latest_audit_artifact_cleanup_preview_payload(output_dir=audit_output_dir)
    return str(preview_path), dict(payload or {})


def _normalize_scope_entries(scope_entries) -> list[dict]:
    normalized = []
    seen = set()
    for item in scope_entries or []:
        row = dict(item or {})
        relative_path = str(row.get("relative_path") or "").strip()
        entry_kind = str(row.get("entry_kind") or "").strip().lower()
        if not relative_path or entry_kind not in {"file", "directory"}:
            continue
        key = (relative_path, entry_kind)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "relative_path": relative_path,
                "entry_kind": entry_kind,
            }
        )
    return normalized


def _normalize_checkpoint_every(value) -> int:
    try:
        parsed = int(value if value is not None else DEFAULT_CHECKPOINT_EVERY)
    except (TypeError, ValueError):
        parsed = DEFAULT_CHECKPOINT_EVERY
    return max(1, parsed)


def _resolve_target_dir(preview_payload: dict, expected_target_dir: Optional[str]) -> Path:
    execution_plan = dict(preview_payload.get("execution_plan") or {})
    preview_target_dir = str(
        execution_plan.get("target_dir")
        or preview_payload.get("target_dir")
        or ""
    ).strip()
    if not preview_target_dir:
        raise ValueError("preview payload missing target audit directory")

    resolved_preview_dir = _resolve_project_path(preview_target_dir)
    if expected_target_dir:
        resolved_expected_dir = _resolve_project_path(expected_target_dir)
        if resolved_expected_dir != resolved_preview_dir:
            raise ValueError("expected_target_dir does not match preview target_dir")

    return resolved_preview_dir


def _build_scope_entry_records(
    scope_entries: list[dict],
    target_dir: Path,
) -> tuple[list[dict], dict[tuple[str, str], Path]]:
    records = []
    resolved_paths = {}

    for item in scope_entries:
        relative_path = str(item.get("relative_path") or "")
        entry_kind = str(item.get("entry_kind") or "")
        resolved_path = (target_dir / relative_path).resolve()
        try:
            resolved_path.relative_to(target_dir)
        except ValueError as exc:
            raise ValueError(f"scope entry escapes target audit directory: {relative_path}") from exc

        size_bytes = 0
        if resolved_path.exists():
            if resolved_path.is_file():
                size_bytes = int(resolved_path.stat().st_size)
            elif resolved_path.is_dir():
                size_bytes = sum(child.stat().st_size for child in resolved_path.rglob("*") if child.is_file())

        record = {
            "relative_path": relative_path,
            "entry_kind": entry_kind,
            "absolute_path": str(resolved_path),
            "status": "planned",
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 4),
            "started_at": None,
            "finished_at": None,
            "error": "",
        }
        records.append(record)
        resolved_paths[(relative_path, entry_kind)] = resolved_path

    return records, resolved_paths


def _validate_apply_prerequisites(
    *,
    confirm_apply: Optional[str],
    scope_entries: list[dict],
) -> None:
    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")
    if not scope_entries:
        raise ValueError("apply requires at least one scope entry")


def render_audit_artifact_cleanup_execution_markdown(payload: dict) -> str:
    """渲染执行 checkpoint Markdown。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    lines = [
        "# audit artifact cleanup execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- source_preview_artifact: {payload.get('source_audit_artifact_cleanup_preview_artifact')}",
        f"- target_dir: {parameters.get('target_dir')}",
        f"- expected_target_dir: {parameters.get('expected_target_dir')}",
        f"- checkpoint_every: {parameters.get('checkpoint_every')}",
        f"- scope_entry_total: {summary.get('scope_entry_total', 0)}",
        f"- deleted_entry_count: {summary.get('deleted_entry_count', 0)}",
        f"- missing_entry_count: {summary.get('missing_entry_count', 0)}",
        f"- failed_entry_count: {summary.get('failed_entry_count', 0)}",
        f"- deleted_size_mb: {summary.get('deleted_size_mb', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## entries"])
    for entry in payload.get("entries") or []:
        lines.append(
            "- " + json.dumps(
                {
                    "relative_path": entry.get("relative_path"),
                    "entry_kind": entry.get("entry_kind"),
                    "status": entry.get("status"),
                    "size_bytes": entry.get("size_bytes"),
                    "error": entry.get("error"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return "\n".join(lines).strip() + "\n"


def _persist_checkpoint(payload: dict, json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_audit_artifact_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )


def execute_audit_artifact_cleanup(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    expected_target_dir: Optional[str] = None,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
) -> Tuple[Path, Path, dict]:
    """执行或预演 audit artifact 清理。"""
    normalized_mode = _normalize_mode(mode)
    normalized_checkpoint_every = _normalize_checkpoint_every(checkpoint_every)
    source_preview_artifact, preview_payload = _load_preview_payload(
        preview_payload=preview_payload,
        source_preview_artifact=source_preview_artifact,
        audit_output_dir=output_dir,
    )

    preview_payload = dict(preview_payload or {})
    preview_summary = dict(preview_payload.get("summary") or {})
    execution_plan = dict(preview_payload.get("execution_plan") or {})
    scope_entries = _normalize_scope_entries(execution_plan.get("scope_entries") or [])
    target_dir = _resolve_target_dir(preview_payload, expected_target_dir)
    entry_records, resolved_paths = _build_scope_entry_records(scope_entries, target_dir)
    json_path, md_path = _resolve_output_paths(output_dir)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "source_audit_artifact_cleanup_preview_artifact": source_preview_artifact or "",
        "source_preview_generated_at": preview_payload.get("generated_at") or "",
        "parameters": {
            "target_dir": str(target_dir),
            "expected_target_dir": str(_resolve_project_path(expected_target_dir)) if expected_target_dir else "",
            "scope_entry_total": len(scope_entries),
            "checkpoint_every": normalized_checkpoint_every,
        },
        "preview_summary": preview_summary,
        "summary": {
            "scope_entry_total": len(scope_entries),
            "deleted_entry_count": 0,
            "missing_entry_count": 0,
            "failed_entry_count": 0,
            "processed_entry_count": 0,
            "deleted_size_bytes": 0,
            "deleted_size_mb": 0.0,
        },
        "findings": [],
        "guardrails": list(execution_plan.get("guardrails") or []),
        "entries": entry_records,
    }

    if normalized_mode == "apply":
        _validate_apply_prerequisites(
            confirm_apply=confirm_apply,
            scope_entries=scope_entries,
        )
        payload["findings"].append("已通过确认串和路径边界校验，可以进入 audit 条目删除阶段。")
    else:
        payload["findings"].append("当前为 dry-run，只输出计划和 checkpoint，不执行任何删除。")

    if scope_entries:
        payload["findings"].append(
            f"本次 scope 共 {len(scope_entries)} 个条目，目标目录为 {target_dir}。"
        )
    else:
        payload["findings"].append("当前预演未发现可自动清理的 audit 条目。")

    _persist_checkpoint(payload, json_path, md_path)

    if normalized_mode == "dry-run":
        payload["status"] = "dry-run"
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    payload["status"] = "running"
    for entry in payload["entries"]:
        entry["status"] = "pending"
    _persist_checkpoint(payload, json_path, md_path)

    last_checkpoint_processed = 0
    for index, entry in enumerate(payload["entries"], start=1):
        entry["status"] = "running"
        entry["started_at"] = datetime.now().isoformat()

        resolved_path = resolved_paths[(entry["relative_path"], entry["entry_kind"])]
        try:
            if not resolved_path.exists():
                entry["status"] = "missing"
                payload["summary"]["missing_entry_count"] += 1
            else:
                if resolved_path.is_dir():
                    shutil.rmtree(resolved_path)
                else:
                    resolved_path.unlink()
                entry["status"] = "deleted"
                payload["summary"]["deleted_entry_count"] += 1
                payload["summary"]["deleted_size_bytes"] += int(entry.get("size_bytes") or 0)
        except Exception as exc:
            entry["status"] = "failed"
            entry["error"] = f"{type(exc).__name__}: {exc}"
            payload["summary"]["failed_entry_count"] += 1

        entry["finished_at"] = datetime.now().isoformat()
        payload["summary"]["processed_entry_count"] = index
        payload["summary"]["deleted_size_mb"] = round(
            payload["summary"]["deleted_size_bytes"] / (1024 * 1024),
            4,
        )
        should_force_persist = entry["status"] == "failed"
        if should_force_persist or (
            index - last_checkpoint_processed >= normalized_checkpoint_every
        ):
            _persist_checkpoint(payload, json_path, md_path)
            last_checkpoint_processed = index

    if payload["summary"]["failed_entry_count"] > 0:
        payload["status"] = "failed"
        payload["findings"].append("部分 audit 条目删除失败，需先处理失败项后再继续后续维护。")
    else:
        payload["status"] = "applied"
        payload["findings"].append("全部选定 audit 条目已执行完成。")
        if payload["summary"]["missing_entry_count"] > 0:
            payload["findings"].append("部分 audit 条目在执行前已不存在，已按 missing 记录。")
    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload
