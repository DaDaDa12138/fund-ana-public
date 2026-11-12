#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库备份目录清理执行器。

默认 dry-run；只有显式 apply 且满足确认串、预演产物、路径边界校验时才会执行。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.database_backup_cleanup_preview import (
    load_latest_database_backup_cleanup_preview_payload,
)
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATABASE_BACKUP_CLEANUP_EXECUTION_JSON_PATTERN = "database_backup_cleanup_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_DATABASE_BACKUP_CLEANUP"
DEFAULT_CHECKPOINT_EVERY = 50


def load_latest_database_backup_cleanup_execution_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新数据库备份 cleanup 执行 checkpoint。"""
    return load_latest_json_artifact(
        DATABASE_BACKUP_CLEANUP_EXECUTION_JSON_PATTERN,
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


def _resolve_audit_dir(output_dir: str) -> Path:
    artifact_dir = _resolve_project_path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _resolve_output_paths(output_dir: str, timestamp: Optional[str] = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_audit_dir(output_dir)
    return (
        artifact_dir / f"database_backup_cleanup_execution_{ts}.json",
        artifact_dir / f"database_backup_cleanup_execution_{ts}.md",
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

    preview_path, payload = load_latest_database_backup_cleanup_preview_payload(output_dir=audit_output_dir)
    return str(preview_path), dict(payload or {})


def _normalize_scope_files(scope_files) -> list[str]:
    normalized = []
    seen = set()
    for item in scope_files or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _validate_selected_manual_review_files(
    selected_manual_review_files,
    *,
    allowed_manual_review_files: list[str],
) -> list[str]:
    selected_files = _normalize_scope_files(selected_manual_review_files)
    if not selected_files:
        return []

    allowed_set = set(allowed_manual_review_files)
    invalid_files = [item for item in selected_files if item not in allowed_set]
    if invalid_files:
        raise ValueError(
            "selected_manual_review_files contains files outside preview manual_review_files: "
            + ", ".join(invalid_files)
        )
    return selected_files


def _merge_effective_scope_files(
    scope_files: list[str],
    selected_manual_review_files: list[str],
) -> list[str]:
    merged = []
    seen = set()
    for item in list(scope_files) + list(selected_manual_review_files):
        if item in seen:
            continue
        seen.add(item)
        merged.append(item)
    return merged


def _normalize_checkpoint_every(value) -> int:
    try:
        parsed = int(value if value is not None else DEFAULT_CHECKPOINT_EVERY)
    except (TypeError, ValueError):
        parsed = DEFAULT_CHECKPOINT_EVERY
    return max(1, parsed)


def _resolve_target_backup_dir(preview_payload: dict, expected_backup_dir: Optional[str]) -> Path:
    execution_plan = dict(preview_payload.get("execution_plan") or {})
    preview_backup_dir = str(
        execution_plan.get("target_dir")
        or preview_payload.get("backup_dir")
        or ""
    ).strip()
    if not preview_backup_dir:
        raise ValueError("preview payload missing target backup directory")

    resolved_preview_dir = _resolve_project_path(preview_backup_dir)
    if expected_backup_dir:
        resolved_expected_dir = _resolve_project_path(expected_backup_dir)
        if resolved_expected_dir != resolved_preview_dir:
            raise ValueError("expected_backup_dir does not match preview target_dir")

    return resolved_preview_dir


def _build_scope_file_records(scope_files: list[str], target_backup_dir: Path) -> tuple[list[dict], dict[str, Path]]:
    records = []
    resolved_paths = {}

    for relative_path in scope_files:
        resolved_path = (target_backup_dir / relative_path).resolve()
        try:
            resolved_path.relative_to(target_backup_dir)
        except ValueError as exc:
            raise ValueError(f"scope file escapes target backup directory: {relative_path}") from exc

        size_bytes = 0
        if resolved_path.exists() and resolved_path.is_file():
            size_bytes = int(resolved_path.stat().st_size)

        record = {
            "relative_path": relative_path,
            "absolute_path": str(resolved_path),
            "status": "planned",
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 4),
            "started_at": None,
            "finished_at": None,
            "error": "",
        }
        records.append(record)
        resolved_paths[relative_path] = resolved_path

    return records, resolved_paths


def _validate_apply_prerequisites(
    *,
    confirm_apply: Optional[str],
    scope_files: list[str],
) -> None:
    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")
    if not scope_files:
        raise ValueError("apply requires at least one scope file")


def render_database_backup_cleanup_execution_markdown(payload: dict) -> str:
    """渲染执行 checkpoint Markdown。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    lines = [
        "# database backup cleanup execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- source_preview_artifact: {payload.get('source_database_backup_cleanup_preview_artifact')}",
        f"- target_backup_dir: {parameters.get('target_backup_dir')}",
        f"- expected_backup_dir: {parameters.get('expected_backup_dir')}",
        f"- checkpoint_every: {parameters.get('checkpoint_every')}",
        f"- scope_file_total: {summary.get('scope_file_total', 0)}",
        f"- deleted_file_count: {summary.get('deleted_file_count', 0)}",
        f"- missing_file_count: {summary.get('missing_file_count', 0)}",
        f"- failed_file_count: {summary.get('failed_file_count', 0)}",
        f"- deleted_size_mb: {summary.get('deleted_size_mb', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## files"])
    for file_item in payload.get("files") or []:
        lines.append(
            "- " + json.dumps(
                {
                    "relative_path": file_item.get("relative_path"),
                    "status": file_item.get("status"),
                    "size_bytes": file_item.get("size_bytes"),
                    "error": file_item.get("error"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )

    return "\n".join(lines).strip() + "\n"


def _persist_checkpoint(payload: dict, json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_database_backup_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )


def execute_database_backup_cleanup(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    selected_manual_review_files=None,
    output_dir: str = DEFAULT_AUDIT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    expected_backup_dir: Optional[str] = None,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
) -> Tuple[Path, Path, dict]:
    """执行或预演数据库备份目录清理。"""
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
    scope_files = _normalize_scope_files(execution_plan.get("scope_files") or [])
    manual_review_files = _normalize_scope_files(execution_plan.get("manual_review_files") or [])
    selected_manual_review_files = _validate_selected_manual_review_files(
        selected_manual_review_files,
        allowed_manual_review_files=manual_review_files,
    )
    effective_scope_files = _merge_effective_scope_files(scope_files, selected_manual_review_files)
    target_backup_dir = _resolve_target_backup_dir(preview_payload, expected_backup_dir)
    file_records, resolved_paths = _build_scope_file_records(effective_scope_files, target_backup_dir)
    json_path, md_path = _resolve_output_paths(output_dir)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "source_database_backup_cleanup_preview_artifact": source_preview_artifact or "",
        "source_preview_generated_at": preview_payload.get("generated_at") or "",
        "parameters": {
            "target_backup_dir": str(target_backup_dir),
            "expected_backup_dir": str(_resolve_project_path(expected_backup_dir)) if expected_backup_dir else "",
            "scope_file_total": len(effective_scope_files),
            "auto_scope_file_total": len(scope_files),
            "manual_review_file_total": len(manual_review_files),
            "selected_manual_review_file_total": len(selected_manual_review_files),
            "checkpoint_every": normalized_checkpoint_every,
        },
        "preview_summary": preview_summary,
        "summary": {
            "scope_file_total": len(effective_scope_files),
            "auto_scope_file_total": len(scope_files),
            "manual_review_file_total": len(manual_review_files),
            "selected_manual_review_file_total": len(selected_manual_review_files),
            "deleted_file_count": 0,
            "missing_file_count": 0,
            "failed_file_count": 0,
            "processed_file_count": 0,
            "deleted_size_bytes": 0,
            "deleted_size_mb": 0.0,
        },
        "findings": [],
        "guardrails": list(execution_plan.get("guardrails") or []),
        "manual_review_files": manual_review_files,
        "selected_manual_review_files": selected_manual_review_files,
        "files": file_records,
    }

    if normalized_mode == "apply":
        _validate_apply_prerequisites(
            confirm_apply=confirm_apply,
            scope_files=effective_scope_files,
        )
        payload["findings"].append("已通过确认串和路径边界校验，可以进入备份文件删除阶段。")
    else:
        payload["findings"].append("当前为 dry-run，只输出计划和 checkpoint，不执行任何删除。")

    if effective_scope_files:
        payload["findings"].append(
            f"本次 scope 共 {len(effective_scope_files)} 个文件，目标目录为 {target_backup_dir}。"
        )
    else:
        payload["findings"].append("当前预演未发现可自动清理的数据库备份文件。")

    if selected_manual_review_files:
        payload["findings"].append(
            f"本次额外纳入 {len(selected_manual_review_files)} 个已人工签字的 manual review 文件进入执行 scope。"
        )

    if manual_review_files:
        payload["findings"].append(
            f"另有 {len(manual_review_files)} 个维护快照或未识别备份需要人工复核，不会进入自动删除。"
        )

    _persist_checkpoint(payload, json_path, md_path)

    if normalized_mode == "dry-run":
        payload["status"] = "dry-run"
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    payload["status"] = "running"
    for item in payload["files"]:
        item["status"] = "pending"
    _persist_checkpoint(payload, json_path, md_path)

    last_checkpoint_processed = 0
    for item in payload["files"]:
        item["status"] = "running"
        item["started_at"] = datetime.now().isoformat()

        target_path = resolved_paths[item["relative_path"]]
        try:
            if not target_path.exists():
                item["status"] = "missing"
                payload["summary"]["missing_file_count"] += 1
            elif not target_path.is_file():
                item["status"] = "failed"
                item["error"] = f"Path is not a file: {target_path}"
                payload["summary"]["failed_file_count"] += 1
            else:
                deleted_size_bytes = int(target_path.stat().st_size)
                target_path.unlink()
                item["status"] = "deleted"
                item["size_bytes"] = deleted_size_bytes
                item["size_mb"] = round(deleted_size_bytes / (1024 * 1024), 4)
                payload["summary"]["deleted_file_count"] += 1
                payload["summary"]["deleted_size_bytes"] += deleted_size_bytes
        except Exception as exc:
            item["status"] = "failed"
            item["error"] = f"{type(exc).__name__}: {exc}"
            payload["summary"]["failed_file_count"] += 1
        finally:
            item["finished_at"] = datetime.now().isoformat()
            payload["summary"]["processed_file_count"] += 1
            payload["summary"]["deleted_size_mb"] = round(
                payload["summary"]["deleted_size_bytes"] / (1024 * 1024),
                4,
            )
            processed_count = int(payload["summary"]["processed_file_count"] or 0)
            should_force_persist = item["status"] == "failed"
            if should_force_persist or (
                processed_count - last_checkpoint_processed >= normalized_checkpoint_every
            ):
                _persist_checkpoint(payload, json_path, md_path)
                last_checkpoint_processed = processed_count

    if payload["summary"]["failed_file_count"] > 0:
        payload["status"] = "failed"
        payload["findings"].append("部分备份文件删除失败，需先处理失败项后再继续后续维护。")
    else:
        payload["status"] = "applied"
        payload["findings"].append("选定数据库备份 scope 已执行完成。")
        if payload["summary"]["missing_file_count"] > 0:
            payload["findings"].append("部分备份文件在执行前已不存在，已按 missing 记录。")

    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload
