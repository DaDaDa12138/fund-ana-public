#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Execution checkpoint for compressing database backup snapshots."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.database_backup_compression_preview import (
    DEFAULT_ACTIVE_DATABASE_PATH,
    DEFAULT_OUTPUT_DIR,
    load_latest_database_backup_compression_preview_payload,
)
from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.verified_backup_compression import compress_verified_backup


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATABASE_BACKUP_COMPRESSION_EXECUTION_JSON_PATTERN = "database_backup_compression_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_DATABASE_BACKUP_COMPRESSION"
DEFAULT_CHECKPOINT_EVERY = 10


def load_latest_database_backup_compression_execution_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        DATABASE_BACKUP_COMPRESSION_EXECUTION_JSON_PATTERN,
        output_dir=output_dir,
    )


def _resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_ROOT / path).resolve()


def _resolve_audit_dir(output_dir: str | Path) -> Path:
    artifact_dir = _resolve_project_path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _resolve_output_paths(output_dir: str | Path, timestamp: Optional[str] = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    artifact_dir = _resolve_audit_dir(output_dir)
    return (
        artifact_dir / f"database_backup_compression_execution_{ts}.json",
        artifact_dir / f"database_backup_compression_execution_{ts}.md",
    )


def _normalize_mode(mode: str) -> str:
    value = str(mode or "dry-run").strip().lower().replace("_", "-")
    if value not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    return value


def _normalize_checkpoint_every(value) -> int:
    try:
        parsed = int(value if value is not None else DEFAULT_CHECKPOINT_EVERY)
    except (TypeError, ValueError):
        parsed = DEFAULT_CHECKPOINT_EVERY
    return max(1, parsed)


def _load_preview_payload(
    *,
    preview_payload: Optional[dict],
    source_preview_artifact: Optional[str],
    audit_output_dir: str | Path,
) -> tuple[str, dict]:
    if preview_payload is not None:
        return str(source_preview_artifact or "").strip(), dict(preview_payload or {})
    if source_preview_artifact:
        preview_path = _resolve_project_path(source_preview_artifact)
        return str(preview_path), json.loads(preview_path.read_text(encoding="utf-8"))
    preview_path, payload = load_latest_database_backup_compression_preview_payload(
        output_dir=str(audit_output_dir)
    )
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


def _resolve_target_backup_dir(preview_payload: dict, expected_backup_dir: Optional[str | Path]) -> Path:
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
        raw_path = target_backup_dir / relative_path
        if any(path.is_symlink() for path in (raw_path, *raw_path.parents)):
            raise ValueError(f"backup scope must not contain symlinks: {relative_path}")
        resolved_path = (target_backup_dir / relative_path).resolve()
        try:
            resolved_path.relative_to(target_backup_dir)
        except ValueError as exc:
            raise ValueError(f"scope file escapes target backup directory: {relative_path}") from exc
        compressed_path = resolved_path.with_suffix(resolved_path.suffix + ".zst")
        size_bytes = int(resolved_path.stat().st_size) if resolved_path.exists() and resolved_path.is_file() else 0
        records.append(
            {
                "relative_path": relative_path,
                "absolute_path": str(resolved_path),
                "compressed_relative_path": compressed_path.relative_to(target_backup_dir).as_posix(),
                "compressed_absolute_path": str(compressed_path),
                "status": "planned",
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / (1024 * 1024), 4),
                "compressed_size_bytes": 0,
                "compressed_size_mb": 0.0,
                "freed_size_bytes": 0,
                "freed_size_mb": 0.0,
                "started_at": None,
                "finished_at": None,
                "error": "",
            }
        )
        resolved_paths[relative_path] = resolved_path
    return records, resolved_paths


def _validate_apply_prerequisites(*, confirm_apply: Optional[str], scope_files: list[str]) -> None:
    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")
    if not scope_files:
        raise ValueError("apply requires at least one scope file")


def render_database_backup_compression_execution_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    lines = [
        "# database backup compression execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- source_preview_artifact: {payload.get('source_database_backup_compression_preview_artifact')}",
        f"- target_backup_dir: {parameters.get('target_backup_dir')}",
        f"- scope_file_total: {summary.get('scope_file_total', 0)}",
        f"- compressed_file_count: {summary.get('compressed_file_count', 0)}",
        f"- existing_compressed_verified_file_count: {summary.get('existing_compressed_verified_file_count', 0)}",
        f"- source_removed_file_count: {summary.get('source_removed_file_count', 0)}",
        f"- failed_file_count: {summary.get('failed_file_count', 0)}",
        f"- freed_size_mb: {summary.get('freed_size_mb', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## files"])
    for item in payload.get("files") or []:
        lines.append(
            "- "
            + json.dumps(
                {
                    "relative_path": item.get("relative_path"),
                    "status": item.get("status"),
                    "size_bytes": item.get("size_bytes"),
                    "compressed_size_bytes": item.get("compressed_size_bytes"),
                    "freed_size_bytes": item.get("freed_size_bytes"),
                    "error": item.get("error"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return "\n".join(lines).strip() + "\n"


def _persist_checkpoint(payload: dict, json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_database_backup_compression_execution_markdown(payload),
        encoding="utf-8",
    )


def execute_database_backup_compression(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    expected_backup_dir: Optional[str | Path] = None,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
    remove_source_after_verified: bool = True,
) -> Tuple[Path, Path, dict]:
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
    target_backup_dir = _resolve_target_backup_dir(preview_payload, expected_backup_dir)
    file_records, resolved_paths = _build_scope_file_records(scope_files, target_backup_dir)
    json_path, md_path = _resolve_output_paths(output_dir)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "source_database_backup_compression_preview_artifact": source_preview_artifact or "",
        "source_preview_generated_at": preview_payload.get("generated_at") or "",
        "parameters": {
            "target_backup_dir": str(target_backup_dir),
            "expected_backup_dir": str(_resolve_project_path(expected_backup_dir)) if expected_backup_dir else "",
            "scope_file_total": len(scope_files),
            "checkpoint_every": normalized_checkpoint_every,
            "remove_source_after_verified": bool(remove_source_after_verified),
        },
        "preview_summary": preview_summary,
        "summary": {
            "scope_file_total": len(scope_files),
            "compressed_file_count": 0,
            "existing_compressed_verified_file_count": 0,
            "source_removed_file_count": 0,
            "missing_file_count": 0,
            "failed_file_count": 0,
            "processed_file_count": 0,
            "source_size_bytes": sum(int(item.get("size_bytes") or 0) for item in file_records),
            "compressed_size_bytes": 0,
            "freed_size_bytes": 0,
            "freed_size_mb": 0.0,
            "removed_source_size_bytes": 0,
            "capacity_scope": "source_plus_compressed_plus_verification_manifest_allocated_blocks",
        },
        "findings": [],
        "guardrails": list(execution_plan.get("guardrails") or []),
        "files": file_records,
    }
    if normalized_mode == "apply":
        _validate_apply_prerequisites(confirm_apply=confirm_apply, scope_files=scope_files)
        payload["findings"].append("已通过确认串和路径边界校验，可以进入备份压缩阶段。")
    else:
        payload["findings"].append("当前为 dry-run，只输出压缩计划和 checkpoint，不执行压缩或源文件移除。")

    if not scope_files:
        payload["findings"].append("当前预演未发现可压缩的数据库备份文件。")
        payload["status"] = "dry-run" if normalized_mode == "dry-run" else "skipped"
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

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
        source_path = resolved_paths[item["relative_path"]]
        compressed_path = source_path.with_suffix(source_path.suffix + ".zst")
        try:
            if not source_path.exists():
                item["status"] = "missing"
                payload["summary"]["missing_file_count"] += 1
            elif not source_path.is_file():
                item["status"] = "failed"
                item["error"] = f"Path is not a file: {source_path}"
                payload["summary"]["failed_file_count"] += 1
            else:
                source_size = int(source_path.stat().st_size)
                item["size_bytes"] = source_size
                item["size_mb"] = round(source_size / (1024 * 1024), 4)
                active_path = (preview_payload.get("parameters") or {}).get("active_database_path")
                started = time.monotonic()
                proof = compress_verified_backup(
                    source_path,
                    active_paths=[DEFAULT_ACTIVE_DATABASE_PATH, *([_resolve_project_path(active_path)] if active_path else [])],
                    remove_source=remove_source_after_verified,
                )
                item.update(proof)
                item["compression_elapsed_seconds"] = round(time.monotonic() - started, 3)
                if proof["existing_compressed"]:
                    payload["summary"]["existing_compressed_verified_file_count"] += 1
                    item["status"] = "existing_compressed_verified"
                else:
                    payload["summary"]["compressed_file_count"] += 1
                    item["status"] = "compressed"

                compressed_size = int(compressed_path.stat().st_size)
                item["compressed_size_bytes"] = compressed_size
                item["compressed_size_mb"] = round(compressed_size / (1024 * 1024), 4)
                payload["summary"]["compressed_size_bytes"] += compressed_size
                item["freed_size_bytes"] = proof["net_reclaimed_bytes"]
                item["freed_size_mb"] = round(proof["net_reclaimed_bytes"] / (1024 * 1024), 4)
                payload["summary"]["freed_size_bytes"] += proof["net_reclaimed_bytes"]
                payload["summary"]["removed_source_size_bytes"] += proof["removed_source_size_bytes"]
                if proof["source_removed"]:
                    payload["summary"]["source_removed_file_count"] += 1
                    item["status"] = item["status"] + "_source_removed"
        except Exception as exc:
            item["status"] = "failed"
            item["error"] = f"{type(exc).__name__}: {exc}"
            payload["summary"]["failed_file_count"] += 1
        finally:
            item["finished_at"] = datetime.now().isoformat()
            payload["summary"]["processed_file_count"] += 1
            payload["summary"]["freed_size_mb"] = round(
                payload["summary"]["freed_size_bytes"] / (1024 * 1024),
                4,
            )
            processed_count = int(payload["summary"]["processed_file_count"] or 0)
            if item["status"] == "failed" or (
                processed_count - last_checkpoint_processed >= normalized_checkpoint_every
            ):
                _persist_checkpoint(payload, json_path, md_path)
                last_checkpoint_processed = processed_count

    if payload["summary"]["failed_file_count"] > 0 or payload["summary"]["missing_file_count"] > 0:
        payload["status"] = "failed"
        payload["findings"].append("部分备份压缩失败，需先处理失败项后再继续后续维护。")
    else:
        payload["status"] = "applied"
        payload["findings"].append("选定数据库备份压缩 scope 已执行完成。")

    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload
