#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选输出目录历史产物清理执行器。

默认 dry-run；只有显式 apply 且满足确认串、预演产物、路径边界校验时才会执行。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_output_cleanup_preview import (
    SCREENING_OUTPUT_CLEANUP_PREVIEW_JSON_PATTERN,
    load_latest_screening_output_cleanup_preview_payload,
)
from utils.screening_output_cleanup_runbook import (
    load_latest_screening_output_cleanup_runbook_payload,
)
from utils.screening_output_cleanup_preflight import (
    load_latest_screening_output_cleanup_preflight_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCREENING_OUTPUT_CLEANUP_EXECUTION_JSON_PATTERN = "screening_output_cleanup_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_SCREENING_OUTPUT_CLEANUP"
DEFAULT_CHECKPOINT_EVERY = 100


def load_latest_screening_output_cleanup_execution_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选输出 cleanup 执行 checkpoint。"""
    return load_latest_json_artifact(
        SCREENING_OUTPUT_CLEANUP_EXECUTION_JSON_PATTERN,
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


def _normalize_artifact_value(value) -> str:
    return str(value or "").strip()


def _artifact_refs_match(left, right) -> bool:
    normalized_left = _normalize_artifact_value(left)
    normalized_right = _normalize_artifact_value(right)
    return bool(normalized_left) and bool(normalized_right) and normalized_left == normalized_right


def _artifact_generated_at_match(left, right) -> bool:
    normalized_left = _normalize_artifact_value(left)
    normalized_right = _normalize_artifact_value(right)
    if not normalized_left or not normalized_right:
        return True
    return normalized_left == normalized_right


def _load_optional_artifact_payload(
    *,
    payload: Optional[dict],
    source_artifact: Optional[str],
    output_dir: str,
    loader,
) -> tuple[str, dict]:
    if payload is not None:
        return str(source_artifact or "").strip(), dict(payload or {})

    if source_artifact:
        artifact_path = _resolve_project_path(source_artifact)
        loaded_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        return str(artifact_path), dict(loaded_payload or {})

    try:
        artifact_path, loaded_payload = loader(output_dir=output_dir)
    except Exception:
        return "", {}
    return str(artifact_path), dict(loaded_payload or {})


def _resolve_output_paths(output_dir: str, timestamp: Optional[str] = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_audit_dir(output_dir)
    return (
        artifact_dir / f"screening_output_cleanup_execution_{ts}.json",
        artifact_dir / f"screening_output_cleanup_execution_{ts}.md",
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

    preview_path, payload = load_latest_screening_output_cleanup_preview_payload(output_dir=audit_output_dir)
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


def _resolve_target_output_dir(preview_payload: dict, expected_output_dir: Optional[str]) -> Path:
    execution_plan = dict(preview_payload.get("execution_plan") or {})
    preview_output_dir = str(
        execution_plan.get("target_dir")
        or preview_payload.get("output_dir")
        or ""
    ).strip()
    if not preview_output_dir:
        raise ValueError("preview payload missing target output directory")

    resolved_preview_dir = _resolve_project_path(preview_output_dir)
    if expected_output_dir:
        resolved_expected_dir = _resolve_project_path(expected_output_dir)
        if resolved_expected_dir != resolved_preview_dir:
            raise ValueError("expected_output_dir does not match preview target_dir")

    return resolved_preview_dir


def _build_scope_file_records(scope_files: list[str], target_output_dir: Path) -> tuple[list[dict], dict[str, Path]]:
    records = []
    resolved_paths = {}

    for relative_path in scope_files:
        resolved_path = (target_output_dir / relative_path).resolve()
        try:
            resolved_path.relative_to(target_output_dir)
        except ValueError as exc:
            raise ValueError(f"scope file escapes target output directory: {relative_path}") from exc

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
    target_output_dir: Path,
    preview_payload: dict,
    source_preview_artifact: Optional[str],
    runbook_payload: dict,
    source_runbook_artifact: Optional[str],
    preflight_payload: dict,
    source_preflight_artifact: Optional[str],
) -> None:
    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")
    if not scope_files:
        raise ValueError("apply requires at least one scope file")

    normalized_source_preview_artifact = _normalize_artifact_value(source_preview_artifact)
    normalized_source_runbook_artifact = _normalize_artifact_value(source_runbook_artifact)
    normalized_source_preflight_artifact = _normalize_artifact_value(source_preflight_artifact)
    if not normalized_source_runbook_artifact or not runbook_payload:
        raise ValueError("apply requires matching runbook artifact")
    if not normalized_source_preflight_artifact or not preflight_payload:
        raise ValueError("apply requires matching preflight artifact")

    preflight_checks = dict(preflight_payload.get("checks") or {})
    if not preflight_checks.get("ready_for_manual_signoff"):
        raise ValueError("preflight is not ready_for_manual_signoff")

    if not _artifact_refs_match(
        runbook_payload.get("source_screening_output_cleanup_preview_artifact"),
        normalized_source_preview_artifact,
    ):
        raise ValueError("runbook does not match preview artifact")
    if not _artifact_generated_at_match(
        runbook_payload.get("source_screening_output_cleanup_preview_generated_at"),
        preview_payload.get("generated_at"),
    ):
        raise ValueError("runbook preview generated_at does not match selected preview")

    if not _artifact_refs_match(
        preflight_payload.get("source_screening_output_cleanup_preview_artifact"),
        normalized_source_preview_artifact,
    ):
        raise ValueError("preflight does not match preview artifact")
    if not _artifact_refs_match(
        preflight_payload.get("source_screening_output_cleanup_runbook_artifact"),
        normalized_source_runbook_artifact,
    ):
        raise ValueError("preflight does not match runbook artifact")
    if not _artifact_generated_at_match(
        preflight_payload.get("source_screening_output_cleanup_preview_generated_at"),
        preview_payload.get("generated_at"),
    ):
        raise ValueError("preflight preview generated_at does not match selected preview")
    if not _artifact_generated_at_match(
        preflight_payload.get("source_screening_output_cleanup_runbook_generated_at"),
        runbook_payload.get("generated_at"),
    ):
        raise ValueError("preflight runbook generated_at does not match selected runbook")

    runbook_target_output_dir = _normalize_artifact_value(runbook_payload.get("target_output_dir"))
    if runbook_target_output_dir and _resolve_project_path(runbook_target_output_dir) != target_output_dir:
        raise ValueError("runbook target_output_dir does not match preview target_output_dir")

    preflight_target_output_dir = _normalize_artifact_value(preflight_payload.get("target_output_dir"))
    if preflight_target_output_dir and _resolve_project_path(preflight_target_output_dir) != target_output_dir:
        raise ValueError("preflight target_output_dir does not match preview target_output_dir")


def _normalize_checkpoint_every(value) -> int:
    try:
        parsed = int(value if value is not None else DEFAULT_CHECKPOINT_EVERY)
    except (TypeError, ValueError):
        parsed = DEFAULT_CHECKPOINT_EVERY
    return max(1, parsed)


def render_screening_output_cleanup_execution_markdown(payload: dict) -> str:
    """渲染执行 checkpoint Markdown。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    lines = [
        "# screening output cleanup execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- source_preview_artifact: {payload.get('source_screening_output_cleanup_preview_artifact')}",
        f"- source_runbook_artifact: {payload.get('source_screening_output_cleanup_runbook_artifact')}",
        f"- source_preflight_artifact: {payload.get('source_screening_output_cleanup_preflight_artifact')}",
        f"- target_output_dir: {parameters.get('target_output_dir')}",
        f"- expected_output_dir: {parameters.get('expected_output_dir')}",
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


def write_screening_output_cleanup_execution_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    """写出筛选输出 cleanup 执行 checkpoint JSON/Markdown。"""
    json_path, md_path = _resolve_output_paths(output_dir, timestamp=timestamp)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_screening_output_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path


def _persist_checkpoint(payload: dict, json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_screening_output_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )


def execute_screening_output_cleanup(
    *,
    preview_payload: Optional[dict] = None,
    runbook_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    source_runbook_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    expected_output_dir: Optional[str] = None,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
) -> Tuple[Path, Path, dict]:
    """执行或预演筛选输出目录历史产物清理。"""
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
    target_output_dir = _resolve_target_output_dir(preview_payload, expected_output_dir)
    file_records, resolved_paths = _build_scope_file_records(scope_files, target_output_dir)
    json_path, md_path = _resolve_output_paths(output_dir)
    requested_runbook_artifact = str(source_runbook_artifact or "").strip()
    requested_preflight_artifact = str(source_preflight_artifact or "").strip()
    source_runbook_artifact, runbook_payload = _load_optional_artifact_payload(
        payload=runbook_payload,
        source_artifact=source_runbook_artifact,
        output_dir=output_dir,
        loader=load_latest_screening_output_cleanup_runbook_payload,
    )
    source_preflight_artifact, preflight_payload = _load_optional_artifact_payload(
        payload=preflight_payload,
        source_artifact=source_preflight_artifact,
        output_dir=output_dir,
        loader=load_latest_screening_output_cleanup_preflight_payload,
    )

    runbook_matches_preview = bool(source_runbook_artifact) and _artifact_refs_match(
        runbook_payload.get("source_screening_output_cleanup_preview_artifact"),
        source_preview_artifact,
    ) and _artifact_generated_at_match(
        runbook_payload.get("source_screening_output_cleanup_preview_generated_at"),
        preview_payload.get("generated_at"),
    )
    if requested_runbook_artifact and source_runbook_artifact and not runbook_matches_preview:
        raise ValueError("selected runbook does not match selected preview artifact")
    if not runbook_matches_preview:
        source_runbook_artifact = ""
        runbook_payload = {}

    preflight_matches_chain = bool(source_preflight_artifact) and _artifact_refs_match(
        preflight_payload.get("source_screening_output_cleanup_preview_artifact"),
        source_preview_artifact,
    ) and _artifact_generated_at_match(
        preflight_payload.get("source_screening_output_cleanup_preview_generated_at"),
        preview_payload.get("generated_at"),
    )
    if source_runbook_artifact:
        preflight_matches_chain = preflight_matches_chain and _artifact_refs_match(
            preflight_payload.get("source_screening_output_cleanup_runbook_artifact"),
            source_runbook_artifact,
        ) and _artifact_generated_at_match(
            preflight_payload.get("source_screening_output_cleanup_runbook_generated_at"),
            runbook_payload.get("generated_at"),
        )
    if requested_preflight_artifact and source_preflight_artifact and not preflight_matches_chain:
        raise ValueError("selected preflight does not match selected preview/runbook chain")
    if not preflight_matches_chain:
        source_preflight_artifact = ""
        preflight_payload = {}

    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "source_screening_output_cleanup_preview_artifact": source_preview_artifact or "",
        "source_preview_generated_at": preview_payload.get("generated_at") or "",
        "source_screening_output_cleanup_runbook_artifact": "",
        "source_runbook_generated_at": "",
        "source_screening_output_cleanup_preflight_artifact": "",
        "source_preflight_generated_at": "",
        "parameters": {
            "target_output_dir": str(target_output_dir),
            "expected_output_dir": str(_resolve_project_path(expected_output_dir)) if expected_output_dir else "",
            "scope_file_total": len(scope_files),
            "checkpoint_every": normalized_checkpoint_every,
        },
        "preview_summary": preview_summary,
        "summary": {
            "scope_file_total": len(scope_files),
            "deleted_file_count": 0,
            "missing_file_count": 0,
            "failed_file_count": 0,
            "processed_file_count": 0,
            "deleted_size_bytes": 0,
            "deleted_size_mb": 0.0,
        },
        "findings": [],
        "guardrails": list(execution_plan.get("guardrails") or []),
        "files": file_records,
    }

    if source_runbook_artifact:
        payload["source_screening_output_cleanup_runbook_artifact"] = source_runbook_artifact
        payload["source_runbook_generated_at"] = str(runbook_payload.get("generated_at") or "").strip()
    if source_preflight_artifact:
        payload["source_screening_output_cleanup_preflight_artifact"] = source_preflight_artifact
        payload["source_preflight_generated_at"] = str(preflight_payload.get("generated_at") or "").strip()

    if normalized_mode == "apply":
        _validate_apply_prerequisites(
            confirm_apply=confirm_apply,
            scope_files=scope_files,
            target_output_dir=target_output_dir,
            preview_payload=preview_payload,
            source_preview_artifact=source_preview_artifact,
            runbook_payload=runbook_payload,
            source_runbook_artifact=source_runbook_artifact,
            preflight_payload=preflight_payload,
            source_preflight_artifact=source_preflight_artifact,
        )
        payload["findings"].append("已通过确认串和路径边界校验，可以进入文件删除阶段。")
    else:
        payload["findings"].append("当前为 dry-run，只输出计划和 checkpoint，不执行任何删除。")

    if scope_files:
        payload["findings"].append(
            f"本次 scope 共 {len(scope_files)} 个文件，目标目录为 {target_output_dir}。"
        )
    else:
        payload["findings"].append("当前预演未发现需要清理的历史输出文件。")

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
        payload["findings"].append("部分文件删除失败，需先处理失败项后再继续后续维护。")
    else:
        payload["status"] = "applied"
        payload["findings"].append("选定 scope 已执行完成。")
        if payload["summary"]["missing_file_count"] > 0:
            payload["findings"].append("部分文件在执行前已不存在，已按 missing 记录。")

    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload
