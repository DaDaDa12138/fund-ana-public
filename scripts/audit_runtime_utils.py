from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.runtime_replay import (
    build_replay_precision_summary,
    load_runtime_payload,
    merge_runtime_artifacts,
    resolve_runtime_artifact_path,
)


def load_json(path: Path) -> dict[str, Any]:
    try:
        from backend.task_audit_storage import is_maintenance_audit_path, read_task_audit

        if path.name.endswith((".json.gz", ".json.legacy.gz")) or is_maintenance_audit_path(path):
            return read_task_audit(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def build_audit_artifact_path(
    audit_dir: Path,
    prefix: str,
    *,
    run_id: str = "",
    suffix: str = ".json",
) -> Path:
    audit_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    clean_run_id = (
        clean_text(run_id)
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
    )
    stem = f"{prefix}_{clean_run_id}_{timestamp}" if clean_run_id else f"{prefix}_{timestamp}"
    candidate = audit_dir / f"{stem}{suffix}"
    duplicate_index = 2
    while candidate.exists():
        candidate = audit_dir / f"{stem}_{duplicate_index}{suffix}"
        duplicate_index += 1
    return candidate


def resolve_run_payload(
    settings,
    *,
    run_id: str = "",
    project_root: Path,
) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]:
    latest_payload = load_json(settings.latest_report_file)
    if not latest_payload:
        raise RuntimeError(f"latest_report_missing_or_invalid: {settings.latest_report_file}")

    latest_run_id = clean_text(latest_payload.get("run_id"))
    if not latest_run_id:
        raise RuntimeError("latest_report_missing_run_id")

    target_run_id = clean_text(run_id) or latest_run_id
    payload = latest_payload if target_run_id == latest_run_id else load_runtime_payload(settings, target_run_id)
    artifacts = merge_runtime_artifacts(payload.get("artifacts") or {})
    replay_precision = build_replay_precision_summary(
        payload.get("artifacts") or {},
        project_root=project_root,
    )
    return target_run_id, payload, artifacts, replay_precision


def resolve_artifact_project_path(
    artifacts: dict[str, Any],
    artifact_key: str,
    *,
    project_root: Path,
) -> tuple[str, Path | None]:
    artifact_value = clean_text(artifacts.get(artifact_key))
    if not artifact_value:
        return "", None
    return artifact_value, resolve_runtime_artifact_path(
        artifact_value,
        project_root=project_root,
    )
