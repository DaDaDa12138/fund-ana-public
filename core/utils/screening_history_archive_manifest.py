#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Manifest contract for sharded screening-history cold archives.

The screening-history archive is a recovery source used by the hot-delete
guard.  The manifest therefore deliberately fails closed: every declared
shard must stay inside the active archive directory, use a supported format,
and have a unique path/id before callers may trust it.

Paths stored in the manifest are relative to the manifest directory.  This
keeps test/runtime directories relocatable without permitting arbitrary path
escape through a modified manifest.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


MANIFEST_SCHEMA_VERSION = 1
DEFAULT_MANIFEST_FILENAME = "screening_history_archive_manifest.json"
SUPPORTED_SHARD_FORMATS = {"sqlite", "parquet"}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_project_root(project_root: str | Path | None) -> Path:
    if project_root is not None:
        return Path(project_root).expanduser().resolve()
    return Path(__file__).resolve().parent.parent.parent


def resolve_archive_path(
    path_value: str | Path,
    *,
    project_root: str | Path | None = None,
) -> Path:
    root = _resolve_project_root(project_root)
    path = Path(path_value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def resolve_manifest_path(
    active_archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> Path:
    active = resolve_archive_path(active_archive_path, project_root=project_root)
    if manifest_path is None or not _normalize_text(manifest_path):
        return (active.parent / DEFAULT_MANIFEST_FILENAME).resolve()
    candidate = Path(manifest_path).expanduser()
    return (
        candidate.resolve()
        if candidate.is_absolute()
        else (_resolve_project_root(project_root) / candidate).resolve()
    )


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _relative_manifest_path(path: Path, manifest_path: Path) -> str:
    resolved = path.resolve()
    parent = manifest_path.parent.resolve()
    if not _is_relative_to(resolved, parent):
        raise ValueError(f"archive shard must stay inside {parent}: {resolved}")
    return resolved.relative_to(parent).as_posix()


def _resolve_shard_path(value: Any, *, manifest_path: Path) -> Path:
    raw_text = _normalize_text(value)
    if not raw_text:
        raise ValueError("archive shard path is blank")
    raw = Path(raw_text).expanduser()
    resolved = raw.resolve() if raw.is_absolute() else (manifest_path.parent / raw).resolve()
    if not _is_relative_to(resolved, manifest_path.parent.resolve()):
        raise ValueError(f"archive shard path escapes manifest directory: {raw_text}")
    return resolved


def _normalize_shard(
    raw: Any,
    *,
    role: str,
    manifest_path: Path,
    default_id: str,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    source = dict(raw) if isinstance(raw, dict) else {}
    shard_id = _normalize_text(source.get("id")) or default_id
    shard_format = _normalize_text(source.get("format")).lower() or "sqlite"
    state = _normalize_text(source.get("state")).lower() or (
        "writable" if role == "active" else "immutable"
    )
    try:
        absolute_path = _resolve_shard_path(source.get("path"), manifest_path=manifest_path)
    except ValueError as exc:
        errors.append(str(exc))
        absolute_path = manifest_path.parent / f"invalid-{default_id}.db"
    if shard_format not in SUPPORTED_SHARD_FORMATS:
        errors.append(f"unsupported archive shard format {shard_format}: {shard_id}")
    if role == "active" and state != "writable":
        errors.append(f"active archive shard must have state=writable: {shard_id}")
    if role == "active" and shard_format != "sqlite":
        errors.append(f"active archive shard must use SQLite: {shard_id}")
    if role == "immutable" and state != "immutable":
        errors.append(f"immutable archive shard must have state=immutable: {shard_id}")

    exists = absolute_path.exists() and absolute_path.is_file()
    stat = absolute_path.stat() if exists else None
    normalized = {
        **source,
        "declared_file_size_bytes": _normalize_int(source.get("file_size_bytes")),
        "declared_file_mtime_ns": _normalize_int(source.get("file_mtime_ns")),
        "id": shard_id,
        "role": role,
        "format": shard_format,
        "state": state,
        "path": _normalize_text(source.get("path")),
        "absolute_path": str(absolute_path),
        "exists": exists,
        "file_size_bytes": int(stat.st_size) if stat else 0,
        "file_mtime_ns": int(stat.st_mtime_ns) if stat else 0,
        "storage_size_bytes": int(stat.st_size) if stat else 0,
    }
    if shard_format == "parquet":
        for key in ("manifest_sha256", "source_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", str(source.get(key) or "")):
                errors.append(f"parquet shard requires {key}: {shard_id}")
        if not errors:
            try:
                from utils.screening_archive_access import parquet_shard_storage_bytes

                normalized["storage_size_bytes"] = parquet_shard_storage_bytes(normalized)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                errors.append(f"parquet shard unavailable: {shard_id}: {exc}")
    return normalized, errors


def build_initial_manifest_payload(
    active_archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    active = resolve_archive_path(active_archive_path, project_root=project_root)
    resolved_manifest = resolve_manifest_path(
        active,
        manifest_path=manifest_path,
        project_root=project_root,
    )
    if resolved_manifest.parent.resolve() != active.parent.resolve():
        raise ValueError("screening archive manifest must share the active shard directory")
    now = datetime.now().isoformat()
    payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generation": 1,
        "created_at": now,
        "updated_at": now,
        "active_shard": {
            "id": "active",
            "path": _relative_manifest_path(active, resolved_manifest),
            "format": "sqlite",
            "state": "writable",
        },
        "immutable_shards": [],
    }
    return resolved_manifest, payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def write_screening_archive_manifest(
    manifest_path: str | Path,
    payload: dict[str, Any],
) -> Path:
    path = Path(manifest_path).expanduser().resolve()
    _atomic_write_json(path, payload)
    return path


def ensure_screening_archive_manifest(
    active_archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> tuple[Path, dict[str, Any], bool]:
    resolved_manifest, initial = build_initial_manifest_payload(
        active_archive_path,
        manifest_path=manifest_path,
        project_root=project_root,
    )
    if resolved_manifest.exists():
        state = load_screening_archive_manifest(
            active_archive_path,
            manifest_path=resolved_manifest,
            project_root=project_root,
        )
        if state.get("status") != "ready":
            raise ValueError(
                "screening archive manifest is invalid: "
                + "; ".join(state.get("errors") or ["unknown_error"])
            )
        return resolved_manifest, dict(state.get("payload") or {}), False
    write_screening_archive_manifest(resolved_manifest, initial)
    return resolved_manifest, initial, True


def load_screening_archive_manifest(
    active_archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Load and validate a manifest, or return a legacy single-shard view."""
    active = resolve_archive_path(active_archive_path, project_root=project_root)
    resolved_manifest = resolve_manifest_path(
        active,
        manifest_path=manifest_path,
        project_root=project_root,
    )
    if resolved_manifest.parent.resolve() != active.parent.resolve():
        return {
            "status": "invalid",
            "manifest_exists": resolved_manifest.exists(),
            "manifest_path": str(resolved_manifest),
            "active_archive_path": str(active),
            "generation": 0,
            "shards": [],
            "errors": ["screening archive manifest must share the active shard directory"],
            "payload": {},
        }

    if not resolved_manifest.exists():
        exists = active.exists() and active.is_file()
        stat = active.stat() if exists else None
        return {
            "status": "legacy",
            "manifest_exists": False,
            "manifest_path": str(resolved_manifest),
            "active_archive_path": str(active),
            "generation": 0,
            "shards": [
                {
                    "id": "legacy-active",
                    "role": "active",
                    "format": "sqlite",
                    "state": "writable",
                    "path": active.name,
                    "absolute_path": str(active),
                    "exists": exists,
                    "file_size_bytes": int(stat.st_size) if stat else 0,
                    "file_mtime_ns": int(stat.st_mtime_ns) if stat else 0,
                }
            ],
            "errors": [],
            "payload": {},
        }

    try:
        payload = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "invalid",
            "manifest_exists": True,
            "manifest_path": str(resolved_manifest),
            "active_archive_path": str(active),
            "generation": 0,
            "shards": [],
            "errors": [f"manifest_read_failed:{type(exc).__name__}:{exc}"],
            "payload": {},
        }
    if not isinstance(payload, dict):
        payload = {}

    errors: list[str] = []
    schema_version = _normalize_int(payload.get("schema_version"))
    generation = _normalize_int(payload.get("generation"))
    if schema_version != MANIFEST_SCHEMA_VERSION:
        errors.append(
            f"unsupported manifest schema_version={schema_version}; expected={MANIFEST_SCHEMA_VERSION}"
        )
    if generation < 1:
        errors.append("manifest generation must be >= 1")

    active_shard, active_errors = _normalize_shard(
        payload.get("active_shard"),
        role="active",
        manifest_path=resolved_manifest,
        default_id="active",
    )
    errors.extend(active_errors)
    if Path(active_shard["absolute_path"]).resolve() != active.resolve():
        errors.append(
            "manifest active shard does not match configured archive target: "
            f"{active_shard['absolute_path']} != {active}"
        )

    immutable_source = payload.get("immutable_shards")
    if immutable_source is None:
        immutable_source = []
    if not isinstance(immutable_source, list):
        errors.append("manifest immutable_shards must be a list")
        immutable_source = []
    immutable: list[dict[str, Any]] = []
    for index, raw in enumerate(immutable_source):
        shard, shard_errors = _normalize_shard(
            raw,
            role="immutable",
            manifest_path=resolved_manifest,
            default_id=f"immutable-{index + 1}",
        )
        immutable.append(shard)
        errors.extend(shard_errors)

    shards = [active_shard, *immutable]
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    for shard in shards:
        shard_id = _normalize_text(shard.get("id"))
        shard_path = str(Path(shard["absolute_path"]).resolve())
        if shard_id in seen_ids:
            errors.append(f"duplicate archive shard id: {shard_id}")
        seen_ids.add(shard_id)
        if shard_path in seen_paths:
            errors.append(f"duplicate archive shard path: {shard_path}")
        seen_paths.add(shard_path)

    return {
        "status": "ready" if not errors else "invalid",
        "manifest_exists": True,
        "manifest_path": str(resolved_manifest),
        "active_archive_path": str(active),
        "generation": generation,
        "schema_version": schema_version,
        "shards": shards,
        "errors": errors,
        "payload": payload,
    }


def refresh_active_manifest_metadata(
    active_archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Create/load the manifest and atomically advance active metadata."""
    active = resolve_archive_path(active_archive_path, project_root=project_root)
    resolved_manifest, payload, _created = ensure_screening_archive_manifest(
        active,
        manifest_path=manifest_path,
        project_root=project_root,
    )
    state = load_screening_archive_manifest(
        active,
        manifest_path=resolved_manifest,
        project_root=project_root,
    )
    if state.get("status") != "ready":
        raise ValueError(
            "screening archive manifest is invalid: "
            + "; ".join(state.get("errors") or ["unknown_error"])
        )
    updated = dict(payload)
    active_entry = dict(updated.get("active_shard") or {})
    stat = active.stat() if active.exists() and active.is_file() else None
    active_entry.update(
        {
            "id": _normalize_text(active_entry.get("id")) or "active",
            "path": _relative_manifest_path(active, resolved_manifest),
            "format": "sqlite",
            "state": "writable",
            "file_size_bytes": int(stat.st_size) if stat else 0,
            "file_mtime_ns": int(stat.st_mtime_ns) if stat else 0,
        }
    )
    active_entry.update(dict(metadata or {}))
    updated["active_shard"] = active_entry
    updated["schema_version"] = MANIFEST_SCHEMA_VERSION
    updated["generation"] = max(1, _normalize_int(updated.get("generation"), 1)) + 1
    updated["updated_at"] = datetime.now().isoformat()
    updated.setdefault("created_at", updated["updated_at"])
    updated.setdefault("immutable_shards", [])
    write_screening_archive_manifest(resolved_manifest, updated)
    return resolved_manifest, updated


def queryable_sqlite_shards(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(shard)
        for shard in state.get("shards") or []
        if _normalize_text(shard.get("format")).lower() == "sqlite"
    ]


def queryable_archive_shards(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(shard) for shard in state.get("shards") or []
            if _normalize_text(shard.get("format")).lower() in SUPPORTED_SHARD_FORMATS]


def immutable_sqlite_paths(state: dict[str, Any]) -> list[Path]:
    return [
        Path(str(shard.get("absolute_path"))).resolve()
        for shard in queryable_sqlite_shards(state)
        if shard.get("role") == "immutable"
    ]


def total_existing_shard_bytes(shards: Iterable[dict[str, Any]]) -> int:
    return sum(
        _normalize_int(shard.get("storage_size_bytes", shard.get("file_size_bytes")))
        for shard in shards
        if bool(shard.get("exists"))
    )
