#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backup evidence for screening history cleanup apply windows."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from utils.sqlite_backup_snapshot import create_verified_sqlite_backup, sqlite_snapshot_metadata
from utils.verified_backup_compression import (
    DEFAULT_COMPRESSED_SPACE_MULTIPLIER, DEFAULT_UNCOMPRESSED_SPACE_MULTIPLIER,
    backup_space_budget,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/screening_history_cleanup"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_screening_cleanup"
BACKUP_EVIDENCE_JSON_PATTERN = "screening_history_cleanup_backup_evidence_*.json"


def _resolve_project_path(value: str | Path, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _project_relative(path: str | Path, *, project_root: Path = PROJECT_ROOT) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sqlite_readonly_metadata(path: Path) -> tuple[str, dict]:
    if not path.exists() or not path.is_file():
        return "", {}
    try:
        conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=30)
        try:
            conn.execute('BEGIN')
            metadata = sqlite_snapshot_metadata(conn, path)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return f"error:{exc}", {}
    return "ok", metadata


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def _path_writable(path: Path) -> bool:
    try:
        return path.exists() and os.access(path, os.W_OK | os.X_OK)
    except OSError:
        return False


def _latest_matching_backup_artifacts(
    *,
    backup_dir: Path,
    backup_prefix: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not backup_dir.exists() or not backup_dir.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for pattern in (f"{backup_prefix}_*.db", f"{backup_prefix}_*.db.zst"):
        for path in backup_dir.glob(pattern):
            if not path.is_file():
                continue
            stat = path.stat()
            rows.append(
                {
                    "path": str(path),
                    "size_bytes": int(stat.st_size),
                    "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(
                        timespec="seconds"
                    ),
                    "format": "zstd_sqlite" if path.name.endswith(".db.zst") else "sqlite",
                }
            )
    rows.sort(key=lambda item: str(item.get("mtime") or ""), reverse=True)
    return rows[: max(1, int(limit or 5))]


def _resolve_optional_backup_file(
    backup_file: str | Path | None,
    *,
    project_root: Path,
) -> Path | None:
    text = str(backup_file or "").strip()
    if not text:
        return None
    return _resolve_project_path(text, project_root=project_root)


def _build_blocker(code: str, detail: str, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "detail": detail}
    payload.update(extra)
    return payload


def build_screening_history_cleanup_backup_readiness_payload(
    *,
    source_db_path: str | Path | None,
    backup_dir: str | Path | None = None,
    project_root: str | Path = PROJECT_ROOT,
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    backup_file: str | Path | None = None,
    compress: bool = True,
    auto_create_backup_if_missing: bool = True,
    space_multiplier: float | None = None,
) -> dict:
    """Read-only readiness check for the rollback backup required by cleanup apply."""
    root = Path(project_root).resolve()
    source_path = (
        _resolve_project_path(source_db_path, project_root=root)
        if source_db_path
        else None
    )
    resolved_backup_dir = (
        _resolve_project_path(backup_dir, project_root=root)
        if backup_dir
        else (
            source_path.parent / "backups"
            if source_path is not None
            else _resolve_project_path("backend/data/backups", project_root=root)
        )
    )
    explicit_backup_path = _resolve_optional_backup_file(backup_file, project_root=root)
    backup_prefix = str(backup_prefix or DEFAULT_BACKUP_PREFIX).strip() or DEFAULT_BACKUP_PREFIX
    source_exists = bool(source_path and source_path.exists() and source_path.is_file())
    source_size = int(source_path.stat().st_size) if source_exists and source_path else 0
    parent_for_creation = _nearest_existing_parent(resolved_backup_dir)
    backup_dir_exists = resolved_backup_dir.exists()
    backup_dir_is_dir = backup_dir_exists and resolved_backup_dir.is_dir()
    backup_dir_writable = _path_writable(resolved_backup_dir) if backup_dir_is_dir else False
    parent_writable = _path_writable(parent_for_creation)
    can_create_backup_dir = (not backup_dir_exists) and parent_for_creation.is_dir() and parent_writable
    disk_check_path = resolved_backup_dir if backup_dir_is_dir else parent_for_creation
    source_readonly_probe, source_metadata = (
        _sqlite_readonly_metadata(source_path) if source_exists and source_path else ("", {})
    )
    budget = backup_space_budget(
        disk_check_path, max(source_size, source_metadata.get('source_logical_size_bytes', 0)),
        compress=compress, create_snapshot=True, space_multiplier=space_multiplier,
    )
    multiplier = budget['space_multiplier']
    required_free_bytes = budget['required_free_bytes']
    explicit_backup_exists = bool(
        explicit_backup_path and explicit_backup_path.exists() and explicit_backup_path.is_file()
    )
    zstd_path = shutil.which("zstd") if compress or (
        explicit_backup_path is not None and str(explicit_backup_path).endswith(".zst")
    ) else None
    latest_artifacts = _latest_matching_backup_artifacts(
        backup_dir=resolved_backup_dir,
        backup_prefix=backup_prefix,
    )
    blockers: list[dict[str, Any]] = []

    if not source_path:
        blockers.append(
            _build_blocker(
                "source_db_path_missing",
                "source database path is required before backup creation can be planned",
            )
        )
    elif not source_exists:
        blockers.append(
            _build_blocker(
                "source_db_missing",
                f"source database does not exist: {source_path}",
                source_db_path=str(source_path),
            )
        )
    elif source_readonly_probe != "ok":
        blockers.append(
            _build_blocker(
                "source_db_readonly_probe_failed",
                f"source database readonly probe failed: {source_readonly_probe}",
                source_db_path=str(source_path),
            )
        )

    if explicit_backup_path is not None and not explicit_backup_exists:
        blockers.append(
            _build_blocker(
                "explicit_backup_missing",
                f"configured backup_file does not exist: {explicit_backup_path}",
                backup_file=str(explicit_backup_path),
            )
        )

    if explicit_backup_path is None:
        if not auto_create_backup_if_missing:
            blockers.append(
                _build_blocker(
                    "backup_missing_and_auto_create_disabled",
                    "apply requires backup_file or auto_create_backup_if_missing=true",
                )
            )
        if backup_dir_exists and not backup_dir_is_dir:
            blockers.append(
                _build_blocker(
                    "backup_dir_not_directory",
                    f"backup path exists but is not a directory: {resolved_backup_dir}",
                )
            )
        elif not backup_dir_exists and not can_create_backup_dir:
            blockers.append(
                _build_blocker(
                    "backup_dir_parent_not_writable",
                    f"backup directory cannot be created under: {parent_for_creation}",
                    backup_dir=str(resolved_backup_dir),
                    parent_for_creation=str(parent_for_creation),
                )
            )
        elif backup_dir_exists and not backup_dir_writable:
            blockers.append(
                _build_blocker(
                    "backup_dir_not_writable",
                    f"backup directory is not writable: {resolved_backup_dir}",
                )
            )
        if compress and not zstd_path:
            blockers.append(
                _build_blocker(
                    "zstd_unavailable",
                    "zstd executable is required because backup_compress=true",
                )
            )
        if source_size > 0 and not budget['sufficient']:
            blockers.append(
                _build_blocker(
                    "insufficient_backup_disk_space",
                    "free disk space is below the conservative backup creation requirement",
                    free_bytes=budget['disk_free_bytes'],
                    required_free_bytes=required_free_bytes,
                    space_multiplier=multiplier,
                )
            )
    elif str(explicit_backup_path).endswith(".zst") and not zstd_path:
        blockers.append(
            _build_blocker(
                "zstd_unavailable_for_existing_backup",
                "zstd executable is required to validate or restore the configured .zst backup",
                backup_file=str(explicit_backup_path),
            )
        )

    creation_mode = (
        "explicit_backup_file"
        if explicit_backup_path is not None
        else (
            "auto_create_on_apply"
            if auto_create_backup_if_missing
            else "blocked_missing_backup"
        )
    )
    status = "ready" if not blockers else "blocked"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "mode": "readiness",
        "backup_required_for_apply": True,
        "backup_checked": True,
        "creation_mode": creation_mode,
        "auto_create_backup_if_missing": bool(auto_create_backup_if_missing),
        "backup_prefix": backup_prefix,
        "compress": bool(compress),
        "source_db_path": str(source_path) if source_path else "",
        "source_db_exists": source_exists,
        "source_db_size_bytes": source_size,
        "source_db_readonly_probe": source_readonly_probe,
        "source_db_logical_size_bytes": source_metadata.get('source_logical_size_bytes'),
        "source_db_wal_size_bytes": source_metadata.get('source_wal_size_bytes'),
        "snapshot_metadata": source_metadata,
        "backup_file": str(explicit_backup_path) if explicit_backup_path else "",
        "backup_file_exists": explicit_backup_exists,
        "backup_dir": str(resolved_backup_dir),
        "backup_dir_exists": backup_dir_exists,
        "backup_dir_is_dir": backup_dir_is_dir,
        "backup_dir_writable": backup_dir_writable,
        "backup_dir_can_be_created": can_create_backup_dir,
        "backup_dir_parent_for_creation": str(parent_for_creation),
        "backup_dir_parent_writable": parent_writable,
        "zstd_required": bool(compress)
        or bool(explicit_backup_path and str(explicit_backup_path).endswith(".zst")),
        "zstd_available": bool(zstd_path),
        "zstd_path": str(zstd_path or ""),
        "disk_check_path": str(disk_check_path),
        "disk_total_bytes": budget['disk_total_bytes'],
        "disk_free_bytes": budget['disk_free_bytes'],
        "required_free_bytes": required_free_bytes,
        "space_multiplier": multiplier,
        "disk_space_margin_bytes": budget['disk_space_margin_bytes'],
        "metadata_reserve_bytes": budget['metadata_reserve_bytes'],
        "latest_matching_backup_count": len(latest_artifacts),
        "latest_matching_backups": latest_artifacts,
        "blocker_count": len(blockers),
        "blockers": blockers,
    }


def _write_json_md(
    *,
    payload: dict,
    output_dir: str | Path,
    prefix: str,
    markdown_title: str,
    project_root: Path = PROJECT_ROOT,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    ts = timestamp or _now_ts()
    out_dir = _resolve_project_path(output_dir, project_root=project_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{prefix}_{ts}.json"
    md_path = out_dir / f"{prefix}_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"# {markdown_title}",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- status: {payload.get('status', '')}",
    ]
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return json_path, md_path


def _backup_sidecar_paths(db_backup_path: Path) -> dict[str, Path]:
    return {
        "wal": db_backup_path.parent / f"{db_backup_path.name}-wal",
        "shm": db_backup_path.parent / f"{db_backup_path.name}-shm",
    }


def _sidecar_snapshot(paths: dict[str, Path]) -> dict[str, dict[str, Any]]:
    snapshot: dict[str, dict[str, Any]] = {}
    for key, path in paths.items():
        exists = path.exists()
        snapshot[key] = {
            "path": str(path),
            "exists": exists,
            "size_bytes": int(path.stat().st_size) if exists else 0,
        }
    return snapshot


def _cleanup_backup_sidecars_for_artifact(db_backup_path: Path) -> dict[str, Any]:
    """Remove SQLite backup sidecars once the backup DB is known to be standalone."""
    paths = _backup_sidecar_paths(db_backup_path)
    before = _sidecar_snapshot(paths)
    checkpoint_attempted = False
    checkpoint_result: list[tuple[Any, ...]] = []

    wal_path = paths["wal"]
    if wal_path.exists() and wal_path.stat().st_size > 0:
        checkpoint_attempted = True
        conn = sqlite3.connect(str(db_backup_path), timeout=120)
        try:
            checkpoint_result = [
                tuple(row)
                for row in conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            ]
        finally:
            conn.close()

    after_checkpoint = _sidecar_snapshot(paths)
    wal_still_nonempty = (
        paths["wal"].exists()
        and int(paths["wal"].stat().st_size) > 0
    )
    if wal_still_nonempty:
        raise RuntimeError(
            f"backup WAL sidecar is not empty after checkpoint: {paths['wal']}"
        )

    removed: list[dict[str, Any]] = []
    for key, path in paths.items():
        if not path.exists():
            continue
        size = int(path.stat().st_size)
        path.unlink()
        removed.append({"kind": key, "path": str(path), "size_bytes": size})

    return {
        "sidecars_before": before,
        "checkpoint_attempted": checkpoint_attempted,
        "checkpoint_result": checkpoint_result,
        "sidecars_after_checkpoint": after_checkpoint,
        "removed_sidecars": removed,
        "removed_sidecar_count": len(removed),
        "sidecars_after_cleanup": _sidecar_snapshot(paths),
    }


def create_screening_history_cleanup_backup(
    *,
    source_db_path: str | Path,
    backup_dir: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    project_root: str | Path = PROJECT_ROOT,
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    timestamp: Optional[str] = None,
    purpose: str = "screening_history_cleanup",
    compress: bool = True,
    keep_uncompressed: bool = False,
    compute_sha256: bool = True,
    dry_run: bool = False,
) -> Tuple[Path, Path, dict]:
    """Create a verified whole-DB rollback point with its producer's purpose."""
    root = Path(project_root).resolve()
    source_path = _resolve_project_path(source_db_path, project_root=root)
    resolved_backup_dir = _resolve_project_path(backup_dir, project_root=root)
    ts = timestamp or _now_ts()
    backup_label = f"{backup_prefix}_{ts}"
    db_backup_path = resolved_backup_dir / f"{backup_label}.db"
    planned_artifact_path = (
        db_backup_path.with_suffix(db_backup_path.suffix + ".zst")
        if compress
        else db_backup_path
    )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "dry-run" if dry_run else "pending",
        "mode": "dry-run" if dry_run else "create",
        "summary": {
            "source_db_path": str(source_path),
            "source_db_exists": source_path.exists() and source_path.is_file(),
            "source_db_size_bytes": source_path.stat().st_size if source_path.exists() else 0,
            "backup_dir": str(resolved_backup_dir),
            "backup_label": backup_label,
            "backup_purpose": purpose,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_artifact_path": _project_relative(planned_artifact_path, project_root=root),
            "backup_artifact_format": "zstd_sqlite" if compress else "sqlite",
            "compress": bool(compress),
            "keep_uncompressed": bool(keep_uncompressed),
            "created": False,
            "source_sqlite_quick_check": "",
            "sqlite_quick_check": "",
            "backup_artifact_size_bytes": 0,
            "backup_artifact_sha256": "",
            "zstd_test_succeeded": False,
            "elapsed_seconds": 0.0,
        },
        "findings": [],
    }
    if not source_path.exists() or not source_path.is_file():
        payload["status"] = "blocked"
        payload["findings"].append(f"source database does not exist: {source_path}")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="screening_history_cleanup_backup_evidence",
            markdown_title="screening history cleanup backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)
    if dry_run:
        payload["findings"].append("dry-run only; no backup file was created.")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="screening_history_cleanup_backup_evidence",
            markdown_title="screening history cleanup backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)

    backup_result = create_verified_sqlite_backup(
        source_path, db_backup_path, purpose=purpose, backup_label=backup_label,
        compress=compress, keep_uncompressed=keep_uncompressed,
        check_source=True,
    )
    artifact_path = backup_result["artifact_path"]
    compression_summary = backup_result
    quick_check = backup_result["sqlite_quick_check"]
    sha256 = backup_result["artifact_sha256"] if compute_sha256 else ""
    elapsed = backup_result["elapsed_seconds"]
    source_quick_check = backup_result["source_sqlite_quick_check"]
    sidecar_cleanup = _cleanup_backup_sidecars_for_artifact(db_backup_path)
    payload["status"] = "success"
    payload["summary"].update(
        {
            "created": True,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_db_exists": db_backup_path.exists(),
            "backup_artifact_path": _project_relative(artifact_path, project_root=root),
            "backup_artifact_absolute_path": str(artifact_path),
            "backup_artifact_size_bytes": artifact_path.stat().st_size,
            "backup_artifact_size_mb": round(artifact_path.stat().st_size / (1024 * 1024), 2),
            "backup_artifact_sha256": sha256,
            "verification_manifest_path": _project_relative(
                backup_result["verification_manifest_path"], project_root=root),
            "verification_manifest_sha256": backup_result["verification_manifest_sha256"],
            "recovery_verification": backup_result["recovery_verification"],
            "snapshot_sha256": backup_result["snapshot_sha256"],
            "decoded_sha256": backup_result["decoded_sha256"],
            "snapshot_metadata": backup_result["snapshot_metadata"],
            "backup_space_budget": backup_result["backup_space_budget"],
            "source_sqlite_quick_check": source_quick_check,
            "sqlite_quick_check": quick_check,
            "backup_sidecar_cleanup": sidecar_cleanup,
            "zstd_test_succeeded": bool(compression_summary.get("zstd_test_succeeded")),
            "uncompressed_removed": bool(compression_summary.get("uncompressed_removed")),
            "compression_elapsed_seconds": compression_summary.get(
                "compression_elapsed_seconds",
                0.0,
            ),
            "elapsed_seconds": elapsed,
        }
    )
    payload["findings"].append(
        f"{purpose} rollback backup was created and verified."
    )
    return _write_json_md(
        payload=payload,
        output_dir=output_dir,
        prefix="screening_history_cleanup_backup_evidence",
        markdown_title="screening history cleanup backup evidence",
        project_root=root,
        timestamp=ts,
    ) + (payload,)
