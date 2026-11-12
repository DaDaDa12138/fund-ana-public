"""Lossless, restartable migration of completed legacy task audit files.

New tasks already use gzip. This bounded operator migration removes only an
unchanged plain copy after independent restoration of its adjacent gzip.
"""
from __future__ import annotations

from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import time
from typing import Any, Callable
from uuid import uuid4

from backend.task_audit_storage import (
    MAX_TASK_AUDIT_BYTES, _no_symlinks, is_legacy_task_audit_path, migration_paths,
    read_migrated_task_audit, read_original_artifact_bytes,
)

MIN_AGE_SECONDS = 3600
TERMINAL_STATUSES = {"success", "degraded", "failed", "skipped", "warning"}
AUDIT_SCOPES = {
    "legacy_scheduler_task_audit_json": "scheduler_task_runs",
    "legacy_task_history_extra_data_json": "task_history_extra_data",
}


def identity(path: Path) -> dict[str, int]:
    info = path.lstat()
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
            "mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid,
            "links": info.st_nlink, "allocated_bytes": info.st_blocks * 512}


def _read_candidate(path: Path, *, now: float) -> tuple[bytes, dict, dict]:
    if not is_legacy_task_audit_path(path):
        raise ValueError("legacy_audit_path_outside_scope")
    _no_symlinks(path)
    before = identity(path)
    if not stat.S_ISREG(before["mode"]) or before["links"] != 1:
        raise ValueError("legacy_audit_requires_unshared_regular_file")
    if now - before["mtime_ns"] / 1_000_000_000 < MIN_AGE_SECONDS:
        raise ValueError("legacy_audit_recent_write_protected")
    original = read_original_artifact_bytes(path)
    payload = json.loads(original)
    if not isinstance(payload, dict):
        raise ValueError("legacy_audit_object_required")
    if path.parent.parent.name == "task_history_extra_data":
        result = payload.get("full_payload")
        task_id = payload.get("task_history_id")
        if (payload.get("schema_version") != 1 or type(task_id) is not int or task_id < 1
                or not path.name.startswith(f"task_history_extra_data_{task_id}_")
                or payload.get("source") not in {"guarded_task_history_extra_data_sidecarization", "scheduler_finalize"}
                or not isinstance(result, dict) or result.get("status") not in TERMINAL_STATUSES
                or not isinstance(payload.get("task_name"), str) or not payload["task_name"]
                or payload["task_name"] != result.get("task_name")):
            raise ValueError("legacy_sidecar_identity_or_terminal_state_invalid")
        full = json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if (type(payload.get("full_payload_bytes")) is not int or len(full) != payload["full_payload_bytes"]
                or hashlib.sha256(full).hexdigest() != payload.get("full_payload_sha256")):
            raise ValueError("legacy_sidecar_full_payload_sha_mismatch")
        task = {"task_history_id": task_id, "task_name": payload["task_name"],
                "status": result["status"], "started_at": result.get("started_at"),
                "finished_at": result.get("finished_at")}
    else:
        task, result = payload.get("task") or {}, payload.get("task_result") or {}
        if (not isinstance(task, dict) or not isinstance(result, dict)
                or task.get("status") not in TERMINAL_STATUSES or task.get("status") != result.get("status")
                or payload.get("persistence_stage") not in (None, "completion_recorded")):
            raise ValueError("legacy_audit_not_terminal")
    if identity(path) != before:
        raise ValueError("legacy_audit_changed_during_read")
    return original, task, before


def plan_legacy_task_audit_compression(
    root: Path, *, max_files: int = 20000, scope: str = "legacy_scheduler_task_audit_json",
) -> dict[str, Any]:
    if not 1 <= max_files <= 20000:
        raise ValueError("legacy_audit_file_budget_invalid")
    if scope not in AUDIT_SCOPES:
        raise ValueError("legacy_audit_scope_invalid")
    root = root.resolve()
    directory = root / "reports/.audit" / AUDIT_SCOPES[scope]
    _no_symlinks(directory)
    now = time.time()
    candidates, retained = [], []
    for path in sorted(directory.glob("*/*.json")):
        relative = str(path.relative_to(root))
        if len(candidates) >= max_files:
            retained.append({"path": relative, "reason": "file_budget"})
            continue
        try:
            original, task, before = _read_candidate(path, now=now)
            candidates.append({"path": relative, "sha256": hashlib.sha256(original).hexdigest(),
                               "identity": before, "task": task})
        except (OSError, ValueError, TypeError) as exc:
            retained.append({"path": relative, "reason": str(exc)})
    return {"schema_version": 1, "scope": scope, "mode": "dry-run",
            "generated_at": datetime.now().astimezone().isoformat(), "min_age_seconds": MIN_AGE_SECONDS,
            "candidates": candidates, "retained": retained, "removed_count": 0,
            "candidate_count": len(candidates), "retained_count": len(retained),
            "candidate_logical_bytes": sum(row["identity"]["size"] for row in candidates),
            "candidate_allocated_bytes": sum(row["identity"]["allocated_bytes"] for row in candidates),
            "net_reclaimed_disk_bytes": None}


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _install(path: Path, data: bytes, source: dict) -> bool:
    """Create, never replace. A completed earlier delivery may be reused."""
    _no_symlinks(path)
    if path.exists():
        if path.stat().st_size > MAX_TASK_AUDIT_BYTES or path.read_bytes() != data:
            raise ValueError("legacy_audit_destination_conflict")
        return False
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fchmod(stream.fileno(), stat.S_IMODE(source["mode"]))
            os.fchown(stream.fileno(), source["uid"], source["gid"])
            os.fsync(stream.fileno())
        os.link(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return True


def _independent_restore(path: Path, original: bytes) -> None:
    # Restore to a real independent file; gzip CRC alone is not an original-SHA
    # proof. The temporary file is automatically closed and removed on failure.
    with gzip.open(path, "rb") as source, tempfile.TemporaryFile(dir=path.parent) as restored:
        count = 0
        while block := source.read(1024 * 1024):
            count += len(block)
            if count > MAX_TASK_AUDIT_BYTES:
                raise ValueError("legacy_audit_restore_budget_exceeded")
            restored.write(block)
        restored.flush()
        os.fsync(restored.fileno())
        restored.seek(0)
        if restored.read() != original:
            raise ValueError("legacy_audit_independent_restore_mismatch")


def _allocation(paths: tuple[Path, ...]) -> int:
    seen, total = set(), 0
    for path in paths:
        if path.exists():
            info = path.lstat()
            key = (info.st_dev, info.st_ino)
            if key not in seen:
                total += info.st_blocks * 512
                seen.add(key)
    return total


def compress_legacy_task_audit(root: Path, candidate: dict[str, Any]) -> dict[str, Any]:
    return _compress_closed_artifact(
        root, candidate, read_candidate=lambda path: _read_candidate(path, now=time.time()),
        read_migrated=read_migrated_task_audit,
    )


def _compress_closed_artifact(
    root: Path, candidate: dict[str, Any], *,
    read_candidate: Callable[[Path], tuple[bytes, dict, dict]],
    read_migrated: Callable[[Path], tuple[bytes, bytes, dict]],
    receipt_identity_key: str = "task_identity",
) -> dict[str, Any]:
    """Shared delivery transaction; each caller supplies its terminal contract."""
    if receipt_identity_key not in {"task_identity", "audit_identity"}:
        raise ValueError("legacy_audit_receipt_identity_key_invalid")
    root = root.resolve()
    path = root / candidate["path"]
    if not path.resolve().is_relative_to(root):
        raise ValueError("legacy_audit_path_outside_project")
    stored_path, receipt_path = migration_paths(path)
    before_allocation = _allocation((path, stored_path, receipt_path))
    outcome = {"path": candidate["path"], "status": "failed", "removed": False,
               "independent_restore_verified": False, "net_reclaimed_disk_bytes": None,
               "estimated_net_file_allocation_reduction_bytes": None}
    try:
        original, task, before = read_candidate(path)
        original_sha = hashlib.sha256(original).hexdigest()
        if before != candidate["identity"] or original_sha != candidate["sha256"] or task != candidate.get("task"):
            raise ValueError("legacy_audit_plan_source_changed")
        if shutil.disk_usage(path.parent).free < 256 * 1024 * 1024 + 3 * len(original):
            raise ValueError("legacy_audit_insufficient_restore_space")
        stored = gzip.compress(original, compresslevel=6, mtime=0)
        if len(stored) + 4096 >= before["allocated_bytes"]:
            outcome.update(status="retained", reason="no_allocation_benefit",
                           estimated_net_file_allocation_reduction_bytes=0)
            return outcome
        receipt = {"schema_version": 1, "codec": "gzip", "original_name": path.name,
                   "stored_name": stored_path.name, "original_bytes": len(original),
                   "stored_bytes": len(stored), "original_sha256": original_sha,
                   "stored_sha256": hashlib.sha256(stored).hexdigest(), "original_identity": before,
                   "task_identity": {key: task.get(key)
                                     for key in ("task_history_id", "task_name", "started_at", "status", "finished_at",
                                                 "source_audit_path", "source_audit_sha256")},
                   "created_at": datetime.now().astimezone().isoformat()}
        if receipt_identity_key == "audit_identity":
            receipt.pop("task_identity")
            receipt["audit_identity"] = task
        _install(stored_path, stored, before)
        _independent_restore(stored_path, original)
        outcome["independent_restore_verified"] = True
        if receipt_path.exists():
            verified, compressed, receipt = read_migrated(path)
            if verified != original or compressed != stored or receipt.get("original_identity") != before:
                raise ValueError("legacy_audit_existing_receipt_mismatch")
        else:
            _install(receipt_path, (json.dumps(receipt, ensure_ascii=False, indent=2) + "\n").encode(), before)
        if read_migrated(path)[0] != original:
            raise ValueError("legacy_audit_reader_verification_failed")
        _no_symlinks(path)
        verified_original, verified_task, verified_identity = read_candidate(path)
        if (verified_original, verified_task, verified_identity) != (original, task, before):
            raise ValueError("legacy_audit_terminal_source_changed_before_removal")
        if identity(path) != before or path.read_bytes() != original or identity(path) != before:
            raise ValueError("legacy_audit_source_changed_before_removal")
        path.unlink()
        outcome["removed"] = True
        _sync_directory(path.parent)
        if read_original_artifact_bytes(path) != original:
            raise ValueError("legacy_audit_original_path_read_failed")
        outcome.update(status="compressed", original_sha256=original_sha,
                       original_bytes=len(original), stored_bytes=len(stored),
                       estimated_net_file_allocation_reduction_bytes=(
                           before_allocation - _allocation((path, stored_path, receipt_path))))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        outcome["error"] = str(exc)
    return outcome


def restore_legacy_task_audit(path: Path, destination: Path) -> dict[str, Any]:
    return _restore_migrated_artifact(path, destination, read_migrated=read_migrated_task_audit)


def _restore_migrated_artifact(
    path: Path, destination: Path, *, read_migrated: Callable[[Path], tuple[bytes, bytes, dict]],
) -> dict[str, Any]:
    original, _, receipt = read_migrated(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _no_symlinks(destination)
    with destination.open("xb") as stream:
        stream.write(original)
        stream.flush()
        os.fsync(stream.fileno())
    if destination.read_bytes() != original:
        raise ValueError("legacy_audit_restored_file_mismatch")
    source = receipt["original_identity"]
    os.chmod(destination, stat.S_IMODE(source["mode"]))
    os.chown(destination, source["uid"], source["gid"])
    os.utime(destination, ns=(source["mtime_ns"], source["mtime_ns"]))
    _sync_directory(destination.parent)
    return {"path": str(destination), "sha256": receipt["original_sha256"],
            "bytes": len(original), "full_restore_verified": True}
