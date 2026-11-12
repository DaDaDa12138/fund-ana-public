"""Preserve completed scheduler logs as verified gzip, with their old paths readable."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from typing import Any

from backend.legacy_task_audit_compression import (
    _compress_closed_artifact, _restore_migrated_artifact, identity,
)
from backend.task_audit_storage import (
    MAX_TASK_AUDIT_BYTES, TASK_LOG_NAME, _no_symlinks,
    decode_task_audit_bytes, is_task_log_path, read_migrated_task_log,
    read_original_artifact_bytes, task_log_io_lock, task_log_terminal_fields, task_log_timestamp,
)

MIN_LOG_BYTES = 256 * 1024
MAX_BATCH_FILES = 32
MAX_BATCH_BYTES = 128 * 1024 * 1024
MAX_DIRECTORY_ENTRIES = 100000
MAX_PLAN_SECONDS = 30


def _read_candidate(root: Path, path: Path, *, now: float, keep_days: int) -> tuple[bytes, dict, dict]:
    if path.parent != root / "logs/tasks" or not is_task_log_path(path):
        raise ValueError("task_log_path_outside_scope")
    _no_symlinks(path)
    before = identity(path)
    if not stat.S_ISREG(before["mode"]) or before["links"] != 1:
        raise ValueError("task_log_requires_unshared_regular_file")
    if not MIN_LOG_BYTES <= before["size"] <= MAX_TASK_AUDIT_BYTES:
        raise ValueError("task_log_size_outside_scope")
    if now - before["mtime_ns"] / 1_000_000_000 < keep_days * 86400:
        raise ValueError("task_log_recent_write_protected")
    original = read_original_artifact_bytes(path)
    terminal = task_log_terminal_fields(path, original)
    began, ended = (task_log_timestamp(terminal[key]) for key in ("started_at", "finished_at"))
    if ended < began or now - ended.timestamp() < keep_days * 86400:
        raise ValueError("task_log_terminal_time_protected")
    audit_path = root / terminal["source_audit_path"]
    _no_symlinks(audit_path)
    audit_bytes = read_original_artifact_bytes(audit_path)
    audit = json.loads(decode_task_audit_bytes(audit_bytes, compressed=audit_path.name.endswith(".json.gz")))
    if not isinstance(audit, dict):
        raise ValueError("task_log_terminal_audit_invalid")
    task, result = audit.get("task"), audit.get("task_result")
    if (not isinstance(task, dict) or not isinstance(result, dict)
            or type(task.get("task_history_id")) is not int or task["task_history_id"] != terminal["task_history_id"]
            or task.get("task_name") != terminal["task_name"] or task.get("status") != terminal["status"]
            or result.get("status") != terminal["status"]
            or task_log_timestamp(task.get("started_at")) != began
            or task_log_timestamp(task.get("finished_at")) != ended
            or audit.get("persistence_stage") not in (None, "completion_recorded")):
        raise ValueError("task_log_terminal_audit_identity_mismatch")
    if result.get("finished_at") is not None and task_log_timestamp(result["finished_at"]) != ended:
        raise ValueError("task_log_terminal_audit_time_mismatch")
    if identity(path) != before or read_original_artifact_bytes(audit_path) != audit_bytes:
        raise ValueError("task_log_source_changed_during_read")
    task_identity = {key: task.get(key) for key in (
        "task_history_id", "task_name", "started_at", "finished_at", "status")}
    task_identity.update(source_audit_path=audit_path.relative_to(root).as_posix(),
                         source_audit_sha256=hashlib.sha256(audit_bytes).hexdigest())
    return original, task_identity, before


def plan_task_log_compression(
    root: Path, *, keep_days: int = 14, max_files: int = MAX_BATCH_FILES,
    max_bytes: int = MAX_BATCH_BYTES, now: float | None = None,
) -> dict[str, Any]:
    if not 1 <= keep_days <= 3650 or not 1 <= max_files <= MAX_BATCH_FILES or not MIN_LOG_BYTES <= max_bytes <= MAX_BATCH_BYTES:
        raise ValueError("task_log_compression_policy_out_of_bounds")
    root = root.resolve()
    directory = root / "logs/tasks"
    _no_symlinks(directory)
    now = time.time() if now is None else now
    result = {
        "schema_version": 1, "generated_at": datetime.now().astimezone().isoformat(),
        "keep_plain_days": keep_days, "max_files": max_files, "max_bytes": max_bytes,
        "min_log_bytes": MIN_LOG_BYTES, "directory_entries_scanned": 0,
        "scan_complete": True, "candidates": [], "retained_samples": [],
        "candidate_logical_bytes": 0, "removed_count": 0, "net_reclaimed_disk_bytes": None,
    }
    retained = Counter()
    if not directory.exists():
        result.update(candidate_count=0, retained_reasons={})
        return result
    paths = []
    with os.scandir(directory) as entries:
        for entry in entries:
            result["directory_entries_scanned"] += 1
            if result["directory_entries_scanned"] > MAX_DIRECTORY_ENTRIES:
                result.update(scan_complete=False, reason="task_log_directory_scan_budget")
                result.update(candidate_count=0, retained_reasons={})
                return result
            if TASK_LOG_NAME.fullmatch(entry.name):
                paths.append(Path(entry.path))
    began = time.monotonic()
    for path in sorted(paths, key=lambda item: int(TASK_LOG_NAME.fullmatch(item.name)["task_id"])):
        if time.monotonic() - began > MAX_PLAN_SECONDS:
            result.update(scan_complete=False, reason="task_log_plan_time_budget")
            break
        relative = path.relative_to(root).as_posix()
        try:
            if len(result["candidates"]) >= max_files:
                retained["batch_file_budget"] += 1
                continue
            info = path.lstat()
            if not MIN_LOG_BYTES <= info.st_size <= MAX_TASK_AUDIT_BYTES:
                retained["task_log_size_outside_scope"] += 1
                continue
            if now - info.st_mtime < keep_days * 86400:
                retained["task_log_recent_write_protected"] += 1
                continue
            if result["candidate_logical_bytes"] + info.st_size > max_bytes:
                retained["batch_byte_budget"] += 1
                continue
            original, task, before = _read_candidate(root, path, now=now, keep_days=keep_days)
            result["candidates"].append({"path": relative, "sha256": hashlib.sha256(original).hexdigest(),
                                         "identity": before, "task": task})
            result["candidate_logical_bytes"] += len(original)
        except (OSError, ValueError, TypeError) as exc:
            reason = str(exc)
            retained[reason] += 1
            if len(result["retained_samples"]) < 64:
                result["retained_samples"].append({"path": relative, "reason": reason})
    result.update(candidate_count=len(result["candidates"]), retained_reasons=dict(retained))
    return result


def compress_task_log(root: Path, candidate: dict[str, Any], *, keep_days: int = 14) -> dict[str, Any]:
    if not 1 <= keep_days <= 3650:
        raise ValueError("task_log_compression_policy_out_of_bounds")
    root = root.resolve()
    try:
        with task_log_io_lock(root / "logs/tasks", blocking=False):
            return _compress_closed_artifact(
                root, candidate,
                read_candidate=lambda path: _read_candidate(root, path, now=time.time(), keep_days=keep_days),
                read_migrated=read_migrated_task_log,
            )
    except BlockingIOError:
        return {"path": candidate["path"], "status": "retained", "reason": "task_log_writer_busy",
                "removed": False, "independent_restore_verified": False,
                "estimated_net_file_allocation_reduction_bytes": 0, "net_reclaimed_disk_bytes": None}


def run_task_log_compression(root: Path, *, keep_days: int = 14, apply: bool = False) -> dict[str, Any]:
    result = plan_task_log_compression(root, keep_days=keep_days)
    result.update(dry_run=not apply, outcomes=[], compressed_count=0,
                  completed_file_allocation_reduction_bytes=0, errors=[])
    if apply:
        for candidate in result["candidates"]:
            outcome = compress_task_log(root, candidate, keep_days=keep_days)
            result["outcomes"].append(outcome)
            if outcome["status"] == "compressed":
                result["compressed_count"] += 1
                result["completed_file_allocation_reduction_bytes"] += outcome["estimated_net_file_allocation_reduction_bytes"]
            elif outcome["status"] == "failed":
                result["errors"].append({"path": candidate["path"], "error": outcome.get("error")})
    result["removed_count"] = sum(bool(row.get("removed")) for row in result["outcomes"])
    result["scope"] = ("Completed scheduler task logs only; retain full original bytes, terminal identity and old logical paths. "
                       "A verified independent restoration precedes removal of the plain representation. "
                       "Compressed history is preserved; this is not a total-history size cap or deletion of old failures.")
    return result


def restore_task_log(path: Path, destination: Path) -> dict[str, Any]:
    return _restore_migrated_artifact(path, destination, read_migrated=read_migrated_task_log)
