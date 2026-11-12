"""Bounded lossless migration of immutable global maintenance audit JSON.

The newest artifact in each family, recent writes, shared files, mutable state,
and run-local publication artifacts are protected. No task success is inferred
from a storage operation; old statuses and bytes are retained verbatim.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
import stat
import time
from typing import Any

from backend.legacy_task_audit_compression import (
    _compress_closed_artifact, _no_symlinks, _restore_migrated_artifact, identity,
)
from backend.task_audit_storage import (
    MAINTENANCE_AUDIT_FAMILIES, MAINTENANCE_AUDIT_PREFIXES, glob_json_artifacts, is_maintenance_audit_path,
    maintenance_audit_identity, read_migrated_maintenance_audit, read_original_artifact_bytes,
)

MIN_AGE_SECONDS = 24 * 3600
MIN_FILE_BYTES = 32 * 1024
MAX_BATCH_FILES = 256
MAX_BATCH_BYTES = 128 * 1024 * 1024
SCOPE = "timestamped_global_maintenance_audit_json"


def _read_candidate(path: Path, *, now: float) -> tuple[bytes, dict, dict]:
    if not is_maintenance_audit_path(path):
        raise ValueError("maintenance_audit_path_outside_scope")
    _no_symlinks(path)
    before = identity(path)
    if not stat.S_ISREG(before["mode"]) or before["links"] != 1:
        raise ValueError("maintenance_audit_unshared_regular_file_required")
    if before["size"] < MIN_FILE_BYTES:
        raise ValueError("maintenance_audit_small_file_protected")
    if now - before["mtime_ns"] / 1_000_000_000 < MIN_AGE_SECONDS:
        raise ValueError("maintenance_audit_recent_write_protected")
    family = path.parent.name
    prefix = MAINTENANCE_AUDIT_PREFIXES[family]
    timestamp = path.name[len(prefix) + 1:len(prefix) + 16]
    datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
    paths = glob_json_artifacts(path.parent, f"{prefix}_*.json")
    if paths and path == paths[-1]:
        raise ValueError("maintenance_audit_latest_protected")
    original = read_original_artifact_bytes(path)
    audit = maintenance_audit_identity(path, original)
    if identity(path) != before:
        raise ValueError("maintenance_audit_changed_during_read")
    return original, audit, before


def plan_maintenance_audit_compression(
    root: Path, *, max_files: int = 32, max_input_bytes: int = MAX_BATCH_BYTES,
) -> dict[str, Any]:
    if type(max_files) is not int or not 1 <= max_files <= MAX_BATCH_FILES:
        raise ValueError("maintenance_audit_file_budget_invalid")
    if type(max_input_bytes) is not int or not 1 <= max_input_bytes <= MAX_BATCH_BYTES:
        raise ValueError("maintenance_audit_byte_budget_invalid")
    root = root.resolve()
    now = time.time()
    candidates, retained = [], []
    total = 0
    for family in sorted(MAINTENANCE_AUDIT_FAMILIES):
        directory = root / "reports/audit" / family
        _no_symlinks(directory)
        for path in sorted(directory.glob("*.json")):
            relative = str(path.relative_to(root))
            if len(candidates) >= max_files:
                retained.append({"path": relative, "reason": "batch_file_budget"})
                continue
            try:
                if total + path.lstat().st_size > max_input_bytes:
                    retained.append({"path": relative, "reason": "batch_byte_budget"})
                    continue
                original, audit, before = _read_candidate(path, now=now)
                candidates.append({"path": relative, "sha256": hashlib.sha256(original).hexdigest(),
                                   "identity": before, "audit": audit})
                total += len(original)
            except (OSError, ValueError, TypeError) as exc:
                retained.append({"path": relative, "reason": str(exc)})
    return {"schema_version": 1, "scope": SCOPE, "mode": "dry-run",
            "generated_at": datetime.now().astimezone().isoformat(),
            "min_age_seconds": MIN_AGE_SECONDS, "min_file_bytes": MIN_FILE_BYTES,
            "max_files": max_files, "max_input_bytes": max_input_bytes,
            "candidates": candidates, "retained": retained,
            "candidate_count": len(candidates), "retained_count": len(retained),
            "candidate_logical_bytes": total,
            "candidate_allocated_bytes": sum(row["identity"]["allocated_bytes"] for row in candidates),
            "removed_count": 0, "net_reclaimed_disk_bytes": None}


def compress_maintenance_audit(root: Path, candidate: dict[str, Any]) -> dict[str, Any]:
    path = Path(candidate["path"])
    if (path.is_absolute() or ".." in path.parts or len(path.parts) != 4
            or path.parts[:2] != ("reports", "audit") or not is_maintenance_audit_path(path)):
        raise ValueError("maintenance_audit_candidate_outside_project_scope")
    bound_candidate = {**candidate, "task": candidate["audit"]}
    return _compress_closed_artifact(
        root, bound_candidate, read_candidate=lambda path: _read_candidate(path, now=time.time()),
        read_migrated=read_migrated_maintenance_audit, receipt_identity_key="audit_identity",
    )


def apply_maintenance_audit_plan(root: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """The operator holds the workflow writer lock for this exact preview."""
    candidates = plan.get("candidates")
    if (plan.get("schema_version") != 1 or plan.get("scope") != SCOPE or plan.get("mode") != "dry-run"
            or not isinstance(candidates, list) or len(candidates) > MAX_BATCH_FILES):
        raise ValueError("maintenance_audit_plan_invalid")
    paths = []
    total = 0
    for row in candidates:
        path = Path(row["path"])
        size = row["identity"]["size"]
        if (path.is_absolute() or ".." in path.parts or len(path.parts) != 4
                or path.parts[:2] != ("reports", "audit") or not is_maintenance_audit_path(path)
                or type(size) is not int or size < MIN_FILE_BYTES):
            raise ValueError("maintenance_audit_plan_scope_invalid")
        paths.append(path)
        total += size
    if len(paths) != len(set(paths)) or total > MAX_BATCH_BYTES:
        raise ValueError("maintenance_audit_plan_budget_or_duplicate_invalid")
    result = {"schema_version": 1, "scope": SCOPE, "mode": "apply",
              "started_at": datetime.now().astimezone().isoformat(),
              "planned_count": len(candidates), "files": [], "net_reclaimed_disk_bytes": None}
    for candidate in candidates:
        outcome = compress_maintenance_audit(root, candidate)
        result["files"].append(outcome)
        if outcome["status"] == "failed":
            break
    rows = result["files"]
    failed = sum(row["status"] == "failed" for row in rows)
    result.update(finished_at=datetime.now().astimezone().isoformat(),
                  removed_count=sum(row["removed"] for row in rows), failed_count=failed,
                  compressed_count=sum(row["status"] == "compressed" for row in rows),
                  retained_count=sum(row["status"] == "retained" for row in rows),
                  unprocessed_count=len(candidates) - len(rows),
                  estimated_net_file_allocation_reduction_bytes=(None if failed else sum(
                      row["estimated_net_file_allocation_reduction_bytes"] for row in rows)))
    return result


def restore_maintenance_audit(path: Path, destination: Path) -> dict[str, Any]:
    return _restore_migrated_artifact(path, destination, read_migrated=read_migrated_maintenance_audit)
