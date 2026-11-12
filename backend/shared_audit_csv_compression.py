"""Compress complete hardlink groups of closed global audit CSV attachments.

Every alias keeps its existing logical path and independently verified receipt.
All representations are delivered before any plain alias is removed. Partial
delivery/removal is rediscovered from the receipts without changing audit data.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time
from typing import Any

from backend.file_usage import assert_files_inactive
from backend.legacy_audit_csv_compression import MIN_AGE_SECONDS, MIN_FILE_BYTES, MAX_BATCH_FILES, MAX_BATCH_BYTES
from backend.legacy_task_audit_compression import _allocation, _independent_restore, _install, _sync_directory, identity
from backend.storage import WorkflowLockBusyError, WorkflowStore
from backend.task_audit_storage import (
    LEGACY_AUDIT_CSV_FAMILIES, MAX_RECEIPT_BYTES, MAX_TASK_AUDIT_BYTES, _no_symlinks,
    decode_task_audit_bytes, is_legacy_audit_csv_path, legacy_audit_csv_identity,
    migration_paths, read_migrated_audit_csv,
)

SCOPE = "shared_legacy_global_audit_csv_groups"
MAX_GROUP_FILES = 16
MAX_DIRECTORY_ENTRIES = 20000
MAX_PLAN_SECONDS = 30
MAX_APPLY_SECONDS = 120
_SOURCE_FIELDS = ("device", "inode", "size", "mtime_ns", "ctime_ns", "mode", "uid", "gid", "links", "allocated_bytes")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _paths(root: Path, names: list[str]) -> list[Path]:
    if (not isinstance(names, list) or not 2 <= len(names) <= MAX_GROUP_FILES
            or any(not isinstance(name, str) for name in names) or names != sorted(set(names))):
        raise ValueError("shared_csv_group_paths_invalid")
    paths = []
    for name in names:
        relative = Path(name)
        if (relative.is_absolute() or ".." in relative.parts or str(relative) != name or len(relative.parts) != 4
                or relative.parts[:2] != ("reports", "audit") or not is_legacy_audit_csv_path(relative)):
            raise ValueError("shared_csv_group_path_outside_scope")
        path = root / relative
        _no_symlinks(path)
        paths.append(path)
    return paths


def _read_bytes(path: Path, limit: int) -> bytes:
    _no_symlinks(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit or os.listxattr(stream.fileno()):
            raise ValueError("shared_csv_file_type_size_or_attributes_invalid")
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        if (len(raw) != info.st_size or len(raw) > limit
                or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink)):
            raise ValueError("shared_csv_file_changed_during_read")
    current = path.lstat()
    if ((current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns, current.st_nlink)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink)):
        raise ValueError("shared_csv_file_replaced_during_read")
    return raw


def _context(path: Path, now: float) -> dict:
    audit = legacy_audit_csv_identity(path)
    context = path.parent / audit["context_name"]
    if now - context.stat().st_mtime < MIN_AGE_SECONDS:
        raise ValueError("shared_csv_recent_context_protected")
    contexts = list(path.parent.glob(path.parent.name + "_*.json"))
    if context == max(contexts, key=lambda item: (item.stat().st_mtime_ns, item.name)):
        raise ValueError("shared_csv_latest_context_protected")
    return audit


def _read_group(root: Path, names: list[str], expected: dict | None = None) -> tuple[bytes, dict, dict, bytes | None]:
    paths = _paths(root, names)
    states, receipts, audits = {}, {}, {}
    source = expected.get("identity") if expected else None
    for name, path in zip(names, paths):
        stored, receipt_path = migration_paths(path)
        _no_symlinks(stored)
        _no_symlinks(receipt_path)
        if Path(str(path) + ".gz").exists() or Path(str(path) + ".gz").is_symlink():
            raise ValueError("shared_csv_native_counterpart_protected")
        states[name] = identity(path) if path.exists() else None
        if receipt_path.exists():
            receipt = json.loads(_read_bytes(receipt_path, MAX_RECEIPT_BYTES))
            if not isinstance(receipt, dict) or receipt.get("shared_source_paths") != names:
                raise ValueError("shared_csv_existing_receipt_outside_group")
            receipts[name] = receipt
            if source is None:
                source = receipt.get("original_identity")
        audits[name] = _context(path, time.time())
    present = [name for name in names if states[name] is not None]
    if source is None and present:
        source = states[present[0]]
    if (not isinstance(source, dict) or any(type(source.get(key)) is not int or source[key] < 0 for key in _SOURCE_FIELDS)
            or not stat.S_ISREG(source["mode"]) or source["links"] != len(names)
            or not MIN_FILE_BYTES <= source["size"] <= MAX_TASK_AUDIT_BYTES
            or time.time() - source["mtime_ns"] / 1e9 < MIN_AGE_SECONDS):
        raise ValueError("shared_csv_source_identity_or_age_invalid")
    for path in paths:
        for representation in migration_paths(path):
            if representation.exists():
                metadata = identity(representation)
                if any(metadata[key] != source[key] for key in ("mode", "uid", "gid")):
                    raise ValueError("shared_csv_representation_metadata_changed")
    for name in present:
        current = states[name]
        if (any(current[key] != source[key] for key in _SOURCE_FIELDS if key not in {"ctime_ns", "links"})
                or current["links"] != len(present) or (len(present) == len(names) and current != source)):
            raise ValueError("shared_csv_source_changed_or_external_link")
    raw = _read_bytes(root / present[0], MAX_TASK_AUDIT_BYTES) if present else None
    compressed = None
    for name, path in zip(names, paths):
        stored, receipt_path = migration_paths(path)
        if name in receipts:
            original, encoded, receipt = read_migrated_audit_csv(path)
            if (receipt != receipts[name] or receipt.get("original_identity") != source
                    or receipt.get("audit_identity") != audits[name]):
                raise ValueError("shared_csv_receipt_identity_mismatch")
            if raw is None:
                raw = original
            if original != raw:
                raise ValueError("shared_csv_receipt_bytes_mismatch")
        elif stored.exists():
            # A crash between data delivery and receipt delivery is recoverable
            # only while the unchanged plain source is still available.
            if name not in present or raw is None:
                raise ValueError("shared_csv_missing_original_and_receipt")
            encoded = _read_bytes(stored, MAX_TASK_AUDIT_BYTES)
            if decode_task_audit_bytes(encoded, compressed=True) != raw:
                raise ValueError("shared_csv_existing_gzip_conflicts")
        else:
            if name not in present:
                raise ValueError("shared_csv_missing_original_and_storage")
            continue
        if compressed is not None and encoded != compressed:
            raise ValueError("shared_csv_compressed_representations_differ")
        compressed = encoded
    if raw is None or len(raw) != source["size"]:
        raise ValueError("shared_csv_source_size_mismatch")
    digest = _sha(raw)
    for audit in audits.values():
        if "artifact_sha256" in audit and audit["artifact_sha256"] != digest:
            raise ValueError("shared_csv_context_artifact_sha_mismatch")
    group = {"paths": names, "identity": source, "sha256": digest, "audits": audits}
    if expected is not None and group != expected:
        raise ValueError("shared_csv_group_changed_since_plan")
    if any(identity(root / name) != states[name] for name in present):
        raise ValueError("shared_csv_source_changed_during_group_read")
    if any(legacy_audit_csv_identity(root / name) != audit for name, audit in audits.items()):
        raise ValueError("shared_csv_context_changed_during_group_read")
    return raw, group, states, compressed


def plan_shared_audit_csv_compression(root: Path, *, max_files: int = MAX_BATCH_FILES,
                                      max_input_bytes: int = MAX_BATCH_BYTES) -> dict[str, Any]:
    if (type(max_files) is not int or not 1 <= max_files <= MAX_BATCH_FILES
            or type(max_input_bytes) is not int or not 1 <= max_input_bytes <= MAX_BATCH_BYTES):
        raise ValueError("shared_csv_plan_budget_invalid")
    root, began = Path(root).resolve(), time.monotonic()
    result = {"schema_version": 1, "scope": SCOPE, "mode": "dry-run", "generated_at": datetime.now().astimezone().isoformat(),
              "scan_complete": True, "candidates": [], "candidate_count": 0, "candidate_group_count": 0,
              "candidate_logical_bytes": 0, "candidate_allocated_bytes": 0, "retained_samples": [], "errors": []}
    groups, pending, retained, scanned = {}, {}, Counter(), 0
    for family in sorted(LEGACY_AUDIT_CSV_FAMILIES):
        directory = root / "reports/audit" / family
        _no_symlinks(directory)
        if not directory.exists():
            continue
        with os.scandir(directory) as entries:
            for entry in entries:
                scanned += 1
                if scanned > MAX_DIRECTORY_ENTRIES or time.monotonic() - began > MAX_PLAN_SECONDS:
                    result.update(scan_complete=False, reason="shared_csv_scan_budget", directory_entries_scanned=scanned)
                    return result
                if not entry.name.endswith(".csv"):
                    continue
                path = Path(entry.path)
                try:
                    info = identity(path)
                    name = str(path.relative_to(root))
                    if stat.S_ISREG(info["mode"]) and info["links"] > 1:
                        groups.setdefault((info["device"], info["inode"]), []).append(name)
                    receipt_path = migration_paths(path)[1] if is_legacy_audit_csv_path(path) else None
                    if receipt_path and receipt_path.exists():
                        receipt = json.loads(_read_bytes(receipt_path, MAX_RECEIPT_BYTES))
                        names = receipt.get("shared_source_paths") if isinstance(receipt, dict) else None
                        if names is not None:
                            _paths(root, names)
                            if name not in names:
                                raise ValueError("shared_csv_pending_group_path_missing")
                            pending[tuple(names)] = names
                except (OSError, ValueError, TypeError) as error:
                    retained[str(error)] += 1
    groups_by_paths = {tuple(sorted(names)): sorted(names) for names in groups.values()}
    pending_members = {name for names in pending.values() for name in names}
    groups_by_paths = {key: names for key, names in groups_by_paths.items() if not set(names) & pending_members}
    groups_by_paths.update(pending)
    for names in sorted(groups_by_paths.values()):
        if time.monotonic() - began > MAX_PLAN_SECONDS:
            result.update(scan_complete=False, reason="shared_csv_plan_time_budget")
            break
        try:
            if result["candidate_count"] + len(names) > max_files:
                raise ValueError("shared_csv_batch_file_budget")
            raw, group, states, _ = _read_group(root, names)
            if result["candidate_logical_bytes"] + len(raw) > max_input_bytes:
                raise ValueError("shared_csv_batch_byte_budget")
            if not any(states.values()):
                continue
            result["candidates"].append(group)
            result["candidate_count"] += len(names)
            result["candidate_logical_bytes"] += len(raw)
            result["candidate_allocated_bytes"] += group["identity"]["allocated_bytes"]
        except (OSError, ValueError, TypeError, KeyError, EOFError) as error:
            retained[str(error)] += 1
            if len(result["retained_samples"]) < 64:
                result["retained_samples"].append({"paths": names, "reason": str(error)})
    result.update(candidate_group_count=len(result["candidates"]), retained_reasons=dict(retained),
                  directory_entries_scanned=scanned, elapsed_seconds=round(time.monotonic() - began, 3))
    return result


def _compress_group(root: Path, group: dict, *, deadline: float) -> dict:
    names = group["paths"]
    paths = _paths(root, names)
    family = tuple(path for original in paths for path in (original, *migration_paths(original)))
    before_allocation = _allocation(family)
    result = {"paths": names, "status": "failed", "removed_paths": [], "independent_restore_verified": False,
              "estimated_net_file_allocation_reduction_bytes": None, "net_reclaimed_disk_bytes": None}
    try:
        raw, _, states, compressed = _read_group(root, names, group)
        if not any(states.values()):
            result.update(status="already_stored", estimated_net_file_allocation_reduction_bytes=0)
            return result
        assert_files_inactive({name: value for name, value in states.items() if value})
        if shutil.disk_usage(paths[0].parent).free < 256 * 1024**2 + 3 * len(raw):
            raise ValueError("shared_csv_insufficient_restore_space")
        compressed = gzip.compress(raw, compresslevel=6, mtime=0) if compressed is None else compressed
        source = group["identity"]
        receipts = {}
        for path in paths:
            stored, receipt_path = migration_paths(path)
            metadata = {"schema_version": 1, "codec": "gzip", "original_name": path.name, "stored_name": stored.name,
                        "original_bytes": len(raw), "stored_bytes": len(compressed), "original_sha256": group["sha256"],
                        "stored_sha256": _sha(compressed), "original_identity": source,
                        "audit_identity": group["audits"][str(path.relative_to(root))], "shared_source_paths": names,
                        "created_at": datetime.now().astimezone().isoformat()}
            encoded = _read_bytes(receipt_path, MAX_RECEIPT_BYTES) if receipt_path.exists() else (
                json.dumps(metadata, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            if len(encoded) > MAX_RECEIPT_BYTES:
                raise ValueError("shared_csv_receipt_budget_exceeded")
            receipts[receipt_path] = encoded
        estimated_stored = ((len(compressed) + 4095) // 4096 + sum((len(raw) + 4095) // 4096 for raw in receipts.values())) * 4096
        if estimated_stored >= source["allocated_bytes"]:
            result.update(status="retained", reason="no_allocation_benefit", estimated_net_file_allocation_reduction_bytes=0)
            return result
        existing = [migration_paths(path)[0] for path in paths if migration_paths(path)[0].exists()]
        first = existing[0] if existing else migration_paths(paths[0])[0]
        _install(first, compressed, source)
        for path in paths:
            stored, _ = migration_paths(path)
            if not stored.exists():
                _no_symlinks(stored)
                os.link(first, stored, follow_symlinks=False)
                _sync_directory(stored.parent)
            if not stored.samefile(first) or _read_bytes(stored, MAX_TASK_AUDIT_BYTES) != compressed:
                raise ValueError("shared_csv_compressed_aliases_not_shared")
        _independent_restore(first, raw)
        result["independent_restore_verified"] = True
        for receipt_path, encoded in receipts.items():
            _install(receipt_path, encoded, source)
        _read_group(root, names, group)
        for name, path in zip(names, paths):
            if time.monotonic() > deadline:
                raise ValueError("shared_csv_apply_time_budget")
            _, _, current, _ = _read_group(root, names, group)
            present = {key: value for key, value in current.items() if value}
            if name not in present:
                continue
            stored_identities = {stored: identity(stored) for original in paths for stored in migration_paths(original)}
            assert_files_inactive(present)
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                pinned = os.fstat(descriptor)
                if (pinned.st_dev, pinned.st_ino) != (source["device"], source["inode"]) or identity(path) != current[name]:
                    raise ValueError("shared_csv_source_changed_before_unlink")
                if any(identity(stored) != before for stored, before in stored_identities.items()):
                    raise ValueError("shared_csv_storage_changed_before_unlink")
                if any(legacy_audit_csv_identity(root / member) != audit for member, audit in group["audits"].items()):
                    raise ValueError("shared_csv_context_changed_before_unlink")
                path.unlink()
                result["removed_paths"].append(name)
                if os.fstat(descriptor).st_nlink != len(present) - 1:
                    raise ValueError("shared_csv_links_changed_during_unlink")
            finally:
                os.close(descriptor)
            _sync_directory(path.parent)
        verified, _, states, _ = _read_group(root, names, group)
        if verified != raw or any(states.values()):
            raise ValueError("shared_csv_final_read_failed")
        result.update(status="compressed", original_sha256=group["sha256"], original_bytes=len(raw),
                      shared_compressed_bytes=len(compressed),
                      estimated_net_file_allocation_reduction_bytes=before_allocation - _allocation(family))
    except (OSError, ValueError, RuntimeError, TypeError, KeyError, EOFError) as error:
        result.update(error=f"{type(error).__name__}:{error}", partial_removal=bool(result["removed_paths"]))
    return result


def apply_shared_audit_csv_plan(root: Path, plan: dict) -> dict:
    """Caller holds WorkflowStore; every path, context and source is revalidated."""
    groups = plan.get("candidates")
    if (plan.get("schema_version") != 1 or plan.get("scope") != SCOPE or plan.get("mode") != "dry-run"
            or plan.get("scan_complete") is not True or not isinstance(groups, list)):
        raise ValueError("shared_csv_plan_invalid")
    root = Path(root).resolve()
    names, total = [], 0
    for group in groups:
        paths = _paths(root, group["paths"])
        names.extend(paths)
        size = group["identity"]["size"]
        if type(size) is not int or not MIN_FILE_BYTES <= size <= MAX_TASK_AUDIT_BYTES:
            raise ValueError("shared_csv_plan_size_invalid")
        total += size
    if len(names) > MAX_BATCH_FILES or len(names) != len(set(names)) or total > MAX_BATCH_BYTES:
        raise ValueError("shared_csv_plan_budget_or_duplicate_invalid")
    result = {"schema_version": 1, "scope": SCOPE, "mode": "apply", "status": "complete",
              "started_at": datetime.now().astimezone().isoformat(), "outcomes": [], "errors": [],
              "scan_complete": True, "candidate_count": len(names), "candidate_group_count": len(groups),
              "net_reclaimed_disk_bytes": None}
    deadline = time.monotonic() + MAX_APPLY_SECONDS
    for group in groups:
        if time.monotonic() > deadline:
            result.update(status="deferred", reason="shared_csv_apply_time_budget")
            break
        outcome = _compress_group(root, group, deadline=deadline)
        result["outcomes"].append(outcome)
        if outcome["status"] == "failed":
            result["errors"].append({"paths": group["paths"], "error": outcome["error"]})
            result["status"] = "failed"
            break
    result.update(compressed_group_count=sum(row["status"] == "compressed" for row in result["outcomes"]),
                  removed_count=sum(len(row["removed_paths"]) for row in result["outcomes"]),
                  completed_file_allocation_reduction_bytes=sum(row["estimated_net_file_allocation_reduction_bytes"]
                      for row in result["outcomes"] if row["status"] == "compressed"),
                  unprocessed_group_count=len(groups) - len(result["outcomes"]), finished_at=datetime.now().astimezone().isoformat())
    return result


def run_shared_audit_csv_compression(settings: Any, *, apply: bool = False) -> dict:
    plan = plan_shared_audit_csv_compression(settings.project_root)
    if not apply or not plan["scan_complete"] or not plan["candidates"]:
        return {**plan, "status": "preview" if not apply else "no_candidates" if plan["scan_complete"] else "deferred",
                "removed_count": 0, "compressed_group_count": 0, "completed_file_allocation_reduction_bytes": 0}
    store = WorkflowStore(settings.state_file)
    try:
        with store._lock:
            store._acquire_run_lock_locked("shared_audit_csv_compression")
        return apply_shared_audit_csv_plan(settings.project_root, plan)
    except WorkflowLockBusyError:
        return {**plan, "status": "deferred", "reason": "workflow_lock_busy", "removed_count": 0,
                "compressed_group_count": 0, "completed_file_allocation_reduction_bytes": 0}
    finally:
        store.close()
