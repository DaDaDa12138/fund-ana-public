"""Retire completed, superseded cleanup backups with current recovery proof.

Legacy, failed, unique migration and paired backups are outside automatic scope.
The caller invokes this after this week's task and screening cleanup completes.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
from typing import Any, Callable, Iterator

from backend.maintenance_backup_recovery import RecoveryAudit, verify_screening_recovery, verify_task_recovery
from backend.storage import WorkflowLockBusyError, WorkflowStore
from utils.verified_backup_compression import BACKUP_METADATA_RESERVE_BYTES, _atomic_json, _fsync_directory


PURPOSES = {"screening_history_cleanup", "task_history_cleanup"}
DIAGNOSTIC_SECTIONS = {
    "database_backup_cleanup_preview", "database_backup_cleanup_execution",
    "database_backup_compression_preview", "database_backup_compression_execution",
    "database_governance_health_audit", "database_optimization_completion_audit",
    "screening_history_cleanup_execution", "task_history_cleanup_execution", "maintenance_backup_retention",
}


def summarize_maintenance_backup_retention(result: dict[str, Any]) -> dict[str, Any]:
    """Keep detailed recovery identities only in the bound audit artifact."""
    return {key: result[key] for key in (
        "schema_version", "mode", "status", "started_at", "finished_at", "retention_as_of", "artifact_path",
        "policy", "deleted_file_count", "deleted_allocated_bytes", "candidate_file_count", "retained_file_count",
        "verification_blocked_count", "recovery_scope", "verification_manifests_retained", "skip_reason", "error",
    ) if key in result}


def _date(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    return result.astimezone()


def _file_identity(path: Path) -> dict:
    s = path.lstat()
    if path.is_symlink() or not path.is_file() or s.st_nlink != 1:
        raise ValueError("backup_must_be_regular_with_single_link")
    return {"device": s.st_dev, "inode": s.st_ino, "size_bytes": s.st_size,
            "mtime_ns": s.st_mtime_ns, "ctime_ns": s.st_ctime_ns, "allocated_bytes": s.st_blocks * 512}


def _manifest_metadata(path: Path, root: Path, source: Path) -> dict:
    manifest = path.with_suffix(path.suffix + ".manifest.json")
    audit = RecoveryAudit(root)
    payload = audit.read_json(manifest)
    snapshot = payload.get("snapshot") or {}
    purpose = snapshot.get("purpose")
    if purpose not in PURPOSES:
        raise ValueError("backup_purpose_outside_automatic_cleanup_scope")
    if (payload.get("schema_version") != 1 or snapshot.get("schema_version") != 1
            or snapshot.get("method") != "sqlite_online_backup"
            or snapshot.get("backup_label") != path.name.removesuffix(".db.zst")
            or audit.path(snapshot.get("source_database_path")) != source.resolve()
            or audit.path(payload.get("compressed_path")) != path.resolve()
            or audit.path(payload.get("source_path")) != path.with_suffix("").resolve()
            or payload.get("verification") != "full_decoded_bytes_equal_unchanged_offline_SQLite"
            or payload.get("sqlite_quick_check") != "ok" or payload.get("source_removed") is not True
            or payload.get("source_sha256") != payload.get("decoded_sha256")
            or payload.get("source_size_bytes") != payload.get("decoded_size_bytes")
            or type(payload.get("source_size_bytes")) is not int or payload["source_size_bytes"] <= 0
            or not all(re.fullmatch(r"[0-9a-f]{64}", str(payload.get(key) or ""))
                       for key in ("compressed_sha256", "source_sha256"))):
        raise ValueError("backup_complete_snapshot_manifest_required")
    raw = path.with_suffix("")
    if any(p.exists() for p in [raw, Path(f"{raw}-wal"), Path(f"{raw}-shm"), Path(f"{raw}-journal")]):
        raise ValueError("backup_has_uncompressed_or_journal_peer")
    identity = _file_identity(path)
    if identity["size_bytes"] != payload.get("compressed_size_bytes"):
        raise ValueError("backup_compressed_length_mismatch")
    return {"purpose": purpose, "snapshot_at": _date(snapshot["read_transaction_observed_at"]).isoformat(),
            "manifest_path": str(manifest), "manifest_sha256": audit.files[str(manifest)]["sha256"],
            "sha256": payload["compressed_sha256"], "identity": identity}


def _execution_for_backup(path: Path, purpose: str, reports_dir: Path, audit: RecoveryAudit) -> tuple[Path, dict]:
    pattern = purpose + "_execution_*.json"
    folder = reports_dir / "audit" / purpose
    matches = []
    for candidate in sorted(folder.glob(pattern)):
        audit.check_budget()
        # Read only the producer's actual execution records, never previews or
        # synthetic test workspaces. Malformed execution history prevents reuse.
        candidate = audit.path(candidate)
        if candidate.stat().st_size > 64 * 1024 * 1024:
            raise ValueError("cleanup_execution_exceeds_metadata_budget")
        raw = candidate.read_bytes()
        if path.name.encode() not in raw and path.with_suffix("").name.encode() not in raw:
            continue
        payload = audit.read_json(candidate, hashlib.sha256(raw).hexdigest())
        binding = ((payload.get("backup") or {}).get("backup_artifact_path")
                   if purpose == "task_history_cleanup" else (payload.get("parameters") or {}).get("backup_file"))
        if not binding or audit.path(binding) != path.resolve() or payload.get("mode") != "apply":
            continue
        matches.append((candidate, payload))
    if len(matches) != 1:
        raise ValueError("unique_completed_cleanup_execution_required")
    return matches[0]


def _active_references(settings: Any, path: Path, audit: RecoveryAudit) -> list[dict]:
    blocked = []
    tokens = (path.name, path.with_suffix("").name)

    def walk(value: Any, source: str, keys: tuple[str, ...] = ()) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key not in DIAGNOSTIC_SECTIONS:
                    walk(item, source, (*keys, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, source, (*keys, str(index)))
        elif isinstance(value, str) and any(token in value for token in tokens):
            blocked.append({"file": source, "key": ".".join(keys)})

    for candidate in sorted(settings.database_path.parent.glob("*.json")):
        # WorkflowStore is held during apply; scheduler diagnostics can change
        # without creating an active recovery dependency. They are inspected,
        # not added as immutable archive files.
        payload = json.loads(candidate.read_text())
        walk(payload, str(candidate))
    for candidate in (settings.latest_report_file, settings.publication_registry_file, settings.config_path):
        if not candidate.is_file():
            raise ValueError("backup_retention_runtime_control_missing")
        raw = candidate.read_text()
        if any(token in raw for token in tokens):
            blocked.append({"file": str(candidate), "key": "document"})
    return blocked


def _process_file_descriptors() -> Iterator[Path]:
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            continue
        yield from descriptors


def _assert_not_open(path: Path) -> None:
    info = path.stat()
    wanted = info.st_dev, info.st_ino
    for descriptor in _process_file_descriptors():
        try:
            current = descriptor.stat()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if (current.st_dev, current.st_ino) == wanted:
            raise ValueError("backup_is_open")


def _retire_file(path: Path, expected: dict, audit: RecoveryAudit, reference_check: Callable[[], list[dict]]) -> None:
    if _file_identity(path) != expected:
        raise ValueError("backup_changed_before_retirement")
    _assert_not_open(path)
    if reference_check():
        raise ValueError("active_reference_changed_before_unlink")
    audit.assert_stable()
    path.unlink()


def run_maintenance_backup_retention(
    settings: Any, *, mode: str = "dry-run", now: datetime | None = None,
    log: Callable[[str], None] = lambda message: None,
) -> dict[str, Any]:
    if mode not in {"dry-run", "apply"}:
        raise ValueError("unsupported_backup_retention_mode")
    current = now.astimezone() if now else datetime.now().astimezone()
    keep = max(1, int(getattr(settings, "scheduler_weekly_maintenance_backup_retention_keep_per_purpose", 1)))
    min_days = max(1, int(getattr(settings, "scheduler_weekly_maintenance_backup_retention_min_age_days", 14)))
    limit = max(1, int(getattr(settings, "scheduler_weekly_maintenance_backup_retention_max_delete_files", 2)))
    root = settings.project_root.resolve()
    directory = settings.database_path.parent / "backups"
    output = settings.reports_dir / "audit" / "database_backup_retention"
    output.mkdir(parents=True, exist_ok=True)
    artifact = output / f"database_backup_retention_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
    result: dict[str, Any] = {"schema_version": 1, "mode": mode, "status": "running",
        "started_at": datetime.now().astimezone().isoformat(), "retention_as_of": current.isoformat(), "artifact_path": str(artifact),
        "policy": {"purposes": sorted(PURPOSES), "keep_per_purpose": keep, "min_age_days": min_days, "max_delete_files": limit},
        "files": [], "deleted_file_count": 0, "deleted_allocated_bytes": 0, "candidate_file_count": 0,
        "retained_file_count": 0, "verification_blocked_count": 0,
        "recovery_scope": "Complete deleted database values and task full-payload sidecars; arbitrary log/report dependencies are not included in a database backup.",
        "verification_manifests_retained": True}
    if not directory.exists():
        result.update(status="skipped", skip_reason="backup_directory_missing")
        _atomic_json(artifact, result)
        return result
    audit = RecoveryAudit(root)
    store = WorkflowStore(settings.state_file)
    acquired = False
    try:
        with ExitStack() as stack:
            if mode == "apply":
                with store._lock:
                    store._acquire_run_lock_locked("maintenance_backup_retention")
                    acquired = True
                for lock_name in (".backup_creation.lock", ".backup_compression.lock"):
                    handle = stack.enter_context((directory / lock_name).open("a+"))
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if shutil.disk_usage(output).free < BACKUP_METADATA_RESERVE_BYTES:
                raise ValueError("insufficient_space_for_retention_evidence")
            families = defaultdict(list)
            for path in sorted(directory.glob("*.db.zst")):
                item = {"path": str(path), "status": "retained", "reason": "legacy_or_unmanaged_recovery_point"}
                result["files"].append(item)
                try:
                    item.update(_manifest_metadata(path, root, settings.database_path))
                    families[item["purpose"]].append(item)
                except (ValueError, OSError, KeyError, TypeError) as exc:
                    item["reason"] = str(exc)
                    if (path.with_suffix(path.suffix + ".manifest.json").exists()
                            and str(exc) != "backup_purpose_outside_automatic_cleanup_scope"):
                        result["verification_blocked_count"] += 1
            protected = {}
            candidates = []
            for purpose, family in families.items():
                family.sort(key=lambda row: (row["snapshot_at"], row["path"]), reverse=True)
                protected[purpose] = family[:keep]
                for item in family[:keep]:
                    item["reason"] = "latest_backup_for_purpose"
                for item in family[keep:]:
                    if _date(item["snapshot_at"]) > current - timedelta(days=min_days):
                        item["reason"] = "within_minimum_retention_age"
                    else:
                        candidates.append(item)
            candidates.sort(key=lambda row: (row["snapshot_at"], row["path"]))
            for item in candidates[limit:]:
                item["reason"] = "per_run_retirement_limit"
            candidates = candidates[:limit]
            _atomic_json(artifact, result)
            for item in candidates:
                path = Path(item["path"])
                try:
                    checkpoint_files = dict(audit.files)
                    checkpoint_links = dict(audit.control_links)
                    checkpoint_migrated = set(audit.migrated_originals)
                    audit.bind(item["manifest_path"], item["manifest_sha256"])
                    audit.bind(path, item["sha256"])
                    for replacement in protected[item["purpose"]]:
                        audit.bind(replacement["manifest_path"], replacement["manifest_sha256"])
                        audit.bind(replacement["path"], replacement["sha256"])
                        if _file_identity(Path(replacement["path"])) != replacement["identity"]:
                            raise ValueError("replacement_backup_changed")
                    producer, execution = _execution_for_backup(path, item["purpose"], settings.reports_dir, audit)
                    item["cleanup_execution_path"] = str(producer)
                    item["recovery"] = (verify_task_recovery(execution, audit) if item["purpose"] == "task_history_cleanup"
                        else verify_screening_recovery(execution, audit,
                             audit.path(settings.scheduler_weekly_screening_history_archive_execution_archive_target_path)))
                    references = _active_references(settings, path, audit)
                    if references:
                        item["active_references"] = references
                        raise ValueError("active_recovery_reference_requires_review")
                    for control in (settings.latest_report_file, settings.publication_registry_file, settings.config_path):
                        audit.bind_control(control)
                    item["verified_files"] = dict(audit.files)
                    item["verified_control_links"] = dict(audit.control_links)
                    item.update(status="candidate", reason="completed_cleanup_with_reverified_values_and_newer_recovery_point")
                    result["candidate_file_count"] += 1
                    _atomic_json(artifact, result)
                    audit.assert_stable()
                    if mode == "apply":
                        if _active_references(settings, path, audit):
                            raise ValueError("recovery_reference_changed_before_retirement")
                        audit.assert_stable()
                        item["status"] = "retiring"
                        _atomic_json(artifact, result)
                        try:
                            _retire_file(path, item["identity"], audit, lambda: _active_references(settings, path, audit))
                        finally:
                            # Interruption or fsync failure after unlink cannot
                            # turn an actually removed file into a retained one.
                            if not path.exists() and not path.is_symlink():
                                item.update(status="deleted", deleted_at=datetime.now().astimezone().isoformat(),
                                            unlink_outcome="absent_after_retirement_attempt")
                                result["deleted_file_count"] += 1
                                result["deleted_allocated_bytes"] += item["identity"]["allocated_bytes"]
                        _fsync_directory(path.parent)
                        # The deleted input is intentionally absent; every
                        # replacement and archive must remain unchanged.
                        audit.files.pop(str(path))
                        audit.assert_stable()
                    log(f"维护备份保留: {path.name}; {item['status']}; {item['reason']}")
                except (ValueError, OSError, KeyError, TypeError, sqlite3.Error) as exc:
                    if item["status"] == "deleted":
                        # A failure after unlink must remain an actual deletion
                        # in the receipt, and must stop any subsequent removal.
                        raise
                    if item["status"] != "deleted":
                        item.update(status="retained", reason="recovery_verification_blocked", error=f"{type(exc).__name__}: {exc}")
                    result["verification_blocked_count"] += 1
                    log(f"维护备份保留检查未通过: {path.name}; {exc}")
                    audit.files = checkpoint_files
                    audit.control_links = checkpoint_links
                    audit.migrated_originals = checkpoint_migrated
                _atomic_json(artifact, result)
            result["retained_file_count"] = sum(item["status"] == "retained" for item in result["files"])
            result["status"] = "degraded" if result["verification_blocked_count"] else ("applied" if mode == "apply" else "dry-run")
    except (WorkflowLockBusyError, BlockingIOError):
        result.update(status="deferred", skip_reason="workflow_or_backup_lock_busy")
    except BaseException as exc:
        result.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        if not isinstance(exc, Exception):
            raise
    finally:
        result["retained_file_count"] = sum(item["status"] == "retained" for item in result["files"])
        if acquired:
            with store._lock:
                store._release_run_lock_locked()
        result["finished_at"] = datetime.now().astimezone().isoformat()
        _atomic_json(artifact, result)
    log(f"维护备份自动保留完成: {result['status']}; 删除{result['deleted_file_count']}份; 实际分配字节{result['deleted_allocated_bytes']}")
    return result
