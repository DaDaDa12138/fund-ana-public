"""Bounded lossless storage for closed, dated core CSV outputs.

The writer's existing stats/sidecar proves that CSV serialization closed. It
does not prove that the fund data, screening run or publication was successful.
No business file, sidecar, database value or historical outcome is rewritten.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time
from typing import Any, Callable
from uuid import uuid4

from backend.storage import WorkflowLockBusyError, WorkflowStore
from core.utils.core_csv_storage import (
    FORMAT, MAX_CSV_BYTES, MAX_RECEIPT_BYTES, RESTORE_RESERVE_BYTES,
    CoreCsvStorageError, _read_regular, csv_storage_paths, file_identity,
    has_stored_csv, materialize_stored_csv, read_stored_csv, sync_directory,
)

MIN_CSV_BYTES = 32 * 1024
MAX_BATCH_FILES = 256
MAX_BATCH_BYTES = 512 * 1024**2
MAX_DIRECTORY_ENTRIES = 100000
MAX_PLAN_SECONDS = 30
MAX_APPLY_SECONDS = 120
MAX_CLOSE_EVIDENCE_BYTES = 4 * 1024**2
MIN_KEEP_DAYS = 30
_STAMP = r"(?P<stamp>20\d{6}_\d{6})"
_PERIOD = re.compile(
    r"^债券(?:型)?基金_(?:一周期|一月期|三月期|半年期|[1-5]年期)_"
    r"(?:达标全量|Top)\d+_" + _STAMP + r"(?P<simple>_simple)?\.csv$"
)
_OBSERVATION = re.compile(
    r"^债券(?:型)?基金_(?:一周期|一月期|三月期|半年期)_未满1Y观察_\d+_" + _STAMP + r"\.csv$"
)
_MERGER = re.compile(
    r"^(?P<prefix>长期优质基金|近期优质基金|债券(?:型)?基金_多周期交集_优选)_" + _STAMP + r"\.csv$"
)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _directory(settings) -> Path:
    root = Path(settings.project_root).resolve()
    directory = Path(getattr(settings, "core_output_dir", root / "reports/core_output")).absolute()
    if not directory.is_relative_to(root):
        raise ValueError("core_csv_directory_outside_project")
    if any(item.is_symlink() for item in (directory, *directory.parents)):
        raise ValueError("core_csv_directory_symlink")
    return directory


def _policy(keep_days: int, max_files: int, max_bytes: int) -> None:
    if (not MIN_KEEP_DAYS <= keep_days <= 3650 or not 1 <= max_files <= MAX_BATCH_FILES
            or not MIN_CSV_BYTES <= max_bytes <= MAX_BATCH_BYTES):
        raise ValueError("core_csv_compression_policy_out_of_bounds")


def _closed_output(path: Path, info: os.stat_result, *, cutoff: float) -> dict[str, Any]:
    """Bind only known writers whose metadata is emitted after CSV close."""
    period, observation, merger = (_PERIOD.fullmatch(path.name), _OBSERVATION.fullmatch(path.name),
                                   _MERGER.fullmatch(path.name))
    match = period or observation or merger
    if match is None:
        raise ValueError("core_csv_writer_outside_verified_scope")
    if datetime.strptime(match["stamp"], "%Y%m%d_%H%M%S").timestamp() >= cutoff:
        raise ValueError("core_csv_filename_date_protected")
    if period:
        basename = path.stem.removesuffix("_simple")
        evidence_path = path.with_name(basename + "_stats.json")
        binding_key = "simple_csv" if period["simple"] else "csv"
    elif observation:
        evidence_path = path.with_name(path.stem + "_report_meta.json")
        binding_key = "report_file"
    else:
        prefix = merger["prefix"].replace("_多周期交集_优选", "_多周期交集")
        evidence_path = path.with_name(f"{prefix}_详细报告_{merger['stamp']}_report_meta.json")
        binding_key = "csv_file"
    raw, evidence_stat = _read_regular(evidence_path, MAX_CLOSE_EVIDENCE_BYTES)
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("core_csv_close_evidence_invalid")
    binding = (payload.get("output_files") or {}).get(binding_key) if period else payload.get(binding_key)
    if not isinstance(binding, str) or Path(binding).name != path.name:
        raise ValueError("core_csv_close_evidence_path_mismatch")
    if period and not str(payload.get("run_id") or "").strip():
        raise ValueError("core_csv_close_evidence_run_missing")
    if observation and payload.get("artifact_type") != "bond_short_term_one_year_observation_report":
        raise ValueError("core_csv_close_evidence_kind_mismatch")
    if merger and payload.get("artifact_type") != "multi_period_merger_report":
        raise ValueError("core_csv_close_evidence_kind_mismatch")
    generated = datetime.fromisoformat(str(payload.get("generated_at") or "")).timestamp()
    if (generated >= cutoff or evidence_stat.st_mtime >= cutoff
            or evidence_stat.st_mtime_ns < info.st_mtime_ns):
        raise ValueError("core_csv_close_evidence_time_protected")
    return {"path": evidence_path.name, "sha256": _sha(raw), "identity": list(file_identity(evidence_stat)),
            "boundary": "metadata_written_after_csv_closed", "business_outcome_verified": False}


def _candidate(path: Path, *, now: float, keep_days: int) -> tuple[bytes, os.stat_result, dict]:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise ValueError("core_csv_requires_unshared_regular_file")
    if not MIN_CSV_BYTES <= before.st_size <= MAX_CSV_BYTES:
        raise ValueError("core_csv_size_outside_scope")
    cutoff = now - keep_days * 86400
    if before.st_mtime >= cutoff:
        raise ValueError("core_csv_recent_write_protected")
    if os.listxattr(path, follow_symlinks=False):
        raise ValueError("core_csv_extended_attributes_protected")
    evidence = _closed_output(path, before, cutoff=cutoff)
    raw, read_stat = _read_regular(path, MAX_CSV_BYTES)
    if file_identity(read_stat) != file_identity(before):
        raise ValueError("core_csv_source_changed_during_plan")
    return raw, before, evidence


def plan_core_csv_compression(settings, *, keep_days: int = 30, max_files: int = MAX_BATCH_FILES,
                              max_bytes: int = MAX_BATCH_BYTES, now: float | None = None) -> dict[str, Any]:
    _policy(keep_days, max_files, max_bytes)
    directory = _directory(settings)
    now = time.time() if now is None else now
    result = {"schema_version": 1, "directory": str(directory),
              "generated_at": datetime.now().astimezone().isoformat(), "keep_plain_days": keep_days,
              "max_files": max_files, "max_bytes": max_bytes, "min_csv_bytes": MIN_CSV_BYTES,
              "scan_complete": True, "directory_entries_scanned": 0, "candidates": [],
              "candidate_logical_bytes": 0, "retained_samples": [], "stored_count": 0}
    retained: Counter = Counter()
    paths: list[tuple[float, Path]] = []
    began = time.monotonic()
    if directory.exists():
        with os.scandir(directory) as entries:
            for entry in entries:
                result["directory_entries_scanned"] += 1
                if result["directory_entries_scanned"] > MAX_DIRECTORY_ENTRIES or time.monotonic() - began > MAX_PLAN_SECONDS:
                    result.update(scan_complete=False, reason="core_csv_directory_scan_budget")
                    paths = []
                    break
                if entry.name.endswith(".csv.zst.receipt.json"):
                    result["stored_count"] += 1
                if entry.name.endswith(".csv"):
                    try:
                        info = entry.stat(follow_symlinks=False)
                        paths.append((info.st_mtime, Path(entry.path)))
                    except OSError:
                        retained["core_csv_directory_entry_changed"] += 1
    for _, path in sorted(paths, key=lambda item: (item[0], item[1].name)):
        if time.monotonic() - began > MAX_PLAN_SECONDS:
            result.update(scan_complete=False, reason="core_csv_plan_time_budget")
            break
        if len(result["candidates"]) >= max_files:
            retained["batch_file_budget"] += 1
            continue
        try:
            if result["candidate_logical_bytes"] + path.lstat().st_size > max_bytes:
                retained["batch_byte_budget"] += 1
                continue
            raw, info, evidence = _candidate(path, now=now, keep_days=keep_days)
            if has_stored_csv(path):
                previous, metadata = read_stored_csv(path)
                if previous != raw or metadata["original_identity"].get("file_identity") != list(file_identity(info)):
                    raise ValueError("core_csv_storage_conflicts_with_plain_source")
            result["candidates"].append({"name": path.name, "identity": list(file_identity(info)),
                                         "sha256": _sha(raw), "bytes": len(raw), "closed_output": evidence})
            result["candidate_logical_bytes"] += len(raw)
        except (OSError, ValueError, TypeError, AttributeError) as error:
            reason = str(error) if isinstance(error, ValueError) else type(error).__name__
            retained[reason] += 1
            if len(result["retained_samples"]) < 64:
                result["retained_samples"].append({"name": path.name, "reason": reason})
    result.update(candidate_count=len(result["candidates"]), retained_reasons=dict(retained),
                  elapsed_seconds=round(time.monotonic() - began, 3))
    return result


def _write_new(path: Path, raw: bytes, *, info: os.stat_result) -> None:
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fchown(stream.fileno(), info.st_uid, info.st_gid)
        os.fchmod(stream.fileno(), stat.S_IMODE(info.st_mode))
        os.fsync(stream.fileno())


def _compress_candidate(directory: Path, candidate: dict, *, keep_days: int,
                        read_candidate: Callable[[Path], tuple[bytes, os.stat_result, dict]] | None = None) -> dict[str, Any]:
    """Caller holds WorkflowStore's run lock for the entire batch."""
    read_candidate = read_candidate or (lambda path: _candidate(path, now=time.time(), keep_days=keep_days))
    name = candidate["name"]
    if Path(name).name != name:
        raise ValueError("core_csv_candidate_path_invalid")
    path = directory / name
    stored, receipt = csv_storage_paths(path)
    outcome = {"name": name, "status": "retained", "removed": False, "independent_restore_verified": False,
               "completed_file_allocation_reduction_bytes": 0, "net_reclaimed_disk_bytes": None}
    temporary = [directory / f".core-csv-{uuid4().hex}.tmp" for _ in range(3)]
    created: dict[Path, tuple[int, int]] = {}
    try:
        raw, info, evidence = read_candidate(path)
        if (list(file_identity(info)) != candidate["identity"] or _sha(raw) != candidate["sha256"]
                or evidence != candidate["closed_output"]):
            raise ValueError("core_csv_source_changed_since_plan")
        required_free = 2 * len(raw) + MAX_RECEIPT_BYTES + RESTORE_RESERVE_BYTES
        if shutil.disk_usage(directory).free < required_free:
            outcome["reason"] = "core_csv_insufficient_temporary_space"
            return outcome
        if has_stored_csv(path):
            previous, metadata = read_stored_csv(path)
            if previous != raw or metadata["original_identity"].get("file_identity") != candidate["identity"]:
                raise ValueError("core_csv_storage_conflicts_with_plain_source")
        else:
            import pyarrow as pa

            compressed = pa.Codec("zstd", compression_level=3).compress(raw).to_pybytes()
            metadata = {
                "schema_version": 1, "format": FORMAT, "logical_name": name, "codec": "zstd",
                "original_identity": {"size": info.st_size, "mode": info.st_mode, "uid": info.st_uid,
                                      "gid": info.st_gid, "mtime_ns": info.st_mtime_ns,
                                      "atime_ns": info.st_atime_ns, "mtime": info.st_mtime,
                                      "file_identity": list(file_identity(info))},
                "original_sha256": _sha(raw), "stored_sha256": _sha(compressed), "stored_bytes": len(compressed),
                "closed_output": evidence, "stored_at": datetime.now().astimezone().isoformat(),
            }
            encoded = (json.dumps(metadata, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            if len(encoded) > MAX_RECEIPT_BYTES:
                raise ValueError("core_csv_receipt_budget_exceeded")
            _write_new(temporary[0], compressed, info=info)
            _write_new(temporary[1], encoded, info=info)
            for scratch, final in zip(temporary[:2], (stored, receipt)):
                os.link(scratch, final, follow_symlinks=False)
                scratch_info = scratch.lstat()
                created[final] = (scratch_info.st_dev, scratch_info.st_ino)
                scratch.unlink()
            sync_directory(directory)
        verified, verified_metadata = read_stored_csv(path)
        if verified != raw or verified_metadata != metadata:
            raise ValueError("core_csv_committed_storage_mismatch")
        restore = materialize_stored_csv(path, temporary[2])
        restored, restored_info = _read_regular(temporary[2], MAX_CSV_BYTES)
        if (restored != raw or restored_info.st_mtime_ns != info.st_mtime_ns
                or restored_info.st_mode != info.st_mode
                or (restored_info.st_uid, restored_info.st_gid) != (info.st_uid, info.st_gid)
                or restored_info.st_ino == info.st_ino):
            raise ValueError("core_csv_independent_restore_mismatch")
        outcome.update(independent_restore_verified=restore["full_restore_verified"],
                       original_sha256=metadata["original_sha256"], stored_sha256=metadata["stored_sha256"],
                       original_bytes=len(raw), stored_bytes=stored.stat().st_size,
                       receipt_bytes=receipt.stat().st_size,
                       temporary_bytes_bound=2 * len(raw) + MAX_RECEIPT_BYTES)
        allocation = sum(item.stat().st_blocks * 512 for item in (stored, receipt))
        if allocation >= info.st_blocks * 512:
            outcome["reason"] = "core_csv_no_allocation_saving"
            return outcome
        # Recheck both the original and its post-close evidence just before unlink.
        latest_raw, latest_info, latest_evidence = read_candidate(path)
        if (latest_raw != raw or file_identity(latest_info) != file_identity(info) or latest_evidence != evidence):
            raise ValueError("core_csv_source_changed_before_retirement")
        path.unlink()
        outcome.update(removed=True, status="compressed",
                       completed_file_allocation_reduction_bytes=info.st_blocks * 512 - allocation)
        sync_directory(directory)
    except (OSError, ValueError, TypeError, RuntimeError) as error:
        outcome.update(status="failed", error=f"{type(error).__name__}: {error}")
    finally:
        for scratch in temporary:
            scratch.unlink(missing_ok=True)
        # If interrupted after retirement, the complete, verified pair remains.
        # A crash before retirement leaves the ordinary CSV authoritative.
        if not outcome["removed"] and path.exists():
            for final, inode in reversed(list(created.items())):
                if final.exists() and (final.lstat().st_dev, final.lstat().st_ino) == inode:
                    final.unlink()
            if created:
                sync_directory(directory)
    return outcome


def run_core_csv_compression(settings, *, keep_days: int = 30, apply: bool = False,
                             max_files: int = MAX_BATCH_FILES, max_bytes: int = MAX_BATCH_BYTES,
                             now: float | None = None) -> dict[str, Any]:
    result = plan_core_csv_compression(settings, keep_days=keep_days, max_files=max_files, max_bytes=max_bytes, now=now)
    result.update(dry_run=not apply, status="dry_run" if not apply else "applied", outcomes=[],
                  compressed_count=0, removed_count=0, completed_file_allocation_reduction_bytes=0,
                  net_reclaimed_disk_bytes=None, errors=[])
    result["scope"] = ("Configured core output top-level CSV only; exact original bytes and metadata retained. "
                       "Per-batch budgets are not a total history cap. Allocation excludes audit files and Docker; "
                       "no host disk reclamation is inferred. Existing missing/failed business evidence is unchanged.")
    if not apply or not result["candidates"]:
        return result
    store = WorkflowStore(settings.state_file)
    acquired = False
    began = time.monotonic()
    try:
        with store._lock:
            store._acquire_run_lock_locked("core_csv_compression")
            acquired = True
        for candidate in result["candidates"]:
            if time.monotonic() - began > MAX_APPLY_SECONDS:
                result.update(status="deferred", reason="core_csv_apply_time_budget")
                break
            outcome = _compress_candidate(_directory(settings), candidate, keep_days=keep_days)
            result["outcomes"].append(outcome)
            if outcome["status"] == "failed":
                result["errors"].append({"path": candidate["name"], "error": outcome["error"]})
                result["status"] = "failed"
                break
    except WorkflowLockBusyError:
        result.update(status="deferred", reason="workflow_lock_busy")
    finally:
        if acquired:
            with store._lock:
                store._release_run_lock_locked()
    result["compressed_count"] = sum(row["status"] == "compressed" for row in result["outcomes"])
    result["removed_count"] = sum(row["removed"] for row in result["outcomes"])
    result["completed_file_allocation_reduction_bytes"] = sum(
        row["completed_file_allocation_reduction_bytes"] for row in result["outcomes"])
    result["apply_elapsed_seconds"] = round(time.monotonic() - began, 3)
    return result


def restore_core_csv(settings, path: Path, destination: Path | None = None) -> dict[str, Any]:
    directory = _directory(settings)
    path = Path(path).absolute()
    if path.parent != directory:
        raise ValueError("core_csv_restore_path_outside_scope")
    store = WorkflowStore(settings.state_file)
    with store._lock:
        store._acquire_run_lock_locked("core_csv_restore")
    try:
        return materialize_stored_csv(path, path if destination is None else destination)
    finally:
        with store._lock:
            store._release_run_lock_locked()
