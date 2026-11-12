"""Lossless operator migration for closed legacy holding-exclusion CSVs only.

Committed runs, revision directories, shared files and recent outputs remain
ordinary files. The existing CSV codec retains original bytes and metadata;
neither historical report JSON nor publication manifests are rewritten.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any

from backend.core_csv_compression import _compress_candidate, MAX_CLOSE_EVIDENCE_BYTES
from backend.publication_transaction import load_publication_head, publication_io_lock
from backend.storage import WorkflowLockBusyError, WorkflowStore
from core.utils.core_csv_storage import (
    _read_regular, file_identity, has_stored_csv, materialize_stored_csv,
)

MIN_KEEP_DAYS = 30
MAX_FILES = 32
MAX_INPUT_BYTES = 256 * 1024**2
MAX_FILE_BYTES = 64 * 1024**2
MAX_SCAN_ENTRIES = 10000
MAX_SECONDS = 120
RUN = re.compile(r"(?P<stamp>20\d{6}_\d{6})_sync")


def _safe(path: Path) -> None:
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError("legacy_report_csv_symlink_protected")


def _run_directory(settings: Any, path: Path) -> str:
    root = Path(settings.project_root).absolute()
    roots = {Path(settings.runs_dir).absolute(), Path(settings.archived_runs_dir).absolute()}
    if any(not directory.is_relative_to(root) for directory in roots):
        raise ValueError("legacy_report_csv_root_outside_project")
    _safe(path)
    match = RUN.fullmatch(path.parent.name)
    if (path.parent.parent not in roots or match is None
            or path.name != f"当日最终优质基金_持有期排除清单_{match['stamp']}.csv"):
        raise ValueError("legacy_report_csv_path_outside_scope")
    return path.parent.name


def _candidate(settings: Any, path: Path, protected: set[str]) -> tuple[bytes, os.stat_result, dict]:
    run_id = _run_directory(settings, path)
    if (run_id in protected or (path.parent / "validated.json").exists()
            or (path.parent / "publication_state.json").exists()):
        raise ValueError("legacy_report_csv_committed_run_protected")
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or not 32 * 1024 <= info.st_size <= MAX_FILE_BYTES
            or os.listxattr(path, follow_symlinks=False)):
        raise ValueError("legacy_report_csv_type_links_size_or_attributes_protected")
    cutoff = time.time() - MIN_KEEP_DAYS * 86400
    if (info.st_mtime >= cutoff
            or datetime.strptime(run_id[:15], "%Y%m%d_%H%M%S").timestamp() >= cutoff):
        raise ValueError("legacy_report_csv_recent_output_protected")
    report_path = path.parent / f"bond_analysis_report_{run_id}.json"
    metadata_path = path.with_name(path.stem + "_report_meta.json")
    evidence = {"boundary": "closed_legacy_report_and_holding_exclusion_metadata",
                "business_outcome_verified": False, "files": []}
    payloads = []
    for source in (report_path, metadata_path):
        _safe(source)
        raw, source_info = _read_regular(source, MAX_CLOSE_EVIDENCE_BYTES)
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("legacy_report_csv_evidence_object_required")
        generated = datetime.fromisoformat(str(payload.get("generated_at") or "")).timestamp()
        if (generated >= cutoff or source_info.st_mtime >= cutoff
                or source_info.st_mtime_ns < info.st_mtime_ns):
            raise ValueError("legacy_report_csv_close_evidence_time_protected")
        evidence["files"].append({"path": source.name, "sha256": hashlib.sha256(raw).hexdigest(),
                                  "identity": list(file_identity(source_info))})
        payloads.append(payload)
    report, metadata = payloads
    expected = f"reports/runs/{run_id}/{path.name}"
    archived = str(path.relative_to(Path(settings.project_root).absolute()))
    if (report.get("run_id") != run_id
            or (report.get("artifacts") or {}).get("excluded_csv") not in {expected, archived}
            or metadata.get("report_type") != "holding_period_excluded_list"
            or metadata.get("primary_run_id") != run_id
            or Path(str(metadata.get("output_file") or "")).name != path.with_suffix(".xlsx").name):
        raise ValueError("legacy_report_csv_close_evidence_binding_invalid")
    raw, read_info = _read_regular(path, MAX_FILE_BYTES)
    if file_identity(info) != file_identity(read_info):
        raise ValueError("legacy_report_csv_changed_during_read")
    return raw, info, evidence


def plan_legacy_report_csv_compression(settings: Any, *, max_files: int = MAX_FILES) -> dict:
    if type(max_files) is not int or not 1 <= max_files <= MAX_FILES:
        raise ValueError("legacy_report_csv_file_budget_invalid")
    head = load_publication_head(settings)
    protected = set((head.get("committed_runs") or {}).keys())
    protected.add(str(head.get("current_published_run_id") or ""))
    result = {"schema_version": 1, "scope": "legacy_run_root_holding_exclusion_csv",
              "generated_at": datetime.now().astimezone().isoformat(), "keep_plain_days": MIN_KEEP_DAYS,
              "max_files": max_files, "max_input_bytes": MAX_INPUT_BYTES,
              "scan_complete": True, "candidates": [], "candidate_logical_bytes": 0,
              "retained_reasons": {}, "stored_count": 0}
    began = time.monotonic()
    retained: Counter = Counter()
    scanned = 0
    for directory in sorted({Path(settings.runs_dir).absolute(), Path(settings.archived_runs_dir).absolute()}):
        _safe(directory)
        for path in sorted(directory.glob("*/当日最终优质基金_持有期排除清单_*.csv*")):
            scanned += 1
            if scanned > MAX_SCAN_ENTRIES or time.monotonic() - began > 30:
                result["scan_complete"] = False
                break
            if path.name.endswith(".csv.zst.receipt.json"):
                result["stored_count"] += 1
            if path.suffix != ".csv":
                continue
            if len(result["candidates"]) >= max_files:
                retained["batch_file_budget"] += 1
                continue
            try:
                if result["candidate_logical_bytes"] + path.lstat().st_size > MAX_INPUT_BYTES:
                    retained["batch_byte_budget"] += 1
                    continue
                raw, info, evidence = _candidate(settings, path, protected)
                # A leftover storage pair is retained for explicit inspection.
                if has_stored_csv(path):
                    raise ValueError("legacy_report_csv_existing_pair_requires_review")
                result["candidates"].append({"path": str(path.relative_to(settings.project_root)),
                                             "name": path.name, "bytes": len(raw),
                                             "identity": list(file_identity(info)),
                                             "sha256": hashlib.sha256(raw).hexdigest(),
                                             "closed_output": evidence})
                result["candidate_logical_bytes"] += len(raw)
            except (OSError, ValueError, TypeError, AttributeError) as error:
                retained[str(error) if isinstance(error, ValueError) else type(error).__name__] += 1
    result.update(candidate_count=len(result["candidates"]), retained_reasons=dict(retained),
                  elapsed_seconds=round(time.monotonic() - began, 3))
    return result


def run_legacy_report_csv_compression(settings: Any, *, apply: bool = False,
                                      max_files: int = MAX_FILES) -> dict:
    store = WorkflowStore(settings.state_file) if apply else None
    try:
        if store:
            with store._lock:
                store._acquire_run_lock_locked("legacy_report_csv_compression")
        # Only apply needs to exclude publication writers; previews remain read-only.
        if apply:
            with publication_io_lock(settings):
                return _run(settings, apply=True, max_files=max_files)
        return _run(settings, apply=False, max_files=max_files)
    except WorkflowLockBusyError:
        return {"status": "deferred", "reason": "workflow_lock_busy", "errors": [], "removed_count": 0}
    finally:
        if store:
            store.close()


def _run(settings: Any, *, apply: bool, max_files: int) -> dict:
    result = plan_legacy_report_csv_compression(settings, max_files=max_files)
    result.update(status="dry_run" if not apply else "applied", dry_run=not apply, outcomes=[], errors=[],
                  removed_count=0, compressed_count=0, completed_file_allocation_reduction_bytes=0,
                  net_reclaimed_disk_bytes=None)
    if not apply or not result["scan_complete"]:
        if apply:
            result["status"] = "deferred"
        return result
    protected = set((load_publication_head(settings).get("committed_runs") or {}).keys())
    began = time.monotonic()
    for candidate in result["candidates"]:
        if time.monotonic() - began > MAX_SECONDS:
            result.update(status="deferred", reason="apply_time_budget")
            break
        path = Path(settings.project_root) / candidate["path"]
        outcome = _compress_candidate(path.parent, candidate, keep_days=MIN_KEEP_DAYS,
                                      read_candidate=lambda path: _candidate(settings, path, protected))
        outcome["path"] = candidate["path"]
        result["outcomes"].append(outcome)
        if outcome["status"] == "failed":
            result["errors"].append({"path": candidate["path"], "error": outcome["error"]})
            result["status"] = "failed"
            break
    result["compressed_count"] = sum(row["status"] == "compressed" for row in result["outcomes"])
    result["removed_count"] = sum(row["removed"] for row in result["outcomes"])
    result["completed_file_allocation_reduction_bytes"] = sum(
        row["completed_file_allocation_reduction_bytes"] for row in result["outcomes"])
    return result


def restore_legacy_report_csv(settings: Any, path: Path, destination: Path) -> dict:
    path = path.absolute()
    _run_directory(settings, path)
    store = WorkflowStore(settings.state_file)
    try:
        with store._lock:
            store._acquire_run_lock_locked("legacy_report_csv_restore")
        return materialize_stored_csv(path, destination)
    finally:
        store.close()
