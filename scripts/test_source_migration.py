"""Migrate retired-workspace source packages without rewriting their evidence.

Only legacy successful-test packages with an independently verified workspace
retirement are eligible. Release source archives without that lineage remain
unchanged. The old receipt stays immutable; a durable, hash-bound migration
record switches the existing recovery entry to a self-contained schema-2 pack.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import time

from scripts import test_artifact_storage as legacy
from scripts import test_source_storage as storage
from scripts import source_object_archive as objects

STAGE_NAME = ".source_migration_stage"
INTENT_NAME = "intent.json"
MAX_SCAN_ENTRIES = 100000
MAX_SOURCE_BYTES = 128 * 1024**2
DEFAULT_MAX_RUNS = 8
DEFAULT_MAX_BYTES = 256 * 1024**2
DEFAULT_MAX_SECONDS = 120
DEFAULT_MIN_AGE_HOURS = 24


def _json(path: Path) -> dict:
    storage._identity(legacy._safe_path(path))
    if path.stat().st_size > 16 * 1024**2:
        raise legacy.ArtifactStorageError("source_migration_metadata_too_large")
    return legacy._regular_json(path)


def _record_paths(run: Path) -> list[Path]:
    names = (storage.MANIFEST_NAME, "source_manifest.json", "summary.json",
             "workspace_retired.json", legacy.MANIFEST_NAME)
    return [run / name for name in names]


def _candidate(run: Path, now: float, min_age_hours: float) -> tuple[dict | None, str]:
    receipt = _json(run / storage.MANIFEST_NAME)
    if receipt.get("schema_version") != 1 or receipt.get("artifact_type") != storage.ARTIFACT_TYPE:
        return None, "not_legacy_successful_source_package"
    if not receipt.get("origin_archive_manifest_sha256") or not receipt.get("origin_archive_sha256"):
        return None, "source_release_or_unreviewed_lineage_preserved"
    retired, summary = _json(run / "workspace_retired.json"), _json(run / "summary.json")
    worker = summary.get("worker") or {}
    restrictions = worker.get("restrictions") or {}
    outcome = receipt.get("test_outcome") or {}
    if (summary.get("scope") != "isolated_unit_regression" or summary.get("source_unchanged") is not True
            or type(summary.get("exit_code")) is not int or summary["exit_code"] != 0
            or summary.get("timed_out") is not False or type(worker.get("exit_code")) is not int
            or worker["exit_code"] != 0 or restrictions.get("scope") != "isolated_unit_regression"
            or restrictions.get("network_socket_creation") != "denied"
            or restrictions.get("business_paths_outside_workspace") != "read_write_denied"
            or type(outcome.get("exit_code")) is not int or outcome["exit_code"] != 0
            or outcome.get("timed_out") is not False
            or summary.get("source_sha256") != receipt.get("source_sha256")
            or retired.get("status") != "retired" or retired.get("retired_archive") != legacy.ARCHIVE_NAME
            or retired.get("original_sha256") != receipt["origin_archive_sha256"]
            or retired.get("source_sha256") != receipt.get("source_sha256")
            or retired.get("source_bytes_preserved") is not True
            or retired.get("original_test_workspace_recoverable") is not False
            or type(retired.get("test_exit_code")) is not int or retired["test_exit_code"] != 0
            or legacy._sha(run / legacy.MANIFEST_NAME) != receipt["origin_archive_manifest_sha256"]):
        raise legacy.ArtifactStorageError("legacy_source_retirement_lineage_unverified")
    for name in ("workspace", legacy.ARCHIVE_NAME):
        path = run / name
        if path.exists() or path.is_symlink():
            return None, "original_workspace_still_present"
    paths = _record_paths(run)
    if (run / storage.ARCHIVE_NAME).exists():
        paths.append(run / storage.ARCHIVE_NAME)
    if now - max(path.stat().st_mtime for path in paths) < min_age_hours * 3600:
        return None, "within_source_migration_minimum_age"
    if not objects.split_entries(receipt["entries"])[1]:
        return None, "no_large_source_objects"
    logical = sum(item.get("size", 0) for item in receipt["entries"].values())
    if logical > MAX_SOURCE_BYTES:
        return None, "source_package_exceeds_migration_byte_budget"
    return {"receipt": receipt, "logical_source_bytes": logical}, "eligible"


def _discover(root: Path, deadline: float) -> tuple[list[Path], bool, int]:
    audit = legacy._safe_path(root / "reports" / "audit")
    if not audit.exists():
        return [], True, 0
    found, scanned = [], 0
    def walk_error(error):
        raise error
    for base, dirs, files in os.walk(audit, followlinks=False, onerror=walk_error):
        scanned += len(dirs) + len(files)
        if scanned > MAX_SCAN_ENTRIES or time.monotonic() > deadline:
            return found, False, scanned
        dirs[:] = sorted(name for name in dirs if name not in {
            "workspace", storage.MIGRATED_PACKAGE_NAME, objects.OBJECTS_NAME,
            "__pycache__", ".pytest_cache"} and not name.startswith(".")
            and not (Path(base) / name).is_symlink())
        if storage.MANIFEST_NAME in files:
            found.append(Path(base))
    return sorted(found), True, scanned


def _same_records(records: dict[Path, tuple[tuple, str]]) -> None:
    for path, (identity, digest) in records.items():
        if storage._identity(path) != identity or legacy._sha(path) != digest:
            raise legacy.ArtifactStorageError("source_migration_evidence_changed:" + path.name)


def _cleanup_stage(stage: Path, original_receipt_sha: str, deadline: float) -> None:
    if not stage.exists():
        return
    stage = legacy._safe_path(stage)
    intent = _json(stage / INTENT_NAME)
    if intent != {"schema_version": 1, "original_receipt_sha256": original_receipt_sha}:
        raise legacy.ArtifactStorageError("unowned_source_migration_stage_preserved")
    entries = legacy._scan(stage, deadline)
    counts = Counter((item["device"], item["inode"]) for item in entries.values() if item["kind"] == "file")
    if any(item["kind"] not in {"file", "directory"} or
           (item["kind"] == "file" and item["links"] != counts[(item["device"], item["inode"])])
           for item in entries.values()):
        raise legacy.ArtifactStorageError("source_migration_stage_has_external_links")
    storage.assert_files_inactive(entries)
    legacy._remove_tree(stage, entries)
    storage._sync(stage.parent)


def _verify_new_package(run: Path, receipt: dict, package: Path, target: Path, deadline: float) -> None:
    _, migrated = storage._verified_source(package)
    if (migrated["schema_version"] != 2 or migrated["entries"] != receipt["entries"]
            or migrated["source_sha256"] != receipt["source_sha256"]
            or migrated["source_manifest_sha256"] != receipt["source_manifest_sha256"]):
        raise legacy.ArtifactStorageError("source_migration_package_content_changed")
    process = subprocess.run(
        [sys.executable, "-I", str(package / objects.READER_NAME), "--manifest",
         str(package / storage.MANIFEST_NAME), "--target", str(target)],
        cwd=package, capture_output=True, text=True, timeout=max(0.01, deadline - time.monotonic()))
    if process.returncode != 0:
        raise legacy.ArtifactStorageError("migrated_independent_reader_failed:" + process.stderr[-1000:])
    restored = legacy._scan(target, deadline)
    if legacy._content(restored) != legacy._content(receipt["entries"]) or legacy._hardlinks(restored) != legacy._hardlinks(receipt["entries"]):
        raise legacy.ArtifactStorageError("migrated_independent_source_restore_mismatch")
    legacy._remove_tree(target, restored)


def _remove_old_representation(root: Path, run: Path, receipt: dict, marker: dict,
                               records: dict[Path, tuple[tuple, str]]) -> dict:
    archive = run / storage.ARCHIVE_NAME
    pool = legacy._safe_path(root / "reports" / storage.POOL_NAME)
    old_inode = tuple(marker["original_archive_inode"])
    result = {"removed_old_archive_path": False, "removed_newly_orphaned_pool_object": False,
              "removed_old_allocated_bytes": 0}
    with storage._pool_lock(pool):
        # Share the complete new package before dropping any legacy source path.
        new_archive, new_receipt = storage._verified_source(run)
        candidates = [(new_archive, new_receipt["archive_sha256"], new_receipt["archive_bytes"], ".tar.gz"),
                      (new_archive.parent / objects.READER_NAME, new_receipt["reader_sha256"], new_receipt["reader_bytes"], ".pyz")]
        candidates += [(new_archive.parent / objects.OBJECTS_NAME / (key + ".gz"), key, value["bytes"], ".gz")
                       for key, value in new_receipt["object_index"].items()]
        shares = [storage._share_file(pool, *candidate) for candidate in candidates]
        result["sharing"] = dict(Counter(item["status"] for item in shares))
        paths = [archive, pool / (receipt["archive_sha256"] + ".tar.gz")]
        for path in paths:
            if not path.exists() and not path.is_symlink():
                continue
            identity = storage._identity(legacy._safe_path(path))
            info = path.stat()
            # Only retire the precise old inode. Other copies/pool entries belong
            # to ordinary GC, and additional hardlinks continue protecting it.
            if (info.st_dev, info.st_ino) != old_inode:
                if path != archive:
                    continue
                raise legacy.ArtifactStorageError("old_source_archive_identity_changed")
            if path != archive and info.st_nlink != 1:
                continue
            if info.st_size != receipt["archive_bytes"] or legacy._sha(path) != receipt["archive_sha256"]:
                raise legacy.ArtifactStorageError("old_source_archive_sha256_changed")
            storage.assert_files_inactive({str(path): {"device": info.st_dev, "inode": info.st_ino}})
            _same_records(records)
            if storage._identity(path) != identity or path.stat().st_nlink != info.st_nlink:
                raise legacy.ArtifactStorageError("old_source_archive_changed_before_removal")
            path.unlink()
            storage._sync(path.parent)
            if info.st_nlink == 1:
                result["removed_old_allocated_bytes"] += info.st_blocks * 512
            result["removed_old_archive_path" if path == archive else "removed_newly_orphaned_pool_object"] = True
    return result


def _migrate_one(root: Path, run: Path, candidate: dict, deadline: float) -> dict:
    receipt = candidate["receipt"]
    records = {path: (storage._identity(path), legacy._sha(path)) for path in _record_paths(run)}
    old_receipt_sha = records[run / storage.MANIFEST_NAME][1]
    stage, package, marker_path = run / STAGE_NAME, run / storage.MIGRATED_PACKAGE_NAME, run / storage.MIGRATION_NAME
    result = {"run_dir": str(run.relative_to(root)), "source_sha256": receipt["source_sha256"],
              "status": "preserved", "old_compressed_bytes_recovery_guaranteed": True}
    fs = os.statvfs(run)
    if fs.f_bavail * fs.f_frsize < candidate["logical_source_bytes"] * 3 + 64 * 1024**2:
        raise legacy.ArtifactStorageError("insufficient_source_migration_space")
    try:
        _cleanup_stage(stage, old_receipt_sha, deadline)
        if marker_path.exists() or marker_path.is_symlink():
            storage._verified_source(run)
            marker = _json(marker_path)
        else:
            old_archive, original = storage._verified_source(run)
            if original != receipt:
                raise legacy.ArtifactStorageError("legacy_source_receipt_changed")
            original_stat = old_archive.stat()
            original_identity = storage._identity(old_archive)
            storage.assert_files_inactive({"old_archive": {"device": original_stat.st_dev, "inode": original_stat.st_ino}})
            stage.mkdir(mode=0o700)
            legacy._atomic_json(stage / INTENT_NAME, {"schema_version": 1, "original_receipt_sha256": old_receipt_sha})
            storage._sync(stage)
            storage._sync(run)
            if not package.exists() and not package.is_symlink():
                source, pending = stage / "source", stage / "package"
                legacy._restore(old_archive, receipt["entries"], source, deadline)
                pending.mkdir(mode=0o700)
                with (pending / "source_manifest.json").open("xb") as stream:
                    stream.write((run / "source_manifest.json").read_bytes())
                    stream.flush()
                    os.fsync(stream.fileno())
                inline, large = objects.split_entries(receipt["entries"])
                migrated = {**receipt, "schema_version": 2}
                migrated.update(objects.write_objects(pending / objects.OBJECTS_NAME, source, large, deadline))
                storage._write_source_archive(pending / storage.ARCHIVE_NAME, source, inline)
                migrated.update(archive_sha256=legacy._sha(pending / storage.ARCHIVE_NAME),
                                archive_bytes=(pending / storage.ARCHIVE_NAME).stat().st_size)
                migrated.update(objects.write_reader(pending / objects.READER_NAME))
                legacy._atomic_json(pending / storage.MANIFEST_NAME, migrated)
                storage._sync(pending)
                _verify_new_package(run, receipt, pending, stage / "verify", deadline)
                objects._budget(deadline)
                _same_records(records)
                if storage._identity(old_archive) != original_identity:
                    raise legacy.ArtifactStorageError("old_source_archive_changed_during_migration")
                pending.rename(package)
                storage._sync(run)
            else:
                # Delivery succeeded before an earlier interruption. Revalidate
                # the exact source and shipped reader before committing it.
                _verify_new_package(run, receipt, package, stage / "verify", deadline)
            _same_records(records)
            marker = {"schema_version": 1, "status": "committed", "package": storage.MIGRATED_PACKAGE_NAME,
                      "original_receipt_sha256": old_receipt_sha,
                      "original_archive_sha256": receipt["archive_sha256"],
                      "original_archive_inode": [original_stat.st_dev, original_stat.st_ino],
                      "source_sha256": receipt["source_sha256"],
                      "source_manifest_sha256": receipt["source_manifest_sha256"],
                      "package_receipt_sha256": legacy._sha(package / storage.MANIFEST_NAME),
                      "committed_at": datetime.now().astimezone().isoformat(),
                      "scope": "Exact source content, modes, mtimes and hardlinks; old gzip representation may be retired. Original test results and legacy receipt remain unchanged."}
            legacy._atomic_json(marker_path, marker)
            storage._sync(run)
            # The real old-manifest reader now follows the new representation.
            # No old bytes are removed unless that binding verifies completely.
            storage._verified_source(run)
        objects._budget(deadline)
        _same_records(records)
        result.update(_remove_old_representation(root, run, receipt, marker, records))
        _same_records(records)
        storage._verified_source(run)
        result.update(status="migrated", old_compressed_bytes_recovery_guaranteed=False,
                      source_file_count=sum(item["kind"] == "file" for item in receipt["entries"].values()),
                      independent_reader_verified=True)
    finally:
        _cleanup_stage(stage, old_receipt_sha, deadline + 30)
    return result


def migrate_legacy_sources(project_root: Path, *, apply: bool = False, now: float | None = None,
                           run_dirs: list[Path] | None = None, max_runs: int = DEFAULT_MAX_RUNS,
                           min_age_hours: float = DEFAULT_MIN_AGE_HOURS,
                           max_source_bytes: int = DEFAULT_MAX_BYTES,
                           max_seconds: float = DEFAULT_MAX_SECONDS) -> dict:
    root = legacy._safe_path(project_root)
    if not 1 <= max_runs <= 16 or not 1 <= min_age_hours or not 0 < max_source_bytes <= DEFAULT_MAX_BYTES or not 0 < max_seconds <= DEFAULT_MAX_SECONDS:
        raise ValueError("invalid_source_migration_budget")
    started = time.monotonic()
    deadline, now = started + max_seconds, time.time() if now is None else now
    result = {"scope": "retired_successful_workspace_legacy_sources", "dry_run": not apply,
              "candidate_count": 0, "attempted_count": 0, "migrated_count": 0, "removed_old_allocated_bytes": 0,
              "scan_complete": True, "batch_complete": True, "scanned_entries": 0,
              "allocation_accounting_complete": True,
              "protected_reasons": {}, "items": [], "errors": [],
              "net_reclaimed_disk_bytes": None,
              "limits": {"max_runs": max_runs, "min_age_hours": min_age_hours,
                         "max_source_bytes": max_source_bytes, "max_seconds": max_seconds},
              "accounting_scope": "Old last-link allocation removed only; excludes new packages, sharing, records and directory blocks. Measure whole-project net allocation separately."}
    reasons, selected_bytes = Counter(), 0
    if run_dirs is None:
        runs, result["scan_complete"], result["scanned_entries"] = _discover(root, min(deadline, started + 30))
    else:
        runs = sorted(set(Path(path) for path in run_dirs))
    for raw in runs:
        if time.monotonic() > deadline:
            result["batch_complete"] = False
            break
        try:
            run = legacy._safe_path(raw)
            if not run.is_relative_to(root / "reports" / "audit"):
                raise legacy.ArtifactStorageError("source_migration_outside_audit_scope")
            with storage.source_archive_lock(run / storage.MANIFEST_NAME, exclusive=apply):
                candidate, reason = _candidate(run, now, min_age_hours)
                if candidate is None:
                    reasons[reason] += 1
                    continue
                marker = run / storage.MIGRATION_NAME
                pool_old = root / "reports" / storage.POOL_NAME / (candidate["receipt"]["archive_sha256"] + ".tar.gz")
                if marker.is_file() and not (run / storage.ARCHIVE_NAME).exists() and not (run / STAGE_NAME).exists() and not (pool_old.exists() and pool_old.stat().st_nlink == 1):
                    storage._verified_source(run)
                    reasons["already_migrated"] += 1
                    continue
                result["candidate_count"] += 1
                if result["attempted_count"] >= max_runs or selected_bytes + candidate["logical_source_bytes"] > max_source_bytes:
                    result["batch_complete"] = False
                    continue
                selected_bytes += candidate["logical_source_bytes"]
                result["attempted_count"] += 1
                if apply:
                    item = _migrate_one(root, run, candidate, deadline)
                    result["migrated_count"] += 1
                    if result["removed_old_allocated_bytes"] is not None:
                        result["removed_old_allocated_bytes"] += item["removed_old_allocated_bytes"]
                else:
                    item = {"run_dir": str(run.relative_to(root)), "status": "candidate",
                            "source_sha256": candidate["receipt"]["source_sha256"],
                            "logical_source_bytes": candidate["logical_source_bytes"]}
                result["items"].append(item)
        except (OSError, ValueError, KeyError, TypeError, EOFError, tarfile.TarError, legacy.ArtifactStorageError, subprocess.SubprocessError) as error:
            if apply and (raw / storage.MIGRATION_NAME).exists():
                result["allocation_accounting_complete"] = False
                result["removed_old_allocated_bytes"] = None
            result["errors"].append({"run_dir": str(raw), "error": str(error),
                                     "temporary_stage_present": (raw / STAGE_NAME).exists(),
                                     "migration_record_present": (raw / storage.MIGRATION_NAME).exists()})
    result["protected_reasons"] = dict(reasons)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    result["status"] = "degraded" if result["errors"] or not result["scan_complete"] else "complete" if result["batch_complete"] else "batch_limit_reached"
    return result
