"""Keep reproducible sources for successful tests, and full failure workspaces.

Successful generated fixtures are disposable. Source archives normalize tar
headers so identical sources can share storage without changing source bytes,
per-run source timestamps, test results, or the isolation boundary.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import tarfile
import time
import uuid

from scripts import test_artifact_storage as legacy
from scripts import source_object_archive as objects
from scripts.isolation_support import CONTEXT_FILE, manifest_digest
from backend.file_usage import FileUsageError, assert_files_inactive as _assert_files_inactive

ARCHIVE_NAME = "source.tar.gz"
MANIFEST_NAME = "source_archive.json"
ARTIFACT_TYPE = "successful_isolated_test_sources"
POOL_NAME = ".test_source_pool"
MIGRATION_NAME = "source_migration.json"
MIGRATED_PACKAGE_NAME = "source_package_v2"


@contextmanager
def source_archive_lock(manifest: Path, *, exclusive: bool = False):
    """The immutable receipt is also a lock inode; read-only recovery works."""
    manifest = legacy._safe_path(manifest)
    descriptor = os.open(manifest, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise legacy.ArtifactStorageError("source_receipt_regular_file_required")
        fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        current = manifest.stat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise legacy.ArtifactStorageError("source_receipt_replaced_while_locking")
        yield
    finally:
        os.close(descriptor)


def _identity(path: Path) -> tuple:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise legacy.ArtifactStorageError("source_archive_regular_file_required")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode)


def _sync(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _process_paths():
    return Path("/proc").iterdir()


def assert_files_inactive(entries: dict[str, dict]) -> None:
    try:
        _assert_files_inactive(entries, processes=_process_paths())
    except FileUsageError as error:
        code = str(error)
        prefix = "test_workspace_" if code in {"file_open", "file_mapped"} else "test_"
        raise legacy.ArtifactStorageError(prefix + code) from error


def _source_entries(entries: dict, files: dict[str, str]) -> dict:
    if not isinstance(files, dict) or not files:
        raise legacy.ArtifactStorageError("test_source_manifest_empty")
    names = {"."}
    for name, expected in files.items():
        path = PurePosixPath(name)
        item = entries.get(name, {})
        if (not isinstance(expected, str) or re.fullmatch(r"[a-f0-9]{64}", expected) is None
                or path.is_absolute() or ".." in path.parts or str(path) != name
                or item.get("kind") != "file" or item.get("sha256") != expected):
            raise legacy.ArtifactStorageError("test_source_bytes_changed:" + name)
        names.add(name)
        names.update(str(parent) for parent in path.parents)
    selected = {name: entries[name] for name in sorted(names)}
    legacy._validate_entries(selected)
    return selected


def _write_source_archive(archive: Path, workspace: Path, entries: dict) -> None:
    seen = {}
    with archive.open("xb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=6) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as package:
                for name, item in entries.items():
                    member = tarfile.TarInfo(legacy._archive_member_name(name))
                    member.mode = item["mode"]
                    member.mtime = member.uid = member.gid = 0
                    if item["kind"] == "directory":
                        member.type = tarfile.DIRTYPE
                        package.addfile(member)
                    else:
                        key = item["device"], item["inode"]
                        if key in seen:
                            member.type, member.linkname = tarfile.LNKTYPE, seen[key]
                            package.addfile(member)
                        else:
                            seen[key] = member.name
                            member.size = item["size"]
                            with (workspace / name).open("rb") as reader:
                                package.addfile(member, reader)
        raw.flush()
        os.fsync(raw.fileno())
    archive.chmod(0o600)


def _reclaimable_allocation(entries: dict) -> int:
    counts = Counter((item["device"], item["inode"]) for item in entries.values())
    seen, total = set(), 0
    for item in entries.values():
        key = item["device"], item["inode"]
        if key not in seen and (item["kind"] == "directory" or item["links"] == counts[key]):
            total += item["allocated_bytes"]
        seen.add(key)
    return total


def store_completed_workspace(run_dir: Path, worker_process) -> dict:
    """Use source-only retention for proven success; preserve full failures."""
    try:
        run_dir = legacy._safe_path(run_dir)
        summary = legacy._regular_json(run_dir / "summary.json")
    except (OSError, ValueError, legacy.ArtifactStorageError):
        return legacy.archive_completed_workspace(run_dir, worker_process)
    if type(summary.get("exit_code")) is not int or summary["exit_code"] != 0 or summary.get("timed_out") is not False:
        return legacy.archive_completed_workspace(run_dir, worker_process)
    started, restore_dir, entries, removing = time.monotonic(), None, None, False
    staging = None
    result = {"status": "retained", "workspace_removed": False,
              "original_workspace_recoverable": True, "full_restore_verified": False}
    try:
        workspace = legacy._safe_path(run_dir / "workspace")
        source = legacy._regular_json(run_dir / "source_manifest.json")
        records = {name: (run_dir / name).read_bytes() for name in ("summary.json", "source_manifest.json")}
        if json.loads(records["summary.json"]) != summary or json.loads(records["source_manifest.json"]) != source:
            raise legacy.ArtifactStorageError("test_records_changed_during_read")
        context = legacy._regular_json(workspace / CONTEXT_FILE)
        worker = legacy._regular_json(workspace / ".test_result.json")
        outcome = legacy._test_outcome(summary, worker, worker_process)
        restrictions = worker.get("restrictions", {})
        if (summary.get("scope") != "isolated_unit_regression" or context.get("scope") != "isolated_unit_regression"
                or restrictions.get("scope") != "isolated_unit_regression"
                or restrictions.get("network_socket_creation") != "denied"
                or restrictions.get("business_paths_outside_workspace") != "read_write_denied"
                or summary.get("worker") != worker):
            raise legacy.ArtifactStorageError("successful_test_isolation_not_proven")
        digest = manifest_digest(source["files"])
        if not digest == source.get("sha256") == summary.get("source_sha256") == context.get("source_sha256"):
            raise legacy.ArtifactStorageError("test_source_identity_mismatch")
        archive, descriptor = run_dir / ARCHIVE_NAME, run_dir / MANIFEST_NAME
        temporary = archive.with_name(archive.name + ".tmp")
        targets = (archive, descriptor, temporary, run_dir / legacy.ARCHIVE_NAME, run_dir / legacy.MANIFEST_NAME,
                   run_dir / objects.OBJECTS_NAME, run_dir / objects.READER_NAME)
        if any(path.exists() or path.is_symlink() for path in targets):
            raise legacy.ArtifactStorageError("existing_archive_not_overwritten")
        deadline = started + legacy.MAX_SECONDS
        entries = legacy._scan(workspace, deadline)
        selected = _source_entries(entries, source["files"])
        logical = sum(item.get("size", 0) for item in selected.values())
        fs = os.statvfs(run_dir)
        if fs.f_bavail * fs.f_frsize < 2 * logical + 64 * 1024**2:
            raise legacy.ArtifactStorageError("insufficient_source_archive_and_restore_space")
        assert_files_inactive(entries)
        inline, large = objects.split_entries(selected)
        receipt = {"schema_version": 2 if large else 1, "artifact_type": ARTIFACT_TYPE, "archive": ARCHIVE_NAME,
                   "source_sha256": digest, "source_manifest_sha256": hashlib.sha256(records["source_manifest.json"]).hexdigest(),
                   "entries": selected, "test_outcome": outcome, "source_restore_verified": True,
                   "original_workspace_recoverable": False,
                   "verified_at": datetime.now().astimezone().isoformat(),
                   "scope": "Exact source bytes, modes, mtimes and hardlinks only; successful generated test workspace is disposable."}
        if large:
            staging = run_dir / (".source_package_" + uuid.uuid4().hex)
            staging.mkdir(mode=0o700)
            (staging / "source_manifest.json").write_bytes(records["source_manifest.json"])
            temporary = staging / ARCHIVE_NAME
            receipt.update(objects.write_objects(staging / objects.OBJECTS_NAME, workspace, large, deadline))
            objects.verify_objects(staging, receipt, deadline)
        _write_source_archive(temporary, workspace, inline)
        legacy._verify_archive(temporary, inline)
        receipt.update(archive_sha256=legacy._sha(temporary), archive_bytes=temporary.stat().st_size)
        restore_dir = run_dir / (".source_restore_verify_" + uuid.uuid4().hex)
        if large:
            objects.restore_objects(temporary, receipt, restore_dir, deadline)
            legacy._remove_tree(restore_dir, selected)
            receipt.update(objects.write_reader(staging / objects.READER_NAME))
            legacy._atomic_json(staging / MANIFEST_NAME, receipt)
            # A release must remain recoverable even without this installed
            # project. Exercise the exact shipped reader in isolated Python.
            restored = subprocess.run(
                [sys.executable, "-I", str(staging / objects.READER_NAME),
                 "--manifest", str(staging / MANIFEST_NAME), "--target", str(restore_dir)],
                cwd=staging, capture_output=True, text=True, timeout=max(0.01, deadline - time.monotonic()))
            if restored.returncode != 0:
                raise legacy.ArtifactStorageError("independent_source_reader_failed:" + restored.stderr[-1000:])
            actual = legacy._scan(restore_dir, deadline)
            if (legacy._content(actual) != legacy._content(selected)
                    or legacy._hardlinks(actual) != legacy._hardlinks(selected)):
                raise legacy.ArtifactStorageError("independent_source_reader_mismatch")
        else:
            legacy._restore(temporary, selected, restore_dir, deadline)
        if legacy._scan(workspace, deadline) != entries:
            raise legacy.ArtifactStorageError("workspace_changed_before_retirement")
        if any((run_dir / name).read_bytes() != raw for name, raw in records.items()):
            raise legacy.ArtifactStorageError("test_records_changed_before_retirement")
        legacy._stopped_worker(worker_process)
        assert_files_inactive(entries)
        if large:
            # Receipt is the commit marker; every local dependency is delivered
            # first. No source-only package depends on a global pool path.
            for name in (objects.OBJECTS_NAME, ARCHIVE_NAME, objects.READER_NAME):
                (staging / name).replace(run_dir / name)
            _sync(run_dir)
            (staging / MANIFEST_NAME).replace(descriptor)
            (staging / "source_manifest.json").unlink()
            staging.rmdir()
            staging = None
        else:
            temporary.replace(archive)
            legacy._atomic_json(descriptor, receipt)
        _sync(run_dir)
        legacy._remove_tree(restore_dir, selected)
        restore_dir = None
        legacy._stopped_worker(worker_process)
        assert_files_inactive(entries)
        result.update(archive=ARCHIVE_NAME, manifest=MANIFEST_NAME, source_sha256=digest,
                      archive_sha256=receipt["archive_sha256"], source_restore_verified=True,
                      test_outcome=outcome, source_file_count=len(source["files"]),
                      schema_version=receipt["schema_version"], source_object_count=len(receipt.get("object_index", {})),
                      original_reclaimable_allocated_bytes=_reclaimable_allocation(entries),
                      source_archive_and_manifest_allocated_bytes=(archive.stat().st_blocks + descriptor.stat().st_blocks) * 512)
        result["source_storage_allocated_bytes"] = result["source_archive_and_manifest_allocated_bytes"]
        if large:
            result["source_storage_allocated_bytes"] += sum(path.stat().st_blocks * 512 for path in (
                run_dir / objects.READER_NAME, run_dir / objects.OBJECTS_NAME,
                *(run_dir / objects.OBJECTS_NAME).iterdir()))
            result["independent_reader_verified"] = True
        removing = True
        legacy._remove_tree(workspace, entries)
        result.update(status="source_archived", workspace_removed=True, original_workspace_recoverable=False)
    except (OSError, ValueError, KeyError, TypeError, EOFError, tarfile.TarError,
            subprocess.SubprocessError, legacy.ArtifactStorageError) as error:
        result["reason"] = f"{type(error).__name__}:{error}"
        if removing:
            result.update(status="source_archive_available_cleanup_incomplete", workspace_removed=not workspace.exists(),
                          original_workspace_recoverable=False)
    finally:
        if staging is not None and staging.exists():
            result["temporary_package_retained"] = str(staging)
        if restore_dir is not None and restore_dir.exists():
            try:
                legacy._remove_tree(restore_dir, selected)
            except OSError:
                result["temporary_restore_retained"] = str(restore_dir)
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def _verified_source(run_dir: Path) -> tuple[Path, dict]:
    run_dir = legacy._safe_path(run_dir)
    receipt = legacy._regular_json(run_dir / MANIFEST_NAME)
    source_path = run_dir / "source_manifest.json"
    source = legacy._regular_json(source_path)
    if (type(receipt.get("schema_version")) is not int or receipt["schema_version"] not in (1, 2)
            or receipt.get("artifact_type") != ARTIFACT_TYPE
            or receipt.get("archive") != ARCHIVE_NAME or receipt.get("source_restore_verified") is not True
            or receipt.get("original_workspace_recoverable") is not False
            or legacy._sha(source_path) != receipt.get("source_manifest_sha256")
            or manifest_digest(source["files"]) != source.get("sha256")
            or receipt.get("source_sha256") != source["sha256"]):
        raise legacy.ArtifactStorageError("invalid_test_source_archive")
    selected = _source_entries(receipt["entries"], source["files"])
    if selected != receipt["entries"]:
        raise legacy.ArtifactStorageError("test_source_archive_extra_members")
    migration_path = run_dir / MIGRATION_NAME
    if migration_path.exists() or migration_path.is_symlink():
        migration = legacy._regular_json(migration_path)
        package = legacy._safe_path(run_dir / MIGRATED_PACKAGE_NAME)
        if (receipt["schema_version"] != 1 or type(migration.get("schema_version")) is not int
                or migration["schema_version"] != 1 or migration.get("status") != "committed"
                or migration.get("package") != MIGRATED_PACKAGE_NAME
                or migration.get("original_receipt_sha256") != legacy._sha(run_dir / MANIFEST_NAME)
                or migration.get("original_archive_sha256") != receipt["archive_sha256"]
                or migration.get("source_sha256") != receipt["source_sha256"]
                or migration.get("source_manifest_sha256") != legacy._sha(source_path)
                or migration.get("package_receipt_sha256") != legacy._sha(package / MANIFEST_NAME)
                or (package / MIGRATION_NAME).exists() or (package / MIGRATION_NAME).is_symlink()):
            raise legacy.ArtifactStorageError("invalid_test_source_migration")
        archive, migrated = _verified_source(package)
        if (migrated["schema_version"] != 2 or migrated["entries"] != receipt["entries"]
                or migrated["source_sha256"] != receipt["source_sha256"]
                or migrated["source_manifest_sha256"] != receipt["source_manifest_sha256"]):
            raise legacy.ArtifactStorageError("migrated_source_identity_mismatch")
        return archive, migrated
    archive = legacy._safe_path(run_dir / ARCHIVE_NAME)
    _identity(archive)
    if archive.stat().st_size != receipt["archive_bytes"] or legacy._sha(archive) != receipt["archive_sha256"]:
        raise legacy.ArtifactStorageError("source_archive_sha256_mismatch")
    if receipt["schema_version"] == 2:
        selected = objects.verify_objects(run_dir, receipt, time.monotonic() + legacy.MAX_SECONDS)
        objects.verify_reader(run_dir, receipt)
    legacy._verify_archive(archive, selected)
    return archive, receipt


def restore_sources(manifest_path: Path, target: Path) -> dict:
    manifest_path = legacy._safe_path(manifest_path)
    if manifest_path.name != MANIFEST_NAME:
        raise legacy.ArtifactStorageError("source_archive_manifest_name_invalid")
    with source_archive_lock(manifest_path):
        archive, receipt = _verified_source(manifest_path.parent)
        if receipt["schema_version"] == 2:
            objects.restore_objects(archive, receipt, target, time.monotonic() + legacy.MAX_SECONDS)
        else:
            legacy._restore(archive, receipt["entries"], target, time.monotonic() + legacy.MAX_SECONDS)
    return {"status": "source_restored", "source_sha256": receipt["source_sha256"], "target": str(target),
            "source_restore_verified": True, "original_workspace_recoverable": False,
            "source_file_count": sum(item["kind"] == "file" for item in receipt["entries"].values())}


@contextmanager
def _pool_lock(pool: Path, *, create: bool = True):
    pool = legacy._safe_path(pool)
    if create:
        pool.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(pool / ".lock", os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def _share_file(pool: Path, archive: Path, digest: str, size: int, suffix: str) -> dict:
    original = _identity(archive)
    destination = pool / (digest + suffix)
    if destination.exists() or destination.is_symlink():
        legacy._safe_path(destination)
        _identity(destination)
        if destination.stat().st_size != size or legacy._sha(destination) != digest:
            raise legacy.ArtifactStorageError("test_source_pool_object_corrupt")
        if os.path.samefile(archive, destination):
            return {"status": "already_shared", "reclaimed_allocated_bytes_estimate": 0}
        assert_files_inactive({archive.name: {"device": original[0], "inode": original[1]}})
        temporary = archive.with_name(".source_share_" + uuid.uuid4().hex)
        os.link(destination, temporary, follow_symlinks=False)
        try:
            if _identity(archive) != original or legacy._sha(archive) != digest:
                raise legacy.ArtifactStorageError("test_source_archive_changed_before_sharing")
            info = archive.stat()
            reclaimed = info.st_blocks * 512 if info.st_nlink == 1 else 0
            temporary.replace(archive)
            _sync(archive.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
        return {"status": "shared", "reclaimed_allocated_bytes_estimate": reclaimed}
    if _identity(archive) != original or legacy._sha(archive) != digest:
        raise legacy.ArtifactStorageError("test_source_archive_changed_before_sharing")
    os.link(archive, destination, follow_symlinks=False)
    if legacy._sha(destination) != digest or not os.path.samefile(archive, destination):
        destination.unlink()
        raise legacy.ArtifactStorageError("test_source_archive_changed_while_registering")
    _sync(pool)
    return {"status": "registered", "reclaimed_allocated_bytes_estimate": 0}

def share_test_sources(project_root: Path, run_dir: Path) -> dict:
    """Share immutable local package files; a run remains independently movable."""
    with source_archive_lock(run_dir / MANIFEST_NAME):
        return _share_sources_unlocked(project_root, run_dir)


def _share_sources_unlocked(project_root: Path, run_dir: Path) -> dict:
    archive, receipt = _verified_source(run_dir)
    pool = legacy._safe_path(project_root / "reports" / POOL_NAME)
    candidates = [(archive, receipt["archive_sha256"], receipt["archive_bytes"], ".tar.gz")]
    if receipt["schema_version"] == 2:
        candidates.append((archive.parent / objects.READER_NAME, receipt["reader_sha256"], receipt["reader_bytes"], ".pyz"))
        candidates.extend((archive.parent / objects.OBJECTS_NAME / (key + ".gz"), key, record["bytes"], ".gz")
                          for key, record in receipt["object_index"].items())
    with _pool_lock(pool):
        results = [_share_file(pool, *candidate) for candidate in candidates]
    statuses = Counter(row["status"] for row in results)
    return {"status": next(status for status in ("shared", "registered", "already_shared") if statuses[status]),
            "reclaimed_allocated_bytes_estimate": sum(row["reclaimed_allocated_bytes_estimate"] for row in results),
            "file_counts": dict(statuses)}


def collect_unused_test_sources(project_root: Path, *, apply: bool = False, now: float | None = None) -> dict:
    """Only cache objects with no remaining source-archive hardlinks can expire."""
    pool = legacy._safe_path(project_root / "reports" / POOL_NAME)
    now = time.time() if now is None else now
    result = {"scope": "unreferenced_test_source_objects", "dry_run": not apply, "planned_count": 0,
              "removed_count": 0, "removed_allocated_bytes_estimate": 0, "scan_complete": True, "errors": []}
    if not pool.exists():
        return result
    started, hashed_bytes = time.monotonic(), 0
    with _pool_lock(pool, create=apply):
        with os.scandir(pool) as entries:
            for index, entry in enumerate(entries):
                if index >= 4096 or time.monotonic() - started > 30:
                    result["scan_complete"] = False
                    break
                if entry.name == ".lock":
                    continue
                try:
                    path = legacy._safe_path(Path(entry.path))
                    if re.fullmatch(r"[a-f0-9]{64}\.(tar\.gz|gz|pyz)", entry.name) is None:
                        continue
                    identity = _identity(path)
                    info = path.stat()
                    if info.st_nlink != 1 or now - info.st_mtime < 86400:
                        continue
                    if result["planned_count"] >= 32:
                        continue
                    if hashed_bytes + info.st_size > 256 * 1024**2:
                        result["scan_complete"] = False
                        break
                    hashed_bytes += info.st_size
                    if legacy._sha(path) != entry.name.split(".", 1)[0]:
                        raise legacy.ArtifactStorageError("test_source_pool_object_corrupt")
                    assert_files_inactive({entry.name: {"device": info.st_dev, "inode": info.st_ino}})
                    result["planned_count"] += 1
                    if apply:
                        if _identity(path) != identity or path.stat().st_nlink != 1:
                            raise legacy.ArtifactStorageError("test_source_pool_object_changed")
                        path.unlink()
                        _sync(pool)
                        result["removed_count"] += 1
                        result["removed_allocated_bytes_estimate"] += info.st_blocks * 512
                except (OSError, ValueError, legacy.ArtifactStorageError) as error:
                    result["errors"].append({"path": entry.name, "error": str(error)})
    return result
