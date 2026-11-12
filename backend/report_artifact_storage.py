"""Share completed report files without changing their bytes or download paths.

Only the workflow's final, immutable core CSV/XLSX/JSON copies and explicitly
referenced report CSV/XLSX files enter this pool. Mutable core output, report
control JSON, global Tracker aliases and unfinished reports never do.
Every report retains ordinary local files: losing the pool, moving a report or
deleting another report does not break reads. Any later edit must use a new
copy/revision, as report_builder already does.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Iterator
from uuid import uuid4

from backend.publication_transaction import reports_root


POOL_NAME = ".report_artifact_pool"
MIN_FILE_BYTES = 256 * 1024
MAX_FILE_BYTES = 64 * 1024**2
MAX_RUN_FILES = 256
MAX_RUN_BYTES = 512 * 1024**2
MAX_CORE_DIRECTORY_DEPTH = 4
MAX_POOL_FILES = 2048
MAX_POOL_BYTES = 512 * 1024**2
MIN_GC_AGE_SECONDS = 24 * 3600
MAX_GC_FILES = 32
_OBJECT_NAME = re.compile(r"[0-9a-f]{64}\.[0-9a-f]{64}\Z")
_TEMP_NAME = re.compile(r"\.link-[0-9a-f]{32}\.tmp\Z")


class ReportArtifactStorageError(RuntimeError):
    pass


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _safe_path(path: Path, root: Path) -> None:
    if not path.is_absolute() or not path.is_relative_to(root):
        raise ReportArtifactStorageError("artifact_path_outside_root")
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise ReportArtifactStorageError("artifact_symlink_not_allowed")
        if candidate == root:
            break


def _fingerprint(path: Path, *, max_bytes: int = MAX_FILE_BYTES) -> tuple[os.stat_result, str]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= max_bytes:
            raise ReportArtifactStorageError("artifact_type_or_size_not_supported")
        # Do not merge ACL/security metadata we have not validated.
        if os.listxattr(stream.fileno()):
            raise ReportArtifactStorageError("artifact_extended_attributes_not_supported")
        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            block = stream.read(min(1024**2, remaining))
            if not block:
                raise ReportArtifactStorageError("artifact_truncated_during_read")
            digest.update(block)
            remaining -= len(block)
        if stream.read(1) or _identity(os.fstat(stream.fileno())) != _identity(before):
            raise ReportArtifactStorageError("artifact_changed_during_read")
    if _identity(path.lstat()) != _identity(before):
        raise ReportArtifactStorageError("artifact_replaced_during_read")
    return before, digest.hexdigest()


def _object_name(info: os.stat_result, digest: str) -> str:
    metadata = [info.st_size, stat.S_IMODE(info.st_mode), info.st_uid, info.st_gid, info.st_mtime_ns]
    encoded = json.dumps(metadata, separators=(",", ":")).encode()
    return f"{digest}.{hashlib.sha256(encoded).hexdigest()}"


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def _pool_lock(pool: Path, root: Path) -> Iterator[None]:
    _safe_path(pool, root)
    pool.mkdir(exist_ok=True)
    descriptor = os.open(pool / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ReportArtifactStorageError("artifact_pool_lock_invalid")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def _pool_entries(pool: Path) -> list[tuple[Path, os.stat_result]]:
    entries = []
    with os.scandir(pool) as iterator:
        for entry in iterator:
            if entry.name == ".lock":
                continue
            if not (_OBJECT_NAME.fullmatch(entry.name) or _TEMP_NAME.fullmatch(entry.name)):
                raise ReportArtifactStorageError("artifact_pool_unknown_entry")
            info = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode):
                raise ReportArtifactStorageError("artifact_pool_non_regular_entry")
            entries.append((Path(entry.path), info))
            if len(entries) > MAX_POOL_FILES + MAX_RUN_FILES:
                raise ReportArtifactStorageError("artifact_pool_scan_budget_exceeded")
    return entries


def _collect_locked(pool: Path, *, apply: bool, now: float) -> dict[str, Any]:
    result: dict[str, Any] = {"dry_run": not apply, "planned": [], "removed": [], "errors": [],
                              "removed_allocated_bytes_estimate": 0, "net_reclaimed_disk_bytes": None}
    entries = _pool_entries(pool)
    # Orphaned atomic-replacement links must be released before checking the
    # object link count. ctime tracks link/unlink; mtime is the report's mtime.
    entries.sort(key=lambda row: (not bool(_TEMP_NAME.fullmatch(row[0].name)), row[1].st_ctime_ns))
    for path, _ in entries:
        info = path.lstat()
        temporary = bool(_TEMP_NAME.fullmatch(path.name))
        if info.st_ctime > now - MIN_GC_AGE_SECONDS or (not temporary and info.st_nlink != 1):
            continue
        if len(result["planned"]) >= MAX_GC_FILES:
            break
        item = {"name": path.name, "bytes": info.st_size, "temporary": temporary}
        result["planned"].append(item)
        if not apply:
            continue
        try:
            current = path.lstat()
            if _identity(current) != _identity(info) or current.st_nlink != info.st_nlink:
                raise ReportArtifactStorageError("artifact_pool_gc_identity_changed")
            path.unlink()
            result["removed"].append(item)
            if info.st_nlink == 1:
                result["removed_allocated_bytes_estimate"] += info.st_blocks * 512
        except (OSError, ReportArtifactStorageError) as error:
            result["errors"].append({"name": path.name, "error": str(error)})
    if apply and result["removed"]:
        _sync_directory(pool)
    return result


def collect_unused_report_artifacts(settings: Any, *, apply: bool = False,
                                    now: float | None = None) -> dict[str, Any]:
    root = reports_root(settings).absolute()
    pool = root / POOL_NAME
    _safe_path(pool, root)
    if not pool.exists():
        return {"dry_run": not apply, "planned": [], "removed": [], "errors": [],
                "removed_allocated_bytes_estimate": 0, "net_reclaimed_disk_bytes": None}
    with _pool_lock(pool, root):
        return _collect_locked(pool, apply=apply, now=time.time() if now is None else now)


def _make_room(pool: Path, size: int) -> tuple[bool, int]:
    entries = _pool_entries(pool)
    total = sum(info.st_size for _, info in entries)
    count = len(entries)
    evicted = 0
    # Dropping a lookup link never removes a surviving report. Young orphan
    # objects and crash leftovers remain protected until normal GC.
    for path, info in sorted(entries, key=lambda row: row[1].st_ctime_ns):
        if count < MAX_POOL_FILES and total + size <= MAX_POOL_BYTES:
            return True, evicted
        if _TEMP_NAME.fullmatch(path.name) or info.st_nlink <= 1:
            continue
        current = path.lstat()
        if _identity(current) != _identity(info) or current.st_nlink <= 1:
            raise ReportArtifactStorageError("artifact_pool_eviction_identity_changed")
        path.unlink()
        count -= 1
        total -= info.st_size
        evicted += 1
    return count < MAX_POOL_FILES and total + size <= MAX_POOL_BYTES, evicted


def _share_file(path: Path, pool: Path, root: Path) -> dict[str, Any]:
    _safe_path(path, root)
    before, digest = _fingerprint(path)
    name = _object_name(before, digest)
    cached = pool / name
    item: dict[str, Any] = {"path": path.relative_to(root).as_posix(), "sha256": digest,
                            "bytes": before.st_size, "pool_key": name, "evicted_lookup_links": 0,
                            "removed_allocated_bytes_estimate": 0}
    if not cached.exists():
        room, evicted = _make_room(pool, before.st_size)
        item["evicted_lookup_links"] = evicted
        if not room:
            return {**item, "action": "skipped_pool_budget"}
        if _identity(path.lstat()) != _identity(before):
            raise ReportArtifactStorageError("artifact_changed_before_pool_link")
        os.link(path, cached, follow_symlinks=False)
        try:
            cached_info, cached_digest = _fingerprint(cached)
            if (_object_name(cached_info, cached_digest) != name
                    or (cached_info.st_dev, cached_info.st_ino) != (before.st_dev, before.st_ino)):
                raise ReportArtifactStorageError("artifact_changed_during_pool_link")
            _sync_directory(pool)
        except BaseException:
            cached.unlink(missing_ok=True)
            raise
        return {**item, "action": "indexed_local_file"}
    cached_info, cached_digest = _fingerprint(cached)
    if _object_name(cached_info, cached_digest) != name:
        raise ReportArtifactStorageError("artifact_pool_checksum_or_metadata_mismatch")
    if (cached_info.st_dev, cached_info.st_ino) == (before.st_dev, before.st_ino):
        return {**item, "action": "already_shared"}
    temporary = pool / f".link-{uuid4().hex}.tmp"
    try:
        os.link(cached, temporary, follow_symlinks=False)
        linked_info, linked_digest = _fingerprint(temporary)
        if _object_name(linked_info, linked_digest) != name:
            raise ReportArtifactStorageError("artifact_pool_changed_before_replace")
        if (_identity(path.lstat()) != _identity(before)
                or _identity(temporary.lstat()) != _identity(linked_info)):
            raise ReportArtifactStorageError("artifact_changed_before_replace")
        os.replace(temporary, path)
        # Count only the removed inode's last directory link, never its logical
        # size once per alias. Open readers may delay actual disk reclamation.
        if before.st_nlink == 1:
            item["removed_allocated_bytes_estimate"] = before.st_blocks * 512
        _sync_directory(path.parent)
        _sync_directory(pool)
    finally:
        temporary.unlink(missing_ok=True)
    return {**item, "action": "shared"}


def _report_candidates(settings: Any, report: dict[str, Any], report_path: Path,
                       root: Path, result: dict[str, Any]) -> Iterator[Path]:
    """Bound the entire traversal, including ignored files and directories.

    core_artifacts contains independent copies made for this report. Nested
    .audit JSON is part of those inputs, not the run's later task logs. Top-level
    report files require an explicit artifact reference and exclude control JSON.
    """
    directory = report_path.parent / "core_artifacts"
    _safe_path(directory, root)
    pending = [(directory, 0)] if directory.is_dir() else []
    seen = 0
    while pending:
        current, depth = pending.pop()
        _safe_path(current, root)
        with os.scandir(current) as entries:
            for entry in entries:
                seen += 1
                if seen > MAX_RUN_FILES:
                    result["run_budget_exhausted"] = True
                    return
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    if depth >= MAX_CORE_DIRECTORY_DEPTH:
                        result["run_budget_exhausted"] = True
                    else:
                        pending.append((Path(entry.path), depth + 1))
                elif (stat.S_ISREG(info.st_mode) and Path(entry.name).suffix.lower() in {".csv", ".xlsx", ".json"}
                      and MIN_FILE_BYTES <= info.st_size <= MAX_FILE_BYTES):
                    yield Path(entry.path)

    referenced: set[Path] = set()
    for value in (report.get("artifacts") or {}).values():
        seen += 1
        if seen > MAX_RUN_FILES:
            result["run_budget_exhausted"] = True
            return
        if not isinstance(value, str):
            continue
        raw = Path(value)
        if raw.suffix.lower() not in {".csv", ".xlsx"}:
            continue
        path = Path(os.path.abspath(raw if raw.is_absolute() else Path(settings.project_root) / raw))
        if path.parent != report_path.parent or path in referenced:
            continue
        referenced.add(path)
        _safe_path(path, root)
        info = path.lstat()
        if stat.S_ISREG(info.st_mode) and MIN_FILE_BYTES <= info.st_size <= MAX_FILE_BYTES:
            yield path


def share_finalized_report(settings: Any, report: dict[str, Any]) -> dict[str, Any]:
    """Called after ALL report writers/audits, while the workflow lock is held.

    This completion boundary is about artifact writers. It precedes recording
    workflow success, so interruption must preserve every report and local file.
    The optional storage receipt is separate from the immutable publication.
    """
    root = reports_root(settings).absolute()
    run_id = str(report.get("run_id") or "")
    if not run_id or Path(run_id).name != run_id or run_id in {".", ".."}:
        raise ReportArtifactStorageError("artifact_report_run_id_invalid")
    raw = Path(str((report.get("artifacts") or {}).get("report_json") or ""))
    path = Path(os.path.abspath(raw if raw.is_absolute() else Path(settings.project_root) / raw))
    run_dir = Path(settings.runs_dir).absolute() / run_id
    _safe_path(path, root)
    relative = path.relative_to(run_dir)
    if (path.name != f"bond_analysis_report_{run_id}.json"
            or not (len(relative.parts) == 1 or (len(relative.parts) == 3 and relative.parts[0] == "revisions"))):
        raise ReportArtifactStorageError("artifact_report_revision_path_invalid")
    report_info, report_sha = _fingerprint(path, max_bytes=32 * 1024**2)
    if json.loads(path.read_text(encoding="utf-8")) != report or _identity(path.lstat()) != _identity(report_info):
        raise ReportArtifactStorageError("artifact_report_payload_mismatch")
    result: dict[str, Any] = {"schema_version": 1, "run_id": run_id, "report_sha256": report_sha,
        "started_at": datetime.now().astimezone().isoformat(), "files": [], "errors": [],
        "removed_allocated_bytes_estimate": 0, "net_reclaimed_disk_bytes": None,
        "run_budget_exhausted": False,
        "scope": "Final core_artifacts CSV/XLSX/JSON including bounded nested copies, plus referenced report-root CSV/XLSX. Bytes, size, mode, owner and mtime preserved. Control JSON and mutable aliases excluded. Pool budgets bound lookup links, not report lifetime or total retained data."}
    with _pool_lock(root / POOL_NAME, root):
        result["gc"] = _collect_locked(root / POOL_NAME, apply=True, now=time.time())
        result["errors"].extend(result["gc"]["errors"])
        total = 0
        for candidate in _report_candidates(settings, report, path, root, result):
            info = candidate.lstat()
            if total + info.st_size > MAX_RUN_BYTES:
                result["run_budget_exhausted"] = True
                break
            total += info.st_size
            try:
                item = _share_file(candidate, root / POOL_NAME, root)
                result["files"].append(item)
                result["removed_allocated_bytes_estimate"] += item["removed_allocated_bytes_estimate"]
            except (OSError, ReportArtifactStorageError) as error:
                result["errors"].append({"path": candidate.relative_to(root).as_posix(), "error": str(error)})
        current_pool = _pool_entries(root / POOL_NAME)
        result["pool"] = {"files": len(current_pool), "logical_bytes": sum(info.st_size for _, info in current_pool),
                          "max_files": MAX_POOL_FILES, "max_bytes": MAX_POOL_BYTES}
    result["pool_budget_skipped_files"] = sum(item["action"] == "skipped_pool_budget" for item in result["files"])
    result["status"] = ("partial" if result["errors"] or result["run_budget_exhausted"]
                        or result["pool_budget_skipped_files"] else "complete")
    result["shared_files"] = sum(item["action"] == "shared" for item in result["files"])
    result["finished_at"] = datetime.now().astimezone().isoformat()
    return result
