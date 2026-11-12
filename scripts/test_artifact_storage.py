"""Lossless storage for completed isolated tests, preserving their original outcome."""
from __future__ import annotations

from collections import defaultdict
import ctypes
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import tarfile
import time
import uuid

from scripts.isolation_support import CONTEXT_FILE, manifest_digest

ARCHIVE_NAME = "workspace.tar.gz"
MANIFEST_NAME = "workspace_archive.json"
MAX_FILES = 20000
MAX_BYTES = 512 * 1024**2
MAX_SECONDS = 180


class ArtifactStorageError(RuntimeError):
    pass


def _regular_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise ArtifactStorageError(f"regular_json_required:{path.name}")
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ArtifactStorageError(f"object_required:{path.name}")
    return payload


def _safe_path(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ArtifactStorageError("symlink_in_workspace_path")
    return path


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _scan(workspace: Path, deadline: float) -> dict[str, dict]:
    result, pending, total = {}, [workspace], 0
    while pending:
        if len(result) >= MAX_FILES or time.monotonic() > deadline:
            raise ArtifactStorageError("workspace_resource_budget_exhausted")
        path = pending.pop()
        info = path.lstat()
        name = path.relative_to(workspace).as_posix()
        item = {"mode": stat.S_IMODE(info.st_mode), "mtime_ns": info.st_mtime_ns,
                "device": info.st_dev, "inode": info.st_ino, "links": info.st_nlink,
                "allocated_bytes": info.st_blocks * 512}
        if stat.S_ISDIR(info.st_mode):
            item["kind"] = "directory"
            with os.scandir(path) as entries:
                pending.extend(sorted((Path(entry.path) for entry in entries), reverse=True))
        elif stat.S_ISLNK(info.st_mode):
            item.update(kind="symlink", target=os.readlink(path))
        elif stat.S_ISREG(info.st_mode):
            total += info.st_size
            if total > MAX_BYTES:
                raise ArtifactStorageError("workspace_byte_budget_exhausted")
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
                opened = os.fstat(stream.fileno())
                digest = hashlib.sha256()
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
                final = os.fstat(stream.fileno())
            identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
            if identity(info) != identity(opened) or identity(opened) != identity(final):
                raise ArtifactStorageError("workspace_changed_while_reading")
            item.update(kind="file", size=info.st_size, sha256=digest.hexdigest())
        else:
            raise ArtifactStorageError("unsupported_workspace_entry:" + name)
        result[name] = item
    return dict(sorted(result.items()))


def _content(entries: dict[str, dict]) -> dict:
    return {name: {key: value for key, value in item.items()
                   if key in {"kind", "mode", "mtime_ns", "target", "size", "sha256"}}
            for name, item in entries.items()}


def _hardlinks(entries: dict[str, dict]) -> list[list[str]]:
    groups = defaultdict(list)
    for name, item in entries.items():
        if item["kind"] == "file":
            groups[(item["device"], item["inode"])].append(name)
    return sorted(sorted(names) for names in groups.values() if len(names) > 1)


def _archive_member_name(name: str) -> str:
    return "workspace" if name == "." else "workspace/" + name


def _verify_archive(archive: Path, entries: dict[str, dict]) -> None:
    # Tar iteration stops at its end marker, which can precede gzip's CRC/size
    # trailer. Drain the bounded compression stream before trusting recovery.
    decoded, deadline = 0, time.monotonic() + MAX_SECONDS
    with gzip.open(archive, "rb") as compressed:
        for block in iter(lambda: compressed.read(1024 * 1024), b""):
            decoded += len(block)
            if decoded > MAX_BYTES + MAX_FILES * 8192 + 10240 or time.monotonic() > deadline:
                raise ArtifactStorageError("archive_decode_resource_budget_exhausted")
    expected = {_archive_member_name(name): item for name, item in entries.items()}
    seen = set()
    with tarfile.open(archive, "r:gz") as package:
        for member in package:
            if member.name in seen or member.name not in expected:
                raise ArtifactStorageError("archive_members_differ")
            seen.add(member.name)
            item = expected[member.name]
            if stat.S_IMODE(member.mode) != item["mode"]:
                raise ArtifactStorageError("archive_mode_mismatch")
            if item["kind"] == "file":
                if not (member.isfile() or member.islnk()):
                    raise ArtifactStorageError("archive_file_type_mismatch")
                if member.islnk() and (member.linkname not in expected or expected[member.linkname]["kind"] != "file"):
                    raise ArtifactStorageError("archive_hardlink_outside_members")
                digest, size = hashlib.sha256(), 0
                with package.extractfile(member) as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                        size += len(block)
                if digest.hexdigest() != item["sha256"] or size != item["size"]:
                    raise ArtifactStorageError("archive_file_bytes_mismatch")
            elif item["kind"] == "symlink":
                if not member.issym() or member.linkname != item["target"]:
                    raise ArtifactStorageError("archive_symlink_mismatch")
            elif item["kind"] != "directory" or not member.isdir():
                raise ArtifactStorageError("archive_directory_mismatch")
    if seen != set(expected):
        raise ArtifactStorageError("archive_members_missing")


def _validate_entries(entries: dict) -> None:
    if not isinstance(entries, dict) or entries.get(".", {}).get("kind") != "directory":
        raise ArtifactStorageError("archive_root_missing")
    for name, item in entries.items():
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or str(path) != name or item.get("kind") not in {"file", "directory", "symlink"}:
            raise ArtifactStorageError("unsafe_archive_member")
        for parent in path.parents:
            if entries.get(str(parent), {}).get("kind") != "directory":
                raise ArtifactStorageError("archive_member_parent_is_not_directory")


def _restore(archive: Path, entries: dict, target: Path, deadline: float) -> None:
    _validate_entries(entries)
    target = _safe_path(target)
    target.mkdir(exist_ok=False)
    for name, item in sorted(entries.items(), key=lambda pair: len(PurePosixPath(pair[0]).parts)):
        if name != "." and item["kind"] == "directory":
            (target / name).mkdir()
    symlinks, hardlinks = [], []
    with tarfile.open(archive, "r:gz") as package:
        for member in package:
            if time.monotonic() > deadline:
                raise ArtifactStorageError("restore_resource_budget_exhausted")
            name = "." if member.name == "workspace" else member.name.removeprefix("workspace/")
            item = entries[name]
            path = target / name
            if member.islnk():
                hardlinks.append((path, target / member.linkname.removeprefix("workspace/")))
            elif item["kind"] == "file":
                with package.extractfile(member) as source, path.open("xb") as output:
                    shutil.copyfileobj(source, output, 1024 * 1024)
            elif item["kind"] == "symlink":
                symlinks.append((path, item["target"]))
    for path, source in hardlinks:
        os.link(source, path, follow_symlinks=False)
    # No data is ever extracted through a symbolic link; link targets are only
    # restored after all regular paths have been created.
    for path, value in symlinks:
        path.symlink_to(value)
    for name, item in sorted(entries.items(), key=lambda pair: len(PurePosixPath(pair[0]).parts), reverse=True):
        path = target / name
        if item["kind"] != "symlink":
            path.chmod(item["mode"])
        os.utime(path, ns=(item["mtime_ns"], item["mtime_ns"]), follow_symlinks=False)
    restored = _scan(target, deadline)
    if _content(restored) != _content(entries) or _hardlinks(restored) != _hardlinks(entries):
        raise ArtifactStorageError("restored_workspace_mismatch")


def _remove_tree(root: Path, entries: dict) -> None:
    for name, item in sorted(entries.items(), key=lambda pair: len(PurePosixPath(pair[0]).parts)):
        if item["kind"] == "directory":
            (root / name).chmod(item["mode"] | stat.S_IRWXU)
    shutil.rmtree(root)


def _atomic_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def enable_worker_child_tracking() -> bool:
    """Adopt orphaned test children, including children that start a new session."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        return libc.prctl(36, 1, 0, 0, 0) == 0  # PR_SET_CHILD_SUBREAPER
    except (AttributeError, OSError):
        return False


def _stopped_worker(process: subprocess.Popen) -> int:
    return_code = process.poll()
    if return_code is None:
        raise ArtifactStorageError("test_worker_still_running")
    tracking = ctypes.c_int()
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        tracked = libc.prctl(37, ctypes.byref(tracking), 0, 0, 0) == 0 and tracking.value == 1  # PR_GET_CHILD_SUBREAPER
    except (AttributeError, OSError):
        tracked = False
    if not tracked:
        raise ArtifactStorageError("worker_child_tracking_unavailable")
    while True:
        try:
            child, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if child == 0:
            raise ArtifactStorageError("worker_descendants_still_running")
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return return_code
    raise ArtifactStorageError("worker_process_group_still_exists")


def _test_outcome(summary: dict, worker: dict | None, process: subprocess.Popen | None) -> dict:
    exit_code = summary.get("exit_code")
    timed_out = summary.get("timed_out")
    if (type(exit_code) is not int or type(timed_out) is not bool
            or summary.get("source_unchanged") is not True):
        raise ArtifactStorageError("test_terminal_not_verified")
    if process is None:
        # Compatibility for older successful-run maintenance callers. New
        # automatic retirement additionally requires the actual worker handle.
        if (exit_code != 0 or timed_out or worker is None
                or type(worker.get("exit_code")) is not int or worker["exit_code"] != 0):
            raise ArtifactStorageError("test_not_verified_successful")
        process_code = None
    else:
        if summary.get("worker_child_tracking_available") is not True:
            raise ArtifactStorageError("worker_child_tracking_unavailable")
        process_code = _stopped_worker(process)
        if exit_code != (124 if timed_out else process_code):
            raise ArtifactStorageError("test_process_exit_mismatch")
        if worker is not None and (type(worker.get("exit_code")) is not int
                or (not timed_out and worker["exit_code"] != process_code)):
            raise ArtifactStorageError("test_worker_exit_mismatch")
        if worker is None and exit_code == 0:
            raise ArtifactStorageError("successful_test_worker_result_missing")
    return {"exit_code": exit_code, "timed_out": timed_out,
            "worker_result_present": worker is not None,
            "worker_exit_code": worker.get("exit_code") if worker is not None else None,
            "process_return_code": process_code,
            "worker_process_group_stopped": True if process is not None else None,
            "worker_descendants_stopped": True if process is not None else None}


def archive_successful_workspace(run_dir: Path) -> dict:
    """Legacy successful-run entry point; nonzero or incomplete runs are retained."""
    return _archive_workspace(run_dir)


def archive_completed_workspace(run_dir: Path, worker_process: subprocess.Popen) -> dict:
    """Keep failed/timeout evidence losslessly only after the worker group exits."""
    return _archive_workspace(run_dir, worker_process=worker_process)


def _archive_workspace(run_dir: Path, *, worker_process: subprocess.Popen | None = None) -> dict:
    started = time.monotonic()
    restore_dir = None
    entries = None
    retirement_started = False
    result = {"status": "retained", "workspace_removed": False}
    try:
        run_dir = _safe_path(run_dir)
        workspace = _safe_path(run_dir / "workspace")
        summary = _regular_json(run_dir / "summary.json")
        manifest = _regular_json(run_dir / "source_manifest.json")
        context = _regular_json(workspace / CONTEXT_FILE)
        worker_path = workspace / ".test_result.json"
        worker = _regular_json(worker_path) if worker_path.exists() or worker_path.is_symlink() else None
        digest = manifest_digest(manifest["files"])
        if summary.get("scope") != "isolated_unit_regression" or context.get("scope") != "isolated_unit_regression":
            raise ArtifactStorageError("isolated_test_scope_required")
        outcome = _test_outcome(summary, worker, worker_process)
        result["test_outcome"] = outcome
        if not digest == manifest.get("sha256") == context.get("source_sha256") == summary.get("source_sha256"):
            raise ArtifactStorageError("test_source_identity_mismatch")
        archive, descriptor = run_dir / ARCHIVE_NAME, run_dir / MANIFEST_NAME
        temporary = archive.with_name(archive.name + ".tmp")
        if any(path.exists() or path.is_symlink() for path in (archive, descriptor, temporary)):
            raise ArtifactStorageError("existing_archive_not_overwritten")
        deadline = started + MAX_SECONDS
        entries = _scan(workspace, deadline)
        logical = sum(item.get("size", 0) for item in entries.values())
        fs = os.statvfs(run_dir)
        if fs.f_bavail * fs.f_frsize < 2 * logical + 64 * 1024**2:
            raise ArtifactStorageError("insufficient_archive_and_restore_space")
        seen_inodes = set()
        allocated = 0
        for item in entries.values():
            key = item["device"], item["inode"]
            if key not in seen_inodes:
                allocated += item["allocated_bytes"]
                seen_inodes.add(key)
        with tarfile.open(temporary, "x:gz", compresslevel=6, dereference=False) as package:
            for name in entries:
                if time.monotonic() > deadline:
                    raise ArtifactStorageError("archive_resource_budget_exhausted")
                package.add(workspace / name, arcname=_archive_member_name(name), recursive=False)
        temporary.chmod(0o600)
        _verify_archive(temporary, entries)
        restore_dir = run_dir / (".workspace_restore_verify_" + uuid.uuid4().hex)
        _restore(temporary, entries, restore_dir, deadline)
        if _scan(workspace, deadline) != entries:
            raise ArtifactStorageError("workspace_changed_before_retirement")
        if worker_process is not None:
            _stopped_worker(worker_process)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        temporary.replace(archive)
        receipt = {"schema_version": 1, "verified_at": datetime.now().astimezone().isoformat(),
                   "source_sha256": digest, "archive": ARCHIVE_NAME, "archive_sha256": _sha(archive),
                   "archive_bytes": archive.stat().st_size, "original_logical_bytes": logical,
                   "original_allocated_bytes": allocated, "entries": entries,
                   "full_restore_verified": True, "regular_bytes_and_links_preserved": True,
                   "scope": "Completed isolated test workspace; original test outcome retained, not production business data.",
                   "test_outcome": outcome,
                   "restore_command": ["python3", "scripts/restore_isolated_test_workspace.py", "--manifest",
                                       str(descriptor), "--target", "<new-empty-workspace-path>"]}
        _atomic_json(descriptor, receipt)
        directory_fd = os.open(run_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        result.update(archive=ARCHIVE_NAME, manifest=MANIFEST_NAME, source_sha256=digest,
                      archive_sha256=receipt["archive_sha256"], full_restore_verified=True,
                      original_allocated_bytes=allocated, archive_bytes=receipt["archive_bytes"],
                      archive_and_manifest_allocated_bytes=(archive.stat().st_blocks + descriptor.stat().st_blocks) * 512,
                      member_count=len(entries))
        _remove_tree(restore_dir, entries)
        restore_dir = None
        if worker_process is not None:
            _stopped_worker(worker_process)
        retirement_started = True
        _remove_tree(workspace, entries)
        result.update(status="archived", workspace_removed=True)
    except (OSError, ValueError, KeyError, TypeError, EOFError, tarfile.TarError, ArtifactStorageError) as error:
        result.update(reason=f"{type(error).__name__}:{error}")
        if retirement_started:
            result.update(status="archive_available_cleanup_incomplete", workspace_removed=not workspace.exists())
    finally:
        if restore_dir is not None and restore_dir.exists() and entries is not None:
            try:
                _remove_tree(restore_dir, entries)
            except OSError:
                result["temporary_restore_retained"] = str(restore_dir)
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def restore_workspace(manifest_path: Path, target: Path) -> dict:
    manifest_path = _safe_path(manifest_path)
    receipt = _regular_json(manifest_path)
    if receipt.get("schema_version") != 1 or receipt.get("archive") != ARCHIVE_NAME or receipt.get("full_restore_verified") is not True:
        raise ArtifactStorageError("unsupported_workspace_archive")
    archive = _safe_path(manifest_path.parent / ARCHIVE_NAME)
    if _sha(archive) != receipt["archive_sha256"]:
        raise ArtifactStorageError("archive_sha256_mismatch")
    entries = receipt["entries"]
    _validate_entries(entries)
    _verify_archive(archive, entries)
    _restore(archive, entries, target, time.monotonic() + MAX_SECONDS)
    return {"status": "restored", "member_count": len(entries), "target": str(target),
            "source_sha256": receipt["source_sha256"], "full_restore_verified": True}
