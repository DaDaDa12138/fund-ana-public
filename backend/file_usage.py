"""Fail closed when a local artifact is still open or memory-mapped."""
from __future__ import annotations

from collections.abc import Iterable
import os
from pathlib import Path


class FileUsageError(RuntimeError):
    pass


def _process_paths() -> Iterable[Path]:
    return Path("/proc").iterdir()


def assert_files_inactive(entries: dict[str, dict], *, processes: Iterable[Path] | None = None) -> None:
    """Inspect kernel identity metadata without opening another process's data."""
    identities = {(item["device"], item["inode"]) for item in entries.values()}
    mapped = {(os.major(dev), os.minor(dev), inode) for dev, inode in identities}
    inodes = {inode for _, inode in identities}
    for process in _process_paths() if processes is None else processes:
        if not process.name.isdigit():
            continue
        try:
            mounts = None
            for descriptor in (process / "fdinfo").iterdir():
                try:
                    fields = dict(line.split(":", 1) for line in descriptor.read_text().splitlines() if ":" in line)
                except FileNotFoundError:
                    continue
                if "ino" not in fields or "mnt_id" not in fields:
                    raise FileUsageError("file_descriptor_identity_unavailable")
                inode = int(fields["ino"])
                if inode not in inodes:
                    continue
                if mounts is None:
                    mounts = {int(parts[0]): tuple(int(value) for value in parts[2].split(":"))
                              for parts in (line.split() for line in (process / "mountinfo").read_text().splitlines())}
                mount = mounts.get(int(fields["mnt_id"]))
                if mount is None:
                    raise FileUsageError("file_descriptor_mount_unavailable")
                if (*mount, inode) in mapped:
                    raise FileUsageError("file_open")
            for line in (process / "maps").read_text().splitlines():
                fields = line.split(maxsplit=5)
                major, minor = (int(part, 16) for part in fields[3].split(":"))
                if (major, minor, int(fields[4])) in mapped:
                    raise FileUsageError("file_mapped")
        except (FileNotFoundError, ProcessLookupError):
            continue
