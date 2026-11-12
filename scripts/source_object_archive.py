"""Per-run, self-contained source packages with reusable large-file objects.

Objects are compressed independently of names, modes, and timestamps. Those
properties remain in the receipt; sharing storage never makes two originally
independent source files into hardlinks when restored.
"""
from __future__ import annotations

import gzip
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time
import zipfile

from scripts import test_artifact_storage as legacy

OBJECTS_NAME = "source_objects"
READER_NAME = "restore_sources.pyz"
MIN_OBJECT_BYTES = 32 * 1024


def split_entries(entries: dict) -> tuple[dict, dict]:
    objects = {name: item for name, item in entries.items()
               if item["kind"] == "file" and item["size"] >= MIN_OBJECT_BYTES}
    return {name: item for name, item in entries.items() if name not in objects}, objects


def _budget(deadline: float) -> None:
    if time.monotonic() > deadline:
        raise legacy.ArtifactStorageError("source_object_resource_budget_exhausted")


def write_objects(directory: Path, workspace: Path, entries: dict, deadline: float) -> dict:
    directory.mkdir(mode=0o700)
    paths, index, by_content = {}, {}, {}
    for name, item in entries.items():
        _budget(deadline)
        key = item["sha256"], item["size"]
        if key not in by_content:
            temporary = directory / ".writing"
            digest, size = hashlib.sha256(), 0
            with (workspace / name).open("rb") as source, temporary.open("xb") as output:
                with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0, compresslevel=6) as compressed:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        _budget(deadline)
                        digest.update(block)
                        size += len(block)
                        if size > item["size"]:
                            raise legacy.ArtifactStorageError("source_object_source_changed")
                        compressed.write(block)
                output.flush()
                os.fsync(output.fileno())
            if (digest.hexdigest(), size) != key:
                raise legacy.ArtifactStorageError("source_object_source_changed")
            temporary.chmod(0o600)
            object_id = legacy._sha(temporary)
            temporary.rename(directory / (object_id + ".gz"))
            by_content[key] = object_id
            index[object_id] = {"bytes": (directory / (object_id + ".gz")).stat().st_size,
                                "source_sha256": key[0], "source_bytes": key[1]}
        paths[name] = by_content[key]
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"object_min_bytes": MIN_OBJECT_BYTES, "object_paths": paths, "object_index": index,
            "objects_directory": OBJECTS_NAME}


def _layout(receipt: dict) -> tuple[dict, dict]:
    entries = receipt["entries"]
    legacy._validate_entries(entries)
    if len(entries) > legacy.MAX_FILES:
        raise legacy.ArtifactStorageError("source_object_member_budget_exhausted")
    total, inodes = 0, {}
    for item in entries.values():
        if item["kind"] not in {"file", "directory"}:
            raise legacy.ArtifactStorageError("source_object_member_type_invalid")
        for field in ("mode", "mtime_ns", "device", "inode"):
            if type(item.get(field)) is not int:
                raise legacy.ArtifactStorageError("source_object_metadata_invalid")
        if not 0 <= item["mode"] <= 0o7777:
            raise legacy.ArtifactStorageError("source_object_mode_invalid")
        if item["kind"] == "file":
            if type(item.get("size")) is not int or not 0 <= item["size"] <= legacy.MAX_BYTES:
                raise legacy.ArtifactStorageError("source_object_size_invalid")
            if not isinstance(item.get("sha256"), str) or re.fullmatch(r"[a-f0-9]{64}", item["sha256"]) is None:
                raise legacy.ArtifactStorageError("source_object_hash_invalid")
            total += item["size"]
            identity = item["device"], item["inode"]
            content = tuple(item[field] for field in ("size", "sha256", "mode", "mtime_ns"))
            if inodes.setdefault(identity, content) != content:
                raise legacy.ArtifactStorageError("source_object_hardlink_metadata_invalid")
    if total > legacy.MAX_BYTES:
        raise legacy.ArtifactStorageError("source_object_byte_budget_exhausted")
    inline, large = split_entries(entries)
    paths, index = receipt.get("object_paths"), receipt.get("object_index")
    if (type(receipt.get("object_min_bytes")) is not int or receipt["object_min_bytes"] != MIN_OBJECT_BYTES
            or receipt.get("objects_directory") != OBJECTS_NAME or not large
            or not isinstance(paths, dict) or not isinstance(index, dict) or set(paths) != set(large)
            or any(not isinstance(value, str) for value in paths.values()) or set(paths.values()) != set(index)):
        raise legacy.ArtifactStorageError("source_object_mapping_invalid")
    for name, object_id in paths.items():
        if re.fullmatch(r"[a-f0-9]{64}", object_id) is None:
            raise legacy.ArtifactStorageError("source_object_name_invalid")
        record, item = index[object_id], large[name]
        if (not isinstance(record, dict) or type(record.get("bytes")) is not int
                or not 0 < record["bytes"] <= legacy.MAX_BYTES + 1024**2
                or type(record.get("source_bytes")) is not int or record["source_bytes"] != item["size"]
                or record.get("source_sha256") != item["sha256"]):
            raise legacy.ArtifactStorageError("source_object_mapping_content_mismatch")
    return inline, large


def verify_objects(package: Path, receipt: dict, deadline: float) -> dict:
    inline, _ = _layout(receipt)
    directory = legacy._safe_path(package / OBJECTS_NAME)
    if not directory.is_dir():
        raise legacy.ArtifactStorageError("source_objects_directory_missing")
    expected = {object_id + ".gz" for object_id in receipt["object_index"]}
    actual = set()
    with os.scandir(directory) as names:
        for entry in names:
            _budget(deadline)
            if entry.name not in expected or not entry.is_file(follow_symlinks=False):
                raise legacy.ArtifactStorageError("source_objects_members_differ")
            actual.add(entry.name)
    if actual != expected:
        raise legacy.ArtifactStorageError("source_objects_members_missing")
    for object_id, record in receipt["object_index"].items():
        _budget(deadline)
        path = legacy._safe_path(directory / (object_id + ".gz"))
        if path.stat().st_size != record["bytes"] or legacy._sha(path) != object_id:
            raise legacy.ArtifactStorageError("source_object_compressed_sha256_mismatch")
        digest, size = hashlib.sha256(), 0
        with gzip.open(path, "rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                _budget(deadline)
                size += len(block)
                if size > record["source_bytes"]:
                    raise legacy.ArtifactStorageError("source_object_decoded_size_exceeded")
                digest.update(block)
        if size != record["source_bytes"] or digest.hexdigest() != record["source_sha256"]:
            raise legacy.ArtifactStorageError("source_object_decoded_bytes_mismatch")
    return inline


def restore_objects(archive: Path, receipt: dict, target: Path, deadline: float) -> None:
    inline, large = _layout(receipt)
    legacy._restore(archive, inline, target, deadline)
    # Inline restoration has restored directory permissions already. Temporarily
    # permit owner writes, then reinstate every recorded mode and mtime below.
    entries = receipt["entries"]
    ordered = sorted(entries, key=lambda name: len(PurePosixPath(name).parts))
    for name in ordered:
        if entries[name]["kind"] == "directory":
            (target / name).chmod(entries[name]["mode"] | stat.S_IRWXU)
    hardlinks = {}
    for name, item in large.items():
        _budget(deadline)
        destination = target / name
        identity = item["device"], item["inode"]
        if identity in hardlinks:
            os.link(hardlinks[identity], destination, follow_symlinks=False)
            continue
        object_id = receipt["object_paths"][name]
        size = 0
        with gzip.open(archive.parent / OBJECTS_NAME / (object_id + ".gz"), "rb") as source, destination.open("xb") as output:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                _budget(deadline)
                size += len(block)
                if size > item["size"]:
                    raise legacy.ArtifactStorageError("source_object_changed_during_restore")
                output.write(block)
        hardlinks[identity] = destination
    for name in reversed(ordered):
        path, item = target / name, entries[name]
        path.chmod(item["mode"])
        os.utime(path, ns=(item["mtime_ns"], item["mtime_ns"]))
    restored = legacy._scan(target, deadline)
    if legacy._content(restored) != legacy._content(entries) or legacy._hardlinks(restored) != legacy._hardlinks(entries):
        raise legacy.ArtifactStorageError("restored_source_objects_mismatch")


def write_reader(path: Path) -> dict:
    """Ship the exact stdlib reader, independent of any installed project copy."""
    root = Path(__file__).resolve().parents[1]
    files = ("scripts/test_source_storage.py", "scripts/source_object_archive.py",
             "scripts/test_artifact_storage.py", "scripts/isolation_support.py", "backend/file_usage.py")
    payloads = {name: (root / name).read_bytes() for name in files}
    payloads.update({"scripts/__init__.py": b"", "backend/__init__.py": b"", "__main__.py": b'''import argparse
import json
from pathlib import Path
from scripts.test_source_storage import restore_sources

parser = argparse.ArgumentParser(description="Restore verified test sources into a new directory; no generated fixtures.")
parser.add_argument("--manifest", type=Path, required=True)
parser.add_argument("--target", type=Path, required=True)
args = parser.parse_args()
print(json.dumps(restore_sources(args.manifest, args.target)))
'''})
    with path.open("xb") as output:
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as package:
            for name, raw in sorted(payloads.items()):
                item = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                item.compress_type = zipfile.ZIP_DEFLATED
                item.external_attr = 0o100644 << 16
                package.writestr(item, raw)
        output.flush()
        os.fsync(output.fileno())
    path.chmod(0o600)
    return {"reader": READER_NAME, "reader_sha256": legacy._sha(path), "reader_bytes": path.stat().st_size}


def verify_reader(package: Path, receipt: dict) -> None:
    if receipt.get("reader") != READER_NAME or type(receipt.get("reader_bytes")) is not int:
        raise legacy.ArtifactStorageError("source_reader_descriptor_invalid")
    path = legacy._safe_path(package / READER_NAME)
    if (not stat.S_ISREG(path.stat().st_mode) or path.stat().st_size != receipt["reader_bytes"]
            or legacy._sha(path) != receipt.get("reader_sha256")):
        raise legacy.ArtifactStorageError("source_reader_sha256_mismatch")
