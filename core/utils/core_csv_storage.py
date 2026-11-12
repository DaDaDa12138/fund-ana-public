"""Read losslessly archived core CSV through its original logical filename.

Storage metadata describes original bytes and filesystem attributes only. It is
not evidence that the source values, lineage or screening strategy were valid.
Normal files retain precedence, including while an archive is being committed.
Importing this module performs no filesystem I/O and does not load Arrow.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from io import BytesIO, TextIOWrapper
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
from types import SimpleNamespace
from typing import Any, Iterator
from uuid import uuid4

FORMAT = "fund_ana_core_csv_zstd_v1"
MAX_CSV_BYTES = 64 * 1024**2
MAX_STORED_BYTES = MAX_CSV_BYTES + 1024**2
MAX_RECEIPT_BYTES = 64 * 1024
RESTORE_RESERVE_BYTES = 64 * 1024**2
STORED_SUFFIX = ".zst"
RECEIPT_SUFFIX = ".zst.receipt.json"
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class CoreCsvStorageError(ValueError):
    pass


def csv_storage_paths(path: str | Path) -> tuple[Path, Path]:
    path = Path(path)
    if path.suffix != ".csv":
        raise CoreCsvStorageError("core_csv_original_suffix_required")
    return path.with_name(path.name + STORED_SUFFIX), path.with_name(path.name + RECEIPT_SUFFIX)


def file_identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)


def _read_regular(path: Path, maximum: int) -> tuple[bytes, os.stat_result]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= maximum:
            raise CoreCsvStorageError("core_csv_storage_type_or_size_invalid")
        raw = stream.read(maximum + 1)
        if len(raw) != before.st_size or file_identity(os.fstat(stream.fileno())) != file_identity(before):
            raise CoreCsvStorageError("core_csv_storage_changed_during_read")
    if file_identity(path.lstat()) != file_identity(before):
        raise CoreCsvStorageError("core_csv_storage_replaced_during_read")
    return raw, before


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CoreCsvStorageError("core_csv_duplicate_receipt_key")
        result[key] = value
    return result


def has_stored_csv(path: str | Path) -> bool:
    """Include incomplete pairs so corruption cannot silently disappear in glob."""
    path = Path(path)
    return path.suffix == ".csv" and any(os.path.lexists(item) for item in csv_storage_paths(path))


def _stored_csv_metadata(path: str | Path) -> tuple[dict[str, Any], tuple, tuple]:
    path = Path(path)
    stored, receipt = csv_storage_paths(path)
    try:
        encoded, receipt_info = _read_regular(receipt, MAX_RECEIPT_BYTES)
        metadata = json.loads(encoded, object_pairs_hook=_unique_pairs)
        stored_info = stored.lstat()
    except FileNotFoundError as error:
        if has_stored_csv(path):
            raise CoreCsvStorageError("core_csv_storage_pair_incomplete") from error
        raise
    if not isinstance(metadata, dict) or metadata.get("format") != FORMAT:
        raise CoreCsvStorageError("core_csv_receipt_format_invalid")
    if type(metadata.get("schema_version")) is not int or metadata["schema_version"] != 1:
        raise CoreCsvStorageError("core_csv_receipt_version_invalid")
    if metadata.get("logical_name") != path.name or metadata.get("codec") != "zstd":
        raise CoreCsvStorageError("core_csv_receipt_path_or_codec_mismatch")
    original = metadata.get("original_identity")
    if not isinstance(original, dict):
        raise CoreCsvStorageError("core_csv_original_identity_missing")
    for key in ("size", "mode", "uid", "gid", "mtime_ns", "atime_ns"):
        if type(original.get(key)) is not int or original[key] < 0:
            raise CoreCsvStorageError("core_csv_original_identity_invalid")
    if original["size"] > MAX_CSV_BYTES or not stat.S_ISREG(original["mode"]):
        raise CoreCsvStorageError("core_csv_original_type_or_size_invalid")
    mtime = original.get("mtime")
    if (type(mtime) not in (int, float) or not math.isfinite(mtime)
            or abs(mtime - original["mtime_ns"] / 1_000_000_000) > 0.000001):
        raise CoreCsvStorageError("core_csv_original_time_invalid")
    size = metadata.get("stored_bytes")
    if type(size) is not int or not 0 < size <= MAX_STORED_BYTES:
        raise CoreCsvStorageError("core_csv_stored_size_invalid")
    if not stat.S_ISREG(stored_info.st_mode) or stored_info.st_size != size:
        raise CoreCsvStorageError("core_csv_stored_type_or_length_mismatch")
    for key in ("original_sha256", "stored_sha256"):
        if not isinstance(metadata.get(key), str) or not _SHA.fullmatch(metadata[key]):
            raise CoreCsvStorageError("core_csv_receipt_sha_invalid")
    return metadata, file_identity(receipt_info), file_identity(stored_info)


def stored_csv_metadata(path: str | Path) -> tuple[dict[str, Any], tuple, tuple]:
    try:
        return _stored_csv_metadata(path)
    except CoreCsvStorageError:
        raise
    except (OSError, ValueError, UnicodeError) as error:
        raise CoreCsvStorageError("core_csv_storage_metadata_unreadable") from error


def _read_stored_csv(path: str | Path) -> tuple[bytes, dict[str, Any]]:
    """Fully verify a stored frame, including when the original still exists."""
    path = Path(path)
    metadata, receipt_identity, stored_identity = stored_csv_metadata(path)
    stored, receipt = csv_storage_paths(path)
    compressed, info = _read_regular(stored, MAX_STORED_BYTES)
    if file_identity(info) != stored_identity or hashlib.sha256(compressed).hexdigest() != metadata["stored_sha256"]:
        raise CoreCsvStorageError("core_csv_stored_sha_mismatch")
    import pyarrow as pa

    try:
        raw = pa.decompress(compressed, decompressed_size=metadata["original_identity"]["size"],
                            codec="zstd", asbytes=True)
    except (pa.ArrowException, ValueError) as error:
        raise CoreCsvStorageError("core_csv_decompression_failed") from error
    if (len(raw) != metadata["original_identity"]["size"]
            or hashlib.sha256(raw).hexdigest() != metadata["original_sha256"]):
        raise CoreCsvStorageError("core_csv_original_sha_or_length_mismatch")
    if (file_identity(receipt.lstat()) != receipt_identity
            or file_identity(stored.lstat()) != stored_identity):
        raise CoreCsvStorageError("core_csv_storage_changed_during_verification")
    return raw, metadata


def read_stored_csv(path: str | Path) -> tuple[bytes, dict[str, Any]]:
    try:
        return _read_stored_csv(path)
    except CoreCsvStorageError:
        raise
    except (OSError, ValueError) as error:
        raise CoreCsvStorageError("core_csv_storage_unreadable") from error


def artifact_is_file(path: str | Path) -> bool:
    path = Path(path)
    if path.exists() or path.is_symlink():
        return path.is_file()
    if has_stored_csv(path):
        stored_csv_metadata(path)
        return True
    return False


def original_artifact_stat(path: str | Path):
    """Return original display/order attributes, never a fabricated live inode."""
    path = Path(path)
    try:
        return path.stat()
    except FileNotFoundError:
        if path.is_symlink() or not has_stored_csv(path):
            raise
    metadata, _, _ = stored_csv_metadata(path)
    original = metadata["original_identity"]
    return SimpleNamespace(st_size=original["size"], st_mtime=original["mtime"],
                           st_mtime_ns=original["mtime_ns"], st_mode=original["mode"],
                           st_uid=original["uid"], st_gid=original["gid"])


def stored_csv_cache_token(path: str | Path) -> str:
    metadata, receipt_identity, stored_identity = stored_csv_metadata(path)
    payload = [metadata["original_sha256"], metadata["stored_sha256"], receipt_identity, stored_identity]
    return "core_csv:" + hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def glob_artifacts(directory: str | Path, pattern: str, *, recursive: bool = False) -> list[Path]:
    """Find logical names once, including archived CSV and incomplete pairs."""
    directory = Path(directory)
    glob = directory.rglob if recursive else directory.glob
    result: dict[Path, None] = {}
    for candidate in glob(pattern):
        if candidate.name.endswith((".csv" + STORED_SUFFIX, ".csv" + RECEIPT_SUFFIX)):
            suffix = RECEIPT_SUFFIX if candidate.name.endswith(RECEIPT_SUFFIX) else STORED_SUFFIX
            logical = candidate.with_name(candidate.name.removesuffix(suffix))
            if logical.match(pattern):
                result[logical] = None
        else:
            result[candidate] = None
    for suffix in (STORED_SUFFIX, RECEIPT_SUFFIX):
        for candidate in glob(pattern + suffix):
            if candidate.name.endswith(".csv" + suffix):
                result[candidate.with_name(candidate.name.removesuffix(suffix))] = None
    return list(result)


def read_original_artifact_bytes(path: str | Path) -> bytes:
    path = Path(path)
    try:
        return path.read_bytes()
    except FileNotFoundError:
        if path.is_symlink() or not has_stored_csv(path):
            raise
    return read_stored_csv(path)[0]


@contextmanager
def open_original_artifact(path: str | Path) -> Iterator:
    path = Path(path)
    try:
        stream = path.open("rb")
    except FileNotFoundError:
        if path.is_symlink() or not has_stored_csv(path):
            raise
        stream = BytesIO(read_stored_csv(path)[0])
    with stream:
        yield stream


def read_csv_artifact(path: str | Path, *args, **kwargs):
    """Preserve pandas' original arguments and native compression handling."""
    import pandas as pd

    path = Path(path)
    if path.exists() or path.is_symlink() or not has_stored_csv(path):
        return pd.read_csv(path, *args, **kwargs)
    raw, _ = read_stored_csv(path)
    return pd.read_csv(BytesIO(raw), *args, **kwargs)


@contextmanager
def open_csv_artifact(path: str | Path) -> Iterator:
    """CSV text reader preserving the original UTF-8/BOM/newline contract."""
    with open_original_artifact(path) as raw:
        with TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
            yield stream


def materialize_stored_csv(path: str | Path, destination: str | Path, *,
                           overwrite: bool = False, preserve_owner: bool = True) -> dict[str, Any]:
    """Create an independent ordinary CSV; retained storage is a separate step."""
    path, destination = Path(path), Path(destination)
    raw, metadata = read_stored_csv(path)
    original = metadata["original_identity"]
    if destination.is_symlink() or (os.path.lexists(destination) and not overwrite):
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(destination.parent).free < len(raw) + RESTORE_RESERVE_BYTES:
        raise CoreCsvStorageError("core_csv_restore_insufficient_space")
    temporary = destination.with_name(f".csv-restore-{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if preserve_owner:
            os.chown(temporary, original["uid"], original["gid"])
        os.chmod(temporary, stat.S_IMODE(original["mode"]))
        os.utime(temporary, ns=(original["atime_ns"], original["mtime_ns"]))
        verified, _ = _read_regular(temporary, MAX_CSV_BYTES)
        if verified != raw:
            raise CoreCsvStorageError("core_csv_materialized_bytes_mismatch")
        os.utime(temporary, ns=(original["atime_ns"], original["mtime_ns"]))
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, destination)
        else:
            os.link(temporary, destination, follow_symlinks=False)
            temporary.unlink()
        sync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": str(destination), "sha256": metadata["original_sha256"],
            "bytes": len(raw), "mtime_ns": original["mtime_ns"], "full_restore_verified": True}


def copy_original_artifact(source: str | Path, destination: str | Path) -> Path:
    source, destination = Path(source), Path(destination)
    if source.exists() or source.is_symlink() or not has_stored_csv(source):
        return Path(shutil.copy2(source, destination))
    materialize_stored_csv(source, destination, overwrite=True, preserve_owner=False)
    return destination


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
