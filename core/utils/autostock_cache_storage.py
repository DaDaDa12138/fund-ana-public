"""Lossless AutoStock cache storage; source dates and payload values are unchanged."""
from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
import stat
from typing import Any, Callable
from uuid import uuid4
import zlib

MAX_CACHE_BYTES = 64 * 1024 * 1024


def _paths(cache_dir: Path, cache_key: str) -> tuple[Path, Path]:
    if not cache_key or cache_key.startswith(".") or Path(cache_key).name != cache_key or "\\" in cache_key:
        raise ValueError("invalid_autostock_cache_key")
    return cache_dir / f"{cache_key}.json", cache_dir / f"{cache_key}.json.gz"


def read_cache_bytes(path: Path, *, max_bytes: int = MAX_CACHE_BYTES) -> bytes:
    if max_bytes < 1 or path.is_symlink():
        raise ValueError("invalid_autostock_cache_file")
    with path.open("rb") as stream:
        stored = stream.read(max_bytes + 1)
    if len(stored) > max_bytes:
        raise ValueError("autostock_cache_exceeds_read_budget")
    if not path.name.endswith(".json.gz"):
        return stored
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(stored), mode="rb") as stream:
            original = stream.read(max_bytes + 1)
    except (OSError, EOFError, zlib.error) as exc:
        raise ValueError("autostock_cache_gzip_invalid") from exc
    if len(original) > max_bytes:
        raise ValueError("autostock_cache_exceeds_read_budget")
    return original


def read_cache(cache_dir: Path, cache_key: str) -> dict[str, Any]:
    legacy, compressed = _paths(Path(cache_dir), cache_key)
    if compressed.exists() or compressed.is_symlink():
        original = read_cache_bytes(compressed)
        # A broken or conflicting new cache must never fall back to an older
        # passing result. Identical files can coexist during a verified migration.
        if (legacy.exists() or legacy.is_symlink()) and read_cache_bytes(legacy) != original:
            raise ValueError("autostock_cache_format_conflict")
    else:
        original = read_cache_bytes(legacy)
    payload = json.loads(original.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("autostock_cache_object_required")
    return payload


def cache_keys(cache_dir: Path) -> list[str]:
    keys = set()
    for path in Path(cache_dir).glob("*.json*"):
        if path.name.startswith("."):
            continue
        if path.name.endswith(".json.gz"):
            keys.add(path.name[:-8])
        elif path.name.endswith(".json"):
            keys.add(path.name[:-5])
    return sorted(keys)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_cache(
    cache_dir: Path, cache_key: str, payload: dict[str, Any], *,
    json_writer: Callable[[Path, dict[str, Any]], Any],
) -> Path:
    """Preserve the existing serializer and publish only a fully verified gzip."""
    legacy, target = _paths(Path(cache_dir), cache_key)
    if legacy.is_symlink() or target.is_symlink():
        raise ValueError("invalid_autostock_cache_file")
    target.parent.mkdir(parents=True, exist_ok=True)
    token = uuid4().hex
    raw_tmp = target.with_name(f".{cache_key}.{token}.raw.tmp")
    gzip_tmp = target.with_name(f".{cache_key}.{token}.json.gz")
    try:
        json_writer(raw_tmp, payload)
        original = read_cache_bytes(raw_tmp)
        if not isinstance(json.loads(original.decode("utf-8")), dict):
            raise ValueError("autostock_cache_object_required")
        stored = gzip.compress(original, compresslevel=6, mtime=0)
        with gzip_tmp.open("xb") as stream:
            stream.write(stored)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(gzip_tmp, stat.S_IMODE(raw_tmp.stat().st_mode))
        if read_cache_bytes(gzip_tmp) != original:
            raise ValueError("autostock_cache_written_bytes_mismatch")
        os.replace(gzip_tmp, target)
        _sync_directory(target.parent)
        # The old writer replaced this same logical key. Retire its legacy
        # representation only after the replacement is durable and verified.
        legacy.unlink(missing_ok=True)
        _sync_directory(target.parent)
        return target
    finally:
        raw_tmp.unlink(missing_ok=True)
        gzip_tmp.unlink(missing_ok=True)
