"""Crash-safe AutoStock ledger for processes sharing one local volume."""
from __future__ import annotations
from contextlib import contextmanager
import fcntl
import json
import math
import os
from pathlib import Path
from typing import Any, Iterator, BinaryIO
from uuid import uuid4


class QuotaStateError(ValueError):
    pass


def parse_state(raw: str) -> dict[str, Any]:
    if not raw.strip():
        raise QuotaStateError("AutoStock quota ledger is empty; remaining budget is unknown")
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict) or not isinstance(payload.get("recent_call_timestamps"), list):
            raise ValueError("invalid structure")
        if payload.get("schema_version") not in (None, 1, 2):
            raise ValueError("unsupported schema version")
        timestamps = [float(value) for value in payload["recent_call_timestamps"]]
        last = float(payload.get("last_call_time") or 0.0)
        blocked = float(payload.get("blocked_until") or 0.0)
        if not all(math.isfinite(value) and value >= 0 for value in [last, blocked, *timestamps]):
            raise ValueError("invalid timestamp")
        return {"schema_version": 2, "last_call_time": last,
                "recent_call_timestamps": sorted(timestamps), "blocked_until": blocked}
    except (ValueError, TypeError, KeyError) as exc:
        raise QuotaStateError(f"AutoStock quota ledger is invalid: {exc}") from exc


@contextmanager
def quota_state_lock(path: Path, *, exclusive: bool = True) -> Iterator[BinaryIO]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    # The inode never changes, including when the JSON data file is replaced.
    with lock_path.open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield stream
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def read_locked_state(path: Path, lock_stream: BinaryIO) -> dict[str, Any]:
    if path.exists():
        return parse_state(path.read_text(encoding="utf-8"))
    # Persisted marker distinguishes a first boot from a disappeared ledger.
    # No HTTP attempt starts until both JSON and marker have been fsynced.
    if os.fstat(lock_stream.fileno()).st_size:
        raise QuotaStateError("AutoStock initialized quota ledger is missing")
    return {"schema_version": 2, "last_call_time": 0.0, "recent_call_timestamps": [], "blocked_until": 0.0}


def write_locked_state(path: Path, state: dict[str, Any], lock_stream: BinaryIO) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump({**state, "schema_version": 2}, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if not os.fstat(lock_stream.fileno()).st_size:
            lock_stream.write(b"initialized_v2\n")
            lock_stream.flush()
            os.fsync(lock_stream.fileno())
    finally:
        temporary.unlink(missing_ok=True)


def read_quota_state(path: Path) -> dict[str, Any]:
    with quota_state_lock(path, exclusive=False) as stream:
        return read_locked_state(path, stream)
