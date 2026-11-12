"""Immutable compressed input packs with self-contained, local capture links.

The SQLite catalogue is a disposable lookup cache. Replay reads only the files
named and hashed by its capture manifest, never the catalogue or live database.
Global publication, local linking and garbage collection share one lock.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from typing import Any
from uuid import uuid4

from .calculation_capture import (
    CalculationCaptureError, MAX_LINE_BYTES, MIN_FREE_BYTES,
    _hash, _safe, _sync_directory, capture_storage_write,
)
from .calculation_chunking import (
    CHUNK_FORMAT, CHUNK_THRESHOLD_BYTES, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES,
    iter_content_chunks,
)

ENV_SHARED_DIRECTORY = "FUND_ANA_CALCULATION_SHARED_DIR"
FORMAT = "zstd_pack_v1"
MAGIC = b"FUND-ANA-CALCULATION-PACK\x00\x01\n"
TARGET_PACK_BYTES = 16 * 1024**2
# A small cached object must not pull an unrelated large pack into each run.
# Compact sparse reuse locally; all directory and free-space limits still apply.
MIN_SPARSE_REUSE_PACK_BYTES = 1024**2
MAX_REUSE_AMPLIFICATION = 8
MAX_PACK_BYTES = MAX_LINE_BYTES + 1024**2
MAX_PACK_FILES = 512
MAX_POOL_BYTES = 8 * 1024**3
MAX_CATALOG_BYTES = 512 * 1024**2
MAX_CACHE_EVICTIONS = 32
MAX_CACHE_MAINTENANCE_SECONDS = 2.0
CATALOG = "catalog.sqlite"
POOL_LOCK = ".pool.lock"
_PACK_NAME = re.compile(r"[0-9a-f]{32}\.pack")
_DIGEST = re.compile(r"[0-9a-f]{64}")


def is_pack_name(name: str) -> bool:
    return isinstance(name, str) and _PACK_NAME.fullmatch(name) is not None


def _identity(info: os.stat_result) -> tuple[int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


@contextmanager
def _pool_lock(root: Path):
    path = _safe(root / POOL_LOCK)
    with path.open("a+b") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode) or path.stat().st_size:
            raise CalculationCaptureError("invalid_capture_pool_lock")
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _catalog(root: Path) -> sqlite3.Connection:
    path = _safe(root / CATALOG)
    for suffix in ("-journal", "-wal", "-shm"):
        _safe(path.with_name(path.name + suffix))
    if path.exists() and (not path.is_file() or path.stat().st_size > MAX_CATALOG_BYTES):
        raise CalculationCaptureError("invalid_or_oversized_capture_catalog")
    # CaptureWriter serializes its worker threads; the pool lock also orders
    # every catalogue operation across processes.
    connection = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            raise CalculationCaptureError("unsupported_capture_catalog_version")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA cache_size=-4096")
        if version == 0:
            fs = os.statvfs(root)
            if fs.f_bavail * fs.f_frsize < MIN_FREE_BYTES + 64 * 1024:
                raise CalculationCaptureError("capture_disk_budget_exhausted")
            if connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchone():
                raise CalculationCaptureError("unrecognized_capture_catalog")
            connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("CREATE TABLE packs(name TEXT PRIMARY KEY, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL) WITHOUT ROWID")
            connection.execute("CREATE TABLE objects(kind TEXT NOT NULL, digest BLOB NOT NULL, pack TEXT NOT NULL, offset INTEGER NOT NULL, compressed_bytes INTEGER NOT NULL, raw_bytes INTEGER NOT NULL, compressed_sha256 BLOB NOT NULL, PRIMARY KEY(kind,digest)) WITHOUT ROWID")
            connection.execute("CREATE INDEX objects_by_pack ON objects(pack)")
            connection.execute("PRAGMA user_version=1")
            connection.commit()
            _sync_directory(root)
        return connection
    except BaseException:
        connection.close()
        raise


def _pool_size(root: Path) -> int:
    total = 0
    for path in root.iterdir():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise CalculationCaptureError("invalid_capture_pool_member")
        total += info.st_size
    return total


def _catalog_used_bytes(connection: sqlite3.Connection) -> int:
    """Free SQLite pages can be reused without growing the bounded catalogue."""
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    pages = connection.execute("PRAGMA page_count").fetchone()[0]
    free = connection.execute("PRAGMA freelist_count").fetchone()[0]
    return (pages - free) * page_size


def _make_cache_room(connection: sqlite3.Connection, pool: Path, size: int, reserve: int) -> dict:
    """Caller holds the pool lock. Evict lookup links, never capture-local files.

    A saturated lookup catalogue must not freeze its oldest contents forever.
    Only packs with a surviving local hardlink may lose their global alias.
    A crash after unlink is handled by the existing stale-cache recovery path.
    The complete new local pack remains usable if bounded eviction is insufficient.
    """
    total = _pool_size(pool)
    result = {"cache_lookup_links_evicted": 0, "cache_object_mappings_evicted": 0}

    def fits() -> bool:
        return (total + size + reserve <= MAX_POOL_BYTES
                and _catalog_used_bytes(connection) + reserve <= MAX_CATALOG_BYTES)

    # Do not evict anything when disk headroom cannot support the new index,
    # or when even an empty lookup cache cannot accommodate the requested pack.
    fs = os.statvfs(pool)
    if (fs.f_bavail * fs.f_frsize < MIN_FREE_BYTES + reserve
            or size + reserve >= MAX_POOL_BYTES or reserve >= MAX_CATALOG_BYTES):
        return {**result, "can_cache": False}
    if fits():
        return {**result, "can_cache": True}
    started = time.monotonic()
    candidates = []
    for path in pool.iterdir():
        if not is_pack_name(path.name):
            continue
        info = path.lstat()
        if stat.S_ISREG(info.st_mode) and info.st_nlink > 1:
            candidates.append((info.st_ctime_ns, path, info))
    # Linking a reused pack updates ctime; prefer aliases not recently reused.
    for _, path, before in sorted(candidates, key=lambda row: (row[0], row[1].name)):
        if (result["cache_lookup_links_evicted"] >= MAX_CACHE_EVICTIONS
                or time.monotonic() - started >= MAX_CACHE_MAINTENANCE_SECONDS):
            break
        current = path.lstat()
        if _identity(current) != _identity(before) or current.st_nlink <= 1:
            raise CalculationCaptureError("capture_cache_eviction_identity_changed")
        connection.execute("BEGIN IMMEDIATE")
        try:
            path.unlink()
            count = connection.execute("DELETE FROM objects WHERE pack=?", (path.name,)).rowcount
            connection.execute("DELETE FROM packs WHERE name=?", (path.name,))
            connection.commit()
        except BaseException:
            connection.rollback()
            _sync_directory(pool)
            raise
        total -= before.st_size
        result["cache_lookup_links_evicted"] += 1
        result["cache_object_mappings_evicted"] += count
        if fits():
            break
    if result["cache_lookup_links_evicted"]:
        _sync_directory(pool)
    return {**result, "can_cache": fits()}


def pack_members(storage: Any) -> dict[str, dict]:
    if (not isinstance(storage, dict) or storage.get("format") != FORMAT
            or not isinstance(storage.get("packs"), list)
            or not 0 < len(storage["packs"]) <= MAX_PACK_FILES):
        raise CalculationCaptureError("invalid_capture_pack_manifest")
    result = {}
    for row in storage["packs"]:
        if (not isinstance(row, dict) or set(row) != {"file", "bytes", "sha256"}
                or not is_pack_name(row["file"]) or row["file"] in result
                or type(row["bytes"]) is not int or not len(MAGIC) < row["bytes"] <= MAX_PACK_BYTES
                or not isinstance(row["sha256"], str) or not _DIGEST.fullmatch(row["sha256"])):
            raise CalculationCaptureError("invalid_capture_pack_identity")
        result[row["file"]] = row
    return result


class PackReader:
    def __init__(self, directory: Path, storage: dict):
        self.directory = _safe(directory)
        self.members = pack_members(storage)
        self.verified: dict[str, tuple] = {}
        self.used: set[str] = set()

    def verify(self, name: str) -> Path:
        if name not in self.members:
            raise CalculationCaptureError("unbound_capture_pack")
        member = self.members[name]
        path = _safe(self.directory / name)
        info = path.stat()
        identity = _identity(info)
        if not stat.S_ISREG(info.st_mode) or info.st_size != member["bytes"]:
            raise CalculationCaptureError("capture_pack_size_mismatch")
        if name in self.verified:
            if self.verified[name] != identity:
                raise CalculationCaptureError("capture_pack_changed_during_read")
        else:
            if _hash(path) != member["sha256"] or _identity(path.stat()) != identity:
                raise CalculationCaptureError("capture_pack_hash_mismatch")
            with path.open("rb") as stream:
                if stream.read(len(MAGIC)) != MAGIC:
                    raise CalculationCaptureError("invalid_capture_pack_header")
            self.verified[name] = identity
        return path

    def get(self, reference: dict, digest: str) -> bytes:
        if isinstance(reference, dict) and reference.get("format") == CHUNK_FORMAT:
            if (set(reference) != {"format", "raw_bytes", "chunks"}
                    or type(reference.get("raw_bytes")) is not int
                    or not 0 < reference["raw_bytes"] <= MAX_LINE_BYTES
                    or not isinstance(reference.get("chunks"), list)
                    or not 0 < len(reference["chunks"]) <= MAX_LINE_BYTES // MIN_CHUNK_BYTES + 1
                    or not isinstance(digest, str) or not _DIGEST.fullmatch(digest)):
                raise CalculationCaptureError("invalid_capture_chunk_manifest")
            total = 0
            for number, chunk in enumerate(reference["chunks"]):
                if (not isinstance(chunk, dict) or set(chunk) != {"sha256", "packed"}
                        or not isinstance(chunk["packed"], dict)
                        or type(chunk["packed"].get("raw_bytes")) is not int
                        or not 0 < chunk["packed"]["raw_bytes"] <= MAX_CHUNK_BYTES
                        or (number < len(reference["chunks"]) - 1
                            and chunk["packed"]["raw_bytes"] < MIN_CHUNK_BYTES)):
                    raise CalculationCaptureError("invalid_capture_chunk_reference")
                total += chunk["packed"]["raw_bytes"]
            if total != reference["raw_bytes"]:
                raise CalculationCaptureError("capture_chunk_total_mismatch")
            raw = b"".join(self._get_one(chunk["packed"], chunk["sha256"])
                           for chunk in reference["chunks"])
            if len(raw) != total or hashlib.sha256(raw).hexdigest() != digest:
                raise CalculationCaptureError("capture_chunked_object_hash_mismatch")
            return raw
        return self._get_one(reference, digest)

    def _get_one(self, reference: dict, digest: str) -> bytes:
        if (not isinstance(reference, dict)
                or set(reference) != {"file", "offset", "compressed_bytes", "raw_bytes", "compressed_sha256"}
                or not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
                or not isinstance(reference.get("compressed_sha256"), str)
                or not _DIGEST.fullmatch(reference["compressed_sha256"])
                or any(type(reference.get(key)) is not int for key in ("offset", "compressed_bytes", "raw_bytes"))
                or not len(MAGIC) <= reference["offset"] < MAX_PACK_BYTES
                or not 0 < reference["compressed_bytes"] <= MAX_PACK_BYTES
                or not 0 <= reference["raw_bytes"] <= MAX_LINE_BYTES):
            raise CalculationCaptureError("invalid_capture_pack_reference")
        name = reference["file"]
        if not is_pack_name(name):
            raise CalculationCaptureError("invalid_capture_pack_reference")
        path = self.verify(name)
        if reference["offset"] + reference["compressed_bytes"] > self.members[name]["bytes"]:
            raise CalculationCaptureError("capture_pack_reference_out_of_bounds")
        with path.open("rb") as stream:
            if _identity(os.fstat(stream.fileno())) != self.verified[name]:
                raise CalculationCaptureError("capture_pack_changed_during_read")
            stream.seek(reference["offset"])
            compressed = stream.read(reference["compressed_bytes"])
        if (len(compressed) != reference["compressed_bytes"]
                or hashlib.sha256(compressed).hexdigest() != reference["compressed_sha256"]):
            raise CalculationCaptureError("capture_packed_object_hash_mismatch")
        import pyarrow as pa

        try:
            raw = pa.decompress(compressed, decompressed_size=reference["raw_bytes"], codec="zstd", asbytes=True)
        except (ValueError, OSError) as exc:
            raise CalculationCaptureError("capture_packed_object_decompression_failed") from exc
        if len(raw) != reference["raw_bytes"] or hashlib.sha256(raw).hexdigest() != digest:
            raise CalculationCaptureError("capture_packed_object_raw_hash_mismatch")
        self.used.add(name)
        return raw


class CapturePackStore:
    """Append each new object once; seal packs before advertising cache entries."""

    def __init__(self, directory: Path, pool: Path):
        self.directory, self.pool = _safe(directory), _safe(pool)
        if self.directory.is_relative_to(self.pool) or self.pool.is_relative_to(self.directory):
            raise CalculationCaptureError("capture_pool_overlaps_capture")
        self.pool.mkdir(parents=True, exist_ok=True)
        if self.pool.stat().st_dev != self.directory.stat().st_dev:
            raise CalculationCaptureError("capture_pool_requires_same_filesystem")
        with _pool_lock(self.pool):
            self.connection = _catalog(self.pool)
        self.members: dict[str, dict] = {}
        self.known: dict[tuple[str, str], dict] = {}
        self.pending: list[tuple] = []
        self.stream = None
        self.partial = None
        self.current_name = None
        self.stats: Counter = Counter()
        self.pool_verified: dict[str, tuple] = {}

    def _remember(self, member: dict) -> None:
        name = member["file"]
        if name in self.members and self.members[name] != member:
            raise CalculationCaptureError("capture_pack_manifest_conflict")
        if name not in self.members and len(self.members) >= MAX_PACK_FILES:
            raise CalculationCaptureError("capture_pack_count_budget_exhausted")
        self.members[name] = member

    def _link_existing(self, member: dict) -> None:
        name = member["file"]
        source = _safe(self.pool / name)
        reader = PackReader(self.pool, {"format": FORMAT, "packs": [member]})
        if name in self.pool_verified:
            reader.verified[name] = self.pool_verified[name]
        reader.verify(name)
        self.pool_verified[name] = reader.verified[name]
        target = _safe(self.directory / name)
        # Several journals share the same capture directory. A preceding
        # session may already have linked this pack into that directory.
        with capture_storage_write(self.directory, 0 if target.exists() else member["bytes"]):
            if not target.exists():
                os.link(source, target)
            elif not target.samefile(source):
                raise CalculationCaptureError("capture_local_pack_link_changed")
        _sync_directory(self.directory)
        self._remember(member)

    def put(self, kind: str, raw: bytes, digest: str) -> dict:
        if (self.connection is None or kind not in {"source", "object"}
                or not isinstance(raw, bytes) or len(raw) > MAX_LINE_BYTES
                or hashlib.sha256(raw).hexdigest() != digest):
            raise CalculationCaptureError("invalid_capture_pack_object")
        if len(raw) < CHUNK_THRESHOLD_BYTES:
            return self._put_one(kind, raw, digest)
        chunks = []
        for chunk in iter_content_chunks(raw):
            chunk_sha = hashlib.sha256(chunk).hexdigest()
            chunks.append({"sha256": chunk_sha, "packed": self._put_one("chunk", chunk, chunk_sha)})
        self.stats["chunked_objects"] += 1
        self.stats["chunk_references"] += len(chunks)
        return {"format": CHUNK_FORMAT, "raw_bytes": len(raw), "chunks": chunks}

    def _put_one(self, kind: str, raw: bytes, digest: str) -> dict:
        if (self.connection is None or kind not in {"source", "object", "chunk"}
                or len(raw) > MAX_LINE_BYTES or hashlib.sha256(raw).hexdigest() != digest):
            raise CalculationCaptureError("invalid_capture_pack_object")
        key = kind, digest
        if key in self.known:
            self.stats["within_session_reuses"] += 1
            return self.known[key]
        with _pool_lock(self.pool):
            row = self.connection.execute(
                "SELECT o.pack,o.offset,o.compressed_bytes,o.raw_bytes,o.compressed_sha256,p.bytes,p.sha256 "
                "FROM objects o JOIN packs p ON p.name=o.pack WHERE o.kind=? AND o.digest=?",
                (kind, bytes.fromhex(digest)),
            ).fetchone()
            if row is not None:
                name, offset, compressed_bytes, raw_bytes, compressed_sha, size, pack_sha = row
                if raw_bytes != len(raw):
                    raise CalculationCaptureError("capture_catalog_object_conflict")
                member = {"file": name, "bytes": size, "sha256": pack_sha}
                pack_members({"format": FORMAT, "packs": [member]})
                if _safe(self.pool / name).exists():
                    reference = {"file": name, "offset": offset, "compressed_bytes": compressed_bytes,
                                 "raw_bytes": raw_bytes, "compressed_sha256": compressed_sha.hex()}
                    compact = (not (self.directory / name).exists()
                               and size > MIN_SPARSE_REUSE_PACK_BYTES
                               and size > compressed_bytes * MAX_REUSE_AMPLIFICATION)
                    if not compact:
                        self._link_existing(member)
                    # The catalogue is not trusted as business data. Reuse must
                    # decode to the exact input bytes supplied by this call.
                    reader = PackReader(self.pool if compact else self.directory,
                                        {"format": FORMAT, "packs": [member]})
                    if name in self.pool_verified:
                        reader.verified[name] = self.pool_verified[name]
                    if reader.get(reference, digest) != raw:
                        raise CalculationCaptureError("capture_catalog_input_mismatch")
                    self.pool_verified[name] = reader.verified[name]
                    if not compact:
                        self.known[key] = reference
                        self.stats["shared_objects_reused"] += 1
                        self.stats["shared_raw_bytes_reused"] += len(raw)
                        return reference
                    # Re-encode the verified actual operand into a bounded
                    # local pack below, preserving self-contained replay.
                    self.stats["sparse_shared_objects_compacted"] += 1
                    self.stats["sparse_shared_compressed_bytes"] += compressed_bytes
                else:
                    # A crash after unlink but before a catalogue transaction
                    # may leave a stale entry. It cannot supply replay input.
                    self.connection.execute("BEGIN IMMEDIATE")
                    try:
                        self.connection.execute("DELETE FROM objects WHERE pack=?", (name,))
                        self.connection.execute("DELETE FROM packs WHERE name=?", (name,))
                        self.connection.commit()
                    except BaseException:
                        self.connection.rollback()
                        raise
                    self.stats["missing_cached_packs"] += 1
        import pyarrow as pa

        compressed = pa.Codec("zstd", compression_level=6).compress(raw).to_pybytes()
        if len(compressed) + len(MAGIC) > MAX_PACK_BYTES:
            raise CalculationCaptureError("capture_packed_object_too_large")
        if self.stream is not None and self.stream.tell() + len(compressed) > TARGET_PACK_BYTES:
            self._seal()
        if self.stream is None:
            self.current_name = uuid4().hex + ".pack"
            self.partial = _safe(self.directory / (self.current_name + ".partial"))
            with capture_storage_write(self.directory, len(MAGIC)):
                self.stream = self.partial.open("xb", buffering=0)
                os.chmod(self.partial, 0o600)
                self.stream.write(MAGIC)
        offset = self.stream.tell()
        with capture_storage_write(self.directory, len(compressed)):
            if self.stream.write(compressed) != len(compressed):
                raise CalculationCaptureError("capture_pack_short_write")
        reference = {"file": self.current_name, "offset": offset, "compressed_bytes": len(compressed),
                     "raw_bytes": len(raw), "compressed_sha256": hashlib.sha256(compressed).hexdigest()}
        self.pending.append((kind, bytes.fromhex(digest), self.current_name, offset,
                             len(compressed), len(raw), bytes.fromhex(reference["compressed_sha256"])))
        self.known[key] = reference
        self.stats["new_objects"] += 1
        self.stats["new_compressed_object_bytes"] += len(compressed)
        return reference

    def _seal(self) -> None:
        if self.stream is None:
            return
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.stream.close()
        self.stream = None
        member = {"file": self.current_name, "bytes": self.partial.stat().st_size, "sha256": _hash(self.partial)}
        pack_members({"format": FORMAT, "packs": [member]})
        self._remember(member)
        target = _safe(self.directory / self.current_name)
        with capture_storage_write(self.directory, 0):
            os.link(self.partial, target)
            self.partial.unlink()
        self.partial = None
        _sync_directory(self.directory)
        with _pool_lock(self.pool):
            reserve = max(1024**2, len(self.pending) * 768)
            room = _make_cache_room(self.connection, self.pool, member["bytes"], reserve)
            self.stats.update({key: room[key] for key in (
                "cache_lookup_links_evicted", "cache_object_mappings_evicted")})
            if room["can_cache"]:
                pooled = _safe(self.pool / self.current_name)
                os.link(target, pooled)
                _sync_directory(self.pool)
                self.connection.execute("BEGIN IMMEDIATE")
                try:
                    self.connection.execute("INSERT INTO packs VALUES (?,?,?)",
                                            (self.current_name, member["bytes"], member["sha256"]))
                    # Prefer a smaller verified pack after sparse compaction.
                    # Keeping the first large pack forever re-encodes the same
                    # small operands in every later capture.
                    self.connection.executemany(
                        "INSERT INTO objects VALUES (?,?,?,?,?,?,?) "
                        "ON CONFLICT(kind,digest) DO UPDATE SET "
                        "pack=excluded.pack, offset=excluded.offset, "
                        "compressed_bytes=excluded.compressed_bytes, raw_bytes=excluded.raw_bytes, "
                        "compressed_sha256=excluded.compressed_sha256 "
                        "WHERE (SELECT bytes FROM packs WHERE name=objects.pack) > "
                        "(SELECT bytes FROM packs WHERE name=excluded.pack)", self.pending)
                    self.connection.commit()
                except BaseException:
                    self.connection.rollback()
                    raise
                self.stats["packs_added_to_shared_pool"] += 1
            else:
                # The complete local pack still captures every byte. A full
                # lookup cache must not discard input or loosen capture gates.
                self.stats["local_packs_without_cache_budget"] += 1
        self.pending = []
        self.current_name = None

    def finish(self) -> dict:
        self._seal()
        self.connection.close()
        self.connection = None
        return {"format": FORMAT, "packs": [self.members[key] for key in sorted(self.members)],
                "stats": dict(self.stats), "replay_dependency": "capture_local_files_only"}

    def close(self) -> None:
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def collect_unused_packs(pool: Path, *, apply: bool = False, max_packs: int = 32,
                         min_age_seconds: int = 86400, now: float | None = None) -> dict:
    """Retire only global packs with no remaining local hardlink references."""
    pool = _safe(pool)
    result = {"dry_run": not apply, "planned": [], "removed": [], "retained": 0,
              "removed_allocated_bytes_estimate": 0, "partial_removal_possible": False, "errors": [],
              "scope": "Unreferenced shared-pool packs only. Capture directories are not deleted. Allocation is a gross st_blocks estimate; open readers and concurrent writes can change physical reclamation."}
    if not pool.exists():
        return result
    if type(max_packs) is not int or not 0 <= max_packs <= 128 or min_age_seconds < 0:
        raise CalculationCaptureError("invalid_capture_pool_gc_budget")
    clock = time.time() if now is None else now
    with _pool_lock(pool):
        candidates = []
        for path in pool.iterdir():
            if not is_pack_name(path.name):
                continue
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_mtime > clock - min_age_seconds):
                result["retained"] += 1
                continue
            candidates.append((info.st_mtime_ns, path, info))
        selected = sorted(candidates)[:max_packs]
        result["retained"] += len(candidates) - len(selected)
        result["planned"] = [{"file": path.name, "bytes": info.st_size,
                              "allocated_bytes": info.st_blocks * 512} for _, path, info in selected]
        if not apply or not selected:
            return result
        connection = _catalog(pool)
        try:
            for (_, path, info), item in zip(selected, result["planned"]):
                try:
                    current = path.lstat()
                    if not stat.S_ISREG(current.st_mode) or current.st_nlink != 1 or _identity(current) != _identity(info):
                        raise CalculationCaptureError("capture_pool_gc_identity_changed")
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute("DELETE FROM objects WHERE pack=?", (path.name,))
                    connection.execute("DELETE FROM packs WHERE name=?", (path.name,))
                    path.unlink()
                    connection.commit()
                    result["removed"].append(item)
                    result["removed_allocated_bytes_estimate"] += info.st_blocks * 512
                except (OSError, sqlite3.Error, CalculationCaptureError) as error:
                    connection.rollback()
                    if not path.exists():
                        result["partial_removal_possible"] = True
                        result["removed_allocated_bytes_estimate"] = None
                    result["errors"].append({"file": path.name, "error": f"{type(error).__name__}:{error}"})
                    break
            _sync_directory(pool)
        finally:
            connection.close()
    return result
