"""Bind reusable rank aggregates to the read transaction that produced them."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable, Iterator


def sqlite_file_token(path: str | Path | None, *, header_length: int = 100,
                      include_ctime: bool = True) -> str | None:
    if not path:
        return None
    target = Path(path)
    try:
        info = target.stat()
        with target.open("rb") as stream:
            header = stream.read(header_length)
        if len(header) != header_length:
            return None
        return json.dumps([str(target.resolve()), info.st_dev, info.st_ino, info.st_size,
                           info.st_mtime_ns, info.st_ctime_ns if include_ctime else None,
                           header.hex()], separators=(",", ":"))
    except OSError:
        return None


def sqlite_snapshot_token(path: str | Path | None) -> str | None:
    main = sqlite_file_token(path)
    if main is None:
        return None
    wal_path = Path(str(path) + "-wal")
    wal_size = 0
    try:
        # Opening/closing a reader may create or remove an empty WAL without
        # changing any database value. A nonempty WAL must keep its identity.
        wal_size = wal_path.stat().st_size
        # SQLite fchmod()s a WAL when opening it; ctime alone changes on a
        # reader. Track the WAL generation and committed wal-index header too:
        # writes can reuse an allocated WAL tail without increasing file size.
        wal = sqlite_file_token(wal_path, header_length=32, include_ctime=False) if wal_size else None
        if wal_size and wal is None:
            return None
        index = None
        if wal_size:
            with Path(str(path) + "-shm").open("rb") as stream:
                header = stream.read(96)
            # SQLite documents two copies of the 48-byte WalIndexHdr. Only a
            # complete initialized pair can identify a committed generation;
            # locks/read marks lie outside this region. Recovery or a writer
            # may briefly make the pair inconsistent: bypass caching then.
            if len(header) != 96 or header[:48] != header[48:] or header[12] != 1:
                return None
            index = header[:48].hex()
    except FileNotFoundError:
        if wal_size:
            return None
        wal = None
        index = None
    except OSError:
        return None
    return json.dumps([main, wal, index], separators=(",", ":"))


def _binding(archive: dict[str, Any] | None) -> tuple:
    value = archive or {}
    return (bool(value.get("attached")), str(value.get("alias") or "rank_archive_db"),
            str(value.get("archive_path") or ""),
            str(value.get("archive_table_name") or "fund_rank_history_archive"),
            str(value.get("archive_cutoff_date") or ""))


def _connection_paths_match(conn: sqlite3.Connection, path: Path, archive: dict[str, Any] | None) -> bool:
    databases = {row[1]: row[2] for row in conn.execute("PRAGMA database_list")}
    if not databases.get("main") or Path(databases["main"]).resolve() != path.resolve():
        return False
    value = archive or {}
    if value.get("attached"):
        alias = str(value.get("alias") or "rank_archive_db")
        raw = value.get("archive_path")
        if not raw or not databases.get(alias) or Path(databases[alias]).resolve() != Path(raw).resolve():
            return False
    return True


def _data_version(conn: sqlite3.Connection, alias: str = "main") -> int:
    if not alias or not alias.replace("_", "").isascii() or not alias.replace("_", "").isalnum():
        raise ValueError("unsafe_rank_cache_archive_identifier")
    return int(conn.execute(f'PRAGMA "{alias}".data_version').fetchone()[0])


@dataclass
class RankCacheSnapshot:
    connection: sqlite3.Connection
    database_path: Path
    main_token: str | None
    total_changes: int
    verified: bool
    main_data_version: int
    archive_token: str | None = None
    archive_data_version: int | None = None
    archive_binding: tuple | None = None
    local_stats: dict[str, Any] | None = field(default=None, repr=False)

    @classmethod
    def start(cls, conn: sqlite3.Connection, path: Path, begin: Callable) -> RankCacheSnapshot:
        before = sqlite_snapshot_token(path)
        already_pinned = conn.in_transaction
        changes = conn.total_changes
        begin(conn)
        verified = (not already_pinned and before is not None
                    and before == sqlite_snapshot_token(path)
                    and _connection_paths_match(conn, path, None))
        return cls(conn, Path(path), before, changes, verified, _data_version(conn))

    def attach(self, attach: Callable, **kwargs: Any) -> dict[str, Any]:
        before = sqlite_snapshot_token(kwargs.get("archive_path"))
        result = attach(self.connection, **kwargs)
        self.archive_binding = _binding(result)
        if result.get("attached"):
            self.archive_token = before
            self.archive_data_version = _data_version(self.connection, self.archive_binding[1])
            self.verified = (self.verified and before is not None
                             and before == sqlite_snapshot_token(result.get("archive_path"))
                             and _connection_paths_match(self.connection, self.database_path, result))
        return result

    def read_view_current(self, conn: sqlite3.Connection, path: Path, archive: dict[str, Any] | None) -> bool:
        # data_version stays fixed in a pinned read, but changes when that
        # connection starts a new snapshot after another connection commits.
        # File tokens also guard reattachment/replacement and conservatively
        # bypass shared caching during concurrent writes/checkpoints.
        try:
            return (self.connection is conn and self.database_path.resolve() == Path(path).resolve()
                    and conn.in_transaction and conn.total_changes == self.total_changes
                    and self.archive_binding == _binding(archive)
                    and _data_version(conn) == self.main_data_version
                    and (not (archive or {}).get("attached") or (
                        self.archive_data_version == _data_version(conn, self.archive_binding[1]))))
        except (sqlite3.Error, OSError, ValueError):
            return False

    def usable(self, conn: sqlite3.Connection, path: Path, archive: dict[str, Any] | None) -> bool:
        return (self.read_view_current(conn, path, archive)
                and _connection_paths_match(conn, Path(path), archive)
                and self.main_token == sqlite_snapshot_token(path)
                and (not (archive or {}).get("attached")
                     or self.archive_token == sqlite_snapshot_token((archive or {}).get("archive_path"))))

    def require_read_view(self, path: Path, archive: dict[str, Any] | None) -> None:
        if not self.read_view_current(self.connection, path, archive):
            raise RuntimeError("rank_read_snapshot_lost")

    def cacheable(self, path: Path, archive: dict[str, Any] | None) -> bool:
        return self.verified and self.usable(self.connection, path, archive)


@contextmanager
def rank_cache_snapshot(
    conn: sqlite3.Connection, path: Path, archive: dict[str, Any] | None,
    supplied: RankCacheSnapshot | None, *, begin: Callable,
) -> Iterator[RankCacheSnapshot]:
    """Untracked existing transactions may compute, but never use shared caches.

    A helper called without a transaction owns a short read transaction. The
    main/cold file tokens must stay stable across pinning both snapshots. The
    normal report caller supplies tokens captured around its original pinning,
    before loading a possibly newer archive binding.
    """
    if supplied is not None and supplied.usable(conn, path, archive):
        yield supplied
        return
    owned = not conn.in_transaction
    cold_before = sqlite_snapshot_token((archive or {}).get("archive_path"))
    try:
        scope = RankCacheSnapshot.start(conn, path, begin)
        scope.archive_binding = _binding(archive)
        if (archive or {}).get("attached"):
            alias, table = scope.archive_binding[1], scope.archive_binding[3]
            if not all(name and name.replace("_", "").isalnum() for name in (alias, table)):
                raise ValueError("unsafe_rank_cache_archive_identifier")
            conn.execute(f'SELECT 1 FROM "{alias}"."{table}" LIMIT 1').fetchone()
            scope.archive_token = cold_before
            scope.archive_data_version = _data_version(conn, alias)
            scope.verified = (scope.verified and cold_before is not None
                              and cold_before == sqlite_snapshot_token((archive or {}).get("archive_path")))
        scope.verified = (scope.verified and scope.main_token == sqlite_snapshot_token(path)
                          and _connection_paths_match(conn, Path(path), archive))
        yield scope
    finally:
        if owned and conn.in_transaction:
            conn.rollback()
