"""Rank caches must describe the SQLite snapshot actually being queried."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import pytest

from backend import report_builder as report
from backend.rank_cache_snapshot import RankCacheSnapshot, sqlite_snapshot_token
from database.rank_history_queries import (
    attach_rank_history_archive_if_available, begin_rank_history_read_snapshot,
)


@pytest.fixture(autouse=True)
def isolated_rank_caches(monkeypatch: pytest.MonkeyPatch):
    report._LONG_TERM_RANK_METRICS_CACHE.clear()
    report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
    monkeypatch.setattr(report, "_long_term_history_rank_snapshot_stats_sidecar_path", lambda _path: None)
    yield
    report._LONG_TERM_RANK_METRICS_CACHE.clear()
    report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()


def _database(path: Path, table: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute(f"CREATE TABLE {table} (fcode TEXT, rank_date TEXT, period TEXT, percentile REAL)")
    connection.commit()
    return connection


def _insert(connection: sqlite3.Connection, table: str, code: str, value: float) -> None:
    connection.execute(f"INSERT INTO {table} VALUES (?, '2026-01-15', 'overall', ?)", (code, value))
    connection.commit()


def _attach(connection: sqlite3.Connection, archive: Path) -> dict:
    connection.execute("ATTACH DATABASE ? AS rank_archive_db", (str(archive),))
    return {"attached": True, "alias": "rank_archive_db", "archive_path": str(archive),
            "archive_table_name": "fund_rank_history_archive", "archive_cutoff_date": "2026-02-01"}


def _read(kind: str, connection: sqlite3.Connection, path: Path, binding: dict | None,
          snapshot: RankCacheSnapshot | None = None) -> set[str]:
    if kind == "stats":
        return report._read_long_term_history_rank_snapshot_stats(
            connection, path, binding, cache_snapshot=snapshot)["codes"]
    frame = report._read_long_term_rank_metrics(connection, path, binding, cache_snapshot=snapshot)
    return set(frame["fcode"]) if not frame.empty else set()


def _scope(connection: sqlite3.Connection, path: Path, archive: Path | None = None):
    snapshot = RankCacheSnapshot.start(connection, path, begin_rank_history_read_snapshot)
    binding = snapshot.attach(attach_rank_history_archive_if_available, archive_path=archive,
                              archive_cutoff_date="2026-02-01")
    return snapshot, binding


@pytest.mark.parametrize("kind,disk_cache", [("stats", False), ("stats", True), ("metrics", False)])
def test_old_read_snapshot_cannot_poison_newer_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, disk_cache: bool,
) -> None:
    path = tmp_path / "hot.db"
    if disk_cache:
        monkeypatch.setattr(report, "_long_term_history_rank_snapshot_stats_sidecar_path", lambda _path: tmp_path / "stats.json")
    with closing(_database(path, "fund_rank_history")) as writer, closing(sqlite3.connect(path)) as old_reader:
        _insert(writer, "fund_rank_history", "000001", .2)
        begin_rank_history_read_snapshot(old_reader)
        _insert(writer, "fund_rank_history", "000002", .8)
        assert _read(kind, old_reader, path, None) == {"000001"}
        old_reader.rollback()
        if disk_cache:
            report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
        with closing(sqlite3.connect(path)) as reader:
            begin_rank_history_read_snapshot(reader)
            assert _read(kind, reader, path, None) == {"000001", "000002"}


@pytest.mark.parametrize("kind", ["stats", "metrics"])
def test_newer_cache_cannot_replace_an_older_pinned_snapshot(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as writer, closing(sqlite3.connect(path)) as old_reader:
        _insert(writer, "fund_rank_history", "000001", .2)
        begin_rank_history_read_snapshot(old_reader)
        _insert(writer, "fund_rank_history", "000002", .8)
        with closing(sqlite3.connect(path)) as reader:
            begin_rank_history_read_snapshot(reader)
            assert _read(kind, reader, path, None) == {"000001", "000002"}
        assert _read(kind, old_reader, path, None) == {"000001"}


def test_committed_archive_wal_change_invalidates_rank_metrics(tmp_path: Path) -> None:
    path, archive = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(_database(path, "fund_rank_history")) as hot, closing(_database(archive, "fund_rank_history_archive")) as writer:
        _insert(writer, "fund_rank_history_archive", "000001", .2)
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        with closing(sqlite3.connect(path)) as reader:
            binding = _attach(reader, archive)
            assert _read("metrics", reader, path, binding) == {"000001"}
        original = (archive.stat().st_size, archive.stat().st_mtime_ns)
        _insert(writer, "fund_rank_history_archive", "000002", .8)
        assert (archive.stat().st_size, archive.stat().st_mtime_ns) == original
        assert Path(str(archive) + "-wal").stat().st_size > 0
        with closing(sqlite3.connect(path)) as reader:
            binding = _attach(reader, archive)
            assert _read("metrics", reader, path, binding) == {"000001", "000002"}


@pytest.mark.parametrize("kind", ["stats", "metrics"])
def test_older_attached_snapshot_cannot_poison_new_archive_cache(tmp_path: Path, kind: str) -> None:
    path, archive = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(_database(path, "fund_rank_history")) as hot, closing(_database(archive, "fund_rank_history_archive")) as writer:
        _insert(writer, "fund_rank_history_archive", "000001", .2)
        with closing(sqlite3.connect(path)) as old_reader:
            begin_rank_history_read_snapshot(old_reader)
            binding = _attach(old_reader, archive)
            old_reader.execute("SELECT * FROM rank_archive_db.fund_rank_history_archive").fetchall()
            _insert(writer, "fund_rank_history_archive", "000002", .8)
            assert _read(kind, old_reader, path, binding) == {"000001"}
        with closing(sqlite3.connect(path)) as reader:
            begin_rank_history_read_snapshot(reader)
            binding = _attach(reader, archive)
            assert _read(kind, reader, path, binding) == {"000001", "000002"}


@pytest.mark.parametrize("kind,disk_cache", [("stats", False), ("stats", True), ("metrics", False)])
@pytest.mark.parametrize("cold", [False, True])
@pytest.mark.parametrize("new_first", [False, True])
def test_report_bound_scopes_keep_old_and_new_members_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, disk_cache: bool,
    cold: bool, new_first: bool,
) -> None:
    path, archive = tmp_path / "hot.db", tmp_path / "cold.db"
    if disk_cache:
        monkeypatch.setattr(report, "_long_term_history_rank_snapshot_stats_sidecar_path",
                            lambda _path: tmp_path / "stats.json")
    with closing(_database(path, "fund_rank_history")) as hot, \
            closing(_database(archive, "fund_rank_history_archive")) as cold_writer, \
            closing(sqlite3.connect(path)) as old_reader:
        writer, table = (cold_writer, "fund_rank_history_archive") if cold else (hot, "fund_rank_history")
        _insert(writer, table, "000001", .2)
        old, old_binding = _scope(old_reader, path, archive if cold else None)
        assert old.cacheable(path, old_binding)
        _insert(writer, table, "000002", .8)

        def read_new():
            if disk_cache:
                report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
            with closing(sqlite3.connect(path)) as reader:
                new, binding = _scope(reader, path, archive if cold else None)
                assert new.cacheable(path, binding)
                assert _read(kind, reader, path, binding, new) == {"000001", "000002"}

        if new_first:
            read_new()
        assert _read(kind, old_reader, path, old_binding, old) == {"000001"}
        read_new()
        assert _read(kind, old_reader, path, old_binding, old) == {"000001"}


@pytest.mark.parametrize("during_pin", [False, True])
def test_commit_between_pin_and_binding_cannot_label_old_rows_as_new(tmp_path: Path, during_pin: bool) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as writer, closing(sqlite3.connect(path)) as reader:
        _insert(writer, "fund_rank_history", "000001", .2)

        def pin(conn):
            begin_rank_history_read_snapshot(conn)
            if during_pin:
                _insert(writer, "fund_rank_history", "000002", .8)

        snapshot = RankCacheSnapshot.start(reader, path, pin)
        if not during_pin:
            _insert(writer, "fund_rank_history", "000002", .8)
        binding = snapshot.attach(attach_rank_history_archive_if_available, archive_path=None)
        assert not snapshot.cacheable(path, binding)
        assert _read("stats", reader, path, binding, snapshot) == {"000001"}
        assert not report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE
        with closing(sqlite3.connect(path)) as new_reader:
            assert _read("stats", new_reader, path, None) == {"000001", "000002"}


@pytest.mark.parametrize("untracked", [False, True])
def test_restarted_transaction_cannot_reuse_old_local_stats(tmp_path: Path, untracked: bool) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as writer, closing(sqlite3.connect(path)) as reader:
        _insert(writer, "fund_rank_history", "000001", .2)
        if untracked:
            begin_rank_history_read_snapshot(reader)
            _insert(writer, "fund_rank_history", "000002", .8)
        snapshot, binding = _scope(reader, path)
        assert snapshot.verified is not untracked
        assert _read("stats", reader, path, binding, snapshot) == {"000001"}
        assert snapshot.local_stats is not None
        if not untracked:
            _insert(writer, "fund_rank_history", "000002", .8)
        reader.rollback()
        begin_rank_history_read_snapshot(reader)
        assert _read("stats", reader, path, binding, snapshot) == {"000001", "000002"}


def test_own_write_invalidates_scope_without_committing_caller_transaction(tmp_path: Path) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as reader:
        _insert(reader, "fund_rank_history", "000001", .2)
        snapshot, binding = _scope(reader, path)
        assert _read("stats", reader, path, binding, snapshot) == {"000001"}
        reader.execute("INSERT INTO fund_rank_history VALUES ('000002','2026-01-15','overall',.8)")
        assert not snapshot.usable(reader, path, binding)
        assert _read("stats", reader, path, binding, snapshot) == {"000001", "000002"}
        assert reader.in_transaction
        reader.rollback()
        assert _read("stats", reader, path, None) == {"000001"}
        assert not reader.in_transaction


def test_shared_cache_hits_and_local_results_are_independent_copies(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "hot.db"
    sidecar = tmp_path / "stats.json"
    monkeypatch.setattr(report, "_long_term_history_rank_snapshot_stats_sidecar_path", lambda _path: sidecar)
    with closing(_database(path, "fund_rank_history")) as writer:
        _insert(writer, "fund_rank_history", "000001", .2)
        with closing(sqlite3.connect(path)) as reader:
            snapshot, binding = _scope(reader, path)
            first = report._read_long_term_history_rank_snapshot_stats(reader, path, binding, cache_snapshot=snapshot)
            first["codes"].clear()
            first["latest_date_map"].clear()
            second = report._read_long_term_history_rank_snapshot_stats(reader, path, binding, cache_snapshot=snapshot)
            assert second["codes"] == {"000001"}
            assert second["latest_date_map"] == {"000001": "2026-01-15"}
            frame = report._read_long_term_rank_metrics(reader, path, binding, cache_snapshot=snapshot)
            frame.loc[:, "fcode"] = "changed-by-caller"
            assert _read("metrics", reader, path, binding, snapshot) == {"000001"}
        report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()

        def unexpected_query(*_args, **_kwargs):
            raise AssertionError("unchanged source should reuse the verified disk/RAM cache")

        monkeypatch.setattr(report, "_aggregate_long_term_history_rank_snapshot_stats", unexpected_query)
        monkeypatch.setattr(report, "_read_long_term_rank_metrics_from_hot_using_snapshot", unexpected_query)
        with closing(sqlite3.connect(path)) as reader:
            snapshot, binding = _scope(reader, path)
            assert _read("stats", reader, path, binding, snapshot) == {"000001"}
            assert _read("metrics", reader, path, binding, snapshot) == {"000001"}


@pytest.mark.parametrize("damage", ["null", "list", "bad_json", "bad_schema", "source_type",
                                    "source_mismatch", "missing_count", "bool_count", "negative_count",
                                    "missing_hot", "foreign_code", "missing_digest", "bad_digest"])
def test_damaged_sidecar_is_recomputed(tmp_path: Path, monkeypatch, damage: str) -> None:
    path, sidecar = tmp_path / "hot.db", tmp_path / "stats.json"
    monkeypatch.setattr(report, "_long_term_history_rank_snapshot_stats_sidecar_path", lambda _path: sidecar)
    with closing(_database(path, "fund_rank_history")) as writer:
        _insert(writer, "fund_rank_history", "000001", .2)
        with closing(sqlite3.connect(path)) as reader:
            assert _read("stats", reader, path, None) == {"000001"}
        payload = json.loads(sidecar.read_text())
        if damage in {"null", "list"}:
            payload = None if damage == "null" else []
        elif damage == "bad_schema":
            payload["schema_version"] = "broken"
        elif damage == "source_type":
            payload["source_fingerprint"] = []
        elif damage == "source_mismatch":
            payload["source_fingerprint"]["archive_alias"] = "different"
        elif damage == "missing_count":
            payload["stats"].pop("record_count")
        elif damage == "bool_count":
            payload["stats"]["record_count"] = True
        elif damage == "negative_count":
            payload["stats"]["record_count"] = -1
        elif damage == "missing_hot":
            payload["stats"].pop("hot_percentile_codes")
        elif damage == "foreign_code":
            payload["stats"]["feature_ready_codes"] = ["999999"]
            payload["stats_sha256"] = hashlib.sha256(json.dumps(payload["stats"], ensure_ascii=False,
                                                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        elif damage == "missing_digest":
            payload.pop("stats_sha256")
        elif damage == "bad_digest":
            payload["stats_sha256"] = "0" * 64
        sidecar.write_text("{" if damage == "bad_json" else json.dumps(payload))
        report._LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
        calls = []
        aggregate = report._aggregate_long_term_history_rank_snapshot_stats

        def counted(*args):
            calls.append(True)
            return aggregate(*args)

        monkeypatch.setattr(report, "_aggregate_long_term_history_rank_snapshot_stats", counted)
        with closing(sqlite3.connect(path)) as reader:
            assert _read("stats", reader, path, None) == {"000001"}
        assert calls == [True]
        assert json.loads(sidecar.read_text())["stats"]["record_count"] == 1
        assert list(tmp_path.glob("stats.json.*.tmp")) == []


@pytest.mark.parametrize("cold", [False, True])
def test_same_date_edits_deletions_and_checkpoints_invalidate_cache(tmp_path: Path, cold: bool) -> None:
    path, archive = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(_database(path, "fund_rank_history")) as hot, \
            closing(_database(archive, "fund_rank_history_archive")) as cold_writer:
        writer, table = (cold_writer, "fund_rank_history_archive") if cold else (hot, "fund_rank_history")
        _insert(writer, table, "000001", .2)

        def value():
            with closing(sqlite3.connect(path)) as reader:
                snapshot, binding = _scope(reader, path, archive if cold else None)
                stats = report._read_long_term_history_rank_snapshot_stats(reader, path, binding, cache_snapshot=snapshot)
                metrics = report._read_long_term_rank_metrics(reader, path, binding, cache_snapshot=snapshot)
                return stats, metrics

        first, first_metrics = value()
        assert first_metrics.iloc[0]["rank_avg_percentile_12m"] == pytest.approx(.2)
        writer.execute(f"UPDATE {table} SET percentile=.8 WHERE fcode='000001'")
        writer.commit()
        updated, updated_metrics = value()
        assert updated["latest_date_map"] == first["latest_date_map"]
        assert updated_metrics.iloc[0]["rank_avg_percentile_12m"] == pytest.approx(.8)
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        assert value()[1].iloc[0]["rank_avg_percentile_12m"] == pytest.approx(.8)
        writer.execute(f"DELETE FROM {table} WHERE fcode='000001'")
        writer.commit()
        deleted, deleted_metrics = value()
        assert deleted["record_count"] == 0 and deleted["codes"] == set()
        assert deleted_metrics.empty


def test_archive_path_alias_and_cutoff_are_part_of_cache_binding(tmp_path: Path) -> None:
    path, first, second = (tmp_path / name for name in ("hot.db", "first.db", "second.db"))
    with closing(_database(path, "fund_rank_history")), \
            closing(_database(first, "fund_rank_history_archive")) as first_writer, \
            closing(_database(second, "fund_rank_history_archive")) as second_writer:
        _insert(first_writer, "fund_rank_history_archive", "000001", .2)
        _insert(second_writer, "fund_rank_history_archive", "000002", .8)
        keys = set()
        for archive, alias, cutoff, codes in [(first, "rank_archive_db", "2026-02-01", {"000001"}),
                                               (second, "rank_archive_db", "2026-02-01", {"000002"}),
                                               (second, "other_rank", "2026-02-01", {"000002"}),
                                               (second, "other_rank", "2026-01-01", set())]:
            with closing(sqlite3.connect(path)) as reader:
                snapshot = RankCacheSnapshot.start(reader, path, begin_rank_history_read_snapshot)
                binding = snapshot.attach(attach_rank_history_archive_if_available, archive_path=archive,
                                          alias=alias, archive_cutoff_date=cutoff)
                keys.add(report._long_term_rank_metrics_cache_key(path, binding, snapshot))
                assert _read("stats", reader, path, binding, snapshot) == codes
                assert _read("metrics", reader, path, binding, snapshot) == codes
        assert len(keys) == 4


def test_empty_wal_has_no_data_to_invalidate_a_cache(tmp_path: Path) -> None:
    path = tmp_path / "main.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE fund_rank_history (fcode TEXT)")
    before = sqlite_snapshot_token(path)
    wal = Path(str(path) + "-wal")
    wal.touch()
    assert sqlite_snapshot_token(path) == before
    wal.touch()
    assert sqlite_snapshot_token(path) == before
    wal.unlink()
    assert sqlite_snapshot_token(path) == before


def test_reused_wal_tail_is_detected_even_when_size_and_mtime_match(tmp_path: Path) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as writer:
        for number in range(1, 17):
            _insert(writer, "fund_rank_history", str(number).zfill(6), .1)
        writer.execute("PRAGMA wal_checkpoint(RESTART)")
        writer.execute("UPDATE fund_rank_history SET percentile=.2")
        writer.commit()
        with closing(sqlite3.connect(path)) as reader:
            first = report._read_long_term_rank_metrics(reader, path, None)
            assert first["rank_avg_percentile_12m"].tolist() == pytest.approx([.2] * 16)
        wal = Path(str(path) + "-wal")
        original = wal.stat()
        before_token = sqlite_snapshot_token(path)
        writer.execute("UPDATE fund_rank_history SET percentile=.8")
        writer.commit()
        assert wal.stat().st_size == original.st_size
        # Model a filesystem timestamp with insufficient resolution. The
        # actual WAL still contains a new commit; no business row is mocked.
        os.utime(wal, ns=(original.st_atime_ns, original.st_mtime_ns))
        assert wal.stat().st_mtime_ns == original.st_mtime_ns
        assert sqlite_snapshot_token(path) != before_token
        with closing(sqlite3.connect(path)) as reader:
            second = report._read_long_term_rank_metrics(reader, path, None)
            assert second["rank_avg_percentile_12m"].tolist() == pytest.approx([.8] * 16)


def test_pandas_rollback_must_not_silently_move_to_a_new_read_snapshot(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "hot.db"
    with closing(_database(path, "fund_rank_history")) as writer, closing(sqlite3.connect(path)) as reader:
        _insert(writer, "fund_rank_history", "000001", .2)
        snapshot, binding = _scope(reader, path)
        assert _read("stats", reader, path, binding, snapshot) == {"000001"}
        read_sql = report.pd.read_sql_query
        calls = []

        def fail_first(sql, conn, **kwargs):
            calls.append(sql)
            if len(calls) == 1:
                try:
                    return read_sql("SELECT missing_column FROM fund_rank_history", conn)
                finally:
                    # pandas rolls back the read transaction on this genuine
                    # SQLite error. The next unguarded fallback would see new rows.
                    assert not reader.in_transaction
                    _insert(writer, "fund_rank_history", "000002", .8)
            return read_sql(sql, conn, **kwargs)

        monkeypatch.setattr(report.pd, "read_sql_query", fail_first)
        with pytest.raises(RuntimeError, match="rank_read_snapshot_lost"):
            report._read_long_term_rank_metrics(reader, path, binding, cache_snapshot=snapshot)
        assert len(calls) == 1 and not report._LONG_TERM_RANK_METRICS_CACHE
