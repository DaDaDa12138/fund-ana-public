"""Synthetic, isolated storage/recovery checks; no production business fixtures."""
from contextlib import closing
import hashlib
import json
import sqlite3
import struct
import zlib

import pytest

from database import screening_pick_payloads as payloads
from database.screening_decision_payloads import canonical_source
from scripts import migrate_pick_snapshots as migration

RAW = json.dumps({"risk_metrics": {"raw_sharpe_ratio": None, "sharpe_ratio": None,
                 "raw_sortino_ratio": 0.0, "max_drawdown_duration": 0},
                 "coverage": {"period_days": 365}, "note": "仅作隔离恢复试验 " * 40}, ensure_ascii=False, indent=2)


@pytest.mark.parametrize("value", [None, "", "0", "null", "{}", "not json", RAW, RAW + "\n"])
def test_snapshot_roundtrip_preserves_raw_text_null_whitespace_and_zero(value):
    stored = payloads.encode_snapshot(value)
    assert payloads.decode_snapshot(stored) == value
    assert payloads.encode_snapshot(stored) == stored
    if value in (RAW, RAW + "\n"):
        assert isinstance(stored, bytes) and len(stored) < len(value.encode())
    else:
        assert stored == value


@pytest.mark.parametrize("damage", ["header", "short", "length", "sha", "truncated", "trailing", "stream", "oversize", "utf8", "bomb", "type"])
def test_invalid_compression_is_an_error_not_missing_business_data(damage):
    value = payloads.encode_snapshot(RAW)
    if damage == "header":
        value = b"UNKNOWN!" + value[8:]
    elif damage == "short":
        value = value[:30]
    elif damage == "length":
        value = value[:8] + struct.pack(">Q", 1) + value[16:]
    elif damage == "sha":
        value = value[:16] + b"x" * 32 + value[48:]
    elif damage == "truncated":
        value = value[:-1]
    elif damage == "trailing":
        value += b"x"
    elif damage == "stream":
        value += zlib.compress(b"second stream")
    elif damage == "oversize":
        value = value[:8] + struct.pack(">Q", payloads.MAX_DECODED_BYTES + 1) + value[16:]
    elif damage == "utf8":
        value = payloads.MAGIC + struct.pack(">Q", 1) + hashlib.sha256(b"\xff").digest() + zlib.compress(b"\xff")
    elif damage == "bomb":
        value = payloads.MAGIC + struct.pack(">Q", 1) + hashlib.sha256(b"x").digest() + zlib.compress(b"x" * 1000000)
    else:
        value = 0
    with pytest.raises(payloads.PickPayloadError):
        payloads.decode_snapshot(value)
    with pytest.raises(payloads.PickPayloadError):
        payloads.encode_snapshot(value)


def test_incompressible_or_large_legacy_text_remains_text(monkeypatch):
    monkeypatch.setattr(payloads, "MAX_DECODED_BYTES", 100)
    assert payloads.encode_snapshot(RAW) == RAW
    text = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    assert payloads.encode_snapshot(text) == text


def make_picks(path):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE screening_picks(id INTEGER PRIMARY KEY,run_id TEXT,fcode TEXT,fund_type TEXT,"
                 "period_years REAL,value REAL,snapshot_json TEXT,picked_at NUM)")
    conn.executemany("INSERT INTO screening_picks VALUES(?,?,?,?,?,?,?,?)", [
        (-(2**63), "run-a", "unit-a", "bond", 1.0, 0.0, RAW, "2026-01-01 01:02:03.123456"),
        (-1, "run-a", "unit-b", "bond", 1.0, None, RAW + "\n", None),
        (0, "run-a", "unit-c", "bond", 1.0, -1.0, None, ""),
        (4, "run-b", "unit-d", "bond", 1.0, 1.0, "", "2026-01-02"),
        (9, "run-b", "unit-e", "bond", 1.0, 0.0, "0", "2026-01-02"),
        (12, "run-b", "unit-f", "bond", 1.0, None, RAW, "2026-01-02"),
    ])
    conn.commit()
    return conn


def full_rows(conn, *, decoded=False):
    if not decoded:
        return conn.execute("SELECT * FROM screening_picks ORDER BY id").fetchall()
    columns = [row[1] for row in conn.execute("PRAGMA table_info(screening_picks)")]
    projection, source = canonical_source(conn, "screening_picks", columns)
    return conn.execute(f"SELECT {projection} FROM {source} ORDER BY h.id").fetchall()


def test_orm_new_writes_and_legacy_reads_are_exact_and_keep_text_schema(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from database.models import ScreeningPick

    engine = create_engine("sqlite:///" + str(tmp_path / "orm.db"))
    ScreeningPick.__table__.create(engine)
    with Session(engine) as session, session.begin():
        session.add_all([ScreeningPick(run_id="unit-run", fcode=f"unit-{index}", rank=index + 1, snapshot_json=value)
                         for index, value in enumerate([RAW, None, "", "0"])])
    with engine.begin() as conn:
        assert conn.exec_driver_sql("SELECT typeof(snapshot_json) FROM screening_picks ORDER BY id").fetchall() == [
            ("blob",), ("null",), ("text",), ("text",)]
        assert next(row[2] for row in conn.exec_driver_sql("PRAGMA table_info(screening_picks)") if row[1] == "snapshot_json") == "TEXT"
        conn.exec_driver_sql("INSERT INTO screening_picks(run_id,fcode,rank,snapshot_json) VALUES(?,?,?,?)", ("unit-run", "unit-old", 5, RAW))
    with Session(engine) as session:
        assert [row.snapshot_json for row in session.query(ScreeningPick).order_by(ScreeningPick.id)] == [RAW, None, "", "0", RAW]
    engine.dispose()


def test_forward_reverse_migration_preserves_all_values_types_and_ids(tmp_path):
    database = tmp_path / "picks.db"
    with closing(make_picks(database)) as conn:
        original = full_rows(conn)
    forward = migration.migrate(database, tmp_path / "compress.json", mode="compress", batch_rows=2)
    assert forward["status"] == "complete" and forward["selected_rows"] == 6
    assert forward["payloads_compressed"] == 3 and forward["minimum_id"] == -(2**63)
    with closing(sqlite3.connect(database)) as conn:
        assert full_rows(conn, decoded=True) == original
        assert conn.execute("SELECT COUNT(*) FROM screening_picks WHERE typeof(snapshot_json)='blob'").fetchone()[0] == 3
    again = migration.migrate(database, tmp_path / "compress.json", mode="compress", batch_rows=2, resume=True)
    assert again == forward
    restored = migration.migrate(database, tmp_path / "restore.json", mode="restore", batch_rows=2)
    assert restored["payloads_restored"] == 3
    assert restored["business_value_chain_sha256"] == forward["business_value_chain_sha256"]
    with closing(sqlite3.connect(database)) as conn:
        assert full_rows(conn) == original
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def pause_after_first_page(monkeypatch):
    ticks = iter([0.0, 0.1, 2.0])
    monkeypatch.setattr(migration.time, "monotonic", lambda: next(ticks, 2.1))


def test_fixed_cutoff_and_budget_resume_leave_later_rows_untouched(tmp_path, monkeypatch):
    database, checkpoint = tmp_path / "picks.db", tmp_path / "progress.json"
    with closing(make_picks(database)):
        pass
    with monkeypatch.context() as local:
        pause_after_first_page(local)
        first = migration.migrate(database, checkpoint, mode="compress", batch_rows=2, through_id=4, max_seconds=1)
    assert first["status"] == "partial_budget_exhausted" and first["selected_rows"] == 2
    result = migration.migrate(database, checkpoint, mode="compress", batch_rows=2, resume=True)
    assert result["status"] == "complete" and result["selected_rows"] == 4
    with closing(sqlite3.connect(database)) as conn:
        assert conn.execute("SELECT snapshot_json FROM screening_picks WHERE id=12").fetchone()[0] == RAW


def test_committed_page_before_checkpoint_can_replay_without_data_loss(tmp_path, monkeypatch):
    database, checkpoint = tmp_path / "picks.db", tmp_path / "progress.json"
    with closing(make_picks(database)) as conn:
        original = full_rows(conn)
    real_save = migration.save

    def interrupted(path, result):
        if result["completed_pages"] == 1:
            raise KeyboardInterrupt("unit interruption after commit")
        real_save(path, result)

    with monkeypatch.context() as local:
        local.setattr(migration, "save", interrupted)
        with pytest.raises(KeyboardInterrupt):
            migration.migrate(database, checkpoint, mode="compress", batch_rows=2)
    assert json.loads(checkpoint.read_text())["last_id"] is None
    result = migration.migrate(database, checkpoint, mode="compress", batch_rows=2, resume=True)
    assert result["status"] == "complete" and result["selected_rows"] == 6
    with closing(sqlite3.connect(database)) as conn:
        assert full_rows(conn, decoded=True) == original


@pytest.mark.parametrize("damage", ["value", "storage"])
def test_trigger_damage_rolls_back_whole_page_and_can_resume(tmp_path, damage):
    database, checkpoint = tmp_path / "picks.db", tmp_path / "progress.json"
    with closing(make_picks(database)) as conn, conn:
        original = full_rows(conn)
        update = "value=99" if damage == "value" else "snapshot_json=OLD.snapshot_json"
        conn.execute("CREATE TRIGGER unit_damage AFTER UPDATE OF snapshot_json ON screening_picks "
                     f"BEGIN UPDATE screening_picks SET {update} WHERE id=NEW.id; END")
    with pytest.raises(ValueError, match="business_row_changed|representation_mismatch"):
        migration.migrate(database, checkpoint, mode="compress", batch_rows=2)
    with closing(sqlite3.connect(database)) as conn, conn:
        assert full_rows(conn) == original
        conn.execute("DROP TRIGGER unit_damage")
    assert migration.migrate(database, checkpoint, mode="compress", batch_rows=2, resume=True)["status"] == "complete"


@pytest.mark.parametrize("damage", ["mode", "database", "table_schema", "selected_rows", "last_id", "business_value_chain_sha256"])
def test_bad_checkpoints_cannot_mutate_database_or_overwrite_evidence(tmp_path, monkeypatch, damage):
    database, checkpoint = tmp_path / "picks.db", tmp_path / "progress.json"
    with closing(make_picks(database)):
        pass
    with monkeypatch.context() as local:
        pause_after_first_page(local)
        result = migration.migrate(database, checkpoint, mode="compress", batch_rows=2, max_seconds=1)
    result[damage] = "invalid"
    checkpoint.write_text(json.dumps(result))
    original_checkpoint, original_database = checkpoint.read_bytes(), database.read_bytes()
    with pytest.raises(ValueError, match="checkpoint"):
        migration.migrate(database, checkpoint, mode="compress", batch_rows=2, resume=True)
    assert checkpoint.read_bytes() == original_checkpoint and database.read_bytes() == original_database


def test_resume_rechecks_earlier_pages_not_only_last_page(tmp_path):
    database, checkpoint = tmp_path / "picks.db", tmp_path / "progress.json"
    with closing(make_picks(database)):
        pass
    migration.migrate(database, checkpoint, mode="compress", batch_rows=2)
    with closing(sqlite3.connect(database)) as conn, conn:
        conn.execute("UPDATE screening_picks SET value=123 WHERE id=?", (-(2**63),))
    before = checkpoint.read_bytes()
    with pytest.raises(ValueError, match="completed_values_changed"):
        migration.migrate(database, checkpoint, mode="compress", batch_rows=2, resume=True)
    assert checkpoint.read_bytes() == before


def test_detail_and_metric_readers_decode_blob_and_propagate_storage_corruption(tmp_path):
    from backend.report_builder import _read_pick_snapshot_metrics_from_detail
    from database.screening_pick_metric_snapshots import _parse_snapshot_payload

    with closing(make_picks(tmp_path / "picks.db")) as conn, conn:
        conn.execute("DELETE FROM screening_picks WHERE id!=12")
        conn.execute("UPDATE screening_picks SET snapshot_json=?", (payloads.encode_snapshot(RAW),))
        frame = _read_pick_snapshot_metrics_from_detail(conn)
        assert frame["max_drawdown_duration"].tolist() == [0]
        assert _parse_snapshot_payload(payloads.encode_snapshot(RAW))["risk_metrics"]["raw_sharpe_ratio"] is None
        conn.execute("UPDATE screening_picks SET snapshot_json=?", (b"damaged",))
        with pytest.raises(payloads.PickPayloadError):
            _read_pick_snapshot_metrics_from_detail(conn)
        with pytest.raises(payloads.PickPayloadError):
            _parse_snapshot_payload(b"damaged")


def add_archive_tables(conn):
    from scripts.export_screening_parquet import TABLES

    conn.execute("CREATE TABLE screening_runs(id INT,run_id TEXT,started_at NUM,fund_type TEXT)")
    conn.executemany("INSERT INTO screening_runs VALUES(?,?,?,?)", [
        (1, "run-a", "2026-01-01 01:02:03", "bond"), (2, "run-b", "2026-01-02 01:02:03", "bond")])
    for table in TABLES:
        if table not in {"screening_runs", "screening_picks"}:
            conn.execute(f"CREATE TABLE {table}(id INT,run_id TEXT)")
    conn.commit()


def test_sqlite_archive_keeps_original_text_affinity_and_blocks_damaged_snapshots(tmp_path):
    from utils.screening_history_archive_execution import _execute_archive_export
    from utils.screening_archive_access import verify_sqlite_hot_values

    hot, cold = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(make_picks(hot)) as conn:
        original = full_rows(conn)
        add_archive_tables(conn)
    migration.migrate(hot, tmp_path / "progress.json", mode="compress")
    result = _execute_archive_export(db_path=hot, archive_target_path=cold, run_ids=["run-a", "run-b"], chunk_size=2)
    assert result["status"] == "archive-exported", result
    with closing(sqlite3.connect(cold)) as archive, closing(sqlite3.connect(hot)) as source:
        assert full_rows(archive) == original
        assert next(row[2] for row in archive.execute("PRAGMA table_info(screening_picks)") if row[1] == "snapshot_json") == "TEXT"
        assert verify_sqlite_hot_values(archive, source, ["run-a", "run-b"])["row_count"] == 8
    with closing(sqlite3.connect(hot)) as conn, conn:
        conn.execute("UPDATE screening_picks SET snapshot_json=? WHERE id=12", (b"damaged",))
    failed = _execute_archive_export(db_path=hot, archive_target_path=tmp_path / "damaged.db",
                                    run_ids=["run-a", "run-b"], chunk_size=2)
    assert failed["status"] == "failed"


def test_direct_parquet_export_and_restore_decode_snapshots_preserving_original_rowid(tmp_path):
    from scripts.export_screening_parquet import TABLES, export_shard, restore_run

    hot = tmp_path / "sealed.db"
    with closing(make_picks(hot)) as conn:
        original = full_rows(conn)
        add_archive_tables(conn)
        counts = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
    migration.migrate(hot, tmp_path / "progress.json", mode="compress")
    stat = hot.stat()
    shard = {"state": "immutable", "sha256": hashlib.sha256(hot.read_bytes()).hexdigest(),
             "file_size_bytes": stat.st_size, "file_mtime_ns": stat.st_mtime_ns, "table_row_counts": counts}
    export = tmp_path / "parquet"
    assert export_shard(hot, export, shard, batch_rows=2)["status"] == "verified"
    for run in ("run-a", "run-b"):
        target = tmp_path / (run + ".db")
        assert restore_run(export, run, target)["quick_check"] == "ok"
        with closing(sqlite3.connect(target)) as conn:
            assert full_rows(conn) == [row for row in original if row[1] == run]
            assert conn.execute("SELECT rowid,id FROM screening_picks").fetchall() == [(row[0], row[0]) for row in original if row[1] == run]


def test_truth_audit_reports_checked_scope_and_blocks_damaged_storage(tmp_path):
    from scripts.audit_data_truthfulness import _audit_source_truth_manifest_table

    spec = {"name": "screening_picks", "required": True, "min_row_count": 1,
            "required_fields": ["id", "snapshot_json"]}
    with closing(make_picks(tmp_path / "picks.db")) as conn, conn:
        conn.row_factory = sqlite3.Row
        conn.execute("UPDATE screening_picks SET snapshot_json=? WHERE id=12", (payloads.encode_snapshot(RAW),))
        info, blockers, _ = _audit_source_truth_manifest_table(conn, spec)
        check = info["snapshot_storage_check"]
        assert not blockers and check["sampled_row_count"] == 6
        assert check["storage_counts"] == {"compressed_blob": 1, "text": 4, "null": 1, "invalid": 0}
        assert check["full_history_verified"] is False
        conn.execute("UPDATE screening_picks SET snapshot_json=? WHERE id=12", (b"damaged",))
        info, blockers, _ = _audit_source_truth_manifest_table(conn, spec)
        assert blockers == ["source_truth_pick_snapshot_storage_invalid"]
        assert info["snapshot_storage_check"]["storage_counts"]["invalid"] == 1
