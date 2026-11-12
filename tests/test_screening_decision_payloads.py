"""Synthetic, isolated checks for exact decision payload sharing and recovery."""
from contextlib import closing
import hashlib
import json
import sqlite3

import pytest

from database import screening_decision_payloads as payloads
from utils.screening_history_archive_execution import _execute_archive_export
from scripts.export_screening_parquet import TABLES, export_shard, restore_run


RAW = '{"zero":0,"missing":null,"label":"测试"}'


def make_legacy(path, *, factory=sqlite3.Connection):
    conn = sqlite3.connect(path, factory=factory)
    conn.execute("CREATE TABLE screening_decisions(id INTEGER PRIMARY KEY,run_id TEXT,fcode TEXT,step_id INTEGER,"
                 "step_name TEXT,passed NUM,reason_code TEXT,reason_detail TEXT,metrics_json TEXT,created_at NUM)")
    conn.executemany("INSERT INTO screening_decisions VALUES(?,?,?,?,?,?,?,?,?,?)", [
        (1, "run-a", "unit-a", 1, "unit", 1, "pass", "original", RAW, "2026-01-01 01:02:03"),
        (2, "run-a", "unit-b", 1, "unit", 0, "fail", "other fund", RAW, "2026-01-01 01:02:04"),
        (4, "run-b", "unit-a", 2, "unit", 0, "missing", None, None, None),
        (5, "run-b", "unit-b", 2, "unit", 1, "empty", None, "", None),
        (9, "run-b", "unit-c", 3, "unit", 1, "null", None, "null", None),
        (12, "run-b", "unit-d", 3, "unit", 1, "zero", None, "0", None),
    ])
    conn.commit()
    return conn


def metrics(conn):
    return conn.execute("SELECT id,metrics_json FROM screening_decisions ORDER BY id").fetchall()


def full_values(conn):
    columns = [row[1] for row in conn.execute("PRAGMA table_info(screening_decisions)")]
    projection, source = payloads.canonical_source(conn, "screening_decisions", columns)
    payloads.verify_reference_scope(conn, ["run-a", "run-b"])
    return conn.execute(f"SELECT {projection} FROM {source} ORDER BY h.id").fetchall()


def add_archive_tables(conn):
    conn.execute("CREATE TABLE screening_runs(id INT,run_id TEXT,started_at NUM,fund_type TEXT)")
    conn.executemany("INSERT INTO screening_runs VALUES(?,?,?,?)", [
        (1, "run-a", "2026-01-01 01:02:03", "bond"), (2, "run-b", "2026-01-02 01:02:03", "bond")])
    for table in TABLES:
        if table not in {"screening_runs", "screening_decisions"}:
            conn.execute(f"CREATE TABLE {table}(id INT,run_id TEXT)")
    conn.commit()


@pytest.mark.parametrize("remove_inline", [False, True])
def test_full_rows_and_original_text_null_empty_zero_are_preserved(tmp_path, remove_inline):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        before = full_values(conn)
        conn.commit()
        with conn:
            payloads.ensure_storage(conn)
            result = payloads.share_existing_rows(conn, metrics(conn), remove_inline=remove_inline)
        assert result["all_original_payloads_match"]
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.PAYLOADS}").fetchone()[0] == 4
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS}").fetchone()[0] == 5
        assert full_values(conn) == before
        assert payloads.resolve_metric_texts(conn, metrics(conn)) == [RAW, RAW, None, "", "null", "0"]
        conn.commit()
        with conn:
            repeated = payloads.share_existing_rows(conn, metrics(conn), remove_inline=remove_inline)
        assert repeated["references_inserted"] == 0
        assert full_values(conn) == before


def test_sqlite_cleanup_proof_resolves_shared_text_without_collapsing_null_empty_or_zero(tmp_path):
    from utils.screening_archive_access import verify_sqlite_hot_values

    with closing(make_legacy(tmp_path / "hot.db")) as hot, closing(sqlite3.connect(tmp_path / "cold.db")) as cold:
        hot.backup(cold)
        with hot:
            payloads.ensure_storage(hot)
            payloads.share_existing_rows(hot, metrics(hot), remove_inline=True)
        proof = verify_sqlite_hot_values(cold, hot, ["run-a", "run-b"])
        assert proof["row_count"] == 6
        assert all(row["all_values_match"] for row in proof["table_checks"])
        assert payloads.resolve_metric_texts(hot, metrics(hot)) == [RAW, RAW, None, "", "null", "0"]
        cold.commit()
        with cold:
            cold.execute("UPDATE screening_decisions SET metrics_json='0' WHERE id=4")
        with pytest.raises(ValueError, match="sqlite_hot_values_mismatch:screening_decisions:run-b"):
            verify_sqlite_hot_values(cold, hot, ["run-a", "run-b"])


def test_sqlite_cleanup_proof_retains_declared_ids_but_ignores_implicit_storage_rowid(tmp_path):
    from utils.screening_archive_access import verify_sqlite_hot_values

    with closing(sqlite3.connect(tmp_path / "hot.db")) as hot, closing(sqlite3.connect(tmp_path / "cold.db")) as cold:
        for conn, rowid in ((hot, 1), (cold, 500)):
            conn.execute("CREATE TABLE screening_decisions(id INT,run_id TEXT,metrics_json TEXT)")
            conn.execute("INSERT INTO screening_decisions(rowid,id,run_id,metrics_json) VALUES(?,9,'run-a',?)", (rowid, RAW))
            conn.commit()
        assert verify_sqlite_hot_values(cold, hot, ["run-a"])["row_count"] == 1
        cold.commit()
        with cold:
            cold.execute("UPDATE screening_decisions SET id=10 WHERE run_id='run-a'")
        with pytest.raises(ValueError, match="sqlite_hot_values_mismatch:screening_decisions:run-a"):
            verify_sqlite_hot_values(cold, hot, ["run-a"])


def test_equal_json_with_different_raw_bytes_is_not_coalesced(tmp_path):
    with closing(make_legacy(tmp_path / "hot.db")) as conn, conn:
        payloads.ensure_storage(conn)
        other = json.dumps(json.loads(RAW), ensure_ascii=True, indent=2)
        ids = payloads.intern_payloads(conn, [RAW, other, RAW, None])
        assert ids[0] == ids[2] and ids[0] != ids[1] and ids[3] is None


@pytest.mark.parametrize("foreign_keys", [0, 1])
def test_reference_guards_cascade_and_gc_preserve_other_funds(tmp_path, foreign_keys):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        conn.execute(f"PRAGMA foreign_keys={foreign_keys}")
        with conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
        shared_id = conn.execute(f"SELECT payload_id FROM {payloads.REFS} WHERE decision_id=1").fetchone()[0]
        for sql, params in [
            (f"DELETE FROM {payloads.PAYLOADS} WHERE id=?", (shared_id,)),
            (f"UPDATE {payloads.PAYLOADS} SET payload='different' WHERE id=?", (shared_id,)),
            (f"DELETE FROM {payloads.REFS} WHERE decision_id=1", ()),
            ("UPDATE screening_decisions SET metrics_json='different' WHERE id=1", ()),
            ("UPDATE screening_decisions SET id=99 WHERE id=1", ()),
        ]:
            with pytest.raises(sqlite3.IntegrityError), conn:
                conn.execute(sql, params)
        with conn:
            assert conn.execute("DELETE FROM screening_decisions WHERE id=1").rowcount == 1
            assert payloads.collect_unreferenced_payloads(conn) == 0
        assert payloads.resolve_metric_texts(conn, [(2, None)]) == [RAW]
        with conn:
            assert conn.execute("DELETE FROM screening_decisions WHERE id=2").rowcount == 1
            assert payloads.collect_unreferenced_payloads(conn) == 1
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS} WHERE decision_id IN (1,2)").fetchone()[0] == 0


@pytest.mark.parametrize("damage", ["missing_object", "wrong_sha", "missing_tables", "different_inline"])
def test_corrupt_storage_is_an_error_not_a_missing_metric(tmp_path, damage):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        with conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
        conn.execute("PRAGMA foreign_keys=OFF")
        if damage == "missing_object":
            conn.execute("DROP TRIGGER decision_payload_referenced")
            conn.execute(f"DELETE FROM {payloads.PAYLOADS}")
        elif damage == "wrong_sha":
            conn.execute("DROP TRIGGER decision_payload_immutable")
            conn.execute(f"UPDATE {payloads.PAYLOADS} SET sha256=zeroblob(32) WHERE id=(SELECT MIN(id) FROM {payloads.PAYLOADS})")
        elif damage == "missing_tables":
            conn.execute(f"DROP TABLE {payloads.REFS}")
            conn.execute(f"DROP TABLE {payloads.PAYLOADS}")
        else:
            conn.execute("DROP TRIGGER decision_payload_update_inline")
            conn.execute("UPDATE screening_decisions SET metrics_json='wrong' WHERE id=1")
        conn.commit()
        with pytest.raises(payloads.DecisionPayloadError):
            payloads.resolve_metric_texts(conn, metrics(conn))


def test_rollback_keeps_inline_and_does_not_leave_partial_storage(tmp_path):
    class FailSchema(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if "CREATE TRIGGER IF NOT EXISTS decision_payload_ref_valid" in sql:
                raise sqlite3.OperationalError("injected schema write failure")
            return super().execute(sql, *args, **kwargs)

    with closing(make_legacy(tmp_path / "hot.db", factory=FailSchema)) as conn:
        original = metrics(conn)
        with pytest.raises(sqlite3.OperationalError), conn:
            payloads.ensure_storage(conn)
        assert not payloads.storage_available(conn)
        assert metrics(conn) == original


def test_digest_collision_is_rejected_without_overwriting_an_object(tmp_path, monkeypatch):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        with conn:
            payloads.ensure_storage(conn)
        monkeypatch.setattr(payloads, "_digest", lambda value: b"x" * 32)
        with conn:
            payloads.intern_payloads(conn, ["first raw value"])
        with pytest.raises(payloads.DecisionPayloadError, match="collision"), conn:
            payloads.intern_payloads(conn, ["different raw value"])
        assert conn.execute(f"SELECT payload FROM {payloads.PAYLOADS}").fetchall() == [("first raw value",)]


def test_sqlite_backup_and_reverse_migration_restore_exact_legacy_rows(tmp_path):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        before = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
        with conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
        with closing(sqlite3.connect(tmp_path / "backup.db")) as copy:
            conn.backup(copy)
        with closing(sqlite3.connect(tmp_path / "backup.db")) as copy:
            assert full_values(copy) == before
            copy.commit()
            with copy:
                result = payloads.restore_inline_rows(copy, metrics(copy))
                assert result["references_removed"] == 5
                assert payloads.collect_unreferenced_payloads(copy) == 4
            assert copy.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == before
            assert copy.execute("PRAGMA quick_check").fetchone()[0] == "ok"


@pytest.mark.parametrize("mutation", ["passed=1", "metrics_json='different raw text'"])
@pytest.mark.parametrize("immutable", [False, True])
def test_equal_row_counts_with_wrong_archive_values_cannot_pass_recovery_gate(tmp_path, mutation, immutable):
    hot, cold = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(make_legacy(hot)) as conn:
        add_archive_tables(conn)
        with conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
    kwargs = {"db_path": hot, "archive_target_path": cold, "run_ids": ["run-a", "run-b"], "chunk_size": 100}
    assert _execute_archive_export(**kwargs)["status"] == "archive-exported"
    with closing(sqlite3.connect(cold)) as conn, conn:
        conn.execute(f"UPDATE screening_decisions SET {mutation} WHERE id=2")
    if immutable:
        kwargs.update(archive_target_path=tmp_path / "next_active.db",
                      immutable_archive_shards=[{"absolute_path": str(cold), "id": "unit-sealed"}])
    result = _execute_archive_export(**kwargs)
    assert result["status"] == "failed", result
    with closing(sqlite3.connect(hot)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM screening_decisions").fetchone()[0] == 6
        assert payloads.resolve_metric_texts(conn, [(2, None)]) == [RAW]


def test_tracker_bulk_writer_and_reader_keep_fund_identity_and_bounded_selects(tmp_path, monkeypatch):
    from database.connection import DatabaseManager
    from database.models import Base
    from bond_fund_screening.core import screening_tracker

    manager = DatabaseManager(db_path=str(tmp_path / "orm.db"))
    Base.metadata.create_all(manager.engine)
    monkeypatch.setattr(screening_tracker, "get_db_manager", lambda: manager)
    tracker = screening_tracker.ScreeningTracker(1)
    tracker.run_id = "unit-bulk-run"
    rows = [{"run_id": tracker.run_id, "fcode": f"unit-{index}", "step_id": 1, "step_name": "unit",
             "passed": index % 2 == 0, "metrics_json": RAW if index % 3 else None}
            for index in range(1503)]
    assert tracker._save_decisions(rows) == 1503
    with manager.get_session(readonly=True) as session:
        conn = session.connection().connection.driver_connection
        statements = []
        conn.set_trace_callback(statements.append)
        stored = conn.execute("SELECT id,metrics_json FROM screening_decisions ORDER BY id").fetchall()
        assert payloads.resolve_metric_texts(conn, stored) == [row["metrics_json"] for row in rows]
        assert sum(sql.startswith("SELECT") for sql in statements) < 15
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.PAYLOADS}").fetchone()[0] == 1
    actual = screening_tracker.query_fund_decisions("unit-1", tracker.run_id)
    assert len(actual) == 1 and actual[0]["metrics"] == json.loads(RAW) and actual[0]["passed"] is False
    manager.engine.dispose()


def test_canonical_archive_parquet_and_restore_have_self_contained_original_values(tmp_path):
    hot, cold = tmp_path / "hot.db", tmp_path / "cold.db"
    with closing(make_legacy(hot)) as conn:
        add_archive_tables(conn)
        original = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
        conn.commit()
        with conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
    result = _execute_archive_export(db_path=hot, archive_target_path=cold, run_ids=["run-a", "run-b"], chunk_size=100)
    assert result["status"] == "archive-exported", result
    with closing(sqlite3.connect(cold)) as conn:
        assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == original
        assert not payloads.storage_available(conn)
        counts = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
    stat = cold.stat()
    shard = {"state": "immutable", "sha256": hashlib.sha256(cold.read_bytes()).hexdigest(),
             "file_size_bytes": stat.st_size, "file_mtime_ns": stat.st_mtime_ns, "table_row_counts": counts}
    directory = tmp_path / "parquet"
    export_shard(cold, directory, shard, batch_rows=2)
    from utils.screening_archive_access import verify_parquet_hot_values

    binding = {**shard, "format": "parquet", "role": "immutable", "absolute_path": str(directory / "manifest.json"),
               "manifest_sha256": hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
               "source_sha256": shard["sha256"]}
    with closing(sqlite3.connect(hot)) as conn:
        assert verify_parquet_hot_values(binding, ["run-a", "run-b"], conn)["run_count"] == 2
    recovered = tmp_path / "restored.db"
    assert restore_run(directory, "run-a", recovered)["quick_check"] == "ok"
    with closing(sqlite3.connect(recovered)) as conn:
        assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == [row for row in original if row[1] == "run-a"]


@pytest.mark.parametrize("batch_rows", [1, 2, 5000])
def test_migration_three_phases_preserve_all_business_rows_and_proof_chain(tmp_path, batch_rows):
    from scripts.migrate_decision_payloads import migrate

    database = tmp_path / "hot.db"
    with closing(make_legacy(database)) as conn:
        original = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
        conn.execute("CREATE TABLE unrelated(value BLOB)")
        conn.execute("INSERT INTO unrelated VALUES(?)", (b"\x00untouched\xff",))
        conn.commit()
    results = []
    for mode in ("share", "compact", "restore"):
        checkpoint = tmp_path / f"{mode}.json"
        result = migrate(database, checkpoint, mode=mode, batch_rows=batch_rows)
        results.append(result)
        assert result["status"] == "complete" and result["selected_rows"] == 6
        checkpoint_bytes = checkpoint.read_bytes()
        assert migrate(database, checkpoint, mode=mode, batch_rows=batch_rows, resume=True) == result
        assert checkpoint.read_bytes() == checkpoint_bytes
        with closing(sqlite3.connect(database)) as conn:
            assert full_values(conn) == original
            assert conn.execute("SELECT value FROM unrelated").fetchall() == [(b"\x00untouched\xff",)]
            if mode == "share":
                assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == original
            if mode == "restore":
                assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS}").fetchone()[0] == 0
                assert conn.execute(f"SELECT COUNT(*) FROM {payloads.PAYLOADS}").fetchone()[0] == 0
                assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == original
    assert len({result["business_value_chain_sha256"] for result in results}) == 1
    assert results[0]["references_inserted"] == results[1]["inline_payloads_retired"] == results[2]["references_removed"] == 5
    assert results[2]["unreferenced_objects_collected"] == 4


def test_migration_does_not_skip_zero_negative_or_minimum_sqlite_rowids(tmp_path):
    from scripts.migrate_decision_payloads import migrate

    database = tmp_path / "hot.db"
    with closing(make_legacy(database)) as conn, conn:
        conn.execute("UPDATE screening_decisions SET id=? WHERE id=1", (-(2**63),))
        conn.execute("UPDATE screening_decisions SET id=0 WHERE id=2")
        conn.execute("UPDATE screening_decisions SET id=-1 WHERE id=4")
        original = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
    for mode in ("compact", "restore"):
        result = migrate(database, tmp_path / f"{mode}.json", mode=mode, batch_rows=2)
        assert result["selected_rows"] == 6 and result["minimum_id"] == -(2**63)
    with closing(sqlite3.connect(database)) as conn:
        assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == original


def pause_after_first_page(monkeypatch):
    from scripts import migrate_decision_payloads as migration

    ticks = iter([0.0, 0.1, 2.0])
    monkeypatch.setattr(migration.time, "monotonic", lambda: next(ticks, 2.1))


def test_migration_budget_is_resumable_and_fixed_cutoff_leaves_later_rows_inline(tmp_path, monkeypatch):
    from scripts import migrate_decision_payloads as migration

    database, checkpoint = tmp_path / "hot.db", tmp_path / "progress.json"
    with closing(make_legacy(database)):
        pass
    with monkeypatch.context() as local:
        pause_after_first_page(local)
        first = migration.migrate(database, checkpoint, mode="compact", batch_rows=2, through_id=5, max_seconds=1)
    assert first["status"] == "partial_budget_exhausted" and first["last_id"] == 2
    result = migration.migrate(database, checkpoint, mode="compact", batch_rows=2, resume=True)
    assert result["status"] == "complete" and result["selected_rows"] == 4 and result["through_id"] == 5
    with closing(sqlite3.connect(database)) as conn:
        assert metrics(conn) == [(1, None), (2, None), (4, None), (5, None), (9, "null"), (12, "0")]
        assert payloads.resolve_metric_texts(conn, metrics(conn)) == [RAW, RAW, None, "", "null", "0"]


def test_committed_page_interrupted_before_checkpoint_replays_without_losing_values(tmp_path, monkeypatch):
    from scripts import migrate_decision_payloads as migration

    database, checkpoint = tmp_path / "hot.db", tmp_path / "progress.json"
    with closing(make_legacy(database)) as conn:
        original = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
    real_save = migration.save

    def interrupt(path, result):
        if result["completed_pages"] == 1:
            raise KeyboardInterrupt("injected after commit, before durable checkpoint")
        real_save(path, result)

    with monkeypatch.context() as local:
        local.setattr(migration, "save", interrupt)
        with pytest.raises(KeyboardInterrupt):
            migration.migrate(database, checkpoint, mode="compact", batch_rows=2)
    assert json.loads(checkpoint.read_text())["last_id"] is None
    with closing(sqlite3.connect(database)) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS}").fetchone()[0] == 2
        assert full_values(conn) == original
    result = migration.migrate(database, checkpoint, mode="compact", batch_rows=2, resume=True)
    assert result["status"] == "complete" and result["selected_rows"] == 6
    with closing(sqlite3.connect(database)) as conn:
        assert full_values(conn) == original
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS}").fetchone()[0] == 5


@pytest.mark.parametrize("damage", ["schema", "path", "mode", "batch", "columns", "counter", "range", "digest", "status", "missing_field"])
def test_rejected_checkpoint_remains_byte_identical_and_cannot_mutate_database(tmp_path, monkeypatch, damage):
    from scripts import migrate_decision_payloads as migration

    database, checkpoint = tmp_path / "hot.db", tmp_path / "progress.json"
    with closing(make_legacy(database)):
        pass
    with monkeypatch.context() as local:
        pause_after_first_page(local)
        candidate = migration.migrate(database, checkpoint, mode="compact", batch_rows=2, max_seconds=1)
    if damage == "schema":
        candidate["schema_version"] = 999
    elif damage == "path":
        candidate["database"]["path"] = str(tmp_path / "different.db")
    elif damage == "mode":
        candidate["mode"] = "restore"
    elif damage == "batch":
        candidate["batch_rows"] = 3
    elif damage == "columns":
        candidate["columns"] = ["wrong"]
    elif damage == "counter":
        candidate["selected_rows"] = -1
    elif damage == "range":
        candidate["last_id"] = 999
    elif damage == "digest":
        candidate["business_value_chain_sha256"] = "bad"
    elif damage == "status":
        candidate["status"] = "silently_successful"
    else:
        candidate.pop("through_id")
    checkpoint.write_text(json.dumps(candidate))
    before_checkpoint, before_database = checkpoint.read_bytes(), database.read_bytes()
    with pytest.raises(ValueError, match="migration_checkpoint"):
        migration.migrate(database, checkpoint, mode="compact", batch_rows=2, resume=True)
    assert checkpoint.read_bytes() == before_checkpoint
    assert database.read_bytes() == before_database


def test_resume_detects_changed_last_page_without_rewriting_its_evidence(tmp_path, monkeypatch):
    from scripts import migrate_decision_payloads as migration

    database, checkpoint = tmp_path / "hot.db", tmp_path / "progress.json"
    with closing(make_legacy(database)):
        pass
    with monkeypatch.context() as local:
        pause_after_first_page(local)
        migration.migrate(database, checkpoint, mode="compact", batch_rows=2, max_seconds=1)
    with closing(sqlite3.connect(database)) as conn, conn:
        conn.execute("UPDATE screening_decisions SET reason_detail='external change' WHERE id=1")
    before = checkpoint.read_bytes()
    with pytest.raises(ValueError, match="last_page_changed"):
        migration.migrate(database, checkpoint, mode="compact", batch_rows=2, resume=True)
    assert checkpoint.read_bytes() == before


def test_full_row_proof_rolls_back_unrelated_column_damage_and_can_resume(tmp_path):
    from scripts.migrate_decision_payloads import migrate

    database, checkpoint = tmp_path / "hot.db", tmp_path / "progress.json"
    with closing(make_legacy(database)) as conn, conn:
        original = conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall()
        conn.execute("CREATE TRIGGER damage_reason AFTER UPDATE OF metrics_json ON screening_decisions "
                     "BEGIN UPDATE screening_decisions SET reason_detail='unexpected mutation' WHERE id=NEW.id; END")
    with pytest.raises(ValueError, match="full_business_row_changed"):
        migrate(database, checkpoint, mode="compact", batch_rows=2)
    assert json.loads(checkpoint.read_text())["status"] == "failed"
    with closing(sqlite3.connect(database)) as conn, conn:
        assert conn.execute("SELECT * FROM screening_decisions ORDER BY id").fetchall() == original
        conn.execute("DROP TRIGGER damage_reason")
    assert migrate(database, checkpoint, mode="compact", batch_rows=2, resume=True)["status"] == "complete"


def test_helper_rereads_actual_inline_after_retirement_before_claiming_success(tmp_path):
    with closing(make_legacy(tmp_path / "hot.db")) as conn:
        original = metrics(conn)
        conn.execute("CREATE TRIGGER keep_inline AFTER UPDATE OF metrics_json ON screening_decisions "
                     "WHEN NEW.metrics_json IS NULL AND OLD.metrics_json IS NOT NULL "
                     "BEGIN UPDATE screening_decisions SET metrics_json=OLD.metrics_json WHERE id=NEW.id; END")
        conn.commit()
        with pytest.raises(payloads.DecisionPayloadError, match="stored_decision"), conn:
            payloads.ensure_storage(conn)
            payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
        assert metrics(conn) == original


def test_reverse_migration_gc_is_bounded_complete_and_resumable_after_interruption(tmp_path, monkeypatch):
    from scripts import migrate_decision_payloads as migration

    database, checkpoint = tmp_path / "hot.db", tmp_path / "restore.json"
    with closing(make_legacy(database)) as conn, conn:
        payloads.ensure_storage(conn)
        payloads.share_existing_rows(conn, metrics(conn), remove_inline=True)
        payloads.intern_payloads(conn, [f"unreferenced-unit-payload-{index}" for index in range(5200)])
    real_save = migration.save

    def interrupt(path, result):
        if result["unreferenced_objects_collected"]:
            assert result["status"] != "complete"
            raise KeyboardInterrupt("injected during garbage collection")
        real_save(path, result)

    with monkeypatch.context() as local:
        local.setattr(migration, "save", interrupt)
        with pytest.raises(KeyboardInterrupt):
            migration.migrate(database, checkpoint, mode="restore")
    assert json.loads(checkpoint.read_text())["status"] != "complete"
    with closing(sqlite3.connect(database)) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {payloads.PAYLOADS}").fetchone()[0] == 204
    result = migration.migrate(database, checkpoint, mode="restore", resume=True)
    assert result["status"] == "complete" and result["unreferenced_objects_remaining"] == 0


def test_empty_database_completes_without_fabricating_a_cutoff(tmp_path):
    from scripts.migrate_decision_payloads import migrate

    database, checkpoint = tmp_path / "empty.db", tmp_path / "empty.json"
    with closing(make_legacy(database)) as conn, conn:
        conn.execute("DELETE FROM screening_decisions")
    result = migrate(database, checkpoint, mode="compact")
    assert result["status"] == "complete" and result["selected_rows"] == 0
    assert result["minimum_id"] is result["through_id"] is result["last_id"] is None
    assert migrate(database, checkpoint, mode="compact", resume=True) == result
    with pytest.raises(ValueError, match="cutoff"):
        migrate(database, tmp_path / "invalid.json", mode="share", through_id=1)


@pytest.mark.parametrize("cutoff", [-10, 0, 13])
def test_out_of_range_cutoff_does_not_create_checkpoint_or_change_database(tmp_path, cutoff):
    from scripts.migrate_decision_payloads import migrate

    database, checkpoint = tmp_path / "hot.db", tmp_path / "invalid.json"
    with closing(make_legacy(database)):
        pass
    before = database.read_bytes()
    with pytest.raises(ValueError, match="cutoff"):
        migrate(database, checkpoint, mode="compact", through_id=cutoff)
    assert not checkpoint.exists() and database.read_bytes() == before


@pytest.mark.parametrize("alias", ["database", "hardlink", "wal", "symlink"])
def test_checkpoint_cannot_overwrite_database_or_its_sidecar(tmp_path, alias):
    from scripts.migrate_decision_payloads import migrate

    database = tmp_path / "hot.db"
    with closing(make_legacy(database)):
        pass
    checkpoint = tmp_path / "checkpoint.json"
    if alias == "database":
        checkpoint = database
    elif alias == "hardlink":
        checkpoint.hardlink_to(database)
    elif alias == "wal":
        checkpoint = database.with_name(database.name + "-wal")
    else:
        checkpoint.symlink_to(database)
    before = database.read_bytes()
    with pytest.raises(ValueError, match="migration_checkpoint"):
        migrate(database, checkpoint, mode="compact")
    assert database.read_bytes() == before


def test_main_does_not_release_a_workflow_lock_it_failed_to_acquire(tmp_path, monkeypatch):
    import threading
    from types import SimpleNamespace
    from scripts import migrate_decision_payloads as migration

    database = tmp_path / "hot.db"
    with closing(make_legacy(database)):
        pass
    releases = []

    class BusyStore:
        _lock = threading.RLock()

        def _acquire_run_lock_locked(self, run_id):
            raise RuntimeError("workflow already running")

        def _release_run_lock_locked(self):
            releases.append(True)

    monkeypatch.setattr(migration, "load_settings", lambda: SimpleNamespace(database_path=database, state_file=tmp_path / "state.json"))
    monkeypatch.setattr(migration, "assert_scheduler_idle", lambda settings: None)
    monkeypatch.setattr(migration, "WorkflowStore", lambda path: BusyStore())
    monkeypatch.setattr(migration.sys, "argv", ["migration", "--checkpoint", str(tmp_path / "progress.json"), "--mode", "compact"])
    with pytest.raises(RuntimeError, match="workflow already running"):
        migration.main()
    assert releases == []
