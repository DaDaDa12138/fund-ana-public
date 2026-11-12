"""Exercise production archive/recovery callers with isolated Parquet shards."""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

from database import ScreeningDecision, ScreeningPick, ScreeningRun, get_db_manager, init_database
from scripts.audit_screening_archive_reader import audit_copy, verify_copy
from scripts.export_screening_parquet import export_shard
from utils.screening_archive_access import probe_parquet_shard
from utils.screening_history_archive_execution import execute_screening_history_archive
from utils.screening_history_archive_manifest import load_screening_archive_manifest
from utils.screening_history_archive_preview import build_screening_history_archive_preview_payload
from utils.screening_history_archive_rotation import build_screening_history_archive_rotation_payload
from utils.screening_history_cleanup_execution import _build_archive_apply_guard, _delete_batch


def archive_run(root, hot, active, run_ids, timestamp):
    preview = build_screening_history_archive_preview_payload(
        db_path=hot, preview_payload={"db_path": str(hot), "summary": {"candidate_run_count": len(run_ids)},
            "execution_plan": {"scope_run_ids": run_ids}},
        preflight_payload={"checks": {"ready_for_manual_signoff": True}},
        archive_target_path=active, project_root=root, output_dir=root / "audit",
    )
    return execute_screening_history_archive(
        db_path=hot, preview_payload=preview, archive_target_path=active, project_root=root,
        output_dir=root / "audit", mode="archive-export", timestamp=timestamp,
    )


def apply_guard(archive, execution, run_ids):
    root, hot, active, _, _ = archive
    return _build_archive_apply_guard(
        selected_run_ids=run_ids, archive_execution_payload=execution,
        source_archive_execution_artifact="isolated_execution.json", require_archive_guard=True,
        project_root=root, preflight_payload={"db_path": str(hot)}, db_path=hot,
        archive_target_path=active,
    )


@pytest.fixture
def archive(tmp_path):
    hot = tmp_path / "hot.db"
    active = tmp_path / "backend/data/archives/screening_history_details_archive.db"
    assert init_database(str(hot), drop_existing=True)
    manager = get_db_manager(str(hot))
    with manager.get_session(task_name="seed_parquet_recovery_fixture") as session:
        for index, run_id in enumerate(["run-a", "run-b", "run-active"], start=1):
            session.add(ScreeningRun(run_id=run_id, started_at=datetime(2026, 5 - index if index < 3 else 6, 1),
                                     status="success", fund_type="bond", period_years=5,
                                     fund_count_in=2, fund_count_pass=1))
            session.add_all([
                ScreeningDecision(run_id=run_id, fcode="000001", step_id=1, passed=False,
                                  reason_code=None, metrics_json=None),
                ScreeningDecision(run_id=run_id, fcode="000002", step_id=1, passed=True,
                                  reason_code="", metrics_json='{"真实零":0.0}'),
                ScreeningPick(run_id=run_id, fcode="000002", rank=1, fund_type="bond", period_years=5),
            ])
    assert archive_run(tmp_path, hot, active, ["run-a", "run-b"], "first")[2]["status"] == "archive-exported"
    result = build_screening_history_archive_rotation_payload(
        active_archive_path=active, project_root=tmp_path, mode="rotate", max_active_size_bytes=1,
        min_active_run_count=1, confirm_rotate=True, compute_sha256=True, timestamp="fixture_rotation",
    )
    assert result["status"] == "rotated"
    manifest_path = active.parent / "screening_history_archive_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    original = manifest["immutable_shards"][0]
    directory = active.parent / "analytics/fixture"
    export_shard(active.parent / original["path"], directory, original, batch_rows=2)
    signature = hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
    manifest["immutable_shards"][0] = {**original, "format": "parquet",
        "path": str((directory / "manifest.json").relative_to(active.parent)),
        "manifest_sha256": signature, "source_sha256": original["sha256"],
        "sqlite_reference": deepcopy(original)}
    manifest["generation"] += 1
    manifest_path.write_text(json.dumps(manifest))
    binding = {"shard_id": original["id"], "directory": str(directory.relative_to(tmp_path)),
               "manifest_sha256": signature, "source_sha256": original["sha256"]}
    return tmp_path, hot, active, directory, binding


def test_archive_reuses_parquet_and_sums_active_shard_before_hot_deletion(archive):
    root, hot, active, _, _ = archive
    runs = ["run-a", "run-b", "run-active"]
    _, _, execution = archive_run(root, hot, active, runs, "mixed")
    assert execution["status"] == "archive-exported", execution["summary"]
    assert execution["summary"]["scope_run_count_reused_from_immutable"] == 2
    assert execution["summary"]["scope_run_count_written_to_active"] == 1
    assert all(item["exact_archive_match"] for item in execution["table_results"])
    proof = execution["archive_shard_checks"][0]
    assert proof["format"] == "parquet" and proof["quick_check"] is None
    assert proof["hot_value_verification"]["run_count"] == 2
    guard = apply_guard(archive, execution, runs)
    assert guard["status"] == "ok", guard
    assert len(guard["table_checks"]) == 7
    with closing(sqlite3.connect(hot)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM screening_runs").fetchone()[0] == 3
    _, _, repeated = archive_run(root, hot, active, runs, "repeated")
    assert repeated["status"] == "archive-exported"
    assert repeated["summary"]["inserted_row_count"] == 0


@pytest.mark.parametrize("damage", ["missing", "same_size", "manifest_sha", "source_sha", "counts", "active_parquet"])
def test_missing_or_changed_recovery_data_blocks_export_and_hot_delete(archive, damage):
    root, hot, active, directory, _ = archive
    _, _, execution = archive_run(root, hot, active, ["run-a"], "before_damage")
    assert execution["status"] == "archive-exported"
    if damage in {"missing", "same_size"}:
        path = next(directory.rglob("*.parquet"))
        if damage == "missing":
            path.unlink()
        else:
            value = bytearray(path.read_bytes())
            value[8] ^= 1
            path.write_bytes(value)
    else:
        path = active.parent / "screening_history_archive_manifest.json"
        manifest = json.loads(path.read_text())
        entry = manifest["immutable_shards"][0]
        if damage == "manifest_sha":
            entry.pop("manifest_sha256")
        elif damage == "source_sha":
            entry["source_sha256"] = "0" * 64
        elif damage == "counts":
            entry["table_row_counts"]["screening_decisions"] += 1
        else:
            manifest["active_shard"]["format"] = "parquet"
        path.write_text(json.dumps(manifest))
    guard = apply_guard(archive, execution, ["run-a"])
    assert guard["status"] == "blocked" and guard["blocker_count"] > 0
    assert archive_run(root, hot, active, ["run-a"], "after_damage")[2]["status"] == "failed"
    with closing(sqlite3.connect(hot)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM screening_runs").fetchone()[0] == 3


@pytest.mark.parametrize("change", ["values", "row_count", "schema"])
def test_hot_revision_cannot_be_discarded_even_with_valid_parquet(archive, change):
    root, hot, active, _, _ = archive
    _, _, execution = archive_run(root, hot, active, ["run-a"], "before_revision")
    assert execution["status"] == "archive-exported"
    with closing(sqlite3.connect(hot)) as conn:
        if change == "values":
            conn.execute("UPDATE screening_decisions SET metrics_json='0' WHERE run_id='run-a' AND metrics_json IS NULL")
        elif change == "schema":
            conn.execute("ALTER TABLE screening_decisions ADD COLUMN new_history TEXT")
        else:
            conn.execute("DELETE FROM screening_decisions WHERE run_id='run-a' AND metrics_json IS NULL")
        conn.commit()
    guard = apply_guard(archive, execution, ["run-a"])
    assert guard["status"] == "blocked"
    assert any(row["code"] == "parquet_archive_recovery_verification_failed" for row in guard["blockers"])
    assert archive_run(root, hot, active, ["run-a"], "after_revision")[2]["status"] == "failed"


@pytest.mark.parametrize("side,statement", [
    ("active", "UPDATE screening_decisions SET metrics_json='0' WHERE run_id='run-active' AND fcode='000001'"),
    ("active", "UPDATE screening_decisions SET passed=0 WHERE run_id='run-active' AND fcode='000002'"),
    ("hot", "UPDATE screening_decisions SET fcode='000003' WHERE run_id='run-active' AND fcode='000002'"),
    ("active", "UPDATE screening_runs SET fund_type='hybrid' WHERE run_id='run-active'"),
    ("active", "UPDATE screening_picks SET rank=2 WHERE run_id='run-active'"),
    ("active", "UPDATE screening_decisions SET id=id+100 WHERE run_id='run-active'"),
    ("hot", "ALTER TABLE screening_decisions ADD COLUMN new_history TEXT"),
])
def test_sqlite_history_changes_with_equal_counts_block_hot_deletion(archive, side, statement):
    """A completed export and SQLite quick_check cannot replace value comparison."""
    root, hot, active, _, _ = archive
    runs = ["run-active"]
    _, _, execution = archive_run(root, hot, active, runs, "sqlite_before_revision")
    assert execution["status"] == "archive-exported"
    with closing(sqlite3.connect(active if side == "active" else hot)) as conn, conn:
        conn.execute(statement)
    with closing(sqlite3.connect(hot)) as conn:
        before = conn.execute("SELECT id,run_id,fcode,passed FROM screening_decisions ORDER BY id").fetchall()
    guard = apply_guard(archive, execution, runs)
    (root / "guard_after_mutation.json").write_text(json.dumps(guard, ensure_ascii=False, indent=2))
    assert guard["status"] == "blocked", guard
    assert any(row["code"] == "sqlite_archive_recovery_verification_failed" for row in guard["blockers"])
    with closing(sqlite3.connect(hot)) as conn:
        assert conn.execute("SELECT id,run_id,fcode,passed FROM screening_decisions ORDER BY id").fetchall() == before


def hot_history_rows(path):
    from backend.screening_archive_reader import TABLES

    with closing(sqlite3.connect(path)) as conn:
        return {table: conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid').fetchall() for table in TABLES}


@pytest.mark.parametrize("shared", [False, True])
def test_mixed_archive_proof_allows_only_unchanged_batches_under_sqlite_writer_lock(archive, monkeypatch, shared):
    from database import screening_decision_payloads as payloads
    from utils import screening_archive_access as access

    root, hot, active, _, _ = archive
    runs = ["run-a", "run-b", "run-active"]
    if shared:
        with closing(sqlite3.connect(hot)) as conn, conn:
            payloads.ensure_storage(conn)
            rows = conn.execute("SELECT id,metrics_json FROM screening_decisions ORDER BY id").fetchall()
            payloads.share_existing_rows(conn, rows, remove_inline=True)
    _, _, execution = archive_run(root, hot, active, runs, "before_batches")
    assert execution["status"] == "archive-exported"
    guard = apply_guard(archive, execution, runs)
    assert guard["status"] == "ok", guard
    assert all(row["exact_values_match"] and row["row_count_match"] for row in guard["table_checks"])
    verify = access.verify_hot_before_delete
    lock_checks = []

    def verify_with_competing_writer(conn, batch, proofs):
        with closing(sqlite3.connect(hot, timeout=0)) as competing:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competing.execute("UPDATE screening_runs SET status='changed' WHERE run_id=?", (batch[0],))
        lock_checks.append(tuple(batch))
        return verify(conn, batch, proofs)

    monkeypatch.setattr(access, "verify_hot_before_delete", verify_with_competing_writer)
    manager = get_db_manager(str(hot))
    results = [_delete_batch(manager, batch, delete_runs=True, archive_value_checks=guard["archive_shard_checks"])
               for batch in (["run-a"], ["run-b", "run-active"])]
    assert len(lock_checks) == 2
    assert sum(row["archive_rows_reverified_in_write_transaction"] for row in results) == sum(
        row["hot_scope_row_count"] for row in guard["table_checks"])
    assert sum(row["deleted_decisions"] for row in results) == 6
    assert sum(row["deleted_picks"] for row in results) == 3
    assert sum(row["deleted_runs"] for row in results) == 3
    assert all(not rows for rows in hot_history_rows(hot).values())
    if shared:
        with closing(sqlite3.connect(hot)) as conn:
            assert conn.execute(f"SELECT COUNT(*) FROM {payloads.REFS}").fetchone()[0] == 0
            assert conn.execute(f"SELECT COUNT(*) FROM {payloads.PAYLOADS}").fetchone()[0] == 0


@pytest.mark.parametrize("change,error", [
    ("hot_value", "hot_values_changed_before_archive_delete"),
    ("hot_schema", "hot_values_changed_before_archive_delete"),
    ("sqlite_wal", "archive_recovery_files_changed"),
    ("parquet_file", "archive_recovery_files_changed"),
    ("registry", "archive_recovery_files_changed"),
    ("missing_proof", "archive_complete_value_proof_missing"),
    ("duplicate_proof", "archive_value_proof_duplicate_run"),
])
def test_change_after_guard_rolls_back_entire_delete_batch(archive, change, error):
    root, hot, active, directory, _ = archive
    runs = ["run-a", "run-active"]
    _, _, execution = archive_run(root, hot, active, runs, "before_late_change")
    guard = apply_guard(archive, execution, runs)
    assert guard["status"] == "ok", guard
    proofs = deepcopy(guard["archive_shard_checks"])
    with closing(sqlite3.connect(active)) as writer:
        if change == "hot_value":
            with closing(sqlite3.connect(hot)) as conn, conn:
                conn.execute("UPDATE screening_decisions SET metrics_json='0' WHERE run_id='run-active' AND metrics_json IS NULL")
        elif change == "hot_schema":
            with closing(sqlite3.connect(hot)) as conn, conn:
                conn.execute("ALTER TABLE screening_decisions ADD COLUMN new_history TEXT")
        elif change == "sqlite_wal":
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("UPDATE screening_runs SET status='changed' WHERE run_id='run-active'")
            writer.commit()
            assert Path(f"{active}-wal").stat().st_size > 0
        elif change == "parquet_file":
            next(directory.rglob("*.parquet")).touch()
        elif change == "registry":
            (active.parent / "screening_history_archive_manifest.json").touch()
        elif change == "missing_proof":
            proofs = []
        else:
            proofs.append(deepcopy(proofs[0]))
        before = hot_history_rows(hot)
        with pytest.raises(ValueError, match=error):
            _delete_batch(get_db_manager(str(hot)), runs, delete_runs=True, archive_value_checks=proofs)
        assert hot_history_rows(hot) == before


def test_sqlite_recovery_identity_is_checked_after_value_verification(archive, monkeypatch):
    from utils import screening_archive_access as access

    root, hot, active, _, _ = archive
    _, _, execution = archive_run(root, hot, active, ["run-active"], "changed_during_verify")
    verify = access.verify_sqlite_hot_values

    def verify_then_change(*args):
        proof = verify(*args)
        active.touch()
        return proof

    monkeypatch.setattr(access, "verify_sqlite_hot_values", verify_then_change)
    guard = apply_guard(archive, execution, ["run-active"])
    assert guard["status"] == "blocked"
    assert any(row["code"] == "sqlite_archive_recovery_verification_failed" for row in guard["blockers"])


def test_recovery_change_during_delete_rolls_back_before_commit(archive, monkeypatch):
    from utils import screening_history_cleanup_execution as cleanup

    root, hot, active, _, _ = archive
    runs = ["run-active"]
    _, _, execution = archive_run(root, hot, active, runs, "changed_before_commit")
    guard = apply_guard(archive, execution, runs)
    assert guard["status"] == "ok", guard
    mark_stats = cleanup.mark_storage_stats_after_detail_cleanup

    def mark_then_change(*args, **kwargs):
        result = mark_stats(*args, **kwargs)
        active.touch()
        return result

    monkeypatch.setattr(cleanup, "mark_storage_stats_after_detail_cleanup", mark_then_change)
    before = hot_history_rows(hot)
    with pytest.raises(ValueError, match="archive_recovery_files_changed"):
        _delete_batch(get_db_manager(str(hot)), runs, delete_runs=True, archive_value_checks=guard["archive_shard_checks"])
    assert hot_history_rows(hot) == before


@pytest.mark.parametrize("duplicate", ["active", "immutable"])
def test_duplicate_runs_across_formats_block_reuse_and_delete(archive, duplicate):
    root, hot, active, _, _ = archive
    _, _, execution = archive_run(root, hot, active, ["run-a"], "before_duplicate")
    assert execution["status"] == "archive-exported"
    manifest_path = active.parent / "screening_history_archive_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    original = manifest["immutable_shards"][0]["sqlite_reference"]
    if duplicate == "immutable":
        manifest["immutable_shards"].append({**original, "id": "duplicate-sqlite"})
        manifest_path.write_text(json.dumps(manifest))
    else:
        with closing(sqlite3.connect(active)) as conn:
            conn.execute("ATTACH DATABASE ? AS old", (str(active.parent / original["path"]),))
            for table in original["table_row_counts"]:
                conn.execute(f'INSERT INTO "{table}" SELECT * FROM old."{table}" WHERE run_id=?', ("run-a",))
            conn.commit()
    assert apply_guard(archive, execution, ["run-a"])["status"] == "blocked"
    assert archive_run(root, hot, active, ["run-a"], "duplicated")[2]["status"] == "failed"


def test_archive_execution_hash_binding_is_required_for_delete(archive):
    root, hot, active, _, _ = archive
    _, _, execution = archive_run(root, hot, active, ["run-a"], "binding")
    entry = next(item for item in execution["archive_manifest"]["shards"] if item["format"] == "parquet")
    entry["manifest_sha256"] = "0" * 64
    guard = apply_guard(archive, execution, ["run-a"])
    assert guard["status"] == "blocked"
    assert any("binding_mismatch" in row.get("detail", "") for row in guard["blockers"])


def test_absent_run_is_zero_but_missing_declared_partition_is_an_error(archive):
    root, _, active, directory, _ = archive
    state = load_screening_archive_manifest(active, project_root=root)
    shard = next(row for row in state["shards"] if row["format"] == "parquet")
    result = probe_parquet_shard(shard, ["missing-run"])
    assert result["present_run_ids"] == [] and set(result["table_counts"].values()) == {0}
    next(directory.rglob("*.parquet")).unlink()
    with pytest.raises(FileNotFoundError):
        probe_parquet_shard(shard, ["missing-run"])


def test_integrity_observer_survives_sqlite_retirement_without_claiming_double_read(archive):
    root, _, active, _, binding = archive
    comparison = audit_copy(root, binding, repeats=1)
    assert comparison["status"] == "matched" and len(comparison["comparisons"]) == 14
    manifest = json.loads((active.parent / "screening_history_archive_manifest.json").read_text())
    (active.parent / manifest["immutable_shards"][0]["sqlite_reference"]["path"]).unlink()
    result = verify_copy(root, binding, repeats=1)
    assert result["status"] == "matched" and result["sqlite_reference_read"] is False
    assert len(result["comparisons"]) == 14
    assert result["full_shard_integrity"]["verified_file_count"] > 0
    assert all(row["reference"] == "sealed_manifest_row_group_and_run_hashes" for row in result["comparisons"])


def test_hybrid_observer_discovers_new_production_parquet_shards(archive):
    from backend.screening_archive_shadow import archive_observation_plan

    root, _, _, _, binding = archive
    plan = archive_observation_plan(root, {'production_reader': 'hybrid', 'shadow_copies': []})
    assert plan == {'mode': 'read_only_integrity', 'copies': [binding], 'copy_modes': ['read_only_integrity']}
    changed = {**binding, 'manifest_sha256': '0' * 64}
    with pytest.raises(ValueError, match='binding_mismatch'):
        archive_observation_plan(root, {'production_reader': 'hybrid', 'shadow_copies': [changed]})


def test_archive_and_recovery_checks_close_database_descriptors_without_gc(archive):
    import gc
    import os

    root, hot, active, _, _ = archive

    def open_databases():
        paths = []
        for path in Path('/proc/self/fd').iterdir():
            try:
                target = os.readlink(path)
            except OSError:
                continue
            if str(root) in target and '.db' in target:
                paths.append(target)
        return sorted(paths)

    gc.collect()
    before = open_databases()
    enabled = gc.isenabled()
    gc.disable()
    try:
        for index in range(3):
            _, _, execution = archive_run(root, hot, active, ['run-a'], f'fd_{index}')
            assert execution['status'] == 'archive-exported'
            assert apply_guard(archive, execution, ['run-a'])['status'] == 'ok'
            assert open_databases() == before
    finally:
        if enabled:
            gc.enable()
