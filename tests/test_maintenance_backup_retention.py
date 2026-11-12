"""Recovery/retention failures against isolated SQLite fixtures, never fund data."""
from contextlib import closing
from dataclasses import fields
from datetime import datetime
from functools import partial
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import pytest

from backend import maintenance_backup_retention as retention
from backend.maintenance_backup_recovery import RecoveryAudit, verify_screening_recovery
from backend.storage import WorkflowStore
from utils import task_history_cleanup_execution as cleanup
from utils.screening_archive_access import verify_sqlite_hot_values
from utils.sqlite_backup_snapshot import create_verified_sqlite_backup


NOW = datetime.fromisoformat("2026-10-10T12:00:00+08:00")


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    data = tmp_path / "backend/data"
    data.mkdir(parents=True)
    reports = tmp_path / "reports"
    reports.mkdir()
    source = data / "bond_funds.db"
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("CREATE TABLE task_history(id INTEGER PRIMARY KEY,task_name TEXT NOT NULL,status TEXT NOT NULL,"
                     "started_at TEXT NOT NULL,completed_at TEXT,duration INTEGER,records_processed INTEGER,"
                     "records_inserted INTEGER,records_updated INTEGER,records_failed INTEGER,error_message TEXT,"
                     "error_traceback TEXT,extra_data TEXT)")
        conn.execute("INSERT INTO task_history VALUES(999,'fixture','success','2026-10-09','2026-10-09',1,0,0,0,0,NULL,'',NULL)")
    config = tmp_path / "config.yaml"
    config.write_text("fixture: true\n")
    for name in ["latest.json", "publication_registry.json"]:
        (reports / name).write_text('{"fixture":"no_business_publication"}')
    monkeypatch.setattr(cleanup, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(cleanup, "_project_relative", partial(cleanup._project_relative, project_root=tmp_path))
    monkeypatch.setattr(cleanup, "_resolve_project_path", partial(cleanup._resolve_project_path, project_root=tmp_path))
    # The test worker has all Docker capabilities dropped. Its parent's fd/0
    # is not stat-able there; exercise real inode checks on our own descriptors.
    # Production scans all visible processes and still fails closed on EACCES.
    monkeypatch.setattr(retention, "_process_file_descriptors", lambda: (Path("/proc") / str(os.getpid()) / "fd").iterdir())
    return SimpleNamespace(project_root=tmp_path, database_path=source, reports_dir=reports,
        state_file=data / "workflow_state.json", config_path=config,
        latest_report_file=reports / "latest.json", publication_registry_file=reports / "publication_registry.json",
        scheduler_weekly_maintenance_backup_retention_keep_per_purpose=1,
        scheduler_weekly_maintenance_backup_retention_min_age_days=14,
        scheduler_weekly_maintenance_backup_retention_max_delete_files=2,
        scheduler_weekly_screening_history_archive_execution_archive_target_path="backend/data/archives/active.db")


def backup(runtime, label, date, purpose="task_history_cleanup"):
    destination = runtime.database_path.parent / "backups" / f"fixture_{label}.db"
    result = create_verified_sqlite_backup(runtime.database_path, destination, purpose=purpose, backup_label=f"fixture_{label}")
    path = result["artifact_path"]
    manifest = Path(result["verification_manifest_path"])
    metadata = json.loads(manifest.read_text())
    # Fixture-only age control; database/compressed bytes and their proofs stay
    # those produced by the actual backup creator above.
    metadata["snapshot"]["read_transaction_observed_at"] = date
    manifest.write_text(json.dumps(metadata))
    return path


def task_point(runtime, number=1, *, raw=None, date="2026-08-01T12:00:00+08:00"):
    with closing(sqlite3.connect(runtime.database_path)) as conn, conn:
        conn.execute("INSERT INTO task_history VALUES(?,'fixture','success','2026-01-01','',1,0,0,0,NULL,'',NULL,?)", (number, raw))
    point = backup(runtime, str(number), date)
    plan = cleanup.build_task_history_cleanup_plan_payload(db_path=runtime.database_path,
        generated_at=NOW.isoformat(), retention_days=14, recent_keep_per_task_status=1)
    artifact, _, payload = cleanup.execute_task_history_cleanup(db_path=runtime.database_path,
        plan_payload=plan, backup_file=point, output_dir=runtime.reports_dir / "audit/task_history_cleanup",
        archive_dir=runtime.reports_dir / ".audit/task_history_cleanup", mode="apply",
        confirm_apply=cleanup.REQUIRED_APPLY_CONFIRMATION, timestamp=f"fixture_{number}")
    assert payload["status"] == "success"
    return point, artifact, payload


def run(runtime, mode="apply"):
    return retention.run_maintenance_backup_retention(runtime, mode=mode, now=NOW)


def file_result(result, path):
    return next(item for item in result["files"] if item["path"] == str(path))


@pytest.mark.parametrize("raw", [None, "", '{"missing":null,"zero":0}'])
def test_only_old_successful_point_is_retired_and_original_values_survive(runtime, raw):
    old, _, execution = task_point(runtime, raw=raw)
    newer = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    archive = runtime.project_root / execution["archive"]["rows_archive_path"]
    original_archive = archive.read_bytes()
    preview = run(runtime, "dry-run")
    assert preview["candidate_file_count"] == 1 and preview["deleted_file_count"] == 0
    assert old.exists() and newer.exists()
    result = run(runtime)
    assert result["status"] == "applied" and result["deleted_file_count"] == 1
    assert result["deleted_allocated_bytes"] == file_result(result, old)["identity"]["allocated_bytes"]
    assert not old.exists() and newer.is_file() and archive.read_bytes() == original_archive
    assert json.loads(original_archive)["row"]["extra_data"] == raw
    assert old.with_suffix(old.suffix + ".manifest.json").is_file()


@pytest.mark.parametrize("fault", ["missing_execution", "failed_cleanup", "old_proof", "archive_missing", "archive_tamper", "all_values_changed_with_updated_file_sha", "newer_corrupt", "newer_same_length_corrupt", "manifest_source", "manifest_original_sha"])
def test_incomplete_or_changed_recovery_never_retires_a_point(runtime, fault):
    old, artifact, execution = task_point(runtime)
    newer = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    rows = runtime.project_root / execution["archive"]["rows_archive_path"]
    if fault == "missing_execution":
        artifact.unlink()
    elif fault == "failed_cleanup":
        execution["status"] = "failed"
    elif fault == "old_proof":
        execution["archive"].pop("full_value_verification")
    elif fault == "archive_missing":
        rows.unlink()
    elif fault == "archive_tamper":
        rows.write_text('{}\n')
    elif fault == "all_values_changed_with_updated_file_sha":
        item = json.loads(rows.read_text())
        item["row"]["extra_data"] = ""
        item["row_sha256"] = cleanup._raw_row_sha256(item["row"])
        item.update(extra_data_bytes=0, extra_data_sha256=hashlib.sha256(b"").hexdigest())
        rows.write_text(json.dumps(item) + "\n")
        execution["archive"]["rows_archive_sha256"] = hashlib.sha256(rows.read_bytes()).hexdigest()
    elif fault == "newer_corrupt":
        with newer.open("ab") as stream:
            stream.write(b"fixture damage")
    elif fault == "newer_same_length_corrupt":
        data = bytearray(newer.read_bytes())
        data[-1] ^= 1
        newer.write_bytes(data)
    else:
        manifest = old.with_suffix(old.suffix + ".manifest.json")
        payload = json.loads(manifest.read_text())
        if fault == "manifest_source":
            payload["snapshot"]["source_database_path"] = str(runtime.project_root / "wrong.db")
        else:
            payload["source_sha256"] = "0" * 64
        manifest.write_text(json.dumps(payload))
    if fault not in {"missing_execution", "manifest_source", "manifest_original_sha"}:
        artifact.write_text(json.dumps(execution))
    result = run(runtime)
    assert result["deleted_file_count"] == 0 and old.exists() and newer.exists()
    assert file_result(result, old)["status"] == "retained"
    expected_reasons = {
        "missing_execution": "unique_completed_cleanup_execution_required",
        "failed_cleanup": "task_complete_recovery_proof_required",
        "old_proof": "task_complete_recovery_proof_required",
        "archive_missing": "No such file or directory",
        "archive_tamper": "recovery_file_sha_or_identity_mismatch",
        "all_values_changed_with_updated_file_sha": "task_recovery_values_changed_since_cleanup",
        "newer_corrupt": "latest_backup_for_purpose",
        "newer_same_length_corrupt": "recovery_file_sha_or_identity_mismatch",
        "manifest_source": "backup_complete_snapshot_manifest_required",
        "manifest_original_sha": "backup_complete_snapshot_manifest_required",
    }
    item = file_result(result, old)
    assert expected_reasons[fault] in item.get("error", item["reason"])
    assert result["status"] == "degraded" and result["verification_blocked_count"] >= 1


def test_latest_age_limit_and_unknown_purposes_are_independently_protected(runtime):
    old, _, _ = task_point(runtime, 1)
    second, _, _ = task_point(runtime, 2, date="2026-08-02T12:00:00+08:00")
    recent, _, _ = task_point(runtime, 3, date="2026-10-08T12:00:00+08:00")
    latest = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    unique = backup(runtime, "sidecar", "2026-01-01T12:00:00+08:00", purpose="task_history_extra_data_sidecar")
    legacy = latest.parent / "legacy.db.zst"
    legacy.write_bytes(b"unverified fixture")
    runtime.scheduler_weekly_maintenance_backup_retention_max_delete_files = 1
    result = run(runtime)
    assert not old.exists()
    assert all(path.is_file() for path in [second, recent, latest, unique, legacy])
    assert file_result(result, recent)["reason"] == "within_minimum_retention_age"
    assert file_result(result, second)["reason"] == "per_run_retirement_limit"
    assert file_result(result, latest)["reason"] == "latest_backup_for_purpose"


@pytest.mark.parametrize("lock_kind", ["workflow", "creation", "compression", "open_file"])
def test_conflicting_work_or_open_restore_preserves_backups(runtime, lock_kind):
    old, _, _ = task_point(runtime)
    newer = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    store = WorkflowStore(runtime.state_file)
    if lock_kind == "workflow":
        with store._lock:
            store._acquire_run_lock_locked("fixture_busy")
        try:
            result = run(runtime)
        finally:
            with store._lock:
                store._release_run_lock_locked()
        assert result["status"] == "deferred"
    elif lock_kind in {"creation", "compression"}:
        with (old.parent / f".backup_{lock_kind}.lock").open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = run(runtime)
        assert result["status"] == "deferred"
    else:
        with old.open("rb"):
            result = run(runtime)
        assert file_result(result, old)["error"].endswith("backup_is_open")
    assert old.exists() and newer.exists() and result["deleted_file_count"] == 0


def test_inaccessible_process_descriptors_do_not_allow_retirement(runtime, monkeypatch):
    old, _, _ = task_point(runtime)
    backup(runtime, "newer", "2026-10-09T12:00:00+08:00")

    def denied():
        raise PermissionError("fixture inaccessible descriptor")

    monkeypatch.setattr(retention, "_process_file_descriptors", denied)
    result = run(runtime)
    assert old.is_file() and result["deleted_file_count"] == 0
    assert "PermissionError" in file_result(result, old)["error"]


@pytest.mark.parametrize("late_change", ["archive", "replacement", "publication", "active_reference"])
def test_last_moment_changes_are_checked_before_unlink(runtime, monkeypatch, late_change):
    old, _, execution = task_point(runtime)
    newer = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")

    scanned = []

    def change_after_fd_scan(path):
        scanned.append(path)
        if late_change == "archive":
            (runtime.project_root / execution["archive"]["rows_archive_path"]).write_text('{}\n')
        elif late_change == "replacement":
            with newer.open("ab") as stream:
                stream.write(b"changed fixture")
        elif late_change == "publication":
            runtime.publication_registry_file.write_text('{"fixture":"changed"}')
        else:
            (runtime.database_path.parent / "recovery_state.json").write_text(json.dumps({"restore_input": str(old)}))

    monkeypatch.setattr(retention, "_assert_not_open", change_after_fd_scan)
    result = run(runtime)
    assert old.is_file() and newer.is_file() and result["deleted_file_count"] == 0
    assert scanned == [old]
    error = file_result(result, old)["error"]
    assert ("active_reference_changed_before_unlink" if late_change == "active_reference"
            else "recovery_files_changed_before_retirement") in error


def test_interrupted_retirement_keeps_a_durable_non_success_receipt(runtime, monkeypatch):
    old, _, _ = task_point(runtime)
    backup(runtime, "newer", "2026-10-09T12:00:00+08:00")

    def interrupt(*args):
        raise KeyboardInterrupt("fixture stop before unlink")

    monkeypatch.setattr(retention, "_retire_file", interrupt)
    with pytest.raises(KeyboardInterrupt):
        run(runtime)
    receipt = json.loads(next((runtime.reports_dir / "audit/database_backup_retention").glob("*.json")).read_text())
    assert old.exists() and receipt["status"] == "failed" and receipt["deleted_file_count"] == 0


@pytest.mark.parametrize("failure", ["fsync", "archive_after_unlink", "interrupt_after_unlink"])
def test_failure_after_unlink_records_the_actual_deletion_and_stops(runtime, monkeypatch, failure):
    old, _, execution = task_point(runtime, 1)
    second, _, _ = task_point(runtime, 2, date="2026-08-02T12:00:00+08:00")
    newest = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    original = retention._retire_file

    def fail_after_unlink(*args):
        original(*args)
        if failure == "archive_after_unlink":
            (runtime.project_root / execution["archive"]["rows_archive_path"]).write_text('{}\n')
        elif failure == "interrupt_after_unlink":
            raise KeyboardInterrupt("fixture interruption after unlink")

    def fail_fsync(path):
        raise OSError("fixture directory fsync failure")

    monkeypatch.setattr(retention, "_retire_file", fail_after_unlink)
    if failure == "fsync":
        monkeypatch.setattr(retention, "_fsync_directory", fail_fsync)
    if failure == "interrupt_after_unlink":
        with pytest.raises(KeyboardInterrupt):
            run(runtime)
    else:
        assert run(runtime)["status"] == "failed"
    receipt = json.loads(next((runtime.reports_dir / "audit/database_backup_retention").glob("*.json")).read_text())
    assert not old.exists() and second.is_file() and newest.is_file()
    assert receipt["status"] == "failed" and receipt["deleted_file_count"] == 1
    assert file_result(receipt, old)["status"] == "deleted"
    assert receipt["deleted_allocated_bytes"] == file_result(receipt, old)["identity"]["allocated_bytes"]


@pytest.mark.parametrize("change_link", [False, True])
def test_published_symlink_chain_is_supported_and_bound(runtime, monkeypatch, change_link):
    old, _, _ = task_point(runtime)
    backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    generation = runtime.reports_dir / "generation"
    generation.mkdir()
    report = generation / "report.json"
    runtime.latest_report_file.rename(report)
    current = runtime.reports_dir / ".publication_current"
    current.symlink_to(generation.name, target_is_directory=True)
    runtime.latest_report_file.symlink_to(".publication_current/report.json")
    if change_link:
        next_generation = runtime.reports_dir / "generation_next"
        next_generation.mkdir()
        (next_generation / "report.json").write_bytes(report.read_bytes())

        def switch_after_scan(path):
            current.unlink()
            current.symlink_to(next_generation.name, target_is_directory=True)

        monkeypatch.setattr(retention, "_assert_not_open", switch_after_scan)
    result = run(runtime)
    if change_link:
        assert old.is_file() and result["deleted_file_count"] == 0
        assert "publication_control_link_changed" in file_result(result, old)["error"]
    else:
        assert result["status"] == "applied" and not old.exists()
        assert len(file_result(result, old)["verified_control_links"]) == 2


def test_compacted_tasks_require_the_bound_full_payload_to_still_match(runtime):
    sidecar = runtime.reports_dir / "fixture_sidecar.json"
    data = {"source": "isolated test", "value": None}
    encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    sidecar.write_text(json.dumps({"task_history_id": 1, "full_payload": data}))
    raw = json.dumps({"task_history_extra_data_compacted": True, "task_history_extra_data_schema_version": 1,
        "task_history_full_payload": {"available": True, "sidecar_project_path": str(sidecar),
            "full_payload_bytes": len(encoded), "full_payload_sha256": hashlib.sha256(encoded).hexdigest()}})
    old, _, _ = task_point(runtime, raw=raw)
    backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    preview = run(runtime, "dry-run")
    assert file_result(preview, old)["recovery"]["verified_full_payload_sidecars"] == 1
    sidecar.write_text(json.dumps({"task_history_id": 1, "full_payload": {"source": "isolated test", "value": 0}}))
    result = run(runtime)
    assert old.exists() and result["deleted_file_count"] == 0


@pytest.mark.parametrize("fault", [None, "changed_original", "changed_final_field", "sealed_audit"])
def test_legacy_scheduler_payload_requires_the_original_byte_hash(runtime, fault):
    sidecar = runtime.reports_dir / "legacy_task_audit.json"
    original = {"status": "success", "raw_text": "", "missing": None, "zero": 0}
    post_audit = {"task_run_audit": {"artifact_path": str(sidecar)}, "task_run_audit_path": str(sidecar),
                  "workflow_run_id": "isolated_fixture", "publication_result": {"status": "fixture_only"}, "status_detail": None}
    full = json.dumps({**original, **post_audit}, ensure_ascii=False, separators=(",", ":")).encode()
    audit = {"task": {"task_history_id": 1}, "task_result": dict(original)}
    compact = {**post_audit, "task_history_extra_data_compacted": True, "task_history_extra_data_schema_version": 1,
        "task_history_full_payload": {"available": True, "sidecar_project_path": str(sidecar),
            "source": "scheduler_task_run_audit", "full_payload_bytes": len(full),
            "full_payload_sha256": hashlib.sha256(full).hexdigest()}}
    if fault == "changed_original":
        audit["task_result"]["missing"] = 0
    elif fault == "changed_final_field":
        compact["workflow_run_id"] = "different_fixture"
    elif fault == "sealed_audit":
        audit["persistence_stage"] = "completion_recorded"
    sidecar.write_text(json.dumps(audit))
    old, _, _ = task_point(runtime, raw=json.dumps(compact))
    backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    result = run(runtime)
    if fault:
        assert old.exists() and result["deleted_file_count"] == 0
        assert "task_recovery_sidecar_value_mismatch" in file_result(result, old)["error"]
    else:
        assert not old.exists() and result["deleted_file_count"] == 1
        proof = file_result(result, old)["recovery"]["full_payload_proofs"][0]
        assert proof["sha256"] == hashlib.sha256(full).hexdigest()
        assert proof["method"] == "legacy_scheduler_fields_with_exact_original_sha"


def test_symlinks_hardlinks_and_uncompressed_peers_never_enter_retirement(runtime):
    old, _, _ = task_point(runtime)
    newer = backup(runtime, "newer", "2026-10-09T12:00:00+08:00")
    linked = old.with_name("linked.db.zst")
    os.link(old, linked)
    result = run(runtime)
    assert old.exists() and linked.exists() and result["deleted_file_count"] == 0
    linked.unlink()
    raw_peer = old.with_suffix("")
    raw_peer.write_bytes(b"restore in progress fixture")
    assert run(runtime)["deleted_file_count"] == 0
    raw_peer.unlink()
    target = runtime.project_root / "keep.zst"
    old.rename(target)
    old.symlink_to(target)
    assert run(runtime)["deleted_file_count"] == 0 and target.exists() and newer.exists()


@pytest.mark.parametrize("fault", [None, "value", "missing_run", "legacy_proof"])
def test_screening_recovery_checks_all_seven_typed_tables(runtime, fault):
    from backend.screening_archive_reader import TABLES

    archive = runtime.project_root / runtime.scheduler_weekly_screening_history_archive_execution_archive_target_path
    archive.parent.mkdir()
    with closing(sqlite3.connect(archive)) as conn, conn:
        for table in TABLES:
            conn.execute(f"CREATE TABLE {table}(id INTEGER,run_id TEXT,value REAL,notes TEXT)")
            conn.execute(f"INSERT INTO {table} VALUES(1,'fixture-run',NULL,'')")
        proof = verify_sqlite_hot_values(conn, conn, ["fixture-run"])
    manifest = archive.parent / "screening_history_archive_manifest.json"
    manifest.write_text(json.dumps({"schema_version": 1, "generation": 1,
        "active_shard": {"id": "active", "path": archive.name, "format": "sqlite", "state": "writable"}, "immutable_shards": []}))
    execution = {"mode": "apply", "status": "applied", "summary": {"failed_batch_count": 0},
        "post_apply_consistency": {"status": "ok"},
        "batches": [{"status": "applied", "run_ids": ["fixture-run"], "archive_rows_reverified_in_write_transaction": 7}],
        "archive_guard": {"status": "ok", "blockers": [], "archive_shard_checks": [{"hot_value_verification": proof}]}}
    if fault == "value":
        with closing(sqlite3.connect(archive)) as conn, conn:
            conn.execute("UPDATE screening_picks SET value=0")
    elif fault == "missing_run":
        with closing(sqlite3.connect(archive)) as conn, conn:
            conn.execute("DELETE FROM screening_runs")
    elif fault == "legacy_proof":
        execution["batches"][0].pop("archive_rows_reverified_in_write_transaction")
    if fault:
        with pytest.raises(ValueError):
            verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), archive)
    else:
        assert verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), archive)["row_count"] == 7


def test_outside_root_recovery_references_are_rejected(runtime):
    with pytest.raises(ValueError, match="outside_project"):
        RecoveryAudit(runtime.project_root).path("../other_project/private.db")
    with pytest.raises(ValueError, match="outside_project"):
        RecoveryAudit(runtime.project_root).path("/other_project/private.db")


@pytest.mark.parametrize("damage", [False, True])
@pytest.mark.parametrize("active_state", ["with_schema", "rotated_empty", "unexpected_schema", "immutable_empty"])
def test_parquet_recovery_keeps_the_same_complete_value_rule(runtime, damage, active_state):
    from backend.screening_archive_reader import TABLES
    from scripts.export_screening_parquet import export_shard

    active = runtime.project_root / runtime.scheduler_weekly_screening_history_archive_execution_archive_target_path
    active.parent.mkdir()
    sealed = active.parent / "sealed.db"
    for path in [active, sealed]:
        with closing(sqlite3.connect(path)) as conn, conn:
            if path == active and active_state != "with_schema":
                if active_state == "unexpected_schema":
                    conn.execute("CREATE TABLE unexpected(value TEXT)")
                else:
                    conn.execute("VACUUM")
                continue
            conn.execute("CREATE TABLE screening_runs(id INTEGER,run_id TEXT,started_at TEXT,fund_type TEXT)")
            if path == sealed:
                conn.execute("INSERT INTO screening_runs VALUES(1,'fixture-run','2026-08-01','bond')")
            for table in TABLES[1:]:
                conn.execute(f"CREATE TABLE {table}(id INTEGER,run_id TEXT,value REAL,notes TEXT)")
                if path == sealed:
                    conn.execute(f"INSERT INTO {table} VALUES(1,'fixture-run',NULL,'')")
    with closing(sqlite3.connect(sealed)) as conn:
        proof = verify_sqlite_hot_values(conn, conn, ["fixture-run"])
    source_sha = hashlib.sha256(sealed.read_bytes()).hexdigest()
    original = {"state": "immutable", "sha256": source_sha, "file_size_bytes": sealed.stat().st_size,
                "file_mtime_ns": sealed.stat().st_mtime_ns, "table_row_counts": {table: 1 for table in TABLES}}
    destination = active.parent / "columnar"
    export_shard(sealed, destination, original, batch_rows=2)
    manifest = active.parent / "screening_history_archive_manifest.json"
    manifest_payload = {"schema_version": 1, "generation": 2,
        "active_shard": {"id": "active", "path": active.name, "format": "sqlite", "state": "writable"},
        "immutable_shards": [{"id": "sealed", "path": "columnar/manifest.json", "format": "parquet", "state": "immutable",
            "source_sha256": source_sha, "table_row_counts": {table: 1 for table in TABLES},
            "manifest_sha256": hashlib.sha256((destination / "manifest.json").read_bytes()).hexdigest()}]}
    if active_state == "immutable_empty":
        empty = active.parent / "empty_sealed.db"
        active.rename(empty)
        with closing(sqlite3.connect(active)) as conn:
            conn.execute("VACUUM")
        manifest_payload["immutable_shards"].append({
            "id": "empty", "path": empty.name, "format": "sqlite", "state": "immutable"})
    manifest.write_text(json.dumps(manifest_payload))
    execution = {"mode": "apply", "status": "applied", "summary": {"failed_batch_count": 0},
        "post_apply_consistency": {"status": "ok"},
        "batches": [{"status": "applied", "run_ids": ["fixture-run"], "archive_rows_reverified_in_write_transaction": 7}],
        "archive_guard": {"status": "ok", "blockers": [], "archive_shard_checks": [{"hot_value_verification": proof}]}}
    if damage:
        path = next(destination.rglob("*.parquet"))
        with path.open("ab") as stream:
            stream.write(b"fixture corruption")
        with pytest.raises(ValueError):
            verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), active)
    elif active_state in {"unexpected_schema", "immutable_empty"}:
        with pytest.raises(sqlite3.OperationalError, match="no such table: screening_runs"):
            verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), active)
    else:
        assert verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), active)["row_count"] == 7
        # An empty active shard never supplies missing historical runs.
        manifest_payload["immutable_shards"] = []
        manifest.write_text(json.dumps(manifest_payload))
        with pytest.raises(ValueError, match="screening_current_recovery_scope_missing"):
            verify_screening_recovery(execution, RecoveryAudit(runtime.project_root), active)


@pytest.mark.parametrize("status", ["applied", "degraded", "failed", "deferred"])
def test_scheduler_retention_follows_task_cleanup_and_preserves_its_outcome(runtime, monkeypatch, status):
    from backend import scheduler as scheduling
    from backend.config import Settings
    from backend.scheduler_diagnostics import summarize_scheduler_run_for_api
    from backend.task_history_extra_data_sidecar import summarize_task_result_for_history
    import scripts.cleanup_disk_usage as disk_cleanup

    for field in fields(Settings):
        if field.name.startswith("scheduler_weekly_") and field.name.endswith("_enabled"):
            setattr(runtime, field.name, False)
    runtime.scheduler_weekly_task_history_cleanup_execution_enabled = True
    runtime.scheduler_weekly_task_history_cleanup_execution_auto_apply_enabled = True
    runtime.scheduler_weekly_task_history_cleanup_execution_min_apply_candidate_total = 1
    runtime.scheduler_weekly_maintenance_backup_retention_enabled = True
    runtime.scheduler_weekly_nav_history_limit = 0
    runtime.scheduler_weekly_long_term_history_limit = 0
    runtime.archived_runs_dir = runtime.reports_dir / "archive"
    scheduler = scheduling.ProjectScheduler.__new__(scheduling.ProjectScheduler)
    scheduler.settings = runtime
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None
    scheduler._attempt_refresh_risk_free_rate = lambda *args, **kwargs: {"succeeded": True}
    scheduler._build_risk_free_rate_drift_audit = lambda *args, **kwargs: {"alert_triggered": False}
    stages = []

    def cleanup_stage(**kwargs):
        stages.append("tasks_" + kwargs["mode"])
        return runtime.reports_dir / "execution.json", runtime.reports_dir / "execution.md", {
            "status": "success" if kwargs["mode"] == "apply" else "dry-run",
            "summary": {"candidate_total": 1, "deleted_count": int(kwargs["mode"] == "apply")},
        }

    def retention_stage(*args, **kwargs):
        assert stages == ["tasks_dry-run", "tasks_apply"]
        assert kwargs["mode"] == "apply"
        stages.append("retention")
        return {"status": status, "mode": "apply", "artifact_path": "reports/audit/fixture_retention.json",
                "deleted_file_count": 1 if status == "applied" else 0, "deleted_allocated_bytes": 4096 if status == "applied" else 0,
                "verification_blocked_count": int(status == "degraded"), "files": [{"fixture_heavy_proof": [None, "", 0]}]}

    monkeypatch.setattr(scheduling, "execute_task_history_cleanup", cleanup_stage)
    monkeypatch.setattr(retention, "run_maintenance_backup_retention", retention_stage)
    monkeypatch.setattr(disk_cleanup, "run_cleanup", lambda **kwargs: SimpleNamespace(to_dict=lambda: {}))
    result = scheduler._execute_weekly_maintenance(collection_enabled=False, bond_universe_sync_enabled=False)
    assert stages == ["tasks_dry-run", "tasks_apply", "retention"]
    summary = result["maintenance_backup_retention"]
    assert summary["status"] == status and "files" not in summary
    degraded = status in {"failed", "degraded"}
    assert result["degraded"] is degraded
    if degraded:
        assert result["status"] == "degraded" and result["degraded_stage"] == "maintenance-backup-retention"
        assert result["maintenance_backup_retention_error"] == "maintenance_backup_recovery_verification_failed"
    api = summarize_scheduler_run_for_api(result, project_root=runtime.project_root)
    task = summarize_task_result_for_history(result)
    for target in (api, task):
        assert target["maintenance_backup_retention"] == summary
        assert target.get("maintenance_backup_retention_error") == result["maintenance_backup_retention_error"]
