"""Archived task logs must preserve history, error semantics and every reader."""
from datetime import datetime, timedelta
import gzip
import hashlib
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace

from flask import Flask
import pytest

from backend import legacy_task_audit_compression as delivery
from backend import task_audit_storage as storage
from backend import task_log_compression as migration
from backend.api_routes.reports import register_reports_routes
from backend.replay_evidence import content_hash
from backend.scheduler import ProjectScheduler
from backend.scheduler_diagnostics import _read_text_head_tail, _read_text_tail
from scripts.audit_data_truthfulness import _file_evidence, _read_complete_collection_log
from scripts.audit_runtime_logs import collect_recent_task_log_sources, tail_file
from scripts.cleanup_disk_usage import run_cleanup


def log_fixture(root, *, task_id=7, status="failed", compressed_audit=False):
    began = (datetime.now() - timedelta(days=31, minutes=20)).replace(microsecond=0)
    if task_id != 7:
        began -= timedelta(seconds=task_id)
    ended = began + timedelta(minutes=5)
    task_name = "scheduled_daily_bond_refresh"
    path = root / "logs/tasks" / f"task_{task_id}_{task_name}.log"
    audit = root / "reports/.audit/scheduler_task_runs" / began.strftime("%Y%m%d") / (
        began.strftime("%Y%m%d_%H%M%S") + "_" + task_name + ".json")
    if compressed_audit:
        audit = Path(f"{audit}.gz")
    path.parent.mkdir(parents=True, exist_ok=True)
    audit.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "task": {"task_history_id": task_id, "task_name": task_name, "task_key": "daily_refresh",
                 "started_at": began.isoformat(), "finished_at": ended.isoformat(), "status": status},
        "task_result": {"status": status, "finished_at": ended.isoformat(), "records_processed": None,
                        "records_inserted": 0, "error": "original synthetic failure" if status == "failed" else None},
        "workflow_summary": {"synthetic_test_only": "archival provenance, not fund business data\n" * 2000},
    }
    audit_raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()
    audit.write_bytes(gzip.compress(audit_raw, mtime=0) if compressed_audit else audit_raw)
    prefix = began.strftime("%Y-%m-%d %H:%M:%S")
    original = (
        f"{prefix} [task-start] task_id={task_id} task_name={task_name} started_at={began.isoformat()}\n"
        f"{prefix} subprocess_index=/app/reports/runs/test/logs/subprocess/index.json\n"
        + ('raw_test_evidence={"missing":null,"observed_zero":0}\n' * 7000)
        + ("ERROR original synthetic failure remains recorded\n" if status == "failed" else "")
        + f"{ended:%Y-%m-%d %H:%M:%S} [task-finish] status={status} finished_at={ended.isoformat()} "
          f"task_run_audit_path=/app/{audit.relative_to(root).as_posix()}\n"
    ).encode()
    path.write_bytes(original)
    os.chmod(path, 0o640)
    stamp = int((ended + timedelta(seconds=1)).timestamp() * 1_000_000_000)
    os.utime(path, ns=(stamp, stamp))
    os.utime(audit, ns=(stamp, stamp))
    return path, original, audit, payload


def compress(root):
    plan = migration.plan_task_log_compression(root)
    assert plan["candidate_count"] == 1 and plan["removed_count"] == 0
    outcome = migration.compress_task_log(root, plan["candidates"][0])
    assert outcome["status"] == "compressed", outcome
    return outcome


@pytest.mark.parametrize("status", sorted(storage.TASK_LOG_TERMINAL_STATUSES))
@pytest.mark.parametrize("compressed_audit", [False, True])
def test_original_values_readers_runtime_diagnostics_and_independent_restore(tmp_path, status, compressed_audit):
    path, original, audit, payload = log_fixture(tmp_path, status=status, compressed_audit=compressed_audit)
    audit_before = audit.read_bytes()
    settings = SimpleNamespace(project_root=tmp_path)
    scheduler = object.__new__(ProjectScheduler)
    scheduler.settings = settings
    index = tmp_path / "reports/runs/test/logs/subprocess/index.json"
    index.parent.mkdir(parents=True, exist_ok=True)
    index.write_text("{}")
    assert scheduler._subprocess_index_path_from_task_log(str(path)) == index
    evidence = _file_evidence(settings, path)
    metadata = storage.task_log_metadata(path)
    sources = collect_recent_task_log_sources(tmp_path / "logs", project_root=tmp_path, max_files=2, max_lines=20)
    tails = (_read_text_tail(path, max_bytes=120), _read_text_head_tail(path, max_bytes=120),
             ProjectScheduler._read_scheduler_text_tail(path, max_bytes=120))
    outcome = compress(tmp_path)
    assert not path.exists() and storage.artifact_exists(path)
    assert storage.read_original_artifact_bytes(path) == original
    assert scheduler._subprocess_index_path_from_task_log(str(path)) == index
    assert _read_complete_collection_log(path).encode() == original
    assert _file_evidence(settings, path) == evidence and storage.task_log_metadata(path) == metadata
    assert content_hash(path) == hashlib.sha256(original).hexdigest()
    assert tails == (_read_text_tail(path, max_bytes=120), _read_text_head_tail(path, max_bytes=120),
                     ProjectScheduler._read_scheduler_text_tail(path, max_bytes=120))
    assert collect_recent_task_log_sources(tmp_path / "logs", project_root=tmp_path, max_files=2, max_lines=20) == sources
    assert audit.read_bytes() == audit_before and storage.read_task_audit(audit) == payload
    assert payload["task_result"]["records_processed"] is None and payload["task_result"]["records_inserted"] == 0
    target = tmp_path / "restore/original.log"
    restored = migration.restore_task_log(path, target)
    assert restored["full_restore_verified"] and target.read_bytes() == original
    assert storage.task_log_metadata(target) == metadata
    assert target.stat().st_mode & 0o777 == 0o640
    assert outcome["independent_restore_verified"] and outcome["net_reclaimed_disk_bytes"] is None
    assert outcome["estimated_net_file_allocation_reduction_bytes"] > 0
    with pytest.raises(FileExistsError):
        migration.restore_task_log(path, target)


def test_log_binding_also_reads_already_migrated_task_audit(tmp_path):
    path, original, audit, _ = log_fixture(tmp_path)
    plan = delivery.plan_legacy_task_audit_compression(tmp_path)
    assert plan["candidate_count"] == 1
    assert delivery.compress_legacy_task_audit(tmp_path, plan["candidates"][0])["status"] == "compressed"
    assert not audit.exists()
    compress(tmp_path)
    assert storage.read_original_artifact_bytes(path) == original


def test_old_url_full_download_conditional_get_and_source_metadata(tmp_path):
    from backend import app as app_module

    path, original, _, _ = log_fixture(tmp_path)
    tokens = app_module._path_cache_token(path), app_module._file_content_cache_token(path)
    app = Flask(__name__)
    callbacks = {name: lambda *a, **kw: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    register_reports_routes(app, **callbacks)
    url = "/api/files/" + path.relative_to(tmp_path).as_posix()
    with app.test_client() as client:
        before = client.get(url + "?download=1")
        compress(tmp_path)
        response = client.get(url + "?download=1")
        assert response.status_code == before.status_code == 200
        assert response.data == before.data == original
        for header in ("Content-Type", "Content-Length", "Content-Disposition", "Last-Modified"):
            assert response.headers[header] == before.headers[header]
        assert response.headers["X-Fund-ANA-Original-SHA256"] == hashlib.sha256(original).hexdigest()
        assert response.headers["X-Fund-ANA-Storage-Codec"] == "gzip"
        assert client.get(url, headers={"If-None-Match": response.headers["ETag"]}).status_code == 304
        assert tokens == (app_module._path_cache_token(path), app_module._file_content_cache_token(path))
        storage.migration_paths(path)[0].write_bytes(b"damaged")
        assert client.get(url).status_code == 409
        assert app_module._path_cache_token(path) == app_module._file_content_cache_token(path) == "missing"


@pytest.mark.parametrize("damage", ["gzip", "original_sha", "identity", "status", "other_terminal_status",
                                    "terminal_time", "audit_path", "mtime", "missing_receipt", "duplicate_key"])
def test_corrupt_log_or_receipt_does_not_become_valid_history(tmp_path, damage):
    path, _, _, _ = log_fixture(tmp_path)
    compress(tmp_path)
    stored, receipt_path = storage.migration_paths(path)
    receipt = json.loads(receipt_path.read_text())
    if damage == "gzip":
        stored.write_bytes(stored.read_bytes()[:-3])
    elif damage == "missing_receipt":
        receipt_path.unlink()
    elif damage == "duplicate_key":
        receipt_path.write_text('{"schema_version":1,' + receipt_path.read_text()[1:])
    else:
        if damage == "original_sha":
            receipt["original_sha256"] = "0" * 64
        elif damage == "identity":
            receipt["task_identity"]["task_history_id"] = 8
        elif damage == "status":
            receipt["task_identity"]["status"] = "running"
        elif damage == "other_terminal_status":
            receipt["task_identity"]["status"] = "success"
        elif damage == "terminal_time":
            receipt["task_identity"]["finished_at"] = receipt["task_identity"]["started_at"]
        elif damage == "audit_path":
            receipt["task_identity"]["source_audit_path"] = "reports/.audit/scheduler_task_runs/unrelated.json"
        else:
            receipt["original_identity"]["mtime_ns"] = "unknown"
        receipt_path.write_text(json.dumps(receipt))
    with pytest.raises((OSError, ValueError)):
        storage.read_original_artifact_bytes(path)
    assert not storage.artifact_exists(path)
    assert _read_text_tail(path) == _read_text_head_tail(path) == ""
    with pytest.raises((OSError, ValueError)):
        _read_complete_collection_log(path)


@pytest.mark.parametrize("reason", ["recent", "missing_finish", "running", "wrong_id", "wrong_status",
                                    "wrong_time", "missing_audit", "audit_symlink", "shared_log", "log_symlink"])
def test_missing_or_ambiguous_terminal_evidence_preserves_original(tmp_path, reason):
    path, original, audit, payload = log_fixture(tmp_path)
    if reason == "recent":
        os.utime(path, None)
    elif reason == "missing_finish":
        path.write_bytes(original + b"later unfinished activity\n")
        os.utime(path, (audit.stat().st_mtime,) * 2)
    elif reason in {"running", "wrong_id", "wrong_status", "wrong_time"}:
        if reason == "running":
            payload["task"]["status"] = payload["task_result"]["status"] = "running"
        elif reason == "wrong_id":
            payload["task"]["task_history_id"] = 9
        elif reason == "wrong_status":
            payload["task_result"]["status"] = "success"
        else:
            payload["task"]["finished_at"] = payload["task"]["started_at"]
        audit.write_text(json.dumps(payload))
    elif reason == "missing_audit":
        audit.unlink()
    elif reason == "shared_log":
        os.link(path, tmp_path / "shared.log")
    else:
        source = audit if reason == "audit_symlink" else path
        target = tmp_path / "outside_source"
        source.rename(target)
        source.symlink_to(target)
    before = path.read_bytes()
    plan = migration.plan_task_log_compression(tmp_path)
    assert plan["candidate_count"] == 0 and plan["removed_count"] == 0
    assert path.read_bytes() == before
    assert not storage.migration_paths(path)[0].exists()


def test_plan_is_read_only_and_bounds_files_and_bytes(tmp_path):
    for task_id in (7, 8, 9):
        log_fixture(tmp_path, task_id=task_id)
    plan = migration.plan_task_log_compression(tmp_path, max_files=2)
    assert plan["candidate_count"] == 2 and plan["removed_count"] == 0
    assert all((tmp_path / item["path"]).is_file() for item in plan["candidates"])
    size = plan["candidates"][0]["identity"]["size"]
    assert migration.plan_task_log_compression(tmp_path, max_bytes=size)["candidate_count"] == 1
    for kwargs in ({"max_files": 33}, {"max_bytes": migration.MAX_BATCH_BYTES + 1}, {"keep_days": 0}):
        with pytest.raises(ValueError, match="policy"):
            migration.plan_task_log_compression(tmp_path, **kwargs)


def test_runtime_log_line_boundaries_match_text_file_iteration(tmp_path):
    path, original, audit, _ = log_fixture(tmp_path)
    prefix, finish = original.rsplit(b"\n", 2)[:2]
    path.write_bytes(prefix + b"\nERROR one\vtwo\fthree\xc2\x85four\xe2\x80\xa8five\r\nsix\rseven\n" + finish + b"\n")
    os.utime(path, (audit.stat().st_mtime,) * 2)
    before = tail_file(path, max_lines=20)
    assert any("\v" in line and "\u2028" in line for line in before)
    compress(tmp_path)
    assert tail_file(path, max_lines=20) == before


def test_writer_lock_and_archived_identity_prevent_appending_into_retired_log(tmp_path):
    path, original, _, _ = log_fixture(tmp_path)
    plan = migration.plan_task_log_compression(tmp_path)
    with storage.task_log_io_lock(path.parent):
        result = migration.compress_task_log(tmp_path, plan["candidates"][0])
    assert result["status"] == "retained" and result["reason"] == "task_log_writer_busy"
    assert path.read_bytes() == original
    compress(tmp_path)
    scheduler = object.__new__(ProjectScheduler)
    scheduler._task_log_state = lambda: {"path": path}
    with pytest.raises(RuntimeError, match="already_archived"):
        scheduler._append_task_log_line("must not create a new plain tail")
    assert not path.exists() and storage.read_original_artifact_bytes(path) == original


@pytest.mark.parametrize("stage", ["restore", "receipt", "source_changed"])
def test_partial_delivery_keeps_original_and_can_resume_verified_state(tmp_path, monkeypatch, stage):
    path, original, audit, _ = log_fixture(tmp_path)
    before_audit = audit.read_bytes()
    candidate = migration.plan_task_log_compression(tmp_path)["candidates"][0]
    original_install = delivery._install
    original_restore = delivery._independent_restore

    def restore(*args):
        original_restore(*args)
        if stage == "restore":
            raise OSError("simulated interrupted restore")
        if stage == "source_changed":
            audit.write_text("{}")

    def install(target, *args):
        if stage == "receipt" and target.suffix == ".receipt":
            raise OSError("simulated interrupted receipt delivery")
        return original_install(target, *args)

    monkeypatch.setattr(delivery, "_independent_restore", restore)
    monkeypatch.setattr(delivery, "_install", install)
    result = migration.compress_task_log(tmp_path, candidate)
    assert result["status"] == "failed" and not result["removed"]
    assert path.read_bytes() == original
    monkeypatch.undo()
    audit.write_bytes(before_audit)
    resumed = migration.compress_task_log(tmp_path, candidate)
    assert resumed["status"] == "compressed", resumed
    assert storage.read_original_artifact_bytes(path) == original


def test_weekly_cleanup_integration_and_dated_logs_only_scope(tmp_path):
    path, original, _, _ = log_fixture(tmp_path)
    settings = SimpleNamespace(project_root=tmp_path, core_root=tmp_path / "core",
                               reports_dir=tmp_path / "reports", archived_runs_dir=tmp_path / "reports/archive/superseded_runs")
    preview = run_cleanup(settings, apply=False).to_dict()
    assert preview["task_log_compression"]["candidate_count"] == 1
    assert preview["task_log_compression"]["compressed_count"] == 0 and path.is_file()
    logs_only = run_cleanup(settings, apply=True, logs_only=True).to_dict()
    assert logs_only["task_log_compression"] is None and path.is_file()
    result = run_cleanup(settings, apply=True).to_dict()
    assert not result["errors"]
    assert result["task_log_compression"]["compressed_count"] == 1
    assert result["task_log_compression"]["removed_count"] == 1
    assert result["task_log_compression"]["net_reclaimed_disk_bytes"] is None
    assert storage.read_original_artifact_bytes(path) == original


def test_arbitrary_gzip_fallback_and_small_read_budgets_remain_rejected(tmp_path):
    other = tmp_path / "config.log"
    Path(f"{other}.gz").write_bytes(gzip.compress(b"not a task log"))
    with pytest.raises(FileNotFoundError):
        storage.read_original_artifact_bytes(other)
    path, _, _, _ = log_fixture(tmp_path)
    compress(tmp_path)
    with pytest.raises(ValueError, match="budget"):
        storage.read_original_artifact_bytes(path, max_bytes=100)
    with pytest.raises(ValueError, match="path"):
        storage.read_migrated_task_audit(path)
