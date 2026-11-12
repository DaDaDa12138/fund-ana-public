"""Storage fault injection uses synthetic CSV values, never production funds."""
from datetime import datetime
import hashlib
import inspect
import io
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import time
from types import SimpleNamespace

from flask import Flask
import pandas as pd
import pytest

from backend import core_csv_compression as migration
from backend.api_routes.reports import register_reports_routes
from backend.storage import WorkflowStore
from core.utils import core_csv_storage as storage


STAMP = datetime(2026, 4, 10, 1, 2, 3).timestamp()
STAMP_NS = int(STAMP) * 10**9 + 123456789
NAME = "债券型基金_1年期_达标全量3_20260410_010203.csv"
CONTENT = ("identifier,text,missing_vs_zero,precision\r\n" +
           'T001,"synthetic,quote",,0.12345678901234567\r\n'
           'T002,"two\nlines",0,-0.000000123456789\r\n'
           'T003,"中文 ""quote""",-1.5,1.000000000000001\r\n' * 1400).encode("utf-8-sig")


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / "project"
    output = root / "reports/core_output"
    output.mkdir(parents=True)
    return SimpleNamespace(project_root=root, reports_dir=root / "reports", core_output_dir=output,
                           core_root=root / "core", state_file=root / "state.json",
                           runs_dir=root / "reports/runs", archived_runs_dir=root / "reports/archive",
                           scheduler_weekly_disk_cleanup_core_csv_enabled=True,
                           scheduler_weekly_disk_cleanup_core_csv_keep_days=30)


def write_output(settings, name=NAME, raw=CONTENT):
    path = settings.core_output_dir / name
    path.write_bytes(raw)
    path.chmod(0o640)
    os.utime(path, ns=(STAMP_NS, STAMP_NS))
    stem = path.stem.removesuffix("_simple")
    context = path.with_name(stem + "_stats.json")
    context.write_text(json.dumps({
        "generated_at": "2026-04-10T01:02:03", "run_id": "synthetic-storage-run",
        "status": "failed", "unknown_count": None, "observed_zero": 0,
        "output_files": {"csv": stem + ".csv", "simple_csv": stem + "_simple.csv"},
    }))
    os.utime(context, ns=(STAMP_NS + 10**9, STAMP_NS + 10**9))
    return path, context


def compress(settings):
    result = migration.run_core_csv_compression(settings, apply=True)
    assert result["compressed_count"] == 1 and not result["errors"], result
    return result


def client(settings):
    app = Flask(__name__)
    options = {key: lambda *a, **kw: {} for key in inspect.signature(register_reports_routes).parameters if key != "app"}
    from backend.artifact_paths import resolve_project_artifact_path

    options["resolve_project_file"] = lambda value: resolve_project_artifact_path(value, project_root=settings.project_root)
    register_reports_routes(app, **options)
    return app.test_client()


def test_complete_bytes_metadata_missing_values_and_failed_context_survive(settings):
    from backend import task_audit_storage

    path, context = write_output(settings)
    before = path.stat()
    original_context = context.read_bytes()
    preview = migration.run_core_csv_compression(settings)
    assert preview["candidate_count"] == 1 and preview["compressed_count"] == 0
    assert path.read_bytes() == CONTENT
    result = compress(settings)
    assert not path.exists()
    assert context.read_bytes() == original_context
    assert json.loads(original_context)["status"] == "failed"
    assert result["net_reclaimed_disk_bytes"] is None
    assert result["outcomes"][0]["independent_restore_verified"]
    assert storage.read_original_artifact_bytes(path) == CONTENT
    assert task_audit_storage.read_original_artifact_bytes(path) == CONTENT
    assert task_audit_storage.artifact_exists(path)
    pd.testing.assert_frame_equal(storage.read_csv_artifact(path, dtype=str), pd.read_csv(io.BytesIO(CONTENT), dtype=str))
    info = storage.original_artifact_stat(path)
    assert (info.st_mtime, info.st_mtime_ns, info.st_size, info.st_mode) == (
        before.st_mtime, before.st_mtime_ns, before.st_size, before.st_mode)
    restored = settings.project_root / "restored.csv"
    migration.restore_core_csv(settings, path, restored)
    # The old inode can be reused after unlink. Compare live objects instead.
    assert restored.read_bytes() == CONTENT and restored.stat().st_nlink == 1
    assert all(not restored.samefile(part) for part in storage.csv_storage_paths(path))
    assert restored.stat().st_mtime_ns == STAMP_NS
    assert result["completed_file_allocation_reduction_bytes"] == before.st_blocks * 512 - sum(
        part.stat().st_blocks * 512 for part in storage.csv_storage_paths(path))


@pytest.mark.parametrize("damage", ["recent", "recent_evidence", "missing_evidence", "invalid_evidence",
                                   "wrong_binding", "missing_run", "metadata_before_csv", "hardlink",
                                   "symlink", "extended_attributes", "unknown_writer", "oversize", "tiny"])
def test_unproven_or_active_outputs_are_retained(settings, damage):
    path, context = write_output(settings)
    if damage == "recent":
        os.utime(path, None)
    elif damage == "recent_evidence":
        os.utime(context, None)
    elif damage == "missing_evidence":
        context.unlink()
    elif damage == "invalid_evidence":
        context.write_text("{broken")
    elif damage in {"wrong_binding", "missing_run"}:
        payload = json.loads(context.read_bytes())
        if damage == "wrong_binding":
            payload["output_files"]["csv"] = "other.csv"
        else:
            payload["run_id"] = None
        context.write_text(json.dumps(payload))
        os.utime(context, ns=(STAMP_NS + 10**9,) * 2)
    elif damage == "metadata_before_csv":
        os.utime(context, ns=(STAMP_NS - 1,) * 2)
    elif damage == "hardlink":
        os.link(path, path.with_suffix(".copy"))
    elif damage == "symlink":
        target = path.with_suffix(".target")
        path.rename(target)
        path.symlink_to(target)
    elif damage == "extended_attributes":
        os.setxattr(path, "user.storage-test", b"must preserve")
    elif damage == "unknown_writer":
        new = path.with_name("unknown_20260410_010203.csv")
        path.rename(new)
        path = new
    elif damage == "oversize":
        with path.open("ab") as stream:
            stream.truncate(storage.MAX_CSV_BYTES + 1)
        os.utime(path, ns=(STAMP_NS,) * 2)
    elif damage == "tiny":
        path.write_bytes(b"header\n")
        os.utime(path, ns=(STAMP_NS,) * 2)
    expected = path.read_bytes()
    result = migration.run_core_csv_compression(settings, apply=True)
    assert result["candidate_count"] == result["removed_count"] == 0
    assert path.read_bytes() == expected
    assert not storage.has_stored_csv(path)


def test_busy_workflow_and_low_space_leave_originals(settings, monkeypatch):
    path, _ = write_output(settings)
    store = WorkflowStore(settings.state_file)
    with store._lock:
        store._acquire_run_lock_locked("test-writer")
    try:
        result = migration.run_core_csv_compression(settings, apply=True)
        assert result["status"] == "deferred" and result["reason"] == "workflow_lock_busy"
        assert not result["outcomes"]
    finally:
        with store._lock:
            store._release_run_lock_locked()
    monkeypatch.setattr(migration.shutil, "disk_usage", lambda p: SimpleNamespace(free=0))
    result = migration.run_core_csv_compression(settings, apply=True)
    assert result["removed_count"] == 0
    assert result["outcomes"][0]["reason"] == "core_csv_insufficient_temporary_space"
    assert path.read_bytes() == CONTENT and not storage.has_stored_csv(path)


@pytest.mark.parametrize("point", ["receipt_write", "before_unlink", "restore_corrupt", "source_change", "after_unlink"])
def test_failures_preserve_recoverability_and_report_actual_removal(settings, monkeypatch, point):
    path, context = write_output(settings)
    if point == "receipt_write":
        real = migration._write_new
        calls = []

        def fail(file, raw, **kw):
            calls.append(file)
            if len(calls) == 2:
                raise OSError("injected write failure")
            return real(file, raw, **kw)
        monkeypatch.setattr(migration, "_write_new", fail)
    elif point == "before_unlink":
        real = Path.unlink

        def fail(file, *a, **kw):
            if file == path:
                raise OSError("injected unlink failure")
            return real(file, *a, **kw)
        monkeypatch.setattr(Path, "unlink", fail)
    elif point in {"restore_corrupt", "source_change"}:
        real = migration.materialize_stored_csv

        def fail(file, destination, **kw):
            result = real(file, destination, **kw)
            if point == "restore_corrupt":
                destination.write_bytes(b"corrupt independent restore")
            else:
                context.write_text("{}")
            return result
        monkeypatch.setattr(migration, "materialize_stored_csv", fail)
    else:
        real = migration.sync_directory

        def fail(directory):
            if not path.exists():
                raise OSError("injected post-unlink fsync failure")
            return real(directory)
        monkeypatch.setattr(migration, "sync_directory", fail)
    result = migration.run_core_csv_compression(settings, apply=True)
    assert result["errors"] and result["status"] == "failed"
    assert storage.read_original_artifact_bytes(path) == CONTENT
    assert result["removed_count"] == (1 if point == "after_unlink" else 0)
    assert path.exists() is (point != "after_unlink")
    assert not list(settings.core_output_dir.glob(".core-csv-*.tmp"))


def _crash_before_unlink(settings, path):
    real = Path.unlink

    def crash(file, *args, **kwargs):
        if file == path:
            os._exit(77)
        return real(file, *args, **kwargs)
    Path.unlink = crash
    migration.run_core_csv_compression(settings, apply=True)


def test_process_death_leaves_original_and_complete_pair_then_resumes(settings):
    path, _ = write_output(settings)
    process = multiprocessing.get_context("fork").Process(target=_crash_before_unlink, args=(settings, path))
    process.start()
    process.join(15)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("crash fixture did not finish")
    assert process.exitcode == 77
    assert path.read_bytes() == CONTENT and storage.read_stored_csv(path)[0] == CONTENT
    assert len(storage.glob_artifacts(path.parent, "*.csv")) == 1
    result = compress(settings)
    assert result["removed_count"] == 1 and storage.read_original_artifact_bytes(path) == CONTENT
    # Crash leftovers are never broadly deleted by a subsequent maintenance run.
    assert list(settings.core_output_dir.glob(".core-csv-*.tmp"))


@pytest.mark.parametrize("damage", ["frame", "truncated", "missing_frame", "missing_receipt", "bad_json",
                                   "wrong_sha", "wrong_size", "wrong_name", "wrong_codec", "duplicate_key",
                                   "oversize", "frame_symlink", "receipt_symlink"])
def test_corrupt_storage_is_visible_and_fails_instead_of_becoming_empty(settings, damage):
    path, _ = write_output(settings)
    compress(settings)
    frame, receipt = storage.csv_storage_paths(path)
    if damage == "frame":
        data = frame.read_bytes()
        frame.write_bytes(b"X" + data[1:])
    elif damage == "truncated":
        frame.write_bytes(frame.read_bytes()[:8])
    elif damage == "missing_frame":
        frame.unlink()
    elif damage == "missing_receipt":
        receipt.unlink()
    elif damage == "bad_json":
        receipt.write_text("{bad")
    elif damage.endswith("symlink"):
        original = frame if damage == "frame_symlink" else receipt
        target = original.with_name(original.name + ".target")
        original.rename(target)
        original.symlink_to(target)
    else:
        data = json.loads(receipt.read_bytes())
        if damage == "wrong_sha":
            data["original_sha256"] = "0" * 64
        elif damage == "wrong_size":
            data["original_identity"]["size"] += 1
        elif damage == "wrong_name":
            data["logical_name"] = "other.csv"
        elif damage == "wrong_codec":
            data["codec"] = "gzip"
        elif damage == "oversize":
            data["original_identity"]["size"] = storage.MAX_CSV_BYTES + 1
        encoded = json.dumps(data)
        if damage == "duplicate_key":
            encoded = encoded[:-1] + ',"schema_version":1}'
        receipt.write_text(encoded)
    assert path in storage.glob_artifacts(path.parent, "*.csv")
    with pytest.raises(storage.CoreCsvStorageError):
        storage.read_csv_artifact(path)
    response = client(settings).get("/api/files/" + path.relative_to(settings.project_root).as_posix())
    assert response.status_code == 409


def test_http_contract_etag_range_head_and_conditional_requests_are_unchanged(settings):
    path, _ = write_output(settings)
    browser = client(settings)
    url = "/api/files/" + path.relative_to(settings.project_root).as_posix() + "?download=1"
    response = browser.get(url)
    assert response.status_code == 200 and response.data == CONTENT
    headers = {key: response.headers[key] for key in (
        "Content-Type", "Content-Length", "ETag", "Last-Modified", "Content-Disposition", "Cache-Control")}
    compress(settings)
    response = browser.get(url)
    assert response.status_code == 200 and response.data == CONTENT
    assert {key: response.headers[key] for key in headers} == headers
    assert response.headers["X-Fund-ANA-Storage-Codec"] == "zstd"
    assert browser.get(url, headers={"If-None-Match": headers["ETag"]}).status_code == 304
    assert browser.get(url, headers={"If-Modified-Since": headers["Last-Modified"]}).status_code == 304
    response = browser.get(url, headers={"Range": "bytes=3-100"})
    assert response.status_code == 206 and response.data == CONTENT[3:101]
    response = browser.head(url)
    assert response.status_code == 200 and response.data == b"" and response.headers["Content-Length"] == str(len(CONTENT))


def test_discovery_csv_consumers_and_new_freeze_keep_exact_sources(settings):
    from backend.core_runner import _latest_matching_file
    from backend.report_builder import _materialize_run_local_core_artifacts, _read_tabular_file
    from backend.report_inputs import load_actual_official_df
    from bond_fund_screening.report_catalog import scan_report_catalog_entries
    from scripts.audit_data_truthfulness import _read_csv_header_and_sample_codes

    path, _ = write_output(settings)
    before_frame = _read_tabular_file(path)
    before_catalog = scan_report_catalog_entries(path.parent)
    before_header = _read_csv_header_and_sample_codes(path)
    compress(settings)
    assert _latest_matching_file(path.parent, ["*.csv"]) == path
    assert scan_report_catalog_entries(path.parent) == before_catalog
    assert _read_csv_header_and_sample_codes(path) == before_header
    pd.testing.assert_frame_equal(_read_tabular_file(path), before_frame)
    frozen = _materialize_run_local_core_artifacts(settings=settings, run_dir=settings.runs_dir / "new-run",
        artifacts={"long_csv": str(path.relative_to(settings.project_root)), "core_output_dir": str(path.parent)})
    target = settings.project_root / frozen["long_csv"]
    assert target.is_file() and target.read_bytes() == CONTENT and not storage.has_stored_csv(target)
    assert target.stat().st_mtime_ns == STAMP_NS
    actual = load_actual_official_df({"filtered_csv": str(path)}, project_root=settings.project_root)
    pd.testing.assert_frame_equal(actual, pd.read_csv(io.BytesIO(CONTENT)))


def test_offline_replay_and_recovery_keep_original_sha_without_materializing(settings):
    from backend.maintenance_backup_recovery import RecoveryAudit
    from backend.screening_baseline_replay import file_sha256

    path, _ = write_output(settings)
    expected = hashlib.sha256(CONTENT).hexdigest()
    assert file_sha256(path) == expected
    plain_audit = RecoveryAudit(settings.project_root)
    assert plain_audit.bind(path, expected) == path
    plain_audit.assert_stable()
    compress(settings)
    physical = storage.csv_storage_paths(path)
    before = {part: part.read_bytes() for part in physical}
    audit = RecoveryAudit(settings.project_root)
    docker_path = Path("/app") / path.relative_to(settings.project_root)
    assert audit.bind(docker_path, expected) == path
    assert file_sha256(path) == expected
    assert not path.exists()
    assert {part: part.read_bytes() for part in physical} == before
    assert set(audit.files) == {str(part) for part in physical}
    audit.assert_stable()
    with pytest.raises(ValueError, match="sha_or_identity_mismatch"):
        RecoveryAudit(settings.project_root).bind(path, "0" * 64)

    # A restored ordinary file retains the established reader precedence,
    # while an in-progress audit must reject that representation change.
    path.write_bytes(b"new ordinary representation\n")
    assert file_sha256(path) == hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="migrated_representation_changed"):
        audit.assert_stable()


@pytest.mark.parametrize("damage", ["missing_receipt", "missing_stored", "stored_bytes",
                                   "original_sha", "stored_sha", "logical_name"])
def test_offline_csv_readers_reject_incomplete_or_corrupt_storage(settings, damage):
    from backend.maintenance_backup_recovery import RecoveryAudit
    from backend.screening_baseline_replay import file_sha256

    path, _ = write_output(settings)
    compress(settings)
    stored, receipt = storage.csv_storage_paths(path)
    if damage == "missing_receipt":
        receipt.unlink()
    elif damage == "missing_stored":
        stored.unlink()
    elif damage == "stored_bytes":
        stored.write_bytes(b"!" + stored.read_bytes()[1:])
    else:
        metadata = json.loads(receipt.read_bytes())
        key = {"original_sha": "original_sha256", "stored_sha": "stored_sha256",
               "logical_name": "logical_name"}[damage]
        metadata[key] = "0" * 64 if damage.endswith("sha") else "another.csv"
        receipt.write_text(json.dumps(metadata))
    with pytest.raises((OSError, ValueError)):
        file_sha256(path)
    with pytest.raises((OSError, ValueError)):
        RecoveryAudit(settings.project_root).bind(path, hashlib.sha256(CONTENT).hexdigest())
    assert not path.exists()


@pytest.mark.parametrize("change", ["stored", "receipt", "original", "original_symlink"])
def test_recovery_rechecks_compressed_csv_files_before_retirement(settings, change):
    from backend.maintenance_backup_recovery import RecoveryAudit

    path, _ = write_output(settings)
    compress(settings)
    stored, receipt = storage.csv_storage_paths(path)
    audit = RecoveryAudit(settings.project_root)
    audit.bind(path, hashlib.sha256(CONTENT).hexdigest())
    if change == "stored":
        stored.write_bytes(stored.read_bytes() + b"!")
    elif change == "receipt":
        receipt.write_bytes(receipt.read_bytes() + b"\n")
    elif change == "original":
        path.write_bytes(CONTENT)
    else:
        path.symlink_to(path.with_name("absent.csv"))
    with pytest.raises(ValueError, match="recovery_(migrated_representation|files)_changed"):
        audit.assert_stable()


def test_recovery_rejects_receipt_change_during_original_csv_read(settings, monkeypatch):
    from backend import maintenance_backup_recovery as recovery

    path, _ = write_output(settings)
    compress(settings)
    _, receipt = storage.csv_storage_paths(path)
    real_read = recovery.read_stored_csv

    def changed_read(value):
        result = real_read(value)
        receipt.write_bytes(receipt.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(recovery, "read_stored_csv", changed_read)
    with pytest.raises(ValueError, match="recovery_files_changed"):
        recovery.RecoveryAudit(settings.project_root).bind(path, hashlib.sha256(CONTENT).hexdigest())


def test_source_headers_keep_logical_identity_but_cache_detects_storage_changes(settings, monkeypatch):
    import backend.app as app

    path, _ = write_output(settings)
    settings.latest_report_file = settings.reports_dir / "latest.json"
    settings.publication_registry_file = settings.reports_dir / "registry.json"
    settings.latest_report_file.write_text("{}")
    settings.publication_registry_file.write_text("{}")
    monkeypatch.setattr(app, "settings", settings)
    monkeypatch.setattr(app, "_resolve_project_file_artifact_source", lambda p: {
        "report_payload": {}, "publication": {}, "resolved_path": path,
        "artifact_key": "", "artifact_path": "",
    })
    relative = path.relative_to(settings.project_root).as_posix()
    before = app._build_project_file_source_headers(path, relative_path=relative, download=True)
    before_cache = app._path_cache_token(path)
    compress(settings)
    assert app._build_project_file_source_headers(path, relative_path=relative, download=True) == before
    assert app._path_cache_token(path) != before_cache
    stored, receipt = storage.csv_storage_paths(path)
    receipt.write_bytes(receipt.read_bytes() + b"\n")
    changed_cache = app._path_cache_token(path)
    stored.write_bytes(b"!" + stored.read_bytes()[1:])
    assert app._path_cache_token(path) != changed_cache
    with pytest.raises(storage.CoreCsvStorageError):
        app._file_content_cache_token(path)


def test_plain_file_precedence_restore_no_overwrite_and_writer_guards(settings):
    from bond_fund_screening.tasks.output_retention import _delete_file_and_sidecar
    from bond_fund_screening.report_artifact_metadata import rename_report_with_sidecar

    path, _ = write_output(settings)
    compress(settings)
    migration.restore_core_csv(settings, path)
    assert path.read_bytes() == CONTENT
    with pytest.raises(FileExistsError):
        migration.restore_core_csv(settings, path)
    for action in (lambda: _delete_file_and_sidecar(path), lambda: rename_report_with_sidecar(path, path.with_name("new.csv"))):
        with pytest.raises(ValueError, match="explicit_restore"):
            action()
    path.write_bytes(b"new complete ordinary representation\n")
    assert storage.read_original_artifact_bytes(path) == path.read_bytes()
    assert len(storage.glob_artifacts(path.parent, "*.csv")) == 1


def test_batch_budget_and_scheduled_cleanup_have_separate_accounting(settings):
    from scripts.cleanup_disk_usage import run_cleanup

    path, _ = write_output(settings)
    second, _ = write_output(settings, NAME.replace("1年期", "2年期"))
    result = migration.run_core_csv_compression(settings, max_files=1)
    assert result["candidate_count"] == 1 and result["retained_reasons"]["batch_file_budget"] == 1
    result = run_cleanup(settings, apply=True, logs_only=True).to_dict()
    assert result["core_csv_compression"] is None and path.exists() and second.exists()
    result = run_cleanup(settings, apply=True).to_dict()
    assert result["core_csv_compression"]["compressed_count"] == 2
    assert result["accounting"]["removed_logical_bytes"] == 0
    assert result["accounting"]["net_reclaimed_disk_bytes"] is None


def test_configuration_reads_policy_from_weekly_disk_cleanup():
    from backend.config import load_settings

    configured = load_settings()
    assert configured.scheduler_weekly_disk_cleanup_core_csv_enabled is True
    assert configured.scheduler_weekly_disk_cleanup_core_csv_keep_days == 30
