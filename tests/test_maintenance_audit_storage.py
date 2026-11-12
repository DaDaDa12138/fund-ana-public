"""Complete historical maintenance evidence survives bounded compression."""
from datetime import datetime
import gzip
import hashlib
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time

from flask import Flask
import pytest

from backend import maintenance_audit_compression as migration
from backend import task_audit_storage as storage
from backend.api_routes.reports import register_reports_routes
from backend.audit_support import read_json_file
from backend.maintenance_backup_recovery import RecoveryAudit
from backend.replay_evidence import content_hash
from backend.runtime_replay import read_json_file as read_runtime_json
from scripts.audit_runtime_utils import load_json
from scripts.audit_data_truthfulness import _file_evidence, _read_json


def audit_file(root, family="nav_history_maintenance", *, latest=True, stamp="20260801_030000"):
    directory = root / "reports/audit" / family
    directory.mkdir(parents=True, exist_ok=True)
    prefix = storage.MAINTENANCE_AUDIT_PREFIXES[family]
    path = directory / f"{prefix}_{stamp}.json"
    # Synthetic operational messages only; no fabricated fund business data.
    payload = {"generated_at": "2026-08-01T03:00:00", "status": "failed",
               "unknown_count": None, "observed_zero": 0,
               "operational_messages": ["isolated test: upstream request failed"] * 3000,
               "audit_before": {}, "audit_after": {}, "maintenance_result": {},
               "samples_by_action": {}, "samples_by_blocker": {}, "samples_by_source": {},
               "samples_by_reason": {}, "primary_reason_counts": {}, "audit_error": None}
    if family == "data_truthfulness":
        payload.update(schema_version=1, artifact_type="data_truthfulness_audit", status="needs_review",
                       sections={"input": {"missing": None, "observed_zero": 0}},
                       blockers=["isolated test: upstream evidence missing"], warnings=[],
                       blocker_count=1, warning_count=0)
    raw = (json.dumps(payload, ensure_ascii=False, indent=3) + " \n\n").encode()
    path.write_bytes(raw)
    os.utime(path, (time.time() - 3 * 86400,) * 2)
    if latest:
        newest = directory / f"{prefix}_20260802_030000.json.gz"
        storage.write_task_audit(newest, {**payload, "generated_at": "2026-08-02T03:00:00"})
    return path, raw, payload


def compress(root):
    plan = migration.plan_maintenance_audit_compression(root)
    assert plan["candidate_count"] == 1, plan
    result = migration.apply_maintenance_audit_plan(root, plan)
    assert result["compressed_count"] == 1 and result["failed_count"] == 0, result
    return result


@pytest.mark.parametrize("family", sorted(storage.MAINTENANCE_AUDIT_FAMILIES))
def test_original_values_sha_generic_readers_and_independent_restore(tmp_path, family):
    from backend import app as app_module

    path, original, payload = audit_file(tmp_path, family)
    before = migration.identity(path)
    settings = SimpleNamespace(project_root=tmp_path)
    evidence = _file_evidence(settings, path)
    result = compress(tmp_path)
    stored, receipt = storage.migration_paths(path)
    assert stored.name.endswith(".json.legacy.gz")
    assert not path.exists() and storage.artifact_exists(path)
    assert storage.read_original_artifact_bytes(path) == gzip.decompress(stored.read_bytes()) == original
    for current in (path, stored):
        assert storage.read_task_audit(current) == payload
        assert read_json_file(current) == read_runtime_json(current) == load_json(current) == payload
        assert _read_json(current) == app_module._safe_json_load(current) == payload
    assert content_hash(path) == hashlib.sha256(original).hexdigest()
    assert _file_evidence(settings, path) == evidence
    audit = RecoveryAudit(tmp_path)
    assert audit.read_json(path, hashlib.sha256(original).hexdigest()) == payload
    assert set(audit.files) == {str(stored), str(receipt)}
    record = json.loads(receipt.read_text())
    assert record["audit_identity"] == {"family": family, "generated_at": payload["generated_at"]}
    assert "task_identity" not in record
    target = tmp_path / "restore/original.json"
    assert migration.restore_maintenance_audit(path, target)["full_restore_verified"]
    assert target.read_bytes() == original
    actual = migration.identity(target)
    for key in ("size", "mtime_ns", "mode", "uid", "gid"):
        assert actual[key] == before[key]
    assert result["estimated_net_file_allocation_reduction_bytes"] > 0
    assert result["net_reclaimed_disk_bytes"] is None
    with pytest.raises(FileExistsError):
        migration.restore_maintenance_audit(path, target)
    with pytest.raises(ValueError, match="read_only"):
        storage.write_task_audit(path, payload, replace_existing=True)


@pytest.mark.parametrize("damage", ["gzip", "missing_receipt", "wrong_sha", "wrong_identity", "duplicate_key"])
def test_damaged_receipt_never_reclassifies_legacy_gzip_as_native(tmp_path, damage):
    path, _, _ = audit_file(tmp_path)
    compress(tmp_path)
    stored, receipt = storage.migration_paths(path)
    value = json.loads(receipt.read_text())
    if damage == "gzip":
        stored.write_bytes(stored.read_bytes()[:-3])
    elif damage == "missing_receipt":
        receipt.unlink()
    elif damage == "duplicate_key":
        receipt.write_text(receipt.read_text().replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'))
    else:
        if damage == "wrong_sha":
            value["original_sha256"] = "0" * 64
        else:
            value["audit_identity"]["family"] = "different_scope"
        receipt.write_text(json.dumps(value))
    assert path in storage.glob_json_artifacts(path.parent, "nav_history_maintenance_*.json")
    assert stored not in storage.glob_json_artifacts(path.parent, "nav_history_maintenance_*.json")
    for current in (path, stored):
        with pytest.raises((OSError, ValueError)):
            storage.read_task_audit(current)
        assert _read_json(current) == {}
    assert not storage.artifact_exists(path)


@pytest.mark.parametrize("protected", ["recent", "latest", "shared", "symlink", "incomplete", "auxiliary", "run_local"])
def test_active_or_out_of_scope_originals_are_preserved(tmp_path, protected):
    path, original, payload = audit_file(tmp_path, latest=protected != "latest")
    if protected == "recent":
        os.utime(path, None)
    elif protected == "shared":
        os.link(path, tmp_path / "shared.json")
    elif protected == "symlink":
        target = tmp_path / "target.json"
        path.rename(target)
        path.symlink_to(target)
    elif protected == "incomplete":
        payload.pop("audit_after")
        path.write_text(json.dumps(payload))
        os.utime(path, (time.time() - 3 * 86400,) * 2)
    elif protected == "auxiliary":
        path = path.rename(path.parent / "retry_failed_codes.json")
    elif protected == "run_local":
        target = tmp_path / "reports/runs/example/audit/nav_history_maintenance" / path.name
        target.parent.mkdir(parents=True)
        path = path.rename(target)
    plan = migration.plan_maintenance_audit_compression(tmp_path)
    assert plan["candidate_count"] == 0
    assert path.exists()
    if protected != "incomplete":
        assert path.read_bytes() == original


@pytest.mark.parametrize("failure", ["restore", "unlink", "source_changed", "conflict"])
def test_interruption_and_conflict_preserve_original_and_can_resume(tmp_path, monkeypatch, failure):
    from backend import legacy_task_audit_compression as transaction

    path, original, _ = audit_file(tmp_path)
    plan = migration.plan_maintenance_audit_compression(tmp_path)
    if failure == "restore":
        def fail(*args):
            raise OSError("test restoration interruption")
        monkeypatch.setattr(transaction, "_independent_restore", fail)
    elif failure == "unlink":
        unlink = Path.unlink
        def fail(current, *args, **kwargs):
            if current == path:
                raise OSError("test removal interruption")
            return unlink(current, *args, **kwargs)
        monkeypatch.setattr(Path, "unlink", fail)
    elif failure == "source_changed":
        path.write_bytes(original + b"\n")
        os.utime(path, (time.time() - 3 * 86400,) * 2)
    else:
        storage.migration_paths(path)[0].write_bytes(b"existing unrelated data")
    result = migration.apply_maintenance_audit_plan(tmp_path, plan)
    assert result["failed_count"] == 1 and result["removed_count"] == 0
    assert result["estimated_net_file_allocation_reduction_bytes"] is None
    assert path.read_bytes() == original + (b"\n" if failure == "source_changed" else b"")
    if failure in {"restore", "unlink"}:
        paths = storage.glob_json_artifacts(path.parent, "nav_history_maintenance_*.json")
        assert paths.count(path) == 1 and storage.migration_paths(path)[0] not in paths
        monkeypatch.undo()
        assert migration.apply_maintenance_audit_plan(tmp_path, plan)["compressed_count"] == 1


def test_scan_and_apply_enforce_file_byte_and_duplicate_budgets(tmp_path):
    path, raw, _ = audit_file(tmp_path)
    plan = migration.plan_maintenance_audit_compression(tmp_path, max_files=1, max_input_bytes=len(raw) - 1)
    assert plan["candidate_count"] == 0
    with pytest.raises(ValueError, match="budget"):
        migration.plan_maintenance_audit_compression(tmp_path, max_files=257)
    plan = migration.plan_maintenance_audit_compression(tmp_path)
    plan["candidates"] *= 2
    with pytest.raises(ValueError, match="duplicate"):
        migration.apply_maintenance_audit_plan(tmp_path, plan)
    assert path.read_bytes() == raw
    compress(tmp_path)
    with pytest.raises(ValueError, match="budget"):
        storage.read_task_audit(path, max_bytes=100)


def test_latest_loaders_mixed_formats_cache_changes_and_failure_semantics(tmp_path, monkeypatch):
    from backend import app as app_module
    from backend.nav_history_maintenance import load_latest_nav_history_maintenance_payload
    from backend.long_term_history_maintenance import load_latest_long_term_history_maintenance_payload
    from backend.risk_coverage_gap_audit import load_latest_risk_coverage_gap_audit_payload

    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs")
    monkeypatch.setattr(app_module, "settings", settings)
    for family, loader in (
        ("nav_history_maintenance", load_latest_nav_history_maintenance_payload),
        ("long_term_history_maintenance", load_latest_long_term_history_maintenance_payload),
        ("risk_coverage_gap_audit", load_latest_risk_coverage_gap_audit_payload),
    ):
        root = tmp_path / family
        path, original, _ = audit_file(root, family)
        current_settings = SimpleNamespace(reports_dir=root / "reports")
        expected = loader(current_settings)
        assert expected[0].name.endswith(".json.gz") and expected[1]["status"] == "failed"
        before = app_module._glob_cache_token(path.parent, family + "_*.json")
        digest = app_module._file_content_cache_token(path)
        compress(root)
        assert loader(current_settings) == expected
        assert app_module._glob_cache_token(path.parent, family + "_*.json") != before
        assert app_module._file_content_cache_token(path) == digest
        assert app_module._path_cache_token(path) != "missing"
        relative = path.relative_to(tmp_path)
        assert app_module._resolve_existing_report_artifact_path(str(relative)) == path
        expected[0].write_bytes(b"invalid gzip")
        assert loader(current_settings) == (expected[0], None)
        assert storage.read_original_artifact_bytes(path) == original


def test_module_writers_preserve_values_and_leave_mutable_state_plain(tmp_path):
    from backend import nav_history_maintenance, long_term_history_maintenance, risk_coverage_gap_audit

    for module in (nav_history_maintenance, long_term_history_maintenance, risk_coverage_gap_audit):
        directory = tmp_path / module.__name__
        path = directory / "artifact.json.gz"
        payload = {"status": "degraded", "unknown_count": None, "observed_zero": 0}
        module._write_json(path, payload)
        assert storage.read_task_audit(path) == payload
        with pytest.raises(FileExistsError):
            module._write_json(path, payload)
        state = directory / "state.json"
        module._write_json(state, payload)
        assert json.loads(state.read_text()) == payload
        module._write_json(state, {**payload, "status": "failed"})
        assert json.loads(state.read_text())["status"] == "failed"
    dated = tmp_path / "dated.json.gz"
    nav_history_maintenance._write_json(dated, {"generated_at": datetime(2026, 8, 1)})
    assert storage.read_task_audit(dated)["generated_at"] == "2026-08-01T00:00:00"


@pytest.mark.parametrize("family", ["nav_history_maintenance", "data_truthfulness"])
def test_original_url_native_download_and_conditional_request(tmp_path, family):
    path, original, payload = audit_file(tmp_path, family)
    compress(tmp_path)
    app = Flask(__name__)
    callbacks = {name: lambda *a, **kw: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    callbacks["build_project_file_source_headers"] = lambda *a, **kw: {"X-Fund-ANA-Source": "project_file_artifact_binary"}
    register_reports_routes(app, **callbacks)
    url = "/api/files/" + str(path.relative_to(tmp_path))
    with app.test_client() as client:
        response = client.get(url + "?download=1")
        assert response.status_code == 200 and response.data == original
        assert response.headers["Content-Type"].startswith("application/json")
        assert path.name in response.headers["Content-Disposition"]
        assert response.headers["X-Fund-ANA-Original-SHA256"] == hashlib.sha256(original).hexdigest()
        assert response.headers["X-Fund-ANA-Source"] == "project_file_artifact_binary"
        assert client.get(url, headers={"If-None-Match": response.headers["ETag"]}).status_code == 304
        legacy_url = "/api/files/" + str(storage.migration_paths(path)[0].relative_to(tmp_path))
        response = client.get(legacy_url)
        assert response.status_code == 200 and gzip.decompress(response.data) == original
        native = path.parent / f"{storage.MAINTENANCE_AUDIT_PREFIXES[family]}_20260803_030000.json.gz"
        storage.write_task_audit(native, payload)
        response = client.get("/api/files/" + str(native.relative_to(tmp_path)) + "?download=1")
        assert response.status_code == 200 and response.data == native.read_bytes()
        assert json.loads(gzip.decompress(response.data)) == payload
        storage.migration_paths(path)[0].write_bytes(b"corrupt")
        assert client.get(url).status_code == 409
        assert client.get(legacy_url).status_code == 409


def test_truthfulness_writer_and_latest_reader_preserve_original_audit_status(tmp_path, monkeypatch):
    from backend import app as app_module
    from scripts.audit_data_truthfulness import write_data_truthfulness_audit

    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")
    monkeypatch.setattr(app_module, "settings", settings)
    path, original, payload = audit_file(tmp_path, "data_truthfulness")
    expected = app_module._load_latest_data_truthfulness_audit_artifact()
    assert expected[0].name.endswith(".json.gz") and expected[1]["status"] == "needs_review"
    compress(tmp_path)
    # Compression has a newer filesystem timestamp than the latest audit.
    stored = storage.migration_paths(path)[0]
    os.utime(stored, (time.time() + 60,) * 2)
    assert app_module._load_latest_data_truthfulness_audit_artifact() == expected
    assert storage.read_original_artifact_bytes(path) == original
    expected[0].write_bytes(b"invalid gzip")
    assert app_module._load_latest_data_truthfulness_audit_artifact() == (expected[0], {})
    expected[0].unlink()
    assert app_module._load_latest_data_truthfulness_audit_artifact() == (path, payload)

    written = write_data_truthfulness_audit(settings, payload)
    assert written.name.endswith(".json.gz")
    assert storage.read_task_audit(written) == payload
    assert app_module._load_latest_data_truthfulness_audit_artifact() == (written, payload)
    assert payload["sections"]["input"] == {"missing": None, "observed_zero": 0}
    assert not list(written.parent.glob("*.tmp"))


@pytest.mark.parametrize("damage", ["artifact_type", "schema_version", "running", "sections", "missing_list",
                                    "count_mismatch", "boolean_count", "prefix"])
def test_truthfulness_incomplete_or_wrong_identity_is_not_migrated(tmp_path, damage):
    path, _, payload = audit_file(tmp_path, "data_truthfulness")
    if damage == "artifact_type":
        payload["artifact_type"] = "different_audit"
    elif damage == "schema_version":
        payload["schema_version"] = True
    elif damage == "running":
        payload["status"] = "running"
    elif damage == "sections":
        payload.pop("sections")
    elif damage == "missing_list":
        payload.pop("warnings")
    elif damage == "count_mismatch":
        payload["blocker_count"] = 0
    elif damage == "boolean_count":
        payload["blocker_count"] = True
    elif damage == "prefix":
        path = path.rename(path.parent / "data_truthfulness_20260801_030000.json")
    original = json.dumps(payload).encode()
    path.write_bytes(original)
    os.utime(path, (time.time() - 3 * 86400,) * 2)
    plan = migration.plan_maintenance_audit_compression(tmp_path)
    assert plan["candidate_count"] == 0
    assert path.read_bytes() == original


def test_latest_truthfulness_plain_artifact_stays_protected(tmp_path):
    path, original, _ = audit_file(tmp_path, "data_truthfulness", latest=False)
    plan = migration.plan_maintenance_audit_compression(tmp_path)
    assert plan["candidate_count"] == 0
    assert plan["retained"][0]["reason"] == "maintenance_audit_latest_protected"
    assert path.read_bytes() == original
