"""Historical operational evidence must survive storage migration byte for byte."""
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

from backend import legacy_task_audit_compression as migration
from backend import task_audit_storage as storage
from backend.api_routes.reports import register_reports_routes
from backend.maintenance_backup_recovery import RecoveryAudit, _verify_task_sidecar
from backend.replay_evidence import content_hash
from backend.scheduler_diagnostics import _read_json_object, build_recent_scheduler_failure_recovery_summary
from scripts.audit_data_truthfulness import _file_evidence, _read_json


def legacy_file(root, *, status="failed", persistence=None):
    path = root / "reports/.audit/scheduler_task_runs/20260801/20260801_030000_task.json"
    path.parent.mkdir(parents=True)
    result = {"status": status, "records_processed": None, "records_inserted": 0,
              "warning_reason": None, "error": "historical operational failure"}
    payload = {"task": {"task_history_id": 7, "task_name": "scheduled_weekly_bond_maintenance",
                        "task_key": "weekly_maintenance",
                        "status": status, "started_at": "2026-08-01T03:00:00", "finished_at": None},
               "task_result": result, "workflow_summary": {"logs_tail": ["完整历史日志"] * 3000}}
    if persistence is not None:
        payload["persistence_stage"] = persistence
    # Preserve formatting and trailing whitespace too, not just equal objects.
    original = (json.dumps(payload, ensure_ascii=False, indent=3) + "  \n\n").encode()
    path.write_bytes(original)
    os.utime(path, (time.time() - 7200,) * 2)
    return path, original, payload


def compress(root):
    plan = migration.plan_legacy_task_audit_compression(root)
    assert plan["candidate_count"] == 1
    assert plan["removed_count"] == 0 and plan["net_reclaimed_disk_bytes"] is None
    result = migration.compress_legacy_task_audit(root, plan["candidates"][0])
    assert result["status"] == "compressed", result
    return result


def test_complete_original_values_hashes_readers_and_restore_survive(tmp_path):
    path, original, payload = legacy_file(tmp_path)
    settings = SimpleNamespace(project_root=tmp_path)
    before_evidence = _file_evidence(settings, path)
    before_identity = migration.identity(path)
    outcome = compress(tmp_path)
    stored, receipt = storage.migration_paths(path)
    assert not path.exists() and stored.is_file() and receipt.is_file()
    assert storage.artifact_exists(path)
    assert storage.read_original_artifact_bytes(path) == gzip.decompress(stored.read_bytes()) == original
    assert storage.read_task_audit(path) == storage.read_task_audit(stored) == payload
    assert _read_json_object(path) == _read_json(path) == payload
    assert _file_evidence(settings, path) == before_evidence
    assert content_hash(path) == hashlib.sha256(original).hexdigest()
    assert outcome["independent_restore_verified"] is True
    assert outcome["net_reclaimed_disk_bytes"] is None
    assert outcome["estimated_net_file_allocation_reduction_bytes"] == (
        before_identity["allocated_bytes"] - stored.stat().st_blocks * 512 - receipt.stat().st_blocks * 512)
    target = tmp_path / "restored/original.json"
    restored = migration.restore_legacy_task_audit(path, target)
    assert restored["full_restore_verified"] and target.read_bytes() == original
    assert target.stat().st_mtime_ns == before_identity["mtime_ns"]
    with pytest.raises(FileExistsError):
        migration.restore_legacy_task_audit(path, target)
    for name in (path, stored):
        with pytest.raises(ValueError, match="read_only"):
            storage.write_task_audit(name, payload, replace_existing=True)


def test_recovery_binds_both_physical_files_and_preserves_full_payload_sha(tmp_path):
    path, original, payload = legacy_file(tmp_path)
    original_result = json.dumps(payload["task_result"], ensure_ascii=False, separators=(",", ":")).encode()
    compact = {"task_history_extra_data_compacted": True, "task_history_extra_data_schema_version": 1,
               "task_history_full_payload": {"available": True, "source": "scheduler_task_run_audit",
                   "sidecar_project_path": str(path.relative_to(tmp_path)),
                   "full_payload_bytes": len(original_result),
                   "full_payload_sha256": hashlib.sha256(original_result).hexdigest()}}
    old = _verify_task_sidecar(json.dumps(compact), 7, RecoveryAudit(tmp_path))
    compress(tmp_path)
    audit = RecoveryAudit(tmp_path)
    assert audit.read_json(path, hashlib.sha256(original).hexdigest()) == payload
    assert _verify_task_sidecar(json.dumps(compact), 7, audit) == old
    stored, receipt = storage.migration_paths(path)
    assert set(audit.files) == {str(stored), str(receipt)}
    assert audit.files[str(stored)]["sha256"] == hashlib.sha256(stored.read_bytes()).hexdigest()
    changed = bytearray(stored.read_bytes()); changed[-5] ^= 1
    stored.write_bytes(changed)
    with pytest.raises(ValueError, match="changed"):
        audit.assert_stable()


def test_existing_historical_proof_resolvers_keep_original_bytes_and_path(tmp_path):
    from backend import collection_count_correction as counts
    from backend import refresh_collection_correction as refresh
    from backend import weekly_warning_correction as weekly

    path, original, payload = legacy_file(tmp_path)
    compress(tmp_path)
    relative = str(path.relative_to(tmp_path))
    for module in (counts, refresh, weekly):
        assert module._resolve(tmp_path, "/app/" + relative) == path
    assert counts._read(path) == refresh._read(path) == original
    assert weekly._read_object(path) == payload


@pytest.mark.parametrize("damage", ["compressed", "original_sha", "wrong_name", "missing_receipt", "bad_json"])
def test_corrupt_or_missing_migration_evidence_never_becomes_valid(tmp_path, damage):
    path, _, _ = legacy_file(tmp_path)
    compress(tmp_path)
    stored, receipt = storage.migration_paths(path)
    value = json.loads(receipt.read_text())
    if damage == "compressed":
        stored.write_bytes(stored.read_bytes()[:-4])
    elif damage == "missing_receipt":
        receipt.unlink()
    elif damage == "bad_json":
        receipt.write_text("[]")
    else:
        value["original_sha256" if damage == "original_sha" else "original_name"] = "0" * 64
        receipt.write_text(json.dumps(value))
    with pytest.raises((OSError, ValueError)):
        storage.read_task_audit(path)
    assert storage.artifact_exists(path) is False
    assert _read_json(path) == _read_json_object(path) == {}


def test_migration_budget_and_scope_do_not_enable_arbitrary_gzip_fallback(tmp_path):
    path, _, _ = legacy_file(tmp_path)
    compress(tmp_path)
    with pytest.raises(ValueError, match="budget"):
        storage.read_task_audit(path, max_bytes=100)
    other = tmp_path / "config.json"
    Path(f"{other}.gz").write_bytes(gzip.compress(b'{"value":0}'))
    with pytest.raises(FileNotFoundError):
        storage.read_task_audit(other)


@pytest.mark.parametrize("protected", ["recent", "running", "unsealed", "shared", "symlink"])
def test_nonterminal_active_and_linked_originals_are_retained(tmp_path, protected):
    path, original, _ = legacy_file(tmp_path, status="running" if protected == "running" else "failed",
                                    persistence="audit_collected_before_completion" if protected == "unsealed" else None)
    if protected == "recent":
        os.utime(path, None)
    elif protected == "shared":
        os.link(path, tmp_path / "shared")
    elif protected == "symlink":
        target = tmp_path / "target"; path.rename(target); path.symlink_to(target)
    plan = migration.plan_legacy_task_audit_compression(tmp_path)
    assert plan["candidate_count"] == 0 and plan["retained_count"] == 1
    assert path.read_bytes() == original


@pytest.mark.parametrize("failure", ["unlink", "restore", "source_changed", "space", "destination"])
def test_failed_migration_preserves_original_and_never_claims_space_reclaimed(tmp_path, monkeypatch, failure):
    path, original, _ = legacy_file(tmp_path)
    candidate = migration.plan_legacy_task_audit_compression(tmp_path)["candidates"][0]
    if failure == "unlink":
        unlink = Path.unlink
        def fail_unlink(current, *args, **kwargs):
            if current == path:
                raise OSError("simulated delete failure")
            return unlink(current, *args, **kwargs)
        monkeypatch.setattr(Path, "unlink", fail_unlink)
    elif failure == "restore":
        def fail_restore(*args):
            raise OSError("simulated restore failure")
        monkeypatch.setattr(migration, "_independent_restore", fail_restore)
    elif failure == "source_changed":
        path.write_bytes(original + b"\n")
        os.utime(path, (time.time() - 7200,) * 2)
    elif failure == "space":
        monkeypatch.setattr(migration.shutil, "disk_usage", lambda _: SimpleNamespace(free=1))
    else:
        storage.migration_paths(path)[0].write_bytes(b"do not replace")
    result = migration.compress_legacy_task_audit(tmp_path, candidate)
    assert result["status"] == "failed" and result["removed"] is False
    assert result["estimated_net_file_allocation_reduction_bytes"] is None
    assert result["net_reclaimed_disk_bytes"] is None and path.exists()
    assert path.read_bytes() == original + (b"\n" if failure == "source_changed" else b"")
    if failure in {"unlink", "restore"}:
        monkeypatch.undo()
        assert migration.compress_legacy_task_audit(tmp_path, candidate)["status"] == "compressed"
        assert storage.read_original_artifact_bytes(path) == original


def test_original_url_download_and_etag_are_bytes_preserving(tmp_path):
    path, original, _ = legacy_file(tmp_path)
    compress(tmp_path)
    app = Flask(__name__)
    callbacks = {name: lambda *a, **kw: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    register_reports_routes(app, **callbacks)
    url = "/api/files/" + str(path.relative_to(tmp_path))
    with app.test_client() as client:
        response = client.get(url + "?download=1")
        assert response.status_code == 200 and response.data == original
        assert response.headers["Content-Type"].startswith("application/json")
        assert path.name in response.headers["Content-Disposition"]
        assert response.headers["X-Fund-ANA-Original-SHA256"] == hashlib.sha256(original).hexdigest()
        assert client.get(url, headers={"If-None-Match": response.headers["ETag"]}).status_code == 304
        storage.migration_paths(path)[0].write_bytes(b"corrupt")
        assert client.get(url).status_code == 409


def test_interrupted_migration_never_double_counts_failure_recovery(tmp_path, monkeypatch):
    path, original, _ = legacy_file(tmp_path)
    candidate = migration.plan_legacy_task_audit_compression(tmp_path)["candidates"][0]
    def fail_restore(*args):
        raise OSError("simulated interrupted restoration")
    monkeypatch.setattr(migration, "_independent_restore", fail_restore)
    assert migration.compress_legacy_task_audit(tmp_path, candidate)["status"] == "failed"
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")
    before = build_recent_scheduler_failure_recovery_summary(settings, now=datetime(2026, 8, 2))
    assert before["scanned_run_total"] == 1
    monkeypatch.undo()
    assert migration.compress_legacy_task_audit(tmp_path, candidate)["status"] == "compressed"
    after = build_recent_scheduler_failure_recovery_summary(settings, now=datetime(2026, 8, 2))
    assert after == before
    assert storage.read_original_artifact_bytes(path) == original


def test_file_resolver_rejects_sibling_with_same_project_name_prefix(tmp_path, monkeypatch):
    from backend import app as app_module

    root = tmp_path / "Fund-ANA"; root.mkdir()
    sibling = tmp_path / "Fund-ANA-backup"; sibling.mkdir()
    monkeypatch.setattr(app_module, "settings", SimpleNamespace(project_root=root))
    assert app_module._resolve_project_file("reports/audit.json") == root / "reports/audit.json"
    with pytest.raises(ValueError, match="非法路径"):
        app_module._resolve_project_file("../Fund-ANA-backup/private.json")
