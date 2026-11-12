"""Legacy exports preserve original bytes, evidence binding, and failure states."""
from datetime import datetime
import gzip
import hashlib
import inspect
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time

from flask import Flask
import pandas as pd
import pytest

from backend import legacy_audit_csv_compression as migration
from backend import task_audit_storage as storage
from backend.api_routes.reports import register_reports_routes
from backend.maintenance_backup_recovery import RecoveryAudit
from backend.replay_evidence import content_hash
from scripts.audit_data_truthfulness import _file_evidence


def audit_csv(root, family="score_dedup_shadow", *, latest=True):
    directory = root / "reports/audit" / family
    directory.mkdir(parents=True, exist_ok=True)
    prefix = family + "_20260801_020000_sync_20260801_030000_123456"
    path = directory / (prefix + "_samples.csv")
    # These are explicitly synthetic storage values, never production funds.
    frame = pd.DataFrame({"identifier": ["000001", "000002", "000003"] * 1800,
                          "text,quoted": ['synthetic "quote"', "first\nsecond", "中文"] * 1800,
                          "missing_vs_zero": [None, 0.0, -1.25] * 1800})
    raw = frame.to_csv(index=False, lineterminator="\r\n").encode("utf-8-sig")
    path.write_bytes(raw)
    context = directory / (prefix + ".json")
    context.write_text(json.dumps({"run_id": "20260801_020000_sync", "generated_at": "2026-08-01T03:00:00",
                                   "status": "failed", "passed": False, "unknown_count": None,
                                   "observed_zero": 0, "artifacts": {"samples": str(path)}}, ensure_ascii=False))
    for current in (path, context):
        os.utime(current, (time.time() - 3 * 86400,) * 2)
    if latest:
        newer = directory / (family + "_20260802_020000_sync_20260802_030000.json")
        newer.write_text(json.dumps({"run_id": "20260802_020000_sync", "generated_at": "2026-08-02T03:00:00"}))
    return path, raw, context


def compress(root):
    plan = migration.plan_legacy_audit_csv_compression(root)
    assert plan["candidate_count"] == 1, plan
    result = migration.apply_legacy_audit_csv_plan(root, plan)
    assert result["compressed_count"] == 1 and result["failed_count"] == 0, result
    return result


def independent_timestamp_csv(root, *, path_style="relative", declared_sha=True):
    path, raw, context = audit_csv(root)
    payload = json.loads(context.read_bytes())
    payload["generated_at"] = "2026-07-31T23:59:59+08:00"
    relative = path.relative_to(root)
    attachment = str({"relative": relative, "host": path, "container": Path("/app") / relative}[path_style])
    payload["artifacts"] = {"samples": ({"path": attachment, "sha256": hashlib.sha256(raw).hexdigest()}
                                         if declared_sha else attachment)}
    context.write_text(json.dumps(payload, ensure_ascii=False))
    os.utime(context, (time.time() - 3 * 86400,) * 2)
    return path, raw, context


@pytest.mark.parametrize("path_style", ["relative", "host", "container"])
@pytest.mark.parametrize("declared_sha", [True, False])
def test_independent_timestamps_require_explicit_binding_and_preserve_originals(tmp_path, path_style, declared_sha):
    path, raw, context = independent_timestamp_csv(tmp_path, path_style=path_style, declared_sha=declared_sha)
    context_raw = context.read_bytes()
    compress(tmp_path)
    restored, _, receipt = storage.read_migrated_audit_csv(path)
    identity = receipt["audit_identity"]
    assert identity["identity_contract"] == "explicit_attachment_independent_timestamps_v1"
    assert identity["generated_at"] == "2026-07-31T23:59:59+08:00"
    assert identity["filename_timestamp"] == "20260801_030000_123456"
    assert identity["artifact_key"] == "samples"
    assert identity["artifact_path"] == str(path.relative_to(tmp_path))
    assert ("artifact_sha256" in identity) is declared_sha
    assert restored == raw and context.read_bytes() == context_raw
    assert json.loads(context_raw)["unknown_count"] is None
    assert json.loads(context_raw)["observed_zero"] == 0
    assert json.loads(context_raw)["passed"] is False
    destination = tmp_path / "restored.csv"
    assert migration.restore_legacy_audit_csv(path, destination)["full_restore_verified"]
    assert destination.read_bytes() == raw
    audit = RecoveryAudit(tmp_path)
    audit.bind(path, hashlib.sha256(raw).hexdigest())
    audit.assert_stable()


def test_original_receipt_identity_is_unchanged(tmp_path):
    path, raw, context = audit_csv(tmp_path)
    payload = json.loads(context.read_bytes())
    del payload["artifacts"]
    context.write_text(json.dumps(payload))
    os.utime(context, (time.time() - 3 * 86400,) * 2)
    expected = {"family": "score_dedup_shadow", "run_id": "20260801_020000_sync",
                "generated_at": "2026-08-01T03:00:00", "context_name": context.name,
                "context_sha256": hashlib.sha256(context.read_bytes()).hexdigest()}
    compress(tmp_path)
    restored, _, receipt = storage.read_migrated_audit_csv(path)
    assert receipt["audit_identity"] == expected and restored == raw


@pytest.mark.parametrize("damage", ["missing_map", "wrong_map_type", "wrong_path", "basename_only",
                                   "traversal", "duplicate", "wrong_sha", "null_sha", "boolean_sha", "wrong_run"])
def test_independent_timestamps_never_relax_attachment_identity(tmp_path, damage):
    path, raw, context = independent_timestamp_csv(tmp_path)
    payload = json.loads(context.read_bytes())
    attachment = payload["artifacts"]["samples"]
    if damage == "missing_map":
        del payload["artifacts"]
    elif damage == "wrong_map_type":
        payload["artifacts"] = [attachment]
    elif damage == "wrong_path":
        attachment["path"] = "reports/audit/other/" + path.name
    elif damage == "basename_only":
        attachment["path"] = path.name
    elif damage == "traversal":
        attachment["path"] = str(path.parent / ".." / path.parent.name / path.name)
    elif damage == "duplicate":
        payload["artifacts"]["another"] = dict(attachment)
    elif damage == "wrong_sha":
        attachment["sha256"] = "0" * 64
    elif damage == "null_sha":
        attachment["sha256"] = None
    elif damage == "boolean_sha":
        attachment["sha256"] = True
    else:
        payload["run_id"] = "20260802_020000_sync"
    context.write_text(json.dumps(payload))
    os.utime(context, (time.time() - 3 * 86400,) * 2)
    plan = migration.plan_legacy_audit_csv_compression(tmp_path)
    assert plan["candidate_count"] == 0 and len(plan["retained"]) == 1
    assert path.read_bytes() == raw


def test_original_context_digest_is_checked_against_restored_csv(tmp_path):
    path, _, context = independent_timestamp_csv(tmp_path)
    compress(tmp_path)
    payload = json.loads(context.read_bytes())
    payload["artifacts"]["samples"]["sha256"] = "0" * 64
    context.write_text(json.dumps(payload))
    _, receipt_path = storage.migration_paths(path)
    receipt = json.loads(receipt_path.read_bytes())
    receipt["audit_identity"]["artifact_sha256"] = "0" * 64
    receipt["audit_identity"]["context_sha256"] = hashlib.sha256(context.read_bytes()).hexdigest()
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="receipt_identity_mismatch"):
        storage.read_migrated_audit_csv(path)


@pytest.mark.parametrize("family", sorted(storage.LEGACY_AUDIT_CSV_FAMILIES))
def test_lossless_original_path_restore_evidence_and_context(tmp_path, family, monkeypatch):
    from backend import app as app_module

    path, original, context = audit_csv(tmp_path, family)
    context_raw = context.read_bytes()
    identity = migration.identity(path)
    settings = SimpleNamespace(project_root=tmp_path)
    monkeypatch.setattr(app_module, "settings", settings)
    evidence = _file_evidence(settings, path)
    token = app_module._file_content_cache_token(path)
    result = compress(tmp_path)
    stored, receipt_path = storage.migration_paths(path)
    assert stored.name.endswith(".csv.legacy.gz")
    assert not path.exists() and storage.artifact_exists(path)
    assert storage.read_original_artifact_bytes(path) == original == gzip.decompress(stored.read_bytes())
    assert storage.read_original_artifact_bytes(stored) == stored.read_bytes()
    assert content_hash(path) == hashlib.sha256(original).hexdigest()
    assert _file_evidence(settings, path) == evidence
    assert app_module._file_content_cache_token(path) == token
    assert app_module._path_cache_token(path) != "missing"
    assert app_module._resolve_existing_report_artifact_path(str(path.relative_to(tmp_path))) == path
    actual = pd.read_csv(io.BytesIO(storage.read_original_artifact_bytes(path)), dtype={"identifier": str})
    expected = pd.read_csv(io.BytesIO(original), dtype={"identifier": str})
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.iloc[0]["identifier"] == "000001" and pd.isna(actual.iloc[0]["missing_vs_zero"])
    assert actual.iloc[1]["missing_vs_zero"] == 0 and actual.iloc[1]["text,quoted"] == "first\nsecond"
    assert context.read_bytes() == context_raw
    receipt = json.loads(receipt_path.read_text())
    assert receipt["audit_identity"]["context_sha256"] == hashlib.sha256(context_raw).hexdigest()
    assert "task_identity" not in receipt and "status" not in receipt["audit_identity"]
    audit = RecoveryAudit(tmp_path)
    assert audit.bind(path, hashlib.sha256(original).hexdigest()) == path
    assert set(audit.files) == {str(stored), str(receipt_path), str(context)}
    audit.assert_stable()
    destination = tmp_path / "restored.csv"
    assert migration.restore_legacy_audit_csv(path, destination)["full_restore_verified"]
    assert destination.read_bytes() == original
    restored = migration.identity(destination)
    assert all(restored[key] == identity[key] for key in ("size", "mtime_ns", "mode", "uid", "gid"))
    assert result["estimated_net_file_allocation_reduction_bytes"] > 0
    assert result["net_reclaimed_disk_bytes"] is None
    with pytest.raises(FileExistsError):
        migration.restore_legacy_audit_csv(path, destination)
    cache_before = app_module._path_cache_token(path)
    context.write_bytes(context_raw + b"\n")
    assert app_module._path_cache_token(path) != cache_before
    with pytest.raises(ValueError, match="identity_mismatch"):
        storage.read_original_artifact_bytes(path)
    with pytest.raises(ValueError):
        audit.assert_stable()


@pytest.mark.parametrize("damage", ["truncated", "missing_receipt", "wrong_sha", "wrong_identity", "duplicate_key", "missing_context"])
def test_corruption_never_bypasses_validation_through_explicit_gzip(tmp_path, damage):
    path, _, context = audit_csv(tmp_path)
    compress(tmp_path)
    stored, receipt = storage.migration_paths(path)
    value = json.loads(receipt.read_text())
    if damage == "truncated":
        stored.write_bytes(stored.read_bytes()[:-3])
    elif damage == "missing_receipt":
        receipt.unlink()
    elif damage == "missing_context":
        context.unlink()
    elif damage == "duplicate_key":
        receipt.write_text(receipt.read_text().replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'))
    else:
        if damage == "wrong_sha":
            value["original_sha256"] = "0" * 64
        else:
            value["audit_identity"]["run_id"] = "different_run"
        receipt.write_text(json.dumps(value))
    for current in (path, stored):
        with pytest.raises((OSError, ValueError)):
            storage.read_original_artifact_bytes(current)
    assert not storage.artifact_exists(path)


@pytest.mark.parametrize("protected", ["recent", "recent_context", "latest", "shared", "symlink",
                                      "no_context", "identity", "native_counterpart", "run_local", "unknown_family"])
def test_active_shared_unbound_and_out_of_scope_files_are_kept(tmp_path, protected):
    path, original, context = audit_csv(tmp_path, latest=protected != "latest")
    if protected == "recent":
        os.utime(path, None)
    elif protected == "recent_context":
        os.utime(context, None)
    elif protected == "shared":
        os.link(path, tmp_path / "shared.csv")
    elif protected == "symlink":
        target = tmp_path / "target.csv"
        path.rename(target)
        path.symlink_to(target)
    elif protected == "no_context":
        context.unlink()
    elif protected == "identity":
        value = json.loads(context.read_text())
        value["run_id"] = "wrong_run"
        context.write_text(json.dumps(value))
        os.utime(context, (time.time() - 3 * 86400,) * 2)
    elif protected == "native_counterpart":
        Path(f"{path}.gz").write_bytes(gzip.compress(original))
    elif protected in {"run_local", "unknown_family"}:
        directory = tmp_path / ("reports/runs/example/audit/score_dedup_shadow" if protected == "run_local"
                                else "reports/audit/unknown_family")
        directory.mkdir(parents=True)
        path = path.rename(directory / path.name)
    assert migration.plan_legacy_audit_csv_compression(tmp_path)["candidate_count"] == 0
    assert path.read_bytes() == original


@pytest.mark.parametrize("independent_timestamps", [False, True])
@pytest.mark.parametrize("failure", ["restore", "unlink", "source_changed", "context_changed", "conflict"])
def test_interruption_keeps_original_and_resume_revalidates_context(tmp_path, monkeypatch, failure, independent_timestamps):
    from backend import legacy_task_audit_compression as transaction

    path, original, context = independent_timestamp_csv(tmp_path) if independent_timestamps else audit_csv(tmp_path)
    plan = migration.plan_legacy_audit_csv_compression(tmp_path)
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
    elif failure == "context_changed":
        context.write_bytes(context.read_bytes() + b"\n")
        os.utime(context, (time.time() - 3 * 86400,) * 2)
    else:
        storage.migration_paths(path)[0].write_bytes(b"existing unrelated data")
    result = migration.apply_legacy_audit_csv_plan(tmp_path, plan)
    assert result["failed_count"] == 1 and result["removed_count"] == 0
    assert path.read_bytes() == original + (b"\n" if failure == "source_changed" else b"")
    if failure in {"restore", "unlink"}:
        monkeypatch.undo()
        assert migration.apply_legacy_audit_csv_plan(tmp_path, plan)["compressed_count"] == 1


def test_budgets_duplicates_and_unsafe_plans_never_remove_source(tmp_path):
    path, original, _ = audit_csv(tmp_path)
    assert migration.plan_legacy_audit_csv_compression(tmp_path, max_input_bytes=len(original) - 1)["candidate_count"] == 0
    with pytest.raises(ValueError, match="budget"):
        migration.plan_legacy_audit_csv_compression(tmp_path, max_files=257)
    plan = migration.plan_legacy_audit_csv_compression(tmp_path)
    plan["candidates"] *= 2
    with pytest.raises(ValueError, match="duplicate"):
        migration.apply_legacy_audit_csv_plan(tmp_path, plan)
    plan["candidates"] = plan["candidates"][:1]
    plan["candidates"][0]["path"] = "../" + plan["candidates"][0]["path"]
    with pytest.raises(ValueError, match="scope"):
        migration.apply_legacy_audit_csv_plan(tmp_path, plan)
    assert path.read_bytes() == original
    compress(tmp_path)
    with pytest.raises(ValueError, match="budget"):
        storage.read_original_artifact_bytes(path, max_bytes=100)


@pytest.mark.parametrize("independent_timestamps", [False, True])
def test_downloads_keep_csv_bytes_mime_filename_etag_and_source(tmp_path, independent_timestamps):
    path, original, _ = independent_timestamp_csv(tmp_path) if independent_timestamps else audit_csv(tmp_path)
    compress(tmp_path)
    app = Flask(__name__)
    callbacks = {name: lambda *a, **kw: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    callbacks["build_project_file_source_headers"] = lambda *a, **kw: {"X-Fund-ANA-Source": "project_file_artifact_binary"}
    register_reports_routes(app, **callbacks)
    original_url = "/api/files/" + str(path.relative_to(tmp_path))
    stored, receipt = storage.migration_paths(path)
    gzip_url = "/api/files/" + str(stored.relative_to(tmp_path))
    with app.test_client() as client:
        for url, expected, mime in ((original_url, original, "text/csv"),
                                    (gzip_url, stored.read_bytes(), "application/gzip")):
            response = client.get(url + "?download=1")
            assert response.status_code == 200 and response.data == expected
            assert response.headers["Content-Type"].startswith(mime)
            assert (stored.name if url == gzip_url else path.name) in response.headers["Content-Disposition"]
            assert response.headers["X-Fund-ANA-Original-SHA256"] == hashlib.sha256(original).hexdigest()
            assert response.headers["X-Fund-ANA-Source"] == "project_file_artifact_binary"
            assert client.get(url, headers={"If-None-Match": response.headers["ETag"]}).status_code == 304
        receipt.unlink()
        assert client.get(original_url).status_code == 404
        assert client.get(gzip_url).status_code == 404
