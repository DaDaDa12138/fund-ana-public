"""Shared audit storage preserves exact CSV bytes, aliases and failure evidence."""
from datetime import datetime
import gzip
import hashlib
import inspect
import json
import mmap
import os
from pathlib import Path
from types import SimpleNamespace
import time

from flask import Flask
import pytest

from backend import file_usage
from backend import shared_audit_csv_compression as storage
from backend.api_routes.reports import register_reports_routes
from backend.legacy_audit_csv_compression import restore_legacy_audit_csv
from backend.maintenance_backup_recovery import RecoveryAudit
from backend.replay_evidence import content_hash
from backend.task_audit_storage import artifact_exists, migration_paths, read_migrated_audit_csv, read_original_artifact_bytes


@pytest.fixture(autouse=True)
def owned_process_metadata(monkeypatch):
    # Landlock denies access to protected parent metadata. Unit cases inspect
    # real descriptors/maps owned by this worker; the real operator checks all.
    monkeypatch.setattr(file_usage, "_process_paths", lambda: [Path("/proc") / str(os.getpid())])


def shared_csv(root, count=2):
    directory = root / "reports/audit/score_dedup_shadow"
    directory.mkdir(parents=True)
    # Storage fixtures only. Never used as fund observations or production data.
    raw = b'\xef\xbb\xbfidentifier,text,missing,zero\r\n' + (
        b'000001,"synthetic,quote",,0\r\n000002,"first\nsecond",,-1.25\r\n' * 1200)
    paths, contexts = [], []
    old = time.time() - 3 * 86400
    for number in range(count):
        prefix = f"score_dedup_shadow_20260801_020000_sync_20260801_0300{number:02d}"
        path = directory / (prefix + "_samples.csv")
        if paths:
            os.link(paths[0], path)
        else:
            path.write_bytes(raw)
            path.chmod(0o640)
            os.utime(path, (old, old))
        context = directory / (prefix + ".json")
        context.write_text(json.dumps({"run_id": "20260801_020000_sync", "generated_at": f"2026-08-01T03:00:{number:02d}",
                                      "status": "failed", "passed": False, "unknown_count": None, "real_zero": 0,
                                      "artifacts": {"samples": str(path.relative_to(root))}}))
        os.utime(context, (old, old))
        paths.append(path)
        contexts.append(context)
    (directory / "score_dedup_shadow_20260802_020000_sync_20260802_030000.json").write_text(
        '{"run_id":"20260802_020000_sync","generated_at":"2026-08-02T03:00:00"}')
    return paths, raw, contexts


def compress(root):
    plan = storage.plan_shared_audit_csv_compression(root)
    assert plan["candidate_group_count"] == 1, plan
    result = storage.apply_shared_audit_csv_plan(root, plan)
    assert result["status"] == "complete" and not result["errors"], result
    assert result["compressed_group_count"] == 1
    return result


@pytest.mark.parametrize("count", [2, 3])
def test_all_aliases_keep_exact_data_metadata_context_and_existing_readers(tmp_path, count):
    paths, raw, contexts = shared_csv(tmp_path, count)
    original = paths[0].stat()
    context_bytes = {path: path.read_bytes() for path in contexts}
    result = compress(tmp_path)
    assert result["removed_count"] == count
    assert result["outcomes"][0]["independent_restore_verified"]
    assert result["completed_file_allocation_reduction_bytes"] > 0
    digest = hashlib.sha256(raw).hexdigest()
    files = []
    audit = RecoveryAudit(tmp_path)
    for number, path in enumerate(paths):
        assert not path.exists() and artifact_exists(path)
        assert read_original_artifact_bytes(path) == raw
        restored, _, receipt = read_migrated_audit_csv(path)
        assert restored == raw and receipt["original_sha256"] == digest
        assert receipt["shared_source_paths"] == [str(item.relative_to(tmp_path)) for item in paths]
        assert receipt["original_identity"]["links"] == count
        assert content_hash(path) == digest
        audit.bind(path, digest)
        destination = tmp_path / "restored" / str(number)
        assert restore_legacy_audit_csv(path, destination)["full_restore_verified"]
        recovered = destination.stat()
        assert destination.read_bytes() == raw
        assert (recovered.st_mode, recovered.st_uid, recovered.st_gid, recovered.st_mtime_ns) == (
            original.st_mode, original.st_uid, original.st_gid, original.st_mtime_ns)
        files.append(migration_paths(path)[0])
    assert len({(path.stat().st_dev, path.stat().st_ino) for path in files}) == 1
    assert files[0].stat().st_nlink == count
    assert {path: path.read_bytes() for path in contexts} == context_bytes
    assert json.loads(contexts[0].read_bytes())["unknown_count"] is None
    assert json.loads(contexts[0].read_bytes())["passed"] is False
    audit.assert_stable()
    assert storage.plan_shared_audit_csv_compression(tmp_path)["candidate_count"] == 0


@pytest.mark.parametrize("failure", ["second_unlink", "first_receipt", "second_receipt", "restore"])
def test_interrupted_delivery_and_partial_removal_resume_from_a_fresh_plan(tmp_path, monkeypatch, failure):
    paths, raw, contexts = shared_csv(tmp_path)
    expected_context = [path.read_bytes() for path in contexts]
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    with monkeypatch.context() as patch:
        if failure == "second_unlink":
            unlink = Path.unlink
            def fail(path, *args, **kwargs):
                if path == paths[1]:
                    raise OSError("synthetic unlink interruption")
                return unlink(path, *args, **kwargs)
            patch.setattr(Path, "unlink", fail)
        elif failure == "restore":
            def fail(*args):
                raise OSError("synthetic restore interruption")
            patch.setattr(storage, "_independent_restore", fail)
        else:
            install = storage._install
            target = migration_paths(paths[0 if failure == "first_receipt" else 1])[1]
            def fail(path, *args):
                if path == target:
                    raise OSError("synthetic receipt interruption")
                return install(path, *args)
            patch.setattr(storage, "_install", fail)
        result = storage.apply_shared_audit_csv_plan(tmp_path, plan)
    assert result["status"] == "failed" and result["errors"]
    assert result["removed_count"] == (1 if failure == "second_unlink" else 0)
    assert all(read_original_artifact_bytes(path) == raw for path in paths)
    resumed = storage.plan_shared_audit_csv_compression(tmp_path)
    assert resumed["candidate_group_count"] == 1, resumed
    final = storage.apply_shared_audit_csv_plan(tmp_path, resumed)
    assert final["status"] == "complete" and not final["errors"], final
    assert result["removed_count"] + final["removed_count"] == 2
    assert all(not path.exists() and read_original_artifact_bytes(path) == raw for path in paths)
    assert migration_paths(paths[0])[0].samefile(migration_paths(paths[1])[0])
    assert [path.read_bytes() for path in contexts] == expected_context
    again = storage.apply_shared_audit_csv_plan(tmp_path, resumed)
    assert again["removed_count"] == 0 and again["outcomes"][0]["status"] == "already_stored"


@pytest.mark.parametrize("protection", ["outside_link", "too_many_links", "recent", "context_missing", "context_changed", "native_gzip", "symlink", "small"])
def test_unknown_or_unready_groups_keep_all_plain_sources(tmp_path, protection):
    paths, raw, contexts = shared_csv(tmp_path, storage.MAX_GROUP_FILES + 1 if protection == "too_many_links" else 2)
    if protection == "outside_link":
        os.link(paths[0], tmp_path / "outside.csv")
    elif protection == "recent":
        os.utime(paths[0], None)
    elif protection == "context_missing":
        contexts[1].unlink()
    elif protection == "context_changed":
        contexts[1].write_text('{"run_id":"wrong","generated_at":"2026-08-01T03:00:01"}')
        os.utime(contexts[1], (time.time() - 3 * 86400,) * 2)
    elif protection == "native_gzip":
        Path(str(paths[1]) + ".gz").write_bytes(gzip.compress(raw))
    elif protection == "symlink":
        paths[1].unlink()
        paths[1].symlink_to(paths[0])
    elif protection == "small":
        paths[0].write_bytes(b"small\n")
        os.utime(paths[0], (time.time() - 3 * 86400,) * 2)
    result = storage.plan_shared_audit_csv_compression(tmp_path)
    assert result["candidate_count"] == 0, result
    assert all(path.read_bytes() == (b"small\n" if protection == "small" else raw) for path in paths)
    assert all(not migration_paths(path)[0].exists() for path in paths)


@pytest.mark.parametrize("change", ["new_link", "bytes", "context", "missing_alias", "replaced_alias"])
def test_changed_sources_or_context_between_plan_and_apply_are_preserved(tmp_path, change):
    paths, raw, contexts = shared_csv(tmp_path)
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    if change == "new_link":
        os.link(paths[0], tmp_path / "new.csv")
    elif change == "bytes":
        paths[0].write_bytes(raw + b"changed")
    elif change == "context":
        contexts[1].write_bytes(contexts[1].read_bytes() + b"\n")
        os.utime(contexts[1], (time.time() - 3 * 86400,) * 2)
    elif change == "missing_alias":
        paths[1].unlink()
    else:
        paths[1].unlink()
        paths[1].write_bytes(raw)
    before = {path: path.read_bytes() for path in paths if path.exists()}
    result = storage.apply_shared_audit_csv_plan(tmp_path, plan)
    assert result["status"] == "failed" and result["removed_count"] == 0
    assert {path: path.read_bytes() for path in paths if path.exists()} == before


@pytest.mark.parametrize("kind", ["fd", "mmap"])
def test_open_or_mapped_source_blocks_retirement(tmp_path, kind):
    paths, raw, _ = shared_csv(tmp_path)
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    with paths[0].open("rb") as stream:
        mapped = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) if kind == "mmap" else None
        try:
            if mapped is not None:
                stream.close()
            result = storage.apply_shared_audit_csv_plan(tmp_path, plan)
        finally:
            if mapped is not None:
                mapped.close()
    assert result["status"] == "failed" and result["removed_count"] == 0
    # CPython may keep an internal duplicate FD for a mmap after the caller's
    # FD is closed; either kernel identity check must protect the source.
    assert any(reason in str(result["errors"]) for reason in ("file_open", "file_mapped"))
    assert all(path.read_bytes() == raw for path in paths)


@pytest.mark.parametrize("target", ["gzip", "receipt", "context"])
def test_storage_mutation_after_verification_prevents_original_deletion(tmp_path, monkeypatch, target):
    paths, raw, contexts = shared_csv(tmp_path)
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    check = storage.assert_files_inactive
    calls = 0
    def change_after_check(entries):
        nonlocal calls
        calls += 1
        check(entries)
        if calls == 2:
            changed = contexts[0] if target == "context" else migration_paths(paths[0])[0 if target == "gzip" else 1]
            changed.write_bytes(changed.read_bytes() + b"corrupt")
    monkeypatch.setattr(storage, "assert_files_inactive", change_after_check)
    result = storage.apply_shared_audit_csv_plan(tmp_path, plan)
    assert result["status"] == "failed" and result["removed_count"] == 0
    assert all(path.read_bytes() == raw for path in paths)


def test_new_link_during_last_unlink_is_not_counted_as_reclaimed(tmp_path, monkeypatch):
    paths, raw, _ = shared_csv(tmp_path)
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    unlink = Path.unlink
    def add_link(path, *args, **kwargs):
        if path == paths[-1]:
            os.link(path, tmp_path / "unexpected.csv")
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", add_link)
    result = storage.apply_shared_audit_csv_plan(tmp_path, plan)
    assert result["status"] == "failed" and result["removed_count"] == 2
    assert result["completed_file_allocation_reduction_bytes"] == 0
    assert result["outcomes"][0]["estimated_net_file_allocation_reduction_bytes"] is None
    assert all(read_original_artifact_bytes(path) == raw for path in paths)
    assert (tmp_path / "unexpected.csv").read_bytes() == raw


def test_plan_limits_charge_shared_data_once_and_reject_unsafe_or_duplicate_groups(tmp_path):
    paths, raw, _ = shared_csv(tmp_path)
    assert storage.plan_shared_audit_csv_compression(tmp_path, max_input_bytes=len(raw))["candidate_count"] == 2
    assert storage.plan_shared_audit_csv_compression(tmp_path, max_input_bytes=len(raw)-1)["candidate_count"] == 0
    assert storage.plan_shared_audit_csv_compression(tmp_path, max_files=1)["candidate_count"] == 0
    with pytest.raises(ValueError, match="budget"):
        storage.plan_shared_audit_csv_compression(tmp_path, max_files=257)
    plan = storage.plan_shared_audit_csv_compression(tmp_path)
    plan["candidates"] *= 2
    with pytest.raises(ValueError, match="duplicate"):
        storage.apply_shared_audit_csv_plan(tmp_path, plan)
    plan["candidates"] = plan["candidates"][:1]
    plan["candidates"][0]["paths"][0] = "../outside.csv"
    with pytest.raises(ValueError, match="path"):
        storage.apply_shared_audit_csv_plan(tmp_path, plan)
    assert all(path.read_bytes() == raw for path in paths)


def test_every_download_alias_keeps_bytes_mime_filename_sha_and_failure_semantics(tmp_path):
    paths, raw, _ = shared_csv(tmp_path)
    compress(tmp_path)
    app = Flask(__name__)
    callbacks = {name: lambda *args, **kwargs: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    callbacks["build_project_file_source_headers"] = lambda *args, **kwargs: {"X-Fund-ANA-Source": "project_file_artifact_binary"}
    register_reports_routes(app, **callbacks)
    with app.test_client() as client:
        for path in paths:
            stored, receipt = migration_paths(path)
            for target, expected, mime in ((path, raw, "text/csv"), (stored, stored.read_bytes(), "application/gzip")):
                url = "/api/files/" + str(target.relative_to(tmp_path))
                response = client.get(url + "?download=1")
                assert response.status_code == 200 and response.data == expected
                assert response.headers["Content-Type"].startswith(mime)
                assert target.name in response.headers["Content-Disposition"]
                assert response.headers["X-Fund-ANA-Original-SHA256"] == hashlib.sha256(raw).hexdigest()
                assert client.get(url, headers={"If-None-Match":response.headers["ETag"]}).status_code == 304
        migration_paths(paths[0])[1].unlink()
        assert client.get("/api/files/" + str(paths[0].relative_to(tmp_path))).status_code == 404
        assert client.get("/api/files/" + str(paths[1].relative_to(tmp_path))).data == raw


def test_cleanup_passes_apply_and_reports_shared_group_results(tmp_path, monkeypatch):
    from scripts import cleanup_disk_usage

    calls = []
    expected = {"status":"complete", "errors":[], "scan_complete":True, "candidate_group_count":1,
                "compressed_group_count":1, "removed_count":2, "completed_file_allocation_reduction_bytes":1234}
    def run(settings, *, apply):
        calls.append((settings.project_root, apply))
        return expected
    monkeypatch.setattr(storage, "run_shared_audit_csv_compression", run)
    settings = SimpleNamespace(project_root=tmp_path, core_root=tmp_path/"core", reports_dir=tmp_path/"reports",
                               archived_runs_dir=tmp_path/"reports/archived", scheduler_weekly_disk_cleanup_core_csv_enabled=False)
    report = cleanup_disk_usage.run_cleanup(settings, apply=True, now=datetime.now())
    assert calls == [(tmp_path, True)]
    assert report.to_dict()["shared_audit_csv_compression"] == expected


def test_dry_run_and_busy_workflow_do_not_delete_or_create_storage(tmp_path, monkeypatch):
    paths, raw, _ = shared_csv(tmp_path)
    settings = SimpleNamespace(project_root=tmp_path, state_file=tmp_path/"state.json")
    assert storage.run_shared_audit_csv_compression(settings)["status"] == "preview"
    assert not settings.state_file.exists()
    class BusyStore:
        def __init__(self, *args):
            from threading import Lock
            self._lock = Lock()
        def _acquire_run_lock_locked(self, *args):
            raise storage.WorkflowLockBusyError("synthetic busy workflow")
        def close(self):
            pass
    monkeypatch.setattr(storage, "WorkflowStore", BusyStore)
    result = storage.run_shared_audit_csv_compression(settings, apply=True)
    assert result["status"] == "deferred" and result["reason"] == "workflow_lock_busy"
    assert all(path.read_bytes() == raw and not migration_paths(path)[0].exists() for path in paths)
