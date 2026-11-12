"""Artifact contracts use non-business bytes; no fund data or production paths."""
import hashlib
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import time
from types import SimpleNamespace

from flask import Flask
import pytest

from backend import report_artifact_storage as storage


CONTENT = b"artifact contract bytes; not fund observations\n" * 7000
STAMP = 1_700_000_000_123_456_789


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / "project"
    reports = root / "reports"
    reports.mkdir(parents=True)
    return SimpleNamespace(project_root=root, reports_dir=reports, runs_dir=reports / "runs",
        archived_runs_dir=reports / "archive", core_root=root / "core", state_file=root / "state.json",
        tiantian_base_url="http://disabled.invalid/tiantian", autostock_base_url="http://disabled.invalid/autostock")


def artifact(path, content=CONTENT):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    path.chmod(0o640)
    os.utime(path, ns=(STAMP, STAMP))
    return path


def report(settings, run_id, *, content=CONTENT, revision=False):
    root = settings.runs_dir / run_id
    if revision:
        root = root / "revisions" / "contract-revision"
    path = root / f"bond_analysis_report_{run_id}.json"
    output = artifact(root / "core_artifacts" / "结果.csv", content)
    payload = {"run_id": run_id, "artifacts": {
        "report_json": str(path.relative_to(settings.project_root)),
        "report_markdown": str(path.with_suffix(".md").relative_to(settings.project_root)),
    }}
    path.write_text(json.dumps(payload, ensure_ascii=False))
    return payload, output


def pool_objects(settings):
    return list((settings.reports_dir / storage.POOL_NAME).glob("[0-9a-f]*"))


def download_client(settings):
    from backend.api_routes.reports import register_reports_routes

    app = Flask(__name__)
    options = {name: lambda *args, **kwargs: {} for name in inspect.signature(register_reports_routes).parameters
               if name != "app"}
    options["resolve_project_file"] = lambda name: settings.project_root / name
    register_reports_routes(app, **options)
    return app.test_client()


@pytest.mark.parametrize("revision", [False, True])
def test_cross_run_alias_sharing_preserves_full_download_contract_and_independent_reads(settings, revision):
    first_report, first = report(settings, "first", revision=revision)
    alias = first.with_name("alias.xlsx")
    shutil.copy2(first, alias)
    second_report, second = report(settings, "second", revision=revision)
    before = second.stat()
    client = download_client(settings)
    url = "/api/files/" + second.relative_to(settings.project_root).as_posix()
    with client.get(url) as response:
        assert response.status_code == 200 and response.data == CONTENT
        headers = {key: response.headers.get(key) for key in (
            "ETag", "Last-Modified", "Content-Length", "Content-Disposition", "Content-Type")}
    first_result = storage.share_finalized_report(settings, first_report)
    second_result = storage.share_finalized_report(settings, second_report)
    assert first_result["shared_files"] == second_result["shared_files"] == 1
    assert second_result["removed_allocated_bytes_estimate"] == before.st_blocks * 512
    assert first.samefile(alias) and first.samefile(second)
    after = second.stat()
    assert (before.st_size, before.st_mode, before.st_uid, before.st_gid, before.st_mtime_ns) == (
        after.st_size, after.st_mode, after.st_uid, after.st_gid, after.st_mtime_ns)
    with client.get(url) as response:
        assert response.data == CONTENT
        assert {key: response.headers.get(key) for key in headers} == headers
    with client.get(url, headers={"If-None-Match": headers["ETag"]}) as response:
        assert response.status_code == 304
    with client.get(url, headers={"Range": "bytes=11-35"}) as response:
        assert response.status_code == 206 and response.data == CONTENT[11:36]
    restored = settings.project_root / "independent.csv"
    shutil.copy2(second, restored)
    assert restored.read_bytes() == CONTENT and not restored.samefile(second)
    shutil.rmtree(settings.runs_dir / "first")
    shutil.rmtree(settings.reports_dir / storage.POOL_NAME)
    assert second.read_bytes() == restored.read_bytes() == CONTENT


@pytest.mark.parametrize("difference", ["mtime", "mode", "bytes"])
def test_identical_looking_files_require_all_content_and_metadata_to_match(settings, difference):
    first_report, first = report(settings, "first")
    second_report, second = report(settings, "second")
    if difference == "mtime":
        os.utime(second, ns=(STAMP, STAMP + 1))
    elif difference == "mode":
        second.chmod(0o600)
    else:
        artifact(second, b"Z" + CONTENT[1:])
    expected = second.read_bytes()
    storage.share_finalized_report(settings, first_report)
    result = storage.share_finalized_report(settings, second_report)
    assert result["shared_files"] == 0 and not first.samefile(second)
    assert second.read_bytes() == expected


def test_post_materialization_and_alias_writes_are_detached_from_shared_history(settings):
    from backend.report_builder import _materialize_run_local_core_artifacts, _normalize_lightweight_run_local_artifacts

    old_report, old = report(settings, "first")
    storage.share_finalized_report(settings, old_report)
    current = settings.runs_dir / "second"
    localized = _materialize_run_local_core_artifacts(settings=settings, run_dir=current,
        artifacts={"long_csv": str(old.relative_to(settings.project_root))})
    fresh = settings.project_root / localized["long_csv"]
    assert not fresh.samefile(old)
    fresh.write_bytes(b"new writer output")
    assert old.read_bytes() == CONTENT
    dated = artifact(current / "core_artifacts" / "output_20260912_010101.csv")
    localized = _normalize_lightweight_run_local_artifacts(settings=settings, run_id="20260913_010101_sync",
        run_dir=current, localized_artifacts={"long_csv": str(dated.relative_to(settings.project_root))})
    renamed = settings.project_root / localized["long_csv"]
    renamed.write_bytes(b"post-copy annotation")
    assert dated.read_bytes() == old.read_bytes() == CONTENT


def test_mutable_sources_control_json_unreferenced_root_files_and_symlinks_are_excluded(settings):
    payload, selected = report(settings, "run")
    untouched = [artifact(settings.reports_dir / "core_output" / "mutable.csv"),
                 artifact(selected.parent.parent / "publication_state.json"),
                 artifact(selected.parent.parent / "logs" / "task.json"),
                 artifact(selected.parent.parent / "tracker.xlsx"),
                 artifact(selected.with_suffix(".jsonl")),
                 artifact(selected.with_suffix(".json.gz")),
                 artifact(selected.parent / "small.csv", b"small")]
    linked = selected.parent / "link.csv"
    linked.symlink_to(untouched[0])
    (selected.parent / "linked_directory").symlink_to(untouched[0].parent, target_is_directory=True)
    identities = {path: storage._identity(path.stat()) for path in untouched}
    result = storage.share_finalized_report(settings, payload)
    assert len(result["files"]) == 1
    assert identities == {path: storage._identity(path.stat()) for path in untouched}
    assert linked.is_symlink() and not selected.samefile(untouched[0])


@pytest.mark.parametrize("location", ["core_json", "nested_json", "nested_csv", "root_csv", "root_xlsx"])
def test_extended_final_files_preserve_original_json_bytes_downloads_and_independent_reads(settings, location):
    content = json.dumps({"scope": "synthetic storage contract, not fund data",
                          "values": [{"missing": None, "zero": 0, "status": "unknown", "text": "原始字符"}] * 7000},
                         ensure_ascii=False, indent=2).encode()
    outputs = []
    payloads = []
    for run_id in ("first", "second"):
        payload, default = report(settings, run_id, revision=True)
        relative = {"core_json": "core_artifacts/metadata.json",
                    "nested_json": "core_artifacts/.audit/validation/result.json",
                    "nested_csv": "core_artifacts/nested/result.csv",
                    "root_csv": "listed.csv", "root_xlsx": "listed.xlsx"}[location]
        output = artifact(default.parent.parent / relative, content)
        if location.startswith("root_"):
            payload["artifacts"]["listed"] = str(output.relative_to(settings.project_root))
            payload["artifacts"]["listed_alias"] = payload["artifacts"]["listed"]
            (settings.project_root / payload["artifacts"]["report_json"]).write_text(json.dumps(payload))
        outputs.append(output)
        payloads.append(payload)
    first, second = outputs
    before = second.stat()
    client = download_client(settings)
    url = "/api/files/" + second.relative_to(settings.project_root).as_posix()
    with client.get(url) as response:
        assert response.status_code == 200 and response.data == content
        headers = {key: response.headers.get(key) for key in (
            "ETag", "Last-Modified", "Content-Length", "Content-Disposition", "Content-Type")}
    for payload in payloads:
        outcome = storage.share_finalized_report(settings, payload)
        assert outcome["status"] == "complete"
    assert first.samefile(second)
    assert sum(row["path"] == second.relative_to(settings.reports_dir).as_posix() for row in outcome["files"]) == 1
    after = second.stat()
    assert (before.st_size, before.st_mode, before.st_uid, before.st_gid, before.st_mtime_ns) == (
        after.st_size, after.st_mode, after.st_uid, after.st_gid, after.st_mtime_ns)
    with client.get(url) as response:
        assert response.data == content and {key: response.headers.get(key) for key in headers} == headers
    with client.get(url, headers={"If-None-Match": headers["ETag"]}) as response:
        assert response.status_code == 304
    with client.get(url, headers={"Range": "bytes=11-35"}) as response:
        assert response.status_code == 206 and response.data == content[11:36]
    restored = settings.project_root / ("independent" + second.suffix)
    shutil.copy2(second, restored)
    assert not restored.samefile(second) and restored.read_bytes() == content
    shutil.rmtree(settings.runs_dir / "first")
    shutil.rmtree(settings.reports_dir / storage.POOL_NAME)
    assert second.read_bytes() == content and json.loads(second.read_bytes()) == json.loads(content)


def test_nested_json_materialization_and_rename_detach_from_previous_report(settings):
    from backend.report_builder import _materialize_run_local_core_artifacts, _normalize_lightweight_run_local_artifacts

    payload, default = report(settings, "old")
    old = artifact(default.parent / ".audit/validation/result_20260912_010101.json")
    storage.share_finalized_report(settings, payload)
    current = settings.runs_dir / "20260913_010101_sync"
    localized = _materialize_run_local_core_artifacts(settings=settings, run_dir=current, artifacts={
        "core_output_dir": str(default.parent), "autostock_validation_audit_file": str(old)})
    fresh = settings.project_root / localized["autostock_validation_audit_file"]
    assert fresh.parent.relative_to(current).as_posix() == "core_artifacts/.audit/validation"
    assert not fresh.samefile(old)
    normalized = _normalize_lightweight_run_local_artifacts(settings=settings, run_id=current.name,
        run_dir=current, localized_artifacts=localized)
    renamed = settings.project_root / normalized["autostock_validation_audit_file"]
    assert not renamed.samefile(fresh) and not renamed.samefile(old)
    renamed.write_bytes(b"later annotation belongs to the new copy")
    assert fresh.read_bytes() == old.read_bytes() == CONTENT


def test_nested_traversal_uses_one_budget_including_ignored_entries(settings, monkeypatch):
    payload, default = report(settings, "run")
    for index in range(3):
        artifact(default.parent / f"branch_{index}" / "deep" / "result.json")
        artifact(default.parent / f"ignored_{index}.log", b"not eligible")
    monkeypatch.setattr(storage, "MAX_RUN_FILES", 4)
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "partial" and result["run_budget_exhausted"]
    assert not any("result.json" in row["path"] for row in result["files"])
    assert all(path.stat().st_nlink == 1 for path in default.parent.glob("branch_*/deep/result.json"))


def test_depth_budget_preserves_unvisited_nested_files(settings, monkeypatch):
    payload, default = report(settings, "run")
    shallow = artifact(default.parent / "level_one" / "shallow.json")
    deep = artifact(default.parent / "level_one" / "level_two" / "deep.json")
    before = storage._identity(deep.stat())
    monkeypatch.setattr(storage, "MAX_CORE_DIRECTORY_DEPTH", 1)
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "partial" and result["run_budget_exhausted"]
    assert any(row["path"].endswith("shallow.json") for row in result["files"])
    assert not any(row["path"].endswith("deep.json") for row in result["files"])
    assert storage._identity(deep.stat()) == before and shallow.read_bytes() == deep.read_bytes() == CONTENT


def test_nested_and_root_files_share_the_existing_run_byte_budget(settings, monkeypatch):
    payload, default = report(settings, "run")
    nested = artifact(default.parent / ".audit/validation/result.json")
    root_file = artifact(default.parent.parent / "listed.xlsx")
    payload["artifacts"]["listed"] = str(root_file)
    (settings.project_root / payload["artifacts"]["report_json"]).write_text(json.dumps(payload))
    monkeypatch.setattr(storage, "MAX_RUN_BYTES", len(CONTENT))
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "partial" and result["run_budget_exhausted"]
    assert len(result["files"]) == 1 and result["files"][0]["path"].endswith("结果.csv")
    assert nested.stat().st_nlink == root_file.stat().st_nlink == 1
    assert nested.read_bytes() == root_file.read_bytes() == CONTENT


def test_referenced_report_outputs_work_without_core_artifacts_directory(settings):
    payload, default = report(settings, "run", revision=True)
    local = artifact(default.parent.parent / "final.xlsx")
    outside = artifact(settings.reports_dir / "global_alias.xlsx")
    payload["artifacts"].update(final_excel=str(local), external_excel=str(outside))
    (settings.project_root / payload["artifacts"]["report_json"]).write_text(json.dumps(payload))
    shutil.rmtree(default.parent)
    original = storage._identity(outside.stat())
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "complete" and len(result["files"]) == 1
    assert local.stat().st_nlink == 2 and storage._identity(outside.stat()) == original


@pytest.mark.parametrize("fault", ["bytes", "mtime"])
def test_corrupt_pool_cannot_replace_a_valid_new_report(settings, fault):
    first_report, _ = report(settings, "first")
    second_report, second = report(settings, "second")
    storage.share_finalized_report(settings, first_report)
    cached = pool_objects(settings)[0]
    if fault == "bytes":
        artifact(cached, b"X" + CONTENT[1:])
    else:
        os.utime(cached, ns=(STAMP, STAMP + 1))
    original = storage._identity(second.stat())
    result = storage.share_finalized_report(settings, second_report)
    assert result["status"] == "partial" and "mismatch" in result["errors"][0]["error"]
    assert second.read_bytes() == CONTENT and storage._identity(second.stat()) == original


def test_late_target_change_aborts_atomic_replacement(settings, monkeypatch):
    first_report, first = report(settings, "first")
    second_report, second = report(settings, "second")
    storage.share_finalized_report(settings, first_report)
    real_link = os.link

    def change_during_link(source, target, **kwargs):
        real_link(source, target, **kwargs)
        if Path(target).name.startswith(".link-"):
            artifact(second, b"Z" + CONTENT[1:])

    monkeypatch.setattr(storage.os, "link", change_during_link)
    result = storage.share_finalized_report(settings, second_report)
    assert result["status"] == "partial" and result["shared_files"] == 0
    assert second.read_bytes() == b"Z" + CONTENT[1:] and first.read_bytes() == CONTENT
    assert not list((settings.reports_dir / storage.POOL_NAME).glob(".link-*"))


@pytest.mark.parametrize("error", [PermissionError("read-only filesystem"), OSError(18, "cross-device link")])
def test_link_failure_preserves_source_and_report(settings, monkeypatch, error):
    payload, path = report(settings, "run")
    before = storage._identity(path.stat())
    monkeypatch.setattr(storage.os, "link", lambda *args, **kwargs: (_ for _ in ()).throw(error))
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "partial" and result["shared_files"] == 0
    assert path.read_bytes() == CONTENT and storage._identity(path.stat()) == before


def test_process_exit_mid_replacement_keeps_all_reports_and_gc_removes_only_crash_link(settings):
    first_report, first = report(settings, "first")
    second_report, second = report(settings, "second")
    storage.share_finalized_report(settings, first_report)

    def interrupted_writer():
        storage.os.replace = lambda *args, **kwargs: os._exit(77)
        storage.share_finalized_report(settings, second_report)

    process = multiprocessing.get_context("fork").Process(target=interrupted_writer)
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 77
    pool = settings.reports_dir / storage.POOL_NAME
    assert len(list(pool.glob(".link-*"))) == 1
    assert first.read_bytes() == second.read_bytes() == CONTENT
    assert not first.samefile(second)
    cleanup = storage.collect_unused_report_artifacts(settings, apply=True, now=time.time() + 2 * 86400)
    assert len(cleanup["removed"]) == 1 and cleanup["removed"][0]["temporary"]
    assert cleanup["removed_allocated_bytes_estimate"] == 0
    retry = storage.share_finalized_report(settings, second_report)
    assert retry["shared_files"] == 1 and first.samefile(second)


@pytest.mark.parametrize("budget", ["count", "bytes"])
def test_pool_admission_evicts_lookup_links_without_removing_report_data(settings, monkeypatch, budget):
    first_report, first = report(settings, "first")
    second_report, second = report(settings, "second", content=b"Z" + CONTENT[1:])
    monkeypatch.setattr(storage, "MAX_POOL_FILES" if budget == "count" else "MAX_POOL_BYTES",
                        1 if budget == "count" else len(CONTENT))
    storage.share_finalized_report(settings, first_report)
    result = storage.share_finalized_report(settings, second_report)
    assert result["status"] == "complete" and len(pool_objects(settings)) == 1
    assert result["files"][0]["evicted_lookup_links"] == 1
    assert first.read_bytes() == CONTENT and second.read_bytes() == b"Z" + CONTENT[1:]


def test_young_orphan_protection_can_skip_admission_without_growing_pool(settings, monkeypatch):
    first_report, first = report(settings, "first")
    second_report, second = report(settings, "second", content=b"Z" + CONTENT[1:])
    monkeypatch.setattr(storage, "MAX_POOL_BYTES", len(CONTENT))
    storage.share_finalized_report(settings, first_report)
    first.unlink()
    result = storage.share_finalized_report(settings, second_report)
    assert result["status"] == "partial" and result["pool_budget_skipped_files"] == 1
    assert result["pool"]["logical_bytes"] == len(CONTENT) and second.read_bytes() == b"Z" + CONTENT[1:]


def test_gc_requires_old_unreferenced_objects_and_dry_run_never_claims_reclamation(settings):
    payload, path = report(settings, "run")
    storage.share_finalized_report(settings, payload)
    assert not storage.collect_unused_report_artifacts(settings, apply=True, now=time.time() + 2 * 86400)["planned"]
    path.unlink()
    assert not storage.collect_unused_report_artifacts(settings, apply=True)["planned"]
    cached = pool_objects(settings)[0]
    before = cached.stat()
    preview = storage.collect_unused_report_artifacts(settings, now=time.time() + 2 * 86400)
    assert len(preview["planned"]) == 1 and not preview["removed"]
    assert preview["removed_allocated_bytes_estimate"] == 0 and cached.exists()
    executed = storage.collect_unused_report_artifacts(settings, apply=True, now=time.time() + 2 * 86400)
    assert len(executed["removed"]) == 1 and executed["removed_allocated_bytes_estimate"] == before.st_blocks * 512
    assert executed["net_reclaimed_disk_bytes"] is None and not cached.exists()


def test_gc_failure_and_batch_limit_preserve_unremoved_entries(settings, monkeypatch):
    for i in range(3):
        payload, path = report(settings, f"run_{i}", content=str(i).encode() + CONTENT[1:])
        storage.share_finalized_report(settings, payload)
        path.unlink()
    monkeypatch.setattr(storage, "MAX_GC_FILES", 1)
    future = time.time() + 2 * 86400
    with monkeypatch.context() as temporary:
        temporary.setattr(Path, "unlink", lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("denied")))
        result = storage.collect_unused_report_artifacts(settings, apply=True, now=future)
    assert result["errors"] and not result["removed"] and len(pool_objects(settings)) == 3
    result = storage.collect_unused_report_artifacts(settings, apply=True, now=future)
    assert len(result["removed"]) == 1 and len(pool_objects(settings)) == 2


@pytest.mark.parametrize("fault", ["report_mismatch", "report_symlink", "pool_symlink", "outside_run", "unknown_pool_file"])
def test_invalid_scope_or_pool_never_changes_artifacts(settings, fault):
    payload, path = report(settings, "run")
    report_path = settings.project_root / payload["artifacts"]["report_json"]
    pool = settings.reports_dir / storage.POOL_NAME
    if fault == "report_mismatch":
        payload["different"] = True
    elif fault == "report_symlink":
        moved = report_path.with_suffix(".old")
        report_path.rename(moved)
        report_path.symlink_to(moved)
    elif fault == "pool_symlink":
        pool.symlink_to(path.parent, target_is_directory=True)
    elif fault == "outside_run":
        payload["run_id"] = "other"
    else:
        pool.mkdir()
        artifact(pool / "unknown.csv")
    before = storage._identity(path.stat())
    with pytest.raises((ValueError, storage.ReportArtifactStorageError)):
        storage.share_finalized_report(settings, payload)
    assert storage._identity(path.stat()) == before and path.read_bytes() == CONTENT


def test_busy_pool_does_not_wait_or_mutate_report(settings):
    payload, path = report(settings, "run")
    pool = settings.reports_dir / storage.POOL_NAME
    with storage._pool_lock(pool, settings.reports_dir):
        with pytest.raises(BlockingIOError):
            storage.share_finalized_report(settings, payload)
    assert path.stat().st_nlink == 1 and path.read_bytes() == CONTENT


def test_run_byte_budget_preserves_unprocessed_files(settings, monkeypatch):
    payload, first = report(settings, "run")
    second = artifact(first.with_name("second.csv"), b"Z" + CONTENT[1:])
    monkeypatch.setattr(storage, "MAX_RUN_BYTES", len(CONTENT))
    result = storage.share_finalized_report(settings, payload)
    assert result["status"] == "partial" and result["run_budget_exhausted"]
    assert len(result["files"]) == 1 and first.read_bytes() == CONTENT and second.read_bytes() == b"Z" + CONTENT[1:]


def test_weekly_cleanup_calls_pool_gc_and_logs_only_leaves_it_untouched(settings):
    from datetime import datetime
    from scripts.cleanup_disk_usage import run_cleanup

    payload, path = report(settings, "run")
    storage.share_finalized_report(settings, payload)
    path.unlink()
    future = datetime.fromtimestamp(time.time() + 2 * 86400)
    logs = run_cleanup(settings, logs_only=True, apply=True, now=future).to_dict()
    assert logs["report_artifact_pool_gc"] is None and len(pool_objects(settings)) == 1
    preview = run_cleanup(settings, apply=False, now=future).to_dict()
    assert len(preview["report_artifact_pool_gc"]["planned"]) == 1
    assert not preview["report_artifact_pool_gc"]["removed"]
    applied = run_cleanup(settings, apply=True, now=future).to_dict()
    assert len(applied["report_artifact_pool_gc"]["removed"]) == 1 and not pool_objects(settings)


def test_workflow_sharing_occurs_after_final_writer_and_does_not_mutate_publication_payload(settings, monkeypatch):
    from backend import workflow

    manager = workflow.WorkflowManager(settings)
    manager._schema_ready = True
    calls = []
    created = {}

    def build(**kwargs):
        payload, path = report(settings, kwargs["run_id"], revision=True)
        created.update(payload=payload, path=path)
        calls.append("build")
        return payload

    def publish(**kwargs):
        calls.append("publish")
        return {"latest_canonical_run_id": kwargs["run_id"]}, kwargs["report"]

    def shadow(*args, **kwargs):
        calls.append("shadow")
        artifact(created["path"], b"S" + CONTENT[1:])

    def final_audit(*args, **kwargs):
        calls.append("last_writer")
        artifact(created["path"], b"F" + CONTENT[1:])

    original = storage.share_finalized_report

    def share(settings, payload):
        assert calls == ["build", "publish", "shadow", "last_writer"]
        assert manager.store.has_active_lock()
        calls.append("share")
        return original(settings, payload)

    monkeypatch.setattr(workflow, "run_project_pipeline", lambda *args: {})
    monkeypatch.setattr(workflow, "build_report_payload", build)
    monkeypatch.setattr(manager, "_publish_formal_report", publish)
    monkeypatch.setattr(manager, "_run_annual_return_gate_shadow_audit", shadow)
    monkeypatch.setattr(manager, "_run_frozen_baseline_audit", final_audit)
    monkeypatch.setattr(storage, "share_finalized_report", share)
    try:
        payload = manager.run_sync({})
        assert manager.store.snapshot()["status"] == "succeeded"
        assert calls[-1] == "share" and payload == created["payload"]
        assert pool_objects(settings)[0].read_bytes() == b"F" + CONTENT[1:]
        receipt = json.loads((settings.runs_dir / payload["run_id"] / "logs/report_artifact_storage.json").read_text())
        assert receipt["status"] == "complete"
        assert receipt["files"][0]["sha256"] == hashlib.sha256(b"F" + CONTENT[1:]).hexdigest()
    finally:
        manager.store.close()


def test_optional_storage_failure_keeps_successful_workflow_result(settings, monkeypatch):
    from backend.workflow import WorkflowManager

    manager = WorkflowManager(settings)
    payload, path = report(settings, "run")
    manager.store.start("run", {})
    monkeypatch.setattr(storage, "share_finalized_report", lambda *args: (_ for _ in ()).throw(OSError("pool full")))
    try:
        manager._share_final_report_artifacts("run", payload)
        manager.store.finish_success(payload)
        assert manager.store.snapshot()["status"] == "succeeded" and path.read_bytes() == CONTENT
    finally:
        manager.store.close()
