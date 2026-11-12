"""Legacy export storage contracts use synthetic bytes, not fund observations."""
from datetime import datetime
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

from backend import legacy_report_csv_compression as migration
from backend import core_csv_compression
from core.utils import core_csv_storage as storage

RUN = "20260410_010203_sync"
STAMP = int(datetime(2026, 4, 10, 1, 2, 3).timestamp()) * 10**9 + 123456789
CONTENT = ("synthetic_identifier,missing,zero,text\r\nX001,,0,测试\r\n" * 2500).encode("utf-8-sig")


def client(settings):
    from backend.api_routes.reports import register_reports_routes
    from backend.artifact_paths import resolve_project_artifact_path
    app = Flask(__name__)
    options = {key: lambda *a, **kw: {} for key in inspect.signature(register_reports_routes).parameters if key != "app"}
    options["resolve_project_file"] = lambda value: resolve_project_artifact_path(value, project_root=settings.project_root)
    register_reports_routes(app, **options)
    return app.test_client()


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / "project"
    return SimpleNamespace(project_root=root, reports_dir=root / "reports", runs_dir=root / "reports/runs",
                           archived_runs_dir=root / "reports/archive/superseded_runs", state_file=root / "state.json")


def output(settings, *, archived=False, run_id=RUN):
    directory = (settings.archived_runs_dir if archived else settings.runs_dir) / run_id
    directory.mkdir(parents=True)
    path = directory / f"当日最终优质基金_持有期排除清单_{run_id[:15]}.csv"
    path.write_bytes(CONTENT)
    path.chmod(0o640)
    os.utime(path, ns=(STAMP, STAMP))
    sources = [directory / f"bond_analysis_report_{run_id}.json", path.with_name(path.stem + "_report_meta.json")]
    payloads = [{"run_id": run_id, "generated_at": "2026-04-10T01:02:04", "status": "failed",
                 "artifacts": {"excluded_csv": f"reports/runs/{run_id}/{path.name}"}},
                {"generated_at": "2026-04-10T01:02:04", "primary_run_id": run_id,
                 "report_type": "holding_period_excluded_list", "output_file": str(path.with_suffix(".xlsx"))}]
    for source, payload in zip(sources, payloads):
        source.write_text(json.dumps(payload))
        os.utime(source, ns=(STAMP + 10**9,) * 2)
    return path, sources


@pytest.mark.parametrize("archived", [False, True])
def test_original_bytes_metadata_http_and_independent_restore(settings, archived):
    path, sources = output(settings, archived=archived)
    source_bytes = [p.read_bytes() for p in sources]
    before = path.stat()
    http = client(settings)
    url = "/api/files/" + str(path.relative_to(settings.project_root))
    response = http.get(url)
    assert response.status_code == 200 and response.data == CONTENT
    headers = {key: response.headers[key] for key in ("ETag", "Last-Modified", "Content-Length", "Content-Type")}
    plan = migration.run_legacy_report_csv_compression(settings)
    assert plan["candidate_count"] == 1 and path.exists()
    result = migration.run_legacy_report_csv_compression(settings, apply=True)
    assert result["compressed_count"] == 1 and not result["errors"]
    assert not path.exists() and storage.read_original_artifact_bytes(path) == CONTENT
    assert source_bytes == [p.read_bytes() for p in sources]
    response = http.get(url)
    assert response.status_code == 200 and response.data == CONTENT
    assert {key: response.headers[key] for key in headers} == headers
    assert http.get(url, headers={"If-None-Match": headers["ETag"]}).status_code == 304
    ranged = http.get(url, headers={"Range": "bytes=5-50"})
    assert ranged.status_code == 206 and ranged.data == CONTENT[5:51]
    restored = settings.project_root / "independent.csv"
    migration.restore_legacy_report_csv(settings, path, restored)
    info = restored.stat()
    assert restored.read_bytes() == CONTENT
    assert (info.st_size, info.st_mode, info.st_uid, info.st_gid, info.st_mtime_ns) == (
        before.st_size, before.st_mode, before.st_uid, before.st_gid, before.st_mtime_ns)
    assert result["outcomes"][0]["independent_restore_verified"]
    assert migration.run_legacy_report_csv_compression(settings)["candidate_count"] == 0
    if archived:
        from backend.publication import _rebase_artifact_path
        for raw in (f"reports/runs/{RUN}/{path.name}", str(settings.runs_dir / RUN / path.name)):
            rebased = _rebase_artifact_path(settings, raw, run_id=RUN, actual_run_dir=path.parent)
            actual = Path(rebased) if Path(rebased).is_absolute() else settings.project_root / rebased
            assert actual == path


@pytest.mark.parametrize("damage", ["recent", "hardlink", "symlink", "committed", "manifest", "revision",
                                   "missing_context", "wrong_role", "wrong_run", "wrong_binding",
                                   "early_context", "xattr", "unrelated_file"])
def test_protected_or_unproven_outputs_are_never_retired(settings, monkeypatch, damage):
    path, sources = output(settings)
    if damage == "recent":
        os.utime(path, None)
    elif damage == "hardlink":
        os.link(path, path.with_suffix(".alias"))
    elif damage == "symlink":
        target = path.with_suffix(".original")
        path.rename(target)
        path.symlink_to(target)
    elif damage == "committed":
        monkeypatch.setattr(migration, "load_publication_head", lambda settings: {"committed_runs": {RUN: {}}})
    elif damage == "manifest":
        (path.parent / "validated.json").write_text("{}")
    elif damage == "revision":
        target = path.parent / "revisions/frozen"
        target.mkdir(parents=True)
        path.rename(target / path.name)
        path = target / path.name
    elif damage == "missing_context":
        sources[1].unlink()
    elif damage in {"wrong_role", "wrong_run", "wrong_binding"}:
        source = sources[0] if damage == "wrong_binding" else sources[1]
        payload = json.loads(source.read_bytes())
        if damage == "wrong_binding":
            payload["artifacts"]["excluded_csv"] = "elsewhere.csv"
        else:
            payload["report_type" if damage == "wrong_role" else "primary_run_id"] = "unverified"
        source.write_text(json.dumps(payload))
        os.utime(source, ns=(STAMP + 10**9,) * 2)
    elif damage == "early_context":
        os.utime(sources[1], ns=(STAMP - 1,) * 2)
    elif damage == "xattr":
        os.setxattr(path, "user.storage-test", b"preserve")
    elif damage == "unrelated_file":
        target = path.with_name("正式推荐债券基金_20260410_010203.csv")
        path.rename(target)
        path = target
    result = migration.run_legacy_report_csv_compression(settings, apply=True)
    assert result["removed_count"] == result["candidate_count"] == 0
    assert path.read_bytes() == CONTENT and not storage.has_stored_csv(path)


def test_source_changed_after_plan_is_retained(settings, monkeypatch):
    path, _ = output(settings)
    original = migration.plan_legacy_report_csv_compression
    def plan(*args, **kwargs):
        result = original(*args, **kwargs)
        path.write_bytes(b"changed\n" + CONTENT)
        os.utime(path, ns=(STAMP,) * 2)
        return result
    monkeypatch.setattr(migration, "plan_legacy_report_csv_compression", plan)
    result = migration.run_legacy_report_csv_compression(settings, apply=True)
    assert result["status"] == "failed" and not result["removed_count"]
    assert path.read_bytes() == b"changed\n" + CONTENT and not storage.has_stored_csv(path)


def test_busy_workflow_and_low_space_preserve_original(settings, monkeypatch):
    from backend.storage import WorkflowStore
    path, _ = output(settings)
    store = WorkflowStore(settings.state_file)
    try:
        with store._lock:
            store._acquire_run_lock_locked("contract_lock_holder")
        result = migration.run_legacy_report_csv_compression(settings, apply=True)
        assert result["status"] == "deferred" and path.exists()
    finally:
        store.close()
    monkeypatch.setattr(core_csv_compression.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
    result = migration.run_legacy_report_csv_compression(settings, apply=True)
    assert not result["removed_count"] and path.read_bytes() == CONTENT
    assert not storage.has_stored_csv(path)


def test_corrupt_storage_fails_http_instead_of_returning_empty_success(settings):
    path, _ = output(settings)
    assert migration.run_legacy_report_csv_compression(settings, apply=True)["compressed_count"] == 1
    stored, _ = storage.csv_storage_paths(path)
    stored.write_bytes(b"corrupt")
    response = client(settings).get("/api/files/" + str(path.relative_to(settings.project_root)))
    assert response.status_code == 409
    with pytest.raises(ValueError):
        storage.read_original_artifact_bytes(path)


def test_file_budget_retains_remaining_original(settings):
    first, _ = output(settings)
    second, _ = output(settings, run_id="20260411_010203_sync")
    result = migration.run_legacy_report_csv_compression(settings, apply=True, max_files=1)
    assert result["compressed_count"] == 1
    assert sum(path.exists() for path in (first, second)) == 1
    assert result["retained_reasons"]["batch_file_budget"] == 1
