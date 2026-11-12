"""Real archive I/O on synthetic source fixtures; no fund data or production paths."""
from collections import Counter
import fcntl
import hashlib
import json
import mmap
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from scripts import test_artifact_storage as legacy
from scripts import test_source_storage as storage
from scripts import test_source_migration as migration
from scripts import source_object_archive as objects
from scripts.isolation_support import manifest_digest


@pytest.fixture(autouse=True)
def owned_process_metadata(monkeypatch):
    monkeypatch.setattr(storage, "_process_paths", lambda: [Path("/proc") / str(os.getpid())])


def put(path, value):
    path.write_text(json.dumps(value))
    os.utime(path, (time.time() - 3 * 86400,) * 2)


def legacy_source(root, name="legacy"):
    run = root / "reports/audit" / name
    workspace = run / "workspace"
    (workspace / "module").mkdir(parents=True)
    source = ("# Synthetic source fixture; not business data.\n" +
              "\n".join(f"VALUE_{number} = {number}" for number in range(5000)) + "\n").encode()
    original = workspace / "module/example.py"
    original.write_bytes(source)
    original.chmod(0o640)
    os.utime(original, ns=(1700000000000000000,) * 2)
    os.link(original, workspace / "module/linked.py")
    independent = workspace / "module/independent.py"
    independent.write_bytes(source)
    independent.chmod(0o600)
    os.utime(independent, ns=(1700000000000001234,) * 2)
    (workspace / "small.py").write_text("# Synthetic source fixture\nUNKNOWN = None\n")
    entries = legacy._scan(workspace, time.monotonic() + 30)
    files = {name: item["sha256"] for name, item in entries.items() if item["kind"] == "file"}
    digest = manifest_digest(files)
    put(run / "source_manifest.json", {"sha256": digest, "files": files})
    put(run / legacy.MANIFEST_NAME, {"schema_version": 1, "synthetic_old_workspace_descriptor": True})
    original_archive_sha = hashlib.sha256(b"synthetic former workspace, previously retired").hexdigest()
    worker = {"exit_code": 0, "restrictions": {"scope": "isolated_unit_regression",
              "network_socket_creation": "denied", "business_paths_outside_workspace": "read_write_denied"}}
    put(run / "summary.json", {"scope": "isolated_unit_regression", "source_unchanged": True,
        "exit_code": 0, "timed_out": False, "source_sha256": digest, "worker": worker})
    put(run / "workspace_retired.json", {"schema_version": 1, "status": "retired",
        "retired_archive": legacy.ARCHIVE_NAME, "original_sha256": original_archive_sha,
        "source_sha256": digest, "source_bytes_preserved": True, "test_exit_code": 0,
        "original_test_workspace_recoverable": False})
    (run / "pytest.log").write_text("synthetic original passed test log\n")
    archive = run / storage.ARCHIVE_NAME
    storage._write_source_archive(archive, workspace, entries)
    os.utime(archive, (time.time() - 7200,) * 2)
    put(run / storage.MANIFEST_NAME, {"schema_version": 1, "artifact_type": storage.ARTIFACT_TYPE,
        "archive": storage.ARCHIVE_NAME, "archive_sha256": legacy._sha(archive),
        "archive_bytes": archive.stat().st_size, "entries": entries,
        "source_sha256": digest, "source_manifest_sha256": legacy._sha(run / "source_manifest.json"),
        "source_restore_verified": True, "original_workspace_recoverable": False,
        "test_outcome": {"exit_code": 0, "timed_out": False},
        "origin_archive_sha256": original_archive_sha,
        "origin_archive_manifest_sha256": legacy._sha(run / legacy.MANIFEST_NAME)})
    legacy._remove_tree(workspace, entries)
    return run, entries


def apply(root, **kwargs):
    return migration.migrate_legacy_sources(root, apply=True, min_age_hours=1, **kwargs)


def assert_restored(run, target, entries):
    result = storage.restore_sources(run / storage.MANIFEST_NAME, target)
    actual = legacy._scan(target, time.monotonic() + 30)
    assert legacy._content(actual) == legacy._content(entries)
    assert legacy._hardlinks(actual) == legacy._hardlinks(entries)
    assert result["source_restore_verified"] and result["original_workspace_recoverable"] is False
    assert os.path.samefile(target / "module/example.py", target / "module/linked.py")
    assert not os.path.samefile(target / "module/example.py", target / "module/independent.py")


def test_migration_preserves_original_records_and_all_source_metadata_and_standalone_recovery(tmp_path):
    run, entries = legacy_source(tmp_path)
    before = {p.name: p.read_bytes() for p in run.iterdir() if p.name != storage.ARCHIVE_NAME}
    result = apply(tmp_path)
    assert result["status"] == "complete" and result["migrated_count"] == 1, result
    assert result["net_reclaimed_disk_bytes"] is None
    assert not (run / storage.ARCHIVE_NAME).exists()
    assert before == {run.joinpath(name).name: (run / name).read_bytes() for name in before}
    assert not (run / migration.STAGE_NAME).exists()
    assert_restored(run, tmp_path / "restored", entries)
    shutil.rmtree(tmp_path / "reports" / storage.POOL_NAME)
    moved = tmp_path / "independent_run"
    run.rename(moved)
    command = [sys.executable, "-I", str(moved / storage.MIGRATED_PACKAGE_NAME / objects.READER_NAME),
               "--manifest", str(moved / storage.MANIFEST_NAME), "--target", str(tmp_path / "independent_restore")]
    completed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    restored = legacy._scan(tmp_path / "independent_restore", time.monotonic() + 30)
    assert legacy._content(restored) == legacy._content(entries)
    assert legacy._hardlinks(restored) == legacy._hardlinks(entries)


@pytest.mark.parametrize("damage", ["archive", "source_manifest", "failure", "isolation", "retirement", "origin"])
def test_unproven_or_corrupt_source_packages_preserve_old_representation(tmp_path, damage):
    run, _ = legacy_source(tmp_path)
    archive = run / storage.ARCHIVE_NAME
    if damage == "archive":
        archive.write_bytes(archive.read_bytes()[:-7])
        os.utime(archive, (time.time() - 7200,) * 2)
    elif damage == "source_manifest":
        p = run / "source_manifest.json"
        d = json.loads(p.read_text()); d["sha256"] = "0" * 64
        put(p, d)
    elif damage in {"failure", "isolation"}:
        p = run / "summary.json"; d = json.loads(p.read_text())
        if damage == "failure": d["exit_code"] = 1
        else: d["worker"]["restrictions"]["network_socket_creation"] = "not_denied"
        put(p, d)
    elif damage == "retirement":
        p = run / "workspace_retired.json"; d = json.loads(p.read_text())
        d["original_test_workspace_recoverable"] = True
        put(p, d)
    else:
        put(run / legacy.MANIFEST_NAME, {"changed_original_descriptor": True})
    before = archive.read_bytes()
    result = apply(tmp_path)
    assert result["errors"] and result["migrated_count"] == 0, result
    assert archive.read_bytes() == before
    assert not (run / storage.MIGRATION_NAME).exists()


@pytest.mark.parametrize("mapped", [False, True])
def test_open_or_mapped_legacy_archive_blocks_migration(tmp_path, mapped):
    run, _ = legacy_source(tmp_path)
    reader = (run / storage.ARCHIVE_NAME).open("rb")
    mapping = mmap.mmap(reader.fileno(), 0, access=mmap.ACCESS_READ) if mapped else None
    if mapped: reader.close()
    try:
        result = apply(tmp_path)
        assert result["errors"] and result["migrated_count"] == 0, result
        assert (run / storage.ARCHIVE_NAME).is_file()
        assert not (run / storage.MIGRATION_NAME).exists()
    finally:
        if mapping is not None: mapping.close()
        reader.close()


def test_recovery_lock_prevents_concurrent_migration(tmp_path):
    run, entries = legacy_source(tmp_path)
    with storage.source_archive_lock(run / storage.MANIFEST_NAME):
        result = apply(tmp_path)
    assert result["errors"] and result["migrated_count"] == 0
    assert_restored(run, tmp_path / "before_migration", entries)
    assert apply(tmp_path)["migrated_count"] == 1


def test_release_packages_recent_sources_and_live_workspaces_are_protected(tmp_path):
    pinned, _ = legacy_source(tmp_path, "pinned")
    p = pinned / storage.MANIFEST_NAME; d = json.loads(p.read_text())
    d.pop("origin_archive_sha256"); put(p, d)
    recent, _ = legacy_source(tmp_path, "recent")
    os.utime(recent / storage.ARCHIVE_NAME, None)
    active, _ = legacy_source(tmp_path, "active")
    (active / "workspace").mkdir()
    result = apply(tmp_path)
    assert not result["errors"] and result["migrated_count"] == 0
    assert result["protected_reasons"] == {"source_release_or_unreviewed_lineage_preserved": 1,
        "within_source_migration_minimum_age": 1, "original_workspace_still_present": 1}


def test_preview_and_batch_limits_do_not_claim_unexecuted_work(tmp_path):
    for name in ("one", "two"): legacy_source(tmp_path, name)
    preview = migration.migrate_legacy_sources(tmp_path, min_age_hours=1, max_runs=1)
    assert preview["candidate_count"] == 2 and preview["migrated_count"] == 0
    assert preview["attempted_count"] == 1 and preview["status"] == "batch_limit_reached"
    assert not list((tmp_path / "reports/audit").glob("*/source_migration.json"))
    first = apply(tmp_path, max_runs=1)
    assert first["migrated_count"] == 1 and not first["batch_complete"]
    second = apply(tmp_path, max_runs=1)
    assert second["migrated_count"] == 1 and second["protected_reasons"]["already_migrated"] == 1
    final = apply(tmp_path)
    assert final["migrated_count"] == 0 and final["protected_reasons"]["already_migrated"] == 2


def test_low_space_preserves_old_archive_and_creates_no_new_package(tmp_path, monkeypatch):
    run, _ = legacy_source(tmp_path)
    monkeypatch.setattr(migration.os, "statvfs", lambda path: SimpleNamespace(f_bavail=0, f_frsize=4096))
    result = apply(tmp_path)
    assert result["errors"][0]["error"] == "insufficient_source_migration_space"
    assert (run / storage.ARCHIVE_NAME).is_file()
    assert not (run / storage.MIGRATED_PACKAGE_NAME).exists()


@pytest.mark.parametrize("point", ["commit", "retirement"])
def test_interruption_after_package_delivery_or_commit_can_resume_without_repacking(tmp_path, monkeypatch, point):
    run, entries = legacy_source(tmp_path)
    if point == "commit":
        original = legacy._atomic_json
        def interrupt(path, value):
            if path.name == storage.MIGRATION_NAME: raise OSError("synthetic commit interruption")
            return original(path, value)
        monkeypatch.setattr(legacy, "_atomic_json", interrupt)
    else:
        original = migration._remove_old_representation
        def interrupt(*args): raise OSError("synthetic retirement interruption")
        monkeypatch.setattr(migration, "_remove_old_representation", interrupt)
    first = apply(tmp_path)
    assert first["errors"] and (run / storage.ARCHIVE_NAME).exists()
    package = run / storage.MIGRATED_PACKAGE_NAME
    package_sha = legacy._sha(package / storage.MANIFEST_NAME)
    assert_restored(run, tmp_path / "interrupted_restore", entries)
    if point == "commit": monkeypatch.setattr(legacy, "_atomic_json", original)
    else: monkeypatch.setattr(migration, "_remove_old_representation", original)
    second = apply(tmp_path)
    assert second["migrated_count"] == 1 and not second["errors"], second
    assert legacy._sha(package / storage.MANIFEST_NAME) == package_sha
    assert_restored(run, tmp_path / "resumed_restore", entries)


def test_shared_old_pool_object_is_only_removed_after_its_last_migrated_source_link(tmp_path):
    first, _ = legacy_source(tmp_path, "one")
    second, _ = legacy_source(tmp_path, "two")
    storage.share_test_sources(tmp_path, first)
    storage.share_test_sources(tmp_path, second)
    receipt = json.loads((first / storage.MANIFEST_NAME).read_text())
    old_pool = tmp_path / "reports" / storage.POOL_NAME / (receipt["archive_sha256"] + ".tar.gz")
    assert os.path.samefile(first / storage.ARCHIVE_NAME, second / storage.ARCHIVE_NAME)
    old_bytes = old_pool.stat().st_blocks * 512
    one = apply(tmp_path, run_dirs=[first])
    assert one["migrated_count"] == 1 and one["removed_old_allocated_bytes"] == 0
    assert old_pool.exists() and (second / storage.ARCHIVE_NAME).exists()
    two = apply(tmp_path, run_dirs=[second])
    assert two["migrated_count"] == 1 and two["removed_old_allocated_bytes"] == old_bytes
    assert not old_pool.exists()
    first_object = next((first / storage.MIGRATED_PACKAGE_NAME / objects.OBJECTS_NAME).glob("*.gz"))
    second_object = next((second / storage.MIGRATED_PACKAGE_NAME / objects.OBJECTS_NAME).glob("*.gz"))
    assert os.path.samefile(first_object, second_object)


def test_corrupt_migration_marker_does_not_fall_back_to_unverified_success(tmp_path):
    run, _ = legacy_source(tmp_path)
    assert apply(tmp_path)["migrated_count"] == 1
    p = run / storage.MIGRATION_NAME; d = json.loads(p.read_text())
    d["source_sha256"] = "0" * 64; p.write_text(json.dumps(d))
    with pytest.raises(legacy.ArtifactStorageError, match="invalid_test_source_migration"):
        storage.restore_sources(run / storage.MANIFEST_NAME, tmp_path / "must_not_restore")
    again = apply(tmp_path)
    assert again["errors"] and again["migrated_count"] == 0


def test_changed_evidence_during_sharing_prevents_old_archive_removal(tmp_path, monkeypatch):
    run, _ = legacy_source(tmp_path)
    original = storage._share_file
    def change(*args):
        result = original(*args)
        (run / "summary.json").write_text("changed synthetic summary\n")
        return result
    monkeypatch.setattr(storage, "_share_file", change)
    result = apply(tmp_path)
    assert result["errors"] and result["migrated_count"] == 0
    assert (run / storage.ARCHIVE_NAME).is_file()


def test_symlinks_and_unowned_staging_are_not_removed(tmp_path):
    run, _ = legacy_source(tmp_path)
    outside = tmp_path / "outside_archive"
    (run / storage.ARCHIVE_NAME).rename(outside)
    (run / storage.ARCHIVE_NAME).symlink_to(outside)
    result = apply(tmp_path)
    assert result["errors"] and outside.is_file()
    (run / storage.ARCHIVE_NAME).unlink(); outside.rename(run / storage.ARCHIVE_NAME)
    stage = run / migration.STAGE_NAME
    stage.mkdir(); (stage / "unknown.txt").write_text("unowned data\n")
    result = apply(tmp_path)
    assert result["errors"] and (stage / "unknown.txt").read_text() == "unowned data\n"
    assert (run / storage.ARCHIVE_NAME).exists()


def test_weekly_cleanup_runs_migration_and_logs_only_does_not(tmp_path):
    from scripts import cleanup_disk_usage as cleanup
    run, _ = legacy_source(tmp_path)
    os.utime(run / storage.ARCHIVE_NAME, (time.time() - 3 * 86400,) * 2)
    settings = SimpleNamespace(project_root=tmp_path, core_root=tmp_path / "core",
                               archived_runs_dir=tmp_path / "archive", runs_dir=tmp_path / "runs")
    assert cleanup.run_cleanup(settings, logs_only=True).to_dict()["test_source_migration"] is None
    report = cleanup.run_cleanup(settings, apply=True).to_dict()
    assert report["test_source_migration"]["migrated_count"] == 1, report
    assert not (run / storage.ARCHIVE_NAME).exists()
