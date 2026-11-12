"""Source retention drops successful fixtures without losing source or outcomes."""
from contextlib import closing
import ctypes
import hashlib
import json
import mmap
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

import pytest

from scripts import test_artifact_storage as legacy
from scripts import test_source_storage as storage
from scripts import source_object_archive as objects
from scripts.isolation_support import manifest_digest


@pytest.fixture(autouse=True)
def owned_process_metadata(monkeypatch):
    # The Landlock-protected pytest worker cannot inspect its trusted parent.
    # These fixture tests inspect real descriptors/maps for their own process;
    # the unmodified global check also runs in the actual runner after pytest.
    monkeypatch.setattr(storage, "_process_paths", lambda: [Path("/proc") / str(os.getpid())])


@pytest.fixture
def worker_processes():
    assert legacy.enable_worker_child_tracking()
    processes = []

    def create(exit_code=0, *, running=False):
        program = "import time; time.sleep(60)" if running else f"raise SystemExit({exit_code})"
        process = subprocess.Popen([sys.executable, "-c", program], start_new_session=True,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(process)
        if not running:
            assert process.wait(timeout=10) == exit_code
        return process

    yield create
    for process in processes:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)


def write_json(path, value):
    path.write_text(json.dumps(value))


def completed_run(root, *, code=0, name="run", large=False):
    run = root / name
    workspace = run / "workspace"
    (workspace / "module").mkdir(parents=True)
    source = b"# Synthetic storage fixture, never production fund data.\nVALUE = None\n"
    if large:
        source *= 800
    (workspace / "module/example.py").write_bytes(source)
    (workspace / "module/example.py").chmod(0o640)
    (workspace / "scratch").mkdir()
    with closing(sqlite3.connect(workspace / "scratch/synthetic.db")) as connection:
        connection.execute("CREATE TABLE synthetic (label, value)")
        connection.executemany("INSERT INTO synthetic VALUES (?,?)", [("missing", None), ("zero", 0)])
        connection.commit()
    files = {"module/example.py": hashlib.sha256(source).hexdigest()}
    digest = manifest_digest(files)
    worker = {"exit_code": code, "restrictions": {"scope": "isolated_unit_regression",
              "network_socket_creation": "denied", "business_paths_outside_workspace": "read_write_denied"}}
    write_json(run / "source_manifest.json", {"sha256": digest, "files": files})
    write_json(run / "summary.json", {"scope": "isolated_unit_regression", "source_sha256": digest,
                                      "source_unchanged": True, "exit_code": code, "timed_out": False,
                                      "worker_child_tracking_available": True, "worker": worker})
    write_json(workspace / ".test_result.json", worker)
    write_json(workspace / ".test_isolation.json", {"scope": "isolated_unit_regression", "source_sha256": digest})
    (run / "pytest.log").write_text("Synthetic test result and diagnostic log\n")
    return run, workspace, source


def archive_source(run, process):
    result = storage.store_completed_workspace(run, process)
    assert result["status"] == "source_archived", result
    assert result["source_restore_verified"] and result["original_workspace_recoverable"] is False
    assert result["workspace_removed"] and result["full_restore_verified"] is False
    return result


@pytest.mark.parametrize("large", [False, True])
def test_success_keeps_exact_source_modes_times_results_and_logs_but_discards_generated_db(tmp_path, worker_processes, large):
    run, workspace, source = completed_run(tmp_path, large=large)
    info = (workspace / "module/example.py").stat()
    originals = {name: (run / name).read_bytes() for name in ("summary.json", "source_manifest.json", "pytest.log")}
    archive_source(run, worker_processes())
    assert not workspace.exists() and not (run / legacy.ARCHIVE_NAME).exists()
    assert originals == {name: (run / name).read_bytes() for name in originals}
    target = tmp_path / "source_copy"
    result = storage.restore_sources(run / storage.MANIFEST_NAME, target)
    assert result["status"] == "source_restored" and result["original_workspace_recoverable"] is False
    assert (target / "module/example.py").read_bytes() == source
    restored = (target / "module/example.py").stat()
    assert restored.st_mode == info.st_mode and restored.st_mtime_ns == info.st_mtime_ns
    assert not (target / "scratch").exists() and not (target / ".test_result.json").exists()
    assert not list(run.glob(".source_restore_verify_*"))


@pytest.mark.parametrize("code", [1, 2, 5])
@pytest.mark.parametrize("large", [False, True])
def test_failed_tests_keep_full_original_failure_database_and_exit(tmp_path, worker_processes, code, large):
    run, workspace, _ = completed_run(tmp_path, code=code, large=large)
    original = (workspace / "scratch/synthetic.db").read_bytes()
    result = storage.store_completed_workspace(run, worker_processes(code))
    assert result["status"] == "archived" and result["test_outcome"]["exit_code"] == code
    assert not (run / storage.ARCHIVE_NAME).exists()
    legacy.restore_workspace(run / legacy.MANIFEST_NAME, tmp_path / "failure_copy")
    assert (tmp_path / "failure_copy/scratch/synthetic.db").read_bytes() == original


@pytest.mark.parametrize("large", [False, True])
def test_timeout_preserves_absent_worker_result_and_full_workspace(tmp_path, worker_processes, large):
    run, workspace, _ = completed_run(tmp_path, large=large)
    process = worker_processes(running=True)
    process.terminate()
    process.wait(timeout=10)
    summary = json.loads((run / "summary.json").read_text())
    summary.update(exit_code=124, timed_out=True, worker={})
    write_json(run / "summary.json", summary)
    (workspace / ".test_result.json").unlink()
    result = storage.store_completed_workspace(run, process)
    assert result["status"] == "archived" and result["test_outcome"]["timed_out"] is True
    assert result["test_outcome"]["worker_result_present"] is False
    assert not (run / storage.ARCHIVE_NAME).exists()


@pytest.mark.parametrize("damage", ["source_bytes", "source_sha", "isolation", "worker_disagrees", "source_changed", "source_symlink"])
def test_unverified_success_does_not_discard_any_fixture(tmp_path, worker_processes, damage):
    run, workspace, _ = completed_run(tmp_path)
    summary = json.loads((run / "summary.json").read_text())
    if damage == "source_bytes":
        (workspace / "module/example.py").write_text("changed source\n")
    elif damage == "source_sha":
        summary["source_sha256"] = "0" * 64
    elif damage == "source_changed":
        summary["source_unchanged"] = False
    elif damage == "source_symlink":
        (workspace / "module/example.py").unlink()
        (workspace / "module/example.py").symlink_to("../scratch/synthetic.db")
    else:
        summary["worker"]["restrictions"]["network_socket_creation"] = "not_proven"
        if damage == "isolation":
            write_json(workspace / ".test_result.json", summary["worker"])
    write_json(run / "summary.json", summary)
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "retained" and (workspace / "scratch/synthetic.db").is_file()
    assert not (run / storage.MANIFEST_NAME).exists()


def test_live_worker_and_detached_children_cannot_be_discarded(tmp_path, worker_processes):
    run, workspace, _ = completed_run(tmp_path)
    process = worker_processes(running=True)
    result = storage.store_completed_workspace(run, process)
    assert "worker_still_running" in result["reason"] and workspace.exists()
    process.terminate()
    process.wait(timeout=10)
    process = worker_processes()
    worker_processes(running=True)
    result = storage.store_completed_workspace(run, process)
    assert "descendants_still_running" in result["reason"] and workspace.exists()


@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("large", [False, True])
def test_open_or_mapped_fixtures_prevent_disposal(tmp_path, worker_processes, mapped, large):
    run, workspace, _ = completed_run(tmp_path, large=large)
    process = worker_processes()
    with (workspace / "scratch/synthetic.db").open("rb") as reader:
        mapping = mmap.mmap(reader.fileno(), 0, access=mmap.ACCESS_READ) if mapped else None
        try:
            result = storage.store_completed_workspace(run, process)
            assert result["status"] == "retained" and workspace.exists()
            assert "test_workspace_file_" in result["reason"]
        finally:
            if mapping is not None:
                mapping.close()


@pytest.mark.parametrize("large", [False, True])
def test_broken_archive_retains_original_workspace(tmp_path, worker_processes, monkeypatch, large):
    run, workspace, _ = completed_run(tmp_path, large=large)
    process = worker_processes()
    real = storage._write_source_archive

    def corrupt(path, *args):
        real(path, *args)
        path.write_bytes(path.read_bytes()[:-10])

    monkeypatch.setattr(storage, "_write_source_archive", corrupt)
    result = storage.store_completed_workspace(run, process)
    assert result["status"] == "retained" and workspace.exists()
    assert not (run / storage.MANIFEST_NAME).exists()


@pytest.mark.parametrize("large", [False, True])
def test_low_space_never_starts_a_source_archive_or_discards_fixtures(tmp_path, worker_processes, monkeypatch, large):
    from types import SimpleNamespace

    run, workspace, _ = completed_run(tmp_path, large=large)
    monkeypatch.setattr(storage.os, "statvfs", lambda path: SimpleNamespace(f_bavail=1, f_frsize=4096))
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "retained" and "insufficient_source_archive" in result["reason"]
    assert workspace.exists() and not (run / storage.ARCHIVE_NAME).exists()
    assert not (run / (storage.ARCHIVE_NAME + ".tmp")).exists()


def test_unavailable_process_metadata_retains_originals(tmp_path, worker_processes, monkeypatch):
    run, workspace, _ = completed_run(tmp_path)

    def unavailable():
        raise PermissionError("synthetic unavailable process metadata")

    monkeypatch.setattr(storage, "_process_paths", unavailable)
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "retained" and workspace.is_dir()
    assert "PermissionError" in result["reason"] and not (run / storage.ARCHIVE_NAME).exists()


@pytest.mark.parametrize("large", [False, True])
def test_late_source_write_or_worker_prevents_disposal(tmp_path, worker_processes, monkeypatch, large):
    run, workspace, _ = completed_run(tmp_path, large=large)
    restore = legacy._restore

    def late_write(*args):
        restore(*args)
        (workspace / "module/example.py").write_text("late source edit\n")

    monkeypatch.setattr(legacy, "_restore", late_write)
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "retained" and "workspace_changed_before_retirement" in result["reason"]
    assert (workspace / "module/example.py").read_text() == "late source edit\n"


@pytest.mark.parametrize("large", [False, True])
def test_partial_delete_has_truthful_status_and_still_recovers_sources(tmp_path, worker_processes, monkeypatch, large):
    run, workspace, source = completed_run(tmp_path, large=large)
    remove = legacy._remove_tree

    def interrupt(path, entries):
        if path == workspace:
            (workspace / "module/example.py").unlink()
            raise OSError("synthetic partial delete")
        remove(path, entries)

    monkeypatch.setattr(legacy, "_remove_tree", interrupt)
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "source_archive_available_cleanup_incomplete"
    assert result["original_workspace_recoverable"] is False and result["workspace_removed"] is False
    assert (workspace / "scratch/synthetic.db").exists()
    storage.restore_sources(run / storage.MANIFEST_NAME, tmp_path / "copy")
    assert (tmp_path / "copy/module/example.py").read_bytes() == source


@pytest.mark.parametrize("damage", ["archive", "manifest", "source_manifest", "extra_member"])
def test_damaged_source_recovery_never_creates_destination(tmp_path, worker_processes, damage):
    run, _, _ = completed_run(tmp_path)
    archive_source(run, worker_processes())
    if damage == "archive":
        archive = run / storage.ARCHIVE_NAME
        archive.write_bytes(archive.read_bytes()[:-5])
    elif damage == "source_manifest":
        (run / "source_manifest.json").write_text("{}")
    else:
        path = run / storage.MANIFEST_NAME
        value = json.loads(path.read_text())
        if damage == "manifest":
            value["source_sha256"] = "0" * 64
        else:
            value["entries"]["extra"] = value["entries"]["module/example.py"]
        write_json(path, value)
    with pytest.raises((ValueError, KeyError, legacy.ArtifactStorageError)):
        storage.restore_sources(run / storage.MANIFEST_NAME, tmp_path / "never_created")
    assert not (tmp_path / "never_created").exists()


def test_deterministic_archives_share_one_inode_but_restore_each_runs_timestamps(tmp_path, worker_processes):
    runs = []
    for number in (1, 2):
        run, workspace, source = completed_run(tmp_path, name=f"run{number}")
        modified = 1700000000 + number * 10
        os.utime(workspace / "module/example.py", (modified, modified))
        archive_source(run, worker_processes())
        result = storage.share_test_sources(tmp_path, run)
        assert result["status"] == ("registered" if number == 1 else "shared")
        if number == 2:
            assert result["reclaimed_allocated_bytes_estimate"] > 0
        storage.restore_sources(run / storage.MANIFEST_NAME, tmp_path / f"source{number}")
        restored = tmp_path / f"source{number}/module/example.py"
        assert restored.read_bytes() == source and restored.stat().st_mtime == modified
        runs.append(run)
    assert os.path.samefile(runs[0] / storage.ARCHIVE_NAME, runs[1] / storage.ARCHIVE_NAME)
    assert (runs[0] / storage.ARCHIVE_NAME).stat().st_nlink == 3
    assert storage.share_test_sources(tmp_path, runs[0])["status"] == "already_shared"


def test_corrupt_pool_object_does_not_replace_valid_per_run_archive(tmp_path, worker_processes):
    run, _, _ = completed_run(tmp_path)
    result = archive_source(run, worker_processes())
    pool = tmp_path / "reports" / storage.POOL_NAME
    pool.mkdir(parents=True)
    (pool / (result["archive_sha256"] + ".tar.gz")).write_bytes(b"synthetic corruption")
    original = (run / storage.ARCHIVE_NAME).read_bytes()
    with pytest.raises(legacy.ArtifactStorageError, match="pool_object_corrupt"):
        storage.share_test_sources(tmp_path, run)
    assert (run / storage.ARCHIVE_NAME).read_bytes() == original


def test_gc_protects_referenced_archives_then_only_removes_expired_last_link(tmp_path, worker_processes):
    run, _, _ = completed_run(tmp_path)
    result = archive_source(run, worker_processes())
    storage.share_test_sources(tmp_path, run)
    pool = tmp_path / "reports" / storage.POOL_NAME
    obj = pool / (result["archive_sha256"] + ".tar.gz")
    os.utime(obj, (time.time() - 2 * 86400,) * 2)
    assert storage.collect_unused_test_sources(tmp_path, apply=True)["planned_count"] == 0
    (run / storage.ARCHIVE_NAME).unlink()
    before = {p.name for p in pool.iterdir()}
    result = storage.collect_unused_test_sources(tmp_path)
    assert result["planned_count"] == 1 and result["removed_count"] == 0
    assert {p.name for p in pool.iterdir()} == before
    allocated = obj.stat().st_blocks * 512
    result = storage.collect_unused_test_sources(tmp_path, apply=True)
    assert result["removed_count"] == 1 and result["removed_allocated_bytes_estimate"] == allocated
    assert not obj.exists()


def test_gc_is_bounded_and_keeps_open_corrupt_and_unrecognized_files(tmp_path):
    pool = tmp_path / "reports" / storage.POOL_NAME
    with storage._pool_lock(pool):
        pass
    for number in range(35):
        value = f"synthetic source cache {number}".encode()
        path = pool / (hashlib.sha256(value).hexdigest() + ".tar.gz")
        path.write_bytes(value)
        os.utime(path, (time.time() - 2 * 86400,) * 2)
    retained = pool / "unrecognized.txt"
    retained.write_text("keep")
    result = storage.collect_unused_test_sources(tmp_path, apply=True)
    assert result["planned_count"] == result["removed_count"] == 32
    assert retained.read_text() == "keep" and len(list(pool.glob("*.tar.gz"))) == 3
    one = next(pool.glob("*.tar.gz"))
    with one.open("rb"):
        result = storage.collect_unused_test_sources(tmp_path, apply=True)
        assert one.exists() and any("test_workspace_file_open" in row["error"] for row in result["errors"])
    one.write_bytes(b"corrupt")
    os.utime(one, (time.time() - 2 * 86400,) * 2)
    result = storage.collect_unused_test_sources(tmp_path, apply=True)
    assert one.exists() and any("corrupt" in row["error"] for row in result["errors"])


def test_pool_failure_does_not_rewrite_a_passed_test_result(monkeypatch, tmp_path):
    from scripts import run_isolated_tests as runner

    summary = {"exit_code": 0, "timed_out": False, "workspace_storage": {"status": "source_archived"}}
    def fail(*args):
        raise OSError("synthetic pool failure")
    monkeypatch.setattr(runner, "share_test_sources", fail)
    runner._share_sources(tmp_path, summary)
    assert summary["exit_code"] == 0 and summary["timed_out"] is False
    assert summary["source_pool_sharing"]["status"] == "retained"


def test_weekly_cleanup_includes_source_pool_gc_and_logs_only_skips_it(tmp_path):
    from scripts import cleanup_disk_usage as cleanup
    from types import SimpleNamespace

    settings = SimpleNamespace(project_root=tmp_path, core_root=tmp_path / "core",
                               archived_runs_dir=tmp_path / "archive", runs_dir=tmp_path / "runs")
    report = cleanup.run_cleanup(settings, apply=False)
    assert report.to_dict()["test_source_pool_gc"]["scope"] == "unreferenced_test_source_objects"
    assert cleanup.run_cleanup(settings, logs_only=True).to_dict()["test_source_pool_gc"] is None


def register_fixture_sources(run, workspace, names):
    """Extend only this synthetic run's source identity, including its context."""
    source = json.loads((run / "source_manifest.json").read_text())
    for name in names:
        source["files"][name] = hashlib.sha256((workspace / name).read_bytes()).hexdigest()
    source["sha256"] = manifest_digest(source["files"])
    write_json(run / "source_manifest.json", source)
    for path in (run / "summary.json", workspace / ".test_isolation.json"):
        value = json.loads(path.read_text())
        value["source_sha256"] = source["sha256"]
        write_json(path, value)


def test_different_versions_share_unchanged_large_files_and_move_without_pool(tmp_path, worker_processes):
    runs, object_paths = [], []
    for number in (1, 2):
        run, workspace, source = completed_run(tmp_path, name=f"run{number}", large=True)
        (workspace / "module/version.py").write_text(f"SYNTHETIC_VERSION = {number}\n")
        os.utime(workspace / "module/example.py", ns=(1700000000000000000 + number,) * 2)
        register_fixture_sources(run, workspace, ["module/version.py"])
        result = archive_source(run, worker_processes())
        assert result["schema_version"] == 2 and result["independent_reader_verified"]
        assert result["source_object_count"] == 1
        shared = storage.share_test_sources(tmp_path, run)
        assert shared["status"] == ("registered" if number == 1 else "shared")
        if number == 2:
            assert shared["reclaimed_allocated_bytes_estimate"] > 0
        object_paths.append(next((run / objects.OBJECTS_NAME).iterdir()))
        runs.append(run)
    assert os.path.samefile(*object_paths)
    assert not os.path.samefile(*(run / storage.ARCHIVE_NAME for run in runs))
    shutil.rmtree(tmp_path / "reports" / storage.POOL_NAME)
    moved = tmp_path / "moved_run"
    runs[0].rename(moved)
    storage.restore_sources(moved / storage.MANIFEST_NAME, tmp_path / "restored")
    restored = tmp_path / "restored/module/example.py"
    assert restored.read_bytes() == source
    assert restored.stat().st_mtime_ns == 1700000000000000001
    assert (tmp_path / "restored/module/version.py").read_text() == "SYNTHETIC_VERSION = 1\n"


def test_large_object_recovery_preserves_true_hardlinks_and_independent_same_content(tmp_path, worker_processes):
    run, workspace, source = completed_run(tmp_path, large=True)
    os.link(workspace / "module/example.py", workspace / "module/linked.py")
    (workspace / "module/independent.py").write_bytes(source)
    (workspace / "module/independent.py").chmod(0o600)
    os.utime(workspace / "module/independent.py", ns=(1700000000123456789,) * 2)
    (workspace / "module").chmod(0o550)
    register_fixture_sources(run, workspace, ["module/linked.py", "module/independent.py"])
    before = legacy._scan(workspace, time.monotonic() + 20)
    archive_source(run, worker_processes())
    receipt = json.loads((run / storage.MANIFEST_NAME).read_text())
    assert len(receipt["object_index"]) == 1 and len(receipt["object_paths"]) == 3
    target = tmp_path / "copy"
    storage.restore_sources(run / storage.MANIFEST_NAME, target)
    assert os.path.samefile(target / "module/example.py", target / "module/linked.py")
    assert not os.path.samefile(target / "module/example.py", target / "module/independent.py")
    after = legacy._scan(target, time.monotonic() + 20)
    assert legacy._content(after) == legacy._content({name: before[name] for name in after})


def test_bundled_reader_restores_without_project_imports_or_pool(tmp_path, worker_processes):
    run, _, source = completed_run(tmp_path, large=True)
    archive_source(run, worker_processes())
    clean_cwd = tmp_path / "outside_project"
    clean_cwd.mkdir()
    (clean_cwd / "scripts.py").write_text("raise AssertionError('must not import local scripts')\n")
    target = clean_cwd / "recovered"
    result = subprocess.run(
        [sys.executable, "-I", str(run / objects.READER_NAME), "--manifest", str(run / storage.MANIFEST_NAME),
         "--target", str(target)], cwd=clean_cwd, env={**os.environ, "PYTHONPATH": str(clean_cwd)},
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["source_restore_verified"]
    assert (target / "module/example.py").read_bytes() == source


@pytest.mark.parametrize("damage", ["missing", "compressed_sha", "compressed_size", "decoded_sha", "crc",
                                    "path", "directory", "mapping_missing", "mapping_extra", "object_extra",
                                    "symlink", "reader", "hardlink_metadata"])
def test_invalid_object_package_rejected_before_creating_recovery_target(tmp_path, worker_processes, damage):
    run, _, _ = completed_run(tmp_path, large=True)
    archive_source(run, worker_processes())
    receipt_path = run / storage.MANIFEST_NAME
    receipt = json.loads(receipt_path.read_text())
    object_id, record = next(iter(receipt["object_index"].items()))
    object_path = run / objects.OBJECTS_NAME / (object_id + ".gz")
    if damage == "missing":
        object_path.unlink()
    elif damage == "compressed_sha":
        object_path.write_bytes(b"x" * record["bytes"])
    elif damage == "compressed_size":
        record["bytes"] += 1
    elif damage == "decoded_sha":
        record["source_sha256"] = "0" * 64
    elif damage == "crc":
        raw = bytearray(object_path.read_bytes())
        raw[-8] ^= 1
        changed_id = hashlib.sha256(raw).hexdigest()
        changed = object_path.with_name(changed_id + ".gz")
        changed.write_bytes(raw)
        object_path.unlink()
        receipt["object_index"] = {changed_id: record}
        receipt["object_paths"]["module/example.py"] = changed_id
    elif damage == "path":
        receipt["object_paths"]["module/example.py"] = "../outside"
        receipt["object_index"] = {"../outside": record}
    elif damage == "directory":
        receipt["objects_directory"] = "../outside"
    elif damage == "mapping_missing":
        receipt["object_paths"].clear()
    elif damage == "mapping_extra":
        receipt["object_paths"]["../outside.py"] = object_id
    elif damage == "object_extra":
        (run / objects.OBJECTS_NAME / ("0" * 64 + ".gz")).write_bytes(b"extra")
    elif damage == "symlink":
        moved = run / "outside.gz"
        object_path.rename(moved)
        object_path.symlink_to(moved)
    elif damage == "reader":
        (run / objects.READER_NAME).write_bytes(b"bad reader")
    else:
        receipt["entries"]["module/example.py"]["mode"] = True
    write_json(receipt_path, receipt)
    target = tmp_path / "never_created"
    with pytest.raises((legacy.ArtifactStorageError, OSError, ValueError, EOFError)):
        storage.restore_sources(receipt_path, target)
    assert not target.exists()


def test_partial_object_delivery_keeps_workspace_and_does_not_commit_receipt(tmp_path, worker_processes, monkeypatch):
    run, workspace, source = completed_run(tmp_path, large=True)
    replace = Path.replace

    def fail_reader_delivery(path, target):
        if path.name == objects.READER_NAME and path.parent.name.startswith(".source_package_"):
            raise OSError("synthetic interrupted package delivery")
        return replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_reader_delivery)
    result = storage.store_completed_workspace(run, worker_processes())
    assert result["status"] == "retained" and result["original_workspace_recoverable"]
    assert (workspace / "module/example.py").read_bytes() == source
    assert (workspace / "scratch/synthetic.db").exists()
    assert not (run / storage.MANIFEST_NAME).exists()
    assert Path(result["temporary_package_retained"]).is_dir()


@pytest.mark.parametrize("mapped", [False, True])
def test_object_pool_gc_requires_no_local_reference_and_no_live_reader(tmp_path, worker_processes, mapped):
    run, _, _ = completed_run(tmp_path, large=True)
    archive_source(run, worker_processes())
    storage.share_test_sources(tmp_path, run)
    object_path = next((run / objects.OBJECTS_NAME).iterdir())
    pooled = tmp_path / "reports" / storage.POOL_NAME / object_path.name
    os.utime(pooled, (time.time() - 2 * 86400,) * 2)
    assert storage.collect_unused_test_sources(tmp_path, apply=True)["planned_count"] == 0
    object_path.unlink()
    with pooled.open("rb") as reader:
        mapping = None
        if mapped:
            # Python's mmap keeps a duplicate descriptor on 3.11. Use the
            # kernel mapping directly to exercise maps after all FDs close.
            libc = ctypes.CDLL(None, use_errno=True)
            libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                                  ctypes.c_int, ctypes.c_int, ctypes.c_long]
            libc.mmap.restype = ctypes.c_void_p
            libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            size = pooled.stat().st_size
            mapping = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_PRIVATE, reader.fileno(), 0)
            assert mapping != ctypes.c_void_p(-1).value
        try:
            if mapped:
                reader.close()
            result = storage.collect_unused_test_sources(tmp_path, apply=True)
            assert result["removed_count"] == 0 and pooled.exists()
            assert any(("file_mapped" if mapped else "file_open") in row["error"] for row in result["errors"])
        finally:
            if mapping is not None:
                assert libc.munmap(mapping, size) == 0
    assert storage.collect_unused_test_sources(tmp_path, apply=True)["removed_count"] == 1


def test_corrupt_shared_large_object_cannot_replace_valid_local_package(tmp_path, worker_processes):
    run, _, source = completed_run(tmp_path, large=True)
    archive_source(run, worker_processes())
    original = next((run / objects.OBJECTS_NAME).iterdir())
    pool = tmp_path / "reports" / storage.POOL_NAME
    with storage._pool_lock(pool):
        (pool / original.name).write_bytes(b"corrupt")
    with pytest.raises(legacy.ArtifactStorageError, match="pool_object_corrupt"):
        storage.share_test_sources(tmp_path, run)
    storage.restore_sources(run / storage.MANIFEST_NAME, tmp_path / "still_recovers")
    assert (tmp_path / "still_recovers/module/example.py").read_bytes() == source
