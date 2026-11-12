from __future__ import annotations

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import test_artifact_storage as storage
from scripts.isolation_support import manifest_digest


def _write_json(path, payload):
    path.write_text(json.dumps(payload))


def _completed_run(tmp_path):
    run = tmp_path / "run"
    workspace = run / "workspace"
    (workspace / "fixtures").mkdir(parents=True)
    source = b"# isolated archive fixture\nVALUE = None\n"
    (workspace / "module.py").write_bytes(source)
    files = {"module.py": hashlib.sha256(source).hexdigest()}
    digest = manifest_digest(files)
    _write_json(run / "source_manifest.json", {"sha256": digest, "files": files})
    _write_json(run / "summary.json", {"scope": "isolated_unit_regression", "exit_code": 0,
                                       "timed_out": False, "source_unchanged": True, "source_sha256": digest})
    _write_json(workspace / ".test_isolation.json", {"scope": "isolated_unit_regression", "source_sha256": digest})
    _write_json(workspace / ".test_result.json", {"exit_code": 0})
    (run / "pytest.log").write_text("fixture passed\n")
    return run, workspace


def test_full_round_trip_preserves_bytes_null_zero_links_modes_and_provenance(tmp_path):
    run, workspace = _completed_run(tmp_path)
    binary = bytes(range(256)) * 7
    data = workspace / "fixtures/data.bin"
    data.write_bytes(binary)
    data.chmod(0o400)
    os.link(data, workspace / "fixtures/data_hardlink.bin")
    (workspace / "fixtures/data_link").symlink_to("data.bin")
    outside = tmp_path / "outside_guard"
    outside.write_text("external link target must not be read or overwritten")
    (workspace / "external_link").symlink_to(outside)
    with closing(sqlite3.connect(workspace / "fixtures/sample.db")) as connection:
        connection.execute("CREATE TABLE fixture (label TEXT, value REAL)")
        connection.executemany("INSERT INTO fixture VALUES (?, ?)", [("missing", None), ("zero", 0.0)])
        connection.commit()
    before = {name: (run / name).read_bytes() for name in ("summary.json", "pytest.log", "source_manifest.json")}
    outcome = storage.archive_successful_workspace(run)
    assert outcome["status"] == "archived", outcome
    assert outcome["full_restore_verified"] and not workspace.exists()
    assert not list(run.glob(".workspace_restore_verify_*"))
    assert before == {name: (run / name).read_bytes() for name in before}
    restored = tmp_path / "restored"
    receipt = storage.restore_workspace(run / storage.MANIFEST_NAME, restored)
    assert receipt["full_restore_verified"]
    assert (restored / "fixtures/data.bin").read_bytes() == binary
    assert (restored / "fixtures/data.bin").stat().st_mode & 0o777 == 0o400
    assert (restored / "fixtures/data.bin").stat().st_ino == (restored / "fixtures/data_hardlink.bin").stat().st_ino
    assert os.readlink(restored / "fixtures/data_link") == "data.bin"
    assert os.readlink(restored / "external_link") == str(outside)
    assert outside.read_text() == "external link target must not be read or overwritten"
    with closing(sqlite3.connect(restored / "fixtures/sample.db")) as connection:
        assert connection.execute("SELECT * FROM fixture ORDER BY label").fetchall() == [("missing", None), ("zero", 0.0)]


@pytest.mark.parametrize("field,value", [("exit_code", 1), ("timed_out", True), ("source_unchanged", False)])
def test_failed_incomplete_or_changed_source_runs_keep_the_workspace(tmp_path, field, value):
    run, workspace = _completed_run(tmp_path)
    summary = json.loads((run / "summary.json").read_text())
    summary[field] = value
    _write_json(run / "summary.json", summary)
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "retained" and workspace.is_dir()
    assert not (run / storage.ARCHIVE_NAME).exists()


def test_worker_failure_cannot_be_overridden_by_successful_parent_summary(tmp_path):
    run, workspace = _completed_run(tmp_path)
    _write_json(workspace / ".test_result.json", {"exit_code": 1})
    assert storage.archive_successful_workspace(run)["status"] == "retained"
    assert workspace.is_dir()


def test_mismatched_source_identity_cannot_retire_files(tmp_path):
    run, workspace = _completed_run(tmp_path)
    context = json.loads((workspace / ".test_isolation.json").read_text())
    context["source_sha256"] = "0" * 64
    _write_json(workspace / ".test_isolation.json", context)
    result = storage.archive_successful_workspace(run)
    assert "test_source_identity_mismatch" in result["reason"]
    assert workspace.is_dir()


def test_insufficient_free_space_preserves_originals_without_partial_archive(tmp_path, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    monkeypatch.setattr(storage.os, "statvfs", lambda path: SimpleNamespace(f_bavail=1, f_frsize=4096))
    result = storage.archive_successful_workspace(run)
    assert "insufficient_archive_and_restore_space" in result["reason"]
    assert workspace.is_dir() and not (run / storage.ARCHIVE_NAME).exists()


def test_corrupt_new_archive_does_not_remove_any_original(tmp_path, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    verify = storage._verify_archive

    def corrupt(archive, entries):
        archive.write_bytes(archive.read_bytes()[:40])
        return verify(archive, entries)

    monkeypatch.setattr(storage, "_verify_archive", corrupt)
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "retained" and not result["workspace_removed"]
    assert (workspace / "module.py").is_file()
    assert not (run / storage.MANIFEST_NAME).exists()


@pytest.mark.parametrize("damage", ["missing_footer", "wrong_crc"])
def test_gzip_footer_must_validate_before_retiring_full_workspace(tmp_path, monkeypatch, damage):
    run, workspace = _completed_run(tmp_path)
    verify = storage._verify_archive

    def corrupt(archive, entries):
        raw = archive.read_bytes()
        if damage == "missing_footer":
            archive.write_bytes(raw[:-8])
        else:
            damaged = bytearray(raw)
            damaged[-8] ^= 1
            archive.write_bytes(damaged)
        return verify(archive, entries)

    monkeypatch.setattr(storage, "_verify_archive", corrupt)
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "retained" and workspace.is_dir()
    assert not (run / storage.MANIFEST_NAME).exists()


def test_manifest_failure_retains_original_and_complete_archive(tmp_path, monkeypatch):
    run, workspace = _completed_run(tmp_path)

    def fail(*args):
        raise OSError("injected manifest persistence failure")

    monkeypatch.setattr(storage, "_atomic_json", fail)
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "retained" and workspace.is_dir()
    assert (run / storage.ARCHIVE_NAME).is_file()
    assert not (run / storage.MANIFEST_NAME).exists()


def test_late_workspace_change_prevents_retirement(tmp_path, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    restore = storage._restore

    def change_after_verification(*args):
        restore(*args)
        (workspace / "module.py").write_text("late writer changed this fixture\n")

    monkeypatch.setattr(storage, "_restore", change_after_verification)
    result = storage.archive_successful_workspace(run)
    assert "workspace_changed_before_retirement" in result["reason"]
    assert (workspace / "module.py").read_text() == "late writer changed this fixture\n"
    assert not (run / storage.MANIFEST_NAME).exists()


def test_interrupted_retirement_keeps_verified_recovery_and_reports_incomplete(tmp_path, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    remove = storage._remove_tree

    def interrupt(root, entries):
        if root == workspace:
            (workspace / "module.py").unlink()
            raise OSError("injected partial cleanup")
        return remove(root, entries)

    monkeypatch.setattr(storage, "_remove_tree", interrupt)
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "archive_available_cleanup_incomplete"
    assert result["full_restore_verified"] and not result["workspace_removed"]
    destination = tmp_path / "recovered"
    storage.restore_workspace(run / storage.MANIFEST_NAME, destination)
    assert (destination / "module.py").read_text() == "# isolated archive fixture\nVALUE = None\n"


def test_corrupt_finished_archive_is_rejected_before_creating_restore_target(tmp_path):
    run, workspace = _completed_run(tmp_path)
    assert storage.archive_successful_workspace(run)["status"] == "archived"
    archive = run / storage.ARCHIVE_NAME
    archive.write_bytes(archive.read_bytes()[:-5])
    destination = tmp_path / "never_created"
    with pytest.raises(storage.ArtifactStorageError, match="archive_sha256_mismatch"):
        storage.restore_workspace(run / storage.MANIFEST_NAME, destination)
    assert not destination.exists()


def test_existing_restore_destination_is_untouched(tmp_path):
    run, workspace = _completed_run(tmp_path)
    assert storage.archive_successful_workspace(run)["status"] == "archived"
    destination = tmp_path / "keep"
    destination.mkdir()
    (destination / "marker").write_text("keep")
    with pytest.raises(FileExistsError):
        storage.restore_workspace(run / storage.MANIFEST_NAME, destination)
    assert (destination / "marker").read_text() == "keep"


def test_archive_and_symlink_paths_are_never_followed_or_overwritten(tmp_path):
    run, workspace = _completed_run(tmp_path)
    archive = run / storage.ARCHIVE_NAME
    archive.write_bytes(b"retained archive fixture")
    result = storage.archive_successful_workspace(run)
    assert result["status"] == "retained" and workspace.is_dir()
    assert archive.read_bytes() == b"retained archive fixture"
    link = tmp_path / "run_link"
    link.symlink_to(run)
    assert "symlink_in_workspace_path" in storage.archive_successful_workspace(link)["reason"]


def test_special_file_does_not_block_or_get_discarded(tmp_path):
    run, workspace = _completed_run(tmp_path)
    os.mkfifo(workspace / "pipe")
    result = storage.archive_successful_workspace(run)
    assert "unsupported_workspace_entry" in result["reason"]
    assert workspace.is_dir()
    (workspace / "pipe").unlink()


@pytest.mark.parametrize("name,kind", [("../outside", "file"), ("/absolute", "directory"), ("link/file", "file")])
def test_unsafe_restore_paths_are_rejected(name, kind):
    entries = {".": {"kind": "directory"}, "link": {"kind": "symlink", "target": "/tmp"}, name: {"kind": kind}}
    with pytest.raises(storage.ArtifactStorageError):
        storage._validate_entries(entries)


@pytest.fixture
def worker_processes():
    assert storage.enable_worker_child_tracking()
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


def _set_terminal(run, exit_code, *, timed_out=False, worker_exit_code=None):
    summary = json.loads((run / "summary.json").read_text())
    summary.update(exit_code=exit_code, timed_out=timed_out, worker_child_tracking_available=True)
    _write_json(run / "summary.json", summary)
    worker_path = run / "workspace/.test_result.json"
    if worker_exit_code is None:
        worker_path.unlink()
    else:
        _write_json(worker_path, {"exit_code": worker_exit_code})


@pytest.mark.parametrize("exit_code", [0, 1, 2, 3, 4, 5])
def test_terminal_archiving_preserves_the_actual_exit_and_complete_failure_evidence(tmp_path, worker_processes, exit_code):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(exit_code)
    _set_terminal(run, exit_code, worker_exit_code=exit_code)
    (workspace / "fixtures/observed.json").write_text('{"synthetic_missing": null, "synthetic_zero": 0}')
    before = {name: (run / name).read_bytes() for name in ("summary.json", "pytest.log", "source_manifest.json")}
    worker_bytes = (workspace / ".test_result.json").read_bytes()
    result = storage.archive_completed_workspace(run, process)
    assert result["status"] == "archived" and result["test_outcome"]["exit_code"] == exit_code
    assert result["test_outcome"]["process_return_code"] == exit_code
    assert result["test_outcome"]["worker_descendants_stopped"] is True
    assert process.returncode == exit_code and not workspace.exists()
    assert before == {name: (run / name).read_bytes() for name in before}
    target = tmp_path / "restored"
    storage.restore_workspace(run / storage.MANIFEST_NAME, target)
    assert (target / ".test_result.json").read_bytes() == worker_bytes
    assert json.loads((target / "fixtures/observed.json").read_text()) == {"synthetic_missing": None, "synthetic_zero": 0}
    descriptor = json.loads((run / storage.MANIFEST_NAME).read_text())
    assert descriptor["test_outcome"] == result["test_outcome"]


@pytest.mark.parametrize("timed_out", [False, True])
def test_missing_worker_terminal_is_preserved_after_a_confirmed_crash_or_timeout(tmp_path, worker_processes, timed_out):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(running=True)
    process.terminate()
    process.wait(timeout=10)
    assert process.returncode < 0
    exit_code = 124 if timed_out else process.returncode
    _set_terminal(run, exit_code, timed_out=timed_out)
    result = storage.archive_completed_workspace(run, process)
    assert result["status"] == "archived"
    outcome = result["test_outcome"]
    assert outcome["exit_code"] == exit_code and outcome["timed_out"] is timed_out
    assert outcome["worker_result_present"] is False and outcome["worker_exit_code"] is None
    assert outcome["process_return_code"] == process.returncode
    target = tmp_path / "restored"
    storage.restore_workspace(run / storage.MANIFEST_NAME, target)
    assert not (target / ".test_result.json").exists()
    assert (target / "module.py").is_file()


@pytest.mark.parametrize("damage", ["parent_exit", "worker_exit", "source_changed", "tracking_not_enabled",
                                    "missing_success_worker", "boolean_exit", "string_exit", "invalid_worker_exit"])
def test_inconsistent_or_unverified_terminal_cannot_retire_files(tmp_path, worker_processes, damage):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(0 if damage == "missing_success_worker" else 1)
    _set_terminal(run, process.returncode, worker_exit_code=process.returncode)
    summary = json.loads((run / "summary.json").read_text())
    if damage == "parent_exit":
        summary["exit_code"] = 0
    elif damage == "worker_exit":
        _write_json(workspace / ".test_result.json", {"exit_code": 0})
    elif damage == "source_changed":
        summary["source_unchanged"] = False
    elif damage == "tracking_not_enabled":
        summary["worker_child_tracking_available"] = False
    elif damage == "missing_success_worker":
        (workspace / ".test_result.json").unlink()
    elif damage in {"boolean_exit", "string_exit"}:
        summary["exit_code"] = True if damage == "boolean_exit" else "1"
    else:
        _write_json(workspace / ".test_result.json", {"exit_code": True})
    _write_json(run / "summary.json", summary)
    result = storage.archive_completed_workspace(run, process)
    assert result["status"] == "retained" and workspace.is_dir()
    assert not (run / storage.ARCHIVE_NAME).exists()


def test_a_still_running_worker_cannot_be_retired_by_a_fabricated_terminal(tmp_path, worker_processes):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(running=True)
    _set_terminal(run, 1, worker_exit_code=1)
    result = storage.archive_completed_workspace(run, process)
    assert "test_worker_still_running" in result["reason"] and workspace.is_dir()


def test_a_separate_live_child_session_prevents_retirement(tmp_path, worker_processes):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(1)
    child = worker_processes(running=True)
    assert os.getpgid(child.pid) != process.pid
    _set_terminal(run, 1, worker_exit_code=1)
    result = storage.archive_completed_workspace(run, process)
    assert "worker_descendants_still_running" in result["reason"] and workspace.is_dir()


def test_kernel_tracking_is_verified_instead_of_trusting_the_summary(tmp_path, worker_processes, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(1)
    _set_terminal(run, 1, worker_exit_code=1)
    monkeypatch.setattr(storage.ctypes, "CDLL", lambda *a, **kw: SimpleNamespace(prctl=lambda *a: -1))
    result = storage.archive_completed_workspace(run, process)
    assert "worker_child_tracking_unavailable" in result["reason"] and workspace.is_dir()


def test_a_child_started_during_restore_validation_keeps_original_failure_files(tmp_path, worker_processes, monkeypatch):
    run, workspace = _completed_run(tmp_path)
    process = worker_processes(1)
    _set_terminal(run, 1, worker_exit_code=1)
    restore = storage._restore

    def with_late_child(*args):
        restore(*args)
        worker_processes(running=True)

    monkeypatch.setattr(storage, "_restore", with_late_child)
    result = storage.archive_completed_workspace(run, process)
    assert "worker_descendants_still_running" in result["reason"] and workspace.is_dir()
    assert not (run / storage.MANIFEST_NAME).exists()
