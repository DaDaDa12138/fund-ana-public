"""Cleanup reports describe actual effects, including partial failures and sharing."""
from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend import forward_validation_cache
from scripts import cleanup_disk_usage as cleanup


NOW = datetime(2026, 9, 12, 14, 0)


def settings(root: Path) -> SimpleNamespace:
    return SimpleNamespace(project_root=root, core_root=root / "core",
                           archived_runs_dir=root / "archive", runs_dir=root / "runs")


def old_file(root: Path, name: str, content: bytes = b"stored diagnostic evidence\n") -> Path:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    modified = (NOW - timedelta(days=40)).timestamp()
    os.utime(path, (modified, modified))
    return path


def run(root: Path, *, apply: bool = True) -> cleanup.CleanupReport:
    return cleanup.run_cleanup(settings(root), apply=apply, now=NOW,
                               archive_keep_count=0, archive_keep_days=0)


def archive_file(root: Path, name: str = "report.json") -> Path:
    return old_file(root, "archive/20260801_010000_sync/" + name)


def candidate_file(root: Path) -> Path:
    return old_file(root, "backend/data/maintenance/nav_history_vacuum/"
                    "bond_funds_vacuum_candidate_20260801_010000.db")


def test_dry_run_lists_candidates_without_claiming_any_deletion(tmp_path: Path) -> None:
    archive, candidate = archive_file(tmp_path), candidate_file(tmp_path)
    log = old_file(tmp_path, "logs/collector_2026-08-01.log")
    payload = run(tmp_path, apply=False).to_dict()
    assert all(path.exists() for path in (archive, candidate, log))
    for scope in ("archive", "logs", "maintenance_candidates"):
        assert payload[scope]["planned_total"] == 1
        assert payload[scope]["planned_bytes_total"] > 0
        assert payload[scope]["removed_total"] == payload[scope]["removed_bytes_total"] == 0
        assert not payload[scope]["removed"]
    assert payload["accounting"]["removed_logical_bytes"] == 0
    assert payload["accounting"]["net_reclaimed_disk_bytes"] is None


@pytest.mark.parametrize("scope", ["archive", "maintenance_candidates"])
def test_failed_deletion_is_not_in_completed_count_or_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                          scope: str) -> None:
    protected = archive_file(tmp_path) if scope == "archive" else candidate_file(tmp_path)
    target = protected.parent if scope == "archive" else protected
    if scope == "archive":
        def reject(*args, **kwargs):
            raise PermissionError("archive is read-only")
        monkeypatch.setattr(cleanup.shutil, "rmtree", reject)
    else:
        original = Path.unlink

        def reject(path, *args, **kwargs):
            if path == target:
                raise PermissionError("candidate is read-only")
            return original(path, *args, **kwargs)
        monkeypatch.setattr(Path, "unlink", reject)
    payload = run(tmp_path).to_dict()
    assert protected.read_bytes() == b"stored diagnostic evidence\n"
    assert payload[scope]["planned_total"] == payload[scope]["failed_total"] == 1
    assert payload[scope]["removed_total"] == payload[scope]["removed_bytes_total"] == 0
    assert payload[scope]["kept_total"] == 1
    assert payload["errors"]


def test_partial_directory_failure_is_not_reported_as_intact_or_fully_removed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first, second = archive_file(tmp_path, "first.json"), archive_file(tmp_path, "second.json")

    def partially_remove(path):
        first.unlink()
        raise PermissionError("second child could not be removed")

    monkeypatch.setattr(cleanup.shutil, "rmtree", partially_remove)
    payload = run(tmp_path).to_dict()
    assert not first.exists() and second.exists()
    assert payload["archive"]["removed_total"] == 0
    assert payload["archive"]["failed_total"] == 1
    assert payload["accounting"]["partial_removal_possible"] is True
    assert payload["accounting"]["net_logical_bytes"] is None
    assert payload["accounting"]["removed_allocated_bytes_estimate"] is None


def test_preservation_failure_keeps_archive_and_leaves_added_cache_size_unknown(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = archive_file(tmp_path)
    cache = tmp_path / "runs/.forward_validation_cache/preserved.xlsx"

    def preserve_then_fail(*args):
        cache.parent.mkdir(parents=True)
        cache.write_bytes(original.read_bytes())
        raise OSError("cache metadata delivery failed after workbook copy")

    monkeypatch.setattr(forward_validation_cache, "preserve_cache_sources_before_removal", preserve_then_fail)
    payload = run(tmp_path).to_dict()
    assert original.exists() and cache.exists()
    assert payload["archive"]["removed_total"] == 0 and payload["archive"]["failed_total"] == 1
    assert payload["forward_cache_added_bytes"] is None
    assert payload["accounting"]["net_logical_bytes"] is None


def test_removing_a_shared_log_does_not_claim_the_surviving_files_blocks(tmp_path: Path) -> None:
    log = old_file(tmp_path, "logs/collector_2026-08-01.log", b"log data\n" * 10000)
    retained = tmp_path / "retained.log"
    os.link(log, retained)
    original_blocks = retained.stat().st_blocks
    payload = run(tmp_path).to_dict()
    assert not log.exists() and retained.stat().st_blocks == original_blocks
    assert payload["logs"]["removed_bytes_total"] == retained.stat().st_size
    assert payload["logs"]["removed_allocated_bytes_estimate"] == 0


def test_archive_internal_hardlinks_are_counted_once_for_allocated_space(tmp_path: Path) -> None:
    first = archive_file(tmp_path)
    second = first.with_name("duplicate.json")
    os.link(first, second)
    expected_allocated = (first.stat().st_blocks + first.parent.stat().st_blocks) * 512
    logical_size = first.stat().st_size * 2
    payload = run(tmp_path).to_dict()
    assert not first.parent.exists()
    assert payload["archive"]["removed_bytes_total"] == logical_size
    assert payload["archive"]["removed_allocated_bytes_estimate"] == expected_allocated


def test_cache_preservation_hardlink_is_counted_before_archive_deletion(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = archive_file(tmp_path)
    retained = tmp_path / "preserved.xlsx"
    directory_blocks = original.parent.stat().st_blocks * 512

    def preserve(settings, directory):
        os.link(original, retained)
        return {"preserved": [{"new_cache_bytes": 0, "workbook": str(retained)}]}

    monkeypatch.setattr(forward_validation_cache, "preserve_cache_sources_before_removal", preserve)
    payload = run(tmp_path).to_dict()
    assert not original.exists() and retained.exists()
    assert payload["archive"]["removed_allocated_bytes_estimate"] == directory_blocks


def test_sparse_candidate_reports_logical_and_allocated_size_separately(tmp_path: Path) -> None:
    candidate = candidate_file(tmp_path)
    with candidate.open("wb") as stream:
        stream.seek(16 * 1024 * 1024)
        stream.write(b"temporary candidate")
    modified = (NOW - timedelta(days=40)).timestamp()
    os.utime(candidate, (modified, modified))
    info = candidate.stat()
    assert info.st_blocks * 512 < info.st_size
    payload = run(tmp_path).to_dict()
    assert not candidate.exists()
    assert payload["maintenance_candidates"]["removed_bytes_total"] == info.st_size
    assert payload["maintenance_candidates"]["removed_allocated_bytes_estimate"] == info.st_blocks * 512
    assert payload["accounting"]["net_reclaimed_disk_bytes"] is None


@pytest.mark.parametrize("change", ["write", "replace", "missing"])
def test_candidate_is_rechecked_after_planning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                             change: str) -> None:
    candidate = candidate_file(tmp_path)
    planner = cleanup.plan_maintenance_candidate_cleanup

    def changed(*args, **kwargs):
        result = planner(*args, **kwargs)
        if change == "write":
            candidate.write_bytes(b"new candidate generation")
        elif change == "replace":
            replacement = old_file(tmp_path, "replacement.db", b"replacement generation")
            os.replace(replacement, candidate)
        else:
            candidate.unlink()
        return result

    monkeypatch.setattr(cleanup, "plan_maintenance_candidate_cleanup", changed)
    payload = run(tmp_path).to_dict()
    assert candidate.exists() is (change != "missing")
    assert payload["maintenance_candidates"]["removed_total"] == 0
    assert payload["maintenance_candidates"]["failed_total"] == 1
    if change == "missing":
        assert payload["maintenance_candidates"]["kept_total"] == 0


def test_archive_symlink_does_not_delete_or_charge_its_target(tmp_path: Path) -> None:
    target = old_file(tmp_path, "outside/original.json", b"unique original\n" * 10000)
    member = archive_file(tmp_path)
    link = member.with_name("external.json")
    link.symlink_to(target)
    expected = (member.stat().st_blocks + member.parent.stat().st_blocks + link.lstat().st_blocks) * 512
    payload = run(tmp_path).to_dict()
    assert target.exists() and not member.parent.exists()
    assert payload["archive"]["removed_allocated_bytes_estimate"] == expected


def test_symlinked_archive_and_candidate_directories_are_never_cleaned(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    archive = archive_file(outside)
    candidate = candidate_file(outside)
    root = tmp_path / "project"
    root.mkdir()
    (root / "archive").symlink_to(outside / "archive", target_is_directory=True)
    parent = root / "backend/data/maintenance"
    parent.mkdir(parents=True)
    (parent / "nav_history_vacuum").symlink_to(candidate.parent, target_is_directory=True)
    payload = run(root).to_dict()
    assert archive.exists() and candidate.exists()
    assert payload["archive"]["planned_total"] == payload["maintenance_candidates"]["planned_total"] == 0
