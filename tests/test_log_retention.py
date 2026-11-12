"""Retention covers both actual writer directories without deleting active logs."""
from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import cleanup_disk_usage as cleanup


NOW = datetime(2026, 9, 12, 13, 0)


def log_file(root: Path, name: str, age: int = 40) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text("retained diagnostic content\n", encoding="utf-8")
    modified = (NOW - timedelta(days=age)).timestamp()
    os.utime(path, (modified, modified))
    return path


def settings(root: Path, core: Path | None = None) -> SimpleNamespace:
    return SimpleNamespace(project_root=root, core_root=core or root / "core",
                           archived_runs_dir=root / "archive", runs_dir=root / "runs")


def test_log_retention_covers_configured_core_root_and_preserves_task_evidence(tmp_path: Path) -> None:
    core = tmp_path / "configured_engine"
    old_web = log_file(tmp_path / "logs", "database_2026-08-01.log")
    old_core = log_file(core / "logs", "collector_2026-08-01.log.2")
    task = log_file(core / "logs/tasks", "task_2026-08-01.log")
    evidence = log_file(tmp_path / "reports/.audit/subprocess_logs", "collector_2026-08-01.log")
    fresh = log_file(core / "logs", "database_2026-09-12.log", age=0)
    unrelated = log_file(tmp_path / "core/logs", "database_2026-08-01.log")
    config = settings(tmp_path, core)
    preview = cleanup.run_cleanup(config, apply=False, now=NOW)
    assert {item.path for item in preview.logs_removed} == {old_web, old_core}
    assert old_web.exists() and old_core.exists()
    result = cleanup.run_cleanup(config, apply=True, now=NOW)
    assert not old_web.exists() and not old_core.exists()
    assert all(path.exists() for path in (task, evidence, fresh, unrelated))
    assert not result.errors
    assert result.to_dict()["logs"]["roots"] == [str(tmp_path / "logs"), str(core / "logs")]
    assert result.to_dict()["logs"]["removed_total"] == 2


def test_log_retention_preserves_recently_written_old_name_boundary_and_unknown(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    active = log_file(logs, "database_2026-07-01.log", age=0)
    boundary = log_file(logs, "collector_2026-08-29.log", age=40)
    recently_written = log_file(logs, "api_2026-08-01.log", age=14)
    unknown = log_file(logs, "manual_analyzer_债券_30day_20260801.log")
    malformed = log_file(logs, "database_2026-02-30.log")
    old = log_file(logs, "database_2026-08-01.log", age=40)
    kept, removed = cleanup.plan_logs_cleanup(logs, keep_days=14, now=NOW)
    assert set(kept) == {active, boundary, recently_written, unknown, malformed}
    assert [item.path for item in removed] == [old]


def test_analyzer_logs_use_existing_retention_in_both_writer_directories(tmp_path: Path) -> None:
    old_short = log_file(tmp_path / "logs", "analyzer_债券_30day_20260801.log")
    old_long = log_file(tmp_path / "core/logs", "analyzer_可转债_3year_20260801.log")
    active = log_file(tmp_path / "core/logs", "analyzer_债券_90day_20260801.log", age=0)
    boundary = log_file(tmp_path / "logs", "analyzer_债券_1year_20260829.log")
    task = log_file(tmp_path / "logs/tasks", "analyzer_债券_30day_20260801.log")
    config = settings(tmp_path)
    preview = cleanup.run_cleanup(config, apply=False, logs_only=True, now=NOW)
    assert {item.path for item in preview.logs_removed} == {old_short, old_long}
    assert old_short.exists() and old_long.exists()
    result = cleanup.run_cleanup(config, apply=True, logs_only=True, now=NOW)
    assert not old_short.exists() and not old_long.exists()
    assert all(path.exists() for path in (active, boundary, task))
    assert result.to_dict()["logs"]["removed_total"] == 2
    assert not result.errors


@pytest.mark.parametrize("name", [
    "analyzer_债券_30day_20260230.log",
    "analyzer_债券_30day_20260801.log.1",
    "analyzer_债券_30day_20260801.log.backup",
    "analyzer_债券_30day_20260801.log\n",
    "analyzer_债券_0day_20260801.log",
    "analyzer_债券_unknown_20260801.log",
    "analyzer__30day_20260801.log",
    "custom_20260801.log",
])
def test_unknown_or_invalid_analyzer_names_are_preserved(tmp_path: Path, name: str) -> None:
    path = log_file(tmp_path, name)
    kept, removed = cleanup.plan_logs_cleanup(tmp_path, keep_days=14, now=NOW)
    assert kept == [path] and removed == []


def test_log_retention_never_follows_directory_or_file_symlinks(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    target = log_file(outside, "database_2026-08-01.log")
    project = tmp_path / "project"
    project.mkdir()
    (project / "logs").symlink_to(outside, target_is_directory=True)
    core_logs = project / "core/logs"
    core_logs.mkdir(parents=True)
    link = core_logs / target.name
    link.symlink_to(target)
    result = cleanup.run_cleanup(settings(project), apply=True, now=NOW)
    assert target.exists() and link.is_symlink() and (project / "logs").is_symlink()
    assert not result.logs_removed


def test_log_retention_deduplicates_same_configured_directory(tmp_path: Path) -> None:
    old = log_file(tmp_path / "logs", "database_2026-08-01.log")
    result = cleanup.run_cleanup(settings(tmp_path, tmp_path), apply=True, now=NOW)
    assert not old.exists() and not result.errors
    assert len(result.logs_removed) == 1
    assert len(result.log_roots) == 1


def test_log_retention_logs_only_does_not_touch_archive_or_publication(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    old = log_file(tmp_path / "core/logs", "database_2026-08-01.log")
    archived = log_file(tmp_path / "archive/20260101_010000_sync", "report.json")

    def unrelated_operation(*args, **kwargs):
        raise AssertionError("logs-only must not plan archives, maintenance or refresh publication")

    monkeypatch.setattr(cleanup, "plan_archive_cleanup", unrelated_operation)
    monkeypatch.setattr(cleanup, "plan_maintenance_candidate_cleanup", unrelated_operation)
    monkeypatch.setattr(cleanup, "_settings_support_publication_registry_refresh", unrelated_operation)
    result = cleanup.run_cleanup(settings(tmp_path), logs_only=True, apply=True, now=NOW)
    assert not old.exists() and archived.exists() and not result.errors
    assert result.to_dict()["scope"] == "dated_logs"
    assert not result.publication_registry_refreshed


@pytest.mark.parametrize("change", ["append", "replace", "symlink"])
def test_log_retention_rechecks_identity_before_deleting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                        change: str) -> None:
    path = log_file(tmp_path / "logs", "database_2026-08-01.log")
    original = cleanup.plan_maintenance_candidate_cleanup

    def change_after_log_planning(*args, **kwargs):
        result = original(*args, **kwargs)
        if change == "append":
            with path.open("a") as stream:
                stream.write("new evidence\n")
        else:
            target = log_file(tmp_path / "preserved", path.name)
            if change == "replace":
                os.replace(target, path)
            else:
                path.unlink()
                path.symlink_to(target)
        return result

    monkeypatch.setattr(cleanup, "plan_maintenance_candidate_cleanup", change_after_log_planning)
    result = cleanup.run_cleanup(settings(tmp_path), apply=True, now=NOW)
    assert path.exists() and path in result.logs_kept
    assert result.to_dict()["logs"]["removed_total"] == 0
    assert result.errors


def test_log_retention_does_not_report_failed_unlink_as_removed(tmp_path: Path,
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    path = log_file(tmp_path / "logs", "database_2026-08-01.log")
    original = Path.unlink

    def failed_unlink(self, *args, **kwargs):
        if self == path:
            raise PermissionError("read-only log")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", failed_unlink)
    result = cleanup.run_cleanup(settings(tmp_path), apply=True, now=NOW)
    assert path.exists() and not result.logs_removed
    assert result.errors and result.to_dict()["logs"]["removed_bytes_total"] == 0
