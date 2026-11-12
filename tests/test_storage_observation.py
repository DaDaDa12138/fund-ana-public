"""Filesystem/accounting and scheduling drills, with no real fund data or API calls."""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend import storage_observation as storage

BASE = datetime(2026, 9, 9, 1, tzinfo=timezone.utc).timestamp()


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / "project"
    value = SimpleNamespace(
        project_root=root, timezone="Asia/Shanghai", config_path=root / "config/project.yaml",
        database_path=root / "backend/data/example.db", reports_dir=root / "reports",
        runs_dir=root / "reports/runs", archived_runs_dir=root / "reports/archive/runs",
        core_output_dir=root / "reports/core_output", core_root=root / "core",
        latest_report_file=root / "reports/latest.json",
        publication_registry_file=root / "reports/publication_registry.json",
        autostock_rate_limit_shared_state_path=root / "backend/data/budget.json",
        storage_observation_enabled=True, storage_observation_interval_hours=6,
        storage_observation_timeout_seconds=45, storage_observation_retain_observations=28,
    )
    for path in (value.database_path.parent, value.runs_dir, value.archived_runs_dir, value.core_output_dir,
                 root / "logs", root / "core/logs", value.config_path.parent):
        path.mkdir(parents=True, exist_ok=True)
    for path in (value.database_path, value.latest_report_file, value.publication_registry_file,
                 value.autostock_rate_limit_shared_state_path):
        path.write_bytes(b"filesystem fixture only")
    return value


def write(path: Path, size: int = 4096) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


def observe(settings, when=BASE, source="interval"):
    return storage.observe_due_storage_usage(settings, task_id=7, task_name="synthetic_storage_task",
                                             trigger_source=source, log=lambda text: None, clock=lambda: when)


@pytest.fixture
def worker(settings, monkeypatch):
    state = SimpleNamespace(when=BASE, calls=[])

    def run(command, **kwargs):
        state.calls.append((command, kwargs))
        result = storage.collect_storage_snapshot(settings, clock=lambda: state.when)
        return SimpleNamespace(stdout=json.dumps(result), stderr="", returncode=0 if result["status"] == "complete" else 2)

    monkeypatch.setattr(storage.subprocess, "run", run)
    return state


def test_allocation_matches_du_and_does_not_follow_symlinks_or_double_count(settings, tmp_path):
    data = settings.database_path.parent
    compressed = write(data / "backups/example.db.zst", 8192)
    write(data / "archives/cold.parquet", 8192)
    write(data / "archives/recovery.db.zst", 4096)
    write(data / "archives/active.db", 4096)
    write(Path(str(settings.database_path) + "-wal"), 4096)
    write(settings.reports_dir / "audit/case/run/workspace/copy.py")
    write(settings.reports_dir / "audit/case/recovery.db.zst")
    outside = write(tmp_path / "outside/large.bin", 1_000_000)
    (settings.reports_dir / "external-directory").symlink_to(outside.parent, target_is_directory=True)
    (settings.reports_dir / "external-file").symlink_to(outside)
    (settings.reports_dir / "duplicate").hardlink_to(compressed)
    with (data / "sparse.bin").open("wb") as stream:
        stream.truncate(8 * 1024 * 1024)
    result = storage.collect_storage_snapshot(settings)
    paths = [data, settings.reports_dir, settings.project_root / "logs", settings.core_root / "logs"]
    du = subprocess.check_output(["du", "-s", "-B1", *map(str, paths)], text=True)
    assert result["status"] == "complete" and result["error_count"] == 0
    assert result["totals"]["allocated_bytes"] == sum(int(line.split()[0]) for line in du.splitlines())
    assert result["totals"]["allocated_bytes"] == sum(row["allocated_bytes"] for row in result["buckets"].values())
    assert result["totals"]["logical_bytes"] > result["totals"]["allocated_bytes"]
    assert result["totals"]["duplicate_inode_entries"] == 1
    assert result["totals"]["symlinks"] == 2
    assert len(result["roots"]) == 4
    assert result["buckets"]["backups_compressed"]["allocated_bytes"] == compressed.stat().st_blocks * 512
    assert result["buckets"]["test_workspaces"]["regular_files"] == 1
    assert result["buckets"]["audit_database_copies"]["regular_files"] == 1
    assert result["buckets"]["online_journals"]["regular_files"] == 1
    assert result["legacy_logical_metrics"]["database_bytes"] == settings.database_path.stat().st_size


def test_root_symlink_is_not_scanned_as_an_empty_or_complete_directory(settings, tmp_path):
    external = write(tmp_path / "external/data.bin", 8192)
    (settings.project_root / "logs").rmdir()
    (settings.project_root / "logs").symlink_to(external.parent, target_is_directory=True)
    snapshot = storage.collect_storage_snapshot(settings)
    assert snapshot["status"] == "partial" and snapshot["totals"]["allocated_bytes"] is None
    assert any("storage_scope_has_symlink" in error["reason"] for error in snapshot["errors"])


def test_frozen_inputs_revisions_and_historical_evidence_have_separate_budgets(settings):
    paths = {
        "run_calculation_inputs": settings.runs_dir / "synthetic/calculation_inputs/operand.pack",
        "run_revisions": settings.runs_dir / "synthetic/revisions/revision/result.csv",
        "audit_implementation": settings.reports_dir / "audit/implementation_20260907/evidence.json",
        "audit_research": settings.reports_dir / "audit/annual_return_gate_shadow/evidence.json",
        "calculation_pool": settings.reports_dir / ".calculation_input_pool/catalog.sqlite",
        "test_source_pool": settings.reports_dir / ".test_source_pool/source.gz",
        "report_artifact_pool": settings.reports_dir / ".report_artifact_pool/artifact",
    }
    for path in paths.values():
        write(path)
    snapshot = storage.collect_storage_snapshot(settings)
    assert snapshot["status"] == "complete"
    for bucket in paths:
        assert snapshot["buckets"][bucket]["regular_files"] == 1
    assert snapshot["totals"]["allocated_bytes"] == sum(
        item["allocated_bytes"] for item in snapshot["buckets"].values())


def test_unreadable_directory_keeps_partial_counts_and_null_complete_totals(settings, monkeypatch):
    denied = settings.reports_dir.stat().st_ino
    original = storage.os.scandir

    def scan(fd):
        if os.fstat(fd).st_ino == denied:
            raise PermissionError("isolated permission drill")
        return original(fd)

    monkeypatch.setattr(storage.os, "scandir", scan)
    result = storage.collect_storage_snapshot(settings)
    assert result["status"] == "partial"
    assert result["totals"]["allocated_bytes"] is result["totals"]["logical_bytes"] is None
    assert result["totals"]["observed_allocated_bytes"] > 0
    assert result["legacy_logical_metrics"]["runs_dir_bytes"] is None
    assert result["error_count"] == 1


def test_scanner_budget_exhaustion_never_reports_zero_usage_as_complete(settings):
    counter = iter([0, *([100] * 100)])
    result = storage.collect_storage_snapshot(settings, max_seconds=1, monotonic=lambda: next(counter))
    assert result["status"] == "partial" and result["totals"]["allocated_bytes"] is None
    assert result["errors"][0]["reason"] == "scan_resource_budget_exhausted"


def test_missing_database_remains_unknown(settings):
    settings.database_path.unlink()
    result = storage.collect_storage_snapshot(settings)
    assert result["status"] == "partial"
    assert result["legacy_logical_metrics"]["database_bytes"] is None


def test_manual_and_repeated_requests_cannot_manufacture_natural_history(settings, worker):
    assert observe(settings, source="manual")["reason"] == "not_an_automatic_trigger"
    assert not storage._index_path(settings).exists()
    first = observe(settings)
    assert first["status"] == "complete"
    for offset in range(1, 8):
        assert observe(settings, BASE + offset)["reason"] == "window_already_attempted"
    assert len(worker.calls) == 1
    result = storage.load_storage_usage(settings, clock=lambda: BASE)
    assert result["status"] == "ok"
    assert result["growth"]["complete_natural_window_count"] == result["growth"]["distinct_local_dates"] == 1
    assert result["growth"]["average_net_allocated_bytes_per_day"] is None


def test_growth_requires_seven_dates_and_preserves_negative_real_changes(settings, worker):
    large = write(settings.reports_dir / "audit/temporary.bin", 2_000_000)
    observe(settings)
    large.unlink()
    for day in range(1, 7):
        worker.when = BASE + day * 86400
        observe(settings, worker.when)
        result = storage.load_storage_usage(settings, clock=lambda: worker.when)
        if day < 6:
            assert result["growth"]["average_net_allocated_bytes_per_day"] is None
    growth = result["growth"]
    assert growth["status"] == "observed" and growth["distinct_local_dates"] == 7
    assert growth["elapsed_days"] == 6
    assert growth["net_allocated_change_bytes"] < 0
    assert growth["average_net_allocated_bytes_per_day"] < 0
    assert growth["by_bucket"]["audit_other"]["net_allocated_change_bytes"] < 0


def test_index_is_bounded_and_new_schema_signature_cannot_reuse_old_growth(settings, worker, monkeypatch):
    for offset in range(35):
        worker.when = BASE + offset * 21600
        observe(settings, worker.when)
    assert len(json.loads(storage._index_path(settings).read_text())["observations"]) == 28
    monkeypatch.setattr(storage, "measurement_signature", lambda settings: "changed-definition")
    result = storage.load_storage_usage(settings, clock=lambda: worker.when)
    assert result["status"] == "not_observed" and result["snapshot"] is None
    assert result["growth"]["complete_natural_window_count"] == 0


def test_failed_attempt_retains_previous_complete_snapshot_and_retries_next_window(settings, worker, monkeypatch):
    observe(settings)
    before = storage.load_storage_usage(settings, clock=lambda: BASE)["snapshot"]

    def timeout(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(storage.subprocess, "run", timeout)
    failed = observe(settings, BASE + 21600)
    assert failed["status"] == "failed" and failed["timed_out"] is True
    result = storage.load_storage_usage(settings, clock=lambda: BASE + 21600)
    assert result["status"] == "degraded" and result["snapshot"] == before
    assert result["last_attempt"]["status"] == "failed"
    assert observe(settings, BASE + 21601)["last_attempt_status"] == "failed"
    stale = storage.load_storage_usage(settings, clock=lambda: BASE + 86400)
    assert stale["status"] == "stale" and stale["stale"] is True


def test_partial_scan_and_subprocess_contract_errors_are_not_success(settings, worker, monkeypatch):
    settings.database_path.unlink()
    result = observe(settings)
    assert result["status"] == "partial" and result["allocated_bytes"] is None
    api = storage.load_storage_usage(settings, clock=lambda: BASE)
    assert api["status"] == "partial" and api["database_bytes"] is None
    assert api["snapshot"]["totals"]["observed_allocated_bytes"] > 0
    assert api["growth"]["complete_natural_window_count"] == 0
    monkeypatch.setattr(storage.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stdout='{"schema_version": 1, "status": "complete"}', stderr=""))
    result = observe(settings, BASE + 21600)
    assert result["status"] == "failed" and "contract_mismatch" in result["error"]


def test_interrupted_process_is_recorded_before_retry(settings, worker):
    observe(settings)
    path = storage._index_path(settings)
    state = json.loads(path.read_text())
    state["observations"][-1].update(status="running")
    path.write_text(json.dumps(state))
    assert observe(settings)["status"] == "complete"
    entries = json.loads(path.read_text())["observations"]
    assert len(entries) == 2 and entries[0]["status"] == "interrupted"
    assert storage.load_storage_usage(settings, clock=lambda: BASE)["growth"]["complete_natural_window_count"] == 1


@pytest.mark.parametrize("body", ["{invalid", "[]", '{"schema_version": 1, "observations": [null]}'])
def test_corrupt_index_is_not_silently_reset_or_exposed_as_zero(settings, worker, body):
    path = storage._index_path(settings)
    path.parent.mkdir(parents=True)
    path.write_text(body)
    result = storage.load_storage_usage(settings)
    assert result["status"] == "unavailable" and result["database_bytes"] is None
    with pytest.raises(ValueError):
        observe(settings)
    assert path.read_text() == body and not worker.calls


def test_saved_snapshot_tampering_is_detected(settings, worker):
    observe(settings)
    path = storage._index_path(settings)
    state = json.loads(path.read_text())
    state["observations"][-1]["snapshot"]["totals"]["allocated_bytes"] += 1
    path.write_text(json.dumps(state))
    assert "snapshot_hash_mismatch" in storage.load_storage_usage(settings)["error"]


def test_competing_observer_does_not_spawn_a_second_scan(settings, worker):
    path = storage._index_path(settings)
    path.parent.mkdir(parents=True)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert observe(settings)["reason"] == "storage_observer_busy"
    assert not worker.calls


def test_api_reads_only_saved_metadata_and_preserves_unknown_before_observation(settings, worker, monkeypatch):
    from backend import app as app_module
    monkeypatch.setattr(app_module, "settings", settings)
    response = app_module.app.test_client().get("/api/storage/usage")
    assert response.status_code == 200
    assert response.json["status"] == "not_observed" and response.json["database_bytes"] is None
    observe(settings)
    monkeypatch.setattr(storage, "collect_storage_snapshot", Mock(side_effect=AssertionError("API must not scan")))
    monkeypatch.setattr(storage.os, "scandir", Mock(side_effect=AssertionError("API must not walk directories")))
    response = app_module.app.test_client().get("/api/storage/usage")
    assert response.status_code == 200 and response.json["snapshot"]["status"] == "complete"
    assert "no-store" in response.headers["Cache-Control"]


@pytest.mark.parametrize("business_status", ["skipped", "degraded", "failed", "success"])
def test_observer_failure_does_not_replace_business_outcome(settings, monkeypatch, business_status):
    from backend.scheduler import ProjectScheduler
    from backend import screening_archive_shadow
    instance = ProjectScheduler.__new__(ProjectScheduler)
    instance.settings = settings
    instance._manual_run_lock = threading.Lock()
    instance._startup_recovery_context = threading.local()
    instance.state_store = SimpleNamespace(start_task=Mock())
    instance.task_recorder = SimpleNamespace(start_task=lambda name: {"id": 3})
    for name in ("_ensure_runtime_host_allowed", "_write_heartbeat_file", "_clear_task_log_context",
                 "_enqueue_post_task_catch_up_if_needed", "_append_task_log_line"):
        setattr(instance, name, Mock())
    instance._enter_task_log_context = lambda **kwargs: "isolated.log"
    instance._host_context = lambda: "isolated_fixture"
    instance._workflow_summary_for_task_audit = lambda: {"status": "success"}
    instance._finalize_task = lambda **kwargs: kwargs["result"]
    monkeypatch.setattr(screening_archive_shadow, "observe_due_archive_shadow", lambda *args, **kwargs: {"status": "skipped"})
    monkeypatch.setattr(storage, "observe_due_storage_usage", Mock(side_effect=OSError("isolated disk error")))
    result = instance._run_task("synthetic_task", lambda: {"status": business_status}, trigger_source="interval")
    assert result["status"] == business_status and result["storage_observation"]["status"] == "failed"
    assert not instance._manual_run_lock.locked()
