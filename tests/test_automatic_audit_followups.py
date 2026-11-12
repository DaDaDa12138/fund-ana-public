"""Synthetic scheduler evidence drills; no real automatic windows are claimed."""
from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend import core_runner
from backend import screening_archive_shadow as shadow
from backend.screening_archive_shadow import observe_due_archive_shadow


def environment(tmp_path, monkeypatch):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")
    config_path = tmp_path / "config/screening_parquet_bindings.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps({"schema_version": 1, "production_reader": "sqlite", "shadow_copies": [{"shard_id": "fixture"}],
                                       "automatic_shadow": {"enabled": True, "interval_hours": 4, "repeats": 3,
                                                            "timeout_seconds": 180, "required_windows": 7}}))
    for name in ("backend/screening_archive_reader.py", "scripts/audit_screening_archive_reader.py",
                 "core/utils/screening_archive_access.py", "core/utils/screening_history_archive_manifest.py",
                 "core/database/screening_decision_payloads.py", "core/database/screening_pick_payloads.py",
                 "backend/screening_archive_shadow.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic code signature fixture")
    calls = []

    def observation_plan(root, bindings):
        mode = 'read_only_integrity' if bindings['production_reader'] == 'parquet' else 'read_only_shadow'
        return {'mode': mode, 'copies': bindings['shadow_copies'], 'copy_modes': [mode]}

    monkeypatch.setattr(shadow, 'archive_observation_plan', observation_plan)

    def run(**kwargs):
        calls.append(kwargs)
        command = kwargs["cmd"]
        signature = command[command.index("--expected-bindings-sha256") + 1]
        artifact = settings.reports_dir / f"audit/shadow_fixture_{len(calls)}.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        mode = "read_only_integrity" if json.loads(config_path.read_text())["production_reader"] == "parquet" else "read_only_shadow"
        plan = observation_plan(tmp_path, json.loads(config_path.read_text()))
        artifact.write_text(json.dumps({"passed": True, "mode": mode, "bindings_sha256": signature,
                                        "observation_plan": plan,
                                        "copies": [{"binding": {"shard_id": "fixture"}, "status": "matched", "aggregation_aligned": True,
                                                    "comparisons": [{"aligned": True}] * 7, "peak_process_rss_kib": 100}]}))
        return SimpleNamespace(return_code=0, duration_seconds=0.1, timed_out=False, success=True,
                               meta_path=str(settings.reports_dir / "metadata.json"),
                               stdout_tail=[json.dumps({"artifact_path": str(artifact.relative_to(tmp_path))})])

    monkeypatch.setattr(core_runner, "run_structured_subprocess", run)
    return settings, calls


def observe(settings, clock_value, *, source="interval"):
    return observe_due_archive_shadow(settings, task_id=123, task_name="scheduled_snapshot_watchdog_refresh",
                                      trigger_source=source, log=lambda text: None, clock=lambda: clock_value)


def test_manual_or_repeated_calls_do_not_manufacture_seven_windows(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    assert observe(settings, 100_000, source="unspecified")["reason"] == "not_an_automatic_trigger"
    first = observe(settings, 100_000)
    assert first["status"] == "matched" and first["consecutive_matched_windows"] == 1
    for _ in range(7):
        duplicate = observe(settings, 100_001)
        assert duplicate["reason"] == "window_already_observed"
        assert duplicate["consecutive_matched_windows"] == 1
    assert len(calls) == 1 and duplicate["observation_requirement_met"] is False
    for offset in range(1, 7):
        result = observe(settings, 100_000 + offset * 14400)
    assert len(calls) == 7 and result["consecutive_matched_windows"] == 7
    assert result["observation_requirement_met"] is True
    assert result["source_retirement_authorized"] is result["production_reader_changed"] is False


def test_changed_or_missing_evidence_invalidates_prior_window(tmp_path, monkeypatch):
    settings, _ = environment(tmp_path, monkeypatch)
    result = observe(settings, 100_000)
    (tmp_path / result["artifact_path"]).write_text("changed")
    next_result = observe(settings, 100_000 + 14400)
    assert next_result["consecutive_matched_windows"] == 1
    ledger = json.loads((settings.reports_dir / "audit/screening_archive_reader/automatic_windows.json").read_text())
    assert ledger["entries"][0]["status"] == "invalid_evidence"


@pytest.mark.parametrize("dependency", ["core/database/screening_pick_payloads.py", "core/database/screening_decision_payloads.py"])
def test_storage_decoder_changes_start_new_observation_windows(tmp_path, monkeypatch, dependency):
    settings, calls = environment(tmp_path, monkeypatch)
    first = observe(settings, 100_000)
    (tmp_path / dependency).write_text("# changed isolated storage decoder")
    second = observe(settings, 100_001)
    assert len(calls) == 2 and first["status"] == second["status"] == "matched"
    assert second["consecutive_matched_windows"] == 1
    assert second["observation_requirement_met"] is False


def test_a_new_reader_version_requires_new_observations(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    observe(settings, 100_000)
    (tmp_path / "backend/screening_archive_reader.py").write_text("# changed fixture")
    result = observe(settings, 100_000)
    assert len(calls) == 2 and result["consecutive_matched_windows"] == 1


def test_production_integrity_windows_do_not_reuse_sqlite_shadow_counts(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    assert observe(settings, 100_000)["mode"] == "read_only_shadow"
    path = tmp_path / "config/screening_parquet_bindings.json"
    binding = json.loads(path.read_text())
    binding["production_reader"] = "parquet"
    path.write_text(json.dumps(binding))
    result = observe(settings, 100_000)
    assert len(calls) == 2
    assert result["mode"] == "read_only_integrity"
    assert result["status"] == "matched"
    assert result["consecutive_matched_windows"] == 1
    assert not result["source_retirement_authorized"]


def test_archive_adapter_change_invalidates_automatic_observations(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    observe(settings, 100_000)
    (tmp_path / "core/utils/screening_archive_access.py").write_text("# changed adapter")
    result = observe(settings, 100_000)
    assert len(calls) == 2 and result["consecutive_matched_windows"] == 1


def test_subprocess_failure_is_saved_without_a_success_count(tmp_path, monkeypatch):
    settings, _ = environment(tmp_path, monkeypatch)

    def fail(**kwargs):
        return SimpleNamespace(return_code=124, duration_seconds=180, timed_out=True, success=False,
                               meta_path=str(settings.reports_dir / "failed.json"))

    monkeypatch.setattr(core_runner, "run_structured_subprocess", fail)
    result = observe(settings, 100_000)
    assert result["status"] == "failed" and result["timed_out"] is True
    assert result["consecutive_matched_windows"] == 0
    assert observe(settings, 100_001)["reason"] == "window_already_observed"


def test_broken_index_is_not_reset_to_a_new_clean_history(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    path = settings.reports_dir / "audit/screening_archive_reader/automatic_windows.json"
    path.parent.mkdir(parents=True)
    path.write_text("{bad")
    with pytest.raises(ValueError):
        observe(settings, 100_000)
    assert path.read_text() == "{bad" and not calls


def test_interrupted_observer_can_retry_without_double_counting_window(tmp_path, monkeypatch):
    settings, calls = environment(tmp_path, monkeypatch)
    result = observe(settings, 100_000)
    path = settings.reports_dir / "audit/screening_archive_reader/automatic_windows.json"
    state = json.loads(path.read_text())
    state["entries"][0]["status"] = "running"
    path.write_text(json.dumps(state))
    retried = observe(settings, 100_001)
    assert len(calls) == 2 and retried["consecutive_matched_windows"] == 1
    entries = json.loads(path.read_text())["entries"]
    assert entries[0]["status"] == "interrupted" and entries[1]["status"] == "matched"
    assert entries[0]["attempt_id"] == result["attempt_id"]


def test_frozen_baseline_subprocess_targets_the_committed_report_and_preserves_it(tmp_path, monkeypatch):
    from backend import workflow

    manager = workflow.WorkflowManager.__new__(workflow.WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "reports/runs",
                                       workflow_task_timeout_seconds={"frozen-baseline-replay": 60})
    logs = []
    manager.store = SimpleNamespace(set_phase=lambda phase: None, append_log=logs.append)
    report_path = tmp_path / "report.json"
    report_path.write_text('{"fixture":"frozen"}')
    before = report_path.read_bytes()
    monkeypatch.setattr(workflow, "resolve_committed_path", lambda settings, path: report_path)
    monkeypatch.setattr(core_runner, "build_subprocess_log_index", lambda **kwargs: None)
    calls = []

    def run(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(success=False, return_code=2, meta_path="fixture-meta")

    monkeypatch.setattr(core_runner, "run_structured_subprocess", run)
    manager._run_frozen_baseline_audit({"run_id": "fixture", "artifacts": {"screening_input_snapshot_json_gz": "snapshot.gz",
                                                                           "report_json": "report.json"}})
    assert calls[0]["cmd"][-2:] == ["--report", str(report_path)]
    assert calls[0]["timeout"] == 60
    assert report_path.read_bytes() == before
    assert any("未通过" in message for message in logs)
    assert not any("核对通过" in message for message in logs)


@pytest.mark.parametrize("shadow_skipped", [False, True])
def test_automatic_baseline_runs_once_after_the_last_commit(tmp_path, monkeypatch, shadow_skipped):
    from backend import workflow

    manager = workflow.WorkflowManager.__new__(workflow.WorkflowManager)
    manager.settings = SimpleNamespace(tiantian_base_url="fixture", autostock_base_url="fixture")
    manager.store = SimpleNamespace(set_phase=lambda phase: None, append_log=lambda text: None)
    report = {"run_id": "fixture", "artifacts": {"report_markdown": "fixture.md"}}
    events = []
    monkeypatch.setattr(workflow, "run_project_pipeline", lambda *args: {})
    monkeypatch.setattr(workflow, "build_report_payload", lambda **kwargs: events.append("build") or report)
    monkeypatch.setattr(manager, "_run_annual_return_gate_shadow_audit", lambda run, **kwargs: events.append("shadow"))
    monkeypatch.setattr(manager, "_publish_formal_report", lambda **kwargs: events.append("commit") or ({"latest_canonical_run_id": "fixture"}, report))
    monkeypatch.setattr(manager, "_run_frozen_baseline_audit", lambda result: events.append("baseline"))
    assert manager._execute("fixture", {"skip_annual_return_gate_shadow_audit": shadow_skipped}) == report
    expected = ["build", "commit", "baseline"] if shadow_skipped else ["build", "commit", "shadow", "baseline"]
    assert events == expected
