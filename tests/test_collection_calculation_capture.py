"""Synthetic lifecycle tests; these are not evidence of production collection."""
from __future__ import annotations

from datetime import datetime, timedelta
import io
import json
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from backend import calculation_input_capture as binding
from backend import collection_calculation_capture as collection
from backend import core_runner
from core.utils import calculation_capture as capture


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    root = tmp_path / "synthetic_project"
    core = root / "core"
    core.mkdir(parents=True)
    source = core / "main.py"
    source.write_text("# synthetic calculation child for provenance tests\n")
    output = root / "reports/core_output"
    output.mkdir(parents=True)
    settings = SimpleNamespace(
        project_root=root, core_root=core, core_output_dir=output, reports_dir=root / "reports",
        runs_dir=root / "reports/runs", database_path=root / "unused.db",
        tiantian_base_url="http://invalid.test", run_collection_by_default=True,
        run_screening_by_default=True,
    )
    monkeypatch.delenv("FUND_ANA_WORKFLOW_RUN_ID", raising=False)
    monkeypatch.delenv(capture.ENV_CAPTURE_RUN_ID, raising=False)
    monkeypatch.delenv(capture.ENV_CAPTURE_SCOPE, raising=False)
    monkeypatch.setattr(capture, "PROJECT_ROOT", root)
    monkeypatch.setattr(core_runner, "ensure_core_output_link", lambda settings: output)
    monkeypatch.setattr(core_runner, "discover_core_artifacts", lambda output: {"core_output_dir": str(output)})
    monkeypatch.setattr(core_runner, "build_core_commands", lambda settings, options: [
        core_runner.StepResult("calculate-risk", [sys.executable, "main.py", "calculate-risk", "--yes"])
    ])
    state = SimpleNamespace(settings=settings, logs=[], failure=None, commands=[])

    def child(*, name, cmd, cwd, env, log_dir, **kwargs):
        state.commands.append((cmd, dict(env)))
        if state.failure == "before_child":
            raise RuntimeError("synthetic upstream failure")
        started = datetime.now().astimezone().isoformat()
        with monkeypatch.context() as child_patch:
            child_patch.chdir(cwd)
            child_patch.setattr(sys, "argv", cmd[1:])
            for key in ("FUND_ANA_WORKFLOW_RUN_ID", capture.ENV_DIRECTORY, "FUND_ANA_CALCULATION_SHARED_DIR",
                        capture.ENV_CAPTURE_RUN_ID, capture.ENV_CAPTURE_SCOPE):
                if key in env:
                    child_patch.setenv(key, env[key])
                else:
                    child_patch.delenv(key, raising=False)
            writer = capture.CaptureWriter(Path(env[capture.ENV_DIRECTORY]), "database_risk", {}, code_paths=[source])
            writer.event("synthetic_schema", pd.DataFrame({"missing_or_zero": [None, 0.0]}))
            if state.failure == "interrupted":
                writer._close()
            else:
                writer.finish("raised" if state.failure == "failed_child" else "returned")
        failed = state.failure in {"interrupted", "failed_child"}
        meta = {
            "task_name": name, "command": cmd, "cwd": str(cwd), "return_code": 1 if failed else 0,
            "success": not failed, "timed_out": False, "started_at": started,
            "finished_at": datetime.now().astimezone().isoformat(),
        }
        (log_dir / "synthetic.meta.json").write_text(json.dumps(meta))
        if failed:
            raise core_runner.CoreStepFailedError(stage=name, return_code=1,
                                                  meta_path=str(log_dir / "synthetic.meta.json"))
        return []

    monkeypatch.setattr(core_runner, "_run_streaming_subprocess", child)
    state.run = lambda: core_runner.run_core_pipeline(settings, {
        "run_collection": True, "run_screening": False, "collection_profile": "daily",
    }, state.logs.append)
    return state


def task_context(task_id=123):
    return collection.collection_task_context(
        task_id=task_id, task_name="synthetic_daily_collection",
        started_at=(datetime.now().astimezone() - timedelta(seconds=1)).isoformat(), trigger_source="synthetic",
    )


def test_collection_without_workflow_keeps_inputs_and_exact_task_binding(pipeline):
    with task_context() as state:
        artifacts = pipeline.run()
        references = collection.completed_collection_captures()
    assert len(references) == len(state["attempts"]) == 1
    reference = references[0]
    checked = collection.validate_collection_capture_reference(pipeline.settings, reference)
    assert checked["task"]["task_history_id"] == 123
    assert checked["source_workflow_run_id"] is None
    assert checked["pipeline_outcome"] == "succeeded"
    assert checked["scientific_reproduction_status"] == "not_verified"
    assert collection.completed_collection_captures() == []
    assert "FUND_ANA_WORKFLOW_RUN_ID" not in pipeline.commands[0][1]
    assert pipeline.commands[0][1][capture.ENV_CAPTURE_SCOPE] == "collection"
    index = Path(artifacts["calculation_input_index_file"])
    payload = binding.validate_calculation_input_index(index, full_journals=True)
    assert payload["capture_scope"] == "collection" and payload["expected_scopes"] == {"database_risk": 1}
    journal = payload["sessions"][0]
    assert journal["schema_version"] == 3
    assert journal["storage"]["replay_dependency"] == "capture_local_files_only"
    rows = list(capture.iter_journal(index.parent / journal["file"]))
    frame = capture.decode(next(row["value"] for row in rows if row["record"] == "object"))
    assert pd.isna(frame.iloc[0, 0]) and frame.iloc[1, 0] == 0.0
    # A validated reference can be checked again when freezing the report.
    assert collection.validate_collection_capture_reference(pipeline.settings, checked) == checked


def test_standalone_pipeline_has_no_invented_scheduler_or_workflow(pipeline):
    artifacts = pipeline.run()
    reference = artifacts["collection_calculation_capture"]
    assert reference["task"] is None and reference["source_workflow_run_id"] is None
    assert collection.validate_collection_capture_reference(pipeline.settings, reference)["capture_complete"]


def test_repeated_attempts_have_distinct_paths_and_do_not_overwrite_inputs(pipeline):
    with task_context() as state:
        first = pipeline.run()
        first_path = Path(first["calculation_input_index_file"])
        before = first_path.read_bytes()
        second = pipeline.run()
    assert len(state["attempts"]) == 2
    assert first_path != Path(second["calculation_input_index_file"])
    assert first_path.read_bytes() == before
    assert len({row["capture_run_id"] for row in state["attempts"]}) == 2


@pytest.mark.parametrize("failure", ["before_child", "interrupted", "failed_child"])
def test_collection_failure_keeps_terminal_and_never_advertises_complete_inputs(pipeline, failure):
    pipeline.failure = failure
    with task_context() as state:
        with pytest.raises((RuntimeError, core_runner.CoreStepFailedError)):
            pipeline.run()
        assert collection.completed_collection_captures() == []
    record = state["attempts"][0]
    terminal = json.loads((pipeline.settings.project_root / record["terminal_path"]).read_text())
    assert terminal["pipeline_outcome"] == "failed" and not terminal["capture_complete"]
    assert terminal["pipeline_error"] and terminal["capture_error"]
    assert not list((pipeline.settings.project_root / record["execution_path"]).parent.glob(
        "calculation_inputs/calculation_input_index_*.json"))
    if failure == "interrupted":
        assert list((pipeline.settings.project_root / record["execution_path"]).parent.glob(
            "calculation_inputs/*.partial"))


@pytest.mark.parametrize("field,value", [
    ("capture_run_id", "collect_20260913_000000_" + "0" * 32),
    ("task", {"task_history_id": 999}),
    ("index_sha256", "0" * 64),
    ("execution_path", "../outside.json"),
    ("pipeline_outcome", "failed"),
    ("source_workflow_run_id", "invented_workflow"),
])
def test_collection_reference_rejects_wrong_owner_source_and_path(pipeline, field, value):
    with task_context():
        pipeline.run()
        reference = collection.completed_collection_captures()[0]
    reference[field] = value
    with pytest.raises(binding.CalculationCaptureError):
        collection.validate_collection_capture_reference(pipeline.settings, reference)


def test_scheduler_hands_collection_reference_to_workflow_cli(pipeline, monkeypatch):
    from backend.scheduler import ProjectScheduler

    service = ProjectScheduler.__new__(ProjectScheduler)
    service.settings = pipeline.settings
    monkeypatch.setattr(service, "_should_use_workflow_manager_fallback", lambda: False)
    monkeypatch.setattr(service, "_python_bin", lambda: sys.executable)
    commands = []

    def popen(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(stdout=io.StringIO('{"run_id":"synthetic_later_workflow"}\n'), wait=lambda: 0)

    monkeypatch.setattr("backend.scheduler.subprocess.Popen", popen)
    with task_context():
        pipeline.run()
        expected = collection.completed_collection_captures()[0]
        service._run_workflow_in_subprocess({"run_collection": False, "run_screening": True})
    command = commands[0]
    assert json.loads(command[command.index("--collection-calculation-capture") + 1]) == expected
    assert "--skip-collection" in command


@pytest.mark.parametrize("fault", [None, "wrong_workflow", "wrong_task", "changed_sha", "missing_reference"])
def test_auditor_binds_parent_capture_without_equating_workflow_ids(pipeline, fault):
    from copy import deepcopy
    from scripts.audit_data_truthfulness import _refresh_collection_index_from_task_log

    with task_context():
        pipeline.run()
        reference = collection.completed_collection_captures()[0]
    reference = deepcopy(reference)
    root = pipeline.settings.project_root
    index_path = (root / reference["execution_path"]).parent / "logs/subprocess/subprocess_index.json"
    task_log = root / "parent.log"
    task_log.write_text(f"subprocess_index={index_path} count=1 failed=0\n")
    workflow_id = "synthetic_later_workflow"
    task = deepcopy(reference["task"])
    if fault == "wrong_task":
        task["task_history_id"] += 1
    if fault == "changed_sha":
        reference["index_sha256"] = "0" * 64
    task_audit = {"task": task, "workflow_summary": {
        "run_id": "wrong_workflow" if fault == "wrong_workflow" else workflow_id,
        "options": {"collection_calculation_captures": [] if fault == "missing_reference" else [reference]},
    }}
    _, _, checked, errors, _ = _refresh_collection_index_from_task_log(
        pipeline.settings, task_log_path_raw=str(task_log), run_id=workflow_id,
        required_tasks=(), task_audit=task_audit,
    )
    binding_errors = [e for e in errors if "capture_binding_invalid" in e or "run_id_mismatch" in e]
    if fault is None:
        assert not binding_errors
        assert checked["parent_capture_binding"]["verified"] is True
        assert checked["parent_capture_binding"]["capture_run_id"] != workflow_id
    else:
        assert binding_errors
        assert not checked["parent_capture_binding"]


@pytest.mark.parametrize("failed", [False, True])
def test_scheduler_result_keeps_capture_task_identity_even_when_runner_raises(pipeline, monkeypatch, failed):
    from backend.scheduler import ProjectScheduler

    service = ProjectScheduler.__new__(ProjectScheduler)
    service.settings = pipeline.settings
    service._manual_run_lock = threading.Lock()
    service._startup_recovery_context = SimpleNamespace()
    service.state_store = SimpleNamespace(start_task=lambda name: None)
    service.task_recorder = SimpleNamespace(start_task=lambda name: {"id": 456})
    for name in ("_ensure_runtime_host_allowed", "_write_heartbeat_file", "_clear_task_log_context"):
        monkeypatch.setattr(service, name, lambda: None)
    monkeypatch.setattr(service, "_enter_task_log_context", lambda **kwargs: str(pipeline.settings.project_root / "task.log"))
    monkeypatch.setattr(service, "_append_task_log_line", lambda message: None)
    monkeypatch.setattr(service, "_enqueue_post_task_catch_up_if_needed", lambda **kwargs: None)
    monkeypatch.setattr(service, "_host_context", lambda: "synthetic")
    monkeypatch.setattr(service, "_finalize_task", lambda **kwargs: kwargs["result"])

    def run():
        pipeline.run()
        if failed:
            raise RuntimeError("synthetic later phase failure")
        return {"status": "success"}

    result = service._run_task("synthetic_scheduled_refresh", run, trigger_source="synthetic")
    assert result["status"] == ("failed" if failed else "success")
    reference = result["collection_calculation_captures"][0]
    assert reference["task"]["task_history_id"] == 456
    assert reference["task"]["task_name"] == "synthetic_scheduled_refresh"
    assert reference["capture_complete"] and reference["pipeline_outcome"] == "succeeded"
    assert not service._manual_run_lock.locked() and collection.completed_collection_captures() == []


def test_task_contexts_do_not_share_attempts():
    with task_context(1) as outer:
        outer["attempts"].append({"capture_complete": True, "terminal_path": "outer"})
        with task_context(2) as inner:
            assert collection.completed_collection_captures() == []
            inner["attempts"].append({"capture_complete": True, "terminal_path": "inner"})
            assert collection.completed_collection_captures()[0]["terminal_path"] == "inner"
        assert collection.completed_collection_captures()[0]["terminal_path"] == "outer"
    assert collection.completed_collection_captures() == []


def test_report_snapshot_keeps_earlier_collection_separate_from_its_workflow(pipeline, monkeypatch):
    from dataclasses import replace
    from backend.config import load_settings
    from backend.report_builder import _build_report_artifacts_summary
    from backend import screening_replay_snapshot as snapshot

    with task_context():
        pipeline.run()
        references = collection.completed_collection_captures()
    report_dir = pipeline.settings.runs_dir / "synthetic_later_workflow/revisions/one"
    report_dir.mkdir(parents=True)
    frame = pd.DataFrame({"synthetic_value": [0.0, None]})
    filters = {key: frame.copy() for key in (
        "candidate_df", "dynamic_publish_official_df", "final_selection_tracker_features_df",
        "final_selection_contraction_df", "dynamic_official_df",
    )}
    filters.update(style_thresholds={}, global_thresholds={}, style_meta={}, effective_nav_candidate_date="2026-09-11")
    report_paths = {
        f"{name}_{kind}": report_dir / f"{name}.{extension}"
        for name in ("project_tracker", "filtered", "dynamic_filtered", "threshold_compare",
                     "watch", "dynamic_watch", "excluded", "stage_b_reason", "screening_audit")
        for kind, extension in (("excel", "xlsx"), ("csv", "csv"), ("excel_meta", "json"))
    }
    report_paths.update(report_json=report_dir / "report.json", report_md=report_dir / "report.md")
    artifacts = _build_report_artifacts_summary(
        settings=pipeline.settings,
        artifacts={"collection_calculation_input_captures": references,
                   "collection_calculation_capture": references[0], "unavailable_file": None},
        report_paths=report_paths, forward_validation_artifacts={},
    )
    assert artifacts["core"]["collection_calculation_input_captures"] == references
    assert artifacts["core"]["collection_calculation_capture"] == references[0]
    assert artifacts["core"]["unavailable_file"] is None
    assert artifacts["core"]["collection_calculation_input_captures"] is not references
    summary = {
        "run_id": "synthetic_later_workflow", "generated_at": datetime.now().astimezone().isoformat(),
        "artifacts": artifacts,
    }
    monkeypatch.setattr(snapshot, "source_code_hashes", lambda root: {"synthetic": "fixture"})
    settings = replace(load_settings(), project_root=pipeline.settings.project_root,
                       reports_dir=pipeline.settings.reports_dir, runs_dir=pipeline.settings.runs_dir)
    path = snapshot.write_screening_input_snapshot(
        settings, summary, universe=frame, filters=filters,
        materialized={"official_df": frame}, report_inputs={"decision_map": {}, "soft_warning_map": {}},
    )
    loaded = snapshot.load_screening_input_snapshot(path)
    assert loaded["run_id"] == "synthetic_later_workflow"
    earlier = loaded["inputs"]["collection_calculation_input_captures"][0]
    assert earlier["capture_run_id"] == references[0]["capture_run_id"]
    assert earlier["source_workflow_run_id"] is None
    assert earlier["task"]["task_history_id"] == 123
    assert earlier["scientific_reproduction_status"] == "not_verified"
    assert "calculation_input_capture" not in loaded["inputs"]
