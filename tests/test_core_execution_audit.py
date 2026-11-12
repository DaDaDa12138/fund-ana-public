"""Synthetic execution provenance checks; never production fund observations."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from backend.calculation_input_capture import build_calculation_input_index, prepare_capture_directory
from backend.core_execution_audit import (
    _summarize_stages, build_core_execution_audit, core_execution_markdown,
)
from backend.core_runner import _build_and_log_subprocess_index
from core.bond_fund_screening.tasks.subprocess_logging import run_structured_subprocess


@pytest.fixture
def execution(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
                               runs_dir=tmp_path / "reports/runs")

    def make(run_id="synthetic_execution", *, child=False, return_code=0):
        directory = prepare_capture_directory(settings, run_id)
        log_dir = settings.runs_dir / run_id / "logs/subprocess"
        log_dir.mkdir(parents=True)
        plan = ["collect-period-returns"] if child else []
        if child:
            run_structured_subprocess(
                task_name="collect-period-returns",
                cmd=[sys.executable, "-c", f"print('synthetic child; no fund data'); raise SystemExit({return_code})"],
                cwd=tmp_path, log_dir=log_dir, stream_output=False,
            )
        bound = _build_and_log_subprocess_index(
            settings=settings, log_dir=log_dir, workflow_run_id=run_id,
            log=lambda message: None, planned_task_names=plan,
        )
        path = build_calculation_input_index(
            settings, directory, run_id=run_id, planned_tasks=plan, subprocess_index=bound,
        )
        return settings, run_id, path, bound

    return make


def audit_for(execution):
    settings, run_id, path, _ = execution
    return build_core_execution_audit(
        settings=settings, run_id=run_id, artifacts={"calculation_input_index_file": str(path)},
        report_mode="full_report", publication_control={"eligible_for_formal_publication": False},
    )


def test_explicit_empty_plan_records_no_execution_without_fake_success(execution):
    record = execution()
    audit = audit_for(record)
    assert audit["evidence_status"] == "verified" and audit["process_count"] == 0
    assert audit["plan_recorded"] is True
    assert all(stage["status"] == "not_scheduled" for stage in audit["stages"])
    assert audit["business_data_changes"] == "not_assessed"
    assert audit["scientific_reproduction_status"] == "not_verified"
    assert "aligned" not in audit
    markdown = "\n".join(core_execution_markdown(audit))
    assert "本轮未安排执行" in markdown and "已记录进程成功退出" not in markdown


@pytest.mark.parametrize("return_code,status", [(0, "succeeded"), (2, "failed")])
def test_actual_child_exit_is_reported_without_claiming_data_updates(execution, return_code, status):
    audit = audit_for(execution(child=True, return_code=return_code))
    collection = audit["stages"][0]
    assert collection["status"] == status and collection["observed_process_count"] == 1
    task = next(row for row in collection["tasks"] if row["task_name"] == "collect-period-returns")
    assert task["attempts"][0]["return_code"] == return_code
    assert all(stage["status"] == "not_scheduled" for stage in audit["stages"][1:])
    assert audit["business_data_changes"] == "not_assessed"


def test_post_report_audit_cannot_replace_the_frozen_core_execution(execution):
    record = execution()
    settings, run_id, _, _ = record
    mutable = settings.runs_dir / run_id / "logs/subprocess/subprocess_index.json"
    mutable.write_text(json.dumps({"run_id": run_id, "items": [{"task_name": "annual-return-gate-shadow", "success": False}]}))
    assert audit_for(record)["process_count"] == 0


@pytest.mark.parametrize("damage", ["bound_bytes", "missing_index", "different_run", "symlink"])
def test_invalid_or_foreign_capture_never_becomes_current_success(execution, damage):
    settings, run_id, path, bound = execution(child=True)
    if damage == "bound_bytes":
        (path.parent / "bound_subprocess_index.json").write_text('{}')
    elif damage == "missing_index":
        path.unlink()
    elif damage == "different_run":
        run_id = "different_synthetic_run"
    else:
        alias = path.parent.parent / "alias.json"
        alias.symlink_to(path)
        path = alias
    audit = audit_for((settings, run_id, path, bound))
    assert audit["evidence_status"] == "invalid"
    assert audit["process_count"] is None and audit["stages"] == []
    assert "执行状态保持未知" in "\n".join(core_execution_markdown(audit))


def test_missing_evidence_and_legacy_descriptions_stay_unknown(tmp_path):
    audit = build_core_execution_audit(settings=SimpleNamespace(), run_id="new", artifacts={}, report_mode="full_report")
    assert audit["process_count"] is None and audit["plan_recorded"] is None
    assert audit["evidence_status"] == "unavailable"
    legacy = {"aligned": True, "stages": [{"status": "aligned"}]}
    assert "历史架构说明不作为执行证明" in "\n".join(core_execution_markdown(legacy))


def child_item(**changes):
    return {
        "task_name": "collect-nav", "return_code": 0, "success": True, "timed_out": False,
        "started_at": "2026-01-01T10:00:00", "finished_at": "2026-01-01T10:00:01",
        "command": ["python", "main.py", "collect-nav"], **changes,
    }


@pytest.mark.parametrize("changes", [
    {"return_code": None}, {"return_code": False}, {"return_code": "0"}, {"success": "false"},
    {"success": True, "return_code": 1}, {"timed_out": None}, {"finished_at": None},
    {"finished_at": "2025-12-31T10:00:00"}, {"command": []},
])
def test_missing_or_contradictory_metadata_cannot_render_as_success(changes):
    stages, _ = _summarize_stages({"planned_task_names": ["collect-nav"], "items": [child_item(**changes)]})
    assert stages[0]["status"] == "invalid_evidence"


def test_timeout_remains_timeout_even_with_zero_exit_code():
    stages, _ = _summarize_stages({"planned_task_names": ["collect-nav"], "items": [
        child_item(return_code=0, timed_out=True, success=False),
    ]})
    assert stages[0]["status"] == "timed_out"


def test_successful_master_does_not_prove_nine_children_or_tracker_ran():
    stages, _ = _summarize_stages({"planned_task_names": ["daily-screening-master"], "items": [
        child_item(task_name="daily-screening-master"),
    ]})
    by_key = {stage["key"]: stage for stage in stages}
    assert by_key["screening_master"]["status"] == "succeeded"
    assert all(by_key[key]["status"] == "incomplete" for key in ("screening_periods", "merge", "core_tracker"))


def test_legacy_missing_plan_does_not_invent_skips_and_repeated_attempts_are_retained():
    stages, _ = _summarize_stages({"items": [child_item(), child_item(return_code=2, success=False)]})
    assert stages[0]["status"] == "multiple_attempts"
    assert stages[1]["status"] == "not_observed"
    nav = next(task for task in stages[0]["tasks"] if task["task_name"] == "collect-nav")
    assert [item["return_code"] for item in nav["attempts"]] == [0, 2]


def test_lightweight_and_full_report_use_current_execution_without_mutating_base(execution, monkeypatch):
    from backend import report_builder

    settings, run_id, path, _ = execution()
    settings.database_path = settings.project_root / "unused.db"
    settings.effective_nav_coverage_ratio = 0.9
    settings.effective_nav_weekday_only = True
    settings.preview_top_n = 5
    base = {"run_id": "older_run", "counts": {}, "artifacts": {},
            "internal_audit": {"core_flow_audit": {"aligned": True}}, "project_publish_summary": {}}
    original = deepcopy(base)
    import pandas as pd
    monkeypatch.setattr(report_builder, "_materialize_run_local_core_artifacts", lambda **kw: kw["artifacts"])
    monkeypatch.setattr(report_builder, "_normalize_lightweight_run_local_artifacts", lambda **kw: kw["localized_artifacts"])
    monkeypatch.setattr(report_builder, "_build_run_local_risk_audit_artifacts", lambda **kw: {})
    monkeypatch.setattr(report_builder, "load_effective_nav_snapshot", lambda *args, **kw: SimpleNamespace(to_dict=lambda: {}))
    for name in ("_read_metadata", "_read_return_snapshot", "_read_risk_snapshot"):
        monkeypatch.setattr(report_builder, name, lambda *args, **kw: pd.DataFrame())
    monkeypatch.setattr(report_builder, "_load_run_local_frame_for_data_source_audit", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(report_builder, "_refresh_lightweight_final_selection_contraction", lambda **kw: kw["publish_summary"])
    monkeypatch.setattr(report_builder, "_build_long_term_history_snapshot", lambda *args, **kw: {})
    monkeypatch.setattr(report_builder, "_build_long_term_feature_frame", lambda *args, **kw: (pd.DataFrame(), {}))
    seen = []
    monkeypatch.setattr(report_builder, "build_markdown_report", lambda settings, current, *args: seen.append(deepcopy(current["internal_audit"]["core_flow_audit"])) or 'synthetic report')
    artifacts = {"calculation_input_index_file": str(path)}
    report = report_builder.build_lightweight_report_payload(
        settings=settings, run_id=run_id, artifacts=artifacts, base_payload=base,
    )
    flow = report["internal_audit"]["core_flow_audit"]
    assert flow == seen[0] and flow["run_id"] == run_id
    assert flow["reused_from_run_id"] == "older_run" and flow["report_mode"] == "lightweight_refresh"
    assert flow["process_count"] == 0 and "aligned" not in flow
    assert base == original
    full = report_builder._build_internal_audit_summary(
        settings=settings, run_id=run_id, artifacts=artifacts, publication_control={},
        core_base_dir=settings.project_root, final_meta={}, final_screening_summary={},
        accumulation_summary={}, quality_gate_summary={},
    )
    assert full["core_flow_audit"]["process_count"] == 0
    assert full["core_architecture"]["scope"] == "architecture_description"
    assert all("status" not in stage for stage in full["core_architecture"]["stages"])
