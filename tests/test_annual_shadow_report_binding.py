"""Publication identity guards; all funds/counts in this module are test fixtures."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.annual_return_gate_shadow import (
    FrozenAnnualScenarioIncomplete,
    annual_shadow_report_binding,
    build_incomplete_annual_shadow_summary,
    load_latest_annual_return_gate_shadow_payload,
)
from backend.api_payloads.dashboard import _with_annual_return_shadow_truth_contract
from backend.publish_reason_summary import _build_annual_return_gate_shadow_decision_summary
from backend import workflow
from scripts.audit_data_truthfulness import _validate_live_dashboard_strategy_shadow_contract


def _report(root, revision="current", count=2):
    path = root / "reports/runs/fixture/revisions" / revision / "report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {"run_id": "fixture", "publication_control": {"revision": revision, "commit_protocol": 1},
              "counts": {"final_count": count}, "artifacts": {"report_json": str(path.relative_to(root))}}
    path.write_text(json.dumps(report))
    return report


def _binding(root, report):
    path = root / report["artifacts"]["report_json"]
    return {"run_id": report["run_id"], "revision": report["publication_control"]["revision"],
            "report_path": report["artifacts"]["report_json"],
            "report_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _shadow(root, report, stamp="01", mutate=None):
    binding = _binding(root, report)
    summary = build_incomplete_annual_shadow_summary(
        report, {"baseline_verified": True, "source_files": {"report_json": {
            "path": binding["report_path"], "sha256": binding["report_sha256"]}}},
        baseline_count=report["counts"]["final_count"],
        issue=FrozenAnnualScenarioIncomplete("missing_tracker_history", missing_codes=["000001"], stage_frames={}),
    )
    if mutate:
        mutate(summary)
    path = root / "reports/audit/annual_return_gate_shadow" / f"annual_return_gate_shadow_fixture_{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary))
    return path, summary


def _settings(root):
    return SimpleNamespace(project_root=root, reports_dir=root / "reports")


def test_equal_counts_do_not_make_another_revision_the_current_baseline(tmp_path):
    current = _report(tmp_path)
    old = _report(tmp_path, "previous")
    path, _ = _shadow(tmp_path, old)
    before = path.read_bytes()
    _, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=current)
    assert payload["report_binding"]["status"] == "missing"
    assert payload["baseline_final_count"] is None and payload["shadow_final_count"] is None
    assert payload["baseline_verification"]["baseline_verified"] is False
    assert path.read_bytes() == before


@pytest.mark.parametrize("count", [0, 2])
def test_selects_matching_revision_even_when_other_revision_audit_is_newer(tmp_path, count):
    current = _report(tmp_path, count=count)
    old = _report(tmp_path, "previous", count=count)
    matching, _ = _shadow(tmp_path, current, "01")
    _shadow(tmp_path, old, "02")
    path, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=current)
    assert path == matching and payload["baseline_final_count"] == count
    assert payload["report_binding"]["status"] == "matched"
    assert payload["report_binding"]["report_sha256"] == _binding(tmp_path, current)["report_sha256"]
    assert payload["report_binding"]["artifact_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("mutate", [
    lambda s: s["baseline_verification"]["source_files"]["report_json"].update(sha256="0" * 64),
    lambda s: s["baseline_verification"]["source_files"]["report_json"].pop("sha256"),
    lambda s: s.update(source_report="reports/unrelated.json"),
    lambda s: s.update(run_id="other"),
])
def test_corrupt_matching_revision_never_falls_back_to_an_older_pass(tmp_path, mutate):
    current = _report(tmp_path)
    _shadow(tmp_path, current, "01")
    path, _ = _shadow(tmp_path, current, "02", mutate=mutate)
    _, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=current)
    assert payload["report_binding"]["status"] == "invalid"
    assert payload["baseline_final_count"] is None
    assert payload["baseline_verification"]["baseline_verified"] is False
    assert payload["report_binding"]["artifact_path"] == str(path.relative_to(tmp_path))


def test_truncated_artifact_is_an_explicit_failure_with_unknown_counts(tmp_path):
    current = _report(tmp_path)
    path, _ = _shadow(tmp_path, current)
    path.write_text('{"run_id":')
    _, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=current)
    assert payload["report_binding"]["status"] == "invalid"
    assert payload["baseline_final_count"] is None and payload["strategy_comparison_allowed"] is False


@pytest.mark.parametrize("style", ["native", "docker"])
def test_report_and_manifest_path_aliases_preserve_revision_and_sha(tmp_path, style):
    report = _report(tmp_path)
    expected = _binding(tmp_path, report)
    relative = report["artifacts"]["report_json"]
    represented = str(tmp_path / relative) if style == "native" else "/app/" + relative

    def aliases(summary):
        summary["source_report"] = represented
        summary["source_report_binding"] = {**expected, "report_path": represented}
        summary["baseline_verification"]["source_files"]["report_json"]["path"] = represented

    path, _ = _shadow(tmp_path, report, mutate=aliases)
    source_path = tmp_path / relative
    originals = {item: item.read_bytes() for item in (source_path, path)}
    report_alias = deepcopy(report)
    report_alias["artifacts"]["report_json"] = represented
    assert annual_shadow_report_binding(_settings(tmp_path), report_alias) == expected
    loaded_path, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=report_alias)
    assert loaded_path == path and payload["report_binding"]["status"] == "matched"
    assert payload["report_binding"]["report_sha256"] == expected["report_sha256"]
    assert payload["shadow_final_count"] is None
    assert all(item.read_bytes() == raw for item, raw in originals.items())


@pytest.mark.parametrize("field", ["run_id", "revision", "report_sha256", "report_path"])
def test_portable_manifest_never_relaxes_other_binding_fields(tmp_path, field):
    report = _report(tmp_path)

    def corrupted(summary):
        binding = _binding(tmp_path, report)
        binding["report_path"] = "/app/" + binding["report_path"]
        binding[field] = "different-source"
        summary["source_report_binding"] = binding

    _shadow(tmp_path, report, "01")
    path, _ = _shadow(tmp_path, report, "02", mutate=corrupted)
    loaded_path, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=report)
    assert loaded_path == path and payload["report_binding"]["status"] == "invalid"
    assert payload["baseline_final_count"] is None and payload["shadow_final_count"] is None


def test_report_reader_rejects_audit_symlink_outside_project(tmp_path):
    report = _report(tmp_path)
    path, _ = _shadow(tmp_path, report)
    outside = tmp_path.parent / (tmp_path.name + "_external.json")
    path.rename(outside)
    path.symlink_to(outside)
    loaded_path, payload = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=report)
    assert loaded_path == path and payload["report_binding"]["status"] == "invalid"
    assert payload["shadow_final_count"] is None


def test_expected_report_binding_does_not_allow_explicit_cross_run_fallback(tmp_path):
    current = _report(tmp_path)
    path, _ = _shadow(tmp_path, current)
    path.rename(path.with_name("annual_return_gate_shadow_other_01.json"))
    _, payload = load_latest_annual_return_gate_shadow_payload(
        _settings(tmp_path), run_id="other", allow_fallback_to_any_run=True, report=current)
    assert payload["report_binding"]["status"] == "missing"
    assert payload["baseline_final_count"] is None


def test_api_exclusion_summary_cannot_fall_back_to_embedded_other_revision(tmp_path, monkeypatch):
    import backend.app as app

    report = _report(tmp_path)
    old = _report(tmp_path, "previous", count=7)
    _, shadow = _shadow(tmp_path, old)
    report["project_publish_summary"] = {"annual_return_gate_shadow": shadow}
    monkeypatch.setattr(app, "settings", _settings(tmp_path))
    result = app._load_exclusion_reason_summary(report)
    current = result["annual_return_gate_shadow"]
    assert current["report_binding"]["status"] == "missing"
    assert current["baseline_final_count"] is None


def test_workflow_audit_passes_explicit_report_path_to_child(tmp_path, monkeypatch):
    from backend import core_runner

    report = _report(tmp_path)
    manager = workflow.WorkflowManager.__new__(workflow.WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "reports/runs")
    manager.store = SimpleNamespace(append_log=lambda value: None)
    commands = []

    def run(**kwargs):
        commands.append(kwargs["cmd"])
        return SimpleNamespace(success=True, stdout_tail=[], stderr_tail=[])

    monkeypatch.setattr(core_runner, "run_structured_subprocess", run)
    monkeypatch.setattr(core_runner, "build_subprocess_log_index", lambda **kwargs: None)
    followups = []
    monkeypatch.setattr(manager, "_run_score_dedup_shadow_audit",
                        lambda run_id, *, report=None: followups.append((run_id, report)))
    manager._run_annual_return_gate_shadow_audit("fixture", report=report)
    assert len(commands) == 1 and "--run-id" not in commands[0]
    assert commands[0][-2:] == ["--report", str(tmp_path / report["artifacts"]["report_json"])]
    assert followups == [("fixture", report)]


@pytest.mark.parametrize("field,invalid", [("revision", "previous"), ("report_sha256", "0" * 64)])
def test_truth_audit_rejects_equal_counts_with_a_different_publication_binding(tmp_path, field, invalid):
    report = _report(tmp_path)
    _, shadow = _shadow(tmp_path, report)
    expected = _binding(tmp_path, report)
    shadow["report_binding"] = {**expected, "status": "matched", field: invalid}
    shadow["decision_summary"] = _build_annual_return_gate_shadow_decision_summary(shadow)
    shadow = _with_annual_return_shadow_truth_contract(shadow)
    _, blockers = _validate_live_dashboard_strategy_shadow_contract(
        {"publish_reason_summary": {"annual_return_gate_shadow": shadow}},
        expected_publication={"counts": report["counts"], "report_binding": expected},
    )
    assert f"dashboard_annual_return_shadow_report_{field}_mismatch" in blockers


def test_missing_audit_is_pending_but_invalid_evidence_is_a_truth_blocker(tmp_path):
    report = _report(tmp_path)
    expected = _binding(tmp_path, report)
    _, missing = load_latest_annual_return_gate_shadow_payload(_settings(tmp_path), report=report)
    for status in ("missing", "invalid"):
        payload = deepcopy(missing)
        payload["report_binding"]["status"] = status
        payload["decision_summary"] = _build_annual_return_gate_shadow_decision_summary(payload)
        payload = _with_annual_return_shadow_truth_contract(payload)
        _, blockers = _validate_live_dashboard_strategy_shadow_contract(
            {"publish_reason_summary": {"annual_return_gate_shadow": payload}},
            expected_publication={"counts": report["counts"], "report_binding": expected},
        )
        if status == "missing":
            assert blockers == []
        else:
            assert "dashboard_annual_return_shadow_report_binding_invalid" in blockers


@pytest.mark.parametrize("shadow_raises", [False, True])
def test_shadow_cannot_rebuild_or_republish_changing_live_inputs(tmp_path, monkeypatch, shadow_raises):
    events = []
    manager = workflow.WorkflowManager.__new__(workflow.WorkflowManager)
    manager.settings = SimpleNamespace(tiantian_base_url="http://fixture", autostock_base_url="http://fixture")
    manager.store = SimpleNamespace(set_phase=lambda value: None, append_log=lambda value: None)
    report = {"run_id": "fixture", "counts": {"final_count": 2},
              "publication_control": {"commit_protocol": 1, "revision": "first"},
              "artifacts": {"report_markdown": "reports/fixture.md", "report_json": "reports/fixture.json"}}
    snapshot = deepcopy(report)

    def build(**kwargs):
        events.append("build")
        if events.count("build") > 1:
            # Simulates an upstream observation changing while the audit runs.
            return {**report, "counts": {"final_count": 3}}
        return report

    def publish(**kwargs):
        events.append("publish")
        return {"latest_canonical_run_id": "fixture"}, kwargs["report"]

    def shadow(run_id, *, report=None):
        events.append("shadow")
        if report is not None:
            assert report is snapshot_report
        if shadow_raises:
            raise OSError("fixture child launch failed")

    snapshot_report = report
    monkeypatch.setattr(workflow, "run_project_pipeline", lambda *args: {})
    monkeypatch.setattr(workflow, "build_report_payload", build)
    monkeypatch.setattr(manager, "_publish_formal_report", publish)
    monkeypatch.setattr(manager, "_run_annual_return_gate_shadow_audit", shadow)
    monkeypatch.setattr(manager, "_run_frozen_baseline_audit", lambda report: events.append("baseline"))
    if shadow_raises:
        with pytest.raises(OSError):
            manager._execute("fixture", {})
        assert events == ["build", "publish", "shadow"]
    else:
        result = manager._execute("fixture", {})
        assert result is report and result == snapshot
        assert events == ["build", "publish", "shadow", "baseline"]
