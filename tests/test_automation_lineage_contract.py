from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from backend.scheduler_diagnostics import build_scheduler_chain_audit
from backend.scheduler_lineage import (
    busy_skip_has_current_publication, daily_screening_evidence, is_busy_skip_without_screening,
)
from bond_fund_screening.tasks.subprocess_logging import classify_stderr_signals
from scripts import audit_data_truthfulness as audit


@pytest.fixture
def skipped_run():
    return {"status": "skipped", "screening_triggered": False,
            "skip_reason": "workflow_busy_locked", "started_at": "2026-09-09T07:30:00",
            "finished_at": "2026-09-09T07:30:00", "screening_run_id": None,
            "report_refresh_run_id": None, "no_action_category": "automation_guard"}


def test_busy_skip_null_lineage_requires_explicit_consistent_evidence(skipped_run):
    evidence = daily_screening_evidence(skipped_run)
    payload = {"daily_run": skipped_run, "automation_autonomy": {
        "daily_screening_run_id": None, "automation_lineage": {"daily_screening_evidence": evidence}},
        "source_basis": {"daily_screening_run_id": None, "daily_screening_evidence": deepcopy(evidence)}}
    assert audit._daily_screening_absence_explained(payload)
    payload["source_basis"]["daily_screening_evidence"]["screening_triggered"] = True
    assert not audit._daily_screening_absence_explained(payload)
    assert skipped_run["screening_run_id"] is None


@pytest.mark.parametrize("field,value", [
    ("status", "failed"), ("status", None), ("screening_triggered", None),
    ("screening_triggered", True), ("screening_triggered", 0), ("skip_reason", "unknown"),
    ("started_at", None), ("finished_at", "invalid"),
    ("finished_at", "2026-09-09T07:29:00"), ("screening_run_id", "unverified-run"),
])
def test_missing_or_conflicting_skip_fields_do_not_explain_null(skipped_run, field, value):
    skipped_run[field] = value
    assert not is_busy_skip_without_screening(skipped_run)


def test_busy_skip_is_covered_only_by_current_ready_formal_publication(skipped_run):
    overview = {"daily_run": skipped_run, "effective_nav_snapshot": {
        "candidate_date": "2026-09-08", "ready": True}, "publication_layers": {
        "current_published_run": {"run_id": "accepted-run", "published": True, "publishable": True,
            "snapshot": {"candidate_date": "2026-09-08", "risk_1y_effective_end_date": "2026-09-08"}}}}
    before = deepcopy(overview)
    assert busy_skip_has_current_publication(skipped_run, overview)
    assert "daily_refresh_skip_guard" not in {i["code"] for i in build_scheduler_chain_audit(overview)["open_issues"]}
    assert overview == before
    published = overview["publication_layers"]["current_published_run"]
    for key, value in [("published", False), ("publishable", False), ("publish_hold", True), ("publish_blocked", True)]:
        changed = deepcopy(overview)
        changed["publication_layers"]["current_published_run"][key] = value
        assert not busy_skip_has_current_publication(skipped_run, changed)
        assert "daily_refresh_skip_guard" in {i["code"] for i in build_scheduler_chain_audit(changed)["open_issues"]}
    published["snapshot"]["risk_1y_effective_end_date"] = "2026-09-07"
    assert not busy_skip_has_current_publication(skipped_run, overview)


def test_latest_scheduled_skip_does_not_replace_previous_executed_run(tmp_path):
    db = tmp_path / "audit.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE task_history (id INTEGER PRIMARY KEY, task_name TEXT,
            status TEXT, started_at TEXT, completed_at TEXT, duration INTEGER,
            records_processed INTEGER, records_failed INTEGER, error_message TEXT, extra_data TEXT)""")
        for task_id, status, extra in [
            (1, "success", {"screening_triggered": True, "screening_run_id": "scheduled-ok", "screening_final_count": 2}),
            (2, "failed", {"screening_triggered": True, "screening_run_id": "scheduled-failed"}),
            (3, "skipped", {"screening_triggered": False, "skip_reason": "workflow_busy_locked"}),
        ]:
            conn.execute("INSERT INTO task_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                task_id, "scheduled_daily_bond_refresh", status, "2026-09-09 07:30:00",
                "2026-09-09 07:30:01", 1, None, None, None, json.dumps(extra)))
    result, blockers, _ = audit._find_scheduled_refresh_task_truth(
        SimpleNamespace(database_path=db), run_id="operator-run", final_count=2)
    assert result["matched"] is False
    assert result["latest_scheduler_run"]["row"]["id"] == 3
    assert result["latest_scheduler_run"]["run_id"] is None
    assert result["latest_successful_screening_run"]["run_id"] == "scheduled-ok"
    assert blockers == ["scheduled_refresh_task_history_missing_for_run"]


def test_compact_dashboard_source_keeps_skipped_daily_evidence(monkeypatch, skipped_run):
    import backend.app as app

    monkeypatch.setattr(app, "_load_scheduler_state_snapshot_cached", lambda: {
        "last_runs": {"scheduled_daily_bond_refresh": deepcopy(skipped_run)}})
    basis = app._build_dashboard_latest_source_basis(
        {"run_id": "real-publication"}, detail_scope="compact", compact=True,
        sample_limit=0, include_full_samples=False)
    assert basis["daily_screening_run_id"] is None
    assert basis["daily_screening_evidence"] == daily_screening_evidence(skipped_run)
    assert audit._daily_screening_absence_explained({"source_basis": basis})


def test_dashboard_explicit_daily_snapshot_does_not_borrow_later_run(monkeypatch, skipped_run):
    import backend.app as app

    def unexpected_load():
        raise AssertionError("explicit daily snapshot must stay bound to the response")

    monkeypatch.setattr(app, "_load_scheduler_state_snapshot_cached", unexpected_load)
    basis = app._build_dashboard_latest_source_basis(
        {"run_id": "real-publication", "daily_run": skipped_run}, detail_scope="compact",
        compact=True, sample_limit=0, include_full_samples=False)
    assert basis["daily_screening_run_id"] is None
    assert basis["daily_screening_evidence"] == daily_screening_evidence(skipped_run)


@pytest.fixture
def annual_gap(tmp_path):
    reports = tmp_path / "reports"
    directory = reports / "audit/annual_return_gate_shadow"
    directory.mkdir(parents=True)
    report = reports / "source_report.json"
    report.write_text('{"run_id":"run-test"}')
    snapshot = reports / "snapshot.json.gz"
    snapshot.write_bytes(b"bound snapshot fixture")
    payload = {"schema_version": 2, "run_id": "run-test", "status": "scenario_inputs_incomplete",
        "scenario_status": "missing_tracker_history", "passed": False, "shadow_final_count": None,
        "production_thresholds_changed": False, "later_database_reads": False, "runtime_upstream_requests": 0,
        "baseline_verification": {"run_id": "run-test", "baseline_verified": True,
            "checks": [{"aligned": True} for _ in range(9)], "snapshot_path": str(snapshot),
            "snapshot_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
            "source_files": {"report_json": {"path": str(report), "sha256": hashlib.sha256(report.read_bytes()).hexdigest()}}}}
    artifact = directory / "annual_return_gate_shadow_run-test_fixture.json"
    stdout, stderr, meta = [tmp_path / name for name in ("child.stdout.log", "child.stderr.log", "child.meta.json")]
    stderr.write_text("")
    meta.write_text("{}")
    item = {"task_name": "annual-return-gate-shadow", "command": ["python", "scripts/audit_annual_return_gate_shadow.py"],
        "success": False, "return_code": 2, "timed_out": False, "timeout_reason": None,
        "stdout_path": str(stdout), "stderr_path": str(stderr), "meta_path": str(meta)}

    def finalize():
        artifact.write_text(json.dumps(payload))
        stdout.write_text(json.dumps({"artifact": str(artifact), "artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "status": payload["status"], "scenario_status": payload["scenario_status"], "passed": False}) + "\n")
        item["stderr_signal_summary"] = classify_stderr_signals(
            task_name=item["task_name"], stderr_path=str(stderr), success=False,
            return_code=item["return_code"], timed_out=item["timed_out"], timeout_reason=None)

    finalize()
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=reports)

    def validate():
        return audit._validate_subprocess_index_payload(settings,
            index_file={"exists": True, "path": str(tmp_path / "index.json")},
            index_payload={"run_id": "run-test", "items": [item], "subprocess_count": 1,
                "success_count": 0, "failed_count": 1, "failed_task_names": [item["task_name"]]},
            expected_run_id="run-test", required_tasks=[], optional_tasks=[], issue_prefix="subprocess")

    return SimpleNamespace(payload=payload, artifact=artifact, item=item, finalize=finalize,
                           validate=validate, snapshot=snapshot, stderr=stderr, stdout=stdout,
                           report=report, settings=settings)


def test_bound_annual_gap_keeps_failed_child_and_null_scenario(annual_gap):
    result, blockers, warnings = annual_gap.validate()
    assert blockers == []
    assert "subprocess_annual_shadow_scenario_inputs_incomplete" in warnings
    assert result["failed_count"] == 1
    assert result["success_count"] == 0
    child = result["tasks"][0]
    assert child["success"] is False and child["return_code"] == 2
    assert child["annual_shadow_terminal"]["business_status"] == "degraded"
    assert annual_gap.payload["shadow_final_count"] is None


@pytest.mark.parametrize("damage", ["artifact_sha", "snapshot_sha", "baseline_failed", "baseline_check_failed",
    "wrong_run", "unknown_status", "computed_count", "upstream_call", "timeout", "stderr_error", "exit_one",
    "malformed_baseline", "malformed_checks", "malformed_source_file"])
def test_annual_gap_with_damaged_evidence_or_real_failure_still_blocks(annual_gap, damage):
    if damage == "baseline_failed":
        annual_gap.payload["baseline_verification"]["baseline_verified"] = False
    elif damage == "baseline_check_failed":
        annual_gap.payload["baseline_verification"]["checks"][0]["aligned"] = False
    elif damage == "wrong_run":
        annual_gap.payload["run_id"] = "other-run"
    elif damage == "unknown_status":
        annual_gap.payload["status"] = "unexpected_failure"
    elif damage == "computed_count":
        annual_gap.payload["shadow_final_count"] = 0
    elif damage == "upstream_call":
        annual_gap.payload["runtime_upstream_requests"] = 1
    elif damage == "timeout":
        annual_gap.item["timed_out"] = True
    elif damage == "exit_one":
        annual_gap.item["return_code"] = 1
    elif damage == "stderr_error":
        annual_gap.stderr.write_text("Traceback (most recent call last):\nRuntimeError: independent failure\n")
    elif damage == "malformed_baseline":
        annual_gap.payload["baseline_verification"] = ["invalid"]
    elif damage == "malformed_checks":
        annual_gap.payload["baseline_verification"]["checks"] = ["invalid"] * 9
    elif damage == "malformed_source_file":
        annual_gap.payload["baseline_verification"]["source_files"]["report_json"] = "invalid"
    annual_gap.finalize()
    if damage == "artifact_sha":
        annual_gap.artifact.write_text(annual_gap.artifact.read_text() + " ")
    elif damage == "snapshot_sha":
        annual_gap.snapshot.write_bytes(b"damaged")
    result, blockers, _ = annual_gap.validate()
    assert blockers
    assert result["failed_count"] == 1 and result["tasks"][0]["success"] is False


def _bind_annual_gap_report(case):
    source = case.payload["baseline_verification"]["source_files"]["report_json"]
    case.payload["source_report"] = source["path"]
    case.payload["source_report_binding"] = {
        "run_id": "run-test", "revision": None, "report_path": source["path"], "report_sha256": source["sha256"]}


@pytest.mark.parametrize("style", ["native", "relative", "docker"])
def test_complete_index_consumes_portable_terminal_and_source_bindings(annual_gap, style):
    case = annual_gap

    def represented(path):
        relative = path.relative_to(case.settings.project_root).as_posix()
        return {"native": str(path), "relative": relative, "docker": "/app/" + relative}[style]

    baseline = case.payload["baseline_verification"]
    baseline["source_files"]["report_json"]["path"] = represented(case.report)
    baseline["snapshot_path"] = represented(case.snapshot)
    _bind_annual_gap_report(case)
    case.finalize()
    terminal = json.loads(case.stdout.read_text())
    terminal["artifact"] = represented(case.artifact)
    case.stdout.write_text(json.dumps(terminal) + "\n")
    originals = {path: path.read_bytes() for path in (case.report, case.snapshot, case.artifact, case.stdout)}
    result, blockers, warnings = case.validate()
    assert blockers == [] and "subprocess_annual_shadow_scenario_inputs_incomplete" in warnings
    assert result["success_count"] == 0 and result["failed_count"] == 1
    child = result["tasks"][0]
    assert child["success"] is False and child["return_code"] == 2
    assert child["annual_shadow_terminal"]["baseline_verified"] is True
    assert case.payload["shadow_final_count"] is None
    assert all(path.read_bytes() == raw for path, raw in originals.items())


@pytest.mark.parametrize("damage", ["source_run", "source_revision", "binding_run", "binding_sha", "binding_path",
    "binding_revision", "outside_source", "source_symlink", "source_traversal", "snapshot_symlink", "missing_snapshot"])
def test_checksum_matching_sources_still_require_project_run_and_revision(annual_gap, damage):
    case = annual_gap
    _bind_annual_gap_report(case)
    source = case.payload["baseline_verification"]["source_files"]["report_json"]
    binding = case.payload["source_report_binding"]
    if damage == "source_run":
        case.report.write_text('{"run_id":"other-run"}')
        source["sha256"] = binding["report_sha256"] = hashlib.sha256(case.report.read_bytes()).hexdigest()
    elif damage == "source_revision":
        case.payload["source_publication_control"] = {"revision": "other-revision"}
    elif damage == "binding_run":
        binding["run_id"] = "other-run"
    elif damage == "binding_sha":
        binding["report_sha256"] = "0" * 64
    elif damage == "binding_revision":
        binding["revision"] = "other-revision"
    elif damage == "binding_path":
        other = case.report.with_name("other-report.json")
        other.write_bytes(case.report.read_bytes())
        binding["report_path"] = str(other)
    elif damage == "source_traversal":
        source["path"] = "reports/../reports/source_report.json"
    elif damage == "missing_snapshot":
        case.snapshot.unlink()
    else:
        root = case.settings.project_root
        outside = root.parent / (root.name + "_external")
        outside.mkdir()
        original = case.snapshot if damage == "snapshot_symlink" else case.report
        target = outside / original.name
        original.rename(target)
        if damage.endswith("symlink"):
            original.symlink_to(target)
        else:
            source["path"] = str(target)
    case.finalize()
    result, blockers, _ = case.validate()
    assert blockers and result["failed_count"] == 1
    assert result["tasks"][0]["success"] is False and result["tasks"][0]["return_code"] == 2
