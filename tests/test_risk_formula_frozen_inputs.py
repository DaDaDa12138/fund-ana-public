"""Synthetic fixtures test audit evidence contracts, never investment outcomes."""
from dataclasses import replace
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import pytest
import requests

from backend import frozen_strategy_audit as frozen
from backend.config import load_settings
from scripts import audit_risk_metric_formula_shadow as cli


@pytest.fixture
def source(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                       database_path=tmp_path / "forbidden.db", exclude_holding_days_gt=0)
    directory = settings.reports_dir / "runs/synthetic/revisions/a"
    directory.mkdir(parents=True)
    paths = {key: directory / name for key, name in (
        ("report_json", "report.json"), ("screening_input_snapshot_json_gz", "snapshot.gz"),
        ("filtered_csv", "formal.csv"), ("watch_csv", "watch.csv"),
        ("dynamic_filtered_csv", "dynamic.csv"), ("dynamic_watch_csv", "dynamic_watch.csv"),
    )}
    report = {"run_id": "synthetic", "generated_at": "2026-09-08T00:02:00",
              "publication_control": {"mode": "diagnostic_only", "revision": "a"},
              "artifacts": {key: str(path.relative_to(tmp_path)) for key, path in paths.items()}}
    for key, path in paths.items():
        path.write_text(json.dumps(report) if key == "report_json" else "synthetic source")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", [cli.__file__, "--report", str(paths["report_json"])])
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **kw: pytest.fail("audit read current database"))
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("audit requested current API"))
    return settings, report, paths


def context_for(source, monkeypatch, *, empty_official=False):
    settings, _, _ = source
    rows = []
    for i in range(1, 4):
        row = {"基金代码": f"{i:06d}", "基金名称": f"Synthetic Bond {i}", "债基风格标签": "综合纯债",
               "has_holding_period": 1, "holding_days": 30 if i < 3 else 180,
               "正式投资评分": 10. * i}
        for metric in ("return_6m", "return_1y", "annual_return", "return_3y", "sharpe_ratio",
                       "sortino_ratio", "calmar_ratio", "positive_ratio", "scale_billion"):
            row[metric] = float(i)
        for metric in ("max_drawdown", "volatility", "fee_total_numeric"):
            row[metric] = float(4 - i)
        rows.append(row)
    universe = pd.DataFrame(rows)
    official = universe.iloc[:0 if empty_official else 2].copy()
    times = {"decision": datetime(2026, 9, 7, 23, 59, 59), "fixed_post_review": datetime(2026, 9, 8),
             "dynamic_post_review": datetime(2026, 9, 8, 0, 0, 1)}
    context = {"settings": replace(settings, exclude_holding_days_gt=90),
        "snapshot": {"snapshot_date": "2026-09-04", "inputs": {"decision_map": {}, "soft_warning_map": {}}},
        "frames": {"scored_universe_before_holding_gate": universe, "formal_after_all_filters": official,
                   "final_selection_tracker_features": pd.DataFrame()},
        "autostock_status_map": {}, "evaluation_times": times}
    baseline = {"status": "matched", "baseline_verified": True,
                "evaluation_times": {key: value.isoformat() for key, value in times.items()}}
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: (baseline, context))
    return context


def output(capsys):
    terminal = json.loads(capsys.readouterr().out.splitlines()[-1])
    path = Path(terminal["artifact_path"])
    assert terminal["artifact_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    payload = json.loads(path.read_text())
    assert terminal["passed"] is payload["passed"] and terminal["status"] == payload["status"]
    return payload


def complete_result(context):
    official = context["frames"]["formal_after_all_filters"]
    return {"status": "computed", "official": official, "dynamic": official, "post_review": official}


@pytest.mark.parametrize("invalid", ["missing", "corrupt", "other_revision"])
def test_invalid_frozen_source_fails_without_fallback_or_zero_counts(source, capsys, invalid):
    settings, report, paths = source
    snapshot = paths["screening_input_snapshot_json_gz"]
    if invalid == "missing":
        snapshot.unlink()
    elif invalid == "corrupt":
        snapshot.write_bytes(gzip.compress(json.dumps({"schema_version": 1, "run_id": "synthetic",
            "inputs": {}, "inputs_sha256": "incorrect"}).encode()))
    else:
        other = snapshot.parent.parent / "b/snapshot.gz"
        other.parent.mkdir()
        other.write_bytes(snapshot.read_bytes())
        report["artifacts"]["screening_input_snapshot_json_gz"] = str(other.relative_to(settings.project_root))
        paths["report_json"].write_text(json.dumps(report))
    before = paths["report_json"].read_bytes()
    assert cli.main() == 2
    payload = output(capsys)
    assert payload["status"] == "frozen_audit_failed" and payload["passed"] is False
    assert payload["artifacts"] == {} and payload["scenarios"] == []
    assert payload["baseline_actual_formal_count"] is None and payload["scoring_universe_count"] is None
    assert payload["strategy_comparison_allowed"] is False
    assert paths["report_json"].read_bytes() == before


def test_baseline_mismatch_stops_before_formula_experiments(source, monkeypatch, capsys):
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: ({"status": "mismatch", "baseline_verified": False}, {}))
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unverified scenario"))
    assert cli.main() == 2
    payload = output(capsys)
    assert payload["status"] == "baseline_not_verified" and payload["artifacts"] == {}
    assert payload["baseline_actual_formal_count"] is None and not payload["strategy_comparison_allowed"]


def test_baseline_only_keeps_observed_zero_without_comparing_scenarios(source, monkeypatch, capsys):
    settings, report, _ = source
    context_for(source, monkeypatch, empty_official=True)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--baseline-only"])
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unrequested scenario"))
    assert cli.main() == 0
    payload = output(capsys)
    assert payload["passed"] and payload["status"] == "baseline_verified"
    assert payload["baseline_actual_formal_count"] == 0 and payload["scoring_universe_count"] == 3
    assert payload["scenarios"] == [] and payload["strategy_comparison_allowed"] is False
    assert payload["source_publication_control"] == report["publication_control"]
    entry = payload["artifacts"]["baseline_members"]
    assert entry["rows"] == 0 and pd.read_csv(settings.project_root / entry["path"]).empty


@pytest.mark.parametrize("status,key", [
    ("missing_tracker_history", "missing_tracker_history_codes"),
    ("missing_autostock_observations", "missing_autostock_codes"),
    ("missing_frozen_incremental_review", "missing_incremental_review_codes"),
])
def test_partial_success_and_nav_gaps_cannot_be_reported_as_a_complete_audit(source, monkeypatch, capsys, status, key):
    settings, _, _ = source
    context = context_for(source, monkeypatch)
    before = context["frames"]["scored_universe_before_holding_gate"].copy(deep=True)
    calls = []
    def scenario(actual_context, *, scored_universe):
        assert actual_context is context and actual_context["settings"].exclude_holding_days_gt == 90
        assert scored_universe["holding_days"].tolist() == [30, 30, 180]
        assert scored_universe["基金代码"].tolist() == ["000001", "000002", "000003"]
        calls.append(True)
        if len(calls) == 1:
            return complete_result(context)
        return {"status": status, key: ["000002"], "post_review": context["frames"]["formal_after_all_filters"]}
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", scenario)
    assert cli.main() == 2
    payload = output(capsys)
    assert len(calls) == 6 and len(payload["scenarios"]) == 8
    assert payload["status"] == "scenario_inputs_incomplete" and not payload["passed"]
    assert payload["completed_scenario_count"] == 1 and not payload["strategy_comparison_allowed"]
    for row in payload["scenarios"][1:]:
        assert row["formal_count"] is None and row["formal_membership_vs_actual"] is None
        assert not row["strategy_comparison_allowed"]
        assert f"{row['variant']}_scenario_members" not in payload["artifacts"]
    assert [row["status"] for row in payload["scenarios"][-2:]] == ["missing_frozen_nav_window"] * 2
    for row in payload["scenarios"][1:6]:
        assert row[key] == ["000002"]
        artifact = payload["artifacts"][f"{row['variant']}_missing_inputs"]
        assert pd.read_csv(settings.project_root / artifact["path"], dtype=str)["基金代码"].tolist() == ["000002"]
    membership = pd.read_csv(settings.project_root / payload["artifacts"]["membership"]["path"])
    assert membership["variant"].unique().tolist() == [cli.VARIANTS[0][0]]
    pd.testing.assert_frame_equal(context["frames"]["scored_universe_before_holding_gate"], before)


def test_actual_shared_gate_blocks_unrecorded_incremental_review(source, monkeypatch, capsys):
    context = context_for(source, monkeypatch)
    unsafe = pd.DataFrame([{"基金代码": "000002", "基金名称": "Synthetic Bond C",
        "项目准入方式": "增强债基补充准入", "项目质量信心": "中信心"}])
    def select(**kwargs):
        assert kwargs["evaluation_times"] is context["evaluation_times"]
        assert kwargs["tracker_features"] is context["frames"]["final_selection_tracker_features"]
        return {"status": "computed", "official": unsafe, "post_review": unsafe, "dynamic": unsafe}
    monkeypatch.setattr(frozen, "run_report_selection", select)
    assert cli.main() == 2
    payload = output(capsys)
    assert payload["baseline_verification"]["baseline_verified"]
    assert payload["completed_scenario_count"] == 0
    for item in payload["scenarios"][:6]:
        assert item["status"] == "missing_frozen_incremental_review"
        assert item["missing_incremental_review_codes"] == ["000002"]
        assert item["formal_count"] is None
    assert "membership" not in payload["artifacts"]


def test_changed_source_discards_even_completed_scenarios(source, monkeypatch, capsys):
    _, _, paths = source
    context = context_for(source, monkeypatch)
    def change(*a, **kw):
        paths["watch_csv"].write_text("different synthetic source")
        return complete_result(context)
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", change)
    assert cli.main() == 2
    payload = output(capsys)
    assert "source_changed" in payload["error"] and payload["status"] == "frozen_audit_failed"
    assert payload["scenarios"] == [] and payload["artifacts"] == {}
    assert not payload["source_verified_after_replay"] and not payload["strategy_comparison_allowed"]


def test_recorded_sortino_inputs_enable_both_real_scenario_paths(source, monkeypatch, capsys):
    from backend.frozen_risk_observations import RISK_FIELDS

    context = context_for(source, monkeypatch)
    frame = context["frames"]["scored_universe_before_holding_gate"]
    frame["fcode"] = frame["基金代码"]
    for field in RISK_FIELDS:
        if field not in frame:
            frame[field] = None
    frame["period"] = "1y"
    frame["start_date"], frame["end_date"] = "2025-09-04", "2026-09-04"
    frame["calculated_at"], frame["nav_snapshot_signature"] = "2026-09-07T12:00:00", "synthetic_signature"
    frame["data_points"] = 240
    records = [{"fcode": row["fcode"], "risk_record": row,
                "standard_input_sha256": row["fcode"] * 10 + "0000", "annual_mar": 0.,
                "zero_downside": row["fcode"] == "000001", "positive_excess_return": True,
                "production_fallback_triggered": False}
               for row in frame.to_dict("records")]
    monkeypatch.setattr(cli, "read_frozen_risk_observations", lambda *a, **kw:
                        {"sources": [], "observations": records})
    calls = []
    def scenario(actual_context, *, scored_universe):
        calls.append(scored_universe.copy(deep=True))
        return complete_result(context)
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", scenario)
    assert cli.main() == 0
    payload = output(capsys)
    assert len(calls) == payload["completed_scenario_count"] == 8
    assert payload["frozen_risk_inputs"]["matched_risk_rows"] == 3
    assert payload["frozen_risk_inputs"]["zero_downside_count"] == 1
    assert payload["formula_contract"]["historical_nav_window_available"]
    assert pd.isna(calls[-2].iloc[0]["sortino_ratio"])
    assert calls[-1].iloc[0]["sortino_ratio"] == 10.
    assert all(item["status"] == "computed" for item in payload["scenarios"])
    assert payload["production_formulas_changed"] is False
    assert payload["production_switch_recommended"] is False


def test_changed_original_risk_source_discards_all_scenario_outputs(source, monkeypatch, capsys):
    context = context_for(source, monkeypatch)
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: complete_result(context))
    def changed(*a, **kw):
        raise ValueError("frozen_risk_source_changed")
    monkeypatch.setattr(cli, "verify_risk_observation_sources", changed)
    assert cli.main() == 2
    payload = output(capsys)
    assert payload["status"] == "frozen_audit_failed" and payload["scenarios"] == []
    assert payload["frozen_risk_inputs"] is None and payload["artifacts"] == {}
    assert payload["formula_contract"]["historical_nav_window_available"] is False
    assert payload["risk_input_sources_verified_after_replay"] is False


@pytest.mark.parametrize("legacy", [False, True])
def test_original_workflow_reference_schema_keeps_its_actual_sha_and_run(source, monkeypatch, legacy):
    from backend import calculation_input_capture

    settings, _, _ = source
    reference = {"path": "reports/runs/original/calculation_inputs/index.json", "sha256": "a" * 64,
        "source_workflow_run_id": "original", "capture_complete": True,
        "scopes": {"database_risk": 1}, "period_days": []}
    actual = {**reference, "capture_scope": "workflow", "capture_run_id": "original"}
    if not legacy:
        reference = actual.copy()
    monkeypatch.setattr(calculation_input_capture, "capture_index_reference", lambda *a: actual)
    context = {"snapshot": {"inputs": {"calculation_input_capture": reference}}}
    assert cli._risk_input_paths(settings, context, []) == [settings.project_root / reference["path"]]
    context["snapshot"]["inputs"]["calculation_input_capture"] = {**reference, "sha256": "b" * 64}
    with pytest.raises(ValueError, match="index_reference_changed"):
        cli._risk_input_paths(settings, context, [])


@pytest.mark.parametrize("empty", [False, True])
def test_complete_scenarios_preserve_exact_membership_and_compressed_evidence(source, monkeypatch, capsys, empty):
    settings, _, _ = source
    context = context_for(source, monkeypatch, empty_official=empty)
    # These cases require no NAV experiment; the default CLI always includes its explicit NAV gaps.
    monkeypatch.setattr(cli, "VARIANTS", cli.VARIANTS[:1])
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: complete_result(context))
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--risk-free-rate", "0.0075"])
    assert cli.main() == 0
    payload = output(capsys)
    assert payload["status"] == "computed" and payload["passed"] and payload["strategy_comparison_allowed"]
    delta = payload["scenarios"][0]["formal_membership_vs_actual"]
    assert delta["added_count"] == delta["removed_count"] == 0
    assert payload["scenarios"][0]["formal_count"] == (0 if empty else 2)
    assumptions = payload["formula_contract"]
    assert assumptions["risk_free_rate"] == .0075 and "not_historical" in assumptions["risk_free_rate_source"]
    assert assumptions["min_drawdown_by_style"] == cli.CALMAR_MIN_DD_BY_STYLE
    for name, digest in assumptions["code_sha256"].items():
        assert digest == hashlib.sha256((cli.PROJECT_ROOT / name).read_bytes()).hexdigest()
    for entry in payload["artifacts"].values():
        path = settings.project_root / entry["path"]
        assert path.name.endswith(".csv.gz")
        raw = gzip.decompress(path.read_bytes())
        assert raw.startswith(b"\xef\xbb\xbf")
        assert len(raw) == entry["uncompressed_bytes"]
        assert hashlib.sha256(raw).hexdigest() == entry["uncompressed_sha256"]
        assert len(pd.read_csv(path)) == entry["rows"]
    assert not payload["production_switch_recommended"] and not payload["production_formulas_changed"]


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_nonfinite_formula_assumption_is_rejected_before_replay(source, monkeypatch, value):
    monkeypatch.setattr(sys, "argv", [*sys.argv, f"--risk-free-rate={value}"])
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: pytest.fail("invalid assumption compared"))
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
