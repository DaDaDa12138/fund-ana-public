"""Synthetic fixtures verify audit provenance and units, not investment results."""
from dataclasses import replace
from datetime import datetime
import gzip
import json
import math
from pathlib import Path
import sqlite3
import statistics
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from backend import frozen_strategy_audit, report_builder
from backend.api_payloads.positive_ratio import (
    OVERLAP_COUNT_FIELDS, build_positive_ratio_semantics_summary, normalize_low_vol_positive_ratio_shadow_watch,
)
from backend.config import load_settings
from backend.screening_replay_snapshot import load_screening_input_snapshot, restore_frame, write_screening_input_snapshot
from scripts import audit_core_dynamic_overlap as core
from scripts import audit_low_vol_annual_positive_ratio_overlap as annual
from scripts import audit_low_vol_risk_metric_distortion as monthly


@pytest.fixture(params=[core, annual, monthly], ids=["core", "annual", "monthly"])
def source(request, tmp_path, monkeypatch):
    cli = request.param
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports", database_path=tmp_path / "forbidden.db")
    directory = settings.reports_dir / "runs/fixture/revisions/a"
    directory.mkdir(parents=True)
    files = {key: directory / name for key, name in (
        ("report_json", "report.json"), ("screening_input_snapshot_json_gz", "snapshot.gz"),
        ("filtered_csv", "formal.csv"), ("watch_csv", "watch.csv"),
        ("dynamic_filtered_csv", "dynamic.csv"), ("dynamic_watch_csv", "dynamic_watch.csv"),
    )}
    side = directory / "core_artifacts"
    side.mkdir()
    for name in ("long.csv", "short.csv"):
        (side / name).write_text("fcode,avg_combo_score,avg_sharpe,min_return,score_std\n000001,1,1,1,1\n000002,2,2,2,2\n")
    report = {"run_id": "fixture", "publication_control": {"mode": "diagnostic_only", "revision": "a"},
              "artifacts": {key: str(path.relative_to(tmp_path)) for key, path in files.items()}}
    report["artifacts"]["core"] = {"long_csv": str((side / "long.csv").relative_to(tmp_path)),
                                   "short_csv": str((side / "short.csv").relative_to(tmp_path))}
    for key, path in files.items():
        path.write_text(json.dumps(report) if key == "report_json" else "synthetic source")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", [cli.__file__, "--report", str(files["report_json"])])
    for module in (sqlite3, sqlite3.dbapi2):
        monkeypatch.setattr(module, "connect", lambda *a, **kw: pytest.fail("audit queried current SQL"))
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("audit requested current API"))
    return cli, settings, report, files


def read_output(capsys):
    terminal = json.loads(capsys.readouterr().out.splitlines()[-1])
    return json.loads(Path(terminal["artifact_path"]).read_text())


def context_for(source, monkeypatch, *, empty_official=False):
    cli, settings, _, _ = source
    rows = [{"基金代码": f"{i:06d}", "基金名称": f"Synthetic Bond {i}", "债基风格标签": "中短债",
             "has_holding_period": 0, "holding_days": None, "基础门禁通过": True,
             "dynamic_threshold_verdict": "正式推荐" if i == 1 else "自动诊断",
             "dynamic_threshold_reason": "", "dynamic_threshold_reason_codes": "" if i == 1 else "below_annual_floor",
             "项目推荐结论": "正式推荐" if i == 1 else "自动诊断", "正式投资评分": 0.,
             "1年-正收益比率": 50., "核心类别": "fixture"} for i in (1, 2)]
    candidate = pd.DataFrame(rows)
    official = candidate.iloc[:0 if empty_official else 1].copy()
    thresholds = {"weighted_score_min": 0., "short_score_min": 0., "long_score_min": 0., "annual_return_min": 0., "sharpe_min": 0.}
    context = {"settings": settings, "snapshot": {"snapshot_date": "2026-01-01", "inputs": {
        "configuration": {},
        "decision_map": {row["基金代码"]: {"fcode": row["基金代码"], "fund_name": row["基金名称"],
                         "final_mainlist_selected": True, "fund_category": "fixture"} for row in rows}, "soft_warning_map": {}}},
        "frames": {"scored_universe_before_holding_gate": candidate.copy(), "formal_after_all_filters": official,
                   "final_selection_tracker_features": pd.DataFrame()}, "evaluation_times": {}, "autostock_status_map": {},
        "result": {"candidate": candidate, "publish_track": candidate.copy(), "official": official,
                   "style_thresholds": {}, "style_meta": {}, "global_thresholds": thresholds}}
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: ({"baseline_verified": True, "status": "matched"}, context))
    if cli is annual:
        # This fixture isolates CLI routing; definition/file validation has its
        # own complete on-disk evidence tests in test_low_vol_audit_sidecar.py.
        monkeypatch.setattr(cli, "build_low_vol_audit_definition", lambda *a, **kw: {"synthetic_definition": True})
    return context


@pytest.mark.parametrize("invalid", ["missing", "corrupt", "other_revision"])
def test_invalid_frozen_sources_never_fall_back_or_export_fake_counts(source, monkeypatch, capsys, invalid):
    cli, settings, report, paths = source
    path = paths["screening_input_snapshot_json_gz"]
    if invalid == "missing":
        path.unlink()
    elif invalid == "corrupt":
        path.write_bytes(gzip.compress(json.dumps({"schema_version": 1, "run_id": "fixture", "inputs": {}, "inputs_sha256": "bad"}).encode()))
    else:
        alternate = path.parent.parent / "b/snapshot.gz"
        alternate.parent.mkdir()
        alternate.write_bytes(path.read_bytes())
        report["artifacts"]["screening_input_snapshot_json_gz"] = str(alternate.relative_to(settings.project_root))
        paths["report_json"].write_text(json.dumps(report))
    assert cli.main() == 2
    output = read_output(capsys)
    assert not output["passed"] and output["artifacts"] == {}
    assert not output["source_verified_after_replay"]


def test_baseline_failure_never_runs_an_experiment(source, monkeypatch, capsys):
    cli, _, _, _ = source
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: ({"baseline_verified": False, "status": "mismatch"}, {}))
    monkeypatch.setattr(core, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unverified scenario"))
    assert cli.main() == 2
    assert not read_output(capsys)["artifacts"]


def test_baseline_only_preserves_known_empty_members(source, monkeypatch, capsys):
    cli, settings, _, _ = source
    context_for(source, monkeypatch, empty_official=True)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--baseline-only"])
    monkeypatch.setattr(core, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unrequested scenario"))
    assert cli.main() == 0
    output = read_output(capsys)
    assert output["status"] == "baseline_verified" and output["source_verified_after_replay"]
    artifact = output["artifacts"]["baseline_members"]
    assert pd.read_csv(settings.project_root / artifact["path"]).empty


@pytest.mark.parametrize("status,key", [("missing_tracker_history", "missing_tracker_history_codes"),
    ("missing_autostock_observations", "missing_autostock_codes"), ("missing_frozen_incremental_review", "missing_incremental_review_codes")])
def test_partial_release_keeps_unknown_impact_separate_from_zero(source, monkeypatch, capsys, status, key):
    cli, settings, _, _ = source
    context = context_for(source, monkeypatch)
    if cli is monthly:
        assert cli.main() == 2
        output = read_output(capsys)
        assert output["status"] == "missing_frozen_nav_window"
        assert output["monthly_shadow_ready_count"] is None and output["consistency_delta_alert_count"] is None
        assert output["focus_sample_count"] == 2
        return
    calls = []
    def scenario(actual, **kwargs):
        calls.append(kwargs["dynamic_release_codes"])
        assert actual is context
        if kwargs["dynamic_release_codes"]:
            return {"status": status, key: ["000002"]}
        return {"status": "computed", "official": context["frames"]["formal_after_all_filters"]}
    monkeypatch.setattr(core, "run_frozen_strategy_scenario", scenario)
    assert cli.main() == 2
    output = read_output(capsys)
    assert output["status"] == "scenario_inputs_incomplete" and output["source_verified_after_replay"]
    rows = output["scenarios"] if cli is core else output["scenario_impacts"]
    assert len(rows) == output["requested_scenario_count"]
    if cli is annual:
        assert len(rows) == 44
    for row in rows:
        if row["targeted_count"]:
            assert row["formal_count"] is None and row["direct_final_impact_count"] is None and row[key] == ["000002"]
        else:
            assert row["status"] == "computed" and row["added_official_count"] == row["removed_official_count"] == 0
    assert len(calls) == 2  # identical inputs share computation, not business observations
    assert output["completed_scenario_count"] == sum(row["status"] == "computed" for row in rows)


def test_source_change_discards_all_outputs(source, monkeypatch, capsys):
    cli, _, _, paths = source
    context = context_for(source, monkeypatch)
    def verification(*a):
        paths["watch_csv"].write_text("changed source during replay")
        return {"baseline_verified": True, "status": "matched"}, context
    monkeypatch.setattr(cli, "verify_frozen_baseline", verification)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--baseline-only"])
    assert cli.main() == 2
    output = read_output(capsys)
    assert "source_changed" in output["error"] and not output["artifacts"]


def test_empty_release_still_runs_complete_input_gates(monkeypatch):
    candidate = pd.DataFrame({"基金代码": ["000001"], "基础门禁通过": [True], "dynamic_threshold_verdict": ["正式推荐"]})
    context = {"frames": {"scored_universe_before_holding_gate": candidate, "formal_after_all_filters": candidate},
               "result": {"candidate": candidate}}
    monkeypatch.setattr(core, "run_frozen_strategy_scenario", lambda *a, **kw: {"status": "missing_frozen_incremental_review", "missing_incremental_review_codes": ["000001"]})
    output = core._run_release_shadow(frozen_context=context, target_codes=set())
    assert output["targeted_count"] == 0 and output["direct_final_impact_count"] is None
    with pytest.raises(ValueError, match="not_threshold_blocked"):
        core._run_release_shadow(frozen_context=context, target_codes={"000001"})


def test_release_option_reaches_the_frozen_selector_and_incremental_gate(monkeypatch):
    frame = pd.DataFrame([{"基金代码": "000001", "基金名称": "Synthetic Bond C", "项目准入方式": "增强债基补充准入", "项目质量信心": "中信心"}])
    context = {"settings": object(), "snapshot": {"inputs": {"decision_map": {}, "soft_warning_map": {}}},
               "frames": {"final_selection_tracker_features": pd.DataFrame()}, "autostock_status_map": {}, "evaluation_times": {}}
    def selection(**kwargs):
        assert kwargs["dynamic_release_codes"] == {"000001"}
        assert kwargs["evaluation_times"] is context["evaluation_times"]
        return {"status": "computed", "official": frame}
    monkeypatch.setattr(frozen_strategy_audit, "run_report_selection", selection)
    result = frozen_strategy_audit.run_frozen_strategy_scenario(context, scored_universe=frame, dynamic_release_codes={"000001"})
    assert result["status"] == "missing_frozen_incremental_review" and "official" not in result


def monthly_frames():
    dates = pd.bdate_range("2026-01-02", periods=61)
    nav = pd.DataFrame({"fcode": ["000001"] * len(dates), "nav_date": dates,
                        "daily_growth": [35.] + [.1] * 20 + [-.05] * 20 + [.02] * 20})
    window = pd.DataFrame({"fcode": ["000001"], "start_date": [dates[0]], "end_date": [dates[-1]], "data_points": [None], "risk_metric_rowid": [None]})
    return pd.DataFrame({"基金代码": ["000001"]}), window, nav


def test_monthly_adapter_converts_percent_once_and_keeps_missing_metadata():
    focus, risk, nav = monthly_frames()
    row = monthly._build_monthly_shadow_frame(focus_df=focus, risk_window_df=risk, nav_history_df=nav).iloc[0]
    compounded = [math.prod([1 + value / 100.] * 20) - 1 for value in (.1, -.05, .02)]
    assert row["consistency_trading20"] == round(statistics.stdev(compounded) * math.sqrt(12), 6)
    assert row["positive_month_ratio_trading20_pct"] == pytest.approx(66.666667)
    assert pd.isna(row["risk_data_points"]) and pd.isna(row["risk_metric_rowid"])
    shared = report_builder._compute_low_vol_monthly_shadow_metrics(pd.Series(nav["daily_growth"].to_numpy(), index=nav["nav_date"]))
    assert row["positive_month_ratio_calendar_pct"] == shared["positive_month_ratio_calendar_pct"]


@pytest.mark.parametrize("absent", ["risk", "nav", "both"])
def test_missing_monthly_input_never_becomes_zero_risk_or_merge_error(absent):
    focus, risk, nav = monthly_frames()
    if absent in {"risk", "both"}:
        risk = pd.DataFrame()
    if absent in {"nav", "both"}:
        nav = pd.DataFrame()
    frame = monthly._build_monthly_shadow_frame(focus_df=focus, risk_window_df=risk, nav_history_df=nav)
    assert len(focus.merge(frame, on="基金代码")) == 1
    assert frame["consistency_calendar"].isna().all() and frame["calendar_month_count"].isna().all()


@pytest.mark.parametrize("value", [float("inf"), -float("inf")])
def test_nonfinite_monthly_growth_is_not_replaced_with_a_zero_return(value):
    focus, risk, nav = monthly_frames()
    nav.loc[10, "daily_growth"] = value
    frame = monthly._build_monthly_shadow_frame(focus_df=focus, risk_window_df=risk, nav_history_df=nav)
    assert frame.loc[0, "monthly_status"] == "nonfinite_daily_growth" and pd.isna(frame.loc[0, "consistency_trading20"])


def verified_watch():
    overlap = {key: 0 for key in OVERLAP_COUNT_FIELDS}
    overlap.update(available=True, passed=True, status="computed", source_verified_after_replay=True,
                   baseline_verification={"baseline_verified": True}, source_binding={"run_id": "fixture", "revision": "a"},
                   positive_without_annual_direct_final_impact_count=4, positive_only_direct_final_impact_count=1)
    return {"low_vol_positive_ratio_shadow_watch": {"available": True, "focus_sample_count": 5,
        "monthly_shadow_ready_count": 5, "low_vol_overlap_summary": overlap}}, {"run_id": "fixture", "publication_control": {"revision": "a"}}


def test_verified_positive_only_counterfactual_is_not_added_to_its_aggregate():
    publish, identity = verified_watch()
    watch = normalize_low_vol_positive_ratio_shadow_watch(publish, report_identity=identity)
    semantics = build_positive_ratio_semantics_summary(publish, watch)
    assert watch["low_vol_overlap_summary"]["available"]
    assert semantics["positive_ratio_independent_direct_final_impact_count"] == 4
    assert semantics["positive_ratio_only_direct_final_impact_count"] == 1
    assert semantics["annual_positive_direct_final_impact_count"] == 0


@pytest.mark.parametrize("invalid", ["other_run", "other_revision", "legacy", "partial", "missing_count"])
def test_unverified_overlap_stays_unknown_through_normalization_and_compact_api(invalid):
    from backend.api_payloads.dashboard import _reduce_low_vol_positive_ratio_shadow_watch_for_compact
    from backend.risk_adjusted_metric_stack import normalize_positive_ratio_review_evidence
    publish, identity = verified_watch()
    raw = publish["low_vol_positive_ratio_shadow_watch"]
    overlap = raw["low_vol_overlap_summary"]
    if invalid == "other_run":
        overlap["source_binding"]["run_id"] = "older"
    elif invalid == "other_revision":
        overlap["source_binding"]["revision"] = "b"
    elif invalid == "legacy":
        overlap.pop("baseline_verification")
    elif invalid == "partial":
        overlap["passed"] = False
        overlap["status"] = "scenario_inputs_incomplete"
    else:
        overlap["positive_only_direct_final_impact_count"] = None
    raw["headline"] = raw["explanation"] = "stale claim of verified impact"
    watch = normalize_low_vol_positive_ratio_shadow_watch(publish, report_identity=identity)
    semantics = watch["semantics"]
    assert not watch["low_vol_overlap_summary"]["available"]
    assert all(watch["low_vol_overlap_summary"][key] is None for key in OVERLAP_COUNT_FIELDS)
    assert semantics["strategy_materiality_status"] == "unknown" and semantics["production_gate_change_required"] is None
    assert semantics["positive_ratio_independent_direct_final_impact_count"] is None
    assert "stale claim" not in watch["headline"] + watch["explanation"]
    compact = _reduce_low_vol_positive_ratio_shadow_watch_for_compact(watch)
    assert compact["low_vol_overlap_summary"]["annual_positive_direct_final_impact_count"] is None
    review = normalize_positive_ratio_review_evidence({"available": True, "positive_ratio_governance": {"positive_ratio_only_direct_final_impact_count": 1}}, semantics)
    assert review["positive_ratio_governance"]["positive_ratio_only_direct_final_impact_count"] is None


def test_report_capture_freezes_exact_monthly_query_rows_and_latest_window(tmp_path, monkeypatch):
    settings = replace(load_settings(), database_path=tmp_path / "fixture.db")
    focus, _, nav = monthly_frames()
    focus = focus.assign(债基风格标签="中短债", dynamic_threshold_reason_codes="below_positive_ratio_floor",
                         **{"1年-正收益比率": 10., "正式投资评分": 0., "基金名称": "Synthetic"})
    with sqlite3.connect(settings.database_path) as connection:
        connection.executescript("CREATE TABLE fund_risk_metrics(fcode TEXT, period TEXT, start_date TEXT, end_date TEXT, data_points INTEGER); CREATE TABLE fund_nav_history(fcode TEXT, nav_date TEXT, daily_growth REAL);")
        first, last = nav["nav_date"].iloc[[0, -1]]
        connection.executemany("INSERT INTO fund_risk_metrics VALUES(?,?,?,?,?)", [
            ("000001", "1y", "2025-01-01", last.date().isoformat(), 999),
            ("000001", "1y", first.date().isoformat(), last.date().isoformat(), len(nav))])
        connection.executemany("INSERT INTO fund_nav_history VALUES(?,?,?)", [
            (row.fcode, row.nav_date.date().isoformat(), row.daily_growth) for row in nav.itertuples()])
    monkeypatch.setattr(report_builder, "load_effective_nav_snapshot", lambda *a: SimpleNamespace(risk_1y_effective_end_date=last.date().isoformat(), risk_1y_latest_end_date=None))
    captured = {}
    summary = report_builder._build_low_vol_positive_ratio_shadow_watch(focus, settings=settings, preview_top_n=10, replay_inputs=captured)
    assert summary["monthly_shadow_ready_count"] == 1
    assert captured["risk_windows"].iloc[0]["risk_metric_rowid"] == 2
    assert captured["risk_windows"].iloc[0]["data_points"] == len(nav)
    pd.testing.assert_frame_equal(captured["nav_history"], nav)
    assert not summary["low_vol_overlap_summary"]["available"]
    root = load_settings().project_root
    destination = tmp_path / "snapshot"
    destination.mkdir()
    report = {"run_id": "fixture", "generated_at": "2026-04-01T00:00:00", "artifacts": {"report_json": str(destination / "report.json")}}
    empty = pd.DataFrame({"基金代码": pd.Series(dtype=str)})
    filters = {key: empty for key in ("candidate_df", "dynamic_publish_official_df", "final_selection_tracker_features_df", "final_selection_contraction_df", "dynamic_official_df")}
    filters.update(style_thresholds={}, global_thresholds={}, style_meta={}, effective_nav_candidate_date=last.date().isoformat(), low_vol_monthly_replay_inputs=captured)
    path = write_screening_input_snapshot(replace(settings, project_root=root), report, universe=empty, filters=filters,
        materialized={"official_df": empty}, report_inputs={"decision_map": {}, "soft_warning_map": {}})
    frozen = load_screening_input_snapshot(path, expected_run_id="fixture")
    pd.testing.assert_frame_equal(restore_frame(frozen["inputs"]["frames"]["low_vol_nav_history"]), nav)
    assert frozen["inputs"]["low_vol_monthly_input_contract"]["daily_growth_unit"] == "percent"
