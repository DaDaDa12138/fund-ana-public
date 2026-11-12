"""Synthetic fixtures exercise audit correctness, never production fund evidence."""
from dataclasses import replace
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from backend import frozen_strategy_audit as frozen
from backend.config import load_settings
from backend.score_dedup_shadow import SHADOW_SCORE_CONTRACT_VERSION
from scripts import audit_dynamic_threshold_robustness as thresholds
from scripts import audit_score_dedup_shadow as scores


@pytest.fixture(params=[scores, thresholds], ids=["score", "threshold"])
def source(request, tmp_path, monkeypatch):
    cli = request.param
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                       database_path=tmp_path / "forbidden.db", exclude_holding_days_gt=0)
    directory = settings.reports_dir / "runs/fixture/revisions/a"
    directory.mkdir(parents=True)
    files = {key: directory / name for key, name in (
        ("report_json", "report.json"), ("screening_input_snapshot_json_gz", "snapshot.gz"),
        ("filtered_csv", "formal.csv"), ("watch_csv", "watch.csv"),
        ("dynamic_filtered_csv", "dynamic.csv"), ("dynamic_watch_csv", "dynamic_watch.csv"),
    )}
    report = {"run_id": "fixture", "publication_control": {"mode": "diagnostic_only", "revision": "a"},
              "artifacts": {key: str(path.relative_to(tmp_path)) for key, path in files.items()}}
    for key, path in files.items():
        path.write_text(json.dumps(report) if key == "report_json" else "synthetic source")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", [cli.__file__, "--report", str(files["report_json"])])
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **kw: pytest.fail("selection opened a current database"))
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("selection requested current API data"))
    return cli, settings, report, files


def result(source, capsys):
    terminal = json.loads(capsys.readouterr().out)
    cli, settings, _, _ = source
    if cli is scores:
        assert not Path(terminal["artifact_path"]).is_absolute()
    return json.loads((settings.project_root / terminal["artifact_path"]).read_text())


@pytest.mark.parametrize("style", ["native", "relative", "docker"])
@pytest.mark.parametrize("archived", [False, True])
@pytest.mark.parametrize("configured_alias", [False, True])
def test_source_manifest_paths_are_portable_and_preserve_same_revision_archives(source, style, archived, configured_alias):
    _, settings, report, paths = source
    original_root = settings.project_root
    if configured_alias:
        alias = original_root.parent / (original_root.name + "_alias")
        alias.symlink_to(original_root, target_is_directory=True)
        settings = replace(settings, project_root=alias, reports_dir=alias / "reports")
    for key, path in paths.items():
        relative = path.relative_to(original_root).as_posix()
        report["artifacts"][key] = {"native": str(settings.project_root / relative),
                                    "relative": relative, "docker": "/app/" + relative}[style]
    paths["report_json"].write_text(json.dumps(report))
    originals = {key: path.read_bytes() for key, path in paths.items()}
    if archived:
        destination = settings.project_root / "reports/archive/superseded_runs/fixture/revisions/a"
        destination.parent.mkdir(parents=True)
        paths["report_json"].parent.rename(destination)
    manifest = frozen.bind_frozen_report_sources(settings, report)
    assert manifest["run_id"] == "fixture" and manifest["revision"] == "a"
    assert set(manifest["files"]) == set(frozen.FROZEN_SOURCE_KEYS)
    for key, binding in manifest["files"].items():
        assert not Path(binding["path"]).is_absolute()
        assert (settings.project_root / binding["path"]).read_bytes() == originals[key]
        assert binding["sha256"] == hashlib.sha256(originals[key]).hexdigest()
        assert ("/archive/" in binding["path"]) is archived


@pytest.mark.parametrize("invalid", ["missing", "corrupt", "other_revision"])
def test_invalid_source_never_falls_back_to_current_data_or_reports_zero(source, capsys, invalid):
    cli, settings, report, paths = source
    snapshot = paths["screening_input_snapshot_json_gz"]
    if invalid == "missing":
        snapshot.unlink()
    elif invalid == "corrupt":
        snapshot.write_bytes(gzip.compress(json.dumps({"schema_version": 1, "run_id": "fixture",
            "inputs": {}, "inputs_sha256": "incorrect"}).encode()))
    else:
        other = snapshot.parent.parent / "b/snapshot.gz"
        other.parent.mkdir()
        other.write_bytes(snapshot.read_bytes())
        report["artifacts"]["screening_input_snapshot_json_gz"] = str(other.relative_to(settings.project_root))
        paths["report_json"].write_text(json.dumps(report))
    before = paths["report_json"].read_bytes()
    assert cli.main() == 2
    output = result(source, capsys)
    assert output["passed"] is False and output["artifacts"] == {}
    if cli is scores:
        assert output["candidate_count"] is None and output["membership_impact"] is None
        assert output["shadow_replay"]["final_count"] is None
    else:
        assert output["baseline_actual_final_count"] is None and output["scenarios"] == []
    assert paths["report_json"].read_bytes() == before


def test_baseline_mismatch_stops_before_any_scenario(source, monkeypatch, capsys):
    cli, _, _, _ = source
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: ({"status": "mismatch", "baseline_verified": False}, {}))
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unverified baseline compared"))
    assert cli.main() == 2
    output = result(source, capsys)
    assert not output["passed"] and not output["artifacts"]


def context_for(source, monkeypatch, *, empty_official=False):
    cli, settings, _, _ = source
    rows = []
    for i in range(1, 4):
        row = {"基金代码": f"{i:06d}", "基金名称": f"Synthetic Bond {i}", "债基风格标签": "综合纯债",
               "has_holding_period": 1, "holding_days": 30 if i < 3 else 180,
               "正式榜分组": "稳健债基榜", "正式投资评分": 10. * i, "近期得分": 10. * i, "长期得分": 10. * i,
               "基础门禁通过": True, "项目推荐结论": "正式推荐", "dynamic_threshold_verdict": "正式推荐"}
        for metric in ("return_6m", "return_1y", "annual_return", "return_3y", "sharpe_ratio", "sortino_ratio", "calmar_ratio", "positive_ratio", "scale_billion"):
            row[metric] = float(i)
        for metric in ("max_drawdown", "volatility", "fee_total_numeric"):
            row[metric] = float(4-i)
        row["综合加权得分"] = row["正式投资评分"]
        for column, metric in (("1年-年化收益(%)", "annual_return"), ("1年-夏普比率", "sharpe_ratio"),
            ("1年-索提诺比率", "sortino_ratio"), ("1年-卡玛比率", "calmar_ratio"),
            ("1年-正收益比率", "positive_ratio"), ("1年-最大回撤(%)", "max_drawdown"),
            ("1年-波动率", "volatility"), ("费率合计(%)", "fee_total_numeric")):
            row[column] = row[metric]
        rows.append(row)
    universe = pd.DataFrame(rows)
    eligible = universe.iloc[:2].copy()
    official = eligible.iloc[:0].copy() if empty_official else eligible.copy()
    times = {"decision": datetime(2026, 9, 7, 23, 59, 59), "fixed_post_review": datetime(2026, 9, 8),
             "dynamic_post_review": datetime(2026, 9, 8, 0, 0, 1)}
    context = {"settings": replace(settings, exclude_holding_days_gt=90),
        "snapshot": {"snapshot_date": "2026-09-04", "inputs": {"decision_map": {}, "soft_warning_map": {}}},
        "frames": {"scored_universe_before_holding_gate": universe, "formal_after_all_filters": official,
                   "final_selection_tracker_features": pd.DataFrame()},
        "evaluation_times": times, "autostock_status_map": {},
        "result": {"candidate": eligible, "post_review": eligible, "publish_track": eligible,
                   "official": official, "style_thresholds": {}, "style_meta": {}, "global_thresholds": {}}}
    baseline = {"baseline_verified": True, "status": "matched", "evaluation_times": {k: v.isoformat() for k,v in times.items()}}
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: (baseline, context))
    return context


def test_baseline_only_keeps_known_zero_and_does_not_run_scenarios(source, monkeypatch, capsys):
    cli, _, _, _ = source
    context_for(source, monkeypatch, empty_official=True)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--baseline-only"])
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", lambda *a, **kw: pytest.fail("unrequested scenario"))
    assert cli.main() == 0
    output = result(source, capsys)
    assert output["status"] == "baseline_verified" and output["passed"]
    assert output["source_publication_control"]["mode"] == "diagnostic_only"
    if cli is scores:
        assert output["replay_alignment"]["baseline_count"] == 0
        assert output["shadow_replay"]["final_count"] is None
    else:
        assert output["baseline_actual_final_count"] == 0
        assert output["baseline_recomputed_final_count"] == 0


@pytest.mark.parametrize("status,key", [("missing_tracker_history", "missing_tracker_history_codes"),
    ("missing_autostock_observations", "missing_autostock_codes"),
    ("missing_frozen_incremental_review", "missing_incremental_review_codes")])
def test_incomplete_scenario_exports_only_observed_stages_and_explicit_gaps(source, monkeypatch, capsys, status, key):
    cli, settings, _, _ = source
    context = context_for(source, monkeypatch)
    before = context["frames"]["scored_universe_before_holding_gate"].copy(deep=True)
    def scenario(actual_context, **kwargs):
        assert actual_context is context and len(kwargs["scored_universe"]) == 3
        assert actual_context["settings"].exclude_holding_days_gt == 90
        assert kwargs["scored_universe"]["holding_days"].tolist() == [30, 30, 180]
        if cli is scores:
            assert kwargs["scored_universe"]["正式投资评分"].tolist() == [33.3333, 66.6667, 100.0]
        return {"status": status, key: ["000002"], "candidate": context["result"]["candidate"],
                "post_review": context["result"]["post_review"]}
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", scenario)
    assert cli.main() == 2
    output = result(source, capsys)
    assert output["status"] == "scenario_inputs_incomplete" and not output["passed"]
    assert not any("scenario_members" in key or "membership" in key for key in output["artifacts"])
    if cli is scores:
        assert output["membership_impact"] is None and output["shadow_replay"]["final_count"] is None
        assert output["candidate_count"] == 2 and output["scoring_universe_count"] == 3
    else:
        assert len(output["scenarios"]) == 2
        assert all(row["scenario_final_count"] is None and row[key] == ["000002"] for row in output["scenarios"])
    gaps = [entry for name, entry in output["artifacts"].items() if "missing_inputs" in name]
    assert gaps
    for entry in gaps:
        assert pd.read_csv(settings.project_root / entry["path"], dtype=str)["基金代码"].tolist() == ["000002"]
    pd.testing.assert_frame_equal(context["frames"]["scored_universe_before_holding_gate"], before)


def test_changing_a_bound_source_after_baseline_invalidates_all_comparisons(source, monkeypatch, capsys):
    cli, _, _, paths = source
    context_for(source, monkeypatch)
    def change(*a, **kw):
        paths["watch_csv"].write_text("changed synthetic source")
        return {"status": "missing_tracker_history", "missing_tracker_history_codes": ["000001"],
                "candidate": pd.DataFrame(), "post_review": pd.DataFrame()}
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", change)
    assert cli.main() == 2
    output = result(source, capsys)
    assert "source_changed" in output["error"] and not output["artifacts"]


def test_complete_no_change_scenario_keeps_actual_zero_changes(source, monkeypatch, capsys):
    cli, _, _, _ = source
    context = context_for(source, monkeypatch)
    def scenario(*args, **kwargs):
        return {"status": "computed", "candidate": context["result"]["candidate"],
                "post_review": context["result"]["post_review"], "publish_track": context["result"]["publish_track"],
                "official": context["result"]["official"]}
    monkeypatch.setattr(cli, "run_frozen_strategy_scenario", scenario)
    assert cli.main() == 0
    output = result(source, capsys)
    assert output["passed"] and output["status"] == "computed"
    if cli is scores:
        assert output["membership_impact"]["added_count"] == output["membership_impact"]["removed_count"] == 0
        assert output["forward_window_readiness"]["nav_evidence"] is None
        assert output["production_switch_supported_now"] is False
    else:
        assert all(row["added_count"] == row["removed_count"] == 0 for row in output["scenarios"])
        assert "membership_compare" in output["artifacts"]
        assert output["production_switch_recommended"] is False


def test_incremental_review_is_a_required_stage_for_both_experiments(monkeypatch):
    frame = pd.DataFrame([{"基金代码": "000001", "基金名称": "Synthetic Bond C",
        "项目准入方式": "增强债基补充准入", "项目质量信心": "中信心"}])
    context = {"settings": object(), "snapshot": {"inputs": {"decision_map": {}, "soft_warning_map": {}}},
               "frames": {"final_selection_tracker_features": pd.DataFrame()}, "autostock_status_map": {}, "evaluation_times": {}}
    monkeypatch.setattr(frozen, "run_report_selection", lambda **kw: {"status": "computed", "official": frame,
        "candidate": frame, "post_review": frame})
    output = frozen.run_frozen_strategy_scenario(context, scored_universe=frame)
    assert output["status"] == "missing_frozen_incremental_review"
    assert output["missing_incremental_review_codes"] == ["000001"]
    assert "official" not in output


def test_threshold_override_preserves_real_saved_nav_boundary_reasons_and_clock(monkeypatch):
    from backend import project_publish_decisions as decisions
    from backend.screening_baseline_replay import resolve_evaluation_times, run_report_selection
    from backend.screening_replay_snapshot import restore_frame
    data = json.loads((Path(__file__).parent / "fixtures/replay_clock_boundary_20260907.json").read_text())
    frame = restore_frame(data["frame"])
    times, _ = resolve_evaluation_times({"generated_at": data["generated_at"], "inputs": {}})
    class ForbiddenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            raise AssertionError("read current time for a historical gate")
    monkeypatch.setattr(decisions, "datetime", ForbiddenClock)
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **kw: pytest.fail("read current database"))
    context = {"settings": load_settings(), "snapshot": {"inputs": {
        "decision_map": data["decision_map"], "soft_warning_map": data["soft_warning_map"]}},
        "frames": {"final_selection_tracker_features": pd.DataFrame(columns=["基金代码"])},
        "evaluation_times": times, "autostock_status_map": {}}
    baseline = frozen.run_frozen_strategy_scenario(context, scored_universe=frame)
    replay = frozen.run_frozen_strategy_scenario(context, scored_universe=frame,
        dynamic_threshold_override=tuple(baseline[key] for key in ("style_thresholds", "global_thresholds", "style_meta")))
    pd.testing.assert_frame_equal(baseline["candidate"], replay["candidate"])
    for row in replay["candidate"].to_dict(orient="records"):
        assert row["dynamic_threshold_reason"] == data["expected_base_reasons"][row["基金代码"]]


def test_membership_join_keeps_one_style_column_and_complete_member_changes():
    baseline = pd.DataFrame({"基金代码": ["000001", "000002"], "基金名称": ["Synthetic A", "Synthetic B"],
        "债基风格标签": ["synthetic", "synthetic"], "正式榜分组": ["synthetic", "synthetic"], "正式投资评分": [0., None]})
    scenario = baseline.iloc[1:].copy()
    frame = thresholds._build_membership_compare_frame(baseline_official_df=baseline,
        scenarios={"sample": {"official_df": scenario, "candidate_df": baseline}})
    assert frame["基金代码"].tolist() == ["000001", "000002"]
    assert frame["债基风格标签"].tolist() == ["synthetic", "synthetic"]
    assert not any(str(col).endswith(("_x", "_y")) for col in frame.columns)
    assert frame.loc[0, "baseline_score"] == 0. and pd.isna(frame.loc[1, "baseline_score"])
    assert frame["sample_change"].tolist() == ["稳健场景移出正式榜", "仍在正式榜"]


@pytest.mark.parametrize("include_accum", [False, True])
def test_forward_nav_is_replayable_separately_and_rejects_another_selection(tmp_path, monkeypatch, include_accum):
    settings = replace(load_settings(), project_root=tmp_path, database_path=tmp_path / "synthetic.db")
    frame = pd.DataFrame({"fcode": ["000001"] * 3, "nav_date": ["2026-01-01", "2026-01-02", "2026-01-03"],
                          "unit_nav": [0., None, 1.], "accum_nav": [0., None, 1.]})
    if not include_accum:
        frame = frame.drop(columns="accum_nav")
    monkeypatch.setattr(scores, "_load_nav_history_for_codes", lambda *a, **kw: frame.copy())
    path = tmp_path / "outcomes.gz"
    first, proof = scores._load_forward_nav_evidence(settings, snapshot_date="2026-01-01", codes=["000001"],
        selection_sha256="a"*64, output_path=path)
    monkeypatch.setattr(scores, "_load_nav_history_for_codes", lambda *a, **kw: pytest.fail("read current NAV during replay"))
    second, replay = scores._load_forward_nav_evidence(settings, snapshot_date="2026-01-01", codes=["000001"],
        selection_sha256="a"*64, output_path=tmp_path / "unused.gz", source_path=path)
    pd.testing.assert_frame_equal(first, second)
    assert proof["sha256"] == replay["sha256"] and replay["live_database_read_for_outcomes_only"] is False
    with pytest.raises(ValueError, match="selection_mismatch"):
        scores._load_forward_nav_evidence(settings, snapshot_date="2026-01-01", codes=["000001"],
            selection_sha256="b"*64, output_path=tmp_path / "unused.gz", source_path=path)
    assert not (tmp_path / "unused.gz").exists()


def test_legacy_maturity_flags_do_not_qualify_as_verified_strategy_history(tmp_path):
    for day in range(1, 4):
        (tmp_path / f"score_dedup_shadow_{day}.json").write_text(json.dumps({
            "contract": {"contract_version": SHADOW_SCORE_CONTRACT_VERSION},
            "forward_window_readiness": {"status": "ready", "ready_window_count": 3, "snapshot_date": f"2025-01-{day:02d}"}}))
    summary = scores._historical_shadow_readiness(tmp_path, project_root=tmp_path)
    assert summary["mature_distinct_snapshot_count"] == 0
    assert summary["excluded_unverified_artifact_count"] == 3 and not summary["multi_snapshot_requirement_met"]


@pytest.fixture
def mature_history(tmp_path, monkeypatch):
    """Synthetic NAV tests evidence integrity, not investment effectiveness."""
    settings = replace(load_settings(), project_root=tmp_path, database_path=tmp_path / "synthetic.db")
    files = {}
    for key in frozen.FROZEN_SOURCE_KEYS:
        path = tmp_path / key
        path.write_text(f"synthetic source {key}")
        files[key] = {"path": path.name, "sha256": scores.file_sha256(path)}
    binding = {"run_id": "fixture", "revision": "a", "files": files}
    contract = {"contract_version": SHADOW_SCORE_CONTRACT_VERSION}
    membership = {"added_codes": ["000001"], "removed_codes": ["000002"]}
    selection_hash = scores._selection_sha256(binding, contract, membership)
    dates = pd.date_range("2025-01-01", periods=181)
    # This no-distribution fixture supplies its explicit total-return inputs;
    # production must not infer missing daily returns from the price series.
    frame = pd.DataFrame([{"fcode": code, "nav_date": date, "unit_nav": 1. + i * slope,
                          "accum_nav": None,
                          "daily_growth": None if i == 0 else 100 * slope / (1. + (i - 1) * slope)}
                          for code, slope in (("000001", .001), ("000002", -.0001))
                          for i, date in enumerate(dates)])
    monkeypatch.setattr(scores, "_load_nav_history_for_codes", lambda *a, **kw: frame.copy())
    _, evidence = scores._load_forward_nav_evidence(settings, snapshot_date="2025-01-01", codes=["000001", "000002"],
        selection_sha256=selection_hash, output_path=tmp_path / "outcomes.gz")
    forward = scores._forward_validation(snapshot_date="2025-01-01", nav_df=frame, **membership)
    assert forward["status"] == "ready" and forward["ready_window_count"] == 3
    payload = {"contract": contract, "input_contract": frozen.FROZEN_STRATEGY_CONTRACT, "passed": True,
        "baseline_verification": {"baseline_verified": True,
            "checks": [{"name": name, "aligned": True} for name in sorted(scores.REQUIRED_BASELINE_CHECKS)],
            "snapshot_sha256": files["screening_input_snapshot_json_gz"]["sha256"]},
        "shadow_replay": {"status": "computed"}, "source_binding": binding,
        "membership_impact": membership, "forward_window_readiness": {**forward, "nav_evidence": evidence}}
    path = tmp_path / "score_dedup_shadow_fixture.json"
    path.write_text(json.dumps(payload))
    return path, payload


def test_maturity_requires_reproduced_metrics_and_counts_each_date_once(mature_history, tmp_path):
    path, payload = mature_history
    (tmp_path / "score_dedup_shadow_same_date.json").write_text(path.read_text())
    summary = scores._historical_shadow_readiness(tmp_path, project_root=tmp_path)
    assert summary["mature_distinct_snapshot_count"] == 1
    assert summary["reviewed_same_contract_artifact_count"] == 2
    assert summary["excluded_unverified_artifact_count"] == 0
    assert summary["multi_snapshot_requirement_met"] is False


@pytest.mark.parametrize("invalid", ["baseline_type", "forward_type", "window_count", "source_sha", "metrics", "truncated_nav"])
def test_invalid_history_is_excluded_without_aborting_the_next_audit(mature_history, tmp_path, invalid):
    path, payload = mature_history
    if invalid == "baseline_type":
        payload["baseline_verification"] = ["invalid"]
    elif invalid == "forward_type":
        payload["forward_window_readiness"] = ["invalid"]
    elif invalid == "window_count":
        payload["forward_window_readiness"]["ready_window_count"] = "invalid"
    elif invalid == "source_sha":
        (tmp_path / "report_json").write_text("changed synthetic source")
    elif invalid == "metrics":
        payload["forward_window_readiness"]["windows"][0]["added"]["avg_return_pct"] += 1
    else:
        nav_path = tmp_path / "outcomes.gz"
        nav_path.write_bytes(nav_path.read_bytes()[:20])
        payload["forward_window_readiness"]["nav_evidence"]["sha256"] = scores.file_sha256(nav_path)
    path.write_text(json.dumps(payload))
    summary = scores._historical_shadow_readiness(tmp_path, project_root=tmp_path)
    assert summary["mature_distinct_snapshot_count"] == 0
    assert summary["excluded_unverified_artifact_count"] == 1


@pytest.mark.parametrize("path_style", ["native", "relative", "docker"])
def test_score_workflow_logs_only_a_sha_bound_history_gap(tmp_path, monkeypatch, path_style):
    from backend.workflow import WorkflowManager
    directory = tmp_path / "reports/audit/score_dedup_shadow"
    directory.mkdir(parents=True)
    path = directory / "score_dedup_shadow_fixture_now.json"
    payload = {"schema_version": 2, "input_contract": frozen.FROZEN_STRATEGY_CONTRACT,
        "run_id": "fixture", "status": "scenario_inputs_incomplete", "passed": False,
        "baseline_verification": {"baseline_verified": True}, "membership_impact": None,
        "shadow_replay": {"status": "missing_tracker_history", "final_count": None,
                          "missing_tracker_history_codes": ["000001"]}}
    path.write_text(json.dumps(payload))
    relative = path.relative_to(tmp_path).as_posix()
    terminal = {"artifact_path": {"native": str(path), "relative": relative, "docker": "/app/" + relative}[path_style],
        "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "run_id": "fixture", "status": payload["status"], "scenario_status": "missing_tracker_history", "passed": False}
    completed = SimpleNamespace(success=False, return_code=2, stdout_tail=[json.dumps(terminal)],
                                stderr_tail=[], meta_path="synthetic.meta.json")
    monkeypatch.setattr("backend.core_runner.run_structured_subprocess", lambda **kw: completed)
    monkeypatch.setattr("backend.core_runner.build_subprocess_log_index", lambda **kw: {})
    logs = []
    manager = object.__new__(WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs")
    manager.store = SimpleNamespace(append_log=logs.append)
    manager._run_score_dedup_shadow_audit("fixture")
    assert any("情景历史输入不足" in line and "returncode=2" in line for line in logs)
    assert not any("审计完成" in line for line in logs)
    path.write_text(path.read_text()+" ")
    logs.clear()
    manager._run_score_dedup_shadow_audit("fixture")
    assert any("执行失败" in line for line in logs)
    assert not any("情景历史输入不足" in line for line in logs)


def test_workflow_passes_the_same_explicit_report_to_both_shadow_audits(tmp_path, monkeypatch):
    from backend.workflow import WorkflowManager
    commands = []
    report_path = tmp_path / "diagnostic/revision_a/report.json"
    def run(**kwargs):
        commands.append(kwargs["cmd"])
        return SimpleNamespace(success=True, return_code=0, stdout_tail=[], stderr_tail=[], meta_path="synthetic.meta.json")
    monkeypatch.setattr("backend.core_runner.run_structured_subprocess", run)
    monkeypatch.setattr("backend.core_runner.build_subprocess_log_index", lambda **kw: {})
    monkeypatch.setattr("backend.workflow.resolve_committed_path", lambda *args: report_path)
    manager = object.__new__(WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "runs")
    manager.store = SimpleNamespace(append_log=lambda line: None)
    manager._run_annual_return_gate_shadow_audit("fixture", report={"artifacts": {"report_json": "diagnostic/revision_a/report.json"}})
    assert len(commands) == 2
    assert all(command[-2:] == ["--report", str(report_path)] for command in commands)
