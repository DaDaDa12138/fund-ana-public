"""Historical replay safeguards; synthetic rows are test data, never fund evidence."""
from dataclasses import replace
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.annual_return_gate_shadow import (
    FrozenAnnualScenarioIncomplete, build_incomplete_annual_shadow_summary,
    load_incomplete_annual_shadow_terminal,
)
from backend.config import load_settings
from backend import project_publish_decisions as decisions
from backend.publish_pipeline import apply_publish_post_filters
from backend.publish_reason_summary import (
    _build_annual_return_gate_shadow_decision_summary, enrich_publish_reason_summary,
)
from backend.screening_replay_snapshot import restore_frame
from scripts import audit_annual_return_gate_shadow as cli
from scripts.audit_data_truthfulness import _validate_live_dashboard_strategy_shadow_contract


def _features():
    return pd.DataFrame([{"基金代码": "000001", "出现次数": 0, "出现频率(%)": 0.,
                          "连续缺席期数": 0, "当前是否在榜": "否", "跟踪总期数": 12}])


def _pending():
    summary = build_incomplete_annual_shadow_summary(
        {"run_id": "historical"}, {"status": "matched", "baseline_verified": True}, baseline_count=1,
        issue=FrozenAnnualScenarioIncomplete("missing_tracker_history", missing_codes=["000001"], stage_frames={}),
    )
    summary["decision_summary"] = _build_annual_return_gate_shadow_decision_summary(summary)
    summary["truth_contract"] = {
        "truth_scope": "strategy_shadow_observation", "publish_truth": False,
        "official_count_field": "counts.final_count", "shadow_count_field": "shadow_final_count",
        "business_value_synthesis": "forbidden", "direct_threshold_relaxation_recommended_now": False,
        "missing_value_policy": "preserve_missing_semantics",
    }
    return summary


class ForbiddenClock(datetime):
    @classmethod
    def now(cls, tz=None):
        raise AssertionError("historical gate consulted current clock")


def test_annual_prepare_replays_five_real_nav_boundaries_with_the_saved_clock(monkeypatch):
    data = json.loads((Path(__file__).parent / "fixtures/replay_clock_boundary_20260907.json").read_text())
    monkeypatch.setattr(decisions, "datetime", ForbiddenClock)
    candidate, _, _, _ = cli._prepare_shadow_candidate_df(
        load_settings(), universe_df=restore_frame(data["frame"]), decision_map=data["decision_map"],
        soft_warning_map=data["soft_warning_map"], evaluated_at=datetime.fromisoformat(data["generated_at"]),
    )
    assert len(candidate) == 5
    for row in candidate.to_dict(orient="records"):
        assert row["dynamic_threshold_reason"] == data["expected_base_reasons"][row["基金代码"]]
        assert "stale_nav" not in row["基础推荐原因代码"]


def test_shared_post_filter_wrapper_preserves_historical_scale_age(monkeypatch):
    monkeypatch.setattr(decisions, "datetime", ForbiddenClock)
    monkeypatch.setattr(decisions, "confidence_from_thresholds", lambda row, thresholds: ("中信心", 4., "synthetic"))
    row = {"基金代码": "000001", "债基风格标签": "综合纯债", "dynamic_threshold_verdict": "正式推荐",
           "dynamic_threshold_reason_codes": "", "manager_min_total_days": 800,
           "manager_avg_score_overall": 8.5, "scale_report_date": "2026-01-01"}
    result = apply_publish_post_filters(pd.DataFrame([row]), threshold_mode="dynamic", style_thresholds={},
                                       global_thresholds={}, evaluated_at=datetime(2026, 1, 2))
    assert result.iloc[0]["项目推荐结论"] == "正式推荐"
    assert result.iloc[0]["规模数据状态"] == "新鲜"


@pytest.fixture
def frozen_materializer(monkeypatch):
    row = {"基金代码": "000001", "基金名称": "Synthetic Bond A", "债基风格标签": "综合纯债",
           "dynamic_threshold_reason_codes": "below_annual_floor", "dynamic_threshold_verdict": "自动诊断",
           "dynamic_threshold_watch_bucket": "动态质量诊断", "dynamic_threshold_reason": "年化收益不足",
           "项目推荐结论": "正式推荐", "推荐理由": "synthetic", "项目推荐原因代码": "",
           "项目质量信心": "高信心", "项目准入方式": "动态阈值准入"}
    candidate = pd.DataFrame([row])
    context = {"frames": {"final_selection_tracker_features": _features()},
               "evaluation_times": {"dynamic_post_review": datetime(2026, 9, 7, 23, 59)}}
    # These fixtures exercise the legacy frozen AutoStock contract. Production
    # now uses the provider-aware Eastmoney path, so make the historical
    # provider explicit instead of inheriting the live project configuration.
    replay_settings = replace(load_settings(), autostock_enabled=True, publication_validation_provider="autostock")
    selected_inputs = []
    observed_tracks = []
    monkeypatch.setattr(cli, "_build_final_selection_tracker_features", lambda *a, **kw: pytest.fail("read live Tracker"))
    monkeypatch.setattr(cli, "_safe_supplement_official_autostock_statuses", lambda *a, **kw: pytest.fail("queried later API"))
    monkeypatch.setattr(cli, "_apply_incremental_safety_watch_alignment", lambda **kw: pytest.fail("read later report cohort"))
    monkeypatch.setattr(cli, "_materialize_track_frame", lambda frame, **kw: observed_tracks.append(frame.copy()) or frame.copy())

    def review(frame, **kwargs):
        assert kwargs["evaluated_at"] == context["evaluation_times"]["dynamic_post_review"]
        return frame.copy()

    def select(frame, **kwargs):
        selected_inputs.append(kwargs["tracker_features_df"].copy())
        return frame.copy()

    monkeypatch.setattr(cli, "_apply_publish_post_filters", review)
    monkeypatch.setattr(cli, "_apply_final_official_selection", select)

    def run(status="strict_match", supplement=False):
        return cli._materialize_shadow_publish_result(
            replay_settings, candidate_df=candidate, style_thresholds={}, global_thresholds={},
            run_id="historical", generated_at="2026-09-07T23:59:00", snapshot_date="2026-09-04",
            autostock_status_map={"000001": {"AutoStock原始校验状态": status}},
            supplement_runtime_autostock=supplement, frozen_context=context,
        )

    return candidate, context, run, selected_inputs, observed_tracks


def test_frozen_materializer_keeps_real_zero_history_and_does_not_claim_new_api_observation(frozen_materializer):
    candidate, _, run, selected, tracks = frozen_materializer
    before = candidate.copy(deep=True)
    official, _, runtime_available, _, features = run()
    assert official["基金代码"].tolist() == ["000001"]
    assert official["AutoStock原始校验状态"].tolist() == ["strict_match"]
    assert runtime_available is False and selected[0].iloc[0]["出现次数"] == 0
    assert features.iloc[0]["出现频率(%)"] == 0.
    assert tracks[0].iloc[0]["dynamic_threshold_reason_codes"] == ""
    pd.testing.assert_frame_equal(candidate, before)


def test_missing_tracker_stops_before_selection_without_defaulting_to_zero(frozen_materializer):
    _, context, run, selected, _ = frozen_materializer
    context["frames"]["final_selection_tracker_features"].loc[0, "出现次数"] = None
    with pytest.raises(FrozenAnnualScenarioIncomplete) as error:
        run()
    assert error.value.status == "missing_tracker_history" and error.value.missing_codes == ["000001"]
    assert not selected
    assert "scenario_members" not in error.value.stage_frames


@pytest.mark.parametrize("status", [None, "", "invalid"])
def test_blank_or_unknown_enum_autostock_is_an_evidence_gap(frozen_materializer, status):
    with pytest.raises(FrozenAnnualScenarioIncomplete) as error:
        frozen_materializer[2](status)
    assert error.value.status == "missing_autostock_observations"
    assert error.value.missing_codes == ["000001"]


def test_hard_block_is_still_removed_in_frozen_annual_scenario(frozen_materializer):
    official, diagnoses, _, _, _ = frozen_materializer[2]("hard_block")
    assert official.empty and diagnoses["基金代码"].tolist() == ["000001"]


def test_historical_context_rejects_requesting_current_api_supplement(frozen_materializer):
    with pytest.raises(ValueError, match="cannot_supplement_current_autostock"):
        frozen_materializer[2](supplement=True)


def test_incremental_review_is_not_skipped_for_secondary_enhanced_share(frozen_materializer):
    candidate, _, run, _, _ = frozen_materializer
    candidate.loc[0, "基金名称"] = "Synthetic Bond C"
    candidate.loc[0, "项目质量信心"] = "中信心"
    candidate.loc[0, "项目准入方式"] = "增强债基补充准入"
    with pytest.raises(FrozenAnnualScenarioIncomplete) as error:
        run()
    assert error.value.status == "missing_frozen_incremental_review"


def test_verified_previous_membership_reaches_frozen_annual_review_without_live_history(frozen_materializer):
    from backend.incremental_review_replay import CONTRACT

    candidate, context, run, _, _ = frozen_materializer
    candidate.loc[0, "基金名称"] = "Synthetic Bond C"
    candidate.loc[0, "项目质量信心"] = "中信心"
    candidate.loc[0, "项目准入方式"] = "增强债基补充准入"
    context["incremental_review_history"] = {"contract": CONTRACT, "status": "captured", "previous_codes": ["000001"]}
    official, _, runtime_available, _, _ = run()
    assert official["基金代码"].tolist() == ["000001"] and runtime_available is False


@pytest.mark.parametrize("corrupt", [False, True])
def test_cli_rejects_missing_or_corrupt_snapshot_without_current_data_fallback(tmp_path, monkeypatch, capsys, corrupt):
    report = {"run_id": "historical", "artifacts": {}}
    if corrupt:
        snapshot = tmp_path / "snapshot.gz"
        with gzip.open(snapshot, "wt", encoding="utf-8") as stream:
            json.dump({"schema_version": 1, "run_id": "historical", "inputs": {}, "inputs_sha256": "bad"}, stream)
        report["artifacts"]["screening_input_snapshot_json_gz"] = str(snapshot)
    source = tmp_path / "source.json"
    source.write_text(json.dumps(report))
    original = source.read_bytes()
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(cli, "_build_candidate_universe", lambda *a, **kw: pytest.fail("read current universe"))
    monkeypatch.setattr(cli, "_build_frozen_annual_summary", lambda *a: pytest.fail("unverified comparison"))
    monkeypatch.setattr(sys, "argv", ["audit_annual_return_gate_shadow.py", "--report", str(source)])
    assert cli.main() == 2
    terminal = json.loads(capsys.readouterr().out)
    assert not Path(terminal["artifact"]).is_absolute()
    result = json.loads((tmp_path / terminal["artifact"]).read_text())
    assert result["status"] == "baseline_not_verified"
    assert result["baseline_final_count"] is None and result["shadow_final_count"] is None
    assert result["decision_summary"]["shadow_final_count"] is None
    assert result["artifacts"] == {} and source.read_bytes() == original
    assert terminal["artifact_sha256"] == hashlib.sha256((tmp_path / terminal["artifact"]).read_bytes()).hexdigest()


def test_baseline_only_preserves_a_verified_empty_list_and_skips_scenario(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"run_id": "empty", "artifacts": {}}))
    monkeypatch.setattr(cli, "load_settings", lambda: replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports"))
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: (
        {"status": "matched", "baseline_verified": True}, {"frames": {"formal_after_all_filters": pd.DataFrame()}}))
    monkeypatch.setattr(cli, "_build_frozen_annual_summary", lambda *a: pytest.fail("unrequested scenario"))
    monkeypatch.setattr(sys, "argv", ["audit_annual_return_gate_shadow.py", "--report", str(source), "--baseline-only"])
    assert cli.main() == 0
    terminal = json.loads(capsys.readouterr().out)
    assert not Path(terminal["artifact"]).is_absolute()
    result = json.loads((tmp_path / terminal["artifact"]).read_text())
    assert result["baseline_final_count"] == 0 and result["decision_summary"]["baseline_final_count"] == 0
    assert result["shadow_final_count"] is None and result["scenario_status"] == "not_requested"


@pytest.mark.parametrize("write_failure", [False, True])
def test_cli_keeps_annual_path_contract_and_delivers_only_complete_compressed_exports(
    tmp_path, monkeypatch, capsys, write_failure,
):
    from backend.annual_return_gate_shadow import _load_shadow_for_report

    source = tmp_path / "source.json"
    report = {"run_id": "synthetic", "publication_control": {"revision": "fixture"},
              "artifacts": {"report_json": "source.json"}}
    source.write_text(json.dumps(report))
    original = source.read_bytes()
    baseline = {"status": "matched", "baseline_verified": True, "source_files": {
        "report_json": {"path": "source.json", "sha256": hashlib.sha256(original).hexdigest()}}}
    frame = pd.DataFrame({"基金代码": ["000001", "000002"], "合成数值": [0., None],
                          "合成说明": ['逗号,引号"', "第一行\n第二行"],
                          "dynamic_threshold_reason_codes": ["", "below_annual_floor"]})
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *a: (
        baseline, {"frames": {"formal_after_all_filters": frame.iloc[:1]}}))

    def incomplete(*args):
        raise FrozenAnnualScenarioIncomplete("missing_tracker_history", missing_codes=["000002"],
            stage_frames={"candidate_before_annual_relief": frame})

    monkeypatch.setattr(cli, "_build_frozen_annual_summary", incomplete)
    monkeypatch.setattr(sys, "argv", ["audit_annual_return_gate_shadow.py", "--report", str(source)])
    if write_failure:
        def failed_write(*args):
            raise OSError("synthetic export failure")

        monkeypatch.setattr(cli, "write_compressed_audit_csv", failed_write)
        with pytest.raises(OSError, match="synthetic export failure"):
            cli.main()
        assert not list((settings.reports_dir / "audit/annual_return_gate_shadow").iterdir())
    else:
        assert cli.main() == 2
        terminal = json.loads(capsys.readouterr().out)
        assert not Path(terminal["artifact"]).is_absolute()
        summary_path = tmp_path / terminal["artifact"]
        result = json.loads(summary_path.read_text())
        assert result["shadow_final_count"] is None and result["scenario_status"] == "missing_tracker_history"
        assert set(result["artifacts"]) == set(result["artifact_storage"]) == {
            "candidate_before_annual_relief", "missing_inputs"}
        for key, value in result["artifacts"].items():
            assert isinstance(value, str) and value.endswith(".csv.gz")
            artifact = tmp_path / value
            metadata = result["artifact_storage"][key]
            raw = gzip.decompress(artifact.read_bytes())
            assert hashlib.sha256(raw).hexdigest() == metadata["uncompressed_sha256"]
            assert hashlib.sha256(artifact.read_bytes()).hexdigest() == metadata["sha256"]
            assert len(raw) == metadata["uncompressed_bytes"] and artifact.stat().st_size == metadata["bytes"]
            if key == "candidate_before_annual_relief":
                assert raw == frame.to_csv(index=False).encode("utf-8-sig")
                assert metadata["rows"] == 2 and metadata["columns"] == 4
        loaded_path, loaded = _load_shadow_for_report(settings, report)
        assert loaded_path == summary_path and loaded["report_binding"]["status"] == "matched"
        assert loaded["artifacts"] == result["artifacts"] and loaded["shadow_final_count"] is None
    assert source.read_bytes() == original


def test_incomplete_summary_and_api_preserve_missing_counts_and_report_the_gap():
    summary = _pending()
    decision = summary["decision_summary"]
    assert decision["available"] is False and decision["baseline_final_count"] == 1
    assert decision["shadow_final_count"] is None and decision["net_final_count_delta"] is None
    result, blockers = _validate_live_dashboard_strategy_shadow_contract(
        {"publish_reason_summary": {"annual_return_gate_shadow": summary}}, expected_publication={"counts": {"final_count": 1}})
    assert blockers == [] and result["scenario_status"] == "missing_tracker_history"


def test_focus_summary_and_scoped_dashboard_keep_unknown_impact_as_null():
    from backend.api_payloads.dashboard import _reduce_publish_reason_summary_for_scoped_pages

    summary = enrich_publish_reason_summary({"available": True, "annual_return_gate_shadow": _pending()})
    focus = next(row for row in summary["focus_metric_summary"] if row["metric_key"] == "annual_return_gate_shadow")
    assert focus["net_final_count_delta"] is None and focus["portfolio_direct_relieved_count"] is None
    assert focus["intercepted_count"] is None and "净增未计算" in focus["top_display_reasons"]
    reduced = _reduce_publish_reason_summary_for_scoped_pages(summary)
    assert reduced["annual_return_gate_shadow"]["shadow_final_count"] is None
    assert reduced["annual_return_gate_shadow"]["decision_summary"]["available"] is False
    fallback = enrich_publish_reason_summary({"available": True, "focus_metric_summary": [focus]})
    restored = next(row for row in fallback["focus_metric_summary"] if row["metric_key"] == "annual_return_gate_shadow")
    assert restored["portfolio_direct_relieved_count"] is None


@pytest.mark.parametrize("field,value,error", [
    ("shadow_final_count", 0, "uncomputed_counts_not_null"),
    ("strategy_comparison_allowed", True, "incomplete_marked_available"),
    ("missing_inputs", {}, "missing_evidence_not_explained"),
])
def test_truthfulness_audit_rejects_fake_completion_or_unexplained_gaps(field, value, error):
    summary = _pending()
    summary[field] = value
    _, blockers = _validate_live_dashboard_strategy_shadow_contract(
        {"publish_reason_summary": {"annual_return_gate_shadow": summary}}, expected_publication={"counts": {"final_count": 1}})
    assert f"dashboard_annual_return_shadow_{error}" in blockers


def test_decision_summary_retains_recorded_zero_and_does_not_fill_unknown_forward_counts():
    payload = {"baseline_final_count": 0, "shadow_final_count": 0, "net_final_count_delta": 0,
               "local_style_band_shadow_scenarios": [{"candidate_count": 0, "direct_relieved_final_count": 0}]}
    result = _build_annual_return_gate_shadow_decision_summary(payload)
    assert result["shadow_final_count"] == 0 and result["net_final_count_delta"] == 0
    assert result["top_local_shadow_scenarios"][0]["forward_validation_ready_window_count"] is None


def test_incomplete_terminal_requires_matching_run_path_and_bytes(tmp_path):
    directory = tmp_path / "reports/audit/annual_return_gate_shadow"
    directory.mkdir(parents=True)
    path = directory / "annual_return_gate_shadow_historical_now.json"
    path.write_text(json.dumps(_pending()))
    terminal = {"artifact": str(path), "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "passed": False, "status": "scenario_inputs_incomplete", "scenario_status": "missing_tracker_history"}
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")
    kwargs = {"run_id": "historical", "stdout_tail": [json.dumps(terminal)], "return_code": 2}
    assert load_incomplete_annual_shadow_terminal(settings, **kwargs)["status"] == "scenario_inputs_incomplete"
    assert load_incomplete_annual_shadow_terminal(settings, **{**kwargs, "run_id": "another"}) is None
    assert load_incomplete_annual_shadow_terminal(settings, **{**kwargs, "return_code": 1}) is None
    path.write_text(path.read_text() + " ")
    assert load_incomplete_annual_shadow_terminal(settings, **kwargs) is None


def test_workflow_logs_bound_evidence_gap_and_preserves_nonzero_exit(tmp_path, monkeypatch):
    from backend.workflow import WorkflowManager

    directory = tmp_path / "reports/audit/annual_return_gate_shadow"
    directory.mkdir(parents=True)
    path = directory / "annual_return_gate_shadow_historical_now.json"
    path.write_text(json.dumps(_pending()))
    terminal = json.dumps({"artifact": str(path), "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                           "passed": False, "status": "scenario_inputs_incomplete", "scenario_status": "missing_tracker_history"})
    result = SimpleNamespace(success=False, return_code=2, stdout_tail=[terminal], stderr_tail=[], meta_path="synthetic.meta.json")
    monkeypatch.setattr("backend.core_runner.run_structured_subprocess", lambda **kw: result)
    monkeypatch.setattr("backend.core_runner.build_subprocess_log_index", lambda **kw: {})
    logs = []
    manager = object.__new__(WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs")
    manager.store = SimpleNamespace(append_log=logs.append)
    monkeypatch.setattr(manager, "_run_score_dedup_shadow_audit", lambda *a: pytest.fail("unverified followup"))
    manager._run_annual_return_gate_shadow_audit("historical")
    assert any("历史证据核验未完成" in line and "returncode=2" in line for line in logs)
    assert not any("审计完成" in line for line in logs)
    path.write_text(path.read_text() + " ")
    logs.clear()
    manager._run_annual_return_gate_shadow_audit("historical")
    assert any("执行失败" in line for line in logs)
