"""Frozen-input contracts; synthetic fixtures below are not business evidence."""
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

from backend import holding_period_gate_shadow as holding
from backend import report_builder
from backend.config import load_settings
from backend.screening_baseline_replay import (
    missing_autostock_observation_codes, missing_tracker_history_codes,
)
from scripts import audit_holding_period_gate_impact as cli


def _features() -> pd.DataFrame:
    return pd.DataFrame([{"基金代码": "000001", "出现次数": 0, "出现频率(%)": 0.0,
                          "连续缺席期数": 0, "当前是否在榜": "否", "跟踪总期数": 12}])


def test_recorded_zero_history_is_available_but_null_or_blank_is_not():
    candidate = pd.DataFrame({"基金代码": ["000001"]})
    frame = _features()
    assert missing_tracker_history_codes(candidate, frame) == []
    for missing in (None, "", float("nan"), float("inf")):
        invalid = frame.astype(object).copy()
        invalid.loc[0, "出现次数"] = missing
        assert missing_tracker_history_codes(candidate, invalid) == ["000001"]
    assert frame.loc[0, "出现次数"] == 0


def test_a_code_only_tracker_row_or_duplicate_is_not_complete_history():
    candidates = pd.DataFrame({"基金代码": ["000001", "000002"]})
    assert missing_tracker_history_codes(candidates, candidates) == ["000001", "000002"]
    duplicate = pd.concat([_features(), _features()], ignore_index=True)
    assert missing_tracker_history_codes(candidates, duplicate) == ["000001", "000002"]
    assert missing_tracker_history_codes(candidates.iloc[:0], pd.DataFrame()) == []


@pytest.mark.parametrize("value", [None, "", "unrecognised"])
def test_blank_exported_autostock_rows_do_not_count_as_observations(value):
    statuses = {"000001": {"AutoStock原始校验状态": value},
                "000002": {"AutoStock原始校验状态": "hard_block"},
                "000003": {"AutoStock原始校验状态": "strict_match"},
                "000004": {"AutoStock原始校验状态": "not_checked"}}
    assert missing_autostock_observation_codes(set(statuses), statuses) == ["000001", "000004"]
    assert missing_autostock_observation_codes({"000005"}, statuses) == ["000005"]


@pytest.mark.parametrize("status", ["unknown", "not_checked"])
def test_only_original_results_can_replay_recorded_autostock_degradation(status):
    statuses = {code: {"AutoStock原始校验状态": status} for code in ("000001", "000002")}
    statuses["000003"] = {"AutoStock原始校验状态": "hard_block"}
    assert missing_autostock_observation_codes(set(statuses), statuses) == ["000001", "000002"]
    assert missing_autostock_observation_codes(set(statuses), statuses,
        observed_unvalidated_codes={"000001"}) == ["000002"]
    # Membership alone cannot make a missing or unrecognised value an outcome.
    for invalid in (None, "", "unrecognised"):
        statuses["000001"]["AutoStock原始校验状态"] = invalid
        assert missing_autostock_observation_codes({"000001"}, statuses,
            observed_unvalidated_codes={"000001"}) == ["000001"]


def test_disabled_or_replaced_autostock_is_not_a_replay_safety_gate():
    statuses = {"000001": {"AutoStock原始校验状态": "not_checked"}}
    disabled = SimpleNamespace(autostock_enabled=False, publication_validation_provider="eastmoney_cross_endpoint")
    replaced = SimpleNamespace(autostock_enabled=True, publication_validation_provider="eastmoney_cross_endpoint")
    # Legacy AutoStock state alone is never accepted as the replacement
    # provider's evidence.
    assert missing_autostock_observation_codes({"000001"}, statuses, settings=disabled) == ["000001"]
    assert missing_autostock_observation_codes({"000001"}, statuses, settings=replaced) == ["000001"]
    statuses["000001"] = {
        "数据校验原始状态": "strict_match",
        "数据校验来源": "eastmoney_cross_endpoint",
        "数据校验契约版本": "test-contract",
    }
    assert missing_autostock_observation_codes({"000001"}, statuses, settings=disabled) == []


def test_missing_snapshot_does_not_read_current_database_or_attempt_a_scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(report_builder, "_read_metadata", lambda *a: pytest.fail("later database read"))
    monkeypatch.setattr(holding, "run_report_selection", lambda **kw: pytest.fail("unverified baseline used"))
    settings = replace(load_settings(), project_root=tmp_path)
    summary, exports = holding.build_holding_period_gate_shadow(settings, {"run_id": "test", "artifacts": {}})
    assert summary["status"] == "baseline_not_verified" and summary["passed"] is False
    assert summary["baseline"]["final_count"] is None
    assert summary["scenario_without_gate"]["final_count"] is None
    assert summary["scenario_without_gate"]["status"] == "not_evaluated"
    assert not exports and summary["later_database_reads"] is False


def test_corrupt_snapshot_fails_before_any_later_data_fallback(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.gz"
    with gzip.open(snapshot, "wt", encoding="utf-8") as stream:
        json.dump({"schema_version": 1, "run_id": "test", "inputs": {}, "inputs_sha256": "invalid"}, stream)
    monkeypatch.setattr(report_builder, "_read_metadata", lambda *a: pytest.fail("later database read"))
    summary, exports = holding.build_holding_period_gate_shadow(
        replace(load_settings(), project_root=tmp_path),
        {"run_id": "test", "artifacts": {"screening_input_snapshot_json_gz": str(snapshot)}},
    )
    assert summary["baseline"]["status"] == "invalid_frozen_inputs"
    assert "signature_mismatch" in summary["baseline_verification"]["reason"]
    assert not exports and summary["passed"] is False


@pytest.fixture
def verified_context(monkeypatch):
    universe = pd.DataFrame({"基金代码": ["000001", "000002"], "holding_days": [0, 180], "综合评分": [70., 90.]})
    baseline = {"status": "matched", "baseline_verified": True,
                "evaluation_times": {"decision": "2026-09-07T23:59:59", "dynamic_post_review": "2026-09-08T00:00:01"},
                "time_reference_source": "frozen_stage_timestamps"}
    context = {"snapshot": {"snapshot_date": "2026-09-04", "inputs": {"decision_map": {}, "soft_warning_map": {}}},
               "settings": SimpleNamespace(exclude_holding_days_gt=90),
               "frames": {"scored_universe_before_holding_gate": universe,
                          "formal_after_all_filters": universe.iloc[:1].copy(),
                          "dynamic_reference": universe.iloc[:1].copy(),
                          "final_selection_tracker_features": _features()},
               "evaluation_times": {key: datetime.fromisoformat(value) for key, value in baseline["evaluation_times"].items()},
               "autostock_status_map": {}}
    monkeypatch.setattr(holding, "verify_frozen_baseline", lambda *a: (baseline, context))
    monkeypatch.setattr(report_builder, "_assign_project_scores", lambda frame: frame.copy())
    return context


def test_explicit_baseline_only_does_not_compute_a_scenario(verified_context, monkeypatch):
    monkeypatch.setattr(holding, "run_report_selection", lambda **kw: pytest.fail("scenario not requested"))
    summary, exports = holding.build_holding_period_gate_shadow(None, {"run_id": "test"}, baseline_only=True)
    assert summary["passed"] is True and summary["status"] == "baseline_verified"
    assert summary["scenario_without_gate"]["status"] == "not_requested"
    assert summary["scenario_without_gate"]["final_count"] is None
    assert set(exports) == {"baseline_members"}


@pytest.mark.parametrize("status,key,code", [
    ("missing_tracker_history", "missing_tracker_history_codes", "000002"),
    ("missing_autostock_observations", "missing_autostock_codes", "000001"),
])
def test_incomplete_scenarios_export_gaps_and_stage_data_without_a_final_list(verified_context, monkeypatch, status, key, code):
    universe = verified_context["frames"]["scored_universe_before_holding_gate"]
    before = universe.copy(deep=True)

    def partial(**kwargs):
        pd.testing.assert_frame_equal(kwargs["scored_universe"], universe)
        assert kwargs["apply_holding_gate"] is False
        assert kwargs["settings"] is verified_context["settings"]
        assert kwargs["evaluation_times"] == verified_context["evaluation_times"]
        return {"status": status, key: [code], "candidate": universe, "post_review": universe,
                "style_thresholds": {}, "global_thresholds": {}}

    monkeypatch.setattr(holding, "run_report_selection", partial)
    summary, exports = holding.build_holding_period_gate_shadow(None, {"run_id": "test"})
    assert summary["passed"] is False and summary["status"] == "scenario_inputs_incomplete"
    assert summary["baseline"]["final_count"] == 1
    assert summary["scenario_without_gate"]["final_count"] is None
    assert summary["scenario_without_gate"][key] == [code]
    assert "scenario_members" not in exports and "impact" not in summary
    assert exports["missing_inputs"]["基金代码"].tolist() == [code]
    assert "未完成计算" in holding.render_holding_period_gate_shadow(summary)
    assert verified_context["settings"].exclude_holding_days_gt == 90
    pd.testing.assert_frame_equal(universe, before)


def test_incremental_guard_cannot_be_omitted_when_its_cohort_is_not_frozen(verified_context, monkeypatch):
    frame = pd.DataFrame([{"基金代码": "000002", "基金名称": "Synthetic bond C",
                           "项目准入方式": "增强债基补充准入", "项目质量信心": "中信心"}])
    monkeypatch.setattr(holding, "run_report_selection", lambda **kw: {
        "status": "computed", "candidate": frame, "post_review": frame, "official": frame,
        "dynamic": frame, "style_thresholds": {}, "global_thresholds": {},
    })
    summary, exports = holding.build_holding_period_gate_shadow(None, {"run_id": "test"})
    scenario = summary["scenario_without_gate"]
    assert scenario["status"] == "missing_frozen_incremental_review"
    assert scenario["missing_incremental_review_codes"] == ["000002"]
    assert scenario["final_count"] is None and scenario["strategy_comparison_allowed"] is False
    assert "scenario_members" not in exports


def test_completed_scenario_with_no_incremental_guard_candidates_is_labelled_counterfactual(verified_context, monkeypatch):
    frame = verified_context["frames"]["scored_universe_before_holding_gate"]
    monkeypatch.setattr(holding, "run_report_selection", lambda **kw: {
        "status": "computed", "candidate": frame, "post_review": frame, "official": frame,
        "dynamic": frame, "style_thresholds": {}, "global_thresholds": {},
    })
    summary, exports = holding.build_holding_period_gate_shadow(None, {"run_id": "test"})
    assert summary["passed"] is True and summary["scenario_without_gate"]["final_count"] == 2
    assert summary["impact"]["added_codes"] == ["000002"]
    assert summary["production_holding_gate_changed"] is False and summary["production_switch_recommended"] is False
    assert len(exports["scenario_members"]) == 2


def test_cli_emits_incomplete_evidence_and_nonzero_exit_without_touching_source_report(tmp_path, monkeypatch, capsys):
    report = tmp_path / "source.json"
    report.write_text(json.dumps({"run_id": "historical", "artifacts": {}}))
    before = report.read_bytes()
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(cli, "_build_candidate_universe", lambda *a: pytest.fail("old live loader called"))
    monkeypatch.setattr(report_builder, "_read_metadata", lambda *a: pytest.fail("later database read"))
    monkeypatch.setattr(sys, "argv", ["audit_holding_period_gate_impact.py", "--report", str(report)])
    assert cli.main() == 2
    terminal = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(terminal["artifact"]).read_text())
    assert payload["baseline"]["status"] == "inputs_missing"
    assert payload["scenario_without_gate"]["final_count"] is None
    assert payload["artifacts"] == {}
    assert report.read_bytes() == before


def test_cli_compresses_full_holding_exports_while_preserving_incomplete_status(tmp_path, monkeypatch, capsys):
    report = tmp_path / "source.json"
    report.write_text(json.dumps({"run_id": "synthetic", "artifacts": {}}))
    original = report.read_bytes()
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports")
    frame = pd.DataFrame({"基金代码": ["000001", "000002"], "合成数值": [0., None],
                          "合成说明": ["", "换行\n原文"]})
    summary = {"run_id": "synthetic", "passed": False, "status": "scenario_inputs_incomplete",
               "baseline": {"status": "matched", "final_count": 1},
               "scenario_without_gate": {"status": "missing_tracker_history", "final_count": None}}
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(holding, "build_holding_period_gate_shadow", lambda *a, **kw: (
        summary, {"candidate": frame, "headers_only": frame.iloc[:0]}))
    monkeypatch.setattr(sys, "argv", ["audit_holding_period_gate_impact.py", "--report", str(report)])
    assert cli.main() == 2
    terminal = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(terminal["artifact"]).read_text())
    assert payload["scenario_without_gate"]["final_count"] is None and payload["passed"] is False
    for name, expected in (("candidate", frame), ("headers_only", frame.iloc[:0])):
        record = payload["artifacts"][name]
        path = Path(record["path"])
        assert path.name.endswith(".csv.gz") and record["format"] == "csv_gzip"
        raw = gzip.decompress(path.read_bytes())
        assert raw == expected.to_csv(index=False).encode("utf-8-sig")
        assert hashlib.sha256(raw).hexdigest() == record["uncompressed_sha256"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]
        assert len(raw) == record["uncompressed_bytes"] and path.stat().st_size == record["bytes"]
        assert (record["rows"], record["columns"]) == expected.shape
        restored = pd.read_csv(path, dtype={"基金代码": str})
        if name == "candidate":
            assert restored["基金代码"].tolist() == ["000001", "000002"]
            assert restored.loc[0, "合成数值"] == 0. and pd.isna(restored.loc[1, "合成数值"])
    assert report.read_bytes() == original
