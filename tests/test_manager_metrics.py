"""Career/score disclosure boundaries; synthetic cases never become business data."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook

from backend import project_tracker, screening_replay_snapshot
from backend.api_payloads.recommendations import build_tracker_current_response
from backend.config import load_settings
from backend.manager_metrics import (
    MANAGER_DISPLAY_COLUMNS, build_manager_metrics, coverage_text,
    manager_metric_contract, normalize_manager_frame, with_manager_display_columns,
)
from backend.project_publish_decisions import apply_publish_post_filters
from backend.report_builder import _read_manager_snapshot
from backend.report_workbook import _add_sheet_with_style


def test_real_partial_score_examples_keep_known_mean_and_disclose_all_bindings():
    fixture = json.loads((Path(__file__).parent / "fixtures/manager_metric_coverage_20260908.json").read_text())
    actual = build_manager_metrics(pd.DataFrame(fixture["funds"]), pd.DataFrame(fixture["managers"])).set_index("fcode")
    expected = {
        "004238": (998, 7.871), "006211": (2299, 8.9), "007347": (1508, 8.277),
        "018137": (368, 7.89), "018961": (998, 7.871), "020716": (368, 8.3745),
    }
    assert set(actual.index) == set(expected)
    for code, (days, score) in expected.items():
        row = actual.loc[code]
        assert row["manager_min_total_days"] == days
        assert row["manager_avg_score_overall"] == score
        assert row["manager_count"] == row["manager_profile_count"] == row["manager_total_days_count"] == 2
        assert row["manager_scored_count"] == 1


@pytest.mark.parametrize("score,expected_count,expected_mean", [(None, 1, 8.0), (0.0, 2, 4.0), (6.0, 2, 7.0)])
def test_missing_score_is_not_a_zero_in_the_mean(score, expected_count, expected_mean):
    row = build_manager_metrics(
        pd.DataFrame([{"fcode": "000001", "manager_ids": "a,b"}]),
        pd.DataFrame([{"mgr_id": "a", "total_days": 500, "score_overall": 8.0},
                      {"mgr_id": "b", "total_days": 0, "score_overall": score}]),
    ).iloc[0]
    assert row["manager_avg_score_overall"] == expected_mean
    assert row["manager_scored_count"] == expected_count
    assert row["manager_min_total_days"] == 0
    assert row["manager_total_days_count"] == 2


@pytest.mark.parametrize("profiles", [pd.DataFrame(), pd.DataFrame([{"mgr_id": "a", "total_days": None, "score_overall": None}])])
def test_missing_profiles_and_all_missing_values_keep_the_bound_denominator(profiles):
    frame = build_manager_metrics(pd.DataFrame([{"fcode": "000001", "manager_ids": "a,b"}]), profiles)
    row = frame.iloc[0]
    assert row["manager_count"] == 2
    assert row["manager_profile_count"] == len(profiles)
    assert row["manager_scored_count"] == row["manager_total_days_count"] == 0
    assert pd.isna(row["manager_avg_score_overall"]) and pd.isna(row["manager_min_total_days"])
    displayed = with_manager_display_columns(frame).iloc[0]
    assert displayed["经理平均综合评分"] == displayed["经理最短从业天数"] == ""
    assert displayed["经理评分覆盖"] == displayed["经理从业数据覆盖"] == "0/2"


@pytest.mark.parametrize("value", [None, "", pd.NA, float("nan")])
def test_no_recorded_manager_binding_is_not_a_claim_of_no_manager(value):
    row = build_manager_metrics(pd.DataFrame([{"fcode": "000001", "manager_ids": value}]), pd.DataFrame()).iloc[0]
    assert row["manager_count"] == 0
    assert coverage_text(row["manager_scored_count"], row["manager_count"]) == "未记录绑定"


def test_duplicate_binding_ids_are_counted_once_and_conflicting_profiles_rejected():
    funds = pd.DataFrame([{"fcode": "000001", "manager_ids": "a| a,b"}])
    profiles = pd.DataFrame([{"mgr_id": "a", "score_overall": 8.0}, {"mgr_id": "b", "score_overall": 6.0}])
    row = build_manager_metrics(funds, profiles).iloc[0]
    assert row["manager_count"] == 2 and row["manager_avg_score_overall"] == 7.0
    with pytest.raises(ValueError, match="duplicate_id"):
        build_manager_metrics(funds, pd.concat([profiles, profiles.iloc[:1]]))


@pytest.mark.parametrize("available,total", [(None, 2), (1, None), (-1, 2), (3, 2), (0.5, 2), (0, 0.5), (float("inf"), 2)])
def test_unknown_or_invalid_coverage_does_not_become_fake_zero(available, total):
    assert coverage_text(available, total) == "未记录"


def test_legacy_tracker_migrates_labels_without_imputing_coverage():
    old = pd.DataFrame([{"基金代码": "000001", "经理稳定性": "稳定", "经理最短任职天数": "0",
                         "经理平均综合评分": "0.00", "出现次数": "3", "当前是否在榜": "是"}])
    untouched = old.copy(deep=True)
    internal = project_tracker._restore_project_tracker_internal_view(old)
    assert internal.iloc[0]["最新经理最短从业天数"] == "0"
    assert internal.iloc[0]["最新经理评分覆盖"] == "未记录"
    exported = project_tracker._build_project_tracker_export_view(internal)
    assert exported.iloc[0]["经理从业与评分状态"] == "已知数据达标"
    assert exported.iloc[0]["经理平均综合评分"] == "0.00"
    assert exported.iloc[0]["经理从业数据覆盖"] == "未记录"
    assert exported.iloc[0]["出现次数"] == "3"
    assert "经理最短任职天数" not in exported
    pd.testing.assert_frame_equal(old, untouched)


def test_canonical_missing_value_is_not_overwritten_by_legacy_alias():
    frame = pd.DataFrame([{"经理最短任职天数": "365", "经理最短从业天数": None}])
    result = normalize_manager_frame(frame)
    assert pd.isna(result.iloc[0]["经理最短从业天数"])
    assert "经理最短任职天数" not in result


def _gate_row(**extra):
    return {
        "基金代码": "000001", "基金名称": "synthetic", "债基风格标签": "综合纯债",
        "dynamic_threshold_verdict": "正式推荐", "dynamic_threshold_watch_bucket": "",
        "dynamic_threshold_reason": "", "dynamic_threshold_reason_codes": "",
        "综合加权得分": 80.0, "近期得分": 78.0, "长期得分": 82.0,
        "1年-年化收益(%)": 5.0, "1年-夏普比率": 2.0, "1年-索提诺比率": 2.0,
        "1年-卡玛比率": 5.0, "1年-正收益比率": 70.0, "1年-最大回撤(%)": 0.01,
        "1年-波动率": 0.02, "费率合计(%)": 0.3, "基金规模(亿)": 20.0,
        "manager_min_total_days": 800, "manager_avg_score_overall": 8.5,
        "scale_report_date": datetime.now().date().isoformat(), "enhanced_supplemental_pass": False,
        **extra,
    }


def _gate(frame):
    thresholds = {"weighted_score_min": 60, "short_score_min": 58, "long_score_min": 60,
                  "annual_return_min": 3, "sharpe_min": 1, "sortino_min": 1, "calmar_min": 3,
                  "positive_ratio_min": 55, "fee_total_max": 0.6, "max_drawdown_max": 0.02,
                  "volatility_max": 0.05, "scale_billion_min": 2}
    return apply_publish_post_filters(frame, threshold_mode="dynamic", style_thresholds={}, global_thresholds=thresholds)


def test_known_manager_score_subset_remains_explicit_and_all_missing_score_is_not_a_formal_pass():
    before = _gate(pd.DataFrame([_gate_row()])).iloc[0]
    after = _gate(pd.DataFrame([_gate_row(manager_count=2, manager_scored_count=1, manager_total_days_count=2)])).iloc[0]
    assert before["项目推荐结论"] == "正式推荐"
    assert after["项目推荐结论"] == "正式推荐"
    assert after["经理评分覆盖"] == "1/2"
    assert after["经理从业数据覆盖"] == "2/2"
    assert after["经理从业与评分状态"] == "已知数据达标/评分覆盖不全"
    missing = _gate(pd.DataFrame([_gate_row(manager_count=2, manager_scored_count=0,
                                            manager_avg_score_overall=None,
                                            manager_total_days_count=2)])).iloc[0]
    assert missing["项目推荐结论"] == "自动诊断"
    assert missing["项目推荐原因代码"] == "manager_score_missing"
    assert before["经理评分覆盖"] == "未记录"
    short = _gate(pd.DataFrame([_gate_row(manager_min_total_days=100)])).iloc[0]
    assert short["项目推荐结论"] == "自动诊断"
    assert short["项目推荐原因代码"] == "manager_tenure_short"
    assert short["经理从业与评分状态"] == "从业偏短"


def test_excel_preserves_coverage_as_text_and_explains_its_denominator(tmp_path):
    book = Workbook()
    frame = with_manager_display_columns(pd.DataFrame([{
        "manager_count": 2, "manager_scored_count": 1, "manager_total_days_count": 2,
        "manager_avg_score_overall": 0.0, "manager_min_total_days": 0,
    }]))[MANAGER_DISPLAY_COLUMNS]
    _add_sheet_with_style(book, title="经理", df=frame)
    path = tmp_path / "coverage.xlsx"
    book.save(path)
    read = load_workbook(path)
    assert read["经理"].cell(2, 4).value == "1/2"
    assert read["经理"].cell(2, 3).value == "0.00"
    assert "有评分经理人数" in read["经理"].cell(1, 4).comment.text
    read.close()


def test_source_snapshot_includes_exact_binding_profiles_and_missing_values(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path)
    (tmp_path / "revision").mkdir()
    frame = pd.DataFrame({"基金代码": ["000001"]})
    metadata = pd.DataFrame({"fcode": ["000001"], "manager_ids": ["a,b"], "manager_names": ["A,B"]})
    profiles = pd.DataFrame({"mgr_id": ["a", "b"], "total_days": [0, None],
                             "score_overall": [0.0, None], "last_updated_at": ["2026-09-07", None]})
    filters = {key: frame.copy() for key in ("candidate_df", "dynamic_publish_official_df",
        "final_selection_tracker_features_df", "final_selection_contraction_df", "dynamic_official_df")}
    filters.update(style_thresholds={}, global_thresholds={}, style_meta={}, effective_nav_candidate_date="2026-09-07")
    summary = {"run_id": "fixture", "generated_at": "2026-09-08", "artifacts": {"report_json": "revision/report.json"}}
    monkeypatch.setattr(screening_replay_snapshot, "source_code_hashes", lambda root: {})
    path = screening_replay_snapshot.write_screening_input_snapshot(
        settings, summary, universe=frame, filters=filters, materialized={"official_df": frame},
        report_inputs={"decision_map": {}, "soft_warning_map": {}, "metadata_df": metadata, "manager_snapshot_df": profiles},
    )
    payload = screening_replay_snapshot.load_screening_input_snapshot(path)
    frames = payload["inputs"]["frames"]
    pd.testing.assert_frame_equal(screening_replay_snapshot.restore_frame(frames["manager_profiles"]), profiles)
    pd.testing.assert_frame_equal(screening_replay_snapshot.restore_frame(frames["fund_manager_bindings"]), metadata)
    assert payload["inputs"]["manager_metric_contract"]["career_days"]["continuous_fund_tenure"] is False
    assert payload["inputs"]["manager_metric_contract"]["time_basis"]["field_observation_date"] is None


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("score,expected", [("0.00", "0.00"), (0, "0.00"), ("8.30", "8.30"), (None, ""), ("", "")])
def test_tracker_api_projects_only_saved_manager_values(legacy, score, expected):
    from backend.app import _normalize_number_text

    row = {"基金代码": "000001", "基金名称": "fixture", "当前是否在榜": "是", "正式投资评分": "80",
           "出现次数": "3", "出现频率(%)": "100", "经理平均综合评分": score}
    if legacy:
        row.update({"经理稳定性": "稳定", "经理最短任职天数": "0"})
    else:
        row.update({"经理从业与评分状态": "评分偏低", "经理最短从业天数": "0",
                    "经理评分覆盖": "1/2", "经理从业数据覆盖": "2/2"})
    payload = build_tracker_current_response(
        {}, load_tracker_dataset_fn=lambda p: {"tracker_records": [row]},
        normalize_portfolio_groups=lambda p: {}, normalize_number_text=_normalize_number_text,
        format_tracker_occurrence_text=lambda *a: "3 / 100%",
        apply_since_inception_annualized_return_to_funds_fn=lambda rows: rows,
    )
    actual = payload["rows"][0]
    assert actual["经理平均综合评分"] == expected
    assert actual["经理最短从业天数"] == "0"
    assert actual["经理评分覆盖"] == ("未记录" if legacy else "1/2")
    assert payload["manager_metric_semantics"] == manager_metric_contract()
    assert "经理稳定性" not in actual


def test_profile_reader_records_stored_update_time_without_touching_database(tmp_path):
    import sqlite3

    path = tmp_path / "profiles.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE fund_managers(mgr_id TEXT, total_days INTEGER, score_overall REAL, last_updated_at TEXT)")
        conn.execute("INSERT INTO fund_managers VALUES('a',0,0,'2026-09-07')")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    row = _read_manager_snapshot(path).iloc[0]
    assert row["last_updated_at"] == "2026-09-07"
    assert row["total_days"] == row["score_overall"] == 0
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
