"""Replay clock contracts, including five saved real NAV-boundary cases."""
from datetime import datetime, timedelta
import json
from pathlib import Path

import pandas as pd
import pytest

from backend import project_publish_decisions as decisions
from backend.config import load_settings
from backend.screening_baseline_replay import resolve_evaluation_times, run_report_selection
from backend.screening_replay_snapshot import restore_frame


STAGES = ("decision", "fixed_post_review", "dynamic_post_review")


def test_explicit_stage_clocks_preserve_midnight_and_timezone_boundaries():
    raw = dict(zip(STAGES, ("2026-09-07T23:59:59.123456+08:00", "2026-09-08T00:00:01+08:00", "2026-09-07T16:00:02Z")))
    times, source = resolve_evaluation_times({"generated_at": "2030-01-01T00:00:00", "inputs": {"evaluation_times": raw}})
    assert source == "frozen_stage_timestamps"
    assert times["decision"] == datetime(2026, 9, 7, 23, 59, 59, 123456)
    assert times["fixed_post_review"] == datetime(2026, 9, 8, 0, 0, 1)
    assert times["dynamic_post_review"] == datetime(2026, 9, 8, 0, 0, 2)


def test_legacy_snapshot_uses_its_timestamp_and_labels_the_evidence_limit():
    times, source = resolve_evaluation_times({"generated_at": "2026-09-07T20:45:00", "inputs": {}})
    assert source == "legacy_snapshot_generated_at"
    assert times == {stage: datetime(2026, 9, 7, 20, 45) for stage in STAGES}


@pytest.mark.parametrize("value", [None, "", "invalid", "2026-09-07", 0])
def test_absent_or_invalid_legacy_clock_never_falls_back_to_now(value):
    with pytest.raises(ValueError, match="evaluation_time_missing_or_invalid"):
        resolve_evaluation_times({"generated_at": value, "inputs": {}})


@pytest.mark.parametrize("value", [None, {}, {"decision": "2026-09-07T00:00:00"}])
def test_incomplete_new_stage_clocks_do_not_silently_use_legacy_time(value):
    with pytest.raises(ValueError, match="evaluation_times_incomplete"):
        resolve_evaluation_times({"generated_at": "2026-09-07T20:45:00", "inputs": {"evaluation_times": value}})


class ForbiddenClock(datetime):
    @classmethod
    def now(cls, tz=None):
        raise AssertionError("Replay must not read today's clock for a historical gate")


def test_five_real_nav_boundaries_keep_their_frozen_reasons_after_the_day_changes(monkeypatch):
    fixture = json.loads((Path(__file__).parent / "fixtures/replay_clock_boundary_20260907.json").read_text())
    frame = restore_frame(fixture["frame"])
    times, _ = resolve_evaluation_times({"generated_at": fixture["generated_at"], "inputs": {}})
    monkeypatch.setattr(decisions, "datetime", ForbiddenClock)
    result = run_report_selection(
        settings=load_settings(), scored_universe=frame,
        decision_map=fixture["decision_map"], soft_warning_map=fixture["soft_warning_map"],
        tracker_features=pd.DataFrame(columns=["基金代码"]), autostock_status_map={}, evaluation_times=times,
    )
    for row in result["candidate"].to_dict(orient="records"):
        expected = fixture["expected_base_reasons"][row["基金代码"]]
        assert row["fixed_threshold_reason"] == row["dynamic_threshold_reason"] == expected
        assert "净值日期过旧" not in expected
    shifted = decisions.append_project_decision_columns(
        frame, decision_map=fixture["decision_map"], soft_warning_map=fixture["soft_warning_map"],
        evaluated_at=times["decision"] + timedelta(days=1),
    )
    assert len(shifted) == 5
    assert shifted["基础推荐原因代码"].str.contains("stale_nav").all()


def test_scale_age_uses_the_post_review_stage_clock(monkeypatch):
    row = {"基金代码": "000001", "债基风格标签": "综合纯债",
           "dynamic_threshold_verdict": "正式推荐", "dynamic_threshold_reason_codes": "",
           "manager_min_total_days": 800, "manager_avg_score_overall": 8.5,
           "scale_report_date": "2026-01-01"}
    monkeypatch.setattr(decisions, "datetime", ForbiddenClock)
    monkeypatch.setattr(decisions, "confidence_from_thresholds", lambda row, thresholds: ("中信心", 4.0, "synthetic"))
    time = datetime(2026, 1, 1) + timedelta(days=decisions.SCALE_DATA_THRESHOLDS["max_report_age_days"])
    fresh = decisions.apply_publish_post_filters(pd.DataFrame([row]), threshold_mode="dynamic",
        style_thresholds={}, global_thresholds={}, evaluated_at=time).iloc[0]
    stale = decisions.apply_publish_post_filters(pd.DataFrame([row]), threshold_mode="dynamic",
        style_thresholds={}, global_thresholds={}, evaluated_at=time + timedelta(days=1)).iloc[0]
    assert fresh["项目推荐结论"] == "正式推荐" and fresh["规模数据状态"] == "新鲜"
    assert stale["项目推荐结论"] == "自动诊断"
    assert stale["项目推荐原因代码"] == "scale_report_stale"
