from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.audit_tracker_auxiliary_scores import (
    _classify_risk_snapshot_row,
    _tracker_auxiliary_rebuild_required,
)


def test_tracker_auxiliary_rebuild_flag_follows_material_score_drift() -> None:
    assert _tracker_auxiliary_rebuild_required(
        {"risk_score": {"changed_gt_0_01_count": 2}}
    ) is True
    assert _tracker_auxiliary_rebuild_required(
        {"risk_score": {"changed_gt_0_01_count": 0}}
    ) is False


def test_tracker_risk_snapshot_classification_distinguishes_policy_and_source_lag() -> None:
    expected = "2026-08-21"

    assert _classify_risk_snapshot_row(
        {
            "risk_end_date": None,
            "latest_nav_date": expected,
            "has_holding_period": 1,
            "holding_days": 120,
            "nav_window_coverage_days": 365,
            "nav_window_rows": 243,
        },
        expected_risk_end=expected,
        holding_days_threshold=90,
    ) == "policy_excluded_holding_period"

    assert _classify_risk_snapshot_row(
        {
            "risk_end_date": "2026-07-28",
            "latest_nav_date": "2026-07-28",
            "has_holding_period": 0,
            "holding_days": 0,
            "nav_window_coverage_days": 365,
            "nav_window_rows": 243,
        },
        expected_risk_end=expected,
        holding_days_threshold=90,
    ) == "source_nav_stale_risk_aligned"

    assert _classify_risk_snapshot_row(
        {
            "risk_end_date": None,
            "latest_nav_date": "2026-07-28",
            "has_holding_period": 0,
            "holding_days": 0,
            "nav_window_coverage_days": 215,
            "nav_window_rows": 111,
        },
        expected_risk_end=expected,
        holding_days_threshold=90,
    ) == "source_nav_stale_insufficient_1y_history"


def test_tracker_risk_snapshot_classification_flags_unexplained_missing_row() -> None:
    assert _classify_risk_snapshot_row(
        {
            "risk_end_date": None,
            "latest_nav_date": "2026-08-21",
            "has_holding_period": 0,
            "holding_days": 0,
            "nav_window_coverage_days": 365,
            "nav_window_rows": 243,
        },
        expected_risk_end="2026-08-21",
        holding_days_threshold=90,
    ) == "risk_missing_unexplained"
