"""Isolated regressions for holding-period scheduler snapshots."""

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.report_builder import _build_report_counts_summary
from backend.scheduler import ProjectScheduler


def _create_funds_database(path: Path, rows: list[tuple[object, ...]]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE funds (
                   fcode TEXT,
                   ftype TEXT,
                   has_holding_period INTEGER,
                   holding_days INTEGER,
                   holding_period_source TEXT
               )"""
        )
        conn.executemany(
            "INSERT INTO funds VALUES (?, '债券型', ?, ?, ?)",
            rows,
        )


def test_snapshot_distinguishes_reviewed_no_signal_from_unknown(tmp_path: Path) -> None:
    database = tmp_path / "holding_period_snapshot.db"
    _create_funds_database(
        database,
        [
            ("000001", 1, 30, "name"),
            ("000002", 1, 90, "name"),
            ("000003", 1, 91, "name"),
            ("000004", 1, 999, "generic_placeholder"),
            ("000005", 0, None, "no_signal_reviewed"),
            ("000006", 0, 0, "name"),
            ("000007", 0, None, None),
        ],
    )

    snapshot = ProjectScheduler._read_holding_period_snapshot(
        SimpleNamespace(database_path=database, exclude_holding_days_gt=90)
    )

    assert snapshot["bond_fund_total"] == 7
    # The legacy 999 generic placeholder is not a measured duration.
    assert snapshot["holding_period_known_total"] == 3
    assert snapshot["holding_period_no_signal_total"] == 2
    assert snapshot["holding_period_unknown_total"] == 2
    assert snapshot["holding_period_name_unparsed_total"] == 0
    assert snapshot["holding_period_generic_total"] == 1
    assert snapshot["holding_period_blocked_total"] == 1
    assert snapshot["holding_period_within_threshold_total"] == 2
    assert snapshot["holding_period_no_signal_code_sum"] == 11
    assert snapshot["holding_period_unknown_code_sum"] == 11
    assert snapshot["holding_period_blocked_code_sum"] == 3
    assert snapshot["holding_period_generic_code_sum"] == 4


def test_snapshot_treats_null_without_source_metadata_as_unknown(tmp_path: Path) -> None:
    database = tmp_path / "legacy_holding_period_snapshot.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE funds (fcode TEXT, ftype TEXT, has_holding_period INTEGER, holding_days INTEGER)")
        conn.execute("INSERT INTO funds VALUES ('000008', '债券型', 0, NULL)")

    snapshot = ProjectScheduler._read_holding_period_snapshot(
        SimpleNamespace(database_path=database, exclude_holding_days_gt=90)
    )

    assert snapshot["bond_fund_total"] == 1
    assert snapshot["holding_period_no_signal_total"] == 0
    assert snapshot["holding_period_unknown_total"] == 1
    assert snapshot["holding_period_unknown_code_sum"] == 8


def test_snapshot_uses_name_to_classify_null_holding_period(tmp_path: Path) -> None:
    database = tmp_path / "holding_period_name_snapshot.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """CREATE TABLE funds (
                   fcode TEXT,
                   shortname TEXT,
                   ftype TEXT,
                   has_holding_period INTEGER,
                   holding_days INTEGER,
                   holding_period_source TEXT
               )"""
        )
        conn.executemany(
            "INSERT INTO funds VALUES (?, ?, '债券型', ?, ?, ?)",
            [
                ("000009", "普通债券A", 0, None, None),
                ("000010", "示例30天持有期债券A", 0, None, None),
                ("000011", "滚动持有债券A", 0, None, None),
            ],
        )

    snapshot = ProjectScheduler._read_holding_period_snapshot(
        SimpleNamespace(database_path=database, exclude_holding_days_gt=90)
    )

    assert snapshot["bond_fund_total"] == 3
    assert snapshot["holding_period_no_signal_total"] == 2
    assert snapshot["holding_period_unknown_total"] == 0
    assert snapshot["holding_period_known_total"] == 1
    assert snapshot["holding_period_within_threshold_total"] == 1


def test_snapshot_uses_fullname_longer_term(tmp_path: Path) -> None:
    database = tmp_path / "fullname.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE funds (fcode TEXT, ftype TEXT, shortname TEXT, fullname TEXT, holding_days INTEGER)")
        conn.execute("INSERT INTO funds VALUES ('000012', '债券型', '30天持有期债券A', '180天持有期债券基金', NULL)")

    snapshot = ProjectScheduler._read_holding_period_snapshot(
        SimpleNamespace(database_path=database, exclude_holding_days_gt=90)
    )

    assert snapshot["holding_period_blocked_total"] == 1
    assert snapshot["holding_period_within_threshold_total"] == 0


def test_empty_detail_impact_uses_name_policy_instead_of_generic_placeholder():
    context = {
        "900001": {"shortname": "普通定开债券A", "holding_days": 999,
                   "has_holding_period": 1, "holding_period_source": "name_generic",
                   "latest_nav_date": "2026-10-20", "has_effective_1y_risk": True},
        "900002": {"shortname": "30天持有期债券A", "fullname": "180天持有期债券基金",
                   "holding_days": None, "has_holding_period": 0,
                   "latest_nav_date": "2026-10-20", "has_effective_1y_risk": True},
    }
    result = ProjectScheduler._classify_empty_detail_report_impact(
        {"funds": [{"fcode": code} for code in context]}, context,
        candidate_date="2026-10-20", risk_effective_end_date="2026-10-20",
        holding_days_threshold=90,
    )
    assert result["impact_counts"]["holding_period_blocked"] == 1
    assert result["impact_counts"]["screening_relevant"] == 1
    assert context["900002"]["holding_days"] is None


def test_report_counts_unknown_separately_from_name_unparsed_and_no_signal() -> None:
    empty = pd.DataFrame()
    statuses = ["结构化持有期未知", "名称未识别持有期限制", "未见持有期限制信号"]
    args = {
        name: empty
        for name in (
            "long_df", "short_df", "merged", "excluded_df", "candidate_df",
            "stable_official_export_full", "enhanced_official_export_full",
            "high_score_rejected_df", "high_score_not_buyable_df",
            "high_score_friction_sample_df", "high_score_scale_gate_sample_df",
            "high_score_scale_gate_near_df", "high_score_scale_gate_near_review_sample_df",
            "high_score_fee_gate_sample_df", "dynamic_official_df", "dynamic_internal_watch_df",
        )
    }
    args.update(
        universe_snapshot={},
        long_term_history_snapshot={},
        generic_holding_placeholder_watch_summary={},
        low_vol_positive_ratio_shadow_watch_summary={},
        official_df=pd.DataFrame({"持有期状态": statuses}),
        internal_watch_df=pd.DataFrame({"持有期状态": statuses}),
        stage_b_reason_summary={},
        overlap_codes=set(),
        fixed_only_codes=set(),
        dynamic_only_codes=set(),
        final_screening_summary={},
        project_tracker_summary={},
        forward_validation_summary={},
    )

    counts = _build_report_counts_summary(**args)

    assert counts["final_holding_period_unknown_count"] == 1
    assert counts["final_holding_period_name_unparsed_count"] == 1
    assert counts["final_holding_period_no_signal_count"] == 1
    assert counts["internal_watch_holding_period_unknown_count"] == 1
    assert counts["internal_watch_holding_period_name_unparsed_count"] == 1
    assert counts["internal_watch_holding_period_no_signal_count"] == 1
