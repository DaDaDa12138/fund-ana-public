"""Undefined denominators differ from defined ratios whose numerator is zero."""
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
import sqlite3
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))

from analyzers.risk_analyzer import RiskAnalyzer
from backend.effective_nav import _risk_metric_ready_predicate
from backend.favorites_export import build_favorites_export_workbook
from backend.risk_metric_status import load_undefined_risk_windows, overlay_risk_unavailability, prepare_risk_display_row
from bond_fund_screening.core.standard_risk_metrics import calculate_standard_risk_metrics
from database.connection import DatabaseManager
from database.migrations import migrate_add_columns
from database.models import Base, Fund, FundNavHistory, FundReturns, FundRiskMetrics
from openpyxl import load_workbook


def _nav_frame(returns: np.ndarray | None = None) -> pd.DataFrame:
    returns = np.zeros(30) if returns is None else returns
    dates = pd.date_range("2025-01-01", periods=len(returns) + 1, freq="12D")
    nav = np.concatenate(([1.0], np.cumprod(1 + returns)))
    return pd.DataFrame({"nav": nav, "accum_nav": nav, "daily_growth": None}, index=dates)


def test_defined_zero_ratios_remain_real_zero() -> None:
    analyzer = RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": 0.0}}, db_manager=object())
    frame = _nav_frame(np.array([0.001, -0.001] * 15))
    metrics = analyzer._calculate_period_metrics(
        frame, "1y", api_return=0.0, api_return_date=frame.index[-1], api_snapshot_date=frame.index[-1],
    )
    assert metrics is not None and metrics["volatility"] > 0 and metrics["max_drawdown"] > 0
    for key in ("sharpe_ratio", "sortino_ratio", "calmar_ratio", "raw_sharpe_ratio", "raw_sortino_ratio"):
        assert metrics[key] == 0.0
    assert metrics["undefined_metric_reasons"] == {}
    assert metrics["sharpe_ratio_cap_hit"] is False
    assert metrics["sortino_ratio_cap_hit"] is False


def test_constant_returns_below_mar_can_have_defined_sortino() -> None:
    metrics = calculate_standard_risk_metrics(
        np.zeros(30), np.ones(31), annual_return=0.0, periods_per_year=252,
        risk_free_rate=0.02, mar=0.02,
    )
    assert metrics["sharpe_ratio"] is None
    assert metrics["sharpe_ratio_cap_hit"] is None
    assert metrics["raw_sortino_ratio"] == pytest.approx(-np.sqrt(252))
    assert metrics["sortino_ratio_cap_hit"] is True
    assert "sortino_ratio" not in metrics["undefined_metric_reasons"]


@pytest.mark.parametrize("writer", ["single", "replace_fund", "batch", "replace_all"])
def test_all_risk_writers_preserve_null_reasons_and_real_zero(tmp_path: Path, writer: str) -> None:
    manager = DatabaseManager(db_path=str(tmp_path / "risk.db"), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    analyzer = RiskAnalyzer(config={"risk_metrics": {"periods": ["1y"], "risk_free_rate": 0.015}}, db_manager=manager)
    analyzer._write_full_refresh_audit = lambda **kwargs: None
    frame = _nav_frame()
    with manager.get_session() as session:
        session.add(Fund(fcode="000001", shortname="隔离零波动样本", has_holding_period=False))
        session.add(FundReturns(fcode="000001", latest_nav_date=frame.index[-1].date(), latest_nav=1.0))
        session.add_all(FundNavHistory(fcode="000001", nav_date=when.date(), unit_nav=1.0, accum_nav=1.0) for when in frame.index)
    if writer in {"batch", "replace_all"}:
        result = analyzer.calculate_all_funds(workers=1, force_update=True, target_fcodes=["000001"] if writer == "batch" else None)
    else:
        result = analyzer.calculate_fund_risk("000001", force_update=writer == "replace_fund")
    assert result == 1
    with manager.get_session(readonly=True) as session:
        row = session.query(FundRiskMetrics).one()
        for key in ("sharpe_ratio", "sortino_ratio", "calmar_ratio", "raw_sharpe_ratio", "raw_sortino_ratio", "sharpe_ratio_cap_hit", "sortino_ratio_cap_hit"):
            assert getattr(row, key) is None
        assert row.annual_return == row.volatility == row.max_drawdown == 0.0
        assert row.max_drawdown_duration == 0
        reasons = json.loads(row.undefined_metric_reasons_json)
        assert set(reasons) == {"sharpe_ratio", "sortino_ratio", "calmar_ratio"}
    with sqlite3.connect(manager.db_path) as connection:
        assert connection.execute(f"SELECT COUNT(*) FROM fund_risk_metrics risk WHERE {_risk_metric_ready_predicate(connection, 'risk')}").fetchone()[0] == 0


def _display_row() -> dict:
    return {
        "1年-年化收益(%)": 0.0, "1年-波动率": 0.0, "1年-最大回撤(%)": 0.0,
        "1年-夏普比率": None, "1年-索提诺比率": None, "1年-卡玛比率": None,
        "1年-VaR95(%)": 0.0, "1年-VaR99(%)": 0.0, "1年-正收益比率": 0.0,
        "__risk_end_date": "2026-09-09", "__risk_data_points": 242, "__risk_snapshot_ready": 0,
        "__risk_undefined_reasons_json": json.dumps({
            "sharpe_ratio": "zero_or_negligible_volatility",
            "sortino_ratio": "zero_or_negligible_downside_deviation",
            "calmar_ratio": "zero_drawdown",
        }),
    }


def test_display_explains_undefined_values_without_discarding_known_zero() -> None:
    row = prepare_risk_display_row(_display_row())
    assert row["1年-夏普比率"] is None
    assert row["1年-年化收益(%)"] == row["1年-最大回撤(%)"] == 0.0
    assert len(row["__risk_metric_unavailability"]) == 3


@pytest.mark.parametrize("raw", [None, "{", "[]", '{"sharpe_ratio":"unknown"}'])
def test_unknown_incomplete_risk_row_does_not_become_displayable(raw: str | None) -> None:
    row = _display_row()
    row["__risk_undefined_reasons_json"] = raw
    result = prepare_risk_display_row(row)
    assert result["1年-年化收益(%)"] is None
    assert not result.get("__risk_metric_unavailability")


def test_same_snapshot_old_zero_is_corrected_but_different_window_is_preserved() -> None:
    snapshot = prepare_risk_display_row(_display_row())
    current = overlay_risk_unavailability({"最新净值日期": "2026-09-09", "1年-夏普比率": "0"}, snapshot)
    assert current["1年-夏普比率"] == ""
    assert current["__risk_metric_unavailability"]["1年-夏普比率"]["end_date"] == "2026-09-09"
    historical = overlay_risk_unavailability({"最新净值日期": "2026-08-01", "1年-夏普比率": "0"}, snapshot)
    assert historical["1年-夏普比率"] == "0"
    assert "1年-夏普比率" not in historical.get("__risk_metric_unavailability", {})
    different_value = overlay_risk_unavailability({"最新净值日期": "2026-09-09", "1年-夏普比率": "0.2"}, snapshot)
    assert different_value["1年-夏普比率"] == "0.2"
    assert "1年-夏普比率" not in different_value.get("__risk_metric_unavailability", {})


def test_favorite_overlay_does_not_invent_a_date_match_or_replace_real_zero() -> None:
    from backend.app import _overlay_favorite_row_with_database_snapshot

    snapshot = prepare_risk_display_row(_display_row())
    snapshot["最新净值日期"] = "2026-09-09"
    snapshot["近1月(%)"] = "0.3"
    undated = _overlay_favorite_row_with_database_snapshot({"1年-夏普比率": "0", "近1月(%)": 0.0}, snapshot)
    assert undated["最新净值日期"] == "2026-09-09"
    assert undated["1年-夏普比率"] == "0"
    assert undated["近1月(%)"] == 0.0
    assert "1年-夏普比率" not in undated.get("__risk_metric_unavailability", {})
    matched = _overlay_favorite_row_with_database_snapshot({"最新净值日期": "2026-09-09", "1年-夏普比率": "0"}, snapshot)
    assert matched["1年-夏普比率"] == ""
    assert matched["__risk_metric_unavailability"]["1年-夏普比率"]["end_date"] == "2026-09-09"


def test_favorite_sql_reader_preserves_known_zero_but_rejects_unexplained_missing_window(tmp_path: Path, monkeypatch) -> None:
    import backend.app as backend_app

    manager = DatabaseManager(db_path=str(tmp_path / "display.db"), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    with manager.get_session() as session:
        for code, reasons in (("000001", _display_row()["__risk_undefined_reasons_json"]), ("000002", None)):
            session.add(Fund(fcode=code, shortname="隔离风险语义样本", has_holding_period=False))
            session.add(FundReturns(fcode=code, latest_nav_date=date(2026, 9, 9), latest_nav=1.0))
            session.add(FundRiskMetrics(
                fcode=code, period="1y", start_date=date(2025, 9, 9), end_date=date(2026, 9, 9),
                volatility=0.0, annual_return=0.0, max_drawdown=0.0, var_95=0.0, var_99=0.0,
                positive_ratio=0.0, data_points=242, undefined_metric_reasons_json=reasons,
            ))
            session.add_all(FundNavHistory(fcode=code, nav_date=when.date(), unit_nav=1.0, accum_nav=1.0)
                            for when in pd.date_range(end="2026-09-09", periods=31, freq="12D"))
    monkeypatch.setattr(backend_app, "settings", replace(backend_app.settings, database_path=Path(manager.db_path)))
    monkeypatch.setattr(backend_app, "load_sales_fee_validation_classification_map", lambda: {})
    rows = backend_app._fetch_favorite_fund_database_snapshot(["000001", "000002"])
    assert rows["000001"]["1年-夏普比率"] == ""
    assert rows["000001"]["1年-波动率"] == rows["000001"]["1年-年化收益(%)"] == "0"
    assert rows["000001"]["__risk_metric_unavailability"]["1年-夏普比率"]["end_date"] == "2026-09-09"
    assert rows["000002"]["1年-年化收益(%)"] == ""
    assert not rows["000002"].get("__risk_metric_unavailability")
    from backend.risk_sync_audit import _load_flagged_rows
    with sqlite3.connect(manager.db_path) as connection:
        windows = load_undefined_risk_windows(connection)
        assert list(windows) == [("000001", "1y")]
        assert windows[("000001", "1y")]["risk_ready"] is False
        flagged = _load_flagged_rows(connection, periods=["1y"], coverage_grace_days=7, min_data_points=30)
        assert [row["only_undefined_ratio_gaps"] for row in flagged] == [True, False]
        assert flagged[0]["period_statuses"][0]["aligned"] is False
        assert flagged[0]["period_statuses"][0]["calculation_status"] == "computed_ratio_undefined"
        assert "calculation_status" not in flagged[1]["period_statuses"][0]
        # An older explained row must never override a newer unexplained one.
        connection.execute("UPDATE fund_risk_metrics SET end_date='2026-09-08' WHERE fcode='000001'")
        # Legacy schemas can contain multiple snapshots; the current schema's
        # (fcode, period) unique constraint remains in place in the main fixture.
        columns = [row[1] for row in connection.execute("PRAGMA table_info(fund_risk_metrics)")]
        with sqlite3.connect(":memory:") as legacy:
            legacy.execute(f"CREATE TABLE fund_risk_metrics ({','.join(columns)})")
            legacy.executemany(f"INSERT INTO fund_risk_metrics VALUES ({','.join('?' for _ in columns)})",
                               connection.execute("SELECT * FROM fund_risk_metrics").fetchall())
            legacy.execute("INSERT INTO fund_risk_metrics (fcode,period,end_date) VALUES ('000001','1y','2026-09-09')")
            assert load_undefined_risk_windows(legacy) == {}


@pytest.mark.parametrize("include_details", [False, True])
def test_favorite_payload_projection_keeps_undefined_reason(monkeypatch, include_details: bool) -> None:
    import backend.app as backend_app

    snapshot = prepare_risk_display_row(_display_row())
    snapshot.update({"基金代码": "000001", "最新净值日期": "2026-09-09"})
    for name, value in (
        ("_load_favorite_recommendation_pool_codes", []),
        ("_compute_since_inception_annualized_return_map", {}),
        ("_load_since_inception_annual_return_breakdown_map", {}),
        ("_fetch_project_universe_fund_snapshot", {}),
        ("_load_tracker_style_metric_benchmark_map", {}),
        ("_build_style_metric_reference_map", {}),
        ("_favorite_query_history_snapshot", {"records": []}),
        ("_extract_favorite_style_threshold_payload", {"styles": [], "global": {}, "missing_styles": []}),
    ):
        monkeypatch.setattr(backend_app, name, lambda *args, _value=value, **kwargs: _value)
    monkeypatch.setattr(backend_app, "_fetch_favorite_fund_database_snapshot", lambda codes: {"000001": snapshot})
    monkeypatch.setattr(backend_app, "_load_favorite_fund_score_dataset", lambda payload: {
        "000001": {"基金代码": "000001", "最新净值日期": "2026-09-09", "1年-夏普比率": "0"},
    })
    response = backend_app._build_favorite_fund_scores_response(
        {}, ["000001"], save_history=False, include_details=include_details,
        include_recommendation_pool=False, include_autostock=False,
    )
    assert response["rows"] == response["funds"]
    assert response["rows"][0]["1年-夏普比率"] == ""
    assert response["rows"][0]["__risk_metric_unavailability"]["1年-夏普比率"]["reason_code"] == "zero_or_negligible_volatility"
    assert response["source_basis"]["autostock_auxiliary"]["upstream_requested_count"] == 0


def test_risk_audit_does_not_equate_null_cap_flag_with_false() -> None:
    from scripts.audit_data_truthfulness import _flag_metric_mismatch

    assert _flag_metric_mismatch(None, None) is None
    assert _flag_metric_mismatch(0, False) is None
    assert _flag_metric_mismatch(None, False)["reason"] == "missing_flag_side"
    assert _flag_metric_mismatch(False, None)["reason"] == "missing_flag_side"


def test_excel_preserves_blank_ratio_reason_and_numeric_zero() -> None:
    explanation = "窗口内波动率为零，夏普比率无法定义；数据截至 2026-09-09"
    stream, _, count = build_favorites_export_workbook({
        "schema_version": 1, "metadata": {}, "tables": [{
            "name": "隔离语义验证", "columns": [
                {"key": "基金代码", "label": "基金代码"},
                {"key": "1年-夏普比率", "label": "1年夏普"},
                {"key": "1年-年化收益(%)", "label": "1年年化收益"},
            ], "rows": [{"cells": [{"text": "000001"}, {"text": None, "comment": explanation}, {"text": "0"}]}],
        }],
    })
    sheet = load_workbook(stream).worksheets[0]
    assert count == 1 and sheet["A5"].value == "000001"
    assert sheet["B5"].value is None and sheet["B5"].comment.text == explanation
    assert sheet["C5"].value == 0 and sheet["C5"].data_type == "n"


def test_schema_upgrade_preserves_unknown_reason_on_old_rows(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE fund_risk_metrics (fcode TEXT, period TEXT, sharpe_ratio REAL)")
        connection.execute("INSERT INTO fund_risk_metrics VALUES ('000001','1y',0.0)")
    assert migrate_add_columns(str(path))
    assert migrate_add_columns(str(path))
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT sharpe_ratio, undefined_metric_reasons_json FROM fund_risk_metrics").fetchone()
        assert row == (0.0, None)
