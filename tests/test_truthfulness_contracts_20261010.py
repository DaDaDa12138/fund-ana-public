"""Real dividend regression plus synthetic, explicitly isolated contract fixtures."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.forward_validation import _evaluate_code_forward_metrics, FORWARD_RETURN_CONTRACT
from backend.lifetime_returns import load_platform_lifetime_returns, platform_annualized_map
from backend.publication import _resolve_publish_blockers
from backend.truthfulness_gate import publication_truthfulness_checks
from backend.valuation_contract import validate_valuation
from backend.value_contract import numeric_observation
from core.bond_fund_screening.core.metric_formula_contract import VERSION


def dividend_frame() -> pd.DataFrame:
    payload = json.loads((Path(__file__).parent / "fixtures/forward_dividend_100066_20261010.json").read_text())
    assert hashlib.sha256(json.dumps(payload["rows"], ensure_ascii=False, sort_keys=True).encode()).hexdigest() == payload["rows_sha256"]
    frame = pd.DataFrame(payload["rows"])
    frame["nav_date"] = pd.to_datetime(frame["nav_date"])
    return frame


def test_real_dividend_forward_metrics_use_provider_daily_returns() -> None:
    frame = dividend_frame()
    result = _evaluate_code_forward_metrics(frame, snapshot_date=datetime(2026, 7, 9), horizon_days=32)
    assert result["status"] == "ready"
    assert result["return_pct"] == pytest.approx(0.2403, abs=0.0001)
    assert result["unit_nav_price_change_pct"] == pytest.approx(-0.4722, abs=0.0001)
    returns = frame.daily_growth.iloc[1:].to_numpy() / 100
    wealth = np.concatenate(([1], np.cumprod(1 + returns)))
    assert result["max_drawdown_pct"] == pytest.approx(abs((wealth / np.maximum.accumulate(wealth) - 1).min()) * 100, abs=0.0001)
    assert result["volatility_pct"] == pytest.approx(np.std(returns, ddof=1) * np.sqrt(252) * 100, abs=0.0001)
    assert result["formula_version"] == FORWARD_RETURN_CONTRACT["version"]


@pytest.mark.parametrize("kind", ["null", "duplicate", "gap", "invalid", "unit_missing"])
def test_forward_input_boundaries_preserve_missing_semantics(kind: str) -> None:
    frame = dividend_frame()
    expected = "ready"
    if kind == "null":
        frame.loc[8, "daily_growth"] = None
        expected = "total_return_missing"
    elif kind == "duplicate":
        frame.loc[8, "nav_date"] = frame.loc[7, "nav_date"]
        expected = "duplicate_nav_dates"
    elif kind == "gap":
        frame = frame.drop(index=range(2, 15))
        expected = "history_gap"
    elif kind == "invalid":
        frame.loc[8, "daily_growth"] = -100
        expected = "invalid_total_return"
    else:
        frame["unit_nav"] = None
    result = _evaluate_code_forward_metrics(frame, snapshot_date=datetime(2026, 7, 9), horizon_days=32)
    assert result["status"] == expected
    if expected != "ready":
        assert "return_pct" not in result
    else:
        assert result["return_pct"] == pytest.approx(0.2403, abs=0.0001)
        assert result["unit_nav_price_change_pct"] is None


@pytest.mark.parametrize("field,value,status", [
    ("NAV", 0, "observed"), ("NAV", None, "provider_null"),
    ("NAV", "", "provider_null"), ("NAV", "bad", "parse_failed"),
    ("NAV", float("inf"), "parse_failed"), ("NAV", True, "parse_failed"),
])
def test_nullable_numeric_contract(field: str, value, status: str) -> None:
    result = numeric_observation({field: value}, field)
    assert result["status"] == status
    assert result["value"] == (0 if value == 0 else None)


def test_unrequested_and_absent_fields_are_distinct() -> None:
    assert numeric_observation({}, "NAV", requested=False)["status"] == "not_requested"
    assert numeric_observation({}, "NAV")["status"] == "field_absent"


def test_lifetime_return_uses_return_source_date_and_rejects_newer_nav_overlay(tmp_path: Path) -> None:
    db = tmp_path / "dated.db"
    with sqlite3.connect(db) as conn:
        conn.executescript("""CREATE TABLE funds(fcode TEXT, estab_date TEXT);
            CREATE TABLE fund_returns(fcode TEXT, latest_nav_date TEXT, return_since REAL,
                period_return_date TEXT, period_return_observed_at TEXT,
                period_return_source TEXT, period_return_source_sha256 TEXT);
            INSERT INTO funds VALUES('000001','2020-01-01');
            INSERT INTO fund_returns VALUES('000001','2026-10-09',50,'2026-10-08',
                '2026-10-09T10:00:00','fixture-provider','fixture-sha');""")
    item = load_platform_lifetime_returns(db, ["000001"])["000001"]
    assert item["status"] == "stale" and item["value"] is None
    assert item["end_date"] == "2026-10-08"
    assert item["response_sha256"] == "fixture-sha"
    assert platform_annualized_map(db, ["000001"]) == {}


def estimate_row() -> dict:
    return {"FCODE": "000001", "GSZ": 1.001, "GSZZL": 0.1, "NAV": 1.0,
            "GZTIME": "2026-10-09 10:00", "PDATE": "2026-10-08"}


@pytest.mark.parametrize("change,reason", [
    ({"GZTIME": "2026-10-08 10:00"}, "valuation_stale"),
    ({"GZTIME": None}, "valuation_time_invalid"),
    ({"GZTIME": "2026-10-09 11:00"}, "valuation_time_future"),
    ({"PDATE": "2026-10-09"}, "valuation_base_date_mismatch"),
    ({"GSZZL": 10}, "valuation_base_nav_mismatch"),
    ({"GSZ": "bad"}, "valuation_parse_failed"),
    ({"GSZZL": None}, "valuation_fields_incomplete"),
    ({"GSZ": None, "GSZZL": None}, "valuation_not_provided"),
    ({"NAV": 0}, "valuation_values_invalid"),
])
def test_estimate_rejects_stale_partial_or_unverified_baseline(change: dict, reason: str) -> None:
    result = validate_valuation({**estimate_row(), **change}, now=datetime(2026, 10, 9, 10, 5))
    assert result["available"] is False
    assert result["reason"] == reason
    assert result["raw_dates"]["PDATE"] == change.get("PDATE", "2026-10-08")


def test_zero_estimated_change_is_valid_and_weekend_estimate_is_stale() -> None:
    row = {**estimate_row(), "GSZ": 1.0, "GSZZL": 0}
    result = validate_valuation(row, now=datetime(2026, 10, 9, 10, 5))
    assert result["available"] is True
    assert result["field_observations"]["GSZZL"]["value"] == 0
    assert validate_valuation(row, now=datetime(2026, 10, 10, 10, 5))["reason"] == "valuation_stale"


def test_platform_lifetime_and_accum_nav_ratio_are_separate(tmp_path: Path, monkeypatch) -> None:
    from backend.report_builder import _compute_report_accum_nav_ratio_annualized_return_map
    db = tmp_path / "fixture.db"
    with sqlite3.connect(db) as conn:
        conn.executescript("""CREATE TABLE funds(fcode TEXT, estab_date TEXT);
            CREATE TABLE fund_returns(fcode TEXT, latest_nav_date TEXT, return_since REAL);
            CREATE TABLE fund_nav_history(fcode TEXT, nav_date TEXT, unit_nav REAL, accum_nav REAL);""")
        conn.execute("INSERT INTO funds VALUES('000001','2020-01-01')")
        conn.execute("INSERT INTO fund_returns VALUES('000001','2026-01-01',50)")
        conn.executemany("INSERT INTO fund_nav_history VALUES(?,?,?,?)", [
            ("000001", "2020-01-01", 1.0, 1.0), ("000001", "2026-01-01", 1.1, 1.3)])
    settings = SimpleNamespace(database_path=db, project_root=tmp_path)
    platform = platform_annualized_map(db, ["000001"])
    local = _compute_report_accum_nav_ratio_annualized_return_map(settings, ["000001"])
    assert platform["000001"] != local["000001"]
    observations = load_platform_lifetime_returns(db, ["000001"])
    assert observations["000001"]["raw_return_pct"] == 50
    assert observations["000001"]["upstream_field"] == "SYL_LN"
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE fund_returns SET return_since=NULL")
    assert platform_annualized_map(db, ["000001"]) == {}
    assert _compute_report_accum_nav_ratio_annualized_return_map(settings, ["000001"]) == local
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE fund_nav_history SET accum_nav=NULL")
    assert _compute_report_accum_nav_ratio_annualized_return_map(settings, ["000001"]) == {}


@pytest.mark.parametrize("field", ["holding_days", "scale_billion", "establish_days"])
@pytest.mark.parametrize("missing", [None, "", "bad", -1, float("inf")])
def test_unknown_liquidity_input_is_unscorable(field: str, missing) -> None:
    from core.bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine
    fixture = {"holding_days": 0, "scale_billion": 5, "establish_days": 1500,
               "annual_return": 3, "positive_ratio": 70, "sharpe_ratio": 2,
               "sortino_ratio": 3, "max_drawdown": .01, "var_95": -.001,
               "volatility": .02, "calmar_ratio": 2}
    result = EnhancedScoringEngine().calculate_scores(pd.DataFrame([{**fixture, field: missing}])).iloc[0]
    assert pd.isna(result.liquidity_score)
    assert pd.isna(result.comprehensive_score)
    assert result.grade == "不可评分"
    assert result.score_status == "missing_input"


def publication_fixture() -> tuple[dict, dict]:
    summary = {"generated_at": "2026-10-10T14:00:00",
               "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-10-09"}}}
    inputs = {"official_rows": [{"基金代码": "000001", "最新单位净值": 1, "最新累计净值": 1,
               "1年-年化收益(%)": 0, "1年-最大回撤(%)": 0, "1年-波动率": 0}],
              "fund_metadata_rows": [{"fcode": "000001", "holding_days": 0}],
              "risk_snapshot_rows": [{"fcode": "000001", "period": "1y", "end_date": "2026-10-09",
                  "formula_contract_json": json.dumps({"version": VERSION, "engine": "database",
                       "return_source": "daily_growth", "return_observations_verified": True})}]}
    return summary, inputs


@pytest.mark.parametrize("kind", ["old_date", "holding_null", "formula_null", "history_gap", "critical_null"])
def test_new_publication_input_gate_fails_closed(kind: str) -> None:
    summary, inputs = publication_fixture()
    assert all(check["aligned"] for check in publication_truthfulness_checks(summary, inputs))
    if kind == "old_date":
        summary["database_freshness"]["effective_nav_snapshot"]["candidate_date"] = "2026-09-29"
    elif kind == "holding_null":
        inputs["fund_metadata_rows"][0]["holding_days"] = None
    elif kind == "formula_null":
        inputs["risk_snapshot_rows"][0]["formula_contract_json"] = None
    elif kind == "history_gap":
        inputs["risk_snapshot_rows"][0]["formula_contract_json"] = json.dumps({"version": VERSION,
            "engine": "database", "return_source": "daily_growth", "return_observations_verified": False})
    else:
        inputs["official_rows"][0]["1年-波动率"] = None
    assert any(not check["aligned"] for check in publication_truthfulness_checks(summary, inputs))


def test_required_truthfulness_checks_cannot_be_omitted() -> None:
    summary, _ = publication_fixture()
    summary["counts"] = {"final_count": 1}
    summary["publication_control"] = {"truthfulness_checks_required": True}
    assert "publication_truthfulness_unverified" in {b["code"] for b in _resolve_publish_blockers(summary)}


def test_actual_formula_parameters_are_persisted_in_risk_rows() -> None:
    from analyzers.risk_analyzer import RiskAnalyzer
    dates = pd.date_range("2025-01-01", periods=366, freq="D")
    returns = np.resize(np.array([.0001, -.00005, .0002]), 365)
    nav = np.concatenate(([1], np.cumprod(1 + returns)))
    frame = pd.DataFrame({"nav": nav, "accum_nav": nav,
                          "daily_growth": np.concatenate(([np.nan], returns * 100))}, index=dates)
    analyzer = RiskAnalyzer(config={"risk_metrics": {"min_data_points": 30, "risk_free_rate": .013}}, db_manager=object())
    metrics = analyzer._calculate_period_metrics(frame, "1y")
    row = analyzer._metric_write_record("000001", "1y", metrics, datetime(2026, 1, 2))
    contract = json.loads(row["formula_contract_json"])
    assert contract["version"] == VERSION
    assert contract["risk_free_rate"] == .013
    assert contract["return_observations_verified"] is True
    assert contract["standard_metric_claim"] is False


@pytest.mark.parametrize("schema_version", [None, 1, 2, 3])
def test_legacy_lifecycle_derived_values_are_not_relabelled_as_platform(
    tmp_path: Path, monkeypatch, schema_version: int | None,
) -> None:
    from backend import app as api
    path = tmp_path / "tracker.json"
    path.write_text(json.dumps({"run_id": "fixture", "generated_at": "2026-10-09T12:00:00", "derived_fields": {
        "schema_version": schema_version, "annualized_return_by_code": {"000001": 0}}}))
    monkeypatch.setattr(api, "_resolve_existing_report_artifact_path", lambda _: path)
    monkeypatch.setattr(api, "_load_cached_style_metric_benchmark_map", lambda *a, **k: {})
    monkeypatch.setattr(api, "_compute_since_inception_annualized_return_map", lambda _: pytest.fail("must not read later database"))
    fields = api._build_tracker_current_detail_derived_fields({"run_id": "fixture", "generated_at": "2026-10-09T12:00:00",
        "publication_control": {"commit_protocol": "fixture"}, "artifacts": {"project_tracker_current_detail_json": str(path)}},
        [{"基金代码": "000001"}], style_threshold_payload={})
    assert fields["annualized_return_by_code"] == ({"000001": 0} if schema_version == 2 else {})
    assert fields["accum_nav_ratio_annualized_by_code"] == {}
    row = {"基金代码": "000001"}
    api._apply_tracker_current_derived_fields_to_rows({"000001": row}, ["000001"], fields)
    assert row["平台成立以来收益年化(%)"] == ("0" if schema_version == 2 else "")
    assert row["成立以来收益口径"] == {"status": "legacy_definition_unknown"}


def test_recommendation_export_projects_both_lifetime_definitions() -> None:
    from backend import app as api
    rows = api._recommendation_export_rows([{"基金代码": "000001",
        "平台成立以来收益年化(%)": 0, "累计净值比年化(%)": 5.32}])
    assert float(rows.iloc[0]["平台成立以来收益年化(%)"]) == 0
    assert float(rows.iloc[0]["累计净值比年化(%)"]) == 5.32


@pytest.mark.parametrize("platform", [None, 0])
def test_committed_recommendation_workbook_keeps_frozen_lifetime_inputs(platform, monkeypatch) -> None:
    from backend import app as api
    from openpyxl import load_workbook
    fund = {"基金代码": "000001", "基金名称": "隔离测试基金",
        "平台成立以来收益年化(%)": platform, "累计净值比年化(%)": 5.32}
    portfolio = {"fund_count": 1, "group_count": 1, "funds": [fund],
        "groups": [{"name": "隔离测试分组", "count": 1, "funds": [fund]}]}
    monkeypatch.setattr(api, "recommendations_display_report", lambda _settings, payload: payload)
    monkeypatch.setattr(api, "_build_recommendations_detail_response",
        lambda payload, **kwargs: {"portfolio_suggestions": deepcopy(portfolio)})
    monkeypatch.setattr(api, "_load_tracker_occurrence_map", lambda _: {})
    monkeypatch.setattr(api, "_apply_since_inception_annualized_return_to_portfolio_summary",
        lambda _: pytest.fail("committed export must not recompute against later database"))
    buffer, _ = api._build_recommendations_export_workbook({"run_id": "fixture",
        "publication_control": {"commit_protocol": 1}, "project_publish_summary": {}})
    workbook = load_workbook(buffer, read_only=True, data_only=True)
    sheet = workbook["全部推荐基金"]
    values = list(sheet.values)
    row = dict(zip(values[0], values[1]))
    if platform is None:
        assert row["平台成立以来收益年化(%)"] is None
    else:
        assert float(row["平台成立以来收益年化(%)"]) == platform
    assert float(row["累计净值比年化(%)"]) == 5.32
    assert "成立以来年化收益率(%)" not in values[0]
    workbook.close()
