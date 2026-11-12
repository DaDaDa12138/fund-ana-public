"""Real directory input plus explicit missing-state fixtures; isolated SQLite only."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))

from collectors.fund_collector import FundCollector
from collectors.nav_collector import NavCollector
from database.connection import DatabaseManager
from database.models import Fund, FundReturns, FundNavHistory, FundRiskMetrics
from core.utils.buyability import normalize_buyability
from backend import autostock_nav_tail_fill as tail_fill
from backend.bond_universe_sync import _save_missing_records
from backend.future_outlier_monitor import _load_monitor_snapshot
from backend.long_term_history_maintenance import _classify_long_term_gap_blocker
from backend.plain_open_tail_follow_up import load_current_returns_snapshot
from backend.project_publish_decisions import append_project_decision_columns, apply_publish_post_filters
from backend.project_quality_thresholds import apply_fixed_quality_thresholds, apply_dynamic_quality_thresholds
from backend.project_report_support import build_stage_b_reason_ledger
from backend.report_builder import _read_metadata
from backend.report_workbook import _write_multisheet_workbook
from backend.risk_coverage_gap_audit import _classify_non_candidate_tail_sample

FIXTURE = json.loads((ROOT / "tests/fixtures/buyability_directory_20260915.json").read_text())
DIRECTORY_ROW = FIXTURE["directory_rows"][0]
CODE, _, NAME, FUND_TYPE, _ = DIRECTORY_ROW
FORMAL = json.loads((ROOT / "tests/fixtures/buyability_formal_20260915.json").read_text())


@pytest.fixture
def database(tmp_path):
    manager = DatabaseManager(str(tmp_path / "buyability.sqlite"))
    for model in (Fund, FundReturns, FundNavHistory, FundRiskMetrics):
        model.__table__.create(manager.engine)
    yield manager
    manager.engine.dispose()


def test_directory_without_purchase_flag_is_not_inserted_as_buyable(database):
    collector = FundCollector(config={}, db_manager=database, api_client=SimpleNamespace())
    result = _save_missing_records(
        SimpleNamespace(fund_collector=collector), [], [CODE],
        {CODE: {"name": NAME, "type": FUND_TYPE}},
    )
    assert result["fund_inserts"] == 1 and result["failed_records"] == 0
    with database.get_session(readonly=True) as session:
        fund = session.get(Fund, CODE)
        assert fund.shortname == NAME
        assert fund.data_source == "autostock_directory_fallback"
        assert fund.is_buyable is None
        assert fund.purchase_status is None


@pytest.mark.parametrize("values", [{}, {"is_buyable": None}])
def test_missing_and_explicit_null_model_values_remain_null(database, values):
    with database.get_session() as session:
        session.add(Fund(fcode=CODE, shortname=NAME, **values))
    with database.get_session(readonly=True) as session:
        assert session.get(Fund, CODE).is_buyable is None


@pytest.mark.parametrize("flag", [None, True, False])
def test_metadata_preserves_unknown_and_both_known_states(database, flag):
    with database.get_session() as session:
        session.add(Fund(fcode=CODE, shortname=NAME))
    # Explicit isolated field-state fixture, not a claim about this fund.
    with database.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE funds SET is_buyable=?", (flag,))
    value = _read_metadata(Path(database.db_path)).iloc[0]["is_buyable"]
    if flag is None:
        assert pd.isna(value)
    else:
        assert value == flag


@pytest.mark.parametrize("flag,label,reason,investment", [
    (None, "申购状态未知", "buyability_unknown", "待确认"),
    (False, "不可申购", "not_buyable", "不可投资"),
    (True, "可申购", None, "可投资"),
])
def test_decision_gate_reports_missing_separately_from_suspension(flag, label, reason, investment):
    # Other missing columns intentionally exercise the real incomplete-data gate.
    result = append_project_decision_columns(pd.DataFrame([
        {"fcode": CODE, "shortname": NAME, "ftype": FUND_TYPE, "is_buyable": flag}
    ]), decision_map={}, soft_warning_map={}).iloc[0]
    assert result["是否可申购"] == label
    assert result["可投资性状态"] == investment
    reasons = result["基础推荐原因代码"].split("|")
    if reason:
        assert reason in reasons and not result["基础门禁通过"]
    if flag is None:
        assert "not_buyable" not in reasons
        assert result["数据就绪状态"] == "待补齐"
        assert result["质量结论状态"] == "待评估"
        assert "不可申购" not in result["推荐备注"]


@pytest.mark.parametrize("flag", [True, False])
def test_sparse_directory_update_keeps_an_explicit_existing_flag(database, flag):
    with database.get_session() as session:
        session.add(Fund(fcode=CODE, shortname=NAME, is_buyable=flag))
    collector = FundCollector(config={}, db_manager=database, api_client=SimpleNamespace())
    result = collector._save_funds([{"fcode": CODE, "shortname": NAME, "is_buyable": None}], return_stats=True)
    assert result["fund_updates"] == 1
    with database.get_session(readonly=True) as session:
        assert session.get(Fund, CODE).is_buyable is flag


@pytest.mark.parametrize("value,expected", [
    (True, True), (False, False), (1, True), (0, False), (1.0, True), (0.0, False),
    ("true", True), ("False", False), ("1", True), ("0", False),
    (None, None), ("--", None), ("", None), (1.9, None), (-1, None),
    (float("nan"), None), (float("inf"), None), (pd.NA, None),
])
def test_invalid_flags_do_not_become_purchase_decisions(value, expected):
    assert normalize_buyability(value) is expected
    assert FundCollector._parse_buyable_flag({"BUY": value}) is expected


def test_explicit_false_is_not_overridden_by_secondary_true_flag():
    assert FundCollector._parse_buyable_flag({"BUY": False, "ISBUY": True}) is False
    assert FundCollector._parse_buyable_flag({"BUY": None, "ISBUY": True}) is True


@pytest.mark.parametrize("flag,expected_reason", [(True, ""), (False, "not_buyable"), (None, "buyability_unknown")])
def test_only_missing_flag_blocks_an_otherwise_ready_real_frozen_input(flag, expected_reason):
    source = dict(FORMAL["frozen_row"], is_buyable=flag)
    code = source["fcode"]
    result = append_project_decision_columns(
        pd.DataFrame([source]), decision_map={code: FORMAL["decision"]},
        soft_warning_map={code: FORMAL["soft_warning"]},
        evaluated_at=datetime.fromisoformat(FORMAL["evaluated_at"]).replace(tzinfo=None),
    ).iloc[0]
    assert result["基础推荐原因代码"] == expected_reason
    assert bool(result["基础门禁通过"]) is (flag is True)


@pytest.mark.parametrize("mode", ["fixed", "dynamic"])
def test_unknown_gate_survives_quality_filters_ledger_and_excel(tmp_path, mode):
    from openpyxl import load_workbook

    source = dict(FORMAL["frozen_row"], is_buyable=None)
    code = source["fcode"]
    clock = datetime.fromisoformat(FORMAL["evaluated_at"]).replace(tzinfo=None)
    base = append_project_decision_columns(pd.DataFrame([source]), decision_map={code: FORMAL["decision"]},
                                          soft_warning_map={code: FORMAL["soft_warning"]}, evaluated_at=clock)
    thresholds = {key: FORMAL[key] for key in ("style_thresholds", "global_thresholds")}
    quality = (apply_fixed_quality_thresholds(base) if mode == "fixed" else
               apply_dynamic_quality_thresholds(base, **thresholds, style_meta=FORMAL["style_meta"]))
    result = apply_publish_post_filters(quality, threshold_mode=mode, **thresholds, evaluated_at=clock)
    row = result.iloc[0]
    assert row["项目推荐结论"] != "正式推荐"
    assert row["可投资性状态"] == "待确认"
    assert row["数据就绪状态"] == "待补齐"
    assert row["质量结论状态"] == "待评估"
    ledger, _ = build_stage_b_reason_ledger(candidate_df=result, excluded_df=pd.DataFrame(),
                                           official_codes=set(), normalize_code=lambda v: str(v or ""))
    assert ledger.iloc[0]["主排除原因代码"] == "buyability_unknown"
    assert ledger.iloc[0]["是否可申购"] == "申购状态未知"
    output = tmp_path / "unknown.xlsx"
    _write_multisheet_workbook(output, main_sheet_name="未入榜原因", main_df=ledger)
    workbook = load_workbook(output, read_only=True)
    try:
        header, values = list(workbook.active.values)[:2]
        assert dict(zip(header, values))["是否可申购"] == "申购状态未知"
    finally:
        workbook.close()


@pytest.mark.parametrize("flag", [None, True, False])
def test_refresh_selection_and_diagnostic_readers_preserve_state(database, monkeypatch, flag):
    with database.get_session() as session:
        session.add(Fund(fcode=CODE, shortname=NAME, is_buyable=flag))
    settings = SimpleNamespace(database_path=Path(database.db_path))
    assert load_current_returns_snapshot(settings, [CODE])[CODE]["is_buyable"] is flag
    assert _load_monitor_snapshot(settings, [CODE])[0]["is_buyable"] is flag
    assert tail_fill._load_fund_candidate_metadata(settings.database_path, codes=[CODE])[CODE]["is_buyable"] is flag
    nav = object.__new__(NavCollector)
    nav.db_manager = database
    assert (CODE in nav._get_all_funds(only_active=True)) is (flag is not False)

    monkeypatch.setattr(tail_fill, "_load_latest_missing_response_observation_rows", lambda _: (None, [
        {"fcode": CODE, "missing_business_days": 1, "last_review_date": "2026-09-14"}
    ]))
    deferred = tail_fill._read_low_priority_missing_response_retry_deferrals(
        settings.database_path, business_date=date(2026, 9, 15), publish_code_sets={"available": True},
    )
    assert (CODE in deferred) is (flag is False)


@pytest.mark.parametrize("flag,reason", [(None, "buyability_unknown"), (False, "not_buyable"), (True, "none")])
def test_long_term_diagnostic_does_not_label_unknown_as_suspended(flag, reason):
    record = {"buyable": flag, "active_universe": True, "fresh_nav": True, "risk_ready": True,
              "screenable_universe": True}
    assert _classify_long_term_gap_blocker(record) == reason


@pytest.mark.parametrize("flag,category", [
    (None, "plain_tail_buyability_unknown"),
    (False, "plain_open_tail_paused_monitor"),
    (True, "plain_open_tail_follow_up"),
])
def test_risk_tail_diagnostic_distinguishes_unknown_from_suspension(flag, category):
    assert _classify_non_candidate_tail_sample(
        latest_nav_date="2026-09-14", latest_risk_end_date="2026-09-11",
        effective_candidate_date="2026-09-14", effective_end_date="2026-09-14",
        shortname=NAME, has_holding_period=False, holding_days=0, is_buyable=flag, purchase_status="",
    ) == category
