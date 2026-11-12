"""Official closures and real NAV fixtures; no production paths or network."""
from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core.bond_fund_screening.core.trading_calendar import expected_open_dates, continuity_evidence
from core.bond_fund_screening.core.filtering_engine import FilteringEngine
from analyzers.risk_analyzer import RiskAnalyzer
from core.bond_fund_screening.core.metric_formula_contract import VERSION


def test_public_formula_contract_uses_actual_strategy_formula_version():
    from backend.risk_metric_contract import risk_metric_formula_contract

    assert risk_metric_formula_contract()["formula_version"] == VERSION


def test_sse_proven_spring_festival_gap_is_not_missing_returns():
    result = continuity_evidence(['2026-02-13', '2026-02-24'])
    assert result['verified'] is True
    assert result['max_raw_gap_days'] == 11
    assert result['proven_closed_long_gap_count'] == 1
    assert result['missing_open_day_count'] == 0
    assert result['calendar']['response_sha256'] == '7ea10c16202c4748f0ad5bcdfe6ee07b85b5462589504604b17c9d63865e7d8c'


@pytest.mark.parametrize('dates', [
    ['2026-02-12', '2026-02-24'],  # Feb13 was an open Friday.
    ['2026-02-13', '2026-02-25'],  # Feb24 reopened; its return is missing.
    ['2026-03-02', '2026-03-04'],  # A short, real business-day gap.
    ['2020-02-01', '2020-02-20'],  # Unverified annual calendar.
])
def test_missing_trading_days_or_unknown_long_holidays_never_verify(dates):
    assert continuity_evidence(dates)['verified'] is False


def test_unknown_calendar_is_explicit_and_cannot_change_point_denominator():
    assert expected_open_dates('2020-09-13', '2020-10-13') is None
    result = continuity_evidence(['2020-10-09', '2020-10-12'])
    assert result['uncovered_gap_count'] == 1
    assert result['uncovered_date_policy'].endswith('calendar_not_verified')


def real_frame():
    payload = json.loads((Path(__file__).parent/'fixtures/nav_100066_short_20261013.json').read_text())
    assert hashlib.sha256(json.dumps(payload['rows'],sort_keys=True,ensure_ascii=False).encode()).hexdigest() == payload['rows_sha256']
    frame = pd.DataFrame(payload['rows'])
    frame['nav_date'] = pd.to_datetime(frame['nav_date'])
    return frame


def test_real_month_window_passes_with_same_returns_and_calendar_denominator():
    frame = real_frame()
    engine = FilteringEngine(period_years=30/365, data_loader=SimpleNamespace(), enable_tracking=False, verbose=False)
    _, metrics, meta = engine._calculate_layer6_single_fund('100066', frame, ['volatility'],window_end_date=date(2026,10,13))
    assert engine._get_layer6_min_nav_points() == 17  # Original annual-average denominator.
    assert meta['status'] == 'OK' and metrics is not None
    assert meta['expected_open_day_count'] == 16
    assert meta['required_nav_points'] == 14
    assert meta['nav_count'] == 16
    assert meta['observation_coverage_ratio_required'] == .8
    selected, _ = engine._prepare_layer6_nav_window(frame, date(2026,10,13))
    assert len(selected) == meta['nav_count']
    pd.testing.assert_frame_equal(selected,frame[frame.nav_date >= '2026-09-11'])


def test_actual_missing_monthly_open_day_remains_blocked():
    frame = real_frame()
    frame = frame[frame.nav_date != '2026-09-21']
    engine = FilteringEngine(period_years=30/365,data_loader=SimpleNamespace(), enable_tracking=False,verbose=False)
    _, metrics, meta = engine._calculate_layer6_single_fund('100066',frame,['volatility'],window_end_date=date(2026,10,13))
    assert metrics is None and meta['status'] == 'NAV_DATA_GAP'
    assert meta['calendar_continuity']['missing_open_day_samples'] == ['2026-09-21']


def test_sparse_monthly_observations_do_not_become_daily():
    engine = FilteringEngine(period_years=30/365,data_loader=SimpleNamespace(), enable_tracking=False,verbose=False)
    _, metrics, meta = engine._calculate_layer6_single_fund('100066',real_frame().iloc[::2],['volatility'],window_end_date=date(2026,10,13))
    assert metrics is None and meta['status'] in {'INSUFFICIENT_NAV_POINTS','LOW_FREQUENCY_NAV_OBSERVATIONS'}


def test_database_contract_records_real_holiday_reason_and_preserves_numeric_formula():
    # Dates come from the sourced annual calendar; numbers are isolated synthetic fixtures.
    dates = [date(2026,1,5), *expected_open_dates('2026-01-05','2026-12-31')]
    values = [1 + i*.0001 for i in range(len(dates))]
    frame = pd.DataFrame({'nav':values,'accum_nav':values,'daily_growth':[None]+[.01]*(len(dates)-1)},index=pd.to_datetime(dates))
    analyzer = RiskAnalyzer(config={'risk_metrics':{'min_data_points':30}},db_manager=object())
    metrics = analyzer._calculate_period_metrics(frame,'1y')
    contract = metrics['formula_contract']
    assert contract['version'] == VERSION
    assert contract['return_observations_verified'] is True
    assert contract['date_continuity']['proven_closed_long_gap_count'] == 1
    before = metrics['annual_return'],metrics['max_drawdown'],metrics['volatility']
    # Removing a real open-day return must fail verification, never synthesize it.
    missing = frame.drop(pd.Timestamp('2026-02-13'))
    altered = analyzer._calculate_period_metrics(missing,'1y')
    assert altered['formula_contract']['return_observations_verified'] is False
    assert before[0] is not None


def test_changed_nav_scan_invalidates_old_formula_without_changing_nav(tmp_path):
    import sqlite3
    from cli.aux_commands import _resolve_changed_nav_risk_candidates
    from utils import build_nav_snapshot_signature
    db = tmp_path/'isolated_contract_scan.db'
    snapshot = {'latest_nav_date':'2026-10-12','latest_nav':1,'period_return_date':'2026-10-12'}
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE funds (fcode TEXT, has_holding_period INTEGER, holding_days INTEGER)')
        conn.execute('CREATE TABLE fund_returns (fcode TEXT, latest_nav_date TEXT, latest_nav REAL, period_return_date TEXT)')
        conn.execute('CREATE TABLE fund_nav_history (fcode TEXT, nav_date TEXT)')
        conn.execute('CREATE TABLE fund_risk_metrics (fcode TEXT, period TEXT, end_date TEXT, nav_snapshot_signature TEXT, max_drawdown_duration INTEGER, formula_contract_json TEXT)')
        conn.execute("INSERT INTO funds VALUES ('000001',0,0)")
        conn.execute("INSERT INTO fund_returns VALUES ('000001','2026-10-12',1,'2026-10-12')")
        conn.executemany('INSERT INTO fund_nav_history VALUES (?,?)', [('000001',d.date().isoformat()) for d in pd.bdate_range('2025-10-13','2026-10-12')])
        conn.execute('INSERT INTO fund_risk_metrics VALUES (?,?,?,?,?,?)',('000001','1y','2026-10-12',build_nav_snapshot_signature(snapshot),1,json.dumps({'version':'strategy-risk-2026-10-10.1'})))
    config = {'risk_metrics': {'periods':['1y'],'min_data_points':30}}
    assert _resolve_changed_nav_risk_candidates(db,config) == ['000001']
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE fund_risk_metrics SET formula_contract_json=?',(json.dumps({'version':VERSION}),))
    assert _resolve_changed_nav_risk_candidates(db,config) == []
