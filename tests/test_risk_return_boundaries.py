"""Real frozen windows must retain their first in-window return, including zero."""
from __future__ import annotations

import gzip
import json
import math
from pathlib import Path
import sys

import pandas as pd
import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'core'))
from analyzers.risk_analyzer import RiskAnalyzer
from bond_fund_screening.core.risk_calculator import RiskCalculator
from backend.calculation_input_audit import independent_standard_metrics


FIXTURE = json.loads(gzip.decompress(
    (ROOT / 'tests/fixtures/risk_window_first_growth_missing_20260912.json.gz').read_bytes()))
CASES = [(snapshot['source']['label'], snapshot['parameters'], case)
         for snapshot in FIXTURE['snapshots'] for case in snapshot['cases']]
CASE_IDS = [f"{label}-{case['fcode']}" for label, _, case in CASES]


@pytest.mark.parametrize('label,parameters,case', CASES, ids=CASE_IDS)
def test_persisted_risk_retains_first_valid_return_from_real_window(label, parameters, case) -> None:
    frame = pd.DataFrame({'daily_growth': case['daily_growth']}, index=pd.to_datetime(case['dates']))
    original = frame.copy(deep=True)
    calculator = RiskAnalyzer(config={'risk_metrics': {
        'risk_free_rate': parameters['risk_free_rate'], 'mar': parameters['mar'],
        'trading_days_per_year': parameters['trading_days'], 'min_data_points': 30,
    }}, db_manager=object())
    result = calculator._calculate_period_metrics(frame, '1y',
        api_return=case['api_return'], api_return_date=case['api_return_date'], api_snapshot_date=case['dates'][-1])
    # This historical fixture contains growth only. Interior missing returns
    # cannot be bridged by dropping rows or inventing a NAV fallback.
    if not np.isfinite(frame['daily_growth'].iloc[1:]).all():
        assert result is None
        pd.testing.assert_frame_equal(frame, original)
        return
    assert result is not None
    expected = case['expected_correct_persisted']
    assert result['data_points'] == expected['data_points']
    # This older fixture predates duration expectations. Derive spells from
    # its real returns by grouping underwater observations between peak hits.
    wealth = pd.concat([pd.Series([1.0]), (1 + frame['daily_growth'].iloc[1:].dropna() / 100).cumprod()], ignore_index=True)
    underwater = wealth.lt(wealth.cummax())
    expected_duration = int(underwater.groupby((~underwater).cumsum()).sum().max())
    assert result['max_drawdown_duration'] == expected_duration
    returns = frame['daily_growth'].iloc[1:] / 100
    frequency = min(parameters['trading_days'], len(returns) / ((frame.index[-1] - frame.index[0]).days / 365.25))
    current = independent_standard_metrics(returns, wealth, annual_return=expected['annual_return'] / 100,
        periods_per_year=frequency, risk_free_rate=parameters['risk_free_rate'], mar=parameters['mar'])
    expected = {**expected, **{key: current[key] for key in ('raw_sortino_ratio', 'sortino_ratio')}}
    for key in ('volatility', 'max_drawdown', 'annual_return', 'raw_sharpe_ratio', 'raw_sortino_ratio',
                'sharpe_ratio', 'sortino_ratio', 'calmar_ratio', 'var_95', 'var_99', 'positive_ratio'):
        if expected[key] is None:
            assert result[key] is None
        else:
            assert result[key] == pytest.approx(expected[key], rel=1e-10, abs=1e-12), key
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize('label,parameters,case', CASES, ids=CASE_IDS)
def test_screening_risk_uses_same_real_window_boundary(label, parameters, case) -> None:
    frame = pd.DataFrame({'nav_date': pd.to_datetime(case['dates']), 'daily_growth': case['daily_growth']})
    original = frame.copy(deep=True)
    calculator = RiskCalculator(risk_free_rate=parameters['risk_free_rate'], mar=parameters['mar'],
                                trading_days_per_year=parameters['trading_days'])
    result = calculator.calculate_all_metrics(frame, period_years=1.0)
    growth = frame['daily_growth'].iloc[1:]
    if not np.isfinite(growth).all():
        for key in ('volatility', 'max_drawdown', 'raw_sharpe_ratio', 'raw_sortino_ratio', 'calmar_ratio'):
            assert result[key] is None
        pd.testing.assert_frame_equal(frame, original)
        return
    returns = growth / 100
    span = (frame['nav_date'].iloc[-1] - frame['nav_date'].iloc[0]).days
    annual = math.expm1(math.fsum(math.log1p(value) for value in returns) * 365.25 / span)
    frequency = min(parameters['trading_days'], len(returns) / (span / 365.25))
    wealth = np.r_[1., (1 + returns).cumprod()]
    expected = independent_standard_metrics(returns, wealth, annual_return=annual, periods_per_year=frequency,
        risk_free_rate=parameters['risk_free_rate'], mar=parameters['mar'])
    for key in ('volatility', 'max_drawdown', 'raw_sharpe_ratio', 'raw_sortino_ratio', 'sharpe_ratio', 'sortino_ratio'):
        assert result[key] == (None if expected[key] is None else pytest.approx(round(expected[key], 4), abs=1e-12)), key
    assert result['formula_contract']['return_source'] == 'daily_growth'
    assert result['formula_contract']['annualization_factor'] == pytest.approx(frequency)
    pd.testing.assert_frame_equal(frame, original)
