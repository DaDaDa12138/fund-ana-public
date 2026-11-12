from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bond_fund_screening.core.risk_calculator import RiskCalculator


def test_weekly_observations_use_calendar_year_and_observed_frequency():
    dates = pd.date_range("2024-01-01", "2024-12-31", freq="7D")
    phase = np.linspace(0.0, 2.0 * np.pi, len(dates))
    accumulated = np.geomspace(1.0, 1.03, len(dates)) * np.exp(0.0005 * np.sin(phase))
    nav = pd.DataFrame({
        "nav_date": dates,
        "unit_nav": accumulated,
        "accum_nav": accumulated,
        "daily_growth": np.full(len(dates), np.nan),
    })

    result = RiskCalculator(risk_free_rate=0.0, mar=0.0).calculate_all_metrics(
        nav, period_years=1,
    )

    span_days = (dates[-1] - dates[0]).days
    total_return = accumulated[-1] / accumulated[0] - 1.0
    expected = np.expm1(np.log1p(total_return) * 365.25 / span_days)
    periods_per_year = result["formula_contract"]["annualization_factor"]
    expected_volatility = np.std(np.diff(accumulated) / accumulated[:-1], ddof=1) * np.sqrt(periods_per_year)
    assert periods_per_year < 100
    assert result["volatility"] == pytest.approx(expected_volatility, abs=5e-5)
    assert result["raw_sharpe_ratio"] == pytest.approx(expected / expected_volatility)
