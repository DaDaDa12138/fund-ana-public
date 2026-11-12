import numpy as np
import pandas as pd
import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))

from analyzers.risk_analyzer import RiskAnalyzer
from bond_fund_screening.core.risk_calculator import RiskCalculator
from bond_fund_screening.core.standard_risk_metrics import (
    calculate_standard_risk_metrics,
    return_observation_diagnostics,
)


def test_standard_core_preserves_non_computable_semantics() -> None:
    result = calculate_standard_risk_metrics(
        np.array([0.0, 0.0]), np.array([1.0, 1.0, 1.0]),
        annual_return=0.0, periods_per_year=252, risk_free_rate=0.02, mar=0.0,
    )
    assert result["volatility"] == 0.0
    assert result["max_drawdown"] == 0.0
    assert result["sharpe_ratio"] is None
    assert result["sortino_ratio"] is None


def test_standard_core_all_positive_returns_keeps_sortino_undefined() -> None:
    result = calculate_standard_risk_metrics(
        np.array([0.001, 0.002, 0.0015]),
        np.array([1.0, 1.001, 1.003002, 1.004506503]),
        annual_return=0.4, periods_per_year=252, risk_free_rate=0.02, mar=0.0,
    )
    assert result["raw_sortino_ratio"] is None
    assert result["sortino_ratio"] is None
    assert result["sortino_ratio_cap_hit"] is None
    assert result["undefined_metric_reasons"]["sortino_ratio"] == "zero_or_negligible_downside_deviation"
    calculator = RiskCalculator(mar=0.)
    assert calculator._calculate_sortino_from_values(np.array([.001, .002, .0015]), .4, .0005) is None


def test_non_negative_diagnostics_keep_zero_separate_from_positive_returns() -> None:
    diagnostics = return_observation_diagnostics(np.array([0.01, 0.0, -0.01]))

    assert diagnostics["non_negative_ratio"] == pytest.approx(200 / 3)
    assert diagnostics["positive_day_count"] == 1
    assert diagnostics["zero_day_count"] == 1
    assert diagnostics["negative_day_count"] == 1
    assert diagnostics["return_adjustment"] == "none"


@pytest.mark.parametrize("kind", ["duplicate", "short", "sparse"])
def test_persisted_entry_rejects_invalid_observation_windows(kind: str) -> None:
    dates = pd.date_range("2025-01-01", periods=31, freq="12D")
    if kind == "duplicate":
        dates = dates.to_series().reset_index(drop=True)
        dates.iloc[-1] = dates.iloc[-2]
        dates = pd.DatetimeIndex(dates)
    elif kind == "short":
        dates = pd.date_range("2025-01-01", periods=10, freq="30D")
    elif kind == "sparse":
        dates = pd.date_range("2025-01-01", periods=31, freq="5D")
    nav = np.linspace(1.0, 1.02, len(dates))
    frame = pd.DataFrame({"nav": nav, "accum_nav": nav, "daily_growth": None}, index=dates)
    analyzer = RiskAnalyzer(config={"risk_metrics": {"min_data_points": 30}}, db_manager=object())
    assert analyzer._calculate_period_metrics(frame, "1y") is None


def test_two_public_entries_share_common_metric_results() -> None:
    dates = pd.date_range("2025-01-01", periods=366, freq="D")
    returns = np.resize(np.array([0.0003, -0.0001, 0.0002]), 365)
    nav = np.concatenate(([1.0], np.cumprod(1 + returns)))
    # The first NAV point has no in-window return. Every following real
    # observation belongs to the window, including the first non-null value.
    effective_returns = returns
    live_frame = pd.DataFrame({
        "nav_date": dates, "unit_nav": nav, "accum_nav": nav,
        "daily_growth": np.concatenate(([np.nan], returns * 100)),
    })
    persisted_frame = live_frame.rename(columns={"unit_nav": "nav"}).set_index("nav_date")
    analyzer = RiskAnalyzer(config={"risk_metrics": {"min_data_points": 30, "risk_free_rate": 0.02}}, db_manager=object())
    persisted = analyzer._calculate_period_metrics(
        persisted_frame, "1y", api_return=None,
        api_return_date=None, api_snapshot_date=dates[-1],
    )
    live = RiskCalculator(risk_free_rate=0.02).calculate_all_metrics(live_frame)
    assert persisted is not None
    for key in ("volatility", "max_drawdown", "raw_sharpe_ratio", "raw_sortino_ratio"):
        assert persisted[key] == pytest.approx(live[key], rel=5e-4, abs=5e-5)
