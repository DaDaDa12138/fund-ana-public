from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
SCREENING_ROOT = CORE_ROOT / "bond_fund_screening"
for candidate in (PROJECT_ROOT, SCREENING_ROOT, CORE_ROOT):
    while str(candidate) in sys.path:
        sys.path.remove(str(candidate))
for candidate in (PROJECT_ROOT, SCREENING_ROOT, CORE_ROOT):
    sys.path.insert(0, str(candidate))


from bond_fund_screening.core.filtering_engine import FilteringEngine
from bond_fund_screening.core.risk_calculator import RiskCalculator


def _daily_nav(dates: pd.DatetimeIndex) -> pd.DataFrame:
    growth = np.resize(np.array([0.01, 0.02, -0.01, 0.015]), len(dates))
    accum_nav = np.cumprod(1.0 + growth / 100.0)
    return pd.DataFrame(
        {
            "nav_date": dates,
            "unit_nav": accum_nav,
            "accum_nav": accum_nav,
            "daily_growth": growth,
        }
    )


def test_risk_calculator_daily_growth_drawdown_includes_window_start() -> None:
    calc = RiskCalculator(risk_free_rate=0.015, mar=0.0, trading_days_per_year=252)
    dates = pd.bdate_range("2026-01-02", periods=12)
    daily_growth = np.array([0.0, -10.0] + [0.0] * 10)
    accum_nav = np.cumprod(1.0 + daily_growth / 100.0)
    nav = pd.DataFrame(
        {
            "nav_date": dates,
            "unit_nav": accum_nav,
            "accum_nav": accum_nav,
            "daily_growth": daily_growth,
        }
    )

    metrics = calc.calculate_all_metrics(nav, period_years=12 / 365)

    assert metrics["max_drawdown"] == 0.1
    assert metrics["max_drawdown_duration"] == 11


def test_layer6_rejects_low_frequency_and_large_gap_nav_windows() -> None:
    engine = FilteringEngine(
        period_years=1,
        data_loader=SimpleNamespace(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
    )
    risk_cols = ["sharpe_ratio", "calmar_ratio", "max_drawdown", "volatility", "consistency"]

    weekly_nav = _daily_nav(pd.date_range("2025-01-01", periods=53, freq="7D"))
    _code, metrics, meta = engine._calculate_layer6_single_fund(
        "weekly",
        weekly_nav,
        risk_cols,
    )
    assert metrics is None
    assert meta["status"] == "LOW_FREQUENCY_NAV_OBSERVATIONS"
    assert meta["nav_count"] < meta["required_nav_points"]

    dense_dates = pd.bdate_range("2025-01-01", "2025-12-31")
    dates_with_gap = dense_dates[
        ~((dense_dates >= pd.Timestamp("2025-06-02")) & (dense_dates <= pd.Timestamp("2025-07-20")))
    ]
    gap_nav = _daily_nav(dates_with_gap)
    _code, metrics, meta = engine._calculate_layer6_single_fund(
        "large-gap",
        gap_nav,
        risk_cols,
    )
    assert metrics is None
    assert meta["status"] == "NAV_DATA_GAP"
    assert meta["max_gap_days"] > meta["max_gap_threshold_days"]


def test_layer6_rejects_short_coverage_even_with_enough_observations() -> None:
    engine = FilteringEngine(
        period_years=1,
        data_loader=SimpleNamespace(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
    )
    short_coverage_nav = _daily_nav(pd.bdate_range("2025-03-03", periods=220))

    _code, metrics, meta = engine._calculate_layer6_single_fund(
        "short-coverage",
        short_coverage_nav,
        ["max_drawdown", "volatility", "consistency"],
    )

    assert metrics is None
    assert meta["nav_count"] >= meta["required_nav_points"]
    assert meta["status"] == "INSUFFICIENT_NAV_COVERAGE"
    assert meta["coverage_days"] < meta["required_coverage_days"]


def test_layer6_uses_pre_window_nav_boundary_across_long_market_closure() -> None:
    engine = FilteringEngine(
        period_years=180 / 365,
        data_loader=SimpleNamespace(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
    )
    dates = pd.DatetimeIndex(
        [pd.Timestamp("2026-02-13")]
        + list(pd.bdate_range("2026-02-24", "2026-08-14"))
    )
    nav = _daily_nav(dates)

    _code, metrics, meta = engine._calculate_layer6_single_fund(
        "spring-festival-boundary",
        nav,
        ["sharpe_ratio", "calmar_ratio", "max_drawdown", "volatility", "consistency"],
        window_end_date=date(2026, 8, 17),
    )

    assert metrics is not None
    assert meta["status"] == "OK"
    assert meta["requested_period_start"] == "2026-02-18"
    assert meta["requested_period_end"] == "2026-08-17"
    assert meta["start_boundary_point_date"] == "2026-02-13"
    assert meta["start_boundary_predecessor_used"] is True
    assert meta["coverage_days"] >= meta["required_coverage_days"]


def test_layer6_without_pre_window_boundary_still_fails_closed() -> None:
    engine = FilteringEngine(
        period_years=180 / 365,
        data_loader=SimpleNamespace(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
    )
    nav = _daily_nav(pd.bdate_range("2026-02-24", "2026-08-14"))

    _code, metrics, meta = engine._calculate_layer6_single_fund(
        "missing-boundary",
        nav,
        ["sharpe_ratio", "calmar_ratio", "max_drawdown", "volatility", "consistency"],
        window_end_date=date(2026, 8, 17),
    )

    assert metrics is None
    assert meta["status"] == "INSUFFICIENT_NAV_COVERAGE"
    assert meta["start_boundary_point_date"] is None
    assert meta["start_boundary_predecessor_used"] is False


def test_layer6_small_cohort_fails_closed_without_nav_only_fallback() -> None:
    class _UnexpectedLoader:
        def get_batch_nav_history(self, *_args, **_kwargs):
            raise AssertionError("small cohort must not enter NAV-only fallback")

    engine = FilteringEngine(
        period_years=1,
        data_loader=_UnexpectedLoader(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
    )
    df = pd.DataFrame({"fcode": [f"{idx:06d}" for idx in range(3)]})

    result = engine._layer6_risk_metrics_filter(df)

    assert result.empty
    stats = engine.stats["layer_stats"][-1]
    assert stats["metadata"]["reason_code"] == "RISK_COHORT_TOO_SMALL"
    assert stats["metadata"]["fail_closed"] is True


def test_layer6_all_metrics_unavailable_fails_closed() -> None:
    nav = _daily_nav(pd.bdate_range("2025-01-01", "2025-12-31"))

    class _Loader:
        def get_batch_nav_history(self, fcodes, *_args, **_kwargs):
            return {code: nav.copy() for code in fcodes}

    engine = FilteringEngine(
        period_years=1,
        data_loader=_Loader(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
        as_of_date=date(2025, 12, 31),
    )
    engine.risk_calculator = SimpleNamespace(
        calculate_all_metrics=lambda *_args, **_kwargs: {
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "calmar_ratio": None,
            "max_drawdown": None,
            "volatility": None,
            "consistency": None,
            "calmar_style_tag": None,
            "calmar_min_drawdown_floor": None,
            "calmar_min_drawdown_floor_source": None,
        }
    )
    df = pd.DataFrame({"fcode": [f"{idx:06d}" for idx in range(10)]})

    result = engine._layer6_risk_metrics_filter(df)

    assert result.empty
    stats = engine.stats["layer_stats"][-1]
    assert stats["metadata"]["reason_code"] == "NO_CALCULABLE_RISK_RESULTS"
    assert stats["metadata"]["calculation_status_counts"] == {
        "RISK_METRICS_UNAVAILABLE": 10
    }


def test_layer6_calculable_risk_cohort_too_small_fails_closed() -> None:
    dense_nav = _daily_nav(pd.bdate_range("2025-01-01", "2025-12-31"))
    sparse_nav = _daily_nav(pd.date_range("2025-01-01", periods=53, freq="7D"))

    class _Loader:
        def get_batch_nav_history(self, fcodes, *_args, **_kwargs):
            return {
                code: dense_nav.copy() if code == "000000" else sparse_nav.copy()
                for code in fcodes
            }

    engine = FilteringEngine(
        period_years=1,
        data_loader=_Loader(),
        verbose=False,
        enable_tracking=False,
        fund_type="bond",
        as_of_date=date(2025, 12, 31),
    )
    df = pd.DataFrame({"fcode": [f"{idx:06d}" for idx in range(10)]})

    result = engine._layer6_risk_metrics_filter(df)

    assert result.empty
    stats = engine.stats["layer_stats"][-1]
    assert stats["metadata"]["reason_code"] == "RISK_CALCULABLE_COHORT_TOO_SMALL"
    assert stats["metadata"]["calculable_cohort_size"] == 1
