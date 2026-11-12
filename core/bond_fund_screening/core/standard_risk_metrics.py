"""Project-wide strategy risk metric calculation core.

Both the persisted risk pipeline and the live screening pipeline must call this
module so formula, units and non-computable semantics cannot drift apart.
All returns and risk values accepted/returned here use decimal units.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
from utils.calculation_capture import capture_call


def annualize_compound_return(total_return: float, span_days: float) -> Optional[float]:
    """Calendar-time CAGR; observation count is not elapsed time."""
    if not np.isfinite([total_return, span_days]).all() or span_days <= 0 or total_return <= -1:
        return None
    with np.errstate(over="ignore", invalid="ignore"):
        result = float(np.expm1(np.log1p(total_return) * 365.25 / span_days))
    return result if np.isfinite(result) else None


def estimate_periods_per_year(observation_count: int, actual_years: float,
                              maximum: float = 252.0) -> float:
    """Observed interval density, bounded by the configured annual maximum."""
    if observation_count <= 0 or actual_years <= 0 or maximum <= 0:
        return float(maximum)
    return max(1.0, min(float(observation_count) / actual_years, float(maximum)))


def calculate_calmar_ratio(annual_return: float, max_drawdown: Optional[float], *,
                          risk_free_rate: float = 0.0, floor: float = 0.0,
                          cap: float | None = None) -> Optional[float]:
    """Explicit standard (default) or strategy-adjusted Calmar variant.

    Floors and excess-return numerators are policy parameters, not corrections
    to the standard annual-return/drawdown definition. Never infer a duration,
    loss or a finite standard ratio from a zero denominator.
    """
    if max_drawdown is None or not np.isfinite([annual_return, max_drawdown, risk_free_rate, floor]).all():
        return None
    if max_drawdown < 0 or floor < 0:
        return None
    denominator = max(float(max_drawdown), float(floor))
    if denominator == 0:
        return None
    ratio = (float(annual_return) - float(risk_free_rate)) / denominator
    return float(np.clip(ratio, -cap, cap)) if cap is not None else ratio


def calculate_drawdown_stats(nav_values: np.ndarray) -> tuple[Optional[float], Optional[int]]:
    """Return drawdown and the longest strictly underwater observation count.

    A return to the previous peak ends a spell. An unrecovered final spell is
    included; missing calendar dates do not add observations. Invalid inputs
    remain missing, while a valid series that never falls below a peak has 0.
    """
    nav = np.asarray(nav_values, dtype=float)
    if nav.ndim != 1 or len(nav) < 2 or not np.isfinite(nav).all() or (nav <= 0).any():
        return None, None
    running_max = np.maximum.accumulate(nav)
    max_drawdown = abs(float(np.min((nav - running_max) / running_max)))
    duration = longest = 0
    for underwater in nav < running_max:
        duration = duration + 1 if underwater else 0
        longest = max(longest, duration)
    return max_drawdown, longest


def return_observation_diagnostics(returns: np.ndarray) -> dict[str, object]:
    """Observed sign and jump facts only; never infer a cause or edit returns."""
    values = np.asarray(returns, dtype=float)
    if values.ndim != 1 or len(values) == 0 or not np.isfinite(values).all():
        return {
            "non_negative_ratio": None,
            "active_positive_ratio": None,
            "positive_day_count": None,
            "negative_day_count": None,
            "zero_day_count": None,
            "return_adjustment": "none",
        }
    active = np.abs(values) > 1e-6
    return {
        "non_negative_ratio": float(np.mean(values >= 0) * 100),
        "active_positive_ratio": float(np.mean(values[active] > 0) * 100) if active.any() else None,
        "active_day_absolute_tolerance": 1e-6,
        "positive_day_count": int(np.sum(values > 0)),
        "negative_day_count": int(np.sum(values < 0)),
        "zero_day_count": int(np.sum(values == 0)),
        "return_adjustment": "none",
    }


@capture_call("risk.standard_metrics")
def calculate_standard_risk_metrics(
    returns: np.ndarray,
    nav_values: np.ndarray,
    *,
    annual_return: float,
    periods_per_year: float,
    risk_free_rate: float,
    mar: float,
) -> dict[str, object]:
    """Calculate project annual-return ratios, retaining undefined denominators.

    The historical function name is retained for internal compatibility. These
    ratios must be presented as strategy indicators, not standard Sharpe/Sortino.
    """
    values = np.asarray(returns, dtype=float)
    nav = np.asarray(nav_values, dtype=float)
    empty = {
        "volatility": None, "max_drawdown": None, "max_drawdown_duration": None,
        "raw_sharpe_ratio": None, "sharpe_ratio": None,
        "sharpe_ratio_cap_hit": None, "raw_sortino_ratio": None,
        "sortino_ratio": None, "sortino_ratio_cap_hit": None,
        "undefined_metric_reasons": {
            "sharpe_ratio": "invalid_observations",
            "sortino_ratio": "invalid_observations",
            "max_drawdown_duration": "invalid_observations",
        },
    }
    if len(values) < 2 or len(nav) < 2 or periods_per_year <= 0:
        return empty
    if (not np.isfinite(values).all() or not np.isfinite(nav).all() or (nav <= 0).any()
            or not np.isfinite([annual_return, periods_per_year, risk_free_rate, mar]).all()):
        return empty

    daily_std = float(np.std(values, ddof=1))
    volatility = daily_std * float(np.sqrt(periods_per_year))
    max_drawdown, max_drawdown_duration = calculate_drawdown_stats(nav)

    raw_sharpe: Optional[float]
    if abs(volatility) < 1e-10:
        raw_sharpe = None
    else:
        raw_sharpe = (float(annual_return) - float(risk_free_rate)) / volatility

    daily_mar = float(mar) / float(periods_per_year)
    downside = np.minimum(values - daily_mar, 0.0)
    downside_variance = float(np.mean(downside * downside))
    if downside_variance < 1e-20:
        # A zero downside deviation has no defined Sortino denominator. Keep
        # it missing rather than substituting an unrelated volatility proxy.
        raw_sortino = None
    else:
        downside_std = float(np.sqrt(downside_variance) * np.sqrt(periods_per_year))
        raw_sortino = None if abs(downside_std) < 1e-10 else (float(annual_return) - float(mar)) / downside_std

    undefined_reasons = {}
    if raw_sharpe is None:
        undefined_reasons["sharpe_ratio"] = "zero_or_negligible_volatility"
    if raw_sortino is None:
        undefined_reasons["sortino_ratio"] = "zero_or_negligible_downside_deviation"

    return {
        "volatility": volatility,
        "max_drawdown": max_drawdown,
        "standard_calmar_ratio": calculate_calmar_ratio(annual_return, max_drawdown),
        "max_drawdown_duration": max_drawdown_duration,
        "raw_sharpe_ratio": raw_sharpe,
        # The public Sharpe display cap is upper-only.  Negative observations
        # retain their full adverse signal so a declining series is not made
        # to look merely "less bad" by a symmetric presentation clamp.
        "sharpe_ratio": min(raw_sharpe, 5.0) if raw_sharpe is not None else None,
        "sharpe_ratio_cap_hit": None if raw_sharpe is None else raw_sharpe > 5.0,
        "raw_sortino_ratio": raw_sortino,
        "sortino_ratio": max(min(raw_sortino, 10.0), -10.0) if raw_sortino is not None else None,
        "sortino_ratio_cap_hit": None if raw_sortino is None else abs(raw_sortino) > 10.0,
        "undefined_metric_reasons": undefined_reasons,
    }
