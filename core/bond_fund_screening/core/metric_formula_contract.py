"""Versioned descriptions of project strategy formulas, without business data."""
from __future__ import annotations

from typing import Any

VERSION = "strategy-risk-2026-10-28.3"


def strategy_formula_contract(*, engine: str, periods_per_year: float, risk_free_rate: float,
                              mar: float, return_source: str, verified: bool,
                              annual_return_source: str, calmar_floor: float | None = None) -> dict[str, Any]:
    return {
        "version": VERSION, "metric_class": "project_strategy_indicator", "engine": engine,
        "frequency": "observed returns", "annualization_factor": periods_per_year,
        "risk_free_rate": risk_free_rate, "minimum_acceptable_return": mar,
        "return_source": return_source, "return_observations_verified": verified,
        "annual_return_source": annual_return_source,
        "local_annual_return_formula": "expm1(log1p(compound_return)*365.25/calendar_span_days)",
        "frequency_estimation": "min(configured_maximum,observed_intervals/(calendar_span_days/365.25)); minimum 1",
        "sharpe_formula": "(annual_return-risk_free_rate)/(sample_std(returns)*sqrt(N))",
        "sortino_formula": "(annual_return-MAR)/(sqrt(mean(min(returns-MAR/N,0)^2))*sqrt(N))",
        "zero_downside_policy": "Sortino denominator is undefined when downside deviation is zero; preserve NULL and expose undefined_metric_reasons",
        "calmar_formula": ("annual_return/max_drawdown; zero drawdown is NULL" if engine == "database"
                           else "clip((annual_return-risk_free_rate)/max(abs(max_drawdown),style_floor),-20,20)"),
        "calmar_variants": {
            "database": "unadjusted annual return divided by observed drawdown; zero drawdown NULL",
            "screening": "strategy excess return divided by style-floored drawdown, clipped [-20,20]",
            "not_comparable_without_variant": True,
        },
        "calmar_floor": calmar_floor,
        "calmar_variant": "annual_return_over_drawdown" if engine == "database" else "strategy_excess_return_with_style_floor",
        "shared_formula_core": "bond_fund_screening.core.standard_risk_metrics",
        "monthly_definition": "20-observation buckets, not calendar months" if engine == "screening" else "not calculated",
        "missing_policy": "undefined denominators remain NULL; fallback source cannot verify total returns",
        "verification_basis": "finite complete observed returns and bounded date gaps; not independent supplier verification",
        "standard_metric_claim": False,
    }
