from __future__ import annotations

import math
from typing import Any, Mapping

import pandas as pd


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    numeric = pd.to_numeric(
        frame.get(column, pd.Series(index=frame.index, dtype=float)),
        errors="coerce",
    )
    return numeric.replace([math.inf, -math.inf], math.nan)


def standard_calmar_series(frame: pd.DataFrame) -> pd.Series:
    """PerformanceAnalytics-style Calmar: annual return / absolute drawdown."""
    annual_return = _numeric_series(frame, "annual_return") / 100.0
    max_drawdown = _numeric_series(frame, "max_drawdown").abs()
    result = annual_return / max_drawdown.where(max_drawdown.gt(0))
    return result.where(result.map(math.isfinite))


def adjusted_calmar_series(
    frame: pd.DataFrame,
    *,
    risk_free_rate: float,
    min_drawdown_by_style: Mapping[str, float],
    deduct_risk_free_rate: bool,
    cap_abs: float | None = 20.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Replay the Layer6 drawdown floor, with/without its risk-free deduction."""
    annual_return = _numeric_series(frame, "annual_return") / 100.0
    max_drawdown = _numeric_series(frame, "max_drawdown").abs()
    style = frame.get(
        "债基风格标签",
        pd.Series("", index=frame.index, dtype=object),
    ).fillna("").astype(str).str.strip()
    default_floor = float(min_drawdown_by_style.get("default", 0.005))
    floor = style.map(
        lambda value: float(min_drawdown_by_style.get(value, default_floor))
    )
    # A floor adjusts an observed drawdown; it is not evidence for a missing one.
    effective_drawdown = pd.concat([max_drawdown, floor], axis=1).max(axis=1).where(max_drawdown.notna())
    numerator = annual_return - float(risk_free_rate) if deduct_risk_free_rate else annual_return
    result = numerator / effective_drawdown.where(effective_drawdown.gt(0))
    result = result.where(result.map(math.isfinite))
    if cap_abs is not None:
        result = result.clip(lower=-abs(float(cap_abs)), upper=abs(float(cap_abs)))
    floor_bound = max_drawdown.lt(floor) & max_drawdown.notna()
    return result, effective_drawdown, floor_bound


def apply_metric_variant(
    frame: pd.DataFrame,
    *,
    variant: str,
    risk_free_rate: float,
    min_drawdown_by_style: Mapping[str, float],
    zero_downside_codes: set[str] | None = None,
    positive_zero_downside_codes: set[str] | None = None,
) -> pd.DataFrame:
    """Apply an observation-only metric contract variant before score replay."""
    current = frame.copy()
    code_series = current.get(
        "基金代码",
        current.get("fcode", pd.Series("", index=current.index, dtype=object)),
    ).fillna("").astype(str).str.strip().str.zfill(6)

    if variant == "production_capped_score_raw_display":
        return current

    if variant == "raw_sharpe_sortino_score":
        raw_sharpe = _numeric_series(current, "raw_sharpe_ratio").combine_first(
            _numeric_series(current, "1年-夏普比率")
        )
        raw_sortino = _numeric_series(current, "raw_sortino_ratio").combine_first(
            _numeric_series(current, "1年-索提诺比率")
        )
        current["sharpe_ratio"] = raw_sharpe
        current["sortino_ratio"] = raw_sortino
        current["1年-夏普比率"] = raw_sharpe
        current["1年-索提诺比率"] = raw_sortino
        return current

    if variant == "consistent_capped_sharpe_sortino":
        capped_sharpe = _numeric_series(current, "1年-夏普比率(展示封顶)").combine_first(
            _numeric_series(current, "sharpe_ratio")
        )
        capped_sortino = _numeric_series(current, "1年-索提诺比率(展示封顶)").combine_first(
            _numeric_series(current, "sortino_ratio")
        )
        current["sharpe_ratio"] = capped_sharpe
        current["sortino_ratio"] = capped_sortino
        current["1年-夏普比率"] = capped_sharpe
        current["1年-索提诺比率"] = capped_sortino
        return current

    if variant in {"standard_calmar_recomputed", "style_floor_calmar", "layer6_adjusted_calmar"}:
        if variant == "standard_calmar_recomputed":
            calmar = standard_calmar_series(current)
            effective_drawdown = _numeric_series(current, "max_drawdown").abs()
            floor_bound = pd.Series(False, index=current.index, dtype=bool)
        else:
            calmar, effective_drawdown, floor_bound = adjusted_calmar_series(
                current,
                risk_free_rate=risk_free_rate,
                min_drawdown_by_style=min_drawdown_by_style,
                deduct_risk_free_rate=variant == "layer6_adjusted_calmar",
                cap_abs=20.0,
            )
        current["calmar_ratio"] = calmar
        current["1年-卡玛比率"] = calmar
        current["shadow_calmar_effective_drawdown"] = effective_drawdown
        current["shadow_calmar_floor_bound"] = floor_bound
        return current

    affected = set(zero_downside_codes or set())
    positive_affected = set(positive_zero_downside_codes or set())
    if variant in {"sortino_zero_downside_missing", "sortino_zero_downside_bounded"}:
        if not positive_affected.issubset(affected):
            raise ValueError("positive_zero_downside_codes_require_zero_downside_evidence")
        affected_mask = code_series.isin(affected)
        current.loc[affected_mask, "sortino_ratio"] = pd.NA
        current.loc[affected_mask, "raw_sortino_ratio"] = pd.NA
        current.loc[affected_mask, "1年-索提诺比率"] = pd.NA
        for field in ("1年-索提诺比率(展示封顶)", "sortino_ratio_cap_hit", "1年-索提诺cap命中"):
            if field in current:
                current.loc[affected_mask, field] = pd.NA
        if variant == "sortino_zero_downside_bounded":
            positive_mask = code_series.isin(positive_affected)
            current.loc[positive_mask, "sortino_ratio"] = 10.0
            current.loc[positive_mask, "raw_sortino_ratio"] = 10.0
            current.loc[positive_mask, "1年-索提诺比率"] = 10.0
            if "1年-索提诺比率(展示封顶)" in current:
                current.loc[positive_mask, "1年-索提诺比率(展示封顶)"] = 10.0
        return current

    raise ValueError(f"unsupported risk metric shadow variant: {variant}")


def membership_delta(baseline_codes: set[str], scenario_codes: set[str]) -> dict[str, Any]:
    added = sorted(scenario_codes - baseline_codes)
    removed = sorted(baseline_codes - scenario_codes)
    union = baseline_codes | scenario_codes
    return {
        "aligned": not added and not removed,
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(added) + len(removed),
        "jaccard": round(len(baseline_codes & scenario_codes) / len(union), 6) if union else 1.0,
        "added_codes": added,
        "removed_codes": removed,
    }


__all__ = [
    "adjusted_calmar_series",
    "apply_metric_variant",
    "membership_delta",
    "standard_calmar_series",
]
