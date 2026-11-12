#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
净值历史一致性审计。

目标：
1. 基于项目现有字段 `unit_nav / accum_nav / daily_growth` 做只读一致性审计；
2. 量化 `accum_nav` 分红差额异常回退、`daily_growth` 与首尾收益冲突等问题；
3. 输出可复用的 JSON / Markdown 产物，不修改数据库与调度逻辑。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from database.connection import resolve_db_path
from utils.buyability import normalize_buyability
from utils.fund_code_alias_audit import (
    DEFAULT_AUDIT_DIR,
    PROJECT_ROOT,
    load_latest_json_artifact,
    open_readonly_db,
)


NAV_HISTORY_CONSISTENCY_AUDIT_JSON_PATTERN = "nav_history_consistency_audit_*.json"
NAV_HISTORY_CONSISTENCY_AUDIT_SQL = """
SELECT
    n.fcode,
    f.shortname,
    f.ftype,
    f.type_category,
    f.holding_days,
    f.is_buyable,
    f.purchase_status,
    n.nav_date,
    n.unit_nav,
    n.accum_nav,
    n.daily_growth
FROM fund_nav_history AS n
JOIN funds AS f
  ON f.fcode = n.fcode
WHERE n.unit_nav IS NOT NULL
  AND (
        COALESCE(f.type_category, '') = 'fixed_income'
        OR COALESCE(f.ftype, '') LIKE '%债%'
      )
ORDER BY n.fcode, n.nav_date
"""

DEFAULT_WINDOW_CONFIG: Tuple[Tuple[str, int], ...] = (
    ("1y", 365),
    ("3y", 365 * 3),
    ("5y", 365 * 5),
)

DEFAULT_ACCUM_GAP_REGRESSION_TOLERANCE = 0.01
DEFAULT_DAILY_GROWTH_CUMULATIVE_DIFF_TOLERANCE = 0.005
DEFAULT_DAILY_GROWTH_STEP_DIFF_TOLERANCE = 0.003
DEFAULT_MIN_WINDOW_POINTS = 20
DEFAULT_MIN_DAILY_GROWTH_COVERAGE_RATIO = 0.8
DEFAULT_MIN_DAILY_LIKE_POINTS_PER_YEAR = 180.0
DEFAULT_REVIEW_ROW_LIMIT = 300
DEFAULT_UNIT_ACCUM_SCALE_MISMATCH_RATIO = 20.0
DEFAULT_UNIT_ACCUM_SCALE_STABILITY_TOLERANCE = 0.2
DEFAULT_RESOLVED_GAP_REGRESSION_TOLERANCE = 0.05
DEFAULT_RESOLVED_GAP_END_TOLERANCE = 0.001
DEFAULT_RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE = 0.005
ACTIONABLE_ISSUE_KEYS = frozenset(
    {
        "unit_accum_scale_mismatch",
        "accum_nav_gap_regression",
        "daily_growth_step_conflicts_with_unit_nav",
        "daily_growth_total_return_conflicts_with_unit_nav",
    }
)


def _resolve_output_dir_path(output_dir: str) -> Path:
    raw_path = Path(output_dir)
    if raw_path.is_absolute():
        return raw_path.resolve()

    if raw_path.parts and raw_path.parts[0] == PROJECT_ROOT.name:
        return (PROJECT_ROOT.parent / raw_path).resolve()

    return (PROJECT_ROOT / raw_path).resolve()


def load_latest_nav_history_consistency_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新净值历史一致性审计产物。"""
    return load_latest_json_artifact(
        NAV_HISTORY_CONSISTENCY_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return float(number)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_window_configs(
    window_configs: Optional[Sequence[Tuple[str, int]]] = None,
) -> Tuple[Tuple[str, int], ...]:
    normalized: List[Tuple[str, int]] = []
    for label, days in window_configs or DEFAULT_WINDOW_CONFIG:
        normalized_label = str(label or "").strip()
        normalized_days = _safe_int(days, default=0)
        if not normalized_label or normalized_days <= 0:
            continue
        normalized.append((normalized_label, normalized_days))
    return tuple(normalized)


def _has_reliable_accum_nav_series(
    df: pd.DataFrame,
    *,
    accum_gap_regression_tolerance: float,
) -> bool:
    """对齐主链口径，仅在累计净值序列自身可靠时才优先使用。"""
    if df is None or df.empty or "accum_nav" not in df.columns:
        return False

    accum = pd.to_numeric(df["accum_nav"], errors="coerce")
    if accum.isna().any() or (accum <= 0).any():
        return False

    if "unit_nav" not in df.columns:
        return True

    unit = pd.to_numeric(df["unit_nav"], errors="coerce")
    if unit.isna().any() or (unit <= 0).any():
        return True

    if _has_stable_unit_accum_scale_mismatch(unit, accum):
        return True

    dividend_gap = accum - unit
    if dividend_gap.isna().any() or (dividend_gap < -1e-6).any():
        return False

    max_regression = float((dividend_gap.cummax() - dividend_gap).max() or 0.0)
    if max_regression <= float(accum_gap_regression_tolerance):
        return True

    return _has_resolved_small_gap_regression(
        unit,
        accum,
        dividend_gap=dividend_gap,
        max_regression=max_regression,
        regression_tolerance=accum_gap_regression_tolerance,
    )


def _has_stable_unit_accum_scale_mismatch(
    unit: pd.Series,
    accum: pd.Series,
    *,
    mismatch_ratio: float = DEFAULT_UNIT_ACCUM_SCALE_MISMATCH_RATIO,
    stability_tolerance: float = DEFAULT_UNIT_ACCUM_SCALE_STABILITY_TOLERANCE,
) -> bool:
    scale_df = pd.DataFrame({"unit_nav": unit, "accum_nav": accum}).dropna()
    if scale_df.empty:
        return False

    scale_df = scale_df[(scale_df["unit_nav"] > 0) & (scale_df["accum_nav"] > 0)]
    if scale_df.empty:
        return False

    ratio_series = scale_df["unit_nav"] / scale_df["accum_nav"]
    ratio_median = _safe_float(ratio_series.median()) if not ratio_series.empty else None
    if ratio_median is None or ratio_median <= 0:
        return False

    if len(ratio_series) >= 3:
        ratio_p10 = _safe_float(ratio_series.quantile(0.10))
        ratio_p90 = _safe_float(ratio_series.quantile(0.90))
    else:
        ratio_p10 = ratio_median
        ratio_p90 = ratio_median

    if ratio_p10 is None or ratio_p90 is None:
        return False

    ratio_band = (ratio_p90 - ratio_p10) / ratio_median if ratio_median > 0 else None
    return bool(
        ratio_median >= float(mismatch_ratio)
        and ratio_band is not None
        and ratio_band <= float(stability_tolerance)
    )


def _has_resolved_small_gap_regression(
    unit: pd.Series,
    accum: pd.Series,
    *,
    dividend_gap: pd.Series,
    max_regression: float,
    regression_tolerance: float,
    resolved_regression_tolerance: float = DEFAULT_RESOLVED_GAP_REGRESSION_TOLERANCE,
    resolved_gap_end_tolerance: float = DEFAULT_RESOLVED_GAP_END_TOLERANCE,
    total_return_diff_tolerance: float = DEFAULT_RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE,
) -> bool:
    if max_regression > max(float(resolved_regression_tolerance), float(regression_tolerance)):
        return False

    try:
        final_gap = abs(float(dividend_gap.iloc[-1]))
        unit_start = float(unit.iloc[0])
        unit_end = float(unit.iloc[-1])
        accum_start = float(accum.iloc[0])
        accum_end = float(accum.iloc[-1])
    except (TypeError, ValueError, IndexError):
        return False

    if min(unit_start, unit_end, accum_start, accum_end) <= 0:
        return False
    if final_gap > max(float(resolved_gap_end_tolerance), float(regression_tolerance)):
        return False

    unit_total_return = (unit_end / unit_start) - 1.0
    accum_total_return = (accum_end / accum_start) - 1.0
    return abs(unit_total_return - accum_total_return) <= float(total_return_diff_tolerance)


def _estimate_points_per_year(
    row_count: int,
    *,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> float:
    span_days = max(int((end_date - start_date).days), 1)
    actual_years = span_days / 365.0
    if actual_years <= 0:
        return 0.0
    return float(row_count) / actual_years


def _can_use_daily_growth_window(
    df: pd.DataFrame,
    *,
    min_points: int,
    min_daily_growth_coverage_ratio: float,
    min_daily_like_points_per_year: float,
) -> bool:
    if df is None or df.empty:
        return False
    if "daily_growth" not in df.columns or "nav_date" not in df.columns:
        return False

    daily_growth_series = pd.to_numeric(df["daily_growth"], errors="coerce").dropna()
    coverage_ratio = len(daily_growth_series) / len(df) if len(df) else 0.0
    if len(daily_growth_series) < int(min_points) or coverage_ratio < float(min_daily_growth_coverage_ratio):
        return False

    estimated_points_per_year = _estimate_points_per_year(
        len(daily_growth_series),
        start_date=df["nav_date"].min(),
        end_date=df["nav_date"].max(),
    )
    return estimated_points_per_year >= float(min_daily_like_points_per_year)


def _split_issue_keys(issue_keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    actionable = sorted({key for key in issue_keys if key in ACTIONABLE_ISSUE_KEYS})
    informational = sorted({key for key in issue_keys if key not in ACTIONABLE_ISSUE_KEYS})
    return actionable, informational


def _is_screening_relevant_record(meta: Dict[str, Any]) -> bool:
    holding_days = _safe_int(meta.get("holding_days"), default=0)
    if holding_days > 90:
        return False

    if meta.get("is_buyable") is True:
        return True

    purchase_status = str(meta.get("purchase_status") or "").strip()
    return purchase_status in {"开放申购", "开放", "可申购"}


def _build_window_consistency_record(
    window_df: pd.DataFrame,
    *,
    label: str,
    days: int,
    cumulative_diff_tolerance: float,
    step_diff_tolerance: float,
    min_points: int,
    min_daily_growth_coverage_ratio: float,
    min_daily_like_points_per_year: float,
    use_unit_nav_checks: bool,
    use_accum_nav_checks: bool,
) -> Optional[dict]:
    if window_df.empty:
        return None
    if len(window_df) < min_points:
        return None

    unit_series = pd.to_numeric(window_df["unit_nav"], errors="coerce").dropna()
    if len(unit_series) < 2 or unit_series.iloc[0] <= 0 or unit_series.iloc[-1] <= 0:
        return None

    daily_growth_series = pd.to_numeric(window_df["daily_growth"], errors="coerce").dropna() / 100.0
    coverage_ratio = len(daily_growth_series) / len(window_df)
    if len(daily_growth_series) < min_points or coverage_ratio < min_daily_growth_coverage_ratio:
        return {
            "label": label,
            "days": int(days),
            "row_count": int(len(window_df)),
            "daily_growth_point_count": int(len(daily_growth_series)),
            "daily_growth_coverage_ratio": round(float(coverage_ratio), 6),
            "status": "insufficient_daily_growth",
            "issue_keys": [],
        }

    estimated_points_per_year = _estimate_points_per_year(
        len(daily_growth_series),
        start_date=window_df["nav_date"].min(),
        end_date=window_df["nav_date"].max(),
    )
    if estimated_points_per_year < float(min_daily_like_points_per_year):
        return {
            "label": label,
            "days": int(days),
            "row_count": int(len(window_df)),
            "daily_growth_point_count": int(len(daily_growth_series)),
            "daily_growth_coverage_ratio": round(float(coverage_ratio), 6),
            "estimated_points_per_year": round(float(estimated_points_per_year), 6),
            "status": "insufficient_daily_growth_density",
            "issue_keys": [],
        }

    daily_growth_total_return = float((1.0 + daily_growth_series).prod() - 1.0)
    unit_total_return = float(unit_series.iloc[-1] / unit_series.iloc[0] - 1.0)
    diff_vs_unit = abs(daily_growth_total_return - unit_total_return)

    accum_series = pd.to_numeric(window_df["accum_nav"], errors="coerce").dropna()
    accum_total_return = None
    diff_vs_accum = None
    if len(accum_series) >= 2 and accum_series.iloc[0] > 0 and accum_series.iloc[-1] > 0:
        accum_total_return = float(accum_series.iloc[-1] / accum_series.iloc[0] - 1.0)
        diff_vs_accum = abs(daily_growth_total_return - accum_total_return)

    window_df["prev_unit_nav"] = window_df["unit_nav"].shift(1)
    window_df["prev_accum_nav"] = window_df["accum_nav"].shift(1)
    window_df["unit_step_return"] = window_df["unit_nav"] / window_df["prev_unit_nav"] - 1.0
    window_df["accum_step_return"] = window_df["accum_nav"] / window_df["prev_accum_nav"] - 1.0
    window_df["daily_growth_return"] = window_df["daily_growth"] / 100.0
    window_df["unit_step_diff"] = (window_df["daily_growth_return"] - window_df["unit_step_return"]).abs()
    window_df["accum_step_diff"] = (window_df["daily_growth_return"] - window_df["accum_step_return"]).abs()

    unit_step_issue_mask = (
        use_unit_nav_checks
        & window_df["prev_unit_nav"].notna()
        & (window_df["prev_unit_nav"] > 0)
        & window_df["daily_growth_return"].notna()
        & (window_df["unit_step_diff"] > float(step_diff_tolerance))
    )
    accum_step_issue_mask = (
        use_accum_nav_checks
        & window_df["prev_accum_nav"].notna()
        & (window_df["prev_accum_nav"] > 0)
        & window_df["daily_growth_return"].notna()
        & window_df["accum_nav"].notna()
        & (window_df["accum_step_diff"] > float(step_diff_tolerance))
    )

    issue_keys: List[str] = []
    if bool(unit_step_issue_mask.any()):
        issue_keys.append("daily_growth_step_conflicts_with_unit_nav")
    if bool(accum_step_issue_mask.any()):
        issue_keys.append("daily_growth_step_conflicts_with_accum_nav")
    if use_unit_nav_checks and diff_vs_unit is not None and diff_vs_unit > cumulative_diff_tolerance:
        issue_keys.append("daily_growth_total_return_conflicts_with_unit_nav")
    if use_accum_nav_checks and diff_vs_accum is not None and diff_vs_accum > cumulative_diff_tolerance:
        issue_keys.append("daily_growth_total_return_conflicts_with_accum_nav")

    status = "ok" if not issue_keys else "flagged"
    return {
        "label": label,
        "days": int(days),
        "row_count": int(len(window_df)),
        "daily_growth_point_count": int(len(daily_growth_series)),
        "daily_growth_coverage_ratio": round(float(coverage_ratio), 6),
        "estimated_points_per_year": round(float(estimated_points_per_year), 6),
        "daily_growth_total_return": round(daily_growth_total_return, 6),
        "unit_nav_total_return": round(unit_total_return, 6),
        "accum_nav_total_return": round(accum_total_return, 6) if accum_total_return is not None else None,
        "abs_diff_vs_unit_nav": round(diff_vs_unit, 6),
        "abs_diff_vs_accum_nav": round(diff_vs_accum, 6) if diff_vs_accum is not None else None,
        "window_unit_step_conflict_count": int(unit_step_issue_mask.sum()),
        "window_accum_step_conflict_count": int(accum_step_issue_mask.sum()),
        "max_window_unit_step_diff": round(_safe_float(window_df["unit_step_diff"].max()) or 0.0, 6),
        "max_window_accum_step_diff": round(_safe_float(window_df["accum_step_diff"].max()) or 0.0, 6),
        "status": status,
        "issue_keys": issue_keys,
    }


def analyze_fund_nav_history_consistency(
    rows: Sequence[dict],
    *,
    accum_gap_regression_tolerance: float = DEFAULT_ACCUM_GAP_REGRESSION_TOLERANCE,
    daily_growth_cumulative_diff_tolerance: float = DEFAULT_DAILY_GROWTH_CUMULATIVE_DIFF_TOLERANCE,
    daily_growth_step_diff_tolerance: float = DEFAULT_DAILY_GROWTH_STEP_DIFF_TOLERANCE,
    min_window_points: int = DEFAULT_MIN_WINDOW_POINTS,
    min_daily_growth_coverage_ratio: float = DEFAULT_MIN_DAILY_GROWTH_COVERAGE_RATIO,
    min_daily_like_points_per_year: float = DEFAULT_MIN_DAILY_LIKE_POINTS_PER_YEAR,
    unit_accum_scale_mismatch_ratio: float = DEFAULT_UNIT_ACCUM_SCALE_MISMATCH_RATIO,
    unit_accum_scale_stability_tolerance: float = DEFAULT_UNIT_ACCUM_SCALE_STABILITY_TOLERANCE,
    window_configs: Optional[Sequence[Tuple[str, int]]] = None,
) -> Optional[dict]:
    if not rows:
        return None

    df = pd.DataFrame(rows).copy()
    if df.empty:
        return None

    df["nav_date"] = pd.to_datetime(df["nav_date"], errors="coerce")
    df["unit_nav"] = pd.to_numeric(df["unit_nav"], errors="coerce")
    df["accum_nav"] = pd.to_numeric(df.get("accum_nav"), errors="coerce")
    df["daily_growth"] = pd.to_numeric(df.get("daily_growth"), errors="coerce")
    df = df.dropna(subset=["nav_date", "unit_nav"]).sort_values("nav_date").reset_index(drop=True)
    if len(df) < 2:
        return None

    meta = dict(rows[0])
    meta.pop("nav_date", None)
    meta.pop("unit_nav", None)
    meta.pop("accum_nav", None)
    meta.pop("daily_growth", None)

    df["prev_unit_nav"] = df["unit_nav"].shift(1)
    df["prev_accum_nav"] = df["accum_nav"].shift(1)
    df["unit_step_return"] = df["unit_nav"] / df["prev_unit_nav"] - 1.0
    df["accum_step_return"] = df["accum_nav"] / df["prev_accum_nav"] - 1.0
    df["daily_growth_return"] = df["daily_growth"] / 100.0

    unit_step_mask = (
        df["prev_unit_nav"].notna()
        & (df["prev_unit_nav"] > 0)
        & df["daily_growth_return"].notna()
    )
    accum_step_mask = (
        df["prev_accum_nav"].notna()
        & (df["prev_accum_nav"] > 0)
        & df["daily_growth_return"].notna()
        & df["accum_nav"].notna()
    )

    df["unit_step_diff"] = (df["daily_growth_return"] - df["unit_step_return"]).abs()
    df["accum_step_diff"] = (df["daily_growth_return"] - df["accum_step_return"]).abs()

    df["accum_gap"] = df["accum_nav"] - df["unit_nav"]
    df["prev_accum_gap"] = df["accum_gap"].shift(1)
    df["accum_gap_regression"] = df["prev_accum_gap"] - df["accum_gap"]

    scale_df = df.loc[
        df["unit_nav"].notna()
        & df["accum_nav"].notna()
        & (df["unit_nav"] > 0)
        & (df["accum_nav"] > 0),
        ["unit_nav", "accum_nav"],
    ].copy()
    scale_ratio_series = scale_df["unit_nav"] / scale_df["accum_nav"] if not scale_df.empty else pd.Series(dtype=float)
    ratio_median = _safe_float(scale_ratio_series.median()) if not scale_ratio_series.empty else None
    ratio_p10 = _safe_float(scale_ratio_series.quantile(0.10)) if len(scale_ratio_series) >= 3 else ratio_median
    ratio_p90 = _safe_float(scale_ratio_series.quantile(0.90)) if len(scale_ratio_series) >= 3 else ratio_median
    ratio_band = None
    if ratio_median and ratio_median > 0 and ratio_p10 is not None and ratio_p90 is not None:
        ratio_band = (ratio_p90 - ratio_p10) / ratio_median
    has_unit_accum_scale_mismatch = bool(
        ratio_median
        and ratio_median >= float(unit_accum_scale_mismatch_ratio)
        and ratio_band is not None
        and ratio_band <= float(unit_accum_scale_stability_tolerance)
    )
    accum_nav_reliable = bool(
        _has_reliable_accum_nav_series(
            df,
            accum_gap_regression_tolerance=accum_gap_regression_tolerance,
        )
    )
    use_unit_nav_fallback_checks = bool(
        (not has_unit_accum_scale_mismatch) and (not accum_nav_reliable)
    )
    full_series_daily_like = False
    daily_growth_non_null = pd.to_numeric(df["daily_growth"], errors="coerce").dropna()
    if len(daily_growth_non_null) >= int(min_window_points):
        full_series_daily_like = _estimate_points_per_year(
            len(daily_growth_non_null),
            start_date=df["nav_date"].min(),
            end_date=df["nav_date"].max(),
        ) >= float(min_daily_like_points_per_year)

    gap_regression_mask = (
        df["accum_gap_regression"].notna()
        & (df["accum_gap_regression"] > float(accum_gap_regression_tolerance))
    )
    unit_step_issue_mask = (
        unit_step_mask
        & (df["unit_step_diff"] > float(daily_growth_step_diff_tolerance))
        if use_unit_nav_fallback_checks and full_series_daily_like
        else pd.Series(False, index=df.index)
    )
    accum_step_issue_mask = (
        accum_step_mask
        & (df["accum_step_diff"] > float(daily_growth_step_diff_tolerance))
        if accum_nav_reliable and full_series_daily_like
        else pd.Series(False, index=df.index)
    )

    if has_unit_accum_scale_mismatch:
        gap_regression_mask = pd.Series(False, index=df.index)
        unit_step_issue_mask = pd.Series(False, index=df.index)
        accum_step_issue_mask = pd.Series(False, index=df.index)

    normalized_window_configs = _normalize_window_configs(window_configs)
    latest_nav_date = df["nav_date"].max()
    window_analysis_rows: List[Tuple[str, int, pd.DataFrame, bool, bool]] = []
    for label, days in normalized_window_configs:
        window_df = df.loc[df["nav_date"] >= (latest_nav_date - pd.Timedelta(days=days))].copy()
        if len(window_df) < int(min_window_points):
            window_analysis_rows.append((label, int(days), window_df, False, False))
            continue

        window_accum_nav_reliable = bool(
            _has_reliable_accum_nav_series(
                window_df,
                accum_gap_regression_tolerance=accum_gap_regression_tolerance,
            )
        )
        window_can_use_daily_growth = _can_use_daily_growth_window(
            window_df,
            min_points=int(min_window_points),
            min_daily_growth_coverage_ratio=float(min_daily_growth_coverage_ratio),
            min_daily_like_points_per_year=float(min_daily_like_points_per_year),
        )
        window_analysis_rows.append(
            (
                label,
                int(days),
                window_df,
                window_accum_nav_reliable,
                window_can_use_daily_growth,
            )
        )

    window_records = []
    for label, days, window_df, window_accum_nav_reliable, _window_can_use_daily_growth in window_analysis_rows:
        window_record = _build_window_consistency_record(
            window_df,
            label=label,
            days=days,
            cumulative_diff_tolerance=float(daily_growth_cumulative_diff_tolerance),
            step_diff_tolerance=float(daily_growth_step_diff_tolerance),
            min_points=int(min_window_points),
            min_daily_growth_coverage_ratio=float(min_daily_growth_coverage_ratio),
            min_daily_like_points_per_year=float(min_daily_like_points_per_year),
            use_unit_nav_checks=(not has_unit_accum_scale_mismatch) and (not window_accum_nav_reliable),
            use_accum_nav_checks=window_accum_nav_reliable,
        )
        if window_record is not None:
            window_records.append(window_record)

    issue_keys = set()
    if has_unit_accum_scale_mismatch:
        issue_keys.add("unit_accum_scale_mismatch")
    if bool(gap_regression_mask.any()):
        issue_keys.add("accum_nav_gap_regression")
    if bool(unit_step_issue_mask.any()):
        issue_keys.add("daily_growth_step_conflicts_with_unit_nav")
    if bool(accum_step_issue_mask.any()):
        issue_keys.add("daily_growth_step_conflicts_with_accum_nav")
    for record in window_records:
        for key in record.get("issue_keys") or []:
            issue_keys.add(key)

    current_screening_blocking_window_labels: List[str] = []
    for label, _days, window_df, window_accum_nav_reliable, window_can_use_daily_growth in window_analysis_rows:
        if len(window_df) < int(min_window_points):
            continue
        if (not window_can_use_daily_growth) and (not window_accum_nav_reliable):
            current_screening_blocking_window_labels.append(label)

    top_gap_samples = (
        df.loc[gap_regression_mask, ["nav_date", "prev_accum_gap", "accum_gap", "accum_gap_regression"]]
        .sort_values("accum_gap_regression", ascending=False)
        .head(3)
    )
    top_step_samples = (
        df.loc[unit_step_issue_mask | accum_step_issue_mask, [
            "nav_date",
            "daily_growth_return",
            "unit_step_return",
            "accum_step_return",
            "unit_step_diff",
            "accum_step_diff",
        ]]
        .assign(
            sample_severity=lambda x: x[["unit_step_diff", "accum_step_diff"]].max(axis=1)
        )
        .sort_values("sample_severity", ascending=False)
        .head(3)
    )

    max_window_unit_diff = max(
        (
            _safe_float(item.get("abs_diff_vs_unit_nav")) or 0.0
            for item in window_records
        ),
        default=0.0,
    )
    max_window_accum_diff = max(
        (
            _safe_float(item.get("abs_diff_vs_accum_nav")) or 0.0
            for item in window_records
        ),
        default=0.0,
    )
    max_gap_regression = _safe_float(df["accum_gap_regression"].max()) or 0.0
    review_priority_score = max_gap_regression
    if has_unit_accum_scale_mismatch:
        review_priority_score += 1.0
    elif accum_nav_reliable:
        review_priority_score += max_window_accum_diff
    else:
        review_priority_score += max_window_unit_diff

    actionable_issue_keys, informational_issue_keys = _split_issue_keys(sorted(issue_keys))
    screening_relevant = _is_screening_relevant_record(meta)
    effective_actionable_issue_keys = actionable_issue_keys if screening_relevant else []
    effective_informational_issue_keys = sorted(
        set(informational_issue_keys).union(actionable_issue_keys if not screening_relevant else [])
    )

    return {
        "fcode": str(meta.get("fcode") or ""),
        "shortname": str(meta.get("shortname") or ""),
        "ftype": str(meta.get("ftype") or ""),
        "type_category": str(meta.get("type_category") or ""),
        "holding_days": _safe_int(meta.get("holding_days"), default=0),
        "is_buyable": normalize_buyability(meta.get("is_buyable")),
        "purchase_status": str(meta.get("purchase_status") or ""),
        "screening_relevant": bool(screening_relevant),
        "current_screening_blocking": bool(current_screening_blocking_window_labels),
        "current_screening_blocking_window_labels": current_screening_blocking_window_labels,
        "current_screening_blocking_window_count": int(len(current_screening_blocking_window_labels)),
        "row_count": int(len(df)),
        "first_nav_date": str(df["nav_date"].min().date()),
        "latest_nav_date": str(df["nav_date"].max().date()),
        "issue_keys": sorted(issue_keys),
        "issue_count": int(len(issue_keys)),
        "actionable_issue_keys": effective_actionable_issue_keys,
        "actionable_issue_count": int(len(effective_actionable_issue_keys)),
        "informational_issue_keys": effective_informational_issue_keys,
        "informational_issue_count": int(len(effective_informational_issue_keys)),
        "accum_nav_reliable": bool(accum_nav_reliable),
        "uses_unit_nav_fallback_checks": bool(use_unit_nav_fallback_checks),
        "unit_accum_scale_mismatch": bool(has_unit_accum_scale_mismatch),
        "unit_accum_ratio_median": round(ratio_median, 6) if ratio_median is not None else None,
        "unit_accum_ratio_p10": round(ratio_p10, 6) if ratio_p10 is not None else None,
        "unit_accum_ratio_p90": round(ratio_p90, 6) if ratio_p90 is not None else None,
        "unit_accum_ratio_band": round(ratio_band, 6) if ratio_band is not None else None,
        "max_accum_gap_regression": round(max_gap_regression, 6),
        "accum_gap_regression_count": int(gap_regression_mask.sum()),
        "max_unit_step_diff": round(_safe_float(df["unit_step_diff"].max()) or 0.0, 6),
        "max_accum_step_diff": round(_safe_float(df["accum_step_diff"].max()) or 0.0, 6),
        "daily_growth_unit_step_conflict_count": int(unit_step_issue_mask.sum()),
        "daily_growth_accum_step_conflict_count": int(accum_step_issue_mask.sum()),
        "window_records": window_records,
        "top_accum_gap_regression_samples": [
            {
                "nav_date": str(row["nav_date"].date()),
                "prev_accum_gap": round(_safe_float(row["prev_accum_gap"]) or 0.0, 6),
                "accum_gap": round(_safe_float(row["accum_gap"]) or 0.0, 6),
                "accum_gap_regression": round(_safe_float(row["accum_gap_regression"]) or 0.0, 6),
            }
            for _, row in top_gap_samples.iterrows()
        ],
        "top_daily_growth_conflict_samples": [
            {
                "nav_date": str(row["nav_date"].date()),
                "daily_growth_return": round(_safe_float(row["daily_growth_return"]) or 0.0, 6),
                "unit_step_return": round(_safe_float(row["unit_step_return"]) or 0.0, 6),
                "accum_step_return": round(_safe_float(row["accum_step_return"]) or 0.0, 6),
                "unit_step_diff": round(_safe_float(row["unit_step_diff"]) or 0.0, 6),
                "accum_step_diff": round(_safe_float(row["accum_step_diff"]) or 0.0, 6),
            }
            for _, row in top_step_samples.iterrows()
        ],
        "review_priority_score": round(review_priority_score, 6),
    }


def _iter_grouped_nav_rows(conn) -> Iterable[Tuple[dict, List[dict]]]:
    current_fcode = None
    current_meta: Optional[dict] = None
    current_rows: List[dict] = []

    for row in conn.execute(NAV_HISTORY_CONSISTENCY_AUDIT_SQL):
        record = dict(row)
        fcode = str(record.get("fcode") or "").strip()
        meta = {
            "fcode": fcode,
            "shortname": record.get("shortname"),
            "ftype": record.get("ftype"),
            "type_category": record.get("type_category"),
            "holding_days": record.get("holding_days"),
            "is_buyable": record.get("is_buyable"),
            "purchase_status": record.get("purchase_status"),
        }
        nav_row = {
            "nav_date": record.get("nav_date"),
            "unit_nav": record.get("unit_nav"),
            "accum_nav": record.get("accum_nav"),
            "daily_growth": record.get("daily_growth"),
        }

        if current_fcode is None:
            current_fcode = fcode
            current_meta = meta

        if fcode != current_fcode:
            if current_meta and current_rows:
                yield current_meta, current_rows
            current_fcode = fcode
            current_meta = meta
            current_rows = []

        current_rows.append(nav_row)

    if current_meta and current_rows:
        yield current_meta, current_rows


def build_nav_history_consistency_audit_payload(
    db_path: Optional[str] = None,
    *,
    output_dir: str = DEFAULT_AUDIT_DIR,
    accum_gap_regression_tolerance: float = DEFAULT_ACCUM_GAP_REGRESSION_TOLERANCE,
    daily_growth_cumulative_diff_tolerance: float = DEFAULT_DAILY_GROWTH_CUMULATIVE_DIFF_TOLERANCE,
    daily_growth_step_diff_tolerance: float = DEFAULT_DAILY_GROWTH_STEP_DIFF_TOLERANCE,
    min_window_points: int = DEFAULT_MIN_WINDOW_POINTS,
    min_daily_growth_coverage_ratio: float = DEFAULT_MIN_DAILY_GROWTH_COVERAGE_RATIO,
    min_daily_like_points_per_year: float = DEFAULT_MIN_DAILY_LIKE_POINTS_PER_YEAR,
    unit_accum_scale_mismatch_ratio: float = DEFAULT_UNIT_ACCUM_SCALE_MISMATCH_RATIO,
    unit_accum_scale_stability_tolerance: float = DEFAULT_UNIT_ACCUM_SCALE_STABILITY_TOLERANCE,
    review_row_limit: int = DEFAULT_REVIEW_ROW_LIMIT,
    window_configs: Optional[Sequence[Tuple[str, int]]] = None,
) -> dict:
    resolved_db_path = resolve_db_path(db_path) if db_path else resolve_db_path()
    issue_counter: Counter[str] = Counter()
    actionable_issue_counter: Counter[str] = Counter()
    informational_issue_counter: Counter[str] = Counter()
    window_issue_counter: Counter[str] = Counter()
    review_rows: List[dict] = []
    actionable_review_rows: List[dict] = []
    screening_relevant_review_rows: List[dict] = []
    informational_review_rows: List[dict] = []
    current_screening_blocking_review_rows: List[dict] = []
    current_screening_blocking_window_total = 0
    total_nav_rows = 0
    total_funds = 0

    with open_readonly_db(resolved_db_path) as conn:
        for meta, rows in _iter_grouped_nav_rows(conn):
            total_funds += 1
            total_nav_rows += len(rows)
            record = analyze_fund_nav_history_consistency(
                [{**meta, **row} for row in rows],
                accum_gap_regression_tolerance=accum_gap_regression_tolerance,
                daily_growth_cumulative_diff_tolerance=daily_growth_cumulative_diff_tolerance,
                daily_growth_step_diff_tolerance=daily_growth_step_diff_tolerance,
                min_window_points=min_window_points,
                min_daily_growth_coverage_ratio=min_daily_growth_coverage_ratio,
                min_daily_like_points_per_year=min_daily_like_points_per_year,
                unit_accum_scale_mismatch_ratio=unit_accum_scale_mismatch_ratio,
                unit_accum_scale_stability_tolerance=unit_accum_scale_stability_tolerance,
                window_configs=window_configs,
            )
            if not record:
                continue

            for issue_key in record.get("issue_keys") or []:
                issue_counter[issue_key] += 1
            for issue_key in record.get("actionable_issue_keys") or []:
                actionable_issue_counter[issue_key] += 1
            for issue_key in record.get("informational_issue_keys") or []:
                informational_issue_counter[issue_key] += 1
            for window_record in record.get("window_records") or []:
                for issue_key in window_record.get("issue_keys") or []:
                    window_issue_counter[f"{window_record['label']}:{issue_key}"] += 1

            if record.get("issue_keys"):
                review_rows.append(record)
            if record.get("actionable_issue_keys"):
                actionable_review_rows.append(record)
                if record.get("screening_relevant"):
                    screening_relevant_review_rows.append(record)
            elif record.get("informational_issue_keys"):
                informational_review_rows.append(record)
            if record.get("current_screening_blocking"):
                current_screening_blocking_review_rows.append(record)
                current_screening_blocking_window_total += int(
                    len(record.get("current_screening_blocking_window_labels") or [])
                )

    def _sort_key(item: dict) -> tuple:
        return (
            _safe_float(item.get("review_priority_score")) or 0.0,
            _safe_float(item.get("max_accum_gap_regression")) or 0.0,
            _safe_int(item.get("actionable_issue_count")),
            _safe_int(item.get("issue_count")),
            item.get("fcode") or "",
        )

    review_rows.sort(
        key=_sort_key,
        reverse=True,
    )
    actionable_review_rows.sort(
        key=_sort_key,
        reverse=True,
    )
    screening_relevant_review_rows.sort(
        key=_sort_key,
        reverse=True,
    )
    current_screening_blocking_review_rows.sort(
        key=lambda item: (
            _safe_int(item.get("current_screening_blocking_window_count")),
            _safe_float(item.get("review_priority_score")) or 0.0,
            item.get("fcode") or "",
        ),
        reverse=True,
    )
    informational_review_rows.sort(
        key=lambda item: (
            _safe_int(item.get("informational_issue_count")),
            _safe_float(item.get("review_priority_score")) or 0.0,
            item.get("fcode") or "",
        ),
        reverse=True,
    )

    summary = {
        "bond_fund_count": int(total_funds),
        "bond_nav_row_count": int(total_nav_rows),
        "funds_with_any_issue": int(len(review_rows)),
        "funds_with_actionable_issue": int(len(actionable_review_rows)),
        "funds_with_screening_relevant_actionable_issue": int(len(screening_relevant_review_rows)),
        "funds_with_current_screening_blocking_issue": int(len(current_screening_blocking_review_rows)),
        "windows_with_current_screening_blocking_issue": int(current_screening_blocking_window_total),
        "funds_with_informational_issue_only": int(len(informational_review_rows)),
        "funds_with_unit_accum_scale_mismatch": int(issue_counter.get("unit_accum_scale_mismatch", 0)),
        "funds_with_accum_nav_gap_regression": int(issue_counter.get("accum_nav_gap_regression", 0)),
        "funds_with_daily_growth_unit_step_conflict": int(issue_counter.get("daily_growth_step_conflicts_with_unit_nav", 0)),
        "funds_with_daily_growth_accum_step_conflict": int(issue_counter.get("daily_growth_step_conflicts_with_accum_nav", 0)),
        "funds_with_daily_growth_unit_total_return_conflict": int(issue_counter.get("daily_growth_total_return_conflicts_with_unit_nav", 0)),
        "funds_with_daily_growth_accum_total_return_conflict": int(issue_counter.get("daily_growth_total_return_conflicts_with_accum_nav", 0)),
    }

    findings = []
    if summary["funds_with_unit_accum_scale_mismatch"] > 0:
        findings.append(
            "已识别到一批 `unit_nav` 与 `accum_nav` 长期不在同一数值尺度的样本，"
            "这类基金需要单独治理，不能直接套用普通净值一致性规则。"
        )
    if summary["funds_with_accum_nav_gap_regression"] > 0:
        findings.append(
            "已识别到 `accum_nav - unit_nav` 在历史序列中出现异常回退的基金，"
            "这类样本若继续误用累计净值，会直接污染收益与风险回退口径。"
        )
    if summary["funds_with_actionable_issue"] > 0:
        findings.append(
            "当前更适合作为实施清单高优先项的是 `unit_accum_scale_mismatch / accum_nav_gap_regression / "
            "unit_nav fallback 冲突` 这类会直接影响主链回退判断的异常。"
        )
    if summary["funds_with_current_screening_blocking_issue"] > 0:
        findings.append(
            "当前正式筛选真正仍会被迫退回 `unit_nav` 的样本已单独收敛，"
            "应优先关注 `current_screening_blocking` 集合，而不是全部历史 actionable。"
        )
    if (
        summary["funds_with_daily_growth_unit_total_return_conflict"] > 0
        or summary["funds_with_daily_growth_accum_total_return_conflict"] > 0
    ):
        findings.append(
            "已识别到 `daily_growth` 复利收益与 `unit_nav/accum_nav` 首尾收益显著冲突的基金，"
            "其中大量 `accum_nav` 冲突更适合先作为信息性审计样本保留，不应直接等同于主链高优先阻断项。"
        )
    if not findings:
        findings.append("当前未识别到高于审计阈值的显著净值一致性异常。")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(Path(resolved_db_path).resolve()),
        "output_dir": str(_resolve_output_dir_path(output_dir)),
        "row_source": "fund_nav_history + funds",
        "scope": "bond_and_fixed_income_index_funds",
        "thresholds": {
            "accum_gap_regression_tolerance": float(accum_gap_regression_tolerance),
            "daily_growth_cumulative_diff_tolerance": float(daily_growth_cumulative_diff_tolerance),
            "daily_growth_step_diff_tolerance": float(daily_growth_step_diff_tolerance),
            "min_window_points": int(min_window_points),
            "min_daily_growth_coverage_ratio": float(min_daily_growth_coverage_ratio),
            "min_daily_like_points_per_year": float(min_daily_like_points_per_year),
            "unit_accum_scale_mismatch_ratio": float(unit_accum_scale_mismatch_ratio),
            "unit_accum_scale_stability_tolerance": float(unit_accum_scale_stability_tolerance),
            "windows": [
                {"label": label, "days": int(days)}
                for label, days in _normalize_window_configs(window_configs)
            ],
        },
        "consumers": [
            "core/analyzers/risk_analyzer.py",
            "core/bond_fund_screening/core/risk_calculator.py",
            "core/bond_fund_screening/core/data_loader.py",
            "backend/report_builder.py",
        ],
        "summary": summary,
        "findings": findings,
        "issue_counts": dict(issue_counter),
        "actionable_issue_counts": dict(actionable_issue_counter),
        "informational_issue_counts": dict(informational_issue_counter),
        "window_issue_counts": dict(window_issue_counter),
        "top_review_rows": review_rows[: min(int(review_row_limit), len(review_rows))],
        "top_actionable_review_rows": actionable_review_rows[: min(int(review_row_limit), len(actionable_review_rows))],
        "top_screening_relevant_actionable_review_rows": screening_relevant_review_rows[
            : min(int(review_row_limit), len(screening_relevant_review_rows))
        ],
        "top_current_screening_blocking_review_rows": current_screening_blocking_review_rows[
            : min(int(review_row_limit), len(current_screening_blocking_review_rows))
        ],
        "top_informational_review_rows": informational_review_rows[: min(int(review_row_limit), len(informational_review_rows))],
    }
    return payload


def render_nav_history_consistency_audit_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    thresholds = payload.get("thresholds") or {}
    lines = [
        "# 净值历史一致性审计",
        "",
        f"- 生成时间: `{payload.get('generated_at', '')}`",
        f"- 数据库: `{payload.get('db_path', '')}`",
        f"- 范围: `{payload.get('scope', '')}`",
        "",
        "## 总览",
        "",
        f"- bond_fund_count: `{summary.get('bond_fund_count', 0)}`",
        f"- bond_nav_row_count: `{summary.get('bond_nav_row_count', 0)}`",
        f"- funds_with_any_issue: `{summary.get('funds_with_any_issue', 0)}`",
        f"- funds_with_actionable_issue: `{summary.get('funds_with_actionable_issue', 0)}`",
        f"- funds_with_current_screening_blocking_issue: `{summary.get('funds_with_current_screening_blocking_issue', 0)}`",
        f"- windows_with_current_screening_blocking_issue: `{summary.get('windows_with_current_screening_blocking_issue', 0)}`",
        f"- funds_with_informational_issue_only: `{summary.get('funds_with_informational_issue_only', 0)}`",
        f"- funds_with_unit_accum_scale_mismatch: `{summary.get('funds_with_unit_accum_scale_mismatch', 0)}`",
        f"- funds_with_accum_nav_gap_regression: `{summary.get('funds_with_accum_nav_gap_regression', 0)}`",
        f"- funds_with_daily_growth_unit_total_return_conflict: `{summary.get('funds_with_daily_growth_unit_total_return_conflict', 0)}`",
        f"- funds_with_daily_growth_accum_total_return_conflict: `{summary.get('funds_with_daily_growth_accum_total_return_conflict', 0)}`",
        "",
        "## 阈值",
        "",
        f"- accum_gap_regression_tolerance: `{thresholds.get('accum_gap_regression_tolerance')}`",
        f"- daily_growth_cumulative_diff_tolerance: `{thresholds.get('daily_growth_cumulative_diff_tolerance')}`",
        f"- daily_growth_step_diff_tolerance: `{thresholds.get('daily_growth_step_diff_tolerance')}`",
        f"- min_window_points: `{thresholds.get('min_window_points')}`",
        f"- min_daily_growth_coverage_ratio: `{thresholds.get('min_daily_growth_coverage_ratio')}`",
        f"- min_daily_like_points_per_year: `{thresholds.get('min_daily_like_points_per_year')}`",
        "",
        "## 影响链路",
        "",
    ]
    for item in payload.get("consumers") or []:
        lines.append(f"- `{item}`")

    lines.extend([
        "",
        "## 发现",
        "",
    ])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend([
        "",
        "## 问题分布",
        "",
    ])
    for key, value in sorted((payload.get("issue_counts") or {}).items()):
        lines.append(f"- `{key}`: `{value}`")

    lines.extend([
        "",
        "## 可执行问题分布",
        "",
    ])
    for key, value in sorted((payload.get("actionable_issue_counts") or {}).items()):
        lines.append(f"- `{key}`: `{value}`")

    lines.extend([
        "",
        "## 周期分布",
        "",
    ])
    for key, value in sorted((payload.get("window_issue_counts") or {}).items()):
        lines.append(f"- `{key}`: `{value}`")

    lines.extend([
        "",
        "## 当前筛选阻塞样本",
        "",
    ])
    top_current_rows = payload.get("top_current_screening_blocking_review_rows") or []
    if not top_current_rows:
        lines.append("- 无")
    else:
        for item in top_current_rows[:30]:
            lines.append(
                f"- `{item.get('fcode')}` {item.get('shortname')} | "
                f"windows={','.join(item.get('current_screening_blocking_window_labels') or [])} | "
                f"holding_days={item.get('holding_days')} | buyable={item.get('is_buyable')} | "
                f"purchase_status={item.get('purchase_status')}"
            )

    lines.extend([
        "",
        "## 高优先样本",
        "",
    ])
    top_rows = payload.get("top_actionable_review_rows") or []
    if not top_rows:
        lines.append("- 无")
    else:
        for item in top_rows[:30]:
            lines.append(
                f"- `{item.get('fcode')}` {item.get('shortname')} | "
                f"type={item.get('ftype')} | issues={','.join(item.get('actionable_issue_keys') or [])} | "
                f"max_gap_regression={item.get('max_accum_gap_regression')} | "
                f"latest_nav_date={item.get('latest_nav_date')}"
            )

    return "\n".join(lines).strip() + "\n"


def write_nav_history_consistency_audit_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir_path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_consistency_audit_{ts}.json"
    md_path = artifact_dir / f"nav_history_consistency_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_nav_history_consistency_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    payload = build_nav_history_consistency_audit_payload()
    json_path, md_path = write_nav_history_consistency_audit_artifacts(payload)
    print(json.dumps({
        "json_path": str(json_path),
        "md_path": str(md_path),
        "summary": payload.get("summary"),
        "findings": payload.get("findings"),
    }, ensure_ascii=False, indent=2))
