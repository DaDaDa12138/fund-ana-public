#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""净值观测窗口质量判断。

该模块只负责日期层的数据质量语义，不计算基金收益或风险指标。数据库风险引擎
与实时 Layer6 共用同一套“自然日覆盖期”定义，避免两个入口各自实现后发生漂移。
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd


def assess_nav_observation_quality(
    nav_dates: Iterable[Any],
    *,
    period_days: int,
    coverage_grace_days: int = 7,
    required_observation_points: Optional[int] = None,
    max_gap_threshold_days: Optional[int] = None,
    window_start: Any = None,
    window_end: Any = None,
    max_boundary_age_days: Optional[int] = None,
) -> dict[str, Any]:
    """汇总净值窗口的覆盖期、观测密度和最大自然日缺口。

    ``required_coverage_days = max(period_days - coverage_grace_days, 1)``
    与数据库风险引擎既有周期覆盖语义一致。观测点门槛和最大缺口门槛由调用方
    传入，从而保留不同风险入口现有的周期配置。
    """

    raw_dates = pd.Series(list(nav_dates), dtype="object")
    parsed_dates = pd.to_datetime(raw_dates, errors="coerce")
    invalid_date_count = int(parsed_dates.isna().sum())
    valid_dates = parsed_dates.dropna().sort_values()
    duplicate_date_count = int(valid_dates.duplicated().sum())
    unique_dates = valid_dates.drop_duplicates()

    observation_count = int(len(unique_dates))
    if observation_count >= 2:
        coverage_days = int((unique_dates.iloc[-1] - unique_dates.iloc[0]).days)
        gaps = unique_dates.diff().dropna().dt.days
        max_gap_days = int(gaps.max()) if not gaps.empty else 0
    else:
        coverage_days = 0
        max_gap_days = 9999

    normalized_period_days = max(int(period_days), 1)
    normalized_grace_days = max(int(coverage_grace_days), 0)
    required_coverage_days = max(normalized_period_days - normalized_grace_days, 1)

    density_sufficient = (
        True
        if required_observation_points is None
        else observation_count >= max(int(required_observation_points), 1)
    )
    # A predecessor supplies a return baseline, but time before the requested
    # window is not an observation gap inside that window. Keep its raw gap
    # for audit and bound the predecessor's age separately. No holiday dates
    # or synthetic observations are inferred here.
    continuity_max_gap_days = max_gap_days
    boundary_age_days = None
    boundary_sufficient = True
    if (window_start is None) != (window_end is None):
        raise ValueError("both NAV continuity window boundaries are required")
    if window_start is not None:
        start = pd.Timestamp(window_start).normalize()
        end = pd.Timestamp(window_end).normalize()
        if pd.isna(start) or pd.isna(end) or start >= end:
            raise ValueError("invalid NAV continuity window")
        eligible = unique_dates[unique_dates <= end]
        before = eligible[eligible <= start]
        if not before.empty:
            boundary_age_days = int((start - before.iloc[-1]).days)
            boundary_sufficient = (
                max_boundary_age_days is None
                or boundary_age_days <= max(int(max_boundary_age_days), 0)
            )
        in_window = eligible[eligible > start]
        checkpoints = [start, *in_window.tolist(), end]
        continuity_max_gap_days = max(
            int((right - left).days)
            for left, right in zip(checkpoints, checkpoints[1:])
        )
    continuity_sufficient = boundary_sufficient and (
        True
        if max_gap_threshold_days is None
        else continuity_max_gap_days <= max(int(max_gap_threshold_days), 0)
    )

    return {
        "observation_count": observation_count,
        "invalid_date_count": invalid_date_count,
        "duplicate_date_count": duplicate_date_count,
        "coverage_days": coverage_days,
        "required_coverage_days": required_coverage_days,
        "coverage_grace_days": normalized_grace_days,
        "coverage_sufficient": bool(coverage_days >= required_coverage_days),
        "required_observation_points": (
            None
            if required_observation_points is None
            else max(int(required_observation_points), 1)
        ),
        "observation_density_sufficient": bool(density_sufficient),
        "max_gap_days": max_gap_days,
        "continuity_max_gap_days": continuity_max_gap_days,
        "continuity_basis": (
            "requested_window_natural_days_v2" if window_start is not None
            else "observation_span_natural_days_v1"
        ),
        "boundary_age_days": boundary_age_days,
        "boundary_sufficient": bool(boundary_sufficient),
        "max_boundary_age_days": max_boundary_age_days,
        "max_gap_threshold_days": (
            None
            if max_gap_threshold_days is None
            else max(int(max_gap_threshold_days), 0)
        ),
        "continuity_sufficient": bool(continuity_sufficient),
    }
