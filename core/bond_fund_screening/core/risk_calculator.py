#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
风险计算器模块 - RiskCalculator

负责实时计算6个核心风险指标：
1. 夏普比率 (Sharpe Ratio)
2. 索提诺比率 (Sortino Ratio)
3. 卡玛比率 (Calmar Ratio)
4. 最大回撤 (Max Drawdown)
5. 波动率 (Volatility)
6. 收益一致性 (Consistency)
"""

from bond_fund_screening.core.metric_formula_contract import strategy_formula_contract

import numpy as np
import pandas as pd
import math
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from bond_fund_screening.config import (
    RISK_FREE_RATE,
    MAR,
    TRADING_DAYS_PER_YEAR,
)
from bond_fund_screening.core.standard_risk_metrics import (
    calculate_drawdown_stats, calculate_standard_risk_metrics, annualize_compound_return,
    estimate_periods_per_year, calculate_calmar_ratio,
    return_observation_diagnostics,
)
from utils.calculation_capture import capture_call

UNIT_ACCUM_SCALE_MISMATCH_RATIO = 20.0
UNIT_ACCUM_SCALE_STABILITY_TOLERANCE = 0.2
RESOLVED_GAP_REGRESSION_TOLERANCE = 0.05
RESOLVED_GAP_END_TOLERANCE = 0.001
RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE = 0.005


# ============================================================================
# Step 7-6: 月度收益纯函数（从 _calculate_consistency 提取，可独立复用）
# ============================================================================

def compute_monthly_returns_from_daily(
    daily_returns: pd.Series,
    trading_days_per_month: int = 20,
    mode: str = "trading20",
) -> List[float]:
    """
    将日收益率按固定交易日数切分为月度累计收益率。

    默认 ``mode='trading20'`` 保持历史行为（固定 20 日切月，与 _calculate_consistency
    原始实现一致）。新增 ``mode='calendar'`` 按 ``daily_returns.index`` 的自然月
    分组累乘，适合做影子口径对照（P1-05 / B7.3）。

    Args:
        daily_returns: 日收益率序列（小数形式）。calendar 模式要求 index 是
            pandas DatetimeIndex 或可转为时间戳的单调序列。
        trading_days_per_month: 每月交易日数（默认 20，仅 trading20 模式使用）
        mode: ``'trading20'`` 或 ``'calendar'``

    Returns:
        月度累计收益率列表（可能为空）
    """
    if mode == "calendar":
        return compute_monthly_returns_by_calendar(daily_returns)

    n_months = len(daily_returns) // trading_days_per_month
    monthly_returns = []
    for i in range(n_months):
        start_idx = i * trading_days_per_month
        end_idx = start_idx + trading_days_per_month
        month_data = daily_returns.iloc[start_idx:end_idx]
        month_return = (1 + month_data).prod() - 1
        monthly_returns.append(month_return)
    return monthly_returns


def compute_monthly_returns_by_calendar(daily_returns: pd.Series) -> List[float]:
    """按 daily_returns.index 的自然月做累乘（P1-05 影子口径）。

    - 要求 index 是可转 DatetimeIndex 的序列；否则返回空列表让调用方降级。
    - 月末对齐：`.index.to_period('M')` 分组，每月 ``(1+r).prod() - 1``。
    - 首尾残月与 trading20 行为差异是本口径的观察点，由调用方对比并记录。
    """

    if daily_returns is None or len(daily_returns) == 0:
        return []

    idx = daily_returns.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.to_datetime(idx, errors="raise")
        except Exception:
            return []

    series = pd.Series(daily_returns.to_numpy(copy=False), index=idx)
    grouped = series.groupby(idx.to_period("M"))
    return [float((1 + chunk).prod() - 1) for _label, chunk in grouped]


def compute_positive_month_ratio(
    monthly_returns: List[float],
    min_months: int = 2,
) -> Optional[float]:
    """
    计算正收益月份占比。

    Args:
        monthly_returns: 月度累计收益率列表
        min_months: 最少月数（不足则返回 None）

    Returns:
        正收益月份占比（0~1），月数不足返回 None
    """
    if len(monthly_returns) < min_months:
        return None
    positive_count = sum(1 for r in monthly_returns if r > 0)
    return positive_count / len(monthly_returns)


class RiskCalculator:
    """
    风险计算器类

    提供6个核心风险指标的计算方法
    """

    def __init__(
        self,
        risk_free_rate: Optional[float] = None,
        mar: float = MAR,
        trading_days_per_year: int = TRADING_DAYS_PER_YEAR
    ):
        """
        初始化风险计算器

        Args:
            risk_free_rate: 无风险利率（年化）。None 时按 runtime_overrides → env →
                config.RISK_FREE_RATE 的优先级解析，支持季度审视后运维只动
                runtime overrides 文件而不动核心 config.py（见 P1-03 / B7.1）。
            mar: 最低可接受回报（年化）
            trading_days_per_year: 年交易日数
        """
        if risk_free_rate is None:
            # 延迟 import 避免循环依赖与 core 初始加载开销
            from bond_fund_screening.runtime_overrides import get_risk_free_rate

            resolution = get_risk_free_rate(RISK_FREE_RATE)
            self.risk_free_rate = float(resolution.value)
            self.risk_free_rate_source = resolution.source
            self.risk_free_rate_updated_at = resolution.updated_at
        else:
            self.risk_free_rate = float(risk_free_rate)
            self.risk_free_rate_source = "explicit"
            self.risk_free_rate_updated_at = None
        self.mar = mar
        self.trading_days_per_year = trading_days_per_year

    def _annualize_std(self, daily_std: float, annualization_sqrt: Optional[float] = None) -> float:
        """将日频标准差年化。"""
        if annualization_sqrt is None:
            annualization_sqrt = np.sqrt(self.trading_days_per_year)
        return daily_std * annualization_sqrt

    def _calculate_sharpe_from_std(
        self,
        daily_std: float,
        annual_return: float,
        annualization_sqrt: Optional[float] = None,
    ) -> Optional[float]:
        """基于已计算的日收益标准差计算夏普比率。"""
        volatility = self._annualize_std(daily_std, annualization_sqrt)

        # v1.9.57 P1-6: 使用 EPSILON 替代精确零比较，避免浮点溢出
        if abs(volatility) < 1e-10:
            return None

        sharpe_ratio = (annual_return - self.risk_free_rate) / volatility
        return sharpe_ratio

    def _calculate_sortino_from_values(
        self,
        daily_returns_values: np.ndarray,
        annual_return: float,
        daily_std: float,
        annualization_sqrt: Optional[float] = None,
    ) -> Optional[float]:
        """基于已计算的日收益数组与标准差计算索提诺比率。"""
        if len(daily_returns_values) < 2:
            return None

        if annualization_sqrt is None:
            annualization_sqrt = np.sqrt(self.trading_days_per_year)

        # v1.9.58 P1-1: 使用全周期半方差（Sortino & Price 1994 标准定义）
        # 将 MAR 转换为日频（年化 MAR / 252）
        daily_mar = self.mar / self.trading_days_per_year
        downside_diff = np.minimum(daily_returns_values - daily_mar, 0.0)
        downside_variance = float(np.mean(downside_diff * downside_diff))  # 分母为 N（全部天数）

        if downside_variance < 1e-20:
            return None

        downside_std = np.sqrt(downside_variance) * annualization_sqrt

        # v1.9.57 P1-6: 使用 EPSILON 替代精确零比较
        if abs(downside_std) < 1e-10:
            return None

        sortino_ratio = (annual_return - self.mar) / downside_std
        return sortino_ratio

    def _calculate_monthly_metrics_from_values(
        self,
        daily_returns_values: np.ndarray,
        trading_days_per_month: int = 20,
    ) -> Tuple[Optional[float], Optional[float]]:
        """一次计算一致性与正收益月占比，避免重复切月。"""
        if len(daily_returns_values) < trading_days_per_month * 2:
            return None, None

        n_months = len(daily_returns_values) // trading_days_per_month
        if n_months < 2:
            return None, None

        trimmed = daily_returns_values[: n_months * trading_days_per_month]
        monthly_returns = np.prod(
            1.0 + trimmed.reshape(n_months, trading_days_per_month),
            axis=1,
        ) - 1.0

        monthly_std = float(np.std(monthly_returns, ddof=1))
        consistency = monthly_std * np.sqrt(12)
        positive_month_ratio = float(np.count_nonzero(monthly_returns > 0) / len(monthly_returns))
        return consistency, positive_month_ratio

    def _calculate_drawdown_stats_from_values(
        self,
        nav_values: np.ndarray,
    ) -> Tuple[Optional[float], Optional[int]]:
        """一次计算最大回撤与最长连续水下观测数。"""
        return calculate_drawdown_stats(nav_values)

    @capture_call('screening_risk.metrics', attributes=(
        'risk_free_rate', 'risk_free_rate_source', 'risk_free_rate_updated_at', 'mar', 'trading_days_per_year',
    ))
    def calculate_all_metrics(
        self,
        nav_data: pd.DataFrame,
        period_years: float = 1.0,
        style_tag: Optional[str] = None,
    ) -> Dict[str, object]:
        """
        计算所有6个风险指标

        Args:
            nav_data: 历史净值数据，包含列：nav_date, unit_nav, accum_nav (可选), daily_growth
            period_years: 分析周期（年），传递给 _select_nav_column
                         - 所有周期统一优先使用 accum_nav（累计净值）
                         - 仅在 accum_nav 不可用时回退到 unit_nav

        Returns:
            包含6个指标的字典，如果数据不足则相应指标为None
        """
        empty_metrics = {
            'sharpe_ratio': None,
            'sortino_ratio': None,
            'raw_sharpe_ratio': None,
            'raw_sortino_ratio': None,
            'sharpe_ratio_cap_hit': None,
            'sortino_ratio_cap_hit': None,
            'undefined_metric_reasons': {
                'sharpe_ratio': 'invalid_observations',
                'sortino_ratio': 'invalid_observations',
                'max_drawdown_duration': 'invalid_observations',
            },
            'calmar_ratio': None,
            'max_drawdown': None,
            'volatility': None,
            'consistency': None,
            'positive_month_ratio': None,
            'max_drawdown_duration': None,
            'formula_contract': None,
            'calmar_style_tag': str(style_tag or "").strip() or None,
            'calmar_min_drawdown_floor': None,
            'calmar_min_drawdown_floor_source': None,
            'calmar_effective_drawdown': None,
        }

        if len(nav_data) < 3:  # 至少需要3个数据点（支持超短周期如一周期）
            return empty_metrics
        if nav_data['nav_date'].isna().any() or nav_data['nav_date'].duplicated().any():
            return empty_metrics

        # v1.9.82: Layer6 批量净值查询已按 nav_date 升序返回。
        # 对已排序数据跳过 sort/reset，可减少实时风险计算的固定开销；
        # 若调用方传入乱序数据，仍保留原有排序兜底。
        if not nav_data['nav_date'].is_monotonic_increasing:
            nav_data = nav_data.sort_values('nav_date').reset_index(drop=True)

        # 优先使用 daily_growth 列计算日收益率
        # daily_growth 由API直接提供（已消除分红除息影响），精度高于 pct_change(accum_nav)
        # accum_nav 仅保留4位小数，在低波动率基金中会产生大量零值，导致波动率偏低
        use_daily_growth = False
        nav_column = None
        if self._can_use_daily_growth_series(nav_data):
            # daily_growth[0] 是周期首日相对于周期外前一日的涨跌幅
            # 必须先按窗口位置排除首点，再去除缺失值；不能多丢掉首个有效收益。
            dg = pd.to_numeric(nav_data['daily_growth'].iloc[1:], errors='coerce')
            daily_returns = dg / 100  # daily_growth 是百分比形式，转为小数
            use_daily_growth = True

        if not use_daily_growth:
            # 回退：使用净值列的 pct_change。
            # P1-04 / B7.2: 先做一次局部 accum_nav 补齐，避免分红样本单点 NaN 导致整段
            # fallback 到 unit_nav。补齐失败的整段才 fallback。
            nav_data_filled, _gap_fill_count = self._gap_fill_accum_nav(nav_data)
            nav_data = nav_data_filled
            nav_column = self._select_nav_column(nav_data_filled, period_years)
            if nav_column not in nav_data_filled:
                return empty_metrics
            nav_values = pd.to_numeric(nav_data_filled[nav_column], errors='coerce')
            if not np.isfinite(nav_values).all() or nav_values.le(0).any():
                return empty_metrics
            daily_returns = nav_data_filled[nav_column].pct_change().dropna()

        if len(daily_returns) < 2:
            return empty_metrics

        daily_returns_values = daily_returns.to_numpy(copy=False)

        # 计算年化收益率
        days = (nav_data.iloc[-1]['nav_date'] - nav_data.iloc[0]['nav_date']).days

        if use_daily_growth:
            # 基于 daily_growth 复利计算总收益率（更精确）
            total_return = (1 + daily_returns).prod() - 1
        else:
            start_nav = nav_data.iloc[0][nav_column]
            end_nav = nav_data.iloc[-1][nav_column]
            if start_nav == 0:
                total_return = 0
            else:
                total_return = (end_nav / start_nav) - 1

        annual_return = annualize_compound_return(float(total_return), days)
        if annual_return is None:
            return empty_metrics
        periods_per_year = estimate_periods_per_year(len(daily_returns), days / 365.25,
                                                   self.trading_days_per_year)

        # 基于 daily_growth 重建累计净值序列用于回撤相关指标计算
        if use_daily_growth:
            # daily_returns 的第一项表示窗口首个净值点之后的首段收益。必须前置
            # 窗口起点 1.0，否则首段即下跌时 cumprod 会从下跌后的净值起算，
            # running max 看不到窗口起点并把这段真实回撤漏掉。
            cumulative_nav = np.concatenate(
                ([1.0], np.cumprod(1.0 + daily_returns_values))
            )

        # 计算各指标（复用日收益标准差、月度切分、回撤统计）
        common = calculate_standard_risk_metrics(
            daily_returns_values,
            cumulative_nav if use_daily_growth else nav_data[nav_column].dropna().to_numpy(copy=False),
            annual_return=annual_return,
            periods_per_year=periods_per_year,
            risk_free_rate=self.risk_free_rate,
            mar=self.mar,
        )
        raw_sharpe = common['raw_sharpe_ratio']
        raw_sortino = common['raw_sortino_ratio']
        sharpe = common['sharpe_ratio']
        sortino = common['sortino_ratio']
        max_dd = common['max_drawdown']
        dd_duration = common['max_drawdown_duration']
        calmar_floor, calmar_floor_source = self._resolve_calmar_min_drawdown(style_tag)
        calmar_effective_drawdown = (
            max(abs(max_dd), calmar_floor) if max_dd is not None and not pd.isna(max_dd) else None
        )
        calmar = self._calculate_calmar_from_values(annual_return, max_dd, style_tag=style_tag)
        volatility = common['volatility']
        consistency, pos_month_ratio = self._calculate_monthly_metrics_from_values(daily_returns_values)

        # v1.9.60 P0-1: 统一清洗 NaN/Inf → None，防止泄漏到下游
        def _sanitize(value):
            if value is None:
                return None
            try:
                if math.isnan(value) or math.isinf(value):
                    return None
            except (TypeError, ValueError):
                return None
            return value

        return {
            'sharpe_ratio': _sanitize(round(sharpe, 4) if sharpe is not None else None),
            'sortino_ratio': _sanitize(round(sortino, 4) if sortino is not None else None),
            'raw_sharpe_ratio': _sanitize(round(raw_sharpe, 4) if raw_sharpe is not None else None),
            'raw_sortino_ratio': _sanitize(round(raw_sortino, 4) if raw_sortino is not None else None),
            'sharpe_ratio_cap_hit': common['sharpe_ratio_cap_hit'],
            'sortino_ratio_cap_hit': None if raw_sortino is None else abs(raw_sortino) > 10.0,
            'undefined_metric_reasons': dict(common['undefined_metric_reasons']),
            'calmar_ratio': _sanitize(round(calmar, 4) if calmar is not None else None),
            'standard_calmar_ratio': calculate_calmar_ratio(annual_return, max_dd),
            'observation_diagnostics': return_observation_diagnostics(daily_returns_values),
            'max_drawdown': _sanitize(round(max_dd, 4) if max_dd is not None else None),
            'volatility': _sanitize(round(volatility, 4) if volatility is not None else None),
            'consistency': _sanitize(round(consistency, 4) if consistency is not None else None),
            'positive_month_ratio': _sanitize(round(pos_month_ratio, 4) if pos_month_ratio is not None else None),
            'max_drawdown_duration': _sanitize(dd_duration),
            'formula_contract': strategy_formula_contract(
                engine='screening', periods_per_year=periods_per_year,
                risk_free_rate=self.risk_free_rate, mar=self.mar,
                return_source='daily_growth' if use_daily_growth else nav_column,
                verified=bool(use_daily_growth and 'daily_growth' in nav_data
                              and pd.to_numeric(nav_data['daily_growth'].iloc[1:], errors='coerce').notna().all()),
                annual_return_source='local_compound_return', calmar_floor=calmar_floor),
            'calmar_style_tag': str(style_tag or "").strip() or None,
            'calmar_min_drawdown_floor': _sanitize(calmar_floor),
            'calmar_min_drawdown_floor_source': calmar_floor_source,
            'calmar_effective_drawdown': _sanitize(
                round(calmar_effective_drawdown, 6) if calmar_effective_drawdown is not None else None
            ),
        }

    def _select_nav_column(self, nav_data: pd.DataFrame, period_years: float) -> str:
        """
        选择使用的净值列

        无完整日增长序列时优先使用关系校验通过的累计净值，再回退单位净值。
        累计净值比不等于分红再投资总回报；单位净值会受除息影响。两种回退
        均在 formula_contract 中标记来源且不声称总回报观测已验证。

        P1-04 / B7.2: 原先要求 `accum_nav.notna().all()` 且全部 >0，**单点 NaN 即整段**
        回退 unit_nav，让分红基金的风险指标失真。现在由 ``_gap_fill_accum_nav`` 在本
        方法之前做局部补齐：少量 NaN/零值用 ``unit_nav * adjust_factor`` 或前值累乘
        ``daily_growth`` 补出后，仍优先走 accum_nav；整段缺失才 fallback 到 unit_nav。

        Args:
            nav_data: 净值数据
            period_years: 分析周期（保留参数，当前未使用）

        Returns:
            净值列名 ('unit_nav' 或 'accum_nav')
        """
        if 'accum_nav' in nav_data.columns:
            accum_valid = nav_data['accum_nav'].notna().all() and (nav_data['accum_nav'] > 0).all()
            if accum_valid and self._has_reliable_accum_nav_series(nav_data):
                return 'accum_nav'
        return 'unit_nav'

    def _can_use_daily_growth_series(self, nav_data: pd.DataFrame) -> bool:
        """仅在样本密度接近日频时才直接信任 daily_growth。"""
        if nav_data is None or getattr(nav_data, "empty", True):
            return False
        if 'daily_growth' not in nav_data.columns or 'nav_date' not in nav_data.columns:
            return False

        dg = pd.to_numeric(nav_data['daily_growth'].iloc[1:], errors='coerce')
        if len(dg) < 10 or not np.isfinite(dg).all() or dg.le(-100).any():
            return False

        nav_dates = pd.to_datetime(nav_data['nav_date'], errors='coerce')
        if nav_dates.isna().any():
            return False

        span_days = max(int((nav_dates.iloc[-1] - nav_dates.iloc[0]).days), 1)
        actual_years = max(span_days / 365.25, 1 / 365.25)
        estimated_points_per_year = len(dg) / actual_years
        minimum_daily_like_points = min(float(self.trading_days_per_year), 180.0)
        return estimated_points_per_year >= minimum_daily_like_points

    @staticmethod
    def _has_reliable_accum_nav_series(nav_data: pd.DataFrame, *, regression_tolerance: float = 0.01) -> bool:
        """累计净值只有在与单位净值关系稳定时才可作为回退真值。"""
        if nav_data is None or getattr(nav_data, "empty", True):
            return False
        if 'accum_nav' not in nav_data.columns:
            return False

        accum = pd.to_numeric(nav_data['accum_nav'], errors='coerce')
        if accum.isna().any() or (accum <= 0).any():
            return False

        if 'unit_nav' not in nav_data.columns:
            return True

        unit = pd.to_numeric(nav_data['unit_nav'], errors='coerce')
        if unit.isna().any() or (unit <= 0).any():
            return True

        if RiskCalculator._has_stable_unit_accum_scale_mismatch(unit, accum):
            # ETF/LOF 等样本可能长期使用百元尺度累计净值；这类结构性尺度差异不应被误判为
            # accum_nav 不可靠，否则主链会错误回退到 unit_nav，放大收益/风险失真。
            return True

        dividend_gap = accum - unit
        if dividend_gap.isna().any() or (dividend_gap < -1e-6).any():
            return False

        max_regression = float((dividend_gap.cummax() - dividend_gap).max() or 0.0)
        if max_regression <= regression_tolerance:
            return True

        return RiskCalculator._has_resolved_small_gap_regression(
            unit,
            accum,
            dividend_gap=dividend_gap,
            max_regression=max_regression,
            regression_tolerance=regression_tolerance,
        )

    @staticmethod
    def _has_stable_unit_accum_scale_mismatch(
        unit: pd.Series,
        accum: pd.Series,
        *,
        mismatch_ratio: float = UNIT_ACCUM_SCALE_MISMATCH_RATIO,
        stability_tolerance: float = UNIT_ACCUM_SCALE_STABILITY_TOLERANCE,
    ) -> bool:
        scale_df = pd.DataFrame({"unit_nav": unit, "accum_nav": accum}).dropna()
        if scale_df.empty:
            return False

        scale_df = scale_df[(scale_df["unit_nav"] > 0) & (scale_df["accum_nav"] > 0)]
        if scale_df.empty:
            return False

        ratio_series = scale_df["unit_nav"] / scale_df["accum_nav"]
        ratio_median = float(ratio_series.median()) if not ratio_series.empty else None
        if ratio_median is None or ratio_median <= 0:
            return False

        if len(ratio_series) >= 3:
            ratio_p10 = float(ratio_series.quantile(0.10))
            ratio_p90 = float(ratio_series.quantile(0.90))
        else:
            ratio_p10 = ratio_median
            ratio_p90 = ratio_median

        ratio_band = (ratio_p90 - ratio_p10) / ratio_median if ratio_median > 0 else None
        return bool(
            ratio_median >= float(mismatch_ratio)
            and ratio_band is not None
            and ratio_band <= float(stability_tolerance)
        )

    @staticmethod
    def _has_resolved_small_gap_regression(
        unit: pd.Series,
        accum: pd.Series,
        *,
        dividend_gap: pd.Series,
        max_regression: float,
        regression_tolerance: float,
        resolved_regression_tolerance: float = RESOLVED_GAP_REGRESSION_TOLERANCE,
        resolved_gap_end_tolerance: float = RESOLVED_GAP_END_TOLERANCE,
        total_return_diff_tolerance: float = RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE,
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

    @staticmethod
    def _gap_fill_accum_nav(nav_data: pd.DataFrame) -> "tuple[pd.DataFrame, int]":
        """P1-04 / B7.2: 对 ``accum_nav`` 的局部空/零做保守补齐。

        补齐规则（按优先级）：
        1. 若当点 ``adjust_factor`` 存在且有效，用 ``unit_nav * adjust_factor``。
        2. 否则用前一点已知 accum_nav 配合 ``daily_growth`` 累乘：
           ``prev_accum * (1 + daily_growth/100)``。
        3. 以上都算不出来的点保持 NaN，由 ``_select_nav_column`` 整段 fallback。

        返回 (修改后的 DataFrame 副本, 实际补齐的点数)。
        """
        if nav_data is None or getattr(nav_data, "empty", True):
            return nav_data, 0
        if 'accum_nav' not in nav_data.columns:
            return nav_data, 0

        working = nav_data.copy()
        initial_missing_mask = working['accum_nav'].isna() | (working['accum_nav'] <= 0)
        initial_missing_count = int(initial_missing_mask.sum())
        if initial_missing_count == 0:
            return working, 0

        if 'adjust_factor' in working.columns and 'unit_nav' in working.columns:
            adj = working['adjust_factor']
            unit = working['unit_nav']
            candidate = unit * adj
            fillable = initial_missing_mask & adj.notna() & (adj > 0) & unit.notna() & (unit > 0)
            working.loc[fillable, 'accum_nav'] = candidate[fillable]

        remaining_mask = working['accum_nav'].isna() | (working['accum_nav'] <= 0)
        if bool(remaining_mask.any()) and 'daily_growth' in working.columns:
            dg_values = working['daily_growth'].tolist()
            values = working['accum_nav'].tolist()
            last_value: float | None = None
            for idx in range(len(values)):
                v = values[idx]
                if pd.notna(v) and v > 0:
                    last_value = float(v)
                    continue
                if last_value is None:
                    continue
                try:
                    growth = float(dg_values[idx])
                except (TypeError, ValueError):
                    continue
                if pd.isna(growth):
                    continue
                new_value = last_value * (1.0 + growth / 100.0)
                if new_value > 0:
                    values[idx] = new_value
                    last_value = new_value
            working['accum_nav'] = values

        final_missing_mask = working['accum_nav'].isna() | (working['accum_nav'] <= 0)
        filled_count = initial_missing_count - int(final_missing_mask.sum())
        return working, max(0, filled_count)

    def _calculate_calmar_from_values(
        self,
        annual_return: float,
        max_drawdown: Optional[float],
        style_tag: Optional[str] = None,
    ) -> Optional[float]:
        """
        从已计算的值计算卡玛比率（避免重复计算max_drawdown）

        Args:
            annual_return: 年化收益率
            max_drawdown: 最大回撤
            style_tag: 债基风格标签（``中短债 / 现金增强 / 综合纯债 / ...``）。
                若提供，Calmar 分母下限按 ``config.CALMAR_MIN_DD_BY_STYLE``
                分风格取值；否则回退 default=0.005（与历史行为一致）。

        Returns:
            卡玛比率

        v1.9.69 P3: 使用 0.5% 作为回撤下限，保留低回撤基金区分度。
        低于 0.005 的回撤实质是统计噪声（债基日常波动 0.3-0.5%），
        无下限会导致所有"低回撤"基金集中 cap 到同分。

        P1-06 / B7.4: style_tag 扩展后，中短债 / 现金增强这类低波动风格使用 0.002
        下限，扩大 Calmar 风格内区分度；其他风格保持 0.005。
        """
        if max_drawdown is None or pd.isna(max_drawdown):
            return None
        min_dd, _source = self._resolve_calmar_min_drawdown(style_tag)
        return calculate_calmar_ratio(annual_return, max_drawdown,
                                     risk_free_rate=self.risk_free_rate, floor=min_dd, cap=20.0)

    @staticmethod
    def _resolve_calmar_min_drawdown(style_tag: Optional[str] = None) -> Tuple[float, str]:
        """解析 Calmar 分母下限与来源，供主指标和审计字段共用。"""
        try:
            from bond_fund_screening.config import CALMAR_MIN_DD_BY_STYLE
        except Exception:  # pragma: no cover - 兼容直接脚本运行
            try:
                from config import CALMAR_MIN_DD_BY_STYLE  # type: ignore
            except Exception:  # pragma: no cover - 保底 0.005
                CALMAR_MIN_DD_BY_STYLE = {"default": 0.005}

        normalized_style = str(style_tag or "").strip()
        if normalized_style and normalized_style in CALMAR_MIN_DD_BY_STYLE:
            return float(CALMAR_MIN_DD_BY_STYLE[normalized_style]), "style"
        return float(CALMAR_MIN_DD_BY_STYLE.get("default", 0.005)), "default"

    def _calculate_sharpe(
        self,
        daily_returns: pd.Series,
        annual_return: float
    ) -> Optional[float]:
        """
        计算夏普比率

        公式：(年化收益率 - 无风险利率) / 年化波动率

        Args:
            daily_returns: 日收益率序列
            annual_return: 年化收益率

        Returns:
            夏普比率
        """
        if len(daily_returns) < 2:
            return None

        return self._calculate_sharpe_from_std(
            float(np.std(daily_returns.to_numpy(copy=False), ddof=1)),
            annual_return,
        )

    def _calculate_sortino(
        self,
        daily_returns: pd.Series,
        annual_return: float
    ) -> Optional[float]:
        """
        计算索提诺比率

        v1.9.58 P1-1: 采用 Sortino & Price (1994) 标准定义
        公式：(年化收益率 - MAR) / 下行偏差
        下行偏差 DD = sqrt( (1/N) * Σ min(0, Ri - MAR)^2 )  # 对所有 i=1..N

        Args:
            daily_returns: 日收益率序列
            annual_return: 年化收益率

        Returns:
            索提诺比率
        """
        if len(daily_returns) < 2:
            return None

        return self._calculate_sortino_from_values(
            daily_returns.to_numpy(copy=False),
            annual_return,
            float(np.std(daily_returns.to_numpy(copy=False), ddof=1)),
        )

    def _calculate_max_drawdown(
        self,
        nav_data: pd.DataFrame,
        nav_column: str = 'unit_nav'
    ) -> Optional[float]:
        """
        计算最大回撤

        公式：max((峰值净值 - 当前净值) / 峰值净值)

        Args:
            nav_data: 历史净值数据，包含unit_nav或accum_nav列
            nav_column: 使用的净值列名（unit_nav或accum_nav）

        Returns:
            最大回撤（**正数小数**，如0.0837表示8.37%的回撤）

        修复说明：
            统一为小数形式，与其他指标（volatility、consistency）保持一致
            旧版返回百分比（8.37），新版返回小数（0.0837）
        """
        if len(nav_data) < 2:
            return None

        # 前置条件：nav_data 已由 calculate_all_metrics 按 nav_date 排序
        nav_series = nav_data[nav_column]
        if nav_series.isna().any():
            # 保持历史 NaN 处理口径不变
            cumulative_max = nav_series.cummax()
            drawdown = (nav_series - cumulative_max) / cumulative_max
            return abs(drawdown.min())

        max_drawdown, _ = self._calculate_drawdown_stats_from_values(nav_series.to_numpy(copy=False))
        return max_drawdown

    def _calculate_max_drawdown_duration(
        self,
        nav_data: pd.DataFrame,
        nav_column: str = 'unit_nav'
    ) -> Optional[int]:
        """
        计算最长连续水下观测数，包括期末尚未恢复的区间。

        净值严格低于此前峰值时累计，达到或超过峰值时归零。
        没有净值观测的日期不累计；该值不是自然天数或承诺恢复时间。

        Args:
            nav_data: 净值数据
            nav_column: 净值列名

        Returns:
            最长连续水下观测数，数据不足返回 None
        """
        if len(nav_data) < 2:
            return None

        nav_series = nav_data[nav_column].dropna().values
        if len(nav_series) < 2:
            return None
        _, max_duration = self._calculate_drawdown_stats_from_values(nav_series)
        return max_duration

    def _calculate_volatility(
        self,
        daily_returns: pd.Series
    ) -> Optional[float]:
        """
        计算波动率（年化标准差）

        公式：std(日收益率) × √252

        Args:
            daily_returns: 日收益率序列

        Returns:
            年化波动率
        """
        if len(daily_returns) < 2:
            return None

        return self._annualize_std(float(np.std(daily_returns.to_numpy(copy=False), ddof=1)))

    def _calculate_consistency(
        self,
        daily_returns: pd.Series
    ) -> Optional[float]:
        """
        计算收益一致性 - 月度收益的标准差（年化）

        定义：将日收益率按月分组，计算每月的累计收益率，
              然后计算月度收益的标准差并年化。

        **值越小表示收益越稳定一致**（负向指标）

        Args:
            daily_returns: 日收益率序列（小数形式，如0.001表示0.1%）

        Returns:
            年化的月度收益标准差（小数形式）
        """
        # 不足 40 个交易日无法稳定切出 2 个月度窗口，直接返回 None
        if len(daily_returns) < 40:
            return None

        # Step 7-6: 复用提取的纯函数
        monthly_returns = compute_monthly_returns_from_daily(daily_returns)

        if len(monthly_returns) < 2:
            return None

        monthly_std = float(np.std(monthly_returns, ddof=1))
        annualized_std = monthly_std * np.sqrt(12)
        return annualized_std

    def calculate_median_metrics(
        self,
        metrics_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        计算各风险指标的中位数

        Args:
            metrics_df: 包含风险指标的DataFrame

        Returns:
            各指标中位数的字典
        """
        median_dict = {}

        for metric in ['sharpe_ratio', 'sortino_ratio', 'calmar_ratio',
                       'max_drawdown', 'volatility', 'consistency']:
            if metric in metrics_df.columns:
                median_dict[metric] = metrics_df[metric].median()

        return median_dict

    def check_metrics_pass(
        self,
        metrics: Dict[str, Optional[float]],
        medians: Dict[str, float],
        min_pass_count: int = 4,
        excluded_metrics: Optional[List[str]] = None
    ) -> Tuple[bool, int]:
        """
        检查风险指标是否通过筛选

        条件：至少4/6个指标优于中位数

        Args:
            metrics: 基金的风险指标字典
            medians: 各指标的中位数字典
            min_pass_count: 最少通过指标数（默认4）
            excluded_metrics: 需要排除的指标列表（如短周期排除consistency），
                             排除后调整分母和最低通过数

        Returns:
            (是否通过, 通过指标数)
        """
        if excluded_metrics is None:
            excluded_metrics = []

        pass_count = 0
        total_metrics = 0

        # 正向指标：值越大越好
        positive_metrics = ['sharpe_ratio', 'sortino_ratio', 'calmar_ratio']
        for metric in positive_metrics:
            if metric in excluded_metrics:
                continue
            # v1.9.58 P0-2: None 值指标不计入分母，避免通过率被人为压低
            # v1.9.60 P0-1: 同时检查 NaN（round(np.nan,4) 返回 nan，不是 None）
            if metric not in metrics or metrics[metric] is None or (isinstance(metrics[metric], float) and math.isnan(metrics[metric])):
                continue
            total_metrics += 1
            if metric in medians and metrics[metric] >= medians[metric]:
                pass_count += 1

        # 负向指标：值越小越好（最大回撤、波动率、一致性）
        negative_metrics = ['max_drawdown', 'volatility', 'consistency']
        for metric in negative_metrics:
            if metric in excluded_metrics:
                continue
            # v1.9.58 P0-2: None 值指标不计入分母
            # v1.9.60 P0-1: 同时检查 NaN
            if metric not in metrics or metrics[metric] is None or (isinstance(metrics[metric], float) and math.isnan(metrics[metric])):
                continue
            total_metrics += 1
            if metric in medians and metrics[metric] <= medians[metric]:
                pass_count += 1

        # v1.9.57 P0-4: 排除指标后保持原始通过比例（向上取整避免标准降低）
        # 原始 4/6=67%，排除1个后应为 ceil(0.67*5)=4，而非 round(0.67*5)=3
        if excluded_metrics:
            target_ratio = min_pass_count / 6.0
            adjusted_pass_count = max(1, math.ceil(target_ratio * total_metrics))
        else:
            adjusted_pass_count = min_pass_count

        is_pass = pass_count >= adjusted_pass_count
        return is_pass, pass_count
