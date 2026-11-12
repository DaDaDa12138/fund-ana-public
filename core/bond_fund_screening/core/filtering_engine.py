#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选引擎模块 - FilteringEngine

实现10层渐进式筛选架构：
1. 第1层：基础资格筛选
2. 第2层：流动性筛选
3. 第3层：基础成熟度筛选
4. 第4层：周期适配筛选
5. 第5层：规模分层筛选
6. 第6层：风险指标筛选（实时计算）
7. 第7层：异常值筛选
8. 第8层：业绩持续性筛选
9. 第9层：多维度交叉验证
10. 第10层：最终精选

支持通过Profile配置不同基金类型的筛选阈值
增强版：支持筛选决策追踪（可解释性）
"""

import pandas as pd
import numpy as np
import logging
import json
import os
import math
from datetime import date as date_type
from typing import Any, Dict, Tuple, List, Optional, Union
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.calculation_capture import capture_call
from bond_fund_screening.core.nav_window import select_nav_period_window
from utils.holding_period_policy import combine_holding_period_names, holding_period_gate_reason

_CAPTURE_POLICY_FIELDS = (
    'period_years', 'period_days', 'fund_type', 'runtime_fund_type', 'as_of_date',
    'freshness_strategy_override', 'disable_bond_short_term_one_year_observability_gate',
    'is_short_term', 'holding_keywords', 'min_establish_days', 'min_scale_baseline',
    'max_annual_return', 'min_annual_return', 'max_drawdown_threshold',
    'consistency_pass_count', 'min_absolute_return', 'risk_metric_min_funds',
    'max_holding_days', 'risk_pass_count', 'scale_threshold',
)

# tqdm是可选依赖，用于显示进度条
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

from bond_fund_screening.config import (
    MAX_HOLDING_DAYS,
    MAX_HOLDING_DAYS_SHORT_TERM,
    LONG_HOLDING_KEYWORDS,
    MIN_ESTABLISH_DAYS,
    MIN_SCALE_BASELINE,
    SCALE_THRESHOLDS,
    SCALE_THRESHOLDS_SHORT_TERM,
    RISK_METRIC_PASS_COUNT,
    RISK_METRIC_PASS_COUNT_SHORT_TERM,
    RISK_METRIC_MIN_FUNDS,
    RISK_METRIC_QC,
    MAX_ANNUAL_RETURN,
    MIN_ANNUAL_RETURN,
    MAX_PERIOD_RETURN_SHORT_TERM,
    CONSISTENCY_PASS_COUNT,
    TOP_DIMENSION_RATIO,
    MIN_TOP_DIMENSIONS,
    DEFAULT_TOP_N,
    RISK_CALC_THREADS,
    MIN_ABSOLUTE_RETURN,
    MIN_ABSOLUTE_RETURN_SHORT_TERM,
    MAX_DRAWDOWN_THRESHOLD,
    # Step 3: 分桶配置
    BUCKETING_ENABLED,
    BUCKET_KEY_FIELD,
    BUCKET_MIN_SAMPLES,
    BUCKET_FALLBACK_TO_GLOBAL,
    FINAL_RANKING_MODE,
    # Step 4: 回撤阈值与持续性窗口配置
    SHORT_TERM_MODE_THRESHOLD_DAYS,
    DRAWDOWN_THRESHOLDS,
    SHADOW_DRAWDOWN_THRESHOLDS,
    CONSISTENCY_RETURN_COLS,
    get_consistency_return_cols,
    TRADING_DAYS_PER_YEAR,
)
from bond_fund_screening.core.risk_calculator import RiskCalculator
from bond_fund_screening.core.nav_observation_quality import assess_nav_observation_quality
from bond_fund_screening.core.data_loader import DataLoader
from bond_fund_screening.core.scoring_engine import canonicalize_runtime_fund_type_key


CALMAR_STYLE_TAG_COLUMNS = (
    '债基风格标签',
    'style_tag',
    'bond_style_tag',
    '风格标签',
)

# 导入追踪器（可选）
try:
    from bond_fund_screening.core.screening_tracker import (
        ScreeningTracker,
        get_config_snapshot,
        get_runtime_dependency_snapshot,
        get_runtime_context_snapshot,
        normalize_decision_persist_mode,
    )
    HAS_TRACKER = True
except ImportError:
    HAS_TRACKER = False
    ScreeningTracker = None
    def normalize_decision_persist_mode(mode):
        return 'all'

# 导入Profile支持
sys.path.append(str(Path(__file__).parent.parent.parent))
try:
    from config.fund_type_registry import FundTypeProfile, get_profile
    HAS_PROFILE = True
except ImportError:
    FundTypeProfile = None
    get_profile = None
    HAS_PROFILE = False


def stable_sort_desc(
    df: pd.DataFrame,
    primary_col: str,
    tie_break_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    按主排序列降序、代码列升序做稳定排序，避免同分基金顺序漂移。
    """
    if primary_col not in df.columns:
        return df.copy()

    sort_cols = [primary_col]
    ascending = [False]

    for col in (tie_break_cols or ['fcode', '基金代码']):
        if col in df.columns and col not in sort_cols:
            sort_cols.append(col)
            ascending.append(True)

    return df.sort_values(by=sort_cols, ascending=ascending, kind='mergesort')


class FilteringEngine:
    """
    筛选引擎类

    负责执行10层渐进式筛选
    增强版：支持筛选决策追踪
    """

    def __init__(
        self,
        period_years: float,
        data_loader: DataLoader,
        verbose: bool = True,
        profile: Optional['FundTypeProfile'] = None,
        enable_tracking: bool = True,
        fund_type: str = 'bond',
        as_of_date: Optional[date_type] = None,
        freshness_strategy_override: Optional[str] = None,
        disable_bond_short_term_one_year_observability_gate: bool = False,
    ):
        """
        初始化筛选引擎

        Args:
            period_years: 分析周期（年，可以是小数如0.02表示7天）
            data_loader: 数据加载器实例
            verbose: 是否打印详细信息
            profile: 基金类型配置Profile（可选，用于自定义阈值）
            enable_tracking: 是否启用决策追踪
            fund_type: 基金类型（bond/stock/hybrid）
            as_of_date: 回测截断日期（date对象），默认None=使用最新数据
            freshness_strategy_override: 新鲜度策略运行时覆写（soft/hard）
            disable_bond_short_term_one_year_observability_gate:
                是否关闭债基短周期 1Y 可观测性正式门槛。
                仅供“独立观察输出”影子链路复用，正式主链必须保持关闭状态为 False。
        """
        self.period_years = period_years
        self.period_days = round(period_years * 365)
        self.data_loader = data_loader
        self.verbose = verbose
        self.profile = profile
        self.fund_type = fund_type
        self.runtime_fund_type = canonicalize_runtime_fund_type_key(fund_type, default='bond')
        self.as_of_date = as_of_date  # Step 7-1: 回测截断日期
        self.freshness_strategy_override = freshness_strategy_override
        self.disable_bond_short_term_one_year_observability_gate = bool(
            disable_bond_short_term_one_year_observability_gate
        )
        self.decision_persist_mode = normalize_decision_persist_mode(
            os.getenv('SCREENING_DECISION_PERSIST_MODE')
        )
        self.risk_calculator = RiskCalculator()
        self.layer4_one_year_observable_blocked_df = pd.DataFrame()
        self.layer4_one_year_observable_blocked_summary = {}

        # 判断分析类型（短期 vs 长期）
        self.is_short_term = self.period_days <= SHORT_TERM_MODE_THRESHOLD_DAYS

        # 从Profile或默认配置加载阈值
        self._load_thresholds()

        # 筛选统计
        self.stats = {
            'layer_stats': [],  # 每层统计
            'total_start': 0,
            'total_end': 0,
        }

        # 初始化追踪器
        self.enable_tracking = enable_tracking and HAS_TRACKER
        self.tracker: Optional[ScreeningTracker] = None
        if self.enable_tracking:
            profile_name = self._resolve_profile_name(profile)
            self.tracker = ScreeningTracker(
                period_years=period_years,
                fund_type=self.runtime_fund_type,
                profile_name=profile_name,
                decision_persist_mode=self.decision_persist_mode,
            )

    @staticmethod
    def _resolve_profile_name(profile: Optional['FundTypeProfile']) -> Optional[str]:
        """将 profile 实例解析为稳定的常量名，避免 tracking 元数据长期为空。"""
        if profile is None:
            return None

        explicit_name = getattr(profile, 'name', None)
        if explicit_name:
            return str(explicit_name)

        fund_type_value = getattr(getattr(profile, 'fund_type', None), 'value', None)
        profile_name_map = {
            '31': 'BOND_PROFILE',
            '32': 'BOND_FIXED_PROFILE',
            '25': 'STOCK_PROFILE',
            '27': 'HYBRID_PROFILE',
            '26': 'INDEX_PROFILE',
            '35': 'MONEY_PROFILE',
            '36': 'QDII_PROFILE',
            '37': 'FOF_PROFILE',
        }
        return profile_name_map.get(str(fund_type_value)) if fund_type_value is not None else None

    def _load_thresholds(self):
        """从Profile或默认配置加载阈值"""
        def get_closest_value(config_dict, key, default):
            if key in config_dict:
                return config_dict[key]
            closest_key = min(config_dict.keys(), key=lambda x: abs(x - key))
            if abs(closest_key - key) < 0.01:
                return config_dict[closest_key]
            return default

        if self.profile is not None:
            # 使用Profile配置
            self.holding_keywords = self.profile.holding_keywords
            self.min_establish_days = self.profile.min_establish_days
            self.min_scale_baseline = self.profile.min_scale_baseline
            self.max_annual_return = self.profile.max_annual_return
            self.min_annual_return = self.profile.min_annual_return
            self.max_drawdown_threshold = self.profile.max_drawdown_threshold
            self.consistency_pass_count = self.profile.consistency_pass_count
            self.min_absolute_return = self.profile.min_absolute_return
            self.risk_metric_pass_count_base = self.profile.risk_metric_pass_count
            self.risk_metric_min_funds = self.profile.risk_metric_min_funds

            # 根据周期获取规模阈值
            if self.is_short_term:
                # 短周期：使用Profile的min_scale_baseline作为Layer 3的基础门槛
                # Layer 5的规模阈值应高于Layer 3，以实现真正的规模分层
                # 策略：取Profile.min_scale_baseline * 1.5作为Layer 5阈值，确保分层有效
                profile_baseline = self.profile.min_scale_baseline
                self.scale_threshold = profile_baseline * 1.5
                # Layer 3基础门槛保持较低，让更多基金进入后续筛选
                self.min_scale_baseline = profile_baseline * 0.8
            else:
                period_int = int(self.period_years)
                self.scale_threshold = self.profile.scale_thresholds.get(period_int, 1.0)

            # 短期周期调整
            if self.is_short_term:
                self.max_holding_days = self.period_days  # 短期持有期 = 周期天数
                self.risk_pass_count = get_closest_value(
                    RISK_METRIC_PASS_COUNT_SHORT_TERM,
                    self.period_years,
                    max(3, self.risk_metric_pass_count_base - 1),
                )
                merged_min_absolute_return = dict(MIN_ABSOLUTE_RETURN_SHORT_TERM)
                if isinstance(self.profile.min_absolute_return, dict):
                    merged_min_absolute_return.update(self.profile.min_absolute_return)
                self.min_absolute_return = merged_min_absolute_return
            else:
                self.max_holding_days = self.profile.max_holding_days
                self.risk_pass_count = self.risk_metric_pass_count_base
        else:
            # 使用默认配置（向后兼容）
            self.holding_keywords = LONG_HOLDING_KEYWORDS
            self.min_establish_days = MIN_ESTABLISH_DAYS
            self.min_scale_baseline = MIN_SCALE_BASELINE
            self.max_annual_return = MAX_ANNUAL_RETURN
            self.min_annual_return = MIN_ANNUAL_RETURN
            self.max_drawdown_threshold = MAX_DRAWDOWN_THRESHOLD
            self.consistency_pass_count = CONSISTENCY_PASS_COUNT
            self.min_absolute_return = MIN_ABSOLUTE_RETURN
            self.risk_metric_min_funds = RISK_METRIC_MIN_FUNDS

            # 动态选择阈值（根据分析类型）
            if self.is_short_term:
                self.max_holding_days = get_closest_value(MAX_HOLDING_DAYS_SHORT_TERM, self.period_years, 30)
                self.risk_pass_count = get_closest_value(RISK_METRIC_PASS_COUNT_SHORT_TERM, self.period_years, 3)
                self.min_absolute_return = MIN_ABSOLUTE_RETURN_SHORT_TERM

                # 短周期Layer 5规模阈值：取SCALE_THRESHOLDS_SHORT_TERM值，确保高于Layer 3
                short_term_scale = get_closest_value(SCALE_THRESHOLDS_SHORT_TERM, self.period_years, 0.5)
                self.scale_threshold = max(short_term_scale, MIN_SCALE_BASELINE * 1.5)
                # Layer 3基础门槛保持较低
                self.min_scale_baseline = min(short_term_scale, MIN_SCALE_BASELINE)
            else:
                self.max_holding_days = MAX_HOLDING_DAYS
                self.scale_threshold = SCALE_THRESHOLDS.get(int(self.period_years), 1.0)
                self.risk_pass_count = RISK_METRIC_PASS_COUNT

    def _compute_bucket_medians(
        self,
        df: pd.DataFrame,
        value_cols: List[str],
        bucket_col: str = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
        """
        计算分桶中位数统计

        Args:
            df: 包含数据的DataFrame，必须有 bucket_col 列
            value_cols: 需要计算中位数的列名列表
            bucket_col: 分桶键列名，默认使用配置的 BUCKET_KEY_FIELD

        Returns:
            (bucket_medians_df, global_medians_series, bucket_sizes_dict)
            - bucket_medians_df: 每个bucket的中位数，index为bucket_key
            - global_medians_series: 全局中位数
            - bucket_sizes_dict: 每个bucket的样本数
        """
        def _safe_median(series: pd.Series):
            """避免全空序列触发 RuntimeWarning: Mean of empty slice。"""
            cleaned = series.dropna()
            if cleaned.empty:
                return np.nan
            return cleaned.median()

        if not value_cols:
            return pd.DataFrame(), pd.Series(dtype=float), {}

        if bucket_col is None:
            bucket_col = BUCKET_KEY_FIELD

        # 确保 bucket_col 存在
        if bucket_col not in df.columns:
            # 返回空结果，调用方会降级为全局比较
            global_medians = pd.Series(
                {col: _safe_median(df[col]) for col in value_cols},
                index=value_cols,
                dtype=float
            )
            return pd.DataFrame(), global_medians, {}

        # 计算全局中位数（用于 fallback）
        global_medians = pd.Series(
            {col: _safe_median(df[col]) for col in value_cols},
            index=value_cols,
            dtype=float
        )

        # 计算每个 bucket 的样本数
        bucket_sizes = df.groupby(bucket_col).size().to_dict()

        # 计算每个 bucket 的中位数
        bucket_medians = df.groupby(bucket_col)[value_cols].agg(_safe_median)

        return bucket_medians, global_medians, bucket_sizes

    def _get_bucket_info_for_row(
        self,
        row_bucket_key: str,
        bucket_medians: pd.DataFrame,
        global_medians: pd.Series,
        bucket_sizes: Dict[str, int],
        value_cols: List[str]
    ) -> Tuple[Dict[str, float], int, bool]:
        """
        获取单行数据对应的 bucket 中位数信息

        Args:
            row_bucket_key: 该行的 bucket key (ftype)
            bucket_medians: bucket 中位数 DataFrame
            global_medians: 全局中位数 Series
            bucket_sizes: bucket 样本数字典
            value_cols: 指标列名列表

        Returns:
            (medians_dict, bucket_size, is_fallback)
            - medians_dict: 该行应使用的中位数字典
            - bucket_size: bucket 样本数
            - is_fallback: 是否降级为全局比较
        """
        bucket_size = bucket_sizes.get(row_bucket_key, 0)
        is_fallback = False

        if not BUCKETING_ENABLED:
            # 分桶未启用，使用全局中位数
            medians_dict = global_medians[value_cols].to_dict()
            is_fallback = True
        elif bucket_size < BUCKET_MIN_SAMPLES:
            # 样本不足
            if BUCKET_FALLBACK_TO_GLOBAL:
                # 降级为全局中位数
                medians_dict = global_medians[value_cols].to_dict()
                is_fallback = True
            else:
                # 不降级，使用 bucket 自身中位数（可能不可靠）
                if row_bucket_key in bucket_medians.index:
                    medians_dict = bucket_medians.loc[row_bucket_key, value_cols].to_dict()
                else:
                    medians_dict = global_medians[value_cols].to_dict()
                    is_fallback = True
        else:
            # 正常使用 bucket 中位数
            if row_bucket_key in bucket_medians.index:
                medians_dict = bucket_medians.loc[row_bucket_key, value_cols].to_dict()
            else:
                medians_dict = global_medians[value_cols].to_dict()
                is_fallback = True

        return medians_dict, bucket_size, is_fallback

    @capture_call('screening.layers', attributes=_CAPTURE_POLICY_FIELDS)
    def run_all_layers(
        self,
        df: pd.DataFrame,
        apply_layer10: bool = True,
        layer10_sort_col: Optional[str] = None,
        layer10_top_n: Optional[int] = None,
        defer_tracker_finish: bool = False,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        执行所有10层筛选

        Args:
            df: 输入的基金DataFrame
            apply_layer10: 是否执行第10层最终精选
            layer10_sort_col: 第10层排序字段（None 时自动回退）
            layer10_top_n: 第10层输出数量上限（None 时默认不截断）
            defer_tracker_finish: 是否延迟 run 完成标记到上层总流程

        Returns:
            (筛选后的DataFrame, 统计信息字典)
        """
        self.stats['total_start'] = len(df)

        # 启动追踪器
        if self.tracker:
            freshness_override_snapshot = None
            if self.freshness_strategy_override:
                freshness_override_snapshot = {'strategy': self.freshness_strategy_override}
            config_snapshot = get_config_snapshot(
                freshness_config_override=freshness_override_snapshot
            ) if HAS_TRACKER else {}
            config_snapshot['decision_persist_mode'] = self.decision_persist_mode
            config_snapshot.update(get_runtime_context_snapshot())
            config_snapshot.update(get_runtime_dependency_snapshot())
            self.tracker.start_run(fund_count_in=len(df), config_snapshot=config_snapshot)
            tracker_run_id = self.tracker.get_persisted_run_id()
            tracking_skip_context = self.tracker.get_persistence_skip_context()
            self.stats['tracking'] = {
                'persisted': bool(tracker_run_id),
                'run_id': tracker_run_id,
                'skip_reason': tracking_skip_context.get('reason'),
                'matched_run_id': tracking_skip_context.get('matched_run_id'),
            }
        else:
            tracker_run_id = None
            tracking_skip_context = {}

        if self.verbose:
            print(f"\n{'='*80}")
            print(f"开始10层渐进式筛选 ({self.period_years}年期)")
            print(f"初始基金池: {len(df):,} 只")
            if tracker_run_id:
                print(f"追踪ID: {tracker_run_id[:8]}...")
            elif tracking_skip_context.get('reason'):
                matched_run_id = tracking_skip_context.get('matched_run_id')
                matched_run_label = f", matched={matched_run_id[:8]}..." if matched_run_id else ""
                print(f"追踪落库: 已跳过 ({tracking_skip_context['reason']}{matched_run_label})")
            print(f"{'='*80}")

        try:
            # 第1层：基础资格筛选
            df = self._layer1_basic_filter(df)

            # 第2层：流动性筛选
            df = self._layer2_holding_period_filter(df)

            # 第3层：基础成熟度筛选
            df = self._layer3_maturity_filter(df)

            # 第4层：周期适配筛选
            df = self._layer4_period_filter(df)

            # 第5层：规模分层筛选
            df = self._layer5_scale_filter(df)

            # v1.9.60 P1-4: 费率筛选（Layer 5.5）
            df = self._layer5_5_fee_rate_filter(df)

            # 第6层：风险指标筛选（实时计算）
            # 注意: Layer 6 内部已合并原 Layer 7(异常值) 和 Layer 8(持续性) 的筛选逻辑
            df = self._layer6_risk_metrics_filter(df)

            # 第9层：多维度交叉验证（需要评分数据，暂时跳过）
            # df = self._layer9_cross_validation_filter(df)

            # 第10层：最终精选（可由上层在评分后再执行）
            if apply_layer10:
                df = self._layer10_final_selection(
                    df,
                    sort_col=layer10_sort_col,
                    top_n=layer10_top_n,
                )

            self.stats['total_end'] = len(df)

            # 完成追踪
            if self.tracker and not defer_tracker_finish:
                self.tracker.finish_run(fund_count_pass=len(df), success=True)

            if self.verbose:
                print(f"\n{'='*80}")
                print(f"筛选完成！")
                print(f"最终输出: {len(df):,} 只基金")
                print(f"总筛除率: {(1 - len(df)/self.stats['total_start'])*100:.1f}%")
                if self.tracker:
                    print(f"追踪ID: {self.tracker.run_id}")
                print(f"{'='*80}\n")

            return df, self.stats

        except Exception as e:
            # 记录失败
            if self.tracker:
                self.tracker.finish_run(fund_count_pass=0, success=False, fail_reason=str(e))
            raise

    def _get_layer4_min_establish_requirement(self) -> Tuple[int, str]:
        """统一计算第4层最低成立时长门槛，避免上下游诊断口径再次漂移。"""
        if self.period_years < 1:
            min_establish_days = max(self.period_days * 3, 180)
            return min_establish_days, f"{min_establish_days}天（{self.period_days}天×3）"

        period_years_int = round(self.period_years)
        if period_years_int <= 2:
            min_establish_years = 1
        elif period_years_int == 3:
            min_establish_years = 2
        else:
            min_establish_years = 3

        min_establish_days = min_establish_years * 365
        return min_establish_days, f"{min_establish_years}年（{min_establish_days}天）"

    def _needs_bond_short_term_one_year_observability_gate(self) -> bool:
        """债券短期链路必须具备1年可观测基础，才能进入后续投资主榜。"""
        return (
            self.runtime_fund_type == 'bond'
            and self.is_short_term
            and not self.disable_bond_short_term_one_year_observability_gate
        )

    def _get_bond_short_term_one_year_observability_mask(
        self,
        df: pd.DataFrame,
    ) -> Tuple[pd.Series, Dict[str, int]]:
        """
        为债券短期链路计算“1年可观测性”门槛。

        规则：
        - 成立满365天
        - `return_1y` 非空

        这是最终主榜/累积跟踪关键指标（1年收益、夏普、最大回撤）的前置必要条件。
        """
        establish_days = self._coerce_numeric_series(df, 'establish_days')
        return_1y = self._coerce_numeric_series(df, 'return_1y')

        establish_ready = establish_days >= 365
        return_ready = return_1y.notna()
        mask = establish_ready & return_ready

        blocked_mask = ~mask
        metadata = {
            'one_year_observable_required_min_establish_days': 365,
            'one_year_observable_blocked_total': int(blocked_mask.sum()),
            'one_year_observable_under_365_days': int((~establish_ready).sum()),
            'one_year_observable_missing_return_1y': int((~return_ready).sum()),
            'one_year_observable_missing_return_1y_with_establish_ready': int(
                (establish_ready & (~return_ready)).sum()
            ),
        }
        return mask, metadata

    def _coerce_numeric_series(self, df: pd.DataFrame, column: str) -> pd.Series:
        """将列统一转为与输入同索引长度的数值序列；缺列时返回全空序列。"""
        if column in df.columns:
            return pd.to_numeric(df[column], errors='coerce')
        return pd.Series([np.nan] * len(df), index=df.index, dtype='float64')

    def _get_layer6_min_nav_points(self) -> int:
        """
        计算 Layer6 风险计算所需的最小净值点数。

        说明：
        - `period_days` 是日历天，但净值序列通常按交易日更新；
        - 若直接使用 `period_days * 0.8`，1年期会被错误要求约292个点，
          超过真实日净值基金常见的 240~252 个交易日覆盖；
        - 因此先换算为预期交易日点数，再应用 80% 覆盖率门槛。
        """
        expected_trading_days = max(
            1,
            round(self.period_days * TRADING_DAYS_PER_YEAR / 365),
        )
        coverage_required_points = math.ceil(expected_trading_days * 0.8)
        min_metric_points = 3 if self.period_days <= 7 else 10
        return max(min_metric_points, coverage_required_points)

    def _get_layer6_max_gap_threshold_days(self) -> int:
        """返回 Layer6 既有的周期化最大自然日缺口门槛。"""
        return min(45, max(self.period_days // 3, 7))

    @staticmethod
    def _get_layer6_period_coverage_grace_days() -> int:
        """复用数据库风险引擎的默认自然日覆盖容差。"""
        return 7

    def _get_layer6_start_boundary_lookback_days(self) -> int:
        """为目标窗口起点补取最近一个可用净值点。"""
        return max(
            self._get_layer6_period_coverage_grace_days(),
            self._get_layer6_max_gap_threshold_days(),
        )

    @capture_call('screening_risk.window', attributes=('period_years', 'period_days', 'as_of_date'))
    def _prepare_layer6_nav_window(
        self,
        nav_data,
        window_end_date: Optional[date_type],
    ) -> Tuple[object, Dict[str, object]]:
        """保留正式窗口及起点之前最近一个净值点。

        基金净值只在交易日发布。若目标起点落在春节等连续休市区间，直接从
        ``end_date - period_days`` 查询会丢掉起点基准净值，并把首个交易日的
        ``daily_growth`` 当作窗口外收益删除。这里仅补入最近一个起点基准点；
        更早的查询上下文不会进入风险计算。
        """
        return select_nav_period_window(
            nav_data,
            period_days=self.period_days,
            window_end_date=window_end_date,
            boundary_lookback_days=self._get_layer6_start_boundary_lookback_days(),
        )

    def _layer1_basic_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第1层：基础资格筛选

        条件：
        - 有单位净值数据
        - 有主周期收益率数据
        - Step 5: 数据新鲜度检查（strategy='soft'仅标记，'hard'淘汰）
        """
        from bond_fund_screening.config import FRESHNESS_CONFIG

        layer_name = "第1层：基础资格筛选"
        step_id = 1
        start_count = len(df)

        freshness_strategy = self.freshness_strategy_override or FRESHNESS_CONFIG.get('strategy', 'soft')

        # 筛选条件
        has_nav = df['latest_nav'].notna() & (df['latest_nav'] > 0)

        # 对于4年期，DB无return_4y字段，用成立时间替代收益率检查
        # 四年收益必须有完整真实历史，缺失不能通过 Layer 1。
        if self.period_years == 4:
            has_return = (df['establish_days'] >= self.period_days) & df['main_return'].notna()
        else:
            has_return = df['main_return'].notna()

        # Step 5: 新鲜度条件
        has_freshness_cols = 'is_fresh' in df.columns
        if has_freshness_cols and freshness_strategy == 'hard':
            is_fresh = df['is_fresh'].fillna(False)
        else:
            # soft 模式或无新鲜度列：不额外淘汰
            is_fresh = pd.Series([True] * len(df), index=df.index)

        mask = has_nav & has_return & is_fresh

        # 记录决策
        if self.tracker:
            # Step 5: 构建新鲜度 metrics 快照
            freshness_metrics_cache = {}
            if has_freshness_cols:
                for idx, row in df.iterrows():
                    freshness_metrics_cache[idx] = {
                        'nav_latest_date': str(row.get('nav_latest_date', '')) if pd.notna(row.get('nav_latest_date')) else None,
                        'nav_age_days': int(row['nav_age_days']) if pd.notna(row.get('nav_age_days')) else None,
                        'is_fresh': bool(row.get('is_fresh', False)),
                        'freshness_reason': row.get('freshness_reason', 'N/A'),
                        'max_nav_age_days': FRESHNESS_CONFIG['max_nav_age_days'],
                        'skip_weekends': FRESHNESS_CONFIG['skip_weekends'],
                        'strategy': freshness_strategy,
                        'shadow_age_fail_7d': bool(row.get('shadow_age_fail_7d', False)),
                        'shadow_age_fail_14d': bool(row.get('shadow_age_fail_14d', False)),
                        'shadow_age_fail_30d': bool(row.get('shadow_age_fail_30d', False)),
                    }

            for idx, row in df.iterrows():
                fcode = row['fcode']
                metrics = freshness_metrics_cache.get(idx)

                if not has_nav.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='NO_NAV_DATA',
                        reason_detail=f"latest_nav={row.get('latest_nav', 'N/A')}",
                        metrics=metrics
                    )
                elif not has_return.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='NO_RETURN_DATA',
                        reason_detail=f"main_return={row.get('main_return', 'N/A')}",
                        metrics=metrics
                    )
                elif has_freshness_cols and freshness_strategy == 'hard' and not is_fresh.loc[idx]:
                    # Step 5 hard 模式：淘汰不新鲜的基金
                    reason = row.get('freshness_reason', 'NAV_TOO_OLD')
                    # 映射到标准 reason_code
                    if reason == 'NO_NAV_HISTORY':
                        reason_code = 'NO_NAV_HISTORY'
                    else:
                        reason_code = 'NAV_TOO_OLD'
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code=reason_code,
                        reason_detail=(
                            f"nav_latest_date={row.get('nav_latest_date', 'N/A')}, "
                            f"nav_age_days={row.get('nav_age_days', 'N/A')}, "
                            f"max_nav_age_days={FRESHNESS_CONFIG['max_nav_age_days']}, "
                            f"skip_weekends={FRESHNESS_CONFIG['skip_weekends']}"
                        ),
                        metrics=metrics
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED',
                        metrics=metrics
                    )

        df_filtered = df[mask].copy()

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        min_establish_days, min_establish_label = self._get_layer4_min_establish_requirement()
        establish_days = pd.to_numeric(df.get('establish_days'), errors='coerce')
        missing_return = df['main_return'].isna()

        under_full_period_blocked = 0
        under_full_period_missing_return = 0
        if self.period_days > min_establish_days:
            under_full_period_blocked = int(
                (
                    (establish_days >= min_establish_days)
                    & (establish_days < self.period_days)
                    & (~has_return)
                ).sum()
            )
            under_full_period_missing_return = int(
                (
                    (establish_days >= min_establish_days)
                    & (establish_days < self.period_days)
                    & missing_return
                ).sum()
            )

        older_but_missing_return = int(
            ((establish_days >= self.period_days) & missing_return).sum()
        )

        layer1_extra_info = (
            f"完整周期收益门控: 第4层最低门槛≥{min_establish_label}, "
            f"完整分析周期={self.period_days}天, "
            f"未满完整周期而被主收益门控挡掉={under_full_period_blocked}只"
            f"（其中主收益缺失={under_full_period_missing_return}只）, "
            f"已满完整周期但仍缺少主收益={older_but_missing_return}只"
        )
        layer1_metadata = {
            'layer4_min_establish_days': int(min_establish_days),
            'layer4_min_establish_label': min_establish_label,
            'full_period_days': int(self.period_days),
            'under_full_period_blocked': under_full_period_blocked,
            'under_full_period_missing_return': under_full_period_missing_return,
            'older_but_missing_return': older_but_missing_return,
        }

        self._record_layer_stats(
            layer_name,
            start_count,
            end_count,
            filtered_count,
            filter_rate,
            extra_info=layer1_extra_info,
            metadata=layer1_metadata,
        )

        # Step 5C: 记录新鲜度汇总（在 Layer1 输入数据上统计，不受筛选影响）
        if self.tracker and has_freshness_cols:
            self.tracker.record_freshness_summary(df)

        return df_filtered

    def _layer2_holding_period_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第2层：流动性筛选（持有期筛选）

        条件：无持有期限制 OR 持有期 ≤ 动态阈值
        """
        layer_name = "第2层：流动性筛选"
        step_id = 2
        start_count = len(df)

        name_columns = [name for name in ('shortname', 'fullname', 'fund_name', '基金名称') if name in df]
        name_values = (
            df[name_columns].apply(lambda row: combine_holding_period_names(*row), axis=1)
            if name_columns else pd.Series('', index=df.index)
        )
        reasons = df.apply(
            lambda row: holding_period_gate_reason(
                has_holding_period=row.get('has_holding_period'),
                holding_days=row.get('holding_days'),
                holding_period_source=row.get('holding_period_source'),
                holding_keywords=row.get('holding_keywords'),
                threshold_days=self.max_holding_days,
                fund_name=name_values.loc[row.name],
            ),
            axis=1,
        )
        mask = reasons.isna()

        # 记录决策
        if self.tracker:
            for idx, row in df.iterrows():
                fcode = row['fcode']
                if reasons.loc[idx] == 'holding_period_limit':
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='HOLDING_PERIOD_TOO_LONG',
                        reason_detail=f"holding_days={row.get('holding_days')} or name term > threshold={self.max_holding_days}"
                    )
                elif reasons.loc[idx] == 'holding_days_unknown':
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='HOLDING_PERIOD_UNKNOWN',
                        reason_detail='holding days and usable fund name unavailable or conflicting'
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED'
                    )

        df_filtered = df[mask].copy()

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        self._record_layer_stats(layer_name, start_count, end_count, filtered_count, filter_rate)

        return df_filtered

    def _layer3_maturity_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第3层：数据完整性筛选

        v1.9.56 P1-3: 重新定义为数据完整性筛选，移除成立天数检查
        （成立天数由Layer 4统一负责，避免Layer 3/4完全重叠）

        条件：
        - 有成立日期（establish_days 非空且 > 0）
        - 基金规模 ≥ min_scale_baseline
        - 有基金经理信息（manager_names 非空）
        """
        layer_name = "第3层：数据完整性筛选"
        step_id = 3
        start_count = len(df)

        # 数据完整性检查（替代原来的成立天数检查）
        has_estab = df['establish_days'].notna() & (df['establish_days'] > 0)
        scale_ok = df['scale_billion'] >= self.min_scale_baseline
        manager_name_text = df['manager_names'].fillna('').astype(str).str.strip()
        if 'manager_ids' in df.columns:
            manager_id_text = df['manager_ids'].fillna('').astype(str).str.strip()
        else:
            manager_id_text = pd.Series('', index=df.index, dtype='object')
        has_manager = (manager_name_text != '') | (manager_id_text != '')
        mask = has_estab & scale_ok & has_manager

        # 记录决策
        if self.tracker:
            for idx, row in df.iterrows():
                fcode = row['fcode']
                est_days = row.get('establish_days', 0)
                scale = row.get('scale_billion', 0)
                manager = row.get('manager_names', '')
                manager_ids = row.get('manager_ids', '')

                if not has_estab.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='NO_ESTAB_DATE',
                        reason_detail=f"establish_days={est_days} (无有效成立日期)"
                    )
                elif not scale_ok.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='SCALE_TOO_SMALL',
                        reason_detail=f"scale_billion={scale:.2f} < threshold={self.min_scale_baseline}"
                    )
                elif not has_manager.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='NO_MANAGER_INFO',
                        reason_detail=(
                            f"manager_names='{manager}', manager_ids='{manager_ids}' "
                            "(无基金经理标识信息)"
                        )
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED'
                    )

        df_filtered = df[mask].copy()

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        self._record_layer_stats(
            layer_name,
            start_count,
            end_count,
            filtered_count,
            filter_rate,
            extra_info=f"基础规模阈值: {self.min_scale_baseline}亿元",
        )

        return df_filtered

    def _layer4_period_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第4层：周期自适应成熟度筛选

        条件：根据分析周期动态调整成立时间要求
        - 短周期（<1年）：要求成立≥分析周期天数×3（至少有3个完整周期的数据），最低180天
        - 债券短周期附加要求：必须具备真实1年可观测收益（成立≥365天且 `return_1y` 非空）
        - 1-2年期：成立≥1年
        - 3年期：成立≥2年
        - 4-5年期：成立≥3年
        """
        layer_name = "第4层：周期自适应成熟度筛选"
        step_id = 4
        start_count = len(df)

        min_establish_days, min_establish_label = self._get_layer4_min_establish_requirement()

        establish_days = pd.to_numeric(df.get('establish_days'), errors='coerce')
        base_mask = establish_days >= min_establish_days
        mask = base_mask.copy()
        self.layer4_one_year_observable_blocked_df = pd.DataFrame()
        self.layer4_one_year_observable_blocked_summary = {}

        one_year_observable_metadata = {
            'one_year_observable_required_min_establish_days': None,
            'one_year_observable_blocked_total': 0,
            'one_year_observable_under_365_days': 0,
            'one_year_observable_missing_return_1y': 0,
            'one_year_observable_missing_return_1y_with_establish_ready': 0,
        }
        one_year_observable_mask = pd.Series([True] * len(df), index=df.index)
        one_year_observable_gate_enabled = self._needs_bond_short_term_one_year_observability_gate()

        if one_year_observable_gate_enabled:
            one_year_observable_mask, one_year_observable_metadata = (
                self._get_bond_short_term_one_year_observability_mask(df)
            )
            return_1y = self._coerce_numeric_series(df, 'return_1y')
            establish_ready = establish_days >= 365
            return_ready = return_1y.notna()
            blocked_by_one_year_gate = base_mask & (~one_year_observable_mask)
            blocked_candidates = df[blocked_by_one_year_gate].copy()
            if not blocked_candidates.empty:
                reason_codes = np.where(
                    (~establish_ready.loc[blocked_candidates.index]) & (~return_ready.loc[blocked_candidates.index]),
                    'UNDER_365_DAYS_AND_MISSING_RETURN_1Y',
                    np.where(
                        ~establish_ready.loc[blocked_candidates.index],
                        'UNDER_365_DAYS',
                        'MISSING_RETURN_1Y',
                    ),
                )
                reason_texts = []
                for reason_code in reason_codes.tolist():
                    if reason_code == 'UNDER_365_DAYS_AND_MISSING_RETURN_1Y':
                        reason_texts.append('成立未满365天，且缺少 return_1y')
                    elif reason_code == 'UNDER_365_DAYS':
                        reason_texts.append('成立未满365天')
                    else:
                        reason_texts.append('已满365天，但缺少 return_1y')
                blocked_candidates['bond_short_term_one_year_observation_status'] = '未满1Y，仅观察'
                blocked_candidates['bond_short_term_one_year_observation_reason_code'] = reason_codes
                blocked_candidates['bond_short_term_one_year_observation_reason'] = reason_texts
                blocked_candidates['bond_short_term_one_year_observation_required_gate'] = (
                    '成立≥365天且return_1y非空'
                )
            self.layer4_one_year_observable_blocked_df = blocked_candidates
            self.layer4_one_year_observable_blocked_summary = {
                'blocked_candidate_count': int(len(blocked_candidates)),
                'under_365_days_count': int((base_mask & (~establish_ready)).sum()),
                'missing_return_1y_count': int((base_mask & (~return_ready)).sum()),
                'required_gate': '成立≥365天且return_1y非空',
            }
            mask = mask & one_year_observable_mask

        # 记录决策
        if self.tracker:
            for idx, row in df.iterrows():
                fcode = row['fcode']
                est_days = row.get('establish_days', 0)
                return_1y = row.get('return_1y', None)

                if not base_mask.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='PERIOD_MISMATCH',
                        reason_detail=f"establish_days={est_days} < required={min_establish_days} ({min_establish_label})"
                    )
                elif one_year_observable_gate_enabled and not one_year_observable_mask.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='PERIOD_MISMATCH',
                        reason_detail=(
                            "bond_short_term_requires_1y_observable: "
                            f"establish_days={est_days}, return_1y={return_1y}, "
                            "required=成立≥365天且return_1y非空"
                        )
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED'
                    )

        df_filtered = df[mask].copy()

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        has_valid_establish_days = start_count > 0 and establish_days.notna().any()
        input_min_establish_days = int(establish_days.min()) if has_valid_establish_days else None
        input_max_establish_days = int(establish_days.max()) if has_valid_establish_days else None
        below_threshold_in_input = int((establish_days < min_establish_days).sum()) if start_count > 0 else 0
        zero_increment_due_to_upstream_gate = bool(
            start_count > 0
            and filtered_count == 0
            and input_min_establish_days is not None
            and input_min_establish_days >= min_establish_days
        )

        layer4_extra_info = f"要求：成立≥{min_establish_label} [{round(self.period_years, 2)}年期分析]"
        if input_min_establish_days is not None:
            layer4_extra_info += f"；输入最小成立={input_min_establish_days}天"
        if one_year_observable_gate_enabled:
            layer4_extra_info += (
                "；债券短期附加门槛: 需具备1年可观测收益"
                f"（成立≥365天且return_1y非空），附加筛除="
                f"{one_year_observable_metadata['one_year_observable_blocked_total']}只"
                f"（成立不足1年="
                f"{one_year_observable_metadata['one_year_observable_under_365_days']}只，"
                f"已满1年但return_1y缺失="
                f"{one_year_observable_metadata['one_year_observable_missing_return_1y_with_establish_ready']}只）"
            )
        if zero_increment_due_to_upstream_gate:
            layer4_extra_info += "；本层无增量筛除（输入已全部满足最低成熟度门槛）"

        layer4_metadata = {
            'required_min_establish_days': int(min_establish_days),
            'required_min_establish_label': min_establish_label,
            'input_min_establish_days': input_min_establish_days,
            'input_max_establish_days': input_max_establish_days,
            'below_threshold_in_input': below_threshold_in_input,
            'zero_increment_due_to_upstream_gate': zero_increment_due_to_upstream_gate,
            'one_year_observable_gate_enabled': one_year_observable_gate_enabled,
        }
        layer4_metadata.update(one_year_observable_metadata)

        self._record_layer_stats(
            layer_name,
            start_count,
            end_count,
            filtered_count,
            filter_rate,
            extra_info=layer4_extra_info,
            metadata=layer4_metadata,
        )

        return df_filtered

    def _layer5_scale_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第5层：规模分层筛选

        条件：根据分析周期动态提升规模要求
        """
        layer_name = "第5层：规模分层筛选"
        step_id = 5
        start_count = len(df)

        mask = df['scale_billion'] >= self.scale_threshold

        # 记录决策
        if self.tracker:
            for idx, row in df.iterrows():
                fcode = row['fcode']
                scale = row.get('scale_billion', 0)

                if not mask.loc[idx]:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='SCALE_BELOW_THRESHOLD',
                        reason_detail=f"scale_billion={scale:.2f} < threshold={self.scale_threshold}"
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED'
                    )

        df_filtered = df[mask].copy()

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        self._record_layer_stats(
            layer_name, start_count, end_count, filtered_count, filter_rate,
            extra_info=f"规模阈值: {self.scale_threshold}亿元"
        )

        return df_filtered

    def _layer5_5_fee_rate_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        v1.9.60 P1-4: 费率筛选

        Finominal 2024 研究表明费率是唯一具有正向预测力的基金筛选指标。
        低费率基金样本外超额收益+3.0%，高费率基金-14.8%。
        筛选条件：持续持有费率（管理费+托管费+销售服务费）< 阈值（按基金类型区分）
        """
        layer_name = "第5.5层：费率筛选"
        step_id = 55  # 使用55避免与现有层冲突
        start_count = len(df)

        from bond_fund_screening.config import FEE_RATE_THRESHOLDS
        fee_threshold = FEE_RATE_THRESHOLDS.get(self.runtime_fund_type, 1.0)

        # v1.9.68 P1-S1: 费率缺失的基金不再放行，与评分引擎对齐（评分层对NaN费率赋最低分）
        # 修改前：~has_fee | (费率<=阈值)  → NaN费率基金直接通过
        # 修改后：has_fee & (费率<=阈值)  → NaN费率基金被淘汰，统一数据质量标准
        fee_metric_col = 'fee_screening_rate' if 'fee_screening_rate' in df.columns else 'total_fee_rate'
        fee_status_col = 'sales_fee_source_status' if 'sales_fee_source_status' in df.columns else None
        fee_floor_col = 'fee_screening_uses_floor' if 'fee_screening_uses_floor' in df.columns else None

        has_fee = df[fee_metric_col].notna() & (df[fee_metric_col] > 0)
        mask = has_fee & (df[fee_metric_col] <= fee_threshold)

        if self.tracker:
            for idx, row in df.iterrows():
                fcode = row['fcode']
                fee = row.get(fee_metric_col)
                fee_status = str(row.get(fee_status_col) or '') if fee_status_col else ''
                uses_floor = bool(row.get(fee_floor_col)) if fee_floor_col else False
                if not mask.loc[idx]:
                    if not has_fee.loc[idx]:
                        if fee_status == 'shadow_code_candidate':
                            reason_code = 'FEE_RATE_SHADOW_CODE'
                            reason_detail = "基金代码疑似影子代码，销售服务费口径不可直接信任"
                        else:
                            reason_code = 'FEE_RATE_MISSING'
                            reason_detail = "费率数据缺失或为0，不满足数据质量要求"
                        self.tracker.record_decision(
                            fcode=fcode, step_id=step_id, step_name=layer_name,
                            passed=False, reason_code=reason_code,
                            reason_detail=reason_detail
                        )
                    else:
                        if uses_floor:
                            reason_code = 'FEE_RATE_FLOOR_TOO_HIGH'
                            reason_detail = (
                                f"fee_screening_rate(core_fee_rate)={fee:.2f}% > threshold={fee_threshold:.1f}%"
                            )
                        else:
                            reason_code = 'FEE_RATE_TOO_HIGH'
                            reason_detail = f"total_fee_rate={fee:.2f}% > threshold={fee_threshold:.1f}%"
                        self.tracker.record_decision(
                            fcode=fcode, step_id=step_id, step_name=layer_name,
                            passed=False, reason_code=reason_code,
                            reason_detail=reason_detail
                        )
                else:
                    reason_code = 'FEE_RATE_FLOOR_PASSED' if uses_floor else 'PASSED'
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code=reason_code
                    )

        df_filtered = df[mask].copy()
        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        self._record_layer_stats(
            layer_name, start_count, end_count, filtered_count, filter_rate,
            extra_info=f"费率阈值: {fee_threshold}%（筛选列: {fee_metric_col}）"
        )

        return df_filtered

    @capture_call('screening_risk.cohort', attributes=_CAPTURE_POLICY_FIELDS)
    def _layer6_risk_metrics_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第6层：风险指标筛选（实时计算）

        条件：至少N个有效风险指标优于中位数
        （N 由 risk_pass_count 与有效指标列数量动态裁剪得到）

        这是最复杂的一层，需要：
        1. 对每只基金查询历史净值
        2. 实时计算6个风险指标
        3. 计算所有基金的各指标中位数
        4. 判断每只基金是否通过
        """
        layer_name = "第6层：风险指标筛选（实时计算）"
        step_id = 6
        start_count = len(df)

        if start_count < self.risk_metric_min_funds:
            if self.verbose:
                print(
                    f"\n⚠️  {layer_name}: 基金数{start_count}只不足同类比较最低样本数"
                    f"{self.risk_metric_min_funds}只，按 fail-closed 淘汰。"
                )

            for fcode in df['fcode'].tolist():
                self._record_layer6_decision(
                    fcode=fcode,
                    step_id=step_id,
                    step_name=layer_name,
                    passed=False,
                    reason_code='RISK_COHORT_TOO_SMALL',
                    reason_detail=(
                        f"cohort_size={start_count} < required={self.risk_metric_min_funds}; "
                        "无法形成可靠的同类风险中位数"
                    ),
                    metrics={
                        'layer6_stage': '6A',
                        'cohort_size': start_count,
                        'required_cohort_size': self.risk_metric_min_funds,
                        'fail_closed': True,
                    },
                )

            self._record_layer_stats(
                layer_name,
                start_count,
                0,
                start_count,
                100.0 if start_count > 0 else 0.0,
                extra_info=(
                    f"fail-closed：样本数{start_count} < 同类比较最低要求"
                    f"{self.risk_metric_min_funds}"
                ),
                metadata={
                    'fail_closed': True,
                    'reason_code': 'RISK_COHORT_TOO_SMALL',
                    'cohort_size': start_count,
                    'required_cohort_size': self.risk_metric_min_funds,
                },
            )
            return df.iloc[0:0].copy()

        if self.verbose:
            print(f"\n⏳ {layer_name}: 开始实时计算风险指标...")
            print(f"   待计算基金数: {start_count:,} 只")
            print(f"   使用{RISK_CALC_THREADS}线程并发计算...")

        # 批量获取历史净值
        fcodes = df['fcode'].tolist()
        style_tag_map = self._build_layer6_style_tag_map(df)
        window_end_date = self.as_of_date or date_type.today()
        end_date_str = window_end_date.isoformat()
        query_days = self.period_days + self._get_layer6_start_boundary_lookback_days()
        nav_dict = self.data_loader.get_batch_nav_history(
            fcodes,
            query_days,
            end_date=end_date_str,
        )

        # 并发计算风险指标
        risk_metrics_list = []
        risk_metric_value_cols, structurally_excluded_risk_cols = self._get_layer6_risk_metric_value_cols()
        calc_meta_map = {}

        # 使用线程池
        with ThreadPoolExecutor(max_workers=RISK_CALC_THREADS) as executor:
            futures = [
                executor.submit(
                    self._calculate_layer6_single_fund,
                    fcode,
                    nav_dict.get(fcode),
                    risk_metric_value_cols,
                    style_tag_map.get(fcode),
                    window_end_date,
                )
                for fcode in fcodes
            ]

            if self.verbose and HAS_TQDM:
                # 使用tqdm显示进度条
                completed_futures = tqdm(as_completed(futures), total=len(fcodes), desc="   计算进度")
            else:
                # 不显示进度条
                completed_futures = as_completed(futures)
                if self.verbose:
                    print(f"   计算中...")

            for future in completed_futures:
                fcode, metrics, calc_meta = future.result()
                calc_meta_map[fcode] = calc_meta or {}
                if metrics is not None:
                    metrics['fcode'] = fcode
                    risk_metrics_list.append(metrics)

        calculation_status_counts: Dict[str, int] = {}
        for fcode in fcodes:
            calc_status = str(
                calc_meta_map.get(fcode, {}).get('status') or 'RISK_METRICS_UNAVAILABLE'
            )
            calculation_status_counts[calc_status] = (
                calculation_status_counts.get(calc_status, 0) + 1
            )
        boundary_predecessor_count = sum(
            1
            for calc_meta in calc_meta_map.values()
            if calc_meta.get('start_boundary_predecessor_used') is True
        )
        boundary_point_missing_count = sum(
            1
            for calc_meta in calc_meta_map.values()
            if not calc_meta.get('start_boundary_point_date')
        )
        if self.verbose:
            print(
                "   风险可计算状态: "
                + ", ".join(
                    f"{status}={count}"
                    for status, count in sorted(calculation_status_counts.items())
                )
            )
            print(
                f"   窗口起点基准: 使用前置净值={boundary_predecessor_count}, "
                f"缺少起点基准={boundary_point_missing_count}"
            )

        if len(risk_metrics_list) == 0:
            if self.verbose:
                print("   ⚠️  无法计算任何基金的风险指标，按 fail-closed 全部淘汰。")

            for fcode in fcodes:
                calc_meta = calc_meta_map.get(fcode, {})
                status = str(calc_meta.get('status') or 'RISK_METRICS_UNAVAILABLE')
                self._record_layer6_decision(
                    fcode=fcode,
                    step_id=step_id,
                    step_name=layer_name,
                    passed=False,
                    reason_code=status,
                    reason_detail=(
                        "风险指标无可用结果，禁止退化为仅净值放行；"
                        f"nav_points={calc_meta.get('nav_count', 0)}"
                    ),
                    metrics={**calc_meta, 'layer6_stage': '6A', 'fail_closed': True},
                )

            self._record_layer_stats(
                layer_name,
                start_count,
                0,
                start_count,
                100.0 if start_count > 0 else 0.0,
                extra_info="fail-closed：无任何可计算风险指标，禁止仅按 NAV 放行",
                metadata={
                    'fail_closed': True,
                    'reason_code': 'NO_CALCULABLE_RISK_RESULTS',
                    'calculation_status_counts': calculation_status_counts,
                    'start_boundary_predecessor_used_count': boundary_predecessor_count,
                    'start_boundary_point_missing_count': boundary_point_missing_count,
                },
            )
            return df.iloc[0:0].copy()

        calculable_count = len(risk_metrics_list)
        if calculable_count < self.risk_metric_min_funds:
            if self.verbose:
                print(
                    f"   ⚠️  可计算风险指标仅{calculable_count}只，低于同类比较最低样本数"
                    f"{self.risk_metric_min_funds}只，按 fail-closed 全部淘汰。"
                )

            for fcode in fcodes:
                calc_meta = calc_meta_map.get(fcode, {})
                calc_status = str(calc_meta.get('status') or 'RISK_METRICS_UNAVAILABLE')
                if calc_status == 'OK':
                    reason_code = 'RISK_CALCULABLE_COHORT_TOO_SMALL'
                    reason_detail = (
                        f"calculable_cohort_size={calculable_count} < "
                        f"required={self.risk_metric_min_funds}; 无法形成可靠的同类风险中位数"
                    )
                else:
                    reason_code = calc_status
                    reason_detail = f"risk_calculation_status={calc_status}"
                self._record_layer6_decision(
                    fcode=fcode,
                    step_id=step_id,
                    step_name=layer_name,
                    passed=False,
                    reason_code=reason_code,
                    reason_detail=reason_detail,
                    metrics={
                        **calc_meta,
                        'layer6_stage': '6A',
                        'calculable_cohort_size': calculable_count,
                        'required_cohort_size': self.risk_metric_min_funds,
                        'fail_closed': True,
                    },
                )

            self._record_layer_stats(
                layer_name,
                start_count,
                0,
                start_count,
                100.0 if start_count > 0 else 0.0,
                extra_info=(
                    f"fail-closed：可计算风险样本{calculable_count} < 同类比较最低要求"
                    f"{self.risk_metric_min_funds}"
                ),
                metadata={
                    'fail_closed': True,
                    'reason_code': 'RISK_CALCULABLE_COHORT_TOO_SMALL',
                    'input_cohort_size': start_count,
                    'calculable_cohort_size': calculable_count,
                    'required_cohort_size': self.risk_metric_min_funds,
                    'calculation_status_counts': calculation_status_counts,
                    'start_boundary_predecessor_used_count': boundary_predecessor_count,
                    'start_boundary_point_missing_count': boundary_point_missing_count,
                },
            )
            return df.iloc[0:0].copy()

        # 转换为DataFrame
        risk_df = pd.DataFrame(risk_metrics_list)

        bucket_col = BUCKET_KEY_FIELD

        # 风险指标质量门控（默认 warn，不改变筛选结果）
        qc_cfg = RISK_METRIC_QC if isinstance(RISK_METRIC_QC, dict) else {}
        qc_enabled = bool(qc_cfg.get('enabled', False))
        qc_mode = str(qc_cfg.get('mode', 'warn')).lower()
        qc_thresholds = qc_cfg.get('thresholds', {}) if isinstance(qc_cfg.get('thresholds'), dict) else {}
        qc_issue_map = {}

        if qc_enabled:
            max_abs_sharpe = float(qc_thresholds.get('max_abs_sharpe_ratio', 5.0))
            max_abs_sortino = float(qc_thresholds.get('max_abs_sortino_ratio', 10.0))
            max_abs_calmar = float(qc_thresholds.get('max_abs_calmar_ratio', 20.0))
            max_volatility = float(qc_thresholds.get('max_volatility', 1.5))
            max_consistency = float(qc_thresholds.get('max_consistency', 1.0))
            max_drawdown = float(qc_thresholds.get('max_drawdown', 0.95))

            for _, row in risk_df.iterrows():
                issues = []
                fcode = row.get('fcode')

                sharpe_val = row.get('sharpe_ratio')
                if pd.notna(sharpe_val) and abs(sharpe_val) > max_abs_sharpe:
                    issues.append(f"abs(sharpe_ratio)={abs(float(sharpe_val)):.4f}>{max_abs_sharpe:.4f}")

                sortino_val = row.get('sortino_ratio')
                if pd.notna(sortino_val) and abs(sortino_val) > max_abs_sortino:
                    issues.append(f"abs(sortino_ratio)={abs(float(sortino_val)):.4f}>{max_abs_sortino:.4f}")

                calmar_val = row.get('calmar_ratio')
                if pd.notna(calmar_val) and abs(calmar_val) > max_abs_calmar:
                    issues.append(f"abs(calmar_ratio)={abs(float(calmar_val)):.4f}>{max_abs_calmar:.4f}")

                volatility_val = row.get('volatility')
                if pd.notna(volatility_val) and float(volatility_val) > max_volatility:
                    issues.append(f"volatility={float(volatility_val):.4f}>{max_volatility:.4f}")

                consistency_val = row.get('consistency')
                if pd.notna(consistency_val) and float(consistency_val) > max_consistency:
                    issues.append(f"consistency={float(consistency_val):.4f}>{max_consistency:.4f}")

                drawdown_val = row.get('max_drawdown')
                if pd.notna(drawdown_val) and float(drawdown_val) > max_drawdown:
                    issues.append(f"max_drawdown={float(drawdown_val):.4f}>{max_drawdown:.4f}")

                if issues and fcode:
                    qc_issue_map[fcode] = issues

            qc_issue_samples = self._build_risk_qc_issue_samples(df, qc_issue_map, bucket_col)

            if self.verbose and qc_issue_map:
                qc_action = "淘汰" if qc_mode == 'hard' else "告警"
                print(f"   风险QC异常基金: {len(qc_issue_map)} 只（mode={qc_mode}, {qc_action}）")
                for sample in qc_issue_samples:
                    bucket_key = sample.get('bucket_key') or '-'
                    shortname = sample.get('shortname') or '-'
                    issues = '; '.join(sample.get('issues', []))
                    print(f"     - {sample['fcode']} {shortname} [{bucket_key}] -> {issues}")
        else:
            qc_issue_samples = []

        # Step 3: 合并 ftype 到 risk_df 用于分桶
        if bucket_col in df.columns:
            ftype_map = df.set_index('fcode')[bucket_col].to_dict()
            risk_df[bucket_col] = risk_df['fcode'].map(ftype_map)

        # 风险指标列
        risk_cols = ['sharpe_ratio', 'sortino_ratio', 'calmar_ratio',
                     'max_drawdown', 'volatility', 'consistency']
        available_cols = [c for c in risk_cols if c in risk_df.columns]

        # Layer 6B：执行同类相对风险比较
        (
            pass_periods_map,
            bucket_info_map,
            bucket_sizes,
            global_medians_dict,
            effective_cols,
            n_cols,
            configured_required_pass,
            required_pass,
            required_pass_notes,
        ) = self._layer6_stage_6b_relative_gate(
            df=df,
            risk_df=risk_df,
            available_cols=available_cols,
            risk_metric_value_cols=risk_metric_value_cols,
            structurally_excluded_risk_cols=structurally_excluded_risk_cols,
            bucket_col=bucket_col,
        )

        # Step 4: 获取 main_return 的映射来源（用于持续性窗口追溯）
        default_main_return_mapped_from = self._get_main_return_mapping()

        if self.verbose:
            if BUCKETING_ENABLED and bucket_sizes:
                print(f"\n   分桶统计 (按 {bucket_col}):")
                for bk, bs in sorted(bucket_sizes.items(), key=lambda x: -x[1])[:5]:
                    print(f"     {bk}: {bs} 只")
                print(f"   全局中位数 (fallback):")
            else:
                print(f"\n   风险指标中位数:")
            for metric in available_cols:
                print(f"     {metric:20s}: {global_medians_dict.get(metric, 0):.4f}")

        # 将 pass_periods 映射回 df（无风险数据的基金默认为0）
        df['pass_periods'] = df['fcode'].map(pass_periods_map).fillna(0).astype(int)
        if qc_enabled:
            df['risk_qc_issue_count'] = df['fcode'].map(lambda x: len(qc_issue_map.get(x, []))).fillna(0).astype(int)

        # 6A 可计算性是独立硬门禁。即使某个极端批次的有效比较列数量退化为 0，
        # 也不能因 required_pass=0 让无风险结果的基金自动通过。
        mask_calculable = df['fcode'].map(
            lambda code: calc_meta_map.get(code, {}).get('status') == 'OK'
        ).fillna(False).astype(bool)
        mask_relative = (df['pass_periods'] >= required_pass) & mask_calculable
        min_absolute_return, mask_absolute, mask_qc = self._layer6_stage_6c_absolute_qc_gate(
            df=df,
            qc_enabled=qc_enabled,
            qc_mode=qc_mode,
            qc_issue_map=qc_issue_map,
        )
        mask = mask_calculable & mask_relative & mask_absolute & mask_qc

        # 记录决策（包含 bucket 信息 + Step 4 窗口追溯信息）
        if self.tracker:
            for i, (idx, row) in enumerate(df.iterrows()):
                fcode = row['fcode']
                pass_periods = row['pass_periods']
                main_ret = row.get('main_return')
                qc_issues = qc_issue_map.get(fcode, [])
                default_bucket_key = ''
                if bucket_col in df.columns and pd.notna(row.get(bucket_col)):
                    default_bucket_key = str(row.get(bucket_col))
                bucket_info = bucket_info_map.get(fcode, {
                    'bucket_key': default_bucket_key,
                    'bucket_size': bucket_sizes.get(default_bucket_key, 0),
                    'is_fallback': True,
                    'col_medians': {},
                    'nan_auto_pass': 0,
                })

                detail_suffix = f", bucket_key={bucket_info['bucket_key']}, bucket_size={bucket_info['bucket_size']}, fallback={bucket_info['is_fallback']}"

                nan_auto = bucket_info.get('nan_auto_pass', 0)
                if nan_auto > 0:
                    detail_suffix += f", nan_auto_pass={nan_auto}"
                if qc_issues:
                    detail_suffix += f", qc_issues={len(qc_issues)}"

                calc_meta = calc_meta_map.get(fcode, {})
                calc_status = calc_meta.get('status')
                nav_count = calc_meta.get('nav_count')
                required_nav_points = calc_meta.get('required_nav_points')
                valid_risk_metric_count = calc_meta.get('valid_risk_metric_count')

                # Step 4: 构建包含窗口追溯信息的 metrics
                base_metrics = {
                    'pass_count': pass_periods,
                    'pass_periods': pass_periods,
                    'required_periods': required_pass,
                    'configured_required_periods': configured_required_pass,
                    'effective_periods': n_cols,
                    'bucket_key': bucket_info['bucket_key'],
                    'bucket_size': bucket_info['bucket_size'],
                    'is_fallback': bucket_info['is_fallback'],
                    # Step 4: 窗口追溯信息
                    'windows_used': effective_cols,
                    'main_return_mapped_from': (
                        str(row.get('main_return_mapped_from')).strip()
                        if pd.notna(row.get('main_return_mapped_from'))
                        and str(row.get('main_return_mapped_from')).strip()
                        else default_main_return_mapped_from
                    ),
                    'period_days': self.period_days,
                    'raw_nav_count': calc_meta.get('raw_nav_count'),
                    'requested_period_start': calc_meta.get('requested_period_start'),
                    'requested_period_end': calc_meta.get('requested_period_end'),
                    'start_boundary_point_date': calc_meta.get('start_boundary_point_date'),
                    'start_boundary_predecessor_used': calc_meta.get(
                        'start_boundary_predecessor_used'
                    ),
                    # 风险QC追溯
                    'risk_qc_enabled': qc_enabled,
                    'risk_qc_mode': qc_mode,
                    'risk_qc_issues': qc_issues,
                    'nav_count': nav_count,
                    'required_nav_points': required_nav_points,
                    'coverage_days': calc_meta.get('coverage_days'),
                    'required_coverage_days': calc_meta.get('required_coverage_days'),
                    'coverage_grace_days': calc_meta.get('coverage_grace_days'),
                    'max_gap_days': calc_meta.get('max_gap_days'),
                    'max_gap_threshold_days': calc_meta.get('max_gap_threshold_days'),
                    'invalid_date_count': calc_meta.get('invalid_date_count'),
                    'duplicate_date_count': calc_meta.get('duplicate_date_count'),
                    'valid_risk_metric_count': valid_risk_metric_count,
                    'calmar_style_tag': calc_meta.get('calmar_style_tag'),
                    'calmar_min_drawdown_floor': calc_meta.get('calmar_min_drawdown_floor'),
                    'calmar_min_drawdown_floor_source': calc_meta.get('calmar_min_drawdown_floor_source'),
                    'layer6_substages': ['6A', '6B', '6C'],
                }

                if calc_status and calc_status != 'OK':
                    calculation_reason_details = {
                        'NO_NAV_HISTORY': "未找到周期内净值历史",
                        'INVALID_NAV_DATES': (
                            f"invalid_nav_dates={calc_meta.get('invalid_date_count')}"
                        ),
                        'DUPLICATE_NAV_DATES': (
                            f"duplicate_nav_dates={calc_meta.get('duplicate_date_count')}"
                        ),
                        'INSUFFICIENT_NAV_POINTS': (
                            f"nav_points={nav_count} < required={required_nav_points}"
                        ),
                        'LOW_FREQUENCY_NAV_OBSERVATIONS': (
                            f"nav_points={nav_count} < required={required_nav_points}"
                        ),
                        'INSUFFICIENT_NAV_COVERAGE': (
                            f"coverage_days={calc_meta.get('coverage_days')} < "
                            f"required={calc_meta.get('required_coverage_days')}"
                        ),
                        'NAV_DATA_GAP': (
                            f"max_gap={calc_meta.get('max_gap_days')}d > "
                            f"threshold={calc_meta.get('max_gap_threshold_days')}d"
                        ),
                        'RISK_METRICS_UNAVAILABLE': (
                            f"valid_risk_metric_count=0, nav_points={nav_count}"
                        ),
                    }
                    self._record_layer6_decision(
                        fcode=fcode,
                        step_id=step_id,
                        step_name=layer_name,
                        passed=False,
                        reason_code=calc_status,
                        reason_detail=(
                            calculation_reason_details.get(
                                calc_status,
                                f"risk_calculation_status={calc_status}",
                            )
                            + detail_suffix
                        ),
                        metrics=base_metrics,
                    )
                elif not mask_relative.loc[idx]:
                    self._record_layer6_decision(
                        fcode=fcode,
                        step_id=step_id,
                        step_name=layer_name,
                        passed=False,
                        reason_code='CONSISTENCY_FAIL',
                        reason_detail=f"pass_periods={pass_periods} < required={required_pass}{detail_suffix}",
                        metrics=base_metrics,
                    )
                elif not mask_absolute.loc[idx]:
                    base_metrics['main_return'] = main_ret
                    self._record_layer6_decision(
                        fcode=fcode,
                        step_id=step_id,
                        step_name=layer_name,
                        passed=False,
                        reason_code='ABSOLUTE_RETURN_FAIL',
                        reason_detail=f"main_return={main_ret:.2f}% < threshold={min_absolute_return:.1f}%{detail_suffix}",
                        metrics=base_metrics,
                    )
                elif not mask_qc.loc[idx]:
                    self._record_layer6_decision(
                        fcode=fcode,
                        step_id=step_id,
                        step_name=layer_name,
                        passed=False,
                        reason_code='RISK_METRICS_QC_FAIL',
                        reason_detail=f"risk_qc_failed({qc_mode}): {'; '.join(qc_issues)}{detail_suffix}",
                        metrics=base_metrics,
                    )
                else:
                    self._record_layer6_decision(
                        fcode=fcode,
                        step_id=step_id,
                        step_name=layer_name,
                        passed=True,
                        reason_code='PASSED',
                        reason_detail=f"pass_periods={pass_periods}{detail_suffix}",
                        metrics=base_metrics,
                    )

        df_filtered = df[mask].copy()

        # v1.9.59 P0-1: 将 Layer 6 计算的风险指标合并回主 DataFrame
        # 确保 CSV 导出和多周期合并器能获取 sharpe_ratio 等指标
        # v1.9.60: 同时合并 max_drawdown_duration（不参与筛选，仅供展示）
        extra_risk_cols = [
            'max_drawdown_duration',
            'raw_sharpe_ratio',
            'raw_sortino_ratio',
            'sharpe_ratio_cap_hit',
            'sortino_ratio_cap_hit',
            'calmar_style_tag',
            'calmar_min_drawdown_floor',
            'calmar_min_drawdown_floor_source',
            'calmar_effective_drawdown',
        ]
        all_merge_cols = list(set(
            [c for c in available_cols if c in risk_df.columns] +
            [c for c in extra_risk_cols if c in risk_df.columns]
        ))
        if all_merge_cols:
            risk_merge_df = risk_df[['fcode'] + all_merge_cols].copy()
            df_filtered = df_filtered.merge(risk_merge_df, on='fcode', how='left', suffixes=('', '_risk'))
            # 处理可能的重复列（如果 df 中已有同名列）
            for col in all_merge_cols:
                if f'{col}_risk' in df_filtered.columns:
                    df_filtered[col] = df_filtered[f'{col}_risk']
                    df_filtered.drop(columns=[f'{col}_risk'], inplace=True)

        # Step 4: 添加持续性窗口追溯列到输出 DataFrame
        df_filtered['windows_used'] = json.dumps(effective_cols)
        if 'main_return_mapped_from' in df_filtered.columns:
            df_filtered['main_return_mapped_from'] = df_filtered['main_return_mapped_from'].where(
                df_filtered['main_return_mapped_from'].notna()
                & df_filtered['main_return_mapped_from'].astype(str).str.strip().ne(''),
                default_main_return_mapped_from,
            )
        else:
            df_filtered['main_return_mapped_from'] = default_main_return_mapped_from

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        # 统计被各条件筛除的数量
        relative_fail = (~mask_relative).sum()
        absolute_fail = (~mask_absolute).sum()
        qc_fail = (~mask_qc).sum() if qc_enabled and qc_mode == 'hard' else 0

        self._record_layer_stats(
            layer_name,
            start_count,
            end_count,
            filtered_count,
            filter_rate,
            extra_info=(
                f"要求至少{required_pass}/{n_cols}个有效风险指标≥中位数(配置阈值={configured_required_pass})"
                f"{'；' + '，'.join(required_pass_notes) if required_pass_notes else ''}"
                f" 且 主周期收益≥{self._format_threshold_percent(min_absolute_return)}"
                f" | risk_qc={qc_mode}, qc_issues={len(qc_issue_map)}, qc_filtered={qc_fail}"
            ),
            metadata={
                'risk_qc_enabled': qc_enabled,
                'risk_qc_mode': qc_mode,
                'risk_qc_issue_count': len(qc_issue_map),
                'risk_qc_filtered_count': int(qc_fail),
                'risk_qc_issue_samples': qc_issue_samples,
                'calculation_status_counts': calculation_status_counts,
                'start_boundary_predecessor_used_count': boundary_predecessor_count,
                'start_boundary_point_missing_count': boundary_point_missing_count,
                'query_period_days': int(query_days),
                'query_start_boundary_lookback_days': (
                    self._get_layer6_start_boundary_lookback_days()
                ),
                'structurally_excluded_risk_metrics': structurally_excluded_risk_cols,
                'layer6_substage_contract': self._get_layer6_stage_contract(),
                'layer6_reason_stage_map': self._get_layer6_reason_stage_map(),
                'layer6_topn_diff_tolerance': 0,
                'calmar_floor_audit': self._build_layer6_calmar_floor_audit(
                    risk_df,
                    passed_fcodes=set(df_filtered['fcode'].astype(str)),
                ),
            },
        )

        return df_filtered

    def _get_main_return_mapping(self) -> str:
        """
        Step 4: 获取 main_return 的映射来源（根据 period_days）

        Returns:
            映射来源字段名（如 'return_1m', 'return_3m' 等）
        """
        if self.period_days <= 7:
            return 'return_1w'
        elif self.period_days <= 30:
            return 'return_1m'
        elif self.period_days <= 90:
            return 'return_3m'
        elif self.period_days <= 180:
            return 'return_6m'
        elif self.period_days <= 365:
            return 'return_1y'
        elif self.period_days <= 730:
            return 'return_2y'
        elif self.period_days <= 1095:
            return 'return_3y'
        elif self.period_days <= 1460:
            return 'verified_daily_growth_4y'
        else:
            return 'return_5y'

    def _layer10_final_selection(
        self,
        df: pd.DataFrame,
        sort_col: Optional[str] = None,
        top_n: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        第10层：最终精选

        智能动态输出（默认保留全部达标基金）
        Step 3: 支持 global/bucket 两种排名模式，并输出 bucket 排名信息

        Args:
            df: 输入 DataFrame
            sort_col: 排序字段，优先级高于默认字段
            top_n: 输出数量上限，None 时默认不截断
        """
        layer_name = "第10层：最终精选"
        step_id = 10
        start_count = len(df)
        bucket_col = BUCKET_KEY_FIELD

        # 排序字段（可由上层显式指定，例如 selected_combo_score）
        if not sort_col or sort_col not in df.columns:
            if 'combo_score' in df.columns:
                sort_col = 'combo_score'
            else:
                sort_col = 'main_return'

        # Step 3: 计算 bucket 排名和百分位（无论哪种模式都计算，用于输出）
        if bucket_col in df.columns:
            df['bucket_key'] = df[bucket_col]
            df['bucket_size'] = df.groupby(bucket_col)[bucket_col].transform('count')
            df['bucket_rank'] = df.groupby(bucket_col)[sort_col].rank(ascending=False, method='min').astype(int)
            df['bucket_percentile'] = (df['bucket_rank'] / df['bucket_size'] * 100).round(2)
            df['is_fallback'] = df['bucket_size'] < BUCKET_MIN_SAMPLES
        else:
            df['bucket_key'] = ''
            df['bucket_size'] = start_count
            df['bucket_rank'] = df[sort_col].rank(ascending=False, method='min').astype(int)
            df['bucket_percentile'] = (df['bucket_rank'] / start_count * 100).round(2)
            df['is_fallback'] = True

        limit_all_eligible = top_n is None
        if not limit_all_eligible:
            top_n = min(max(int(top_n), 1), len(df))

        if limit_all_eligible:
            df_filtered = stable_sort_desc(df, sort_col).copy()
            extra_info = "global模式: 保留全部达标基金"
        elif FINAL_RANKING_MODE == 'bucket' and bucket_col in df.columns:
            # Step 3: bucket 模式 - 按 bucket 分配名额
            bucket_sizes = df.groupby(bucket_col).size()
            total_size = len(df)

            # 计算每个 bucket 的配额
            quotas = {}
            for bk, bs in bucket_sizes.items():
                quota = max(1, round(top_n * bs / total_size))
                quota = min(quota, bs)  # 不超过 bucket 大小
                quotas[bk] = quota

            # 调整配额总和等于 top_n
            total_quota = sum(quotas.values())
            if total_quota != top_n:
                # 按 bucket 大小排序，从大到小调整
                sorted_buckets = sorted(quotas.keys(), key=lambda x: bucket_sizes[x], reverse=True)
                diff = top_n - total_quota
                for bk in sorted_buckets:
                    if diff == 0:
                        break
                    if diff > 0:
                        # 需要增加配额
                        max_add = bucket_sizes[bk] - quotas[bk]
                        add = min(diff, max_add)
                        quotas[bk] += add
                        diff -= add
                    else:
                        # 需要减少配额
                        reduce = min(-diff, quotas[bk] - 1)
                        quotas[bk] -= reduce
                        diff += reduce

            # 从每个 bucket 取 Top quota
            selected_dfs = []
            for bk, quota in quotas.items():
                bucket_df = stable_sort_desc(df[df[bucket_col] == bk], sort_col).head(quota).copy()
                selected_dfs.append(bucket_df)

            df_filtered = pd.concat(selected_dfs, ignore_index=False) if selected_dfs else df.head(0)
            df_filtered = stable_sort_desc(df_filtered, sort_col)
            extra_info = f"bucket模式: 按{bucket_col}分配名额, 共{len(quotas)}个bucket"
        else:
            # global 模式（默认）- 全局排名取 TopN
            df_sorted = stable_sort_desc(df, sort_col)
            df_filtered = df_sorted.head(top_n).copy()
            extra_info = f"global模式: 输出Top {top_n}"

        # 记录决策（包含 bucket 信息）
        if self.tracker:
            selected_fcodes = set(df_filtered['fcode'].tolist())
            df_sorted_for_rank = stable_sort_desc(df, sort_col)

            for rank, (idx, row) in enumerate(df_sorted_for_rank.iterrows(), 1):
                fcode = row['fcode']
                bucket_key = row.get('bucket_key', '')
                bucket_size = row.get('bucket_size', start_count)
                bucket_rank = row.get('bucket_rank', rank)
                bucket_pct = row.get('bucket_percentile', 0)
                is_fallback = row.get('is_fallback', False)

                detail = f"global_rank={rank}/{start_count}, bucket_key={bucket_key}, bucket_size={bucket_size}, bucket_rank={bucket_rank}, bucket_pct={bucket_pct:.1f}%, fallback={is_fallback}"

                if fcode in selected_fcodes:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=True, reason_code='PASSED',
                        reason_detail=detail,
                        metrics={
                            'global_rank': rank,
                            'bucket_key': bucket_key,
                            'bucket_size': int(bucket_size),
                            'bucket_rank': int(bucket_rank),
                            'bucket_percentile': bucket_pct,
                            'is_fallback': is_fallback,
                        }
                    )
                else:
                    self.tracker.record_decision(
                        fcode=fcode, step_id=step_id, step_name=layer_name,
                        passed=False, reason_code='RANK_CUTOFF',
                        reason_detail=detail,
                        metrics={
                            'global_rank': rank,
                            'bucket_key': bucket_key,
                            'bucket_size': int(bucket_size),
                            'bucket_rank': int(bucket_rank),
                            'bucket_percentile': bucket_pct,
                            'is_fallback': is_fallback,
                        }
                    )

        end_count = len(df_filtered)
        filtered_count = start_count - end_count
        filter_rate = (filtered_count / start_count * 100) if start_count > 0 else 0

        self._record_layer_stats(
            layer_name,
            start_count,
            end_count,
            filtered_count,
            filter_rate,
            extra_info=extra_info
        )

        return df_filtered

    @staticmethod
    def _check_nav_continuity(nav_data) -> int:
        """检查净值数据连续性，返回最大缺口天数"""
        return _check_nav_continuity_impl(nav_data)

    def _record_layer_stats(
        self,
        layer_name: str,
        start_count: int,
        end_count: int,
        filtered_count: int,
        filter_rate: float,
        extra_info: str = "",
        metadata: Optional[Dict] = None
    ):
        """记录每层统计信息"""
        stats = {
            'layer_name': layer_name,
            'start_count': start_count,
            'end_count': end_count,
            'filtered_count': filtered_count,
            'filter_rate': filter_rate,
            'extra_info': extra_info,
        }
        if metadata:
            stats['metadata'] = metadata
        self.stats['layer_stats'].append(stats)

        if self.verbose:
            print(f"\n✅ {layer_name}")
            print(f"   输入: {start_count:,} 只")
            print(f"   输出: {end_count:,} 只")
            print(f"   筛除: {filtered_count:,} 只 ({filter_rate:.1f}%)")
            if extra_info:
                print(f"   {extra_info}")

    @staticmethod
    def _get_layer6_reason_stage_map() -> Dict[str, str]:
        """
        Layer6 旧 reason_code 到新 6A/6B/6C 语义阶段的兼容映射。

        该映射只用于审计和追踪，不改变 step_id=6 与 legacy reason_code。
        """
        return {
            'RISK_COHORT_TOO_SMALL': '6A',
            'RISK_CALCULABLE_COHORT_TOO_SMALL': '6A',
            'NO_NAV_HISTORY': '6A',
            'INVALID_NAV_DATES': '6A',
            'DUPLICATE_NAV_DATES': '6A',
            'NAV_DATA_GAP': '6A',
            'INSUFFICIENT_NAV_POINTS': '6A',
            'LOW_FREQUENCY_NAV_OBSERVATIONS': '6A',
            'INSUFFICIENT_NAV_COVERAGE': '6A',
            'RISK_METRICS_UNAVAILABLE': '6A',
            'CONSISTENCY_FAIL': '6B',
            'RISK_METRICS_FAIL': '6B',
            'ABSOLUTE_RETURN_FAIL': '6C',
            'DRAWDOWN_TOO_HIGH': '6C',
            'RISK_METRICS_QC_FAIL': '6C',
            'PASSED': '6C',
        }

    @classmethod
    def _get_layer6_stage_contract(cls) -> Dict[str, Dict[str, object]]:
        """返回 Layer6 6A/6B/6C 的语义契约，用于审计和回归校验。"""
        mapping = cls._get_layer6_reason_stage_map()
        return {
            '6A': {
                'name': '风险指标可计算性与数据质量门禁',
                'legacy_reason_codes': sorted(
                    code for code, stage in mapping.items() if stage == '6A'
                ),
                'hard_fail': True,
            },
            '6B': {
                'name': '同类相对风险表现',
                'legacy_reason_codes': sorted(
                    code for code, stage in mapping.items() if stage == '6B'
                ),
                'hard_fail': True,
            },
            '6C': {
                'name': '绝对收益与风险QC底线',
                'legacy_reason_codes': sorted(
                    code for code, stage in mapping.items() if stage == '6C'
                ),
                'hard_fail': True,
            },
        }

    @classmethod
    def _resolve_layer6_stage(cls, reason_code: Optional[str]) -> str:
        """根据 legacy reason_code 解析 Layer6 子阶段。"""
        if not reason_code:
            return '6C'
        return cls._get_layer6_reason_stage_map().get(reason_code, '6C')

    @classmethod
    def _annotate_layer6_metrics(
        cls,
        metrics: Optional[Dict],
        reason_code: Optional[str],
    ) -> Optional[Dict]:
        """
        为 Layer6 metrics 注入阶段语义，但保持 legacy reason_code 不变。
        """
        if metrics is None:
            return metrics

        result = dict(metrics)
        stage_code = result.get('layer6_stage') or cls._resolve_layer6_stage(reason_code)
        stage_contract = cls._get_layer6_stage_contract()
        result['layer6_stage'] = stage_code
        result['layer6_stage_name'] = stage_contract.get(stage_code, {}).get('name')
        result['layer6_contract_version'] = 'v1'
        result['legacy_step_id'] = 6
        result['legacy_reason_code'] = reason_code
        if 'pass_periods' in result and 'pass_count' not in result:
            result['pass_count'] = result['pass_periods']
        return result

    def _record_layer6_decision(
        self,
        *,
        fcode: str,
        step_id: int,
        step_name: str,
        passed: bool,
        reason_code: str,
        reason_detail: str,
        metrics: Optional[Dict] = None,
    ):
        """统一记录 Layer6 决策，注入 6A/6B/6C 语义审计字段。"""
        if not self.tracker:
            return

        self.tracker.record_decision(
            fcode=fcode,
            step_id=step_id,
            step_name=step_name,
            passed=passed,
            reason_code=reason_code,
            reason_detail=reason_detail,
            metrics=self._annotate_layer6_metrics(metrics, reason_code),
        )

    @classmethod
    def _build_layer6_calmar_floor_audit(
        cls,
        risk_df: pd.DataFrame,
        passed_fcodes: Optional[set[str]] = None,
        sample_limit: int = 12,
    ) -> Dict[str, Any]:
        """汇总 Layer6 Calmar 风格化分母下限，供 stats sidecar 审计。"""
        audit_cols = {
            'fcode',
            'calmar_style_tag',
            'calmar_min_drawdown_floor',
            'calmar_min_drawdown_floor_source',
            'calmar_effective_drawdown',
            'max_drawdown',
            'calmar_ratio',
        }
        if risk_df is None or risk_df.empty:
            return {
                'available': False,
                'reason': 'empty_risk_metrics',
            }
        if not audit_cols.intersection(risk_df.columns):
            return {
                'available': False,
                'reason': 'calmar_audit_columns_missing',
                'calculated_count': int(len(risk_df)),
            }

        def _clean_label(value: Any, missing_label: str = 'unknown') -> str:
            if value is None:
                return missing_label
            try:
                if pd.isna(value):
                    return missing_label
            except (TypeError, ValueError):
                pass
            text = str(value).strip()
            if not text or text.lower() in {'nan', 'none', 'null'}:
                return missing_label
            return text

        def _clean_number(value: Any, digits: int = 6) -> Optional[float]:
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if math.isnan(number) or math.isinf(number):
                return None
            return round(number, digits)

        def _count_records(frame: pd.DataFrame, col: str, output_col: str, limit: int = 20) -> List[Dict[str, Any]]:
            if col not in frame.columns:
                return []
            counts = frame[col].value_counts(dropna=False).head(limit)
            return [
                {
                    output_col: _clean_label(value),
                    'count': int(count),
                }
                for value, count in counts.items()
            ]

        working = risk_df.copy()
        if 'calmar_style_tag' in working.columns:
            working['_calmar_style_label'] = working['calmar_style_tag'].map(
                lambda value: _clean_label(value)
            )
        else:
            working['_calmar_style_label'] = 'unknown'
        if 'calmar_min_drawdown_floor_source' in working.columns:
            working['_calmar_floor_source_label'] = working['calmar_min_drawdown_floor_source'].map(
                lambda value: _clean_label(value)
            )
        else:
            working['_calmar_floor_source_label'] = 'unknown'
        if 'calmar_min_drawdown_floor' in working.columns:
            working['_calmar_floor_value'] = working['calmar_min_drawdown_floor'].map(_clean_number)
        else:
            working['_calmar_floor_value'] = None
        working['_calmar_floor_value_label'] = working['_calmar_floor_value'].map(
            lambda value: _clean_label(value)
        )

        passed_set = {str(code).strip() for code in passed_fcodes or set() if str(code).strip()}
        if passed_fcodes is not None and 'fcode' in working.columns:
            working['_layer6_passed'] = working['fcode'].map(lambda value: str(value).strip() in passed_set)
        else:
            working['_layer6_passed'] = None

        by_floor_style = (
            working
            .groupby(
                ['_calmar_floor_source_label', '_calmar_floor_value_label', '_calmar_style_label'],
                dropna=False,
            )
            .size()
            .reset_index(name='count')
            .sort_values('count', ascending=False)
            .head(30)
        )
        floor_style_counts = [
            {
                'floor_source': row['_calmar_floor_source_label'],
                'floor_value': _clean_number(row['_calmar_floor_value_label']),
                'style_tag': row['_calmar_style_label'],
                'count': int(row['count']),
            }
            for _, row in by_floor_style.iterrows()
        ]

        sample_cols = [
            col for col in [
                'fcode',
                'calmar_style_tag',
                'calmar_min_drawdown_floor',
                'calmar_min_drawdown_floor_source',
                'max_drawdown',
                'calmar_effective_drawdown',
                'calmar_ratio',
            ]
            if col in working.columns
        ]
        if sample_cols:
            sample_df = working.sort_values(
                ['_calmar_floor_value_label', '_calmar_style_label', 'fcode']
                if 'fcode' in working.columns else ['_calmar_floor_value_label', '_calmar_style_label']
            )
            sample_df = sample_df.groupby(
                ['_calmar_floor_value_label', '_calmar_floor_source_label'],
                dropna=False,
                group_keys=False,
            ).head(2)
            sample_rows = []
            for _, row in sample_df.head(sample_limit).iterrows():
                sample = {}
                for col in sample_cols:
                    if col in {'calmar_min_drawdown_floor', 'max_drawdown', 'calmar_effective_drawdown', 'calmar_ratio'}:
                        sample[col] = _clean_number(row.get(col), digits=6 if col != 'calmar_ratio' else 4)
                    else:
                        sample[col] = _clean_label(row.get(col), missing_label='')
                sample_rows.append(sample)
        else:
            sample_rows = []

        result: Dict[str, Any] = {
            'available': True,
            'scope': 'layer6_calculated_risk_metrics_before_relative_absolute_qc_gates',
            'calculated_count': int(len(working)),
            'style_tag_counts': _count_records(working, '_calmar_style_label', 'style_tag'),
            'floor_source_counts': _count_records(working, '_calmar_floor_source_label', 'floor_source'),
            'floor_value_counts': _count_records(working, '_calmar_floor_value_label', 'floor_value'),
            'floor_style_counts': floor_style_counts,
            'sample_rows': sample_rows,
        }
        if passed_fcodes is not None:
            passed_working = working[working['_layer6_passed'] == True]
            result['passed'] = {
                'passed_count': int(len(passed_working)),
                'rejected_after_calculation_count': int(len(working) - len(passed_working)),
                'floor_source_counts': _count_records(passed_working, '_calmar_floor_source_label', 'floor_source'),
                'floor_value_counts': _count_records(passed_working, '_calmar_floor_value_label', 'floor_value'),
            }
        return result

    @capture_call('screening_risk.fund', attributes=_CAPTURE_POLICY_FIELDS)
    def _calculate_layer6_single_fund(
        self,
        fcode: str,
        nav_data,
        risk_metric_value_cols: List[str],
        style_tag: Optional[str] = None,
        window_end_date: Optional[date_type] = None,
    ) -> Tuple[str, Optional[Dict], Dict]:
        """Layer6 6A：计算单只基金风险指标与可计算性元数据。"""
        nav_data, window_meta = self._prepare_layer6_nav_window(
            nav_data,
            window_end_date,
        )
        if nav_data is None or getattr(nav_data, 'empty', False):
            return fcode, None, {
                **window_meta,
                'status': 'NO_NAV_HISTORY',
                'nav_count': 0,
                'calmar_style_tag': style_tag,
            }

        min_data_points = 3 if self.period_days <= 7 else 10
        nav_count = len(nav_data)
        if nav_count < min_data_points:
            return fcode, None, {
                **window_meta,
                'status': 'INSUFFICIENT_NAV_POINTS',
                'nav_count': nav_count,
                'required_nav_points': min_data_points,
                'calmar_style_tag': style_tag,
            }

        if 'nav_date' not in nav_data.columns:
            return fcode, None, {
                **window_meta,
                'status': 'INVALID_NAV_DATES',
                'nav_count': nav_count,
                'invalid_date_count': nav_count,
                'calmar_style_tag': style_tag,
            }

        required_nav_points = self._get_layer6_min_nav_points()
        from bond_fund_screening.core.trading_calendar import expected_open_dates, continuity_evidence, SOURCE as calendar_source
        open_dates = (expected_open_dates(window_meta['requested_period_start'], window_meta['requested_period_end'])
                      if window_meta['requested_period_start'] is not None and window_meta['requested_period_end'] is not None
                      else None)
        if open_dates is not None:
            # Keep the 80% coverage ratio and minimum calculation sample. The
            # denominator uses verified open dates plus the return baseline.
            required_nav_points = max(min_data_points, math.ceil((len(open_dates) + 1) * 0.8))
        max_gap_threshold_days = self._get_layer6_max_gap_threshold_days()
        calendar_continuity = continuity_evidence(
            pd.to_datetime(nav_data['nav_date'], errors='coerce').dropna(),
            max_calendar_gap_days=max_gap_threshold_days,
        )
        observation_quality = assess_nav_observation_quality(
            nav_data['nav_date'],
            period_days=self.period_days,
            coverage_grace_days=self._get_layer6_period_coverage_grace_days(),
            required_observation_points=required_nav_points,
            max_gap_threshold_days=max_gap_threshold_days,
            window_start=window_meta['requested_period_start'],
            window_end=window_meta['requested_period_end'],
            max_boundary_age_days=self._get_layer6_start_boundary_lookback_days(),
        )
        quality_meta = {
            **window_meta,
            'nav_count': int(observation_quality['observation_count']),
            'required_nav_points': required_nav_points,
            'coverage_days': int(observation_quality['coverage_days']),
            'required_coverage_days': int(observation_quality['required_coverage_days']),
            'coverage_grace_days': int(observation_quality['coverage_grace_days']),
            'max_gap_days': int(observation_quality['max_gap_days']),
            'continuity_max_gap_days': int(observation_quality['continuity_max_gap_days']),
            'continuity_basis': observation_quality['continuity_basis'],
            'boundary_age_days': observation_quality['boundary_age_days'],
            'boundary_sufficient': observation_quality['boundary_sufficient'],
            'max_boundary_age_days': observation_quality['max_boundary_age_days'],
            'max_gap_threshold_days': max_gap_threshold_days,
            'invalid_date_count': int(observation_quality['invalid_date_count']),
            'duplicate_date_count': int(observation_quality['duplicate_date_count']),
            'calmar_style_tag': style_tag,
            'observation_calendar': dict(calendar_source) if open_dates is not None else None,
            'expected_open_day_count': len(open_dates) if open_dates is not None else None,
            'observation_coverage_ratio_required': 0.8,
            'calendar_continuity': calendar_continuity,
        }

        if quality_meta['invalid_date_count'] > 0:
            return fcode, None, {**quality_meta, 'status': 'INVALID_NAV_DATES'}
        if quality_meta['duplicate_date_count'] > 0:
            return fcode, None, {**quality_meta, 'status': 'DUPLICATE_NAV_DATES'}
        if not observation_quality['observation_density_sufficient']:
            return fcode, None, {
                **quality_meta,
                'status': 'LOW_FREQUENCY_NAV_OBSERVATIONS',
            }
        if not observation_quality['coverage_sufficient']:
            return fcode, None, {**quality_meta, 'status': 'INSUFFICIENT_NAV_COVERAGE'}
        if not observation_quality['continuity_sufficient']:
            return fcode, None, {**quality_meta, 'status': 'NAV_DATA_GAP'}
        if not calendar_continuity['verified']:
            return fcode, None, {**quality_meta, 'status': 'NAV_DATA_GAP'}

        metrics = self.risk_calculator.calculate_all_metrics(
            nav_data,
            self.period_years,
            style_tag=style_tag,
        )
        metrics['nav_count'] = nav_count
        valid_metric_count = sum(
            1 for col in risk_metric_value_cols if pd.notna(metrics.get(col))
        )
        metrics['valid_risk_metric_count'] = int(valid_metric_count)
        status = 'OK' if valid_metric_count > 0 else 'RISK_METRICS_UNAVAILABLE'
        calc_meta = {
            **quality_meta,
            'status': status,
            'valid_risk_metric_count': int(valid_metric_count),
            'calmar_style_tag': metrics.get('calmar_style_tag'),
            'calmar_min_drawdown_floor': metrics.get('calmar_min_drawdown_floor'),
            'calmar_min_drawdown_floor_source': metrics.get('calmar_min_drawdown_floor_source'),
        }
        if status != 'OK':
            return fcode, None, calc_meta
        return fcode, metrics, calc_meta

    @classmethod
    def _build_layer6_style_tag_map(cls, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """为 Layer6 风险计算解析每只基金的 Calmar 风格标签。"""
        if df is None or df.empty or 'fcode' not in df.columns:
            return {}

        result: Dict[str, Optional[str]] = {}
        for _, row in df.iterrows():
            fcode = str(row.get('fcode') or '').strip()
            if not fcode:
                continue
            result[fcode] = cls._resolve_layer6_calmar_style_tag(row)
        return result

    @classmethod
    def _resolve_layer6_calmar_style_tag(cls, row) -> Optional[str]:
        """从候选行解析 Calmar 风格标签，优先使用项目正式风格字段。"""
        for col in CALMAR_STYLE_TAG_COLUMNS:
            try:
                value = row.get(col)
            except AttributeError:
                value = None
            normalized = cls._normalize_layer6_calmar_style_tag(value)
            if normalized:
                return normalized

        ftype = cls._get_row_value(row, 'ftype')
        sub_type = cls._get_row_value(row, 'sub_type')
        benchmark_family = cls._get_row_value(row, 'benchmark_family')
        benchmark = cls._get_row_value(row, 'benchmark')
        full_text = "\n".join(
            str(value or "").strip()
            for value in (ftype, sub_type, benchmark_family, benchmark)
            if str(value or "").strip()
        )

        if '混合二级' in full_text:
            return '二级债增强'
        if '混合一级' in full_text:
            return '一级债增强'
        if '可转债' in full_text or '转债' in full_text:
            return '转债增强'
        if '中短债' in full_text or '短债' in full_text:
            return '中短债'
        if '指数型-固收' in full_text or ('指数' in full_text and '固收' in full_text):
            return '固收指数'
        if '利率债' in full_text or '政金债' in full_text or '政策性金融债' in full_text:
            return '利率债'
        if '信用债' in full_text or '中高等级' in full_text:
            return '信用债'
        if '现金增强' in full_text or '同业存单' in full_text:
            return '现金增强'
        if canonicalize_runtime_fund_type_key(ftype, default=None) == 'bond':
            return '综合纯债'
        return None

    @staticmethod
    def _get_row_value(row, key: str):
        try:
            return row.get(key)
        except AttributeError:
            return None

    @staticmethod
    def _normalize_layer6_calmar_style_tag(value) -> Optional[str]:
        text = str(value or "").strip()
        if not text or text.lower() in {'nan', 'none', 'null'}:
            return None
        return text

    def _layer6_stage_6b_relative_gate(
        self,
        df: pd.DataFrame,
        risk_df: pd.DataFrame,
        available_cols: List[str],
        risk_metric_value_cols: List[str],
        structurally_excluded_risk_cols: List[str],
        bucket_col: str,
    ):
        """Layer6 6B：执行同类相对风险比较，返回通过门禁所需上下文。"""
        bucket_medians, global_medians, bucket_sizes = self._compute_bucket_medians(
            risk_df, available_cols, bucket_col
        )
        global_medians_dict = global_medians.to_dict()

        pass_periods_map = {}
        bucket_info_map = {}
        for _, row in risk_df.iterrows():
            fcode = row['fcode']
            row_bucket_key = row.get(bucket_col, '')
            medians_for_row, bucket_size, is_fallback = self._get_bucket_info_for_row(
                row_bucket_key, bucket_medians, global_medians, bucket_sizes, available_cols
            )

            negative_risk_metrics = {'max_drawdown', 'volatility', 'consistency'}
            count = 0
            nan_median_count = 0
            col_medians = {}
            for col in available_cols:
                median_val = medians_for_row.get(col, global_medians_dict.get(col))
                col_medians[col] = median_val
                if pd.notna(median_val):
                    if pd.notna(row.get(col)):
                        if col in negative_risk_metrics:
                            passed = row[col] <= median_val
                        else:
                            passed = row[col] >= median_val
                        if passed:
                            count += 1
                else:
                    nan_median_count += 1

            pass_periods_map[fcode] = count
            bucket_info_map[fcode] = {
                'bucket_key': row_bucket_key,
                'bucket_size': bucket_size,
                'is_fallback': is_fallback,
                'col_medians': col_medians,
                'nan_auto_pass': nan_median_count,
            }

        effective_cols = [
            c for c in risk_metric_value_cols
            if c in available_cols and pd.notna(global_medians_dict.get(c))
        ]
        n_cols = len(effective_cols)
        configured_required_pass = getattr(self, 'risk_pass_count', self.consistency_pass_count)
        try:
            configured_required_pass = int(configured_required_pass)
        except (TypeError, ValueError):
            configured_required_pass = 1
        configured_required_pass = max(1, configured_required_pass)
        effective_configured_required_pass = configured_required_pass
        required_pass_notes: List[str] = []
        excluded_risk_col_count = len(structurally_excluded_risk_cols)
        if excluded_risk_col_count:
            effective_configured_required_pass = max(
                1,
                effective_configured_required_pass - excluded_risk_col_count,
            )
            if self.period_days <= 30:
                required_pass_notes.append(
                    "短周期排除失真指标"
                    + "/".join(structurally_excluded_risk_cols)
                    + "，阈值同步调整"
                )
            elif self.period_days <= 180:
                required_pass_notes.append(
                    "中短周期排除失真指标"
                    + "/".join(structurally_excluded_risk_cols)
                )

        if n_cols == 0:
            required_pass = 0
        else:
            required_pass = min(effective_configured_required_pass, n_cols)

        return (
            pass_periods_map,
            bucket_info_map,
            bucket_sizes,
            global_medians_dict,
            effective_cols,
            n_cols,
            configured_required_pass,
            required_pass,
            required_pass_notes,
        )

    def _layer6_stage_6c_absolute_qc_gate(
        self,
        df: pd.DataFrame,
        qc_enabled: bool,
        qc_mode: str,
        qc_issue_map: Dict[str, List[str]],
    ) -> Tuple[float, pd.Series, pd.Series]:
        """Layer6 6C：执行绝对收益与风险QC门禁。"""
        if isinstance(self.min_absolute_return, dict):
            period_key = int(self.period_years) if self.period_years >= 1 else round(self.period_years, 2)
            min_absolute_return = self.min_absolute_return.get(period_key, 0.0)
        else:
            min_absolute_return = float(self.min_absolute_return) if self.min_absolute_return else 0.0

        mask_nan_return = df['main_return'].isna()
        mask_absolute = df['main_return'] >= min_absolute_return
        mask_absolute = mask_absolute | mask_nan_return
        mask_qc = pd.Series(True, index=df.index)
        if qc_enabled and qc_mode == 'hard' and qc_issue_map:
            mask_qc = ~df['fcode'].isin(set(qc_issue_map.keys()))

        return min_absolute_return, mask_absolute, mask_qc

    def _get_layer6_risk_metric_value_cols(self) -> Tuple[List[str], List[str]]:
        """
        返回 Layer6 参与相对比较的风险指标列。

        基于当前真实产物分布做结构性排除：

        - 一周期/一月期（<=30天）排除 `sharpe_ratio / sortino_ratio / calmar_ratio`
        - 三月期/半年期（<=180天）排除 `sortino_ratio`

        原因：
        - 超短窗口里年化收益与下行波动会被极小分母放大；
        - 真实产物中 `sortino_ratio` 在 7/30/90/180 天窗口普遍失去区分力；
        - `calmar_ratio` 在 7/30 天窗口也会因回撤接近 0 而大面积失真。
        """
        cols = [
            'sharpe_ratio',
            'sortino_ratio',
            'calmar_ratio',
            'max_drawdown',
            'volatility',
            'consistency',
        ]
        excluded_cols: List[str] = []
        if self.period_days <= 180:
            cols = [col for col in cols if col != 'sortino_ratio']
            excluded_cols.append('sortino_ratio')
        if self.period_days <= 30:
            for col in ('sharpe_ratio', 'calmar_ratio'):
                if col in cols:
                    cols = [item for item in cols if item != col]
                    excluded_cols.append(col)
        return cols, excluded_cols

    @staticmethod
    def _format_threshold_percent(value: float) -> str:
        """统一格式化百分比阈值，避免 0.05 被误显示为 0.1。"""
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)

        formatted = f"{numeric:.2f}".rstrip('0').rstrip('.')
        if formatted in {'-0', '-0.0', '-0.00'}:
            formatted = '0'
        return f"{formatted}%"

    @staticmethod
    def _build_risk_qc_issue_samples(
        df: pd.DataFrame,
        qc_issue_map: Dict[str, List[str]],
        bucket_col: str,
        limit: int = 5,
    ) -> List[Dict]:
        """构建风险QC异常基金样本，增强日志与产物的可追溯性。"""
        if df is None or df.empty or not qc_issue_map:
            return []

        detail_columns = ['fcode']
        if 'shortname' in df.columns:
            detail_columns.append('shortname')
        if bucket_col in df.columns:
            detail_columns.append(bucket_col)

        detail_rows = (
            df[detail_columns]
            .drop_duplicates(subset=['fcode'])
            .set_index('fcode')
            .to_dict('index')
        )

        samples = []
        for fcode in sorted(qc_issue_map.keys())[:max(0, limit)]:
            row = detail_rows.get(fcode, {})
            sample = {
                'fcode': fcode,
                'issues': list(qc_issue_map.get(fcode, [])),
            }
            if 'shortname' in row:
                sample['shortname'] = row.get('shortname')
            if bucket_col in row:
                sample['bucket_key'] = row.get(bucket_col)
            samples.append(sample)

        return samples


# ============================================================================
# 测试函数
# ============================================================================

def test_filtering_engine():
    """测试筛选引擎"""
    print("="*80)
    print("测试 FilteringEngine 模块")


# ============================================================================
# 辅助方法：净值连续性检查
# ============================================================================

logger = logging.getLogger(__name__)


def _check_nav_continuity_impl(nav_data) -> int:
    """
    检查净值数据的连续性，返回最大缺口天数

    Args:
        nav_data: DataFrame，必须含 'nav_date' 列

    Returns:
        最大缺口天数（无数据时返回 9999 表示数据不足）
    """
    if nav_data is None or len(nav_data) < 2:
        return 9999  # 数据不足，视为极大缺口
    try:
        dates = pd.to_datetime(nav_data['nav_date']).sort_values()
        gaps = dates.diff().dropna().dt.days
        return int(gaps.max()) if len(gaps) > 0 else 0
    except Exception as e:
        logger.warning(f"净值连续性检查异常: {e}")
        return 9999


if __name__ == "__main__":
    test_filtering_engine()
