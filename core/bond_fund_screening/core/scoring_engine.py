#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评分引擎模块 - ScoringEngine

实现双评分系统：
- 短期潜力分（40%）: 月涨幅25% + 周涨幅20% + 3月收益15% + 夏普比率20% + 波动率20%
- 长期价值分（60%）: 年涨幅30% + 稳定性25% + 最大回撤20% + 索提诺15% + 卡玛10%
- 组合评分: 短期40% + 长期60%

支持通过Profile配置不同基金类型的评分权重和绝对基准
"""

import pandas as pd
from typing import Any, Dict, Optional, Union
from pathlib import Path
import sys
from bond_fund_screening.config import (
    SHORT_TERM_WEIGHTS,
    SHORT_TERM_WEIGHTS_AGGRESSIVE,
    SHORT_TERM_WEIGHTS_BALANCED,
    LONG_TERM_WEIGHTS,
    LONG_TERM_WEIGHTS_AGGRESSIVE,
    LONG_TERM_WEIGHTS_BALANCED,
    SCORE_COMBO_WEIGHTS,
    SCORE_COMBO_WEIGHTS_AGGRESSIVE,
    SCORE_COMBO_WEIGHTS_BALANCED,
    WEIGHT_PROFILES,
    NORMALIZATION_WEIGHTS,
    ABSOLUTE_BASELINES,
    SCORING_SHADOW_CONFIG,
    SCORING_POLICY,
    SCORING_POLICY_GUARDS,
    CONSISTENCY_V2_CONFIG,
)

# 导入Profile支持
sys.path.append(str(Path(__file__).parent.parent.parent))
try:
    from config.fund_type_registry import FundTypeProfile, get_profile
    HAS_PROFILE = True
except ImportError:
    FundTypeProfile = None
    get_profile = None
    HAS_PROFILE = False


def select_score_key(
    fund_type: str = 'bond',
    period_years: int = 3,
    override: str = None,
    policy: Dict = None,
) -> str:
    """
    根据配置选择最终评分列名。

    优先级：override > per_fund_type > default_score_key

    Args:
        fund_type: 基金类型（bond/stock/hybrid）
        period_years: 分析周期（年）
        override: CLI 或调用方显式指定的 score_key（最高优先级）
        policy: SCORING_POLICY 字典（默认从 config 读取）

    Returns:
        选中的 score_key 字符串
    """
    if override:
        return override

    if policy is None:
        policy = SCORING_POLICY

    fund_type = canonicalize_runtime_fund_type_key(fund_type, default='bond')
    key = resolve_per_fund_type_score_key(
        policy.get('per_fund_type', {}),
        fund_type=fund_type,
        period_years=period_years,
    )
    if key:
        return key

    return policy.get('default_score_key', 'combo_score')


def _period_key_candidates(period_years: Union[int, float, str, None]) -> list:
    """为策略 period key 生成兼容查找候选。"""
    candidates = []

    def _add(value):
        if value is None:
            return
        value = str(value)
        if value and value not in candidates:
            candidates.append(value)

    _add(period_years)

    try:
        numeric = float(period_years)
    except (TypeError, ValueError):
        return candidates

    if numeric.is_integer():
        _add(int(numeric))

    normalized = f"{numeric:.2f}".rstrip('0').rstrip('.')
    _add(normalized)
    return candidates


def canonicalize_runtime_fund_type_key(
    fund_type: Any,
    default: Optional[str] = 'bond',
) -> Optional[str]:
    """
    将运行时 fund_type 归一化为单一策略键。

    DataLoader 可接受列表语义（例如 `bond -> ['31', '32']`），但策略选择、
    阈值查表与追踪表结构仍按单个 bucket 设计；这里仅负责把运行时入口
    归一化成稳定的单值键，不改变数据加载口径。
    """

    def _normalize_scalar(value: Any) -> Optional[str]:
        if value is None:
            return None

        if hasattr(value, 'name') and hasattr(value, 'value'):
            for candidate in (getattr(value, 'name', None), getattr(value, 'value', None)):
                normalized = _normalize_scalar(candidate)
                if normalized:
                    return normalized
            return None

        text = str(value).strip()
        if not text:
            return None

        lowered = text.lower()
        if lowered in {'bond', 'fixed_income', '31', '32', '003', '008', '009', '2949'}:
            return 'bond'
        if lowered in {'stock', 'equity', '25', '26'}:
            return 'stock'
        if lowered in {'hybrid', '27'}:
            return 'hybrid'
        if lowered in {'money', 'money_market', '35'}:
            return 'money'
        if lowered in {'qdii', 'alternative', '36'}:
            return 'qdii'
        if lowered in {'fof', 'multi_asset', '37'}:
            return 'fof'

        if text == '指数型-固收':
            return 'bond'
        if text.startswith(('债券', '定开债', '理财', '短期理财')):
            return 'bond'
        if text.startswith('混合'):
            return 'hybrid'
        if text.startswith('货币'):
            return 'money'
        if text.startswith('QDII'):
            return 'qdii'
        if text.startswith('FOF'):
            return 'fof'
        if text == '指数型' or text.startswith('指数型-') or text.startswith('股票'):
            return 'stock'

        return None

    if isinstance(fund_type, list):
        for item in fund_type:
            normalized = canonicalize_runtime_fund_type_key(item, default=None)
            if normalized:
                return normalized
        return default

    return _normalize_scalar(fund_type) or default


def resolve_per_fund_type_score_key(
    per_fund_type: Dict,
    fund_type: Union[str, list] = 'bond',
    period_years: Union[int, float, str, None] = None,
) -> Optional[str]:
    """
    兼容解析 per_fund_type 中的 score_key。

    支持两种口径：
    1) {'bond': 'shadow_combo_score_abs20'}
    2) {'bond': {'3': 'shadow_combo_score_abs20'}}
    """
    if not isinstance(per_fund_type, dict):
        return None

    runtime_fund_type = canonicalize_runtime_fund_type_key(fund_type, default=None)
    if not runtime_fund_type:
        return None

    ft_config = per_fund_type.get(runtime_fund_type)
    if isinstance(ft_config, str) and ft_config:
        return ft_config

    if not isinstance(ft_config, dict):
        return None

    for key in _period_key_candidates(period_years):
        score_key = ft_config.get(key)
        if isinstance(score_key, str) and score_key:
            return score_key

    return None


class ScoringEngine:
    """
    评分引擎类

    提供双评分系统的计算方法，支持多种权重配置和基金类型Profile
    """

    def __init__(
        self,
        weight_profile: str = 'standard',
        fund_type_profile: Optional['FundTypeProfile'] = None,
        period_years: float = 1.0,
    ):
        """
        初始化评分引擎

        Args:
            weight_profile: 权重配置类型
                - 'standard': 标准配置（用于1-5年期，默认）
                - 'aggressive': 激进型配置（用于1周/1月，70%短期+30%长期）
                - 'balanced': 平衡型配置（用于3月/半年，55%短期+45%长期）
            fund_type_profile: 基金类型配置Profile（可选，用于自定义权重和基准）
                如果指定此参数，将优先使用Profile中的配置
            period_years: 分析周期（年），用于年化收益率计算
                v1.9.57 P0-5: 从外部猴子补丁改为构造函数参数，确保独立使用时不会缺失
        """
        self.period_years = period_years
        self.fund_type_profile = fund_type_profile

        # 从Profile或默认配置加载权重和基准
        self._load_weights(weight_profile)

        # 保存配置类型
        self.weight_profile = weight_profile

    def _load_weights(self, weight_profile: str):
        """从Profile或默认配置加载权重和基准"""
        if self.fund_type_profile is not None:
            # 使用Profile配置
            self.short_term_weights = self.fund_type_profile.short_term_weights
            self.long_term_weights = self.fund_type_profile.long_term_weights
            self.combo_weights = self.fund_type_profile.combo_weights
            self.absolute_baselines = self.fund_type_profile.absolute_baselines
        else:
            # 使用默认配置（向后兼容）
            if weight_profile == 'aggressive':
                self.short_term_weights = SHORT_TERM_WEIGHTS_AGGRESSIVE
                self.combo_weights = SCORE_COMBO_WEIGHTS_AGGRESSIVE
                self.long_term_weights = LONG_TERM_WEIGHTS_AGGRESSIVE
            elif weight_profile == 'balanced':
                self.short_term_weights = SHORT_TERM_WEIGHTS_BALANCED
                self.combo_weights = SCORE_COMBO_WEIGHTS_BALANCED
                self.long_term_weights = LONG_TERM_WEIGHTS_BALANCED
            else:
                # 标准配置（默认）
                self.short_term_weights = SHORT_TERM_WEIGHTS
                self.combo_weights = SCORE_COMBO_WEIGHTS
                self.long_term_weights = LONG_TERM_WEIGHTS

            # 使用默认绝对基准
            self.absolute_baselines = ABSOLUTE_BASELINES

        # v1.9.57 P2-4: 按周期调整绝对基准
        # annual_return 的 good/bad 基准在不同周期下含义不同：
        # - 长周期(≥3年): 年化6%对债券偏高，降至4%/good, -2%/bad
        # - 短周期(<1年): 年化收益噪声大，放宽至3%/good, -1%/bad
        # - 中期(1-3年): 保持默认 6%/good, 2%/bad
        # 深拷贝避免污染全局配置或Profile原始数据
        self.absolute_baselines = dict(self.absolute_baselines)
        if self.period_years and self.period_years >= 3:
            self.absolute_baselines['annual_return'] = {
                'good': 4.0, 'bad': -2.0, 'direction': 'positive',
            }
            self.absolute_baselines['main_return'] = {
                'good': 4.0, 'bad': -2.0, 'direction': 'positive',
            }
        elif self.period_years and self.period_years < 1:
            self.absolute_baselines['annual_return'] = {
                'good': 3.0, 'bad': -1.0, 'direction': 'positive',
            }
            self.absolute_baselines['main_return'] = {
                'good': 3.0, 'bad': -1.0, 'direction': 'positive',
            }

    def _get_annual_return_series(self, df: pd.DataFrame) -> pd.Series:
        """统一构造年化收益序列，避免主评分与影子评分重复计算。"""
        if 'main_return' in df.columns and hasattr(self, 'period_years') and self.period_years and self.period_years > 0:
            # 保护：main_return < -100% 时 (1 + x) 为负数，分数次幂产生复数/NaN
            growth_factor = (1 + df['main_return'] / 100).clip(lower=0.001)
            return (growth_factor ** (1 / self.period_years) - 1) * 100
        if 'return_1y' in df.columns:
            return df['return_1y']
        return pd.Series(0.0, index=df.index)

    @staticmethod
    def _resolve_working_risk_ratio_series(
        df: pd.DataFrame,
        metric_name: str,
    ) -> Optional[pd.Series]:
        """
        统一评分/排名阶段的风险比率工作口径。

        `fund_risk_metrics.sharpe_ratio/sortino_ratio` 当前仍保留展示封顶值，
        真值存放在 `raw_*` 字段。评分与排名不能继续直接使用封顶列，否则高值
        基金会被压成同分。这里只在工作阶段优先提升 raw 列，不改写原始列语义。
        """
        raw_col = f'raw_{metric_name}'
        base_series = None
        if metric_name in df.columns:
            base_series = pd.to_numeric(df[metric_name], errors='coerce')

        if raw_col not in df.columns:
            return base_series

        raw_series = pd.to_numeric(df[raw_col], errors='coerce')
        if base_series is None:
            return raw_series
        return raw_series.combine_first(base_series)

    def _get_period_days(self) -> Optional[int]:
        """统一将 period_years 折算为自然日，用于短周期结构性失真保护。"""
        if self.period_years is None:
            return None
        try:
            period_days = int(round(float(self.period_years) * 365))
        except (TypeError, ValueError):
            return None
        return max(period_days, 0)

    def _get_structurally_excluded_risk_metrics(self) -> set[str]:
        """
        返回当前周期应结构性排除的风险比率。

        基于真实产物核对：
        - 7 天 / 30 天窗口里 `sharpe_ratio / sortino_ratio / calmar_ratio` 大面积失真；
        - 90 天 / 180 天窗口里 `sortino_ratio` 仍普遍被极低下行波动放大。
        """
        period_days = self._get_period_days()
        excluded: set[str] = set()
        if period_days is None:
            return excluded
        if period_days <= 180:
            excluded.add('sortino_ratio')
        if period_days <= 30:
            excluded.update({'sharpe_ratio', 'calmar_ratio'})
        return excluded

    def _build_score_feature_series(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        预构造评分所需的指标序列。

        主评分、影子评分与 cstv2 会重复使用同一批特征，统一预处理可减少
        to_numeric / 年化收益换算 / 负向指标取反等重复开销。
        """
        features: Dict[str, pd.Series] = {
            'annual_return': self._get_annual_return_series(df),
        }
        excluded_metrics = self._get_structurally_excluded_risk_metrics()

        direct_columns = (
            'return_1w',
            'return_1m',
            'return_3m',
            'sharpe_ratio',
            'sortino_ratio',
            'calmar_ratio',
            'positive_month_ratio',
        )
        for col in direct_columns:
            if col in excluded_metrics:
                continue

            if col in {'sharpe_ratio', 'sortino_ratio'}:
                working_series = self._resolve_working_risk_ratio_series(df, col)
                if working_series is not None:
                    features[col] = working_series
                continue

            if col in df.columns:
                features[col] = pd.to_numeric(df[col], errors='coerce')

        numeric_negative_columns = (
            'volatility',
            'consistency',
            'max_drawdown',
            'max_drawdown_duration',
            'total_fee_rate',
        )
        for col in numeric_negative_columns:
            if col in df.columns:
                features[col] = -pd.to_numeric(df[col], errors='coerce')

        return features

    def _build_normalization_components(
        self,
        series: pd.Series,
        metric_name: str = None,
    ) -> Dict[str, pd.Series]:
        """拆分归一化中的相对排名与绝对基准部分，供多种权重复用。"""
        empty_result = pd.Series(0.0, index=series.index)
        if series.isna().all() or len(series) == 0:
            return {
                'empty_result': empty_result,
                'valid_mask': pd.Series(False, index=series.index),
            }

        valid_mask = series.notna()
        if valid_mask.sum() == 0:
            return {
                'empty_result': empty_result,
                'valid_mask': valid_mask,
            }

        relative_score = pd.Series(0.0, index=series.index)
        relative_score[valid_mask] = series[valid_mask].rank(pct=True)

        absolute_bonus = pd.Series(0.0, index=series.index)
        if metric_name and metric_name in self.absolute_baselines:
            baseline = self.absolute_baselines[metric_name]
            good_value = baseline['good']
            bad_value = baseline['bad']
            direction = baseline['direction']

            if good_value == bad_value:
                absolute_bonus[valid_mask] = 0.5
            elif direction == 'positive':
                absolute_bonus[valid_mask] = series[valid_mask].apply(
                    lambda x: 1.0 if x >= good_value else (
                        0.0 if x <= bad_value else (x - bad_value) / (good_value - bad_value)
                    )
                )
            else:
                original_values = -series[valid_mask]
                absolute_bonus[valid_mask] = original_values.apply(
                    lambda x: 1.0 if x <= good_value else (
                        0.0 if x >= bad_value else (bad_value - x) / (bad_value - good_value)
                    )
                )

        return {
            'relative_score': relative_score,
            'absolute_bonus': absolute_bonus,
            'valid_mask': valid_mask,
        }

    def _compose_normalized_score(
        self,
        components: Dict[str, pd.Series],
        relative_weight: float,
        absolute_weight: float,
        penalty_mode: str,
    ) -> pd.Series:
        """按指定权重组合归一化结果，并保留原有 NaN 惩罚规则。"""
        empty_result = components.get('empty_result')
        if empty_result is not None:
            return empty_result

        relative_score = components['relative_score']
        absolute_bonus = components['absolute_bonus']
        valid_mask = components['valid_mask']
        normalized = relative_score * relative_weight + absolute_bonus * absolute_weight
        normalized = normalized.copy()

        n_valid = int(valid_mask.sum())
        if penalty_mode == 'default':
            if n_valid >= 20:
                nan_penalty = max(normalized[valid_mask].quantile(0.05) * 0.5, 0.0001)
            elif n_valid >= 10:
                nan_penalty = max(normalized[valid_mask].quantile(0.05) * 0.3, 0.0001)
            elif n_valid >= 3:
                nan_penalty = max(normalized[valid_mask].min() * 0.3, 0.0001)
            else:
                nan_penalty = 0.0001
        else:
            if n_valid >= 10:
                nan_penalty = max(normalized[valid_mask].quantile(0.01) * 0.5, 0.0001)
            elif n_valid >= 3:
                nan_penalty = max(normalized[valid_mask].min() * 0.3, 0.0001)
            else:
                nan_penalty = 0.0001

        normalized[~valid_mask] = nan_penalty
        return normalized

    def _build_normalized_score_cache(
        self,
        feature_series: Dict[str, pd.Series],
        abs_weights: list,
    ) -> Dict[str, Dict]:
        """
        为主评分与影子评分预计算归一化结果。

        影子评分之间的主要区别仅在 absolute_weight，不必反复做 rank / absolute
        baseline 计算。
        """
        cache = {
            'default': {},
            'variant': {},
        }
        for abs_weight in abs_weights:
            cache['variant'][abs_weight] = {}

        relative_weight = NORMALIZATION_WEIGHTS['relative']
        absolute_weight = NORMALIZATION_WEIGHTS['absolute']

        for metric_name, series in feature_series.items():
            components = self._build_normalization_components(series, metric_name)
            cache['default'][metric_name] = self._compose_normalized_score(
                components,
                relative_weight=relative_weight,
                absolute_weight=absolute_weight,
                penalty_mode='default',
            )
            for abs_weight in abs_weights:
                cache['variant'][abs_weight][metric_name] = self._compose_normalized_score(
                    components,
                    relative_weight=1.0 - abs_weight,
                    absolute_weight=abs_weight,
                    penalty_mode='variant',
                )

        return cache

    def _get_cached_normalized_score(
        self,
        metric_name: str,
        series: pd.Series,
        normalized_cache: Optional[Dict[str, Dict]] = None,
        abs_weight: Optional[float] = None,
    ) -> pd.Series:
        """优先复用预计算结果，缺失时回退到原始归一化逻辑。"""
        if normalized_cache is not None:
            if abs_weight is None:
                cached = normalized_cache.get('default', {}).get(metric_name)
            else:
                cached = normalized_cache.get('variant', {}).get(abs_weight, {}).get(metric_name)
            if cached is not None:
                return cached

        if abs_weight is None:
            return self._normalize_score(series, metric_name)
        return self._normalize_score_variant(series, metric_name, abs_weight)

    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有基金的双评分

        Args:
            df: 包含收益率和风险指标的DataFrame

        Returns:
            添加了评分列的DataFrame
        """
        df = df.copy()
        feature_series = self._build_score_feature_series(df)
        shadow_weights = []
        shadow_cfg = SCORING_SHADOW_CONFIG
        if shadow_cfg.get('enabled', False):
            shadow_weights = list(shadow_cfg.get('absolute_weight_candidates', []))
        normalized_cache = self._build_normalized_score_cache(feature_series, shadow_weights)

        # 计算短期潜力分
        df['short_term_score'] = self._calculate_short_term_score(
            df,
            feature_series=feature_series,
            normalized_cache=normalized_cache,
        )

        # 计算长期价值分
        df['long_term_score'] = self._calculate_long_term_score(
            df,
            feature_series=feature_series,
            normalized_cache=normalized_cache,
        )

        # 计算组合评分
        df['combo_score'] = (
            df['short_term_score'] * self.combo_weights['short_term'] +
            df['long_term_score'] * self.combo_weights['long_term']
        )

        # Step 7-2: 影子评分（不改变主评分，仅追加额外列用于 A/B 对照）
        if shadow_cfg.get('enabled', False):
            for abs_w in shadow_weights:
                pct = int(abs_w * 100)
                label = shadow_cfg.get('label_fmt', 'abs{pct}').format(pct=pct)
                shadow_st = self._calculate_shadow_short_term_score(
                    df,
                    abs_w,
                    feature_series=feature_series,
                    normalized_cache=normalized_cache,
                )
                shadow_lt = self._calculate_shadow_long_term_score(
                    df,
                    abs_w,
                    feature_series=feature_series,
                    normalized_cache=normalized_cache,
                )
                df[f'shadow_short_term_score_{label}'] = shadow_st
                df[f'shadow_long_term_score_{label}'] = shadow_lt
                df[f'shadow_combo_score_{label}'] = (
                    shadow_st * self.combo_weights['short_term'] +
                    shadow_lt * self.combo_weights['long_term']
                )

        # Step 7-6: 一致性V2 shadow 评分（用 positive_month_ratio 替代 consistency）
        cstv2_cfg = CONSISTENCY_V2_CONFIG
        if cstv2_cfg.get('enabled', False) and 'positive_month_ratio' in df.columns:
            cstv2_label = cstv2_cfg.get('label', 'cstv2')
            shadow_lt_cstv2 = self._calculate_cstv2_long_term_score(
                df,
                feature_series=feature_series,
                normalized_cache=normalized_cache,
            )
            shadow_st_cstv2 = df['short_term_score']  # 短期分不变
            df[f'shadow_combo_score_{cstv2_label}'] = (
                shadow_st_cstv2 * self.combo_weights['short_term'] +
                shadow_lt_cstv2 * self.combo_weights['long_term']
            )

        # Step 7-4/7-5: 评分策略选择 + guarded fallback
        chosen_key = select_score_key(
            fund_type=getattr(self, '_fund_type', 'bond'),
            period_years=getattr(self, '_period_years', 3),
            override=getattr(self, '_score_key_override', None),
        )
        is_override = bool(getattr(self, '_score_key_override', None))
        guards = getattr(self, '_policy_guards', SCORING_POLICY_GUARDS)

        baseline_key = 'combo_score'
        fallback = False
        reason = ''
        final_key = baseline_key

        # baseline 自身不是“fallback”，否则会把正常基线路径误记为 low_rows /
        # override_used，污染 rollout 监控、CSV 产物和策略评估。
        if chosen_key != baseline_key:
            if chosen_key not in df.columns:
                fallback = True
                reason = 'missing_column'
            else:
                col = df[chosen_key]
                n_rows = len(df)
                coverage = col.notna().mean() if n_rows > 0 else 0.0
                std_val = col.std(skipna=True) if n_rows > 0 else 0.0

                if n_rows < guards.get('min_rows', 200):
                    fallback = True
                    reason = 'low_rows'
                elif coverage < guards.get('min_coverage', 0.80):
                    fallback = True
                    reason = 'low_coverage'
                elif guards.get('min_std_nonzero', True) and (pd.isna(std_val) or std_val == 0):
                    fallback = True
                    reason = 'zero_variance'

            if not fallback:
                final_key = chosen_key
                if is_override:
                    reason = 'override_used'

        df['selected_score_key'] = final_key
        df['selected_combo_score'] = df[final_key]
        df['selected_score_fallback'] = fallback
        df['selected_score_reason'] = reason

        return df

    def _calculate_short_term_score(
        self,
        df: pd.DataFrame,
        feature_series: Optional[Dict[str, pd.Series]] = None,
        normalized_cache: Optional[Dict[str, Dict]] = None,
    ) -> pd.Series:
        """
        计算短期潜力分

        组成（P0-E1修复后）：
        - return_1m: 20%
        - return_3m: 20%
        - sharpe_ratio: 30%
        - sortino_ratio: 15%
        - volatility: 15%（负向，低波动高分）
        """
        score = pd.Series(0.0, index=df.index)
        feature_series = feature_series or self._build_score_feature_series(df)

        # 月涨幅（20%）
        if 'return_1m' in self.short_term_weights and 'return_1m' in feature_series:
            score += self._get_cached_normalized_score(
                'return_1m', feature_series['return_1m'], normalized_cache=normalized_cache
            ) * self.short_term_weights['return_1m']

        # 周涨幅（标准配置已删除 return_1w，但 aggressive/balanced 模式仍定义此权重，需保留兼容）
        if 'return_1w' in self.short_term_weights and 'return_1w' in feature_series:
            score += self._get_cached_normalized_score(
                'return_1w', feature_series['return_1w'], normalized_cache=normalized_cache
            ) * self.short_term_weights['return_1w']

        # 3月收益（20%）
        if 'return_3m' in self.short_term_weights and 'return_3m' in feature_series:
            score += self._get_cached_normalized_score(
                'return_3m', feature_series['return_3m'], normalized_cache=normalized_cache
            ) * self.short_term_weights['return_3m']

        # 夏普比率（30%）
        if 'sharpe_ratio' in self.short_term_weights and 'sharpe_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sharpe_ratio', feature_series['sharpe_ratio'], normalized_cache=normalized_cache
            ) * self.short_term_weights['sharpe_ratio']

        # 索提诺比率（15%，新增）
        if 'sortino_ratio' in self.short_term_weights and 'sortino_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sortino_ratio', feature_series['sortino_ratio'], normalized_cache=normalized_cache
            ) * self.short_term_weights['sortino_ratio']

        # 波动率（15%，负向）
        if 'volatility' in self.short_term_weights and 'volatility' in feature_series:
            score += self._get_cached_normalized_score(
                'volatility', feature_series['volatility'], normalized_cache=normalized_cache
            ) * self.short_term_weights['volatility']

        # 归一化到0-100
        score = score * 100
        return score

    def _calculate_long_term_score(
        self,
        df: pd.DataFrame,
        feature_series: Optional[Dict[str, pd.Series]] = None,
        normalized_cache: Optional[Dict[str, Dict]] = None,
    ) -> pd.Series:
        """
        计算长期价值分

        组成：
        - annual_return: 30%（年化收益率）
        - consistency: 25%（稳定性，P0-R1修复后为负向指标）
        - max_drawdown: 15%（负向，低回撤高分）（v1.9.63: 20%→15%）
        - max_drawdown_duration: 5%（负向，连续水下观测越少得分越高）（v1.9.63新增）
        - sortino_ratio: 10%
        - calmar_ratio: 10%
        - total_fee_rate: 5%（负向，低持续持有费率高分）
        """
        score = pd.Series(0.0, index=df.index)
        feature_series = feature_series or self._build_score_feature_series(df)

        score += self._get_cached_normalized_score(
            'annual_return', feature_series['annual_return'], normalized_cache=normalized_cache
        ) * self.long_term_weights['annual_return']

        # 稳定性（25%）
        # P0-R1修复后：consistency改为月度标准差（负向指标，值越小越好）
        if 'consistency' in feature_series:
            score += self._get_cached_normalized_score(
                'consistency', feature_series['consistency'], normalized_cache=normalized_cache
            ) * self.long_term_weights['consistency']

        # 最大回撤（20%，负向：值越小越好，需要反向计算）
        if 'max_drawdown' in feature_series:
            score += self._get_cached_normalized_score(
                'max_drawdown', feature_series['max_drawdown'], normalized_cache=normalized_cache
            ) * self.long_term_weights['max_drawdown']

        # 索提诺比率（15%）
        if 'sortino_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sortino_ratio', feature_series['sortino_ratio'], normalized_cache=normalized_cache
            ) * self.long_term_weights['sortino_ratio']

        # 卡玛比率（10%）
        if 'calmar_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'calmar_ratio', feature_series['calmar_ratio'], normalized_cache=normalized_cache
            ) * self.long_term_weights['calmar_ratio']

        # v1.9.63: 最大回撤持续期（负向，连续水下观测越少得分越高）
        if 'max_drawdown_duration' in self.long_term_weights and 'max_drawdown_duration' in feature_series:
            weight = self.long_term_weights['max_drawdown_duration']
            score += self._get_cached_normalized_score(
                'max_drawdown_duration',
                feature_series['max_drawdown_duration'],
                normalized_cache=normalized_cache,
            ) * weight

        # v1.9.61 P1-3: 费率因子（负向，低费率高分）
        if 'total_fee_rate' in self.long_term_weights and 'total_fee_rate' in feature_series:
            weight = self.long_term_weights['total_fee_rate']
            score += self._get_cached_normalized_score(
                'total_fee_rate',
                feature_series['total_fee_rate'],
                normalized_cache=normalized_cache,
            ) * weight

        # 归一化到0-100
        score = score * 100
        return score

    def _calculate_shadow_short_term_score(
        self,
        df: pd.DataFrame,
        abs_weight: float,
        feature_series: Optional[Dict[str, pd.Series]] = None,
        normalized_cache: Optional[Dict[str, Dict]] = None,
    ) -> pd.Series:
        """
        影子版短期潜力分（使用自定义 absolute_weight）。

        逻辑与 _calculate_short_term_score 完全一致，
        仅将 _normalize_score 替换为 _normalize_score_variant(abs_weight=...)。
        """
        score = pd.Series(0.0, index=df.index)
        feature_series = feature_series or self._build_score_feature_series(df)

        if 'return_1m' in self.short_term_weights and 'return_1m' in feature_series:
            score += self._get_cached_normalized_score(
                'return_1m', feature_series['return_1m'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['return_1m']

        if 'return_1w' in self.short_term_weights and 'return_1w' in feature_series:
            score += self._get_cached_normalized_score(
                'return_1w', feature_series['return_1w'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['return_1w']

        if 'return_3m' in self.short_term_weights and 'return_3m' in feature_series:
            score += self._get_cached_normalized_score(
                'return_3m', feature_series['return_3m'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['return_3m']

        if 'sharpe_ratio' in self.short_term_weights and 'sharpe_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sharpe_ratio', feature_series['sharpe_ratio'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['sharpe_ratio']

        if 'sortino_ratio' in self.short_term_weights and 'sortino_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sortino_ratio', feature_series['sortino_ratio'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['sortino_ratio']

        if 'volatility' in self.short_term_weights and 'volatility' in feature_series:
            score += self._get_cached_normalized_score(
                'volatility', feature_series['volatility'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.short_term_weights['volatility']

        score = score * 100
        return score

    def _calculate_shadow_long_term_score(
        self,
        df: pd.DataFrame,
        abs_weight: float,
        feature_series: Optional[Dict[str, pd.Series]] = None,
        normalized_cache: Optional[Dict[str, Dict]] = None,
    ) -> pd.Series:
        """
        影子版长期价值分（使用自定义 absolute_weight）。

        逻辑与 _calculate_long_term_score 完全一致，
        仅将 _normalize_score 替换为 _normalize_score_variant(abs_weight=...)。
        """
        score = pd.Series(0.0, index=df.index)
        feature_series = feature_series or self._build_score_feature_series(df)

        score += self._get_cached_normalized_score(
            'annual_return', feature_series['annual_return'], normalized_cache=normalized_cache, abs_weight=abs_weight
        ) * self.long_term_weights['annual_return']

        if 'consistency' in feature_series:
            score += self._get_cached_normalized_score(
                'consistency', feature_series['consistency'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.long_term_weights['consistency']

        if 'max_drawdown' in feature_series:
            score += self._get_cached_normalized_score(
                'max_drawdown', feature_series['max_drawdown'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.long_term_weights['max_drawdown']

        if 'sortino_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sortino_ratio', feature_series['sortino_ratio'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.long_term_weights['sortino_ratio']

        if 'calmar_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'calmar_ratio', feature_series['calmar_ratio'], normalized_cache=normalized_cache, abs_weight=abs_weight
            ) * self.long_term_weights['calmar_ratio']

        # v1.9.63: 最大回撤持续期（与 _calculate_long_term_score 保持同步）
        if 'max_drawdown_duration' in self.long_term_weights and 'max_drawdown_duration' in feature_series:
            weight = self.long_term_weights['max_drawdown_duration']
            score += self._get_cached_normalized_score(
                'max_drawdown_duration',
                feature_series['max_drawdown_duration'],
                normalized_cache=normalized_cache,
                abs_weight=abs_weight,
            ) * weight

        # v1.9.61 P1-3: 费率因子（与 _calculate_long_term_score 保持同步）
        if 'total_fee_rate' in self.long_term_weights and 'total_fee_rate' in feature_series:
            weight = self.long_term_weights['total_fee_rate']
            score += self._get_cached_normalized_score(
                'total_fee_rate',
                feature_series['total_fee_rate'],
                normalized_cache=normalized_cache,
                abs_weight=abs_weight,
            ) * weight

        score = score * 100
        return score

    def _calculate_cstv2_long_term_score(
        self,
        df: pd.DataFrame,
        feature_series: Optional[Dict[str, pd.Series]] = None,
        normalized_cache: Optional[Dict[str, Dict]] = None,
    ) -> pd.Series:
        """
        Step 7-6: 一致性V2版长期价值分。

        与 _calculate_long_term_score 完全一致，唯一区别：
        将 consistency（负向，月度标准差）的 25% 权重替换为
        positive_month_ratio（正向，正收益月占比）。

        方案1（最小变更）：consistency 的 0.25 权重整体转移给 positive_month_ratio。
        """
        score = pd.Series(0.0, index=df.index)
        feature_series = feature_series or self._build_score_feature_series(df)

        score += self._get_cached_normalized_score(
            'annual_return', feature_series['annual_return'], normalized_cache=normalized_cache
        ) * self.long_term_weights['annual_return']

        # 正收益月占比（25%）- 替代 consistency，正向指标
        if 'positive_month_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'positive_month_ratio',
                feature_series['positive_month_ratio'],
                normalized_cache=normalized_cache,
            ) * self.long_term_weights['consistency']

        # 最大回撤（20%）- 与 baseline 一致
        if 'max_drawdown' in feature_series:
            score += self._get_cached_normalized_score(
                'max_drawdown', feature_series['max_drawdown'], normalized_cache=normalized_cache
            ) * self.long_term_weights['max_drawdown']

        # 索提诺比率（15%）- 与 baseline 一致
        if 'sortino_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'sortino_ratio', feature_series['sortino_ratio'], normalized_cache=normalized_cache
            ) * self.long_term_weights['sortino_ratio']

        # 卡玛比率（10%）- 与 baseline 一致
        if 'calmar_ratio' in feature_series:
            score += self._get_cached_normalized_score(
                'calmar_ratio', feature_series['calmar_ratio'], normalized_cache=normalized_cache
            ) * self.long_term_weights['calmar_ratio']

        # v1.9.63: 最大回撤持续期（与 _calculate_long_term_score 保持同步）
        if 'max_drawdown_duration' in self.long_term_weights and 'max_drawdown_duration' in feature_series:
            weight = self.long_term_weights['max_drawdown_duration']
            score += self._get_cached_normalized_score(
                'max_drawdown_duration',
                feature_series['max_drawdown_duration'],
                normalized_cache=normalized_cache,
            ) * weight

        # v1.9.61 P1-3: 费率因子（与 _calculate_long_term_score 保持同步）
        if 'total_fee_rate' in self.long_term_weights and 'total_fee_rate' in feature_series:
            weight = self.long_term_weights['total_fee_rate']
            score += self._get_cached_normalized_score(
                'total_fee_rate',
                feature_series['total_fee_rate'],
                normalized_cache=normalized_cache,
            ) * weight

        score = score * 100
        return score

    def _normalize_score(self, series: pd.Series, metric_name: str = None) -> pd.Series:
        """
        🔧 P0-E3修复：混合归一化评分到0-1区间

        方法：相对排名90% + 绝对基准10%
        - 相对排名：使用百分位排名（纯相对位置）
        - 绝对基准：根据指标的"优秀"和"不合格"阈值给予奖励/惩罚

        Args:
            series: 待归一化的数据序列
            metric_name: 指标名称（用于查找绝对基准，如果为None则仅使用相对排名）

        Returns:
            归一化后的评分序列（0-1区间）
        """
        if series.isna().all() or len(series) == 0:
            return pd.Series(0.0, index=series.index)

        # 去除NaN
        valid_mask = series.notna()
        if valid_mask.sum() == 0:
            return pd.Series(0.0, index=series.index)

        # 1. 相对排名部分（90%）
        relative_score = pd.Series(0.0, index=series.index)
        relative_score[valid_mask] = series[valid_mask].rank(pct=True)

        # 2. 绝对基准部分（10%）
        absolute_bonus = pd.Series(0.0, index=series.index)

        if metric_name and metric_name in self.absolute_baselines:
            baseline = self.absolute_baselines[metric_name]
            good_value = baseline['good']
            bad_value = baseline['bad']
            direction = baseline['direction']

            # 计算绝对得分（0-1区间）
            # 正向指标：值 >= good → 1.0, 值 <= bad → 0.0, 中间线性插值
            # 负向指标：值 <= good → 1.0, 值 >= bad → 0.0, 中间线性插值
            if good_value == bad_value:
                # 防御除零：good == bad 时无法线性插值，返回中性分 0.5
                absolute_bonus[valid_mask] = 0.5
            elif direction == 'positive':
                # 正向：越大越好
                absolute_bonus[valid_mask] = series[valid_mask].apply(
                    lambda x: 1.0 if x >= good_value else (
                        0.0 if x <= bad_value else (x - bad_value) / (good_value - bad_value)
                    )
                )
            else:
                # 负向：越小越好
                # v1.9.28修复：调用方已对负向指标取反（如 -volatility），
                # 需还原原始值再与绝对基准比较，避免双重取反导致全部满分
                original_values = -series[valid_mask]
                absolute_bonus[valid_mask] = original_values.apply(
                    lambda x: 1.0 if x <= good_value else (
                        0.0 if x >= bad_value else (bad_value - x) / (bad_value - good_value)
                    )
                )
        normalized = (
            relative_score * NORMALIZATION_WEIGHTS['relative'] +
            absolute_bonus * NORMALIZATION_WEIGHTS['absolute']
        )

        # v1.9.67 P2-4: NaN 惩罚分分段稳定化
        # - n_valid >= 20: 使用 q05 * 0.5（样本充足，q05 稳定）
        # - 10 <= n_valid < 20: 使用 q05 * 0.3（中等样本，降低权重）
        # - 3 <= n_valid < 10: 使用 min * 0.3（避免 quantile 边界跳变）
        # - n_valid < 3: 统一最低分
        n_valid = valid_mask.sum()
        if n_valid >= 20:
            nan_penalty = max(normalized[valid_mask].quantile(0.05) * 0.5, 0.0001)
        elif n_valid >= 10:
            nan_penalty = max(normalized[valid_mask].quantile(0.05) * 0.3, 0.0001)
        elif n_valid >= 3:
            nan_penalty = max(normalized[valid_mask].min() * 0.3, 0.0001)
        else:
            nan_penalty = 0.0001
        normalized[~valid_mask] = nan_penalty

        return normalized

    def _normalize_score_variant(
        self,
        series: pd.Series,
        metric_name: str = None,
        abs_weight: float = 0.10,
    ) -> pd.Series:
        """
        可参数化的归一化融合函数（用于影子评分对照）。

        与 _normalize_score 逻辑完全一致，但 absolute_weight 可自定义。
        默认路径（_normalize_score）不受影响。

        Args:
            series: 待归一化的数据序列
            metric_name: 指标名称（用于查找绝对基准）
            abs_weight: 绝对基准权重（0~1），相对权重 = 1 - abs_weight

        Returns:
            归一化后的评分序列（0-1区间）
        """
        if series.isna().all() or len(series) == 0:
            return pd.Series(0.0, index=series.index)

        valid_mask = series.notna()
        if valid_mask.sum() == 0:
            return pd.Series(0.0, index=series.index)

        # 1. 相对排名部分
        relative_score = pd.Series(0.0, index=series.index)
        relative_score[valid_mask] = series[valid_mask].rank(pct=True)

        # 2. 绝对基准部分
        absolute_bonus = pd.Series(0.0, index=series.index)

        if metric_name and metric_name in self.absolute_baselines:
            baseline = self.absolute_baselines[metric_name]
            good_value = baseline['good']
            bad_value = baseline['bad']
            direction = baseline['direction']

            # 除零保护：good_value == bad_value 时，所有值归一化为 0.5
            if good_value == bad_value:
                absolute_bonus[valid_mask] = 0.5
            elif direction == 'positive':
                absolute_bonus[valid_mask] = series[valid_mask].apply(
                    lambda x: 1.0 if x >= good_value else (
                        0.0 if x <= bad_value else (x - bad_value) / (good_value - bad_value)
                    )
                )
            else:
                # 负向：越小越好
                # v1.9.28修复：调用方已对负向指标取反，需还原原始值再与绝对基准比较
                original_values = -series[valid_mask]
                absolute_bonus[valid_mask] = original_values.apply(
                    lambda x: 1.0 if x <= good_value else (
                        0.0 if x >= bad_value else (bad_value - x) / (bad_value - good_value)
                    )
                )

        # 3. 混合归一化（自定义权重）
        rel_weight = 1.0 - abs_weight
        normalized = relative_score * rel_weight + absolute_bonus * abs_weight

        # v1.9.58 P1-6: NaN 惩罚分小样本稳定化（与 _normalize_score 保持一致）
        n_valid = valid_mask.sum()
        if n_valid >= 10:
            nan_penalty = max(normalized[valid_mask].quantile(0.01) * 0.5, 0.0001)
        elif n_valid >= 3:
            nan_penalty = max(normalized[valid_mask].min() * 0.3, 0.0001)
        else:
            nan_penalty = 0.0001
        normalized[~valid_mask] = nan_penalty

        return normalized

    def add_dimension_ranks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加多个维度的排名

        用于第9层多维度交叉验证

        Args:
            df: 包含评分和指标的DataFrame

        Returns:
            添加了排名列的DataFrame
        """
        df = df.copy()

        # 组合评分排名
        if 'combo_score' in df.columns:
            df['rank_combo'] = df['combo_score'].rank(ascending=False, method='min')

        # 夏普比率排名
        excluded_metrics = self._get_structurally_excluded_risk_metrics()
        sharpe_rank_series = None
        if 'sharpe_ratio' not in excluded_metrics:
            sharpe_rank_series = self._resolve_working_risk_ratio_series(df, 'sharpe_ratio')
        if sharpe_rank_series is not None:
            df['rank_sharpe'] = sharpe_rank_series.rank(ascending=False, method='min')

        # 最大回撤排名（负向，回撤小的排名靠前）
        if 'max_drawdown' in df.columns:
            df['rank_drawdown'] = df['max_drawdown'].rank(ascending=True, method='min')

        # 收益稳定性排名（负向：月度标准差，值越小越稳定，rank越靠前）
        if 'consistency' in df.columns:
            df['rank_consistency'] = df['consistency'].rank(ascending=True, method='min')

        # 年化收益排名
        if 'main_return' in df.columns:
            df['rank_return'] = df['main_return'].rank(ascending=False, method='min')

        return df

    def calculate_dimension_pass_count(
        self,
        df: pd.DataFrame,
        top_ratio: float = 0.3
    ) -> pd.Series:
        """
        计算每只基金在多个维度中排名前N%的维度数

        Args:
            df: 包含排名列的DataFrame
            top_ratio: 前N%阈值（默认30%）

        Returns:
            每只基金通过的维度数
        """
        threshold = max(int(len(df) * top_ratio), 1)  # 至少为1，防止小数据集截断为0

        dimension_count = pd.Series(0, index=df.index)

        # 检查各维度排名
        rank_columns = ['rank_combo', 'rank_sharpe', 'rank_drawdown', 'rank_consistency', 'rank_return']

        for col in rank_columns:
            if col in df.columns:
                dimension_count += (df[col] <= threshold).astype(int)

        return dimension_count
