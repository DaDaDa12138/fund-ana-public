#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版评分引擎 - EnhancedScoringEngine

四维评分系统（使用混合归一化：80%相对排名 + 20%绝对基准）：
- 收益能力分（25%）: 年化收益、超额收益、正收益比率
- 风险控制分（35%）: 夏普比率、索提诺比率、最大回撤、VaR
- 稳定性分（25%）: 收益一致性、卡玛比率、多周期一致性
- 流动性分（15%）: 持有期限制、基金规模、成立时间

混合归一化方法说明：
- 解决纯百分位排名在市场整体不佳时的"矮子里拔将军"问题
- 80%基于相对排名：在基金池中的相对位置
- 20%基于绝对基准：与行业标准水平比较

支持三种风险偏好模式：
- 保守型（conservative）: 更重视风险控制
- 平衡型（balanced）: 默认模式
- 进取型（aggressive）: 更重视收益
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
from pathlib import Path
import sys

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent.parent))

from bond_fund_screening.config import RISK_FREE_RATE as DEFAULT_RISK_FREE_RATE
from bond_fund_screening.runtime_overrides import get_risk_free_rate
from utils.holding_period_policy import combine_holding_period_names, holding_period_gate_reason, resolve_holding_period_days


def get_db_path() -> Path:
    """返回项目数据库路径。"""
    from database.connection import resolve_db_path

    return Path(resolve_db_path())


@dataclass
class ScoreResult:
    """评分结果数据类"""
    return_score: float          # 收益能力分 (0-100)
    risk_score: float            # 风险控制分 (0-100)
    stability_score: float       # 稳定性分 (0-100)
    liquidity_score: float       # 流动性分 (0-100)
    comprehensive_score: float   # 综合评分 (0-100)
    grade: str                   # 评分等级 (A/B/C/D/E)
    risk_profile: str            # 风险偏好模式


class EnhancedScoringEngine:
    """
    增强版四维评分引擎

    基于现代投资组合理论和晨星评级方法论设计，
    提供科学、可解释的基金综合评分。
    """

    # 三种风险偏好模式的维度权重
    RISK_PROFILES = {
        'conservative': {
            'return': 0.20,
            'risk': 0.40,
            'stability': 0.25,
            'liquidity': 0.15,
            'name_cn': '保守型'
        },
        'balanced': {
            'return': 0.25,
            'risk': 0.35,
            'stability': 0.25,
            'liquidity': 0.15,
            'name_cn': '平衡型'
        },
        'aggressive': {
            'return': 0.35,
            'risk': 0.25,
            'stability': 0.25,
            'liquidity': 0.15,
            'name_cn': '进取型'
        },
    }

    # 收益能力分指标权重
    RETURN_WEIGHTS = {
        'annual_return': 0.40,      # 年化收益率
        'excess_return': 0.30,      # 超额收益
        'positive_ratio': 0.30,     # 正收益比率
    }

    # 风险控制分指标权重
    RISK_WEIGHTS = {
        'sharpe_ratio': 0.35,       # 夏普比率
        'sortino_ratio': 0.25,      # 索提诺比率
        'max_drawdown': 0.25,       # 最大回撤（负向）
        'var_95': 0.15,             # VaR 95% 下尾收益率（越接近 0 越好）
    }

    # 稳定性分指标权重
    STABILITY_WEIGHTS = {
        'consistency': 0.40,        # 收益一致性（负向）
        'calmar_ratio': 0.30,       # 卡玛比率
        'multi_period': 0.30,       # 多周期一致性
    }

    # 流动性分指标权重
    LIQUIDITY_WEIGHTS = {
        'holding_period': 0.50,     # 持有期限制
        'scale': 0.30,              # 基金规模
        'maturity': 0.20,           # 成立时间
    }

    # 评分等级阈值
    GRADE_THRESHOLDS = {
        'A': 80,   # >= 80
        'B': 65,   # >= 65
        'C': 50,   # >= 50
        'D': 35,   # >= 35
        'E': 0,    # < 35
    }

    # 分类绝对基准配置（用于混合归一化：90%相对排名 + 10%绝对基准）
    # 解决纯百分位排名在市场整体不佳时的"矮子里拔将军"问题
    # 不同类型基金使用不同的绝对基准
    CATEGORY_BASELINES = {
        # 债券型基金 - 低风险低收益
        'bond': {
            'annual_return': {'good': 6.0, 'bad': 2.0, 'direction': 'positive'},
            'excess_return': {'good': 4.0, 'bad': 0.0, 'direction': 'positive'},
            'positive_ratio': {'good': 65.0, 'bad': 50.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},
            'sortino_ratio': {'good': 1.5, 'bad': 0.5, 'direction': 'positive'},
            'max_drawdown': {'good': 0.03, 'bad': 0.10, 'direction': 'negative'},
            'var_95': {'good': -0.05, 'bad': -0.15, 'direction': 'positive'},
            'volatility': {'good': 0.02, 'bad': 0.06, 'direction': 'negative'},
            'calmar_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},
        },
        # 混合型-偏债 - 低中风险
        'hybrid_bond': {
            'annual_return': {'good': 8.0, 'bad': 3.0, 'direction': 'positive'},
            'excess_return': {'good': 6.0, 'bad': 1.0, 'direction': 'positive'},
            'positive_ratio': {'good': 60.0, 'bad': 45.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.8, 'bad': 0.2, 'direction': 'positive'},
            'sortino_ratio': {'good': 1.2, 'bad': 0.4, 'direction': 'positive'},
            'max_drawdown': {'good': 0.05, 'bad': 0.15, 'direction': 'negative'},
            'var_95': {'good': -0.08, 'bad': -0.20, 'direction': 'positive'},
            'volatility': {'good': 0.04, 'bad': 0.10, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.8, 'bad': 0.2, 'direction': 'positive'},
        },
        # 混合型-平衡 - 中等风险
        'hybrid_balanced': {
            'annual_return': {'good': 12.0, 'bad': 5.0, 'direction': 'positive'},
            'excess_return': {'good': 10.0, 'bad': 3.0, 'direction': 'positive'},
            'positive_ratio': {'good': 55.0, 'bad': 40.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.7, 'bad': 0.2, 'direction': 'positive'},
            'sortino_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},
            'max_drawdown': {'good': 0.10, 'bad': 0.25, 'direction': 'negative'},
            'var_95': {'good': -0.10, 'bad': -0.25, 'direction': 'positive'},
            'volatility': {'good': 0.08, 'bad': 0.18, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.6, 'bad': 0.2, 'direction': 'positive'},
        },
        # 混合型-偏股 - 中高风险
        'hybrid_stock': {
            'annual_return': {'good': 20.0, 'bad': 8.0, 'direction': 'positive'},
            'excess_return': {'good': 18.0, 'bad': 6.0, 'direction': 'positive'},
            'positive_ratio': {'good': 55.0, 'bad': 40.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.6, 'bad': 0.15, 'direction': 'positive'},
            'sortino_ratio': {'good': 0.9, 'bad': 0.25, 'direction': 'positive'},
            'max_drawdown': {'good': 0.15, 'bad': 0.35, 'direction': 'negative'},
            'var_95': {'good': -0.12, 'bad': -0.30, 'direction': 'positive'},
            'volatility': {'good': 0.12, 'bad': 0.25, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.5, 'bad': 0.15, 'direction': 'positive'},
        },
        # 股票型基金 - 高风险高收益
        'stock': {
            'annual_return': {'good': 25.0, 'bad': 10.0, 'direction': 'positive'},
            'excess_return': {'good': 23.0, 'bad': 8.0, 'direction': 'positive'},
            'positive_ratio': {'good': 55.0, 'bad': 40.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.5, 'bad': 0.1, 'direction': 'positive'},
            'sortino_ratio': {'good': 0.8, 'bad': 0.2, 'direction': 'positive'},
            'max_drawdown': {'good': 0.20, 'bad': 0.40, 'direction': 'negative'},
            'var_95': {'good': -0.15, 'bad': -0.35, 'direction': 'positive'},
            'volatility': {'good': 0.15, 'bad': 0.30, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.4, 'bad': 0.1, 'direction': 'positive'},
        },
        # 指数型基金 - 跟踪市场
        'index': {
            'annual_return': {'good': 20.0, 'bad': 5.0, 'direction': 'positive'},
            'excess_return': {'good': 18.0, 'bad': 3.0, 'direction': 'positive'},
            'positive_ratio': {'good': 55.0, 'bad': 40.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.5, 'bad': 0.1, 'direction': 'positive'},
            'sortino_ratio': {'good': 0.8, 'bad': 0.2, 'direction': 'positive'},
            'max_drawdown': {'good': 0.18, 'bad': 0.40, 'direction': 'negative'},
            'var_95': {'good': -0.15, 'bad': -0.35, 'direction': 'positive'},
            'volatility': {'good': 0.15, 'bad': 0.30, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.4, 'bad': 0.1, 'direction': 'positive'},
        },
        # 货币型基金 - 极低风险
        'money': {
            'annual_return': {'good': 2.5, 'bad': 1.5, 'direction': 'positive'},
            'excess_return': {'good': 0.8, 'bad': 0.0, 'direction': 'positive'},
            'positive_ratio': {'good': 95.0, 'bad': 80.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 2.0, 'bad': 0.5, 'direction': 'positive'},
            'sortino_ratio': {'good': 3.0, 'bad': 1.0, 'direction': 'positive'},
            'max_drawdown': {'good': 0.001, 'bad': 0.005, 'direction': 'negative'},
            'var_95': {'good': -0.01, 'bad': -0.05, 'direction': 'positive'},
            'volatility': {'good': 0.005, 'bad': 0.02, 'direction': 'negative'},
            'calmar_ratio': {'good': 5.0, 'bad': 1.0, 'direction': 'positive'},
        },
        # QDII基金 - 海外市场，高波动
        'qdii': {
            'annual_return': {'good': 20.0, 'bad': 5.0, 'direction': 'positive'},
            'excess_return': {'good': 18.0, 'bad': 3.0, 'direction': 'positive'},
            'positive_ratio': {'good': 55.0, 'bad': 40.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.5, 'bad': 0.1, 'direction': 'positive'},
            'sortino_ratio': {'good': 0.8, 'bad': 0.2, 'direction': 'positive'},
            'max_drawdown': {'good': 0.18, 'bad': 0.40, 'direction': 'negative'},
            'var_95': {'good': -0.15, 'bad': -0.35, 'direction': 'positive'},
            'volatility': {'good': 0.15, 'bad': 0.30, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.4, 'bad': 0.1, 'direction': 'positive'},
        },
        # FOF基金 - 分散风险
        'fof': {
            'annual_return': {'good': 10.0, 'bad': 3.0, 'direction': 'positive'},
            'excess_return': {'good': 8.0, 'bad': 1.0, 'direction': 'positive'},
            'positive_ratio': {'good': 60.0, 'bad': 45.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.7, 'bad': 0.2, 'direction': 'positive'},
            'sortino_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},
            'max_drawdown': {'good': 0.08, 'bad': 0.20, 'direction': 'negative'},
            'var_95': {'good': -0.08, 'bad': -0.20, 'direction': 'positive'},
            'volatility': {'good': 0.06, 'bad': 0.15, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.6, 'bad': 0.2, 'direction': 'positive'},
        },
        # REITs基金 - 不动产
        'reits': {
            'annual_return': {'good': 12.0, 'bad': 4.0, 'direction': 'positive'},
            'excess_return': {'good': 10.0, 'bad': 2.0, 'direction': 'positive'},
            'positive_ratio': {'good': 60.0, 'bad': 45.0, 'direction': 'positive'},
            'sharpe_ratio': {'good': 0.6, 'bad': 0.15, 'direction': 'positive'},
            'sortino_ratio': {'good': 0.9, 'bad': 0.25, 'direction': 'positive'},
            'max_drawdown': {'good': 0.12, 'bad': 0.30, 'direction': 'negative'},
            'var_95': {'good': -0.10, 'bad': -0.25, 'direction': 'positive'},
            'volatility': {'good': 0.10, 'bad': 0.22, 'direction': 'negative'},
            'calmar_ratio': {'good': 0.5, 'bad': 0.15, 'direction': 'positive'},
        },
    }

    # 默认绝对基准（用于未知类型，使用债券型作为默认）
    ABSOLUTE_BASELINES = CATEGORY_BASELINES['bond']

    # 绝对基准权重（占总评分的比例）
    # 从10%提高到20%，解决纯相对排名在市场整体不佳时的"矮子里拔将军"问题
    ABSOLUTE_WEIGHT = 0.20  # 20%绝对基准 + 80%相对排名

    # 基金类型名称映射（从天天基金类型到内部类别）
    FUND_TYPE_MAPPING = {
        # 债券型
        '债券型': 'bond',
        '中长期纯债': 'bond',
        '短期纯债': 'bond',
        '一级债基': 'bond',
        '二级债基': 'bond',
        '可转债基': 'bond',
        '债券指数': 'bond',
        # 混合型-偏债
        '混合型-偏债': 'hybrid_bond',
        '偏债混合型': 'hybrid_bond',
        # 混合型-平衡
        '混合型-平衡': 'hybrid_balanced',
        '平衡混合型': 'hybrid_balanced',
        # 混合型-偏股
        '混合型-偏股': 'hybrid_stock',
        '偏股混合型': 'hybrid_stock',
        '灵活配置型': 'hybrid_stock',
        '混合型-灵活': 'hybrid_stock',    # 灵活配置混合型
        '混合型': 'hybrid_stock',  # 默认混合型按偏股处理
        # 股票型
        '股票型': 'stock',
        '普通股票型': 'stock',
        '股票指数': 'index',
        '增强指数型': 'index',
        # 指数型
        '指数型': 'index',
        'ETF': 'index',
        'LOF': 'index',
        'ETF联接': 'index',
        '指数型-其他': 'index',        # 黄金ETF等商品指数
        '指数型-海外股票': 'index',    # 海外指数
        # 货币型
        '货币型': 'money',
        '货币市场型': 'money',
        # QDII
        'QDII': 'qdii',
        'QDII-股票型': 'qdii',
        'QDII-混合型': 'qdii',
        'QDII-混合偏股': 'qdii',      # 偏股QDII
        'QDII-混合偏债': 'qdii',      # 偏债QDII
        'QDII-债券型': 'qdii',
        'QDII-指数型': 'qdii',
        # FOF
        'FOF': 'fof',
        'FOF-进取型': 'fof',
        'FOF-平衡型': 'fof',
        'FOF-稳健型': 'fof',
        'FOF-保守型': 'fof',
        # REITs
        'REITs': 'reits',
        '不动产信托': 'reits',
    }

    def __init__(self, risk_profile: str = 'balanced', fund_category: str = 'bond'):
        """
        初始化评分引擎

        Args:
            risk_profile: 风险偏好模式
                - 'conservative': 保守型（风险控制40%）
                - 'balanced': 平衡型（默认，风险控制35%）
                - 'aggressive': 进取型（收益35%）
        """
        if risk_profile not in self.RISK_PROFILES:
            raise ValueError(f"无效的风险偏好模式: {risk_profile}，"
                           f"可选: {list(self.RISK_PROFILES.keys())}")

        self.risk_profile = risk_profile
        self.dimension_weights = self.RISK_PROFILES[risk_profile]
        resolution = get_risk_free_rate(float(DEFAULT_RISK_FREE_RATE))
        self.risk_free_rate = float(resolution.value)
        self.risk_free_rate_source = resolution.source
        self.risk_free_rate_updated_at = resolution.updated_at

        # 设置基金类别和对应的绝对基准
        self.set_fund_category(fund_category)

    def set_fund_category(self, category: str) -> None:
        """
        设置当前评分使用的基金类别

        Args:
            category: 基金类别，可以是：
                - 内部类别名（bond, stock, hybrid_stock等）
                - 天天基金类型名（债券型, 股票型等）
        """
        # 先尝试直接匹配内部类别
        if category in self.CATEGORY_BASELINES:
            self._current_category = category
        # 再尝试从类型映射中查找
        elif category in self.FUND_TYPE_MAPPING:
            self._current_category = self.FUND_TYPE_MAPPING[category]
        else:
            # 未知类型，使用混合偏股作为默认（比债券型更宽松）
            self._current_category = 'hybrid_stock'

        # 更新当前使用的绝对基准
        self.current_baselines = self.CATEGORY_BASELINES[self._current_category]

    @classmethod
    def map_fund_type(cls, fund_type: str) -> str:
        """
        将天天基金类型映射到内部类别

        Args:
            fund_type: 天天基金的基金类型名称

        Returns:
            内部类别名（bond, stock, hybrid_stock等）
        """
        if fund_type in cls.CATEGORY_BASELINES:
            return fund_type
        return cls.FUND_TYPE_MAPPING.get(fund_type, 'hybrid_stock')

    def _calc_absolute_bonus(self, value: float, metric_name: str) -> float:
        """
        计算绝对基准奖励分数

        根据指标值与绝对基准的比较，返回0-1之间的奖励分数：
        - 达到good水平：返回1.0
        - 介于good和bad之间：线性插值
        - 低于bad水平：返回0.0

        使用当前基金类别对应的基准配置（self.current_baselines）

        Args:
            value: 指标值
            metric_name: 指标名称

        Returns:
            0-1之间的奖励分数
        """
        if metric_name not in self.current_baselines:
            return 0.5  # 没有基准的指标返回中等分数

        if pd.isna(value):
            return 0.0

        baseline = self.current_baselines[metric_name]
        good = baseline['good']
        bad = baseline['bad']
        direction = baseline['direction']

        if direction == 'positive':
            # 正向指标：值越大越好
            if value >= good:
                return 1.0
            elif value <= bad:
                return 0.0
            elif good == bad:
                return 0.5  # 防止除零：good==bad时返回中性分
            else:
                return (value - bad) / (good - bad)
        else:
            # 负向指标：值越小越好
            if value <= good:
                return 1.0
            elif value >= bad:
                return 0.0
            elif bad == good:
                return 0.5  # 防止除零：bad==good时返回中性分
            else:
                return (bad - value) / (bad - good)

    def _calc_mixed_score(self, df: pd.DataFrame, column: str, weight: float,
                          is_negative: bool = False) -> pd.Series:
        """
        计算混合归一化分数（80%相对排名 + 20%绝对基准）

        Args:
            df: 数据框
            column: 列名
            weight: 该指标在维度中的权重
            is_negative: 是否为负向指标（小更好）

        Returns:
            加权后的分数序列
        """
        score = pd.Series(0.0, index=df.index)

        if column not in df.columns:
            return score

        valid = df[column].notna()
        if not valid.any():
            return score

        # 80% 相对排名分数
        if is_negative:
            relative_score = (-df.loc[valid, column]).rank(pct=True)
        else:
            relative_score = df.loc[valid, column].rank(pct=True)

        # 20% 绝对基准分数
        absolute_scores = df.loc[valid, column].apply(
            lambda x: self._calc_absolute_bonus(x, column)
        )

        # 混合归一化
        mixed_score = (
            relative_score * (1 - self.ABSOLUTE_WEIGHT) +
            absolute_scores * self.ABSOLUTE_WEIGHT
        )

        score[valid] = mixed_score * weight

        return score

    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        批量计算基金评分

        Args:
            df: 包含基金数据的DataFrame，需要以下字段：
                - annual_return: 年化收益率(%)
                - sharpe_ratio: 夏普比率
                - sortino_ratio: 索提诺比率
                - max_drawdown: 最大回撤(%)
                - var_95: VaR 95%
                - volatility: 波动率
                - calmar_ratio: 卡玛比率
                - positive_ratio: 正收益比率
                - holding_days: 持有期天数
                - scale_billion: 基金规模(亿元)
                - establish_days: 成立天数

        Returns:
            添加了评分列的DataFrame
        """
        df = df.copy()

        # 计算各维度分数
        df['return_score'] = self._calc_return_scores(df)
        df['risk_score'] = self._calc_risk_scores(df)
        df['stability_score'] = self._calc_stability_scores(df)
        df['liquidity_score'] = self._calc_liquidity_scores(df)

        # 计算综合评分
        weights = self.dimension_weights
        df['comprehensive_score'] = (
            df['return_score'] * weights['return'] +
            df['risk_score'] * weights['risk'] +
            df['stability_score'] * weights['stability'] +
            df['liquidity_score'] * weights['liquidity']
        )

        df['score_status'] = np.where(df['comprehensive_score'].notna(), 'scored', 'missing_input')

        # 评分等级
        df['grade'] = df['comprehensive_score'].apply(self._get_grade)

        # 风险偏好模式
        df['score_mode'] = self.dimension_weights['name_cn']

        return df

    def calculate_single_fund(self, fund_data: dict) -> ScoreResult:
        """
        计算单只基金的评分

        Args:
            fund_data: 基金数据字典

        Returns:
            ScoreResult对象
        """
        df = pd.DataFrame([fund_data])
        result_df = self.calculate_scores(df)
        row = result_df.iloc[0]

        return ScoreResult(
            return_score=row['return_score'],
            risk_score=row['risk_score'],
            stability_score=row['stability_score'],
            liquidity_score=row['liquidity_score'],
            comprehensive_score=row['comprehensive_score'],
            grade=row['grade'],
            risk_profile=self.risk_profile
        )

    def _calc_return_scores(self, df: pd.DataFrame) -> pd.Series:
        """
        计算收益能力分（使用混合归一化：80%相对排名 + 20%绝对基准）

        组成：
        - 年化收益率：40%
        - 超额收益（年化收益-无风险利率）：30%
        - 正收益比率：30%
        """
        score = pd.Series(0.0, index=df.index)
        weights = self.RETURN_WEIGHTS

        # 年化收益率（40%）- 使用混合归一化
        score += self._calc_mixed_score(df, 'annual_return', weights['annual_return'])

        # 超额收益（30%）- 需要先计算超额收益列
        if 'annual_return' in df.columns:
            df_temp = df.copy()
            # 年化收益率是百分比形式（如5.0表示5%），无风险利率转为百分比
            df_temp['excess_return'] = df['annual_return'] - self.risk_free_rate * 100
            score += self._calc_mixed_score(df_temp, 'excess_return', weights['excess_return'])

        # 正收益比率（30%）- 使用混合归一化
        score += self._calc_mixed_score(df, 'positive_ratio', weights['positive_ratio'])

        return score * 100

    def _calc_risk_scores(self, df: pd.DataFrame) -> pd.Series:
        """
        计算风险控制分（使用混合归一化：80%相对排名 + 20%绝对基准）

        组成：
        - 夏普比率：35%（正向）
        - 索提诺比率：25%（正向）
        - 最大回撤：25%（负向，小更好）
        - VaR 95%：15%（下尾收益率，数值越大、越接近 0 越好）
        """
        score = pd.Series(0.0, index=df.index)
        weights = self.RISK_WEIGHTS

        # 夏普比率（正向，35%）
        score += self._calc_mixed_score(df, 'sharpe_ratio', weights['sharpe_ratio'])

        # 索提诺比率（正向，25%）
        score += self._calc_mixed_score(df, 'sortino_ratio', weights['sortino_ratio'])

        # 最大回撤（负向，25%）- 回撤小的排名高
        score += self._calc_mixed_score(df, 'max_drawdown', weights['max_drawdown'], is_negative=True)

        # VaR 保存为有符号下尾收益率（例如 -0.003 表示 -0.3%）。
        # 因此数值越大、越接近 0 越好；不能沿用最大回撤的“小值优先”反向排序。
        score += self._calc_mixed_score(df, 'var_95', weights['var_95'])

        return score * 100

    def _calc_stability_scores(self, df: pd.DataFrame) -> pd.Series:
        """
        计算稳定性分（使用混合归一化：80%相对排名 + 20%绝对基准）

        组成：
        - 收益一致性：40%（负向，波动小更好）
        - 卡玛比率：30%（正向）
        - 多周期一致性：30%（如果有历史数据）
        """
        score = pd.Series(0.0, index=df.index)
        weights = self.STABILITY_WEIGHTS

        # 收益一致性（负向，40%）- 波动小的排名高
        if 'consistency' in df.columns:
            # consistency是月度收益标准差，值越小越稳定
            score += self._calc_mixed_score(df, 'consistency', weights['consistency'], is_negative=True)
        elif 'volatility' in df.columns:
            # 如果没有consistency，使用volatility作为替代
            score += self._calc_mixed_score(df, 'volatility', weights['consistency'], is_negative=True)

        # 卡玛比率（正向，30%）
        score += self._calc_mixed_score(df, 'calmar_ratio', weights['calmar_ratio'])

        # 多周期一致性（30%）
        if 'multi_period_consistency' in df.columns:
            valid = df['multi_period_consistency'].notna()
            if valid.any():
                score[valid] += df.loc[valid, 'multi_period_consistency'].rank(pct=True) * weights['multi_period']
        else:
            # 没有多周期数据时，使用卡玛比率补充权重
            score += self._calc_mixed_score(df, 'calmar_ratio', weights['multi_period'])

        return score * 100

    def _calc_liquidity_scores(self, df: pd.DataFrame) -> pd.Series:
        """
        计算流动性分

        组成：
        - 持有期限制：50%（分档评分）
        - 基金规模：30%（倒U型，2-10亿最优）
        - 成立时间：20%（分档评分）
        """
        score = pd.Series(0.0, index=df.index)
        weights = self.LIQUIDITY_WEIGHTS

        for field, scorer, weight in (
            ('holding_days', self._holding_period_score, weights['holding_period']),
            ('scale_billion', self._scale_score, weights['scale']),
            ('establish_days', self._maturity_score, weights['maturity']),
        ):
            values = pd.to_numeric(df.get(field, pd.Series(float('nan'), index=df.index)), errors='coerce')
            if field == 'holding_days':
                # Resolve missing structured days from the name for scoring only.
                # The database column remains NULL: a name-derived value is
                # evidence for the policy, not a fabricated API value.
                source = df.get('holding_period_source', pd.Series('', index=df.index)).fillna('').astype(str).str.strip()
                keywords = df.get('holding_keywords', pd.Series('', index=df.index)).fillna('').astype(str).str.strip()
                has_period = pd.to_numeric(
                    df.get('has_holding_period', pd.Series(0, index=df.index)), errors='coerce'
                ).fillna(0).astype(int)
                values = values.copy()
                shortname = df.get('shortname', pd.Series('', index=df.index))
                display_name = df.get('基金名称', pd.Series('', index=df.index))
                fund_name = df.get('fund_name', pd.Series('', index=df.index))
                fullname = df.get('fullname', pd.Series('', index=df.index))
                for position in range(len(values)):
                    short = shortname.iloc[position] if position < len(shortname) else ''
                    display = display_name.iloc[position] if position < len(display_name) else ''
                    alias = fund_name.iloc[position] if position < len(fund_name) else ''
                    full = fullname.iloc[position] if position < len(fullname) else ''
                    short = short if pd.notna(short) else ''
                    display = display if pd.notna(display) else ''
                    alias = alias if pd.notna(alias) else ''
                    full = full if pd.notna(full) else ''
                    name = combine_holding_period_names(short, display, alias, full)
                    raw_days = values.iloc[position]
                    resolved_days = resolve_holding_period_days(
                        holding_days=raw_days,
                        fund_name=name,
                        holding_period_source=source.iloc[position],
                        holding_keywords=keywords.iloc[position],
                    )
                    if resolved_days is not None:
                        values.iloc[position] = (
                            float('nan') if resolved_days == 0 and has_period.iloc[position] == 1
                            else float(resolved_days)
                        )
                        continue
                    values.iloc[position] = float('nan')
                    # A non-empty ordinary name (or an explicitly reviewed
                    # no-signal row) receives the ordinary-fund score. A name
                    # without a concrete term follows the same shared policy.
                    if holding_period_gate_reason(
                        has_holding_period=has_period.iloc[position],
                        holding_days=raw_days,
                        holding_period_source=source.iloc[position],
                        holding_keywords=keywords.iloc[position],
                        fund_name=name,
                    ) is None:
                        values.iloc[position] = 0
            score += values.apply(scorer) * weight

        return score

    @staticmethod
    def _holding_period_score(days: float) -> float:
        """
        持有期评分（分档）

        无持有期限制 -> 100分
        ≤7天 -> 90分
        ≤30天 -> 75分
        ≤90天 -> 50分
        ≤180天 -> 25分
        >180天 -> 10分
        """
        if pd.isna(days) or not np.isfinite(days) or days < 0:
            return float("nan")
        if days == 0:
            return 100.0
        elif days <= 7:
            return 90.0
        elif days <= 30:
            return 75.0
        elif days <= 90:
            return 50.0
        elif days <= 180:
            return 25.0
        else:
            return 10.0

    @staticmethod
    def _scale_score(scale: float) -> float:
        """
        规模评分（倒U型，2-10亿最优）

        <0.5亿 -> 30分（规模过小，流动性风险）
        0.5-2亿 -> 70分（规模偏小）
        2-10亿 -> 100分（最优规模区间）
        10-50亿 -> 80分（规模偏大）
        50-100亿 -> 60分（规模较大）
        >100亿 -> 40分（规模过大，船大难掉头）
        """
        if pd.isna(scale) or not np.isfinite(scale) or scale < 0:
            return float("nan")
        if scale < 0.5:
            return 30.0
        elif scale < 2:
            return 70.0
        elif scale < 10:
            return 100.0
        elif scale < 50:
            return 80.0
        elif scale < 100:
            return 60.0
        else:
            return 40.0

    @staticmethod
    def _maturity_score(days: float) -> float:
        """
        成熟度评分（分档）

        <1年 -> 30分（成立时间过短）
        1-2年 -> 60分
        2-3年 -> 80分
        3-5年 -> 100分（最优）
        >5年 -> 90分（超长期略降）
        """
        if pd.isna(days) or not np.isfinite(days) or days < 0:
            return float("nan")
        if days < 365:
            return 30.0
        elif days < 730:
            return 60.0
        elif days < 1095:
            return 80.0
        elif days < 1825:
            return 100.0
        else:
            return 90.0

    def _get_grade(self, score: float) -> str:
        """
        获取评分等级

        A: >= 80（优秀，强烈推荐）
        B: >= 65（良好，推荐关注）
        C: >= 50（中等，谨慎考虑）
        D: >= 35（较差，不建议）
        E: < 35（差，避免）
        """
        if pd.isna(score):
            return '不可评分'
        elif score >= self.GRADE_THRESHOLDS['A']:
            return 'A'
        elif score >= self.GRADE_THRESHOLDS['B']:
            return 'B'
        elif score >= self.GRADE_THRESHOLDS['C']:
            return 'C'
        elif score >= self.GRADE_THRESHOLDS['D']:
            return 'D'
        else:
            return 'E'

    @classmethod
    def get_grade_description(cls, grade: str) -> str:
        """获取评分等级描述"""
        descriptions = {
            'A': '优秀，强烈推荐',
            'B': '良好，推荐关注',
            'C': '中等，谨慎考虑',
            'D': '较差，不建议',
            'E': '差，避免',
        }
        return descriptions.get(grade, '未知')

    @classmethod
    def get_profile_description(cls, profile: str) -> str:
        """获取风险偏好模式描述"""
        if profile not in cls.RISK_PROFILES:
            return '未知模式'

        weights = cls.RISK_PROFILES[profile]
        return (f"{weights['name_cn']}：收益{int(weights['return']*100)}% + "
                f"风险{int(weights['risk']*100)}% + "
                f"稳定{int(weights['stability']*100)}% + "
                f"流动{int(weights['liquidity']*100)}%")


def get_fund_metrics_from_db(fcodes: List[str], period: str = '1y') -> pd.DataFrame:
    """
    从数据库获取基金风险指标

    Args:
        fcodes: 基金代码列表
        period: 计算周期（1y/3y/5y）

    Returns:
        包含风险指标的DataFrame
    """
    from database import DatabaseManager, FundRiskMetrics, Fund

    db_path = get_db_path()
    db = DatabaseManager(db_path=str(db_path))

    results = []

    with db.get_session() as session:
        for fcode in fcodes:
            # 获取风险指标
            metrics = session.query(FundRiskMetrics).filter_by(
                fcode=fcode, period=period
            ).first()

            # 获取基金基本信息
            fund = session.query(Fund).filter_by(fcode=fcode).first()

            if metrics or fund:
                # 计算成立天数
                establish_days = None
                if fund and fund.estab_date:
                    from datetime import datetime
                    establish_days = (datetime.now().date() - fund.estab_date).days

                results.append({
                    'fcode': fcode,
                    'shortname': fund.shortname if fund else None,
                    'fullname': fund.fullname if fund else None,
                    'has_holding_period': fund.has_holding_period if fund else None,
                    'holding_period_source': fund.holding_period_source if fund else None,
                    'holding_keywords': fund.holding_keywords if fund else None,
                    'sharpe_ratio': metrics.sharpe_ratio if metrics else None,
                    'sortino_ratio': metrics.sortino_ratio if metrics else None,
                    'calmar_ratio': metrics.calmar_ratio if metrics else None,
                    'max_drawdown': metrics.max_drawdown if metrics else None,
                    'volatility': metrics.volatility if metrics else None,
                    'var_95': metrics.var_95 if metrics else None,
                    'annual_return': metrics.annual_return if metrics else None,
                    'positive_ratio': metrics.positive_ratio if metrics else None,
                    'scale_billion': fund.scale_billion if fund else None,
                    'holding_days': fund.holding_days if fund else None,
                    'establish_days': establish_days,
                })

    return pd.DataFrame(results)
