#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
债券型基金Profile配置

当前项目直接由本模块提供并注册债券 Profile。
如需修改债券筛选参数，请直接修改本文件中的 BOND_PROFILE / BOND_FIXED_PROFILE。

从现有 config.py 提取的债券型基金筛选和评分参数。
包含普通债券型(31)和定开债(32)的配置。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.fund_type_registry import (
    FundTypeCode,
    FundCategory,
    FundTypeProfile,
    register_profile,
)
from bond_fund_screening.config import LONG_TERM_WEIGHTS, ABSOLUTE_BASELINES


# ============================================================================
# 债券型基金特有配置
# ============================================================================

# 长持有期关键词列表（用于流动性筛选）
BOND_HOLDING_KEYWORDS = [
    '定开', '定期开放', '封闭', '封闭式',
    '半年持有', '6个月持有', '180天持有',
    '一年持有', '12个月持有', '1年持有',
    '两年持有', '24个月持有', '2年持有',
    '三年持有', '36个月持有', '3年持有',
    '6个月定开', '一年定开', '两年定开', '三年定开',
    '锁定期', '封闭期', '持有至到期'
]

# 规模分层阈值（周期年数 -> 规模阈值亿元）
# 与 config.py SCALE_THRESHOLDS 保持同步，提高基础门槛降低清盘和流动性风险
BOND_SCALE_THRESHOLDS = {
    1: 2.0,  # 1年期：2.0亿
    2: 2.0,  # 2年期：2.0亿
    3: 2.5,  # 3年期：2.5亿
    4: 3.0,  # 4年期：3.0亿
    5: 3.0,  # 5年期：3.0亿
}

# 最低绝对收益阈值（周期年数 -> 最低累计收益率%）
# 适应低利率环境：年化基准从2%降至1.5%，避免过度淘汰
BOND_MIN_ABSOLUTE_RETURN = {
    1: 1.5,    # 1年期：≥1.5%（年化1.5%）
    2: 3.0,    # 2年期：≥3.0%（年化1.5%）
    3: 4.5,    # 3年期：≥4.5%（年化1.5%）
    4: 6.0,    # 4年期：≥6.0%（年化1.5%）
    5: 7.5,    # 5年期：≥7.5%（年化1.5%）
}

# 短期潜力分权重
BOND_SHORT_TERM_WEIGHTS = {
    'return_1m': 0.20,     # 月涨幅：20%
    'return_3m': 0.20,     # 3月收益：20%
    'sharpe_ratio': 0.30,  # 夏普比率：30%
    'sortino_ratio': 0.15, # 索提诺比率：15%
    'volatility': 0.15,    # 波动率：15%（负向指标）
}

# 长期价值分权重
BOND_LONG_TERM_WEIGHTS = dict(LONG_TERM_WEIGHTS)

# 组合评分权重
BOND_COMBO_WEIGHTS = {
    'short_term': 0.40,  # 短期潜力分：40%
    'long_term': 0.60,   # 长期价值分：60%
}

# 绝对基准配置（定义各指标的"优秀"和"不合格"阈值）
# 基于2013-2024债券型基金指数：年化5.05%，夏普1.24，回撤-5%
BOND_ABSOLUTE_BASELINES = {
    'max_drawdown_duration': dict(ABSOLUTE_BASELINES['max_drawdown_duration']),
    'total_fee_rate': dict(ABSOLUTE_BASELINES['total_fee_rate']),
    # 收益率指标（正向，越高越好）
    'return_1m': {'good': 0.8, 'bad': -0.3, 'direction': 'positive'},
    'return_3m': {'good': 2.0, 'bad': -0.5, 'direction': 'positive'},
    'return_1y': {'good': 6.0, 'bad': 2.0, 'direction': 'positive'},
    'annual_return': {'good': 6.0, 'bad': 2.0, 'direction': 'positive'},
    'main_return': {'good': 6.0, 'bad': 2.0, 'direction': 'positive'},

    # 风险调整指标（正向，越高越好）
    'sharpe_ratio': {'good': 0.8, 'bad': 0.3, 'direction': 'positive'},
    'sortino_ratio': {'good': 1.2, 'bad': 0.5, 'direction': 'positive'},
    'calmar_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},

    # 风险指标（负向，越低越好）- 统一为小数形式
    'volatility': {'good': 0.025, 'bad': 0.08, 'direction': 'negative'},
    'max_drawdown': {'good': 0.03, 'bad': 0.08, 'direction': 'negative'},
    'consistency': {'good': 0.02, 'bad': 0.06, 'direction': 'negative'},
}


# ============================================================================
# 创建债券型Profile实例
# ============================================================================

BOND_PROFILE = FundTypeProfile(
    # 基本信息
    fund_type=FundTypeCode.BOND,
    category=FundCategory.FIXED_INCOME,
    display_name="债券型",
    description="投资于固定收益类资产的基金，风险较低，追求稳定收益",

    # 流动性筛选
    max_holding_days=90,
    holding_keywords=BOND_HOLDING_KEYWORDS,

    # 成熟度筛选
    min_establish_days=365,
    min_scale_baseline=1.0,

    # 规模分层
    scale_thresholds=BOND_SCALE_THRESHOLDS,

    # 风险指标筛选
    risk_metric_pass_count=4,  # 4/6指标优于中位数
    risk_metric_min_funds=10,

    # 异常值筛选
    max_annual_return=12.0,    # 债券型最高年化12%
    min_annual_return=-5.0,    # 债券型最低年化-5%
    max_drawdown_threshold=0.08,  # 最大回撤≤8%

    # 业绩持续性
    consistency_pass_count=2,
    min_absolute_return=BOND_MIN_ABSOLUTE_RETURN,

    # 无风险利率
    risk_free_rate=0.015,  # 1.5%

    # 评分权重
    short_term_weights=BOND_SHORT_TERM_WEIGHTS,
    long_term_weights=BOND_LONG_TERM_WEIGHTS,
    combo_weights=BOND_COMBO_WEIGHTS,

    # 绝对基准
    absolute_baselines=BOND_ABSOLUTE_BASELINES,
)

# 定开债Profile（继承债券型大部分配置，但流动性要求更宽松）
BOND_FIXED_PROFILE = FundTypeProfile(
    fund_type=FundTypeCode.BOND_FIXED,
    category=FundCategory.FIXED_INCOME,
    display_name="定开债",
    description="定期开放式债券基金，流动性受限但收益可能更高",

    # 定开债的持有期限制更宽松（因为本身就有定期开放周期）
    max_holding_days=365,
    holding_keywords=[],  # 定开债不需要过滤持有期关键词

    # 其他参数与普通债券型相同
    min_establish_days=365,
    min_scale_baseline=1.0,
    scale_thresholds=BOND_SCALE_THRESHOLDS,
    risk_metric_pass_count=4,
    risk_metric_min_funds=10,
    max_annual_return=12.0,
    min_annual_return=-5.0,
    max_drawdown_threshold=0.08,
    consistency_pass_count=2,
    min_absolute_return=BOND_MIN_ABSOLUTE_RETURN,
    risk_free_rate=0.015,
    short_term_weights=BOND_SHORT_TERM_WEIGHTS,
    long_term_weights=BOND_LONG_TERM_WEIGHTS,
    combo_weights=BOND_COMBO_WEIGHTS,
    absolute_baselines=BOND_ABSOLUTE_BASELINES,
)

# 注册Profile
register_profile(BOND_PROFILE)
register_profile(BOND_FIXED_PROFILE)


# ============================================================================
# 测试函数
# ============================================================================

def test_bond_profile():
    """测试债券型Profile"""
    print("=" * 60)
    print("测试 Bond Profile")
    print("=" * 60)

    print(f"\n债券型Profile:")
    print(f"  类型代码: {BOND_PROFILE.fund_type.value}")
    print(f"  显示名称: {BOND_PROFILE.display_name}")
    print(f"  大类: {BOND_PROFILE.category.display_name}")
    print(f"  最大持有期: {BOND_PROFILE.max_holding_days}天")
    print(f"  规模阈值: {BOND_PROFILE.scale_thresholds}")
    print(f"  最大年化: {BOND_PROFILE.max_annual_return}%")
    print(f"  最大回撤: {BOND_PROFILE.max_drawdown_threshold*100}%")

    print(f"\n定开债Profile:")
    print(f"  类型代码: {BOND_FIXED_PROFILE.fund_type.value}")
    print(f"  最大持有期: {BOND_FIXED_PROFILE.max_holding_days}天")

    print("\n" + "=" * 60)
    print("✅ Bond Profile 测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_bond_profile()
