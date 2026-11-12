#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选运行期配置与轻量规则辅助工具。"""

from __future__ import annotations


STALE_RISK_METRIC_1Y_LAG_DAYS = 7

FINAL_MAINLIST_REQUIRED_COLUMNS = (
    '1年-年化收益(%)',
    '1年-夏普比率',
    '1年-最大回撤(%)',
)

AUTOSTOCK_MAINLIST_REQUIRED_LOCAL_COLUMNS = (
    '基金代码',
    '基金名称',
    '最新净值日期',
    '最新单位净值',
    '近1周(%)',
    '近1月(%)',
    '近3月(%)',
    '近6月(%)',
    '近1年(%)',
)

AUTOSTOCK_MAINLIST_RETURN_FIELD_MAPPINGS = (
    ('近1周(%)', 'lastWeekGrowth', 'return_1w'),
    ('近1月(%)', 'lastMonthGrowth', 'return_1m'),
    ('近3月(%)', 'lastThreeMonthsGrowth', 'return_3m'),
    ('近6月(%)', 'lastSixMonthsGrowth', 'return_6m'),
    ('近1年(%)', 'lastYearGrowth', 'return_1y'),
)

FINAL_SCREENING_PRIMARY_SCORE_COLUMNS = (
    '平均综合评分',
    'avg_combo_score',
    'selected_combo_score',
    'combo_score',
)

FINAL_SCREENING_FALLBACK_SCORE_COLUMNS = (
    '综合稳定性得分',
    '综合稳定性评分',
    'comprehensive_score',
)

FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS = 2

AUTOSTOCK_VALIDATION_EXPORT_COLUMNS = (
    'autostock_validation_status',
    'autostock_warning_display',
)

AUTOSTOCK_VALIDATION_CACHE_TTL_SECONDS = 14400
AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION = 'v6'
FINAL_SCREENING_POLICY_SNAPSHOT_SCHEMA_VERSION = 'v2'
FINAL_SCREENING_DECISION_LEDGER_SCHEMA_VERSION = 'v2'

FINAL_FUND_TYPE_LABEL_MAP = {
    '003': '纯债型基金',
    '008': '定开债基金',
    '004': '混合债基',
    '005': '可转债基金',
    '001': '股票型基金',
    '002': '混合型基金',
    '006': '货币型基金',
    '007': '指数型基金',
    '009': 'FOF基金',
    '010': 'ETF联接',
    '011': '其他基金',
    '债券型-混合一级': '混合一级债基',
    '债券型-长债': '长期债基',
}

_AUTOSTOCK_CROSS_VALIDATION_FUND_TYPES = frozenset({'bond', 'stock', 'hybrid'})
_HOLDING_PERIOD_FILTER_FUND_TYPES = frozenset({'bond'})


def should_apply_autostock_cross_validation(fund_type: str) -> bool:
    """判断基金类型是否启用 AutoStock 双源交叉验证。"""
    return str(fund_type or '').strip().lower() in _AUTOSTOCK_CROSS_VALIDATION_FUND_TYPES


def should_filter_holding_period_funds(fund_type: str) -> bool:
    """判断基金类型是否在最终名单阶段过滤持有期产品。"""
    return str(fund_type or '').strip().lower() in _HOLDING_PERIOD_FILTER_FUND_TYPES


def get_bond_final_input_patterns(today: str) -> dict[str, str]:
    """返回债券日终筛选所依赖的长短期详细报告文件模式。"""
    return {
        'long_today': f"长期优质基金_详细报告_{today}_*.xlsx",
        'long_fallback': "长期优质基金_详细报告_*.xlsx",
        'short_today': f"近期优质基金_详细报告_{today}_*.xlsx",
        'short_fallback': "近期优质基金_详细报告_*.xlsx",
    }
