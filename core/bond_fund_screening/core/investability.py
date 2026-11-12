#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 8-5: 可投性 Guardrails 纯函数

不改变筛选逻辑，仅在 pick 级别打标（flag）+ 前端提示。

Usage:
    from bond_fund_screening.core.investability import evaluate_investability_flags
    flags = evaluate_investability_flags(snapshot, fund_type='bond')
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# 默认配置（可被 INVESTABILITY_CONFIG 覆盖）
_DEFAULT_CONFIG = {
    'stale_nav_days': 10,
    'min_coverage_ratio': 0.80,
    'extreme_drawdown': {
        'bond': -0.15,
        'stock': -0.50,
        'hybrid': -0.35,
    },
    'min_scale': {
        'bond': 0.5,
        'stock': 1.0,
        'hybrid': 0.8,
    },
}


def evaluate_investability_flags(
    snapshot: Dict,
    fund_type: str = 'bond',
    config: Optional[Dict] = None,
) -> Dict:
    """
    根据 pick 快照数据评估可投性标记。

    Args:
        snapshot: 快照字典，包含 nav_age_days, is_fresh, risk_metrics,
                  coverage, scale_billion, is_fallback 等字段
        fund_type: 基金类型 (bond/stock/hybrid)
        config: 可选的覆盖配置（默认使用 _DEFAULT_CONFIG）

    Returns:
        {
            "verdict": "ok" | "warn" | "avoid",
            "flags": [
                {"key": "stale_nav", "severity": "warn", "label": "...", "detail": "..."},
                ...
            ]
        }
    """
    cfg = config or _DEFAULT_CONFIG
    flags = []

    # 1. stale_nav: 净值不新鲜
    nav_age = snapshot.get('nav_age_days')
    stale_threshold = cfg.get('stale_nav_days', 10)
    if nav_age is not None and nav_age > stale_threshold:
        flags.append({
            'key': 'stale_nav',
            'severity': 'warn',
            'label': '净值数据过期',
            'detail': f'净值过期{nav_age}天（阈值{stale_threshold}天）',
        })

    # 2. low_coverage: 数据覆盖率不足
    coverage = snapshot.get('coverage', {})
    coverage_ratio = coverage.get('coverage_ratio')
    min_cov = cfg.get('min_coverage_ratio', 0.80)
    if coverage_ratio is not None and coverage_ratio < min_cov:
        flags.append({
            'key': 'low_coverage',
            'severity': 'warn',
            'label': '数据覆盖不足',
            'detail': f'覆盖率{coverage_ratio:.1%}（阈值{min_cov:.0%}）',
        })

    # 3. extreme_drawdown: 极端回撤
    risk = snapshot.get('risk_metrics', {})
    dd = risk.get('max_drawdown')
    dd_limits = cfg.get('extreme_drawdown', {})
    dd_limit = dd_limits.get(fund_type, dd_limits.get('bond', -0.15))
    if dd is not None and dd < dd_limit:
        flags.append({
            'key': 'extreme_drawdown',
            'severity': 'avoid',
            'label': '极端回撤',
            'detail': f'最大回撤{dd:.2%}（阈值{dd_limit:.0%}）',
        })

    # 4. tiny_scale: 规模过小
    scale = snapshot.get('scale_billion')
    min_scales = cfg.get('min_scale', {})
    min_s = min_scales.get(fund_type, min_scales.get('bond', 0.5))
    if scale is not None and scale < min_s:
        flags.append({
            'key': 'tiny_scale',
            'severity': 'warn',
            'label': '规模过小',
            'detail': f'规模{scale:.2f}亿（阈值{min_s}亿）',
        })

    # 5. fallback_scoring: 走了降级通道
    if snapshot.get('is_fallback'):
        flags.append({
            'key': 'fallback_scoring',
            'severity': 'warn',
            'label': '评分降级通道',
            'detail': '该基金在风险指标层使用了降级（全局中位数）评估',
        })

    # 综合 verdict
    severities = [f['severity'] for f in flags]
    if 'avoid' in severities:
        verdict = 'avoid'
    elif 'warn' in severities:
        verdict = 'warn'
    else:
        verdict = 'ok'

    return {
        'verdict': verdict,
        'flags': flags,
    }
