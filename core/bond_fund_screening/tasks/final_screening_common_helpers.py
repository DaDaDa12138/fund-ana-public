#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选通用归一化与基础辅助工具。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def infer_fund_type(short_file: Path, long_file: Path) -> str:
    """从长短期输入文件名推断基金类型。"""
    combined = str(short_file.name) + str(long_file.name)
    if '股票' in combined:
        return 'stock'
    if '混合' in combined:
        return 'hybrid'
    return 'bond'


def normalize_fcode(value):
    """
    统一基金代码格式，避免 Excel/SQLite 键格式不一致导致交叉匹配失败。

    规则：
    1. 去掉空值与空字符串
    2. 处理 Excel 数值导出导致的 ".0" 后缀
    3. 纯数字左侧补零到 6 位
    """
    if pd.isna(value):
        return None

    code = str(value).strip()
    if not code or code.lower() == 'nan':
        return None

    if code.endswith('.0') and code[:-2].isdigit():
        code = code[:-2]

    if code.isdigit() and len(code) < 6:
        code = code.zfill(6)

    return code


def normalize_fcodes_in_df(
    df: pd.DataFrame,
    dataset_name: str,
    *,
    normalize_fcode_fn,
    print_fn,
) -> pd.DataFrame:
    """标准化 DataFrame 的 fcode 列并清理非法/重复代码。"""
    if 'fcode' not in df.columns:
        raise KeyError(f"{dataset_name} 缺少 fcode 列")

    normalized_df = df.copy()
    normalized_df['fcode'] = normalized_df['fcode'].map(normalize_fcode_fn)

    invalid_count = int(normalized_df['fcode'].isna().sum())
    if invalid_count > 0:
        print_fn(f"  ⚠️ {dataset_name} 存在 {invalid_count} 条无效基金代码，已过滤")
        normalized_df = normalized_df[normalized_df['fcode'].notna()].copy()

    dup_count = int(normalized_df['fcode'].duplicated().sum())
    if dup_count > 0:
        print_fn(f"  ⚠️ {dataset_name} 存在 {dup_count} 条重复基金代码，按首条保留")
        normalized_df = normalized_df.drop_duplicates(subset=['fcode'], keep='first').copy()

    return normalized_df


def resolve_display_annual_return_1y(
    risk_1y,
    returns,
    *,
    stale_risk_metric_1y_lag_days: int,
):
    """为最终报告选择更贴近当前净值窗口的 1 年收益口径。"""
    if risk_1y is None:
        return None

    annual_return = risk_1y.annual_return
    if annual_return is None or returns is None or returns.return_1y is None:
        return annual_return

    risk_end_date = getattr(risk_1y, 'end_date', None)
    from utils.period_return_snapshot import RETURN_PUBLIC_FIELDS, has_dated_period_snapshot
    period_date = getattr(returns, 'period_return_date', None)
    if risk_end_date is None or not has_dated_period_snapshot(
        {field: getattr(returns, field, None) for field in RETURN_PUBLIC_FIELDS},
    ):
        return annual_return

    lag_days = (period_date - risk_end_date).days
    if lag_days > stale_risk_metric_1y_lag_days:
        return returns.return_1y

    return annual_return


def prepare_lookup_fcodes(fcodes: list, *, normalize_fcode_fn) -> tuple[list[tuple[object, str | None]], list[str]]:
    """预处理基金代码列表，返回原始与标准代码映射及去重后的标准代码列表。"""
    pairs = []
    normalized_codes = []
    seen = set()

    for fcode in fcodes:
        normalized = normalize_fcode_fn(fcode)
        pairs.append((fcode, normalized))
        if normalized and normalized not in seen:
            normalized_codes.append(normalized)
            seen.add(normalized)

    return pairs, normalized_codes


def has_fixed_holding_period(fund_name: str, *, holding_keywords: list[str] | tuple[str, ...]) -> bool:
    """Return whether a name proves a contractual term above 90 days."""
    if not fund_name:
        return False
    from utils.holding_period_policy import resolve_holding_period_days

    days = resolve_holding_period_days(holding_days=None, fund_name=fund_name)
    return days is not None and days > 90
