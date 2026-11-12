#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金份额归一化与母基金级去重工具。

目标：
1. 识别 A/C/E 等不同份额后缀
2. 在不改动既有评分逻辑的前提下，按母基金维度做结果去重
3. 为导出结果保留关联份额信息，避免只剩一个代码后失去解释能力
"""

from __future__ import annotations

from typing import Iterable
import re

import pandas as pd


_SHARE_CLASS_SUFFIXES = (
    '人民币',
    '美元',
    '份额',
    'A',
    'B',
    'C',
    'D',
    'E',
    'F',
    'H',
    'I',
    'R',
    'Y',
)


def _dedup_values(values: Iterable) -> list[str]:
    """按原顺序去重并清理空值。"""
    result = []
    seen = set()
    for value in values:
        if pd.isna(value):
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result


def normalize_base_fund_name(name) -> str:
    """
    将基金名称归一化为“母基金名称”。

    当前只处理明确、低风险的末尾份额后缀，避免误伤正常基金名。
    """
    if pd.isna(name):
        return ''

    text = str(name).strip()
    if not text:
        return ''

    for suffix in _SHARE_CLASS_SUFFIXES:
        if not text.endswith(suffix):
            continue
        candidate = text[:-len(suffix)].strip()
        if len(candidate) < 2:
            continue
        # 仅在主体末尾明显仍是中文、右括号或数字时，才认为后缀是份额标识。
        if re.search(r'[\u4e00-\u9fff）)\d]$', candidate):
            return candidate

    return text


def collapse_share_classes(
    df: pd.DataFrame,
    *,
    name_col: str,
    code_col: str = 'fcode',
) -> pd.DataFrame:
    """
    在已排序 DataFrame 上按母基金维度去重，保留每组首条记录。

    约定：
    1. 调用方需先完成排序
    2. 首条记录视为该母基金的代表份额
    3. 额外输出关联份额信息，方便导出与解释
    """
    if df.empty or name_col not in df.columns:
        return df.copy()

    result = df.copy()
    result['base_fund_name'] = result[name_col].map(normalize_base_fund_name)
    result['share_class_count'] = 1
    result['share_class_names'] = result[name_col].map(
        lambda value: '' if pd.isna(value) else str(value).strip()
    )
    if code_col in result.columns:
        result['share_class_codes'] = result[code_col].map(
            lambda value: '' if pd.isna(value) else str(value).strip()
        )
    else:
        result['share_class_codes'] = ''

    keep_indexes = []
    for _, group in result.groupby('base_fund_name', sort=False, dropna=False):
        representative_idx = group.index[0]
        keep_indexes.append(representative_idx)

        names = _dedup_values(group[name_col].tolist())
        codes = _dedup_values(group[code_col].tolist()) if code_col in group.columns else []
        base_name = normalize_base_fund_name(group.iloc[0][name_col])

        result.at[representative_idx, 'base_fund_name'] = base_name
        result.at[representative_idx, 'share_class_count'] = max(len(names), 1)
        result.at[representative_idx, 'share_class_names'] = ' / '.join(names)
        result.at[representative_idx, 'share_class_codes'] = ' / '.join(codes)

    collapsed = result.loc[keep_indexes].copy()
    collapsed['share_class_count'] = collapsed['share_class_count'].fillna(1).astype(int)
    return collapsed
