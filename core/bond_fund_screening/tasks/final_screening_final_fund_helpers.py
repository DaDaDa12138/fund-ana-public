#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终主榜导出数据装配与展示规整辅助工具。"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd


_FINAL_EXPORT_WEIGHT_COLUMNS = (
    'fcode',
    'weighted_score',
    'fund_category',
    'short_score',
    'long_score',
    '1年-最长回撤观测数',
    'base_fund_name',
    'share_class_count',
    'share_class_names',
    'share_class_codes',
)


def _build_weighted_export_frame(df_weighted_top: pd.DataFrame) -> pd.DataFrame:
    """裁剪最终主榜导出所需的加权字段。"""
    return df_weighted_top[list(_FINAL_EXPORT_WEIGHT_COLUMNS)].copy()


def build_final_fund_export_frame(
    *,
    df_complete: pd.DataFrame,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    final_fcodes: list[str],
    df_weighted_top: pd.DataFrame,
    sort_fn: Callable[[pd.DataFrame, str], pd.DataFrame],
    normalize_fcode: Callable[[object], str | None],
) -> tuple[pd.DataFrame, bool]:
    """构造当日最终优质基金导出数据，返回结果与是否使用本地 fallback。"""
    weighted_export = _build_weighted_export_frame(df_weighted_top)

    if df_complete.empty:
        df_final = pd.concat([
            df_short[df_short['fcode'].isin(final_fcodes)],
            df_long[df_long['fcode'].isin(final_fcodes)],
        ]).drop_duplicates(subset=['fcode'])
        df_final = df_final.merge(weighted_export, on='fcode', how='left')
        df_final = sort_fn(df_final, 'weighted_score').reset_index(drop=True)
        return df_final, True

    df_final = df_complete.copy()
    if '基金代码' in df_final.columns:
        df_final['基金代码'] = df_final['基金代码'].map(normalize_fcode)
        df_final = df_final[df_final['基金代码'].isin(final_fcodes)].copy()

    df_final = df_final.merge(
        weighted_export,
        left_on='基金代码',
        right_on='fcode',
        how='left',
    )
    if 'fcode' in df_final.columns:
        df_final = df_final.drop(columns=['fcode'])

    df_final = sort_fn(df_final, 'weighted_score').reset_index(drop=True)
    return df_final, False


def rename_final_fund_export_columns(df_final: pd.DataFrame) -> pd.DataFrame:
    """统一最终主榜导出字段名称。"""
    if 'weighted_score' not in df_final.columns:
        return df_final

    return df_final.rename(columns={
        'weighted_score': '综合加权得分',
        'fund_category': '优选类别',
        'short_score': '近期得分',
        'long_score': '长期得分',
        '1年-最长回撤观测数': '1年-最长回撤观测数',
        'base_fund_name': '母基金名称',
        'share_class_count': '关联份额数',
        'share_class_names': '关联份额名称',
        'share_class_codes': '关联份额代码',
    })


def prepare_final_fund_export_frame(
    df_final: pd.DataFrame,
    *,
    fund_type_map: dict,
    autostock_enabled: bool,
    autostock_validation_status_map: dict[str, dict],
    normalize_fcode: Callable[[object], str | None],
) -> tuple[pd.DataFrame, dict]:
    """应用最终主榜导出前的展示规整规则。"""
    df_final = df_final.copy()

    if '基金代码' in df_final.columns:
        df_final['基金代码'] = df_final['基金代码'].map(normalize_fcode)
        if autostock_enabled:
            from bond_fund_screening.tasks.final_screening_validation_display import apply_validation_display
            df_final = apply_validation_display(df_final, autostock_validation_status_map, normalize_fcode)

    if '基金类型' in df_final.columns:
        df_final['基金类型'] = df_final['基金类型'].astype(str).map(
            lambda value: fund_type_map.get(value, value if len(value) > 3 else '其他')
        )

    if '基金规模(亿)' in df_final.columns:
        df_final['基金规模(亿)'] = df_final['基金规模(亿)'].apply(
            lambda value: round(value, 2) if pd.notna(value) else None
        )

    removed_manager_column = False
    if '基金经理' in df_final.columns:
        df_final = df_final.drop(columns=['基金经理'])
        removed_manager_column = True

    dropped_empty_columns = [
        column_name
        for column_name in df_final.columns
        if df_final[column_name].isna().all()
    ]
    if dropped_empty_columns:
        df_final = df_final.drop(columns=dropped_empty_columns)

    return df_final, {
        'removed_manager_column': removed_manager_column,
        'dropped_empty_columns': dropped_empty_columns,
    }
