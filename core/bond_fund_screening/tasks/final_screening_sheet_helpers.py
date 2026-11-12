#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选对比报告表格组装辅助工具。"""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def load_final_screening_input_frame(report_file, *, report_label: str, print_fn) -> pd.DataFrame:
    """读取最终筛选输入 Excel，优先取“全部交集基金”sheet。"""
    try:
        df = pd.read_excel(report_file, sheet_name='全部交集基金')
        print_fn(f"  {report_label}数量: {len(df)} (来自'全部交集基金')")
    except ValueError:
        df = pd.read_excel(report_file, sheet_name=0)
        print_fn(f"  {report_label}数量: {len(df)} (来自第一个sheet)")

    if '基金代码' in df.columns:
        df = df.rename(columns={'基金代码': 'fcode'})
    return df


def build_final_screening_code_stats(df_short: pd.DataFrame, df_long: pd.DataFrame) -> dict:
    """统计长短期输入的代码级与母基金级规模。"""
    codes_short = set(df_short['fcode'].tolist())
    codes_long = set(df_long['fcode'].tolist())
    raw_intersection = codes_short & codes_long
    raw_only_short = codes_short - codes_long
    raw_only_long = codes_long - codes_short

    short_base_names = {
        str(value).strip()
        for value in df_short.get('base_fund_name', pd.Series(dtype=str)).tolist()
        if str(value).strip()
    }
    long_base_names = {
        str(value).strip()
        for value in df_long.get('base_fund_name', pd.Series(dtype=str)).tolist()
        if str(value).strip()
    }

    return {
        'codes_short': codes_short,
        'codes_long': codes_long,
        'raw_intersection': raw_intersection,
        'raw_only_short': raw_only_short,
        'raw_only_long': raw_only_long,
        'short_base_names': short_base_names,
        'long_base_names': long_base_names,
        'shared_base_names': short_base_names & long_base_names,
    }


def build_intersection_sheet(
    *,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    intersection_base_names: set[str],
    normalize_fcode_fn,
    get_latest_nav_data_fn,
    print_fn,
) -> pd.DataFrame:
    """组装交集基金 sheet。"""
    if not intersection_base_names:
        df_empty = pd.DataFrame(columns=['基金代码', '基金名称', '说明'])
        df_empty.loc[0] = ['', '', '没有同时在近期和长期都表现优异的基金']
        return df_empty

    print_fn(f"  正在为交集基金获取最新净值数据...")

    df_short_inter = df_short[df_short['base_fund_name'].isin(intersection_base_names)].copy()
    df_long_inter = df_long[df_long['base_fund_name'].isin(intersection_base_names)].copy()

    df_short_inter.set_index('base_fund_name', inplace=True)
    df_long_inter.set_index('base_fund_name', inplace=True)
    df_short_inter = df_short_inter.add_prefix('近期_')
    df_long_inter = df_long_inter.add_prefix('长期_')

    df_sheet3 = pd.concat([df_short_inter, df_long_inter], axis=1)
    df_sheet3.reset_index(inplace=True)
    df_sheet3.rename(columns={'index': '母基金名称', 'base_fund_name': '母基金名称'}, inplace=True)

    if '近期_fcode' in df_sheet3.columns or '长期_fcode' in df_sheet3.columns:
        df_sheet3['nav_lookup_fcode'] = df_sheet3.apply(
            lambda row: normalize_fcode_fn(row.get('近期_fcode')) or normalize_fcode_fn(row.get('长期_fcode')),
            axis=1,
        )
        df_sheet3['基金代码'] = df_sheet3.apply(
            lambda row: (
                f"{normalize_fcode_fn(row.get('近期_fcode'))} / {normalize_fcode_fn(row.get('长期_fcode'))}"
                if (
                    normalize_fcode_fn(row.get('近期_fcode'))
                    and normalize_fcode_fn(row.get('长期_fcode'))
                    and normalize_fcode_fn(row.get('近期_fcode')) != normalize_fcode_fn(row.get('长期_fcode'))
                )
                else (
                    normalize_fcode_fn(row.get('近期_fcode'))
                    or normalize_fcode_fn(row.get('长期_fcode'))
                )
            ),
            axis=1,
        )
    else:
        df_sheet3['nav_lookup_fcode'] = None
        df_sheet3['基金代码'] = None

    if '母基金名称' in df_sheet3.columns:
        df_sheet3['基金名称'] = df_sheet3['母基金名称']

    near_score_col = None
    long_score_col = None
    for col in df_sheet3.columns:
        if '近期_' in col and ('综合稳定性得分' in col or '综合稳定性评分' in col):
            near_score_col = col
            break
    for col in df_sheet3.columns:
        if '长期_' in col and ('综合稳定性得分' in col or '综合稳定性评分' in col):
            long_score_col = col
            break

    if near_score_col and long_score_col:
        df_sheet3['总计-综合稳定性得分'] = (
            df_sheet3[near_score_col].fillna(0) + df_sheet3[long_score_col].fillna(0)
        )
    else:
        print_fn(f"  ⚠️ 未找到综合稳定性评分列,无法计算总计得分")
        df_sheet3['总计-综合稳定性得分'] = 0

    latest_nav_data = get_latest_nav_data_fn([
        fcode
        for fcode in df_sheet3['nav_lookup_fcode'].tolist()
        if normalize_fcode_fn(fcode)
    ])
    df_sheet3['最新日增长率(%)'] = df_sheet3['nav_lookup_fcode'].apply(
        lambda value: latest_nav_data.get(str(value), {}).get('daily_growth')
    )
    df_sheet3['最新净值日期'] = df_sheet3['nav_lookup_fcode'].apply(
        lambda value: latest_nav_data.get(str(value), {}).get('nav_date').strftime('%Y-%m-%d')
        if latest_nav_data.get(str(value), {}).get('nav_date') else None
    )

    df_sheet3 = df_sheet3.sort_values(
        by='总计-综合稳定性得分',
        ascending=False,
        na_position='last',
    ).reset_index(drop=True)

    cols = df_sheet3.columns.tolist()
    new_cols = ['基金代码', '基金名称']
    if 'nav_lookup_fcode' in cols:
        cols.remove('nav_lookup_fcode')
    new_cols.extend(['总计-综合稳定性得分', '最新日增长率(%)', '最新净值日期'])
    for col in cols:
        if col not in new_cols:
            new_cols.append(col)

    df_sheet3 = df_sheet3[new_cols]
    print_fn(f"  ✅ 交集基金表已添加总计得分和最新净值数据,已按总计得分降序排序")
    return df_sheet3


def build_single_side_sheet(
    df_source: pd.DataFrame,
    *,
    effective_codes: set[str],
    empty_message: str,
) -> pd.DataFrame:
    """组装单边优质基金 sheet。"""
    if effective_codes:
        return df_source[df_source['fcode'].isin(effective_codes)].copy()

    df_empty = pd.DataFrame(columns=['基金代码', '基金名称', '说明'])
    df_empty.loc[0] = ['', '', empty_message]
    return df_empty


def build_summary_sheet(
    *,
    code_stats: dict,
    candidate_summary: dict,
    input_lineage: dict,
    short_term_file,
    long_term_file,
    short_report_date: str | None,
    long_report_date: str | None,
    generated_at: datetime | None = None,
) -> pd.DataFrame:
    """组装统计汇总 sheet。"""
    short_base_names = code_stats['short_base_names']
    long_base_names = code_stats['long_base_names']
    generated_at = generated_at or datetime.now()

    return pd.DataFrame({
        '指标': [
            '近期优质基金总数',
            '长期优质基金总数',
            '交集基金数量',
            '邻近交集数量',
            '仅近期优质数量',
            '仅长期优质数量',
            '原始仅近期优质数量',
            '原始仅长期优质数量',
            '交集占近期比例',
            '交集占长期比例',
            '报告目标业务日',
            '近期输入业务日',
            '长期输入业务日',
            '输入配对状态',
            '是否历史回退输入',
            '输入来源说明',
            '近期文件名',
            '长期文件名',
            '生成时间',
        ],
        '数值': [
            len(code_stats['codes_short']),
            len(code_stats['codes_long']),
            candidate_summary['intersection_count'],
            candidate_summary['soft_intersection_count'],
            candidate_summary['only_short_count'],
            candidate_summary['only_long_count'],
            candidate_summary['raw_only_short_count'],
            candidate_summary['raw_only_long_count'],
            f"{candidate_summary['intersection_count']/len(short_base_names)*100:.2f}%" if len(short_base_names) > 0 else "0%",
            f"{candidate_summary['intersection_count']/len(long_base_names)*100:.2f}%" if len(long_base_names) > 0 else "0%",
            input_lineage['today_date'],
            short_report_date or '未知',
            long_report_date or '未知',
            input_lineage['input_pair_status_label'],
            '是' if input_lineage['used_stale_inputs'] else '否',
            input_lineage['input_lineage_note'],
            short_term_file.name,
            long_term_file.name,
            generated_at.strftime('%Y-%m-%d %H:%M:%S'),
        ],
    })
