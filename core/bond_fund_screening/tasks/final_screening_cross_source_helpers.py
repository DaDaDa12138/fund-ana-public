#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选 cross-score / cross-period 数据源辅助工具。"""

from __future__ import annotations

import logging
import sqlite3
from functools import partial

import pandas as pd


def sqlite_table_has_column(conn, table_name: str, column_name: str) -> bool:
    """判断 SQLite 表是否包含指定列。"""
    try:
        columns = pd.read_sql(f"PRAGMA table_info({table_name})", conn)
    except Exception:
        return False
    if columns.empty or 'name' not in columns.columns:
        return False
    return str(column_name) in set(columns['name'].astype(str).tolist())


def load_latest_run_ids(
    conn,
    *,
    fund_type: str,
    period_condition: str,
    target_date: str | None,
    normalize_report_date_fn,
) -> list:
    """按基金类型、周期分组和目标日期加载每周期最新成功 run_id。"""
    query = f"""
        SELECT run_id, period_years, finished_at,
               ROW_NUMBER() OVER (PARTITION BY period_years ORDER BY finished_at DESC) as rn
        FROM screening_runs
        WHERE status = 'success' AND fund_type = ? AND {period_condition}
    """
    params = [fund_type]
    normalized_target_date = normalize_report_date_fn(target_date)
    if normalized_target_date:
        query += " AND DATE(finished_at) = ?"
        params.append(normalized_target_date)

    runs_df = pd.read_sql(query, conn, params=params)
    if runs_df.empty:
        return []
    latest_runs = runs_df[runs_df['rn'] == 1]
    return latest_runs['run_id'].tolist()


def build_cross_source_runtime(
    *,
    project_db_path_getter,
    normalize_fcode_fn,
    normalize_report_date_fn,
    normalize_base_fund_name_fn,
):
    """构造 cross-source 查询所需的运行时依赖集合。"""
    return {
        'project_db_path_getter': project_db_path_getter,
        'normalize_fcode_fn': normalize_fcode_fn,
        'normalize_base_fund_name_fn': normalize_base_fund_name_fn,
        'load_latest_run_ids_fn': partial(
            load_latest_run_ids,
            normalize_report_date_fn=normalize_report_date_fn,
        ),
        'sqlite_table_has_column_fn': sqlite_table_has_column,
    }


def load_cross_scores_with_base_maps(
    *,
    fund_type: str,
    short_target_date: str | None,
    long_target_date: str | None,
    project_db_path_getter,
    normalize_fcode_fn,
    normalize_base_fund_name_fn,
    load_latest_run_ids_fn,
    sqlite_table_has_column_fn,
    logger=None,
) -> tuple[dict, dict, dict, dict]:
    """从 screening_picks 表加载近期/长期评分及母基金级 fallback 映射。"""
    logger = logger or logging.getLogger(__name__)
    try:
        db_path = project_db_path_getter()
        if not db_path.exists():
            return {}, {}, {}, {}

        with sqlite3.connect(str(db_path)) as conn:
            picks_has_shortname = sqlite_table_has_column_fn(conn, 'screening_picks', 'shortname')
            short_run_ids = load_latest_run_ids_fn(
                conn,
                fund_type=fund_type,
                period_condition='period_years < 1',
                target_date=short_target_date,
            )
            long_run_ids = load_latest_run_ids_fn(
                conn,
                fund_type=fund_type,
                period_condition='period_years >= 1',
                target_date=long_target_date,
            )

            def _query_scores(run_ids):
                if not run_ids:
                    return {}, {}
                placeholders = ','.join('?' * len(run_ids))
                shortname_select = 'shortname' if picks_has_shortname else "'' AS shortname"
                df = pd.read_sql(
                    f"SELECT fcode, {shortname_select}, AVG(COALESCE(selected_combo_score, combo_score)) as avg_score "
                    f"FROM screening_picks WHERE run_id IN ({placeholders}) "
                    f"GROUP BY fcode, shortname",
                    conn,
                    params=run_ids,
                )
                df['fcode'] = df['fcode'].map(normalize_fcode_fn)
                df = df[df['fcode'].notna()].copy()
                if df.empty:
                    return {}, {}
                df = df.groupby('fcode', as_index=False)['avg_score'].mean()
                df = df[df['avg_score'].notna()].copy()
                if df.empty:
                    return {}, {}

                raw_df = pd.read_sql(
                    f"SELECT fcode, {shortname_select}, COALESCE(selected_combo_score, combo_score) as avg_score "
                    f"FROM screening_picks WHERE run_id IN ({placeholders})",
                    conn,
                    params=run_ids,
                )
                raw_df['fcode'] = raw_df['fcode'].map(normalize_fcode_fn)
                raw_df = raw_df[raw_df['fcode'].notna()].copy()
                raw_df['base_fund_name'] = raw_df['shortname'].map(normalize_base_fund_name_fn)
                raw_df = raw_df[raw_df['avg_score'].notna()].copy()
                base_scores = {}
                if not raw_df.empty:
                    base_df = raw_df[
                        raw_df['base_fund_name'].astype(str).str.strip() != ''
                    ].copy()
                    if not base_df.empty:
                        base_df = base_df.groupby('base_fund_name', as_index=False)['avg_score'].mean()
                        base_scores = dict(zip(base_df['base_fund_name'], base_df['avg_score']))

                return dict(zip(df['fcode'], df['avg_score'])), base_scores

            short_scores, short_base_scores = _query_scores(short_run_ids)
            long_scores, long_base_scores = _query_scores(long_run_ids)
            return short_scores, long_scores, short_base_scores, long_base_scores
    except Exception as exc:
        logger.warning(f"_load_cross_scores(fund_type={fund_type}) 查询失败: {exc}")
        return {}, {}, {}, {}


def load_cross_period_counts_with_base_maps(
    *,
    fund_type: str,
    short_target_date: str | None,
    long_target_date: str | None,
    project_db_path_getter,
    normalize_fcode_fn,
    normalize_base_fund_name_fn,
    load_latest_run_ids_fn,
    sqlite_table_has_column_fn,
    logger=None,
) -> tuple[dict, dict, dict, dict]:
    """加载 cross-period 覆盖数及母基金级 fallback 映射。"""
    logger = logger or logging.getLogger(__name__)
    try:
        db_path = project_db_path_getter()
        if not db_path.exists():
            return {}, {}, {}, {}

        with sqlite3.connect(str(db_path)) as conn:
            picks_has_shortname = sqlite_table_has_column_fn(conn, 'screening_picks', 'shortname')
            short_run_ids = load_latest_run_ids_fn(
                conn,
                fund_type=fund_type,
                period_condition='period_years < 1',
                target_date=short_target_date,
            )
            long_run_ids = load_latest_run_ids_fn(
                conn,
                fund_type=fund_type,
                period_condition='period_years >= 1',
                target_date=long_target_date,
            )

            def _query_period_counts(run_ids):
                if not run_ids:
                    return {}, {}
                placeholders = ','.join('?' * len(run_ids))
                shortname_select = 'shortname' if picks_has_shortname else "'' AS shortname"
                df = pd.read_sql(
                    f"SELECT fcode, {shortname_select}, period_years "
                    f"FROM screening_picks WHERE run_id IN ({placeholders})",
                    conn,
                    params=run_ids,
                )
                if df.empty:
                    return {}, {}
                df['fcode'] = df['fcode'].map(normalize_fcode_fn)
                df = df[df['fcode'].notna()].copy()
                if df.empty:
                    return {}, {}
                df['period_years'] = pd.to_numeric(df['period_years'], errors='coerce')

                code_df = df[['fcode', 'period_years']].dropna().drop_duplicates()
                code_counts = {}
                if not code_df.empty:
                    code_df = code_df.groupby('fcode', as_index=False)['period_years'].nunique()
                    code_df = code_df.rename(columns={'period_years': 'period_count'})
                    code_df['period_count'] = code_df['period_count'].astype(int)
                    code_counts = dict(zip(code_df['fcode'], code_df['period_count']))

                df['base_fund_name'] = df['shortname'].map(normalize_base_fund_name_fn)
                base_df = df[
                    (df['base_fund_name'].astype(str).str.strip() != '') &
                    df['period_years'].notna()
                ][['base_fund_name', 'period_years']].drop_duplicates()
                base_counts = {}
                if not base_df.empty:
                    base_df = base_df.groupby('base_fund_name', as_index=False)['period_years'].nunique()
                    base_df = base_df.rename(columns={'period_years': 'period_count'})
                    base_df['period_count'] = base_df['period_count'].astype(int)
                    base_counts = dict(zip(base_df['base_fund_name'], base_df['period_count']))

                return code_counts, base_counts

            short_counts, short_base_counts = _query_period_counts(short_run_ids)
            long_counts, long_base_counts = _query_period_counts(long_run_ids)
            return short_counts, long_counts, short_base_counts, long_base_counts
    except Exception as exc:
        logger.warning(f"_load_cross_period_counts(fund_type={fund_type}) 查询失败: {exc}")
        return {}, {}, {}, {}
