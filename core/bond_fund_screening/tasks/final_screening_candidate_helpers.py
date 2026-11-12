#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选候选池、加权主榜与软交集辅助工具。"""

from __future__ import annotations

import pandas as pd

from bond_fund_screening.core.filtering_engine import stable_sort_desc
from bond_fund_screening.share_class_utils import collapse_share_classes, normalize_base_fund_name


def select_weighted_final_candidates(
    df_weighted: pd.DataFrame,
    *,
    fund_type: str,
    top_n_limit: int,
    name_col: str = 'fund_name',
    collapse_share_classes_enabled: bool = False,
    excluded_fcodes: set[str] | None = None,
    normalize_fcode_fn,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
) -> pd.DataFrame:
    """从加权排序结果中选出最终候选。"""
    if df_weighted.empty:
        return df_weighted

    target_count = min(max(int(top_n_limit), 0), len(df_weighted))
    if target_count <= 0:
        return df_weighted.head(0).copy()

    selected = df_weighted.copy()

    excluded_fcodes = {
        normalize_fcode_fn(fcode)
        for fcode in (excluded_fcodes or set())
        if normalize_fcode_fn(fcode)
    }
    if excluded_fcodes and 'fcode' in selected.columns:
        selected = selected[~selected['fcode'].isin(excluded_fcodes)].copy()

    if should_filter_holding_period_fn(fund_type) and name_col in selected.columns:
        selected = selected[
            ~selected[name_col].apply(
                lambda value: has_fixed_holding_period_fn(str(value)) if pd.notna(value) else False
            )
        ].copy()

    if collapse_share_classes_enabled:
        selected = collapse_share_classes(selected, name_col=name_col, code_col='fcode')
    return selected.head(target_count).copy()


def summarize_candidate_pool_reduction(
    df_weighted: pd.DataFrame,
    *,
    fund_type: str,
    name_col: str = 'fund_name',
    collapse_share_classes_enabled: bool = False,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
) -> tuple[pd.DataFrame, dict]:
    """返回候选池压缩后的数据与结构化压缩来源统计。"""
    selected = df_weighted.copy()
    reduction_counts = {
        'holding_period_filtered': 0,
        'share_class_collapsed': 0,
    }

    if should_filter_holding_period_fn(fund_type) and name_col in selected.columns:
        holding_mask = selected[name_col].apply(
            lambda value: has_fixed_holding_period_fn(str(value)) if pd.notna(value) else False
        )
        reduction_counts['holding_period_filtered'] = int(holding_mask.sum())
        selected = selected[~holding_mask].copy()

    if collapse_share_classes_enabled:
        before_collapse = len(selected)
        selected = collapse_share_classes(selected, name_col=name_col, code_col='fcode')
        reduction_counts['share_class_collapsed'] = max(before_collapse - len(selected), 0)

    reduction_counts['total_removed'] = int(sum(reduction_counts.values()))
    return selected, reduction_counts


def sort_weighted_candidates(df: pd.DataFrame, score_col: str = 'weighted_score') -> pd.DataFrame:
    """最终名单统一按加权分稳定排序，同分时按基金代码升序。"""
    return stable_sort_desc(df, score_col, tie_break_cols=['fcode', '基金代码'])


def resolve_final_screening_score_column(
    df: pd.DataFrame,
    *,
    primary_score_columns: tuple[str, ...],
    fallback_score_columns: tuple[str, ...],
) -> str | None:
    """解析最终筛选主权重应使用的评分列。"""
    columns = list(df.columns)
    for column in primary_score_columns:
        if column in columns:
            return column

    for column in fallback_score_columns:
        if column in columns:
            return column

    for column in columns:
        column_text = str(column)
        column_lower = column_text.lower()
        if '平均综合评分' in column_text or 'avg_combo_score' in column_lower:
            return column
    for column in columns:
        column_text = str(column)
        column_lower = column_text.lower()
        if '综合稳定性' in column_text or 'comprehensive_score' in column_lower:
            return column
    return None


def resolve_final_screening_direct_score_column(
    df: pd.DataFrame,
    *,
    fund_type: str | None = None,
    primary_score_columns: tuple[str, ...],
    fallback_score_columns: tuple[str, ...],
) -> str | None:
    """解析最终主榜对直接入围候选应使用的评分列。"""
    normalized_fund_type = str(fund_type or '').strip().lower()
    if normalized_fund_type == 'bond':
        for column in fallback_score_columns:
            if column in df.columns:
                return column

        for column in df.columns:
            column_text = str(column)
            column_lower = column_text.lower()
            if '综合稳定性' in column_text or 'comprehensive_score' in column_lower:
                return column

    return resolve_final_screening_score_column(
        df,
        primary_score_columns=primary_score_columns,
        fallback_score_columns=fallback_score_columns,
    )


def resolve_frame_name_column(df: pd.DataFrame) -> str | None:
    """解析 DataFrame 中可用于母基金归一化的名称列。"""
    for column in ('fund_name', '基金名称', 'shortname'):
        if column in df.columns:
            return column
    return None


def ensure_base_fund_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """确保 DataFrame 包含可用的 base_fund_name 列。"""
    result = df.copy()
    name_col = resolve_frame_name_column(result)

    if 'base_fund_name' not in result.columns:
        if name_col is None:
            result['base_fund_name'] = ''
        else:
            result['base_fund_name'] = result[name_col].map(normalize_base_fund_name)
        return result

    result['base_fund_name'] = result['base_fund_name'].fillna('')
    empty_mask = result['base_fund_name'].astype(str).str.strip() == ''
    if empty_mask.any() and name_col is not None:
        result.loc[empty_mask, 'base_fund_name'] = result.loc[empty_mask, name_col].map(
            normalize_base_fund_name
        )
    return result


def build_base_representative_lookup(
    df: pd.DataFrame,
    *,
    score_col: str | None,
) -> dict[str, pd.Series]:
    """按母基金构建代表记录映射。"""
    if df.empty:
        return {}

    working = ensure_base_fund_name_column(df)
    if score_col and score_col in working.columns:
        working = sort_weighted_candidates(working, score_col).reset_index(drop=True)

    lookup: dict[str, pd.Series] = {}
    for _, row in working.iterrows():
        base_name = str(row.get('base_fund_name') or '').strip()
        if not base_name or base_name in lookup:
            continue
        lookup[base_name] = row
    return lookup


def get_score_from_df(df: pd.DataFrame, fcode: str, score_col: str | None) -> float | None:
    """从 DataFrame 获取基金得分。"""
    if score_col is None or 'fcode' not in df.columns or score_col not in df.columns:
        return None

    row = df[df['fcode'] == fcode]
    if row.empty:
        return None

    value = row[score_col].iloc[0]
    if pd.isna(value):
        return None
    return float(value)


def resolve_score_with_fallback(
    primary_score: float | None,
    fallback_scores: dict,
    fcode: str,
) -> float | None:
    """仅当主来源缺失时才回退到补充分值。"""
    if primary_score is not None:
        return primary_score
    return fallback_scores.get(fcode)


def resolve_score_with_base_fallback(
    primary_score: float | None,
    fallback_scores: dict | None,
    fcode: str,
    *,
    base_name: str | None = None,
    fallback_base_scores: dict | None = None,
    normalize_fcode_fn,
) -> float | None:
    """在代码级缺失时允许按母基金口径回退补充分值。"""
    if primary_score is not None:
        return primary_score

    normalized_fcode = normalize_fcode_fn(fcode)
    if normalized_fcode and fallback_scores:
        fallback_score = fallback_scores.get(normalized_fcode)
        if fallback_score is not None:
            return fallback_score

    normalized_base_name = str(base_name or '').strip()
    if normalized_base_name and fallback_base_scores:
        return fallback_base_scores.get(normalized_base_name)

    return None


def resolve_cross_period_count_with_base_fallback(
    fallback_period_counts: dict | None,
    fcode: str,
    *,
    base_name: str | None = None,
    fallback_base_period_counts: dict | None = None,
    normalize_fcode_fn,
) -> int:
    """在代码级缺失时允许按母基金口径回退多周期支撑数。"""
    normalized_fcode = normalize_fcode_fn(fcode)
    if normalized_fcode and fallback_period_counts:
        value = fallback_period_counts.get(normalized_fcode)
        if value is not None:
            return int(value or 0)

    normalized_base_name = str(base_name or '').strip()
    if normalized_base_name and fallback_base_period_counts:
        value = fallback_base_period_counts.get(normalized_base_name)
        if value is not None:
            return int(value or 0)

    return 0


def build_weighted_final_candidate_frame(
    *,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    fund_type: str,
    short_score_col: str | None,
    long_score_col: str | None,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
    policy: dict | None = None,
    min_cross_periods: int = 2,
    resolve_policy_fn,
    normalize_fcode_fn,
) -> tuple[pd.DataFrame, dict]:
    """构建最终主榜候选池。"""
    cross_short_period_counts = cross_short_period_counts or {}
    cross_long_period_counts = cross_long_period_counts or {}
    cross_short_base_scores = cross_short_base_scores or {}
    cross_long_base_scores = cross_long_base_scores or {}
    cross_short_base_period_counts = cross_short_base_period_counts or {}
    cross_long_base_period_counts = cross_long_base_period_counts or {}
    resolved_policy = resolve_policy_fn(policy=policy, fund_type=fund_type)

    short_frame = ensure_base_fund_name_column(df_short)
    long_frame = ensure_base_fund_name_column(df_long)

    short_lookup = {
        row['fcode']: row
        for _, row in short_frame.iterrows()
        if row.get('fcode')
    }
    long_lookup = {
        row['fcode']: row
        for _, row in long_frame.iterrows()
        if row.get('fcode')
    }

    short_base_lookup = build_base_representative_lookup(short_frame, score_col=short_score_col)
    long_base_lookup = build_base_representative_lookup(long_frame, score_col=long_score_col)

    base_short = set(short_base_lookup)
    base_long = set(long_base_lookup)
    hard_intersection_bases = base_short & base_long

    rows = []
    soft_bases = set()
    all_codes = set(short_lookup) | set(long_lookup)

    for fcode in all_codes:
        short_row = short_lookup.get(fcode)
        long_row = long_lookup.get(fcode)
        row_ref = short_row if short_row is not None else long_row
        base_name = str(row_ref.get('base_fund_name') or '').strip() if row_ref is not None else ''
        short_rep = short_base_lookup.get(base_name)
        long_rep = long_base_lookup.get(base_name)

        short_score = get_score_from_df(short_frame, fcode, short_score_col)
        long_score = get_score_from_df(long_frame, fcode, long_score_col)
        short_used_fallback = False
        long_used_fallback = False

        if base_name in hard_intersection_bases:
            if short_score is None and short_rep is not None:
                short_score = get_score_from_df(
                    short_frame,
                    normalize_fcode_fn(short_rep.get('fcode')),
                    short_score_col,
                )
            if long_score is None and long_rep is not None:
                long_score = get_score_from_df(
                    long_frame,
                    normalize_fcode_fn(long_rep.get('fcode')),
                    long_score_col,
                )
            category = '交集优选'
        else:
            if short_score is None:
                short_score = resolve_score_with_base_fallback(
                    None,
                    cross_short_scores,
                    fcode,
                    base_name=base_name,
                    fallback_base_scores=cross_short_base_scores,
                    normalize_fcode_fn=normalize_fcode_fn,
                )
                short_used_fallback = short_score is not None
            if long_score is None:
                long_score = resolve_score_with_base_fallback(
                    None,
                    cross_long_scores,
                    fcode,
                    base_name=base_name,
                    fallback_base_scores=cross_long_base_scores,
                    normalize_fcode_fn=normalize_fcode_fn,
                )
                long_used_fallback = long_score is not None

            soft_eligible = short_score is not None and long_score is not None
            if soft_eligible and short_used_fallback:
                soft_eligible = resolve_cross_period_count_with_base_fallback(
                    cross_short_period_counts,
                    fcode,
                    base_name=base_name,
                    fallback_base_period_counts=cross_short_base_period_counts,
                    normalize_fcode_fn=normalize_fcode_fn,
                ) >= int(min_cross_periods)
            if soft_eligible and long_used_fallback:
                soft_eligible = resolve_cross_period_count_with_base_fallback(
                    cross_long_period_counts,
                    fcode,
                    base_name=base_name,
                    fallback_base_period_counts=cross_long_base_period_counts,
                    normalize_fcode_fn=normalize_fcode_fn,
                ) >= int(min_cross_periods)

            if soft_eligible:
                category = '邻近交集'
                if base_name:
                    soft_bases.add(base_name)
            elif short_row is not None:
                category = '近期优质'
            else:
                category = '长期优质'

        short_score_source = 'local'
        if short_score is None:
            short_score_source = 'missing'
        elif short_used_fallback:
            normalized_fcode = normalize_fcode_fn(fcode)
            if normalized_fcode and normalized_fcode in cross_short_scores:
                short_score_source = 'cross_short_code'
            elif base_name and base_name in cross_short_base_scores:
                short_score_source = 'cross_short_base'

        long_score_source = 'local'
        if long_score is None:
            long_score_source = 'missing'
        elif long_used_fallback:
            normalized_fcode = normalize_fcode_fn(fcode)
            if normalized_fcode and normalized_fcode in cross_long_scores:
                long_score_source = 'cross_long_code'
            elif base_name and base_name in cross_long_base_scores:
                long_score_source = 'cross_long_base'

        short_score_value = 0.0 if short_score is None else float(short_score)
        long_score_value = 0.0 if long_score is None else float(long_score)

        if category == '交集优选':
            weighted_score = (
                short_score_value * float(resolved_policy['short_score_weight'])
                + long_score_value * float(resolved_policy['long_score_weight'])
                + float(resolved_policy['hard_intersection_bonus'])
            )
            is_intersection = True
        elif category == '邻近交集':
            weighted_score = (
                short_score_value * float(resolved_policy['short_score_weight'])
                + long_score_value * float(resolved_policy['long_score_weight'])
                + float(resolved_policy['soft_intersection_bonus'])
            )
            is_intersection = False
        elif category == '近期优质':
            weighted_score = short_score_value * float(resolved_policy['short_score_weight'])
            is_intersection = False
        else:
            weighted_score = long_score_value * float(resolved_policy['long_score_weight'])
            is_intersection = False

        chosen_row = row_ref
        if short_row is not None and long_row is not None and short_row.get('fcode') != long_row.get('fcode'):
            chosen_row = long_row if long_score_value > short_score_value else short_row

        fund_name = None
        runtime_ftype = None
        if chosen_row is not None:
            for name_key in ('fund_name', '基金名称', 'shortname'):
                if chosen_row.get(name_key):
                    fund_name = chosen_row.get(name_key)
                    break
            for ftype_key in ('基金类型', 'ftype', 'fund_type'):
                if chosen_row.get(ftype_key):
                    runtime_ftype = chosen_row.get(ftype_key)
                    break

        rows.append({
            'fcode': normalize_fcode_fn(fcode),
            'fund_name': fund_name,
            '基金类型': runtime_ftype,
            'ftype': runtime_ftype,
            'base_fund_name': base_name,
            'short_score': short_score_value,
            'long_score': long_score_value,
            'weighted_score': float(weighted_score),
            'fund_category': category,
            'is_intersection': is_intersection,
            '_short_score_source': short_score_source,
            '_long_score_source': long_score_source,
        })

    df_weighted = pd.DataFrame(rows)
    if not df_weighted.empty:
        df_weighted = sort_weighted_candidates(df_weighted, 'weighted_score').reset_index(drop=True)

    soft_only_bases = soft_bases - hard_intersection_bases
    only_short_bases = base_short - base_long - soft_only_bases
    only_long_bases = base_long - base_short - soft_only_bases

    summary = {
        'union_candidate_count': int(len(base_short | base_long)),
        'intersection_count': int(len(hard_intersection_bases)),
        'soft_intersection_count': int(len(soft_only_bases)),
        'only_short_count': int(len(only_short_bases)),
        'only_long_count': int(len(only_long_bases)),
        'raw_only_short_count': int(len(set(short_lookup) - set(long_lookup))),
        'raw_only_long_count': int(len(set(long_lookup) - set(short_lookup))),
        'raw_intersection_count': int(len(set(short_lookup) & set(long_lookup))),
    }
    return df_weighted, summary


def build_soft_intersection_audit_map(
    df_weighted: pd.DataFrame,
    *,
    cross_short_scores: dict | None,
    cross_long_scores: dict | None,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
    normalize_fcode_fn,
    safe_float_fn,
) -> dict[str, dict]:
    """为邻近交集候选构造解释字段。"""
    if df_weighted is None or df_weighted.empty:
        return {}

    audit_map: dict[str, dict] = {}
    for _, row in df_weighted.iterrows():
        if str(row.get('fund_category') or '').strip() != '邻近交集':
            continue

        fcode = normalize_fcode_fn(row.get('fcode'))
        if not fcode:
            continue
        base_name = str(row.get('base_fund_name') or '').strip()
        short_score = safe_float_fn(row.get('short_score'))
        long_score = safe_float_fn(row.get('long_score'))
        short_score_source = str(row.get('_short_score_source') or '').strip()
        long_score_source = str(row.get('_long_score_source') or '').strip()

        support_source = []
        support_parts = []
        support_count = 0

        short_code_score = None
        if cross_short_scores:
            short_code_score = cross_short_scores.get(fcode)
        short_base_score = None
        if base_name and cross_short_base_scores:
            short_base_score = cross_short_base_scores.get(base_name)
        if short_score is not None and (
            short_score_source in {'cross_short_code', 'cross_short_base'}
            or not short_score_source and (short_code_score is not None or short_base_score is not None)
        ):
            if short_score_source == 'cross_short_code' or (
                not short_score_source and short_code_score is not None
            ):
                support_source.append('short_code')
                support_parts.append('近期侧由代码级补分池补齐')
                support_count = max(support_count, int((cross_short_period_counts or {}).get(fcode) or 0))
            elif short_score_source == 'cross_short_base' or (
                not short_score_source and short_base_score is not None
            ):
                support_source.append('short_base')
                support_parts.append('近期侧由母基金补分池补齐')
                support_count = max(
                    support_count,
                    int((cross_short_base_period_counts or {}).get(base_name) or 0),
                )

        long_code_score = None
        if cross_long_scores:
            long_code_score = cross_long_scores.get(fcode)
        long_base_score = None
        if base_name and cross_long_base_scores:
            long_base_score = cross_long_base_scores.get(base_name)
        if long_score is not None and (
            long_score_source in {'cross_long_code', 'cross_long_base'}
            or not long_score_source and (long_code_score is not None or long_base_score is not None)
        ):
            if long_score_source == 'cross_long_code' or (
                not long_score_source and long_code_score is not None
            ):
                support_source.append('long_code')
                support_parts.append('长期侧由代码级补分池补齐')
                support_count = max(support_count, int((cross_long_period_counts or {}).get(fcode) or 0))
            elif long_score_source == 'cross_long_base' or (
                not long_score_source and long_base_score is not None
            ):
                support_source.append('long_base')
                support_parts.append('长期侧由母基金补分池补齐')
                support_count = max(
                    support_count,
                    int((cross_long_base_period_counts or {}).get(base_name) or 0),
                )

        if not support_source:
            support_parts.append('双侧分数均已满足邻近交集补齐条件')

        audit_map[fcode] = {
            'soft_intersection_reason': '；'.join(support_parts),
            'soft_intersection_support_source': ','.join(support_source) if support_source else 'scored_on_both_sides',
            'cross_period_support_count': int(support_count),
        }

    return audit_map


def build_single_side_watchlist(
    df_weighted: pd.DataFrame,
    *,
    fund_type: str,
    target_category: str,
    top_n_limit: int,
    excluded_fcodes: set[str] | None = None,
    collapse_share_classes_enabled: bool = True,
    normalize_fcode_fn,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
) -> pd.DataFrame:
    """从加权结果中提取单边候选观察池。"""
    if df_weighted.empty:
        return df_weighted.head(0).copy()

    candidates = df_weighted[df_weighted['fund_category'] == target_category].copy()
    if candidates.empty:
        return candidates

    candidates = sort_weighted_candidates(candidates, 'weighted_score').reset_index(drop=True)
    return select_weighted_final_candidates(
        df_weighted=candidates,
        fund_type=fund_type,
        top_n_limit=top_n_limit,
        collapse_share_classes_enabled=collapse_share_classes_enabled,
        excluded_fcodes=excluded_fcodes,
        normalize_fcode_fn=normalize_fcode_fn,
        should_filter_holding_period_fn=should_filter_holding_period_fn,
        has_fixed_holding_period_fn=has_fixed_holding_period_fn,
    )


def build_soft_intersection(
    *,
    codes_short: set,
    codes_long: set,
    intersection: set,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    short_score_col: str | None,
    long_score_col: str | None,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
    min_cross_periods: int = 1,
    normalize_fcode_fn,
) -> set:
    """计算软交集。"""
    soft_intersection = set()
    normalized_min_cross_periods = max(int(min_cross_periods or 1), 1)
    cross_short_period_counts = cross_short_period_counts or {}
    cross_long_period_counts = cross_long_period_counts or {}
    cross_short_base_scores = cross_short_base_scores or {}
    cross_long_base_scores = cross_long_base_scores or {}
    cross_short_base_period_counts = cross_short_base_period_counts or {}
    cross_long_base_period_counts = cross_long_base_period_counts or {}
    short_base_name_lookup = {
        normalize_fcode_fn(row.get('fcode')): str(row.get('base_fund_name') or '').strip()
        for _, row in ensure_base_fund_name_column(df_short).iterrows()
        if normalize_fcode_fn(row.get('fcode'))
    }
    long_base_name_lookup = {
        normalize_fcode_fn(row.get('fcode')): str(row.get('base_fund_name') or '').strip()
        for _, row in ensure_base_fund_name_column(df_long).iterrows()
        if normalize_fcode_fn(row.get('fcode'))
    }
    for fcode in (codes_short | codes_long) - intersection:
        local_short_score = get_score_from_df(df_short, fcode, short_score_col)
        local_long_score = get_score_from_df(df_long, fcode, long_score_col)
        base_name = short_base_name_lookup.get(fcode) or long_base_name_lookup.get(fcode) or ''
        short_score = resolve_score_with_base_fallback(
            local_short_score,
            cross_short_scores,
            fcode,
            base_name=base_name,
            fallback_base_scores=cross_short_base_scores,
            normalize_fcode_fn=normalize_fcode_fn,
        )
        long_score = resolve_score_with_base_fallback(
            local_long_score,
            cross_long_scores,
            fcode,
            base_name=base_name,
            fallback_base_scores=cross_long_base_scores,
            normalize_fcode_fn=normalize_fcode_fn,
        )
        if short_score is None or long_score is None:
            continue

        if local_short_score is None:
            short_period_count = resolve_cross_period_count_with_base_fallback(
                cross_short_period_counts,
                fcode,
                base_name=base_name,
                fallback_base_period_counts=cross_short_base_period_counts,
                normalize_fcode_fn=normalize_fcode_fn,
            )
            if short_period_count < normalized_min_cross_periods:
                continue
        if local_long_score is None:
            long_period_count = resolve_cross_period_count_with_base_fallback(
                cross_long_period_counts,
                fcode,
                base_name=base_name,
                fallback_base_period_counts=cross_long_base_period_counts,
                normalize_fcode_fn=normalize_fcode_fn,
            )
            if long_period_count < normalized_min_cross_periods:
                continue

        soft_intersection.add(fcode)
    return soft_intersection
