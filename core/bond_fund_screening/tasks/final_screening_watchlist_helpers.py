#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选观察池、解释摘要与候选决策账本辅助工具。"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pandas as pd

from bond_fund_screening.share_class_utils import normalize_base_fund_name


def rebind_watchlist_to_validated_representatives(
    df_watchlist: pd.DataFrame,
    *,
    df_weighted_selected_pool: pd.DataFrame,
    name_col: str = 'fund_name',
) -> pd.DataFrame:
    """将观察池条目重绑定到已完成主榜校验的代表份额。"""
    if df_watchlist.empty or df_weighted_selected_pool.empty:
        return df_watchlist.copy()

    rebound = df_watchlist.copy()
    if 'base_fund_name' not in rebound.columns:
        rebound['base_fund_name'] = rebound[name_col].map(normalize_base_fund_name)
    else:
        rebound['base_fund_name'] = rebound['base_fund_name'].fillna('')
        empty_base_mask = rebound['base_fund_name'].astype(str).str.strip() == ''
        if empty_base_mask.any():
            rebound.loc[empty_base_mask, 'base_fund_name'] = rebound.loc[
                empty_base_mask, name_col
            ].map(normalize_base_fund_name)

    selected_pool = df_weighted_selected_pool.copy()
    if 'base_fund_name' not in selected_pool.columns:
        selected_pool['base_fund_name'] = selected_pool[name_col].map(normalize_base_fund_name)
    else:
        selected_pool['base_fund_name'] = selected_pool['base_fund_name'].fillna('')
        empty_base_mask = selected_pool['base_fund_name'].astype(str).str.strip() == ''
        if empty_base_mask.any():
            selected_pool.loc[empty_base_mask, 'base_fund_name'] = selected_pool.loc[
                empty_base_mask, name_col
            ].map(normalize_base_fund_name)

    selected_columns = [
        'base_fund_name',
        'fcode',
        'fund_name',
        'weighted_score',
        'short_score',
        'long_score',
        'is_intersection',
        '1年-最长回撤观测数',
    ]
    selected_lookup = selected_pool[selected_columns].drop_duplicates(
        subset=['base_fund_name'],
        keep='first',
    )
    rebound = rebound.merge(
        selected_lookup,
        on='base_fund_name',
        how='inner',
        suffixes=('', '_validated'),
    )

    for column in (
        'fcode',
        'fund_name',
        'weighted_score',
        'short_score',
        'long_score',
        'is_intersection',
        '1年-最长回撤观测数',
    ):
        validated_col = f'{column}_validated'
        if validated_col not in rebound.columns:
            continue
        if column in rebound.columns:
            rebound[column] = rebound[validated_col].combine_first(rebound[column])
        else:
            rebound[column] = rebound[validated_col]
        rebound = rebound.drop(columns=[validated_col])

    return rebound


def annotate_watchlist_with_mainlist_context(
    df_watchlist: pd.DataFrame,
    *,
    df_weighted_top: pd.DataFrame,
    df_weighted_selected_pool: pd.DataFrame,
) -> pd.DataFrame:
    """为观察池补充主榜状态、分差和结构性原因解释。"""
    if df_watchlist.empty:
        return df_watchlist.copy()

    annotated = df_watchlist.copy()
    top_codes = set(df_weighted_top['fcode'].tolist()) if 'fcode' in df_weighted_top.columns else set()
    rank_lookup = {}
    if 'fcode' in df_weighted_selected_pool.columns:
        rank_lookup = {
            fcode: idx
            for idx, fcode in enumerate(df_weighted_selected_pool['fcode'].tolist(), start=1)
        }

    top_cutoff_score = None
    if not df_weighted_top.empty and 'weighted_score' in df_weighted_top.columns:
        top_cutoff_score = float(df_weighted_top['weighted_score'].iloc[-1])

    category_counts = {}
    if 'fund_category' in df_weighted_top.columns and not df_weighted_top.empty:
        category_counts = df_weighted_top['fund_category'].value_counts().to_dict()
    top_category_summary = '、'.join(
        f"{category}{count}只" for category, count in category_counts.items()
    )

    annotated['主榜状态'] = ''
    annotated['名单层级'] = 'watch'
    annotated['推荐状态'] = 'watch_only'
    annotated['观察原因代码'] = ''
    annotated['主榜位次'] = None
    annotated['主榜末位得分'] = round(top_cutoff_score, 2) if top_cutoff_score is not None else None
    annotated['距主榜末位差距'] = None
    annotated['未进主榜原因'] = ''

    for idx, row in annotated.iterrows():
        fcode = row.get('fcode')
        weighted_score = float(row.get('weighted_score', 0) or 0)
        rank = rank_lookup.get(fcode)
        in_main_list = fcode in top_codes
        gap_to_cutoff = None
        if top_cutoff_score is not None:
            gap_to_cutoff = round(max(top_cutoff_score - weighted_score, 0.0), 2)

        if row.get('fund_category') == '近期优质':
            dimension_gap = '仅近期突出，长期确认不足'
        elif row.get('fund_category') == '长期优质':
            dimension_gap = '仅长期稳定，近期动能不足'
        else:
            dimension_gap = '单一维度有效，双周期共振不足'

        reason_parts = []
        if in_main_list:
            status = '已进入主榜'
            list_tier = 'main'
            recommendation_status = 'official'
            watch_only_reason_code = ''
            reason_parts.append('该基金已进入最终主榜，观察池保留其单边特征用于后续跟踪')
        else:
            status = '观察池候选'
            list_tier = 'watch'
            recommendation_status = 'watch_only'
            reason_parts.append(dimension_gap)
            reason_parts.append('未形成双周期共振加分')
            if gap_to_cutoff is not None:
                if gap_to_cutoff > 0:
                    reason_parts.append(f"与主榜分数线还差 {gap_to_cutoff:.2f} 分")
                else:
                    reason_parts.append('已接近主榜分数线，但稳定排序仍靠后')
            if top_category_summary:
                reason_parts.append(f"当前主榜更偏向双周期共振基金（{top_category_summary}）")
            if row.get('fund_category') == '近期优质':
                action_hint = '更适合继续观察长期确认信号'
            elif row.get('fund_category') == '长期优质':
                action_hint = '更适合中长期跟踪，等待近期动能改善'
            else:
                action_hint = '继续观察双周期共振是否形成'
            reason_parts.append(f"操作提示：{action_hint}")
            if row.get('fund_category') == '近期优质':
                watch_only_reason_code = 'SHORT_ONLY_SIGNAL'
            elif row.get('fund_category') == '长期优质':
                watch_only_reason_code = 'LONG_ONLY_SIGNAL'
            else:
                watch_only_reason_code = 'NO_MAINLIST_CONSENSUS'

        annotated.at[idx, '主榜状态'] = status
        annotated.at[idx, '名单层级'] = list_tier
        annotated.at[idx, '推荐状态'] = recommendation_status
        annotated.at[idx, '观察原因代码'] = watch_only_reason_code
        annotated.at[idx, '主榜位次'] = rank
        annotated.at[idx, '距主榜末位差距'] = gap_to_cutoff
        annotated.at[idx, '未进主榜原因'] = '；'.join(reason_parts)

    return annotated


def build_final_screening_decision_ledger_records(
    *,
    df_weighted: pd.DataFrame,
    fund_type: str,
    df_weighted_selected_pool_prevalidation: pd.DataFrame,
    df_weighted_selected_pool: pd.DataFrame,
    df_weighted_top: pd.DataFrame,
    incomplete_candidates_df: pd.DataFrame | None,
    autostock_failed_candidates_df: pd.DataFrame | None,
    autostock_validation_status_map: dict[str, dict] | None,
    short_watchlist: pd.DataFrame | None,
    long_watchlist: pd.DataFrame | None,
    candidate_summary: dict | None,
    top_cutoff_score: float | None,
    soft_intersection_audit_map: dict[str, dict] | None = None,
    bond_subtype_edge_governance: dict | None = None,
    final_mainlist_required_columns: tuple[str, ...],
    normalize_fcode_fn,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
) -> list[dict]:
    """构造最终筛选候选级决策账本记录。"""
    candidate_summary = candidate_summary or {}
    autostock_validation_status_map = autostock_validation_status_map or {}
    soft_intersection_audit_map = soft_intersection_audit_map or {}
    bond_subtype_edge_governance = bond_subtype_edge_governance or {}
    normalized_fund_type = str(fund_type or '').strip().lower()
    subtype_edge_in_lookup = {
        str(item.get('in_fcode') or '').strip(): item
        for item in (bond_subtype_edge_governance.get('rows') or [])
        if str(item.get('in_fcode') or '').strip()
    }
    subtype_edge_out_lookup = {
        str(item.get('out_fcode') or '').strip(): item
        for item in (bond_subtype_edge_governance.get('rows') or [])
        if str(item.get('out_fcode') or '').strip()
    }

    incomplete_reason_lookup: dict[str, str] = {}
    incomplete_status_lookup: dict[str, str] = {}
    if isinstance(incomplete_candidates_df, pd.DataFrame) and not incomplete_candidates_df.empty:
        required_cols = [col for col in final_mainlist_required_columns if col in incomplete_candidates_df.columns]
        for _, row in incomplete_candidates_df.iterrows():
            fcode = normalize_fcode_fn(row.get('基金代码'))
            if not fcode:
                continue
            missing_cols = [
                col for col in required_cols
                if pd.isna(row.get(col))
            ]
            if missing_cols:
                incomplete_reason_lookup[fcode] = '缺少关键指标: ' + ', '.join(missing_cols)
                incomplete_status_lookup[fcode] = 'filtered_missing_metrics'
            else:
                incomplete_reason_lookup[fcode] = '关键指标查验失败'
                incomplete_status_lookup[fcode] = 'filtered_missing_metrics'

    autostock_failed_lookup: dict[str, dict] = {}
    if isinstance(autostock_failed_candidates_df, pd.DataFrame) and not autostock_failed_candidates_df.empty:
        for _, row in autostock_failed_candidates_df.iterrows():
            fcode = normalize_fcode_fn(row.get('基金代码') or row.get('fcode'))
            if not fcode:
                continue
            autostock_failed_lookup[fcode] = {
                'reason': str(row.get('失败原因') or '').strip(),
                'warnings': [item for item in str(row.get('告警原因') or '').split('；') if item],
            }

    watchlist_lookup: dict[str, dict] = {}
    for df_watchlist, watchlist_name in (
        (short_watchlist, 'only_short_watchlist'),
        (long_watchlist, 'only_long_watchlist'),
    ):
        if not isinstance(df_watchlist, pd.DataFrame) or df_watchlist.empty:
            continue
        for _, row in df_watchlist.iterrows():
            fcode = normalize_fcode_fn(row.get('fcode') or row.get('代码'))
            if not fcode:
                continue
            reasons = [
                part.strip()
                for part in re.split(r'[；;]+', str(row.get('未进主榜原因', '') or '').strip())
                if part.strip()
            ]
            watchlist_lookup[fcode] = {
                'watchlist_name': watchlist_name,
                'watchlist_status': str(row.get('主榜状态') or '').strip(),
                'list_tier': str(row.get('名单层级') or '').strip() or 'watch',
                'recommendation_status': str(row.get('推荐状态') or '').strip() or 'watch_only',
                'watch_only_reason_code': str(row.get('观察原因代码') or '').strip() or None,
                'watchlist_reasons': reasons,
                'mainlist_rank': (
                    int(row.get('主榜位次'))
                    if pd.notna(row.get('主榜位次'))
                    else None
                ),
                'gap_to_cutoff': (
                    round(float(row.get('距主榜末位差距')), 4)
                    if pd.notna(row.get('距主榜末位差距'))
                    else None
                ),
            }

    prevalidation_codes = set()
    prevalidation_base_representative: dict[str, str] = {}
    if isinstance(df_weighted_selected_pool_prevalidation, pd.DataFrame) and 'fcode' in df_weighted_selected_pool_prevalidation.columns:
        prevalidation_codes = {
            normalize_fcode_fn(code)
            for code in df_weighted_selected_pool_prevalidation['fcode'].tolist()
            if normalize_fcode_fn(code)
        }
        if 'base_fund_name' in df_weighted_selected_pool_prevalidation.columns:
            for _, row in df_weighted_selected_pool_prevalidation.iterrows():
                base_name = str(row.get('base_fund_name') or '').strip()
                fcode = normalize_fcode_fn(row.get('fcode'))
                if base_name and fcode and base_name not in prevalidation_base_representative:
                    prevalidation_base_representative[base_name] = fcode

    eligible_codes = set()
    if isinstance(df_weighted_selected_pool, pd.DataFrame) and 'fcode' in df_weighted_selected_pool.columns:
        eligible_codes = {
            normalize_fcode_fn(code)
            for code in df_weighted_selected_pool['fcode'].tolist()
            if normalize_fcode_fn(code)
        }

    top_rank_lookup: dict[str, int] = {}
    top_codes = set()
    if isinstance(df_weighted_top, pd.DataFrame) and 'fcode' in df_weighted_top.columns:
        top_rank_lookup = {
            normalize_fcode_fn(code): idx
            for idx, code in enumerate(df_weighted_top['fcode'].tolist(), start=1)
            if normalize_fcode_fn(code)
        }
        top_codes = set(top_rank_lookup)

    union_candidate_count = int(candidate_summary.get('union_candidate_count', 0) or 0)
    records = []
    if not isinstance(df_weighted, pd.DataFrame) or df_weighted.empty:
        return records

    for _, row in df_weighted.iterrows():
        fcode = normalize_fcode_fn(row.get('fcode'))
        if not fcode:
            continue

        candidate_stage = 'weighted_union_candidate'
        if fcode in top_codes:
            candidate_stage = 'final_mainlist'
        elif fcode in eligible_codes:
            candidate_stage = 'eligible_pool'
        elif fcode in prevalidation_codes:
            candidate_stage = 'prevalidation_pool'
        elif fcode in incomplete_reason_lookup:
            candidate_stage = 'filtered_by_data_completeness'

        autostock_state = autostock_validation_status_map.get(fcode, {})
        autostock_failure = autostock_failed_lookup.get(fcode, {})
        watchlist_state = watchlist_lookup.get(fcode, {})

        selected_pool_status = (
            'final_mainlist'
            if fcode in top_codes else
            'eligible_pool'
            if fcode in eligible_codes else
            'prevalidation_pool'
            if fcode in prevalidation_codes else
            'excluded'
        )

        excluded_reason_parts = []
        data_completeness_status = 'passed'
        data_completeness_reason = ''
        if fcode in incomplete_reason_lookup:
            data_completeness_status = incomplete_status_lookup.get(fcode) or 'filtered_missing_metrics'
            data_completeness_reason = incomplete_reason_lookup.get(fcode) or ''
            excluded_reason_parts.append(data_completeness_reason)

        autostock_validation_status = str(
            autostock_state.get('autostock_validation_status')
            or ('hard_block' if autostock_failure else '')
            or 'not_checked'
        ).strip()
        autostock_validation_reason = str(
            autostock_failure.get('reason')
            or '；'.join(autostock_state.get('autostock_hard_reasons', []) or [])
            or ''
        ).strip()
        autostock_warning_reasons = list(
            autostock_failure.get('warnings')
            or autostock_state.get('autostock_warning_reasons', [])
            or []
        )
        autostock_warning_display = str(
            autostock_state.get('autostock_warning_display') or ''
        ).strip()
        if autostock_validation_status == 'hard_block' and autostock_validation_reason:
            excluded_reason_parts.append(f'数据校验拦截: {autostock_validation_reason}')

        fund_name = str(row.get('fund_name') or '').strip()
        base_fund_name = str(row.get('base_fund_name') or '').strip()
        representative_fcode = prevalidation_base_representative.get(base_fund_name)
        if (
            fcode not in prevalidation_codes and
            data_completeness_status == 'passed' and
            not autostock_validation_reason
        ):
            if should_filter_holding_period_fn(normalized_fund_type) and has_fixed_holding_period_fn(fund_name):
                excluded_reason_parts.append('候选池压缩阶段因持有期/定开约束被过滤')
            elif representative_fcode and representative_fcode != fcode:
                excluded_reason_parts.append(
                    f'候选池压缩阶段因同母基金份额合并被折叠，保留代表份额 {representative_fcode}'
                )

        if fcode in subtype_edge_out_lookup:
            excluded_reason_parts.append('主榜边缘子类型治理替换出榜')
        elif fcode in eligible_codes and fcode not in top_codes:
            excluded_reason_parts.append('进入主榜前候选池，但未达到最终主榜锚点')
        if watchlist_state.get('watchlist_reasons') and fcode not in top_codes:
            excluded_reason_parts.extend(watchlist_state['watchlist_reasons'])

        if fcode in top_codes:
            list_tier = 'main'
            recommendation_status = 'official'
            watch_only_reason_code = None
        elif watchlist_state:
            list_tier = watchlist_state.get('list_tier') or 'watch'
            recommendation_status = watchlist_state.get('recommendation_status') or 'watch_only'
            watch_only_reason_code = watchlist_state.get('watch_only_reason_code')
        else:
            list_tier = 'excluded'
            recommendation_status = 'not_recommended'
            watch_only_reason_code = None

        record = {
            'fcode': fcode,
            'fund_name': fund_name,
            'base_fund_name': base_fund_name,
            'candidate_stage': candidate_stage,
            'list_tier': list_tier,
            'recommendation_status': recommendation_status,
            'watch_only_reason_code': watch_only_reason_code,
            'fund_category': str(row.get('fund_category') or '').strip(),
            'weighted_score': round(float(row.get('weighted_score', 0.0) or 0.0), 4),
            'short_score': round(float(row.get('short_score', 0.0) or 0.0), 4),
            'long_score': round(float(row.get('long_score', 0.0) or 0.0), 4),
            'union_candidate_count': union_candidate_count,
            'data_completeness_status': data_completeness_status,
            'data_completeness_reason': data_completeness_reason,
            'validation_source': autostock_state.get('validation_source'),
            'validation_contract': autostock_state.get('validation_contract'),
            'validation_status': autostock_state.get('validation_status'),
            'autostock_validation_status': autostock_validation_status,
            'autostock_validation_reason': autostock_validation_reason,
            'autostock_warning_reasons': autostock_warning_reasons,
            'autostock_warning_display': autostock_warning_display,
            'selected_pool_status': selected_pool_status,
            'final_mainlist_selected': bool(fcode in top_codes),
            'top_n_rank': top_rank_lookup.get(fcode),
            'top_n_cutoff_score': round(float(top_cutoff_score), 4) if top_cutoff_score is not None else None,
            'watchlist_name': watchlist_state.get('watchlist_name'),
            'watchlist_status': watchlist_state.get('watchlist_status'),
            'watchlist_reasons': list(watchlist_state.get('watchlist_reasons') or []),
            'watchlist_mainlist_rank': watchlist_state.get('mainlist_rank'),
            'watchlist_gap_to_cutoff': watchlist_state.get('gap_to_cutoff'),
            'excluded_reason': '；'.join(part for part in excluded_reason_parts if part),
            'soft_intersection_reason': None,
            'soft_intersection_support_source': None,
            'cross_period_support_count': None,
            'bond_subtype_edge_governance': None,
        }
        record.update(soft_intersection_audit_map.get(fcode, {}))
        if fcode in subtype_edge_in_lookup:
            edge_item = subtype_edge_in_lookup[fcode]
            record['bond_subtype_edge_governance'] = {
                'action': 'replaced_in',
                'counterparty_fcode': edge_item.get('out_fcode'),
                'counterparty_fund_name': edge_item.get('out_fund_name'),
                'score_gap': edge_item.get('score_gap'),
            }
        elif fcode in subtype_edge_out_lookup:
            edge_item = subtype_edge_out_lookup[fcode]
            record['bond_subtype_edge_governance'] = {
                'action': 'replaced_out',
                'counterparty_fcode': edge_item.get('in_fcode'),
                'counterparty_fund_name': edge_item.get('in_fund_name'),
                'score_gap': edge_item.get('score_gap'),
            }
        records.append(record)

    records.sort(
        key=lambda item: (
            0 if item.get('final_mainlist_selected') else 1,
            -(float(item.get('weighted_score', 0.0) or 0.0)),
            str(item.get('fcode') or ''),
        )
    )
    return records


def normalize_watchlist_reason_part(reason_part: str) -> str | None:
    """将观察池中的逐条原因归一为可汇总口径。"""
    part = str(reason_part or '').strip()
    if not part:
        return None
    if part.startswith('仅近期突出，长期确认不足') or part.startswith('缺少长期维度确认'):
        return '单边优势，另一维确认不足'
    if part.startswith('仅长期稳定，近期动能不足') or part.startswith('缺少近期维度确认'):
        return '单边优势，另一维确认不足'
    if part.startswith('单一维度有效，双周期共振不足') or part.startswith('缺少双维度共振'):
        return '单边优势，另一维确认不足'
    if part.startswith('未形成双周期共振加分') or part.startswith('未获得交集/邻近交集加分'):
        return '双周期共振不足'
    if part.startswith('与主榜分数线还差') or part.startswith('已接近主榜分数线'):
        return '与主榜分数线仍有差距'
    if part.startswith('当前主榜更偏向双周期共振基金') or part.startswith('当前主榜主要由'):
        return '当前主榜更偏向双周期共振基金'
    if part.startswith('操作提示：更适合继续观察长期确认信号'):
        return '操作提示：继续观察长期确认'
    if part.startswith('操作提示：更适合中长期跟踪，等待近期动能改善'):
        return '操作提示：中长期跟踪'
    if part.startswith('操作提示：继续观察双周期共振是否形成'):
        return '操作提示：等待双周期共振'
    if part.startswith('该基金已进入最终主榜'):
        return '已进入主榜，观察池仅保留单边特征'
    return part


def summarize_watchlist_context(*watchlists: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """汇总观察池状态分布与主要落选原因。"""
    status_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()

    for df_watchlist in watchlists:
        if df_watchlist is None or df_watchlist.empty:
            continue
        for _, row in df_watchlist.iterrows():
            status = str(row.get('主榜状态', '') or '').strip()
            if status:
                status_counter[status] += 1

            if status == '已进入主榜':
                continue

            reason_text = str(row.get('未进主榜原因', '') or '').strip()
            if not reason_text:
                continue

            for part in re.split(r'[；;]+', reason_text):
                normalized = normalize_watchlist_reason_part(part)
                if normalized:
                    reason_counter[normalized] += 1

    status_counts = [
        {'status': status, 'count': int(count)}
        for status, count in status_counter.most_common()
    ]
    reason_counts = [
        {'reason': reason, 'count': int(count)}
        for reason, count in reason_counter.most_common(6)
    ]
    return status_counts, reason_counts


def format_summary_count_items(
    items: list[dict] | None,
    *,
    label_key: str,
    count_key: str = 'count',
    limit: int = 3,
    label_transform=None,
) -> str:
    """将结构化计数项压缩为适合摘要展示的短句。"""
    if not items:
        return ''

    parts = []
    for item in items:
        label = str(item.get(label_key, '') or '').strip()
        count = int(item.get(count_key, 0) or 0)
        if not label or count <= 0:
            continue
        if callable(label_transform):
            label = str(label_transform(label) or '').strip()
        if not label:
            continue
        parts.append(f"{label}{count}只")
        if len(parts) >= max(int(limit), 1):
            break
    return '、'.join(parts)


def is_watchlist_action_hint(reason: str) -> bool:
    """判断观察池原因是否属于后续动作提示。"""
    return str(reason or '').strip().startswith('操作提示：')


def build_strategy_validation_note(fund_type: str) -> str:
    """返回经实跑验证后的分类型策略说明。"""
    normalized = str(fund_type or '').strip().lower()
    if normalized == 'stock':
        return '股票链 expanded 虽扩大了双周期共振候选池，但独立 TopN 回测未跑赢 baseline，因此当前不建议把扩容本身视为收益增强信号。'
    if normalized == 'hybrid':
        return '混合链 expanded 虽扩大了交集与邻近交集，但独立 TopN 回测同样未跑赢 baseline，因此当前不继续放宽。'
    return '债券链当前不建议继续扩容，重点是解释为什么入选、为什么被压到观察池。'


def build_final_screening_explanations(
    *,
    fund_type: str,
    union_candidate_count: int,
    eligible_pool_count: int,
    dedup_or_filtered_removed: int,
    top_n_trimmed: int,
    final_output_count: int,
    intersection_count: int,
    soft_intersection_count: int,
    only_short_count: int,
    only_long_count: int,
    final_top_category_counts: list[dict] | None,
    watchlist_reason_counts: list[dict] | None,
    candidate_pool_reduction: dict | None,
    data_completeness_filtered: int = 0,
    autostock_filtered: int = 0,
    drawdown_duration_distribution: dict | None = None,
    mainlist_selection_mode: str = 'fixed_top_n',
    mainlist_selection_anchor_top_n: int | None = None,
    mainlist_score_band_delta: float = 0.0,
    mainlist_selection_expanded_count: int = 0,
    bond_subtype_edge_governance: dict | None = None,
) -> dict:
    """构建最终筛选面向用户的解释摘要。"""
    bond_subtype_edge_governance = bond_subtype_edge_governance or {}
    category_summary = format_summary_count_items(
        final_top_category_counts,
        label_key='category',
        count_key='count',
        limit=4,
    )
    final_top_category_counts = final_top_category_counts or []
    resonance_count = int(intersection_count or 0) + int(soft_intersection_count or 0)
    single_side_count = int(only_short_count or 0) + int(only_long_count or 0)
    resonance_main_list_count = sum(
        int(item.get('count', 0) or 0)
        for item in final_top_category_counts
        if str(item.get('category', '')).strip() in {'交集优选', '邻近交集'}
    )
    intersection_note = (
        '这里的交集/邻近交集表示同时进入近期与长期两个视角，不自动等于双维度都处于高分段'
        if resonance_count > 0
        else ''
    )
    main_list_parts = []
    if final_output_count > 0:
        main_list_parts.append(f"当前主榜共 {int(final_output_count)} 只")
        if category_summary:
            main_list_parts.append(f"来源结构为 {category_summary}")
        if bool(bond_subtype_edge_governance.get('applied')):
            main_list_parts.append(
                f"为缓解单一子类型过度集中，主榜边缘已执行 {int(bond_subtype_edge_governance.get('replacement_count', 0) or 0)} 只小规模替换"
            )
        if resonance_main_list_count > 0:
            resonance_ratio = round(
                (resonance_main_list_count / max(int(final_output_count), 1)) * 100
            )
            main_list_parts.append(
                f"主榜中双周期共振来源共 {resonance_main_list_count} 只，占主榜约 {resonance_ratio}%"
            )
            if resonance_count > resonance_main_list_count:
                main_list_parts.append(
                    f"候选池内共有 {resonance_count} 只交集/邻近交集基金参与竞争"
                )
            if single_side_count > 0:
                main_list_parts.append('单边候选更多被保留在观察池，主榜仍优先保留双周期共振更强的基金')
        elif resonance_count > 0:
            main_list_parts.append(f"候选池内共有 {resonance_count} 只交集/邻近交集基金参与竞争")
            if single_side_count > 0:
                main_list_parts.append('当前主榜未完全由双周期共振来源构成，单边强势基金仍占有一定席位')
        elif single_side_count > 0:
            main_list_parts.append('当前主榜主要由单边强势基金构成，需继续跟踪双周期共振是否形成')
        if intersection_note:
            main_list_parts.append(intersection_note)
    else:
        main_list_parts.append('当前尚未形成可输出的最终主榜')
    main_list_summary = '；'.join(main_list_parts) + '。'

    watchlist_reason_counts = watchlist_reason_counts or []
    diagnostic_reasons = [
        item for item in watchlist_reason_counts
        if not is_watchlist_action_hint(item.get('reason'))
    ]
    action_hints = [
        item for item in watchlist_reason_counts
        if is_watchlist_action_hint(item.get('reason'))
    ]
    diagnostic_summary = format_summary_count_items(
        diagnostic_reasons,
        label_key='reason',
        count_key='count',
        limit=3,
    )
    action_summary = format_summary_count_items(
        action_hints,
        label_key='reason',
        count_key='count',
        limit=2,
        label_transform=lambda label: label.replace('操作提示：', '', 1),
    )
    if diagnostic_summary and action_summary:
        watchlist_summary = (
            f"观察池未进主榜主要因为 {diagnostic_summary}；后续跟踪动作以 {action_summary} 为主。"
        )
    elif diagnostic_summary:
        watchlist_summary = f"观察池未进主榜主要因为 {diagnostic_summary}。"
    elif action_summary:
        watchlist_summary = f"观察池当前以 {action_summary} 这类跟踪动作作为后续重点。"
    else:
        watchlist_summary = '当前观察池为空，或暂未形成稳定的未进主榜原因归因。'

    reduction = candidate_pool_reduction or {}
    reduction_parts = []
    holding_filtered = int(reduction.get('holding_period_filtered', 0) or 0)
    share_collapsed = int(reduction.get('share_class_collapsed', 0) or 0)
    if holding_filtered > 0:
        reduction_parts.append(f"定开/持有期过滤 {holding_filtered} 条")
    if share_collapsed > 0:
        reduction_parts.append(f"同母基金份额合并 {share_collapsed} 条")

    candidate_pool_parts = [
        f"并集候选 {int(union_candidate_count)} 只，压缩到主榜前候选 {int(eligible_pool_count)} 只",
    ]
    if dedup_or_filtered_removed > 0:
        candidate_pool_parts.append(f"其中去重/过滤移除 {int(dedup_or_filtered_removed)} 条")
    if str(mainlist_selection_mode).strip() == 'score_band_topn_anchor':
        anchor_top_n = int(mainlist_selection_anchor_top_n or final_output_count or 0)
        candidate_pool_parts.append(
            f"正式主榜以锚点规模 {anchor_top_n} 只的末位分数为锚点，并按 {float(mainlist_score_band_delta):.2f} 分分数带扩容"
        )
        if int(mainlist_selection_expanded_count or 0) > 0:
            candidate_pool_parts.append(
                f"分数带额外吸收 {int(mainlist_selection_expanded_count)} 只临界高分基金"
            )
    if top_n_trimmed > 0 and str(mainlist_selection_mode).strip() == 'score_band_topn_anchor':
        candidate_pool_parts.append(f"扩容后仍有 {int(top_n_trimmed)} 只位于锚点分数带之外")
    elif top_n_trimmed > 0:
        candidate_pool_parts.append(f"再经主榜锚点截断淘汰 {int(top_n_trimmed)} 只")
    if bool(bond_subtype_edge_governance.get('applied')):
        candidate_pool_parts.append(
            f"另外对主榜末位执行了 {int(bond_subtype_edge_governance.get('replacement_count', 0) or 0)} 只同分段边缘替换，用于缓解长债过度集中"
        )
    if int(data_completeness_filtered or 0) > 0:
        candidate_pool_parts.append(
            f"另有 {int(data_completeness_filtered)} 只因关键指标不完整未进入投资主榜"
        )
    if int(autostock_filtered or 0) > 0:
        candidate_pool_parts.append(
            f"另有 {int(autostock_filtered)} 只因双源数据交叉验证未通过被拦截"
        )
    drawdown_duration_distribution = drawdown_duration_distribution or {}
    if drawdown_duration_distribution:
        median_duration = drawdown_duration_distribution.get('median')
        p90_duration = drawdown_duration_distribution.get('p90')
        if median_duration is not None and p90_duration is not None:
            candidate_pool_parts.append(
                f"候选池1年最长回撤观测数中位数约 {median_duration} 次，P90 约 {p90_duration} 次（含未恢复区间）"
            )
    if reduction_parts:
        candidate_pool_parts.append(f"压缩主要来自 {'、'.join(reduction_parts)}")
    if str(fund_type).strip().lower() == 'bond' and holding_filtered > 0:
        candidate_pool_parts.append('债券链的持有期过滤属于可交易性约束，不是额外装饰规则')
    candidate_pool_summary = '；'.join(candidate_pool_parts) + '。'

    strategy_validation_note = build_strategy_validation_note(fund_type)
    headline_highlights = [
        main_list_summary,
        watchlist_summary,
        candidate_pool_summary,
        strategy_validation_note,
    ]

    return {
        'narrative_summary': {
            'main_list': main_list_summary,
            'watchlist': watchlist_summary,
            'candidate_pool': candidate_pool_summary,
            'strategy_note': strategy_validation_note,
        },
        'strategy_validation_note': strategy_validation_note,
        'headline_highlights': headline_highlights,
    }


def export_single_side_watchlist(
    df_watchlist: pd.DataFrame,
    *,
    output_dir: Path,
    timestamp: str,
    filename_prefix: str,
) -> Path | None:
    """导出单边候选观察池 CSV，保留解释所需的份额聚合信息。"""
    if df_watchlist.empty:
        return None

    export_df = df_watchlist.copy()
    if 'fund_name' not in export_df.columns:
        export_df['fund_name'] = ''
    if 'base_fund_name' not in export_df.columns:
        export_df['base_fund_name'] = export_df['fund_name']
    if 'share_class_count' not in export_df.columns:
        export_df['share_class_count'] = 1
    if 'share_class_names' not in export_df.columns:
        export_df['share_class_names'] = export_df['fund_name']
    if 'share_class_codes' not in export_df.columns:
        export_df['share_class_codes'] = export_df.get('fcode', '')
    if 'short_score' not in export_df.columns:
        export_df['short_score'] = None
    if 'long_score' not in export_df.columns:
        export_df['long_score'] = None
    if 'weighted_score' not in export_df.columns:
        export_df['weighted_score'] = None
    if 'fund_category' not in export_df.columns:
        export_df['fund_category'] = ''

    export_df['share_class_count'] = export_df['share_class_count'].fillna(1).astype(int)
    export_df = export_df.rename(columns={
        'fcode': '代码',
        'fund_name': '名称',
        'weighted_score': '观察得分',
        'fund_category': '候选类别',
        '名单层级': '名单层级',
        '推荐状态': '推荐状态',
        '观察原因代码': '观察原因代码',
        'short_score': '近期得分',
        'long_score': '长期得分',
        'base_fund_name': '母基金名称',
        'share_class_count': '关联份额数',
        'share_class_names': '关联份额名称',
        'share_class_codes': '关联份额代码',
    })
    export_df = export_df[
        [
            '代码',
            '名称',
            '观察得分',
            '候选类别',
            '名单层级',
            '推荐状态',
            '观察原因代码',
            '主榜状态',
            '主榜位次',
            '主榜末位得分',
            '距主榜末位差距',
            '未进主榜原因',
            '近期得分',
            '长期得分',
            '母基金名称',
            '关联份额数',
            '关联份额名称',
            '关联份额代码',
        ]
    ].copy()

    output_file = output_dir / f"{filename_prefix}_{timestamp}.csv"
    export_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    return output_file
