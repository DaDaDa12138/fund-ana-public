#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选主榜阶段编排辅助工具。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


_AUTOSTOCK_FAILED_EXPORT_COLUMNS = ['基金代码', '基金名称', '失败原因', '告警原因']
_MAINLIST_DRAWDOWN_COLUMNS = ('1年-最长回撤观测数', '1年-最大回撤(重算)')
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


@dataclass(slots=True)
class MainlistQualityGateResult:
    """最终主榜质量门禁阶段结果。"""

    incomplete_fcodes: set[str]
    completeness_summary: dict
    incomplete_candidates_df: pd.DataFrame
    df_weighted_for_mainlist: pd.DataFrame
    df_weighted_selected_pool_prevalidation: pd.DataFrame
    candidate_pool_reduction: dict
    prevalidation_selected_pool_count: int
    autostock_failed_fcodes: set[str]
    autostock_validation_summary: dict
    autostock_failed_candidates_df: pd.DataFrame
    autostock_validation_detail_df: pd.DataFrame
    autostock_validation_status_map: dict[str, dict]
    drawdown_duration_summary: dict


@dataclass(slots=True)
class MainlistSelectionResult:
    """最终主榜选择与观察池阶段结果。"""

    df_weighted_selected_pool: pd.DataFrame
    df_weighted_top: pd.DataFrame
    selection_summary: dict
    final_screening_summary: dict
    short_watchlist_csv: Path | None
    long_watchlist_csv: Path | None
    final_screening_decision_records: list[dict]


def _resolve_runtime_ftype_column(df: pd.DataFrame) -> str | None:
    """解析当前主榜/候选池中可用的基金类型列。"""
    for column in ('基金类型', 'ftype', 'fund_type'):
        if column in df.columns:
            return column
    return None


def select_mainlist_by_policy(
    df_weighted_selected_pool: pd.DataFrame,
    *,
    fund_type: str,
    mainlist_anchor_limit: int | None,
    mainlist_selection_mode: str = 'fixed_top_n',
    mainlist_score_band_delta: float = 0.0,
) -> tuple[pd.DataFrame, dict]:
    """按主榜选择策略生成最终正式主榜。"""
    if not isinstance(df_weighted_selected_pool, pd.DataFrame) or df_weighted_selected_pool.empty:
        return df_weighted_selected_pool.head(0).copy(), {
            'selection_mode': str(mainlist_selection_mode or 'fixed_top_n').strip() or 'fixed_top_n',
            'selection_anchor_top_n': max(int(mainlist_anchor_limit or 0), 0),
            'selection_score_band_delta': float(mainlist_score_band_delta or 0.0),
            'selection_cutoff_score': None,
            'selection_expanded_count': 0,
        }

    normalized_fund_type = str(fund_type or '').strip().lower()
    normalized_mode = str(mainlist_selection_mode or 'fixed_top_n').strip() or 'fixed_top_n'

    if normalized_mode == 'all_eligible':
        selected = df_weighted_selected_pool.copy()
        selected = selected.sort_values(
            by=['weighted_score', 'fcode'],
            ascending=[False, True],
            kind='mergesort',
        ).reset_index(drop=True)
        return selected, {
            'selection_mode': normalized_mode,
            'selection_anchor_top_n': int(len(selected)),
            'selection_score_band_delta': 0.0,
            'selection_cutoff_score': (
                round(float(selected['weighted_score'].iloc[-1]), 4)
                if not selected.empty
                else None
            ),
            'selection_expanded_count': 0,
        }

    anchor_count = min(max(int(mainlist_anchor_limit or 0), 0), len(df_weighted_selected_pool))
    if anchor_count <= 0:
        return df_weighted_selected_pool.head(0).copy(), {
            'selection_mode': normalized_mode,
            'selection_anchor_top_n': 0,
            'selection_score_band_delta': float(mainlist_score_band_delta or 0.0),
            'selection_cutoff_score': None,
            'selection_expanded_count': 0,
        }

    anchor_df = df_weighted_selected_pool.head(anchor_count).copy()
    cutoff_score = float(anchor_df['weighted_score'].iloc[-1])
    score_band_delta = max(float(mainlist_score_band_delta or 0.0), 0.0)

    if normalized_fund_type == 'bond' and normalized_mode == 'score_band_topn_anchor' and score_band_delta > 0:
        floor_score = cutoff_score - score_band_delta
        selected = df_weighted_selected_pool[
            df_weighted_selected_pool['weighted_score'].astype(float) >= floor_score
        ].copy()
    else:
        selected = anchor_df
        normalized_mode = 'fixed_top_n'

    return selected, {
        'selection_mode': normalized_mode,
        'selection_anchor_top_n': int(anchor_count),
        'selection_score_band_delta': score_band_delta,
        'selection_cutoff_score': round(cutoff_score, 4),
        'selection_expanded_count': max(int(len(selected) - len(anchor_df)), 0),
    }


def apply_bond_subtype_edge_governance(
    df_weighted_selected_pool: pd.DataFrame,
    df_weighted_top: pd.DataFrame,
    *,
    fund_type: str,
    autostock_validation_status_map: dict[str, dict] | None,
    enabled: bool,
    dominant_share_warn_pct: float,
    max_replacements: int,
    score_gap_tolerance: float,
) -> tuple[pd.DataFrame, dict]:
    """在债基正式主榜边缘做极小规模的子类型平衡替换。"""
    meta = {
        'enabled': bool(enabled),
        'applied': False,
        'reason': 'disabled',
        'ftype_column': None,
        'dominant_ftype': None,
        'dominant_ftype_share_pct': None,
        'warn_share_pct': float(dominant_share_warn_pct or 0.0),
        'max_replacements': int(max_replacements or 0),
        'score_gap_tolerance': float(score_gap_tolerance or 0.0),
        'replacement_count': 0,
        'rows': [],
    }
    normalized_fund_type = str(fund_type or '').strip().lower()
    autostock_validation_status_map = autostock_validation_status_map or {}
    if not enabled or normalized_fund_type != 'bond':
        meta['reason'] = 'not_applicable'
        return df_weighted_top.copy(), meta
    if not isinstance(df_weighted_top, pd.DataFrame) or df_weighted_top.empty:
        meta['reason'] = 'empty_mainlist'
        return df_weighted_top.copy(), meta
    if not isinstance(df_weighted_selected_pool, pd.DataFrame) or df_weighted_selected_pool.empty:
        meta['reason'] = 'empty_selected_pool'
        return df_weighted_top.copy(), meta

    ftype_col = _resolve_runtime_ftype_column(df_weighted_top)
    if ftype_col is None or ftype_col not in df_weighted_selected_pool.columns:
        meta['reason'] = 'missing_ftype_column'
        return df_weighted_top.copy(), meta
    meta['ftype_column'] = ftype_col

    ftype_counter = Counter(
        str(value or '').strip() or '<unknown>'
        for value in df_weighted_top[ftype_col].tolist()
    )
    if not ftype_counter:
        meta['reason'] = 'empty_ftype_distribution'
        return df_weighted_top.copy(), meta

    dominant_ftype, dominant_count = ftype_counter.most_common(1)[0]
    dominant_share_pct = round(dominant_count / len(df_weighted_top) * 100, 2)
    meta['dominant_ftype'] = dominant_ftype
    meta['dominant_ftype_share_pct'] = dominant_share_pct
    if dominant_share_pct < float(dominant_share_warn_pct or 0.0):
        meta['reason'] = 'dominant_share_below_warn_threshold'
        return df_weighted_top.copy(), meta
    if int(max_replacements or 0) <= 0:
        meta['reason'] = 'zero_replacement_budget'
        return df_weighted_top.copy(), meta

    current_top_codes = set(df_weighted_top['fcode'].astype(str).tolist())
    outside_candidates = df_weighted_selected_pool[
        ~df_weighted_selected_pool['fcode'].astype(str).isin(current_top_codes)
    ].copy()
    if outside_candidates.empty:
        meta['reason'] = 'no_outside_candidates'
        return df_weighted_top.copy(), meta

    replacement_pool = outside_candidates[
        outside_candidates[ftype_col].astype(str).str.strip() != str(dominant_ftype).strip()
    ].copy()
    if replacement_pool.empty:
        meta['reason'] = 'no_non_dominant_candidates'
        return df_weighted_top.copy(), meta

    if 'autostock_validation_status' in replacement_pool.columns:
        replacement_pool = replacement_pool[
            replacement_pool['autostock_validation_status'].astype(str) == 'strict_match'
        ].copy()
    else:
        replacement_pool = replacement_pool[
            replacement_pool['fcode'].astype(str).map(
                lambda code: str(
                    autostock_validation_status_map.get(str(code).strip(), {}).get('autostock_validation_status') or ''
                ).strip() == 'strict_match'
            )
        ].copy()
    if replacement_pool.empty:
        meta['reason'] = 'no_strict_match_replacements'
        return df_weighted_top.copy(), meta

    replacement_pool = replacement_pool.sort_values(
        by=['weighted_score', 'fcode'],
        ascending=[False, True],
    ).reset_index(drop=True)

    governed = df_weighted_top.copy()
    governed = governed.sort_values(by=['weighted_score', 'fcode'], ascending=[False, True]).reset_index(drop=True)
    candidate_rows = governed[
        governed[ftype_col].astype(str).str.strip() == str(dominant_ftype).strip()
    ].sort_values(by=['weighted_score', 'fcode'], ascending=[True, True]).reset_index(drop=True)
    if candidate_rows.empty:
        meta['reason'] = 'no_dominant_rows_to_replace'
        return governed, meta

    replacement_rows = []
    used_in_codes: set[str] = set()
    used_out_codes: set[str] = set()
    tolerance = float(score_gap_tolerance or 0.0)

    for _, out_row in candidate_rows.iterrows():
        if len(replacement_rows) >= int(max_replacements):
            break
        out_code = str(out_row.get('fcode') or '').strip()
        out_score = float(out_row.get('weighted_score') or 0.0)
        for _, in_row in replacement_pool.iterrows():
            in_code = str(in_row.get('fcode') or '').strip()
            if not in_code or in_code in used_in_codes or out_code in used_out_codes:
                continue
            in_score = float(in_row.get('weighted_score') or 0.0)
            score_gap = round(out_score - in_score, 4)
            if score_gap > tolerance:
                continue
            replacement_rows.append({
                'out_fcode': out_code,
                'out_fund_name': out_row.get('fund_name'),
                'out_ftype': out_row.get(ftype_col),
                'out_weighted_score': out_score,
                'in_fcode': in_code,
                'in_fund_name': in_row.get('fund_name'),
                'in_ftype': in_row.get(ftype_col),
                'in_weighted_score': in_score,
                'score_gap': score_gap,
            })
            used_out_codes.add(out_code)
            used_in_codes.add(in_code)
            break

    if not replacement_rows:
        meta['reason'] = 'no_replacements_within_score_gap'
        return governed, meta

    governed = governed[
        ~governed['fcode'].astype(str).isin(used_out_codes)
    ].copy()
    governed = pd.concat(
        [
            governed,
            replacement_pool[
                replacement_pool['fcode'].astype(str).isin(used_in_codes)
            ].copy(),
        ],
        ignore_index=True,
    )
    governed = governed.sort_values(by=['weighted_score', 'fcode'], ascending=[False, True]).reset_index(drop=True)

    meta['applied'] = True
    meta['reason'] = 'replaced_dominant_edge_rows'
    meta['replacement_count'] = len(replacement_rows)
    meta['rows'] = replacement_rows
    return governed, meta


@dataclass(slots=True)
class MainlistPipelineResult:
    """最终主榜完整编排结果。"""

    quality_gate: MainlistQualityGateResult
    selection: MainlistSelectionResult
    final_export: dict


def run_mainlist_quality_gate_stage(
    *,
    df_weighted: pd.DataFrame,
    fund_type: str,
    final_policy: dict,
    autostock_enabled: bool,
    normalize_fcode_fn,
    filter_final_candidates_by_data_completeness_fn,
    summarize_candidate_pool_reduction_fn,
    build_autostock_validation_summary_fn,
    cross_validate_candidate_pool_with_autostock_fn,
    build_autostock_validation_status_map_fn,
    analyze_candidate_pool_drawdown_duration_fn,
    build_drawdown_duration_summary_fn,
    print_completeness_summary_fn,
    print_autostock_validation_summary_fn,
    print_autostock_validation_skipped_fn,
    print_drawdown_duration_summary_fn,
) -> MainlistQualityGateResult:
    """执行最终主榜的完整性校验、AutoStock 校验与回撤持续期分析。"""
    incomplete_fcodes, completeness_summary, incomplete_candidates_df, complete_candidates_df = (
        filter_final_candidates_by_data_completeness_fn(df_weighted)
    )
    print_completeness_summary_fn(completeness_summary, incomplete_fcodes)

    df_weighted_for_mainlist = df_weighted
    if incomplete_fcodes:
        df_weighted_for_mainlist = df_weighted[
            ~df_weighted['fcode'].isin(incomplete_fcodes)
        ].copy()

    df_weighted_selected_pool_prevalidation, candidate_pool_reduction = (
        summarize_candidate_pool_reduction_fn(
            df_weighted_for_mainlist,
            fund_type=fund_type,
            collapse_share_classes_enabled=True,
        )
    )
    prevalidation_selected_pool_count = len(df_weighted_selected_pool_prevalidation)

    autostock_failed_fcodes = set()
    autostock_validation_summary = build_autostock_validation_summary_fn(
        reason='not_applicable'
    )
    autostock_failed_candidates_df = pd.DataFrame(
        columns=_AUTOSTOCK_FAILED_EXPORT_COLUMNS
    )
    autostock_validation_detail_df = pd.DataFrame(
        columns=_AUTOSTOCK_FAILED_EXPORT_COLUMNS
    )
    autostock_validation_status_map: dict[str, dict] = {}

    if autostock_enabled:
        if completeness_summary.get('enabled'):
            selected_pool_codes = set(
                df_weighted_selected_pool_prevalidation['fcode'].tolist()
            )
            df_autostock_source = (
                complete_candidates_df[
                    complete_candidates_df['基金代码'].map(normalize_fcode_fn).isin(
                        selected_pool_codes
                    )
                ].copy()
                if not complete_candidates_df.empty
                else pd.DataFrame()
            )

            if df_autostock_source.empty:
                raise RuntimeError(
                    '最终候选池缺少 AutoStock 校验所需的本地完整信息，已阻断最终主榜生成'
                )

            (
                validated_candidates,
                autostock_failed_fcodes,
                autostock_validation_summary,
                autostock_failed_candidates_df,
            ) = cross_validate_candidate_pool_with_autostock_fn(
                df_weighted_selected_pool_prevalidation,
                df_local_complete=df_autostock_source,
            )
            df_weighted_for_mainlist = validated_candidates
            autostock_validation_detail_df = pd.DataFrame(
                autostock_validation_summary.pop('_detail_records', []) or []
            )
            autostock_validation_status_map = (
                build_autostock_validation_status_map_fn(
                    autostock_validation_detail_df,
                    failed_codes=autostock_failed_fcodes,
                )
            )

            print_autostock_validation_summary_fn(
                autostock_validation_summary,
                autostock_failed_fcodes,
            )
        else:
            raise RuntimeError(
                '关键指标完整性校验未成功，无法继续执行最终主榜的 AutoStock 双源验证，已阻断输出'
            )
    else:
        print_autostock_validation_skipped_fn(fund_type)

    drawdown_duration_summary = build_drawdown_duration_summary_fn(
        reason='not_applicable'
    )
    if fund_type == 'bond':
        df_weighted_for_mainlist, drawdown_duration_summary = (
            analyze_candidate_pool_drawdown_duration_fn(
                df_weighted_for_mainlist,
                analysis_window_days=int(
                    final_policy.get(
                        'drawdown_duration_analysis_window_days',
                        365,
                    ) or 365
                ),
                nav_buffer_days=int(
                    final_policy.get('drawdown_duration_nav_buffer_days', 30) or 30
                ),
            )
        )
        print_drawdown_duration_summary_fn(drawdown_duration_summary)

    for metric_col in _MAINLIST_DRAWDOWN_COLUMNS:
        if metric_col not in df_weighted_for_mainlist.columns:
            df_weighted_for_mainlist[metric_col] = None

    return MainlistQualityGateResult(
        incomplete_fcodes=incomplete_fcodes,
        completeness_summary=completeness_summary,
        incomplete_candidates_df=incomplete_candidates_df,
        df_weighted_for_mainlist=df_weighted_for_mainlist,
        df_weighted_selected_pool_prevalidation=df_weighted_selected_pool_prevalidation,
        candidate_pool_reduction=candidate_pool_reduction,
        prevalidation_selected_pool_count=prevalidation_selected_pool_count,
        autostock_failed_fcodes=autostock_failed_fcodes,
        autostock_validation_summary=autostock_validation_summary,
        autostock_failed_candidates_df=autostock_failed_candidates_df,
        autostock_validation_detail_df=autostock_validation_detail_df,
        autostock_validation_status_map=autostock_validation_status_map,
        drawdown_duration_summary=drawdown_duration_summary,
    )


def run_mainlist_selection_stage(
    *,
    df_weighted: pd.DataFrame,
    fund_type: str,
    final_policy: dict,
    mainlist_anchor_limit: int | None,
    single_side_watchlist_limit: int,
    candidate_summary: dict,
    raw_intersection_count: int,
    incomplete_fcodes: set[str],
    completeness_summary: dict,
    incomplete_candidates_df: pd.DataFrame,
    df_weighted_selected_pool_prevalidation: pd.DataFrame,
    df_weighted_for_mainlist: pd.DataFrame,
    candidate_pool_reduction: dict,
    autostock_failed_fcodes: set[str],
    autostock_validation_summary: dict,
    autostock_failed_candidates_df: pd.DataFrame,
    autostock_validation_status_map: dict[str, dict],
    drawdown_duration_summary: dict,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict,
    cross_long_period_counts: dict,
    cross_short_base_scores: dict,
    cross_long_base_scores: dict,
    cross_short_base_period_counts: dict,
    cross_long_base_period_counts: dict,
    output_dir: Path,
    timestamp: str,
    output_prefix: str | None,
    build_final_selection_summary_fn,
    build_watchlist_bundle_fn,
    export_single_side_watchlist_fn,
    build_final_output_name_fn,
    print_weighted_union_statistics_fn,
    print_watchlist_exports_fn,
) -> MainlistSelectionResult:
    """执行最终主榜截断、观察池生成与导出前摘要装配。"""
    df_weighted_selected_pool = df_weighted_for_mainlist.copy()
    df_weighted_top, mainlist_selection_meta = select_mainlist_by_policy(
        df_weighted_selected_pool,
        fund_type=fund_type,
        mainlist_anchor_limit=mainlist_anchor_limit,
        mainlist_selection_mode=str(final_policy.get('mainlist_selection_mode', 'fixed_top_n') or 'fixed_top_n'),
        mainlist_score_band_delta=float(final_policy.get('mainlist_score_band_delta', 0.0) or 0.0),
    )
    df_weighted_top, bond_subtype_edge_governance = apply_bond_subtype_edge_governance(
        df_weighted_selected_pool,
        df_weighted_top,
        fund_type=fund_type,
        autostock_validation_status_map=autostock_validation_status_map,
        enabled=bool(final_policy.get('bond_subtype_edge_governance_enabled', False)),
        dominant_share_warn_pct=float(final_policy.get('bond_subtype_edge_warn_share_pct', 0.0) or 0.0),
        max_replacements=int(final_policy.get('bond_subtype_edge_max_replacements', 0) or 0),
        score_gap_tolerance=float(final_policy.get('bond_subtype_edge_score_gap_tolerance', 0.0) or 0.0),
    )

    selection_summary = build_final_selection_summary_fn(
        df_weighted=df_weighted,
        df_weighted_selected_pool=df_weighted_selected_pool,
        df_weighted_top=df_weighted_top,
        candidate_summary=candidate_summary,
        completeness_summary=completeness_summary,
        incomplete_fcodes=incomplete_fcodes,
        autostock_validation_summary=autostock_validation_summary,
        autostock_failed_fcodes=autostock_failed_fcodes,
        autostock_validation_status_map=autostock_validation_status_map,
        drawdown_duration_summary=drawdown_duration_summary,
        candidate_pool_reduction=candidate_pool_reduction,
        single_side_watchlist_limit=single_side_watchlist_limit,
        raw_intersection_count=raw_intersection_count,
        fund_type=fund_type,
        mainlist_selection_meta=mainlist_selection_meta,
        bond_subtype_edge_governance=bond_subtype_edge_governance,
    )
    print_weighted_union_statistics_fn(
        candidate_summary=candidate_summary,
        incomplete_fcodes=incomplete_fcodes,
        autostock_failed_fcodes=autostock_failed_fcodes,
        prevalidation_selected_pool_count=len(df_weighted_selected_pool_prevalidation),
        dedup_or_filtered_removed=selection_summary['dedup_or_filtered_removed'],
        top_n_trimmed=selection_summary['top_n_trimmed'],
        top_n_final=selection_summary['top_n_final'],
        candidate_pool_reduction=candidate_pool_reduction,
        top_category_counts=selection_summary['top_category_counts'],
        mainlist_selection_mode=selection_summary['mainlist_selection_mode'],
        mainlist_score_band_delta=float(selection_summary.get('mainlist_score_band_delta', 0.0) or 0.0),
    )

    final_screening_summary = dict(selection_summary['final_screening_summary'])

    watchlist_excluded_fcodes = set(incomplete_fcodes) | set(autostock_failed_fcodes)
    watchlist_bundle = build_watchlist_bundle_fn(
        df_weighted=df_weighted,
        fund_type=fund_type,
        single_side_watchlist_limit=single_side_watchlist_limit,
        watchlist_excluded_fcodes=watchlist_excluded_fcodes,
        df_weighted_selected_pool=df_weighted_selected_pool,
        df_weighted_top=df_weighted_top,
        candidate_summary=candidate_summary,
        top_cutoff_score=selection_summary['top_cutoff_score'],
        incomplete_candidates_df=incomplete_candidates_df,
        autostock_failed_candidates_df=autostock_failed_candidates_df,
        autostock_validation_status_map=autostock_validation_status_map,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        candidate_pool_reduction=candidate_pool_reduction,
        final_screening_summary=final_screening_summary,
        df_weighted_selected_pool_prevalidation=df_weighted_selected_pool_prevalidation,
    )
    short_watchlist = watchlist_bundle['short_watchlist']
    long_watchlist = watchlist_bundle['long_watchlist']
    final_screening_summary.update(watchlist_bundle['summary_updates'])

    short_watchlist_csv = export_single_side_watchlist_fn(
        short_watchlist,
        output_dir=output_dir,
        timestamp=timestamp,
        filename_prefix=build_final_output_name_fn(
            f"仅近期优质观察池_{single_side_watchlist_limit}只",
            output_prefix=output_prefix,
        ),
    )
    long_watchlist_csv = export_single_side_watchlist_fn(
        long_watchlist,
        output_dir=output_dir,
        timestamp=timestamp,
        filename_prefix=build_final_output_name_fn(
            f"仅长期优质观察池_{single_side_watchlist_limit}只",
            output_prefix=output_prefix,
        ),
    )
    print_watchlist_exports_fn(
        short_watchlist_csv=short_watchlist_csv,
        short_watchlist_count=len(short_watchlist),
        long_watchlist_csv=long_watchlist_csv,
        long_watchlist_count=len(long_watchlist),
    )

    return MainlistSelectionResult(
        df_weighted_selected_pool=df_weighted_selected_pool,
        df_weighted_top=df_weighted_top,
        selection_summary=selection_summary,
        final_screening_summary=final_screening_summary,
        short_watchlist_csv=short_watchlist_csv,
        long_watchlist_csv=long_watchlist_csv,
        final_screening_decision_records=watchlist_bundle[
            'final_screening_decision_records'
        ],
    )


def run_mainlist_pipeline(
    *,
    final_fund_file: Path,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    df_weighted: pd.DataFrame,
    fund_type: str,
    final_policy: dict,
    mainlist_anchor_limit: int | None,
    single_side_watchlist_limit: int,
    candidate_summary: dict,
    raw_intersection_count: int,
    autostock_enabled: bool,
    autostock_validation_status_map: dict[str, dict] | None,
    short_report_date: str | None,
    long_report_date: str | None,
    input_pair_status_label: str,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict,
    cross_long_period_counts: dict,
    cross_short_base_scores: dict,
    cross_long_base_scores: dict,
    cross_short_base_period_counts: dict,
    cross_long_base_period_counts: dict,
    output_dir: Path,
    timestamp: str,
    output_prefix: str | None,
    normalize_fcode_fn,
    filter_final_candidates_by_data_completeness_fn,
    summarize_candidate_pool_reduction_fn,
    build_autostock_validation_summary_fn,
    cross_validate_candidate_pool_with_autostock_fn,
    build_autostock_validation_status_map_fn,
    analyze_candidate_pool_drawdown_duration_fn,
    build_drawdown_duration_summary_fn,
    build_final_selection_summary_fn,
    build_bond_mainlist_structure_audit_fn,
    build_bond_fee_semantics_audit_fn,
    build_watchlist_bundle_fn,
    export_single_side_watchlist_fn,
    build_final_output_name_fn,
    get_fund_complete_info_fn,
    load_sales_fee_validation_classification_map_fn,
    sort_weighted_candidates_fn,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
    fund_type_label_map: dict,
    write_final_fund_workbook_fn,
    print_completeness_summary_fn,
    print_autostock_validation_summary_fn,
    print_autostock_validation_skipped_fn,
    print_drawdown_duration_summary_fn,
    print_weighted_union_statistics_fn,
    print_watchlist_exports_fn,
    print_complete_info_fetch_start_fn,
    print_complete_info_fetch_result_fn,
    print_holding_period_filter_status_fn,
    print_export_cleanup_summary_fn,
    print_final_fund_workbook_written_fn,
) -> MainlistPipelineResult:
    """串联最终主榜质量门禁、主榜选择与 Excel 导出。"""
    quality_gate = run_mainlist_quality_gate_stage(
        df_weighted=df_weighted,
        fund_type=fund_type,
        final_policy=final_policy,
        autostock_enabled=autostock_enabled,
        normalize_fcode_fn=normalize_fcode_fn,
        filter_final_candidates_by_data_completeness_fn=(
            filter_final_candidates_by_data_completeness_fn
        ),
        summarize_candidate_pool_reduction_fn=summarize_candidate_pool_reduction_fn,
        build_autostock_validation_summary_fn=build_autostock_validation_summary_fn,
        cross_validate_candidate_pool_with_autostock_fn=(
            cross_validate_candidate_pool_with_autostock_fn
        ),
        build_autostock_validation_status_map_fn=(
            build_autostock_validation_status_map_fn
        ),
        analyze_candidate_pool_drawdown_duration_fn=(
            analyze_candidate_pool_drawdown_duration_fn
        ),
        build_drawdown_duration_summary_fn=build_drawdown_duration_summary_fn,
        print_completeness_summary_fn=print_completeness_summary_fn,
        print_autostock_validation_summary_fn=(
            print_autostock_validation_summary_fn
        ),
        print_autostock_validation_skipped_fn=(
            print_autostock_validation_skipped_fn
        ),
        print_drawdown_duration_summary_fn=print_drawdown_duration_summary_fn,
    )

    selection = run_mainlist_selection_stage(
        df_weighted=df_weighted,
        fund_type=fund_type,
        final_policy=final_policy,
        mainlist_anchor_limit=mainlist_anchor_limit,
        single_side_watchlist_limit=single_side_watchlist_limit,
        candidate_summary=candidate_summary,
        raw_intersection_count=raw_intersection_count,
        incomplete_fcodes=quality_gate.incomplete_fcodes,
        completeness_summary=quality_gate.completeness_summary,
        incomplete_candidates_df=quality_gate.incomplete_candidates_df,
        df_weighted_selected_pool_prevalidation=(
            quality_gate.df_weighted_selected_pool_prevalidation
        ),
        df_weighted_for_mainlist=quality_gate.df_weighted_for_mainlist,
        candidate_pool_reduction=quality_gate.candidate_pool_reduction,
        autostock_failed_fcodes=quality_gate.autostock_failed_fcodes,
        autostock_validation_summary=quality_gate.autostock_validation_summary,
        autostock_failed_candidates_df=quality_gate.autostock_failed_candidates_df,
        autostock_validation_status_map=(
            quality_gate.autostock_validation_status_map
        ),
        drawdown_duration_summary=quality_gate.drawdown_duration_summary,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        output_dir=output_dir,
        timestamp=timestamp,
        output_prefix=output_prefix,
        build_final_selection_summary_fn=build_final_selection_summary_fn,
        build_watchlist_bundle_fn=build_watchlist_bundle_fn,
        export_single_side_watchlist_fn=export_single_side_watchlist_fn,
        build_final_output_name_fn=build_final_output_name_fn,
        print_weighted_union_statistics_fn=print_weighted_union_statistics_fn,
        print_watchlist_exports_fn=print_watchlist_exports_fn,
    )

    final_export = run_final_fund_export_stage(
        final_fund_file=final_fund_file,
        df_short=df_short,
        df_long=df_long,
        df_weighted_top=selection.df_weighted_top,
        fund_type=fund_type,
        autostock_enabled=autostock_enabled,
        autostock_validation_status_map=(
            autostock_validation_status_map
            if autostock_validation_status_map is not None
            else quality_gate.autostock_validation_status_map
        ),
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        input_pair_status_label=input_pair_status_label,
        get_fund_complete_info_fn=get_fund_complete_info_fn,
        sort_weighted_candidates_fn=sort_weighted_candidates_fn,
        normalize_fcode_fn=normalize_fcode_fn,
        should_filter_holding_period_fn=should_filter_holding_period_fn,
        has_fixed_holding_period_fn=has_fixed_holding_period_fn,
        fund_type_label_map=fund_type_label_map,
        write_final_fund_workbook_fn=write_final_fund_workbook_fn,
        print_complete_info_fetch_start_fn=print_complete_info_fetch_start_fn,
        print_complete_info_fetch_result_fn=print_complete_info_fetch_result_fn,
        print_holding_period_filter_status_fn=(
            print_holding_period_filter_status_fn
        ),
        print_export_cleanup_summary_fn=print_export_cleanup_summary_fn,
        print_final_fund_workbook_written_fn=(
            print_final_fund_workbook_written_fn
        ),
    )

    if fund_type == 'bond':
        structure_audit = build_bond_mainlist_structure_audit_fn(
            final_export.get('df_complete'),
            autostock_validation_status_map=(
                autostock_validation_status_map
                if autostock_validation_status_map is not None
                else quality_gate.autostock_validation_status_map
            ),
            normalize_fcode_fn=normalize_fcode_fn,
        )
        selection.final_screening_summary['mainlist_structure_audit'] = structure_audit
        selection.final_screening_summary['mainlist_soft_warning_count'] = int(
            structure_audit.get(
                'mainlist_soft_warning_count',
                selection.final_screening_summary.get('mainlist_soft_warning_count', 0),
            ) or 0
        )
        selection.final_screening_summary['bond_fee_semantics_audit'] = (
            build_bond_fee_semantics_audit_fn(
                final_export.get('df_complete'),
                sales_fee_validation_classification_map=(
                    load_sales_fee_validation_classification_map_fn()
                ),
                normalize_fcode_fn=normalize_fcode_fn,
            )
        )

    return MainlistPipelineResult(
        quality_gate=quality_gate,
        selection=selection,
        final_export=final_export,
    )


def run_final_fund_export_stage(
    *,
    final_fund_file: Path,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    df_weighted_top: pd.DataFrame,
    fund_type: str,
    autostock_enabled: bool,
    autostock_validation_status_map: dict[str, dict],
    short_report_date: str | None,
    long_report_date: str | None,
    input_pair_status_label: str,
    get_fund_complete_info_fn,
    sort_weighted_candidates_fn,
    normalize_fcode_fn,
    should_filter_holding_period_fn,
    has_fixed_holding_period_fn,
    fund_type_label_map: dict,
    write_final_fund_workbook_fn,
    print_complete_info_fetch_start_fn,
    print_complete_info_fetch_result_fn,
    print_holding_period_filter_status_fn,
    print_export_cleanup_summary_fn,
    print_final_fund_workbook_written_fn,
) -> dict:
    """执行最终主榜完整信息回填、展示规整与 Excel 写出。"""
    final_fcodes = df_weighted_top['fcode'].tolist()
    print_complete_info_fetch_start_fn(len(final_fcodes))

    df_complete = get_fund_complete_info_fn(final_fcodes)
    weighted_export = df_weighted_top[list(_FINAL_EXPORT_WEIGHT_COLUMNS)].copy()

    if df_complete.empty:
        df_final = pd.concat([
            df_short[df_short['fcode'].isin(final_fcodes)],
            df_long[df_long['fcode'].isin(final_fcodes)],
        ]).drop_duplicates(subset=['fcode'])
        df_final = df_final.merge(weighted_export, on='fcode', how='left')
        df_final = sort_weighted_candidates_fn(df_final, 'weighted_score').reset_index(drop=True)
        used_complete_info_fallback = True
    else:
        df_final = df_complete.copy()
        if '基金代码' in df_final.columns:
            df_final['基金代码'] = df_final['基金代码'].map(normalize_fcode_fn)
            df_final = df_final[df_final['基金代码'].isin(final_fcodes)].copy()

        df_final = df_final.merge(
            weighted_export,
            left_on='基金代码',
            right_on='fcode',
            how='left',
        )
        if 'fcode' in df_final.columns:
            df_final = df_final.drop(columns=['fcode'])

        df_final = sort_weighted_candidates_fn(df_final, 'weighted_score').reset_index(drop=True)
        used_complete_info_fallback = False
    print_complete_info_fetch_result_fn(used_complete_info_fallback)

    if 'weighted_score' in df_final.columns:
        df_final = df_final.rename(columns={
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

    name_col = '基金名称' if '基金名称' in df_final.columns else 'shortname'
    if should_filter_holding_period_fn(fund_type):
        holding_count = 0
        if name_col in df_final.columns:
            holding_count = int(
                df_final[name_col].apply(
                    lambda value: (
                        has_fixed_holding_period_fn(str(value))
                        if pd.notna(value)
                        else False
                    )
                ).sum()
            )
        print_holding_period_filter_status_fn(
            fund_type=fund_type,
            filter_applied=True,
            holding_count=holding_count,
        )
    else:
        print_holding_period_filter_status_fn(
            fund_type=fund_type,
            filter_applied=False,
            holding_count=0,
        )

    df_final = df_final.copy()
    if '基金代码' in df_final.columns:
        df_final['基金代码'] = df_final['基金代码'].map(normalize_fcode_fn)
        if autostock_enabled:
            from bond_fund_screening.tasks.final_screening_validation_display import apply_validation_display
            df_final = apply_validation_display(df_final, autostock_validation_status_map, normalize_fcode_fn)

    if '基金类型' in df_final.columns:
        df_final['基金类型'] = df_final['基金类型'].astype(str).map(
            lambda value: fund_type_label_map.get(
                value,
                value if len(value) > 3 else '其他',
            )
        )

    if '基金规模(亿)' in df_final.columns:
        df_final['基金规模(亿)'] = df_final['基金规模(亿)'].apply(
            lambda value: round(value, 2) if pd.notna(value) else None
        )

    removed_manager_column = False
    if '基金经理' in df_final.columns:
        df_final = df_final.drop(columns=['基金经理'])
        removed_manager_column = True

    removed_internal_columns = []
    if '业绩基准' in df_final.columns:
        df_final = df_final.drop(columns=['业绩基准'])
        removed_internal_columns.append('业绩基准')
    for internal_column in ('管理费率', '托管费率', '销售服务费率'):
        if internal_column in df_final.columns:
            df_final = df_final.drop(columns=[internal_column])
            removed_internal_columns.append(internal_column)

    dropped_empty_columns = [
        column_name
        for column_name in df_final.columns
        if df_final[column_name].isna().all()
    ]
    if dropped_empty_columns:
        df_final = df_final.drop(columns=dropped_empty_columns)

    export_cleanup_summary = {
        'removed_manager_column': removed_manager_column,
        'removed_internal_columns': removed_internal_columns,
        'dropped_empty_columns': dropped_empty_columns,
    }
    print_export_cleanup_summary_fn(export_cleanup_summary)

    holding_period_count = write_final_fund_workbook_fn(
        final_fund_file=final_fund_file,
        df_final=df_final,
    )
    print_final_fund_workbook_written_fn(
        final_fund_file=final_fund_file,
        fund_count=len(df_final),
        column_count=len(df_final.columns),
        holding_period_count=holding_period_count,
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        input_pair_status_label=input_pair_status_label,
    )

    return {
        'df_final': df_final,
        'df_complete': df_complete,
        'final_fcodes': final_fcodes,
        'used_complete_info_fallback': used_complete_info_fallback,
        'export_cleanup_summary': export_cleanup_summary,
        'holding_period_count': holding_period_count,
    }
