#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选主榜摘要与观察池编排辅助工具。"""

from __future__ import annotations

from collections import Counter

import pandas as pd

from utils.sales_fee_semantics_audit import (
    build_sales_fee_semantics_payload,
    classify_sales_fee_status,
)


_BOND_MAINLIST_FTYPE_CONCENTRATION_WARN_SHARE = 60.0
_BOND_MAINLIST_COMPANY_CONCENTRATION_WARN_SHARE = 12.0
_BOND_MAINLIST_BENCHMARK_CONCENTRATION_WARN_SHARE = 60.0


def _normalize_structure_counter(counter: Counter, *, selected_total: int) -> list[dict]:
    """将结构分布计数规整为稳定输出结构。"""
    rows = []
    for name, count in counter.most_common():
        rows.append({
            'name': str(name or '未知'),
            'count': int(count),
            'selected_share_pct': round(count / selected_total * 100, 2) if selected_total else 0.0,
        })
    return rows


def _classify_bond_benchmark_family(text: object) -> str:
    """对债基基准文本做轻量归类，仅用于结构偏斜审计。"""
    value = str(text or '').strip()
    if not value:
        return 'unknown'
    if any(token in value for token in ('国开', '国债', '政金债', '农发', '国债指数')):
        return 'rate_policy'
    if any(token in value for token in ('信用', '中高等级', '产业债', '城投')):
        return 'credit'
    if '可转债' in value:
        return 'convertible'
    if any(token in value for token in ('存款', '活期')):
        return 'cash_plus'
    if any(token in value for token in ('综合', '全债', '中债')):
        return 'aggregate'
    return 'other'


def build_bond_mainlist_structure_audit(
    df_mainlist_complete: pd.DataFrame,
    *,
    autostock_validation_status_map: dict[str, dict] | None = None,
    normalize_fcode_fn=None,
) -> dict:
    """
    基于最终主榜完整信息构造债基结构偏斜审计摘要。

    约束：
    1. 仅作为正式产物中的审计摘要，不参与排序和门禁；
    2. 依赖已存在的完整信息查询，不新增额外数据库扫描；
    3. 告警阈值为启发式审计阈值，不宣称为投资科学硬边界。
    """
    if not isinstance(df_mainlist_complete, pd.DataFrame) or df_mainlist_complete.empty:
        return {
            'available': False,
            'reason': 'empty_mainlist',
            'selected_count': 0,
        }

    selected_total = int(len(df_mainlist_complete))
    ftype_counter = Counter(
        str(value or '未知').strip() or '未知'
        for value in df_mainlist_complete.get('基金类型', pd.Series(dtype=object)).tolist()
    )
    company_counter = Counter(
        str(value or '未知').strip() or '未知'
        for value in df_mainlist_complete.get('基金公司', pd.Series(dtype=object)).tolist()
    )
    benchmark_counter = Counter(
        _classify_bond_benchmark_family(value)
        for value in df_mainlist_complete.get('业绩基准', pd.Series(dtype=object)).tolist()
    )

    ftype_counts = _normalize_structure_counter(ftype_counter, selected_total=selected_total)
    company_counts = _normalize_structure_counter(company_counter, selected_total=selected_total)
    benchmark_family_counts = _normalize_structure_counter(
        benchmark_counter,
        selected_total=selected_total,
    )

    dominant_ftype_share = float(ftype_counts[0]['selected_share_pct']) if ftype_counts else 0.0
    dominant_company_share = float(company_counts[0]['selected_share_pct']) if company_counts else 0.0
    dominant_benchmark_share = (
        float(benchmark_family_counts[0]['selected_share_pct']) if benchmark_family_counts else 0.0
    )

    normalized_codes = []
    if '基金代码' in df_mainlist_complete.columns:
        for code in df_mainlist_complete['基金代码'].tolist():
            normalized = normalize_fcode_fn(code) if normalize_fcode_fn is not None else str(code or '').strip()
            if normalized:
                normalized_codes.append(str(normalized))

    status_map = autostock_validation_status_map or {}
    soft_warning_count = 0
    if normalized_codes and status_map:
        soft_warning_count = sum(
            1
            for code in normalized_codes
            if status_map.get(code, {}).get('autostock_validation_status') == 'soft_pass_with_warnings'
        )

    concentration_flags = {
        'ftype_concentrated': dominant_ftype_share >= _BOND_MAINLIST_FTYPE_CONCENTRATION_WARN_SHARE,
        'company_concentrated': dominant_company_share >= _BOND_MAINLIST_COMPANY_CONCENTRATION_WARN_SHARE,
        'benchmark_family_concentrated': (
            dominant_benchmark_share >= _BOND_MAINLIST_BENCHMARK_CONCENTRATION_WARN_SHARE
        ),
    }

    warnings = []
    if concentration_flags['ftype_concentrated'] and ftype_counts:
        warnings.append(
            f"债基主榜子类型集中度偏高：{ftype_counts[0]['name']} 占比 {ftype_counts[0]['selected_share_pct']}%"
        )
    if concentration_flags['company_concentrated'] and company_counts:
        warnings.append(
            f"债基主榜公司集中度偏高：{company_counts[0]['name']} 占比 {company_counts[0]['selected_share_pct']}%"
        )
    if concentration_flags['benchmark_family_concentrated'] and benchmark_family_counts:
        warnings.append(
            "债基主榜 benchmark family 集中度偏高："
            f"{benchmark_family_counts[0]['name']} 占比 {benchmark_family_counts[0]['selected_share_pct']}%"
        )

    return {
        'available': True,
        'selected_count': selected_total,
        'heuristic_thresholds': {
            'ftype_warn_share_pct': _BOND_MAINLIST_FTYPE_CONCENTRATION_WARN_SHARE,
            'company_warn_share_pct': _BOND_MAINLIST_COMPANY_CONCENTRATION_WARN_SHARE,
            'benchmark_family_warn_share_pct': _BOND_MAINLIST_BENCHMARK_CONCENTRATION_WARN_SHARE,
        },
        'ftype_counts': ftype_counts,
        'company_counts': company_counts,
        'benchmark_family_counts': benchmark_family_counts,
        'dominant_ftype': ftype_counts[0]['name'] if ftype_counts else None,
        'dominant_ftype_share_pct': dominant_ftype_share,
        'dominant_company': company_counts[0]['name'] if company_counts else None,
        'dominant_company_share_pct': dominant_company_share,
        'dominant_benchmark_family': benchmark_family_counts[0]['name'] if benchmark_family_counts else None,
        'dominant_benchmark_family_share_pct': dominant_benchmark_share,
        'mainlist_soft_warning_count': int(soft_warning_count),
        'concentration_flags': concentration_flags,
        'warnings': warnings,
    }


def build_bond_fee_semantics_audit(
    df_mainlist_complete: pd.DataFrame,
    *,
    sales_fee_validation_classification_map: dict[str, str] | None = None,
    normalize_fcode_fn=None,
) -> dict:
    """
    基于债基主榜样本的数据库快照构建费率语义审计摘要。

    约束：
    1. 仅服务于 sidecar / decision ledger / 审计摘要，不回写正式门禁；
    2. 摘要基于最终主榜样本的数据库快照，不等同于 Layer 5.5 运行时完整回放；
    3. official_placeholder / shadow_code 仅在存在外部校验分类映射时才输出。
    """
    if not isinstance(df_mainlist_complete, pd.DataFrame) or df_mainlist_complete.empty:
        return {
            'available': False,
            'reason': 'empty_mainlist',
            'selected_count': 0,
            'data_source': 'mainlist_db_snapshot',
        }

    required_columns = {'基金代码', '基金名称', '基金类型', '管理费率', '托管费率', '销售服务费率'}
    if not required_columns.issubset(set(df_mainlist_complete.columns)):
        return {
            'available': False,
            'reason': 'missing_required_columns',
            'selected_count': int(len(df_mainlist_complete)),
            'data_source': 'mainlist_db_snapshot',
            'missing_columns': sorted(required_columns - set(df_mainlist_complete.columns)),
        }

    rows = []
    validation_map = sales_fee_validation_classification_map or {}
    status_counter = Counter()
    total_fee_status_counter = Counter()
    fee_component_status_counters = {
        'mgr_fee_rate': Counter(),
        'trust_fee_rate': Counter(),
        'sales_fee_rate': Counter(),
    }
    component_complete_rows = 0
    component_missing_rows = 0
    component_invalid_rows = 0
    total_fee_zero_rows = 0
    total_fee_positive_rows = 0
    total_fee_support_samples = []
    warning_samples = []

    def _fee_numeric(value):
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric < 0:
            return None
        return numeric

    for row in df_mainlist_complete.to_dict(orient='records'):
        raw_code = row.get('基金代码')
        fcode = (
            str(normalize_fcode_fn(raw_code))
            if normalize_fcode_fn is not None and raw_code is not None
            else str(raw_code or '').strip()
        )
        mgr_fee_rate = row.get('管理费率')
        trust_fee_rate = row.get('托管费率')
        sales_fee_rate = row.get('销售服务费率')
        mgr_fee_rate = None if pd.isna(mgr_fee_rate) else mgr_fee_rate
        trust_fee_rate = None if pd.isna(trust_fee_rate) else trust_fee_rate
        sales_fee_rate = None if pd.isna(sales_fee_rate) else sales_fee_rate
        has_core_fee_pair = pd.notna(mgr_fee_rate) and pd.notna(trust_fee_rate)
        mgr_fee_status = classify_sales_fee_status(mgr_fee_rate)
        trust_fee_status = classify_sales_fee_status(trust_fee_rate)
        sales_fee_status = classify_sales_fee_status(sales_fee_rate)
        component_statuses = {
            'mgr_fee_rate': mgr_fee_status,
            'trust_fee_rate': trust_fee_status,
            'sales_fee_rate': sales_fee_status,
        }
        for component_key, component_status in component_statuses.items():
            fee_component_status_counters[component_key][component_status] += 1
        component_has_missing = any(status == 'null' for status in component_statuses.values())
        component_has_invalid = any(status == 'invalid' for status in component_statuses.values())
        if component_has_invalid:
            component_invalid_rows += 1
        elif component_has_missing:
            component_missing_rows += 1
        else:
            component_complete_rows += 1

        known_fee_total = sum(
            value
            for value in (
                _fee_numeric(mgr_fee_rate),
                _fee_numeric(trust_fee_rate),
                _fee_numeric(sales_fee_rate),
            )
            if value is not None
        )
        if component_has_invalid:
            total_fee_support_status = 'invalid_component'
        elif component_has_missing:
            total_fee_support_status = (
                'missing_component_as_zero_positive'
                if known_fee_total > 0
                else 'missing_component_as_zero_zero'
            )
        else:
            total_fee_support_status = 'exact_positive' if known_fee_total > 0 else 'exact_zero'
        total_fee_status_counter[total_fee_support_status] += 1
        if known_fee_total > 0:
            total_fee_positive_rows += 1
        elif not component_has_invalid:
            total_fee_zero_rows += 1

        runtime_semantic_status = None
        if sales_fee_status == 'invalid':
            runtime_semantic_status = 'sales_fee_invalid'
        elif sales_fee_status == 'null':
            if not has_core_fee_pair:
                runtime_semantic_status = 'core_fee_missing'
            elif fcode and validation_map.get(fcode) == 'official_placeholder_confirmed':
                runtime_semantic_status = 'official_placeholder_confirmed'
            elif fcode and validation_map.get(fcode) == 'shadow_code_candidate':
                runtime_semantic_status = 'shadow_code_candidate'
            else:
                runtime_semantic_status = 'sales_fee_missing_unresolved'
        else:
            runtime_semantic_status = 'exact'

        status_counter[runtime_semantic_status] += 1
        rows.append({
            'fcode': fcode,
            'shortname': row.get('基金名称'),
            'fullname': row.get('基金名称'),
            'ftype': row.get('基金类型'),
            'type_code': None,
            'type_category': 'bond',
            'mgr_fee_rate': mgr_fee_rate,
            'trust_fee_rate': trust_fee_rate,
            'sales_fee_rate': sales_fee_rate,
            'benchmark': row.get('业绩基准'),
        })

        if runtime_semantic_status in {
            'sales_fee_missing_unresolved',
            'sales_fee_invalid',
            'shadow_code_candidate',
            'core_fee_missing',
        } and fcode:
            warning_samples.append({
                'fcode': fcode,
                'shortname': str(row.get('基金名称') or '').strip(),
                'runtime_semantic_status': runtime_semantic_status,
                'sales_fee_status': sales_fee_status,
                'total_fee_support_status': total_fee_support_status,
            })
        if (
            total_fee_support_status != 'exact_positive'
            and total_fee_support_status != 'exact_zero'
            and fcode
            and len(total_fee_support_samples) < 10
        ):
            total_fee_support_samples.append({
                'fcode': fcode,
                'shortname': str(row.get('基金名称') or '').strip(),
                'total_fee_support_status': total_fee_support_status,
                'mgr_fee_status': mgr_fee_status,
                'trust_fee_status': trust_fee_status,
                'sales_fee_status': sales_fee_status,
                'known_fee_total': round(float(known_fee_total), 6),
            })

    payload = build_sales_fee_semantics_payload(rows)
    summary = payload.get('summary') or {}

    warnings = []
    missing_component_total = int(
        total_fee_status_counter.get('missing_component_as_zero_positive', 0)
        + total_fee_status_counter.get('missing_component_as_zero_zero', 0)
    )
    if missing_component_total > 0:
        warnings.append(
            f"主榜样本中有 {missing_component_total} 只费率合计依赖缺失组件按 0 处理，不能解释为完整费率"
        )
    if total_fee_status_counter.get('invalid_component', 0) > 0:
        warnings.append(
            f"主榜样本中有 {int(total_fee_status_counter['invalid_component'])} 只存在非法费率组件，费率解释能力不足"
        )
    if status_counter.get('sales_fee_missing_unresolved', 0) > 0:
        warnings.append(
            f"主榜样本中仍有 {int(status_counter['sales_fee_missing_unresolved'])} 只销售服务费缺失且未完成语义确认"
        )
    if status_counter.get('sales_fee_invalid', 0) > 0:
        warnings.append(
            f"主榜样本中有 {int(status_counter['sales_fee_invalid'])} 只销售服务费为非法值"
        )
    if status_counter.get('shadow_code_candidate', 0) > 0:
        warnings.append(
            f"主榜样本中有 {int(status_counter['shadow_code_candidate'])} 只疑似影子代码，销售服务费口径需谨慎解释"
        )
    if status_counter.get('core_fee_missing', 0) > 0:
        warnings.append(
            f"主榜样本中有 {int(status_counter['core_fee_missing'])} 只连管理费/托管费都不完整，费率解释能力不足"
        )

    return {
        'available': True,
        'data_source': 'mainlist_db_snapshot',
        'scope': 'bond_mainlist_only',
        'runtime_replay': False,
        'selected_count': int(len(df_mainlist_complete)),
        'summary': {
            'total_rows': int(summary.get('total_rows', 0) or 0),
            'null_rows': int(summary.get('null_rows', 0) or 0),
            'zero_rows': int(summary.get('zero_rows', 0) or 0),
            'positive_rows': int(summary.get('positive_rows', 0) or 0),
            'invalid_rows': int(summary.get('invalid_rows', 0) or 0),
            'sales_fee_null_rows': int(summary.get('null_rows', 0) or 0),
            'sales_fee_zero_rows': int(summary.get('zero_rows', 0) or 0),
            'sales_fee_positive_rows': int(summary.get('positive_rows', 0) or 0),
            'sales_fee_invalid_rows': int(summary.get('invalid_rows', 0) or 0),
            'null_rows_with_core_fee_pair': int(summary.get('null_rows_with_core_fee_pair', 0) or 0),
            'null_rows_requiring_family_review': int(summary.get('null_rows_requiring_family_review', 0) or 0),
            'family_count': int(summary.get('family_count', 0) or 0),
            'fee_component_complete_rows': int(component_complete_rows),
            'fee_component_missing_rows': int(component_missing_rows),
            'fee_component_invalid_rows': int(component_invalid_rows),
            'total_fee_zero_rows': int(total_fee_zero_rows),
            'total_fee_positive_rows': int(total_fee_positive_rows),
            'total_fee_missing_component_as_zero_rows': int(missing_component_total),
            'total_fee_invalid_component_rows': int(total_fee_status_counter.get('invalid_component', 0)),
        },
        'fee_component_status_counts': {
            key: dict(counter)
            for key, counter in fee_component_status_counters.items()
        },
        'total_fee_semantic_status_counts': dict(total_fee_status_counter),
        'runtime_semantic_status_counts': dict(status_counter),
        'family_pattern_counts': dict(payload.get('family_pattern_counts') or {}),
        'sample_missing_review_rows': list(payload.get('sample_missing_review_rows') or [])[:10],
        'sample_total_fee_support_rows': total_fee_support_samples[:10],
        'sample_warning_funds': warning_samples[:10],
        'warnings': warnings,
        'notes': [
            '该摘要基于最终主榜样本对应的数据库费率快照，不等同于 Layer 5.5 运行时完整回放。',
            'official_placeholder_confirmed / shadow_code_candidate 仅在存在定向复核分类映射时才会标注。',
            '历史字段 null_rows/zero_rows/positive_rows/invalid_rows 描述的是销售服务费率组件，不等同于费率合计。',
            'total_fee_semantic_status_counts 描述三项费率合计是否由完整组件支持；missing_component_as_zero 表示仅用于解释审计，不改变生产门禁。',
        ],
    }


def build_final_selection_summary(
    *,
    df_weighted: pd.DataFrame,
    df_weighted_selected_pool: pd.DataFrame,
    df_weighted_top: pd.DataFrame,
    candidate_summary: dict,
    completeness_summary: dict,
    incomplete_fcodes: set[str],
    autostock_validation_summary: dict,
    autostock_failed_fcodes: set[str],
    autostock_validation_status_map: dict[str, dict],
    drawdown_duration_summary: dict,
    candidate_pool_reduction: dict,
    single_side_watchlist_limit: int,
    final_mainlist_required_columns: tuple[str, ...],
    raw_intersection_count: int,
    fund_type: str,
    mainlist_selection_meta: dict | None = None,
    bond_subtype_edge_governance: dict | None = None,
) -> dict:
    """构造最终主榜摘要与核心统计。"""
    mainlist_selection_meta = mainlist_selection_meta or {}
    bond_subtype_edge_governance = bond_subtype_edge_governance or {}
    top_n_final = len(df_weighted_top)
    eligible_pool_count = len(df_weighted_selected_pool)
    dedup_or_filtered_removed = int(candidate_pool_reduction.get('total_removed', 0) or 0)
    mainlist_selection_mode = (
        str(mainlist_selection_meta.get('selection_mode') or 'fixed_top_n').strip()
        or 'fixed_top_n'
    )
    top_n_trimmed = 0 if mainlist_selection_mode == 'all_eligible' else max(eligible_pool_count - top_n_final, 0)
    mainlist_selection_anchor_top_n = int(
        mainlist_selection_meta.get('selection_anchor_top_n', top_n_final) or top_n_final
    )
    mainlist_score_band_delta = float(
        mainlist_selection_meta.get('selection_score_band_delta', 0.0) or 0.0
    )
    mainlist_selection_expanded_count = int(
        mainlist_selection_meta.get('selection_expanded_count', 0) or 0
    )

    top_cutoff_score = None
    final_top_category_counts = []
    top_category_counts = {}
    if len(df_weighted_top) > 0:
        category_counts = df_weighted_top['fund_category'].value_counts()
        top_cutoff_score = round(float(df_weighted_top['weighted_score'].iloc[-1]), 2)
        final_top_category_counts = [
            {
                'category': str(category),
                'count': int(count),
            }
            for category, count in category_counts.items()
        ]
        top_category_counts = {
            str(category): int(count)
            for category, count in category_counts.items()
        }

    strict_match_count = sum(
        1 for item in autostock_validation_status_map.values()
        if item.get('autostock_validation_status') == 'strict_match'
    )
    soft_warning_count = sum(
        1 for item in autostock_validation_status_map.values()
        if item.get('autostock_validation_status') == 'soft_pass_with_warnings'
    )

    final_screening_summary = {
        'fund_type': fund_type,
        'union_candidate_count': candidate_summary['union_candidate_count'],
        'pre_mainlist_candidate_count': len(df_weighted),
        'data_completeness_checked_count': int(completeness_summary.get('checked_count', 0) or 0),
        'data_completeness_filtered_count': int(len(incomplete_fcodes)),
        'data_completeness_required_columns': list(final_mainlist_required_columns),
        'data_completeness_filtered_fcodes': completeness_summary.get('sample_fcodes', []),
        'autostock_cross_validation_checked_count': int(autostock_validation_summary.get('checked_count', 0) or 0),
        'autostock_cross_validation_filtered_count': int(len(autostock_failed_fcodes)),
        'autostock_cross_validation_passed_count': int(autostock_validation_summary.get('passed_count', 0) or 0),
        'autostock_cross_validation_filtered_fcodes': autostock_validation_summary.get('sample_fcodes', []),
        'autostock_cross_validation_reason_counts': autostock_validation_summary.get('reason_counts', []),
        'autostock_cross_validation_warning_counts': autostock_validation_summary.get('warning_counts', []),
        'autostock_cross_validation_batch_meta': autostock_validation_summary.get('batch_meta', {}),
        'autostock_validation_cache_hit': bool(autostock_validation_summary.get('cache_hit', False)),
        'autostock_validation_cache_generated_at': autostock_validation_summary.get('cache_generated_at'),
        'autostock_strict_match_count': int(strict_match_count),
        'autostock_soft_warning_count': int(soft_warning_count),
        'mainlist_soft_warning_count': int(soft_warning_count),
        'drawdown_duration_checked_count': int(drawdown_duration_summary.get('checked_count', 0) or 0),
        'drawdown_duration_missing_count': int(drawdown_duration_summary.get('missing_count', 0) or 0),
        'drawdown_duration_missing_fcodes': drawdown_duration_summary.get('sample_missing_fcodes', []),
        'drawdown_duration_analysis_window_days': drawdown_duration_summary.get('analysis_window_days'),
        'drawdown_duration_nav_buffer_days': drawdown_duration_summary.get('nav_buffer_days'),
        'drawdown_duration_distribution': drawdown_duration_summary.get('distribution', {}),
        'drawdown_duration_high_duration_samples': drawdown_duration_summary.get('high_duration_samples', []),
        'eligible_pool_count': eligible_pool_count,
        'dedup_or_filtered_removed': dedup_or_filtered_removed,
        'candidate_pool_reduction': {
            'holding_period_filtered': int(candidate_pool_reduction.get('holding_period_filtered', 0)),
            'share_class_collapsed': int(candidate_pool_reduction.get('share_class_collapsed', 0)),
        },
        'mainlist_selection_mode': mainlist_selection_mode,
        'mainlist_selection_anchor_top_n': mainlist_selection_anchor_top_n,
        'mainlist_score_band_delta': mainlist_score_band_delta,
        'mainlist_selection_expanded_count': mainlist_selection_expanded_count,
        'bond_subtype_edge_governance': bond_subtype_edge_governance,
        'top_n_trimmed': top_n_trimmed,
        'final_output_count': top_n_final,
        'intersection_count': candidate_summary['intersection_count'],
        'soft_intersection_count': candidate_summary['soft_intersection_count'],
        'only_short_count': candidate_summary['only_short_count'],
        'only_long_count': candidate_summary['only_long_count'],
        'raw_only_short_count': candidate_summary['raw_only_short_count'],
        'raw_only_long_count': candidate_summary['raw_only_long_count'],
        'raw_intersection_count': int(raw_intersection_count),
        'top_cutoff_score': top_cutoff_score,
        'single_side_watchlist_limit': single_side_watchlist_limit,
        'final_top_category_counts': final_top_category_counts,
    }

    return {
        'top_n_final': top_n_final,
        'eligible_pool_count': eligible_pool_count,
        'dedup_or_filtered_removed': dedup_or_filtered_removed,
        'mainlist_selection_mode': mainlist_selection_mode,
        'mainlist_selection_anchor_top_n': mainlist_selection_anchor_top_n,
        'mainlist_score_band_delta': mainlist_score_band_delta,
        'mainlist_selection_expanded_count': mainlist_selection_expanded_count,
        'bond_subtype_edge_governance': bond_subtype_edge_governance,
        'top_n_trimmed': top_n_trimmed,
        'top_cutoff_score': top_cutoff_score,
        'final_top_category_counts': final_top_category_counts,
        'top_category_counts': top_category_counts,
        'strict_match_count': int(strict_match_count),
        'soft_warning_count': int(soft_warning_count),
        'final_screening_summary': final_screening_summary,
    }


def build_watchlist_bundle(
    *,
    df_weighted: pd.DataFrame,
    fund_type: str,
    single_side_watchlist_limit: int,
    watchlist_excluded_fcodes: set[str],
    df_weighted_selected_pool: pd.DataFrame,
    df_weighted_top: pd.DataFrame,
    candidate_summary: dict,
    top_cutoff_score: float | None,
    incomplete_candidates_df: pd.DataFrame,
    autostock_failed_candidates_df: pd.DataFrame,
    autostock_validation_status_map: dict[str, dict],
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict,
    cross_long_period_counts: dict,
    cross_short_base_scores: dict,
    cross_long_base_scores: dict,
    cross_short_base_period_counts: dict,
    cross_long_base_period_counts: dict,
    candidate_pool_reduction: dict,
    final_screening_summary: dict,
    build_single_side_watchlist_fn,
    rebind_watchlist_fn,
    annotate_watchlist_fn,
    summarize_watchlist_context_fn,
    build_explanations_fn,
    build_soft_intersection_audit_map_fn,
    build_decision_ledger_records_fn,
    df_weighted_selected_pool_prevalidation: pd.DataFrame,
) -> dict:
    """构造观察池、解释摘要与候选决策账本。"""
    short_watchlist = build_single_side_watchlist_fn(
        df_weighted,
        fund_type=fund_type,
        target_category='近期优质',
        top_n_limit=single_side_watchlist_limit,
        excluded_fcodes=watchlist_excluded_fcodes,
        collapse_share_classes_enabled=True,
    )
    short_watchlist = rebind_watchlist_fn(
        short_watchlist,
        df_weighted_selected_pool=df_weighted_selected_pool,
    )
    short_watchlist = annotate_watchlist_fn(
        short_watchlist,
        df_weighted_top=df_weighted_top,
        df_weighted_selected_pool=df_weighted_selected_pool,
    )

    long_watchlist = build_single_side_watchlist_fn(
        df_weighted,
        fund_type=fund_type,
        target_category='长期优质',
        top_n_limit=single_side_watchlist_limit,
        excluded_fcodes=watchlist_excluded_fcodes,
        collapse_share_classes_enabled=True,
    )
    long_watchlist = rebind_watchlist_fn(
        long_watchlist,
        df_weighted_selected_pool=df_weighted_selected_pool,
    )
    long_watchlist = annotate_watchlist_fn(
        long_watchlist,
        df_weighted_top=df_weighted_top,
        df_weighted_selected_pool=df_weighted_selected_pool,
    )

    watchlist_status_counts, watchlist_reason_counts = summarize_watchlist_context_fn(
        short_watchlist,
        long_watchlist,
    )
    explanation_payload = build_explanations_fn(
        fund_type=fund_type,
        union_candidate_count=candidate_summary['union_candidate_count'],
        eligible_pool_count=int(final_screening_summary.get('eligible_pool_count', 0) or 0),
        dedup_or_filtered_removed=int(final_screening_summary.get('dedup_or_filtered_removed', 0) or 0),
        top_n_trimmed=int(final_screening_summary.get('top_n_trimmed', 0) or 0),
        final_output_count=int(final_screening_summary.get('final_output_count', 0) or 0),
        intersection_count=candidate_summary['intersection_count'],
        soft_intersection_count=candidate_summary['soft_intersection_count'],
        only_short_count=candidate_summary['only_short_count'],
        only_long_count=candidate_summary['only_long_count'],
        final_top_category_counts=final_screening_summary.get('final_top_category_counts', []),
        watchlist_reason_counts=watchlist_reason_counts,
        candidate_pool_reduction=candidate_pool_reduction,
        data_completeness_filtered=int(final_screening_summary.get('data_completeness_filtered_count', 0) or 0),
        autostock_filtered=int(final_screening_summary.get('autostock_cross_validation_filtered_count', 0) or 0),
        drawdown_duration_distribution=final_screening_summary.get('drawdown_duration_distribution', {}),
        mainlist_selection_mode=str(final_screening_summary.get('mainlist_selection_mode') or 'fixed_top_n'),
        mainlist_selection_anchor_top_n=int(
            final_screening_summary.get('mainlist_selection_anchor_top_n', 0) or 0
        ),
        mainlist_score_band_delta=float(
            final_screening_summary.get('mainlist_score_band_delta', 0.0) or 0.0
        ),
        mainlist_selection_expanded_count=int(
            final_screening_summary.get('mainlist_selection_expanded_count', 0) or 0
        ),
        bond_subtype_edge_governance=final_screening_summary.get('bond_subtype_edge_governance') or {},
    )

    summary_updates = {
        'watchlist_export_count': int(len(short_watchlist) + len(long_watchlist)),
        'watchlist_status_counts': watchlist_status_counts,
        'watchlist_reason_counts': watchlist_reason_counts,
        'narrative_summary': explanation_payload['narrative_summary'],
        'strategy_validation_note': explanation_payload['strategy_validation_note'],
        'headline_highlights': explanation_payload['headline_highlights'],
    }

    soft_intersection_audit_map = build_soft_intersection_audit_map_fn(
        df_weighted,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
    )
    final_screening_decision_records = build_decision_ledger_records_fn(
        df_weighted=df_weighted,
        fund_type=fund_type,
        df_weighted_selected_pool_prevalidation=df_weighted_selected_pool_prevalidation,
        df_weighted_selected_pool=df_weighted_selected_pool,
        df_weighted_top=df_weighted_top,
        incomplete_candidates_df=incomplete_candidates_df,
        autostock_failed_candidates_df=autostock_failed_candidates_df,
        autostock_validation_status_map=autostock_validation_status_map,
        short_watchlist=short_watchlist,
        long_watchlist=long_watchlist,
        candidate_summary=candidate_summary,
        top_cutoff_score=top_cutoff_score,
        soft_intersection_audit_map=soft_intersection_audit_map,
        bond_subtype_edge_governance=final_screening_summary.get('bond_subtype_edge_governance') or {},
    )

    return {
        'short_watchlist': short_watchlist,
        'long_watchlist': long_watchlist,
        'summary_updates': summary_updates,
        'explanation_payload': explanation_payload,
        'soft_intersection_audit_map': soft_intersection_audit_map,
        'final_screening_decision_records': final_screening_decision_records,
    }
