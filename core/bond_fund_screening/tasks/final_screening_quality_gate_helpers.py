#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选质量门禁辅助工具。"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json

import pandas as pd
from utils.calculation_capture import capture_context, record_event
from bond_fund_screening.core.nav_window import select_nav_period_window
from utils.drawdown_duration import DRAWDOWN_OBSERVATION_DEFINITION

from bond_fund_screening.tasks import final_screening_autostock_helpers as autostock_budget_helpers


def _hash_payload(value) -> str | None:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
            default=str,
        )
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _build_autostock_validation_source_evidence(
    *,
    candidate_codes: list[str],
    codes_to_fetch: list[str],
    reused_entries: dict,
    batch_meta: dict,
    source_type: str,
    reason: str | None = None,
    detail_records: list | None = None,
) -> dict:
    raw_evidence = (
        dict(batch_meta.get('source_evidence') or {})
        if isinstance(batch_meta.get('source_evidence'), dict)
        else {}
    )
    evidence = {
        'schema_version': 1,
        'task': 'autostock_validation',
        'source': raw_evidence.get('source') or (
            'AutoStock validation cache'
            if source_type == 'cache'
            else 'AutoStock /fund'
        ),
        'source_type': source_type,
        'base_url': raw_evidence.get('base_url') or '',
        'endpoint_path': raw_evidence.get('endpoint_path') or (
            'cache'
            if source_type == 'cache'
            else '/fund'
        ),
        'request_window': raw_evidence.get('request_window') or {
            'started_at': None,
            'finished_at': None,
        },
        'target_count': int(len(candidate_codes)),
        'api_target_count': int(len(codes_to_fetch)),
        'cache_reused_count': int(len(reused_entries)),
        'request_count': int(raw_evidence.get('request_count') or 0),
        'response_count': int(raw_evidence.get('response_count') or 0),
        'accepted_response_count': int(raw_evidence.get('accepted_response_count') or 0),
        'failed_response_count': int(raw_evidence.get('failed_response_count') or 0),
        'transport_exception_count': int(raw_evidence.get('transport_exception_count') or 0),
        'returned_record_count': int(raw_evidence.get('returned_record_count') or 0),
        'returned_unique_count': int(raw_evidence.get('returned_unique_count') or 0),
        'missing_count': int(raw_evidence.get('missing_count') or batch_meta.get('missing_count') or 0),
        'failed_batch_count': int(raw_evidence.get('failed_batch_count') or batch_meta.get('failed_batch_count') or 0),
        'quota_exhausted': bool(raw_evidence.get('quota_exhausted') or batch_meta.get('quota_exhausted')),
        'last_error': raw_evidence.get('last_error') or batch_meta.get('last_error'),
        'response_hash': raw_evidence.get('response_hash') or _hash_payload(detail_records or []),
        'sample_codes': list(raw_evidence.get('sample_codes') or candidate_codes[:10]),
        'source_complete': bool(raw_evidence.get('source_complete', True)),
        'reason': reason,
    }
    if source_type == 'cache':
        evidence.update({
            'source_complete': True,
            'returned_record_count': int(len(detail_records or [])),
            'returned_unique_count': int(len(candidate_codes)),
            'missing_count': 0,
            'failed_batch_count': 0,
            'last_error': None,
        })
    if source_type == 'budget_degraded':
        evidence['source_complete'] = False
    if source_type == 'api_batch':
        evidence['source_complete'] = bool(
            evidence['failed_response_count'] == 0
            and evidence['transport_exception_count'] == 0
            and evidence['failed_batch_count'] == 0
            and evidence['missing_count'] == 0
            and not evidence['quota_exhausted']
        )
    return evidence


def _build_autostock_row_lookup(rows: list, normalize_fcode_fn) -> dict[str, dict]:
    """按基金代码去重 AutoStock `/fund` 返回行，保留首次真实响应。"""
    lookup: dict[str, dict] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        fcode = normalize_fcode_fn(row.get('code'))
        if not fcode or fcode in lookup:
            continue
        lookup[fcode] = row
    return lookup


def _missing_autostock_codes(codes: list[str], lookup: dict[str, dict], normalize_fcode_fn) -> list[str]:
    seen = set()
    missing_codes = []
    for raw_code in codes or []:
        fcode = normalize_fcode_fn(raw_code)
        if not fcode or fcode in seen:
            continue
        seen.add(fcode)
        if fcode not in lookup:
            missing_codes.append(fcode)
    return missing_codes


def _compact_autostock_fetch_meta(meta: dict | None) -> dict:
    source_evidence = dict((meta or {}).get('source_evidence') or {})
    return {
        'request_count': int(source_evidence.get('request_count') or 0),
        'response_count': int(source_evidence.get('response_count') or 0),
        'accepted_response_count': int(source_evidence.get('accepted_response_count') or 0),
        'failed_response_count': int(source_evidence.get('failed_response_count') or 0),
        'transport_exception_count': int(source_evidence.get('transport_exception_count') or 0),
        'failed_batch_count': int((meta or {}).get('failed_batch_count') or source_evidence.get('failed_batch_count') or 0),
        'quota_exhausted': bool((meta or {}).get('quota_exhausted') or source_evidence.get('quota_exhausted')),
        'last_error': (meta or {}).get('last_error') or source_evidence.get('last_error'),
    }


def _build_combined_autostock_source_evidence(
    *,
    metas: list[dict],
    rows: list,
    target_codes: list[str],
    missing_codes: list[str],
) -> dict:
    request_count = 0
    response_count = 0
    accepted_response_count = 0
    failed_response_count = 0
    transport_exception_count = 0
    failed_batch_count = 0
    quota_exhausted = False
    last_error = None
    base_evidence = {}

    for meta in metas or []:
        evidence = dict((meta or {}).get('source_evidence') or {})
        if evidence and not base_evidence:
            base_evidence = evidence
        request_count += int(evidence.get('request_count') or 0)
        response_count += int(evidence.get('response_count') or 0)
        accepted_response_count += int(evidence.get('accepted_response_count') or 0)
        failed_response_count += int(evidence.get('failed_response_count') or 0)
        transport_exception_count += int(evidence.get('transport_exception_count') or 0)
        failed_batch_count += int((meta or {}).get('failed_batch_count') or evidence.get('failed_batch_count') or 0)
        quota_exhausted = quota_exhausted or bool((meta or {}).get('quota_exhausted') or evidence.get('quota_exhausted'))
        last_error = (meta or {}).get('last_error') or evidence.get('last_error') or last_error

    returned_codes = {
        str(row.get('code')).strip()
        for row in rows or []
        if isinstance(row, dict) and str(row.get('code') or '').strip()
    }
    evidence = {
        'schema_version': 1,
        'task': 'autostock_validation',
        'source': base_evidence.get('source') or 'AutoStock /fund',
        'source_type': base_evidence.get('source_type') or 'api',
        'base_url': base_evidence.get('base_url') or '',
        'endpoint_path': base_evidence.get('endpoint_path') or '/fund',
        'request_window': {
            'started_at': (base_evidence.get('request_window') or {}).get('started_at'),
            'finished_at': None,
        },
        'target_count': int(len(target_codes)),
        'request_count': int(request_count),
        'response_count': int(response_count),
        'accepted_response_count': int(accepted_response_count),
        'failed_response_count': int(failed_response_count),
        'transport_exception_count': int(transport_exception_count),
        'returned_record_count': int(len(rows or [])),
        'returned_unique_count': int(len(returned_codes)),
        'missing_count': int(len(missing_codes)),
        'failed_batch_count': int(failed_batch_count),
        'quota_exhausted': bool(quota_exhausted),
        'last_error': str(last_error)[:300] if last_error else None,
        'response_hash': _hash_payload(rows or []),
        'sample_codes': list(sorted(returned_codes))[:10],
        'missing_codes_preview': list(missing_codes[:20]),
    }
    for meta in reversed(metas or []):
        meta_window = ((meta or {}).get('source_evidence') or {}).get('request_window') or {}
        if meta_window.get('finished_at'):
            evidence['request_window']['finished_at'] = meta_window.get('finished_at')
            break
    evidence['source_complete'] = bool(
        evidence['failed_response_count'] == 0
        and evidence['transport_exception_count'] == 0
        and evidence['failed_batch_count'] == 0
        and evidence['missing_count'] == 0
        and not evidence['quota_exhausted']
    )
    return evidence


def _recover_autostock_missing_rows(
    *,
    client,
    initial_rows: list,
    initial_meta: dict,
    codes_to_fetch: list[str],
    batch_size: int,
    normalize_fcode_fn,
) -> tuple[list, dict]:
    """对 AutoStock `/fund` 成功响应但缺行的代码做小批量重试。"""
    all_rows = list(initial_rows or [])
    metas = [dict(initial_meta or {})]
    lookup = _build_autostock_row_lookup(all_rows, normalize_fcode_fn)
    missing_codes = _missing_autostock_codes(codes_to_fetch, lookup, normalize_fcode_fn)
    recovery = {
        'attempted': False,
        'initial_missing_count': int(len(missing_codes)),
        'recovered_count': 0,
        'remaining_missing_count': int(len(missing_codes)),
        'stages': [],
    }

    if not missing_codes or bool((initial_meta or {}).get('quota_exhausted')):
        combined_meta = dict(initial_meta or {})
        combined_meta['source_evidence'] = _build_combined_autostock_source_evidence(
            metas=metas,
            rows=all_rows,
            target_codes=codes_to_fetch,
            missing_codes=missing_codes,
        )
        combined_meta['fallback_recovery'] = recovery
        return all_rows, combined_meta

    if bool((initial_meta or {}).get('circuit_breaker_open')):
        recovery.update({
            'skipped_reason': 'transport_circuit_open',
            'circuit_breaker_reason': (initial_meta or {}).get('circuit_breaker_reason'),
        })
        combined_meta = dict(initial_meta or {})
        combined_meta['source_evidence'] = _build_combined_autostock_source_evidence(
            metas=metas,
            rows=all_rows,
            target_codes=codes_to_fetch,
            missing_codes=missing_codes,
        )
        combined_meta['fallback_recovery'] = recovery
        return all_rows, combined_meta

    retry_batch_size = max(1, min(20, int(batch_size or 1), len(missing_codes)))
    retry_stages = [('fund_batch_missing_retry', retry_batch_size)]
    if len(missing_codes) <= 10:
        retry_stages.append(('fund_batch_single_retry', 1))

    recovery['attempted'] = True
    for stage_name, stage_batch_size in retry_stages:
        if not missing_codes:
            break

        before_missing_count = len(missing_codes)
        try:
            retry_rows, retry_meta = client.get_fund_batch_with_meta(
                missing_codes,
                batch_size=stage_batch_size,
            )
        except Exception as exc:
            retry_meta = {
                'last_error': str(exc),
                'quota_exhausted': False,
                'failed_batch_count': 1,
                'source_evidence': {
                    'schema_version': 1,
                    'task': 'autostock_validation',
                    'source': 'AutoStock /fund',
                    'source_type': 'api',
                    'endpoint_path': '/fund',
                    'request_count': 0,
                    'response_count': 0,
                    'accepted_response_count': 0,
                    'failed_response_count': 1,
                    'transport_exception_count': 1,
                    'returned_record_count': 0,
                    'returned_unique_count': 0,
                    'missing_count': before_missing_count,
                    'failed_batch_count': 1,
                    'quota_exhausted': False,
                    'last_error': str(exc)[:300],
                    'source_complete': False,
                },
            }
            retry_rows = []

        retry_meta = dict(retry_meta or {})
        metas.append(retry_meta)
        retry_lookup = _build_autostock_row_lookup(retry_rows or [], normalize_fcode_fn)
        recovered_codes = [code for code in missing_codes if code in retry_lookup]
        for code in recovered_codes:
            lookup[code] = retry_lookup[code]
            all_rows.append(retry_lookup[code])

        missing_codes = _missing_autostock_codes(missing_codes, lookup, normalize_fcode_fn)
        recovery['stages'].append({
            'stage': stage_name,
            'requested_total': int(before_missing_count),
            'requested_batch_size': int(stage_batch_size),
            'returned_unique_total': int(len(retry_lookup)),
            'recovered_count': int(len(recovered_codes)),
            'remaining_missing_count': int(len(missing_codes)),
            'quota_exhausted': bool(retry_meta.get('quota_exhausted')),
            'last_error': retry_meta.get('last_error'),
            'meta': _compact_autostock_fetch_meta(retry_meta),
        })

        if retry_meta.get('quota_exhausted'):
            break

    recovery['recovered_count'] = max(0, int(recovery['initial_missing_count']) - int(len(missing_codes)))
    recovery['remaining_missing_count'] = int(len(missing_codes))

    combined_meta = dict(initial_meta or {})
    combined_meta['returned_total'] = int(len(all_rows))
    combined_meta['returned_unique_total'] = int(len(lookup))
    combined_meta['missing_count'] = int(len(missing_codes))
    combined_meta['missing_codes_preview'] = list(missing_codes[:20])
    combined_meta['quota_exhausted'] = bool(any((meta or {}).get('quota_exhausted') for meta in metas))
    combined_meta['last_error'] = next(
        (
            (meta or {}).get('last_error')
            for meta in reversed(metas)
            if (meta or {}).get('last_error')
        ),
        None,
    )
    combined_meta['failed_batch_count'] = sum(
        int((meta or {}).get('failed_batch_count') or 0)
        for meta in metas
    )
    combined_meta['fallback_recovery'] = recovery
    combined_meta['source_evidence'] = _build_combined_autostock_source_evidence(
        metas=metas,
        rows=all_rows,
        target_codes=codes_to_fetch,
        missing_codes=missing_codes,
    )
    return all_rows, combined_meta


def build_drawdown_duration_summary(reason: str | None = None) -> dict:
    """构建 1 年最长回撤观测数分析摘要。"""
    return {
        'enabled': False,
        'reason': reason,
        'checked_count': 0,
        'missing_count': 0,
        'sample_missing_fcodes': [],
        'analysis_window_days': None,
        'nav_buffer_days': None,
        'duration_unit': 'nav_observations',
        'duration_definition': DRAWDOWN_OBSERVATION_DEFINITION,
        'window_policy': 'latest_nav_date; (end - analysis_window_days, end] plus latest NAV at or before start',
        'distribution': {},
        'high_duration_samples': [],
    }


def filter_final_candidates_by_data_completeness(
    df_weighted: pd.DataFrame,
    *,
    required_columns: tuple[str, ...],
    normalize_fcode_fn,
    get_fund_complete_info_fn,
    print_fn=print,
) -> tuple[set[str], dict, pd.DataFrame, pd.DataFrame]:
    """识别关键投资指标不完整的候选基金。"""
    empty_incomplete = pd.DataFrame(columns=['基金代码', '基金名称', *required_columns])
    empty_complete = pd.DataFrame()
    summary = {
        'enabled': False,
        'checked_count': 0,
        'filtered_count': 0,
        'required_columns': list(required_columns),
        'reason': None,
        'sample_fcodes': [],
    }

    if df_weighted.empty or 'fcode' not in df_weighted.columns:
        summary['reason'] = 'empty_candidates'
        return set(), summary, empty_incomplete, empty_complete

    candidate_codes = [
        normalized
        for normalized in (normalize_fcode_fn(value) for value in df_weighted['fcode'].tolist())
        if normalized
    ]
    if not candidate_codes:
        summary['reason'] = 'empty_candidates'
        return set(), summary, empty_incomplete, empty_complete

    try:
        df_complete = get_fund_complete_info_fn(candidate_codes)
    except Exception as exc:
        print_fn(f"  ⚠️ 关键指标完整性校验失败，回退原候选池: {exc}")
        summary['reason'] = 'lookup_failed'
        return set(), summary, empty_incomplete, empty_complete

    if df_complete.empty:
        print_fn("  ⚠️ 关键指标完整性校验未返回有效数据，回退原候选池")
        summary['reason'] = 'lookup_empty'
        return set(), summary, empty_incomplete, empty_complete

    missing_required_columns = [col for col in required_columns if col not in df_complete.columns]
    if missing_required_columns:
        print_fn(
            "  ⚠️ 关键指标完整性校验结果缺少必要列，回退原候选池: "
            f"{', '.join(missing_required_columns)}"
        )
        summary['reason'] = 'missing_required_columns'
        return set(), summary, empty_incomplete, empty_complete

    df_complete = df_complete.copy()
    df_complete['基金代码'] = df_complete['基金代码'].map(normalize_fcode_fn)
    df_complete = df_complete.dropna(subset=['基金代码']).copy()

    returned_codes = {
        code
        for code in df_complete['基金代码'].tolist()
        if code
    }
    missing_codes = [
        code
        for code in candidate_codes
        if code not in returned_codes
    ]

    incomplete_mask = df_complete[list(required_columns)].isna().any(axis=1)
    incomplete_df = df_complete.loc[
        incomplete_mask,
        ['基金代码', '基金名称', *required_columns],
    ].copy()

    if missing_codes:
        name_lookup = {
            normalize_fcode_fn(fcode): name
            for fcode, name in zip(
                df_weighted['fcode'].tolist(),
                df_weighted.get('fund_name', pd.Series([''] * len(df_weighted))).tolist(),
            )
            if normalize_fcode_fn(fcode)
        }
        missing_rows = [
            {
                '基金代码': code,
                '基金名称': name_lookup.get(code) or '',
                **{col: None for col in required_columns},
            }
            for code in missing_codes
        ]
        incomplete_df = pd.concat(
            [incomplete_df, pd.DataFrame(missing_rows)],
            ignore_index=True,
        )

    incomplete_codes = {
        code
        for code in incomplete_df['基金代码'].tolist()
        if code
    }

    summary.update({
        'enabled': True,
        'checked_count': int(len(candidate_codes)),
        'filtered_count': int(len(incomplete_codes)),
        'sample_fcodes': incomplete_df['基金代码'].head(10).tolist(),
    })
    return incomplete_codes, summary, incomplete_df, df_complete


def analyze_candidate_pool_drawdown_duration(
    df_candidates: pd.DataFrame,
    *,
    analysis_window_days: int,
    nav_buffer_days: int = 30,
    build_drawdown_duration_summary_fn,
    normalize_fcode_fn,
    data_loader_factory,
    db_path_getter,
    risk_calculator_factory,
    print_fn=print,
) -> tuple[pd.DataFrame, dict]:
    """在年度窗口及其起点基准净值上计算最长回撤观测数。"""
    summary = build_drawdown_duration_summary_fn()
    summary['analysis_window_days'] = int(analysis_window_days)
    summary['nav_buffer_days'] = int(nav_buffer_days)

    if df_candidates.empty or 'fcode' not in df_candidates.columns:
        summary['reason'] = 'empty_candidates'
        return df_candidates.copy(), summary

    candidate_codes = [
        normalized
        for normalized in (normalize_fcode_fn(value) for value in df_candidates['fcode'].tolist())
        if normalized
    ]
    if not candidate_codes:
        summary['reason'] = 'empty_candidates'
        return df_candidates.copy(), summary

    loader = None
    try:
        loader = data_loader_factory(db_path_getter())
        nav_history_map = loader.get_batch_nav_history(
            candidate_codes,
            days=int(analysis_window_days) + int(nav_buffer_days),
        )
    except Exception as exc:
        print_fn(f"  ⚠️ 1年最长回撤观测数分析失败，跳过该项披露: {exc}")
        summary['reason'] = 'nav_history_lookup_failed'
        return df_candidates.copy(), summary
    finally:
        if loader is not None:
            try:
                loader.close()
            except Exception:
                pass

    calculator = risk_calculator_factory()
    duration_map = {}
    drawdown_map = {}
    missing_codes = []

    for fcode in candidate_codes:
        nav_data = nav_history_map.get(fcode)
        if nav_data is None or nav_data.empty:
            missing_codes.append(fcode)
            continue

        dates = pd.to_datetime(nav_data.get('nav_date', pd.Series(dtype=object)), errors='coerce')
        if dates.empty or dates.isna().any() or dates.duplicated().any():
            missing_codes.append(fcode)
            continue
        nav_window, window_metadata = select_nav_period_window(
            nav_data,
            period_days=int(analysis_window_days),
            window_end_date=dates.max(),
            boundary_lookback_days=int(nav_buffer_days),
        )
        with capture_context(fcode=fcode, purpose='final_drawdown_duration'):
            record_event('screening_risk.final_drawdown_window', window_metadata)
            metrics = calculator.calculate_all_metrics(nav_window, period_years=1.0)
        duration = metrics.get('max_drawdown_duration')
        drawdown = metrics.get('max_drawdown')
        if duration is None:
            missing_codes.append(fcode)
            continue

        duration_map[fcode] = int(duration)
        drawdown_map[fcode] = float(drawdown) if drawdown is not None else None

    enriched_df = df_candidates.copy()
    enriched_df['1年-最长回撤观测数'] = enriched_df['fcode'].map(duration_map)
    enriched_df['1年-最大回撤(重算)'] = enriched_df['fcode'].map(drawdown_map)

    duration_series = pd.to_numeric(
        enriched_df['1年-最长回撤观测数'],
        errors='coerce',
    ).dropna()
    distribution = {}
    if not duration_series.empty:
        distribution = {
            'min': int(duration_series.min()),
            'median': round(float(duration_series.median()), 2),
            'p75': round(float(duration_series.quantile(0.75)), 2),
            'p90': round(float(duration_series.quantile(0.90)), 2),
            'p95': round(float(duration_series.quantile(0.95)), 2),
            'max': int(duration_series.max()),
            'gt_60_count': int((duration_series > 60).sum()),
            'gt_90_count': int((duration_series > 90).sum()),
            'gt_120_count': int((duration_series > 120).sum()),
            'gt_150_count': int((duration_series > 150).sum()),
            'gt_180_count': int((duration_series > 180).sum()),
        }

    high_duration_df = enriched_df.sort_values(
        by=['1年-最长回撤观测数', 'weighted_score'],
        ascending=[False, False],
        na_position='last',
    )
    high_duration_samples = []
    for _, row in high_duration_df.head(10).iterrows():
        duration = row.get('1年-最长回撤观测数')
        if pd.isna(duration):
            continue
        high_duration_samples.append({
            'fcode': row.get('fcode'),
            'fund_name': row.get('fund_name'),
            'fund_category': row.get('fund_category'),
            'weighted_score': round(float(row.get('weighted_score', 0.0) or 0.0), 4),
            'max_drawdown_duration_observations': int(duration),
            'recomputed_max_drawdown': (
                round(float(row.get('1年-最大回撤(重算)')), 4)
                if pd.notna(row.get('1年-最大回撤(重算)'))
                else None
            ),
        })

    summary.update({
        'enabled': True,
        'reason': None,
        'checked_count': int(len(candidate_codes)),
        'missing_count': int(len(missing_codes)),
        'sample_missing_fcodes': missing_codes[:10],
        'distribution': distribution,
        'high_duration_samples': high_duration_samples,
    })
    return enriched_df, summary


def cross_validate_candidate_pool_with_autostock(
    df_candidates: pd.DataFrame,
    *,
    df_local_complete: pd.DataFrame | None = None,
    build_validation_summary_fn,
    normalize_fcode_fn,
    get_fund_complete_info_fn,
    required_local_columns: tuple[str, ...],
    load_autostock_cross_validation_config_fn,
    build_autostock_validation_cache_key_fn,
    load_autostock_validation_cache_fn,
    build_autostock_local_signature_fn,
    build_autostock_cache_entry_map_fn,
    load_recent_autostock_validation_caches_fn,
    autostock_client_cls,
    compare_candidate_with_autostock_fn,
    write_autostock_validation_cache_fn,
    cache_schema_version: str,
    shared_state_path: str | None = None,
) -> tuple[pd.DataFrame, set[str], dict, pd.DataFrame]:
    """使用 AutoStock 对最终候选池做批量交叉验证。"""
    empty_failed = pd.DataFrame(columns=['基金代码', '基金名称', '失败原因', '告警原因'])
    summary = build_validation_summary_fn()

    if df_candidates.empty or 'fcode' not in df_candidates.columns:
        summary['reason'] = 'empty_candidates'
        return df_candidates.head(0).copy(), set(), summary, empty_failed

    candidate_codes = [
        normalize_fcode_fn(value)
        for value in df_candidates['fcode'].tolist()
        if normalize_fcode_fn(value)
    ]
    if not candidate_codes:
        summary['reason'] = 'empty_candidates'
        return df_candidates.head(0).copy(), set(), summary, empty_failed

    df_local = (
        df_local_complete.copy()
        if isinstance(df_local_complete, pd.DataFrame)
        else get_fund_complete_info_fn(candidate_codes)
    )
    if df_local.empty:
        raise RuntimeError('AutoStock 交叉验证前置查询失败：本地候选完整信息为空，已阻断最终主榜生成')

    missing_local_columns = [
        col for col in required_local_columns
        if col not in df_local.columns
    ]
    if missing_local_columns:
        raise RuntimeError(
            "AutoStock 交叉验证前置查询缺少必要列: "
            + ", ".join(missing_local_columns)
        )

    df_local = df_local.copy()
    df_local['基金代码'] = df_local['基金代码'].map(normalize_fcode_fn)
    df_local = df_local.dropna(subset=['基金代码']).drop_duplicates(subset=['基金代码'], keep='first')
    local_lookup = {
        row['基金代码']: row
        for _, row in df_local.iterrows()
    }

    client_config = load_autostock_cross_validation_config_fn()
    if client_config.get('enabled', True) is not True:
        raise RuntimeError(
            'independent_source_unavailable: AutoStock 当前停用；'
            '天天基金与 Eastmoney 主备接口同属一个供应商，不能替代独立来源校验，已保留旧正式榜'
        )
    cache_key = build_autostock_validation_cache_key_fn(
        candidate_codes,
        df_local,
        fund_type=None,
        config_fingerprint={
            'client_class_id': f'{autostock_client_cls.__module__}.{autostock_client_cls.__qualname__}',
            'nav_relative_tolerance_pct': client_config.get('nav_relative_tolerance_pct'),
            'nav_absolute_tolerance': client_config.get('nav_absolute_tolerance'),
            'nav_date_soft_lag_days': client_config.get('nav_date_soft_lag_days'),
            'return_relative_tolerance_pct': client_config.get('return_relative_tolerance_pct'),
            'return_absolute_tolerance': client_config.get('return_absolute_tolerance'),
            'return_small_base_threshold': client_config.get('return_small_base_threshold'),
        },
    )
    cached_payload = load_autostock_validation_cache_fn(cache_key)
    if cached_payload:
        cached_summary = cached_payload.get('summary') if isinstance(cached_payload.get('summary'), dict) else {}
        cached_failed_records = cached_payload.get('failed_records') if isinstance(cached_payload.get('failed_records'), list) else []
        cached_detail_records = cached_payload.get('detail_records') if isinstance(cached_payload.get('detail_records'), list) else []
        cached_failed_codes = {
            normalize_fcode_fn(code)
            for code in cached_payload.get('failed_codes', [])
            if normalize_fcode_fn(code)
        }
        cached_valid_codes = {
            normalize_fcode_fn(code)
            for code in cached_payload.get('valid_codes', [])
            if normalize_fcode_fn(code)
        }
        cached_summary = dict(cached_summary)
        cached_summary.pop('_detail_records', None)
        cached_batch_meta = dict(cached_summary.get('batch_meta') or {})
        cached_batch_meta.update({
            'requested_total': int(len(candidate_codes)),
            'requested_unique_total': int(len(candidate_codes)),
            'requested_batch_size': 0,
            'effective_batch_size': 0,
            'capped': False,
            'returned_total': 0,
            'returned_unique_total': 0,
            'missing_count': 0,
            'missing_codes_preview': [],
            'quota_exhausted': False,
            'last_error': None,
            'max_workers': 0,
            'cache_reused_count': int(len(candidate_codes)),
            'cache_miss_count': 0,
            'fetch_call_needed_count': 0,
            'allowed_call_count': 0,
            'skipped_call_count': 0,
            'reserved_allowed_call_count': 0,
            'can_complete_full_fetch': True,
            'reserve_overridden_for_full_fetch': False,
            'quota_limited': False,
            'quota_remaining': None,
            'quota_reserved_for_automation': 0,
            'max_fetch_calls_per_run': 0,
            'run_call_limit_exceeded': False,
            'quota_unavailable': False,
            'quota_degraded': False,
            'rate_limit_wait_seconds': 0.0,
            'shared_state_path': '',
            'state_read_error': None,
            'budget_precheck_skipped_reason': 'cache_hit',
        })
        cached_source_evidence = _build_autostock_validation_source_evidence(
            candidate_codes=candidate_codes,
            codes_to_fetch=[],
            reused_entries={code: {} for code in candidate_codes},
            batch_meta=cached_batch_meta,
            source_type='cache',
            reason='cache_hit',
            detail_records=cached_detail_records,
        )
        cached_summary['batch_meta'] = cached_batch_meta
        cached_summary['source_evidence'] = cached_source_evidence
        cached_summary['cache_reused_count'] = int(len(candidate_codes))
        cached_summary['cache_hit'] = True
        cached_summary['cache_generated_at'] = cached_payload.get('generated_at')
        validated_df = df_candidates[
            df_candidates['fcode'].isin(cached_valid_codes)
        ].copy()
        failed_df = pd.DataFrame(cached_failed_records) if cached_failed_records else empty_failed
        cached_summary['_detail_records'] = cached_detail_records
        return validated_df, cached_failed_codes, cached_summary, failed_df

    local_signature_map = {
        fcode: build_autostock_local_signature_fn(row)
        for fcode, row in local_lookup.items()
    }
    reused_entries = {}
    recent_cache_map = build_autostock_cache_entry_map_fn(load_recent_autostock_validation_caches_fn())
    for fcode in candidate_codes:
        cache_entry = recent_cache_map.get(fcode)
        if not cache_entry:
            continue
        if cache_entry.get('local_signature') != local_signature_map.get(fcode):
            continue
        result_entry = cache_entry.get('result')
        if not isinstance(result_entry, dict):
            continue
        reused_entries[fcode] = dict(result_entry)

    codes_to_fetch = [fcode for fcode in candidate_codes if fcode not in reused_entries]

    batch_size = max(1, min(
        int(client_config['batch_size']),
        autostock_client_cls.SAFE_FUND_BATCH_SIZE,
    ))
    batch_meta = {
        'requested_total': int(len(candidate_codes)),
        'requested_unique_total': int(len(candidate_codes)),
        'requested_batch_size': batch_size,
        'effective_batch_size': batch_size,
        'capped': False,
        'returned_total': 0,
        'returned_unique_total': 0,
        'missing_count': 0,
        'missing_codes_preview': [],
        'quota_exhausted': False,
        'last_error': None,
        'max_workers': 0,
        'cache_reused_count': int(len(reused_entries)),
        'cache_miss_count': int(len(codes_to_fetch)),
        'fetch_call_needed_count': 0,
        'allowed_call_count': 0,
        'skipped_call_count': 0,
        'reserved_allowed_call_count': 0,
        'can_complete_full_fetch': True,
        'reserve_overridden_for_full_fetch': False,
        'quota_limited': False,
        'quota_remaining': None,
        'quota_reserved_for_automation': 0,
        'max_fetch_calls_per_run': 0,
        'run_call_limit_exceeded': False,
        'quota_unavailable': False,
        'quota_degraded': False,
        'rate_limit_wait_seconds': 0.0,
        'shared_state_path': '',
        'state_read_error': None,
        'budget_precheck_skipped_reason': None,
    }
    autostock_lookup = {}
    if codes_to_fetch:
        needed_call_count = (len(codes_to_fetch) + batch_size - 1) // batch_size
        quota_snapshot = autostock_budget_helpers.read_autostock_quota_snapshot(
            shared_state_path=shared_state_path,
            max_calls_per_hour=int(client_config.get('max_calls_per_hour') or 0),
            min_interval_seconds=float(client_config.get('min_interval') or 0.0),
            fail_closed=True,
        )
        budget_plan = autostock_budget_helpers.plan_autostock_call_budget(
            quota_snapshot=quota_snapshot,
            needed_call_count=needed_call_count,
            reserve_calls=int(client_config.get('budget_reserve_calls') or 0),
            max_fetch_calls_per_run=int(client_config.get('max_fetch_calls_per_run') or 0),
        )
        batch_meta.update({
            'fetch_call_needed_count': int(needed_call_count),
            'allowed_call_count': int(budget_plan.get('allowed_call_count') or 0),
            'skipped_call_count': int(budget_plan.get('skipped_call_count') or 0),
            'reserved_allowed_call_count': int(budget_plan.get('reserved_allowed_call_count') or 0),
            'can_complete_full_fetch': bool(budget_plan.get('can_complete_full_fetch')),
            'reserve_overridden_for_full_fetch': bool(budget_plan.get('reserve_overridden_for_full_fetch')),
            'quota_limited': bool(budget_plan.get('quota_limited')),
            'quota_remaining': budget_plan.get('quota_remaining'),
            'quota_reserved_for_automation': int(
                budget_plan.get('quota_reserved_for_automation') or 0
            ),
            'max_fetch_calls_per_run': int(budget_plan.get('max_fetch_calls_per_run') or 0),
            'run_call_limit_exceeded': bool(budget_plan.get('run_call_limit_exceeded')),
            'quota_unavailable': bool(budget_plan.get('quota_unavailable')),
            'quota_degraded': bool(budget_plan.get('quota_degraded')),
            'rate_limit_wait_seconds': float(quota_snapshot.get('rate_limit_wait_seconds') or 0.0),
            'shared_state_path': quota_snapshot.get('shared_state_path') or '',
            'state_read_error': quota_snapshot.get('state_read_error'),
            'budget_precheck_skipped_reason': None,
        })
        if bool(budget_plan.get('quota_degraded')):
            source_evidence = _build_autostock_validation_source_evidence(
                candidate_codes=candidate_codes,
                codes_to_fetch=codes_to_fetch,
                reused_entries=reused_entries,
                batch_meta=batch_meta,
                source_type='budget_degraded',
                reason='autostock_budget_unavailable',
                detail_records=[],
            )
            summary.update({
                'enabled': True,
                'reason': 'autostock_budget_unavailable',
                'checked_count': int(len(candidate_codes)),
                'filtered_count': 0,
                'passed_count': 0,
                'sample_fcodes': [],
                'reason_counts': [],
                'warning_counts': [],
                'batch_meta': dict(batch_meta),
                'source_evidence': source_evidence,
                'cache_reused_count': int(len(reused_entries)),
                'cache_hit': False,
            })
            message = (
                'AutoStock 交叉验证预算不足，已阻断最终主榜生成: '
                f"reason=autostock_budget_unavailable, "
                f"needed_call_count={budget_plan.get('needed_call_count')}, "
                f"allowed_call_count={budget_plan.get('allowed_call_count')}, "
                f"quota_remaining={budget_plan.get('quota_remaining')}, "
                f"reserve_calls={budget_plan.get('quota_reserved_for_automation')}, "
                f"max_fetch_calls_per_run={budget_plan.get('max_fetch_calls_per_run')}, "
                f"shared_state_path={quota_snapshot.get('shared_state_path') or ''}"
            )
            raise RuntimeError(message)

        client = autostock_client_cls(
            base_url=client_config['base_url'],
            min_interval=client_config['min_interval'],
            timeout=client_config['timeout'],
            max_calls_per_hour=(
                int(client_config['max_calls_per_hour'])
                if int(client_config['max_calls_per_hour']) > 0 else None
            ),
            shared_state_path=shared_state_path,
        )

        try:
            autostock_rows, fetched_batch_meta = client.get_fund_batch_with_meta(codes_to_fetch, batch_size=batch_size)
        except Exception as exc:
            raise RuntimeError(f'AutoStock 交叉验证失败，已阻断最终主榜生成: {exc}') from exc

        autostock_rows, fetched_batch_meta = _recover_autostock_missing_rows(
            client=client,
            initial_rows=autostock_rows,
            initial_meta=dict(fetched_batch_meta or {}),
            codes_to_fetch=codes_to_fetch,
            batch_size=batch_size,
            normalize_fcode_fn=normalize_fcode_fn,
        )

        batch_meta.update(dict(fetched_batch_meta or {}))
        batch_meta['requested_total'] = int(len(candidate_codes))
        batch_meta['requested_unique_total'] = int(len(candidate_codes))
        batch_meta['cache_reused_count'] = int(len(reused_entries))
        batch_meta['cache_miss_count'] = int(len(codes_to_fetch))

        if batch_meta.get('quota_exhausted'):
            raise RuntimeError('AutoStock 免费额度/频率限制触发，无法完成最终主榜交叉验证，已阻断输出')

        if batch_meta.get('circuit_breaker_open'):
            raise RuntimeError(
                'AutoStock 传输链路连续失败，已触发快速熔断并阻断最终主榜生成: '
                f"reason=autostock_transport_circuit_open, "
                f"attempted_batch_count={batch_meta.get('attempted_batch_count')}, "
                f"total_batch_count={batch_meta.get('total_batch_count')}, "
                f"unrequested_count={batch_meta.get('unrequested_count')}, "
                f"max_consecutive_transport_failure_count="
                f"{batch_meta.get('max_consecutive_transport_failure_count')}, "
                f"last_error={batch_meta.get('last_error')}"
            )

        residual_missing_count = int(batch_meta.get('missing_count', 0) or 0)
        residual_missing_ratio = (
            residual_missing_count / len(candidate_codes)
            if candidate_codes else 0.0
        )
        residual_missing_max_count = int(client_config.get('residual_missing_max_count', 2) or 2)
        residual_missing_max_ratio = float(client_config.get('residual_missing_max_ratio', 0.002) or 0.002)
        residual_missing_demoted = bool(
            residual_missing_count > 0
            and residual_missing_count <= residual_missing_max_count
            and residual_missing_ratio <= residual_missing_max_ratio
            and not batch_meta.get('last_error')
        )
        batch_meta.update({
            'residual_missing_count': residual_missing_count,
            'residual_missing_ratio': round(residual_missing_ratio, 8),
            'residual_missing_max_count': residual_missing_max_count,
            'residual_missing_max_ratio': residual_missing_max_ratio,
            'residual_missing_demoted_to_hard_block': residual_missing_demoted,
        })

        if residual_missing_count > 0 and not residual_missing_demoted:
            recovery = batch_meta.get('fallback_recovery') if isinstance(batch_meta.get('fallback_recovery'), dict) else {}
            raise RuntimeError(
                "AutoStock 批量交叉验证未完整返回全部候选数据，"
                f"missing_count={batch_meta.get('missing_count')}, "
                f"recovered_count={recovery.get('recovered_count')}, "
                f"remaining_missing_count={recovery.get('remaining_missing_count')}, "
                f"last_error={batch_meta.get('last_error')}"
            )

        autostock_lookup = {
            normalize_fcode_fn(row.get('code')): row
            for row in autostock_rows
            if isinstance(row, dict) and normalize_fcode_fn(row.get('code'))
        }
    else:
        batch_meta['effective_batch_size'] = 0
        batch_meta['requested_batch_size'] = 0
        batch_meta['budget_precheck_skipped_reason'] = 'all_recent_cache_reused'

    failed_records = []
    detail_records = []
    failed_codes = set()
    warning_counter = Counter()
    reason_counter = Counter()
    valid_codes = []

    for fcode in candidate_codes:
        local_row = local_lookup.get(fcode)
        if local_row is None:
            failed_codes.add(fcode)
            failed_records.append({
                '基金代码': fcode,
                '基金名称': '',
                '失败原因': 'local_lookup_missing',
                '告警原因': '',
            })
            reason_counter['local_lookup_missing'] += 1
            continue

        reused_entry = reused_entries.get(fcode)
        if reused_entry:
            detail = dict(reused_entry.get('details') or {})
            hard_reasons = list(reused_entry.get('hard_reasons') or [])
            warning_reasons = list(reused_entry.get('warning_reasons') or [])
            detail_records.append(detail)
            for warning_reason in warning_reasons:
                warning_counter[warning_reason] += 1
            if not hard_reasons:
                valid_codes.append(fcode)
                continue

            failed_codes.add(fcode)
            for hard_reason in hard_reasons:
                reason_counter[hard_reason] += 1
            failed_records.append(detail)
            continue

        autostock_row = autostock_lookup.get(fcode)
        if autostock_row is None:
            failed_codes.add(fcode)
            failed_records.append({
                '基金代码': fcode,
                '基金名称': local_row.get('基金名称'),
                '失败原因': 'missing_in_autostock',
                '告警原因': '',
            })
            reason_counter['missing_in_autostock'] += 1
            continue

        comparison = compare_candidate_with_autostock_fn(local_row, autostock_row, client_config)
        detail_records.append(comparison['details'])
        for warning_reason in comparison['warning_reasons']:
            warning_counter[warning_reason] += 1

        if comparison['passed']:
            valid_codes.append(fcode)
            continue

        failed_codes.add(fcode)
        for hard_reason in comparison['hard_reasons']:
            reason_counter[hard_reason] += 1
        failed_records.append(comparison['details'])

    validated_df = df_candidates[
        df_candidates['fcode'].isin(set(valid_codes))
    ].copy()

    failed_df = pd.DataFrame(failed_records) if failed_records else empty_failed
    failed_sample_codes = (
        failed_df['基金代码'].dropna().astype(str).map(normalize_fcode_fn).dropna().head(10).tolist()
        if not failed_df.empty and '基金代码' in failed_df.columns
        else []
    )
    summary.update({
        'enabled': True,
        'reason': None,
        'checked_count': int(len(candidate_codes)),
        'filtered_count': int(len(failed_codes)),
        'passed_count': int(len(valid_codes)),
        'sample_fcodes': failed_sample_codes[:10],
        'reason_counts': [
            {'reason': reason, 'count': int(count)}
            for reason, count in reason_counter.most_common(8)
        ],
        'warning_counts': [
            {'reason': reason, 'count': int(count)}
            for reason, count in warning_counter.most_common(4)
        ],
        'batch_meta': dict(batch_meta),
        'source_evidence': _build_autostock_validation_source_evidence(
            candidate_codes=candidate_codes,
            codes_to_fetch=codes_to_fetch,
            reused_entries=reused_entries,
            batch_meta=batch_meta,
            source_type='cache' if reused_entries and not codes_to_fetch else 'api_batch',
            reason='all_recent_cache_reused' if reused_entries and not codes_to_fetch else None,
            detail_records=detail_records,
        ),
        'cache_reused_count': int(len(reused_entries)),
    })
    summary['cache_hit'] = False
    if reused_entries and not codes_to_fetch:
        summary['cache_hit'] = True
        summary['cache_generated_at'] = max(
            (
                str(entry.get('generated_at') or '').strip()
                for entry in reused_entries.values()
                if str(entry.get('generated_at') or '').strip()
            ),
            default=None,
        )
    summary['_detail_records'] = detail_records
    cache_summary = dict(summary)
    cache_summary.pop('_detail_records', None)
    result_by_fcode = {}
    for detail in detail_records:
        fcode = normalize_fcode_fn(detail.get('基金代码'))
        if not fcode:
            continue
        hard_reasons = [
            item for item in str(detail.get('失败原因') or '').split('；')
            if item
        ]
        warning_reasons = [
            item for item in str(detail.get('告警原因') or '').split('；')
            if item
        ]
        result_by_fcode[fcode] = {
            'details': detail,
            'hard_reasons': hard_reasons,
            'warning_reasons': warning_reasons,
            'passed': len(hard_reasons) == 0,
        }
    cache_payload = {
        'schema_version': str(cache_schema_version or '').strip(),
        'generated_at': datetime.now().isoformat(),
        'fund_type': str(df_local.get('基金类型').iloc[0] if '基金类型' in df_local.columns and not df_local.empty else ''),
        'candidate_codes': list(candidate_codes),
        'config_fingerprint': {
            'client_class_id': f'{autostock_client_cls.__module__}.{autostock_client_cls.__qualname__}',
            'nav_relative_tolerance_pct': client_config.get('nav_relative_tolerance_pct'),
            'nav_absolute_tolerance': client_config.get('nav_absolute_tolerance'),
            'nav_date_soft_lag_days': client_config.get('nav_date_soft_lag_days'),
            'return_relative_tolerance_pct': client_config.get('return_relative_tolerance_pct'),
            'return_absolute_tolerance': client_config.get('return_absolute_tolerance'),
            'return_small_base_threshold': client_config.get('return_small_base_threshold'),
        },
        'summary': cache_summary,
        'failed_codes': sorted(failed_codes),
        'valid_codes': sorted(set(valid_codes)),
        'failed_records': failed_df.to_dict(orient='records') if not failed_df.empty else [],
        'detail_records': detail_records,
        'local_row_map': dict(local_signature_map),
        'result_by_fcode': result_by_fcode,
    }
    write_autostock_validation_cache_fn(cache_key, cache_payload)
    return validated_df, failed_codes, summary, failed_df
