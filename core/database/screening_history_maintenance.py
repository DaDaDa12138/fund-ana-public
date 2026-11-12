#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选历史维护工具

设计目标：
1. 控制 screening_decisions / screening_picks 持续膨胀
2. 默认安全：优先预览，不默认删除 screening_runs 汇总
3. 保护最近运行和每个 fund_type + period_years 的最新成功运行

注意：
- 删除 SQLite 行不会立即缩小数据库文件体积，若需要真实回收磁盘空间，还需要后续执行 VACUUM。
- 本模块默认不执行 VACUUM，避免在运行环境中引入更长的独占锁窗口。
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from database.connection import get_db_manager
from database.models import ScreeningDecision, ScreeningPick, ScreeningRun
from database.screening_decision_summaries import delete_screening_decision_summaries_for_run_ids
from database.screening_pick_metric_snapshots import delete_pick_metric_snapshots_for_run_ids
from database.screening_run_storage_stats import (
    count_detail_rows_by_run_ids,
    count_detail_rows_for_run_ids,
    fetch_materialized_count_map,
    fetch_materialized_counts,
    mark_storage_stats_after_detail_cleanup,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScreeningHistoryCleanupConfig:
    retention_days: int = 30
    dry_run: bool = True
    delete_runs: bool = False
    keep_latest_success_per_period: bool = True
    max_sample_runs: int = 20
    delete_batch_run_limit: int = 100


def _serialize_run(row: ScreeningRun) -> Dict:
    return {
        'run_id': row.run_id,
        'status': row.status,
        'fund_type': row.fund_type,
        'period_years': row.period_years,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'finished_at': row.finished_at.isoformat() if row.finished_at else None,
        'fund_count_pass': row.fund_count_pass,
    }


def _collect_runs(session) -> List[ScreeningRun]:
    return session.query(ScreeningRun).order_by(ScreeningRun.started_at.desc()).all()


def _run_event_at(row: ScreeningRun) -> datetime:
    return row.finished_at or row.started_at or datetime.min


def _normalize_delete_batch_run_limit(delete_batch_run_limit: Optional[int]) -> int:
    try:
        value = int(delete_batch_run_limit or 100)
    except (TypeError, ValueError):
        value = 100
    return max(1, value)


def _iter_run_id_batches(candidate_run_ids: Sequence[str], delete_batch_run_limit: int):
    batch_limit = _normalize_delete_batch_run_limit(delete_batch_run_limit)
    for index in range(0, len(candidate_run_ids), batch_limit):
        yield candidate_run_ids[index:index + batch_limit]


def _count_candidate_detail_rows(session, candidate_run_ids: Sequence[str]) -> Tuple[int, int, Dict]:
    """统计候选 run 的 decision/pick 行数，优先使用物化统计表。"""
    normalized_run_ids = [str(run_id).strip() for run_id in candidate_run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return 0, 0, {
            'count_source': 'none',
            'storage_stats_covered_run_count': 0,
            'storage_stats_missing_run_count': 0,
        }

    materialized_decisions, materialized_picks, covered_count, missing_run_ids = fetch_materialized_counts(
        session,
        normalized_run_ids,
    )
    if missing_run_ids:
        fallback_decisions, fallback_picks = count_detail_rows_for_run_ids(session, missing_run_ids)
    else:
        fallback_decisions, fallback_picks = 0, 0

    count_source = 'materialized' if not missing_run_ids and covered_count else 'detail_scan'
    if covered_count and missing_run_ids:
        count_source = 'mixed'

    return (
        int(materialized_decisions + fallback_decisions),
        int(materialized_picks + fallback_picks),
        {
            'count_source': count_source,
            'storage_stats_covered_run_count': int(covered_count),
            'storage_stats_missing_run_count': int(len(missing_run_ids)),
            'storage_stats_missing_run_samples': list(missing_run_ids[:20]),
        },
    )


def _collect_candidate_detail_counts(session, candidate_run_ids: Sequence[str]) -> Tuple[Dict[str, Tuple[int, int]], Dict]:
    """Return per-run detail counts and counting metadata for cleanup candidates."""
    normalized_run_ids = [str(run_id).strip() for run_id in candidate_run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return {}, {
            'count_source': 'none',
            'storage_stats_covered_run_count': 0,
            'storage_stats_missing_run_count': 0,
            'storage_stats_missing_run_samples': [],
        }

    materialized_counts, missing_run_ids = fetch_materialized_count_map(
        session,
        normalized_run_ids,
    )
    detail_counts = count_detail_rows_by_run_ids(session, missing_run_ids) if missing_run_ids else {}
    zero_materialized_run_ids = [
        run_id
        for run_id, counts in materialized_counts.items()
        if sum(counts) <= 0
    ]
    # Materialized zeroes are normally produced by cleanup apply. Verify them
    # against detail tables so stale stats cannot hide real rows.
    if zero_materialized_run_ids:
        detail_counts.update(count_detail_rows_by_run_ids(session, zero_materialized_run_ids))
    count_map = {**materialized_counts, **detail_counts}

    covered_count = len(materialized_counts)
    if not missing_run_ids and covered_count:
        count_source = 'materialized'
    elif covered_count and missing_run_ids:
        count_source = 'mixed'
    else:
        count_source = 'detail_scan'

    return count_map, {
        'count_source': count_source,
        'storage_stats_covered_run_count': int(covered_count),
        'storage_stats_missing_run_count': int(len(missing_run_ids)),
        'storage_stats_missing_run_samples': list(missing_run_ids[:20]),
    }


def _delete_run_scoped_rows_in_batches(
    manager,
    candidate_run_ids: Sequence[str],
    *,
    delete_runs: bool,
    delete_batch_run_limit: int,
    task_name_prefix: str,
) -> Dict:
    batch_limit = _normalize_delete_batch_run_limit(delete_batch_run_limit)
    deleted_decisions = 0
    deleted_payload_objects = 0
    deleted_picks = 0
    deleted_pick_metric_snapshots = 0
    deleted_decision_reason_summaries = 0
    deleted_run_latest_activities = 0
    deleted_runs = 0
    delete_batch_count = 0

    for batch_index, batch_run_ids in enumerate(
        _iter_run_id_batches(candidate_run_ids, batch_limit),
        start=1,
    ):
        with manager.get_session(
            task_name=f"{task_name_prefix}_batch_{batch_index}",
            readonly=False,
        ) as session:
            batch_deleted_decisions = session.query(ScreeningDecision).filter(
                ScreeningDecision.run_id.in_(batch_run_ids)
            ).delete(synchronize_session=False)
            from database.screening_decision_payloads import collect_unreferenced_payloads

            deleted_payload_objects += collect_unreferenced_payloads(session.connection().connection.driver_connection)
            batch_deleted_picks = session.query(ScreeningPick).filter(
                ScreeningPick.run_id.in_(batch_run_ids)
            ).delete(synchronize_session=False)
            batch_deleted_runs = 0
            if delete_runs:
                batch_deleted_runs = session.query(ScreeningRun).filter(
                    ScreeningRun.run_id.in_(batch_run_ids)
                ).delete(synchronize_session=False)
            batch_metric_sync = delete_pick_metric_snapshots_for_run_ids(
                session,
                batch_run_ids,
            )
            batch_decision_summary_sync = delete_screening_decision_summaries_for_run_ids(
                session,
                batch_run_ids,
            )
            mark_storage_stats_after_detail_cleanup(
                session,
                batch_run_ids,
                delete_runs=delete_runs,
                stats_source=task_name_prefix[:40],
            )

        delete_batch_count = batch_index
        deleted_decisions += batch_deleted_decisions or 0
        deleted_picks += batch_deleted_picks or 0
        deleted_pick_metric_snapshots += int(batch_metric_sync.get('deleted_count') or 0)
        deleted_decision_reason_summaries += int(
            batch_decision_summary_sync.get('deleted_reason_count') or 0
        )
        deleted_run_latest_activities += int(
            batch_decision_summary_sync.get('deleted_latest_activity_count') or 0
        )
        deleted_runs += batch_deleted_runs or 0

        logger.warning(
            "筛选历史批次清理已执行: task=%s batch=%d batch_run_count=%d deleted_decisions=%d deleted_picks=%d deleted_pick_metric_snapshots=%d deleted_decision_reason_summaries=%d deleted_run_latest_activities=%d deleted_runs=%d",
            task_name_prefix,
            batch_index,
            len(batch_run_ids),
            batch_deleted_decisions or 0,
            batch_deleted_picks or 0,
            int(batch_metric_sync.get('deleted_count') or 0),
            int(batch_decision_summary_sync.get('deleted_reason_count') or 0),
            int(batch_decision_summary_sync.get('deleted_latest_activity_count') or 0),
            batch_deleted_runs or 0,
        )

    return {
        'deleted_decisions': deleted_decisions,
        'deleted_payload_objects': deleted_payload_objects,
        'deleted_picks': deleted_picks,
        'deleted_pick_metric_snapshots': deleted_pick_metric_snapshots,
        'deleted_decision_reason_summaries': deleted_decision_reason_summaries,
        'deleted_run_latest_activities': deleted_run_latest_activities,
        'deleted_runs': deleted_runs,
        'deleted_total': deleted_decisions + deleted_picks + deleted_runs,
        'delete_batch_run_limit': batch_limit,
        'delete_batch_count': delete_batch_count,
    }


def _collect_protected_run_ids(
    runs: Sequence[ScreeningRun],
    cutoff_dt: datetime,
    keep_latest_success_per_period: bool = True,
) -> Tuple[set, Dict[str, int]]:
    protected_run_ids = set()
    reasons = Counter()

    for row in runs:
        if row.started_at and row.started_at >= cutoff_dt:
            if row.run_id not in protected_run_ids:
                protected_run_ids.add(row.run_id)
                reasons['recent_window'] += 1

    if runs:
        latest_run = runs[0]
        if latest_run.run_id not in protected_run_ids:
            protected_run_ids.add(latest_run.run_id)
            reasons['latest_overall'] += 1

    if keep_latest_success_per_period:
        latest_success_keys = set()
        for row in runs:
            if row.status != 'success':
                continue
            key = (row.fund_type or '', row.period_years)
            if key in latest_success_keys:
                continue
            latest_success_keys.add(key)
            if row.run_id not in protected_run_ids:
                protected_run_ids.add(row.run_id)
                reasons['latest_success_per_period'] += 1

    return protected_run_ids, dict(reasons)


def build_screening_history_cleanup_plan(
    session,
    retention_days: int = 30,
    keep_latest_success_per_period: bool = True,
    max_sample_runs: int = 20,
) -> Dict:
    """
    构建筛选历史清理计划（只读）。
    """
    now = datetime.now()
    cutoff_dt = now - timedelta(days=retention_days)
    runs = _collect_runs(session)
    protected_run_ids, protected_reason_counts = _collect_protected_run_ids(
        runs,
        cutoff_dt=cutoff_dt,
        keep_latest_success_per_period=keep_latest_success_per_period,
    )

    candidate_rows = [
        row for row in runs
        if row.run_id not in protected_run_ids
        and row.status != 'running'
        and row.started_at is not None
        and row.started_at < cutoff_dt
    ]
    raw_candidate_run_ids = [row.run_id for row in candidate_rows]
    detail_count_map, count_meta = _collect_candidate_detail_counts(session, raw_candidate_run_ids)
    zero_detail_run_ids = [
        run_id
        for run_id in raw_candidate_run_ids
        if sum(detail_count_map.get(run_id, (0, 0))) <= 0
    ]
    candidate_rows = [
        row
        for row in candidate_rows
        if sum(detail_count_map.get(row.run_id, (0, 0))) > 0
    ]
    candidate_run_ids = [row.run_id for row in candidate_rows]
    decision_count = sum(int(detail_count_map.get(run_id, (0, 0))[0]) for run_id in candidate_run_ids)
    pick_count = sum(int(detail_count_map.get(run_id, (0, 0))[1]) for run_id in candidate_run_ids)

    status_counter = Counter((row.status or 'unknown') for row in candidate_rows)
    fund_type_counter = Counter((row.fund_type or 'unknown') for row in candidate_rows)

    return {
        'generated_at': now.isoformat(),
        'cutoff_date': cutoff_dt.isoformat(),
        'retention_days': retention_days,
        'keep_latest_success_per_period': keep_latest_success_per_period,
        'protected_run_count': len(protected_run_ids),
        'protected_reason_counts': protected_reason_counts,
        'candidate_run_count': len(candidate_rows),
        'raw_candidate_run_count': len(raw_candidate_run_ids),
        'zero_detail_candidate_run_count': len(zero_detail_run_ids),
        'zero_detail_candidate_run_samples': zero_detail_run_ids[:max_sample_runs],
        'candidate_status_counts': dict(status_counter),
        'candidate_fund_type_counts': dict(fund_type_counter),
        'candidate_run_samples': [_serialize_run(row) for row in candidate_rows[:max_sample_runs]],
        'decisions_to_delete': decision_count,
        'picks_to_delete': pick_count,
        'runs_to_delete': len(candidate_rows),
        'estimated_rows_to_delete': decision_count + pick_count,
        'vacuum_recommended': bool(decision_count or pick_count),
        **count_meta,
        '_candidate_run_ids': candidate_run_ids,
    }


def build_recent_duplicate_success_cleanup_plan(
    session,
    recent_days: int = 30,
    fund_types: Optional[Sequence[str]] = None,
    max_sample_runs: int = 20,
) -> Dict:
    """
    构建近窗重复成功 run 清理计划（只读）。

    规则：
    1. 仅统计最近 N 天内的 success run
    2. 对每个 (fund_type, period_years) 仅保留最新一条成功运行
    3. 其余 success run 视为重复候选
    """
    now = datetime.now()
    cutoff_dt = now - timedelta(days=recent_days)
    normalized_fund_types = [
        str(item).strip()
        for item in (fund_types or [])
        if str(item).strip()
    ]

    query = session.query(ScreeningRun).filter(
        ScreeningRun.status == 'success',
        ScreeningRun.started_at.isnot(None),
        ScreeningRun.started_at >= cutoff_dt,
    )
    if normalized_fund_types:
        query = query.filter(ScreeningRun.fund_type.in_(normalized_fund_types))

    recent_success_runs = query.all()
    sorted_runs = sorted(recent_success_runs, key=_run_event_at, reverse=True)

    kept_keys = set()
    kept_rows: List[ScreeningRun] = []
    candidate_rows: List[ScreeningRun] = []
    for row in sorted_runs:
        key = (row.fund_type or 'unknown', row.period_years)
        if key in kept_keys:
            candidate_rows.append(row)
            continue
        kept_keys.add(key)
        kept_rows.append(row)

    raw_candidate_run_ids = [row.run_id for row in candidate_rows]
    detail_count_map, count_meta = _collect_candidate_detail_counts(session, raw_candidate_run_ids)
    zero_detail_run_ids = [
        run_id
        for run_id in raw_candidate_run_ids
        if sum(detail_count_map.get(run_id, (0, 0))) <= 0
    ]
    candidate_rows = [
        row
        for row in candidate_rows
        if sum(detail_count_map.get(row.run_id, (0, 0))) > 0
    ]
    candidate_run_ids = [row.run_id for row in candidate_rows]
    decision_count = sum(int(detail_count_map.get(run_id, (0, 0))[0]) for run_id in candidate_run_ids)
    pick_count = sum(int(detail_count_map.get(run_id, (0, 0))[1]) for run_id in candidate_run_ids)

    candidate_fund_type_counts = Counter((row.fund_type or 'unknown') for row in candidate_rows)
    candidate_period_counts = Counter(
        f"{row.fund_type or 'unknown'}:{row.period_years}"
        for row in candidate_rows
    )
    candidate_day_counts = Counter(
        (row.started_at.date().isoformat() if row.started_at else 'unknown')
        for row in candidate_rows
    )

    return {
        'generated_at': now.isoformat(),
        'cutoff_date': cutoff_dt.isoformat(),
        'recent_days': recent_days,
        'fund_types': normalized_fund_types,
        'recent_success_run_count': len(sorted_runs),
        'kept_latest_run_count': len(kept_rows),
        'candidate_run_count': len(candidate_rows),
        'raw_candidate_run_count': len(raw_candidate_run_ids),
        'zero_detail_candidate_run_count': len(zero_detail_run_ids),
        'zero_detail_candidate_run_samples': zero_detail_run_ids[:max_sample_runs],
        'candidate_fund_type_counts': dict(candidate_fund_type_counts),
        'candidate_period_counts': dict(candidate_period_counts),
        'candidate_day_counts': dict(candidate_day_counts),
        'candidate_run_samples': [_serialize_run(row) for row in candidate_rows[:max_sample_runs]],
        'kept_run_samples': [_serialize_run(row) for row in kept_rows[:max_sample_runs]],
        'decisions_to_delete': decision_count,
        'picks_to_delete': pick_count,
        'runs_to_delete': len(candidate_rows),
        'estimated_rows_to_delete': decision_count + pick_count,
        **count_meta,
        '_candidate_run_ids': candidate_run_ids,
    }


def cleanup_recent_duplicate_success_runs(
    db_manager=None,
    recent_days: int = 30,
    fund_types: Optional[Sequence[str]] = None,
    dry_run: bool = True,
    delete_runs: bool = False,
    max_sample_runs: int = 20,
    delete_batch_run_limit: int = 100,
) -> Dict:
    """
    预览或清理近窗重复成功 run 的明细。
    """
    manager = db_manager or get_db_manager()
    task_name = 'preview_recent_duplicate_success_runs'
    with manager.get_session(task_name=task_name, readonly=True) as session:
        plan = build_recent_duplicate_success_cleanup_plan(
            session,
            recent_days=recent_days,
            fund_types=fund_types,
            max_sample_runs=max_sample_runs,
        )
        candidate_run_ids = plan.pop('_candidate_run_ids', [])
    result = {
        **plan,
        'dry_run': dry_run,
        'delete_runs': delete_runs,
        'deleted_decisions': 0,
        'deleted_picks': 0,
        'deleted_pick_metric_snapshots': 0,
        'deleted_decision_reason_summaries': 0,
        'deleted_run_latest_activities': 0,
        'deleted_runs': 0,
        'deleted_total': 0,
        'delete_batch_run_limit': _normalize_delete_batch_run_limit(delete_batch_run_limit),
        'delete_batch_count': 0,
    }

    if dry_run or not candidate_run_ids:
        logger.info(
            "近窗重复成功 run 预览: candidates=%d decisions=%d picks=%d dry_run=%s fund_types=%s recent_days=%d",
            result['candidate_run_count'],
            result['decisions_to_delete'],
            result['picks_to_delete'],
            dry_run,
            ','.join(fund_types or []) if fund_types else '<all>',
            recent_days,
        )
        return result

    result.update(
        _delete_run_scoped_rows_in_batches(
            manager,
            candidate_run_ids,
            delete_runs=delete_runs,
            delete_batch_run_limit=delete_batch_run_limit,
            task_name_prefix='cleanup_recent_duplicate_success_runs',
        )
    )

    logger.warning(
        "近窗重复成功 run 清理已执行: candidate_runs=%d deleted_decisions=%d deleted_picks=%d deleted_runs=%d delete_batch_count=%d batch_run_limit=%d fund_types=%s recent_days=%d",
        result['candidate_run_count'],
        result['deleted_decisions'],
        result['deleted_picks'],
        result['deleted_runs'],
        result['delete_batch_count'],
        result['delete_batch_run_limit'],
        ','.join(fund_types or []) if fund_types else '<all>',
        recent_days,
    )
    return result


def cleanup_screening_history(
    db_manager=None,
    config: Optional[ScreeningHistoryCleanupConfig] = None,
    retention_days: int = 30,
    dry_run: bool = True,
    delete_runs: bool = False,
    keep_latest_success_per_period: bool = True,
    max_sample_runs: int = 20,
    delete_batch_run_limit: int = 100,
) -> Dict:
    """
    清理或预览筛选历史。
    """
    manager = db_manager or get_db_manager()
    cfg = config or ScreeningHistoryCleanupConfig(
        retention_days=retention_days,
        dry_run=dry_run,
        delete_runs=delete_runs,
        keep_latest_success_per_period=keep_latest_success_per_period,
        max_sample_runs=max_sample_runs,
        delete_batch_run_limit=delete_batch_run_limit,
    )

    with manager.get_session(task_name='preview_screening_history_cleanup', readonly=True) as session:
        plan = build_screening_history_cleanup_plan(
            session,
            retention_days=cfg.retention_days,
            keep_latest_success_per_period=cfg.keep_latest_success_per_period,
            max_sample_runs=cfg.max_sample_runs,
        )
        candidate_run_ids = plan.pop('_candidate_run_ids', [])
    result = {
        **plan,
        'dry_run': cfg.dry_run,
        'delete_runs': cfg.delete_runs,
        'deleted_decisions': 0,
        'deleted_picks': 0,
        'deleted_pick_metric_snapshots': 0,
        'deleted_decision_reason_summaries': 0,
        'deleted_run_latest_activities': 0,
        'deleted_runs': 0,
        'deleted_total': 0,
        'delete_batch_run_limit': _normalize_delete_batch_run_limit(cfg.delete_batch_run_limit),
        'delete_batch_count': 0,
    }

    if cfg.dry_run or not candidate_run_ids:
        logger.info(
            "筛选历史维护预览: candidates=%d decisions=%d picks=%d dry_run=%s",
            result['candidate_run_count'],
            result['decisions_to_delete'],
            result['picks_to_delete'],
            cfg.dry_run,
        )
        return result

    result.update(
        _delete_run_scoped_rows_in_batches(
            manager,
            candidate_run_ids,
            delete_runs=cfg.delete_runs,
            delete_batch_run_limit=cfg.delete_batch_run_limit,
            task_name_prefix='cleanup_screening_history',
        )
    )

    logger.warning(
        "筛选历史维护已执行: candidate_runs=%d deleted_decisions=%d deleted_picks=%d deleted_pick_metric_snapshots=%d deleted_runs=%d delete_batch_count=%d batch_run_limit=%d",
        result['candidate_run_count'],
        result['deleted_decisions'],
        result['deleted_picks'],
        result['deleted_pick_metric_snapshots'],
        result['deleted_runs'],
        result['delete_batch_count'],
        result['delete_batch_run_limit'],
    )
    return result
