#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""筛选 run 存储统计物化表维护。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Sequence

from sqlalchemy import func, inspect

from database.connection import get_db_manager
from database.models import (
    ScreeningDecision,
    ScreeningPick,
    ScreeningRun,
    ScreeningRunStorageStats,
)


def _normalize_run_ids(run_ids: Optional[Sequence[str]]) -> list[str]:
    normalized = []
    seen = set()
    for item in run_ids or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _iter_chunks(values: Sequence[str], chunk_size: int = 500):
    size = max(1, int(chunk_size or 500))
    for start in range(0, len(values), size):
        yield list(values[start:start + size])


def storage_stats_table_exists(session) -> bool:
    """检查物化统计表是否存在。"""
    try:
        inspector = inspect(session.bind)
        return "screening_run_storage_stats" in set(inspector.get_table_names())
    except Exception:
        return False


def fetch_materialized_counts(
    session,
    run_ids: Sequence[str],
) -> tuple[int, int, int, list[str]]:
    """读取 run_id 范围内的物化 decision/pick 行数。

    Returns:
        (decision_count, pick_count, covered_run_count, missing_run_ids)
    """
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids or not storage_stats_table_exists(session):
        return 0, 0, 0, normalized_run_ids

    decision_total = 0
    pick_total = 0
    covered_run_ids: set[str] = set()

    for chunk in _iter_chunks(normalized_run_ids):
        rows = session.query(
            ScreeningRunStorageStats.run_id,
            ScreeningRunStorageStats.decision_count,
            ScreeningRunStorageStats.pick_count,
        ).filter(
            ScreeningRunStorageStats.run_id.in_(chunk)
        ).all()
        for row in rows:
            run_id = str(row.run_id or "").strip()
            if not run_id:
                continue
            covered_run_ids.add(run_id)
            decision_total += int(row.decision_count or 0)
            pick_total += int(row.pick_count or 0)

    missing_run_ids = [run_id for run_id in normalized_run_ids if run_id not in covered_run_ids]
    return decision_total, pick_total, len(covered_run_ids), missing_run_ids


def fetch_materialized_count_map(
    session,
    run_ids: Sequence[str],
) -> tuple[dict[str, tuple[int, int]], list[str]]:
    """Return materialized decision/pick counts keyed by run_id."""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids or not storage_stats_table_exists(session):
        return {}, normalized_run_ids

    counts: dict[str, tuple[int, int]] = {}
    for chunk in _iter_chunks(normalized_run_ids):
        rows = session.query(
            ScreeningRunStorageStats.run_id,
            ScreeningRunStorageStats.decision_count,
            ScreeningRunStorageStats.pick_count,
        ).filter(
            ScreeningRunStorageStats.run_id.in_(chunk)
        ).all()
        for row in rows:
            run_id = str(row.run_id or "").strip()
            if not run_id:
                continue
            counts[run_id] = (int(row.decision_count or 0), int(row.pick_count or 0))

    missing_run_ids = [run_id for run_id in normalized_run_ids if run_id not in counts]
    return counts, missing_run_ids


def fetch_materialized_totals(session) -> tuple[int, int, int, bool]:
    """读取物化统计表中的全局 decision/pick 总量。"""
    if not storage_stats_table_exists(session):
        return 0, 0, 0, False

    row = session.query(
        func.count(ScreeningRunStorageStats.run_id),
        func.coalesce(func.sum(ScreeningRunStorageStats.decision_count), 0),
        func.coalesce(func.sum(ScreeningRunStorageStats.pick_count), 0),
    ).one()
    stats_run_count = int(row[0] or 0)
    decision_total = int(row[1] or 0)
    pick_total = int(row[2] or 0)
    return decision_total, pick_total, stats_run_count, True


def count_detail_rows_for_run_ids(
    session,
    run_ids: Sequence[str],
) -> tuple[int, int]:
    """从明细表精确统计 run_id 范围内 decision/pick 行数。"""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids:
        return 0, 0

    decision_count = 0
    pick_count = 0
    for chunk in _iter_chunks(normalized_run_ids):
        decision_count += int(session.query(func.count(ScreeningDecision.id)).filter(
            ScreeningDecision.run_id.in_(chunk)
        ).scalar() or 0)
        pick_count += int(session.query(func.count(ScreeningPick.id)).filter(
            ScreeningPick.run_id.in_(chunk)
        ).scalar() or 0)
    return decision_count, pick_count


def count_detail_rows_by_run_ids(
    session,
    run_ids: Sequence[str],
) -> dict[str, tuple[int, int]]:
    """Return exact decision/pick detail counts keyed by run_id."""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids:
        return {}

    counts: dict[str, list[int]] = {run_id: [0, 0] for run_id in normalized_run_ids}
    for chunk in _iter_chunks(normalized_run_ids):
        for row in session.query(
            ScreeningDecision.run_id,
            func.count(ScreeningDecision.id),
        ).filter(
            ScreeningDecision.run_id.in_(chunk)
        ).group_by(ScreeningDecision.run_id).all():
            run_id = str(row[0] or "").strip()
            if run_id in counts:
                counts[run_id][0] = int(row[1] or 0)
        for row in session.query(
            ScreeningPick.run_id,
            func.count(ScreeningPick.id),
        ).filter(
            ScreeningPick.run_id.in_(chunk)
        ).group_by(ScreeningPick.run_id).all():
            run_id = str(row[0] or "").strip()
            if run_id in counts:
                counts[run_id][1] = int(row[1] or 0)
    return {run_id: (values[0], values[1]) for run_id, values in counts.items()}


def refresh_screening_run_storage_stats(
    *,
    db_manager=None,
    run_ids: Optional[Sequence[str]] = None,
    recent_days: Optional[int] = None,
    fund_types: Optional[Sequence[str]] = None,
    status: Optional[str] = None,
    stats_source: str = "manual_refresh",
    chunk_size: int = 500,
) -> dict:
    """刷新筛选 run 的物化存储统计。

    不删除任何明细数据；仅 upsert `screening_run_storage_stats`。
    """
    manager = db_manager or get_db_manager()
    normalized_run_ids = _normalize_run_ids(run_ids)
    normalized_fund_types = [
        str(item).strip()
        for item in (fund_types or [])
        if str(item).strip()
    ]

    with manager.get_session(task_name="refresh_screening_run_storage_stats", readonly=False) as session:
        if not storage_stats_table_exists(session):
            ScreeningRunStorageStats.__table__.create(bind=session.bind, checkfirst=True)

        query = session.query(ScreeningRun)
        if normalized_run_ids:
            query = query.filter(ScreeningRun.run_id.in_(normalized_run_ids))
        if recent_days is not None:
            cutoff_dt = datetime.now() - timedelta(days=max(1, int(recent_days)))
            query = query.filter(ScreeningRun.started_at >= cutoff_dt)
        if normalized_fund_types:
            query = query.filter(ScreeningRun.fund_type.in_(normalized_fund_types))
        if status:
            query = query.filter(ScreeningRun.status == str(status).strip())

        run_rows = query.order_by(ScreeningRun.started_at.desc()).all()
        target_run_ids = [str(row.run_id) for row in run_rows if str(row.run_id or "").strip()]

        decision_counts: dict[str, int] = {}
        pick_counts: dict[str, int] = {}
        for chunk in _iter_chunks(target_run_ids, chunk_size=chunk_size):
            for row in session.query(
                ScreeningDecision.run_id,
                func.count(ScreeningDecision.id),
            ).filter(
                ScreeningDecision.run_id.in_(chunk)
            ).group_by(ScreeningDecision.run_id).all():
                decision_counts[str(row[0])] = int(row[1] or 0)
            for row in session.query(
                ScreeningPick.run_id,
                func.count(ScreeningPick.id),
            ).filter(
                ScreeningPick.run_id.in_(chunk)
            ).group_by(ScreeningPick.run_id).all():
                pick_counts[str(row[0])] = int(row[1] or 0)

        now = datetime.now()
        upserted = 0
        for run in run_rows:
            run_id = str(run.run_id or "").strip()
            if not run_id:
                continue
            existing = session.query(ScreeningRunStorageStats).filter_by(run_id=run_id).first()
            if existing is None:
                existing = ScreeningRunStorageStats(run_id=run_id, created_at=now)
                session.add(existing)
            existing.fund_type = run.fund_type
            existing.period_years = run.period_years
            existing.status = run.status
            existing.started_at = run.started_at
            existing.finished_at = run.finished_at
            existing.decision_count = int(decision_counts.get(run_id, 0))
            existing.pick_count = int(pick_counts.get(run_id, 0))
            existing.stats_source = str(stats_source or "manual_refresh")[:40]
            existing.refreshed_at = now
            upserted += 1

    return {
        "status": "success",
        "run_count": len(target_run_ids),
        "upserted_count": upserted,
        "decision_count_total": sum(decision_counts.values()),
        "pick_count_total": sum(pick_counts.values()),
        "stats_source": str(stats_source or "manual_refresh")[:40],
    }


def mark_storage_stats_after_detail_cleanup(
    session,
    run_ids: Sequence[str],
    *,
    delete_runs: bool,
    stats_source: str = "cleanup_apply",
) -> dict:
    """在筛选明细清理后同步物化统计表。

    该函数必须在执行删除的同一事务 session 内调用。
    """
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids or not storage_stats_table_exists(session):
        return {
            "updated_count": 0,
            "deleted_count": 0,
            "skipped": True,
            "reason": "no_run_ids_or_stats_table_missing",
        }

    updated_count = 0
    deleted_count = 0
    if delete_runs:
        for chunk in _iter_chunks(normalized_run_ids):
            deleted_count += int(session.query(ScreeningRunStorageStats).filter(
                ScreeningRunStorageStats.run_id.in_(chunk)
            ).delete(synchronize_session=False) or 0)
    else:
        now = datetime.now()
        source = str(stats_source or "cleanup_apply")[:40]
        for chunk in _iter_chunks(normalized_run_ids):
            rows = session.query(ScreeningRunStorageStats).filter(
                ScreeningRunStorageStats.run_id.in_(chunk)
            ).all()
            for row in rows:
                row.decision_count = 0
                row.pick_count = 0
                row.stats_source = source
                row.refreshed_at = now
                updated_count += 1

    return {
        "updated_count": updated_count,
        "deleted_count": deleted_count,
        "skipped": False,
        "reason": "",
    }
