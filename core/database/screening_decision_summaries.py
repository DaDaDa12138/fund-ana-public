#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""筛选 decision 原因分布与 run 最新活动摘要维护。"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import func, inspect

from database.connection import get_db_manager
from database.models import (
    ScreeningDecision,
    ScreeningDecisionReasonSummary,
    ScreeningPick,
    ScreeningRun,
    ScreeningRunLatestActivity,
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


def decision_summary_tables_exist(session) -> bool:
    try:
        inspector = inspect(session.bind)
        tables = set(inspector.get_table_names())
        return {
            "screening_decision_reason_summary",
            "screening_run_latest_activity",
        }.issubset(tables)
    except Exception:
        return False


def _ensure_tables(session) -> None:
    if not decision_summary_tables_exist(session):
        ScreeningDecisionReasonSummary.__table__.create(bind=session.bind, checkfirst=True)
        ScreeningRunLatestActivity.__table__.create(bind=session.bind, checkfirst=True)


def _delete_existing_summary_rows(
    session,
    run_ids: Sequence[str],
    *,
    chunk_size: int,
    delete_all: bool = False,
) -> tuple[int, int]:
    if not run_ids and delete_all:
        deleted_reasons = int(
            session.query(ScreeningDecisionReasonSummary).delete(synchronize_session=False) or 0
        )
        deleted_latest = int(
            session.query(ScreeningRunLatestActivity).delete(synchronize_session=False) or 0
        )
        return deleted_reasons, deleted_latest
    if not run_ids:
        return 0, 0

    deleted_reasons = 0
    deleted_latest = 0
    for chunk in _iter_chunks(run_ids, chunk_size=chunk_size):
        deleted_reasons += int(
            session.query(ScreeningDecisionReasonSummary).filter(
                ScreeningDecisionReasonSummary.run_id.in_(chunk)
            ).delete(synchronize_session=False) or 0
        )
        deleted_latest += int(
            session.query(ScreeningRunLatestActivity).filter(
                ScreeningRunLatestActivity.run_id.in_(chunk)
            ).delete(synchronize_session=False) or 0
        )
    return deleted_reasons, deleted_latest


def _target_runs_query(session, run_ids: Sequence[str], recent_days: int | None):
    query = session.query(ScreeningRun)
    if run_ids:
        query = query.filter(ScreeningRun.run_id.in_(run_ids))
    if recent_days is not None:
        from datetime import timedelta

        cutoff = datetime.now() - timedelta(days=max(1, int(recent_days)))
        query = query.filter(ScreeningRun.started_at >= cutoff)
    return query


def refresh_screening_decision_summaries(
    *,
    db_manager=None,
    run_ids: Optional[Sequence[str]] = None,
    recent_days: Optional[int] = None,
    stats_source: str = "manual_refresh",
    chunk_size: int = 500,
) -> dict:
    """刷新 decision 原因摘要和 run 最新活动摘要。

    该函数只聚合真实明细数据；没有 decision/pick 明细的 run 会保留为 0 计数。
    历史 decision 若没有 created_at，则 latest_decision_at 保持 NULL，不伪造时间。
    """
    manager = db_manager or get_db_manager()
    normalized_run_ids = _normalize_run_ids(run_ids)
    source = str(stats_source or "manual_refresh")[:40]
    batch_size = max(1, int(chunk_size or 500))

    with manager.get_session(task_name="refresh_screening_decision_summaries", readonly=False) as session:
        _ensure_tables(session)
        run_rows = _target_runs_query(
            session,
            normalized_run_ids,
            recent_days,
        ).order_by(ScreeningRun.started_at.desc()).all()
        target_run_ids = [str(row.run_id or "").strip() for row in run_rows if str(row.run_id or "").strip()]

        deleted_reasons, deleted_latest = _delete_existing_summary_rows(
            session,
            target_run_ids,
            chunk_size=batch_size,
            delete_all=not normalized_run_ids and recent_days is None,
        )
        if not target_run_ids:
            return {
                "status": "success",
                "run_count": 0,
                "reason_inserted_count": 0,
                "latest_activity_inserted_count": 0,
                "deleted_reason_count": deleted_reasons,
                "deleted_latest_activity_count": deleted_latest,
                "stats_source": source,
            }

        now = datetime.now()
        run_meta = {str(row.run_id): row for row in run_rows}
        reason_inserted = 0
        latest_inserted = 0

        for chunk in _iter_chunks(target_run_ids, chunk_size=batch_size):
            reason_rows = session.query(
                ScreeningDecision.run_id,
                ScreeningDecision.step_id,
                func.min(ScreeningDecision.step_name),
                ScreeningDecision.passed,
                func.coalesce(ScreeningDecision.reason_code, ""),
                func.count(ScreeningDecision.id),
                func.min(ScreeningDecision.created_at),
                func.max(ScreeningDecision.created_at),
            ).filter(
                ScreeningDecision.run_id.in_(chunk)
            ).group_by(
                ScreeningDecision.run_id,
                ScreeningDecision.step_id,
                ScreeningDecision.passed,
                func.coalesce(ScreeningDecision.reason_code, ""),
            ).all()

            pending_reasons = []
            for row in reason_rows:
                run = run_meta.get(str(row[0]))
                pending_reasons.append(
                    ScreeningDecisionReasonSummary(
                        run_id=str(row[0]),
                        fund_type=run.fund_type if run else None,
                        period_years=run.period_years if run else None,
                        status=run.status if run else None,
                        step_id=int(row[1] or 0),
                        step_name=row[2],
                        passed=bool(row[3]),
                        reason_code=str(row[4] or ""),
                        decision_count=int(row[5] or 0),
                        first_decision_at=row[6],
                        latest_decision_at=row[7],
                        stats_source=source,
                        refreshed_at=now,
                        created_at=now,
                    )
                )
            if pending_reasons:
                session.bulk_save_objects(pending_reasons)
                reason_inserted += len(pending_reasons)

            decision_latest = {
                str(row[0]): row
                for row in session.query(
                    ScreeningDecision.run_id,
                    func.count(ScreeningDecision.id),
                    func.min(ScreeningDecision.created_at),
                    func.max(ScreeningDecision.created_at),
                ).filter(
                    ScreeningDecision.run_id.in_(chunk)
                ).group_by(ScreeningDecision.run_id).all()
            }
            pick_latest = {
                str(row[0]): row
                for row in session.query(
                    ScreeningPick.run_id,
                    func.count(ScreeningPick.id),
                    func.min(ScreeningPick.picked_at),
                    func.max(ScreeningPick.picked_at),
                ).filter(
                    ScreeningPick.run_id.in_(chunk)
                ).group_by(ScreeningPick.run_id).all()
            }

            pending_latest = []
            for run_id in chunk:
                run = run_meta.get(str(run_id))
                decision_row = decision_latest.get(str(run_id))
                pick_row = pick_latest.get(str(run_id))
                latest_decision_at = decision_row[3] if decision_row else None
                latest_pick_at = pick_row[3] if pick_row else None
                latest_values = [
                    value for value in (latest_decision_at, latest_pick_at) if value is not None
                ]
                latest_detail_at = max(latest_values) if latest_values else None
                pending_latest.append(
                    ScreeningRunLatestActivity(
                        run_id=str(run_id),
                        fund_type=run.fund_type if run else None,
                        period_years=run.period_years if run else None,
                        status=run.status if run else None,
                        started_at=run.started_at if run else None,
                        finished_at=run.finished_at if run else None,
                        decision_count=int(decision_row[1] or 0) if decision_row else 0,
                        pick_count=int(pick_row[1] or 0) if pick_row else 0,
                        first_decision_at=decision_row[2] if decision_row else None,
                        latest_decision_at=latest_decision_at,
                        first_pick_at=pick_row[2] if pick_row else None,
                        latest_pick_at=latest_pick_at,
                        latest_detail_at=latest_detail_at,
                        stats_source=source,
                        refreshed_at=now,
                        created_at=now,
                    )
                )
            if pending_latest:
                session.bulk_save_objects(pending_latest)
                latest_inserted += len(pending_latest)
            session.expunge_all()

    return {
        "status": "success",
        "run_count": len(target_run_ids),
        "reason_inserted_count": reason_inserted,
        "latest_activity_inserted_count": latest_inserted,
        "deleted_reason_count": deleted_reasons,
        "deleted_latest_activity_count": deleted_latest,
        "stats_source": source,
    }


def delete_screening_decision_summaries_for_run_ids(
    session,
    run_ids: Sequence[str],
) -> dict:
    """筛选明细清理后同步删除 decision 摘要。"""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids or not decision_summary_tables_exist(session):
        return {
            "deleted_reason_count": 0,
            "deleted_latest_activity_count": 0,
            "skipped": True,
            "reason": "no_run_ids_or_summary_table_missing",
        }

    deleted_reasons, deleted_latest = _delete_existing_summary_rows(
        session,
        normalized_run_ids,
        chunk_size=500,
    )
    return {
        "deleted_reason_count": deleted_reasons,
        "deleted_latest_activity_count": deleted_latest,
        "skipped": False,
        "reason": "",
    }
