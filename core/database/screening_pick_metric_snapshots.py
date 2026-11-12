#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""筛选 pick 风险指标轻量快照维护。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import inspect

from database.connection import get_db_manager
from database.models import ScreeningPick, ScreeningPickMetricSnapshot
from database.screening_pick_payloads import decode_snapshot


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


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _parse_snapshot_payload(snapshot_json: str | bytes | None) -> dict:
    snapshot_json = decode_snapshot(snapshot_json)
    if not snapshot_json:
        return {}
    try:
        payload = json.loads(snapshot_json)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _risk_value(risk_metrics: dict, key: str) -> float | None:
    if not isinstance(risk_metrics, dict):
        return None
    return _safe_float(risk_metrics.get(key))


def pick_metric_snapshots_table_exists(session) -> bool:
    """检查 pick 风险指标快照表是否存在。"""
    try:
        inspector = inspect(session.bind)
        return "screening_pick_metric_snapshots" in set(inspector.get_table_names())
    except Exception:
        return False


def _ensure_table(session) -> None:
    if not pick_metric_snapshots_table_exists(session):
        ScreeningPickMetricSnapshot.__table__.create(bind=session.bind, checkfirst=True)


def _apply_pick_filters(query, *, run_ids: Sequence[str], fund_type: str | None, period_years: float | None):
    if run_ids:
        query = query.filter(ScreeningPick.run_id.in_(run_ids))
    if fund_type:
        query = query.filter(ScreeningPick.fund_type == fund_type)
    if period_years is not None:
        epsilon = 0.0001
        query = query.filter(
            ScreeningPick.period_years >= float(period_years) - epsilon,
            ScreeningPick.period_years <= float(period_years) + epsilon,
        )
    return query


def _delete_existing_snapshots(
    session,
    *,
    run_ids: Sequence[str],
    fund_type: str | None,
    period_years: float | None,
    chunk_size: int,
) -> int:
    deleted = 0
    if run_ids:
        for chunk in _iter_chunks(run_ids, chunk_size=chunk_size):
            chunk_query = session.query(ScreeningPickMetricSnapshot).filter(
                ScreeningPickMetricSnapshot.run_id.in_(chunk)
            )
            if fund_type:
                chunk_query = chunk_query.filter(ScreeningPickMetricSnapshot.fund_type == fund_type)
            if period_years is not None:
                epsilon = 0.0001
                chunk_query = chunk_query.filter(
                    ScreeningPickMetricSnapshot.period_years >= float(period_years) - epsilon,
                    ScreeningPickMetricSnapshot.period_years <= float(period_years) + epsilon,
                )
            deleted += int(chunk_query.delete(synchronize_session=False) or 0)
        return deleted

    query = session.query(ScreeningPickMetricSnapshot)
    if fund_type:
        query = query.filter(ScreeningPickMetricSnapshot.fund_type == fund_type)
    if period_years is not None:
        epsilon = 0.0001
        query = query.filter(
            ScreeningPickMetricSnapshot.period_years >= float(period_years) - epsilon,
            ScreeningPickMetricSnapshot.period_years <= float(period_years) + epsilon,
        )
    return int(query.delete(synchronize_session=False) or 0)


def _build_metric_snapshot(row: ScreeningPick, *, now: datetime, stats_source: str) -> ScreeningPickMetricSnapshot:
    payload = _parse_snapshot_payload(row.snapshot_json)
    risk_metrics = payload.get("risk_metrics") if isinstance(payload, dict) else {}
    if not isinstance(risk_metrics, dict):
        risk_metrics = {}

    return ScreeningPickMetricSnapshot(
        run_id=str(row.run_id or ""),
        fcode=str(row.fcode or ""),
        fund_type=row.fund_type,
        period_years=row.period_years,
        pick_id=int(row.id),
        rank=row.rank,
        picked_at=row.picked_at,
        sharpe_ratio=_risk_value(risk_metrics, "sharpe_ratio") if "sharpe_ratio" in risk_metrics else _safe_float(row.sharpe_ratio),
        raw_sharpe_ratio=_risk_value(risk_metrics, "raw_sharpe_ratio"),
        sortino_ratio=_risk_value(risk_metrics, "sortino_ratio"),
        raw_sortino_ratio=_risk_value(risk_metrics, "raw_sortino_ratio"),
        calmar_ratio=_risk_value(risk_metrics, "calmar_ratio"),
        max_drawdown=_risk_value(risk_metrics, "max_drawdown") if "max_drawdown" in risk_metrics else _safe_float(row.max_drawdown),
        max_drawdown_duration=_risk_value(risk_metrics, "max_drawdown_duration"),
        volatility=_risk_value(risk_metrics, "volatility") if "volatility" in risk_metrics else _safe_float(row.volatility),
        stats_source=str(stats_source or "manual_refresh")[:40],
        refreshed_at=now,
        created_at=now,
    )


def refresh_screening_pick_metric_snapshots(
    *,
    db_manager=None,
    run_ids: Optional[Sequence[str]] = None,
    fund_type: str | None = None,
    period_years: float | None = None,
    stats_source: str = "manual_refresh",
    chunk_size: int = 1000,
) -> dict:
    """刷新筛选 pick 的轻量风险指标快照。

    只抽取 `screening_picks.snapshot_json` 中已有的指标；缺失值保持为 NULL。
    """
    manager = db_manager or get_db_manager()
    normalized_run_ids = _normalize_run_ids(run_ids)
    normalized_fund_type = str(fund_type or "").strip() or None
    normalized_period_years = _safe_float(period_years) if period_years is not None else None
    batch_size = max(1, int(chunk_size or 1000))

    with manager.get_session(task_name="refresh_screening_pick_metric_snapshots", readonly=False) as session:
        _ensure_table(session)
        deleted_count = _delete_existing_snapshots(
            session,
            run_ids=normalized_run_ids,
            fund_type=normalized_fund_type,
            period_years=normalized_period_years,
            chunk_size=batch_size,
        )

        now = datetime.now()
        inserted_count = 0
        last_id = 0
        while True:
            query = session.query(ScreeningPick).filter(ScreeningPick.id > last_id)
            query = _apply_pick_filters(
                query,
                run_ids=normalized_run_ids,
                fund_type=normalized_fund_type,
                period_years=normalized_period_years,
            ).order_by(ScreeningPick.id.asc()).limit(batch_size)
            rows = query.all()
            if not rows:
                break

            pending = [
                _build_metric_snapshot(row, now=now, stats_source=stats_source)
                for row in rows
                if row.id is not None
            ]
            if pending:
                session.bulk_save_objects(pending)
                inserted_count += len(pending)
            last_id = int(rows[-1].id or last_id)
            session.expunge_all()

    return {
        "status": "success",
        "deleted_count": deleted_count,
        "inserted_count": inserted_count,
        "run_id_count": len(normalized_run_ids),
        "fund_type": normalized_fund_type,
        "period_years": normalized_period_years,
        "stats_source": str(stats_source or "manual_refresh")[:40],
    }


def delete_pick_metric_snapshots_for_run_ids(
    session,
    run_ids: Sequence[str],
) -> dict:
    """筛选明细清理后同步删除 pick 指标快照。"""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids or not pick_metric_snapshots_table_exists(session):
        return {
            "deleted_count": 0,
            "skipped": True,
            "reason": "no_run_ids_or_metric_table_missing",
        }

    deleted_count = 0
    for chunk in _iter_chunks(normalized_run_ids):
        deleted_count += int(session.query(ScreeningPickMetricSnapshot).filter(
            ScreeningPickMetricSnapshot.run_id.in_(chunk)
        ).delete(synchronize_session=False) or 0)
    return {
        "deleted_count": deleted_count,
        "skipped": False,
        "reason": "",
    }
