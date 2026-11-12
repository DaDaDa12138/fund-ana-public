#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
screening 存储增长审计工具。

目标：
1. 基于真实 SQLite 数据库按 run_id 汇总 screening_decisions/screening_picks 增量；
2. 优先复用 run 级物化摘要，只有摘要缺失时才回退明细点查；
3. 按日 × fund_type × decision_persist_mode 聚合，观察写入规模变化；
4. 输出 JSON/Markdown 产物，便于持续对比优化效果。
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from database.connection import resolve_db_path


DEFAULT_AUDIT_DIR = "audit"
VALID_COUNT_TABLES = {"screening_decisions", "screening_picks"}
LATEST_ACTIVITY_TABLE = "screening_run_latest_activity"
RUN_STORAGE_STATS_TABLE = "screening_run_storage_stats"
VALID_SUMMARY_COUNT_TABLES = {LATEST_ACTIVITY_TABLE, RUN_STORAGE_STATS_TABLE}
DECISION_REASON_SUMMARY_TABLE = "screening_decision_reason_summary"
PICK_METRIC_SNAPSHOT_TABLE = "screening_pick_metric_snapshots"


def open_readonly_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """以只读方式打开 SQLite 数据库。"""
    resolved = resolve_db_path(db_path or "database/bond_funds.db")
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def parse_decision_persist_mode(snapshot_json: Optional[str]) -> str:
    """
    从 config_snapshot_json 解析 decision_persist_mode。

    返回值：
    - picked_only
    - all
    - legacy_or_all: 旧 run 或未显式记录模式的 run
    """
    if not snapshot_json:
        return "legacy_or_all"

    try:
        snapshot = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return "legacy_or_all"

    mode = str(snapshot.get("decision_persist_mode") or "").strip().lower()
    if mode == "picked_only":
        return "picked_only"
    if mode == "all":
        return "all"
    return "legacy_or_all"


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """检查 SQLite 表是否存在。"""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def fetch_recent_run_rows(
    conn: sqlite3.Connection,
    *,
    days: int = 14,
    fund_types: Optional[Sequence[str]] = None,
    now: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    """读取最近 N 天的 screening_runs。"""
    anchor = now or datetime.now()
    cutoff = (anchor - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        SELECT
            run_id,
            started_at,
            finished_at,
            status,
            fund_type,
            period_years,
            fund_count_in,
            fund_count_pass,
            fund_count_fail,
            config_snapshot_json,
            code_version
        FROM screening_runs
        WHERE started_at >= ?
    """
    params: List[object] = [cutoff]

    normalized_fund_types = [str(item).strip() for item in (fund_types or []) if str(item).strip()]
    if normalized_fund_types:
        placeholders = ",".join("?" for _ in normalized_fund_types)
        sql += f" AND fund_type IN ({placeholders})"
        params.extend(normalized_fund_types)

    sql += " ORDER BY started_at DESC"
    if limit is not None and int(limit) > 0:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def fetch_recent_run_metrics_rows(
    conn: sqlite3.Connection,
    *,
    days: int = 14,
    fund_types: Optional[Sequence[str]] = None,
    now: Optional[datetime] = None,
    limit: Optional[int] = None,
    prefer_materialized: bool = True,
) -> List[dict]:
    """
    读取最近 N 天 run 的 decision/pick 计数。

    优先读取 `screening_run_latest_activity` 物化摘要；目标 run 缺行时，
    先使用 `screening_run_storage_stats` 补位，只有两个物化摘要都缺失
    时才对缺失 run 回退到明细表 run_id 点查。
    """
    anchor = now or datetime.now()
    cutoff = (anchor - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    recent_runs_filter = "WHERE r.started_at >= ?"
    params: List[object] = [cutoff]

    normalized_fund_types = [str(item).strip() for item in (fund_types or []) if str(item).strip()]
    if normalized_fund_types:
        placeholders = ",".join("?" for _ in normalized_fund_types)
        recent_runs_filter += f" AND r.fund_type IN ({placeholders})"
        params.extend(normalized_fund_types)

    latest_activity_exists = prefer_materialized and sqlite_table_exists(conn, LATEST_ACTIVITY_TABLE)
    storage_stats_exists = prefer_materialized and sqlite_table_exists(conn, RUN_STORAGE_STATS_TABLE)

    if latest_activity_exists:
        sql = f"""
            SELECT
                r.run_id,
                r.started_at,
                r.finished_at,
                r.status,
                r.fund_type,
                r.period_years,
                r.fund_count_in,
                r.fund_count_pass,
                r.fund_count_fail,
                r.config_snapshot_json,
                r.code_version,
                COALESCE(a.decision_count, 0) AS decision_count,
                COALESCE(a.pick_count, 0) AS pick_count,
                CASE
                    WHEN a.run_id IS NULL THEN 'detail_scan_fallback'
                    ELSE 'screening_run_latest_activity'
                END AS count_source
            FROM screening_runs r
            LEFT JOIN screening_run_latest_activity a
              ON a.run_id = r.run_id
            {recent_runs_filter}
            ORDER BY r.started_at DESC
        """
        materialized_params = list(params)
        if limit is not None and int(limit) > 0:
            sql += " LIMIT ?"
            materialized_params.append(int(limit))
        rows = [dict(row) for row in conn.execute(sql, materialized_params).fetchall()]

        fallback_run_ids = [
            str(row.get("run_id") or "").strip()
            for row in rows
            if str(row.get("count_source") or "") == "detail_scan_fallback"
        ]
        if fallback_run_ids and storage_stats_exists:
            storage_counts = fetch_summary_count_by_run_ids(
                conn,
                summary_table=RUN_STORAGE_STATS_TABLE,
                run_ids=fallback_run_ids,
            )
            for row in rows:
                run_id = str(row.get("run_id") or "").strip()
                counts = storage_counts.get(run_id)
                if counts is None:
                    continue
                row["decision_count"] = int(counts[0])
                row["pick_count"] = int(counts[1])
                row["count_source"] = RUN_STORAGE_STATS_TABLE
            fallback_run_ids = [
                run_id
                for run_id in fallback_run_ids
                if run_id not in storage_counts
            ]
        if fallback_run_ids:
            decision_counts = fetch_count_by_run_ids(
                conn,
                table_name="screening_decisions",
                run_ids=fallback_run_ids,
            )
            pick_counts = fetch_count_by_run_ids(
                conn,
                table_name="screening_picks",
                run_ids=fallback_run_ids,
            )
            for row in rows:
                run_id = str(row.get("run_id") or "").strip()
                if run_id not in fallback_run_ids:
                    continue
                row["decision_count"] = int(decision_counts.get(run_id, 0))
                row["pick_count"] = int(pick_counts.get(run_id, 0))
        return rows

    if storage_stats_exists:
        sql = f"""
            SELECT
                r.run_id,
                r.started_at,
                r.finished_at,
                r.status,
                r.fund_type,
                r.period_years,
                r.fund_count_in,
                r.fund_count_pass,
                r.fund_count_fail,
                r.config_snapshot_json,
                r.code_version,
                COALESCE(s.decision_count, 0) AS decision_count,
                COALESCE(s.pick_count, 0) AS pick_count,
                CASE
                    WHEN s.run_id IS NULL THEN 'detail_scan_fallback'
                    ELSE 'screening_run_storage_stats'
                END AS count_source
            FROM screening_runs r
            LEFT JOIN screening_run_storage_stats s
              ON s.run_id = r.run_id
            {recent_runs_filter}
            ORDER BY r.started_at DESC
        """
        storage_params = list(params)
        if limit is not None and int(limit) > 0:
            sql += " LIMIT ?"
            storage_params.append(int(limit))
        rows = [dict(row) for row in conn.execute(sql, storage_params).fetchall()]
        fallback_run_ids = [
            str(row.get("run_id") or "").strip()
            for row in rows
            if str(row.get("count_source") or "") == "detail_scan_fallback"
        ]
        if fallback_run_ids:
            decision_counts = fetch_count_by_run_ids(
                conn,
                table_name="screening_decisions",
                run_ids=fallback_run_ids,
            )
            pick_counts = fetch_count_by_run_ids(
                conn,
                table_name="screening_picks",
                run_ids=fallback_run_ids,
            )
            for row in rows:
                run_id = str(row.get("run_id") or "").strip()
                if run_id not in fallback_run_ids:
                    continue
                row["decision_count"] = int(decision_counts.get(run_id, 0))
                row["pick_count"] = int(pick_counts.get(run_id, 0))
        return rows

    sql = f"""
        SELECT
            r.run_id,
            r.started_at,
            r.finished_at,
            r.status,
            r.fund_type,
            r.period_years,
            r.fund_count_in,
            r.fund_count_pass,
            r.fund_count_fail,
            r.config_snapshot_json,
            r.code_version,
            (
                SELECT COUNT(*)
                FROM screening_decisions d
                WHERE d.run_id = r.run_id
            ) AS decision_count,
            (
                SELECT COUNT(*)
                FROM screening_picks p
                WHERE p.run_id = r.run_id
            ) AS pick_count,
            'detail_scan' AS count_source
        FROM screening_runs r
        {recent_runs_filter}
        ORDER BY r.started_at DESC
    """
    if limit is not None and int(limit) > 0:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def fetch_summary_count_by_run_ids(
    conn: sqlite3.Connection,
    *,
    summary_table: str,
    run_ids: Sequence[str],
    chunk_size: int = 500,
) -> Dict[str, Tuple[int, int]]:
    """从 run 级物化摘要表读取 decision/pick 计数。"""
    if summary_table not in VALID_SUMMARY_COUNT_TABLES:
        raise ValueError(f"不支持的摘要表名: {summary_table}")
    if not sqlite_table_exists(conn, summary_table):
        return {}

    normalized_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return {}

    counts: Dict[str, Tuple[int, int]] = {}
    for chunk_start in range(0, len(normalized_run_ids), max(1, int(chunk_size))):
        chunk = normalized_run_ids[chunk_start:chunk_start + max(1, int(chunk_size))]
        placeholders = ",".join("?" for _ in chunk)
        sql = f"""
            SELECT run_id, decision_count, pick_count
            FROM {summary_table}
            WHERE run_id IN ({placeholders})
        """
        for row in conn.execute(sql, chunk).fetchall():
            counts[str(row["run_id"])] = (
                int(row["decision_count"] or 0),
                int(row["pick_count"] or 0),
            )

    return counts


def fetch_count_by_run_ids(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    run_ids: Sequence[str],
    chunk_size: int = 500,
) -> Dict[str, int]:
    """按 run_id 批量统计表行数，避免全表扫描。"""
    if table_name not in VALID_COUNT_TABLES:
        raise ValueError(f"不支持的表名: {table_name}")

    normalized_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return {}

    counts: Dict[str, int] = {}
    for chunk_start in range(0, len(normalized_run_ids), max(1, int(chunk_size))):
        chunk = normalized_run_ids[chunk_start:chunk_start + max(1, int(chunk_size))]
        placeholders = ",".join("?" for _ in chunk)
        sql = f"""
            SELECT run_id, COUNT(*) AS row_count
            FROM {table_name}
            WHERE run_id IN ({placeholders})
            GROUP BY run_id
        """
        for row in conn.execute(sql, chunk).fetchall():
            counts[str(row["run_id"])] = int(row["row_count"] or 0)

    return counts


def fetch_materialized_coverage_summary(conn: sqlite3.Connection) -> Dict[str, object]:
    """统计筛选 run 级摘要表覆盖率。"""
    run_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM screening_runs").fetchone()["cnt"] or 0)
    coverage: Dict[str, object] = {
        "screening_runs": {
            "exists": sqlite_table_exists(conn, "screening_runs"),
            "run_count": run_count,
        }
    }

    for summary_table in (LATEST_ACTIVITY_TABLE, RUN_STORAGE_STATS_TABLE):
        if not sqlite_table_exists(conn, summary_table):
            coverage[summary_table] = {
                "exists": False,
                "covered_run_count": 0,
                "missing_run_count": run_count,
                "coverage_ratio": 0.0 if run_count else 1.0,
            }
            continue
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS covered_run_count,
                COALESCE(SUM(decision_count), 0) AS decision_total,
                COALESCE(SUM(pick_count), 0) AS pick_total
            FROM {summary_table}
            """
        ).fetchone()
        covered_run_count = int(row["covered_run_count"] or 0)
        coverage[summary_table] = {
            "exists": True,
            "covered_run_count": covered_run_count,
            "missing_run_count": max(0, run_count - covered_run_count),
            "coverage_ratio": round(covered_run_count / run_count, 6) if run_count else 1.0,
            "decision_total": int(row["decision_total"] or 0),
            "pick_total": int(row["pick_total"] or 0),
        }

    distinct_run_tables = {
        "screening_decision_reason_summary": "run_id",
        "screening_pick_metric_snapshots": "run_id",
    }
    for table_name, run_id_column in distinct_run_tables.items():
        if not sqlite_table_exists(conn, table_name):
            coverage[table_name] = {
                "exists": False,
                "covered_run_count": 0,
                "missing_run_count": run_count,
                "coverage_ratio": 0.0 if run_count else 1.0,
            }
            continue
        row = conn.execute(
            f"SELECT COUNT(DISTINCT {run_id_column}) AS covered_run_count FROM {table_name}"
        ).fetchone()
        covered_run_count = int(row["covered_run_count"] or 0)
        coverage[table_name] = {
            "exists": True,
            "covered_run_count": covered_run_count,
            "missing_run_count": max(0, run_count - covered_run_count),
            "coverage_ratio": round(covered_run_count / run_count, 6) if run_count else 1.0,
        }

    return coverage


def _fetch_missing_latest_activity_rows(conn: sqlite3.Connection) -> List[dict]:
    """读取缺少 latest activity 摘要的 run 元数据。"""
    latest_exists = sqlite_table_exists(conn, LATEST_ACTIVITY_TABLE)
    storage_exists = sqlite_table_exists(conn, RUN_STORAGE_STATS_TABLE)

    storage_select = """
        s.run_id AS storage_run_id,
        s.decision_count AS storage_decision_count,
        s.pick_count AS storage_pick_count,
        s.stats_source AS storage_stats_source,
        s.refreshed_at AS storage_refreshed_at
    """ if storage_exists else """
        NULL AS storage_run_id,
        NULL AS storage_decision_count,
        NULL AS storage_pick_count,
        NULL AS storage_stats_source,
        NULL AS storage_refreshed_at
    """
    latest_join = (
        f"LEFT JOIN {LATEST_ACTIVITY_TABLE} a ON a.run_id = r.run_id"
        if latest_exists
        else ""
    )
    storage_join = (
        f"LEFT JOIN {RUN_STORAGE_STATS_TABLE} s ON s.run_id = r.run_id"
        if storage_exists
        else ""
    )
    latest_filter = "WHERE a.run_id IS NULL" if latest_exists else ""
    rows = conn.execute(
        f"""
        SELECT
            r.run_id,
            r.started_at,
            r.finished_at,
            r.status,
            r.fund_type,
            r.period_years,
            {storage_select}
        FROM screening_runs r
        {latest_join}
        {storage_join}
        {latest_filter}
        ORDER BY r.started_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_latest_activity_gap_analysis(
    conn: sqlite3.Connection,
    *,
    sample_limit: int = 20,
) -> Dict[str, object]:
    """
    分析 `screening_run_latest_activity` 缺口。

    该审计只读，不回填数据库。若缺口 run 的明细仍在热库，可作为
    `refresh_screening_decision_summaries()` 的安全候选；若明细已清理但
    `screening_run_storage_stats` 存在，则应继续以 storage stats 作为计数主来源，
    不伪造 decision/pick 时间字段。
    """
    latest_exists = sqlite_table_exists(conn, LATEST_ACTIVITY_TABLE)
    storage_exists = sqlite_table_exists(conn, RUN_STORAGE_STATS_TABLE)
    missing_rows = _fetch_missing_latest_activity_rows(conn)
    run_ids = [str(row.get("run_id") or "").strip() for row in missing_rows if str(row.get("run_id") or "").strip()]
    decision_counts = fetch_count_by_run_ids(conn, table_name="screening_decisions", run_ids=run_ids)
    pick_counts = fetch_count_by_run_ids(conn, table_name="screening_picks", run_ids=run_ids)

    limit = max(0, int(sample_limit or 0))
    samples: Dict[str, List[dict]] = {
        "refresh_candidates": [],
        "storage_stats_primary": [],
        "unresolved": [],
    }
    summary = {
        "latest_activity_exists": bool(latest_exists),
        "storage_stats_exists": bool(storage_exists),
        "missing_run_count": len(run_ids),
        "storage_stats_covered_count": 0,
        "storage_stats_missing_count": 0,
        "detail_present_count": 0,
        "detail_absent_count": 0,
        "refresh_candidate_count": 0,
        "storage_stats_primary_count": 0,
        "storage_stats_primary_nonzero_count": 0,
        "storage_stats_primary_zero_count": 0,
        "unresolved_count": 0,
        "detail_decision_count_total_for_missing": 0,
        "detail_pick_count_total_for_missing": 0,
        "storage_decision_count_total_for_missing": 0,
        "storage_pick_count_total_for_missing": 0,
    }

    def append_sample(bucket: str, row: dict, *, detail_decisions: int, detail_picks: int) -> None:
        if limit <= 0 or len(samples[bucket]) >= limit:
            return
        samples[bucket].append({
            "run_id": row.get("run_id"),
            "started_at": row.get("started_at"),
            "status": row.get("status"),
            "fund_type": row.get("fund_type"),
            "period_years": row.get("period_years"),
            "storage_decision_count": int(row.get("storage_decision_count") or 0),
            "storage_pick_count": int(row.get("storage_pick_count") or 0),
            "storage_stats_source": row.get("storage_stats_source"),
            "detail_decision_count": detail_decisions,
            "detail_pick_count": detail_picks,
        })

    for row in missing_rows:
        run_id = str(row.get("run_id") or "").strip()
        detail_decisions = int(decision_counts.get(run_id, 0))
        detail_picks = int(pick_counts.get(run_id, 0))
        storage_covered = bool(row.get("storage_run_id"))
        storage_decisions = int(row.get("storage_decision_count") or 0)
        storage_picks = int(row.get("storage_pick_count") or 0)
        detail_present = detail_decisions > 0 or detail_picks > 0
        storage_nonzero = storage_decisions > 0 or storage_picks > 0

        summary["detail_decision_count_total_for_missing"] += detail_decisions
        summary["detail_pick_count_total_for_missing"] += detail_picks
        summary["storage_decision_count_total_for_missing"] += storage_decisions
        summary["storage_pick_count_total_for_missing"] += storage_picks

        if storage_covered:
            summary["storage_stats_covered_count"] += 1
        else:
            summary["storage_stats_missing_count"] += 1

        if detail_present:
            summary["detail_present_count"] += 1
            summary["refresh_candidate_count"] += 1
            append_sample("refresh_candidates", row, detail_decisions=detail_decisions, detail_picks=detail_picks)
            continue

        summary["detail_absent_count"] += 1
        if storage_covered:
            summary["storage_stats_primary_count"] += 1
            if storage_nonzero:
                summary["storage_stats_primary_nonzero_count"] += 1
            else:
                summary["storage_stats_primary_zero_count"] += 1
            append_sample("storage_stats_primary", row, detail_decisions=detail_decisions, detail_picks=detail_picks)
        else:
            summary["unresolved_count"] += 1
            append_sample("unresolved", row, detail_decisions=detail_decisions, detail_picks=detail_picks)

    if summary["missing_run_count"] == 0:
        status = "ok"
    elif summary["refresh_candidate_count"] > 0 or summary["unresolved_count"] > 0:
        status = "review"
    else:
        status = "storage_stats_primary"
    summary["status"] = status

    return {
        "summary": summary,
        "samples": {key: value for key, value in samples.items() if value},
    }


def fetch_summary_snapshot_coverage_analysis(
    conn: sqlite3.Connection,
    *,
    sample_limit: int = 20,
) -> Dict[str, object]:
    """
    审计 decision reason summary 与 pick metric snapshot 覆盖率。

    只读取 `screening_runs`、run 级物化统计和摘要表本身；不扫描
    `screening_decisions` / `screening_picks` 明细表。已清理且
    storage stats 为 0 的历史 run 不会被误判为需要刷新摘要。
    """
    storage_exists = sqlite_table_exists(conn, RUN_STORAGE_STATS_TABLE)
    latest_exists = sqlite_table_exists(conn, LATEST_ACTIVITY_TABLE)
    reason_exists = sqlite_table_exists(conn, DECISION_REASON_SUMMARY_TABLE)
    metric_exists = sqlite_table_exists(conn, PICK_METRIC_SNAPSHOT_TABLE)

    storage_join = (
        f"LEFT JOIN {RUN_STORAGE_STATS_TABLE} s ON s.run_id = r.run_id"
        if storage_exists
        else ""
    )
    latest_join = (
        f"LEFT JOIN {LATEST_ACTIVITY_TABLE} a ON a.run_id = r.run_id"
        if latest_exists
        else ""
    )
    reason_join = (
        f"""
        LEFT JOIN (
            SELECT
                run_id,
                COUNT(*) AS reason_group_count,
                COALESCE(SUM(decision_count), 0) AS reason_decision_count,
                MIN(refreshed_at) AS reason_first_refreshed_at,
                MAX(refreshed_at) AS reason_latest_refreshed_at
            FROM {DECISION_REASON_SUMMARY_TABLE}
            GROUP BY run_id
        ) rs ON rs.run_id = r.run_id
        """
        if reason_exists
        else ""
    )
    metric_join = (
        f"""
        LEFT JOIN (
            SELECT
                run_id,
                COUNT(*) AS metric_snapshot_count,
                MIN(refreshed_at) AS metric_first_refreshed_at,
                MAX(refreshed_at) AS metric_latest_refreshed_at
            FROM {PICK_METRIC_SNAPSHOT_TABLE}
            GROUP BY run_id
        ) ms ON ms.run_id = r.run_id
        """
        if metric_exists
        else ""
    )

    storage_select = (
        "s.run_id AS storage_run_id, s.decision_count AS storage_decision_count, "
        "s.pick_count AS storage_pick_count"
        if storage_exists
        else "NULL AS storage_run_id, NULL AS storage_decision_count, NULL AS storage_pick_count"
    )
    latest_select = (
        "a.run_id AS latest_run_id, a.decision_count AS latest_decision_count, "
        "a.pick_count AS latest_pick_count"
        if latest_exists
        else "NULL AS latest_run_id, NULL AS latest_decision_count, NULL AS latest_pick_count"
    )
    reason_select = (
        "rs.reason_group_count, rs.reason_decision_count, "
        "rs.reason_first_refreshed_at, rs.reason_latest_refreshed_at"
        if reason_exists
        else (
            "NULL AS reason_group_count, NULL AS reason_decision_count, "
            "NULL AS reason_first_refreshed_at, NULL AS reason_latest_refreshed_at"
        )
    )
    metric_select = (
        "ms.metric_snapshot_count, ms.metric_first_refreshed_at, ms.metric_latest_refreshed_at"
        if metric_exists
        else "NULL AS metric_snapshot_count, NULL AS metric_first_refreshed_at, NULL AS metric_latest_refreshed_at"
    )

    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
                r.run_id,
                r.started_at,
                r.finished_at,
                r.status,
                r.fund_type,
                r.period_years,
                {storage_select},
                {latest_select},
                {reason_select},
                {metric_select}
            FROM screening_runs r
            {storage_join}
            {latest_join}
            {reason_join}
            {metric_join}
            ORDER BY r.started_at DESC
            """
        ).fetchall()
    ]

    limit = max(0, int(sample_limit or 0))
    samples: Dict[str, List[dict]] = {
        "reason_refresh_candidates": [],
        "reason_stale_extra": [],
        "metric_refresh_candidates": [],
        "metric_stale_extra": [],
        "baseline_missing": [],
    }
    summary = {
        "status": "ok",
        "run_count": len(rows),
        "baseline": {
            "storage_stats_exists": bool(storage_exists),
            "latest_activity_exists": bool(latest_exists),
            "storage_stats_primary_run_count": 0,
            "latest_activity_fallback_run_count": 0,
            "baseline_missing_run_count": 0,
            "nonzero_decision_run_count": 0,
            "nonzero_pick_run_count": 0,
        },
        "decision_reason_summary": {
            "exists": bool(reason_exists),
            "covered_run_count": 0,
            "consistent_nonzero_run_count": 0,
            "missing_nonzero_run_count": 0,
            "mismatch_run_count": 0,
            "stale_extra_run_count": 0,
            "refresh_candidate_count": 0,
            "baseline_decision_count_total": 0,
            "summary_decision_count_total": 0,
        },
        "pick_metric_snapshots": {
            "exists": bool(metric_exists),
            "covered_run_count": 0,
            "consistent_nonzero_run_count": 0,
            "missing_nonzero_run_count": 0,
            "mismatch_run_count": 0,
            "stale_extra_run_count": 0,
            "refresh_candidate_count": 0,
            "baseline_pick_count_total": 0,
            "snapshot_count_total": 0,
        },
    }

    def _append_sample(bucket: str, row: dict, *, baseline_source: str, baseline_decisions: int, baseline_picks: int) -> None:
        if limit <= 0 or len(samples[bucket]) >= limit:
            return
        samples[bucket].append({
            "run_id": row.get("run_id"),
            "started_at": row.get("started_at"),
            "status": row.get("status"),
            "fund_type": row.get("fund_type"),
            "period_years": row.get("period_years"),
            "baseline_source": baseline_source,
            "baseline_decision_count": baseline_decisions,
            "baseline_pick_count": baseline_picks,
            "reason_group_count": int(row.get("reason_group_count") or 0),
            "reason_decision_count": int(row.get("reason_decision_count") or 0),
            "metric_snapshot_count": int(row.get("metric_snapshot_count") or 0),
            "reason_latest_refreshed_at": row.get("reason_latest_refreshed_at"),
            "metric_latest_refreshed_at": row.get("metric_latest_refreshed_at"),
        })

    for row in rows:
        storage_run_id = str(row.get("storage_run_id") or "").strip()
        latest_run_id = str(row.get("latest_run_id") or "").strip()
        if storage_run_id:
            baseline_source = RUN_STORAGE_STATS_TABLE
            baseline_decisions = int(row.get("storage_decision_count") or 0)
            baseline_picks = int(row.get("storage_pick_count") or 0)
            summary["baseline"]["storage_stats_primary_run_count"] += 1
        elif latest_run_id:
            baseline_source = LATEST_ACTIVITY_TABLE
            baseline_decisions = int(row.get("latest_decision_count") or 0)
            baseline_picks = int(row.get("latest_pick_count") or 0)
            summary["baseline"]["latest_activity_fallback_run_count"] += 1
        else:
            baseline_source = "missing"
            baseline_decisions = 0
            baseline_picks = 0
            summary["baseline"]["baseline_missing_run_count"] += 1
            _append_sample(
                "baseline_missing",
                row,
                baseline_source=baseline_source,
                baseline_decisions=baseline_decisions,
                baseline_picks=baseline_picks,
            )

        reason_groups = int(row.get("reason_group_count") or 0)
        reason_decisions = int(row.get("reason_decision_count") or 0)
        metric_snapshots = int(row.get("metric_snapshot_count") or 0)

        summary["decision_reason_summary"]["baseline_decision_count_total"] += baseline_decisions
        summary["decision_reason_summary"]["summary_decision_count_total"] += reason_decisions
        summary["pick_metric_snapshots"]["baseline_pick_count_total"] += baseline_picks
        summary["pick_metric_snapshots"]["snapshot_count_total"] += metric_snapshots
        if reason_groups > 0:
            summary["decision_reason_summary"]["covered_run_count"] += 1
        if metric_snapshots > 0:
            summary["pick_metric_snapshots"]["covered_run_count"] += 1
        if baseline_decisions > 0:
            summary["baseline"]["nonzero_decision_run_count"] += 1
            if not reason_exists or reason_groups == 0:
                summary["decision_reason_summary"]["missing_nonzero_run_count"] += 1
                summary["decision_reason_summary"]["refresh_candidate_count"] += 1
                _append_sample(
                    "reason_refresh_candidates",
                    row,
                    baseline_source=baseline_source,
                    baseline_decisions=baseline_decisions,
                    baseline_picks=baseline_picks,
                )
            elif reason_decisions != baseline_decisions:
                summary["decision_reason_summary"]["mismatch_run_count"] += 1
                summary["decision_reason_summary"]["refresh_candidate_count"] += 1
                _append_sample(
                    "reason_refresh_candidates",
                    row,
                    baseline_source=baseline_source,
                    baseline_decisions=baseline_decisions,
                    baseline_picks=baseline_picks,
                )
            else:
                summary["decision_reason_summary"]["consistent_nonzero_run_count"] += 1
        elif reason_groups > 0 or reason_decisions > 0:
            summary["decision_reason_summary"]["stale_extra_run_count"] += 1
            _append_sample(
                "reason_stale_extra",
                row,
                baseline_source=baseline_source,
                baseline_decisions=baseline_decisions,
                baseline_picks=baseline_picks,
            )

        if baseline_picks > 0:
            summary["baseline"]["nonzero_pick_run_count"] += 1
            if not metric_exists or metric_snapshots == 0:
                summary["pick_metric_snapshots"]["missing_nonzero_run_count"] += 1
                summary["pick_metric_snapshots"]["refresh_candidate_count"] += 1
                _append_sample(
                    "metric_refresh_candidates",
                    row,
                    baseline_source=baseline_source,
                    baseline_decisions=baseline_decisions,
                    baseline_picks=baseline_picks,
                )
            elif metric_snapshots != baseline_picks:
                summary["pick_metric_snapshots"]["mismatch_run_count"] += 1
                summary["pick_metric_snapshots"]["refresh_candidate_count"] += 1
                _append_sample(
                    "metric_refresh_candidates",
                    row,
                    baseline_source=baseline_source,
                    baseline_decisions=baseline_decisions,
                    baseline_picks=baseline_picks,
                )
            else:
                summary["pick_metric_snapshots"]["consistent_nonzero_run_count"] += 1
        elif metric_snapshots > 0:
            summary["pick_metric_snapshots"]["stale_extra_run_count"] += 1
            _append_sample(
                "metric_stale_extra",
                row,
                baseline_source=baseline_source,
                baseline_decisions=baseline_decisions,
                baseline_picks=baseline_picks,
            )

    if (
        summary["baseline"]["baseline_missing_run_count"] > 0
        or not reason_exists
        or not metric_exists
        or summary["decision_reason_summary"]["refresh_candidate_count"] > 0
        or summary["decision_reason_summary"]["stale_extra_run_count"] > 0
        or summary["pick_metric_snapshots"]["refresh_candidate_count"] > 0
        or summary["pick_metric_snapshots"]["stale_extra_run_count"] > 0
    ):
        summary["status"] = "review"

    return {
        "summary": summary,
        "samples": {key: value for key, value in samples.items() if value},
    }


def fetch_table_totals(conn: sqlite3.Connection) -> Dict[str, object]:
    """统计核心表总行数，优先使用 run 级物化摘要。"""
    totals: Dict[str, object] = {}
    run_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM screening_runs").fetchone()["cnt"] or 0)
    totals["screening_runs"] = run_count

    for summary_table in (LATEST_ACTIVITY_TABLE, RUN_STORAGE_STATS_TABLE):
        if not sqlite_table_exists(conn, summary_table):
            continue
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS run_count,
                COALESCE(SUM(decision_count), 0) AS decision_total,
                COALESCE(SUM(pick_count), 0) AS pick_total
            FROM {summary_table}
            """
        ).fetchone()
        summary_run_count = int(row["run_count"] or 0)
        if summary_run_count == run_count:
            totals["screening_decisions"] = int(row["decision_total"] or 0)
            totals["screening_picks"] = int(row["pick_total"] or 0)
            totals["count_source"] = summary_table
            return totals

    for table_name in ("screening_decisions", "screening_picks"):
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()
        totals[table_name] = int(row["cnt"] or 0)
    totals["count_source"] = "detail_scan"
    return totals


def build_run_metrics(
    run_rows: Sequence[dict],
    decision_count_by_run: Optional[Dict[str, int]] = None,
    pick_count_by_run: Optional[Dict[str, int]] = None,
) -> List[dict]:
    """将 run 元数据与 decision/pick 计数合并。"""
    metrics: List[dict] = []
    decision_count_by_run = decision_count_by_run or {}
    pick_count_by_run = pick_count_by_run or {}

    for run in run_rows:
        run_id = str(run.get("run_id") or "").strip()
        started_at = str(run.get("started_at") or "").strip()
        started_day = started_at[:10] if len(started_at) >= 10 else "unknown"
        persist_mode = parse_decision_persist_mode(run.get("config_snapshot_json"))
        decision_count = int(run.get("decision_count", decision_count_by_run.get(run_id, 0)) or 0)
        pick_count = int(run.get("pick_count", pick_count_by_run.get(run_id, 0)) or 0)
        count_source = str(
            run.get("count_source")
            or ("input_row" if "decision_count" in run or "pick_count" in run else "runs_only")
        )

        metrics.append({
            "run_id": run_id,
            "started_at": started_at,
            "started_day": started_day,
            "status": str(run.get("status") or ""),
            "fund_type": str(run.get("fund_type") or "unknown"),
            "period_years": run.get("period_years"),
            "fund_count_in": int(run.get("fund_count_in") or 0),
            "fund_count_pass": int(run.get("fund_count_pass") or 0),
            "fund_count_fail": int(run.get("fund_count_fail") or 0),
            "decision_persist_mode": persist_mode,
            "decision_count": decision_count,
            "pick_count": pick_count,
            "count_source": count_source,
            "decisions_per_pick": round(decision_count / pick_count, 4) if pick_count > 0 else None,
            "decisions_per_input_fund": round(decision_count / int(run.get("fund_count_in") or 1), 4)
            if int(run.get("fund_count_in") or 0) > 0 else None,
            "code_version": run.get("code_version"),
        })

    return metrics


def aggregate_daily_growth(run_metrics: Sequence[dict]) -> List[dict]:
    """按日 × fund_type × decision_persist_mode 聚合增长指标。"""
    grouped: Dict[Tuple[str, str, str], dict] = {}

    for row in run_metrics:
        key = (
            str(row.get("started_day") or "unknown"),
            str(row.get("fund_type") or "unknown"),
            str(row.get("decision_persist_mode") or "legacy_or_all"),
        )
        bucket = grouped.setdefault(key, {
            "date": key[0],
            "fund_type": key[1],
            "decision_persist_mode": key[2],
            "run_count": 0,
            "success_run_count": 0,
            "failed_run_count": 0,
            "fund_count_in_total": 0,
            "fund_count_pass_total": 0,
            "decision_count_total": 0,
            "pick_count_total": 0,
        })

        bucket["run_count"] += 1
        bucket["success_run_count"] += 1 if row.get("status") == "success" else 0
        bucket["failed_run_count"] += 1 if row.get("status") == "failed" else 0
        bucket["fund_count_in_total"] += int(row.get("fund_count_in") or 0)
        bucket["fund_count_pass_total"] += int(row.get("fund_count_pass") or 0)
        bucket["decision_count_total"] += int(row.get("decision_count") or 0)
        bucket["pick_count_total"] += int(row.get("pick_count") or 0)

    daily_rows = []
    for bucket in grouped.values():
        run_count = int(bucket["run_count"] or 0)
        pick_count_total = int(bucket["pick_count_total"] or 0)
        decision_count_total = int(bucket["decision_count_total"] or 0)

        bucket["avg_decisions_per_run"] = round(decision_count_total / run_count, 4) if run_count > 0 else 0.0
        bucket["avg_picks_per_run"] = round(pick_count_total / run_count, 4) if run_count > 0 else 0.0
        bucket["avg_decisions_per_pick"] = (
            round(decision_count_total / pick_count_total, 4) if pick_count_total > 0 else None
        )
        daily_rows.append(bucket)

    return sorted(
        daily_rows,
        key=lambda item: (item["date"], item["fund_type"], item["decision_persist_mode"]),
        reverse=True,
    )


def summarize_storage_growth(run_metrics: Sequence[dict], table_totals: Dict[str, int]) -> dict:
    """生成审计摘要。"""
    summary = {
        "run_count": len(run_metrics),
        "success_run_count": 0,
        "failed_run_count": 0,
        "decision_count_total": 0,
        "pick_count_total": 0,
        "table_totals": dict(table_totals or {}),
        "persist_mode_breakdown": {},
        "count_source_breakdown": {},
    }

    breakdown = defaultdict(lambda: {
        "run_count": 0,
        "decision_count_total": 0,
        "pick_count_total": 0,
    })
    source_breakdown = defaultdict(lambda: {
        "run_count": 0,
        "decision_count_total": 0,
        "pick_count_total": 0,
    })

    for row in run_metrics:
        status = str(row.get("status") or "")
        if status == "success":
            summary["success_run_count"] += 1
        elif status == "failed":
            summary["failed_run_count"] += 1

        decision_count = int(row.get("decision_count") or 0)
        pick_count = int(row.get("pick_count") or 0)
        persist_mode = str(row.get("decision_persist_mode") or "legacy_or_all")
        count_source = str(row.get("count_source") or "unknown")

        summary["decision_count_total"] += decision_count
        summary["pick_count_total"] += pick_count

        breakdown[persist_mode]["run_count"] += 1
        breakdown[persist_mode]["decision_count_total"] += decision_count
        breakdown[persist_mode]["pick_count_total"] += pick_count
        source_breakdown[count_source]["run_count"] += 1
        source_breakdown[count_source]["decision_count_total"] += decision_count
        source_breakdown[count_source]["pick_count_total"] += pick_count

    normalized_breakdown = {}
    for persist_mode, bucket in breakdown.items():
        pick_total = int(bucket["pick_count_total"] or 0)
        decision_total = int(bucket["decision_count_total"] or 0)
        normalized_breakdown[persist_mode] = {
            **bucket,
            "avg_decisions_per_pick": round(decision_total / pick_total, 4) if pick_total > 0 else None,
        }
    summary["persist_mode_breakdown"] = normalized_breakdown
    summary["count_source_breakdown"] = dict(source_breakdown)

    return summary


def build_storage_growth_findings(summary: dict) -> List[str]:
    """生成 storage growth 审计发现。"""
    findings: List[str] = []
    coverage = dict(summary.get("materialized_coverage") or {})
    storage_stats = dict(coverage.get(RUN_STORAGE_STATS_TABLE) or {})
    latest_activity = dict(coverage.get(LATEST_ACTIVITY_TABLE) or {})
    count_source_breakdown = dict(summary.get("count_source_breakdown") or {})

    storage_exists = bool(storage_stats.get("exists"))
    storage_missing = int(storage_stats.get("missing_run_count") or 0)
    if storage_exists and storage_missing == 0:
        findings.append("screening_run_storage_stats 覆盖全部 screening_runs，可作为 run 级行数统计主来源。")
    elif storage_exists:
        findings.append(
            "screening_run_storage_stats 未覆盖全部 run，"
            f"missing_run_count={storage_missing}；缺失 run 仍可能回退明细点查。"
        )
    else:
        findings.append("screening_run_storage_stats 缺失，run 级行数统计会回退到明细表。")

    latest_missing = int(latest_activity.get("missing_run_count") or 0)
    if bool(latest_activity.get("exists")) and latest_missing > 0:
        findings.append(
            "screening_run_latest_activity 存在缺口，"
            f"missing_run_count={latest_missing}；本审计会优先用 storage_stats 补齐计数。"
        )
    gap_analysis = dict(summary.get("latest_activity_gap_analysis") or {})
    if gap_analysis:
        refresh_candidate_count = int(gap_analysis.get("refresh_candidate_count") or 0)
        unresolved_count = int(gap_analysis.get("unresolved_count") or 0)
        storage_primary_count = int(gap_analysis.get("storage_stats_primary_count") or 0)
        if refresh_candidate_count > 0:
            findings.append(
                "latest_activity 缺口中仍有热库明细可刷新，"
                f"refresh_candidate_count={refresh_candidate_count}；应运行 decision summary 刷新而不是扫大表。"
            )
        if unresolved_count > 0:
            findings.append(
                "latest_activity 缺口中存在无 storage_stats 且无热库明细的 run，"
                f"unresolved_count={unresolved_count}；需要先确认历史归档或数据源状态。"
            )
        if (
            int(gap_analysis.get("missing_run_count") or 0) > 0
            and refresh_candidate_count == 0
            and unresolved_count == 0
            and storage_primary_count > 0
        ):
            findings.append(
                "latest_activity 缺口均无热库明细且已有 storage_stats 覆盖，"
                f"storage_stats_primary_count={storage_primary_count}；不伪造明细时间字段。"
            )

    detail_fallback_runs = 0
    for source_name, bucket in count_source_breakdown.items():
        if str(source_name).startswith("detail_scan"):
            detail_fallback_runs += int((bucket or {}).get("run_count") or 0)
    if detail_fallback_runs > 0:
        findings.append(
            "本次窗口仍有 run 使用明细表回退计数，"
            f"run_count={detail_fallback_runs}；应刷新 run 级物化摘要后再扩大查询复用。"
        )
    else:
        findings.append("本次窗口未使用 screening_decisions/screening_picks 明细回退计数。")

    snapshot_coverage = dict(summary.get("summary_snapshot_coverage") or {})
    if snapshot_coverage:
        baseline = dict(snapshot_coverage.get("baseline") or {})
        reason_summary = dict(snapshot_coverage.get("decision_reason_summary") or {})
        metric_summary = dict(snapshot_coverage.get("pick_metric_snapshots") or {})
        baseline_missing = int(baseline.get("baseline_missing_run_count") or 0)
        reason_refresh_candidates = int(reason_summary.get("refresh_candidate_count") or 0)
        reason_stale_extra = int(reason_summary.get("stale_extra_run_count") or 0)
        metric_refresh_candidates = int(metric_summary.get("refresh_candidate_count") or 0)
        metric_stale_extra = int(metric_summary.get("stale_extra_run_count") or 0)
        if baseline_missing > 0:
            findings.append(
                "summary snapshot 覆盖审计存在缺少 run 级基准的 run，"
                f"baseline_missing_run_count={baseline_missing}；需先补齐 storage stats 或 latest activity。"
            )
        if reason_refresh_candidates > 0 or metric_refresh_candidates > 0:
            findings.append(
                "summary snapshot 覆盖审计发现可刷新候选，"
                f"reason_refresh_candidates={reason_refresh_candidates}，"
                f"metric_refresh_candidates={metric_refresh_candidates}；"
                "后续策略分析应先刷新摘要，避免回退扫明细表。"
            )
        if reason_stale_extra > 0 or metric_stale_extra > 0:
            findings.append(
                "summary snapshot 覆盖审计发现摘要与 run 级基准不一致的残留，"
                f"reason_stale_extra={reason_stale_extra}，"
                f"metric_stale_extra={metric_stale_extra}；需随历史 cleanup/refresh 对齐。"
            )
        if (
            baseline_missing == 0
            and reason_refresh_candidates == 0
            and reason_stale_extra == 0
            and metric_refresh_candidates == 0
            and metric_stale_extra == 0
            and bool(reason_summary.get("exists"))
            and bool(metric_summary.get("exists"))
        ):
            findings.append(
                "screening_decision_reason_summary 与 screening_pick_metric_snapshots "
                "覆盖当前非零 run，可作为原因分布和 pick 风险快照分析主来源。"
            )

    return findings


def build_top_noisy_runs(run_metrics: Sequence[dict], limit: int = 20) -> List[dict]:
    """筛出 decision 写入量最高的 run。"""
    sorted_rows = sorted(
        run_metrics,
        key=lambda item: (
            int(item.get("decision_count") or 0),
            int(item.get("pick_count") or 0),
            str(item.get("started_at") or ""),
        ),
        reverse=True,
    )
    top_rows = []
    for row in sorted_rows[: max(1, int(limit))]:
        top_rows.append({
            "run_id": row.get("run_id"),
            "started_at": row.get("started_at"),
            "fund_type": row.get("fund_type"),
            "status": row.get("status"),
            "period_years": row.get("period_years"),
            "decision_persist_mode": row.get("decision_persist_mode"),
            "fund_count_in": row.get("fund_count_in"),
            "fund_count_pass": row.get("fund_count_pass"),
            "decision_count": row.get("decision_count"),
            "pick_count": row.get("pick_count"),
            "count_source": row.get("count_source"),
            "decisions_per_pick": row.get("decisions_per_pick"),
        })
    return top_rows


def build_screening_storage_growth_audit_payload(
    *,
    db_path: Optional[str] = None,
    days: int = 14,
    fund_types: Optional[Sequence[str]] = None,
    top_runs: int = 20,
    now: Optional[datetime] = None,
    include_row_counts: bool = True,
    max_runs: Optional[int] = None,
    include_table_totals: bool = True,
) -> dict:
    """构建完整审计载荷。"""
    with open_readonly_db(db_path) as conn:
        if include_row_counts:
            run_rows = fetch_recent_run_metrics_rows(
                conn,
                days=days,
                fund_types=fund_types,
                now=now,
                limit=max_runs,
            )
        else:
            run_rows = fetch_recent_run_rows(
                conn,
                days=days,
                fund_types=fund_types,
                now=now,
                limit=max_runs,
            )
        table_totals = fetch_table_totals(conn) if include_table_totals else {}
        materialized_coverage = fetch_materialized_coverage_summary(conn)
        latest_activity_gap_analysis = fetch_latest_activity_gap_analysis(conn)
        summary_snapshot_coverage = fetch_summary_snapshot_coverage_analysis(conn)

    run_metrics = build_run_metrics(run_rows)
    daily_rows = aggregate_daily_growth(run_metrics)
    summary = summarize_storage_growth(run_metrics, table_totals)
    summary["materialized_coverage"] = materialized_coverage
    summary["latest_activity_gap_analysis"] = latest_activity_gap_analysis.get("summary") or {}
    summary["summary_snapshot_coverage"] = summary_snapshot_coverage.get("summary") or {}
    findings = build_storage_growth_findings(summary)

    resolved_db_path = resolve_db_path(db_path or "database/bond_funds.db")
    db_file_size_bytes = Path(resolved_db_path).stat().st_size if Path(resolved_db_path).exists() else 0

    return {
        "status": "success",
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "db_path": resolved_db_path,
            "db_file_size_bytes": db_file_size_bytes,
            "days": int(days),
            "fund_types": [str(item).strip() for item in (fund_types or []) if str(item).strip()],
            "top_runs": int(top_runs),
            "count_mode": "full" if include_row_counts else "runs_only",
            "max_runs": int(max_runs) if max_runs is not None else None,
            "includes_table_totals": bool(include_table_totals),
        },
        "summary": summary,
        "findings": findings,
        "latest_activity_gap_samples": latest_activity_gap_analysis.get("samples") or {},
        "summary_snapshot_coverage_samples": summary_snapshot_coverage.get("samples") or {},
        "daily_rows": daily_rows,
        "top_noisy_runs": build_top_noisy_runs(run_metrics, limit=top_runs),
    }


def render_screening_storage_growth_markdown(payload: dict) -> str:
    """渲染 Markdown 审计报告。"""
    meta = payload.get("meta") or {}
    summary = payload.get("summary") or {}
    daily_rows = payload.get("daily_rows") or []
    top_runs = payload.get("top_noisy_runs") or []

    lines = [
        "# screening storage growth audit",
        "",
        "## meta",
        f"- generated_at: {meta.get('generated_at')}",
        f"- db_path: {meta.get('db_path')}",
        f"- db_file_size_bytes: {meta.get('db_file_size_bytes', 0)}",
        f"- days: {meta.get('days')}",
        f"- fund_types: {', '.join(meta.get('fund_types') or ['all'])}",
        "",
        "## summary",
        f"- run_count: {summary.get('run_count', 0)}",
        f"- success_run_count: {summary.get('success_run_count', 0)}",
        f"- failed_run_count: {summary.get('failed_run_count', 0)}",
        f"- decision_count_total: {summary.get('decision_count_total', 0)}",
        f"- pick_count_total: {summary.get('pick_count_total', 0)}",
        f"- table_totals: {json.dumps(summary.get('table_totals', {}), ensure_ascii=False)}",
        f"- materialized_coverage: {json.dumps(summary.get('materialized_coverage', {}), ensure_ascii=False)}",
        f"- latest_activity_gap_analysis: {json.dumps(summary.get('latest_activity_gap_analysis', {}), ensure_ascii=False)}",
        f"- summary_snapshot_coverage: {json.dumps(summary.get('summary_snapshot_coverage', {}), ensure_ascii=False)}",
        f"- count_source_breakdown: {json.dumps(summary.get('count_source_breakdown', {}), ensure_ascii=False)}",
        "",
        "## findings",
    ]

    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## persist_mode_breakdown"])
    for persist_mode, bucket in sorted((summary.get("persist_mode_breakdown") or {}).items()):
        lines.append(
            "- "
            + json.dumps(
                {"decision_persist_mode": persist_mode, **bucket},
                ensure_ascii=False,
                sort_keys=True,
            )
        )

    lines.extend(["", "## top_daily_rows"])
    for row in daily_rows[:20]:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## top_noisy_runs"])
    for row in top_runs[:20]:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    return "\n".join(lines) + "\n"


def write_screening_storage_growth_artifacts(payload: dict, output_dir: str = DEFAULT_AUDIT_DIR) -> Tuple[Path, Path]:
    """写出 JSON + Markdown 产物。"""
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (Path.cwd() / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"screening_storage_growth_audit_{ts}.json"
    md_path = artifact_dir / f"screening_storage_growth_audit_{ts}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_storage_growth_markdown(payload), encoding="utf-8")
    return json_path, md_path
