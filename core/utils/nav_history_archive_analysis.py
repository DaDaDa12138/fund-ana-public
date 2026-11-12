#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only NAV history storage and archive planning helpers."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from sqlalchemy import func

from database.connection import get_db_manager, resolve_db_path
from database.models import FundNavHistory
from utils.nav_history_archive_maintenance_defaults import (
    build_nav_history_archive_target_path,
    load_nav_history_archive_maintenance_defaults,
)


DEFAULT_RETENTION_YEAR_OPTIONS = (3, 4, 5, 6, 7)
DEFAULT_RECOMMENDED_RETENTION_YEARS = 5
DEFAULT_TOP_LIMIT = 20


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_date(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    return str(value).strip()


def _parse_iso_date(value: Any) -> Optional[date]:
    text = _format_date(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - int(years))
    except ValueError:
        # Leap day fallback.
        return value.replace(month=2, day=28, year=value.year - int(years))


def _round_mb(num_bytes: int | float) -> float:
    return round(float(num_bytes or 0) / 1024 / 1024, 2)


def _manager_db_path(manager) -> str:
    return str(Path(getattr(manager, "db_path", resolve_db_path("database/bond_funds.db"))).resolve())


@contextmanager
def _sqlite_readonly_connection(manager):
    """Open a sqlite connection for PRAGMA/dbstat reads without mutating WAL mode."""
    db_path = str(getattr(manager, "db_path", "") or "")
    if db_path and db_path != ":memory:" and Path(db_path).exists():
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
        return

    raw = manager.engine.raw_connection()
    try:
        raw.row_factory = sqlite3.Row
        yield raw
    finally:
        raw.close()


def _fetch_all_dicts(conn, sql: str, params: Iterable[Any] = ()) -> list[dict]:
    cursor = conn.execute(sql, tuple(params))
    rows = cursor.fetchall()
    result = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            columns = [item[0] for item in cursor.description or []]
            result.append(dict(zip(columns, row)))
    return result


def _fetch_one_value(conn, sql: str, params: Iterable[Any] = (), default: Any = None) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return default
    return row[0]


def _table_names(conn) -> list[str]:
    rows = _fetch_all_dicts(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    return [str(row.get("name") or "").strip() for row in rows if str(row.get("name") or "").strip()]


def _index_table_map(conn, table_names: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table_name in table_names:
        try:
            rows = _fetch_all_dicts(conn, f"PRAGMA index_list({table_name})")
        except sqlite3.Error:
            continue
        for row in rows:
            index_name = str(row.get("name") or "").strip()
            if index_name:
                mapping[index_name] = table_name
    return mapping


def _dbstat_sizes(conn) -> dict[str, int]:
    try:
        rows = _fetch_all_dicts(
            conn,
            "SELECT name, SUM(pgsize) AS size_bytes FROM dbstat GROUP BY name",
        )
    except sqlite3.Error:
        return {}
    return {
        str(row.get("name") or "").strip(): int(row.get("size_bytes") or 0)
        for row in rows
        if str(row.get("name") or "").strip()
    }


def _estimate_size_mb(row_count: int, total_rows: int, total_size_mb: float) -> float:
    if row_count <= 0 or total_rows <= 0 or total_size_mb <= 0:
        return 0.0
    return round((float(row_count) / float(total_rows)) * float(total_size_mb), 2)


def _get_sqlite_storage_metrics(manager=None) -> dict:
    """Return read-only physical SQLite metrics expected by governance tools."""
    manager = manager or get_db_manager()
    db_path = _manager_db_path(manager)
    db_size_bytes = os.path.getsize(db_path) if db_path != ":memory:" and Path(db_path).exists() else 0
    wal_path = f"{db_path}-wal"
    shm_path = f"{db_path}-shm"

    with _sqlite_readonly_connection(manager) as conn:
        page_size = _normalize_int(_fetch_one_value(conn, "PRAGMA page_size", default=0))
        page_count = _normalize_int(_fetch_one_value(conn, "PRAGMA page_count", default=0))
        freelist_count = _normalize_int(_fetch_one_value(conn, "PRAGMA freelist_count", default=0))

    freelist_bytes = page_size * freelist_count
    return {
        "database_path": db_path,
        "database_size_bytes": db_size_bytes,
        "database_size_mb": _round_mb(db_size_bytes),
        "database_live_size_bytes": db_size_bytes,
        "database_live_size_mb": _round_mb(db_size_bytes),
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "freelist_bytes": freelist_bytes,
        "freelist_mb": _round_mb(freelist_bytes),
        "wal_size_bytes": os.path.getsize(wal_path) if Path(wal_path).exists() else 0,
        "shm_size_bytes": os.path.getsize(shm_path) if Path(shm_path).exists() else 0,
    }


def build_primary_database_hotspots(manager=None, *, top_limit: int = DEFAULT_TOP_LIMIT) -> dict:
    """Build table/index size hotspots from sqlite dbstat when available."""
    manager = manager or get_db_manager()
    db_path = _manager_db_path(manager)
    with _sqlite_readonly_connection(manager) as conn:
        table_names = _table_names(conn)
        index_mapping = _index_table_map(conn, table_names)
        sizes = _dbstat_sizes(conn)

    table_rows = [
        {
            "name": name,
            "size_bytes": int(sizes.get(name, 0)),
            "size_mb": _round_mb(sizes.get(name, 0)),
        }
        for name in table_names
    ]
    table_rows.sort(key=lambda item: item.get("size_bytes") or 0, reverse=True)

    index_rows = []
    for index_name, table_name in index_mapping.items():
        size_bytes = int(sizes.get(index_name, 0))
        index_rows.append(
            {
                "name": index_name,
                "table_name": table_name,
                "size_bytes": size_bytes,
                "size_mb": _round_mb(size_bytes),
            }
        )
    index_rows.sort(key=lambda item: item.get("size_bytes") or 0, reverse=True)

    total_size_bytes = sum(item.get("size_bytes") or 0 for item in table_rows + index_rows)
    return {
        "status": "ok" if sizes else "dbstat_unavailable",
        "database_path": db_path,
        "dbstat_available": bool(sizes),
        "largest_table": table_rows[0] if table_rows else {},
        "top_tables": table_rows[: max(1, int(top_limit))],
        "top_indexes": index_rows[: max(1, int(top_limit))],
        "object_size_total_bytes": total_size_bytes,
        "object_size_total_mb": _round_mb(total_size_bytes),
    }


def _build_nav_history_aggregate_snapshot(manager=None) -> dict:
    """Return aggregate NAV history facts without changing database state."""
    manager = manager or get_db_manager()
    with manager.get_session(task_name="nav_history_aggregate_snapshot", readonly=True) as session:
        row = (
            session.query(
                func.count(FundNavHistory.id),
                func.count(func.distinct(FundNavHistory.fcode)),
                func.min(FundNavHistory.nav_date),
                func.max(FundNavHistory.nav_date),
            )
            .one()
        )
    row_count = _normalize_int(row[0])
    fund_count = _normalize_int(row[1])
    return {
        "nav_history_row_count": row_count,
        "nav_history_fund_count": fund_count,
        "earliest_nav_date": _format_date(row[2]),
        "latest_nav_date": _format_date(row[3]),
        "avg_rows_per_fund": round(row_count / fund_count, 2) if fund_count else 0.0,
    }


def _nav_history_year_distribution(manager=None) -> list[dict]:
    manager = manager or get_db_manager()
    with _sqlite_readonly_connection(manager) as conn:
        rows = _fetch_all_dicts(
            conn,
            """
            SELECT strftime('%Y', nav_date) AS year,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT fcode) AS fund_count
            FROM fund_nav_history
            GROUP BY strftime('%Y', nav_date)
            ORDER BY year
            """,
        )
    return [
        {
            "year": str(row.get("year") or "").strip(),
            "row_count": _normalize_int(row.get("row_count")),
            "fund_count": _normalize_int(row.get("fund_count")),
        }
        for row in rows
    ]


def build_nav_history_storage_profile(
    manager=None,
    *,
    aggregate_snapshot: Optional[dict] = None,
) -> dict:
    """Build a read-only NAV history profile for archive planning."""
    manager = manager or get_db_manager()
    snapshot = _normalize_dict(aggregate_snapshot) or _build_nav_history_aggregate_snapshot(manager)
    year_distribution = _nav_history_year_distribution(manager)

    status = "empty"
    row_count = _normalize_int(snapshot.get("nav_history_row_count"))
    fund_count = _normalize_int(snapshot.get("nav_history_fund_count"))
    avg_rows_per_fund = _normalize_float(snapshot.get("avg_rows_per_fund"))
    if row_count > 0:
        status = "long_history_accumulation"
    if avg_rows_per_fund >= 1500:
        status = "dense_fund_outliers"

    return {
        "available": row_count > 0,
        "status": status,
        "nav_history_row_count": row_count,
        "nav_history_fund_count": fund_count,
        "earliest_nav_date": str(snapshot.get("earliest_nav_date") or ""),
        "latest_nav_date": str(snapshot.get("latest_nav_date") or ""),
        "avg_rows_per_fund": avg_rows_per_fund,
        "year_distribution": year_distribution,
    }


def _nav_history_table_and_index_size_mb(database_hotspots: dict) -> tuple[float, float]:
    hotspots = _normalize_dict(database_hotspots)
    table_size = 0.0
    for item in _normalize_list(hotspots.get("top_tables")):
        row = _normalize_dict(item)
        if str(row.get("name") or "") == "fund_nav_history":
            table_size = _normalize_float(row.get("size_mb"))
            break
    index_size = 0.0
    for item in _normalize_list(hotspots.get("top_indexes")):
        row = _normalize_dict(item)
        if str(row.get("table_name") or "") == "fund_nav_history":
            index_size += _normalize_float(row.get("size_mb"))
    return table_size, round(index_size, 2)


def _count_nav_rows_before(manager, cutoff: date) -> tuple[int, int]:
    with manager.get_session(task_name="nav_history_retention_preview", readonly=True) as session:
        query = session.query(FundNavHistory).filter(FundNavHistory.nav_date < cutoff)
        row_count = _normalize_int(query.with_entities(func.count(FundNavHistory.id)).scalar())
        fund_count = _normalize_int(query.with_entities(func.count(func.distinct(FundNavHistory.fcode))).scalar())
    return row_count, fund_count


def build_nav_history_retention_preview(
    manager=None,
    *,
    database_hotspots: Optional[dict] = None,
    aggregate_snapshot: Optional[dict] = None,
    retention_year_options: Iterable[int] = DEFAULT_RETENTION_YEAR_OPTIONS,
) -> dict:
    """Preview cold NAV rows for multiple retention windows."""
    manager = manager or get_db_manager()
    snapshot = _normalize_dict(aggregate_snapshot) or _build_nav_history_aggregate_snapshot(manager)
    hotspots = _normalize_dict(database_hotspots) or build_primary_database_hotspots(manager)
    total_rows = _normalize_int(snapshot.get("nav_history_row_count"))
    latest_nav_date = _parse_iso_date(snapshot.get("latest_nav_date"))
    table_size_mb, index_size_mb = _nav_history_table_and_index_size_mb(hotspots)
    hot_zone_size_mb = round(table_size_mb + index_size_mb, 2)

    if not latest_nav_date or total_rows <= 0:
        return {
            "available": False,
            "status": "empty",
            "active_recommended_years": DEFAULT_RECOMMENDED_RETENTION_YEARS,
            "retention_options": [],
            "latest_nav_date": str(snapshot.get("latest_nav_date") or ""),
            "earliest_nav_date": str(snapshot.get("earliest_nav_date") or ""),
        }

    options = []
    for years in sorted({int(item) for item in retention_year_options if int(item) > 0}):
        cutoff = _subtract_years(latest_nav_date, years)
        cold_rows, cold_funds = _count_nav_rows_before(manager, cutoff)
        share_pct = round((cold_rows / total_rows) * 100, 2) if total_rows else 0.0
        options.append(
            {
                "retention_years": years,
                "archive_cutoff_date": cutoff.isoformat(),
                "estimated_archive_row_count": cold_rows,
                "estimated_archive_fund_count": cold_funds,
                "estimated_archive_share_pct": share_pct,
                "estimated_archive_table_size_mb": _estimate_size_mb(cold_rows, total_rows, table_size_mb),
                "estimated_archive_index_size_mb": _estimate_size_mb(cold_rows, total_rows, index_size_mb),
                "estimated_archive_size_mb": _estimate_size_mb(cold_rows, total_rows, hot_zone_size_mb),
            }
        )

    return {
        "available": True,
        "status": "ready",
        "latest_nav_date": latest_nav_date.isoformat(),
        "earliest_nav_date": str(snapshot.get("earliest_nav_date") or ""),
        "nav_history_row_count": total_rows,
        "nav_history_table_size_mb": table_size_mb,
        "nav_history_index_size_mb": index_size_mb,
        "nav_history_hot_zone_size_mb": hot_zone_size_mb,
        "active_recommended_years": DEFAULT_RECOMMENDED_RETENTION_YEARS,
        "retention_options": options,
    }


def _pick_retention_option(retention_preview: dict, preferred_years: int) -> dict:
    options = _normalize_list(_normalize_dict(retention_preview).get("retention_options"))
    for item in options:
        row = _normalize_dict(item)
        if _normalize_int(row.get("retention_years")) == int(preferred_years):
            return row
    return _normalize_dict(options[-1]) if options else {}


def build_nav_history_archive_strategy_preview(
    nav_history_storage_profile: dict,
    nav_history_retention_preview: dict,
    *,
    maintenance_defaults: Optional[dict] = None,
) -> dict:
    """Pick the conservative NAV archive strategy for a future manual window."""
    profile = _normalize_dict(nav_history_storage_profile)
    retention = _normalize_dict(nav_history_retention_preview)
    if not profile.get("available") or not retention.get("available"):
        return {
            "available": False,
            "status": "unavailable",
            "message": "NAV history profile or retention preview is unavailable.",
        }

    recommended_years = _normalize_int(
        retention.get("active_recommended_years"),
        DEFAULT_RECOMMENDED_RETENTION_YEARS,
    )
    option = _pick_retention_option(retention, recommended_years)
    archive_cutoff_date = str(option.get("archive_cutoff_date") or "").strip()
    estimated_rows = _normalize_int(option.get("estimated_archive_row_count"))
    estimated_size_mb = _normalize_float(option.get("estimated_archive_size_mb"))
    defaults = maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    archive_target_path = build_nav_history_archive_target_path(
        archive_cutoff_date,
        maintenance_defaults=defaults,
    )

    status = "archive_candidate" if estimated_rows > 0 else "observe"
    message = (
        f"Conservative {recommended_years}y retention has {estimated_rows} cold NAV rows before {archive_cutoff_date}."
        if estimated_rows > 0
        else f"Conservative {recommended_years}y retention currently has no cold NAV rows to archive."
    )
    return {
        "available": True,
        "status": status,
        "message": message,
        "recommended_retention_years": recommended_years,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": estimated_rows,
        "estimated_archive_fund_count": _normalize_int(option.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": estimated_size_mb,
        "estimated_archive_table_size_mb": _normalize_float(option.get("estimated_archive_table_size_mb")),
        "estimated_archive_index_size_mb": _normalize_float(option.get("estimated_archive_index_size_mb")),
        "estimated_archive_share_pct": _normalize_float(option.get("estimated_archive_share_pct")),
        "latest_nav_date": str(retention.get("latest_nav_date") or profile.get("latest_nav_date") or ""),
        "archive_target_path": archive_target_path,
        "archive_table_name": str(defaults.get("archive_table_name") or "fund_nav_history_archive"),
    }


def _sample_nav_values(manager) -> tuple[str, str]:
    with manager.get_session(task_name="nav_history_index_audit_sample", readonly=True) as session:
        fcode = (
            session.query(FundNavHistory.fcode)
            .filter(FundNavHistory.fcode.isnot(None))
            .order_by(FundNavHistory.fcode)
            .limit(1)
            .scalar()
        )
        nav_date = (
            session.query(func.max(FundNavHistory.nav_date))
            .scalar()
        )
    return str(fcode or ""), _format_date(nav_date)


def _explain_query(conn, sql: str, params: Iterable[Any]) -> list[str]:
    try:
        rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", tuple(params)).fetchall()
    except sqlite3.Error as exc:
        return [f"EXPLAIN failed: {exc}"]
    details = []
    for row in rows:
        try:
            details.append(str(row["detail"]))
        except (TypeError, IndexError):
            details.append(str(row[-1]))
    return details


def build_nav_history_index_audit(manager=None, *, index_name: str = "idx_nav_fcode") -> dict:
    """Audit whether legacy NAV indexes are still present or used by query plans."""
    manager = manager or get_db_manager()
    sample_fcode, sample_nav_date = _sample_nav_values(manager)
    with _sqlite_readonly_connection(manager) as conn:
        index_rows = _fetch_all_dicts(conn, "PRAGMA index_list(fund_nav_history)")
        indexes = [
            {
                "name": str(row.get("name") or ""),
                "unique": bool(row.get("unique")),
                "origin": str(row.get("origin") or ""),
            }
            for row in index_rows
        ]
        query_specs = [
            (
                "lookup_by_fcode_ordered",
                "SELECT fcode, nav_date FROM fund_nav_history WHERE fcode = ? ORDER BY nav_date DESC LIMIT 5",
                (sample_fcode,),
            ),
            (
                "count_by_fcode",
                "SELECT COUNT(*) FROM fund_nav_history WHERE fcode = ?",
                (sample_fcode,),
            ),
            (
                "count_by_nav_date",
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date >= ?",
                (sample_nav_date,),
            ),
        ]
        explain_samples = []
        for query_name, sql, params in query_specs:
            explain_samples.append(
                {
                    "query_name": query_name,
                    "sql": sql,
                    "plan": _explain_query(conn, sql, params),
                }
            )

    index_names = {item.get("name") for item in indexes}
    legacy_hits = 0
    any_hits = 0
    for sample in explain_samples:
        plan_text = " ".join(sample.get("plan") or [])
        if index_name in plan_text:
            legacy_hits += 1
        if "INDEX" in plan_text.upper():
            any_hits += 1

    if index_name not in index_names:
        status = "missing"
        runtime_drop_candidate = False
    elif legacy_hits > 0:
        status = "keep"
        runtime_drop_candidate = False
    else:
        status = "review"
        runtime_drop_candidate = True

    return {
        "available": True,
        "status": status,
        "index_name": index_name,
        "index_exists": index_name in index_names,
        "runtime_drop_candidate": runtime_drop_candidate,
        "legacy_used_by_query_count": legacy_hits,
        "used_by_query_count": any_hits,
        "sample_fcode": sample_fcode,
        "sample_nav_date": sample_nav_date,
        "indexes": indexes,
        "query_plan_samples": explain_samples,
    }
