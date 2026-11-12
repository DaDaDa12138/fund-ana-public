#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only fund_rank_history storage and archive-readiness audit."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple
from urllib.parse import quote

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/rank_history_governance"
RANK_HISTORY_GOVERNANCE_AUDIT_JSON_PATTERN = "rank_history_governance_audit_*.json"
DEFAULT_RETENTION_YEAR_OPTIONS = (1, 2, 3, 4, 5)
DEFAULT_RECOMMENDED_RETENTION_YEARS = 2
DEFAULT_TOP_LIMIT = 20
PRODUCTION_DEPENDENCY_SPECS = (
    {
        "dependency_id": "long_term_rank_presence_and_staleness",
        "file": "backend/long_term_history_maintenance.py",
        "purpose": "rank presence and MAX(rank_date) gap classification",
        "required_snippets": (
            "build_rank_history_union_source_sql",
            "load_latest_rank_history_archive_binding",
            "SELECT DISTINCT fcode FROM ({rank_source_sql})",
            "read_latest_rank_dates(conn, rank_archive_attach)",
        ),
    },
    {
        "dependency_id": "report_long_term_rank_coverage",
        "file": "backend/report_builder.py",
        "purpose": "rank coverage, latest rank date, and feature readiness counts",
        "required_snippets": (
            "load_latest_rank_history_archive_binding",
            "attach_rank_history_archive_if_available",
            "_aggregate_long_term_history_rank_snapshot_stats",
            "_load_cached_long_term_history_rank_snapshot_stats",
        ),
    },
    {
        "dependency_id": "report_rank_12m_metrics",
        "file": "backend/report_builder.py",
        "purpose": "latest-rank anchored 12 month percentile metrics",
        "required_snippets": (
            "_read_long_term_rank_metrics_from_hot_using_snapshot",
            "_read_long_term_rank_metrics_from_archive_missing_hot_using_snapshot",
            "_read_long_term_rank_metrics_from_archive_missing_hot",
            "rank_archive_attach",
        ),
    },
)


def load_latest_rank_history_governance_audit_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        RANK_HISTORY_GOVERNANCE_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _read_project_text(project_root: str | Path | None, relative_path: str) -> str:
    root = Path(project_root).resolve() if project_root else PROJECT_ROOT
    try:
        return (root / relative_path).read_text(encoding="utf-8")
    except Exception:
        return ""


def build_rank_history_production_dependencies(
    *,
    project_root: str | Path | None = None,
) -> list[dict]:
    """Inspect current production code for rank cold-query compatibility."""
    rows = []
    for spec in PRODUCTION_DEPENDENCY_SPECS:
        row = dict(spec)
        snippets = tuple(row.pop("required_snippets", ()) or ())
        source_text = _read_project_text(project_root, _normalize_text(row.get("file")))
        missing = [snippet for snippet in snippets if snippet not in source_text]
        row["cold_query_ready"] = bool(source_text) and not missing
        row["missing_snippet_count"] = len(missing)
        row["missing_snippets"] = missing[:5]
        rows.append(row)
    return rows


def _round_mb(num_bytes: int | float) -> float:
    return round(float(num_bytes or 0) / 1024 / 1024, 2)


def _parse_iso_date(value: Any) -> Optional[date]:
    text = _normalize_text(value)
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
        return value.replace(month=2, day=28, year=value.year - int(years))


def _resolve_db_path(db_path: str | Path | None) -> str:
    return resolve_db_path(str(db_path or "database/bond_funds.db"))


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


@contextmanager
def _readonly_connection(db_path: str | Path | None):
    resolved = _resolve_db_path(db_path)
    if resolved == ":memory:":
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(_sqlite_readonly_uri(Path(resolved)), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=120000")
    except sqlite3.Error:
        pass
    try:
        yield conn
    finally:
        conn.close()


def _fetch_all_dicts(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[dict]:
    cursor = conn.execute(sql, tuple(params))
    columns = [item[0] for item in cursor.description or []]
    rows = []
    for row in cursor.fetchall():
        if isinstance(row, sqlite3.Row):
            rows.append(dict(row))
        else:
            rows.append(dict(zip(columns, row)))
    return rows


def _fetch_one_dict(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> dict:
    rows = _fetch_all_dicts(conn, sql, params)
    return rows[0] if rows else {}


def _fetch_one_value(
    conn: sqlite3.Connection,
    sql: str,
    params: Iterable[Any] = (),
    default: Any = None,
) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    return default if row is None else row[0]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    try:
        rows = _fetch_all_dicts(conn, f"PRAGMA table_info({table_name})")
    except sqlite3.Error:
        return set()
    return {_normalize_text(row.get("name")) for row in rows if _normalize_text(row.get("name"))}


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = _fetch_all_dicts(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    return [_normalize_text(row.get("name")) for row in rows if _normalize_text(row.get("name"))]


def _index_table_map(conn: sqlite3.Connection, table_names: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table_name in table_names:
        try:
            rows = _fetch_all_dicts(conn, f"PRAGMA index_list({table_name})")
        except sqlite3.Error:
            continue
        for row in rows:
            index_name = _normalize_text(row.get("name"))
            if index_name:
                mapping[index_name] = table_name
    return mapping


def _dbstat_sizes(conn: sqlite3.Connection) -> dict[str, int]:
    try:
        rows = _fetch_all_dicts(
            conn,
            "SELECT name, SUM(pgsize) AS size_bytes FROM dbstat GROUP BY name",
        )
    except sqlite3.Error:
        return {}
    return {
        _normalize_text(row.get("name")): _normalize_int(row.get("size_bytes"))
        for row in rows
        if _normalize_text(row.get("name"))
    }


def _build_sqlite_storage_metrics(db_path: str | Path | None) -> dict:
    resolved = _resolve_db_path(db_path)
    db_size_bytes = (
        os.path.getsize(resolved)
        if resolved != ":memory:" and Path(resolved).exists()
        else 0
    )
    with _readonly_connection(resolved) as conn:
        page_size = _normalize_int(_fetch_one_value(conn, "PRAGMA page_size", default=0))
        page_count = _normalize_int(_fetch_one_value(conn, "PRAGMA page_count", default=0))
        freelist_count = _normalize_int(_fetch_one_value(conn, "PRAGMA freelist_count", default=0))
        quick_check = _normalize_text(_fetch_one_value(conn, "PRAGMA quick_check", default=""))
    return {
        "database_path": resolved,
        "database_size_bytes": db_size_bytes,
        "database_size_mb": _round_mb(db_size_bytes),
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "freelist_bytes": page_size * freelist_count,
        "freelist_mb": _round_mb(page_size * freelist_count),
        "quick_check": quick_check,
    }


def _build_database_hotspots(
    conn: sqlite3.Connection,
    *,
    database_path: str,
    top_limit: int = DEFAULT_TOP_LIMIT,
) -> dict:
    table_names = _table_names(conn)
    index_mapping = _index_table_map(conn, table_names)
    sizes = _dbstat_sizes(conn)
    table_rows = [
        {
            "name": name,
            "size_bytes": _normalize_int(sizes.get(name)),
            "size_mb": _round_mb(sizes.get(name, 0)),
        }
        for name in table_names
    ]
    table_rows.sort(key=lambda item: item.get("size_bytes") or 0, reverse=True)
    index_rows = []
    for index_name, table_name in index_mapping.items():
        size_bytes = _normalize_int(sizes.get(index_name))
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
        "database_path": database_path,
        "dbstat_available": bool(sizes),
        "largest_table": table_rows[0] if table_rows else {},
        "top_tables": table_rows[: max(1, int(top_limit))],
        "top_indexes": index_rows[: max(1, int(top_limit))],
        "object_size_total_bytes": total_size_bytes,
        "object_size_total_mb": _round_mb(total_size_bytes),
    }


def _find_table_size(database_hotspots: dict, table_name: str) -> float:
    for item in _normalize_list(_normalize_dict(database_hotspots).get("top_tables")):
        row = _normalize_dict(item)
        if _normalize_text(row.get("name")) == table_name:
            return _normalize_float(row.get("size_mb"))
    return 0.0


def _collect_table_indexes(database_hotspots: dict, table_name: str) -> list[dict]:
    rows = []
    for item in _normalize_list(_normalize_dict(database_hotspots).get("top_indexes")):
        row = _normalize_dict(item)
        if _normalize_text(row.get("table_name")) == table_name:
            rows.append(row)
    return rows


def _estimate_size_mb(row_count: int, total_rows: int, total_size_mb: float) -> float:
    if row_count <= 0 or total_rows <= 0 or total_size_mb <= 0:
        return 0.0
    return round((float(row_count) / float(total_rows)) * float(total_size_mb), 2)


def _build_rank_history_profile(conn: sqlite3.Connection) -> dict:
    if not _table_exists(conn, "fund_rank_history"):
        return {
            "available": False,
            "status": "table_missing",
            "rank_history_row_count": 0,
            "rank_history_fund_count": 0,
            "earliest_rank_date": "",
            "latest_rank_date": "",
            "avg_rows_per_fund": 0.0,
            "columns": [],
            "year_distribution": [],
            "period_distribution": [],
        }
    columns = _table_columns(conn, "fund_rank_history")
    has_fcode = "fcode" in columns
    has_rank_date = "rank_date" in columns
    has_period = "period" in columns
    aggregate = _fetch_one_dict(
        conn,
        f"""
        SELECT
            COUNT(*) AS row_count
            {', COUNT(DISTINCT fcode) AS fund_count' if has_fcode else ', 0 AS fund_count'}
            {', MIN(rank_date) AS earliest_rank_date, MAX(rank_date) AS latest_rank_date' if has_rank_date else ", '' AS earliest_rank_date, '' AS latest_rank_date"}
        FROM fund_rank_history
        """,
    )
    row_count = _normalize_int(aggregate.get("row_count"))
    fund_count = _normalize_int(aggregate.get("fund_count"))
    year_distribution = []
    if has_rank_date:
        year_distribution = [
            {
                "year": _normalize_text(row.get("year")),
                "row_count": _normalize_int(row.get("row_count")),
                "fund_count": _normalize_int(row.get("fund_count")),
            }
            for row in _fetch_all_dicts(
                conn,
                f"""
                SELECT substr(rank_date, 1, 4) AS year,
                       COUNT(*) AS row_count
                       {', COUNT(DISTINCT fcode) AS fund_count' if has_fcode else ', 0 AS fund_count'}
                FROM fund_rank_history
                GROUP BY substr(rank_date, 1, 4)
                ORDER BY year
                """,
            )
        ]
    period_distribution = []
    if has_period:
        period_distribution = [
            {
                "period": _normalize_text(row.get("period")) or "unknown",
                "row_count": _normalize_int(row.get("row_count")),
                "fund_count": _normalize_int(row.get("fund_count")),
            }
            for row in _fetch_all_dicts(
                conn,
                f"""
                SELECT COALESCE(period, '') AS period,
                       COUNT(*) AS row_count
                       {', COUNT(DISTINCT fcode) AS fund_count' if has_fcode else ', 0 AS fund_count'}
                FROM fund_rank_history
                GROUP BY COALESCE(period, '')
                ORDER BY row_count DESC
                """,
            )
        ]
    latest_rank_date = _normalize_text(aggregate.get("latest_rank_date"))
    status = "empty"
    if row_count > 0:
        status = "hot_history_accumulation"
    if row_count > 1_000_000:
        status = "large_hot_table"
    return {
        "available": True,
        "status": status,
        "rank_history_row_count": row_count,
        "rank_history_fund_count": fund_count,
        "earliest_rank_date": _normalize_text(aggregate.get("earliest_rank_date")),
        "latest_rank_date": latest_rank_date,
        "avg_rows_per_fund": round(row_count / fund_count, 2) if fund_count else 0.0,
        "columns": sorted(columns),
        "year_distribution": year_distribution,
        "period_distribution": period_distribution,
    }


def _count_rank_rows_before(conn: sqlite3.Connection, cutoff: date) -> tuple[int, int]:
    columns = _table_columns(conn, "fund_rank_history")
    has_fcode = "fcode" in columns
    row = _fetch_one_dict(
        conn,
        f"""
        SELECT COUNT(*) AS row_count
               {', COUNT(DISTINCT fcode) AS fund_count' if has_fcode else ', 0 AS fund_count'}
        FROM fund_rank_history
        WHERE rank_date < ?
        """,
        (cutoff.isoformat(),),
    )
    return _normalize_int(row.get("row_count")), _normalize_int(row.get("fund_count"))


def _build_retention_preview(
    conn: sqlite3.Connection,
    *,
    profile: dict,
    database_hotspots: dict,
    retention_year_options: Iterable[int] = DEFAULT_RETENTION_YEAR_OPTIONS,
) -> dict:
    columns = set(_normalize_list(profile.get("columns")))
    latest_rank_date = _parse_iso_date(profile.get("latest_rank_date"))
    total_rows = _normalize_int(profile.get("rank_history_row_count"))
    table_size_mb = _find_table_size(database_hotspots, "fund_rank_history")
    index_size_mb = round(
        sum(_normalize_float(row.get("size_mb")) for row in _collect_table_indexes(database_hotspots, "fund_rank_history")),
        2,
    )
    hot_zone_size_mb = round(table_size_mb + index_size_mb, 2)
    if "rank_date" not in columns or not latest_rank_date or total_rows <= 0:
        return {
            "available": False,
            "status": "unavailable",
            "active_recommended_years": DEFAULT_RECOMMENDED_RETENTION_YEARS,
            "retention_options": [],
            "latest_rank_date": _normalize_text(profile.get("latest_rank_date")),
            "earliest_rank_date": _normalize_text(profile.get("earliest_rank_date")),
            "reason": "rank_date_missing_or_empty",
        }
    options = []
    for years in sorted({int(item) for item in retention_year_options if int(item) > 0}):
        cutoff = _subtract_years(latest_rank_date, years)
        cold_rows, cold_funds = _count_rank_rows_before(conn, cutoff)
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
        "latest_rank_date": latest_rank_date.isoformat(),
        "earliest_rank_date": _normalize_text(profile.get("earliest_rank_date")),
        "rank_history_row_count": total_rows,
        "rank_history_table_size_mb": table_size_mb,
        "rank_history_index_size_mb": index_size_mb,
        "rank_history_hot_zone_size_mb": hot_zone_size_mb,
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


def _explain_query(conn: sqlite3.Connection, sql: str, params: Iterable[Any]) -> list[str]:
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


def _build_query_plan_audit(conn: sqlite3.Connection, profile: dict) -> dict:
    if not _table_exists(conn, "fund_rank_history"):
        return {"available": False, "status": "table_missing", "query_plan_samples": []}
    columns = set(_normalize_list(profile.get("columns")))
    index_rows = _fetch_all_dicts(conn, "PRAGMA index_list(fund_rank_history)")
    indexes = [
        {
            "name": _normalize_text(row.get("name")),
            "unique": bool(row.get("unique")),
            "origin": _normalize_text(row.get("origin")),
        }
        for row in index_rows
    ]
    sample_fcode = ""
    sample_rank_date = _normalize_text(profile.get("latest_rank_date"))
    if "fcode" in columns:
        sample_fcode = _normalize_text(
            _fetch_one_value(
                conn,
                "SELECT fcode FROM fund_rank_history WHERE fcode IS NOT NULL ORDER BY fcode LIMIT 1",
                default="",
            )
        )
    query_specs = []
    if {"fcode", "rank_date"}.issubset(columns):
        query_specs.append(
            (
                "lookup_latest_by_fcode",
                "SELECT fcode, rank_date FROM fund_rank_history WHERE fcode = ? ORDER BY rank_date DESC LIMIT 5",
                (sample_fcode,),
            )
        )
        query_specs.append(
            (
                "max_rank_date_by_fcode",
                "SELECT fcode, MAX(rank_date) FROM fund_rank_history GROUP BY fcode",
                (),
            )
        )
    if "rank_date" in columns:
        query_specs.append(
            (
                "count_recent_rank_rows",
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date >= ?",
                (sample_rank_date,),
            )
        )
    if {"fcode", "rank_date", "percentile"}.issubset(columns):
        query_specs.append(
            (
                "rank_12m_metrics",
                """
                WITH latest_rank AS (
                    SELECT fcode, MAX(rank_date) AS latest_rank_date
                    FROM fund_rank_history
                    WHERE percentile IS NOT NULL
                    GROUP BY fcode
                )
                SELECT fund_rank_history.fcode, COUNT(*) AS rank_count_12m
                FROM fund_rank_history
                JOIN latest_rank USING (fcode)
                WHERE fund_rank_history.percentile IS NOT NULL
                  AND fund_rank_history.rank_date >= date(latest_rank.latest_rank_date, '-365 day')
                GROUP BY fund_rank_history.fcode
                """,
                (),
            )
        )
    samples = [
        {
            "query_name": query_name,
            "sql": " ".join(sql.split()),
            "plan": _explain_query(conn, sql, params),
        }
        for query_name, sql, params in query_specs
    ]
    index_hit_count = 0
    for sample in samples:
        if "INDEX" in " ".join(sample.get("plan") or []).upper():
            index_hit_count += 1
    return {
        "available": True,
        "status": "ok",
        "sample_fcode": sample_fcode,
        "sample_rank_date": sample_rank_date,
        "indexes": indexes,
        "query_plan_samples": samples,
        "index_used_by_query_count": index_hit_count,
    }


def _build_strategy_preview(retention_preview: dict) -> dict:
    if not _normalize_dict(retention_preview).get("available"):
        return {
            "available": False,
            "status": "unavailable",
            "message": "rank history retention preview is unavailable.",
        }
    option = _pick_retention_option(retention_preview, DEFAULT_RECOMMENDED_RETENTION_YEARS)
    estimated_rows = _normalize_int(option.get("estimated_archive_row_count"))
    cutoff = _normalize_text(option.get("archive_cutoff_date"))
    status = "archive_candidate" if estimated_rows > 0 else "observe"
    return {
        "available": True,
        "status": status,
        "message": (
            f"Conservative {DEFAULT_RECOMMENDED_RETENTION_YEARS}y retention has {estimated_rows} cold rank rows before {cutoff}."
            if estimated_rows > 0
            else f"Conservative {DEFAULT_RECOMMENDED_RETENTION_YEARS}y retention currently has no cold rank rows to archive."
        ),
        "recommended_retention_years": DEFAULT_RECOMMENDED_RETENTION_YEARS,
        "archive_cutoff_date": cutoff,
        "estimated_archive_row_count": estimated_rows,
        "estimated_archive_fund_count": _normalize_int(option.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": _normalize_float(option.get("estimated_archive_size_mb")),
        "estimated_archive_table_size_mb": _normalize_float(option.get("estimated_archive_table_size_mb")),
        "estimated_archive_index_size_mb": _normalize_float(option.get("estimated_archive_index_size_mb")),
        "estimated_archive_share_pct": _normalize_float(option.get("estimated_archive_share_pct")),
        "latest_rank_date": _normalize_text(retention_preview.get("latest_rank_date")),
        "archive_target_path": f"backend/data/archives/fund_rank_history_before_{cutoff}.db" if cutoff else "",
        "archive_table_name": "fund_rank_history_archive",
    }


def _build_summary(
    *,
    storage_metrics: dict,
    database_hotspots: dict,
    profile: dict,
    retention_preview: dict,
    strategy_preview: dict,
    query_plan_audit: dict,
    production_dependencies: list[dict],
) -> dict:
    database_size_mb = _normalize_float(storage_metrics.get("database_size_mb"))
    table_size_mb = _find_table_size(database_hotspots, "fund_rank_history")
    index_rows = _collect_table_indexes(database_hotspots, "fund_rank_history")
    index_size_mb = round(sum(_normalize_float(row.get("size_mb")) for row in index_rows), 2)
    hot_zone_size_mb = round(table_size_mb + index_size_mb, 2)
    hot_zone_share_pct = (
        round((hot_zone_size_mb / database_size_mb) * 100, 1)
        if database_size_mb > 0 and hot_zone_size_mb > 0
        else 0.0
    )
    estimated_size_mb = _normalize_float(strategy_preview.get("estimated_archive_size_mb"))
    estimated_share_of_db_pct = (
        round((estimated_size_mb / database_size_mb) * 100, 1)
        if database_size_mb > 0 and estimated_size_mb > 0
        else 0.0
    )
    dependency_count = len(production_dependencies)
    uncovered_dependencies = [
        item for item in production_dependencies if not bool(item.get("cold_query_ready"))
    ]
    estimated_rows = _normalize_int(strategy_preview.get("estimated_archive_row_count"))
    blocker_count = 0
    status = "observe"
    recommended_direction_key = "rank_history_observe"
    recommended_direction_label = "Observe rank history"
    headline = "fund_rank_history is in observation mode"
    message = "No immediate rank history archive action is ready."
    action_hint = "Keep weekly read-only audit evidence current."
    if estimated_rows > 0 and uncovered_dependencies:
        status = "rank_archive_query_compat_required"
        recommended_direction_key = "rank_history_query_compat_before_archive"
        recommended_direction_label = "Rank cold-query compatibility first"
        headline = "rank history has cold candidates but production queries are not cold-safe yet"
        message = (
            f"fund_rank_history has {estimated_rows} candidate cold rows before "
            f"{strategy_preview.get('archive_cutoff_date') or ''}, but {len(uncovered_dependencies)} "
            "production dependency groups still read the hot table directly."
        )
        action_hint = (
            "Build rank hot/cold union or materialized latest/12m summaries before any archive export or hot delete."
        )
        blocker_count = 1
    elif estimated_rows > 0:
        status = "rank_archive_preparation_priority"
        recommended_direction_key = "rank_history_archive_preparation"
        recommended_direction_label = "Rank archive preparation"
        headline = "rank history is ready for archive runbook preparation"
        message = f"fund_rank_history has {estimated_rows} candidate cold rows."
        action_hint = "Prepare runbook, preflight, digest gate, and rollback evidence before any mutation."
    return {
        "status": status,
        "recommended_direction_key": recommended_direction_key,
        "recommended_direction_label": recommended_direction_label,
        "headline": headline,
        "message": message,
        "action_hint": action_hint,
        "database_path": _normalize_text(storage_metrics.get("database_path")),
        "database_size_mb": database_size_mb,
        "database_quick_check": _normalize_text(storage_metrics.get("quick_check")),
        "rank_history_row_count": _normalize_int(profile.get("rank_history_row_count")),
        "rank_history_fund_count": _normalize_int(profile.get("rank_history_fund_count")),
        "earliest_rank_date": _normalize_text(profile.get("earliest_rank_date")),
        "latest_rank_date": _normalize_text(profile.get("latest_rank_date")),
        "avg_rows_per_fund": _normalize_float(profile.get("avg_rows_per_fund")),
        "rank_history_table_size_mb": table_size_mb,
        "rank_history_index_total_size_mb": index_size_mb,
        "rank_history_hot_zone_size_mb": hot_zone_size_mb,
        "rank_history_hot_zone_share_of_db_pct": hot_zone_share_pct,
        "recommended_retention_years": _normalize_int(
            strategy_preview.get("recommended_retention_years"),
            DEFAULT_RECOMMENDED_RETENTION_YEARS,
        ),
        "archive_cutoff_date": _normalize_text(strategy_preview.get("archive_cutoff_date")),
        "estimated_archive_row_count": estimated_rows,
        "estimated_archive_fund_count": _normalize_int(strategy_preview.get("estimated_archive_fund_count")),
        "estimated_archive_size_mb": estimated_size_mb,
        "estimated_archive_share_pct": _normalize_float(strategy_preview.get("estimated_archive_share_pct")),
        "estimated_archive_share_of_db_pct": estimated_share_of_db_pct,
        "archive_target_path": _normalize_text(strategy_preview.get("archive_target_path")),
        "archive_table_name": _normalize_text(strategy_preview.get("archive_table_name")),
        "production_dependency_count": dependency_count,
        "production_uncovered_dependency_count": len(uncovered_dependencies),
        "query_plan_index_used_by_query_count": _normalize_int(
            query_plan_audit.get("index_used_by_query_count")
        ),
        "blocker_count": blocker_count,
        "read_only": True,
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
    }


def _build_findings(summary: dict, profile: dict, strategy_preview: dict) -> list[str]:
    findings = []
    row_count = _normalize_int(summary.get("rank_history_row_count"))
    fund_count = _normalize_int(summary.get("rank_history_fund_count"))
    if row_count > 0:
        findings.append(
            f"fund_rank_history currently has {row_count} rows across {fund_count} funds, "
            f"from {summary.get('earliest_rank_date') or '-'} to {summary.get('latest_rank_date') or '-'}."
        )
    hot_zone_size = _normalize_float(summary.get("rank_history_hot_zone_size_mb"))
    if hot_zone_size > 0:
        findings.append(
            f"fund_rank_history table and visible indexes account for about {hot_zone_size} MB "
            f"({summary.get('rank_history_hot_zone_share_of_db_pct') or 0}% of the database)."
        )
    estimated_rows = _normalize_int(summary.get("estimated_archive_row_count"))
    if estimated_rows > 0:
        findings.append(
            f"Keeping the latest {summary.get('recommended_retention_years') or DEFAULT_RECOMMENDED_RETENTION_YEARS} years hot "
            f"would make {estimated_rows} rows before {summary.get('archive_cutoff_date') or '-'} cold candidates."
        )
    if _normalize_int(summary.get("production_uncovered_dependency_count")) > 0:
        findings.append(
            "Rank archive mutation is blocked until production readers stop depending on hot-only fund_rank_history queries."
        )
    message = _normalize_text(strategy_preview.get("message"))
    if message:
        findings.append(message)
    return findings


def _build_blockers(summary: dict) -> list[dict]:
    blockers = []
    if _normalize_int(summary.get("blocker_count")) > 0:
        blockers.append(
            {
                "blocker_id": "rank_history_hot_only_production_dependencies",
                "severity": "high",
                "message": (
                    "fund_rank_history has cold archive candidates, but production long-term/report "
                    "queries have not been migrated to a cold-safe union or summary layer."
                ),
                "required_action": (
                    "Implement rank history cold-query compatibility and digest gates before archive export or hot delete."
                ),
            }
        )
    return blockers


def _build_recommendations(summary: dict) -> list[str]:
    if _normalize_text(summary.get("recommended_direction_key")) == "rank_history_query_compat_before_archive":
        return [
            "Add a rank history query helper equivalent to the NAV hot/cold union helper.",
            "Migrate long-term rank readiness, latest rank date, and report 12m percentile metrics to that helper or to materialized summaries.",
            "Only after query compatibility, add runbook/preflight/digest/delete-gate runners for fund_rank_history.",
        ]
    if _normalize_text(summary.get("recommended_direction_key")) == "rank_history_archive_preparation":
        return [
            "Prepare a rank archive runbook and preflight using the same backup/digest discipline as NAV.",
            "Keep page-limited export/delete and never combine hot delete with VACUUM in weekly maintenance.",
        ]
    return [
        "Keep weekly read-only rank history storage evidence current.",
        "Revisit archive readiness when rank history hot-zone size or cold candidates continue to grow.",
    ]


def build_rank_history_governance_audit_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    storage_metrics: Optional[dict] = None,
    database_hotspots: Optional[dict] = None,
    rank_history_profile: Optional[dict] = None,
    retention_preview: Optional[dict] = None,
    strategy_preview: Optional[dict] = None,
    query_plan_audit: Optional[dict] = None,
    production_dependencies: Optional[list[dict]] = None,
) -> dict:
    resolved_db_path = _resolve_db_path(db_path)
    with _readonly_connection(resolved_db_path) as conn:
        metrics = _normalize_dict(storage_metrics) or _build_sqlite_storage_metrics(resolved_db_path)
        hotspots = _normalize_dict(database_hotspots) or _build_database_hotspots(
            conn,
            database_path=resolved_db_path,
        )
        profile = _normalize_dict(rank_history_profile) or _build_rank_history_profile(conn)
        retention = _normalize_dict(retention_preview) or _build_retention_preview(
            conn,
            profile=profile,
            database_hotspots=hotspots,
        )
        strategy = _normalize_dict(strategy_preview) or _build_strategy_preview(retention)
        plan_audit = _normalize_dict(query_plan_audit) or _build_query_plan_audit(conn, profile)
    dependencies = (
        production_dependencies
        if isinstance(production_dependencies, list)
        else build_rank_history_production_dependencies(project_root=project_root)
    )
    summary = _build_summary(
        storage_metrics=metrics,
        database_hotspots=hotspots,
        profile=profile,
        retention_preview=retention,
        strategy_preview=strategy,
        query_plan_audit=plan_audit,
        production_dependencies=dependencies,
    )
    findings = _build_findings(summary, profile, strategy)
    blockers = _build_blockers(summary)
    recommendations = _build_recommendations(summary)
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database_path": resolved_db_path,
        "summary": summary,
        "findings": findings,
        "blockers": blockers,
        "recommendations": recommendations,
        "storage_metrics": metrics,
        "database_hotspots": hotspots,
        "rank_history_profile": profile,
        "rank_history_retention_preview": retention,
        "rank_history_archive_strategy_preview": strategy,
        "rank_history_query_plan_audit": plan_audit,
        "production_dependencies": dependencies,
        "safety": {
            "read_only": True,
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }


def render_rank_history_governance_audit_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# fund_rank_history governance audit",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Database: {payload.get('database_path', '')}",
        f"- Status: {summary.get('status', '')}",
        f"- Recommended direction: {summary.get('recommended_direction_label', '')}",
        f"- Blockers: {summary.get('blocker_count', 0)}",
        "",
        "## Summary",
        "",
        f"- {summary.get('message', '')}",
        f"- {summary.get('action_hint', '')}",
        "",
        "## Findings",
        "",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    blockers = payload.get("blockers") or []
    if blockers:
        lines.extend(["", "## Blockers", ""])
        for item in blockers:
            lines.append(f"- {item.get('blocker_id', '')}: {item.get('message', '')}")
    lines.extend(["", "## Recommendations", ""])
    for item in payload.get("recommendations") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_rank_history_governance_audit_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"rank_history_governance_audit_{ts}.json"
    md_path = artifact_dir / f"rank_history_governance_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_rank_history_governance_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    audit_payload = build_rank_history_governance_audit_payload()
    audit_json_path, audit_md_path = write_rank_history_governance_audit_artifacts(audit_payload)
    print(
        json.dumps(
            {
                "artifact_path": str(audit_json_path),
                "markdown_path": str(audit_md_path),
                "summary": audit_payload.get("summary") or {},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
