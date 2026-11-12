#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Page-limited fund_rank_history hot/cold archive runner."""

from __future__ import annotations

import json
import os
import tempfile
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple
from urllib.parse import quote

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.rank_history_archive_preflight import (
    ARCHIVE_COLUMNS,
    ARCHIVE_TABLE_NAME,
    DEFAULT_OUTPUT_DIR,
    REQUIRED_HOT_COLUMNS,
    RANK_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN,
    load_latest_rank_history_archive_preflight_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN = "rank_history_archive_apply_runner_*.json"
DEFAULT_ARCHIVE_PAGE_LIMIT = 500_000
ALLOWED_MODES = {"dry_run", "archive_export", "hot_delete", "auto"}
HOT_DB_ALIAS = "rank_hot_db"
ARCHIVE_DB_ALIAS = "rank_archive_db"


def load_latest_rank_history_archive_apply_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def _round_mb(num_bytes: int | float) -> float:
    return round(float(num_bytes or 0) / 1024 / 1024, 2)


def _quote_sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _normalize_table_name(value: Any) -> str:
    text = _normalize_text(value) or ARCHIVE_TABLE_NAME
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text):
        return ARCHIVE_TABLE_NAME
    return text


def _resolve_project_root(project_root: str | Path | None) -> Path:
    return Path(project_root).resolve() if project_root else PROJECT_ROOT


def _resolve_output_dir(output_dir: str | Path, project_root: Path) -> Path:
    path = Path(output_dir)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _resolve_path(value: Any, project_root: Path) -> Optional[Path]:
    text = _normalize_text(value)
    if not text:
        return None
    path = Path(text)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _resolve_first_existing_path(values: Iterable[Any], project_root: Path) -> Optional[Path]:
    fallback: Optional[Path] = None
    for value in values:
        path = _resolve_path(value, project_root)
        if path is None:
            continue
        if fallback is None:
            fallback = path
        if path.exists():
            return path
    return fallback


def _stringify_project_path(path: Optional[Path], project_root: Path) -> str:
    if path is None:
        return ""
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _fetch_all_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> list[dict]:
    cursor = conn.execute(sql, tuple(params))
    columns = [item[0] for item in cursor.description or []]
    rows = []
    for row in cursor.fetchall():
        if isinstance(row, sqlite3.Row):
            rows.append(dict(row))
        else:
            rows.append(dict(zip(columns, row)))
    return rows


def _fetch_one_dict(
    conn: sqlite3.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> dict:
    rows = _fetch_all_dicts(conn, sql, params)
    return rows[0] if rows else {}


def _table_exists(conn: sqlite3.Connection, table_name: str, *, schema: str = "main") -> bool:
    return (
        conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _table_columns(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    schema: str = "main",
) -> set[str]:
    if not _table_exists(conn, table_name, schema=schema):
        return set()
    rows = _fetch_all_dicts(conn, f"PRAGMA {schema}.table_info({table_name})")
    return {_normalize_text(row.get("name")) for row in rows if _normalize_text(row.get("name"))}


def _sqlite_quick_check(path: Optional[Path]) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    try:
        with sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return _normalize_text(row[0] if row else "")
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _load_json_path(path_text: str | Path, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return path, payload


def _load_latest_preflight_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Path, dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    path, payload = load_latest_rank_history_archive_preflight_payload(
        output_dir=str(resolved_output_dir),
    )
    return path, payload


def _build_select_columns(columns: set[str]) -> list[str]:
    return [column for column in ARCHIVE_COLUMNS if column in columns]


def _qualified_column_list(alias: str, columns: list[str]) -> str:
    return ", ".join(f"{alias}.{column}" for column in columns)


def _null_safe_equal(left: str, right: str) -> str:
    return f"(({left} = {right}) OR ({left} IS NULL AND {right} IS NULL))"


def _rank_exact_match_sql(
    hot_alias: str,
    archive_alias: str,
    columns: list[str],
) -> str:
    match_columns = [column for column in columns if column in set(REQUIRED_HOT_COLUMNS)]
    predicates = [
        f"{archive_alias}.fcode = {hot_alias}.fcode",
        f"{archive_alias}.rank_date = {hot_alias}.rank_date",
        f"COALESCE({archive_alias}.period, '') = COALESCE({hot_alias}.period, '')",
    ]
    for column in match_columns:
        if column in {"fcode", "rank_date", "period"}:
            continue
        predicates.append(_null_safe_equal(f"{archive_alias}.{column}", f"{hot_alias}.{column}"))
    return " AND ".join(predicates)


def _rank_key_match_sql(hot_alias: str, archive_alias: str) -> str:
    return (
        f"{archive_alias}.fcode = {hot_alias}.fcode "
        f"AND {archive_alias}.rank_date = {hot_alias}.rank_date "
        f"AND COALESCE({archive_alias}.period, '') = COALESCE({hot_alias}.period, '')"
    )


def _rank_delete_exact_match_sql(
    hot_alias: str,
    archive_alias: str,
    columns: list[str],
) -> str:
    match_columns = [column for column in columns if column in set(REQUIRED_HOT_COLUMNS)]
    predicates = [
        f"{archive_alias}.fcode = {hot_alias}.fcode",
        f"{archive_alias}.rank_date = {hot_alias}.rank_date",
        f"{archive_alias}.period IS {hot_alias}.period",
    ]
    for column in match_columns:
        if column in {"fcode", "rank_date", "period"}:
            continue
        predicates.append(_null_safe_equal(f"{archive_alias}.{column}", f"{hot_alias}.{column}"))
    return " AND ".join(predicates)


def _archive_high_water_key(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    cutoff_date: str,
) -> dict:
    if not _table_exists(conn, table_name):
        return {}
    row = conn.execute(
        f"""
        SELECT
            rank_date,
            fcode,
            COALESCE(period, '') AS period_key
        FROM {table_name}
        WHERE rank_date < ?
        ORDER BY rank_date DESC, fcode DESC, COALESCE(period, '') DESC
        LIMIT 1
        """,
        (cutoff_date,),
    ).fetchone()
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return {
        "rank_date": row[0],
        "fcode": row[1],
        "period_key": row[2],
    }


def _ensure_archive_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            fcode TEXT NOT NULL,
            rank_date TEXT NOT NULL,
            period TEXT,
            rank_position INTEGER,
            category_total INTEGER,
            percentile REAL,
            created_at TEXT
        )
        """
    )
    existing_columns = _table_columns(conn, table_name)
    if "created_at" not in existing_columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN created_at TEXT")
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_key
        ON {table_name} (fcode, rank_date, COALESCE(period, ''))
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_fcode_date
        ON {table_name} (fcode, rank_date)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_rank_date
        ON {table_name} (rank_date)
        """
    )


def _archive_table_schema_valid(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    schema: str = "main",
) -> bool:
    columns = _table_columns(conn, table_name, schema=schema)
    return set(REQUIRED_HOT_COLUMNS).issubset(columns)


def _inspect_primary_db(
    *,
    db_path: Path,
    cutoff_date: str,
) -> dict:
    if not db_path.exists() or not db_path.is_file():
        return {"available": False, "error": f"primary database missing: {db_path}"}
    conn = sqlite3.connect(_sqlite_readonly_uri(db_path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        columns = _table_columns(conn, "fund_rank_history")
        if not set(REQUIRED_HOT_COLUMNS).issubset(columns):
            return {
                "available": False,
                "error": "fund_rank_history required columns missing",
                "columns": sorted(columns),
            }
        total = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS total_rank_row_count,
                COUNT(DISTINCT fcode) AS total_rank_fund_count,
                MIN(rank_date) AS min_rank_date,
                MAX(rank_date) AS max_rank_date
            FROM fund_rank_history
            """,
        )
        cold = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS live_cold_row_count,
                COUNT(DISTINCT fcode) AS live_cold_fund_count,
                MIN(rank_date) AS live_cold_min_rank_date,
                MAX(rank_date) AS live_cold_max_rank_date
            FROM fund_rank_history
            WHERE rank_date < ?
            """,
            (cutoff_date,),
        )
        duplicate_keys = _normalize_int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT fcode, rank_date, COALESCE(period, '') AS period_key, COUNT(*) AS c
                    FROM fund_rank_history
                    WHERE rank_date < ?
                    GROUP BY fcode, rank_date, COALESCE(period, '')
                    HAVING c > 1
                )
                """,
                (cutoff_date,),
            ).fetchone()[0]
        )
        page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "available": True,
            "columns": sorted(columns),
            "selected_archive_columns": _build_select_columns(columns),
            "duplicate_cold_business_key_count": duplicate_keys,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "database_size_mb": _round_mb(db_path.stat().st_size),
            **total,
            **cold,
        }
    finally:
        conn.close()


def _inspect_archive_db(
    *,
    archive_target_path: Optional[Path],
    archive_table_name: str,
    cutoff_date: str,
) -> dict:
    if archive_target_path is None:
        return {"archive_target_exists": False, "archive_table_exists": False}
    if not archive_target_path.exists() or not archive_target_path.is_file():
        return {
            "archive_target_exists": False,
            "archive_table_exists": False,
            "archive_target_size_mb": 0.0,
            "archive_quick_check": "",
        }
    payload = {
        "archive_target_exists": True,
        "archive_target_size_mb": _round_mb(archive_target_path.stat().st_size),
        "archive_table_exists": False,
        "archive_quick_check": _sqlite_quick_check(archive_target_path),
    }
    conn = sqlite3.connect(_sqlite_readonly_uri(archive_target_path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = _table_exists(conn, archive_table_name)
        payload["archive_table_exists"] = table_exists
        if not table_exists:
            return payload
        columns = _table_columns(conn, archive_table_name)
        schema_valid = set(REQUIRED_HOT_COLUMNS).issubset(columns)
        payload["archive_columns"] = sorted(columns)
        payload["archive_table_schema_valid"] = schema_valid
        if not schema_valid:
            return payload
        counts = _fetch_one_dict(
            conn,
            f"""
            SELECT
                COUNT(*) AS existing_archive_row_count,
                COUNT(DISTINCT fcode) AS existing_archive_fund_count,
                MIN(rank_date) AS existing_archive_min_rank_date,
                MAX(rank_date) AS existing_archive_max_rank_date
            FROM {archive_table_name}
            WHERE rank_date < ?
            """,
            (cutoff_date,),
        )
        out_of_scope = _fetch_one_dict(
            conn,
            f"SELECT COUNT(*) AS archive_out_of_scope_row_count FROM {archive_table_name} WHERE rank_date >= ?",
            (cutoff_date,),
        )
        total = _fetch_one_dict(
            conn,
            f"SELECT COUNT(*) AS existing_archive_total_row_count FROM {archive_table_name}",
        )
        payload.update(counts)
        payload.update(out_of_scope)
        payload.update(total)
        return payload
    except sqlite3.Error as exc:
        payload["archive_inspection_error"] = str(exc)
        return payload
    finally:
        conn.close()


def _inspect_match_state(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    cutoff_date: str,
    columns: list[str],
) -> dict:
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _rank_exact_match_sql("hot", "archived", columns)
    key_match = _rank_key_match_sql("hot", "archived")
    conn = sqlite3.connect(_sqlite_readonly_uri(db_path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        match_state = _fetch_one_dict(
            conn,
            f"""
            SELECT
                COUNT(*) AS inspected_hot_cold_row_count,
                SUM(CASE WHEN archived.fcode IS NULL THEN 1 ELSE 0 END)
                    AS hot_unarchived_row_count,
                SUM(CASE WHEN archived.fcode IS NOT NULL AND {exact_match} THEN 1 ELSE 0 END)
                    AS exact_matched_hot_cold_row_count,
                SUM(CASE WHEN archived.fcode IS NOT NULL AND NOT ({exact_match}) THEN 1 ELSE 0 END)
                    AS key_conflict_row_count
            FROM fund_rank_history AS hot
            LEFT JOIN {ARCHIVE_DB_ALIAS}.{archive_table_name} AS archived
                ON {key_match}
            WHERE hot.rank_date < ?
            """,
            (cutoff_date,),
        )
        return {
            "match_inspection_status": "executed",
            "exact_match_full_scan_executed": True,
            "inspected_hot_cold_row_count": _normalize_int(
                match_state.get("inspected_hot_cold_row_count")
            ),
            "exact_matched_hot_cold_row_count": _normalize_int(
                match_state.get("exact_matched_hot_cold_row_count")
            ),
            "hot_unarchived_row_count": _normalize_int(
                match_state.get("hot_unarchived_row_count")
            ),
            "key_conflict_row_count": _normalize_int(
                match_state.get("key_conflict_row_count")
            ),
        }
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _skipped_match_state(*, reason: str, archive_missing_row_count: int) -> dict:
    return {
        "match_inspection_status": "skipped",
        "skip_reason": reason,
        "exact_match_full_scan_executed": False,
        "exact_matched_hot_cold_row_count": 0,
        "hot_unarchived_row_count": max(0, int(archive_missing_row_count or 0)),
        "key_conflict_row_count": 0,
    }


def _execute_archive_export_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    cutoff_date: str,
    columns: list[str],
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        }
    archive_target_path.parent.mkdir(parents=True, exist_ok=True)
    hot_uri = _sqlite_readonly_uri(db_path)
    column_sql = ", ".join(columns)
    hot_column_sql = _qualified_column_list("hot", columns)
    exact_key = (
        "archived.fcode = hot.fcode "
        "AND archived.rank_date = hot.rank_date "
        "AND COALESCE(archived.period, '') = COALESCE(hot.period, '')"
    )
    conn = sqlite3.connect(str(archive_target_path), uri=True, isolation_level=None, timeout=120)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {HOT_DB_ALIAS}", (hot_uri,))
        attached = True
        conn.execute("BEGIN IMMEDIATE")
        _ensure_archive_table(conn, archive_table_name)
        if not _archive_table_schema_valid(conn, archive_table_name):
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": "archive_table_schema_invalid",
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
            }
        before = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {archive_table_name} WHERE rank_date < ?",
                (cutoff_date,),
            ).fetchone()[0]
        )
        high_water = _archive_high_water_key(
            conn,
            archive_table_name,
            cutoff_date=cutoff_date,
        )
        high_water_predicate = ""
        high_water_params: tuple[Any, ...] = ()
        if high_water:
            high_rank_date = _normalize_text(high_water.get("rank_date"))
            high_fcode = _normalize_text(high_water.get("fcode"))
            high_period_key = _normalize_text(high_water.get("period_key"))
            high_water_predicate = """
              AND (
                  hot.rank_date > ?
                  OR (hot.rank_date = ? AND hot.fcode > ?)
                  OR (
                      hot.rank_date = ?
                      AND hot.fcode = ?
                      AND COALESCE(hot.period, '') > ?
                  )
              )
            """
            high_water_params = (
                high_rank_date,
                high_rank_date,
                high_fcode,
                high_rank_date,
                high_fcode,
                high_period_key,
            )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {archive_table_name} ({column_sql})
            SELECT {hot_column_sql}
            FROM {HOT_DB_ALIAS}.fund_rank_history AS hot
            WHERE hot.rank_date < ?
              {high_water_predicate}
              AND NOT EXISTS (
                  SELECT 1
                  FROM {archive_table_name} AS archived
                  WHERE {exact_key}
              )
            ORDER BY hot.rank_date, hot.fcode, COALESCE(hot.period, '')
            LIMIT ?
            """,
            (cutoff_date, *high_water_params, int(page_limit)),
        )
        after = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {archive_table_name} WHERE rank_date < ?",
                (cutoff_date,),
            ).fetchone()[0]
        )
        conn.execute("COMMIT")
        return {
            "status": "executed",
            "archive_sql_executed": True,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "existing_before": before,
            "existing_after": after,
            "inserted_row_count": max(after - before, 0),
            "high_water_before": high_water,
            "archive_target_size_mb": _round_mb(archive_target_path.stat().st_size),
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {
            "status": "failed",
            "error": str(exc),
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_hot_delete_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    cutoff_date: str,
    columns: list[str],
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        }
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _rank_delete_exact_match_sql("hot", "archived", columns)
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date < ?",
                (cutoff_date,),
            ).fetchone()[0]
        )
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            f"""
            DELETE FROM fund_rank_history
            WHERE rowid IN (
                SELECT hot.rowid
                FROM {ARCHIVE_DB_ALIAS}.{archive_table_name} AS archived
                JOIN fund_rank_history AS hot
                  ON {exact_match}
                WHERE archived.rank_date < ?
                ORDER BY archived.rank_date, archived.fcode, archived.period
                LIMIT ?
            )
            """,
            (cutoff_date, int(page_limit)),
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        conn.execute("COMMIT")
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date < ?",
                (cutoff_date,),
            ).fetchone()[0]
        )
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "status": "executed",
            "hot_delete_sql_executed": True,
            "delete_sql_executed": True,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "live_cold_rows_before": before,
            "live_cold_rows_after": after,
            "deleted_row_count": deleted_count,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {
            "status": "failed",
            "error": str(exc),
            "hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _build_gate(key: str, label: str, status: str, detail: str, action_hint: str = "") -> dict:
    return {
        "key": key,
        "label": label,
        "status": status,
        "detail": detail,
        "action_hint": action_hint,
    }


def _build_findings(summary: dict, blockers: list[str]) -> list[str]:
    if blockers:
        return [
            f"rank 历史热冷治理存在 {len(blockers)} 个阻断项，本次不会执行危险写入。",
            *blockers[:4],
        ]
    if summary.get("archive_binding_active"):
        return [
            "rank 历史冷库已物化且热库 cutoff 前数据已清零，运行期 cold binding 可生效。"
        ]
    if _normalize_int(summary.get("archive_missing_row_count")) > 0:
        return [
            "rank 历史归档仍在分页物化中；本次最多只写入一个 archive page，不删除热库数据。",
            (
                f"已归档 {summary.get('existing_archive_row_count', 0)} 行，"
                f"剩余 {summary.get('archive_missing_row_count', 0)} 行。"
            ),
        ]
    if _normalize_int(summary.get("live_cold_row_count")) > 0:
        return [
            "rank 历史冷库已完整物化，可进入 hot_delete 页级删除；删除仅针对冷库中精确匹配的热库行。"
        ]
    return ["rank 历史热冷治理 dry-run 已完成；本次未执行归档、删除或 VACUUM。"]


def build_rank_history_archive_apply_runner_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    preflight_payload: Optional[dict] = None,
    source_preflight_artifact: Optional[str] = None,
    backup_artifact_path: str | Path | None = None,
    require_backup_for_hot_delete: bool = True,
    mode: str = "dry_run",
    page_limit: int = DEFAULT_ARCHIVE_PAGE_LIMIT,
) -> dict:
    root = _resolve_project_root(project_root)
    requested_mode = _normalize_text(mode) or "dry_run"
    effective_page_limit = max(0, int(page_limit or 0))

    if preflight_payload is None:
        if source_preflight_artifact:
            preflight_artifact_path, preflight_payload = _load_json_path(
                source_preflight_artifact,
                root,
            )
        else:
            preflight_artifact_path, preflight_payload = _load_latest_preflight_payload(
                output_dir=output_dir,
                project_root=root,
            )
    else:
        preflight_artifact_path = (
            _resolve_path(source_preflight_artifact, root)
            if source_preflight_artifact
            else None
        )
    preflight_payload = _normalize_dict(preflight_payload)
    preflight_summary = _normalize_dict(preflight_payload.get("summary"))
    preflight_checks = _normalize_dict(preflight_payload.get("checks"))

    active_db_path = _resolve_path(
        db_path or preflight_summary.get("database_path") or "backend/data/bond_funds.db",
        root,
    )
    if active_db_path is None:
        active_db_path = Path(resolve_db_path("database/bond_funds.db")).resolve()
    archive_target_path = _resolve_first_existing_path(
        (
            preflight_summary.get("archive_target_path"),
            preflight_summary.get("archive_target_absolute_path"),
        ),
        root,
    )
    archive_table_name = _normalize_table_name(
        preflight_summary.get("archive_table_name") or ARCHIVE_TABLE_NAME
    )
    backup_path = _resolve_path(backup_artifact_path, root)
    backup_artifact_exists = bool(backup_path and backup_path.exists() and backup_path.is_file())
    cutoff_date = _normalize_text(preflight_summary.get("archive_cutoff_date"))
    expected_archive_row_count = max(
        _normalize_int(preflight_summary.get("effective_expected_archive_row_count")),
        _normalize_int(preflight_summary.get("candidate_row_count")),
        _normalize_int(preflight_summary.get("governance_estimated_archive_row_count")),
    )
    expected_archive_fund_count = max(
        _normalize_int(preflight_summary.get("effective_expected_archive_fund_count")),
        _normalize_int(preflight_summary.get("candidate_fund_count")),
        _normalize_int(preflight_summary.get("governance_estimated_archive_fund_count")),
    )
    active_db_exists = bool(active_db_path.exists() and active_db_path.is_file())
    active_db_quick_check = _sqlite_quick_check(active_db_path) if active_db_exists else ""

    primary_snapshot: dict[str, Any] = {"available": False}
    archive_snapshot: dict[str, Any] = {}
    match_snapshot: dict[str, Any] = {}
    selected_columns: list[str] = []
    if active_db_exists and cutoff_date:
        primary_snapshot = _inspect_primary_db(
            db_path=active_db_path,
            cutoff_date=cutoff_date,
        )
        selected_columns = list(primary_snapshot.get("selected_archive_columns") or [])
    if archive_target_path and cutoff_date:
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
        )
    archive_exists = bool(archive_snapshot.get("archive_target_exists"))
    archive_table_exists = bool(archive_snapshot.get("archive_table_exists"))
    archive_table_schema_valid = bool(
        archive_snapshot.get("archive_table_schema_valid")
        if archive_table_exists
        else True
    )
    archive_quick_check = _normalize_text(archive_snapshot.get("archive_quick_check"))
    live_cold_row_count = _normalize_int(primary_snapshot.get("live_cold_row_count"))
    live_cold_fund_count = _normalize_int(primary_snapshot.get("live_cold_fund_count"))
    existing_archive_row_count = _normalize_int(archive_snapshot.get("existing_archive_row_count"))
    existing_archive_fund_count = _normalize_int(archive_snapshot.get("existing_archive_fund_count"))
    archive_out_of_scope_row_count = _normalize_int(
        archive_snapshot.get("archive_out_of_scope_row_count")
    )
    archive_missing_row_count = max(expected_archive_row_count - existing_archive_row_count, 0)
    archive_materialized_and_reconciled = bool(
        expected_archive_row_count > 0
        and existing_archive_row_count == expected_archive_row_count
        and archive_out_of_scope_row_count == 0
        and (
            expected_archive_fund_count <= 0
            or existing_archive_fund_count == expected_archive_fund_count
        )
    )
    archive_binding_active = bool(
        archive_materialized_and_reconciled and live_cold_row_count == 0
    )
    auto_effective_mode = requested_mode
    if requested_mode == "auto":
        auto_effective_mode = (
            "archive_export" if archive_missing_row_count > 0 else "hot_delete"
        )
    can_inspect_match = bool(
        active_db_exists
        and archive_target_path
        and archive_exists
        and archive_table_exists
        and archive_table_schema_valid
        and cutoff_date
        and selected_columns
    )
    needs_exact_match_scan = bool(
        can_inspect_match
        and archive_materialized_and_reconciled
        and live_cold_row_count <= 0
    )
    if needs_exact_match_scan:
        match_snapshot = _inspect_match_state(
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
            columns=selected_columns,
        )
    elif can_inspect_match:
        match_snapshot = _skipped_match_state(
            reason=(
                "archive_not_fully_materialized"
                if not archive_materialized_and_reconciled
                else "hot_delete_batch_match_scan_deferred"
            ),
            archive_missing_row_count=archive_missing_row_count,
        )
    else:
        match_snapshot = _skipped_match_state(
            reason="archive_match_context_unavailable",
            archive_missing_row_count=archive_missing_row_count,
        )
    hot_unarchived_row_count = _normalize_int(match_snapshot.get("hot_unarchived_row_count"))
    key_conflict_row_count = _normalize_int(match_snapshot.get("key_conflict_row_count"))
    exact_matched_hot_cold_row_count = _normalize_int(
        match_snapshot.get("exact_matched_hot_cold_row_count")
    )

    gates = [
        _build_gate(
            "mode",
            "执行模式",
            "ok" if requested_mode in ALLOWED_MODES else "blocked",
            f"requested_mode={requested_mode}; allowed_modes={sorted(ALLOWED_MODES)}",
        ),
        _build_gate(
            "preflight_ready",
            "preflight 已通过",
            "ok"
            if preflight_summary.get("ready_for_archive_runbook")
            and _normalize_int(preflight_summary.get("blocker_count")) == 0
            else "blocked",
            f"source_preflight_artifact={preflight_artifact_path or ''}",
        ),
        _build_gate(
            "database",
            "活动主库",
            "ok" if active_db_exists and primary_snapshot.get("available") else "blocked",
            f"active_db_path={active_db_path}; error={primary_snapshot.get('error') or ''}",
        ),
        _build_gate(
            "cutoff",
            "归档 cutoff",
            "ok" if cutoff_date else "blocked",
            f"archive_cutoff_date={cutoff_date}",
        ),
        _build_gate(
            "columns",
            "rank 必需列",
            "ok" if set(REQUIRED_HOT_COLUMNS).issubset(set(selected_columns)) else "blocked",
            f"selected_columns={selected_columns}",
        ),
        _build_gate(
            "primary_quick_check",
            "主库 quick_check",
            "ok" if active_db_quick_check.lower() == "ok" else "blocked",
            f"quick_check={active_db_quick_check}",
        ),
        _build_gate(
            "archive_target",
            "冷库目标",
            "ok" if archive_target_path is not None else "blocked",
            f"archive_target_path={archive_target_path or ''}",
        ),
        _build_gate(
            "archive_quick_check",
            "冷库 quick_check",
            "ok" if (not archive_exists or archive_quick_check.lower() == "ok") else "blocked",
            f"archive_exists={archive_exists}; quick_check={archive_quick_check}",
        ),
        _build_gate(
            "archive_schema",
            "冷库 schema",
            "ok" if archive_table_schema_valid else "blocked",
            f"schema_valid={archive_table_schema_valid}",
        ),
        _build_gate(
            "archive_counts",
            "冷库计数",
            "ok" if existing_archive_row_count <= expected_archive_row_count else "blocked",
            (
                f"existing_archive_row_count={existing_archive_row_count}; "
                f"expected_archive_row_count={expected_archive_row_count}"
            ),
        ),
        _build_gate(
            "archive_out_of_scope",
            "冷库越界",
            "ok" if archive_out_of_scope_row_count == 0 else "blocked",
            f"archive_out_of_scope_row_count={archive_out_of_scope_row_count}",
        ),
        _build_gate(
            "hot_duplicate_keys",
            "热库业务键重复",
            "ok"
            if _normalize_int(primary_snapshot.get("duplicate_cold_business_key_count")) == 0
            else "blocked",
            (
                "duplicate_cold_business_key_count="
                f"{_normalize_int(primary_snapshot.get('duplicate_cold_business_key_count'))}"
            ),
        ),
        _build_gate(
            "page_limit",
            "分页上限",
            "ok" if requested_mode == "dry_run" or effective_page_limit > 0 else "blocked",
            f"page_limit={effective_page_limit}",
        ),
    ]
    if auto_effective_mode == "hot_delete":
        gates.extend(
            [
                _build_gate(
                    "archive_materialized",
                    "冷库完整物化",
                    "ok" if archive_materialized_and_reconciled else "blocked",
                    (
                        f"existing_archive_row_count={existing_archive_row_count}; "
                        f"expected_archive_row_count={expected_archive_row_count}; "
                        f"archive_out_of_scope_row_count={archive_out_of_scope_row_count}"
                    ),
                ),
                _build_gate(
                    "hot_rows_archived",
                    "热库待删行已精确归档",
                    "ok"
                    if (
                        not bool(match_snapshot.get("exact_match_full_scan_executed"))
                        or hot_unarchived_row_count == 0
                    )
                    else "blocked",
                    (
                        f"exact_match_full_scan_executed="
                        f"{bool(match_snapshot.get('exact_match_full_scan_executed'))}; "
                        f"hot_unarchived_row_count={hot_unarchived_row_count}; "
                        "delete_sql_enforces_exact_match=true"
                    ),
                ),
                _build_gate(
                    "rank_key_conflicts",
                    "冷热业务键冲突",
                    "ok"
                    if (
                        not bool(match_snapshot.get("exact_match_full_scan_executed"))
                        or key_conflict_row_count == 0
                    )
                    else "blocked",
                    (
                        f"exact_match_full_scan_executed="
                        f"{bool(match_snapshot.get('exact_match_full_scan_executed'))}; "
                        f"key_conflict_row_count={key_conflict_row_count}"
                    ),
                ),
                _build_gate(
                    "hot_delete_backup",
                    "热库删除备份",
                    "ok"
                    if (
                        not require_backup_for_hot_delete
                        or live_cold_row_count <= 0
                        or backup_artifact_exists
                    )
                    else "blocked",
                    (
                        f"require_backup_for_hot_delete={require_backup_for_hot_delete}; "
                        f"backup_artifact_path={backup_path or ''}; "
                        f"backup_artifact_exists={backup_artifact_exists}"
                    ),
                    "hot_delete 前必须有可追溯的 rank 归档专用备份。",
                ),
            ]
        )
    blockers = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "blocked"
    ]

    archive_export_result = {
        "status": "not_requested"
        if auto_effective_mode not in {"archive_export"}
        else "not_executed",
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "inserted_row_count": 0,
    }
    hot_delete_result = {
        "status": "not_requested" if auto_effective_mode != "hot_delete" else "not_executed",
        "hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "deleted_row_count": 0,
    }

    if (
        auto_effective_mode == "archive_export"
        and not blockers
        and archive_target_path is not None
        and archive_missing_row_count > 0
    ):
        archive_export_result = _execute_archive_export_page(
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
            columns=selected_columns,
            page_limit=effective_page_limit,
        )
        if archive_export_result.get("status") == "failed":
            blockers.append(
                "archive export failed: "
                + _normalize_text(archive_export_result.get("error") or "unknown")
            )
    elif auto_effective_mode == "hot_delete" and not blockers and archive_target_path is not None:
        hot_delete_result = _execute_hot_delete_page(
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
            columns=selected_columns,
            page_limit=effective_page_limit,
        )
        if hot_delete_result.get("status") == "failed":
            blockers.append(
                "hot delete failed: "
                + _normalize_text(hot_delete_result.get("error") or "unknown")
            )

    if archive_target_path and cutoff_date:
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
        )
    if active_db_exists and cutoff_date:
        primary_snapshot = _inspect_primary_db(db_path=active_db_path, cutoff_date=cutoff_date)
        selected_columns = list(primary_snapshot.get("selected_archive_columns") or selected_columns)
    live_cold_row_count = _normalize_int(primary_snapshot.get("live_cold_row_count"))
    live_cold_fund_count = _normalize_int(primary_snapshot.get("live_cold_fund_count"))
    existing_archive_row_count = _normalize_int(archive_snapshot.get("existing_archive_row_count"))
    existing_archive_fund_count = _normalize_int(archive_snapshot.get("existing_archive_fund_count"))
    archive_out_of_scope_row_count = _normalize_int(
        archive_snapshot.get("archive_out_of_scope_row_count")
    )
    archive_missing_row_count = max(expected_archive_row_count - existing_archive_row_count, 0)
    archive_materialized_and_reconciled = bool(
        expected_archive_row_count > 0
        and existing_archive_row_count == expected_archive_row_count
        and archive_out_of_scope_row_count == 0
        and (
            expected_archive_fund_count <= 0
            or existing_archive_fund_count == expected_archive_fund_count
        )
    )
    can_inspect_match_after = bool(
        active_db_exists
        and archive_target_path
        and archive_target_path.exists()
        and bool(archive_snapshot.get("archive_table_exists"))
        and bool(archive_snapshot.get("archive_table_schema_valid"))
        and cutoff_date
        and selected_columns
    )
    hot_delete_deleted_row_count = _normalize_int(hot_delete_result.get("deleted_row_count"))
    no_progress_hot_delete = bool(
        auto_effective_mode == "hot_delete"
        and str(hot_delete_result.get("status") or "") == "executed"
        and hot_delete_deleted_row_count == 0
        and live_cold_row_count > 0
    )
    needs_exact_match_scan_after = bool(
        can_inspect_match_after
        and archive_materialized_and_reconciled
        and (live_cold_row_count <= 0 or no_progress_hot_delete)
    )
    if needs_exact_match_scan_after:
        match_snapshot = _inspect_match_state(
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
            columns=selected_columns,
        )
    elif can_inspect_match_after:
        match_snapshot = _skipped_match_state(
            reason=(
                "archive_not_fully_materialized"
                if not archive_materialized_and_reconciled
                else "hot_delete_batch_match_scan_deferred"
            ),
            archive_missing_row_count=archive_missing_row_count,
        )
    else:
        match_snapshot = _skipped_match_state(
            reason="archive_match_context_unavailable",
            archive_missing_row_count=archive_missing_row_count,
        )
    hot_unarchived_row_count = _normalize_int(match_snapshot.get("hot_unarchived_row_count"))
    key_conflict_row_count = _normalize_int(match_snapshot.get("key_conflict_row_count"))
    exact_matched_hot_cold_row_count = _normalize_int(
        match_snapshot.get("exact_matched_hot_cold_row_count")
    )
    if no_progress_hot_delete:
        if not bool(match_snapshot.get("exact_match_full_scan_executed")):
            blockers.append("hot delete made no progress and exact-match diagnostic did not run.")
        if hot_unarchived_row_count > 0:
            blockers.append(
                f"hot delete made no progress with {hot_unarchived_row_count} unarchived hot rows."
            )
        if key_conflict_row_count > 0:
            blockers.append(
                f"hot delete made no progress with {key_conflict_row_count} rank key conflicts."
            )
    archive_binding_active = bool(
        archive_materialized_and_reconciled
        and live_cold_row_count == 0
        and (
            not can_inspect_match_after
            or (
                bool(match_snapshot.get("exact_match_full_scan_executed"))
                and hot_unarchived_row_count == 0
                and key_conflict_row_count == 0
            )
        )
    )
    if blockers:
        runner_status = "blocked"
        auto_governance_stage = "rank_archive_blocked"
    elif archive_binding_active:
        runner_status = "archive_binding_active"
        auto_governance_stage = "rank_hot_cold_binding_active"
    elif archive_missing_row_count > 0:
        runner_status = "rank_archive_export_in_progress"
        auto_governance_stage = "rank_archive_export_in_progress"
    elif live_cold_row_count > 0:
        runner_status = "rank_archive_export_complete_ready_for_hot_delete"
        auto_governance_stage = "rank_archive_export_complete_ready_for_hot_delete"
    else:
        runner_status = "rank_archive_not_required"
        auto_governance_stage = "rank_archive_not_required"

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "effective_mode": auto_effective_mode,
        "archive_export_supported": True,
        "hot_delete_supported": True,
        "vacuum_supported": False,
        "source_preflight_artifact": _stringify_project_path(preflight_artifact_path, root),
        "preflight_status": preflight_summary.get("status") or "",
        "preflight_ready": bool(preflight_checks.get("ready_for_archive_runbook")),
        "require_backup_for_hot_delete": bool(require_backup_for_hot_delete),
        "backup_artifact_path": _stringify_project_path(backup_path, root),
        "backup_artifact_absolute_path": str(backup_path or ""),
        "backup_artifact_exists": backup_artifact_exists,
        "archive_cutoff_date": cutoff_date,
        "active_db_path": _stringify_project_path(active_db_path, root),
        "active_db_absolute_path": str(active_db_path),
        "active_db_quick_check": _sqlite_quick_check(active_db_path),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "archive_target_exists": bool(archive_snapshot.get("archive_target_exists")),
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_quick_check": _normalize_text(archive_snapshot.get("archive_quick_check")),
        "archive_table_name": archive_table_name,
        "archive_table_exists": bool(archive_snapshot.get("archive_table_exists")),
        "archive_table_schema_valid": bool(archive_snapshot.get("archive_table_schema_valid")),
        "selected_archive_columns": selected_columns,
        "expected_archive_row_count": expected_archive_row_count,
        "expected_archive_fund_count": expected_archive_fund_count,
        "existing_archive_row_count": existing_archive_row_count,
        "existing_archive_fund_count": existing_archive_fund_count,
        "archive_out_of_scope_row_count": archive_out_of_scope_row_count,
        "archive_missing_row_count": archive_missing_row_count,
        "archive_materialized_and_reconciled": archive_materialized_and_reconciled,
        "archive_binding_active": archive_binding_active,
        "live_cold_row_count": live_cold_row_count,
        "live_cold_fund_count": live_cold_fund_count,
        "live_cold_min_rank_date": primary_snapshot.get("live_cold_min_rank_date") or "",
        "live_cold_max_rank_date": primary_snapshot.get("live_cold_max_rank_date") or "",
        "exact_matched_hot_cold_row_count": exact_matched_hot_cold_row_count,
        "hot_unarchived_row_count": hot_unarchived_row_count,
        "key_conflict_row_count": key_conflict_row_count,
        "page_limit": effective_page_limit,
        "archive_export_result_status": archive_export_result.get("status") or "",
        "archive_export_inserted_row_count": _normalize_int(
            archive_export_result.get("inserted_row_count")
        ),
        "hot_delete_result_status": hot_delete_result.get("status") or "",
        "hot_delete_deleted_row_count": _normalize_int(hot_delete_result.get("deleted_row_count")),
        "archive_sql_executed": bool(archive_export_result.get("archive_sql_executed")),
        "hot_delete_sql_executed": bool(hot_delete_result.get("hot_delete_sql_executed")),
        "delete_sql_executed": bool(hot_delete_result.get("delete_sql_executed")),
        "vacuum_executed": False,
        "blocker_count": len(blockers),
    }
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "summary": summary,
        "gate_checks": gates,
        "blockers": blockers,
        "primary_snapshot": primary_snapshot,
        "archive_snapshot": archive_snapshot,
        "match_snapshot": match_snapshot,
        "archive_export_result": archive_export_result,
        "hot_delete_result": hot_delete_result,
        "findings": _build_findings(summary, blockers),
        "safety": {
            "archive_sql_executed": bool(archive_export_result.get("archive_sql_executed")),
            "delete_sql_executed": bool(hot_delete_result.get("delete_sql_executed")),
            "vacuum_executed": False,
            "hot_delete_exact_match_required": True,
        },
    }


def render_rank_history_archive_apply_runner_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# rank history archive apply runner",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Runner status: {summary.get('runner_status', '')}",
        f"- Mode: {summary.get('mode', '')}",
        f"- Effective mode: {summary.get('effective_mode', '')}",
        f"- Cutoff: {summary.get('archive_cutoff_date', '')}",
        f"- Expected archive rows: {summary.get('expected_archive_row_count', 0)}",
        f"- Existing archive rows: {summary.get('existing_archive_row_count', 0)}",
        f"- Archive missing rows: {summary.get('archive_missing_row_count', 0)}",
        f"- Live cold rows: {summary.get('live_cold_row_count', 0)}",
        f"- Archive binding active: {summary.get('archive_binding_active', False)}",
        f"- Blockers: {summary.get('blocker_count', 0)}",
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
            lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_rank_history_archive_apply_runner_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"rank_history_archive_apply_runner_{ts}.json"
    md_path = artifact_dir / f"rank_history_archive_apply_runner_{ts}.md"
    md_path.write_text(render_rank_history_archive_apply_runner_markdown(payload), encoding="utf-8")
    # Only the complete, durable JSON is discoverable as an active binding.
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=artifact_dir,
                                         prefix=".rank-binding-", suffix=".tmp", delete=False) as handle:
            temp_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, json_path)
        directory_fd = os.open(artifact_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    return json_path, md_path
