#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rolling NAV hot/cold archive runner for an already active binding."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_apply_runner import (
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    DEFAULT_OUTPUT_DIR,
    NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NAV_HISTORY_ARCHIVE_ROLLING_RUNNER_JSON_PATTERN = (
    "nav_history_archive_rolling_runner_*.json"
)
ALLOWED_MODES = {"dry_run", "rolling_archive"}
ARCHIVE_TABLE_NAME = "fund_nav_history_archive"
ROLLING_HOT_DB_ALIAS = "rolling_hot_db"
ROLLING_ARCHIVE_DB_ALIAS = "rolling_archive_db"
REQUIRED_NAV_ARCHIVE_COLUMNS = ("fcode", "nav_date", "unit_nav", "accum_nav", "daily_growth")


def load_latest_nav_history_archive_rolling_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_ROLLING_RUNNER_JSON_PATTERN,
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


def _parse_iso_date(value: Any) -> Optional[date]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


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
    if not path.is_absolute():
        return (project_root / path).resolve()
    if path.exists():
        return path.resolve()
    parts = path.parts
    for anchor in ("backend", "reports", "config", "logs", "core", "frontend", "scripts"):
        if anchor not in parts:
            continue
        anchor_index = parts.index(anchor)
        remapped = project_root.joinpath(*parts[anchor_index:]).resolve()
        if remapped.exists() or remapped.parent.exists():
            return remapped
    return path.resolve()


def _stringify_project_path(path: Optional[Path], project_root: Path) -> str:
    if path is None:
        return ""
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _open_readonly_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=120000")
    except sqlite3.Error:
        pass
    return conn


def _sqlite_quick_check(path: Optional[Path]) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    try:
        with _open_readonly_db(path) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0] if row else "").strip()
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _normalize_table_name(value: Any) -> str:
    text = _normalize_text(value) or ARCHIVE_TABLE_NAME
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text):
        return ARCHIVE_TABLE_NAME
    return text


def _fetch_one_dict(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict:
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return dict(row)
    columns = [item[0] for item in cursor.description or []]
    return dict(zip(columns, row))


def _fetch_all_dicts(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cursor = conn.execute(sql, params)
    rows = cursor.fetchall()
    columns = [item[0] for item in cursor.description or []]
    result: list[dict] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            result.append(dict(zip(columns, row)))
    return result


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _archive_table_schema_valid(conn: sqlite3.Connection, table_name: str) -> bool:
    rows = _fetch_all_dicts(conn, f"PRAGMA table_info({table_name})")
    columns = {str(row.get("name") or "").strip() for row in rows}
    return set(REQUIRED_NAV_ARCHIVE_COLUMNS).issubset(columns)


def _ensure_archive_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            fcode TEXT NOT NULL,
            nav_date TEXT NOT NULL,
            unit_nav REAL NOT NULL,
            accum_nav REAL,
            daily_growth REAL,
            PRIMARY KEY (fcode, nav_date)
        )
        """
    )


def _load_latest_active_apply_runner_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Optional[Path], dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    try:
        candidates = sorted(resolved_output_dir.glob(NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN))
    except OSError:
        return None, {}
    for candidate in reversed(candidates):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        summary = _normalize_dict(payload.get("summary")) if isinstance(payload, dict) else {}
        if not summary.get("archive_binding_active"):
            continue
        if _normalize_text(summary.get("runner_status")) != "archive_binding_active":
            continue
        return candidate, payload
    return None, {}


def _nav_exact_match_sql(hot_alias: str, archive_alias: str = "archived") -> str:
    return (
        f"{archive_alias}.fcode = {hot_alias}.fcode "
        f"AND {archive_alias}.nav_date = {hot_alias}.nav_date "
        f"AND (({archive_alias}.unit_nav = {hot_alias}.unit_nav) "
        f"OR ({archive_alias}.unit_nav IS NULL AND {hot_alias}.unit_nav IS NULL)) "
        f"AND (({archive_alias}.accum_nav = {hot_alias}.accum_nav) "
        f"OR ({archive_alias}.accum_nav IS NULL AND {hot_alias}.accum_nav IS NULL)) "
        f"AND (({archive_alias}.daily_growth = {hot_alias}.daily_growth) "
        f"OR ({archive_alias}.daily_growth IS NULL AND {hot_alias}.daily_growth IS NULL))"
    )


def _inspect_primary_db(
    *,
    db_path: Path,
    active_cutoff_date: date,
    current_cutoff_date: date,
    page_limit: int,
) -> dict:
    if not db_path.exists() or not db_path.is_file():
        return {"available": False, "error": f"primary database missing: {db_path}"}
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    with _open_readonly_db(db_path) as conn:
        total = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS total_nav_row_count,
                COUNT(DISTINCT fcode) AS total_nav_fund_count,
                MIN(nav_date) AS min_nav_date,
                MAX(nav_date) AS max_nav_date
            FROM fund_nav_history
            """,
        )
        before_active = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_active_cutoff_row_count,
                COUNT(DISTINCT fcode) AS hot_before_active_cutoff_fund_count,
                MIN(nav_date) AS hot_before_active_cutoff_min_nav_date,
                MAX(nav_date) AS hot_before_active_cutoff_max_nav_date
            FROM fund_nav_history
            WHERE nav_date < ?
            """,
            (active_cutoff,),
        )
        before_current = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_current_cutoff_row_count,
                COUNT(DISTINCT fcode) AS hot_before_current_cutoff_fund_count,
                MIN(nav_date) AS hot_before_current_cutoff_min_nav_date,
                MAX(nav_date) AS hot_before_current_cutoff_max_nav_date
            FROM fund_nav_history
            WHERE nav_date < ?
            """,
            (current_cutoff,),
        )
        rolling = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS rolling_hot_candidate_row_count,
                COUNT(DISTINCT fcode) AS rolling_hot_candidate_fund_count,
                MIN(nav_date) AS rolling_hot_candidate_min_nav_date,
                MAX(nav_date) AS rolling_hot_candidate_max_nav_date
            FROM fund_nav_history
            WHERE nav_date >= ? AND nav_date < ?
            """,
            (active_cutoff, current_cutoff),
        )
        sample_rows = _fetch_all_dicts(
            conn,
            """
            SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth
            FROM fund_nav_history
            WHERE nav_date >= ? AND nav_date < ?
            ORDER BY nav_date, fcode
            LIMIT ?
            """,
            (active_cutoff, current_cutoff, min(max(1, int(page_limit or 1)), 20)),
        )
        page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "available": True,
        **total,
        **before_active,
        **before_current,
        **rolling,
        "sample_rolling_rows": sample_rows,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "database_size_mb": _round_mb(db_path.stat().st_size),
    }


def _inspect_archive_db(
    *,
    archive_target_path: Optional[Path],
    archive_table_name: str,
    active_cutoff_date: date,
    current_cutoff_date: date,
) -> dict:
    table_name = _normalize_table_name(archive_table_name)
    if archive_target_path is None:
        return {"archive_target_exists": False, "archive_table_exists": False}
    if not archive_target_path.exists() or not archive_target_path.is_file():
        return {
            "archive_target_exists": False,
            "archive_table_exists": False,
            "archive_target_size_mb": 0.0,
        }
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    payload: dict[str, Any] = {
        "archive_target_exists": True,
        "archive_target_size_mb": _round_mb(archive_target_path.stat().st_size),
        "archive_table_exists": False,
    }
    try:
        with _open_readonly_db(archive_target_path) as conn:
            table_exists = _table_exists(conn, table_name)
            payload["archive_table_exists"] = table_exists
            if not table_exists:
                return payload
            schema_valid = _archive_table_schema_valid(conn, table_name)
            payload["archive_table_schema_valid"] = schema_valid
            if not schema_valid:
                return payload
            before_active = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS archive_before_active_cutoff_row_count,
                    COUNT(DISTINCT fcode) AS archive_before_active_cutoff_fund_count,
                    MIN(nav_date) AS archive_before_active_cutoff_min_nav_date,
                    MAX(nav_date) AS archive_before_active_cutoff_max_nav_date
                FROM {table_name}
                WHERE nav_date < ?
                """,
                (active_cutoff,),
            )
            before_current = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS archive_before_current_cutoff_row_count,
                    COUNT(DISTINCT fcode) AS archive_before_current_cutoff_fund_count,
                    MIN(nav_date) AS archive_before_current_cutoff_min_nav_date,
                    MAX(nav_date) AS archive_before_current_cutoff_max_nav_date
                FROM {table_name}
                WHERE nav_date < ?
                """,
                (current_cutoff,),
            )
            rolling = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS rolling_archive_window_row_count,
                    COUNT(DISTINCT fcode) AS rolling_archive_window_fund_count,
                    MIN(nav_date) AS rolling_archive_window_min_nav_date,
                    MAX(nav_date) AS rolling_archive_window_max_nav_date
                FROM {table_name}
                WHERE nav_date >= ? AND nav_date < ?
                """,
                (active_cutoff, current_cutoff),
            )
            out_of_scope = _fetch_one_dict(
                conn,
                f"SELECT COUNT(*) AS archive_out_of_scope_current_row_count FROM {table_name} WHERE nav_date >= ?",
                (current_cutoff,),
            )
            total = _fetch_one_dict(
                conn,
                f"SELECT COUNT(*) AS archive_total_row_count FROM {table_name}",
            )
            payload.update(before_active)
            payload.update(before_current)
            payload.update(rolling)
            payload.update(out_of_scope)
            payload.update(total)
    except sqlite3.Error as exc:
        payload["archive_inspection_error"] = str(exc)
    return payload


def _inspect_rolling_match_state(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    current_cutoff_date: date,
) -> dict:
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _nav_exact_match_sql("hot", "archived")
    with sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        try:
            hot_unarchived = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS hot_unarchived_window_row_count
                FROM fund_nav_history AS hot
                WHERE hot.nav_date >= ? AND hot.nav_date < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                """,
                (active_cutoff, current_cutoff),
            )
            exact_matched = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS exact_matched_window_row_count
                FROM fund_nav_history AS hot
                WHERE hot.nav_date >= ? AND hot.nav_date < ?
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                """,
                (active_cutoff, current_cutoff),
            )
            key_conflict = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS key_conflict_window_row_count
                FROM fund_nav_history AS hot
                INNER JOIN {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                    ON archived.fcode = hot.fcode
                   AND archived.nav_date = hot.nav_date
                WHERE hot.nav_date >= ? AND hot.nav_date < ?
                  AND NOT ({exact_match})
                """,
                (active_cutoff, current_cutoff),
            )
        finally:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
    return {
        **hot_unarchived,
        **exact_matched,
        **key_conflict,
    }


def _inspect_stale_hot_before_active_state(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
) -> dict:
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _nav_exact_match_sql("hot", "archived")
    with sqlite3.connect(_sqlite_readonly_uri(db_path), timeout=120, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        try:
            exact_matched = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS stale_hot_exact_matched_row_count
                FROM fund_nav_history AS hot
                WHERE hot.nav_date < ?
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                """,
                (active_cutoff,),
            )
            missing_key = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS stale_hot_missing_archive_key_row_count
                FROM fund_nav_history AS hot
                WHERE hot.nav_date < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE archived.fcode = hot.fcode
                        AND archived.nav_date = hot.nav_date
                  )
                """,
                (active_cutoff,),
            )
            missing_profile = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT
                        hot.fcode,
                        hot.nav_date,
                        hot.unit_nav,
                        hot.accum_nav,
                        hot.daily_growth
                    FROM fund_nav_history AS hot
                    WHERE hot.nav_date < ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE archived.fcode = hot.fcode
                            AND archived.nav_date = hot.nav_date
                      )
                )
                SELECT
                    COUNT(*) AS stale_hot_missing_archive_candidate_row_count,
                    COUNT(DISTINCT fcode) AS stale_hot_missing_archive_candidate_fund_count,
                    MIN(nav_date) AS stale_hot_missing_archive_candidate_min_nav_date,
                    MAX(nav_date) AS stale_hot_missing_archive_candidate_max_nav_date,
                    SUM(CASE WHEN fcode IS NULL OR TRIM(fcode) = '' THEN 1 ELSE 0 END)
                        AS stale_hot_missing_archive_invalid_fcode_row_count,
                    SUM(CASE WHEN nav_date IS NULL OR TRIM(nav_date) = '' THEN 1 ELSE 0 END)
                        AS stale_hot_missing_archive_invalid_nav_date_row_count,
                    SUM(CASE WHEN unit_nav IS NULL THEN 1 ELSE 0 END)
                        AS stale_hot_missing_archive_null_unit_nav_row_count
                FROM missing
                """,
                (active_cutoff,),
            )
            missing_unique = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT hot.fcode, hot.nav_date
                    FROM fund_nav_history AS hot
                    WHERE hot.nav_date < ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE archived.fcode = hot.fcode
                            AND archived.nav_date = hot.nav_date
                      )
                )
                SELECT COUNT(*) AS stale_hot_missing_archive_unique_key_count
                FROM (
                    SELECT fcode, nav_date
                    FROM missing
                    GROUP BY fcode, nav_date
                )
                """,
                (active_cutoff,),
            )
            missing_duplicates = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT hot.fcode, hot.nav_date
                    FROM fund_nav_history AS hot
                    WHERE hot.nav_date < ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE archived.fcode = hot.fcode
                            AND archived.nav_date = hot.nav_date
                      )
                ),
                duplicate_keys AS (
                    SELECT fcode, nav_date, COUNT(*) AS row_count
                    FROM missing
                    GROUP BY fcode, nav_date
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COUNT(*) AS stale_hot_missing_archive_duplicate_key_count,
                    COALESCE(SUM(row_count), 0) AS stale_hot_missing_archive_duplicate_key_row_count
                FROM duplicate_keys
                """,
                (active_cutoff,),
            )
            missing_sample = _fetch_all_dicts(
                conn,
                f"""
                SELECT hot.fcode, hot.nav_date, hot.unit_nav, hot.accum_nav, hot.daily_growth
                FROM fund_nav_history AS hot
                WHERE hot.nav_date < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE archived.fcode = hot.fcode
                        AND archived.nav_date = hot.nav_date
                  )
                ORDER BY hot.nav_date, hot.fcode
                LIMIT 20
                """,
                (active_cutoff,),
            )
            key_conflict = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS stale_hot_key_conflict_row_count
                FROM fund_nav_history AS hot
                INNER JOIN {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                    ON archived.fcode = hot.fcode
                   AND archived.nav_date = hot.nav_date
                WHERE hot.nav_date < ?
                  AND NOT ({exact_match})
                """,
                (active_cutoff,),
            )
        finally:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
    return {
        **exact_matched,
        **missing_key,
        **missing_profile,
        **missing_unique,
        **missing_duplicates,
        "sample_stale_hot_missing_archive_rows": missing_sample,
        **key_conflict,
    }


def _summary_path_matches(
    summary: dict,
    keys: tuple[str, ...],
    expected_path: Optional[Path],
    project_root: Path,
) -> bool:
    if expected_path is None:
        return False
    expected = expected_path.resolve()
    for key in keys:
        candidate = _resolve_path(summary.get(key), project_root)
        if candidate is not None and candidate.resolve() == expected:
            return True
    return False


def _load_reusable_stale_hot_before_active_evidence(
    *,
    output_dir: str | Path,
    project_root: Path,
    active_db_path: Optional[Path],
    archive_target_path: Optional[Path],
    archive_table_name: str,
    active_cutoff_text: str,
    current_hot_before_active_count: int | None = None,
) -> dict:
    """Reuse a previous heavy stale-hot scan only as delete-page evidence.

    The reused evidence never deletes data by itself. The later DELETE still
    requires exact cold-archive NAV equality for every row selected.
    """
    if not active_db_path or not archive_target_path or not active_cutoff_text:
        return {}
    try:
        artifact_path, payload = load_latest_nav_history_archive_rolling_runner_payload(
            output_dir=str(output_dir)
        )
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    summary = _normalize_dict(payload.get("summary"))
    if not summary:
        return {}
    if _normalize_text(summary.get("active_cutoff_date")) != active_cutoff_text:
        return {}
    if _normalize_table_name(summary.get("archive_table_name")) != archive_table_name:
        return {}
    if not _summary_path_matches(
        summary,
        ("active_db_absolute_path", "active_db_path"),
        active_db_path,
        project_root,
    ):
        return {}
    if not _summary_path_matches(
        summary,
        ("archive_target_absolute_path", "archive_target_path"),
        archive_target_path,
        project_root,
    ):
        return {}

    exact_rows = _normalize_int(summary.get("stale_hot_exact_matched_row_count"), -1)
    missing_rows = _normalize_int(summary.get("stale_hot_missing_archive_key_row_count"), -1)
    conflict_rows = _normalize_int(summary.get("stale_hot_key_conflict_row_count"), -1)
    source_hot_before_active = _normalize_int(
        summary.get("hot_before_active_cutoff_row_count"), -1
    )
    if exact_rows <= 0 or missing_rows < 0 or conflict_rows != 0 or source_hot_before_active <= 0:
        return {}
    if (
        current_hot_before_active_count is not None
        and current_hot_before_active_count >= 0
        and source_hot_before_active != current_hot_before_active_count
    ):
        return {}
    return {
        "stale_hot_exact_matched_row_count": exact_rows,
        "stale_hot_missing_archive_key_row_count": missing_rows,
        "stale_hot_key_conflict_row_count": conflict_rows,
        "evidence_reused": True,
        "evidence_source_artifact": str(artifact_path),
        "evidence_scope": "stale_hot_before_active_counts_only",
        "evidence_truth_policy": "delete_still_requires_exact_archive_match",
        "source_hot_before_active_cutoff_row_count": source_hot_before_active,
        "source_runner_status": _normalize_text(summary.get("runner_status")),
        "source_generated_at": _normalize_text(payload.get("generated_at")),
    }


def _execute_rolling_archive_export_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    current_cutoff_date: date,
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "archive_sql_executed": False,
        }
    table_name = _normalize_table_name(archive_table_name)
    archive_target_path.parent.mkdir(parents=True, exist_ok=True)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    hot_uri = _sqlite_readonly_uri(db_path)
    conn = sqlite3.connect(str(archive_target_path), uri=True, isolation_level=None, timeout=120)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_HOT_DB_ALIAS}", (hot_uri,))
        attached = True
        conn.execute("BEGIN IMMEDIATE")
        _ensure_archive_table(conn, table_name)
        if not _archive_table_schema_valid(conn, table_name):
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": "archive_table_schema_invalid",
                "archive_sql_executed": False,
            }
        before = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE nav_date >= ? AND nav_date < ?",
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {table_name}
                (fcode, nav_date, unit_nav, accum_nav, daily_growth)
            SELECT
                hot.fcode,
                hot.nav_date,
                hot.unit_nav,
                hot.accum_nav,
                hot.daily_growth
            FROM {ROLLING_HOT_DB_ALIAS}.fund_nav_history AS hot
            WHERE hot.nav_date >= ?
              AND hot.nav_date < ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM {table_name} AS archived
                  WHERE archived.fcode = hot.fcode
                    AND archived.nav_date = hot.nav_date
              )
            ORDER BY hot.nav_date, hot.fcode
            LIMIT ?
            """,
            (active_cutoff, current_cutoff, int(page_limit)),
        )
        after = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE nav_date >= ? AND nav_date < ?",
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        conn.execute("COMMIT")
        return {
            "status": "executed",
            "archive_sql_executed": True,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "rolling_archive_window_before": before,
            "rolling_archive_window_after": after,
            "inserted_row_count": max(after - before, 0),
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
                conn.execute(f"DETACH DATABASE {ROLLING_HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_stale_hot_missing_archive_and_delete_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "stale_hot_missing_archive_sql_executed": False,
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "inserted_row_count": 0,
            "deleted_row_count": 0,
        }
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(
            f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}",
            (str(archive_target_path),),
        )
        attached = True
        archive_table_exists = (
            conn.execute(
                f"""
                SELECT 1
                FROM {ROLLING_ARCHIVE_DB_ALIAS}.sqlite_master
                WHERE type='table' AND name=?
                """,
                (table_name,),
            ).fetchone()
            is not None
        )
        if not archive_table_exists:
            return {
                "status": "failed",
                "error": "archive_table_missing",
                "stale_hot_missing_archive_sql_executed": False,
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "inserted_row_count": 0,
                "deleted_row_count": 0,
            }
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP TABLE IF EXISTS temp.selected_stale_hot_missing_nav")
        conn.execute(
            """
            CREATE TEMP TABLE selected_stale_hot_missing_nav (
                hot_rowid INTEGER PRIMARY KEY,
                fcode TEXT NOT NULL,
                nav_date TEXT NOT NULL,
                unit_nav REAL NOT NULL,
                accum_nav REAL,
                daily_growth REAL
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO temp.selected_stale_hot_missing_nav
                (hot_rowid, fcode, nav_date, unit_nav, accum_nav, daily_growth)
            SELECT
                hot.rowid,
                hot.fcode,
                hot.nav_date,
                hot.unit_nav,
                hot.accum_nav,
                hot.daily_growth
            FROM fund_nav_history AS hot
            WHERE hot.nav_date < ?
              AND hot.fcode IS NOT NULL
              AND TRIM(hot.fcode) != ''
              AND hot.nav_date IS NOT NULL
              AND TRIM(hot.nav_date) != ''
              AND hot.unit_nav IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                  WHERE archived.fcode = hot.fcode
                    AND archived.nav_date = hot.nav_date
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM fund_nav_history AS duplicate_hot
                  WHERE duplicate_hot.fcode = hot.fcode
                    AND duplicate_hot.nav_date = hot.nav_date
                    AND duplicate_hot.rowid != hot.rowid
              )
            ORDER BY hot.nav_date, hot.fcode
            LIMIT ?
            """,
            (active_cutoff, int(page_limit)),
        )
        selected_count = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM temp.selected_stale_hot_missing_nav"
            ).fetchone()[0]
        )
        if selected_count <= 0:
            conn.execute("ROLLBACK")
            return {
                "status": "stale_hot_missing_archive_evidence_expired",
                "stale_hot_missing_archive_sql_executed": False,
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "inserted_row_count": 0,
                "deleted_row_count": 0,
                "selected_row_count": 0,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            }
        archive_before = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name}
                WHERE nav_date < ?
                """,
                (active_cutoff,),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            INSERT INTO {ROLLING_ARCHIVE_DB_ALIAS}.{table_name}
                (fcode, nav_date, unit_nav, accum_nav, daily_growth)
            SELECT
                selected.fcode,
                selected.nav_date,
                selected.unit_nav,
                selected.accum_nav,
                selected.daily_growth
            FROM temp.selected_stale_hot_missing_nav AS selected
            WHERE NOT EXISTS (
                SELECT 1
                FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                WHERE archived.fcode = selected.fcode
                  AND archived.nav_date = selected.nav_date
            )
            ORDER BY selected.nav_date, selected.fcode
            """,
        )
        inserted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        if inserted_count != selected_count:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": (
                    "stale_hot_missing_archive_insert_count_mismatch:"
                    f" selected={selected_count}, inserted={inserted_count}"
                ),
                "stale_hot_missing_archive_sql_executed": False,
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "selected_row_count": selected_count,
                "inserted_row_count": inserted_count,
                "deleted_row_count": 0,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            }
        exact_match = _nav_exact_match_sql("selected", "archived")
        exact_count = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM temp.selected_stale_hot_missing_nav AS selected
                WHERE EXISTS (
                    SELECT 1
                    FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                    WHERE {exact_match}
                )
                """
            ).fetchone()[0]
        )
        if exact_count != selected_count:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": (
                    "stale_hot_missing_archive_exact_match_mismatch:"
                    f" selected={selected_count}, exact={exact_count}"
                ),
                "stale_hot_missing_archive_sql_executed": False,
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "selected_row_count": selected_count,
                "inserted_row_count": inserted_count,
                "deleted_row_count": 0,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            }
        hot_before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
                (active_cutoff,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            DELETE FROM fund_nav_history
            WHERE rowid IN (
                SELECT hot_rowid
                FROM temp.selected_stale_hot_missing_nav
            )
            """,
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        if deleted_count != selected_count:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": (
                    "stale_hot_missing_archive_delete_count_mismatch:"
                    f" selected={selected_count}, deleted={deleted_count}"
                ),
                "stale_hot_missing_archive_sql_executed": False,
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "selected_row_count": selected_count,
                "inserted_row_count": inserted_count,
                "deleted_row_count": deleted_count,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            }
        archive_after = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name}
                WHERE nav_date < ?
                """,
                (active_cutoff,),
            ).fetchone()[0]
        )
        hot_after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
                (active_cutoff,),
            ).fetchone()[0]
        )
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        conn.execute("COMMIT")
        return {
            "status": "executed",
            "stale_hot_missing_archive_sql_executed": True,
            "archive_sql_executed": True,
            "delete_sql_executed": True,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "selected_row_count": selected_count,
            "inserted_row_count": inserted_count,
            "deleted_row_count": deleted_count,
            "stale_hot_archive_rows_before": archive_before,
            "stale_hot_archive_rows_after": archive_after,
            "stale_hot_rows_before": hot_before,
            "stale_hot_rows_after": hot_after,
            "page_count": page_count,
            "freelist_count": freelist_count,
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
            "stale_hot_missing_archive_sql_executed": False,
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "inserted_row_count": 0,
            "deleted_row_count": 0,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_hot_missing_nav")
        except sqlite3.Error:
            pass
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_stale_hot_before_active_delete_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "stale_hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        }
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _nav_exact_match_sql("hot", "archived")
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
                (active_cutoff,),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            DELETE FROM fund_nav_history
            WHERE rowid IN (
                SELECT hot.rowid
                FROM fund_nav_history AS hot
                WHERE hot.nav_date < ?
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                ORDER BY hot.nav_date, hot.fcode
                LIMIT ?
            )
            """,
            (active_cutoff, int(page_limit)),
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
                (active_cutoff,),
            ).fetchone()[0]
        )
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "status": "executed",
            "stale_hot_delete_sql_executed": True,
            "delete_sql_executed": True,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "stale_hot_rows_before": before,
            "stale_hot_rows_after": after,
            "deleted_row_count": deleted_count,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    except sqlite3.Error as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "stale_hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_rolling_hot_delete_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    current_cutoff_date: date,
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
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _nav_exact_match_sql("hot", "archived")
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date >= ? AND nav_date < ?",
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            DELETE FROM fund_nav_history
            WHERE rowid IN (
                SELECT hot.rowid
                FROM fund_nav_history AS hot
                WHERE hot.nav_date >= ?
                  AND hot.nav_date < ?
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                ORDER BY hot.nav_date, hot.fcode
                LIMIT ?
            )
            """,
            (active_cutoff, current_cutoff, int(page_limit)),
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date >= ? AND nav_date < ?",
                (active_cutoff, current_cutoff),
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
            "rolling_hot_rows_before": before,
            "rolling_hot_rows_after": after,
            "deleted_row_count": deleted_count,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    except sqlite3.Error as exc:
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
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
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


def _build_updated_active_binding_payload(
    *,
    root: Path,
    source_active_artifact: Optional[Path],
    active_summary: dict,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    current_cutoff_date: date,
    primary_snapshot: dict,
    archive_snapshot: dict,
    rolling_summary: dict,
) -> dict:
    archive_rows = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_row_count"))
    archive_funds = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_fund_count"))
    hot_total = _normalize_int(primary_snapshot.get("total_nav_row_count"))
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = {
        "runner_status": "archive_binding_active",
        "auto_governance_stage": "hot_cold_binding_active",
        "mode": "rolling_archive_binding",
        "dry_run_only": True,
        "dry_run_safe": True,
        "archive_export_supported": True,
        "archive_apply_supported": False,
        "delete_supported": False,
        "vacuum_supported": False,
        "archive_export_executed": False,
        "archive_export_result_status": "rolling_archive_complete",
        "archive_export_inserted_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_inserted_row_count")
        ),
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "archive_binding_active": True,
        "archive_materialized_and_reconciled": True,
        "archive_partial_conflict": False,
        "archive_export_in_progress": False,
        "archive_out_of_scope_row_count": _normalize_int(
            archive_snapshot.get("archive_out_of_scope_current_row_count")
        ),
        "archive_table_schema_valid": bool(archive_snapshot.get("archive_table_schema_valid")),
        "archive_inspection_error": _normalize_text(archive_snapshot.get("archive_inspection_error")),
        "active_db_quick_check": _sqlite_quick_check(db_path),
        "archive_quick_check": _sqlite_quick_check(archive_target_path),
        "readiness_status": "post_split_binding_health",
        "ready_for_manual_window_execution": False,
        "cold_query_compat_status": "post_split_binding_health",
        "query_compat_safe": True,
        "safe_to_apply_archive_now": False,
        "production_uncovered_cold_dependency_count": 0,
        "archive_cutoff_date": current_cutoff_date.isoformat(),
        "expected_archive_row_count": archive_rows,
        "expected_archive_fund_count": archive_funds,
        "live_cold_row_count": _normalize_int(
            primary_snapshot.get("hot_before_current_cutoff_row_count")
        ),
        "live_cold_fund_count": _normalize_int(
            primary_snapshot.get("hot_before_current_cutoff_fund_count")
        ),
        "live_cold_min_nav_date": primary_snapshot.get("hot_before_current_cutoff_min_nav_date") or "",
        "live_cold_max_nav_date": primary_snapshot.get("hot_before_current_cutoff_max_nav_date") or "",
        "live_counts_match_expected": True,
        "total_nav_row_count": hot_total,
        "total_nav_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "hot_retained_row_count": hot_total,
        "hot_retained_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "effective_hot_cold_row_count": hot_total + archive_rows,
        "active_db_path": str(db_path),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path),
        "archive_target_exists": True,
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_table_name": archive_table_name,
        "archive_table_exists": True,
        "existing_archive_row_count": archive_rows,
        "existing_archive_fund_count": archive_funds,
        "archive_missing_row_count": 0,
        "backup_artifact_path": _normalize_text(active_summary.get("backup_artifact_path")),
        "backup_artifact_absolute_path": _normalize_text(
            active_summary.get("backup_artifact_absolute_path")
        ),
        "backup_artifact_exists": bool(active_summary.get("backup_artifact_exists")),
        "page_limit": _normalize_int(rolling_summary.get("page_limit")),
        "page_preview_count": 0,
        "blocker_count": 0,
        "review_item_count": 0,
        "page_apply_supported": False,
        "execution_mode": "rolling_archive_binding",
        "source_active_apply_runner_artifact": str(source_active_artifact or ""),
        "source_rolling_runner_status": rolling_summary.get("runner_status") or "",
        "rolling_archive_required": False,
        "rolling_archive_supported": True,
        "rolling_archive_cutoff_date": current_cutoff_date.isoformat(),
        "rolling_archive_candidate_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_candidate_row_count")
        ),
        "rolling_archive_inserted_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_inserted_row_count")
        ),
        "rolling_archive_deleted_row_count": _normalize_int(
            rolling_summary.get("rolling_hot_delete_deleted_row_count")
        ),
        "rolling_archive_blocked_reason": "",
    }
    return {
        "generated_at": now_text,
        "available": True,
        "message": "NAV rolling archive completed and active cold binding was advanced.",
        "source_active_nav_history_archive_apply_runner_artifact": str(source_active_artifact or ""),
        "source_active_nav_history_archive_apply_runner_summary": active_summary,
        "summary": summary,
        "gate_checks": [],
        "blockers": [],
        "review_items": [],
        "findings": [
            "NAV rolling archive completed; active binding cutoff can be advanced without changing runtime query semantics.",
        ],
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "rolling_archive_binding_payload": True,
        },
    }


def _build_refreshed_active_binding_payload(
    *,
    root: Path,
    source_active_artifact: Optional[Path],
    active_summary: dict,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    primary_snapshot: dict,
    archive_snapshot: dict,
    rolling_summary: dict,
) -> dict:
    archive_rows = _normalize_int(archive_snapshot.get("archive_before_active_cutoff_row_count"))
    archive_funds = _normalize_int(archive_snapshot.get("archive_before_active_cutoff_fund_count"))
    hot_total = _normalize_int(primary_snapshot.get("total_nav_row_count"))
    live_cold_rows = _normalize_int(primary_snapshot.get("hot_before_active_cutoff_row_count"))
    live_cold_funds = _normalize_int(primary_snapshot.get("hot_before_active_cutoff_fund_count"))
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = {
        "runner_status": "archive_binding_active",
        "auto_governance_stage": "hot_cold_binding_active_stale_missing_archive_refresh",
        "mode": "rolling_archive_binding_refresh",
        "dry_run_only": True,
        "dry_run_safe": True,
        "archive_export_supported": True,
        "archive_apply_supported": False,
        "delete_supported": False,
        "vacuum_supported": False,
        "archive_export_executed": False,
        "archive_export_result_status": "stale_missing_archive_refresh",
        "archive_export_inserted_row_count": _normalize_int(
            rolling_summary.get("stale_hot_missing_archive_inserted_row_count")
        ),
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "archive_binding_active": True,
        "archive_materialized_and_reconciled": live_cold_rows == 0,
        "archive_partial_conflict": False,
        "archive_export_in_progress": live_cold_rows > 0,
        "archive_out_of_scope_row_count": _normalize_int(
            archive_snapshot.get("archive_out_of_scope_current_row_count")
        ),
        "archive_table_schema_valid": bool(archive_snapshot.get("archive_table_schema_valid")),
        "archive_inspection_error": _normalize_text(archive_snapshot.get("archive_inspection_error")),
        "active_db_quick_check": _sqlite_quick_check(db_path),
        "archive_quick_check": _sqlite_quick_check(archive_target_path),
        "readiness_status": "post_split_binding_health",
        "ready_for_manual_window_execution": False,
        "cold_query_compat_status": "post_split_binding_health",
        "query_compat_safe": True,
        "safe_to_apply_archive_now": False,
        "production_uncovered_cold_dependency_count": 0,
        "archive_cutoff_date": active_cutoff_date.isoformat(),
        "expected_archive_row_count": archive_rows,
        "expected_archive_fund_count": archive_funds,
        "live_cold_row_count": live_cold_rows,
        "live_cold_fund_count": live_cold_funds,
        "live_cold_min_nav_date": primary_snapshot.get("hot_before_active_cutoff_min_nav_date") or "",
        "live_cold_max_nav_date": primary_snapshot.get("hot_before_active_cutoff_max_nav_date") or "",
        "live_counts_match_expected": live_cold_rows == 0,
        "total_nav_row_count": hot_total,
        "total_nav_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "hot_retained_row_count": hot_total,
        "hot_retained_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "effective_hot_cold_row_count": hot_total + archive_rows,
        "active_db_path": str(db_path),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path),
        "archive_target_exists": True,
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_table_name": archive_table_name,
        "archive_table_exists": True,
        "existing_archive_row_count": archive_rows,
        "existing_archive_fund_count": archive_funds,
        "archive_missing_row_count": live_cold_rows,
        "backup_artifact_path": _normalize_text(active_summary.get("backup_artifact_path")),
        "backup_artifact_absolute_path": _normalize_text(
            active_summary.get("backup_artifact_absolute_path")
        ),
        "backup_artifact_exists": bool(active_summary.get("backup_artifact_exists")),
        "page_limit": _normalize_int(rolling_summary.get("page_limit")),
        "page_preview_count": 0,
        "blocker_count": 0,
        "review_item_count": 0,
        "page_apply_supported": False,
        "execution_mode": "rolling_archive_binding_refresh",
        "source_active_apply_runner_artifact": str(source_active_artifact or ""),
        "source_rolling_runner_status": rolling_summary.get("runner_status") or "",
        "rolling_archive_required": bool(live_cold_rows > 0),
        "rolling_archive_supported": True,
        "rolling_archive_cutoff_date": rolling_summary.get("rolling_archive_cutoff_date") or "",
        "rolling_archive_candidate_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_candidate_row_count")
        ),
        "rolling_archive_inserted_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_inserted_row_count")
        ),
        "rolling_archive_deleted_row_count": _normalize_int(
            rolling_summary.get("rolling_hot_delete_deleted_row_count")
        ),
        "stale_hot_missing_archive_refresh": True,
        "stale_hot_missing_archive_remaining_row_count": _normalize_int(
            rolling_summary.get("stale_hot_missing_archive_key_row_count")
        ),
        "stale_hot_missing_archive_inserted_row_count": _normalize_int(
            rolling_summary.get("stale_hot_missing_archive_inserted_row_count")
        ),
        "stale_hot_missing_archive_deleted_row_count": _normalize_int(
            rolling_summary.get("stale_hot_missing_archive_deleted_row_count")
        ),
        "rolling_archive_blocked_reason": "",
    }
    return {
        "generated_at": now_text,
        "available": True,
        "message": "NAV active binding evidence refreshed after real stale hot rows were archived and deleted in one transaction.",
        "source_active_nav_history_archive_apply_runner_artifact": str(source_active_artifact or ""),
        "source_active_nav_history_archive_apply_runner_summary": active_summary,
        "summary": summary,
        "gate_checks": [],
        "blockers": [],
        "review_items": [],
        "findings": [
            "NAV active binding evidence refreshed after stale hot rows missing cold keys were copied from the real hot DB into the cumulative cold archive and deleted from hot storage.",
        ],
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "stale_missing_archive_refresh_payload": True,
            "business_rows_from_real_hot_database": True,
            "no_synthetic_nav_values": True,
        },
    }


def build_nav_history_archive_rolling_runner_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    governance_payload: Optional[dict] = None,
    active_binding_payload: Optional[dict] = None,
    source_active_binding_artifact: Optional[str] = None,
    mode: str = "dry_run",
    page_limit: int = DEFAULT_ARCHIVE_PAGE_LIMIT,
) -> dict:
    """Run or preview cumulative rolling archive maintenance.

    The runner only deletes hot rows whose exact NAV payload is already present
    in the cumulative archive DB. It never creates a new cold DB and never runs
    VACUUM.
    """
    root = _resolve_project_root(project_root)
    requested_mode = _normalize_text(mode) or "dry_run"
    effective_page_limit = max(0, int(page_limit or 0))

    if active_binding_payload is None:
        active_artifact_path, active_binding_payload = _load_latest_active_apply_runner_payload(
            output_dir=output_dir,
            project_root=root,
        )
    else:
        active_artifact_path = _resolve_path(source_active_binding_artifact, root)
    active_binding_payload = dict(active_binding_payload or {})
    active_summary = _normalize_dict(active_binding_payload.get("summary"))
    governance_payload = dict(governance_payload or {})
    governance_summary = _normalize_dict(governance_payload.get("summary"))
    governance_strategy = _normalize_dict(
        governance_payload.get("nav_history_archive_strategy_preview")
    )

    active_cutoff_text = _normalize_text(active_summary.get("archive_cutoff_date"))
    current_cutoff_text = _normalize_text(
        governance_summary.get("archive_cutoff_date")
        or governance_strategy.get("archive_cutoff_date")
    )
    active_cutoff_date = _parse_iso_date(active_cutoff_text)
    current_cutoff_date = _parse_iso_date(current_cutoff_text)
    active_db_path = _resolve_path(
        db_path or active_summary.get("active_db_path") or "backend/data/bond_funds.db",
        root,
    )
    archive_target_path = _resolve_path(
        active_summary.get("archive_target_absolute_path")
        or active_summary.get("archive_target_path"),
        root,
    )
    archive_table_name = _normalize_table_name(
        active_summary.get("archive_table_name") or ARCHIVE_TABLE_NAME
    )
    expected_active_archive_rows = _normalize_int(
        active_summary.get("existing_archive_row_count")
        or active_summary.get("expected_archive_row_count")
    )
    expected_active_archive_funds = _normalize_int(
        active_summary.get("existing_archive_fund_count")
        or active_summary.get("expected_archive_fund_count")
    )
    active_db_exists = bool(active_db_path and active_db_path.exists() and active_db_path.is_file())
    archive_exists = bool(
        archive_target_path and archive_target_path.exists() and archive_target_path.is_file()
    )
    active_db_quick_check = _sqlite_quick_check(active_db_path) if active_db_exists else ""
    archive_quick_check = _sqlite_quick_check(archive_target_path) if archive_exists else ""

    primary_snapshot: dict = {"available": False}
    archive_snapshot: dict = {}
    match_snapshot: dict = {}
    stale_hot_snapshot: dict = {}
    stale_hot_evidence_reused = False
    if active_cutoff_date and current_cutoff_date and active_db_path and active_db_exists:
        try:
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                page_limit=effective_page_limit,
            )
        except sqlite3.Error as exc:
            primary_snapshot = {"available": False, "error": str(exc)}
    if active_cutoff_date and current_cutoff_date:
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            active_cutoff_date=active_cutoff_date,
            current_cutoff_date=current_cutoff_date,
        )
    archive_table_schema_valid = bool(archive_snapshot.get("archive_table_schema_valid"))
    if (
        active_cutoff_date
        and active_db_path
        and archive_target_path
        and active_db_exists
        and archive_exists
        and archive_table_schema_valid
    ):
        if requested_mode == "rolling_archive":
            stale_hot_snapshot = _load_reusable_stale_hot_before_active_evidence(
                output_dir=output_dir,
                project_root=root,
                active_db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_text=active_cutoff_text,
                current_hot_before_active_count=_normalize_int(
                    primary_snapshot.get("hot_before_active_cutoff_row_count"), -1
                ),
            )
            stale_hot_evidence_reused = bool(stale_hot_snapshot.get("evidence_reused"))
        if not stale_hot_snapshot:
            try:
                stale_hot_snapshot = _inspect_stale_hot_before_active_state(
                    db_path=active_db_path,
                    archive_target_path=archive_target_path,
                    archive_table_name=archive_table_name,
                    active_cutoff_date=active_cutoff_date,
                )
            except sqlite3.Error as exc:
                stale_hot_snapshot = {"inspection_error": str(exc)}
    current_cutoff_advances = bool(
        active_cutoff_date
        and current_cutoff_date
        and current_cutoff_date > active_cutoff_date
    )
    rolling_hot_candidate_rows = _normalize_int(
        primary_snapshot.get("rolling_hot_candidate_row_count")
    )
    if (
        active_cutoff_date
        and current_cutoff_date
        and current_cutoff_advances
        and rolling_hot_candidate_rows > 0
        and active_db_path
        and archive_target_path
        and active_db_exists
        and archive_exists
        and archive_table_schema_valid
    ):
        try:
            match_snapshot = _inspect_rolling_match_state(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
            )
        except sqlite3.Error as exc:
            match_snapshot = {"inspection_error": str(exc)}

    hot_before_active = _normalize_int(
        primary_snapshot.get("hot_before_active_cutoff_row_count")
    )
    hot_before_current = _normalize_int(
        primary_snapshot.get("hot_before_current_cutoff_row_count")
    )
    rolling_hot_candidate_funds = _normalize_int(
        primary_snapshot.get("rolling_hot_candidate_fund_count")
    )
    archive_before_active_rows = _normalize_int(
        archive_snapshot.get("archive_before_active_cutoff_row_count")
    )
    archive_before_active_funds = _normalize_int(
        archive_snapshot.get("archive_before_active_cutoff_fund_count")
    )
    archive_before_current_rows = _normalize_int(
        archive_snapshot.get("archive_before_current_cutoff_row_count")
    )
    archive_before_current_funds = _normalize_int(
        archive_snapshot.get("archive_before_current_cutoff_fund_count")
    )
    archive_out_of_scope_current_rows = _normalize_int(
        archive_snapshot.get("archive_out_of_scope_current_row_count")
    )
    hot_unarchived_window_rows = _normalize_int(
        match_snapshot.get("hot_unarchived_window_row_count")
    )
    exact_matched_window_rows = _normalize_int(
        match_snapshot.get("exact_matched_window_row_count")
    )
    key_conflict_window_rows = _normalize_int(
        match_snapshot.get("key_conflict_window_row_count")
    )
    stale_hot_exact_matched_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
    )
    stale_hot_missing_archive_key_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_key_row_count")
    )
    stale_hot_missing_archive_candidate_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_candidate_row_count")
    )
    stale_hot_missing_archive_unique_key_count = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_unique_key_count")
    )
    stale_hot_missing_archive_duplicate_key_count = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_count")
    )
    stale_hot_missing_archive_duplicate_key_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_row_count")
    )
    stale_hot_missing_archive_invalid_fcode_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_invalid_fcode_row_count")
    )
    stale_hot_missing_archive_invalid_nav_date_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_invalid_nav_date_row_count")
    )
    stale_hot_missing_archive_null_unit_nav_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_null_unit_nav_row_count")
    )
    stale_hot_key_conflict_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_key_conflict_row_count")
    )
    stale_hot_inspection_error = _normalize_text(stale_hot_snapshot.get("inspection_error"))
    stale_hot_repair_required = bool(hot_before_active > 0)
    stale_hot_missing_archive_supported = bool(
        hot_before_active > 0
        and stale_hot_exact_matched_rows == 0
        and stale_hot_missing_archive_key_rows > 0
        and stale_hot_missing_archive_candidate_rows == stale_hot_missing_archive_key_rows
        and stale_hot_missing_archive_unique_key_count == stale_hot_missing_archive_key_rows
        and stale_hot_missing_archive_duplicate_key_count == 0
        and stale_hot_missing_archive_duplicate_key_rows == 0
        and stale_hot_missing_archive_invalid_fcode_rows == 0
        and stale_hot_missing_archive_invalid_nav_date_rows == 0
        and stale_hot_missing_archive_null_unit_nav_rows == 0
        and stale_hot_key_conflict_rows == 0
        and not stale_hot_inspection_error
        and not stale_hot_evidence_reused
    )
    stale_hot_exact_delete_supported = bool(
        hot_before_active > 0
        and stale_hot_exact_matched_rows > 0
        and stale_hot_key_conflict_rows == 0
        and not stale_hot_inspection_error
    )
    stale_hot_repairable = bool(
        hot_before_active == 0
        or (
            hot_before_active > 0
            and (
                stale_hot_exact_matched_rows == hot_before_active
                or (
                    stale_hot_missing_archive_supported
                    and stale_hot_exact_matched_rows + stale_hot_missing_archive_key_rows
                    == hot_before_active
                )
            )
            and stale_hot_key_conflict_rows == 0
            and not stale_hot_inspection_error
        )
    )
    rolling_required = bool(
        stale_hot_repair_required
        or (
            current_cutoff_advances
            and (rolling_hot_candidate_rows > 0 or exact_matched_window_rows > 0)
        )
    )

    gates = [
        _build_gate(
            "mode",
            "执行模式",
            "ok" if requested_mode in ALLOWED_MODES else "blocked",
            f"requested_mode={requested_mode}; allowed_modes={sorted(ALLOWED_MODES)}",
            "滚动归档只支持 dry_run 和 rolling_archive。",
        ),
        _build_gate(
            "active_binding_artifact",
            "已生效绑定证据",
            "ok" if active_summary and active_summary.get("archive_binding_active") else "blocked",
            f"source_active_apply_runner_artifact={active_artifact_path or ''}",
            "滚动归档必须基于 archive_binding_active 的累计冷库绑定证据。",
        ),
        _build_gate(
            "cutoff_progress",
            "滚动 cutoff",
            "ok" if active_cutoff_date and current_cutoff_date else "blocked",
            f"active_cutoff={active_cutoff_text}, current_cutoff={current_cutoff_text}",
            "必须同时解析已生效 cutoff 和当前保留窗口 cutoff。",
        ),
        _build_gate(
            "primary_database",
            "活动主库",
            "ok" if active_db_exists and primary_snapshot.get("available") else "blocked",
            f"active_db_path={active_db_path}, error={primary_snapshot.get('error') or ''}",
            "必须能读取活动主库 fund_nav_history。",
        ),
        _build_gate(
            "archive_database",
            "累计冷库",
            "ok" if archive_exists and archive_snapshot.get("archive_table_exists") else "blocked",
            f"archive_target_path={archive_target_path}, table={archive_table_name}",
            "滚动归档只能追加到已存在的累计冷库，不能创建新小冷库替代绑定。",
        ),
        _build_gate(
            "archive_schema",
            "冷库 schema",
            "ok" if archive_table_schema_valid else "blocked",
            f"schema_valid={archive_table_schema_valid}",
            "冷库表必须包含标准 NAV 列。",
        ),
        _build_gate(
            "primary_quick_check",
            "主库 quick_check",
            "ok" if active_db_quick_check.lower() == "ok" else "blocked",
            f"quick_check={active_db_quick_check}",
            "主库 quick_check 不为 ok 时不执行滚动归档。",
        ),
        _build_gate(
            "archive_quick_check",
            "冷库 quick_check",
            "ok" if archive_quick_check.lower() == "ok" else "blocked",
            f"quick_check={archive_quick_check}",
            "冷库 quick_check 不为 ok 时不执行滚动归档。",
        ),
        _build_gate(
            "active_hot_empty",
            "已生效 cutoff 前热库回流",
            "ok" if stale_hot_repairable else "blocked",
            (
                f"hot_before_active_cutoff_row_count={hot_before_active}, "
                f"stale_exact={stale_hot_exact_matched_rows}, "
                f"stale_missing_archive_key={stale_hot_missing_archive_key_rows}, "
                f"stale_key_conflict={stale_hot_key_conflict_rows}, "
                f"inspection_error={stale_hot_inspection_error}"
            ),
            "旧 cutoff 前热库回流行可在冷库完全一致时删除；冷库缺键行仅在唯一、无冲突、关键字段完整时按真实热库行补证并同事务删除。",
        ),
        _build_gate(
            "active_archive_reconciled",
            "已生效冷库计数闭合",
            "ok"
            if (
                expected_active_archive_rows > 0
                and archive_before_active_rows == expected_active_archive_rows
                and (
                    expected_active_archive_funds <= 0
                    or archive_before_active_funds == expected_active_archive_funds
                )
            )
            else "blocked",
            (
                f"archive_before_active={archive_before_active_rows}, "
                f"expected_active_archive_rows={expected_active_archive_rows}, "
                f"archive_before_active_funds={archive_before_active_funds}, "
                f"expected_active_archive_funds={expected_active_archive_funds}"
            ),
            "旧 cutoff 前冷库计数必须与上一轮 active binding 证据一致。",
        ),
        _build_gate(
            "archive_out_of_scope_current",
            "当前 cutoff 外冷库行",
            "ok" if archive_out_of_scope_current_rows == 0 else "blocked",
            f"archive_out_of_scope_current_row_count={archive_out_of_scope_current_rows}",
            "累计冷库不得包含当前保留窗口之后的热数据。",
        ),
        _build_gate(
            "rolling_key_conflicts",
            "滚动窗口键值冲突",
            "ok" if key_conflict_window_rows == 0 else "blocked",
            f"key_conflict_window_row_count={key_conflict_window_rows}",
            "同一 fcode/nav_date 在冷库已有不同 NAV 值时必须停止并复核。",
        ),
        _build_gate(
            "rolling_page_limit",
            "滚动分页上限",
            "ok" if requested_mode != "rolling_archive" or effective_page_limit > 0 else "blocked",
            f"page_limit={effective_page_limit}",
            "rolling_archive 模式必须有正数 page_limit。",
        ),
    ]
    blockers = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "blocked"
    ]
    stale_hot_exact_delete_blockers = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "blocked" and gate.get("key") != "active_hot_empty"
    ]
    review_items: list[str] = []
    if not blockers and not current_cutoff_advances:
        review_items.append(
            f"current cutoff has not advanced: active={active_cutoff_text}, current={current_cutoff_text}"
        )

    archive_export_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "inserted_row_count": 0,
    }
    stale_hot_delete_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "stale_hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "deleted_row_count": 0,
    }
    stale_hot_missing_archive_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "stale_hot_missing_archive_sql_executed": False,
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "inserted_row_count": 0,
    }
    hot_delete_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "deleted_row_count": 0,
    }

    if (
        requested_mode == "rolling_archive"
        and not (stale_hot_exact_delete_blockers if stale_hot_exact_delete_supported else blockers)
        and (current_cutoff_advances or stale_hot_repair_required)
        and active_db_path
        and archive_target_path
        and active_cutoff_date
        and current_cutoff_date
    ):
        if stale_hot_missing_archive_supported and stale_hot_missing_archive_key_rows > 0:
            stale_hot_missing_archive_result = _execute_stale_hot_missing_archive_and_delete_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                page_limit=min(effective_page_limit, stale_hot_missing_archive_key_rows),
            )
            if stale_hot_missing_archive_result.get("status") == "failed":
                blockers.append(
                    "旧 cutoff 前热库缺冷库 key 补证写入/删除失败: "
                    + _normalize_text(
                        stale_hot_missing_archive_result.get("error") or "unknown"
                    )
                )
            elif (
                stale_hot_missing_archive_key_rows > 0
                and _normalize_int(stale_hot_missing_archive_result.get("inserted_row_count")) == 0
            ):
                blockers.append(
                    "旧 cutoff 前热库缺冷库 key 补证未写入任何行，当前证据可能已过期。"
                )
            if not blockers:
                primary_snapshot = _inspect_primary_db(
                    db_path=active_db_path,
                    active_cutoff_date=active_cutoff_date,
                    current_cutoff_date=current_cutoff_date,
                    page_limit=effective_page_limit,
                )
                archive_snapshot = _inspect_archive_db(
                    archive_target_path=archive_target_path,
                    archive_table_name=archive_table_name,
                    active_cutoff_date=active_cutoff_date,
                    current_cutoff_date=current_cutoff_date,
                )
                stale_hot_snapshot = _inspect_stale_hot_before_active_state(
                    db_path=active_db_path,
                    archive_target_path=archive_target_path,
                    archive_table_name=archive_table_name,
                    active_cutoff_date=active_cutoff_date,
                )
                hot_before_active = _normalize_int(
                    primary_snapshot.get("hot_before_active_cutoff_row_count")
                )
                archive_before_active_rows = _normalize_int(
                    archive_snapshot.get("archive_before_active_cutoff_row_count")
                )
                archive_before_active_funds = _normalize_int(
                    archive_snapshot.get("archive_before_active_cutoff_fund_count")
                )
                archive_before_current_rows = _normalize_int(
                    archive_snapshot.get("archive_before_current_cutoff_row_count")
                )
                archive_before_current_funds = _normalize_int(
                    archive_snapshot.get("archive_before_current_cutoff_fund_count")
                )
                archive_out_of_scope_current_rows = _normalize_int(
                    archive_snapshot.get("archive_out_of_scope_current_row_count")
                )
                stale_hot_exact_matched_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
                )
                stale_hot_missing_archive_key_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_key_row_count")
                )
                stale_hot_missing_archive_candidate_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_candidate_row_count")
                )
                stale_hot_missing_archive_unique_key_count = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_unique_key_count")
                )
                stale_hot_missing_archive_duplicate_key_count = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_count")
                )
                stale_hot_missing_archive_duplicate_key_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_row_count")
                )
                stale_hot_missing_archive_invalid_fcode_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_invalid_fcode_row_count")
                )
                stale_hot_missing_archive_invalid_nav_date_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_invalid_nav_date_row_count")
                )
                stale_hot_missing_archive_null_unit_nav_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_null_unit_nav_row_count")
                )
                stale_hot_key_conflict_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_key_conflict_row_count")
                )
                if stale_hot_key_conflict_rows:
                    blockers.append(
                        f"旧 cutoff 前热库补证后出现 {stale_hot_key_conflict_rows} 行同键数值冲突。"
                    )
        if stale_hot_exact_matched_rows > 0:
            stale_hot_delete_result = _execute_stale_hot_before_active_delete_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                page_limit=min(effective_page_limit, stale_hot_exact_matched_rows),
            )
            if stale_hot_delete_result.get("status") == "failed":
                blockers.append(
                    "旧 cutoff 前热库回流删除失败: "
                    + _normalize_text(stale_hot_delete_result.get("error") or "unknown")
                )
            deleted_stale_hot_rows = _normalize_int(
                stale_hot_delete_result.get("deleted_row_count")
            )
            if stale_hot_evidence_reused and deleted_stale_hot_rows > 0:
                stale_hot_snapshot = {
                    **stale_hot_snapshot,
                    "stale_hot_exact_matched_row_count": max(
                        stale_hot_exact_matched_rows - deleted_stale_hot_rows,
                        0,
                    ),
                    "post_delete_estimated_from_reused_evidence": True,
                    "post_delete_deleted_row_count": deleted_stale_hot_rows,
                }
                stale_hot_exact_matched_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
                )
                stale_hot_rows_after_delete = _normalize_int(
                    stale_hot_delete_result.get("stale_hot_rows_after")
                )
                if stale_hot_rows_after_delete >= 0:
                    hot_before_active = stale_hot_rows_after_delete
                    active_hot_empty_detail = (
                        "已生效 cutoff 前热库回流: "
                        f"hot_before_active_cutoff_row_count={hot_before_active}, "
                        f"stale_exact_post_delete_estimated={stale_hot_exact_matched_rows}, "
                        f"stale_missing_archive_key={stale_hot_missing_archive_key_rows}, "
                        f"stale_key_conflict={stale_hot_key_conflict_rows}, "
                        "inspection_source=reused_evidence_post_delete_estimate"
                    )
                    blockers = [
                        active_hot_empty_detail
                        if item.startswith("已生效 cutoff 前热库回流:")
                        else item
                        for item in blockers
                    ]
            if not blockers:
                primary_snapshot = _inspect_primary_db(
                    db_path=active_db_path,
                    active_cutoff_date=active_cutoff_date,
                    current_cutoff_date=current_cutoff_date,
                    page_limit=effective_page_limit,
                )
                if stale_hot_evidence_reused:
                    stale_hot_snapshot = {
                        **stale_hot_snapshot,
                        "post_delete_estimated_from_reused_evidence": True,
                        "post_delete_deleted_row_count": deleted_stale_hot_rows,
                    }
                else:
                    stale_hot_snapshot = _inspect_stale_hot_before_active_state(
                        db_path=active_db_path,
                        archive_target_path=archive_target_path,
                        archive_table_name=archive_table_name,
                        active_cutoff_date=active_cutoff_date,
                    )
                hot_before_active = _normalize_int(
                    primary_snapshot.get("hot_before_active_cutoff_row_count")
                )
                stale_hot_rows_after_delete = _normalize_int(
                    stale_hot_delete_result.get("stale_hot_rows_after"), -1
                )
                if stale_hot_evidence_reused and stale_hot_rows_after_delete >= 0:
                    hot_before_active = stale_hot_rows_after_delete
                stale_hot_exact_matched_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
                )
                stale_hot_missing_archive_key_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_key_row_count")
                )
                stale_hot_missing_archive_candidate_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_candidate_row_count")
                )
                stale_hot_missing_archive_unique_key_count = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_unique_key_count")
                )
                stale_hot_missing_archive_duplicate_key_count = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_count")
                )
                stale_hot_missing_archive_duplicate_key_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_duplicate_key_row_count")
                )
                stale_hot_missing_archive_invalid_fcode_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_invalid_fcode_row_count")
                )
                stale_hot_missing_archive_invalid_nav_date_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_invalid_nav_date_row_count")
                )
                stale_hot_missing_archive_null_unit_nav_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_null_unit_nav_row_count")
                )
                stale_hot_key_conflict_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_key_conflict_row_count")
                )
                if stale_hot_missing_archive_key_rows or stale_hot_key_conflict_rows:
                    blockers.append(
                        "旧 cutoff 前热库回流存在冷库缺失或同键数值冲突。"
                    )
                elif stale_hot_evidence_reused and (
                    stale_hot_exact_matched_rows > 0
                    and _normalize_int(stale_hot_delete_result.get("deleted_row_count")) == 0
                ):
                    blockers.append(
                        "旧 cutoff 前热库回流复用证据已过期：本页未删除任何 exact match 行。"
                    )
        if not blockers and hot_before_active == 0 and hot_unarchived_window_rows > 0:
            archive_export_result = _execute_rolling_archive_export_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                page_limit=min(effective_page_limit, hot_unarchived_window_rows),
            )
            if archive_export_result.get("status") == "failed":
                blockers.append(
                    "滚动归档写入失败: "
                    + _normalize_text(archive_export_result.get("error") or "unknown")
                )
        if not blockers:
            archive_snapshot = _inspect_archive_db(
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
            )
            match_snapshot = _inspect_rolling_match_state(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
            )
            exact_matched_window_rows = _normalize_int(
                match_snapshot.get("exact_matched_window_row_count")
            )
            key_conflict_window_rows = _normalize_int(
                match_snapshot.get("key_conflict_window_row_count")
            )
            if key_conflict_window_rows > 0:
                blockers.append(
                    f"滚动窗口存在 {key_conflict_window_rows} 行 fcode/nav_date 键值冲突。"
                )
        if not blockers and hot_before_active == 0 and exact_matched_window_rows > 0:
            hot_delete_result = _execute_rolling_hot_delete_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                page_limit=min(effective_page_limit, exact_matched_window_rows),
            )
            if hot_delete_result.get("status") == "failed":
                blockers.append(
                    "滚动热库删除失败: "
                    + _normalize_text(hot_delete_result.get("error") or "unknown")
                )
        if not blockers:
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                page_limit=effective_page_limit,
            )
            archive_snapshot = _inspect_archive_db(
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
            )
            match_snapshot = _inspect_rolling_match_state(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
            )

    post_hot_before_current = _normalize_int(
        primary_snapshot.get("hot_before_current_cutoff_row_count")
    )
    post_hot_before_active = _normalize_int(
        primary_snapshot.get("hot_before_active_cutoff_row_count")
    )
    if stale_hot_evidence_reused:
        stale_hot_rows_after_delete = _normalize_int(
            stale_hot_delete_result.get("stale_hot_rows_after"), -1
        )
        if stale_hot_rows_after_delete >= 0:
            post_hot_before_active = stale_hot_rows_after_delete
    post_rolling_hot_candidate_rows = _normalize_int(
        primary_snapshot.get("rolling_hot_candidate_row_count")
    )
    post_archive_before_current_rows = _normalize_int(
        archive_snapshot.get("archive_before_current_cutoff_row_count")
    )
    post_archive_before_current_funds = _normalize_int(
        archive_snapshot.get("archive_before_current_cutoff_fund_count")
    )
    post_hot_unarchived_window_rows = _normalize_int(
        match_snapshot.get("hot_unarchived_window_row_count")
    )
    post_exact_matched_window_rows = _normalize_int(
        match_snapshot.get("exact_matched_window_row_count")
    )

    updated_active_binding_payload: dict = {}
    if blockers:
        runner_status = "rolling_archive_blocked"
        auto_governance_stage = "rolling_archive_blocked"
    elif (
        requested_mode == "dry_run"
        and rolling_required
    ):
        runner_status = "rolling_archive_ready"
        auto_governance_stage = "rolling_archive_ready"
    elif requested_mode == "dry_run":
        runner_status = "rolling_archive_not_required"
        auto_governance_stage = "hot_cold_binding_active"
    elif (
        (current_cutoff_advances or stale_hot_repair_required)
        and post_hot_before_current == 0
        and current_cutoff_date is not None
    ):
        runner_status = "rolling_archive_complete_binding_ready"
        auto_governance_stage = "rolling_archive_complete_binding_ready"
    elif not current_cutoff_advances:
        runner_status = "rolling_archive_not_required"
        auto_governance_stage = "hot_cold_binding_active"
    elif (
        bool(archive_export_result.get("archive_sql_executed"))
        or bool(stale_hot_missing_archive_result.get("archive_sql_executed"))
        or bool(stale_hot_missing_archive_result.get("delete_sql_executed"))
        or bool(stale_hot_delete_result.get("stale_hot_delete_sql_executed"))
        or bool(hot_delete_result.get("hot_delete_sql_executed"))
    ):
        runner_status = "rolling_archive_in_progress"
        auto_governance_stage = "rolling_archive_in_progress"
    else:
        runner_status = "rolling_archive_not_required"
        auto_governance_stage = "hot_cold_binding_active"

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "dry_run_only": requested_mode == "dry_run",
        "dry_run_safe": not blockers,
        "rolling_archive_supported": True,
        "rolling_archive_required": rolling_required,
        "rolling_archive_executed": bool(
            archive_export_result.get("archive_sql_executed")
            or stale_hot_missing_archive_result.get("archive_sql_executed")
            or stale_hot_missing_archive_result.get("delete_sql_executed")
            or stale_hot_delete_result.get("stale_hot_delete_sql_executed")
            or hot_delete_result.get("hot_delete_sql_executed")
        ),
        "archive_sql_executed": bool(
            archive_export_result.get("archive_sql_executed")
            or stale_hot_missing_archive_result.get("archive_sql_executed")
        ),
        "delete_sql_executed": bool(
            stale_hot_missing_archive_result.get("delete_sql_executed")
            or
            stale_hot_delete_result.get("delete_sql_executed")
            or hot_delete_result.get("delete_sql_executed")
        ),
        "vacuum_executed": False,
        "active_cutoff_date": active_cutoff_text,
        "rolling_archive_cutoff_date": current_cutoff_text,
        "archive_cutoff_date": current_cutoff_text if runner_status == "rolling_archive_complete_binding_ready" else active_cutoff_text,
        "current_cutoff_advances": current_cutoff_advances,
        "active_db_path": _stringify_project_path(active_db_path, root),
        "active_db_absolute_path": str(active_db_path or ""),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "archive_table_name": archive_table_name,
        "active_db_quick_check": active_db_quick_check,
        "archive_quick_check": archive_quick_check,
        "page_limit": effective_page_limit,
        "expected_active_archive_row_count": expected_active_archive_rows,
        "expected_active_archive_fund_count": expected_active_archive_funds,
        "hot_before_active_cutoff_row_count": post_hot_before_active,
        "stale_hot_repair_required": stale_hot_repair_required,
        "stale_hot_exact_delete_supported": stale_hot_exact_delete_supported,
        "stale_hot_missing_archive_supported": stale_hot_missing_archive_supported,
        "stale_hot_evidence_reused": stale_hot_evidence_reused,
        "stale_hot_evidence_source_artifact": _normalize_text(
            stale_hot_snapshot.get("evidence_source_artifact")
        ),
        "stale_hot_evidence_scope": _normalize_text(
            stale_hot_snapshot.get("evidence_scope")
        ),
        "stale_hot_evidence_truth_policy": _normalize_text(
            stale_hot_snapshot.get("evidence_truth_policy")
        ),
        "stale_hot_post_delete_estimated_from_reused_evidence": bool(
            stale_hot_snapshot.get("post_delete_estimated_from_reused_evidence")
        ),
        "stale_hot_exact_matched_row_count": stale_hot_exact_matched_rows,
        "stale_hot_missing_archive_key_row_count": stale_hot_missing_archive_key_rows,
        "stale_hot_missing_archive_candidate_row_count": stale_hot_missing_archive_candidate_rows,
        "stale_hot_missing_archive_unique_key_count": stale_hot_missing_archive_unique_key_count,
        "stale_hot_missing_archive_duplicate_key_count": stale_hot_missing_archive_duplicate_key_count,
        "stale_hot_missing_archive_duplicate_key_row_count": stale_hot_missing_archive_duplicate_key_rows,
        "stale_hot_missing_archive_invalid_fcode_row_count": stale_hot_missing_archive_invalid_fcode_rows,
        "stale_hot_missing_archive_invalid_nav_date_row_count": stale_hot_missing_archive_invalid_nav_date_rows,
        "stale_hot_missing_archive_null_unit_nav_row_count": stale_hot_missing_archive_null_unit_nav_rows,
        "stale_hot_key_conflict_row_count": stale_hot_key_conflict_rows,
        "stale_hot_missing_archive_result_status": stale_hot_missing_archive_result.get("status") or "",
        "stale_hot_missing_archive_selected_row_count": _normalize_int(
            stale_hot_missing_archive_result.get("selected_row_count")
        ),
        "stale_hot_missing_archive_inserted_row_count": _normalize_int(
            stale_hot_missing_archive_result.get("inserted_row_count")
        ),
        "stale_hot_missing_archive_deleted_row_count": _normalize_int(
            stale_hot_missing_archive_result.get("deleted_row_count")
        ),
        "stale_hot_delete_result_status": stale_hot_delete_result.get("status") or "",
        "stale_hot_delete_deleted_row_count": _normalize_int(
            stale_hot_delete_result.get("deleted_row_count")
        ),
        "hot_before_current_cutoff_row_count": post_hot_before_current,
        "rolling_archive_candidate_row_count": rolling_hot_candidate_rows,
        "rolling_archive_candidate_fund_count": rolling_hot_candidate_funds,
        "rolling_hot_candidate_row_count_after": post_rolling_hot_candidate_rows,
        "archive_before_active_cutoff_row_count": archive_before_active_rows,
        "archive_before_active_cutoff_fund_count": archive_before_active_funds,
        "archive_before_current_cutoff_row_count": post_archive_before_current_rows,
        "archive_before_current_cutoff_fund_count": post_archive_before_current_funds,
        "archive_out_of_scope_current_row_count": archive_out_of_scope_current_rows,
        "rolling_archive_window_row_count": _normalize_int(
            archive_snapshot.get("rolling_archive_window_row_count")
        ),
        "hot_unarchived_window_row_count": post_hot_unarchived_window_rows,
        "exact_matched_window_row_count": post_exact_matched_window_rows,
        "key_conflict_window_row_count": key_conflict_window_rows,
        "rolling_archive_result_status": archive_export_result.get("status") or "",
        "rolling_archive_inserted_row_count": _normalize_int(
            archive_export_result.get("inserted_row_count")
        ),
        "rolling_hot_delete_result_status": hot_delete_result.get("status") or "",
        "rolling_hot_delete_deleted_row_count": _normalize_int(
            hot_delete_result.get("deleted_row_count")
        ),
        "updated_active_binding_ready": runner_status == "rolling_archive_complete_binding_ready",
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "source_active_apply_runner_artifact": str(active_artifact_path or ""),
    }

    if (
        runner_status == "rolling_archive_complete_binding_ready"
        and active_db_path
        and archive_target_path
        and current_cutoff_date
    ):
        updated_active_binding_payload = _build_updated_active_binding_payload(
            root=root,
            source_active_artifact=active_artifact_path,
            active_summary=active_summary,
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            current_cutoff_date=current_cutoff_date,
            primary_snapshot=primary_snapshot,
            archive_snapshot=archive_snapshot,
            rolling_summary=summary,
        )
    elif (
        stale_hot_missing_archive_result.get("status") == "executed"
        and active_db_path
        and archive_target_path
        and active_cutoff_date
        and not blockers
    ):
        updated_active_binding_payload = _build_refreshed_active_binding_payload(
            root=root,
            source_active_artifact=active_artifact_path,
            active_summary=active_summary,
            db_path=active_db_path,
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            active_cutoff_date=active_cutoff_date,
            primary_snapshot=primary_snapshot,
            archive_snapshot=archive_snapshot,
            rolling_summary=summary,
        )

    findings: list[str] = []
    if blockers and stale_hot_delete_result.get("stale_hot_delete_sql_executed"):
        findings.append(
            "NAV rolling archive deleted exact stale hot rows, but unresolved stale hot rows remain blocked."
        )
        findings.extend(blockers[:5])
    elif (
        stale_hot_missing_archive_result.get("status") == "executed"
        and runner_status == "rolling_archive_in_progress"
    ):
        findings.append(
            "NAV rolling archive archived real stale hot rows missing cold keys and deleted the same rows from hot storage in one transaction."
        )
        findings.append(
            f"本页补证 {summary.get('stale_hot_missing_archive_inserted_row_count', 0)} 行，删除 {summary.get('stale_hot_missing_archive_deleted_row_count', 0)} 行；旧 cutoff 前仍剩 {post_hot_before_active} 行。"
        )
    elif blockers:
        findings.append(f"NAV rolling archive blocked with {len(blockers)} blockers.")
        findings.extend(blockers[:5])
    elif runner_status == "rolling_archive_ready":
        findings.append(
            f"NAV rolling archive is ready for {rolling_hot_candidate_rows} hot rows before {current_cutoff_text}."
        )
    elif runner_status == "rolling_archive_complete_binding_ready":
        findings.append(
            "NAV rolling archive completed; hot rows before the current cutoff are now zero and the active binding can be advanced."
        )
    elif runner_status == "rolling_archive_in_progress":
        findings.append(
            f"NAV rolling archive advanced one page; {post_hot_before_current} hot rows remain before {current_cutoff_text}."
        )
    else:
        findings.append("NAV rolling archive is not required for the current retention cutoff.")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(active_summary),
        "summary": summary,
        "source_active_nav_history_archive_apply_runner_artifact": str(active_artifact_path or ""),
        "source_active_nav_history_archive_apply_runner_summary": active_summary,
        "blockers": blockers,
        "review_items": review_items,
        "gate_checks": gates,
        "primary_snapshot": primary_snapshot,
        "archive_snapshot": archive_snapshot,
        "match_snapshot": match_snapshot,
        "stale_hot_evidence_snapshot": stale_hot_snapshot if stale_hot_evidence_reused else {},
        "archive_export_result": archive_export_result,
        "stale_hot_missing_archive_result": stale_hot_missing_archive_result,
        "stale_hot_delete_result": stale_hot_delete_result,
        "hot_delete_result": hot_delete_result,
        "updated_active_binding_payload": updated_active_binding_payload,
        "findings": findings,
        "safety": {
            "archive_sql_executed": bool(
                archive_export_result.get("archive_sql_executed")
                or stale_hot_missing_archive_result.get("archive_sql_executed")
            ),
            "delete_sql_executed": bool(
                stale_hot_missing_archive_result.get("delete_sql_executed")
                or stale_hot_delete_result.get("delete_sql_executed")
                or hot_delete_result.get("delete_sql_executed")
            ),
            "vacuum_executed": False,
            "delete_requires_exact_archive_match": True,
            "stale_hot_delete_requires_exact_archive_match": True,
            "stale_hot_missing_archive_uses_real_hot_rows": True,
            "stale_hot_missing_archive_single_transaction": True,
            "stale_hot_missing_archive_no_synthetic_nav_values": True,
            "archive_target_is_existing_cumulative_binding": archive_exists,
        },
    }


def render_nav_history_archive_rolling_runner_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive rolling runner",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- runner_status: {summary.get('runner_status', '')}",
        f"- auto_governance_stage: {summary.get('auto_governance_stage', '')}",
        f"- mode: {summary.get('mode', '')}",
        f"- active_cutoff_date: {summary.get('active_cutoff_date', '')}",
        f"- rolling_archive_cutoff_date: {summary.get('rolling_archive_cutoff_date', '')}",
        f"- rolling_archive_candidate_row_count: {summary.get('rolling_archive_candidate_row_count', 0)}",
        f"- rolling_archive_inserted_row_count: {summary.get('rolling_archive_inserted_row_count', 0)}",
        f"- rolling_hot_delete_deleted_row_count: {summary.get('rolling_hot_delete_deleted_row_count', 0)}",
        f"- hot_before_current_cutoff_row_count: {summary.get('hot_before_current_cutoff_row_count', 0)}",
        f"- archive_sql_executed: {summary.get('archive_sql_executed', False)}",
        f"- delete_sql_executed: {summary.get('delete_sql_executed', False)}",
        f"- vacuum_executed: {summary.get('vacuum_executed', False)}",
        f"- updated_active_binding_ready: {summary.get('updated_active_binding_ready', False)}",
        f"- blocker_count: {summary.get('blocker_count', 0)}",
        "",
        "## blockers",
    ]
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## review_items"])
    for item in payload.get("review_items") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_rolling_runner_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"nav_history_archive_rolling_runner_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_rolling_runner_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_rolling_runner_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
