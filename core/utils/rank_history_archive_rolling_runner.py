#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rolling fund_rank_history hot/cold archive runner for an active binding."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.rank_history_archive_apply_runner import (
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    DEFAULT_OUTPUT_DIR,
    RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
    write_rank_history_archive_apply_runner_artifacts,
)
from utils.rank_archive_checkpoint import (
    binding_fingerprint, read_checkpoint, record_checkpoint, scoped_counts,
)
from utils.rank_history_archive_preflight import (
    ARCHIVE_COLUMNS,
    ARCHIVE_TABLE_NAME,
    REQUIRED_HOT_COLUMNS,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RANK_HISTORY_ARCHIVE_ROLLING_RUNNER_JSON_PATTERN = (
    "rank_history_archive_rolling_runner_*.json"
)
ALLOWED_MODES = {"dry_run", "rolling_archive"}
ROLLING_HOT_DB_ALIAS = "rolling_rank_hot_db"
ROLLING_ARCHIVE_DB_ALIAS = "rolling_rank_archive_db"
DEFAULT_RECOMMENDED_RETENTION_YEARS = 2
STALE_HOT_REPAIR_PAGE_LIMIT = DEFAULT_ARCHIVE_PAGE_LIMIT


def load_latest_rank_history_archive_rolling_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        RANK_HISTORY_ARCHIVE_ROLLING_RUNNER_JSON_PATTERN,
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


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - int(years))
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - int(years))


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
        return _normalize_text(row[0] if row else "")
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _normalize_table_name(value: Any) -> str:
    text = _normalize_text(value) or ARCHIVE_TABLE_NAME
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text):
        return ARCHIVE_TABLE_NAME
    return text


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


def _build_select_columns(columns: set[str]) -> list[str]:
    return [column for column in ARCHIVE_COLUMNS if column in columns]


def _null_safe_equal(left: str, right: str) -> str:
    return f"(({left} = {right}) OR ({left} IS NULL AND {right} IS NULL))"


def _require_iso_rank_dates(
    conn: sqlite3.Connection,
    table: str,
    where: str = "1 = 1",
    params: Iterable[Any] = (),
) -> None:
    """Reject ambiguous keys before using the cold TEXT date index.

    DATE/NUMERIC affinity otherwise converts numeric-looking cold keys during
    equality. Only canonical ISO text dates have the same equality under both
    affinities. Validate the selected hot rows, within the write transaction
    for mutations; never normalize or delete malformed dates implicitly.
    All SQL fragments here are internal table names and predicates.
    """
    for row in conn.execute(f"SELECT DISTINCT rank_date FROM {table} WHERE {where}", tuple(params)):
        value = row[0]
        try:
            valid = isinstance(value, str) and date.fromisoformat(value).isoformat() == value
        except ValueError:
            valid = False
        if not valid:
            raise sqlite3.IntegrityError(f"invalid_rank_date_for_text_archive_match:{value!r}")


def _rank_exact_match_sql(hot_alias: str, archive_alias: str, columns: list[str]) -> str:
    predicates = [
        f"{archive_alias}.fcode = {hot_alias}.fcode",
        f"{archive_alias}.rank_date = CAST({hot_alias}.rank_date AS TEXT)",
        f"{archive_alias}.period IS {hot_alias}.period",
    ]
    for column in columns:
        if column in {"fcode", "rank_date", "period"}:
            continue
        if column in set(REQUIRED_HOT_COLUMNS):
            predicates.append(_null_safe_equal(f"{archive_alias}.{column}", f"{hot_alias}.{column}"))
    return " AND ".join(predicates)


def _rank_key_match_sql(hot_alias: str, archive_alias: str) -> str:
    return (
        f"{archive_alias}.fcode = {hot_alias}.fcode "
        f"AND {archive_alias}.rank_date = CAST({hot_alias}.rank_date AS TEXT) "
        f"AND COALESCE({archive_alias}.period, '') = COALESCE({hot_alias}.period, '')"
    )


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
    if "created_at" not in _table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN created_at TEXT")
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_key
        ON {table_name} (fcode, rank_date, COALESCE(period, ''))
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
    return set(REQUIRED_HOT_COLUMNS).issubset(_table_columns(conn, table_name, schema=schema))


def _load_latest_active_apply_runner_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Optional[Path], dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    try:
        candidates = sorted(resolved_output_dir.glob(RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN))
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


def _infer_current_cutoff_from_primary_db(
    *,
    db_path: Path,
    retention_years: int = DEFAULT_RECOMMENDED_RETENTION_YEARS,
) -> dict:
    if not db_path.exists() or not db_path.is_file():
        return {
            "archive_cutoff_date": "",
            "latest_rank_date": "",
            "status": "primary_database_missing",
        }
    try:
        with _open_readonly_db(db_path) as conn:
            latest_rank_date = _normalize_text(
                conn.execute("SELECT MAX(rank_date) FROM fund_rank_history").fetchone()[0]
            )
    except sqlite3.Error as exc:
        return {
            "archive_cutoff_date": "",
            "latest_rank_date": "",
            "status": "failed",
            "error": str(exc),
        }
    latest = _parse_iso_date(latest_rank_date)
    if latest is None:
        return {
            "archive_cutoff_date": "",
            "latest_rank_date": latest_rank_date,
            "status": "latest_rank_date_missing",
        }
    cutoff = _subtract_years(latest, retention_years)
    return {
        "archive_cutoff_date": cutoff.isoformat(),
        "latest_rank_date": latest.isoformat(),
        "retention_years": int(retention_years),
        "status": "inferred_from_latest_rank_date",
    }


def _inspect_primary_window_state(
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
        columns = _table_columns(conn, "fund_rank_history")
        if not set(REQUIRED_HOT_COLUMNS).issubset(columns):
            return {
                "available": False,
                "error": "fund_rank_history required columns missing",
                "columns": sorted(columns),
            }
        before_active = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_active_cutoff_row_count,
                MIN(rank_date) AS hot_before_active_cutoff_min_rank_date,
                MAX(rank_date) AS hot_before_active_cutoff_max_rank_date
            FROM fund_rank_history
            WHERE rank_date < ?
            """,
            (active_cutoff,),
        )
        before_current = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_current_cutoff_row_count,
                MIN(rank_date) AS hot_before_current_cutoff_min_rank_date,
                MAX(rank_date) AS hot_before_current_cutoff_max_rank_date
            FROM fund_rank_history
            WHERE rank_date < ?
            """,
            (current_cutoff,),
        )
        rolling = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS rolling_hot_candidate_row_count,
                COUNT(DISTINCT fcode) AS rolling_hot_candidate_fund_count,
                MIN(rank_date) AS rolling_hot_candidate_min_rank_date,
                MAX(rank_date) AS rolling_hot_candidate_max_rank_date
            FROM fund_rank_history
            WHERE rank_date >= ? AND rank_date < ?
            """,
            (active_cutoff, current_cutoff),
        )
        rolling_rows = _normalize_int(rolling.get("rolling_hot_candidate_row_count"))
        duplicate_keys = 0
        sample_rows: list[dict] = []
        if rolling_rows > 0:
            duplicate_keys = _normalize_int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT fcode, rank_date, COALESCE(period, '') AS period_key, COUNT(*) AS c
                        FROM fund_rank_history
                        WHERE rank_date >= ? AND rank_date < ?
                        GROUP BY fcode, rank_date, COALESCE(period, '')
                        HAVING c > 1
                    )
                    """,
                    (active_cutoff, current_cutoff),
                ).fetchone()[0]
            )
            sample_rows = _fetch_all_dicts(
                conn,
                """
                SELECT fcode, rank_date, period, rank_position, category_total, percentile, created_at
                FROM fund_rank_history
                WHERE rank_date >= ? AND rank_date < ?
                ORDER BY rank_date, fcode, COALESCE(period, '')
                LIMIT ?
                """,
                (active_cutoff, current_cutoff, min(max(1, int(page_limit or 1)), 20)),
            )
        page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "available": True,
        "inspection_mode": "window_state",
        "columns": sorted(columns),
        "selected_archive_columns": _build_select_columns(columns),
        "total_rank_row_count": 0,
        "total_rank_fund_count": 0,
        "min_rank_date": "",
        "max_rank_date": "",
        "hot_before_active_cutoff_fund_count": 0,
        "hot_before_current_cutoff_fund_count": 0,
        "duplicate_rolling_business_key_count": duplicate_keys,
        **before_active,
        **before_current,
        **rolling,
        "sample_rolling_rows": sample_rows,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "database_size_mb": _round_mb(db_path.stat().st_size),
    }


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
        before_active = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_active_cutoff_row_count,
                COUNT(DISTINCT fcode) AS hot_before_active_cutoff_fund_count,
                MIN(rank_date) AS hot_before_active_cutoff_min_rank_date,
                MAX(rank_date) AS hot_before_active_cutoff_max_rank_date
            FROM fund_rank_history
            WHERE rank_date < ?
            """,
            (active_cutoff,),
        )
        before_current = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_before_current_cutoff_row_count,
                COUNT(DISTINCT fcode) AS hot_before_current_cutoff_fund_count,
                MIN(rank_date) AS hot_before_current_cutoff_min_rank_date,
                MAX(rank_date) AS hot_before_current_cutoff_max_rank_date
            FROM fund_rank_history
            WHERE rank_date < ?
            """,
            (current_cutoff,),
        )
        rolling = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS rolling_hot_candidate_row_count,
                COUNT(DISTINCT fcode) AS rolling_hot_candidate_fund_count,
                MIN(rank_date) AS rolling_hot_candidate_min_rank_date,
                MAX(rank_date) AS rolling_hot_candidate_max_rank_date
            FROM fund_rank_history
            WHERE rank_date >= ? AND rank_date < ?
            """,
            (active_cutoff, current_cutoff),
        )
        duplicate_keys = _normalize_int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT fcode, rank_date, COALESCE(period, '') AS period_key, COUNT(*) AS c
                    FROM fund_rank_history
                    WHERE rank_date >= ? AND rank_date < ?
                    GROUP BY fcode, rank_date, COALESCE(period, '')
                    HAVING c > 1
                )
                """,
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        sample_rows = _fetch_all_dicts(
            conn,
            """
            SELECT fcode, rank_date, period, rank_position, category_total, percentile, created_at
            FROM fund_rank_history
            WHERE rank_date >= ? AND rank_date < ?
            ORDER BY rank_date, fcode, COALESCE(period, '')
            LIMIT ?
            """,
            (active_cutoff, current_cutoff, min(max(1, int(page_limit or 1)), 20)),
        )
        page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "available": True,
        "columns": sorted(columns),
        "selected_archive_columns": _build_select_columns(columns),
        "duplicate_rolling_business_key_count": duplicate_keys,
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
            payload.update(
                _fetch_one_dict(
                    conn,
                    f"""
                    SELECT
                        COUNT(*) AS archive_before_active_cutoff_row_count,
                        COUNT(DISTINCT fcode) AS archive_before_active_cutoff_fund_count,
                        MIN(rank_date) AS archive_before_active_cutoff_min_rank_date,
                        MAX(rank_date) AS archive_before_active_cutoff_max_rank_date
                    FROM {table_name}
                    WHERE rank_date < ?
                    """,
                    (active_cutoff,),
                )
            )
            payload.update(
                _fetch_one_dict(
                    conn,
                    f"""
                    SELECT
                        COUNT(*) AS archive_before_current_cutoff_row_count,
                        COUNT(DISTINCT fcode) AS archive_before_current_cutoff_fund_count,
                        MIN(rank_date) AS archive_before_current_cutoff_min_rank_date,
                        MAX(rank_date) AS archive_before_current_cutoff_max_rank_date
                    FROM {table_name}
                    WHERE rank_date < ?
                    """,
                    (current_cutoff,),
                )
            )
            payload.update(
                _fetch_one_dict(
                    conn,
                    f"""
                    SELECT
                        COUNT(*) AS rolling_archive_window_row_count,
                        COUNT(DISTINCT fcode) AS rolling_archive_window_fund_count,
                        MIN(rank_date) AS rolling_archive_window_min_rank_date,
                        MAX(rank_date) AS rolling_archive_window_max_rank_date
                    FROM {table_name}
                    WHERE rank_date >= ? AND rank_date < ?
                    """,
                    (active_cutoff, current_cutoff),
                )
            )
            payload.update(
                _fetch_one_dict(
                    conn,
                    f"SELECT COUNT(*) AS archive_out_of_scope_current_row_count FROM {table_name} WHERE rank_date >= ?",
                    (current_cutoff,),
                )
            )
            payload.update(
                _fetch_one_dict(
                    conn,
                    f"SELECT COUNT(*) AS archive_total_row_count FROM {table_name}",
                )
            )
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
    columns: list[str],
) -> dict:
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _rank_exact_match_sql("hot", "archived", columns)
    key_match = _rank_key_match_sql("hot", "archived")
    with sqlite3.connect(_sqlite_readonly_uri(db_path), timeout=120, isolation_level=None, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        try:
            _require_iso_rank_dates(
                conn, "fund_rank_history", "rank_date >= ? AND rank_date < ?",
                (active_cutoff, current_cutoff),
            )
            hot_unarchived = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS hot_unarchived_window_row_count
                FROM fund_rank_history AS hot
                WHERE hot.rank_date >= ? AND hot.rank_date < ?
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
                FROM fund_rank_history AS hot
                WHERE hot.rank_date >= ? AND hot.rank_date < ?
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
                FROM fund_rank_history AS hot
                INNER JOIN {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                    ON {key_match}
                WHERE hot.rank_date >= ? AND hot.rank_date < ?
                  AND NOT ({exact_match})
                """,
                (active_cutoff, current_cutoff),
            )
        finally:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
    return {**hot_unarchived, **exact_matched, **key_conflict}


def _inspect_stale_hot_before_active_state(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    columns: list[str],
    page_limit: int,
) -> dict:
    effective_page_limit = min(
        max(1, int(page_limit or 1)),
        STALE_HOT_REPAIR_PAGE_LIMIT,
    )
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _rank_exact_match_sql("hot", "archived", columns)
    key_match = _rank_key_match_sql("hot", "archived")
    with sqlite3.connect(_sqlite_readonly_uri(db_path), timeout=120, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_hot")
            conn.execute(
                """
                CREATE TEMP TABLE selected_stale_rank_hot AS
                SELECT
                    hot.rowid AS hot_rowid,
                    hot.fcode,
                    hot.rank_date,
                    hot.period,
                    hot.rank_position,
                    hot.category_total,
                    hot.percentile,
                    hot.created_at
                FROM fund_rank_history AS hot
                WHERE hot.rank_date < ?
                ORDER BY hot.rank_date, hot.rowid
                LIMIT ?
                """,
                (active_cutoff, effective_page_limit),
            )
            _require_iso_rank_dates(conn, "temp.selected_stale_rank_hot")
            selected = _fetch_one_dict(
                conn,
                """
                SELECT
                    COUNT(*) AS stale_hot_inspected_row_count,
                    MIN(rank_date) AS stale_hot_inspected_min_rank_date,
                    MAX(rank_date) AS stale_hot_inspected_max_rank_date
                FROM temp.selected_stale_rank_hot
                """,
            )
            exact_matched = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS stale_hot_exact_matched_row_count
                FROM temp.selected_stale_rank_hot AS hot
                WHERE 1 = 1
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                """,
            )
            missing_key = _fetch_one_dict(
                conn,
                f"""
                SELECT COUNT(*) AS stale_hot_missing_archive_key_row_count
                FROM temp.selected_stale_rank_hot AS hot
                WHERE 1 = 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {key_match}
                  )
                """,
            )
            missing_profile = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT hot.fcode, hot.rank_date, hot.period
                    FROM temp.selected_stale_rank_hot AS hot
                    WHERE 1 = 1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE {key_match}
                      )
                )
                SELECT
                    COUNT(*) AS stale_hot_missing_archive_candidate_row_count,
                    COUNT(DISTINCT fcode) AS stale_hot_missing_archive_candidate_fund_count,
                    MIN(rank_date) AS stale_hot_missing_archive_candidate_min_rank_date,
                    MAX(rank_date) AS stale_hot_missing_archive_candidate_max_rank_date,
                    SUM(CASE WHEN fcode IS NULL OR TRIM(fcode) = '' THEN 1 ELSE 0 END)
                        AS stale_hot_missing_archive_invalid_fcode_row_count,
                    SUM(CASE WHEN rank_date IS NULL OR TRIM(rank_date) = '' THEN 1 ELSE 0 END)
                        AS stale_hot_missing_archive_invalid_rank_date_row_count
                FROM missing
                """,
            )
            missing_unique = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT hot.fcode, hot.rank_date, COALESCE(hot.period, '') AS period_key
                    FROM temp.selected_stale_rank_hot AS hot
                    WHERE 1 = 1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE {key_match}
                      )
                )
                SELECT COUNT(*) AS stale_hot_missing_archive_unique_key_count
                FROM (
                    SELECT fcode, rank_date, period_key
                    FROM missing
                    GROUP BY fcode, rank_date, period_key
                )
                """,
            )
            missing_duplicates = _fetch_one_dict(
                conn,
                f"""
                WITH missing AS (
                    SELECT hot.fcode, hot.rank_date, COALESCE(hot.period, '') AS period_key
                    FROM temp.selected_stale_rank_hot AS hot
                    WHERE 1 = 1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                          WHERE {key_match}
                      )
                ),
                duplicate_keys AS (
                    SELECT fcode, rank_date, period_key, COUNT(*) AS row_count
                    FROM missing
                    GROUP BY fcode, rank_date, period_key
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COUNT(*) AS stale_hot_missing_archive_duplicate_key_count,
                    COALESCE(SUM(row_count), 0) AS stale_hot_missing_archive_duplicate_key_row_count
                FROM duplicate_keys
                """,
            )
            missing_sample = _fetch_all_dicts(
                conn,
                f"""
                SELECT hot.fcode, hot.rank_date, hot.period, hot.rank_position,
                       hot.category_total, hot.percentile, hot.created_at
                FROM temp.selected_stale_rank_hot AS hot
                WHERE 1 = 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {key_match}
                  )
                ORDER BY hot.rank_date, hot.rowid
                LIMIT 20
                """,
            )
            key_conflict = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS stale_hot_key_conflict_row_count,
                    SUM(
                        CASE
                            WHEN hot.created_at IS NOT NULL
                             AND julianday(hot.created_at) IS NOT NULL
                             AND (
                                 archived.created_at IS NULL
                                 OR (
                                     julianday(archived.created_at) IS NOT NULL
                                     AND julianday(hot.created_at) > julianday(archived.created_at)
                                 )
                             )
                            THEN 1 ELSE 0
                        END
                    ) AS stale_hot_newer_conflict_row_count
                FROM temp.selected_stale_rank_hot AS hot
                INNER JOIN {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                    ON {key_match}
                WHERE NOT ({exact_match})
                """,
            )
        finally:
            try:
                conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_hot")
            except sqlite3.Error:
                pass
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_ARCHIVE_DB_ALIAS}")
            except sqlite3.Error:
                pass
    return {
        **selected,
        **exact_matched,
        **missing_key,
        **missing_profile,
        **missing_unique,
        **missing_duplicates,
        "sample_stale_hot_missing_archive_rows": missing_sample,
        **key_conflict,
    }


def _execute_stale_hot_archive_reconcile_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    columns: list[str],
    page_limit: int,
    checkpoint_context: Optional[dict] = None,
) -> dict:
    """Copy real stale-hot revisions into cold storage before any hot delete.

    The archive transaction commits first. A later exact-match delete may fail
    safely and leave a duplicate hot row for the next run; it can never delete
    a row whose current values were not verified in the archive.
    """
    started_at = datetime.now()
    effective_page_limit = min(
        max(1, int(page_limit or 1)),
        STALE_HOT_REPAIR_PAGE_LIMIT,
    )
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "archive_sql_executed": False,
            "selected_row_count": 0,
        }
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    hot_uri = _sqlite_readonly_uri(db_path)
    selected_columns = [column for column in ARCHIVE_COLUMNS if column in set(columns)]
    if not set(REQUIRED_HOT_COLUMNS).issubset(selected_columns):
        return {
            "status": "failed",
            "error": "selected_archive_columns_missing_required_fields",
            "archive_sql_executed": False,
            "selected_row_count": 0,
        }
    exact_match = _rank_exact_match_sql("hot", "archived", selected_columns)
    key_match = _rank_key_match_sql("hot", "archived")
    column_sql = ", ".join(selected_columns)
    hot_column_sql = ", ".join(f"hot.{column}" for column in selected_columns)
    conn = sqlite3.connect(str(archive_target_path), timeout=120, isolation_level=None)
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
                "selected_row_count": 0,
            }
        checkpoint_before = None
        if checkpoint_context is not None:
            checkpoint_before = scoped_counts(conn, table_name, active_cutoff)
            if checkpoint_before != (
                checkpoint_context["validated_rows"], checkpoint_context["validated_funds"],
            ):
                raise sqlite3.IntegrityError("rank_archive_counts_changed_before_copy")
        conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_reconcile")
        conn.execute(
            f"""
            CREATE TEMP TABLE selected_stale_rank_reconcile AS
            SELECT
                hot.rowid AS hot_rowid,
                {hot_column_sql}
            FROM {ROLLING_HOT_DB_ALIAS}.fund_rank_history AS hot
            WHERE hot.rank_date < ?
            ORDER BY hot.rank_date, hot.rowid
            LIMIT ?
            """,
            (active_cutoff, effective_page_limit),
        )
        _require_iso_rank_dates(conn, "temp.selected_stale_rank_reconcile")
        conn.execute(
            "CREATE UNIQUE INDEX idx_selected_stale_rank_reconcile_rowid "
            "ON selected_stale_rank_reconcile (hot_rowid)"
        )
        conn.execute(
            "CREATE INDEX idx_selected_stale_rank_reconcile_key "
            "ON selected_stale_rank_reconcile (fcode, rank_date, period)"
        )
        selected_count = _normalize_int(
            conn.execute("SELECT COUNT(*) FROM temp.selected_stale_rank_reconcile").fetchone()[0]
        )
        exact_count = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM temp.selected_stale_rank_reconcile AS hot
                WHERE EXISTS (
                    SELECT 1 FROM {table_name} AS archived WHERE {exact_match}
                )
                """
            ).fetchone()[0]
        )
        conflict_count = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM temp.selected_stale_rank_reconcile AS hot
                INNER JOIN {table_name} AS archived ON {key_match}
                WHERE NOT ({exact_match})
                """
            ).fetchone()[0]
        )
        newer_conflict_count = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM temp.selected_stale_rank_reconcile AS hot
                INNER JOIN {table_name} AS archived ON {key_match}
                WHERE NOT ({exact_match})
                  AND hot.created_at IS NOT NULL
                  AND julianday(hot.created_at) IS NOT NULL
                  AND (
                      archived.created_at IS NULL
                      OR (
                          julianday(archived.created_at) IS NOT NULL
                          AND julianday(hot.created_at) > julianday(archived.created_at)
                      )
                  )
                """
            ).fetchone()[0]
        )
        unsafe_conflict_count = max(0, conflict_count - newer_conflict_count)
        if unsafe_conflict_count > 0:
            raise sqlite3.IntegrityError(
                "stale_hot_conflict_source_order_unresolved:"
                f"conflicts={conflict_count}, newer_hot={newer_conflict_count}, "
                f"unsafe={unsafe_conflict_count}"
            )
        missing_count = max(0, selected_count - exact_count - conflict_count)
        replace_count = conflict_count + missing_count
        conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_replace")
        conn.execute(
            f"""
            CREATE TEMP TABLE selected_stale_rank_replace AS
            SELECT hot.*
            FROM temp.selected_stale_rank_reconcile AS hot
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} AS archived WHERE {exact_match}
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX idx_selected_stale_rank_replace_rowid "
            "ON selected_stale_rank_replace (hot_rowid)"
        )
        conn.execute(
            "CREATE INDEX idx_selected_stale_rank_replace_key "
            "ON selected_stale_rank_replace (fcode, rank_date, period)"
        )
        materialized_replace_count = _normalize_int(
            conn.execute("SELECT COUNT(*) FROM temp.selected_stale_rank_replace").fetchone()[0]
        )
        if materialized_replace_count != replace_count:
            raise sqlite3.IntegrityError(
                "stale_hot_replace_materialization_count_mismatch:"
                f"expected={replace_count}, materialized={materialized_replace_count}"
            )

        if conflict_count > 0:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fund_rank_history_archive_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fcode TEXT NOT NULL,
                    rank_date TEXT NOT NULL,
                    period TEXT,
                    old_rank_position INTEGER,
                    old_category_total INTEGER,
                    old_percentile REAL,
                    old_created_at TEXT,
                    new_rank_position INTEGER,
                    new_category_total INTEGER,
                    new_percentile REAL,
                    new_created_at TEXT,
                    reconciled_at TEXT NOT NULL,
                    source TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_rank_archive_revisions_key
                ON fund_rank_history_archive_revisions (fcode, rank_date, period)
                """
            )
            conn.execute(
                f"""
                INSERT INTO fund_rank_history_archive_revisions (
                    fcode, rank_date, period,
                    old_rank_position, old_category_total, old_percentile, old_created_at,
                    new_rank_position, new_category_total, new_percentile, new_created_at,
                    reconciled_at, source
                )
                SELECT
                    hot.fcode, hot.rank_date, hot.period,
                    archived.rank_position, archived.category_total,
                    archived.percentile, archived.created_at,
                    hot.rank_position, hot.category_total,
                    hot.percentile, hot.created_at,
                    ?, 'hot_reingestion_newer_source'
                FROM temp.selected_stale_rank_replace AS hot
                INNER JOIN {table_name} AS archived ON {key_match}
                WHERE NOT ({exact_match})
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )
            revision_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
            if revision_count != conflict_count:
                raise sqlite3.IntegrityError(
                    "stale_hot_revision_count_mismatch:"
                    f"expected={conflict_count}, inserted={revision_count}"
                )
            conn.execute(
                f"""
                DELETE FROM {table_name}
                WHERE rowid IN (
                    SELECT archived.rowid
                    FROM {table_name} AS archived
                    INNER JOIN temp.selected_stale_rank_replace AS hot ON {key_match}
                )
                """
            )
            deleted_conflict_count = _normalize_int(
                conn.execute("SELECT changes()").fetchone()[0]
            )
            if deleted_conflict_count != conflict_count:
                raise sqlite3.IntegrityError(
                    "stale_hot_conflict_delete_count_mismatch:"
                    f"expected={conflict_count}, deleted={deleted_conflict_count}"
                )
        else:
            revision_count = 0
            deleted_conflict_count = 0

        if replace_count > 0:
            conn.execute(
                f"""
                INSERT INTO {table_name} ({column_sql})
                SELECT {hot_column_sql}
                FROM temp.selected_stale_rank_replace AS hot
                """
            )
            inserted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
            if inserted_count != replace_count:
                raise sqlite3.IntegrityError(
                    "stale_hot_replace_insert_count_mismatch:"
                    f"expected={replace_count}, inserted={inserted_count}"
                )
        else:
            inserted_count = 0

        verified_count = _normalize_int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM temp.selected_stale_rank_reconcile AS hot
                WHERE EXISTS (
                    SELECT 1 FROM {table_name} AS archived WHERE {exact_match}
                )
                """
            ).fetchone()[0]
        )
        if verified_count != selected_count:
            raise sqlite3.IntegrityError(
                "stale_hot_archive_verify_count_mismatch:"
                f"selected={selected_count}, verified={verified_count}"
            )
        checkpoint_id = None
        if checkpoint_context is not None and checkpoint_before is not None and replace_count:
            checkpoint_id = record_checkpoint(
                conn, checkpoint_context, before=checkpoint_before,
                after=scoped_counts(conn, table_name, active_cutoff),
                inserted_rows=missing_count, revision_rows=revision_count,
            )
        conn.execute("COMMIT")
        return {
            "status": "reconciled",
            "archive_sql_executed": bool(deleted_conflict_count or inserted_count),
            "selected_row_count": selected_count,
            "exact_row_count": exact_count,
            "conflict_row_count": conflict_count,
            "newer_conflict_row_count": newer_conflict_count,
            "unsafe_conflict_row_count": unsafe_conflict_count,
            "revision_audit_row_count": revision_count,
            "updated_row_count": deleted_conflict_count,
            "missing_row_count": missing_count,
            "inserted_row_count": missing_count,
            "replacement_inserted_row_count": inserted_count,
            "verified_row_count": verified_count,
            "checkpoint_id": checkpoint_id,
            "page_limit": effective_page_limit,
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
            "selected_row_count": 0,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_replace")
        except sqlite3.Error:
            pass
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_reconcile")
        except sqlite3.Error:
            pass
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_rolling_archive_export_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    current_cutoff_date: date,
    columns: list[str],
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {"status": "skipped", "skip_reason": "page_limit_not_positive", "archive_sql_executed": False}
    table_name = _normalize_table_name(archive_table_name)
    archive_target_path.parent.mkdir(parents=True, exist_ok=True)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    hot_uri = _sqlite_readonly_uri(db_path)
    column_sql = ", ".join(columns)
    hot_column_sql = ", ".join(f"hot.{column}" for column in columns)
    key_match = _rank_key_match_sql("hot", "archived")
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
            return {"status": "failed", "error": "archive_table_schema_invalid", "archive_sql_executed": False}
        _require_iso_rank_dates(
            conn, f"{ROLLING_HOT_DB_ALIAS}.fund_rank_history",
            "rank_date >= ? AND rank_date < ?", (active_cutoff, current_cutoff),
        )
        before = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE rank_date >= ? AND rank_date < ?",
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {table_name} ({column_sql})
            SELECT {hot_column_sql}
            FROM {ROLLING_HOT_DB_ALIAS}.fund_rank_history AS hot
            WHERE hot.rank_date >= ?
              AND hot.rank_date < ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM {table_name} AS archived
                  WHERE {key_match}
              )
            ORDER BY hot.rank_date, hot.fcode, COALESCE(hot.period, '')
            LIMIT ?
            """,
            (active_cutoff, current_cutoff, int(page_limit)),
        )
        after = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE rank_date >= ? AND rank_date < ?",
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
        return {"status": "failed", "error": str(exc), "archive_sql_executed": False}
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {ROLLING_HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _execute_stale_hot_before_active_delete_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    active_cutoff_date: date,
    columns: list[str],
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    effective_page_limit = min(
        max(1, int(page_limit or 1)),
        STALE_HOT_REPAIR_PAGE_LIMIT,
    )
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
    exact_match = _rank_exact_match_sql("hot", "archived", columns)
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date < ?",
                (active_cutoff,),
            ).fetchone()[0]
        )
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            f"""
            CREATE TEMP TABLE selected_stale_rank_delete AS
            SELECT
                hot.rowid AS hot_rowid,
                hot.fcode,
                hot.rank_date,
                hot.period,
                hot.rank_position,
                hot.category_total,
                hot.percentile,
                hot.created_at
            FROM fund_rank_history AS hot
            WHERE hot.rank_date < ?
            ORDER BY hot.rank_date, hot.rowid
            LIMIT ?
            """,
            (active_cutoff, effective_page_limit),
        )
        _require_iso_rank_dates(conn, "temp.selected_stale_rank_delete")
        conn.execute(
            "CREATE INDEX idx_selected_stale_rank_delete_rowid "
            "ON selected_stale_rank_delete (hot_rowid)"
        )
        conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_exact_delete")
        conn.execute(
            f"""
            CREATE TEMP TABLE selected_stale_rank_exact_delete AS
            SELECT hot.hot_rowid, hot.rank_date
            FROM selected_stale_rank_delete AS hot
            WHERE EXISTS (
                SELECT 1
                FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                WHERE {exact_match}
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX idx_selected_stale_rank_exact_delete_rowid "
            "ON selected_stale_rank_exact_delete (hot_rowid)"
        )
        selected_count = _normalize_int(
            conn.execute("SELECT COUNT(*) FROM selected_stale_rank_exact_delete").fetchone()[0]
        )
        conn.execute(
            """
            DELETE FROM fund_rank_history
            WHERE rowid IN (
                SELECT hot_rowid
                FROM selected_stale_rank_exact_delete
                ORDER BY rank_date, hot_rowid
            )
            """
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        if deleted_count != selected_count:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": (
                    "stale_hot_delete_count_mismatch:"
                    f"selected={selected_count}, deleted={deleted_count}"
                ),
                "stale_hot_delete_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
            }
        conn.execute("COMMIT")
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date < ?",
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
            "page_limit": effective_page_limit,
            "selected_row_count": selected_count,
            "stale_hot_rows_before": before,
            "stale_hot_rows_after": after,
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
            "stale_hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 3),
        }
    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_exact_delete")
        except sqlite3.Error:
            pass
        try:
            conn.execute("DROP TABLE IF EXISTS temp.selected_stale_rank_delete")
        except sqlite3.Error:
            pass
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
    columns: list[str],
    page_limit: int,
) -> dict:
    started_at = datetime.now()
    if page_limit <= 0:
        return {"status": "skipped", "skip_reason": "page_limit_not_positive", "delete_sql_executed": False}
    table_name = _normalize_table_name(archive_table_name)
    active_cutoff = active_cutoff_date.isoformat()
    current_cutoff = current_cutoff_date.isoformat()
    archive_uri = _sqlite_readonly_uri(archive_target_path)
    exact_match = _rank_exact_match_sql("hot", "archived", columns)
    conn = sqlite3.connect(str(db_path), timeout=120, isolation_level=None, uri=True)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {ROLLING_ARCHIVE_DB_ALIAS}", (archive_uri,))
        attached = True
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date >= ? AND rank_date < ?",
                (active_cutoff, current_cutoff),
            ).fetchone()[0]
        )
        conn.execute("BEGIN IMMEDIATE")
        _require_iso_rank_dates(
            conn, "fund_rank_history", "rank_date >= ? AND rank_date < ?",
            (active_cutoff, current_cutoff),
        )
        conn.execute(
            f"""
            DELETE FROM fund_rank_history
            WHERE rowid IN (
                SELECT hot.rowid
                FROM fund_rank_history AS hot
                WHERE hot.rank_date >= ?
                  AND hot.rank_date < ?
                  AND EXISTS (
                      SELECT 1
                      FROM {ROLLING_ARCHIVE_DB_ALIAS}.{table_name} AS archived
                      WHERE {exact_match}
                  )
                ORDER BY hot.rank_date, hot.fcode, COALESCE(hot.period, '')
                LIMIT ?
            )
            """,
            (active_cutoff, current_cutoff, int(page_limit)),
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        conn.execute("COMMIT")
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_rank_history WHERE rank_date >= ? AND rank_date < ?",
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
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {"status": "failed", "error": str(exc), "delete_sql_executed": False}
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
    selected_columns: list[str],
    current_cutoff_date: date,
    primary_snapshot: dict,
    archive_snapshot: dict,
    rolling_summary: dict,
) -> dict:
    archive_rows = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_row_count"))
    archive_funds = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_fund_count"))
    hot_total = _normalize_int(primary_snapshot.get("total_rank_row_count"))
    live_cold_rows = _normalize_int(primary_snapshot.get("hot_before_current_cutoff_row_count"))
    materialized = live_cold_rows == 0 or bool(rolling_summary.get("binding_materialized_verified"))
    summary = {
        "runner_status": "archive_binding_active",
        "auto_governance_stage": "rank_hot_cold_binding_active",
        "mode": "rolling_archive_binding",
        "effective_mode": "dry_run",
        "archive_export_supported": True,
        "hot_delete_supported": True,
        "vacuum_supported": False,
        "source_preflight_artifact": _normalize_text(active_summary.get("source_preflight_artifact")),
        "preflight_status": "post_split_binding_health",
        "preflight_ready": True,
        "require_backup_for_hot_delete": False,
        "backup_artifact_path": _normalize_text(active_summary.get("backup_artifact_path")),
        "backup_artifact_absolute_path": _normalize_text(active_summary.get("backup_artifact_absolute_path")),
        "backup_artifact_exists": bool(active_summary.get("backup_artifact_exists")),
        "archive_cutoff_date": current_cutoff_date.isoformat(),
        "active_db_path": _stringify_project_path(db_path, root),
        "active_db_absolute_path": str(db_path),
        "active_db_quick_check": _sqlite_quick_check(db_path),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path),
        "archive_target_exists": True,
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_quick_check": _sqlite_quick_check(archive_target_path),
        "archive_table_name": archive_table_name,
        "archive_table_exists": True,
        "archive_table_schema_valid": True,
        "selected_archive_columns": selected_columns,
        "expected_archive_row_count": archive_rows,
        "expected_archive_fund_count": archive_funds,
        "existing_archive_row_count": archive_rows,
        "existing_archive_fund_count": archive_funds,
        "archive_out_of_scope_row_count": _normalize_int(
            archive_snapshot.get("archive_out_of_scope_current_row_count")
        ),
        "archive_missing_row_count": 0 if materialized else None,
        "archive_materialized_and_reconciled": materialized,
        "archive_binding_active": True,
        "live_cold_row_count": _normalize_int(
            primary_snapshot.get("hot_before_current_cutoff_row_count")
        ),
        "live_cold_fund_count": _normalize_int(
            primary_snapshot.get("hot_before_current_cutoff_fund_count")
        ),
        "live_cold_min_rank_date": primary_snapshot.get("hot_before_current_cutoff_min_rank_date") or "",
        "live_cold_max_rank_date": primary_snapshot.get("hot_before_current_cutoff_max_rank_date") or "",
        "exact_matched_hot_cold_row_count": live_cold_rows if materialized else None,
        "hot_unarchived_row_count": 0 if materialized else None,
        "key_conflict_row_count": 0 if materialized else None,
        "page_limit": _normalize_int(rolling_summary.get("page_limit")),
        "archive_export_result_status": str(rolling_summary.get("binding_refresh_reason") or "rolling_archive_complete"),
        "archive_export_inserted_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_inserted_row_count")
        ),
        "hot_delete_result_status": "rolling_archive_complete" if live_cold_rows == 0 else "hot_cleanup_pending",
        "hot_delete_deleted_row_count": _normalize_int(
            rolling_summary.get("rolling_hot_delete_deleted_row_count")
        ),
        "archive_sql_executed": False,
        "hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "effective_hot_cold_row_count": hot_total + archive_rows if live_cold_rows == 0 else None,
        "effective_row_count_missing_reason": "" if live_cold_rows == 0 else "hot_cold_overlap_not_fully_counted",
        "rolling_archive_supported": True,
        "rolling_archive_required": bool(live_cold_rows > 0 or rolling_summary.get("rolling_archive_required")),
        "rolling_archive_cutoff_date": str(rolling_summary.get("rolling_archive_cutoff_date") or current_cutoff_date.isoformat()),
        "rolling_archive_candidate_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_candidate_row_count")
        ),
        "rolling_archive_inserted_row_count": _normalize_int(
            rolling_summary.get("rolling_archive_inserted_row_count")
        ),
        "rolling_archive_deleted_row_count": _normalize_int(
            rolling_summary.get("rolling_hot_delete_deleted_row_count")
        ),
        "source_active_apply_runner_artifact": str(source_active_artifact or ""),
        "blocker_count": 0,
    }
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "summary": summary,
        "gate_checks": [],
        "blockers": [],
        "findings": [
            "rank cold binding was durably refreshed; remaining hot copies are reported separately."
        ],
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "rolling_archive_binding_payload": True,
        },
    }


def build_rank_history_archive_rolling_runner_payload(
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
    """Run or preview cumulative rank archive maintenance.

    This runner only uses an already active cumulative cold DB. It never creates
    a new cold DB and never runs VACUUM.
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
    governance_summary = _normalize_dict(_normalize_dict(governance_payload).get("summary"))

    active_db_path = _resolve_path(
        db_path or active_summary.get("active_db_absolute_path") or active_summary.get("active_db_path") or "backend/data/bond_funds.db",
        root,
    )
    if active_db_path is None:
        active_db_path = (root / "backend/data/bond_funds.db").resolve()
    inferred_cutoff = _infer_current_cutoff_from_primary_db(db_path=active_db_path)
    active_cutoff_text = _normalize_text(active_summary.get("archive_cutoff_date"))
    current_cutoff_text = _normalize_text(
        governance_summary.get("archive_cutoff_date")
        or inferred_cutoff.get("archive_cutoff_date")
    )
    active_cutoff_date = _parse_iso_date(active_cutoff_text)
    current_cutoff_date = _parse_iso_date(current_cutoff_text)
    archive_target_path = _resolve_first_existing_path(
        (
            active_summary.get("archive_target_absolute_path"),
            active_summary.get("archive_target_path"),
        ),
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
    active_binding_available = bool(
        active_summary.get("archive_binding_active")
        and _normalize_text(active_summary.get("runner_status")) == "archive_binding_active"
    )
    active_db_exists = bool(active_db_path and active_db_path.exists() and active_db_path.is_file())
    archive_exists = bool(
        archive_target_path and archive_target_path.exists() and archive_target_path.is_file()
    )
    active_db_quick_check = ""
    archive_quick_check = ""

    primary_snapshot: dict[str, Any] = {"available": False}
    archive_snapshot: dict[str, Any] = {}
    match_snapshot: dict[str, Any] = {}
    stale_hot_snapshot: dict[str, Any] = {}
    selected_columns: list[str] = []
    if active_cutoff_date and current_cutoff_date and active_db_path and active_db_exists:
        try:
            primary_snapshot = _inspect_primary_window_state(
                db_path=active_db_path,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                page_limit=effective_page_limit,
            )
            selected_columns = list(primary_snapshot.get("selected_archive_columns") or [])
        except sqlite3.Error as exc:
            primary_snapshot = {"available": False, "error": str(exc)}
    current_cutoff_advances = bool(
        active_cutoff_date
        and current_cutoff_date
        and current_cutoff_date > active_cutoff_date
    )
    rolling_hot_candidate_rows = _normalize_int(
        primary_snapshot.get("rolling_hot_candidate_row_count")
    )
    initial_hot_before_active = _normalize_int(
        primary_snapshot.get("hot_before_active_cutoff_row_count")
    )
    quick_check_required = bool(
        requested_mode == "rolling_archive"
        and (
            (current_cutoff_advances and rolling_hot_candidate_rows > 0)
            or initial_hot_before_active > 0
        )
    )
    if quick_check_required:
        active_db_quick_check = _sqlite_quick_check(active_db_path) if active_db_exists else ""
        archive_quick_check = _sqlite_quick_check(archive_target_path) if archive_exists else ""
    else:
        active_db_quick_check = "skipped_light_probe" if active_db_exists else ""
        archive_quick_check = "skipped_light_probe" if archive_exists else ""
    if active_cutoff_date and current_cutoff_date:
        # A saved binding is an expectation, not a measured cold count. In
        # particular an interrupted copy may have committed additional rows.
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            active_cutoff_date=active_cutoff_date,
            current_cutoff_date=current_cutoff_date,
        )
    checkpoint_context = {
        "binding_sha256": binding_fingerprint(active_binding_payload),
        "archive_table": archive_table_name, "cutoff": active_cutoff_text,
        "binding_rows": expected_active_archive_rows, "binding_funds": expected_active_archive_funds,
        "validated_rows": _normalize_int(archive_snapshot.get("archive_before_active_cutoff_row_count")),
        "validated_funds": _normalize_int(archive_snapshot.get("archive_before_active_cutoff_fund_count")),
    }
    recovered_checkpoint: dict[str, Any] = {}
    if (
        archive_exists and archive_snapshot.get("archive_table_schema_valid")
        and not archive_snapshot.get("archive_inspection_error")
        and (
            checkpoint_context["validated_rows"] != expected_active_archive_rows
            or (expected_active_archive_funds > 0
                and checkpoint_context["validated_funds"] != expected_active_archive_funds)
        )
    ):
        try:
            with _open_readonly_db(archive_target_path) as conn:
                checkpoint = read_checkpoint(conn, checkpoint_context)
            if (
                checkpoint
                and checkpoint["after_rows"] == checkpoint_context["validated_rows"]
                and checkpoint["after_funds"] == checkpoint_context["validated_funds"]
            ):
                recovered_checkpoint = checkpoint
                expected_active_archive_rows = checkpoint["after_rows"]
                expected_active_archive_funds = checkpoint["after_funds"]
        except sqlite3.Error as exc:
            archive_snapshot["archive_inspection_error"] = f"checkpoint_read_failed:{exc}"
    archive_table_schema_valid = bool(archive_snapshot.get("archive_table_schema_valid"))
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
        and selected_columns
    ):
        try:
            match_snapshot = _inspect_rolling_match_state(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                columns=selected_columns,
            )
        except sqlite3.Error as exc:
            match_snapshot = {"inspection_error": str(exc)}
    if (
        active_cutoff_date
        and active_db_path
        and archive_target_path
        and active_db_exists
        and archive_exists
        and archive_table_schema_valid
        and selected_columns
    ):
        try:
            stale_hot_snapshot = _inspect_stale_hot_before_active_state(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                columns=selected_columns,
                page_limit=effective_page_limit,
            )
        except sqlite3.Error as exc:
            stale_hot_snapshot = {"inspection_error": str(exc)}

    hot_before_active = _normalize_int(primary_snapshot.get("hot_before_active_cutoff_row_count"))
    hot_before_current = _normalize_int(primary_snapshot.get("hot_before_current_cutoff_row_count"))
    rolling_hot_candidate_funds = _normalize_int(primary_snapshot.get("rolling_hot_candidate_fund_count"))
    archive_before_active_rows = _normalize_int(archive_snapshot.get("archive_before_active_cutoff_row_count"))
    archive_before_active_funds = _normalize_int(archive_snapshot.get("archive_before_active_cutoff_fund_count"))
    archive_before_current_rows = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_row_count"))
    archive_before_current_funds = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_fund_count"))
    archive_out_of_scope_current_rows = _normalize_int(
        archive_snapshot.get("archive_out_of_scope_current_row_count")
    )
    hot_unarchived_window_rows = _normalize_int(match_snapshot.get("hot_unarchived_window_row_count"))
    exact_matched_window_rows = _normalize_int(match_snapshot.get("exact_matched_window_row_count"))
    key_conflict_window_rows = _normalize_int(match_snapshot.get("key_conflict_window_row_count"))
    stale_hot_exact_matched_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
    )
    stale_hot_inspected_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_inspected_row_count")
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
    stale_hot_missing_archive_invalid_rank_date_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_missing_archive_invalid_rank_date_row_count")
    )
    stale_hot_key_conflict_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_key_conflict_row_count")
    )
    stale_hot_newer_conflict_rows = _normalize_int(
        stale_hot_snapshot.get("stale_hot_newer_conflict_row_count")
    )
    stale_hot_unsafe_conflict_rows = max(
        0,
        stale_hot_key_conflict_rows - stale_hot_newer_conflict_rows,
    )
    stale_hot_inspection_error = _normalize_text(stale_hot_snapshot.get("inspection_error"))
    stale_hot_repair_required = bool(hot_before_active > 0)
    stale_hot_archive_reconcile_supported = bool(
        hot_before_active > 0
        and stale_hot_inspected_rows > 0
        and stale_hot_inspected_rows
        == (
            stale_hot_exact_matched_rows
            + stale_hot_missing_archive_key_rows
            + stale_hot_key_conflict_rows
        )
        and stale_hot_unsafe_conflict_rows == 0
        and not stale_hot_inspection_error
    )
    stale_hot_exact_delete_supported = bool(
        hot_before_active > 0
        and stale_hot_inspected_rows > 0
        and stale_hot_exact_matched_rows > 0
        and not stale_hot_inspection_error
    )
    stale_hot_repairable = bool(
        hot_before_active == 0
        or stale_hot_archive_reconcile_supported
    )
    duplicate_rolling_keys = _normalize_int(primary_snapshot.get("duplicate_rolling_business_key_count"))
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
        ),
        _build_gate(
            "active_binding_artifact",
            "已生效绑定证据",
            "ok" if active_binding_available else "blocked",
            f"source_active_apply_runner_artifact={active_artifact_path or ''}",
        ),
        _build_gate(
            "cutoff_progress",
            "滚动 cutoff",
            "ok" if active_cutoff_date and current_cutoff_date
            and current_cutoff_date >= active_cutoff_date else "blocked",
            f"active_cutoff={active_cutoff_text}, current_cutoff={current_cutoff_text}; "
            f"inference_error={inferred_cutoff.get('error') or ''}",
        ),
        _build_gate(
            "primary_database",
            "活动主库",
            "ok" if active_db_exists and primary_snapshot.get("available") else "blocked",
            f"active_db_path={active_db_path}, error={primary_snapshot.get('error') or ''}",
        ),
        _build_gate(
            "archive_database",
            "累计冷库",
            "ok" if archive_exists and archive_snapshot.get("archive_table_exists") else "blocked",
            f"archive_target_path={archive_target_path}, table={archive_table_name}",
        ),
        _build_gate(
            "archive_schema",
            "冷库 schema",
            "ok" if archive_table_schema_valid else "blocked",
            f"schema_valid={archive_table_schema_valid}",
        ),
        _build_gate(
            "archive_inspection",
            "冷库实际计数核查",
            "blocked" if archive_snapshot.get("archive_inspection_error") else "ok",
            f"inspection_error={archive_snapshot.get('archive_inspection_error') or ''}; "
            f"recovered_checkpoint_id={recovered_checkpoint.get('id')}",
        ),
        _build_gate(
            "primary_quick_check",
            "主库 quick_check",
            "ok"
            if (not quick_check_required or active_db_quick_check.lower() == "ok")
            else "blocked",
            f"quick_check={active_db_quick_check}; required={quick_check_required}",
        ),
        _build_gate(
            "archive_quick_check",
            "冷库 quick_check",
            "ok"
            if (not quick_check_required or archive_quick_check.lower() == "ok")
            else "blocked",
            f"quick_check={archive_quick_check}; required={quick_check_required}",
        ),
        _build_gate(
            "active_hot_empty",
            "已生效 cutoff 前热库回流",
            (
                "ok"
                if hot_before_active == 0
                else "review"
                if stale_hot_archive_reconcile_supported
                else "blocked"
            ),
            (
                f"hot_before_active_cutoff_row_count={hot_before_active}, "
                f"stale_exact={stale_hot_exact_matched_rows}, "
                f"stale_missing_archive_key={stale_hot_missing_archive_key_rows}, "
                f"stale_key_conflict={stale_hot_key_conflict_rows}, "
                f"stale_newer_conflict={stale_hot_newer_conflict_rows}, "
                f"stale_unsafe_conflict={stale_hot_unsafe_conflict_rows}, "
                f"inspection_error={stale_hot_inspection_error}"
            ),
            "旧 cutoff 前热库回流会先将真实热记录补入/修订冷库并记录冲突审计；只有逐列复核一致后才允许删除热副本。",
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
        ),
        _build_gate(
            "archive_out_of_scope_current",
            "当前 cutoff 外冷库行",
            "ok" if archive_out_of_scope_current_rows == 0 else "blocked",
            f"archive_out_of_scope_current_row_count={archive_out_of_scope_current_rows}",
        ),
        _build_gate(
            "rolling_match_inspection",
            "滚动窗口匹配核查",
            "blocked" if match_snapshot.get("inspection_error") else "ok",
            f"inspection_error={match_snapshot.get('inspection_error') or ''}",
        ),
        _build_gate(
            "rolling_key_conflicts",
            "滚动窗口键值冲突",
            "ok" if key_conflict_window_rows == 0 else "blocked",
            f"key_conflict_window_row_count={key_conflict_window_rows}",
        ),
        _build_gate(
            "rolling_duplicate_hot_keys",
            "滚动热库业务键重复",
            "ok" if duplicate_rolling_keys == 0 else "blocked",
            f"duplicate_rolling_business_key_count={duplicate_rolling_keys}",
        ),
        _build_gate(
            "rolling_page_limit",
            "滚动分页上限",
            "ok" if requested_mode != "rolling_archive" or effective_page_limit > 0 else "blocked",
            f"page_limit={effective_page_limit}",
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
        "inserted_row_count": 0,
    }
    stale_hot_archive_reconcile_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "archive_sql_executed": False,
        "selected_row_count": 0,
        "conflict_row_count": 0,
        "updated_row_count": 0,
        "missing_row_count": 0,
        "inserted_row_count": 0,
    }
    stale_hot_delete_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "stale_hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "deleted_row_count": 0,
    }
    hot_delete_result = {
        "status": "not_requested" if requested_mode != "rolling_archive" else "not_executed",
        "hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "deleted_row_count": 0,
    }
    updated_active_binding_payload: dict = {}
    committed_binding_artifacts: dict[str, str] = {}
    committed_binding_cutoff = active_cutoff_date

    def publish_binding(cutoff: date, reason: str, *, materialized: bool = False) -> bool:
        """Publish cold visibility before deleting rows outside the old cutoff."""
        nonlocal updated_active_binding_payload, committed_binding_artifacts, committed_binding_cutoff
        nonlocal primary_snapshot
        if primary_snapshot.get("inspection_mode") == "window_state":
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path, active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date, page_limit=effective_page_limit,
            )
        binding_primary = dict(primary_snapshot)
        binding_archive = dict(archive_snapshot)
        if cutoff == active_cutoff_date:
            for key, value in primary_snapshot.items():
                if key.startswith("hot_before_active_cutoff_"):
                    binding_primary[key.replace("before_active_", "before_current_")] = value
            for key, value in archive_snapshot.items():
                if key.startswith("archive_before_active_cutoff_"):
                    binding_archive[key.replace("before_active_", "before_current_")] = value
        context = {
            "page_limit": effective_page_limit, "binding_refresh_reason": reason,
            "binding_materialized_verified": materialized,
            "rolling_archive_required": _normalize_int(primary_snapshot.get("hot_before_current_cutoff_row_count")) > 0,
            "rolling_archive_cutoff_date": current_cutoff_text,
            "rolling_archive_candidate_row_count": rolling_hot_candidate_rows,
            "rolling_archive_inserted_row_count": archive_export_result.get("inserted_row_count", 0),
            "rolling_hot_delete_deleted_row_count": hot_delete_result.get("deleted_row_count", 0),
        }
        try:
            if not primary_snapshot.get("available") or archive_snapshot.get("archive_inspection_error"):
                raise sqlite3.IntegrityError("rank_binding_post_copy_inspection_failed")
            candidate = _build_updated_active_binding_payload(
                root=root, source_active_artifact=active_artifact_path, active_summary=active_summary,
                db_path=active_db_path, archive_target_path=archive_target_path,
                archive_table_name=archive_table_name, selected_columns=selected_columns,
                current_cutoff_date=cutoff, primary_snapshot=binding_primary,
                archive_snapshot=binding_archive, rolling_summary=context,
            )
            if any(candidate["summary"][key].lower() != "ok"
                   for key in ("active_db_quick_check", "archive_quick_check")):
                raise sqlite3.IntegrityError("rank_binding_post_copy_quick_check_failed")
            paths = write_rank_history_archive_apply_runner_artifacts(
                candidate, output_dir=_resolve_output_dir(output_dir, root),
                timestamp=datetime.now().strftime("%Y%m%d_%H%M%S_%f") + "_rank_rolling_binding",
            )
            updated_active_binding_payload = candidate
            committed_binding_artifacts = {"artifact_path": str(paths[0]), "markdown_path": str(paths[1])}
            committed_binding_cutoff = cutoff
            return True
        except (OSError, ValueError, sqlite3.Error) as exc:
            blockers.append(f"rank active binding publish failed: {exc}")
            return False

    if (
        requested_mode == "rolling_archive"
        and not (
            stale_hot_exact_delete_blockers
            if stale_hot_archive_reconcile_supported
            else blockers
        )
        and (current_cutoff_advances or stale_hot_repair_required)
        and active_db_path
        and archive_target_path
        and active_cutoff_date
        and current_cutoff_date
        and selected_columns
    ):
        if stale_hot_inspected_rows > 0:
            stale_hot_archive_reconcile_result = (
                _execute_stale_hot_archive_reconcile_page(
                    db_path=active_db_path,
                    archive_target_path=archive_target_path,
                    archive_table_name=archive_table_name,
                    active_cutoff_date=active_cutoff_date,
                    columns=selected_columns,
                    page_limit=min(effective_page_limit, stale_hot_inspected_rows),
                    checkpoint_context=checkpoint_context,
                )
            )
            if stale_hot_archive_reconcile_result.get("status") == "failed":
                blockers.append(
                    "旧 cutoff 前 rank 热库回流冷库补证失败: "
                    + _normalize_text(
                        stale_hot_archive_reconcile_result.get("error") or "unknown"
                    )
                )
            reconciled_count = _normalize_int(
                stale_hot_archive_reconcile_result.get("selected_row_count")
            )
        else:
            reconciled_count = 0
        if not blockers and reconciled_count > 0:
            stale_hot_delete_result = _execute_stale_hot_before_active_delete_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                columns=selected_columns,
                page_limit=min(effective_page_limit, reconciled_count),
            )
            if stale_hot_delete_result.get("status") == "failed":
                blockers.append(
                    "旧 cutoff 前 rank 热库回流删除失败: "
                    + _normalize_text(stale_hot_delete_result.get("error") or "unknown")
                )
            if not blockers:
                primary_snapshot = _inspect_primary_db(
                    db_path=active_db_path,
                    active_cutoff_date=active_cutoff_date,
                    current_cutoff_date=current_cutoff_date,
                    page_limit=effective_page_limit,
                )
                stale_hot_snapshot = _inspect_stale_hot_before_active_state(
                    db_path=active_db_path,
                    archive_target_path=archive_target_path,
                    archive_table_name=archive_table_name,
                    active_cutoff_date=active_cutoff_date,
                    columns=selected_columns,
                    page_limit=effective_page_limit,
                )
                hot_before_active = _normalize_int(
                    primary_snapshot.get("hot_before_active_cutoff_row_count")
                )
                stale_hot_inspected_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_inspected_row_count")
                )
                stale_hot_exact_matched_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_exact_matched_row_count")
                )
                stale_hot_missing_archive_key_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_missing_archive_key_row_count")
                )
                stale_hot_key_conflict_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_key_conflict_row_count")
                )
                stale_hot_newer_conflict_rows = _normalize_int(
                    stale_hot_snapshot.get("stale_hot_newer_conflict_row_count")
                )
                stale_hot_unsafe_conflict_rows = max(
                    0,
                    stale_hot_key_conflict_rows - stale_hot_newer_conflict_rows,
                )
        if not blockers and hot_before_active == 0 and hot_unarchived_window_rows > 0:
            archive_export_result = _execute_rolling_archive_export_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                columns=selected_columns,
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
                columns=selected_columns,
            )
            exact_matched_window_rows = _normalize_int(match_snapshot.get("exact_matched_window_row_count"))
            key_conflict_window_rows = _normalize_int(match_snapshot.get("key_conflict_window_row_count"))
            if archive_snapshot.get("archive_inspection_error") or match_snapshot.get("inspection_error"):
                blockers.append("rank post-copy archive or match inspection failed")
            if key_conflict_window_rows > 0:
                blockers.append(f"滚动窗口存在 {key_conflict_window_rows} 行 rank 键值冲突。")
        window_fully_materialized = bool(
            current_cutoff_advances and hot_before_active == 0
            and exact_matched_window_rows > 0
            and _normalize_int(match_snapshot.get("hot_unarchived_window_row_count")) == 0
            and _normalize_int(archive_snapshot.get("rolling_archive_window_row_count")) == exact_matched_window_rows
        )
        if (
            not blockers and current_cutoff_advances and hot_before_active == 0
            and exact_matched_window_rows > 0
            and _normalize_int(match_snapshot.get("hot_unarchived_window_row_count")) == 0
            and not window_fully_materialized
        ):
            blockers.append("rank rolling cold window contains rows without current hot copy evidence")
        if not blockers and window_fully_materialized:
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path, active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date, page_limit=effective_page_limit,
            )
            publish_binding(current_cutoff_date, "rolling_window_materialized", materialized=True)
        if not blockers and window_fully_materialized and committed_binding_cutoff == current_cutoff_date:
            hot_delete_result = _execute_rolling_hot_delete_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                active_cutoff_date=active_cutoff_date,
                current_cutoff_date=current_cutoff_date,
                columns=selected_columns,
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
                columns=selected_columns,
            )
            if not primary_snapshot.get("available") or archive_snapshot.get("archive_inspection_error") or match_snapshot.get("inspection_error"):
                blockers.append("rank post-delete inspection failed")

    if (
        not blockers and requested_mode == "rolling_archive"
        and (stale_hot_delete_result.get("delete_sql_executed")
             or stale_hot_archive_reconcile_result.get("archive_sql_executed")
             or hot_delete_result.get("delete_sql_executed") or recovered_checkpoint
             or ("live_cold_row_count" in active_summary
                 and active_summary.get("live_cold_row_count") != primary_snapshot.get("hot_before_active_cutoff_row_count")))
    ):
        if (
            current_cutoff_advances
            and primary_snapshot.get("hot_before_current_cutoff_row_count") == 0
            and archive_snapshot.get("rolling_archive_window_row_count") == 0
        ):
            committed_binding_cutoff = current_cutoff_date
        publish_binding(
            committed_binding_cutoff, "rolling_page_checkpoint",
            materialized=bool(updated_active_binding_payload.get("summary", {}).get("archive_materialized_and_reconciled")),
        )

    post_hot_before_current = _normalize_int(primary_snapshot.get("hot_before_current_cutoff_row_count"))
    post_hot_before_active = _normalize_int(primary_snapshot.get("hot_before_active_cutoff_row_count"))
    post_rolling_hot_candidate_rows = _normalize_int(primary_snapshot.get("rolling_hot_candidate_row_count"))
    post_archive_before_current_rows = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_row_count"))
    post_archive_before_current_funds = _normalize_int(archive_snapshot.get("archive_before_current_cutoff_fund_count"))
    post_archive_before_active_rows = _normalize_int(
        archive_snapshot.get("archive_before_active_cutoff_row_count")
    )
    post_archive_before_active_funds = _normalize_int(
        archive_snapshot.get("archive_before_active_cutoff_fund_count")
    )
    post_hot_unarchived_window_rows = _normalize_int(match_snapshot.get("hot_unarchived_window_row_count"))
    post_exact_matched_window_rows = _normalize_int(match_snapshot.get("exact_matched_window_row_count"))

    rolling_completion_evidence = bool(
        (current_cutoff_advances or stale_hot_repair_required)
        and post_hot_before_current == 0
        and (
            rolling_hot_candidate_rows > 0
            or exact_matched_window_rows > 0
            or bool(stale_hot_archive_reconcile_result.get("archive_sql_executed"))
            or bool(archive_export_result.get("archive_sql_executed"))
            or bool(stale_hot_delete_result.get("stale_hot_delete_sql_executed"))
            or bool(hot_delete_result.get("hot_delete_sql_executed"))
            or bool(recovered_checkpoint)
        )
    )

    if blockers:
        runner_status = "rolling_archive_blocked"
        auto_governance_stage = "rank_rolling_archive_blocked"
    elif requested_mode == "dry_run" and rolling_required:
        runner_status = "rolling_archive_ready"
        auto_governance_stage = "rank_rolling_archive_ready"
    elif requested_mode == "dry_run":
        runner_status = "rolling_archive_not_required"
        auto_governance_stage = "rank_hot_cold_binding_active"
    elif rolling_completion_evidence and current_cutoff_date is not None:
        runner_status = "rolling_archive_complete_binding_ready"
        auto_governance_stage = "rank_rolling_archive_complete_binding_ready"
    elif (
        bool(archive_export_result.get("archive_sql_executed"))
        or bool(stale_hot_archive_reconcile_result.get("archive_sql_executed"))
        or bool(stale_hot_delete_result.get("stale_hot_delete_sql_executed"))
        or bool(hot_delete_result.get("hot_delete_sql_executed"))
    ):
        runner_status = "rolling_archive_in_progress"
        auto_governance_stage = "rank_rolling_archive_in_progress"
    else:
        runner_status = "rolling_archive_not_required"
        auto_governance_stage = "rank_hot_cold_binding_active"

    if (
        not blockers
        and requested_mode == "rolling_archive"
        and current_cutoff_advances
        and post_hot_before_current == 0
        and not rolling_completion_evidence
    ):
        review_items.append(
            "current cutoff only crossed an empty rank-date window; active binding was left unchanged."
        )

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "dry_run_only": requested_mode == "dry_run",
        "dry_run_safe": not blockers,
        "active_binding_available": active_binding_available,
        "rolling_archive_supported": True,
        "rolling_archive_required": rolling_required,
        "rolling_archive_executed": bool(
            stale_hot_archive_reconcile_result.get("archive_sql_executed")
            or archive_export_result.get("archive_sql_executed")
            or stale_hot_delete_result.get("stale_hot_delete_sql_executed")
            or hot_delete_result.get("hot_delete_sql_executed")
        ),
        "archive_sql_executed": bool(
            stale_hot_archive_reconcile_result.get("archive_sql_executed")
            or archive_export_result.get("archive_sql_executed")
        ),
        "hot_delete_sql_executed": bool(
            stale_hot_delete_result.get("stale_hot_delete_sql_executed")
            or hot_delete_result.get("hot_delete_sql_executed")
        ),
        "delete_sql_executed": bool(
            stale_hot_delete_result.get("delete_sql_executed")
            or hot_delete_result.get("delete_sql_executed")
        ),
        "vacuum_executed": False,
        "active_cutoff_date": active_cutoff_text,
        "rolling_archive_cutoff_date": current_cutoff_text,
        "archive_cutoff_date": committed_binding_cutoff.isoformat() if committed_binding_cutoff else active_cutoff_text,
        "current_cutoff_advances": current_cutoff_advances,
        "current_cutoff_source": (
            "governance_payload"
            if _normalize_text(governance_summary.get("archive_cutoff_date"))
            else _normalize_text(inferred_cutoff.get("status") or "inferred")
        ),
        "current_cutoff_inference_error": inferred_cutoff.get("error"),
        "latest_rank_date": _normalize_text(
            governance_summary.get("latest_rank_date") or inferred_cutoff.get("latest_rank_date")
        ),
        "active_db_path": _stringify_project_path(active_db_path, root),
        "active_db_absolute_path": str(active_db_path or ""),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "archive_table_name": archive_table_name,
        "active_db_quick_check": active_db_quick_check,
        "archive_quick_check": archive_quick_check,
        "quick_check_required": quick_check_required,
        "selected_archive_columns": selected_columns,
        "page_limit": effective_page_limit,
        "expected_active_archive_row_count": expected_active_archive_rows,
        "expected_active_archive_fund_count": expected_active_archive_funds,
        "recovered_reconcile_checkpoint": recovered_checkpoint,
        "hot_before_active_cutoff_row_count": post_hot_before_active,
        "stale_hot_repair_required": stale_hot_repair_required,
        "stale_hot_page_limit": min(
            max(1, effective_page_limit),
            STALE_HOT_REPAIR_PAGE_LIMIT,
        ),
        "stale_hot_inspected_row_count": stale_hot_inspected_rows,
        "stale_hot_inspected_min_rank_date": _normalize_text(
            stale_hot_snapshot.get("stale_hot_inspected_min_rank_date")
        ),
        "stale_hot_inspected_max_rank_date": _normalize_text(
            stale_hot_snapshot.get("stale_hot_inspected_max_rank_date")
        ),
        "stale_hot_exact_delete_supported": stale_hot_exact_delete_supported,
        "stale_hot_archive_reconcile_supported": stale_hot_archive_reconcile_supported,
        "stale_hot_missing_archive_supported": stale_hot_archive_reconcile_supported,
        "stale_hot_exact_matched_row_count": stale_hot_exact_matched_rows,
        "stale_hot_missing_archive_key_row_count": stale_hot_missing_archive_key_rows,
        "stale_hot_missing_archive_candidate_row_count": stale_hot_missing_archive_candidate_rows,
        "stale_hot_missing_archive_unique_key_count": stale_hot_missing_archive_unique_key_count,
        "stale_hot_missing_archive_duplicate_key_count": stale_hot_missing_archive_duplicate_key_count,
        "stale_hot_missing_archive_duplicate_key_row_count": stale_hot_missing_archive_duplicate_key_rows,
        "stale_hot_missing_archive_invalid_fcode_row_count": stale_hot_missing_archive_invalid_fcode_rows,
        "stale_hot_missing_archive_invalid_rank_date_row_count": stale_hot_missing_archive_invalid_rank_date_rows,
        "stale_hot_key_conflict_row_count": stale_hot_key_conflict_rows,
        "stale_hot_newer_conflict_row_count": stale_hot_newer_conflict_rows,
        "stale_hot_unsafe_conflict_row_count": stale_hot_unsafe_conflict_rows,
        "stale_hot_archive_reconcile_result_status": (
            stale_hot_archive_reconcile_result.get("status") or ""
        ),
        "stale_hot_archive_reconcile_selected_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("selected_row_count")
        ),
        "stale_hot_archive_reconcile_exact_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("exact_row_count")
        ),
        "stale_hot_archive_reconcile_conflict_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("conflict_row_count")
        ),
        "stale_hot_archive_reconcile_newer_conflict_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("newer_conflict_row_count")
        ),
        "stale_hot_archive_reconcile_unsafe_conflict_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("unsafe_conflict_row_count")
        ),
        "stale_hot_archive_reconcile_revision_audit_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("revision_audit_row_count")
        ),
        "stale_hot_archive_reconcile_updated_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("updated_row_count")
        ),
        "stale_hot_archive_reconcile_missing_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("missing_row_count")
        ),
        "stale_hot_archive_reconcile_inserted_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("inserted_row_count")
        ),
        "stale_hot_archive_reconcile_replacement_inserted_row_count": _normalize_int(
            stale_hot_archive_reconcile_result.get("replacement_inserted_row_count")
        ),
        "stale_hot_delete_result_status": stale_hot_delete_result.get("status") or "",
        "stale_hot_delete_deleted_row_count": _normalize_int(
            stale_hot_delete_result.get("deleted_row_count")
        ),
        "hot_before_current_cutoff_row_count": post_hot_before_current,
        "rolling_archive_candidate_row_count": rolling_hot_candidate_rows,
        "rolling_archive_candidate_fund_count": rolling_hot_candidate_funds,
        "rolling_hot_candidate_row_count_after": post_rolling_hot_candidate_rows,
        "archive_before_active_cutoff_row_count": post_archive_before_active_rows,
        "archive_before_active_cutoff_fund_count": post_archive_before_active_funds,
        "archive_before_current_cutoff_row_count": post_archive_before_current_rows,
        "archive_before_current_cutoff_fund_count": post_archive_before_current_funds,
        "archive_out_of_scope_current_row_count": archive_out_of_scope_current_rows,
        "rolling_archive_window_row_count": _normalize_int(
            archive_snapshot.get("rolling_archive_window_row_count")
        ),
        "hot_unarchived_window_row_count": post_hot_unarchived_window_rows,
        "exact_matched_window_row_count": post_exact_matched_window_rows,
        "key_conflict_window_row_count": key_conflict_window_rows,
        "duplicate_rolling_business_key_count": duplicate_rolling_keys,
        "rolling_archive_result_status": archive_export_result.get("status") or "",
        "rolling_archive_inserted_row_count": _normalize_int(
            archive_export_result.get("inserted_row_count")
        ),
        "rolling_hot_delete_result_status": hot_delete_result.get("status") or "",
        "rolling_hot_delete_deleted_row_count": _normalize_int(
            hot_delete_result.get("deleted_row_count")
        ),
        "updated_active_binding_ready": bool(committed_binding_artifacts),
        "active_binding_committed_before_window_delete": bool(
            hot_delete_result.get("delete_sql_executed") and committed_binding_artifacts
            and committed_binding_cutoff == current_cutoff_date
        ),
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "source_active_apply_runner_artifact": str(active_artifact_path or ""),
    }

    # Unavailable inspections are unknown, while an unexecuted mutation really
    # has zero affected rows. Keep those two cases distinct in CLI/task output.
    if not primary_snapshot.get("available"):
        for key in (
            "hot_before_active_cutoff_row_count", "hot_before_current_cutoff_row_count",
            "rolling_archive_candidate_row_count", "rolling_archive_candidate_fund_count",
            "rolling_hot_candidate_row_count_after", "duplicate_rolling_business_key_count",
            "rolling_archive_required", "stale_hot_repair_required",
        ):
            summary[key] = None
    if not archive_snapshot.get("archive_table_exists") or archive_snapshot.get("archive_inspection_error"):
        for key in (
            "archive_before_active_cutoff_row_count", "archive_before_active_cutoff_fund_count",
            "archive_before_current_cutoff_row_count", "archive_before_current_cutoff_fund_count",
            "archive_out_of_scope_current_row_count", "rolling_archive_window_row_count",
        ):
            summary[key] = None
    if match_snapshot.get("inspection_error") or not current_cutoff_date:
        for key in ("hot_unarchived_window_row_count", "exact_matched_window_row_count", "key_conflict_window_row_count"):
            summary[key] = None
    if stale_hot_snapshot.get("inspection_error") or not primary_snapshot.get("available"):
        for key in (
            "stale_hot_inspected_row_count", "stale_hot_exact_matched_row_count",
            "stale_hot_missing_archive_key_row_count", "stale_hot_key_conflict_row_count",
            "stale_hot_newer_conflict_row_count", "stale_hot_unsafe_conflict_row_count",
        ):
            summary[key] = None

    findings: list[str] = []
    if blockers and stale_hot_delete_result.get("stale_hot_delete_sql_executed"):
        findings.append(
            "rank rolling archive deleted exact stale hot rows, but unresolved stale hot rows remain blocked."
        )
        findings.extend(blockers[:5])
    elif blockers:
        findings.append(f"rank rolling archive blocked with {len(blockers)} blockers.")
        findings.extend(blockers[:5])
    elif runner_status == "rolling_archive_ready":
        findings.append(
            f"rank rolling archive is ready for {rolling_hot_candidate_rows} hot rows before {current_cutoff_text}."
        )
    elif runner_status == "rolling_archive_complete_binding_ready":
        findings.append(
            "rank rolling archive completed; hot rows before the current cutoff are now zero and the active binding can be advanced."
        )
    elif runner_status == "rolling_archive_in_progress":
        findings.append(
            f"rank rolling archive advanced one page; {post_hot_before_current} hot rows remain before {current_cutoff_text}."
        )
    else:
        findings.append("rank rolling archive is not required for the current retention cutoff.")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": active_binding_available,
        "summary": summary,
        "source_active_rank_history_archive_apply_runner_artifact": str(active_artifact_path or ""),
        "source_active_rank_history_archive_apply_runner_summary": active_summary,
        "blockers": blockers,
        "review_items": review_items,
        "gate_checks": gates,
        "primary_snapshot": primary_snapshot,
        "archive_snapshot": archive_snapshot,
        "match_snapshot": match_snapshot,
        "stale_hot_evidence_snapshot": stale_hot_snapshot,
        "stale_hot_archive_reconcile_result": stale_hot_archive_reconcile_result,
        "archive_export_result": archive_export_result,
        "stale_hot_delete_result": stale_hot_delete_result,
        "hot_delete_result": hot_delete_result,
        "updated_active_binding_payload": updated_active_binding_payload,
        "committed_active_binding": committed_binding_artifacts,
        "findings": findings,
        "safety": {
            "archive_sql_executed": bool(
                stale_hot_archive_reconcile_result.get("archive_sql_executed")
                or archive_export_result.get("archive_sql_executed")
            ),
            "delete_sql_executed": bool(
                stale_hot_delete_result.get("delete_sql_executed")
                or hot_delete_result.get("delete_sql_executed")
            ),
            "vacuum_executed": False,
            "delete_requires_exact_archive_match": True,
            "stale_hot_delete_requires_exact_archive_match": True,
            "stale_hot_archive_reconcile_commits_before_hot_delete": True,
            "stale_hot_conflict_revision_audit_preserved": True,
            "stale_hot_conflict_hot_values_are_newer_source": True,
            "stale_hot_missing_archive_auto_insert_supported": True,
            "stale_hot_missing_archive_no_synthetic_rank_values": True,
            "archive_target_is_existing_cumulative_binding": archive_exists,
        },
    }


def render_rank_history_archive_rolling_runner_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# rank history archive rolling runner",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- runner_status: {summary.get('runner_status', '')}",
        f"- auto_governance_stage: {summary.get('auto_governance_stage', '')}",
        f"- mode: {summary.get('mode', '')}",
        f"- active_cutoff_date: {summary.get('active_cutoff_date', '')}",
        f"- rolling_archive_cutoff_date: {summary.get('rolling_archive_cutoff_date', '')}",
        f"- stale_hot_repair_required: {summary.get('stale_hot_repair_required', False)}",
        f"- stale_hot_page_limit: {summary.get('stale_hot_page_limit', 0)}",
        f"- stale_hot_inspected_row_count: {summary.get('stale_hot_inspected_row_count', 0)}",
        f"- stale_hot_archive_reconcile_supported: {summary.get('stale_hot_archive_reconcile_supported', False)}",
        f"- stale_hot_archive_reconcile_selected_row_count: {summary.get('stale_hot_archive_reconcile_selected_row_count', 0)}",
        f"- stale_hot_archive_reconcile_conflict_row_count: {summary.get('stale_hot_archive_reconcile_conflict_row_count', 0)}",
        f"- stale_hot_archive_reconcile_newer_conflict_row_count: {summary.get('stale_hot_archive_reconcile_newer_conflict_row_count', 0)}",
        f"- stale_hot_archive_reconcile_unsafe_conflict_row_count: {summary.get('stale_hot_archive_reconcile_unsafe_conflict_row_count', 0)}",
        f"- stale_hot_archive_reconcile_revision_audit_row_count: {summary.get('stale_hot_archive_reconcile_revision_audit_row_count', 0)}",
        f"- stale_hot_archive_reconcile_inserted_row_count: {summary.get('stale_hot_archive_reconcile_inserted_row_count', 0)}",
        f"- stale_hot_archive_reconcile_replacement_inserted_row_count: {summary.get('stale_hot_archive_reconcile_replacement_inserted_row_count', 0)}",
        f"- stale_hot_exact_matched_row_count: {summary.get('stale_hot_exact_matched_row_count', 0)}",
        f"- stale_hot_delete_deleted_row_count: {summary.get('stale_hot_delete_deleted_row_count', 0)}",
        f"- stale_hot_missing_archive_key_row_count: {summary.get('stale_hot_missing_archive_key_row_count', 0)}",
        f"- stale_hot_key_conflict_row_count: {summary.get('stale_hot_key_conflict_row_count', 0)}",
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


def write_rank_history_archive_rolling_runner_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"rank_history_archive_rolling_runner_{ts}.json"
    md_path = artifact_dir / f"rank_history_archive_rolling_runner_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_rank_history_archive_rolling_runner_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
