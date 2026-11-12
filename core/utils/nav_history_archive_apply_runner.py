#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Strict runner for NAV hot/cold archive governance."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_cold_query_compat_audit import (
    DEFAULT_OUTPUT_DIR,
    NAV_HISTORY_ARCHIVE_COLD_QUERY_COMPAT_AUDIT_JSON_PATTERN,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    NAV_HISTORY_ARCHIVE_MAINTENANCE_READINESS_AUDIT_JSON_PATTERN,
)
from utils.nav_history_archive_runbook import NAV_HISTORY_ARCHIVE_RUNBOOK_JSON_PATTERN


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN = "nav_history_archive_apply_runner_*.json"
DEFAULT_ARCHIVE_PAGE_LIMIT = 100_000
ALLOWED_MODES = {"dry_run", "archive_export"}
ARCHIVE_EXPORT_HOT_DB_ALIAS = "hot_db"
REQUIRED_NAV_ARCHIVE_COLUMNS = ("fcode", "nav_date", "unit_nav", "accum_nav", "daily_growth")


def load_latest_nav_history_archive_apply_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    """Load the latest NAV archive apply runner artifact."""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


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


def _normalize_table_name(value: Any) -> str:
    text = _normalize_text(value) or "fund_nav_history_archive"
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text):
        return "fund_nav_history_archive"
    return text


def _quote_sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _sqlite_readonly_uri(path: Path) -> str:
    # SQLite file: URI needs URL escaping for paths with spaces or non-ASCII chars.
    return f"file:{quote(str(path.resolve()))}?mode=ro"


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


def _load_json_path(path_text: str | Path, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return path, payload


def _load_latest_payload(
    *,
    pattern: str,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Path, dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    candidates = sorted(resolved_output_dir.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No matching artifact: {resolved_output_dir / pattern}")
    path = candidates[-1]
    payload = json.loads(path.read_text(encoding="utf-8"))
    return path, payload


def _open_readonly_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_readonly_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    return conn


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
    result = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            result.append(dict(zip(columns, row)))
    return result


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> list[dict]:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()
    columns = [item[0] for item in cursor.description or []]
    result = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            result.append(dict(zip(columns, row)))
    return result


def _sqlite_quick_check(path: Path) -> str:
    try:
        with _open_readonly_db(path) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0] if row else "").strip()
    except sqlite3.Error as exc:
        return f"error:{exc}"


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


def _archive_table_schema_valid(conn: sqlite3.Connection, table_name: str) -> bool:
    columns = {str(item.get("name") or "").strip() for item in _table_columns(conn, table_name)}
    return set(REQUIRED_NAV_ARCHIVE_COLUMNS).issubset(columns)


def _execute_archive_export_page(
    *,
    db_path: Path,
    archive_target_path: Path,
    archive_table_name: str,
    cutoff_date: date,
    page_limit: int,
) -> dict:
    """Export one missing cold-data page into the archive DB.

    This mutates only the archive target. The hot database is attached read-only
    and only read by the INSERT...SELECT statement.
    """
    started_at = datetime.now()
    if page_limit <= 0:
        return {
            "status": "skipped",
            "skip_reason": "page_limit_not_positive",
            "archive_sql_executed": False,
        }
    archive_target_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = _normalize_table_name(archive_table_name)
    cutoff_text = cutoff_date.isoformat()
    hot_uri = _sqlite_readonly_uri(db_path)

    conn = sqlite3.connect(str(archive_target_path), uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute(
            f"ATTACH DATABASE ? AS {ARCHIVE_EXPORT_HOT_DB_ALIAS}",
            (hot_uri,),
        )
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

        existing_before = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE nav_date < ?",
                (cutoff_text,),
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
            FROM {ARCHIVE_EXPORT_HOT_DB_ALIAS}.fund_nav_history AS hot
            WHERE hot.nav_date < ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM {table_name} AS archived
                  WHERE archived.fcode = hot.fcode
                    AND archived.nav_date = hot.nav_date
              )
            ORDER BY hot.nav_date, hot.fcode
            LIMIT ?
            """,
            (cutoff_text, int(page_limit)),
        )
        existing_after = _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE nav_date < ?",
                (cutoff_text,),
            ).fetchone()[0]
        )
        sample_after = _fetch_all_dicts(
            conn,
            f"""
            SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth
            FROM {table_name}
            WHERE nav_date < ?
            ORDER BY nav_date DESC, fcode DESC
            LIMIT 20
            """,
            (cutoff_text,),
        )
        conn.execute("COMMIT")
        return {
            "status": "executed",
            "archive_sql_executed": True,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "page_limit": int(page_limit),
            "existing_before": existing_before,
            "existing_after": existing_after,
            "inserted_row_count": max(existing_after - existing_before, 0),
            "sample_after": sample_after,
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
                conn.execute(f"DETACH DATABASE {ARCHIVE_EXPORT_HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()


def _build_gate(
    key: str,
    label: str,
    status: str,
    detail: str,
    action_hint: str = "",
) -> dict:
    return {
        "key": key,
        "label": label,
        "status": status,
        "detail": detail,
        "action_hint": action_hint,
    }


def _build_findings(
    *,
    mode: str,
    runner_status: str,
    blockers: list[str],
    review_items: list[str],
    archive_materialized_and_reconciled: bool,
    archive_binding_active: bool,
    live_cold_row_count: int,
    archive_export_result: Optional[dict] = None,
) -> list[str]:
    export_result = _normalize_dict(archive_export_result)
    if blockers:
        return [
            f"NAV 归档治理存在 {len(blockers)} 个阻断项，当前不会进入后续删除或 VACUUM。",
            *blockers[:4],
        ]
    if review_items:
        return [
            f"NAV 归档治理存在 {len(review_items)} 个复核项，当前仅输出执行证据。",
            *review_items[:4],
        ]
    if archive_binding_active:
        return [
            "NAV 归档库已与热库删除后状态对齐，可作为运行期 cold binding 候选。"
        ]
    if archive_materialized_and_reconciled and live_cold_row_count > 0:
        return [
            "归档库计数已与预期冷数据一致，但热库冷数据仍存在；运行期不会把该归档库作为 cold binding，避免重复读取。"
        ]
    if mode == "archive_export" and export_result.get("status") == "executed":
        return [
            "已向独立 NAV 归档库执行 page-limited archive export；未删除热库数据，未执行 VACUUM。",
            (
                f"本页写入 {export_result.get('inserted_row_count', 0)} 行，"
                f"归档库当前冷数据 {export_result.get('existing_after', 0)} 行。"
            ),
        ]
    return [
        f"NAV 归档治理 dry-run 已通过严格门禁，runner_status={runner_status}；本次未执行归档、删除或 VACUUM。"
    ]


def _inspect_primary_db(
    *,
    db_path: Path,
    cutoff_date: date,
    page_limit: int,
) -> dict:
    if not db_path.exists() or not db_path.is_file():
        return {
            "available": False,
            "error": f"primary database missing: {db_path}",
        }

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
        cold = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS live_cold_row_count,
                COUNT(DISTINCT fcode) AS live_cold_fund_count,
                MIN(nav_date) AS live_cold_min_nav_date,
                MAX(nav_date) AS live_cold_max_nav_date
            FROM fund_nav_history
            WHERE nav_date < ?
            """,
            (cutoff_date.isoformat(),),
        )
        hot_after = _fetch_one_dict(
            conn,
            """
            SELECT
                COUNT(*) AS hot_retained_row_count,
                COUNT(DISTINCT fcode) AS hot_retained_fund_count
            FROM fund_nav_history
            WHERE nav_date >= ?
            """,
            (cutoff_date.isoformat(),),
        )
        sample_rows = _fetch_all_dicts(
            conn,
            """
            SELECT
                fcode,
                MIN(nav_date) AS first_nav_date,
                MAX(nav_date) AS last_nav_date,
                COUNT(*) AS row_count
            FROM fund_nav_history
            WHERE nav_date < ?
            GROUP BY fcode
            ORDER BY row_count DESC, fcode
            LIMIT 20
            """,
            (cutoff_date.isoformat(),),
        )
        page_rows = []
        if page_limit > 0:
            page_rows = _fetch_all_dicts(
                conn,
                """
                SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth
                FROM fund_nav_history
                WHERE nav_date < ?
                ORDER BY nav_date, fcode
                LIMIT ?
                """,
                (cutoff_date.isoformat(), min(int(page_limit), 20)),
            )
        page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = _normalize_int(conn.execute("PRAGMA freelist_count").fetchone()[0])

    return {
        "available": True,
        **total,
        **cold,
        **hot_after,
        "sample_cold_funds": sample_rows,
        "page_preview_rows": page_rows,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "database_size_mb": _round_mb(db_path.stat().st_size),
    }


def _inspect_archive_db(
    *,
    archive_target_path: Optional[Path],
    archive_table_name: str,
    cutoff_date: date,
) -> dict:
    archive_table_name = _normalize_table_name(archive_table_name)
    if archive_target_path is None:
        return {"archive_target_exists": False, "archive_table_exists": False}
    if not archive_target_path.exists() or not archive_target_path.is_file():
        return {
            "archive_target_exists": False,
            "archive_table_exists": False,
            "archive_target_size_mb": 0.0,
        }

    payload = {
        "archive_target_exists": True,
        "archive_target_size_mb": _round_mb(archive_target_path.stat().st_size),
        "archive_table_exists": False,
    }
    try:
        with _open_readonly_db(archive_target_path) as conn:
            table_exists = _table_exists(conn, archive_table_name)
            payload["archive_table_exists"] = table_exists
            if not table_exists:
                return payload
            schema_valid = _archive_table_schema_valid(conn, archive_table_name)
            payload["archive_table_schema_valid"] = schema_valid
            if not schema_valid:
                return payload
            counts = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS existing_archive_row_count,
                    COUNT(DISTINCT fcode) AS existing_archive_fund_count,
                    MIN(nav_date) AS existing_archive_min_nav_date,
                    MAX(nav_date) AS existing_archive_max_nav_date
                FROM {archive_table_name}
                WHERE nav_date < ?
                """,
                (cutoff_date.isoformat(),),
            )
            total = _fetch_one_dict(
                conn,
                f"SELECT COUNT(*) AS existing_archive_total_row_count FROM {archive_table_name}",
            )
            out_of_scope = _fetch_one_dict(
                conn,
                f"SELECT COUNT(*) AS archive_out_of_scope_row_count FROM {archive_table_name} WHERE nav_date >= ?",
                (cutoff_date.isoformat(),),
            )
            payload.update(counts)
            payload.update(total)
            payload.update(out_of_scope)
    except sqlite3.Error as exc:
        payload["archive_inspection_error"] = str(exc)
    return payload


def _derive_archive_state(
    *,
    archive_snapshot: dict,
    expected_archive_row_count: int,
    expected_archive_fund_count: int,
    live_cold_row_count: int,
) -> dict:
    existing_archive_row_count = _normalize_int(archive_snapshot.get("existing_archive_row_count"))
    existing_archive_fund_count = _normalize_int(archive_snapshot.get("existing_archive_fund_count"))
    archive_out_of_scope_row_count = _normalize_int(
        archive_snapshot.get("archive_out_of_scope_row_count")
    )
    archive_target_exists = bool(archive_snapshot.get("archive_target_exists"))
    archive_table_exists = bool(archive_snapshot.get("archive_table_exists"))
    archive_table_schema_valid = (
        bool(archive_snapshot.get("archive_table_schema_valid"))
        if archive_table_exists
        else True
    )
    archive_inspection_error = _normalize_text(archive_snapshot.get("archive_inspection_error"))
    archive_row_count_over_expected = bool(
        expected_archive_row_count > 0
        and existing_archive_row_count > expected_archive_row_count
    )
    archive_fund_count_over_expected = bool(
        expected_archive_fund_count > 0
        and existing_archive_fund_count > expected_archive_fund_count
    )
    archive_materialized_and_reconciled = (
        archive_target_exists
        and archive_table_exists
        and archive_table_schema_valid
        and expected_archive_row_count > 0
        and existing_archive_row_count == expected_archive_row_count
        and archive_out_of_scope_row_count == 0
        and (
            expected_archive_fund_count <= 0
            or existing_archive_fund_count == expected_archive_fund_count
        )
    )
    archive_partial_conflict = bool(
        archive_inspection_error
        or (archive_table_exists and not archive_table_schema_valid)
        or archive_out_of_scope_row_count > 0
        or archive_row_count_over_expected
        or archive_fund_count_over_expected
    )
    archive_export_in_progress = bool(
        archive_target_exists
        and archive_table_exists
        and archive_table_schema_valid
        and existing_archive_row_count > 0
        and expected_archive_row_count > 0
        and existing_archive_row_count < expected_archive_row_count
        and archive_out_of_scope_row_count == 0
        and not archive_partial_conflict
    )
    archive_binding_active = bool(
        archive_materialized_and_reconciled and live_cold_row_count == 0
    )
    return {
        "existing_archive_row_count": existing_archive_row_count,
        "existing_archive_fund_count": existing_archive_fund_count,
        "archive_out_of_scope_row_count": archive_out_of_scope_row_count,
        "archive_target_exists": archive_target_exists,
        "archive_table_exists": archive_table_exists,
        "archive_table_schema_valid": archive_table_schema_valid,
        "archive_inspection_error": archive_inspection_error,
        "archive_row_count_over_expected": archive_row_count_over_expected,
        "archive_fund_count_over_expected": archive_fund_count_over_expected,
        "archive_materialized_and_reconciled": archive_materialized_and_reconciled,
        "archive_partial_conflict": archive_partial_conflict,
        "archive_export_in_progress": archive_export_in_progress,
        "archive_binding_active": archive_binding_active,
        "archive_missing_row_count": max(expected_archive_row_count - existing_archive_row_count, 0),
    }


def build_nav_history_archive_binding_health_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    governance_payload: Optional[dict] = None,
    mode: str = "dry_run",
    page_limit: int = DEFAULT_ARCHIVE_PAGE_LIMIT,
) -> dict:
    """Build post-split health evidence for an already active NAV cold binding.

    This path is intentionally non-mutating. Once a cold binding is active,
    weekly maintenance should prove the split remains healthy instead of
    regenerating a pre-delete runbook from the post-delete hot database.
    """
    root = _resolve_project_root(project_root)
    active_artifact_path, active_payload = _load_latest_active_apply_runner_payload(
        output_dir=output_dir,
        project_root=root,
    )
    active_summary = _normalize_dict(active_payload.get("summary")) if active_payload else {}
    if not active_artifact_path or not active_summary:
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "available": False,
            "message": "No active NAV archive binding artifact found.",
            "source_active_nav_history_archive_apply_runner_artifact": "",
            "source_active_nav_history_archive_apply_runner_generated_at": "",
            "source_active_nav_history_archive_apply_runner_summary": {},
            "summary": {
                "runner_status": "archive_binding_health_unavailable",
                "auto_governance_stage": "active_binding_not_found",
                "mode": _normalize_text(mode) or "dry_run",
                "dry_run_only": True,
                "dry_run_safe": True,
                "archive_binding_active": False,
                "source_active_apply_runner_artifact": "",
                "blocker_count": 0,
                "review_item_count": 0,
                "execution_mode": "binding_health_check_unavailable",
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
            },
            "gate_checks": [],
            "blockers": [],
            "review_items": [],
            "primary_snapshot": {},
            "archive_snapshot": {},
            "archive_export_result": {
                "status": "not_executed",
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
            },
            "page_preview": {"page_limit": max(0, int(page_limit or 0)), "sample_rows": []},
            "execution_plan": [],
            "findings": ["当前尚无 active cold binding 证据，继续走归档准备/导出链路。"],
            "safety": {
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
            },
        }
    requested_mode = _normalize_text(mode) or "dry_run"
    effective_page_limit = max(0, int(page_limit or 0))
    governance_payload = dict(governance_payload or {})
    governance_summary = _normalize_dict(governance_payload.get("summary"))
    governance_strategy = _normalize_dict(
        governance_payload.get("nav_history_archive_strategy_preview")
    )

    active_cutoff_date_text = _normalize_text(active_summary.get("archive_cutoff_date"))
    active_cutoff_date = _parse_iso_date(active_cutoff_date_text)
    archive_table_name = _normalize_table_name(
        active_summary.get("archive_table_name") or "fund_nav_history_archive"
    )
    archive_target_path = _resolve_path(
        active_summary.get("archive_target_absolute_path")
        or active_summary.get("archive_target_path"),
        root,
    )
    active_db_path = _resolve_path(
        db_path or active_summary.get("active_db_path") or "backend/data/bond_funds.db",
        root,
    )
    expected_archive_row_count = _normalize_int(
        active_summary.get("existing_archive_row_count")
        or active_summary.get("expected_archive_row_count")
    )
    expected_archive_fund_count = _normalize_int(
        active_summary.get("existing_archive_fund_count")
        or active_summary.get("expected_archive_fund_count")
    )

    current_cutoff_date_text = _normalize_text(
        governance_summary.get("archive_cutoff_date")
        or governance_strategy.get("archive_cutoff_date")
    )
    current_cutoff_date = _parse_iso_date(current_cutoff_date_text)
    current_cold_row_count = _normalize_int(
        governance_summary.get("estimated_archive_row_count")
        or governance_strategy.get("estimated_archive_row_count")
    )
    current_cold_fund_count = _normalize_int(
        governance_summary.get("estimated_archive_fund_count")
        or governance_strategy.get("estimated_archive_fund_count")
    )
    rolling_archive_required = bool(
        active_cutoff_date is not None
        and current_cutoff_date is not None
        and current_cutoff_date > active_cutoff_date
        and current_cold_row_count > 0
    )

    primary_snapshot: dict = {"available": False}
    archive_snapshot: dict = {}
    if active_cutoff_date is not None and active_db_path is not None:
        try:
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path,
                cutoff_date=active_cutoff_date,
                page_limit=effective_page_limit,
            )
        except sqlite3.Error as exc:
            primary_snapshot = {"available": False, "error": str(exc)}
    if active_cutoff_date is not None:
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=active_cutoff_date,
        )

    live_cold_row_count = _normalize_int(primary_snapshot.get("live_cold_row_count"))
    live_cold_fund_count = _normalize_int(primary_snapshot.get("live_cold_fund_count"))
    archive_state = _derive_archive_state(
        archive_snapshot=archive_snapshot,
        expected_archive_row_count=expected_archive_row_count,
        expected_archive_fund_count=expected_archive_fund_count,
        live_cold_row_count=live_cold_row_count,
    )
    existing_archive_row_count = _normalize_int(archive_state.get("existing_archive_row_count"))
    existing_archive_fund_count = _normalize_int(archive_state.get("existing_archive_fund_count"))
    archive_out_of_scope_row_count = _normalize_int(
        archive_state.get("archive_out_of_scope_row_count")
    )
    archive_target_exists = bool(archive_state.get("archive_target_exists"))
    archive_table_exists = bool(archive_state.get("archive_table_exists"))
    archive_table_schema_valid = bool(archive_state.get("archive_table_schema_valid"))
    archive_inspection_error = _normalize_text(archive_state.get("archive_inspection_error"))
    archive_materialized_and_reconciled = bool(
        archive_state.get("archive_materialized_and_reconciled")
    )
    archive_binding_active = bool(archive_state.get("archive_binding_active"))
    archive_partial_conflict = bool(archive_state.get("archive_partial_conflict"))
    active_db_exists = bool(active_db_path and active_db_path.exists() and active_db_path.is_file())
    active_db_quick_check = _sqlite_quick_check(active_db_path) if active_db_exists else ""
    archive_quick_check = (
        _sqlite_quick_check(archive_target_path)
        if archive_target_path and archive_target_path.exists() and archive_target_path.is_file()
        else ""
    )
    effective_hot_cold_row_count = (
        _normalize_int(primary_snapshot.get("total_nav_row_count")) + existing_archive_row_count
        if archive_binding_active
        else 0
    )

    gates = [
        _build_gate(
            "active_binding_artifact",
            "已生效绑定证据",
            "ok" if active_artifact_path and active_summary else "blocked",
            f"source_active_apply_runner_artifact={active_artifact_path or ''}",
            "归档后保活必须基于最近一次 archive_binding_active 的 apply runner 证据。",
        ),
        _build_gate(
            "active_cutoff",
            "已生效 cutoff",
            "ok" if active_cutoff_date is not None else "blocked",
            f"archive_cutoff_date={active_cutoff_date_text}",
            "已生效绑定证据必须包含可解析的 archive_cutoff_date。",
        ),
        _build_gate(
            "primary_database",
            "活动主库",
            "ok" if active_db_exists and primary_snapshot.get("available") else "blocked",
            (
                f"active_db_path={active_db_path}, "
                f"available={primary_snapshot.get('available')}, "
                f"error={primary_snapshot.get('error') or ''}"
            ),
            "必须能以只读方式打开活动主库并读取 fund_nav_history。",
        ),
        _build_gate(
            "primary_quick_check",
            "主库 quick_check",
            "ok" if active_db_quick_check.lower() == "ok" else "blocked",
            f"quick_check={active_db_quick_check}",
            "主库 quick_check 不为 ok 时，不能确认热冷拆分健康。",
        ),
        _build_gate(
            "archive_quick_check",
            "冷库 quick_check",
            "ok" if archive_quick_check.lower() == "ok" else "blocked",
            f"quick_check={archive_quick_check}",
            "冷库 quick_check 不为 ok 时，不能继续作为运行期 cold binding。",
        ),
        _build_gate(
            "hot_cold_reconciled",
            "热冷计数闭合",
            "ok" if archive_binding_active else "blocked",
            (
                f"live_cold_row_count={live_cold_row_count}, "
                f"existing_archive_row_count={existing_archive_row_count}, "
                f"expected_archive_row_count={expected_archive_row_count}, "
                f"out_of_scope_rows={archive_out_of_scope_row_count}, "
                f"schema_valid={archive_table_schema_valid}, "
                f"inspection_error={archive_inspection_error}"
            ),
            "已生效 cutoff 前热库必须为 0，冷库计数、schema 与越界检查必须闭合。",
        ),
        _build_gate(
            "rolling_archive_guard",
            "滚动归档保护",
            "review_required" if rolling_archive_required else "ok",
            (
                f"active_cutoff={active_cutoff_date_text}, "
                f"current_retention_cutoff={current_cutoff_date_text}, "
                f"rolling_candidate_rows={current_cold_row_count}"
            ),
            "已有 active binding 后，下一批滚动归档必须使用累计冷库合并/绑定策略，不能自动创建只含新增日的小冷库。",
        ),
    ]
    blockers = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "blocked"
    ]
    review_items = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "review_required"
    ]

    if blockers:
        runner_status = "archive_binding_health_blocked"
        auto_governance_stage = "hot_cold_binding_health_blocked"
    elif rolling_archive_required:
        runner_status = "archive_binding_active"
        auto_governance_stage = "hot_cold_binding_active_rolling_archive_pending"
    else:
        runner_status = "archive_binding_active"
        auto_governance_stage = "hot_cold_binding_active"

    archive_export_result = {
        "status": "skipped_existing_binding_active" if not blockers else "not_executed",
        "skip_reason": (
            "rolling_archive_guard_requires_cumulative_archive_runner"
            if rolling_archive_required
            else "archive_binding_health_check_only"
        ),
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "inserted_row_count": 0,
    }
    page_preview = {
        "page_limit": effective_page_limit,
        "page_candidate_row_count": 0,
        "sample_rows": [],
    }
    execution_plan = [
        {
            "step": "archive_export",
            "status": (
                "deferred_until_cumulative_rolling_archive_runner"
                if rolling_archive_required
                else "not_planned_existing_binding_active"
            ),
            "estimated_rows": current_cold_row_count if rolling_archive_required else 0,
            "page_limit": effective_page_limit,
            "last_inserted_row_count": 0,
            "sql_preview": [],
        },
        {
            "step": "delete_hot_cold_rows",
            "status": "not_planned_existing_binding_active",
            "estimated_rows": 0,
            "sql_preview": [],
        },
        {
            "step": "postcheck",
            "status": "executed_health_check",
            "sql_preview": ["PRAGMA quick_check;", "PRAGMA freelist_count;"],
        },
        {
            "step": "vacuum",
            "status": "defer_to_followup_window",
            "sql_preview": [],
        },
    ]
    if blockers:
        findings = [
            f"NAV 归档绑定保活存在 {len(blockers)} 个阻断项，当前不写入、不删除、不 VACUUM。",
            *blockers[:4],
        ]
    else:
        findings = [
            "NAV 归档绑定保活检查通过：已生效 cutoff 前热库为 0，冷库可读且 quick_check=ok。",
            f"当前运行期 hot+cold 有效 NAV 行数为 {effective_hot_cold_row_count}。",
        ]
        if rolling_archive_required:
            findings.append(
                "检测到下一批滚动冷数据候选；在累计冷库合并与绑定策略完成前，本任务不会自动写入新的归档库。"
            )

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "dry_run_only": True,
        "dry_run_safe": not blockers,
        "archive_export_supported": False,
        "archive_apply_supported": False,
        "delete_supported": False,
        "vacuum_supported": False,
        "archive_export_executed": False,
        "archive_export_result_status": archive_export_result.get("status") or "",
        "archive_export_inserted_row_count": 0,
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "archive_binding_active": archive_binding_active and not blockers,
        "archive_materialized_and_reconciled": archive_materialized_and_reconciled,
        "archive_partial_conflict": archive_partial_conflict,
        "archive_export_in_progress": False,
        "archive_out_of_scope_row_count": archive_out_of_scope_row_count,
        "archive_table_schema_valid": archive_table_schema_valid,
        "archive_inspection_error": archive_inspection_error,
        "active_db_quick_check": active_db_quick_check,
        "archive_quick_check": archive_quick_check,
        "readiness_status": "post_split_binding_health",
        "ready_for_manual_window_execution": False,
        "cold_query_compat_status": "post_split_binding_health",
        "query_compat_safe": archive_binding_active and not blockers,
        "safe_to_apply_archive_now": False,
        "production_uncovered_cold_dependency_count": 0,
        "archive_cutoff_date": active_cutoff_date_text,
        "expected_archive_row_count": expected_archive_row_count,
        "expected_archive_fund_count": expected_archive_fund_count,
        "live_cold_row_count": live_cold_row_count,
        "live_cold_fund_count": live_cold_fund_count,
        "live_cold_min_nav_date": primary_snapshot.get("live_cold_min_nav_date") or "",
        "live_cold_max_nav_date": primary_snapshot.get("live_cold_max_nav_date") or "",
        "live_counts_match_expected": live_cold_row_count == 0,
        "total_nav_row_count": _normalize_int(primary_snapshot.get("total_nav_row_count")),
        "total_nav_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "hot_retained_row_count": _normalize_int(primary_snapshot.get("hot_retained_row_count")),
        "hot_retained_fund_count": _normalize_int(primary_snapshot.get("hot_retained_fund_count")),
        "effective_hot_cold_row_count": effective_hot_cold_row_count,
        "active_db_path": str(active_db_path or ""),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "archive_target_exists": archive_target_exists,
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_table_name": archive_table_name,
        "archive_table_exists": archive_table_exists,
        "existing_archive_row_count": existing_archive_row_count,
        "existing_archive_fund_count": existing_archive_fund_count,
        "archive_missing_row_count": _normalize_int(archive_state.get("archive_missing_row_count")),
        "backup_artifact_path": _normalize_text(active_summary.get("backup_artifact_path")),
        "backup_artifact_absolute_path": _normalize_text(
            active_summary.get("backup_artifact_absolute_path")
        ),
        "backup_artifact_exists": bool(active_summary.get("backup_artifact_exists")),
        "page_limit": effective_page_limit,
        "page_preview_count": 0,
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "page_apply_supported": False,
        "execution_mode": "binding_health_check",
        "source_active_apply_runner_artifact": str(active_artifact_path or ""),
        "rolling_archive_required": rolling_archive_required,
        "rolling_archive_supported": False,
        "rolling_archive_cutoff_date": current_cutoff_date_text,
        "rolling_archive_candidate_row_count": current_cold_row_count,
        "rolling_archive_candidate_fund_count": current_cold_fund_count,
        "rolling_archive_blocked_reason": (
            "rolling_archive_cumulative_merge_not_implemented"
            if rolling_archive_required
            else ""
        ),
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": bool(active_artifact_path and active_summary),
        "message": "NAV archive binding health check completed.",
        "source_active_nav_history_archive_apply_runner_artifact": str(
            active_artifact_path or ""
        ),
        "source_active_nav_history_archive_apply_runner_generated_at": (
            active_payload.get("generated_at") or ""
        ),
        "source_active_nav_history_archive_apply_runner_summary": active_summary,
        "summary": summary,
        "gate_checks": gates,
        "blockers": blockers,
        "review_items": review_items,
        "primary_snapshot": {
            key: value
            for key, value in primary_snapshot.items()
            if key not in {"page_preview_rows"}
        },
        "archive_snapshot": archive_snapshot,
        "archive_export_result": archive_export_result,
        "page_preview": page_preview,
        "execution_plan": execution_plan,
        "findings": findings,
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
            "rolling_archive_auto_mutation_blocked": rolling_archive_required,
        },
    }


def build_nav_history_archive_apply_runner_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    readiness_payload: Optional[dict] = None,
    cold_query_compat_payload: Optional[dict] = None,
    runbook_payload: Optional[dict] = None,
    source_readiness_artifact: Optional[str] = None,
    source_cold_query_compat_artifact: Optional[str] = None,
    source_runbook_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    mode: str = "dry_run",
    page_limit: int = DEFAULT_ARCHIVE_PAGE_LIMIT,
) -> dict:
    """Build a strict dry-run artifact for NAV archive governance.

    This runner intentionally does not execute archive, delete, checkpoint, or
    VACUUM SQL. It consolidates the current gates and produces activation
    evidence for later automated execution work.
    """
    root = _resolve_project_root(project_root)
    resolved_output_dir = _resolve_output_dir(output_dir, root)
    if readiness_payload is None:
        if source_readiness_artifact:
            readiness_path, readiness_payload = _load_json_path(source_readiness_artifact, root)
        else:
            readiness_path, readiness_payload = _load_latest_payload(
                pattern=NAV_HISTORY_ARCHIVE_MAINTENANCE_READINESS_AUDIT_JSON_PATTERN,
                output_dir=resolved_output_dir,
                project_root=root,
            )
        source_readiness_artifact = str(readiness_path)
    if cold_query_compat_payload is None:
        if source_cold_query_compat_artifact:
            compat_path, cold_query_compat_payload = _load_json_path(
                source_cold_query_compat_artifact,
                root,
            )
        else:
            compat_path, cold_query_compat_payload = _load_latest_payload(
                pattern=NAV_HISTORY_ARCHIVE_COLD_QUERY_COMPAT_AUDIT_JSON_PATTERN,
                output_dir=resolved_output_dir,
                project_root=root,
            )
        source_cold_query_compat_artifact = str(compat_path)
    if runbook_payload is None:
        if source_runbook_artifact:
            runbook_path, runbook_payload = _load_json_path(source_runbook_artifact, root)
        else:
            runbook_path, runbook_payload = _load_latest_payload(
                pattern=NAV_HISTORY_ARCHIVE_RUNBOOK_JSON_PATTERN,
                output_dir=resolved_output_dir,
                project_root=root,
            )
        source_runbook_artifact = str(runbook_path)

    readiness_payload = dict(readiness_payload or {})
    cold_query_compat_payload = dict(cold_query_compat_payload or {})
    runbook_payload = dict(runbook_payload or {})
    readiness_summary = _normalize_dict(readiness_payload.get("summary"))
    compat_summary = _normalize_dict(cold_query_compat_payload.get("summary"))
    runbook_summary = _normalize_dict(runbook_payload.get("summary"))

    archive_cutoff_date = _normalize_text(
        readiness_summary.get("archive_cutoff_date")
        or compat_summary.get("archive_cutoff_date")
        or runbook_summary.get("archive_cutoff_date")
    )
    cutoff_date = _parse_iso_date(archive_cutoff_date)
    archive_table_name = _normalize_table_name(
        readiness_summary.get("archive_table_name")
        or runbook_summary.get("archive_table_name")
        or "fund_nav_history_archive"
    )
    archive_target_path = _resolve_path(
        readiness_summary.get("archive_target_absolute_path")
        or readiness_summary.get("archive_target_path")
        or runbook_summary.get("archive_target_path"),
        root,
    )
    active_db_path = _resolve_path(
        db_path
        or readiness_summary.get("active_db_path")
        or readiness_payload.get("db_path")
        or "backend/data/bond_funds.db",
        root,
    )
    backup_artifact_path = _resolve_path(
        readiness_summary.get("backup_artifact_absolute_path")
        or readiness_summary.get("backup_artifact_path"),
        root,
    )
    requested_mode = _normalize_text(mode) or "dry_run"
    mode_supported = requested_mode in ALLOWED_MODES
    effective_page_limit = max(0, int(page_limit or 0))

    primary_snapshot: dict = {"available": False}
    archive_snapshot: dict = {}
    if cutoff_date is not None and active_db_path is not None:
        try:
            primary_snapshot = _inspect_primary_db(
                db_path=active_db_path,
                cutoff_date=cutoff_date,
                page_limit=effective_page_limit,
            )
        except sqlite3.Error as exc:
            primary_snapshot = {"available": False, "error": str(exc)}
    if cutoff_date is not None:
        archive_snapshot = _inspect_archive_db(
            archive_target_path=archive_target_path,
            archive_table_name=archive_table_name,
            cutoff_date=cutoff_date,
        )

    live_cold_row_count = _normalize_int(primary_snapshot.get("live_cold_row_count"))
    live_cold_fund_count = _normalize_int(primary_snapshot.get("live_cold_fund_count"))
    expected_archive_row_count = _normalize_int(
        readiness_summary.get("estimated_archive_row_count")
        or compat_summary.get("cold_candidate_row_count")
        or runbook_summary.get("estimated_archive_row_count")
    )
    expected_archive_fund_count = _normalize_int(
        readiness_summary.get("estimated_archive_fund_count")
        or compat_summary.get("cold_candidate_fund_count")
        or runbook_summary.get("estimated_archive_fund_count")
    )
    archive_state = _derive_archive_state(
        archive_snapshot=archive_snapshot,
        expected_archive_row_count=expected_archive_row_count,
        expected_archive_fund_count=expected_archive_fund_count,
        live_cold_row_count=live_cold_row_count,
    )
    existing_archive_row_count = _normalize_int(archive_state.get("existing_archive_row_count"))
    existing_archive_fund_count = _normalize_int(archive_state.get("existing_archive_fund_count"))
    archive_out_of_scope_row_count = _normalize_int(
        archive_state.get("archive_out_of_scope_row_count")
    )
    archive_target_exists = bool(archive_state.get("archive_target_exists"))
    archive_table_exists = bool(archive_state.get("archive_table_exists"))
    archive_table_schema_valid = bool(archive_state.get("archive_table_schema_valid"))
    archive_inspection_error = _normalize_text(archive_state.get("archive_inspection_error"))
    archive_parent = archive_target_path.parent if archive_target_path is not None else None
    archive_parent_exists = bool(archive_parent and archive_parent.exists() and archive_parent.is_dir())
    archive_parent_writable = bool(
        archive_parent_exists and os.access(archive_parent, os.W_OK | os.X_OK)
    )
    active_db_exists = bool(active_db_path and active_db_path.exists() and active_db_path.is_file())
    backup_artifact_exists = bool(
        backup_artifact_path and backup_artifact_path.exists() and backup_artifact_path.is_file()
    )
    archive_target_is_primary = bool(
        archive_target_path
        and active_db_path
        and archive_target_path.resolve() == active_db_path.resolve()
    )
    readiness_ready = bool(readiness_summary.get("ready_for_manual_window_execution")) and (
        _normalize_text(readiness_summary.get("readiness_status"))
        == "ready_for_manual_window_execution"
    )
    query_compat_safe = bool(compat_summary.get("query_compat_safe"))
    safe_to_apply_archive_now = bool(compat_summary.get("safe_to_apply_archive_now"))
    uncovered_dependency_count = _normalize_int(
        compat_summary.get("production_uncovered_cold_dependency_count")
    )
    live_counts_match_expected = (
        expected_archive_row_count > 0
        and live_cold_row_count == expected_archive_row_count
        and (
            expected_archive_fund_count <= 0
            or live_cold_fund_count == expected_archive_fund_count
        )
    )
    archive_materialized_and_reconciled = bool(
        archive_state.get("archive_materialized_and_reconciled")
    )
    archive_binding_active = bool(archive_state.get("archive_binding_active"))
    archive_partial_conflict = bool(archive_state.get("archive_partial_conflict"))
    archive_export_in_progress = bool(archive_state.get("archive_export_in_progress"))
    archive_missing_row_count = _normalize_int(archive_state.get("archive_missing_row_count"))

    gates = [
        _build_gate(
            "mode",
            "执行模式",
            "ok" if mode_supported else "blocked",
            f"requested_mode={requested_mode}; allowed_modes={sorted(ALLOWED_MODES)}。",
            "当前仅支持 dry_run 和 archive_export；delete/VACUUM 仍未开放。",
        ),
        _build_gate(
            "archive_export_page_limit",
            "归档分页上限",
            "ok"
            if requested_mode != "archive_export" or effective_page_limit > 0
            else "blocked",
            f"page_limit={effective_page_limit}",
            "archive_export 模式必须显式提供正数 page_limit，避免无界写入。",
        ),
        _build_gate(
            "readiness",
            "维护准备度",
            "ok" if readiness_ready else "blocked",
            f"readiness_status={readiness_summary.get('readiness_status') or ''}",
            "必须先让 readiness audit 达到 ready_for_manual_window_execution。",
        ),
        _build_gate(
            "cold_query_compat",
            "热冷查询兼容",
            "ok" if query_compat_safe and safe_to_apply_archive_now else "blocked",
            (
                f"query_compat_safe={query_compat_safe}, "
                f"safe_to_apply_archive_now={safe_to_apply_archive_now}"
            ),
            "所有生产冷数据依赖必须已被 hot+cold 查询兼容层覆盖。",
        ),
        _build_gate(
            "production_uncovered_dependencies",
            "未覆盖生产依赖",
            "ok" if uncovered_dependency_count == 0 else "blocked",
            f"production_uncovered_cold_dependency_count={uncovered_dependency_count}",
            "存在未覆盖依赖时不能推进热库冷数据归档。",
        ),
        _build_gate(
            "cutoff",
            "归档 cutoff",
            "ok" if cutoff_date is not None else "blocked",
            f"archive_cutoff_date={archive_cutoff_date or ''}",
            "cutoff 必须是 ISO 日期。",
        ),
        _build_gate(
            "primary_database",
            "活动主库",
            "ok" if active_db_exists and primary_snapshot.get("available") else "blocked",
            (
                f"active_db_path={active_db_path}, "
                f"available={primary_snapshot.get('available')}, "
                f"error={primary_snapshot.get('error') or ''}"
            ),
            "必须能以只读方式打开活动主库并读取 fund_nav_history。",
        ),
        _build_gate(
            "backup_artifact",
            "备份基线",
            "ok" if backup_artifact_exists else "blocked",
            f"backup_artifact_path={backup_artifact_path or ''}",
            "必须绑定一份真实存在的归档前主库备份。",
        ),
        _build_gate(
            "archive_target_parent",
            "归档目录",
            "ok" if archive_parent_exists and archive_parent_writable else "blocked",
            f"archive_target_parent={archive_parent or ''}",
            "归档目标父目录必须存在且可写。",
        ),
        _build_gate(
            "archive_target_isolation",
            "归档目标隔离",
            "ok" if not archive_target_is_primary else "blocked",
            f"archive_target_path={archive_target_path or ''}",
            "归档库不能与活动主库指向同一文件。",
        ),
        _build_gate(
            "live_cold_count",
            "热库冷数据计数",
            "ok"
            if live_counts_match_expected or archive_binding_active
            else "blocked",
            (
                f"live_cold_row_count={live_cold_row_count}, "
                f"expected_archive_row_count={expected_archive_row_count}"
            ),
            "归档前热库冷数据计数必须与 readiness/runbook 预期一致；归档后则必须为 0。",
        ),
        _build_gate(
            "archive_existing_state",
            "归档库现状",
            "blocked" if archive_partial_conflict else "ok",
            (
                f"archive_target_exists={archive_target_exists}, "
                f"archive_table_exists={archive_table_exists}, "
                f"schema_valid={archive_table_schema_valid}, "
                f"existing_archive_row_count={existing_archive_row_count}, "
                f"out_of_scope_rows={archive_out_of_scope_row_count}, "
                f"inspection_error={archive_inspection_error}"
            ),
            "部分归档可续跑；schema 异常、越界行、超出预期行数或读取异常必须先人工处理。",
        ),
    ]

    blockers = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "blocked"
    ]
    review_items = [
        f"{gate['label']}: {gate['detail']}"
        for gate in gates
        if gate.get("status") == "review_required"
    ]

    archive_export_result: dict = {
        "status": "not_requested" if requested_mode != "archive_export" else "not_executed",
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
    }
    if requested_mode == "archive_export" and not blockers:
        if archive_missing_row_count <= 0 or archive_materialized_and_reconciled:
            archive_export_result = {
                "status": "skipped",
                "skip_reason": "archive_already_reconciled",
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
                "existing_before": existing_archive_row_count,
                "existing_after": existing_archive_row_count,
                "inserted_row_count": 0,
            }
        elif active_db_path is None or archive_target_path is None or cutoff_date is None:
            archive_export_result = {
                "status": "failed",
                "error": "resolved_path_or_cutoff_missing",
                "archive_sql_executed": False,
                "delete_sql_executed": False,
                "vacuum_executed": False,
            }
        else:
            archive_export_result = _execute_archive_export_page(
                db_path=active_db_path,
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                cutoff_date=cutoff_date,
                page_limit=min(effective_page_limit, archive_missing_row_count),
            )

        if archive_export_result.get("status") == "failed":
            blockers.append(
                "归档写入: "
                + _normalize_text(archive_export_result.get("error") or "archive_export_failed")
            )
            gates.append(
                _build_gate(
                    "archive_export_execution",
                    "归档写入执行",
                    "blocked",
                    _normalize_text(archive_export_result.get("error") or "archive_export_failed"),
                    "archive_export 写入失败时必须停止，保留当前归档库证据并人工复核。",
                )
            )
        elif archive_export_result.get("status") in {"executed", "skipped"} and cutoff_date is not None:
            archive_snapshot = _inspect_archive_db(
                archive_target_path=archive_target_path,
                archive_table_name=archive_table_name,
                cutoff_date=cutoff_date,
            )
            archive_state = _derive_archive_state(
                archive_snapshot=archive_snapshot,
                expected_archive_row_count=expected_archive_row_count,
                expected_archive_fund_count=expected_archive_fund_count,
                live_cold_row_count=live_cold_row_count,
            )
            existing_archive_row_count = _normalize_int(archive_state.get("existing_archive_row_count"))
            existing_archive_fund_count = _normalize_int(archive_state.get("existing_archive_fund_count"))
            archive_out_of_scope_row_count = _normalize_int(
                archive_state.get("archive_out_of_scope_row_count")
            )
            archive_target_exists = bool(archive_state.get("archive_target_exists"))
            archive_table_exists = bool(archive_state.get("archive_table_exists"))
            archive_table_schema_valid = bool(archive_state.get("archive_table_schema_valid"))
            archive_inspection_error = _normalize_text(archive_state.get("archive_inspection_error"))
            archive_materialized_and_reconciled = bool(
                archive_state.get("archive_materialized_and_reconciled")
            )
            archive_binding_active = bool(archive_state.get("archive_binding_active"))
            archive_partial_conflict = bool(archive_state.get("archive_partial_conflict"))
            archive_export_in_progress = bool(archive_state.get("archive_export_in_progress"))
            archive_missing_row_count = _normalize_int(archive_state.get("archive_missing_row_count"))

    if blockers:
        if archive_export_result.get("status") == "failed":
            runner_status = "archive_export_failed"
            auto_governance_stage = "archive_export_failed"
        else:
            runner_status = "blocked"
            auto_governance_stage = "gate_blocked"
    elif archive_binding_active:
        runner_status = "archive_binding_active"
        auto_governance_stage = "hot_cold_binding_active"
    elif archive_materialized_and_reconciled and live_cold_row_count > 0:
        runner_status = "dry_run_ready_for_delete_gate_review"
        auto_governance_stage = "archive_materialized_delete_pending"
    elif archive_export_in_progress:
        runner_status = "archive_export_in_progress"
        auto_governance_stage = "archive_export_in_progress"
    else:
        runner_status = "dry_run_ready_for_archive_export"
        auto_governance_stage = "archive_export_plan_ready"

    page_preview_rows = list(primary_snapshot.get("page_preview_rows") or [])
    page_preview_count = len(page_preview_rows)
    page_preview = {
        "page_limit": effective_page_limit,
        "page_candidate_row_count": min(live_cold_row_count, effective_page_limit)
        if effective_page_limit > 0
        else 0,
        "sample_rows": page_preview_rows[:20],
    }
    execution_plan = [
        {
            "step": "archive_export",
            "status": (
                "executed"
                if archive_export_result.get("status") == "executed"
                else (
                    "planned"
                    if runner_status in {
                        "dry_run_ready_for_archive_export",
                        "archive_export_in_progress",
                    }
                    else "not_planned"
                )
            ),
            "estimated_rows": archive_missing_row_count,
            "page_limit": effective_page_limit,
            "last_inserted_row_count": _normalize_int(
                archive_export_result.get("inserted_row_count")
            ),
            "sql_preview": list(runbook_payload.get("archive_sql_preview") or []),
        },
        {
            "step": "delete_hot_cold_rows",
            "status": (
                "gate_review_required"
                if runner_status == "dry_run_ready_for_delete_gate_review"
                else "not_planned"
            ),
            "estimated_rows": live_cold_row_count,
            "sql_preview": list(runbook_payload.get("delete_sql_preview") or []),
        },
        {
            "step": "postcheck",
            "status": "planned_after_mutation",
            "sql_preview": list(runbook_payload.get("postcheck_sql_preview") or []),
        },
        {
            "step": "vacuum",
            "status": "defer_to_followup_window",
            "sql_preview": [],
        },
    ]

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "dry_run_only": requested_mode == "dry_run",
        "dry_run_safe": not blockers,
        "archive_export_supported": True,
        "archive_apply_supported": requested_mode == "archive_export" and not blockers,
        "delete_supported": False,
        "vacuum_supported": False,
        "archive_export_executed": bool(archive_export_result.get("archive_sql_executed")),
        "archive_export_result_status": archive_export_result.get("status") or "",
        "archive_export_inserted_row_count": _normalize_int(
            archive_export_result.get("inserted_row_count")
        ),
        "archive_sql_executed": bool(archive_export_result.get("archive_sql_executed")),
        "delete_sql_executed": False,
        "vacuum_executed": False,
        "archive_binding_active": archive_binding_active,
        "archive_materialized_and_reconciled": archive_materialized_and_reconciled,
        "archive_partial_conflict": archive_partial_conflict,
        "archive_export_in_progress": archive_export_in_progress,
        "archive_out_of_scope_row_count": archive_out_of_scope_row_count,
        "archive_table_schema_valid": archive_table_schema_valid,
        "archive_inspection_error": archive_inspection_error,
        "readiness_status": readiness_summary.get("readiness_status") or "",
        "ready_for_manual_window_execution": readiness_ready,
        "cold_query_compat_status": compat_summary.get("status") or "",
        "query_compat_safe": query_compat_safe,
        "safe_to_apply_archive_now": safe_to_apply_archive_now,
        "production_uncovered_cold_dependency_count": uncovered_dependency_count,
        "archive_cutoff_date": archive_cutoff_date,
        "expected_archive_row_count": expected_archive_row_count,
        "expected_archive_fund_count": expected_archive_fund_count,
        "live_cold_row_count": live_cold_row_count,
        "live_cold_fund_count": live_cold_fund_count,
        "live_cold_min_nav_date": primary_snapshot.get("live_cold_min_nav_date") or "",
        "live_cold_max_nav_date": primary_snapshot.get("live_cold_max_nav_date") or "",
        "live_counts_match_expected": live_counts_match_expected,
        "total_nav_row_count": _normalize_int(primary_snapshot.get("total_nav_row_count")),
        "total_nav_fund_count": _normalize_int(primary_snapshot.get("total_nav_fund_count")),
        "hot_retained_row_count": _normalize_int(primary_snapshot.get("hot_retained_row_count")),
        "hot_retained_fund_count": _normalize_int(primary_snapshot.get("hot_retained_fund_count")),
        "active_db_path": str(active_db_path or ""),
        "active_db_size_mb": _normalize_float(primary_snapshot.get("database_size_mb")),
        "freelist_count": _normalize_int(primary_snapshot.get("freelist_count")),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "archive_target_exists": archive_target_exists,
        "archive_target_size_mb": _normalize_float(archive_snapshot.get("archive_target_size_mb")),
        "archive_table_name": archive_table_name,
        "archive_table_exists": archive_table_exists,
        "existing_archive_row_count": existing_archive_row_count,
        "existing_archive_fund_count": existing_archive_fund_count,
        "archive_missing_row_count": archive_missing_row_count,
        "backup_artifact_path": _stringify_project_path(backup_artifact_path, root),
        "backup_artifact_absolute_path": str(backup_artifact_path or ""),
        "backup_artifact_exists": backup_artifact_exists,
        "page_limit": effective_page_limit,
        "page_preview_count": page_preview_count,
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
        "page_apply_supported": False,
        "execution_mode": requested_mode,
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "message": "NAV archive apply runner dry-run completed.",
        "source_nav_history_archive_maintenance_readiness_audit_artifact": (
            source_readiness_artifact or ""
        ),
        "source_nav_history_archive_maintenance_readiness_audit_generated_at": (
            readiness_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_cold_query_compat_audit_artifact": (
            source_cold_query_compat_artifact or ""
        ),
        "source_nav_history_archive_cold_query_compat_audit_generated_at": (
            cold_query_compat_payload.get("generated_at") or ""
        ),
        "source_nav_history_archive_runbook_artifact": source_runbook_artifact or "",
        "source_nav_history_archive_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_nav_history_archive_maintenance_readiness_audit_summary": readiness_summary,
        "source_nav_history_archive_cold_query_compat_audit_summary": compat_summary,
        "source_nav_history_archive_runbook_summary": runbook_summary,
        "summary": summary,
        "gate_checks": gates,
        "blockers": blockers,
        "review_items": review_items,
        "primary_snapshot": {
            key: value
            for key, value in primary_snapshot.items()
            if key not in {"page_preview_rows"}
        },
        "archive_snapshot": archive_snapshot,
        "archive_export_result": archive_export_result,
        "page_preview": page_preview,
        "execution_plan": execution_plan,
        "findings": _build_findings(
            mode=requested_mode,
            runner_status=runner_status,
            blockers=blockers,
            review_items=review_items,
            archive_materialized_and_reconciled=archive_materialized_and_reconciled,
            archive_binding_active=archive_binding_active,
            live_cold_row_count=live_cold_row_count,
            archive_export_result=archive_export_result,
        ),
        "safety": {
            "archive_sql_executed": bool(archive_export_result.get("archive_sql_executed")),
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }


def render_nav_history_archive_apply_runner_markdown(payload: dict) -> str:
    """Render a markdown summary for the NAV archive apply runner."""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive apply runner",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- runner_status: {summary.get('runner_status', '')}",
        f"- auto_governance_stage: {summary.get('auto_governance_stage', '')}",
        f"- mode: {summary.get('mode', '')}",
        f"- dry_run_only: {summary.get('dry_run_only', True)}",
        f"- archive_sql_executed: {summary.get('archive_sql_executed', False)}",
        f"- delete_sql_executed: {summary.get('delete_sql_executed', False)}",
        f"- vacuum_executed: {summary.get('vacuum_executed', False)}",
        f"- archive_binding_active: {summary.get('archive_binding_active', False)}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- expected_archive_row_count: {summary.get('expected_archive_row_count', 0)}",
        f"- live_cold_row_count: {summary.get('live_cold_row_count', 0)}",
        f"- existing_archive_row_count: {summary.get('existing_archive_row_count', 0)}",
        f"- archive_missing_row_count: {summary.get('archive_missing_row_count', 0)}",
        f"- backup_artifact_exists: {summary.get('backup_artifact_exists', False)}",
        "",
        "## gate_checks",
    ]
    for item in payload.get("gate_checks") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## execution_plan"])
    for item in payload.get("execution_plan") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## blockers"])
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## review_items"])
    for item in payload.get("review_items") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_apply_runner_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """Write NAV archive apply runner JSON/Markdown artifacts."""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_apply_runner_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_apply_runner_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_apply_runner_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
