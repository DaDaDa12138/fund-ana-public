#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only preflight for a future fund_rank_history hot/cold archive window."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple
from urllib.parse import quote

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.rank_history_governance_audit import (
    DEFAULT_OUTPUT_DIR as DEFAULT_GOVERNANCE_OUTPUT_DIR,
    load_latest_rank_history_governance_audit_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/rank_history_archive"
RANK_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN = "rank_history_archive_preflight_*.json"
RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN = "rank_history_archive_apply_runner_*.json"
ARCHIVE_TABLE_NAME = "fund_rank_history_archive"
ARCHIVE_FILE_PREFIX = "fund_rank_history_before_"
CORE_ARCHIVE_COLUMNS = (
    "fcode",
    "rank_date",
    "period",
    "rank_position",
    "category_total",
    "percentile",
)
OPTIONAL_ARCHIVE_COLUMNS = ("created_at",)
REQUIRED_HOT_COLUMNS = CORE_ARCHIVE_COLUMNS
ARCHIVE_COLUMNS = CORE_ARCHIVE_COLUMNS + OPTIONAL_ARCHIVE_COLUMNS


def load_latest_rank_history_archive_preflight_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        RANK_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_list(value: Any) -> list:
    return value if isinstance(value, list) else []


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


def _quote_sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    resolved = Path(db_path)
    conn = sqlite3.connect(_sqlite_readonly_uri(resolved), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=120000")
    except sqlite3.Error:
        pass
    return conn


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
    rows = _fetch_all_dicts(conn, f"PRAGMA table_info({table_name})")
    return {_normalize_text(row.get("name")) for row in rows if _normalize_text(row.get("name"))}


def _candidate_where_sql(cutoff_date: str) -> str:
    return f"rank_date < {_quote_sql_literal(cutoff_date)}"


def _archive_target_path(cutoff_date: str) -> str:
    return f"backend/data/archives/{ARCHIVE_FILE_PREFIX}{cutoff_date}.db"


def _resolve_path(path_text: str, project_root: str | Path | None) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    root = Path(project_root).resolve() if project_root else PROJECT_ROOT
    return (root / path).resolve()


def _resolve_output_dir(path_text: str | Path, project_root: str | Path | None) -> str:
    path = Path(path_text)
    if path.is_absolute():
        return str(path.resolve())
    root = Path(project_root).resolve() if project_root else PROJECT_ROOT
    return str((root / path).resolve())


def _build_select_columns(existing_columns: set[str]) -> list[str]:
    return [column for column in ARCHIVE_COLUMNS if column in existing_columns]


def _compute_sample_digest(
    conn: sqlite3.Connection,
    *,
    cutoff_date: str,
    columns: list[str],
    sample_limit: int,
) -> dict:
    digest = hashlib.sha256()
    normalized_limit = max(1, int(sample_limit or 1))
    column_sql = ", ".join(columns)
    order_sql = "fcode, rank_date, COALESCE(period, '')"
    rows = conn.execute(
        f"""
        SELECT {column_sql}
        FROM fund_rank_history
        WHERE {_candidate_where_sql(cutoff_date)}
        ORDER BY {order_sql}
        LIMIT ?
        """,
        (normalized_limit,),
    ).fetchall()
    for row in rows:
        digest.update(
            json.dumps([row[column] for column in columns], ensure_ascii=False, default=str).encode("utf-8")
        )
        digest.update(b"\n")
    return {
        "digest_mode": "ordered_head_sample",
        "sample_limit": normalized_limit,
        "sample_row_count": len(rows),
        "sha256": digest.hexdigest(),
    }


def _inspect_archive_target(path: Path, table_name: str, cutoff_date: str) -> dict:
    if not path.exists():
        return {
            "archive_target_exists": False,
            "archive_table_exists": False,
            "archive_row_count": 0,
            "archive_out_of_scope_row_count": 0,
            "archive_table_schema_valid": True,
            "archive_quick_check": "",
        }
    try:
        conn = _connect_readonly(path)
        try:
            exists = _table_exists(conn, table_name)
            columns = _table_columns(conn, table_name) if exists else set()
            schema_valid = set(REQUIRED_HOT_COLUMNS).issubset(columns) if exists else True
            row_count = 0
            out_of_scope_count = 0
            if exists:
                row_count = _normalize_int(
                    conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                )
                if "rank_date" in columns and cutoff_date:
                    out_of_scope_count = _normalize_int(
                        conn.execute(
                            f"SELECT COUNT(*) FROM {table_name} WHERE rank_date >= ?",
                            (cutoff_date,),
                        ).fetchone()[0]
                    )
            quick_check = _normalize_text(conn.execute("PRAGMA quick_check").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return {
            "archive_target_exists": True,
            "archive_table_exists": False,
            "archive_row_count": 0,
            "archive_out_of_scope_row_count": 0,
            "archive_table_schema_valid": False,
            "archive_quick_check": f"error:{exc}",
        }
    return {
        "archive_target_exists": True,
        "archive_table_exists": exists,
        "archive_row_count": row_count,
        "archive_out_of_scope_row_count": out_of_scope_count,
        "archive_table_schema_valid": schema_valid,
        "archive_quick_check": quick_check,
    }


def _load_latest_apply_runner_summary(
    *,
    output_dir: str | Path,
    project_root: str | Path | None,
    archive_target_path: str,
    cutoff_date: str,
) -> dict:
    resolved_output_dir = Path(_resolve_output_dir(output_dir, project_root))
    try:
        candidates = sorted(resolved_output_dir.glob(RANK_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN))
    except OSError:
        return {}
    target_absolute_path = str(_resolve_path(archive_target_path, project_root))
    for candidate in reversed(candidates):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        summary = _normalize_dict(payload.get("summary")) if isinstance(payload, dict) else {}
        if _normalize_text(summary.get("archive_cutoff_date")) != cutoff_date:
            continue
        runner_target = _normalize_text(
            summary.get("archive_target_absolute_path") or summary.get("archive_target_path")
        )
        runner_target_absolute = str(_resolve_path(runner_target, project_root)) if runner_target else ""
        if runner_target_absolute != target_absolute_path:
            continue
        return {
            **summary,
            "source_apply_runner_artifact": str(candidate),
        }
    return {}


def _build_sql_preview(
    *,
    archive_target_path: str,
    archive_table_name: str,
    cutoff_date: str,
    columns: list[str],
) -> dict:
    archive_columns = ", ".join(columns)
    archive_column_defs = {
        "fcode": "fcode TEXT NOT NULL",
        "rank_date": "rank_date TEXT NOT NULL",
        "period": "period TEXT",
        "rank_position": "rank_position INTEGER",
        "category_total": "category_total INTEGER",
        "percentile": "percentile REAL",
        "created_at": "created_at TEXT",
    }
    create_columns = ", ".join(archive_column_defs[column] for column in columns)
    attach_sql = f"ATTACH DATABASE {_quote_sql_literal(archive_target_path)} AS rank_archive_db;"
    return {
        "archive_sql_preview": [
            attach_sql,
            f"CREATE TABLE IF NOT EXISTS rank_archive_db.{archive_table_name} ({create_columns});",
            (
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{archive_table_name}_key "
                f"ON {archive_table_name} (fcode, rank_date, COALESCE(period, ''));"
            ),
            (
                f"INSERT OR IGNORE INTO rank_archive_db.{archive_table_name} ({archive_columns}) "
                f"SELECT {archive_columns} FROM main.fund_rank_history WHERE {_candidate_where_sql(cutoff_date)};"
            ),
            "DETACH DATABASE rank_archive_db;",
        ],
        "delete_sql_preview": [
            (
                "DELETE FROM fund_rank_history WHERE "
                f"{_candidate_where_sql(cutoff_date)} "
                "AND EXISTS (SELECT 1 FROM rank_archive_db.fund_rank_history_archive archived "
                "WHERE archived.fcode = fund_rank_history.fcode "
                "AND archived.rank_date = fund_rank_history.rank_date "
                "AND ((archived.period = fund_rank_history.period) OR (archived.period IS NULL AND fund_rank_history.period IS NULL)));"
            )
        ],
        "postcheck_sql_preview": [
            "PRAGMA quick_check;",
            (
                "SELECT COUNT(*) AS remaining_cold_rows FROM fund_rank_history "
                f"WHERE {_candidate_where_sql(cutoff_date)};"
            ),
        ],
    }


def _build_findings(summary: dict, checks: dict) -> list[str]:
    findings = []
    if checks.get("ready_for_archive_runbook"):
        findings.append(
            "rank 历史归档前置核对通过，可继续生成 runbook/control pack；当前仍未执行归档、删除或 VACUUM。"
        )
    else:
        findings.append(
            "rank 历史归档前置核对未通过；需要先处理 blocker，再进入任何写入或删除窗口。"
        )
    if summary.get("candidate_row_count"):
        findings.append(
            f"当前 cutoff 前 rank 冷数据候选 {summary.get('candidate_row_count')} 行，"
            f"覆盖 {summary.get('candidate_fund_count')} 只基金，日期范围 "
            f"{summary.get('candidate_min_rank_date') or '-'} 至 {summary.get('candidate_max_rank_date') or '-'}。"
        )
    if summary.get("production_uncovered_dependency_count"):
        findings.append(
            f"仍有 {summary.get('production_uncovered_dependency_count')} 组生产 rank 查询依赖未完成冷库兼容。"
        )
    return findings


def build_rank_history_archive_preflight_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    governance_payload: Optional[dict] = None,
    source_governance_artifact: Optional[str] = None,
    governance_output_dir: str = DEFAULT_GOVERNANCE_OUTPUT_DIR,
    archive_output_dir: str = DEFAULT_OUTPUT_DIR,
    digest_sample_limit: int = 1000,
) -> dict:
    if governance_payload is None:
        governance_path, governance_payload = load_latest_rank_history_governance_audit_payload(
            output_dir=_resolve_output_dir(governance_output_dir, project_root),
        )
        source_governance_artifact = str(governance_path)
    governance_payload = _normalize_dict(governance_payload)
    governance_summary = _normalize_dict(governance_payload.get("summary"))
    resolved_db_path = Path(resolve_db_path(str(db_path or "database/bond_funds.db"))).resolve()
    cutoff_date = _normalize_text(governance_summary.get("archive_cutoff_date"))
    archive_table_name = _normalize_text(governance_summary.get("archive_table_name")) or ARCHIVE_TABLE_NAME
    archive_target_path = (
        _normalize_text(governance_summary.get("archive_target_path"))
        or _archive_target_path(cutoff_date)
    )
    archive_target_absolute_path = _resolve_path(archive_target_path, project_root)

    checks: dict[str, Any] = {
        "governance_available": bool(governance_summary),
        "cutoff_present": bool(cutoff_date),
        "production_query_compat_ready": _normalize_int(
            governance_summary.get("production_uncovered_dependency_count")
        )
        == 0,
        "governance_blocker_free": _normalize_int(governance_summary.get("blocker_count")) == 0,
    }
    blockers = []
    conn = _connect_readonly(resolved_db_path)
    try:
        quick_check = _normalize_text(conn.execute("PRAGMA quick_check").fetchone()[0])
        columns = _table_columns(conn, "fund_rank_history")
        selected_columns = _build_select_columns(columns)
        missing_required = [column for column in REQUIRED_HOT_COLUMNS if column not in columns]
        checks["hot_table_exists"] = _table_exists(conn, "fund_rank_history")
        checks["required_columns_present"] = not missing_required
        checks["active_db_quick_check_ok"] = quick_check == "ok"
        if cutoff_date and not missing_required:
            candidate = _fetch_one_dict(
                conn,
                f"""
                SELECT
                    COUNT(*) AS candidate_row_count,
                    COUNT(DISTINCT fcode) AS candidate_fund_count,
                    MIN(rank_date) AS candidate_min_rank_date,
                    MAX(rank_date) AS candidate_max_rank_date
                FROM fund_rank_history
                WHERE {_candidate_where_sql(cutoff_date)}
                """,
            )
            sample_digest = _compute_sample_digest(
                conn,
                cutoff_date=cutoff_date,
                columns=selected_columns,
                sample_limit=digest_sample_limit,
            )
        else:
            candidate = {}
            sample_digest = {
                "digest_mode": "not_computed",
                "sample_limit": 0,
                "sample_row_count": 0,
                "sha256": "",
            }
    finally:
        conn.close()

    candidate_row_count = _normalize_int(candidate.get("candidate_row_count"))
    candidate_fund_count = _normalize_int(candidate.get("candidate_fund_count"))
    checks["candidate_rows_exist"] = candidate_row_count > 0
    checks["candidate_counts_match_governance"] = (
        candidate_row_count == _normalize_int(governance_summary.get("estimated_archive_row_count"))
        and candidate_fund_count == _normalize_int(governance_summary.get("estimated_archive_fund_count"))
    )

    archive_target_inspection = _inspect_archive_target(
        archive_target_absolute_path,
        archive_table_name,
        cutoff_date,
    )
    latest_apply_summary = _load_latest_apply_runner_summary(
        output_dir=archive_output_dir,
        project_root=project_root,
        archive_target_path=str(archive_target_absolute_path),
        cutoff_date=cutoff_date,
    )
    latest_apply_expected_rows = max(
        _normalize_int(latest_apply_summary.get("expected_archive_row_count")),
        _normalize_int(latest_apply_summary.get("existing_archive_row_count")),
    )
    latest_apply_expected_funds = max(
        _normalize_int(latest_apply_summary.get("expected_archive_fund_count")),
        _normalize_int(latest_apply_summary.get("existing_archive_fund_count")),
    )
    effective_expected_archive_row_count = max(
        candidate_row_count,
        _normalize_int(governance_summary.get("estimated_archive_row_count")),
        latest_apply_expected_rows,
    )
    effective_expected_archive_fund_count = max(
        candidate_fund_count,
        _normalize_int(governance_summary.get("estimated_archive_fund_count")),
        latest_apply_expected_funds,
    )
    archive_target_has_rows = bool(
        archive_target_inspection.get("archive_target_exists")
        and archive_target_inspection.get("archive_table_exists")
        and _normalize_int(archive_target_inspection.get("archive_row_count")) > 0
    )
    checks["archive_target_not_conflicting"] = (
        not archive_target_has_rows
        or (
            _normalize_text(archive_target_inspection.get("archive_quick_check")).lower() == "ok"
            and bool(archive_target_inspection.get("archive_table_schema_valid"))
            and _normalize_int(archive_target_inspection.get("archive_row_count"))
            <= effective_expected_archive_row_count
            and _normalize_int(archive_target_inspection.get("archive_out_of_scope_row_count"))
            == 0
        )
    )
    checks["ready_for_archive_runbook"] = all(bool(value) for value in checks.values())

    for check_name, passed in checks.items():
        if not passed:
            blockers.append(
                {
                    "blocker_id": check_name,
                    "severity": "high",
                    "message": f"rank archive preflight check failed: {check_name}",
                }
            )

    summary = {
        "status": (
            "rank_archive_preflight_ready"
            if checks["ready_for_archive_runbook"]
            else "rank_archive_preflight_blocked"
        ),
        "ready_for_archive_runbook": bool(checks["ready_for_archive_runbook"]),
        "database_path": str(resolved_db_path),
        "active_db_quick_check": quick_check,
        "archive_cutoff_date": cutoff_date,
        "archive_target_path": archive_target_path,
        "archive_target_absolute_path": str(archive_target_absolute_path),
        "archive_table_name": archive_table_name,
        "candidate_row_count": candidate_row_count,
        "candidate_fund_count": candidate_fund_count,
        "candidate_min_rank_date": _normalize_text(candidate.get("candidate_min_rank_date")),
        "candidate_max_rank_date": _normalize_text(candidate.get("candidate_max_rank_date")),
        "governance_estimated_archive_row_count": _normalize_int(
            governance_summary.get("estimated_archive_row_count")
        ),
        "governance_estimated_archive_fund_count": _normalize_int(
            governance_summary.get("estimated_archive_fund_count")
        ),
        "production_uncovered_dependency_count": _normalize_int(
            governance_summary.get("production_uncovered_dependency_count")
        ),
        "latest_apply_runner_expected_archive_row_count": latest_apply_expected_rows,
        "latest_apply_runner_expected_archive_fund_count": latest_apply_expected_funds,
        "latest_apply_runner_artifact": _normalize_text(
            latest_apply_summary.get("source_apply_runner_artifact")
        ),
        "effective_expected_archive_row_count": effective_expected_archive_row_count,
        "effective_expected_archive_fund_count": effective_expected_archive_fund_count,
        "blocker_count": len(blockers),
        "archive_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
    }
    sql_preview = _build_sql_preview(
        archive_target_path=archive_target_path,
        archive_table_name=archive_table_name,
        cutoff_date=cutoff_date,
        columns=selected_columns or list(REQUIRED_HOT_COLUMNS),
    )
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_governance_artifact": source_governance_artifact or "",
        "summary": summary,
        "checks": checks,
        "blockers": blockers,
        "findings": _build_findings(summary, checks),
        "archive_target_inspection": archive_target_inspection,
        "candidate_sample_digest": sample_digest,
        "production_dependencies": _normalize_list(governance_payload.get("production_dependencies")),
        "sql_preview": sql_preview,
        "safety": {
            "read_only": True,
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }


def render_rank_history_archive_preflight_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# rank history archive preflight",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Status: {summary.get('status', '')}",
        f"- Ready: {summary.get('ready_for_archive_runbook', False)}",
        f"- Cutoff: {summary.get('archive_cutoff_date', '')}",
        f"- Candidate rows: {summary.get('candidate_row_count', 0)}",
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
            lines.append(f"- {item.get('blocker_id', '')}: {item.get('message', '')}")
    return "\n".join(lines).strip() + "\n"


def write_rank_history_archive_preflight_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"rank_history_archive_preflight_{ts}.json"
    md_path = artifact_dir / f"rank_history_archive_preflight_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_rank_history_archive_preflight_markdown(payload), encoding="utf-8")
    return json_path, md_path
