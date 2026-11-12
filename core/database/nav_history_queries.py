"""
fund_nav_history 常用只读查询。

该模块同时承载 NAV 热/冷库兼容查询的最小 SQL 片段。归档库不存在
时所有 helper 都回退到主库 fund_nav_history，不改变现有热库行为。
"""

from pathlib import Path
import json
import sqlite3

from sqlalchemy import text

from utils.nav_history_archive_maintenance_defaults import (
    DEFAULT_ARCHIVE_TABLE_NAME,
)


NAV_HISTORY_FCODE_GROUP_SQL = """
SELECT fcode, MIN(nav_date) AS first_nav_date
FROM fund_nav_history
GROUP BY fcode
"""

NAV_HISTORY_FCODE_COUNT_SQL = """
SELECT COUNT(*) FROM (
    SELECT fcode, MIN(nav_date) AS first_nav_date
    FROM fund_nav_history
    GROUP BY fcode
) AS nav_fcode_groups
"""


def quote_sql_literal(value: str) -> str:
    """Quote a string literal for generated SQLite SQL snippets."""
    return "'" + str(value or "").replace("'", "''") + "'"


def normalize_archive_table_name(value: object) -> str:
    text_value = str(value or "").strip()
    if not text_value:
        return DEFAULT_ARCHIVE_TABLE_NAME
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text_value):
        return DEFAULT_ARCHIVE_TABLE_NAME
    return text_value


def _resolve_runtime_artifact_path(
    value: object,
    *,
    project_root: str | Path | None,
) -> str:
    path_text = str(value or "").strip()
    if not path_text:
        return ""
    root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[2]
    path = Path(path_text)
    if not path.is_absolute():
        return str((root / path).resolve())
    if path.exists():
        return str(path.resolve())
    parts = path.parts
    for anchor in ("backend", "reports", "config", "logs", "core", "frontend", "scripts"):
        if anchor not in parts:
            continue
        anchor_index = parts.index(anchor)
        remapped = root.joinpath(*parts[anchor_index:]).resolve()
        if remapped.exists():
            return str(remapped)
    return str(path)


def _load_latest_active_archive_runner_binding(
    *,
    project_root: str | Path | None = None,
    audit_dir: str | Path | None = None,
) -> dict[str, str]:
    root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[2]
    resolved_audit_dir = (
        Path(audit_dir).resolve()
        if audit_dir
        else root / "reports" / "audit" / "nav_history_archive"
    )
    try:
        candidates = sorted(
            resolved_audit_dir.glob("nav_history_archive_apply_runner_*.json")
        )
    except OSError:
        return {}
    for candidate in reversed(candidates):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        summary = payload.get("summary") if isinstance(payload, dict) else {}
        if not isinstance(summary, dict):
            continue
        if not summary.get("archive_binding_active"):
            continue
        archive_path = str(
            summary.get("archive_target_absolute_path")
            or summary.get("archive_target_path")
            or ""
        ).strip()
        archive_path = _resolve_runtime_artifact_path(
            archive_path,
            project_root=root,
        )
        active_db_path = str(
            summary.get("active_db_absolute_path")
            or summary.get("active_db_path")
            or ""
        ).strip()
        active_db_path = _resolve_runtime_artifact_path(
            active_db_path,
            project_root=root,
        )
        table_name = str(summary.get("archive_table_name") or DEFAULT_ARCHIVE_TABLE_NAME).strip()
        return {
            "archive_path": archive_path,
            "active_db_path": active_db_path,
            "archive_table_name": normalize_archive_table_name(table_name),
            "archive_cutoff_date": str(summary.get("archive_cutoff_date") or "").strip(),
            "source_apply_runner_artifact": str(candidate),
        }
    return {}


def _load_latest_readiness_archive_binding(
    *,
    project_root: str | Path | None = None,
    audit_dir: str | Path | None = None,
) -> dict[str, str]:
    root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[2]
    resolved_audit_dir = (
        Path(audit_dir).resolve()
        if audit_dir
        else root / "reports" / "audit" / "nav_history_archive"
    )
    try:
        candidates = sorted(
            resolved_audit_dir.glob("nav_history_archive_maintenance_readiness_audit_*.json")
        )
    except OSError:
        return {}
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except Exception:
        return {}
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    if not isinstance(summary, dict):
        return {}
    archive_path = str(
        summary.get("archive_target_absolute_path")
        or summary.get("archive_target_path")
        or ""
    ).strip()
    archive_path = _resolve_runtime_artifact_path(
        archive_path,
        project_root=root,
    )
    table_name = str(summary.get("archive_table_name") or DEFAULT_ARCHIVE_TABLE_NAME).strip()
    return {
        "archive_path": archive_path,
        "archive_table_name": normalize_archive_table_name(table_name),
        "archive_cutoff_date": str(summary.get("archive_cutoff_date") or "").strip(),
        "source_readiness_artifact": str(candidates[-1]),
    }


def load_latest_nav_history_archive_binding(
    *,
    project_root: str | Path | None = None,
    audit_dir: str | Path | None = None,
    allow_readiness_fallback: bool = False,
) -> dict[str, str]:
    """Read the latest active NAV archive binding for optional cold queries.

    Readiness artifacts describe a prepared maintenance window, not a completed
    hot/cold split. Runtime queries only bind an archive after an apply runner
    artifact proves that archived rows are reconciled and no longer present in
    the hot table. The optional readiness fallback is kept for controlled tests
    and ad-hoc diagnostics.
    """
    binding = _load_latest_active_archive_runner_binding(
        project_root=project_root,
        audit_dir=audit_dir,
    )
    if binding:
        return binding
    if allow_readiness_fallback:
        return _load_latest_readiness_archive_binding(
            project_root=project_root,
            audit_dir=audit_dir,
        )
    return {}


def nav_history_archive_binding_matches_database(
    binding: dict | None,
    db_path: str | Path,
    *,
    project_root: str | Path | None = None,
) -> bool:
    """Return whether an active NAV archive binding belongs to the queried hot DB."""
    active_db_path = str((binding or {}).get("active_db_path") or "").strip()
    if not active_db_path:
        return True
    resolved_active = _resolve_runtime_artifact_path(
        active_db_path,
        project_root=project_root,
    )
    try:
        return Path(resolved_active).resolve() == Path(db_path).resolve()
    except OSError:
        return str(Path(resolved_active)) == str(Path(db_path))


def attach_nav_history_archive_if_available(
    conn: sqlite3.Connection,
    *,
    archive_path: str | Path | None,
    archive_table_name: str = DEFAULT_ARCHIVE_TABLE_NAME,
    archive_cutoff_date: str | None = None,
    alias: str = "nav_archive_db",
) -> dict:
    """Attach the NAV archive DB when it exists and has the expected table."""
    path_text = str(archive_path or "").strip()
    if not path_text:
        return {"attached": False, "reason": "archive_path_missing", "alias": alias}
    archive_file = Path(path_text)
    if not archive_file.exists() or not archive_file.is_file():
        return {"attached": False, "reason": "archive_file_missing", "alias": alias}

    table_name = normalize_archive_table_name(archive_table_name)
    try:
        conn.execute(f"ATTACH DATABASE {quote_sql_literal(str(archive_file.resolve()))} AS {alias}")
        row = conn.execute(
            f"SELECT 1 FROM {alias}.sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if row:
            return {
                "attached": True,
                "reason": "attached",
                "alias": alias,
                "archive_path": str(archive_file.resolve()),
                "archive_table_name": table_name,
                "archive_cutoff_date": str(archive_cutoff_date or "").strip(),
            }
        conn.execute(f"DETACH DATABASE {alias}")
        return {"attached": False, "reason": "archive_table_missing", "alias": alias}
    except sqlite3.Error as exc:
        try:
            conn.execute(f"DETACH DATABASE {alias}")
        except sqlite3.Error:
            pass
        return {"attached": False, "reason": f"attach_failed:{exc}", "alias": alias}


def detach_nav_history_archive(
    conn: sqlite3.Connection,
    *,
    alias: str = "nav_archive_db",
) -> None:
    try:
        conn.execute(f"DETACH DATABASE {alias}")
    except sqlite3.Error:
        pass


def build_nav_history_union_source_sql(
    archive_attach: dict | None = None,
    *,
    include_daily_growth: bool = True,
    columns: object = None,
) -> str:
    """Return a SELECT source containing hot rows and attached cold rows."""
    attached = bool((archive_attach or {}).get("attached"))
    alias = str((archive_attach or {}).get("alias") or "nav_archive_db").strip()
    table_name = normalize_archive_table_name(
        (archive_attach or {}).get("archive_table_name") or DEFAULT_ARCHIVE_TABLE_NAME
    )
    if columns is None:
        column_sql = (
            "fcode, nav_date, unit_nav, accum_nav, daily_growth"
            if include_daily_growth
            else "fcode, nav_date, unit_nav, accum_nav"
        )
    else:
        safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
        selected_columns = []
        for item in columns:
            column = str(item or "").strip()
            if not column or any(ch not in safe_chars for ch in column):
                continue
            if column not in selected_columns:
                selected_columns.append(column)
        column_sql = ", ".join(selected_columns or ["fcode", "nav_date"])
    hot_select = f"SELECT {column_sql} FROM fund_nav_history"
    if not attached:
        return hot_select
    cold_select = f"SELECT {column_sql} FROM {alias}.{table_name}"
    cutoff_date = str((archive_attach or {}).get("archive_cutoff_date") or "").strip()
    if cutoff_date:
        cold_select = f"{cold_select} WHERE nav_date < {quote_sql_literal(cutoff_date)}"
    return f"{hot_select}\nUNION ALL\n{cold_select}"


def build_nav_history_fcode_group_statement(limit=None):
    """构建按基金代码聚合的净值只读查询。"""
    params = {}
    sql = NAV_HISTORY_FCODE_GROUP_SQL.strip()

    if limit is not None:
        limit = max(0, int(limit))
        sql = f"{sql}\nLIMIT :limit"
        params["limit"] = limit

    return text(sql), params


def list_nav_history_fcodes(session, limit=None):
    """返回 fund_nav_history 中出现过净值数据的基金代码列表。"""
    if limit is not None and int(limit) <= 0:
        return []

    statement, params = build_nav_history_fcode_group_statement(limit=limit)
    rows = session.execute(statement, params).fetchall()
    return [row[0] for row in rows]


def count_nav_history_fcodes(session):
    """返回 fund_nav_history 中有净值数据的基金数量。"""
    value = session.execute(text(NAV_HISTORY_FCODE_COUNT_SQL)).scalar()
    return int(value or 0)
