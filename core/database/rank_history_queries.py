"""fund_rank_history read helpers with optional hot/cold archive support."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from database.nav_history_queries import quote_sql_literal


DEFAULT_RANK_ARCHIVE_TABLE_NAME = "fund_rank_history_archive"
DEFAULT_RANK_ARCHIVE_AUDIT_DIR = "reports/audit/rank_history_archive"
DEFAULT_RANK_ARCHIVE_APPLY_PATTERN = "rank_history_archive_apply_runner_*.json"
RANK_HISTORY_DEFAULT_COLUMNS = (
    "fcode",
    "rank_date",
    "period",
    "rank_position",
    "category_total",
    "percentile",
)


def begin_rank_history_read_snapshot(conn: sqlite3.Connection) -> None:
    """Pin hot rows before loading the binding that exposes their cold copies.

    Writers publish the binding before deleting newly cold hot rows. Readers of
    an old binding keep those rows in their pinned hot snapshot. The caller owns
    the connection and closes or rolls back this read transaction.
    """
    if not conn.in_transaction:
        conn.execute("BEGIN")
    conn.execute("SELECT rowid FROM fund_rank_history LIMIT 1").fetchone()


def normalize_rank_archive_table_name(value: object) -> str:
    text_value = str(value or "").strip()
    if not text_value:
        return DEFAULT_RANK_ARCHIVE_TABLE_NAME
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text_value):
        return DEFAULT_RANK_ARCHIVE_TABLE_NAME
    return text_value


def normalize_rank_history_columns(columns: object = None) -> tuple[str, ...]:
    raw_columns = columns or RANK_HISTORY_DEFAULT_COLUMNS
    normalized: list[str] = []
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    for item in raw_columns:
        column = str(item or "").strip()
        if not column or any(ch not in safe_chars for ch in column):
            continue
        if column not in normalized:
            normalized.append(column)
    return tuple(normalized or RANK_HISTORY_DEFAULT_COLUMNS)


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


def load_latest_rank_history_archive_binding(
    *,
    project_root: str | Path | None = None,
    audit_dir: str | Path | None = None,
) -> dict[str, str]:
    """Read the latest active rank archive binding, if one exists."""
    root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[2]
    resolved_audit_dir = (
        Path(audit_dir).resolve()
        if audit_dir
        else root / DEFAULT_RANK_ARCHIVE_AUDIT_DIR
    )
    try:
        candidates = sorted(resolved_audit_dir.glob(DEFAULT_RANK_ARCHIVE_APPLY_PATTERN))
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
        active_db_path = str(
            summary.get("active_db_absolute_path")
            or summary.get("active_db_path")
            or ""
        ).strip()
        return {
            "archive_path": _resolve_runtime_artifact_path(archive_path, project_root=root),
            "active_db_path": _resolve_runtime_artifact_path(active_db_path, project_root=root),
            "archive_table_name": normalize_rank_archive_table_name(
                summary.get("archive_table_name") or DEFAULT_RANK_ARCHIVE_TABLE_NAME
            ),
            "archive_cutoff_date": str(summary.get("archive_cutoff_date") or "").strip(),
            "source_apply_runner_artifact": str(candidate),
        }
    return {}


def rank_history_archive_binding_matches_database(
    binding: dict | None,
    db_path: str | Path,
    *,
    project_root: str | Path | None = None,
) -> bool:
    """Return whether an active archive binding belongs to the queried hot DB."""
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


def attach_rank_history_archive_if_available(
    conn: sqlite3.Connection,
    *,
    archive_path: str | Path | None,
    archive_table_name: str = DEFAULT_RANK_ARCHIVE_TABLE_NAME,
    archive_cutoff_date: str | None = None,
    alias: str = "rank_archive_db",
) -> dict:
    """Attach the rank archive DB only when the active binding target exists."""
    path_text = str(archive_path or "").strip()
    if not path_text:
        return {"attached": False, "reason": "archive_path_missing", "alias": alias}
    archive_file = Path(path_text)
    if not archive_file.exists() or not archive_file.is_file():
        return {"attached": False, "reason": "archive_file_missing", "alias": alias}

    table_name = normalize_rank_archive_table_name(archive_table_name)
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


def detach_rank_history_archive(
    conn: sqlite3.Connection,
    *,
    alias: str = "rank_archive_db",
) -> None:
    try:
        conn.execute(f"DETACH DATABASE {alias}")
    except sqlite3.Error:
        pass


def build_latest_rank_dates_sql(*, source: str = "fund_rank_history", cutoff: str = "") -> str:
    """Seek once per fund in the existing (fcode, rank_date, period) index.

    Recursive MIN skips to the next distinct code without walking every
    historical row.  Read directly from the transaction snapshot, so same-day
    corrections, deletions and archive rolls need no cache invalidation.
    """
    parts = source.split(".")
    if not 1 <= len(parts) <= 2 or any(
        not part or not part.replace("_", "").isalnum() for part in parts
    ):
        raise ValueError(f"unsafe rank source: {source}")
    date_filter = f"AND h.rank_date < {quote_sql_literal(cutoff)}" if cutoff else ""
    # Existence is separate from MAX: a legacy row with a NULL/blank date is
    # still history, while a cold-only row at/after the cutoff is not visible.
    return f"""
        WITH RECURSIVE codes(fcode) AS (
            SELECT MIN(fcode) FROM {source}
            UNION ALL
            SELECT (SELECT MIN(fcode) FROM {source} WHERE fcode > codes.fcode)
            FROM codes WHERE codes.fcode IS NOT NULL
        )
        SELECT codes.fcode,
            (SELECT h.rank_date FROM {source} h
             WHERE h.fcode = codes.fcode {date_filter}
             ORDER BY h.rank_date DESC LIMIT 1) AS latest_rank_date
        FROM codes
        WHERE codes.fcode IS NOT NULL
          AND EXISTS (SELECT 1 FROM {source} h WHERE h.fcode = codes.fcode {date_filter})
    """


def read_latest_rank_dates(
    conn: sqlite3.Connection,
    archive_attach: dict | None = None,
) -> dict[str, str | None]:
    """Exact MAX dates over the hot/cold union, including cold-only funds."""
    queries = [build_latest_rank_dates_sql()]
    archive = archive_attach or {}
    if archive.get("attached"):
        alias = str(archive.get("alias") or "rank_archive_db")
        table = normalize_rank_archive_table_name(archive.get("archive_table_name"))
        queries.append(build_latest_rank_dates_sql(
            source=f"{alias}.{table}",
            cutoff=str(archive.get("archive_cutoff_date") or "").strip(),
        ))
    result: dict[str, str | None] = {}
    started_transaction = not conn.in_transaction
    if started_transaction:
        conn.execute("BEGIN")
    try:
        for query in queries:
            for code, latest in conn.execute(query):
                previous = result.get(code)
                # Duplicate hot/cold keys cannot affect MAX(rank_date). Avoid
                # the union's per-history-row anti-join, retaining exact MAX.
                if code not in result or (latest is not None and (previous is None or latest > previous)):
                    result[code] = latest
    finally:
        if started_transaction:
            conn.rollback()
    return result


def build_rank_history_union_source_sql(
    archive_attach: dict | None = None,
    *,
    columns: object = None,
) -> str:
    """Return a SELECT source containing hot rows and attached cold rows."""
    normalized_columns = normalize_rank_history_columns(columns)
    selected_columns = ", ".join(normalized_columns)
    hot_select = f"SELECT {selected_columns} FROM fund_rank_history"
    if not bool((archive_attach or {}).get("attached")):
        return hot_select

    alias = str((archive_attach or {}).get("alias") or "rank_archive_db").strip()
    table_name = normalize_rank_archive_table_name(
        (archive_attach or {}).get("archive_table_name") or DEFAULT_RANK_ARCHIVE_TABLE_NAME
    )
    cold_select = f"""
        SELECT {selected_columns}
        FROM {alias}.{table_name} AS archived_rank
    """
    cutoff_date = str((archive_attach or {}).get("archive_cutoff_date") or "").strip()
    identity_columns = [
        column
        for column in ("fcode", "rank_date", "period")
        if column in normalized_columns
    ]
    if not identity_columns:
        identity_columns = [normalized_columns[0]]
    identity_predicates = [
        f"hot_override.{column} IS archived_rank.{column}"
        for column in identity_columns
    ]
    cold_predicates = [
        "NOT EXISTS (\n"
        "            SELECT 1\n"
        "            FROM fund_rank_history AS hot_override\n"
        f"            WHERE {' AND '.join(identity_predicates)}\n"
        "        )"
    ]
    if cutoff_date:
        cold_predicates.insert(
            0,
            f"archived_rank.rank_date < {quote_sql_literal(cutoff_date)}",
        )
    cold_select = f"{cold_select} WHERE {' AND '.join(cold_predicates)}"
    return f"{hot_select}\nUNION ALL\n{cold_select}"
