#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Execution checkpoint for screening history cold archive governance."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.screening_history_archive_preview import (
    ARCHIVE_TABLES,
    DEFAULT_ARCHIVE_DB_PATH,
    DEFAULT_OUTPUT_DIR,
    load_latest_screening_history_archive_preview_payload,
)
from utils.screening_history_archive_manifest import (
    load_screening_archive_manifest,
    queryable_archive_shards,
    refresh_active_manifest_metadata,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCREENING_HISTORY_ARCHIVE_EXECUTION_JSON_PATTERN = (
    "screening_history_archive_execution_*.json"
)
ALLOWED_MODES = {"dry-run", "archive-export"}
HOT_DB_ALIAS = "screening_hot_db"
DEDUP_KEY_BY_TABLE = {
    "screening_runs": ("run_id",),
    "screening_decisions": ("run_id", "id"),
    "screening_picks": ("run_id", "id"),
    "screening_pick_metric_snapshots": ("run_id", "pick_id"),
    "screening_decision_reason_summary": ("run_id", "step_id", "passed", "reason_code"),
    "screening_run_latest_activity": ("run_id",),
    "screening_run_storage_stats": ("run_id",),
}


def load_latest_screening_history_archive_execution_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        SCREENING_HISTORY_ARCHIVE_EXECUTION_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _round_mb(num_bytes: int | float) -> float:
    return round(float(num_bytes or 0) / 1024 / 1024, 4)


def _normalize_mode(mode: str) -> str:
    value = _normalize_text(mode).lower().replace("_", "-") or "dry-run"
    if value not in ALLOWED_MODES:
        raise ValueError(f"unsupported screening history archive mode: {mode}")
    return value


def _normalize_run_ids(run_ids: Optional[Sequence[Any]]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in run_ids or []:
        value = _normalize_text(item)
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _iter_chunks(values: Sequence[str], chunk_size: int):
    size = max(1, int(chunk_size or 500))
    for start in range(0, len(values), size):
        yield list(values[start:start + size])


def _resolve_project_root(project_root: str | Path | None) -> Path:
    return Path(project_root).expanduser().resolve() if project_root else PROJECT_ROOT


def _resolve_path(value: Any, *, project_root: Path) -> Path:
    text = _normalize_text(value)
    path = Path(text or DEFAULT_ARCHIVE_DB_PATH).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _stringify_project_path(path: Path, *, project_root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _safe_table_name(value: str) -> str:
    text = _normalize_text(value)
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not text or any(ch not in safe_chars for ch in text):
        raise ValueError(f"unsafe table name: {value}")
    return text


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _fetch_all_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> list[dict]:
    cursor = conn.execute(sql, tuple(params))
    columns = [item[0] for item in cursor.description or []]
    result: list[dict] = []
    for row in cursor.fetchall():
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            result.append(dict(zip(columns, row)))
    return result


def _table_exists(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    schema: str = "main",
) -> bool:
    return (
        conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
            (_safe_table_name(table_name),),
        ).fetchone()
        is not None
    )


def _table_info(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    schema: str = "main",
) -> list[dict]:
    if not _table_exists(conn, table_name, schema=schema):
        return []
    return _fetch_all_dicts(
        conn,
        f"PRAGMA {schema}.table_info({_quote_ident(_safe_table_name(table_name))})",
    )


def _table_columns(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    schema: str = "main",
) -> list[str]:
    return [_normalize_text(row.get("name")) for row in _table_info(conn, table_name, schema=schema)]


def _sqlite_quick_check(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return _normalize_text(row[0] if row else "")
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _load_json_path(path_value: str | Path, *, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (project_root / path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return path, payload


def _load_preview(
    *,
    preview_payload: Optional[dict],
    source_preview_artifact: Optional[str],
    output_dir: str | Path,
    project_root: Path,
) -> tuple[str, dict]:
    if preview_payload is not None:
        return _normalize_text(source_preview_artifact), dict(preview_payload or {})
    if source_preview_artifact:
        path, payload = _load_json_path(source_preview_artifact, project_root=project_root)
        return str(path), dict(payload or {})
    path, payload = load_latest_screening_history_archive_preview_payload(
        output_dir=str(output_dir)
    )
    return str(path), dict(payload or {})


def _count_scope_rows(
    conn: sqlite3.Connection,
    table_name: str,
    run_ids: Sequence[str],
    *,
    schema: str = "main",
) -> int:
    if not run_ids or not _table_exists(conn, table_name, schema=schema):
        return 0
    columns = set(_table_columns(conn, table_name, schema=schema))
    if "run_id" not in columns:
        return 0
    total = 0
    qualified = f"{schema}.{_quote_ident(_safe_table_name(table_name))}"
    for chunk in _iter_chunks(run_ids, 900):
        placeholders = ",".join("?" for _ in chunk)
        total += _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {qualified} WHERE run_id IN ({placeholders})",
                tuple(chunk),
            ).fetchone()[0]
        )
    return total


def _open_readonly_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def _scope_counts_for_path(
    path: Path,
    run_ids: Sequence[str],
) -> dict[str, int]:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"archive shard missing: {path}")
    with closing(_open_readonly_db(path)) as conn:
        return {
            table_name: _count_scope_rows(conn, table_name, run_ids)
            for table_name in ARCHIVE_TABLES
        }


def _present_run_ids(
    conn: sqlite3.Connection,
    run_ids: Sequence[str],
    *,
    schema: str = "main",
) -> set[str]:
    normalized = _normalize_run_ids(run_ids)
    found: set[str] = set()
    if not normalized:
        return found
    for table_name in ARCHIVE_TABLES:
        if not _table_exists(conn, table_name, schema=schema):
            continue
        if "run_id" not in set(_table_columns(conn, table_name, schema=schema)):
            continue
        qualified = f"{schema}.{_quote_ident(_safe_table_name(table_name))}"
        for chunk in _iter_chunks(normalized, 900):
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT DISTINCT run_id FROM {qualified} WHERE run_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            found.update(
                _normalize_text(row[0])
                for row in rows
                if row and _normalize_text(row[0])
            )
    return found


def _add_missing_archive_columns(
    conn: sqlite3.Connection,
    table_name: str,
    hot_info: Sequence[dict],
) -> None:
    archive_columns = set(_table_columns(conn, table_name, schema="main"))
    for row in hot_info:
        name = _normalize_text(row.get("name"))
        if not name or name in archive_columns:
            continue
        col_type = _normalize_text(row.get("type")) or "TEXT"
        conn.execute(
            f"ALTER TABLE {_quote_ident(table_name)} ADD COLUMN {_quote_ident(name)} {col_type}"
        )
        archive_columns.add(name)


def _ensure_archive_table(conn: sqlite3.Connection, table_name: str) -> dict:
    table = _safe_table_name(table_name)
    if not _table_exists(conn, table, schema=HOT_DB_ALIAS):
        return {"status": "skipped", "skip_reason": "hot_table_missing", "columns": []}

    hot_info = _table_info(conn, table, schema=HOT_DB_ALIAS)
    if not _table_exists(conn, table, schema="main"):
        from database.screening_decision_payloads import canonical_source

        projection, source = canonical_source(conn, table, [row["name"] for row in hot_info], schema=HOT_DB_ALIAS)
        conn.execute(
            f"""
            CREATE TABLE {_quote_ident(table)}
            AS SELECT {projection} FROM {source} WHERE 0
            """
        )
    _add_missing_archive_columns(conn, table, hot_info)
    columns = _table_columns(conn, table, schema="main")

    if "run_id" in columns:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_archive_run_id ON {_quote_ident(table)} (run_id)"
        )
    if "id" in columns:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_archive_id ON {_quote_ident(table)} (id)"
        )
    return {"status": "ready", "columns": columns}


def _dedup_keys(table_name: str, columns: Sequence[str]) -> tuple[str, ...]:
    configured = DEDUP_KEY_BY_TABLE.get(table_name)
    if configured and all(key in columns for key in configured):
        return configured
    if "run_id" in columns and "id" in columns:
        return ("run_id", "id")
    if "id" in columns:
        return ("id",)
    if "run_id" in columns:
        return ("run_id",)
    return tuple()


def _insert_scope_rows(
    conn: sqlite3.Connection,
    table_name: str,
    run_ids: Sequence[str],
    *,
    chunk_size: int,
) -> dict:
    table = _safe_table_name(table_name)
    ready = _ensure_archive_table(conn, table)
    if ready.get("status") != "ready":
        return {
            "table_name": table,
            "status": ready.get("status") or "skipped",
            "skip_reason": ready.get("skip_reason") or "",
            "hot_scope_row_count": 0,
            "archive_scope_row_count_before": 0,
            "archive_scope_row_count_after": 0,
            "inserted_row_count": 0,
            "exact_archive_match": True,
        }

    columns = [column for column in ready.get("columns") or [] if column]
    if "run_id" not in columns:
        return {
            "table_name": table,
            "status": "failed",
            "error": "run_id_column_missing",
            "hot_scope_row_count": 0,
            "archive_scope_row_count_before": 0,
            "archive_scope_row_count_after": 0,
            "inserted_row_count": 0,
            "exact_archive_match": False,
        }
    keys = _dedup_keys(table, columns)
    if not keys:
        return {
            "table_name": table,
            "status": "failed",
            "error": "dedup_key_missing",
            "hot_scope_row_count": 0,
            "archive_scope_row_count_before": 0,
            "archive_scope_row_count_after": 0,
            "inserted_row_count": 0,
            "exact_archive_match": False,
        }

    hot_scope_before = _count_scope_rows(conn, table, run_ids, schema=HOT_DB_ALIAS)
    archive_before = _count_scope_rows(conn, table, run_ids, schema="main")
    inserted = 0
    column_sql = ", ".join(_quote_ident(column) for column in columns)
    from database.screening_decision_payloads import canonical_source, verify_reference_scope

    if table == "screening_decisions":
        verify_reference_scope(conn, run_ids, schema=HOT_DB_ALIAS)
    select_sql, source_sql = canonical_source(conn, table, columns, schema=HOT_DB_ALIAS)
    dedup_predicate = " AND ".join(
        f"((a.{_quote_ident(key)} = h.{_quote_ident(key)}) OR "
        f"(a.{_quote_ident(key)} IS NULL AND h.{_quote_ident(key)} IS NULL))"
        for key in keys
    )

    for chunk in _iter_chunks(run_ids, chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        before_changes = conn.total_changes
        conn.execute(
            f"""
            INSERT INTO {_quote_ident(table)} ({column_sql})
            SELECT {select_sql}
            FROM {source_sql}
            WHERE h.run_id IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1
                  FROM {_quote_ident(table)} a
                  WHERE {dedup_predicate}
              )
            """,
            tuple(chunk),
        )
        inserted += conn.total_changes - before_changes

    archive_after = _count_scope_rows(conn, table, run_ids, schema="main")
    exact_values_match = _scope_values_match(conn, table, run_ids)
    return {
        "table_name": table,
        "status": "success",
        "hot_scope_row_count": hot_scope_before,
        "archive_scope_row_count_before": archive_before,
        "archive_scope_row_count_after": archive_after,
        "inserted_row_count": inserted,
        "dedup_keys": list(keys),
        "exact_values_match": exact_values_match,
        "exact_archive_match": archive_after == hot_scope_before and exact_values_match,
    }


def _scope_values_match(conn: sqlite3.Connection, table: str, run_ids: Sequence[str]) -> bool:
    """Count equality alone cannot establish recoverability of existing rows."""
    from database.screening_decision_payloads import canonical_source, verify_reference_scope

    if not run_ids:
        return True
    columns = _table_columns(conn, table, schema=HOT_DB_ALIAS)
    if not columns:
        return _count_scope_rows(conn, table, run_ids) == 0
    if set(columns) != set(_table_columns(conn, table)):
        return False
    if table == "screening_decisions":
        verify_reference_scope(conn, run_ids, schema=HOT_DB_ALIAS)
    projection, source = canonical_source(conn, table, columns, schema=HOT_DB_ALIAS)
    keys = _dedup_keys(table, columns)
    if not keys:
        return False
    match = " AND ".join(f'a.{_quote_ident(key)} IS h.{_quote_ident(key)}' for key in keys)
    values = " AND ".join(
        f'(a.{_quote_ident(column)} IS h.{_quote_ident(column)} AND '
        f'typeof(a.{_quote_ident(column)})=typeof(h.{_quote_ident(column)}))' for column in columns)
    for chunk in _iter_chunks(run_ids, 700):
        marks = ",".join("?" for _ in chunk)
        difference = conn.execute(
            f'WITH original AS (SELECT {projection} FROM {source} WHERE h.run_id IN ({marks})) '
            f'SELECT 1 FROM original h WHERE NOT EXISTS '
            f'(SELECT 1 FROM {_quote_ident(table)} a WHERE {match} AND {values}) LIMIT 1', chunk,
        ).fetchone()
        if difference is not None:
            return False
    return True


def _execute_archive_export(
    *,
    db_path: Path,
    archive_target_path: Path,
    run_ids: Sequence[str],
    chunk_size: int,
    immutable_archive_shards: Sequence[dict[str, Any]] = (),
) -> dict:
    if not db_path.exists() or not db_path.is_file():
        return {
            "status": "failed",
            "error": f"hot database missing: {db_path}",
            "table_results": [],
        }
    shard_checks: list[dict[str, Any]] = []
    immutable_counts: dict[str, int] = {table_name: 0 for table_name in ARCHIVE_TABLES}
    immutable_presence: dict[str, list[str]] = {}
    try:
        for shard in immutable_archive_shards:
            shard_path = Path(str(shard.get("absolute_path") or "")).resolve()
            if shard.get("format") == "parquet":
                from utils.screening_archive_access import probe_parquet_shard, verify_parquet_hot_values

                probe = probe_parquet_shard(shard, run_ids)
                if probe["present_run_ids"]:
                    with closing(_open_readonly_db(db_path)) as hot_conn:
                        probe["check"]["hot_value_verification"] = verify_parquet_hot_values(
                            shard, probe["present_run_ids"], hot_conn,
                        )
                shard_checks.append(probe["check"])
                for run_id in probe["present_run_ids"]:
                    immutable_presence.setdefault(run_id, []).append(str(shard_path))
                for table_name in ARCHIVE_TABLES:
                    immutable_counts[table_name] += probe["table_counts"][table_name]
                continue
            exists = shard_path.exists() and shard_path.is_file()
            stat = shard_path.stat() if exists else None
            current_size = int(stat.st_size) if stat else 0
            current_mtime_ns = int(stat.st_mtime_ns) if stat else 0
            verified_size = _normalize_int(
                shard.get("quick_check_verified_size_bytes")
                or shard.get("declared_file_size_bytes")
            )
            verified_mtime_ns = _normalize_int(
                shard.get("quick_check_verified_mtime_ns")
                or shard.get("declared_file_mtime_ns")
            )
            identity_verified = bool(
                exists
                and _normalize_text(shard.get("quick_check")) == "ok"
                and verified_size > 0
                and verified_size == current_size
                and verified_mtime_ns > 0
                and verified_mtime_ns == current_mtime_ns
            )
            quick_check = (
                "trusted_manifest_identity"
                if identity_verified
                else (_sqlite_quick_check(shard_path) if exists else "")
            )
            shard_check = {
                "id": shard.get("id") or "",
                "path": str(shard_path),
                "role": "immutable",
                "exists": exists,
                "file_size_bytes": current_size,
                "file_mtime_ns": current_mtime_ns,
                "identity_verified": identity_verified,
                "quick_check": quick_check,
                "ok": exists and quick_check in {"ok", "trusted_manifest_identity"},
            }
            shard_checks.append(shard_check)
            if not shard_check["ok"]:
                return {
                    "status": "failed",
                    "error": (
                        f"immutable_archive_shard_unavailable:{shard_path}:"
                        f"quick_check={quick_check}"
                    ),
                    "table_results": [],
                    "shard_checks": shard_checks,
                }
            with closing(_open_readonly_db(shard_path)) as shard_conn:
                present = _present_run_ids(shard_conn, run_ids)
                shard_conn.execute(f"ATTACH DATABASE ? AS {HOT_DB_ALIAS}", (_sqlite_readonly_uri(db_path),))
                shard_conn.execute("BEGIN")
                for table_name in ARCHIVE_TABLES:
                    if not _scope_values_match(shard_conn, table_name, sorted(present)):
                        raise ValueError(f"immutable_sqlite_hot_values_mismatch:{table_name}")
                shard_check["full_hot_values_match"] = True
                for run_id in present:
                    immutable_presence.setdefault(run_id, []).append(str(shard_path))
                for table_name in ARCHIVE_TABLES:
                    immutable_counts[table_name] += _count_scope_rows(
                        shard_conn,
                        table_name,
                        run_ids,
                    )
    except Exception as exc:
        return {
            "status": "failed",
            "error": f"immutable_archive_probe_failed:{type(exc).__name__}:{exc}",
            "table_results": [],
            "shard_checks": shard_checks,
        }

    duplicate_immutable_runs = {
        run_id: paths
        for run_id, paths in immutable_presence.items()
        if len(set(paths)) > 1
    }
    if duplicate_immutable_runs:
        return {
            "status": "failed",
            "error": "duplicate_run_across_immutable_archive_shards",
            "duplicate_archive_run_ids": sorted(duplicate_immutable_runs),
            "table_results": [],
            "shard_checks": shard_checks,
        }

    archive_target_path.parent.mkdir(parents=True, exist_ok=True)
    hot_uri = _sqlite_readonly_uri(db_path)
    conn = sqlite3.connect(
        str(archive_target_path),
        timeout=120,
        isolation_level=None,
        uri=True,
    )
    conn.row_factory = sqlite3.Row
    attached = False
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute(f"ATTACH DATABASE ? AS {HOT_DB_ALIAS}", (hot_uri,))
        attached = True
        conn.execute("BEGIN IMMEDIATE")
        active_presence_before = _present_run_ids(conn, run_ids)
        cross_shard_duplicates = sorted(active_presence_before.intersection(immutable_presence))
        if cross_shard_duplicates:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": "duplicate_run_across_active_and_immutable_archive_shards",
                "duplicate_archive_run_ids": cross_shard_duplicates,
                "table_results": [],
                "shard_checks": shard_checks,
            }
        write_run_ids = [run_id for run_id in run_ids if run_id not in immutable_presence]
        table_results: list[dict[str, Any]] = []
        for table_name in ARCHIVE_TABLES:
            target_before = _count_scope_rows(conn, table_name, run_ids)
            row = _insert_scope_rows(
                conn,
                table_name,
                write_run_ids,
                chunk_size=chunk_size,
            )
            hot_full = _count_scope_rows(
                conn,
                table_name,
                run_ids,
                schema=HOT_DB_ALIAS,
            )
            target_after = _count_scope_rows(conn, table_name, run_ids)
            immutable_count = _normalize_int(immutable_counts.get(table_name))
            aggregate_before = target_before + immutable_count
            aggregate_after = target_after + immutable_count
            row.update(
                {
                    "hot_scope_row_count": hot_full,
                    "archive_scope_row_count_before": aggregate_before,
                    "archive_scope_row_count_after": aggregate_after,
                    "target_archive_scope_row_count_before": target_before,
                    "target_archive_scope_row_count_after": target_after,
                    "immutable_archive_scope_row_count": immutable_count,
                    "exact_archive_match": aggregate_after == hot_full and row.get("exact_values_match", True),
                }
            )
            table_results.append(row)
        failed_tables = [
            row for row in table_results
            if row.get("status") == "failed" or not bool(row.get("exact_archive_match", True))
        ]
        if failed_tables:
            conn.execute("ROLLBACK")
            return {
                "status": "failed",
                "error": "archive_verification_failed",
                "table_results": table_results,
                "shard_checks": shard_checks,
            }
        conn.execute("COMMIT")
    except Exception as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}:{exc}",
            "table_results": [],
            "shard_checks": shard_checks,
        }
    finally:
        if attached:
            try:
                conn.execute(f"DETACH DATABASE {HOT_DB_ALIAS}")
            except sqlite3.Error:
                pass
        conn.close()

    return {
        "status": "archive-exported",
        "error": "",
        "table_results": table_results,
        "shard_checks": shard_checks,
        "scope_run_count_written_to_active": len(write_run_ids),
        "scope_run_count_reused_from_immutable": len(run_ids) - len(write_run_ids),
    }


def _build_findings(summary: dict) -> list[str]:
    mode = _normalize_text(summary.get("mode"))
    status = _normalize_text(summary.get("status"))
    run_count = _normalize_int(summary.get("scope_run_count"))
    inserted = _normalize_int(summary.get("inserted_row_count"))
    archived = _normalize_int(summary.get("archive_scope_row_count_after"))
    if status == "failed":
        return [
            f"screening history archive {mode} failed: {summary.get('error') or 'unknown_error'}",
            "热库未删除；需要修复归档阻断后再继续 cleanup/hot-delete。",
        ]
    if mode == "dry-run":
        return [
            f"screening history archive dry-run 已生成，scope run {run_count} 个。",
            "dry-run 不创建或写入冷库，也不会删除热库。",
        ]
    if status == "skipped":
        return ["当前没有筛选历史归档范围，archive-export 跳过。"]
    return [
        f"screening history archive-export 已完成，scope run {run_count} 个，新增冷库行 {inserted} 行。",
        f"冷库中当前 scope 行数 {archived} 行；热库尚未删除，后续仍需受控 cleanup 与 VACUUM 才能缩小主库文件。",
    ]


def execute_screening_history_archive(
    *,
    db_path: str | Path | None = None,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    mode: str = "dry-run",
    archive_target_path: str | Path | None = None,
    project_root: str | Path | None = None,
    chunk_size: int = 500,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path, dict]:
    root = _resolve_project_root(project_root)
    normalized_mode = _normalize_mode(mode)
    source_preview_artifact, preview_payload = _load_preview(
        preview_payload=preview_payload,
        source_preview_artifact=source_preview_artifact,
        output_dir=output_dir,
        project_root=root,
    )
    preview_summary = preview_payload.get("summary") or {}
    run_ids = _normalize_run_ids(preview_payload.get("scope_run_ids") or [])
    resolved_db_path = _resolve_path(
        db_path or preview_payload.get("db_path") or "backend/data/bond_funds.db",
        project_root=root,
    )
    resolved_archive_path = _resolve_path(
        archive_target_path
        or preview_summary.get("archive_target_path")
        or DEFAULT_ARCHIVE_DB_PATH,
        project_root=root,
    )
    archive_manifest_state = load_screening_archive_manifest(
        resolved_archive_path,
        project_root=root,
    )

    preflight_ready = bool(preview_summary.get("cleanup_preflight_ready"))
    archive_result = {"status": "dry-run", "table_results": [], "error": ""}
    status = "dry-run"
    if normalized_mode == "archive-export":
        if not run_ids:
            status = "skipped"
            archive_result = {"status": "skipped", "skip_reason": "no_scope", "table_results": [], "error": ""}
        elif not preflight_ready:
            status = "failed"
            archive_result = {
                "status": "failed",
                "error": "cleanup_preflight_not_ready",
                "table_results": [],
            }
        elif archive_manifest_state.get("status") == "invalid":
            status = "failed"
            archive_result = {
                "status": "failed",
                "error": "archive_manifest_invalid:"
                + "; ".join(archive_manifest_state.get("errors") or ["unknown_error"]),
                "table_results": [],
            }
        else:
            archive_result = _execute_archive_export(
                db_path=resolved_db_path,
                archive_target_path=resolved_archive_path,
                run_ids=run_ids,
                chunk_size=max(1, int(chunk_size or 500)),
                immutable_archive_shards=[
                    shard
                    for shard in queryable_archive_shards(archive_manifest_state)
                    if shard.get("role") == "immutable"
                ],
            )
            status = _normalize_text(archive_result.get("status")) or "failed"

    manifest_update_error = ""
    archive_db_quick_check = ""
    if status == "archive-exported":
        archive_db_quick_check = _sqlite_quick_check(resolved_archive_path)
        if archive_db_quick_check != "ok":
            archive_result["error"] = (
                f"archive_db_quick_check={archive_db_quick_check}"
            )
            status = "failed"

    if status == "archive-exported":
        try:
            manifest_path, manifest_payload = refresh_active_manifest_metadata(
                resolved_archive_path,
                project_root=root,
                metadata={
                    "last_archive_at": datetime.now().isoformat(),
                    "last_archive_scope_run_count": len(run_ids),
                    "last_archive_inserted_row_count": sum(
                        _normalize_int(row.get("inserted_row_count"))
                        for row in archive_result.get("table_results") or []
                    ),
                    "quick_check": "ok",
                    "quick_check_verified_at": datetime.now().isoformat(),
                    "quick_check_verified_size_bytes": resolved_archive_path.stat().st_size,
                    "quick_check_verified_mtime_ns": resolved_archive_path.stat().st_mtime_ns,
                },
            )
            archive_manifest_state = load_screening_archive_manifest(
                resolved_archive_path,
                manifest_path=manifest_path,
                project_root=root,
            )
        except Exception as exc:
            manifest_update_error = f"{type(exc).__name__}:{exc}"
            archive_result["error"] = f"archive_manifest_update_failed:{manifest_update_error}"
            status = "failed"

    table_results = list(archive_result.get("table_results") or [])
    inserted_row_count = sum(_normalize_int(row.get("inserted_row_count")) for row in table_results)
    hot_scope_row_count = sum(_normalize_int(row.get("hot_scope_row_count")) for row in table_results)
    archive_before = sum(_normalize_int(row.get("archive_scope_row_count_before")) for row in table_results)
    archive_after = sum(_normalize_int(row.get("archive_scope_row_count_after")) for row in table_results)
    failed_table_count = sum(
        1
        for row in table_results
        if row.get("status") == "failed" or not bool(row.get("exact_archive_match", True))
    )

    archive_exists = resolved_archive_path.exists() and resolved_archive_path.is_file()
    archive_size_bytes = resolved_archive_path.stat().st_size if archive_exists else 0
    if not archive_db_quick_check and archive_exists:
        archive_db_quick_check = _sqlite_quick_check(resolved_archive_path)
    summary = {
        "status": status,
        "mode": normalized_mode,
        "error": archive_result.get("error") or "",
        "skip_reason": archive_result.get("skip_reason") or "",
        "scope_run_count": len(run_ids),
        "cleanup_preflight_ready": preflight_ready,
        "archive_target_path": str(resolved_archive_path),
        "archive_target_project_path": _stringify_project_path(resolved_archive_path, project_root=root),
        "archive_db_exists": archive_exists,
        "archive_db_size_bytes": archive_size_bytes,
        "archive_db_size_mb": _round_mb(archive_size_bytes),
        "archive_db_quick_check": archive_db_quick_check,
        "archive_manifest_status": archive_manifest_state.get("status") or "",
        "archive_manifest_path": archive_manifest_state.get("manifest_path") or "",
        "archive_manifest_generation": _normalize_int(archive_manifest_state.get("generation")),
        "archive_manifest_error": manifest_update_error,
        "archive_manifest_errors": list(archive_manifest_state.get("errors") or []),
        "archive_shard_count": len(archive_manifest_state.get("shards") or []),
        "archive_immutable_shard_count": sum(
            1
            for shard in archive_manifest_state.get("shards") or []
            if shard.get("role") == "immutable"
        ),
        "hot_scope_row_count": hot_scope_row_count,
        "archive_scope_row_count_before": archive_before,
        "archive_scope_row_count_after": archive_after,
        "inserted_row_count": inserted_row_count,
        "failed_table_count": failed_table_count,
        "table_count": len(table_results),
        "chunk_size": max(1, int(chunk_size or 500)),
        "scope_run_count_written_to_active": _normalize_int(
            archive_result.get("scope_run_count_written_to_active")
        ),
        "scope_run_count_reused_from_immutable": _normalize_int(
            archive_result.get("scope_run_count_reused_from_immutable")
        ),
    }
    payload = {
        "generated_at": datetime.now().isoformat(),
        "status": status,
        "mode": normalized_mode,
        "db_path": str(resolved_db_path),
        "archive_target_path": str(resolved_archive_path),
        "source_screening_history_archive_preview_artifact": source_preview_artifact,
        "summary": summary,
        "table_results": table_results,
        "archive_shard_checks": list(archive_result.get("shard_checks") or []),
        "archive_manifest": {
            "status": archive_manifest_state.get("status") or "",
            "manifest_path": archive_manifest_state.get("manifest_path") or "",
            "generation": _normalize_int(archive_manifest_state.get("generation")),
            "errors": list(archive_manifest_state.get("errors") or []),
            "shards": [
                {
                    "id": shard.get("id"),
                    "role": shard.get("role"),
                    "format": shard.get("format"),
                    "state": shard.get("state"),
                    "absolute_path": shard.get("absolute_path"),
                    "exists": bool(shard.get("exists")),
                    "file_size_bytes": _normalize_int(shard.get("file_size_bytes")),
                    "file_mtime_ns": _normalize_int(shard.get("file_mtime_ns")),
                    "storage_size_bytes": _normalize_int(shard.get("storage_size_bytes")),
                    "manifest_sha256": shard.get("manifest_sha256"),
                    "source_sha256": shard.get("source_sha256"),
                    "quick_check": shard.get("quick_check") or "",
                    "quick_check_verified_at": shard.get("quick_check_verified_at") or "",
                    "quick_check_verified_size_bytes": _normalize_int(
                        shard.get("quick_check_verified_size_bytes")
                    ),
                    "quick_check_verified_mtime_ns": _normalize_int(
                        shard.get("quick_check_verified_mtime_ns")
                    ),
                }
                for shard in archive_manifest_state.get("shards") or []
            ],
        },
        "scope_run_ids": run_ids,
        "sample_scope_run_ids": run_ids[:20],
        "guardrails": [
            "archive-export 只写入冷库，不删除 hot DB 中的任何筛选历史行。",
            "只有 cleanup preflight ready_for_manual_signoff=true 才允许 archive-export。",
            "归档使用 run_id 范围和行级去重键，重复执行应保持幂等。",
            "manifest 分片模式会先复用不可变分片中的完整 run，并对所有分片逐表汇总核验；跨分片重复 run 即阻断。",
            "主库体积回收必须等待后续 cleanup apply 与 VACUUM/replace 证据链。",
        ],
    }
    payload["findings"] = _build_findings(summary)
    json_path, md_path = write_screening_history_archive_execution_artifacts(
        payload,
        output_dir=output_dir,
        timestamp=timestamp,
    )
    return json_path, md_path, payload


def render_screening_history_archive_execution_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening history archive execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {payload.get('status')}",
        f"- mode: {payload.get('mode')}",
        f"- db_path: {payload.get('db_path')}",
        f"- archive_target_path: {payload.get('archive_target_path')}",
        f"- scope_run_count: {summary.get('scope_run_count', 0)}",
        f"- inserted_row_count: {summary.get('inserted_row_count', 0)}",
        f"- archive_scope_row_count_after: {summary.get('archive_scope_row_count_after', 0)}",
        f"- archive_db_quick_check: {summary.get('archive_db_quick_check', '')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## table_results"])
    for row in payload.get("table_results") or []:
        lines.append(
            "- "
            + json.dumps(
                {
                    "table_name": row.get("table_name"),
                    "status": row.get("status"),
                    "hot_scope_row_count": row.get("hot_scope_row_count"),
                    "archive_scope_row_count_after": row.get("archive_scope_row_count_after"),
                    "inserted_row_count": row.get("inserted_row_count"),
                    "exact_archive_match": row.get("exact_archive_match"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    lines.extend(["", "## guardrails"])
    for item in payload.get("guardrails") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_screening_history_archive_execution_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    artifact_dir = _resolve_output_dir(output_dir)
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"screening_history_archive_execution_{ts}.json"
    md_path = artifact_dir / f"screening_history_archive_execution_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_history_archive_execution_markdown(payload), encoding="utf-8")
    return json_path, md_path
