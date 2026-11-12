#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only preview for screening history hot/cold archive governance."""

from __future__ import annotations

import json
from contextlib import closing
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.screening_history_cleanup_preflight import (
    load_latest_screening_history_cleanup_preflight_payload,
)
from utils.screening_history_cleanup_preview import (
    load_latest_screening_history_cleanup_preview_payload,
)
from utils.screening_history_archive_manifest import (
    load_screening_archive_manifest,
    total_existing_shard_bytes,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/screening_history_archive"
DEFAULT_ARCHIVE_DB_PATH = "backend/data/archives/screening_history_details_archive.db"
SCREENING_HISTORY_ARCHIVE_PREVIEW_JSON_PATTERN = (
    "screening_history_archive_preview_*.json"
)
ARCHIVE_TABLES = (
    "screening_runs",
    "screening_decisions",
    "screening_picks",
    "screening_pick_metric_snapshots",
    "screening_decision_reason_summary",
    "screening_run_latest_activity",
    "screening_run_storage_stats",
)
QUICK_CHECK_SIZE_LIMIT_BYTES = 1024 * 1024 * 1024


def load_latest_screening_history_archive_preview_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        SCREENING_HISTORY_ARCHIVE_PREVIEW_JSON_PATTERN,
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


def _iter_chunks(values: Sequence[str], chunk_size: int = 900):
    size = max(1, int(chunk_size or 900))
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


def _fetch_one_dict(
    conn: sqlite3.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> dict:
    cursor = conn.execute(sql, tuple(params))
    row = cursor.fetchone()
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return dict(row)
    columns = [item[0] for item in cursor.description or []]
    return dict(zip(columns, row))


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


def _count_rows_for_run_ids(
    conn: sqlite3.Connection,
    table_name: str,
    run_ids: Sequence[str],
) -> int:
    if not run_ids or not _table_exists(conn, table_name):
        return 0
    columns = _table_columns(conn, table_name)
    if "run_id" not in columns:
        return 0

    total = 0
    for chunk in _iter_chunks(run_ids):
        placeholders = ",".join("?" for _ in chunk)
        total += _normalize_int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE run_id IN ({placeholders})",
                tuple(chunk),
            ).fetchone()[0]
        )
    return total


def _count_total_rows(conn: sqlite3.Connection, table_name: str) -> int:
    if not _table_exists(conn, table_name):
        return 0
    return _normalize_int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _dbstat_available(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT 1 FROM dbstat LIMIT 1").fetchone()
        return True
    except sqlite3.Error:
        return False


def _table_storage_bytes(conn: sqlite3.Connection, table_name: str) -> dict:
    if not _dbstat_available(conn):
        return {
            "dbstat_available": False,
            "table_bytes": 0,
            "index_bytes": 0,
            "total_bytes": 0,
            "index_names": [],
        }
    index_rows = _fetch_all_dicts(
        conn,
        """
        SELECT name
        FROM sqlite_master
        WHERE type='index' AND tbl_name=?
        """,
        (table_name,),
    )
    index_names = [_normalize_text(row.get("name")) for row in index_rows if _normalize_text(row.get("name"))]
    names = [table_name, *index_names]
    placeholders = ",".join("?" for _ in names)
    rows = _fetch_all_dicts(
        conn,
        f"""
        SELECT name, SUM(pgsize) AS bytes
        FROM dbstat
        WHERE name IN ({placeholders})
        GROUP BY name
        """,
        tuple(names),
    )
    byte_map = {_normalize_text(row.get("name")): _normalize_int(row.get("bytes")) for row in rows}
    table_bytes = byte_map.get(table_name, 0)
    index_bytes = sum(byte_map.get(name, 0) for name in index_names)
    return {
        "dbstat_available": True,
        "table_bytes": table_bytes,
        "index_bytes": index_bytes,
        "total_bytes": table_bytes + index_bytes,
        "index_names": index_names,
    }


def _sqlite_quick_check(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(_open_readonly_db(path)) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return _normalize_text(row[0] if row else "")
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _sqlite_readonly_probe(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(_open_readonly_db(path)) as conn:
            row = conn.execute("SELECT 1").fetchone()
        return "ok" if row and _normalize_int(row[0]) == 1 else "unexpected_result"
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _load_json_path(path_value: str | Path, *, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (project_root / path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return path, payload


def _load_cleanup_preview(
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
    path, payload = load_latest_screening_history_cleanup_preview_payload(
        output_dir=str(output_dir)
    )
    return str(path), dict(payload or {})


def _load_cleanup_preflight(
    *,
    preflight_payload: Optional[dict],
    source_preflight_artifact: Optional[str],
    output_dir: str | Path,
    project_root: Path,
) -> tuple[str, dict]:
    if preflight_payload is not None:
        return _normalize_text(source_preflight_artifact), dict(preflight_payload or {})
    if source_preflight_artifact:
        path, payload = _load_json_path(source_preflight_artifact, project_root=project_root)
        return str(path), dict(payload or {})
    try:
        path, payload = load_latest_screening_history_cleanup_preflight_payload(
            output_dir=str(output_dir)
        )
        return str(path), dict(payload or {})
    except FileNotFoundError:
        return "", {}


def _build_findings(summary: dict) -> list[str]:
    scope_run_count = _normalize_int(summary.get("scope_run_count"))
    total_scope_rows = _normalize_int(summary.get("total_scope_row_count"))
    estimated_mb = float(summary.get("estimated_hot_reclaimable_mb") or 0.0)
    preflight_ready = bool(summary.get("cleanup_preflight_ready"))
    archive_exists = bool(summary.get("archive_db_exists"))

    findings: list[str] = []
    if scope_run_count <= 0:
        findings.append("当前 cleanup 范围没有可归档的 screening history run。")
    else:
        findings.append(
            f"当前 cleanup 范围包含 {scope_run_count} 个 run，"
            f"筛选历史明细/摘要合计 {total_scope_rows} 行可先归档到冷库。"
        )
        findings.append(
            f"按当前 dbstat 粗估，这批明细及相关索引对应热库体积约 {estimated_mb:.2f} MB；"
            "实际缩小主库仍需后续热删和受控 VACUUM。"
        )
    if preflight_ready:
        findings.append("cleanup preflight 已确认 live DB 与 preview/runbook 范围一致，可作为归档范围依据。")
    else:
        findings.append("cleanup preflight 未确认范围一致；archive-export 执行阶段应保持阻断。")
    findings.append(
        "归档预览不会删除热库数据，也不会改变发布口径；冷库用于后续热删前的数据可恢复证据。"
    )
    if archive_exists:
        findings.append(
            f"目标冷库已存在，当前大小 {float(summary.get('archive_db_size_mb') or 0.0):.2f} MB。"
        )
    return findings


def build_screening_history_archive_preview_payload(
    *,
    db_path: str | Path | None = None,
    preview_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    archive_target_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> dict:
    """Build a read-only archive preview from the existing cleanup scope."""
    root = _resolve_project_root(project_root)
    source_preview_artifact, preview_payload = _load_cleanup_preview(
        preview_payload=preview_payload,
        source_preview_artifact=source_preview_artifact,
        output_dir=output_dir,
        project_root=root,
    )
    source_preflight_artifact, preflight_payload = _load_cleanup_preflight(
        preflight_payload=preflight_payload,
        source_preflight_artifact=source_preflight_artifact,
        output_dir=output_dir,
        project_root=root,
    )

    execution_plan = preview_payload.get("execution_plan") or {}
    preview_summary = preview_payload.get("summary") or {}
    preflight_checks = preflight_payload.get("checks") or {}
    scope_run_ids = _normalize_run_ids(execution_plan.get("scope_run_ids") or [])

    resolved_db_path = _resolve_path(
        db_path or preview_payload.get("db_path") or "backend/data/bond_funds.db",
        project_root=root,
    )
    resolved_archive_path = _resolve_path(
        archive_target_path or DEFAULT_ARCHIVE_DB_PATH,
        project_root=root,
    )
    archive_manifest = load_screening_archive_manifest(
        resolved_archive_path,
        project_root=root,
    )

    table_rows: list[dict] = []
    total_scope_rows = 0
    estimated_hot_reclaimable_bytes = 0
    database_available = resolved_db_path.exists() and resolved_db_path.is_file()
    database_error = ""
    db_file_size_bytes = resolved_db_path.stat().st_size if database_available else 0
    db_quick_check = ""
    if database_available:
        try:
            with closing(_open_readonly_db(resolved_db_path)) as conn:
                db_quick_check = (
                    _sqlite_quick_check(resolved_db_path)
                    if db_file_size_bytes <= QUICK_CHECK_SIZE_LIMIT_BYTES
                    else "skipped_large_database"
                )
                for table_name in ARCHIVE_TABLES:
                    exists = _table_exists(conn, table_name)
                    total_rows = _count_total_rows(conn, table_name) if exists else 0
                    scope_rows = _count_rows_for_run_ids(conn, table_name, scope_run_ids) if exists else 0
                    storage = _table_storage_bytes(conn, table_name) if exists else {
                        "dbstat_available": False,
                        "table_bytes": 0,
                        "index_bytes": 0,
                        "total_bytes": 0,
                        "index_names": [],
                    }
                    estimated_bytes = (
                        int(float(storage.get("total_bytes") or 0) * (scope_rows / total_rows))
                        if total_rows > 0 and scope_rows > 0
                        else 0
                    )
                    total_scope_rows += scope_rows
                    estimated_hot_reclaimable_bytes += estimated_bytes
                    table_rows.append(
                        {
                            "table_name": table_name,
                            "exists": exists,
                            "total_row_count": total_rows,
                            "scope_row_count": scope_rows,
                            "scope_row_share_pct": (
                                round(scope_rows / total_rows * 100, 4)
                                if total_rows > 0
                                else None
                            ),
                            "storage_total_bytes": int(storage.get("total_bytes") or 0),
                            "storage_table_bytes": int(storage.get("table_bytes") or 0),
                            "storage_index_bytes": int(storage.get("index_bytes") or 0),
                            "estimated_hot_reclaimable_bytes": estimated_bytes,
                            "estimated_hot_reclaimable_mb": _round_mb(estimated_bytes),
                            "index_names": list(storage.get("index_names") or []),
                        }
                    )
        except sqlite3.Error as exc:
            database_available = False
            database_error = str(exc)

    archive_exists = resolved_archive_path.exists() and resolved_archive_path.is_file()
    archive_db_size_bytes = resolved_archive_path.stat().st_size if archive_exists else 0
    preflight_ready = bool(preflight_checks.get("ready_for_manual_signoff"))
    status = "no_scope" if not scope_run_ids else "success"
    if not database_available:
        status = "failed"
    if archive_manifest.get("status") == "invalid":
        status = "failed"

    summary = {
        "status": status,
        "database_available": database_available,
        "database_error": database_error,
        "db_file_size_bytes": db_file_size_bytes,
        "db_file_size_mb": _round_mb(db_file_size_bytes),
        "db_quick_check": db_quick_check,
        "scope_run_count": len(scope_run_ids),
        "total_scope_row_count": total_scope_rows,
        "estimated_hot_reclaimable_bytes": estimated_hot_reclaimable_bytes,
        "estimated_hot_reclaimable_mb": _round_mb(estimated_hot_reclaimable_bytes),
        "archive_target_path": str(resolved_archive_path),
        "archive_target_project_path": _stringify_project_path(resolved_archive_path, project_root=root),
        "archive_db_exists": archive_exists,
        "archive_db_size_bytes": archive_db_size_bytes,
        "archive_db_size_mb": _round_mb(archive_db_size_bytes),
        "archive_db_readonly_probe": (
            _sqlite_readonly_probe(resolved_archive_path) if archive_exists else ""
        ),
        "archive_db_quick_check": (
            _sqlite_quick_check(resolved_archive_path)
            if archive_exists and archive_db_size_bytes <= QUICK_CHECK_SIZE_LIMIT_BYTES
            else ("skipped_large_archive" if archive_exists else "")
        ),
        "archive_manifest_status": archive_manifest.get("status") or "",
        "archive_manifest_path": archive_manifest.get("manifest_path") or "",
        "archive_manifest_generation": _normalize_int(archive_manifest.get("generation")),
        "archive_manifest_errors": list(archive_manifest.get("errors") or []),
        "archive_shard_count": len(archive_manifest.get("shards") or []),
        "archive_immutable_shard_count": sum(
            1
            for shard in archive_manifest.get("shards") or []
            if shard.get("role") == "immutable"
        ),
        "archive_total_shard_size_bytes": total_existing_shard_bytes(
            archive_manifest.get("shards") or []
        ),
        "archive_total_shard_size_mb": _round_mb(
            total_existing_shard_bytes(archive_manifest.get("shards") or [])
        ),
        "cleanup_preflight_ready": preflight_ready,
        "cleanup_preflight_status": "ready" if preflight_ready else "not_ready",
        "cleanup_preview_candidate_run_count": _normalize_int(preview_summary.get("candidate_run_count")),
        "cleanup_preview_estimated_rows_to_delete": _normalize_int(preview_summary.get("estimated_rows_to_delete")),
    }
    payload = {
        "generated_at": datetime.now().isoformat(),
        "status": status,
        "db_path": str(resolved_db_path),
        "archive_target_path": str(resolved_archive_path),
        "source_screening_history_cleanup_preview_artifact": source_preview_artifact,
        "source_screening_history_cleanup_preflight_artifact": source_preflight_artifact,
        "summary": summary,
        "table_scope": table_rows,
        "scope_run_ids": scope_run_ids,
        "archive_manifest": {
            "status": archive_manifest.get("status") or "",
            "manifest_path": archive_manifest.get("manifest_path") or "",
            "generation": _normalize_int(archive_manifest.get("generation")),
            "errors": list(archive_manifest.get("errors") or []),
            "shards": [
                {
                    "id": shard.get("id"),
                    "role": shard.get("role"),
                    "format": shard.get("format"),
                    "state": shard.get("state"),
                    "absolute_path": shard.get("absolute_path"),
                    "exists": bool(shard.get("exists")),
                    "file_size_bytes": _normalize_int(shard.get("file_size_bytes")),
                }
                for shard in archive_manifest.get("shards") or []
            ],
        },
        "sample_scope_run_ids": scope_run_ids[:20],
        "guardrails": [
            "本预览只读取 hot DB 和 cleanup 产物，不写入冷库，不删除热库。",
            "archive-export 只允许在 cleanup preflight ready_for_manual_signoff=true 时执行。",
            "归档冷库不是发布事实来源；前端和报表仍以正式发布链路为准。",
            "热库文件体积只有在归档、热删、VACUUM/replace 三步完成后才会实质下降。",
            "存在 shard manifest 时，后续 archive/cleanup 必须逐分片精确核验；manifest 损坏、越界或分片缺失即阻断。",
        ],
    }
    payload["findings"] = _build_findings(summary)
    return payload


def render_screening_history_archive_preview_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening history archive preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {payload.get('status')}",
        f"- db_path: {payload.get('db_path')}",
        f"- archive_target_path: {payload.get('archive_target_path')}",
        f"- scope_run_count: {summary.get('scope_run_count', 0)}",
        f"- total_scope_row_count: {summary.get('total_scope_row_count', 0)}",
        f"- estimated_hot_reclaimable_mb: {summary.get('estimated_hot_reclaimable_mb', 0)}",
        f"- cleanup_preflight_ready: {summary.get('cleanup_preflight_ready')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## table_scope"])
    for row in payload.get("table_scope") or []:
        lines.append(
            "- "
            + json.dumps(
                {
                    "table_name": row.get("table_name"),
                    "scope_row_count": row.get("scope_row_count"),
                    "total_row_count": row.get("total_row_count"),
                    "estimated_hot_reclaimable_mb": row.get("estimated_hot_reclaimable_mb"),
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


def write_screening_history_archive_preview_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    artifact_dir = _resolve_output_dir(output_dir)
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"screening_history_archive_preview_{ts}.json"
    md_path = artifact_dir / f"screening_history_archive_preview_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_history_archive_preview_markdown(payload), encoding="utf-8")
    return json_path, md_path
