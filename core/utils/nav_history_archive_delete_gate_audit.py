#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hot-delete gate audit for NAV hot/cold archive governance."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import quote

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_apply_runner import (
    DEFAULT_OUTPUT_DIR,
    NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NAV_HISTORY_ARCHIVE_DELETE_GATE_AUDIT_JSON_PATTERN = (
    "nav_history_archive_delete_gate_audit_*.json"
)
REQUIRED_NAV_COLUMNS = ("fcode", "nav_date", "unit_nav", "accum_nav", "daily_growth")


def load_latest_nav_history_archive_delete_gate_audit_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_DELETE_GATE_AUDIT_JSON_PATTERN,
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
    conn = sqlite3.connect(_sqlite_readonly_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_table_name(value: Any) -> str:
    text = _normalize_text(value) or "fund_nav_history_archive"
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in safe_chars for ch in text):
        return "fund_nav_history_archive"
    return text


def _load_json_path(path_text: str | Path, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path, json.loads(path.read_text(encoding="utf-8"))


def _load_latest_apply_runner_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Path, dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    candidates = sorted(resolved_output_dir.glob(NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN))
    if not candidates:
        raise FileNotFoundError(
            f"No matching artifact: {resolved_output_dir / NAV_HISTORY_ARCHIVE_APPLY_RUNNER_JSON_PATTERN}"
        )
    path = candidates[-1]
    return path, json.loads(path.read_text(encoding="utf-8"))


def _fetch_one_dict(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict:
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return {}
    return dict(row) if isinstance(row, sqlite3.Row) else {}


def _fetch_all_dicts(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cursor = conn.execute(sql, params)
    rows = cursor.fetchall()
    result = []
    for row in rows:
        result.append(dict(row) if isinstance(row, sqlite3.Row) else {})
    return result


def _format_digest_value(value: Any) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, float):
        return format(value, ".17g")
    return str(value)


def _digest_nav_rows(
    *,
    conn: sqlite3.Connection,
    table_name: str,
    cutoff_date: str,
) -> dict:
    digest = hashlib.sha256()
    count = 0
    cursor = conn.execute(
        f"""
        SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth
        FROM {table_name}
        WHERE nav_date < ?
        ORDER BY fcode, nav_date
        """,
        (cutoff_date,),
    )
    for row in cursor:
        fields = [_format_digest_value(row[column]) for column in REQUIRED_NAV_COLUMNS]
        digest.update(("\x1f".join(fields) + "\n").encode("utf-8"))
        count += 1
    return {"row_count": count, "sha256": digest.hexdigest()}


def _snapshot_nav_scope(
    *,
    conn: sqlite3.Connection,
    table_name: str,
    cutoff_date: str,
) -> dict:
    return _fetch_one_dict(
        conn,
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT fcode) AS fund_count,
            MIN(nav_date) AS min_nav_date,
            MAX(nav_date) AS max_nav_date
        FROM {table_name}
        WHERE nav_date < ?
        """,
        (cutoff_date,),
    )


def _sample_nav_rows(
    *,
    conn: sqlite3.Connection,
    table_name: str,
    cutoff_date: str,
    sample_limit: int,
    descending: bool = False,
) -> list[dict]:
    direction = "DESC" if descending else "ASC"
    return _fetch_all_dicts(
        conn,
        f"""
        SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth
        FROM {table_name}
        WHERE nav_date < ?
        ORDER BY fcode {direction}, nav_date {direction}
        LIMIT ?
        """,
        (cutoff_date, max(1, int(sample_limit))),
    )


def build_nav_history_archive_delete_gate_audit_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    apply_runner_payload: Optional[dict] = None,
    source_apply_runner_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    sample_limit: int = 20,
    compute_full_digest: bool = True,
) -> dict:
    """Build a read-only evidence payload for the hot-delete gate."""
    root = _resolve_project_root(project_root)
    if apply_runner_payload is None:
        if source_apply_runner_artifact:
            apply_runner_path, apply_runner_payload = _load_json_path(
                source_apply_runner_artifact,
                root,
            )
        else:
            apply_runner_path, apply_runner_payload = _load_latest_apply_runner_payload(
                output_dir=output_dir,
                project_root=root,
            )
    else:
        apply_runner_path = (
            _resolve_path(source_apply_runner_artifact, root)
            if source_apply_runner_artifact
            else None
        )

    apply_runner_payload = dict(apply_runner_payload or {})
    apply_summary = dict(apply_runner_payload.get("summary") or {})
    cutoff_date = _normalize_text(apply_summary.get("archive_cutoff_date"))
    archive_table_name = _normalize_table_name(apply_summary.get("archive_table_name"))
    active_db_path = _resolve_path(
        db_path or apply_summary.get("active_db_path"),
        root,
    )
    archive_target_path = _resolve_path(
        apply_summary.get("archive_target_absolute_path")
        or apply_summary.get("archive_target_path"),
        root,
    )
    backup_artifact_path = _resolve_path(
        apply_summary.get("backup_artifact_absolute_path")
        or apply_summary.get("backup_artifact_path"),
        root,
    )
    expected_archive_row_count = _normalize_int(
        apply_summary.get("expected_archive_row_count")
    )
    expected_archive_fund_count = _normalize_int(
        apply_summary.get("expected_archive_fund_count")
    )
    runner_status = _normalize_text(apply_summary.get("runner_status"))
    blockers: list[str] = []
    review_items: list[str] = []

    if runner_status != "dry_run_ready_for_delete_gate_review":
        blockers.append(
            f"apply runner status must be dry_run_ready_for_delete_gate_review, got {runner_status or 'missing'}."
        )
    if not bool(apply_summary.get("archive_materialized_and_reconciled")):
        blockers.append("apply runner does not prove archive_materialized_and_reconciled=true.")
    if bool(apply_summary.get("archive_binding_active")):
        blockers.append("archive_binding_active is already true; this pre-delete gate is no longer applicable.")
    if bool(apply_summary.get("delete_sql_executed")):
        blockers.append("apply runner indicates delete_sql_executed=true.")
    if bool(apply_summary.get("vacuum_executed")):
        blockers.append("apply runner indicates vacuum_executed=true.")
    if expected_archive_row_count <= 0:
        blockers.append("expected_archive_row_count is missing or non-positive.")
    if not cutoff_date:
        blockers.append("archive_cutoff_date is missing.")
    if active_db_path is None or not active_db_path.exists():
        blockers.append(f"active database missing: {active_db_path or ''}")
    if archive_target_path is None or not archive_target_path.exists():
        blockers.append(f"archive target missing: {archive_target_path or ''}")
    if backup_artifact_path is None or not backup_artifact_path.exists():
        blockers.append(f"backup artifact missing: {backup_artifact_path or ''}")

    hot_snapshot: dict = {}
    archive_snapshot: dict = {}
    digest_summary: dict = {
        "computed": False,
        "hot_sha256": "",
        "archive_sha256": "",
        "match": False,
        "hot_digest_row_count": 0,
        "archive_digest_row_count": 0,
    }
    sample_summary: dict = {
        "sample_limit": max(1, int(sample_limit)),
        "head_match": False,
        "tail_match": False,
        "head_hot": [],
        "head_archive": [],
        "tail_hot": [],
        "tail_archive": [],
    }
    if not blockers:
        try:
            with _open_readonly_db(active_db_path) as hot_conn, _open_readonly_db(archive_target_path) as archive_conn:  # type: ignore[arg-type]
                hot_snapshot = _snapshot_nav_scope(
                    conn=hot_conn,
                    table_name="fund_nav_history",
                    cutoff_date=cutoff_date,
                )
                archive_snapshot = _snapshot_nav_scope(
                    conn=archive_conn,
                    table_name=archive_table_name,
                    cutoff_date=cutoff_date,
                )
                archive_out_of_scope = _fetch_one_dict(
                    archive_conn,
                    f"SELECT COUNT(*) AS out_of_scope_row_count FROM {archive_table_name} WHERE nav_date >= ?",
                    (cutoff_date,),
                )
                archive_snapshot.update(archive_out_of_scope)

                hot_head = _sample_nav_rows(
                    conn=hot_conn,
                    table_name="fund_nav_history",
                    cutoff_date=cutoff_date,
                    sample_limit=sample_limit,
                )
                archive_head = _sample_nav_rows(
                    conn=archive_conn,
                    table_name=archive_table_name,
                    cutoff_date=cutoff_date,
                    sample_limit=sample_limit,
                )
                hot_tail = _sample_nav_rows(
                    conn=hot_conn,
                    table_name="fund_nav_history",
                    cutoff_date=cutoff_date,
                    sample_limit=sample_limit,
                    descending=True,
                )
                archive_tail = _sample_nav_rows(
                    conn=archive_conn,
                    table_name=archive_table_name,
                    cutoff_date=cutoff_date,
                    sample_limit=sample_limit,
                    descending=True,
                )
                sample_summary.update(
                    {
                        "head_match": hot_head == archive_head,
                        "tail_match": hot_tail == archive_tail,
                        "head_hot": hot_head,
                        "head_archive": archive_head,
                        "tail_hot": hot_tail,
                        "tail_archive": archive_tail,
                    }
                )
                if compute_full_digest:
                    hot_digest = _digest_nav_rows(
                        conn=hot_conn,
                        table_name="fund_nav_history",
                        cutoff_date=cutoff_date,
                    )
                    archive_digest = _digest_nav_rows(
                        conn=archive_conn,
                        table_name=archive_table_name,
                        cutoff_date=cutoff_date,
                    )
                    digest_summary.update(
                        {
                            "computed": True,
                            "hot_sha256": hot_digest["sha256"],
                            "archive_sha256": archive_digest["sha256"],
                            "match": hot_digest == archive_digest,
                            "hot_digest_row_count": hot_digest["row_count"],
                            "archive_digest_row_count": archive_digest["row_count"],
                        }
                    )
                else:
                    review_items.append("full digest was not computed; hot delete must remain under review.")
        except sqlite3.Error as exc:
            blockers.append(f"sqlite audit failed: {exc}")

    hot_row_count = _normalize_int(hot_snapshot.get("row_count"))
    hot_fund_count = _normalize_int(hot_snapshot.get("fund_count"))
    archive_row_count = _normalize_int(archive_snapshot.get("row_count"))
    archive_fund_count = _normalize_int(archive_snapshot.get("fund_count"))
    archive_out_of_scope_count = _normalize_int(archive_snapshot.get("out_of_scope_row_count"))
    if not blockers:
        if hot_row_count != expected_archive_row_count:
            blockers.append(
                f"hot cold row count {hot_row_count} != expected {expected_archive_row_count}."
            )
        if archive_row_count != expected_archive_row_count:
            blockers.append(
                f"archive row count {archive_row_count} != expected {expected_archive_row_count}."
            )
        if expected_archive_fund_count > 0 and hot_fund_count != expected_archive_fund_count:
            blockers.append(
                f"hot cold fund count {hot_fund_count} != expected {expected_archive_fund_count}."
            )
        if expected_archive_fund_count > 0 and archive_fund_count != expected_archive_fund_count:
            blockers.append(
                f"archive fund count {archive_fund_count} != expected {expected_archive_fund_count}."
            )
        if archive_out_of_scope_count != 0:
            blockers.append(f"archive contains {archive_out_of_scope_count} out-of-scope rows.")
        if hot_snapshot.get("min_nav_date") != archive_snapshot.get("min_nav_date"):
            blockers.append("hot/archive min_nav_date mismatch.")
        if hot_snapshot.get("max_nav_date") != archive_snapshot.get("max_nav_date"):
            blockers.append("hot/archive max_nav_date mismatch.")
        if not sample_summary.get("head_match") or not sample_summary.get("tail_match"):
            blockers.append("hot/archive sample rows do not match.")
        if compute_full_digest and not digest_summary.get("match"):
            blockers.append("hot/archive full digest mismatch.")

    gate_status = "ready_for_hot_delete_review"
    if blockers:
        gate_status = "blocked"
    elif review_items:
        gate_status = "review_required"

    summary = {
        "delete_gate_status": gate_status,
        "safe_for_hot_delete_review": gate_status == "ready_for_hot_delete_review",
        "hot_delete_sql_executed": False,
        "vacuum_executed": False,
        "source_apply_runner_artifact": _stringify_project_path(apply_runner_path, root),
        "source_apply_runner_status": runner_status,
        "archive_cutoff_date": cutoff_date,
        "expected_archive_row_count": expected_archive_row_count,
        "expected_archive_fund_count": expected_archive_fund_count,
        "hot_cold_row_count": hot_row_count,
        "hot_cold_fund_count": hot_fund_count,
        "archive_row_count": archive_row_count,
        "archive_fund_count": archive_fund_count,
        "archive_out_of_scope_row_count": archive_out_of_scope_count,
        "hot_min_nav_date": hot_snapshot.get("min_nav_date") or "",
        "hot_max_nav_date": hot_snapshot.get("max_nav_date") or "",
        "archive_min_nav_date": archive_snapshot.get("min_nav_date") or "",
        "archive_max_nav_date": archive_snapshot.get("max_nav_date") or "",
        "sample_head_match": bool(sample_summary.get("head_match")),
        "sample_tail_match": bool(sample_summary.get("tail_match")),
        "full_digest_computed": bool(digest_summary.get("computed")),
        "full_digest_match": bool(digest_summary.get("match")),
        "hot_digest_sha256": digest_summary.get("hot_sha256") or "",
        "archive_digest_sha256": digest_summary.get("archive_sha256") or "",
        "active_db_path": _stringify_project_path(active_db_path, root),
        "active_db_absolute_path": str(active_db_path or ""),
        "archive_target_path": _stringify_project_path(archive_target_path, root),
        "archive_target_absolute_path": str(archive_target_path or ""),
        "backup_artifact_path": _stringify_project_path(backup_artifact_path, root),
        "backup_artifact_absolute_path": str(backup_artifact_path or ""),
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
    }
    findings = []
    if blockers:
        findings.append(f"NAV hot delete gate blocked with {len(blockers)} blockers.")
        findings.extend(blockers[:5])
    elif review_items:
        findings.append(f"NAV hot delete gate has {len(review_items)} review items.")
        findings.extend(review_items[:5])
    else:
        findings.append(
            "NAV archive is fully materialized and reconciled against hot cold rows; hot delete may enter the next gated review step."
        )
        findings.append("This audit did not execute hot delete or VACUUM.")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "summary": summary,
        "blockers": blockers,
        "review_items": review_items,
        "hot_snapshot": hot_snapshot,
        "archive_snapshot": archive_snapshot,
        "digest_summary": digest_summary,
        "sample_summary": sample_summary,
        "findings": findings,
        "safety": {
            "hot_delete_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }


def render_nav_history_archive_delete_gate_audit_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive delete gate audit",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- delete_gate_status: {summary.get('delete_gate_status', '')}",
        f"- safe_for_hot_delete_review: {summary.get('safe_for_hot_delete_review', False)}",
        f"- source_apply_runner_status: {summary.get('source_apply_runner_status', '')}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- expected_archive_row_count: {summary.get('expected_archive_row_count', 0)}",
        f"- hot_cold_row_count: {summary.get('hot_cold_row_count', 0)}",
        f"- archive_row_count: {summary.get('archive_row_count', 0)}",
        f"- archive_out_of_scope_row_count: {summary.get('archive_out_of_scope_row_count', 0)}",
        f"- sample_head_match: {summary.get('sample_head_match', False)}",
        f"- sample_tail_match: {summary.get('sample_tail_match', False)}",
        f"- full_digest_match: {summary.get('full_digest_match', False)}",
        f"- hot_delete_sql_executed: {summary.get('hot_delete_sql_executed', False)}",
        f"- vacuum_executed: {summary.get('vacuum_executed', False)}",
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


def write_nav_history_archive_delete_gate_audit_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"nav_history_archive_delete_gate_audit_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_delete_gate_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_delete_gate_audit_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
