#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Page-limited hot delete runner for NAV hot/cold archive governance."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_apply_runner import (
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    DEFAULT_OUTPUT_DIR,
    build_nav_history_archive_apply_runner_payload,
)
from utils.nav_history_archive_delete_gate_audit import (
    NAV_HISTORY_ARCHIVE_DELETE_GATE_AUDIT_JSON_PATTERN,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NAV_HISTORY_ARCHIVE_HOT_DELETE_RUNNER_JSON_PATTERN = (
    "nav_history_archive_hot_delete_runner_*.json"
)
ALLOWED_MODES = {"dry_run", "hot_delete"}


def load_latest_nav_history_archive_hot_delete_runner_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_HOT_DELETE_RUNNER_JSON_PATTERN,
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


def _load_json_path(path_text: str | Path, project_root: Path) -> tuple[Path, dict]:
    path = Path(path_text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path, json.loads(path.read_text(encoding="utf-8"))


def _load_latest_delete_gate_payload(
    *,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Path, dict]:
    resolved_output_dir = _resolve_output_dir(output_dir, project_root)
    candidates = sorted(resolved_output_dir.glob(NAV_HISTORY_ARCHIVE_DELETE_GATE_AUDIT_JSON_PATTERN))
    if not candidates:
        raise FileNotFoundError(
            f"No matching artifact: {resolved_output_dir / NAV_HISTORY_ARCHIVE_DELETE_GATE_AUDIT_JSON_PATTERN}"
        )
    path = candidates[-1]
    return path, json.loads(path.read_text(encoding="utf-8"))


def _count_hot_cold_rows(*, db_path: Path, cutoff_date: str) -> int:
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
            (cutoff_date,),
        ).fetchone()
        return _normalize_int(row[0] if row else 0)


def _execute_hot_delete_page(
    *,
    db_path: Path,
    cutoff_date: str,
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
    conn = sqlite3.connect(str(db_path), timeout=60, isolation_level=None)
    try:
        before = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
                (cutoff_date,),
            ).fetchone()[0]
        )
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            DELETE FROM fund_nav_history
            WHERE rowid IN (
                SELECT rowid
                FROM fund_nav_history
                WHERE nav_date < ?
                ORDER BY nav_date, fcode
                LIMIT ?
            )
            """,
            (cutoff_date, int(page_limit)),
        )
        deleted_count = _normalize_int(conn.execute("SELECT changes()").fetchone()[0])
        conn.execute("COMMIT")
        after = _normalize_int(
            conn.execute(
                "SELECT COUNT(*) FROM fund_nav_history WHERE nav_date < ?",
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
            "hot_cold_rows_before": before,
            "hot_cold_rows_after": after,
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
        conn.close()


def build_nav_history_archive_hot_delete_runner_payload(
    *,
    db_path: str | Path | None = None,
    project_root: str | Path | None = None,
    delete_gate_payload: Optional[dict] = None,
    source_delete_gate_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    mode: str = "dry_run",
    page_limit: int = DEFAULT_ARCHIVE_PAGE_LIMIT,
) -> dict:
    root = _resolve_project_root(project_root)
    requested_mode = _normalize_text(mode) or "dry_run"
    if delete_gate_payload is None:
        if source_delete_gate_artifact:
            delete_gate_path, delete_gate_payload = _load_json_path(source_delete_gate_artifact, root)
        else:
            delete_gate_path, delete_gate_payload = _load_latest_delete_gate_payload(
                output_dir=output_dir,
                project_root=root,
            )
    else:
        delete_gate_path = (
            _resolve_path(source_delete_gate_artifact, root)
            if source_delete_gate_artifact
            else None
        )
    delete_gate_payload = dict(delete_gate_payload or {})
    gate_summary = dict(delete_gate_payload.get("summary") or {})
    cutoff_date = _normalize_text(gate_summary.get("archive_cutoff_date"))
    active_db_path = _resolve_path(
        db_path or gate_summary.get("active_db_absolute_path") or gate_summary.get("active_db_path"),
        root,
    )

    blockers: list[str] = []
    review_items: list[str] = []
    if requested_mode not in ALLOWED_MODES:
        blockers.append(f"unsupported mode: {requested_mode}")
    if requested_mode == "hot_delete" and page_limit <= 0:
        blockers.append("hot_delete mode requires positive page_limit.")
    if gate_summary.get("delete_gate_status") != "ready_for_hot_delete_review":
        blockers.append(
            "delete gate audit must be ready_for_hot_delete_review before hot delete."
        )
    if not bool(gate_summary.get("safe_for_hot_delete_review")):
        blockers.append("delete gate audit safe_for_hot_delete_review is not true.")
    if bool(gate_summary.get("hot_delete_sql_executed")) or bool(
        gate_summary.get("vacuum_executed")
    ):
        blockers.append("delete gate audit must be read-only evidence.")
    if not cutoff_date:
        blockers.append("archive_cutoff_date is missing.")
    if active_db_path is None or not active_db_path.exists():
        blockers.append(f"active database missing: {active_db_path or ''}")

    hot_delete_result = {
        "status": "not_requested" if requested_mode != "hot_delete" else "not_executed",
        "hot_delete_sql_executed": False,
        "delete_sql_executed": False,
        "vacuum_executed": False,
    }
    live_cold_rows_before = 0
    live_cold_rows_after = 0
    if active_db_path is not None and active_db_path.exists() and cutoff_date:
        live_cold_rows_before = _count_hot_cold_rows(
            db_path=active_db_path,
            cutoff_date=cutoff_date,
        )
        live_cold_rows_after = live_cold_rows_before

    if requested_mode == "hot_delete" and not blockers and active_db_path is not None:
        hot_delete_result = _execute_hot_delete_page(
            db_path=active_db_path,
            cutoff_date=cutoff_date,
            page_limit=page_limit,
        )
        if hot_delete_result.get("status") == "failed":
            blockers.append(
                "hot delete execution failed: "
                + _normalize_text(hot_delete_result.get("error") or "unknown")
            )
        live_cold_rows_after = _normalize_int(
            hot_delete_result.get("hot_cold_rows_after"),
            live_cold_rows_before,
        )

    apply_runner_payload = None
    apply_runner_summary: dict[str, Any] = {}
    if active_db_path is not None and cutoff_date:
        try:
            apply_runner_payload = build_nav_history_archive_apply_runner_payload(
                db_path=active_db_path,
                project_root=root,
                output_dir=output_dir,
                mode="dry_run",
            )
            apply_runner_summary = dict(apply_runner_payload.get("summary") or {})
        except Exception as exc:
            review_items.append(f"post-delete apply runner refresh failed: {type(exc).__name__}: {exc}")

    if blockers:
        runner_status = "blocked"
        auto_governance_stage = "hot_delete_blocked"
    elif live_cold_rows_after <= 0 and bool(apply_runner_summary.get("archive_binding_active")):
        runner_status = "hot_delete_complete_binding_ready"
        auto_governance_stage = "hot_delete_complete_binding_ready"
    elif requested_mode == "hot_delete" and bool(hot_delete_result.get("hot_delete_sql_executed")):
        runner_status = "hot_delete_in_progress"
        auto_governance_stage = "hot_delete_in_progress"
    else:
        runner_status = "ready_for_hot_delete"
        auto_governance_stage = "ready_for_hot_delete"

    summary = {
        "runner_status": runner_status,
        "auto_governance_stage": auto_governance_stage,
        "mode": requested_mode,
        "hot_delete_supported": True,
        "vacuum_supported": False,
        "source_delete_gate_artifact": _stringify_project_path(delete_gate_path, root),
        "delete_gate_status": gate_summary.get("delete_gate_status") or "",
        "archive_cutoff_date": cutoff_date,
        "active_db_path": _stringify_project_path(active_db_path, root),
        "active_db_absolute_path": str(active_db_path or ""),
        "page_limit": int(page_limit),
        "live_cold_rows_before": live_cold_rows_before,
        "live_cold_rows_after": live_cold_rows_after,
        "hot_delete_result_status": hot_delete_result.get("status") or "",
        "hot_delete_deleted_row_count": _normalize_int(hot_delete_result.get("deleted_row_count")),
        "hot_delete_sql_executed": bool(hot_delete_result.get("hot_delete_sql_executed")),
        "delete_sql_executed": bool(hot_delete_result.get("delete_sql_executed")),
        "vacuum_executed": False,
        "post_apply_runner_status": apply_runner_summary.get("runner_status") or "",
        "archive_binding_active": bool(apply_runner_summary.get("archive_binding_active")),
        "archive_materialized_and_reconciled": bool(
            apply_runner_summary.get("archive_materialized_and_reconciled")
        ),
        "post_apply_runner_live_cold_row_count": _normalize_int(
            apply_runner_summary.get("live_cold_row_count")
        ),
        "post_apply_runner_existing_archive_row_count": _normalize_int(
            apply_runner_summary.get("existing_archive_row_count")
        ),
        "post_apply_runner_archive_missing_row_count": _normalize_int(
            apply_runner_summary.get("archive_missing_row_count")
        ),
        "freelist_count": _normalize_int(
            hot_delete_result.get("freelist_count"),
            _normalize_int(apply_runner_summary.get("freelist_count")),
        ),
        "blocker_count": len(blockers),
        "review_item_count": len(review_items),
    }
    findings = []
    if blockers:
        findings.append(f"NAV hot delete runner blocked with {len(blockers)} blockers.")
        findings.extend(blockers[:5])
    elif requested_mode == "hot_delete":
        findings.append(
            f"Executed page-limited hot delete for {summary['hot_delete_deleted_row_count']} rows; no VACUUM was executed."
        )
    else:
        findings.append("NAV hot delete runner dry-run is ready; no hot delete or VACUUM was executed.")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "available": True,
        "summary": summary,
        "blockers": blockers,
        "review_items": review_items,
        "hot_delete_result": hot_delete_result,
        "post_apply_runner_summary": apply_runner_summary,
        "post_apply_runner_payload": apply_runner_payload or {},
        "findings": findings,
        "safety": {
            "hot_delete_sql_executed": bool(hot_delete_result.get("hot_delete_sql_executed")),
            "delete_sql_executed": bool(hot_delete_result.get("delete_sql_executed")),
            "vacuum_executed": False,
        },
    }


def render_nav_history_archive_hot_delete_runner_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive hot delete runner",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- runner_status: {summary.get('runner_status', '')}",
        f"- auto_governance_stage: {summary.get('auto_governance_stage', '')}",
        f"- mode: {summary.get('mode', '')}",
        f"- live_cold_rows_before: {summary.get('live_cold_rows_before', 0)}",
        f"- live_cold_rows_after: {summary.get('live_cold_rows_after', 0)}",
        f"- hot_delete_deleted_row_count: {summary.get('hot_delete_deleted_row_count', 0)}",
        f"- hot_delete_sql_executed: {summary.get('hot_delete_sql_executed', False)}",
        f"- vacuum_executed: {summary.get('vacuum_executed', False)}",
        f"- archive_binding_active: {summary.get('archive_binding_active', False)}",
        f"- blocker_count: {summary.get('blocker_count', 0)}",
        "",
        "## blockers",
    ]
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_hot_delete_runner_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"nav_history_archive_hot_delete_runner_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_hot_delete_runner_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_hot_delete_runner_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
