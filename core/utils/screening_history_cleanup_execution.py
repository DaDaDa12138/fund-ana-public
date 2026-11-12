#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选历史清理执行器。

默认 dry-run；只有显式 apply 且满足前置核对、确认串、备份文件存在时才会执行。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple
from urllib.parse import quote

from sqlalchemy import func, inspect

from database.connection import get_db_manager
from database.models import (
    ScreeningDecision,
    ScreeningDecisionReasonSummary,
    ScreeningPick,
    ScreeningPickMetricSnapshot,
    ScreeningRun,
    ScreeningRunLatestActivity,
    ScreeningRunStorageStats,
)
from database.screening_decision_summaries import delete_screening_decision_summaries_for_run_ids
from database.screening_pick_metric_snapshots import delete_pick_metric_snapshots_for_run_ids
from database.screening_run_storage_stats import mark_storage_stats_after_detail_cleanup
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_history_cleanup_preflight import (
    load_latest_screening_history_cleanup_preflight_payload,
)
from utils.screening_history_cleanup_runbook import (
    load_latest_screening_history_cleanup_runbook_payload,
)
from utils.screening_history_cleanup_backup import (
    DEFAULT_BACKUP_PREFIX as DEFAULT_CLEANUP_BACKUP_PREFIX,
    build_screening_history_cleanup_backup_readiness_payload,
)
from utils.screening_history_archive_preview import ARCHIVE_TABLES
from utils.screening_history_archive_manifest import (
    load_screening_archive_manifest,
    queryable_archive_shards,
    resolve_manifest_path,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROJECT_ROOT = PROJECT_ROOT.parent
SCREENING_HISTORY_CLEANUP_EXECUTION_JSON_PATTERN = "screening_history_cleanup_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_SCREENING_HISTORY_CLEANUP"
ARCHIVE_GUARD_STATUS_OK = "ok"
HOT_DB_QUICK_CHECK_SKIP_SIZE_BYTES = 1024 * 1024 * 1024


def load_latest_screening_history_cleanup_execution_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选历史 cleanup 执行 checkpoint。"""
    return load_latest_json_artifact(
        SCREENING_HISTORY_CLEANUP_EXECUTION_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_mode(mode: str) -> str:
    value = str(mode or "dry-run").strip().lower()
    if value not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    return value


def _normalize_batch_index(value, default: int) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, parsed)


def _normalize_run_ids(run_ids: Optional[Sequence[str]]) -> list[str]:
    normalized = []
    seen = set()
    for item in run_ids or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _iter_chunks(values: Sequence[str], chunk_size: int = 500):
    size = max(1, int(chunk_size or 500))
    for start in range(0, len(values), size):
        yield list(values[start:start + size])


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _safe_table_name(value: str) -> str:
    text = str(value or "").strip()
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not text or any(ch not in safe_chars for ch in text):
        raise ValueError(f"unsafe table name: {value}")
    return text


def _open_readonly_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def _sqlite_quick_check(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(_open_readonly_sqlite(path)) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0] if row else "").strip()
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _sqlite_readonly_probe(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(_open_readonly_sqlite(path)) as conn:
            row = conn.execute("SELECT 1").fetchone()
        return "ok" if row and int(row[0]) == 1 else "unexpected_result"
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_safe_table_name(table_name),),
        ).fetchone()
        is not None
    )


def _sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    table = _safe_table_name(table_name)
    if not _sqlite_table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[1]).strip() for row in rows}


def _sqlite_count_rows_for_run_ids(
    conn: sqlite3.Connection,
    table_name: str,
    run_ids: Sequence[str],
) -> int:
    normalized_run_ids = _normalize_run_ids(run_ids)
    table = _safe_table_name(table_name)
    if not normalized_run_ids or not _sqlite_table_exists(conn, table):
        return 0
    if "run_id" not in _sqlite_table_columns(conn, table):
        return 0

    total = 0
    qualified = _quote_ident(table)
    for chunk in _iter_chunks(normalized_run_ids, 900):
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"SELECT COUNT(*) FROM {qualified} WHERE run_id IN ({placeholders})",
                tuple(chunk),
            ).fetchone()[0]
            or 0
        )
    return total


def _sqlite_present_run_ids(
    conn: sqlite3.Connection,
    run_ids: Sequence[str],
) -> set[str]:
    normalized_run_ids = _normalize_run_ids(run_ids)
    found: set[str] = set()
    if not normalized_run_ids:
        return found
    for table_name in ARCHIVE_TABLES:
        table = _safe_table_name(table_name)
        if not _sqlite_table_exists(conn, table):
            continue
        if "run_id" not in _sqlite_table_columns(conn, table):
            continue
        qualified = _quote_ident(table)
        for chunk in _iter_chunks(normalized_run_ids, 900):
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT DISTINCT run_id FROM {qualified} WHERE run_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            found.update(
                str(row[0] or "").strip()
                for row in rows
                if row and str(row[0] or "").strip()
            )
    return found


def _resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def _resolve_guard_path(
    value: str | Path | None,
    *,
    project_root: str | Path | None,
    default_relative: str,
) -> Path:
    root = Path(project_root or DEFAULT_PROJECT_ROOT).expanduser().resolve()
    raw = Path(str(value or default_relative)).expanduser()
    if raw.is_absolute():
        return raw.resolve()
    return (root / raw).resolve()


def _safe_json_load(path: Path) -> tuple[dict, str]:
    try:
        if not path.exists() or not path.is_file():
            return {}, f"missing:{path}"
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, f"{type(exc).__name__}:{exc}"
    if not isinstance(payload, dict):
        return {}, "invalid_json_payload"
    return payload, ""


def _add_protected_run(
    protected: dict[str, set[str]],
    run_id: object,
    source: str,
) -> None:
    value = str(run_id or "").strip()
    if not value:
        return
    protected.setdefault(value, set()).add(source)


def _build_publication_apply_guard(
    *,
    selected_run_ids: Sequence[str],
    require_publication_guard: bool,
    project_root: str | Path | None,
    latest_report_file: str | Path | None,
    publication_registry_file: str | Path | None,
) -> dict:
    normalized_run_ids = _normalize_run_ids(selected_run_ids)
    if not require_publication_guard:
        return {
            "status": "disabled",
            "required": False,
            "selected_run_count": len(normalized_run_ids),
            "protected_run_count": 0,
            "intersecting_run_ids": [],
            "blockers": [],
            "protected_sources": [],
        }

    latest_path = _resolve_guard_path(
        latest_report_file,
        project_root=project_root,
        default_relative="reports/latest.json",
    )
    registry_path = _resolve_guard_path(
        publication_registry_file,
        project_root=project_root,
        default_relative="reports/publication_registry.json",
    )
    latest_payload, latest_error = _safe_json_load(latest_path)
    registry_payload, registry_error = _safe_json_load(registry_path)

    protected: dict[str, set[str]] = {}
    blockers: list[dict] = []
    if latest_error:
        blockers.append(
            {
                "code": "latest_report_unavailable",
                "detail": latest_error,
                "path": str(latest_path),
            }
        )
    else:
        _add_protected_run(protected, latest_payload.get("run_id"), "latest_report.run_id")

    if registry_error:
        blockers.append(
            {
                "code": "publication_registry_unavailable",
                "detail": registry_error,
                "path": str(registry_path),
            }
        )
    else:
        _add_protected_run(
            protected,
            registry_payload.get("latest_canonical_run_id"),
            "publication_registry.latest_canonical_run_id",
        )
        _add_protected_run(
            protected,
            registry_payload.get("current_published_run_id"),
            "publication_registry.current_published_run_id",
        )

        layers = registry_payload.get("automation_layers")
        if isinstance(layers, dict):
            current_published = layers.get("current_published_run")
            if isinstance(current_published, dict):
                _add_protected_run(
                    protected,
                    current_published.get("run_id"),
                    "automation_layers.current_published_run.run_id",
                )
            latest_publishable = layers.get("latest_publishable_run")
            if isinstance(latest_publishable, dict):
                _add_protected_run(
                    protected,
                    latest_publishable.get("run_id"),
                    "automation_layers.latest_publishable_run.run_id",
                )
            latest_successful = layers.get("latest_successful_run")
            if isinstance(latest_successful, dict) and (
                bool(latest_successful.get("published"))
                or bool(latest_successful.get("publishable"))
                or bool(layers.get("latest_successful_is_currently_published"))
            ):
                _add_protected_run(
                    protected,
                    latest_successful.get("run_id"),
                    "automation_layers.latest_successful_run.run_id",
                )

        for index, record in enumerate(registry_payload.get("canonical_runs") or []):
            if not isinstance(record, dict):
                continue
            if bool(record.get("published")) or str(record.get("publish_state") or "") == "published_canonical":
                _add_protected_run(
                    protected,
                    record.get("run_id"),
                    f"canonical_runs[{index}].published",
                )

        integrity = registry_payload.get("publication_integrity")
        if not isinstance(integrity, dict):
            blockers.append(
                {
                    "code": "publication_integrity_missing",
                    "detail": "publication_registry lacks publication_integrity",
                    "path": str(registry_path),
                }
            )
        else:
            if not bool(integrity.get("aligned")):
                blockers.append(
                    {
                        "code": "publication_integrity_not_aligned",
                        "detail": "publication registry integrity is not aligned",
                        "blocker_count": int(integrity.get("blocker_count") or 0),
                    }
                )
            if int(integrity.get("blocker_count") or 0) > 0:
                blockers.append(
                    {
                        "code": "publication_integrity_has_blockers",
                        "detail": "publication registry integrity has blockers",
                        "blocker_count": int(integrity.get("blocker_count") or 0),
                    }
                )

    selected_set = set(normalized_run_ids)
    intersecting_run_ids = sorted(selected_set.intersection(protected))
    if intersecting_run_ids:
        blockers.append(
            {
                "code": "selected_runs_include_published_chain_run",
                "detail": "selected cleanup run_ids intersect latest/current publication chain",
                "run_ids": intersecting_run_ids,
            }
        )

    protected_sources = [
        {
            "run_id": run_id,
            "sources": sorted(sources),
        }
        for run_id, sources in sorted(protected.items())
    ]
    return {
        "status": "ok" if not blockers else "blocked",
        "required": True,
        "latest_report_file": str(latest_path),
        "publication_registry_file": str(registry_path),
        "selected_run_count": len(normalized_run_ids),
        "protected_run_count": len(protected),
        "intersecting_run_ids": intersecting_run_ids,
        "blockers": blockers,
        "protected_sources": protected_sources,
    }


def _build_apply_readiness_payload(
    *,
    selected_batch_count: int,
    selected_run_ids: Sequence[str],
    preflight_checks: dict,
    publication_guard: dict,
    archive_guard: dict,
    backup_readiness: Optional[dict] = None,
) -> dict:
    """Summarize whether the current dry-run scope is ready for a future guarded apply."""
    normalized_run_ids = _normalize_run_ids(selected_run_ids)
    publication_status = str(publication_guard.get("status") or "not_recorded").strip()
    archive_status = str(archive_guard.get("status") or "not_recorded").strip()
    backup_readiness_payload = dict(backup_readiness or {})
    backup_status = str(
        backup_readiness_payload.get("status") or "not_checked_in_dry_run"
    ).strip()
    blockers: list[dict] = []

    def add_blocker(code: str, detail: str, **extra) -> None:
        item = {"code": code, "detail": detail}
        item.update(extra)
        blockers.append(item)

    if not normalized_run_ids:
        return {
            "status": "no_scope",
            "selected_batch_count": int(selected_batch_count or 0),
            "selected_run_count": 0,
            "preflight_ready": bool(preflight_checks.get("ready_for_manual_signoff")),
            "publication_guard_status": publication_status,
            "archive_guard_status": archive_status,
            "backup_required_for_apply": False,
            "backup_checked": False,
            "backup_status": "not_required_no_scope",
            "backup_readiness": {},
            "blocker_count": 0,
            "blockers": [],
        }

    if not bool(preflight_checks.get("ready_for_manual_signoff")):
        add_blocker(
            "preflight_not_ready",
            "cleanup apply requires ready_for_manual_signoff=true",
        )

    if publication_status not in {"ok", "disabled"}:
        add_blocker(
            "publication_guard_blocked",
            f"publication_guard.status={publication_status}",
            guard_blockers=publication_guard.get("blockers") or [],
        )

    if archive_status not in {"ok", "disabled", "not_required"}:
        add_blocker(
            "archive_guard_blocked",
            f"archive_guard.status={archive_status}",
            guard_blockers=archive_guard.get("blockers") or [],
        )

    if backup_readiness_payload and backup_status != "ready":
        add_blocker(
            "backup_readiness_blocked",
            f"backup_readiness.status={backup_status}",
            backup_blockers=backup_readiness_payload.get("blockers") or [],
        )

    return {
        "status": "ready" if not blockers else "blocked",
        "selected_batch_count": int(selected_batch_count or 0),
        "selected_run_count": len(normalized_run_ids),
        "preflight_ready": bool(preflight_checks.get("ready_for_manual_signoff")),
        "publication_guard_status": publication_status,
        "archive_guard_status": archive_status,
        "publication_guard_blocker_count": len(publication_guard.get("blockers") or []),
        "archive_guard_blocker_count": int(
            archive_guard.get("blocker_count", len(archive_guard.get("blockers") or []))
            or 0
        ),
        "backup_required_for_apply": True,
        "backup_checked": bool(backup_readiness_payload),
        "backup_status": backup_status,
        "backup_creation_mode": backup_readiness_payload.get("creation_mode") or "",
        "backup_auto_create_if_missing": bool(
            backup_readiness_payload.get("auto_create_backup_if_missing", False)
        ),
        "backup_blocker_count": int(
            backup_readiness_payload.get(
                "blocker_count",
                len(backup_readiness_payload.get("blockers") or []),
            )
            or 0
        ),
        "backup_readiness": backup_readiness_payload,
        "blocker_count": len(blockers),
        "blockers": blockers,
    }


def _table_exists(session, table_name: str) -> bool:
    try:
        return table_name in set(inspect(session.bind).get_table_names())
    except Exception:
        return False


def _count_model_rows_for_run_ids(session, model, run_ids: Sequence[str]) -> int:
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids:
        return 0
    total = 0
    for chunk in _iter_chunks(normalized_run_ids):
        total += int(
            session.query(func.count(model.run_id)).filter(
                model.run_id.in_(chunk)
            ).scalar()
            or 0
        )
    return total


def _build_post_apply_consistency_payload(
    db_manager,
    *,
    selected_run_ids: Sequence[str],
    delete_runs: bool,
) -> dict:
    normalized_run_ids = _normalize_run_ids(selected_run_ids)
    checks: list[dict] = []
    blockers: list[dict] = []

    def add_check(key: str, ok: bool, **extra) -> None:
        payload = {"key": key, "ok": bool(ok)}
        payload.update(extra)
        checks.append(payload)
        if not ok:
            blockers.append(payload)

    with db_manager.get_session(
        task_name="post_screening_history_cleanup_consistency",
        readonly=True,
    ) as session:
        decision_remaining = _count_model_rows_for_run_ids(
            session,
            ScreeningDecision,
            normalized_run_ids,
        )
        pick_remaining = _count_model_rows_for_run_ids(
            session,
            ScreeningPick,
            normalized_run_ids,
        )
        add_check(
            "screening_decisions_removed",
            decision_remaining == 0,
            remaining_count=decision_remaining,
        )
        add_check(
            "screening_picks_removed",
            pick_remaining == 0,
            remaining_count=pick_remaining,
        )

        if delete_runs:
            run_remaining = _count_model_rows_for_run_ids(
                session,
                ScreeningRun,
                normalized_run_ids,
            )
            add_check(
                "screening_runs_removed",
                run_remaining == 0,
                remaining_count=run_remaining,
            )
        else:
            add_check(
                "screening_runs_retained",
                True,
                retained_run_count=_count_model_rows_for_run_ids(
                    session,
                    ScreeningRun,
                    normalized_run_ids,
                ),
            )

        if _table_exists(session, "screening_pick_metric_snapshots"):
            metric_remaining = _count_model_rows_for_run_ids(
                session,
                ScreeningPickMetricSnapshot,
                normalized_run_ids,
            )
            add_check(
                "pick_metric_snapshots_removed",
                metric_remaining == 0,
                remaining_count=metric_remaining,
            )
        else:
            add_check("pick_metric_snapshots_table_missing", True, skipped=True)

        if _table_exists(session, "screening_decision_reason_summary"):
            reason_remaining = _count_model_rows_for_run_ids(
                session,
                ScreeningDecisionReasonSummary,
                normalized_run_ids,
            )
            add_check(
                "decision_reason_summaries_removed",
                reason_remaining == 0,
                remaining_count=reason_remaining,
            )
        else:
            add_check("decision_reason_summary_table_missing", True, skipped=True)

        if _table_exists(session, "screening_run_latest_activity"):
            latest_remaining = _count_model_rows_for_run_ids(
                session,
                ScreeningRunLatestActivity,
                normalized_run_ids,
            )
            add_check(
                "run_latest_activity_removed",
                latest_remaining == 0,
                remaining_count=latest_remaining,
            )
        else:
            add_check("run_latest_activity_table_missing", True, skipped=True)

        if _table_exists(session, "screening_run_storage_stats"):
            storage_rows = []
            for chunk in _iter_chunks(normalized_run_ids):
                storage_rows.extend(
                    session.query(
                        ScreeningRunStorageStats.run_id,
                        ScreeningRunStorageStats.decision_count,
                        ScreeningRunStorageStats.pick_count,
                    )
                    .filter(ScreeningRunStorageStats.run_id.in_(chunk))
                    .all()
                )
            if delete_runs:
                add_check(
                    "run_storage_stats_removed",
                    len(storage_rows) == 0,
                    remaining_count=len(storage_rows),
                )
            else:
                nonzero_rows = [
                    {
                        "run_id": str(row[0] or ""),
                        "decision_count": int(row[1] or 0),
                        "pick_count": int(row[2] or 0),
                    }
                    for row in storage_rows
                    if int(row[1] or 0) != 0 or int(row[2] or 0) != 0
                ]
                add_check(
                    "run_storage_stats_zeroed",
                    not nonzero_rows,
                    checked_count=len(storage_rows),
                    nonzero_rows=nonzero_rows[:20],
                )
        else:
            add_check("run_storage_stats_table_missing", True, skipped=True)

    return {
        "status": "ok" if not blockers else "failed",
        "selected_run_count": len(normalized_run_ids),
        "delete_runs": bool(delete_runs),
        "checks": checks,
        "blocker_count": len(blockers),
        "blockers": blockers,
    }


def _resolve_output_dir(output_dir: str) -> Path:
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _resolve_output_paths(output_dir: str, timestamp: Optional[str] = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    return (
        artifact_dir / f"screening_history_cleanup_execution_{ts}.json",
        artifact_dir / f"screening_history_cleanup_execution_{ts}.md",
    )


def write_screening_history_cleanup_execution_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    """写出筛选历史 cleanup 执行 checkpoint JSON/Markdown。"""
    json_path, md_path = _resolve_output_paths(output_dir, timestamp=timestamp)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_screening_history_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path


def _persist_checkpoint(payload: dict, json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_screening_history_cleanup_execution_markdown(payload),
        encoding="utf-8",
    )


def _load_artifact_payload(
    *,
    payload: Optional[dict],
    source_artifact: Optional[str],
    output_dir: str,
    loader,
) -> tuple[str, dict]:
    if payload is not None:
        return str(source_artifact or "").strip(), dict(payload or {})

    if source_artifact:
        artifact_path = _resolve_project_path(source_artifact)
        loaded_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        return str(artifact_path), dict(loaded_payload or {})

    artifact_path, loaded_payload = loader(output_dir=output_dir)
    return str(artifact_path), dict(loaded_payload or {})


def _load_optional_json_payload(
    *,
    payload: Optional[dict],
    source_artifact: Optional[str],
) -> tuple[str, dict]:
    if payload is not None:
        return str(source_artifact or "").strip(), dict(payload or {})
    if not source_artifact:
        return "", {}
    artifact_path = _resolve_project_path(source_artifact)
    loaded_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    return str(artifact_path), dict(loaded_payload or {})


def _select_batches(
    batches: Sequence[dict],
    start_batch: int,
    end_batch: Optional[int],
) -> tuple[list[dict], int, Optional[int]]:
    rows = [dict(batch) for batch in (batches or [])]
    if not rows:
        start = _normalize_batch_index(start_batch, 1)
        end = _normalize_batch_index(end_batch, 1) if end_batch is not None else None
        if start != 1 or (end is not None and end != 1):
            raise ValueError("runbook has no batches to select")
        return [], 1, None

    total = len(rows)
    start = _normalize_batch_index(start_batch, 1)
    end = _normalize_batch_index(end_batch, total) if end_batch is not None else total
    if start > total:
        raise ValueError(f"start_batch must be between 1 and {total}")
    if end > total:
        raise ValueError(f"end_batch must be between 1 and {total}")
    if end < start:
        raise ValueError("end_batch must be >= start_batch")

    selected = []
    for index, batch in enumerate(rows, start=1):
        order = int(batch.get("execution_order") or index)
        if start <= order <= end:
            selected.append(batch)
    return selected, start, end


def _build_batch_checkpoint(batch: dict, *, status: str) -> dict:
    run_ids = _normalize_run_ids(batch.get("run_ids") or [])
    return {
        "batch_id": batch.get("batch_id"),
        "execution_order": batch.get("execution_order"),
        "batch_run_count": int(batch.get("batch_run_count") or len(run_ids)),
        "run_ids": run_ids,
        "sample_run_ids": list(batch.get("sample_run_ids") or run_ids[:10]),
        "status": status,
        "started_at": None,
        "finished_at": None,
        "deleted_decisions": 0,
        "deleted_picks": 0,
        "deleted_pick_metric_snapshots": 0,
        "deleted_decision_reason_summaries": 0,
        "deleted_run_latest_activities": 0,
        "deleted_runs": 0,
        "deleted_total": 0,
        "error": "",
    }


def _validate_selected_batches(selected_batches: Sequence[dict]) -> None:
    for batch in selected_batches:
        run_ids = _normalize_run_ids(batch.get("run_ids") or [])
        if not run_ids:
            raise ValueError(
                f"batch {batch.get('batch_id') or batch.get('execution_order')} has no run_ids"
            )


def _validate_apply_prerequisites(
    *,
    selected_batches: Sequence[dict],
    preflight_payload: dict,
    runbook_payload: dict,
    source_runbook_artifact: Optional[str],
    confirm_apply: Optional[str],
    backup_path: Optional[Path],
) -> None:
    if not selected_batches:
        raise ValueError("apply requires at least one selected batch")

    preflight_checks = dict(preflight_payload.get("checks") or {})
    if not preflight_checks.get("ready_for_manual_signoff"):
        raise ValueError("preflight is not ready_for_manual_signoff")

    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")

    if backup_path is None or not backup_path.exists():
        raise ValueError("backup_file is required and must exist before apply")

    preflight_runbook_artifact = str(
        preflight_payload.get("source_screening_history_cleanup_runbook_artifact") or ""
    ).strip()
    normalized_source_runbook_artifact = str(source_runbook_artifact or "").strip()
    if (
        normalized_source_runbook_artifact
        and preflight_runbook_artifact
        and normalized_source_runbook_artifact != preflight_runbook_artifact
    ):
        raise ValueError("preflight does not match the selected runbook artifact")

    runbook_preview_artifact = str(
        runbook_payload.get("source_screening_history_cleanup_preview_artifact") or ""
    ).strip()
    preflight_preview_artifact = str(
        preflight_payload.get("source_screening_history_cleanup_preview_artifact") or ""
    ).strip()
    if runbook_preview_artifact and preflight_preview_artifact and (
        runbook_preview_artifact != preflight_preview_artifact
    ):
        raise ValueError("runbook preview artifact does not match preflight preview artifact")


def _build_archive_apply_guard(
    *,
    selected_run_ids: Sequence[str],
    archive_execution_payload: Optional[dict],
    source_archive_execution_artifact: Optional[str],
    require_archive_guard: bool,
    project_root: str | Path | None,
    preflight_payload: dict,
    db_path: str | Path | None,
    archive_target_path: str | Path | None,
) -> dict:
    normalized_run_ids = _normalize_run_ids(selected_run_ids)
    if not require_archive_guard:
        return {
            "status": "disabled",
            "required": False,
            "selected_run_count": len(normalized_run_ids),
            "blocker_count": 0,
            "blockers": [],
            "table_checks": [],
        }

    source_artifact, archive_payload = _load_optional_json_payload(
        payload=archive_execution_payload,
        source_artifact=source_archive_execution_artifact,
    )
    blockers: list[dict] = []
    table_checks: list[dict] = []

    if not archive_payload:
        blockers.append(
            {
                "code": "archive_execution_evidence_missing",
                "detail": "screening history cleanup apply requires a matching archive execution artifact",
            }
        )
        return {
            "status": "blocked",
            "required": True,
            "selected_run_count": len(normalized_run_ids),
            "source_archive_execution_artifact": source_artifact,
            "blocker_count": len(blockers),
            "blockers": blockers,
            "table_checks": table_checks,
        }

    archive_summary = dict(archive_payload.get("summary") or {})
    archive_status = str(archive_payload.get("status") or "").strip()
    archive_mode = str(archive_payload.get("mode") or archive_summary.get("mode") or "").strip()
    if archive_status != "archive-exported":
        blockers.append(
            {
                "code": "archive_execution_not_exported",
                "detail": f"archive execution status={archive_status}",
            }
        )
    if archive_mode != "archive-export":
        blockers.append(
            {
                "code": "archive_execution_mode_not_archive_export",
                "detail": f"archive execution mode={archive_mode}",
            }
        )

    hot_db_raw = (
        db_path
        or archive_payload.get("db_path")
        or preflight_payload.get("db_path")
        or "backend/data/bond_funds.db"
    )
    archive_db_raw = (
        archive_target_path
        or archive_payload.get("archive_target_path")
        or archive_summary.get("archive_target_path")
        or "backend/data/archives/screening_history_details_archive.db"
    )
    hot_db_path = _resolve_guard_path(
        hot_db_raw,
        project_root=project_root,
        default_relative="backend/data/bond_funds.db",
    )
    archive_db_path = _resolve_guard_path(
        archive_db_raw,
        project_root=project_root,
        default_relative="backend/data/archives/screening_history_details_archive.db",
    )

    archive_manifest_raw = (
        archive_payload.get("archive_manifest_path")
        or archive_summary.get("archive_manifest_path")
        or ((archive_payload.get("archive_manifest") or {}).get("manifest_path")
            if isinstance(archive_payload.get("archive_manifest"), dict)
            else None)
    )
    from utils.screening_archive_access import recovery_file_identities

    resolved_manifest_path = resolve_manifest_path(
        archive_db_path, manifest_path=archive_manifest_raw, project_root=project_root,
    )
    manifest_identity = []
    try:
        if resolved_manifest_path.exists():
            manifest_identity = recovery_file_identities([resolved_manifest_path])
    except OSError as exc:
        blockers.append({"code": "archive_manifest_identity_unavailable", "detail": str(exc)})
    archive_manifest_state = load_screening_archive_manifest(
        archive_db_path,
        manifest_path=archive_manifest_raw,
        project_root=project_root,
    )
    archive_manifest_status = str(archive_manifest_state.get("status") or "").strip()
    if bool(archive_manifest_state.get("manifest_exists")) != bool(manifest_identity):
        blockers.append({"code": "archive_manifest_changed_during_verification",
                         "detail": "archive manifest presence changed while loading recovery scope"})
    archive_manifest_generation = int(archive_manifest_state.get("generation") or 0)
    artifact_manifest_generation = int(archive_summary.get("archive_manifest_generation") or 0)
    if archive_manifest_status == "invalid":
        blockers.append(
            {
                "code": "archive_manifest_invalid",
                "detail": "; ".join(
                    archive_manifest_state.get("errors") or ["unknown_error"]
                ),
                "path": archive_manifest_state.get("manifest_path") or "",
            }
        )
    if archive_manifest_state.get("manifest_exists") and (
        artifact_manifest_generation <= 0
        or artifact_manifest_generation != archive_manifest_generation
    ):
        blockers.append(
            {
                "code": "archive_manifest_generation_mismatch",
                "detail": (
                    f"archive execution generation={artifact_manifest_generation}, "
                    f"current generation={archive_manifest_generation}"
                ),
            }
        )

    hot_exists = hot_db_path.exists() and hot_db_path.is_file()
    archive_exists = archive_db_path.exists() and archive_db_path.is_file()
    hot_db_size_bytes = hot_db_path.stat().st_size if hot_exists else 0
    hot_quick_check_required = (
        hot_exists and hot_db_size_bytes <= HOT_DB_QUICK_CHECK_SKIP_SIZE_BYTES
    )
    hot_readonly_probe = _sqlite_readonly_probe(hot_db_path) if hot_exists else ""
    hot_quick_check = (
        _sqlite_quick_check(hot_db_path)
        if hot_quick_check_required
        else ("skipped_large_hot_db" if hot_exists else "")
    )
    archive_quick_check = ""
    archive_quick_check_source = ""
    archive_shard_checks: list[dict] = []
    archive_shards = queryable_archive_shards(archive_manifest_state)
    parquet_probes: dict[str, dict] = {}
    artifact_manifest = (
        dict(archive_payload.get("archive_manifest") or {})
        if isinstance(archive_payload.get("archive_manifest"), dict)
        else {}
    )
    artifact_shards_by_path = {
        str(Path(str(item.get("absolute_path") or "")).expanduser().resolve()): item
        for item in artifact_manifest.get("shards") or []
        if isinstance(item, dict) and str(item.get("absolute_path") or "").strip()
    }
    execution_shard_checks_by_path = {
        str(Path(str(item.get("path") or "")).expanduser().resolve()): item
        for item in archive_payload.get("archive_shard_checks") or []
        if isinstance(item, dict) and str(item.get("path") or "").strip()
    }
    manifest_identity_binding_ready = bool(
        archive_manifest_state.get("manifest_exists")
        and artifact_manifest_generation > 0
        and artifact_manifest_generation == archive_manifest_generation
    )
    for shard in archive_shards:
        shard_path = Path(str(shard.get("absolute_path") or "")).expanduser().resolve()
        shard_exists = shard_path.exists() and shard_path.is_file()
        shard_stat = shard_path.stat() if shard_exists else None
        shard_size_bytes = int(shard_stat.st_size) if shard_stat else 0
        shard_mtime_ns = int(shard_stat.st_mtime_ns) if shard_stat else 0
        artifact_shard = artifact_shards_by_path.get(str(shard_path)) or {}
        if shard.get("format") == "parquet":
            try:
                from utils.screening_archive_access import probe_parquet_shard

                if (not manifest_identity_binding_ready
                        or artifact_shard.get("format") != "parquet"
                        or any(artifact_shard.get(key) != shard.get(key)
                               for key in ("manifest_sha256", "source_sha256"))):
                    raise ValueError("parquet_archive_execution_binding_mismatch")
                probe = probe_parquet_shard(shard, normalized_run_ids)
                parquet_probes[str(shard_path)] = probe
                archive_shard_checks.append(probe["check"])
            except (ValueError, OSError, KeyError, TypeError) as exc:
                archive_shard_checks.append({"id": shard.get("id"), "role": "immutable", "format": "parquet",
                    "path": str(shard_path), "exists": shard_exists, "ok": False, "error": str(exc)})
                blockers.append({"code": "parquet_archive_recovery_verification_failed", "detail": str(exc),
                                 "shard_id": shard.get("id")})
            continue
        identity_match = bool(
            manifest_identity_binding_ready
            and artifact_shard
            and int(artifact_shard.get("file_size_bytes") or 0) == shard_size_bytes
            and int(artifact_shard.get("file_mtime_ns") or 0) == shard_mtime_ns
        )
        execution_shard_check = execution_shard_checks_by_path.get(str(shard_path)) or {}
        if shard.get("role") == "active":
            prior_quick_check_ok = (
                str(archive_summary.get("archive_db_quick_check") or "").strip() == "ok"
            )
        else:
            prior_quick_check_ok = bool(execution_shard_check.get("ok"))
        readonly_probe = _sqlite_readonly_probe(shard_path) if shard_exists else ""
        identity_trusted = bool(
            shard_exists
            and readonly_probe == "ok"
            and identity_match
            and prior_quick_check_ok
        )
        shard_quick_check = (
            "ok"
            if identity_trusted
            else (_sqlite_quick_check(shard_path) if shard_exists else "")
        )
        quick_check_source = (
            "archive_execution_identity"
            if identity_trusted
            else ("live_full_quick_check" if shard_exists else "")
        )
        shard_check = {
            "id": shard.get("id") or "",
            "role": shard.get("role") or "",
            "format": "sqlite",
            "path": str(shard_path),
            "exists": shard_exists,
            "file_size_bytes": shard_size_bytes,
            "file_mtime_ns": shard_mtime_ns,
            "artifact_identity_match": identity_match,
            "readonly_probe": readonly_probe,
            "quick_check": shard_quick_check,
            "quick_check_source": quick_check_source,
            "ok": shard_exists and shard_quick_check == "ok",
        }
        archive_shard_checks.append(shard_check)
        if shard.get("role") == "active":
            archive_quick_check = shard_quick_check
            archive_quick_check_source = quick_check_source
        if not shard_exists:
            blockers.append(
                {
                    "code": "archive_shard_missing",
                    "detail": f"archive shard missing: {shard_path}",
                    "shard_id": shard.get("id") or "",
                }
            )
        elif shard_quick_check != "ok":
            blockers.append(
                {
                    "code": "archive_shard_quick_check_failed",
                    "detail": f"archive shard quick_check={shard_quick_check}: {shard_path}",
                    "shard_id": shard.get("id") or "",
                }
            )

    if not hot_exists:
        blockers.append(
            {"code": "hot_db_missing", "detail": f"hot db missing: {hot_db_path}"}
        )
    elif hot_readonly_probe != "ok":
        blockers.append(
            {
                "code": "hot_db_readonly_probe_failed",
                "detail": f"hot_db_readonly_probe={hot_readonly_probe}",
            }
        )
    elif hot_quick_check_required and hot_quick_check != "ok":
        blockers.append(
            {
                "code": "hot_db_quick_check_failed",
                "detail": f"hot_db_quick_check={hot_quick_check}",
            }
        )
    if not archive_shards:
        blockers.append(
            {
                "code": "archive_shards_missing",
                "detail": "archive manifest/legacy binding has no queryable archive shard",
            }
        )

    hot_guard_open = (
        hot_exists
        and hot_readonly_probe == "ok"
        and (not hot_quick_check_required or hot_quick_check == "ok")
    )
    shards_ready = bool(archive_shard_checks) and all(
        bool(check.get("ok")) for check in archive_shard_checks
    )
    if hot_guard_open and shards_ready and archive_manifest_status != "invalid":
        from database.screening_decision_payloads import begin_read
        from utils.screening_archive_access import (
            recovery_file_identities, verify_parquet_hot_values, verify_sqlite_hot_values,
        )

        archive_counts_by_table = {table_name: 0 for table_name in ARCHIVE_TABLES}
        verified_counts_by_table = {table_name: 0 for table_name in ARCHIVE_TABLES}
        shard_counts_by_table: dict[str, list[dict]] = {
            table_name: [] for table_name in ARCHIVE_TABLES
        }
        run_shard_presence: dict[str, list[str]] = {}
        shards_by_path = {str(Path(shard["absolute_path"]).resolve()): shard for shard in archive_shards}
        values_verified = True
        with closing(_open_readonly_sqlite(hot_db_path)) as hot_conn:
            # All shard proofs and final hot counts use one consistent hot snapshot.
            begin_read(hot_conn)
            for shard_check in archive_shard_checks:
                shard_path = Path(shard_check["path"]).resolve()
                shard_format = shard_check["format"]
                try:
                    if shard_format == "parquet":
                        probe = parquet_probes[str(shard_path)]
                        present = probe["present_run_ids"]
                        counts = probe["table_counts"]
                        identities = shard_check["recovery_file_identities"]
                        paths = [Path(row["path"]) for row in identities]
                        proof = (verify_parquet_hot_values(shards_by_path[str(shard_path)], present, hot_conn)
                                 if present else None)
                    else:
                        paths = [shard_path, Path(f"{shard_path}-wal"), Path(f"{shard_path}-journal")]
                        identities = recovery_file_identities(paths)
                        with closing(_open_readonly_sqlite(shard_path)) as shard_conn:
                            begin_read(shard_conn)
                            present = sorted(_sqlite_present_run_ids(shard_conn, normalized_run_ids))
                            counts = {table: _sqlite_count_rows_for_run_ids(shard_conn, table, normalized_run_ids)
                                      for table in ARCHIVE_TABLES}
                            proof = verify_sqlite_hot_values(shard_conn, hot_conn, present) if present else None
                    identities = [*identities, *manifest_identity]
                    paths = [*paths, *(Path(row["path"]) for row in manifest_identity)]
                    if recovery_file_identities(paths) != identities:
                        raise ValueError("archive_recovery_files_changed_during_value_verification")
                    shard_check["recovery_file_identities"] = identities
                    shard_check["hot_value_verification"] = proof
                    for check in (proof or {}).get("table_checks") or []:
                        verified_counts_by_table[check["table_name"]] += check["verified_row_count"]
                    for run_id in present:
                        run_shard_presence.setdefault(run_id, []).append(str(shard_path))
                    for table_name, count in counts.items():
                        archive_counts_by_table[table_name] += count
                        shard_counts_by_table[table_name].append({
                            "shard_id": shard_check["id"], "shard_role": shard_check["role"],
                            "shard_path": str(shard_path), "scope_row_count": count,
                        })
                except (ValueError, OSError, KeyError, TypeError, sqlite3.Error) as exc:
                    values_verified = False
                    shard_check.update(ok=False, error=str(exc))
                    blockers.append({"code": f"{shard_format}_archive_recovery_verification_failed",
                                     "detail": str(exc), "shard_id": shard_check["id"]})

            duplicate_run_ids = sorted(run_id for run_id, paths in run_shard_presence.items() if len(paths) > 1)
            missing_run_ids = sorted(set(normalized_run_ids) - set(run_shard_presence))
            if duplicate_run_ids:
                blockers.append({"code": "archive_run_present_in_multiple_shards",
                                 "detail": f"{len(duplicate_run_ids)} selected run(s) are duplicated across shards",
                                 "sample_run_ids": duplicate_run_ids[:20]})
            if missing_run_ids:
                blockers.append({"code": "archive_selected_runs_missing",
                                 "detail": f"{len(missing_run_ids)} selected run(s) lack a verified archive",
                                 "sample_run_ids": missing_run_ids[:20]})
            for table_name in ARCHIVE_TABLES:
                hot_table_exists = _sqlite_table_exists(hot_conn, table_name)
                hot_scope_row_count = (
                    _sqlite_count_rows_for_run_ids(hot_conn, table_name, normalized_run_ids)
                    if hot_table_exists
                    else 0
                )
                archive_scope_row_count = int(archive_counts_by_table.get(table_name) or 0)
                archive_table_exists = any(
                    item["table_exists"]
                    for shard_check in archive_shard_checks
                    for item in (shard_check.get("hot_value_verification") or {}).get("table_checks", [])
                    if item["table_name"] == table_name
                )
                row_count_match = hot_scope_row_count == archive_scope_row_count
                exact_values_match = bool(values_verified and not duplicate_run_ids and not missing_run_ids
                                          and verified_counts_by_table[table_name] == hot_scope_row_count)
                exact_archive_match = row_count_match and exact_values_match
                table_check = {
                    "table_name": table_name,
                    "hot_table_exists": hot_table_exists,
                    "archive_table_exists": archive_table_exists,
                    "hot_scope_row_count": hot_scope_row_count,
                    "archive_scope_row_count": archive_scope_row_count,
                    "archive_shard_scope_counts": shard_counts_by_table.get(table_name) or [],
                    "row_count_match": row_count_match,
                    "exact_values_match": exact_values_match,
                    "exact_archive_match": exact_archive_match,
                    "ok": bool(archive_table_exists or hot_scope_row_count == 0)
                    and exact_archive_match,
                }
                table_checks.append(table_check)
                if not table_check["ok"]:
                    blockers.append(
                        {
                            "code": "archive_scope_mismatch",
                            "detail": (
                                f"{table_name}: hot_scope={hot_scope_row_count}, "
                                f"archive_scope={archive_scope_row_count}, "
                                f"archive_table_exists={archive_table_exists}"
                            ),
                            "table_name": table_name,
                        }
                    )

    return {
        "status": ARCHIVE_GUARD_STATUS_OK if not blockers else "blocked",
        "required": True,
        "selected_run_count": len(normalized_run_ids),
        "source_archive_execution_artifact": source_artifact,
        "archive_execution_status": archive_status,
        "archive_execution_mode": archive_mode,
        "hot_db_path": str(hot_db_path),
        "archive_db_path": str(archive_db_path),
        "archive_manifest_status": archive_manifest_status,
        "archive_manifest_path": archive_manifest_state.get("manifest_path") or "",
        "archive_manifest_generation": archive_manifest_generation,
        "archive_execution_manifest_generation": artifact_manifest_generation,
        "archive_shard_count": len(archive_shard_checks),
        "archive_shard_checks": archive_shard_checks,
        "hot_db_size_bytes": hot_db_size_bytes,
        "hot_db_quick_check_required": hot_quick_check_required,
        "hot_db_quick_check_skip_threshold_bytes": HOT_DB_QUICK_CHECK_SKIP_SIZE_BYTES,
        "hot_db_readonly_probe": hot_readonly_probe,
        "hot_db_quick_check": hot_quick_check,
        "archive_db_quick_check": archive_quick_check,
        "archive_db_quick_check_source": archive_quick_check_source,
        "table_checks": table_checks,
        "blocker_count": len(blockers),
        "blockers": blockers,
    }


def _delete_batch(
    db_manager, run_ids: Sequence[str], *, delete_runs: bool, archive_value_checks: Sequence[dict] | None = None,
) -> dict:
    normalized_run_ids = _normalize_run_ids(run_ids)
    with db_manager.get_session(
        task_name="apply_screening_history_cleanup_batch",
        readonly=False,
    ) as session:
        connection = session.connection().connection.driver_connection
        verified_archive_rows = None
        if archive_value_checks is not None:
            from utils.screening_archive_access import verify_hot_before_delete, verify_recovery_files_unchanged

            # Acquire SQLite's writer lock before reading the values to be deleted.
            connection.execute("BEGIN IMMEDIATE")
            verified_archive_rows = verify_hot_before_delete(connection, normalized_run_ids, archive_value_checks)
        deleted_decisions = session.query(ScreeningDecision).filter(
            ScreeningDecision.run_id.in_(normalized_run_ids)
        ).delete(synchronize_session=False)
        from database.screening_decision_payloads import collect_unreferenced_payloads

        deleted_payload_objects = collect_unreferenced_payloads(session.connection().connection.driver_connection)
        deleted_picks = session.query(ScreeningPick).filter(
            ScreeningPick.run_id.in_(normalized_run_ids)
        ).delete(synchronize_session=False)
        deleted_runs = 0
        if delete_runs:
            deleted_runs = session.query(ScreeningRun).filter(
                ScreeningRun.run_id.in_(normalized_run_ids)
            ).delete(synchronize_session=False)
        metric_sync = delete_pick_metric_snapshots_for_run_ids(
            session,
            normalized_run_ids,
        )
        decision_summary_sync = delete_screening_decision_summaries_for_run_ids(
            session,
            normalized_run_ids,
        )
        stats_sync = mark_storage_stats_after_detail_cleanup(
            session,
            normalized_run_ids,
            delete_runs=delete_runs,
            stats_source="cleanup_execution",
        )
        if archive_value_checks is not None:
            verify_recovery_files_unchanged(archive_value_checks)

    return {
        "archive_rows_reverified_in_write_transaction": verified_archive_rows,
        "deleted_decisions": int(deleted_decisions or 0),
        "deleted_payload_objects": deleted_payload_objects,
        "deleted_picks": int(deleted_picks or 0),
        "deleted_pick_metric_snapshots": int(metric_sync.get("deleted_count") or 0),
        "deleted_decision_reason_summaries": int(
            decision_summary_sync.get("deleted_reason_count") or 0
        ),
        "deleted_run_latest_activities": int(
            decision_summary_sync.get("deleted_latest_activity_count") or 0
        ),
        "deleted_runs": int(deleted_runs or 0),
        "deleted_total": int(deleted_decisions or 0)
        + int(deleted_picks or 0)
        + int(deleted_runs or 0),
        "pick_metric_snapshots_sync": metric_sync,
        "decision_summary_sync": decision_summary_sync,
        "storage_stats_sync": stats_sync,
    }


def execute_screening_history_cleanup_runbook(
    *,
    db_manager=None,
    runbook_payload: Optional[dict] = None,
    preflight_payload: Optional[dict] = None,
    archive_execution_payload: Optional[dict] = None,
    source_runbook_artifact: Optional[str] = None,
    source_preflight_artifact: Optional[str] = None,
    source_archive_execution_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
    mode: str = "dry-run",
    start_batch: int = 1,
    end_batch: Optional[int] = None,
    confirm_apply: Optional[str] = None,
    backup_file: Optional[str] = None,
    backup_dir: str | Path | None = None,
    backup_prefix: str = DEFAULT_CLEANUP_BACKUP_PREFIX,
    backup_compress: bool = True,
    auto_create_backup_if_missing: bool = True,
    require_publication_guard: bool = True,
    require_archive_guard: bool = True,
    archive_target_path: str | Path | None = None,
    project_root: str | Path | None = None,
    latest_report_file: str | Path | None = None,
    publication_registry_file: str | Path | None = None,
) -> Tuple[Path, Path, dict]:
    """执行或预演筛选历史 cleanup runbook。"""
    normalized_mode = _normalize_mode(mode)
    source_runbook_artifact, runbook_payload = _load_artifact_payload(
        payload=runbook_payload,
        source_artifact=source_runbook_artifact,
        output_dir=output_dir,
        loader=load_latest_screening_history_cleanup_runbook_payload,
    )
    source_preflight_artifact, preflight_payload = _load_artifact_payload(
        payload=preflight_payload,
        source_artifact=source_preflight_artifact,
        output_dir=output_dir,
        loader=load_latest_screening_history_cleanup_preflight_payload,
    )

    runbook_summary = dict(runbook_payload.get("summary") or {})
    preflight_summary = dict(preflight_payload.get("summary") or {})
    preflight_checks = dict(preflight_payload.get("checks") or {})
    delete_runs = bool(runbook_summary.get("delete_runs"))
    selected_batches, normalized_start_batch, normalized_end_batch = _select_batches(
        runbook_payload.get("batches") or [],
        start_batch,
        end_batch,
    )
    _validate_selected_batches(selected_batches)

    backup_path = Path(str(backup_file or "")).expanduser() if backup_file else None
    root_for_backup_readiness = Path(project_root or DEFAULT_PROJECT_ROOT).expanduser().resolve()
    source_db_path = (
        preflight_payload.get("db_path")
        or runbook_payload.get("db_path")
        or ""
    )
    resolved_backup_dir = (
        backup_dir
        if backup_dir is not None
        else (
            Path(str(source_db_path)).expanduser().parent / "backups"
            if source_db_path
            else "backend/data/backups"
        )
    )
    json_path, md_path = _resolve_output_paths(output_dir)
    selected_run_count = sum(
        len(_normalize_run_ids(batch.get("run_ids") or []))
        for batch in selected_batches
    )
    selected_run_ids = _normalize_run_ids(
        run_id
        for batch in selected_batches
        for run_id in _normalize_run_ids(batch.get("run_ids") or [])
    )
    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "source_screening_history_cleanup_runbook_artifact": source_runbook_artifact or "",
        "source_screening_history_cleanup_preflight_artifact": source_preflight_artifact or "",
        "source_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_preflight_generated_at": preflight_payload.get("generated_at") or "",
        "parameters": {
            "start_batch": normalized_start_batch,
            "end_batch": normalized_end_batch,
            "selected_batch_count": len(selected_batches),
            "selected_run_count": selected_run_count,
            "delete_runs": delete_runs,
            "backup_file": str(backup_path) if backup_path else "",
            "backup_dir": str(resolved_backup_dir),
            "backup_prefix": backup_prefix,
            "backup_compress": bool(backup_compress),
            "auto_create_backup_if_missing": bool(auto_create_backup_if_missing),
        },
        "preflight_summary": preflight_summary,
        "preflight_checks": preflight_checks,
        "runbook_summary": runbook_summary,
        "summary": {
            "selected_batch_count": len(selected_batches),
            "selected_run_count": selected_run_count,
            "completed_batch_count": 0,
            "failed_batch_count": 0,
            "deleted_decisions": 0,
            "deleted_picks": 0,
            "deleted_pick_metric_snapshots": 0,
            "deleted_decision_reason_summaries": 0,
            "deleted_run_latest_activities": 0,
            "deleted_runs": 0,
            "deleted_total": 0,
        },
        "publication_guard": {
            "status": "not_required" if normalized_mode == "dry-run" else "pending",
            "required": bool(require_publication_guard) if normalized_mode == "apply" else False,
        },
        "archive_guard": {
            "status": "pending" if require_archive_guard and selected_run_ids else "not_required",
            "required": bool(require_archive_guard and selected_run_ids),
            "selected_run_count": len(selected_run_ids),
        },
        "apply_readiness": {
            "status": "pending" if normalized_mode == "dry-run" and selected_run_ids else "not_required",
            "selected_batch_count": len(selected_batches),
            "selected_run_count": len(selected_run_ids),
            "backup_required_for_apply": True,
            "backup_checked": False,
            "backup_status": "not_checked_in_dry_run",
            "backup_readiness": {},
            "blocker_count": 0,
            "blockers": [],
        },
        "post_apply_consistency": {},
        "findings": [],
        "batches": [],
    }

    if normalized_mode == "apply" and not selected_batches:
        payload["status"] = "applied"
        payload["publication_guard"] = {
            "status": "not_required",
            "required": bool(require_publication_guard),
            "selected_run_count": 0,
            "protected_run_count": 0,
            "intersecting_run_ids": [],
            "blockers": [],
            "protected_sources": [],
        }
        payload["archive_guard"] = {
            "status": "not_required",
            "required": bool(require_archive_guard),
            "selected_run_count": 0,
            "blocker_count": 0,
            "blockers": [],
            "table_checks": [],
        }
        payload["apply_readiness"] = {
            "status": "no_scope",
            "selected_batch_count": 0,
            "selected_run_count": 0,
            "backup_required_for_apply": False,
            "backup_checked": False,
            "backup_status": "not_required_no_scope",
            "backup_readiness": {},
            "blocker_count": 0,
            "blockers": [],
        }
        payload["post_apply_consistency"] = {
            "status": "ok",
            "selected_run_count": 0,
            "delete_runs": bool(delete_runs),
            "no_scope": True,
            "checks": [],
            "blocker_count": 0,
            "blockers": [],
        }
        payload["findings"].append(
            "当前为 apply 模式但无待执行批次；按幂等 no-op 完成，不创建备份、不执行删除。"
        )
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    if normalized_mode == "apply":
        _validate_apply_prerequisites(
            selected_batches=selected_batches,
            preflight_payload=preflight_payload,
            runbook_payload=runbook_payload,
            source_runbook_artifact=source_runbook_artifact,
            confirm_apply=confirm_apply,
            backup_path=backup_path,
        )
        publication_guard = _build_publication_apply_guard(
            selected_run_ids=selected_run_ids,
            require_publication_guard=require_publication_guard,
            project_root=project_root,
            latest_report_file=latest_report_file,
            publication_registry_file=publication_registry_file,
        )
        payload["publication_guard"] = publication_guard
        if publication_guard.get("status") == "blocked":
            payload["status"] = "failed"
            payload["findings"].append(
                "发布链保护阻断本次筛选历史 cleanup apply；当前 selected run_id 与正式发布链或发布完整性门禁冲突。"
            )
            _persist_checkpoint(payload, json_path, md_path)
            return json_path, md_path, payload
        archive_guard = _build_archive_apply_guard(
            selected_run_ids=selected_run_ids,
            archive_execution_payload=archive_execution_payload,
            source_archive_execution_artifact=source_archive_execution_artifact,
            require_archive_guard=require_archive_guard,
            project_root=project_root,
            preflight_payload=preflight_payload,
            db_path=preflight_payload.get("db_path") or runbook_payload.get("db_path"),
            archive_target_path=archive_target_path,
        )
        payload["archive_guard"] = archive_guard
        if archive_guard.get("status") == "blocked":
            payload["status"] = "failed"
            payload["findings"].append(
                "冷库归档覆盖门禁阻断本次筛选历史 cleanup apply；当前 selected run_id 尚未被冷库精确覆盖。"
            )
            _persist_checkpoint(payload, json_path, md_path)
            return json_path, md_path, payload
        payload["findings"].append(
            "已通过前置核对、确认串、备份文件、发布链保护和冷库归档覆盖检查，可以进入批次执行。"
        )
    else:
        if selected_run_ids and require_archive_guard:
            archive_guard = _build_archive_apply_guard(
                selected_run_ids=selected_run_ids,
                archive_execution_payload=archive_execution_payload,
                source_archive_execution_artifact=source_archive_execution_artifact,
                require_archive_guard=require_archive_guard,
                project_root=project_root,
                preflight_payload=preflight_payload,
                db_path=preflight_payload.get("db_path") or runbook_payload.get("db_path"),
                archive_target_path=archive_target_path,
            )
            payload["archive_guard"] = archive_guard
            if archive_guard.get("status") == "blocked":
                payload["findings"].append(
                    "冷库归档覆盖 dry-run 审计未通过；当前不执行删除，但未来 cleanup apply 前必须先补齐冷库覆盖。"
                )
            else:
                payload["findings"].append(
                    "冷库归档覆盖 dry-run 审计通过；当前 selected run_id 已被冷库精确覆盖。"
                )
        elif not require_archive_guard:
            payload["archive_guard"] = {
                "status": "disabled",
                "required": False,
                "selected_run_count": len(selected_run_ids),
                "blocker_count": 0,
                "blockers": [],
                "table_checks": [],
            }
        publication_apply_guard = _build_publication_apply_guard(
            selected_run_ids=selected_run_ids,
            require_publication_guard=require_publication_guard,
            project_root=project_root,
            latest_report_file=latest_report_file,
            publication_registry_file=publication_registry_file,
        )
        backup_readiness = {}
        if selected_run_ids:
            backup_readiness = build_screening_history_cleanup_backup_readiness_payload(
                source_db_path=source_db_path,
                backup_dir=resolved_backup_dir,
                project_root=root_for_backup_readiness,
                backup_prefix=backup_prefix,
                backup_file=backup_file,
                compress=bool(backup_compress),
                auto_create_backup_if_missing=bool(auto_create_backup_if_missing),
            )
        payload["apply_readiness"] = _build_apply_readiness_payload(
            selected_batch_count=len(selected_batches),
            selected_run_ids=selected_run_ids,
            preflight_checks=preflight_checks,
            publication_guard=publication_apply_guard,
            archive_guard=payload.get("archive_guard") or {},
            backup_readiness=backup_readiness,
        )
        if payload["apply_readiness"].get("status") == "blocked":
            payload["findings"].append(
                "未来 cleanup apply readiness 审计未通过；当前仍为 dry-run 且不执行删除，切换 apply 前必须先消除阻断。"
            )
        elif payload["apply_readiness"].get("status") == "ready":
            payload["findings"].append(
                "未来 cleanup apply readiness 审计通过；当前 scope 已通过发布链保护和冷库覆盖检查，但 apply 仍需独立备份和显式配置。"
            )
        else:
            payload["findings"].append(
                "未来 cleanup apply readiness 当前无可执行 scope；无需进入删除准备。"
            )
        payload["findings"].append(
            "当前为 dry-run，只输出计划和 checkpoint，不执行任何删除。"
        )

    if selected_batches:
        payload["findings"].append(
            f"本次选择批次 {normalized_start_batch}-{normalized_end_batch}，共 {len(selected_batches)} 批、{selected_run_count} 个 run_id。"
        )
    else:
        payload["findings"].append("当前 runbook 没有待执行批次。")

    _persist_checkpoint(payload, json_path, md_path)

    if normalized_mode == "dry-run":
        payload["status"] = "dry-run"
        payload["batches"] = [
            _build_batch_checkpoint(batch, status="planned")
            for batch in selected_batches
        ]
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    manager = db_manager or get_db_manager()
    payload["status"] = "running"
    payload["batches"] = [
        _build_batch_checkpoint(batch, status="pending")
        for batch in selected_batches
    ]
    _persist_checkpoint(payload, json_path, md_path)

    for batch, batch_result in zip(selected_batches, payload["batches"]):
        batch_result["status"] = "running"
        batch_result["started_at"] = datetime.now().isoformat()
        _persist_checkpoint(payload, json_path, md_path)

        try:
            delete_result = _delete_batch(
                manager,
                batch_result.get("run_ids") or [],
                delete_runs=delete_runs,
                archive_value_checks=(payload["archive_guard"].get("archive_shard_checks") or [])
                if require_archive_guard else None,
            )
            batch_result.update(delete_result)
            batch_result["status"] = "applied"
            batch_result["finished_at"] = datetime.now().isoformat()
            payload["summary"]["completed_batch_count"] += 1
            payload["summary"]["deleted_decisions"] += delete_result["deleted_decisions"]
            payload["summary"]["deleted_picks"] += delete_result["deleted_picks"]
            payload["summary"]["deleted_pick_metric_snapshots"] += delete_result.get("deleted_pick_metric_snapshots", 0)
            payload["summary"]["deleted_decision_reason_summaries"] += delete_result.get("deleted_decision_reason_summaries", 0)
            payload["summary"]["deleted_run_latest_activities"] += delete_result.get("deleted_run_latest_activities", 0)
            payload["summary"]["deleted_runs"] += delete_result["deleted_runs"]
            payload["summary"]["deleted_total"] += delete_result["deleted_total"]
            _persist_checkpoint(payload, json_path, md_path)
        except Exception as exc:
            batch_result["status"] = "failed"
            batch_result["finished_at"] = datetime.now().isoformat()
            batch_result["error"] = f"{type(exc).__name__}: {exc}"
            payload["status"] = "failed"
            payload["summary"]["failed_batch_count"] += 1
            payload["findings"].append(
                f"批次 {batch_result['batch_id']} 执行失败，已停止后续批次；恢复前先核对备份文件与 checkpoint。"
            )
            _persist_checkpoint(payload, json_path, md_path)
            return json_path, md_path, payload

    post_apply_consistency = _build_post_apply_consistency_payload(
        manager,
        selected_run_ids=selected_run_ids,
        delete_runs=delete_runs,
    )
    payload["post_apply_consistency"] = post_apply_consistency
    if str(post_apply_consistency.get("status") or "") != "ok":
        payload["status"] = "failed"
        payload["findings"].append(
            "全部选定批次已执行，但执行后一致性复核失败；恢复或继续前必须核对 checkpoint 与备份。"
        )
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    payload["status"] = "applied"
    payload["findings"].append(
        "全部选定批次已执行完成，执行后一致性复核通过；若需要回收磁盘空间，仍需在独立维护窗口评估 VACUUM。"
    )
    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload


def render_screening_history_cleanup_execution_markdown(payload: dict) -> str:
    """渲染执行 checkpoint Markdown。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    publication_guard = payload.get("publication_guard") or {}
    archive_guard = payload.get("archive_guard") or {}
    apply_readiness = payload.get("apply_readiness") or {}
    post_apply_consistency = payload.get("post_apply_consistency") or {}
    lines = [
        "# screening history cleanup execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- source_runbook_artifact: {payload.get('source_screening_history_cleanup_runbook_artifact')}",
        f"- source_preflight_artifact: {payload.get('source_screening_history_cleanup_preflight_artifact')}",
        f"- backup_file: {parameters.get('backup_file')}",
        f"- start_batch: {parameters.get('start_batch')}",
        f"- end_batch: {parameters.get('end_batch')}",
        f"- selected_batch_count: {summary.get('selected_batch_count', 0)}",
        f"- selected_run_count: {summary.get('selected_run_count', 0)}",
        f"- completed_batch_count: {summary.get('completed_batch_count', 0)}",
        f"- failed_batch_count: {summary.get('failed_batch_count', 0)}",
        f"- deleted_decisions: {summary.get('deleted_decisions', 0)}",
        f"- deleted_picks: {summary.get('deleted_picks', 0)}",
        f"- deleted_pick_metric_snapshots: {summary.get('deleted_pick_metric_snapshots', 0)}",
        f"- deleted_decision_reason_summaries: {summary.get('deleted_decision_reason_summaries', 0)}",
        f"- deleted_run_latest_activities: {summary.get('deleted_run_latest_activities', 0)}",
        f"- deleted_runs: {summary.get('deleted_runs', 0)}",
        f"- deleted_total: {summary.get('deleted_total', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## publication_guard",
            f"- status: {publication_guard.get('status', '')}",
            f"- required: {publication_guard.get('required', False)}",
            f"- protected_run_count: {publication_guard.get('protected_run_count', 0)}",
            f"- intersecting_run_ids: {json.dumps(publication_guard.get('intersecting_run_ids') or [], ensure_ascii=False)}",
            f"- blocker_count: {len(publication_guard.get('blockers') or [])}",
        ]
    )
    for blocker in publication_guard.get("blockers") or []:
        lines.append("- blocker: " + json.dumps(blocker, ensure_ascii=False, sort_keys=True))

    lines.extend(
        [
            "",
            "## archive_guard",
            f"- status: {archive_guard.get('status', '')}",
            f"- required: {archive_guard.get('required', False)}",
            f"- selected_run_count: {archive_guard.get('selected_run_count', 0)}",
            f"- hot_db_readonly_probe: {archive_guard.get('hot_db_readonly_probe', '')}",
            f"- hot_db_quick_check: {archive_guard.get('hot_db_quick_check', '')}",
            f"- hot_db_quick_check_required: {archive_guard.get('hot_db_quick_check_required', '')}",
            f"- archive_db_quick_check: {archive_guard.get('archive_db_quick_check', '')}",
            f"- table_check_count: {len(archive_guard.get('table_checks') or [])}",
            f"- blocker_count: {archive_guard.get('blocker_count', len(archive_guard.get('blockers') or []))}",
        ]
    )
    for blocker in archive_guard.get("blockers") or []:
        lines.append("- blocker: " + json.dumps(blocker, ensure_ascii=False, sort_keys=True))
    for check in archive_guard.get("table_checks") or []:
        lines.append("- table_check: " + json.dumps(check, ensure_ascii=False, sort_keys=True))

    lines.extend(
        [
            "",
            "## apply_readiness",
            f"- status: {apply_readiness.get('status', '')}",
            f"- selected_batch_count: {apply_readiness.get('selected_batch_count', 0)}",
            f"- selected_run_count: {apply_readiness.get('selected_run_count', 0)}",
            f"- preflight_ready: {apply_readiness.get('preflight_ready', '')}",
            f"- publication_guard_status: {apply_readiness.get('publication_guard_status', '')}",
            f"- archive_guard_status: {apply_readiness.get('archive_guard_status', '')}",
            f"- backup_required_for_apply: {apply_readiness.get('backup_required_for_apply', '')}",
            f"- backup_status: {apply_readiness.get('backup_status', '')}",
            f"- backup_checked: {apply_readiness.get('backup_checked', '')}",
            f"- backup_creation_mode: {apply_readiness.get('backup_creation_mode', '')}",
            f"- backup_auto_create_if_missing: {apply_readiness.get('backup_auto_create_if_missing', '')}",
            f"- backup_blocker_count: {apply_readiness.get('backup_blocker_count', 0)}",
            f"- blocker_count: {apply_readiness.get('blocker_count', len(apply_readiness.get('blockers') or []))}",
        ]
    )
    for blocker in apply_readiness.get("blockers") or []:
        lines.append("- blocker: " + json.dumps(blocker, ensure_ascii=False, sort_keys=True))

    lines.extend(
        [
            "",
            "## post_apply_consistency",
            f"- status: {post_apply_consistency.get('status', '')}",
            f"- selected_run_count: {post_apply_consistency.get('selected_run_count', 0)}",
            f"- delete_runs: {post_apply_consistency.get('delete_runs', False)}",
            f"- blocker_count: {post_apply_consistency.get('blocker_count', 0)}",
        ]
    )
    for check in post_apply_consistency.get("checks") or []:
        lines.append("- check: " + json.dumps(check, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## batches"])
    for batch in payload.get("batches") or []:
        lines.append(
            "- " + json.dumps(
                {
                    "batch_id": batch.get("batch_id"),
                    "execution_order": batch.get("execution_order"),
                    "batch_run_count": batch.get("batch_run_count"),
                    "status": batch.get("status"),
                    "deleted_total": batch.get("deleted_total"),
                    "error": batch.get("error"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )

    return "\n".join(lines).strip() + "\n"
