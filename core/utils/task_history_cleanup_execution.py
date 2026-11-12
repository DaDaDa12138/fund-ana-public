#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guarded cleanup/archive execution for task_history rows.

The default mode is dry-run. Apply requires a dry-run plan fingerprint,
explicit confirmation, a verified rollback backup, row hash consistency, and
post-delete consistency checks. It never VACUUMs the database.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
from typing import Any, Optional, Sequence, Tuple

from utils.screening_history_cleanup_backup import create_screening_history_cleanup_backup
from utils.task_history_governance_preview import SNAPSHOT_WATCHDOG_TASK_NAME


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/task_history_cleanup"
DEFAULT_ARCHIVE_DIR = "reports/.audit/task_history_cleanup"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_task_history_cleanup"
TASK_HISTORY_CLEANUP_EXECUTION_JSON_PATTERN = "task_history_cleanup_execution_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_TASK_HISTORY_CLEANUP"
DEFAULT_ELIGIBLE_STATUSES = ("success", "skipped")
PROTECTED_STATUSES = {"running", "failed", "degraded"}
TASK_HISTORY_COLUMNS = (
    "id",
    "task_name",
    "status",
    "started_at",
    "completed_at",
    "duration",
    "records_processed",
    "records_inserted",
    "records_updated",
    "records_failed",
    "error_message",
    "error_traceback",
    "extra_data",
)


def _task_history_select_columns(conn: sqlite3.Connection) -> str:
    columns = [row[1] for row in conn.execute("PRAGMA table_info(task_history)")]
    if set(columns) != set(TASK_HISTORY_COLUMNS):
        raise ValueError("task_history_schema_requires_complete_archive_projection")
    return ", ".join(TASK_HISTORY_COLUMNS)


def _raw_row_sha256(row: dict[str, Any]) -> str:
    """Bind every declared value and its JSON type, without display normalization."""
    if set(row) != set(TASK_HISTORY_COLUMNS):
        raise ValueError("task_history_schema_requires_complete_archive_projection")
    text = json.dumps([row[column] for column in TASK_HISTORY_COLUMNS],
                      ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _describe_task_row(row: sqlite3.Row) -> dict[str, Any]:
    original = {column: row[column] for column in TASK_HISTORY_COLUMNS}
    extra = original["extra_data"]
    if extra is not None and not isinstance(extra, str):
        raise ValueError("task_history_extra_data_requires_text_or_null")
    return {
        **original,
        "_raw_row": original,
        "row_sha256": _raw_row_sha256(original),
        "task_name": _normalize_text(original["task_name"]) or "unknown",
        "status": _normalize_text(original["status"]).lower() or "unknown",
        "started_at": _normalize_text(original["started_at"]),
        "completed_at": _normalize_text(original["completed_at"]) or None,
        "extra_data_bytes": len(extra.encode("utf-8")) if extra is not None else None,
        "extra_data_sha256": _sha256_text(extra) if extra is not None else None,
        "_started_at_dt": _parse_datetime(original["started_at"]),
    }


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(normalized.replace(" ", "T"))
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed


def _resolve_generated_at(value: str | None) -> datetime:
    return _parse_datetime(value) or datetime.now()


def _resolve_project_path(value: str | Path, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _project_relative(path: str | Path, *, project_root: Path = PROJECT_ROOT) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_json_dict(raw_value: Any) -> dict[str, Any]:
    text = _normalize_text(raw_value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _connect_sqlite(db_path: str | Path, *, readonly: bool) -> sqlite3.Connection:
    resolved = Path(db_path).resolve()
    if readonly:
        conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True, timeout=120)
    else:
        conn = sqlite3.connect(str(resolved), timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _sqlite_quick_check(path: Path) -> str:
    conn = _connect_sqlite(path, readonly=True)
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0] if row else "").strip()
    finally:
        conn.close()


def _row_sort_key(row: dict[str, Any]) -> tuple[datetime, int]:
    parsed = row.get("_started_at_dt")
    if not isinstance(parsed, datetime):
        parsed = datetime.min
    return (parsed, _normalize_int(row.get("id")))


def _normalize_statuses(values: Sequence[str] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values or DEFAULT_ELIGIBLE_STATUSES:
        text = _normalize_text(item).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return tuple(result) or DEFAULT_ELIGIBLE_STATUSES


def _read_task_history_rows(db_path: Path) -> list[dict[str, Any]]:
    conn = _connect_sqlite(db_path, readonly=True)
    try:
        if not _table_exists(conn, "task_history"):
            return []
        select_cols = _task_history_select_columns(conn)
        rows = conn.execute(
            f"""
            SELECT {select_cols}
            FROM task_history
            ORDER BY started_at DESC, id DESC
            """
        ).fetchall()
    finally:
        conn.close()

    return [_describe_task_row(row) for row in rows]


def _public_row(row: dict[str, Any], *, include_hash: bool = True) -> dict[str, Any]:
    payload = {
        "id": row.get("id"),
        "task_name": row.get("task_name"),
        "status": row.get("status"),
        "started_at": row.get("started_at"),
        "completed_at": row.get("completed_at"),
        "extra_data_bytes": row.get("extra_data_bytes"),
    }
    if row.get("skip_reason"):
        payload["skip_reason"] = row.get("skip_reason")
    if include_hash:
        payload["extra_data_sha256"] = row.get("extra_data_sha256")
        payload["row_sha256"] = row.get("row_sha256")
    return payload


def _extract_skip_reason(row: dict[str, Any]) -> str:
    payload = _safe_json_dict(row.get("extra_data"))
    return (
        _normalize_text(payload.get("skip_reason"))
        or _normalize_text(payload.get("snapshot_watchdog_reason"))
        or _normalize_text(payload.get("no_action_category"))
        or "unknown"
    )


def _build_snapshot_watchdog_candidate_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    day_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()
    per_day_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    latest_started_at = ""
    for row in rows:
        if row.get("task_name") != SNAPSHOT_WATCHDOG_TASK_NAME or row.get("status") != "skipped":
            continue
        parsed = row.get("_started_at_dt")
        day_key = parsed.date().isoformat() if isinstance(parsed, datetime) else "unknown"
        reason = str(row.get("skip_reason") or _extract_skip_reason(row) or "unknown")
        day_counter[day_key] += 1
        reason_counter[reason] += 1
        per_day_reasons[day_key][reason] += 1
        if not latest_started_at:
            latest_started_at = str(row.get("started_at") or "")

    daily_summary = [
        {
            "date": day_key,
            "skipped_count": int(day_counter[day_key]),
            "skip_reason_counts": dict(per_day_reasons[day_key].most_common()),
        }
        for day_key in sorted(day_counter.keys(), reverse=True)
    ]
    return {
        "task_name": SNAPSHOT_WATCHDOG_TASK_NAME,
        "candidate_skipped_total": sum(day_counter.values()),
        "candidate_skipped_day_count": len(day_counter),
        "latest_started_at": latest_started_at,
        "skip_reason_counts": dict(reason_counter.most_common()),
        "daily_summary": daily_summary,
    }


def _candidate_fingerprint_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": int(row.get("id") or 0),
            "task_name": row.get("task_name"),
            "status": row.get("status"),
            "started_at": row.get("started_at"),
            "completed_at": row.get("completed_at"),
            "extra_data_bytes": row.get("extra_data_bytes"),
            "extra_data_sha256": row.get("extra_data_sha256"),
            "row_sha256": row.get("row_sha256"),
        }
        for row in sorted(rows, key=lambda item: int(item.get("id") or 0))
    ]


def _build_plan_fingerprint(
    *,
    parameters: dict[str, Any],
    candidate_rows: Sequence[dict[str, Any]],
    protected_row_ids: Sequence[int],
) -> str:
    payload = {
        "parameters": parameters,
        "candidate_rows": _candidate_fingerprint_rows(candidate_rows),
        "protected_row_ids": sorted(int(item) for item in protected_row_ids),
    }
    return _sha256_text(_json_dumps(payload))


def build_task_history_cleanup_plan_payload(
    *,
    db_path: str | Path,
    generated_at: str | None = None,
    retention_days: int = 14,
    recent_keep_per_task_status: int = 20,
    eligible_statuses: Sequence[str] | None = DEFAULT_ELIGIBLE_STATUSES,
    max_sample_rows: int = 20,
) -> dict[str, Any]:
    resolved_db_path = _resolve_project_path(db_path)
    generated_dt = _resolve_generated_at(generated_at)
    generated_text = generated_dt.isoformat(timespec="seconds")
    retention_days = max(1, int(retention_days))
    recent_keep_per_task_status = max(1, int(recent_keep_per_task_status))
    max_sample_rows = max(1, int(max_sample_rows))
    normalized_eligible_statuses = _normalize_statuses(eligible_statuses)
    cutoff_dt = generated_dt - timedelta(days=retention_days)
    parameters = {
        "retention_days": retention_days,
        "recent_keep_per_task_status": recent_keep_per_task_status,
        "eligible_statuses": list(normalized_eligible_statuses),
        "protected_statuses": sorted(PROTECTED_STATUSES),
        "retention_cutoff_at": cutoff_dt.isoformat(timespec="seconds"),
    }

    if not resolved_db_path.exists():
        return {
            "generated_at": generated_text,
            "mode": "plan",
            "preview_only": True,
            "status": "missing",
            "db_path": str(resolved_db_path),
            "parameters": parameters,
            "summary": {
                "database_exists": False,
                "table_exists": False,
                "total_rows": 0,
                "candidate_total": 0,
                "protected_row_count": 0,
            },
            "candidate_rows": [],
            "candidate_samples": [],
            "protected_row_ids": [],
            "findings": ["task_history database is missing."],
        }

    rows = _read_task_history_rows(resolved_db_path)
    if not rows:
        return {
            "generated_at": generated_text,
            "mode": "plan",
            "preview_only": True,
            "status": "missing",
            "db_path": str(resolved_db_path),
            "parameters": parameters,
            "summary": {
                "database_exists": True,
                "table_exists": False,
                "total_rows": 0,
                "candidate_total": 0,
                "protected_row_count": 0,
            },
            "candidate_rows": [],
            "candidate_samples": [],
            "protected_row_ids": [],
            "findings": ["task_history table is missing or empty."],
        }

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("task_name")), str(row.get("status")))].append(row)

    protected_ids: set[int] = set()
    protected_reasons: dict[int, set[str]] = defaultdict(set)
    for row in rows:
        row_id = int(row["id"])
        status = str(row.get("status") or "")
        if status in PROTECTED_STATUSES:
            protected_ids.add(row_id)
            protected_reasons[row_id].add(f"protected_status:{status}")
        if row.get("_started_at_dt") is None:
            protected_ids.add(row_id)
            protected_reasons[row_id].add("missing_or_invalid_started_at")
    for group_rows in grouped.values():
        ordered = sorted(group_rows, key=_row_sort_key, reverse=True)
        for row in ordered[:recent_keep_per_task_status]:
            row_id = int(row["id"])
            protected_ids.add(row_id)
            protected_reasons[row_id].add("recent_keep_per_task_status")

    candidates: list[dict[str, Any]] = []
    skipped_by_status: Counter[str] = Counter()
    for row in rows:
        row_id = int(row["id"])
        status = str(row.get("status") or "")
        started_dt = row.get("_started_at_dt")
        if status not in normalized_eligible_statuses:
            skipped_by_status[status] += 1
            continue
        if row_id in protected_ids:
            continue
        if not isinstance(started_dt, datetime) or started_dt >= cutoff_dt:
            continue
        candidate = dict(row)
        candidate["cleanup_reason"] = "retention_cutoff"
        if candidate.get("task_name") == SNAPSHOT_WATCHDOG_TASK_NAME and status == "skipped":
            candidate["skip_reason"] = _extract_skip_reason(candidate)
        candidates.append(candidate)
    candidates.sort(key=_row_sort_key)

    status_counts = Counter(str(row.get("status") or "unknown") for row in rows)
    task_status_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        task_status_counts[str(row.get("task_name") or "unknown")][str(row.get("status") or "unknown")] += 1

    protected_row_ids = sorted(protected_ids)
    public_candidates = [_public_row(row) for row in candidates]
    fingerprint = _build_plan_fingerprint(
        parameters=parameters,
        candidate_rows=public_candidates,
        protected_row_ids=protected_row_ids,
    )
    snapshot_summary = _build_snapshot_watchdog_candidate_summary(candidates)
    candidate_extra_data_bytes = sum(int(row.get("extra_data_bytes") or 0) for row in candidates)
    summary = {
        "database_exists": True,
        "table_exists": True,
        "total_rows": len(rows),
        "status_counts": dict(status_counts.most_common()),
        "task_status_counts": {task: dict(counts) for task, counts in task_status_counts.items()},
        "candidate_total": len(candidates),
        "candidate_extra_data_bytes": candidate_extra_data_bytes,
        "candidate_status_counts": dict(Counter(str(row.get("status") or "") for row in candidates).most_common()),
        "candidate_task_status_counts": dict(Counter(f"{row.get('task_name')}|{row.get('status')}" for row in candidates).most_common()),
        "protected_row_count": len(protected_row_ids),
        "protected_statuses": sorted(PROTECTED_STATUSES),
        "eligible_statuses": list(normalized_eligible_statuses),
        "retention_days": retention_days,
        "retention_cutoff_at": cutoff_dt.isoformat(timespec="seconds"),
        "recent_keep_per_task_status": recent_keep_per_task_status,
        "plan_fingerprint": fingerprint,
        "preview_action": "no_database_mutation",
        "snapshot_watchdog_candidate_skipped_total": snapshot_summary["candidate_skipped_total"],
        "snapshot_watchdog_candidate_skipped_day_count": snapshot_summary["candidate_skipped_day_count"],
    }
    findings = [
        (
            f"task_history cleanup plan candidates={len(candidates)}, "
            f"protected_rows={len(protected_row_ids)}, total_rows={len(rows)}."
        )
    ]
    if snapshot_summary["candidate_skipped_total"]:
        findings.append(
            "snapshot watchdog skipped candidates will be archived with daily skip summary "
            f"before any apply delete: count={snapshot_summary['candidate_skipped_total']}."
        )
    if skipped_by_status:
        findings.append(
            "non-eligible statuses are protected by default: "
            + ", ".join(f"{status}={count}" for status, count in skipped_by_status.most_common())
        )

    return {
        "generated_at": generated_text,
        "mode": "plan",
        "preview_only": True,
        "status": "review" if candidates else "ok",
        "db_path": str(resolved_db_path),
        "parameters": parameters,
        "summary": summary,
        "plan_fingerprint": fingerprint,
        "candidate_rows": public_candidates,
        "candidate_samples": public_candidates[:max_sample_rows],
        "protected_row_ids": protected_row_ids,
        "protected_row_samples": [
            {
                **_public_row(row, include_hash=False),
                "protect_reasons": sorted(protected_reasons.get(int(row["id"]), set())),
            }
            for row in sorted((row for row in rows if int(row["id"]) in protected_ids), key=_row_sort_key, reverse=True)[:max_sample_rows]
        ],
        "snapshot_watchdog_skipped_archive_summary": snapshot_summary,
        "execution_plan": {
            "preview_only": True,
            "delete_rows": False,
            "archive_rows": True,
            "target_table": "task_history",
            "guardrails": [
                "running/failed/degraded statuses are protected by default.",
                "Each task_name/status group keeps the latest N rows.",
                "Apply requires a dry-run plan fingerprint and explicit confirmation.",
                "Apply writes archive JSONL and daily skipped summary before deleting rows.",
                "Apply does not execute VACUUM.",
            ],
        },
        "findings": findings,
    }


def _resolve_output_paths(output_dir: str | Path, *, timestamp: str | None = None) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = _resolve_project_path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    return (
        out_dir / f"task_history_cleanup_execution_{ts}.json",
        out_dir / f"task_history_cleanup_execution_{ts}.md",
    )


def render_task_history_cleanup_execution_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    backup = payload.get("backup") or {}
    post = payload.get("post_apply_consistency") or {}
    archive = payload.get("archive") or {}
    lines = [
        "# task_history cleanup execution",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- db_path: {payload.get('db_path')}",
        f"- candidate_total: {summary.get('candidate_total', 0)}",
        f"- deleted_count: {summary.get('deleted_count', 0)}",
        f"- protected_row_count: {summary.get('protected_row_count', 0)}",
        f"- plan_fingerprint: {payload.get('plan_fingerprint') or summary.get('plan_fingerprint')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## backup",
            f"- status: {backup.get('status', '') or backup.get('backup_status', '')}",
            f"- artifact: {backup.get('backup_artifact_path', '')}",
            "",
            "## archive",
            f"- rows_archive_path: {archive.get('rows_archive_path', '')}",
            f"- rows_archive_sha256: {archive.get('rows_archive_sha256', '')}",
            f"- skipped_summary_path: {archive.get('snapshot_watchdog_skipped_summary_path', '')}",
            "",
            "## post_apply_consistency",
            f"- status: {post.get('status', '')}",
            f"- remaining_candidate_count: {post.get('remaining_candidate_count', '')}",
            f"- protected_missing_count: {post.get('protected_missing_count', '')}",
        ]
    )
    samples = payload.get("candidate_samples") or []
    if samples:
        lines.extend(["", "## candidate_samples"])
        for row in samples[:20]:
            lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))
    return "\n".join(lines).strip() + "\n"


def write_task_history_cleanup_execution_artifacts(
    payload: dict[str, Any],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    timestamp: str | None = None,
) -> Tuple[Path, Path]:
    json_path, md_path = _resolve_output_paths(output_dir, timestamp=timestamp)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_task_history_cleanup_execution_markdown(payload), encoding="utf-8")
    return json_path, md_path


def _load_json_payload(path_value: str | Path) -> dict[str, Any]:
    path = _resolve_project_path(path_value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _prepare_backup(
    *,
    db_path: Path,
    backup_file: str | Path | None,
    backup_dir: str | Path,
    output_dir: str | Path,
    backup_prefix: str,
    compress_backup: bool,
) -> dict[str, Any]:
    if _normalize_text(backup_file):
        backup_path = _resolve_project_path(str(backup_file))
        if not backup_path.exists() or not backup_path.is_file():
            raise ValueError(f"backup_file does not exist: {backup_path}")
        summary: dict[str, Any] = {
            "status": "existing_backup_bound",
            "backup_artifact_path": _project_relative(backup_path),
            "backup_artifact_absolute_path": str(backup_path),
            "backup_artifact_size_bytes": backup_path.stat().st_size,
            "backup_artifact_format": "zstd_sqlite" if backup_path.name.endswith(".zst") else "sqlite",
            "sqlite_quick_check": "",
            "zstd_test_succeeded": False,
        }
        if backup_path.name.endswith(".zst"):
            zstd_bin = shutil.which("zstd")
            if not zstd_bin:
                raise RuntimeError("zstd executable is required to validate .zst backup_file")
            completed = subprocess.run(
                [zstd_bin, "-q", "-t", str(backup_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"zstd backup_file validation failed rc={completed.returncode}: {completed.stderr.strip()}"
                )
            summary["zstd_test_succeeded"] = True
        else:
            quick_check = _sqlite_quick_check(backup_path)
            if quick_check.lower() != "ok":
                raise RuntimeError(f"backup_file quick_check failed: {quick_check}")
            summary["sqlite_quick_check"] = quick_check
        return summary

    _, _, backup_payload = create_screening_history_cleanup_backup(
        source_db_path=str(db_path),
        backup_dir=backup_dir,
        output_dir=output_dir,
        backup_prefix=backup_prefix,
        purpose="task_history_cleanup",
        compress=bool(compress_backup),
        keep_uncompressed=False,
        compute_sha256=True,
        dry_run=False,
    )
    if backup_payload.get("status") != "success":
        raise RuntimeError(f"backup creation failed: {backup_payload.get('status')}")
    return dict(backup_payload.get("summary") or {})


def _archive_rows(
    rows: Sequence[dict[str, Any]],
    *,
    archive_dir: str | Path,
    plan_fingerprint: str,
    timestamp: str | None = None,
) -> dict[str, Any]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_root = _resolve_project_path(archive_dir) / datetime.now().strftime("%Y%m%d")
    archive_root.mkdir(parents=True, exist_ok=True)
    suffix = str(plan_fingerprint or "nofingerprint")[:16]
    rows_path = archive_root / f"task_history_cleanup_rows_{ts}_{suffix}.jsonl"
    summary_path = archive_root / f"task_history_cleanup_snapshot_watchdog_skipped_summary_{ts}_{suffix}.json"

    with rows_path.open("x", encoding="utf-8") as handle:
        for row in rows:
            original = row["_raw_row"]
            payload = {
                "schema_version": 2,
                "archived_at": datetime.now().isoformat(timespec="seconds"),
                "source_table": "task_history",
                "row": original,
                "row_sha256": _raw_row_sha256(original),
                "extra_data_bytes": row.get("extra_data_bytes"),
                "extra_data_sha256": row.get("extra_data_sha256"),
            }
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    snapshot_summary = _build_snapshot_watchdog_candidate_summary(rows)
    summary_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "plan_fingerprint": plan_fingerprint,
        "summary": snapshot_summary,
    }
    with summary_path.open("x", encoding="utf-8") as handle:
        json.dump(summary_payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    from utils.verified_backup_compression import _fsync_directory
    from utils.screening_archive_access import recovery_file_identities

    _fsync_directory(archive_root)
    _fsync_directory(archive_root.parent)
    return {
        "schema_version": 2,
        "columns": list(TASK_HISTORY_COLUMNS),
        "rows_archive_path": _project_relative(rows_path),
        "rows_archive_absolute_path": str(rows_path),
        "rows_archive_size_bytes": rows_path.stat().st_size,
        "rows_archive_sha256": _sha256_file(rows_path),
        "snapshot_watchdog_skipped_summary_path": _project_relative(summary_path),
        "snapshot_watchdog_skipped_summary_absolute_path": str(summary_path),
        "snapshot_watchdog_skipped_summary_size_bytes": summary_path.stat().st_size,
        "snapshot_watchdog_skipped_summary_sha256": _sha256_file(summary_path),
        "archived_row_count": len(rows),
        "recovery_file_identities": recovery_file_identities([rows_path, summary_path]),
    }


def _verify_archived_task_rows(archive: dict[str, Any], rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    from utils.screening_archive_access import recovery_file_identities

    identities = archive["recovery_file_identities"]
    paths = [Path(row["path"]) for row in identities]
    if recovery_file_identities(paths) != identities:
        raise ValueError("task_archive_files_changed")
    expected = {row["id"]: row["row_sha256"] for row in rows}
    if len(expected) != len(rows):
        raise ValueError("task_archive_duplicate_candidate")
    actual = {}
    digest = hashlib.sha256()
    with Path(archive["rows_archive_absolute_path"]).open("rb") as stream:
        for line in stream:
            digest.update(line)
            entry = json.loads(line)
            original = entry.get("row")
            if entry.get("schema_version") != 2 or entry.get("source_table") != "task_history" or not isinstance(original, dict):
                raise ValueError("task_archive_complete_row_required")
            signature = _raw_row_sha256(original)
            if entry.get("row_sha256") != signature or original["id"] in actual:
                raise ValueError("task_archive_row_proof_mismatch")
            extra = original["extra_data"]
            size = len(extra.encode("utf-8")) if extra is not None else None
            extra_sha = _sha256_text(extra) if extra is not None else None
            if entry.get("extra_data_bytes") != size or entry.get("extra_data_sha256") != extra_sha:
                raise ValueError("task_archive_extra_data_proof_mismatch")
            actual[original["id"]] = signature
    if digest.hexdigest() != archive["rows_archive_sha256"] or actual != expected or len(actual) != archive["archived_row_count"]:
        raise ValueError("task_archive_full_values_mismatch")
    if _sha256_file(Path(archive["snapshot_watchdog_skipped_summary_absolute_path"])) != archive["snapshot_watchdog_skipped_summary_sha256"]:
        raise ValueError("task_archive_summary_changed")
    if recovery_file_identities(paths) != identities:
        raise ValueError("task_archive_files_changed_during_verification")
    return {"status": "verified", "row_count": len(actual), "columns": list(TASK_HISTORY_COLUMNS),
            "basis": "all_declared_task_values_preserving_types_and_nulls",
            "row_proofs_sha256": _sha256_text(_json_dumps(sorted(actual.items())))}


def _count_existing_ids(conn: sqlite3.Connection, ids: Sequence[int]) -> int:
    normalized = [int(item) for item in ids if int(item or 0) > 0]
    if not normalized:
        return 0
    total = 0
    for start in range(0, len(normalized), 900):
        chunk = normalized[start:start + 900]
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"SELECT COUNT(*) FROM task_history WHERE id IN ({placeholders})",
                chunk,
            ).fetchone()[0]
            or 0
        )
    return total


def _delete_ids(conn: sqlite3.Connection, ids: Sequence[int], eligible_statuses: Sequence[str]) -> int:
    normalized = [int(item) for item in ids if int(item or 0) > 0]
    statuses = list(_normalize_statuses(eligible_statuses))
    if not normalized:
        return 0
    deleted = 0
    for start in range(0, len(normalized), 500):
        chunk = normalized[start:start + 500]
        id_placeholders = ",".join("?" for _ in chunk)
        status_placeholders = ",".join("?" for _ in statuses)
        cursor = conn.execute(
            f"""
            DELETE FROM task_history
            WHERE id IN ({id_placeholders})
              AND status IN ({status_placeholders})
            """,
            [*chunk, *statuses],
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _load_rows_for_candidate_ids(conn: sqlite3.Connection, candidate_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ids = [int(row.get("id") or 0) for row in candidate_rows if int(row.get("id") or 0) > 0]
    if not ids:
        return []
    rows: list[dict[str, Any]] = []
    for start in range(0, len(ids), 900):
        chunk = ids[start:start + 900]
        placeholders = ",".join("?" for _ in chunk)
        select_cols = _task_history_select_columns(conn)
        fetched = conn.execute(
            f"""
            SELECT {select_cols}
            FROM task_history
            WHERE id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        rows.extend(_describe_task_row(row) for row in fetched)
    rows.sort(key=lambda item: int(item.get("id") or 0))
    return rows


def _validate_current_rows(
    *,
    expected_candidate_rows: Sequence[dict[str, Any]],
    current_rows: Sequence[dict[str, Any]],
    eligible_statuses: Sequence[str],
) -> list[dict[str, Any]]:
    expected_by_id = {int(row.get("id") or 0): dict(row) for row in expected_candidate_rows}
    current_by_id = {int(row.get("id") or 0): dict(row) for row in current_rows}
    errors: list[dict[str, Any]] = []
    statuses = set(_normalize_statuses(eligible_statuses))
    for row_id, expected in expected_by_id.items():
        current = current_by_id.get(row_id)
        if not current:
            errors.append({"id": row_id, "error": "row_missing"})
            continue
        if not expected.get("row_sha256"):
            errors.append({"id": row_id, "error": "complete_row_proof_missing"})
            continue
        for key in ("row_sha256", "task_name", "status", "started_at", "completed_at", "extra_data_sha256"):
            if str(current.get(key) or "") != str(expected.get(key) or ""):
                errors.append(
                    {
                        "id": row_id,
                        "error": "row_changed_since_plan",
                        "field": key,
                        "expected": expected.get(key),
                        "actual": current.get(key),
                    }
                )
                break
        else:
            if int(current.get("extra_data_bytes") or 0) != int(expected.get("extra_data_bytes") or 0):
                errors.append(
                    {
                        "id": row_id,
                        "error": "row_changed_since_plan",
                        "field": "extra_data_bytes",
                        "expected": expected.get("extra_data_bytes"),
                        "actual": current.get("extra_data_bytes"),
                    }
                )
            elif str(current.get("status") or "") not in statuses:
                errors.append({"id": row_id, "error": "status_not_eligible", "status": current.get("status")})
            elif str(current.get("status") or "") in PROTECTED_STATUSES:
                errors.append({"id": row_id, "error": "protected_status", "status": current.get("status")})
    return errors


def execute_task_history_cleanup(
    *,
    db_path: str | Path,
    plan_payload: Optional[dict[str, Any]] = None,
    source_plan_artifact: Optional[str | Path] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    archive_dir: str | Path = DEFAULT_ARCHIVE_DIR,
    mode: str = "dry-run",
    confirm_apply: str | None = None,
    retention_days: int = 14,
    recent_keep_per_task_status: int = 20,
    eligible_statuses: Sequence[str] | None = DEFAULT_ELIGIBLE_STATUSES,
    max_sample_rows: int = 20,
    backup_file: str | Path | None = None,
    backup_dir: str | Path = "backend/data/backups",
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    compress_backup: bool = True,
    timestamp: str | None = None,
) -> Tuple[Path, Path, dict[str, Any]]:
    normalized_mode = _normalize_text(mode).lower() or "dry-run"
    if normalized_mode not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    resolved_db_path = _resolve_project_path(db_path)
    if source_plan_artifact:
        plan_payload = _load_json_payload(source_plan_artifact)
    if plan_payload is None:
        if normalized_mode == "apply":
            raise ValueError("apply requires source_plan_artifact or plan_payload")
        plan_payload = build_task_history_cleanup_plan_payload(
            db_path=resolved_db_path,
            retention_days=retention_days,
            recent_keep_per_task_status=recent_keep_per_task_status,
            eligible_statuses=eligible_statuses,
            max_sample_rows=max_sample_rows,
        )

    plan_generated_at = _normalize_text(
        plan_payload.get("plan_generated_at") or plan_payload.get("generated_at")
    )
    parameters = dict(plan_payload.get("parameters") or {})
    plan_candidate_rows = list(plan_payload.get("candidate_rows") or [])
    plan_protected_row_ids = [
        int(item)
        for item in (plan_payload.get("protected_row_ids") or [])
        if int(item or 0) > 0
    ]
    plan_fingerprint = _normalize_text(plan_payload.get("plan_fingerprint") or (plan_payload.get("summary") or {}).get("plan_fingerprint"))
    execution_payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": normalized_mode,
        "status": "dry-run" if normalized_mode == "dry-run" else "pending",
        "db_path": str(resolved_db_path),
        "source_plan_artifact": str(source_plan_artifact or ""),
        "plan_generated_at": plan_generated_at,
        "plan_fingerprint": plan_fingerprint,
        "parameters": parameters,
        "summary": {
            **dict(plan_payload.get("summary") or {}),
            "candidate_total": len(plan_candidate_rows),
            "protected_row_count": len(plan_protected_row_ids),
            "deleted_count": 0,
        },
        "candidate_rows": plan_candidate_rows,
        "candidate_samples": plan_candidate_rows[:max_sample_rows],
        "protected_row_ids": plan_protected_row_ids,
        "protected_row_samples": list(plan_payload.get("protected_row_samples") or [])[:max_sample_rows],
        "snapshot_watchdog_skipped_archive_summary": plan_payload.get("snapshot_watchdog_skipped_archive_summary") or {},
        "backup": {},
        "archive": {},
        "post_apply_consistency": {},
        "guard_errors": [],
        "findings": [],
    }
    json_path, md_path = write_task_history_cleanup_execution_artifacts(
        execution_payload,
        output_dir=output_dir,
        timestamp=timestamp,
    )

    if not plan_candidate_rows:
        execution_payload["status"] = "no_scope" if normalized_mode == "apply" else "dry-run"
        execution_payload["findings"].append("No task_history cleanup candidates.")
        write_task_history_cleanup_execution_artifacts(execution_payload, output_dir=output_dir, timestamp=json_path.stem.replace("task_history_cleanup_execution_", ""))
        return json_path, md_path, execution_payload

    if normalized_mode == "dry-run":
        execution_payload["status"] = "dry-run"
        execution_payload["findings"].append("dry-run only; database is not mutated.")
        json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
        return json_path, md_path, execution_payload

    if _normalize_text(confirm_apply) != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")

    current_plan = build_task_history_cleanup_plan_payload(
        db_path=resolved_db_path,
        generated_at=plan_generated_at,
        retention_days=int(parameters.get("retention_days") or retention_days),
        recent_keep_per_task_status=int(parameters.get("recent_keep_per_task_status") or recent_keep_per_task_status),
        eligible_statuses=parameters.get("eligible_statuses") or eligible_statuses,
        max_sample_rows=max_sample_rows,
    )
    current_fingerprint = _normalize_text(current_plan.get("plan_fingerprint"))
    if current_fingerprint != plan_fingerprint:
        execution_payload["status"] = "blocked"
        execution_payload["guard_errors"].append(
            {
                "code": "candidate_plan_fingerprint_mismatch",
                "expected": plan_fingerprint,
                "actual": current_fingerprint,
            }
        )
        execution_payload["findings"].append("apply blocked because current candidates differ from the dry-run plan.")
        json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
        raise RuntimeError("candidate plan fingerprint mismatch")

    execution_payload["backup"] = _prepare_backup(
        db_path=resolved_db_path,
        backup_file=backup_file,
        backup_dir=backup_dir,
        output_dir=output_dir,
        backup_prefix=backup_prefix,
        compress_backup=compress_backup,
    )

    conn = _connect_sqlite(resolved_db_path, readonly=False)
    try:
        # Lock before reading/archiving, so a concurrent finalizer cannot revise
        # a row after its recovery copy was written but before it is deleted.
        conn.execute("BEGIN IMMEDIATE")
        current_rows = _load_rows_for_candidate_ids(conn, plan_candidate_rows)
        validation_errors = _validate_current_rows(
            expected_candidate_rows=plan_candidate_rows,
            current_rows=current_rows,
            eligible_statuses=parameters.get("eligible_statuses") or eligible_statuses,
        )
        if validation_errors:
            execution_payload["status"] = "blocked"
            execution_payload["guard_errors"].extend(validation_errors[:50])
            execution_payload["findings"].append("apply blocked because candidate rows changed before archive/delete.")
            json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
            raise RuntimeError("candidate row validation failed")

        execution_payload["archive"] = _archive_rows(
            current_rows,
            archive_dir=archive_dir,
            plan_fingerprint=plan_fingerprint,
            timestamp=timestamp,
        )
        execution_payload["archive"]["full_value_verification"] = _verify_archived_task_rows(
            execution_payload["archive"], current_rows)
        deleted_count = _delete_ids(
            conn,
            [int(row.get("id") or 0) for row in plan_candidate_rows],
            parameters.get("eligible_statuses") or eligible_statuses,
        )
        remaining_candidate_count = _count_existing_ids(
            conn,
            [int(row.get("id") or 0) for row in plan_candidate_rows],
        )
        protected_existing_count = _count_existing_ids(conn, plan_protected_row_ids)
        protected_missing_count = len(plan_protected_row_ids) - protected_existing_count
        post_status = (
            "ok"
            if deleted_count == len(plan_candidate_rows)
            and remaining_candidate_count == 0
            and protected_missing_count == 0
            else "failed"
        )
        execution_payload["post_apply_consistency"] = {
            "status": post_status,
            "expected_delete_count": len(plan_candidate_rows),
            "deleted_count": deleted_count,
            "remaining_candidate_count": remaining_candidate_count,
            "protected_row_count": len(plan_protected_row_ids),
            "protected_existing_count": protected_existing_count,
            "protected_missing_count": protected_missing_count,
            "archive_exists": Path(execution_payload["archive"]["rows_archive_absolute_path"]).exists(),
            "snapshot_watchdog_skipped_summary_exists": Path(
                execution_payload["archive"]["snapshot_watchdog_skipped_summary_absolute_path"]
            ).exists(),
        }
        if post_status != "ok":
            conn.rollback()
            execution_payload["status"] = "blocked"
            execution_payload["findings"].append("delete transaction rolled back because post consistency failed.")
            json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
            raise RuntimeError("post apply consistency failed")
        execution_payload["post_apply_consistency"]["archive_verified_before_commit"] = _verify_archived_task_rows(
            execution_payload["archive"], current_rows)
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        execution_payload["status"] = "blocked" if isinstance(exc, (ValueError, RuntimeError)) else "failed"
        execution_payload["transaction_rolled_back"] = True
        execution_payload["guard_errors"].append({"code": "task_history_cleanup_transaction_failed",
                                                "detail": f"{type(exc).__name__}: {exc}"})
        json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
        raise
    finally:
        conn.close()

    execution_payload["summary"]["deleted_count"] = int(
        execution_payload["post_apply_consistency"].get("deleted_count") or 0
    )
    execution_payload["status"] = "success"
    execution_payload["findings"].append(
        f"task_history cleanup apply succeeded: deleted={execution_payload['summary']['deleted_count']}."
    )
    json_path.write_text(json.dumps(execution_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_task_history_cleanup_execution_markdown(execution_payload), encoding="utf-8")
    return json_path, md_path, execution_payload
