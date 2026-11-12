from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
from typing import Any, Optional, Tuple

from backend.scheduler_status_support import build_task_closure_summary
from backend.task_audit_storage import write_task_audit
from utils.screening_history_cleanup_backup import create_screening_history_cleanup_backup


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_THRESHOLD_BYTES = 262_144
DEFAULT_OUTPUT_DIR = "reports/audit/task_history_extra_data_sidecar"
DEFAULT_SIDECAR_DIR = "reports/.audit/task_history_extra_data"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_task_history_extra_data_sidecar"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_TASK_HISTORY_EXTRA_DATA_SIDECAR"
TASK_HISTORY_EXTRA_DATA_SIDECAR_JSON_PATTERN = "task_history_extra_data_sidecar_*.json"
COMPACT_SCHEMA_VERSION = 1
MAX_HISTORY_LOG_LINES = 12
MAX_HISTORY_LOG_LINE_CHARS = 2_000
MAX_HISTORY_TEXT_CHARS = 4_000
MAX_HISTORY_LIST_ITEMS = 10

TASK_NAME_TO_TASK_KEY = {
    "scheduled_daily_bond_refresh": "daily_refresh",
    "scheduled_intraday_bond_refresh": "intraday_refresh",
    "scheduled_weekly_bond_maintenance": "weekly_maintenance",
    "scheduled_snapshot_watchdog_refresh": "snapshot_watchdog",
}

DIRECT_SUMMARY_FIELDS = (
    "task_name",
    "trigger_source",
    # One bounded observation, including its original evidence hashes and
    # failure/skip state. It must not be truncated like a heavy sample list.
    "archive_shadow_observation",
    "storage_observation",
    "status",
    "started_at",
    "finished_at",
    "business_started_at",
    "business_finished_at",
    "task_timing",
    "skip_reason",
    "screening_reason",
    "warning_reason",
    "degraded",
    "degraded_stage",
    "degradation_summary",
    "screening_triggered",
    "screening_run_id",
    "screening_final_count",
    "maintenance_rescreen_triggered",
    "maintenance_rescreen_run_id",
    "maintenance_rescreen_final_count",
    "report_refresh_triggered",
    "report_refresh_run_id",
    "initial_screening_triggered",
    "initial_screening_run_id",
    "initial_screening_final_count",
    "workflow_run_id",
    "status_detail",
    "publication_result",
    "collection_profile",
    "refresh_window",
    "collection_skipped",
    "records_processed",
    "records_inserted",
    "records_updated",
    "records_succeeded",
    "records_failed",
    "detail_records_processed",
    "detail_records_succeeded",
    "detail_records_failed",
    "detail_records_skipped",
    "detail_collection_summary",
    "detail_collection_summary_errors",
    "company_records_processed",
    "company_records_succeeded",
    "company_records_failed",
    "company_collection_enabled",
    "company_cooldown_count",
    "collection_stage_summaries",
    "manager_collection_summary",
    "manager_collection_summary_errors",
    "collection_enabled",
    "collection_skip_reason",
    "collection_error",
    "collection_degraded",
    "collection_degraded_stage",
    "collection_warning_reason",
    "collection_failures",
    "collection_rescreen_blocked_reason",
    "candidate_date",
    "signature",
    "effective_snapshot_changed_since_collection",
    "startup_catch_up_job_id",
    "startup_catch_up_suspected",
    "startup_catch_up_scheduler_started_at",
    "startup_catch_up_host_context",
    "host_context",
    "host_runtime_snapshot",
    "task_log_path",
    "task_run_audit",
    "task_run_audit_path",
    "task_run_audit_status",
    "task_run_audit_error",
    "runtime_status_summary_refresh_error",
    "error",
)

SNAPSHOT_FIELDS = (
    "before_effective_nav_snapshot",
    "after_effective_nav_snapshot",
    "latest_screened_snapshot",
)

SUMMARY_DICT_FIELDS = (
    "maintenance_data_change_summary",
    "holding_period_change_summary",
    "maintenance_change_items",
    "child_task_digest",
    "task_history_governance_preview",
    "task_history_governance_preview_error",
    "maintenance_backup_retention",
    "maintenance_backup_retention_error",
    "screening_storage_growth_audit",
    "screening_storage_growth_audit_error",
    "database_governance_health_audit",
    "database_governance_health_audit_error",
    "database_optimization_completion_audit",
    "database_optimization_completion_audit_error",
    "nav_history_consistency_audit",
    "nav_history_consistency_audit_error",
    "regression_smoke",
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_bytes(value: Any) -> bytes:
    return _json_dumps(value).encode("utf-8")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_task_name(value: Any) -> str:
    raw = str(value or "unknown").strip() or "unknown"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw)[:80] or "unknown"


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


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = _resolve_project_path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_output_paths(
    output_dir: str | Path,
    *,
    timestamp: str | None = None,
) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    return (
        artifact_dir / f"task_history_extra_data_sidecar_{ts}.json",
        artifact_dir / f"task_history_extra_data_sidecar_{ts}.md",
    )


def infer_scheduler_task_key(task_name: str | None) -> str:
    normalized = str(task_name or "").strip()
    return TASK_NAME_TO_TASK_KEY.get(normalized) or "unknown"


def _truncate_text(value: Any, *, max_chars: int = MAX_HISTORY_TEXT_CHARS) -> str:
    text = str(value or "")
    limit = max(1, int(max_chars or MAX_HISTORY_TEXT_CHARS))
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...[truncated_chars={len(text) - limit}]"


def _compact_logs_tail(value: Any, *, max_lines: int = MAX_HISTORY_LOG_LINES) -> list[str]:
    if not isinstance(value, list):
        return []
    limit = max(0, int(max_lines or 0))
    return [
        _truncate_text(item, max_chars=MAX_HISTORY_LOG_LINE_CHARS)
        for item in value[-limit:]
    ]


def _drop_heavy_sample_payload(value: Any) -> Any:
    if isinstance(value, dict):
        compact: dict[Any, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if "sample" in key.lower() and isinstance(raw_value, (list, dict)):
                continue
            if key in {"logs", "logs_tail"} and isinstance(raw_value, list):
                compact[raw_key] = _compact_logs_tail(raw_value)
                continue
            compact[raw_key] = _drop_heavy_sample_payload(raw_value)
        return compact
    if isinstance(value, list):
        return [_drop_heavy_sample_payload(item) for item in value[:MAX_HISTORY_LIST_ITEMS]]
    if isinstance(value, str):
        return _truncate_text(value)
    return value


def _task_history_id_from_task_info(task_info: dict[str, Any] | None) -> int | None:
    value = _normalize_int((task_info or {}).get("id"), default=0)
    return value or None


def write_task_history_extra_data_sidecar(
    full_payload: dict[str, Any],
    *,
    reports_dir: str | Path,
    task_info: dict[str, Any] | None = None,
    task_name: str | None = None,
    source: str = "scheduler_finalize",
    timestamp: str | None = None,
    original_extra_data_text: str | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    resolved_reports_dir = _resolve_project_path(reports_dir)
    sidecar_root = resolved_reports_dir / ".audit" / "task_history_extra_data" / datetime.now().strftime("%Y%m%d")
    sidecar_root.mkdir(parents=True, exist_ok=True)
    task_history_id = _task_history_id_from_task_info(task_info)
    normalized_task_name = str(task_name or (full_payload or {}).get("task_name") or "").strip() or "unknown"
    name_part = _safe_task_name(normalized_task_name)
    id_part = str(task_history_id or "noid")
    sidecar_path = sidecar_root / f"task_history_extra_data_{id_part}_{name_part}_{ts}.json.gz"

    full_json = _json_dumps(full_payload)
    payload = {
        "schema_version": COMPACT_SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": str(source or "unknown"),
        "task_history_id": task_history_id,
        "task_name": normalized_task_name,
        "full_payload_sha256": _sha256_text(full_json),
        "full_payload_bytes": len(full_json.encode("utf-8")),
        "full_payload": deepcopy(full_payload),
    }
    if original_extra_data_text is not None:
        original_text = str(original_extra_data_text)
        payload["original_extra_data_sha256"] = _sha256_text(original_text)
        payload["original_extra_data_bytes"] = len(original_text.encode("utf-8"))
    write_task_audit(sidecar_path, payload)
    return {
        "available": True,
        "sidecar_path": str(sidecar_path),
        "sidecar_path_relative": _project_relative(sidecar_path),
        "sidecar_project_path": _project_relative(sidecar_path),
        "sidecar_size_bytes": sidecar_path.stat().st_size,
        "full_payload_sha256": payload["full_payload_sha256"],
        "full_payload_bytes": payload["full_payload_bytes"],
        "original_extra_data_sha256": payload.get("original_extra_data_sha256"),
        "original_extra_data_bytes": payload.get("original_extra_data_bytes"),
        "source": str(source or "unknown"),
    }


def summarize_task_result_for_history(
    result: dict[str, Any],
    *,
    task_name: str | None = None,
    task_info: dict[str, Any] | None = None,
    sidecar_metadata: dict[str, Any] | None = None,
    full_payload_bytes: int | None = None,
    full_payload_sha256: str | None = None,
    max_logs_tail: int = 12,
) -> dict[str, Any]:
    payload = dict(result or {})
    normalized_task_name = str(task_name or payload.get("task_name") or "").strip() or "unknown"
    task_key = infer_scheduler_task_key(normalized_task_name)
    closure = build_task_closure_summary(
        task_key=task_key,
        task_name=normalized_task_name,
        result=payload,
    )
    logs_tail = payload.get("logs_tail") or closure.get("logs_tail") or []
    logs_tail = _compact_logs_tail(logs_tail, max_lines=max_logs_tail)

    compact: dict[str, Any] = {}
    compact.update(_drop_heavy_sample_payload(deepcopy(closure)))
    for key in DIRECT_SUMMARY_FIELDS:
        if key in payload and (payload.get(key) not in (None, "") or key.startswith("detail_")
                               or key in {"business_started_at", "business_finished_at", "task_timing"}):
            value = payload.get(key)
            if key == "error_traceback":
                continue
            compact[key] = deepcopy(value)
    for key in SNAPSHOT_FIELDS:
        if isinstance(payload.get(key), dict):
            compact[key] = deepcopy(payload.get(key) or {})
    for key in SUMMARY_DICT_FIELDS:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            compact[key] = _drop_heavy_sample_payload(deepcopy(value))
    if payload.get("error_traceback"):
        compact["error_traceback_tail"] = str(payload.get("error_traceback") or "")[-4000:]
    compact["logs_tail"] = deepcopy(logs_tail)

    sidecar = dict(sidecar_metadata or {})
    if not sidecar and payload.get("task_run_audit_path"):
        sidecar = {
            "available": True,
            "sidecar_path": str(payload.get("task_run_audit_path") or ""),
            "sidecar_path_relative": str(payload.get("task_run_audit_path") or ""),
            "sidecar_project_path": str(payload.get("task_run_audit_path") or ""),
            "source": "scheduler_task_run_audit",
        }

    compact["task_history_extra_data_compacted"] = True
    compact["task_history_extra_data_schema_version"] = COMPACT_SCHEMA_VERSION
    compact["task_history_extra_data_compacted_at"] = datetime.now().isoformat(timespec="seconds")
    compact["task_history_id"] = _task_history_id_from_task_info(task_info)
    compact["task_history_full_payload"] = {
        "available": bool(sidecar.get("available")),
        "sidecar_path": sidecar.get("sidecar_path"),
        "sidecar_path_relative": sidecar.get("sidecar_path_relative"),
        "sidecar_project_path": sidecar.get("sidecar_project_path"),
        "sidecar_size_bytes": sidecar.get("sidecar_size_bytes"),
        "full_payload_bytes": (
            int(full_payload_bytes)
            if full_payload_bytes is not None
            else sidecar.get("full_payload_bytes")
        ),
        "full_payload_sha256": full_payload_sha256 or sidecar.get("full_payload_sha256"),
        "original_extra_data_bytes": sidecar.get("original_extra_data_bytes"),
        "original_extra_data_sha256": sidecar.get("original_extra_data_sha256"),
        "source": sidecar.get("source"),
    }
    return compact


def compact_task_result_for_history_if_needed(
    result: dict[str, Any],
    *,
    task_name: str | None = None,
    task_info: dict[str, Any] | None = None,
    reports_dir: str | Path | None = None,
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
    force: bool = False,
    write_sidecar: bool = True,
    source: str = "scheduler_finalize",
    fail_open: bool = True,
) -> dict[str, Any]:
    payload = dict(result or {})
    if payload.get("task_history_extra_data_compacted"):
        return deepcopy(payload)
    full_json = _json_dumps(payload)
    full_bytes = len(full_json.encode("utf-8"))
    full_sha = _sha256_text(full_json)
    if not force and full_bytes < max(1, int(threshold_bytes or DEFAULT_THRESHOLD_BYTES)):
        return deepcopy(payload)

    sidecar_metadata: dict[str, Any] = {}
    if write_sidecar and reports_dir is not None:
        try:
            sidecar_metadata = write_task_history_extra_data_sidecar(
                payload,
                reports_dir=reports_dir,
                task_info=task_info,
                task_name=task_name,
                source=source,
            )
        except Exception as exc:
            if not fail_open:
                raise
            fallback = deepcopy(payload)
            fallback["task_history_extra_data_compaction_error"] = str(exc)
            return fallback

    return summarize_task_result_for_history(
        payload,
        task_name=task_name,
        task_info=task_info,
        sidecar_metadata=sidecar_metadata,
        full_payload_bytes=full_bytes,
        full_payload_sha256=full_sha,
    )


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    resolved = Path(db_path)
    conn = sqlite3.connect(f"file:{resolved.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_json_dict(raw_value: Any) -> dict[str, Any]:
    text = str(raw_value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _top_key_sizes(payload: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, value in (payload or {}).items():
        try:
            size = len(_json_bytes(value))
        except (TypeError, ValueError):
            size = len(str(value).encode("utf-8"))
        rows.append({"key": str(key), "type": type(value).__name__, "bytes": size})
    rows.sort(key=lambda item: int(item.get("bytes") or 0), reverse=True)
    return rows[: max(0, int(limit or 0))]


def build_task_history_extra_data_sidecar_preview_payload(
    *,
    db_path: str | Path,
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
    max_sample_rows: int = 20,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_text = generated_at or datetime.now().isoformat(timespec="seconds")
    resolved_db_path = _resolve_project_path(db_path)
    threshold = max(1, int(threshold_bytes or DEFAULT_THRESHOLD_BYTES))
    sample_limit = max(0, int(max_sample_rows or 0))
    if not resolved_db_path.exists():
        return {
            "generated_at": generated_text,
            "status": "missing",
            "preview_only": True,
            "db_path": str(resolved_db_path),
            "summary": {
                "table_exists": False,
                "candidate_total": 0,
                "threshold_bytes": threshold,
            },
            "candidate_rows": [],
            "samples": [],
            "findings": ["database missing"],
        }

    conn = _connect_readonly(resolved_db_path)
    try:
        if not _table_exists(conn, "task_history"):
            return {
                "generated_at": generated_text,
                "status": "missing",
                "preview_only": True,
                "db_path": str(resolved_db_path),
                "summary": {
                    "table_exists": False,
                    "candidate_total": 0,
                    "threshold_bytes": threshold,
                },
                "candidate_rows": [],
                "samples": [],
                "findings": ["task_history table missing"],
            }
        rows = conn.execute(
            """
            SELECT
                id,
                task_name,
                status,
                started_at,
                completed_at,
                length(COALESCE(extra_data, '')) AS extra_data_chars,
                length(CAST(COALESCE(extra_data, '') AS BLOB)) AS extra_data_bytes,
                extra_data
            FROM task_history
            WHERE length(CAST(COALESCE(extra_data, '') AS BLOB)) >= ?
            ORDER BY extra_data_bytes DESC, started_at DESC, id DESC
            """,
            (threshold,),
        ).fetchall()
    finally:
        conn.close()

    candidates: list[dict[str, Any]] = []
    invalid_json_rows: list[dict[str, Any]] = []
    already_compacted_rows: list[dict[str, Any]] = []
    task_status_counts: Counter[str] = Counter()
    candidate_bytes_total = 0
    compacted_bytes_total = 0
    for row in rows:
        raw_extra = str(row["extra_data"] or "")
        extra_bytes = int(row["extra_data_bytes"] or 0)
        parsed = _safe_json_dict(raw_extra)
        base_row = {
            "id": int(row["id"]),
            "task_name": str(row["task_name"] or "").strip(),
            "status": str(row["status"] or "").strip(),
            "started_at": str(row["started_at"] or "").strip(),
            "completed_at": str(row["completed_at"] or "").strip(),
            "extra_data_chars": int(row["extra_data_chars"] or 0),
            "extra_data_bytes": extra_bytes,
            "extra_data_sha256": _sha256_text(raw_extra),
        }
        if not parsed:
            invalid_json_rows.append(base_row)
            continue
        if parsed.get("task_history_extra_data_compacted"):
            already_compacted_rows.append(base_row)
            compacted_bytes_total += extra_bytes
            continue
        task_status_counts[f"{base_row['task_name']}|{base_row['status']}"] += 1
        base_row["top_key_sizes"] = _top_key_sizes(parsed)
        candidates.append(base_row)
        candidate_bytes_total += extra_bytes

    max_extra_data_bytes = max((int(row.get("extra_data_bytes") or 0) for row in candidates), default=0)
    summary = {
        "table_exists": True,
        "threshold_bytes": threshold,
        "candidate_total": len(candidates),
        "candidate_extra_data_bytes_total": candidate_bytes_total,
        "max_candidate_extra_data_bytes": max_extra_data_bytes,
        "invalid_json_row_count": len(invalid_json_rows),
        "already_compacted_row_count": len(already_compacted_rows),
        "already_compacted_bytes_total": compacted_bytes_total,
        "task_status_counts": dict(task_status_counts.most_common()),
        "preview_action": "no_database_mutation",
    }
    status = "review" if candidates else "ok"
    findings = [
        (
            f"task_history large extra_data candidates={len(candidates)}, "
            f"bytes={candidate_bytes_total}, threshold={threshold}."
        )
    ]
    if invalid_json_rows:
        findings.append(f"invalid JSON rows are skipped: count={len(invalid_json_rows)}.")
    if already_compacted_rows:
        findings.append(f"already compacted rows are skipped: count={len(already_compacted_rows)}.")
    return {
        "generated_at": generated_text,
        "status": status,
        "preview_only": True,
        "db_path": str(resolved_db_path),
        "summary": summary,
        "candidate_rows": candidates,
        "samples": candidates[:sample_limit],
        "invalid_json_samples": invalid_json_rows[:sample_limit],
        "already_compacted_samples": already_compacted_rows[:sample_limit],
        "execution_plan": {
            "preview_only": True,
            "delete_rows": False,
            "rewrite_extra_data": True,
            "sidecar_dir": DEFAULT_SIDECAR_DIR,
            "guardrails": [
                "apply requires explicit confirmation",
                "apply creates or binds a verified database backup first",
                "full original extra_data is written to sidecar JSON before rewriting the row",
                "task_history rows are not deleted and VACUUM is not executed",
            ],
        },
        "findings": findings,
    }


def render_task_history_extra_data_sidecar_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# task_history extra_data sidecar",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode', 'preview')}",
        f"- status: {payload.get('status')}",
        f"- db_path: {payload.get('db_path')}",
        f"- threshold_bytes: {summary.get('threshold_bytes')}",
        f"- candidate_total: {summary.get('candidate_total')}",
        f"- candidate_extra_data_bytes_total: {summary.get('candidate_extra_data_bytes_total')}",
        f"- updated_count: {summary.get('updated_count', 0)}",
        f"- remaining_candidate_total: {summary.get('remaining_candidate_total')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## task_status_counts"])
    for key, value in (summary.get("task_status_counts") or {}).items():
        lines.append(f"- {key}: {value}")
    samples = payload.get("samples") or []
    if samples:
        lines.extend(["", "## samples"])
        for row in samples[:20]:
            lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))
    return "\n".join(lines).strip() + "\n"


def write_task_history_extra_data_sidecar_artifacts(
    payload: dict[str, Any],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    timestamp: str | None = None,
) -> Tuple[Path, Path]:
    json_path, md_path = _resolve_output_paths(output_dir, timestamp=timestamp)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_task_history_extra_data_sidecar_markdown(payload), encoding="utf-8")
    return json_path, md_path


def _load_json_payload(path_value: str | Path) -> dict[str, Any]:
    path = _resolve_project_path(path_value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _prepare_backup(
    *,
    db_path: str | Path,
    backup_file: str | Path | None = None,
    backup_dir: str | Path,
    output_dir: str | Path,
    backup_prefix: str,
    compress_backup: bool,
) -> dict[str, Any]:
    if str(backup_file or "").strip():
        backup_path = _resolve_project_path(str(backup_file))
        if not backup_path.exists() or not backup_path.is_file():
            raise ValueError(f"backup_file does not exist: {backup_path}")
        summary: dict[str, Any] = {
            "status": "existing_backup_bound",
            "backup_artifact_path": _project_relative(backup_path),
            "backup_artifact_absolute_path": str(backup_path),
            "backup_artifact_size_bytes": backup_path.stat().st_size,
            "backup_artifact_format": "zstd_sqlite" if backup_path.name.endswith(".zst") else "sqlite",
            "zstd_test_succeeded": False,
            "sqlite_quick_check": "",
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
            conn = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True, timeout=30)
            try:
                row = conn.execute("PRAGMA quick_check").fetchone()
                quick_check = str(row[0] if row else "").strip()
            finally:
                conn.close()
            summary["sqlite_quick_check"] = quick_check
            if quick_check.lower() != "ok":
                raise RuntimeError(f"backup_file quick_check failed: {quick_check}")
        return summary

    _, _, backup_payload = create_screening_history_cleanup_backup(
        source_db_path=str(db_path),
        backup_dir=backup_dir,
        output_dir=output_dir,
        backup_prefix=backup_prefix,
        purpose="task_history_extra_data_sidecar",
        compress=bool(compress_backup),
        keep_uncompressed=False,
        compute_sha256=True,
        dry_run=False,
    )
    if backup_payload.get("status") != "success":
        raise RuntimeError(f"backup creation failed: {backup_payload.get('status')}")
    return dict(backup_payload.get("summary") or {})


def execute_task_history_extra_data_sidecarization(
    *,
    db_path: str | Path,
    reports_dir: str | Path = "reports",
    preview_payload: Optional[dict[str, Any]] = None,
    source_preview_artifact: Optional[str | Path] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
    backup_file: str | Path | None = None,
    backup_dir: str | Path = "backend/data/backups",
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    compress_backup: bool = True,
    commit_every: int = 20,
) -> Tuple[Path, Path, dict[str, Any]]:
    normalized_mode = str(mode or "dry-run").strip().lower()
    if normalized_mode not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    resolved_db_path = _resolve_project_path(db_path)
    if source_preview_artifact:
        preview_payload = _load_json_payload(source_preview_artifact)
    if preview_payload is None:
        preview_payload = build_task_history_extra_data_sidecar_preview_payload(
            db_path=resolved_db_path,
            threshold_bytes=threshold_bytes,
            max_sample_rows=20,
        )
    candidate_rows = list(preview_payload.get("candidate_rows") or [])
    summary = dict(preview_payload.get("summary") or {})
    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "db_path": str(resolved_db_path),
        "source_preview_artifact": str(source_preview_artifact or ""),
        "summary": {
            **summary,
            "updated_count": 0,
            "sidecar_written_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "compact_extra_data_bytes_total": 0,
            "remaining_candidate_total": None,
        },
        "backup": {},
        "samples": candidate_rows[:20],
        "updated_rows": [],
        "failed_rows": [],
        "findings": [],
    }
    json_path, md_path = write_task_history_extra_data_sidecar_artifacts(payload, output_dir)

    if not candidate_rows:
        payload["status"] = "no_scope" if normalized_mode == "apply" else "dry-run"
        payload["findings"].append("No task_history extra_data rows require sidecarization.")
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_task_history_extra_data_sidecar_markdown(payload), encoding="utf-8")
        return json_path, md_path, payload

    if normalized_mode == "dry-run":
        payload["status"] = "dry-run"
        payload["findings"].append("dry-run only; database is not mutated.")
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_task_history_extra_data_sidecar_markdown(payload), encoding="utf-8")
        return json_path, md_path, payload

    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")

    payload["backup"] = _prepare_backup(
        db_path=resolved_db_path,
        backup_file=backup_file,
        backup_dir=backup_dir,
        output_dir=output_dir,
        backup_prefix=backup_prefix,
        compress_backup=compress_backup,
    )
    payload["status"] = "running"
    payload["findings"].append("backup created and confirmation matched; starting guarded sidecarization.")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_task_history_extra_data_sidecar_markdown(payload), encoding="utf-8")

    expected_by_id = {int(row.get("id")): dict(row) for row in candidate_rows if row.get("id") is not None}
    conn = sqlite3.connect(str(resolved_db_path), timeout=120)
    conn.row_factory = sqlite3.Row
    processed_since_commit = 0
    try:
        for row_id, expected in expected_by_id.items():
            row = conn.execute(
                """
                SELECT id, task_name, status, started_at, completed_at, extra_data
                FROM task_history
                WHERE id = ?
                """,
                (row_id,),
            ).fetchone()
            if row is None:
                payload["summary"]["failed_count"] += 1
                payload["failed_rows"].append({"id": row_id, "error": "row_missing"})
                continue
            raw_extra = str(row["extra_data"] or "")
            raw_hash = _sha256_text(raw_extra)
            raw_bytes = len(raw_extra.encode("utf-8"))
            if raw_hash != expected.get("extra_data_sha256") or raw_bytes != int(expected.get("extra_data_bytes") or 0):
                payload["summary"]["failed_count"] += 1
                payload["failed_rows"].append({"id": row_id, "error": "row_changed_since_preview"})
                continue
            parsed = _safe_json_dict(raw_extra)
            if not parsed or parsed.get("task_history_extra_data_compacted"):
                payload["summary"]["skipped_count"] += 1
                continue
            task_info = {
                "id": row_id,
                "task_name": str(row["task_name"] or "").strip(),
                "started_at": str(row["started_at"] or "").strip(),
            }
            sidecar = write_task_history_extra_data_sidecar(
                parsed,
                reports_dir=reports_dir,
                task_info=task_info,
                task_name=task_info["task_name"],
                source="guarded_task_history_extra_data_sidecarization",
                original_extra_data_text=raw_extra,
            )
            compact = summarize_task_result_for_history(
                parsed,
                task_name=task_info["task_name"],
                task_info=task_info,
                sidecar_metadata=sidecar,
                full_payload_bytes=sidecar.get("full_payload_bytes"),
                full_payload_sha256=sidecar.get("full_payload_sha256"),
            )
            compact_json = json.dumps(compact, ensure_ascii=False)
            cursor = conn.execute(
                """
                UPDATE task_history
                SET extra_data = ?
                WHERE id = ? AND extra_data = ?
                """,
                (compact_json, row_id, raw_extra),
            )
            if cursor.rowcount != 1:
                payload["summary"]["failed_count"] += 1
                payload["failed_rows"].append({"id": row_id, "error": "guarded_update_failed"})
                continue
            payload["summary"]["updated_count"] += 1
            payload["summary"]["sidecar_written_count"] += 1
            payload["summary"]["compact_extra_data_bytes_total"] += len(compact_json.encode("utf-8"))
            if len(payload["updated_rows"]) < 100:
                payload["updated_rows"].append(
                    {
                        "id": row_id,
                        "task_name": task_info["task_name"],
                        "original_extra_data_bytes": raw_bytes,
                        "compact_extra_data_bytes": len(compact_json.encode("utf-8")),
                        "sidecar_path": sidecar.get("sidecar_project_path"),
                    }
                )
            processed_since_commit += 1
            if processed_since_commit >= max(1, int(commit_every or 20)):
                conn.commit()
                processed_since_commit = 0
        conn.commit()
    finally:
        conn.close()

    post_preview = build_task_history_extra_data_sidecar_preview_payload(
        db_path=resolved_db_path,
        threshold_bytes=threshold_bytes,
        max_sample_rows=20,
    )
    payload["summary"]["remaining_candidate_total"] = int(
        (post_preview.get("summary") or {}).get("candidate_total") or 0
    )
    original_bytes = int(summary.get("candidate_extra_data_bytes_total") or 0)
    compact_bytes = int(payload["summary"].get("compact_extra_data_bytes_total") or 0)
    payload["summary"]["estimated_extra_data_bytes_reduced"] = max(0, original_bytes - compact_bytes)
    payload["status"] = "success" if int(payload["summary"]["failed_count"] or 0) == 0 else "partial"
    payload["findings"].append(
        "sidecarization finished: "
        f"updated={payload['summary']['updated_count']}, "
        f"failed={payload['summary']['failed_count']}, "
        f"remaining={payload['summary']['remaining_candidate_total']}."
    )
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_task_history_extra_data_sidecar_markdown(payload), encoding="utf-8")
    return json_path, md_path, payload
