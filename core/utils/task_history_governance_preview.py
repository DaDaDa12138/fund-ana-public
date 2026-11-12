#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only task_history governance preview.

The preview identifies task_history retention and sidecarization candidates
without deleting rows or rewriting extra_data. It is intended for weekly
maintenance evidence, not for direct cleanup execution.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any, Optional, Tuple

from utils.fund_code_alias_audit import load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/task_history_governance"
TASK_HISTORY_GOVERNANCE_PREVIEW_JSON_PATTERN = "task_history_governance_preview_*.json"
SNAPSHOT_WATCHDOG_TASK_NAME = "scheduled_snapshot_watchdog_refresh"


def load_latest_task_history_governance_preview_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        TASK_HISTORY_GOVERNANCE_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def _resolve_generated_at(generated_at: str | None) -> datetime:
    parsed = _parse_datetime(generated_at)
    return parsed or datetime.now()


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    resolved = Path(db_path)
    uri = f"file:{resolved.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_json_dict(raw_value: Any) -> dict[str, Any]:
    text = _normalize_text(raw_value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _row_sort_key(row: dict[str, Any]) -> tuple[datetime, int]:
    parsed = row.get("_started_at_dt")
    if not isinstance(parsed, datetime):
        parsed = datetime.min
    return (parsed, _normalize_int(row.get("id")))


def _sample_rows(rows: list[dict[str, Any]], max_sample_rows: int) -> list[dict[str, Any]]:
    sample = []
    for row in rows[: max(0, int(max_sample_rows))]:
        sample.append(
            {
                "id": row.get("id"),
                "task_name": row.get("task_name"),
                "status": row.get("status"),
                "started_at": row.get("started_at"),
                "completed_at": row.get("completed_at"),
                "extra_data_bytes": row.get("extra_data_bytes"),
            }
        )
    return sample


def _build_snapshot_watchdog_skipped_summary(
    conn: sqlite3.Connection,
    *,
    max_daily_summary_days: int,
) -> dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                started_at,
                extra_data,
                length(CAST(COALESCE(extra_data, '') AS BLOB)) AS extra_data_bytes
            FROM task_history
            WHERE task_name = ? AND status = 'skipped'
            ORDER BY started_at DESC, id DESC
            """,
            (SNAPSHOT_WATCHDOG_TASK_NAME,),
        ).fetchall()
    except sqlite3.Error as exc:
        return {
            "available": False,
            "error": str(exc),
            "skipped_total": 0,
            "daily_summary": [],
            "skip_reason_counts": {},
        }

    day_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()
    per_day_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    latest_started_at = None
    max_extra_data_bytes = 0
    for row in rows:
        started_text = _normalize_text(row["started_at"])
        parsed = _parse_datetime(started_text)
        day_key = parsed.date().isoformat() if parsed else "unknown"
        payload = _safe_json_dict(row["extra_data"])
        reason = (
            _normalize_text(payload.get("skip_reason"))
            or _normalize_text(payload.get("snapshot_watchdog_reason"))
            or _normalize_text(payload.get("no_action_category"))
            or "unknown"
        )
        day_counter[day_key] += 1
        reason_counter[reason] += 1
        per_day_reasons[day_key][reason] += 1
        if latest_started_at is None and started_text:
            latest_started_at = started_text
        max_extra_data_bytes = max(max_extra_data_bytes, _normalize_int(row["extra_data_bytes"]))

    daily_summary = []
    for day_key in sorted(day_counter.keys(), reverse=True)[: max(1, int(max_daily_summary_days))]:
        daily_summary.append(
            {
                "date": day_key,
                "skipped_count": int(day_counter[day_key]),
                "skip_reason_counts": dict(per_day_reasons[day_key].most_common()),
            }
        )

    return {
        "available": True,
        "task_name": SNAPSHOT_WATCHDOG_TASK_NAME,
        "skipped_total": len(rows),
        "skipped_day_count": len(day_counter),
        "latest_started_at": latest_started_at,
        "max_extra_data_bytes": max_extra_data_bytes,
        "skip_reason_counts": dict(reason_counter.most_common()),
        "daily_summary": daily_summary,
        "daily_summary_scope": f"latest_{max(1, int(max_daily_summary_days))}_days",
    }


def _build_findings(summary: dict[str, Any], snapshot_watchdog_skipped: dict[str, Any]) -> list[str]:
    findings = [
        (
            f"task_history 当前共有 {summary.get('total_rows', 0)} 行；"
            f"retention preview 候选 {summary.get('retention_candidate_total', 0)} 行，"
            f"大 extra_data sidecar 候选 {summary.get('large_extra_data_candidate_total', 0)} 行。"
        )
    ]
    skipped_total = int(snapshot_watchdog_skipped.get("skipped_total") or 0)
    if skipped_total:
        top_reason = next(iter((snapshot_watchdog_skipped.get("skip_reason_counts") or {}).items()), None)
        reason_text = f"，主因 {top_reason[0]}={top_reason[1]}" if top_reason else ""
        findings.append(
            f"snapshot watchdog skipped 历史共有 {skipped_total} 条{reason_text}；"
            "后续可按日级摘要化保留最新窗口与失败样本。"
        )
    if int(summary.get("large_extra_data_candidate_total") or 0) > 0:
        findings.append(
            "存在大 extra_data 记录；建议后续采用 sidecar 化，数据库只保留摘要、sidecar path 与 hash。"
        )
    if int(summary.get("retention_candidate_total") or 0) == 0 and int(summary.get("large_extra_data_candidate_total") or 0) == 0:
        findings.append("当前 task_history 未发现需要治理的 retention 或 sidecar 候选。")
    return findings


def build_task_history_governance_preview_payload(
    *,
    db_path: str | Path,
    generated_at: str | None = None,
    retention_days: int = 14,
    recent_keep_per_task_status: int = 20,
    large_extra_data_threshold_bytes: int = 262_144,
    max_sample_rows: int = 20,
    max_daily_summary_days: int = 30,
) -> dict[str, Any]:
    resolved_db_path = Path(db_path)
    generated_dt = _resolve_generated_at(generated_at)
    generated_text = generated_dt.isoformat(timespec="seconds")
    retention_days = max(1, int(retention_days))
    recent_keep_per_task_status = max(1, int(recent_keep_per_task_status))
    large_extra_data_threshold_bytes = max(1, int(large_extra_data_threshold_bytes))
    max_sample_rows = max(1, int(max_sample_rows))
    max_daily_summary_days = max(1, int(max_daily_summary_days))
    cutoff_dt = generated_dt - timedelta(days=retention_days)

    if not resolved_db_path.exists():
        return {
            "generated_at": generated_text,
            "preview_only": True,
            "db_path": str(resolved_db_path),
            "status": "missing",
            "summary": {
                "status": "review",
                "table_exists": False,
                "database_exists": False,
                "total_rows": 0,
                "retention_candidate_total": 0,
                "large_extra_data_candidate_total": 0,
            },
            "findings": ["task_history 所在数据库不存在，无法生成治理 preview。"],
            "execution_plan": {
                "preview_only": True,
                "delete_rows": False,
                "rewrite_extra_data": False,
            },
        }

    conn = _connect_readonly(resolved_db_path)
    try:
        if not _table_exists(conn, "task_history"):
            return {
                "generated_at": generated_text,
                "preview_only": True,
                "db_path": str(resolved_db_path),
                "status": "missing",
                "summary": {
                    "status": "review",
                    "table_exists": False,
                    "database_exists": True,
                    "total_rows": 0,
                    "retention_candidate_total": 0,
                    "large_extra_data_candidate_total": 0,
                },
                "findings": ["数据库中不存在 task_history 表，无法生成任务历史治理 preview。"],
                "execution_plan": {
                    "preview_only": True,
                    "delete_rows": False,
                    "rewrite_extra_data": False,
                },
            }

        raw_rows = conn.execute(
            """
            SELECT
                id,
                task_name,
                status,
                started_at,
                completed_at,
                duration,
                records_processed,
                records_inserted,
                records_updated,
                records_failed,
                length(CAST(COALESCE(extra_data, '') AS BLOB)) AS extra_data_bytes
            FROM task_history
            ORDER BY started_at DESC, id DESC
            """
        ).fetchall()
        snapshot_watchdog_skipped = _build_snapshot_watchdog_skipped_summary(
            conn,
            max_daily_summary_days=max_daily_summary_days,
        )
    finally:
        conn.close()

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        started_at = _normalize_text(raw["started_at"])
        rows.append(
            {
                "id": _normalize_int(raw["id"]),
                "task_name": _normalize_text(raw["task_name"]) or "unknown",
                "status": _normalize_text(raw["status"]) or "unknown",
                "started_at": started_at,
                "completed_at": _normalize_text(raw["completed_at"]) or None,
                "duration": raw["duration"],
                "records_processed": raw["records_processed"],
                "records_inserted": raw["records_inserted"],
                "records_updated": raw["records_updated"],
                "records_failed": raw["records_failed"],
                "extra_data_bytes": _normalize_int(raw["extra_data_bytes"]),
                "_started_at_dt": _parse_datetime(started_at),
            }
        )

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["task_name"], row["status"])].append(row)

    protected_ids: set[int] = set()
    task_status_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        task_status_counts[row["task_name"]][row["status"]] += 1
    for group_rows in grouped.values():
        ordered = sorted(group_rows, key=_row_sort_key, reverse=True)
        for row in ordered[:recent_keep_per_task_status]:
            protected_ids.add(int(row["id"]))

    retention_candidates = []
    for row in rows:
        started_dt = row.get("_started_at_dt")
        if row["status"] == "running":
            continue
        if int(row["id"]) in protected_ids:
            continue
        if not isinstance(started_dt, datetime):
            continue
        if started_dt < cutoff_dt:
            retention_candidates.append(row)
    retention_candidates.sort(key=_row_sort_key)

    large_extra_data_candidates = [
        row
        for row in rows
        if int(row.get("extra_data_bytes") or 0) >= large_extra_data_threshold_bytes
    ]
    large_extra_data_candidates.sort(key=lambda item: int(item.get("extra_data_bytes") or 0), reverse=True)

    status_counts = Counter(row["status"] for row in rows)
    task_counts = Counter(row["task_name"] for row in rows)
    total_extra_data_bytes = sum(int(row.get("extra_data_bytes") or 0) for row in rows)
    candidate_extra_data_bytes = sum(int(row.get("extra_data_bytes") or 0) for row in retention_candidates)
    large_extra_data_bytes = sum(int(row.get("extra_data_bytes") or 0) for row in large_extra_data_candidates)
    preview_status = (
        "review"
        if retention_candidates or large_extra_data_candidates or int(snapshot_watchdog_skipped.get("skipped_total") or 0)
        else "ok"
    )
    summary = {
        "status": preview_status,
        "table_exists": True,
        "database_exists": True,
        "total_rows": len(rows),
        "status_counts": dict(status_counts.most_common()),
        "task_counts": dict(task_counts.most_common()),
        "task_status_counts": {task: dict(counts) for task, counts in task_status_counts.items()},
        "retention_days": retention_days,
        "retention_cutoff_at": cutoff_dt.isoformat(timespec="seconds"),
        "recent_keep_per_task_status": recent_keep_per_task_status,
        "retention_candidate_total": len(retention_candidates),
        "retention_candidate_extra_data_bytes": candidate_extra_data_bytes,
        "large_extra_data_threshold_bytes": large_extra_data_threshold_bytes,
        "large_extra_data_candidate_total": len(large_extra_data_candidates),
        "large_extra_data_total_bytes": large_extra_data_bytes,
        "max_extra_data_bytes": max((int(row.get("extra_data_bytes") or 0) for row in rows), default=0),
        "total_extra_data_bytes": total_extra_data_bytes,
        "snapshot_watchdog_skipped_total": int(snapshot_watchdog_skipped.get("skipped_total") or 0),
        "snapshot_watchdog_skipped_day_count": int(snapshot_watchdog_skipped.get("skipped_day_count") or 0),
        "preview_action": "no_database_mutation",
    }

    payload = {
        "generated_at": generated_text,
        "preview_only": True,
        "db_path": str(resolved_db_path),
        "status": "success",
        "parameters": {
            "retention_days": retention_days,
            "recent_keep_per_task_status": recent_keep_per_task_status,
            "large_extra_data_threshold_bytes": large_extra_data_threshold_bytes,
            "max_sample_rows": max_sample_rows,
            "max_daily_summary_days": max_daily_summary_days,
        },
        "summary": summary,
        "findings": _build_findings(summary, snapshot_watchdog_skipped),
        "snapshot_watchdog_skipped": snapshot_watchdog_skipped,
        "retention_candidate_samples": _sample_rows(retention_candidates, max_sample_rows),
        "large_extra_data_candidate_samples": _sample_rows(large_extra_data_candidates, max_sample_rows),
        "execution_plan": {
            "preview_only": True,
            "delete_rows": False,
            "rewrite_extra_data": False,
            "candidate_action": "future_guarded_cleanup_or_sidecarization",
            "target_table": "task_history",
            "sidecarization_note": (
                "大 extra_data 记录仅作为 sidecar 候选输出；本 preview 不写 sidecar，"
                "也不清空 task_history.extra_data。"
            ),
            "guardrails": [
                "running 状态记录不纳入 retention candidate。",
                "每个 task_name/status 至少保留最近 N 条记录。",
                "本 preview 只读数据库，不执行 DELETE、UPDATE 或 VACUUM。",
                "后续若执行 sidecar 化，数据库需保留摘要、sidecar path 与 hash，并保留审计产物。",
            ],
        },
    }
    return payload


def render_task_history_governance_preview_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    lines = [
        "# task_history governance preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- db_path: {payload.get('db_path')}",
        f"- status: {payload.get('status')}",
        f"- preview_only: {payload.get('preview_only')}",
        "",
        "## summary",
        f"- total_rows: {summary.get('total_rows', 0)}",
        f"- retention_days: {summary.get('retention_days', parameters.get('retention_days'))}",
        f"- retention_cutoff_at: {summary.get('retention_cutoff_at')}",
        f"- recent_keep_per_task_status: {summary.get('recent_keep_per_task_status')}",
        f"- retention_candidate_total: {summary.get('retention_candidate_total', 0)}",
        f"- large_extra_data_threshold_bytes: {summary.get('large_extra_data_threshold_bytes')}",
        f"- large_extra_data_candidate_total: {summary.get('large_extra_data_candidate_total', 0)}",
        f"- max_extra_data_bytes: {summary.get('max_extra_data_bytes', 0)}",
        f"- total_extra_data_bytes: {summary.get('total_extra_data_bytes', 0)}",
        f"- snapshot_watchdog_skipped_total: {summary.get('snapshot_watchdog_skipped_total', 0)}",
        f"- snapshot_watchdog_skipped_day_count: {summary.get('snapshot_watchdog_skipped_day_count', 0)}",
        f"- preview_action: {summary.get('preview_action')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## status_counts"])
    for key, value in (summary.get("status_counts") or {}).items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## snapshot_watchdog_skipped_daily_summary"])
    daily_summary = ((payload.get("snapshot_watchdog_skipped") or {}).get("daily_summary") or [])
    if not daily_summary:
        lines.append("- no skipped watchdog rows")
    else:
        for item in daily_summary[:30]:
            lines.append(
                "- "
                + json.dumps(
                    {
                        "date": item.get("date"),
                        "skipped_count": item.get("skipped_count"),
                        "skip_reason_counts": item.get("skip_reason_counts") or {},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## guardrails"])
    for item in (payload.get("execution_plan") or {}).get("guardrails") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_task_history_governance_preview_artifacts(
    payload: dict[str, Any],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    json_path = artifact_dir / f"task_history_governance_preview_{ts}.json"
    markdown_path = artifact_dir / f"task_history_governance_preview_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_task_history_governance_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path
