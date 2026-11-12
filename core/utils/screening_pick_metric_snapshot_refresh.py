#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guarded refresh for historical screening pick metric snapshots."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple

from database.connection import get_db_manager, resolve_db_path
from database.screening_pick_metric_snapshots import refresh_screening_pick_metric_snapshots
from utils.screening_history_cleanup_backup import create_screening_history_cleanup_backup
from utils.screening_storage_growth_audit import (
    LATEST_ACTIVITY_TABLE,
    PICK_METRIC_SNAPSHOT_TABLE,
    RUN_STORAGE_STATS_TABLE,
    fetch_count_by_run_ids,
    open_readonly_db,
    sqlite_table_exists,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/screening_pick_metric_snapshot_refresh"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_pick_metric_snapshot_refresh"
PICK_METRIC_SNAPSHOT_REFRESH_JSON_PATTERN = "screening_pick_metric_snapshot_refresh_*.json"
REQUIRED_APPLY_CONFIRMATION = "CONFIRM_SCREENING_PICK_METRIC_SNAPSHOT_REFRESH"
DEFAULT_REFRESH_CHUNK_SIZE = 50
DEFAULT_PICK_BATCH_SIZE = 1000
DEFAULT_CHECKPOINT_EVERY = 1


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
        artifact_dir / f"screening_pick_metric_snapshot_refresh_{ts}.json",
        artifact_dir / f"screening_pick_metric_snapshot_refresh_{ts}.md",
    )


def _normalize_mode(mode: str) -> str:
    value = str(mode or "dry-run").strip().lower()
    if value not in {"dry-run", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    return value


def _normalize_run_ids(run_ids: Optional[Sequence[str]]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in run_ids or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _iter_chunks(values: Sequence[str], chunk_size: int):
    size = max(1, int(chunk_size or DEFAULT_REFRESH_CHUNK_SIZE))
    for start in range(0, len(values), size):
        yield list(values[start:start + size])


def _sqlite_quick_check(path: Path) -> str:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0] if row else "").strip()
    finally:
        conn.close()


def _load_json_payload(path_value: str | Path) -> dict:
    path = _resolve_project_path(path_value)
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_pick_metric_snapshot_refresh_scope(
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Return run-level refresh candidates without scanning screening_picks details."""
    storage_exists = sqlite_table_exists(conn, RUN_STORAGE_STATS_TABLE)
    latest_exists = sqlite_table_exists(conn, LATEST_ACTIVITY_TABLE)
    metric_exists = sqlite_table_exists(conn, PICK_METRIC_SNAPSHOT_TABLE)

    storage_join = (
        f"LEFT JOIN {RUN_STORAGE_STATS_TABLE} s ON s.run_id = r.run_id"
        if storage_exists
        else ""
    )
    latest_join = (
        f"LEFT JOIN {LATEST_ACTIVITY_TABLE} a ON a.run_id = r.run_id"
        if latest_exists
        else ""
    )
    metric_join = (
        f"""
        LEFT JOIN (
            SELECT
                run_id,
                COUNT(*) AS metric_snapshot_count,
                MIN(refreshed_at) AS metric_first_refreshed_at,
                MAX(refreshed_at) AS metric_latest_refreshed_at
            FROM {PICK_METRIC_SNAPSHOT_TABLE}
            GROUP BY run_id
        ) ms ON ms.run_id = r.run_id
        """
        if metric_exists
        else ""
    )
    storage_select = (
        "s.run_id AS storage_run_id, s.pick_count AS storage_pick_count"
        if storage_exists
        else "NULL AS storage_run_id, NULL AS storage_pick_count"
    )
    latest_select = (
        "a.run_id AS latest_run_id, a.pick_count AS latest_pick_count"
        if latest_exists
        else "NULL AS latest_run_id, NULL AS latest_pick_count"
    )
    metric_select = (
        "ms.metric_snapshot_count, ms.metric_first_refreshed_at, ms.metric_latest_refreshed_at"
        if metric_exists
        else "NULL AS metric_snapshot_count, NULL AS metric_first_refreshed_at, NULL AS metric_latest_refreshed_at"
    )

    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
                r.run_id,
                r.started_at,
                r.finished_at,
                r.status,
                r.fund_type,
                r.period_years,
                {storage_select},
                {latest_select},
                {metric_select}
            FROM screening_runs r
            {storage_join}
            {latest_join}
            {metric_join}
            ORDER BY r.started_at DESC
            """
        ).fetchall()
    ]

    candidates: list[dict[str, Any]] = []
    stale_extra_rows: list[dict[str, Any]] = []
    baseline_missing_rows: list[dict[str, Any]] = []
    summary = {
        "run_count": len(rows),
        "storage_stats_exists": bool(storage_exists),
        "latest_activity_exists": bool(latest_exists),
        "metric_snapshot_table_exists": bool(metric_exists),
        "storage_stats_primary_run_count": 0,
        "latest_activity_fallback_run_count": 0,
        "baseline_missing_run_count": 0,
        "baseline_nonzero_pick_run_count": 0,
        "covered_nonzero_pick_run_count": 0,
        "refresh_candidate_count": 0,
        "missing_nonzero_run_count": 0,
        "mismatch_nonzero_run_count": 0,
        "stale_extra_run_count": 0,
        "baseline_pick_count_total": 0,
        "candidate_baseline_pick_count_total": 0,
        "candidate_existing_snapshot_count_total": 0,
        "existing_snapshot_count_total": 0,
        "expected_insert_count_total": 0,
        "expected_delete_count_total": 0,
        "expected_row_delta": 0,
    }

    for row in rows:
        storage_run_id = str(row.get("storage_run_id") or "").strip()
        latest_run_id = str(row.get("latest_run_id") or "").strip()
        if storage_run_id:
            baseline_source = RUN_STORAGE_STATS_TABLE
            baseline_pick_count = int(row.get("storage_pick_count") or 0)
            summary["storage_stats_primary_run_count"] += 1
        elif latest_run_id:
            baseline_source = LATEST_ACTIVITY_TABLE
            baseline_pick_count = int(row.get("latest_pick_count") or 0)
            summary["latest_activity_fallback_run_count"] += 1
        else:
            baseline_source = "missing"
            baseline_pick_count = 0
            summary["baseline_missing_run_count"] += 1
            baseline_missing_rows.append({
                **row,
                "baseline_source": baseline_source,
                "baseline_pick_count": baseline_pick_count,
                "metric_snapshot_count": int(row.get("metric_snapshot_count") or 0),
            })

        metric_snapshot_count = int(row.get("metric_snapshot_count") or 0)
        summary["baseline_pick_count_total"] += baseline_pick_count
        summary["existing_snapshot_count_total"] += metric_snapshot_count
        if baseline_pick_count > 0:
            summary["baseline_nonzero_pick_run_count"] += 1
            if metric_snapshot_count == baseline_pick_count:
                summary["covered_nonzero_pick_run_count"] += 1
                continue
            reason = "missing_snapshot" if metric_snapshot_count == 0 else "count_mismatch"
            if reason == "missing_snapshot":
                summary["missing_nonzero_run_count"] += 1
            else:
                summary["mismatch_nonzero_run_count"] += 1
            candidate = {
                "run_id": row.get("run_id"),
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "status": row.get("status"),
                "fund_type": row.get("fund_type"),
                "period_years": row.get("period_years"),
                "baseline_source": baseline_source,
                "baseline_pick_count": baseline_pick_count,
                "metric_snapshot_count": metric_snapshot_count,
                "missing_snapshot_count": max(0, baseline_pick_count - metric_snapshot_count),
                "refresh_reason": reason,
                "metric_latest_refreshed_at": row.get("metric_latest_refreshed_at"),
            }
            candidates.append(candidate)
            summary["candidate_baseline_pick_count_total"] += baseline_pick_count
            summary["candidate_existing_snapshot_count_total"] += metric_snapshot_count
        elif metric_snapshot_count > 0:
            stale_extra_rows.append({
                "run_id": row.get("run_id"),
                "started_at": row.get("started_at"),
                "status": row.get("status"),
                "fund_type": row.get("fund_type"),
                "period_years": row.get("period_years"),
                "baseline_source": baseline_source,
                "baseline_pick_count": baseline_pick_count,
                "metric_snapshot_count": metric_snapshot_count,
                "metric_latest_refreshed_at": row.get("metric_latest_refreshed_at"),
            })

    summary["refresh_candidate_count"] = len(candidates)
    summary["stale_extra_run_count"] = len(stale_extra_rows)
    summary["expected_insert_count_total"] = int(summary["candidate_baseline_pick_count_total"])
    summary["expected_delete_count_total"] = int(summary["candidate_existing_snapshot_count_total"])
    summary["expected_row_delta"] = (
        int(summary["expected_insert_count_total"])
        - int(summary["expected_delete_count_total"])
    )
    return {
        "summary": summary,
        "candidates": candidates,
        "stale_extra_rows": stale_extra_rows,
        "baseline_missing_rows": baseline_missing_rows,
    }


def build_pick_metric_snapshot_refresh_preview_payload(
    *,
    db_path: str | None = None,
    max_sample_rows: int = 20,
    include_detail_verification: bool = True,
) -> dict[str, Any]:
    resolved_db_path = resolve_db_path(db_path or "database/bond_funds.db")
    limit = max(0, int(max_sample_rows or 0))
    with open_readonly_db(resolved_db_path) as conn:
        scope = fetch_pick_metric_snapshot_refresh_scope(conn)
        candidates = list(scope.get("candidates") or [])
        detail_mismatch_rows: list[dict[str, Any]] = []
        detail_counts: dict[str, int] = {}
        if include_detail_verification and candidates:
            detail_counts = fetch_count_by_run_ids(
                conn,
                table_name="screening_picks",
                run_ids=[str(row.get("run_id") or "") for row in candidates],
            )
            for row in candidates:
                run_id = str(row.get("run_id") or "").strip()
                detail_pick_count = int(detail_counts.get(run_id, 0))
                row["detail_pick_count"] = detail_pick_count
                row["detail_count_matches_baseline"] = (
                    detail_pick_count == int(row.get("baseline_pick_count") or 0)
                )
                if not row["detail_count_matches_baseline"]:
                    detail_mismatch_rows.append(dict(row))

    summary = dict(scope.get("summary") or {})
    detail_verification = {
        "enabled": bool(include_detail_verification),
        "status": "skipped",
        "verified_run_count": 0,
        "detail_mismatch_run_count": 0,
        "detail_pick_count_total_for_candidates": 0,
    }
    if include_detail_verification:
        detail_total = sum(int(detail_counts.get(str(row.get("run_id") or ""), 0)) for row in candidates)
        mismatch_count = len(detail_mismatch_rows)
        detail_verification.update({
            "status": "matched" if mismatch_count == 0 else "mismatch",
            "verified_run_count": len(candidates),
            "detail_mismatch_run_count": mismatch_count,
            "detail_pick_count_total_for_candidates": detail_total,
        })
    summary["detail_verification"] = detail_verification

    if summary.get("baseline_missing_run_count"):
        status = "blocked"
    elif detail_verification.get("status") == "mismatch":
        status = "blocked"
    elif summary.get("refresh_candidate_count") or summary.get("stale_extra_run_count"):
        status = "review"
    else:
        status = "ok"
    summary["status"] = status

    findings: list[str] = []
    if summary.get("refresh_candidate_count"):
        findings.append(
            "screening_pick_metric_snapshots 存在可刷新候选，"
            f"run_count={summary.get('refresh_candidate_count')}，"
            f"expected_insert_count={summary.get('expected_insert_count_total')}。"
        )
    else:
        findings.append("screening_pick_metric_snapshots 已覆盖全部非零 pick run。")
    if summary.get("stale_extra_run_count"):
        findings.append(
            "存在 run 级基准已归零但 metric snapshot 仍残留的 run，"
            f"stale_extra_run_count={summary.get('stale_extra_run_count')}。"
        )
    if detail_verification.get("status") == "mismatch":
        findings.append(
            "候选 run 的 screening_picks 明细计数与 run 级基准不一致，"
            f"detail_mismatch_run_count={detail_verification.get('detail_mismatch_run_count')}；"
            "禁止 apply，需先复核 storage stats。"
        )
    elif include_detail_verification:
        findings.append("候选 run 的 screening_picks 明细计数与 run 级基准一致。")

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "mode": "preview",
        "db_path": resolved_db_path,
        "summary": summary,
        "execution_plan": {
            "candidate_run_ids": [str(row.get("run_id") or "") for row in candidates],
            "candidate_run_count": len(candidates),
            "recommended_refresh_chunk_size": DEFAULT_REFRESH_CHUNK_SIZE,
            "guardrails": [
                "只刷新 baseline_pick_count > 0 且 metric snapshot 缺失或计数不一致的 run",
                "apply 前必须有显式确认串和真实主库备份",
                "缺失业务值保持 NULL，不填伪 0",
                "不改变筛选门槛、风险指标公式或发布口径",
            ],
        },
        "samples": {
            "refresh_candidates": candidates[:limit],
            "detail_mismatches": detail_mismatch_rows[:limit],
            "stale_extra_rows": list(scope.get("stale_extra_rows") or [])[:limit],
            "baseline_missing_rows": list(scope.get("baseline_missing_rows") or [])[:limit],
        },
        "findings": findings,
    }


def render_pick_metric_snapshot_refresh_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening pick metric snapshot refresh",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- mode: {payload.get('mode')}",
        f"- status: {payload.get('status')}",
        f"- db_path: {payload.get('db_path')}",
        f"- refresh_candidate_count: {summary.get('refresh_candidate_count', 0)}",
        f"- expected_insert_count_total: {summary.get('expected_insert_count_total', 0)}",
        f"- expected_delete_count_total: {summary.get('expected_delete_count_total', 0)}",
        f"- stale_extra_run_count: {summary.get('stale_extra_run_count', 0)}",
        f"- detail_verification: {json.dumps(summary.get('detail_verification', {}), ensure_ascii=False)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## summary"])
    lines.append(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    samples = payload.get("samples") or {}
    if samples:
        lines.extend(["", "## samples"])
        for key, rows in samples.items():
            lines.append(f"### {key}")
            for row in rows or []:
                lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))
    return "\n".join(lines).strip() + "\n"


def write_pick_metric_snapshot_refresh_artifacts(
    payload: dict[str, Any],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    timestamp: str | None = None,
) -> tuple[Path, Path]:
    json_path, md_path = _resolve_output_paths(output_dir, timestamp=timestamp)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_pick_metric_snapshot_refresh_markdown(payload), encoding="utf-8")
    return json_path, md_path


def _persist_checkpoint(payload: dict[str, Any], json_path: Path, md_path: Path) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_pick_metric_snapshot_refresh_markdown(payload), encoding="utf-8")


def _resolve_backup_file(backup_file: str | Path | None) -> Path | None:
    if not str(backup_file or "").strip():
        return None
    return _resolve_project_path(str(backup_file))


def _prepare_backup(
    *,
    db_path: str,
    backup_file: str | Path | None,
    backup_dir: str | Path,
    output_dir: str | Path,
    auto_create_backup_if_missing: bool,
    backup_prefix: str,
    compress_backup: bool,
) -> dict[str, Any]:
    explicit_backup = _resolve_backup_file(backup_file)
    if explicit_backup is not None:
        if not explicit_backup.exists() or not explicit_backup.is_file():
            raise ValueError(f"backup_file does not exist: {explicit_backup}")
        backup_summary = {
            "status": "existing_backup_bound",
            "backup_artifact_path": _project_relative(explicit_backup),
            "backup_artifact_absolute_path": str(explicit_backup),
            "backup_artifact_size_bytes": explicit_backup.stat().st_size,
            "sqlite_quick_check": "",
        }
        if not explicit_backup.name.endswith(".zst"):
            backup_summary["sqlite_quick_check"] = _sqlite_quick_check(explicit_backup)
            if str(backup_summary["sqlite_quick_check"]).lower() != "ok":
                raise ValueError(
                    f"backup_file quick_check failed: {backup_summary['sqlite_quick_check']}"
                )
        return backup_summary

    if not auto_create_backup_if_missing:
        raise ValueError("apply requires backup_file or auto_create_backup_if_missing=true")

    _, _, backup_payload = create_screening_history_cleanup_backup(
        source_db_path=db_path,
        backup_dir=backup_dir,
        output_dir=output_dir,
        backup_prefix=backup_prefix,
        purpose="screening_pick_metric_snapshot_refresh",
        compress=bool(compress_backup),
        keep_uncompressed=False,
        compute_sha256=True,
        dry_run=False,
    )
    backup_summary = dict(backup_payload.get("summary") or {})
    if backup_payload.get("status") != "success":
        raise ValueError(f"backup creation failed: {backup_payload.get('status')}")
    return {
        "status": "backup_created",
        **backup_summary,
    }


def execute_pick_metric_snapshot_refresh(
    *,
    db_path: str | None = None,
    preview_payload: Optional[dict[str, Any]] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    mode: str = "dry-run",
    confirm_apply: Optional[str] = None,
    refresh_chunk_size: int = DEFAULT_REFRESH_CHUNK_SIZE,
    pick_batch_size: int = DEFAULT_PICK_BATCH_SIZE,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
    backup_file: str | Path | None = None,
    backup_dir: str | Path = "backend/data/backups",
    auto_create_backup_if_missing: bool = True,
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    compress_backup: bool = True,
    stats_source: str = "guarded_metric_snapshot_refresh",
) -> Tuple[Path, Path, dict[str, Any]]:
    normalized_mode = _normalize_mode(mode)
    resolved_db_path = resolve_db_path(db_path or "database/bond_funds.db")
    if source_preview_artifact:
        preview_payload = _load_json_payload(source_preview_artifact)
    if preview_payload is None or normalized_mode == "apply":
        preview_payload = build_pick_metric_snapshot_refresh_preview_payload(
            db_path=resolved_db_path,
            include_detail_verification=True,
        )
    else:
        preview_payload = dict(preview_payload or {})

    preview_summary = dict(preview_payload.get("summary") or {})
    detail_verification = dict(preview_summary.get("detail_verification") or {})
    candidate_run_ids = _normalize_run_ids(
        (preview_payload.get("execution_plan") or {}).get("candidate_run_ids") or []
    )
    json_path, md_path = _resolve_output_paths(output_dir)
    chunk_size = max(1, int(refresh_chunk_size or DEFAULT_REFRESH_CHUNK_SIZE))
    normalized_checkpoint_every = max(1, int(checkpoint_every or DEFAULT_CHECKPOINT_EVERY))
    batch_size = max(1, int(pick_batch_size or DEFAULT_PICK_BATCH_SIZE))

    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": normalized_mode,
        "status": "planned" if normalized_mode == "dry-run" else "pending",
        "db_path": resolved_db_path,
        "source_preview_artifact": str(source_preview_artifact or ""),
        "preview_summary": preview_summary,
        "parameters": {
            "refresh_chunk_size": chunk_size,
            "pick_batch_size": batch_size,
            "checkpoint_every": normalized_checkpoint_every,
            "backup_file": str(backup_file or ""),
            "backup_dir": str(backup_dir),
            "auto_create_backup_if_missing": bool(auto_create_backup_if_missing),
            "backup_prefix": str(backup_prefix or DEFAULT_BACKUP_PREFIX),
            "compress_backup": bool(compress_backup),
            "stats_source": str(stats_source or "guarded_metric_snapshot_refresh")[:40],
        },
        "backup": {},
        "summary": {
            "candidate_run_count": len(candidate_run_ids),
            "expected_insert_count_total": int(preview_summary.get("expected_insert_count_total") or 0),
            "expected_delete_count_total": int(preview_summary.get("expected_delete_count_total") or 0),
            "processed_run_count": 0,
            "deleted_count": 0,
            "inserted_count": 0,
            "failed_chunk_count": 0,
            "remaining_refresh_candidate_count": None,
            "elapsed_seconds": 0.0,
        },
        "chunks": [],
        "findings": [],
    }

    if not candidate_run_ids:
        payload["status"] = "no_scope" if normalized_mode == "apply" else "dry-run"
        payload["findings"].append("当前没有需要刷新的 screening_pick_metric_snapshots 候选。")
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    if detail_verification.get("status") != "matched":
        payload["status"] = "blocked"
        payload["findings"].append("候选 run 的 detail verification 未通过，禁止刷新。")
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    if normalized_mode == "dry-run":
        payload["status"] = "dry-run"
        payload["findings"].append("当前为 dry-run，只输出刷新计划，不写数据库。")
        _persist_checkpoint(payload, json_path, md_path)
        return json_path, md_path, payload

    if str(confirm_apply or "").strip() != REQUIRED_APPLY_CONFIRMATION:
        raise ValueError("confirm_apply mismatch")

    payload["backup"] = _prepare_backup(
        db_path=resolved_db_path,
        backup_file=backup_file,
        backup_dir=backup_dir,
        output_dir=output_dir,
        auto_create_backup_if_missing=bool(auto_create_backup_if_missing),
        backup_prefix=str(backup_prefix or DEFAULT_BACKUP_PREFIX),
        compress_backup=bool(compress_backup),
    )
    payload["findings"].append("已通过确认串、detail verification 和备份校验，可以进入分批刷新。")
    payload["status"] = "running"
    _persist_checkpoint(payload, json_path, md_path)

    manager = get_db_manager(resolved_db_path)
    started = time.monotonic()
    processed_since_checkpoint = 0
    for index, chunk in enumerate(_iter_chunks(candidate_run_ids, chunk_size), start=1):
        chunk_record: dict[str, Any] = {
            "chunk_index": index,
            "run_count": len(chunk),
            "run_ids": chunk,
            "status": "running",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": "",
            "deleted_count": 0,
            "inserted_count": 0,
            "error": "",
        }
        payload["chunks"].append(chunk_record)
        _persist_checkpoint(payload, json_path, md_path)
        try:
            result = refresh_screening_pick_metric_snapshots(
                db_manager=manager,
                run_ids=chunk,
                stats_source=str(stats_source or "guarded_metric_snapshot_refresh")[:40],
                chunk_size=batch_size,
            )
            chunk_record["deleted_count"] = int(result.get("deleted_count") or 0)
            chunk_record["inserted_count"] = int(result.get("inserted_count") or 0)
            chunk_record["status"] = "success"
            payload["summary"]["processed_run_count"] += len(chunk)
            payload["summary"]["deleted_count"] += int(result.get("deleted_count") or 0)
            payload["summary"]["inserted_count"] += int(result.get("inserted_count") or 0)
        except Exception as exc:  # pragma: no cover - exercised by integration failures
            chunk_record["status"] = "failed"
            chunk_record["error"] = str(exc)
            payload["summary"]["failed_chunk_count"] += 1
            payload["status"] = "failed"
            payload["findings"].append(f"刷新 chunk {index} 失败：{exc}")
            chunk_record["finished_at"] = datetime.now().isoformat(timespec="seconds")
            payload["summary"]["elapsed_seconds"] = round(time.monotonic() - started, 3)
            _persist_checkpoint(payload, json_path, md_path)
            return json_path, md_path, payload

        chunk_record["finished_at"] = datetime.now().isoformat(timespec="seconds")
        processed_since_checkpoint += 1
        if processed_since_checkpoint >= normalized_checkpoint_every:
            payload["summary"]["elapsed_seconds"] = round(time.monotonic() - started, 3)
            _persist_checkpoint(payload, json_path, md_path)
            processed_since_checkpoint = 0

    post_preview = build_pick_metric_snapshot_refresh_preview_payload(
        db_path=resolved_db_path,
        include_detail_verification=True,
    )
    post_summary = dict(post_preview.get("summary") or {})
    remaining = int(post_summary.get("refresh_candidate_count") or 0)
    payload["post_refresh_summary"] = post_summary
    payload["summary"]["remaining_refresh_candidate_count"] = remaining
    payload["summary"]["elapsed_seconds"] = round(time.monotonic() - started, 3)
    if remaining == 0 and int(payload["summary"].get("failed_chunk_count") or 0) == 0:
        payload["status"] = "success"
        payload["findings"].append("screening_pick_metric_snapshots guarded refresh 完成，候选已清零。")
    else:
        payload["status"] = "review"
        payload["findings"].append(
            f"screening_pick_metric_snapshots guarded refresh 后仍有候选 {remaining} 个。"
        )
    _persist_checkpoint(payload, json_path, md_path)
    return json_path, md_path, payload
