#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bounded whole-file rotation for the screening-history active archive.

Rotation never rewrites business rows.  It checkpoints the active SQLite
shard, verifies it, atomically renames it inside the archive directory, then
advances the shard manifest.  If the manifest update fails, the rename is
rolled back before returning a failed result.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import closing
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from utils.screening_history_archive_manifest import (
    MANIFEST_SCHEMA_VERSION,
    ensure_screening_archive_manifest,
    load_screening_archive_manifest,
    resolve_archive_path,
    resolve_manifest_path,
    write_screening_archive_manifest,
)
from utils.screening_history_archive_preview import ARCHIVE_TABLES, DEFAULT_ARCHIVE_DB_PATH


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/screening_history_archive_rotation"
DEFAULT_MAX_ACTIVE_SIZE_BYTES = 8 * 1024 * 1024 * 1024
ALLOWED_MODES = {"dry-run", "auto", "rotate"}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_mode(value: Any) -> str:
    mode = _normalize_text(value).lower().replace("_", "-") or "dry-run"
    if mode not in ALLOWED_MODES:
        raise ValueError(f"unsupported screening archive rotation mode: {value}")
    return mode


def _sqlite_readonly_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()))}?mode=ro"


def _safe_table_name(value: str) -> str:
    text = _normalize_text(value)
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not text or any(ch not in safe_chars for ch in text):
        raise ValueError(f"unsafe table name: {value}")
    return text


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_safe_table_name(table_name),),
        ).fetchone()
        is not None
    )


def _quick_check(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)) as conn:
            conn.execute("PRAGMA busy_timeout=120000")
            row = conn.execute("PRAGMA quick_check").fetchone()
        return _normalize_text(row[0] if row else "")
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _readonly_probe(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with closing(sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)) as conn:
            conn.execute("PRAGMA busy_timeout=120000")
            row = conn.execute("SELECT 1").fetchone()
        return "ok" if row and _normalize_int(row[0]) == 1 else "unexpected_result"
    except sqlite3.Error as exc:
        return f"error:{exc}"


def _sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _inspect_active_shard(
    path: Path,
    *,
    include_table_counts: bool,
    include_quick_check: bool,
) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    result: dict[str, Any] = {
        "exists": exists,
        "path": str(path),
        "file_size_bytes": path.stat().st_size if exists else 0,
        "file_mtime_ns": path.stat().st_mtime_ns if exists else 0,
        "readonly_probe": _readonly_probe(path) if exists else "",
        "quick_check": _quick_check(path) if exists and include_quick_check else "not_run",
        "screening_run_count": 0,
        "min_started_at": "",
        "max_started_at": "",
        "table_row_counts": {},
    }
    if not exists:
        return result
    with closing(sqlite3.connect(_sqlite_readonly_uri(path), uri=True, timeout=120)) as conn:
        conn.execute("PRAGMA busy_timeout=120000")
        if _table_exists(conn, "screening_runs"):
            row = conn.execute(
                "SELECT COUNT(*), MIN(started_at), MAX(started_at) FROM screening_runs"
            ).fetchone()
            result["screening_run_count"] = _normalize_int(row[0] if row else 0)
            result["min_started_at"] = _normalize_text(row[1] if row else "")
            result["max_started_at"] = _normalize_text(row[2] if row else "")
        if include_table_counts:
            for table_name in ARCHIVE_TABLES:
                result["table_row_counts"][table_name] = (
                    _normalize_int(
                        conn.execute(
                            f'SELECT COUNT(*) FROM "{_safe_table_name(table_name)}"'
                        ).fetchone()[0]
                    )
                    if _table_exists(conn, table_name)
                    else 0
                )
    return result


def _date_token(value: Any, default: str) -> str:
    text = _normalize_text(value)
    digits = "".join(ch for ch in text[:10] if ch.isdigit())
    return digits if len(digits) == 8 else default


def _checkpoint_active(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(path), timeout=120, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        values = tuple(row or (0, 0, 0))
        busy = _normalize_int(values[0] if len(values) > 0 else 0)
        return {
            "status": "ok" if busy == 0 else "blocked",
            "busy": busy,
            "log_frames": _normalize_int(values[1] if len(values) > 1 else 0),
            "checkpointed_frames": _normalize_int(values[2] if len(values) > 2 else 0),
        }
    finally:
        conn.close()


def _sidecar_paths(path: Path) -> list[Path]:
    return [Path(str(path) + "-wal"), Path(str(path) + "-shm")]


def _build_findings(summary: dict[str, Any]) -> list[str]:
    status = _normalize_text(summary.get("status"))
    if status == "blocked":
        return [
            "screening history archive rotation 被安全门禁阻断；活动分片和 manifest 均未切换。",
            f"阻断原因：{summary.get('error') or 'unknown_error'}",
        ]
    if status.startswith("skipped"):
        return [
            "活动 screening 冷库尚未达到轮转条件；本轮未改名、未移动、未删除任何归档数据。"
        ]
    if status == "dry-run":
        return [
            "rotation dry-run 仅核验活动分片与阈值，不改名、不更新 manifest。"
        ]
    return [
        "活动 screening 冷库已通过 checkpoint/quick_check 后原子轮转为不可变 SQLite 分片。",
        "新的空活动 SQLite 已在原路径创建；cleanup guard 会跨 manifest 全部分片求和核验。",
    ]


def build_screening_history_archive_rotation_payload(
    *,
    active_archive_path: str | Path = DEFAULT_ARCHIVE_DB_PATH,
    manifest_path: str | Path | None = None,
    project_root: str | Path | None = None,
    mode: str = "dry-run",
    max_active_size_bytes: int = DEFAULT_MAX_ACTIVE_SIZE_BYTES,
    min_active_run_count: int = 1,
    confirm_rotate: bool = False,
    compute_sha256: bool = True,
    timestamp: Optional[str] = None,
) -> dict[str, Any]:
    root = Path(project_root).expanduser().resolve() if project_root else PROJECT_ROOT
    active = resolve_archive_path(active_archive_path, project_root=root)
    resolved_manifest = resolve_manifest_path(
        active,
        manifest_path=manifest_path,
        project_root=root,
    )
    normalized_mode = _normalize_mode(mode)
    threshold = max(1, int(max_active_size_bytes or DEFAULT_MAX_ACTIVE_SIZE_BYTES))
    min_runs = max(1, int(min_active_run_count or 1))
    generated_at = datetime.now().isoformat()
    inspection = _inspect_active_shard(
        active,
        include_table_counts=False,
        include_quick_check=False,
    )
    manifest_state = load_screening_archive_manifest(
        active,
        manifest_path=resolved_manifest,
        project_root=root,
    )
    blockers: list[dict[str, Any]] = []
    if manifest_state.get("status") == "invalid":
        blockers.append(
            {
                "code": "archive_manifest_invalid",
                "detail": "; ".join(manifest_state.get("errors") or ["unknown_error"]),
            }
        )
    if inspection.get("exists") and inspection.get("readonly_probe") != "ok":
        blockers.append(
            {
                "code": "active_shard_readonly_probe_failed",
                "detail": f"readonly_probe={inspection.get('readonly_probe')}",
            }
        )

    size_ready = _normalize_int(inspection.get("file_size_bytes")) >= threshold
    runs_ready = _normalize_int(inspection.get("screening_run_count")) >= min_runs
    should_rotate = bool(inspection.get("exists")) and size_ready and runs_ready
    execute_rotation = normalized_mode == "rotate" or (
        normalized_mode == "auto" and should_rotate
    )
    status = "dry-run" if normalized_mode == "dry-run" else "skipped_below_threshold"
    error = ""
    rotation_result: dict[str, Any] = {}

    if not inspection.get("exists"):
        status = "skipped_no_active_shard"
    elif blockers:
        status = "blocked"
        error = str(blockers[0].get("code") or "rotation_precheck_failed")
    elif normalized_mode == "rotate" and not confirm_rotate:
        status = "blocked"
        error = "explicit_rotation_requires_confirm_rotate"
        blockers.append(
            {
                "code": error,
                "detail": "mode=rotate requires confirm_rotate=true",
            }
        )
    elif normalized_mode == "auto" and not should_rotate:
        status = "skipped_below_threshold"
    elif execute_rotation:
        checkpoint = _checkpoint_active(active)
        rotation_result["checkpoint"] = checkpoint
        if checkpoint.get("status") != "ok":
            status = "blocked"
            error = "active_shard_checkpoint_busy"
            blockers.append({"code": error, "detail": json.dumps(checkpoint, sort_keys=True)})
        elif _quick_check(active) != "ok":
            status = "blocked"
            error = "active_shard_post_checkpoint_quick_check_failed"
            blockers.append({"code": error, "detail": str(active)})
        else:
            sealed_inspection = _inspect_active_shard(
                active,
                include_table_counts=True,
                include_quick_check=False,
            )
            sha256 = _sha256_file(active) if compute_sha256 else ""
            ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
            min_token = _date_token(sealed_inspection.get("min_started_at"), "unknown")
            max_token = _date_token(sealed_inspection.get("max_started_at"), "unknown")
            immutable_name = (
                f"screening_history_details_{min_token}_{max_token}_{ts}.db"
            )
            immutable_path = (active.parent / immutable_name).resolve()
            if immutable_path.exists():
                status = "blocked"
                error = f"immutable_target_exists:{immutable_path}"
                blockers.append({"code": "immutable_target_exists", "detail": str(immutable_path)})
            else:
                manifest_file, manifest_payload, _created = ensure_screening_archive_manifest(
                    active,
                    manifest_path=resolved_manifest,
                    project_root=root,
                )
                moved_pairs: list[tuple[Path, Path]] = []
                created_new_active = False
                try:
                    active.replace(immutable_path)
                    moved_pairs.append((immutable_path, active))
                    for old_sidecar in _sidecar_paths(active):
                        if not old_sidecar.exists():
                            continue
                        new_sidecar = Path(str(immutable_path) + old_sidecar.name[len(active.name):])
                        old_sidecar.replace(new_sidecar)
                        moved_pairs.append((new_sidecar, old_sidecar))

                    with closing(sqlite3.connect(str(active), timeout=120)) as new_active_conn:
                        new_active_conn.execute("PRAGMA journal_mode=DELETE")
                        new_active_conn.execute("PRAGMA user_version=1")
                        new_active_conn.commit()
                    created_new_active = True

                    updated = dict(manifest_payload)
                    immutable_entries = list(updated.get("immutable_shards") or [])
                    immutable_entries.append(
                        {
                            "id": f"screening-{ts}",
                            "path": immutable_path.relative_to(manifest_file.parent).as_posix(),
                            "format": "sqlite",
                            "state": "immutable",
                            "sealed_at": datetime.now().isoformat(),
                            "file_size_bytes": _normalize_int(
                                sealed_inspection.get("file_size_bytes")
                            ),
                            "file_mtime_ns": immutable_path.stat().st_mtime_ns,
                            "sha256": sha256,
                            "quick_check": "ok",
                            "quick_check_verified_at": datetime.now().isoformat(),
                            "quick_check_verified_size_bytes": immutable_path.stat().st_size,
                            "quick_check_verified_mtime_ns": immutable_path.stat().st_mtime_ns,
                            "screening_run_count": _normalize_int(
                                sealed_inspection.get("screening_run_count")
                            ),
                            "min_started_at": sealed_inspection.get("min_started_at") or "",
                            "max_started_at": sealed_inspection.get("max_started_at") or "",
                            "table_row_counts": dict(
                                sealed_inspection.get("table_row_counts") or {}
                            ),
                        }
                    )
                    updated["immutable_shards"] = immutable_entries
                    active_entry = dict(updated.get("active_shard") or {})
                    active_entry.update(
                        {
                            "id": _normalize_text(active_entry.get("id")) or "active",
                            "path": active.name,
                            "format": "sqlite",
                            "state": "writable",
                            "file_size_bytes": active.stat().st_size,
                            "file_mtime_ns": active.stat().st_mtime_ns,
                            "last_rotated_at": datetime.now().isoformat(),
                        }
                    )
                    updated["active_shard"] = active_entry
                    updated["schema_version"] = MANIFEST_SCHEMA_VERSION
                    updated["generation"] = max(
                        1, _normalize_int(updated.get("generation"), 1)
                    ) + 1
                    updated["updated_at"] = datetime.now().isoformat()
                    write_screening_archive_manifest(manifest_file, updated)
                    status = "rotated"
                    rotation_result.update(
                        {
                            "immutable_shard_path": str(immutable_path),
                            "sha256": sha256,
                            "manifest_generation": updated["generation"],
                            "sealed_inspection": sealed_inspection,
                        }
                    )
                    moved_pairs.clear()
                except Exception as exc:
                    if created_new_active and active.exists():
                        active.unlink()
                    for current_path, rollback_path in reversed(moved_pairs):
                        if current_path.exists() and not rollback_path.exists():
                            current_path.replace(rollback_path)
                    status = "blocked"
                    error = f"rotation_apply_failed:{type(exc).__name__}:{exc}"
                    blockers.append({"code": "rotation_apply_failed", "detail": error})

    final_manifest_state = load_screening_archive_manifest(
        active,
        manifest_path=resolved_manifest,
        project_root=root,
    )
    summary = {
        "status": status,
        "mode": normalized_mode,
        "error": error,
        "active_archive_path": str(active),
        "manifest_path": str(resolved_manifest),
        "manifest_status": final_manifest_state.get("status") or "",
        "manifest_generation": _normalize_int(final_manifest_state.get("generation")),
        "active_exists": bool(inspection.get("exists")),
        "active_size_bytes": _normalize_int(inspection.get("file_size_bytes")),
        "active_screening_run_count": _normalize_int(inspection.get("screening_run_count")),
        "max_active_size_bytes": threshold,
        "min_active_run_count": min_runs,
        "size_threshold_reached": size_ready,
        "run_threshold_reached": runs_ready,
        "should_rotate": should_rotate,
        "compute_sha256": bool(compute_sha256),
        "immutable_shard_count": sum(
            1
            for shard in final_manifest_state.get("shards") or []
            if shard.get("role") == "immutable"
        ),
    }
    payload = {
        "generated_at": generated_at,
        "status": status,
        "mode": normalized_mode,
        "summary": summary,
        "active_inspection": inspection,
        "rotation_result": rotation_result,
        "manifest": {
            "status": final_manifest_state.get("status") or "",
            "path": final_manifest_state.get("manifest_path") or "",
            "generation": _normalize_int(final_manifest_state.get("generation")),
            "errors": list(final_manifest_state.get("errors") or []),
            "shards": list(final_manifest_state.get("shards") or []),
        },
        "blockers": blockers,
        "guardrails": [
            "rotation 不复制、不删除业务行，只在同一 archive 目录内原子改名活动 SQLite 文件。",
            "checkpoint busy、quick_check 非 ok、manifest 越界/损坏或目标重名均 fail-closed。",
            "dry-run/未达阈值只做轻量只读探针；只有确定轮转后才执行全库 quick_check。",
            "显式 mode=rotate 必须 confirm_rotate=true；自动模式只在配置阈值达到时执行。",
            "轮转后的不可变分片仍保留为可直接查询的 SQLite；压缩需另行建立可验证索引后再实施。",
        ],
    }
    payload["findings"] = _build_findings(summary)
    return payload


def render_screening_history_archive_rotation_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening history archive rotation",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {payload.get('status')}",
        f"- mode: {payload.get('mode')}",
        f"- active_archive_path: {summary.get('active_archive_path', '')}",
        f"- active_size_bytes: {summary.get('active_size_bytes', 0)}",
        f"- active_screening_run_count: {summary.get('active_screening_run_count', 0)}",
        f"- max_active_size_bytes: {summary.get('max_active_size_bytes', 0)}",
        f"- manifest_generation: {summary.get('manifest_generation', 0)}",
        "",
        "## findings",
    ]
    lines.extend(f"- {item}" for item in payload.get("findings") or [])
    lines.extend(["", "## blockers"])
    lines.extend(
        f"- {json.dumps(item, ensure_ascii=False, sort_keys=True)}"
        for item in payload.get("blockers") or []
    )
    lines.extend(["", "## guardrails"])
    lines.extend(f"- {item}" for item in payload.get("guardrails") or [])
    return "\n".join(lines).strip() + "\n"


def write_screening_history_archive_rotation_artifacts(
    payload: dict[str, Any],
    *,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    path = Path(output_dir).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = path / f"screening_history_archive_rotation_{ts}.json"
    md_path = path / f"screening_history_archive_rotation_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_screening_history_archive_rotation_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
