#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backup evidence for fund_rank_history hot delete windows."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.sqlite_backup_snapshot import create_verified_sqlite_backup


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/rank_history_archive"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_rank_archive"
BACKUP_EVIDENCE_JSON_PATTERN = "rank_history_archive_backup_evidence_*.json"


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


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


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json_md(
    *,
    payload: dict,
    output_dir: str | Path,
    prefix: str,
    markdown_title: str,
    project_root: Path = PROJECT_ROOT,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    ts = timestamp or _now_ts()
    out_dir = _resolve_project_path(output_dir, project_root=project_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{prefix}_{ts}.json"
    md_path = out_dir / f"{prefix}_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"# {markdown_title}",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- status: {payload.get('status', '')}",
    ]
    for key, value in _normalize_dict(payload.get("summary")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return json_path, md_path


def create_rank_history_archive_backup(
    *,
    source_db_path: str | Path,
    backup_dir: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    project_root: str | Path = PROJECT_ROOT,
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    timestamp: Optional[str] = None,
    compress: bool = True,
    keep_uncompressed: bool = False,
    compute_sha256: bool = True,
    dry_run: bool = False,
) -> tuple[Path, Path, dict]:
    """Create a whole-DB rollback point before rank hot delete."""
    root = Path(project_root).resolve()
    source_path = _resolve_project_path(source_db_path, project_root=root)
    resolved_backup_dir = _resolve_project_path(backup_dir, project_root=root)
    ts = timestamp or _now_ts()
    backup_label = f"{backup_prefix}_{ts}"
    db_backup_path = resolved_backup_dir / f"{backup_label}.db"
    planned_artifact_path = (
        db_backup_path.with_suffix(db_backup_path.suffix + ".zst")
        if compress
        else db_backup_path
    )
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "dry-run" if dry_run else "pending",
        "mode": "dry-run" if dry_run else "create",
        "summary": {
            "source_db_path": str(source_path),
            "source_db_exists": source_path.exists() and source_path.is_file(),
            "source_db_size_bytes": source_path.stat().st_size if source_path.exists() else 0,
            "backup_dir": str(resolved_backup_dir),
            "backup_label": backup_label,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_artifact_path": _project_relative(planned_artifact_path, project_root=root),
            "backup_artifact_format": "zstd_sqlite" if compress else "sqlite",
            "compress": bool(compress),
            "keep_uncompressed": bool(keep_uncompressed),
            "created": False,
            "sqlite_quick_check": "",
            "backup_artifact_size_bytes": 0,
            "backup_artifact_sha256": "",
            "zstd_test_succeeded": False,
            "elapsed_seconds": 0.0,
        },
        "findings": [],
    }
    if not source_path.exists() or not source_path.is_file():
        payload["status"] = "blocked"
        payload["findings"].append(f"source database does not exist: {source_path}")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="rank_history_archive_backup_evidence",
            markdown_title="rank history archive backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)
    if dry_run:
        payload["findings"].append("dry-run only; no backup file was created.")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="rank_history_archive_backup_evidence",
            markdown_title="rank history archive backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)

    backup_result = create_verified_sqlite_backup(
        source_path, db_backup_path, purpose="rank_history_archive", backup_label=backup_label,
        compress=compress, keep_uncompressed=keep_uncompressed,
        check_source=False,
    )
    artifact_path = backup_result["artifact_path"]
    compression_summary = backup_result
    quick_check = backup_result["sqlite_quick_check"]
    sha256 = backup_result["artifact_sha256"] if compute_sha256 else ""
    elapsed = backup_result["elapsed_seconds"]
    payload["status"] = "success"
    payload["summary"].update(
        {
            "created": True,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_db_exists": db_backup_path.exists(),
            "backup_artifact_path": _project_relative(artifact_path, project_root=root),
            "backup_artifact_absolute_path": str(artifact_path),
            "backup_artifact_size_bytes": artifact_path.stat().st_size,
            "backup_artifact_size_mb": round(artifact_path.stat().st_size / (1024 * 1024), 2),
            "backup_artifact_sha256": sha256,
            "verification_manifest_path": _project_relative(
                backup_result["verification_manifest_path"], project_root=root),
            "verification_manifest_sha256": backup_result["verification_manifest_sha256"],
            "recovery_verification": backup_result["recovery_verification"],
            "snapshot_sha256": backup_result["snapshot_sha256"],
            "decoded_sha256": backup_result["decoded_sha256"],
            "snapshot_metadata": backup_result["snapshot_metadata"],
            "backup_space_budget": backup_result["backup_space_budget"],
            "sqlite_quick_check": quick_check,
            "zstd_test_succeeded": bool(compression_summary.get("zstd_test_succeeded")),
            "uncompressed_removed": bool(compression_summary.get("uncompressed_removed")),
            "compression_elapsed_seconds": compression_summary.get(
                "compression_elapsed_seconds",
                0.0,
            ),
            "elapsed_seconds": elapsed,
        }
    )
    payload["findings"].append(
        "rank history hot-delete rollback backup created and verified."
    )
    return _write_json_md(
        payload=payload,
        output_dir=output_dir,
        prefix="rank_history_archive_backup_evidence",
        markdown_title="rank history archive backup evidence",
        project_root=root,
        timestamp=ts,
    ) + (payload,)
