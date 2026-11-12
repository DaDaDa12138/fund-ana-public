#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only preview for compressing database backup snapshots."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.database_backup_cleanup_preview import (
    SUPPORTED_EXTENSIONS,
    _classify_backup_file,
)
from utils.fund_code_alias_audit import load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/database_backup_compression"
DEFAULT_DATABASE_BACKUP_DIR = PROJECT_ROOT / "backend" / "data" / "backups"
DEFAULT_ACTIVE_DATABASE_PATH = PROJECT_ROOT / "backend" / "data" / "bond_funds.db"
DATABASE_BACKUP_COMPRESSION_PREVIEW_JSON_PATTERN = "database_backup_compression_preview_*.json"
DEFAULT_MIN_COMPRESS_SIZE_MB = 128.0


def load_latest_database_backup_compression_preview_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        DATABASE_BACKUP_COMPRESSION_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def _resolve_project_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_ROOT / path).resolve()


def _resolve_backup_dir(backup_dir: Optional[str | Path]) -> Path:
    target_dir = _resolve_project_path(backup_dir or DEFAULT_DATABASE_BACKUP_DIR).resolve()
    if target_dir.exists() and not target_dir.is_dir():
        raise ValueError(f"database backup path is not a directory: {target_dir}")
    return target_dir


def _resolve_active_database_path(active_database_path: Optional[str | Path]) -> Path:
    return _resolve_project_path(active_database_path or DEFAULT_ACTIVE_DATABASE_PATH)


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _normalize_positive_float(value, default: float) -> float:
    try:
        parsed = float(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, parsed)


def _is_supported_uncompressed_sqlite_backup(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name.lower().endswith(".zst"):
        return False
    return path.suffix.lower() in SUPPORTED_EXTENSIONS


def _compressed_peer(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".zst")


def _serialize_candidate(
    path: Path,
    *,
    backup_dir: Path,
    active_database_path: Path,
) -> dict:
    stat = path.stat()
    classified = _classify_backup_file(path)
    compressed_path = _compressed_peer(path)
    existing_compressed = compressed_path.exists() and compressed_path.is_file()
    relative_path = path.relative_to(backup_dir).as_posix()
    compressed_relative_path = compressed_path.relative_to(backup_dir).as_posix()
    return {
        "relative_path": relative_path,
        "absolute_path": str(path),
        "compressed_relative_path": compressed_relative_path,
        "compressed_absolute_path": str(compressed_path),
        "name": path.name,
        "size_bytes": int(stat.st_size),
        "size_mb": round(stat.st_size / (1024 * 1024), 4),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "backup_type": classified["backup_type"],
        "backup_label": classified["backup_label"],
        "family_key": classified["family_key"],
        "family_label": classified["family_label"],
        "timestamp_token": classified["timestamp_token"],
        "timestamp": classified["timestamp"].isoformat() if classified["timestamp"] else None,
        "existing_compressed": existing_compressed,
        "existing_compressed_size_bytes": (
            int(compressed_path.stat().st_size) if existing_compressed else 0
        ),
        "active_database_path": str(active_database_path),
        "action": (
            "verify_existing_compressed_and_remove_source"
            if existing_compressed
            else "compress_and_remove_source_after_verification"
        ),
    }


def build_database_backup_compression_preview_payload(
    *,
    backup_dir: Optional[str | Path] = None,
    active_database_path: Optional[str | Path] = None,
    min_compress_size_mb: float = DEFAULT_MIN_COMPRESS_SIZE_MB,
    max_sample_files: int = 20,
    now: Optional[datetime] = None,
) -> dict:
    """Build a read-only plan for compressing large uncompressed backup DBs."""
    target_dir = _resolve_backup_dir(backup_dir)
    resolved_active_database_path = _resolve_active_database_path(active_database_path)
    effective_now = now or datetime.now()
    normalized_min_size_mb = _normalize_positive_float(
        min_compress_size_mb,
        DEFAULT_MIN_COMPRESS_SIZE_MB,
    )
    min_size_bytes = int(normalized_min_size_mb * 1024 * 1024)
    sample_limit = max(1, int(max_sample_files or 20))

    entries: list[dict] = []
    skipped_too_small = 0
    skipped_active_database = 0
    scanned_uncompressed_file_count = 0
    if target_dir.exists():
        for path in sorted(target_dir.rglob("*")):
            if not _is_supported_uncompressed_sqlite_backup(path):
                continue
            scanned_uncompressed_file_count += 1
            resolved_path = path.resolve()
            if resolved_path == resolved_active_database_path:
                skipped_active_database += 1
                continue
            size_bytes = int(path.stat().st_size)
            if size_bytes < min_size_bytes:
                skipped_too_small += 1
                continue
            entries.append(
                _serialize_candidate(
                    resolved_path,
                    backup_dir=target_dir,
                    active_database_path=resolved_active_database_path,
                )
            )

    entries.sort(
        key=lambda item: (
            -int(item.get("size_bytes") or 0),
            str(item.get("relative_path") or ""),
        )
    )
    total_source_size_bytes = sum(int(item.get("size_bytes") or 0) for item in entries)
    existing_compressed_count = sum(1 for item in entries if item.get("existing_compressed"))
    existing_compressed_size_bytes = sum(
        int(item.get("existing_compressed_size_bytes") or 0)
        for item in entries
        if item.get("existing_compressed")
    )

    summary = {
        "target_dir": str(target_dir),
        "active_database_path": str(resolved_active_database_path),
        "min_compress_size_mb": normalized_min_size_mb,
        "scanned_uncompressed_file_count": scanned_uncompressed_file_count,
        "candidate_file_count": len(entries),
        "candidate_size_bytes": total_source_size_bytes,
        "candidate_size_mb": round(total_source_size_bytes / (1024 * 1024), 2),
        "existing_compressed_candidate_count": existing_compressed_count,
        "existing_compressed_size_bytes": existing_compressed_size_bytes,
        "existing_compressed_size_mb": round(existing_compressed_size_bytes / (1024 * 1024), 2),
        "estimated_source_reclaim_size_bytes": total_source_size_bytes,
        "estimated_source_reclaim_size_mb": round(total_source_size_bytes / (1024 * 1024), 2),
        "skipped_too_small_file_count": skipped_too_small,
        "skipped_active_database_file_count": skipped_active_database,
    }
    findings = [
        (
            f"发现 {summary['candidate_file_count']} 个未压缩数据库备份候选，"
            f"源文件累计约 {summary['candidate_size_mb']} MB。"
        )
    ]
    if entries:
        findings.append(
            "压缩执行须验证离线 SQLite 完整性及全部解压字节SHA一致；保留压缩备份和恢复清单后才移除原件。"
        )
    else:
        findings.append("当前没有达到压缩阈值的未压缩数据库备份。")

    return {
        "generated_at": effective_now.isoformat(),
        "dry_run": True,
        "backup_dir": str(target_dir),
        "parameters": {
            "active_database_path": str(resolved_active_database_path),
            "min_compress_size_mb": normalized_min_size_mb,
            "max_sample_files": sample_limit,
        },
        "summary": summary,
        "findings": findings,
        "candidate_file_samples": entries[:sample_limit],
        "execution_plan": {
            "dry_run": True,
            "target_dir": str(target_dir),
            "scope_file_total": len(entries),
            "scope_files": [str(item.get("relative_path") or "") for item in entries],
            "remove_source_after_verified": True,
            "guardrails": [
                "只处理备份目录内未压缩的 SQLite 备份文件。",
                "不会处理当前活动数据库文件。",
                "完整解压字节、大小、SHA及源身份须一致，且没有未检查点WAL/journal，才允许移除源文件。",
                "该治理不删除回滚语义，只把回滚点从 .db/.sqlite 转为 .zst。",
            ],
        },
    }


def render_database_backup_compression_preview_markdown(payload: dict) -> str:
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    execution_plan = payload.get("execution_plan") or {}
    lines = [
        "# database backup compression preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- backup_dir: {payload.get('backup_dir')}",
        f"- min_compress_size_mb: {parameters.get('min_compress_size_mb')}",
        "",
        "## summary",
        f"- scanned_uncompressed_file_count: {summary.get('scanned_uncompressed_file_count', 0)}",
        f"- candidate_file_count: {summary.get('candidate_file_count', 0)}",
        f"- candidate_size_mb: {summary.get('candidate_size_mb', 0)}",
        f"- existing_compressed_candidate_count: {summary.get('existing_compressed_candidate_count', 0)}",
        f"- estimated_source_reclaim_size_mb: {summary.get('estimated_source_reclaim_size_mb', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## candidate_files"])
    candidates = payload.get("candidate_file_samples") or []
    if not candidates:
        lines.append("- 无候选")
    else:
        for item in candidates:
            lines.append(
                "- "
                + json.dumps(
                    {
                        "relative_path": item.get("relative_path"),
                        "compressed_relative_path": item.get("compressed_relative_path"),
                        "size_mb": item.get("size_mb"),
                        "backup_type": item.get("backup_type"),
                        "action": item.get("action"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## execution_plan"])
    lines.append(f"- scope_file_total: {execution_plan.get('scope_file_total', 0)}")
    lines.append(f"- remove_source_after_verified: {execution_plan.get('remove_source_after_verified')}")
    lines.append("- guardrails:")
    for item in execution_plan.get("guardrails") or []:
        lines.append(f"  - {item}")
    return "\n".join(lines).strip() + "\n"


def write_database_backup_compression_preview_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: str | None = None,
) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    json_path = artifact_dir / f"database_backup_compression_preview_{ts}.json"
    md_path = artifact_dir / f"database_backup_compression_preview_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_database_backup_compression_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
