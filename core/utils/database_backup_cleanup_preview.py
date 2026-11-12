#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库备份目录清理 dry-run 预演。

目标：
1. 区分调度器月备份、维护窗口快照和未识别备份；
2. 月备份按保留份数预演候选文件；
3. 维护型快照默认保护每个家族的最新文件；
4. 全程只读，不执行任何删除。
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DATABASE_BACKUP_DIR = PROJECT_ROOT / "backend" / "data" / "backups"
DEFAULT_ACTIVE_DATABASE_PATH = PROJECT_ROOT / "backend" / "data" / "bond_funds.db"
DATABASE_BACKUP_CLEANUP_PREVIEW_JSON_PATTERN = "database_backup_cleanup_preview_*.json"
SUPPORTED_EXTENSIONS = {".db", ".sqlite", ".sqlite3"}
SUPPORTED_COMPRESSED_SUFFIXES = {
    ".db.zst",
    ".sqlite.zst",
    ".sqlite3.zst",
}
MANUAL_SNAPSHOT_REVIEW_RATIO_THRESHOLD = 1.5
MANUAL_SNAPSHOT_REVIEW_MIN_DELTA_BYTES = 1024 * 1024 * 1024
DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT = 6
DEFAULT_UNMANAGED_RETENTION_DAYS = 14
DEFAULT_KEEP_LATEST_MANUAL_SNAPSHOT_PER_FAMILY = 1
SCHEDULED_BACKUP_PATTERN = re.compile(
    r"^bond_funds_backup_(\d{8}_\d{6})(\.(?:db|sqlite|sqlite3)(?:\.zst)?)$",
    re.IGNORECASE,
)
MANUAL_SNAPSHOT_PATTERN = re.compile(
    r"^bond_funds_((?:pre|post)_[a-z0-9_]+)_(\d{8}(?:_\d{6})?(?:_[a-z0-9_]+)?)(\.(?:db|sqlite|sqlite3)(?:\.zst)?)$",
    re.IGNORECASE,
)


def load_latest_database_backup_cleanup_preview_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新数据库备份 cleanup dry-run 预演产物。"""
    return load_latest_json_artifact(
        DATABASE_BACKUP_CLEANUP_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def get_database_backup_cleanup_policy(config: Optional[dict] = None) -> dict:
    """从运行时配置提取数据库备份保留策略，缺失时回退默认值。"""
    scheduler_cleanup = (
        dict(config.get("scheduler", {}).get("cleanup", {}))
        if isinstance(config, dict)
        else {}
    )
    backup_cleanup = dict(scheduler_cleanup.get("database_backups", {}))
    return {
        "scheduled_keep_count": _normalize_positive_int(
            backup_cleanup.get("scheduled_keep_count"),
            DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT,
        ),
        "unmanaged_retention_days": _normalize_positive_int(
            backup_cleanup.get("unmanaged_retention_days"),
            DEFAULT_UNMANAGED_RETENTION_DAYS,
        ),
        "keep_latest_manual_snapshot_per_family": _normalize_positive_int(
            backup_cleanup.get("keep_latest_manual_snapshot_per_family"),
            DEFAULT_KEEP_LATEST_MANUAL_SNAPSHOT_PER_FAMILY,
        ),
    }


def _resolve_backup_dir(backup_dir: Optional[str | Path]) -> Path:
    target_dir = Path(backup_dir or DEFAULT_DATABASE_BACKUP_DIR)
    if not target_dir.is_absolute():
        target_dir = (PROJECT_ROOT / target_dir).resolve()
    if target_dir.exists() and not target_dir.is_dir():
        raise ValueError(f"database backup path is not a directory: {target_dir}")
    return target_dir


def _resolve_active_database_path(
    active_database_path: Optional[str | Path],
    *,
    backup_dir: Path,
) -> Path:
    if active_database_path is None:
        return (backup_dir.parent / DEFAULT_ACTIVE_DATABASE_PATH.name).resolve()

    target_path = Path(active_database_path)
    if not target_path.is_absolute():
        target_path = (PROJECT_ROOT / target_path).resolve()
    else:
        target_path = target_path.resolve()
    return target_path


def _resolve_audit_dir(output_dir: str) -> Path:
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _normalize_positive_int(value, default: int) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, parsed)


def _list_backup_files(backup_dir: Path) -> list[Path]:
    if not backup_dir.exists():
        return []
    files = []
    for path in backup_dir.rglob("*"):
        if not path.is_file():
            continue
        lower_name = path.name.lower()
        if (
            path.suffix.lower() not in SUPPORTED_EXTENSIONS
            and not any(lower_name.endswith(suffix) for suffix in SUPPORTED_COMPRESSED_SUFFIXES)
        ):
            continue
        files.append(path)
    return sorted(files)


def _parse_backup_timestamp(timestamp_token: str) -> datetime | None:
    text = str(timestamp_token or "").strip()
    for pattern in (r"(\d{8}_\d{6})", r"(\d{8})"):
        match = re.search(pattern, text)
        if not match:
            continue
        token = match.group(1)
        try:
            if "_" in token:
                return datetime.strptime(token, "%Y%m%d_%H%M%S")
            return datetime.strptime(token, "%Y%m%d")
        except ValueError:
            continue
    return None


def _classify_backup_file(path: Path) -> dict:
    scheduled_match = SCHEDULED_BACKUP_PATTERN.match(path.name)
    if scheduled_match:
        timestamp_token = scheduled_match.group(1)
        timestamp = datetime.strptime(timestamp_token, "%Y%m%d_%H%M%S")
        return {
            "backup_type": "scheduled_backup",
            "backup_label": "调度器月备份",
            "family_key": "scheduled_backup",
            "family_label": "调度器月备份",
            "timestamp_token": timestamp_token,
            "timestamp": timestamp,
        }

    manual_match = MANUAL_SNAPSHOT_PATTERN.match(path.name)
    if manual_match:
        snapshot_key = manual_match.group(1).lower()
        timestamp_token = manual_match.group(2)
        timestamp = _parse_backup_timestamp(timestamp_token)
        snapshot_label = snapshot_key.replace("_", " ")
        return {
            "backup_type": "manual_snapshot",
            "backup_label": "维护窗口快照",
            "family_key": f"manual_snapshot:{snapshot_key}",
            "family_label": f"维护窗口快照 / {snapshot_label}",
            "timestamp_token": timestamp_token,
            "timestamp": timestamp,
        }

    return {
        "backup_type": "unmanaged_backup",
        "backup_label": "未识别备份",
        "family_key": f"unmanaged:{path.suffix.lower() or '<no_ext>'}",
        "family_label": "未识别备份",
        "timestamp_token": "",
        "timestamp": None,
    }


def _serialize_backup_file(path: Path, *, base_dir: Path) -> dict:
    stat = path.stat()
    classified = _classify_backup_file(path)
    return {
        "relative_path": str(path.relative_to(base_dir)),
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
    }


def _build_group_rows(entries: Sequence[dict]) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for entry in entries:
        grouped[str(entry.get("family_key") or "")].append(entry)
    return grouped


def _sort_entry_key(entry: dict) -> tuple[str, str, str]:
    return (
        str(entry.get("timestamp") or ""),
        str(entry.get("modified_at") or ""),
        str(entry.get("relative_path") or ""),
    )


def select_scheduled_backup_cleanup_candidates(
    backup_dir: str | Path,
    *,
    keep_count: int = DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT,
) -> list[Path]:
    """返回应由调度器自动清理的月备份文件。"""
    normalized_keep_count = _normalize_positive_int(keep_count, 6)
    target_dir = _resolve_backup_dir(backup_dir)
    entries = [
        _serialize_backup_file(path, base_dir=target_dir)
        for path in _list_backup_files(target_dir)
    ]
    scheduled_entries = [entry for entry in entries if entry.get("backup_type") == "scheduled_backup"]
    scheduled_entries.sort(key=_sort_entry_key, reverse=True)
    return [
        target_dir / str(entry.get("relative_path") or "")
        for entry in scheduled_entries[normalized_keep_count:]
    ]


def _build_candidate_groups(
    candidate_entries: Sequence[dict],
    *,
    disposition: str,
    reason: str,
    runtime_dependency: str,
    protected_by_family: Optional[dict[str, list[dict]]] = None,
) -> list[dict]:
    protected_by_family = protected_by_family or {}
    groups = []
    for family_key, rows in _build_group_rows(candidate_entries).items():
        sorted_rows = sorted(rows, key=_sort_entry_key)
        total_size_bytes = sum(int(item.get("size_bytes") or 0) for item in sorted_rows)
        protected_rows = protected_by_family.get(family_key) or []
        protected_rows = sorted(protected_rows, key=_sort_entry_key, reverse=True)
        groups.append(
            {
                "key": family_key,
                "label": str(rows[0].get("family_label") or family_key),
                "disposition": disposition,
                "reason": reason,
                "runtime_dependency": runtime_dependency,
                "candidate_file_count": len(sorted_rows),
                "candidate_size_bytes": total_size_bytes,
                "candidate_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "oldest_candidate_timestamp": sorted_rows[0].get("timestamp") or sorted_rows[0].get("modified_at"),
                "newest_candidate_timestamp": sorted_rows[-1].get("timestamp") or sorted_rows[-1].get("modified_at"),
                "latest_protected_path": protected_rows[0].get("relative_path") if protected_rows else None,
                "sample_files": [
                    {
                        "relative_path": item.get("relative_path"),
                        "size_bytes": item.get("size_bytes"),
                        "timestamp": item.get("timestamp"),
                        "modified_at": item.get("modified_at"),
                    }
                    for item in sorted_rows[:10]
                ],
            }
        )

    groups.sort(
        key=lambda item: (
            -int(item.get("candidate_size_bytes") or 0),
            -int(item.get("candidate_file_count") or 0),
            str(item.get("key") or ""),
        )
    )
    return groups


def _build_findings(summary: dict) -> list[str]:
    findings = [
        (
            f"备份目录共 {summary.get('total_file_count', 0)} 个文件，"
            f"累计约 {summary.get('total_size_mb', 0)} MB。"
        ),
        (
            f"其中调度器月备份 {summary.get('scheduled_backup_count', 0)} 个，"
            f"维护窗口快照 {summary.get('manual_snapshot_count', 0)} 个，"
            f"未识别备份 {summary.get('unmanaged_backup_count', 0)} 个。"
        ),
    ]

    auto_count = int(summary.get("auto_candidate_file_count", 0) or 0)
    manual_count = int(summary.get("manual_review_candidate_file_count", 0) or 0)
    if auto_count > 0:
        findings.append(
            f"存在 {auto_count} 个调度器月备份超出保留份数，可在维护窗口自动清理。"
        )
    if manual_count > 0:
        findings.append(
            f"存在 {manual_count} 个备份需要人工复核后再决定是否删除。"
        )
    if auto_count == 0 and manual_count == 0:
        findings.append("当前没有发现需要进入清理或人工复核的数据库备份文件。")

    oversized_count = int(summary.get("oversized_manual_snapshot_review_file_count", 0) or 0)
    if oversized_count > 0:
        findings.append(
            f"存在 {oversized_count} 个维护窗口快照虽然仍是家族内最新文件，但体积显著高于当前主库，需要在维护窗口人工确认是否继续保留回滚点。"
        )
    return findings


def _is_oversized_manual_snapshot_review_candidate(
    entry: dict,
    *,
    active_database_size_bytes: int,
) -> bool:
    if active_database_size_bytes <= 0:
        return False

    snapshot_size_bytes = int(entry.get("size_bytes") or 0)
    if snapshot_size_bytes <= 0:
        return False

    size_delta_bytes = snapshot_size_bytes - active_database_size_bytes
    if size_delta_bytes < MANUAL_SNAPSHOT_REVIEW_MIN_DELTA_BYTES:
        return False

    return snapshot_size_bytes >= int(active_database_size_bytes * MANUAL_SNAPSHOT_REVIEW_RATIO_THRESHOLD)


def build_database_backup_cleanup_preview_payload(
    *,
    backup_dir: Optional[str | Path] = None,
    active_database_path: Optional[str | Path] = None,
    scheduled_keep_count: int = DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT,
    unmanaged_retention_days: int = DEFAULT_UNMANAGED_RETENTION_DAYS,
    keep_latest_manual_snapshot_per_family: int = DEFAULT_KEEP_LATEST_MANUAL_SNAPSHOT_PER_FAMILY,
    max_sample_files: int = 20,
    now: Optional[datetime] = None,
) -> dict:
    """构建数据库备份 cleanup dry-run 预演载荷。"""
    target_dir = _resolve_backup_dir(backup_dir)
    resolved_active_database_path = _resolve_active_database_path(
        active_database_path,
        backup_dir=target_dir,
    )
    effective_now = now or datetime.now()
    normalized_keep_count = _normalize_positive_int(scheduled_keep_count, 6)
    normalized_unmanaged_days = _normalize_positive_int(unmanaged_retention_days, 14)
    normalized_manual_keep = _normalize_positive_int(keep_latest_manual_snapshot_per_family, 1)
    normalized_sample_limit = _normalize_positive_int(max_sample_files, 20)

    entries = [
        _serialize_backup_file(path, base_dir=target_dir)
        for path in _list_backup_files(target_dir)
    ]

    active_database_exists = resolved_active_database_path.exists() and resolved_active_database_path.is_file()
    active_database_size_bytes = (
        int(resolved_active_database_path.stat().st_size)
        if active_database_exists
        else 0
    )

    scheduled_entries = [entry for entry in entries if entry.get("backup_type") == "scheduled_backup"]
    manual_entries = [entry for entry in entries if entry.get("backup_type") == "manual_snapshot"]
    unmanaged_entries = [entry for entry in entries if entry.get("backup_type") == "unmanaged_backup"]

    scheduled_entries.sort(key=_sort_entry_key, reverse=True)
    auto_candidate_entries = scheduled_entries[normalized_keep_count:]
    protected_scheduled_entries = scheduled_entries[:normalized_keep_count]

    protected_manual_entries: list[dict] = []
    manual_review_entries: list[dict] = []
    protected_manual_by_family: dict[str, list[dict]] = defaultdict(list)
    oversized_manual_snapshot_review_entries: list[dict] = []
    for family_key, rows in _build_group_rows(manual_entries).items():
        sorted_rows = sorted(rows, key=_sort_entry_key, reverse=True)
        protected_rows = sorted_rows[:normalized_manual_keep]
        review_rows = sorted_rows[normalized_manual_keep:]
        manual_review_entries.extend(review_rows)
        for entry in protected_rows:
            if _is_oversized_manual_snapshot_review_candidate(
                entry,
                active_database_size_bytes=active_database_size_bytes,
            ):
                oversized_entry = dict(entry)
                oversized_entry["manual_review_reason"] = "oversized_vs_active_database"
                oversized_entry["active_database_size_bytes"] = active_database_size_bytes
                oversized_entry["active_database_size_mb"] = round(active_database_size_bytes / (1024 * 1024), 4)
                oversized_entry["snapshot_to_active_ratio"] = round(
                    int(entry.get("size_bytes") or 0) / active_database_size_bytes,
                    4,
                )
                oversized_manual_snapshot_review_entries.append(oversized_entry)
                manual_review_entries.append(oversized_entry)
                continue

            protected_manual_entries.append(entry)
            protected_manual_by_family[family_key].append(entry)

    unmanaged_cutoff = effective_now - timedelta(days=normalized_unmanaged_days)
    unmanaged_review_entries = []
    protected_unmanaged_entries = []
    for entry in unmanaged_entries:
        modified_at = datetime.fromisoformat(str(entry.get("modified_at")))
        if modified_at < unmanaged_cutoff:
            unmanaged_review_entries.append(entry)
        else:
            protected_unmanaged_entries.append(entry)

    auto_candidate_entries = sorted(auto_candidate_entries, key=_sort_entry_key)
    manual_review_entries = sorted(
        list(manual_review_entries) + list(unmanaged_review_entries),
        key=lambda item: (
            str(item.get("timestamp") or item.get("modified_at") or ""),
            str(item.get("relative_path") or ""),
        ),
    )

    auto_candidate_groups = _build_candidate_groups(
        auto_candidate_entries,
        disposition="auto_cleanup_candidate",
        reason=f"超出调度器月备份保留份数 {normalized_keep_count}",
        runtime_dependency="仅用于维护窗口回滚；确认最近月备份可用后，可清理更早月备份",
        protected_by_family={"scheduled_backup": protected_scheduled_entries},
    )
    manual_review_groups = _build_candidate_groups(
        manual_review_entries,
        disposition="manual_review_candidate",
        reason="需要人工确认是否仍保留维护窗口快照或未识别备份",
        runtime_dependency="维护型快照和未识别备份不参与自动删除，删除前应确认恢复策略",
        protected_by_family=protected_manual_by_family,
    )
    candidate_groups = auto_candidate_groups + manual_review_groups
    candidate_groups.sort(
        key=lambda item: (
            -int(item.get("candidate_size_bytes") or 0),
            -int(item.get("candidate_file_count") or 0),
            str(item.get("key") or ""),
        )
    )

    protected_entries = sorted(
        protected_scheduled_entries + protected_manual_entries + protected_unmanaged_entries,
        key=lambda item: _sort_entry_key(item),
        reverse=True,
    )

    total_size_bytes = sum(int(item.get("size_bytes") or 0) for item in entries)
    auto_candidate_size_bytes = sum(int(item.get("size_bytes") or 0) for item in auto_candidate_entries)
    manual_review_size_bytes = sum(int(item.get("size_bytes") or 0) for item in manual_review_entries)
    candidate_entries = auto_candidate_entries + manual_review_entries

    summary = {
        "total_file_count": len(entries),
        "total_size_bytes": total_size_bytes,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        "active_database_path": str(resolved_active_database_path),
        "active_database_exists": active_database_exists,
        "active_database_size_bytes": active_database_size_bytes,
        "active_database_size_mb": round(active_database_size_bytes / (1024 * 1024), 2),
        "scheduled_backup_count": len(scheduled_entries),
        "scheduled_backup_size_bytes": sum(int(item.get("size_bytes") or 0) for item in scheduled_entries),
        "scheduled_backup_size_mb": round(
            sum(int(item.get("size_bytes") or 0) for item in scheduled_entries) / (1024 * 1024),
            2,
        ),
        "manual_snapshot_count": len(manual_entries),
        "manual_snapshot_size_bytes": sum(int(item.get("size_bytes") or 0) for item in manual_entries),
        "manual_snapshot_size_mb": round(
            sum(int(item.get("size_bytes") or 0) for item in manual_entries) / (1024 * 1024),
            2,
        ),
        "unmanaged_backup_count": len(unmanaged_entries),
        "unmanaged_backup_size_bytes": sum(int(item.get("size_bytes") or 0) for item in unmanaged_entries),
        "unmanaged_backup_size_mb": round(
            sum(int(item.get("size_bytes") or 0) for item in unmanaged_entries) / (1024 * 1024),
            2,
        ),
        "protected_file_count": len(protected_entries),
        "candidate_file_count": len(candidate_entries),
        "candidate_size_bytes": auto_candidate_size_bytes + manual_review_size_bytes,
        "candidate_size_mb": round((auto_candidate_size_bytes + manual_review_size_bytes) / (1024 * 1024), 2),
        "auto_candidate_file_count": len(auto_candidate_entries),
        "auto_candidate_size_bytes": auto_candidate_size_bytes,
        "auto_candidate_size_mb": round(auto_candidate_size_bytes / (1024 * 1024), 2),
        "manual_review_candidate_file_count": len(manual_review_entries),
        "manual_review_candidate_size_bytes": manual_review_size_bytes,
        "manual_review_candidate_size_mb": round(manual_review_size_bytes / (1024 * 1024), 2),
        "oversized_manual_snapshot_review_file_count": len(oversized_manual_snapshot_review_entries),
        "oversized_manual_snapshot_review_size_bytes": sum(
            int(item.get("size_bytes") or 0) for item in oversized_manual_snapshot_review_entries
        ),
        "oversized_manual_snapshot_review_size_mb": round(
            sum(int(item.get("size_bytes") or 0) for item in oversized_manual_snapshot_review_entries) / (1024 * 1024),
            2,
        ),
        "oldest_candidate_timestamp": candidate_entries[0].get("timestamp") if candidate_entries else None,
        "newest_candidate_timestamp": candidate_entries[-1].get("timestamp") if candidate_entries else None,
    }

    return {
        "generated_at": effective_now.isoformat(),
        "dry_run": True,
        "backup_dir": str(target_dir),
        "parameters": {
            "active_database_path": str(resolved_active_database_path),
            "scheduled_keep_count": normalized_keep_count,
            "unmanaged_retention_days": normalized_unmanaged_days,
            "keep_latest_manual_snapshot_per_family": normalized_manual_keep,
            "max_sample_files": normalized_sample_limit,
        },
        "summary": summary,
        "findings": _build_findings(summary),
        "candidate_groups": candidate_groups,
        "candidate_file_samples": candidate_entries[:normalized_sample_limit],
        "protected_file_samples": protected_entries[:normalized_sample_limit],
        "execution_plan": {
            "dry_run": True,
            "target_dir": str(target_dir),
            "scope_file_total": len(auto_candidate_entries),
            "scope_files": [str(item.get("relative_path") or "") for item in auto_candidate_entries],
            "manual_review_file_total": len(manual_review_entries),
            "manual_review_files": [str(item.get("relative_path") or "") for item in manual_review_entries],
            "guardrails": [
                "仅调度器月备份中超出保留份数的文件才会进入自动清理 scope。",
                "维护窗口快照默认保护每个家族的最新文件。",
                "未识别备份只进入人工复核清单，不会自动删除。",
            ],
        },
    }


def render_database_backup_cleanup_preview_markdown(payload: dict) -> str:
    """渲染 Markdown 预演报告。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    execution_plan = payload.get("execution_plan") or {}

    lines = [
        "# database backup cleanup preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- backup_dir: {payload.get('backup_dir')}",
        f"- scheduled_keep_count: {parameters.get('scheduled_keep_count')}",
        f"- unmanaged_retention_days: {parameters.get('unmanaged_retention_days')}",
        f"- keep_latest_manual_snapshot_per_family: {parameters.get('keep_latest_manual_snapshot_per_family')}",
        "",
        "## summary",
        f"- total_file_count: {summary.get('total_file_count', 0)}",
        f"- total_size_mb: {summary.get('total_size_mb', 0)}",
        f"- active_database_size_mb: {summary.get('active_database_size_mb', 0)}",
        f"- scheduled_backup_count: {summary.get('scheduled_backup_count', 0)}",
        f"- manual_snapshot_count: {summary.get('manual_snapshot_count', 0)}",
        f"- unmanaged_backup_count: {summary.get('unmanaged_backup_count', 0)}",
        f"- candidate_file_count: {summary.get('candidate_file_count', 0)}",
        f"- auto_candidate_file_count: {summary.get('auto_candidate_file_count', 0)}",
        f"- manual_review_candidate_file_count: {summary.get('manual_review_candidate_file_count', 0)}",
        f"- oversized_manual_snapshot_review_file_count: {summary.get('oversized_manual_snapshot_review_file_count', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## candidate_groups"])
    candidate_groups = payload.get("candidate_groups") or []
    if not candidate_groups:
        lines.append("- 无候选")
    else:
        for item in candidate_groups[:20]:
            lines.append(
                "- " + json.dumps(
                    {
                        "label": item.get("label"),
                        "disposition": item.get("disposition"),
                        "candidate_file_count": item.get("candidate_file_count"),
                        "candidate_size_mb": item.get("candidate_size_mb"),
                        "latest_protected_path": item.get("latest_protected_path"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## execution_plan"])
    lines.append(f"- scope_file_total: {execution_plan.get('scope_file_total', 0)}")
    lines.append(f"- manual_review_file_total: {execution_plan.get('manual_review_file_total', 0)}")
    lines.append("- guardrails:")
    for item in execution_plan.get("guardrails") or []:
        lines.append(f"  - {item}")
    return "\n".join(lines) + "\n"


def write_database_backup_cleanup_preview_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, Path]:
    """写出 JSON + Markdown 产物。"""
    artifact_dir = _resolve_audit_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"database_backup_cleanup_preview_{timestamp}.json"
    markdown_path = artifact_dir / f"database_backup_cleanup_preview_{timestamp}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_database_backup_cleanup_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path
