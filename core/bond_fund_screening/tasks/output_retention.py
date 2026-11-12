#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同业务日筛选产物保留工具。

目标：
1. 同一业务日、同一产物家族只保留最新版本；
2. 仅清理带完整时间戳的产物，避免误删固定文件名；
3. 删除主文件时同步删除其 sidecar 元数据；
4. 跨业务日历史文件不受影响。
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from bond_fund_screening.report_artifact_metadata import resolve_report_meta_path
from bond_fund_screening.report_catalog import rebuild_report_manifest
from utils.core_csv_storage import CoreCsvStorageError, has_stored_csv
from utils import report_history_store as history_store


_TIMESTAMPED_ARTIFACT_RE = re.compile(
    r"^(?P<prefix>.+)_(?P<day>20\d{6})_(?P<time>\d{6})(?P<tail>(?:_[^.]+)*)$"
)
def _parse_timestamped_artifact(path: str | Path) -> dict | None:
    """解析带业务时间戳的产物文件名。"""
    artifact_path = Path(path)
    match = _TIMESTAMPED_ARTIFACT_RE.match(artifact_path.stem)
    if not match:
        return None

    prefix = str(match.group("prefix") or "").strip()
    day_token = str(match.group("day") or "").strip()
    time_token = str(match.group("time") or "").strip()
    tail = str(match.group("tail") or "")
    if not prefix or not day_token or not time_token:
        return None

    return {
        "path": artifact_path,
        "prefix": prefix,
        "normalized_prefix": prefix,
        "day_token": day_token,
        "time_token": time_token,
        "timestamp_token": f"{day_token}_{time_token}",
        "tail": tail,
        "suffix": artifact_path.suffix.lower(),
    }


def _build_family_descriptor(
    *,
    prefix: str,
    day_token: str,
    suffix: str,
    tail: str = "",
) -> dict:
    """构造家族描述，供显式家族清理使用。"""
    normalized_suffix = str(suffix or "").strip().lower()
    normalized_tail = str(tail or "").strip()
    if normalized_tail and not normalized_tail.startswith("_"):
        normalized_tail = f"_{normalized_tail}"
    return {
        "normalized_prefix": str(prefix or "").strip(),
        "day_token": str(day_token or "").strip(),
        "tail": normalized_tail,
        "suffix": normalized_suffix,
    }


def _matches_family(parsed: dict | None, family: dict) -> bool:
    """判断文件是否属于指定家族。"""
    if not parsed:
        return False
    return (
        parsed.get("day_token") == family.get("day_token")
        and parsed.get("tail") == family.get("tail")
        and parsed.get("suffix") == family.get("suffix")
        and parsed.get("normalized_prefix") == family.get("normalized_prefix")
    )


def _iter_same_day_family_files(directory: str | Path, family: dict) -> list[Path]:
    """列出同业务日、同家族的所有时间戳文件。"""
    artifact_dir = Path(directory)
    if not artifact_dir.exists() or not artifact_dir.is_dir():
        return []

    candidates: list[tuple[str, float, str, Path]] = []
    for path in artifact_dir.iterdir():
        if not path.is_file():
            continue
        parsed = _parse_timestamped_artifact(path)
        if not _matches_family(parsed, family):
            continue
        try:
            mtime = float(path.stat().st_mtime)
        except OSError:
            mtime = 0.0
        candidates.append((
            str(parsed.get("timestamp_token") or ""),
            mtime,
            path.name,
            path,
        ))

    candidates.sort()
    return [item[3] for item in candidates]


def _history_protection_reason(path: Path) -> str | None:
    """Registered historical sources retire only through lifecycle receipts.

    An unreadable ledger cannot prove that a file is unreferenced. Keeping the
    bytes does not change the tracker's business-date replacement selection.
    """
    try:
        options = history_store.project_options(path)
        if options is None or options[2] == 'legacy':
            return None
        root, database, _ = options
        with history_store.connection(database) as conn:
            for source in (path, resolve_report_meta_path(path)):
                relative = history_store.relative(root, source)
                if conn.execute('SELECT 1 FROM sources WHERE path=? LIMIT 1', (relative,)).fetchone():
                    return 'history_source_requires_lifecycle'
    except (OSError, ValueError, sqlite3.Error):
        return 'history_dependency_check_unavailable'
    return None


def _delete_file_and_sidecar(path: Path) -> list[str]:
    """删除主文件并同步删除其 sidecar。"""
    if has_stored_csv(path):
        raise CoreCsvStorageError("archived_csv_requires_explicit_restore_before_retirement")
    if _history_protection_reason(path):
        return []
    deleted: list[str] = []

    try:
        if path.exists():
            path.unlink()
            deleted.append(str(path))
    except FileNotFoundError:
        pass

    sidecar_path = resolve_report_meta_path(path)
    if sidecar_path != path:
        try:
            if sidecar_path.exists():
                sidecar_path.unlink()
                deleted.append(str(sidecar_path))
        except FileNotFoundError:
            pass

    return deleted


def cleanup_same_day_family_versions(
    directory: str | Path,
    *,
    prefix: str,
    day_token: str,
    suffix: str,
    tail: str = "",
    keep_file: str | Path | None = None,
    keep_latest_when_missing: bool = True,
    refresh_manifest: bool = True,
) -> dict:
    """
    清理同业务日同家族旧版本。

    keep_file 为 None 时：
    1. keep_latest_when_missing=True 时，若家族下存在文件，则自动保留最新一个；
    2. keep_latest_when_missing=False 时，删除该家族同日全部版本。
    """
    family = _build_family_descriptor(
        prefix=prefix,
        day_token=day_token,
        suffix=suffix,
        tail=tail,
    )
    family_files = _iter_same_day_family_files(directory, family)
    keep_path = Path(keep_file) if keep_file else (
        family_files[-1] if (keep_latest_when_missing and family_files) else None
    )

    deleted_files: list[str] = []
    retained_files: list[dict] = []
    for path in family_files:
        if keep_path is not None and path == keep_path:
            continue
        reason = _history_protection_reason(path)
        if reason:
            retained_files.append({'path': str(path), 'reason': reason})
            continue
        deleted_files.extend(_delete_file_and_sidecar(path))

    if deleted_files and refresh_manifest:
        rebuild_report_manifest(Path(directory))

    return {
        "day_token": family["day_token"],
        "family_prefix": family["normalized_prefix"],
        "family_tail": family["tail"],
        "suffix": family["suffix"],
        "kept_file": str(keep_path) if keep_path else None,
        "deleted_files": deleted_files,
        "deleted_count": len(deleted_files),
        "retained_files": retained_files,
    }


def cleanup_same_day_versions_for_file(
    path: str | Path,
    *,
    refresh_manifest: bool = True,
) -> dict:
    """按给定文件自动识别家族并清理同日旧版本。"""
    artifact_path = Path(path)
    parsed = _parse_timestamped_artifact(artifact_path)
    if not parsed:
        return {
            "kept_file": str(artifact_path),
            "deleted_files": [],
            "deleted_count": 0,
            "reason": "not_timestamped",
        }

    return cleanup_same_day_family_versions(
        artifact_path.parent,
        prefix=str(parsed["prefix"]),
        day_token=str(parsed["day_token"]),
        suffix=str(parsed["suffix"]),
        tail=str(parsed["tail"]),
        keep_file=None,
        refresh_manifest=refresh_manifest,
    )


def remove_same_day_family_versions(
    directory: str | Path,
    *,
    prefix: str,
    day_token: str,
    suffix: str,
    tail: str = "",
    refresh_manifest: bool = True,
) -> dict:
    """显式删除同业务日某家族的全部版本，常用于本轮未再生成的可选产物。"""
    return cleanup_same_day_family_versions(
        directory,
        prefix=prefix,
        day_token=day_token,
        suffix=suffix,
        tail=tail,
        keep_file=None,
        keep_latest_when_missing=False,
        refresh_manifest=refresh_manifest,
    )
