#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选输出目录历史产物清理 dry-run 预演。

目标：
1. 仅纳入带时间戳的历史输出文件；
2. 默认保护每个产物家族的最新文件；
3. 全程只读，不执行任何删除。
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCREENING_OUTPUT_DIR = PROJECT_ROOT / "bond_fund_screening" / "output"
SCREENING_OUTPUT_CLEANUP_PREVIEW_JSON_PATTERN = "screening_output_cleanup_preview_*.json"
PROTECTED_FILENAMES = {".gitkeep"}
DEFAULT_RETENTION_DAYS = 14
DEFAULT_KEEP_LATEST_PER_FAMILY = True
TIMESTAMP_PATTERNS = (
    ("%Y%m%d_%H%M%S", re.compile(r"(?<!\d)(\d{8}_\d{6})(?!\d)")),
    ("%Y-%m-%d", re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")),
    ("%Y%m%d", re.compile(r"(?<!\d)(\d{8})(?!\d)")),
)


def load_latest_screening_output_cleanup_preview_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选输出 cleanup dry-run 预演产物。"""
    return load_latest_json_artifact(
        SCREENING_OUTPUT_CLEANUP_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def get_screening_output_cleanup_policy(config: Optional[dict] = None) -> dict:
    """从运行时配置提取筛选输出保留策略，缺失时回退默认值。"""
    scheduler_cleanup = (
        dict(config.get("scheduler", {}).get("cleanup", {}))
        if isinstance(config, dict)
        else {}
    )
    output_cleanup = dict(scheduler_cleanup.get("screening_outputs", {}))
    keep_latest = output_cleanup.get("keep_latest_per_family")
    return {
        "retention_days": _normalize_positive_int(
            output_cleanup.get("retention_days"),
            DEFAULT_RETENTION_DAYS,
        ),
        "keep_latest_per_family": DEFAULT_KEEP_LATEST_PER_FAMILY if keep_latest is None else bool(keep_latest),
    }


def _resolve_output_artifact_dir(output_dir: Optional[str | Path]) -> Path:
    target_dir = Path(output_dir or DEFAULT_SCREENING_OUTPUT_DIR)
    if not target_dir.is_absolute():
        target_dir = (PROJECT_ROOT / target_dir).resolve()
    if target_dir.exists() and not target_dir.is_dir():
        raise ValueError(f"screening output path is not a directory: {target_dir}")
    return target_dir


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


def _normalize_preview_detail_level(value: Optional[str]) -> str:
    normalized = str(value or "full").strip().lower()
    if normalized == "overview":
        return "overview"
    return "full"


def _to_relative_output_path(path: Path, base_dir: Path) -> str:
    return os.path.relpath(
        os.path.abspath(os.fspath(path)),
        os.path.abspath(os.fspath(base_dir)),
    )


def _extract_timestamp_context(path: Path) -> dict:
    stem = path.stem
    for date_format, pattern in TIMESTAMP_PATTERNS:
        match = pattern.search(stem)
        if not match:
            continue

        raw_token = match.group(1)
        try:
            parsed_at = datetime.strptime(raw_token, date_format)
        except ValueError:
            continue

        prefix = stem[: match.start()].rstrip("._- ")
        suffix = stem[match.end() :].lstrip("._- ")
        family_parts = [prefix or "<root>", suffix or "<main>", path.suffix.lower() or "<no_ext>"]
        family_key = " | ".join(family_parts)

        family_label = prefix or path.suffix.lower().lstrip(".") or path.name
        if suffix:
            family_label = f"{family_label} / {suffix}"

        return {
            "timestamped": True,
            "timestamp_token": raw_token,
            "timestamp": parsed_at,
            "family_key": family_key,
            "family_label": family_label,
        }

    return {
        "timestamped": False,
        "timestamp_token": "",
        "timestamp": None,
        "family_key": f"stable | {path.stem or path.name} | {path.suffix.lower() or '<no_ext>'}",
        "family_label": path.stem or path.name,
    }


def _serialize_file(path: Path, *, base_dir: Path) -> dict:
    stat = path.stat()
    timestamp_info = _extract_timestamp_context(path)
    return {
        "relative_path": _to_relative_output_path(path, base_dir),
        "name": path.name,
        "size_bytes": int(stat.st_size),
        "size_mb": round(stat.st_size / (1024 * 1024), 4),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "timestamped": bool(timestamp_info["timestamped"]),
        "timestamp_token": timestamp_info["timestamp_token"],
        "timestamp": timestamp_info["timestamp"].isoformat() if timestamp_info["timestamp"] else None,
        "family_key": str(timestamp_info["family_key"]),
        "family_label": str(timestamp_info["family_label"]),
    }


def _list_output_files(output_dir: Path) -> list[Path]:
    if not output_dir.exists():
        return []

    files = []
    for dirpath, _, filenames in os.walk(output_dir, onerror=lambda _: None):
        dir_path = Path(dirpath)
        for filename in filenames:
            if filename in PROTECTED_FILENAMES:
                continue
            files.append(dir_path / filename)
    return sorted(files, key=lambda path: str(path))


def _pick_latest_family_files(entries: Sequence[dict]) -> dict[str, dict]:
    latest = {}
    for entry in entries:
        if not entry.get("timestamped"):
            continue

        key = str(entry.get("family_key") or "")
        previous = latest.get(key)
        current_sort_key = (
            str(entry.get("timestamp") or ""),
            str(entry.get("modified_at") or ""),
            str(entry.get("relative_path") or ""),
        )
        previous_sort_key = (
            str(previous.get("timestamp") or ""),
            str(previous.get("modified_at") or ""),
            str(previous.get("relative_path") or ""),
        ) if previous else None
        if previous is None or current_sort_key > previous_sort_key:
            latest[key] = entry
    return latest


def _build_candidate_groups(
    candidate_entries: Sequence[dict],
    latest_by_family: dict[str, dict],
    *,
    include_sample_files: bool = True,
) -> list[dict]:
    grouped = defaultdict(list)
    for entry in candidate_entries:
        grouped[str(entry.get("family_key") or "")].append(entry)

    groups = []
    for family_key, rows in grouped.items():
        sorted_rows = sorted(
            rows,
            key=lambda item: (
                str(item.get("timestamp") or ""),
                str(item.get("relative_path") or ""),
            ),
        )
        latest_protected = latest_by_family.get(family_key) or {}
        total_size_bytes = sum(int(item.get("size_bytes") or 0) for item in sorted_rows)
        groups.append(
            {
                "family_key": family_key,
                "label": str(rows[0].get("family_label") or family_key),
                "candidate_file_count": len(sorted_rows),
                "candidate_size_bytes": total_size_bytes,
                "candidate_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "oldest_candidate_timestamp": sorted_rows[0].get("timestamp"),
                "newest_candidate_timestamp": sorted_rows[-1].get("timestamp"),
                "latest_protected_path": latest_protected.get("relative_path"),
                "latest_protected_timestamp": latest_protected.get("timestamp"),
                "sample_files": (
                    [
                        {
                            "relative_path": item.get("relative_path"),
                            "size_bytes": item.get("size_bytes"),
                            "timestamp": item.get("timestamp"),
                        }
                        for item in sorted_rows[:10]
                    ]
                    if include_sample_files
                    else []
                ),
            }
        )

    groups.sort(
        key=lambda item: (
            -int(item.get("candidate_size_bytes") or 0),
            -int(item.get("candidate_file_count") or 0),
            str(item.get("family_key") or ""),
        )
    )
    return groups


def _build_findings(summary: dict) -> list[str]:
    findings = [
        (
            f"输出目录共 {summary.get('total_file_count', 0)} 个文件，"
            f"其中带时间戳的历史产物 {summary.get('timestamped_file_count', 0)} 个。"
        ),
        (
            f"默认按产物家族保护最新文件 {summary.get('protected_latest_file_count', 0)} 个；"
            f"当前可进入维护窗口的候选文件 {summary.get('candidate_file_count', 0)} 个，"
            f"累计约 {summary.get('candidate_size_mb', 0)} MB。"
        ),
    ]

    if int(summary.get("candidate_file_count", 0) or 0) > 0:
        findings.append(
            "候选范围仅包含超过保留窗口的时间戳产物；未带时间戳的当前工作簿、跟踪表和占位文件默认不会被删除。"
        )
    else:
        findings.append("当前没有发现需要清理的历史筛选输出产物。")
    return findings


def build_screening_output_cleanup_preview_payload(
    *,
    output_dir: Optional[str | Path] = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    keep_latest_per_family: bool = DEFAULT_KEEP_LATEST_PER_FAMILY,
    max_sample_files: int = 20,
    detail_level: str = "full",
    now: Optional[datetime] = None,
) -> dict:
    """构建筛选输出目录 cleanup dry-run 预演载荷。"""
    artifact_dir = _resolve_output_artifact_dir(output_dir)
    effective_now = now or datetime.now()
    normalized_retention_days = _normalize_positive_int(retention_days, DEFAULT_RETENTION_DAYS)
    normalized_sample_limit = _normalize_positive_int(max_sample_files, 20)
    normalized_detail_level = _normalize_preview_detail_level(detail_level)
    include_samples = normalized_detail_level != "overview"
    cutoff_dt = effective_now - timedelta(days=normalized_retention_days)

    total_file_count = 0
    total_size_bytes = 0
    timestamped_entries = []
    timestamped_family_keys = set()
    for path in _list_output_files(artifact_dir):
        try:
            stat = path.stat()
        except OSError:
            continue

        total_file_count += 1
        total_size_bytes += int(stat.st_size)
        timestamp_info = _extract_timestamp_context(path)
        if not timestamp_info.get("timestamped"):
            continue

        family_key = str(timestamp_info.get("family_key") or "")
        timestamped_family_keys.add(family_key)
        timestamped_entries.append({
            "relative_path": _to_relative_output_path(path, artifact_dir),
            "name": path.name,
            "size_bytes": int(stat.st_size),
            "size_mb": round(stat.st_size / (1024 * 1024), 4),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "timestamped": True,
            "timestamp_token": timestamp_info.get("timestamp_token"),
            "timestamp": timestamp_info["timestamp"].isoformat() if timestamp_info.get("timestamp") else None,
            "family_key": family_key,
            "family_label": str(timestamp_info.get("family_label") or ""),
        })

    latest_by_family = _pick_latest_family_files(timestamped_entries) if keep_latest_per_family else {}
    protected_latest_paths = {
        str(item.get("relative_path") or "")
        for item in latest_by_family.values()
        if str(item.get("relative_path") or "").strip()
    }

    candidate_entries = []
    for entry in timestamped_entries:
        timestamp_value = entry.get("timestamp")
        timestamp_dt = datetime.fromisoformat(timestamp_value) if timestamp_value else None
        if timestamp_dt is None or timestamp_dt >= cutoff_dt:
            continue
        if keep_latest_per_family and str(entry.get("relative_path") or "") in protected_latest_paths:
            continue
        candidate_entries.append(entry)

    candidate_entries.sort(
        key=lambda item: (
            str(item.get("timestamp") or ""),
            str(item.get("relative_path") or ""),
        )
    )

    candidate_groups = _build_candidate_groups(
        candidate_entries,
        latest_by_family,
        include_sample_files=include_samples,
    )
    candidate_file_paths = [str(item.get("relative_path") or "") for item in candidate_entries]
    candidate_size_bytes = sum(int(item.get("size_bytes") or 0) for item in candidate_entries)
    latest_protected_samples = sorted(
        latest_by_family.values(),
        key=lambda item: (
            str(item.get("timestamp") or ""),
            str(item.get("relative_path") or ""),
        ),
        reverse=True,
    )

    summary = {
        "total_file_count": total_file_count,
        "total_size_bytes": total_size_bytes,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        "timestamped_file_count": len(timestamped_entries),
        "timestamped_family_count": len(timestamped_family_keys),
        "protected_latest_file_count": len(protected_latest_paths),
        "candidate_file_count": len(candidate_entries),
        "candidate_family_count": len(candidate_groups),
        "candidate_size_bytes": candidate_size_bytes,
        "candidate_size_mb": round(candidate_size_bytes / (1024 * 1024), 2),
        "oldest_candidate_timestamp": candidate_entries[0].get("timestamp") if candidate_entries else None,
        "newest_candidate_timestamp": candidate_entries[-1].get("timestamp") if candidate_entries else None,
        "cutoff_timestamp": cutoff_dt.isoformat(),
    }

    return {
        "generated_at": effective_now.isoformat(),
        "dry_run": True,
        "output_dir": str(artifact_dir),
        "parameters": {
            "retention_days": normalized_retention_days,
            "keep_latest_per_family": bool(keep_latest_per_family),
            "max_sample_files": normalized_sample_limit,
        },
        "summary": summary,
        "findings": _build_findings(summary),
        "candidate_groups": candidate_groups,
        "candidate_file_samples": candidate_entries[:normalized_sample_limit] if include_samples else [],
        "latest_protected_samples": latest_protected_samples[:normalized_sample_limit] if include_samples else [],
        "execution_plan": {
            "dry_run": True,
            "target_dir": str(artifact_dir),
            "scope_file_total": len(candidate_file_paths),
            "scope_files": candidate_file_paths if include_samples else [],
            "guardrails": [
                "仅纳入带时间戳且超过保留窗口的历史输出文件。",
                "默认保护每个产物家族的最新文件。",
                "未带时间戳的长期跟踪工作簿和占位文件默认不会被删除。",
                "真实删除后将无法从输出目录恢复历史报告，执行前应先完成人工复核。",
            ],
        },
    }


def render_screening_output_cleanup_preview_markdown(payload: dict) -> str:
    """渲染 Markdown 预演报告。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    candidate_groups = payload.get("candidate_groups") or []
    execution_plan = payload.get("execution_plan") or {}

    lines = [
        "# screening output cleanup preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- output_dir: {payload.get('output_dir')}",
        f"- retention_days: {parameters.get('retention_days')}",
        f"- keep_latest_per_family: {parameters.get('keep_latest_per_family')}",
        "",
        "## summary",
        f"- total_file_count: {summary.get('total_file_count', 0)}",
        f"- timestamped_file_count: {summary.get('timestamped_file_count', 0)}",
        f"- protected_latest_file_count: {summary.get('protected_latest_file_count', 0)}",
        f"- candidate_file_count: {summary.get('candidate_file_count', 0)}",
        f"- candidate_family_count: {summary.get('candidate_family_count', 0)}",
        f"- candidate_size_mb: {summary.get('candidate_size_mb', 0)}",
        f"- cutoff_timestamp: {summary.get('cutoff_timestamp')}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## candidate_groups"])
    if not candidate_groups:
        lines.append("- 无候选")
    else:
        for item in candidate_groups[:20]:
            lines.append(
                "- " + json.dumps(
                    {
                        "label": item.get("label"),
                        "candidate_file_count": item.get("candidate_file_count"),
                        "candidate_size_mb": item.get("candidate_size_mb"),
                        "latest_protected_path": item.get("latest_protected_path"),
                        "sample_files": item.get("sample_files"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## execution_plan"])
    lines.append(f"- target_dir: {execution_plan.get('target_dir')}")
    lines.append(f"- scope_file_total: {execution_plan.get('scope_file_total', 0)}")
    lines.append("- guardrails:")
    for item in execution_plan.get("guardrails") or []:
        lines.append(f"  - {item}")
    return "\n".join(lines) + "\n"


def write_screening_output_cleanup_preview_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, Path]:
    """写出 JSON + Markdown 产物。"""
    artifact_dir = _resolve_audit_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"screening_output_cleanup_preview_{timestamp}.json"
    markdown_path = artifact_dir / f"screening_output_cleanup_preview_{timestamp}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_screening_output_cleanup_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path
