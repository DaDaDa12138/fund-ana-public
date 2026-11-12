#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit 目录历史产物清理 dry-run 预演。

目标：
1. 仅扫描 audit 目录顶层条目，避免误删运行期目录结构；
2. 超过保留窗口的时间戳实验目录可直接进入自动清理候选；
3. 顶层时间戳文件默认仅保护每个家族的最新文件；
4. tmp* 草稿条目按更短窗口纳入候选；
5. 全程只读，不执行任何删除。
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TARGET_AUDIT_DIR = PROJECT_ROOT / "audit"
AUDIT_ARTIFACT_CLEANUP_PREVIEW_JSON_PATTERN = "audit_artifact_cleanup_preview_*.json"
DEFAULT_RETENTION_DAYS = 7
DEFAULT_TMP_RETENTION_DAYS = 3
DEFAULT_KEEP_LATEST_FILE_PER_FAMILY = 1
PROTECTED_ENTRY_NAMES = {"__pycache__"}
PROTECTED_ENTRY_PREFIXES = ("audit_artifact_cleanup_",)
TIMESTAMP_SUFFIX_PATTERN = re.compile(r"^(?P<prefix>.+)_(?P<token>\d{8}_\d{6})$")


def load_latest_audit_artifact_cleanup_preview_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 audit artifact cleanup dry-run 预演产物。"""
    return load_latest_json_artifact(
        AUDIT_ARTIFACT_CLEANUP_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def _resolve_target_audit_dir(target_dir: Optional[str | Path]) -> Path:
    resolved = Path(target_dir or DEFAULT_TARGET_AUDIT_DIR)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    if resolved.exists() and not resolved.is_dir():
        raise ValueError(f"audit path is not a directory: {resolved}")
    return resolved


def _resolve_output_dir(output_dir: str) -> Path:
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


def _entry_size_bytes(path: Path) -> int:
    if path.is_file():
        return int(path.stat().st_size)
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += int(child.stat().st_size)
    return total


def _should_skip_entry(path: Path) -> bool:
    if path.name in PROTECTED_ENTRY_NAMES:
        return True
    return any(path.name.startswith(prefix) for prefix in PROTECTED_ENTRY_PREFIXES)


def _classify_entry(path: Path) -> dict:
    name = path.name
    stem = path.stem if path.is_file() else path.name
    is_tmp = name.startswith("tmp")
    timestamp = None
    timestamp_token = ""
    family_key = ""
    family_label = ""

    match = TIMESTAMP_SUFFIX_PATTERN.match(stem)
    if match:
        timestamp_token = match.group("token")
        try:
            timestamp = datetime.strptime(timestamp_token, "%Y%m%d_%H%M%S")
        except ValueError:
            timestamp = None
        prefix = match.group("prefix").rstrip("._- ")
        if path.is_dir():
            family_key = f"dir:{prefix or '<root>'}"
            family_label = prefix or path.name
        else:
            family_key = f"file:{prefix or '<root>'}:{path.suffix.lower() or '<no_ext>'}"
            family_label = prefix or path.name

    if is_tmp:
        return {
            "entry_type": "tmp_entry",
            "entry_label": "临时草稿条目",
            "family_key": family_key or f"tmp:{path.suffix.lower() or '<dir>' if path.is_file() else '<dir>'}",
            "family_label": family_label or "tmp scratch",
            "timestamped": bool(timestamp),
            "timestamp": timestamp,
            "timestamp_token": timestamp_token,
        }

    if timestamp is not None:
        return {
            "entry_type": "timestamped_directory" if path.is_dir() else "timestamped_file",
            "entry_label": "时间戳实验目录" if path.is_dir() else "时间戳审计文件",
            "family_key": family_key,
            "family_label": family_label,
            "timestamped": True,
            "timestamp": timestamp,
            "timestamp_token": timestamp_token,
        }

    return {
        "entry_type": "stable_entry",
        "entry_label": "稳定条目",
        "family_key": f"stable:{name}",
        "family_label": name,
        "timestamped": False,
        "timestamp": None,
        "timestamp_token": "",
    }


def _serialize_entry(path: Path, *, base_dir: Path) -> dict:
    stat = path.stat()
    classified = _classify_entry(path)
    size_bytes = _entry_size_bytes(path)
    return {
        "relative_path": str(path.relative_to(base_dir)),
        "name": path.name,
        "entry_kind": "directory" if path.is_dir() else "file",
        "size_bytes": size_bytes,
        "size_mb": round(size_bytes / (1024 * 1024), 4),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "entry_type": classified["entry_type"],
        "entry_label": classified["entry_label"],
        "family_key": classified["family_key"],
        "family_label": classified["family_label"],
        "timestamped": bool(classified["timestamped"]),
        "timestamp_token": classified["timestamp_token"],
        "timestamp": classified["timestamp"].isoformat() if classified["timestamp"] else None,
    }


def _list_top_level_entries(target_dir: Path) -> list[Path]:
    if not target_dir.exists():
        return []
    entries = []
    for path in sorted(target_dir.iterdir()):
        if _should_skip_entry(path):
            continue
        entries.append(path)
    return entries


def _pick_latest_file_by_family(entries: Sequence[dict]) -> dict[str, dict]:
    latest = {}
    for entry in entries:
        if entry.get("entry_type") != "timestamped_file":
            continue
        key = str(entry.get("family_key") or "")
        sort_key = (
            str(entry.get("timestamp") or ""),
            str(entry.get("modified_at") or ""),
            str(entry.get("relative_path") or ""),
        )
        previous = latest.get(key)
        previous_key = (
            str(previous.get("timestamp") or ""),
            str(previous.get("modified_at") or ""),
            str(previous.get("relative_path") or ""),
        ) if previous else None
        if previous is None or sort_key > previous_key:
            latest[key] = entry
    return latest


def _build_candidate_groups(
    candidate_entries: Sequence[dict],
    latest_files_by_family: dict[str, dict],
) -> list[dict]:
    grouped = defaultdict(list)
    for entry in candidate_entries:
        grouped[str(entry.get("family_key") or "")].append(entry)

    groups = []
    for family_key, rows in grouped.items():
        sorted_rows = sorted(
            rows,
            key=lambda item: (
                str(item.get("timestamp") or item.get("modified_at") or ""),
                str(item.get("relative_path") or ""),
            ),
        )
        latest_protected = latest_files_by_family.get(family_key) or {}
        total_size_bytes = sum(int(item.get("size_bytes") or 0) for item in sorted_rows)
        groups.append(
            {
                "family_key": family_key,
                "label": str(sorted_rows[0].get("family_label") or family_key),
                "entry_type": str(sorted_rows[0].get("entry_type") or ""),
                "candidate_entry_count": len(sorted_rows),
                "candidate_size_bytes": total_size_bytes,
                "candidate_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "oldest_candidate_timestamp": sorted_rows[0].get("timestamp") or sorted_rows[0].get("modified_at"),
                "newest_candidate_timestamp": sorted_rows[-1].get("timestamp") or sorted_rows[-1].get("modified_at"),
                "latest_protected_path": latest_protected.get("relative_path"),
                "sample_entries": [
                    {
                        "relative_path": item.get("relative_path"),
                        "entry_kind": item.get("entry_kind"),
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
            -int(item.get("candidate_entry_count") or 0),
            str(item.get("family_key") or ""),
        )
    )
    return groups


def _build_findings(summary: dict) -> list[str]:
    findings = [
        (
            f"audit 顶层共 {summary.get('total_entry_count', 0)} 个条目，"
            f"累计约 {summary.get('total_size_mb', 0)} MB。"
        ),
        (
            f"其中时间戳实验目录 {summary.get('timestamped_directory_count', 0)} 个、"
            f"时间戳审计文件 {summary.get('timestamped_file_count', 0)} 个、"
            f"tmp 草稿条目 {summary.get('tmp_entry_count', 0)} 个。"
        ),
        (
            f"当前候选条目 {summary.get('candidate_entry_count', 0)} 个，"
            f"累计约 {summary.get('candidate_size_mb', 0)} MB。"
        ),
    ]
    if int(summary.get("candidate_entry_count", 0) or 0) > 0:
        findings.append(
            "自动候选仅包含超过保留窗口的时间戳实验目录、家族内非最新的历史文件，以及过期 tmp 草稿条目。"
        )
    else:
        findings.append("当前没有发现可直接进入维护窗口的 audit 历史产物。")
    return findings


def build_audit_artifact_cleanup_preview_payload(
    *,
    target_dir: Optional[str | Path] = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    tmp_retention_days: int = DEFAULT_TMP_RETENTION_DAYS,
    keep_latest_file_per_family: int = DEFAULT_KEEP_LATEST_FILE_PER_FAMILY,
    max_sample_entries: int = 20,
    now: Optional[datetime] = None,
) -> dict:
    """构建 audit artifact cleanup dry-run 预演载荷。"""
    resolved_target_dir = _resolve_target_audit_dir(target_dir)
    effective_now = now or datetime.now()
    normalized_retention_days = _normalize_positive_int(retention_days, DEFAULT_RETENTION_DAYS)
    normalized_tmp_retention_days = _normalize_positive_int(
        tmp_retention_days,
        DEFAULT_TMP_RETENTION_DAYS,
    )
    normalized_keep_latest = _normalize_positive_int(
        keep_latest_file_per_family,
        DEFAULT_KEEP_LATEST_FILE_PER_FAMILY,
    )
    normalized_sample_limit = _normalize_positive_int(max_sample_entries, 20)
    retention_cutoff = effective_now - timedelta(days=normalized_retention_days)
    tmp_cutoff = effective_now - timedelta(days=normalized_tmp_retention_days)

    entries = [
        _serialize_entry(path, base_dir=resolved_target_dir)
        for path in _list_top_level_entries(resolved_target_dir)
    ]
    latest_files_by_family = _pick_latest_file_by_family(entries)
    protected_latest_paths = {
        str(item.get("relative_path") or "")
        for item in latest_files_by_family.values()
        if str(item.get("relative_path") or "").strip()
    }

    candidate_entries = []
    for entry in entries:
        entry_type = str(entry.get("entry_type") or "")
        relative_path = str(entry.get("relative_path") or "")
        timestamp_value = entry.get("timestamp")
        timestamp_dt = datetime.fromisoformat(timestamp_value) if timestamp_value else None
        modified_at_dt = datetime.fromisoformat(str(entry.get("modified_at")))

        if entry_type == "tmp_entry":
            if modified_at_dt < tmp_cutoff:
                candidate_entries.append(entry)
            continue

        if entry_type == "timestamped_directory":
            if timestamp_dt is not None and timestamp_dt < retention_cutoff:
                candidate_entries.append(entry)
            continue

        if entry_type == "timestamped_file":
            if timestamp_dt is None or timestamp_dt >= retention_cutoff:
                continue
            if normalized_keep_latest > 0 and relative_path in protected_latest_paths:
                continue
            candidate_entries.append(entry)

    candidate_entries.sort(
        key=lambda item: (
            str(item.get("timestamp") or item.get("modified_at") or ""),
            str(item.get("relative_path") or ""),
        )
    )

    candidate_groups = _build_candidate_groups(candidate_entries, latest_files_by_family)
    candidate_size_bytes = sum(int(item.get("size_bytes") or 0) for item in candidate_entries)
    total_size_bytes = sum(int(item.get("size_bytes") or 0) for item in entries)
    protected_latest_samples = sorted(
        latest_files_by_family.values(),
        key=lambda item: (
            str(item.get("timestamp") or ""),
            str(item.get("modified_at") or ""),
            str(item.get("relative_path") or ""),
        ),
        reverse=True,
    )

    summary = {
        "total_entry_count": len(entries),
        "total_size_bytes": total_size_bytes,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        "directory_count": sum(1 for item in entries if item.get("entry_kind") == "directory"),
        "file_count": sum(1 for item in entries if item.get("entry_kind") == "file"),
        "timestamped_directory_count": sum(1 for item in entries if item.get("entry_type") == "timestamped_directory"),
        "timestamped_file_count": sum(1 for item in entries if item.get("entry_type") == "timestamped_file"),
        "tmp_entry_count": sum(1 for item in entries if item.get("entry_type") == "tmp_entry"),
        "protected_latest_file_count": len(protected_latest_paths),
        "candidate_entry_count": len(candidate_entries),
        "candidate_directory_count": sum(1 for item in candidate_entries if item.get("entry_kind") == "directory"),
        "candidate_file_count": sum(1 for item in candidate_entries if item.get("entry_kind") == "file"),
        "candidate_size_bytes": candidate_size_bytes,
        "candidate_size_mb": round(candidate_size_bytes / (1024 * 1024), 2),
        "oldest_candidate_timestamp": candidate_entries[0].get("timestamp") if candidate_entries else None,
        "newest_candidate_timestamp": candidate_entries[-1].get("timestamp") if candidate_entries else None,
        "retention_cutoff": retention_cutoff.isoformat(),
        "tmp_retention_cutoff": tmp_cutoff.isoformat(),
    }

    return {
        "generated_at": effective_now.isoformat(),
        "dry_run": True,
        "target_dir": str(resolved_target_dir),
        "parameters": {
            "retention_days": normalized_retention_days,
            "tmp_retention_days": normalized_tmp_retention_days,
            "keep_latest_file_per_family": normalized_keep_latest,
            "max_sample_entries": normalized_sample_limit,
        },
        "summary": summary,
        "findings": _build_findings(summary),
        "candidate_groups": candidate_groups,
        "candidate_entry_samples": candidate_entries[:normalized_sample_limit],
        "protected_latest_file_samples": protected_latest_samples[:normalized_sample_limit],
        "execution_plan": {
            "dry_run": True,
            "target_dir": str(resolved_target_dir),
            "scope_entry_total": len(candidate_entries),
            "scope_entries": [
                {
                    "relative_path": str(item.get("relative_path") or ""),
                    "entry_kind": str(item.get("entry_kind") or ""),
                }
                for item in candidate_entries
            ],
            "guardrails": [
                "仅扫描 audit 顶层条目，不递归拆分目录内部文件作为独立候选。",
                "时间戳实验目录只有超过保留窗口后才会进入自动清理范围。",
                "时间戳审计文件默认保护每个家族的最新文件。",
                "tmp 草稿条目按更短保留窗口进入候选，稳定条目默认不会自动删除。",
            ],
        },
    }


def render_audit_artifact_cleanup_preview_markdown(payload: dict) -> str:
    """渲染 Markdown 预演报告。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    candidate_groups = payload.get("candidate_groups") or []
    execution_plan = payload.get("execution_plan") or {}

    lines = [
        "# audit artifact cleanup preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- target_dir: {payload.get('target_dir')}",
        f"- retention_days: {parameters.get('retention_days')}",
        f"- tmp_retention_days: {parameters.get('tmp_retention_days')}",
        f"- keep_latest_file_per_family: {parameters.get('keep_latest_file_per_family')}",
        "",
        "## summary",
        f"- total_entry_count: {summary.get('total_entry_count', 0)}",
        f"- total_size_mb: {summary.get('total_size_mb', 0)}",
        f"- timestamped_directory_count: {summary.get('timestamped_directory_count', 0)}",
        f"- timestamped_file_count: {summary.get('timestamped_file_count', 0)}",
        f"- tmp_entry_count: {summary.get('tmp_entry_count', 0)}",
        f"- candidate_entry_count: {summary.get('candidate_entry_count', 0)}",
        f"- candidate_size_mb: {summary.get('candidate_size_mb', 0)}",
        f"- retention_cutoff: {summary.get('retention_cutoff')}",
        f"- tmp_retention_cutoff: {summary.get('tmp_retention_cutoff')}",
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
                        "entry_type": item.get("entry_type"),
                        "candidate_entry_count": item.get("candidate_entry_count"),
                        "candidate_size_mb": item.get("candidate_size_mb"),
                        "latest_protected_path": item.get("latest_protected_path"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## execution_plan"])
    lines.append(f"- scope_entry_total: {execution_plan.get('scope_entry_total', 0)}")
    lines.append("- guardrails:")
    for item in execution_plan.get("guardrails") or []:
        lines.append(f"  - {item}")

    return "\n".join(lines).strip() + "\n"


def write_audit_artifact_cleanup_preview_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, Path]:
    """写出 JSON + Markdown 产物。"""
    artifact_dir = _resolve_output_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"audit_artifact_cleanup_preview_{timestamp}.json"
    markdown_path = artifact_dir / f"audit_artifact_cleanup_preview_{timestamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_audit_artifact_cleanup_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path
