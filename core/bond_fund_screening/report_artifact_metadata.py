#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告产物 sidecar 元数据工具。

目标：
1. 为派生产物补充显式运行链路；
2. 避免报告页仅依赖文件时间窗猜测绑定 screening_runs；
3. 在无法解析单一 run_id 时，保留多源 run_id 审计上下文。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from bond_fund_screening.config import OUTPUT_DIR
from utils.core_csv_storage import CoreCsvStorageError, artifact_is_file, has_stored_csv
from bond_fund_screening.report_catalog import (
    atomic_write_json_file,
    rebuild_report_manifest,
)


REPORT_META_FILE_SUFFIX = "_report_meta.json"
_CONTAINER_OUTPUT_DIR = Path("/app/bond_fund_screening/output")


def _resolve_output_base_dir(output_dir: str | Path | None = None) -> Path:
    """返回 sidecar 路径字段默认使用的输出根目录。"""
    return Path(output_dir) if output_dir else Path(OUTPUT_DIR)


def serialize_output_path_reference(
    file_path: str | Path | None,
    *,
    output_dir: str | Path | None = None,
) -> str | None:
    """将 OUTPUT_DIR 内路径序列化为相对路径，避免写入容器绝对路径。"""
    if not file_path:
        return None

    base_dir = _resolve_output_base_dir(output_dir)
    path_obj = Path(file_path)
    try:
        relative_path = path_obj.resolve().relative_to(base_dir.resolve())
        return relative_path.as_posix()
    except (OSError, ValueError):
        return str(path_obj)


def resolve_output_path_reference(
    file_path: str | Path | None,
    *,
    output_dir: str | Path | None = None,
) -> Path | None:
    """解析 sidecar 中的相对/宿主机绝对/容器绝对路径到当前环境可访问路径。"""
    if not file_path:
        return None

    raw_text = str(file_path).strip()
    if not raw_text:
        return None

    base_dir = _resolve_output_base_dir(output_dir)
    raw_path = Path(raw_text)

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
        try:
            relative_to_container = raw_path.relative_to(_CONTAINER_OUTPUT_DIR)
        except ValueError:
            relative_to_container = None
        if relative_to_container is not None:
            candidates.append(base_dir / relative_to_container)
    else:
        candidates.append(base_dir / raw_path)

    seen = set()
    normalized_candidates = []
    for candidate in candidates:
        normalized = str(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(candidate)

    for candidate in normalized_candidates:
        if candidate.exists() or artifact_is_file(candidate):
            return candidate

    return normalized_candidates[0] if normalized_candidates else None


def _normalize_extra_meta_paths(value, *, key: str | None = None, output_dir: str | Path | None = None):
    """按键名约定把 sidecar 额外元数据中的路径字段序列化为相对路径。"""
    if isinstance(value, dict):
        return {
            item_key: _normalize_extra_meta_paths(
                item_value,
                key=str(item_key),
                output_dir=output_dir,
            )
            for item_key, item_value in value.items()
        }

    if isinstance(value, list):
        if key == "derived_from_files":
            return [
                serialize_output_path_reference(item, output_dir=output_dir)
                for item in value
                if item
            ]
        return [
            _normalize_extra_meta_paths(item, key=key, output_dir=output_dir)
            for item in value
        ]

    normalized_key = str(key or "").strip().lower()
    if normalized_key.endswith(("_file", "_csv", "_path")) and value:
        return serialize_output_path_reference(value, output_dir=output_dir)

    return value


def resolve_report_meta_path(report_file: str | Path) -> Path:
    """根据报告文件路径返回 sidecar 元数据路径。"""
    report_path = Path(report_file)
    return report_path.parent / f"{report_path.stem}{REPORT_META_FILE_SUFFIX}"


def load_report_meta(report_file: str | Path) -> dict | None:
    """读取报告 sidecar 元数据。"""
    meta_path = resolve_report_meta_path(report_file)
    if not meta_path.exists():
        return None

    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None

    return payload if isinstance(payload, dict) else None


def _load_json_dict(json_file: str | Path) -> dict | None:
    """读取 JSON 文件并确保返回 dict。"""
    json_path = Path(json_file)
    if not json_path.exists():
        return None

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None

    return payload if isinstance(payload, dict) else None


def _dedupe_str_list(values: Iterable) -> list[str]:
    """去重并保留顺序。"""
    seen = set()
    results: list[str] = []
    for value in values or []:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


def _normalize_source_run_entry(entry: dict, *, fallback_source_file: Path | None = None) -> dict | None:
    """标准化 source_runs 中的单项结构。"""
    if not isinstance(entry, dict):
        return None

    run_id = str(entry.get("run_id") or "").strip()
    if not run_id:
        return None

    normalized = {
        "run_id": run_id,
        "fund_type": entry.get("fund_type"),
        "fund_type_name": entry.get("fund_type_name"),
        "period": entry.get("period"),
        "period_years": entry.get("period_years"),
        "period_days": entry.get("period_days"),
        "source_kind": entry.get("source_kind"),
        "source_file": entry.get("source_file"),
        "stats_file": entry.get("stats_file"),
        "report_file": entry.get("report_file"),
        "report_filename": entry.get("report_filename"),
    }

    if fallback_source_file is not None and not normalized["source_file"]:
        normalized["source_file"] = str(fallback_source_file)

    return normalized


def _dedupe_source_runs(source_runs: Iterable[dict]) -> list[dict]:
    """按 run_id 去重 source_runs。"""
    deduped: list[dict] = []
    seen = set()
    for entry in source_runs or []:
        normalized = _normalize_source_run_entry(entry)
        if not normalized:
            continue
        run_id = normalized["run_id"]
        if run_id in seen:
            continue
        seen.add(run_id)
        deduped.append(normalized)
    return deduped


def collect_source_runs_from_stats_file(
    stats_file: str | Path,
    *,
    source_file: str | Path | None = None,
    source_kind: str = "period_simple_csv",
) -> list[dict]:
    """从 analyzer stats.json 中提取 run 绑定。"""
    stats_path = Path(stats_file)
    if not stats_path.exists():
        return []

    try:
        payload = json.loads(stats_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []

    if not isinstance(payload, dict):
        return []

    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        artifact_meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
        run_id = str(artifact_meta.get("run_id") or "").strip()
    if not run_id:
        return []

    source_path = Path(source_file) if source_file else None
    return _dedupe_source_runs([
        {
            "run_id": run_id,
            "fund_type": payload.get("fund_type"),
            "fund_type_name": payload.get("fund_type_name"),
            "period": payload.get("period"),
            "period_years": payload.get("period_years"),
            "period_days": payload.get("period_days"),
            "source_kind": source_kind,
            "source_file": str(source_path) if source_path else None,
            "stats_file": str(stats_path),
        }
    ])


def collect_source_runs_from_period_simple_csv(csv_file: str | Path) -> list[dict]:
    """从 analyzer 产出的 simple.csv 对应 stats.json 中提取 run 绑定。"""
    csv_path = Path(csv_file)
    stats_path = csv_path.with_name(csv_path.name.replace("_simple.csv", "_stats.json"))
    return collect_source_runs_from_stats_file(
        stats_path,
        source_file=csv_path,
        source_kind="period_simple_csv",
    )


def collect_source_runs_from_report_meta(report_file: str | Path) -> list[dict]:
    """从报告 sidecar 中提取显式 run 绑定，可兼容单 run 与多源 run。"""
    report_path = Path(report_file)
    payload = load_report_meta(report_path)
    if not payload:
        return []

    source_runs = payload.get("source_runs")
    if isinstance(source_runs, list):
        normalized = []
        for entry in source_runs:
            normalized_entry = _normalize_source_run_entry(entry, fallback_source_file=report_path)
            if normalized_entry:
                normalized.append(normalized_entry)
        if normalized:
            return _dedupe_source_runs(normalized)

    source_run_ids = payload.get("source_run_ids")
    if isinstance(source_run_ids, list):
        normalized = []
        for run_id in _dedupe_str_list(source_run_ids):
            resolved_report_file = resolve_output_path_reference(
                payload.get("report_file"),
                output_dir=report_path.parent,
            )
            normalized.append(
                {
                    "run_id": run_id,
                    "fund_type": payload.get("fund_type"),
                    "fund_type_name": payload.get("fund_type_name"),
                    "period": payload.get("period"),
                    "period_years": payload.get("period_years"),
                    "period_days": payload.get("period_days"),
                    "source_kind": payload.get("artifact_type") or "report_file",
                    "source_file": str(report_path),
                    "report_file": str(resolved_report_file or report_path),
                    "report_filename": payload.get("report_filename") or report_path.name,
                }
            )
        if normalized:
            return _dedupe_source_runs(normalized)

    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        return []

    resolved_report_file = resolve_output_path_reference(
        payload.get("report_file"),
        output_dir=report_path.parent,
    )
    return _dedupe_source_runs([
        {
            "run_id": run_id,
            "fund_type": payload.get("fund_type"),
            "fund_type_name": payload.get("fund_type_name"),
            "period": payload.get("period"),
            "period_years": payload.get("period_years"),
            "period_days": payload.get("period_days"),
            "source_kind": payload.get("artifact_type") or "report_file",
            "source_file": str(report_path),
            "report_file": str(resolved_report_file or report_path),
            "report_filename": payload.get("report_filename") or report_path.name,
        }
    ])


def write_derived_report_meta(
    report_file: str | Path,
    *,
    artifact_type: str,
    source_runs: Iterable[dict] | None = None,
    derived_from_files: Iterable[str | Path] | None = None,
    generated_at: Optional[datetime] = None,
    extra_meta: Optional[dict] = None,
    refresh_manifest: bool = True,
) -> Path:
    """为派生 Excel 报告写出 sidecar 元数据。"""
    report_path = Path(report_file)
    meta_path = resolve_report_meta_path(report_path)
    output_dir = report_path.parent
    normalized_runs = _dedupe_source_runs(source_runs or [])
    payload = {
        "artifact_type": artifact_type,
        "generated_at": (generated_at or datetime.now()).isoformat(),
        "report_file": serialize_output_path_reference(report_path, output_dir=output_dir),
        "report_filename": report_path.name,
        "source_runs": normalized_runs,
        "source_run_ids": [entry["run_id"] for entry in normalized_runs],
        "source_run_count": len(normalized_runs),
        "derived_from_files": [
            serialize_output_path_reference(path, output_dir=output_dir)
            for path in (derived_from_files or [])
            if path
        ],
    }

    if extra_meta:
        payload.update(
            _normalize_extra_meta_paths(extra_meta, output_dir=output_dir)
        )

    atomic_write_json_file(meta_path, payload)
    if refresh_manifest:
        rebuild_report_manifest(report_path.parent)
    return meta_path


def backfill_report_meta_from_stats_file(
    report_file: str | Path,
    *,
    stats_file: str | Path | None = None,
    overwrite: bool = False,
    refresh_manifest: bool = True,
) -> Path | None:
    """基于 analyzer stats.json 为单 run 正式报告回填 sidecar。"""
    report_path = Path(report_file)
    meta_path = resolve_report_meta_path(report_path)

    if meta_path.exists() and not overwrite:
        return meta_path

    stats_path = Path(stats_file) if stats_file else report_path.with_name(f"{report_path.stem}_stats.json")
    stats_payload = _load_json_dict(stats_path)
    if not stats_payload:
        return None

    artifact_meta = stats_payload.get("artifact_meta") if isinstance(stats_payload.get("artifact_meta"), dict) else {}
    run_id = str(stats_payload.get("run_id") or artifact_meta.get("run_id") or "").strip()
    if not run_id:
        return None

    output_files = dict(stats_payload.get("output_files") or {})
    output_dir = report_path.parent
    meta_payload = {
        "artifact_type": "period_screening_report",
        "generated_at": (
            stats_payload.get("generated_at")
            or artifact_meta.get("generated_at")
            or datetime.now().isoformat()
        ),
        "run_id": run_id,
        "fund_type": stats_payload.get("fund_type") or artifact_meta.get("fund_type"),
        "fund_type_name": stats_payload.get("fund_type_name") or artifact_meta.get("fund_type_name"),
        "period": stats_payload.get("period") or artifact_meta.get("period"),
        "period_output_name": stats_payload.get("period_output_name") or artifact_meta.get("period_output_name"),
        "period_years": stats_payload.get("period_years") or artifact_meta.get("period_years"),
        "period_days": stats_payload.get("period_days") or artifact_meta.get("period_days"),
        "top_n": stats_payload.get("top_n") or artifact_meta.get("top_n"),
        "requested_top_n": stats_payload.get("requested_top_n") or artifact_meta.get("requested_top_n"),
        "actual_output_count": (
            stats_payload.get("actual_output_count")
            or artifact_meta.get("actual_output_count")
            or stats_payload.get("top_n")
        ),
        "base_filename": stats_payload.get("base_filename") or artifact_meta.get("base_filename"),
        "report_file": serialize_output_path_reference(report_path, output_dir=output_dir),
        "report_filename": report_path.name,
        "stats_file": serialize_output_path_reference(stats_path, output_dir=output_dir),
        "csv_file": (
            serialize_output_path_reference(
                stats_payload.get("csv_file")
                or output_files.get("csv")
                or artifact_meta.get("csv_file"),
                output_dir=output_dir,
            )
        ),
        "simple_csv_file": (
            serialize_output_path_reference(
                stats_payload.get("simple_csv_file")
                or output_files.get("simple_csv")
                or artifact_meta.get("simple_csv_file"),
                output_dir=output_dir,
            )
        ),
    }

    atomic_write_json_file(meta_path, meta_payload)

    output_files["excel"] = str(report_path)
    output_files["report_meta"] = str(meta_path)
    stats_payload["output_files"] = output_files
    stats_payload["excel_file"] = str(report_path)
    stats_payload["report_file"] = str(report_path)
    stats_payload["report_meta_file"] = str(meta_path)

    if artifact_meta:
        artifact_meta.update({
            "report_file": str(report_path),
            "report_meta_file": str(meta_path),
        })
        stats_payload["artifact_meta"] = artifact_meta

    atomic_write_json_file(stats_path, stats_payload)
    if refresh_manifest:
        rebuild_report_manifest(report_path.parent)
    return meta_path


def rename_report_with_sidecar(
    old_report_file: str | Path,
    new_report_file: str | Path,
    *,
    refresh_manifest: bool = True,
) -> tuple[Path, Path | None]:
    """重命名报告文件，并同步重命名/回填其 sidecar 元数据。"""
    old_path = Path(old_report_file)
    new_path = Path(new_report_file)

    if has_stored_csv(old_path) or has_stored_csv(new_path):
        raise CoreCsvStorageError("archived_csv_requires_explicit_restore_before_rename")

    if old_path != new_path and old_path.exists():
        old_path.rename(new_path)

    old_meta_path = resolve_report_meta_path(old_path)
    new_meta_path = resolve_report_meta_path(new_path)
    if not old_meta_path.exists():
        if refresh_manifest:
            rebuild_report_manifest(new_path.parent)
        return new_path, None

    try:
        payload = json.loads(old_meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        payload = {}

    if not isinstance(payload, dict):
        payload = {}

    payload["report_file"] = serialize_output_path_reference(new_path, output_dir=new_path.parent)
    payload["report_filename"] = new_path.name
    atomic_write_json_file(new_meta_path, payload)

    if old_meta_path != new_meta_path and old_meta_path.exists():
        old_meta_path.unlink(missing_ok=True)

    if refresh_manifest:
        rebuild_report_manifest(new_path.parent)
    return new_path, new_meta_path
