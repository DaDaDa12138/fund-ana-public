#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
正式报告目录编目与 manifest 工具。

目标：
1. 统一扫描正式报告目录的元数据口径；
2. 为新生成的正式报告维护完整 manifest，避免 API 每次全量扫描；
3. 保持 sidecar / manifest 写入原子化，降低半写入状态被 API 读到的概率。
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from utils.core_csv_storage import artifact_is_file, glob_artifacts, original_artifact_stat


REPORT_META_FILE_SUFFIX = "_report_meta.json"
REPORT_MANIFEST_DIRNAME = ".catalog"
REPORT_MANIFEST_FILENAME = "report_artifact_manifest.json"
REPORT_MANIFEST_VERSION = 4
_REPORT_MANIFEST_DIR_MTIME_EPSILON = 1e-6
SUPPORTED_REPORT_EXTENSIONS = {".xlsx", ".csv", ".json"}
PRIMARY_PREVIEW_EXTENSIONS = {".xlsx"}
JSON_ARTIFACT_FILE_MODE = 0o644
EXCLUDED_FILENAMES = {".gitkeep"}
EXCLUDED_SUFFIXES = {REPORT_META_FILE_SUFFIX}
INCLUDED_JSON_SUFFIXES = {"_stats.json"}


def is_user_visible_report_file(report_file: str | Path) -> bool:
    """判断文件是否属于用户可见的正式报告产物。"""
    report_path = Path(report_file)
    filename = report_path.name
    suffix = report_path.suffix.lower()

    if filename in EXCLUDED_FILENAMES:
        return False
    if suffix not in SUPPORTED_REPORT_EXTENSIONS:
        return False
    if any(filename.endswith(excluded_suffix) for excluded_suffix in EXCLUDED_SUFFIXES):
        return False
    if suffix == ".json" and not any(filename.endswith(allowed_suffix) for allowed_suffix in INCLUDED_JSON_SUFFIXES):
        return False
    return True


def build_report_catalog_id(report_path: str | Path) -> str:
    """生成稳定的目录条目 ID，避免不同后缀产物冲突。"""
    path = Path(report_path)
    suffix = path.suffix.lower().lstrip(".") or "file"
    return f"{path.stem}__{suffix}"


def resolve_report_family_id(report_path: str | Path) -> str:
    """返回同一产物家族的稳定 family_id。"""
    return Path(report_path).stem


def resolve_report_artifact_kind(report_path: str | Path) -> str:
    """根据文件名和后缀识别产物类型。"""
    path = Path(report_path)
    filename = path.name
    suffix = path.suffix.lower()

    if suffix == ".xlsx":
        return "excel_report"
    if suffix == ".json":
        if filename.endswith("_stats.json"):
            return "stats_snapshot"
        return "json_artifact"
    if suffix == ".csv":
        if filename.endswith("_simple.csv"):
            return "simple_csv"
        if "观察池" in filename:
            return "watchlist_csv"
        return "csv_report"
    return "file"


def atomic_write_json_file(json_file: str | Path, payload: dict) -> Path:
    """原子写出 JSON 文件，避免读取方拿到半写入内容。"""
    json_path = Path(json_file)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None

    try:
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(json_path.parent),
            delete=False,
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)

        tmp_path.replace(json_path)
        try:
            os.chmod(json_path, JSON_ARTIFACT_FILE_MODE)
        except OSError:
            pass
        return json_path
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


def format_size(size_bytes):
    """格式化文件大小。"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"


def resolve_report_period_and_type(filename: str) -> tuple[str, str]:
    """从报告文件名解析周期与基金类型。"""
    period = "未知"
    fund_type = "其他"

    if filename.startswith("股票型_") or filename.startswith("股票型基金_"):
        fund_type = "股票型"
    elif filename.startswith("混合型_") or filename.startswith("混合型基金_"):
        fund_type = "混合型"
    elif filename.startswith("债券型_") or filename.startswith("债券基金_") or filename.startswith("债券型基金_"):
        fund_type = "债券型"
    else:
        fund_type_patterns = [
            ("债券型基金", "债券型"),
            ("债券基金", "债券型"),
            ("股票型基金", "股票型"),
            ("股票基金", "股票型"),
            ("混合型基金", "混合型"),
            ("混合基金", "混合型"),
            ("指数型基金", "指数型"),
            ("指数基金", "指数型"),
            ("货币型基金", "货币型"),
            ("货币基金", "货币型"),
            ("定开债", "定开债"),
            ("QDII", "QDII"),
            ("FOF", "FOF"),
            ("ETF", "ETF"),
            ("LOF", "LOF"),
            ("优质股票型基金", "股票型"),
            ("优质混合型基金", "混合型"),
        ]
        for pattern, type_name in fund_type_patterns:
            if pattern in filename:
                fund_type = type_name
                break

    if fund_type == "其他":
        summary_keywords = [
            "优质基金累积跟踪",
            "当日最终优质基金",
            "近期与长期优质基金对比报告",
            "近期优质基金",
            "长期优质基金",
        ]
        if any(keyword in filename for keyword in summary_keywords):
            fund_type = "债券型"

    if "当日最终优质基金" in filename:
        period = "当日精选"
    elif "近期与长期优质基金对比报告" in filename:
        period = "对比报告"
    elif "优质基金累积跟踪" in filename:
        period = "累积跟踪"
    else:
        for report_period in ["一周期", "一月期", "三月期", "半年期", "1年期", "2年期", "3年期", "4年期", "5年期", "多周期"]:
            if report_period in filename:
                period = report_period
                break

    return period, fund_type


def resolve_report_meta_path(report_file: str | Path) -> Path:
    """返回报告 sidecar 路径。"""
    report_path = Path(report_file)
    return report_path.parent / f"{report_path.stem}{REPORT_META_FILE_SUFFIX}"


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


def _extract_source_run_ids(report_meta: dict | None) -> list[str]:
    """从 sidecar 中提取去重后的 run_id 列表。"""
    if not isinstance(report_meta, dict):
        return []

    raw_ids = []
    direct_run_id = str(report_meta.get("run_id") or "").strip()
    if direct_run_id:
        raw_ids.append(direct_run_id)

    source_run_ids = report_meta.get("source_run_ids")
    if isinstance(source_run_ids, list):
        raw_ids.extend(source_run_ids)

    source_runs = report_meta.get("source_runs")
    if isinstance(source_runs, list):
        for entry in source_runs:
            if isinstance(entry, dict):
                raw_ids.append(entry.get("run_id"))

    normalized = []
    seen = set()
    for run_id in raw_ids:
        run_id = str(run_id or "").strip()
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)
        normalized.append(run_id)
    return normalized


def _normalize_catalog_artifact_type(
    *,
    artifact_type: str | None,
    filename: str,
    file_ext: str,
    period: str,
    source_run_count: int,
    lineage_ready: bool,
) -> str | None:
    """统一补齐缺失的报告 artifact_type。"""
    explicit_artifact_type = str(artifact_type or "").strip()
    if explicit_artifact_type:
        return explicit_artifact_type

    normalized_file_ext = str(file_ext or "").strip().lower()
    if normalized_file_ext not in {".xlsx", ".csv"}:
        return None

    if not lineage_ready and int(source_run_count or 0) <= 0:
        return None

    if period not in {"一周期", "一月期", "三月期", "半年期", "1年期", "2年期", "3年期", "4年期", "5年期"}:
        return None

    if "Top" not in filename:
        return None

    summary_keywords = (
        "当日最终优质基金",
        "近期与长期优质基金对比报告",
        "优质基金累积跟踪",
        "详细报告",
        "policy_monitor",
    )
    if any(keyword in filename for keyword in summary_keywords):
        return None

    return "period_screening_report"


def build_report_catalog_entry(
    report_file: str | Path,
    *,
    entry_stat=None,
    report_meta: dict | None = None,
) -> dict | None:
    """构建单个正式报告的目录条目。"""
    report_path = Path(report_file)
    if not is_user_visible_report_file(report_path) or not artifact_is_file(report_path):
        return None

    try:
        file_stat = entry_stat or original_artifact_stat(report_path)
    except OSError:
        return None

    report_meta_path = resolve_report_meta_path(report_path)
    meta_payload = report_meta if isinstance(report_meta, dict) else _load_json_dict(report_meta_path)
    source_run_ids = _extract_source_run_ids(meta_payload)
    final_screening_summary = (meta_payload or {}).get("final_screening_summary")
    if not isinstance(final_screening_summary, dict):
        final_screening_summary = None
    risk_metric_governance = (meta_payload or {}).get("risk_metric_governance")
    if not isinstance(risk_metric_governance, dict):
        risk_metric_governance = None
    accumulation_summary = (meta_payload or {}).get("accumulation_summary")
    if not isinstance(accumulation_summary, dict):
        accumulation_summary = None
    input_lineage = (meta_payload or {}).get("input_lineage")
    if not isinstance(input_lineage, dict):
        input_lineage = None

    input_pair_status = (meta_payload or {}).get("input_pair_status")
    if input_pair_status in (None, "") and input_lineage:
        input_pair_status = input_lineage.get("input_pair_status")
    input_pair_status_label = (meta_payload or {}).get("input_pair_status_label")
    if input_pair_status_label in (None, "") and input_lineage:
        input_pair_status_label = input_lineage.get("input_pair_status_label")
    used_stale_inputs = (meta_payload or {}).get("used_stale_inputs")
    if used_stale_inputs is None and input_lineage:
        used_stale_inputs = input_lineage.get("used_stale_inputs")
    input_lineage_note = (meta_payload or {}).get("input_lineage_note")
    if input_lineage_note in (None, "") and input_lineage:
        input_lineage_note = input_lineage.get("input_lineage_note")
    period, fund_type = resolve_report_period_and_type(report_path.name)
    sidecar_exists = report_meta_path.exists()
    lineage_ready = bool(source_run_ids)
    legacy_unverifiable = not sidecar_exists and not lineage_ready
    if lineage_ready:
        lineage_status = "lineage_ready"
    elif sidecar_exists:
        lineage_status = "sidecar_unbound"
    else:
        lineage_status = "legacy_unverifiable"

    artifact_kind = resolve_report_artifact_kind(report_path)
    file_ext = report_path.suffix.lower()

    return {
        "id": build_report_catalog_id(report_path),
        "family_id": resolve_report_family_id(report_path),
        "filename": report_path.name,
        "file_ext": file_ext,
        "artifact_kind": artifact_kind,
        "is_primary_report": file_ext in PRIMARY_PREVIEW_EXTENSIONS,
        "fund_type": fund_type,
        "period": period,
        "size": file_stat.st_size,
        "size_human": format_size(file_stat.st_size),
        "created_time": datetime.fromtimestamp(file_stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "created_ts": file_stat.st_mtime,
        "sidecar_exists": sidecar_exists,
        "report_meta_file": str(report_meta_path) if sidecar_exists else None,
        "artifact_type": _normalize_catalog_artifact_type(
            artifact_type=(meta_payload or {}).get("artifact_type"),
            filename=report_path.name,
            file_ext=file_ext,
            period=period,
            source_run_count=len(source_run_ids),
            lineage_ready=lineage_ready,
        ),
        "source_run_count": len(source_run_ids),
        "lineage_ready": lineage_ready,
        "legacy_unverifiable": legacy_unverifiable,
        "lineage_status": lineage_status,
        "final_screening_summary": dict(final_screening_summary) if final_screening_summary else None,
        "risk_metric_governance": dict(risk_metric_governance) if risk_metric_governance else None,
        "accumulation_summary": dict(accumulation_summary) if accumulation_summary else None,
        "input_pair_status": input_pair_status,
        "input_pair_status_label": input_pair_status_label,
        "used_stale_inputs": used_stale_inputs,
        "input_lineage_note": input_lineage_note,
        "input_lineage": dict(input_lineage) if input_lineage else None,
    }


def scan_report_catalog_entries(output_dir: str | Path) -> tuple[list[dict], int]:
    """扫描输出目录并构建完整报告目录条目。"""
    output_path = Path(output_dir)
    if not output_path.exists():
        return [], 0

    entries_with_stat = []
    for report_path in glob_artifacts(output_path, "*"):
        if not is_user_visible_report_file(report_path) or not artifact_is_file(report_path):
            continue
        try:
            entry_stat = original_artifact_stat(report_path)
        except OSError:
            continue
        entries_with_stat.append((report_path, entry_stat))

    entries_with_stat.sort(key=lambda item: (item[1].st_mtime, item[0].name), reverse=True)

    reports = []
    for report_path, entry_stat in entries_with_stat:
        entry = build_report_catalog_entry(report_path, entry_stat=entry_stat)
        if entry is not None:
            reports.append(entry)

    return reports, len(entries_with_stat)


def count_user_visible_report_files(output_dir: str | Path) -> int:
    """仅统计目录下用户可见正式报告文件数量。"""
    output_path = Path(output_dir)
    if not output_path.exists():
        return 0

    count = 0
    for report_path in glob_artifacts(output_path, "*"):
        if is_user_visible_report_file(report_path) and artifact_is_file(report_path):
            count += 1
    return count


def resolve_report_manifest_path(output_dir: str | Path) -> Path:
    """返回正式报告 manifest 路径。"""
    output_path = Path(output_dir)
    return output_path / REPORT_MANIFEST_DIRNAME / REPORT_MANIFEST_FILENAME


def load_report_manifest_entries(output_dir: str | Path) -> tuple[list[dict], int] | None:
    """加载 fresh manifest；缺失、损坏或过期时返回 None。"""
    output_path = Path(output_dir)
    manifest_path = resolve_report_manifest_path(output_path)
    payload = _load_json_dict(manifest_path)
    if not payload:
        return None

    try:
        manifest_version = int(payload.get("version"))
    except (TypeError, ValueError):
        return None
    if manifest_version != REPORT_MANIFEST_VERSION:
        return None

    raw_reports = payload.get("reports")
    if not isinstance(raw_reports, list):
        return None

    try:
        stored_dir_mtime = float(payload.get("dir_mtime"))
        current_dir_mtime = float(output_path.stat().st_mtime)
    except (OSError, TypeError, ValueError):
        return None

    if abs(stored_dir_mtime - current_dir_mtime) > _REPORT_MANIFEST_DIR_MTIME_EPSILON:
        return None

    reports = []
    for entry in raw_reports:
        if isinstance(entry, dict):
            normalized_entry = dict(entry)
            normalized_entry["artifact_type"] = _normalize_catalog_artifact_type(
                artifact_type=normalized_entry.get("artifact_type"),
                filename=str(normalized_entry.get("filename") or ""),
                file_ext=str(normalized_entry.get("file_ext") or ""),
                period=str(normalized_entry.get("period") or "未知"),
                source_run_count=int(normalized_entry.get("source_run_count") or 0),
                lineage_ready=bool(normalized_entry.get("lineage_ready")),
            )
            reports.append(normalized_entry)

    total_all = payload.get("total_all")
    try:
        total_all = int(total_all)
    except (TypeError, ValueError):
        total_all = len(reports)

    current_total_all = count_user_visible_report_files(output_path)
    if total_all != current_total_all or len(reports) != current_total_all:
        return None

    return reports, total_all


def rebuild_report_manifest(output_dir: str | Path) -> Path:
    """重建完整正式报告 manifest。"""
    output_path = Path(output_dir)
    manifest_path = resolve_report_manifest_path(output_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    reports, total_all = scan_report_catalog_entries(output_path)
    try:
        dir_mtime = float(output_path.stat().st_mtime)
    except OSError:
        dir_mtime = 0.0

    atomic_write_json_file(
        manifest_path,
        {
            "version": REPORT_MANIFEST_VERSION,
            "generated_at": datetime.now().isoformat(),
            "output_dir": str(output_path),
            "dir_mtime": dir_mtime,
            "total_all": int(total_all),
            "reports": reports,
        },
    )
    return manifest_path


def rebuild_report_manifests_for_paths(*paths: str | Path | None) -> list[Path]:
    """按产物文件所属目录去重后统一重建 manifest。"""
    directories: list[Path] = []
    seen = set()
    for path in paths:
        if not path:
            continue
        directory = Path(path).parent.resolve()
        normalized = str(directory)
        if normalized in seen:
            continue
        seen.add(normalized)
        directories.append(directory)

    rebuilt_paths: list[Path] = []
    for directory in directories:
        rebuilt_paths.append(rebuild_report_manifest(directory))
    return rebuilt_paths
