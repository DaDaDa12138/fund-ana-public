#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选输入文件配对与业务日血缘辅助工具。"""

from __future__ import annotations

import re
from datetime import datetime
from datetime import date as date_type
from pathlib import Path
from utils.core_csv_storage import glob_artifacts, original_artifact_stat


def normalize_report_date(value) -> str | None:
    """将多种日期输入规范化为 YYYY-MM-DD。"""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date_type):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r'20\d{6}', text):
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    if re.fullmatch(r'20\d{2}-\d{2}-\d{2}', text):
        return text
    return None


def extract_report_date(
    report_file: Path,
    *,
    compact_pattern,
    iso_pattern,
    normalize_report_date_fn,
) -> str | None:
    """从详细报告文件名中提取发布日期。"""
    name = str(getattr(report_file, 'name', report_file) or '')
    compact_match = compact_pattern.search(name)
    if compact_match:
        return normalize_report_date_fn(compact_match.group('date'))

    iso_match = iso_pattern.search(name)
    if iso_match:
        return normalize_report_date_fn(iso_match.group('date'))

    return None


def build_input_lineage_meta(
    short_term_file: Path,
    long_term_file: Path,
    *,
    today_date: str | None = None,
    normalize_report_date_fn,
    extract_report_date_fn,
    today_provider=None,
) -> dict:
    """构建最终筛选输入文件的来源血缘信息。"""
    normalized_today = normalize_report_date_fn(today_date)
    if not normalized_today:
        today_provider = today_provider or (lambda: date_type.today().isoformat())
        normalized_today = str(today_provider()).strip()

    short_report_date = extract_report_date_fn(short_term_file)
    long_report_date = extract_report_date_fn(long_term_file)

    if short_report_date and long_report_date:
        if short_report_date == normalized_today and long_report_date == normalized_today:
            input_pair_status = 'current'
            used_stale_inputs = False
            input_pair_status_label = '当日输入'
            input_lineage_note = (
                f"近期与长期输入均来自当日业务日 {normalized_today}，未触发历史回退。"
            )
        elif short_report_date == long_report_date:
            input_pair_status = 'stale'
            used_stale_inputs = True
            input_pair_status_label = '历史回退'
            input_lineage_note = (
                f"近期与长期输入均回退到共同业务日 {short_report_date}，"
                "输出文件生成时间晚于实际输入业务日。"
            )
        else:
            input_pair_status = 'mixed'
            used_stale_inputs = True
            input_pair_status_label = '跨日混用'
            input_lineage_note = (
                f"近期输入业务日为 {short_report_date}，长期输入业务日为 {long_report_date}，"
                "存在跨业务日混用风险。"
            )
    else:
        input_pair_status = 'unknown'
        used_stale_inputs = any(
            report_date and report_date != normalized_today
            for report_date in (short_report_date, long_report_date)
        )
        input_pair_status_label = '待核验'
        input_lineage_note = (
            "无法从输入文件名完整识别近期/长期业务日，请结合原始文件名与生成链路人工核对。"
        )

    return {
        'today_date': normalized_today,
        'short_report_date': short_report_date,
        'long_report_date': long_report_date,
        'input_pair_status': input_pair_status,
        'input_pair_status_label': input_pair_status_label,
        'used_stale_inputs': used_stale_inputs,
        'input_lineage_note': input_lineage_note,
    }


def allow_stale_final_inputs(*, env) -> bool:
    """
    是否允许最终筛选回退到历史产物。

    默认关闭，避免长/短期筛选失败时混用旧文件。
    """
    return str(env.get('ALLOW_STALE_INPUTS', '')).strip().lower() in ('1', 'true', 'yes', 'on')


def find_latest_file(pattern: str, directory: Path) -> Path | None:
    """查找匹配模式的最新文件。"""
    files = sorted(glob_artifacts(directory, pattern), key=lambda p: (original_artifact_stat(p).st_mtime, p.name), reverse=True)
    return files[0] if files else None


def collect_latest_files_by_report_date(
    pattern: str,
    directory: Path,
    *,
    extract_report_date_fn,
) -> dict[str, Path]:
    """按业务日期聚合同类报告文件，并保留每个业务日最新产物。"""
    files = sorted(glob_artifacts(directory, pattern), key=lambda p: (original_artifact_stat(p).st_mtime, p.name), reverse=True)
    latest_by_date: dict[str, Path] = {}
    for file in files:
        report_date = extract_report_date_fn(file)
        if report_date and report_date not in latest_by_date:
            latest_by_date[report_date] = file
    return latest_by_date


def collect_preferred_lineage_files_by_report_date(
    *,
    directory: Path,
    filename_prefix: str,
    manifest_loader,
    extract_report_date_fn,
) -> dict[str, Path]:
    """
    按业务日选择 lineage 更完整的详细报告文件。

    排序原则：
    1. `lineage_ready=True` 优先于 `sidecar_unbound`
    2. `source_run_count` 更多者优先
    3. 同口径下再取 `created_time` 更新者
    """
    manifest_snapshot = manifest_loader(directory)
    if not manifest_snapshot:
        return {}

    reports, _ = manifest_snapshot
    grouped: dict[str, list[dict]] = {}
    for entry in reports:
        if not isinstance(entry, dict):
            continue
        filename = str(entry.get('filename') or '').strip()
        if not filename.startswith(filename_prefix):
            continue
        report_date = extract_report_date_fn(Path(filename))
        if not report_date:
            continue
        grouped.setdefault(report_date, []).append(entry)

    def _sort_key(entry: dict) -> tuple[int, int, str]:
        return (
            1 if entry.get('lineage_ready') else 0,
            int(entry.get('source_run_count') or 0),
            str(entry.get('created_time') or ''),
        )

    selected: dict[str, Path] = {}
    for report_date, entries in grouped.items():
        best = sorted(entries, key=_sort_key, reverse=True)[0]
        filename = str(best.get('filename') or '').strip()
        if filename:
            selected[report_date] = Path(directory) / filename
    return selected


def find_required_input_file(
    *,
    today_pattern: str,
    fallback_pattern: str,
    directory: Path,
    label: str,
    allow_stale: bool | None = None,
    allow_stale_fn,
    find_latest_file_fn,
    print_fn=print,
) -> Path:
    """
    查找最终筛选所需输入文件。

    默认强制要求使用当天产物；仅在显式开启 allow_stale 时才回退到最近文件。
    """
    stale_allowed = allow_stale_fn() if allow_stale is None else allow_stale

    today_file = find_latest_file_fn(today_pattern, directory)
    if today_file:
        return today_file

    if not stale_allowed:
        raise FileNotFoundError(
            f"未找到今天生成的{label}文件（pattern={today_pattern}），"
            "默认禁止回退旧文件；如确认需要使用旧产物，请显式设置 ALLOW_STALE_INPUTS=1"
        )

    fallback_file = find_latest_file_fn(fallback_pattern, directory)
    if fallback_file:
        print_fn(f"  ℹ️  未找到今天的{label}文件，已按 ALLOW_STALE_INPUTS=1 回退到最近文件...")
        return fallback_file

    raise FileNotFoundError(
        f"未找到{label}文件（today_pattern={today_pattern}, fallback_pattern={fallback_pattern}）"
    )


def find_required_input_pair(
    *,
    short_today_pattern: str,
    short_fallback_pattern: str,
    short_label: str,
    long_today_pattern: str,
    long_fallback_pattern: str,
    long_label: str,
    directory: Path,
    allow_stale: bool | None = None,
    allow_stale_fn,
    collect_preferred_lineage_files_by_report_date_fn,
    collect_latest_files_by_report_date_fn,
    find_latest_file_fn,
    normalize_report_date_fn,
    compact_date_pattern,
    iso_date_pattern,
    print_fn=print,
) -> tuple[Path, Path]:
    """
    为长短期对比选择同一业务日的一对输入文件。

    默认要求当天两侧产物都存在；显式开启 stale 后，回退到最近共同业务日，
    避免分别回退各自最新文件而混用不同业务日数据。
    """
    stale_allowed = allow_stale_fn() if allow_stale is None else allow_stale

    short_today_prefix = short_today_pattern.split('*', 1)[0]
    long_today_prefix = long_today_pattern.split('*', 1)[0]
    preferred_short_by_date = collect_preferred_lineage_files_by_report_date_fn(
        directory=directory,
        filename_prefix=short_today_prefix,
    )
    preferred_long_by_date = collect_preferred_lineage_files_by_report_date_fn(
        directory=directory,
        filename_prefix=long_today_prefix,
    )
    report_date_match = compact_date_pattern.search(short_today_pattern) or iso_date_pattern.search(short_today_pattern)
    target_today_date = normalize_report_date_fn(report_date_match.group('date')) if report_date_match else None

    short_today_file = preferred_short_by_date.get(target_today_date) or find_latest_file_fn(short_today_pattern, directory)
    long_today_file = preferred_long_by_date.get(target_today_date) or find_latest_file_fn(long_today_pattern, directory)
    if short_today_file and long_today_file:
        return short_today_file, long_today_file

    if not stale_allowed:
        if not long_today_file:
            raise FileNotFoundError(
                f"未找到今天生成的{long_label}文件（pattern={long_today_pattern}），"
                "默认禁止回退旧文件；如确认需要使用旧产物，请显式设置 ALLOW_STALE_INPUTS=1"
            )
        raise FileNotFoundError(
            f"未找到今天生成的{short_label}文件（pattern={short_today_pattern}），"
            "默认禁止回退旧文件；如确认需要使用旧产物，请显式设置 ALLOW_STALE_INPUTS=1"
        )

    short_by_date = collect_preferred_lineage_files_by_report_date_fn(
        directory=directory,
        filename_prefix=short_fallback_pattern.split('*', 1)[0],
    ) or collect_latest_files_by_report_date_fn(short_fallback_pattern, directory)
    long_by_date = collect_preferred_lineage_files_by_report_date_fn(
        directory=directory,
        filename_prefix=long_fallback_pattern.split('*', 1)[0],
    ) or collect_latest_files_by_report_date_fn(long_fallback_pattern, directory)
    common_dates = sorted(set(short_by_date) & set(long_by_date), reverse=True)
    if common_dates:
        target_date = common_dates[0]
        print_fn(
            f"  ℹ️  未找到今天成对生成的输入文件，"
            f"已按 ALLOW_STALE_INPUTS=1 回退到最近共同业务日 {target_date}..."
        )
        return short_by_date[target_date], long_by_date[target_date]

    short_latest_date = sorted(short_by_date, reverse=True)[0] if short_by_date else '无'
    long_latest_date = sorted(long_by_date, reverse=True)[0] if long_by_date else '无'
    raise FileNotFoundError(
        f"未找到可配对的{short_label}/{long_label}文件；"
        f"{short_label}最新业务日={short_latest_date}，{long_label}最新业务日={long_latest_date}"
    )
