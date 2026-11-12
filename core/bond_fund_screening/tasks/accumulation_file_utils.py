#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
累积跟踪输入文件工具

统一处理：
1. 当天结果优先
2. 默认禁止静默回退旧文件
3. 首次全量导入时按业务日期去重，仅保留当日最新文件
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path


ACCUMULATION_INPUT_FILE_ENV = 'FUND_ANA_ACCUMULATION_INPUT_FILE'
FINAL_RESULT_FILENAME_RE = re.compile(r'^当日最终优质基金_\d{8}_\d{6}\.xlsx$')


def allow_stale_accumulation_inputs() -> bool:
    """是否允许累积跟踪回退到历史结果文件。"""
    for env_name in ('ALLOW_STALE_ACCUMULATION_INPUTS', 'ALLOW_STALE_INPUTS'):
        value = os.environ.get(env_name, '').strip().lower()
        if value in ('1', 'true', 'yes', 'on'):
            return True
    return False


def deduplicate_files_by_date(files: list[Path]) -> list[Path]:
    """同一业务日期仅保留最新文件。"""
    latest_by_date = {}
    for file in files:
        match = re.search(r'_(\d{8})_', file.name)
        date_key = match.group(1) if match else file.name
        current = latest_by_date.get(date_key)
        if current is None or file.stat().st_mtime >= current.stat().st_mtime:
            latest_by_date[date_key] = file
    return sorted(latest_by_date.values(), key=lambda p: p.stat().st_mtime)


def find_latest_file(pattern: str, directory: Path) -> Path | None:
    """按 mtime 倒序查找最新文件。"""
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def resolve_explicit_final_file(
    directory: Path,
    input_file: str | Path,
    *,
    label: str,
) -> Path:
    """Validate an exact same-run final output passed by the parent pipeline."""
    raw_path = str(input_file or '').strip()
    if not raw_path:
        raise FileNotFoundError(f'未提供{label}的同轮精确输入路径')

    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = Path(directory) / candidate
    try:
        resolved_candidate = candidate.resolve(strict=True)
        resolved_directory = Path(directory).resolve(strict=True)
    except OSError as exc:
        raise FileNotFoundError(f'{label}同轮精确输入不存在: {candidate}') from exc

    if not resolved_candidate.is_file():
        raise FileNotFoundError(f'{label}同轮精确输入不是文件: {resolved_candidate}')
    if resolved_candidate.parent != resolved_directory:
        raise FileNotFoundError(
            f'{label}同轮精确输入不在配置输出目录内: {resolved_candidate}'
        )
    if not FINAL_RESULT_FILENAME_RE.fullmatch(resolved_candidate.name):
        raise FileNotFoundError(
            f'{label}同轮精确输入文件名不符合正式产物约定: {resolved_candidate.name}'
        )
    return resolved_candidate


def find_required_final_file(
    directory: Path,
    today_pattern: str,
    fallback_pattern: str,
    label: str,
    allow_stale: bool | None = None,
) -> Path:
    """
    查找累积跟踪所需最终结果文件。

    默认强制要求当天产物，避免在上游未产出时静默回退旧文件。
    """
    stale_allowed = allow_stale_accumulation_inputs() if allow_stale is None else allow_stale

    today_file = find_latest_file(today_pattern, directory)
    if today_file:
        return today_file

    if not stale_allowed:
        raise FileNotFoundError(
            f"未找到今天生成的{label}文件（pattern={today_pattern}），"
            "默认禁止回退旧文件；如确认需要补跑历史结果，请显式设置 "
            "ALLOW_STALE_ACCUMULATION_INPUTS=1"
        )

    fallback_file = find_latest_file(fallback_pattern, directory)
    if fallback_file:
        print(f"  ℹ️  未找到今天的{label}文件，已按 ALLOW_STALE_ACCUMULATION_INPUTS=1 回退到最近文件...")
        return fallback_file

    raise FileNotFoundError(
        f"未找到{label}文件（today_pattern={today_pattern}, fallback_pattern={fallback_pattern}）"
    )


def build_today_pattern(prefix: str, now: datetime | None = None) -> str:
    """构造当天最终优质基金文件模式。"""
    today = (now or datetime.now()).strftime('%Y%m%d')
    return f"{prefix}_{today}_*.xlsx"
