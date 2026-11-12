#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
周期任务输出文件辅助工具
"""

from pathlib import Path
from typing import Dict, Optional, Set, Union

from bond_fund_screening.config import SHORT_TERM_PERIOD_CONFIGS
from utils.core_csv_storage import glob_artifacts, original_artifact_stat

EMPTY_RESULT_MARKERS = (
    "⚠️ 筛选后无基金通过，跳过评分/统计/保存阶段",
    "最终输出: 0 只基金",
)


def build_period_simple_csv_pattern(fund_type_name: str, period: Union[str, int]) -> str:
    """
    构建单周期 simple.csv 文件匹配模式。
    """
    if isinstance(period, str):
        period_config = SHORT_TERM_PERIOD_CONFIGS[period]
        period_name = period_config['output_name']
    else:
        period_name = f"{period}年期"
    return f"{fund_type_name}_{period_name}_*_simple.csv"


def snapshot_matching_files(input_dir: Path, pattern: str) -> Set[Path]:
    """获取当前匹配文件快照。"""
    return {path.resolve() for path in glob_artifacts(input_dir, pattern)}


def find_current_run_output_file(input_dir: Path, pattern: str, before_files: Set[Path]) -> Optional[Path]:
    """
    仅从本轮新增文件中定位输出，禁止回退历史文件。
    """
    candidates = sorted(
        glob_artifacts(input_dir, pattern),
        key=lambda path: (original_artifact_stat(path).st_mtime, path.name),
        reverse=True,
    )
    for candidate in candidates:
        if candidate.resolve() not in before_files:
            return candidate
    return None


def resolve_current_run_output(
    input_dir: Path,
    pattern: str,
    before_files: Set[Path],
    result_df=None,
    stdout_text: Optional[str] = None,
):
    """
    统一解析单周期任务的本轮输出。

    Returns:
        (success, output_file, empty_result)
    """
    output_file = find_current_run_output_file(input_dir, pattern, before_files)
    if output_file is not None:
        return True, output_file, False

    is_empty_df = bool(result_df is not None and getattr(result_df, 'empty', False))
    has_empty_marker = bool(stdout_text and any(marker in stdout_text for marker in EMPTY_RESULT_MARKERS))
    if is_empty_df or has_empty_marker:
        return True, None, True

    return False, None, False


def normalize_period_file_map(period_files: Optional[Dict[Union[str, int], Path]]) -> Dict[str, Path]:
    """统一周期文件映射的 key 类型。"""
    if not period_files:
        return {}
    return {str(key): Path(value) for key, value in period_files.items()}
