#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析产物选择辅助工具
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Iterable

import pandas as pd
from utils.core_csv_storage import artifact_is_file, glob_artifacts, original_artifact_stat, read_csv_artifact


MULTI_PERIOD_REPORT_PATTERNS = (
    "长期优质基金_详细报告_*.xlsx",
    "债券型基金_多周期交集_详细报告_*.xlsx",
    "债券基金_多周期交集_详细报告_*.xlsx",
)

MULTI_PERIOD_CSV_PATTERNS = (
    "长期优质基金_*.csv",
    "债券型基金_多周期交集_优选_*.csv",
    "债券基金_多周期交集_优选_*.csv",
)

COMPARISON_REPORT_PATTERNS = (
    "近期与长期优质基金对比报告_*.xlsx",
)

_TIMESTAMP_RE = re.compile(r"(\d{8}_\d{6})")

_MULTI_PERIOD_REPORT_COLUMN_MAP = {
    "基金代码": "fcode",
    "基金名称": "shortname",
    "基金类型": "ftype",
    "出现周期数": "period_count",
    "出现周期": "periods",
    "平均收益率(%)": "avg_return",
    "最低收益率(%)": "min_return",
    "最高收益率(%)": "max_return",
    "收益率标准差(%)": "return_std",
    "平均综合评分": "avg_combo_score",
    "最低综合评分": "min_combo_score",
    "最高综合评分": "max_combo_score",
    "评分标准差": "score_std",
    "平均夏普比率": "avg_sharpe",
    "基金规模(亿元)": "scale_billion",
    "1年收益率(%)": "return_1y",
    "1年评分": "score_1y",
    "2年收益率(%)": "return_2y",
    "2年评分": "score_2y",
    "3年收益率(%)": "return_3y",
    "3年评分": "score_3y",
    "4年收益率(%)": "return_4y",
    "4年评分": "score_4y",
    "5年收益率(%)": "return_5y",
    "5年评分": "score_5y",
    "周期数得分": "period_score",
    "平均评分(标准化)": "avg_score_normalized",
    "稳定性得分": "stability_score",
    "综合稳定性得分": "comprehensive_score",
}


def _normalize_fcode(value) -> str | None:
    """统一基金代码格式，避免 Excel 数值列丢失前导零。"""
    if pd.isna(value):
        return None

    code = str(value).strip()
    if not code or code.lower() == "nan":
        return None

    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]

    if code.isdigit() and len(code) < 6:
        code = code.zfill(6)

    return code


def extract_artifact_timestamp(path: Path) -> datetime | None:
    """从产物文件名提取最后一个 YYYYMMDD_HHMMSS 时间戳。"""
    matches = _TIMESTAMP_RE.findall(path.name)
    if not matches:
        return None
    return datetime.strptime(matches[-1], "%Y%m%d_%H%M%S")


def _artifact_sort_key(path: Path) -> tuple:
    timestamp = extract_artifact_timestamp(path)
    timestamp_value = timestamp or datetime.min
    try:
        mtime = original_artifact_stat(path).st_mtime
    except OSError:
        mtime = 0.0
    return (timestamp_value, mtime, path.name)


def find_latest_artifact(output_dir: Path, patterns: Iterable[str]) -> Path | None:
    """按文件名时间戳优先、mtime 兜底，选择最新产物。"""
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in glob_artifacts(output_dir, pattern) if artifact_is_file(path))
    if not candidates:
        return None
    return max(candidates, key=_artifact_sort_key)


def find_latest_multi_period_report(output_dir: Path) -> Path | None:
    """选择最新的多周期详细报告。"""
    return find_latest_artifact(output_dir, MULTI_PERIOD_REPORT_PATTERNS)


def find_latest_multi_period_csv(output_dir: Path) -> Path | None:
    """选择最新的多周期综合 CSV。"""
    return find_latest_artifact(output_dir, MULTI_PERIOD_CSV_PATTERNS)


def normalize_multi_period_report_frame(df: pd.DataFrame) -> pd.DataFrame:
    """将详细报告中的中文列统一映射为分析脚本使用的字段名。"""
    normalized = df.rename(columns=_MULTI_PERIOD_REPORT_COLUMN_MAP).copy()
    if "fcode" in normalized.columns:
        normalized["fcode"] = normalized["fcode"].map(_normalize_fcode)
    if "period_count" in normalized.columns:
        normalized["period_count"] = pd.to_numeric(normalized["period_count"], errors="coerce")
    return normalized


def load_multi_period_analysis(output_dir: Path) -> dict:
    """加载当前最新的多周期分析产物，优先详细报告，兼容历史 CSV。"""
    latest_report = find_latest_multi_period_report(output_dir)
    if latest_report is not None:
        df_multi = normalize_multi_period_report_frame(
            pd.read_excel(latest_report, sheet_name="全部交集基金")
        )
        if "period_count" in df_multi.columns:
            period_counts = (
                pd.to_numeric(df_multi["period_count"], errors="coerce")
                .dropna()
                .astype(int)
                .value_counts()
                .sort_index(ascending=False)
            )
            df_5period = df_multi[df_multi["period_count"] == 5].copy()
            df_4period = df_multi[df_multi["period_count"] == 4].copy()
        else:
            period_counts = pd.Series(dtype="int64")
            df_5period = pd.DataFrame()
            df_4period = pd.DataFrame()

        return {
            "latest_source": latest_report,
            "source_kind": "report",
            "df_multi": df_multi,
            "df_5period": df_5period,
            "df_4period": df_4period,
            "period_counts": period_counts,
        }

    latest_csv = find_latest_multi_period_csv(output_dir)
    if latest_csv is None:
        return {
            "latest_source": None,
            "source_kind": None,
            "df_multi": pd.DataFrame(),
            "df_5period": pd.DataFrame(),
            "df_4period": pd.DataFrame(),
            "period_counts": pd.Series(dtype="int64"),
        }

    df_multi = read_csv_artifact(latest_csv)
    if "period_count" in df_multi.columns:
        period_counts = df_multi["period_count"].value_counts().sort_index(ascending=False)
        df_5period = df_multi[df_multi["period_count"] == 5].copy()
        df_4period = df_multi[df_multi["period_count"] == 4].copy()
    else:
        period_counts = pd.Series(dtype="int64")
        df_5period = pd.DataFrame()
        df_4period = pd.DataFrame()

    return {
        "latest_source": latest_csv,
        "source_kind": "csv",
        "df_multi": df_multi,
        "df_5period": df_5period,
        "df_4period": df_4period,
        "period_counts": period_counts,
    }
