#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金累积跟踪器 - AccumulationTracker

功能：
1. 维护一个固定的Excel文件，记录每日优质基金的累积情况
2. 跟踪每个基金的出现次数（只记录重复出现的基金）
3. 记录首次出现日期和最后出现日期
4. 更新最新的综合评分和年化收益
5. 统计总对比次数

核心逻辑（重复出现才记录）：
- 第1次出现某基金：内部记录出现次数=1，但不显示在Excel中
- 第2次出现某基金：出现次数=2，开始显示在Excel中
- 第3次及以后：出现次数+1，继续显示
- 意义：只有重复出现的基金才有参考价值（说明长期稳定优质）

Excel结构：
- Row 1 (Metadata): A1="总对比次数" B1=数值（已对比多少份"当日最终优质基金"）
- Row 2 (Headers): 基金代码 | 基金名称 | 出现次数 | 首次出现日期 | 最后出现日期 | 最新综合评分 | 最新年化收益(%)
- Data Rows: 基金累积数据（仅显示出现次数>=2的基金）

使用示例：
    from bond_fund_screening.utils.accumulation_tracker import AccumulationTracker

    tracker = AccumulationTracker(tracker_file='优质基金累积跟踪.xlsx')
    result = tracker.update_from_daily_result(
        daily_result_file='当日最终优质基金_20251112_080600.xlsx'
    )
    print(f"总对比次数: {result['total_comparisons']}")
    print(f"重复出现基金数: {result['total_funds']}")  # 只统计出现次数>=2的
"""

import hashlib
import json
import pandas as pd
try:
    from core.utils import report_history_store as history_store
except ModuleNotFoundError:  # Core's standalone CLI has only core/ on sys.path.
    from utils import report_history_store as history_store
from utils.drawdown_duration import normalize_drawdown_duration_columns
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
import sys

from bond_fund_screening.config import COLUMN_NAME_MAPPING
from bond_fund_screening.core.filtering_engine import stable_sort_desc
try:
    from bond_fund_screening.report_artifact_metadata import (
        collect_source_runs_from_report_meta,
        write_derived_report_meta,
    )
    from bond_fund_screening.report_catalog import rebuild_report_manifests_for_paths
    from bond_fund_screening.tasks.accumulation_file_utils import deduplicate_files_by_date
    from bond_fund_screening.tasks.output_retention import cleanup_same_day_versions_for_file
except ModuleNotFoundError:
    from report_artifact_metadata import (
        collect_source_runs_from_report_meta,
        write_derived_report_meta,
    )
    from report_catalog import rebuild_report_manifests_for_paths
    from tasks.accumulation_file_utils import deduplicate_files_by_date
    from tasks.output_retention import cleanup_same_day_versions_for_file


_TRACKER_INT_COLUMNS = (
    'appearance_count',
    'days_since_last',
    'consecutive_absence',
)
_TRACKER_FLOAT_COLUMNS = (
    'appearance_ratio',
    'latest_return',
    'latest_sharpe_ratio',
    'latest_max_drawdown',
    'latest_max_drawdown_duration',
    'return_score',
    'risk_score',
    'stability_score',
    'liquidity_score',
    'comprehensive_score',
)
TRACKER_REQUIRED_METRIC_COLUMNS = (
    'main_return',
    'sharpe_ratio',
    'max_drawdown',
)


def _get_project_db_path() -> Path:
    """返回项目数据库路径。"""
    from database.connection import resolve_db_path

    return Path(resolve_db_path())


def _normalize_fcode(value) -> str:
    """统一基金代码格式，兼容 Excel 数值列与空值。"""
    if pd.isna(value):
        return ''

    code = str(value).strip()
    if not code or code.lower() == 'nan':
        return ''

    if code.endswith('.0') and code[:-2].isdigit():
        code = code[:-2]

    if code.isdigit() and len(code) < 6:
        code = code.zfill(6)

    return code


def _normalize_tracker_date(value) -> str:
    """统一累积跟踪中的日期文本。"""
    if pd.isna(value):
        return ''
    text = str(value).strip()
    if not text:
        return ''
    if ' ' in text:
        text = text.split(' ', 1)[0]
    return text


class AccumulationTracker:
    """
    基金累积跟踪器类

    管理一个固定的Excel文件，记录每日优质基金的累积情况
    """

    # Excel列定义（完整四维评分系统）
    COLUMNS = [
        'fcode',                  # 基金代码
        'shortname',              # 基金名称
        'appearance_count',       # 出现次数
        'appearance_ratio',       # 出现频率
        'first_appear_date',      # 首次出现日期
        'last_appear_date',       # 最后出现日期
        'days_since_last',        # 距今天数
        'consecutive_absence',    # 连续缺席天数
        'latest_return',          # 最新年化收益(%)
        'latest_sharpe_ratio',    # 最新夏普比率
        'latest_max_drawdown',    # 最新最大回撤(%)
        'latest_max_drawdown_duration',  # 最新最长回撤观测数
        # 完整四维评分系统（收益25% + 风险35% + 稳定25% + 流动15%）
        'return_score',           # 收益能力分 (0-100)
        'risk_score',             # 风险控制分 (0-100)
        'stability_score',        # 稳定性分 (0-100)
        'liquidity_score',        # 流动性分 (0-100)
        'comprehensive_score',    # 综合评分 (0-100)
        'grade',                  # 评分等级 (A/B/C/D/E)
        'score_mode',             # 评分模式 (保守/平衡/进取)
    ]

    COLUMN_NAMES_CN = {
        'fcode': '基金代码',
        'shortname': '基金名称',
        'appearance_count': '出现次数',
        'appearance_ratio': '出现频率(%)',
        'first_appear_date': '首次出现日期',
        'last_appear_date': '最后出现日期',
        'days_since_last': '距今天数',
        'consecutive_absence': '连续缺席天数',
        'latest_return': '最新年化收益(%)',
        'latest_sharpe_ratio': '最新夏普比率',
        'latest_max_drawdown': '最新最大回撤(%)',
        'latest_max_drawdown_duration': '最新最长回撤观测数',
        # 完整四维评分系统
        'return_score': '收益能力分',
        'risk_score': '风险控制分',
        'stability_score': '稳定性分',
        'liquidity_score': '流动性分',
        'comprehensive_score': '综合评分',
        'grade': '评分等级',
        'score_mode': '评分模式',
    }

    PROCESSED_RESULTS_SHEET = '处理记录'
    PROCESSED_RESULTS_COLUMNS = [
        'source_date',
        'source_file_name',
        'source_file_hash',
        'processed_at',
        'source_report_file',
        'source_run_ids',
    ]

    def __init__(self, tracker_file: str, verbose: bool = True):
        """
        初始化累积跟踪器

        Args:
            tracker_file: 累积跟踪Excel文件路径
            verbose: 是否显示详细信息
        """
        self.tracker_file = Path(tracker_file)
        self.verbose = verbose
        self.df_tracker = None
        self.total_comparisons = 0
        self.processed_results = []
        self.processed_source_dates = set()
        self.processed_source_hashes = set()
        self.last_incomplete_funds = []

    def _log(self, message: str):
        """输出日志"""
        if self.verbose:
            print(message)

    def _build_empty_tracker_df(self) -> pd.DataFrame:
        """创建带稳定 schema 的空跟踪表，避免后续列 dtype 漂移。"""
        columns = {}
        for col in self.COLUMNS:
            if col in _TRACKER_INT_COLUMNS:
                columns[col] = pd.Series(dtype='Int64')
            elif col in _TRACKER_FLOAT_COLUMNS:
                columns[col] = pd.Series(dtype='Float64')
            else:
                columns[col] = pd.Series(dtype='object')
        return pd.DataFrame(columns)

    def _normalize_tracker_frame(self, frame: Optional[pd.DataFrame]) -> pd.DataFrame:
        """补齐缺失列并统一数值列 dtype，避免 concat 时发生 dtype 推断漂移。"""
        if frame is None:
            return self._build_empty_tracker_df()

        frame = frame.copy()
        for col in self.COLUMNS:
            if col not in frame.columns:
                if col in _TRACKER_INT_COLUMNS:
                    frame[col] = pd.Series(pd.NA, index=frame.index, dtype='Int64')
                elif col in _TRACKER_FLOAT_COLUMNS:
                    frame[col] = pd.Series(pd.NA, index=frame.index, dtype='Float64')
                else:
                    frame[col] = pd.Series(None, index=frame.index, dtype='object')

        frame = frame[self.COLUMNS].copy()

        for col in _TRACKER_INT_COLUMNS:
            frame[col] = pd.to_numeric(frame[col], errors='coerce').astype('Int64')

        for col in _TRACKER_FLOAT_COLUMNS:
            frame[col] = pd.to_numeric(frame[col], errors='coerce').astype('Float64')

        return frame

    def _normalize_tracker_schema(self):
        """补齐缺失列并统一数值列 dtype，避免 pandas FutureWarning。"""
        self.df_tracker = self._normalize_tracker_frame(self.df_tracker)

    def _load_tracker(self) -> bool:
        """
        加载累积跟踪文件

        优先读取"完整数据"sheet（包含所有基金，包括出现次数=1的）
        如果不存在该sheet，则读取第一个sheet

        Returns:
            是否成功加载（如果文件不存在，返回False但不报错）
        """
        if not self.tracker_file.exists():
            self._log(f"  ℹ️  累积跟踪文件不存在，将创建新文件: {self.tracker_file}")
            self.df_tracker = self._build_empty_tracker_df()
            self.total_comparisons = 0
            self.processed_results = []
            self.processed_source_dates = set()
            self.processed_source_hashes = set()
            return False

        wb = None
        try:
            # ⭐ 优先读取"完整数据"sheet（包含所有基金，包括出现次数=1的）
            try:
                df = pd.read_excel(self.tracker_file, sheet_name='完整数据', skiprows=1)
                self._log(f"  ✅ 从'完整数据'sheet加载（包含所有基金）")
            except (ValueError, KeyError):
                # Fallback: 读取第一个sheet（兼容旧格式）
                df = pd.read_excel(self.tracker_file, skiprows=1)
                self._log(f"  ℹ️  从第一个sheet加载（旧格式兼容）")

            # 反向映射中文列名
            df = normalize_drawdown_duration_columns(df)
            reverse_mapping = {v: k for k, v in self.COLUMN_NAMES_CN.items()}
            df = df.rename(columns=reverse_mapping)

            # ⭐ 关键修复：确保fcode列为字符串类型（6位，前导零）
            if 'fcode' in df.columns:
                df['fcode'] = df['fcode'].map(_normalize_fcode)

            self.df_tracker = df
            self._normalize_tracker_schema()

            # 读取总对比次数（第一行第二列）
            wb = load_workbook(self.tracker_file)
            ws = wb.worksheets[0]  # 读取第一个sheet的元数据
            self.total_comparisons = ws.cell(1, 2).value or 0
            self._load_processed_results(wb)

            self._log(f"  ✅ 加载累积跟踪文件: {self.tracker_file}")
            self._log(f"     已累积基金数: {len(self.df_tracker)}")
            self._log(f"     总对比次数: {self.total_comparisons}")
            if self.processed_results:
                self._log(f"     已处理结果数: {len(self.processed_results)}")

            return True

        except Exception as e:
            self._log(f"  ⚠️  加载累积跟踪文件失败: {e}")
            self.df_tracker = self._build_empty_tracker_df()
            self.total_comparisons = 0
            self.processed_results = []
            self.processed_source_dates = set()
            self.processed_source_hashes = set()
            return False
        finally:
            if wb is not None:
                wb.close()

    def _load_processed_results(self, workbook):
        """加载已处理结果记录，用于避免同一日结果被重复累加。"""
        self.processed_results = []
        self.processed_source_dates = set()
        self.processed_source_hashes = set()

        if self.PROCESSED_RESULTS_SHEET not in workbook.sheetnames:
            return

        ws = workbook[self.PROCESSED_RESULTS_SHEET]
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or not any(row):
                continue

            record = dict(zip(self.PROCESSED_RESULTS_COLUMNS, row))
            source_date = str(record.get('source_date') or '').strip()
            source_hash = str(record.get('source_file_hash') or '').strip()

            if not source_date:
                continue

            record['source_date'] = source_date
            record['source_file_name'] = str(record.get('source_file_name') or '').strip()
            record['source_file_hash'] = source_hash
            record['processed_at'] = str(record.get('processed_at') or '').strip()
            record['source_report_file'] = str(record.get('source_report_file') or '').strip()
            record['source_run_ids'] = self._parse_source_run_ids(record.get('source_run_ids'))
            self.processed_results.append(record)
            self.processed_source_dates.add(source_date)
            if source_hash:
                self.processed_source_hashes.add(source_hash)

    @staticmethod
    def _parse_source_run_ids(raw_value) -> list[str]:
        """解析隐藏 sheet 中持久化的 source_run_ids。"""
        if isinstance(raw_value, list):
            values = raw_value
        else:
            text = str(raw_value or '').strip()
            if not text:
                return []
            try:
                values = json.loads(text)
            except (json.JSONDecodeError, TypeError, ValueError):
                return []

        if not isinstance(values, list):
            return []

        seen = set()
        normalized = []
        for value in values:
            run_id = str(value or '').strip()
            if not run_id or run_id in seen:
                continue
            seen.add(run_id)
            normalized.append(run_id)
        return normalized

    def _extract_result_date(self, daily_result_file: Path) -> str:
        """从结果文件名提取业务日期。"""
        import re

        date_match = re.search(r'_(\d{8})_', daily_result_file.name)
        if date_match:
            date_str = date_match.group(1)
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

        today = datetime.now().strftime("%Y-%m-%d")
        self._log(f"  ⚠️  无法从文件名提取日期，使用系统日期: {today}")
        return today

    def _compute_file_hash(self, file_path: Path) -> str:
        """计算结果文件摘要，便于识别完全相同的重复输入。"""
        return history_store.original_hash(file_path, 'sha1')

    def _build_processed_record_from_file(self, report_path: Path) -> dict:
        """根据最终优质基金文件构造处理记录。"""
        return {
            'source_date': self._extract_result_date(report_path),
            'source_file_name': report_path.name,
            'source_file_hash': self._compute_file_hash(report_path),
            'processed_at': datetime.now().isoformat(timespec='seconds'),
            'source_report_file': str(report_path),
            'source_run_ids': [
                str(item.get('run_id') or '').strip()
                for item in collect_source_runs_from_report_meta(report_path)
                if str(item.get('run_id') or '').strip()
            ],
        }

    def _list_latest_scope_final_files(self) -> list[Path]:
        """列出当前基金类型范围内按业务日去重后的最终优质基金文件。"""
        _, fund_type_name = self._infer_tracker_scope()
        if fund_type_name == '股票型':
            pattern = '股票型_当日最终优质基金_*.xlsx'
        elif fund_type_name == '混合型':
            pattern = '混合型_当日最终优质基金_*.xlsx'
        else:
            pattern = '当日最终优质基金_*.xlsx'

        files = sorted(self.tracker_file.parent.glob(pattern), key=lambda path: path.stat().st_mtime)
        historical = history_store.archived_core_paths(self.tracker_file.parent, pattern)
        if not historical:
            return deduplicate_files_by_date(files)
        import re
        all_files = {str(path): (path, mtime) for path, mtime in historical}
        all_files.update({str(path): (path, path.stat().st_mtime) for path in files})
        by_date = {}
        for path, mtime in sorted(all_files.values(), key=lambda item: item[1]):
            match = re.search(r'_(\d{8})_', path.name)
            by_date[match.group(1) if match else path.name] = (path, mtime)
        return [path for path, _ in sorted(by_date.values(), key=lambda item: item[1])]

    def _ensure_processed_results_match_latest_files(self, *, exclude_source_date: str = '') -> dict | None:
        """
        确保处理记录与当前目录中的“同一业务日最新最终文件”一致。

        仅在发现“已处理过的业务日”对应文件不是当前目录中该业务日最新结果时触发重建，
        避免累积跟踪长期沿用旧版本最终文件，同时不擅自吞入尚未处理的新业务日。
        """
        if not self.processed_results:
            return None

        expected_files = self._list_latest_scope_final_files()
        if not expected_files:
            return None

        current_by_date = {}
        for record in self._sort_processed_records(self.processed_results):
            source_date = _normalize_tracker_date(record.get('source_date'))
            if not source_date:
                continue
            current_by_date[source_date] = dict(record)

        repaired_by_date = dict(current_by_date)
        mismatch_dates = []

        for expected_path in expected_files:
            source_date = self._extract_result_date(expected_path)
            if source_date == exclude_source_date:
                continue
            expected_hash = self._compute_file_hash(expected_path)
            current_record = current_by_date.get(source_date)

            if current_record is None:
                continue

            current_path = self._resolve_processed_report_path(current_record)
            current_name = current_path.name if current_path is not None else str(current_record.get('source_file_name') or '').strip()
            current_hash = str(current_record.get('source_file_hash') or '').strip()

            if current_name != expected_path.name or (current_hash and current_hash != expected_hash):
                repaired_by_date[source_date] = self._build_processed_record_from_file(expected_path)
                mismatch_dates.append(source_date)

        if not mismatch_dates:
            return None

        latest_update_stats = self._rebuild_tracker_from_records(list(repaired_by_date.values()))
        self._save_tracker()
        tracker_report_meta_file = self._write_tracker_report_meta(latest_update_stats)

        return {
            'mismatch_dates': sorted(set(mismatch_dates)),
            'tracker_report_meta_file': str(tracker_report_meta_file),
            'total_comparisons': self.total_comparisons,
        }

    def _check_processed_result(self, source_date: str, source_file_hash: str) -> Optional[str]:
        """
        检查结果文件是否已处理。

        Returns:
            None 表示未处理；否则返回跳过原因。
        """
        if source_file_hash and source_file_hash in self.processed_source_hashes:
            return 'same_file_hash'
        if source_date in self.processed_source_dates:
            return 'same_business_date'
        return None

    @staticmethod
    def _sort_processed_records(records: list[dict]) -> list[dict]:
        """按业务日和处理时间稳定排序处理记录。"""
        return sorted(
            records,
            key=lambda item: (
                _normalize_tracker_date(item.get('source_date')),
                str(item.get('processed_at') or ''),
                str(item.get('source_file_name') or ''),
            ),
        )

    def _mark_processed_result(self, source_date: str, daily_result_file: Path, source_file_hash: str):
        """登记已处理的结果文件。"""
        source_run_ids = [
            str(item.get('run_id') or '').strip()
            for item in collect_source_runs_from_report_meta(daily_result_file)
            if str(item.get('run_id') or '').strip()
        ]
        record = {
            'source_date': source_date,
            'source_file_name': daily_result_file.name,
            'source_file_hash': source_file_hash,
            'processed_at': datetime.now().isoformat(timespec='seconds'),
            'source_report_file': str(daily_result_file),
            'source_run_ids': source_run_ids,
        }
        self.processed_results.append(record)
        self.processed_source_dates.add(source_date)
        if source_file_hash:
            self.processed_source_hashes.add(source_file_hash)

    def _rebuild_tracker_from_records(self, records: list[dict]) -> dict:
        """
        根据处理记录全量重建累积跟踪状态。

        使用场景：
        - 同一业务日重跑后，需要用最新结果替换当天旧结果；
        - 避免在增量状态上做“减一再加一”的脆弱回滚逻辑。
        """
        self.df_tracker = self._build_empty_tracker_df()
        self.total_comparisons = 0
        self.processed_results = []
        self.processed_source_dates = set()
        self.processed_source_hashes = set()
        self.last_incomplete_funds = []

        latest_update_stats = {
            'new_funds': 0,
            'updated_funds': 0,
            'continuous_funds': 0,
            'returning_funds': 0,
            'dropped_funds': 0,
            'filtered_dingkai_funds': 0,
            'daily_funds': 0,
            'filtered_incomplete_funds': 0,
            'filtered_incomplete_fund_samples': [],
        }

        for record in self._sort_processed_records(records):
            report_path = self._resolve_processed_report_path(record)
            if report_path is None or not history_store.available(report_path, 'core_raw'):
                raise FileNotFoundError(
                    f"累积跟踪重建失败：缺少历史结果文件 {report_path or record}"
                )

            source_date = _normalize_tracker_date(record.get('source_date')) or self._extract_result_date(report_path)
            source_file_hash = str(record.get('source_file_hash') or '').strip() or self._compute_file_hash(report_path)

            df_daily = self._read_daily_result(report_path)
            df_daily, incomplete_records = self._filter_incomplete_daily_funds(df_daily)
            self.last_incomplete_funds = incomplete_records

            latest_update_stats = self._update_tracker_data(df_daily, source_date)
            latest_update_stats['filtered_incomplete_funds'] = len(incomplete_records)
            latest_update_stats['filtered_incomplete_fund_samples'] = incomplete_records[:10]
            self._mark_processed_result(source_date, report_path, source_file_hash)

        return latest_update_stats

    def _infer_tracker_scope(self) -> tuple[str, str]:
        """根据累积跟踪文件名推断基金类型。"""
        filename = self.tracker_file.name
        if filename.startswith('股票型_'):
            return 'stock', '股票型'
        if filename.startswith('混合型_'):
            return 'hybrid', '混合型'
        return 'bond', '债券型'

    def _resolve_processed_report_path(self, record: dict) -> Path | None:
        """解析处理记录对应的最终优质基金报告路径。"""
        source_report_file = str(record.get('source_report_file') or '').strip()
        if source_report_file:
            report_path = Path(source_report_file)
            if report_path.exists():
                return report_path

        source_file_name = str(record.get('source_file_name') or '').strip()
        if source_file_name:
            fallback_path = self.tracker_file.parent / source_file_name
            if history_store.available(fallback_path, 'core_raw'):
                return fallback_path

        if source_report_file:
            return Path(source_report_file)
        return None

    def _collect_tracker_source_context(self) -> tuple[list[dict], list[Path]]:
        """聚合累积跟踪文件的上游最终优质基金来源。"""
        source_runs = []
        derived_from_files: list[Path] = []
        seen_files = set()

        for record in self.processed_results:
            report_path = self._resolve_processed_report_path(record)
            if report_path is not None:
                report_path_str = str(report_path)
                if report_path_str not in seen_files:
                    derived_from_files.append(report_path)
                    seen_files.add(report_path_str)

                file_source_runs = collect_source_runs_from_report_meta(report_path)
                if file_source_runs:
                    source_runs.extend(file_source_runs)
                    continue

            fallback_source_file = str(report_path) if report_path is not None else ''
            for run_id in record.get('source_run_ids') or []:
                source_runs.append({
                    'run_id': run_id,
                    'source_kind': 'daily_final_fund_report',
                    'source_file': fallback_source_file or None,
                    'report_file': fallback_source_file or None,
                    'report_filename': report_path.name if report_path is not None else None,
                })

        return source_runs, derived_from_files

    def _build_accumulation_summary(self, latest_update_stats: dict | None = None) -> dict:
        """构建累积跟踪的结构化解释摘要。"""
        df_tracker = self.df_tracker.copy() if self.df_tracker is not None else self._build_empty_tracker_df()
        if 'fcode' in df_tracker.columns:
            df_tracker['fcode'] = df_tracker['fcode'].map(_normalize_fcode)
        if 'last_appear_date' in df_tracker.columns:
            df_tracker['last_appear_date'] = df_tracker['last_appear_date'].map(_normalize_tracker_date)
        if 'appearance_count' in df_tracker.columns:
            df_tracker['appearance_count'] = pd.to_numeric(df_tracker['appearance_count'], errors='coerce')
        if 'consecutive_absence' in df_tracker.columns:
            df_tracker['consecutive_absence'] = pd.to_numeric(df_tracker['consecutive_absence'], errors='coerce')

        latest_source_date = ''
        if self.processed_results:
            latest_source_date = max(
                (
                    _normalize_tracker_date(record.get('source_date'))
                    for record in self.processed_results
                    if _normalize_tracker_date(record.get('source_date'))
                ),
                default='',
            )

        tracked_fund_count = int(len(df_tracker))
        repeated_fund_count = int((df_tracker['appearance_count'] >= 2).sum()) if 'appearance_count' in df_tracker.columns else 0
        current_active_count = 0
        current_repeated_active_count = 0
        if latest_source_date and 'last_appear_date' in df_tracker.columns:
            current_mask = df_tracker['last_appear_date'] == latest_source_date
            current_active_count = int(current_mask.sum())
            if 'appearance_count' in df_tracker.columns:
                current_repeated_active_count = int((current_mask & (df_tracker['appearance_count'] >= 2)).sum())

        at_risk_count = 0
        if 'consecutive_absence' in df_tracker.columns:
            at_risk_count = int((df_tracker['consecutive_absence'] >= 3).sum())

        transitions = latest_update_stats or {}
        transition_items = [
            {'label': '新增入榜', 'count': int(transitions.get('new_funds', 0) or 0)},
            {'label': '连续在榜', 'count': int(transitions.get('continuous_funds', 0) or 0)},
            {'label': '缺席后回归', 'count': int(transitions.get('returning_funds', 0) or 0)},
            {'label': '本期掉出', 'count': int(transitions.get('dropped_funds', 0) or 0)},
            {'label': '关键指标缺失过滤', 'count': int(transitions.get('filtered_incomplete_funds', 0) or 0)},
        ]
        transition_items = [item for item in transition_items if item['count'] > 0]
        transition_summary = '、'.join(f"{item['label']}{item['count']}只" for item in transition_items[:4])

        overview = (
            f"累积跟踪已覆盖 {int(self.total_comparisons)} 期结果，内部累计记录 {tracked_fund_count} 只基金，"
            f"其中重复上榜 {repeated_fund_count} 只。"
        )
        if transition_summary and latest_source_date:
            latest_changes = f"最新一期（{latest_source_date}）的变化为：{transition_summary}。"
        elif latest_source_date:
            latest_changes = f"最新一期业务日为 {latest_source_date}，当前未记录到额外的变化分类。"
        else:
            latest_changes = '当前尚未形成可用的最新一期变化摘要。'

        if latest_source_date:
            risk_hint = (
                f"当前仍在最新一期名单中的基金有 {current_active_count} 只，其中重复上榜基金 {current_repeated_active_count} 只；"
                f"连续缺席至少 3 期的历史基金有 {at_risk_count} 只。"
            )
        else:
            risk_hint = f"当前累计重复上榜基金 {repeated_fund_count} 只，连续缺席至少 3 期的历史基金有 {at_risk_count} 只。"

        headline_highlights = [
            overview,
            latest_changes,
            risk_hint,
            '累积跟踪用于识别连续稳定出现、缺席后回归和热度降温，不替代当日主榜。',
        ]

        return {
            'total_comparisons': int(self.total_comparisons),
            'tracked_fund_count': tracked_fund_count,
            'repeated_fund_count': repeated_fund_count,
            'current_active_count': current_active_count,
            'current_repeated_active_count': current_repeated_active_count,
            'at_risk_absence_count': at_risk_count,
            'latest_filtered_incomplete_fund_count': int(transitions.get('filtered_incomplete_funds', 0) or 0),
            'latest_filtered_incomplete_fund_samples': [
                record.get('fcode')
                for record in (transitions.get('filtered_incomplete_fund_samples') or [])
                if record.get('fcode')
            ],
            'latest_source_date': latest_source_date or None,
            'latest_transition_counts': transition_items,
            'narrative_summary': {
                'overview': overview,
                'latest_changes': latest_changes,
                'risk_hint': risk_hint,
            },
            'headline_highlights': headline_highlights,
        }

    def _write_tracker_report_meta(
        self,
        latest_update_stats: dict | None = None,
        *,
        refresh_manifest: bool = True,
    ) -> Path:
        """为累积跟踪文件写出聚合 lineage sidecar。"""
        fund_type, fund_type_name = self._infer_tracker_scope()
        source_runs, derived_from_files = self._collect_tracker_source_context()
        accumulation_summary = self._build_accumulation_summary(latest_update_stats=latest_update_stats)
        processed_result_files = [
            record.get('source_file_name')
            for record in self.processed_results
            if record.get('source_file_name')
        ]
        return write_derived_report_meta(
            self.tracker_file,
            artifact_type='accumulation_tracker_report',
            source_runs=source_runs,
            derived_from_files=derived_from_files,
            extra_meta={
                'fund_type': fund_type,
                'fund_type_name': fund_type_name,
                'processed_result_count': len(self.processed_results),
                'processed_result_files': processed_result_files,
                'processed_source_dates': [record.get('source_date') for record in self.processed_results if record.get('source_date')],
                'total_comparisons': accumulation_summary.get('total_comparisons'),
                'display_fund_count': accumulation_summary.get('repeated_fund_count'),
                'internal_fund_count': accumulation_summary.get('tracked_fund_count'),
                'accumulation_summary': accumulation_summary,
            },
            refresh_manifest=refresh_manifest,
        )

    def _cleanup_same_day_daily_results(
        self,
        daily_result_file: str | Path,
        *,
        refresh_manifest: bool = True,
    ) -> dict:
        """清理同业务日旧版最终主榜文件，确保上游重复运行只保留最新版本。"""
        return cleanup_same_day_versions_for_file(
            daily_result_file,
            refresh_manifest=refresh_manifest,
        )

    def _read_daily_result(self, daily_result_file: Path) -> pd.DataFrame:
        """
        读取每日最终优质基金文件

        Args:
            daily_result_file: 每日最终优质基金Excel文件路径

        Returns:
            DataFrame
        """
        # 直接读取当日最终优质基金文件
        if not history_store.available(daily_result_file, 'core_raw'):
            raise FileNotFoundError(f"文件不存在: {daily_result_file}")

        df = history_store.read_frame(daily_result_file, 'core_raw',
            lambda: normalize_drawdown_duration_columns(pd.read_excel(daily_result_file, sheet_name=0)))
        options = history_store.project_options(daily_result_file)
        if options and options[2] != 'legacy' and daily_result_file.exists():
            if history_store.source_info(options[0], options[1], daily_result_file, kind='core_raw') is None:
                history_store.put_views(options[0], options[1], daily_result_file,
                                        {'core_raw': history_store.frame_payload(df)})
        source_description = daily_result_file.name

        # 标准化列名映射
        column_mapping = {
            # 基础字段
            '基金代码': 'fcode',
            '基金名称': 'shortname',
            # 收益率字段（"当日最终优质基金"文件中的列名）
            '1年-年化收益(%)': 'main_return',
            # 风险指标字段（新增）- 支持多种列名格式
            '夏普比率': 'sharpe_ratio',
            '1年-夏普比率': 'sharpe_ratio',
            '平均夏普比率': 'sharpe_ratio',
            '最大回撤(%)': 'max_drawdown',
            '1年-最大回撤(%)': 'max_drawdown',
            '最长回撤观测数': 'max_drawdown_duration',
            '1年-最长回撤观测数': 'max_drawdown_duration',
        }

        df = df.rename(columns=column_mapping)

        # 检查必要列是否存在
        if 'fcode' not in df.columns or 'shortname' not in df.columns:
            raise ValueError(f"文件缺少必要列: 基金代码或基金名称")

        self._log(f"  📂 读取数据源: {source_description}")
        self._log(f"     基金数量: {len(df)}")

        # 显示包含的关键字段
        available_fields = []
        if 'fcode' in df.columns:
            available_fields.append('基金代码')
        if 'shortname' in df.columns:
            available_fields.append('基金名称')
        if 'main_return' in df.columns:
            available_fields.append('年化收益')
        if 'sharpe_ratio' in df.columns:
            available_fields.append('夏普比率')
        if 'max_drawdown' in df.columns:
            available_fields.append('最大回撤')
        if 'max_drawdown_duration' in df.columns:
            available_fields.append('最大回撤持续期')

        self._log(f"     包含字段: {', '.join(available_fields)}")

        return df

    def _filter_incomplete_daily_funds(self, df_daily: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
        """过滤关键指标缺失的基金，避免写入累积跟踪。"""
        if df_daily.empty:
            return df_daily.copy(), []

        missing_columns = [col for col in TRACKER_REQUIRED_METRIC_COLUMNS if col not in df_daily.columns]
        if missing_columns:
            self._log(
                "  ⚠️  累积跟踪输入缺少关键指标列，回退为兼容模式: "
                + ", ".join(missing_columns)
            )
            return df_daily.copy(), []

        incomplete_mask = df_daily[list(TRACKER_REQUIRED_METRIC_COLUMNS)].isna().any(axis=1)
        incomplete_records = []
        if incomplete_mask.any():
            for _, row in df_daily.loc[incomplete_mask].iterrows():
                incomplete_records.append({
                    'fcode': _normalize_fcode(row.get('fcode')),
                    'shortname': row.get('shortname', ''),
                    'missing_fields': [
                        col for col in TRACKER_REQUIRED_METRIC_COLUMNS
                        if pd.isna(row.get(col))
                    ],
                })

        return df_daily.loc[~incomplete_mask].copy(), incomplete_records

    def _update_tracker_data(self, df_daily: pd.DataFrame, today: str) -> Dict[str, int]:
        """
        根据每日结果更新累积跟踪数据

        Args:
            df_daily: 每日结果DataFrame
            today: 今日日期字符串 (YYYY-MM-DD)
        """
        # 提取每日结果中的关键字段
        daily_funds = {}
        existing_absence_map = {}
        if self.df_tracker is not None and not self.df_tracker.empty and 'fcode' in self.df_tracker.columns:
            for _, row in self.df_tracker.iterrows():
                fcode = _normalize_fcode(row.get('fcode'))
                if not fcode:
                    continue
                absence_value = row.get('consecutive_absence', 0)
                if pd.isna(absence_value):
                    absence_value = 0
                existing_absence_map[fcode] = int(absence_value)
        for _, row in df_daily.iterrows():
            # ⭐ 统一格式：6位字符串，带前导零
            fcode = _normalize_fcode(row['fcode'])
            if not fcode:
                continue

            shortname = row.get('shortname', '')

            # 获取年化收益率
            return_rate = None
            if 'main_return' in df_daily.columns and pd.notna(row.get('main_return')):
                try:
                    return_rate = float(row['main_return'])
                except (ValueError, TypeError):
                    pass  # 数值转换失败

            # 获取夏普比率（新增）
            sharpe_ratio = None
            if 'sharpe_ratio' in df_daily.columns and pd.notna(row.get('sharpe_ratio')):
                try:
                    sharpe_ratio = float(row['sharpe_ratio'])
                except (ValueError, TypeError):
                    pass  # 数值转换失败

            # 获取最大回撤
            max_drawdown = None
            if 'max_drawdown' in df_daily.columns and pd.notna(row.get('max_drawdown')):
                try:
                    max_drawdown = float(row['max_drawdown'])
                except (ValueError, TypeError):
                    pass  # 数值转换失败

            max_drawdown_duration = None
            if 'max_drawdown_duration' in df_daily.columns and pd.notna(row.get('max_drawdown_duration')):
                try:
                    max_drawdown_duration = float(row['max_drawdown_duration'])
                except (ValueError, TypeError):
                    pass  # 数值转换失败

            daily_funds[fcode] = {
                'shortname': row.get('shortname', ''),
                'return': return_rate,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'max_drawdown_duration': max_drawdown_duration,
                # 完整四维评分字段（稍后从数据库计算）
                'return_score': None,
                'risk_score': None,
                'stability_score': None,
                'liquidity_score': None,
                'comprehensive_score': None,
                'grade': None,
                'score_mode': None,
            }

        # 更新累积跟踪数据
        new_funds_count = 0
        updated_funds_count = 0
        continuous_funds_count = 0
        returning_funds_count = 0
        new_rows = []  # 收集新行，避免循环中逐行 concat

        for fcode, fund_info in daily_funds.items():
            # 查找是否已存在
            existing = self.df_tracker[self.df_tracker['fcode'] == fcode]

            if len(existing) > 0:
                previous_absence = int(existing_absence_map.get(fcode, 0) or 0)
                if previous_absence > 0:
                    returning_funds_count += 1
                else:
                    continuous_funds_count += 1
                # 已存在：增加出现次数，更新最后出现日期和最新数据
                idx = existing.index[0]
                self.df_tracker.at[idx, 'appearance_count'] = self.df_tracker.at[idx, 'appearance_count'] + 1
                self.df_tracker.at[idx, 'last_appear_date'] = today
                self.df_tracker.at[idx, 'latest_return'] = fund_info['return']
                self.df_tracker.at[idx, 'latest_sharpe_ratio'] = fund_info['sharpe_ratio']
                self.df_tracker.at[idx, 'latest_max_drawdown'] = fund_info['max_drawdown']
                self.df_tracker.at[idx, 'latest_max_drawdown_duration'] = fund_info.get('max_drawdown_duration')
                # 完整四维评分字段
                self.df_tracker.at[idx, 'return_score'] = fund_info.get('return_score')
                self.df_tracker.at[idx, 'risk_score'] = fund_info.get('risk_score')
                self.df_tracker.at[idx, 'stability_score'] = fund_info.get('stability_score')
                self.df_tracker.at[idx, 'liquidity_score'] = fund_info.get('liquidity_score')
                self.df_tracker.at[idx, 'comprehensive_score'] = fund_info.get('comprehensive_score')
                self.df_tracker.at[idx, 'grade'] = fund_info.get('grade')
                self.df_tracker.at[idx, 'score_mode'] = fund_info.get('score_mode')
                # 重置连续缺席天数（因为今天出现了）
                self.df_tracker.at[idx, 'consecutive_absence'] = 0
                updated_funds_count += 1
            else:
                # 不存在：新增记录
                new_row = {
                    'fcode': fcode,
                    'shortname': fund_info['shortname'],
                    'appearance_count': 1,
                    'appearance_ratio': None,  # 稍后计算
                    'first_appear_date': today,
                    'last_appear_date': today,
                    'days_since_last': 0,  # 稍后计算
                    'consecutive_absence': 0,
                    'latest_return': fund_info['return'],
                    'latest_sharpe_ratio': fund_info['sharpe_ratio'],
                    'latest_max_drawdown': fund_info['max_drawdown'],
                    'latest_max_drawdown_duration': fund_info.get('max_drawdown_duration'),
                    # 完整四维评分字段
                    'return_score': fund_info.get('return_score'),
                    'risk_score': fund_info.get('risk_score'),
                    'stability_score': fund_info.get('stability_score'),
                    'liquidity_score': fund_info.get('liquidity_score'),
                    'comprehensive_score': fund_info.get('comprehensive_score'),
                    'grade': fund_info.get('grade'),
                    'score_mode': fund_info.get('score_mode'),
                }
                new_rows.append(new_row)
                new_funds_count += 1

        # 批量追加新行，避免逐行 concat 触发 FutureWarning
        if new_rows:
            df_new = self._normalize_tracker_frame(pd.DataFrame(new_rows, columns=self.COLUMNS))
            if self.df_tracker.empty:
                self.df_tracker = df_new
            else:
                self.df_tracker = pd.concat(
                    [self._normalize_tracker_frame(self.df_tracker), df_new],
                    ignore_index=True,
                )
            self._normalize_tracker_schema()

        # 增加总对比次数
        self.total_comparisons += 1

        # 更新未出现基金的连续缺席天数
        today_appeared_fcodes = set(daily_funds.keys())
        dropped_funds_count = 0
        for idx in self.df_tracker.index:
            fcode = self.df_tracker.at[idx, 'fcode']
            if fcode not in today_appeared_fcodes:
                if int(existing_absence_map.get(fcode, 0) or 0) == 0:
                    dropped_funds_count += 1
                # 今天未出现，增加连续缺席天数
                current_absence = self.df_tracker.at[idx, 'consecutive_absence']
                if pd.isna(current_absence):
                    current_absence = 0
                self.df_tracker.at[idx, 'consecutive_absence'] = current_absence + 1

        self._log(f"\n  📊 更新统计:")
        self._log(f"     新增基金: {new_funds_count}")
        self._log(f"     更新基金: {updated_funds_count}")
        self._log(f"     连续在榜: {continuous_funds_count}")
        self._log(f"     缺席后回归: {returning_funds_count}")
        self._log(f"     本期掉出: {dropped_funds_count}")
        self._log(f"     总对比次数: {self.total_comparisons}")

        return {
            'new_funds': new_funds_count,
            'updated_funds': updated_funds_count,
            'continuous_funds': continuous_funds_count,
            'returning_funds': returning_funds_count,
            'dropped_funds': dropped_funds_count,
            'filtered_dingkai_funds': 0,
            'daily_funds': len(daily_funds),
        }

    def _fill_missing_fund_names(self):
        """从数据库补充缺失的基金名称"""
        try:
            from database import DatabaseManager
            from database.models import Fund

            # 找出缺失基金名称的记录
            missing_name_mask = self.df_tracker['shortname'].isna() | (self.df_tracker['shortname'] == '')
            missing_name_count = missing_name_mask.sum()

            if missing_name_count == 0:
                return

            self._log(f"  ℹ️  发现 {missing_name_count} 个基金缺失名称，从数据库补充...")

            # 连接数据库
            db_path = _get_project_db_path()
            db_manager = DatabaseManager(db_path=str(db_path), wal_mode=True)

            filled_count = 0
            with db_manager.get_session() as session:
                for idx in self.df_tracker[missing_name_mask].index:
                    fcode = self.df_tracker.at[idx, 'fcode']
                    normalized_fcode = _normalize_fcode(fcode)
                    if not normalized_fcode:
                        continue

                    # 查询数据库
                    fund = session.query(Fund).filter(Fund.fcode == normalized_fcode).first()
                    if fund and fund.shortname:
                        self.df_tracker.at[idx, 'shortname'] = fund.shortname
                        filled_count += 1

            self._log(f"  ✅ 从数据库补充了 {filled_count} 个基金名称")

        except Exception as e:
            self._log(f"  ⚠️  从数据库补充基金名称失败: {e}")

    def _calculate_enhanced_scores(self, risk_profile: str = 'balanced'):
        """
        使用增强版评分引擎计算完整四维评分

        四维评分系统（收益25% + 风险35% + 稳定25% + 流动15%）：
        - return_score: 收益能力分 (0-100)
        - risk_score: 风险控制分 (0-100)
        - stability_score: 稳定性分 (0-100)
        - liquidity_score: 流动性分 (0-100)
        - comprehensive_score: 综合评分 (0-100)
        - grade: 评分等级 (A/B/C/D/E)
        - score_mode: 评分模式 (保守/平衡/进取)

        Args:
            risk_profile: 风险偏好模式 (conservative/balanced/aggressive)
        """
        try:
            from database import DatabaseManager
            from database.models import FundRiskMetrics, Fund
            from bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine

            self._log(f"  📊 计算增强版四维评分 (模式: {risk_profile})...")

            # 获取所有基金代码
            fcodes = self.df_tracker['fcode'].tolist()
            if not fcodes:
                return

            # 连接数据库获取风险指标
            db_path = _get_project_db_path()
            db_manager = DatabaseManager(db_path=str(db_path), wal_mode=True)

            fund_data_list = []
            with db_manager.get_session() as session:
                for fcode in fcodes:
                    normalized_fcode = _normalize_fcode(fcode)
                    if not normalized_fcode:
                        continue

                    # 获取1年期风险指标
                    metrics = session.query(FundRiskMetrics).filter_by(
                        fcode=normalized_fcode, period='1y'
                    ).first()

                    # 获取基金基本信息
                    fund = session.query(Fund).filter_by(fcode=normalized_fcode).first()

                    # 计算成立天数
                    establish_days = None
                    if fund and fund.estab_date:
                        from datetime import datetime
                        establish_days = (datetime.now().date() - fund.estab_date).days

                    fund_data_list.append({
                        'fcode': normalized_fcode,
                        'shortname': fund.shortname if fund else None,
                        'fullname': fund.fullname if fund else None,
                        'has_holding_period': fund.has_holding_period if fund else None,
                        'holding_period_source': fund.holding_period_source if fund else None,
                        'holding_keywords': fund.holding_keywords if fund else None,
                        'sharpe_ratio': metrics.sharpe_ratio if metrics else None,
                        'sortino_ratio': metrics.sortino_ratio if metrics else None,
                        'calmar_ratio': metrics.calmar_ratio if metrics else None,
                        'max_drawdown': metrics.max_drawdown if metrics else None,
                        'volatility': metrics.volatility if metrics else None,
                        'var_95': metrics.var_95 if metrics else None,
                        'annual_return': metrics.annual_return if metrics else None,
                        'positive_ratio': metrics.positive_ratio if metrics else None,
                        'scale_billion': fund.scale_billion if fund else None,
                        'holding_days': fund.holding_days if fund else None,
                        'establish_days': establish_days,
                    })

            # 使用增强版评分引擎计算评分
            if fund_data_list:
                import pandas as pd
                df_metrics = pd.DataFrame(fund_data_list)

                engine = EnhancedScoringEngine(risk_profile=risk_profile)
                df_scored = engine.calculate_scores(df_metrics)

                # 更新评分到tracker
                for _, row in df_scored.iterrows():
                    fcode = row['fcode']
                    mask = self.df_tracker['fcode'] == fcode
                    if mask.any():
                        idx = self.df_tracker[mask].index[0]
                        # 完整四维评分（四舍五入到2位小数）
                        self.df_tracker.at[idx, 'return_score'] = round(row.get('return_score', 0), 2) if pd.notna(row.get('return_score')) else None
                        self.df_tracker.at[idx, 'risk_score'] = round(row.get('risk_score', 0), 2) if pd.notna(row.get('risk_score')) else None
                        self.df_tracker.at[idx, 'stability_score'] = round(row.get('stability_score', 0), 2) if pd.notna(row.get('stability_score')) else None
                        self.df_tracker.at[idx, 'liquidity_score'] = round(row.get('liquidity_score', 0), 2) if pd.notna(row.get('liquidity_score')) else None
                        self.df_tracker.at[idx, 'comprehensive_score'] = round(row.get('comprehensive_score', 0), 2) if pd.notna(row.get('comprehensive_score')) else None
                        self.df_tracker.at[idx, 'grade'] = row.get('grade')
                        self.df_tracker.at[idx, 'score_mode'] = row.get('score_mode')

                self._log(f"  ✅ 已计算 {len(df_scored)} 只基金的完整四维评分")

        except ImportError as e:
            self._log(f"  ⚠️  增强版评分引擎未安装: {e}")
        except Exception as e:
            self._log(f"  ⚠️  计算增强版评分失败: {e}")
            import traceback
            traceback.print_exc()

    def _save_tracker(self):
        """
        保存累积跟踪文件

        格式：
        - 第1行：元数据（总对比次数）
        - 第2行：表头
        - 第3行起：数据（仅包含出现次数>=2的基金）

        注意：
        - 内存中的 self.df_tracker 保留所有基金（包括出现次数=1的）
        - Excel中只显示出现次数>=2的基金（重复出现才有参考价值）
        """
        # 补零基金代码（转为6位）
        self.df_tracker['fcode'] = self.df_tracker['fcode'].map(_normalize_fcode)

        # 从数据库补充缺失的基金名称
        self._fill_missing_fund_names()

        # 计算增强版四维评分
        self._calculate_enhanced_scores(risk_profile='balanced')

        # 计算出现频率（新增）
        if self.total_comparisons > 0:
            # 确保appearance_count是数值类型
            self.df_tracker['appearance_count'] = pd.to_numeric(self.df_tracker['appearance_count'], errors='coerce')
            self.df_tracker['appearance_ratio'] = (
                self.df_tracker['appearance_count'] / self.total_comparisons * 100
            ).round(2)
        else:
            self.df_tracker['appearance_ratio'] = 0.0

        # 计算距今天数（新增）
        from datetime import datetime
        today = datetime.now()

        # 确保数值列是正确的类型
        numeric_cols = ['consecutive_absence', 'days_since_last', 'latest_return',
                       'latest_sharpe_ratio', 'latest_max_drawdown', 'latest_max_drawdown_duration',
                       'return_score', 'risk_score', 'stability_score',
                       'liquidity_score', 'comprehensive_score']
        for col in numeric_cols:
            if col in self.df_tracker.columns:
                self.df_tracker[col] = pd.to_numeric(self.df_tracker[col], errors='coerce')

        for idx in self.df_tracker.index:
            last_appear_date = self.df_tracker.at[idx, 'last_appear_date']
            if pd.notna(last_appear_date):
                try:
                    # 解析日期字符串
                    if isinstance(last_appear_date, str):
                        last_date = datetime.strptime(last_appear_date, '%Y-%m-%d')
                    else:
                        last_date = last_appear_date
                    days_since = (today - last_date).days
                    self.df_tracker.at[idx, 'days_since_last'] = int(days_since)
                except (ValueError, TypeError, AttributeError):
                    self.df_tracker.at[idx, 'days_since_last'] = None  # 日期解析失败
            else:
                self.df_tracker.at[idx, 'days_since_last'] = None

        # ⭐ 核心过滤：只保存出现次数>=2的基金到Excel
        df_filtered = self.df_tracker[self.df_tracker['appearance_count'] >= 2].copy()

        # 记录统计信息
        total_funds = len(self.df_tracker)
        repeated_funds = len(df_filtered)
        single_appearance_funds = total_funds - repeated_funds

        self._log(f"\n  📊 累积统计:")
        self._log(f"     总基金数（内部）: {total_funds}")
        self._log(f"     重复出现基金（显示）: {repeated_funds}")
        self._log(f"     仅出现1次（隐藏）: {single_appearance_funds}")

        # 按综合评分排序（降序，评分高的排前面）
        df_sorted = stable_sort_desc(df_filtered, 'comprehensive_score').reset_index(drop=True)

        # ⭐ 关键修复：按照COLUMNS定义的顺序重排列，确保Excel列顺序正确
        # 问题原因：DataFrame动态添加列时，列顺序不会自动与COLUMNS定义匹配
        df_sorted = df_sorted[self.COLUMNS]

        # 准备导出（列名转中文）
        df_export = df_sorted.copy()
        df_export = df_export.rename(columns=self.COLUMN_NAMES_CN)

        # 创建Excel文件（包含2个sheet）
        self.tracker_file.parent.mkdir(parents=True, exist_ok=True)

        # 准备完整数据（包含所有基金），按综合评分排序
        df_full = stable_sort_desc(self.df_tracker, 'comprehensive_score').reset_index(drop=True)
        # ⭐ 关键修复：按照COLUMNS定义的顺序重排列
        df_full = df_full[self.COLUMNS]
        df_full_export = df_full.copy()
        df_full_export = df_full_export.rename(columns=self.COLUMN_NAMES_CN)

        # 使用openpyxl创建工作簿
        from openpyxl import Workbook
        wb = Workbook()

        def _to_excel_value(value):
            return None if pd.isna(value) else value

        # === Sheet 1: 重复出现基金（可见，默认sheet） ===
        ws1 = wb.active
        ws1.title = '重复出现基金'

        # 写入元数据（第1行）
        ws1.cell(1, 1, '总对比次数')
        ws1.cell(1, 2, self.total_comparisons)

        # 写入表头（第2行）
        for col_idx, col_name in enumerate(df_export.columns, start=1):
            ws1.cell(2, col_idx, col_name)

        # 写入数据（第3行起）- 只显示出现次数>=2的
        for row_idx, (_, row) in enumerate(df_export.iterrows(), start=3):
            for col_idx, value in enumerate(row, start=1):
                ws1.cell(row_idx, col_idx, _to_excel_value(value))

        # 应用样式到第一个sheet
        self._apply_styling(ws1)

        # === Sheet 2: 完整数据（包含所有基金，供程序读取） ===
        ws2 = wb.create_sheet('完整数据')

        # 写入元数据（第1行）
        ws2.cell(1, 1, '总对比次数')
        ws2.cell(1, 2, self.total_comparisons)

        # 写入表头（第2行）
        for col_idx, col_name in enumerate(df_full_export.columns, start=1):
            ws2.cell(2, col_idx, col_name)

        # 写入数据（第3行起）- 包含所有基金（包括出现次数=1的）
        for row_idx, (_, row) in enumerate(df_full_export.iterrows(), start=3):
            for col_idx, value in enumerate(row, start=1):
                ws2.cell(row_idx, col_idx, _to_excel_value(value))

        # 应用样式到第二个sheet
        self._apply_styling(ws2)

        # === Sheet 3: 处理记录（隐藏，用于避免重复累加） ===
        ws3 = wb.create_sheet(self.PROCESSED_RESULTS_SHEET)
        ws3.sheet_state = 'hidden'
        for col_idx, col_name in enumerate(self.PROCESSED_RESULTS_COLUMNS, start=1):
            ws3.cell(1, col_idx, col_name)
        for row_idx, record in enumerate(self.processed_results, start=2):
            for col_idx, col_name in enumerate(self.PROCESSED_RESULTS_COLUMNS, start=1):
                value = record.get(col_name)
                if isinstance(value, (list, dict)):
                    value = json.dumps(value, ensure_ascii=False)
                ws3.cell(row_idx, col_idx, value)

        # 保存工作簿
        # v1.9.58: 添加 PermissionError 降级处理（Docker 容器以 root 创建的文件可能无写入权限）
        try:
            wb.save(self.tracker_file)
        except PermissionError:
            fallback_path = Path(self.tracker_file).parent / f"_{Path(self.tracker_file).name}"
            self._log(f"  ⚠️ 无法写入 {self.tracker_file}（权限不足），降级保存到: {fallback_path}")
            wb.save(fallback_path)

        self._log(f"  💾 保存累积跟踪文件: {self.tracker_file}")
        self._log(f"     Excel中显示基金数: {repeated_funds} (仅重复出现的)")
        self._log(f"     内存中保留基金数: {total_funds} (包含单次出现的)")

    def _apply_styling(self, ws):
        """
        为Excel工作表应用样式

        Args:
            ws: openpyxl工作表对象
        """
        # 定义样式
        metadata_fill = PatternFill(start_color='E7E6E6', end_color='E7E6E6', fill_type='solid')
        metadata_font = Font(bold=True, size=12)

        header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
        header_font = Font(bold=True, color='FFFFFF', size=11)
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

        center_alignment = Alignment(horizontal='center', vertical='center')
        left_alignment = Alignment(horizontal='left', vertical='center')

        # 元数据行样式（第1行）
        ws.cell(1, 1).fill = metadata_fill
        ws.cell(1, 1).font = metadata_font
        ws.cell(1, 2).fill = metadata_fill
        ws.cell(1, 2).font = metadata_font
        ws.cell(1, 2).alignment = center_alignment

        # 表头样式（第2行）
        for col_idx in range(1, len(self.COLUMNS) + 1):
            cell = ws.cell(2, col_idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        # 数据行样式（第3行起）
        for row_idx in range(3, ws.max_row + 1):
            for col_idx in range(1, len(self.COLUMNS) + 1):
                cell = ws.cell(row_idx, col_idx)

                # 对齐方式
                if col_idx in [1, 2]:  # 基金代码、基金名称
                    cell.alignment = left_alignment
                else:
                    cell.alignment = center_alignment

                # 数值格式
                # 1-基金代码, 2-基金名称, 3-出现次数, 4-出现频率, 5-首次出现, 6-最后出现
                # 7-距今天数, 8-连续缺席, 9-年化收益, 10-夏普比率, 11-最大回撤, 12-最大回撤持续期
                # 13-收益能力分, 14-风险控制分, 15-稳定性分, 16-流动性分, 17-综合评分
                # 18-评分等级, 19-评分模式
                if col_idx == 3:  # 出现次数
                    cell.number_format = '0'
                elif col_idx == 4:  # 出现频率(%)
                    cell.number_format = '0.00'
                elif col_idx in [5, 6]:  # 首次出现日期、最后出现日期
                    cell.number_format = 'YYYY-MM-DD'
                elif col_idx in [7, 8]:  # 距今天数、连续缺席天数
                    cell.number_format = '0'
                elif col_idx == 12:  # 最长回撤观测数
                    cell.number_format = '0'
                elif col_idx in [9, 10, 11, 13, 14, 15, 16, 17]:  # 数值型列
                    cell.number_format = '0.00'
                # 18-评分等级, 19-评分模式 为文本，不需要特殊格式

        # 自动调整列宽
        for col_idx in range(1, len(self.COLUMNS) + 1):
            col_letter = get_column_letter(col_idx)
            max_length = 0

            for row in ws.iter_rows(min_col=col_idx, max_col=col_idx):
                for cell in row:
                    try:
                        if cell.value:
                            cell_length = len(str(cell.value))
                            max_length = max(max_length, cell_length)
                    except (TypeError, AttributeError):
                        pass  # 无法获取单元格长度，跳过

            adjusted_width = min(max(max_length + 2, 10), 50)
            ws.column_dimensions[col_letter].width = adjusted_width

    def update_from_daily_result(self, daily_result_file: str) -> Dict:
        """
        根据每日最终优质基金文件更新累积跟踪

        Args:
            daily_result_file: 每日最终优质基金Excel文件路径

        Returns:
            结果字典，包含：
            - success: 是否成功
            - total_comparisons: 总对比次数
            - total_funds: 累积基金总数
            - new_funds: 本次新增基金数
            - updated_funds: 本次更新基金数
            - tracker_file: 累积跟踪文件路径
        """
        self._log("\n" + "="*80)
        self._log("【基金累积跟踪更新】")
        self._log("="*80)

        try:
            # 1. 加载累积跟踪文件
            self._log("\n步骤1: 加载累积跟踪文件")
            self._load_tracker()
            self._normalize_tracker_schema()

            # 2. 读取每日结果
            self._log("\n步骤2: 读取每日结果文件")
            daily_path = Path(daily_result_file)
            source_date = self._extract_result_date(daily_path)
            source_file_hash = self._compute_file_hash(daily_path)
            consistency_repair = self._ensure_processed_results_match_latest_files(exclude_source_date=source_date)
            if consistency_repair:
                repaired_dates = '、'.join(consistency_repair.get('mismatch_dates') or [])
                self._log(
                    "  ℹ️  检测到累积跟踪使用了非最新最终文件，已按当前目录中的最新业务日结果重建: "
                    f"{repaired_dates}"
                )

            skip_reason = self._check_processed_result(source_date, source_file_hash)
            if skip_reason:
                if skip_reason == 'same_business_date':
                    self._log(f"  ℹ️  检测到同一业务日重跑，将使用最新结果覆盖: {daily_path.name}")
                    remaining_records = [
                        record for record in self.processed_results
                        if _normalize_tracker_date(record.get('source_date')) != source_date
                    ]
                    new_record = {
                        'source_date': source_date,
                        'source_file_name': daily_path.name,
                        'source_file_hash': source_file_hash,
                        'processed_at': datetime.now().isoformat(timespec='seconds'),
                        'source_report_file': str(daily_path),
                        'source_run_ids': [
                            str(item.get('run_id') or '').strip()
                            for item in collect_source_runs_from_report_meta(daily_path)
                            if str(item.get('run_id') or '').strip()
                        ],
                    }
                    latest_update_stats = self._rebuild_tracker_from_records(remaining_records + [new_record])

                    self._log("\n步骤4: 保存累积跟踪文件")
                    self._save_tracker()
                    tracker_report_meta_file = self._write_tracker_report_meta(
                        latest_update_stats,
                        refresh_manifest=False,
                    )
                    cleanup_summary = self._cleanup_same_day_daily_results(
                        daily_path,
                        refresh_manifest=False,
                    )
                    rebuild_report_manifests_for_paths(self.tracker_file, daily_path)
                    if cleanup_summary.get('deleted_count', 0) > 0:
                        self._log(
                            f"  🧹 同业务日旧版最终主榜已清理: {cleanup_summary['deleted_count']} 个文件"
                        )
                    repeated_funds_count = len(self.df_tracker[self.df_tracker['appearance_count'] >= 2])
                    return {
                        'success': True,
                        'skipped': False,
                        'reprocessed_same_business_date': True,
                        'processed_result_count': int(self.total_comparisons),
                        'total_comparisons': self.total_comparisons,
                        'total_funds': repeated_funds_count,
                        'total_internal_funds': len(self.df_tracker),
                        'new_funds': int(latest_update_stats.get('new_funds', 0) or 0),
                        'updated_funds': int(latest_update_stats.get('updated_funds', 0) or 0),
                        'tracker_file': str(self.tracker_file),
                        'tracker_report_meta_file': str(tracker_report_meta_file),
                    }

                self._log(f"  ℹ️  跳过重复结果: {daily_path.name}")
                self._log(f"     原因: {skip_reason}")
                latest_update_stats = self._rebuild_tracker_from_records(list(self.processed_results))
                self._save_tracker()
                cleanup_summary = self._cleanup_same_day_daily_results(
                    daily_path,
                    refresh_manifest=False,
                )
                tracker_report_meta_file = self._write_tracker_report_meta(
                    latest_update_stats,
                    refresh_manifest=False,
                )
                rebuild_report_manifests_for_paths(self.tracker_file, daily_path)
                if cleanup_summary.get('deleted_count', 0) > 0:
                    self._log(
                        f"  🧹 同业务日旧版最终主榜已清理: {cleanup_summary['deleted_count']} 个文件"
                    )
                repeated_funds_count = len(self.df_tracker[self.df_tracker['appearance_count'] >= 2])
                return {
                    'success': True,
                    'skipped': True,
                    'skip_reason': skip_reason,
                    'processed_result_count': int(self.total_comparisons),
                    'total_comparisons': self.total_comparisons,
                    'total_funds': repeated_funds_count,
                    'total_internal_funds': len(self.df_tracker),
                    'new_funds': 0,
                    'updated_funds': 0,
                    'tracker_file': str(self.tracker_file),
                    'tracker_report_meta_file': str(tracker_report_meta_file),
                }

            df_daily = self._read_daily_result(daily_path)
            df_daily, incomplete_records = self._filter_incomplete_daily_funds(df_daily)
            self.last_incomplete_funds = incomplete_records
            if incomplete_records:
                self._log(
                    "  ⚠️ 发现关键指标不完整基金，已从累积跟踪剔除: "
                    f"{len(incomplete_records)} 只"
                )
                preview_codes = [
                    record.get('fcode')
                    for record in incomplete_records[:5]
                    if record.get('fcode')
                ]
                if preview_codes:
                    self._log(f"     样本: {', '.join(preview_codes)}")

            # 3. 更新累积跟踪数据
            self._log("\n步骤3: 更新累积跟踪数据")
            update_stats = self._update_tracker_data(df_daily, source_date)
            update_stats['filtered_incomplete_funds'] = len(incomplete_records)
            update_stats['filtered_incomplete_fund_samples'] = incomplete_records[:10]
            self._mark_processed_result(source_date, daily_path, source_file_hash)

            # 4. 保存累积跟踪文件
            self._log("\n步骤4: 保存累积跟踪文件")
            self._save_tracker()
            tracker_report_meta_file = self._write_tracker_report_meta(
                update_stats,
                refresh_manifest=False,
            )
            cleanup_summary = self._cleanup_same_day_daily_results(
                daily_path,
                refresh_manifest=False,
            )
            rebuild_report_manifests_for_paths(self.tracker_file, daily_path)
            if cleanup_summary.get('deleted_count', 0) > 0:
                self._log(
                    f"  🧹 同业务日旧版最终主榜已清理: {cleanup_summary['deleted_count']} 个文件"
                )

            self._log("\n" + "="*80)
            self._log("✅ 累积跟踪更新完成！")
            self._log("="*80)

            # 统计重复出现的基金数（出现次数>=2）
            repeated_funds_count = len(self.df_tracker[self.df_tracker['appearance_count'] >= 2])

            return {
                'success': True,
                'skipped': False,
                'processed_result_count': int(self.total_comparisons),
                'total_comparisons': self.total_comparisons,
                'total_funds': repeated_funds_count,  # 只统计重复出现的基金
                'total_internal_funds': len(self.df_tracker),  # 内部记录的总基金数（包括单次）
                'new_funds': update_stats['new_funds'],
                'updated_funds': update_stats['updated_funds'],
                'continuous_funds': update_stats.get('continuous_funds', 0),
                'returning_funds': update_stats.get('returning_funds', 0),
                'dropped_funds': update_stats.get('dropped_funds', 0),
                'tracker_file': str(self.tracker_file),
                'tracker_report_meta_file': str(tracker_report_meta_file),
            }

        except Exception as e:
            self._log(f"\n❌ 累积跟踪更新失败: {e}")
            import traceback
            traceback.print_exc()

            return {
                'success': False,
                'processed_result_count': 0,
                'total_comparisons': 0,
                'total_funds': 0,
                'new_funds': 0,
                'updated_funds': 0,
                'tracker_file': None,
                'message': str(e)
            }


def main():
    """测试函数"""
    import argparse

    parser = argparse.ArgumentParser(description='基金累积跟踪器')
    parser.add_argument('--daily-result', required=True, help='每日结果Excel文件路径')
    parser.add_argument('--tracker-file', required=True, help='累积跟踪文件路径')
    parser.add_argument('--quiet', action='store_true', help='静默模式')

    args = parser.parse_args()

    tracker = AccumulationTracker(
        tracker_file=args.tracker_file,
        verbose=not args.quiet
    )

    result = tracker.update_from_daily_result(args.daily_result)

    if result['success']:
        print(f"\n✅ 累积跟踪更新成功")
        print(f"   总对比次数: {result['total_comparisons']}")
        print(f"   累积基金数: {result['total_funds']}")
        print(f"   本次新增: {result['new_funds']}")
        print(f"   本次更新: {result['updated_funds']}")
        return 0
    else:
        print(f"\n❌ 失败: {result.get('message', '未知错误')}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
