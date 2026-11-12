#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多周期合并分析脚本

功能：
1. 读取1-5年期的分析结果
2. 交集分析：找出在多个周期都表现优秀的基金
3. 跨周期稳定性评估
4. 生成合并Excel报告

使用方法:
    python3 -m bond_fund_screening.analyzers.multi_period_merger
或
    python3 analyzers/multi_period_merger.py --input-dir output/
"""

import argparse
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import json
import re
import sys
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

from bond_fund_screening.config import (
    OUTPUT_DIR,
    COLUMN_NAME_MAPPING,
    PERCENTAGE_FIELDS,
    PERCENTAGE_VALUE_FIELDS,
    FUND_TYPE_MAPPING,
    INTERSECTION_QUALITY_FILTERS,
)
from bond_fund_screening.core.filtering_engine import stable_sort_desc
from utils.core_csv_storage import artifact_is_file, glob_artifacts, original_artifact_stat, read_csv_artifact
try:
    from bond_fund_screening.share_class_utils import collapse_share_classes
except ModuleNotFoundError:
    from share_class_utils import collapse_share_classes
try:
    from bond_fund_screening.report_artifact_metadata import (
        collect_source_runs_from_period_simple_csv,
        resolve_output_path_reference,
        write_derived_report_meta,
    )
    from bond_fund_screening.report_catalog import rebuild_report_manifests_for_paths
    from bond_fund_screening.tasks.output_retention import cleanup_same_day_versions_for_file
except ModuleNotFoundError:
    # 兼容直接脚本运行时仅将 bond_fund_screening 目录加入 sys.path 的场景
    from report_artifact_metadata import (
        collect_source_runs_from_period_simple_csv,
        resolve_output_path_reference,
        write_derived_report_meta,
    )
    from report_catalog import rebuild_report_manifests_for_paths
    from tasks.output_retention import cleanup_same_day_versions_for_file


class MultiPeriodMerger:
    """多周期合并分析器"""

    # v1.9.60 P1-2: 统一周期排序权重，避免字符串排序导致 '1month' < '1week'
    _PERIOD_SORT_KEY = {'1week': 0, '1month': 1, '3month': 2, '6month': 3}
    _LONG_PERIOD_ALIASES = {
        '1year': 1,
        '2year': 2,
        '3year': 3,
        '4year': 4,
        '5year': 5,
    }

    @classmethod
    def _period_key(cls, p) -> int:
        """返回周期排序键，短期用映射表，长期用数字"""
        return cls._PERIOD_SORT_KEY.get(p, int(p) if str(p).isdigit() else 99)

    @classmethod
    def _normalize_period(cls, period):
        """将 Web/CLI 的标准周期标识归一化为内部周期表示。"""
        if isinstance(period, str):
            normalized = period.strip()
            return cls._LONG_PERIOD_ALIASES.get(normalized, normalized)
        return period

    @classmethod
    def _normalize_periods(cls, periods: Optional[List]) -> Optional[List]:
        """批量归一化周期列表。"""
        if periods is None:
            return None
        return [cls._normalize_period(period) for period in periods]

    @staticmethod
    def _extract_file_sort_meta(filepath: Path) -> Tuple[str, int, int, float]:
        """
        提取结果文件排序元数据。

        排序优先级：
        1. 文件名中的业务时间戳最新
        2. 同一时间戳下优先选择“达标全量”产物，其次选择输出数量更大的产物
        3. mtime 作为兜底
        """
        name = filepath.stem
        top_n_match = re.search(r'Top(\d+)', name)
        all_eligible_match = re.search(r'达标全量(\d+)', name)
        timestamp_match = re.search(r'(\d{8}_\d{6})', name)

        selected_count = 0
        is_all_eligible = 0
        if all_eligible_match:
            selected_count = int(all_eligible_match.group(1))
            is_all_eligible = 1
        elif top_n_match:
            selected_count = int(top_n_match.group(1))
        timestamp = timestamp_match.group(1) if timestamp_match else ''

        try:
            mtime = original_artifact_stat(filepath).st_mtime
        except OSError:
            mtime = 0.0

        return timestamp, is_all_eligible, selected_count, mtime

    @staticmethod
    def _normalize_risk_ratio_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        统一风险比率展示口径。

        多周期合并依赖 simple/full CSV，历史产物可能只包含封顶后的 sharpe_ratio/sortino_ratio；
        新产物如果已经带出 raw_*，则这里优先使用原始值覆盖展示/聚合列，避免跨周期平均值继续被封顶污染。
        """
        if df is None or df.empty:
            return df

        normalized = df.copy()
        if 'raw_sharpe_ratio' in normalized.columns:
            normalized['sharpe_ratio'] = normalized['raw_sharpe_ratio'].combine_first(normalized.get('sharpe_ratio'))
        if 'raw_sortino_ratio' in normalized.columns:
            normalized['sortino_ratio'] = normalized['raw_sortino_ratio'].combine_first(normalized.get('sortino_ratio'))
        return normalized

    def __init__(
        self,
        input_dir: Path,
        verbose: bool = True,
        is_short_term: bool = False,
        fund_type_name: str = "债券型基金",
        period_files: Optional[Dict[str, Path]] = None,
        collapse_share_classes: bool = False,
    ):
        """
        初始化合并分析器

        Args:
            input_dir: 输入目录（包含各周期分析结果）
            verbose: 是否显示详细信息
            is_short_term: 是否为短期周期分析（默认False为长期）
            fund_type_name: 基金类型名称前缀（默认"债券型基金"，与base_analyzer生成的文件前缀一致）
            period_files: 可选，本轮周期产物文件映射；传入后仅加载这些文件，避免混入旧产物
            collapse_share_classes: 是否在稳定性总榜按母基金维度去重
        """
        self.input_dir = input_dir
        self.verbose = verbose
        self.is_short_term = is_short_term
        self.fund_type_name = fund_type_name
        self.collapse_share_classes = collapse_share_classes
        self.period_files = {str(k): Path(v) for k, v in (period_files or {}).items()}
        self._strict_period_files = period_files is not None
        self.period_data: Dict[str, pd.DataFrame] = {}  # 改为str以支持'1week'等
        self.period_stats: Dict[str, dict] = {}  # 改为str以支持'1week'等
        self.period_input_files: Dict[str, Path] = {}

    def _log(self, message: str):
        """输出日志"""
        if self.verbose:
            print(message)

    @staticmethod
    def _get_effective_score(row_data: Dict) -> float:
        """
        获取跨周期汇总应使用的实际评分口径。

        优先使用 selected_combo_score，保持与筛选主链路一致；
        当旧产物或历史数据缺少该列时，再回退到 combo_score。
        """
        for key in ('selected_combo_score', 'combo_score'):
            value = row_data.get(key)
            if value is not None and pd.notna(value):
                return float(value)
        return float('nan')

    def load_period_results(self, periods: List = None) -> bool:
        """
        加载各周期的分析结果

        Args:
            periods: 要加载的周期列表
                    - 长期：[1,2,3,4,5] (年期)
                    - 短期：['1week','1month','3month','6month']

        Returns:
            是否成功加载所有周期
        """
        # 默认周期
        if periods is None:
            if self.is_short_term:
                periods = ['1week', '1month', '3month', '6month']
            else:
                periods = [1, 2, 3, 4, 5]
        periods = self._normalize_periods(periods)

        self._log("\n" + "="*80)
        self._log("【阶段1】加载各周期分析结果")
        self._log("="*80)

        from config import SHORT_TERM_PERIOD_CONFIGS

        for period in periods:
            # 确定搜索模式
            if isinstance(period, str):
                # 短期周期：使用output_name
                if period not in SHORT_TERM_PERIOD_CONFIGS:
                    self._log(f"⚠️  未知的短期周期名称: '{period}'，跳过")
                    continue
                period_name = SHORT_TERM_PERIOD_CONFIGS[period]['output_name']
                pattern = f"{self.fund_type_name}_{period_name}_*_simple.csv"
            else:
                # 长期周期：使用年期
                period_name = f"{period}年期"
                pattern = f"{self.fund_type_name}_{period_name}_*_simple.csv"

            csv_file = self.period_files.get(str(period))
            if csv_file is None and self._strict_period_files:
                self._log(f"  ℹ️  {period_name}: 本轮未生成周期产物，跳过历史文件回退")
                continue

            if csv_file is None:
                matching_files = glob_artifacts(self.input_dir, pattern)

                if not matching_files:
                    self._log(f"⚠️  未找到{period_name}结果文件: {pattern}")
                    continue

                # v1.9.206：优先按产物时间选择最新文件，避免旧 Top500 覆盖本轮较小 TopN 正式结果
                matching_files = sorted(
                    matching_files,
                    key=self._extract_file_sort_meta,
                    reverse=True
                )

                csv_file = matching_files[0]

                # 检查选中的文件基金数量
                _, is_all_eligible, selected_count, _ = self._extract_file_sort_meta(csv_file)
                if is_all_eligible:
                    self._log(f"  ℹ️  {period_name}: 已选用达标全量产物，输出 {selected_count} 只")
                elif selected_count < 100:
                    self._log(f"  ⚠️  {period_name}: 选中的旧产物仅有Top{selected_count}，建议重跑生成达标全量结果")
            elif not artifact_is_file(csv_file):
                self._log(f"⚠️  {period_name} 指定结果文件不存在: {csv_file}")
                continue

            self._log(f"  📂 {period_name}: {csv_file.name}")
            self.period_input_files[str(period)] = csv_file

            # 读取CSV
            df = self._normalize_risk_ratio_columns(read_csv_artifact(csv_file))
            self.period_data[str(period)] = df  # 统一使用字符串key

            # 尝试加载统计信息
            stats_file = csv_file.parent / csv_file.name.replace('_simple.csv', '_stats.json')
            if stats_file.exists():
                with open(stats_file, 'r', encoding='utf-8') as f:
                    self.period_stats[str(period)] = json.load(f)

        loaded_count = len(self.period_data)
        self._log(f"\n✅ 成功加载 {loaded_count} 个周期的数据")

        if loaded_count == 0:
            self._log("❌ 未找到任何周期数据！")
            return False

        return True

    def analyze_intersections(self) -> Dict[str, pd.DataFrame]:
        """
        交集分析：找出在多个周期都表现优秀的基金

        Returns:
            不同交集级别的基金数据字典
        """
        self._log("\n" + "="*80)
        self._log("【阶段2】多周期交集分析")
        self._log("="*80)

        if not self.period_data:
            self._log("❌ 没有可用的周期数据！")
            return {}

        # 获取所有基金代码的出现次数
        all_funds = {}  # {fcode: {period: row_data}}

        for period, df in self.period_data.items():
            for idx, row in df.iterrows():
                fcode = row['fcode']
                if fcode not in all_funds:
                    all_funds[fcode] = {}
                all_funds[fcode][period] = row.to_dict()

        # 统计每个基金出现的周期数
        fund_period_counts = {fcode: len(periods) for fcode, periods in all_funds.items()}

        # 生成不同级别的交集
        intersections = {}
        # v1.9.60 P1-2: 使用统一排序键
        available_periods = sorted(self.period_data.keys(), key=self._period_key)
        max_periods = len(available_periods)

        for n in range(2, max_periods + 1):
            funds_in_n_periods = [fcode for fcode, count in fund_period_counts.items()
                                  if count >= n]

            if not funds_in_n_periods:
                continue

            # 构建这些基金的详细数据
            intersection_data = []
            for fcode in funds_in_n_periods:
                fund_info = all_funds[fcode]

                # 基础信息（取最短周期的）
                # v1.9.60 P1-2: 使用统一类方法排序
                first_period = min(fund_info.keys(), key=self._period_key)
                base_info = fund_info[first_period].copy()

                # 计算跨周期统计（使用 pd.Series 安全处理 NaN）
                returns = pd.Series([data.get('main_return', float('nan')) for data in fund_info.values()])
                combo_scores = pd.Series([
                    self._get_effective_score(data) for data in fund_info.values()
                ])
                # 安全地获取 sharpe_ratio（短期数据可能没有此字段）
                sharpe_ratios = [data.get('sharpe_ratio') for data in fund_info.values()]
                valid_sharpe_ratios = [s for s in sharpe_ratios if s is not None and pd.notna(s)]

                record = {
                    'fcode': fcode,
                    'shortname': base_info['shortname'],
                    'ftype': base_info['ftype'],
                    'period_count': len(fund_info),
                    # v1.9.60 P1-2: 使用统一排序键
                    'periods': ','.join(str(p) for p in sorted(fund_info.keys(), key=self._period_key)),
                    'avg_return': returns.mean(),          # NaN-safe
                    'min_return': returns.min(),            # NaN-safe
                    'max_return': returns.max(),            # NaN-safe
                    'return_std': returns.std(),            # 已是 pd.Series
                    'avg_combo_score': combo_scores.mean(), # NaN-safe
                    'min_combo_score': combo_scores.min(),  # NaN-safe
                    'max_combo_score': combo_scores.max(),  # NaN-safe
                    'score_std': combo_scores.std(),        # 已是 pd.Series
                    # 只有在有有效 sharpe_ratio 时才计算平均值
                    'avg_sharpe': sum(valid_sharpe_ratios) / len(valid_sharpe_ratios) if valid_sharpe_ratios else None,
                    'scale_billion': base_info.get('scale_billion', 0),
                }

                # 添加各周期的详细数据
                for period in available_periods:
                    # 生成规范列名：纯数字用 'y' 后缀（如 return_1y），含文字直接使用（如 return_1week）
                    suffix = f'{period}y' if period.isdigit() else period
                    if period in fund_info:
                        record[f'return_{suffix}'] = fund_info[period]['main_return']
                        record[f'score_{suffix}'] = self._get_effective_score(fund_info[period])
                    else:
                        record[f'return_{suffix}'] = None
                        record[f'score_{suffix}'] = None

                intersection_data.append(record)

            # 转为DataFrame并排序
            df_intersection = pd.DataFrame(intersection_data)
            df_intersection = stable_sort_desc(df_intersection, 'avg_combo_score')

            intersections[f'{n}periods'] = df_intersection

            self._log(f"  ✅ {n}周期交集: {len(df_intersection)} 只基金")

        self._log(f"\n✅ 交集分析完成")
        return intersections

    def _filter_dingkai_funds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤债券链路中的定开/长持有期基金

        Args:
            df: 输入DataFrame

        Returns:
            过滤后的DataFrame
        """
        if df.empty or 'shortname' not in df.columns:
            return df

        # 该过滤本质上是债券最终名单的流动性约束。
        # 混合型/股票型若沿用此规则，会误删合法的持有期产品。
        if '债券' not in str(self.fund_type_name):
            return df

        # 定开债关键词
        dingkai_keywords = ['定开', '定期开放', '月定开', '年定开', '封闭', '持有期']

        def is_dingkai(name):
            if pd.isna(name):
                return False
            name_str = str(name)
            return any(kw in name_str for kw in dingkai_keywords)

        before_count = len(df)
        df_filtered = df[~df['shortname'].apply(is_dingkai)].copy()
        after_count = len(df_filtered)

        if before_count > after_count:
            self._log(f"  🔍 过滤定开/长持有期基金: {before_count} -> {after_count} (移除 {before_count - after_count} 只)")

        return df_filtered

    def _filter_low_quality_funds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        🔧 新增：过滤低质量基金

        过滤条件（从INTERSECTION_QUALITY_FILTERS配置读取）：
        1. 平均综合评分 < min_avg_score 的基金（排除表现差的基金）
        2. 平均夏普比率 < min_avg_sharpe 的基金（排除风险调整收益为负的基金）
        3. 最低收益率 < min_period_return 的基金（排除有严重亏损周期的基金）
        4. 评分标准差 > max_score_std 的基金（排除极不稳定的基金）

        Args:
            df: 输入DataFrame

        Returns:
            过滤后的DataFrame
        """
        if df.empty:
            return df

        # 从配置读取阈值
        min_avg_score = INTERSECTION_QUALITY_FILTERS.get('min_avg_score', 55)
        min_avg_sharpe = INTERSECTION_QUALITY_FILTERS.get('min_avg_sharpe', 0)
        min_period_return = INTERSECTION_QUALITY_FILTERS.get('min_period_return', -3)
        max_score_std = INTERSECTION_QUALITY_FILTERS.get('max_score_std', 15)

        before_count = len(df)
        filters_applied = []

        # 条件1: 平均综合评分 >= min_avg_score
        if 'avg_combo_score' in df.columns:
            mask_score = df['avg_combo_score'] >= min_avg_score
            removed_score = (~mask_score).sum()
            if removed_score > 0:
                filters_applied.append(f"平均评分<{min_avg_score}: {removed_score}只")
        else:
            mask_score = pd.Series(True, index=df.index)

        # 条件2: 平均夏普比率 >= min_avg_sharpe（允许None值通过）
        if 'avg_sharpe' in df.columns:
            mask_sharpe = (df['avg_sharpe'].isna()) | (df['avg_sharpe'] >= min_avg_sharpe)
            removed_sharpe = (~mask_sharpe).sum()
            if removed_sharpe > 0:
                filters_applied.append(f"平均夏普<{min_avg_sharpe}: {removed_sharpe}只")
        else:
            mask_sharpe = pd.Series(True, index=df.index)

        # 条件3: 最低收益率 >= min_period_return
        if 'min_return' in df.columns:
            mask_return = df['min_return'] >= min_period_return
            removed_return = (~mask_return).sum()
            if removed_return > 0:
                filters_applied.append(f"最低收益<{min_period_return}%: {removed_return}只")
        else:
            mask_return = pd.Series(True, index=df.index)

        # 条件4: 评分标准差 <= max_score_std
        # 与综合排名语义保持一致：NaN 视为“稳定性信息缺失”，不直接判失败，
        # 后续 generate_stability_ranking 会将其按中性稳定性处理。
        if 'score_std' in df.columns:
            mask_std = df['score_std'].isna() | (df['score_std'] <= max_score_std)
            removed_std = (~mask_std).sum()
            if removed_std > 0:
                filters_applied.append(f"评分标准差>{max_score_std}: {removed_std}只")
        else:
            mask_std = pd.Series(True, index=df.index)

        # 应用所有过滤条件
        combined_mask = mask_score & mask_sharpe & mask_return & mask_std
        df_filtered = df[combined_mask].copy()

        after_count = len(df_filtered)
        removed_count = before_count - after_count

        if removed_count > 0:
            self._log(f"  🔍 质量过滤: {before_count} -> {after_count} (移除 {removed_count} 只低质量基金)")
            for detail in filters_applied:
                self._log(f"      - {detail}")

        return df_filtered

    def _cap_subtype_ratio(self, df: pd.DataFrame, max_ratio: float = 0.50) -> pd.DataFrame:
        """
        v1.9.59 P2-2: 限制单一子类型在结果中的最大占比

        当某子类型（如混合二级/可转债）占比超过 max_ratio 时，
        按 avg_combo_score 降序截断至上限，保留其他类型全部基金。

        Args:
            df: 输入DataFrame
            max_ratio: 单一子类型最大占比（默认50%）

        Returns:
            处理后的DataFrame
        """
        if df.empty or 'ftype' not in df.columns:
            return df

        type_counts = df['ftype'].value_counts()
        unique_types = len(type_counts)

        # 单一子类型时，占比控制没有意义；若强行截断只会无端砍半结果集。
        if unique_types <= 1:
            self._log("  ℹ️ 子类型占比控制跳过: 当前仅1个子类型")
            return df

        # 防御性保护：若配置本身不可满足，则不做破坏性截断。
        if max_ratio <= 0 or max_ratio >= 1:
            self._log(f"  ⚠️ 子类型占比控制跳过: max_ratio={max_ratio} 非法")
            return df
        if unique_types * max_ratio < 1:
            self._log(
                f"  ⚠️ 子类型占比控制跳过: {unique_types}个子类型与上限{max_ratio:.2f}组合不可满足"
            )
            return df

        sort_col = 'avg_combo_score' if 'avg_combo_score' in df.columns else 'comprehensive_score'
        if sort_col not in df.columns:
            return df

        # 先按子类型内部评分排序，后续只需保留各类型前 N 只。
        grouped = {}
        for ftype, type_df in df.groupby('ftype', sort=False):
            grouped[ftype] = stable_sort_desc(type_df, sort_col).copy()

        original_counts = {ftype: len(type_df) for ftype, type_df in grouped.items()}
        target_counts = original_counts.copy()

        # 迭代收缩超限子类型，直到最终占比真正满足约束。
        # 约束基于“收缩后的总量”，而不是原始总量。
        while True:
            total = sum(target_counts.values())
            if total <= 0:
                break

            oversize = [
                (ftype, count / total, count)
                for ftype, count in target_counts.items()
                if count > 0 and (count / total) > max_ratio
            ]
            if not oversize:
                break

            oversize.sort(key=lambda item: (item[1], item[2]), reverse=True)
            trimmed = False
            for ftype, _, count in oversize:
                # 至少保留1只，避免占比控制退化为整类清空。
                if count > 1:
                    target_counts[ftype] -= 1
                    trimmed = True
                    break
            if not trimmed:
                self._log("  ⚠️ 子类型占比控制提前停止: 无法在保留各子类型至少1只的前提下继续收缩")
                return df

        changed_types = []
        parts = []
        for ftype, type_df in grouped.items():
            keep_count = target_counts.get(ftype, len(type_df))
            if keep_count < len(type_df):
                changed_types.append((ftype, len(type_df), keep_count))
            parts.append(type_df.head(keep_count))

        if not changed_types:
            return df

        df_result = pd.concat(parts, ignore_index=True)
        df_result = stable_sort_desc(df_result, sort_col).reset_index(drop=True)

        for ftype, orig_count, keep_count in changed_types:
            self._log(
                f"  🔍 子类型占比控制: {ftype} {orig_count} -> {keep_count} (上限{max_ratio*100:.0f}%)"
            )

        self._log(f"  📊 占比控制后: {len(df)} -> {len(df_result)} 只基金")
        return df_result

    def generate_stability_ranking(self, intersections: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        生成综合稳定性排名

        综合考虑：
        1. 出现周期数（权重40%）
        2. 平均评分（权重35%）
        3. 评分稳定性（权重25%，std越小越好）

        Args:
            intersections: 交集分析结果

        Returns:
            综合排名DataFrame
        """
        self._log("\n" + "="*80)
        self._log("【阶段3】综合稳定性排名")
        self._log("="*80)

        # 合并所有交集数据
        all_data = []
        for key, df in intersections.items():
            all_data.append(df)

        if not all_data:
            self._log("❌ 没有可用的交集数据！")
            return pd.DataFrame()

        df_all = pd.concat(all_data, ignore_index=True)
        df_all = df_all.drop_duplicates(subset=['fcode'])

        # 过滤定开债基金
        df_all = self._filter_dingkai_funds(df_all)

        # 🔧 新增：质量过滤 - 排除低质量基金
        df_all = self._filter_low_quality_funds(df_all)

        # 空DataFrame保护：质量过滤后可能无基金剩余
        if df_all.empty:
            self._log("⚠️ 质量过滤后无基金剩余，跳过稳定性排名")
            return df_all

        # 归一化各个指标（0-100分）
        max_periods = df_all['period_count'].max()

        # 周期数得分（线性，越多越好）
        df_all['period_score'] = (df_all['period_count'] / max_periods) * 100

        # 平均评分（已经是0-100）
        df_all['avg_score_normalized'] = df_all['avg_combo_score']

        # 稳定性得分（std越小越好，需要反转）
        max_std = df_all['score_std'].max()
        # 处理 NaN：当存在 NaN 时 max() 可能返回 NaN，使用 skipna
        if pd.isna(max_std):
            max_std = df_all['score_std'].dropna().max() if df_all['score_std'].notna().any() else 0
        if max_std > 0:
            # NaN 的 score_std 视为中等稳定性（50分）
            df_all['stability_score'] = df_all['score_std'].apply(
                lambda x: (1 - x / max_std) * 100 if pd.notna(x) else 50
            )
        else:
            df_all['stability_score'] = 100

        # 综合稳定性得分
        df_all['comprehensive_score'] = (
            df_all['period_score'] * 0.40 +
            df_all['avg_score_normalized'] * 0.35 +
            df_all['stability_score'] * 0.25
        )

        # 排序
        df_all = stable_sort_desc(df_all, 'comprehensive_score')

        if self.collapse_share_classes:
            # v1.9.71 P1: 母基金级去重，避免 A/C/E 等多份额占满稳定性总榜
            before_collapse = len(df_all)
            df_all = collapse_share_classes(df_all, name_col='shortname', code_col='fcode')
            after_collapse = len(df_all)
            if after_collapse < before_collapse:
                self._log(
                    f"  🧹 母基金去重: {before_collapse} -> {after_collapse} "
                    f"(移除 {before_collapse - after_collapse} 条重复份额)"
                )
            df_all = stable_sort_desc(df_all, 'comprehensive_score')

        # v1.9.59 P2-2: 可转债（混合二级）占比上限控制
        # 若已启用母基金去重，则占比控制必须基于去重后的候选池，
        # 避免同一母基金多份额先挤占名额、再在去重阶段误删独立基金。
        df_all = self._cap_subtype_ratio(df_all)

        self._log(f"  综合评分范围: {df_all['comprehensive_score'].min():.2f} ~ {df_all['comprehensive_score'].max():.2f}")
        self._log(f"✅ 生成 {len(df_all)} 只基金的稳定性排名")

        return df_all

    def _prepare_dataframe_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备DataFrame用于导出，处理列名和数值格式

        Args:
            df: 原始DataFrame

        Returns:
            处理后的DataFrame
        """
        # 复制避免修改原始数据
        df_display = df.copy()

        # 转换基金类型为中文
        if 'ftype' in df_display.columns:
            def _safe_ftype_map(x):
                """安全的基金类型映射，处理 NaN 值"""
                if pd.isna(x):
                    return '未知'
                s = str(x)
                return FUND_TYPE_MAPPING.get(s, FUND_TYPE_MAPPING.get(int(x) if s.isdigit() else x, s))
            df_display['ftype'] = df_display['ftype'].apply(_safe_ftype_map)

        # 处理百分比字段：需要×100
        for col in PERCENTAGE_FIELDS:
            if col in df_display.columns:
                df_display[col] = df_display[col] * 100

        # 格式化数值字段：减少小数位数
        import numpy as np
        for col in df_display.columns:
            if df_display[col].dtype in [np.float64, np.float32, 'float64', 'float32']:
                # 评分字段保留2位小数
                if 'score' in col.lower():
                    df_display[col] = df_display[col].round(2)
                # 规模保留2位小数
                elif 'scale' in col.lower():
                    df_display[col] = df_display[col].round(2)
                # 收益率保留2位小数
                elif 'return' in col.lower():
                    df_display[col] = df_display[col].round(2)
                # 波动率、回撤保留2位小数（已经×100）
                elif col in PERCENTAGE_FIELDS:
                    df_display[col] = df_display[col].round(2)
                # 比率类保留2位小数（优化：从4位改为2位）
                elif 'ratio' in col.lower() or 'sharpe' in col.lower():
                    df_display[col] = df_display[col].round(2)
                # 标准差保留2位
                elif 'std' in col.lower():
                    df_display[col] = df_display[col].round(2)
                # 天数保留整数
                elif 'days' in col.lower():
                    df_display[col] = df_display[col].round(0).astype('Int64')
                # 其他默认2位
                else:
                    df_display[col] = df_display[col].round(2)

        # 重命名列为中文
        column_mapping = {}
        for col in df_display.columns:
            if col in COLUMN_NAME_MAPPING:
                column_mapping[col] = COLUMN_NAME_MAPPING[col]
            # 如果没有映射，保留原列名

        df_display = df_display.rename(columns=column_mapping)

        return df_display

    def save_results(self, intersections: Dict[str, pd.DataFrame],
                     stability_ranking: pd.DataFrame,
                     output_name: str = None) -> Tuple[Path, Path]:
        """
        保存分析结果

        Args:
            intersections: 交集分析结果
            stability_ranking: 稳定性排名
            output_name: 自定义输出文件名前缀（如"长期优质基金"）
                        如果为None，使用默认名称

        Returns:
            (CSV文件路径, Excel文件路径)
        """
        self._log("\n" + "="*80)
        self._log("【阶段4】保存分析结果")
        self._log("="*80)

        report_date = str(os.environ.get('FUND_ANA_REPORT_DATE') or '').strip()
        if not re.fullmatch(r'20\d{6}', report_date):
            report_date = datetime.now().strftime('%Y%m%d')
        timestamp = f"{report_date}_{datetime.now().strftime('%H%M%S')}"

        # 确定文件名前缀
        if output_name:
            csv_prefix = f"{output_name}_{timestamp}"
            excel_prefix = f"{output_name}_详细报告_{timestamp}"
        else:
            csv_prefix = f"{self.fund_type_name}_多周期交集_优选_{timestamp}"
            excel_prefix = f"{self.fund_type_name}_多周期交集_详细报告_{timestamp}"

        # 保存CSV（综合排名，原始数据）
        csv_file = self.input_dir / f"{csv_prefix}.csv"
        stability_ranking.to_csv(csv_file, index=False, encoding='utf-8-sig')
        self._log(f"  💾 CSV文件: {csv_file}")

        # 保存Excel（包含所有交集级别，格式化和中文列名）
        excel_file = self.input_dir / f"{excel_prefix}.xlsx"

        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet 1: 综合排名（Top 50）
            top_n = min(50, len(stability_ranking))
            df_top = self._prepare_dataframe_for_export(stability_ranking.head(top_n))
            df_top.to_excel(writer, sheet_name='综合稳定性排名Top50', index=False)

            # Sheet 2: 全部交集基金（所有出现在2个及以上周期的基金，去重）
            # 这是用于对比分析的完整列表
            df_all_intersection = self._prepare_dataframe_for_export(stability_ranking)
            df_all_intersection.to_excel(writer, sheet_name='全部交集基金', index=False)

            # Sheet 3-N: 各交集级别
            for key in sorted(intersections.keys(), reverse=True):
                df = intersections[key]
                df_formatted = self._prepare_dataframe_for_export(df.head(100))
                sheet_name = key.replace('periods', '周期交集')
                df_formatted.to_excel(writer, sheet_name=sheet_name, index=False)

            # 最后一个Sheet: 说明
            explanation = pd.DataFrame({
                '指标': [
                    '综合稳定性得分',
                    '周期数得分',
                    '平均评分',
                    '稳定性得分',
                    '出现周期数',
                    '平均综合评分',
                    '评分标准差',
                    '平均收益率',
                    '收益率标准差',
                ],
                '说明': [
                    '综合考虑周期数(40%)、平均评分(35%)、稳定性(25%)的综合得分',
                    '基金出现的周期数越多，得分越高（0-100）',
                    '跨周期平均组合评分（0-100）',
                    '评分标准差的反向得分，越稳定得分越高（0-100）',
                    '基金在多少个周期中都表现优秀',
                    '跨周期平均组合评分（原始值）',
                    '组合评分的标准差，越小表示越稳定',
                    '跨周期平均收益率（%）',
                    '收益率的标准差，越小表示收益越稳定',
                ]
            })
            explanation.to_excel(writer, sheet_name='指标说明', index=False)

        self._log(f"  💾 Excel文件: {excel_file}")

        # 应用样式和格式化
        self._apply_excel_styling(excel_file)

        source_runs = []
        ordered_period_files = sorted(
            self.period_input_files.items(),
            key=lambda item: self._period_key(item[0]),
        )
        bond_short_term_one_year_observation_artifacts = []
        for _, source_file in ordered_period_files:
            source_runs.extend(collect_source_runs_from_period_simple_csv(source_file))
            stats_file = source_file.parent / source_file.name.replace('_simple.csv', '_stats.json')
            if not self.is_short_term or not stats_file.exists():
                continue
            try:
                stats_payload = json.loads(stats_file.read_text(encoding='utf-8'))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if not isinstance(stats_payload, dict):
                continue
            output_files = dict(stats_payload.get('output_files') or {})
            observation_csv = resolve_output_path_reference(
                output_files.get('bond_short_term_one_year_observation_csv'),
                output_dir=stats_file.parent,
            )
            if observation_csv is None or not artifact_is_file(observation_csv):
                continue
            observation_meta = resolve_output_path_reference(
                output_files.get('bond_short_term_one_year_observation_report_meta'),
                output_dir=stats_file.parent,
            )
            period_key = str(stats_payload.get('period') or source_file.stem).strip()
            period_token = str(stats_payload.get('period') or '').strip() or source_file.stem
            period_output_name = str(
                stats_payload.get('period_output_name')
                or stats_payload.get('period')
                or period_key
            ).strip() or period_key
            bond_short_term_one_year_observation_artifacts.append({
                'key': f'bond_short_term_one_year_observation_csv_{str(period_token).strip()}',
                'label': f'债基短期未满1Y观察({period_output_name})',
                'kind': 'csv',
                'period': period_key,
                'period_label': period_output_name,
                'file_path': str(observation_csv),
                'report_meta_file': str(observation_meta) if observation_meta else None,
            })
        report_meta_path = write_derived_report_meta(
            excel_file,
            artifact_type='multi_period_merger_report',
            source_runs=source_runs,
            derived_from_files=[path for _, path in ordered_period_files],
            extra_meta={
                'fund_type_name': self.fund_type_name,
                'is_short_term': self.is_short_term,
                'csv_file': str(csv_file),
                'periods': [str(period) for period, _ in ordered_period_files],
                'bond_short_term_one_year_observation_artifacts': (
                    bond_short_term_one_year_observation_artifacts
                    if self.is_short_term else []
                ),
            },
            refresh_manifest=False,
        )
        self._log(f"  💾 报告元数据: {report_meta_path}")

        deleted_count = 0
        for path in (csv_file, excel_file):
            cleanup_summary = cleanup_same_day_versions_for_file(
                path,
                refresh_manifest=False,
            )
            deleted_count += int(cleanup_summary.get('deleted_count', 0) or 0)
        if deleted_count > 0:
            self._log(f"  🧹 同业务日旧版多周期产物已清理: {deleted_count} 个文件")

        rebuild_report_manifests_for_paths(csv_file, excel_file)

        self._log(f"✅ 结果已保存")

        return csv_file, excel_file

    def _apply_excel_styling(self, excel_file: Path):
        """
        为Excel文件应用样式和格式化

        功能:
        1. 为包含"持有"的基金行添加浅黄色背景（区分持有期基金和普通基金）
        2. 优化数值小数点精度
        3. 设置表头样式
        4. 自动调整列宽
        """
        wb = None
        try:
            wb = load_workbook(excel_file)

            # 定义样式
            # 持有期基金背景色（浅黄色）
            holding_fill = PatternFill(start_color='FFF9E6', end_color='FFF9E6', fill_type='solid')
            # 表头样式（深蓝色背景，白色字体）
            header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
            header_font = Font(bold=True, color='FFFFFF', size=11)
            header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            # 普通单元格对齐
            center_alignment = Alignment(horizontal='center', vertical='center')
            left_alignment = Alignment(horizontal='left', vertical='center')

            # 需要应用数值格式的列名模式
            decimal_4_cols = ['sharpe_ratio', 'volatility', 'max_drawdown']  # 保留4位小数
            decimal_2_cols = ['return', 'scale_billion', 'score', 'std', 'avg', 'min', 'max']  # 保留2位小数

            # 遍历所有工作表
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]

                # 跳过"指标说明"工作表
                if sheet_name == '指标说明':
                    continue

                # 获取表头行（第一行）
                header_row = list(ws.iter_rows(min_row=1, max_row=1, values_only=False))[0]
                header_values = [cell.value for cell in header_row]

                # 应用表头样式
                for cell in header_row:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = header_alignment

                # 获取 shortname 列的索引（用于判断是否包含"持有"）
                shortname_col_idx = None
                if 'shortname' in header_values:
                    shortname_col_idx = header_values.index('shortname')

                # 遍历数据行（从第2行开始）
                for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=False), start=2):
                    # 检查是否为持有期基金（shortname包含"持有"）
                    is_holding_fund = False
                    if shortname_col_idx is not None and row[shortname_col_idx].value:
                        shortname_value = str(row[shortname_col_idx].value)
                        if '持有' in shortname_value:
                            is_holding_fund = True

                    # 遍历该行的所有单元格
                    for col_idx, cell in enumerate(row):
                        # 应用持有期基金背景色
                        if is_holding_fund:
                            cell.fill = holding_fill

                        # 应用数值格式和对齐
                        col_name = header_values[col_idx] if col_idx < len(header_values) else ''

                        # 文本列左对齐
                        if col_name in ['fcode', 'shortname', 'ftype', 'periods']:
                            cell.alignment = left_alignment
                        else:
                            cell.alignment = center_alignment

                        # 数值格式化
                        if cell.value is not None and isinstance(cell.value, (int, float)):
                            # 保留4位小数的列
                            if any(pattern in col_name for pattern in decimal_4_cols):
                                cell.number_format = '0.0000'
                            # 保留2位小数的列
                            elif any(pattern in col_name for pattern in decimal_2_cols):
                                cell.number_format = '0.00'
                            # 整数列
                            elif col_name in ['period_count']:
                                cell.number_format = '0'

                # 自动调整列宽
                for col_idx, col in enumerate(ws.columns, start=1):
                    col_letter = get_column_letter(col_idx)
                    max_length = 0

                    for cell in col:
                        try:
                            if cell.value:
                                cell_length = len(str(cell.value))
                                max_length = max(max_length, cell_length)
                        except (TypeError, AttributeError):
                            pass  # 无法获取单元格长度，跳过

                    # 设置列宽（最小10，最大50）
                    adjusted_width = min(max(max_length + 2, 10), 50)
                    ws.column_dimensions[col_letter].width = adjusted_width

            # 保存修改后的工作簿
            wb.save(excel_file)
            self._log(f"  🎨 已应用样式: 持有期基金标记为浅黄色背景")

        except Exception as e:
            self._log(f"  ⚠️  应用样式时出错: {e}")
            # 即使样式应用失败，也不影响文件本身
        finally:
            if wb is not None:
                wb.close()

    def print_summary(self, intersections: Dict[str, pd.DataFrame],
                     stability_ranking: pd.DataFrame):
        """打印分析摘要"""
        self._log("\n" + "="*80)
        self._log("分析摘要")
        self._log("="*80)

        self._log(f"加载周期数: {len(self.period_data)}")
        self._log(f"可用周期: {sorted(self.period_data.keys())}")

        self._log("\n交集统计:")
        for key in sorted(intersections.keys(), reverse=True):
            df = intersections[key]
            n = key.replace('periods', '')
            self._log(f"  {n}周期交集: {len(df)} 只基金")

        if not stability_ranking.empty:
            self._log("\n🏆 Top 10 多周期稳定优选基金:")
            self._log("-" * 80)
            for idx, row in stability_ranking.head(10).iterrows():
                period_tokens = [p.strip() for p in str(row['periods']).split(',') if p.strip()]
                if self.is_short_term:
                    short_period_map = {
                        '1week': '1周',
                        '1month': '1月',
                        '3month': '3月',
                        '6month': '6月',
                    }
                    periods_str = '+'.join(short_period_map.get(p, p) for p in period_tokens)
                else:
                    periods_str = '+'.join(f"{p}年" if str(p).isdigit() else str(p) for p in period_tokens)
                self._log(
                    f"  #{idx+1:2d}  {row['fcode']}  {row['shortname']:<20s}  "
                    f"周期:{periods_str:<12s}  综合分:{row['comprehensive_score']:>6.2f}  "
                    f"平均收益:{row['avg_return']:>6.2f}%"
                )

    def run(self, periods: List = None, output_name: str = None) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        运行完整的多周期合并分析

        Args:
            periods: 要分析的周期列表（默认根据is_short_term自动选择）
            output_name: 自定义输出文件名前缀

        Returns:
            (稳定性排名, 交集分析结果)
        """
        # 1. 加载数据
        if not self.load_period_results(periods=periods):
            raise RuntimeError("未能加载任何周期数据")

        # 2. 交集分析
        intersections = self.analyze_intersections()

        if not intersections:
            raise RuntimeError("交集分析失败")

        # 3. 稳定性排名
        stability_ranking = self.generate_stability_ranking(intersections)

        if stability_ranking.empty:
            raise RuntimeError("稳定性排名失败")

        # 4. 保存结果
        self.save_results(intersections, stability_ranking, output_name=output_name)

        # 5. 打印摘要
        self.print_summary(intersections, stability_ranking)

        return stability_ranking, intersections


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='多周期基金交集分析')
    parser.add_argument('--input-dir', type=str, default=str(OUTPUT_DIR),
                       help=f'输入目录（默认: {OUTPUT_DIR}）')
    parser.add_argument('--quiet', action='store_true',
                       help='静默模式，减少输出')
    parser.add_argument('--mode', choices=['long', 'short'], default='long',
                       help='分析模式: long=长期(1-5年), short=短期(1周-6月)')
    parser.add_argument('--fund-type-name', type=str, default='债券型基金',
                       help='结果文件前缀，例如 债券型基金/股票型基金/混合型基金')
    parser.add_argument('--periods', type=str, default='',
                       help='指定要合并的周期，逗号分隔，例如 1week,1month 或 1year,2year')

    args = parser.parse_args()

    try:
        periods = None
        if args.periods.strip():
            periods = [
                item.strip()
                for item in args.periods.split(',')
                if item.strip()
            ]

        merger = MultiPeriodMerger(
            input_dir=Path(args.input_dir),
            verbose=not args.quiet,
            is_short_term=(args.mode == 'short'),
            fund_type_name=args.fund_type_name,
            collapse_share_classes=True,
        )

        stability_ranking, intersections = merger.run(periods=periods)

        print("\n" + "="*80)
        print("✅ 多周期交集分析完成！")
        print("="*80)

        return 0

    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
