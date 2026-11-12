#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel报告生成器 - ExcelGenerator

生成格式化的Excel报告，包含：
- 筛选结果工作表
- 筛选概览工作表
- 详细数据工作表
- 筛除原因工作表
- 统计分析工作表
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, Optional

# 尝试导入openpyxl
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils.dataframe import dataframe_to_rows
    from openpyxl.chart import BarChart, Reference, LineChart
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    print("⚠️  警告: 未安装openpyxl库，Excel生成功能将受限")
    print("   安装命令: pip install openpyxl")

from bond_fund_screening.config import (
    EXCEL_SHEET_NAMES, EXCEL_FORMATS,
    COLUMN_NAME_MAPPING, PERCENTAGE_FIELDS, PERCENTAGE_VALUE_FIELDS,
    BUCKETING_ENABLED, BUCKET_KEY_FIELD, BUCKET_MIN_SAMPLES
)


class ExcelGenerator:
    """
    Excel报告生成器类

    生成包含多个工作表的格式化Excel报告
    """

    @staticmethod
    def _safe_round(value, ndigits=2, default=None):
        """安全的round，统一处理 None/NaN/np.nan/pd.NaT 等所有缺失值类型
        v1.9.59 P1-3: 缺失值返回 None 而非 0，避免被误判为零回撤/零波动"""
        if value is None or pd.isna(value):
            return default
        try:
            return round(value, ndigits)
        except (TypeError, ValueError):
            return default

    def __init__(
        self,
        period_years: Optional[int] = None,
        period_days: Optional[int] = None
    ):
        """
        初始化生成器

        Args:
            period_years: 分析周期（年），与period_days二选一
            period_days: 分析周期（天），与period_years二选一，优先使用
        """
        # 参数验证
        if period_days is None and period_years is None:
            raise ValueError("必须提供period_years或period_days参数")

        # 优先使用period_days
        if period_days is not None:
            self.period_days = period_days
            self.period_years = period_days / 365
            # 短期周期使用天数命名
            from bond_fund_screening.config import PERIOD_CONFIGS
            period_key = round(period_days / 365, 2)
            if period_key in PERIOD_CONFIGS:
                self.period_name = PERIOD_CONFIGS[period_key]['name']
            else:
                self.period_name = f"{period_days}天期"
        else:
            self.period_years = period_years
            self.period_days = period_years * 365
            self.period_name = f"{period_years}年期"

        if not HAS_OPENPYXL:
            raise ImportError("需要安装openpyxl库才能生成Excel报告")

    def generate_report(
        self,
        df_result: pd.DataFrame,
        stats: Dict,
        output_file: Path
    ) -> Path:
        """
        生成完整的Excel报告

        Args:
            df_result: 筛选结果DataFrame
            stats: 统计信息字典
            output_file: 输出文件路径

        Returns:
            生成的Excel文件路径
        """
        # 创建工作簿
        wb = Workbook()
        # 删除默认工作表
        wb.remove(wb.active)

        # Step 7-5: 暂存 df 供 overview sheet 读取策略摘要
        self._df = df_result

        # 工作表1: 筛选结果
        self._create_results_sheet(wb, df_result)

        # 工作表2: 筛选概览
        self._create_overview_sheet(wb, stats)

        # 工作表3: 详细数据
        self._create_details_sheet(wb, df_result)

        # 工作表4: 筛除原因统计
        if 'filter' in stats and 'layer_stats' in stats['filter']:
            self._create_filter_reasons_sheet(wb, stats['filter'])

        # 工作表5: 统计分析
        if 'analysis' in stats:
            self._create_statistics_sheet(wb, stats['analysis'])

        # 工作表6: 分桶汇总（Step 3新增）
        if BUCKETING_ENABLED and 'bucket_key' in df_result.columns:
            self._create_bucket_summary_sheet(wb, df_result)

        # 工作表7: 影子评估汇总（Step 4新增）
        if 'shadow_fail_2pct' in df_result.columns:
            self._create_shadow_evaluation_sheet(wb, df_result)

        # 工作表8: 新鲜度统计（Step 5D新增）
        if 'is_fresh' in df_result.columns:
            self._create_freshness_summary_sheet(wb, df_result)

        # 保存文件
        wb.save(output_file)
        return output_file

    def _create_results_sheet(self, wb: Workbook, df: pd.DataFrame):
        """
        创建筛选结果工作表

        显示核心信息和排名
        """
        ws = wb.create_sheet(EXCEL_SHEET_NAMES['results'])

        # 标题
        ws['A1'] = f'债券基金智能筛选系统 - {self.period_name}分析结果'
        ws['A1'].font = Font(size=16, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws['A1'].alignment = Alignment(horizontal='center', vertical='center')
        ws.merge_cells('A1:K1')
        ws.row_dimensions[1].height = 25

        # 生成时间
        ws['A2'] = f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        ws['A2'].font = Font(size=10, italic=True)

        # 表头（从第4行开始）
        headers = [
            '排名', '基金代码', '基金名称', '基金类型',
            f'{self.period_years}年收益率%', '基金规模(亿)',
            '夏普比率', '最大回撤%', '波动率%',
            '短期潜力分', '长期价值分', '综合评分'
        ]

        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=4, column=col_idx, value=header)
            cell.font = Font(bold=True, color='FFFFFF', size=11)
            cell.fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self._get_border()

        # 数据行
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            row_num = idx + 4

            # 排名
            ws.cell(row=row_num, column=1, value=idx)

            # 基金代码
            ws.cell(row=row_num, column=2, value=row['fcode'])

            # 基金名称
            ws.cell(row=row_num, column=3, value=row['shortname'])

            # 基金类型
            ws.cell(row=row_num, column=4, value=row.get('ftype', ''))

            # 收益率
            main_return = self._safe_round(row.get('main_return', 0), 2) or 0
            cell = ws.cell(row=row_num, column=5, value=main_return)
            cell.number_format = '0.00'
            # 根据收益率设置颜色
            if main_return >= 10:
                cell.font = Font(color='FF0000', bold=True)  # 红色
            elif main_return >= 5:
                cell.font = Font(color='FF6600')  # 橙色

            # 基金规模
            scale = self._safe_round(row.get('scale_billion', 0), 2) or 0
            ws.cell(row=row_num, column=6, value=scale).number_format = '0.00'

            # 夏普比率
            sharpe = self._safe_round(row.get('sharpe_ratio'), 2)
            ws.cell(row=row_num, column=7, value=sharpe).number_format = '0.00'

            # 最大回撤（原始值为小数，需转百分比）
            max_dd_raw = self._safe_round(row.get('max_drawdown'), 4)
            max_dd = round(max_dd_raw * 100, 2) if max_dd_raw is not None else None
            ws.cell(row=row_num, column=8, value=max_dd).number_format = '0.00'

            # 波动率（转为百分比）
            vol_raw = self._safe_round(row.get('volatility'), 4)
            vol = round(vol_raw * 100, 2) if vol_raw is not None else None
            ws.cell(row=row_num, column=9, value=vol).number_format = '0.00'

            # 短期潜力分
            short_score = self._safe_round(row.get('short_term_score', 0), 2) or 0
            ws.cell(row=row_num, column=10, value=short_score).number_format = '0.00'

            # 长期价值分
            long_score = self._safe_round(row.get('long_term_score', 0), 2) or 0
            ws.cell(row=row_num, column=11, value=long_score).number_format = '0.00'

            # 综合评分
            combo_score = self._safe_round(row.get('combo_score', 0), 2) or 0
            cell = ws.cell(row=row_num, column=12, value=combo_score)
            cell.number_format = '0.00'
            cell.font = Font(bold=True)

            # 设置边框
            for col in range(1, 13):
                ws.cell(row=row_num, column=col).border = self._get_border('thin')

            # 交替行背景色
            if idx % 2 == 0:
                for col in range(1, 13):
                    ws.cell(row=row_num, column=col).fill = PatternFill(
                        start_color='F0F0F0', end_color='F0F0F0', fill_type='solid'
                    )

        # 调整列宽
        column_widths = [6, 10, 25, 18, 14, 12, 11, 11, 10, 11, 11, 11]
        for col_idx, width in enumerate(column_widths, start=1):
            ws.column_dimensions[self._get_column_letter(col_idx)].width = width

        # 冻结前4行和前3列
        ws.freeze_panes = 'D5'

    def _create_overview_sheet(self, wb: Workbook, stats: Dict):
        """
        创建筛选概览工作表

        显示筛选流程统计
        """
        ws = wb.create_sheet(EXCEL_SHEET_NAMES['overview'])

        # 标题
        ws['A1'] = f'筛选概览 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True)
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws.merge_cells('A1:E1')

        # 总体统计
        row = 3
        ws[f'A{row}'] = '总体统计'
        ws[f'A{row}'].font = Font(size=12, bold=True)
        row += 1

        if 'filter' in stats:
            filter_stats = stats['filter']
            ws[f'A{row}'] = '初始基金池:'
            ws[f'B{row}'] = filter_stats['total_start']
            row += 1

            ws[f'A{row}'] = '最终输出:'
            ws[f'B{row}'] = filter_stats['total_end']
            row += 1

            if filter_stats['total_start'] > 0:
                ws[f'A{row}'] = '总筛除率:'
                rate = (1 - filter_stats['total_end']/filter_stats['total_start'])*100
                ws[f'B{row}'] = f"{rate:.1f}%"
            row += 2

        # 各层筛选统计
        ws[f'A{row}'] = '各层筛选统计'
        ws[f'A{row}'].font = Font(size=12, bold=True)
        row += 1

        # 表头
        headers = ['层级', '输入', '输出', '筛除', '筛除率', '说明']
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
        row += 1

        # 各层数据
        if 'filter' in stats and 'layer_stats' in stats['filter']:
            for layer_stat in stats['filter']['layer_stats']:
                ws.cell(row=row, column=1, value=layer_stat['layer_name'])
                ws.cell(row=row, column=2, value=layer_stat['start_count'])
                ws.cell(row=row, column=3, value=layer_stat['end_count'])
                ws.cell(row=row, column=4, value=layer_stat['filtered_count'])
                ws.cell(row=row, column=5, value=f"{layer_stat['filter_rate']:.1f}%")
                ws.cell(row=row, column=6, value=layer_stat.get('extra_info', ''))

                # 设置边框
                for col in range(1, 7):
                    ws.cell(row=row, column=col).border = self._get_border('thin')

                row += 1

        # 调整列宽
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 12
        ws.column_dimensions['F'].width = 40

        # Step 7-5: 评分策略摘要（从 df 中提取，最小侵入）
        if hasattr(self, '_df') and self._df is not None and 'selected_score_key' in self._df.columns:
            row += 2
            ws[f'A{row}'] = '评分策略'
            ws[f'A{row}'].font = Font(size=12, bold=True)
            row += 1
            score_key = self._df['selected_score_key'].iloc[0] if len(self._df) > 0 else 'combo_score'
            fallback = self._df['selected_score_fallback'].iloc[0] if 'selected_score_fallback' in self._df.columns and len(self._df) > 0 else False
            reason = self._df['selected_score_reason'].iloc[0] if 'selected_score_reason' in self._df.columns and len(self._df) > 0 else ''
            ws[f'A{row}'] = 'selected_score_key:'
            ws[f'B{row}'] = str(score_key)
            row += 1
            ws[f'A{row}'] = 'fallback:'
            ws[f'B{row}'] = str(fallback)
            row += 1
            ws[f'A{row}'] = 'reason:'
            ws[f'B{row}'] = str(reason)

    def _create_details_sheet(self, wb: Workbook, df: pd.DataFrame):
        """
        创建详细数据工作表

        包含所有列的完整数据，列名转换为中文，数值格式化
        """
        ws = wb.create_sheet(EXCEL_SHEET_NAMES['details'])

        # 复制DataFrame以避免修改原始数据
        df_display = df.copy()

        # 处理百分比字段：需要×100
        for col in PERCENTAGE_FIELDS:
            if col in df_display.columns:
                df_display[col] = df_display[col] * 100

        # 格式化数值字段：减少小数位数
        for col in df_display.columns:
            if df_display[col].dtype in [np.float64, np.float32]:
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
                elif 'ratio' in col.lower():
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
            else:
                # 如果没有映射，保留原列名
                column_mapping[col] = col

        df_display = df_display.rename(columns=column_mapping)

        # 标题
        ws['A1'] = f'详细数据 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws.merge_cells(f'A1:{self._get_column_letter(len(df_display.columns))}1')

        # 将DataFrame写入Excel
        for r_idx, row in enumerate(dataframe_to_rows(df_display, index=False, header=True), start=3):
            for c_idx, value in enumerate(row, start=1):
                # openpyxl 不支持 pandas NA/NaT，统一转为 None
                if pd.isna(value) if not isinstance(value, (str, bool)) else False:
                    value = None
                cell = ws.cell(row=r_idx, column=c_idx, value=value)

                # 表头样式
                if r_idx == 3:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
                    cell.border = self._get_border()
                    cell.alignment = Alignment(horizontal='center', vertical='center')

                # 数据行
                else:
                    cell.border = self._get_border('thin')

                    # 数值格式
                    if isinstance(value, (int, float)) and not pd.isna(value):
                        # 根据列名设置数字格式
                        col_name = df_display.columns[c_idx - 1]

                        if '比率' in col_name or 'ratio' in col_name.lower():
                            cell.number_format = '0.0000'
                        elif '%' in col_name or 'percent' in col_name.lower():
                            cell.number_format = '0.00'
                        elif '分' in col_name or 'score' in col_name.lower():
                            cell.number_format = '0.00'
                        elif '亿' in col_name:
                            cell.number_format = '0.00'
                        else:
                            cell.number_format = '0.00'

        # 调整列宽（自动）
        for col_idx in range(1, len(df_display.columns) + 1):
            max_length = 0
            column_letter = self._get_column_letter(col_idx)

            # 遍历该列的所有单元格
            for row_idx in range(3, min(ws.max_row + 1, 103)):  # 限制最多检查100行
                cell = ws.cell(row=row_idx, column=col_idx)
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))

            adjusted_width = min(max(max_length + 2, 10), 50)
            ws.column_dimensions[column_letter].width = adjusted_width

        # 冻结表头
        ws.freeze_panes = 'A4'

    def _create_filter_reasons_sheet(self, wb: Workbook, filter_stats: Dict):
        """
        创建筛除原因工作表
        """
        ws = wb.create_sheet(EXCEL_SHEET_NAMES['reasons'])

        # 标题
        ws['A1'] = f'筛除原因分析 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws.merge_cells('A1:D1')

        row = 3
        ws[f'A{row}'] = '筛选层级'
        ws[f'B{row}'] = '筛除数量'
        ws[f'C{row}'] = '筛除率'
        ws[f'D{row}'] = '主要原因'

        for col in range(1, 5):
            cell = ws.cell(row=row, column=col)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()

        row += 1

        # 填充数据
        if 'layer_stats' in filter_stats:
            for layer_stat in filter_stats['layer_stats']:
                if layer_stat['filtered_count'] > 0:
                    ws.cell(row=row, column=1, value=layer_stat['layer_name'])
                    ws.cell(row=row, column=2, value=layer_stat['filtered_count'])
                    ws.cell(row=row, column=3, value=f"{layer_stat['filter_rate']:.1f}%")
                    ws.cell(row=row, column=4, value=layer_stat.get('extra_info', ''))

                    for col in range(1, 5):
                        ws.cell(row=row, column=col).border = self._get_border('thin')

                    row += 1

        # 调整列宽
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 40

    def _create_statistics_sheet(self, wb: Workbook, analysis_stats: Dict):
        """
        创建统计分析工作表
        """
        ws = wb.create_sheet(EXCEL_SHEET_NAMES['statistics'])

        # 标题
        ws['A1'] = f'统计分析 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws.merge_cells('A1:E1')

        row = 3

        # 基本统计
        ws[f'A{row}'] = '基本统计'
        ws[f'A{row}'].font = Font(size=12, bold=True)
        row += 1

        ws[f'A{row}'] = '基金数量:'
        ws[f'B{row}'] = analysis_stats.get('count', 0)
        row += 1

        ws[f'A{row}'] = '分析周期:'
        ws[f'B{row}'] = analysis_stats.get('period', '')
        row += 2

        # 收益率统计
        if 'return' in analysis_stats:
            ws[f'A{row}'] = '收益率统计'
            ws[f'A{row}'].font = Font(size=12, bold=True)
            row += 1

            return_stats = analysis_stats['return']
            for key in ['min', 'max', 'mean', 'median']:
                ws[f'A{row}'] = f'  {key}:'
                ws[f'B{row}'] = self._safe_round(return_stats.get(key, 0), 2)
                ws[f'B{row}'].number_format = '0.00'
                row += 1
            row += 1

        # 规模统计
        if 'scale' in analysis_stats:
            ws[f'A{row}'] = '规模统计 (亿元)'
            ws[f'A{row}'].font = Font(size=12, bold=True)
            row += 1

            scale_stats = analysis_stats['scale']
            for key in ['min', 'max', 'mean', 'median', 'total']:
                ws[f'A{row}'] = f'  {key}:'
                ws[f'B{row}'] = self._safe_round(scale_stats.get(key, 0), 2)
                ws[f'B{row}'].number_format = '0.00'
                row += 1
            row += 1

        # 风险指标统计
        if 'risk_metrics' in analysis_stats:
            ws[f'A{row}'] = '风险指标统计'
            ws[f'A{row}'].font = Font(size=12, bold=True)
            row += 1

            # 表头
            ws.cell(row=row, column=1, value='指标')
            ws.cell(row=row, column=2, value='最小值')
            ws.cell(row=row, column=3, value='最大值')
            ws.cell(row=row, column=4, value='平均值')
            ws.cell(row=row, column=5, value='中位数')

            for col in range(1, 6):
                cell = ws.cell(row=row, column=col)
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            row += 1

            # 数据
            risk_metrics = analysis_stats['risk_metrics']
            for metric_name, metric_stats in risk_metrics.items():
                ws.cell(row=row, column=1, value=metric_name)
                ws.cell(row=row, column=2, value=self._safe_round(metric_stats.get('min'), 2)).number_format = '0.00'
                ws.cell(row=row, column=3, value=self._safe_round(metric_stats.get('max'), 2)).number_format = '0.00'
                ws.cell(row=row, column=4, value=self._safe_round(metric_stats.get('mean'), 2)).number_format = '0.00'
                ws.cell(row=row, column=5, value=self._safe_round(metric_stats.get('median'), 2)).number_format = '0.00'
                row += 1

        # 调整列宽
        ws.column_dimensions['A'].width = 25
        for col in ['B', 'C', 'D', 'E']:
            ws.column_dimensions[col].width = 15

    def _create_bucket_summary_sheet(self, wb: Workbook, df: pd.DataFrame):
        """
        创建分桶汇总工作表（Step 3新增）

        按 ftype 分组统计：
        - bucket_total: 该类型基金总数
        - selected_count: 入选数量
        - selected_rate: 入选率
        - fallback_count: 降级数量（样本不足使用全局中位数）
        """
        ws = wb.create_sheet('分桶汇总')

        # 标题
        ws['A1'] = f'分桶汇总统计 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        ws.merge_cells('A1:F1')

        # 说明
        ws['A2'] = f'分桶字段: {BUCKET_KEY_FIELD} | 最小样本数: {BUCKET_MIN_SAMPLES}'
        ws['A2'].font = Font(size=10, italic=True, color='666666')
        ws.merge_cells('A2:F2')

        # 表头
        row = 4
        headers = ['基金类型', '入选数量', '组内总数', '入选率(%)', '降级数量', '降级率(%)']
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
            cell.alignment = Alignment(horizontal='center', vertical='center')
        row += 1

        # 按 bucket_key 分组统计
        if 'bucket_key' in df.columns and len(df) > 0:
            # 计算各分桶统计
            bucket_stats = df.groupby('bucket_key').agg({
                'fcode': 'count',  # 入选数量
                'bucket_size': 'first',  # 组内总数（每行相同）
                'is_fallback': 'sum'  # 降级数量
            }).reset_index()

            bucket_stats.columns = ['bucket_key', 'selected_count', 'bucket_total', 'fallback_count']

            # 计算比率
            bucket_stats['selected_rate'] = (bucket_stats['selected_count'] / bucket_stats['bucket_total'] * 100).round(2)
            bucket_stats['fallback_rate'] = (bucket_stats['fallback_count'] / bucket_stats['selected_count'] * 100).round(2)

            # 按入选数量降序排列
            bucket_stats = bucket_stats.sort_values('selected_count', ascending=False)

            # 填充数据
            for _, stat_row in bucket_stats.iterrows():
                ws.cell(row=row, column=1, value=stat_row['bucket_key'])
                ws.cell(row=row, column=2, value=int(stat_row['selected_count']))
                ws.cell(row=row, column=3, value=int(stat_row['bucket_total']))
                ws.cell(row=row, column=4, value=stat_row['selected_rate']).number_format = '0.00'
                ws.cell(row=row, column=5, value=int(stat_row['fallback_count']))
                ws.cell(row=row, column=6, value=stat_row['fallback_rate']).number_format = '0.00'

                # 设置边框
                for col in range(1, 7):
                    ws.cell(row=row, column=col).border = self._get_border('thin')

                # 交替行背景色
                if (row - 5) % 2 == 0:
                    for col in range(1, 7):
                        ws.cell(row=row, column=col).fill = PatternFill(
                            start_color='F0F0F0', end_color='F0F0F0', fill_type='solid'
                        )

                row += 1

            # 汇总行
            row += 1
            ws.cell(row=row, column=1, value='合计').font = Font(bold=True)
            ws.cell(row=row, column=2, value=int(bucket_stats['selected_count'].sum())).font = Font(bold=True)
            ws.cell(row=row, column=3, value='-')
            total_selected = bucket_stats['selected_count'].sum()
            total_fallback = bucket_stats['fallback_count'].sum()
            ws.cell(row=row, column=4, value='-')
            ws.cell(row=row, column=5, value=int(total_fallback)).font = Font(bold=True)
            if total_selected > 0:
                ws.cell(row=row, column=6, value=round(total_fallback / total_selected * 100, 2)).number_format = '0.00'
            else:
                ws.cell(row=row, column=6, value=0)

            for col in range(1, 7):
                ws.cell(row=row, column=col).border = self._get_border()
                ws.cell(row=row, column=col).fill = PatternFill(
                    start_color='E2EFDA', end_color='E2EFDA', fill_type='solid'
                )

        # 调整列宽
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 12
        ws.column_dimensions['F'].width = 12

    def _create_shadow_evaluation_sheet(self, wb: Workbook, df: pd.DataFrame):
        """
        创建影子评估汇总工作表（Step 4新增）

        统计不同回撤阈值下的淘汰情况：
        - 当前阈值（8%）: 实际入选数量
        - 5%阈值: 假设淘汰数量
        - 2%阈值: 假设淘汰数量
        """
        ws = wb.create_sheet('影子评估')

        # 标题
        ws['A1'] = f'影子评估汇总 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='8B4513', end_color='8B4513', fill_type='solid')
        ws.merge_cells('A1:E1')

        # 说明
        ws['A2'] = '影子评估：模拟不同回撤阈值下的筛选结果（不影响实际筛选）'
        ws['A2'].font = Font(size=10, italic=True, color='666666')
        ws.merge_cells('A2:E2')

        # 汇总统计表
        row = 4
        ws.cell(row=row, column=1, value='回撤阈值').font = Font(bold=True)
        ws.cell(row=row, column=2, value='入选数量').font = Font(bold=True)
        ws.cell(row=row, column=3, value='淘汰数量').font = Font(bold=True)
        ws.cell(row=row, column=4, value='淘汰率(%)').font = Font(bold=True)
        ws.cell(row=row, column=5, value='说明').font = Font(bold=True)

        for col in range(1, 6):
            ws.cell(row=row, column=col).fill = PatternFill(
                start_color='D9E1F2', end_color='D9E1F2', fill_type='solid'
            )
            ws.cell(row=row, column=col).border = self._get_border()
            ws.cell(row=row, column=col).alignment = Alignment(horizontal='center')

        total_count = len(df)

        # 当前阈值（8%）- 实际结果
        row += 1
        ws.cell(row=row, column=1, value='8%（当前）')
        ws.cell(row=row, column=2, value=total_count)
        ws.cell(row=row, column=3, value=0)
        ws.cell(row=row, column=4, value=0).number_format = '0.00'
        ws.cell(row=row, column=5, value='实际筛选结果')
        for col in range(1, 6):
            ws.cell(row=row, column=col).border = self._get_border('thin')
        ws.cell(row=row, column=1).fill = PatternFill(
            start_color='E2EFDA', end_color='E2EFDA', fill_type='solid'
        )

        # 5%阈值 - 影子评估
        if 'shadow_fail_5pct' in df.columns:
            row += 1
            fail_5pct = df['shadow_fail_5pct'].sum() if df['shadow_fail_5pct'].dtype == bool else int(df['shadow_fail_5pct'].sum())
            pass_5pct = total_count - fail_5pct
            fail_rate_5pct = (fail_5pct / total_count * 100) if total_count > 0 else 0

            ws.cell(row=row, column=1, value='5%（影子）')
            ws.cell(row=row, column=2, value=pass_5pct)
            ws.cell(row=row, column=3, value=fail_5pct)
            ws.cell(row=row, column=4, value=round(fail_rate_5pct, 2)).number_format = '0.00'
            ws.cell(row=row, column=5, value='假设使用5%阈值')
            for col in range(1, 6):
                ws.cell(row=row, column=col).border = self._get_border('thin')
            ws.cell(row=row, column=1).fill = PatternFill(
                start_color='FFF2CC', end_color='FFF2CC', fill_type='solid'
            )

        # 2%阈值 - 影子评估
        if 'shadow_fail_2pct' in df.columns:
            row += 1
            fail_2pct = df['shadow_fail_2pct'].sum() if df['shadow_fail_2pct'].dtype == bool else int(df['shadow_fail_2pct'].sum())
            pass_2pct = total_count - fail_2pct
            fail_rate_2pct = (fail_2pct / total_count * 100) if total_count > 0 else 0

            ws.cell(row=row, column=1, value='2%（影子）')
            ws.cell(row=row, column=2, value=pass_2pct)
            ws.cell(row=row, column=3, value=fail_2pct)
            ws.cell(row=row, column=4, value=round(fail_rate_2pct, 2)).number_format = '0.00'
            ws.cell(row=row, column=5, value='假设使用2%阈值')
            for col in range(1, 6):
                ws.cell(row=row, column=col).border = self._get_border('thin')
            ws.cell(row=row, column=1).fill = PatternFill(
                start_color='FCE4D6', end_color='FCE4D6', fill_type='solid'
            )

        # 按基金类型分组的影子评估统计
        row += 2
        ws.cell(row=row, column=1, value='按基金类型分组统计').font = Font(bold=True, size=12)
        ws.merge_cells(f'A{row}:E{row}')

        row += 1
        headers = ['基金类型', '入选数量', '5%淘汰数', '2%淘汰数', '2%淘汰率(%)']
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
            cell.alignment = Alignment(horizontal='center')

        # 按 ftype 分组统计
        if 'ftype' in df.columns and len(df) > 0:
            agg_dict = {'fcode': 'count'}
            if 'shadow_fail_5pct' in df.columns:
                agg_dict['shadow_fail_5pct'] = 'sum'
            if 'shadow_fail_2pct' in df.columns:
                agg_dict['shadow_fail_2pct'] = 'sum'

            type_stats = df.groupby('ftype').agg(agg_dict).reset_index()
            type_stats.columns = ['ftype', 'count'] + [c for c in type_stats.columns[2:]]

            # 按数量降序排列
            type_stats = type_stats.sort_values('count', ascending=False)

            for _, stat_row in type_stats.iterrows():
                row += 1
                ws.cell(row=row, column=1, value=stat_row['ftype'])
                ws.cell(row=row, column=2, value=int(stat_row['count']))

                fail_5 = int(stat_row.get('shadow_fail_5pct', 0)) if 'shadow_fail_5pct' in stat_row else 0
                fail_2 = int(stat_row.get('shadow_fail_2pct', 0)) if 'shadow_fail_2pct' in stat_row else 0
                count = stat_row['count']

                ws.cell(row=row, column=3, value=fail_5)
                ws.cell(row=row, column=4, value=fail_2)
                fail_rate = (fail_2 / count * 100) if count > 0 else 0
                ws.cell(row=row, column=5, value=round(fail_rate, 2)).number_format = '0.00'

                for col in range(1, 6):
                    ws.cell(row=row, column=col).border = self._get_border('thin')

                # 交替行背景色
                if (row % 2) == 0:
                    for col in range(1, 6):
                        ws.cell(row=row, column=col).fill = PatternFill(
                            start_color='F0F0F0', end_color='F0F0F0', fill_type='solid'
                        )

        # 调整列宽
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 14

    def _create_freshness_summary_sheet(self, wb: Workbook, df: pd.DataFrame):
        """
        创建新鲜度统计工作表（Step 5D新增）

        统计数据新鲜度分布：
        - 全局汇总：新鲜/过期/无净值 数量与占比
        - 影子阈值对比：7d/14d/30d 不同阈值下的过期情况
        - 按基金类型分组统计
        """
        ws = wb.create_sheet('新鲜度统计')

        # ── 标题 ──
        ws['A1'] = f'数据新鲜度统计 - {self.period_name}'
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='2E7D32', end_color='2E7D32', fill_type='solid')
        ws.merge_cells('A1:F1')

        # 说明
        ws['A2'] = '数据新鲜度：基于最新净值日期判断基金数据是否过期（soft模式仅标记，hard模式淘汰）'
        ws['A2'].font = Font(size=10, italic=True, color='666666')
        ws.merge_cells('A2:F2')

        total_count = len(df)

        # ── Part 1: 全局新鲜度汇总 ──
        row = 4
        ws.cell(row=row, column=1, value='全局新鲜度汇总').font = Font(bold=True, size=12)
        ws.merge_cells(f'A{row}:F{row}')

        row += 1
        headers_p1 = ['分类', '数量', '占比(%)', '说明']
        for col_idx, header in enumerate(headers_p1, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
            cell.alignment = Alignment(horizontal='center')

        # 新鲜基金
        fresh_count = int(df['is_fresh'].sum()) if total_count > 0 else 0
        stale_count = int((~df['is_fresh'] & df['nav_age_days'].notna()).sum()) if total_count > 0 else 0
        no_nav_count = int(df['nav_age_days'].isna().sum()) if total_count > 0 else 0

        summary_rows = [
            ('新鲜（is_fresh=True）', fresh_count, '数据在有效期内', 'E2EFDA'),
            ('过期（NAV_TOO_OLD）', stale_count, '净值日期超过阈值', 'FFF2CC'),
            ('无净值（NO_NAV_HISTORY）', no_nav_count, '无净值历史记录', 'FCE4D6'),
            ('合计', total_count, '全部入选基金', 'D9E1F2'),
        ]

        for label, count, desc, color in summary_rows:
            row += 1
            ws.cell(row=row, column=1, value=label)
            ws.cell(row=row, column=2, value=count)
            pct = round(count / total_count * 100, 2) if total_count > 0 else 0
            ws.cell(row=row, column=3, value=pct).number_format = '0.00'
            ws.cell(row=row, column=4, value=desc)
            for col in range(1, 5):
                ws.cell(row=row, column=col).border = self._get_border('thin')
            ws.cell(row=row, column=1).fill = PatternFill(
                start_color=color, end_color=color, fill_type='solid'
            )

        # ── Part 2: 影子阈值对比 ──
        row += 2
        ws.cell(row=row, column=1, value='影子阈值对比（不同过期天数下的过期数量）').font = Font(bold=True, size=12)
        ws.merge_cells(f'A{row}:F{row}')

        row += 1
        headers_p2 = ['过期阈值', '过期数量', '过期率(%)', '说明']
        for col_idx, header in enumerate(headers_p2, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
            cell.alignment = Alignment(horizontal='center')

        shadow_thresholds = [
            ('shadow_age_fail_7d', '7天', 'FFF2CC'),
            ('shadow_age_fail_14d', '14天', 'FCE4D6'),
            ('shadow_age_fail_30d', '30天', 'F8D7DA'),
        ]

        for col_name, label, color in shadow_thresholds:
            if col_name in df.columns:
                row += 1
                fail_count = int(df[col_name].sum()) if total_count > 0 else 0
                fail_rate = round(fail_count / total_count * 100, 2) if total_count > 0 else 0

                ws.cell(row=row, column=1, value=f'{label}（影子）')
                ws.cell(row=row, column=2, value=fail_count)
                ws.cell(row=row, column=3, value=fail_rate).number_format = '0.00'
                ws.cell(row=row, column=4, value=f'假设使用{label}过期阈值')
                for col in range(1, 5):
                    ws.cell(row=row, column=col).border = self._get_border('thin')
                ws.cell(row=row, column=1).fill = PatternFill(
                    start_color=color, end_color=color, fill_type='solid'
                )

        # ── Part 3: 按基金类型分组统计 ──
        row += 2
        ws.cell(row=row, column=1, value='按基金类型分组统计').font = Font(bold=True, size=12)
        ws.merge_cells(f'A{row}:F{row}')

        row += 1
        headers_p3 = ['基金类型', '总数', '新鲜数', '过期数', '无净值数', '新鲜率(%)']
        for col_idx, header in enumerate(headers_p3, start=1):
            cell = ws.cell(row=row, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
            cell.border = self._get_border()
            cell.alignment = Alignment(horizontal='center')

        if 'ftype' in df.columns and len(df) > 0:
            # 按 ftype 分组计算新鲜度统计
            def _freshness_agg(group):
                g_total = len(group)
                g_fresh = int(group['is_fresh'].sum())
                g_stale = int((~group['is_fresh'] & group['nav_age_days'].notna()).sum())
                g_no_nav = int(group['nav_age_days'].isna().sum())
                g_fresh_rate = round(g_fresh / g_total * 100, 2) if g_total > 0 else 0
                return pd.Series({
                    'total': g_total,
                    'fresh': g_fresh,
                    'stale': g_stale,
                    'no_nav': g_no_nav,
                    'fresh_rate': g_fresh_rate,
                })

            type_stats = df.groupby('ftype').apply(_freshness_agg, include_groups=False).reset_index()
            type_stats = type_stats.sort_values('total', ascending=False)

            for _, stat_row in type_stats.iterrows():
                row += 1
                ws.cell(row=row, column=1, value=stat_row['ftype'])
                ws.cell(row=row, column=2, value=int(stat_row['total']))
                ws.cell(row=row, column=3, value=int(stat_row['fresh']))
                ws.cell(row=row, column=4, value=int(stat_row['stale']))
                ws.cell(row=row, column=5, value=int(stat_row['no_nav']))
                ws.cell(row=row, column=6, value=stat_row['fresh_rate']).number_format = '0.00'

                for col in range(1, 7):
                    ws.cell(row=row, column=col).border = self._get_border('thin')

                # 交替行背景色
                if (row % 2) == 0:
                    for col in range(1, 7):
                        ws.cell(row=row, column=col).fill = PatternFill(
                            start_color='F0F0F0', end_color='F0F0F0', fill_type='solid'
                        )

        # 调整列宽
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 12
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 14
        ws.column_dimensions['F'].width = 14

    def _get_border(self, style='medium'):
        """获取边框样式"""
        return Border(
            left=Side(style=style),
            right=Side(style=style),
            top=Side(style=style),
            bottom=Side(style=style)
        )

    def _get_column_letter(self, col_idx: int) -> str:
        """
        将列索引转换为列字母

        Args:
            col_idx: 列索引（从1开始）

        Returns:
            列字母（如'A', 'B', 'AA'等）
        """
        from openpyxl.utils import get_column_letter
        return get_column_letter(col_idx)
