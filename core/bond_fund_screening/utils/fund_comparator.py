#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金交集对比工具 - FundComparator

功能：
1. 读取两个Excel文件（例如长期、短期优质基金）
2. 提取基金代码列表
3. 计算交集（同时出现在两个文件中的基金）
4. 合并两个文件的数据（保留更全面的字段）
5. 生成新的Excel报告

使用示例：
    from bond_fund_screening.utils.fund_comparator import FundComparator

    comparator = FundComparator()
    result = comparator.compare_and_merge(
        file1='长期优质基金_20251112_080000.xlsx',
        file2='近期优质基金_20251112_080500.xlsx',
        output_file='当日最终优质基金_20251112_080600.xlsx'
    )
    print(f"交集基金数量: {result['intersection_count']}")
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import sys

from bond_fund_screening.config import COLUMN_NAME_MAPPING, FUND_TYPE_MAPPING
from bond_fund_screening.core.filtering_engine import stable_sort_desc


def _normalize_fcode(value) -> str:
    """统一基金代码格式，兼容 Excel 数值列与文本小数表示。"""
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


class FundComparator:
    """
    基金交集对比工具类

    用于对比两个基金筛选结果文件，找出交集并生成合并报告
    """

    def __init__(self, verbose: bool = True):
        """
        初始化对比工具

        Args:
            verbose: 是否显示详细信息
        """
        self.verbose = verbose

    def _log(self, message: str):
        """输出日志"""
        if self.verbose:
            print(message)

    def _read_excel_first_sheet(self, file_path: Path) -> pd.DataFrame:
        """
        读取Excel文件的第一个工作表

        Args:
            file_path: Excel文件路径

        Returns:
            DataFrame
        """
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        # 读取第一个sheet
        df = pd.read_excel(file_path, sheet_name=0)

        # 如果列名是中文，需要反向映射回英文字段名
        reverse_mapping = {v: k for k, v in COLUMN_NAME_MAPPING.items()}
        df = df.rename(columns=reverse_mapping)
        if 'fcode' in df.columns:
            df['fcode'] = df['fcode'].map(_normalize_fcode)

        self._log(f"  📂 读取文件: {file_path.name}")
        self._log(f"     基金数量: {len(df)}")

        return df

    def _extract_fund_codes(self, df: pd.DataFrame) -> set:
        """
        从DataFrame中提取基金代码集合

        Args:
            df: 基金数据DataFrame

        Returns:
            基金代码集合
        """
        if 'fcode' not in df.columns:
            raise ValueError("DataFrame中缺少fcode列")

        return {
            code
            for code in df['fcode'].map(_normalize_fcode).tolist()
            if code
        }

    def _calculate_intersection(self, codes1: set, codes2: set) -> set:
        """
        计算两个基金代码集合的交集

        Args:
            codes1: 第一个基金代码集合
            codes2: 第二个基金代码集合

        Returns:
            交集基金代码集合
        """
        intersection = codes1 & codes2
        self._log(f"\n  📊 交集统计:")
        self._log(f"     文件1基金数: {len(codes1)}")
        self._log(f"     文件2基金数: {len(codes2)}")
        self._log(f"     交集基金数: {len(intersection)}")

        return intersection

    def _merge_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame,
                         intersection_codes: set) -> pd.DataFrame:
        """
        合并两个DataFrame，保留交集基金的数据

        合并策略：
        1. 保留两个文件共有的列
        2. 对于数值列，计算平均值
        3. 对于文本列，优先使用第一个文件的值
        4. 添加来源标记列

        Args:
            df1: 第一个DataFrame
            df2: 第二个DataFrame
            intersection_codes: 交集基金代码集合

        Returns:
            合并后的DataFrame
        """
        # 筛选出交集基金
        df1_filtered = df1.copy()
        df2_filtered = df2.copy()
        df1_filtered['fcode'] = df1_filtered['fcode'].map(_normalize_fcode)
        df2_filtered['fcode'] = df2_filtered['fcode'].map(_normalize_fcode)
        df1_filtered = df1_filtered[df1_filtered['fcode'].isin(intersection_codes)].copy()
        df2_filtered = df2_filtered[df2_filtered['fcode'].isin(intersection_codes)].copy()

        # 将fcode列转换为字符串类型（保持与intersection_codes一致）
        df1_filtered['fcode'] = df1_filtered['fcode'].map(_normalize_fcode)
        df2_filtered['fcode'] = df2_filtered['fcode'].map(_normalize_fcode)

        # 设置fcode为索引以便合并
        df1_filtered.set_index('fcode', inplace=True)
        df2_filtered.set_index('fcode', inplace=True)

        # 获取共有列
        common_columns = list(set(df1_filtered.columns) & set(df2_filtered.columns))

        # 基础信息列（优先使用df1的值）
        base_info_cols = ['shortname', 'ftype', 'estab_date', 'scale_billion']

        # 开始合并
        merged_data = []

        for fcode in intersection_codes:
            if fcode not in df1_filtered.index or fcode not in df2_filtered.index:
                continue

            row1 = df1_filtered.loc[fcode]
            row2 = df2_filtered.loc[fcode]

            merged_row = {'fcode': fcode}

            # 处理每一列
            for col in common_columns:
                val1 = row1[col] if col in row1.index else None
                val2 = row2[col] if col in row2.index else None

                # 基础信息优先使用df1
                if col in base_info_cols:
                    merged_row[col] = val1 if pd.notna(val1) else val2
                # 数值列计算平均值
                elif pd.api.types.is_numeric_dtype(df1_filtered[col]) or \
                     pd.api.types.is_numeric_dtype(df2_filtered[col]):
                    if pd.notna(val1) and pd.notna(val2):
                        merged_row[col] = (float(val1) + float(val2)) / 2
                    elif pd.notna(val1):
                        merged_row[col] = val1
                    else:
                        merged_row[col] = val2
                # 文本列优先使用df1
                else:
                    merged_row[col] = val1 if pd.notna(val1) else val2

            merged_data.append(merged_row)

        # 创建合并后的DataFrame
        df_merged = pd.DataFrame(merged_data)

        # 添加来源标记
        df_merged['data_source'] = '长短期交集'

        # 如果有综合评分列，按评分排序
        if 'comprehensive_score' in df_merged.columns:
            df_merged = stable_sort_desc(df_merged, 'comprehensive_score')
        elif 'avg_combo_score' in df_merged.columns:
            df_merged = stable_sort_desc(df_merged, 'avg_combo_score')
        elif 'combo_score' in df_merged.columns:
            df_merged = stable_sort_desc(df_merged, 'combo_score')

        self._log(f"\n  ✅ 数据合并完成")
        self._log(f"     合并列数: {len(df_merged.columns)}")
        self._log(f"     合并行数: {len(df_merged)}")

        return df_merged

    def _prepare_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备DataFrame用于导出，处理列名和格式

        Args:
            df: 原始DataFrame

        Returns:
            处理后的DataFrame
        """
        df_export = df.copy()

        # 先对所有数值列进行精度处理（四舍五入到2位小数）
        for col in df_export.columns:
            if df_export[col].dtype in ['float64', 'float32']:
                # 对数值列四舍五入到2位小数，避免浮点数精度问题
                df_export[col] = df_export[col].round(2)

        # 转换基金类型为中文
        if 'ftype' in df_export.columns:
            df_export['ftype'] = df_export['ftype'].apply(
                lambda x: FUND_TYPE_MAPPING.get(str(x), FUND_TYPE_MAPPING.get(int(x) if pd.notna(x) and str(x).isdigit() else x, str(x)))
            )

        # 列重命名为中文
        column_mapping = {}
        for col in df_export.columns:
            if col in COLUMN_NAME_MAPPING:
                column_mapping[col] = COLUMN_NAME_MAPPING[col]

        df_export = df_export.rename(columns=column_mapping)

        # 重新排列列顺序（如果存在这些列）
        priority_cols = ['fcode', 'shortname', 'ftype', 'comprehensive_score',
                        'avg_combo_score', 'combo_score', 'avg_return', 'main_return',
                        'scale_billion', 'sharpe_ratio', 'data_source']

        # 构建新的列顺序
        ordered_cols = []
        for col in priority_cols:
            # 检查英文列名和中文列名
            if col in df_export.columns:
                ordered_cols.append(col)
            elif col in COLUMN_NAME_MAPPING and COLUMN_NAME_MAPPING[col] in df_export.columns:
                ordered_cols.append(COLUMN_NAME_MAPPING[col])

        # 添加剩余列
        for col in df_export.columns:
            if col not in ordered_cols:
                ordered_cols.append(col)

        df_export = df_export[ordered_cols]

        return df_export

    def _add_metric_explanations(self, ws, header_values):
        """
        在工作表底部添加指标说明

        Args:
            ws: openpyxl工作表对象
            header_values: 表头列表
        """
        # 找到数据结束行（最后一行有数据的行）
        last_data_row = ws.max_row

        # 定义说明区域样式
        title_fill = PatternFill(start_color='FFE699', end_color='FFE699', fill_type='solid')
        title_font = Font(bold=True, size=12, color='000000')
        title_alignment = Alignment(horizontal='center', vertical='center')

        metric_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')
        metric_font = Font(size=10)
        metric_alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)

        border_side = Side(style='thin', color='000000')
        border = Border(left=border_side, right=border_side, top=border_side, bottom=border_side)

        # 空2行
        start_row = last_data_row + 3

        # 添加标题行
        title_cell = ws.cell(row=start_row, column=1)
        title_cell.value = '📊 指标说明 - 帮助您理解为什么这些是优质基金'
        title_cell.fill = title_fill
        title_cell.font = title_font
        title_cell.alignment = title_alignment
        title_cell.border = border

        # 合并标题单元格（跨3列）
        ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=3)

        # 定义指标说明数据
        explanations = [
            {
                '指标名称': '综合稳定性得分',
                '指标含义': '跨长期和短期的综合稳定性评价（0-100分）',
                '计算方式': '周期数得分40% + 平均评分35% + 稳定性得分25%。出现周期越多、评分越高越稳定，得分越高'
            },
            {
                '指标名称': '出现周期数',
                '指标含义': '基金在多少个分析周期中都表现优秀',
                '计算方式': '统计基金同时出现在几个周期的Top150榜单中。最少2个周期，最多可达9个周期（5个长期+4个短期）'
            },
            {
                '指标名称': '周期数得分',
                '指标含义': '基于出现周期数的标准化得分（0-100分）',
                '计算方式': '(出现周期数 / 最大周期数) × 100。周期数越多表示基金在不同时间跨度下都表现优秀'
            },
            {
                '指标名称': '平均综合评分',
                '指标含义': '各周期综合评分的平均值（0-100分）',
                '计算方式': '对基金在各个周期的综合评分求平均。综合评分=短期潜力分40% + 长期价值分60%'
            },
            {
                '指标名称': '稳定性得分',
                '指标含义': '评分波动性的反向评价（0-100分）',
                '计算方式': '100 - (评分标准差 / 最大标准差) × 100。标准差越小表示各周期表现越稳定，得分越高'
            },
            {
                '指标名称': '平均收益率(%)',
                '指标含义': '各周期收益率的平均值',
                '计算方式': '对基金在各个周期的主要收益率求平均。反映基金的整体盈利能力'
            },
            {
                '指标名称': '收益率标准差(%)',
                '指标含义': '收益率的波动程度',
                '计算方式': '各周期收益率的标准差。数值越小表示收益越稳定，风险越低'
            },
            {
                '指标名称': '平均夏普比率',
                '指标含义': '风险调整后的收益评价指标',
                '计算方式': '(年化收益率 - 无风险利率) / 波动率。数值越高表示单位风险的超额收益越高，投资性价比越好'
            },
            {
                '指标名称': '基金规模(亿元)',
                '指标含义': '基金的资产管理规模',
                '计算方式': '基金净资产总额。规模适中的基金（1-50亿）通常流动性好且灵活性强'
            },
            {
                '指标名称': '评分标准差',
                '指标含义': '各周期评分的离散程度',
                '计算方式': '各周期综合评分的标准差。数值越小表示各周期表现越一致'
            }
        ]

        # 添加说明表格表头
        header_row = start_row + 1
        headers = ['指标名称', '指标含义', '计算方式']

        header_font_bold = Font(bold=True, size=10)
        header_fill_gray = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')

        for col_idx, header in enumerate(headers, 1):
            cell = ws.cell(row=header_row, column=col_idx)
            cell.value = header
            cell.font = header_font_bold
            cell.fill = header_fill_gray
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = border

        # 设置说明表格列宽
        ws.column_dimensions['A'].width = 20
        ws.column_dimensions['B'].width = 35
        ws.column_dimensions['C'].width = 65

        # 添加说明数据行
        current_row = header_row + 1
        for explanation in explanations:
            for col_idx, header in enumerate(headers, 1):
                cell = ws.cell(row=current_row, column=col_idx)
                cell.value = explanation[header]
                cell.font = metric_font
                cell.fill = metric_fill if current_row % 2 == 0 else PatternFill(start_color='FFFFFF', end_color='FFFFFF', fill_type='solid')
                cell.alignment = metric_alignment
                cell.border = border

            # 设置行高（使文字能完整显示）
            ws.row_dimensions[current_row].height = 45
            current_row += 1

        # 添加底部说明文字
        note_row = current_row + 1
        note_cell = ws.cell(row=note_row, column=1)
        note_cell.value = '💡 说明：以上基金同时满足长期（1-5年）和短期（1周-6月）多周期稳定优秀表现，是经过严格筛选的高质量债券基金。'
        note_cell.font = Font(size=9, italic=True, color='666666')
        note_cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        ws.merge_cells(start_row=note_row, start_column=1, end_row=note_row, end_column=3)
        ws.row_dimensions[note_row].height = 30

    def _apply_excel_styling(self, excel_file: Path):
        """
        为Excel文件应用样式和格式化

        Args:
            excel_file: Excel文件路径
        """
        wb = None
        try:
            wb = load_workbook(excel_file)
            ws = wb.active

            # 定义样式
            header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
            header_font = Font(bold=True, color='FFFFFF', size=11)
            header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

            holding_fill = PatternFill(start_color='FFF9E6', end_color='FFF9E6', fill_type='solid')
            center_alignment = Alignment(horizontal='center', vertical='center')
            left_alignment = Alignment(horizontal='left', vertical='center')

            # 获取表头
            header_row = list(ws.iter_rows(min_row=1, max_row=1, values_only=False))[0]
            header_values = [cell.value for cell in header_row]

            # 应用表头样式
            for cell in header_row:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = header_alignment

            # 查找shortname列索引
            shortname_col_idx = None
            for idx, val in enumerate(header_values):
                if val and ('shortname' in str(val).lower() or '基金简称' in str(val)):
                    shortname_col_idx = idx
                    break

            # 遍历数据行
            for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=False), start=2):
                # 检查是否为持有期基金
                is_holding_fund = False
                if shortname_col_idx is not None and row[shortname_col_idx].value:
                    shortname_value = str(row[shortname_col_idx].value)
                    if '持有' in shortname_value:
                        is_holding_fund = True

                # 应用样式
                for col_idx, cell in enumerate(row):
                    if is_holding_fund:
                        cell.fill = holding_fill

                    # 对齐方式
                    col_name = header_values[col_idx] if col_idx < len(header_values) else ''
                    if any(text in str(col_name) for text in ['代码', '简称', '类型', '来源', 'fcode', 'shortname', 'ftype']):
                        cell.alignment = left_alignment
                    else:
                        cell.alignment = center_alignment

                    # 数值格式（统一使用2位小数）
                    if cell.value is not None and isinstance(cell.value, (int, float)):
                        # 代码和周期数等整数列不需要小数
                        if any(text in str(col_name) for text in ['代码', 'fcode', '周期数', '类型', 'period_count']):
                            cell.number_format = '0'
                        # 其他数值列统一2位小数
                        elif any(text in str(col_name) for text in [
                            '评分', 'score', '得分',
                            '收益', 'return',
                            '规模', 'scale',
                            '夏普', 'sharpe', 'ratio',
                            '标准差', 'std',
                            '稳定性', 'stability',
                            '平均', 'avg',
                            '最低', 'min',
                            '最高', 'max'
                        ]):
                            cell.number_format = '0.00'

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

                adjusted_width = min(max(max_length + 2, 10), 50)
                ws.column_dimensions[col_letter].width = adjusted_width

            # 在数据表格下方添加指标说明
            self._add_metric_explanations(ws, header_values)

            wb.save(excel_file)
            self._log(f"  🎨 已应用Excel样式和指标说明")

        except Exception as e:
            self._log(f"  ⚠️  应用样式时出错: {e}")
        finally:
            if wb is not None:
                wb.close()

    def compare_and_merge(self, file1: str, file2: str, output_file: str) -> Dict:
        """
        对比两个Excel文件并生成交集合并报告

        Args:
            file1: 第一个Excel文件路径（例如长期优质基金）
            file2: 第二个Excel文件路径（例如短期优质基金）
            output_file: 输出文件路径

        Returns:
            结果字典，包含：
            - intersection_count: 交集基金数量
            - file1_count: 文件1基金数量
            - file2_count: 文件2基金数量
            - output_file: 输出文件路径
            - success: 是否成功
        """
        self._log("\n" + "="*80)
        self._log("【基金交集对比分析】")
        self._log("="*80)

        try:
            # 1. 读取两个Excel文件
            self._log("\n步骤1: 读取Excel文件")
            path1 = Path(file1)
            path2 = Path(file2)

            df1 = self._read_excel_first_sheet(path1)
            df2 = self._read_excel_first_sheet(path2)

            # 2. 提取基金代码
            self._log("\n步骤2: 提取基金代码")
            codes1 = self._extract_fund_codes(df1)
            codes2 = self._extract_fund_codes(df2)

            # 3. 计算交集
            self._log("\n步骤3: 计算交集")
            intersection = self._calculate_intersection(codes1, codes2)

            if len(intersection) == 0:
                self._log("\n⚠️  警告: 两个文件没有交集基金！")
                return {
                    'success': False,
                    'intersection_count': 0,
                    'file1_count': len(codes1),
                    'file2_count': len(codes2),
                    'output_file': None,
                    'message': '没有交集基金'
                }

            # 4. 合并数据
            self._log("\n步骤4: 合并基金数据")
            df_merged = self._merge_dataframes(df1, df2, intersection)

            # 5. 准备导出
            self._log("\n步骤5: 准备Excel导出")
            df_export = self._prepare_for_export(df_merged)

            # 6. 保存Excel
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df_export.to_excel(output_path, index=False, engine='openpyxl')
            self._log(f"  💾 保存文件: {output_path}")

            # 7. 应用样式
            self._apply_excel_styling(output_path)

            self._log("\n" + "="*80)
            self._log("✅ 交集对比分析完成！")
            self._log("="*80)

            return {
                'success': True,
                'intersection_count': len(intersection),
                'file1_count': len(codes1),
                'file2_count': len(codes2),
                'output_file': str(output_path),
                'message': '交集分析成功'
            }

        except Exception as e:
            self._log(f"\n❌ 对比分析失败: {e}")
            import traceback
            traceback.print_exc()

            return {
                'success': False,
                'intersection_count': 0,
                'file1_count': 0,
                'file2_count': 0,
                'output_file': None,
                'message': str(e)
            }


def main():
    """测试函数"""
    import argparse

    parser = argparse.ArgumentParser(description='基金交集对比工具')
    parser.add_argument('--file1', required=True, help='第一个Excel文件路径')
    parser.add_argument('--file2', required=True, help='第二个Excel文件路径')
    parser.add_argument('--output', required=True, help='输出文件路径')
    parser.add_argument('--quiet', action='store_true', help='静默模式')

    args = parser.parse_args()

    comparator = FundComparator(verbose=not args.quiet)
    result = comparator.compare_and_merge(args.file1, args.file2, args.output)

    if result['success']:
        print(f"\n✅ 成功生成交集报告: {result['output_file']}")
        print(f"   交集基金数: {result['intersection_count']}")
        return 0
    else:
        print(f"\n❌ 失败: {result['message']}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
