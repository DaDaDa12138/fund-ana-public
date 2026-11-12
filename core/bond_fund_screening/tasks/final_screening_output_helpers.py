#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选输出、Excel 导出与重命名辅助工具。"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from bond_fund_screening.report_artifact_metadata import (
    rename_report_with_sidecar,
    serialize_output_path_reference,
)
from bond_fund_screening.report_catalog import atomic_write_json_file, rebuild_report_manifest


def build_simple_csv_frame(df: pd.DataFrame) -> pd.DataFrame:
    """将筛选结果规整为用户下载用的简版 CSV。"""
    df_simple = df[['fcode']].copy()
    if '基金名称' in df.columns:
        df_simple['基金名称'] = df['基金名称']
    elif 'shortname' in df.columns:
        df_simple['基金名称'] = df['shortname']
    df_simple.columns = ['代码', '名称']
    return df_simple


def export_simple_csv(df: pd.DataFrame, output_file: Path) -> Path:
    """导出简版 CSV。"""
    build_simple_csv_frame(df).to_csv(output_file, index=False, encoding='utf-8-sig')
    return output_file


def write_comparison_report_workbook(
    *,
    output_file: Path,
    sheet_frames: Sequence[tuple[str, pd.DataFrame]],
    holding_period_detector: Callable[[str], bool],
) -> Path:
    """写出并美化最终筛选对比报告工作簿。"""
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in sheet_frames:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    wb = load_workbook(output_file)
    try:
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_font = Font(name='微软雅黑', size=11, bold=True, color="FFFFFF")
        header_alignment = Alignment(horizontal='center', vertical='center')

        even_row_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
        odd_row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
        holding_period_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

        red_font = Font(name='微软雅黑', size=11, color="FF0000", bold=True)
        green_font = Font(name='微软雅黑', size=11, color="008000", bold=True)
        black_font = Font(name='微软雅黑', size=11, color="000000")

        border = Border(
            left=Side(style='thin', color='D0D0D0'),
            right=Side(style='thin', color='D0D0D0'),
            top=Side(style='thin', color='D0D0D0'),
            bottom=Side(style='thin', color='D0D0D0')
        )

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]

            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = header_alignment
                cell.border = border

            fund_name_col_idx = None
            daily_growth_col_idx = None
            for idx, cell in enumerate(ws[1], start=1):
                if cell.value:
                    if '基金名称' in str(cell.value) or 'shortname' in str(cell.value):
                        fund_name_col_idx = idx
                    elif '最新日增长率' in str(cell.value):
                        daily_growth_col_idx = idx

            for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                is_holding_period_fund = False
                if fund_name_col_idx and sheet_name == '3-交集基金':
                    fund_name_cell = ws.cell(row=row_idx, column=fund_name_col_idx)
                    if fund_name_cell.value:
                        is_holding_period_fund = holding_period_detector(str(fund_name_cell.value))

                fill = holding_period_fill if is_holding_period_fund else (
                    even_row_fill if row_idx % 2 == 0 else odd_row_fill
                )

                for col_idx, cell in enumerate(row, start=1):
                    cell.fill = fill
                    cell.border = border
                    cell.alignment = Alignment(horizontal='left', vertical='center')

                    if daily_growth_col_idx and col_idx == daily_growth_col_idx and sheet_name == '3-交集基金':
                        if cell.value is not None:
                            try:
                                growth_value = float(cell.value)
                                if growth_value > 0:
                                    cell.font = red_font
                                elif growth_value < 0:
                                    cell.font = green_font
                                else:
                                    cell.font = black_font
                            except (ValueError, TypeError):
                                pass

            for column in ws.columns:
                max_length = 0
                column_letter = get_column_letter(column[0].column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except (TypeError, AttributeError):
                        pass
                ws.column_dimensions[column_letter].width = min(max_length + 2, 50)

            ws.freeze_panes = 'A2'

        wb.save(output_file)
    finally:
        wb.close()

    return output_file


def write_final_fund_workbook(
    *,
    final_fund_file: Path,
    df_final: pd.DataFrame,
    holding_period_detector: Callable[[str], bool],
) -> int:
    """写出并美化当日最终优质基金工作簿，返回定开/持有期基金数量。"""
    df_final.to_excel(final_fund_file, index=False, engine='openpyxl')

    wb = load_workbook(final_fund_file)
    try:
        ws = wb.active

        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_font = Font(name='微软雅黑', size=11, bold=True, color="FFFFFF")
        holding_period_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

        red_font = Font(name='微软雅黑', size=10, color="FF0000", bold=True)
        green_font = Font(name='微软雅黑', size=10, color="008000", bold=True)
        normal_font = Font(name='微软雅黑', size=10, color="000000")

        thin_border = Border(
            left=Side(style='thin', color='B4B4B4'),
            right=Side(style='thin', color='B4B4B4'),
            top=Side(style='thin', color='B4B4B4'),
            bottom=Side(style='thin', color='B4B4B4')
        )

        center_alignment = Alignment(horizontal='center', vertical='center')
        left_alignment = Alignment(horizontal='left', vertical='center')

        growth_cols = []
        for col_idx, col_name in enumerate(df_final.columns, 1):
            if '(%)' in col_name and (
                '增长' in col_name or '收益' in col_name or '年' in col_name or '月' in col_name or '周' in col_name
            ):
                growth_cols.append(col_idx)

        fund_name_col_idx = None
        for col_idx, col_name in enumerate(df_final.columns, 1):
            if col_name == '基金名称':
                fund_name_col_idx = col_idx
                break

        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_alignment
            cell.border = thin_border

        holding_period_count = 0
        for row_idx in range(2, ws.max_row + 1):
            is_holding_period = False
            if fund_name_col_idx:
                fund_name_cell = ws.cell(row=row_idx, column=fund_name_col_idx)
                if fund_name_cell.value and holding_period_detector(str(fund_name_cell.value)):
                    is_holding_period = True
                    holding_period_count += 1

            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = thin_border

                if is_holding_period:
                    cell.fill = holding_period_fill

                if col_idx in growth_cols and cell.value is not None:
                    try:
                        value = float(cell.value)
                        if value > 0:
                            cell.font = red_font
                        elif value < 0:
                            cell.font = green_font
                        else:
                            cell.font = normal_font
                    except (ValueError, TypeError):
                        cell.font = normal_font
                elif not is_holding_period:
                    cell.font = normal_font

                if col_idx == 1:
                    cell.alignment = center_alignment
                elif col_idx == fund_name_col_idx:
                    cell.alignment = left_alignment
                else:
                    cell.alignment = center_alignment

        column_widths = {
            '基金代码': 12,
            '基金名称': 25,
            '基金类型': 14,
            '风险等级': 10,
            '成立日期': 12,
            '基金规模(亿)': 14,
            '基金公司': 18,
            '基金经理': 15,
            'AutoStock校验状态': 16,
            'AutoStock校验备注': 32,
        }

        for col_idx in range(1, ws.max_column + 1):
            col_name = ws.cell(row=1, column=col_idx).value
            if col_name in column_widths:
                ws.column_dimensions[get_column_letter(col_idx)].width = column_widths[col_name]
            elif '(%)' in str(col_name):
                ws.column_dimensions[get_column_letter(col_idx)].width = 14
            else:
                ws.column_dimensions[get_column_letter(col_idx)].width = 12

        ws.freeze_panes = 'A2'
        wb.save(final_fund_file)
    finally:
        wb.close()

    return holding_period_count


def _patch_json_file(json_file: str | Path | None, updates: dict) -> None:
    """按需补丁更新 JSON 文件中的路径引用。"""
    if not json_file:
        return

    json_path = Path(json_file)
    if not json_path.exists():
        return

    try:
        payload = json.loads(json_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        payload = {}

    if not isinstance(payload, dict):
        payload = {}

    def _normalize_value(value, *, key: str | None = None):
        if value is None:
            return None
        if isinstance(value, dict):
            normalized = {}
            for item_key, item_value in value.items():
                normalized_value = _normalize_value(item_value, key=str(item_key))
                if normalized_value is None:
                    continue
                normalized[str(item_key)] = normalized_value
            return normalized
        if isinstance(value, list):
            normalized_items = []
            for item in value:
                normalized_item = _normalize_value(item, key=key)
                if normalized_item is None:
                    continue
                normalized_items.append(normalized_item)
            return normalized_items
        normalized_key = str(key or '').strip().lower()
        if normalized_key.endswith(('_file', '_csv', '_path')):
            return serialize_output_path_reference(value, output_dir=json_path.parent)
        return value

    normalized_updates = {}
    for key, value in updates.items():
        normalized_value = _normalize_value(value, key=key)
        if normalized_value is None:
            continue
        normalized_updates[key] = normalized_value

    payload.update(normalized_updates)
    atomic_write_json_file(json_path, payload)


def _load_json_file(json_file: str | Path | None) -> dict:
    """读取 JSON 文件并确保返回 dict。"""
    if not json_file:
        return {}

    json_path = Path(json_file)
    if not json_path.exists():
        return {}

    try:
        payload = json.loads(json_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}

    return payload if isinstance(payload, dict) else {}


def rename_prefixed_final_outputs(
    result: dict,
    *,
    output_dir: Path,
    prefix: str,
    timestamp: str | None = None,
    refresh_manifest: bool = True,
) -> dict:
    """
    为股票型/混合型最终筛选结果补充前缀，并同步修正 sidecar 引用。

    约束：
    1. 仅重命名主报告与最终优质基金 Excel
    2. 单边观察池 / 简版 CSV 保持原名，避免额外引入命名分叉
    3. 所有 sidecar 中的跨文件引用必须同步到新路径
    """
    normalized_prefix = str(prefix).strip()
    if not normalized_prefix:
        return result

    output_dir = Path(output_dir)
    output_timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    result.pop('md_file', None)

    old_excel = Path(result['output_file'])
    new_excel = output_dir / f"{normalized_prefix}_近期与长期优质基金对比报告_{output_timestamp}.xlsx"
    if old_excel.exists():
        new_excel, new_meta = rename_report_with_sidecar(
            old_excel,
            new_excel,
            refresh_manifest=False,
        )
        result['output_file'] = str(new_excel)
        result['comparison_report_meta_file'] = str(new_meta) if new_meta else None

    old_final = Path(result['final_fund_file'])
    new_final = output_dir / f"{normalized_prefix}_当日最终优质基金_{output_timestamp}.xlsx"
    if old_final.exists():
        new_final, new_meta = rename_report_with_sidecar(
            old_final,
            new_final,
            refresh_manifest=False,
        )
        result['final_fund_file'] = str(new_final)
        result['final_fund_report_meta_file'] = str(new_meta) if new_meta else None

    bond_short_term_one_year_observation_artifacts = result.get(
        'bond_short_term_one_year_observation_artifacts'
    )
    if bond_short_term_one_year_observation_artifacts is None:
        comparison_meta_payload = _load_json_file(result.get('comparison_report_meta_file'))
        final_meta_payload = _load_json_file(result.get('final_fund_report_meta_file'))
        bond_short_term_one_year_observation_artifacts = (
            comparison_meta_payload.get('bond_short_term_one_year_observation_artifacts')
            or final_meta_payload.get('bond_short_term_one_year_observation_artifacts')
        )
        if bond_short_term_one_year_observation_artifacts is not None:
            result['bond_short_term_one_year_observation_artifacts'] = (
                bond_short_term_one_year_observation_artifacts
            )

    _patch_json_file(
        result.get('comparison_report_meta_file'),
        {
            'only_short_simple_csv': result.get('only_short_simple_csv'),
            'only_long_simple_csv': result.get('only_long_simple_csv'),
            'only_short_watchlist_csv': result.get('only_short_watchlist_csv'),
            'only_long_watchlist_csv': result.get('only_long_watchlist_csv'),
            'bond_short_term_one_year_observation_artifacts': result.get(
                'bond_short_term_one_year_observation_artifacts'
            ),
        },
    )
    _patch_json_file(
        result.get('final_fund_report_meta_file'),
        {
            'comparison_report_file': result.get('output_file'),
            'only_short_simple_csv': result.get('only_short_simple_csv'),
            'only_long_simple_csv': result.get('only_long_simple_csv'),
            'only_short_watchlist_csv': result.get('only_short_watchlist_csv'),
            'only_long_watchlist_csv': result.get('only_long_watchlist_csv'),
            'bond_short_term_one_year_observation_artifacts': result.get(
                'bond_short_term_one_year_observation_artifacts'
            ),
        },
    )
    if refresh_manifest:
        rebuild_report_manifest(output_dir)
    return result
