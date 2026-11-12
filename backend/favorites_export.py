"""Create a download from the displayed favorite query, without fetching data."""
from __future__ import annotations

from datetime import datetime
from io import BytesIO
import math
import re
from typing import Any

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_ROWS = 2000
MAX_COLUMNS = 60
MAX_TABLES = 24
NUMERIC_FIELD = re.compile(r"评分|得分|收益|回撤|频率|规模|夏普|波动率|费率|权重|比率|净值|\(%\)")
NUMBER_TEXT = re.compile(r"^[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?%?$")
SOURCE_FILLS = {"user": "FDF7EE", "shared": "EEF8ED", "recommendation": "F0F5FB"}
TEXT_COLORS = {"positive": "C43C3C", "negative": "15805F", "default": "183B4E"}


def _text(value: Any, *, limit: int, required: bool = False) -> str:
    if not isinstance(value, str) or len(value) > limit or ILLEGAL_CHARACTERS_RE.search(value):
        raise ValueError("导出内容格式错误或文字过长")
    if required and not value.strip():
        raise ValueError("导出表格缺少名称或字段")
    return value


def _write_display_value(cell: Any, key: str, text: str | None) -> None:
    if text is None:
        cell.value = None
        return
    # A code is always text. Convert only a single displayed numeric value;
    # compound cells (e.g. return plus volatility) retain their displayed lines.
    if key != "基金代码" and NUMERIC_FIELD.search(key) and NUMBER_TEXT.fullmatch(text):
        number_text = text.replace(",", "").rstrip("%")
        significant_digits = len(number_text.lstrip("+-").replace(".", "").lstrip("0"))
        value = float(number_text)
        if math.isfinite(value) and significant_digits <= 15:
            percent = text.endswith("%")
            cell.value = value / 100 if percent else value
            decimals = len(number_text.partition(".")[2])
            pattern = ("#,##0" if "," in text else "0") + ("." + "0" * decimals if decimals else "")
            if percent:
                pattern += "%"
            positive = ("+" if text.startswith("+") else "") + pattern
            zero = "-" + pattern if text.startswith("-") and value == 0 else positive
            cell.number_format = f"{positive};-{pattern};{zero}"
            return
    cell.value = text
    # Even names/labels starting with '=' must remain literal text, never formulas.
    cell.data_type = "s"
    cell.number_format = "@"


def build_favorites_export_workbook(payload: Any) -> tuple[BytesIO, str, int]:
    if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int or payload["schema_version"] != 1:
        raise ValueError("导出数据格式错误，请刷新页面后重试")
    tables = payload.get("tables")
    if not isinstance(tables, list) or not 1 <= len(tables) <= MAX_TABLES:
        raise ValueError("没有可导出的基金表格")
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict) or len(metadata) > 16:
        raise ValueError("查询信息格式错误")
    for key, value in metadata.items():
        _text(key, limit=60, required=True)
        _text(value, limit=2000)

    # Validate the complete view before creating a partial workbook.
    total_rows = 0
    for table in tables:
        if not isinstance(table, dict):
            raise ValueError("基金表格格式错误")
        _text(table.get("name"), limit=80, required=True)
        columns, rows = table.get("columns"), table.get("rows")
        if not isinstance(columns, list) or not 1 <= len(columns) <= MAX_COLUMNS:
            raise ValueError("导出字段数量不正确")
        if not isinstance(rows, list) or not rows:
            raise ValueError("没有可导出的基金")
        total_rows += len(rows)
        if total_rows > MAX_ROWS:
            raise ValueError(f"单次最多导出{MAX_ROWS}行")
        keys = []
        for column in columns:
            if not isinstance(column, dict):
                raise ValueError("导出字段格式错误")
            keys.append(_text(column.get("key"), limit=80, required=True))
            _text(column.get("label"), limit=80, required=True)
        if len(set(keys)) != len(keys) or "基金代码" not in keys:
            raise ValueError("导出字段重复或缺少基金代码")
        code_index = keys.index("基金代码")
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("cells"), list) or len(row["cells"]) != len(columns):
                raise ValueError("基金行与字段数量不一致")
            for cell in row["cells"]:
                if not isinstance(cell, dict):
                    raise ValueError("单元格格式错误")
                if cell.get("text") is not None:
                    _text(cell["text"], limit=4000)
                if cell.get("comment") is not None:
                    _text(cell["comment"], limit=500)
            code = row["cells"][code_index].get("text")
            if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
                raise ValueError("导出基金代码格式错误")

    workbook = Workbook()
    workbook.remove(workbook.active)
    border = Border(bottom=Side(style="hair", color="D9E5E9"))
    used_titles = {"查询信息"}
    for table in tables:
        title = re.sub(r"[\\/*?:\[\]]", "_", table["name"]).strip("'")[:31] or "基金列表"
        base_title, suffix = title, 1
        while title.casefold() in used_titles:
            suffix += 1
            ending = f"_{suffix}"
            title = base_title[:31 - len(ending)] + ending
        used_titles.add(title.casefold())
        sheet = workbook.create_sheet(title)
        columns, rows = table["columns"], table["rows"]
        last_col = get_column_letter(len(columns))
        sheet.merge_cells(f"A1:{last_col}1")
        sheet["A1"] = table["name"]
        sheet["A1"].data_type = "s"
        sheet["A1"].font = Font(name="Microsoft YaHei", size=16, bold=True, color="183B4E")
        sheet.row_dimensions[1].height = 34
        sheet.merge_cells(f"A2:{last_col}2")
        sheet["A2"] = f"{metadata.get('查询名称', '收藏基金')} · {len(rows)}只 · {metadata.get('查询完成时间', '')}"
        sheet["A2"].data_type = "s"
        sheet["A2"].font = Font(name="Microsoft YaHei", size=10, color="657C87")
        for index, column in enumerate(columns, 1):
            cell = sheet.cell(4, index, column["label"])
            cell.data_type = "s"
            cell.font = Font(name="Microsoft YaHei", size=11, bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="176B75")
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            width = {"基金代码": 12, "基金名称": 34, "实时估值": 26, "出现次数&出现频率(%)": 23}.get(column["key"], 19)
            sheet.column_dimensions[get_column_letter(index)].width = width
        sheet.row_dimensions[4].height = 30
        for row_index, row in enumerate(rows, 5):
            lines = 1
            for column_index, (column, display) in enumerate(zip(columns, row["cells"]), 1):
                cell = sheet.cell(row_index, column_index)
                text = display.get("text")
                _write_display_value(cell, column["key"], text)
                if display.get("comment"):
                    cell.comment = Comment(display["comment"], "Fund-ANA")
                lines = max(lines, (text or "").count("\n") + 1)
                cell.font = Font(name="Microsoft YaHei", size=11, color=TEXT_COLORS.get(display.get("tone"), TEXT_COLORS["default"]))
                fill = "FFF1A8" if display.get("highlight") is True else SOURCE_FILLS.get(row.get("source"), "FFFFFF")
                cell.fill = PatternFill("solid", fgColor=fill)
                cell.border = border
                cell.alignment = Alignment(horizontal="left" if column["key"] == "基金名称" else "center", vertical="center", wrap_text=True)
            sheet.row_dimensions[row_index].height = min(96, max(36, lines * 17))
        sheet.freeze_panes = "C5" if len(columns) >= 2 else "A5"
        sheet.auto_filter.ref = f"A4:{last_col}{4 + len(rows)}"
        sheet.sheet_view.showGridLines = False
        sheet.sheet_properties.pageSetUpPr.fitToPage = True
        sheet.page_setup.orientation = "landscape"
        sheet.page_setup.paperSize = sheet.PAPERSIZE_A3
        sheet.page_setup.fitToWidth = 1
        sheet.page_setup.fitToHeight = 0
        sheet.print_title_rows = "1:4"
    info = workbook.create_sheet("查询信息")
    exported_at = datetime.now().astimezone()
    for key, value in {**metadata, "导出时间": exported_at.isoformat(timespec="seconds"), "导出基金数": str(total_rows)}.items():
        info.append([key, value])
        for cell in info[info.max_row]:
            cell.data_type = "s"
            cell.font = Font(name="Microsoft YaHei", size=11)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        info.row_dimensions[info.max_row].height = 32
    info.column_dimensions["A"].width = 22
    info.column_dimensions["B"].width = 88
    info.freeze_panes = "B1"
    workbook.active = 0
    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    label = re.sub(r"[\\/:*?\"<>|\r\n]", "_", metadata.get("查询名称", "收藏基金")).strip(" .")[:40] or "收藏基金"
    return buffer, f"{label}_{exported_at:%Y%m%d_%H%M%S}.xlsx", total_rows
