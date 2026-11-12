"""Download contracts using isolated fixtures, never production business data."""
from copy import deepcopy
from io import BytesIO
from pathlib import Path
import inspect
import sys
from unittest.mock import Mock

from flask import Flask
from openpyxl import load_workbook
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.favorites_export import MAX_REQUEST_BYTES, build_favorites_export_workbook
from backend.api_routes.favorites import register_favorites_routes


@pytest.fixture
def snapshot():
    keys = ["基金代码", "基金名称", "近1月(%)", "最新单位净值", "正式投资评分"]
    return {"schema_version": 1, "metadata": {"查询名称": "隔离测试", "查询完成时间": "2026-01-01T00:00:00Z"}, "tables": [{
        "name": "隔离测试表", "columns": [{"key": key, "label": key} for key in keys],
        "rows": [
            {"source": "shared", "cells": [{"text": value} for value in ["000001", "=1+1", "+0.00%", "1.0234", "95.20\n总评 2/10"]]},
            {"source": "user", "cells": [{"text": value} for value in ["000002", "孤立测试行", None, "0.0000", None]]},
        ],
    }]}


@pytest.fixture
def export_client():
    app = Flask(__name__)
    forbidden = Mock(side_effect=AssertionError("export must not fetch, persist, schedule or select funds"))
    kwargs = {name: forbidden for name in inspect.signature(register_favorites_routes).parameters if name != "app"}
    register_favorites_routes(app, **kwargs)
    return app.test_client(), forbidden


def test_real_xlsx_keeps_display_precision_codes_nulls_and_styles(snapshot):
    original = deepcopy(snapshot)
    snapshot["tables"][0]["rows"][0]["cells"][2]["highlight"] = True
    buffer, filename, count = build_favorites_export_workbook(snapshot)
    assert buffer.getvalue().startswith(b"PK") and filename.endswith(".xlsx") and count == 2
    book = load_workbook(buffer, data_only=False)
    sheet = book.worksheets[0]
    assert sheet["A5"].value == "000001" and sheet["A5"].data_type == "s"
    assert sheet["B5"].value == "=1+1" and sheet["B5"].data_type == "s"
    assert sheet["C5"].value == 0 and sheet["C5"].number_format == "+0.00%;-0.00%;+0.00%"
    assert sheet["C6"].value is None and sheet["D6"].value == 0
    assert sheet["D5"].value == 1.0234 and sheet["D5"].number_format == "0.0000;-0.0000;0.0000"
    assert sheet["E5"].value == "95.20\n总评 2/10" and sheet["E6"].value is None
    assert sheet["C5"].fill.fgColor.rgb == "00FFF1A8"
    assert sheet["A5"].fill.fgColor.rgb == "00EEF8ED"
    assert sheet.freeze_panes == "C5" and sheet.auto_filter.ref == "A4:E6"
    assert book.sheetnames == ["隔离测试表", "查询信息"]
    assert snapshot["tables"][0]["rows"][1] == original["tables"][0]["rows"][1]


def test_group_tables_keep_order_and_unique_excel_names(snapshot):
    group = deepcopy(snapshot["tables"][0])
    group["name"] = "同名/" + "长" * 40
    snapshot["tables"] = [deepcopy(group), deepcopy(group)]
    snapshot["tables"][1]["rows"].reverse()
    buffer, _, count = build_favorites_export_workbook(snapshot)
    book = load_workbook(buffer)
    assert count == 4
    assert len(set(book.sheetnames)) == 3
    assert all(len(name) <= 31 and "/" not in name for name in book.sheetnames)
    assert book.worksheets[0]["A5"].value == "000001"
    assert book.worksheets[1]["A5"].value == "000002"


@pytest.mark.parametrize("text,value,format_text", [
    ("-1.25%", -0.0125, "0.00%;-0.00%;0.00%"),
    ("-0.00%", 0, "0.00%;-0.00%;-0.00%"),
    ("1,234.50", 1234.5, "#,##0.00;-#,##0.00;#,##0.00"),
    ("1234567890123456.00", "1234567890123456.00", "@"),
])
def test_numeric_conversion_does_not_lose_units_or_excel_precision(snapshot, text, value, format_text):
    snapshot["tables"][0]["rows"][0]["cells"][2]["text"] = text
    buffer, _, _ = build_favorites_export_workbook(snapshot)
    cell = load_workbook(buffer).worksheets[0]["C5"]
    assert cell.value == value and cell.number_format == format_text


def test_download_uses_only_the_submitted_display_snapshot(export_client, snapshot):
    client, forbidden = export_client
    response = client.post("/api/favorites/export", json=snapshot)
    assert response.status_code == 200
    assert response.mimetype == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["X-Export-Row-Count"] == "2"
    assert "filename*=UTF-8''" in response.headers["Content-Disposition"]
    assert load_workbook(BytesIO(response.data)).worksheets[0]["A6"].value == "000002"
    forbidden.assert_not_called()


@pytest.mark.parametrize("payload", [None, [], {}, {"schema_version": True, "tables": []}, {"schema_version": 1, "tables": []}])
def test_empty_or_invalid_snapshot_is_an_error(export_client, payload):
    client, forbidden = export_client
    response = client.post("/api/favorites/export", json=payload)
    assert response.status_code == 400
    assert response.is_json and response.json["error"]
    forbidden.assert_not_called()


@pytest.mark.parametrize("damage", ["short_row", "missing_code", "non_ascii_code", "illegal_text", "duplicate_column"])
def test_malformed_table_does_not_return_partial_xlsx(export_client, snapshot, damage):
    table = snapshot["tables"][0]
    if damage == "short_row": table["rows"][0]["cells"].pop()
    elif damage == "missing_code": table["rows"][0]["cells"][0]["text"] = None
    elif damage == "non_ascii_code": table["rows"][0]["cells"][0]["text"] = "０００００１"
    elif damage == "illegal_text": table["rows"][0]["cells"][1]["text"] = "bad\x00text"
    else: table["columns"][1]["key"] = "基金代码"
    response = export_client[0].post("/api/favorites/export", json=snapshot)
    assert response.status_code == 400 and response.is_json


def test_large_request_is_bounded(export_client):
    response = export_client[0].post("/api/favorites/export", data=b" " * (MAX_REQUEST_BYTES + 1), content_type="application/json")
    assert response.status_code == 413


def test_return_date_comment_does_not_change_excel_values(snapshot):
    snapshot["tables"][0]["rows"][0]["cells"][2]["comment"] = "分析快照收益；收益基准日未记录"
    buffer, _, count = build_favorites_export_workbook(snapshot)
    cell = load_workbook(buffer).worksheets[0]["C5"]
    assert count == 2 and cell.value == 0 and cell.number_format == "+0.00%;-0.00%;+0.00%"
    assert cell.comment.text == "分析快照收益；收益基准日未记录"


@pytest.mark.parametrize("comment", ["x" * 501, "bad\x00comment", 123])
def test_invalid_comment_rejects_whole_download(snapshot, export_client, comment):
    snapshot["tables"][0]["rows"][0]["cells"][2]["comment"] = comment
    assert export_client[0].post("/api/favorites/export", json=snapshot).status_code == 400
