from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

import pandas as pd
import pytest
from openpyxl import load_workbook

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.project_formal_groups import FORMAL_GROUP_LABEL
from backend.project_tracker import (
    PROJECT_TRACKER_COLUMNS,
    PROJECT_TRACKER_EXPORT_COLUMNS,
    PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
    PROJECT_TRACKER_STATE_PREVIEW_COLUMNS,
    PROJECT_TRACKER_TOP_DECILE_RULES,
    _apply_project_tracker_workbook_presentation,
    _build_project_final_tracker,
    _build_project_tracker_export_view,
    _load_tracker_source_frame,
    _read_project_tracker_export_sheet,
    _update_project_tracker_workbook_for_duplicate_snapshot,
)
from backend.metric_units import format_metric_display_text
from backend.report_builder import ACCUMULATION_SUMMARY_COLUMNS, EXCLUDED_STATE_PREVIEW_COLUMNS
from backend.report_workbook import _materialize_multisheet_workbook, _write_multisheet_workbook


def _summary_row_value(ws, label: str):
    for col_idx in range(1, ws.max_column + 1, 2):
        if ws.cell(row=1, column=col_idx).value == label:
            return ws.cell(row=1, column=col_idx + 1).value
    return None


def test_metric_display_units_convert_decimal_ratio_without_mutating_raw_semantics() -> None:
    assert format_metric_display_text("1年-最大回撤(%)", 0.0123, suffix=True) == "1.23%"
    assert format_metric_display_text("1年-波动率", "0.00152", suffix=True) == "0.15%"
    assert format_metric_display_text("1年-年化收益(%)", 3.45, suffix=True) == "3.45"
    assert format_metric_display_text("1年-最大回撤(%)", "", suffix=True) == ""


def test_workbook_formats_decimal_ratio_risk_fields_as_percent_but_keeps_raw_values(tmp_path: Path) -> None:
    workbook_path = tmp_path / "risk_units.xlsx"
    _write_multisheet_workbook(
        workbook_path,
        main_sheet_name="Sheet1",
        main_df=pd.DataFrame(
            [
                {
                    "基金代码": "000001",
                    "1年-最大回撤(%)": 0.0123,
                    "1年-波动率": 0.00152,
                    "1年-VaR95(%)": -0.0008,
                    "1年-年化收益(%)": 3.45,
                }
            ]
        ),
    )

    wb = load_workbook(workbook_path, data_only=True)
    try:
        ws = wb["Sheet1"]
        assert ws["B2"].value == 0.0123
        assert ws["B2"].number_format == "0.00%"
        assert ws["C2"].value == 0.00152
        assert ws["C2"].number_format == "0.00%"
        assert ws["D2"].value == -0.0008
        assert ws["D2"].number_format == "0.00%"
        assert ws["E2"].value == "3.45"
    finally:
        wb.close()


@pytest.mark.parametrize("provenance_in_main", [False, True])
def test_project_tracker_prefers_explicit_formal_group_from_report_detail(tmp_path: Path, provenance_in_main: bool) -> None:
    filtered_excel = tmp_path / "正式推荐债券基金_20260410_000000.xlsx"
    sheet1_df = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金名称": "测试纯债A",
                "基金公司": "测试基金",
                "基金规模(亿)": "10.0",
                "1年-年化收益(%)": "4.5",
                "1年-夏普比率": "2.1",
                "1年-最大回撤(%)": "0.8",
                "正式投资评分": "88.0",
            },
            {
                "基金代码": "000002",
                "基金名称": "测试增强A",
                "基金公司": "测试基金",
                "基金规模(亿)": "12.0",
                "1年-年化收益(%)": "6.5",
                "1年-夏普比率": "1.8",
                "1年-最大回撤(%)": "2.8",
                "正式投资评分": "82.0",
            },
        ]
    )
    detail_df = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金名称": "测试纯债A",
                "正式榜分组": "纯债稳健榜",
                "债基风格标签": "综合纯债",
                "项目推荐结论": "正式推荐",
                "推荐理由": "测试",
                "推荐备注": "测试",
                "区间收益基准日": "2026-04-09",
                "区间收益获取时间": "2026-04-10T00:00:00",
                "区间收益来源": "fundMNRank",
                "区间收益来源SHA256": "a" * 64,
            },
            {
                "基金代码": "000002",
                "基金名称": "测试增强A",
                "正式榜分组": "增强债弹性榜",
                "债基风格标签": "中短债",
                "项目推荐结论": "正式推荐",
                "推荐理由": "测试",
                "推荐备注": "测试",
            },
        ]
    )
    if provenance_in_main:
        for column in ("区间收益基准日", "区间收益获取时间", "区间收益来源", "区间收益来源SHA256"):
            sheet1_df[column] = detail_df[column]
    with pd.ExcelWriter(filtered_excel) as writer:
        sheet1_df.to_excel(writer, sheet_name="Sheet1", index=False)
        detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    settings = SimpleNamespace(
        project_root=tmp_path,
        runs_dir=tmp_path / "runs",
    )
    settings.runs_dir.mkdir(parents=True, exist_ok=True)

    tracker_df, process_df, summary = _build_project_final_tracker(
        settings,
        current_run_id="20260410_000000_sync",
        current_generated_at="2026-04-10T00:00:00",
        current_filtered_excel=filtered_excel,
        current_final_count=2,
        current_nav_snapshot_date="2026-04-10",
    )

    assert len(process_df.index) == 1
    assert summary["current_active_count"] == 2
    tracker_groups = dict(zip(tracker_df["基金代码"], tracker_df["正式榜分组"]))
    assert tracker_groups["000001"] == "纯债稳健榜"
    assert tracker_groups["000002"] == "增强债弹性榜"
    source_fields = ["区间收益基准日", "区间收益获取时间", "区间收益来源", "区间收益来源SHA256"]
    export_df = _build_project_tracker_export_view(tracker_df)
    for frame in (tracker_df, export_df):
        dated = frame.set_index("基金代码").loc["000001"]
        unknown = frame.set_index("基金代码").loc["000002"]
        for field in source_fields:
            assert dated[field] == detail_df.iloc[0][field]
            assert pd.isna(unknown[field]) or unknown[field] == ""


def test_load_tracker_source_frame_coalesces_holding_period_status_from_merge(tmp_path: Path) -> None:
    filtered_excel = tmp_path / "正式推荐债券基金_20260410_000001.xlsx"
    sheet1_df = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金名称": "测试债基A",
                "基金公司": "测试基金",
                "持有期状态": "已确认<=90天",
            }
        ]
    )
    detail_df = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金名称": "测试债基A",
                "持有期状态": "名称未识别持有期限制",
                "债基风格标签": "综合纯债",
            }
        ]
    )
    with pd.ExcelWriter(filtered_excel) as writer:
        sheet1_df.to_excel(writer, sheet_name="Sheet1", index=False)
        detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    merged_df, _, _ = _load_tracker_source_frame(filtered_excel)

    assert "持有期状态" in merged_df.columns
    assert "持有期状态_x" not in merged_df.columns
    assert "持有期状态_y" not in merged_df.columns
    assert merged_df.loc[0, "持有期状态"] == "已确认<=90天"


def test_project_tracker_reuses_existing_tracker_for_duplicate_snapshot(tmp_path: Path) -> None:
    settings = SimpleNamespace(
        project_root=tmp_path,
        reports_dir=tmp_path,
        runs_dir=tmp_path / "runs",
    )
    settings.runs_dir.mkdir(parents=True, exist_ok=True)

    tracker_internal_df = pd.DataFrame([{column: "" for column in PROJECT_TRACKER_COLUMNS}])
    tracker_internal_df.loc[0, "基金代码"] = "000001"
    tracker_internal_df.loc[0, "基金名称"] = "测试基金A"
    tracker_internal_df.loc[0, FORMAL_GROUP_LABEL] = "纯债稳健榜"
    tracker_internal_df.loc[0, "正式投资评分"] = "90.5"
    tracker_internal_df.loc[0, "出现次数"] = "2"
    tracker_internal_df.loc[0, "出现频率(%)"] = "100.0"
    tracker_internal_df.loc[0, "首次出现日期"] = "2026-04-10"
    tracker_internal_df.loc[0, "最后出现日期"] = "2026-04-13"
    tracker_internal_df.loc[0, "连续缺席期数"] = "0"
    tracker_internal_df.loc[0, "当前是否在榜"] = "是"
    tracker_export_df = _build_project_tracker_export_view(tracker_internal_df)

    process_df = pd.DataFrame(
        [
            {
                "业务日期": "2026-04-13",
                "净值快照日期": "2026-04-13",
                "快照签名": "dup-signature",
                "项目批次": "20260413_000000_sync",
                "生成时间": "2026-04-13T12:00:00",
                "正式推荐数量": "1",
                "来源文件": "reports/runs/20260413_000000_sync/正式推荐债券基金.xlsx",
            }
        ]
    )
    tracker_workbook = tmp_path / "最终优质基金累积跟踪.xlsx"
    _write_multisheet_workbook(
        tracker_workbook,
        main_sheet_name="完整数据",
        main_df=tracker_export_df,
        extra_sheets=[
            {"title": "当前在榜", "df": tracker_export_df},
            {"title": "纯债稳健榜", "df": tracker_export_df},
            {"title": "增强债弹性榜", "df": pd.DataFrame(columns=tracker_export_df.columns)},
            {"title": "处理记录", "df": process_df},
        ],
        left_align_column="基金名称",
        column_widths=PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
        top_decile_rules=PROJECT_TRACKER_TOP_DECILE_RULES,
    )
    _apply_project_tracker_workbook_presentation(
        tracker_workbook,
        summary={
            "tracked_fund_count": 1,
            "current_active_count": 1,
            "repeated_fund_count": 1,
            "total_periods": 1,
            "processed_run_count": 1,
            "unique_snapshot_count": 1,
            "latest_business_date": "2026-04-13",
            "latest_nav_snapshot_date": "2026-04-13",
        },
    )

    current_excel = tmp_path / "正式推荐债券基金_20260414_000000.xlsx"
    current_excel.write_text("", encoding="utf-8")
    from backend.project_tracker import TRACKER_HISTORY_INPUT_CONTRACT, _tracker_history_input_signature
    (tmp_path / "最终优质基金累积跟踪_report_meta.json").write_text(json.dumps({
        "accumulation_summary": {
            "history_input_contract": TRACKER_HISTORY_INPUT_CONTRACT,
            "history_input_signature": _tracker_history_input_signature(settings, [], "20260414_000000_sync"),
        },
    }), encoding="utf-8")

    tracker_df, reused_process_df, summary = _build_project_final_tracker(
        settings,
        current_run_id="20260414_000000_sync",
        current_generated_at="2026-04-14T12:00:00",
        current_filtered_excel=current_excel,
        current_final_count=1,
        current_nav_snapshot_date="2026-04-13",
        load_tracker_source_frame=lambda _: (pd.DataFrame([{"基金代码": "000001"}]), "dup-signature", "2026-04-13"),
    )

    assert len(tracker_df.index) == 1
    assert len(reused_process_df.index) == 2
    assert summary["latest_run_is_new_snapshot"] is False
    assert summary["latest_run_duplicate_of_run_id"] == "20260413_000000_sync"


def test_project_tracker_workbook_presentation_is_idempotent(tmp_path: Path) -> None:
    workbook_path = tmp_path / "tracker.xlsx"
    tracker_df = pd.DataFrame(
        [
            {
                "序号": 1,
                "基金代码": "000001",
                "基金名称": "测试基金A",
                "正式榜分组": "纯债稳健榜",
                "正式投资评分": 90.5,
                "出现次数": 3,
                "出现频率(%)": 100.0,
                "基金规模(亿)": 12.3,
                "近1月(%)": 0.52,
                "近3月(%)": 1.21,
                "1年-年化收益(%)": 3.88,
                "1年-夏普比率": 2.35,
                "1年-最大回撤(%)": 0.42,
                "1年-波动率": 0.88,
                "最新净值日期": "2026-04-10",
            },
            {
                "序号": 2,
                "基金代码": "000002",
                "基金名称": "测试基金B",
                "正式榜分组": "增强债弹性榜",
                "正式投资评分": 84.2,
                "出现次数": 2,
                "出现频率(%)": 66.67,
                "基金规模(亿)": 8.6,
                "近1月(%)": -0.18,
                "近3月(%)": 0.67,
                "1年-年化收益(%)": 5.12,
                "1年-夏普比率": 1.82,
                "1年-最大回撤(%)": 1.65,
                "1年-波动率": 2.31,
                "最新净值日期": "2026-04-10",
            },
        ]
    )
    process_df = pd.DataFrame(
        [
            {
                "业务日期": "2026-04-10",
                "净值快照日期": "2026-04-10",
                "快照签名": "abc123",
                "项目批次": "20260410_000000_sync",
                "生成时间": "2026-04-10T12:00:00",
                "正式推荐数量": 2,
                "来源文件": "reports/runs/20260410_000000_sync/正式推荐债券基金.xlsx",
            }
        ]
    )
    summary = {
        "tracked_fund_count": 2,
        "current_active_count": 2,
        "repeated_fund_count": 1,
        "total_periods": 3,
        "processed_run_count": 1,
        "unique_snapshot_count": 1,
        "latest_business_date": "2026-04-10",
        "latest_nav_snapshot_date": "2026-04-10",
    }

    _write_multisheet_workbook(
        workbook_path,
        main_sheet_name="完整数据",
        main_df=tracker_df,
        extra_sheets=[
            {"title": "当前在榜", "df": tracker_df},
            {"title": "纯债稳健榜", "df": tracker_df.iloc[[0]].reset_index(drop=True)},
            {"title": "增强债弹性榜", "df": tracker_df.iloc[[1]].reset_index(drop=True)},
            {"title": "处理记录", "df": process_df},
        ],
        left_align_column="基金名称",
        top_decile_rules=PROJECT_TRACKER_TOP_DECILE_RULES,
    )

    _apply_project_tracker_workbook_presentation(workbook_path, summary=summary)
    _apply_project_tracker_workbook_presentation(workbook_path, summary=summary)

    wb = load_workbook(workbook_path)
    try:
        expected_first_code = {
            "完整数据": "000001",
            "当前在榜": "000001",
            "纯债稳健榜": "000001",
            "增强债弹性榜": "000002",
        }
        for sheet_name in ["完整数据", "当前在榜", "纯债稳健榜", "增强债弹性榜"]:
            ws = wb[sheet_name]
            assert ws["A1"].value == "总跟踪基金数"
            assert _summary_row_value(ws, "已处理批次数") == 1
            assert ws["A2"].value.startswith("跟踪信息")
            if sheet_name != "完整数据":
                expected_scope = "当期全部正式在榜" if sheet_name == "当前在榜" else f"当期{sheet_name}"
                assert expected_scope in ws["A2"].value
            assert ws["A3"].value == "序号"
            assert ws["B3"].value == "基金代码"
            assert ws["C3"].value == "基金名称"
            assert ws["B4"].value == expected_first_code[sheet_name]
            assert ws.freeze_panes == "D4"
            assert ws.auto_filter.ref is not None and ws.auto_filter.ref.startswith("A3:")
            assert ws.auto_filter.sortState is not None
            assert [(item.ref, item.descending) for item in ws.auto_filter.sortState.sortCondition] == [
                ("E4:E5" if sheet_name in {"完整数据", "当前在榜"} else "E4:E4", True),
                ("G4:G5" if sheet_name in {"完整数据", "当前在榜"} else "G4:G4", True),
            ]
            assert len(ws.conditional_formatting) == 10
            merged_ranges = {str(item) for item in ws.merged_cells.ranges}
            assert "A2:G2" in merged_ranges
            assert all(not str(item).startswith("A1:") for item in ws.merged_cells.ranges)

        process_ws = wb["处理记录"]
        assert process_ws.freeze_panes == "A2"
        assert process_ws.auto_filter.ref == process_ws.dimensions
    finally:
        wb.close()


def test_project_tracker_duplicate_snapshot_update_only_rewrites_process_sheet(tmp_path: Path) -> None:
    workbook_path = tmp_path / "tracker_duplicate_update.xlsx"
    tracker_df = pd.DataFrame(
        [
            {
                "序号": 1,
                "基金代码": "000001",
                "基金名称": "测试基金A",
                "正式榜分组": "纯债稳健榜",
                "正式投资评分": 90.5,
                "出现次数": 3,
                "出现频率(%)": 100.0,
                "基金规模(亿)": 12.3,
            }
        ]
    )
    process_df = pd.DataFrame(
        [
            {
                "业务日期": "2026-04-13",
                "净值快照日期": "2026-04-13",
                "快照签名": "dup-signature",
                "项目批次": "20260413_000000_sync",
                "生成时间": "2026-04-13T12:00:00",
                "正式推荐数量": "1",
                "来源文件": "reports/runs/20260413_000000_sync/正式推荐债券基金.xlsx",
            }
        ]
    )
    _write_multisheet_workbook(
        workbook_path,
        main_sheet_name="完整数据",
        main_df=tracker_df,
        extra_sheets=[
            {"title": "当前在榜", "df": tracker_df},
            {"title": "纯债稳健榜", "df": tracker_df},
            {"title": "增强债弹性榜", "df": pd.DataFrame(columns=tracker_df.columns)},
            {"title": "处理记录", "df": process_df},
        ],
        left_align_column="基金名称",
        top_decile_rules=PROJECT_TRACKER_TOP_DECILE_RULES,
    )
    _apply_project_tracker_workbook_presentation(
        workbook_path,
        summary={
            "tracked_fund_count": 1,
            "current_active_count": 1,
            "repeated_fund_count": 1,
            "total_periods": 1,
            "processed_run_count": 1,
            "unique_snapshot_count": 1,
            "latest_business_date": "2026-04-13",
            "latest_nav_snapshot_date": "2026-04-13",
        },
    )

    updated_process_df = pd.concat(
        [
            process_df,
            pd.DataFrame(
                [
                    {
                        "业务日期": "2026-04-14",
                        "净值快照日期": "2026-04-13",
                        "快照签名": "dup-signature",
                        "项目批次": "20260414_000000_sync",
                        "生成时间": "2026-04-14T12:00:00",
                        "正式推荐数量": "1",
                        "来源文件": "reports/runs/20260414_000000_sync/正式推荐债券基金.xlsx",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    updated = _update_project_tracker_workbook_for_duplicate_snapshot(
        workbook_path,
        process_df=updated_process_df,
        summary={
            "tracked_fund_count": 1,
            "current_active_count": 1,
            "repeated_fund_count": 1,
            "total_periods": 1,
            "processed_run_count": 2,
            "unique_snapshot_count": 1,
            "latest_business_date": "2026-04-14",
            "latest_nav_snapshot_date": "2026-04-13",
        },
    )

    assert updated is True
    wb = load_workbook(workbook_path)
    try:
        current_ws = wb["当前在榜"]
        assert current_ws["A1"].value == "总跟踪基金数"
        assert _summary_row_value(current_ws, "已处理批次数") == 2
        assert _summary_row_value(current_ws, "唯一快照数") == 1
        assert _summary_row_value(current_ws, "最新业务日期") == "2026-04-14"
        assert current_ws["A3"].value == "序号"
        assert current_ws["B4"].value == "000001"
        assert current_ws.freeze_panes == "D4"

        process_ws = wb["处理记录"]
        assert process_ws.max_row == 3
        assert process_ws["D3"].value == "20260414_000000_sync"
        assert process_ws.freeze_panes == "A2"
        assert process_ws.auto_filter.ref == process_ws.dimensions
    finally:
        wb.close()


def _write_tracker_scope_fixture(path: Path, *, history: str = "enhanced") -> None:
    stable = {column: "" for column in PROJECT_TRACKER_EXPORT_COLUMNS}
    stable.update({"基金代码": "000001", "基金名称": "当期纯债", "正式榜分组": "纯债稳健榜",
                   "正式投资评分": 90, "当前是否在榜": "是"})
    enhanced = {**stable, "基金代码": "000002", "基金名称": "历史增强",
                "正式榜分组": "增强债弹性榜", "当前是否在榜": "否"}
    current = pd.DataFrame([stable], columns=PROJECT_TRACKER_EXPORT_COLUMNS)
    complete = pd.DataFrame([stable, enhanced], columns=PROJECT_TRACKER_EXPORT_COLUMNS)
    if history == "missing_group":
        complete = complete.drop(columns="正式榜分组")
    elif history == "unknown_group":
        complete.loc[1, "正式榜分组"] = ""
    elif history == "zero_enhanced":
        complete = current.copy()
    elif history == "empty":
        complete = complete.iloc[:0]
    sheets = [
        {"title": "当前在榜", "df": current},
        {"title": "纯债稳健榜", "df": current},
        {"title": "增强债弹性榜", "df": current.iloc[:0]},
    ]
    if history == "missing_sheet":
        _write_multisheet_workbook(path, main_sheet_name="当前在榜", main_df=current,
                                  extra_sheets=sheets[1:])
    else:
        _write_multisheet_workbook(path, main_sheet_name="完整数据", main_df=complete,
                                  extra_sheets=sheets)


def test_tracker_scope_notes_explain_empty_current_board_without_adding_business_rows(tmp_path: Path) -> None:
    path = tmp_path / "scope.xlsx"
    _write_tracker_scope_fixture(path)
    # Scope counts must come from records, rather than borrowing a global summary.
    summary = {"tracked_fund_count": 999, "current_active_count": 888}
    _apply_project_tracker_workbook_presentation(path, summary=summary)
    _apply_project_tracker_workbook_presentation(path, summary=summary)

    workbook = load_workbook(path)
    try:
        for title, count in [("完整数据", 2), ("当前在榜", 1), ("纯债稳健榜", 1), ("增强债弹性榜", 0)]:
            ws = workbook[title]
            assert list(next(ws.iter_rows(min_row=3, max_row=3, values_only=True))) == PROJECT_TRACKER_EXPORT_COLUMNS
            assert ws.max_column == len(PROJECT_TRACKER_EXPORT_COLUMNS)
            assert ws.max_row == count + 3
            assert ws.freeze_panes == "D4"
            assert ws.auto_filter.ref == f"A3:CC{count + 3}"
            assert _summary_row_value(ws, "总跟踪基金数") == 999
            assert _summary_row_value(ws, "当前在榜数") == 888
            assert f"本表实际记录数：{count}" in ws["Y1"].value
            assert ws["A1"].comment.text == ws["Y1"].value
            assert "Y1:CC1" in {str(item) for item in ws.merged_cells.ranges}
        assert "历史累计（含已离榜）" in workbook["完整数据"]["Y1"].value
        assert workbook["完整数据"]["A2"].value == "跟踪信息"
        assert "当期全部正式在榜" in workbook["当前在榜"]["Y1"].value
        assert "当期全部正式在榜 1只（历史2只" in workbook["当前在榜"]["A2"].value
        assert "当期纯债稳健榜 1只（历史1只" in workbook["纯债稳健榜"]["A2"].value
        assert workbook["增强债弹性榜"]["A2"].value == "跟踪信息｜当期增强债弹性榜 0只（历史1只，详见完整数据）"
        note = workbook["增强债弹性榜"]["Y1"].value
        assert "当期增强债弹性榜正式在榜" in note
        assert "本组历史记录数：1" in note
        assert "本期暂无符合全部发布条件的基金" in note
        assert "历史曾入榜不代表本期推荐，可在完整数据按正式榜分组查询" in note
    finally:
        workbook.close()
    assert _read_project_tracker_export_sheet(path, "增强债弹性榜").empty
    assert _read_project_tracker_export_sheet(path, "完整数据")["基金代码"].tolist() == ["000001", "000002"]
    assert _read_project_tracker_export_sheet(path, "当前在榜")["基金代码"].tolist() == ["000001"]


@pytest.mark.parametrize("history, expected", [
    ("missing_sheet", "未知"), ("missing_group", "未知"), ("unknown_group", "未知"),
    ("zero_enhanced", "0"), ("empty", "0"),
])
def test_tracker_scope_note_distinguishes_unknown_history_from_real_zero(tmp_path: Path, history: str, expected: str) -> None:
    path = tmp_path / f"history_{history}.xlsx"
    _write_tracker_scope_fixture(path, history=history)
    _apply_project_tracker_workbook_presentation(path, summary={"tracked_fund_count": 999})
    workbook = load_workbook(path)
    try:
        ws = workbook["增强债弹性榜"]
        assert f"本组历史记录数：{expected}。" in ws["Y1"].value
        expected_history = "历史记录数未知" if expected == "未知" else f"历史{expected}只"
        assert f"当期增强债弹性榜 0只（{expected_history}，详见完整数据）" in ws["A2"].value
        assert "本表实际记录数：0" in ws["Y1"].value
        assert ws.max_row == 3
    finally:
        workbook.close()


def test_tracker_duplicate_refresh_updates_scope_note_and_keeps_empty_board(tmp_path: Path) -> None:
    path = tmp_path / "duplicate_scope.xlsx"
    _write_tracker_scope_fixture(path)
    _apply_project_tracker_workbook_presentation(path, summary={"processed_run_count": 1})
    workbook = load_workbook(path)
    workbook["增强债弹性榜"]["Y1"] = "旧说明"
    workbook.save(path)
    workbook.close()
    process = pd.DataFrame([{"项目批次": "duplicate", "业务日期": "2026-10-17"}])
    for batch_count in [2, 3]:
        assert _update_project_tracker_workbook_for_duplicate_snapshot(
            path, process_df=process, summary={"processed_run_count": batch_count, "latest_business_date": "2026-10-17"},
        )
    workbook = load_workbook(path)
    try:
        ws = workbook["增强债弹性榜"]
        assert _summary_row_value(ws, "已处理批次数") == 3
        assert _summary_row_value(ws, "最新业务日期") == "2026-10-17"
        assert "本表实际记录数：0；本组历史记录数：1。" in ws["Y1"].value
        assert "本期暂无符合全部发布条件的基金" in ws["Y1"].value
        assert ws["A2"].value == "跟踪信息｜当期增强债弹性榜 0只（历史1只，详见完整数据）"
        assert ws["A1"].comment.text == ws["Y1"].value
        assert ws.max_row == 3
        assert ws.max_column == len(PROJECT_TRACKER_EXPORT_COLUMNS)
        assert ws.auto_filter.ref == "A3:CC3"
        assert workbook["当前在榜"]["B4"].value == "000001"
        assert workbook["完整数据"]["B5"].value == "000002"
    finally:
        workbook.close()
    assert _read_project_tracker_export_sheet(path, "增强债弹性榜").empty


def test_materialize_multisheet_workbook_reuses_previous_same_snapshot_artifact(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    previous_run_dir = reports_dir / "runs" / "prev_run"
    previous_run_dir.mkdir(parents=True, exist_ok=True)
    previous_excel = previous_run_dir / "正式推荐债券基金_prev.xlsx"
    previous_csv = previous_run_dir / "正式推荐债券基金_prev.csv"
    previous_meta = previous_run_dir / "正式推荐债券基金_prev_report_meta.json"

    df = pd.DataFrame(
        [
            {"基金代码": "000001", "基金名称": "测试债基A", "正式投资评分": "88.1"},
            {"基金代码": "000002", "基金名称": "测试债基B", "正式投资评分": "85.2"},
        ]
    )
    extra_sheets = [{"title": "发布摘要", "df": pd.DataFrame([{"指标": "正式推荐数量", "数值": 2}]), "left_align_column": "指标"}]
    _write_multisheet_workbook(
        previous_excel,
        main_sheet_name="Sheet1",
        main_df=df,
        extra_sheets=extra_sheets,
        left_align_column="基金名称",
    )
    df.to_csv(previous_csv, index=False, encoding="utf-8-sig")

    reuse_probe = _materialize_multisheet_workbook(
        settings=SimpleNamespace(project_root=tmp_path, reports_dir=reports_dir),
        output_file=tmp_path / "probe.xlsx",
        artifact_key="filtered_excel",
        artifact_meta_key="filtered_excel_meta",
        main_sheet_name="Sheet1",
        main_df=df,
        extra_sheets=extra_sheets,
        left_align_column="基金名称",
        current_snapshot_date="2026-04-13",
    )

    previous_meta.write_text(
        json.dumps(
            {
                "content_signature": reuse_probe["content_signature"],
                "output_file": "reports/runs/prev_run/正式推荐债券基金_prev.xlsx",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (reports_dir / "latest.json").write_text(
        json.dumps(
            {
                "run_id": "prev_run",
                "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-04-13"}},
                "artifacts": {
                    "filtered_excel": "reports/runs/prev_run/正式推荐债券基金_prev.xlsx",
                    "filtered_csv": "reports/runs/prev_run/正式推荐债券基金_prev.csv",
                    "filtered_excel_meta": "reports/runs/prev_run/正式推荐债券基金_prev_report_meta.json",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    output_excel = tmp_path / "current.xlsx"
    output_csv = tmp_path / "current.csv"
    result = _materialize_multisheet_workbook(
        settings=SimpleNamespace(project_root=tmp_path, reports_dir=reports_dir),
        output_file=output_excel,
        artifact_key="filtered_excel",
        artifact_meta_key="filtered_excel_meta",
        main_sheet_name="Sheet1",
        main_df=df,
        extra_sheets=extra_sheets,
        left_align_column="基金名称",
        current_snapshot_date="2026-04-13",
        csv_output_file=output_csv,
        csv_artifact_key="filtered_csv",
        csv_df=df,
    )

    assert result["reused_from"]["run_id"] == "prev_run"
    assert output_excel.exists()
    assert output_csv.exists()
    wb = load_workbook(output_excel)
    try:
        assert wb["Sheet1"]["A2"].value == "000001"
        assert wb["发布摘要"]["A2"].value == "正式推荐数量"
    finally:
        wb.close()


def test_materialize_multisheet_workbook_skips_reuse_when_signature_changes(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    previous_run_dir = reports_dir / "runs" / "prev_run"
    previous_run_dir.mkdir(parents=True, exist_ok=True)
    previous_excel = previous_run_dir / "正式推荐债券基金_prev.xlsx"
    previous_meta = previous_run_dir / "正式推荐债券基金_prev_report_meta.json"

    old_df = pd.DataFrame([{"基金代码": "000001", "基金名称": "测试债基A", "正式投资评分": "88.1"}])
    new_df = pd.DataFrame([{"基金代码": "000001", "基金名称": "测试债基A", "正式投资评分": "91.5"}])
    _write_multisheet_workbook(
        previous_excel,
        main_sheet_name="Sheet1",
        main_df=old_df,
        extra_sheets=[],
        left_align_column="基金名称",
    )
    previous_meta.write_text(
        json.dumps({"content_signature": "old-signature"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (reports_dir / "latest.json").write_text(
        json.dumps(
            {
                "run_id": "prev_run",
                "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-04-13"}},
                "artifacts": {
                    "filtered_excel": "reports/runs/prev_run/正式推荐债券基金_prev.xlsx",
                    "filtered_excel_meta": "reports/runs/prev_run/正式推荐债券基金_prev_report_meta.json",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    output_excel = tmp_path / "current.xlsx"
    result = _materialize_multisheet_workbook(
        settings=SimpleNamespace(project_root=tmp_path, reports_dir=reports_dir),
        output_file=output_excel,
        artifact_key="filtered_excel",
        artifact_meta_key="filtered_excel_meta",
        main_sheet_name="Sheet1",
        main_df=new_df,
        extra_sheets=[],
        left_align_column="基金名称",
        current_snapshot_date="2026-04-13",
    )

    assert result["reused_from"] is None


@pytest.mark.parametrize("previous_source", ["missing_column", "changed_source", "same_source"])
def test_reused_workbook_always_exports_current_csv_provenance(tmp_path, previous_source):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path)
    main = pd.DataFrame([{"基金代码": "000001", "基金名称": "isolated CSV reuse fixture", "1年-波动率": 0.001}])
    current = main.assign(区间收益来源="isolated current source")
    previous = (main if previous_source == "missing_column" else
                main.assign(区间收益来源="isolated old source") if previous_source == "changed_source" else current)
    common = dict(settings=settings, artifact_key="filtered_excel", artifact_meta_key="filtered_excel_meta",
                  main_sheet_name="Sheet1", main_df=main, csv_artifact_key="filtered_csv")
    first = _materialize_multisheet_workbook(**common, output_file=tmp_path / "first.xlsx",
        csv_output_file=tmp_path / "first.csv", csv_df=previous)
    original_excel = (tmp_path / "first.xlsx").read_bytes()
    original_csv = (tmp_path / "first.csv").read_bytes()
    (tmp_path / "meta.json").write_text(json.dumps({"content_signature": first["content_signature"]}))
    (tmp_path / "latest.json").write_text(json.dumps({"run_id": "isolated-previous",
        "artifacts": {"filtered_excel": "first.xlsx", "filtered_csv": "first.csv", "filtered_excel_meta": "meta.json"}}))
    second = _materialize_multisheet_workbook(**common, output_file=tmp_path / "second.xlsx",
        csv_output_file=tmp_path / "csv/second.csv", csv_df=current)
    result = pd.read_csv(tmp_path / "csv/second.csv", dtype=str, keep_default_na=False)
    assert second["reused_from"]["run_id"] == "isolated-previous"
    assert second["reused_from"]["csv_file"] is None
    assert result.loc[0, "区间收益来源"] == "isolated current source"
    assert float(result.loc[0, "1年-波动率"]) == pytest.approx(0.1)
    assert (tmp_path / "second.xlsx").read_bytes() == original_excel
    assert (tmp_path / "first.xlsx").read_bytes() == original_excel
    assert (tmp_path / "first.csv").read_bytes() == original_csv


def test_state_preview_columns_drop_legacy_score_echo_fields() -> None:
    assert ACCUMULATION_SUMMARY_COLUMNS == [
        "基金代码",
        "基金名称",
        "出现次数",
        "出现频率(%)",
        "首次出现日期",
        "最后出现日期",
        "连续缺席天数",
        "最新年化收益(%)",
        "最新夏普比率",
        "最新最大回撤(%)",
    ]
    assert "综合评分" not in ACCUMULATION_SUMMARY_COLUMNS
    assert "评分等级" not in ACCUMULATION_SUMMARY_COLUMNS
    assert "评分模式" not in ACCUMULATION_SUMMARY_COLUMNS

    assert PROJECT_TRACKER_STATE_PREVIEW_COLUMNS == [
        "基金代码",
        "基金名称",
        "正式榜分组",
        "正式投资评分",
        "出现次数",
        "出现频率(%)",
        "首次出现日期",
        "最后出现日期",
        "连续缺席期数",
        "当前是否在榜",
        "基金公司",
        "债基风格标签",
        "最新年化收益(%)",
        "最新夏普比率",
        "最新最大回撤(%)",
        "最新波动率",
        "最新近1周(%)",
        "最新近1月(%)",
        "最新近3月(%)",
        "区间收益基准日",
        "最新净值日期",
        "最新持有期状态",
        "项目质量信心",
    ]
    assert "最新综合加权得分" not in PROJECT_TRACKER_STATE_PREVIEW_COLUMNS
    assert "最新近期得分" not in PROJECT_TRACKER_STATE_PREVIEW_COLUMNS
    assert "最新长期得分" not in PROJECT_TRACKER_STATE_PREVIEW_COLUMNS

    assert EXCLUDED_STATE_PREVIEW_COLUMNS == [
        "基金代码",
        "基金名称",
        "基金类型",
        "基金公司",
        "基金规模(亿)",
        "持有期状态",
        "是否可申购",
        "申购状态",
        "债基风格标签",
        "基准归类",
        "正式投资评分",
        "项目质量信心",
    ]
    assert "综合评分" not in EXCLUDED_STATE_PREVIEW_COLUMNS
    assert "评分等级" not in EXCLUDED_STATE_PREVIEW_COLUMNS
    assert "评分模式" not in EXCLUDED_STATE_PREVIEW_COLUMNS
