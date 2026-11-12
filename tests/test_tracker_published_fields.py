"""Published confidence and return evidence survive Tracker materialization."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.project_report_exports import RECOMMENDATION_DETAIL_COLUMNS, REFERENCE_FINAL_COLUMNS
from backend.project_tracker import (
    PROJECT_TRACKER_COLUMNS,
    PROJECT_TRACKER_EXPORT_COLUMNS,
    PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME,
    _build_project_final_tracker,
    _load_tracker_source_frame,
    _tracker_source_identity,
    _try_reuse_project_final_tracker,
)


SOURCE_FIELDS = ("区间收益基准日", "区间收益获取时间", "区间收益来源", "区间收益来源SHA256")


def _write_formal(path: Path, records: list[dict]) -> None:
    frame = pd.DataFrame(records)
    with pd.ExcelWriter(path) as writer:
        frame.reindex(columns=REFERENCE_FINAL_COLUMNS).to_excel(writer, sheet_name="Sheet1", index=False)
        frame.reindex(columns=RECOMMENDATION_DETAIL_COLUMNS).to_excel(writer, sheet_name="推荐理由与风格", index=False)


def _formal(code: str, confidence: str | None = "中信心") -> dict:
    return {"基金代码": code, "基金名称": "Synthetic bond " + code, "正式榜分组": "纯债稳健榜",
            "基金公司": "Synthetic", "基金规模(亿)": 20.0, "正式投资评分": 90.0,
            "综合加权得分": 90.0, "近期得分": 90.0, "长期得分": 90.0,
            "1年-年化收益(%)": 5.0, "1年-夏普比率": 2.0, "最新净值日期": "2026-04-13",
            "项目质量信心": confidence}


def test_formal_detail_contract_preserves_confidence_and_return_provenance(tmp_path: Path) -> None:
    path = tmp_path / "formal.xlsx"
    known = {**_formal("990001"), **dict(zip(SOURCE_FIELDS, ("2026-04-13", "2026-04-14T07:31:00", "isolated_api", "a" * 64)))}
    unknown = {**_formal("990002", None), **dict.fromkeys(SOURCE_FIELDS)}
    _write_formal(path, [known, unknown])
    frame, _, _ = _load_tracker_source_frame(path)
    rows = frame.set_index("基金代码")
    assert rows.loc["990001", "项目质量信心"] == "中信心"
    assert rows.loc["990002", "项目质量信心"] == ""
    for field in SOURCE_FIELDS:
        assert rows.loc["990001", field] == known[field]
        assert rows.loc["990002", field] == ""
    assert len(PROJECT_TRACKER_EXPORT_COLUMNS) == 81


def test_full_tracker_inherits_current_publication_and_preserves_historical_label(tmp_path: Path) -> None:
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path, runs_dir=tmp_path / "runs")
    settings.runs_dir.mkdir()
    previous = tmp_path / "previous.xlsx"
    current = tmp_path / "current.xlsx"
    _write_formal(previous, [{**_formal("990009", "中信心"), "最新净值日期": "2026-04-10"}])
    _write_formal(current, [_formal("990001", "中信心"), _formal("990002", None)])
    # Preserve an already recorded historical label even when the old formal
    # workbook carried a different contract. Current labels come from current.
    with pd.ExcelWriter(tmp_path / "最终优质基金累积跟踪.xlsx") as writer:
        pd.DataFrame([{"基金代码": "990009", "项目质量信心": "边界观察"}]).to_excel(
            writer, sheet_name="完整数据", index=False, startrow=2)
    history = [{"run_id": "previous", "generated_at": "2026-04-10T08:00:00", "business_date": "2026-04-10",
                "nav_snapshot_date": "2026-04-10", "excel_path": previous, "final_count": 1},
               {"run_id": "current", "generated_at": "2026-04-14T08:00:00", "business_date": "2026-04-14",
                "nav_snapshot_date": "2026-04-13", "excel_path": current, "final_count": 2}]
    tracker, _, _ = _build_project_final_tracker(
        settings, current_run_id="current", current_generated_at="2026-04-14T08:00:00",
        current_filtered_excel=current, current_final_count=2, current_nav_snapshot_date="2026-04-13",
        load_project_report_history=lambda *args, **kwargs: history,
    )
    rows = tracker.set_index("基金代码")
    assert rows.loc["990001", "项目质量信心"] == "中信心"
    assert rows.loc["990002", "项目质量信心"] == ""
    assert rows.loc["990009", "项目质量信心"] == "边界观察"
    assert rows.loc["990009", "当前是否在榜"] == "否"


@pytest.mark.parametrize("current_confidence", ["中信心", None])
def test_duplicate_snapshot_updates_confidence_even_when_other_metrics_do_not_change(tmp_path: Path, current_confidence) -> None:
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path, runs_dir=tmp_path / "runs")
    settings.runs_dir.mkdir()
    source = pd.DataFrame([_formal("990001", current_confidence)])
    signature, nav_date = _tracker_source_identity(source)
    old_internal = {column: "" for column in PROJECT_TRACKER_COLUMNS}
    for field, value in source.iloc[0].to_dict().items():
        target = PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME.get(field, field)
        if target in old_internal:
            old_internal[target] = value
    old_internal.update({"基金代码": "990001", "基金名称": "Synthetic bond 990001", "当前是否在榜": "是",
                         "项目质量信心": "高信心", "出现次数": "100", "出现频率(%)": "100",
                         "首次出现日期": "2026-04-10", "最后出现日期": nav_date})
    process = pd.DataFrame([{"业务日期": "2026-04-13", "净值快照日期": nav_date, "快照签名": signature,
                             "项目批次": "previous", "生成时间": "2026-04-13T08:00:00", "正式推荐数量": "1",
                             "来源文件": "previous.xlsx"}])
    result = _try_reuse_project_final_tracker(
        settings, current_run_id="current", current_generated_at="2026-04-14T08:00:00",
        current_filtered_excel=tmp_path / "current.xlsx", current_final_count=1, current_nav_snapshot_date=nav_date,
        read_project_tracker_export_sheet=lambda *args: pd.DataFrame([old_internal]),
        read_project_tracker_process_sheet=lambda *args: process,
        restore_project_tracker_internal_view=lambda frame: frame.copy(),
        load_tracker_source_frame=lambda *args: (source, signature, nav_date),
    )
    assert result is not None
    tracker, _, summary = result
    assert tracker.loc[0, "项目质量信心"] == (current_confidence or "")
    assert tracker.loc[0, "出现次数"] == "100"
    assert summary["current_metrics_refreshed"] is True


def test_loader_keeps_explicit_missing_confidence_in_main_sheet(tmp_path: Path) -> None:
    path = tmp_path / "formal.xlsx"
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame([{"基金代码": "990001", "项目质量信心": None}]).to_excel(writer, sheet_name="Sheet1", index=False)
        pd.DataFrame([{"基金代码": "990001", "项目质量信心": "高信心"}]).to_excel(writer, sheet_name="推荐理由与风格", index=False)
    frame, _, _ = _load_tracker_source_frame(path)
    assert frame.loc[0, "项目质量信心"] == ""
    assert "项目质量信心_x" not in frame and "项目质量信心_y" not in frame
