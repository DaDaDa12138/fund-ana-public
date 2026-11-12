"""Exercise the Excel-to-tracker validation projection, including missing states."""
from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend import project_tracker
from backend.fund_cross_validation import CONTRACT, PROVIDER
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS, validation_status_value


VALIDATION = {
    "数据校验原始状态": "strict_match",
    "数据校验状态": "通过",
    "数据校验备注": "",
    "数据校验告警原因": "",
    "数据校验来源": PROVIDER,
    "数据校验契约版本": CONTRACT,
}


def write_source(path: Path, main_validation: dict | None, detail_validation: dict | None) -> None:
    # No fabricated financial observations are needed to test column transport.
    main = {"基金代码": "900001", "基金名称": "isolated column fixture", "最新单位净值": None}
    detail = {"基金代码": "900001", "推荐理由": "isolated column fixture"}
    if main_validation is not None:
        main.update(main_validation)
    if detail_validation is not None:
        detail.update(detail_validation)
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame([main]).to_excel(writer, sheet_name="Sheet1", index=False)
        pd.DataFrame([detail]).to_excel(writer, sheet_name="推荐理由与风格", index=False)


def build_export(tmp_path: Path, path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "runs")
    settings.runs_dir.mkdir(exist_ok=True)
    tracker, _, summary = project_tracker._build_project_final_tracker(
        settings,
        current_run_id="isolated_projection_run",
        current_generated_at="2026-10-13T11:00:00",
        current_filtered_excel=path,
        current_final_count=1,
    )
    assert summary["current_active_count"] == 1
    return tracker, project_tracker._build_project_tracker_export_view(tracker)


@pytest.mark.parametrize("placement", ["main", "detail", "both"])
def test_validation_snapshot_survives_excel_tracker_export_and_restore(tmp_path: Path, placement: str) -> None:
    path = tmp_path / "source.xlsx"
    write_source(path, VALIDATION if placement != "detail" else None,
                 VALIDATION if placement != "main" else None)
    source, _, _ = project_tracker._load_tracker_source_frame(path)
    tracker, exported = build_export(tmp_path, path)
    restored = project_tracker._restore_project_tracker_internal_view(exported)

    for column, expected in VALIDATION.items():
        assert source.iloc[0][column] == expected
        assert f"{column}_x" not in source and f"{column}_y" not in source
        assert exported.iloc[0][column] == expected
        assert tracker.iloc[0][f"最新{column}"] == expected
        assert restored.iloc[0][f"最新{column}"] == expected
    assert source.iloc[0]["最新单位净值"] == ""
    assert exported.iloc[0]["最新单位净值"] == ""


@pytest.mark.parametrize("main_status", [None, "", "not_checked", "hard_block"])
def test_canonical_missing_or_blocked_status_does_not_inherit_detail_pass(tmp_path: Path, main_status) -> None:
    path = tmp_path / "canonical.xlsx"
    canonical = {**VALIDATION, "数据校验原始状态": main_status, "数据校验状态": None}
    write_source(path, canonical, VALIDATION)
    source, _, _ = project_tracker._load_tracker_source_frame(path)
    _, exported = build_export(tmp_path, path)
    expected = "" if main_status is None else main_status
    for frame in (source, exported):
        assert frame.iloc[0]["数据校验原始状态"] == expected
        assert frame.iloc[0]["数据校验状态"] == ""
        assert validation_status_value(frame.iloc[0]) != "strict_match"


def test_canonical_conflicting_validation_values_remain_bound_to_main_sheet(tmp_path: Path) -> None:
    path = tmp_path / "conflicting.xlsx"
    main = {**VALIDATION, "数据校验原始状态": "hard_block", "数据校验状态": "拦截",
            "数据校验备注": "isolated mismatch", "数据校验告警原因": "isolated mismatch"}
    write_source(path, main, VALIDATION)
    source, _, _ = project_tracker._load_tracker_source_frame(path)
    _, exported = build_export(tmp_path, path)
    for frame in (source, exported):
        for column, expected in main.items():
            assert frame.iloc[0][column] == expected


def test_legacy_workbook_does_not_invent_new_source_validation(tmp_path: Path) -> None:
    path = tmp_path / "legacy.xlsx"
    write_source(path, {"AutoStock原始校验状态": "hard_block", "AutoStock校验状态": "拦截"}, None)
    source, _, _ = project_tracker._load_tracker_source_frame(path)
    _, exported = build_export(tmp_path, path)
    assert not set(DATA_VALIDATION_COLUMNS).intersection(source.columns)
    for column in DATA_VALIDATION_COLUMNS:
        assert exported.iloc[0][column] == ""
    assert validation_status_value(exported.iloc[0]) == "hard_block"
