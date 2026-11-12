from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.project_tracker import (
    _build_project_final_tracker,
    _build_project_tracker_unique_snapshots_from_process_rows,
    _load_tracker_source_frame,
)


def _source(root: Path, run_id: str, generated_at: str, nav_date: str, code: str) -> dict:
    path = root / f"{run_id}.xlsx"
    pd.DataFrame([{"基金代码": code, "基金名称": "测试输入", "最新净值日期": nav_date,
                   "正式投资评分": 80.0, "1年-年化收益(%)": 3.0}]).to_excel(path, index=False)
    return {"run_id": run_id, "generated_at": generated_at, "business_date": generated_at[:10],
            "nav_snapshot_date": nav_date, "final_count": 1, "excel_path": path}


def _build(settings, history, reuse=None):
    current = history[-1]
    return _build_project_final_tracker(
        settings, current_run_id=current["run_id"], current_generated_at=current["generated_at"],
        current_filtered_excel=current["excel_path"], current_final_count=current["final_count"],
        current_nav_snapshot_date=current["nav_snapshot_date"],
        load_project_report_history=lambda *args, **kwargs: history,
        try_reuse_project_final_tracker=reuse,
    )


def test_rebuild_counts_unique_nav_observations_and_keeps_latest_revision(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path)
    first = _source(tmp_path, "first", "2026-05-19T10:00:00", "2026-05-18", "000001")
    revised = _source(tmp_path, "revised", "2026-05-20T10:00:00", "2026-05-18", "000002")
    current = _source(tmp_path, "current", "2026-05-21T10:00:00", "2026-05-20", "000002")
    tracker, process, summary = _build(settings, [first, revised, current])
    assert len(process) == summary["processed_run_count"] == 3
    assert summary["total_periods"] == summary["unique_snapshot_count"] == 2
    assert summary["duplicate_snapshot_run_count"] == 1
    assert set(tracker["基金代码"]) == {"000002"}
    assert tracker.iloc[0]["出现次数"] == 2
    assert summary["latest_membership_change_summary"]["previous_run_id"] == "revised"
    reused_snapshots = _build_project_tracker_unique_snapshots_from_process_rows(
        settings, process, load_tracker_source_frame=_load_tracker_source_frame)
    assert [item["run_id"] for item in reused_snapshots] == ["revised", "current"]


@pytest.mark.parametrize("change", ["removed_from_publishable_history", "source_content_changed", "missing_cache_contract"])
def test_cached_tracker_requires_same_verified_history(tmp_path, change):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path)
    previous = _source(tmp_path, "previous", "2026-05-19T10:00:00", "2026-05-18", "000001")
    current = _source(tmp_path, "current", "2026-05-21T10:00:00", "2026-05-20", "000002")
    history = [previous, current]
    rebuilt = _build(settings, history)
    metadata = {"accumulation_summary": rebuilt[2]}
    meta_path = tmp_path / "最终优质基金累积跟踪_report_meta.json"
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    reuse_calls = []

    def reuse(*args, **kwargs):
        reuse_calls.append(True)
        return rebuilt

    _build(settings, history, reuse)
    assert reuse_calls == [True]
    reuse_calls.clear()
    if change == "removed_from_publishable_history":
        history = [current]
    elif change == "source_content_changed":
        _source(tmp_path, "previous", "2026-05-19T10:00:00", "2026-05-18", "000003")
    else:
        del metadata["accumulation_summary"]["history_input_contract"]
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    tracker, _, summary = _build(settings, history, reuse)
    assert reuse_calls == []
    assert summary["total_periods"] == summary["unique_snapshot_count"]
    if change == "removed_from_publishable_history":
        assert summary["total_periods"] == 1
        assert set(tracker["基金代码"]) == {"000002"}
    if change == "source_content_changed":
        assert set(tracker["基金代码"]) == {"000002", "000003"}


def test_missing_history_source_does_not_reuse_cached_counts(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path)
    previous = _source(tmp_path, "previous", "2026-05-19T10:00:00", "2026-05-18", "000001")
    current = _source(tmp_path, "current", "2026-05-21T10:00:00", "2026-05-20", "000002")
    previous["excel_path"].unlink()
    with pytest.raises(FileNotFoundError):
        _build(settings, [previous, current])
