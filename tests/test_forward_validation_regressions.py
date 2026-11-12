from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from openpyxl import load_workbook

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.forward_validation import (
    FORMAL_GROUP_LABEL,
    ForwardValidationSnapshot,
    _build_snapshot_content_signature,
    _load_formal_frame,
    _load_watch_frame,
    build_forward_validation_package,
)


def _create_forward_validation_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE fund_nav_history (
                fcode TEXT,
                nav_date TEXT,
                unit_nav REAL,
                daily_growth REAL
            )
            """
        )
        rows = [
            ("000001", "2026-04-08", 1.0000),
            ("000001", "2026-05-08", 1.0200),
            ("000001", "2026-07-07", 1.0400),
            ("000002", "2026-04-08", 1.0000),
            ("000002", "2026-05-08", 1.0100),
            ("000002", "2026-07-07", 1.0300),
            ("000003", "2026-04-08", 1.0000),
            ("000003", "2026-05-08", 0.9950),
            ("000003", "2026-07-07", 1.0000),
        ]
        # Synthetic fixture: supply a complete provider-return series rather
        # than treating three distant NAV marks as daily total-return history.
        dense_rows = []
        for code in sorted({row[0] for row in rows}):
            points = pd.DataFrame([row[1:] for row in rows if row[0] == code], columns=['date', 'nav'])
            points['date'] = pd.to_datetime(points['date'])
            series = points.set_index('date')['nav'].reindex(pd.date_range(points.date.min(), points.date.max())).interpolate()
            growth = series.pct_change() * 100
            dense_rows.extend((code, day.date().isoformat(), float(nav), None if pd.isna(growth.loc[day]) else float(growth.loc[day]))
                              for day, nav in series.items())
        conn.executemany(
            "INSERT INTO fund_nav_history (fcode, nav_date, unit_nav, daily_growth) VALUES (?, ?, ?, ?)", dense_rows,
        )
        conn.commit()
    finally:
        conn.close()
    return path


def _write_forward_validation_report_bundle(
    run_dir: Path,
    *,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    final_count: int,
    stable_count: int,
    enhanced_count: int,
) -> tuple[Path, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    filtered_excel = run_dir / f"正式推荐债券基金_{run_id}.xlsx"
    watch_excel = run_dir / f"自动诊断池_债券基金_{run_id}.xlsx"

    formal_main_df = pd.DataFrame(
        [
            {"基金代码": "000001", "基金名称": "测试纯债A", "正式榜分组": "纯债稳健榜", "正式投资评分": "88.0"},
            {"基金代码": "000002", "基金名称": "测试增强A", "正式榜分组": "增强债弹性榜", "正式投资评分": "82.0"},
        ]
    )
    formal_detail_df = pd.DataFrame(
        [
            {"基金代码": "000001", "基金名称": "测试纯债A", "正式榜分组": "纯债稳健榜", "债基风格标签": "综合纯债", "是否含权益/转债增强": "否"},
            {"基金代码": "000002", "基金名称": "测试增强A", "正式榜分组": "增强债弹性榜", "债基风格标签": "二级债增强", "是否含权益/转债增强": "是"},
        ]
    )
    with pd.ExcelWriter(filtered_excel) as writer:
        formal_main_df.to_excel(writer, sheet_name="Sheet1", index=False)
        formal_detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    watch_df = pd.DataFrame(
        [
            {"基金代码": "000003", "基金名称": "测试候选A", "债基风格标签": "综合纯债", "是否含权益/转债增强": "否", "自动诊断分类": "质量告警诊断"},
        ]
    )
    with pd.ExcelWriter(watch_excel) as writer:
        watch_df.to_excel(writer, sheet_name="Sheet1", index=False)

    payload = {
        "run_id": run_id,
        "generated_at": generated_at,
        "counts": {
            "final_count": final_count,
            "stable_bond_final_count": stable_count,
            "enhanced_bond_final_count": enhanced_count,
        },
        "database_freshness": {
            "effective_nav_snapshot": {
                "candidate_date": snapshot_date,
                "candidate_fund_count": 3,
                "coverage_ratio_vs_total": 1.0,
                "ready": True,
            },
            "risk_1y_freshness": {
                "coverage_sufficient": True,
                "candidate_date": snapshot_date,
                "risk_1y_effective_end_date": snapshot_date,
                "dates_aligned": True,
            },
            "risk_1y_aligned_with_effective_nav": True,
        },
        "artifacts": {
            "filtered_excel": str(filtered_excel),
            "watch_excel": str(watch_excel),
        },
    }
    report_path = run_dir / f"bond_analysis_report_{run_id}.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return filtered_excel, watch_excel


def _attach_forward_validation_artifacts(run_dir: Path, run_id: str, result: dict[str, object]) -> None:
    report_path = run_dir / f"bond_analysis_report_{run_id}.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    artifacts = payload.setdefault("artifacts", {})
    artifacts.update(
        {
            "forward_validation_excel": result["artifacts"]["excel"],
            "forward_validation_json": result["artifacts"]["json"],
            "forward_validation_markdown": result["artifacts"]["markdown"],
        }
    )
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_forward_validation_package_builds_ready_window_summary(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    current_run_dir = runs_dir / "20260410_150653_sync"
    archived_run_dir = archived_runs_dir / "20260409_193238_sync"
    filtered_excel, watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    _write_forward_validation_report_bundle(
        archived_run_dir,
        run_id="20260409_193238_sync",
        generated_at="2026-04-09T19:36:40",
        snapshot_date="2026-03-31",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=5,
    )

    result = build_forward_validation_package(
        settings,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=2,
        stable_final_count=1,
        enhanced_final_count=1,
    )

    summary = result["summary"]
    assert summary["available"] is True
    assert summary["snapshot_total"] == 2
    assert summary["latest_snapshot_date"] == "2026-04-08"
    assert summary["status_label"] == "可滚动评估"
    window_map = {item["窗口"]: item for item in summary["window_summaries"]}
    assert window_map["1个月"]["已就绪快照数"] >= 1
    assert window_map["1个月"]["最新正式榜有效基金数"] == 2
    assert window_map["3个月"]["已就绪快照数"] >= 1
    comparison_preview = summary["comparison_preview"]
    assert any(item["样本组"] == "正式推荐整体" for item in comparison_preview)
    assert summary["incremental_review"]["available"] is True
    assert summary["incremental_review"]["new_fund_count"] == 0
    assert Path(result["artifacts"]["excel"]).exists()
    assert Path(result["artifacts"]["json"]).exists()
    assert Path(result["artifacts"]["markdown"]).exists()


def test_forward_validation_package_handles_empty_formal_sheet_with_columns(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_empty_formal.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    current_run_dir = runs_dir / "20260411_120000_sync"
    current_run_dir.mkdir(parents=True, exist_ok=True)

    filtered_excel = current_run_dir / "正式推荐债券基金_20260411_120000.xlsx"
    watch_excel = current_run_dir / "自动诊断池_债券基金_20260411_120000.xlsx"

    pd.DataFrame(columns=["基金代码", "基金名称", FORMAL_GROUP_LABEL]).to_excel(
        filtered_excel,
        sheet_name="Sheet1",
        index=False,
    )
    pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金名称": "测试债基A",
                FORMAL_GROUP_LABEL: "纯债稳健榜",
                "自动诊断分类": "质量告警诊断",
            }
        ]
    ).to_excel(watch_excel, sheet_name="Sheet1", index=False)

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    result = build_forward_validation_package(
        settings,
        run_id="20260411_120000_sync",
        generated_at="2026-04-11T12:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=0,
        stable_final_count=0,
        enhanced_final_count=0,
    )

    summary = result["summary"]
    assert summary["available"] is True
    assert summary["snapshot_total"] == 1
    assert summary["incremental_review"]["available"] is False
    assert summary["incremental_review"]["blocking_reason"] == "insufficient_snapshot_history"
    assert Path(result["artifacts"]["excel"]).exists()
    assert Path(result["artifacts"]["json"]).exists()
    assert Path(result["artifacts"]["markdown"]).exists()


def test_forward_validation_package_builds_incremental_review_for_new_formal_samples(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_incremental_review.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    current_run_dir = runs_dir / "20260410_150653_sync"
    archived_run_dir = archived_runs_dir / "20260409_193238_sync"
    filtered_excel, watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=3,
        stable_count=2,
        enhanced_count=1,
    )
    _write_forward_validation_report_bundle(
        archived_run_dir,
        run_id="20260409_193238_sync",
        generated_at="2026-04-09T19:36:40",
        snapshot_date="2026-03-31",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    current_main_df = pd.read_excel(filtered_excel, sheet_name="Sheet1", dtype=str).fillna("")
    current_main_df = pd.concat(
        [
            current_main_df,
            pd.DataFrame(
                [
                    {
                        "基金代码": "000003",
                        "基金名称": "测试新增纯债A",
                        "正式榜分组": "纯债稳健榜",
                        "正式投资评分": "84.0",
                        "1年-年化收益(%)": "3.2",
                        "1年-最大回撤(%)": "0.0100",
                        "1年-波动率": "0.0200",
                        "持有期状态": "已确认<=90天",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    current_detail_df = pd.read_excel(filtered_excel, sheet_name="推荐理由与风格", dtype=str).fillna("")
    current_detail_df = pd.concat(
        [
            current_detail_df,
            pd.DataFrame(
                [
                    {
                        "基金代码": "000003",
                        "基金名称": "测试新增纯债A",
                        "正式榜分组": "纯债稳健榜",
                        "债基风格标签": "综合纯债",
                        "是否含权益/转债增强": "否",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    with pd.ExcelWriter(filtered_excel) as writer:
        current_main_df.to_excel(writer, sheet_name="Sheet1", index=False)
        current_detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    result = build_forward_validation_package(
        settings,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=3,
        stable_final_count=2,
        enhanced_final_count=1,
    )

    incremental_review = result["summary"]["incremental_review"]
    assert incremental_review["available"] is True
    assert incremental_review["baseline_run_id"] == "20260409_193238_sync"
    assert incremental_review["new_fund_count"] == 1
    assert incremental_review["healthy_count"] == 1
    assert incremental_review["track_needed_count"] == 0
    assert incremental_review["light_track_count"] == 0
    assert incremental_review["review_preview"][0]["基金代码"] == "000003"
    assert incremental_review["review_preview"][0]["即时体检结论"] == "当前未见显著拖累"
    workbook = load_workbook(filename=result["artifacts"]["excel"], read_only=True)
    assert "新增样本体检" in workbook.sheetnames


def test_forward_validation_snapshot_signature_prefers_report_meta(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    filtered_excel = tmp_path / "正式推荐债券基金_20260416_000000.xlsx"
    watch_excel = tmp_path / "自动诊断池_债券基金_20260416_000000.xlsx"
    filtered_excel.write_bytes(b"placeholder")
    watch_excel.write_bytes(b"placeholder")
    filtered_meta = filtered_excel.with_name(f"{filtered_excel.stem}_report_meta.json")
    watch_meta = watch_excel.with_name(f"{watch_excel.stem}_report_meta.json")
    filtered_meta.write_text(
        json.dumps({"content_signature": "filtered-signature"}, ensure_ascii=False),
        encoding="utf-8",
    )
    watch_meta.write_text(
        json.dumps({"content_signature": "watch-signature"}, ensure_ascii=False),
        encoding="utf-8",
    )
    snapshot = ForwardValidationSnapshot(
        run_id="20260416_000000_sync",
        generated_at="2026-04-16T00:00:00",
        snapshot_date="2026-04-15",
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=392,
        stable_final_count=164,
        enhanced_final_count=228,
    )

    def _fail(*args, **kwargs):
        raise AssertionError("should not fall back to reading excel when report meta signatures exist")

    monkeypatch.setattr("backend.forward_validation._load_formal_frame", _fail)
    monkeypatch.setattr("backend.forward_validation._load_watch_frame", _fail)

    signature = _build_snapshot_content_signature(snapshot)

    assert signature


def test_forward_validation_snapshot_signature_falls_back_to_legacy_report_meta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    filtered_excel = tmp_path / "正式推荐债券基金_20260410_000000.xlsx"
    watch_excel = tmp_path / "内部观察层_债券基金_20260410_000000.xlsx"
    filtered_excel.write_bytes(b"placeholder")
    watch_excel.write_bytes(b"placeholder")
    filtered_meta = filtered_excel.with_name(f"{filtered_excel.stem}_report_meta.json")
    watch_meta = watch_excel.with_name(f"{watch_excel.stem}_report_meta.json")
    filtered_meta.write_text(
        json.dumps(
            {
                "report_type": "filtered_final_main",
                "output_file": str(filtered_excel),
                "counts": {"official_recommend_count": 298, "excluded_count": 1427},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    watch_meta.write_text(
        json.dumps(
            {
                "report_type": "internal_watchlist",
                "output_file": str(watch_excel),
                "counts": {"internal_watch_count": 5401, "quality_warning_watch_count": 622},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot = ForwardValidationSnapshot(
        run_id="20260410_000000_sync",
        generated_at="2026-04-10T00:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=298,
        stable_final_count=149,
        enhanced_final_count=149,
    )

    def _fail(*args, **kwargs):
        raise AssertionError("legacy report meta should avoid falling back to reading excel")

    monkeypatch.setattr("backend.forward_validation._load_formal_frame", _fail)
    monkeypatch.setattr("backend.forward_validation._load_watch_frame", _fail)

    signature = _build_snapshot_content_signature(snapshot)

    assert signature


def test_forward_validation_loads_snapshot_groups_from_csv_before_excel(tmp_path: Path) -> None:
    filtered_excel = tmp_path / "正式推荐债券基金_20260423_000000.xlsx"
    watch_excel = tmp_path / "自动诊断池_债券基金_20260423_000000.xlsx"
    filtered_excel.write_bytes(b"placeholder")
    watch_excel.write_bytes(b"placeholder")

    pd.DataFrame(
        [
            {"基金代码": "000001", "基金名称": "纯债A", FORMAL_GROUP_LABEL: "纯债稳健榜"},
            {"基金代码": "000002", "基金名称": "增强A", FORMAL_GROUP_LABEL: "增强债弹性榜"},
        ]
    ).to_csv(filtered_excel.with_suffix(".csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"基金代码": "000003", "基金名称": "纯债候选", "债基风格标签": "综合纯债", "是否含权益/转债增强": "否"},
            {"基金代码": "000004", "基金名称": "增强候选", "债基风格标签": "二级债增强", "是否含权益/转债增强": "是"},
        ]
    ).to_csv(watch_excel.with_suffix(".csv"), index=False, encoding="utf-8-sig")

    formal_df = _load_formal_frame(filtered_excel)
    watch_df = _load_watch_frame(watch_excel)

    assert formal_df["正式榜分组"].tolist() == ["纯债稳健榜", "增强债弹性榜"]
    assert watch_df["正式榜分组"].tolist() == ["纯债稳健榜", "增强债弹性榜"]


def test_forward_validation_package_reuses_previous_same_snapshot_signature(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_reuse.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    previous_run_dir = runs_dir / "20260410_090000_sync"
    current_run_dir = runs_dir / "20260410_150653_sync"

    previous_filtered_excel, previous_watch_excel = _write_forward_validation_report_bundle(
        previous_run_dir,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    current_filtered_excel, current_watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    _write_forward_validation_report_bundle(
        archived_runs_dir / "20260409_193238_sync",
        run_id="20260409_193238_sync",
        generated_at="2026-04-09T19:36:40",
        snapshot_date="2026-03-31",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    previous_result = build_forward_validation_package(
        settings,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=previous_filtered_excel,
        watch_excel=previous_watch_excel,
        final_count=2,
        stable_final_count=1,
        enhanced_final_count=1,
    )
    _attach_forward_validation_artifacts(previous_run_dir, "20260410_090000_sync", previous_result)

    with patch("backend.forward_validation._load_nav_history_for_codes", side_effect=AssertionError("should reuse cached forward validation")):
        current_result = build_forward_validation_package(
            settings,
            run_id="20260410_150653_sync",
            generated_at="2026-04-10T15:08:27",
            snapshot_date="2026-04-08",
            filtered_excel=current_filtered_excel,
            watch_excel=current_watch_excel,
            final_count=2,
            stable_final_count=1,
            enhanced_final_count=1,
        )

    summary = current_result["summary"]
    assert summary["latest_snapshot_run_id"] == "20260410_150653_sync"
    assert summary["reused_from_run_id"] == "20260410_090000_sync"
    assert summary["generated_at"] == "2026-04-10T15:08:27"
    assert summary["incremental_review"]["available"] is True
    previous_payload = json.loads(Path(previous_result["artifacts"]["json"]).read_text(encoding="utf-8"))
    reused_payload = json.loads(Path(current_result["artifacts"]["json"]).read_text(encoding="utf-8"))
    assert previous_payload["summary"]["latest_snapshot_run_id"] == "20260410_090000_sync"
    assert previous_payload["snapshot_rows"][0]["生成时间"] == "2026-04-10T09:00:00"
    reused_snapshot_df = pd.read_excel(Path(current_result["artifacts"]["excel"]), sheet_name="快照清单", dtype=str).fillna("")
    reused_comparison_df = pd.read_excel(Path(current_result["artifacts"]["excel"]), sheet_name="验证摘要", dtype=str).fillna("")
    assert previous_payload["detail_row_count"] > settings.preview_top_n
    assert previous_payload["detail_rows_truncated"] is True
    assert reused_payload["detail_row_count"] == previous_payload["detail_row_count"]
    assert reused_snapshot_df.iloc[0]["运行批次"] == "20260410_150653_sync"
    assert reused_snapshot_df.iloc[0]["生成时间"] == "2026-04-10T15:08:27"
    assert set(reused_comparison_df[reused_comparison_df["快照日期"] == "2026-04-08"]["运行批次"]) == {"20260410_150653_sync"}
    assert reused_payload["metadata"]["reused_from_run_id"] == "20260410_090000_sync"
    assert reused_payload["metadata"]["input_signature"]


@pytest.mark.parametrize("source_change", ["removed", "modified"])
def test_forward_validation_recomputes_full_details_if_source_changes_before_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_change: str,
) -> None:
    import backend.forward_validation as forward

    settings = SimpleNamespace(
        database_path=_create_forward_validation_db(tmp_path / "history.db"),
        project_root=tmp_path, runs_dir=tmp_path / "reports/runs",
        archived_runs_dir=tmp_path / "reports/archive/superseded_runs", preview_top_n=15,
    )
    previous_run = "20260410_090000_sync"
    current_run = "20260410_150653_sync"
    bundles = {}
    for run_id, generated_at in ((previous_run, "2026-04-10T09:00:00"),
                                 (current_run, "2026-04-10T15:08:27")):
        filtered, watch = _write_forward_validation_report_bundle(
            settings.runs_dir / run_id, run_id=run_id, generated_at=generated_at,
            snapshot_date="2026-04-08", final_count=2, stable_count=1, enhanced_count=1,
        )
        bundles[run_id] = dict(run_id=run_id, generated_at=generated_at, snapshot_date="2026-04-08",
                               filtered_excel=filtered, watch_excel=watch, final_count=2,
                               stable_final_count=1, enhanced_final_count=1)
    previous = build_forward_validation_package(settings, **bundles[previous_run])
    _attach_forward_validation_artifacts(settings.runs_dir / previous_run, previous_run, previous)
    full_count = json.loads(Path(previous["artifacts"]["json"]).read_text())["detail_row_count"]
    assert full_count > settings.preview_top_n
    original_reuse = forward._try_reuse_forward_validation_package
    original_nav = forward._load_nav_history_for_codes
    nav_calls = []

    def invalidate_source(*args, **kwargs):
        reused = original_reuse(*args, **kwargs)
        assert reused is not None
        path = reused["reused_excel_from"]
        if source_change == "removed":
            path.unlink()
        else:
            content = bytearray(path.read_bytes())
            content[100] ^= 1
            path.write_bytes(content)
        return reused

    def record_nav(*args, **kwargs):
        nav_calls.append(True)
        return original_nav(*args, **kwargs)

    monkeypatch.setattr(forward, "_try_reuse_forward_validation_package", invalidate_source)
    monkeypatch.setattr(forward, "_load_nav_history_for_codes", record_nav)
    current = build_forward_validation_package(settings, **bundles[current_run])
    payload = json.loads(Path(current["artifacts"]["json"]).read_text())
    workbook = load_workbook(current["artifacts"]["excel"], read_only=True)
    try:
        assert nav_calls
        assert payload["detail_row_count"] == full_count
        assert workbook["基金明细"].max_row - 1 == full_count
        assert "reused_from_run_id" not in current["summary"]
    finally:
        workbook.close()


def test_forward_validation_package_does_not_reuse_when_same_snapshot_signature_changes(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_no_reuse.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    previous_run_dir = runs_dir / "20260410_090000_sync"
    current_run_dir = runs_dir / "20260410_150653_sync"

    previous_filtered_excel, previous_watch_excel = _write_forward_validation_report_bundle(
        previous_run_dir,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    current_filtered_excel, current_watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    current_main_df = pd.read_excel(current_filtered_excel, sheet_name="Sheet1", dtype=str).fillna("")
    current_main_df = pd.concat(
        [
            current_main_df,
            pd.DataFrame(
                [
                    {
                        "基金代码": "000003",
                        "基金名称": "测试增强B",
                        "正式榜分组": "增强债弹性榜",
                        "正式投资评分": "79.0",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    current_detail_df = pd.read_excel(current_filtered_excel, sheet_name="推荐理由与风格", dtype=str).fillna("")
    current_detail_df = pd.concat(
        [
            current_detail_df,
            pd.DataFrame(
                [
                    {
                        "基金代码": "000003",
                        "基金名称": "测试增强B",
                        "正式榜分组": "增强债弹性榜",
                        "债基风格标签": "二级债增强",
                        "是否含权益/转债增强": "是",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    with pd.ExcelWriter(current_filtered_excel) as writer:
        current_main_df.to_excel(writer, sheet_name="Sheet1", index=False)
        current_detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    previous_result = build_forward_validation_package(
        settings,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=previous_filtered_excel,
        watch_excel=previous_watch_excel,
        final_count=2,
        stable_final_count=1,
        enhanced_final_count=1,
    )
    _attach_forward_validation_artifacts(previous_run_dir, "20260410_090000_sync", previous_result)

    call_counter = {"count": 0}
    original_loader = build_forward_validation_package.__globals__["_load_nav_history_for_codes"]

    def counting_loader(*args, **kwargs):
        call_counter["count"] += 1
        return original_loader(*args, **kwargs)

    with patch("backend.forward_validation._load_nav_history_for_codes", side_effect=counting_loader):
        current_result = build_forward_validation_package(
            settings,
            run_id="20260410_150653_sync",
            generated_at="2026-04-10T15:08:27",
            snapshot_date="2026-04-08",
            filtered_excel=current_filtered_excel,
            watch_excel=current_watch_excel,
            final_count=3,
            stable_final_count=1,
            enhanced_final_count=2,
        )

    assert call_counter["count"] > 0
    assert not current_result["summary"].get("reused_from_run_id")


def test_forward_validation_package_does_not_reuse_when_history_signature_changes(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_history_signature.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    previous_run_dir = runs_dir / "20260410_090000_sync"
    current_run_dir = runs_dir / "20260410_150653_sync"
    older_run_dir = archived_runs_dir / "20260409_193238_sync"

    previous_filtered_excel, previous_watch_excel = _write_forward_validation_report_bundle(
        previous_run_dir,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    current_filtered_excel, current_watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    _write_forward_validation_report_bundle(
        older_run_dir,
        run_id="20260409_193238_sync",
        generated_at="2026-04-09T19:36:40",
        snapshot_date="2026-03-31",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    previous_result = build_forward_validation_package(
        settings,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=previous_filtered_excel,
        watch_excel=previous_watch_excel,
        final_count=2,
        stable_final_count=1,
        enhanced_final_count=1,
    )
    _attach_forward_validation_artifacts(previous_run_dir, "20260410_090000_sync", previous_result)

    older_filtered_path = older_run_dir / "正式推荐债券基金_20260409_193238_sync.xlsx"
    older_main_df = pd.read_excel(older_filtered_path, sheet_name="Sheet1", dtype=str).fillna("")
    older_main_df.loc[0, "基金代码"] = "000003"
    older_detail_df = pd.read_excel(older_filtered_path, sheet_name="推荐理由与风格", dtype=str).fillna("")
    older_detail_df.loc[0, "基金代码"] = "000003"
    with pd.ExcelWriter(older_filtered_path) as writer:
        older_main_df.to_excel(writer, sheet_name="Sheet1", index=False)
        older_detail_df.to_excel(writer, sheet_name="推荐理由与风格", index=False)

    call_counter = {"count": 0}
    original_loader = build_forward_validation_package.__globals__["_load_nav_history_for_codes"]

    def counting_loader(*args, **kwargs):
        call_counter["count"] += 1
        return original_loader(*args, **kwargs)

    with patch("backend.forward_validation._load_nav_history_for_codes", side_effect=counting_loader):
        current_result = build_forward_validation_package(
            settings,
            run_id="20260410_150653_sync",
            generated_at="2026-04-10T15:08:27",
            snapshot_date="2026-04-08",
            filtered_excel=current_filtered_excel,
            watch_excel=current_watch_excel,
            final_count=2,
            stable_final_count=1,
            enhanced_final_count=1,
        )

    assert call_counter["count"] > 0
    assert not current_result["summary"].get("reused_from_run_id")


def test_forward_validation_package_reuses_legacy_artifact_when_history_inputs_unchanged(tmp_path: Path) -> None:
    db_path = _create_forward_validation_db(tmp_path / "forward_validation_legacy_reuse.db")
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archived_runs_dir = reports_dir / "archive" / "superseded_runs"
    previous_run_dir = runs_dir / "20260410_090000_sync"
    current_run_dir = runs_dir / "20260410_150653_sync"

    previous_filtered_excel, previous_watch_excel = _write_forward_validation_report_bundle(
        previous_run_dir,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )
    current_filtered_excel, current_watch_excel = _write_forward_validation_report_bundle(
        current_run_dir,
        run_id="20260410_150653_sync",
        generated_at="2026-04-10T15:08:27",
        snapshot_date="2026-04-08",
        final_count=2,
        stable_count=1,
        enhanced_count=1,
    )

    settings = SimpleNamespace(
        database_path=db_path,
        project_root=tmp_path,
        runs_dir=runs_dir,
        archived_runs_dir=archived_runs_dir,
        preview_top_n=15,
    )

    previous_result = build_forward_validation_package(
        settings,
        run_id="20260410_090000_sync",
        generated_at="2026-04-10T09:00:00",
        snapshot_date="2026-04-08",
        filtered_excel=previous_filtered_excel,
        watch_excel=previous_watch_excel,
        final_count=2,
        stable_final_count=1,
        enhanced_final_count=1,
    )
    _attach_forward_validation_artifacts(previous_run_dir, "20260410_090000_sync", previous_result)

    legacy_json_path = Path(previous_result["artifacts"]["json"])
    legacy_payload = json.loads(legacy_json_path.read_text(encoding="utf-8"))
    legacy_payload["metadata"].pop("history_signature", None)
    legacy_json_path.write_text(json.dumps(legacy_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with patch("backend.forward_validation._load_nav_history_for_codes", side_effect=AssertionError("should reuse legacy forward validation")):
        current_result = build_forward_validation_package(
            settings,
            run_id="20260410_150653_sync",
            generated_at="2026-04-10T15:08:27",
            snapshot_date="2026-04-08",
            filtered_excel=current_filtered_excel,
            watch_excel=current_watch_excel,
            final_count=2,
            stable_final_count=1,
            enhanced_final_count=1,
        )

    assert current_result["summary"]["reused_from_run_id"] == "20260410_090000_sync"
