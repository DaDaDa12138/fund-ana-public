from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from backend import scheduler as scheduler_module
from backend.project_tracker import (
    PROJECT_TRACKER_COLUMNS,
    _build_project_tracker_export_view,
    _try_reuse_project_final_tracker,
)
from backend.publication import _resolve_publish_blockers
from backend.publication_checks import build_publication_member_checks
from backend.publication_checks import FULL_NUMERIC_FIELDS, FULL_TEXT_FIELDS, write_publication_basis
from backend.project_report_exports import PERIOD_RETURN_SOURCE_COLUMNS
from backend.scheduler import ProjectScheduler
from backend.workflow import _workflow_publication_control
from database.rank_history_queries import build_rank_history_union_source_sql, read_latest_rank_dates


@pytest.mark.parametrize("scenario", ["intraday_before_collection", "intraday_after_collection", "daily_lineage_repair"])
def test_automatic_rescreen_can_formally_publish_with_same_run_risk_provenance(monkeypatch, tmp_path, scenario):
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        database_path=tmp_path / "unused.db", latest_report_file=tmp_path / "latest.json",
        effective_nav_coverage_ratio=0.9, effective_nav_weekday_only=True,
    )
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._log = lambda message: None
    scheduler._build_host_runtime_snapshot = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._build_holding_period_change_summary = lambda **kwargs: {}
    scheduler._build_maintenance_data_change_summary = lambda **kwargs: {"changed": False}
    scheduler._build_data_quality_audits_compat = lambda **kwargs: {}
    scheduler._daily_refresh_lineage_rescreen_required = lambda **kwargs: {"required": scenario == "daily_lineage_repair"}
    snapshot = SimpleNamespace(candidate_date="2026-09-04", signature="current", to_dict=lambda: {})
    monkeypatch.setattr(scheduler_module, "load_effective_nav_snapshot", lambda *a, **k: snapshot)
    monkeypatch.setattr(scheduler_module, "load_latest_screened_db_snapshot", lambda *a: {})
    changes = [(scenario == "intraday_before_collection", "before"), (scenario == "intraday_after_collection", "after")]
    monkeypatch.setattr(scheduler_module, "screening_snapshot_changed", lambda *a: changes.pop(0))
    monkeypatch.setattr(scheduler_module, "run_project_pipeline", lambda *a: {})
    monkeypatch.setattr(scheduler_module, "run_autostock_nav_tail_fill", lambda *a, **k: {})
    monkeypatch.setattr(scheduler_module, "_build_intraday_autostock_tail_fill_skip_summary", lambda *a: {
        "selection": {"raw_candidate_total": 0, "cooldown_skipped_total": 0}, "maintenance_result": {},
    })
    options_seen = []
    def workflow(options, **kwargs):
        options_seen.append(options)
        return {"run_id": "audited-run", "counts": {"final_count": 1}}
    scheduler._run_workflow_in_subprocess = workflow
    result = scheduler._execute_scheduled_refresh(
        task_name="audit", refresh_window="daily" if scenario.startswith("daily") else "intraday",
        nav_history_sync_enabled=False, nav_history_sync_limit=0,
        long_term_history_enabled=False, long_term_history_limit=0,
    )
    assert result["screening_triggered"] is True
    assert len(options_seen) == 1
    options = options_seen[0]
    assert _workflow_publication_control(options)["eligible_for_formal_publication"] is True
    assert options["force_calculate_risk"] is True
    assert options["risk_refresh_changed_nav_only"] is True
    assert options.get("skip_risk_calculation") is not True


def test_latest_rank_seek_matches_union_and_observes_corrections_and_deletes():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE fund_rank_history(fcode TEXT, rank_date TEXT, period TEXT)")
    conn.execute("CREATE INDEX rank_key ON fund_rank_history(fcode, rank_date, period)")
    conn.execute("ATTACH DATABASE ':memory:' AS cold")
    conn.execute("CREATE TABLE cold.history(fcode TEXT, rank_date TEXT, period TEXT)")
    conn.execute("CREATE INDEX cold.rank_key ON history(fcode, rank_date, period)")
    conn.executemany("INSERT INTO fund_rank_history VALUES (?, ?, ?)", [
        ("000001", "2026-09-04", "1y"), ("000001", "2024-01-01", "1y"),
        ("000002", "", "1y"), ("000005", None, "1y"),
    ])
    conn.executemany("INSERT INTO cold.history VALUES (?, ?, ?)", [
        ("000001", "2024-01-01", "1y"), ("000002", "2023-12-01", "1y"),
        ("000003", "2024-09-04", "1y"), ("000004", "2024-02-01", "1y"),
    ])
    archive = {"attached": True, "alias": "cold", "archive_table_name": "history", "archive_cutoff_date": "2024-09-04"}
    def baseline():
        source = build_rank_history_union_source_sql(archive, columns=("fcode", "rank_date"))
        return dict(conn.execute(f"SELECT fcode, MAX(rank_date) FROM ({source}) GROUP BY fcode"))
    assert read_latest_rank_dates(conn, archive) == baseline()
    assert conn.in_transaction  # Caller-owned writes are neither committed nor rolled back.
    conn.execute("DELETE FROM fund_rank_history WHERE rank_date='2026-09-04'")
    assert read_latest_rank_dates(conn, archive) == baseline()
    conn.execute("UPDATE cold.history SET rank_date='2024-03-01' WHERE fcode='000004'")
    assert read_latest_rank_dates(conn, archive) == baseline()
    conn.commit()
    assert read_latest_rank_dates(conn, archive)["000004"] == "2024-03-01"
    assert not conn.in_transaction
    conn.close()


@pytest.mark.parametrize("audit", [None, {}, {"aligned": False}, {"aligned": True, "blocker_count": 1},
                                         {"aligned": True}, {"aligned": True, "checks": [], "check_count": 0},
                                         {"aligned": True, "checks": [{"aligned": False}]},
                                         {"aligned": True, "checks": [None]}])
def test_publication_blocks_missing_or_failed_artifact_evidence(audit):
    report = {"publication_control": {"artifact_checks_required": True}, "internal_audit": {"artifact_consistency": audit}}
    blockers = _resolve_publish_blockers(report)
    assert any(b["code"] == "publication_artifact_integrity_failed" and b["blocking"] for b in blockers)


def test_tracker_duplicate_nav_refreshes_metrics_without_inflating_occurrences(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path)
    old = {column: "" for column in PROJECT_TRACKER_COLUMNS}
    old.update({"基金代码": "000001", "基金名称": "fixture", "当前是否在榜": "是", "出现次数": 2,
                "出现频率(%)": 100, "正式投资评分": 80, "最新夏普比率": 4.2, "最新索提诺比率": 6.0})
    export = _build_project_tracker_export_view(pd.DataFrame([old]))
    process = pd.DataFrame([{"业务日期": "2026-09-06", "净值快照日期": "2026-09-04", "快照签名": "same-nav",
                             "项目批次": "old", "生成时间": "2026-09-06T06:00:00", "正式推荐数量": "1", "来源文件": "old.xlsx"}])
    source = pd.DataFrame([{"基金代码": "000001", "基金名称": "fixture", "正式投资评分": 81.2,
                            "1年-夏普比率": 4.42, "1年-索提诺比率": None}])
    reused = _try_reuse_project_final_tracker(
        settings, current_run_id="new", current_generated_at="2026-09-07T09:00:00",
        current_filtered_excel=tmp_path / "new.xlsx", current_final_count=1,
        read_project_tracker_export_sheet=lambda *a: export,
        read_project_tracker_process_sheet=lambda *a: process,
        load_tracker_source_frame=lambda *a: (source, "same-nav", "2026-09-04"),
    )
    assert reused is not None
    frame, _, summary = reused
    assert summary["latest_run_is_new_snapshot"] is False
    assert summary["total_periods"] == 1
    assert summary["current_metrics_refreshed"] is True
    assert int(frame.loc[0, "出现次数"]) == 2
    assert float(frame.loc[0, "正式投资评分"]) == 81.2
    assert float(frame.loc[0, "最新夏普比率"]) == 4.42
    assert frame.loc[0, "最新索提诺比率"] == ""


@pytest.fixture
def member_artifacts(tmp_path):
    run_dir = tmp_path / "runs" / "test"
    run_dir.mkdir(parents=True)
    row = {"基金代码": "000001", "正式投资评分": "81.2", "1年-年化收益(%)": "3.13", "1年-波动率": "0.33",
           "1年-最大回撤(%)": "0.28", "1年-夏普比率": "4.42", "最新净值日期": "2026-09-04",
           "正式榜分组": "纯债稳健榜", "AutoStock校验状态": "通过"}
    for name in ["formal.csv", "dynamic.csv"]:
        with (run_dir / name).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row)); writer.writeheader(); writer.writerow(row)
    tracker_row = {**row, "1年-波动率": "0.003279", "1年-最大回撤(%)": "0.002797"}
    tracker = {"run_id": "test", "row_count": 1, "rows": [tracker_row]}
    tracker_path = run_dir / "tracker.json"
    tracker_path.write_text(json.dumps(tracker, ensure_ascii=False))
    db_path = tmp_path / "funds.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE funds(fcode TEXT, holding_days INT, mgr_fee_rate REAL, trust_fee_rate REAL, sales_fee_rate REAL)")
        conn.execute("INSERT INTO funds VALUES ('000001',90,0.3,0.1,0.0)")
    settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "runs", database_path=db_path, exclude_holding_days_gt=90)
    report = {"run_id": "test", "counts": {"final_count": 1, "dynamic_final_count": 1}, "artifacts": {
        "filtered_csv": str(run_dir / "formal.csv"), "dynamic_filtered_csv": str(run_dir / "dynamic.csv"),
        "project_tracker_current_detail_json": str(tracker_path),
    }}
    return settings, report, tracker, tracker_path


def test_member_checks_respect_csv_percent_units_and_real_zero_fees(member_artifacts):
    settings, report, _, _ = member_artifacts
    assert all(c["aligned"] for c in build_publication_member_checks(settings, report))


@pytest.fixture
def full_member_artifacts(member_artifacts):
    settings, report, tracker, tracker_path = member_artifacts
    row = tracker["rows"][0]
    for field in FULL_NUMERIC_FIELDS:
        row.setdefault(field, "")
    for field in FULL_TEXT_FIELDS:
        row.setdefault(field, "")
    row.update({"基金代码": "000001", "费率合计(%)": "0.4", "1年-VaR95(%)": "0", "1年-索提诺比率": ""})
    tracker["derived_fields"] = {"data_source_by_code": {"000001": "synthetic_api_fixture"}}
    tracker_path.write_text(json.dumps(tracker))
    report["publication_control"] = {"full_value_checks_required": True}
    report["generated_at"] = "2026-09-07T10:00:00"
    report["artifacts"]["report_json"] = str(tracker_path.parent / "report.json")
    workbook = tracker_path.parent / "tracker.xlsx"
    with pd.ExcelWriter(workbook) as writer:
        pd.DataFrame([row]).to_excel(writer, sheet_name="当前在榜", index=False, startrow=2)
        pd.DataFrame([{"净值快照日期": "2026-09-04", "快照签名": "test-snapshot", "项目批次": "test"}]).to_excel(
            writer, sheet_name="处理记录", index=False)
    report["artifacts"]["project_tracker_excel"] = str(workbook)
    tracker_summary = {"total_periods": 1, "unique_snapshot_count": 1, "processed_run_count": 1}
    report["counts"]["project_tracker_total_periods"] = 1
    report["project_publish_summary"] = {"project_tracker_summary": dict(tracker_summary)}
    metadata_path = tracker_path.parent / "tracker_meta.json"
    metadata_path.write_text(json.dumps({"accumulation_summary": tracker_summary}))
    report["artifacts"]["project_tracker_excel_meta"] = str(metadata_path)
    with sqlite3.connect(settings.database_path) as conn:
        for name, dtype in (("has_holding_period", "INT"), ("holding_period_source", "TEXT"), ("is_buyable", "INT"),
                            ("purchase_status", "TEXT"), ("data_source", "TEXT"), ("data_source_governance_flag", "TEXT")):
            conn.execute(f"ALTER TABLE funds ADD COLUMN {name} {dtype}")
        conn.execute("UPDATE funds SET data_source='synthetic_api_fixture'")
    path = write_publication_basis(settings, report, official_records=[dict(row)], risk_records=[])
    report["artifacts"]["publication_basis_json"] = str(path)
    return settings, report, tracker_path, workbook


def test_full_value_contract_preserves_undefined_and_real_zero(full_member_artifacts):
    settings, report, _, _ = full_member_artifacts
    checks = build_publication_member_checks(settings, report)
    assert all(check["aligned"] for check in checks), checks


@pytest.mark.parametrize('corruption', [None, 'historical_value', 'missing_contract', 'wrong_date', 'missing_risk'])
def test_publication_checks_compare_duration_with_its_risk_source(full_member_artifacts, corruption):
    """All exports can agree and still carry the same incorrect historical value."""
    from core.utils.drawdown_duration import ANNUAL_DRAWDOWN_OBSERVATIONS as field

    settings, report, tracker_path, workbook = full_member_artifacts
    tracker = json.loads(tracker_path.read_text())
    row = tracker['rows'][0]
    row[field] = 67 if corruption == 'historical_value' else 18
    tracker_path.write_text(json.dumps(tracker))
    for key in ('filtered_csv', 'dynamic_filtered_csv'):
        csv_row = {**row, '1年-波动率': '0.33', '1年-最大回撤(%)': '0.28'}
        with Path(report['artifacts'][key]).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(csv_row))
            writer.writeheader()
            writer.writerow(csv_row)
    with pd.ExcelWriter(workbook, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        pd.DataFrame([row]).to_excel(writer, sheet_name='当前在榜', index=False, startrow=2)
    risk = {'fcode': '000001', 'period': '1y', 'max_drawdown_duration': 18, 'data_points': 250,
            'start_date': '2025-09-04', 'end_date': '2026-04-30' if corruption == 'wrong_date' else '2026-09-04',
            'nav_snapshot_signature': 'f' * 64, 'calculated_at': '2026-09-07T10:00:00'}
    report['publication_control']['drawdown_source_checks_required'] = corruption != 'missing_contract'
    write_publication_basis(settings, report, official_records=[row], risk_records=[] if corruption == 'missing_risk' else [risk])
    report['publication_control']['drawdown_source_checks_required'] = True
    checks = build_publication_member_checks(settings, report)
    by_name = {item['name']: item for item in checks}
    assert by_name['formal_all_values_from_frozen_inputs']['aligned'] is True
    assert by_name['formal_drawdown_matches_current_risk_source']['aligned'] is (corruption is None)
    assert not any(item['name'] == 'publication_member_evidence_readable' for item in checks)


@pytest.fixture
def source_member_artifacts(full_member_artifacts):
    """Two isolated tracks; the dynamic-only row has genuinely missing sources."""
    settings, report, tracker_path, _ = full_member_artifacts
    row = json.loads(tracker_path.read_text())["rows"][0]
    row.update({"区间收益基准日": "2026-09-04", "区间收益来源": "isolated source fixture",
                "区间收益来源SHA256": "isolated source fingerprint"})
    dynamic = [dict(row), {**row, "基金代码": "000002", **dict.fromkeys(PERIOD_RETURN_SOURCE_COLUMNS)}]
    report["publication_control"]["csv_return_provenance_checks_required"] = True
    report["counts"]["dynamic_final_count"] = 2
    for artifact, rows in (("filtered_csv", [row]), ("dynamic_filtered_csv", dynamic)):
        from backend.metric_units import apply_metric_display_units_to_frame
        apply_metric_display_units_to_frame(pd.DataFrame(rows)).to_csv(report["artifacts"][artifact], index=False)
    with sqlite3.connect(settings.database_path) as conn:
        conn.execute("INSERT INTO funds(fcode,holding_days,mgr_fee_rate,trust_fee_rate,sales_fee_rate) VALUES ('000002',90,0.3,0.1,0.0)")
    write_publication_basis(settings, report, official_records=[row], risk_records=[], dynamic_records=dynamic)
    return settings, report, row, dynamic


def test_csv_sources_match_complete_formal_and_dynamic_tracks(source_member_artifacts):
    settings, report, _, _ = source_member_artifacts
    checks = build_publication_member_checks(settings, report)
    assert all(check["aligned"] for check in checks), checks
    check = next(c for c in checks if c["name"] == "csv_return_provenance_matches_frozen_inputs")
    assert check["formal_count"] == 1 and check["dynamic_count"] == 2


@pytest.mark.parametrize("artifact", ["filtered_csv", "dynamic_filtered_csv"])
@pytest.mark.parametrize("field", PERIOD_RETURN_SOURCE_COLUMNS)
@pytest.mark.parametrize("fault", ["missing_column", "changed_value"])
def test_csv_sources_reject_missing_or_changed_values(source_member_artifacts, artifact, field, fault):
    settings, report, _, _ = source_member_artifacts
    path = Path(report["artifacts"][artifact])
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if fault == "missing_column":
        frame = frame.drop(columns=[field])
    else:
        frame.loc[0, field] = "isolated corrupt source"
    frame.to_csv(path, index=False)
    checks = build_publication_member_checks(settings, report)
    assert next(c for c in checks if c["name"] == "csv_return_provenance_matches_frozen_inputs")["aligned"] is False


@pytest.mark.parametrize("field", PERIOD_RETURN_SOURCE_COLUMNS)
def test_dynamic_only_null_source_cannot_become_zero(source_member_artifacts, field):
    settings, report, _, _ = source_member_artifacts
    path = Path(report["artifacts"]["dynamic_filtered_csv"])
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame.loc[frame["基金代码"] == "000002", field] = "0"
    frame.to_csv(path, index=False)
    checks = build_publication_member_checks(settings, report)
    check = next(c for c in checks if c["name"] == "csv_return_provenance_matches_frozen_inputs")
    assert check["aligned"] is False
    assert check["samples"] == [{"artifact": "dynamic_filtered_csv", "fcode": "000002", "field": field,
                                  "expected": None, "actual": "0"}]


def test_csv_sources_reject_same_count_dynamic_member_swap(source_member_artifacts):
    settings, report, _, _ = source_member_artifacts
    path = Path(report["artifacts"]["dynamic_filtered_csv"])
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame.loc[frame["基金代码"] == "000002", "基金代码"] = "000003"
    frame.to_csv(path, index=False)
    checks = build_publication_member_checks(settings, report)
    assert next(c for c in checks if c["name"] == "dynamic_csv_count")["aligned"] is True
    assert next(c for c in checks if c["name"] == "csv_return_provenance_matches_frozen_inputs")["aligned"] is False


@pytest.mark.parametrize("fault", ["missing_dynamic", "formal_column", "dynamic_column"])
def test_freezing_csv_sources_requires_both_track_inputs(source_member_artifacts, fault):
    settings, report, row, dynamic = source_member_artifacts
    if fault == "formal_column":
        row.pop("区间收益来源")
    if fault == "dynamic_column":
        dynamic[1].pop("区间收益来源")
    with pytest.raises(ValueError, match="csv_return_provenance"):
        write_publication_basis(settings, report, official_records=[row], risk_records=[],
                                dynamic_records=None if fault == "missing_dynamic" else dynamic)


def test_empty_dynamic_csv_still_requires_source_headers(source_member_artifacts):
    settings, report, row, _ = source_member_artifacts
    report["counts"]["dynamic_final_count"] = 0
    path = Path(report["artifacts"]["dynamic_filtered_csv"])
    pd.DataFrame(columns=list(row)).to_csv(path, index=False)
    write_publication_basis(settings, report, official_records=[row], risk_records=[], dynamic_records=[])
    assert all(c["aligned"] for c in build_publication_member_checks(settings, report))
    pd.DataFrame(columns=["基金代码"]).to_csv(path, index=False)
    checks = build_publication_member_checks(settings, report)
    assert next(c for c in checks if c["name"] == "csv_return_provenance_matches_frozen_inputs")["aligned"] is False


def test_dynamic_source_freeze_is_covered_by_input_signature(source_member_artifacts):
    settings, report, _, _ = source_member_artifacts
    path = Path(report["artifacts"]["publication_basis_json"])
    basis = json.loads(path.read_text())
    basis["inputs"]["dynamic_csv_return_provenance_rows"][1]["区间收益来源"] = "isolated corruption"
    path.write_text(json.dumps(basis))
    checks = build_publication_member_checks(settings, report)
    assert next(c for c in checks if c["name"] == "publication_input_signature")["aligned"] is False


def test_csv_source_check_cannot_silently_skip_without_full_inputs(source_member_artifacts):
    settings, report, _, _ = source_member_artifacts
    report["publication_control"]["full_value_checks_required"] = False
    checks = build_publication_member_checks(settings, report)
    assert checks[0]["aligned"] is False
    assert "csv_return_provenance_requires_full_value_checks" in checks[0]["detail"]


def test_full_value_contract_rejects_processing_count_as_observation_periods(full_member_artifacts):
    settings, report, _, _ = full_member_artifacts
    report["counts"]["project_tracker_total_periods"] = 2
    checks = build_publication_member_checks(settings, report)
    assert [check["name"] for check in checks if not check["aligned"]] == ["tracker_observation_period_counts"]


@pytest.mark.parametrize("fault", ["var95", "sortino_null_as_zero", "fee", "source", "workbook_member", "input_signature"])
def test_full_value_contract_rejects_corrupted_values_and_sources(full_member_artifacts, fault):
    settings, report, tracker_path, workbook = full_member_artifacts
    tracker = json.loads(tracker_path.read_text())
    if fault == "var95": tracker["rows"][0]["1年-VaR95(%)"] = "0.3"
    if fault == "sortino_null_as_zero": tracker["rows"][0]["1年-索提诺比率"] = "0"
    if fault == "fee": tracker["rows"][0]["费率合计(%)"] = "0.0"
    if fault == "source": tracker["derived_fields"]["data_source_by_code"]["000001"] = "wrong_source"
    if fault == "workbook_member":
        row = {**tracker["rows"][0], "基金代码": "000002"}
        pd.DataFrame([row]).to_excel(workbook, sheet_name="当前在榜", index=False, startrow=2)
    if fault == "input_signature":
        basis_path = Path(report["artifacts"]["publication_basis_json"])
        basis = json.loads(basis_path.read_text()); basis["inputs"]["official_rows"][0]["1年-VaR95(%)"] = "0.3"
        basis_path.write_text(json.dumps(basis))
    tracker_path.write_text(json.dumps(tracker))
    checks = build_publication_member_checks(settings, report)
    assert any(not check["aligned"] for check in checks), checks


@pytest.mark.parametrize("fault", ["stale_score", "same_count_wrong_member", "holding", "missing_fee", "hard_block", "missing_file"])
def test_member_checks_reject_real_publication_failure_modes(member_artifacts, fault):
    settings, report, tracker, tracker_path = member_artifacts
    if fault == "stale_score": tracker["rows"][0]["正式投资评分"] = "80"
    elif fault == "same_count_wrong_member": tracker["rows"][0]["基金代码"] = "000002"
    elif fault == "hard_block": tracker["rows"][0]["AutoStock校验状态"] = "拦截"
    elif fault in {"holding", "missing_fee"}:
        with sqlite3.connect(settings.database_path) as conn:
            conn.execute("UPDATE funds SET " + ("holding_days=91" if fault == "holding" else "sales_fee_rate=NULL"))
    tracker_path.write_text(json.dumps(tracker, ensure_ascii=False))
    if fault == "missing_file": tracker_path.unlink()
    assert any(not c["aligned"] for c in build_publication_member_checks(settings, report))
