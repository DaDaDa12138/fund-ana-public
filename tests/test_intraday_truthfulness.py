"""Audit job ownership and business degradation using isolated fixture artifacts."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from bond_fund_screening.tasks.subprocess_logging import classify_stderr_signals
from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
from scripts import audit_data_truthfulness as audit


@pytest.fixture
def intraday(tmp_path: Path):
    run_id = "fixture-intraday-run"
    db = tmp_path / "fixture.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE task_history (id INTEGER PRIMARY KEY, task_name TEXT,
            status TEXT, started_at TEXT, completed_at TEXT, duration INTEGER,
            records_processed INTEGER, records_failed INTEGER, error_message TEXT, extra_data TEXT)""")
    period = {
        "schema_version": 2, "task": "collect-period-returns", "source": "fundMNPeriodIncrease",
        "status": "degraded", "target_count": 0, "saved_fund_count": 0, "failed_count": 0,
        "request_count": 0, "changed_count": 0, "pending_count": 1, "cooldown_count": 1,
        "unselected_count": 0, "outcomes": [], "changed_fcodes": [],
        "cooldown_outcomes": [{"fcode": "fixture-lagged", "status": "date_mismatch_quarantined",
                               "unavailable_without_returns": False, "quarantined_date_lag": True}],
        "nonblocking_missing_fcodes": [], "quarantined_date_lag_fcodes": ["fixture-lagged"],
        "blocking_count": 0, "screening_blocked": False,
    }
    response = tmp_path / "period_response.json"
    response.write_text(json.dumps({"summary": deepcopy(period), "observations": []}))
    period.update(response_artifact=str(response),
                  response_artifact_sha256=hashlib.sha256(response.read_bytes()).hexdigest())
    completion = "\n".join([
        "任务完成统计", "总任务数: 1", "已处理: 1", "成功: 1", "跳过: 0", "失败: 0", "成功率: 100.00%",
    ])
    stdout_by_task = {
        "collect-nav": completion + "\nNavCollector 统计\n总数: 1\n成功: 1\n失败: 0\n跳过: 0\n"
                       "成功率: 100.00%\n增量更新完成: 0条净值记录\n",
        "calculate-risk": completion + "\n风险指标同步完成: 目标基金1只, 生成9个周期\n",
        "collect-period-returns": "api_source_evidence=" + json.dumps(period) + "\n",
    }
    children = []
    for task, stdout in stdout_by_task.items():
        meta, out, err = [tmp_path / f"{task}.{suffix}" for suffix in ("meta.json", "stdout.log", "stderr.log")]
        out.write_text(stdout)
        err.write_text("")
        children.append({
            "task_name": task, "success": True, "return_code": 0, "timed_out": False, "timeout_reason": None,
            "command": ["python", "main.py", task], "duration_seconds": 1,
            "started_at": "2026-09-10T13:45:01", "finished_at": "2026-09-10T13:45:02",
            "meta_path": str(meta), "stdout_path": str(out), "stderr_path": str(err),
            "stderr_signal_summary": classify_stderr_signals(
                task_name=task, stderr_path=err, success=True, return_code=0, timed_out=False),
        })
        meta.write_text(json.dumps(children[-1]))
    index_path = tmp_path / "subprocess_index.json"
    index = {"run_id": None, "items": children, "subprocess_count": 3, "success_count": 3,
             "failed_count": 0, "failed_task_names": [], "period_return_collection_planned": True}
    task_log = tmp_path / "intraday.log"
    task_log.write_text("api_source_evidence=" + json.dumps(period) + "\n"
                        f"[core-pipeline] subprocess_index={index_path} count=3 failed=0\n")
    task_audit_path = tmp_path / "task_audit.json"
    extra = {
        "status": "degraded", "collection_profile": "daily", "degraded": True,
        "degraded_stage": "collect-period-returns", "warning_reason": "period_return_date_lag_quarantined",
        "screening_run_id": run_id, "screening_final_count": 2, "screening_triggered": True,
        "period_return_collection": period, "task_log_path": str(task_log),
        "task_run_audit_path": str(task_audit_path),
    }
    row = {"id": 1, "task_name": audit.AUTOMATION_INTRADAY_REFRESH_TASK_NAME, "status": "degraded"}
    task_audit = {
        "task": {"task_history_id": 1, "task_name": row["task_name"], "status": "degraded"},
        "task_result": deepcopy(extra), "workflow_summary": {"status": "succeeded", "run_id": run_id,
            "options": {"run_collection": False, "run_screening": False}},
    }

    def save():
        index_path.write_text(json.dumps(index))
        task_audit_path.write_text(json.dumps(task_audit))
        with sqlite3.connect(db) as conn:
            conn.execute("INSERT OR REPLACE INTO task_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["id"], row["task_name"], row["status"], "2026-09-10 13:45:00", "2026-09-10 14:00:00",
                900, None, 0, None, json.dumps(extra)))

    save()
    return SimpleNamespace(settings=SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path, database_path=db),
        row=row, extra=extra, task_audit=task_audit, index=index, index_path=index_path, response=response,
        task_log=task_log, period=period, save=save, run_id=run_id)


def check(fixture):
    return audit._find_scheduled_refresh_task_truth(fixture.settings, run_id=fixture.run_id, final_count=2)


def test_intraday_uses_its_own_nav_and_risk_logs_and_keeps_degraded_terminal(intraday):
    result, blockers, warnings = check(intraday)
    assert not blockers
    assert result["matched"] and result["task_name"] == audit.AUTOMATION_INTRADAY_REFRESH_TASK_NAME
    assert result["row"]["status"] == result["task_run_audit"]["audit_task_status"] == "degraded"
    assert result["extra_summary"]["collection_profile"] == "daily"
    assert result["collection_degradation"]["pending_count"] == 1
    assert "scheduled_refresh_collection_degraded:period_return_date_lag_quarantined" in warnings
    assert result["collection_subprocess_index"]["required_tasks"] == [
        "collect-nav", "calculate-risk", "collect-period-returns"]
    upstream = audit._upstream_collection_truth(automation_payload={"scheduled_refresh_task": result},
        freshness_payload={"database_freshness": {"latest_nav_business_date": "2026-09-09"},
            "effective_nav_snapshot": {"candidate_date": "2026-09-09", "risk_1y_effective_end_date": "2026-09-09"},
            "risk_lag": {"coverage_sufficient": True}})
    assert upstream["blockers"] == [] and upstream["status"] == "watch"
    assert upstream["scheduler_task_name"] == audit.AUTOMATION_INTRADAY_REFRESH_TASK_NAME
    assert set(upstream["tasks"]) == {"collect-nav", "calculate-risk"}
    assert upstream["tasks"]["calculate-risk"]["business_summary"]["generated_period_count"] == 9
    assert upstream["tasks"]["collect-nav"]["business_summary"]["inserted_nav_record_count"] == 0


def test_daily_still_requires_fund_directory_even_with_identical_collection_profile(intraday):
    intraday.row["task_name"] = audit.AUTOMATION_SCHEDULED_REFRESH_TASK_NAME
    intraday.task_audit["task"]["task_name"] = intraday.row["task_name"]
    intraday.save()
    _, blockers, _ = check(intraday)
    assert "scheduled_refresh_collection_subprocess_required_tasks_missing:collect-funds" in blockers


def test_compressed_task_audit_preserves_truthful_degraded_terminal(intraday):
    import gzip

    original = Path(intraday.extra["task_run_audit_path"])
    compressed = original.with_suffix(".json.gz")
    intraday.extra["task_run_audit_path"] = str(compressed)
    intraday.task_audit["task_result"]["task_run_audit_path"] = str(compressed)
    intraday.save()
    compressed.write_bytes(gzip.compress(original.read_bytes(), mtime=0))
    result, blockers, warnings = check(intraday)
    assert not blockers
    assert result["collection_degradation"]["verified"] is True
    assert result["task_run_audit"]["audit_task_status"] == "degraded"
    assert "scheduled_refresh_collection_degraded:period_return_date_lag_quarantined" in warnings
    compressed.write_bytes(compressed.read_bytes()[:-3])
    _, blockers, _ = check(intraday)
    assert blockers


def test_successful_intraday_preserves_success_without_inventing_directory_collection(intraday):
    summary = intraday.period
    summary.update(status="skipped", cooldown_count=0, pending_count=0, cooldown_outcomes=[],
                   quarantined_date_lag_fcodes=[])
    source = {key: value for key, value in summary.items() if key not in {"response_artifact", "response_artifact_sha256"}}
    intraday.response.write_text(json.dumps({"summary": source, "observations": []}))
    summary["response_artifact_sha256"] = hashlib.sha256(intraday.response.read_bytes()).hexdigest()
    Path(intraday.index["items"][2]["stdout_path"]).write_text("api_source_evidence=" + json.dumps(summary))
    intraday.task_log.write_text("api_source_evidence=" + json.dumps(summary) + "\n"
                                f"[core-pipeline] subprocess_index={intraday.index_path} count=3 failed=0\n")
    intraday.row["status"] = "success"
    intraday.extra.update(status="success", degraded=False, degraded_stage=None, warning_reason=None)
    intraday.task_audit["task"]["status"] = "success"
    intraday.task_audit["task_result"] = deepcopy(intraday.extra)
    intraday.save()
    result, blockers, warnings = check(intraday)
    assert not blockers and not warnings
    assert result["row"]["status"] == "success"
    assert result["collection_degradation"] is None
    assert "collect-funds" not in result["collection_subprocess_index"]["task_names"]


@pytest.fixture
def compact_intraday(intraday):
    compact = compact_task_result_for_history_if_needed(
        deepcopy(intraday.task_audit["task_result"]), task_name=intraday.row["task_name"],
        task_info={"id": 1}, force=True, write_sidecar=False)
    assert "period_return_collection" not in compact
    intraday.extra.clear()
    intraday.extra.update(compact)
    intraday.save()
    return intraday


def test_compacted_sql_uses_its_exact_bound_full_result(compact_intraday):
    result, blockers, warnings = check(compact_intraday)
    assert not blockers
    proof = result["collection_degradation"]
    assert proof["sql_full_payload_verified"] is True
    assert proof["sql_inline_period_summary_present"] is False
    assert result["row"]["status"] == "degraded"
    assert "scheduled_refresh_collection_degraded:period_return_date_lag_quarantined" in warnings


@pytest.mark.parametrize("damage", ["sha", "length", "path", "binding", "schema", "flag", "result", "inline_null"])
def test_compacted_sql_cannot_borrow_an_unbound_or_changed_result(compact_intraday, damage):
    extra = compact_intraday.extra
    if damage == "sha":
        extra["task_history_full_payload"]["full_payload_sha256"] = "0" * 64
    elif damage == "length":
        extra["task_history_full_payload"]["full_payload_bytes"] += 1
    elif damage == "path":
        path = compact_intraday.response.parent / "other_audit.json"
        path.write_text(json.dumps(compact_intraday.task_audit))
        extra["task_history_full_payload"]["sidecar_project_path"] = str(path)
    elif damage == "binding":
        extra.pop("task_history_full_payload")
    elif damage == "schema":
        extra["task_history_extra_data_schema_version"] = 99
    elif damage == "flag":
        extra.pop("task_history_extra_data_compacted")
    elif damage == "result":
        compact_intraday.task_audit["task_result"]["fixture_changed"] = True
    elif damage == "inline_null":
        extra["period_return_collection"] = None
    compact_intraday.save()
    result, blockers, _ = check(compact_intraday)
    assert result["collection_degradation"] is None
    assert "scheduled_refresh_task_not_success" in blockers


@pytest.mark.parametrize("missing", ["collect-nav", "calculate-risk", "collect-period-returns"])
def test_intraday_missing_required_child_remains_blocking(intraday, missing):
    intraday.index["items"] = [item for item in intraday.index["items"] if item["task_name"] != missing]
    intraday.save()
    _, blockers, _ = check(intraday)
    assert any("required_tasks_missing" in value and missing in value for value in blockers)


@pytest.mark.parametrize("damage", [
    "source_hash", "source_counts", "source_blocking", "sql_summary", "audit_summary", "reason", "stage",
    "task_id", "task_name", "workflow_run", "workflow_failed", "result_run", "result_count", "result_trigger",
    "row_failed", "audit_success", "extra_success", "nav_timeout", "risk_exit", "risk_stdout", "index_missing",
])
def test_intraday_conflicting_evidence_and_actual_failures_still_block(intraday, damage):
    if damage == "source_hash":
        intraday.response.write_bytes(b"corrupt fixture source")
    elif damage == "source_counts":
        intraday.period["pending_count"] = 10
        Path(intraday.index["items"][2]["stdout_path"]).write_text("api_source_evidence=" + json.dumps(intraday.period))
    elif damage == "source_blocking":
        summary = intraday.period
        summary["cooldown_outcomes"] = [{"fcode": "fixture-error", "status": "error"}]
        summary.update(quarantined_date_lag_fcodes=[], blocking_count=1, screening_blocked=True)
        source = {key: value for key, value in summary.items() if key not in {"response_artifact", "response_artifact_sha256"}}
        intraday.response.write_text(json.dumps({"summary": source, "observations": []}))
        summary["response_artifact_sha256"] = hashlib.sha256(intraday.response.read_bytes()).hexdigest()
        Path(intraday.index["items"][2]["stdout_path"]).write_text("api_source_evidence=" + json.dumps(summary))
        intraday.task_audit["task_result"]["period_return_collection"] = deepcopy(summary)
    elif damage in {"sql_summary", "audit_summary"}:
        target = intraday.extra if damage == "sql_summary" else intraday.task_audit["task_result"]
        target["period_return_collection"] = {**target["period_return_collection"], "pending_count": 9}
    elif damage in {"reason", "stage"}:
        intraday.extra["warning_reason" if damage == "reason" else "degraded_stage"] = "unrelated_failure"
    elif damage in {"task_id", "task_name"}:
        intraday.task_audit["task"]["task_history_id" if damage == "task_id" else "task_name"] = "other-task"
    elif damage in {"workflow_run", "workflow_failed"}:
        intraday.task_audit["workflow_summary"]["run_id" if damage == "workflow_run" else "status"] = "other"
    elif damage in {"result_run", "result_count", "result_trigger"}:
        key = {"result_run": "screening_run_id", "result_count": "screening_final_count", "result_trigger": "screening_triggered"}[damage]
        intraday.task_audit["task_result"][key] = None
    elif damage == "row_failed":
        intraday.row["status"] = "failed"
    elif damage == "audit_success":
        intraday.task_audit["task"]["status"] = "success"
    elif damage == "extra_success":
        intraday.extra["status"] = "success"
    elif damage == "nav_timeout":
        intraday.index["items"][0]["timed_out"] = True
    elif damage == "risk_exit":
        intraday.index["items"][1]["return_code"] = 1
    elif damage == "risk_stdout":
        Path(intraday.index["items"][1]["stdout_path"]).write_text("fixture has no completion evidence")
    elif damage == "index_missing":
        intraday.task_log.write_text("fixture has no collection index")
    intraday.save()
    _, blockers, _ = check(intraday)
    assert blockers, damage


def test_intraday_zero_nav_increment_does_not_hide_stale_risk(intraday):
    result, blockers, _ = check(intraday)
    assert not blockers
    upstream = audit._upstream_collection_truth(automation_payload={"scheduled_refresh_task": result},
        freshness_payload={"database_freshness": {"latest_nav_business_date": "2026-09-09"},
            "effective_nav_snapshot": {"candidate_date": "2026-09-09", "risk_1y_effective_end_date": "2026-09-08"},
            "risk_lag": {"coverage_sufficient": False}})
    assert "collect_nav_zero_insert_without_fresh_snapshot_alignment" in upstream["blockers"]
    assert "risk_coverage_not_sufficient_for_collection_truth" in upstream["blockers"]
