"""Isolated SQLite write-failure drills; fixture values never reach production."""
from __future__ import annotations

import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]
from collectors.detail_collector import FundDetailCollector
from database.connection import get_db_manager
from database.models import Base, Fund
from backend import core_runner
from backend.collection_log_summary import classify_collection_log_severity
from backend.core_runner import CoreStepFailedError, StepResult, run_core_pipeline
from backend.scheduler import ProjectScheduler
from backend.scheduler_diagnostics import summarize_scheduler_run_for_api
from cli.aux_commands import cmd_collect_details
from scripts.audit_data_truthfulness import _stdout_result_semantics
from utils.detail_collection_summary import (
    build_detail_collection_summary, detail_collection_source_errors,
    parse_detail_collection_summary, validate_detail_collection_summary,
)


@pytest.fixture
def collector(tmp_path, monkeypatch):
    monkeypatch.setenv("DETAIL_EMPTY_REPORT_DIR", str(tmp_path / "empty_reports"))
    manager = get_db_manager(str(tmp_path / "synthetic_details.db"))
    Base.metadata.create_all(manager.engine)
    with manager.get_session() as session:
        session.add_all(Fund(fcode=f"{number:06d}", shortname="isolated outcome fixture")
                        for number in range(1, 5))

    class Client:
        def get(self, *args, **kwargs):
            raise AssertionError("network transport is not part of this test")

    instance = FundDetailCollector(config={"collection": {"detail": {"batch_size": 500}}},
                                   db_manager=manager, api_client=Client())
    instance._load_target_fcodes = lambda **kwargs: ["000001", "000002"]
    instance.api.get_fund_detail = lambda fcode: {"Success": True, "Datas": {"FCODE": fcode, "BENCH": "synthetic benchmark text"}}
    instance._fetch_stop_watch_payload = lambda fcode: None
    return instance


def persisted_count(collector):
    with collector.db_manager.get_session(readonly=True) as session:
        return session.query(Fund).filter(Fund.benchmark == "synthetic benchmark text").count()


def completion_block(stdout):
    match = re.search(r"任务完成统计\n.*?总任务数:\s*([\d,]+).*?成功:\s*([\d,]+).*?跳过:\s*([\d,]+).*?失败:\s*([\d,]+)",
                      stdout, flags=re.S)
    assert match, stdout
    total, succeeded, skipped, failed = (int(value.replace(",", "")) for value in match.groups())
    return {"total": total, "success": succeeded, "skipped": skipped, "failed": failed}


def test_completion_progress_waits_for_final_sqlite_commit(collector, capsys):
    with collector.db_manager.engine.begin() as conn:
        conn.exec_driver_sql("CREATE TRIGGER reject_fixture_details BEFORE UPDATE OF benchmark ON funds "
                             "BEGIN SELECT RAISE(ABORT, 'synthetic detail write rejection'); END")
    returned = collector.collect_all_details(workers=1)
    output = capsys.readouterr().out
    assert returned == persisted_count(collector) == 0
    assert collector.get_stats()["failed"] == 2
    assert completion_block(output) == {"total": 2, "success": 0, "skipped": 0, "failed": 2}


def test_persistence_preparation_failure_accounts_for_the_whole_pending_batch(collector, capsys):
    collector.default_batch_size = 2
    collector._load_target_fcodes = lambda **kwargs: ["000001", "000002", "000003", "000004"]
    original = collector._load_existing_detail_states
    calls = 0

    def unavailable_once(fcodes):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic persistence preparation failure")
        return original(fcodes)

    collector._load_existing_detail_states = unavailable_once
    returned = collector.collect_all_details(workers=1)
    output = capsys.readouterr().out
    assert returned == persisted_count(collector) == 2
    assert collector.get_stats()["failed"] == 2
    assert collector.get_stats()["skipped"] == 0
    assert collector.get_api_source_evidence()["save_failed_count"] == 2
    assert completion_block(output) == {"total": 4, "success": 2, "skipped": 0, "failed": 2}


def test_unavailable_response_is_not_a_write_failure_or_a_completed_write(collector, capsys):
    normal = collector.api.get_fund_detail
    collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": None} if code == "000002" else normal(code)
    assert collector.collect_all_details(workers=1) == persisted_count(collector) == 1
    output = capsys.readouterr().out
    evidence = collector.get_api_source_evidence()
    assert evidence["request_count"] == 2 and evidence["accepted_response_count"] == 1
    assert evidence["saved_fund_count"] == 1 and evidence["save_failed_count"] == 0
    assert completion_block(output) == {"total": 2, "success": 1, "skipped": 1, "failed": 0}


def test_zero_target_run_does_not_reuse_previous_counters(collector):
    assert collector.collect_all_details(workers=1) == 2
    collector._load_target_fcodes = lambda **kwargs: []
    assert collector.collect_all_details(workers=1) == 0
    assert collector.get_stats()["total"] == 0
    assert collector.get_stats()["success"] == 0
    evidence = collector.get_api_source_evidence()
    assert evidence["target_count"] == evidence["request_count"] == evidence["success_count"] == 0


def invoke_cli(collector, *, fcode=None):
    return cmd_collect_details(
        SimpleNamespace(workers=1, force=False, fcode=fcode), {}, logger=logging.getLogger(__name__),
        ensure_database_initialized=lambda config: "isolated.sqlite",
        emit_command_error=lambda message, exc: 1, collector_cls=lambda **kwargs: collector,
    )


def terminal_lines(collector):
    return ["[collect-details] $ isolated",
            "api_source_evidence=" + collector.get_api_source_evidence_json(),
            "detail_collection_summary=" + json.dumps(collector.detail_collection_summary)]


@pytest.mark.parametrize("scenario,expected", [
    ("success", (2, 0, 0, 0)), ("single", (1, 0, 0, 0)),
    ("unavailable", (1, 0, 1, 0)), ("all_unavailable", (0, 0, 2, 0)),
    ("no_targets", (0, 0, 0, 0)), ("write_rejected", (1, 1, 0, 2)),
    ("transport_error", (1, 1, 0, 2)),
])
def test_cli_terminal_matches_committed_sqlite_and_source_counts(collector, capsys, tmp_path, scenario, expected):
    normal = collector.api.get_fund_detail
    if scenario == "unavailable":
        collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": None} if code == "000002" else normal(code)
    elif scenario == "all_unavailable":
        collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": None}
    elif scenario == "no_targets":
        collector._load_target_fcodes = lambda **kwargs: []
    elif scenario == "write_rejected":
        with collector.db_manager.engine.begin() as conn:
            conn.exec_driver_sql("CREATE TRIGGER reject_one_detail BEFORE UPDATE OF benchmark ON funds "
                                 "WHEN OLD.fcode = '000002' BEGIN SELECT RAISE(ABORT, 'isolated rejection'); END")
    elif scenario == "transport_error":
        def request(code):
            if code == "000002":
                raise OSError("isolated transport exception")
            return normal(code)
        collector.api.get_fund_detail = request
    succeeded, failed, skipped, exit_code = expected
    assert invoke_cli(collector, fcode="000001" if scenario == "single" else None) == exit_code
    stdout = capsys.readouterr().out
    summary, errors = parse_detail_collection_summary(stdout.splitlines())
    assert errors == [] and summary == collector.detail_collection_summary
    assert summary["success_count"] == persisted_count(collector) == succeeded
    assert (summary["failed_count"], summary["skipped_count"]) == (failed, skipped)
    assert summary["target_count"] == succeeded + failed + skipped
    assert summary["success_basis"] == "committed_fund_detail_write"
    assert detail_collection_source_errors(summary, collector.get_api_source_evidence()) == []
    if summary["target_count"]:
        assert stdout.count("任务完成统计") == 1
        assert completion_block(stdout) == {"total": summary["target_count"], "success": succeeded, "failed": failed, "skipped": skipped}
    path = tmp_path / "detail.stdout.log"
    path.write_text(stdout)
    semantics, blockers, _ = _stdout_result_semantics("collect-details", str(path))
    assert semantics["business_summary"]["success_fund_count"] == succeeded
    if failed:
        assert blockers
    else:
        assert blockers == []
    classified = classify_collection_log_severity(stdout.splitlines())
    assert classified["detail_summary_success"] == succeeded
    assert classified["detail_summary_failed"] == failed
    assert classified["detail_summary_skipped"] == skipped
    assert classified["degraded"] is bool(failed)


def test_single_cli_keeps_stop_watch_metadata_without_detail_success(collector, capsys):
    collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": None}
    collector._fetch_stop_watch_payload = lambda code: {"FCODE": code, "FTYPE": "债券型-中短债", "FEATURE": "038"}
    assert invoke_cli(collector, fcode="000001") == 0
    summary, errors = parse_detail_collection_summary(capsys.readouterr().out.splitlines())
    assert errors == []
    assert (summary["success_count"], summary["unavailable_count"], summary["write_failed_count"]) == (0, 1, 0)
    evidence = collector.get_api_source_evidence()
    assert evidence["saved_fund_count"] == evidence["written_record_count"] == evidence["save_failed_count"] == 0
    with collector.db_manager.get_session(readonly=True) as session:
        fund = session.get(Fund, "000001")
        assert fund.ftype == "债券型-中短债" and fund.feature_code == "038"
        assert fund.benchmark is None and fund.cycle is None


@pytest.mark.parametrize("problem", ["missing_summary", "missing_return", "source_contradiction"])
def test_cli_invalid_terminal_exits_nonzero_without_success_marker(collector, capsys, problem):
    original = collector.collect_all_details

    def collection(**kwargs):
        count = original(**kwargs)
        if problem == "missing_summary":
            collector.detail_collection_summary = None
        if problem == "source_contradiction":
            collector.update_api_source_evidence_context(success_count=999)
        return None if problem == "missing_return" else count

    collector.collect_all_details = collection
    assert invoke_cli(collector) == 1
    stdout = capsys.readouterr().out
    assert "detail_collection_summary=" not in stdout
    assert "基金详情处理结束" not in stdout


@pytest.mark.parametrize("outcome,status,counts", [
    ("write_failure", "degraded", (2, 0, 2)),
    ("unavailable", "degraded", (2, 0, 0)),
    ("zero", "skipped", (0, 0, 0)),
])
def test_legacy_scheduler_uses_same_committed_outcomes(collector, outcome, status, counts):
    from schedulers.task_scheduler import TaskScheduler
    from database.models import TaskHistory
    if outcome == "write_failure":
        with collector.db_manager.engine.begin() as conn:
            conn.exec_driver_sql("CREATE TRIGGER reject_details BEFORE UPDATE OF benchmark ON funds "
                                 "BEGIN SELECT RAISE(ABORT, 'isolated rejected'); END")
    elif outcome == "unavailable":
        collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": None}
    else:
        collector._load_target_fcodes = lambda **kwargs: []
    # Constructor initializes the runtime loggers and task locks; importing
    # the scheduler alone must remain free of logging-file side effects.
    with patch("schedulers.task_scheduler.signal.signal"):
        scheduler = TaskScheduler(
            config={"scheduler": {"retry": {"max_attempts": 1, "initial_delay": 0, "max_delay": 0}}},
            db_manager=collector.db_manager, api_client=collector.api,
        )
    scheduler._get_worker_count = lambda *args, **kwargs: 1
    with (patch("schedulers.task_scheduler.FundDetailCollector", return_value=collector),
          patch.object(scheduler, "_execute_with_history", side_effect=lambda name, func, **kwargs: scheduler._execute_with_history_inner(name, func))):
        result = scheduler._weekly_detail_update()
    assert result["metadata"]["status"] == status
    with collector.db_manager.get_session(readonly=True) as session:
        row = session.query(TaskHistory).one()
        assert row.status == status
        assert (row.records_processed, row.records_updated, row.records_failed) == counts


def empty_summary():
    return build_detail_collection_summary(dict(target_count=0, fetch_ready_count=0, fetch_failed_count=0,
                                               unavailable_count=0, write_success_count=0, write_failed_count=0))


@pytest.mark.parametrize("value", [None, True, -1, "0", 0.0])
def test_missing_or_invalid_counts_and_returned_count_are_rejected(value):
    summary = empty_summary()
    with pytest.raises(ValueError, match="returned_count_mismatch"):
        validate_detail_collection_summary(summary, returned_count=value)
    summary["write_success_count"] = value
    with pytest.raises(ValueError, match="count_invalid"):
        validate_detail_collection_summary(summary)


@pytest.mark.parametrize("change", [{"target_count": 1}, {"fetch_ready_count": 1, "target_count": 1},
                                   {"status": "success"}, {"success_basis": "http_response"}])
def test_terminal_cannot_claim_unbalanced_or_unobserved_success(change):
    with pytest.raises(ValueError):
        validate_detail_collection_summary({**empty_summary(), **change})


@pytest.mark.parametrize("problem", ["invalid_json", "duplicate", "source_count", "source_task", "missing_field"])
def test_invalid_new_terminal_is_degraded_and_does_not_fall_back_to_progress(collector, capsys, tmp_path, problem):
    assert invoke_cli(collector) == 0
    stdout = capsys.readouterr().out
    lines = stdout.splitlines()
    terminal_index = next(i for i, line in enumerate(lines) if line.startswith("detail_collection_summary="))
    source_index = next(i for i, line in enumerate(lines) if line.startswith("api_source_evidence="))
    if problem == "invalid_json":
        lines[terminal_index] = "detail_collection_summary={"
    elif problem == "duplicate":
        lines.append(lines[terminal_index])
    elif problem == "missing_field":
        summary = dict(collector.detail_collection_summary)
        del summary["write_failed_count"]
        lines[terminal_index] = "detail_collection_summary=" + json.dumps(summary)
    else:
        source = collector.get_api_source_evidence()
        source["saved_fund_count" if problem == "source_count" else "task"] = 99 if problem == "source_count" else "collect-managers"
        lines[source_index] = "api_source_evidence=" + json.dumps(source)
    classified = classify_collection_log_severity(lines)
    assert classified["degraded"] and classified["detail_collection_summary_errors"]
    assert classified["detail_summary_success"] is None and classified["detail_summary_failed"] is None
    assert all(value is None for value in classified["summary_by_stage"]["collect-details"].values())
    path = tmp_path / "invalid.stdout.log"
    path.write_text("\n".join(lines))
    _, blockers, _ = _stdout_result_semantics("collect-details", str(path))
    assert blockers


def test_old_human_progress_and_terminal_logs_keep_legacy_meaning():
    logs = ["[collect-details] $ legacy", "总任务数: 1000", "成功: 988", "跳过: 0", "失败: 12"]
    classified = classify_collection_log_severity(logs)
    assert classified["detail_summary_total"] == 1000
    assert classified["detail_summary_success"] == 988 and classified["detail_summary_failed"] == 12
    assert "detail_collection_summary" not in classified and not classified["degraded"]


def weekly_scheduler():
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(scheduler_weekly_nav_history_limit=240,
                                         scheduler_weekly_long_term_history_limit=20, collect_companies=False)
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None
    return scheduler


@pytest.mark.parametrize("later_company_failure,invalid_terminal", [(False, False), (True, False), (False, True)])
def test_weekly_preserves_detail_evidence_after_log_tail_and_blocks_both_rescreens(collector, later_company_failure, invalid_terminal):
    with collector.db_manager.engine.begin() as conn:
        conn.exec_driver_sql("CREATE TRIGGER reject_detail BEFORE UPDATE OF benchmark ON funds "
                             "BEGIN SELECT RAISE(ABORT, 'isolated rejected'); END")
    assert collector.collect_all_details(workers=1) == 0
    lines = terminal_lines(collector)
    if invalid_terminal:
        lines[-1] = "detail_collection_summary={"
    scheduler = weekly_scheduler()

    def pipeline(settings, options, log):
        for line in lines:
            log(line)
        for i in range(260):
            log(f"independent collection progress {i}")
        if invalid_terminal:
            return  # A defective caller cannot hide bad structured evidence with exit 0.
        failure = CoreStepFailedError(stage="collect-details", return_code=2, meta_path="isolated.meta.json")
        if later_company_failure:
            next_failure = CoreStepFailedError(stage="collect-companies", return_code=2, meta_path="isolated.company.meta.json")
            next_failure.deferred_failures.append(failure.as_dict())
            raise next_failure
        raise failure

    with (patch("backend.scheduler.run_project_pipeline", side_effect=pipeline),
          patch("backend.scheduler.run_bond_universe_sync", return_value={"diffs_after_sync": {}}) as governance,
          patch("backend.scheduler.run_nav_history_maintenance", return_value={}),
          patch("backend.scheduler.run_long_term_history_maintenance", return_value={}),
          patch.object(scheduler, "_build_empty_detail_payload_audit", return_value={}),
          patch.object(scheduler, "_build_maintenance_data_change_summary", return_value={"changed": True}),
          patch.object(scheduler, "_rescreen_report_after_maintenance") as rescreen,
          patch.object(scheduler, "_rescreen_report_after_risk_parameter_change") as risk_rescreen,
          patch.object(scheduler, "_attempt_refresh_risk_free_rate") as update_rate,
          patch.object(scheduler, "_build_risk_free_rate_drift_audit", return_value={})):
        result = scheduler._execute_weekly_maintenance()
    governance.assert_called_once()
    rescreen.assert_not_called()
    risk_rescreen.assert_not_called()
    update_rate.assert_not_called()
    assert result["status"] == "degraded" and result["collection_degraded"]
    assert result["collection_degraded_stage"] == "collect-details"
    assert result["collection_rescreen_blocked_reason"] == "detail_collection_incomplete"
    assert result["maintenance_rescreen_skipped_reason"] == "detail_collection_incomplete"
    assert result["detail_records_succeeded"] == (None if invalid_terminal else 0)
    assert result["detail_records_failed"] == (None if invalid_terminal else 2)
    if not invalid_terminal:
        assert result["detail_collection_summary"] == collector.detail_collection_summary


@pytest.mark.parametrize("invalid", [False, True])
def test_detail_terminal_survives_history_compaction_sql_state_and_api(collector, tmp_path, invalid):
    from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
    from backend.task_runtime import SqliteTaskRecorder
    from backend.scheduler_state import SchedulerStateStore
    assert collector.collect_all_details(workers=1) == 2
    fields = {"detail_collection_summary": None if invalid else collector.detail_collection_summary,
              "detail_collection_summary_errors": ["isolated invalid"] if invalid else [],
              "detail_records_processed": None if invalid else 2,
              "detail_records_succeeded": None if invalid else 2,
              "detail_records_failed": None if invalid else 0,
              "detail_records_skipped": None if invalid else 0}
    payload = {"task_name": "scheduled_weekly_bond_maintenance", "status": "degraded" if invalid else "success",
               "logs_tail": ["isolated logs " * 30000], **fields}
    compact = compact_task_result_for_history_if_needed(payload, reports_dir=tmp_path, fail_open=False)
    assert compact["task_history_extra_data_compacted"] is True
    recorder = SqliteTaskRecorder(tmp_path / "history.sqlite")
    task = recorder.start_task(payload["task_name"])
    recorder.finish_task(task, status=payload["status"], extra_data=compact)
    with sqlite3.connect(recorder.db_path) as conn:
        sql_payload = json.loads(conn.execute("SELECT extra_data FROM task_history WHERE id=?", (task["id"],)).fetchone()[0])
    # JSON state persistence and SQL both preserve null; API must not default it to zero.
    state_path = tmp_path / "state.json"
    state_store = SchedulerStateStore(state_path)
    state_store.finish_task(payload["task_name"], compact)
    state = state_store.snapshot()["last_runs"][payload["task_name"]]
    for layer in (compact, sql_payload, state, summarize_scheduler_run_for_api(state, project_root=tmp_path)):
        assert {key: layer[key] for key in fields} == fields


def pipeline_settings(tmp_path):
    return SimpleNamespace(project_root=tmp_path, core_root=ROOT / "core", core_output_dir=tmp_path / "output",
                           reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs",
                           database_path=tmp_path / "unused.sqlite", tiantian_base_url="http://unused.invalid",
                           workflow_task_timeout_seconds={}, workflow_task_idle_timeout_seconds={})


@pytest.mark.parametrize("screening,company_exit", [(True, 0), (False, 0), (True, 2)])
def test_actual_child_processes_continue_independent_work_and_never_screen_after_detail_failure(tmp_path, screening, company_exit):
    names = ["collect-details", "collect-nav", "calculate-risk", "collect-companies"]
    if screening:
        names.append("daily-screening-master")
    steps = [StepResult(name, [sys.executable, "-c", "import sys; print(sys.argv[1]); sys.exit(int(sys.argv[2]))",
                             name, str(2 if name == "collect-details" else company_exit if name == "collect-companies" else 0)]) for name in names]
    settings = pipeline_settings(tmp_path)
    with (patch.object(core_runner, "build_core_commands", return_value=steps),
          patch.object(core_runner, "ensure_core_output_link"),
          patch.object(core_runner, "_ensure_tiantian_business_ready_for_stage"),
          patch.object(core_runner, "discover_core_artifacts") as discover):
        with pytest.raises(CoreStepFailedError) as failure:
            run_core_pipeline(settings, {"workflow_run_id": "isolated_details"}, lambda message: None)
    assert failure.value.stage == ("collect-companies" if company_exit else "collect-details")
    if company_exit:
        assert failure.value.deferred_failures[0]["stage"] == "collect-details"
    discover.assert_not_called()
    index = json.loads((settings.runs_dir / "isolated_details/logs/subprocess/subprocess_index.json").read_text())
    records = {item["task_name"]: item for item in index["items"]}
    assert set(records) == {"collect-details", "collect-nav", "calculate-risk", "collect-companies"}
    assert records["collect-details"]["return_code"] == 2
    assert records["collect-nav"]["success"] and records["calculate-risk"]["success"]


@pytest.mark.parametrize("code,timed_out", [(1, False), (2, True), (124, True)])
def test_unknown_detail_failures_and_timeouts_still_stop_immediately(tmp_path, code, timed_out):
    steps = [StepResult(name, []) for name in ("collect-details", "collect-nav")]
    error = CoreStepFailedError(stage="collect-details", return_code=code, timed_out=timed_out, meta_path="isolated")
    with (patch.object(core_runner, "build_core_commands", return_value=steps),
          patch.object(core_runner, "ensure_core_output_link"),
          patch.object(core_runner, "_ensure_tiantian_business_ready_for_stage"),
          patch.object(core_runner, "_run_streaming_subprocess", side_effect=error) as run):
        with pytest.raises(CoreStepFailedError):
            run_core_pipeline(pipeline_settings(tmp_path), {}, lambda message: None)
    assert run.call_count == 1
