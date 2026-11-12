"""Real terminal-log replay plus isolated faults; never touch live API or business DB."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import json
import logging
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend import core_runner
from backend.collection_log_summary import classify_collection_log_severity
from backend.config import load_settings
from backend.core_runner import CoreStepFailedError, StepResult, build_core_commands, run_core_pipeline
from backend.scheduler import ProjectScheduler
from cli.aux_commands import cmd_collect_managers
from collectors.manager_collector import ManagerCollector
from database.connection import DatabaseManager
from database.models import TaskHistory
from scripts.audit_data_truthfulness import _stdout_result_semantics
from utils.manager_collection_summary import build_manager_collection_summary

REAL_LOG = json.loads((ROOT / "tests/fixtures/tiantian_manager_terminal_20260906.json").read_text())
REAL_EVIDENCE = REAL_LOG["api_source_evidence"]


def real_stages():
    listing, details = REAL_EVIDENCE
    assert details["target_count"] == details["request_count"] == 0
    return {
        "fund_manager_list": {"total_funds": listing["target_count"], "success_funds": listing["success_count"],
                              "failed_funds": listing["failed_count"], "empty_funds": listing["empty_count"]},
        "manager_details": {"requested_manager_count": 0, "updated_manager_count": 0,
                            "failed_manager_count": 0, "skipped_manager_count": 0},
    }


class TerminalReplay(ManagerCollector):
    """Keep actual aggregate construction; replace only the two IO stages."""

    def __init__(self, stages=None):
        self.default_workers = 5
        self.stages = deepcopy(real_stages() if stages is None else stages)
        self.last_run_summary = None

    def _collect_fund_managers(self, **kwargs):
        return deepcopy(self.stages["fund_manager_list"])

    def _collect_manager_details(self, *args):
        return deepcopy(self.stages["manager_details"])

    def print_stats(self):
        pass

    def get_manager_list_api_source_evidence_json(self):
        return json.dumps(REAL_EVIDENCE[0])

    def get_manager_detail_api_source_evidence_json(self):
        return json.dumps(REAL_EVIDENCE[1])


def invoke_cli(collector):
    with patch("utils.task_reporter.update_task_history"):
        return cmd_collect_managers(
            SimpleNamespace(workers=5, force=False), {}, logger=logging.getLogger(__name__),
            ensure_database_initialized=lambda config: "isolated_database_not_opened",
            emit_command_error=lambda message, exc: 1, collector_cls=lambda **kwargs: collector,
        )


def real_terminal_lines():
    return [*["api_source_evidence=" + json.dumps(item) for item in REAL_EVIDENCE],
            "manager_collection_summary=" + json.dumps(build_manager_collection_summary(real_stages()))]


def test_real_partial_failure_cli_uses_both_stages_and_preserves_source_evidence(tmp_path, capsys):
    assert invoke_cli(TerminalReplay()) == 2
    stdout = capsys.readouterr().out
    assert "✅ 基金经理采集完成" not in stdout
    assert stdout.count("api_source_evidence=") == 2
    path = tmp_path / "manager.stdout.log"
    path.write_text(stdout)
    semantics, blockers, warnings = _stdout_result_semantics("collect-managers", str(path))
    assert blockers == []
    summary = semantics["business_summary"]
    assert (summary["target_count"], summary["success_count"], summary["failed_count"]) == (1500, 1471, 29)
    assert summary["status"] == "degraded"
    assert "collect_managers_terminal_degraded:manager_collection_failed" in warnings
    classified = classify_collection_log_severity(stdout.splitlines())
    assert classified["summary_failed"] == 29 and classified["degraded"]


@pytest.mark.parametrize("list_counts,detail_counts,exit_code,status", [
    ((0, 0, 0, 0), (0, 0, 0, 0), 0, "skipped"),
    ((0, 0, 0, 0), (3, 0, 0, 3), 0, "skipped"),
    ((2, 2, 0, 0), (3, 3, 0, 0), 0, "success"),
    ((2, 2, 0, 1), (0, 0, 0, 0), 2, "degraded"),
    ((2, 2, 0, 0), (3, 2, 1, 0), 2, "degraded"),
])
def test_isolated_terminal_boundaries(list_counts, detail_counts, exit_code, status, capsys):
    stages = {
        "fund_manager_list": dict(zip(("total_funds", "success_funds", "failed_funds", "empty_funds"), list_counts)),
        "manager_details": dict(zip(("requested_manager_count", "updated_manager_count", "failed_manager_count", "skipped_manager_count"), detail_counts)),
    }
    collector = TerminalReplay(stages)
    assert invoke_cli(collector) == exit_code
    assert collector.last_run_summary["status"] == status
    stdout = capsys.readouterr().out
    assert ("✅ 基金经理采集完成" in stdout) == (status == "success")


@pytest.mark.parametrize("fault", ["missing", "missing_stage", "missing_field", "bool", "float", "negative", "unbalanced", "bad_status", "returned_none", "returned_mismatch"])
def test_bad_or_missing_terminal_summary_never_succeeds(fault, capsys):
    summary = build_manager_collection_summary(real_stages())
    returned = 0
    if fault == "missing":
        summary = None
    elif fault == "missing_stage":
        summary.pop("manager_details")
    elif fault == "missing_field":
        summary["fund_manager_list"].pop("failed_funds")
    elif fault in {"bool", "float", "negative", "unbalanced"}:
        summary["fund_manager_list"]["failed_funds"] = {"bool": True, "float": 29.0, "negative": -1, "unbalanced": 0}[fault]
    elif fault == "bad_status":
        summary["status"] = "success"
    else:
        returned = None if fault == "returned_none" else 1
    collector = SimpleNamespace(last_run_summary=summary, collect_all_managers=lambda **kwargs: returned)
    assert invoke_cli(collector) == 1
    assert "✅ 基金经理采集完成" not in capsys.readouterr().out


@pytest.mark.parametrize("fault", ["duplicate", "malformed", "missing_source", "bad_count", "bad_type"])
def test_new_audit_contract_rejects_invalid_summary_or_source_even_with_old_success_text(tmp_path, fault):
    lines = real_terminal_lines()
    if fault == "duplicate":
        lines.append(lines[-1])
    elif fault == "malformed":
        lines[-1] = "manager_collection_summary={"
    elif fault == "missing_source":
        lines.pop(0)
    else:
        entry = deepcopy(REAL_EVIDENCE[0])
        entry["failed_count"] = 0 if fault == "bad_count" else "29"
        lines[0] = "api_source_evidence=" + json.dumps(entry)
    lines.append("✅ 基金经理采集完成: 0位经理")
    path = tmp_path / "invalid.stdout.log"
    path.write_text("\n".join(lines))
    _, blockers, _ = _stdout_result_semantics("collect-managers", str(path))
    assert blockers
    classification = classify_collection_log_severity(lines)
    assert classification["degraded"] and classification["reason"] == "manager_collection_summary_invalid"


def test_old_immutable_log_contract_remains_compatible(tmp_path):
    path = tmp_path / "old.stdout.log"
    path.write_text("\n".join([*REAL_LOG["completion_stats_text"], *real_terminal_lines()[:-1], "✅ 基金经理采集完成: 0位经理"]))
    semantics, blockers, _ = _stdout_result_semantics("collect-managers", str(path))
    assert blockers == []
    assert semantics["business_summary"] == {"success_manager_count": 0}
    classified = classify_collection_log_severity(path.read_text().splitlines())
    assert classified["summary_failed"] == 29
    assert "manager_collection_summary" not in classified


def test_recovered_http_failure_is_not_a_terminal_business_failure(tmp_path):
    # Isolated retry fixture: two HTTP attempts, one final saved detail.
    stages = {"fund_manager_list": {"total_funds": 0, "success_funds": 0, "failed_funds": 0, "empty_funds": 0},
              "manager_details": {"requested_manager_count": 1, "updated_manager_count": 1,
                                  "failed_manager_count": 0, "skipped_manager_count": 0}}
    listing = deepcopy(REAL_EVIDENCE[1])
    listing.update(source="fundMNMangerList", target_count=0, request_count=0, success_count=0, failed_count=0, empty_count=0)
    detail = deepcopy(REAL_EVIDENCE[1])
    detail.update(target_count=1, request_count=2, response_count=2, accepted_response_count=1,
                  failed_response_count=1, has_data_response_count=1, empty_response_count=1,
                  returned_record_count=1, saved_fund_count=1, written_record_count=1,
                  success_count=1, failed_count=0, skipped_count=0,
                  quality_status_counts={"invalid_datas_type": 1, "ok": 1},
                  quality_payload_type_counts={"NoneType": 1, "dict": 1})
    lines = ["获取经理isolated详情失败，准备重试(1/3): Datas 类型异常: NoneType",
             "api_source_evidence=" + json.dumps(listing), "api_source_evidence=" + json.dumps(detail),
             "manager_collection_summary=" + json.dumps(build_manager_collection_summary(stages))]
    classified = classify_collection_log_severity(lines)
    assert not classified["degraded"] and classified["summary_success"] == 1
    assert classified["manager_retry_count"] == 1
    path = tmp_path / "recovered.stdout.log"
    path.write_text("\n".join(lines))
    semantics, blockers, _ = _stdout_result_semantics("collect-managers", str(path))
    assert blockers == [] and semantics["business_summary"]["status"] == "success"


@pytest.mark.parametrize("profile", ["full", "weekly", "daily"])
@pytest.mark.parametrize("force", [False, True])
def test_collection_and_screening_plan_manager_once_and_honor_force(tmp_path, profile, force):
    settings = replace(load_settings(), database_path=tmp_path / "unused.sqlite")
    with patch.object(core_runner, "should_collect_managers", return_value=True), patch.object(core_runner, "should_refresh_holding_period", return_value=False):
        steps = build_core_commands(settings, {"run_collection": True, "run_screening": True,
                                               "collection_profile": profile, "force_collect_managers": force})
    managers = [step for step in steps if step.name == "collect-managers"]
    assert len(managers) == 1
    assert ("--force" in managers[0].command) is force


def pipeline_settings(tmp_path):
    return SimpleNamespace(project_root=tmp_path, core_root=ROOT / "core", core_output_dir=tmp_path / "output",
                           reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs",
                           database_path=tmp_path / "unused.sqlite", tiantian_base_url="http://unused.invalid",
                           workflow_task_timeout_seconds={}, workflow_task_idle_timeout_seconds={})


@pytest.mark.parametrize("screening,company_exit", [(True, 0), (False, 0), (True, 2)])
def test_real_child_processes_continue_independent_stages_and_never_screen(tmp_path, screening, company_exit):
    runner = tmp_path / "isolated_child.py"
    runner.write_text("import sys\nfrom pathlib import Path\nprint(Path(sys.argv[1]).read_text(), flush=True)\nraise SystemExit(int(sys.argv[2]))\n")
    names = ["collect-managers", "collect-nav", "calculate-risk", "collect-companies"]
    if screening:
        names.append("daily-screening-master")
    steps = []
    for name in names:
        payload = tmp_path / f"{name}.txt"
        payload.write_text("\n".join(real_terminal_lines()) if name == "collect-managers" else f"{name} executed")
        exit_code = 2 if name == "collect-managers" else company_exit if name == "collect-companies" else 0
        steps.append(StepResult(name, [sys.executable, str(runner), str(payload), str(exit_code)]))
    settings = pipeline_settings(tmp_path)
    with (patch.object(core_runner, "build_core_commands", return_value=steps),
          patch.object(core_runner, "ensure_core_output_link"),
          patch.object(core_runner, "_ensure_tiantian_business_ready_for_stage"),
          patch.object(core_runner, "discover_core_artifacts") as discover):
        with pytest.raises(CoreStepFailedError) as error:
            run_core_pipeline(settings, {"workflow_run_id": "isolated_manager_terminal"}, lambda message: None)
    assert error.value.stage == ("collect-companies" if company_exit else "collect-managers")
    if company_exit:
        assert error.value.deferred_failures[0]["stage"] == "collect-managers"
    discover.assert_not_called()
    index = json.loads((settings.runs_dir / "isolated_manager_terminal/logs/subprocess/subprocess_index.json").read_text())
    by_name = {item["task_name"]: item for item in index["items"]}
    assert set(by_name) == {"collect-managers", "collect-nav", "calculate-risk", "collect-companies"}
    assert by_name["collect-managers"]["return_code"] == 2
    assert by_name["collect-nav"]["success"] and by_name["calculate-risk"]["success"]
    assert by_name["collect-companies"]["return_code"] == company_exit


@pytest.mark.parametrize("code,timed_out", [(1, False), (2, True), (124, True)])
def test_unknown_manager_errors_and_timeouts_still_stop_immediately(tmp_path, code, timed_out):
    steps = [StepResult(name, []) for name in ("collect-managers", "collect-nav")]
    failure = CoreStepFailedError(stage="collect-managers", return_code=code, timed_out=timed_out, meta_path="isolated")
    with (patch.object(core_runner, "build_core_commands", return_value=steps),
          patch.object(core_runner, "ensure_core_output_link"),
          patch.object(core_runner, "_ensure_tiantian_business_ready_for_stage"),
          patch.object(core_runner, "_run_streaming_subprocess", side_effect=failure) as run):
        with pytest.raises(CoreStepFailedError):
            run_core_pipeline(pipeline_settings(tmp_path), {}, lambda message: None)
    assert run.call_count == 1


def weekly_scheduler():
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(scheduler_weekly_nav_history_limit=240, scheduler_weekly_long_term_history_limit=20,
                                         collect_companies=True)
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None
    return scheduler


@pytest.mark.parametrize("company_also_failed", [False, True])
def test_weekly_retains_early_manager_evidence_after_long_logs_and_blocks_both_rescreens(company_also_failed):
    scheduler = weekly_scheduler()

    def partial(settings, options, log):
        log("[collect-managers] $ isolated")
        for line in real_terminal_lines():
            log(line)
        for i in range(260):
            log(f"independent maintenance progress {i}")
        failure = CoreStepFailedError(stage="collect-managers", return_code=2, meta_path="isolated.meta.json")
        if company_also_failed:
            company_failure = CoreStepFailedError(stage="collect-companies", return_code=2, meta_path="isolated.company.meta.json")
            company_failure.deferred_failures.append(failure.as_dict())
            raise company_failure
        raise failure

    with (patch("backend.scheduler.run_project_pipeline", side_effect=partial),
          patch("backend.scheduler.run_bond_universe_sync", return_value={"diffs_after_sync": {}}) as governance,
          patch("backend.scheduler.run_nav_history_maintenance", return_value={}),
          patch("backend.scheduler.run_long_term_history_maintenance", return_value={}),
          patch.object(scheduler, "_build_maintenance_data_change_summary", return_value={"changed": True}),
          patch.object(scheduler, "_rescreen_report_after_maintenance") as rescreen,
          patch.object(scheduler, "_rescreen_report_after_risk_parameter_change") as risk_rescreen,
          patch.object(scheduler, "_attempt_refresh_risk_free_rate") as update_rate,
          patch.object(scheduler, "_build_risk_free_rate_drift_audit", return_value={}) as drift):
        result = scheduler._execute_weekly_maintenance()
    governance.assert_called_once()
    drift.assert_called_once()
    rescreen.assert_not_called()
    risk_rescreen.assert_not_called()
    update_rate.assert_not_called()
    assert result["status"] == "degraded" and result["warning_reason"] == "manager_collection_failed"
    assert (result["records_processed"], result["records_succeeded"], result["records_failed"]) == (1500, 1471, 29)
    assert result["manager_collection_summary"]["status"] == "degraded"
    assert result["collection_failures"][0]["return_code"] == 2
    assert result["collection_degraded_stage"] == "collect-managers"
    assert len(result["collection_failures"]) == (2 if company_also_failed else 1)
    assert result["collection_rescreen_blocked_reason"] == "manager_collection_incomplete"
    assert result["maintenance_rescreen_skipped_reason"] == "manager_collection_incomplete"
    assert result["screening_triggered"] is False


def test_weekly_company_only_failure_does_not_block_rescreen_or_claim_network_preflight():
    scheduler = weekly_scheduler()
    failure = CoreStepFailedError(stage="collect-companies", return_code=2, meta_path="isolated")
    with (patch("backend.scheduler.run_project_pipeline", side_effect=failure),
          patch("backend.scheduler.run_bond_universe_sync", return_value={"diffs_after_sync": {}}),
          patch("backend.scheduler.run_nav_history_maintenance", return_value={}),
          patch("backend.scheduler.run_long_term_history_maintenance", return_value={}),
          patch.object(scheduler, "_build_maintenance_data_change_summary", return_value={"changed": True}),
          patch.object(scheduler, "_rescreen_report_after_maintenance", return_value={"status": "success", "run_id": "isolated"}) as rescreen,
          patch.object(scheduler, "_attempt_refresh_risk_free_rate", return_value={"succeeded": True}) as update_rate,
          patch.object(scheduler, "_build_risk_free_rate_drift_audit", return_value={})):
        result = scheduler._execute_weekly_maintenance()
    rescreen.assert_called_once()
    update_rate.assert_called_once()
    assert result["warning_reason"] == "company_collection_failed"
    assert result["tiantian_business_preflight"] is None
    assert result["collection_rescreen_blocked_reason"] is None and result["screening_triggered"]


def test_large_task_compaction_keeps_terminal_counts_and_deferred_failures(tmp_path):
    from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed
    summary = build_manager_collection_summary(real_stages())
    fields = {
        "manager_collection_summary": summary,
        "manager_collection_summary_errors": [],
        "collection_enabled": True,
        "collection_error": "isolated partial collection",
        "collection_degraded": True,
        "collection_degraded_stage": "collect-managers",
        "collection_warning_reason": "manager_collection_failed",
        "collection_failures": [CoreStepFailedError(stage="collect-managers", return_code=2, meta_path="isolated.meta.json").as_dict()],
        "collection_rescreen_blocked_reason": "manager_collection_incomplete",
    }
    payload = {"task_name": "scheduled_weekly_bond_maintenance", "status": "degraded",
               "logs_tail": ["isolated large log " * 20000], **fields}
    compact = compact_task_result_for_history_if_needed(payload, reports_dir=tmp_path, fail_open=False)
    assert compact["task_history_extra_data_compacted"] is True
    assert {field: compact[field] for field in fields} == fields
    from backend.task_audit_storage import read_task_audit
    sidecar = read_task_audit(compact["task_history_full_payload"]["sidecar_path"])
    assert sidecar["full_payload"]["manager_collection_summary"] == summary


def test_task_reporter_and_legacy_scheduler_persist_degraded_terminal(tmp_path, monkeypatch):
    from schedulers.task_scheduler import TaskScheduler
    from utils.task_reporter import update_task_history
    db = DatabaseManager(str(tmp_path / "history.sqlite"))
    TaskHistory.__table__.create(db.engine)
    with db.get_session() as session:
        session.add(TaskHistory(task_name="isolated manager CLI", status="running", started_at=datetime.now()))
    monkeypatch.setenv("TASK_HISTORY_ID", "1")
    with patch("database.DatabaseManager", return_value=db):
        update_task_history(1471, 1500, failed_count=29, status_override="degraded")
    monkeypatch.delenv("TASK_HISTORY_ID")
    with patch("schedulers.task_scheduler.signal.signal"):
        scheduler = TaskScheduler(
            config={"scheduler": {"retry": {"max_attempts": 1, "initial_delay": 0, "max_delay": 0}}},
            db_manager=db,
            api_client=SimpleNamespace(),
        )
    scheduler._get_worker_count = lambda *args, **kwargs: 5
    with patch("schedulers.task_scheduler.ManagerCollector", return_value=TerminalReplay()), \
         patch.object(scheduler, "_execute_with_history", side_effect=lambda name, func, **kwargs: scheduler._execute_with_history_inner(name, func)):
        result = scheduler._weekly_manager_update()
    assert result["metadata"]["status"] == "degraded"
    with db.get_session(readonly=True) as session:
        rows = session.query(TaskHistory).all()
        assert len(rows) == 2
        for row in rows:
            assert (row.status, row.records_processed, row.records_updated, row.records_failed) == ("degraded", 1500, 1471, 29)
    db.engine.dispose()


def test_modern_recorder_writes_final_counts_with_status_and_preserves_unknown(tmp_path):
    from backend.task_runtime import SqliteTaskRecorder
    recorder = SqliteTaskRecorder(tmp_path / "scheduler.sqlite")
    task = recorder.start_task("isolated manager terminal")

    def row():
        with sqlite3.connect(recorder.db_path) as connection:
            return connection.execute("SELECT status,records_processed,records_inserted,records_updated,records_failed FROM task_history WHERE id=?", (task["id"],)).fetchone()

    assert row() == ("running", None, None, None, None)
    recorder.finish_task(task, status="degraded", extra_data={
        "records_processed": 1500, "records_succeeded": 1471, "records_failed": 29,
    })
    assert row() == ("degraded", 1500, None, 1471, 29)
    recorder.finish_task(task, status="skipped", extra_data={"records_processed": 0, "records_updated": 0, "records_inserted": 0, "records_failed": 0})
    assert row() == ("skipped", 0, 0, 0, 0)
    recorder.finish_task(task, status="success", extra_data={})
    assert row() == ("success", None, None, None, None)


@pytest.mark.parametrize("invalid", [True, "29", 29.0, -1])
def test_invalid_physical_task_counts_cannot_be_persisted_as_success(tmp_path, invalid):
    from backend.task_runtime import SqliteTaskRecorder
    recorder = SqliteTaskRecorder(tmp_path / "scheduler.sqlite")
    task = recorder.start_task("isolated invalid count")
    with pytest.raises(ValueError, match="task_history_count_invalid"):
        recorder.finish_task(task, status="success", extra_data={"records_processed": invalid})
    with sqlite3.connect(recorder.db_path) as connection:
        row = connection.execute("SELECT status,records_processed FROM task_history WHERE id=?", (task["id"],)).fetchone()
    assert row == ("running", None)
