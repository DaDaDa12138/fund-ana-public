"""Company-profile contracts; real responses plus clearly isolated fault fixtures."""
from __future__ import annotations

from copy import deepcopy
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta
import json
from io import StringIO
import logging
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from backend.collection_log_summary import classify_collection_log_severity
from backend.config import load_settings
from backend.core_runner import CoreStepFailedError, build_core_commands, _run_streaming_subprocess
from collectors.company_collector import CompanyCollector
from cli.aux_commands import cmd_collect_companies
from database.connection import DatabaseManager
from database.models import CollectionLog, FundCompany, FundManager
from scripts.audit_data_truthfulness import _stdout_result_semantics

PROFILES = json.loads((ROOT / "tests/fixtures/tiantian_company_profiles_20260908.json").read_text())["profiles"]


class ReplayClient:
    def __init__(self, profiles=None):
        self.profiles = deepcopy(PROFILES if profiles is None else profiles)
        self.calls = []

    def get(self, endpoint, params):
        assert endpoint == "/companyApi2"
        assert params["action"] == "companyarchives"
        self.calls.append(dict(params))
        value = self.profiles[params["cc"]]
        if isinstance(value, Exception):
            raise value
        return deepcopy(value)


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(str(tmp_path / "company.sqlite"))
    for model in (FundManager, FundCompany, CollectionLog):
        model.__table__.create(manager.engine)
    with manager.get_session() as session:
        for company_id in PROFILES:
            # Membership rows are test scaffolding, not observed manager data.
            session.add(FundManager(mgr_id=f"test-{company_id}", name="fixture manager",
                                    company_id=company_id))
        session.add(FundManager(mgr_id="test-missing", name="fixture manager", company_id="--"))
    yield manager
    manager.engine.dispose()


def collector_for(db, client=None, **options):
    return CompanyCollector(config={"collection": {"company": options}}, db_manager=db,
                            api_client=client or ReplayClient())


def run_cli(collector):
    return cmd_collect_companies(
        SimpleNamespace(workers=1, force=False), {}, logger=logging.getLogger(__name__),
        ensure_database_initialized=lambda config: collector.db_manager.db_path,
        emit_command_error=lambda *args: 1, collector_cls=lambda **kwargs: collector,
    )


def test_real_responses_insert_required_name_before_commit_and_keep_units(db):
    client = ReplayClient()
    collector = collector_for(db, client)
    assert collector.collect_all_companies(workers=2) == 3
    with db.get_session(readonly=True) as session:
        rows = {row.company_id: row for row in session.query(FundCompany).all()}
        assert set(rows) == set(PROFILES)
        for cid, row in rows.items():
            raw = PROFILES[cid]["data"]
            assert row.company_name == row.full_name == raw["FDMC"]
            assert row.total_scale == float(raw["GLGM"].removesuffix("亿元"))
            assert row.fund_count == int(raw["Count"])
            assert row.manager_count == int(raw["ManagerCount"])
            assert row.establish_date.isoformat() == raw["CLRQ"]
        assert rows["80053708"].registered_capital is None  # actual upstream "---"
    summary = collector.last_run_summary
    assert summary["inserted_count"] == 3 and summary["updated_count"] == 0
    assert summary["failed_count"] == summary["skipped_count"] == 0
    assert summary["invalid_company_id_samples"] == ["--"]
    assert len(client.calls) == 3 and "--" not in {call["cc"] for call in client.calls}


def test_fresh_rows_skip_network_and_stale_rows_refresh_without_force(db):
    collector = collector_for(db)
    assert collector.collect_all_companies() == 3
    second = ReplayClient()
    collector = collector_for(db, second)
    assert collector.collect_all_companies() == 0
    assert collector.last_run_summary["status"] == "skipped"
    assert collector.last_run_summary["fresh_skipped_count"] == 3
    assert second.calls == []
    with db.get_session() as session:
        session.query(FundCompany).filter_by(company_id="80053708").one().last_updated_at = datetime.now() - timedelta(days=29)
    assert collector.collect_all_companies() == 1
    assert second.calls == [{"cc": "80053708", "action": "companyarchives"}]
    assert collector.last_run_summary["updated_count"] == 1


def test_cap_and_cooldown_rotate_away_from_failed_missing_profiles(db):
    client = ReplayClient()
    client.profiles["80000220"] = {"code": "1", "data": None}
    collector = collector_for(db, client, max_targets_per_run=1)
    assert collector.collect_all_companies() == 0
    assert collector.last_run_summary["failed_count"] == 1
    assert collector.last_run_summary["remaining_eligible_count"] == 2
    assert collector.get_stats()["skipped"] == 0
    assert collector.collect_all_companies() == 1
    assert collector.last_run_summary["cooldown_count"] == 1
    assert client.calls[-1]["cc"] != "80000220"
    assert collector.last_run_summary["status"] == "degraded"
    with db.get_session() as session:
        for log in session.query(CollectionLog).all():
            log.created_at = datetime.now() - timedelta(hours=25)
    assert collector.collect_all_companies() == 1
    assert client.calls[-1]["cc"] == "80053708"  # never attempted before the old failure
    assert collector.collect_all_companies(force_update=True) == 0
    assert len(client.calls) == 4  # force still respects the configured cap


def test_new_snapshot_clears_unavailable_fields_and_keeps_real_zero(db):
    collector = collector_for(db)
    collector.collect_all_companies()
    # Isolated boundary mutation, not a claim about the real API response.
    changed = deepcopy(PROFILES)
    changed["80053708"]["data"].pop("GLGM")
    changed["80053708"]["data"].update(Count=0, ManagerCount="0", FRDB="--")
    collector = collector_for(db, ReplayClient(changed))
    assert collector.collect_all_companies(force_update=True) == 3
    with db.get_session(readonly=True) as session:
        row = session.query(FundCompany).filter_by(company_id="80053708").one()
        assert row.total_scale is None and row.legal_rep is None
        assert row.fund_count == 0 and row.manager_count == 0


@pytest.mark.parametrize("fault", ["missing_code", "failure_code", "missing_name", "unitless_scale", "fractional_count"])
def test_invalid_payload_does_not_renew_success_timestamp(db, fault):
    collector = collector_for(db)
    collector.collect_all_companies()
    with db.get_session(readonly=True) as session:
        before = session.query(FundCompany).filter_by(company_id="80053708").one().last_updated_at
    client = ReplayClient()
    payload = client.profiles["80053708"]
    if fault == "missing_code":
        payload.pop("code")
    elif fault == "failure_code":
        payload["code"] = "0"
    elif fault == "missing_name":
        payload["data"]["FDMC"] = "--"
    elif fault == "unitless_scale":
        payload["data"]["GLGM"] = "12340.71"
    else:
        payload["data"]["Count"] = "1.5"
    collector = collector_for(db, client)
    assert collector.collect_all_companies(force_update=True) == 2
    assert collector.last_run_summary["failed_count"] == 1
    assert collector.get_api_source_evidence()["failed_response_count"] == 1
    with db.get_session(readonly=True) as session:
        row = session.query(FundCompany).filter_by(company_id="80053708").one()
        assert row.last_updated_at == before and row.total_scale == 12340.71


@pytest.mark.parametrize("fault", ["empty", "transport", "database"])
def test_failures_have_nonzero_cli_status_and_persisted_evidence(db, tmp_path, capsys, fault):
    client = ReplayClient()
    if fault == "empty":
        client.profiles["80053708"] = {"code": "1", "data": None}
    elif fault == "transport":
        client.profiles["80053708"] = TimeoutError("isolated transport failure")
    else:
        with sqlite3.connect(db.db_path) as conn:
            conn.execute("CREATE TRIGGER fail_fixture_insert BEFORE INSERT ON fund_companies "
                         "WHEN NEW.company_id='80053708' BEGIN SELECT RAISE(ABORT,'isolated storage failure'); END")
    collector = collector_for(db, client)
    assert run_cli(collector) == 2
    stdout = capsys.readouterr().out
    assert "✅ 基金公司采集完成" not in stdout
    path = tmp_path / "stdout.log"
    path.write_text(stdout)
    semantics, blockers, warnings = _stdout_result_semantics("collect-companies", str(path))
    assert semantics["business_summary"]["failed_count"] == 1
    assert not any("count_mismatch" in item for item in blockers)
    if fault == "empty":
        assert warnings
    else:
        assert blockers
    severity = classify_collection_log_severity(["[collect-companies] $ fixture", *stdout.splitlines()])
    assert severity["degraded"] is True and severity["degraded_stage"] == "collect-companies"
    assert severity["summary_failed"] == severity["company_summary_failed"] == 1
    with db.get_session(readonly=True) as session:
        assert session.query(FundCompany).count() == 2
        assert session.query(CollectionLog).filter_by(status="failed", fcode="80053708").count() == 1


def test_cli_success_and_fresh_skip_pass_same_audit_contract(db, tmp_path, capsys):
    collector = collector_for(db)
    for expected_status in ("success", "skipped"):
        assert run_cli(collector) == 0
        stdout = capsys.readouterr().out
        path = tmp_path / f"{expected_status}.log"
        path.write_text(stdout)
        semantics, blockers, warnings = _stdout_result_semantics("collect-companies", str(path))
        assert blockers == [] and warnings == []
        assert semantics["business_summary"]["status"] == expected_status


def test_company_phase_runs_only_full_weekly_and_after_risk():
    settings = load_settings()
    for profile in ("full", "weekly", "daily"):
        names = [step.name for step in build_core_commands(settings, {
            "run_collection": True, "run_screening": False, "collection_profile": profile,
        })]
        if profile == "daily":
            assert "collect-companies" not in names
        else:
            assert names[-1] == "collect-companies"
            assert names.index("calculate-risk") < names.index("collect-companies")
    commands = build_core_commands(replace(settings, collect_companies=False), {
        "run_collection": True, "run_screening": False, "collection_profile": "weekly",
    })
    assert "collect-companies" not in [step.name for step in commands]


def test_subprocess_failure_keeps_stage_and_exit_code(tmp_path):
    from backend import core_runner
    failed = SimpleNamespace(success=False, return_code=2, timed_out=False,
                             timeout_reason=None, meta_path=tmp_path / "meta.json")
    with patch.object(core_runner, "run_structured_subprocess", return_value=failed):
        with pytest.raises(CoreStepFailedError) as error:
            _run_streaming_subprocess(name="collect-companies", cmd=["fixture"], cwd=tmp_path,
                                      env={}, log=lambda message: None)
    assert error.value.stage == "collect-companies" and error.value.return_code == 2


def test_weekly_continues_governance_and_keeps_company_failure():
    from backend.scheduler import ProjectScheduler
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(scheduler_weekly_nav_history_limit=240,
                                         scheduler_weekly_long_term_history_limit=20,
                                         collect_companies=True)
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None

    def fail_company(settings, options, log):
        log("[collect-companies] $ fixture")
        log("api_source_evidence=" + json.dumps({"source": "companyApi2", "target_count": 3,
            "success_count": 2, "failed_count": 1, "skipped_count": 0, "cooldown_count": 0}))
        raise CoreStepFailedError(stage="collect-companies", return_code=2, meta_path="fixture.meta.json")

    with (
        patch("backend.scheduler.run_project_pipeline", side_effect=fail_company),
        patch("backend.scheduler.run_bond_universe_sync", return_value={"diffs_after_sync": {}}) as governance,
        patch("backend.scheduler.run_nav_history_maintenance", return_value={}),
        patch("backend.scheduler.run_long_term_history_maintenance", return_value={}),
    ):
        result = scheduler._execute_weekly_maintenance()
    governance.assert_called_once()
    assert result["status"] == "degraded" and result["warning_reason"] == "company_collection_failed"
    assert result["degraded_stage"] == "collect-companies"
    assert result["company_records_processed"] == 3 and result["company_records_failed"] == 1
    assert result["company_collection_enabled"] is True


def test_invalid_company_summary_cannot_be_success():
    result = classify_collection_log_severity(['api_source_evidence=' + json.dumps({
        "source": "companyApi2", "target_count": 2, "success_count": 2,
        "failed_count": 1, "skipped_count": 0,
    })])
    assert result["degraded"] is True and result["reason"] == "company_collection_summary_invalid"


def test_observed_zero_target_company_stage_keeps_its_counters():
    result = classify_collection_log_severity(['api_source_evidence=' + json.dumps({
        "source": "companyApi2", "target_count": 0, "success_count": 0,
        "failed_count": 0, "skipped_count": 0, "cooldown_count": 0,
    })])
    assert result["degraded"] is False
    assert result["company_summary_total"] == 0
    assert result["company_summary_success"] == 0
    assert result["company_summary_failed"] == 0
    assert result["company_cooldown_count"] == 0
    assert "collect-companies" in result["summary_by_stage"]


def test_legacy_scheduler_preserves_explicit_degraded_status_and_counts(db):
    from database.models import TaskHistory
    from schedulers.task_scheduler import TaskScheduler
    TaskHistory.__table__.create(db.engine)
    # Use the production constructor: logging is intentionally initialized at
    # runtime, not when importing the module. No scheduler thread is started.
    with patch("schedulers.task_scheduler.signal.signal"):
        scheduler = TaskScheduler(
            config={"scheduler": {"retry": {"max_attempts": 1, "initial_delay": 0, "max_delay": 0}}},
            db_manager=db, api_client=ReplayClient(),
        )
    result = {"processed": 3, "inserted": 2, "updated": 0, "failed": 1,
              "metadata": {"status": "degraded", "stage_summary": {"failed_count": 1}}}
    assert scheduler._execute_with_history_inner("isolated company fixture", lambda: result) == result
    with db.get_session(readonly=True) as session:
        row = session.query(TaskHistory).one()
        assert row.status == "degraded" and row.records_processed == 3
        assert row.records_failed == 1 and row.records_inserted == 2


def test_progress_is_visible_before_requests_even_without_module_info_logging(db):
    output = StringIO()
    client = ReplayClient()
    original_get = client.get
    before_request = []

    def capture_request(endpoint, params):
        before_request.append(output.getvalue())
        return original_get(endpoint, params)

    client.get = capture_request
    with redirect_stdout(output):
        assert collector_for(db, client).collect_all_companies(workers=1) == 3
    assert "公司资料刷新:" in before_request[0] and "目标=3" in before_request[0]
    assert "公司资料刷新进度: 3/3 成功=3 失败=0" in output.getvalue()
