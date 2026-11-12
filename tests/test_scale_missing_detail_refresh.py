"""Bounded scale recovery must preserve missing values and committed counts."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
import hashlib
import json
import logging
import sys

import pytest

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / "core")]

from collectors.detail_collector import FundDetailCollector
from collectors.scale_refresh import load_missing_scale_targets, TASK_TYPE
from database.connection import get_db_manager
from database.models import Base, CollectionLog, Fund
from backend.config import load_settings
from backend.core_runner import build_core_commands
from cli.aux_commands import cmd_collect_details
from utils.detail_collection_summary import detail_collection_source_errors


@pytest.fixture
def collector(tmp_path):
    manager = get_db_manager(str(tmp_path / "scale_refresh.sqlite"))
    Base.metadata.create_all(manager.engine)
    with manager.get_session() as session:
        session.add_all([
            Fund(fcode="000001", shortname="missing scale", benchmark="keep benchmark",
                 detail_updated_at=datetime(2026, 9, 27), holding_days=30),
            Fund(fcode="000002", shortname="has scale", scale_billion=1.25),
        ])
    result = FundDetailCollector(config={"collection": {"detail": {
        "scale_refresh_retry_hours": 24, "scale_refresh_max_targets_per_run": 120,
    }}}, db_manager=manager, api_client=object())
    result.empty_detail_report_dir = tmp_path
    result.api.get_fund_detail = lambda code: {"Success": True, "Datas": {
        "FCODE": code, "ENDNAV": "250000000", "FEGMRQ": "2026-09-30", "ENDNAVDT": "2026-10-26",
        "BENCH": "must not replace", "CYCLE": "365",
    }}
    return result


def _run(collector, **kwargs):
    value = collector.collect_all_details(workers=1, scale_missing_only=True, **kwargs)
    summary = collector.detail_collection_summary
    assert value == summary["write_success_count"]
    assert detail_collection_source_errors(summary, collector.get_api_source_evidence()) == []
    return value


def _log(collector):
    with collector.db_manager.get_session(readonly=True) as session:
        log = session.query(CollectionLog).filter_by(task_type=TASK_TYPE).one()
        return log.status, log.message


def test_queue_cooldown_and_exclusion_precede_limit_including_explicit_targets(collector, monkeypatch):
    collector.detail_config["scale_refresh_max_targets_per_run"] = 1
    with collector.db_manager.get_session() as session:
        for code in ("000003", "000004", "000005"):
            session.add(Fund(fcode=code, shortname=code))
        session.add(CollectionLog(task_type=TASK_TYPE, fcode="000003", status="failed"))
        # Logs without a code must not poison the NOT IN query.
        session.add(CollectionLog(task_type=TASK_TYPE, fcode=None, status="failed"))
    monkeypatch.setattr("collectors.scale_refresh.get_runtime_excluded_fund_codes", lambda: {"000004"})
    assert load_missing_scale_targets(collector) == ["000005"]
    assert load_missing_scale_targets(collector, ["000002", "000003", "000004", "000005"]) == ["000005"]


def test_valid_scale_commits_only_scale_fields_and_archives_source(collector):
    assert _run(collector) == 1
    with collector.db_manager.get_session(readonly=True) as session:
        row = session.get(Fund, "000001")
        assert row.scale_billion == 2.5 and str(row.scale_report_date) == "2026-09-30"
        assert row.detail_updated_at == datetime(2026, 9, 27)
        assert row.benchmark == "keep benchmark" and row.holding_days == 30 and row.cycle is None
    assert _log(collector) == ("success", "scale_billion_available")
    evidence = collector.get_api_source_evidence()
    path = Path(evidence["scale_refresh_manifest_path"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == evidence["scale_refresh_manifest_sha256"]
    record = json.loads(path.read_text())["funds"][0]
    response = path.parent / record["response_file"]
    assert hashlib.sha256(response.read_bytes()).hexdigest() == record["response_sha256"]
    assert record["response_representation"] == "serialized_parsed_json"
    assert json.loads(response.read_text())["Datas"]["ENDNAV"] == "250000000"
    assert _run(collector) == 0 and collector.get_api_source_evidence()["request_count"] == 0


@pytest.mark.parametrize("fields,reason", [
    ({"ENDNAV": None}, "upstream_scale_missing"),
    ({"ENDNAV": "--"}, "upstream_scale_missing"),
    ({"ENDNAV": "0"}, "invalid_scale_value"),
    ({"ENDNAV": "nan"}, "invalid_scale_value"),
    ({"ENDNAV": "inf"}, "invalid_scale_value"),
    ({"ENDNAV": "-1"}, "invalid_scale_value"),
    ({"FEGMRQ": None, "ENDNAVDT": None}, "missing_or_invalid_scale_report_date"),
    ({"FEGMRQ": "invalid"}, "missing_or_invalid_scale_report_date"),
    ({"FEGMRQ": (date.today() + timedelta(days=1)).isoformat()}, "future_scale_report_date"),
])
def test_missing_or_invalid_scale_is_unavailable_and_keeps_null(collector, fields, reason):
    original = collector.api.get_fund_detail
    collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": {**original(code)["Datas"], **fields}}
    assert _run(collector) == 0
    assert _log(collector) == ("missing_scale", reason)
    assert collector.detail_collection_summary["status"] == "completed_with_unavailable"
    with collector.db_manager.get_session(readonly=True) as session:
        assert session.get(Fund, "000001").scale_billion is None
    assert load_missing_scale_targets(collector, ["000001", "000002"]) == []


@pytest.mark.parametrize("response", [
    {"Success": False, "ErrCode": 1, "Datas": {"FCODE": "000001", "ENDNAV": "250000000"}},
    {"Success": True, "Datas": {"FCODE": "999999", "ENDNAV": "250000000"}},
    {"Success": True, "Datas": {"ENDNAV": "250000000"}},
])
def test_business_or_identity_failure_is_not_a_success(collector, response):
    collector.api.get_fund_detail = lambda code: response
    assert _run(collector) == 0
    assert collector.detail_collection_summary["fetch_failed_count"] == 1
    assert _log(collector)[0] == "failed"


def test_transport_exception_is_failed_and_cooled(collector):
    def fail(code):
        raise OSError("isolated transport failure")
    collector.api.get_fund_detail = fail
    assert _run(collector) == 0
    assert collector.get_api_source_evidence()["transport_exception_count"] == 1
    assert collector.detail_collection_summary["status"] == "degraded"
    assert load_missing_scale_targets(collector) == []


def test_write_rejection_rolls_back_scale_and_success_log(collector):
    with collector.db_manager.engine.begin() as conn:
        conn.exec_driver_sql("CREATE TRIGGER reject_scale BEFORE UPDATE OF scale_billion ON funds BEGIN SELECT RAISE(ABORT, 'isolated rejection'); END")
    assert _run(collector) == 0
    assert collector.detail_collection_summary["write_failed_count"] == 1
    assert _log(collector)[0] == "failed"
    with collector.db_manager.get_session(readonly=True) as session:
        assert session.get(Fund, "000001").scale_billion is None


def test_selected_then_concurrently_filled_scale_is_not_overwritten(collector):
    original = collector.api.get_fund_detail
    def concurrent_update(code):
        with collector.db_manager.get_session() as session:
            session.get(Fund, code).scale_billion = 9.0
        return original(code)
    collector.api.get_fund_detail = concurrent_update
    assert _run(collector) == 0
    with collector.db_manager.get_session(readonly=True) as session:
        assert session.get(Fund, "000001").scale_billion == 9.0
    assert _log(collector) == ("skipped", "scale_no_longer_missing")


def test_cooldown_log_failure_is_not_swallowed(collector, monkeypatch):
    original = collector.db_manager.get_session
    @contextmanager
    def rejecting_log(*args, **kwargs):
        if kwargs.get("task_name") == "scale_refresh_log":
            raise OSError("isolated log failure")
        with original(*args, **kwargs) as session:
            yield session
    collector.api.get_fund_detail = lambda code: {"Success": True, "Datas": {"FCODE": code}}
    monkeypatch.setattr(collector.db_manager, "get_session", rejecting_log)
    with pytest.raises(OSError, match="isolated log failure"):
        _run(collector)


def test_cli_passes_scale_mode_and_validates_terminal_summary(collector):
    assert cmd_collect_details(SimpleNamespace(workers=1, force=False, fcode="000001", scale_missing_only=True), {},
        logger=logging.getLogger(__name__), ensure_database_initialized=lambda config: "isolated.sqlite",
        emit_command_error=lambda message, exc: 1, collector_cls=lambda **kwargs: collector) == 0
    assert collector.get_api_source_evidence()["collection_mode"] == "scale_missing_only"


def test_daily_uses_bounded_scale_refresh_but_weekly_keeps_full_details():
    settings = load_settings()
    daily = build_core_commands(settings, {"run_collection": True, "run_screening": False, "collection_profile": "daily"})
    weekly = build_core_commands(settings, {"run_collection": True, "run_screening": False, "collection_profile": "weekly"})
    assert "--scale-missing-only" in next(step.command for step in daily if step.name == "collect-details")
    assert "--scale-missing-only" not in next(step.command for step in weekly if step.name == "collect-details")
