"""Attempt/success timestamps and retry behavior in isolated real-binding databases."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import event

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from collectors import manager_collector as module
from collectors.manager_collector import ManagerCollector
from cli.aux_commands import cmd_collect_managers
from backend.collection_log_summary import classify_collection_log_severity
from database.connection import DatabaseManager
from database.migrations import migrate_add_columns
from database.models import Fund, FundManager
from utils.manager_collection_summary import (
    build_manager_collection_summary, manager_collection_source_errors,
    validate_manager_collection_summary,
)

FIXTURE = json.loads((ROOT / "tests/fixtures/tiantian_manager_assignments_20260908.json").read_text())
T0 = datetime(2026, 9, 8, 14, 0, 0)


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        value = T0

        @classmethod
        def now(cls, tz=None):
            assert tz is None
            return cls.value

    monkeypatch.setattr(module, "datetime", Clock)
    return Clock


def make_collector(tmp_path, codes=("000015",), response=None, config=None):
    db = DatabaseManager(db_path=str(tmp_path / "manager-refresh.sqlite"), wal_mode=True)
    Fund.__table__.create(db.engine)
    FundManager.__table__.create(db.engine)
    with db.get_session() as session:
        for code in codes:
            row = {k: v for k, v in FIXTURE["baseline_funds"][code].items() if k != "reason"}
            if row["manager_list_updated_at"]:
                row["manager_list_updated_at"] = datetime.fromisoformat(row["manager_list_updated_at"])
            session.add(Fund(**row))
    calls = []

    class ReplayClient:
        current = response

        def get(self, endpoint, params):
            assert endpoint == "/fundMNMangerList"
            code = params["FCODE"]
            calls.append(code)
            if isinstance(self.current, BaseException):
                raise self.current
            return deepcopy(self.current if self.current is not None else FIXTURE["responses"][code]["payload"])

    client = ReplayClient()
    collector = ManagerCollector(config=config or {}, db_manager=db, api_client=client)
    return collector, db, client, calls


def snapshot(db, code="000015"):
    with db.get_session(readonly=True) as session:
        fund = session.get(Fund, code)
        return {column.name: getattr(fund, column.name) for column in Fund.__table__.columns}


def terminal(listing):
    return build_manager_collection_summary({
        "fund_manager_list": listing,
        "manager_details": {"requested_manager_count": 0, "updated_manager_count": 0,
                            "failed_manager_count": 0, "skipped_manager_count": 0},
    })


@pytest.mark.parametrize("response,status,empty_count", [
    ({"Success": True, "Datas": []}, "empty_response", 1),
    ({"Success": True, "Datas": None}, "invalid_datas_type", 0),
    (ConnectionError("isolated transport failure"), "request_failed", 0),
])
def test_failed_attempt_never_renews_business_fields_and_survives_collector_restart(tmp_path, clock, response, status, empty_count):
    collector, db, client, calls = make_collector(tmp_path, response=response)
    before = snapshot(db)
    listing = collector._collect_fund_managers(force_update=True)
    after = snapshot(db)
    assert {k: v for k, v in after.items() if k not in {"manager_list_attempted_at", "manager_list_last_status"}} == {
        k: v for k, v in before.items() if k not in {"manager_list_attempted_at", "manager_list_last_status"}}
    assert after["manager_list_attempted_at"] == T0 and after["manager_list_last_status"] == status
    assert listing == {"total_funds": 1, "success_funds": 0, "failed_funds": 1,
                       "empty_funds": empty_count, "cooldown_funds": 0}
    assert terminal(listing)["status"] == "degraded"
    restarted = ManagerCollector(config={}, db_manager=db, api_client=client)
    cooldown = restarted._collect_fund_managers()
    assert calls == ["000015"] and cooldown["total_funds"] == 0 and cooldown["cooldown_funds"] == 1
    summary = terminal(cooldown)
    assert summary["status"] == "degraded" and summary["reason"] == "manager_collection_retry_cooldown"
    assert summary["failed_count"] == 0 and summary["cooldown_count"] == 1
    assert validate_manager_collection_summary(summary) == summary
    evidence = restarted.get_manager_list_api_source_evidence()
    assert evidence["request_count"] == evidence["saved_fund_count"] == 0
    assert evidence["cooldown_next_retry_at"] == (T0 + timedelta(hours=24)).isoformat()


def test_cooldown_expires_exactly_and_force_bypasses_it_without_changing_success_contract(tmp_path, clock):
    collector, db, client, calls = make_collector(tmp_path, response={"Success": True, "Datas": []})
    collector._collect_fund_managers(force_update=True)
    clock.value = T0 + timedelta(hours=24) - timedelta(microseconds=1)
    assert collector._collect_fund_managers()["cooldown_funds"] == 1
    clock.value = T0 + timedelta(hours=24)
    assert collector._collect_fund_managers()["failed_funds"] == 1
    assert calls == ["000015", "000015"]
    client.current = None  # Replay the saved real, current assignment.
    clock.value += timedelta(seconds=1)
    assert collector._collect_fund_managers(force_update=True)["success_funds"] == 1
    after = snapshot(db)
    assert after["manager_list_last_status"] == "ok"
    assert after["manager_list_updated_at"] == clock.value
    assert after["manager_ids"] == FIXTURE["baseline_funds"]["000015"]["manager_ids"]
    assert terminal(collector._collect_fund_managers())["status"] == "skipped"
    assert len(calls) == 3


def test_failure_retry_hours_is_configured_separately_from_missing_binding_refresh(tmp_path, clock):
    collector, _, _, calls = make_collector(tmp_path, response={"Success": True, "Datas": []},
        config={"collection": {"manager": {"list_failure_retry_hours": 3, "missing_list_retry_days": 7}}})
    collector._collect_fund_managers(force_update=True)
    clock.value += timedelta(hours=3)
    assert collector._collect_fund_managers()["failed_funds"] == 1
    assert len(calls) == 2


def test_recent_failure_rotates_behind_unattempted_stale_fund(tmp_path, clock):
    collector, db, client, calls = make_collector(tmp_path, codes=("000015", "000147"),
        response={"Success": True, "Datas": []},
        config={"collection": {"manager": {"max_fund_targets_per_run": 1}}})
    with db.get_session() as session:
        session.query(Fund).update({Fund.manager_list_updated_at: None})
    collector._collect_fund_managers(force_update=True, target_fcodes=["000015"])
    clock.value += timedelta(hours=25)
    client.current = None
    result = collector._collect_fund_managers()
    assert result["total_funds"] == result["success_funds"] == 1
    assert calls == ["000015", "000147"]
    assert collector.get_manager_list_api_source_evidence()["remaining_eligible_count"] == 1
    assert collector._collect_fund_managers()["success_funds"] == 1
    assert calls == ["000015", "000147", "000015"]


def test_confirmed_ended_real_history_is_success_and_keeps_seven_day_refresh(tmp_path, clock):
    collector, db, _, calls = make_collector(tmp_path, codes=("002109",))
    assert collector._collect_fund_managers(force_update=True)["success_funds"] == 1
    after = snapshot(db, "002109")
    assert after["manager_ids"] is None and after["manager_names"] is None
    assert after["manager_list_last_status"] == "no_current_managers_confirmed"
    clock.value += timedelta(days=1)
    assert terminal(collector._collect_fund_managers())["status"] == "skipped"
    clock.value += timedelta(days=7)
    assert collector._collect_fund_managers()["success_funds"] == 1
    assert len(calls) == 2


@pytest.mark.parametrize("failure_status_write_fails", [False, True])
def test_business_commit_failure_retains_durable_attempt_and_blocks_repeat_requests(tmp_path, clock, failure_status_write_fails):
    collector, db, _, calls = make_collector(tmp_path)
    before = snapshot(db)

    def reject_business_write(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("UPDATE FUNDS"):
            values = context.compiled_parameters[0]
            if "manager_ids" in values or (failure_status_write_fails and values.get("manager_list_last_status") == "save_failed"):
                raise RuntimeError("isolated business/status write failure")

    event.listen(db.engine, "before_cursor_execute", reject_business_write)
    try:
        result = collector._collect_fund_managers(force_update=True)
    finally:
        event.remove(db.engine, "before_cursor_execute", reject_business_write)
    after = snapshot(db)
    assert result["success_funds"] == 0 and result["failed_funds"] == 1
    assert after["manager_list_updated_at"] == before["manager_list_updated_at"]
    assert after["last_updated_at"] == before["last_updated_at"]
    assert after["manager_list_attempted_at"] == T0
    assert after["manager_list_last_status"] == ("started" if failure_status_write_fails else "save_failed")
    assert collector.get_manager_list_api_source_evidence()["saved_fund_count"] == 0
    assert collector._collect_fund_managers()["cooldown_funds"] == 1 and len(calls) == 1


def test_attempt_write_failure_consumes_no_upstream_quota(tmp_path, clock):
    collector, db, _, calls = make_collector(tmp_path)
    before = snapshot(db)

    def reject_attempt(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("UPDATE FUNDS"):
            raise RuntimeError("isolated attempt storage unavailable")

    event.listen(db.engine, "before_cursor_execute", reject_attempt)
    try:
        result = collector._collect_fund_managers(force_update=True)
    finally:
        event.remove(db.engine, "before_cursor_execute", reject_attempt)
    assert result["failed_funds"] == 1 and not calls
    assert snapshot(db) == before
    evidence = collector.get_manager_list_api_source_evidence()
    assert evidence["request_count"] == 0 and evidence["save_failed_count"] == 1


def test_interrupted_request_keeps_started_cooldown_without_false_success(tmp_path, clock):
    collector, db, client, calls = make_collector(tmp_path, response=KeyboardInterrupt())
    before = snapshot(db)
    with pytest.raises(KeyboardInterrupt):
        collector._collect_fund_managers(force_update=True)
    after = snapshot(db)
    assert after["manager_list_updated_at"] == before["manager_list_updated_at"]
    assert after["manager_list_last_status"] == "started"
    restarted = ManagerCollector(config={}, db_manager=db, api_client=client)
    assert restarted._collect_fund_managers()["cooldown_funds"] == 1 and len(calls) == 1


def test_additive_migration_preserves_all_legacy_values_and_unknown_attempts(tmp_path):
    _, db, _, _ = make_collector(tmp_path)
    db.engine.dispose()
    path = tmp_path / "manager-refresh.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE funds DROP COLUMN manager_list_attempted_at")
        connection.execute("ALTER TABLE funds DROP COLUMN manager_list_last_status")
        original_columns = [row[1] for row in connection.execute("PRAGMA table_info(funds)")]
        before = connection.execute("SELECT * FROM funds ORDER BY fcode").fetchall()
    assert migrate_add_columns(str(path)) is True
    assert migrate_add_columns(str(path)) is True
    with sqlite3.connect(path) as connection:
        projection = ",".join('"' + column + '"' for column in original_columns)
        assert connection.execute(f"SELECT {projection} FROM funds ORDER BY fcode").fetchall() == before
        assert connection.execute("SELECT manager_list_attempted_at,manager_list_last_status FROM funds").fetchall() == [(None, None)]
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"


@pytest.mark.parametrize("invalid", [None, True, -1, "1", 1.0])
def test_cooldown_count_cannot_be_missing_fabricated_or_invalid(invalid):
    listing = {"total_funds": 0, "success_funds": 0, "failed_funds": 0, "empty_funds": 0,
               "cooldown_funds": invalid}
    with pytest.raises(ValueError):
        terminal(listing)


def test_new_cooldown_source_contract_is_explicit_and_old_contract_stays_unchanged():
    listing = {"total_funds": 0, "success_funds": 0, "failed_funds": 0, "empty_funds": 0}
    old = terminal(listing)
    assert "cooldown_count" not in old and validate_manager_collection_summary(old) == old
    forged = {**old, "cooldown_count": 0}
    with pytest.raises(ValueError, match="without_stage_evidence"):
        validate_manager_collection_summary(forged)
    current = terminal({**listing, "cooldown_funds": 0})
    entries = [{"task": "collect-managers", "source": source, "target_count": 0, "request_count": 0}
               for source in ("fundMNMangerList", "fundMSNMangerInfo")]
    assert manager_collection_source_errors(current, entries) == [
        "manager_collection_source_count_mismatch:fundMNMangerList:cooldown_count"]
    entries[0]["cooldown_count"] = 0
    assert manager_collection_source_errors(current, entries) == []


def test_actual_cli_preserves_cooldown_as_degraded_with_zero_new_requests(tmp_path, clock, capsys):
    collector, db, client, calls = make_collector(tmp_path, response={"Success": True, "Datas": []})
    before = snapshot(db)
    with db.get_session() as session:
        session.add(FundManager(mgr_id=before["manager_ids"], name=before["manager_names"], last_updated_at=T0))
    collector._collect_fund_managers(force_update=True)
    capsys.readouterr()
    restarted = ManagerCollector(config={}, db_manager=db, api_client=client)
    with patch("utils.task_reporter.update_task_history") as reporter:
        exit_code = cmd_collect_managers(
            SimpleNamespace(workers=1, force=False), {}, logger=logging.getLogger(__name__),
            ensure_database_initialized=lambda _: str(tmp_path / "manager-refresh.sqlite"),
            emit_command_error=lambda message, exc: 1, collector_cls=lambda **kwargs: restarted,
        )
    assert exit_code == 2 and len(calls) == 1
    assert reporter.call_args.kwargs["status_override"] == "degraded"
    stdout = capsys.readouterr().out
    assert "等待重试1只基金" in stdout and "✅" not in stdout
    classified = classify_collection_log_severity(stdout.splitlines())
    assert classified["degraded"] and classified["reason"] == "manager_collection_retry_cooldown"
    assert classified["summary_total"] == classified["summary_failed"] == 0
    assert classified["manager_collection_summary"]["cooldown_count"] == 1
    assert classified["manager_collection_summary_errors"] == []


def test_successful_subset_does_not_hide_other_funds_in_cooldown(tmp_path, clock):
    collector, db, client, _ = make_collector(tmp_path, codes=("000015", "000147"),
                                            response={"Success": True, "Datas": []})
    collector._collect_fund_managers(force_update=True, target_fcodes=["000015"])
    with db.get_session() as session:
        session.query(Fund).filter(Fund.fcode == "000147").update({Fund.manager_list_updated_at: None})
    client.current = None
    result = collector._collect_fund_managers()
    assert result["success_funds"] == 1 and result["failed_funds"] == 0 and result["cooldown_funds"] == 1
    assert terminal(result)["status"] == "degraded"
