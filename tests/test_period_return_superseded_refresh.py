"""Real stored rank snapshots survive an explicitly older refresh, in isolation."""
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import Date, DateTime

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.collection_log_summary import classify_collection_log_severity
from collectors import fund_collector as fund_module
from collectors import period_return_collector as module
from collectors.period_return_collector import PeriodReturnCollector
from database.connection import DatabaseManager
from database.models import Fund, FundReturns
from utils.period_return_snapshot import (
    RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, period_collection_blocking_count,
    attach_period_return_status,
)

FIXTURE = json.loads((ROOT / "tests/fixtures/period_returns_superseded_20261016.json").read_text())
CODE = "007710"
BUSINESS_FIELDS = (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, "latest_nav", "latest_nav_date", "last_updated_at")


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        value = datetime(2026, 10, 16, 17)

        @classmethod
        def now(cls, tz=None):
            assert tz is None
            return cls.value

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(fund_module, "datetime", Clock)
    return Clock


def typed(model, record):
    values = {column.name: deepcopy(record[column.name]) for column in model.__table__.columns
              if column.name in record}
    for column in model.__table__.columns:
        if isinstance(values.get(column.name), str):
            if isinstance(column.type, DateTime):
                values[column.name] = datetime.fromisoformat(values[column.name])
            elif isinstance(column.type, Date):
                values[column.name] = date.fromisoformat(values[column.name])
    return values


def make_collector(tmp_path):
    db = DatabaseManager(str(tmp_path / "superseded.sqlite"))
    Fund.__table__.create(db.engine)
    FundReturns.__table__.create(db.engine)
    with db.get_session() as session:
        for raw in FIXTURE["records"]:
            session.add(Fund(**typed(Fund, raw)))
            values = typed(FundReturns, raw)
            # Reset only retry ownership in this isolated replay database.
            values.update(period_return_attempted_at=None, period_return_last_status=None)
            session.add(FundReturns(**values))

    class Client:
        calls = None
        response = None
        before_response = None

        def __init__(self):
            self.calls = []

        def get(self, endpoint, params):
            assert endpoint == "/fundMNPeriodIncrease"
            self.calls.append(params["FCODE"])
            if self.before_response:
                self.before_response()
            if isinstance(self.response, Exception):
                raise self.response
            return deepcopy(self.response or next(row["response"] for row in FIXTURE["observations"]
                                                 if row["fcode"] == params["FCODE"]))

    client = Client()
    return PeriodReturnCollector(config={}, db_manager=db, api_client=client), db, client


def read(db, code=CODE):
    with db.get_session(readonly=True) as session:
        row = session.get(FundReturns, code)
        return {column.name: getattr(row, column.name) for column in FundReturns.__table__.columns}


def test_real_older_refresh_is_rejected_without_blocking_verified_newer_inputs(tmp_path, clock):
    collector, db, client = make_collector(tmp_path)
    codes = sorted(raw["fcode"] for raw in FIXTURE["records"])
    before = {code: read(db, code) for code in codes}
    for expected_requests in (2, 0):
        result = collector.collect_current()
        assert result["request_count"] == result["failed_count"] == expected_requests
        assert result["saved_fund_count"] == result["changed_count"] == 0
        assert result["status"] == "degraded" and result["pending_count"] == 2
        assert result["blocking_count"] == 0 and result["screening_blocked"] is False
        assert result["superseded_refresh_fcodes"] == codes
        assert result["nonblocking_missing_fcodes"] == result["quarantined_date_lag_fcodes"] == []
        for code in codes:
            after = read(db, code)
            assert after["period_return_last_status"] == "superseded_refresh_rejected"
            assert {key: after[key] for key in BUSINESS_FIELDS} == {key: before[code][key] for key in BUSINESS_FIELDS}
        health = classify_collection_log_severity(["api_source_evidence=" + json.dumps(result)])
        assert health["degraded"] is True
        assert health["reason"] == "period_return_refresh_superseded"
        collector = PeriodReturnCollector(config={}, db_manager=db, api_client=client)
    assert sorted(client.calls) == codes


def test_cli_handoff_retains_the_rejected_response_and_degraded_evidence(tmp_path, monkeypatch, clock, capsys):
    from core.main import cmd_collect_period_returns

    collector, db, _ = make_collector(tmp_path)
    before = read(db)
    evidence = tmp_path / "evidence"
    monkeypatch.setenv("FUND_ANA_SUBPROCESS_LOG_DIR", str(evidence))
    monkeypatch.setattr(module, "PeriodReturnCollector", lambda config: collector)
    assert cmd_collect_period_returns(SimpleNamespace(fcodes=CODE), {"database": {"path": str(db.db_path)}}) == 0
    line = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("api_source_evidence="))
    summary = json.loads(line.split("=", 1)[1])
    path = Path(summary["response_artifact"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == summary["response_artifact_sha256"]
    payload = json.loads(path.read_text())
    assert payload["observations"][0]["response"] == FIXTURE["observations"][0]["response"]
    assert summary["status"] == "degraded" and summary["failed_count"] == 1
    assert summary["saved_fund_count"] == summary["changed_count"] == summary["blocking_count"] == 0
    assert summary["superseded_refresh_fcodes"] == [CODE]
    after = read(db)
    assert {key: after[key] for key in BUSINESS_FIELDS} == {key: before[key] for key in BUSINESS_FIELDS}


@pytest.mark.parametrize("damage", [
    "hash", "identity", "date", "nav", "return", "missing_return", "row_return", "source", "future_observation",
])
def test_unproven_stored_snapshot_cannot_exempt_an_old_refresh(tmp_path, clock, damage):
    collector, db, _ = make_collector(tmp_path)
    with db.get_session() as session:
        row = session.get(FundReturns, CODE)
        payload = json.loads(row.period_return_source_json)
        if damage == "hash":
            row.period_return_source_sha256 = "0" * 64
        elif damage == "source":
            row.period_return_source = "fundMNPeriodIncrease"
        elif damage == "future_observation":
            row.period_return_observed_at = clock.value + timedelta(days=1)
        elif damage == "row_return":
            row.return_1w = None
        else:
            if damage == "identity": payload["FCODE"] = "007711"
            if damage == "date": payload["FSRQ"] = "2026-10-09"
            if damage == "nav": payload["DWJZ"] = "999"
            if damage == "return": payload["SYL_Z"] = "999"
            if damage == "missing_return": payload.pop("SYL_5N")
            row.period_return_source_json = json.dumps(payload)
            row.period_return_source_sha256 = hashlib.sha256(row.period_return_source_json.encode()).hexdigest()
    before = read(db)
    result = collector.collect_current(fcodes=[CODE])
    assert result["failed_count"] == result["blocking_count"] == 1
    assert result["screening_blocked"] is True and result["superseded_refresh_fcodes"] == []
    after = read(db)
    assert {key: after[key] for key in BUSINESS_FIELDS} == {key: before[key] for key in BUSINESS_FIELDS}


@pytest.mark.parametrize("damage", ["missing_date", "future_date", "invalid_number", "business_failure", "transport"])
def test_invalid_refresh_is_not_superseded_just_because_the_stored_snapshot_is_valid(tmp_path, clock, damage):
    collector, db, client = make_collector(tmp_path)
    response = deepcopy(FIXTURE["observations"][0]["response"])
    if damage == "missing_date": response["Expansion"]["TIME"] = ""
    if damage == "future_date": response["Expansion"]["TIME"] = "2099-01-01"
    if damage == "invalid_number": response["Datas"][0]["syl"] = "NaN"
    if damage == "business_failure": response.update(Success=False, ErrCode=61136)
    client.response = ConnectionError("isolated transport failure") if damage == "transport" else response
    before = read(db)
    result = collector.collect_current(fcodes=[CODE])
    assert result["blocking_count"] == 1 and result["superseded_refresh_fcodes"] == []
    after = read(db)
    assert {key: after[key] for key in BUSINESS_FIELDS} == {key: before[key] for key in BUSINESS_FIELDS}


@pytest.mark.parametrize("damage", ["nav_date", "nav_value", "source_hash", "row_return"])
def test_current_input_changes_revoke_the_exemption_during_cooldown(tmp_path, clock, damage):
    collector, db, client = make_collector(tmp_path)
    assert collector.collect_current(fcodes=[CODE])["superseded_refresh_fcodes"] == [CODE]
    with db.get_session() as session:
        row = session.get(FundReturns, CODE)
        if damage == "nav_date": row.latest_nav_date = date(2026, 10, 13)
        if damage == "nav_value": row.latest_nav = 999
        if damage == "source_hash": row.period_return_source_sha256 = "0" * 64
        if damage == "row_return": row.return_1w = None
    result = PeriodReturnCollector(config={}, db_manager=db, api_client=client).collect_current(fcodes=[CODE])
    assert result["request_count"] == 0 and result["cooldown_count"] == 1
    assert result["blocking_count"] == 1 and result["superseded_refresh_fcodes"] == []


@pytest.mark.parametrize("change", ["date", "value"])
def test_nav_change_during_request_prevents_the_superseded_exemption(tmp_path, clock, change):
    collector, db, client = make_collector(tmp_path)

    def advance_nav():
        with db.get_session() as session:
            row = session.get(FundReturns, CODE)
            if change == "date":
                row.latest_nav_date = date(2026, 10, 13)
            else:
                # Even a verified newer same-date rank correction cannot
                # validate the NAV observed at this request's start.
                row.latest_nav = 999
                payload = json.loads(row.period_return_source_json)
                payload["DWJZ"] = "999"
                row.period_return_source_json = json.dumps(payload)
                row.period_return_source_sha256 = hashlib.sha256(row.period_return_source_json.encode()).hexdigest()

    client.before_response = advance_nav
    result = collector.collect_current(fcodes=[CODE])
    assert result["blocking_count"] == 1 and result["superseded_refresh_fcodes"] == []


@pytest.mark.parametrize("buyable,reason", [(False, "not_buyable"), (None, "buyability_unknown")])
def test_superseded_refresh_does_not_bypass_per_fund_publication_gates(tmp_path, clock, buyable, reason):
    from backend.project_publish_decisions import append_project_decision_columns

    collector, db, _ = make_collector(tmp_path)
    assert collector.collect_current(fcodes=[CODE])["screening_blocked"] is False
    frame = pd.DataFrame([{**read(db), "is_buyable": buyable, "holding_days": 90}])
    frame = attach_period_return_status(frame, mask_unknown=True)
    assert frame["period_return_status"].tolist() == ["dated"]
    decisions = append_project_decision_columns(frame, decision_map={}, soft_warning_map={}, evaluated_at=clock.value)
    assert not decisions["基础门禁通过"].any()
    assert decisions["基础推荐原因代码"].str.contains(reason).all()


@pytest.mark.parametrize("damage", ["wrong_status", "wrong_code_list"])
def test_summary_rejects_unverified_superseded_claims(damage):
    result = {"schema_version": 2, "failed_count": 1, "cooldown_count": 0,
              "outcomes": [{"fcode": CODE, "status": "superseded_refresh_rejected",
                            "superseded_refresh_rejected": True}], "cooldown_outcomes": [],
              "nonblocking_missing_fcodes": [], "superseded_refresh_fcodes": [CODE]}
    if damage == "wrong_status": result["outcomes"][0]["status"] = "date_mismatch"
    else: result["superseded_refresh_fcodes"] = []
    with pytest.raises(ValueError, match="superseded_refresh"):
        period_collection_blocking_count(result)
