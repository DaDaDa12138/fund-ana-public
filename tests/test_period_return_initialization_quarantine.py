"""Real 11/07 provider reset, replayed only in isolated databases.

Negative mutations are validation fixtures, never production fund observations.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import Date, DateTime

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]
from collectors import period_return_collector as module
from collectors import fund_collector as fund_module
from collectors.period_return_collector import PeriodReturnCollector
from database.connection import DatabaseManager
from database.models import Fund, FundReturns
from utils.period_return_snapshot import (
    RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, RETURN_REJECTION_FIELDS,
    INITIALIZATION_QUARANTINE, attach_period_return_status,
    initialization_placeholder, period_collection_blocking_count,
    verified_initialization_quarantine,
)
from backend.collection_log_summary import classify_collection_log_severity

FIXTURE = json.loads((ROOT / "tests/fixtures/period_returns_initialization_reset_20261107.json").read_text())
CODE = "003572"
BUSINESS = (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, "latest_nav", "latest_nav_date", "last_updated_at")


def test_proof_inspector_works_in_a_fresh_process_without_collector_imports():
    script = """
import json, sys
from pathlib import Path
sys.path[:0] = [str(Path.cwd() / 'core')]
from utils.period_return_snapshot import initialization_placeholder
data = json.loads(Path('tests/fixtures/period_returns_initialization_reset_20261107.json').read_text())
for original in data['records']:
    code = original['fund']['fcode']
    response = next(item['response'] for item in data['observations'] if item['fcode'] == code)
    assert initialization_placeholder(response, estab_date=original['fund']['estab_date'],
                                      nav_date=original['returns']['latest_nav_date'])
"""
    result = subprocess.run([sys.executable, "-c", script], cwd=ROOT, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        value = datetime(2026, 11, 7, 23, 50)

        @classmethod
        def now(cls, tz=None):
            return cls.value

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(fund_module, "datetime", Clock)
    return Clock


def typed(model, raw):
    values = {k: deepcopy(v) for k, v in raw.items() if k in model.__table__.columns}
    for column in model.__table__.columns:
        if isinstance(values.get(column.name), str):
            if isinstance(column.type, DateTime):
                values[column.name] = datetime.fromisoformat(values[column.name])
            elif isinstance(column.type, Date):
                values[column.name] = date.fromisoformat(values[column.name])
    return values


def make_collector(tmp_path):
    db = DatabaseManager(str(tmp_path / "reset.sqlite"))
    for model in (Fund, FundReturns):
        model.__table__.create(db.engine)
    with db.get_session() as session:
        for row in FIXTURE["records"]:
            session.add(Fund(**typed(Fund, row["fund"])))
            session.add(FundReturns(**typed(FundReturns, row["returns"])))

    class Client:
        response = None

        def get(self, endpoint, params):
            assert endpoint == "/fundMNPeriodIncrease"
            value = self.response or next(r["response"] for r in FIXTURE["observations"] if r["fcode"] == params["FCODE"])
            if isinstance(value, Exception):
                raise value
            return deepcopy(value)

    client = Client()
    return PeriodReturnCollector(config={}, db_manager=db, api_client=client), db, client


def read(db, code=CODE):
    with db.get_session(readonly=True) as session:
        row = session.get(FundReturns, code)
        return {column.name: getattr(row, column.name) for column in FundReturns.__table__.columns}


def test_real_reset_isolated_without_changing_business_values_and_survives_cooldown(tmp_path, clock):
    collector, db, _ = make_collector(tmp_path)
    codes = sorted(row["fund"]["fcode"] for row in FIXTURE["records"])
    before = {code: read(db, code) for code in codes}
    result = collector.collect_current(fcodes=codes, force_refresh=True)
    assert result["request_count"] == result["failed_count"] == 2
    assert result["saved_fund_count"] == result["changed_count"] == result["blocking_count"] == 0
    assert result["screening_blocked"] is False and result["status"] == "degraded"
    assert result["initialization_placeholder_fcodes"] == codes
    for code in codes:
        after = read(db, code)
        assert after["period_return_last_status"] == INITIALIZATION_QUARANTINE
        assert verified_initialization_quarantine(after)
        assert {key: after[key] for key in BUSINESS} == {key: before[code][key] for key in BUSINESS}
    again = collector.collect_current(fcodes=codes)
    assert again["request_count"] == again["blocking_count"] == 0
    assert again["cooldown_count"] == 2 and again["initialization_placeholder_fcodes"] == codes
    frame = attach_period_return_status(pd.DataFrame([read(db, code) for code in codes]), mask_unknown=True)
    assert frame["period_return_status"].tolist() == ["upstream_response_quarantined"] * 2
    assert frame[list(RETURN_FIELDS)].isna().all().all()
    assert classify_collection_log_severity(["api_source_evidence=" + json.dumps(result)])["reason"] == "period_return_initialization_placeholder_quarantined"


@pytest.mark.parametrize("damage", ["business_failure", "missing_success", "missing_period", "duplicate", "missing_syl", "nonempty", "nonzero", "updating", "wrong_date", "wrong_estab", "future"])
def test_other_failures_and_true_numbers_do_not_get_placeholder_exemption(tmp_path, clock, damage):
    collector, db, client = make_collector(tmp_path)
    response = deepcopy(next(r["response"] for r in FIXTURE["observations"] if r["fcode"] == CODE))
    if damage == "business_failure": response["Success"] = False
    elif damage == "missing_success": response.pop("Success")
    elif damage == "missing_period": response["Datas"].pop(0)
    elif damage == "duplicate": response["Datas"][0] = deepcopy(response["Datas"][-1])
    elif damage == "missing_syl": response["Datas"][0].pop("syl")
    elif damage == "nonempty": response["Datas"][0]["syl"] = "0.00"
    elif damage == "nonzero": response["Datas"][-1]["syl"] = "0.01"
    elif damage == "updating": response["Expansion"]["ISUPDATING"] = True
    elif damage == "wrong_date": response["Expansion"]["TIME"] = "2026-11-02"
    elif damage == "wrong_estab": response["Expansion"]["ESTABDATE"] = "2016-12-08"
    elif damage == "future": response["Expansion"]["TIME"] = "2099-01-01"
    client.response = response
    before = read(db)
    result = collector.collect_current(fcodes=[CODE], force_refresh=True)
    assert result["blocking_count"] == 1 and result["screening_blocked"]
    assert result["initialization_placeholder_fcodes"] == []
    assert {key: read(db)[key] for key in BUSINESS} == {key: before[key] for key in BUSINESS}


@pytest.mark.parametrize("damage", ["proof_hash", "proof_identity", "nav_changed", "accepted_hash", "corrupt_json"])
def test_unverifiable_cooldown_proof_blocks_and_still_masks_fund(tmp_path, clock, damage):
    collector, db, _ = make_collector(tmp_path)
    collector.collect_current(fcodes=[CODE], force_refresh=True)
    with db.get_session() as session:
        row = session.get(FundReturns, CODE)
        if damage == "proof_hash": row.period_return_rejection_sha256 = "0" * 64
        elif damage == "nav_changed": row.latest_nav = 7
        elif damage == "accepted_hash": row.period_return_source_sha256 = "0" * 64
        elif damage == "corrupt_json": row.period_return_rejection_json = "[]"
        else:
            proof = json.loads(row.period_return_rejection_json); proof["fcode"] = "003868"
            row.period_return_rejection_json = json.dumps(proof)
            row.period_return_rejection_sha256 = hashlib.sha256(row.period_return_rejection_json.encode()).hexdigest()
    result = collector.collect_current(fcodes=[CODE])
    assert result["blocking_count"] == 1
    assert result["initialization_placeholder_fcodes"] == []
    assert attach_period_return_status(pd.DataFrame([read(db)]), mask_unknown=True)[list(RETURN_FIELDS)].isna().all().all()


def test_recovery_accepts_real_dated_response_and_clears_quarantine(tmp_path, clock):
    collector, db, client = make_collector(tmp_path)
    collector.collect_current(fcodes=[CODE], force_refresh=True)
    client.response = json.loads(read(db)["period_return_source_json"])["response"]
    result = collector.collect_current(fcodes=[CODE], force_refresh=True)
    assert result["saved_fund_count"] == 1 and result["blocking_count"] == 0
    assert all(read(db)[key] is None for key in RETURN_REJECTION_FIELDS)
    frame = attach_period_return_status(pd.DataFrame([read(db)]), mask_unknown=True)
    assert frame.iloc[0]["period_return_status"] == "dated"
    assert frame.iloc[0]["return_since"] == 6.36


def test_bulk_collector_recovery_clears_rejection_and_preserves_nulls(tmp_path, clock):
    collector, db, _ = make_collector(tmp_path)
    collector.collect_current(fcodes=[CODE], force_refresh=True)
    accepted = typed(FundReturns, FIXTURE["records"][0]["returns"])
    assert accepted["fcode"] == CODE
    accepted["period_return_observed_at"] = clock.value
    incoming = {key: accepted[key] for key in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, "fcode")}
    result = collector._save_funds([{"fcode": CODE, "returns": incoming}], return_stats=True)
    assert result["saved_count"] == result["returns_updates"] == 1
    assert result["failed_records"] == 0
    after = read(db)
    assert all(after[key] is None for key in RETURN_REJECTION_FIELDS)
    assert {key: after[key] for key in RETURN_FIELDS} == {key: accepted[key] for key in RETURN_FIELDS}
    assert attach_period_return_status(pd.DataFrame([after]), mask_unknown=True).iloc[0]["period_return_status"] == "dated"


def test_isolated_placeholder_remains_ineligible_for_formal_publication(tmp_path, clock):
    from backend.project_publish_decisions import append_project_decision_columns

    collector, db, _ = make_collector(tmp_path)
    assert collector.collect_current(fcodes=[CODE], force_refresh=True)["blocking_count"] == 0
    frame = attach_period_return_status(pd.DataFrame([read(db)]), mask_unknown=True)
    result = append_project_decision_columns(frame, decision_map={}, soft_warning_map={}, evaluated_at=clock.value)
    assert not result["基础门禁通过"].any()
    assert result["基础推荐原因代码"].str.contains("period_return_unready").all()


@pytest.mark.parametrize("change", ["date", "value"])
def test_request_nav_identity_change_prevents_placeholder_exemption(tmp_path, clock, change):
    collector, db, _ = make_collector(tmp_path)
    before = read(db)
    collector._last_period_return_response = next(r["response"] for r in FIXTURE["observations"] if r["fcode"] == CODE)
    expected_date = before["latest_nav_date"] - timedelta(days=1) if change == "date" else before["latest_nav_date"]
    expected_value = before["latest_nav"] + 1 if change == "value" else before["latest_nav"]
    result = collector._record_attempt(CODE, clock.value, "date_mismatch",
                                      expected_nav_date=expected_date, expected_nav_value=expected_value)
    assert result["status"] == "date_mismatch"
    assert result["initialization_placeholder_quarantined"] is False
    assert {key: read(db)[key] for key in BUSINESS} == {key: before[key] for key in BUSINESS}


def test_force_refresh_cannot_expand_to_unbounded_universe(tmp_path, clock):
    collector, _, _ = make_collector(tmp_path)
    with pytest.raises(ValueError, match="explicit_fcodes"):
        collector.collect_current(force_refresh=True)


def test_placeholder_summary_cannot_double_count_or_hide_missing_proof(tmp_path, clock):
    collector, _, _ = make_collector(tmp_path)
    result = collector.collect_current(fcodes=[CODE], force_refresh=True)
    result["outcomes"][0].pop("rejection_sha256")
    with pytest.raises(ValueError, match="unverified_initialization"):
        period_collection_blocking_count(result)
