"""Dated period returns: real saved inputs, isolated DBs, no live API calls.

Damaged responses below are explicit validation fixtures, not fund data.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import Date, DateTime, event

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from collectors import fund_collector as fund_module
from collectors import period_return_collector as module
from collectors.fund_collector import FundCollector
from collectors.nav_collector import NavCollector
from collectors.period_return_collector import PeriodReturnCollector
from cli.collection_commands import cmd_collect_period_returns
from database.connection import DatabaseManager
from database.migrations import migrate_add_columns
from database.models import Fund, FundReturns, FundNavHistory
from utils.period_return_snapshot import (
    RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, RETURN_REJECTION_FIELDS, attach_period_return_status,
    build_period_snapshot, has_dated_period_snapshot, select_return_update,
    period_collection_blocking_count,
)

FIXTURE = json.loads((ROOT / "tests/fixtures/period_returns_20260908.json").read_text())
FUNDS = {row["fcode"]: row for row in FIXTURE["funds"]}
RETURNS = {row["fcode"]: row for row in FIXTURE["returns"]}
T0 = datetime(2026, 9, 9, 1)
CODE = "004401"
MISSING_FIXTURE = json.loads((ROOT / "tests/fixtures/period_returns_missing_20260909.json").read_text())
LAG_FIXTURE = json.loads((ROOT / "tests/fixtures/period_returns_lag_20260909.json").read_text())


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        value = T0

        @classmethod
        def now(cls, tz=None):
            assert tz is None
            return cls.value

    monkeypatch.setattr(fund_module, "datetime", Clock)
    monkeypatch.setattr(module, "datetime", Clock)
    return Clock


def typed_row(model, source):
    row = {key: deepcopy(value) for key, value in source.items() if key in model.__table__.columns}
    for column in model.__table__.columns:
        value = row.get(column.name)
        if isinstance(value, str) and isinstance(column.type, DateTime):
            row[column.name] = datetime.fromisoformat(value)
        elif isinstance(value, str) and isinstance(column.type, Date):
            row[column.name] = date.fromisoformat(value)
    return row


def make_collector(tmp_path, *, codes=(CODE,), response=None, config=None, current_nav=True):
    db = DatabaseManager(str(tmp_path / "periods.sqlite"))
    for model in (Fund, FundReturns, FundNavHistory):
        model.__table__.create(db.engine)
    with db.get_session() as session:
        for code in codes:
            session.add(Fund(**typed_row(Fund, FUNDS[code])))
            row = typed_row(FundReturns, RETURNS[code])
            if current_nav:
                # Real current NAVs from the saved AutoStock response.
                actual = next(item for item in FIXTURE["autostock"] if item["code"] == code)
                row.update(latest_nav_date=date.fromisoformat(actual["netWorthDate"]),
                           latest_nav=float(actual["netWorth"]))
            session.add(FundReturns(**row))
    calls = []

    class ReplayClient:
        current = response

        def get(self, endpoint, params):
            assert endpoint == "/fundMNPeriodIncrease"
            assert set(params) == {"FCODE"}
            code = params["FCODE"]
            calls.append(code)
            if isinstance(self.current, BaseException):
                raise self.current
            return deepcopy(self.current if self.current is not None else FIXTURE["periods"][code]["response"])

    client = ReplayClient()
    collector = PeriodReturnCollector(config=config or {}, db_manager=db, api_client=client)
    return collector, db, client, calls


def read_row(db, code=CODE, model=FundReturns):
    with db.get_session(readonly=True) as session:
        row = session.get(model, code)
        return {column.name: getattr(row, column.name) for column in model.__table__.columns}


def period_snapshot(code=CODE):
    payload = FIXTURE["periods"][code]["response"]
    return build_period_snapshot(
        FundCollector._map_period_increase_returns(payload["Datas"]),
        as_of_date=payload["Expansion"]["TIME"],
        observed_at=datetime.fromisoformat(FIXTURE["periods"][code]["observation"]["observed_at"]),
        source="fundMNPeriodIncrease", source_payload={"fcode": code, "response": payload},
    )


def test_nullable_migration_is_idempotent_and_does_not_guess_legacy_dates(tmp_path):
    path = tmp_path / "legacy.sqlite"
    original = RETURNS[CODE]
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE fund_returns (" + ",".join('"' + key + '"' for key in original) + ")")
        conn.execute("INSERT INTO fund_returns VALUES (" + ",".join("?" for _ in original) + ")", tuple(original.values()))
    assert migrate_add_columns(str(path)) is True
    assert migrate_add_columns(str(path)) is True
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = dict(conn.execute("SELECT * FROM fund_returns").fetchone())
        assert {key: row[key] for key in original} == original
        added = set(row) - set(original)
        assert added == {
            *RETURN_PROVENANCE_FIELDS,
            *RETURN_REJECTION_FIELDS,
            "period_return_attempted_at",
            "period_return_last_status",
        }
        assert all(row[key] is None for key in added)


def test_direct_period_cli_migrates_legacy_schema_before_request(tmp_path, monkeypatch, clock):
    from core import main as cli

    collector, db, _, calls = make_collector(tmp_path)
    original = read_row(db)
    with sqlite3.connect(db.db_path) as conn:
        for column in (*RETURN_PROVENANCE_FIELDS, *RETURN_REJECTION_FIELDS,
                       "period_return_attempted_at", "period_return_last_status"):
            conn.execute(f'ALTER TABLE fund_returns DROP COLUMN "{column}"')
    monkeypatch.setenv("FUND_ANA_SUBPROCESS_LOG_DIR", str(tmp_path / "evidence"))

    def construct_after_migration(config):
        upgraded = read_row(db)
        assert all(upgraded[key] is None for key in RETURN_PROVENANCE_FIELDS)
        assert {key: upgraded[key] for key in RETURN_FIELDS} == {key: original[key] for key in RETURN_FIELDS}
        assert calls == []
        return collector

    monkeypatch.setattr(module, "PeriodReturnCollector", construct_after_migration)
    config = {"database": {"path": str(db.db_path)}}
    assert cli.cmd_collect_period_returns(SimpleNamespace(fcodes=CODE), config) == 0
    assert calls == [CODE]
    after = read_row(db)
    assert after["period_return_date"] == date(2026, 9, 8)
    assert after["return_3m"] == 0.53
    # Repeated CLI startup is idempotent and does not consume another request.
    monkeypatch.setattr(module, "PeriodReturnCollector", lambda config: collector)
    assert cli.cmd_collect_period_returns(SimpleNamespace(fcodes=CODE), config) == 0
    assert calls == [CODE]
    assert read_row(db) == after


def test_direct_cli_stops_before_collector_on_failed_migration(tmp_path, monkeypatch):
    from core import main as cli
    from database import migrations

    _, db, _, calls = make_collector(tmp_path)
    before = read_row(db)

    def failed_columns(_path):
        return False

    monkeypatch.setattr(migrations, "migrate_add_columns", failed_columns)
    monkeypatch.setattr(module, "PeriodReturnCollector", lambda **kwargs: pytest.fail("collector created before schema ready"))
    assert cli.cmd_collect_period_returns(SimpleNamespace(fcodes=CODE), {"database": {"path": str(db.db_path)}}) == 1
    assert calls == [] and read_row(db) == before


@pytest.mark.parametrize("failed_step", [
    "migrate_ensure_all_tables", "migrate_add_type_code_fields", "migrate_add_columns",
    "migrate_add_screening_indexes", "migrate_add_risk_metric_indexes",
    "migrate_drop_redundant_nav_fcode_index", "migrate_add_screening_run_storage_stats",
    "migrate_add_screening_pick_metric_snapshots", "migrate_add_screening_decision_summaries",
])
def test_migration_runner_preserves_substep_failure(tmp_path, monkeypatch, failed_step):
    from database import migrations

    names = ["migrate_ensure_all_tables", "migrate_add_type_code_fields", "migrate_add_columns",
             "migrate_add_screening_indexes", "migrate_add_risk_metric_indexes",
             "migrate_drop_redundant_nav_fcode_index", "migrate_add_screening_run_storage_stats",
             "migrate_add_screening_pick_metric_snapshots", "migrate_add_screening_decision_summaries"]
    calls = []
    for name in names:
        def step(path, _name=name):
            calls.append(_name)
            return _name != failed_step
        step.__name__ = name
        monkeypatch.setattr(migrations, name, step)
    with pytest.raises(RuntimeError, match=failed_step):
        migrations.run_all_migrations(str(tmp_path / "migration.sqlite"))
    assert calls == names[:names.index(failed_step) + 1]


def test_web_bootstrap_does_not_cache_failed_migration(tmp_path, monkeypatch):
    from backend.schema_bootstrap import ensure_database_schema, reset_cache_for_tests
    from database import migrations

    settings = SimpleNamespace(database_path=tmp_path / "bootstrap.sqlite", project_root=ROOT, core_root=ROOT / "core")
    original = migrations.migrate_add_columns

    def failed_columns(_path):
        return False

    reset_cache_for_tests()
    try:
        monkeypatch.setattr(migrations, "migrate_add_columns", failed_columns)
        with pytest.raises(RuntimeError, match="failed_columns"):
            ensure_database_schema(settings)
        monkeypatch.setattr(migrations, "migrate_add_columns", original)
        assert ensure_database_schema(settings).ok is True
    finally:
        reset_cache_for_tests()


def test_real_nav_write_does_not_certify_old_period_returns(tmp_path, clock):
    _, db, client, _ = make_collector(tmp_path, current_nav=False)
    before = read_row(db)
    nav = NavCollector(config={}, db_manager=db, api_client=client)
    assert nav._save_nav_records(CODE, [deepcopy(FIXTURE["new_nav_004401"])]) == 1
    after = read_row(db)
    assert after["latest_nav_date"] == date(2026, 9, 8)
    assert after["latest_nav"] == float(FIXTURE["new_nav_004401"]["DWJZ"])
    assert {key: after[key] for key in RETURN_FIELDS} == {key: before[key] for key in RETURN_FIELDS}
    assert after["return_3m"] == 1.61 and after["return_6m"] == 3.03
    assert after["period_return_date"] is None and not has_dated_period_snapshot(after)


def test_missing_daily_change_does_not_keep_old_day_change(tmp_path, clock):
    _, db, client, _ = make_collector(tmp_path, current_nav=False)
    damaged = deepcopy(FIXTURE["new_nav_004401"])
    damaged["JZZZL"] = "--"  # Explicit isolated missing-value fixture.
    assert NavCollector(config={}, db_manager=db, api_client=client)._save_nav_records(CODE, [damaged]) == 1
    assert read_row(db)["daily_growth"] is None


def test_real_period_correction_commits_complete_snapshot_and_restart_skips(tmp_path, clock):
    collector, db, client, calls = make_collector(tmp_path)
    first = collector.collect_current()
    row = read_row(db)
    assert first["saved_fund_count"] == first["changed_count"] == 1
    assert first["failed_count"] == first["pending_count"] == 0
    assert row["return_3m"] == 0.53 and row["return_6m"] == 1.67
    assert row["period_return_date"] == date(2026, 9, 8)
    assert has_dated_period_snapshot(row, verify_payload=True)
    assert row["period_return_observed_at"] == T0
    assert json.loads(row["period_return_source_json"])["response"] == FIXTURE["periods"][CODE]["response"]
    restarted = PeriodReturnCollector(config={}, db_manager=db, api_client=client)
    second = restarted.collect_current()
    assert second["fresh_count"] == 1 and second["status"] == "skipped"
    assert calls == [CODE] and second["request_count"] == 0
    assert restarted.response_observations == []


@pytest.mark.parametrize("damage", ["no_date", "wrong_date", "future_date", "empty", "duplicate", "nonfinite", "boolean", "transport"])
def test_invalid_response_preserves_values_and_durable_failure_cooldown(tmp_path, clock, damage):
    response = deepcopy(FIXTURE["periods"][CODE]["response"])
    if damage == "no_date": response["Expansion"].pop("TIME")
    elif damage == "wrong_date": response["Expansion"]["TIME"] = "2026-09-07"
    elif damage == "future_date": response["Expansion"]["TIME"] = "2026-09-10"
    elif damage == "empty": response["Datas"] = []
    elif damage == "duplicate": response["Datas"].append(deepcopy(response["Datas"][0]))
    elif damage == "nonfinite": response["Datas"][0]["syl"] = "NaN"
    elif damage == "boolean": response["Datas"][0]["syl"] = True
    else: response = ConnectionError("isolated transport failure")
    collector, db, client, calls = make_collector(tmp_path, response=response)
    before = read_row(db)
    first = collector.collect_current()
    assert first["failed_count"] == 1 and first["saved_fund_count"] == 0
    after = read_row(db)
    keys = (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, "latest_nav_date", "latest_nav")
    assert {key: after[key] for key in keys} == {key: before[key] for key in keys}
    restarted = PeriodReturnCollector(config={}, db_manager=db, api_client=client)
    second = restarted.collect_current()
    assert second["cooldown_count"] == second["pending_count"] == 1
    assert second["request_count"] == 0 and second["status"] == "degraded"
    clock.value += timedelta(hours=12)
    client.current = None
    third = restarted.collect_current()
    assert third["saved_fund_count"] == 1 and third["status"] == "batch_complete"
    assert calls == [CODE, CODE]


def test_complete_new_snapshot_clears_missing_period_and_preserves_explicit_zero(tmp_path, clock):
    response = deepcopy(FIXTURE["periods"][CODE]["response"])
    response["Datas"] = [row for row in response["Datas"] if row["title"] != "5N"]
    response["Datas"][0]["syl"] = "0"  # Explicit isolated zero/missing fixture.
    collector, db, _, _ = make_collector(tmp_path, response=response)
    assert read_row(db)["return_5y"] is not None
    assert collector.collect_current()["saved_fund_count"] == 1
    row = read_row(db)
    assert row["return_5y"] is None and row["return_1w"] == 0


@pytest.mark.parametrize("fail_attempt", [True, False])
def test_commit_failure_never_counts_success_or_leaks_partial_snapshot(tmp_path, clock, fail_attempt):
    collector, db, _, calls = make_collector(tmp_path)
    before = read_row(db)
    commits = []

    def reject(session):
        commits.append(1)
        if len(commits) == (1 if fail_attempt else 2):
            raise RuntimeError("isolated commit failure")

    event.listen(db.Session.session_factory, "before_commit", reject)
    try:
        summary = collector.collect_current()
    finally:
        event.remove(db.Session.session_factory, "before_commit", reject)
    assert summary["saved_fund_count"] == summary["changed_count"] == 0
    assert summary["failed_count"] == 1
    assert calls == ([] if fail_attempt else [CODE])
    after = read_row(db)
    assert {key: after[key] for key in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)} == {
        key: before[key] for key in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)}
    assert after["last_updated_at"] == before["last_updated_at"]


def test_nav_and_period_dates_advance_independently(tmp_path, clock):
    collector, db, _, _ = make_collector(tmp_path)
    before = read_row(db)
    incoming = {**period_snapshot(), "fcode": CODE,
                "latest_nav_date": date(2026, 9, 7), "latest_nav": RETURNS[CODE]["latest_nav"]}
    saved = collector._save_funds([{"fcode": CODE, "returns": incoming}], return_stats=True)
    after = read_row(db)
    assert saved["saved_count"] == 1 and saved["failed_records"] == 0
    assert after["latest_nav_date"] == before["latest_nav_date"]
    assert after["latest_nav"] == before["latest_nav"]
    assert after["period_return_date"] == date(2026, 9, 8) and after["return_3m"] == 0.53


def test_partial_dated_mapping_fails_before_fund_metadata_is_queued(tmp_path, clock):
    collector, db, _, _ = make_collector(tmp_path)
    before = read_row(db, model=Fund)
    incoming = {**period_snapshot(), "fcode": CODE}
    incoming.pop("return_5y")
    result = collector._save_funds([{"fcode": CODE, "shortname": "isolated rejected change", "returns": incoming}], return_stats=True)
    assert result["saved_count"] == 0 and result["failed_records"] == 1
    assert read_row(db, model=Fund) == before
    assert read_row(db)["period_return_date"] is None


def test_same_date_observation_order_uses_instant_and_unknown_rows_are_masked():
    newer = period_snapshot()
    older = {**newer, "period_return_observed_at": "2026-09-08T14:00:00Z"}
    assert "period_return_date" not in select_return_update(newer, older)
    assert select_return_update(older, newer)["return_3m"] == newer["return_3m"]
    after_midnight = {**newer, "period_return_date": date(2026, 9, 9),
                      "period_return_observed_at": "2026-09-08T17:00:00Z"}
    assert has_dated_period_snapshot(after_midnight)
    frame = pd.DataFrame([{**RETURNS[CODE], "period_return_date": pd.NaT}, {**newer, "fcode": CODE}])
    masked = attach_period_return_status(frame.copy(), mask_unknown=True)
    assert masked["period_return_status"].tolist() == ["unknown", "dated"]
    assert pd.isna(masked.loc[0, "return_3m"]) and masked.loc[1, "return_3m"] == 0.53


def test_bounded_batches_rotate_and_reused_collector_does_not_reuse_observations(tmp_path, clock):
    codes = ("000406", "000407", CODE)
    collector, db, _, calls = make_collector(tmp_path, codes=codes,
        config={"collection": {"period_return": {"max_targets_per_run": 1}}})
    for remaining in (2, 1, 0):
        result = collector.collect_current()
        assert result["saved_fund_count"] == 1 and result["pending_count"] == remaining
        assert len(collector.response_observations) == 1
    assert set(calls) == set(codes) and len(calls) == 3
    assert collector.collect_current()["request_count"] == 0
    assert collector.response_observations == []


def test_cli_artifact_and_runtime_semantics_agree_for_success_and_cooldown(tmp_path, clock, monkeypatch, capsys):
    from backend.collection_log_summary import classify_collection_log_severity
    from scripts.audit_data_truthfulness import _stdout_result_semantics

    collector, _, client, _ = make_collector(tmp_path, response={"Success": True, "Datas": []})
    monkeypatch.setenv("FUND_ANA_SUBPROCESS_LOG_DIR", str(tmp_path / "evidence"))
    for expected_code in (2, 2, 0):
        if expected_code == 0:
            clock.value += timedelta(hours=12)
            client.current = None
        code = cmd_collect_period_returns(SimpleNamespace(fcodes=CODE), {}, logger=logging.getLogger(__name__),
            ensure_database_initialized=lambda _: "isolated", emit_command_error=lambda *args: 1,
            fund_collector_cls=lambda **kwargs: collector)
        stdout = capsys.readouterr().out
        assert code == expected_code
        path = tmp_path / "stdout.log"
        path.write_text(stdout)
        result, blockers, warnings = _stdout_result_semantics("collect-period-returns", str(path))
        assert blockers == []
        summary = result["business_summary"]
        assert classify_collection_log_severity(stdout.splitlines())["degraded"] == (expected_code == 2)
        assert hashlib.sha256(Path(summary["response_artifact"]).read_bytes()).hexdigest() == summary["response_artifact_sha256"]
        assert bool(warnings) == (expected_code == 2)


def test_real_missing_returns_stay_missing_without_blocking_other_funds(tmp_path, clock, monkeypatch, capsys):
    from backend.collection_log_summary import classify_collection_log_severity
    from scripts.audit_data_truthfulness import _stdout_result_semantics

    db = DatabaseManager(str(tmp_path / "real_missing.sqlite"))
    for model in (Fund, FundReturns):
        model.__table__.create(db.engine)
    with db.get_session() as session:
        for row in MISSING_FIXTURE["funds"]:
            session.add(Fund(**typed_row(Fund, row)))
        for row in MISSING_FIXTURE["returns"]:
            session.add(FundReturns(**typed_row(FundReturns, row)))
    calls = []

    class Client:
        def get(self, endpoint, params):
            assert endpoint == "/fundMNPeriodIncrease"
            code = params["FCODE"]
            calls.append(code)
            return deepcopy(MISSING_FIXTURE["periods"][code]["response"])

    codes = sorted(MISSING_FIXTURE["periods"])
    collector = PeriodReturnCollector(config={}, db_manager=db, api_client=Client())
    monkeypatch.setenv("FUND_ANA_SUBPROCESS_LOG_DIR", str(tmp_path / "evidence"))
    for expected_requests in (4, 0):
        exit_code = cmd_collect_period_returns(SimpleNamespace(fcodes=",".join(codes)), {},
            logger=logging.getLogger(__name__), ensure_database_initialized=lambda _: "isolated",
            emit_command_error=lambda *args: 1, fund_collector_cls=lambda **kwargs: collector)
        stdout = capsys.readouterr().out
        assert exit_code == 0  # Safe handoff, while availability remains degraded.
        path = tmp_path / "stdout.log"
        path.write_text(stdout)
        result, blockers, warnings = _stdout_result_semantics("collect-period-returns", str(path))
        assert blockers == [] and warnings
        summary = result["business_summary"]
        assert summary["request_count"] == expected_requests
        assert summary["failed_count"] == expected_requests
        assert summary["cooldown_count"] == 4 - expected_requests
        assert summary["saved_fund_count"] == summary["changed_count"] == 0
        assert summary["pending_count"] == 4 and summary["status"] == "degraded"
        assert summary["screening_blocked"] is False and summary["blocking_count"] == 0
        assert summary["nonblocking_missing_fcodes"] == codes
        health = classify_collection_log_severity(stdout.splitlines())
        assert health["degraded"] is True
        assert health["reason"] == "period_return_unavailable_without_returns"
        for code in codes:
            row = read_row(db, code)
            assert all(row[field] is None for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS))
            assert not has_dated_period_snapshot(row)
    assert sorted(calls) == codes


@pytest.mark.parametrize("status", ["request_failed", "date_mismatch", "write_or_validation_failed"])
def test_nonmissing_failures_cannot_claim_the_missing_only_exemption(status):
    summary = {"schema_version": 2, "failed_count": 1, "cooldown_count": 0,
        "outcomes": [{"fcode": CODE, "status": status, "unavailable_without_returns": True}],
        "cooldown_outcomes": [], "nonblocking_missing_fcodes": [CODE]}
    with pytest.raises(ValueError, match="nonmissing_failure"):
        period_collection_blocking_count(summary)


def test_zero_is_a_prior_return_and_cannot_claim_missing_only_exemption():
    record = {field: None for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)}
    record["period_return_last_status"] = "missing"
    assert PeriodReturnCollector._unavailable_without_returns(record)
    record["return_1w"] = 0  # Isolated boundary case: zero is observed data.
    assert not PeriodReturnCollector._unavailable_without_returns(record)


def make_lag_collector(tmp_path, clock):
    clock.value = datetime(2026, 9, 9, 6)
    db = DatabaseManager(str(tmp_path / "real_lag.sqlite"))
    for model in (Fund, FundReturns):
        model.__table__.create(db.engine)
    with db.get_session() as session:
        for row in LAG_FIXTURE["funds"]:
            session.add(Fund(**typed_row(Fund, row)))
        for row in LAG_FIXTURE["returns"]:
            values = typed_row(FundReturns, row)
            # Reset only the isolated retry state to exercise the first call.
            # All business numbers, dates and source payloads are unchanged.
            values.update(period_return_attempted_at=None, period_return_last_status=None)
            session.add(FundReturns(**values))
    calls = []

    class Client:
        damaged_response = None

        def get(self, endpoint, params):
            assert endpoint == "/fundMNPeriodIncrease"
            code = params["FCODE"]
            calls.append(code)
            return deepcopy(self.damaged_response or LAG_FIXTURE["periods"][code]["response"])

    client = Client()
    return PeriodReturnCollector(config={}, db_manager=db, api_client=client), db, client, calls


def test_verified_real_lag_is_isolated_from_scoring_and_publication(tmp_path, clock):
    from backend.project_publish_decisions import append_project_decision_columns

    collector, db, _, calls = make_lag_collector(tmp_path, clock)
    codes = sorted(LAG_FIXTURE["periods"])
    before = {code: read_row(db, code) for code in codes}
    for requested in (3, 0):
        summary = collector.collect_current()
        assert summary["request_count"] == summary["failed_count"] == requested
        assert summary["saved_fund_count"] == 0 and summary["status"] == "degraded"
        assert summary["pending_count"] == 3 and summary["blocking_count"] == 0
        assert summary["screening_blocked"] is False
        assert summary["quarantined_date_lag_fcodes"] == codes
        assert summary["nonblocking_missing_fcodes"] == []
        for code in codes:
            after = read_row(db, code)
            assert after["period_return_last_status"] == "date_mismatch_quarantined"
            assert {field: after[field] for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)} == {
                field: before[code][field] for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)}
            assert after["last_updated_at"] == before[code]["last_updated_at"]
    assert sorted(calls) == codes
    frame = pd.DataFrame([read_row(db, code) for code in codes])
    raw = attach_period_return_status(frame.copy())
    masked = attach_period_return_status(frame.copy(), mask_unknown=True)
    assert raw["return_3m"].notna().all()  # Kept for audit and dated display.
    assert masked[list(RETURN_FIELDS)].isna().all().all()
    assert masked["period_return_status"].tolist() == ["date_mismatch"] * 3
    decisions = append_project_decision_columns(masked, decision_map={}, soft_warning_map={}, evaluated_at=clock.value)
    assert not decisions["基础门禁通过"].any()
    assert decisions["基础推荐原因代码"].str.contains("period_return_unready").all()
    assert decisions["数据就绪状态"].eq("待补齐").all()


@pytest.mark.parametrize("damage", ["missing_date", "future_date", "value_conflict", "broken_stored_hash"])
def test_unverified_lag_remains_blocking_and_never_updates_returns(tmp_path, clock, damage):
    collector, db, client, _ = make_lag_collector(tmp_path, clock)
    code = sorted(LAG_FIXTURE["periods"])[0]
    damaged = deepcopy(LAG_FIXTURE["periods"][code]["response"])
    if damage == "missing_date":
        damaged["Expansion"]["TIME"] = ""
    elif damage == "future_date":
        damaged["Expansion"]["TIME"] = "2099-01-01"
    elif damage == "value_conflict":
        damaged["Datas"][0]["syl"] = "999"  # Deliberately invalid test input.
    else:
        with db.get_session() as session:
            session.get(FundReturns, code).period_return_source_sha256 = "0" * 64
    client.damaged_response = damaged
    before = read_row(db, code)
    summary = collector.collect_current(fcodes=[code])
    assert summary["failed_count"] == summary["blocking_count"] == 1
    assert summary["screening_blocked"] is True
    assert summary["quarantined_date_lag_fcodes"] == []
    after = read_row(db, code)
    assert {field: after[field] for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)} == {
        field: before[field] for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)}


@pytest.mark.parametrize("dated", [False, True])
def test_risk_audit_sql_keeps_the_same_independent_date_and_signature_as_calculation(tmp_path, clock, dated):
    from analyzers.risk_analyzer import RiskAnalyzer
    from scripts import audit_data_truthfulness as audit
    from utils import build_nav_snapshot_signature

    collector, db, _, _ = make_collector(tmp_path, current_nav=False)
    if dated:
        # Real Sep 8 returns alongside the earlier real Sep 7 NAV, exercising
        # the date difference that the old audit projection silently erased.
        collector._save_funds([{"fcode": CODE, "returns": {"fcode": CODE, **period_snapshot()}}])
    with db.get_session(readonly=True) as session:
        production = RiskAnalyzer(config={}, db_manager=db)._load_api_returns_map(session, [CODE])[CODE]
    with sqlite3.connect(db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        columns = {row[1] for row in conn.execute("PRAGMA table_info(fund_returns)")}
        fields = ", ".join("fr." + field for field in audit.RISK_RECALCULATION_RETURN_FIELDS)
        row = conn.execute(f"SELECT {fields}{audit._risk_recalculation_return_provenance_select(columns)} FROM fund_returns fr").fetchone()
    restored = audit._risk_recalculation_return_snapshot(row)
    assert build_nav_snapshot_signature(restored) == build_nav_snapshot_signature(production)
    restored_date = restored.get("period_return_date") if has_dated_period_snapshot(restored, verify_payload=True) else None
    assert restored_date == (str(production["api_return_date"]) if production["api_return_date"] else None)
    assert restored_date == ("2026-09-08" if dated else None)
    assert restored["latest_nav_date"] == "2026-09-07"


@pytest.mark.parametrize("window,changed", [("daily", True), ("daily", False), ("intraday", True)])
def test_missing_only_refresh_keeps_warning_after_screening_or_skip(tmp_path, monkeypatch, window, changed):
    from unittest.mock import Mock
    from backend import scheduler as scheduler_module

    # Terminal contract for the four real missing-response fixtures above;
    # this scheduler test does not claim a business publication occurred.
    codes = sorted(MISSING_FIXTURE["periods"])
    summary = {"schema_version": 2, "task": "collect-period-returns", "status": "degraded",
        "target_count": 0, "saved_fund_count": 0, "failed_count": 0, "cooldown_count": len(codes),
        "pending_count": len(codes), "outcomes": [],
        "cooldown_outcomes": [{"fcode": code, "status": "missing", "unavailable_without_returns": True} for code in codes],
        "nonblocking_missing_fcodes": codes, "blocking_count": 0, "screening_blocked": False}
    scheduler = scheduler_module.ProjectScheduler.__new__(scheduler_module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(database_path=tmp_path / "unused.db", latest_report_file=tmp_path / "latest.json",
        reports_dir=tmp_path / "reports", effective_nav_coverage_ratio=0.9, effective_nav_weekday_only=True)
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._log = lambda line: None
    scheduler._build_host_runtime_snapshot = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._build_data_quality_audits_compat = lambda **kwargs: {}
    scheduler._daily_refresh_lineage_rescreen_required = lambda **kwargs: {"required": False}
    scheduler._build_maintenance_data_change_summary = lambda **kwargs: {"changed": False}
    snapshot = SimpleNamespace(candidate_date="2026-09-08", signature="isolated", to_dict=lambda: {"signature": "isolated"})
    monkeypatch.setattr(scheduler_module, "load_effective_nav_snapshot", lambda *a, **kw: snapshot)
    monkeypatch.setattr(scheduler_module, "load_latest_screened_db_snapshot", lambda *a: {})
    decisions = iter([changed, False if window == "intraday" else changed])
    monkeypatch.setattr(scheduler_module, "screening_snapshot_changed", lambda *a: (next(decisions), "isolated_check"))
    monkeypatch.setattr(scheduler_module, "run_autostock_nav_tail_fill", Mock(return_value={}))

    def logged_step(*args, **kwargs):
        log = kwargs.get("log") or args[-1]
        log("api_source_evidence=" + json.dumps(summary))
        for number in range(250):
            log(f"isolated subsequent progress {number}")
        return {"run_id": "isolated_handoff", "counts": {"final_count": None}}

    monkeypatch.setattr(scheduler_module, "run_project_pipeline", logged_step)
    workflow = Mock(side_effect=logged_step)
    scheduler._run_workflow_in_subprocess = workflow
    result = scheduler._execute_scheduled_refresh(task_name="isolated_missing", refresh_window=window,
        nav_history_sync_enabled=False, nav_history_sync_limit=1,
        long_term_history_enabled=False, long_term_history_limit=1)
    assert result["status"] == "degraded" and result["degraded"] is True
    assert result["period_return_collection"] == summary
    assert result["warning_reason"] == "period_return_unavailable_without_returns"
    assert result["screening_triggered"] is changed
    assert workflow.call_count == int(changed)
    assert all("api_source_evidence=" not in line for line in result["logs_tail"])


def comparison_row(code, *, dated):
    actual = next(item for item in FIXTURE["autostock"] if item["code"] == code)
    values = period_snapshot(code) if dated else RETURNS[code]
    row = {"基金代码": code, "基金名称": FUNDS[code]["shortname"], "基金类型": FUNDS[code]["ftype"],
           "最新净值日期": actual["netWorthDate"], "最新单位净值": actual["netWorth"]}
    mapping = {"return_1w": "近1周(%)", "return_1m": "近1月(%)", "return_3m": "近3月(%)",
               "return_6m": "近6月(%)", "return_1y": "近1年(%)"}
    row.update({column: values[field] for field, column in mapping.items()})
    if dated:
        row["区间收益基准日"] = str(values["period_return_date"])
    return pd.Series(row), actual


@pytest.mark.parametrize("code", sorted(FUNDS))
def test_seven_real_same_date_api_snapshots_compare_without_false_period_conflict(code):
    from backend import report_builder
    from backend.config import load_settings

    _, _, compare = report_builder._load_autostock_runtime_for_report(load_settings())
    config = report_builder._load_autostock_cross_validation_config_for_report()
    row, actual = comparison_row(code, dated=True)
    result = compare(row, actual, config)
    assert result["passed"], result
    assert result["hard_reasons"] == []


def test_004401_nav_date_advance_alone_cannot_clear_stale_return_conflict():
    from backend import report_builder
    from backend.config import load_settings

    _, _, compare = report_builder._load_autostock_runtime_for_report(load_settings())
    config = report_builder._load_autostock_cross_validation_config_for_report()
    stale, actual = comparison_row(CODE, dated=False)
    for nav_date in ("2026-09-07", "2026-09-08"):
        stale["最新净值日期"] = nav_date
        result = compare(stale, actual, config)
        assert not result["passed"]
        assert {"period_return_date_missing", "return_3m_mismatch", "return_6m_mismatch"} <= set(result["hard_reasons"])
    dated, _ = comparison_row(CODE, dated=True)
    dated["区间收益基准日"] = "2026-09-07"  # Explicit isolated date-mismatch fixture.
    assert "period_return_date_mismatch" in compare(dated, actual, config)["hard_reasons"]


def test_risk_api_return_date_requires_provenance_and_is_independent_of_nav(tmp_path, clock):
    from analyzers.risk_analyzer import RiskAnalyzer

    collector, db, _, _ = make_collector(tmp_path)
    with db.get_session(readonly=True) as session:
        before = RiskAnalyzer._load_api_returns_map(session, [CODE])[CODE]
    assert before["latest_nav_date"] == date(2026, 9, 8) and before["api_return_date"] is None
    assert collector.collect_current()["saved_fund_count"] == 1
    with db.get_session(readonly=True) as session:
        after = RiskAnalyzer._load_api_returns_map(session, [CODE])[CODE]
    assert after["api_return_date"] == date(2026, 9, 8)
    with db.get_session() as session:
        session.get(FundReturns, CODE).period_return_source_sha256 = "0" * 64
    with db.get_session(readonly=True) as session:
        damaged = RiskAnalyzer._load_api_returns_map(session, [CODE])[CODE]
    assert damaged["api_return_date"] is None


def test_non_dominant_nav_fund_period_correction_changes_automation_signature(tmp_path, clock):
    from backend.effective_nav import _build_signature

    collector, db, _, _ = make_collector(tmp_path, codes=("000406", CODE))
    with sqlite3.connect(db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        # The dominant candidate is a different stored NAV date. This tests
        # the signature scope, not a synthetic fund performance calculation.
        conn.execute("UPDATE fund_returns SET latest_nav_date='2026-09-07' WHERE fcode='000406'")
        before = _build_signature(conn, candidate_date="2026-09-07")
    assert collector.collect_current(fcodes=[CODE])["saved_fund_count"] == 1
    with sqlite3.connect(db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        after = _build_signature(conn, candidate_date="2026-09-07")
        assert before != after
        conn.execute("UPDATE fund_returns SET period_return_observed_at='2026-09-09 02:00:00' WHERE fcode=?", (CODE,))
        assert _build_signature(conn, candidate_date="2026-09-07") == after


@pytest.mark.parametrize("profile,collection,screening", [
    ("full", True, True), ("daily", True, False), ("weekly", True, False),
    ("intraday", True, True), ("full", False, True), ("full", False, False),
])
def test_every_executing_pipeline_has_one_period_step_before_consumers(tmp_path, monkeypatch, profile, collection, screening):
    from dataclasses import replace
    from backend import core_runner
    from backend.config import load_settings

    settings = replace(load_settings(), database_path=tmp_path / "unused.sqlite")
    monkeypatch.setattr(core_runner, "should_collect_managers", lambda _: False)
    monkeypatch.setattr(core_runner, "should_calculate_risk", lambda _: True)
    monkeypatch.setattr(core_runner, "should_refresh_holding_period", lambda _: False)
    commands = core_runner.build_core_commands(settings, {"run_collection": collection, "run_screening": screening,
                                                         "collection_profile": profile})
    names = [step.name for step in commands]
    assert names.count("collect-period-returns") == int(collection or screening)
    if collection or screening:
        period_index = names.index("collect-period-returns")
        assert all(period_index < names.index(name) for name in ("calculate-risk", "daily-screening-master") if name in names)
        if "collect-nav" in names:
            assert names.index("collect-nav") < period_index


@pytest.mark.parametrize("degraded", [False, True])
def test_period_corrections_are_not_limited_to_nav_target_codes_and_failures_survive(tmp_path, monkeypatch, degraded):
    from backend import core_runner

    settings = SimpleNamespace(project_root=tmp_path, core_root=ROOT / "core", core_output_dir=tmp_path / "output",
        reports_dir=tmp_path / "reports", runs_dir=tmp_path / "reports/runs", database_path=tmp_path / "unused.sqlite",
        tiantian_base_url="http://unused.invalid", workflow_task_timeout_seconds={}, workflow_task_idle_timeout_seconds={})
    names = ["collect-nav", "collect-period-returns", "calculate-risk", "collect-managers", "daily-screening-master"]
    steps = [core_runner.StepResult(name, [sys.executable, "isolated.py", "--changed-nav-only", "--force"])
             for name in names]
    monkeypatch.setattr(core_runner, "build_core_commands", lambda *args: steps)
    monkeypatch.setattr(core_runner, "ensure_core_output_link", lambda *args: None)
    monkeypatch.setattr(core_runner, "_ensure_tiantian_business_ready_for_stage", lambda **kwargs: None)
    monkeypatch.setattr(core_runner, "discover_core_artifacts", lambda *args: {})
    # This command-order test replaces every calculation subprocess. It cannot
    # produce a real input journal; capture integrity is tested independently.
    monkeypatch.setattr(
        "backend.calculation_input_capture.build_calculation_input_index",
        lambda settings, directory, **kwargs: directory / "synthetic_command_test_not_a_real_capture.json",
    )
    executed = []

    def run(**kwargs):
        name, command = kwargs["name"], kwargs["cmd"]
        executed.append(name)
        if name == "collect-nav":
            output = Path(command[command.index("--updated-fcodes-output") + 1])
            core_runner._write_risk_refresh_target_codes(output, ["000406"])
        elif name == "calculate-risk":
            assert "--target-fcodes-file" not in command
            assert "--changed-nav-only" in command
        if degraded and name in {"collect-period-returns", "collect-managers"}:
            raise core_runner.CoreStepFailedError(stage=name, return_code=2, meta_path="isolated")
        return []

    monkeypatch.setattr(core_runner, "_run_streaming_subprocess", run)
    options = {"risk_refresh_changed_nav_only": True, "workflow_run_id": "isolated_period_pipeline"}
    if degraded:
        with pytest.raises(core_runner.CoreStepFailedError) as failure:
            core_runner.run_core_pipeline(settings, options, lambda line: None)
        assert failure.value.stage == "collect-managers"
        assert failure.value.deferred_failures[0]["stage"] == "collect-period-returns"
        assert executed == names[:-1]
    else:
        core_runner.run_core_pipeline(settings, options, lambda line: None)
        assert executed == names
    index = json.loads((settings.runs_dir / "isolated_period_pipeline/logs/subprocess/subprocess_index.json").read_text())
    assert index["period_return_collection_planned"] is True


def test_audit_requires_the_new_planned_step_without_rewriting_legacy_expectations(tmp_path):
    from backend.config import load_settings
    from scripts.audit_data_truthfulness import _validate_subprocess_index_payload

    kwargs = dict(index_file={}, expected_run_id="", required_tasks=[], optional_tasks=[], issue_prefix="isolated")
    _, old_errors, _ = _validate_subprocess_index_payload(load_settings(), index_payload={"items": []}, **kwargs)
    _, new_errors, _ = _validate_subprocess_index_payload(load_settings(),
        index_payload={"items": [], "period_return_collection_planned": True}, **kwargs)
    assert old_errors == []
    assert new_errors == ["isolated_required_tasks_missing:collect-period-returns"]


def test_daily_failed_period_request_allows_nav_repair_but_blocks_publication(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from backend import scheduler as scheduler_module
    from backend.core_runner import CoreStepFailedError

    scheduler = scheduler_module.ProjectScheduler.__new__(scheduler_module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(database_path=tmp_path / "unused.db", latest_report_file=tmp_path / "latest.json",
        reports_dir=tmp_path / "reports", effective_nav_coverage_ratio=0.9, effective_nav_weekday_only=True)
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._log = lambda line: None
    scheduler._build_host_runtime_snapshot = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._build_data_quality_audits_compat = lambda **kwargs: {}
    scheduler._resolve_scheduled_long_term_history_limit = lambda **kwargs: 1
    forbidden = Mock(side_effect=AssertionError("must preserve publication while period collection is degraded"))
    scheduler._run_workflow_in_subprocess = forbidden
    scheduler._rescreen_report_after_maintenance = forbidden
    before = SimpleNamespace(candidate_date="2026-09-08", signature="before", to_dict=lambda: {"signature": "before"})
    after = SimpleNamespace(candidate_date="2026-09-08", signature="repaired", to_dict=lambda: {"signature": "repaired"})
    snapshots = iter([before, after])
    monkeypatch.setattr(scheduler_module, "load_effective_nav_snapshot", lambda *a, **kw: next(snapshots))
    monkeypatch.setattr(scheduler_module, "load_latest_screened_db_snapshot", lambda *a: {})
    monkeypatch.setattr(scheduler_module, "screening_snapshot_changed", lambda *a: (True, "isolated_input_change"))
    failure = CoreStepFailedError(stage="collect-period-returns", return_code=2, meta_path="isolated")
    monkeypatch.setattr(scheduler_module, "run_project_pipeline", Mock(side_effect=failure))
    nav = Mock(return_value={"maintenance_result": {"processed_total": 1, "recovered_total": 1}})
    history = Mock(return_value={"maintenance_result": {"processed_total": 0}})
    monkeypatch.setattr(scheduler_module, "run_nav_history_maintenance", nav)
    monkeypatch.setattr(scheduler_module, "run_autostock_nav_tail_fill", Mock(return_value={}))
    monkeypatch.setattr(scheduler_module, "run_long_term_history_maintenance", history)
    result = scheduler._execute_scheduled_refresh(task_name="isolated_daily", refresh_window="daily",
        nav_history_sync_enabled=True, nav_history_sync_limit=1,
        long_term_history_enabled=True, long_term_history_limit=1)
    nav.assert_called_once()
    history.assert_called_once()
    forbidden.assert_not_called()
    assert result["status"] == "degraded" and result["screening_triggered"] is False
    assert result["after_effective_nav_snapshot"] == {"signature": "repaired"}
    assert result["collection_step_failures"][0]["stage"] == "collect-period-returns"
