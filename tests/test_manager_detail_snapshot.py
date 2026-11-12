"""Synthetic manager observations in disposable SQLite; no production/API IO."""
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from collectors.manager_collector import ManagerCollector
from database.models import FundManager
from backend.manager_metrics import build_manager_metrics, with_manager_display_columns

OPTIONAL_FIELDS = {
    "RESUME": "resume", "TOTALDAYS": "total_days", "NETNAV": "manage_scale",
    "FCOUNT": "fund_count", "YIELDSE": "annual_return", "FMAXEARN1": "max_profit",
    "FMAXRETRA1": "max_drawdown", "MGOLD": "score_overall", "SDAY": "score_experience",
    "SY1": "score_return", "JJGSID": "company_id", "JJGS": "company_name", "NEWPHOTOURL": "photo_url",
}


@pytest.fixture
def manager_database(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "manager.sqlite"))
    FundManager.__table__.create(engine)
    with Session(engine) as session:
        session.add(FundManager(
            mgr_id="TEST_MANAGER", name="测试经理", resume="测试旧简介", total_days=800,
            manage_scale=12.5, fund_count=4, annual_return=2.5, max_profit=15,
            max_drawdown=3.5, score_overall=7.5, score_experience=8, score_return=7,
            company_id="TEST_COMPANY", company_name="测试旧公司", photo_url="https://example.invalid/old.png",
            last_updated_at=datetime(2020, 1, 1),
        ))
        session.commit()

    @contextmanager
    def get_session(**kwargs):
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except BaseException:
                session.rollback()
                raise

    def read():
        with Session(engine) as session:
            row = session.get(FundManager, "TEST_MANAGER")
            return {column.name: getattr(row, column.name) for column in FundManager.__table__.columns}

    collector = ManagerCollector.__new__(ManagerCollector)
    collector.db_manager = SimpleNamespace(get_session=get_session)
    yield collector, read, engine
    engine.dispose()


def response(**values):
    return {"Success": True, "Datas": {"MGRID": "TEST_MANAGER", "MGRNAME": "测试经理", **values}}


@pytest.mark.parametrize("missing", [None, "", "--", "omitted"])
def test_manager_snapshot_does_not_renew_old_optional_fields(manager_database, missing):
    collector, read, _ = manager_database
    before = read()
    assert all(before[field] is not None for field in OPTIONAL_FIELDS.values())
    fields = {} if missing == "omitted" else {key: missing for key in OPTIONAL_FIELDS}
    collector.api = SimpleNamespace(get_manager_info=lambda _: response(**fields))
    assert collector.collect_manager_detail("TEST_MANAGER")
    after = read()
    assert all(after[field] is None for field in OPTIONAL_FIELDS.values())
    assert after["mgr_id"] == before["mgr_id"] and after["name"] == before["name"]
    assert after["last_updated_at"] > before["last_updated_at"]

    # Actual aggregate/display readers must lose coverage, not fabricate zero.
    metrics = build_manager_metrics(pd.DataFrame([{"fcode": "000001", "manager_ids": "TEST_MANAGER"}]),
                                    pd.DataFrame([after]))
    row = metrics.iloc[0]
    assert row["manager_count"] == row["manager_profile_count"] == 1
    assert row["manager_scored_count"] == row["manager_total_days_count"] == 0
    assert pd.isna(row["manager_avg_score_overall"]) and pd.isna(row["manager_min_total_days"])
    display = with_manager_display_columns(metrics).iloc[0]
    assert display["经理评分覆盖"] == "0/1" and display["经理从业数据覆盖"] == "0/1"
    assert display["经理平均综合评分"] == display["经理最短从业天数"] == ""


@pytest.mark.parametrize("score", [0, "0", 8.125])
def test_manager_new_observation_keeps_real_zero_and_documented_units(manager_database, score):
    collector, read, _ = manager_database
    collector.api = SimpleNamespace(get_manager_info=lambda _: response(
        TOTALDAYS="0", FCOUNT="0", NETNAV="125000000", YIELDSE="-1.5",
        FMAXEARN1="0.25", FMAXRETRA1="0.015", MGOLD=score, SDAY="0", SY1="0",
        JJGSID=123456, JJGS="测试新公司", RESUME=" 测试新简介\n", NEWPHOTOURL="--",
    ))
    assert collector.collect_manager_detail("TEST_MANAGER")
    row = read()
    assert row["total_days"] == row["fund_count"] == row["score_experience"] == row["score_return"] == 0
    assert row["score_overall"] == float(score)
    assert row["manage_scale"] == 1.25 and row["annual_return"] == -1.5
    assert row["max_profit"] == 25 and row["max_drawdown"] == 1.5
    assert row["company_id"] == "123456" and row["company_name"] == "测试新公司"
    assert row["resume"] == " 测试新简介\n" and row["photo_url"] is None


@pytest.mark.parametrize("invalid", [True, False, float("inf"), float("-inf"), float("nan"), "not-a-number", []])
def test_manager_invalid_numeric_observation_is_unknown_not_old_or_zero(manager_database, invalid):
    collector, read, _ = manager_database
    collector.api = SimpleNamespace(get_manager_info=lambda _: response(
        TOTALDAYS=invalid, FCOUNT=invalid, NETNAV=invalid, MGOLD=invalid, SDAY=invalid,
        SY1=invalid, YIELDSE=invalid, FMAXEARN1=invalid, FMAXRETRA1=invalid,
    ))
    assert collector.collect_manager_detail("TEST_MANAGER")
    row = read()
    assert all(row[column] is None for column in (
        "total_days", "fund_count", "manage_scale", "score_overall", "score_experience",
        "score_return", "annual_return", "max_profit", "max_drawdown",
    ))


@pytest.mark.parametrize("invalid", [-1, 1.5, 2**63, "1.5"])
def test_manager_counts_do_not_truncate_or_store_impossible_values(manager_database, invalid):
    collector, read, _ = manager_database
    assert collector._save_manager_detail("TEST_MANAGER", response(TOTALDAYS=invalid, FCOUNT=invalid)["Datas"])
    assert read()["total_days"] is None and read()["fund_count"] is None


def test_manager_percent_conversion_overflow_is_unknown(manager_database):
    collector, read, _ = manager_database
    assert collector._save_manager_detail("TEST_MANAGER", response(FMAXEARN1=1e308, FMAXRETRA1=1e308)["Datas"])
    assert read()["max_profit"] is None and read()["max_drawdown"] is None


def test_manager_missing_then_known_score_uses_only_latest_response(manager_database):
    collector, read, _ = manager_database
    for raw, expected in (("--", None), ("0", 0), ("8.125", 8.125), (None, None)):
        assert collector._save_manager_detail("TEST_MANAGER", response(TOTALDAYS="800", MGOLD=raw)["Datas"])
        assert read()["score_overall"] == expected
        assert read()["total_days"] == 800


def test_manager_failed_request_preserves_all_values_and_timestamp(manager_database):
    collector, read, _ = manager_database
    before = read()

    def fail(_):
        raise ConnectionError("isolated transport failure")

    collector.api = SimpleNamespace(get_manager_info=fail)
    with pytest.raises(ConnectionError, match="isolated transport failure"):
        collector.collect_manager_detail("TEST_MANAGER")
    assert read() == before


def test_manager_failed_commit_rolls_back_complete_snapshot(manager_database):
    collector, read, engine = manager_database
    before = read()
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TRIGGER fail_manager_update BEFORE UPDATE ON fund_managers "
                                   "BEGIN SELECT RAISE(ABORT, 'isolated write failure'); END")
    assert collector._save_manager_detail("TEST_MANAGER", response(MGOLD="--")["Datas"]) is False
    assert read() == before
