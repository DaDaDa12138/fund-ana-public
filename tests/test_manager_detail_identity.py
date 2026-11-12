"""Synthetic identity faults in disposable tables; never call a live API."""
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from collectors.manager_collector import ManagerCollector
from database.models import FundManager
from utils.tiantian_data_quality import inspect_tiantian_manager_info_payload


@pytest.fixture
def manager_copy(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "manager-copy.db"))
    FundManager.__table__.create(engine)
    with Session(engine) as session:
        session.add(FundManager(mgr_id="M001", name="原测试经理", total_days=365,
                                score_overall=0, company_name="原测试公司",
                                last_updated_at=datetime(2020, 1, 1)))
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

    def rows():
        with Session(engine) as session:
            return [{column.name: getattr(row, column.name) for column in FundManager.__table__.columns}
                    for row in session.query(FundManager).order_by(FundManager.mgr_id)]

    collector = ManagerCollector.__new__(ManagerCollector)
    collector.db_manager = SimpleNamespace(get_session=get_session)
    yield collector, rows
    engine.dispose()


@pytest.mark.parametrize(("fields", "status"), [
    ({"MGRID": "M002", "MGRNAME": "另一测试经理"}, "manager_identity_mismatch"),
    ({"MGRID": "M002", "MGRNAME": "原测试经理", "FCODE": "M001"}, "manager_identity_mismatch"),
    ({"MGRNAME": "原测试经理"}, "invalid_manager_identity"),
    ({"MGRID": None, "MGRNAME": "原测试经理"}, "invalid_manager_identity"),
    ({"MGRID": "--", "MGRNAME": "原测试经理"}, "invalid_manager_identity"),
    ({"MGRID": [], "MGRNAME": "原测试经理"}, "invalid_manager_identity"),
    ({"MGRID": True, "MGRNAME": "原测试经理"}, "invalid_manager_identity"),
    ({"MGRID": "M001", "MGRNAME": "--"}, "invalid_manager_name"),
    ({"MGRID": "M001", "MGRNAME": " "}, "invalid_manager_name"),
    ({"MGRID": "M001", "MGRNAME": False}, "invalid_manager_name"),
    ({"MGRID": "M001", "MGRNAME": {"name": "原测试经理"}}, "invalid_manager_name"),
])
def test_invalid_manager_response_cannot_replace_any_stored_field(manager_copy, monkeypatch, fields, status):
    collector, rows = manager_copy
    before = rows()
    calls = []
    def response(manager_id):
        calls.append(manager_id)
        return {"Success": True, "Datas": {**fields, "TOTALDAYS": "900", "MGOLD": "9"}}
    collector.api = SimpleNamespace(get_manager_info=response)
    monkeypatch.setattr("collectors.manager_collector.time.sleep", lambda _: pytest.fail("Identity faults must not be retried"))

    assert collector.collect_manager_detail("M001") is False
    assert calls == ["M001"]
    assert rows() == before
    evidence = collector.get_manager_detail_api_source_evidence()
    assert evidence["request_count"] == evidence["response_count"] == evidence["failed_response_count"] == 1
    assert evidence["accepted_response_count"] == evidence["saved_fund_count"] == evidence["written_record_count"] == 0
    assert evidence["quality_status_counts"] == {status: 1}

    # An internal/direct save caller cannot bypass the request/response binding.
    assert collector._save_manager_detail("M001", response("M001")["Datas"]) is False
    assert rows() == before


def test_matching_manager_with_real_zero_fields_saves_without_fund_code_confusion(manager_copy):
    collector, rows = manager_copy
    collector.api = SimpleNamespace(get_manager_info=lambda _: {
        "Success": True, "Datas": {"MGRID": "M001", "FCODE": "000001,000002", "MGRNAME": "新测试姓名",
            "TOTALDAYS": "0", "MGOLD": "0", "SDAY": "0", "SY1": "0", "NETNAV": "0", "FCOUNT": "0"},
    })
    assert collector.collect_manager_detail("M001") is True
    row = rows()[0]
    assert row["mgr_id"] == "M001" and row["name"] == "新测试姓名"
    assert all(row[key] == 0 for key in ("total_days", "score_overall", "score_experience", "score_return", "manage_scale", "fund_count"))
    assert row["last_updated_at"] > datetime(2020, 1, 1)
    evidence = collector.get_manager_detail_api_source_evidence()
    assert evidence["saved_fund_count"] == evidence["written_record_count"] == 1


@pytest.mark.parametrize(("returned_id", "requested", "accepted"), [
    (30276903, "30276903", True),
    (" 30276903 ", "30276903", True),
    ("0030276903", "0030276903", True),
    (30276903, "0030276903", False),
])
def test_manager_identity_keeps_leading_zero_semantics(returned_id, requested, accepted):
    data, quality = inspect_tiantian_manager_info_payload(
        {"Success": True, "Datas": {"MGRID": returned_id, "MGRNAME": "测试经理"}},
        expected_manager_id=requested,
    )
    assert quality.accepted is accepted
    assert (data is not None) is accepted


@pytest.mark.parametrize("body", [None, {}, [], {"MGRID": "M001"}])
def test_unusable_manager_detail_does_not_advance_profile_timestamp(manager_copy, body):
    collector, rows = manager_copy
    before = rows()
    assert collector._save_manager_detail("M001", body) is False
    assert rows() == before
