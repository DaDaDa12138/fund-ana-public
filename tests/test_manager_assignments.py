"""Real assignment responses replayed only into isolated SQLite databases."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from collectors.manager_collector import ManagerCollector, _extract_manager_list_fields
from database.connection import DatabaseManager
from database.models import Fund

FIXTURE = json.loads((Path(__file__).parent / "fixtures/tiantian_manager_assignments_20260908.json").read_text())


def payload_for(code: str) -> dict:
    row = FIXTURE["responses"][code]
    assert hashlib.sha256(row["raw_body"].encode()).hexdigest() == row["body_sha256"]
    assert json.loads(row["raw_body"]) == row["payload"]
    return deepcopy(row["payload"])


def seeded_collector(tmp_path: Path, codes: list[str], overrides: dict | None = None):
    db = DatabaseManager(db_path=str(tmp_path / "assignments.db"), wal_mode=True)
    Fund.__table__.create(db.engine)
    with db.get_session() as session:
        for code in codes:
            row = {k: v for k, v in FIXTURE["baseline_funds"][code].items() if k != "reason"}
            if row["manager_list_updated_at"]:
                row["manager_list_updated_at"] = datetime.fromisoformat(row["manager_list_updated_at"])
            session.add(Fund(**row))
    calls = []

    class ReplayClient:
        def get(self, endpoint, params):
            assert endpoint == "/fundMNMangerList"
            code = params["FCODE"]
            calls.append(code)
            return deepcopy(overrides[code]) if overrides and code in overrides else payload_for(code)

    return ManagerCollector(config={}, db_manager=db, api_client=ReplayClient()), db, calls


@pytest.mark.parametrize("code", ["002109", "002964"])
def test_all_ended_real_intervals_do_not_become_current_managers(code):
    payload = payload_for(code)
    assert payload["TotalCount"] == len(payload["Datas"])
    assert all(row["LEMPDATE"] != "--" for row in payload["Datas"])
    assert _extract_manager_list_fields(payload["Datas"]) == {"manager_ids": "", "manager_names": ""}


@pytest.mark.parametrize("code", ["000015", "000147", "020716", "000032"])
def test_real_current_manager_combinations_keep_all_ids_and_names(code):
    fields = _extract_manager_list_fields(payload_for(code)["Datas"])
    original = FIXTURE["baseline_funds"][code]
    assert fields == {"manager_ids": original["manager_ids"], "manager_names": original["manager_names"]}


@pytest.mark.parametrize("code", ["002109", "002964"])
def test_confirmed_ended_history_clears_only_current_binding_and_logs_source(tmp_path, capsys, code):
    collector, db, calls = seeded_collector(tmp_path, [code])
    started = datetime.now()
    result = collector._collect_fund_managers(force_update=True)
    assert result["success_funds"] == 1 and result["failed_funds"] == 0
    assert calls == [code]
    with db.get_session(readonly=True) as session:
        fund = session.get(Fund, code)
        assert fund.manager_ids is None and fund.manager_names is None
        assert fund.manager_list_updated_at >= started
        assert fund.shortname == FIXTURE["baseline_funds"][code]["shortname"]
        assert fund.company_id == FIXTURE["baseline_funds"][code]["company_id"]
        assert fund.is_buyable is False
    receipts = [json.loads(line.split("=", 1)[1]) for line in capsys.readouterr().out.splitlines()
                if line.startswith("manager_assignment_evidence=")]
    assert len(receipts) == 1
    assert receipts[0]["fcode"] == code
    assert receipts[0]["source_payload"] == payload_for(code)
    assert receipts[0]["previous_manager_ids"] == FIXTURE["baseline_funds"][code]["manager_ids"]
    evidence = collector.get_manager_list_api_source_evidence()
    assert evidence["saved_fund_count"] == 1
    assert evidence["no_current_manager_fund_count"] == 1


@pytest.mark.parametrize("case", ["total_mismatch", "missing_total", "wrong_fund", "future_end", "missing_end", "none_datas"])
def test_ambiguous_history_preserves_old_binding_and_timestamp(tmp_path, case):
    code = "002109"
    payload = payload_for(code)
    if case == "total_mismatch":
        payload["TotalCount"] -= 1
    elif case == "missing_total":
        payload.pop("TotalCount")
    elif case == "wrong_fund":
        payload["Datas"][0]["FCODE"] = "000015"
    elif case == "future_end":
        payload["Datas"][0]["LEMPDATE"] = "9999-12-31"
    elif case == "missing_end":
        payload["Datas"][0].pop("LEMPDATE")
    else:
        payload["Datas"] = None
    collector, db, _ = seeded_collector(tmp_path, [code], {code: payload})
    result = collector._collect_fund_managers(force_update=True)
    assert result["success_funds"] == 0 and result["failed_funds"] == 1
    with db.get_session(readonly=True) as session:
        fund = session.get(Fund, code)
        original = FIXTURE["baseline_funds"][code]
        assert fund.manager_ids == original["manager_ids"]
        assert fund.manager_names == original["manager_names"]
        assert fund.manager_list_updated_at == datetime.fromisoformat(original["manager_list_updated_at"])
    assert collector.get_manager_list_api_source_evidence()["saved_fund_count"] == 0


def test_empty_valid_list_never_clears_existing_binding(tmp_path):
    code = "002109"
    collector, db, _ = seeded_collector(tmp_path, [code], {code: {"Success": True, "Datas": []}})
    collector._collect_fund_managers(force_update=True)
    with db.get_session(readonly=True) as session:
        fund = session.get(Fund, code)
        assert fund.manager_ids == FIXTURE["baseline_funds"][code]["manager_ids"]
        assert fund.manager_names == FIXTURE["baseline_funds"][code]["manager_names"]


def test_explicit_fund_scope_cannot_refresh_other_bindings(tmp_path):
    collector, db, calls = seeded_collector(tmp_path, ["002109", "002964"])
    result = collector._collect_fund_managers(force_update=True, target_fcodes=["002109"])
    assert result["total_funds"] == 1 and calls == ["002109"]
    with db.get_session(readonly=True) as session:
        assert session.get(Fund, "002109").manager_ids is None
        assert session.get(Fund, "002964").manager_ids == FIXTURE["baseline_funds"]["002964"]["manager_ids"]
    evidence = collector.get_manager_list_api_source_evidence()
    assert evidence["target_scope"] == "explicit_fund_codes"
    assert evidence["requested_fcodes"] == ["002109"]


def test_failed_batch_commit_does_not_claim_saved_bindings(tmp_path, capsys):
    codes = ["002109", "002964"]
    collector, db, _ = seeded_collector(tmp_path, codes)
    from sqlalchemy import event

    def reject_manager_update(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("UPDATE FUNDS"):
            raise RuntimeError("isolated_write_failure")

    event.listen(db.engine, "before_cursor_execute", reject_manager_update)
    try:
        result = collector._collect_fund_managers(force_update=True)
    finally:
        event.remove(db.engine, "before_cursor_execute", reject_manager_update)
    assert result["success_funds"] == 0 and result["failed_funds"] == 2
    assert collector.get_manager_list_api_source_evidence()["saved_fund_count"] == 0
    assert "manager_assignment_evidence=" not in capsys.readouterr().out
    with db.get_session(readonly=True) as session:
        for code in codes:
            assert session.get(Fund, code).manager_ids == FIXTURE["baseline_funds"][code]["manager_ids"]
