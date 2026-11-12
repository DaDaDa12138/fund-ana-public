"""Isolated transport fixtures; none of these records are business data."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from api import client as client_module
from api.client import APIClient
from api.response_observation import ObservedRankResponse, rank_response_observation
from collectors.fund_collector import FundCollector
from config.api_config import FundType
from utils.fund_list_pagination_audit import record_rank_page_observation


def response(payload, request_id="fixture-request"):
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    return ObservedRankResponse(payload, body=raw, request_id=request_id,
                                http_status=200, proxy_request_id="fixture-proxy"), raw


def test_members_can_be_reconstructed_with_order_duplicates_absence_null_and_zero():
    records = [
        {"FCODE": "fixture-a", "SHORTNAME": "test record", "SYL_1N": 0, "unused": 3},
        {"FCODE": "fixture-a", "SHORTNAME": None, "SYL_1N": None},
        {"FCODE": "fixture-b"},
        {"SHORTNAME": "missing identity"},
        None,
    ]
    page, raw = response({"Datas": deepcopy(records), "TotalCount": 5})
    evidence = {}
    record_rank_page_observation(evidence, page["Datas"], page=1, previous_codes=set(),
                                 declared_total=5, sort_column="SYL_1N", response_payload=page)
    observed = evidence["page_observations"][0]
    assert observed["identity_sort_records"] == [
        {"FCODE": "fixture-a", "SHORTNAME": "test record", "SYL_1N": 0},
        {"FCODE": "fixture-a", "SHORTNAME": None, "SYL_1N": None},
        {"FCODE": "fixture-b"}, {"SHORTNAME": "missing identity"}, None,
    ]
    assert observed["response_observation"]["response_body_sha256"] == hashlib.sha256(raw).hexdigest()
    assert observed["identified_record_count"] == 3 and observed["unique_code_count"] == 2
    assert observed["within_page_duplicate_codes"] == ["fixture-a"]
    # Mutating later processing input cannot rewrite the captured membership.
    page["Datas"][0]["SHORTNAME"] = "changed downstream"
    assert observed["identity_sort_records"][0]["SHORTNAME"] == "test record"
    assert observed["response_observation"]["status"] == "observed"
    assert datetime.fromisoformat(observed["response_observation"]["observed_at"]).utcoffset() == timedelta(hours=8)


def test_bound_transport_is_not_serialized_into_business_json_or_rebound_after_mutation():
    payload = {"Datas": [{"FCODE": "fixture-a", "SYL_1N": 0}], "TotalCount": 1}
    page, raw = response(deepcopy(payload))
    assert page == payload and json.loads(json.dumps(page)) == payload
    assert rank_response_observation(deepcopy(page)) == rank_response_observation(page)
    assert rank_response_observation(dict(page)) == {
        "status": "transport_not_observed", "response_body_sha256": None,
    }
    page["Datas"][0]["FCODE"] = "fixture-other"
    assert rank_response_observation(page) == {
        "status": "parsed_payload_changed", "response_body_sha256": None,
    }


def test_transport_sha_is_entity_bytes_not_reserialized_json():
    payload = {"Datas": [], "TotalCount": 0}
    first = b'{ "Datas": [], "TotalCount": 0 }\n'
    second = b'{"TotalCount":0,"Datas":[]}'
    observations = [rank_response_observation(ObservedRankResponse(
        payload, body=raw, request_id="fixture-request", http_status=200)) for raw in (first, second)]
    assert observations[0]["response_body_sha256"] != observations[1]["response_body_sha256"]
    assert observations[0]["parsed_payload_sha256"] == observations[1]["parsed_payload_sha256"]


@pytest.mark.parametrize("records,total", [([], 0), ([{"FCODE": "fixture-a", "SHORTNAME": "test"}], 1)])
def test_collector_binds_actual_first_page_including_empty_page(records, total):
    page, raw = response({"Datas": records, "TotalCount": total})
    collector = FundCollector.__new__(FundCollector)
    collector.api = SimpleNamespace(get_bond_fund_rank=lambda **kwargs: page)
    collector._parse_fund_list = lambda rows, fund_type: deepcopy(rows)
    assert collector._collect_by_type(FundType.BOND, False) == records
    observed = collector._last_collect_by_type_evidence["page_observations"][0]
    assert observed["identity_sort_records"] == records
    assert observed["response_observation"]["response_body_sha256"] == hashlib.sha256(raw).hexdigest()


def test_collector_keeps_distinct_page_evidence_for_early_empty_page():
    first_rows = [{"FCODE": f"fixture-{i}", "SHORTNAME": "test"} for i in range(30)]
    first, _ = response({"Datas": first_rows, "TotalCount": 60}, "first")
    empty, _ = response({"Datas": [], "TotalCount": 60}, "second")
    collector = FundCollector.__new__(FundCollector)
    collector.api = SimpleNamespace(get_bond_fund_rank=lambda **kwargs: first if kwargs["page_index"] == 1 else empty)
    collector._parse_fund_list = lambda rows, fund_type: deepcopy(rows)
    assert collector._collect_by_type(FundType.BOND, False) == first_rows
    evidence = collector._last_collect_by_type_evidence
    pages = evidence["page_observations"]
    assert [item["response_observation"]["request_id"] for item in pages] == ["first", "second"]
    assert pages[1]["identity_sort_records"] == []
    assert evidence["unique_coverage_status"] == "coverage_not_proven"


def test_noncanonical_input_does_not_gain_false_transport_proof_or_change_values():
    page, _ = response({"Datas": [{"FCODE": "fixture-a", "SYL_1N": float("nan")}]})
    assert rank_response_observation(page) == {
        "status": "parsed_payload_not_canonical_json", "response_body_sha256": None,
    }
    assert str(page["Datas"][0]["SYL_1N"]) == "nan"


def test_http_client_binds_actual_returned_rank_response_without_changing_other_endpoints(monkeypatch):
    payload = {"Datas": [{"FCODE": "fixture-a", "SHORTNAME": "test"}], "TotalCount": 1}
    raw = json.dumps(payload, indent=2).encode()
    transport_response = SimpleNamespace(
        status_code=200, content=raw, headers={"X-Request-ID": "fixture-remote"},
        raise_for_status=lambda: None, json=lambda: deepcopy(payload),
    )
    client = APIClient.__new__(APIClient)
    client.base_url = "https://test.invalid"
    client.base_params = {}
    client.headers = {}
    client.retry_handler = SimpleNamespace(timeout=1)
    client.rate_limiter = SimpleNamespace(record_success=lambda *a: None, record_failure=lambda: None)
    client._get_session = lambda: SimpleNamespace(get=lambda *a, **kw: transport_response)
    client._update_stats = lambda **kw: None
    monkeypatch.setattr(client_module, "HAS_ENHANCED_LOGGER", False)
    rank = client._make_request("GET", "/fundMNRank", request_id="fixture-local")
    detail = client._make_request("GET", "/fundMNDetailInformation", request_id="fixture-detail")
    assert rank == detail == payload and type(detail) is dict
    observed = rank_response_observation(rank)
    assert observed["request_id"] == "fixture-local" and observed["proxy_request_id"] == "fixture-remote"
    assert observed["response_body_bytes"] == len(raw)
    assert observed["response_body_sha256"] == hashlib.sha256(raw).hexdigest()
