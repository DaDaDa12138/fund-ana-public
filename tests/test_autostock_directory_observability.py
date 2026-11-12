"""Isolated transport/cache contract drills; no upstream HTTP or production state."""
from __future__ import annotations

import copy
import json
import multiprocessing
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))
from api import autostock_client as api
from utils import autostock_directory_cache as cache


DIRECTORY = [
    ["000001", "TESTA", "测试债券A", "债券型"],
    ["000002", "TESTB", "测试混合B", "混合型"],
]


def response(data=None, *, status=200, code=200):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps({"code": code, "message": "quota limit" if status == 429 else "response",
                                  "data": data}, ensure_ascii=False).encode()
    result.encoding = "utf-8"
    return result


def make_client(tmp_path, monkeypatch, outcomes, *, cached=False):
    client = api.AutoStockClient(
        min_interval=0, shared_state_path=str(tmp_path / "quota.json"),
        directory_cache_path=str(tmp_path / "directory.json") if cached else None,
    )
    pending = iter(outcomes)
    sent = []

    def send(method, url, **kwargs):
        sent.append((method, url, kwargs))
        result = next(pending)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(client, "_send_http_request", send)
    monkeypatch.setattr(api.time, "sleep", lambda seconds: None)
    return client, sent


def test_cached_directory_retains_origin_but_counts_no_new_http(tmp_path, monkeypatch):
    received = response(DIRECTORY)
    client, sent = make_client(tmp_path, monkeypatch, [received], cached=True)
    assert client.get_all_bond_funds_from_directory()[0]["code"] == "000001"
    first = client.get_last_fund_all_meta()
    assert first["source_evidence"]["returned_unique_count"] == 2
    assert first["http_attempt_count"] == 1
    assert first["response_body_bytes"] == len(received.content)
    assert client.get_all_bond_funds_from_directory()[0]["code"] == "000001"
    second = client.get_last_fund_all_meta()
    evidence = second["source_evidence"]
    assert len(sent) == client.get_call_count() == 1
    assert second["cache_status"] == "hit"
    assert second["http_attempt_count"] == second["http_response_count"] == second["response_body_bytes"] == 0
    assert evidence["request_count"] == evidence["response_count"] == evidence["accepted_response_count"] == 0
    assert evidence["source_type"] == "cache"
    assert evidence["returned_unique_count"] == 2
    assert evidence["sample_codes"] == ["000001"]
    assert evidence["origin_request_metadata"]["http_attempt_count"] == 1
    assert evidence["data_sha256"] == evidence["response_hash"]
    assert evidence["source_complete"] is True


def test_keyword_projection_reuses_full_snapshot_and_does_not_hide_missing_type(tmp_path, monkeypatch):
    rows = DIRECTORY + [["000003", "TESTC", "缺少分类", ""]]
    client, sent = make_client(tmp_path, monkeypatch, [response(rows)], cached=True)
    assert client.get_all_funds(keyword="000001") == [rows[0]]
    assert "keyWord" not in sent[0][1]
    client.get_all_bond_funds_from_directory()
    evidence = client.get_last_fund_all_meta()["source_evidence"]
    assert len(sent) == 1
    assert evidence["returned_record_count"] == 3
    assert evidence["returned_unique_count"] == 2
    assert evidence["parse_error_codes"] == ["000003"]
    assert evidence["source_complete"] is False


@pytest.mark.parametrize("corruption", ["expired", "future", "endpoint", "data", "origin", "schema", "json"])
def test_invalid_directory_cache_fetches_fresh_data_instead_of_using_stale(tmp_path, corruption):
    path = tmp_path / "directory.json"
    now = [1000.0]
    fetches = []

    def fetch():
        fetches.append(True)
        return [[str(len(fetches))]], {"http_attempt_count": 1}

    old_rows, _ = cache.get_directory_snapshot(path, endpoint="https://example/fund/all", ttl_seconds=60,
                                              fetch=fetch, clock=lambda: now[0])
    payload = json.loads(path.read_text())
    if corruption == "expired":
        now[0] += 60
    elif corruption == "future":
        payload["fetched_at_epoch"] += 1
    elif corruption == "endpoint":
        payload["endpoint"] = "https://another.example/fund/all"
    elif corruption == "data":
        payload["data"] = [["999999"]]
    elif corruption == "origin":
        payload["origin_request_metadata"]["http_attempt_count"] = 0
    elif corruption == "schema":
        payload["schema_version"] = 200
    path.write_text("{" if corruption == "json" else json.dumps(payload))
    rows, meta = cache.get_directory_snapshot(path, endpoint="https://example/fund/all", ttl_seconds=60,
                                              fetch=fetch, clock=lambda: now[0])
    assert len(fetches) == 2 and rows != old_rows
    assert meta["cache_status"] == "refreshed" and meta["cache_rejection_reason"]


def test_cache_write_failure_keeps_real_response_and_old_file(tmp_path, monkeypatch):
    path = tmp_path / "directory.json"
    old, _ = cache.get_directory_snapshot(path, endpoint="endpoint", ttl_seconds=60,
                                          fetch=lambda: ([["old"]], {}), clock=lambda: 1000)
    before = path.read_bytes()
    original = Path.replace

    def replace(source, target):
        if target == path:
            raise OSError("injected write failure")
        return original(source, target)

    monkeypatch.setattr(Path, "replace", replace)
    rows, meta = cache.get_directory_snapshot(path, endpoint="endpoint", ttl_seconds=60,
                                              fetch=lambda: ([["fresh"]], {}), clock=lambda: 1060)
    assert rows != old and rows == [["fresh"]]
    assert meta["cache_status"] == "write_failed" and meta["cache_write_error"]
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".*.tmp"))


def _concurrent_client_fetch(directory, start, results):
    root = Path(directory)
    client = api.AutoStockClient(min_interval=0, shared_state_path=str(root / "quota.json"),
                                  directory_cache_path=str(root / "directory.json"))
    client._send_http_request = lambda *args, **kwargs: response(DIRECTORY)
    start.wait(timeout=10)
    rows = client.get_all_bond_funds_from_directory()
    meta = client.get_last_fund_all_meta()
    results.put((rows, meta["http_attempt_count"], meta["cache_status"]))


def test_four_process_cache_miss_consumes_one_shared_quota_slot(tmp_path):
    context = multiprocessing.get_context("fork")
    start, results = context.Event(), context.Queue()
    processes = [context.Process(target=_concurrent_client_fetch, args=(str(tmp_path), start, results)) for _ in range(4)]
    for process in processes:
        process.start()
    start.set()
    observed = [results.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert sum(item[1] for item in observed) == 1
    assert sorted(item[2] for item in observed) == ["hit", "hit", "hit", "refreshed"]
    assert all(item[0] == observed[0][0] for item in observed)
    assert len(json.loads((tmp_path / "quota.json").read_text())["recent_call_timestamps"]) == 1


def test_keyword_quota_error_does_not_fallback_to_full_directory(tmp_path, monkeypatch):
    client, sent = make_client(tmp_path, monkeypatch, [response(status=429, code=429)])
    with pytest.raises(Exception):
        client.get_all_funds(keyword="000001")
    evidence = client.get_last_fund_all_meta()["source_evidence"]
    assert len(sent) == 1 and "keyWord" in sent[0][1]
    assert evidence["http_attempt_count"] == evidence["http_response_count"] == 1
    assert evidence["quota_exhausted"] is True
    assert evidence["source_complete"] is False
    assert evidence["used_keyword_fallback"] is False


def test_successful_retry_records_real_bytes_responses_and_timing(tmp_path, monkeypatch):
    first, second = response(status=503, code=503), response([{"code": "000001", "dayGrowth": 0}])
    client, sent = make_client(tmp_path, monkeypatch, [first, second])
    rows, meta = client.get_fund_batch_with_meta(["000001"])
    assert rows[0]["dayGrowth"] == 0
    assert len(sent) == meta["http_attempt_count"] == meta["http_response_count"] == 2
    assert meta["http_retry_count"] == meta["failed_http_response_count"] == 1
    assert meta["transport_failure_count"] == 0
    assert meta["response_body_bytes"] == len(first.content) + len(second.content)
    assert meta["http_status_counts"] == {"503": 1, "200": 1}
    assert meta["request_elapsed_seconds"] >= meta["http_elapsed_seconds"] >= 0
    assert meta["source_evidence"]["response_body_bytes"] == meta["response_body_bytes"]


@pytest.mark.parametrize("detail", [False, True])
def test_partial_success_and_unattempted_batches_are_separate(tmp_path, monkeypatch, detail):
    first, second = response([{"code": "000001", "dayGrowth": 0}]), response(status=429, code=429)
    client, sent = make_client(tmp_path, monkeypatch, [first, second])
    codes = ["000001", "000002", "000003", "000004", "000005"]
    if detail:
        rows = client.get_fund_detail_batch(codes, batch_size=2)
        meta = client.get_last_fund_detail_batch_meta()
    else:
        rows, meta = client.get_fund_batch_with_meta(codes, batch_size=2)
    assert len(rows) == 1 and len(sent) == 2
    assert meta["partial_success_batch_count"] == 1
    assert meta["attempted_batch_count"] == 2 and meta["unattempted_batch_count"] == 1
    assert meta["missing_count"] == 4
    assert meta["http_attempt_count"] == meta["http_response_count"] == 2
    assert meta["http_retry_count"] == 0
    assert meta["response_body_bytes"] == len(first.content) + len(second.content)
    assert meta["failure_kind_counts"] == {"missing_rows": 1, "upstream_quota": 1}


def test_transport_failure_does_not_invent_a_response_body(tmp_path, monkeypatch):
    client, sent = make_client(tmp_path, monkeypatch, [requests.Timeout("test") for _ in range(3)])
    client.get_fund_detail_batch(["000001"])
    meta = client.get_last_fund_detail_batch_meta()
    assert len(sent) == meta["http_attempt_count"] == meta["transport_failure_count"] == 3
    assert meta["http_retry_count"] == 2
    assert meta["http_response_count"] == meta["response_body_bytes"] == 0
    assert meta["failure_kind_counts"] == {"timeout": 1}


def test_rank_outer_retry_is_counted_without_nested_retry(tmp_path, monkeypatch):
    outcomes = [response(status=503, code=503),
                response({"rank": [{"code": "000001"}], "totalRecord": 2}),
                response({"rank": [], "totalRecord": 2})]
    client, sent = make_client(tmp_path, monkeypatch, outcomes)
    assert client.get_all_bond_funds_from_rank(page_size=1) == [{"code": "000001"}]
    meta = client.get_last_rank_fetch_meta()
    assert len(sent) == meta["http_attempt_count"] == meta["logical_page_attempt_count"] == 3
    assert meta["http_retry_count"] == 1
    assert meta["response_body_bytes"] == sum(len(item.content) for item in outcomes)
    assert meta["source_complete"] is False


def test_rank_business_failure_retains_wire_observation(tmp_path, monkeypatch):
    received = response({"rank": [], "requestFail": 1})
    client, sent = make_client(tmp_path, monkeypatch, [received])
    assert client.get_all_bond_funds_from_rank(page_size=1, max_retries=1) == []
    meta = client.get_last_rank_fetch_meta()
    assert len(sent) == meta["http_attempt_count"] == 1
    assert meta["response_body_bytes"] == len(received.content)
    assert meta["failure_kind_counts"] == {"upstream_business_data_failed": 1}
    assert meta["source_complete"] is False


def test_mocked_unobserved_request_keeps_unknown_counters(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, [response(DIRECTORY)])
    client.get_all_funds()
    assert client.get_last_fund_all_meta()["http_attempt_count"] == 1
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: DIRECTORY)
    client.get_all_funds()
    assert client.get_last_fund_all_meta()["http_attempt_count"] is None
    assert client.get_last_fund_all_meta()["response_body_bytes"] is None


def test_truth_audit_accepts_verified_cache_with_zero_requests_and_rejects_tampering(tmp_path, monkeypatch):
    from scripts import audit_data_truthfulness as audit

    client, _ = make_client(tmp_path, monkeypatch, [response(DIRECTORY)], cached=True)
    client.get_all_bond_funds_from_directory()
    client.get_all_bond_funds_from_directory()
    evidence = client.get_last_fund_all_meta()["source_evidence"]
    evidence.update({"task": "bond_universe_sync.autostock_directory", "business_date": None,
                     "business_date_semantics": "not_provided_by_autostock_directory", "parse_error_count": 0,
                     "database_write_attempt_count": 0, "database_saved_count": 0, "database_fund_insert_count": 0,
                     "database_fund_update_count": 0, "database_failed_record_count": 0, "database_skipped_count": 0,
                     "skip_reason_counts": {}, "failure_reason_counts": {}})
    reports = tmp_path / "reports"
    target = reports / "audit/bond_universe_sync/bond_universe_sync_20260907_000000.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({"autostock_directory_source_evidence": evidence}))
    result = audit._autostock_directory_source_evidence_truth(SimpleNamespace(project_root=tmp_path, reports_dir=reports))
    assert result["blockers"] == []
    assert result["request_count"] == 0 and result["source_type"] == "cache"
    for field, value in [("http_attempt_count", 1), ("data_sha256", "bad"), ("cache_age_seconds", 3601),
                         ("cache_endpoint", "wrong"), ("origin_request_metadata", {}), ("cache_integrity_verified", False)]:
        modified = copy.deepcopy(evidence)
        modified[field] = value
        assert audit._directory_cache_evidence_errors(modified), field
