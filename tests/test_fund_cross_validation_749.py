"""Isolated endpoint contracts; failure mutations are not business observations."""
from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from backend import fund_cross_validation as validation

FIXTURES = Path(__file__).parent / "fixtures" / "fund_cross_validation_749"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
NOW = datetime.fromisoformat("2026-10-13T09:40:00+08:00")


def payload(kind):
    return json.loads(gzip.decompress((FIXTURES / f"{kind}.response.json.gz").read_bytes()))


def response(data=None, *, raw=None, status=200):
    return SimpleNamespace(status_code=status, content=raw if raw is not None else json.dumps(data, ensure_ascii=False).encode())


@pytest.fixture
def settings(tmp_path):
    return SimpleNamespace(reports_dir=tmp_path / "reports", project_root=tmp_path,
        tiantian_valuation_primary_url="https://primary.example.invalid/FundValuationLast",
        tiantian_valuation_fallback_url="https://backup.example.invalid/FundValuationLast",
        eastmoney_fund_info_url="https://info.example.invalid/FundMNFInfo",
        publication_max_snapshot_age_days=10)


def fetch(settings, *, primary=None, info=None, now=NOW):
    primary = payload("valuation") if primary is None else primary
    info = payload("info") if info is None else info
    sent = []
    def post(url, **kwargs):
        sent.append((url, kwargs))
        return response(primary)
    def get(url, **kwargs):
        sent.append((url, kwargs))
        return response(info)
    lookup, meta = validation.fetch_cross_validation(settings, MANIFEST["codes"], post=post, get=get, now=now)
    return lookup, meta, sent


def local_row(code):
    source = MANIFEST["database_reference"]["records"][code]["fund_returns"]
    name = next(row["SHORTNAME"] for row in payload("valuation")["data"] if row["FCODE"] == code)
    return {"基金代码": code, "基金名称": name, "基金类型": "债券型",
            "最新单位净值": source["latest_nav"], "最新净值日期": source["latest_nav_date"],
            "区间收益基准日": source["period_return_date"],
            **{column: source[field] for field, column in validation.LOCAL_RETURN_MAP.items()}}


def test_actual_frozen_responses_match_sql_and_keep_non_independence(settings):
    for source in MANIFEST["sources"]:
        raw = gzip.decompress((FIXTURES / source["file"]).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == source["http_body_sha256"]
    lookup, meta, sent = fetch(settings)
    assert set(lookup) == set(MANIFEST["codes"]) and len(sent) == 2
    assert meta["source_complete"] and meta["source_available"] and meta["missing_gate_passed"]
    assert meta["independent_source"] is False and meta["cache_reused_count"] == 0
    assert meta["provider"] == "eastmoney_cross_endpoint" and meta["contract"] == validation.CONTRACT
    assert meta["source_evidence"]["request_count"] == 2
    for code, upstream in lookup.items():
        result = validation.compare_endpoint_candidate(local_row(code), upstream)
        assert result["passed"] and not result["warning_reasons"]
        assert result["details"]["独立供应商校验"] is False
        for kind in ("valuation", "info"):
            evidence = upstream["_source_evidence"][kind]
            body = gzip.decompress(Path(evidence["response_artifact"]).read_bytes())
            assert hashlib.sha256(body).hexdigest() == evidence["response_sha256"]
            assert evidence["request_started_at"] and evidence["fetched_at"]
    assert "FUNDTYPE" in sent[0][1]["data"]["FIELDS"]
    assert sent[1][1]["params"]["pageIndex"] == 1 and sent[1][1]["params"]["pageSize"] == 20
    assert all(sent[1][1]["params"][key] for key in ("plat", "deviceid", "product", "Version", "appType"))


@pytest.mark.parametrize("value,state", [(None, "upstream_null"), ("--", "upstream_null"),
    ("", "upstream_null"), (float("nan"), "parse_failed"), (float("inf"), "parse_failed"),
    (False, "parse_failed"), ("not-a-number", "parse_failed")])
def test_null_and_nonfinite_returns_cannot_become_zero_or_soft_pass(settings, value, state):
    primary = payload("valuation")
    code = primary["data"][0]["FCODE"]
    primary["data"][0]["SYL_Z"] = value
    lookup, meta, _ = fetch(settings, primary=primary)
    row = lookup[code]
    assert row["return_1w"] is None and row["_field_states"]["return_1w"] == state
    comparison = validation.compare_endpoint_candidate(local_row(code), row)
    assert not comparison["passed"] and "return_1w_missing" in comparison["hard_reasons"]
    assert "return_1w_soft_mismatch" not in comparison["warning_reasons"]
    assert not meta["source_complete"]


def test_real_zero_is_distinct_from_absence(settings):
    primary = payload("valuation")
    code = primary["data"][0]["FCODE"]
    primary["data"][0]["SYL_Z"] = 0
    lookup, _, _ = fetch(settings, primary=primary)
    local = local_row(code)
    local["近1周(%)"] = 0
    assert lookup[code]["return_1w"] == 0 and lookup[code]["_field_states"]["return_1w"] == "real_zero"
    assert validation.compare_endpoint_candidate(local, lookup[code])["passed"]


@pytest.mark.parametrize("field,value,reason", [
    ("PDATE", "2026-09-01", "external_nav_date_stale"),
    ("PDATE", "2026-10-14", "external_nav_date_future"),
    ("PDATE", None, "external_nav_date_invalid"),
    ("FUNDTYPE", "005", "monetary_nav_semantics_unsupported"),
    ("FUNDTYPE", None, "external_type_missing_or_unknown"),
    ("FUNDTYPE", "999", "external_type_missing_or_unknown"),
    ("NAV", 0, "external_nav_invalid"),
])
def test_invalid_or_unsupported_nav_semantics_never_pass(settings, field, value, reason):
    primary = payload("valuation")
    code = primary["data"][0]["FCODE"]
    primary["data"][0][field] = value
    lookup, _, _ = fetch(settings, primary=primary)
    result = validation.compare_endpoint_candidate(local_row(code), lookup[code])
    assert not result["passed"] and reason in result["hard_reasons"]


@pytest.mark.parametrize("field,value,reason", [("NAV", "9.9", "endpoint_nav_mismatch"),
    ("PDATE", "2026-10-09", "endpoint_nav_date_mismatch"), ("PDATE", None, "endpoint_nav_date_mismatch")])
def test_endpoint_rows_must_confirm_nav_and_own_date(settings, field, value, reason):
    info = payload("info")
    code = info["Datas"][0]["FCODE"]
    info["Datas"][0][field] = value
    info["Expansion"] = {"FSRQ": "2026-10-12"}
    lookup, _, _ = fetch(settings, info=info)
    assert reason in lookup[code]["_contract_errors"]
    assert not validation.compare_endpoint_candidate(local_row(code), lookup[code])["passed"]


@pytest.mark.parametrize("mutation", ["business", "error_string", "null", "unexpected", "duplicate"])
def test_mnfinfo_failures_are_observable_unavailable_sources(settings, mutation):
    info = payload("info")
    if mutation == "business":
        info["Success"] = False
    elif mutation == "error_string":
        info["ErrorCode"] = "61136"
    elif mutation == "null":
        info["Datas"] = None
    elif mutation == "unexpected":
        info["Datas"][0]["FCODE"] = "999999"
    else:
        info["Datas"][1]["FCODE"] = info["Datas"][0]["FCODE"]
    lookup, meta, _ = fetch(settings, info=info)
    assert not meta["source_available"] and not meta["source_complete"]
    assert all("fund_info_missing" in row["_contract_errors"] for row in lookup.values())
    if mutation != "null":
        assert meta["last_error"]


def test_transport_fallback_is_bounded_and_does_not_query_autostock(settings):
    calls = []
    def post(url, **kwargs):
        calls.append(url)
        if url == settings.tiantian_valuation_primary_url:
            raise requests.ConnectTimeout("isolated failure")
        return response(payload("valuation"))
    lookup, meta = validation.fetch_cross_validation(settings, MANIFEST["codes"], post=post,
        get=lambda *args, **kwargs: response(payload("info")), now=NOW)
    assert len(calls) == 2 and all("autostock" not in url for url in calls)
    assert meta["source_complete"] and meta["last_error"] is None
    assert len(lookup) == 20 and meta["request_count"] == 3


def test_explicit_pagination_collects_short_pages_and_rejects_repetition(settings):
    calls = []
    original = payload("info")
    def get(url, **kwargs):
        page = kwargs["params"]["pageIndex"]
        calls.append(page)
        current = deepcopy(original)
        current["Datas"] = original["Datas"][(page - 1) * 10:page * 10]
        return response(current)
    _, meta = validation.fetch_cross_validation(settings, MANIFEST["codes"],
        post=lambda *args, **kwargs: response(payload("valuation")), get=get, now=NOW)
    assert calls == [1, 2] and meta["source_complete"]
    def repeated(url, **kwargs):
        current = deepcopy(original)
        current["Datas"] = original["Datas"][:10]
        return response(current)
    _, meta = validation.fetch_cross_validation(settings, MANIFEST["codes"],
        post=lambda *args, **kwargs: response(payload("valuation")), get=repeated, now=NOW)
    assert not meta["source_available"] and meta["last_error"] == "pagination_or_identity_invalid"
    for attempt in meta["attempts"]:
        assert json.loads(Path(attempt["metadata_artifact"]).read_text()) == attempt


def test_production_return_tolerances_and_one_week_only_warning_are_preserved(settings):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))
    from bond_fund_screening.tasks.final_screening_autostock_helpers import compare_return_with_autostock
    for local, external in [(0, 0), (0.9, 0.1), (2.3, 2), (-2.1, -2), (None, 0), (0.5, 0)]:
        expected, old = compare_return_with_autostock(local, external, validation.DEFAULT_CONFIG)
        actual, new = validation._return_compare(local, external, validation.DEFAULT_CONFIG)
        assert actual == expected
        for field in ("mode", "relative_diff_pct", "absolute_diff_pct_point"):
            assert new[field] == old[field]
    lookup, _, _ = fetch(settings)
    code = MANIFEST["codes"][0]
    local = local_row(code)
    local["近1周(%)"] = 9.0
    result = validation.compare_endpoint_candidate(local, lookup[code])
    assert result["passed"] and result["warning_reasons"] == ["return_1w_soft_mismatch"]
    local["近1月(%)"] = 9.0
    result = validation.compare_endpoint_candidate(local, lookup[code])
    assert not result["passed"] and {"return_1w_mismatch", "return_1m_mismatch"} <= set(result["hard_reasons"])


def test_nav_tolerance_conjunction_and_date_lag_keep_independent_return_date_gate(settings):
    lookup, _, _ = fetch(settings)
    code = MANIFEST["codes"][0]
    local = local_row(code)
    # Absolute difference exceeds .001; relative difference remains below 1%.
    local["最新单位净值"] += 0.005
    assert validation.compare_endpoint_candidate(local, lookup[code])["passed"]
    local["最新净值日期"] = "2026-10-13"
    result = validation.compare_endpoint_candidate(local, lookup[code])
    assert result["passed"] and "latest_nav_date_soft_lag" in result["warning_reasons"]
    local["区间收益基准日"] = "2026-10-13"
    result = validation.compare_endpoint_candidate(local, lookup[code])
    assert not result["passed"] and "period_return_date_mismatch" in result["hard_reasons"]


def test_frame_null_is_hard_block_and_overall_source_failure_raises(settings, monkeypatch):
    lookup, meta, _ = fetch(settings)
    code = MANIFEST["codes"][0]
    lookup[code]["return_1m"] = None
    monkeypatch.setattr(validation, "fetch_cross_validation", lambda *args, **kwargs: (lookup, meta))
    candidates = pd.DataFrame({"fcode": MANIFEST["codes"]})
    local = pd.DataFrame([local_row(code) for code in MANIFEST["codes"]])
    validated, failed, summary, failed_df = validation.validate_candidate_frame(settings, candidates, local)
    assert len(validated) == 19 and failed == {code} and summary["filtered_count"] == 1
    assert failed_df.iloc[0]["失败原因"] == "return_1m_missing"
    assert validation.validation_status_map(summary["_detail_records"])[code]["validation_status"] == "hard_block"
    meta["source_available"] = False
    with pytest.raises(RuntimeError, match="source_unavailable"):
        validation.validate_candidate_frame(settings, candidates, local)


@pytest.mark.parametrize("missing_count,count,error,allowed", [(1, 499, None, False), (1, 500, None, True),
    (2, 1000, None, True), (3, 2000, None, False), (1, 500, "timeout", False)])
def test_original_residual_missing_count_and_ratio_and_no_error_gate(settings, monkeypatch, missing_count, count, error, allowed):
    # Synthetic identities exercise aggregate gating only; no synthetic NAV or
    # return is supplied and no row is claimed to be a financial match.
    codes = [f"{number:06d}" for number in range(1, count + 1)]
    monkeypatch.setattr(validation, "fetch_cross_validation", lambda *args, **kwargs: ({}, {
        "source_available": True, "missing_count": missing_count, "last_error": error, "source_evidence": {}}))
    candidates = pd.DataFrame({"fcode": codes})
    local = pd.DataFrame({"基金代码": codes})
    if allowed:
        validated, failed, summary, _ = validation.validate_candidate_frame(settings, candidates, local)
        assert validated.empty and len(failed) == count
        assert summary["batch_meta"]["residual_missing_demoted_to_hard_block"]
    else:
        with pytest.raises(RuntimeError, match="endpoint_validation_incomplete"):
            validation.validate_candidate_frame(settings, candidates, local)


def test_missing_result_or_evidence_cannot_be_relabelled_as_strict_match(settings):
    assert validation.validation_status_map([{"基金代码": "164703"}])["164703"]["validation_status"] == "not_checked"
    lookup, _, _ = fetch(settings)
    code = MANIFEST["codes"][0]
    lookup[code].pop("_source_evidence")
    result = validation.compare_endpoint_candidate(local_row(code), lookup[code])
    assert not result["passed"] and "source_evidence_missing" in result["hard_reasons"]
    missing_reason = {"基金代码": code, "失败原因": "", "告警原因": "", "校验来源": validation.PROVIDER,
                      "校验契约版本": validation.CONTRACT, "校验完成": True, "校验通过": False}
    mapped = validation.validation_status_map([missing_reason])[code]
    assert mapped["validation_status"] == "hard_block" and "validation_failure_reason_missing" in mapped["validation_hard_reasons"]
