"""Isolated evidence contracts; synthetic rows are never fund business evidence."""
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from backend import prospective_autostock as capture
from backend import report_autostock_evidence as evidence
from backend import report_builder as production
from backend.screening_replay_snapshot import freeze_frame
from bond_fund_screening.tasks.final_screening_autostock_helpers import build_autostock_validation_status_map


def rows(count):
    return pd.DataFrame([
        {**{key: None for key in evidence.COMPARE_COLUMNS}, "基金代码": f"{number:06d}",
         "基金名称": "isolated fixture", **{key: "" for key in evidence.STATUS_COLUMNS},
         "AutoStock原始校验状态": "not_checked"}
        for number in range(1, count + 1)
    ])


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    calls, response_modes = [], []
    budgets = []

    class Client:
        SAFE_FUND_BATCH_SIZE = 50

        def __init__(self, **kwargs):
            pass

        def get_fund_batch_with_meta(self, codes, batch_size):
            assert batch_size <= 50 and len(codes) <= 50
            calls.append(list(codes))
            mode = response_modes.pop(0) if response_modes else "all"
            if isinstance(mode, Exception):
                raise mode
            selected = codes[:-1] if mode == "partial" else [] if mode == "empty" else codes
            values = [{"code": code, "fixture_failure": "fixture_hard_block" if mode == "blocked" else ""}
                      for code in selected]
            return values, {"returned_total": len(values), "returned_unique_total": len(values),
                            "missing_count": len(codes) - len(values), "http_attempt_count": 1}

    def compare(local, upstream, configuration):
        reason = upstream.get("fixture_failure", "")
        return {"passed": not reason, "hard_reasons": [reason] if reason else [], "warning_reasons": [],
                "details": {"基金代码": local["基金代码"], "失败原因": reason, "告警原因": ""}}

    def budget(**kwargs):
        budgets.append(kwargs)
        return {"allowed_call_count": kwargs["needed_call_count"], "quota_remaining": 100}

    monkeypatch.setattr(production, "_load_autostock_runtime_for_report",
                        lambda settings: (Client, build_autostock_validation_status_map, compare))
    monkeypatch.setattr(production, "_load_latest_official_autostock_cache",
                        lambda settings: pytest.fail("new research observation consulted the publication cache"))
    monkeypatch.setattr(production, "read_autostock_quota_snapshot", lambda **kwargs: {})
    monkeypatch.setattr(production, "plan_autostock_call_budget", budget)
    monkeypatch.setattr(evidence, "comparison_code_hashes", lambda root: {})

    def plan(settings, *, universe, **kwargs):
        codes = universe["基金代码"].tolist()
        return {"target_codes": codes, "production_target_codes": [], "inherited_compared_codes": [],
                "required_codes": sorted(codes), "families": [{"family": "isolated", "required_codes": codes}],
                "outside_scope": ["all_business_results"], "max_batches": capture.MAX_BATCHES,
                "batch_size": 50}, universe.copy()

    monkeypatch.setattr(capture, "build_observation_plan", plan)
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
        autostock_base_url="https://example.invalid/v1", autostock_rate_limit_shared_state_path=tmp_path / "quota.json",
        autostock_rate_limit_max_calls_per_hour=100, autostock_rate_limit_min_interval_seconds=2.0,
        autostock_batch_fund_default=50)
    return settings, calls, response_modes, budgets


def collect(runtime, count=2):
    universe = rows(count)
    result = capture.capture_strategy_observations(runtime[0], run_id="isolated", revision="r1",
        generated_at="2020-01-01T00:00:00", universe=universe, filters={}, report_inputs={})
    return result, universe


def verify(runtime, result, universe, *, statuses=None, protected=()):
    empty = pd.DataFrame({"基金代码": pd.Series(dtype=str)})
    frozen = {"generated_at": "2020-01-01T00:00:00", "inputs": {
        "prospective_autostock_capture": result,
        "frames": {"formal_after_all_filters": freeze_frame(empty),
                   "dynamic_reference": freeze_frame(pd.DataFrame({"基金代码": list(protected)}))}}}
    report = {"run_id": "isolated", "publication_control": {"revision": "r1"}}
    return capture.validate_strategy_observations(runtime[0], frozen=frozen, report=report,
        universe=universe, production_statuses=statuses or {})


def resign(result, record=None):
    if record is not None:
        record["record_sha256"] = evidence.content_sha256({key: value for key, value in record.items() if key != "record_sha256"})
    capture._signed(result)


def test_complete_batches_preserve_sources_and_do_not_modify_original_rows(runtime):
    result, universe = collect(runtime, 101)
    assert [len(batch) for batch in runtime[1]] == [50, 50, 1]
    assert result["status"] == "captured" and result["compared_count"] == 101
    assert result["production_results_modified"] is False and result["final_selection_computed"] is False
    pd.testing.assert_frame_equal(universe, rows(101))
    checked, observed = verify(runtime, result, universe)
    assert checked["integrity_verified"] and len(observed) == 101
    assert all(call["reserve_calls"] == 12 for call in runtime[3])
    for record in result["records"]:
        assert record["purpose"] == "strategy_observation" and not record["cache_reuses"]
        assert record["upstream_records"] is not None


@pytest.mark.parametrize("mode,expected", [("partial", 1), ("empty", 0), (RuntimeError("fixture timeout"), 0)])
def test_missing_or_failed_api_response_is_not_success_or_zero_business_data(runtime, mode, expected):
    runtime[2].append(mode)
    result, universe = collect(runtime)
    assert result["status"] == "incomplete" and result["compared_count"] == expected
    assert len(result["missing_target_codes"]) == 2 - expected
    checked, observed = verify(runtime, result, universe)
    assert checked["status"] == "incomplete" and len(observed) == expected
    assert universe["最新单位净值"].isna().all()


def test_budget_stops_without_querying_or_fabricating_observations(runtime, monkeypatch):
    monkeypatch.setattr(production, "plan_autostock_call_budget", lambda **kwargs: {"allowed_call_count": 0})
    result, universe = collect(runtime, 80)
    assert not runtime[1] and result["status"] == "incomplete"
    assert result["missing_target_codes"] == sorted(universe["基金代码"])
    assert result["records"][0]["outcome"] == "budget_unavailable"
    assert verify(runtime, result, universe)[0]["compared_count"] == 0


def test_fully_unavailable_batch_stops_later_requests(runtime):
    runtime[2].append("empty")
    result, universe = collect(runtime, 101)
    assert len(runtime[1]) == 1 and result["missing_target_codes"] == sorted(universe["基金代码"])
    assert verify(runtime, result, universe)[0]["status"] == "incomplete"
    assert capture.capture_summary(result)["http_attempt_count"] == 1


def test_transport_without_wire_count_keeps_count_unknown(runtime):
    runtime[2].append(RuntimeError("fixture response lost"))
    result, _ = collect(runtime)
    summary = capture.capture_summary(result)
    assert summary["submitted_batch_count"] == 1 and summary["http_attempt_count"] is None


def test_capture_batch_limit_preserves_unattempted_gap(runtime, monkeypatch):
    monkeypatch.setattr(capture, "MAX_BATCHES", 1)
    result, universe = collect(runtime, 51)
    assert len(runtime[1]) == 1 and result["missing_target_codes"] == ["000051"]
    assert verify(runtime, result, universe)[0]["missing_count"] == 1


def test_planning_failure_has_unknown_target_count_and_preserves_error(runtime, monkeypatch):
    def failed(*args, **kwargs):
        raise ValueError("fixture invalid source")
    monkeypatch.setattr(capture, "build_observation_plan", failed)
    result, universe = collect(runtime)
    assert result["status"] == "capture_failed" and result["target_count"] is None
    assert "fixture invalid source" in result["error"] and not runtime[1]
    assert verify(runtime, result, universe)[1] == {}


def test_raw_hard_block_remains_a_block_when_replayed(runtime):
    runtime[2].append("blocked")
    result, universe = collect(runtime)
    checked, observed = verify(runtime, result, universe)
    assert checked["status"] == "captured"
    assert all(row["AutoStock原始校验状态"] == "hard_block" for row in observed.values())
    assert all("fixture_hard_block" in row["AutoStock校验备注"] for row in observed.values())


@pytest.mark.parametrize("field,value", [("run_id", "other"), ("revision", "other"),
                                        ("production_results_modified", True)])
def test_other_revision_or_mutated_publication_claim_is_rejected(runtime, field, value):
    result, universe = collect(runtime)
    result[field] = value
    resign(result)
    with pytest.raises(ValueError, match="binding_invalid"):
        verify(runtime, result, universe)


def test_source_local_null_cannot_be_replaced_with_zero_even_after_resigning(runtime):
    result, universe = collect(runtime)
    record = result["records"][0]
    record["comparisons"][0]["local_row"]["最新单位净值"] = 0.0
    resign(result, record)
    with pytest.raises(ValueError, match="local_input_mismatch"):
        verify(runtime, result, universe)


def test_request_frame_must_match_observed_local_universe(runtime):
    result, universe = collect(runtime)
    record = result["records"][0]
    column = record["input_frame"]["columns"].index("最新单位净值")
    record["input_frame"]["rows"][0][column] = 0.0
    record["input_frame_sha256"] = evidence.content_sha256(record["input_frame"])
    resign(result, record)
    with pytest.raises(ValueError, match="record_input_mismatch"):
        verify(runtime, result, universe)


def test_comparison_result_is_recomputed_from_frozen_response(runtime):
    result, universe = collect(runtime)
    record = result["records"][0]
    record["comparisons"][0]["result"]["passed"] = False
    resign(result, record)
    with pytest.raises(ValueError, match="comparison_mismatch"):
        verify(runtime, result, universe)


def test_upstream_code_is_bound_to_the_requested_fund(runtime):
    result, universe = collect(runtime)
    record = result["records"][0]
    record["upstream_records"][0]["code"] = "999999"
    record["upstream_records_sha256"] = evidence.content_sha256(record["upstream_records"])
    record["comparisons"][0]["upstream_record_sha256"] = evidence.content_sha256(record["upstream_records"][0])
    resign(result, record)
    with pytest.raises(ValueError, match="upstream_code_mismatch"):
        verify(runtime, result, universe)


@pytest.mark.parametrize("raw", ["hard_block", "strict_match", "soft_pass_with_warnings"])
def test_production_compared_status_cannot_be_overwritten(runtime, raw):
    result, universe = collect(runtime)
    with pytest.raises(ValueError, match="overwrites_production"):
        verify(runtime, result, universe, statuses={"000001": {"AutoStock原始校验状态": raw}})


def test_original_unvalidated_member_cannot_be_retroactively_validated(runtime):
    result, universe = collect(runtime)
    with pytest.raises(ValueError, match="overwrites_production"):
        verify(runtime, result, universe, protected=["000001"],
               statuses={"000001": {"AutoStock原始校验状态": "not_checked"}})


@pytest.mark.parametrize("mutation", ["count", "missing", "timestamp", "unsigned"])
def test_incorrect_completeness_time_or_hash_is_rejected(runtime, mutation):
    result, universe = collect(runtime)
    if mutation == "count":
        result["compared_count"] += 1
    elif mutation == "missing":
        result["missing_target_codes"] = ["000001"]
    elif mutation == "timestamp":
        result["finished_at"] = (datetime.fromisoformat(result["started_at"]) - timedelta(seconds=1)).isoformat()
    else:
        result["observed_statuses"]["000001"]["AutoStock校验备注"] = "tampered"
    if mutation != "unsigned":
        resign(result)
    with pytest.raises(ValueError):
        verify(runtime, result, universe)


def test_legacy_snapshot_remains_explicitly_unavailable(runtime):
    check, observed = capture.validate_strategy_observations(runtime[0], frozen={"inputs": {}},
        report={}, universe=pd.DataFrame(), production_statuses={})
    assert check == {"available": False, "integrity_verified": None, "status": "not_captured"}
    assert observed == {} and not runtime[1]


def test_prospective_supplement_refuses_more_than_one_batch(runtime):
    context = {}
    production._supplement_official_autostock_statuses(rows(51), settings=runtime[0],
        status_context=context, purpose="strategy_observation")
    assert not runtime[1] and context["autostock_supplement_records"][0]["outcome"] == "budget_unavailable"
