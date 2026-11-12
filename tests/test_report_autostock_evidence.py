"""Isolated failure injection for shared supplement results and frozen evidence."""
from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend import report_builder
from backend import report_autostock_evidence as evidence
from bond_fund_screening.tasks.final_screening_autostock_helpers import build_autostock_validation_status_map


def rows(*codes: str) -> pd.DataFrame:
    # No fabricated NAV or returns: these orchestration fixtures have missing values.
    return pd.DataFrame([
        {**{column: None for column in evidence.COMPARE_COLUMNS},
         "基金代码": code, "基金名称": "isolated fixture", "AutoStock原始校验状态": "not_checked",
         "AutoStock告警原因": "", "AutoStock校验状态": "未校验", "AutoStock校验备注": ""}
        for code in codes
    ])


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    calls = []
    responses = [[{"code": "000001", "fixture_failure": "latest_nav_mismatch"}]]

    class Client:
        SAFE_FUND_BATCH_SIZE = 50

        def __init__(self, **kwargs):
            pass

        def get_fund_batch_with_meta(self, codes, batch_size):
            calls.append(list(codes))
            batch = responses[min(len(calls) - 1, len(responses) - 1)]
            if isinstance(batch, Exception):
                raise batch
            return batch, {"returned_total": len(batch), "returned_unique_total": len(batch),
                           "missing_count": len(codes) - len(batch)}

    def compare(local, upstream, config):
        reason = upstream.get("fixture_failure", "")
        warning = upstream.get("fixture_warning", "")
        return {"passed": not reason, "hard_reasons": [reason] if reason else [],
                "warning_reasons": [warning] if warning else [],
                "details": {"基金代码": local["基金代码"], "失败原因": reason, "告警原因": warning}}

    monkeypatch.setattr(report_builder, "_load_latest_official_autostock_cache", lambda settings: {})
    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report",
                        lambda settings: (Client, build_autostock_validation_status_map, compare))
    monkeypatch.setattr(report_builder, "read_autostock_quota_snapshot", lambda **kwargs: {})
    monkeypatch.setattr(report_builder, "plan_autostock_call_budget",
                        lambda **kwargs: {"allowed_call_count": kwargs["needed_call_count"], "quota_remaining": 100})
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
                               autostock_base_url="https://example.invalid/v1",
                               autostock_rate_limit_shared_state_path=tmp_path / "quota.json",
                               autostock_rate_limit_max_calls_per_hour=100,
                               autostock_rate_limit_min_interval_seconds=2.0,
                               autostock_batch_fund_default=50)
    return settings, calls, responses


def test_shared_hard_block_cannot_be_replaced_by_a_second_response(runtime):
    settings, calls, responses = runtime
    responses.append([{"code": "000001"}])
    context = {}
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        rows("000001"), rows("000001"), settings=settings, status_context=context,
    )
    assert calls == [["000001"]]
    for frame in (formal, dynamic):
        kept, rejected = report_builder._demote_autostock_hard_block_rows(frame)
        assert kept.empty and rejected["基金代码"].tolist() == ["000001"]
        assert "latest_nav_mismatch" in rejected.iloc[0]["AutoStock校验备注"]
    record = context["autostock_supplement_records"][0]
    assert record["comparisons"][0]["result"]["hard_reasons"] == ["latest_nav_mismatch"]
    assert record["upstream_records"] == responses[0]


@pytest.mark.parametrize("response,outcome", [([], "empty_response"), (RuntimeError("transport fixture"), "batch_failed")])
def test_missing_or_failed_response_is_not_retried_by_the_second_track(runtime, response, outcome):
    settings, calls, responses = runtime
    responses[:] = [response]
    context = {}
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        rows("000001"), rows("000001"), settings=settings, status_context=context,
    )
    assert calls == [["000001"]]
    assert formal.iloc[0]["AutoStock原始校验状态"] == dynamic.iloc[0]["AutoStock原始校验状态"] == "not_checked"
    record = context["autostock_supplement_records"][0]
    assert record["outcome"] == outcome and not record["comparisons"]
    assert record["upstream_records"] == ([] if isinstance(response, list) else None)


def test_partial_union_retains_missing_members_without_an_extra_query(runtime):
    settings, calls, responses = runtime
    responses[:] = [[{"code": "000001", "fixture_warning": "type_mismatch"}]]
    context = {}
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        rows("000001", "000002"), rows("000001"), settings=settings, status_context=context,
    )
    assert calls == [["000001", "000002"]]
    assert formal["基金代码"].tolist() == ["000001", "000002"]
    assert formal["AutoStock原始校验状态"].tolist() == ["soft_pass_with_warnings", "not_checked"]
    assert context["autostock_supplement_records"][0]["upstream_metadata"]["missing_count"] == 1


def test_new_strict_result_clears_old_warning_text_on_both_tracks(runtime):
    settings, calls, responses = runtime
    responses[:] = [[{"code": "000001"}]]
    old = rows("000001")
    old["AutoStock告警原因"] = "previous unavailable fixture"
    old["AutoStock校验备注"] = "previous unavailable fixture"
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        old, old, settings=settings, status_context={},
    )
    for frame in (formal, dynamic):
        assert frame.iloc[0]["AutoStock原始校验状态"] == "strict_match"
        assert frame.iloc[0]["AutoStock告警原因"] == frame.iloc[0]["AutoStock校验备注"] == ""


@pytest.mark.parametrize("mode", ["success", "empty", "exception", "budget", "already_checked"])
def test_existing_soft_warning_remains_visible_when_another_fund_is_supplemented(runtime, monkeypatch, mode):
    settings, calls, responses = runtime
    responses[:] = [[{"code": "000001"}]]
    if mode == "empty":
        responses[:] = [[]]
    elif mode == "exception":
        responses[:] = [RuntimeError("transport fixture")]
    elif mode == "budget":
        monkeypatch.setattr(report_builder, "plan_autostock_call_budget", lambda **kwargs: {"allowed_call_count": 0})
    current = rows("000001", "000002") if mode != "already_checked" else rows("000002")
    known = current["基金代码"].eq("000002")
    current.loc[known, "AutoStock原始校验状态"] = "soft_pass_with_warnings"
    current.loc[known, "AutoStock告警原因"] = "基金类型口径差异"
    current.loc[known, "AutoStock校验状态"] = "通过（含软告警）"
    context = {}
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        current, current, settings=settings, status_context=context,
    )
    assert calls == ([] if mode in {"budget", "already_checked"} else [["000001"]])
    for frame in (formal, dynamic):
        row = frame.set_index("基金代码").loc["000002"]
        assert row["AutoStock原始校验状态"] == "soft_pass_with_warnings"
        assert row["AutoStock告警原因"] == row["AutoStock校验备注"] == "基金类型口径差异"
    record = context["autostock_supplement_records"][0]
    original_rows = [dict(zip(record["input_frame"]["columns"], row)) for row in record["input_frame"]["rows"]]
    assert next(row for row in original_rows if row["基金代码"] == "000002")["AutoStock校验备注"] == ""
    assert next(row for row in record["output_statuses"] if row["基金代码"] == "000002")["AutoStock校验备注"] == "基金类型口径差异"


def test_already_checked_hard_block_preserves_its_specific_reason(runtime):
    settings, calls, responses = runtime
    current = rows("000001")
    current["AutoStock原始校验状态"] = "hard_block"
    current["AutoStock校验备注"] = "latest_nav_mismatch"
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        current, current, settings=settings, status_context={},
    )
    assert not calls
    for frame in (formal, dynamic):
        assert frame.iloc[0]["AutoStock原始校验状态"] == "hard_block"
        assert frame.iloc[0]["AutoStock校验备注"] == "latest_nav_mismatch"


def test_disagreeing_existing_results_do_not_choose_a_pass(runtime):
    settings, calls, responses = runtime
    formal, dynamic = rows("000001"), rows("000001")
    formal["AutoStock原始校验状态"] = "strict_match"
    dynamic["AutoStock原始校验状态"] = "hard_block"
    with pytest.raises(ValueError, match="existing_status_mismatch"):
        report_builder._supplement_report_autostock_tracks(formal, dynamic, settings=settings, status_context={})
    assert not calls


def test_conflicting_local_inputs_stop_before_any_request(runtime):
    settings, calls, responses = runtime
    formal, dynamic = rows("000001"), rows("000001")
    formal["最新单位净值"] = 0.0  # Boundary injection: zero must not equal missing.
    with pytest.raises(ValueError, match="comparison_inputs_mismatch"):
        report_builder._supplement_report_autostock_tracks(formal, dynamic, settings=settings, status_context={})
    assert not calls


def test_budget_skip_records_no_received_response(runtime, monkeypatch):
    settings, calls, responses = runtime
    monkeypatch.setattr(report_builder, "plan_autostock_call_budget", lambda **kwargs: {"allowed_call_count": 0})
    context = {}
    report_builder._supplement_report_autostock_tracks(rows("000001"), rows("000001"), settings=settings, status_context=context)
    record = context["autostock_supplement_records"][0]
    assert not calls and record["outcome"] == "budget_unavailable"
    assert record["upstream_records"] is None and record["upstream_records_sha256"] is None
    assert record["logical_request_submitted_to_client"] is False
    assert record["logical_request"]["ordered_codes"] == ["000001"]
    assert record["logical_request_sha256"] == evidence.content_sha256(record["logical_request"])


def test_snapshot_round_trip_identity_and_corruption(runtime):
    settings, calls, responses = runtime
    context = {}
    report_builder._supplement_report_autostock_tracks(rows("000001"), rows("000001"), settings=settings, status_context=context)
    run_dir = settings.reports_dir / "runs" / "fixture_run" / "revisions" / "fixture_revision"
    run_dir.mkdir(parents=True)
    for name in evidence.CODE_FILES:
        path = settings.project_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("isolated source identity fixture\n")
    descriptor = evidence.write_supplement_snapshot(project_root=settings.project_root, run_dir=run_dir,
        run_id="fixture_run", generated_at="2026-09-08T00:00:00", context=context)
    loaded = evidence.load_supplement_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
        run_id="fixture_run", revision="fixture_revision")
    assert loaded["records"][0]["comparisons"][0]["local_row"]["最新单位净值"] is None
    assert descriptor["comparison_count"] == 1
    with pytest.raises(ValueError, match="identity_mismatch"):
        evidence.load_supplement_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
            run_id="fixture_run", revision="another_revision")
    path = settings.project_root / descriptor["path"]
    path.write_bytes(path.read_bytes()[:-1] + b"x")
    with pytest.raises(ValueError, match="file_hash_mismatch"):
        evidence.load_supplement_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
            run_id="fixture_run", revision="fixture_revision")
