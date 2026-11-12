"""Failure injection for labelled report validation and immutable evidence."""
from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend import fund_cross_validation as adapter
from backend import project_tracker, report_builder
from backend import report_cross_validation_evidence as evidence
from backend.project_publish_decisions import (
    DATA_VALIDATION_COLUMNS, append_project_decision_columns, normalize_autostock_status,
    validation_status_value,
)
from backend.project_report_exports import REFERENCE_CSV_COLUMNS


@pytest.mark.parametrize("provider", ["eastmoney_cross_endpoint", "autostock", "other_provider", ""])
def test_validation_gate_rule_matches_configured_provider(provider):
    rule = report_builder._validation_gate_rule(SimpleNamespace(publication_validation_provider=provider))
    if provider == "eastmoney_cross_endpoint":
        assert "AutoStock" not in rule
        assert "同供应商跨接口" in rule and "不代表独立供应商核验" in rule
        assert "硬校验失败样本不进入正式推荐主榜" in rule
        assert "软告警样本会保留显式标签" in rule
    elif provider == "autostock":
        assert rule.startswith("AutoStock 硬校验失败")
        assert "软告警样本会保留显式标签" in rule
    else:
        assert "AutoStock" not in rule
        assert (provider or "未配置") in rule
        assert "不能据此视为校验通过" in rule


def test_publish_summary_uses_the_configured_validation_gate_rule():
    settings = SimpleNamespace(publication_validation_provider="eastmoney_cross_endpoint")
    empty = pd.DataFrame()
    summary = report_builder._build_project_publish_summary(
        settings=settings, current_generated_at="2026-10-19T16:00:00", universe_snapshot={},
        candidate_df=empty, fixed_official_audit_df=empty, dynamic_publish_official_df=empty,
        removed_after_final_selection_df=empty, dynamic_baseline_codes=set(),
        dynamic_reference_export_codes=set(), fixed_baseline_codes=set(), fixed_only_codes=set(),
        dynamic_only_codes=set(), overlap_codes=set(), formal_group_distribution=[],
        stable_official_style_distribution=[], enhanced_official_style_distribution=[],
        official_style_distribution=[], official_company_distribution=[], fee_band_distribution=[],
        internal_watch_distribution=[], portfolio_summary={}, generic_holding_placeholder_watch_summary={},
        low_vol_positive_ratio_shadow_watch_summary={}, project_tracker_summary={},
        forward_validation_summary={}, stage_b_reason_summary={}, standard_screening_audit_summary={},
        publish_stage_audit_summary={}, final_selection_contraction_summary={}, dynamic_threshold_detail_df=empty,
        dynamic_official_style_distribution=[], dynamic_official_company_distribution=[],
        dynamic_fee_band_distribution=[], dynamic_internal_watch_distribution=[],
        annual_return_gate_shadow_summary={}, risk_metric_cap_audit={}, threshold_comparison_preview_df=empty,
        data_source_audit_summary={},
    )
    rules = summary["gate_rules"]
    assert report_builder._validation_gate_rule(settings) in rules
    assert not any("AutoStock" in rule for rule in rules)


def rows(*codes: str) -> pd.DataFrame:
    # These orchestration fixtures deliberately contain no fabricated NAV/return values.
    return pd.DataFrame([{**{column: None for column in report_builder.AUTOSTOCK_COMPARE_COLUMNS},
        "基金代码": code, "基金名称": "isolated fixture", "AutoStock原始校验状态": "strict_match",
        "AutoStock校验状态": "通过", "AutoStock告警原因": "old source warning", "AutoStock校验备注": "old source warning"}
        for code in codes])


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    calls = []
    failures = {"000001": "latest_nav_mismatch"}
    warnings = {}
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
        publication_validation_provider="eastmoney_cross_endpoint", autostock_enabled=False)

    def fetch(settings, codes):
        calls.append(list(codes))
        return {code: {"code": code, "injected_failure": failures.get(code),
                       "injected_warning": warnings.get(code)} for code in codes}, {
            "provider": adapter.PROVIDER, "contract": adapter.CONTRACT,
            "source_available": True, "source_complete": True,
            "source_evidence": {"provider": adapter.PROVIDER, "independent_source": False},
        }

    def compare(local, upstream, config):
        failure, warning = upstream.get("injected_failure"), upstream.get("injected_warning")
        return {"passed": not failure, "hard_reasons": [failure] if failure else [],
                "warning_reasons": [warning] if warning else [],
                "details": {"基金代码": local["基金代码"], "失败原因": failure or "",
                    "告警原因": warning or "", "校验来源": adapter.PROVIDER, "校验契约版本": adapter.CONTRACT,
                    "原始证据": {kind: {"endpoint": f"https://example.invalid/isolated/{kind}",
                                         "response_sha256": evidence.content_sha256({})}
                                 for kind in ("valuation", "info")},
                    "校验完成": True, "校验通过": not failure}}

    monkeypatch.setattr(adapter, "fetch_cross_validation", fetch)
    monkeypatch.setattr(adapter, "compare_endpoint_candidate", compare)
    def forbidden(*args, **kwargs):
        raise AssertionError("new provider must never use legacy AutoStock cache/client")
    monkeypatch.setattr(report_builder, "_load_latest_official_autostock_cache", forbidden)
    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report", forbidden)
    return settings, calls, failures, warnings


def test_report_rechecks_union_even_with_existing_conflicting_old_statuses(runtime):
    settings, calls, failures, warnings = runtime
    formal, dynamic = rows("000001", "000002"), rows("000001")
    dynamic["AutoStock原始校验状态"] = "hard_block"
    context = {}
    formal, dynamic = report_builder._supplement_report_autostock_tracks(
        formal, dynamic, settings=settings, status_context=context,
    )
    assert calls == [["000001", "000002"]]
    assert formal["数据校验原始状态"].tolist() == ["hard_block", "strict_match"]
    assert dynamic.iloc[0]["数据校验原始状态"] == "hard_block"
    assert formal.iloc[0]["AutoStock原始校验状态"] == "strict_match"
    assert dynamic.iloc[0]["AutoStock原始校验状态"] == "hard_block"
    for frame in (formal, dynamic):
        kept, blocked = report_builder._demote_autostock_hard_block_rows(frame)
        assert "000001" not in kept["基金代码"].tolist()
        assert blocked["基金代码"].tolist() == ["000001"]
    assert context["data_validation_track_union"]["overlap_codes"] == ["000001"]
    record = context["data_validation_records"][0]
    assert record["comparisons"][0]["result"]["hard_reasons"] == ["latest_nav_mismatch"]
    assert record["independent_source"] is False
    assert not context.get("autostock_supplement_records")


def test_new_result_clears_only_neutral_old_warning(runtime):
    settings, calls, failures, warnings = runtime
    failures.clear()
    old = rows("000001")
    old["数据校验原始状态"] = "soft_pass_with_warnings"
    old["数据校验备注"] = old["数据校验告警原因"] = "stale neutral warning"
    result = report_builder._supplement_official_autostock_statuses(old, settings=settings, status_context={})
    assert calls == [["000001"]]
    assert result.iloc[0]["数据校验原始状态"] == "strict_match"
    assert result.iloc[0]["数据校验备注"] == result.iloc[0]["数据校验告警原因"] == ""
    assert result.iloc[0]["AutoStock校验备注"] == "old source warning"
    assert result.iloc[0]["数据校验来源"] == adapter.PROVIDER


def test_new_warning_stays_labelled_without_overwriting_autostock(runtime):
    settings, calls, failures, warnings = runtime
    failures.clear()
    warnings["000001"] = "return_1w_soft_mismatch"
    result = report_builder._supplement_official_autostock_statuses(rows("000001"), settings=settings, status_context={})
    assert result.iloc[0]["数据校验原始状态"] == "soft_pass_with_warnings"
    assert result.iloc[0]["数据校验备注"] == "近1周收益口径差异"
    assert result.iloc[0]["AutoStock原始校验状态"] == "strict_match"


def test_source_failure_never_inherits_old_pass(runtime, monkeypatch):
    settings, calls, failures, warnings = runtime
    monkeypatch.setattr(adapter, "fetch_cross_validation", lambda settings, codes: ({}, {
        "provider": adapter.PROVIDER, "contract": adapter.CONTRACT,
        "source_available": False, "source_complete": False, "source_evidence": {},
    }))
    # Restore the real comparison, which must reject missing endpoint inputs.
    monkeypatch.undo()
    monkeypatch.setattr(adapter, "fetch_cross_validation", lambda settings, codes: ({}, {
        "provider": adapter.PROVIDER, "contract": adapter.CONTRACT,
        "source_available": False, "source_complete": False, "source_evidence": {},
    }))
    context = {}
    result = report_builder._supplement_official_autostock_statuses(rows("000001"), settings=settings, status_context=context)
    assert result.iloc[0]["数据校验原始状态"] == "hard_block"
    assert result.iloc[0]["AutoStock原始校验状态"] == "strict_match"
    assert context["data_validation_unavailable"]["source_complete"] is False
    assert context["data_validation_records"][0]["outcome"] == "source_unavailable"


@pytest.mark.parametrize("missing_gate_passed", [False, True])
def test_incomplete_source_honours_batch_gate_without_inheriting_a_pass(runtime, monkeypatch, missing_gate_passed):
    settings, calls, failures, warnings = runtime
    monkeypatch.undo()
    monkeypatch.setattr(adapter, "fetch_cross_validation", lambda settings, codes: ({"000001": {"code": "000001"}}, {
        "provider": adapter.PROVIDER, "contract": adapter.CONTRACT,
        "source_available": True, "source_complete": False,
        "missing_gate_passed": missing_gate_passed, "source_evidence": {},
    }))
    context = {}
    result = report_builder._supplement_official_autostock_statuses(
        rows("000001", "000002"), settings=settings, status_context=context,
    )
    assert result["数据校验原始状态"].tolist() == ["hard_block", "hard_block"]
    assert bool(context.get("data_validation_unavailable")) is (not missing_gate_passed)
    kept, rejected = report_builder._demote_autostock_hard_block_rows(result)
    assert kept.empty and len(rejected) == 2


@pytest.mark.parametrize("neutral", [None, "", "not_checked"])
def test_missing_neutral_status_cannot_fall_back_to_legacy_pass(neutral):
    row = {"数据校验原始状态": neutral, "数据校验来源": adapter.PROVIDER,
           "AutoStock原始校验状态": "strict_match"}
    assert normalize_autostock_status(validation_status_value(row)) != "strict_match"


def test_source_without_status_cannot_fall_back_to_legacy_pass():
    row = {"数据校验来源": adapter.PROVIDER, "AutoStock原始校验状态": "strict_match"}
    assert validation_status_value(row) == "not_checked"
    assert validation_status_value({"AutoStock原始校验状态": "hard_block"}) == "hard_block"


def test_old_tracker_blank_neutral_schema_keeps_real_legacy_hard_block():
    row = {**{column: "" for column in DATA_VALIDATION_COLUMNS}, "AutoStock原始校验状态": "hard_block"}
    assert validation_status_value(row) == "hard_block"


def test_new_core_decision_does_not_become_autostock_business_status():
    frame = pd.DataFrame([{"fcode": "000001", "基金名称": "isolated fixture"}])
    result = append_project_decision_columns(frame, decision_map={"000001": {
        "autostock_validation_status": "strict_match", "validation_source": adapter.PROVIDER,
        "validation_contract": adapter.CONTRACT}}, soft_warning_map={})
    assert result.iloc[0]["数据校验原始状态"] == "strict_match"
    assert result.iloc[0]["数据校验来源"] == adapter.PROVIDER
    assert result.iloc[0]["AutoStock原始校验状态"] == ""


def test_csv_and_tracker_keep_neutral_source_fields():
    assert set(DATA_VALIDATION_COLUMNS) <= set(REFERENCE_CSV_COLUMNS)
    assert set(DATA_VALIDATION_COLUMNS) <= set(project_tracker.PROJECT_TRACKER_EXPORT_COLUMNS)
    assert all(project_tracker.PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME[column] == f"最新{column}"
               for column in DATA_VALIDATION_COLUMNS)


def test_snapshot_identity_source_binding_and_corruption(runtime):
    settings, calls, failures, warnings = runtime
    context = {}
    report_builder._supplement_official_autostock_statuses(rows("000001"), settings=settings, status_context=context)
    run_dir = settings.reports_dir / "runs" / "fixture_run" / "revisions" / "fixture_revision"
    run_dir.mkdir(parents=True)
    for name in evidence.CODE_FILES:
        path = settings.project_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("isolated source identity fixture\n")
    descriptor = evidence.write_snapshot(project_root=settings.project_root, run_dir=run_dir,
        run_id="fixture_run", generated_at="2026-10-13T10:00:00", context=context)
    frozen = evidence.load_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
        run_id="fixture_run", revision="fixture_revision")
    assert frozen["records"][0]["local_rows"][0]["最新单位净值"] is None
    assert frozen["records"][0]["upstream_records"]["000001"]["code"] == "000001"
    with pytest.raises(ValueError, match="identity_mismatch"):
        evidence.load_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
            run_id="fixture_run", revision="other_revision")
    path = settings.project_root / descriptor["path"]
    path.write_bytes(path.read_bytes()[:-1] + b"x")
    with pytest.raises(ValueError, match="file_hash_mismatch"):
        evidence.load_snapshot(settings.project_root, descriptor, reports_dir=settings.reports_dir,
            run_id="fixture_run", revision="fixture_revision")
