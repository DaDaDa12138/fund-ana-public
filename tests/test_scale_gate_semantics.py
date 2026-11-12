from __future__ import annotations

import pandas as pd

from backend.project_quality_thresholds import (
    apply_dynamic_quality_thresholds,
    apply_enhanced_supplemental_gate,
    resolve_scale_for_thresholds,
)
from backend.project_publish_decisions import (
    confidence_from_enhanced_metrics,
    confidence_from_thresholds,
)
from backend.report_builder import _build_share_family_support_frame
from scripts.audit_enhanced_scale_gate_shadow import _gate_scope_verification


def _eligible_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "基金代码": "TEST01",
        "债基风格标签": "一级债增强",
        "基础门禁通过": True,
        "dynamic_threshold_verdict": "自动诊断",
        "1年-年化收益(%)": 5.0,
        "1年-夏普比率": 2.0,
        "1年-卡玛比率": 3.0,
        "1年-最大回撤(%)": 0.01,
        "1年-正收益比率": 60.0,
        "费率合计(%)": 0.50,
        "基金规模(亿)": 0.10,
        "合并规模(亿)": 2.50,
    }
    row.update(overrides)
    return row


def test_supplemental_gate_uses_the_same_aggregate_scale_as_dynamic_gate() -> None:
    result = apply_enhanced_supplemental_gate(pd.DataFrame([_eligible_row()]))

    assert bool(result.loc[0, "enhanced_supplemental_pass"]) is True
    assert "规模偏小" not in str(result.loc[0, "enhanced_supplemental_reason"])
    assert resolve_scale_for_thresholds(_eligible_row()) == 2.5


def test_dynamic_and_supplemental_gates_resolve_family_scale_consistently() -> None:
    row = _eligible_row()
    dynamic = apply_dynamic_quality_thresholds(
        pd.DataFrame([row]),
        style_thresholds={"一级债增强": {
            "weighted_score_min": 65.0,
            "short_score_min": 0.0,
            "long_score_min": 0.0,
            "annual_return_min": 4.0,
            "sharpe_min": 0.0,
            "sortino_min": 0.0,
            "calmar_min": 2.0,
            "positive_ratio_min": 50.0,
            "fee_total_max": 1.0,
            "max_drawdown_max": 0.08,
            "volatility_max": 0.12,
            "scale_billion_min": 3.0,
        }},
        global_thresholds={},
        style_meta={"一级债增强": {"threshold_source": "test"}},
    )
    supplemental = apply_enhanced_supplemental_gate(dynamic)

    assert "scale_too_small" in str(dynamic.loc[0, "dynamic_threshold_reason_codes"])
    assert bool(supplemental.loc[0, "enhanced_supplemental_pass"]) is True


def test_missing_scale_remains_a_supplemental_gate_failure() -> None:
    row = _eligible_row(**{"基金规模(亿)": None, "合并规模(亿)": None})
    result = apply_enhanced_supplemental_gate(pd.DataFrame([row]))

    assert bool(result.loc[0, "enhanced_supplemental_pass"]) is False
    assert "规模偏小" in str(result.loc[0, "enhanced_supplemental_reason"])
    assert resolve_scale_for_thresholds(row) is None


def test_real_zero_aggregate_scale_is_not_replaced_by_share_scale() -> None:
    row = _eligible_row(**{"基金规模(亿)": 10.0, "合并规模(亿)": 0.0})

    assert resolve_scale_for_thresholds(row) == 0.0
    result = apply_enhanced_supplemental_gate(pd.DataFrame([row]))
    assert bool(result.loc[0, "enhanced_supplemental_pass"]) is False
    assert "规模偏小" in str(result.loc[0, "enhanced_supplemental_reason"])


def test_nonfinite_family_scale_falls_back_to_finite_share_scale() -> None:
    row = _eligible_row(**{"基金规模(亿)": 2.5, "合并规模(亿)": float("inf")})

    assert resolve_scale_for_thresholds(row) == 2.5


def test_confidence_scores_use_the_same_family_scale_contract() -> None:
    row = _eligible_row(**{"基金规模(亿)": 0.1, "合并规模(亿)": 10.0})
    _, _, regular_note = confidence_from_thresholds(
        row,
        thresholds={
            "weighted_score_min": 60.0,
            "short_score_min": 50.0,
            "long_score_min": 50.0,
            "annual_return_min": 4.0,
            "sharpe_min": 1.0,
            "sortino_min": 1.0,
            "calmar_min": 2.0,
            "positive_ratio_min": 50.0,
            "fee_total_max": 1.0,
            "max_drawdown_max": 0.08,
            "volatility_max": 0.12,
            "scale_billion_min": 2.0,
        },
    )
    _, _, enhanced_note = confidence_from_enhanced_metrics(
        row,
        thresholds={
            "annual_return_min": 4.5,
            "sharpe_min": 1.1,
            "calmar_min": 2.6,
            "positive_ratio_min": 55.0,
            "max_drawdown_max": 0.08,
            "fee_total_max": 1.1,
            "scale_billion_min": 2.0,
        },
    )

    assert "规模边际充足" in regular_note
    assert "规模边际充足" in enhanced_note


def test_family_scale_stays_unknown_if_any_share_scale_is_missing() -> None:
    metadata = pd.DataFrame([
        {"fcode": "000001", "shortname": "测试债券A", "company_id": "C1", "scale_billion": 1.0,
         "scale_report_date": "2026-06-30", "ftype": "债券型"},
        {"fcode": "000002", "shortname": "测试债券C", "company_id": "C1", "scale_billion": None,
         "scale_report_date": "2026-06-30", "ftype": "债券型"},
    ])

    family = _build_share_family_support_frame(metadata)

    assert family["share_class_count"].tolist() == [2, 2]
    assert family["family_scale_billion"].isna().all()


def test_complete_family_scale_sums_known_share_classes() -> None:
    metadata = pd.DataFrame([
        {"fcode": "000001", "shortname": "测试债券A", "company_id": "C1", "scale_billion": 1.0,
         "scale_report_date": "2026-06-30", "ftype": "债券型"},
        {"fcode": "000002", "shortname": "测试债券C", "company_id": "C1", "scale_billion": 2.0,
         "scale_report_date": "2026-06-30", "ftype": "债券型"},
    ])

    family = _build_share_family_support_frame(metadata)

    assert family["family_scale_billion"].tolist() == [3.0, 3.0]


def test_gate_shadow_can_proceed_when_only_post_gate_confidence_differs() -> None:
    baseline = {
        "baseline_verified": False,
        "checks": [
            {"name": name, "aligned": True}
            for name in (
                "complete_scoring_universe",
                "style_thresholds",
                "global_thresholds",
                "style_meta",
                "candidate_decisions_and_reasons",
            )
        ] + [{"name": "post_review_members_scores_reasons", "aligned": False}],
    }

    result = _gate_scope_verification(baseline)

    assert result["gate_inputs_verified"] is True
    assert result["full_report_baseline_verified"] is False
    assert result["failed_checks"] == []


def test_gate_shadow_stops_when_candidate_decisions_do_not_match() -> None:
    baseline = {
        "baseline_verified": False,
        "checks": [{"name": name, "aligned": True} for name in (
            "complete_scoring_universe", "style_thresholds", "global_thresholds", "style_meta",
        )] + [{"name": "candidate_decisions_and_reasons", "aligned": False}],
    }

    result = _gate_scope_verification(baseline)

    assert result["gate_inputs_verified"] is False
    assert result["failed_checks"] == ["candidate_decisions_and_reasons"]

