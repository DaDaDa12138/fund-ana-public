"""Synthetic cache fixtures test evidence handling, not fund performance."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from backend.api_payloads.positive_ratio import OVERLAP_COUNT_FIELDS
from backend.config import load_settings
from backend.recommendations_sidecar import build_recommendations_detail_response


def report_fixture(evidence):
    overlap = {key: 0 for key in OVERLAP_COUNT_FIELDS}
    overlap.update(available=True, passed=True, status="computed", source_verified_after_replay=True,
                   baseline_verification={"baseline_verified": True}, source_binding={"run_id": "fixture", "revision": "a"},
                   positive_without_annual_direct_final_impact_count=4, positive_only_direct_final_impact_count=1)
    if evidence == "other_revision":
        overlap["source_binding"]["revision"] = "b"
    elif evidence == "legacy":
        overlap.pop("baseline_verification")
    elif evidence == "partial":
        overlap.update(passed=False, status="scenario_inputs_incomplete")
    report = {"run_id": "fixture", "publication_control": {"revision": "a"}, "counts": {},
              "project_publish_summary": {"low_vol_positive_ratio_shadow_watch": {
                  "available": True, "focus_sample_count": 5, "monthly_shadow_ready_count": 5,
                  "low_vol_overlap_summary": overlap}}}
    if evidence == "absent":
        report["project_publish_summary"] = {}
    return report


def assert_evidence(response, evidence):
    semantics = response["positive_ratio_semantics"]
    watch = response["low_vol_positive_ratio_shadow_watch"]
    overlap = watch["low_vol_overlap_summary"]
    if evidence == "verified":
        assert overlap["available"] is True
        assert semantics["positive_ratio_independent_direct_final_impact_count"] == 4
        assert semantics["positive_ratio_only_direct_final_impact_count"] == 1
    else:
        assert overlap["available"] is False
        assert semantics["strategy_materiality_status"] == "unknown"
        assert semantics["production_gate_change_required"] is None
        assert semantics["positive_ratio_independent_direct_final_impact_count"] is None
    assert "stale cached claim" not in watch["headline"] + watch["explanation"]


@pytest.mark.parametrize("evidence", ["verified", "other_revision", "legacy", "partial", "absent"])
@pytest.mark.parametrize("cache_hit", [True, False])
def test_recommendation_detail_revalidates_cached_and_built_audit_evidence(evidence, cache_hit):
    import backend.app as app

    report = report_fixture(evidence)
    cached = {"run_id": "fixture", "funds": [], "rows": [], "portfolio_suggestions": {},
              "low_vol_positive_ratio_shadow_watch": {"available": True, "headline": "stale cached claim",
                  "explanation": "stale cached claim", "low_vol_overlap_summary": {"available": True}},
              "positive_ratio_semantics": {"strategy_materiality_status": "minimal",
                                            "positive_ratio_independent_direct_final_impact_count": 5}}
    original_report, original_cached = deepcopy(report), deepcopy(cached)
    with (
        patch("backend.api_payloads.positive_ratio.load_low_vol_overlap_for_report",
              return_value=(report.get("project_publish_summary") or {}).get("low_vol_positive_ratio_shadow_watch", {}).get("low_vol_overlap_summary", {})),
        patch.object(app, "_recommendations_detail_sidecar_enabled", return_value=True),
        patch.object(app, "_build_recommendations_detail_cache_basis", return_value={"schema_version": 2}),
        patch.object(app, "_probe_recommendations_detail_sidecar_status", return_value={"hit": cache_hit, "status": "hit" if cache_hit else "missing"}),
        patch.object(app, "_load_recommendations_detail_sidecar", return_value=cached if cache_hit else None),
        patch.object(app, "build_run_local_recommendations_detail_response", return_value=deepcopy(cached)) as builder,
        patch.object(app, "_write_recommendations_detail_sidecar", side_effect=AssertionError("read-only request wrote a sidecar")),
        patch.object(app, "_attach_recommendations_publication_status", side_effect=lambda source, response: response),
        patch.object(app, "_attach_recommendations_detail_publication_source_contract", side_effect=lambda response: response),
    ):
        response = app._build_recommendations_detail_response(report, audit_read_only=True)
    assert_evidence(response, evidence)
    assert report == original_report and cached == original_cached
    assert builder.call_count == (0 if cache_hit else 1)


@pytest.mark.parametrize("evidence", ["verified", "other_revision", "legacy", "partial", "absent"])
def test_new_recommendation_sidecar_binds_its_report_revision(evidence):
    report = report_fixture(evidence)
    original = deepcopy(report)
    with patch("backend.api_payloads.positive_ratio.load_low_vol_overlap_for_report",
               return_value=(report.get("project_publish_summary") or {}).get("low_vol_positive_ratio_shadow_watch", {}).get("low_vol_overlap_summary", {})):
        result = build_recommendations_detail_response(load_settings(), report)
    assert_evidence(result, evidence)
    assert report == original
