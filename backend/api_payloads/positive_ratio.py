from __future__ import annotations

import math
from typing import Any

from backend.low_vol_overlap_audit import OVERLAP_COUNT_FIELDS, load_low_vol_overlap_for_report

LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT = 15.0


def _optional_count(value: Any) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) and numeric >= 0 and numeric.is_integer() else None
    except (TypeError, ValueError, OverflowError):
        return None


def normalize_low_vol_overlap_summary(
    overlap: dict[str, Any] | None, *, report_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Old, incomplete or differently scoped experiments do not establish impact."""
    raw = overlap or {}
    identity = report_identity or {}
    binding = raw.get("source_binding") or {}
    source_run = binding.get("run_id") or raw.get("run_id")
    source_revision = binding.get("revision")
    expected_revision = (identity.get("publication_control") or {}).get("revision")
    matched = bool(identity.get("run_id") and expected_revision and source_run == identity["run_id"]
                   and source_revision == expected_revision)
    verified = (matched and raw.get("available") is True and raw.get("passed") is True
                and raw.get("status") == "computed" and raw.get("source_verified_after_replay") is True
                and (raw.get("baseline_verification") or {}).get("baseline_verified") is True
                and all(_optional_count(raw.get(field)) is not None for field in OVERLAP_COUNT_FIELDS))
    status = "computed" if verified else "unverified_frozen_source"
    if source_run and identity.get("run_id") and source_run != identity["run_id"]:
        status = "source_run_mismatch"
    elif source_revision and expected_revision and source_revision != expected_revision:
        status = "source_revision_mismatch"
    elif raw.get("status") in {"scenario_inputs_incomplete", "no_same_revision_audit", "invalid_audit_evidence", "baseline_only"}:
        status = raw["status"]
    result = {**raw, "available": verified, "status": status,
              "run_id": source_run, "source_revision": source_revision,
              "report_run_id": identity.get("run_id"), "report_revision": expected_revision}
    for field in OVERLAP_COUNT_FIELDS:
        result[field] = _optional_count(raw.get(field)) if verified else None
    if not verified:
        result["recommendation"] = []
    return result


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _to_ratio_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator / denominator * 100.0), 6)


def _build_shadow_alignment_status(
    watch: dict[str, Any],
) -> str:
    if not bool(watch.get("available")):
        return "shadow_not_available"

    focus_sample_count = _to_int(watch.get("focus_sample_count"))
    monthly_shadow_ready_count = _to_int(watch.get("monthly_shadow_ready_count"))
    if focus_sample_count <= 0 or monthly_shadow_ready_count <= 0:
        return "shadow_data_insufficient"

    if _to_int(watch.get("positive_ratio_gap_alert_count")) > 0:
        return "gap_alert_observed"
    return "gap_alert_not_observed"


def _build_strategy_materiality_status(
    overlap_summary: dict[str, Any],
) -> tuple[str, int | None, int | None]:
    if not bool(overlap_summary.get("available")):
        return ("unknown", None, None)

    positive_without_annual_direct_final_impact_count = _optional_count(
        overlap_summary.get("positive_without_annual_direct_final_impact_count")
    )
    positive_only_direct_final_impact_count = _optional_count(
        overlap_summary.get("positive_only_direct_final_impact_count")
    )
    if positive_without_annual_direct_final_impact_count is None or positive_only_direct_final_impact_count is None:
        return ("unknown", None, positive_only_direct_final_impact_count)
    # The aggregate already targets positive-only candidates. Separate
    # counterfactual results must not be added as though they were disjoint.
    independent_direct_final_impact_count = positive_without_annual_direct_final_impact_count
    if independent_direct_final_impact_count <= 1 and positive_only_direct_final_impact_count <= 0:
        return ("minimal", independent_direct_final_impact_count, positive_only_direct_final_impact_count)
    if independent_direct_final_impact_count <= 5:
        return ("limited", independent_direct_final_impact_count, positive_only_direct_final_impact_count)
    return ("material", independent_direct_final_impact_count, positive_only_direct_final_impact_count)


def build_positive_ratio_semantics_summary(
    publish_summary: dict[str, Any] | None = None,
    shadow_watch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = publish_summary or {}
    watch = shadow_watch or summary.get("low_vol_positive_ratio_shadow_watch") or {}
    existing = dict(summary.get("positive_ratio_semantics") or watch.get("semantics") or {})
    overlap_summary = normalize_low_vol_overlap_summary(
        watch.get("low_vol_overlap_summary"), report_identity=watch.get("report_source_identity"),
    )
    style_summary = watch.get("style_summary") or []
    top_style = style_summary[0] if style_summary else {}
    top_style_label = _clean_text(top_style.get("债基风格标签"))
    top_style_gap = _to_int(top_style.get("自然月正收益差>=15pct样本数"))
    focus_sample_count = _to_int(watch.get("focus_sample_count"))
    monthly_shadow_ready_count = _to_int(watch.get("monthly_shadow_ready_count"))
    positive_ratio_gate_hit_count = _to_int(watch.get("positive_ratio_gate_hit_count"))
    positive_ratio_gap_alert_count = _to_int(watch.get("positive_ratio_gap_alert_count"))
    shadow_alignment_status = _build_shadow_alignment_status(watch)
    strategy_materiality_status, independent_direct_final_impact_count, positive_only_direct_final_impact_count = (
        _build_strategy_materiality_status(overlap_summary)
    )
    production_gate_change_required = None if strategy_materiality_status == "unknown" else strategy_materiality_status == "material"
    if strategy_materiality_status == "material":
        shadow_interpretation_mode = "offline_review_candidate"
        production_governance_decision = "offline_strategy_review_required"
        recommended_audit_path = "audit_low_vol_annual_positive_ratio_overlap"
    elif strategy_materiality_status == "limited":
        shadow_interpretation_mode = "explanation_with_monitoring"
        production_governance_decision = "keep_daily_gate_monitor_shadow"
        recommended_audit_path = "audit_low_vol_annual_positive_ratio_overlap"
    elif strategy_materiality_status == "minimal":
        shadow_interpretation_mode = "explanation_only"
        production_governance_decision = "keep_daily_gate_explanation_only"
        recommended_audit_path = "offline_shadow_audit_only"
    else:
        shadow_interpretation_mode = "unverified_strategy_impact"
        production_governance_decision = "await_frozen_strategy_audit"
        recommended_audit_path = "audit_low_vol_annual_positive_ratio_overlap"

    headline_parts = [
        "项目内 `positive_ratio` 当前定义为 1 年窗口内日频正收益天数占比。",
        "它属于项目内部稳定性代理指标，不是公用行业标准风险指标名词。",
    ]
    if watch.get("available"):
        headline_parts.append(
            f"低波风格 shadow 观察样本 {focus_sample_count} 只，"
            f"其中自然月 vs 日频正收益率差 >= {int(LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT)}pct 的样本 "
            f"{positive_ratio_gap_alert_count} 只。"
        )
    if strategy_materiality_status == "minimal":
        headline_parts.append("当前专项审计显示该差异主要属于解释层观察，不构成主策略阻塞。")
    elif strategy_materiality_status == "limited":
        headline_parts.append("当前专项审计显示该差异需要持续监控，但暂不足以推动生产门槛切换。")
    elif strategy_materiality_status == "material":
        headline_parts.append("当前专项审计显示该差异已具备策略复审价值，需要离线专项复核。")
    else:
        coverage = overlap_summary.get("scenario_coverage") or {}
        if (overlap_summary.get("status") == "scenario_inputs_incomplete"
                and _optional_count(coverage.get("completed_count")) is not None
                and _optional_count(coverage.get("requested_count")) is not None):
            headline_parts.append(
                f"同修订专项实验完成 {coverage.get('completed_count')}/{coverage.get('requested_count')} 个情景，"
                "其余缺少历史输入，策略影响未知。"
            )
        else:
            headline_parts.append("专项实验尚未通过同修订验证，策略影响未知。")

    explanation_parts = [
        "当前生产门槛仍沿用日频口径，shadow 字段只用于解释增强，不直接替换生产阈值。",
    ]
    if watch.get("available"):
        explanation_parts.append(
            f"低波风格中命中正收益门槛的样本为 {positive_ratio_gate_hit_count} 只；"
            f"自然月 shadow 可复算样本 {monthly_shadow_ready_count} 只。"
        )
    if overlap_summary.get("available"):
        explanation_parts.append(
            f"在低波三桶中，年化与正收益共振样本 {overlap_summary.get('annual_positive_targeted_count')} 只，"
            f"直接影响最终正式主榜 {overlap_summary.get('annual_positive_direct_final_impact_count')} 只；"
            f"仅 `positive_ratio` 独立直达主榜 {independent_direct_final_impact_count} 只。"
        )
    if top_style_label and top_style_gap > 0:
        explanation_parts.append(
            f"当前高差最集中的低波风格是 {top_style_label}，高差样本 {top_style_gap} 只。"
        )
    explanation_parts.append(
        f"当前治理结论为 `{production_governance_decision}`，建议路径 `{recommended_audit_path}`。"
    )

    default_semantics = {
        "available": True,
        "metric_key": "positive_ratio",
        "label": "正收益稳定性",
        "definition": "1年日频正收益天数占比",
        "scope": "项目内部稳定性代理指标",
        "metric_family": "internal_stability_proxy",
        "industry_standard": False,
        "strategy_role": "secondary_stability_proxy",
        "production_gate_basis": "daily_positive_return_ratio",
        "production_window": "1y_daily",
        "shadow_metric_basis": "calendar_month_positive_return_ratio",
        "shadow_window": "1y_calendar_month",
        "shadow_interpretation_mode": shadow_interpretation_mode,
        "production_vs_shadow_alignment_status": shadow_alignment_status,
        "shadow_gap_alert_threshold_pct": float(LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT),
        "shadow_focus_sample_count": focus_sample_count,
        "shadow_ready_sample_count": monthly_shadow_ready_count,
        "shadow_ready_sample_ratio_pct": _to_ratio_pct(monthly_shadow_ready_count, focus_sample_count),
        "shadow_gap_alert_count": positive_ratio_gap_alert_count,
        "shadow_gap_alert_ratio_pct": _to_ratio_pct(positive_ratio_gap_alert_count, focus_sample_count),
        "production_gate_hit_count": positive_ratio_gate_hit_count,
        "production_gate_hit_ratio_pct": _to_ratio_pct(positive_ratio_gate_hit_count, focus_sample_count),
        "strategy_materiality_status": strategy_materiality_status,
        "production_gate_change_required": production_gate_change_required,
        "production_governance_decision": production_governance_decision,
        "governance_judgement": "unverified" if production_gate_change_required is None else "not_strategy_blocker"
        if not production_gate_change_required
        else "offline_review_required",
        "recommended_audit_path": recommended_audit_path,
        "positive_ratio_independent_direct_final_impact_count": independent_direct_final_impact_count,
        "positive_ratio_only_direct_final_impact_count": positive_only_direct_final_impact_count,
        "annual_positive_direct_final_impact_count": _optional_count(
            overlap_summary.get("annual_positive_direct_final_impact_count")
        ),
        "annual_without_positive_direct_final_impact_count": _optional_count(
            overlap_summary.get("annual_without_positive_direct_final_impact_count")
        ),
        "positive_without_annual_direct_final_impact_count": _optional_count(
            overlap_summary.get("positive_without_annual_direct_final_impact_count")
        ),
        "headline": " ".join(headline_parts),
        "explanation": " ".join(explanation_parts),
    }
    merged_semantics = dict(existing)
    merged_semantics.update(default_semantics)
    return merged_semantics


def normalize_low_vol_positive_ratio_shadow_watch(
    publish_summary: dict[str, Any] | None = None,
    *, report_identity: dict[str, Any] | None = None, settings: Any | None = None,
) -> dict[str, Any]:
    summary = publish_summary or {}
    raw_watch = summary.get("low_vol_positive_ratio_shadow_watch") or {}
    if not isinstance(raw_watch, dict):
        return {}

    normalized_watch = dict(raw_watch)
    identity = report_identity or {}
    normalized_watch["report_source_identity"] = {
        "run_id": identity.get("run_id"), "publication_control": identity.get("publication_control") or {},
    }
    overlap = (load_low_vol_overlap_for_report(settings, identity) if settings is not None
               else raw_watch.get("low_vol_overlap_summary"))
    normalized_watch["low_vol_overlap_summary"] = normalize_low_vol_overlap_summary(overlap, report_identity=identity)
    semantics = build_positive_ratio_semantics_summary(summary, normalized_watch)
    normalized_watch["semantics"] = semantics
    normalized_watch["headline"] = str(semantics.get("headline") or "")
    normalized_watch["explanation"] = str(semantics.get("explanation") or "")
    return normalized_watch
