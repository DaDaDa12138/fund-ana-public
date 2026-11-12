from __future__ import annotations

from typing import Any


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


def _to_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
        return int(number) if number.is_integer() else None
    except (TypeError, ValueError, OverflowError):
        return None


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return None
    return text or None


def _build_annual_return_gate_shadow_decision_summary(
    shadow_payload: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(shadow_payload or {})
    if not payload:
        return {}

    local_shadow_judgement = payload.get("local_style_band_shadow_judgement") or {}
    scenarios = []
    for item in list(payload.get("local_style_band_shadow_scenarios") or []):
        direct_relieved_final_count = _to_optional_int(item.get("direct_relieved_final_count"))
        candidate_count = _to_optional_int(item.get("candidate_count"))
        shadow_added_count = _to_optional_int(item.get("shadow_added_count"))
        shadow_removed_count = _to_optional_int(item.get("shadow_removed_count"))
        net_final_count_delta = _to_optional_int(item.get("net_final_count_delta"))
        replacement_quality_signal = (
            item.get("replacement_quality_signal")
            if isinstance(item.get("replacement_quality_signal"), dict)
            else {}
        )
        if not replacement_quality_signal:
            replacement_quality_review = (
                item.get("replacement_quality_review")
                if isinstance(item.get("replacement_quality_review"), dict)
                else {}
            )
            replacement_quality_signal = (
                replacement_quality_review.get("quality_signal")
                if isinstance(replacement_quality_review.get("quality_signal"), dict)
                else {}
            )
        forward_validation = (
            item.get("forward_validation")
            if isinstance(item.get("forward_validation"), dict)
            else {}
        )
        scenario = {
            "scenario_key": _clean_text(item.get("scenario_key")),
            "status": item.get("status"),
            "missing_inputs": item.get("missing_inputs"),
            "label": _clean_text(item.get("label")),
            "style_tag": _clean_text(item.get("style_tag")),
            "score_band": _clean_text(item.get("score_band")),
            "annual_gap_gte": item.get("annual_gap_gte"),
            "candidate_count": candidate_count,
            "direct_relieved_final_count": direct_relieved_final_count,
            "shadow_added_count": shadow_added_count,
            "shadow_removed_count": shadow_removed_count,
            "net_final_count_delta": net_final_count_delta,
            "selection_side_effect_only": (None if item.get("status") == "inputs_incomplete"
                                           else bool(item.get("selection_side_effect_only"))),
            "direct_relieved_final_codes": list(item.get("direct_relieved_final_codes") or [])[:5],
            "replacement_quality_direction": _clean_text(
                replacement_quality_signal.get("quality_direction")
            ),
            "replacement_quality_recommended_action": _clean_text(
                replacement_quality_signal.get("recommended_action")
            ),
            "replacement_quality_added_better_metric_count": _to_optional_int(
                replacement_quality_signal.get("added_better_metric_count")
            ),
            "replacement_quality_removed_better_metric_count": _to_optional_int(
                replacement_quality_signal.get("removed_better_metric_count")
            ),
            "replacement_quality_metric_count": _to_optional_int(
                replacement_quality_signal.get("metric_count")
            ),
            "replacement_quality_threshold_change_supported_now": bool(
                replacement_quality_signal.get("threshold_change_supported_now")
            ),
            "forward_validation_status": _clean_text(forward_validation.get("status")),
            "forward_validation_direction": _clean_text(
                forward_validation.get("forward_direction")
            ),
            "forward_validation_recommended_action": _clean_text(
                forward_validation.get("recommended_action")
            ),
            "forward_validation_ready_window_count": _to_optional_int(
                forward_validation.get("ready_window_count")
            ),
            "forward_validation_window_count": _to_optional_int(
                forward_validation.get("window_count")
            ),
            "forward_validation_threshold_change_supported_now": bool(
                forward_validation.get("threshold_change_supported_now")
            ),
        }
        scenarios.append(scenario)

    scenarios.sort(
        key=lambda item: (
            -int(item.get("direct_relieved_final_count") or 0),
            int(item.get("selection_side_effect_only") is True),
            -int(item.get("candidate_count") or 0),
            abs(int(item.get("net_final_count_delta") or 0)),
            str(item.get("scenario_key") or ""),
        )
    )
    top_scenarios = scenarios[:3]
    top_scenario = top_scenarios[0] if top_scenarios else {}
    return {
        "available": payload.get("scenario_status") in (None, "computed"),
        "status": payload.get("status"),
        "scenario_status": payload.get("scenario_status"),
        "baseline_verified": (payload.get("baseline_verification") or {}).get("baseline_verified"),
        "strategy_comparison_allowed": payload.get("strategy_comparison_allowed"),
        "recommended_action": _clean_text(local_shadow_judgement.get("recommended_action")),
        "direct_relief_supported_scenario_total": _to_optional_int(
            local_shadow_judgement.get("direct_relief_supported_scenario_total")
        ),
        "selection_side_effect_only_scenario_total": _to_optional_int(
            local_shadow_judgement.get("selection_side_effect_only_scenario_total")
        ),
        "baseline_final_count": _to_optional_int(payload.get("baseline_final_count")),
        "shadow_final_count": _to_optional_int(payload.get("shadow_final_count")),
        "net_final_count_delta": _to_optional_int(payload.get("net_final_count_delta")),
        "direct_relieved_final_count": _to_optional_int(payload.get("direct_relieved_final_count")),
        "portfolio_direct_relieved_count": _to_optional_int(payload.get("portfolio_direct_relieved_count")),
        "annual_only_candidate_count": _to_optional_int(payload.get("annual_only_candidate_count")),
        "top_local_shadow_scenarios": top_scenarios,
        "top_local_shadow_scenario": top_scenario,
        "local_forward_validation_judgement": (
            payload.get("local_style_band_forward_validation_judgement")
            if isinstance(payload.get("local_style_band_forward_validation_judgement"), dict)
            else {}
        ),
    }


def _classify_publish_focus_metric(reason: dict[str, Any]) -> str | None:
    code = _clean_text(reason.get("code"))
    combined_text = " ".join(
        [
            str(reason.get("reason") or ""),
            str(reason.get("display_reason") or ""),
            str(code or ""),
        ]
    )
    if code and code.startswith("manager_"):
        return "manager_stability"
    if (
        code in {"below_positive_ratio_floor", "positive_ratio_low"}
        or "正收益" in combined_text
    ):
        return "positive_ratio"
    if (
        code in {"below_sharpe_floor", "sharpe_low"}
        or "夏普" in combined_text
        or "Sharpe" in combined_text
    ):
        return "sharpe"
    if (
        code in {"below_annual_floor", "annual_return_low"}
        or "年化" in combined_text
        or "收益" in combined_text
    ):
        return "annual_return"
    if (
        code in {"below_weighted_floor", "below_short_floor", "below_long_floor", "score_low"}
        or "得分" in combined_text
        or "评分" in combined_text
    ):
        return "score"
    return None


def enrich_publish_reason_summary(
    summary: dict[str, Any],
    exclusion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    if not payload.get("available") and (
        payload.get("main_chain") or payload.get("audit_branch")
    ):
        payload["available"] = True
    if not payload.get("available"):
        payload.setdefault("stage_family_summary", [])
        payload.setdefault("focus_metric_summary", [])
        return payload

    stage_lookup: dict[str, dict[str, Any]] = {}
    ordered_stages: list[dict[str, Any]] = []
    for section_key in ("main_chain", "audit_branch"):
        for stage in payload.get(section_key) or []:
            stage_key = _clean_text(stage.get("stage_key"))
            if not stage_key:
                continue
            stage_lookup[stage_key] = stage
            ordered_stages.append(stage)

    stage_family_specs = [
        ("base_gate", "基础门禁", "base_gate"),
        ("dynamic_threshold", "动态阈值", "dynamic_baseline"),
        ("publish_post_review", "发布后复核", "dynamic_post_review"),
        ("final_selection", "最终精选", "final_selection"),
        ("fixed_threshold_audit", "固定阈值审计", "fixed_baseline"),
        ("fixed_post_review_audit", "固定阈值复核", "fixed_post_review"),
    ]
    stage_family_summary: list[dict[str, Any]] = []
    for family_key, family_label, stage_key in stage_family_specs:
        stage = stage_lookup.get(stage_key)
        if not stage:
            continue
        stage_family_summary.append(
            {
                "family_key": family_key,
                "family_label": family_label,
                "stage_key": _clean_text(stage.get("stage_key")),
                "stage_label": _clean_text(stage.get("stage_label")),
                "input_count": _to_int(stage.get("input_count")),
                "passed_count": _to_int(stage.get("passed_count")),
                "removed_count": _to_int(stage.get("removed_count")),
                "removed_rate_pct": _to_float(stage.get("removed_rate_pct")),
                "lead_reason": stage.get("lead_reason") or None,
                "top_reasons": stage.get("top_reasons") or [],
                "explanation": stage.get("explanation"),
            }
        )

    focus_metric_labels = {
        "annual_return": "年化收益主责拦截",
        "score": "综合/长短期评分主责拦截",
        "sharpe": "夏普主责拦截",
        "positive_ratio": "正收益稳定性主责拦截",
        "manager_stability": "经理从业与评分主责拦截",
    }
    focus_metric_summary_map: dict[str, dict[str, Any]] = {}
    raw_focus_metric_summary_map = {
        _clean_text(item.get("metric_key")): dict(item or {})
        for item in list(payload.get("focus_metric_summary") or [])
        if _clean_text(item.get("metric_key"))
    }
    quality_factor_detail_preview = (
        (exclusion_summary or {}).get("quality_factor_detail_preview") or []
    )
    quality_factor_metric_map = {
        "below_annual_floor": "annual_return",
        "below_weighted_floor": "score",
        "below_short_floor": "score",
        "below_long_floor": "score",
        "below_sharpe_floor": "sharpe",
        "below_positive_ratio_floor": "positive_ratio",
    }
    for item in quality_factor_detail_preview:
        reason_code = _clean_text(item.get("原因代码"))
        metric_key = quality_factor_metric_map.get(reason_code or "")
        if metric_key is None:
            continue
        metric_summary = focus_metric_summary_map.setdefault(
            metric_key,
            {
                "metric_key": metric_key,
                "label": focus_metric_labels[metric_key],
                "intercepted_count": 0,
                "stage_keys": [],
                "stage_labels": [],
                "reason_codes": [],
                "top_display_reasons": [],
                "count_basis": "selection_quality_detail_hits",
                "count_basis_note": "按未入榜质量明细命中累计，不代表唯一基金去重数。",
            },
        )
        metric_summary["intercepted_count"] += _to_int(item.get("命中样本数"))
        if reason_code and reason_code not in metric_summary["reason_codes"]:
            metric_summary["reason_codes"].append(reason_code)
        display_reason = _clean_text(item.get("质量细分因子"))
        if display_reason and display_reason not in metric_summary["top_display_reasons"]:
            metric_summary["top_display_reasons"].append(display_reason)

    for stage in ordered_stages:
        stage_key = _clean_text(stage.get("stage_key"))
        stage_label = _clean_text(stage.get("stage_label"))
        for reason in stage.get("top_reasons") or []:
            metric_key = _classify_publish_focus_metric(reason)
            if metric_key is None:
                continue
            metric_summary = focus_metric_summary_map.setdefault(
                metric_key,
                {
                    "metric_key": metric_key,
                    "label": focus_metric_labels[metric_key],
                    "intercepted_count": 0,
                    "stage_keys": [],
                    "stage_labels": [],
                    "reason_codes": [],
                    "top_display_reasons": [],
                    "count_basis": "stage_top_reason_occurrences",
                    "count_basis_note": "按各阶段 top reason 命中累计，不代表唯一基金去重数。",
                },
            )
            if metric_summary["count_basis"] == "stage_top_reason_occurrences":
                metric_summary["intercepted_count"] += _to_int(reason.get("count"))
            if stage_key and stage_key not in metric_summary["stage_keys"]:
                metric_summary["stage_keys"].append(stage_key)
            if stage_label and stage_label not in metric_summary["stage_labels"]:
                metric_summary["stage_labels"].append(stage_label)
            reason_code = _clean_text(reason.get("code"))
            if reason_code and reason_code not in metric_summary["reason_codes"]:
                metric_summary["reason_codes"].append(reason_code)
            display_reason = _clean_text(reason.get("display_reason") or reason.get("reason"))
            if display_reason and display_reason not in metric_summary["top_display_reasons"]:
                metric_summary["top_display_reasons"].append(display_reason)

    score_cluster_explainability = (
        (exclusion_summary or {}).get("score_cluster_explainability")
        or payload.get("score_cluster_explainability")
        or {}
    )
    annual_return_score_sharpe_coupling = (
        (exclusion_summary or {}).get("annual_return_score_sharpe_coupling")
        or payload.get("annual_return_score_sharpe_coupling")
        or {}
    )
    annual_return_gate_shadow = (
        (exclusion_summary or {}).get("annual_return_gate_shadow")
        or payload.get("annual_return_gate_shadow")
        or {}
    )
    high_score_friction_gate_review = (
        (exclusion_summary or {}).get("high_score_friction_gate_review")
        or payload.get("high_score_friction_gate_review")
        or {}
    )
    risk_adjusted_metric_stack_review = (
        (exclusion_summary or {}).get("risk_adjusted_metric_stack_review")
        or payload.get("risk_adjusted_metric_stack_review")
        or {}
    )

    if annual_return_score_sharpe_coupling.get("available"):
        judgement = annual_return_score_sharpe_coupling.get("judgement") or {}
        dominant_local_boundary_review = (
            annual_return_score_sharpe_coupling.get("dominant_local_boundary_review") or {}
        )
        focus_metric_summary_map["annual_return_score_sharpe_coupling"] = {
            "metric_key": "annual_return_score_sharpe_coupling",
            "label": "年化/得分/夏普耦合拆解",
            "intercepted_count": _to_int(annual_return_score_sharpe_coupling.get("focus_scope_total")),
            "stage_keys": ["dynamic_baseline"],
            "stage_labels": ["动态阈值基准"],
            "reason_codes": [
                "below_annual_floor",
                "below_weighted_floor",
                "below_short_floor",
                "below_long_floor",
                "below_sharpe_floor",
            ],
            "top_display_reasons": [
                str(item.get("combo_label") or "")
                for item in list(annual_return_score_sharpe_coupling.get("combo_breakdown") or [])[:4]
                if _to_int(item.get("count")) > 0
            ],
            "count_basis": "selection_quality_detail_hits",
            "count_basis_note": "按仅包含年化收益/得分/夏普三类质量门槛的组合样本拆解，不代表唯一基金去重数。",
            "direct_threshold_relaxation_recommended_now": bool(
                judgement.get("direct_threshold_relaxation_recommended_now")
            ),
            "annual_return_coupled_share_pct": judgement.get("annual_return_coupled_share_pct"),
            "score_requires_annual_return_share_pct": judgement.get(
                "score_requires_annual_return_share_pct"
            ),
            "sharpe_requires_annual_return_share_pct": judgement.get(
                "sharpe_requires_annual_return_share_pct"
            ),
            "annual_return_independent_hit_total": _to_int(
                judgement.get("annual_return_independent_hit_total")
            ),
            "local_boundary_review_recommended_now": bool(
                judgement.get("local_boundary_review_recommended_now")
            ),
            "local_boundary_review_scope": _clean_text(
                judgement.get("local_boundary_review_scope")
            ),
            "high_priority_local_review_style_total": _to_int(
                judgement.get("high_priority_local_review_style_total")
            ),
            "dominant_style_band_label": _clean_text(
                judgement.get("dominant_style_band_label")
            ),
            "dominant_style_band_count": _to_int(
                judgement.get("dominant_style_band_count")
            ),
            "dominant_style_band_share_pct": judgement.get(
                "dominant_style_band_share_pct"
            ),
            "dominant_local_boundary_shadow_review_recommended_now": bool(
                judgement.get("dominant_local_boundary_shadow_review_recommended_now")
            ),
            "dominant_local_boundary_sample_count": _to_int(
                judgement.get("dominant_local_boundary_sample_count")
            ),
            "dominant_local_boundary_weighted_margin_median": judgement.get(
                "dominant_local_boundary_weighted_margin_median"
            ),
            "dominant_local_boundary_annual_gap_median_pct": judgement.get(
                "dominant_local_boundary_annual_gap_median_pct"
            ),
            "dominant_local_boundary_recommended_action": _clean_text(
                dominant_local_boundary_review.get("recommended_action")
            ),
        }

    if annual_return_gate_shadow:
        local_shadow_judgement = annual_return_gate_shadow.get("local_style_band_shadow_judgement") or {}
        decision_summary = _build_annual_return_gate_shadow_decision_summary(
            annual_return_gate_shadow
        )
        focus_metric_summary_map["annual_return_gate_shadow"] = {
            "metric_key": "annual_return_gate_shadow",
            "label": "年化收益影子放宽复核",
            "status": annual_return_gate_shadow.get("status"),
            "scenario_status": annual_return_gate_shadow.get("scenario_status"),
            "intercepted_count": _to_optional_int(annual_return_gate_shadow.get("annual_only_candidate_count")),
            "stage_keys": ["dynamic_baseline", "final_selection"],
            "stage_labels": ["动态阈值基准", "最终精选"],
            "reason_codes": ["below_annual_floor"],
            "top_display_reasons": [
                ("情景未完成，缺少冻结证据" if annual_return_gate_shadow.get("scenario_status") not in (None, "computed")
                 else f"direct_relieved={_to_optional_int(annual_return_gate_shadow.get('direct_relieved_final_count'))}"),
                ("净增未计算" if annual_return_gate_shadow.get("net_final_count_delta") is None
                 else f"net_delta={_to_optional_int(annual_return_gate_shadow.get('net_final_count_delta'))}"),
            ],
            "count_basis": "annual_floor_shadow_relief_candidates",
            "count_basis_note": "按收益门槛影子放宽候选审计汇总，用于说明为何当前不应直接放宽年化收益下限。",
            "shadow_added_count": _to_optional_int(annual_return_gate_shadow.get("shadow_added_count")),
            "shadow_removed_count": _to_optional_int(annual_return_gate_shadow.get("shadow_removed_count")),
            "net_final_count_delta": _to_optional_int(annual_return_gate_shadow.get("net_final_count_delta")),
            "direct_relieved_final_count": _to_optional_int(
                annual_return_gate_shadow.get("direct_relieved_final_count")
            ),
            "portfolio_direct_relieved_count": _to_optional_int(
                annual_return_gate_shadow.get("portfolio_direct_relieved_count")
            ),
            "annual_only_score_ge_80_count": _to_optional_int(
                annual_return_gate_shadow.get("annual_only_score_ge_80_count")
            ),
            "local_style_band_shadow_recommended_action": _clean_text(
                local_shadow_judgement.get("recommended_action")
            ),
            "local_style_band_shadow_direct_relief_supported_scenario_total": _to_optional_int(
                local_shadow_judgement.get("direct_relief_supported_scenario_total")
            ),
            "local_style_band_shadow_selection_side_effect_only_scenario_total": _to_optional_int(
                local_shadow_judgement.get("selection_side_effect_only_scenario_total")
            ),
            "top_local_shadow_scenarios": list(
                decision_summary.get("top_local_shadow_scenarios") or []
            )[:3],
        }

    if score_cluster_explainability.get("available"):
        judgement = score_cluster_explainability.get("judgement") or {}
        focus_metric_summary_map["score_cluster_explainability"] = {
            "metric_key": "score_cluster_explainability",
            "label": "得分簇职责拆分",
            "intercepted_count": _to_int(score_cluster_explainability.get("score_cluster_hit_total")),
            "stage_keys": ["dynamic_baseline"],
            "stage_labels": ["动态阈值基准"],
            "reason_codes": ["below_weighted_floor", "below_short_floor", "below_long_floor"],
            "top_display_reasons": [
                str(item.get("combo_label") or "")
                for item in list(score_cluster_explainability.get("combo_breakdown") or [])[:4]
                if _to_int(item.get("count")) > 0
            ],
            "count_basis": "selection_quality_detail_hits",
            "count_basis_note": "按质量拦截总账中综合/近期/长期得分组合拆解，不代表唯一基金去重数。",
            "suspected_redundancy_present": bool(judgement.get("suspected_redundancy_present")),
            "weighted_anchor_coverage_ratio": judgement.get("weighted_anchor_coverage_ratio"),
            "short_without_weighted_total": _to_int(judgement.get("short_without_weighted_total")),
            "long_without_weighted_total": _to_int(judgement.get("long_without_weighted_total")),
        }

    if high_score_friction_gate_review.get("available"):
        judgement = high_score_friction_gate_review.get("judgement") or {}
        scale_gate = high_score_friction_gate_review.get("scale_gate") or {}
        focus_metric_summary_map["high_score_friction_gate_review"] = {
            "metric_key": "high_score_friction_gate_review",
            "label": "高分摩擦门禁复核",
            "intercepted_count": _to_int(high_score_friction_gate_review.get("high_score_friction_count")),
            "stage_keys": ["publish_post_review", "final_selection"],
            "stage_labels": ["发布后复核", "最终精选"],
            "reason_codes": ["scale_too_small", "fee_too_high"],
            "top_display_reasons": [
                str(item.get("主排除原因") or "")
                for item in list(high_score_friction_gate_review.get("reason_preview") or [])[:4]
                if _to_int(item.get("样本数")) > 0
            ],
            "count_basis": "high_score_friction_cases",
            "count_basis_note": "按高分候选被规模/费率摩擦门槛拦截的样本汇总，不代表应直接放宽阈值。",
            "actionable_threshold_review_total": _to_int(
                judgement.get("actionable_threshold_review_total")
            ),
            "pending_data_verify_total": _to_int(judgement.get("pending_data_verify_total")),
            "dominant_friction_gate": _clean_text(high_score_friction_gate_review.get("dominant_friction_gate")),
            "direct_threshold_relaxation_recommended_now": bool(
                judgement.get("direct_threshold_relaxation_recommended_now")
            ),
            "near_threshold_total": _to_int(scale_gate.get("near_threshold_total")),
        }

    if risk_adjusted_metric_stack_review.get("available"):
        judgement = risk_adjusted_metric_stack_review.get("judgement") or {}
        positive_ratio_governance = (
            risk_adjusted_metric_stack_review.get("positive_ratio_governance") or {}
        )
        dominant_pair = judgement.get("dominant_intra_cluster_pair") or {}
        focus_metric_summary_map["risk_adjusted_metric_stack_review"] = {
            "metric_key": "risk_adjusted_metric_stack_review",
            "label": "风险调整收益栈复核",
            "intercepted_count": _to_int(risk_adjusted_metric_stack_review.get("focus_scope_total")),
            "stage_keys": ["dynamic_baseline"],
            "stage_labels": ["动态阈值基准"],
            "reason_codes": [
                "below_sharpe_floor",
                "below_sortino_floor",
                "below_calmar_floor",
                "below_positive_ratio_floor",
            ],
            "top_display_reasons": [
                f"{item.get('factor_a_label')}+{item.get('factor_b_label')}"
                for item in list(risk_adjusted_metric_stack_review.get("intra_cluster_pair_preview") or [])[:4]
                if _to_int(item.get("pair_count")) > 0
            ],
            "count_basis": "selection_quality_detail_hits",
            "count_basis_note": "按风险调整收益栈重叠拆解汇总，不代表应直接放宽门槛。",
            "direct_threshold_relaxation_recommended_now": bool(
                judgement.get("direct_threshold_relaxation_recommended_now")
            ),
            "high_overlap_pair_total": _to_int(judgement.get("high_overlap_pair_total")),
            "sharpe_review_priority": _clean_text(judgement.get("sharpe_review_priority")),
            "positive_ratio_strategy_materiality_status": _clean_text(
                positive_ratio_governance.get("strategy_materiality_status")
            ),
            "dominant_pair": "+".join(
                [
                    str(item)
                    for item in (
                        dominant_pair.get("factor_a_label"),
                        dominant_pair.get("factor_b_label"),
                    )
                    if _clean_text(item)
                ]
            ),
            "dominant_pair_overlap_ratio": dominant_pair.get("overlap_vs_smaller_ratio"),
        }

    for metric_key in (
        "annual_return_score_sharpe_coupling",
        "annual_return_gate_shadow",
        "score_cluster_explainability",
        "high_score_friction_gate_review",
        "risk_adjusted_metric_stack_review",
    ):
        if focus_metric_summary_map.get(metric_key):
            continue
        raw_metric = raw_focus_metric_summary_map.get(metric_key) or {}
        if not raw_metric:
            continue
        metric_count = _to_optional_int if metric_key == "annual_return_gate_shadow" else _to_int
        focus_metric_summary_map[metric_key] = {
            "metric_key": metric_key,
            "label": raw_metric.get("metric_label") or raw_metric.get("label"),
            "intercepted_count": metric_count(raw_metric.get("intercepted_count")),
            "stage_keys": list(raw_metric.get("stage_keys") or []),
            "stage_labels": list(raw_metric.get("stage_labels") or []),
            "reason_codes": list(raw_metric.get("reason_codes") or []),
            "top_display_reasons": list(raw_metric.get("top_display_reasons") or []),
            "count_basis": raw_metric.get("count_basis"),
            "count_basis_note": raw_metric.get("count_basis_note"),
            "direct_relieved_final_count": metric_count(raw_metric.get("direct_relieved_final_count")),
            "shadow_added_count": metric_count(raw_metric.get("shadow_added_count")),
            "shadow_removed_count": metric_count(raw_metric.get("shadow_removed_count")),
            "net_final_count_delta": metric_count(raw_metric.get("net_final_count_delta")),
            "suspected_redundancy_present": bool(raw_metric.get("suspected_redundancy_present")),
            "weighted_anchor_coverage_ratio": raw_metric.get("weighted_anchor_coverage_ratio"),
            "short_without_weighted_total": metric_count(raw_metric.get("short_without_weighted_total")),
            "long_without_weighted_total": metric_count(raw_metric.get("long_without_weighted_total")),
            "annual_return_coupled_share_pct": raw_metric.get("annual_return_coupled_share_pct"),
            "score_requires_annual_return_share_pct": raw_metric.get(
                "score_requires_annual_return_share_pct"
            ),
            "sharpe_requires_annual_return_share_pct": raw_metric.get(
                "sharpe_requires_annual_return_share_pct"
            ),
            "annual_return_independent_hit_total": metric_count(
                raw_metric.get("annual_return_independent_hit_total")
            ),
            "portfolio_direct_relieved_count": metric_count(
                raw_metric.get("portfolio_direct_relieved_count")
            ),
            "annual_only_score_ge_80_count": metric_count(raw_metric.get("annual_only_score_ge_80_count")),
            "top_local_shadow_scenarios": [
                dict(item or {})
                for item in list(raw_metric.get("top_local_shadow_scenarios") or [])[:3]
            ],
            "actionable_threshold_review_total": metric_count(
                raw_metric.get("actionable_threshold_review_total")
            ),
            "pending_data_verify_total": metric_count(raw_metric.get("pending_data_verify_total")),
            "dominant_friction_gate": raw_metric.get("dominant_friction_gate"),
            "high_overlap_pair_total": metric_count(raw_metric.get("high_overlap_pair_total")),
            "sharpe_review_priority": raw_metric.get("sharpe_review_priority"),
            "positive_ratio_strategy_materiality_status": raw_metric.get(
                "positive_ratio_strategy_materiality_status"
            ),
            "dominant_pair": raw_metric.get("dominant_pair"),
            "dominant_pair_overlap_ratio": raw_metric.get("dominant_pair_overlap_ratio"),
        }

    focus_metric_summary = []
    for metric_key in (
        "annual_return",
        "score",
        "annual_return_score_sharpe_coupling",
        "annual_return_gate_shadow",
        "score_cluster_explainability",
        "high_score_friction_gate_review",
        "risk_adjusted_metric_stack_review",
        "sharpe",
        "positive_ratio",
        "manager_stability",
    ):
        metric_summary = focus_metric_summary_map.get(metric_key)
        if not metric_summary:
            continue
        metric_summary["stage_count"] = len(metric_summary["stage_keys"])
        metric_summary.setdefault("metric_label", metric_summary.get("label"))
        focus_metric_summary.append(metric_summary)

    payload["stage_family_summary"] = stage_family_summary
    payload["focus_metric_summary"] = focus_metric_summary
    payload["responsibility_chain_headline"] = "；".join(
        [
            f"{item['family_label']}淘汰 {item['removed_count']} 只"
            for item in stage_family_summary[:4]
            if _to_int(item.get("removed_count")) > 0
        ]
    )
    if score_cluster_explainability:
        payload["score_cluster_explainability"] = score_cluster_explainability
    if annual_return_score_sharpe_coupling:
        payload["annual_return_score_sharpe_coupling"] = annual_return_score_sharpe_coupling
    if annual_return_gate_shadow:
        annual_return_gate_shadow = dict(annual_return_gate_shadow)
        annual_return_gate_shadow["decision_summary"] = (
            _build_annual_return_gate_shadow_decision_summary(annual_return_gate_shadow)
        )
        payload["annual_return_gate_shadow"] = annual_return_gate_shadow
    if high_score_friction_gate_review:
        payload["high_score_friction_gate_review"] = high_score_friction_gate_review
    if risk_adjusted_metric_stack_review:
        payload["risk_adjusted_metric_stack_review"] = risk_adjusted_metric_stack_review
    return payload
