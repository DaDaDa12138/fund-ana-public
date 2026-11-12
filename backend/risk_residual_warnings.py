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


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_risk_residual_warning_summary(
    risk_lag_summary: dict[str, Any],
    risk_sync_summary: dict[str, Any],
    risk_coverage_gap_summary: dict[str, Any],
) -> dict[str, Any]:
    lag = dict(risk_lag_summary or {})
    sync = dict(risk_sync_summary or {})
    gap = dict(risk_coverage_gap_summary or {})

    blocking_codes: list[str] = []
    warning_codes: list[str] = []
    if not bool(lag.get("dates_aligned", False)):
        blocking_codes.append("risk_dates_not_aligned")
    if not bool(lag.get("coverage_sufficient", False)):
        blocking_codes.append("risk_coverage_insufficient")
    if _to_int(sync.get("actionable_candidate_total")) > 0:
        blocking_codes.append("risk_sync_actionable_candidates_present")
    if _to_int(gap.get("actionable_gap_total")) > 0:
        blocking_codes.append("risk_coverage_actionable_gap_present")
    if _to_int(gap.get("current_publish_blocker_total")) > 0:
        blocking_codes.append("risk_publish_blockers_present")
    if _to_int(sync.get("period_eligible_gap_candidate_total")) > 0:
        warning_codes.append("risk_sync_period_eligible_gap_present")
    if _to_int(sync.get("blocked_by_nav_history_total")) > 0:
        warning_codes.append("risk_sync_blocked_by_nav_history_present")
    if _to_int(sync.get("blocked_by_returns_lag_total")) > 0:
        warning_codes.append("risk_sync_blocked_by_returns_lag_present")
    if _to_int(gap.get("formal_pool_blocking_total")) > 0:
        warning_codes.append("formal_pool_risk_gap_present")

    status = "healthy"
    if blocking_codes:
        status = "blocking"
    elif warning_codes:
        status = "warning"

    warning_label_map = {
        "risk_sync_period_eligible_gap_present": "风险同步存在自然观察期缺口样本",
        "risk_sync_blocked_by_nav_history_present": "风险同步受 NAV 历史缺口限制",
        "risk_sync_blocked_by_returns_lag_present": "风险同步受收益滞后限制",
        "formal_pool_risk_gap_present": "正式池存在自然短历史风险缺口",
    }
    blocking_label_map = {
        "risk_dates_not_aligned": "风险快照日期未对齐",
        "risk_coverage_insufficient": "风险覆盖不足",
        "risk_sync_actionable_candidates_present": "存在可立即执行的风险同步样本",
        "risk_coverage_actionable_gap_present": "存在可立即补算的风险缺口",
        "risk_publish_blockers_present": "正式池存在风险发布阻断",
    }

    warning_details = {
        "risk_sync_period_eligible_gap_present": {
            "count": _to_int(sync.get("period_eligible_gap_candidate_total")),
            "detail": "当前属于观察型 gap，不构成正式发布阻断，但需持续跟踪是否长期滞留。",
        },
        "risk_sync_blocked_by_nav_history_present": {
            "count": _to_int(sync.get("blocked_by_nav_history_total")),
            "detail": "风险指标计算受 NAV 历史缺口限制，需结合 NAV 补链进度观察。",
        },
        "risk_sync_blocked_by_returns_lag_present": {
            "count": _to_int(sync.get("blocked_by_returns_lag_total")),
            "detail": "收益序列尚未满足风险重算条件，需等待收益窗口自然推进。",
        },
        "formal_pool_risk_gap_present": {
            "count": _to_int(gap.get("formal_pool_blocking_total")),
            "detail": "正式池内仍有自然短历史样本缺少 1Y 风险指标，但当前不构成发布阻断。",
        },
    }
    blocking_details = {
        "risk_dates_not_aligned": {
            "detail": "风险快照截止日与有效净值日未对齐。",
        },
        "risk_coverage_insufficient": {
            "detail": (
                f"coverage_ratio_vs_candidate={_to_float(lag.get('coverage_ratio_vs_candidate')):.6f}, "
                f"minimum_coverage_ratio={_to_float(lag.get('minimum_coverage_ratio')):.6f}"
            ),
        },
        "risk_sync_actionable_candidates_present": {
            "count": _to_int(sync.get("actionable_candidate_total")),
            "detail": "存在 NAV 已变化且应立即同步风险指标的样本。",
        },
        "risk_coverage_actionable_gap_present": {
            "count": _to_int(gap.get("actionable_gap_total")),
            "detail": "存在可立即补算的 1Y 风险缺口样本。",
        },
        "risk_publish_blockers_present": {
            "count": _to_int(gap.get("current_publish_blocker_total")),
            "detail": "正式发布池存在当前仍阻断发布的风险缺口。",
        },
    }

    if status == "blocking":
        recommended_action = "repair_risk_data_pipeline_before_strategy_changes"
        reason = "风险数据存在阻断项，当前不应直接修改筛选阈值。"
    elif status == "warning":
        recommended_action = "observe_residual_risk_data_warnings"
        reason = "风险数据主链已可运行，但仍有 residual warning，应先观察再决定是否进入策略调参。"
    else:
        recommended_action = "no_additional_risk_data_action_required"
        reason = "当前风险数据主链健康，残余 warning 不构成独立优先级问题。"

    return {
        "available": True,
        "status": status,
        "blocking_codes": blocking_codes,
        "warning_codes": warning_codes,
        "blocking_items": [
            {
                "code": code,
                "label": blocking_label_map.get(code, code),
                **(blocking_details.get(code) or {}),
            }
            for code in blocking_codes
        ],
        "warning_items": [
            {
                "code": code,
                "label": warning_label_map.get(code, code),
                **(warning_details.get(code) or {}),
            }
            for code in warning_codes
        ],
        "risk_lag_summary": {
            "candidate_date": lag.get("candidate_date"),
            "risk_1y_effective_end_date": lag.get("risk_1y_effective_end_date"),
            "dates_aligned": bool(lag.get("dates_aligned")),
            "coverage_sufficient": bool(lag.get("coverage_sufficient")),
            "coverage_ratio_vs_candidate": _to_float(lag.get("coverage_ratio_vs_candidate")),
            "minimum_coverage_ratio": _to_float(lag.get("minimum_coverage_ratio")),
            "effective_risk_uncovered_candidate_total": _to_int(
                lag.get("effective_risk_uncovered_candidate_total")
            ),
        },
        "risk_sync_summary": {
            "actionable_candidate_total": _to_int(sync.get("actionable_candidate_total")),
            "period_eligible_gap_candidate_total": _to_int(
                sync.get("period_eligible_gap_candidate_total")
            ),
            "blocked_by_nav_history_total": _to_int(sync.get("blocked_by_nav_history_total")),
            "blocked_by_nav_history_lag_total": _to_int(
                sync.get("blocked_by_nav_history_lag_total")
            ),
            "blocked_by_returns_lag_total": _to_int(sync.get("blocked_by_returns_lag_total")),
        },
        "risk_coverage_gap_summary": {
            "actionable_gap_total": _to_int(gap.get("actionable_gap_total")),
            "formal_pool_blocking_total": _to_int(gap.get("formal_pool_blocking_total")),
            "current_publish_blocker_total": _to_int(gap.get("current_publish_blocker_total")),
        },
        "judgement": {
            "data_quality_status": status,
            "recommended_action": recommended_action,
            "direct_threshold_relaxation_recommended_now": False,
            "reason": reason,
        },
    }
