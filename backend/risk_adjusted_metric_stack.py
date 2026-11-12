from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


_RISK_FACTORS = ("sharpe", "sortino", "calmar", "positive_ratio")
_RISK_FACTOR_SET = set(_RISK_FACTORS)
_FACTOR_MARKERS: dict[str, tuple[str, ...]] = {
    "annual_return": ("below_annual_floor", "1年年化收益"),
    "weighted_score": ("below_weighted_floor", "综合加权得分"),
    "short_term_score": ("below_short_floor", "近期得分"),
    "long_term_score": ("below_long_floor", "长期得分"),
    "sharpe": ("below_sharpe_floor", "1年夏普比率"),
    "sortino": ("below_sortino_floor", "1年索提诺比率"),
    "calmar": ("below_calmar_floor", "1年卡玛比率"),
    "positive_ratio": ("below_positive_ratio_floor", "1年正收益比率"),
    "max_drawdown": ("mdd_too_high", "1年最大回撤"),
    "volatility": ("volatility_too_high", "1年波动率"),
    "scale": ("scale_too_small", "基金规模"),
    "fee": ("fee_too_high", "费率合计"),
}
_FACTOR_LABELS = {
    "annual_return": "1年年化收益",
    "weighted_score": "综合加权得分",
    "short_term_score": "近期得分",
    "long_term_score": "长期得分",
    "sharpe": "1年夏普比率",
    "sortino": "1年索提诺比率",
    "calmar": "1年卡玛比率",
    "positive_ratio": "1年正收益比率",
    "max_drawdown": "1年最大回撤",
    "volatility": "1年波动率",
    "scale": "基金规模",
    "fee": "费率合计",
}
_PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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


def _rows(df: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.head(limit).fillna("").to_dict(orient="records")


def _normalize_factor_token(text: Any) -> str:
    normalized = _clean_text(text)
    for factor_key, markers in _FACTOR_MARKERS.items():
        if any(marker in normalized for marker in markers):
            return factor_key
    return "other"


def _factor_label(factor_key: str) -> str:
    return _FACTOR_LABELS.get(factor_key, factor_key)


def _build_factor_counts(detail_df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if detail_df.empty:
        return counts
    for _, row in detail_df.iterrows():
        factor_key = _normalize_factor_token(
            row.get("原因代码") or row.get("质量细分因子")
        )
        if factor_key not in _RISK_FACTOR_SET:
            continue
        counts[factor_key] = max(counts.get(factor_key, 0), _to_int(row.get("命中样本数")))
    return counts


def _build_pair_rows(pair_df: pd.DataFrame, factor_counts: dict[str, int]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if pair_df.empty:
        return rows
    for _, row in pair_df.iterrows():
        factor_a = _normalize_factor_token(row.get("因子A"))
        factor_b = _normalize_factor_token(row.get("因子B"))
        if "other" in {factor_a, factor_b}:
            continue
        if factor_a not in _RISK_FACTOR_SET and factor_b not in _RISK_FACTOR_SET:
            continue
        pair_count = _to_int(row.get("共同命中样本数"))
        count_a = factor_counts.get(factor_a, 0)
        count_b = factor_counts.get(factor_b, 0)
        if pair_count <= 0:
            continue
        denominator_candidates = [count for count in (count_a, count_b) if count > 0]
        overlap_vs_smaller_ratio = (
            round(float(pair_count) / float(min(denominator_candidates)), 4)
            if denominator_candidates
            else 0.0
        )
        rows.append(
            {
                "factor_a": factor_a,
                "factor_b": factor_b,
                "factor_a_label": _factor_label(factor_a),
                "factor_b_label": _factor_label(factor_b),
                "pair_count": pair_count,
                "overlap_vs_smaller_ratio": overlap_vs_smaller_ratio,
                "quality_share_pct": round(_to_float(row.get("占质量拦截样本比例(%)")), 2),
                "intra_cluster": factor_a in _RISK_FACTOR_SET and factor_b in _RISK_FACTOR_SET,
            }
        )
    rows.sort(
        key=lambda item: (
            not bool(item["intra_cluster"]),
            -float(item["overlap_vs_smaller_ratio"]),
            -int(item["pair_count"]),
            str(item["factor_a"]),
            str(item["factor_b"]),
        )
    )
    return rows


def _parse_combo_factor_keys(combo_text: Any) -> tuple[str, ...]:
    factor_keys: list[str] = []
    seen: set[str] = set()
    for token in str(combo_text or "").split("|"):
        factor_key = _normalize_factor_token(token)
        if factor_key == "other" or factor_key in seen:
            continue
        seen.add(factor_key)
        factor_keys.append(factor_key)
    return tuple(factor_keys)


def _build_combo_rows(combo_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if combo_df.empty:
        return rows
    for _, row in combo_df.iterrows():
        factor_keys = _parse_combo_factor_keys(row.get("重叠组合"))
        if len(_RISK_FACTOR_SET.intersection(factor_keys)) < 2:
            continue
        rows.append(
            {
                "factor_keys": list(factor_keys),
                "factor_labels": [_factor_label(item) for item in factor_keys],
                "factor_count": _to_int(row.get("因子数")) or len(factor_keys),
                "sample_count": _to_int(row.get("样本数")),
                "quality_share_pct": round(_to_float(row.get("占质量拦截样本比例(%)")), 2),
                "risk_factor_count": len(_RISK_FACTOR_SET.intersection(factor_keys)),
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item["risk_factor_count"]),
            -int(item["sample_count"]),
            -int(item["factor_count"]),
        )
    )
    return rows


def _build_factor_summary(
    factor_counts: dict[str, int],
    pair_rows: list[dict[str, Any]],
    positive_ratio_semantics: dict[str, Any],
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for factor_key in _RISK_FACTORS:
        related_pairs = [
            row
            for row in pair_rows
            if factor_key in {row["factor_a"], row["factor_b"]}
        ]
        related_pairs_by_strength = sorted(
            related_pairs,
            key=lambda item: (
                -float(item.get("overlap_vs_smaller_ratio") or 0.0),
                -int(item.get("pair_count") or 0),
                not bool(item.get("intra_cluster")),
            ),
        )
        intra_cluster_pairs = [
            row for row in related_pairs_by_strength if bool(row.get("intra_cluster"))
        ]
        related_pairs.sort(
            key=lambda item: (
                not bool(item["intra_cluster"]),
                -float(item["overlap_vs_smaller_ratio"]),
                -int(item["pair_count"]),
            )
        )
        top_partner = related_pairs_by_strength[0] if related_pairs_by_strength else None
        top_intra_cluster_partner = intra_cluster_pairs[0] if intra_cluster_pairs else None
        hit_count = factor_counts.get(factor_key, 0)
        review_priority = "P3"
        if factor_key == "sharpe":
            if top_partner and float(top_partner.get("overlap_vs_smaller_ratio") or 0.0) >= 0.9:
                review_priority = "P1"
            elif hit_count >= 2000:
                review_priority = "P2"
        elif factor_key in {"sortino", "calmar"}:
            if (
                top_intra_cluster_partner
                and float(top_intra_cluster_partner.get("overlap_vs_smaller_ratio") or 0.0)
                >= 0.9
            ):
                review_priority = "P2"
        elif factor_key == "positive_ratio":
            materiality = _clean_text(
                positive_ratio_semantics.get("strategy_materiality_status")
            )
            if materiality == "material":
                review_priority = "P1"
            elif materiality == "limited":
                review_priority = "P2"
            elif materiality == "minimal":
                review_priority = "P3"
            else:
                review_priority = "unverified"

        summary.append(
            {
                "factor_key": factor_key,
                "factor_label": _factor_label(factor_key),
                "hit_count": hit_count,
                "top_partner_factor_key": (
                    top_partner["factor_b"]
                    if top_partner and top_partner["factor_a"] == factor_key
                    else (top_partner["factor_a"] if top_partner else None)
                ),
                "top_partner_factor_label": (
                    top_partner["factor_b_label"]
                    if top_partner and top_partner["factor_a"] == factor_key
                    else (top_partner["factor_a_label"] if top_partner else None)
                ),
                "top_partner_overlap_ratio": (
                    float(top_partner.get("overlap_vs_smaller_ratio") or 0.0)
                    if top_partner
                    else 0.0
                ),
                "top_partner_pair_count": (
                    _to_int(top_partner.get("pair_count")) if top_partner else 0
                ),
                "top_partner_is_intra_cluster": bool(
                    top_partner.get("intra_cluster") if top_partner else False
                ),
                "review_priority": review_priority,
            }
        )
    summary.sort(
        key=lambda item: (
            _PRIORITY_RANK.get(str(item.get("review_priority")), 99),
            -int(item.get("hit_count") or 0),
            str(item.get("factor_key")),
        )
    )
    return summary


def build_risk_adjusted_metric_stack_review(
    *,
    quality_factor_detail_df: pd.DataFrame,
    quality_overlap_pair_df: pd.DataFrame,
    quality_overlap_combo_df: pd.DataFrame,
    positive_ratio_semantics: dict[str, Any] | None = None,
    low_vol_positive_ratio_shadow_watch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    positive_ratio_semantics = dict(positive_ratio_semantics or {})
    low_vol_watch = dict(low_vol_positive_ratio_shadow_watch or {})
    factor_counts = _build_factor_counts(quality_factor_detail_df)
    pair_rows = _build_pair_rows(quality_overlap_pair_df, factor_counts)
    combo_rows = _build_combo_rows(quality_overlap_combo_df)
    factor_summary = _build_factor_summary(
        factor_counts,
        pair_rows,
        positive_ratio_semantics,
    )

    intra_cluster_pairs = [row for row in pair_rows if bool(row.get("intra_cluster"))]
    companion_pairs = [row for row in pair_rows if not bool(row.get("intra_cluster"))]
    dominant_intra_pair = intra_cluster_pairs[0] if intra_cluster_pairs else {}
    high_overlap_pair_total = sum(
        1 for row in intra_cluster_pairs if float(row.get("overlap_vs_smaller_ratio") or 0.0) >= 0.9
    )
    sharpe_row = next((row for row in factor_summary if row.get("factor_key") == "sharpe"), {})

    positive_ratio_watch_top_style = {}
    style_summary = list(low_vol_watch.get("style_summary") or [])
    if style_summary:
        top_style = style_summary[0]
        positive_ratio_watch_top_style = {
            "style_tag": _clean_text(top_style.get("债基风格标签")) or None,
            "gap_alert_count": _to_int(top_style.get("自然月正收益差>=15pct样本数")),
            "sample_count": _to_int(top_style.get("样本数")),
        }

    positive_ratio_governance = {
        "strategy_materiality_status": _clean_text(
            positive_ratio_semantics.get("strategy_materiality_status")
        )
        or None,
        "production_gate_change_required": positive_ratio_semantics.get("production_gate_change_required"),
        "production_governance_decision": _clean_text(
            positive_ratio_semantics.get("production_governance_decision")
        )
        or None,
        "recommended_audit_path": _clean_text(
            positive_ratio_semantics.get("recommended_audit_path")
        )
        or None,
        "positive_ratio_independent_direct_final_impact_count": positive_ratio_semantics.get("positive_ratio_independent_direct_final_impact_count"),
        "positive_ratio_only_direct_final_impact_count": positive_ratio_semantics.get("positive_ratio_only_direct_final_impact_count"),
        "top_shadow_gap_style": positive_ratio_watch_top_style,
    }

    if positive_ratio_governance["production_gate_change_required"]:
        status = "targeted_review_required"
        recommended_action = "review_risk_adjusted_metric_stack"
        reason = "positive_ratio shadow 审计已显示生产门槛具备复审价值，应与风险调整收益簇一起复核。"
    elif high_overlap_pair_total > 0 or str(sharpe_row.get("review_priority")) == "P1":
        status = "secondary_review_required"
        recommended_action = "review_risk_adjusted_metric_stack"
        reason = "风险调整收益簇存在高重叠或夏普强耦合信号，应作为第二优先级做职责收敛复核。"
    else:
        status = "observation_only"
        recommended_action = "keep_risk_adjusted_metric_stack_under_observation"
        reason = "风险调整收益簇目前未显示需要立即收敛的强信号，先保持观察。"

    return {
        "available": True,
        "status": status,
        "focus_scope_total": max(factor_counts.values()) if factor_counts else 0,
        "factor_summary": factor_summary,
        "intra_cluster_pair_preview": intra_cluster_pairs[:8],
        "companion_pair_preview": companion_pairs[:8],
        "combo_preview": combo_rows[:8],
        "positive_ratio_governance": positive_ratio_governance,
        "judgement": {
            "status": status,
            "recommended_action": recommended_action,
            "high_overlap_pair_total": high_overlap_pair_total,
            "dominant_intra_cluster_pair": {
                "factor_a": dominant_intra_pair.get("factor_a"),
                "factor_b": dominant_intra_pair.get("factor_b"),
                "factor_a_label": dominant_intra_pair.get("factor_a_label"),
                "factor_b_label": dominant_intra_pair.get("factor_b_label"),
                "pair_count": _to_int(dominant_intra_pair.get("pair_count")),
                "overlap_vs_smaller_ratio": _to_float(
                    dominant_intra_pair.get("overlap_vs_smaller_ratio")
                ),
            },
            "sharpe_review_priority": _clean_text(sharpe_row.get("review_priority")) or "P3",
            "positive_ratio_strategy_materiality_status": (
                positive_ratio_governance.get("strategy_materiality_status") or "unknown"
            ),
            "direct_threshold_relaxation_recommended_now": False,
            "reason": reason,
        },
    }


def normalize_positive_ratio_review_evidence(
    review: dict[str, Any], semantics: dict[str, Any],
) -> dict[str, Any]:
    """Preserve historical factor observations while withdrawing unverified impact."""
    if not review:
        return {}
    current = dict(review)
    governance = dict(current.get("positive_ratio_governance") or {})
    for key in ("strategy_materiality_status", "production_gate_change_required", "production_governance_decision",
                "recommended_audit_path", "positive_ratio_independent_direct_final_impact_count", "positive_ratio_only_direct_final_impact_count"):
        governance[key] = semantics.get(key)
    current["positive_ratio_governance"] = governance
    judgement = dict(current.get("judgement") or {})
    judgement["positive_ratio_strategy_materiality_status"] = semantics.get("strategy_materiality_status") or "unknown"
    if judgement["positive_ratio_strategy_materiality_status"] == "unknown":
        current["factor_summary"] = [
            {**row, "review_priority": "unverified"} if row.get("factor_key") == "positive_ratio" else dict(row)
            for row in current.get("factor_summary") or []
        ]
        if current.get("status") == "targeted_review_required":
            current["status"] = "inputs_unverified"
            judgement.update(status="inputs_unverified", recommended_action="await_frozen_strategy_audit",
                             reason="正收益策略实验缺少同修订验证，影响保持未知。")
    current["judgement"] = judgement
    return current


def load_risk_adjusted_metric_stack_review_from_workbook(
    workbook_path: Path,
    *,
    positive_ratio_semantics: dict[str, Any] | None = None,
    low_vol_positive_ratio_shadow_watch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = Path(workbook_path)
    if not path.exists():
        raise FileNotFoundError(path)

    def _read_sheet(name: str) -> pd.DataFrame:
        try:
            return pd.read_excel(path, sheet_name=name)
        except ValueError:
            return pd.DataFrame()

    return build_risk_adjusted_metric_stack_review(
        quality_factor_detail_df=_read_sheet("质量细分因子"),
        quality_overlap_pair_df=_read_sheet("质量重叠Pair"),
        quality_overlap_combo_df=_read_sheet("质量重叠Combo"),
        positive_ratio_semantics=positive_ratio_semantics,
        low_vol_positive_ratio_shadow_watch=low_vol_positive_ratio_shadow_watch,
    )
