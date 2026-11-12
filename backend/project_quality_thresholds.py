from __future__ import annotations

from typing import Any

import pandas as pd

from backend.project_publish_decisions import (
    ABSOLUTE_QUALITY_DIAGNOSIS,
    AUTO_DIAGNOSIS_VERDICT,
    DYNAMIC_QUALITY_DIAGNOSIS,
    ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS,
    PROJECT_RECOMMENDATION_REASONS,
)
from backend.fee_semantics import resolve_fee_screening_value
from backend.scale_semantics import resolve_scale_for_thresholds


PROJECT_ABSOLUTE_QUALITY_THRESHOLDS = {
    "weighted_score_min": 58.0,
    "short_score_min": 56.0,
    "long_score_min": 58.0,
    "fee_total_max": 0.60,
    "max_drawdown_max": 0.020,
    "scale_billion_min": 2.0,
    "annual_return_min": 2.0,
    "sharpe_min": 0.8,
    "sortino_min": 0.8,
    "calmar_min": 2.8,
    "positive_ratio_min": 55.0,
    "volatility_max": 0.060,
}

SCALE_NEAR_THRESHOLD_RELIEF = {
    "min_scale_billion": 1.80,
    "min_weighted_score": 80.0,
}

ANNUAL_RETURN_NEAR_THRESHOLD_RELIEF = {
    "max_threshold": 2.0,
    "max_shortfall": 0.15,
    "min_weighted_score": 80.0,
}

POSITIVE_RATIO_NEAR_THRESHOLD_RELIEF = {
    "eligible_styles": {"一级债增强", "二级债增强", "转债增强"},
    "max_shortfall": 0.05,
    "min_weighted_score": 72.0,
}

ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS = {
    "一级债增强",
    "二级债增强",
    "转债增强",
}

ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS = {
    "一级债增强": {"calmar_min"},
    "二级债增强": {"calmar_min"},
}

DYNAMIC_THRESHOLD_CONFIG = {
    "min_style_sample_size": 15,
    "robust_clip_enabled": True,
    "robust_clip_lower_quantile": 0.05,
    "robust_clip_upper_quantile": 0.95,
    "weighted_quantile": 0.70,
    "score_quantile": 0.60,
    "annual_quantile": 0.55,
    "sharpe_quantile": 0.50,
    "sortino_quantile": 0.50,
    "calmar_quantile": 0.50,
    "positive_quantile": 0.45,
    "fee_quantile": 0.85,
    "mdd_quantile": 0.80,
    "volatility_quantile": 0.85,
    "weighted_floor_min": 55.0,
    "score_floor_min": 54.0,
    "fee_cap_max": 0.65,
    "mdd_cap_max": 0.045,
    "volatility_cap_max": 0.080,
}

ROBUST_CLIP_COLUMNS = [
    "综合加权得分",
    "近期得分",
    "长期得分",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-索提诺比率",
    "1年-卡玛比率",
    "1年-正收益比率",
    "费率合计(%)",
    "1年-最大回撤(%)",
    "1年-波动率",
]

PROJECT_SCORE_STYLE_FLOORS = {
    "DEFAULT": {
        "annual_return_min": 2.0,
        "sharpe_min": 0.8,
        "sortino_min": 0.8,
        "calmar_min": 2.5,
        "max_drawdown_max": 0.020,
        "volatility_max": 0.050,
        "positive_ratio_min": 56.0,
        "fee_total_max": 0.65,
    },
    "利率债": {
        "annual_return_min": 2.0,
        "sharpe_min": 0.5,
        "sortino_min": 0.5,
        "calmar_min": 2.5,
        "max_drawdown_max": 0.020,
        "volatility_max": 0.050,
        "positive_ratio_min": 55.0,
        "fee_total_max": 0.65,
    },
    "信用债": {
        "annual_return_min": 2.0,
        "sharpe_min": 0.8,
        "sortino_min": 0.8,
        "calmar_min": 2.5,
        "max_drawdown_max": 0.020,
        "volatility_max": 0.050,
        "positive_ratio_min": 56.0,
        "fee_total_max": 0.65,
    },
    "综合纯债": {
        "annual_return_min": 2.0,
        "sharpe_min": 0.8,
        "sortino_min": 0.8,
        "calmar_min": 2.8,
        "max_drawdown_max": 0.018,
        "volatility_max": 0.045,
        "positive_ratio_min": 56.0,
        "fee_total_max": 0.60,
    },
    "中短债": {
        "annual_return_min": 2.0,
        "sharpe_min": 1.0,
        "sortino_min": 1.0,
        "calmar_min": 4.0,
        "max_drawdown_max": 0.008,
        "volatility_max": 0.025,
        "positive_ratio_min": 58.0,
        "fee_total_max": 0.60,
    },
    "现金增强": {
        "annual_return_min": 2.0,
        "sharpe_min": 1.0,
        "sortino_min": 1.0,
        "calmar_min": 4.0,
        "max_drawdown_max": 0.015,
        "volatility_max": 0.040,
        "positive_ratio_min": 57.0,
        "fee_total_max": 0.60,
    },
    "固收指数": {
        "annual_return_min": 2.0,
        "sharpe_min": 0.8,
        "sortino_min": 0.8,
        "calmar_min": 2.5,
        "max_drawdown_max": 0.020,
        "volatility_max": 0.050,
        "positive_ratio_min": 56.0,
        "fee_total_max": 0.50,
    },
    "一级债增强": {
        "annual_return_min": 4.0,
        "sharpe_min": 1.0,
        "sortino_min": 1.0,
        "calmar_min": 2.4,
        "max_drawdown_max": 0.080,
        "volatility_max": 0.120,
        "positive_ratio_min": 55.0,
        "fee_total_max": 1.10,
    },
    "二级债增强": {
        "annual_return_min": 5.0,
        "sharpe_min": 1.0,
        "sortino_min": 1.0,
        "calmar_min": 2.4,
        "max_drawdown_max": 0.080,
        "volatility_max": 0.120,
        "positive_ratio_min": 55.0,
        "fee_total_max": 1.25,
    },
    "转债增强": {
        "annual_return_min": 3.5,
        "sharpe_min": 1.0,
        "sortino_min": 1.0,
        "calmar_min": 3.0,
        "max_drawdown_max": 0.030,
        "volatility_max": 0.070,
        "positive_ratio_min": 54.0,
        "fee_total_max": 0.70,
    },
}


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _is_near_scale_relief_candidate(row: dict[str, Any]) -> bool:
    scale_billion = resolve_scale_for_thresholds(row)
    weighted_score = _safe_float(row.get("综合加权得分"))

    if scale_billion is None or scale_billion < float(SCALE_NEAR_THRESHOLD_RELIEF["min_scale_billion"]):
        return False
    if weighted_score is None or weighted_score < float(SCALE_NEAR_THRESHOLD_RELIEF["min_weighted_score"]):
        return False
    return True


def _apply_scale_near_relief_reason_codes(
    row: dict[str, Any],
    reason_codes: list[str],
) -> list[str]:
    if "scale_too_small" not in reason_codes:
        return reason_codes
    if len(reason_codes) != 1:
        return reason_codes
    if not _is_near_scale_relief_candidate(row):
        return reason_codes
    return [code for code in reason_codes if code != "scale_too_small"]


def _is_near_annual_return_relief_candidate(
    row: dict[str, Any],
    *,
    annual_threshold: float | None,
) -> bool:
    annual_return = _safe_float(row.get("1年-年化收益(%)"))
    weighted_score = _safe_float(row.get("综合加权得分"))
    share_count = _safe_float(row.get("关联份额数") or row.get("share_class_count"))
    threshold = _safe_float(annual_threshold)

    if threshold is None or threshold > float(ANNUAL_RETURN_NEAR_THRESHOLD_RELIEF["max_threshold"]):
        return False
    if annual_return is None or annual_return < threshold - float(ANNUAL_RETURN_NEAR_THRESHOLD_RELIEF["max_shortfall"]):
        return False
    if weighted_score is None or weighted_score < float(ANNUAL_RETURN_NEAR_THRESHOLD_RELIEF["min_weighted_score"]):
        return False
    if share_count is not None and share_count > 1:
        return False
    return True


def _apply_annual_return_near_relief_reason_codes(
    row: dict[str, Any],
    reason_codes: list[str],
    *,
    annual_threshold: float | None,
) -> list[str]:
    if "below_annual_floor" not in reason_codes:
        return reason_codes
    if len(reason_codes) != 1:
        return reason_codes
    if not _is_near_annual_return_relief_candidate(row, annual_threshold=annual_threshold):
        return reason_codes
    return [code for code in reason_codes if code != "below_annual_floor"]


def _is_primary_share_candidate(row: dict[str, Any]) -> bool:
    share_count = _safe_float(row.get("关联份额数") or row.get("share_class_count"))
    if share_count is not None and share_count > 1:
        return False

    fund_name = str(row.get("基金名称") or "").strip()
    if not fund_name:
        return True
    suffix = fund_name[-1]
    if suffix in {"A", "B", "C", "D", "E", "F", "H", "I", "R", "Y"}:
        return suffix == "A"
    return True


def _is_near_positive_ratio_relief_candidate(
    row: dict[str, Any],
    *,
    positive_ratio_threshold: float | None,
) -> bool:
    positive_ratio = _safe_float(row.get("1年-正收益比率"))
    weighted_score = _safe_float(row.get("综合加权得分"))
    threshold = _safe_float(positive_ratio_threshold)
    style = str(row.get("债基风格标签") or "").strip()

    if style not in set(POSITIVE_RATIO_NEAR_THRESHOLD_RELIEF["eligible_styles"]):
        return False
    if threshold is None or positive_ratio is None:
        return False
    if positive_ratio < threshold - float(POSITIVE_RATIO_NEAR_THRESHOLD_RELIEF["max_shortfall"]):
        return False
    if weighted_score is None or weighted_score < float(POSITIVE_RATIO_NEAR_THRESHOLD_RELIEF["min_weighted_score"]):
        return False
    if not _is_primary_share_candidate(row):
        return False
    return True


def _apply_positive_ratio_near_relief_reason_codes(
    row: dict[str, Any],
    reason_codes: list[str],
    *,
    positive_ratio_threshold: float | None,
) -> list[str]:
    if "below_positive_ratio_floor" not in reason_codes:
        return reason_codes
    if len(reason_codes) != 1:
        return reason_codes
    if not _is_near_positive_ratio_relief_candidate(
        row,
        positive_ratio_threshold=positive_ratio_threshold,
    ):
        return reason_codes
    return [code for code in reason_codes if code != "below_positive_ratio_floor"]


def project_style_floor(style: str) -> dict[str, float]:
    style_key = str(style or "").strip() or "DEFAULT"
    base = dict(PROJECT_SCORE_STYLE_FLOORS["DEFAULT"])
    base.update(PROJECT_SCORE_STYLE_FLOORS.get(style_key, {}))
    return base


def combined_quality_floor(style: str) -> dict[str, float]:
    base = project_style_floor(style)
    style_key = str(style or "").strip() or "DEFAULT"
    relaxed_cap_style = style_key in ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS
    relaxed_floor_fields = ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS.get(style_key, set())

    def _floor(metric: str) -> float:
        base_value = float(base[metric])
        global_value = float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS[metric])
        if metric in relaxed_floor_fields:
            return base_value
        return max(base_value, global_value)

    def _cap(metric: str) -> float:
        base_value = float(base[metric])
        global_value = float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS[metric])
        if relaxed_cap_style:
            return base_value
        return min(base_value, global_value)

    return {
        "annual_return_min": _floor("annual_return_min"),
        "sharpe_min": _floor("sharpe_min"),
        "sortino_min": _floor("sortino_min"),
        "calmar_min": _floor("calmar_min"),
        "max_drawdown_max": _cap("max_drawdown_max"),
        "volatility_max": _cap("volatility_max"),
        "positive_ratio_min": _floor("positive_ratio_min"),
        "fee_total_max": _cap("fee_total_max"),
        "scale_billion_min": max(float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]), 2.0),
    }


def style_relative_thresholds(group: pd.DataFrame, style: str) -> dict[str, float]:
    floors = combined_quality_floor(style)

    def _max_quantile(column: str, quantile: float, floor_value: float) -> float:
        series = pd.to_numeric(group.get(column), errors="coerce").dropna()
        if series.empty:
            return float(floor_value)
        return max(float(floor_value), float(series.quantile(quantile)))

    def _min_quantile(column: str, quantile: float, cap_value: float) -> float:
        if column == "费率合计(%)":
            series = pd.to_numeric(
                group.apply(lambda row: resolve_fee_screening_value(row.to_dict()), axis=1),
                errors="coerce",
            ).dropna()
        else:
            series = pd.to_numeric(group.get(column), errors="coerce").dropna()
        if series.empty:
            return float(cap_value)
        return min(float(cap_value), float(series.quantile(quantile)))

    return {
        "weighted_score_min": max(
            PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["weighted_score_min"],
            _max_quantile(
                "综合加权得分",
                DYNAMIC_THRESHOLD_CONFIG["weighted_quantile"],
                DYNAMIC_THRESHOLD_CONFIG["weighted_floor_min"],
            ),
        ),
        "short_score_min": max(
            PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["short_score_min"],
            _max_quantile(
                "近期得分",
                DYNAMIC_THRESHOLD_CONFIG["score_quantile"],
                DYNAMIC_THRESHOLD_CONFIG["score_floor_min"],
            ),
        ),
        "long_score_min": max(
            PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["long_score_min"],
            _max_quantile(
                "长期得分",
                DYNAMIC_THRESHOLD_CONFIG["score_quantile"],
                DYNAMIC_THRESHOLD_CONFIG["score_floor_min"],
            ),
        ),
        "annual_return_min": _max_quantile(
            "1年-年化收益(%)",
            DYNAMIC_THRESHOLD_CONFIG["annual_quantile"],
            floors["annual_return_min"],
        ),
        "sharpe_min": _max_quantile(
            "1年-夏普比率",
            DYNAMIC_THRESHOLD_CONFIG["sharpe_quantile"],
            floors["sharpe_min"],
        ),
        "sortino_min": _max_quantile(
            "1年-索提诺比率",
            DYNAMIC_THRESHOLD_CONFIG["sortino_quantile"],
            floors["sortino_min"],
        ),
        "calmar_min": _max_quantile(
            "1年-卡玛比率",
            DYNAMIC_THRESHOLD_CONFIG["calmar_quantile"],
            floors["calmar_min"],
        ),
        "positive_ratio_min": _max_quantile(
            "1年-正收益比率",
            DYNAMIC_THRESHOLD_CONFIG["positive_quantile"],
            floors["positive_ratio_min"],
        ),
        "fee_total_max": _min_quantile(
            "费率合计(%)",
            DYNAMIC_THRESHOLD_CONFIG["fee_quantile"],
            floors["fee_total_max"],
        ),
        "max_drawdown_max": _min_quantile(
            "1年-最大回撤(%)",
            DYNAMIC_THRESHOLD_CONFIG["mdd_quantile"],
            floors["max_drawdown_max"],
        ),
        "volatility_max": _min_quantile(
            "1年-波动率",
            DYNAMIC_THRESHOLD_CONFIG["volatility_quantile"],
            floors["volatility_max"],
        ),
        "scale_billion_min": float(floors["scale_billion_min"]),
    }


def _winsorize_series(
    series: pd.Series,
    *,
    lower_quantile: float,
    upper_quantile: float,
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return numeric
    lower_bound = float(valid.quantile(lower_quantile))
    upper_bound = float(valid.quantile(upper_quantile))
    if lower_bound > upper_bound:
        lower_bound, upper_bound = upper_bound, lower_bound
    return numeric.clip(lower=lower_bound, upper=upper_bound)


def prepare_robust_threshold_group(
    df: pd.DataFrame,
    *,
    lower_quantile: float,
    upper_quantile: float,
    clip_columns: list[str] | None = None,
) -> pd.DataFrame:
    adjusted = df.copy()
    for column in clip_columns or ROBUST_CLIP_COLUMNS:
        if column not in adjusted.columns:
            continue
        adjusted[column] = _winsorize_series(
            adjusted[column],
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
        )
    return adjusted


def apply_fixed_quality_thresholds(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    if enriched.empty:
        return enriched

    verdicts: list[str] = []
    watch_buckets: list[str] = []
    reason_texts: list[str] = []
    threshold_labels: list[str] = []
    reason_codes_texts: list[str] = []

    for row in enriched.to_dict(orient="records"):
        base_gate_pass = bool(row.get("基础门禁通过"))
        base_reason = str(row.get("基础推荐理由") or "").strip()
        base_bucket = str(row.get("基础观察分类") or "").strip()
        reason_codes: list[str] = []
        weighted_score = _safe_float(row.get("综合加权得分"))
        annual_return = _safe_float(row.get("1年-年化收益(%)"))
        sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
        calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
        positive_ratio = _safe_float(row.get("1年-正收益比率"))
        fee_total = resolve_fee_screening_value(row)
        max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
        volatility = _safe_float(row.get("1年-波动率"))
        scale_billion = resolve_scale_for_thresholds(row) or 0.0
        style = str(row.get("债基风格标签") or "未知").strip() or "未知"
        style_floors = combined_quality_floor(style)

        if base_gate_pass:
            if weighted_score is None or weighted_score < PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["weighted_score_min"]:
                reason_codes.append("below_weighted_floor")
            if annual_return is None or annual_return < style_floors["annual_return_min"]:
                reason_codes.append("below_annual_floor")
            if calmar_ratio is None or calmar_ratio < style_floors["calmar_min"]:
                reason_codes.append("below_calmar_floor")
            if positive_ratio is None or positive_ratio < style_floors["positive_ratio_min"]:
                reason_codes.append("below_positive_ratio_floor")
            if fee_total is None:
                reason_codes.append("fee_data_missing")
            elif fee_total > style_floors["fee_total_max"]:
                reason_codes.append("fee_too_high")
            if max_drawdown is None or max_drawdown > style_floors["max_drawdown_max"]:
                reason_codes.append("mdd_too_high")
            if volatility is None or volatility > style_floors["volatility_max"]:
                reason_codes.append("volatility_too_high")
            if scale_billion < PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]:
                reason_codes.append("scale_too_small")
            reason_codes = _apply_scale_near_relief_reason_codes(row, reason_codes)
            reason_codes = _apply_annual_return_near_relief_reason_codes(
                row,
                reason_codes,
                annual_threshold=style_floors["annual_return_min"],
            )
            reason_codes = _apply_positive_ratio_near_relief_reason_codes(
                row,
                reason_codes,
                positive_ratio_threshold=style_floors["positive_ratio_min"],
            )

        if not base_gate_pass:
            verdicts.append(AUTO_DIAGNOSIS_VERDICT)
            watch_buckets.append(base_bucket)
            reason_texts.append(base_reason)
            reason_codes_texts.append(str(row.get("基础推荐原因代码") or ""))
        else:
            verdicts.append("正式推荐" if not reason_codes else AUTO_DIAGNOSIS_VERDICT)
            watch_buckets.append("" if not reason_codes else ABSOLUTE_QUALITY_DIAGNOSIS)
            reason_texts.append("；".join(PROJECT_RECOMMENDATION_REASONS[item] for item in reason_codes))
            reason_codes_texts.append("|".join(reason_codes))

        threshold_labels.append(
            f"W≥{PROJECT_ABSOLUTE_QUALITY_THRESHOLDS['weighted_score_min']:.2f} / "
            f"Ann≥{style_floors['annual_return_min']:.2f} / "
            f"Calmar≥{style_floors['calmar_min']:.2f} / "
            f"Pos≥{style_floors['positive_ratio_min']:.2f} / "
            f"Fee≤{style_floors['fee_total_max']:.2f} / "
            f"MDD≤{style_floors['max_drawdown_max']:.4f} / "
            f"Vol≤{style_floors['volatility_max']:.4f} / "
            f"Scale≥{PROJECT_ABSOLUTE_QUALITY_THRESHOLDS['scale_billion_min']:.1f}"
        )

    enriched["fixed_threshold_verdict"] = verdicts
    enriched["fixed_threshold_watch_bucket"] = watch_buckets
    enriched["fixed_threshold_reason"] = reason_texts
    enriched["fixed_threshold_reason_codes"] = reason_codes_texts
    enriched["fixed_threshold_label"] = threshold_labels
    return enriched


def compute_dynamic_style_thresholds(
    df: pd.DataFrame,
) -> tuple[dict[str, dict[str, float]], dict[str, float], dict[str, dict[str, Any]]]:
    style_thresholds: dict[str, dict[str, float]] = {}
    style_meta: dict[str, dict[str, Any]] = {}
    if df.empty:
        return style_thresholds, PROJECT_ABSOLUTE_QUALITY_THRESHOLDS.copy(), style_meta

    robust_clip_enabled = bool(DYNAMIC_THRESHOLD_CONFIG.get("robust_clip_enabled", False))
    lower_quantile = float(DYNAMIC_THRESHOLD_CONFIG.get("robust_clip_lower_quantile", 0.05) or 0.05)
    upper_quantile = float(DYNAMIC_THRESHOLD_CONFIG.get("robust_clip_upper_quantile", 0.95) or 0.95)

    adjusted_global_df = (
        prepare_robust_threshold_group(
            df,
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
        )
        if robust_clip_enabled
        else df
    )
    global_thresholds = style_relative_thresholds(adjusted_global_df, "DEFAULT")
    grouped = df.copy()
    grouped["债基风格标签"] = grouped.get("债基风格标签", "").map(
        lambda value: str(value or "").strip() or "未知"
    )
    for style, group in grouped.groupby("债基风格标签"):
        sample_size = int(len(group.index))
        if len(group.index) < DYNAMIC_THRESHOLD_CONFIG["min_style_sample_size"]:
            style_thresholds[str(style)] = global_thresholds.copy()
            style_meta[str(style)] = {
                "style": str(style),
                "sample_size": sample_size,
                "threshold_source": "global_fallback",
                **global_thresholds,
            }
        else:
            adjusted_group = (
                prepare_robust_threshold_group(
                    group,
                    lower_quantile=lower_quantile,
                    upper_quantile=upper_quantile,
                )
                if robust_clip_enabled
                else group
            )
            thresholds = style_relative_thresholds(adjusted_group, str(style))
            style_thresholds[str(style)] = thresholds
            style_meta[str(style)] = {
                "style": str(style),
                "sample_size": sample_size,
                "threshold_source": "style_quantile_robust" if robust_clip_enabled else "style_quantile",
                **thresholds,
            }
    return style_thresholds, global_thresholds, style_meta


def apply_dynamic_quality_thresholds(
    df: pd.DataFrame,
    *,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    style_meta: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    enriched = df.copy()
    if enriched.empty:
        return enriched

    verdicts: list[str] = []
    watch_buckets: list[str] = []
    reason_texts: list[str] = []
    threshold_labels: list[str] = []
    threshold_sources: list[str] = []
    reason_codes_texts: list[str] = []

    for row in enriched.to_dict(orient="records"):
        style = str(row.get("债基风格标签") or "未知").strip() or "未知"
        thresholds = style_thresholds.get(style) or global_thresholds
        threshold_meta = style_meta.get(style) or {}
        weighted_score = _safe_float(row.get("综合加权得分"))
        annual_return = _safe_float(row.get("1年-年化收益(%)"))
        sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
        calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
        positive_ratio = _safe_float(row.get("1年-正收益比率"))
        fee_total = resolve_fee_screening_value(row)
        max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
        volatility = _safe_float(row.get("1年-波动率"))
        scale_billion = resolve_scale_for_thresholds(row) or 0.0
        base_gate_pass = bool(row.get("基础门禁通过"))
        base_reason = str(row.get("基础推荐理由") or "").strip()
        base_bucket = str(row.get("基础观察分类") or "").strip()

        reason_codes: list[str] = []
        if base_gate_pass:
            if weighted_score is None or weighted_score < thresholds["weighted_score_min"]:
                reason_codes.append("below_weighted_floor")
            if annual_return is None or annual_return < thresholds["annual_return_min"]:
                reason_codes.append("below_annual_floor")
            if calmar_ratio is None or calmar_ratio < thresholds["calmar_min"]:
                reason_codes.append("below_calmar_floor")
            if positive_ratio is None or positive_ratio < thresholds["positive_ratio_min"]:
                reason_codes.append("below_positive_ratio_floor")
            if fee_total is None:
                reason_codes.append("fee_data_missing")
            elif fee_total > thresholds["fee_total_max"]:
                reason_codes.append("fee_too_high")
            if max_drawdown is None or max_drawdown > thresholds["max_drawdown_max"]:
                reason_codes.append("mdd_too_high")
            if volatility is None or volatility > thresholds["volatility_max"]:
                reason_codes.append("volatility_too_high")
            if scale_billion < thresholds["scale_billion_min"]:
                reason_codes.append("scale_too_small")
            reason_codes = _apply_scale_near_relief_reason_codes(row, reason_codes)
            reason_codes = _apply_annual_return_near_relief_reason_codes(
                row,
                reason_codes,
                annual_threshold=thresholds["annual_return_min"],
            )
            reason_codes = _apply_positive_ratio_near_relief_reason_codes(
                row,
                reason_codes,
                positive_ratio_threshold=thresholds["positive_ratio_min"],
            )

        if not base_gate_pass:
            verdicts.append(AUTO_DIAGNOSIS_VERDICT)
            watch_buckets.append(base_bucket)
            reason_texts.append(base_reason)
            reason_codes_texts.append(str(row.get("基础推荐原因代码") or ""))
        else:
            verdicts.append("正式推荐" if not reason_codes else AUTO_DIAGNOSIS_VERDICT)
            watch_buckets.append("" if not reason_codes else DYNAMIC_QUALITY_DIAGNOSIS)
            reason_texts.append("；".join(PROJECT_RECOMMENDATION_REASONS[item] for item in reason_codes))
            reason_codes_texts.append("|".join(reason_codes))

        threshold_labels.append(
            f"{style}: W≥{thresholds['weighted_score_min']:.2f} / "
            f"Ann≥{thresholds['annual_return_min']:.2f} / "
            f"Calmar≥{thresholds['calmar_min']:.2f} / "
            f"Pos≥{thresholds['positive_ratio_min']:.2f} / "
            f"Fee≤{thresholds['fee_total_max']:.2f} / "
            f"MDD≤{thresholds['max_drawdown_max']:.4f} / "
            f"Vol≤{thresholds['volatility_max']:.4f} / "
            f"Scale≥{thresholds['scale_billion_min']:.1f}"
        )
        threshold_sources.append(str(threshold_meta.get("threshold_source") or "global_fallback"))

    enriched["dynamic_threshold_verdict"] = verdicts
    enriched["dynamic_threshold_watch_bucket"] = watch_buckets
    enriched["dynamic_threshold_reason"] = reason_texts
    enriched["dynamic_threshold_reason_codes"] = reason_codes_texts
    enriched["dynamic_threshold_label"] = threshold_labels
    enriched["dynamic_threshold_source"] = threshold_sources
    return enriched


def build_enhanced_supplemental_label(style: str, thresholds: dict[str, float]) -> str:
    return (
        f"{style}补充准入: "
        f"Ann≥{thresholds['annual_return_min']:.2f} / "
        f"Sharpe≥{thresholds['sharpe_min']:.2f} / "
        f"Calmar≥{thresholds['calmar_min']:.2f} / "
        f"MDD≤{thresholds['max_drawdown_max']:.4f} / "
        f"Pos≥{thresholds['positive_ratio_min']:.2f} / "
        f"Fee≤{thresholds['fee_total_max']:.2f} / "
        f"Scale≥{thresholds['scale_billion_min']:.1f}"
    )


def apply_enhanced_supplemental_gate(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    if enriched.empty:
        enriched["enhanced_supplemental_pass"] = pd.Series(dtype=bool)
        enriched["enhanced_supplemental_label"] = pd.Series(dtype=object)
        enriched["enhanced_supplemental_reason"] = pd.Series(dtype=object)
        return enriched

    supplemental_passes: list[bool] = []
    supplemental_labels: list[str] = []
    supplemental_reasons: list[str] = []

    for row in enriched.to_dict(orient="records"):
        style = str(row.get("债基风格标签") or "未知").strip() or "未知"
        thresholds = ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS.get(style)
        current_verdict = str(row.get("dynamic_threshold_verdict") or "").strip()
        base_gate_pass = bool(row.get("基础门禁通过"))

        label = ""
        reason_text = ""
        passed = False

        if thresholds and base_gate_pass and current_verdict != "正式推荐":
            annual_return = _safe_float(row.get("1年-年化收益(%)"))
            sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
            calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
            max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
            positive_ratio = _safe_float(row.get("1年-正收益比率"))
            fee_total = resolve_fee_screening_value(row)
            # Supplemental admission is a second route through the same
            # minimum-scale contract, not a different share-only definition.
            scale_billion = resolve_scale_for_thresholds(row) or 0.0

            reason_items: list[str] = []
            if annual_return is None or annual_return < thresholds["annual_return_min"]:
                reason_items.append("年化收益不足")
            if sharpe_ratio is None or sharpe_ratio < thresholds["sharpe_min"]:
                reason_items.append("夏普不足")
            if calmar_ratio is None or calmar_ratio < thresholds["calmar_min"]:
                reason_items.append("卡玛不足")
            if max_drawdown is None or max_drawdown > thresholds["max_drawdown_max"]:
                reason_items.append("回撤偏高")
            if positive_ratio is None or positive_ratio < thresholds["positive_ratio_min"]:
                reason_items.append("正收益率不足")
            if fee_total is None:
                reason_items.append("费率数据缺失")
            elif fee_total > thresholds["fee_total_max"]:
                reason_items.append("费率偏高")
            if scale_billion < thresholds["scale_billion_min"]:
                reason_items.append("规模偏小")

            label = build_enhanced_supplemental_label(style, thresholds)
            if not reason_items:
                passed = True
                reason_text = f"增强债基补充准入通过；{label}"
            else:
                reason_text = "；".join(reason_items)

        supplemental_passes.append(passed)
        supplemental_labels.append(label)
        supplemental_reasons.append(reason_text)

    enriched["enhanced_supplemental_pass"] = supplemental_passes
    enriched["enhanced_supplemental_label"] = supplemental_labels
    enriched["enhanced_supplemental_reason"] = supplemental_reasons

    pass_mask = enriched["enhanced_supplemental_pass"].astype(bool)
    if pass_mask.any():
        enriched.loc[pass_mask, "dynamic_threshold_verdict"] = "正式推荐"
        enriched.loc[pass_mask, "dynamic_threshold_watch_bucket"] = ""
        enriched.loc[pass_mask, "dynamic_threshold_reason"] = enriched.loc[
            pass_mask, "enhanced_supplemental_reason"
        ]

    return enriched


def build_dynamic_threshold_detail_frame(
    *,
    style_meta: dict[str, dict[str, Any]],
    global_thresholds: dict[str, float],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for style in sorted(style_meta.keys(), key=lambda item: (item != "未知", item)):
        payload = style_meta[style]
        threshold_source = str(payload.get("threshold_source") or "").strip()
        if threshold_source == "style_quantile_robust":
            threshold_source_label = "风格分位数（稳健截尾）"
        elif threshold_source == "style_quantile":
            threshold_source_label = "风格分位数"
        else:
            threshold_source_label = "全局回退"
        rows.append(
            {
                "债基风格标签": payload.get("style"),
                "样本数": int(payload.get("sample_size") or 0),
                "阈值来源": threshold_source_label,
                "综合加权得分下限": round(float(payload.get("weighted_score_min") or 0.0), 2),
                "近期得分下限": round(float(payload.get("short_score_min") or 0.0), 2),
                "长期得分下限": round(float(payload.get("long_score_min") or 0.0), 2),
                "1年年化收益下限": round(float(payload.get("annual_return_min") or 0.0), 2),
                "1年夏普下限": round(float(payload.get("sharpe_min") or 0.0), 2),
                "1年索提诺下限": round(float(payload.get("sortino_min") or 0.0), 2),
                "1年卡玛下限": round(float(payload.get("calmar_min") or 0.0), 2),
                "1年正收益率下限": round(float(payload.get("positive_ratio_min") or 0.0), 2),
                "费率上限(%)": round(float(payload.get("fee_total_max") or 0.0), 2),
                "1年最大回撤上限": round(float(payload.get("max_drawdown_max") or 0.0), 4),
                "1年波动率上限": round(float(payload.get("volatility_max") or 0.0), 4),
                "规模下限(亿)": round(float(payload.get("scale_billion_min") or 0.0), 2),
            }
        )

    rows.append(
        {
            "债基风格标签": "全局基准",
            "样本数": "",
            "阈值来源": "全局分位数",
            "综合加权得分下限": round(float(global_thresholds.get("weighted_score_min") or 0.0), 2),
            "近期得分下限": round(float(global_thresholds.get("short_score_min") or 0.0), 2),
            "长期得分下限": round(float(global_thresholds.get("long_score_min") or 0.0), 2),
            "1年年化收益下限": round(float(global_thresholds.get("annual_return_min") or 0.0), 2),
            "1年夏普下限": round(float(global_thresholds.get("sharpe_min") or 0.0), 2),
            "1年索提诺下限": round(float(global_thresholds.get("sortino_min") or 0.0), 2),
            "1年卡玛下限": round(float(global_thresholds.get("calmar_min") or 0.0), 2),
            "1年正收益率下限": round(float(global_thresholds.get("positive_ratio_min") or 0.0), 2),
            "费率上限(%)": round(float(global_thresholds.get("fee_total_max") or 0.0), 2),
            "1年最大回撤上限": round(float(global_thresholds.get("max_drawdown_max") or 0.0), 4),
            "1年波动率上限": round(float(global_thresholds.get("volatility_max") or 0.0), 4),
            "规模下限(亿)": round(float(global_thresholds.get("scale_billion_min") or 0.0), 2),
        }
    )
    return pd.DataFrame(rows)


__all__ = [
    "ANNUAL_RETURN_NEAR_THRESHOLD_RELIEF",
    "DYNAMIC_THRESHOLD_CONFIG",
    "ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS",
    "ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS",
    "POSITIVE_RATIO_NEAR_THRESHOLD_RELIEF",
    "PROJECT_ABSOLUTE_QUALITY_THRESHOLDS",
    "PROJECT_SCORE_STYLE_FLOORS",
    "ROBUST_CLIP_COLUMNS",
    "SCALE_NEAR_THRESHOLD_RELIEF",
    "apply_dynamic_quality_thresholds",
    "apply_enhanced_supplemental_gate",
    "apply_fixed_quality_thresholds",
    "build_dynamic_threshold_detail_frame",
    "build_enhanced_supplemental_label",
    "combined_quality_floor",
    "compute_dynamic_style_thresholds",
    "prepare_robust_threshold_group",
    "project_style_floor",
    "resolve_scale_for_thresholds",
    "style_relative_thresholds",
]
