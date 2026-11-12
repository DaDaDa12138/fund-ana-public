from __future__ import annotations

import argparse
import hashlib
import inspect
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.audit_support import write_compressed_audit_csv
from backend.artifact_paths import project_relative_artifact_path
from backend.annual_return_gate_shadow import (
    FrozenAnnualScenarioIncomplete, build_incomplete_annual_shadow_summary,
)
from backend.incremental_review_replay import apply_scenario_incremental_review
from backend.screening_baseline_replay import (
    _validation_provider, missing_validation_observation_codes, missing_tracker_history_codes,
    scenario_tracker_features, verify_frozen_baseline,
)
from backend import report_builder as _production

# Compatibility export for callers and isolated audits that patch the
# production AutoStock demotion boundary at the script module level.  The
# implementation remains owned by report_builder; this alias does not enable
# the retired provider or change its production policy.
_demote_autostock_hard_block_rows = _production._demote_autostock_hard_block_rows
from backend.diagnosis_contract import diagnosis_categories, diagnosis_category, summarize_diagnoses
from backend.project_publish_decisions import (
    AUTO_DIAGNOSIS_CATEGORY_COLUMN, AUTO_DIAGNOSIS_VERDICT,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
)
from backend.project_formal_groups import FORMAL_GROUP_LABEL
from backend.publish_reason_summary import (
    _build_annual_return_gate_shadow_decision_summary,
)
from backend.runtime_replay import (
    build_replay_precision_summary as _build_replay_precision_summary,
    load_runtime_payload as _load_runtime_payload,
)
from backend.report_inputs import (
    load_publish_chain_snapshot as _load_publish_chain_snapshot,
    merge_runtime_artifacts as _merge_runtime_artifacts,
    resolve_runtime_artifact_path as _resolve_runtime_artifact_path,
)
from backend.project_quality_thresholds import (
    apply_dynamic_quality_thresholds,
    apply_enhanced_supplemental_gate,
    apply_fixed_quality_thresholds,
)
from backend.publish_pipeline import (
    append_formal_group_column as _append_formal_group_column,
    append_project_decision_columns as _append_project_decision_columns,
    apply_incremental_safety_watch_alignment as _apply_incremental_safety_watch_alignment,
    apply_final_official_selection as _apply_final_official_selection,
    apply_publish_post_filters as _apply_publish_post_filters,
    build_final_selection_tracker_features as _build_final_selection_tracker_features,
    build_portfolio_suggestions as _build_portfolio_suggestions,
    build_priority_pool_tracker_features as _build_priority_pool_tracker_features,
    compute_dynamic_style_thresholds as _compute_dynamic_style_thresholds,
    load_decision_record_map as _load_decision_record_map,
    load_soft_warning_map as _load_soft_warning_map,
    materialize_track_frame as _materialize_track_frame,
    normalize_code as _normalize_code,
    overlay_autostock_statuses as _overlay_autostock_statuses,
    refresh_recommendation_notes as _refresh_recommendation_notes,
    supplement_official_autostock_statuses as _supplement_official_autostock_statuses,
)
from backend.report_builder import (
    FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY,
    FINAL_SELECTION_HIGH_CONFIDENCE_BONUS,
    FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS,
    FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT,
    FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT,
    FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY,
    FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS,
)
from backend.forward_validation import (
    FORWARD_VALIDATION_WINDOWS,
    END_NAV_MAX_LAG_DAYS,
    START_NAV_MAX_LAG_DAYS,
    _build_window_metrics_frame as _build_forward_window_metrics_frame,
    _cohort_metrics as _build_forward_cohort_metrics,
    _load_nav_history_for_codes as _load_forward_nav_history_for_codes,
    _safe_date as _safe_forward_date,
)
from scripts.audit_holding_period_gate_impact import (
    _load_actual_autostock_status_map,
    _build_candidate_universe,
    _load_actual_official_df,
    _resolve_latest_run_payload,
)


def _call_apply_final_official_selection(
    df: pd.DataFrame,
    *,
    settings=None,
    tracker_features_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    try:
        return _apply_final_official_selection(
            df,
            settings=settings,
            tracker_features_df=tracker_features_df,
        )
    except TypeError:
        return _apply_final_official_selection(df)


def _score_band(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric >= 90:
        return "90+"
    if numeric >= 85:
        return "85-89.99"
    if numeric >= 80:
        return "80-84.99"
    if numeric >= 70:
        return "70-79.99"
    if numeric >= 60:
        return "60-69.99"
    return "<60"


def _local_review_score_band(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric >= 80:
        return "80+"
    if numeric >= 70:
        return "70-79.99"
    if numeric >= 60:
        return "60-69.99"
    return "<60"


def _annual_band(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric >= 6.0:
        return "6.0+"
    if numeric >= 4.0:
        return "4.0-5.99"
    if numeric >= 3.0:
        return "3.0-3.99"
    if numeric >= 2.5:
        return "2.5-2.99"
    if numeric >= 2.0:
        return "2.0-2.49"
    if numeric >= 1.5:
        return "1.5-1.99"
    return "<1.5"


def _series_count_map(series: pd.Series) -> dict[str, int]:
    if series.empty:
        return {}
    counts = series.fillna("").astype(str).value_counts()
    return {str(index): int(value) for index, value in counts.items()}


def _clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _safe_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([float("nan")] * len(df.index), index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _safe_bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([False] * len(df.index), index=df.index, dtype=bool)
    return df[column].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def _round_float(value: Any, digits: int = 6) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return round(float(numeric), digits)


def _numeric_stats_payload(series: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return {
            "available": False,
            "valid_count": 0,
            "missing_count": int(len(numeric.index)),
        }
    return {
        "available": True,
        "valid_count": int(len(valid.index)),
        "missing_count": int(len(numeric.index) - len(valid.index)),
        "mean": _round_float(valid.mean()),
        "median": _round_float(valid.median()),
        "min": _round_float(valid.min()),
        "max": _round_float(valid.max()),
    }


def _text_counts_payload(series: pd.Series) -> dict[str, Any]:
    cleaned = series.map(_clean_text)
    known = cleaned[cleaned.astype(str).ne("")]
    counts = known.value_counts()
    return {
        "available": bool(not known.empty),
        "known_count": int(len(known.index)),
        "missing_count": int(len(cleaned.index) - len(known.index)),
        "counts": {str(index): int(value) for index, value in counts.items()},
    }


_REPLACEMENT_NUMERIC_METRICS: tuple[dict[str, str], ...] = (
    {"column": "正式投资评分", "metric_key": "investment_score", "direction": "higher_better"},
    {"column": "综合加权得分", "metric_key": "weighted_score", "direction": "higher_better"},
    {"column": "1年-年化收益(%)", "metric_key": "annual_return_1y_pct", "direction": "higher_better"},
    {"column": "1年-夏普比率", "metric_key": "sharpe_1y", "direction": "higher_better"},
    {"column": "1年-索提诺比率", "metric_key": "sortino_1y", "direction": "higher_better"},
    {"column": "1年-卡玛比率", "metric_key": "calmar_1y", "direction": "higher_better"},
    {"column": "1年-最大回撤(%)", "metric_key": "max_drawdown_1y_pct", "direction": "lower_better"},
    {"column": "1年-波动率", "metric_key": "volatility_1y", "direction": "lower_better"},
    {"column": "1年-正收益比率", "metric_key": "positive_ratio_1y", "direction": "higher_better"},
    {"column": "基金规模(亿)", "metric_key": "fund_size_100m", "direction": "contextual"},
    {"column": "费率合计(%)", "metric_key": "fee_total_pct", "direction": "lower_better"},
    {"column": "出现次数", "metric_key": "tracker_occurrence_count", "direction": "higher_better"},
    {"column": "出现频率(%)", "metric_key": "tracker_occurrence_frequency_pct", "direction": "higher_better"},
    {"column": "跟踪总期数", "metric_key": "tracker_total_periods", "direction": "contextual"},
)

_REPLACEMENT_TEXT_METRICS: tuple[dict[str, str], ...] = (
    {"column": FORMAL_GROUP_LABEL, "metric_key": "formal_group"},
    {"column": "债基风格标签", "metric_key": "style_tag"},
    {"column": "持有期状态", "metric_key": "holding_period_status"},
    {"column": "AutoStock原始校验状态", "metric_key": "autostock_raw_status"},
    {"column": "AutoStock校验状态", "metric_key": "autostock_status"},
    {"column": "项目质量信心", "metric_key": "quality_confidence"},
    {"column": "长期稳定性标签", "metric_key": "long_term_stability_label"},
)

_QUALITY_SIGNAL_FOCUS_METRICS = {
    "investment_score",
    "weighted_score",
    "annual_return_1y_pct",
    "sharpe_1y",
    "sortino_1y",
    "calmar_1y",
    "max_drawdown_1y_pct",
    "volatility_1y",
    "positive_ratio_1y",
    "fee_total_pct",
    "tracker_occurrence_count",
    "tracker_occurrence_frequency_pct",
}


def _merge_tracker_features_for_quality_review(
    focus_df: pd.DataFrame,
    tracker_features_df: pd.DataFrame,
) -> pd.DataFrame:
    if focus_df.empty or tracker_features_df.empty:
        return focus_df

    current = focus_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    tracker_current = tracker_features_df.copy()
    tracker_current["基金代码"] = tracker_current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    tracker_current = tracker_current.drop_duplicates(subset=["基金代码"], keep="first")
    tracker_columns = [
        column
        for column in ["出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]
        if column in tracker_current.columns and column not in current.columns
    ]
    if not tracker_columns:
        return current
    return current.merge(
        tracker_current[["基金代码", *tracker_columns]],
        on="基金代码",
        how="left",
    )


def _build_replacement_role_frame(
    source_df: pd.DataFrame,
    *,
    codes: set[str],
    role: str,
    tracker_features_df: pd.DataFrame,
) -> pd.DataFrame:
    normalized_codes = {_normalize_code(code) for code in codes if _normalize_code(code)}
    if source_df.empty or not normalized_codes:
        return pd.DataFrame()
    current = source_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    current = current[current["基金代码"].isin(normalized_codes)].copy()
    current = current.drop_duplicates(subset=["基金代码"], keep="first")
    if current.empty:
        return current
    current = _merge_tracker_features_for_quality_review(current, tracker_features_df)
    current["_replacement_role"] = role
    return current


def _summarize_replacement_role(role_df: pd.DataFrame) -> dict[str, Any]:
    if role_df.empty:
        return {
            "count": 0,
            "numeric_metrics": {},
            "text_metrics": {},
        }
    numeric_metrics: dict[str, Any] = {}
    for spec in _REPLACEMENT_NUMERIC_METRICS:
        column = spec["column"]
        if column not in role_df.columns:
            numeric_metrics[spec["metric_key"]] = {
                "available": False,
                "valid_count": 0,
                "missing_count": int(len(role_df.index)),
                "source_column": column,
                "direction": spec["direction"],
            }
            continue
        metric_payload = _numeric_stats_payload(role_df[column])
        metric_payload["source_column"] = column
        metric_payload["direction"] = spec["direction"]
        numeric_metrics[spec["metric_key"]] = metric_payload

    text_metrics: dict[str, Any] = {}
    for spec in _REPLACEMENT_TEXT_METRICS:
        column = spec["column"]
        if column not in role_df.columns:
            text_metrics[spec["metric_key"]] = {
                "available": False,
                "known_count": 0,
                "missing_count": int(len(role_df.index)),
                "source_column": column,
                "counts": {},
            }
            continue
        metric_payload = _text_counts_payload(role_df[column])
        metric_payload["source_column"] = column
        text_metrics[spec["metric_key"]] = metric_payload

    return {
        "count": int(len(role_df.index)),
        "numeric_metrics": numeric_metrics,
        "text_metrics": text_metrics,
    }


def _build_replacement_metric_deltas(
    added_summary: dict[str, Any],
    removed_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    deltas: list[dict[str, Any]] = []
    added_metrics = added_summary.get("numeric_metrics") or {}
    removed_metrics = removed_summary.get("numeric_metrics") or {}
    for spec in _REPLACEMENT_NUMERIC_METRICS:
        metric_key = spec["metric_key"]
        added_metric = added_metrics.get(metric_key) or {}
        removed_metric = removed_metrics.get(metric_key) or {}
        if not (added_metric.get("available") and removed_metric.get("available")):
            continue
        added_median = added_metric.get("median")
        removed_median = removed_metric.get("median")
        added_mean = added_metric.get("mean")
        removed_mean = removed_metric.get("mean")
        deltas.append(
            {
                "metric_key": metric_key,
                "source_column": spec["column"],
                "direction": spec["direction"],
                "added_valid_count": int(added_metric.get("valid_count") or 0),
                "removed_valid_count": int(removed_metric.get("valid_count") or 0),
                "added_median": added_median,
                "removed_median": removed_median,
                "median_delta_added_minus_removed": _round_float(
                    float(added_median) - float(removed_median)
                    if added_median is not None and removed_median is not None
                    else None
                ),
                "added_mean": added_mean,
                "removed_mean": removed_mean,
                "mean_delta_added_minus_removed": _round_float(
                    float(added_mean) - float(removed_mean)
                    if added_mean is not None and removed_mean is not None
                    else None
                ),
            }
        )
    return deltas


def _build_replacement_quality_preview(
    added_df: pd.DataFrame,
    removed_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    preview_columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "债基风格标签",
        "_replacement_role",
        "正式投资评分",
        "综合加权得分",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "1年-正收益比率",
        "基金规模(亿)",
        "费率合计(%)",
        "持有期状态",
        "项目质量信心",
        "AutoStock原始校验状态",
        "AutoStock校验状态",
        "长期稳定性标签",
        "出现次数",
        "出现频率(%)",
        "跟踪总期数",
    ]
    current = pd.concat([added_df, removed_df], ignore_index=True, sort=False)
    if current.empty:
        return []
    sort_columns = [
        column
        for column in ["_replacement_role", "正式投资评分", "综合加权得分", "基金代码"]
        if column in current.columns
    ]
    if sort_columns:
        for column in ["正式投资评分", "综合加权得分"]:
            if column in current.columns:
                current[column] = pd.to_numeric(current[column], errors="coerce")
        ascending = [
            True if column in {"_replacement_role", "基金代码"} else False
            for column in sort_columns
        ]
        current = current.sort_values(
            by=sort_columns,
            ascending=ascending,
            kind="mergesort",
        )
    preview_df = current[[column for column in preview_columns if column in current.columns]]
    return preview_df.where(pd.notna(preview_df), "").to_dict(orient="records")


def _build_replacement_quality_review(
    *,
    added_source_df: pd.DataFrame,
    removed_source_df: pd.DataFrame,
    added_codes: set[str],
    removed_codes: set[str],
    added_tracker_features_df: pd.DataFrame,
    removed_tracker_features_df: pd.DataFrame,
    reconstructed_baseline_aligned: bool,
    added_source_label: str = "shadow_official_df",
    removed_source_label: str = "reconstructed_baseline_official_df",
) -> dict[str, Any]:
    added_df = _build_replacement_role_frame(
        added_source_df,
        codes=added_codes,
        role="shadow_added",
        tracker_features_df=added_tracker_features_df,
    )
    removed_df = _build_replacement_role_frame(
        removed_source_df,
        codes=removed_codes,
        role="shadow_removed",
        tracker_features_df=removed_tracker_features_df,
    )
    added_summary = _summarize_replacement_role(added_df)
    removed_summary = _summarize_replacement_role(removed_df)
    comparison_deltas = _build_replacement_metric_deltas(added_summary, removed_summary)
    preview = _build_replacement_quality_preview(added_df, removed_df)

    if not reconstructed_baseline_aligned:
        recommended_action = "fix_reconstructed_baseline_alignment_before_replacement_quality_review"
        reason = "无放宽重建基线未与真实正式榜对齐，替换质量比较不能直接解释为年化门槛放宽造成。"
    elif not added_codes and not removed_codes:
        recommended_action = "observe_only"
        reason = "shadow 未产生新增或剔除样本，暂无替换质量比较需求。"
    else:
        recommended_action = "review_replacement_quality_before_any_threshold_change"
        reason = "shadow 已产生正式榜替换，应先比较新增和剔除样本的真实质量、风险、持有期、AutoStock 状态与历史稳定性，再讨论阈值。"

    return {
        "available": bool(not added_df.empty or not removed_df.empty),
        "count_basis": "shadow_added_vs_removed_final_official_codes",
        "added_count": int(len(added_codes)),
        "removed_count": int(len(removed_codes)),
        "added_observed_count": int(len(added_df.index)),
        "removed_observed_count": int(len(removed_df.index)),
        "reconstructed_baseline_aligned": bool(reconstructed_baseline_aligned),
        "source_labels": {
            "shadow_added": added_source_label,
            "shadow_removed": removed_source_label,
        },
        "role_summaries": {
            "shadow_added": added_summary,
            "shadow_removed": removed_summary,
        },
        "comparison_deltas": comparison_deltas,
        "preview": preview[:20],
        "judgement": {
            "recommended_action": recommended_action,
            "threshold_change_supported_now": False,
            "reason": reason,
        },
    }


def _classify_replacement_delta(delta: dict[str, Any]) -> str:
    direction = str(delta.get("direction") or "").strip()
    median_delta = _round_float(delta.get("median_delta_added_minus_removed"))
    if direction == "contextual":
        return "contextual"
    if median_delta is None:
        return "missing"
    if abs(float(median_delta)) <= 1e-9:
        return "neutral"
    if direction == "higher_better":
        return "added_better" if float(median_delta) > 0 else "removed_better"
    if direction == "lower_better":
        return "added_better" if float(median_delta) < 0 else "removed_better"
    return "unknown"


def _build_replacement_quality_signal(review: dict[str, Any]) -> dict[str, Any]:
    payload = dict(review or {})
    if not payload.get("available"):
        return {
            "available": False,
            "threshold_change_supported_now": False,
            "missing_value_policy": "preserve_missing_semantics",
        }

    focus_deltas: list[dict[str, Any]] = []
    added_better_count = 0
    removed_better_count = 0
    neutral_count = 0
    contextual_count = 0
    missing_count = 0
    for item in list(payload.get("comparison_deltas") or []):
        metric_key = str(item.get("metric_key") or "").strip()
        if metric_key not in _QUALITY_SIGNAL_FOCUS_METRICS:
            continue
        signal = _classify_replacement_delta(item)
        if signal == "added_better":
            added_better_count += 1
        elif signal == "removed_better":
            removed_better_count += 1
        elif signal == "neutral":
            neutral_count += 1
        elif signal == "contextual":
            contextual_count += 1
        elif signal == "missing":
            missing_count += 1
        focus_deltas.append(
            {
                "metric_key": metric_key,
                "source_column": item.get("source_column"),
                "direction": item.get("direction"),
                "added_median": item.get("added_median"),
                "removed_median": item.get("removed_median"),
                "median_delta_added_minus_removed": item.get(
                    "median_delta_added_minus_removed"
                ),
                "signal": signal,
            }
        )

    if removed_better_count > added_better_count:
        quality_direction = "replacement_quality_weaker"
        recommended_action = "reject_local_threshold_change_until_replacement_quality_improves"
        reason = "局部 shadow 新增样本在更多可比真实指标上弱于被替换样本，不能把该场景作为阈值放宽依据。"
    elif added_better_count > removed_better_count:
        quality_direction = "replacement_quality_potentially_better"
        recommended_action = "continue_forward_validation_before_any_threshold_change"
        reason = "局部 shadow 新增样本在更多可比真实指标上优于被替换样本，但仍需前瞻验证和门禁一致性复核。"
    elif added_better_count or removed_better_count:
        quality_direction = "replacement_quality_mixed"
        recommended_action = "compare_forward_performance_before_any_threshold_change"
        reason = "局部 shadow 新增与被替换样本质量信号混合，不能仅凭当前横截面指标调整生产阈值。"
    else:
        quality_direction = "replacement_quality_insufficient_comparable_metrics"
        recommended_action = "collect_more_real_metrics_before_threshold_review"
        reason = "局部 shadow 缺少足够可比真实指标，保持观察。"

    return {
        "available": True,
        "basis": "median_delta_from_replacement_quality_review",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "threshold_change_supported_now": False,
        "quality_direction": quality_direction,
        "recommended_action": recommended_action,
        "reason": reason,
        "added_count": int(payload.get("added_count") or 0),
        "removed_count": int(payload.get("removed_count") or 0),
        "added_observed_count": int(payload.get("added_observed_count") or 0),
        "removed_observed_count": int(payload.get("removed_observed_count") or 0),
        "added_better_metric_count": int(added_better_count),
        "removed_better_metric_count": int(removed_better_count),
        "neutral_metric_count": int(neutral_count),
        "contextual_metric_count": int(contextual_count),
        "missing_metric_count": int(missing_count),
        "metric_count": int(len(focus_deltas)),
        "focus_deltas": focus_deltas,
    }


def _build_local_shadow_replacement_quality_review(
    *,
    local_shadow_official_df: pd.DataFrame,
    reconstructed_baseline_official_df: pd.DataFrame,
    local_added_codes: set[str],
    local_removed_codes: set[str],
    local_final_selection_tracker_features_df: pd.DataFrame,
    reconstructed_baseline_final_selection_tracker_features_df: pd.DataFrame,
    reconstructed_baseline_aligned: bool,
) -> dict[str, Any]:
    review = _build_replacement_quality_review(
        added_source_df=local_shadow_official_df,
        removed_source_df=reconstructed_baseline_official_df,
        added_codes=local_added_codes,
        removed_codes=local_removed_codes,
        added_tracker_features_df=local_final_selection_tracker_features_df,
        removed_tracker_features_df=reconstructed_baseline_final_selection_tracker_features_df,
        reconstructed_baseline_aligned=reconstructed_baseline_aligned,
        added_source_label="local_shadow_official_df",
        removed_source_label="reconstructed_baseline_official_df",
    )
    review["count_basis"] = "local_shadow_added_removed_vs_reconstructed_baseline"
    review["quality_signal"] = _build_replacement_quality_signal(review)
    return review


def _classify_forward_window_delta(
    *,
    added_summary: dict[str, Any],
    removed_summary: dict[str, Any],
) -> str:
    if (
        int(added_summary.get("valid_fund_count") or 0) <= 0
        or int(removed_summary.get("valid_fund_count") or 0) <= 0
    ):
        return "not_comparable"

    added_return = _round_float(added_summary.get("median_return_pct"))
    removed_return = _round_float(removed_summary.get("median_return_pct"))
    added_drawdown = _round_float(added_summary.get("avg_max_drawdown_pct"))
    removed_drawdown = _round_float(removed_summary.get("avg_max_drawdown_pct"))
    added_volatility = _round_float(added_summary.get("avg_volatility_pct"))
    removed_volatility = _round_float(removed_summary.get("avg_volatility_pct"))

    added_better = 0
    removed_better = 0
    comparable = 0
    for left, right, higher_better in (
        (added_return, removed_return, True),
        (added_drawdown, removed_drawdown, False),
        (added_volatility, removed_volatility, False),
    ):
        if left is None or right is None:
            continue
        comparable += 1
        delta = float(left) - float(right)
        if abs(delta) <= 1e-9:
            continue
        if higher_better:
            if delta > 0:
                added_better += 1
            else:
                removed_better += 1
        elif delta < 0:
            added_better += 1
        else:
            removed_better += 1

    if comparable <= 0:
        return "not_comparable"
    if added_better > removed_better:
        return "added_forward_better"
    if removed_better > added_better:
        return "removed_forward_better"
    return "mixed_or_neutral"


def _build_local_shadow_forward_validation(
    settings,
    *,
    scenario_key: Any,
    label: Any,
    snapshot_date: str,
    added_codes: set[str],
    removed_codes: set[str],
) -> dict[str, Any]:
    normalized_added_codes = sorted(
        {_normalize_code(code) for code in added_codes if _normalize_code(code)}
    )
    normalized_removed_codes = sorted(
        {_normalize_code(code) for code in removed_codes if _normalize_code(code)}
    )
    focus_codes = sorted(set(normalized_added_codes) | set(normalized_removed_codes))
    base_payload: dict[str, Any] = {
        "available": False,
        "scenario_key": _clean_text(scenario_key),
        "label": _clean_text(label),
        "basis": "local_shadow_added_removed_forward_nav_history",
        "source": "fund_nav_history",
        "business_value_synthesis": "forbidden",
        "missing_value_policy": "preserve_missing_semantics",
        "threshold_change_supported_now": False,
        "added_count": int(len(normalized_added_codes)),
        "removed_count": int(len(normalized_removed_codes)),
        "ready_window_count": 0,
        "window_count": int(len(FORWARD_VALIDATION_WINDOWS)),
        "window_summaries": [],
        "detail_preview": [],
    }
    if not normalized_added_codes or not normalized_removed_codes:
        return {
            **base_payload,
            "status": "insufficient_replacement_cohorts",
            "recommended_action": "collect_comparable_added_and_removed_samples_before_forward_validation",
            "blocking_reason": "added_or_removed_cohort_empty",
        }

    snapshot_dt = _safe_forward_date(snapshot_date)
    if snapshot_dt is None:
        return {
            **base_payload,
            "status": "snapshot_date_invalid",
            "recommended_action": "fix_snapshot_date_before_forward_validation",
            "blocking_reason": "snapshot_date_invalid",
        }

    max_window_days = max(days for _label, days in FORWARD_VALIDATION_WINDOWS)
    nav_df = _load_forward_nav_history_for_codes(
        settings.database_path,
        focus_codes,
        start_date=(snapshot_dt - pd.Timedelta(days=START_NAV_MAX_LAG_DAYS)).date().isoformat(),
        end_date=(snapshot_dt + pd.Timedelta(days=max_window_days + END_NAV_MAX_LAG_DAYS)).date().isoformat(),
    )
    nav_map = {
        code: group.copy()
        for code, group in nav_df.groupby("fcode", sort=False)
    } if not nav_df.empty else {}

    ready_window_count = 0
    added_better_window_count = 0
    removed_better_window_count = 0
    detail_preview: list[dict[str, Any]] = []
    window_summaries: list[dict[str, Any]] = []
    for window_label, horizon_days in FORWARD_VALIDATION_WINDOWS:
        window_metrics_df = _build_forward_window_metrics_frame(
            focus_codes,
            nav_map,
            snapshot_date=snapshot_dt,
            horizon_days=horizon_days,
        )
        added_summary, added_detail = _build_forward_cohort_metrics(
            "shadow_added",
            normalized_added_codes,
            window_metrics_df,
        )
        removed_summary, removed_detail = _build_forward_cohort_metrics(
            "shadow_removed",
            normalized_removed_codes,
            window_metrics_df,
        )
        window_signal = _classify_forward_window_delta(
            added_summary=added_summary,
            removed_summary=removed_summary,
        )
        comparable = window_signal != "not_comparable"
        if comparable:
            ready_window_count += 1
        if window_signal == "added_forward_better":
            added_better_window_count += 1
        elif window_signal == "removed_forward_better":
            removed_better_window_count += 1

        window_summaries.append(
            {
                "window": window_label,
                "horizon_days": int(horizon_days),
                "status": "ready" if comparable else "future_data_pending",
                "signal": window_signal,
                "added_valid_fund_count": int(added_summary.get("valid_fund_count") or 0),
                "removed_valid_fund_count": int(removed_summary.get("valid_fund_count") or 0),
                "added_median_return_pct": added_summary.get("median_return_pct"),
                "removed_median_return_pct": removed_summary.get("median_return_pct"),
                "median_return_delta_added_minus_removed_pct": (
                    _round_float(
                        float(added_summary.get("median_return_pct")) - float(removed_summary.get("median_return_pct"))
                    )
                    if added_summary.get("median_return_pct") is not None
                    and removed_summary.get("median_return_pct") is not None
                    else None
                ),
                "added_avg_max_drawdown_pct": added_summary.get("avg_max_drawdown_pct"),
                "removed_avg_max_drawdown_pct": removed_summary.get("avg_max_drawdown_pct"),
                "added_avg_volatility_pct": added_summary.get("avg_volatility_pct"),
                "removed_avg_volatility_pct": removed_summary.get("avg_volatility_pct"),
            }
        )
        if len(detail_preview) < 20:
            for role, rows in (("shadow_added", added_detail), ("shadow_removed", removed_detail)):
                for row in rows:
                    if len(detail_preview) >= 20:
                        break
                    detail_preview.append(
                        {
                            "window": window_label,
                            "role": role,
                            "基金代码": row.get("基金代码"),
                            "status": row.get("status"),
                            "start_nav_date": row.get("start_nav_date"),
                            "end_nav_date": row.get("end_nav_date"),
                            "return_pct": row.get("return_pct"),
                            "max_drawdown_pct": row.get("max_drawdown_pct"),
                            "volatility_pct": row.get("volatility_pct"),
                        }
                    )

    if ready_window_count <= 0:
        status = "future_data_pending"
        forward_direction = "forward_validation_pending"
        recommended_action = "wait_for_real_future_nav_before_threshold_change"
        reason = "当前快照之后的真实 NAV 窗口尚未成熟，不能用横截面质量信号替代前瞻表现。"
    elif added_better_window_count > removed_better_window_count:
        status = "ready"
        forward_direction = "added_forward_potentially_better"
        recommended_action = "continue_multi_window_forward_validation_before_any_threshold_change"
        reason = "局部 shadow 新增样本在部分已成熟真实未来窗口中优于被替换样本，但仍需多窗口连续验证。"
    elif removed_better_window_count > added_better_window_count:
        status = "ready"
        forward_direction = "removed_forward_better"
        recommended_action = "reject_local_threshold_change_until_forward_performance_improves"
        reason = "局部 shadow 新增样本在已成熟真实未来窗口中弱于被替换样本，不能作为阈值放宽依据。"
    else:
        status = "ready"
        forward_direction = "forward_validation_mixed"
        recommended_action = "extend_forward_validation_before_any_threshold_change"
        reason = "局部 shadow 新增与被替换样本的真实未来窗口表现混合或接近，不能直接调整生产阈值。"

    return {
        **base_payload,
        "available": True,
        "status": status,
        "forward_direction": forward_direction,
        "recommended_action": recommended_action,
        "reason": reason,
        "blocking_reason": "" if ready_window_count > 0 else "future_window_not_ready",
        "ready_window_count": int(ready_window_count),
        "added_better_window_count": int(added_better_window_count),
        "removed_better_window_count": int(removed_better_window_count),
        "window_summaries": window_summaries,
        "detail_preview": detail_preview,
    }


def _prepare_shadow_candidate_df(
    settings,
    *,
    universe_df: pd.DataFrame,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    evaluated_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, float]], dict[str, float]]:
    candidate_df = _append_project_decision_columns(
        universe_df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        **({"evaluated_at": evaluated_at} if evaluated_at is not None else {}),
    )
    candidate_df = apply_fixed_quality_thresholds(candidate_df)

    dynamic_base_df = candidate_df[candidate_df["基础门禁通过"]].copy()
    style_thresholds, global_thresholds, style_meta = _compute_dynamic_style_thresholds(dynamic_base_df)
    candidate_df = apply_dynamic_quality_thresholds(
        candidate_df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )
    candidate_df = _append_formal_group_column(candidate_df)
    annual_only_df = candidate_df[
        candidate_df["dynamic_threshold_reason_codes"].astype(str).eq("below_annual_floor")
    ].copy()
    return candidate_df, annual_only_df, style_thresholds, global_thresholds


def _materialize_shadow_publish_result(
    settings,
    *,
    candidate_df: pd.DataFrame,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    autostock_status_map: dict[str, dict[str, str]],
    relief_codes: set[str] | None = None,
    supplement_runtime_autostock: bool = True,
    frozen_context: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, bool, pd.DataFrame, pd.DataFrame]:
    if frozen_context is not None and supplement_runtime_autostock:
        raise ValueError("historical_scenario_cannot_supplement_current_autostock")
    shadow_df = candidate_df.copy()
    shadow_df["基金代码"] = shadow_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    annual_only_mask = shadow_df["dynamic_threshold_reason_codes"].astype(str).eq("below_annual_floor")
    relief_mask = annual_only_mask.copy()
    if relief_codes is not None:
        normalized_relief_codes = {
            _normalize_code(code)
            for code in list(relief_codes)
            if _normalize_code(code)
        }
        relief_mask = relief_mask & shadow_df["基金代码"].isin(normalized_relief_codes)
    shadow_df.loc[relief_mask, "dynamic_threshold_verdict"] = "正式推荐"
    shadow_df.loc[relief_mask, "dynamic_threshold_watch_bucket"] = ""
    shadow_df.loc[relief_mask, "dynamic_threshold_reason"] = ""
    shadow_df.loc[relief_mask, "dynamic_threshold_reason_codes"] = ""
    shadow_df = apply_enhanced_supplemental_gate(shadow_df)
    shadow_track_df = _materialize_track_frame(
        shadow_df,
        verdict_column="dynamic_threshold_verdict",
        watch_bucket_column="dynamic_threshold_watch_bucket",
        reason_column="dynamic_threshold_reason",
    )

    publish_track_df = _apply_publish_post_filters(
        shadow_track_df,
        threshold_mode="dynamic",
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        **({"evaluated_at": frozen_context["evaluation_times"]["dynamic_post_review"]}
           if frozen_context is not None else {}),
    )
    publish_official_df = publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    if frozen_context is None:
        final_selection_tracker_features_df = _build_final_selection_tracker_features(
            settings, current_run_id=str(run_id or "").strip(),
            current_generated_at=str(generated_at or "").strip(),
            current_nav_snapshot_date=str(snapshot_date or "").strip(), current_df=publish_official_df,
        )
    else:
        final_selection_tracker_features_df = scenario_tracker_features(frozen_context, publish_official_df)
        missing_history = missing_tracker_history_codes(publish_official_df, final_selection_tracker_features_df)
        if missing_history:
            raise FrozenAnnualScenarioIncomplete("missing_tracker_history", missing_codes=missing_history,
                stage_frames={"candidate_before_annual_relief": candidate_df.copy(),
                              "scenario_post_review": publish_official_df.copy()})
    final_selection = _apply_final_official_selection if frozen_context is not None else _call_apply_final_official_selection
    official_df = final_selection(
        publish_official_df,
        settings=settings,
        tracker_features_df=final_selection_tracker_features_df,
    )
    removed_after_final_selection_df = publish_official_df[
        ~publish_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).isin(
            official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        )
    ].copy()
    if not removed_after_final_selection_df.empty:
        removed_after_final_selection_df["项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
        removed_after_final_selection_df[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS
        removed_after_final_selection_df["发布结论状态"] = "最终精选未入围"
        removed_after_final_selection_df["推荐理由"] = removed_after_final_selection_df.get("推荐理由", "").astype(str).map(
            lambda text: "；".join([item for item in [text.strip(), "通过发布门禁，但未进入最终精选名单"] if item])
        )
        removed_after_final_selection_df["项目推荐原因代码"] = removed_after_final_selection_df.get(
            "项目推荐原因代码",
            pd.Series(dtype=object),
        ).astype(str).map(
            lambda text: "final_selection_removed" if not str(text or "").strip() else f"{str(text).strip()}|final_selection_removed"
        )
        removed_after_final_selection_df["主原因分组"] = "发布复核"
        removed_after_final_selection_df["原因分组汇总"] = removed_after_final_selection_df.get(
            "原因分组汇总",
            pd.Series(dtype=object),
        ).astype(str).map(
            lambda text: "发布复核" if not str(text or "").strip() else f"{str(text).strip()}；发布复核"
        )

    if frozen_context is not None:
        official_codes = set(official_df["基金代码"].map(_normalize_code)) - {""}
        missing_status = missing_validation_observation_codes(
            official_codes, autostock_status_map,
            observed_unvalidated_codes=frozen_context.get(
                "observed_unvalidated_validation_codes",
                frozen_context.get("observed_unvalidated_autostock_codes"),
            ),
            settings=settings,
        )
        if missing_status:
            provider = _validation_provider(settings)
            status = "missing_autostock_observations" if provider == "autostock" else "missing_validation_observations"
            raise FrozenAnnualScenarioIncomplete(status, missing_codes=missing_status,
                stage_frames={"candidate_before_annual_relief": candidate_df.copy(),
                              "scenario_post_review": publish_official_df.copy()})
    autostock_runtime_available = bool(autostock_status_map) if frozen_context is None else False
    official_df = _overlay_autostock_statuses(official_df, publish_official_df)
    if autostock_status_map:
        official_df["_merge_fcode"] = official_df["基金代码"].map(_normalize_code)
        for column in ["AutoStock原始校验状态", "AutoStock告警原因", "AutoStock校验状态", "AutoStock校验备注"]:
            official_df[column] = official_df["_merge_fcode"].map(
                lambda code, col=column: autostock_status_map.get(code, {}).get(col, "") if code else ""
            )
        official_df = official_df.drop(columns=["_merge_fcode"], errors="ignore")
    if supplement_runtime_autostock:
        official_df, autostock_runtime_available = _safe_supplement_official_autostock_statuses(
            official_df,
            settings=settings,
        )
    # A replaced provider must not let a stale AutoStock hard block alter a
    # frozen replay. The report builder only demotes an explicitly identified
    # current validation source in this mode.
    demoter = _demote_autostock_hard_block_rows
    try:
        demoter_parameters = inspect.signature(demoter).parameters
    except (TypeError, ValueError):
        demoter_parameters = {}
    if "allow_legacy_autostock" in demoter_parameters:
        official_df, autostock_blocked_df = demoter(
            official_df, allow_legacy_autostock=_validation_provider(settings) == "autostock",
        )
    else:
        # Preserve compatibility with lightweight test/replay adapters that
        # implement the historical one-argument callable contract.
        official_df, autostock_blocked_df = demoter(official_df)
    if not autostock_blocked_df.empty:
        if removed_after_final_selection_df.empty:
            removed_after_final_selection_df = autostock_blocked_df.copy()
        else:
            removed_after_final_selection_df = pd.concat(
                [removed_after_final_selection_df, autostock_blocked_df],
                ignore_index=True,
            )

    official_df = _refresh_recommendation_notes(official_df)
    official_df = _append_formal_group_column(official_df)
    if frozen_context is None:
        publish_track_df, official_df, _incremental_hit_codes = _apply_incremental_safety_watch_alignment(
            settings=settings, publish_track_df=publish_track_df, official_df=official_df,
            run_id=run_id, generated_at=generated_at, snapshot_date=snapshot_date,
        )
    else:
        incremental_result = apply_scenario_incremental_review(
            frozen_context, {"status": "computed", "official": official_df, "publish_track": publish_track_df},
        )
        if incremental_result["status"] != "computed":
            raise FrozenAnnualScenarioIncomplete("missing_frozen_incremental_review",
                missing_codes=incremental_result["missing_incremental_review_codes"],
                stage_frames={"candidate_before_annual_relief": candidate_df.copy(),
                              "scenario_post_review": publish_official_df.copy()})
        official_df = incremental_result["official"]
        publish_track_df = incremental_result["publish_track"]

    internal_watch_df = pd.concat(
        [
            publish_track_df[publish_track_df["项目推荐结论"] != "正式推荐"].copy(),
            removed_after_final_selection_df,
        ],
        ignore_index=True,
    )
    if "autostock_runtime_available" not in internal_watch_df.columns:
        internal_watch_df["autostock_runtime_available"] = autostock_runtime_available
    official_df["autostock_runtime_available"] = autostock_runtime_available
    return (
        official_df,
        internal_watch_df,
        autostock_runtime_available,
        publish_official_df,
        final_selection_tracker_features_df,
    )


def _build_local_style_band_shadow_scenario_specs(
    coupling_audit: dict[str, Any],
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    dominant_review = coupling_audit.get("dominant_local_boundary_review") or {}
    dominant_style = str(dominant_review.get("style_tag") or "").strip()
    dominant_band = str(dominant_review.get("score_band") or "").strip()
    if dominant_review.get("available") and dominant_style and dominant_band:
        specs.append(
            {
                "scenario_key": "dominant_style_band_all",
                "label": f"{dominant_style}/{dominant_band} 全量局部影子复核",
                "style_tag": dominant_style,
                "score_band": dominant_band,
                "annual_gap_gte": None,
            }
        )
        near_threshold_counts = dominant_review.get("near_threshold_counts") or {}
        for gap_key, gap_floor, gap_label in (
            ("gap_ge_neg_0_10_total", -0.10, "年化缺口>=-0.10pct"),
            ("gap_ge_neg_0_05_total", -0.05, "年化缺口>=-0.05pct"),
        ):
            if int(near_threshold_counts.get(gap_key) or 0) <= 0:
                continue
            specs.append(
                {
                    "scenario_key": gap_key.replace("_total", ""),
                    "label": f"{dominant_style}/{dominant_band} 且{gap_label}",
                    "style_tag": dominant_style,
                    "score_band": dominant_band,
                    "annual_gap_gte": gap_floor,
                }
            )

    seen_pairs = {
        (str(item.get("style_tag") or ""), str(item.get("score_band") or ""))
        for item in specs
    }
    extra_rank = 0
    for item in list(coupling_audit.get("annual_return_only_local_review_priority_preview") or []):
        if extra_rank >= 2:
            break
        if str(item.get("review_priority") or "") != "high":
            continue
        style_tag = str(item.get("债基风格标签") or "").strip()
        score_band = str(item.get("dominant_score_band") or "").strip()
        if not style_tag or score_band not in {"70-79.99", "60-69.99"}:
            continue
        if (style_tag, score_band) in seen_pairs:
            continue
        extra_rank += 1
        specs.append(
            {
                "scenario_key": f"priority_style_band_{extra_rank}",
                "label": f"{style_tag}/{score_band} 局部影子复核",
                "style_tag": style_tag,
                "score_band": score_band,
                "annual_gap_gte": None,
            }
        )
        seen_pairs.add((style_tag, score_band))
    return specs


def _select_local_shadow_relief_candidates(
    annual_only_df: pd.DataFrame,
    *,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    style_tag: str,
    score_band: str,
    annual_gap_gte: float | None = None,
) -> pd.DataFrame:
    if annual_only_df.empty:
        return annual_only_df.copy()

    current = annual_only_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    current["review_score_band"] = current.get("综合加权得分", pd.Series(dtype=object)).map(_local_review_score_band)
    current["annual_return_num"] = _safe_numeric_series(current, "1年-年化收益(%)")
    current["weighted_score_num"] = _safe_numeric_series(current, "综合加权得分")
    current["short_score_num"] = _safe_numeric_series(current, "近期得分")
    current["long_score_num"] = _safe_numeric_series(current, "长期得分")

    def _threshold_value(row: pd.Series, metric_key: str) -> float:
        local_style = str(row.get("债基风格标签") or "").strip()
        style_payload = style_thresholds.get(local_style) or {}
        return float(style_payload.get(metric_key) or global_thresholds.get(metric_key) or 0.0)

    current["annual_floor"] = current.apply(lambda row: _threshold_value(row, "annual_return_min"), axis=1)
    current["weighted_floor"] = current.apply(lambda row: _threshold_value(row, "weighted_score_min"), axis=1)
    current["short_floor"] = current.apply(lambda row: _threshold_value(row, "short_score_min"), axis=1)
    current["long_floor"] = current.apply(lambda row: _threshold_value(row, "long_score_min"), axis=1)
    current["annual_gap_pct"] = current["annual_return_num"] - current["annual_floor"]
    current["weighted_margin"] = current["weighted_score_num"] - current["weighted_floor"]
    current["short_margin"] = current["short_score_num"] - current["short_floor"]
    current["long_margin"] = current["long_score_num"] - current["long_floor"]

    mask = (
        current.get("债基风格标签", pd.Series(dtype=object)).astype(str).eq(style_tag)
        & current["review_score_band"].eq(score_band)
    )
    if annual_gap_gte is not None:
        mask = mask & current["annual_gap_pct"].ge(float(annual_gap_gte))
    return current[mask].copy()


def _resolve_local_shadow_scenario_outcome(
    code: Any,
    *,
    direct_relieved_codes: set[str],
    already_in_actual_codes: set[str],
    watch_reason_map: dict[str, dict[str, str]],
) -> str:
    normalized_code = _normalize_code(code)
    if normalized_code in direct_relieved_codes:
        return "rescued_into_final"
    if normalized_code in already_in_actual_codes:
        return "already_in_actual_final"
    watch_reason = watch_reason_map.get(normalized_code) or {}
    return (
        diagnosis_category(watch_reason)
        or _clean_text(watch_reason.get("发布结论状态"))
        or "not_selected_after_local_shadow"
    )


def _build_final_selection_substitution_explainability(
    candidate_df: pd.DataFrame,
    *,
    tracker_features_df: pd.DataFrame,
    added_codes: set[str],
    removed_codes: set[str],
) -> list[dict[str, Any]]:
    focus_codes = {code for code in added_codes | removed_codes if code}
    if candidate_df.empty or not focus_codes:
        return []

    current = candidate_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    current = current[current["基金代码"].isin(focus_codes)].copy()
    current = current.drop_duplicates(subset=["基金代码"], keep="first")
    if current.empty:
        return []

    tracker_current = tracker_features_df.copy()
    tracker_current["基金代码"] = tracker_current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    tracker_current = tracker_current.drop_duplicates(subset=["基金代码"], keep="first")
    merge_columns = [column for column in tracker_current.columns if column != "基金代码"]
    if merge_columns:
        current = current.merge(
            tracker_current[["基金代码", *merge_columns]],
            on="基金代码",
            how="left",
        )

    for column_name in ["出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]:
        if column_name not in current.columns:
            current[column_name] = pd.NA

    current["正式投资评分"] = pd.to_numeric(current.get("正式投资评分"), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["出现次数"] = pd.to_numeric(current.get("出现次数"), errors="coerce").fillna(1.0)
    current["出现频率(%)"] = pd.to_numeric(current.get("出现频率(%)"), errors="coerce").fillna(100.0)
    current["跟踪总期数"] = pd.to_numeric(current.get("跟踪总期数"), errors="coerce").fillna(1.0)

    confidence_bonus_map = {
        "高信心": FINAL_SELECTION_HIGH_CONFIDENCE_BONUS,
        "中信心": FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS,
        "边界观察": -FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY,
    }
    current["_final_confidence_bonus"] = (
        current.get("项目质量信心", pd.Series("", index=current.index, dtype=object))
        .astype(str)
        .map(confidence_bonus_map)
        .fillna(0.0)
    )
    current["_final_new_high_bonus"] = (
        (
            current["出现次数"].le(2)
            & current["正式投资评分"].ge(85.0)
        ).astype(float)
        * FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS
    )
    current["_final_new_low_penalty"] = (
        (
            current["出现次数"].le(2)
            & current["正式投资评分"].lt(75.0)
        ).astype(float)
        * FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY
    )
    current["_final_history_occurrence_bonus"] = (
        current["出现次数"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT
    )
    current["_final_history_frequency_bonus"] = (
        current["出现频率(%)"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT
    )
    current["_final_stability_score"] = (
        current["正式投资评分"].fillna(0.0)
        + current["_final_confidence_bonus"]
        + current["_final_history_occurrence_bonus"]
        + current["_final_history_frequency_bonus"]
        + current["_final_new_high_bonus"]
        - current["_final_new_low_penalty"]
    )
    current["_substitution_role"] = current["基金代码"].map(
        lambda code: "shadow_added" if code in added_codes else "shadow_removed"
    )
    current = current.sort_values(
        by=["_substitution_role", "_final_stability_score", "正式投资评分", "综合加权得分", "基金代码"],
        ascending=[True, False, False, False, True],
        kind="mergesort",
    )
    preview_columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "债基风格标签",
        "_substitution_role",
        "正式投资评分",
        "综合加权得分",
        "1年-年化收益(%)",
        "项目质量信心",
        "出现次数",
        "出现频率(%)",
        "_final_confidence_bonus",
        "_final_history_occurrence_bonus",
        "_final_history_frequency_bonus",
        "_final_new_high_bonus",
        "_final_new_low_penalty",
        "_final_stability_score",
    ]
    return (
        current[[column for column in preview_columns if column in current.columns]]
        .fillna("")
        .to_dict(orient="records")
    )


def _build_code_delta_summary(
    baseline_codes: set[str],
    current_codes: set[str],
) -> dict[str, Any]:
    added_codes = sorted(current_codes - baseline_codes)
    removed_codes = sorted(baseline_codes - current_codes)
    return {
        "aligned": not added_codes and not removed_codes,
        "added_count": int(len(added_codes)),
        "removed_count": int(len(removed_codes)),
        "net_count_delta": int(len(current_codes) - len(baseline_codes)),
        "added_codes": added_codes,
        "removed_codes": removed_codes,
    }


def _build_narrow_relief_scenarios(
    direct_relieved_df: pd.DataFrame,
    portfolio_hits_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, str]]]]:
    if direct_relieved_df.empty:
        return [], {}

    current = direct_relieved_df.copy()
    current["综合加权得分_num"] = _safe_numeric_series(current, "综合加权得分")
    current["正式投资评分_num"] = _safe_numeric_series(current, "正式投资评分")
    current["primary_share_bool"] = _safe_bool_series(current, "primary_share")
    portfolio_hit_codes = {
        _normalize_code(code)
        for code in portfolio_hits_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    current["portfolio_hit"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(
        lambda code: _normalize_code(code) in portfolio_hit_codes
    )

    scenarios: list[tuple[str, str, pd.Series]] = [
        (
            "score_ge_80",
            "综合加权得分>=80",
            current["综合加权得分_num"].ge(80.0),
        ),
        (
            "score_ge_80_strict_match",
            "综合加权得分>=80 且 AutoStock严格匹配",
            current["综合加权得分_num"].ge(80.0)
            & current.get("AutoStock原始校验状态", pd.Series(dtype=object)).astype(str).eq("strict_match"),
        ),
        (
            "score_ge_80_strict_match_high_confidence",
            "综合加权得分>=80 且 AutoStock严格匹配 且高信心",
            current["综合加权得分_num"].ge(80.0)
            & current.get("AutoStock原始校验状态", pd.Series(dtype=object)).astype(str).eq("strict_match")
            & current.get("项目质量信心", pd.Series(dtype=object)).astype(str).eq("高信心"),
        ),
        (
            "score_ge_80_strict_match_high_confidence_long_term_strong",
            "综合加权得分>=80 且 AutoStock严格匹配 且高信心 且长期稳定性强",
            current["综合加权得分_num"].ge(80.0)
            & current.get("AutoStock原始校验状态", pd.Series(dtype=object)).astype(str).eq("strict_match")
            & current.get("项目质量信心", pd.Series(dtype=object)).astype(str).eq("高信心")
            & current.get("长期稳定性标签", pd.Series(dtype=object)).astype(str).eq("长期稳定性强"),
        ),
        (
            "stable_primary_score_ge_75_strict_match_long_term_strong",
            "纯债稳健榜 且 primary share 且综合加权得分>=75 且 AutoStock严格匹配 且长期稳定性强",
            current.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object)).astype(str).eq("纯债稳健榜")
            & current["primary_share_bool"]
            & current["综合加权得分_num"].ge(75.0)
            & current.get("AutoStock原始校验状态", pd.Series(dtype=object)).astype(str).eq("strict_match")
            & current.get("长期稳定性标签", pd.Series(dtype=object)).astype(str).eq("长期稳定性强"),
        ),
        (
            "stable_primary_score_ge_80_strict_match_long_term_strong",
            "纯债稳健榜 且 primary share 且综合加权得分>=80 且 AutoStock严格匹配 且长期稳定性强",
            current.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object)).astype(str).eq("纯债稳健榜")
            & current["primary_share_bool"]
            & current["综合加权得分_num"].ge(80.0)
            & current.get("AutoStock原始校验状态", pd.Series(dtype=object)).astype(str).eq("strict_match")
            & current.get("长期稳定性标签", pd.Series(dtype=object)).astype(str).eq("长期稳定性强"),
        ),
    ]

    preview_columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "债基风格标签",
        "综合加权得分",
        "正式投资评分",
        "1年-年化收益(%)",
        "项目质量信心",
        "AutoStock原始校验状态",
        "长期稳定性标签",
    ]
    scenario_summary: list[dict[str, Any]] = []
    scenario_previews: dict[str, list[dict[str, str]]] = {}
    for scenario_key, label, mask in scenarios:
        subset = current[mask].copy()
        scenario_summary.append(
            {
                "scenario_key": scenario_key,
                "label": label,
                "count": int(len(subset.index)),
                "portfolio_hit_count": int(subset["portfolio_hit"].sum()),
                "group_counts": _series_count_map(subset.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object))),
                "style_counts": _series_count_map(subset.get("债基风格标签", pd.Series(dtype=object))),
            }
        )
        if subset.empty:
            scenario_previews[scenario_key] = []
            continue
        available_preview_columns = [column for column in preview_columns if column in subset.columns]
        scenario_previews[scenario_key] = (
            subset.sort_values(
                by=["综合加权得分_num", "正式投资评分_num", "基金代码"],
                ascending=[False, False, True],
                kind="mergesort",
            )[available_preview_columns]
            .fillna("")
            .astype(str)
            .head(10)
            .to_dict(orient="records")
        )
    return scenario_summary, scenario_previews


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_无数据_"
    view = df[[column for column in columns if column in df.columns]].fillna("").astype(str)
    header = "| " + " | ".join(view.columns.tolist()) + " |"
    separator = "| " + " | ".join(["---"] * len(view.columns.tolist())) + " |"
    rows = ["| " + " | ".join(row) + " |" for row in view.values.tolist()]
    return "\n".join([header, separator, *rows])
def _safe_supplement_official_autostock_statuses(
    df: pd.DataFrame,
    *,
    settings,
) -> tuple[pd.DataFrame, bool]:
    try:
        return _supplement_official_autostock_statuses(df, settings=settings), True
    except ModuleNotFoundError:
        return df.copy(), False


def _apply_holding_period_baseline_gate(df: pd.DataFrame, threshold_days: int) -> pd.DataFrame:
    current = df.copy()
    blocked_mask = (
        pd.to_numeric(current.get("has_holding_period"), errors="coerce").fillna(0).astype(int).eq(1)
        & pd.to_numeric(current.get("holding_days"), errors="coerce").fillna(0).astype(int).gt(threshold_days)
    )
    return current[~blocked_mask].copy()


def _build_shadow_result(
    settings,
    *,
    universe_df: pd.DataFrame,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    autostock_status_map: dict[str, dict[str, str]],
    frozen_context: dict[str, Any] | None = None,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, dict[str, float]],
    dict[str, float],
    pd.DataFrame,
    pd.DataFrame,
]:
    candidate_df, annual_only_df, style_thresholds, global_thresholds = _prepare_shadow_candidate_df(
        settings,
        universe_df=universe_df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        **({"evaluated_at": frozen_context["evaluation_times"]["decision"]} if frozen_context is not None else {}),
    )
    (
        official_df,
        internal_watch_df,
        _autostock_runtime_available,
        publish_official_df,
        final_selection_tracker_features_df,
    ) = _materialize_shadow_publish_result(
        settings,
        candidate_df=candidate_df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
        autostock_status_map=autostock_status_map,
        relief_codes=None,
        supplement_runtime_autostock=frozen_context is None,
        **({"frozen_context": frozen_context} if frozen_context is not None else {}),
    )
    return (
        annual_only_df,
        official_df,
        internal_watch_df,
        candidate_df,
        style_thresholds,
        global_thresholds,
        publish_official_df,
        final_selection_tracker_features_df,
    )


def _write_markdown(
    *,
    summary: dict[str, Any],
    direct_relieved_df: pd.DataFrame,
    portfolio_hits_df: pd.DataFrame,
    output_path: Path,
) -> None:
    if summary.get("schema_version") == 2:
        display_count = lambda value: "未完成计算" if value is None else str(value)
        baseline = summary.get("baseline_verification") or {}
        provider = str(summary.get("validation_provider") or "current_validation_provider")
        lines = ["# 年化收益门槛冻结审计", "", f"- run：`{summary.get('run_id')}`",
                 f"- 审计状态：`{summary.get('status')}`",
                 f"- 基线核验：`{baseline.get('status')}`",
                 f"- 原正式榜：{display_count(summary.get('baseline_final_count'))}",
                 f"- 全部单因子年化不足放宽情景：`{summary.get('scenario_status')}`",
                 f"- 情景完整成员：{display_count(summary.get('shadow_final_count'))}",
                 f"- 净增：{display_count(summary.get('net_final_count_delta'))}",
                 "", f"仅回放已核验的报告选择层。当前校验来源为 `{provider}`；完整候选、配置、门禁时间、Tracker 和当前校验证据均使用冻结输入；"
                 "缺失时停止相应情景，名单数保持缺失。生产持有期、费用、风险及其他门槛未修改。"]
        missing = summary.get("missing_inputs") or {}
        if missing:
            lines.extend(["", f"缺失阶段：`{missing['stage']}`；基金：" + "、".join(missing["codes"])])
        for item in summary.get("local_style_band_shadow_scenarios") or []:
            lines.extend(["", f"- {item.get('label')}：{item.get('status', 'computed')}；"
                          f"净增{display_count(item.get('net_final_count_delta'))}。"])
            if item.get("missing_inputs"):
                lines.append("  缺失基金：" + "、".join(item["missing_inputs"]["codes"]))
        if baseline.get("time_reference_source") == "legacy_snapshot_generated_at":
            lines.extend(["", "旧快照使用原生成时间，各门禁精确执行时刻未记录。"])
        lines.extend(["", "本入口不读取当前数据库、重新请求上游或计算未来收益。前瞻效果须独立采集并冻结观察期净值；"
                      "单轮成员差异和横截面评分不能证明筛选策略优劣。"])
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return
    decision_summary = summary.get("decision_summary") or {}
    lines = [
        "# 年化收益门槛影子审计",
        "",
        f"- 生成时间：`{summary['generated_at']}`",
        f"- run_id：`{summary['run_id']}`",
        "",
        "## 核心结论",
        "",
        f"- 当前正式主榜：`{summary['baseline_final_count']}`",
        f"- 放开动态层“单因子年化不足”后的影子主榜：`{summary['shadow_final_count']}`",
        f"- 影子主榜净增：`{summary['net_final_count_delta']}`",
        f"- 影子主榜新增代码数：`{summary['shadow_added_count']}`",
        f"- 原正式榜被影子链挤出的代码数：`{summary['shadow_removed_count']}`",
        f"- 直接被放宽救回并进入影子主榜的基金：`{summary['direct_relieved_final_count']}`",
        f"- 单因子年化不足但最终未进影子主榜：`{summary['annual_only_not_in_shadow_final_count']}`",
        f"- 单因子年化不足候选总数：`{summary['annual_only_candidate_count']}`",
        f"- 其中 `综合加权得分 >= 80`：`{summary['annual_only_score_ge_80_count']}`",
        f"- 其中 `综合加权得分 >= 85`：`{summary['annual_only_score_ge_85_count']}`",
        f"- 其中 `纯债稳健榜 + primary share + 综合加权得分 >= 80`：`{summary['stable_primary_score_ge_80_count']}`",
        f"- 其中真正冲进前端 50 只优先池的基金：`{summary['portfolio_direct_relieved_count']}`",
        f"- 审计建议动作：`{decision_summary.get('recommended_action') or 'observe_only'}`",
        "",
        "## 风险判断",
        "",
        "- 如果放开“单因子年化不足”，影子主榜新增规模依然偏大，且伴随少量原正式榜样本替换，说明这不是一条可直接进入主链的安全缓释口。",
        "- 当前高分年化单因子样本极少，且并未集中在纯债稳健 primary share，说明不存在明显的“高质量纯债被系统性错杀”证据。",
        "- 在缺少更窄、更可控条件之前，不建议直接放宽年化收益门槛。",
        f"- 影子链自动诊断分流：经理从业与评分诊断 `{'%s' % summary['manager_stability_watch_count']}`，自动发布门禁诊断 `{'%s' % summary['publish_post_review_watch_count']}`，最终精选未入围 `{'%s' % summary['final_selection_removed_count']}`。",
        (
            "- 当前环境未完整执行 AutoStock 轻量补校验，影子结果可用于方向性审计，但精确人数仍以 canonical 正式产物为准。"
            if not summary.get("autostock_runtime_available")
            else "- 当前环境已完成 AutoStock 轻量补校验，影子结果可直接用于方向性与人数级别参考。"
        ),
        (
            "- 当前 run 缺少可归档复放的核心输入文件，影子审计基于当前数据库近似重放，不能把基线差分直接视为旧 run 精确复盘。"
            if not bool(summary.get("exact_replay_inputs_available", True))
            else "- 当前 run 的核心输入文件可用于精确复放。"
        ),
        "",
        "## 重建基线一致性",
        "",
        "## 窄场景验证",
        "",
    ]
    reconstructed_baseline_vs_actual = summary.get("reconstructed_no_relief_baseline_vs_actual") or {}
    if reconstructed_baseline_vs_actual:
        lines.insert(
            -2,
            f"- 无放宽重建基线与真实正式榜一致：`{bool(reconstructed_baseline_vs_actual.get('aligned'))}`",
        )
        lines.insert(
            -2,
            f"- 相对真实正式榜，重建基线新增：`{reconstructed_baseline_vs_actual.get('added_count', 0)}`，缺失：`{reconstructed_baseline_vs_actual.get('removed_count', 0)}`",
        )
        if not bool(reconstructed_baseline_vs_actual.get("aligned")):
            lines.insert(
                -2,
                "- 当前局部影子复核中的最终精选替代现象，需先扣除重建基线漂移，不能直接视为局部放宽造成的新增副作用。",
            )
    else:
        lines.insert(-2, "- `_无重建基线一致性信息_`")
    lines.insert(-2, "")
    narrow_scenarios = summary.get("narrow_relief_scenarios") or []
    if narrow_scenarios:
        for item in narrow_scenarios:
            lines.append(
                f"- `{item.get('label', '')}`：候选 `{item.get('count', 0)}`，优先池命中 `{item.get('portfolio_hit_count', 0)}`"
            )
    else:
        lines.append("- `_无窄场景结果_`")
    lines.extend(["", "## 局部风格/得分带影子复核", ""])
    local_shadow_scenarios = summary.get("local_style_band_shadow_scenarios") or []
    if local_shadow_scenarios:
        for item in local_shadow_scenarios:
            forward_validation = item.get("forward_validation") or {}
            lines.append(
                f"- `{item.get('label', '')}`：局部候选 `{item.get('candidate_count', 0)}`，"
                f"局部放宽后新增 `{item.get('direct_relieved_final_count', 0)}`，"
                f"影子主榜净变化 `{item.get('net_final_count_delta', 0)}`，"
                f"前瞻状态 `{forward_validation.get('status', 'not_available')}`"
            )
    else:
        lines.append("- `_无局部风格/得分带影子结果_`")
    local_shadow_judgement = summary.get("local_style_band_shadow_judgement") or {}
    if local_shadow_judgement:
        lines.extend(
            [
                "",
                "### 局部影子判断",
                "",
                f"- 推荐动作：`{local_shadow_judgement.get('recommended_action', 'observe_only')}`",
                f"- 理由：{local_shadow_judgement.get('reason', '')}",
            ]
        )
    local_forward_validation_judgement = summary.get("local_style_band_forward_validation_judgement") or {}
    if local_forward_validation_judgement:
        lines.extend(
            [
                "",
                "### 局部前瞻验证判断",
                "",
                f"- 状态：`{local_forward_validation_judgement.get('status', 'not_available')}`",
                f"- 推荐动作：`{local_forward_validation_judgement.get('recommended_action', 'observe_only')}`",
                f"- 支持立即阈值变更：`{bool(local_forward_validation_judgement.get('threshold_change_supported_now'))}`",
                f"- 理由：{local_forward_validation_judgement.get('reason', '')}",
            ]
        )
    replacement_quality_review = summary.get("replacement_quality_review") or {}
    replacement_quality_judgement = replacement_quality_review.get("judgement") or {}
    if replacement_quality_review:
        lines.extend(
            [
                "",
                "## 替换质量复核",
                "",
                f"- 新增样本：`{replacement_quality_review.get('added_count', 0)}`，剔除样本：`{replacement_quality_review.get('removed_count', 0)}`",
                f"- 推荐动作：`{replacement_quality_judgement.get('recommended_action', 'review_replacement_quality_before_any_threshold_change')}`",
                f"- 理由：{replacement_quality_judgement.get('reason', '')}",
            ]
        )
        for item in list(replacement_quality_review.get("comparison_deltas") or [])[:5]:
            lines.append(
                f"- `{item.get('source_column', item.get('metric_key', ''))}` 中位数差异（新增-剔除）：`{item.get('median_delta_added_minus_removed')}`"
            )
    lines.extend(
        [
            "",
            "## 直接救回样本",
            "",
            _markdown_table(
                direct_relieved_df,
                [
                    "基金代码",
                    "基金名称",
                    FORMAL_GROUP_LABEL,
                    "债基风格标签",
                    "综合加权得分",
                    "1年-年化收益(%)",
                    "1年-夏普比率",
                    "1年-最大回撤(%)",
                    "1年-波动率",
                    "费率合计(%)",
                    "长期稳定性标签",
                ],
            ),
            "",
            "## 优先池命中样本",
            "",
            _markdown_table(
                portfolio_hits_df,
                [
                    "组合名称",
                    "基金代码",
                    "基金名称",
                    FORMAL_GROUP_LABEL,
                    "债基风格标签",
                    "投资优先池得分",
                    "入选原因",
                    "风险提示",
                ],
            ),
        ]
    )
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _attach_decision_summary(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    payload["decision_summary"] = _build_annual_return_gate_shadow_decision_summary(payload)
    return payload


def _build_frozen_annual_summary(
    payload: dict[str, Any], baseline: dict[str, Any], context: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    """Use only the context just verified by the full production baseline."""
    if baseline.get("baseline_verified") is not True:
        raise ValueError("annual_shadow_requires_verified_baseline")
    settings = context["settings"]
    run_id = str(payload["run_id"])
    frames = context["frames"]
    frozen = context["snapshot"]
    decision_map = frozen["inputs"]["decision_map"]
    soft_warning_map = frozen["inputs"]["soft_warning_map"]
    autostock_status_map = context["autostock_status_map"]
    actual_official_df = frames["formal_after_all_filters"].copy()
    actual_publish_gate_df = frames["post_review"].copy()
    candidate_universe_df = _production._assign_project_scores(frames["scored_universe_before_holding_gate"])
    replay_precision = {"status": "report_baseline_verified", "baseline_reproduction_verified": True,
                        "baseline_verification": baseline, "exact_replay_inputs_available": True,
                        "upstream_collection_and_core_risk_recalculation_verified": False}
    exact_replay_inputs_available = True
    publish_gate_exact_replay_available = True

    threshold_days = int(settings.exclude_holding_days_gt)
    baseline_universe_df = _apply_holding_period_baseline_gate(candidate_universe_df, threshold_days)
    snapshot_date = str(frozen["snapshot_date"])

    (
        annual_only_df,
        shadow_official_df,
        shadow_internal_watch_df,
        candidate_df,
        style_thresholds,
        global_thresholds,
        shadow_publish_official_df,
        shadow_final_selection_tracker_features_df,
    ) = _build_shadow_result(
        settings,
        universe_df=baseline_universe_df,
        run_id=run_id,
        generated_at=str(payload.get("generated_at") or ""),
        snapshot_date=snapshot_date,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        autostock_status_map=autostock_status_map,
        frozen_context=context,
    )
    # The verifier has already recomputed and compared scores, reasons and every
    # member. Do not run a second, live-data baseline behind that verified label.
    reconstructed_baseline_official_df = context["result"]["official"].copy()
    reconstructed_baseline_publish_official_df = context["result"]["post_review"].copy()
    reconstructed_baseline_final_selection_tracker_features_df = frames["final_selection_tracker_features"].copy()

    actual_official_df["基金代码"] = actual_official_df["基金代码"].map(_normalize_code)
    reconstructed_baseline_official_df["基金代码"] = reconstructed_baseline_official_df["基金代码"].map(_normalize_code)
    shadow_official_df["基金代码"] = shadow_official_df["基金代码"].map(_normalize_code)
    annual_only_df["基金代码"] = annual_only_df["基金代码"].map(_normalize_code)

    actual_codes = {
        code
        for code in actual_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    reconstructed_baseline_codes = {
        code
        for code in reconstructed_baseline_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    reconstructed_publish_gate_codes = {
        code
        for code in reconstructed_baseline_publish_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    shadow_codes = {
        code
        for code in shadow_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    actual_publish_gate_codes = {
        code
        for code in actual_publish_gate_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    annual_only_codes = {
        code
        for code in annual_only_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if code
    }
    shadow_added_codes = sorted(shadow_codes - actual_codes)
    shadow_removed_codes = sorted(actual_codes - shadow_codes)
    direct_relieved_final_codes = sorted((shadow_codes - actual_codes) & annual_only_codes)
    reconstructed_baseline_vs_actual_delta = _build_code_delta_summary(actual_codes, reconstructed_baseline_codes)
    reconstructed_publish_gate_vs_actual_delta = (
        _build_code_delta_summary(actual_publish_gate_codes, reconstructed_publish_gate_codes)
        if publish_gate_exact_replay_available
        else {
            "aligned": False,
            "added_count": 0,
            "removed_count": 0,
            "net_count_delta": 0,
            "added_codes": [],
            "removed_codes": [],
            "reason": "publish_chain_snapshot_json_missing_or_empty",
        }
    )
    shadow_vs_reconstructed_baseline_delta = _build_code_delta_summary(reconstructed_baseline_codes, shadow_codes)

    annual_only_df["综合加权得分"] = pd.to_numeric(annual_only_df.get("综合加权得分"), errors="coerce")
    annual_only_df["1年-年化收益(%)"] = pd.to_numeric(annual_only_df.get("1年-年化收益(%)"), errors="coerce")
    annual_only_df["关联份额数_num"] = pd.to_numeric(annual_only_df.get("关联份额数"), errors="coerce")
    annual_only_df["primary_share"] = annual_only_df["关联份额数_num"].fillna(1).le(1)
    annual_only_df["score_band"] = annual_only_df["综合加权得分"].map(_score_band)
    annual_only_df["annual_band"] = annual_only_df["1年-年化收益(%)"].map(_annual_band)

    direct_relieved_df = annual_only_df[
        annual_only_df["基金代码"].isin(direct_relieved_final_codes)
    ].copy().sort_values(
        by=["综合加权得分", "基金代码"],
        ascending=[False, True],
        kind="mergesort",
    )

    # Preserve the scenario's own observed basis for entrants and its current
    # snapshot identity; original-only rows do not cover a changed candidate set.
    tracker_features_df = shadow_final_selection_tracker_features_df.loc[
        shadow_final_selection_tracker_features_df["基金代码"].map(_normalize_code).isin(shadow_codes)
    ].copy()
    portfolio_df, _ = _build_portfolio_suggestions(
        shadow_official_df.copy(),
        tracker_features_df=tracker_features_df,
    )
    portfolio_df["基金代码"] = portfolio_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    portfolio_hits_df = portfolio_df[
        portfolio_df["基金代码"].isin(direct_relieved_final_codes)
    ].copy().sort_values(
        by=["组合名称", "基金代码"],
        ascending=[True, True],
        kind="mergesort",
    )
    if not direct_relieved_df.empty:
        enrich_columns = [
            column
            for column in [
                "基金代码",
                "项目质量信心",
                "AutoStock原始校验状态",
                "正式投资评分",
            ]
            if column in shadow_official_df.columns
        ]
        if enrich_columns:
            direct_relieved_df = direct_relieved_df.merge(
                shadow_official_df[enrich_columns].drop_duplicates(subset=["基金代码"], keep="first"),
                on="基金代码",
                how="left",
            )
    replacement_quality_review = _build_replacement_quality_review(
        added_source_df=shadow_official_df.copy(),
        removed_source_df=(
            reconstructed_baseline_official_df.copy()
            if bool(reconstructed_baseline_vs_actual_delta.get("aligned"))
            else actual_official_df.copy()
        ),
        added_codes=set(shadow_added_codes),
        removed_codes=set(shadow_removed_codes),
        added_tracker_features_df=shadow_final_selection_tracker_features_df.copy(),
        removed_tracker_features_df=reconstructed_baseline_final_selection_tracker_features_df.copy(),
        reconstructed_baseline_aligned=bool(reconstructed_baseline_vs_actual_delta.get("aligned")),
        added_source_label="shadow_official_df",
        removed_source_label=(
            "reconstructed_baseline_official_df"
            if bool(reconstructed_baseline_vs_actual_delta.get("aligned"))
            else "actual_official_df"
        ),
    )
    narrow_relief_scenarios, narrow_relief_previews = _build_narrow_relief_scenarios(
        direct_relieved_df,
        portfolio_hits_df,
    )
    coupling_audit = (
        (((payload.get("project_publish_summary") or {}).get("standard_screening_audit") or {}).get(
            "annual_return_score_sharpe_coupling"
        ) or {})
    )
    local_shadow_scenarios: list[dict[str, Any]] = []
    local_shadow_previews: dict[str, list[dict[str, str]]] = {}
    local_shadow_specs = _build_local_style_band_shadow_scenario_specs(coupling_audit)
    for scenario_spec in local_shadow_specs:
        local_candidate_df = _select_local_shadow_relief_candidates(
            annual_only_df,
            style_thresholds=style_thresholds,
            global_thresholds=global_thresholds,
            style_tag=str(scenario_spec.get("style_tag") or ""),
            score_band=str(scenario_spec.get("score_band") or ""),
            annual_gap_gte=scenario_spec.get("annual_gap_gte"),
        )
        local_codes = {
            _normalize_code(code)
            for code in local_candidate_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
            if _normalize_code(code)
        }
        if not local_codes:
            local_shadow_scenarios.append(
                {
                    "scenario_key": scenario_spec.get("scenario_key"),
                    "label": scenario_spec.get("label"),
                    "style_tag": scenario_spec.get("style_tag"),
                    "score_band": scenario_spec.get("score_band"),
                    "annual_gap_gte": scenario_spec.get("annual_gap_gte"),
                    "candidate_count": 0,
                    "direct_relieved_final_count": 0,
                    "shadow_added_count": 0,
                    "shadow_removed_count": 0,
                    "net_final_count_delta": 0,
                    "portfolio_direct_relieved_count": 0,
                }
            )
            local_shadow_previews[str(scenario_spec.get("scenario_key") or "")] = []
            continue
        try:
            (
                local_shadow_official_df, _local_shadow_internal_watch_df, _local_autostock_available,
                local_publish_official_df, local_final_selection_tracker_features_df,
            ) = _materialize_shadow_publish_result(
                settings, candidate_df=candidate_df, style_thresholds=style_thresholds,
                global_thresholds=global_thresholds, run_id=run_id,
                generated_at=str(payload.get("generated_at") or ""), snapshot_date=snapshot_date,
                autostock_status_map=autostock_status_map, relief_codes=local_codes,
                supplement_runtime_autostock=False, frozen_context=context,
            )
        except FrozenAnnualScenarioIncomplete as exc:
            local_shadow_scenarios.append({
                **scenario_spec, "status": "inputs_incomplete", "candidate_count": len(local_codes),
                "missing_inputs": {"stage": exc.status, "codes": exc.missing_codes},
                "direct_relieved_final_count": None, "shadow_added_count": None, "shadow_removed_count": None,
                "net_final_count_delta": None, "portfolio_direct_relieved_count": None,
                "selection_side_effect_only": None,
            })
            continue
        local_shadow_official_df["基金代码"] = local_shadow_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
        local_shadow_codes = {
            code
            for code in local_shadow_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
            if code
        }
        reconstructed_local_delta = _build_code_delta_summary(reconstructed_baseline_codes, local_shadow_codes)
        local_internal_watch_df = _local_shadow_internal_watch_df.copy()
        local_internal_watch_df["基金代码"] = local_internal_watch_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
        local_watch_reason_map = {
            _normalize_code(row.get("基金代码")): {
                AUTO_DIAGNOSIS_CATEGORY_COLUMN: diagnosis_category(row),
                "发布结论状态": _clean_text(row.get("发布结论状态")),
            }
            for row in local_internal_watch_df.to_dict(orient="records")
            if _normalize_code(row.get("基金代码"))
        }
        local_shadow_added_codes = sorted(local_shadow_codes - actual_codes)
        local_shadow_removed_codes = sorted(actual_codes - local_shadow_codes)
        local_added_vs_reconstructed_baseline_codes = sorted(local_shadow_codes - reconstructed_baseline_codes)
        local_removed_vs_reconstructed_baseline_codes = sorted(reconstructed_baseline_codes - local_shadow_codes)
        local_direct_relieved_codes = sorted((local_shadow_codes - reconstructed_baseline_codes) & local_codes)
        local_already_in_actual_codes = sorted(local_shadow_codes & actual_codes & local_codes)
        local_direct_relieved_code_set = set(local_direct_relieved_codes)
        local_already_in_actual_code_set = set(local_already_in_actual_codes)
        local_direct_relieved_df = local_candidate_df[
            local_candidate_df["基金代码"].isin(local_direct_relieved_codes)
        ].copy()
        local_direct_relieved_df["项目质量信心"] = local_direct_relieved_df.get("项目质量信心", pd.Series(dtype=object)).fillna("")
        local_direct_relieved_df["AutoStock原始校验状态"] = local_direct_relieved_df.get("AutoStock原始校验状态", pd.Series(dtype=object)).fillna("")
        local_direct_relieved_df["annual_gap_pct"] = pd.to_numeric(local_direct_relieved_df.get("annual_gap_pct"), errors="coerce")
        local_direct_relieved_df["weighted_margin"] = pd.to_numeric(local_direct_relieved_df.get("weighted_margin"), errors="coerce")
        local_direct_relieved_df["short_margin"] = pd.to_numeric(local_direct_relieved_df.get("short_margin"), errors="coerce")
        local_direct_relieved_df["long_margin"] = pd.to_numeric(local_direct_relieved_df.get("long_margin"), errors="coerce")
        local_tracker_features_df = local_final_selection_tracker_features_df.loc[
            local_final_selection_tracker_features_df["基金代码"].map(_normalize_code).isin(local_shadow_codes)
        ].copy()
        local_portfolio_df, _ = _build_portfolio_suggestions(
            local_shadow_official_df.copy(),
            tracker_features_df=local_tracker_features_df,
        )
        local_portfolio_df["基金代码"] = local_portfolio_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
        local_portfolio_hits_df = local_portfolio_df[
            local_portfolio_df["基金代码"].isin(local_direct_relieved_codes)
        ].copy()
        local_substitution_explainability = _build_final_selection_substitution_explainability(
            local_publish_official_df.copy(),
            tracker_features_df=local_final_selection_tracker_features_df,
            added_codes=set(local_added_vs_reconstructed_baseline_codes),
            removed_codes=set(local_removed_vs_reconstructed_baseline_codes),
        )
        local_replacement_quality_review = _build_local_shadow_replacement_quality_review(
            local_shadow_official_df=local_shadow_official_df.copy(),
            reconstructed_baseline_official_df=reconstructed_baseline_official_df.copy(),
            local_added_codes=set(local_added_vs_reconstructed_baseline_codes),
            local_removed_codes=set(local_removed_vs_reconstructed_baseline_codes),
            local_final_selection_tracker_features_df=local_final_selection_tracker_features_df.copy(),
            reconstructed_baseline_final_selection_tracker_features_df=(
                reconstructed_baseline_final_selection_tracker_features_df.copy()
            ),
            reconstructed_baseline_aligned=bool(reconstructed_baseline_vs_actual_delta.get("aligned")),
        )
        local_replacement_quality_signal = (
            local_replacement_quality_review.get("quality_signal") or {}
        )
        local_forward_validation = {
            "available": False, "status": "not_evaluated", "ready_window_count": None,
            "window_count": len(FORWARD_VALIDATION_WINDOWS),
            "recommended_action": "collect_separate_forward_nav_evidence_after_frozen_selection",
            "threshold_change_supported_now": False,
            "reason": "本入口仅回放报告选择；前瞻表现须另用标明观察截止日期的真实净值证据核验。",
        }
        local_final_selection_removed_count = int(
            local_internal_watch_df[
                local_internal_watch_df["基金代码"].isin(local_codes)
                & diagnosis_categories(local_internal_watch_df, required=True).eq(FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS)
            ].shape[0]
        )
        selection_side_effect_only = bool(
            int(reconstructed_local_delta.get("added_count") or 0) > 0
            and int(len(local_direct_relieved_codes)) == 0
        )
        local_shadow_scenarios.append(
            {
                "scenario_key": scenario_spec.get("scenario_key"),
                "label": scenario_spec.get("label"),
                "style_tag": scenario_spec.get("style_tag"),
                "score_band": scenario_spec.get("score_band"),
                "annual_gap_gte": scenario_spec.get("annual_gap_gte"),
                "count_basis": "runtime_dynamic_threshold_annual_only_candidates",
                "candidate_count": int(len(local_candidate_df.index)),
                "final_selected_total": int(len(local_shadow_codes & local_codes)),
                "direct_relieved_final_count": int(len(local_direct_relieved_codes)),
                "already_in_actual_final_count": int(len(local_already_in_actual_codes)),
                "shadow_added_count": int(len(local_shadow_added_codes)),
                "shadow_removed_count": int(len(local_shadow_removed_codes)),
                "net_final_count_delta": int(len(local_shadow_official_df.index) - len(actual_official_df.index)),
                "reconstructed_baseline_added_count_vs_actual": int(reconstructed_baseline_vs_actual_delta.get("added_count") or 0),
                "reconstructed_baseline_removed_count_vs_actual": int(reconstructed_baseline_vs_actual_delta.get("removed_count") or 0),
                "reconstructed_local_added_count_vs_reconstructed_baseline": int(reconstructed_local_delta.get("added_count") or 0),
                "reconstructed_local_removed_count_vs_reconstructed_baseline": int(reconstructed_local_delta.get("removed_count") or 0),
                "reconstructed_local_net_count_delta_vs_reconstructed_baseline": int(reconstructed_local_delta.get("net_count_delta") or 0),
                "portfolio_direct_relieved_count": int(len(local_portfolio_hits_df.index)),
                "final_selection_removed_count": local_final_selection_removed_count,
                "selection_side_effect_only": selection_side_effect_only,
                "candidate_style_counts": _series_count_map(local_candidate_df.get("债基风格标签", pd.Series(dtype=object))),
                "candidate_group_counts": _series_count_map(local_candidate_df.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object))),
                "shadow_added_codes": local_shadow_added_codes,
                "shadow_removed_codes": local_shadow_removed_codes,
                "reconstructed_local_added_codes": local_added_vs_reconstructed_baseline_codes,
                "reconstructed_local_removed_codes": local_removed_vs_reconstructed_baseline_codes,
                "direct_relieved_final_codes": local_direct_relieved_codes,
                "replacement_quality_signal": local_replacement_quality_signal,
                "replacement_quality_review": {
                    "available": bool(local_replacement_quality_review.get("available")),
                    "count_basis": local_replacement_quality_review.get("count_basis"),
                    "added_count": int(local_replacement_quality_review.get("added_count") or 0),
                    "removed_count": int(local_replacement_quality_review.get("removed_count") or 0),
                    "added_observed_count": int(
                        local_replacement_quality_review.get("added_observed_count") or 0
                    ),
                    "removed_observed_count": int(
                        local_replacement_quality_review.get("removed_observed_count") or 0
                    ),
                    "reconstructed_baseline_aligned": bool(
                        local_replacement_quality_review.get("reconstructed_baseline_aligned")
                    ),
                    "comparison_deltas": list(
                        local_replacement_quality_review.get("comparison_deltas") or []
                    )[:8],
                    "judgement": local_replacement_quality_review.get("judgement") or {},
                    "quality_signal": local_replacement_quality_signal,
                },
                "forward_validation": local_forward_validation,
                "final_selection_substitution_explainability": local_substitution_explainability[:8],
            }
        )
        local_preview_df = local_candidate_df.copy()
        local_preview_df["scenario_outcome"] = local_preview_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(
            lambda code: _resolve_local_shadow_scenario_outcome(
                code,
                direct_relieved_codes=local_direct_relieved_code_set,
                already_in_actual_codes=local_already_in_actual_code_set,
                watch_reason_map=local_watch_reason_map,
            )
        )
        preview_columns = [
            "基金代码",
            "基金名称",
            FORMAL_GROUP_LABEL,
            "债基风格标签",
            "综合加权得分",
            "近期得分",
            "长期得分",
            "1年-年化收益(%)",
            "annual_gap_pct",
            "weighted_margin",
            "short_margin",
            "long_margin",
            "scenario_outcome",
            "项目质量信心",
            "AutoStock原始校验状态",
        ]
        local_shadow_previews[str(scenario_spec.get("scenario_key") or "")] = (
            local_preview_df.sort_values(
                by=["综合加权得分", "annual_gap_pct", "基金代码"],
                ascending=[False, False, True],
                kind="mergesort",
            )[[column for column in preview_columns if column in local_preview_df.columns]]
            .fillna("")
            .astype(str)
            .head(10)
            .to_dict(orient="records")
        )

    direct_relief_supported_scenarios = [
        item for item in local_shadow_scenarios if int(item.get("direct_relieved_final_count") or 0) > 0
    ]
    side_effect_only_scenarios = [
        item for item in local_shadow_scenarios if bool(item.get("selection_side_effect_only"))
    ]
    incomplete_scenarios = [item for item in local_shadow_scenarios if item.get("status") == "inputs_incomplete"]
    local_forward_validation_judgement = {
        "available": False, "status": "not_evaluated", "ready_scenario_total": None,
        "eligible_scenario_total": None if incomplete_scenarios else len(direct_relief_supported_scenarios),
        "recommended_action": "collect_separate_forward_nav_evidence_after_frozen_selection",
        "threshold_change_supported_now": False,
        "reason": "本入口没有读取未来NAV，未验证窗口是否成熟；须独立采集前瞻证据，不能用横截面评分代替。",
    }
    if incomplete_scenarios:
        local_style_band_shadow_judgement = {
            "recommended_action": "collect_frozen_scenario_inputs_before_comparison",
            "direct_relief_supported_scenario_total": None,
            "selection_side_effect_only_scenario_total": None,
            "reason": "部分局部情景缺少历史证据，仅保留已完成的全量情景和逐阶段结果。",
        }
    elif direct_relief_supported_scenarios:
        local_style_band_shadow_judgement = {
            "recommended_action": "compare_local_shadow_scenarios_before_any_threshold_change",
            "direct_relief_supported_scenario_total": int(len(direct_relief_supported_scenarios)),
            "selection_side_effect_only_scenario_total": int(len(side_effect_only_scenarios)),
            "reason": "局部影子复核已出现直接救回样本，下一步应比较不同风格/得分带的净增质量，而不是直接进入生产阈值修改。",
        }
    elif not bool(reconstructed_baseline_vs_actual_delta.get("aligned")):
        local_style_band_shadow_judgement = {
            "recommended_action": "fix_reconstructed_baseline_alignment_before_interpreting_local_shadow_side_effects",
            "direct_relief_supported_scenario_total": 0,
            "selection_side_effect_only_scenario_total": int(len(side_effect_only_scenarios)),
            "reason": "当前无放宽重建基线已与真实正式榜不一致，局部影子场景中观测到的替代扰动不能直接归因于局部放宽；需先修正审计重建基线与真实正式榜的一致性。",
        }
    elif side_effect_only_scenarios:
        local_style_band_shadow_judgement = {
            "recommended_action": "do_not_relax_even_locally_until_final_selection_interaction_is_explained",
            "direct_relief_supported_scenario_total": 0,
            "selection_side_effect_only_scenario_total": int(len(side_effect_only_scenarios)),
            "reason": "当前局部放宽没有直接救回目标风格带样本，主要表现为最终精选侧的替代扰动；在解释清楚局部候选与最终精选的相互作用前，不建议进入局部阈值放宽。",
        }
    else:
        local_style_band_shadow_judgement = {
            "recommended_action": "observe_only",
            "direct_relief_supported_scenario_total": 0,
            "selection_side_effect_only_scenario_total": 0,
            "reason": "当前局部影子复核未显示出足够强的直接救回信号，继续观察。",
        }

    summary = {
        "schema_version": 2, "status": "computed_with_scenario_gaps" if incomplete_scenarios else "computed",
        "scenario_status": "computed", "passed": not incomplete_scenarios,
        "incomplete_local_scenario_count": len(incomplete_scenarios),
        "baseline_verification": baseline,
        "source_report": (payload.get("artifacts") or {}).get("report_json"),
        "source_publication_control": payload.get("publication_control"),
        "scope": "report_selection_from_frozen_complete_universe",
        "later_database_reads": False, "runtime_upstream_requests": 0,
        "wall_clock_used_for_replayed_gates": False,
        "production_thresholds_changed": False, "strategy_comparison_allowed": not incomplete_scenarios,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "baseline_final_count": int(len(actual_official_df.index)),
        "shadow_final_count": int(len(shadow_official_df.index)),
        "net_final_count_delta": int(len(shadow_official_df.index) - len(actual_official_df.index)),
        "shadow_added_count": int(len(shadow_added_codes)),
        "shadow_removed_count": int(len(shadow_removed_codes)),
        "annual_only_candidate_count": int(len(annual_only_df.index)),
        "direct_relieved_final_count": int(len(direct_relieved_final_codes)),
        "annual_only_not_in_shadow_final_count": int(len(annual_only_codes - shadow_codes)),
        "annual_only_already_in_actual_final_count": int(len(annual_only_codes & actual_codes)),
        "annual_only_score_ge_80_count": int(annual_only_df["综合加权得分"].ge(80).sum()),
        "annual_only_score_ge_85_count": int(annual_only_df["综合加权得分"].ge(85).sum()),
        "stable_primary_score_ge_80_count": int(
            (
                annual_only_df[FORMAL_GROUP_LABEL].eq("纯债稳健榜")
                & annual_only_df["primary_share"]
                & annual_only_df["综合加权得分"].ge(80)
            ).sum()
        ),
        "stable_primary_score_ge_85_count": int(
            (
                annual_only_df[FORMAL_GROUP_LABEL].eq("纯债稳健榜")
                & annual_only_df["primary_share"]
                & annual_only_df["综合加权得分"].ge(85)
            ).sum()
        ),
        "portfolio_direct_relieved_count": int(len(portfolio_hits_df.index)),
        "portfolio_direct_relieved_codes": portfolio_hits_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist(),
        "autostock_runtime_available": bool(
            pd.Series(shadow_official_df.get("autostock_runtime_available", pd.Series(dtype=object))).fillna(False).astype(bool).any()
        ),
        "replay_precision": replay_precision,
        "exact_replay_inputs_available": bool(exact_replay_inputs_available),
        "publish_gate_exact_replay_available": bool(publish_gate_exact_replay_available),
        **summarize_diagnoses(shadow_internal_watch_df),
        "annual_only_style_counts": _series_count_map(annual_only_df.get("债基风格标签", pd.Series(dtype=object))),
        "annual_only_group_counts": _series_count_map(annual_only_df.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object))),
        "annual_only_score_band_counts": _series_count_map(annual_only_df.get("score_band", pd.Series(dtype=object))),
        "annual_only_annual_band_counts": _series_count_map(annual_only_df.get("annual_band", pd.Series(dtype=object))),
        "shadow_added_codes": shadow_added_codes,
        "shadow_removed_codes": shadow_removed_codes,
        "direct_relieved_final_codes": direct_relieved_final_codes,
        "reconstructed_no_relief_baseline_count": int(len(reconstructed_baseline_codes)),
        "reconstructed_no_relief_baseline_vs_actual": reconstructed_baseline_vs_actual_delta,
        "reconstructed_no_relief_publish_gate_count": int(len(reconstructed_publish_gate_codes)),
        "reconstructed_no_relief_publish_gate_vs_actual": reconstructed_publish_gate_vs_actual_delta,
        "shadow_vs_reconstructed_no_relief_baseline": shadow_vs_reconstructed_baseline_delta,
        "narrow_relief_scenarios": narrow_relief_scenarios,
        "narrow_relief_previews": narrow_relief_previews,
        "replacement_quality_review": replacement_quality_review,
        "local_style_band_shadow_scenarios": local_shadow_scenarios,
        "local_style_band_shadow_previews": local_shadow_previews,
        "local_style_band_shadow_judgement": local_style_band_shadow_judgement,
        "local_style_band_forward_validation_judgement": local_forward_validation_judgement,
    }
    summary = _attach_decision_summary(summary)
    return summary, {"direct_relieved": direct_relieved_df, "portfolio_hits": portfolio_hits_df,
                     "scenario_members": shadow_official_df}


def main() -> int:
    parser = argparse.ArgumentParser(description="使用核验过的冻结报告审计动态层单因子年化收益门槛。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定正式run；默认读取当前canonical。")
    source.add_argument("--report", type=Path, help="显式冻结报告JSON，可用于未提交诊断。")
    parser.add_argument("--baseline-only", action="store_true", help="只核验完整基线，不计算放宽情景。")
    args = parser.parse_args()
    settings = load_settings()
    if args.report:
        payload = json.loads(args.report.read_text(encoding="utf-8"))
    else:
        latest_run_id, latest_payload = _resolve_latest_run_payload(settings)
        run_id = str(args.run_id or latest_run_id).strip()
        payload = latest_payload if run_id == latest_run_id else _load_runtime_payload(settings, run_id)
    try:
        baseline, context = verify_frozen_baseline(settings, payload)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        baseline, context = {"status": "invalid_frozen_inputs", "baseline_verified": False,
                             "reason": f"{type(exc).__name__}: {exc}"}, {}
    baseline_count = len(context["frames"]["formal_after_all_filters"]) if baseline.get("baseline_verified") else None
    exports: dict[str, pd.DataFrame] = {}
    if not baseline.get("baseline_verified") or args.baseline_only:
        summary = build_incomplete_annual_shadow_summary(
            payload, baseline, baseline_count=baseline_count, baseline_only=args.baseline_only)
    else:
        try:
            summary, exports = _build_frozen_annual_summary(payload, baseline, context)
        except FrozenAnnualScenarioIncomplete as exc:
            summary = build_incomplete_annual_shadow_summary(payload, baseline, baseline_count=baseline_count, issue=exc)
            exports = {**exc.stage_frames,
                       "missing_inputs": pd.DataFrame({"基金代码": exc.missing_codes, "缺失阶段": exc.status})}
    summary["generated_at"] = datetime.now().isoformat(timespec="seconds")
    summary["validation_provider"] = _validation_provider(settings)
    summary["validation_observation_policy"] = (
        f"{summary['validation_provider']}:new_entrants_require_complete_observation;"
        " original_results_may_replay_recorded_degradation"
    )
    from backend.annual_return_gate_shadow import annual_shadow_report_binding

    try:
        summary["source_report_binding"] = annual_shadow_report_binding(settings, payload)
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        summary["source_report_binding_error"] = f"{type(exc).__name__}:{exc}"
    summary = _attach_decision_summary(summary)
    audit_dir = settings.reports_dir / "audit" / "annual_return_gate_shadow"
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    base_name = f"annual_return_gate_shadow_{payload.get('run_id', 'unknown')}_{stamp}"
    summary_path = audit_dir / f"{base_name}.json"
    summary["artifacts"] = {}
    summary["artifact_storage"] = {}
    for key, frame in exports.items():
        path = audit_dir / f"{base_name}_{key}.csv.gz"
        metadata = write_compressed_audit_csv(path, frame)
        # Keep the existing string-path contract for annual audit consumers.
        summary["artifacts"][key] = str(path.relative_to(settings.project_root))
        summary["artifact_storage"][key] = metadata
    temporary = summary_path.with_suffix(".json.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False))
    temporary.replace(summary_path)
    markdown_path = summary_path.with_suffix(".md")
    _write_markdown(summary=summary, direct_relieved_df=exports.get("direct_relieved", pd.DataFrame()),
                    portfolio_hits_df=exports.get("portfolio_hits", pd.DataFrame()), output_path=markdown_path)
    print(json.dumps({"artifact": project_relative_artifact_path(summary_path, project_root=settings.project_root),
                      "artifact_sha256": hashlib.sha256(summary_path.read_bytes()).hexdigest(),
                      "status": summary["status"],
                      "scenario_status": summary["scenario_status"], "passed": summary["passed"]}, ensure_ascii=False))
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
