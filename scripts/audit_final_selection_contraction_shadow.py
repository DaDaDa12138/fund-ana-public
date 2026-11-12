#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.report_builder import (  # noqa: E402
    ENHANCED_BOND_GROUP_LABEL,
    FINAL_SELECTION_HIGH_CONFIDENCE_BONUS,
    FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS,
    FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT,
    FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT,
    FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY,
    FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS,
    FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY,
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL,
    _build_final_selection_contraction_explainability,
    _build_final_selection_tracker_features,
    _build_long_term_feature_frame,
    _load_lightweight_tracker_features_for_contraction,
)
from backend.project_quality_thresholds import PROJECT_ABSOLUTE_QUALITY_THRESHOLDS, combined_quality_floor  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    clean_text as _clean_text,
    resolve_artifact_project_path as _resolve_artifact_project_path,
    resolve_run_payload as _resolve_run_payload,
)

DEFAULT_TARGET_DELTAS = (5, 10, 20)
DEFAULT_HISTORY_WEIGHT_MULTIPLIERS = (0.6, 0.8, 1.2)
INCREMENTAL_HARD_BLOCK_REASON_CODES = {
    "autostock_hard_block",
    "not_buyable",
    "holding_period_over_limit",
    "risk_nav_date_not_aligned",
    "risk_data_points_low",
    "annual_return_min_below_floor",
    "calmar_min_below_floor",
    "max_drawdown_max_above_cap",
    "volatility_max_above_cap",
    "fee_too_high",
    "scale_too_small",
}
INCREMENTAL_WATCH_REASON_CODES = {
    "autostock_status_unknown",
    "sharpe_min_below_floor",
    "positive_ratio_min_below_floor",
    "long_term_feature_incomplete",
    "long_term_stability_weak_or_missing",
    "low_return_drawdown_boundary",
    "enhanced_volatility_positive_boundary",
}
INCREMENTAL_BOUNDARY_QUALITY_REASON_CODES = {
    "sharpe_min_below_floor",
    "positive_ratio_min_below_floor",
    "low_return_drawdown_boundary",
    "enhanced_volatility_positive_boundary",
}


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def _records_for_columns(df: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    available = [column for column in columns if column in df.columns]
    if not available:
        return []
    selected = df[available].copy().astype(object)
    selected = selected.where(selected.notna(), "")
    return selected.astype(str).to_dict(orient="records")


def _reason_category(reason_code: Any) -> str:
    text = str(reason_code or "").strip()
    if not text:
        return "missing_reason"
    if text == "hard_block" or "hard_block" in text.split("|"):
        return "hard_block"
    if "final_selection_stability_cap" in text.split("|"):
        return "final_selection_stability_cap"
    return "other_publish_review"


def _category_label(category: str) -> str:
    return {
        "hard_block": "发布后硬拦截",
        "final_selection_stability_cap": "最终精选稳定性收敛",
        "other_publish_review": "其他发布后复核",
        "missing_reason": "原因缺失",
    }.get(category, category)


def _safe_score(value: Any) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce")
    else:
        series = pd.Series(default, index=df.index, dtype=float)
    return series.fillna(default).astype(float)


def _text_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in df.columns:
        series = df[column]
    else:
        series = pd.Series(default, index=df.index, dtype=object)
    return series.fillna(default).astype(str).str.strip()


def _parse_int_list(value: str, default: tuple[int, ...]) -> list[int]:
    if not str(value or "").strip():
        return list(default)
    parsed: list[int] = []
    for item in str(value).split(","):
        text = item.strip()
        if not text:
            continue
        try:
            number = int(text)
        except ValueError:
            continue
        if number > 0 and number not in parsed:
            parsed.append(number)
    return parsed or list(default)


def _parse_float_list(value: str, default: tuple[float, ...]) -> list[float]:
    if not str(value or "").strip():
        return list(default)
    parsed: list[float] = []
    for item in str(value).split(","):
        text = item.strip()
        if not text:
            continue
        try:
            number = float(text)
        except ValueError:
            continue
        if number > 0 and number not in parsed:
            parsed.append(number)
    return parsed or list(default)


def _records_for_sample(df: pd.DataFrame, columns: list[str], limit: int = 8) -> list[dict[str, Any]]:
    if df.empty or limit <= 0:
        return []
    return _records_for_columns(df.head(limit), columns)


def _distribution(df: pd.DataFrame, column: str, *, limit: int = 10) -> list[dict[str, Any]]:
    if df.empty or column not in df.columns:
        return []
    return [
        {"label": str(label), "count": int(count)}
        for label, count in df[column].fillna("").astype(str).value_counts().head(limit).items()
    ]


def _reason_code_distribution_from_detail(df: pd.DataFrame, *, limit: int = 20) -> list[dict[str, Any]]:
    if df.empty:
        return []
    reason_values: list[str] = []
    if "safety_reason_text" in df.columns:
        for value in df["safety_reason_text"].fillna("").astype(str).tolist():
            reason_values.extend([item for item in value.split("|") if item])
    elif "safety_reason_codes" in df.columns:
        for value in df["safety_reason_codes"].tolist():
            if isinstance(value, list):
                reason_values.extend(str(item) for item in value if str(item))
            else:
                text = str(value or "").strip()
                if text and text not in {"[]", "nan", "None"}:
                    reason_values.append(text)
    if not reason_values:
        return []
    counts = pd.Series(reason_values, dtype=object).value_counts().head(limit)
    return [
        {"label": str(label), "count": int(count)}
        for label, count in counts.items()
    ]


def _has_reason(value: Any, reason_code: str) -> bool:
    return reason_code in {item.strip() for item in str(value or "").split("|") if item.strip()}


def _rows_with_any_reason(df: pd.DataFrame, reason_codes: set[str]) -> pd.DataFrame:
    if df.empty or "safety_reason_text" not in df.columns:
        return df.iloc[0:0].copy()
    mask = df["safety_reason_text"].map(
        lambda value: any(_has_reason(value, reason_code) for reason_code in reason_codes)
    )
    return df[mask].copy()


def _reason_set(value: Any) -> set[str]:
    return {item.strip() for item in str(value or "").split("|") if item.strip()}


def _first_present_reason(value: Any, ordered_reason_codes: list[str]) -> str:
    reasons = _reason_set(value)
    for reason_code in ordered_reason_codes:
        if reason_code in reasons:
            return reason_code
    return ""


def _dedupe_queue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code((row or {}).get("基金代码") or (row or {}).get("fund_code"))
        action_key = str((row or {}).get("action_key") or "").strip()
        if not code or not action_key:
            continue
        key = (action_key, code)
        current_delta = _safe_score(row.get("first_incremental_delta"))
        existing = deduped.get(key)
        existing_delta = _safe_score(existing.get("first_incremental_delta")) if existing else None
        if existing is None or (
            current_delta is not None
            and (existing_delta is None or current_delta < existing_delta)
        ):
            deduped[key] = dict(row)
    return sorted(
        deduped.values(),
        key=lambda row: (
            int(_safe_score(row.get("first_incremental_delta")) or 999999),
            str(row.get("action_key") or ""),
            str(row.get("safety_status") or ""),
            str(row.get("基金代码") or ""),
        ),
    )


def _automation_queue_row(
    row: dict[str, Any],
    *,
    action_key: str,
    action_label: str,
    downstream_queue: str,
    queue_policy: str,
    primary_reason_code: str,
) -> dict[str, Any]:
    reason_text = str(row.get("safety_reason_text") or "").strip()
    return {
        "基金代码": _normalize_code(row.get("基金代码")),
        "基金名称": str(row.get("基金名称") or "").strip(),
        "正式榜分组": str(row.get(FORMAL_GROUP_LABEL) or "").strip(),
        "债基风格标签": str(row.get("债基风格标签") or "").strip(),
        "first_incremental_delta": row.get("first_incremental_delta"),
        "scenario_deltas": row.get("scenario_deltas"),
        "safety_status": str(row.get("safety_status") or "").strip(),
        "safety_reason_text": reason_text,
        "primary_reason_code": primary_reason_code,
        "action_key": action_key,
        "action_label": action_label,
        "downstream_queue": downstream_queue,
        "queue_policy": queue_policy,
        "automation_status": "queued_for_governance",
        "formal_score": row.get(FORMAL_SCORE_LABEL),
        "scenario_score": row.get("_scenario_score"),
        "scale_billion": row.get("基金规模(亿)"),
        "fee_total_pct": row.get("费率合计(%)"),
        "holding_days": row.get("holding_days"),
        "is_buyable": row.get("is_buyable"),
        "autostock_raw_status": row.get("AutoStock原始校验状态"),
        "risk_data_points": row.get("risk_data_points"),
        "risk_end_date": row.get("risk_end_date"),
        "latest_nav_date": row.get("最新净值日期"),
        "long_term_label": row.get("长期稳定性标签"),
        "long_term_feature_complete": row.get("长期稳定性特征完整"),
    }


def _build_incremental_candidate_automation_queue(review_df: pd.DataFrame) -> dict[str, Any]:
    if review_df.empty:
        return {
            "available": True,
            "mode": "incremental_candidate_automation_queue",
            "queue_total": 0,
            "queues": {},
            "queue_rows": [],
        }

    queue_rows: list[dict[str, Any]] = []
    for row in review_df.to_dict(orient="records"):
        reason_text = row.get("safety_reason_text")
        if _has_reason(reason_text, "autostock_status_unknown"):
            queue_rows.append(
                _automation_queue_row(
                    row,
                    action_key="resolve_autostock_unknown",
                    action_label="补齐 AutoStock 未知状态",
                    downstream_queue="autostock_maturity_retry_or_dual_source_review",
                    queue_policy="进入 AutoStock 成熟重试或双源核验候选；结果未收敛前不用于扩容。",
                    primary_reason_code="autostock_status_unknown",
                )
            )
        long_term_reason = _first_present_reason(
            reason_text,
            ["long_term_feature_incomplete", "long_term_stability_weak_or_missing"],
        )
        if long_term_reason:
            queue_rows.append(
                _automation_queue_row(
                    row,
                    action_key="backfill_long_term_features",
                    action_label="补齐长周期稳定性特征",
                    downstream_queue="long_term_history_feature_maintenance",
                    queue_policy="进入长周期历史特征补齐候选；仍需遵守本地 retry/上游缺失冷却。",
                    primary_reason_code=long_term_reason,
                )
            )
        hard_reason = _first_present_reason(
            reason_text,
            [
                "autostock_hard_block",
                "not_buyable",
                "holding_period_over_limit",
                "risk_nav_date_not_aligned",
                "risk_data_points_low",
                "annual_return_min_below_floor",
                "calmar_min_below_floor",
                "max_drawdown_max_above_cap",
                "volatility_max_above_cap",
                "fee_too_high",
                "scale_too_small",
            ],
        )
        if hard_reason:
            queue_rows.append(
                _automation_queue_row(
                    row,
                    action_key="review_hard_blocking_reasons",
                    action_label="复核硬阻断原因",
                    downstream_queue="publish_hard_gate_hold",
                    queue_policy="保持发布硬门禁；只有阻断原因由真实数据消除后才可重新评估。",
                    primary_reason_code=hard_reason,
                )
            )
        boundary_reason = _first_present_reason(
            reason_text,
            [
                "sharpe_min_below_floor",
                "positive_ratio_min_below_floor",
                "low_return_drawdown_boundary",
                "enhanced_volatility_positive_boundary",
            ],
        )
        if boundary_reason:
            queue_rows.append(
                _automation_queue_row(
                    row,
                    action_key="inspect_boundary_quality",
                    action_label="检查边界质量信号",
                    downstream_queue="final_selection_boundary_quality_shadow",
                    queue_policy="进入边界质量 shadow 观察；未完成局部对比前不放松生产阈值。",
                    primary_reason_code=boundary_reason,
                )
            )

    queue_rows = _dedupe_queue_rows(queue_rows)
    queue_df = pd.DataFrame(queue_rows)
    queues: dict[str, Any] = {}
    if not queue_df.empty:
        for action_key, group_df in queue_df.groupby("action_key", sort=True):
            queues[str(action_key)] = {
                "candidate_count": int(len(group_df.index)),
                "downstream_queue": str(group_df.iloc[0].get("downstream_queue") or ""),
                "first_incremental_delta_counts": _distribution(group_df, "first_incremental_delta"),
                "safety_status_counts": _distribution(group_df, "safety_status"),
                "primary_reason_counts": _distribution(group_df, "primary_reason_code"),
                "codes": [str(code) for code in group_df["基金代码"].fillna("").astype(str).tolist() if str(code)],
            }
    return {
        "available": True,
        "mode": "incremental_candidate_automation_queue",
        "calculation_note": (
            "该队列只把最终精选扩容候选转换为自动化治理输入；"
            "不直接触发补跑、不放松生产阈值、不绕开发布硬门禁。"
        ),
        "queue_total": int(len(queue_rows)),
        "candidate_code_total": int(len({str(row.get("基金代码") or "") for row in queue_rows if str(row.get("基金代码") or "")})),
        "queues": queues,
        "queue_rows": queue_rows,
    }


def _build_incremental_candidate_governance_summary(
    review_df: pd.DataFrame,
    *,
    incremental_codes_by_delta: dict[int, set[str]],
) -> dict[str, Any]:
    if review_df.empty:
        return {
            "available": True,
            "mode": "incremental_candidate_governance_actions",
            "candidate_count": 0,
            "delta_recommendations": [],
            "action_items": [],
            "automation_queue": _build_incremental_candidate_automation_queue(review_df),
        }

    sample_columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "债基风格标签",
        "first_incremental_delta",
        "safety_status",
        "safety_reason_text",
        FORMAL_SCORE_LABEL,
        "_scenario_score",
        "基金规模(亿)",
        "费率合计(%)",
        "AutoStock原始校验状态",
        "risk_data_points",
        "1年-年化收益(%)",
        "1年-波动率",
        "1年-最大回撤(%)",
        "1年-夏普比率",
        "长期稳定性标签",
        "长期稳定性特征完整",
    ]
    action_groups = [
        {
            "action_key": "resolve_autostock_unknown",
            "action_label": "补齐 AutoStock 未知状态",
            "reason_codes": {"autostock_status_unknown"},
            "policy": "扩容前应先让 AutoStock 状态从 unknown 收敛为严格通过、软告警或硬拦截。",
        },
        {
            "action_key": "backfill_long_term_features",
            "action_label": "补齐长周期稳定性特征",
            "reason_codes": {"long_term_feature_incomplete", "long_term_stability_weak_or_missing"},
            "policy": "长周期特征缺失或偏弱的样本只作为观察候选，不作为提高 final_selection_target_max 的依据。",
        },
        {
            "action_key": "review_hard_blocking_reasons",
            "action_label": "复核硬阻断原因",
            "reason_codes": INCREMENTAL_HARD_BLOCK_REASON_CODES,
            "policy": "硬阻断样本不得通过扩容或权重调整绕过，必须先由数据或策略复核证明阻断原因已消除。",
        },
        {
            "action_key": "inspect_boundary_quality",
            "action_label": "检查边界质量信号",
            "reason_codes": INCREMENTAL_BOUNDARY_QUALITY_REASON_CODES,
            "policy": "边界风险收益信号需保留观察，不应在未完成局部 shadow 对比前放松收益、波动或稳定性权重。",
        },
    ]
    action_items: list[dict[str, Any]] = []
    for group in action_groups:
        group_df = _rows_with_any_reason(review_df, set(group["reason_codes"]))
        action_items.append(
            {
                "action_key": group["action_key"],
                "action_label": group["action_label"],
                "policy": group["policy"],
                "reason_codes": sorted(group["reason_codes"]),
                "candidate_count": int(len(group_df.index)),
                "first_incremental_delta_counts": _distribution(group_df, "first_incremental_delta"),
                "status_counts": _distribution(group_df, "safety_status"),
                "samples": _records_for_sample(group_df, sample_columns, limit=8),
            }
        )

    pass_df = review_df[review_df["safety_status"].eq("pass")].copy() if "safety_status" in review_df.columns else review_df.iloc[0:0].copy()
    block_df = review_df[review_df["safety_status"].eq("block")].copy() if "safety_status" in review_df.columns else review_df.iloc[0:0].copy()
    watch_df = review_df[review_df["safety_status"].eq("watch")].copy() if "safety_status" in review_df.columns else review_df.iloc[0:0].copy()

    delta_recommendations: list[dict[str, Any]] = []
    for delta in sorted(incremental_codes_by_delta):
        codes = incremental_codes_by_delta.get(delta) or set()
        delta_df = review_df[review_df["基金代码"].astype(str).isin(codes)].copy()
        candidate_count = int(len(delta_df.index))
        pass_count = int(delta_df["safety_status"].eq("pass").sum()) if "safety_status" in delta_df.columns else 0
        watch_count = int(delta_df["safety_status"].eq("watch").sum()) if "safety_status" in delta_df.columns else 0
        block_count = int(delta_df["safety_status"].eq("block").sum()) if "safety_status" in delta_df.columns else 0
        if candidate_count > 0 and pass_count == candidate_count:
            recommendation = "eligible_for_threshold_review"
            recommendation_label = "可进入扩容阈值复核"
        elif block_count > 0:
            recommendation = "hold_target_max_until_blockers_resolved"
            recommendation_label = "先消除硬阻断再讨论扩容"
        else:
            recommendation = "hold_target_max_until_watch_reasons_resolved"
            recommendation_label = "先消除观察原因再讨论扩容"
        delta_recommendations.append(
            {
                "target_delta": int(delta),
                "candidate_count": candidate_count,
                "pass_count": pass_count,
                "watch_count": watch_count,
                "block_count": block_count,
                "recommendation": recommendation,
                "recommendation_label": recommendation_label,
            }
        )

    return {
        "available": True,
        "mode": "incremental_candidate_governance_actions",
        "candidate_count": int(len(review_df.index)),
        "status_counts": _distribution(review_df, "safety_status"),
        "delta_recommendations": delta_recommendations,
        "action_items": action_items,
        "automation_queue": _build_incremental_candidate_automation_queue(review_df),
        "clean_pass_count": int(len(pass_df.index)),
        "watch_count": int(len(watch_df.index)),
        "block_count": int(len(block_df.index)),
        "clean_pass_samples": _records_for_sample(pass_df, sample_columns, limit=8),
        "blocked_samples": _records_for_sample(block_df, sample_columns, limit=8),
    }


def _primary_boundary_reason(value: Any) -> str:
    return _first_present_reason(
        value,
        [
            "sharpe_min_below_floor",
            "positive_ratio_min_below_floor",
            "low_return_drawdown_boundary",
            "enhanced_volatility_positive_boundary",
        ],
    )


def _boundary_quality_action(row: dict[str, Any]) -> str:
    reason_text = str(row.get("safety_reason_text") or "").strip()
    if str(row.get("safety_status") or "").strip() == "block":
        return "hold_until_hard_block_resolved"
    if _has_reason(reason_text, "autostock_status_unknown"):
        return "resolve_autostock_before_boundary_decision"
    if _has_reason(reason_text, "long_term_feature_incomplete"):
        return "backfill_long_term_before_boundary_decision"
    primary_reason = _primary_boundary_reason(reason_text)
    if primary_reason:
        return "keep_boundary_watch_without_threshold_change"
    return "observe_only"


def _build_boundary_quality_shadow(
    review_df: pd.DataFrame,
    *,
    contraction_df: pd.DataFrame,
) -> dict[str, Any]:
    if review_df.empty:
        return {
            "available": True,
            "mode": "final_selection_boundary_quality_shadow",
            "candidate_count": 0,
            "style_summary": [],
            "reason_counts": [],
            "candidate_rows": [],
        }

    boundary_df = _rows_with_any_reason(review_df, INCREMENTAL_BOUNDARY_QUALITY_REASON_CODES)
    if boundary_df.empty:
        return {
            "available": True,
            "mode": "final_selection_boundary_quality_shadow",
            "candidate_count": 0,
            "style_summary": [],
            "reason_counts": [],
            "candidate_rows": [],
        }

    current = contraction_df.copy()
    if not current.empty:
        current["基金代码"] = _text_series(current, "基金代码").map(_normalize_code)
        current["债基风格标签"] = _text_series(current, "债基风格标签")
        current["正式精选状态"] = _text_series(current, "正式精选状态")
        current["_final_stability_score_num"] = pd.to_numeric(
            current.get("_final_stability_score"),
            errors="coerce",
        )

    boundary_df = boundary_df.copy()
    boundary_df["基金代码"] = boundary_df.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    boundary_df["primary_boundary_reason"] = boundary_df.get("safety_reason_text", pd.Series(dtype=object)).map(
        _primary_boundary_reason
    )
    boundary_df["boundary_quality_action"] = boundary_df.apply(_boundary_quality_action, axis=1)
    for column in [
        "first_incremental_delta",
        "_scenario_score",
        FORMAL_SCORE_LABEL,
        "出现次数",
        "出现频率(%)",
        "基金规模(亿)",
        "费率合计(%)",
        "1年-年化收益(%)",
        "1年-波动率",
        "1年-最大回撤(%)",
        "1年-夏普比率",
        "1年-卡玛比率",
        "1年-正收益比率",
        "长期稳定性总分",
    ]:
        boundary_df[column] = pd.to_numeric(boundary_df.get(column), errors="coerce")

    selected_boundary_by_style: dict[str, dict[str, Any]] = {}
    if not current.empty:
        for style, selected_df in current[current["正式精选状态"].eq("正式入围")].groupby("债基风格标签"):
            selected_df = selected_df.dropna(subset=["_final_stability_score_num"]).sort_values(
                by=["_final_stability_score_num", FORMAL_SCORE_LABEL, "基金代码"],
                ascending=[True, True, True],
                kind="mergesort",
            )
            if selected_df.empty:
                continue
            selected_boundary_by_style[str(style)] = {
                "selected_min_stability_score": round(float(selected_df["_final_stability_score_num"].iloc[0]), 4),
                "boundary_selected_codes": selected_df.head(5)["基金代码"].astype(str).tolist(),
            }

    candidate_rows: list[dict[str, Any]] = []
    for row in boundary_df.to_dict(orient="records"):
        style = str(row.get("债基风格标签") or "").strip()
        selected_reference = selected_boundary_by_style.get(style) or {}
        selected_min = _safe_score(selected_reference.get("selected_min_stability_score"))
        scenario_score = _safe_score(row.get("_scenario_score"))
        candidate_rows.append(
            {
                "基金代码": _normalize_code(row.get("基金代码")),
                "基金名称": str(row.get("基金名称") or "").strip(),
                "正式榜分组": str(row.get(FORMAL_GROUP_LABEL) or "").strip(),
                "债基风格标签": style,
                "first_incremental_delta": int(_safe_score(row.get("first_incremental_delta")) or 0),
                "safety_status": str(row.get("safety_status") or "").strip(),
                "safety_reason_text": str(row.get("safety_reason_text") or "").strip(),
                "primary_boundary_reason": str(row.get("primary_boundary_reason") or "").strip(),
                "boundary_quality_action": str(row.get("boundary_quality_action") or "").strip(),
                "formal_score": row.get(FORMAL_SCORE_LABEL),
                "scenario_score": scenario_score,
                "selected_min_stability_score": selected_min,
                "score_gap_vs_style_selected_min": (
                    None
                    if scenario_score is None or selected_min is None
                    else round(float(scenario_score) - float(selected_min), 4)
                ),
                "boundary_selected_reference_codes": list(
                    selected_reference.get("boundary_selected_codes") or []
                ),
                "annual_return_1y": row.get("1年-年化收益(%)"),
                "volatility_1y": row.get("1年-波动率"),
                "max_drawdown_1y": row.get("1年-最大回撤(%)"),
                "sharpe_1y": row.get("1年-夏普比率"),
                "calmar_1y": row.get("1年-卡玛比率"),
                "positive_ratio_1y": row.get("1年-正收益比率"),
                "scale_billion": row.get("基金规模(亿)"),
                "fee_total_pct": row.get("费率合计(%)"),
                "long_term_label": row.get("长期稳定性标签"),
                "long_term_feature_complete": row.get("长期稳定性特征完整"),
                "long_term_score": row.get("长期稳定性总分"),
                "autostock_raw_status": row.get("AutoStock原始校验状态"),
                "annual_history_summary": row.get("annual_history_summary"),
            }
        )

    rows_df = pd.DataFrame(candidate_rows)
    style_summary: list[dict[str, Any]] = []
    for style, group_df in rows_df.groupby("债基风格标签", sort=True):
        gap_series = pd.to_numeric(group_df.get("score_gap_vs_style_selected_min"), errors="coerce")
        style_summary.append(
            {
                "style": str(style),
                "candidate_count": int(len(group_df.index)),
                "status_counts": _distribution(group_df, "safety_status"),
                "primary_boundary_reason_counts": _distribution(group_df, "primary_boundary_reason"),
                "action_counts": _distribution(group_df, "boundary_quality_action"),
                "first_incremental_delta_counts": _distribution(group_df, "first_incremental_delta"),
                "max_score_gap_vs_style_selected_min": (
                    None if gap_series.dropna().empty else round(float(gap_series.max()), 4)
                ),
                "min_sharpe_1y": (
                    None
                    if pd.to_numeric(group_df.get("sharpe_1y"), errors="coerce").dropna().empty
                    else round(float(pd.to_numeric(group_df.get("sharpe_1y"), errors="coerce").min()), 4)
                ),
                "max_volatility_1y": (
                    None
                    if pd.to_numeric(group_df.get("volatility_1y"), errors="coerce").dropna().empty
                    else round(float(pd.to_numeric(group_df.get("volatility_1y"), errors="coerce").max()), 6)
                ),
                "min_positive_ratio_1y": (
                    None
                    if pd.to_numeric(group_df.get("positive_ratio_1y"), errors="coerce").dropna().empty
                    else round(float(pd.to_numeric(group_df.get("positive_ratio_1y"), errors="coerce").min()), 4)
                ),
                "candidate_codes": group_df["基金代码"].astype(str).tolist(),
            }
        )

    return {
        "available": True,
        "mode": "final_selection_boundary_quality_shadow",
        "calculation_note": (
            "该 shadow 只审计 automation_queue.inspect_boundary_quality 候选，"
            "对比其风险收益边界、长期稳定性和同风格入围边界差距；不调整生产阈值。"
        ),
        "candidate_count": int(len(rows_df.index)),
        "status_counts": _distribution(rows_df, "safety_status"),
        "reason_counts": _distribution(rows_df, "primary_boundary_reason"),
        "action_counts": _distribution(rows_df, "boundary_quality_action"),
        "style_summary": style_summary,
        "candidate_rows": candidate_rows,
    }


def _artifact_records_by_code(
    snapshot_payload: dict[str, Any],
    keys: list[str],
) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for key in keys:
        rows = (snapshot_payload or {}).get(key) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = _normalize_code(row.get("基金代码") or row.get("fcode"))
            if code and code not in records:
                records[code] = dict(row)
    return records


def _load_database_safety_rows(db_path: Path, codes: set[str]) -> pd.DataFrame:
    columns = [
        "基金代码",
        "基金名称",
        "基金类型",
        "成立日期",
        "基金规模(亿)",
        "has_holding_period",
        "holding_days",
        "holding_period_source",
        "is_buyable",
        "申购状态",
        "管理费率(%)",
        "托管费率(%)",
        "销售服务费率(%)",
        "费率合计(%)",
        "最新净值日期",
        "1年-年化收益(%)",
        "1年-波动率",
        "1年-最大回撤(%)",
        "1年-夏普比率",
        "1年-卡玛比率",
        "1年-正收益比率",
        "risk_data_points",
        "risk_end_date",
    ]
    normalized_codes = sorted({_normalize_code(code) for code in codes if _normalize_code(code)})
    if not normalized_codes or not db_path.exists() or not db_path.is_file():
        return pd.DataFrame(columns=columns)
    placeholders = ",".join("?" for _ in normalized_codes)
    query = f"""
        SELECT
            funds.fcode AS 基金代码,
            funds.shortname AS 基金名称,
            funds.ftype AS 基金类型,
            funds.estab_date AS 成立日期,
            funds.scale_billion AS "基金规模(亿)",
            funds.has_holding_period AS has_holding_period,
            funds.holding_days AS holding_days,
            funds.holding_period_source AS holding_period_source,
            funds.is_buyable AS is_buyable,
            funds.purchase_status AS 申购状态,
            funds.mgr_fee_rate AS "管理费率(%)",
            funds.trust_fee_rate AS "托管费率(%)",
            funds.sales_fee_rate AS "销售服务费率(%)",
            fund_returns.latest_nav_date AS 最新净值日期,
            risk_latest.annual_return AS "1年-年化收益(%)",
            risk_latest.volatility AS "1年-波动率",
            risk_latest.max_drawdown AS "1年-最大回撤(%)",
            risk_latest.sharpe_ratio AS "1年-夏普比率",
            risk_latest.calmar_ratio AS "1年-卡玛比率",
            risk_latest.positive_ratio AS "1年-正收益比率",
            risk_latest.data_points AS risk_data_points,
            risk_latest.end_date AS risk_end_date
        FROM funds
        LEFT JOIN fund_returns ON funds.fcode = fund_returns.fcode
        LEFT JOIN fund_risk_metrics risk_latest
            ON risk_latest.rowid = (
                SELECT risk_row.rowid
                FROM fund_risk_metrics risk_row
                WHERE risk_row.fcode = funds.fcode
                  AND risk_row.period = '1y'
                ORDER BY COALESCE(TRIM(risk_row.end_date), '') DESC, risk_row.rowid DESC
                LIMIT 1
            )
        WHERE funds.fcode IN ({placeholders})
    """
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(query, conn, params=normalized_codes)
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame(columns=columns)
    df["基金代码"] = df["基金代码"].map(_normalize_code)
    for column in [
        "基金规模(亿)",
        "has_holding_period",
        "holding_days",
        "is_buyable",
        "管理费率(%)",
        "托管费率(%)",
        "销售服务费率(%)",
        "1年-年化收益(%)",
        "1年-波动率",
        "1年-最大回撤(%)",
        "1年-夏普比率",
        "1年-卡玛比率",
        "1年-正收益比率",
        "risk_data_points",
    ]:
        df[column] = pd.to_numeric(df.get(column), errors="coerce")
    df["费率合计(%)"] = (
        df[["管理费率(%)", "托管费率(%)", "销售服务费率(%)"]]
        .fillna(0.0)
        .sum(axis=1)
    )
    return df.reindex(columns=columns)


def _latest_years_summary(db_path: Path, codes: set[str]) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({_normalize_code(code) for code in codes if _normalize_code(code)})
    if not normalized_codes or not db_path.exists() or not db_path.is_file():
        return {}
    placeholders = ",".join("?" for _ in normalized_codes)
    conn = sqlite3.connect(str(db_path))
    try:
        rows = pd.read_sql_query(
            f"""
            SELECT fcode, year, annual_return, percentile
            FROM fund_annual_returns
            WHERE fcode IN ({placeholders})
            ORDER BY fcode, year DESC
            """,
            conn,
            params=normalized_codes,
        )
    finally:
        conn.close()
    if rows.empty:
        return {}
    result: dict[str, dict[str, Any]] = {}
    rows["fcode"] = rows["fcode"].map(_normalize_code)
    rows["annual_return"] = pd.to_numeric(rows.get("annual_return"), errors="coerce")
    rows["percentile"] = pd.to_numeric(rows.get("percentile"), errors="coerce")
    for code, group in rows.groupby("fcode"):
        latest = group.head(5).copy()
        valid_returns = latest["annual_return"].dropna()
        result[str(code)] = {
            "latest_years": [
                {
                    "year": int(row["year"]),
                    "annual_return": None if pd.isna(row["annual_return"]) else round(float(row["annual_return"]), 4),
                    "percentile": None if pd.isna(row["percentile"]) else round(float(row["percentile"]), 4),
                }
                for row in latest.to_dict(orient="records")
            ],
            "positive_year_count": int((valid_returns > 0).sum()),
            "valid_year_count": int(len(valid_returns.index)),
            "min_annual_return": None if valid_returns.empty else round(float(valid_returns.min()), 4),
        }
    return result


def _safety_status_label(status: str) -> str:
    return {
        "pass": "安全通过",
        "watch": "观察复核",
        "block": "暂不建议扩容",
    }.get(status, status)


def _max_status(*statuses: str) -> str:
    order = {"pass": 0, "watch": 1, "block": 2}
    return max((status for status in statuses if status), key=lambda item: order.get(item, 0), default="pass")


def _evaluate_incremental_candidate(row: dict[str, Any], *, exclude_holding_days_gt: int) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    style = str(row.get("债基风格标签") or "").strip() or "DEFAULT"
    floors = combined_quality_floor(style)
    scale = _safe_score(row.get("基金规模(亿)"))
    fee_total = _safe_score(row.get("费率合计(%)"))
    holding_days = _safe_score(row.get("holding_days"))
    has_holding_period = int(_safe_score(row.get("has_holding_period")) or 0)
    is_buyable = int(_safe_score(row.get("is_buyable")) or 0)
    autostock_status = str(row.get("AutoStock原始校验状态") or row.get("AutoStock校验状态") or "").strip()
    annual_return = _safe_score(row.get("1年-年化收益(%)"))
    volatility = _safe_score(row.get("1年-波动率"))
    max_drawdown = _safe_score(row.get("1年-最大回撤(%)"))
    sharpe_ratio = _safe_score(row.get("1年-夏普比率"))
    calmar_ratio = _safe_score(row.get("1年-卡玛比率"))
    positive_ratio = _safe_score(row.get("1年-正收益比率"))
    risk_points = _safe_score(row.get("risk_data_points"))
    long_term_ready = str(row.get("长期稳定性特征完整") or "").strip().lower() in {"true", "1", "yes"}
    long_term_tag = str(row.get("长期稳定性标签") or "").strip()
    latest_nav_date = str(row.get("最新净值日期") or "").strip()
    risk_end_date = str(row.get("risk_end_date") or "").strip()

    if autostock_status == "hard_block":
        reason_codes.append("autostock_hard_block")
    elif autostock_status in {"unknown", ""}:
        reason_codes.append("autostock_status_unknown")
    if is_buyable != 1:
        reason_codes.append("not_buyable")
    if has_holding_period == 1 and holding_days is not None and holding_days > exclude_holding_days_gt:
        reason_codes.append("holding_period_over_limit")
    if not latest_nav_date or not risk_end_date or latest_nav_date[:10] != risk_end_date[:10]:
        reason_codes.append("risk_nav_date_not_aligned")
    if risk_points is None or risk_points < 220:
        reason_codes.append("risk_data_points_low")
    for metric_key, threshold_key, relation in [
        ("1年-年化收益(%)", "annual_return_min", "min"),
        ("1年-夏普比率", "sharpe_min", "min"),
        ("1年-卡玛比率", "calmar_min", "min"),
        ("1年-正收益比率", "positive_ratio_min", "min"),
        ("1年-最大回撤(%)", "max_drawdown_max", "max"),
        ("1年-波动率", "volatility_max", "max"),
    ]:
        value = _safe_score(row.get(metric_key))
        threshold = floors.get(threshold_key)
        if value is None or threshold is None:
            reason_codes.append(f"{threshold_key}_missing")
        elif relation == "min" and value < float(threshold):
            reason_codes.append(f"{threshold_key}_below_floor")
        elif relation == "max" and value > float(threshold):
            reason_codes.append(f"{threshold_key}_above_cap")
    if fee_total is None or fee_total > float(floors["fee_total_max"]):
        reason_codes.append("fee_too_high")
    if scale is None or scale < float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]):
        reason_codes.append("scale_too_small")
    if not long_term_ready:
        reason_codes.append("long_term_feature_incomplete")
    elif long_term_tag in {"长期稳定性偏弱", "长期历史缺失", "长期历史部分缺失"}:
        reason_codes.append("long_term_stability_weak_or_missing")
    if annual_return is not None and max_drawdown is not None and annual_return < 3.0 and max_drawdown > 0.006:
        reason_codes.append("low_return_drawdown_boundary")
    if volatility is not None and positive_ratio is not None and volatility > 0.05 and positive_ratio < 58:
        reason_codes.append("enhanced_volatility_positive_boundary")

    if any(code in INCREMENTAL_HARD_BLOCK_REASON_CODES for code in reason_codes):
        status = "block"
    elif any(code in INCREMENTAL_WATCH_REASON_CODES for code in reason_codes):
        status = "watch"
    else:
        status = "pass"
    return status, sorted(set(reason_codes))


def _style_action(
    *,
    selected_min: float | None,
    stability_removed_max: float | None,
    hard_block_removed_max: float | None,
    stability_removed_count: int,
    hard_block_removed_count: int,
) -> str:
    if hard_block_removed_count > 0 and (
        selected_min is None
        or (hard_block_removed_max is not None and hard_block_removed_max > selected_min)
    ):
        return "explain_hard_block_before_threshold_change"
    if stability_removed_count > 0 and selected_min is not None and stability_removed_max is not None:
        if selected_min - stability_removed_max <= 1.0:
            return "review_stability_cap_boundary"
    if stability_removed_count > 0:
        return "observe_stability_cap"
    return "observe_only"


def _prepare_scenario_frame(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        return current
    current["基金代码"] = _text_series(current, "基金代码").map(_normalize_code)
    current = current[current["基金代码"].astype(str).str.strip() != ""].copy()
    current = current.drop_duplicates(subset=["基金代码"], keep="first")
    current[FORMAL_GROUP_LABEL] = _text_series(current, FORMAL_GROUP_LABEL)
    current["债基风格标签"] = _text_series(current, "债基风格标签")
    current["正式精选状态"] = _text_series(current, "正式精选状态")
    current["最终精选原因代码"] = _text_series(current, "最终精选原因代码")
    current["最终精选原因分类"] = current["最终精选原因代码"].map(_reason_category)
    current["_is_hard_block"] = current["最终精选原因分类"].eq("hard_block")
    current["_is_official"] = current["正式精选状态"].eq("正式入围")
    current[FORMAL_SCORE_LABEL] = _numeric_series(current, FORMAL_SCORE_LABEL)
    current["出现次数"] = _numeric_series(current, "出现次数", default=1.0)
    current["出现频率(%)"] = _numeric_series(current, "出现频率(%)", default=100.0)
    current["_final_stability_score"] = _numeric_series(current, "_final_stability_score")
    current["_final_confidence_bonus"] = _numeric_series(current, "_final_confidence_bonus")
    current["_final_new_high_bonus"] = _numeric_series(current, "_final_new_high_bonus")
    current["_final_new_low_penalty"] = _numeric_series(current, "_final_new_low_penalty")
    if "_final_history_occurrence_bonus" in current.columns:
        current["_final_history_occurrence_bonus"] = _numeric_series(current, "_final_history_occurrence_bonus")
    else:
        current["_final_history_occurrence_bonus"] = (
            current["出现次数"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT
        )
    if "_final_history_frequency_bonus" in current.columns:
        current["_final_history_frequency_bonus"] = _numeric_series(current, "_final_history_frequency_bonus")
    else:
        current["_final_history_frequency_bonus"] = (
            current["出现频率(%)"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT
        )
    if current["_final_confidence_bonus"].abs().sum() == 0:
        confidence = _text_series(current, "项目质量信心")
        current["_final_confidence_bonus"] = confidence.map(
            {
                "高信心": FINAL_SELECTION_HIGH_CONFIDENCE_BONUS,
                "中信心": FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS,
                "边界观察": -FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY,
            }
        ).fillna(0.0)
    if current["_final_stability_score"].abs().sum() == 0:
        current["_final_stability_score"] = (
            current[FORMAL_SCORE_LABEL].fillna(0.0)
            + current["_final_confidence_bonus"].fillna(0.0)
            + current["_final_history_occurrence_bonus"].fillna(0.0)
            + current["_final_history_frequency_bonus"].fillna(0.0)
            + current["_final_new_high_bonus"].fillna(0.0)
            - current["_final_new_low_penalty"].fillna(0.0)
        )
    return current.reset_index(drop=True)


def _select_pre_hard_block_codes(
    df: pd.DataFrame,
    *,
    target_total: int,
    group_min_ratio: float,
    score_column: str,
) -> set[str]:
    if df.empty or target_total <= 0:
        return set()
    group_min_ratio = max(0.0, min(float(group_min_ratio or 0.25), 0.49))
    min_group_quota = max(1, int(round(target_total * group_min_ratio)))
    selected_parts: list[pd.DataFrame] = []
    selected_codes: set[str] = set()
    group_label_series = _text_series(df, FORMAL_GROUP_LABEL)
    for group_label in (STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL):
        group_df = df[group_label_series.eq(group_label)].copy()
        if group_df.empty:
            continue
        picked = group_df.sort_values(
            by=[score_column, FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
            ascending=[False, False, False, False, True],
            kind="mergesort",
        ).head(min(len(group_df.index), min_group_quota))
        if picked.empty:
            continue
        selected_parts.append(picked)
        selected_codes.update(picked["基金代码"].astype(str).tolist())

    remaining_slots = max(0, target_total - len(selected_codes))
    if remaining_slots > 0:
        remaining_df = df[~df["基金代码"].astype(str).isin(selected_codes)].copy()
        if not remaining_df.empty:
            picked_remaining = remaining_df.sort_values(
                by=[score_column, FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
                ascending=[False, False, False, False, True],
                kind="mergesort",
            ).head(remaining_slots)
            if not picked_remaining.empty:
                selected_parts.append(picked_remaining)
    if not selected_parts:
        return set()
    selected_df = pd.concat(selected_parts, ignore_index=True)
    return {
        _normalize_code(code)
        for code in selected_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }


def _selection_result_for_codes(
    df: pd.DataFrame,
    selected_codes: set[str],
    *,
    incremental_detail_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_df = df[df["基金代码"].astype(str).isin(selected_codes)].copy()
    final_safe_df = selected_df[~selected_df["_is_hard_block"]].copy()
    official_codes = set(df.loc[df["_is_official"], "基金代码"].astype(str).tolist())
    incremental_df = final_safe_df[
        ~final_safe_df["基金代码"].astype(str).isin(official_codes)
    ].copy()
    incremental_df = incremental_df.sort_values(
        by=["_scenario_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    )
    incremental_detail_map = incremental_detail_map or {}
    detail_rows = [
        incremental_detail_map.get(_normalize_code(code), {})
        for code in incremental_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
    ]
    detail_df = pd.DataFrame([row for row in detail_rows if row])
    reason_counts = _reason_code_distribution_from_detail(detail_df)
    return {
        "pre_hard_block_selected_count": int(len(selected_df.index)),
        "pre_hard_block_hard_block_count": int(selected_df["_is_hard_block"].sum()),
        "post_hard_block_selected_count": int(len(final_safe_df.index)),
        "incremental_safe_count": int(len(incremental_df.index)),
        "incremental_by_style": _distribution(incremental_df, "债基风格标签"),
        "incremental_by_group": _distribution(incremental_df, FORMAL_GROUP_LABEL),
        "incremental_safety_status_counts": _distribution(detail_df, "safety_status"),
        "incremental_safety_reason_counts": reason_counts,
        "incremental_samples": _records_for_sample(
            detail_df if not detail_df.empty else incremental_df,
            [
                "基金代码",
                "基金名称",
                FORMAL_GROUP_LABEL,
                "债基风格标签",
                "最终精选原因代码",
                FORMAL_SCORE_LABEL,
                "safety_status",
                "safety_status_label",
                "safety_reason_codes",
                "safety_reason_text",
                "出现次数",
                "出现频率(%)",
                "基金规模(亿)",
                "费率合计(%)",
                "holding_days",
                "AutoStock原始校验状态",
                "长期稳定性标签",
                "长期稳定性特征完整",
                "1年-年化收益(%)",
                "1年-最大回撤(%)",
                "1年-波动率",
                "1年-夏普比率",
                "1年-卡玛比率",
                "1年-正收益比率",
                "_scenario_score",
            ],
        ),
    }


def _build_target_expansion_shadow(
    df: pd.DataFrame,
    *,
    settings,
    target_deltas: list[int],
    incremental_detail_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    current = _prepare_scenario_frame(df)
    if current.empty:
        return {"available": False, "reason": "empty_contraction_records", "scenarios": []}
    target_total = int(getattr(settings, "final_selection_target_max", 0) or 0)
    if target_total <= 0:
        return {"available": False, "reason": "final_selection_target_max_not_configured", "scenarios": []}
    current["_scenario_score"] = current["_final_stability_score"]
    group_min_ratio = float(getattr(settings, "final_selection_group_min_ratio", 0.25) or 0.25)
    official_count = int(current["_is_official"].sum())
    hard_block_count = int(current["_is_hard_block"].sum())
    baseline_codes = _select_pre_hard_block_codes(
        current,
        target_total=target_total,
        group_min_ratio=group_min_ratio,
        score_column="_scenario_score",
    )
    baseline_result = _selection_result_for_codes(
        current,
        baseline_codes,
        incremental_detail_map=incremental_detail_map,
    )
    scenarios: list[dict[str, Any]] = []
    for delta in target_deltas:
        scenario_target = target_total + int(delta)
        scenario_codes = _select_pre_hard_block_codes(
            current,
            target_total=scenario_target,
            group_min_ratio=group_min_ratio,
            score_column="_scenario_score",
        )
        scenario = _selection_result_for_codes(
            current,
            scenario_codes,
            incremental_detail_map=incremental_detail_map,
        )
        scenario.update(
            {
                "target_total": int(scenario_target),
                "target_delta": int(delta),
                "post_hard_block_increment_vs_current_official": int(
                    scenario["post_hard_block_selected_count"] - official_count
                ),
            }
        )
        scenarios.append(scenario)
    return {
        "available": True,
        "mode": "post_hard_block_safe_shadow",
        "calculation_note": (
            "该 shadow 使用最终精选解释记录重放 target_max 排序，并保留 AutoStock hard_block 排除；"
            "用于判断扩容边界，不会改动生产发布参数。"
        ),
        "target_total": int(target_total),
        "group_min_ratio": round(group_min_ratio, 4),
        "official_count": official_count,
        "hard_block_removed_count": hard_block_count,
        "baseline_replay": baseline_result,
        "baseline_post_hard_block_aligned": baseline_result["post_hard_block_selected_count"] == official_count,
        "target_delta_scenarios": scenarios,
    }


def _build_history_weight_sensitivity_shadow(
    df: pd.DataFrame,
    *,
    settings,
    multipliers: list[float],
) -> dict[str, Any]:
    current = _prepare_scenario_frame(df)
    if current.empty:
        return {"available": False, "reason": "empty_contraction_records", "scenarios": []}
    target_total = int(getattr(settings, "final_selection_target_max", 0) or 0)
    if target_total <= 0:
        return {"available": False, "reason": "final_selection_target_max_not_configured", "scenarios": []}
    group_min_ratio = float(getattr(settings, "final_selection_group_min_ratio", 0.25) or 0.25)
    official_codes = set(current.loc[current["_is_official"], "基金代码"].astype(str).tolist())
    official_count = int(len(official_codes))
    scenarios: list[dict[str, Any]] = []
    for multiplier in multipliers:
        scenario_df = current.copy()
        scenario_df["_scenario_score"] = (
            scenario_df[FORMAL_SCORE_LABEL].fillna(0.0)
            + scenario_df["_final_confidence_bonus"].fillna(0.0)
            + (
                scenario_df["_final_history_occurrence_bonus"].fillna(0.0)
                + scenario_df["_final_history_frequency_bonus"].fillna(0.0)
            )
            * float(multiplier)
            + scenario_df["_final_new_high_bonus"].fillna(0.0)
            - scenario_df["_final_new_low_penalty"].fillna(0.0)
        )
        scenario_codes = _select_pre_hard_block_codes(
            scenario_df,
            target_total=target_total,
            group_min_ratio=group_min_ratio,
            score_column="_scenario_score",
        )
        selected_df = scenario_df[scenario_df["基金代码"].astype(str).isin(scenario_codes)].copy()
        final_safe_df = selected_df[~selected_df["_is_hard_block"]].copy()
        scenario_official_codes = set(final_safe_df["基金代码"].astype(str).tolist())
        gained_codes = scenario_official_codes - official_codes
        lost_codes = official_codes - scenario_official_codes
        gained_df = scenario_df[scenario_df["基金代码"].astype(str).isin(gained_codes)].copy().sort_values(
            by=["_scenario_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
            ascending=[False, False, False, False, True],
            kind="mergesort",
        )
        lost_df = scenario_df[scenario_df["基金代码"].astype(str).isin(lost_codes)].copy().sort_values(
            by=["_scenario_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
            ascending=[True, True, True, True, True],
            kind="mergesort",
        )
        scenarios.append(
            {
                "history_weight_multiplier": round(float(multiplier), 4),
                "post_hard_block_selected_count": int(len(final_safe_df.index)),
                "changed_count": int(len(gained_codes) + len(lost_codes)),
                "gained_count": int(len(gained_codes)),
                "lost_count": int(len(lost_codes)),
                "gained_by_style": _distribution(gained_df, "债基风格标签"),
                "lost_by_style": _distribution(lost_df, "债基风格标签"),
                "gained_samples": _records_for_sample(
                    gained_df,
                    [
                        "基金代码",
                        "基金名称",
                        FORMAL_GROUP_LABEL,
                        "债基风格标签",
                        FORMAL_SCORE_LABEL,
                        "出现次数",
                        "出现频率(%)",
                        "_scenario_score",
                    ],
                ),
                "lost_samples": _records_for_sample(
                    lost_df,
                    [
                        "基金代码",
                        "基金名称",
                        FORMAL_GROUP_LABEL,
                        "债基风格标签",
                        FORMAL_SCORE_LABEL,
                        "出现次数",
                        "出现频率(%)",
                        "_scenario_score",
                    ],
                ),
            }
        )
    return {
        "available": True,
        "mode": "history_weight_multiplier_shadow",
        "calculation_note": (
            "该 shadow 只调整历史出现次数/频率贡献权重，保留正式评分、质量信心、新基金奖惩和 hard_block 排除。"
        ),
        "target_total": int(target_total),
        "official_count": official_count,
        "baseline_history_occurrence_weight": FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT,
        "baseline_history_frequency_weight": FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT,
        "scenarios": scenarios,
    }


def _build_hard_block_source_summary(snapshot_payload: dict[str, Any]) -> dict[str, Any]:
    removed_df = pd.DataFrame((snapshot_payload or {}).get("final_selection_removed_records") or [])
    if removed_df.empty:
        return {"available": False, "reason": "final_selection_removed_records_missing"}
    removed_df["基金代码"] = _text_series(removed_df, "基金代码").map(_normalize_code)
    removed_df["项目推荐原因代码"] = _text_series(removed_df, "项目推荐原因代码")
    removed_df["hard_block_category"] = removed_df["项目推荐原因代码"].map(_reason_category)
    hard_df = removed_df[removed_df["hard_block_category"].eq("hard_block")].copy()
    if hard_df.empty:
        return {"available": True, "hard_block_count": 0, "samples": []}
    hard_df[FORMAL_SCORE_LABEL] = _numeric_series(hard_df, FORMAL_SCORE_LABEL)
    hard_df = hard_df.sort_values(
        by=[FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[False, True],
        kind="mergesort",
    )
    return {
        "available": True,
        "hard_block_count": int(len(hard_df.index)),
        "source_assessment": "autostock_hard_block" if hard_df["AutoStock原始校验状态"].fillna("").astype(str).eq("hard_block").all() else "mixed",
        "by_style": _distribution(hard_df, "债基风格标签"),
        "by_group": _distribution(hard_df, FORMAL_GROUP_LABEL),
        "by_autostock_status": _distribution(hard_df, "AutoStock原始校验状态"),
        "samples": _records_for_sample(
            hard_df,
            [
                "基金代码",
                "基金名称",
                FORMAL_GROUP_LABEL,
                "债基风格标签",
                FORMAL_SCORE_LABEL,
                "项目推荐原因代码",
                "推荐理由",
                "推荐备注",
                "AutoStock原始校验状态",
                "AutoStock告警原因",
            ],
            limit=12,
        ),
    }


def _build_incremental_candidate_safety_review(
    df: pd.DataFrame,
    *,
    snapshot_payload: dict[str, Any],
    settings,
    target_deltas: list[int],
) -> dict[str, Any]:
    current = _prepare_scenario_frame(df)
    if current.empty:
        return {"available": False, "reason": "empty_contraction_records", "candidate_rows": []}
    target_total = int(getattr(settings, "final_selection_target_max", 0) or 0)
    if target_total <= 0:
        return {"available": False, "reason": "final_selection_target_max_not_configured", "candidate_rows": []}

    current["_scenario_score"] = current["_final_stability_score"]
    group_min_ratio = float(getattr(settings, "final_selection_group_min_ratio", 0.25) or 0.25)
    official_codes = set(current.loc[current["_is_official"], "基金代码"].astype(str).tolist())
    incremental_codes_by_delta: dict[int, set[str]] = {}
    all_incremental_codes: set[str] = set()
    for delta in target_deltas:
        scenario_codes = _select_pre_hard_block_codes(
            current,
            target_total=target_total + int(delta),
            group_min_ratio=group_min_ratio,
            score_column="_scenario_score",
        )
        safe_codes = set(current.loc[current["基金代码"].astype(str).isin(scenario_codes) & ~current["_is_hard_block"], "基金代码"].astype(str))
        incremental_codes = safe_codes - official_codes
        incremental_codes_by_delta[int(delta)] = incremental_codes
        all_incremental_codes.update(incremental_codes)
    if not all_incremental_codes:
        return {
            "available": True,
            "candidate_count": 0,
            "status_counts": [],
            "reason_counts": [],
            "governance_summary": _build_incremental_candidate_governance_summary(
                pd.DataFrame(),
                incremental_codes_by_delta=incremental_codes_by_delta,
            ),
            "candidate_rows": [],
        }

    artifact_records = _artifact_records_by_code(
        snapshot_payload,
        ["dynamic_post_review_records", "final_selection_removed_records", "final_official_records"],
    )
    db_df = _load_database_safety_rows(Path(getattr(settings, "database_path", "")), all_incremental_codes)
    db_df["基金代码"] = db_df.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    db_map = {
        str(row.get("基金代码")): row
        for row in db_df.fillna("").to_dict(orient="records")
        if _normalize_code(row.get("基金代码"))
    }
    database_path = Path(getattr(settings, "database_path", ""))
    try:
        long_term_df, _ = (
            _build_long_term_feature_frame(database_path)
            if database_path.exists() and database_path.is_file()
            else (pd.DataFrame(), {})
        )
    except Exception:
        long_term_df = pd.DataFrame()
    long_term_map: dict[str, dict[str, Any]] = {}
    if not long_term_df.empty:
        long_term_df["fcode"] = long_term_df.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
        long_term_map = {
            str(row.get("fcode")): row
            for row in long_term_df.fillna("").to_dict(orient="records")
            if _normalize_code(row.get("fcode"))
        }
    annual_summary_map = _latest_years_summary(Path(getattr(settings, "database_path", "")), all_incremental_codes)

    scenario_source = current[current["基金代码"].astype(str).isin(all_incremental_codes)].copy()
    scenario_source = scenario_source.sort_values(
        by=["_scenario_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    )
    rows: list[dict[str, Any]] = []
    for row in scenario_source.to_dict(orient="records"):
        code = _normalize_code(row.get("基金代码"))
        merged = dict(row)
        merged.update(artifact_records.get(code, {}))
        merged.update(db_map.get(code, {}))
        merged.update(long_term_map.get(code, {}))
        merged["基金代码"] = code
        merged["scenario_deltas"] = [
            int(delta)
            for delta, codes in incremental_codes_by_delta.items()
            if code in codes
        ]
        merged["first_incremental_delta"] = min(merged["scenario_deltas"]) if merged["scenario_deltas"] else None
        merged["annual_history_summary"] = annual_summary_map.get(code, {})
        status, reason_codes = _evaluate_incremental_candidate(
            merged,
            exclude_holding_days_gt=int(getattr(settings, "exclude_holding_days_gt", 90) or 90),
        )
        merged["safety_status"] = status
        merged["safety_status_label"] = _safety_status_label(status)
        merged["safety_reason_codes"] = reason_codes
        merged["safety_reason_text"] = "|".join(reason_codes)
        rows.append(merged)

    review_df = pd.DataFrame(rows)
    if review_df.empty:
        return {"available": True, "candidate_count": 0, "candidate_rows": []}
    review_df = review_df.sort_values(
        by=["first_incremental_delta", "_scenario_score", FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[True, False, False, True],
        kind="mergesort",
    )
    status_counts = _distribution(review_df, "safety_status")
    reason_counts = _reason_code_distribution_from_detail(review_df, limit=20)
    governance_summary = _build_incremental_candidate_governance_summary(
        review_df,
        incremental_codes_by_delta=incremental_codes_by_delta,
    )
    delta_summary: list[dict[str, Any]] = []
    for delta in sorted(incremental_codes_by_delta):
        delta_df = review_df[review_df["scenario_deltas"].map(lambda values, d=delta: d in (values or []))].copy()
        delta_summary.append(
            {
                "target_delta": int(delta),
                "candidate_count": int(len(delta_df.index)),
                "status_counts": _distribution(delta_df, "safety_status"),
                "reason_counts": _reason_code_distribution_from_detail(delta_df, limit=20),
                "block_count": int(delta_df["safety_status"].eq("block").sum()) if "safety_status" in delta_df.columns else 0,
                "watch_count": int(delta_df["safety_status"].eq("watch").sum()) if "safety_status" in delta_df.columns else 0,
                "pass_count": int(delta_df["safety_status"].eq("pass").sum()) if "safety_status" in delta_df.columns else 0,
            }
        )
    return {
        "available": True,
        "mode": "target_delta_incremental_safety_review",
        "calculation_note": (
            "该复核只读取发布链快照、数据库风险/费率/持有期/可申购字段和长周期特征；"
            "用于判断扩容候选是否具备发布安全性，不直接改生产阈值。"
        ),
        "candidate_count": int(len(review_df.index)),
        "status_counts": status_counts,
        "reason_counts": reason_counts,
        "delta_summary": delta_summary,
        "governance_summary": governance_summary,
        "candidate_rows": _records_for_columns(
            review_df,
            [
                "基金代码",
                "基金名称",
                FORMAL_GROUP_LABEL,
                "债基风格标签",
                "scenario_deltas",
                "first_incremental_delta",
                "safety_status",
                "safety_status_label",
                "safety_reason_codes",
                "safety_reason_text",
                FORMAL_SCORE_LABEL,
                "_scenario_score",
                "出现次数",
                "出现频率(%)",
                "基金规模(亿)",
                "费率合计(%)",
                "has_holding_period",
                "holding_days",
                "holding_period_source",
                "is_buyable",
                "申购状态",
                "AutoStock原始校验状态",
                "AutoStock告警原因",
                "最新净值日期",
                "risk_end_date",
                "risk_data_points",
                "1年-年化收益(%)",
                "1年-波动率",
                "1年-最大回撤(%)",
                "1年-夏普比率",
                "1年-卡玛比率",
                "1年-正收益比率",
                "长期稳定性标签",
                "长期稳定性特征完整",
                "长期稳定性总分",
                "annual_history_summary",
                "推荐备注",
            ],
        ),
    }


def _build_shadow_audit(
    snapshot_path: Path,
    *,
    settings,
    localized_artifacts: dict[str, Any],
    current_nav_snapshot_date: str = "",
    target_deltas: list[int] | None = None,
    history_weight_multipliers: list[float] | None = None,
) -> dict[str, Any]:
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    rows = payload.get("final_selection_contraction_records") or []
    if not isinstance(rows, list):
        rows = []
    records_source = "publish_chain_snapshot_records"
    tracker_feature_source = str(payload.get("tracker_feature_source") or "").strip()
    df = pd.DataFrame(rows)
    if df.empty:
        official_df = pd.DataFrame(payload.get("final_official_records") or [])
        removed_df = pd.DataFrame(payload.get("final_selection_removed_records") or [])
        candidate_df = pd.DataFrame(payload.get("dynamic_post_review_records") or [])
        if candidate_df.empty:
            candidate_df = pd.concat([official_df, removed_df], ignore_index=True)
        tracker_features_df = pd.DataFrame()
        if not candidate_df.empty:
            tracker_features_df = _build_final_selection_tracker_features(
                settings,
                current_run_id=str(payload.get("run_id") or "").strip(),
                current_generated_at=str(payload.get("generated_at") or "").strip(),
                current_nav_snapshot_date=str(current_nav_snapshot_date or "").strip(),
                current_df=candidate_df,
            )
            if not tracker_features_df.empty:
                tracker_feature_source = "canonical_history_rebuild"
        if tracker_features_df.empty:
            tracker_features_df = _load_lightweight_tracker_features_for_contraction(
                settings=settings,
                localized_artifacts=localized_artifacts,
            )
            if not tracker_features_df.empty:
                tracker_feature_source = "lightweight_tracker_workbook"
        if not candidate_df.empty:
            df = _build_final_selection_contraction_explainability(
                candidate_df,
                official_df=official_df,
                removed_after_final_selection_df=removed_df,
                tracker_features_df=tracker_features_df,
                settings=settings,
            )
            records_source = (
                "publish_chain_snapshot_rebuild_history_features"
                if not tracker_features_df.empty
                else "publish_chain_snapshot_rebuild_snapshot_features"
            )
    if df.empty:
        return {
            "available": False,
            "reason": "final_selection_contraction_records_missing",
            "records_source": records_source,
            "style_summary": [],
            "reason_code_summary": [],
            "top_removed_samples": [],
            "boundary_selected_samples": [],
            "target_expansion_shadow": {"available": False, "reason": "final_selection_contraction_records_missing"},
            "history_weight_sensitivity_shadow": {"available": False, "reason": "final_selection_contraction_records_missing"},
            "incremental_candidate_safety_review": {"available": False, "reason": "final_selection_contraction_records_missing"},
            "boundary_quality_shadow": {"available": False, "reason": "final_selection_contraction_records_missing"},
            "hard_block_source_summary": _build_hard_block_source_summary(payload),
        }

    df["基金代码"] = df.get("基金代码", pd.Series(dtype=object)).astype(str).str.strip()
    df["债基风格标签"] = df.get("债基风格标签", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    df["正式精选状态"] = df.get("正式精选状态", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    df["最终精选原因代码"] = df.get("最终精选原因代码", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    df["最终精选原因分类"] = df["最终精选原因代码"].map(_reason_category)
    df["_final_stability_score_num"] = pd.to_numeric(df.get("_final_stability_score"), errors="coerce")
    df["正式投资评分_num"] = pd.to_numeric(df.get("正式投资评分"), errors="coerce")
    df["出现次数_num"] = pd.to_numeric(df.get("出现次数"), errors="coerce")
    df["出现频率_num"] = pd.to_numeric(df.get("出现频率(%)"), errors="coerce")

    style_summary: list[dict[str, Any]] = []
    style_reason_summary: list[dict[str, Any]] = []
    style_action_summary: list[dict[str, Any]] = []
    for style in ("中短债", "综合纯债", "固收指数"):
        style_df = df[df["债基风格标签"].eq(style)].copy()
        if style_df.empty:
            continue
        selected_df = style_df[style_df["正式精选状态"].eq("正式入围")].copy()
        removed_df = style_df[style_df["正式精选状态"].eq("最终精选未入围")].copy()
        selected_min = selected_df["_final_stability_score_num"].min()
        removed_max = removed_df["_final_stability_score_num"].max()
        category_counts = removed_df["最终精选原因分类"].value_counts().to_dict()
        category_max_scores: dict[str, float | None] = {}
        for category in ("hard_block", "final_selection_stability_cap", "other_publish_review", "missing_reason"):
            category_df = removed_df[removed_df["最终精选原因分类"].eq(category)]
            max_score = category_df["_final_stability_score_num"].max() if not category_df.empty else None
            category_max_scores[category] = None if max_score is None or pd.isna(max_score) else round(float(max_score), 4)
            if not category_df.empty:
                style_reason_summary.append(
                    {
                        "style": style,
                        "reason_category": category,
                        "reason_category_label": _category_label(category),
                        "removed_count": int(len(category_df.index)),
                        "removed_max_stability_score": category_max_scores[category],
                        "top_removed_codes": category_df.sort_values(
                            by=["_final_stability_score_num", "正式投资评分_num", "出现次数_num", "基金代码"],
                            ascending=[False, False, False, True],
                            kind="mergesort",
                        ).head(5)["基金代码"].tolist(),
                    }
                )
        selected_min_value = None if pd.isna(selected_min) else round(float(selected_min), 4)
        removed_max_value = None if pd.isna(removed_max) else round(float(removed_max), 4)
        recommended_action = _style_action(
            selected_min=_safe_score(selected_min),
            stability_removed_max=category_max_scores.get("final_selection_stability_cap"),
            hard_block_removed_max=category_max_scores.get("hard_block"),
            stability_removed_count=int(category_counts.get("final_selection_stability_cap", 0)),
            hard_block_removed_count=int(category_counts.get("hard_block", 0)),
        )
        style_summary.append(
            {
                "style": style,
                "input_count": int(len(style_df.index)),
                "official_count": int(len(selected_df.index)),
                "removed_count": int(len(removed_df.index)),
                "hard_block_removed_count": int(category_counts.get("hard_block", 0)),
                "stability_cap_removed_count": int(category_counts.get("final_selection_stability_cap", 0)),
                "other_review_removed_count": int(category_counts.get("other_publish_review", 0)),
                "selected_min_stability_score": selected_min_value,
                "removed_max_stability_score": removed_max_value,
                "hard_block_removed_max_stability_score": category_max_scores.get("hard_block"),
                "stability_cap_removed_max_stability_score": category_max_scores.get("final_selection_stability_cap"),
                "stability_score_gap": (
                    None
                    if pd.isna(selected_min) or pd.isna(removed_max)
                    else round(float(selected_min) - float(removed_max), 4)
                ),
                "recommended_action": recommended_action,
                "top_removed_codes": removed_df.sort_values(
                    by=["_final_stability_score_num", "正式投资评分_num", "出现次数_num", "基金代码"],
                    ascending=[False, False, False, True],
                    kind="mergesort",
                ).head(5)["基金代码"].tolist(),
                "top_boundary_selected_codes": selected_df.sort_values(
                    by=["_final_stability_score_num", "正式投资评分_num", "出现次数_num", "基金代码"],
                    ascending=[True, True, True, True],
                    kind="mergesort",
                ).head(5)["基金代码"].tolist(),
            }
        )
        style_action_summary.append(
            {
                "style": style,
                "recommended_action": recommended_action,
                "reason": (
                    "高分未入围样本存在发布后硬拦截，需先解释 hard_block。"
                    if recommended_action == "explain_hard_block_before_threshold_change"
                    else (
                        "稳定性 cap 边界距离较窄，适合继续做局部稳定性参数 shadow。"
                        if recommended_action == "review_stability_cap_boundary"
                        else "暂不建议调整生产参数。"
                    )
                ),
            }
        )

    reason_series = df[df["正式精选状态"].eq("最终精选未入围")]["最终精选原因代码"].fillna("").astype(str)
    reason_code_summary = [
        {"reason_code": str(reason_code), "count": int(count)}
        for reason_code, count in reason_series.value_counts().items()
    ]
    incremental_candidate_safety_review = _build_incremental_candidate_safety_review(
        df,
        snapshot_payload=payload,
        settings=settings,
        target_deltas=target_deltas or list(DEFAULT_TARGET_DELTAS),
    )
    incremental_detail_map = {
        _normalize_code(row.get("基金代码")): row
        for row in incremental_candidate_safety_review.get("candidate_rows", [])
        if isinstance(row, dict) and _normalize_code(row.get("基金代码"))
    }

    top_removed = (
        df[df["正式精选状态"].eq("最终精选未入围")]
        .sort_values(
            by=["_final_stability_score_num", "正式投资评分_num", "出现次数_num", "基金代码"],
            ascending=[False, False, False, True],
            kind="mergesort",
        )
        .head(15)
    )
    boundary_selected = (
        df[df["正式精选状态"].eq("正式入围")]
        .sort_values(
            by=["_final_stability_score_num", "正式投资评分_num", "出现次数_num", "基金代码"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        .head(15)
    )

    return {
        "available": True,
        "run_id": payload.get("run_id"),
        "content_source_run_id": payload.get("content_source_run_id"),
        "report_runtime_run_id": payload.get("report_runtime_run_id"),
        "snapshot_scope": payload.get("snapshot_scope"),
        "tracker_feature_source": tracker_feature_source,
        "records_source": records_source,
        "style_summary": style_summary,
        "style_reason_summary": style_reason_summary,
        "style_action_summary": style_action_summary,
        "reason_code_summary": reason_code_summary,
        "target_expansion_shadow": _build_target_expansion_shadow(
            df,
            settings=settings,
            target_deltas=target_deltas or list(DEFAULT_TARGET_DELTAS),
            incremental_detail_map=incremental_detail_map,
        ),
        "history_weight_sensitivity_shadow": _build_history_weight_sensitivity_shadow(
            df,
            settings=settings,
            multipliers=history_weight_multipliers or list(DEFAULT_HISTORY_WEIGHT_MULTIPLIERS),
        ),
        "incremental_candidate_safety_review": incremental_candidate_safety_review,
        "boundary_quality_shadow": _build_boundary_quality_shadow(
            pd.DataFrame(incremental_candidate_safety_review.get("candidate_rows") or []),
            contraction_df=df,
        ),
        "hard_block_source_summary": _build_hard_block_source_summary(payload),
        "top_removed_samples": _records_for_columns(
            top_removed,
            [
                "基金代码",
                "基金名称",
                "债基风格标签",
                "正式精选状态",
                "最终精选原因代码",
                "最终精选原因分类",
                "正式投资评分",
                "出现次数",
                "出现频率(%)",
                "_final_stability_score",
                "最终精选解释",
            ],
        ),
        "boundary_selected_samples": _records_for_columns(
            boundary_selected,
            [
                "基金代码",
                "基金名称",
                "债基风格标签",
                "正式精选状态",
                "最终精选原因代码",
                "最终精选原因分类",
                "正式投资评分",
                "出现次数",
                "出现频率(%)",
                "_final_stability_score",
                "最终精选解释",
            ],
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="审计最终精选收敛的风格边界。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    parser.add_argument(
        "--target-deltas",
        default=",".join(str(item) for item in DEFAULT_TARGET_DELTAS),
        help="逗号分隔的 target_max 扩容 shadow 增量；默认 5,10,20。",
    )
    parser.add_argument(
        "--history-weight-multipliers",
        default=",".join(str(item) for item in DEFAULT_HISTORY_WEIGHT_MULTIPLIERS),
        help="逗号分隔的历史稳定性权重 shadow 倍数；默认 0.6,0.8,1.2。",
    )
    args = parser.parse_args()
    target_deltas = _parse_int_list(args.target_deltas, DEFAULT_TARGET_DELTAS)
    history_weight_multipliers = _parse_float_list(
        args.history_weight_multipliers,
        DEFAULT_HISTORY_WEIGHT_MULTIPLIERS,
    )

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    try:
        run_id, payload, artifacts, replay_precision = _resolve_run_payload(
            settings,
            run_id=args.run_id,
            project_root=project_root,
        )
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": str(exc),
                    "latest_report_file": str(settings.latest_report_file),
                    "requested_run_id": str(args.run_id or "").strip(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    snapshot_value, snapshot_path = _resolve_artifact_project_path(
        artifacts,
        "publish_chain_snapshot_json",
        project_root=project_root,
    )
    if not snapshot_value:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "publish_chain_snapshot_json_missing",
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    if snapshot_path is None or not snapshot_path.exists():
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "publish_chain_snapshot_json_not_found",
                    "publish_chain_snapshot_json": snapshot_value,
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    audit = _build_shadow_audit(
        snapshot_path,
        settings=settings,
        localized_artifacts=artifacts,
        current_nav_snapshot_date=str(
            (((payload.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("candidate_date") or "")
        ).strip(),
        target_deltas=target_deltas,
        history_weight_multipliers=history_weight_multipliers,
    )
    payload_out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_report_file": str(settings.latest_report_file.relative_to(settings.project_root)),
        "source_report_run_id": _clean_text(payload.get("run_id")),
        "publish_chain_snapshot_json": str(snapshot_path.relative_to(settings.project_root)),
        "replay_precision": replay_precision,
        "final_selection_contraction_shadow": audit,
    }

    audit_dir = settings.reports_dir / "audit" / "final_selection_contraction_shadow"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "final_selection_contraction_shadow",
        run_id=run_id,
    )
    artifact_path.write_text(json.dumps(payload_out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "passed": bool(audit.get("available")),
                "run_id": run_id,
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(settings.project_root)),
                "tracker_feature_source": audit.get("tracker_feature_source"),
                "records_source": audit.get("records_source"),
                "style_summary": audit.get("style_summary"),
                "style_reason_summary": audit.get("style_reason_summary"),
                "style_action_summary": audit.get("style_action_summary"),
                "reason_code_summary": audit.get("reason_code_summary"),
                "target_expansion_shadow": audit.get("target_expansion_shadow"),
                "history_weight_sensitivity_shadow": audit.get("history_weight_sensitivity_shadow"),
                "incremental_candidate_safety_review": audit.get("incremental_candidate_safety_review"),
                "boundary_quality_shadow": audit.get("boundary_quality_shadow"),
                "hard_block_source_summary": audit.get("hard_block_source_summary"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if audit.get("available") else 1


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
