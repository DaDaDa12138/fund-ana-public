from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


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


def _share_pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(float(part) / float(total) * 100.0, 2)


def _review_basis_summary(
    sample_df: pd.DataFrame,
    *,
    suggestion: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if sample_df.empty or "复审建议" not in sample_df.columns:
        return []
    current = sample_df[
        sample_df.get("复审建议", pd.Series(dtype=object)).fillna("").astype(str).eq(suggestion)
    ].copy()
    if current.empty:
        return []
    counter: Counter[str] = Counter()
    for raw_basis in current.get("复审依据", pd.Series(dtype=object)).fillna("").astype(str):
        parts = [part.strip() for part in raw_basis.split("；") if part.strip()]
        if not parts and raw_basis.strip():
            parts = [raw_basis.strip()]
        for part in parts:
            counter[part] += 1
    return [
        {
            "basis": basis,
            "count": int(count),
        }
        for basis, count in counter.most_common(limit)
    ]


def _build_fee_combo_breakdown(sample_df: pd.DataFrame) -> list[dict[str, Any]]:
    if sample_df.empty:
        return []
    counter: Counter[str] = Counter()
    for raw_codes in sample_df.get("原始原因代码", pd.Series(dtype=object)).fillna("").astype(str):
        codes = {item.strip() for item in raw_codes.split("|") if item.strip()}
        if "fee_too_high" not in codes:
            continue
        has_scale = "scale_too_small" in codes
        has_other_quality = any(
            code not in {"fee_too_high", "scale_too_small"} for code in codes
        )
        if not has_scale and not has_other_quality:
            combo_key = "fee_only"
            combo_label = "仅费率门槛"
        elif has_scale and not has_other_quality:
            combo_key = "fee_plus_scale_only"
            combo_label = "费率+规模"
        elif has_scale and has_other_quality:
            combo_key = "fee_plus_scale_and_quality"
            combo_label = "费率+规模+其他质量因子"
        else:
            combo_key = "fee_plus_other_quality"
            combo_label = "费率+其他质量因子"
        counter[(combo_key, combo_label)] += 1
    total = int(len(sample_df.index))
    rows = [
        {
            "combo_key": combo_key,
            "combo_label": combo_label,
            "count": int(count),
            "share_pct": _share_pct(int(count), total),
        }
        for (combo_key, combo_label), count in counter.most_common()
    ]
    rows.sort(key=lambda item: (-int(item["count"]), str(item["combo_key"])))
    return rows


def build_high_score_friction_gate_review(
    *,
    high_score_threshold: float,
    high_score_friction_reason_df: pd.DataFrame,
    high_score_friction_style_df: pd.DataFrame,
    high_score_friction_sample_df: pd.DataFrame,
    high_score_scale_gate_sample_df: pd.DataFrame,
    high_score_scale_gate_near_df: pd.DataFrame,
    high_score_scale_gate_near_review_df: pd.DataFrame,
    high_score_scale_gate_near_review_sample_df: pd.DataFrame,
    high_score_fee_gate_style_df: pd.DataFrame,
    high_score_fee_gate_sample_df: pd.DataFrame,
) -> dict[str, Any]:
    friction_total = int(len(high_score_friction_sample_df.index))
    scale_total = int(len(high_score_scale_gate_sample_df.index))
    scale_near_total = int(len(high_score_scale_gate_near_df.index))
    fee_total = int(len(high_score_fee_gate_sample_df.index))
    candidate_penalty_total = _to_int(
        high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object))
        .eq("候选惩罚项复审")
        .sum()
    )
    pending_data_verify_total = _to_int(
        high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object))
        .eq("待核实数据/份额结构")
        .sum()
    )
    hard_block_total = _to_int(
        high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object))
        .eq("继续硬拦截")
        .sum()
    )

    dominant_reason = {}
    if not high_score_friction_reason_df.empty:
        top_reason = high_score_friction_reason_df.iloc[0].to_dict()
        dominant_reason = {
            "label": _clean_text(top_reason.get("主排除原因")) or None,
            "count": _to_int(top_reason.get("样本数")),
            "share_pct": _to_float(top_reason.get("占高分摩擦样本比例(%)")),
        }

    dominant_style = {}
    if not high_score_friction_style_df.empty:
        top_style = high_score_friction_style_df.iloc[0].to_dict()
        dominant_style = {
            "label": _clean_text(top_style.get("债基风格标签")) or None,
            "count": _to_int(top_style.get("样本数")),
            "share_pct": _to_float(top_style.get("占高分摩擦样本比例(%)")),
        }

    if friction_total <= 0:
        status = "no_friction_cases"
        recommended_action = "no_high_score_friction_action_required"
        reason = "当前不存在高分摩擦样本，无需单独推进规模/费率门禁复核。"
    elif candidate_penalty_total > 0:
        status = "actionable_threshold_review_required"
        recommended_action = "review_high_score_friction_gates"
        reason = "存在满足候选惩罚项复审条件的近门槛样本，应先复核规模门槛职责边界。"
    elif pending_data_verify_total > 0:
        status = "data_verification_only"
        recommended_action = "verify_high_score_share_or_scale_data"
        reason = "当前未发现支持直接放宽摩擦门槛的候选样本，但仍有份额结构或规模数据待核实。"
    else:
        status = "supported_hard_gate"
        recommended_action = "keep_friction_gates_and_observe"
        reason = "高分摩擦样本主要由规模/费率硬门槛构成，近门槛样本未显示放宽门槛的直接证据。"

    dominant_gate = "mixed"
    if scale_total > fee_total:
        dominant_gate = "scale"
    elif fee_total > scale_total:
        dominant_gate = "fee"

    return {
        "available": True,
        "status": status,
        "high_score_threshold": float(high_score_threshold),
        "high_score_friction_count": friction_total,
        "dominant_friction_gate": dominant_gate,
        "dominant_friction_reason": dominant_reason,
        "dominant_friction_style": dominant_style,
        "reason_preview": _rows(high_score_friction_reason_df, limit=5),
        "style_preview": _rows(high_score_friction_style_df, limit=8),
        "sample_preview": _rows(high_score_friction_sample_df, limit=12),
        "scale_gate": {
            "total": scale_total,
            "near_threshold_total": scale_near_total,
            "near_threshold_share_within_scale_gate_pct": _share_pct(
                scale_near_total, scale_total
            ),
            "candidate_penalty_total": candidate_penalty_total,
            "pending_data_verify_total": pending_data_verify_total,
            "hard_block_total": hard_block_total,
            "review_breakdown": _rows(high_score_scale_gate_near_review_df, limit=5),
            "near_threshold_sample_preview": _rows(high_score_scale_gate_near_df, limit=12),
            "review_sample_preview": _rows(
                high_score_scale_gate_near_review_sample_df, limit=12
            ),
            "pending_data_verify_basis_summary": _review_basis_summary(
                high_score_scale_gate_near_review_sample_df,
                suggestion="待核实数据/份额结构",
            ),
            "hard_block_basis_summary": _review_basis_summary(
                high_score_scale_gate_near_review_sample_df,
                suggestion="继续硬拦截",
            ),
        },
        "fee_gate": {
            "total": fee_total,
            "style_preview": _rows(high_score_fee_gate_style_df, limit=8),
            "sample_preview": _rows(high_score_fee_gate_sample_df, limit=12),
            "combo_breakdown": _build_fee_combo_breakdown(high_score_fee_gate_sample_df),
        },
        "judgement": {
            "status": status,
            "recommended_action": recommended_action,
            "actionable_threshold_review_total": candidate_penalty_total,
            "pending_data_verify_total": pending_data_verify_total,
            "direct_threshold_relaxation_recommended_now": False,
            "reason": reason,
        },
    }


def load_high_score_friction_gate_review_from_workbook(
    workbook_path: Path,
    *,
    high_score_threshold: float,
) -> dict[str, Any]:
    path = Path(workbook_path)
    if not path.exists():
        raise FileNotFoundError(path)

    def _read_sheet(name: str) -> pd.DataFrame:
        try:
            return pd.read_excel(path, sheet_name=name)
        except ValueError:
            return pd.DataFrame()

    return build_high_score_friction_gate_review(
        high_score_threshold=high_score_threshold,
        high_score_friction_reason_df=_read_sheet("高分摩擦主因"),
        high_score_friction_style_df=_read_sheet("高分摩擦风格"),
        high_score_friction_sample_df=_read_sheet("高分摩擦样本"),
        high_score_scale_gate_sample_df=_read_sheet("高分规模门槛样本"),
        high_score_scale_gate_near_df=_read_sheet("高分近门槛样本"),
        high_score_scale_gate_near_review_df=_read_sheet("近门槛复审结论"),
        high_score_scale_gate_near_review_sample_df=_read_sheet("近门槛复审样本"),
        high_score_fee_gate_style_df=_read_sheet("高分费率门槛风格"),
        high_score_fee_gate_sample_df=_read_sheet("高分费率门槛样本"),
    )
