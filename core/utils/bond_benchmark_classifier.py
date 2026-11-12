#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
债券型名称 / benchmark 语义分类辅助。

仅覆盖已做过真实样本交叉验证的高置信子集：
- 可转债
- 中短债（只收显式 0-3 年 / 短债指数 / 名称含短债且 benchmark 明示 1 年以下）
- 长债（仅显式利率债）
"""

from __future__ import annotations

from typing import Optional


CONVERTIBLE_NAME_MARKERS = (
    "可转债",
    "转债",
)
CONVERTIBLE_BENCHMARK_MARKERS = (
    "可转换债券",
    "可转债",
)
INTEREST_RATE_NAME_MARKERS = (
    "利率债",
)
SHORT_NAME_MARKERS = (
    "短债",
    "中短债",
)
SHORT_DURATION_MARKERS = (
    "0-3年",
)
SHORT_BENCHMARK_WITH_NAME_MARKERS = (
    "1年以下",
    "短债指数",
)

HIGH_CONFIDENCE_BOND_ACTIONS = {
    "high_confidence_bond_convertible_candidate",
    "high_confidence_bond_short_duration_candidate",
    "high_confidence_bond_interest_rate_candidate",
}


def _normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip()


def _contains_any(text: str, markers) -> bool:
    return any(marker in text for marker in markers)


def classify_bond_unclassified_candidate(
    shortname: Optional[str],
    fullname: Optional[str],
    benchmark: Optional[str],
) -> dict:
    """
    基于名称 / benchmark 对债券型低信息量样本做高置信运行时细分。

    规则保持保守：
    - `纯债 / 中短债` 等名称与上游现有语义标签仍存在冲突，不直接自动细分；
    - 只在当前已验证无冲突的规则上自动落子，其余保持未分类。
    """
    name_text = " ".join(
        part for part in (_normalize_text(shortname), _normalize_text(fullname)) if part
    )
    benchmark_text = _normalize_text(benchmark)

    proposed_action = "manual_review"
    proposed_semantic_ftype = None
    confidence = "low"
    matched_rule = None
    reason = "名称 / benchmark 仍不足以高置信细分，保持债券型未分类。"

    if _contains_any(name_text, CONVERTIBLE_NAME_MARKERS) or _contains_any(
        benchmark_text, CONVERTIBLE_BENCHMARK_MARKERS
    ):
        proposed_action = "high_confidence_bond_convertible_candidate"
        proposed_semantic_ftype = "债券型-可转债"
        confidence = "high"
        matched_rule = "convertible_name_or_benchmark"
        reason = "名称或 benchmark 明确包含可转债信号，可高置信细分为可转债。"
    elif _contains_any(name_text, SHORT_DURATION_MARKERS) or _contains_any(
        benchmark_text, SHORT_DURATION_MARKERS
    ):
        proposed_action = "high_confidence_bond_short_duration_candidate"
        proposed_semantic_ftype = "债券型-中短债"
        confidence = "high"
        matched_rule = "explicit_zero_to_three_year_duration"
        reason = "名称或 benchmark 明确包含 0-3 年久期信号，可高置信细分为中短债。"
    elif _contains_any(name_text, SHORT_NAME_MARKERS) and (
        _contains_any(benchmark_text, SHORT_DURATION_MARKERS)
        or _contains_any(benchmark_text, SHORT_BENCHMARK_WITH_NAME_MARKERS)
    ):
        proposed_action = "high_confidence_bond_short_duration_candidate"
        proposed_semantic_ftype = "债券型-中短债"
        confidence = "high"
        matched_rule = "short_bond_name_with_short_duration_benchmark"
        reason = "名称显式短债且 benchmark 明示短久期，可高置信细分为中短债。"
    elif _contains_any(name_text, INTEREST_RATE_NAME_MARKERS):
        proposed_action = "high_confidence_bond_interest_rate_candidate"
        proposed_semantic_ftype = "债券型-长债"
        confidence = "high"
        matched_rule = "interest_rate_bond_name"
        reason = "名称显式为利率债，当前样本验证可高置信归入长债。"

    return {
        "confidence": confidence,
        "matched_rule": matched_rule,
        "name_text": name_text,
        "benchmark_text": benchmark_text,
        "proposed_action": proposed_action,
        "proposed_semantic_ftype": proposed_semantic_ftype,
        "reason": reason,
    }


def infer_high_confidence_bond_semantic_ftype(
    shortname: Optional[str],
    fullname: Optional[str] = None,
    benchmark: Optional[str] = None,
) -> Optional[str]:
    """仅返回高置信可自动应用的债券型语义子类。"""
    classification = classify_bond_unclassified_candidate(
        shortname=shortname,
        fullname=fullname,
        benchmark=benchmark,
    )
    if classification.get("proposed_action") not in HIGH_CONFIDENCE_BOND_ACTIONS:
        return None
    return classification.get("proposed_semantic_ftype")
