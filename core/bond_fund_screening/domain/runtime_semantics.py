#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""运行时语义单一事实来源辅助。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

import pandas as pd


CanonicalFundType = Literal[
    "bond",
    "bond_fixed",
    "stock",
    "hybrid",
    "index",
    "money",
    "qdii",
    "fof",
]


@dataclass(frozen=True)
class FundRuntimeSemantics:
    """单只基金运行时语义快照。"""

    source_type: str
    canonical_type: CanonicalFundType
    effective_profile_type: CanonicalFundType
    display_type: str
    benchmark_family: Optional[str]
    score_key: Optional[str]
    score_key_reason: Optional[str]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _infer_benchmark_family(benchmark: Any) -> Optional[str]:
    benchmark_text = _normalize_text(benchmark)
    if not benchmark_text:
        return None

    if any(token in benchmark_text for token in ("恒生", "纳斯达克", "标普", "MSCI", "日经")):
        return "overseas_index"
    if any(token in benchmark_text for token in ("中债", "国债", "信用债", "政策性金融债", "存款利率")):
        return "bond_index"
    if any(token in benchmark_text for token in ("沪深", "中证", "上证", "深证", "创业板", "科创")):
        return "equity_index"
    return "other"


def _resolve_canonical_type(
    *,
    source_type: str,
    type_code: Any,
    type_category: Any,
) -> CanonicalFundType:
    normalized_source_type = _normalize_text(source_type)
    normalized_code = _normalize_text(type_code)
    normalized_category = _normalize_text(type_category).lower()
    lowered_source_type = normalized_source_type.lower()

    if normalized_code == "32":
        return "bond_fixed"
    if normalized_code == "31" or normalized_category == "fixed_income":
        return "bond"
    if normalized_code == "26":
        return "index"
    if normalized_code == "25":
        return "stock"
    if normalized_code == "27" or normalized_category == "hybrid":
        return "hybrid"
    if normalized_code == "35" or normalized_category == "money_market":
        return "money"
    if normalized_code == "36" or normalized_category == "alternative":
        return "qdii"
    if normalized_code == "37" or normalized_category == "multi_asset":
        return "fof"

    if normalized_source_type.startswith("QDII") or lowered_source_type.startswith("qdii"):
        return "qdii"
    if normalized_source_type.startswith("FOF") or lowered_source_type.startswith("fof"):
        return "fof"
    if normalized_source_type.startswith("货币"):
        return "money"
    if normalized_source_type.startswith("混合"):
        return "hybrid"
    if normalized_source_type == "指数型" or normalized_source_type.startswith("指数型-"):
        return "index"
    if normalized_source_type.startswith("股票"):
        return "stock"
    if normalized_source_type.startswith(("定开债",)):
        return "bond_fixed"
    if normalized_source_type.startswith(("债券", "理财", "短期理财")):
        return "bond"

    return "bond"


def derive_runtime_semantics(
    *,
    source_type: Any,
    type_code: Any = None,
    type_category: Any = None,
    benchmark: Any = None,
    selected_score_key: Any = None,
    selected_score_reason: Any = None,
) -> FundRuntimeSemantics:
    """基于当前行字段推导运行时语义。"""

    normalized_source_type = _normalize_text(source_type)
    display_type = normalized_source_type or "未知"
    canonical_type = _resolve_canonical_type(
        source_type=normalized_source_type,
        type_code=type_code,
        type_category=type_category,
    )

    # 首阶段只固化语义，不改现有 profile 路由；因此 effective_profile_type 默认与 canonical 一致。
    effective_profile_type = canonical_type

    return FundRuntimeSemantics(
        source_type=normalized_source_type or display_type,
        canonical_type=canonical_type,
        effective_profile_type=effective_profile_type,
        display_type=display_type,
        benchmark_family=_infer_benchmark_family(benchmark),
        score_key=_normalize_text(selected_score_key) or None,
        score_key_reason=_normalize_text(selected_score_reason) or None,
    )


def attach_runtime_semantics_columns(df: pd.DataFrame) -> pd.DataFrame:
    """为 DataFrame 挂载运行时语义列。"""
    if df.empty:
        result = df.copy()
        for column in (
            "runtime_source_type",
            "canonical_type",
            "effective_profile_type",
            "display_type",
            "benchmark_family",
            "runtime_score_key",
            "runtime_score_key_reason",
        ):
            if column not in result.columns:
                result[column] = pd.Series(dtype="object")
        return result

    result = df.copy()
    semantics_series = result.apply(
        lambda row: derive_runtime_semantics(
            source_type=row.get("ftype"),
            type_code=row.get("type_code"),
            type_category=row.get("type_category"),
            benchmark=row.get("benchmark"),
            selected_score_key=row.get("selected_score_key"),
            selected_score_reason=row.get("selected_score_reason"),
        ),
        axis=1,
    )

    result["runtime_source_type"] = semantics_series.map(lambda item: item.source_type)
    result["canonical_type"] = semantics_series.map(lambda item: item.canonical_type)
    result["effective_profile_type"] = semantics_series.map(lambda item: item.effective_profile_type)
    result["display_type"] = semantics_series.map(lambda item: item.display_type)
    result["benchmark_family"] = semantics_series.map(lambda item: item.benchmark_family)
    result["runtime_score_key"] = semantics_series.map(lambda item: item.score_key)
    result["runtime_score_key_reason"] = semantics_series.map(lambda item: item.score_key_reason)
    return result
