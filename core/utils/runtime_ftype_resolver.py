#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataLoader 运行时 ftype 纠偏辅助。
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from utils.runtime_entry_guards import build_name_contains_mask
from utils.bond_benchmark_classifier import infer_high_confidence_bond_semantic_ftype
from utils.fund_type_filters import infer_high_confidence_qdii_semantic_ftype
from utils.hybrid_benchmark_classifier import infer_high_confidence_hybrid_semantic_ftype


def normalize_numeric_ftype_labels(df: pd.DataFrame, fund_type_mapping: dict) -> pd.DataFrame:
    """把历史数字 ftype 映射为中文标签。"""
    result = df.copy()
    result["ftype"] = result["ftype"].apply(
        lambda value: fund_type_mapping.get(str(value), fund_type_mapping.get(value, value))
        if pd.notna(value) else "未知"
    )
    return result


def normalize_bare_primary_ftype_labels(df: pd.DataFrame) -> pd.DataFrame:
    """把裸主类型标记为未分类，避免伪造子类型。"""
    result = df.copy()

    def _normalize_bare_ftype(ftype):
        if isinstance(ftype, str) and ftype in ("债券型", "混合型", "货币型", "指数型"):
            return f"{ftype}-未分类"
        return ftype

    result["ftype"] = result["ftype"].apply(_normalize_bare_ftype)
    return result


def _has_columns(df: pd.DataFrame, *columns: str) -> bool:
    return all(column in df.columns for column in columns)


def apply_runtime_ftype_overlays(
    df: pd.DataFrame,
    *,
    is_qdii_request: bool,
) -> pd.DataFrame:
    """
    应用运行时 ftype 叠加逻辑。

    约束：
    - 只修改运行时 DataFrame，不回写数据库；
    - 保持现有 DataLoader 行为不变，仅做逻辑收口。
    """
    result = df.copy()

    if _has_columns(result, "type_code", "type_category", "ftype"):
        structured_qdii_runtime_mask = (
            result["type_code"].astype(str).eq("36")
            & result["type_category"].astype(str).eq("alternative")
        )
        semantic_qdii_runtime_mask = (
            result["ftype"].astype(str).str.startswith("QDII")
            | result["ftype"].astype(str).str.startswith("指数型-海外")
        )
        generic_qdii_runtime_mask = structured_qdii_runtime_mask & ~semantic_qdii_runtime_mask
        if generic_qdii_runtime_mask.any():
            result.loc[generic_qdii_runtime_mask, "ftype"] = "QDII-未分类"

    if is_qdii_request and "type_code" in result.columns:
        qdii_name_mask = build_name_contains_mask(result, "QDII")
        explicit_qdii_runtime_mask = qdii_name_mask & result["type_code"].astype(str).isin(["3", "4"])
        if explicit_qdii_runtime_mask.any():
            result.loc[explicit_qdii_runtime_mask, "ftype"] = "QDII-未分类"
            result.loc[explicit_qdii_runtime_mask, "type_code"] = "36"
            if "type_category" in result.columns:
                result.loc[explicit_qdii_runtime_mask, "type_category"] = "alternative"

    qdii_runtime_refine_mask = (
        result.get("type_code").astype(str).eq("36")
        & result.get("type_category").astype(str).eq("alternative")
        & result["ftype"].eq("QDII-未分类")
    )
    if qdii_runtime_refine_mask.any():
        inferred_qdii_ftypes = result.loc[qdii_runtime_refine_mask].apply(
            lambda row: infer_high_confidence_qdii_semantic_ftype(
                row.get("shortname"),
                row.get("fullname"),
                row.get("benchmark"),
            ),
            axis=1,
        )
        inferred_qdii_ftypes = inferred_qdii_ftypes.dropna()
        if not inferred_qdii_ftypes.empty:
            result.loc[inferred_qdii_ftypes.index, "ftype"] = inferred_qdii_ftypes

    fof_name_mask = build_name_contains_mask(result, "FOF")
    explicit_fof_runtime_mask = fof_name_mask & result["ftype"].eq("混合型-未分类")
    if explicit_fof_runtime_mask.any():
        result.loc[explicit_fof_runtime_mask, "ftype"] = "FOF-未分类"
        if "type_code" in result.columns:
            result.loc[explicit_fof_runtime_mask, "type_code"] = "37"
        if "type_category" in result.columns:
            result.loc[explicit_fof_runtime_mask, "type_category"] = "multi_asset"

    multi_asset_fof_runtime_mask = (
        result.get("type_code").astype(str).eq("37")
        & result.get("type_category").astype(str).eq("multi_asset")
        & result["ftype"].eq("混合型-未分类")
    )
    if multi_asset_fof_runtime_mask.any():
        result.loc[multi_asset_fof_runtime_mask, "ftype"] = "FOF-未分类"

    hybrid_runtime_refine_mask = (
        result.get("type_code").astype(str).eq("27")
        & result.get("type_category").astype(str).eq("hybrid")
        & result["ftype"].eq("混合型-未分类")
    )
    if hybrid_runtime_refine_mask.any():
        inferred_hybrid_ftypes = result.loc[hybrid_runtime_refine_mask].apply(
            lambda row: infer_high_confidence_hybrid_semantic_ftype(
                row.get("shortname"),
                row.get("fullname"),
                row.get("benchmark"),
            ),
            axis=1,
        )
        inferred_hybrid_ftypes = inferred_hybrid_ftypes.dropna()
        if not inferred_hybrid_ftypes.empty:
            result.loc[inferred_hybrid_ftypes.index, "ftype"] = inferred_hybrid_ftypes

    bond_runtime_refine_mask = (
        result.get("type_code").astype(str).isin(["31", "32"])
        & result.get("type_category").astype(str).eq("fixed_income")
        & result["ftype"].isin(["债券型-未分类", "理财型"])
    )
    if bond_runtime_refine_mask.any():
        inferred_bond_ftypes = result.loc[bond_runtime_refine_mask].apply(
            lambda row: infer_high_confidence_bond_semantic_ftype(
                row.get("shortname"),
                row.get("fullname"),
                row.get("benchmark"),
            ),
            axis=1,
        )
        inferred_bond_ftypes = inferred_bond_ftypes.dropna()
        if not inferred_bond_ftypes.empty:
            result.loc[inferred_bond_ftypes.index, "ftype"] = inferred_bond_ftypes

    return result
