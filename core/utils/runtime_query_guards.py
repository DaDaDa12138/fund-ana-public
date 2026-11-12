#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行时查询条件护栏拼装辅助。
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple, Union


def is_requested_fund_type(
    fund_type: Optional[Union[str, object]],
    target_name: str,
) -> bool:
    """统一判定请求是否命中指定 fund_type。"""
    if isinstance(fund_type, str):
        return fund_type.lower().strip() == target_name
    if hasattr(fund_type, "name") and str(fund_type.name).upper() == target_name.upper():
        return True
    if hasattr(fund_type, "value") and str(fund_type.value).lower() == target_name:
        return True
    return False


def collect_runtime_request_flags(
    fund_type: Optional[Union[str, object, List[object]]],
) -> dict:
    """收集 fund_type 请求对应的运行时护栏标记。"""
    targets: Sequence[object] = fund_type if isinstance(fund_type, list) else [fund_type]
    is_bond_request = any(is_requested_fund_type(target, "bond") for target in targets)
    is_stock_request = any(is_requested_fund_type(target, "stock") for target in targets)
    is_hybrid_request = any(is_requested_fund_type(target, "hybrid") for target in targets)
    return {
        "is_bond_request": is_bond_request,
        "is_stock_request": is_stock_request,
        "is_hybrid_request": is_hybrid_request,
        "is_non_index": is_bond_request or is_stock_request or is_hybrid_request,
        "includes_bond_request": is_bond_request,
        "is_fof_request": any(is_requested_fund_type(target, "fof") for target in targets),
        "is_qdii_request": any(is_requested_fund_type(target, "qdii") for target in targets),
    }


def apply_non_index_runtime_guard(
    condition: str,
    *,
    includes_bond_request: bool,
) -> str:
    """对非指数请求补充指数型排除护栏。"""
    if includes_bond_request:
        return f"({condition}) AND (f.ftype NOT LIKE '指数型%' OR f.ftype = '指数型-固收')"
    return f"({condition}) AND f.ftype NOT LIKE '指数型%'"


def compose_type_condition_with_runtime_guards(
    *,
    base_condition: str,
    flags: dict,
    explicit_fof_guard: Tuple[str, tuple] = ("", ()),
    explicit_qdii_guard: Tuple[str, tuple] = ("", ()),
    hybrid_commodity_guard: Tuple[str, tuple] = ("", ()),
    hybrid_negotiable_cd_guard: Tuple[str, tuple] = ("", ()),
) -> Tuple[str, tuple]:
    """
    基于已解析的 guard SQL 片段拼装最终条件和附加参数。

    约束：
    - 保持 DataLoader 当前拼装顺序不变，避免参数绑定顺序漂移；
    - 这里只做条件组合，不负责最终的 exclusion_clause 拼装。
    """
    condition = base_condition
    extra_params: tuple = ()

    explicit_fof_condition, explicit_fof_params = explicit_fof_guard
    explicit_qdii_condition, explicit_qdii_params = explicit_qdii_guard
    hybrid_commodity_condition, hybrid_commodity_params = hybrid_commodity_guard
    hybrid_negotiable_cd_condition, hybrid_negotiable_cd_params = hybrid_negotiable_cd_guard

    if flags.get("is_fof_request") and explicit_fof_condition:
        condition = f"(({condition}) OR ({explicit_fof_condition}))"
        extra_params += explicit_fof_params

    if flags.get("is_qdii_request") and explicit_qdii_condition:
        condition = f"(({condition}) OR ({explicit_qdii_condition}))"
        extra_params += explicit_qdii_params

    if flags.get("is_hybrid_request"):
        if hybrid_commodity_condition:
            condition = f"({condition}) AND NOT ({hybrid_commodity_condition})"
            extra_params += hybrid_commodity_params
        if hybrid_negotiable_cd_condition:
            condition = f"({condition}) AND NOT ({hybrid_negotiable_cd_condition})"
            extra_params += hybrid_negotiable_cd_params
        if explicit_fof_condition and not flags.get("is_fof_request"):
            condition = f"({condition}) AND NOT ({explicit_fof_condition})"
            extra_params += explicit_fof_params

    return condition, extra_params
