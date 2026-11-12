#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高置信同业存单指数基金识别策略。
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import and_, func, not_, or_


NEGOTIABLE_CD_NAME_MARKERS = ("同业存单",)
NEGOTIABLE_CD_ALIAS_NAME_MARKERS = ("同存AAA指数",)
NEGOTIABLE_CD_ALL_NAME_MARKERS = NEGOTIABLE_CD_NAME_MARKERS + NEGOTIABLE_CD_ALIAS_NAME_MARKERS
NEGOTIABLE_CD_INDEX_NAME_MARKERS = ("中证同业存单AAA指数", "同存AAA指数")
NEGOTIABLE_CD_BENCHMARK_SIGNAL_MARKERS = ("同业存单", "AAA")
NEGOTIABLE_CD_INDEX_BENCHMARK_MARKERS = ("中证同业存单AAA指数", "同业存单AAA指数")
NEGOTIABLE_CD_EXPLICIT_HYBRID_MARKERS = ("混合",)


def _normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip()


def _contains_any(text: str, markers) -> bool:
    return any(marker in text for marker in markers)


def build_fund_name_text(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> str:
    """归一化基金名称文本。"""
    return " ".join(
        part.strip()
        for part in (_normalize_text(shortname), _normalize_text(fullname))
        if part.strip()
    )


def has_explicit_hybrid_name(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> bool:
    """显式混合名称不应命中同业存单指数的高置信债券识别。"""
    return _contains_any(
        build_fund_name_text(shortname, fullname),
        NEGOTIABLE_CD_EXPLICIT_HYBRID_MARKERS,
    )


def has_negotiable_cd_name_signal(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> bool:
    """名称是否命中同业存单指数家族。"""
    return _contains_any(
        build_fund_name_text(shortname, fullname),
        NEGOTIABLE_CD_ALL_NAME_MARKERS,
    )


def has_negotiable_cd_index_signal(
    shortname: Optional[str],
    fullname: Optional[str] = None,
    benchmark: Optional[str] = None,
) -> bool:
    """名称或 benchmark 是否命中同业存单指数信号。"""
    name_text = build_fund_name_text(shortname, fullname)
    benchmark_text = _normalize_text(benchmark)
    return (
        _contains_any(name_text, NEGOTIABLE_CD_INDEX_NAME_MARKERS)
        or _contains_any(benchmark_text, NEGOTIABLE_CD_INDEX_BENCHMARK_MARKERS)
    )


def is_high_confidence_negotiable_cd_candidate(
    shortname: Optional[str],
    fullname: Optional[str] = None,
    benchmark: Optional[str] = None,
) -> bool:
    """判断是否属于高置信同业存单指数基金。"""
    if has_explicit_hybrid_name(shortname, fullname):
        return False
    return (
        has_negotiable_cd_name_signal(shortname, fullname)
        and has_negotiable_cd_index_signal(shortname, fullname, benchmark)
    )


def build_high_confidence_negotiable_cd_sqlalchemy_filter(model):
    """构建 SQLAlchemy 高置信同业存单指数识别条件。"""
    primary_name_signal = or_(
        *(
            condition
            for marker in NEGOTIABLE_CD_NAME_MARKERS
            for condition in (
                func.coalesce(model.shortname, '').like(f'%{marker}%'),
                func.coalesce(model.fullname, '').like(f'%{marker}%'),
            )
        )
    )
    alias_name_signal = or_(
        *(
            condition
            for marker in NEGOTIABLE_CD_ALIAS_NAME_MARKERS
            for condition in (
                func.coalesce(model.shortname, '').like(f'%{marker}%'),
                func.coalesce(model.fullname, '').like(f'%{marker}%'),
            )
        )
    )
    index_name_signal = or_(
        *(
            condition
            for marker in NEGOTIABLE_CD_INDEX_NAME_MARKERS
            for condition in (
                func.coalesce(model.shortname, '').like(f'%{marker}%'),
                func.coalesce(model.fullname, '').like(f'%{marker}%'),
            )
        )
    )
    benchmark_signal = or_(
        *(func.coalesce(model.benchmark, '').like(f'%{marker}%') for marker in NEGOTIABLE_CD_INDEX_BENCHMARK_MARKERS)
    )
    explicit_hybrid_signal = or_(
        *(
            condition
            for marker in NEGOTIABLE_CD_EXPLICIT_HYBRID_MARKERS
            for condition in (
                func.coalesce(model.shortname, '').like(f'%{marker}%'),
                func.coalesce(model.fullname, '').like(f'%{marker}%'),
            )
        )
    )
    return and_(
        or_(
            and_(
                primary_name_signal,
                or_(index_name_signal, benchmark_signal),
                not_(explicit_hybrid_signal),
            ),
            and_(
                alias_name_signal,
                benchmark_signal,
                not_(explicit_hybrid_signal),
            ),
        )
    )


def build_hybrid_negotiable_cd_exclusion_sql(
    *,
    table_alias: str = "f",
    available_columns: Optional[set] = None,
) -> tuple[str, tuple]:
    """
    构建 hybrid 入口的同业存单排除 SQL 条件。

    口径说明：
    - 这里的目标是清理 hybrid 池污染，而不是识别“可归入债券池”的高置信样本；
    - 因此显式 `混合` 名称仍应排除，只要名称或低信息量数值态 benchmark 命中同业存单信号。
    """
    available_columns = available_columns or set()

    params = []
    name_parts = []
    benchmark_parts = []

    for column in ("shortname", "fullname"):
        if available_columns and column not in available_columns:
            continue
        for marker in NEGOTIABLE_CD_NAME_MARKERS:
            name_parts.append(f"COALESCE({table_alias}.{column}, '') LIKE ?")
            params.append(f"%{marker}%")

    if not name_parts:
        return "", ()

    if (not available_columns) or ("benchmark" in available_columns):
        benchmark_parts = [
            f"COALESCE({table_alias}.benchmark, '') LIKE ?"
            for _ in NEGOTIABLE_CD_BENCHMARK_SIGNAL_MARKERS
        ]
        params.extend(f"%{marker}%" for marker in NEGOTIABLE_CD_BENCHMARK_SIGNAL_MARKERS)

    exclusion_parts = [f"({' OR '.join(name_parts)})"]
    if benchmark_parts:
        exclusion_parts.append(
            f"({table_alias}.ftype = '002' AND ({' AND '.join(benchmark_parts)}))"
        )

    return f"({' OR '.join(exclusion_parts)})", tuple(params)


def infer_high_confidence_negotiable_cd_type_fields(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    基于名称识别高置信同业存单指数基金的债券 bucket。

    约束：
    - 只做 bucket 纠偏，不在列表链路直接伪造语义化 `ftype`；
    - 显式 `混合` 名称不命中，避免误伤真实混合基金；
    - 仅覆盖已验证的高置信命名家族：`同业存单...指数`、`同存AAA指数`。
    """
    name_text = build_fund_name_text(shortname, fullname)
    if not name_text or has_explicit_hybrid_name(shortname, fullname):
        return None, None

    if "同存AAA指数" in name_text:
        return "31", "fixed_income"

    if "同业存单" in name_text and "指数" in name_text:
        return "31", "fixed_income"

    return None, None
