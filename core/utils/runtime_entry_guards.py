#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行时入口护栏规则。
"""

from __future__ import annotations

from typing import Optional, Sequence

import pandas as pd


HYBRID_COMMODITY_NAME_MARKERS = (
    "黄金ETF",
    "上海金ETF",
    "白银期货",
    "期货ETF",
)
HYBRID_COMMODITY_BENCHMARK_POSITIVE_MARKERS = (
    "上海黄金交易所",
    "上海期货交易所",
    "大连商品交易所",
    "黄金现货",
    "白银期货",
    "有色金属期货",
    "豆粕期货",
    "Au99.99",
    "AU99.99",
    "Au9999",
    "SHAU",
)
HYBRID_COMMODITY_BENCHMARK_NEGATIVE_MARKERS = (
    "沪深",
    "中证",
    "恒生",
    "纳斯达克",
    "标普",
    "国债",
    "债券",
    "债基",
)
EXPLICIT_FOF_NAME_MARKERS = ("FOF",)
EXPLICIT_QDII_NAME_MARKERS = ("QDII",)
PRODUCT_FORM_TYPE_CODES = ("3", "4")


def _contains_any(text: Optional[str], markers: Sequence[str]) -> bool:
    normalized = str(text or "").strip()
    return any(marker in normalized for marker in markers)


def has_explicit_fof_name(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> bool:
    return _contains_any(shortname, EXPLICIT_FOF_NAME_MARKERS) or _contains_any(
        fullname,
        EXPLICIT_FOF_NAME_MARKERS,
    )


def has_explicit_qdii_name(
    shortname: Optional[str],
    fullname: Optional[str] = None,
) -> bool:
    return _contains_any(shortname, EXPLICIT_QDII_NAME_MARKERS) or _contains_any(
        fullname,
        EXPLICIT_QDII_NAME_MARKERS,
    )


def build_explicit_fof_name_sql(
    *,
    table_alias: str = "f",
    available_columns: Optional[set] = None,
) -> tuple[str, tuple]:
    available_columns = available_columns or set()
    name_parts = []
    params = []
    for column in ("shortname", "fullname"):
        if available_columns and column not in available_columns:
            continue
        for marker in EXPLICIT_FOF_NAME_MARKERS:
            name_parts.append(f"COALESCE({table_alias}.{column}, '') LIKE ?")
            params.append(f"%{marker}%")
    if not name_parts:
        return "", ()
    return f"{table_alias}.ftype = '002' AND ({' OR '.join(name_parts)})", tuple(params)


def build_explicit_qdii_name_sql(
    *,
    table_alias: str = "f",
    available_columns: Optional[set] = None,
) -> tuple[str, tuple]:
    available_columns = available_columns or set()
    name_parts = []
    params = []
    for column in ("shortname", "fullname"):
        if available_columns and column not in available_columns:
            continue
        for marker in EXPLICIT_QDII_NAME_MARKERS:
            name_parts.append(f"COALESCE({table_alias}.{column}, '') LIKE ?")
            params.append(f"%{marker}%")
    if not name_parts:
        return "", ()
    return (
        f"{table_alias}.type_code IN ('3', '4') AND ({' OR '.join(name_parts)})",
        tuple(params),
    )


def build_hybrid_commodity_exclusion_sql(
    *,
    table_alias: str = "f",
    available_columns: Optional[set] = None,
) -> tuple[str, tuple]:
    available_columns = available_columns or set()

    exclusion_parts = []
    params = []

    if (not available_columns) or ("shortname" in available_columns):
        shortname_parts = [
            f"COALESCE({table_alias}.shortname, '') LIKE ?"
            for _ in HYBRID_COMMODITY_NAME_MARKERS
        ]
        exclusion_parts.append(f"({' OR '.join(shortname_parts)})")
        params.extend(f"%{marker}%" for marker in HYBRID_COMMODITY_NAME_MARKERS)

    if (not available_columns) or ("benchmark" in available_columns):
        benchmark_positive_parts = [
            f"COALESCE({table_alias}.benchmark, '') LIKE ?"
            for _ in HYBRID_COMMODITY_BENCHMARK_POSITIVE_MARKERS
        ]
        benchmark_negative_parts = [
            f"COALESCE({table_alias}.benchmark, '') LIKE ?"
            for _ in HYBRID_COMMODITY_BENCHMARK_NEGATIVE_MARKERS
        ]
        exclusion_parts.append(
            f"(({' OR '.join(benchmark_positive_parts)}) AND NOT ({' OR '.join(benchmark_negative_parts)}))"
        )
        params.extend(f"%{marker}%" for marker in HYBRID_COMMODITY_BENCHMARK_POSITIVE_MARKERS)
        params.extend(f"%{marker}%" for marker in HYBRID_COMMODITY_BENCHMARK_NEGATIVE_MARKERS)

    if not exclusion_parts:
        return "", ()

    return f"{table_alias}.ftype = '002' AND ({' OR '.join(exclusion_parts)})", tuple(params)


def build_name_contains_mask(df: pd.DataFrame, marker: str) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    if "shortname" in df.columns:
        mask = mask | df["shortname"].astype(str).str.contains(marker, na=False)
    if "fullname" in df.columns:
        mask = mask | df["fullname"].astype(str).str.contains(marker, na=False)
    return mask
