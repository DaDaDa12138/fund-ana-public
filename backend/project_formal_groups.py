from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd


FORMAL_SCORE_LABEL = "正式投资评分"
FORMAL_GROUP_LABEL = "正式榜分组"
STABLE_BOND_GROUP_LABEL = "纯债稳健榜"
ENHANCED_BOND_GROUP_LABEL = "增强债弹性榜"
ENHANCED_STYLE_TAGS = {"一级债增强", "二级债增强", "转债增强"}


def formal_group_from_row(row: dict[str, Any]) -> str:
    explicit_group = str(row.get(FORMAL_GROUP_LABEL) or "").strip()
    if explicit_group in {STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL}:
        return explicit_group
    style = str(row.get("债基风格标签") or "").strip()
    exposure = str(row.get("是否含权益/转债增强") or "").strip()
    if style in ENHANCED_STYLE_TAGS or exposure == "是":
        return ENHANCED_BOND_GROUP_LABEL
    return STABLE_BOND_GROUP_LABEL


def append_formal_group_column(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        current[FORMAL_GROUP_LABEL] = pd.Series(dtype=object)
        return current
    current[FORMAL_GROUP_LABEL] = [
        formal_group_from_row(row)
        for row in current.to_dict(orient="records")
    ]
    return current


def build_formal_group_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or FORMAL_GROUP_LABEL not in df.columns:
        return pd.DataFrame(columns=[FORMAL_GROUP_LABEL, "数量", "占正式推荐比例(%)"])

    counter = Counter(str(value or "").strip() or STABLE_BOND_GROUP_LABEL for value in df[FORMAL_GROUP_LABEL].tolist())
    total = len(df.index)
    rows: list[dict[str, Any]] = []
    for label in [STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL]:
        count = int(counter.get(label, 0))
        rows.append(
            {
                FORMAL_GROUP_LABEL: label,
                "数量": count,
                "占正式推荐比例(%)": round(count / total * 100, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "ENHANCED_BOND_GROUP_LABEL",
    "ENHANCED_STYLE_TAGS",
    "FORMAL_GROUP_LABEL",
    "FORMAL_SCORE_LABEL",
    "STABLE_BOND_GROUP_LABEL",
    "append_formal_group_column",
    "build_formal_group_distribution",
    "formal_group_from_row",
]
