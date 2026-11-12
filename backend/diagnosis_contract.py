"""Read current and historical diagnosis labels without silently losing rows."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from backend.project_publish_decisions import (
    ABSOLUTE_QUALITY_DIAGNOSIS,
    AUTO_DIAGNOSIS_CATEGORY_COLUMN,
    AUTO_PUBLISH_GATE_DIAGNOSIS,
    DYNAMIC_QUALITY_DIAGNOSIS,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
    MANAGER_STABILITY_DIAGNOSIS,
    NOT_BUYABLE_DIAGNOSIS,
    QUALITY_WARNING_DIAGNOSIS,
    VALIDATION_BLOCK_DIAGNOSIS,
)

LEGACY_CATEGORY_COLUMN = "观察层分类"
LEGACY_CATEGORIES = {
    "不可申购观察": NOT_BUYABLE_DIAGNOSIS,
    "校验拦截观察": VALIDATION_BLOCK_DIAGNOSIS,
    "质量告警观察": QUALITY_WARNING_DIAGNOSIS,
    "绝对质量观察": ABSOLUTE_QUALITY_DIAGNOSIS,
    "动态质量观察": DYNAMIC_QUALITY_DIAGNOSIS,
    "经理稳定性观察": MANAGER_STABILITY_DIAGNOSIS,
    "经理稳定性诊断": MANAGER_STABILITY_DIAGNOSIS,
    "发布后复核观察": AUTO_PUBLISH_GATE_DIAGNOSIS,
}
KNOWN_CATEGORIES = frozenset((
    NOT_BUYABLE_DIAGNOSIS, VALIDATION_BLOCK_DIAGNOSIS, QUALITY_WARNING_DIAGNOSIS,
    ABSOLUTE_QUALITY_DIAGNOSIS, DYNAMIC_QUALITY_DIAGNOSIS,
    MANAGER_STABILITY_DIAGNOSIS, AUTO_PUBLISH_GATE_DIAGNOSIS,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
))
SUMMARY_CATEGORIES = {
    "manager_stability_watch_count": MANAGER_STABILITY_DIAGNOSIS,
    "publish_post_review_watch_count": AUTO_PUBLISH_GATE_DIAGNOSIS,
    "final_selection_removed_count": FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
}


def _category_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "nat"}:
        return ""
    return LEGACY_CATEGORIES.get(text, text)


def diagnosis_category(row: Mapping[str, Any], *, required: bool = False) -> str:
    """Allow blank categories on formal rows, require them when counting diagnoses."""
    current = _category_text(row.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN))
    legacy = _category_text(row.get(LEGACY_CATEGORY_COLUMN))
    if current and legacy and current != legacy:
        raise ValueError("diagnosis_category_conflict")
    value = current or legacy
    if required and not value:
        raise ValueError("diagnosis_category_missing")
    if value and value not in KNOWN_CATEGORIES:
        raise ValueError(f"diagnosis_category_unknown: {value}")
    return value


def diagnosis_categories(frame: pd.DataFrame, *, required: bool = False) -> pd.Series:
    if not frame.columns.is_unique:
        raise ValueError("diagnosis_columns_duplicated")
    return pd.Series(
        (diagnosis_category(row, required=required) for row in frame.to_dict(orient="records")),
        index=frame.index, dtype=object,
    )


def summarize_diagnoses(frame: pd.DataFrame) -> dict[str, int]:
    """A known empty pool is zero; a missing/unknown schema cannot prove zero."""
    categories = diagnosis_categories(frame, required=True)
    return {field: int(categories.eq(category).sum()) for field, category in SUMMARY_CATEGORIES.items()}
