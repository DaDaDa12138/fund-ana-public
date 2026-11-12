"""Preserve the distinction between an observed purchase flag and missing data."""
from __future__ import annotations

from numbers import Real
from typing import Any


def normalize_buyability(value: Any) -> bool | None:
    """Accept explicit binary flags only; unknown values do not imply suspension."""
    if isinstance(value, (bool, Real)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes"}:
            return True
        if text in {"0", "false", "no"}:
            return False
    return None


def buyability_label(value: Any) -> str:
    flag = normalize_buyability(value)
    if flag is True:
        return "可申购"
    if flag is False:
        return "不可申购"
    return "申购状态未知"
