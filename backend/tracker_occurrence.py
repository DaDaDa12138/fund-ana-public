"""Shared nullable display values for counts from a verified Tracker observation."""
from __future__ import annotations

import math
import re
from typing import Any


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def tracker_occurrence_fields(values: dict[str, Any] | None) -> dict[str, Any]:
    """Missing history is unavailable; a recorded zero is still a real value."""
    source = values or {}
    count = _number(source.get("出现次数"))
    count = int(count) if count is not None and count >= 0 and count.is_integer() else None
    frequency = _number(source.get("出现频率(%)"))
    if frequency == 0:
        frequency = 0.0
    frequency_text = (
        f"{frequency:.2f}".rstrip("0").rstrip(".")
        if frequency is not None and 0 <= frequency <= 100 else None
    )
    return {
        "出现次数": count,
        "出现频率(%)": frequency_text,
        "出现次数&出现频率(%)": (
            f"{count if count is not None else '-'} 次 / "
            f"{frequency_text + '%' if frequency_text is not None else '-'}"
        ),
    }


def tracker_occurrence_reason(reason: Any, values: dict[str, Any], total_periods: Any) -> Any:
    """Refresh only generated observation claims, preserving other published reasons."""
    if not isinstance(reason, str):
        return reason
    fields = tracker_occurrence_fields(values)
    count = fields["出现次数"]
    total = _number(total_periods)
    count_text = "累计入围次数暂无可用数据"
    if total is not None and total > 0 and total.is_integer() and count is not None and count <= total:
        count_text = f"累计入围 {count}/{int(total)} 期"
    frequency = fields["出现频率(%)"]
    frequency_text = (
        f"累计出现频率 {float(frequency):.0f}%"
        if frequency is not None else "累计出现频率暂无可用数据"
    )
    reason = re.sub(r"累计入围(?:\s+\d+\s*/\s*\d+\s*期|次数暂无可用数据)", count_text, reason)
    return re.sub(r"累计出现频率(?:\s+\d+(?:\.\d+)?\s*%|暂无可用数据)", frequency_text, reason)
