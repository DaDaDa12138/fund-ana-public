"""Conservative parsing for the public Danjuan fund detail response.

The endpoint exposes useful lock-period facts for some funds, but an empty
``hold_lock_period`` (or ``lock_fund_type=0``) is not proof of unrestricted
redemption.  This module therefore returns a match only when an explicit
period is present in the response.
"""

from __future__ import annotations

import re
from typing import Any

from utils.holding_period_parser import HoldingMatch


_PERIOD_PATTERN = re.compile(
    r"(?P<number>\d+(?:\.\d+)?)\s*(?P<unit>年|个月|月|周|天|日|day|days|month|months|year|years)",
    re.IGNORECASE,
)
_FIELD_PRIORITY = ("hold_lock_period", "close_period", "close_period_describe")
_UNIT_DAYS = {
    "年": 365,
    "year": 365,
    "years": 365,
    "个月": 30,
    "月": 30,
    "month": 30,
    "months": 30,
    "周": 7,
    "天": 1,
    "日": 1,
    "day": 1,
    "days": 1,
}


def _parse_period_days(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text or text in {"--", "---", "0", "0天"}:
        return None
    match = _PERIOD_PATTERN.search(text)
    if not match:
        return None
    try:
        number = float(match.group("number"))
    except (TypeError, ValueError):
        return None
    days = int(round(number * _UNIT_DAYS[match.group("unit").lower()]))
    return days if days > 0 else None


def parse_danjuan_holding_period(payload: Any) -> HoldingMatch | None:
    """Parse an explicit Danjuan lock period without imputing zero days.

    The longest explicit field wins so a descriptive sentence cannot weaken a
    shorter structured value.  The returned source is deliberately distinct
    from the Tiantian ``detail_struct`` source.
    """

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None

    candidates: list[tuple[int, str, str]] = []
    for field in _FIELD_PRIORITY:
        raw = data.get(field)
        days = _parse_period_days(raw)
        if days is not None:
            candidates.append((days, field, str(raw).strip()))
    if not candidates:
        return None
    days, field, raw = max(candidates, key=lambda item: item[0])
    return HoldingMatch(
        days=days,
        keyword=f"danjuan:{field}={raw}",
        source="danjuan_struct",
    )


__all__ = ["parse_danjuan_holding_period"]
