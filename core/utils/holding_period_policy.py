"""Shared holding-period publication policy.

Structured days take priority together with concrete terms in a fund name.
A name without restriction evidence is treated as an ordinary fund. A known
restriction without a measurable term remains unresolved, never ordinary.
"""

from __future__ import annotations

import math
from typing import Any

from .holding_period_parser import parse_holding_match


NO_SIGNAL_REVIEWED_SOURCE = "no_signal_reviewed"
HOLDING_DAYS_UNKNOWN = "holding_days_unknown"
HOLDING_PERIOD_LIMIT = "holding_period_limit"
HOLDING_PERIOD_UNRESOLVED = "holding_period_name_unparsed"


def _name_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"", "nan", "nat", "none", "null", "<na>", "--", "-"} else text


def combine_holding_period_names(*names: Any) -> str:
    """Combine usable short/full names without turning NULL into name text."""
    return " ".join(dict.fromkeys(text for name in names if (text := _name_text(name))))


def normalize_holding_days(value: Any) -> int | None:
    """Return a verified non-negative integer day count, preserving NULL."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0 or not number.is_integer():
        return None
    return int(number)


def _is_missing_holding_days(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _normalize_holding_flag(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or not number.is_integer() or int(number) not in {0, 1}:
        return None
    return int(number)


def is_reviewed_no_holding_signal(
    *,
    has_holding_period: Any,
    holding_days: Any,
    holding_period_source: Any,
) -> bool:
    """Whether an explicit scan found no holding-period restriction signal."""
    has_period = _normalize_holding_flag(has_holding_period)
    return (
        has_period == 0
        and _is_missing_holding_days(holding_days)
        and str(holding_period_source or "").strip() == NO_SIGNAL_REVIEWED_SOURCE
    )


def _is_generic_placeholder(*, holding_days: Any, holding_period_source: Any = None,
                            holding_keywords: Any = None) -> bool:
    if normalize_holding_days(holding_days) != 999:
        return False
    source = str(holding_period_source or "").strip().lower()
    keyword = str(holding_keywords or "").strip()
    return "generic" in source or keyword in {"定开", "定期开放", "封闭式", "定期开放运作方式"}


def resolve_holding_period_days(*, holding_days: Any, fund_name: Any = None,
                                holding_period_source: Any = None,
                                holding_keywords: Any = None) -> int | None:
    """Resolve concrete days from structured metadata and a fund name.

    A legacy 999 value with a generic source/keyword is a placeholder, rather
    than a measured duration. Conflicting concrete values use the longer term.
    """
    structured_days = normalize_holding_days(holding_days)
    if _is_generic_placeholder(holding_days=holding_days,
                                holding_period_source=holding_period_source,
                                holding_keywords=holding_keywords):
        structured_days = None

    name = _name_text(fund_name)
    name_days = None
    if name:
        # The parser keeps the longest concrete term, including concatenated
        # short/full names supplied by callers.
        name_match = parse_holding_match(shortname=name, fullname="")
        if name_match is not None:
            candidate = normalize_holding_days(name_match.days)
            if candidate != 999 or "generic" not in str(name_match.source or "").lower():
                name_days = candidate

    values = [days for days in (structured_days, name_days) if days is not None]
    return max(values) if values else None


def holding_period_gate_reason(
    *,
    has_holding_period: Any,
    holding_days: Any,
    holding_period_source: Any = None,
    holding_keywords: Any = None,
    threshold_days: int = 90,
    fund_name: Any = None,
) -> str | None:
    """Return the shared gate reason, or ``None`` when the row may proceed.

    An ordinary name with no restriction evidence may proceed while its days
    remain NULL. Restriction evidence without a term blocks publication.
    """
    name = _name_text(fund_name)
    days = resolve_holding_period_days(
        holding_days=holding_days,
        fund_name=name,
        holding_period_source=holding_period_source,
        holding_keywords=holding_keywords,
    )
    if days in (None, 0) and has_unresolved_holding_signal(
        has_holding_period=has_holding_period, holding_days=holding_days,
        holding_period_source=holding_period_source, holding_keywords=holding_keywords,
        fund_name=name,
    ):
        return HOLDING_PERIOD_UNRESOLVED
    if days is None:
        if is_reviewed_no_holding_signal(
            has_holding_period=has_holding_period,
            holding_days=holding_days,
            holding_period_source=holding_period_source,
        ):
            return None
        # With a usable name but no measurable duration, the requested policy
        # treats the fund as ordinary and leaves its stored NULL unchanged.
        if name:
            return None
        return HOLDING_DAYS_UNKNOWN
    if days > int(threshold_days):
        return HOLDING_PERIOD_LIMIT
    # A positive holding flag with zero days is not a valid confirmed value.
    if _normalize_holding_flag(has_holding_period) == 1 and days == 0:
        return HOLDING_DAYS_UNKNOWN
    return None


def has_unresolved_holding_signal(*, has_holding_period: Any, holding_days: Any,
                                  holding_period_source: Any = None,
                                  holding_keywords: Any = None,
                                  fund_name: Any = None) -> bool:
    """Restriction evidence is not a duration; legacy 999 is never a fact."""
    if resolve_holding_period_days(
        holding_days=holding_days, fund_name=fund_name,
        holding_period_source=holding_period_source, holding_keywords=holding_keywords,
    ) not in (None, 0):
        return False
    text = " ".join((_name_text(fund_name), _name_text(holding_keywords)))
    return ((_normalize_holding_flag(has_holding_period) == 1
             and normalize_holding_days(holding_days) is None and bool(_name_text(fund_name)))
            or _is_generic_placeholder(holding_days=holding_days,
                                      holding_period_source=holding_period_source,
                                      holding_keywords=holding_keywords)
            or any(token in text for token in ("持有期", "滚动持有", "锁定", "封闭", "定开", "定期开放")))


__all__ = [
    "HOLDING_DAYS_UNKNOWN",
    "HOLDING_PERIOD_LIMIT",
    "HOLDING_PERIOD_UNRESOLVED",
    "NO_SIGNAL_REVIEWED_SOURCE",
    "combine_holding_period_names",
    "holding_period_gate_reason",
    "has_unresolved_holding_signal",
    "is_reviewed_no_holding_signal",
    "normalize_holding_days",
    "resolve_holding_period_days",
]
