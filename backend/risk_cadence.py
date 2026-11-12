from __future__ import annotations

import re
from typing import Any


_EXPLICIT_CADENCE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"定期开放"),
    re.compile(r"定开"),
    re.compile(r"滚动持有"),
    re.compile(r"持有期"),
    re.compile(r"滚动"),
    re.compile(r"月月"),
    re.compile(r"双月"),
    re.compile(r"季季"),
    re.compile(r"理财"),
    re.compile(r"(?:\d+|[一二三四五六七八九十两]+)\s*(?:个?月|天|年)(?:定开|持有|滚动|理财|开放)"),
)


def has_cadence_tail_signal(
    *,
    shortname: Any,
    has_holding_period: Any,
    holding_days: Any,
) -> bool:
    if bool(int(has_holding_period or 0)) and int(holding_days or 0) > 0:
        return True

    name = str(shortname or "").strip()
    if not name:
        return False

    return any(pattern.search(name) for pattern in _EXPLICIT_CADENCE_PATTERNS)
