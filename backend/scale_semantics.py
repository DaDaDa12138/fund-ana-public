from __future__ import annotations

import math
from typing import Any


SCALE_FIELD_PRIORITY = (
    "合并规模(亿)",
    "effective_scale_billion",
    "family_scale_billion",
    "基金规模(亿)",
    "scale_billion",
)


def resolve_scale_for_thresholds(row: dict[str, Any]) -> float | None:
    """Resolve complete family scale first, then fall back to share scale."""
    for key in SCALE_FIELD_PRIORITY:
        value = row.get(key)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric):
            return numeric
    return None
