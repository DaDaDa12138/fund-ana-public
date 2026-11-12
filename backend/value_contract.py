"""Nullable provider observations. A real zero is different from missing input."""
from __future__ import annotations

import math
from typing import Any


def numeric_observation(row: dict[str, Any], field: str, *, requested: bool = True) -> dict[str, Any]:
    raw = row.get(field)
    status = "observed"
    value = None
    if not requested:
        status = "not_requested"
    elif field not in row:
        status = "field_absent"
    elif raw is None or (isinstance(raw, str) and not raw.strip()):
        status = "provider_null"
    else:
        try:
            value = float(raw) if not isinstance(raw, bool) else None
        except (TypeError, ValueError, OverflowError):
            value = None
        if value is None or not math.isfinite(value):
            value, status = None, "parse_failed"
    return {"value": value, "status": status, "raw": raw, "field": field}
