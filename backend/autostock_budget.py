from __future__ import annotations

import json
import math
import sys
import time
from pathlib import Path
from typing import Any

_CORE_ROOT = Path(__file__).resolve().parents[1] / "core"
if str(_CORE_ROOT) not in sys.path:
    sys.path.append(str(_CORE_ROOT))
from utils.autostock_quota_state import read_quota_state

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None


AUTOSTOCK_QUOTA_WINDOW_SECONDS = 3600


def read_autostock_quota_snapshot(
    *,
    shared_state_path: str | Path,
    max_calls_per_hour: int,
    min_interval_seconds: float,
    now: float | None = None,
    fail_closed: bool = True,
) -> dict[str, Any]:
    current_time = time.time() if now is None else float(now)
    max_calls = int(max_calls_per_hour or 100)
    if max_calls <= 0:
        max_calls = 100
    state_path = Path(shared_state_path)
    timestamps: list[float] = []
    last_call_time = 0.0
    blocked_until = 0.0
    read_error: str | None = None

    if state_path:
        try:
            payload = read_quota_state(state_path)
            last_call_time = payload["last_call_time"]
            blocked_until = payload["blocked_until"]
            timestamps = [value for value in payload["recent_call_timestamps"]
                          if current_time - value < AUTOSTOCK_QUOTA_WINDOW_SECONDS]
        except Exception as exc:
            read_error = str(exc)

    quota_limited = max_calls > 0
    recent_call_count = len(timestamps)
    quota_remaining = max(max_calls - recent_call_count, 0) if quota_limited else None
    upstream_cooldown_active = blocked_until > current_time
    if (upstream_cooldown_active or (read_error and fail_closed)) and quota_limited:
        quota_remaining = 0
    quota_exhausted = bool(upstream_cooldown_active or (quota_limited and quota_remaining == 0))
    wait_seconds = max(0.0, blocked_until - current_time)
    if quota_exhausted and timestamps:
        if quota_limited and recent_call_count >= max_calls:
            wait_seconds = max(wait_seconds, AUTOSTOCK_QUOTA_WINDOW_SECONDS - (current_time - min(timestamps)))

    return {
        "quota_limited": quota_limited,
        "max_calls_per_hour": max_calls if quota_limited else None,
        "recent_call_count": recent_call_count,
        "quota_remaining": quota_remaining,
        "quota_exhausted": quota_exhausted,
        "rate_limit_wait_seconds": round(wait_seconds, 3),
        "min_interval_seconds": float(min_interval_seconds),
        "shared_state_path": str(state_path),
        "last_call_time": last_call_time,
        "state_read_error": read_error,
        "upstream_cooldown_active": upstream_cooldown_active,
        "blocked_until": blocked_until or None,
    }


def plan_autostock_call_budget(
    *,
    quota_snapshot: dict[str, Any],
    needed_call_count: int,
    reserve_calls: int,
) -> dict[str, Any]:
    needed = max(0, int(needed_call_count or 0))
    quota_limited = bool(quota_snapshot.get("quota_limited"))
    max_calls = int(quota_snapshot.get("max_calls_per_hour") or 0)
    reserve = max(0, min(int(reserve_calls or 0), max_calls)) if quota_limited else 0
    if quota_limited:
        remaining = max(int(quota_snapshot.get("quota_remaining") or 0), 0)
        available = max(remaining - reserve, 0)
    else:
        remaining = None
        available = needed
    allowed = min(needed, available)
    return {
        "needed_call_count": needed,
        "allowed_call_count": allowed,
        "skipped_call_count": max(0, needed - allowed),
        "quota_limited": quota_limited,
        "quota_remaining": remaining,
        "quota_reserved_for_automation": reserve,
        "quota_exhausted": bool(quota_snapshot.get("quota_exhausted")),
        "quota_unavailable": bool(needed > 0 and quota_limited and available <= 0),
        "quota_degraded": bool(needed > allowed),
    }
