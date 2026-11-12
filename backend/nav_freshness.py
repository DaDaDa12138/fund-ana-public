"""NAV date observations are distinct from an unverified disclosure deadline."""
from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any
from zoneinfo import ZoneInfo


NAV_FRESHNESS_SCHEMA_VERSION = 2
UNVERIFIED_DISCLOSURE_REASON = "verified_market_calendar_and_disclosure_deadline_unavailable"


def _date_observation(value: Any, today: date) -> tuple[str | None, date | None, str, int | None]:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None, None, "missing", None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text, None, "invalid", None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return text, None, "invalid", None
    age = (today - parsed).days
    return text, parsed, "future" if age < 0 else "valid", age


def build_nav_freshness(
    effective_snapshot: dict[str, Any], *, observed_at: datetime, timezone_name: str
) -> dict[str, Any]:
    """Describe observed dates without inventing an expected NAV publication date.

    The clock must identify an instant. Fund dates themselves stay unchanged.
    Negative calendar ages remain negative and are explicitly marked as future.
    Weekdays, snapshot readiness and real publication lag cannot establish a
    fund's dealing calendar or the deadline applicable to its NAV disclosure.
    """
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("nav_freshness_requires_timezone_aware_observation")
    local_time = observed_at.astimezone(ZoneInfo(timezone_name))
    today = local_time.date()
    candidate, candidate_date, candidate_state, candidate_age = _date_observation(
        effective_snapshot.get("candidate_date"), today
    )
    raw, raw_date, raw_state, raw_age = _date_observation(
        effective_snapshot.get("raw_max_latest_nav_date"), today
    )
    if "future" in (candidate_state, raw_state):
        relation = "future_date_unverified"
    elif candidate_date is None or raw_date is None:
        relation = "unknown"
    elif raw_date > candidate_date:
        relation = "raw_ahead"
    elif raw_date < candidate_date:
        relation = "raw_behind"
    else:
        relation = "aligned"
    ready = effective_snapshot.get("ready")
    return {
        "schema_version": NAV_FRESHNESS_SCHEMA_VERSION,
        "observed_at": local_time.isoformat(),
        "timezone": timezone_name,
        "today": today.isoformat(),
        "is_weekday": today.weekday() < 5,
        "effective_candidate_date": candidate,
        "raw_latest_nav_date": raw,
        "effective_candidate_date_status": candidate_state,
        "raw_latest_nav_date_status": raw_state,
        "effective_calendar_age_days": candidate_age,
        "raw_calendar_age_days": raw_age,
        "calendar_age_basis": "local_today_minus_source_date_including_weekends_and_holidays",
        "raw_vs_effective": relation,
        "effective_snapshot_ready": ready if isinstance(ready, bool) else None,
        "effective_snapshot_readiness_reason": effective_snapshot.get("readiness_reason") or None,
        "expected_nav_date": None,
        "disclosure_deadline": None,
        "stale": None,
        "staleness_assessment": "not_assessed",
        "staleness_reason": UNVERIFIED_DISCLOSURE_REASON,
        "trading_calendar_verified": False,
        # Legacy keys remain explicit NULL: their old weekday-only calculation
        # did not establish business days or an overdue disclosure.
        "is_business_day": None,
        "last_business_day": None,
        "lag_days_vs_last_business_day": None,
        "raw_lag_days_vs_last_business_day": None,
        "legacy_business_day_fields_status": "unavailable_without_verified_calendar",
    }
