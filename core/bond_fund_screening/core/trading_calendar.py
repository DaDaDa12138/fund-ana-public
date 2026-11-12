"""Explicitly sourced SSE closures; uncovered years retain unknown semantics."""
from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path
from typing import Iterable, Any

VERSION = "sse-closure-calendar-2026-10-28.1"
_ANNUAL_SOURCES = json.loads(Path(__file__).with_name("trading_calendar_sources.json").read_text())
SOURCE = {
    "version": VERSION, "covered_years": [2021, 2022, 2023, 2024, 2025, 2026],
    "annual_sources": _ANNUAL_SOURCES,
    "url": "https://www.sse.com.cn/disclosure/dealinstruc/closed/",
    "captured_at": "2026-10-13T10:10:34.907534+08:00",
    "response_sha256": "7ea10c16202c4748f0ad5bcdfe6ee07b85b5462589504604b17c9d63865e7d8c",
    "raw_artifact": "reports/audit/implementation_20261013/secondary_validation_749/holding_gate_review/sse_2026_annual_closed_raw.html",
    "scope": "SSE open weekdays for domestic bond NAV input checks; not an overseas or fund-specific calendar",
}
_CLOSURES = (("2026-01-01", "2026-01-03"), ("2026-02-15", "2026-02-23"),
             ("2026-04-04", "2026-04-06"), ("2026-05-01", "2026-05-05"),
             ("2026-06-19", "2026-06-21"), ("2026-09-25", "2026-09-27"),
             ("2026-10-01", "2026-10-07"))
_CLOSURES += tuple(tuple(interval) for source in _ANNUAL_SOURCES.values()
                   for interval in source["closures"])
_CLOSED_DATES = frozenset(
    date.fromisoformat(left) + timedelta(days=offset)
    for left, right in _CLOSURES
    for offset in range((date.fromisoformat(right) - date.fromisoformat(left)).days + 1)
)


def _date(value: Any) -> date:
    return date.fromisoformat(str(value)[:10])


def expected_open_dates(start: Any, end: Any) -> list[date] | None:
    """Open dates in (start, end]; None means the calendar is not verified."""
    left, right = _date(start), _date(end)
    if left > right:
        raise ValueError("invalid_calendar_window")
    if any(year not in SOURCE["covered_years"] for year in range(left.year, right.year + 1)):
        return None
    dates = []
    day = left + timedelta(days=1)
    while day <= right:
        if day.weekday() < 5 and day not in _CLOSED_DATES:
            dates.append(day)
        day += timedelta(days=1)
    return dates


def continuity_evidence(nav_dates: Iterable[Any], *, max_calendar_gap_days: int = 10) -> dict:
    """Reject missing known open days; exempt only proven closed-day gaps.

    Outside the sourced year, the pre-existing bounded natural-gap rule stays
    conservative. The record states that the historical calendar is unknown.
    """
    dates = sorted(set(_date(value) for value in nav_dates))
    missing, unknown_long_gaps, closed_long_gaps = [], [], []
    uncovered_gap_count = 0
    for left, right in zip(dates, dates[1:]):
        gap = (right - left).days
        expected = expected_open_dates(left, right - timedelta(days=1))
        if expected is None:
            uncovered_gap_count += 1
            if gap > max_calendar_gap_days:
                unknown_long_gaps.append({"start": left.isoformat(), "end": right.isoformat(), "gap_days": gap})
        elif expected:
            missing.extend(day.isoformat() for day in expected)
        elif gap > max_calendar_gap_days:
            closed_long_gaps.append({"start": left.isoformat(), "end": right.isoformat(), "gap_days": gap})
    return {
        "verified": len(dates) >= 2 and not missing and not unknown_long_gaps,
        "calendar": dict(SOURCE), "uncovered_gap_count": uncovered_gap_count,
        "uncovered_date_policy": "original_bounded_natural_gap_rule; calendar_not_verified",
        "max_raw_gap_days": max(((b-a).days for a, b in zip(dates, dates[1:])), default=0),
        "missing_open_day_count": len(missing), "missing_open_day_samples": missing[:10],
        "unknown_long_gap_count": len(unknown_long_gaps), "unknown_long_gap_samples": unknown_long_gaps[:10],
        "proven_closed_long_gap_count": len(closed_long_gaps), "proven_closed_long_gap_samples": closed_long_gaps[:10],
    }
