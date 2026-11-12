"""Date-only observations must not become fabricated disclosure conclusions."""
from datetime import datetime, timezone

import pytest

from backend.nav_freshness import build_nav_freshness


def observe(candidate, raw, instant="2026-09-14T08:00:00+08:00", **extra):
    return build_nav_freshness(
        {"candidate_date": candidate, "raw_max_latest_nav_date": raw, **extra},
        observed_at=datetime.fromisoformat(instant), timezone_name="Asia/Shanghai",
    )


@pytest.mark.parametrize("instant,source,age,weekday", [
    ("2026-09-14T07:30:00+08:00", "2026-09-11", 3, True),
    ("2026-09-14T23:59:59+08:00", "2026-09-11", 3, True),
    ("2026-09-12T13:45:00+08:00", "2026-09-11", 1, False),
    ("2026-09-13T13:45:00+08:00", "2026-09-11", 2, False),
    ("2026-10-01T13:45:00+08:00", "2026-09-30", 1, True),
    ("2026-10-07T13:45:00+08:00", "2026-09-30", 7, True),
    ("2026-09-14T15:59:59+00:00", "2026-09-14", 0, True),
    ("2026-09-14T16:00:00+00:00", "2026-09-14", 1, True),
    ("2026-09-14T09:00:00-07:00", "2026-09-14", 1, True),
])
def test_calendar_age_does_not_claim_trading_days_or_overdue_disclosure(instant, source, age, weekday):
    result = observe(source, source, instant, ready=True, readiness_reason="ready")
    assert result["effective_calendar_age_days"] == result["raw_calendar_age_days"] == age
    assert result["is_weekday"] is weekday
    assert result["is_business_day"] is None
    assert result["expected_nav_date"] is None
    assert result["disclosure_deadline"] is None
    assert result["stale"] is None
    assert result["raw_vs_effective"] == "aligned"
    assert result["effective_snapshot_ready"] is True
    assert result["effective_candidate_date"] == result["raw_latest_nav_date"] == source
    assert result["observed_at"].endswith("+08:00")


@pytest.mark.parametrize("candidate,raw,relation", [
    ("2026-09-10", "2026-09-11", "raw_ahead"),
    ("2026-09-11", "2026-09-10", "raw_behind"),
    ("2026-09-11", "2026-09-11", "aligned"),
    (None, "2026-09-11", "unknown"),
    ("2026-09-11", None, "unknown"),
    (None, None, "unknown"),
    ("2026-09-11", "2026-09-15", "future_date_unverified"),
    ("2026-09-15", "2026-09-11", "future_date_unverified"),
    ("2026-09-31", "2026-09-11", "unknown"),
    ("2026-09-11garbage", "2026-09-11", "unknown"),
    ("2026-09-11", "2026-9-11", "unknown"),
])
def test_comparison_requires_two_valid_nonfuture_dates(candidate, raw, relation):
    result = observe(candidate, raw)
    assert result["raw_vs_effective"] == relation
    assert result["stale"] is None
    assert result["effective_snapshot_ready"] is None


@pytest.mark.parametrize("source,state,age", [
    (None, "missing", None), ("", "missing", None),
    ("2026-02-29", "invalid", None), ("2026-09-11T00:00:00", "invalid", None),
    ("2026-09-15", "future", -1), ("2026-09-14", "valid", 0),
])
def test_unknown_or_future_dates_are_not_clamped_to_fake_zero(source, state, age):
    result = observe(source, source, ready=False, readiness_reason="insufficient_coverage")
    assert result["effective_candidate_date_status"] == result["raw_latest_nav_date_status"] == state
    assert result["effective_calendar_age_days"] == result["raw_calendar_age_days"] == age
    assert result["effective_snapshot_ready"] is False
    assert result["effective_snapshot_readiness_reason"] == "insufficient_coverage"
    assert result["stale"] is None


def test_same_instant_uses_configured_timezone_without_rewriting_nav_date():
    instant = datetime(2026, 9, 14, 16, tzinfo=timezone.utc)
    snapshot = {"candidate_date": "2026-09-14", "raw_max_latest_nav_date": "2026-09-14"}
    china = build_nav_freshness(snapshot, observed_at=instant, timezone_name="Asia/Shanghai")
    utc = build_nav_freshness(snapshot, observed_at=instant, timezone_name="UTC")
    assert china["today"] == "2026-09-15" and china["effective_calendar_age_days"] == 1
    assert utc["today"] == "2026-09-14" and utc["effective_calendar_age_days"] == 0
    assert china["effective_candidate_date"] == utc["effective_candidate_date"] == "2026-09-14"


def test_naive_observation_cannot_silently_use_host_timezone():
    with pytest.raises(ValueError, match="timezone_aware_observation"):
        build_nav_freshness({}, observed_at=datetime(2026, 9, 14), timezone_name="Asia/Shanghai")
