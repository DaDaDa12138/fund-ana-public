"""Validate an estimate against its source time and published NAV baseline."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from backend.value_contract import numeric_observation

SHANGHAI = ZoneInfo("Asia/Shanghai")
CONTRACT_VERSION = "tiantian-valuation-2026-10-10.1"


def validate_valuation(row: dict[str, Any], *, now: datetime | None = None,
                       max_age_seconds: int = 1800, max_base_age_days: int = 10) -> dict[str, Any]:
    now = now or datetime.now(SHANGHAI)
    now = now.replace(tzinfo=SHANGHAI) if now.tzinfo is None else now.astimezone(SHANGHAI)
    observations = {field: numeric_observation(row, field) for field in ("GSZ", "GSZZL", "NAV")}
    result = {"formula_version": CONTRACT_VERSION, "field_observations": observations,
              "raw_dates": {field: row.get(field) for field in ("PDATE", "GZTIME")},
              "fetched_at": now.isoformat(timespec="seconds"), "available": False}

    def reject(reason: str, message: str) -> dict[str, Any]:
        return {**result, "reason": reason, "message": message}

    if all(observations[f]["status"] in {"provider_null", "field_absent"} for f in ("GSZ", "GSZZL")):
        return reject("valuation_not_provided", "天天基金当前未为该基金提供盘中估值")
    if any(o["status"] == "parse_failed" for o in observations.values()):
        return reject("valuation_parse_failed", "估值字段无法解析")
    if any(o["status"] != "observed" for o in observations.values()):
        return reject("valuation_fields_incomplete", "估值或基准净值字段不完整")
    try:
        valuation_time = datetime.fromisoformat(str(row.get("GZTIME") or ""))
        valuation_time = (valuation_time.replace(tzinfo=SHANGHAI) if valuation_time.tzinfo is None
                          else valuation_time.astimezone(SHANGHAI))
        base_date = datetime.strptime(str(row.get("PDATE") or ""), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return reject("valuation_time_invalid", "估值时间或净值基准日期缺失/无效")
    result["source_time"] = valuation_time.isoformat(timespec="seconds")
    age = (now - valuation_time).total_seconds()
    result["age_seconds"] = age
    if age < -300:
        return reject("valuation_time_future", "估值时间晚于当前时间")
    if age > max_age_seconds or valuation_time.date() != now.date():
        for f in ("GSZ", "GSZZL"):
            observations[f]["status"] = "stale"
        return reject("valuation_stale", "估值已过期；请以公布净值为准")
    base_age = (valuation_time.date() - base_date).days
    if not 1 <= base_age <= max_base_age_days:
        return reject("valuation_base_date_mismatch", "公布净值日期不能验证为本次估值的基准")
    nav, estimate, change = (observations[f]["value"] for f in ("NAV", "GSZ", "GSZZL"))
    if nav <= 0 or estimate <= 0 or change <= -100:
        return reject("valuation_values_invalid", "估值或基准净值不满足有效范围")
    # NAV/GSZ are generally rounded to four decimals; allow that rounding plus
    # the provider's two-decimal percentage rounding, not a different NAV base.
    expected_change = (estimate / nav - 1.0) * 100.0
    tolerance = 0.01 + 0.0001 / nav * 100.0
    result.update(base_nav_verified=True, percentage_rounding_tolerance=tolerance)
    if abs(expected_change - change) > tolerance:
        result["base_nav_verified"] = False
        return reject("valuation_base_nav_mismatch", "估值涨幅与公布净值基准不一致")
    return {**result, "available": True}
