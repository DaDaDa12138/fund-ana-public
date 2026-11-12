"""Latest fund observations from the Tiantian / Eastmoney primary and backup.

These endpoints share one supplier. They cannot satisfy an independent-source
publication check and do not supply a fund directory or daily NAV history.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable

import requests

from backend.valuation_contract import SHANGHAI

SOURCE = "tiantian_fund_valuation_last"
FIELD_MAP = {
    "NAV": "netWorth", "ACCNAV": "totalWorth", "NAVCHGRT": "dayGrowth",
    "SYL_Z": "lastWeekGrowth", "SYL_Y": "lastMonthGrowth",
    "SYL_3Y": "lastThreeMonthsGrowth", "SYL_6Y": "lastSixMonthsGrowth",
    "SYL_1N": "lastYearGrowth", "SYL_JN": "thisYearGrowth",
    "SYL_LN": "returnSince",
}
FIELDS = ",".join(["FCODE", "SHORTNAME", "PDATE", *FIELD_MAP, "GSZ", "GSZZL", "GZTIME"])
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://h5.1234567.com.cn", "Referer": "https://h5.1234567.com.cn/",
    "User-Agent": "Mozilla/5.0",
}


def number_field(row: dict[str, Any], field: str) -> tuple[float | None, str]:
    if field not in row:
        return None, "field_not_returned"
    value = row[field]
    if value is None or (isinstance(value, str) and value.strip() in {"", "--", "-"}):
        return None, "upstream_null"
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None, "parse_failed"
    if isinstance(value, bool) or not math.isfinite(number):
        return None, "parse_failed"
    return number, "real_zero" if number == 0 else "value"


def normalize_snapshot(row: dict[str, Any], *, today: date, max_age_days: int) -> tuple[dict[str, Any] | None, str | None]:
    code = row.get("FCODE")
    if not isinstance(code, str) or len(code) != 6 or not code.isascii() or not code.isdigit():
        return None, "invalid_identity"
    name = row.get("SHORTNAME")
    if not isinstance(name, str) or not name.strip():
        return None, "name_missing"
    try:
        nav_date = date.fromisoformat(str(row.get("PDATE") or ""))
    except ValueError:
        return None, "nav_date_invalid"
    age = (today - nav_date).days
    if age < 0 or age > max_age_days:
        return None, "nav_date_future" if age < 0 else "stale"
    nav, _ = number_field(row, "NAV")
    if nav is None or nav <= 0:
        return None, "nav_invalid"
    result: dict[str, Any] = {"code": code, "name": name.strip(), "netWorthDate": nav_date.isoformat(),
                              "_source": SOURCE, "_raw_fields": dict(row), "_field_states": {}}
    for field, target in FIELD_MAP.items():
        result[target], result["_field_states"][target] = number_field(row, field)
    # Manager, type, scale and holding period have no equivalent here. Do not
    # copy local values into a response pretending that this API returned them.
    return result, None


def fetch_fund_snapshots(
    settings: Any, codes: list[str], *, max_codes: int = 300,
    post: Callable[..., Any] | None = None, now: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    # Share connections across this query's batches and backup requests. A
    # request-local session avoids sharing mutable transport state between
    # Flask threads and always closes, including on validation/storage errors.
    if post is not None:
        return _fetch_fund_snapshots(settings, codes, max_codes=max_codes, post=post, now=now)
    with requests.Session() as session:
        return _fetch_fund_snapshots(settings, codes, max_codes=max_codes, post=session.post, now=now)


def _fetch_fund_snapshots(
    settings: Any, codes: list[str], *, max_codes: int,
    post: Callable[..., Any], now: datetime | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    clock = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    normalized = list(dict.fromkeys(str(value).strip() for value in codes))
    if any(len(code) != 6 or not code.isascii() or not code.isdigit() for code in normalized):
        raise ValueError("invalid_fund_code")
    selected = normalized[:max(0, max_codes)]
    endpoints = list(dict.fromkeys([settings.tiantian_valuation_primary_url, settings.tiantian_valuation_fallback_url]))
    audit_dir = Path(settings.reports_dir) / "audit" / "fund_snapshot_observations" / clock.strftime("%Y%m%d")
    result: dict[str, dict[str, Any]] = {}
    attempts: list[dict[str, Any]] = []
    max_age = int(getattr(settings, "publication_max_snapshot_age_days", 10))
    for offset in range(0, len(selected), 50):
        pending = selected[offset:offset + 50]
        for endpoint in endpoints:
            if not pending:
                break
            observed_at = datetime.now(SHANGHAI).isoformat()
            attempt: dict[str, Any] = {"endpoint": endpoint, "requested_codes": list(pending),
                                      "fields": FIELDS, "fetched_at": observed_at,
                                      "accepted_codes": [], "rejected_records": []}
            try:
                response = post(endpoint, data={"FCODES": ",".join(pending), "FIELDS": FIELDS},
                                headers=HEADERS, timeout=(5, 12))
                attempt["http_status"] = response.status_code
                body = response.content
                digest = hashlib.sha256(body).hexdigest()
                audit_dir.mkdir(parents=True, exist_ok=True)
                body_path = audit_dir / f"{digest}.response.json"
                if not body_path.exists():
                    body_path.write_bytes(body)
                attempt.update(response_sha256=digest, response_artifact=str(body_path))
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or payload.get("success") is not True or str(payload.get("errorCode")) != "0":
                    raise ValueError("business_response_failed")
                rows = payload.get("data")
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ValueError("invalid_data_shape")
                observed_codes = [row.get("FCODE") for row in rows]
                if any(not isinstance(code, str) or code not in pending for code in observed_codes):
                    raise ValueError("unexpected_identity")
                if len(set(observed_codes)) != len(observed_codes):
                    raise ValueError("duplicate_identity")
                attempt["response_state"] = "rows" if rows else "empty"
                for row in rows:
                    snapshot, reason = normalize_snapshot(row, today=clock.date(), max_age_days=max_age)
                    if snapshot is None:
                        attempt["rejected_records"].append({"code": row.get("FCODE"), "reason": reason})
                        continue
                    snapshot["_source_evidence"] = {"endpoint": endpoint, "fetched_at": observed_at,
                                                   "response_sha256": digest, "response_artifact": str(body_path)}
                    result[snapshot["code"]] = snapshot
                    attempt["accepted_codes"].append(snapshot["code"])
                pending = [code for code in pending if code not in result]
            except (requests.RequestException, ValueError, TypeError) as exc:
                attempt.update(error_type=type(exc).__name__, error=str(exc), response_state="failed")
            attempts.append(attempt)
            # Even failed business responses and transport failures get their
            # own evidence. No missing response hash is invented.
            audit_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = audit_dir / f"{datetime.now(SHANGHAI).strftime('%H%M%S_%f')}.json"
            metadata_path.write_text(json.dumps(attempt, ensure_ascii=False, indent=2), encoding="utf-8")
    missing = [code for code in normalized if code not in result]
    meta = {"status": "empty" if not normalized else "ok" if not missing else "partial",
            "source": SOURCE, "supplier_group": "eastmoney_tiantian", "independent_source": False,
            "requested_count": len(normalized), "returned_count": len(result),
            "upstream_requested_count": len(selected), "upstream_returned_count": len(result),
            "missing_codes": missing, "skipped_over_limit_count": len(normalized) - len(selected),
            "max_request_codes": max_codes, "effective_batch_size": 50, "batch_count": len(attempts),
            "source_complete": not missing, "attempts": attempts,
            "quota_unavailable": False, "quota_exhausted": False,
            "request_source": "favorites_score", "field_mapping": FIELD_MAP}
    return result, meta
