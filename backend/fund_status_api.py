"""Read-only parser and probe for the public Eastmoney fund status table.

The endpoint is a web page data feed rather than a documented JSON API.  It is
useful for purchase/redemption status and (for some fixed-open funds) the next
open date.  It does *not* establish a contractual minimum holding period.  This
module deliberately has no database writer and never maps an open status to
``holding_days=0``.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable, Iterable

import requests


ENDPOINT = "https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx"
SOURCE = "eastmoney_fund_purchase_status"
EXPECTED_COLUMNS = 13
FIELD_NAMES = (
    "fcode",
    "shortname",
    "ftype",
    "nav",
    "nav_date_mmdd",
    "purchase_status",
    "redemption_status",
    "next_open_date",
    "purchase_min",
    "daily_limit",
    "discount",
    "is_buyable_flag",
    "sales_fee",
)


class FundStatusPayloadError(ValueError):
    """Raised when the web feed is not a valid, unambiguous status payload."""


def _balanced_value(text: str, start: int, opening: str, closing: str) -> str:
    if start < 0 or start >= len(text) or text[start] != opening:
        raise FundStatusPayloadError("status_payload_value_missing")
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return text[start:index + 1]
    raise FundStatusPayloadError("status_payload_unbalanced")


def parse_status_payload(body: str | bytes) -> dict[str, Any]:
    """Parse and validate ``reData`` from the public status feed.

    The parser only accepts the observed JSON-compatible representation.  A
    changed JavaScript format therefore fails instead of silently shifting
    columns into the wrong business fields.
    """

    if isinstance(body, bytes):
        try:
            text = body.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise FundStatusPayloadError("status_payload_encoding") from exc
    else:
        text = str(body)
    match = re.search(r"(?:^|\b)var\s+reData\s*=\s*", text)
    if not match:
        match = re.search(r"(?:^|\b)reData\s*=\s*", text)
    if not match:
        raise FundStatusPayloadError("status_payload_marker_missing")
    datas_start = text.find("[", match.end())
    if datas_start < 0:
        raise FundStatusPayloadError("status_payload_datas_missing")
    datas_text = _balanced_value(text, datas_start, "[", "]")
    try:
        datas = json.loads(datas_text)
    except json.JSONDecodeError as exc:
        raise FundStatusPayloadError("status_payload_datas_not_json") from exc
    if not isinstance(datas, list):
        raise FundStatusPayloadError("status_payload_datas_not_list")

    records_match = re.search(r"\brecord\s*:\s*[\"']?(\d+)", text[match.end():])
    declared_records = int(records_match.group(1)) if records_match else None
    rows: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(datas):
        if not isinstance(raw, list) or len(raw) != EXPECTED_COLUMNS:
            raise FundStatusPayloadError(f"status_row_shape:{index}")
        values = [str(value).strip() if value is not None else None for value in raw]
        code = values[0] or ""
        if not re.fullmatch(r"\d{6}", code):
            raise FundStatusPayloadError(f"status_row_identity:{index}")
        row = dict(zip(FIELD_NAMES, values, strict=True))
        previous = seen.get(code)
        if previous is not None:
            reason = "status_duplicate_conflict" if previous != row else "status_duplicate_identity"
            raise FundStatusPayloadError(f"{reason}:{code}")
        seen[code] = row
        rows.append(row)
    if declared_records is not None and declared_records != len(datas):
        raise FundStatusPayloadError("status_record_count_mismatch")
    return {
        "rows": rows,
        "declared_records": declared_records,
        "returned_records": len(rows),
        "duplicate_records": 0,
    }


def _clean_codes(codes: Iterable[str] | None) -> list[str]:
    result: list[str] = []
    for value in codes or ():
        code = str(value).strip()
        if not re.fullmatch(r"\d{6}", code):
            raise ValueError("invalid_fund_code")
        if code not in result:
            result.append(code)
    return result


def fetch_status_snapshot(
    *,
    codes: Iterable[str] | None = None,
    endpoint: str = ENDPOINT,
    timeout: tuple[float, float] = (5.0, 20.0),
    get: Callable[..., Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Fetch one full table and return an auditable, read-only observation."""

    selected = _clean_codes(codes)
    request_get = get or requests.get
    observed = (observed_at or datetime.now().astimezone()).isoformat()
    params = {"t": "8", "page": "1,50000", "js": "reData", "sort": "fcode,asc"}
    result: dict[str, Any] = {
        "schema_version": 1,
        "source": SOURCE,
        "supplier_group": "eastmoney_tiantian",
        "independent_source": False,
        "endpoint": endpoint,
        "observed_at": observed,
        "request_params": params,
        "requested_codes": selected,
        "status": "failed",
        "http_status": None,
        "response_sha256": None,
        "rows": [],
        "missing_codes": list(selected),
        "source_complete": False,
        "error": None,
        "holding_days_written": 0,
        "holding_period_inference": "forbidden",
    }
    try:
        response = request_get(endpoint, params=params, headers={"User-Agent": "Fund-ANA-status-audit/1.0"}, timeout=timeout)
        result["http_status"] = getattr(response, "status_code", None)
        body = getattr(response, "content", b"")
        result["response_sha256"] = hashlib.sha256(body).hexdigest()
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        parsed = parse_status_payload(body)
        rows = parsed["rows"]
        if selected:
            rows = [row for row in rows if row["fcode"] in selected]
        result.update(
            status="ok" if not selected or len(rows) == len(selected) else "partial",
            rows=rows,
            missing_codes=[code for code in selected if code not in {row["fcode"] for row in rows}],
            declared_records=parsed["declared_records"],
            returned_records=parsed["returned_records"],
            duplicate_records=parsed["duplicate_records"],
        )
        result["source_complete"] = not result["missing_codes"]
    except (requests.RequestException, FundStatusPayloadError, TypeError, ValueError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def write_snapshot_artifact(snapshot: dict[str, Any], reports_dir: str | Path) -> Path:
    """Persist a JSON observation without touching application data tables."""

    observed = str(snapshot.get("observed_at") or "")
    day = observed[:10].replace("-", "") or "unknown"
    target_dir = Path(reports_dir) / "audit" / "fund_status_api" / day
    target_dir.mkdir(parents=True, exist_ok=True)
    digest = str(snapshot.get("response_sha256") or "no_response")[:16]
    path = target_dir / f"status_{digest}.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


__all__ = [
    "ENDPOINT",
    "FIELD_NAMES",
    "FundStatusPayloadError",
    "fetch_status_snapshot",
    "parse_status_payload",
    "write_snapshot_artifact",
]
