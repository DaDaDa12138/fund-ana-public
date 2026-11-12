#!/usr/bin/env python3
"""Probe public fund endpoints for explicit holding-period evidence.

This is an audit tool.  It never writes the production database and it never
turns a missing field or ``lock_fund_type=0`` into a zero-day holding period.
The JSON output keeps the request URL, observation time and response hash so a
later review can distinguish a live observation from a durable data contract.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _path in (PROJECT_ROOT, PROJECT_ROOT / "core"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
DEFAULT_CODES = (
    "005138", "005139", "018604", "164703", "023406", "004400", "004401",
    "013522", "013523", "000086", "000078", "000003", "025582",
)


def _probe(code: str, *, timeout: float) -> dict[str, Any]:
    url = f"https://danjuanapp.com/djapi/fund/{code}"
    observed_at = datetime.now().astimezone().isoformat()
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": "Fund-ANA-holding-audit/1.0",
                "Referer": "https://danjuanapp.com/",
            },
            timeout=timeout,
        )
        body_sha256 = hashlib.sha256(response.content).hexdigest()
        try:
            envelope = response.json()
        except ValueError:
            envelope = None
        data = envelope.get("data") if isinstance(envelope, dict) else None
        if not isinstance(data, dict):
            return {
                "fcode": code,
                "url": url,
                "observed_at": observed_at,
                "http_status": response.status_code,
                "business_success": False,
                "response_sha256": body_sha256,
                "error": (envelope or {}).get("message") if isinstance(envelope, dict) else "non_json_or_empty_data",
                "fields": {},
                "explicit_holding_days": None,
            }
        fields = {
            key: data.get(key)
            for key in (
                "fd_code", "fd_name", "close_period", "close_period_describe",
                "hold_lock_period", "lock_fund_type", "subscribe_status", "withdraw_status",
            )
        }
        from core.utils.danjuan_holding import parse_danjuan_holding_period

        match = parse_danjuan_holding_period(envelope)
        return {
            "fcode": code,
            "url": url,
            "observed_at": observed_at,
            "http_status": response.status_code,
            "business_success": response.status_code == 200 and bool(data.get("fd_code")),
            "identity_match": str(data.get("fd_code") or "").strip() == code,
            "response_sha256": body_sha256,
            "fields": fields,
            "explicit_holding_days": match.days if match else None,
            "explicit_holding_keyword": match.keyword if match else None,
        }
    except requests.RequestException as exc:
        return {
            "fcode": code,
            "url": url,
            "observed_at": observed_at,
            "http_status": None,
            "business_success": False,
            "response_sha256": None,
            "error": f"{type(exc).__name__}: {exc}",
            "fields": {},
            "explicit_holding_days": None,
        }


def run(codes: list[str], *, timeout: float, interval: float) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for index, code in enumerate(codes):
        if index:
            time.sleep(max(0.0, interval))
        rows.append(_probe(code, timeout=timeout))
    successful = [row for row in rows if row.get("business_success") and row.get("identity_match")]
    explicit = [row for row in successful if row.get("explicit_holding_days") is not None]
    return {
        "schema_version": 1,
        "generated_at": datetime.now().astimezone().isoformat(),
        "provider": "danjuan_public_detail",
        "supplier_group": "danjuan",
        "independent_source": None,
        "endpoint_template": "https://danjuanapp.com/djapi/fund/{fcode}",
        "scope": "read-only live probe; no database writes",
        "requested_count": len(codes),
        "business_success_count": len(successful),
        "explicit_holding_count": len(explicit),
        "explicit_holding_coverage": round(len(explicit) / len(successful), 4) if successful else 0.0,
        "missing_fields_are_unknown": True,
        "zero_lock_marker_is_not_unrestricted_proof": True,
        "integration_recommendation": (
            "shadow_or_supplement_only; accept explicit hold_lock_period/close_period; "
            "do not write holding_days=0 from empty fields or lock_fund_type=0"
        ),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codes", nargs="+", default=list(DEFAULT_CODES))
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    codes = [str(code).strip().zfill(6) for code in args.codes if str(code).strip()]
    result = run(codes, timeout=max(1.0, args.timeout), interval=args.interval)
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
