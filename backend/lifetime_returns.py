"""Platform lifecycle return is never replaced with a NAV price ratio."""
from __future__ import annotations

from datetime import date
import math
from pathlib import Path
import sqlite3
from typing import Any

from backend.value_contract import numeric_observation

PLATFORM_LABEL = "平台成立以来收益年化(%)"
ACCUM_NAV_LABEL = "累计净值比年化(%)"
FORMULA_VERSION = "lifetime-return-2026-10-10.1"


def load_platform_lifetime_returns(db_path: Path, codes: list[str]) -> dict[str, dict[str, Any]]:
    if not db_path.exists() or not codes:
        return {}
    result = {}
    with sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(fund_returns)")}
        provenance_fields = ("period_return_date", "period_return_observed_at", "period_return_source", "period_return_source_sha256")
        projection = ",".join(f"r.{field}" if field in columns else "NULL" for field in provenance_fields)
        for offset in range(0, len(codes), 500):
            batch = codes[offset:offset + 500]
            placeholders = ",".join("?" for _ in batch)
            try:
                rows = conn.execute(f"""SELECT f.fcode, f.estab_date, r.latest_nav_date, r.return_since, {projection}
                    FROM funds f LEFT JOIN fund_returns r ON f.fcode = r.fcode
                    WHERE f.fcode IN ({placeholders})""", batch).fetchall()
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc) and "no such column" not in str(exc):
                    raise
                rows = []
            for code, start, nav_date, raw, return_date, observed_at, source, source_sha in rows:
                end = return_date if "period_return_date" in columns else nav_date
                observation = numeric_observation({"SYL_LN": raw}, "SYL_LN")
                item = {"value": None, "status": "missing_input", "source": "fund_returns.return_since",
                        "upstream_field": "SYL_LN", "raw_return_pct": raw, "start_date": start,
                        "end_date": end, "formula_version": FORMULA_VERSION,
                        "field_observation": observation, "latest_nav_date": nav_date,
                        "observed_at": observed_at, "provider_endpoint": source, "response_sha256": source_sha,
                        "formula": "100*((1+SYL_LN/100)**(365.25/elapsed_calendar_days)-1)"}
                if return_date is not None and str(return_date) != str(nav_date):
                    item["status"] = "stale"
                    result[str(code)] = item
                    continue
                try:
                    elapsed = (date.fromisoformat(str(end)) - date.fromisoformat(str(start))).days
                    raw_num = observation["value"]
                    if elapsed >= 365 and math.isfinite(raw_num) and raw_num > -100:
                        value = 100 * ((1 + raw_num / 100) ** (365.25 / elapsed) - 1)
                        if math.isfinite(value):
                            item.update(value=value, status="observed", elapsed_days=elapsed)
                except (TypeError, ValueError, OverflowError, ZeroDivisionError):
                    pass
                result[str(code)] = item
    return result


def platform_annualized_map(db_path: Path, codes: list[str]) -> dict[str, str]:
    return {code: f"{item['value']:.2f}".rstrip("0").rstrip(".")
            for code, item in load_platform_lifetime_returns(db_path, codes).items() if item["value"] is not None}
