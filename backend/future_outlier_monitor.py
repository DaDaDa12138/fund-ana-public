from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from core.utils.buyability import normalize_buyability
from backend.plain_open_tail_follow_up import load_codes_from_latest_audit


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_monitor_snapshot(settings: Settings, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []

    conn = sqlite3.connect(str(settings.database_path))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join(["?"] * len(codes))
        rows = conn.execute(
            f"""
            WITH latest_risk AS (
                SELECT
                    r1.fcode,
                    r1.end_date AS latest_risk_end_date,
                    r1.annual_return,
                    COALESCE(r1.raw_sharpe_ratio, r1.sharpe_ratio) AS sharpe_ratio,
                    r1.max_drawdown,
                    r1.volatility
                FROM fund_risk_metrics r1
                WHERE r1.period = '1y'
                  AND r1.rowid = (
                      SELECT latest.rowid
                      FROM fund_risk_metrics latest
                      WHERE latest.fcode = r1.fcode
                        AND latest.period = r1.period
                      ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC
                      LIMIT 1
                  )
            ),
            nav_history_latest AS (
                SELECT fcode, MAX(nav_date) AS nav_history_latest_date
                FROM fund_nav_history
                WHERE fcode IN ({placeholders})
                GROUP BY fcode
            )
            SELECT
                f.fcode,
                COALESCE(f.shortname, '') AS shortname,
                COALESCE(f.ftype, '') AS ftype,
                f.is_buyable,
                COALESCE(f.purchase_status, '') AS purchase_status,
                COALESCE(fr.latest_nav_date, '') AS latest_nav_date,
                fr.latest_nav AS latest_nav,
                fr.daily_growth AS daily_growth,
                COALESCE(nh.nav_history_latest_date, '') AS nav_history_latest_date,
                COALESCE(lr.latest_risk_end_date, '') AS latest_risk_end_date,
                lr.annual_return,
                lr.sharpe_ratio,
                lr.max_drawdown,
                lr.volatility
            FROM funds f
            LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
            LEFT JOIN nav_history_latest nh ON nh.fcode = f.fcode
            LEFT JOIN latest_risk lr ON lr.fcode = f.fcode
            WHERE f.fcode IN ({placeholders})
            ORDER BY f.fcode
            """,
            tuple(codes) + tuple(codes),
        ).fetchall()
    finally:
        conn.close()

    snapshot: list[dict[str, Any]] = []
    for row in rows:
        snapshot.append(
            {
                "fcode": _normalize_code(row["fcode"]),
                "shortname": str(row["shortname"] or "").strip(),
                "ftype": str(row["ftype"] or "").strip(),
                "is_buyable": normalize_buyability(row["is_buyable"]),
                "purchase_status": str(row["purchase_status"] or "").strip(),
                "latest_nav_date": str(row["latest_nav_date"] or "").strip() or None,
                "latest_nav": row["latest_nav"],
                "daily_growth": row["daily_growth"],
                "nav_history_latest_date": str(row["nav_history_latest_date"] or "").strip() or None,
                "latest_risk_end_date": str(row["latest_risk_end_date"] or "").strip() or None,
                "annual_return_1y": row["annual_return"],
                "sharpe_ratio_1y": row["sharpe_ratio"],
                "max_drawdown_1y": row["max_drawdown"],
                "volatility_1y": row["volatility"],
            }
        )
    return snapshot


def run_future_outlier_monitor(
    settings: Settings,
    *,
    category: str = "future_outlier_monitor",
    codes: list[str] | None = None,
    log: Any | None = None,
) -> dict[str, Any]:
    logger = log or (lambda message: None)
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for raw in (codes or load_codes_from_latest_audit(settings, category)):
        code = _normalize_code(raw)
        if not code or code in seen:
            continue
        seen.add(code)
        normalized_codes.append(code)

    monitored = _load_monitor_snapshot(settings, normalized_codes)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "category": str(category or "").strip(),
        "requested_codes": normalized_codes,
        "requested_total": len(normalized_codes),
        "monitored_total": len(monitored),
        "outlier_still_present_total": len(monitored),
        "processed_total": len(monitored),
        "changed_total": 0,
        "status": "success",
        "idle_reason": "no_candidates_from_latest_audit" if not normalized_codes else None,
        "codes_snapshot": monitored,
    }
    artifact_dir = settings.reports_dir / "audit" / "future_outlier_monitor"
    artifact_path = artifact_dir / f"future_outlier_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    _write_json(artifact_path, payload)
    payload["artifact_path"] = str(artifact_path)
    logger(
        "future_outlier 轻量监控完成: "
        f"requested_total={len(normalized_codes)}, "
        f"monitored_total={len(monitored)}"
    )
    return payload
