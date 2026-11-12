from __future__ import annotations

import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from core.utils.buyability import normalize_buyability
from backend.core_runner import resolve_core_python
from backend.nav_history_maintenance import _backfill_returns_from_nav_history, _build_context
from backend.risk_coverage_gap_audit import (
    build_risk_coverage_gap_audit,
    load_latest_risk_coverage_gap_audit_payload,
)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_codes_from_latest_audit(settings: Settings, category: str) -> list[str]:
    _path, payload = load_latest_risk_coverage_gap_audit_payload(settings)
    audit_payload = payload or {}
    category_codes = audit_payload.get("non_candidate_tail_codes_by_category") or {}
    if not category_codes:
        audit_payload = build_risk_coverage_gap_audit(settings, sample_limit=8)
        category_codes = audit_payload.get("non_candidate_tail_codes_by_category") or {}
    raw_codes = category_codes.get(category) or []
    codes: list[str] = []
    seen: set[str] = set()
    for raw in raw_codes:
        code = _normalize_code(raw)
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def load_current_returns_snapshot(settings: Settings, codes: list[str]) -> dict[str, dict[str, Any]]:
    if not codes:
        return {}

    conn = sqlite3.connect(str(settings.database_path))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join(["?"] * len(codes))
        rows = conn.execute(
            f"""
            SELECT
                f.fcode,
                COALESCE(f.shortname, '') AS shortname,
                f.is_buyable,
                COALESCE(f.purchase_status, '') AS purchase_status,
                COALESCE(fr.latest_nav_date, '') AS latest_nav_date,
                COALESCE(fr.latest_nav, 0) AS latest_nav,
                COALESCE(fr.daily_growth, 0) AS daily_growth
            FROM funds f
            LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
            WHERE f.fcode IN ({placeholders})
            ORDER BY f.fcode
            """,
            tuple(codes),
        ).fetchall()
    finally:
        conn.close()

    snapshot: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row["fcode"])
        snapshot[code] = {
            "fcode": code,
            "shortname": str(row["shortname"] or "").strip(),
            "is_buyable": normalize_buyability(row["is_buyable"]),
            "purchase_status": str(row["purchase_status"] or "").strip(),
            "latest_nav_date": str(row["latest_nav_date"] or "").strip() or None,
            "latest_nav": row["latest_nav"],
            "daily_growth": row["daily_growth"],
        }
    return snapshot


def run_changed_nav_risk_refresh(settings: Settings) -> dict[str, Any]:
    command = [
        resolve_core_python(settings),
        "main.py",
        "calculate-risk",
        "--yes",
        "--workers",
        str(settings.risk_workers),
        "--changed-nav-only",
        "--force",
    ]
    completed = subprocess.run(
        command,
        cwd=str(settings.core_root),
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def run_plain_open_tail_follow_up(
    settings: Settings,
    *,
    category: str = "plain_open_tail_follow_up",
    codes: list[str] | None = None,
    skip_risk_refresh: bool = False,
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

    before_snapshot = load_current_returns_snapshot(settings, normalized_codes)
    ctx = _build_context(settings)
    processed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for code in normalized_codes:
        before = before_snapshot.get(code) or {"fcode": code}
        try:
            inserted_records = int(ctx.nav_collector.collect_fund_history(code, force_update=False) or 0)
            _backfill_returns_from_nav_history(ctx, code)
            processed.append(
                {
                    "fcode": code,
                    "shortname": before.get("shortname"),
                    "before_latest_nav_date": before.get("latest_nav_date"),
                    "inserted_records": inserted_records,
                }
            )
        except Exception as exc:
            errors.append(
                {
                    "fcode": code,
                    "shortname": before.get("shortname"),
                    "before_latest_nav_date": before.get("latest_nav_date"),
                    "error": str(exc),
                }
            )

    after_snapshot = load_current_returns_snapshot(settings, normalized_codes)
    changed_codes = [
        code
        for code in normalized_codes
        if (before_snapshot.get(code) or {}).get("latest_nav_date")
        != (after_snapshot.get(code) or {}).get("latest_nav_date")
    ]

    risk_refresh = None
    if changed_codes and not skip_risk_refresh:
        risk_refresh = run_changed_nav_risk_refresh(settings)

    latest_gap_payload = build_risk_coverage_gap_audit(settings, sample_limit=8)
    artifact_dir = settings.reports_dir / "audit" / "plain_open_tail_follow_up"
    artifact_path = artifact_dir / f"plain_open_tail_follow_up_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "category": str(category or "").strip(),
        "requested_codes": normalized_codes,
        "requested_total": len(normalized_codes),
        "processed_total": len(processed),
        "changed_total": len(changed_codes),
        "error_total": len(errors),
        "status": "success",
        "idle_reason": "no_candidates_from_latest_audit" if not normalized_codes else None,
        "changed_codes": changed_codes,
        "processed": processed,
        "errors": errors,
        "before_snapshot": before_snapshot,
        "after_snapshot": after_snapshot,
        "risk_refresh": risk_refresh,
        "latest_risk_coverage_gap_artifact_path": latest_gap_payload.get("artifact_path"),
        "latest_non_candidate_tail_category_counts": latest_gap_payload.get("non_candidate_tail_category_counts") or {},
        "latest_priority_bucket_counts": latest_gap_payload.get("priority_bucket_counts") or {},
    }
    _write_json(artifact_path, payload)
    payload["artifact_path"] = str(artifact_path)
    logger(
        "普通开放型旧尾差低频跟进完成: "
        f"requested_total={len(normalized_codes)}, "
        f"changed_total={len(changed_codes)}, "
        f"error_total={len(errors)}"
    )
    return payload
