from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from backend.effective_nav import load_effective_nav_snapshot


AUDIT_SAMPLE_LIMIT = 50


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_distribution(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT latest_nav_date, COUNT(*) AS fund_count
        FROM fund_returns
        WHERE latest_nav_date IS NOT NULL
        GROUP BY latest_nav_date
        ORDER BY latest_nav_date DESC
        """
    ).fetchall()
    return [
        {
            "latest_nav_date": str(row[0] or "").strip(),
            "fund_count": int(row[1] or 0),
        }
        for row in rows
        if str(row[0] or "").strip()
    ]


def _bucket_counts(conn: sqlite3.Connection, nav_date: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(f.ftype), ''), '<empty>') AS ftype, COUNT(*) AS cnt
        FROM funds f
        JOIN fund_returns r ON r.fcode = f.fcode
        WHERE r.latest_nav_date = ?
        GROUP BY COALESCE(NULLIF(TRIM(f.ftype), ''), '<empty>')
        ORDER BY cnt DESC, ftype ASC
        LIMIT 20
        """,
        (nav_date,),
    ).fetchall()
    return {str(row[0] or "").strip(): int(row[1] or 0) for row in rows}


def _sample_rows(conn: sqlite3.Connection, nav_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            f.fcode,
            COALESCE(f.shortname, '') AS shortname,
            COALESCE(f.ftype, '') AS ftype,
            COALESCE(f.type_code, '') AS type_code,
            COALESCE(r.latest_nav_date, '') AS latest_nav_date
        FROM funds f
        JOIN fund_returns r ON r.fcode = f.fcode
        WHERE r.latest_nav_date = ?
        ORDER BY COALESCE(f.ftype, '') ASC, f.fcode ASC
        LIMIT ?
        """,
        (nav_date, AUDIT_SAMPLE_LIMIT),
    ).fetchall()
    return [
        {
            "fcode": str(row[0] or "").strip(),
            "shortname": str(row[1] or "").strip(),
            "ftype": str(row[2] or "").strip(),
            "type_code": str(row[3] or "").strip(),
            "latest_nav_date": str(row[4] or "").strip(),
        }
        for row in rows
    ]


def load_latest_nav_lag_audit_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "nav_lag_audit"
    if not artifact_dir.exists():
        return None, None
    files = sorted(artifact_dir.glob("nav_lag_audit_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def build_nav_lag_audit(settings: Settings) -> dict[str, Any]:
    try:
        snapshot = load_effective_nav_snapshot(
            settings.database_path,
            coverage_ratio=settings.effective_nav_coverage_ratio,
            weekday_only=settings.effective_nav_weekday_only,
        )
        snapshot_payload = snapshot.to_dict()
        latest_nav_date = str(snapshot.raw_max_latest_nav_date or "").strip()
        previous_nav_date = str(snapshot.previous_eligible_nav_date or "").strip()
        latest_nav_fund_count = int(snapshot.candidate_fund_count or 0)
        previous_nav_fund_count = int(snapshot.previous_eligible_fund_count or 0)
        fresh_window_fund_count = int(snapshot.fresh_window_fund_count or 0)
        coverage_ratio_vs_fresh_window = snapshot.coverage_ratio_vs_fresh_window
    except sqlite3.Error as exc:
        snapshot = None
        snapshot_payload = {}
        latest_nav_date = ""
        previous_nav_date = ""
        latest_nav_fund_count = 0
        previous_nav_fund_count = 0
        fresh_window_fund_count = 0
        coverage_ratio_vs_fresh_window = None
        audit_error = str(exc)
    else:
        audit_error = None
    distribution: list[dict[str, Any]] = []
    latest_bucket_counts: dict[str, int] = {}
    previous_bucket_counts: dict[str, int] = {}
    previous_samples: list[dict[str, Any]] = []

    database_path = Path(settings.database_path)
    if database_path.exists():
        try:
            conn = sqlite3.connect(str(database_path))
            try:
                distribution = _load_distribution(conn)
                latest_bucket_counts = _bucket_counts(conn, latest_nav_date) if latest_nav_date else {}
                previous_bucket_counts = _bucket_counts(conn, previous_nav_date) if previous_nav_date else {}
                previous_samples = _sample_rows(conn, previous_nav_date) if previous_nav_date else []
            finally:
                conn.close()
        except sqlite3.Error as exc:
            audit_error = str(exc)
    else:
        audit_error = audit_error or "database_missing"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "effective_nav_snapshot": snapshot_payload,
        "latest_nav_date": latest_nav_date or None,
        "previous_nav_date": previous_nav_date or None,
        "latest_nav_fund_count": latest_nav_fund_count,
        "previous_nav_fund_count": previous_nav_fund_count,
        "fresh_window_fund_count": fresh_window_fund_count,
        "coverage_ratio_vs_fresh_window": coverage_ratio_vs_fresh_window,
        "latest_nav_ftype_counts": latest_bucket_counts,
        "previous_nav_ftype_counts": previous_bucket_counts,
        "previous_nav_samples": previous_samples,
        "distribution_preview": distribution[:20],
        "audit_error": audit_error,
    }

    reports_dir = getattr(settings, "reports_dir", None)
    artifact_path: Path | None = None
    if reports_dir:
        artifact_dir = Path(reports_dir) / "audit" / "nav_lag_audit"
        artifact_path = artifact_dir / f"nav_lag_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        _write_json(artifact_path, payload)
    payload["artifact_path"] = str(artifact_path) if artifact_path else None
    return payload
