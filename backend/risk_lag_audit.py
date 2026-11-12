from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from backend.risk_metric_status import load_undefined_risk_windows
from backend.effective_nav import (
    _holding_period_limit_predicate,
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    _table_columns,
    load_effective_nav_snapshot,
)
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics


AUDIT_SAMPLE_LIMIT = 50


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _lag_days(left_date: str, right_date: str) -> int | None:
    left_text = str(left_date or "").strip()
    right_text = str(right_date or "").strip()
    if not left_text or not right_text:
        return None
    try:
        left = datetime.strptime(left_text, "%Y-%m-%d").date()
        right = datetime.strptime(right_text, "%Y-%m-%d").date()
    except ValueError:
        return None
    return max(0, (left - right).days)


def _candidate_cte_sql(conn: sqlite3.Connection) -> str:
    holding_gate_clause = f"AND NOT ({_holding_period_limit_predicate(conn)})"
    columns = _table_columns(conn, "funds")
    fields = {name: f"f.{name}" if name in columns else "NULL"
              for name in ("shortname", "ftype", "type_code")}
    return f"""
        candidate AS (
            SELECT
                r.fcode,
                COALESCE({fields['shortname']}, '') AS shortname,
                COALESCE({fields['ftype']}, '') AS ftype,
                COALESCE({fields['type_code']}, '') AS type_code,
                COALESCE(r.latest_nav_date, '') AS latest_nav_date
            FROM fund_returns r
            LEFT JOIN funds f ON f.fcode = r.fcode
            WHERE r.latest_nav_date = ?
            {holding_gate_clause}
        )
    """


def _latest_ready_risk_cte_sql(conn: sqlite3.Connection, cte_name: str = "latest_risk") -> str:
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    return f"""
        {cte_name} AS (
            SELECT
                risk.fcode,
                risk.end_date AS latest_risk_end_date
            FROM fund_risk_metrics risk
            WHERE risk.period = '1y'
              AND risk.end_date IS NOT NULL
              AND TRIM(risk.end_date) <> ''
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
        )
    """


def _effective_ready_risk_cte_sql(conn: sqlite3.Connection, cte_name: str = "effective_risk") -> str:
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    return f"""
        {cte_name} AS (
            SELECT DISTINCT risk.fcode
            FROM fund_risk_metrics risk
            WHERE risk.period = '1y'
              AND COALESCE(TRIM(risk.end_date), '') = ?
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
        )
    """


def _load_candidate_risk_distribution(conn: sqlite3.Connection, candidate_date: str) -> list[dict[str, Any]]:
    candidate_cte = _candidate_cte_sql(conn)
    latest_risk_cte = _latest_ready_risk_cte_sql(conn)
    rows = conn.execute(
        f"""
        WITH {candidate_cte},
        {latest_risk_cte}
        SELECT
            COALESCE(NULLIF(TRIM(latest_risk.latest_risk_end_date), ''), '<missing>') AS latest_risk_end_date,
            COUNT(*) AS fund_count
        FROM candidate
        LEFT JOIN latest_risk ON latest_risk.fcode = candidate.fcode
        GROUP BY COALESCE(NULLIF(TRIM(latest_risk.latest_risk_end_date), ''), '<missing>')
        ORDER BY
            CASE
                WHEN COALESCE(NULLIF(TRIM(latest_risk.latest_risk_end_date), ''), '<missing>') = '<missing>' THEN 1
                ELSE 0
            END ASC,
            latest_risk_end_date DESC
        LIMIT 20
        """,
        (candidate_date,),
    ).fetchall()
    return [
        {
            "latest_risk_end_date": str(row[0] or "").strip(),
            "fund_count": int(row[1] or 0),
        }
        for row in rows
    ]


def _uncovered_candidate_total(
    conn: sqlite3.Connection,
    candidate_date: str,
    effective_risk_end_date: str,
) -> int:
    if not candidate_date:
        return 0
    candidate_cte = _candidate_cte_sql(conn)
    if not effective_risk_end_date:
        row = conn.execute(
            f"""
            WITH {candidate_cte}
            SELECT COUNT(*)
            FROM candidate
            """,
            (candidate_date,),
        ).fetchone()
        return int((row or [0])[0] or 0)

    effective_risk_cte = _effective_ready_risk_cte_sql(conn)
    row = conn.execute(
        f"""
        WITH {candidate_cte},
        {effective_risk_cte}
        SELECT COUNT(*)
        FROM candidate
        LEFT JOIN effective_risk ON effective_risk.fcode = candidate.fcode
        WHERE effective_risk.fcode IS NULL
        """,
        (candidate_date, effective_risk_end_date),
    ).fetchone()
    return int((row or [0])[0] or 0)


def _uncovered_candidate_ftype_counts(
    conn: sqlite3.Connection,
    candidate_date: str,
    effective_risk_end_date: str,
) -> dict[str, int]:
    if not candidate_date:
        return {}
    candidate_cte = _candidate_cte_sql(conn)
    if not effective_risk_end_date:
        rows = conn.execute(
            f"""
            WITH {candidate_cte}
            SELECT COALESCE(NULLIF(TRIM(candidate.ftype), ''), '<empty>') AS ftype, COUNT(*) AS cnt
            FROM candidate
            GROUP BY COALESCE(NULLIF(TRIM(candidate.ftype), ''), '<empty>')
            ORDER BY cnt DESC, ftype ASC
            LIMIT 20
            """,
            (candidate_date,),
        ).fetchall()
        return {str(row[0] or "").strip(): int(row[1] or 0) for row in rows}

    effective_risk_cte = _effective_ready_risk_cte_sql(conn)
    rows = conn.execute(
        f"""
        WITH {candidate_cte},
        {effective_risk_cte}
        SELECT candidate.ftype, COUNT(*) AS cnt
        FROM candidate
        LEFT JOIN effective_risk ON effective_risk.fcode = candidate.fcode
        WHERE effective_risk.fcode IS NULL
        GROUP BY candidate.ftype
        ORDER BY cnt DESC, candidate.ftype ASC
        LIMIT 20
        """,
        (candidate_date, effective_risk_end_date),
    ).fetchall()
    return {str(row[0] or "").strip(): int(row[1] or 0) for row in rows}


def _uncovered_candidate_samples(
    conn: sqlite3.Connection,
    candidate_date: str,
    effective_risk_end_date: str,
) -> list[dict[str, Any]]:
    if not candidate_date:
        return []
    candidate_cte = _candidate_cte_sql(conn)
    if not effective_risk_end_date:
        rows = conn.execute(
            f"""
            WITH {candidate_cte}
            SELECT
                candidate.fcode,
                candidate.shortname,
                candidate.ftype,
                candidate.type_code,
                candidate.latest_nav_date
            FROM candidate
            ORDER BY COALESCE(candidate.ftype, '') ASC, candidate.fcode ASC
            LIMIT ?
            """,
            (candidate_date, AUDIT_SAMPLE_LIMIT),
        ).fetchall()
        return [
            {
                "fcode": str(row[0] or "").strip(),
                "shortname": str(row[1] or "").strip(),
                "ftype": str(row[2] or "").strip(),
                "type_code": str(row[3] or "").strip(),
                "latest_nav_date": str(row[4] or "").strip(),
                "latest_risk_end_date": "",
            }
            for row in rows
        ]

    effective_risk_cte = _effective_ready_risk_cte_sql(conn)
    latest_risk_cte = _latest_ready_risk_cte_sql(conn)
    rows = conn.execute(
        f"""
        WITH {candidate_cte},
        {effective_risk_cte},
        {latest_risk_cte}
        SELECT
            candidate.fcode,
            candidate.shortname,
            candidate.ftype,
            candidate.type_code,
            candidate.latest_nav_date,
            COALESCE(latest_risk.latest_risk_end_date, '') AS latest_risk_end_date
        FROM candidate
        LEFT JOIN effective_risk ON effective_risk.fcode = candidate.fcode
        LEFT JOIN latest_risk ON latest_risk.fcode = candidate.fcode
        WHERE effective_risk.fcode IS NULL
        ORDER BY
            CASE WHEN COALESCE(latest_risk.latest_risk_end_date, '') = '' THEN 0 ELSE 1 END ASC,
            latest_risk.latest_risk_end_date ASC,
            candidate.ftype ASC,
            candidate.fcode ASC
        LIMIT ?
        """,
        (candidate_date, effective_risk_end_date, AUDIT_SAMPLE_LIMIT),
    ).fetchall()
    return [
        {
            "fcode": str(row[0] or "").strip(),
            "shortname": str(row[1] or "").strip(),
            "ftype": str(row[2] or "").strip(),
            "type_code": str(row[3] or "").strip(),
            "latest_nav_date": str(row[4] or "").strip(),
            "latest_risk_end_date": str(row[5] or "").strip(),
        }
        for row in rows
    ]


def load_latest_risk_lag_audit_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "risk_lag_audit"
    if not artifact_dir.exists():
        return None, None
    files = sorted(artifact_dir.glob("risk_lag_audit_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def build_risk_lag_audit(
    settings: Settings,
    *,
    artifact_dir: Path | None = None,
    artifact_path: Path | None = None,
    runtime_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        snapshot = load_effective_nav_snapshot(
            settings.database_path,
            coverage_ratio=settings.effective_nav_coverage_ratio,
            weekday_only=settings.effective_nav_weekday_only,
        )
        snapshot_payload = snapshot.to_dict()
        candidate_date = str(snapshot.candidate_date or "").strip()
        risk_1y_latest_end_date = str(snapshot.risk_1y_latest_end_date or snapshot.risk_1y_max_end_date or "").strip()
        risk_1y_effective_end_date = str(snapshot.risk_1y_effective_end_date or "").strip()
        risk_1y_latest_fund_count = int(snapshot.risk_1y_latest_fund_count or 0)
        risk_1y_effective_fund_count = int(snapshot.risk_1y_effective_fund_count or 0)
        candidate_fund_count = int(snapshot.candidate_fund_count or 0)
        coverage_ratio_vs_candidate = snapshot.risk_1y_effective_coverage_ratio_vs_candidate
    except sqlite3.Error as exc:
        snapshot_payload = {}
        candidate_date = ""
        risk_1y_latest_end_date = ""
        risk_1y_effective_end_date = ""
        risk_1y_latest_fund_count = 0
        risk_1y_effective_fund_count = 0
        candidate_fund_count = 0
        coverage_ratio_vs_candidate = None
        audit_error = str(exc)
    else:
        audit_error = None

    latest_distribution: list[dict[str, Any]] = []
    uncovered_candidate_total = 0
    uncovered_candidate_ftype_counts: dict[str, int] = {}
    uncovered_candidate_samples: list[dict[str, Any]] = []
    database_path = Path(settings.database_path)
    if database_path.exists():
        try:
            conn = sqlite3.connect(str(database_path))
            conn.row_factory = sqlite3.Row
            try:
                latest_distribution = _load_candidate_risk_distribution(conn, candidate_date) if candidate_date else []
                uncovered_candidate_total = _uncovered_candidate_total(
                    conn,
                    candidate_date,
                    risk_1y_effective_end_date,
                )
                uncovered_candidate_ftype_counts = _uncovered_candidate_ftype_counts(
                    conn,
                    candidate_date,
                    risk_1y_effective_end_date,
                )
                uncovered_candidate_samples = _uncovered_candidate_samples(
                    conn,
                    candidate_date,
                    risk_1y_effective_end_date,
                )
                undefined_windows = load_undefined_risk_windows(conn)
                for sample in uncovered_candidate_samples:
                    window = undefined_windows.get((sample["fcode"], "1y"))
                    if window and window["end_date"] == sample["latest_nav_date"]:
                        sample.update(
                            calculation_status=window["calculation_status"],
                            computed_risk_end_date=window["end_date"],
                            undefined_metric_reasons=window["undefined_metric_reasons"],
                        )
            finally:
                conn.close()
        except sqlite3.Error as exc:
            audit_error = str(exc)
    else:
        audit_error = audit_error or "database_missing"

    dates_aligned = bool(candidate_date and risk_1y_effective_end_date and candidate_date == risk_1y_effective_end_date)
    threshold_semantics = build_risk_coverage_threshold_semantics(
        {"coverage_ratio_vs_candidate": coverage_ratio_vs_candidate}
    )
    coverage_sufficient = bool(threshold_semantics["coverage_sufficient"])

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "effective_nav_snapshot": snapshot_payload,
        "candidate_date": candidate_date or None,
        "risk_1y_latest_end_date": risk_1y_latest_end_date or None,
        "risk_1y_latest_fund_count": risk_1y_latest_fund_count,
        "risk_1y_effective_end_date": risk_1y_effective_end_date or None,
        "risk_1y_effective_fund_count": risk_1y_effective_fund_count,
        "candidate_fund_count": candidate_fund_count,
        **threshold_semantics,
        "lag_days_candidate_vs_latest": _lag_days(candidate_date, risk_1y_latest_end_date),
        "lag_days_candidate_vs_effective": _lag_days(candidate_date, risk_1y_effective_end_date),
        "dates_aligned": dates_aligned,
        "coverage_sufficient": coverage_sufficient,
        "aligned": bool(snapshot_payload.get("ready")) and dates_aligned and coverage_sufficient,
        "candidate_latest_risk_distribution_preview": latest_distribution,
        "effective_risk_uncovered_candidate_total": uncovered_candidate_total,
        "effective_risk_uncovered_candidate_ftype_counts": uncovered_candidate_ftype_counts,
        "effective_risk_uncovered_candidate_samples": uncovered_candidate_samples,
        "audit_error": audit_error,
    }

    payload.update(dict(runtime_scope or {}))

    reports_dir = getattr(settings, "reports_dir", None)
    resolved_artifact_path = artifact_path
    if resolved_artifact_path is None and reports_dir:
        resolved_artifact_dir = artifact_dir or (Path(reports_dir) / "audit" / "risk_lag_audit")
        resolved_artifact_dir.mkdir(parents=True, exist_ok=True)
        resolved_artifact_path = resolved_artifact_dir / f"risk_lag_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    if resolved_artifact_path is not None:
        _write_json(resolved_artifact_path, payload)
    payload["artifact_path"] = str(resolved_artifact_path) if resolved_artifact_path else None
    return payload
