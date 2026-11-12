from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from backend.effective_nav import _latest_risk_rowid_predicate, _risk_metric_ready_predicate, _table_columns
from backend.risk_cadence import has_cadence_tail_signal
from backend.risk_metric_status import load_undefined_risk_windows
from core.utils.holding_period_policy import (
    HOLDING_PERIOD_LIMIT,
    combine_holding_period_names,
    holding_period_gate_reason,
    resolve_holding_period_days,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from cli.aux_commands import _resolve_changed_nav_risk_candidates  # noqa: E402
from utils.project_runtime_config import load_runtime_config  # noqa: E402


AUDIT_SAMPLE_LIMIT = 50
_RISK_PERIOD_DAYS = {
    "1y": 365,
    "3y": 365 * 3,
    "5y": 365 * 5,
}


def _period_required_days(period: str, coverage_grace_days: int) -> int:
    return max(_RISK_PERIOD_DAYS.get(period, 0) - max(0, int(coverage_grace_days or 0)), 1)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_core_config(project_root: Path) -> dict[str, Any]:
    return load_runtime_config(project_root / "config" / "project.yaml")


def _resolve_risk_periods(config: dict[str, Any]) -> list[str]:
    periods = (((config or {}).get("risk_metrics") or {}).get("periods") or ["1y", "3y", "5y"])
    resolved: list[str] = []
    for raw in periods:
        period = str(raw or "").strip()
        if period in _RISK_PERIOD_DAYS and period not in resolved:
            resolved.append(period)
    return resolved or ["1y", "3y", "5y"]


def _resolve_coverage_grace_days(config: dict[str, Any]) -> int:
    try:
        return max(0, int((((config or {}).get("risk_metrics") or {}).get("period_coverage_grace_days", 7)) or 7))
    except (TypeError, ValueError):
        return 7


def _resolve_min_data_points(config: dict[str, Any]) -> int:
    try:
        return max(1, int((((config or {}).get("risk_metrics") or {}).get("min_data_points", 30)) or 30))
    except (TypeError, ValueError):
        return 30


def _load_flagged_rows(
    conn: sqlite3.Connection,
    *,
    periods: list[str],
    coverage_grace_days: int,
    min_data_points: int,
) -> list[dict[str, Any]]:
    placeholders = ",".join(["?"] * len(periods))
    required_days_by_period = {
        period: _period_required_days(period, coverage_grace_days)
        for period in periods
    }
    required_observations = max(int(min_data_points or 0) + 1, 2)
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    fund_columns = _table_columns(conn, "funds")
    holding_fields = {
        name: f"f.{name}" if name in fund_columns else "NULL"
        for name in ("shortname", "ftype", "type_code", "has_holding_period", "holding_days",
                     "fullname", "holding_period_source", "holding_keywords")
    }
    rows = conn.execute(
        f"""
        WITH latest_nav AS (
            SELECT fcode, COALESCE(TRIM(latest_nav_date), '') AS latest_nav_date
            FROM fund_returns
            WHERE latest_nav_date IS NOT NULL AND TRIM(latest_nav_date) <> ''
        ),
        nav_coverage AS (
            SELECT
                latest_nav.fcode,
                MIN(nav.nav_date) AS first_nav_date,
                MAX(nav.nav_date) AS last_nav_date,
                CAST(julianday(MAX(nav.nav_date)) - julianday(MIN(nav.nav_date)) AS INT) AS nav_span_days,
                MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_1y,
                MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_1y,
                COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_1y,
                MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_3y,
                MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_3y,
                COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_3y,
                MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_5y,
                MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_5y,
                COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_5y
            FROM latest_nav
            LEFT JOIN fund_nav_history nav ON nav.fcode = latest_nav.fcode
            GROUP BY latest_nav.fcode, latest_nav.latest_nav_date
        ),
        risk_periods AS (
            SELECT
                risk.fcode,
                risk.period,
                COALESCE(TRIM(risk.end_date), '') AS end_date
            FROM fund_risk_metrics risk
            WHERE risk.period IN ({placeholders})
              AND risk.end_date IS NOT NULL
              AND TRIM(risk.end_date) <> ''
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
        ),
        risk_wide AS (
            SELECT
                fcode,
                MAX(CASE WHEN period = '1y' THEN end_date ELSE '' END) AS risk_1y_end_date,
                MAX(CASE WHEN period = '3y' THEN end_date ELSE '' END) AS risk_3y_end_date,
                MAX(CASE WHEN period = '5y' THEN end_date ELSE '' END) AS risk_5y_end_date
            FROM risk_periods
            GROUP BY fcode
        )
        SELECT
            n.fcode,
            n.latest_nav_date,
            COALESCE({holding_fields['shortname']}, '') AS shortname,
            COALESCE({holding_fields['ftype']}, '') AS ftype,
            COALESCE({holding_fields['type_code']}, '') AS type_code,
            {holding_fields['has_holding_period']} AS has_holding_period,
            {holding_fields['holding_days']} AS holding_days,
            COALESCE(s.first_nav_date, '') AS first_nav_date,
            COALESCE(s.last_nav_date, '') AS last_nav_date,
            COALESCE(s.nav_span_days, 0) AS nav_span_days,
            CAST(COALESCE(julianday(s.last_nav_date_1y) - julianday(s.first_nav_date_1y), 0) AS INT) AS coverage_days_1y,
            COALESCE(s.nav_rows_1y, 0) AS nav_rows_1y,
            CAST(COALESCE(julianday(s.last_nav_date_3y) - julianday(s.first_nav_date_3y), 0) AS INT) AS coverage_days_3y,
            COALESCE(s.nav_rows_3y, 0) AS nav_rows_3y,
            CAST(COALESCE(julianday(s.last_nav_date_5y) - julianday(s.first_nav_date_5y), 0) AS INT) AS coverage_days_5y,
            COALESCE(s.nav_rows_5y, 0) AS nav_rows_5y,
            COALESCE(r.risk_1y_end_date, '') AS risk_1y_end_date,
            COALESCE(r.risk_3y_end_date, '') AS risk_3y_end_date,
            COALESCE(r.risk_5y_end_date, '') AS risk_5y_end_date,
            {holding_fields['fullname']} AS fullname,
            {holding_fields['holding_period_source']} AS holding_period_source,
            {holding_fields['holding_keywords']} AS holding_keywords
        FROM latest_nav n
        LEFT JOIN nav_coverage s ON s.fcode = n.fcode
        LEFT JOIN risk_wide r ON r.fcode = n.fcode
        LEFT JOIN funds f ON f.fcode = n.fcode
        ORDER BY n.fcode
        """,
        tuple(periods),
    ).fetchall()

    undefined_windows = load_undefined_risk_windows(conn)
    flagged_rows: list[dict[str, Any]] = []
    for row in rows:
        nav_span_days = int(row[9] or 0)
        coverage_days_by_period = {
            "1y": max(0, int(row[10] or 0)),
            "3y": max(0, int(row[12] or 0)),
            "5y": max(0, int(row[14] or 0)),
        }
        nav_rows_by_period = {
            "1y": max(0, int(row[11] or 0)),
            "3y": max(0, int(row[13] or 0)),
            "5y": max(0, int(row[15] or 0)),
        }
        period_statuses: list[dict[str, Any]] = []
        should_flag = False
        for period in periods:
            eligible = (
                coverage_days_by_period.get(period, 0) >= required_days_by_period.get(period, 1)
                and nav_rows_by_period.get(period, 0) >= required_observations
            )
            risk_end_date = str(
                row[16] if period == "1y" else row[17] if period == "3y" else row[18] if period == "5y" else ""
            ).strip()
            aligned = bool(eligible and risk_end_date and risk_end_date == str(row[1] or "").strip())
            if eligible and not aligned:
                should_flag = True
            period_statuses.append(
                {
                    "period": period,
                    "eligible": eligible,
                    "coverage_days": int(coverage_days_by_period.get(period) or 0),
                    "nav_rows": int(nav_rows_by_period.get(period) or 0),
                    "required_days": int(required_days_by_period.get(period) or 0),
                    "required_nav_rows": required_observations,
                    "risk_end_date": risk_end_date or None,
                    "aligned": aligned,
                }
            )
            window = undefined_windows.get((str(row[0] or "").strip(), period))
            if eligible and not aligned and window and window["end_date"] == str(row[1] or "").strip():
                period_statuses[-1].update(
                    calculation_status=window["calculation_status"],
                    computed_end_date=window["end_date"],
                    undefined_metric_reasons=window["undefined_metric_reasons"],
                )
        if not should_flag:
            continue
        flagged_rows.append(
            {
                "fcode": str(row[0] or "").strip(),
                "latest_nav_date": str(row[1] or "").strip(),
                "shortname": str(row[2] or "").strip(),
                "ftype": str(row[3] or "").strip(),
                "type_code": str(row[4] or "").strip(),
                "has_holding_period": row[5],
                "holding_days": row[6],
                "fullname": row[19],
                "holding_period_source": row[20],
                "holding_keywords": row[21],
                "first_nav_date": str(row[7] or "").strip(),
                "last_nav_date": str(row[8] or "").strip(),
                "last_nav_history_date": str(row[8] or "").strip(),
                "nav_span_days": nav_span_days,
                "coverage_days_by_period": coverage_days_by_period,
                "nav_rows_by_period": nav_rows_by_period,
                "alignment_gap_days": (
                    None
                    if not str(row[1] or "").strip() or not str(row[8] or "").strip()
                    else int((datetime.fromisoformat(str(row[1]).strip()) - datetime.fromisoformat(str(row[8]).strip())).days)
                ),
                "nav_history_aligned": bool(str(row[8] or "").strip() == str(row[1] or "").strip()),
                "period_statuses": period_statuses,
                "only_undefined_ratio_gaps": all(
                    not status["eligible"] or status["aligned"] or status.get("calculation_status") == "computed_ratio_undefined"
                    for status in period_statuses
                ),
            }
        )
    return flagged_rows


def load_latest_risk_sync_audit_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "risk_sync_audit"
    if not artifact_dir.exists():
        return None, None
    files = sorted(artifact_dir.glob("risk_sync_audit_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def build_risk_sync_audit(
    settings: Settings,
    *,
    artifact_dir: Path | None = None,
    artifact_path: Path | None = None,
    runtime_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    core_config = _load_core_config(settings.project_root)
    periods = _resolve_risk_periods(core_config)
    coverage_grace_days = _resolve_coverage_grace_days(core_config)
    min_data_points = _resolve_min_data_points(core_config)
    candidate_codes = _resolve_changed_nav_risk_candidates(settings.database_path, config=core_config)
    try:
        holding_days_threshold = int(
            ((core_config or {}).get("risk_metrics", {}) or {}).get(
                "skip_risk_metrics_for_holding_days_gt",
                (core_config or {}).get("exclude_holding_days_gt", 90),
            ) or 90
        )
    except (TypeError, ValueError):
        holding_days_threshold = 90

    audit_error = None
    flagged_rows: list[dict[str, Any]] = []
    if Path(settings.database_path).exists():
        try:
            conn = sqlite3.connect(str(settings.database_path))
            try:
                flagged_rows = _load_flagged_rows(
                    conn,
                    periods=periods,
                    coverage_grace_days=coverage_grace_days,
                    min_data_points=min_data_points,
                )
            finally:
                conn.close()
        except sqlite3.Error as exc:
            audit_error = str(exc)
    else:
        audit_error = "database_missing"

    period_gap_counts = {period: 0 for period in periods}
    ftype_counts: dict[str, int] = {}
    blocked_by_nav_history_ftype_counts: dict[str, int] = {}
    blocked_by_nav_history_lag_total = 0
    blocked_by_returns_lag_total = 0
    low_priority_cadence_tail_total = 0
    nav_span_buckets = {
        "eligible_5y": 0,
        "eligible_3y_not_5y": 0,
        "eligible_1y_not_3y": 0,
    }
    actionable_candidate_total = 0
    undefined_ratio_candidate_total = 0
    undefined_ratio_candidate_samples: list[dict[str, Any]] = []
    blocked_by_nav_history_total = 0
    actionable_candidate_samples: list[dict[str, Any]] = []
    low_priority_cadence_tail_samples: list[dict[str, Any]] = []
    for row in flagged_rows:
        ftype = str(row.get("ftype") or "").strip() or "<empty>"
        ftype_counts[ftype] = ftype_counts.get(ftype, 0) + 1
        fund_name = combine_holding_period_names(row.get("shortname"), row.get("fullname"))
        holding_period_blocked = holding_period_gate_reason(
            has_holding_period=row.get("has_holding_period"),
            holding_days=row.get("holding_days"),
            fund_name=fund_name,
            holding_period_source=row.get("holding_period_source"),
            holding_keywords=row.get("holding_keywords"),
            threshold_days=holding_days_threshold,
        ) == HOLDING_PERIOD_LIMIT
        if holding_period_blocked:
            continue
        cadence_tail_low_priority = has_cadence_tail_signal(
            shortname=row.get("shortname"),
            has_holding_period=row.get("has_holding_period"),
            holding_days=resolve_holding_period_days(
                holding_days=row.get("holding_days"), fund_name=fund_name,
                holding_period_source=row.get("holding_period_source"),
                holding_keywords=row.get("holding_keywords"),
            ),
        )
        if bool(row.get("nav_history_aligned")):
            if row.get("only_undefined_ratio_gaps"):
                undefined_ratio_candidate_total += 1
                if len(undefined_ratio_candidate_samples) < AUDIT_SAMPLE_LIMIT:
                    undefined_ratio_candidate_samples.append(row)
            elif cadence_tail_low_priority:
                low_priority_cadence_tail_total += 1
                if len(low_priority_cadence_tail_samples) < AUDIT_SAMPLE_LIMIT:
                    low_priority_cadence_tail_samples.append(row)
            else:
                actionable_candidate_total += 1
                if len(actionable_candidate_samples) < AUDIT_SAMPLE_LIMIT:
                    actionable_candidate_samples.append(row)
        else:
            blocked_by_nav_history_total += 1
            gap_days = row.get("alignment_gap_days")
            if isinstance(gap_days, int):
                if gap_days > 0:
                    blocked_by_nav_history_lag_total += 1
                elif gap_days < 0:
                    blocked_by_returns_lag_total += 1
            blocked_by_nav_history_ftype_counts[ftype] = blocked_by_nav_history_ftype_counts.get(ftype, 0) + 1
        nav_span_days = int(row.get("nav_span_days") or 0)
        if nav_span_days >= max(_RISK_PERIOD_DAYS["5y"] - coverage_grace_days, 1):
            nav_span_buckets["eligible_5y"] += 1
        elif nav_span_days >= max(_RISK_PERIOD_DAYS["3y"] - coverage_grace_days, 1):
            nav_span_buckets["eligible_3y_not_5y"] += 1
        else:
            nav_span_buckets["eligible_1y_not_3y"] += 1
        for period_status in row.get("period_statuses") or []:
            period = str(period_status.get("period") or "").strip()
            if period and bool(period_status.get("eligible")) and not bool(period_status.get("aligned")):
                period_gap_counts[period] = period_gap_counts.get(period, 0) + 1

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "configured_periods": periods,
        "period_coverage_grace_days": coverage_grace_days,
        "min_data_points": min_data_points,
        "changed_nav_candidate_total": len(candidate_codes),
        "period_eligible_gap_candidate_total": len(flagged_rows),
        "actionable_candidate_total": actionable_candidate_total,
        "undefined_ratio_candidate_total": undefined_ratio_candidate_total,
        "undefined_ratio_candidate_samples": undefined_ratio_candidate_samples,
        "blocked_by_nav_history_total": blocked_by_nav_history_total,
        "blocked_by_nav_history_lag_total": blocked_by_nav_history_lag_total,
        "blocked_by_returns_lag_total": blocked_by_returns_lag_total,
        "low_priority_cadence_tail_total": low_priority_cadence_tail_total,
        "period_gap_counts": period_gap_counts,
        "nav_span_bucket_counts": nav_span_buckets,
        "ftype_counts": dict(sorted(ftype_counts.items(), key=lambda item: (-item[1], item[0]))[:20]),
        "blocked_by_nav_history_ftype_counts": dict(
            sorted(blocked_by_nav_history_ftype_counts.items(), key=lambda item: (-item[1], item[0]))[:20]
        ),
        "candidate_samples": flagged_rows[:AUDIT_SAMPLE_LIMIT],
        "actionable_candidate_samples": actionable_candidate_samples,
        "low_priority_cadence_tail_samples": low_priority_cadence_tail_samples,
        "audit_error": audit_error,
    }

    payload.update(dict(runtime_scope or {}))

    reports_dir = getattr(settings, "reports_dir", None)
    resolved_artifact_path = artifact_path
    if resolved_artifact_path is None and reports_dir:
        resolved_artifact_dir = artifact_dir or (Path(reports_dir) / "audit" / "risk_sync_audit")
        resolved_artifact_dir.mkdir(parents=True, exist_ok=True)
        resolved_artifact_path = resolved_artifact_dir / f"risk_sync_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    if resolved_artifact_path is not None:
        _write_json(resolved_artifact_path, payload)
    payload["artifact_path"] = str(resolved_artifact_path) if resolved_artifact_path else None
    return payload
