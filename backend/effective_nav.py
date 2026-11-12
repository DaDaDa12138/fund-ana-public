from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.utils.holding_period_policy import (
    HOLDING_PERIOD_LIMIT,
    combine_holding_period_names,
    holding_period_gate_reason,
)


SNAPSHOT_FIELDS = [
    "fcode",
    "latest_nav_date",
    "latest_nav",
    "daily_growth",
    "return_1w",
    "return_1m",
    "return_3m",
    "return_6m",
    "return_1y",
    "return_2y",
    "return_3y",
    "return_5y",
    "return_ytd",
    "return_since",
]

RISK_EFFECTIVE_COVERAGE_RATIO = 0.8
RISK_EFFECTIVE_COVERAGE_OBSERVATION_RATIO = 0.9
RISK_READY_COLUMNS = (
    "annual_return",
    "sharpe_ratio",
    "max_drawdown",
    "volatility",
)


@dataclass(slots=True)
class EffectiveNavSnapshot:
    computed_at: str
    candidate_date: str
    previous_eligible_nav_date: str
    previous_eligible_fund_count: int
    fresh_window_fund_count: int
    coverage_ratio_vs_fresh_window: float | None
    total_funds: int
    fund_returns_total: int
    non_null_latest_nav_count: int
    candidate_fund_count: int
    risk_candidate_fund_count: int
    coverage_ratio_vs_total: float | None
    coverage_ratio_vs_non_null: float | None
    raw_max_latest_nav_date: str
    dominant_latest_nav_date: str
    dominant_latest_nav_count: int
    coverage_threshold_ratio: float
    coverage_threshold_count: int
    total_coverage_threshold_count: int
    weekday_only: bool
    future_outlier_fund_count: int
    risk_1y_fund_count: int
    risk_1y_max_end_date: str
    risk_1y_latest_end_date: str
    risk_1y_latest_fund_count: int
    risk_1y_coverage_ratio_vs_total: float | None
    risk_1y_coverage_ratio_vs_candidate: float | None
    risk_1y_effective_end_date: str
    risk_1y_effective_fund_count: int
    risk_1y_effective_coverage_ratio_vs_total: float | None
    risk_1y_effective_coverage_ratio_vs_candidate: float | None
    signature: str
    ready: bool
    readiness_reason: str
    distribution_preview: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _is_weekend(date_text: str) -> bool:
    try:
        weekday = datetime.strptime(date_text, "%Y-%m-%d").weekday()
    except ValueError:
        return False
    return weekday >= 5


def _query_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None
    return row[0]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1]).strip()
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            if row and str(row["name"] if isinstance(row, sqlite3.Row) else row[1]).strip()
        }
    except sqlite3.Error:
        return set()


def _risk_metric_ready_predicate(conn: sqlite3.Connection, alias: str) -> str:
    columns = _table_columns(conn, "fund_risk_metrics")
    if not set(RISK_READY_COLUMNS).issubset(columns):
        return "1 = 1"
    return " AND ".join(f"{alias}.{column} IS NOT NULL" for column in RISK_READY_COLUMNS)


def _latest_risk_rowid_predicate(alias: str = "risk", latest_alias: str = "latest") -> str:
    return f"""
        {alias}.rowid = (
            SELECT {latest_alias}.rowid
            FROM fund_risk_metrics {latest_alias}
            WHERE {latest_alias}.fcode = {alias}.fcode
              AND {latest_alias}.period = {alias}.period
            ORDER BY COALESCE(TRIM({latest_alias}.end_date), '') DESC, {latest_alias}.rowid DESC
            LIMIT 1
        )
    """


@lru_cache(maxsize=16384)
def _holding_period_limit_value(
    has_holding_period: Any,
    holding_days: Any,
    holding_period_source: Any,
    holding_keywords: Any,
    shortname: Any,
    fullname: Any,
    threshold_days: int,
) -> int:
    # All inputs are part of the key, so changed metadata is never stale.
    return int(holding_period_gate_reason(
        has_holding_period=has_holding_period,
        holding_days=holding_days,
        holding_period_source=holding_period_source,
        holding_keywords=holding_keywords,
        fund_name=combine_holding_period_names(shortname, fullname),
        threshold_days=threshold_days,
    ) == HOLDING_PERIOD_LIMIT)


def _holding_period_limit_predicate(
    conn: sqlite3.Connection,
    alias: str = "f",
    threshold_days: int = 90,
) -> str:
    """Share the concrete-term policy without adding joins or DB writes."""
    columns = _table_columns(conn, "funds")
    fields = ("has_holding_period", "holding_days", "holding_period_source",
              "holding_keywords", "shortname", "fullname")
    arguments = [f"{alias}.{name}" if name in columns else "NULL" for name in fields]
    arguments.append(str(int(threshold_days)))
    conn.create_function("fund_holding_period_over_limit", 7, _holding_period_limit_value,
                         deterministic=True)
    return f"fund_holding_period_over_limit({', '.join(arguments)}) = 1"


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if numerator <= 0 or denominator <= 0:
        return None
    return round(numerator / denominator, 6)


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
            "latest_nav_date": str(row["latest_nav_date"] or ""),
            "fund_count": int(row["fund_count"] or 0),
            "is_weekend": _is_weekend(str(row["latest_nav_date"] or "")),
        }
        for row in rows
    ]


def _load_risk_distribution(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    rows = conn.execute(
        f"""
        SELECT end_date, COUNT(DISTINCT fcode) AS fund_count
        FROM fund_risk_metrics risk
        WHERE risk.period = '1y'
          AND risk.end_date IS NOT NULL
          AND TRIM(risk.end_date) <> ''
          AND {risk_ready_predicate}
          AND {latest_rowid_predicate}
        GROUP BY end_date
        ORDER BY end_date DESC
        """
    ).fetchall()
    return [
        {
            "end_date": str(row["end_date"] or ""),
            "fund_count": int(row["fund_count"] or 0),
        }
        for row in rows
    ]


def _resolve_candidate_date(
    distribution: list[dict[str, Any]],
    *,
    coverage_ratio: float,
    weekday_only: bool,
) -> tuple[str, int, str, int, int, int]:
    eligible_rows = [
        row
        for row in distribution
        if str(row.get("latest_nav_date") or "").strip()
        and not (weekday_only and bool(row.get("is_weekend")))
    ]
    for index, row in enumerate(eligible_rows):
        latest_nav_date = str(row.get("latest_nav_date") or "").strip()
        fund_count = int(row.get("fund_count") or 0)
        previous_row = eligible_rows[index + 1] if index + 1 < len(eligible_rows) else {}
        previous_nav_date = str(previous_row.get("latest_nav_date") or "").strip()
        previous_fund_count = int(previous_row.get("fund_count") or 0)
        fresh_window_fund_count = fund_count + previous_fund_count
        threshold_count = (
            int(math.ceil(float(fresh_window_fund_count) * float(coverage_ratio)))
            if fresh_window_fund_count > 0
            else 0
        )
        if not latest_nav_date or fund_count < threshold_count:
            continue
        return (
            latest_nav_date,
            fund_count,
            previous_nav_date,
            previous_fund_count,
            fresh_window_fund_count,
            threshold_count,
        )
    return "", 0, "", 0, 0, 0


def _resolve_risk_effective_end_date(
    distribution: list[dict[str, Any]],
    *,
    threshold_count: int,
) -> tuple[str, int]:
    for row in distribution:
        end_date = str(row.get("end_date") or "").strip()
        fund_count = int(row.get("fund_count") or 0)
        if not end_date or fund_count < threshold_count:
            continue
        return end_date, fund_count
    return "", 0


def _count_risk_funds_for_candidate(
    conn: sqlite3.Connection,
    *,
    candidate_date: str,
    risk_end_date: str,
) -> int:
    if not candidate_date or not risk_end_date:
        return 0
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    holding_limit_predicate = _holding_period_limit_predicate(conn)
    candidate_sql = f"""
        WITH candidate AS (
            SELECT fr.fcode
            FROM fund_returns fr
            LEFT JOIN funds f ON f.fcode = fr.fcode
            WHERE fr.latest_nav_date = ?
              AND NOT ({holding_limit_predicate})
        )
        SELECT COUNT(DISTINCT candidate.fcode)
        FROM candidate
        JOIN fund_risk_metrics risk
          ON risk.fcode = candidate.fcode
        WHERE risk.period = '1y'
          AND risk.end_date = ?
          AND {risk_ready_predicate}
          AND {latest_rowid_predicate}
        """
    row = conn.execute(
        candidate_sql,
        (candidate_date, risk_end_date),
    ).fetchone()
    return int((row or [0])[0] or 0)


def _count_risk_candidate_funds(
    conn: sqlite3.Connection,
    *,
    candidate_date: str,
) -> int:
    if not candidate_date:
        return 0
    holding_limit_predicate = _holding_period_limit_predicate(conn)
    sql = f"""
        SELECT COUNT(DISTINCT fr.fcode)
        FROM fund_returns fr
        LEFT JOIN funds f ON f.fcode = fr.fcode
        WHERE fr.latest_nav_date = ?
          AND NOT ({holding_limit_predicate})
        """
    row = conn.execute(
        sql,
        (candidate_date,),
    ).fetchone()
    return int((row or [0])[0] or 0)


def _resolve_latest_candidate_risk_snapshot(
    conn: sqlite3.Connection,
    *,
    candidate_date: str,
) -> tuple[str, int]:
    if not candidate_date:
        return "", 0
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    holding_limit_predicate = _holding_period_limit_predicate(conn)
    sql = f"""
        WITH candidate AS (
            SELECT fr.fcode
            FROM fund_returns fr
            LEFT JOIN funds f ON f.fcode = fr.fcode
            WHERE fr.latest_nav_date = ?
              AND NOT ({holding_limit_predicate})
        ),
        candidate_risk AS (
            SELECT risk.end_date, COUNT(DISTINCT risk.fcode) AS fund_count
            FROM fund_risk_metrics risk
            JOIN candidate ON candidate.fcode = risk.fcode
            WHERE risk.period = '1y'
              AND risk.end_date IS NOT NULL
              AND TRIM(risk.end_date) <> ''
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
            GROUP BY risk.end_date
            ORDER BY risk.end_date DESC
            LIMIT 1
        )
        SELECT end_date, fund_count
        FROM candidate_risk
        """
    row = conn.execute(
        sql,
        (candidate_date,),
    ).fetchone()
    if not row:
        return "", 0
    return str(row[0] or "").strip(), int(row[1] or 0)


def _build_signature(
    conn: sqlite3.Connection,
    *,
    candidate_date: str,
) -> str:
    if not candidate_date:
        return ""

    rows = conn.execute(
        """
        SELECT
            fcode,
            latest_nav_date,
            latest_nav,
            daily_growth,
            return_1w,
            return_1m,
            return_3m,
            return_6m,
            return_1y,
            return_2y,
            return_3y,
            return_5y,
            return_ytd,
            return_since
        FROM fund_returns
        WHERE latest_nav_date = ?
        ORDER BY fcode
        """,
        (candidate_date,),
    ).fetchall()

    serialized_rows: list[dict[str, str]] = []
    for row in rows:
        payload: dict[str, str] = {}
        for field in SNAPSHOT_FIELDS:
            payload[field] = str(row[field] if row[field] is not None else "")
        serialized_rows.append(payload)

    payload = {"candidate_date": candidate_date, "rows": serialized_rows}
    columns = _table_columns(conn, "fund_returns")
    if "period_return_date" in columns:
        # Period corrections on a fund outside the dominant NAV date still
        # change screening inputs. Retrieval time/source metadata alone do
        # not trigger a new screening or another Tracker observation.
        return_fields = [field for field in SNAPSHOT_FIELDS if field.startswith("return_")]
        period_rows = conn.execute(
            "SELECT fcode, period_return_date, " + ", ".join(return_fields)
            + " FROM fund_returns WHERE period_return_date IS NOT NULL ORDER BY fcode"
        ).fetchall()
        if period_rows:
            payload["period_returns"] = [dict(row) for row in period_rows]
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def load_effective_nav_snapshot(
    db_path: str | Path,
    *,
    coverage_ratio: float = 0.9,
    weekday_only: bool = True,
    distribution_limit: int = 10,
) -> EffectiveNavSnapshot:
    db_file = Path(db_path)
    now_text = datetime.now().isoformat(timespec="seconds")
    if not db_file.exists():
        return EffectiveNavSnapshot(
            computed_at=now_text,
            candidate_date="",
            previous_eligible_nav_date="",
            previous_eligible_fund_count=0,
            fresh_window_fund_count=0,
            coverage_ratio_vs_fresh_window=None,
            total_funds=0,
            fund_returns_total=0,
            non_null_latest_nav_count=0,
            candidate_fund_count=0,
            risk_candidate_fund_count=0,
            coverage_ratio_vs_total=None,
            coverage_ratio_vs_non_null=None,
            raw_max_latest_nav_date="",
            dominant_latest_nav_date="",
            dominant_latest_nav_count=0,
            coverage_threshold_ratio=float(coverage_ratio),
            coverage_threshold_count=0,
            total_coverage_threshold_count=0,
            weekday_only=bool(weekday_only),
            future_outlier_fund_count=0,
            risk_1y_fund_count=0,
            risk_1y_max_end_date="",
            risk_1y_latest_end_date="",
            risk_1y_latest_fund_count=0,
            risk_1y_coverage_ratio_vs_total=None,
            risk_1y_coverage_ratio_vs_candidate=None,
            risk_1y_effective_end_date="",
            risk_1y_effective_fund_count=0,
            risk_1y_effective_coverage_ratio_vs_total=None,
            risk_1y_effective_coverage_ratio_vs_candidate=None,
            signature="",
            ready=False,
            readiness_reason="database_missing",
            distribution_preview=[],
        )

    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    try:
        total_funds = int(_query_scalar(conn, "SELECT COUNT(*) FROM funds") or 0)
        fund_returns_total = int(_query_scalar(conn, "SELECT COUNT(*) FROM fund_returns") or 0)
        non_null_latest_nav_count = int(
            _query_scalar(conn, "SELECT COUNT(*) FROM fund_returns WHERE latest_nav_date IS NOT NULL") or 0
        )
        risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
        latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
        risk_1y_fund_count = int(
            _query_scalar(
                conn,
                f"""
                SELECT COUNT(DISTINCT risk.fcode)
                FROM fund_risk_metrics risk
                WHERE risk.period = '1y'
                  AND {risk_ready_predicate}
                  AND {latest_rowid_predicate}
                """,
            )
            or 0
        )
        risk_1y_max_end_date = str(
            _query_scalar(
                conn,
                f"""
                SELECT MAX(risk.end_date)
                FROM fund_risk_metrics risk
                WHERE risk.period = '1y'
                  AND {risk_ready_predicate}
                  AND {latest_rowid_predicate}
                """,
            )
            or ""
        ).strip()
        risk_distribution = _load_risk_distribution(conn)
        distribution = _load_distribution(conn)
        raw_max_latest_nav_date = str(distribution[0]["latest_nav_date"]) if distribution else ""
        dominant_entry = max(distribution, key=lambda item: int(item["fund_count"]), default=None)
        dominant_latest_nav_date = str((dominant_entry or {}).get("latest_nav_date") or "")
        dominant_latest_nav_count = int((dominant_entry or {}).get("fund_count") or 0)

        threshold_base = total_funds or non_null_latest_nav_count or fund_returns_total
        total_threshold_count = int(math.ceil(float(threshold_base) * float(coverage_ratio))) if threshold_base > 0 else 0
        (
            candidate_date,
            candidate_fund_count,
            previous_eligible_nav_date,
            previous_eligible_fund_count,
            fresh_window_fund_count,
            threshold_count,
        ) = _resolve_candidate_date(
            distribution,
            coverage_ratio=coverage_ratio,
            weekday_only=weekday_only,
        )
        risk_candidate_fund_count = _count_risk_candidate_funds(
            conn,
            candidate_date=candidate_date,
        )
        risk_effective_threshold_base = (
            risk_candidate_fund_count or candidate_fund_count or total_funds or non_null_latest_nav_count or fund_returns_total
        )
        risk_effective_threshold_count = (
            int(math.ceil(float(risk_effective_threshold_base) * float(RISK_EFFECTIVE_COVERAGE_RATIO)))
            if risk_effective_threshold_base > 0
            else 0
        )
        risk_1y_effective_end_date, risk_1y_effective_fund_count = _resolve_risk_effective_end_date(
            risk_distribution,
            threshold_count=risk_effective_threshold_count,
        )
        risk_1y_latest_end_date, risk_1y_latest_fund_count = _resolve_latest_candidate_risk_snapshot(
            conn,
            candidate_date=candidate_date,
        )
        risk_1y_effective_fund_count = _count_risk_funds_for_candidate(
            conn,
            candidate_date=candidate_date,
            risk_end_date=risk_1y_effective_end_date,
        )
        future_outlier_fund_count = int(
            sum(
                int(item["fund_count"])
                for item in distribution
                if candidate_date and str(item["latest_nav_date"]) > candidate_date
            )
        )
        signature = _build_signature(conn, candidate_date=candidate_date)
    finally:
        conn.close()

    coverage_ratio_vs_total = (
        _safe_ratio(candidate_fund_count, total_funds)
    )
    coverage_ratio_vs_non_null = (
        _safe_ratio(candidate_fund_count, non_null_latest_nav_count)
    )
    coverage_ratio_vs_fresh_window = _safe_ratio(candidate_fund_count, fresh_window_fund_count)
    risk_1y_coverage_ratio_vs_total = _safe_ratio(risk_1y_latest_fund_count, total_funds)
    risk_1y_coverage_ratio_vs_candidate = _safe_ratio(risk_1y_latest_fund_count, risk_candidate_fund_count)
    risk_1y_effective_coverage_ratio_vs_total = _safe_ratio(risk_1y_effective_fund_count, total_funds)
    risk_1y_effective_coverage_ratio_vs_candidate = _safe_ratio(risk_1y_effective_fund_count, risk_candidate_fund_count)

    if total_funds <= 0:
        ready = False
        readiness_reason = "fund_universe_empty"
    elif non_null_latest_nav_count <= 0:
        ready = False
        readiness_reason = "fund_returns_empty"
    elif not candidate_date:
        ready = False
        readiness_reason = "no_effective_nav_snapshot_meeting_threshold"
    else:
        ready = True
        readiness_reason = "ready"

    return EffectiveNavSnapshot(
        computed_at=now_text,
        candidate_date=candidate_date,
        previous_eligible_nav_date=previous_eligible_nav_date,
        previous_eligible_fund_count=previous_eligible_fund_count,
        fresh_window_fund_count=fresh_window_fund_count,
        coverage_ratio_vs_fresh_window=coverage_ratio_vs_fresh_window,
        total_funds=total_funds,
        fund_returns_total=fund_returns_total,
        non_null_latest_nav_count=non_null_latest_nav_count,
        candidate_fund_count=candidate_fund_count,
        risk_candidate_fund_count=risk_candidate_fund_count,
        coverage_ratio_vs_total=coverage_ratio_vs_total,
        coverage_ratio_vs_non_null=coverage_ratio_vs_non_null,
        raw_max_latest_nav_date=raw_max_latest_nav_date,
        dominant_latest_nav_date=dominant_latest_nav_date,
        dominant_latest_nav_count=dominant_latest_nav_count,
        coverage_threshold_ratio=float(coverage_ratio),
        coverage_threshold_count=threshold_count,
        total_coverage_threshold_count=total_threshold_count,
        weekday_only=bool(weekday_only),
        future_outlier_fund_count=future_outlier_fund_count,
        risk_1y_fund_count=risk_1y_fund_count,
        risk_1y_max_end_date=risk_1y_max_end_date,
        risk_1y_latest_end_date=risk_1y_latest_end_date,
        risk_1y_latest_fund_count=risk_1y_latest_fund_count,
        risk_1y_coverage_ratio_vs_total=risk_1y_coverage_ratio_vs_total,
        risk_1y_coverage_ratio_vs_candidate=risk_1y_coverage_ratio_vs_candidate,
        risk_1y_effective_end_date=risk_1y_effective_end_date,
        risk_1y_effective_fund_count=risk_1y_effective_fund_count,
        risk_1y_effective_coverage_ratio_vs_total=risk_1y_effective_coverage_ratio_vs_total,
        risk_1y_effective_coverage_ratio_vs_candidate=risk_1y_effective_coverage_ratio_vs_candidate,
        signature=signature,
        ready=ready,
        readiness_reason=readiness_reason,
        distribution_preview=distribution[: max(0, int(distribution_limit))],
    )


def load_latest_screened_db_snapshot(report_path: str | Path) -> dict[str, Any]:
    path = Path(report_path)
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}

    freshness = (payload.get("database_freshness") or {}).get("effective_nav_snapshot")
    if isinstance(freshness, dict) and str(freshness.get("candidate_date") or "").strip():
        return freshness

    tracker_summary = (payload.get("project_publish_summary") or {}).get("project_tracker_summary") or {}
    tracker_date = str(
        tracker_summary.get("latest_run_nav_snapshot_date")
        or tracker_summary.get("latest_nav_snapshot_date")
        or ""
    ).strip()
    if not tracker_date:
        return {}
    return {
        "candidate_date": tracker_date,
        "signature": "",
        "ready": True,
        "source": "project_tracker_summary",
    }


def screening_snapshot_changed(
    current_snapshot: EffectiveNavSnapshot,
    latest_screened_snapshot: dict[str, Any],
) -> tuple[bool, str]:
    if not current_snapshot.ready:
        return False, str(current_snapshot.readiness_reason or "effective_nav_not_ready")

    latest_date = str(latest_screened_snapshot.get("candidate_date") or "").strip()
    latest_signature = str(latest_screened_snapshot.get("signature") or "").strip()
    raw_latest_date = str(getattr(current_snapshot, "raw_max_latest_nav_date", "") or "").strip()
    future_outlier_fund_count = int(getattr(current_snapshot, "future_outlier_fund_count", 0) or 0)
    pending_raw_nav_completion = bool(
        latest_date
        and latest_date == str(current_snapshot.candidate_date or "").strip()
        and raw_latest_date
        and raw_latest_date > str(current_snapshot.candidate_date or "").strip()
        and future_outlier_fund_count > 0
    )

    if not latest_date:
        return True, "no_previous_screened_snapshot"
    if latest_signature:
        if latest_signature == current_snapshot.signature:
            return False, "effective_nav_snapshot_already_screened"
        if pending_raw_nav_completion:
            return False, "effective_nav_date_already_screened_pending_raw_nav_completion"
        return True, "effective_nav_snapshot_advanced"
    if latest_date == current_snapshot.candidate_date:
        return False, "effective_nav_date_already_screened"
    return True, "effective_nav_date_advanced"
