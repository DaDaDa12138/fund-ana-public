from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from core.utils.buyability import normalize_buyability
from backend.effective_nav import (
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    load_effective_nav_snapshot,
)
from backend.risk_cadence import has_cadence_tail_signal
from backend.risk_metric_status import load_undefined_risk_windows
from backend.task_audit_storage import glob_json_artifacts, read_task_audit, write_task_audit
from backend.report_builder import (
    _build_bond_universe_code_sets,
    _read_metadata,
    _read_return_snapshot,
    _read_risk_snapshot,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from utils.project_runtime_config import load_runtime_config  # noqa: E402
from scripts.audit_runtime_utils import build_audit_artifact_path  # noqa: E402


AUDIT_SAMPLE_LIMIT = 50
_ACTIONABLE_REASON_KEYS = {
    "latest_nav_missing",
    "latest_nav_stale",
    "only_historical_1y_risk_before_effective_snapshot",
    "effective_snapshot_incomplete_fields",
    "effective_snapshot_undefined_ratios",
}
_NATURAL_SHORT_HISTORY_REASON_KEYS = {
    "insufficient_1y_window_span",
    "insufficient_1y_nav_rows",
}
_LOW_PRIORITY_BLOCKED_REASON_KEYS = {
    "holding_period_blocked",
}

def _write_json(path: Path, payload: dict[str, Any]) -> None:
    if path.name.endswith(".json.gz"):
        write_task_audit(path, payload)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_core_config(settings: Settings) -> dict[str, Any]:
    return load_runtime_config(settings.config_path)


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


def _load_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {
            str(row[1]).strip()
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            if len(row) > 1 and str(row[1]).strip()
        }
    except sqlite3.DatabaseError:
        return set()


def _preferred_metric_expr(
    columns: set[str],
    *candidates: str,
    table_alias: str = "",
) -> str:
    alias_prefix = f"{table_alias}." if table_alias else ""
    for column in candidates:
        if column in columns:
            return f"{alias_prefix}{column}"
    return "NULL"


def _resolve_missing_gap_rows(
    conn: sqlite3.Connection,
    *,
    missing_codes: list[str],
    effective_end_date: str,
    candidate_date: str,
) -> list[sqlite3.Row]:
    if not missing_codes:
        return []

    risk_metric_columns = _load_table_columns(conn, "fund_risk_metrics")
    latest_sharpe_expr = _preferred_metric_expr(
        risk_metric_columns,
        "raw_sharpe_ratio",
        "sharpe_ratio",
        table_alias="r1",
    )
    latest_sortino_expr = _preferred_metric_expr(
        risk_metric_columns,
        "raw_sortino_ratio",
        "sortino_ratio",
        table_alias="r1",
    )
    effective_sharpe_expr = _preferred_metric_expr(
        risk_metric_columns,
        "raw_sharpe_ratio",
        "sharpe_ratio",
    )
    effective_sortino_expr = _preferred_metric_expr(
        risk_metric_columns,
        "raw_sortino_ratio",
        "sortino_ratio",
    )
    latest_risk_ready_predicate = _risk_metric_ready_predicate(conn, "r1")
    latest_risk_rowid_predicate = _latest_risk_rowid_predicate("r1", "latest")
    effective_risk_ready_predicate = _risk_metric_ready_predicate(conn, "r2")
    effective_risk_rowid_predicate = _latest_risk_rowid_predicate("r2", "latest")
    placeholders = ",".join(["?"] * len(missing_codes))
    return conn.execute(
        f"""
        WITH raw_latest_risk AS (
            SELECT
                r1.fcode,
                r1.end_date AS raw_latest_risk_end_date,
                r1.annual_return AS raw_latest_annual_return,
                {latest_sharpe_expr} AS raw_latest_sharpe_ratio,
                r1.max_drawdown AS raw_latest_max_drawdown,
                r1.volatility AS raw_latest_volatility,
                {latest_sortino_expr} AS raw_latest_sortino_ratio
            FROM fund_risk_metrics r1
            WHERE r1.period = '1y'
              AND r1.rowid = (
                  SELECT latest.rowid
                  FROM fund_risk_metrics latest
                  WHERE latest.fcode = r1.fcode
                    AND latest.period = '1y'
                  ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC
                  LIMIT 1
              )
        ),
        latest_risk AS (
            SELECT
                r1.fcode,
                r1.end_date AS latest_risk_end_date,
                r1.annual_return AS latest_annual_return,
                {latest_sharpe_expr} AS latest_sharpe_ratio,
                r1.max_drawdown AS latest_max_drawdown,
                r1.volatility AS latest_volatility,
                {latest_sortino_expr} AS latest_sortino_ratio
            FROM fund_risk_metrics r1
            WHERE r1.period = '1y'
              AND r1.end_date IS NOT NULL
              AND TRIM(r1.end_date) <> ''
              AND {latest_risk_ready_predicate}
              AND {latest_risk_rowid_predicate}
        ),
        raw_effective_risk AS (
            SELECT
                r2.fcode,
                r2.end_date AS raw_effective_risk_end_date,
                r2.annual_return AS raw_effective_annual_return,
                {effective_sharpe_expr} AS raw_effective_sharpe_ratio,
                r2.max_drawdown AS raw_effective_max_drawdown,
                r2.volatility AS raw_effective_volatility,
                {effective_sortino_expr} AS raw_effective_sortino_ratio
            FROM fund_risk_metrics r2
            WHERE r2.period = '1y'
              AND r2.end_date = ?
              AND r2.rowid = (
                  SELECT latest.rowid
                  FROM fund_risk_metrics latest
                  WHERE latest.fcode = r2.fcode
                    AND latest.period = '1y'
                    AND latest.end_date = r2.end_date
                  ORDER BY latest.rowid DESC
                  LIMIT 1
              )
        ),
        effective_risk AS (
            SELECT
                r2.fcode,
                r2.end_date,
                r2.annual_return,
                {effective_sharpe_expr} AS sharpe_ratio,
                r2.max_drawdown,
                r2.volatility,
                {effective_sortino_expr} AS sortino_ratio
            FROM fund_risk_metrics r2
            WHERE r2.period = '1y'
              AND r2.end_date = ?
              AND {effective_risk_ready_predicate}
              AND {effective_risk_rowid_predicate}
        ),
        nav_coverage AS (
            SELECT
                fr.fcode,
                MIN(CASE WHEN nh.nav_date BETWEEN date(fr.latest_nav_date, '-365 day') AND fr.latest_nav_date THEN nh.nav_date END) AS nav_start_1y,
                MAX(CASE WHEN nh.nav_date BETWEEN date(fr.latest_nav_date, '-365 day') AND fr.latest_nav_date THEN nh.nav_date END) AS nav_end_1y,
                COUNT(CASE WHEN nh.nav_date BETWEEN date(fr.latest_nav_date, '-365 day') AND fr.latest_nav_date THEN 1 END) AS nav_rows_1y
            FROM fund_returns fr
            LEFT JOIN fund_nav_history nh ON nh.fcode = fr.fcode
            WHERE fr.fcode IN ({placeholders})
            GROUP BY fr.fcode
        )
        SELECT
            f.fcode,
            COALESCE(f.shortname, '') AS shortname,
            COALESCE(f.ftype, '') AS ftype,
            COALESCE(f.has_holding_period, 0) AS has_holding_period,
            COALESCE(f.holding_days, 0) AS holding_days,
            f.is_buyable,
            COALESCE(f.purchase_status, '') AS purchase_status,
            COALESCE(fr.latest_nav_date, '') AS latest_nav_date,
            CASE WHEN COALESCE(fr.latest_nav_date, '') = ? THEN 1 ELSE 0 END AS in_effective_candidate_snapshot,
            COALESCE(raw_latest_risk.raw_latest_risk_end_date, '') AS raw_latest_risk_end_date,
            raw_latest_risk.raw_latest_annual_return,
            raw_latest_risk.raw_latest_sharpe_ratio,
            raw_latest_risk.raw_latest_max_drawdown,
            raw_latest_risk.raw_latest_volatility,
            COALESCE(latest_risk.latest_risk_end_date, '') AS latest_risk_end_date,
            latest_risk.latest_annual_return,
            latest_risk.latest_sharpe_ratio,
            latest_risk.latest_max_drawdown,
            latest_risk.latest_volatility,
            COALESCE(raw_effective_risk.raw_effective_risk_end_date, '') AS raw_effective_risk_end_date,
            raw_effective_risk.raw_effective_annual_return,
            raw_effective_risk.raw_effective_sharpe_ratio,
            raw_effective_risk.raw_effective_max_drawdown,
            raw_effective_risk.raw_effective_volatility,
            COALESCE(effective_risk.end_date, '') AS effective_risk_end_date,
            effective_risk.annual_return AS effective_annual_return,
            effective_risk.sharpe_ratio AS effective_sharpe_ratio,
            effective_risk.max_drawdown AS effective_max_drawdown,
            effective_risk.volatility AS effective_volatility,
            COALESCE(nav_coverage.nav_start_1y, '') AS nav_start_1y,
            COALESCE(nav_coverage.nav_end_1y, '') AS nav_end_1y,
            COALESCE(nav_coverage.nav_rows_1y, 0) AS nav_rows_1y
        FROM funds f
        LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
        LEFT JOIN raw_latest_risk ON raw_latest_risk.fcode = f.fcode
        LEFT JOIN latest_risk ON latest_risk.fcode = f.fcode
        LEFT JOIN raw_effective_risk ON raw_effective_risk.fcode = f.fcode
        LEFT JOIN effective_risk ON effective_risk.fcode = f.fcode
        LEFT JOIN nav_coverage ON nav_coverage.fcode = f.fcode
        WHERE f.fcode IN ({placeholders})
        ORDER BY f.fcode
        """,
        (effective_end_date, effective_end_date, *missing_codes, candidate_date, *missing_codes),
    ).fetchall()


def _classify_non_candidate_tail_sample(
    *,
    latest_nav_date: str,
    latest_risk_end_date: str,
    effective_candidate_date: str,
    effective_end_date: str,
    shortname: str,
    has_holding_period: bool,
    holding_days: int,
    is_buyable: bool | None,
    purchase_status: str,
) -> str:
    latest_nav = str(latest_nav_date or "").strip()
    latest_risk = str(latest_risk_end_date or "").strip()
    candidate_date = str(effective_candidate_date or "").strip()
    risk_end_date = str(effective_end_date or "").strip()

    if (candidate_date and latest_nav and latest_nav > candidate_date) or (risk_end_date and latest_risk and latest_risk > risk_end_date):
        return "future_outlier_monitor"

    if has_cadence_tail_signal(
        shortname=shortname,
        has_holding_period=has_holding_period,
        holding_days=holding_days,
    ):
        return "product_cadence_tail_low_priority"

    normalized_purchase_status = str(purchase_status or "").strip()
    if bool(is_buyable) or ("开放" in normalized_purchase_status):
        return "plain_open_tail_follow_up"

    if is_buyable is None:
        return "plain_tail_buyability_unknown"

    return "plain_open_tail_paused_monitor"


def load_latest_risk_coverage_gap_audit_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "risk_coverage_gap_audit"
    if not artifact_dir.exists():
        return None, None
    files = sorted(glob_json_artifacts(artifact_dir, "risk_coverage_gap_audit_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = read_task_audit(path)
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def build_risk_coverage_gap_audit(
    settings: Settings,
    *,
    sample_limit: int = AUDIT_SAMPLE_LIMIT,
    artifact_dir: Path | None = None,
    artifact_path: Path | None = None,
    runtime_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata_df = _read_metadata(settings.database_path)
    returns_df = _read_return_snapshot(settings.database_path)
    effective_risk_df = _read_risk_snapshot(settings.database_path)
    code_sets = _build_bond_universe_code_sets(
        settings=settings,
        metadata_df=metadata_df,
        returns_df=returns_df,
        risk_df=effective_risk_df,
    )

    core_config = _load_core_config(settings)
    coverage_grace_days = _resolve_coverage_grace_days(core_config)
    min_data_points = _resolve_min_data_points(core_config)
    required_window_days = max(365 - coverage_grace_days, 1)
    required_nav_rows = max(min_data_points + 1, 2)

    snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    )
    effective_end_date = str(
        snapshot.risk_1y_effective_end_date
        or snapshot.risk_1y_latest_end_date
        or ""
    ).strip()
    candidate_date = str(snapshot.candidate_date or "").strip()

    # The audit should explain all active-universe 1y-risk gaps, including
    # funds that are currently blocked by holding-period rules.
    missing_codes = sorted(set(code_sets["active_codes"]) - set(code_sets["risk_ready_codes"]))
    returns_ready_codes = set(code_sets["returns_ready_codes"])
    holding_period_blocked_codes = set(code_sets["holding_period_blocked_codes"])

    conn = sqlite3.connect(str(settings.database_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = _resolve_missing_gap_rows(
            conn,
            missing_codes=missing_codes,
            effective_end_date=effective_end_date,
            candidate_date=candidate_date,
        )
        undefined_windows = load_undefined_risk_windows(conn)
    finally:
        conn.close()

    primary_reason_counts: dict[str, int] = {}
    screening_relevant_reason_counts: dict[str, int] = {}
    samples_by_reason: dict[str, list[dict[str, Any]]] = {}
    effective_candidate_snapshot_gap_reason_counts: dict[str, int] = {}
    non_candidate_tail_reason_counts: dict[str, int] = {}
    non_candidate_tail_category_counts: dict[str, int] = {}
    non_candidate_tail_codes_by_category: dict[str, list[str]] = {}
    effective_candidate_snapshot_gap_samples: dict[str, list[dict[str, Any]]] = {}
    non_candidate_tail_samples: dict[str, list[dict[str, Any]]] = {}
    non_candidate_tail_category_samples: dict[str, list[dict[str, Any]]] = {}
    priority_bucket_counts: dict[str, int] = {
        "actionable_now": 0,
        "natural_short_history": 0,
        "low_priority_blocked": 0,
        "cadence_tail_low_priority": 0,
        "non_candidate_tail": 0,
        "other": 0,
    }
    priority_bucket_reason_counts: dict[str, dict[str, int]] = {
        "actionable_now": {},
        "natural_short_history": {},
        "low_priority_blocked": {},
        "cadence_tail_low_priority": {},
        "non_candidate_tail": {},
        "other": {},
    }
    priority_bucket_samples: dict[str, list[dict[str, Any]]] = {
        "actionable_now": [],
        "natural_short_history": [],
        "low_priority_blocked": [],
        "cadence_tail_low_priority": [],
        "non_candidate_tail": [],
        "other": [],
    }
    priority_bucket_codes: dict[str, list[str]] = {
        "actionable_now": [],
        "natural_short_history": [],
        "low_priority_blocked": [],
        "cadence_tail_low_priority": [],
        "non_candidate_tail": [],
        "other": [],
    }
    current_publish_blocker_reason_counts: dict[str, int] = {}

    def _append_priority_bucket_sample(bucket: str, sample: dict[str, Any]) -> None:
        priority_bucket_samples.setdefault(bucket, [])
        if len(priority_bucket_samples[bucket]) < max(1, int(sample_limit or 1)):
            priority_bucket_samples[bucket].append(dict(sample))

    def _append_priority_bucket_code(bucket: str, code: str) -> None:
        priority_bucket_codes.setdefault(bucket, [])
        if code and code not in priority_bucket_codes[bucket]:
            priority_bucket_codes[bucket].append(code)

    for row in rows:
        code = str(row["fcode"] or "").strip()
        latest_nav_date = str(row["latest_nav_date"] or "").strip()
        raw_latest_risk_end_date = str(row["raw_latest_risk_end_date"] or "").strip()
        ready_latest_risk_end_date = str(row["latest_risk_end_date"] or "").strip()
        latest_risk_end_date = raw_latest_risk_end_date or ready_latest_risk_end_date
        in_effective_candidate_snapshot = bool(int(row["in_effective_candidate_snapshot"] or 0))
        nav_start = str(row["nav_start_1y"] or "").strip()
        nav_end = str(row["nav_end_1y"] or "").strip()
        nav_rows = max(0, int(row["nav_rows_1y"] or 0))
        coverage_days = 0
        if nav_start and nav_end:
            try:
                coverage_days = max(0, (datetime.fromisoformat(nav_end) - datetime.fromisoformat(nav_start)).days)
            except ValueError:
                coverage_days = 0

        nav_fresh = code in returns_ready_codes
        holding_period_blocked = code in holding_period_blocked_codes
        effective_risk_present = bool(str(row["effective_risk_end_date"] or "").strip())
        effective_core_complete = all(
            row[key] is not None
            for key in ("effective_annual_return", "effective_sharpe_ratio", "effective_max_drawdown", "effective_volatility")
        )
        raw_effective_risk_present = bool(str(row["raw_effective_risk_end_date"] or "").strip())
        raw_effective_core_complete = all(
            row[key] is not None
            for key in (
                "raw_effective_annual_return",
                "raw_effective_sharpe_ratio",
                "raw_effective_max_drawdown",
                "raw_effective_volatility",
            )
        )

        undefined_window = undefined_windows.get((code, "1y"))
        known_undefined = bool(
            undefined_window and raw_effective_risk_present and not raw_effective_core_complete
            and undefined_window["end_date"] == str(row["raw_effective_risk_end_date"] or "").strip()
        )
        if not latest_nav_date:
            reason = "latest_nav_missing"
        elif not nav_fresh:
            reason = "latest_nav_stale"
        elif holding_period_blocked:
            reason = "holding_period_blocked"
        elif nav_rows < required_nav_rows:
            reason = "insufficient_1y_nav_rows"
        elif coverage_days < required_window_days:
            reason = "insufficient_1y_window_span"
        elif known_undefined:
            reason = "effective_snapshot_undefined_ratios"
        elif raw_effective_risk_present and not raw_effective_core_complete:
            reason = "effective_snapshot_incomplete_fields"
        elif effective_risk_present and effective_core_complete:
            reason = "unexpected_effective_snapshot_ready"
        elif latest_risk_end_date:
            if effective_end_date and latest_risk_end_date < effective_end_date:
                reason = "only_historical_1y_risk_before_effective_snapshot"
            elif effective_end_date and latest_risk_end_date > effective_end_date:
                reason = "only_future_1y_risk_record"
            else:
                reason = "latest_1y_risk_record_unclassified"
        else:
            reason = "no_1y_risk_record"

        primary_reason_counts[reason] = primary_reason_counts.get(reason, 0) + 1
        if nav_fresh and not holding_period_blocked:
            screening_relevant_reason_counts[reason] = screening_relevant_reason_counts.get(reason, 0) + 1

        sample_payload = {
            "fcode": code,
            "shortname": str(row["shortname"] or "").strip(),
            "ftype": str(row["ftype"] or "").strip(),
            "latest_nav_date": latest_nav_date or None,
            "latest_risk_end_date": latest_risk_end_date or None,
            "ready_latest_risk_end_date": ready_latest_risk_end_date or None,
            "raw_latest_risk_end_date": raw_latest_risk_end_date or None,
            "effective_risk_end_date": str(row["effective_risk_end_date"] or "").strip() or None,
            "raw_effective_risk_end_date": str(row["raw_effective_risk_end_date"] or "").strip() or None,
            "coverage_days_1y": coverage_days,
            "nav_rows_1y": nav_rows,
            "holding_days": int(row["holding_days"] or 0),
        }
        if known_undefined:
            sample_payload.update(
                calculation_status="computed_ratio_undefined",
                undefined_metric_reasons=undefined_window["undefined_metric_reasons"],
                recalculation_can_resolve_with_same_inputs=False,
            )

        samples_by_reason.setdefault(reason, [])
        if len(samples_by_reason[reason]) < max(1, int(sample_limit or 1)):
            samples_by_reason[reason].append(sample_payload)

        screening_relevant = nav_fresh and not holding_period_blocked
        mature_1y_eligible = nav_rows >= required_nav_rows and coverage_days >= required_window_days
        cadence_tail_low_priority = has_cadence_tail_signal(
            shortname=str(row["shortname"] or "").strip(),
            has_holding_period=bool(int(row["has_holding_period"] or 0)),
            holding_days=int(row["holding_days"] or 0),
        )
        priority_bucket = "other"

        if in_effective_candidate_snapshot and screening_relevant and mature_1y_eligible and not effective_risk_present:
            effective_candidate_snapshot_gap_reason_counts[reason] = (
                effective_candidate_snapshot_gap_reason_counts.get(reason, 0) + 1
            )
            effective_candidate_snapshot_gap_samples.setdefault(reason, [])
            if len(effective_candidate_snapshot_gap_samples[reason]) < max(1, int(sample_limit or 1)):
                effective_candidate_snapshot_gap_samples[reason].append(sample_payload)
            if cadence_tail_low_priority:
                priority_bucket = "cadence_tail_low_priority"
            else:
                current_publish_blocker_reason_counts[reason] = current_publish_blocker_reason_counts.get(reason, 0) + 1
                priority_bucket = "actionable_now"
        elif screening_relevant and mature_1y_eligible and latest_risk_end_date and effective_end_date and latest_risk_end_date != effective_end_date:
            tail_category = _classify_non_candidate_tail_sample(
                latest_nav_date=latest_nav_date,
                latest_risk_end_date=latest_risk_end_date,
                effective_candidate_date=candidate_date,
                effective_end_date=effective_end_date,
                shortname=str(row["shortname"] or "").strip(),
                has_holding_period=bool(int(row["has_holding_period"] or 0)),
                holding_days=int(row["holding_days"] or 0),
                is_buyable=normalize_buyability(row["is_buyable"]),
                purchase_status=str(row["purchase_status"] or "").strip(),
            )
            non_candidate_tail_reason_counts[reason] = non_candidate_tail_reason_counts.get(reason, 0) + 1
            non_candidate_tail_category_counts[tail_category] = non_candidate_tail_category_counts.get(tail_category, 0) + 1
            non_candidate_tail_codes_by_category.setdefault(tail_category, [])
            if code not in non_candidate_tail_codes_by_category[tail_category]:
                non_candidate_tail_codes_by_category[tail_category].append(code)
            non_candidate_tail_samples.setdefault(reason, [])
            if len(non_candidate_tail_samples[reason]) < max(1, int(sample_limit or 1)):
                non_candidate_tail_samples[reason].append({**sample_payload, "tail_category": tail_category})
            non_candidate_tail_category_samples.setdefault(tail_category, [])
            if len(non_candidate_tail_category_samples[tail_category]) < max(1, int(sample_limit or 1)):
                non_candidate_tail_category_samples[tail_category].append({**sample_payload, "tail_category": tail_category})
            priority_bucket = "non_candidate_tail"
        elif screening_relevant:
            if reason in _ACTIONABLE_REASON_KEYS:
                priority_bucket = "actionable_now"
            elif reason in _NATURAL_SHORT_HISTORY_REASON_KEYS:
                priority_bucket = "natural_short_history"
            elif reason in _LOW_PRIORITY_BLOCKED_REASON_KEYS:
                priority_bucket = "low_priority_blocked"

        priority_bucket_counts[priority_bucket] = priority_bucket_counts.get(priority_bucket, 0) + 1
        bucket_reason_counts = priority_bucket_reason_counts.setdefault(priority_bucket, {})
        bucket_reason_counts[reason] = bucket_reason_counts.get(reason, 0) + 1
        _append_priority_bucket_sample(
            priority_bucket,
            {
                **sample_payload,
                "reason": reason,
                "priority_bucket": priority_bucket,
            },
        )
        _append_priority_bucket_code(priority_bucket, code)

    ordered_primary_reason_counts = dict(sorted(primary_reason_counts.items(), key=lambda item: (-item[1], item[0])))
    ordered_screening_relevant_reason_counts = dict(
        sorted(screening_relevant_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_effective_candidate_snapshot_gap_reason_counts = dict(
        sorted(effective_candidate_snapshot_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_non_candidate_tail_reason_counts = dict(
        sorted(non_candidate_tail_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_non_candidate_tail_category_counts = dict(
        sorted(non_candidate_tail_category_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_current_publish_blocker_reason_counts = dict(
        sorted(current_publish_blocker_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_priority_bucket_counts = dict(
        sorted(priority_bucket_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    ordered_priority_bucket_reason_counts = {
        bucket: dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])))
        for bucket, reason_counts in priority_bucket_reason_counts.items()
    }
    formal_pool_blocking_reason_counts: dict[str, int] = {}
    for bucket in ("actionable_now", "natural_short_history"):
        for reason, count in (ordered_priority_bucket_reason_counts.get(bucket) or {}).items():
            formal_pool_blocking_reason_counts[reason] = (
                int(formal_pool_blocking_reason_counts.get(reason, 0)) + int(count or 0)
            )
    formal_pool_blocking_reason_counts = dict(
        sorted(formal_pool_blocking_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    formal_pool_blocking_total = int(sum(formal_pool_blocking_reason_counts.values()))
    formal_pool_non_blocking_reason_counts = {
        reason: int(count) - int(formal_pool_blocking_reason_counts.get(reason, 0))
        for reason, count in ordered_screening_relevant_reason_counts.items()
        if int(count) - int(formal_pool_blocking_reason_counts.get(reason, 0)) > 0
    }
    formal_pool_non_blocking_relevant_total = int(
        max(0, sum(ordered_screening_relevant_reason_counts.values()) - formal_pool_blocking_total)
    )
    formal_pool_impact_summary = {
        "formal_pool_blocking_total": formal_pool_blocking_total,
        "formal_pool_actionable_total": int(priority_bucket_counts.get("actionable_now", 0)),
        "formal_pool_natural_short_history_total": int(priority_bucket_counts.get("natural_short_history", 0)),
        "formal_pool_non_blocking_relevant_total": formal_pool_non_blocking_relevant_total,
        "formal_pool_blocking_reason_counts": formal_pool_blocking_reason_counts,
        "formal_pool_non_blocking_reason_counts": formal_pool_non_blocking_reason_counts,
        "current_publish_blocker_total": int(priority_bucket_counts.get("actionable_now", 0)),
        "current_publish_blocker_reason_counts": ordered_current_publish_blocker_reason_counts,
    }

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "effective_candidate_date": candidate_date or None,
        "effective_risk_end_date": effective_end_date or None,
        "required_window_days_1y": required_window_days,
        "required_nav_rows_1y": required_nav_rows,
        "active_universe_total": int(len(code_sets["active_codes"])),
        "returns_ready_total": int(len(code_sets["active_codes"] & code_sets["returns_ready_codes"])),
        "risk_ready_total": int(len(code_sets["active_codes"] & code_sets["risk_ready_codes"])),
        "missing_1y_risk_total": int(len(missing_codes)),
        "undefined_ratio_gap_total": primary_reason_counts.get("effective_snapshot_undefined_ratios", 0),
        "holding_period_blocked_total": int(len(code_sets["active_codes"] & code_sets["holding_period_blocked_codes"])),
        "screening_relevant_gap_total": int(sum(ordered_screening_relevant_reason_counts.values())),
        "actionable_gap_total": int(priority_bucket_counts.get("actionable_now", 0)),
        "natural_short_history_gap_total": int(priority_bucket_counts.get("natural_short_history", 0)),
        "low_priority_blocked_gap_total": int(priority_bucket_counts.get("low_priority_blocked", 0)),
        "cadence_tail_low_priority_gap_total": int(priority_bucket_counts.get("cadence_tail_low_priority", 0)),
        "effective_snapshot_gap_total": int(
            sum(ordered_effective_candidate_snapshot_gap_reason_counts.values())
        ),
        "effective_candidate_snapshot_gap_reason_counts": ordered_effective_candidate_snapshot_gap_reason_counts,
        "non_candidate_tail_total": int(sum(ordered_non_candidate_tail_reason_counts.values())),
        "non_candidate_tail_reason_counts": ordered_non_candidate_tail_reason_counts,
        "non_candidate_tail_category_counts": ordered_non_candidate_tail_category_counts,
        "non_candidate_tail_codes_by_category": non_candidate_tail_codes_by_category,
        "formal_pool_impact_summary": formal_pool_impact_summary,
        "new_or_short_history_gap_total": int(
            ordered_screening_relevant_reason_counts.get("insufficient_1y_window_span", 0)
            + ordered_screening_relevant_reason_counts.get("insufficient_1y_nav_rows", 0)
        ),
        "priority_bucket_counts": ordered_priority_bucket_counts,
        "priority_bucket_reason_counts": ordered_priority_bucket_reason_counts,
        "priority_bucket_codes": priority_bucket_codes,
        "primary_reason_counts": ordered_primary_reason_counts,
        "screening_relevant_reason_counts": ordered_screening_relevant_reason_counts,
        "samples_by_reason": samples_by_reason,
        "effective_candidate_snapshot_gap_samples": effective_candidate_snapshot_gap_samples,
        "non_candidate_tail_samples": non_candidate_tail_samples,
        "non_candidate_tail_category_samples": non_candidate_tail_category_samples,
        "priority_bucket_samples": priority_bucket_samples,
        "audit_error": None,
    }

    payload.update(dict(runtime_scope or {}))

    resolved_artifact_dir = artifact_dir or (settings.reports_dir / "audit" / "risk_coverage_gap_audit")
    resolved_artifact_path = artifact_path or build_audit_artifact_path(
        resolved_artifact_dir,
        "risk_coverage_gap_audit",
        suffix=(".json.gz" if resolved_artifact_dir == settings.reports_dir / "audit" / "risk_coverage_gap_audit"
                else ".json"),
    )
    _write_json(resolved_artifact_path, payload)
    payload["artifact_path"] = str(resolved_artifact_path)
    return payload
