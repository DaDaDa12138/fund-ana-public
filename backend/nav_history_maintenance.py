from __future__ import annotations

import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from core.utils.buyability import normalize_buyability
from backend.effective_nav import load_effective_nav_snapshot
from backend.truncated_history_tail_audit import resolve_confirmed_upstream_limited_codes
from backend.task_audit_storage import glob_json_artifacts, read_task_audit, write_task_audit


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.client import APIClient  # noqa: E402
from collectors.nav_collector import NavCollector  # noqa: E402
from database.connection import get_db_manager  # noqa: E402
from utils.project_runtime_config import load_runtime_config  # noqa: E402
from scripts.audit_runtime_utils import build_audit_artifact_path  # noqa: E402


AUDIT_SAMPLE_LIMIT = 50
NAV_VALUE_MISMATCH_TOLERANCE = 0.000001
SYNC_ISSUE_TYPES = {
    "nav_value_mismatch",
    "nav_history_lag",
    "returns_lag",
    "returns_missing",
    "recent_tail_lag",
}
TRUNCATED_HISTORY_ISSUE = "truncated_history"
ISSUE_PRIORITY = {
    "missing_nav": 0,
    "truncated_history": 1,
    "nav_value_mismatch": 2,
    "returns_missing": 3,
    "nav_history_lag": 4,
    "returns_lag": 5,
    "recent_tail_lag": 6,
}
DEFAULT_BATCH_SIZE = 500
TRUNCATED_HISTORY_NO_PROGRESS_COOLDOWN_THRESHOLD = 3
MAX_TRUNCATED_HISTORY_COOLDOWN_RUNS = 8
RECENT_TAIL_LAG_DAYS = 3
RECENT_TAIL_LAG_HOLDING_PERIOD_THRESHOLD_DAYS = 90
OLD_TAIL_NAME_RESTRICTION_KEYWORDS = ("定开", "持有", "滚动")


@dataclass(slots=True)
class NavHistoryMaintenanceContext:
    settings: Settings
    core_config: dict[str, Any]
    db_manager: Any
    api_client: APIClient
    nav_collector: NavCollector


def _load_core_config(project_root: Path) -> dict[str, Any]:
    return load_runtime_config(project_root / "config" / "project.yaml")


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    if path.name.endswith(".json.gz"):
        write_task_audit(path, payload, default=_json_default)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _maintenance_state_path(settings: Settings) -> Path:
    return settings.scheduler_state_file.with_name("nav_history_maintenance_state.json")


def _load_maintenance_state(settings: Settings) -> dict[str, Any]:
    path = _maintenance_state_path(settings)
    if not path.exists():
        return {"next_index": 0, "updated_at": None, "truncated_history_cooldown": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"next_index": 0, "updated_at": None, "truncated_history_cooldown": {}}
    if not isinstance(payload, dict):
        return {"next_index": 0, "updated_at": None, "truncated_history_cooldown": {}}
    return {
        "next_index": max(0, int(payload.get("next_index") or 0)),
        "updated_at": payload.get("updated_at"),
        "truncated_history_cooldown": _normalize_truncated_history_cooldown(
            payload.get("truncated_history_cooldown")
        ),
    }


def _save_maintenance_state(
    settings: Settings,
    *,
    next_index: int,
    truncated_history_cooldown: dict[str, dict[str, Any]] | None = None,
) -> None:
    path = _maintenance_state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "next_index": max(0, int(next_index or 0)),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "truncated_history_cooldown": _normalize_truncated_history_cooldown(
                    truncated_history_cooldown
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_context(settings: Settings) -> NavHistoryMaintenanceContext:
    core_config = _load_core_config(settings.project_root)
    core_config.setdefault("api", {})
    core_config["api"].setdefault("tiantian", {})
    core_config["api"]["tiantian"]["base_url"] = settings.tiantian_base_url
    db_manager = get_db_manager(db_path=str(settings.database_path))
    api_client = APIClient(core_config)
    nav_collector = NavCollector(config=core_config, db_manager=db_manager, api_client=api_client)
    return NavHistoryMaintenanceContext(
        settings=settings,
        core_config=core_config,
        db_manager=db_manager,
        api_client=api_client,
        nav_collector=nav_collector,
    )


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _parse_iso_date(value: Any) -> datetime.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_recent_tail_lag_candidate(
    *,
    latest_nav_date: str | None,
    last_nav_date: str | None,
    global_latest_nav_dt: datetime.date | None,
    is_buyable: bool | None,
    purchase_status: str | None,
    holding_days: int | None,
) -> bool:
    normalized_latest_nav_date = str(latest_nav_date or "").strip()
    normalized_last_nav_date = str(last_nav_date or "").strip()
    if (
        not normalized_latest_nav_date
        or not normalized_last_nav_date
        or normalized_latest_nav_date != normalized_last_nav_date
        or global_latest_nav_dt is None
    ):
        return False

    last_nav_dt = _parse_iso_date(normalized_last_nav_date)
    if last_nav_dt is None:
        return False

    lag_days = (global_latest_nav_dt - last_nav_dt).days
    if lag_days <= 0 or lag_days > RECENT_TAIL_LAG_DAYS:
        return False

    normalized_purchase_status = str(purchase_status or "").strip()
    active_recent_tail = bool(is_buyable) or ("开放" in normalized_purchase_status)
    if not active_recent_tail:
        return False

    try:
        normalized_holding_days = int(holding_days or 0)
    except (TypeError, ValueError):
        normalized_holding_days = 0

    return (
        normalized_holding_days <= 0
        or normalized_holding_days <= RECENT_TAIL_LAG_HOLDING_PERIOD_THRESHOLD_DAYS
    )


def _read_candidates(db_path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    conn = sqlite3.connect(str(db_path))
    try:
        table_names = {
            str(row[0] or "").strip()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        has_fund_returns = "fund_returns" in table_names
        fund_returns_columns = _table_columns(conn, "fund_returns") if has_fund_returns else set()
        nav_history_columns = _table_columns(conn, "fund_nav_history") if "fund_nav_history" in table_names else set()
        has_returns_latest_nav = "latest_nav" in fund_returns_columns
        has_nav_history_unit_nav = "unit_nav" in nav_history_columns
        returns_join = "LEFT JOIN fund_returns fr ON fr.fcode = f.fcode" if has_fund_returns else ""
        returns_select = "COALESCE(fr.latest_nav_date, '')" if has_fund_returns else "''"
        returns_latest_nav_select = "fr.latest_nav" if has_returns_latest_nav else "NULL"
        nav_latest_value_min_select = (
            "MIN(CASE WHEN h.unit_nav IS NOT NULL AND TRIM(CAST(h.unit_nav AS TEXT)) <> '' THEN CAST(h.unit_nav AS REAL) END)"
            if has_nav_history_unit_nav
            else "NULL"
        )
        nav_latest_value_max_select = (
            "MAX(CASE WHEN h.unit_nav IS NOT NULL AND TRIM(CAST(h.unit_nav AS TEXT)) <> '' THEN CAST(h.unit_nav AS REAL) END)"
            if has_nav_history_unit_nav
            else "NULL"
        )
        rows = conn.execute(
            f"""
            WITH nav_stats AS (
                SELECT
                    fcode,
                    COUNT(*) AS nav_count,
                    MIN(nav_date) AS first_nav_date,
                    MAX(nav_date) AS last_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            ),
            nav_latest_values AS (
                SELECT
                    h.fcode,
                    {nav_latest_value_min_select} AS latest_unit_nav_min,
                    {nav_latest_value_max_select} AS latest_unit_nav_max
                FROM fund_nav_history h
                JOIN nav_stats ns
                  ON ns.fcode = h.fcode
                 AND ns.last_nav_date = h.nav_date
                GROUP BY h.fcode
            )
            SELECT
                f.fcode,
                COALESCE(f.shortname, ''),
                COALESCE(f.ftype, ''),
                f.estab_date,
                {returns_select},
                {returns_latest_nav_select},
                ns.nav_count,
                ns.first_nav_date,
                ns.last_nav_date,
                nlv.latest_unit_nav_min,
                nlv.latest_unit_nav_max,
                f.is_buyable,
                COALESCE(f.purchase_status, ''),
                COALESCE(f.holding_days, 0)
            FROM funds f
            {returns_join}
            LEFT JOIN nav_stats ns ON ns.fcode = f.fcode
            LEFT JOIN nav_latest_values nlv ON nlv.fcode = f.fcode
            WHERE f.type_code IN ('31', '32')
            ORDER BY f.fcode
            """
        ).fetchall()
        global_latest_nav_date_raw = conn.execute(
            """
            SELECT MAX(latest_nav_date)
            FROM fund_returns
            WHERE latest_nav_date IS NOT NULL AND TRIM(latest_nav_date) <> ''
            """
        ).fetchone()
    finally:
        conn.close()

    global_latest_nav_date = str((global_latest_nav_date_raw or [""])[0] or "").strip() or None
    try:
        global_latest_nav_dt = datetime.fromisoformat(global_latest_nav_date).date() if global_latest_nav_date else None
    except ValueError:
        global_latest_nav_dt = None

    candidates: list[dict[str, Any]] = []
    stats = {
        "stored_bond_total": 0,
        "missing_nav_total": 0,
        "truncated_history_total": 0,
        "returns_missing_total": 0,
        "nav_history_lag_total": 0,
        "returns_lag_total": 0,
        "recent_tail_lag_total": 0,
        "nav_value_mismatch_total": 0,
        "global_latest_nav_date": global_latest_nav_date,
    }
    old_tail_rows: list[dict[str, Any]] = []
    today = datetime.now().date()
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        stats["stored_bond_total"] += 1
        estab_date = str(row[3] or "").strip() or None
        latest_nav_date = str(row[4] or "").strip() or None
        latest_nav_value = _parse_float(row[5])
        nav_count = int(row[6] or 0)
        first_nav_date = str(row[7] or "").strip() or None
        last_nav_date = str(row[8] or "").strip() or None
        latest_history_nav_min = _parse_float(row[9])
        latest_history_nav_max = _parse_float(row[10])
        is_buyable = normalize_buyability(row[11])
        purchase_status = str(row[12] or "").strip() or None
        try:
            holding_days = int(row[13] or 0)
        except (TypeError, ValueError):
            holding_days = 0
        if latest_nav_date and global_latest_nav_date and latest_nav_date < global_latest_nav_date:
            old_tail_rows.append(
                {
                    "fcode": code,
                    "shortname": row[1],
                    "latest_nav_date": latest_nav_date,
                    "is_buyable": is_buyable,
                    "purchase_status": purchase_status,
                    "holding_days": holding_days,
                }
            )
        estab_gap_days = None
        if estab_date and first_nav_date:
            try:
                estab_dt = datetime.fromisoformat(estab_date).date()
                first_dt = datetime.fromisoformat(first_nav_date).date()
                estab_gap_days = (first_dt - estab_dt).days
            except ValueError:
                estab_gap_days = None

        issue_types: list[str] = []
        if nav_count <= 0:
            issue_types.append("missing_nav")
            stats["missing_nav_total"] += 1
        elif estab_gap_days is not None and estab_gap_days > 365:
            issue_types.append("truncated_history")
            stats["truncated_history_total"] += 1

        nav_gap_days = None
        if latest_nav_date and last_nav_date:
            try:
                latest_nav_dt = datetime.fromisoformat(latest_nav_date).date()
                last_nav_dt = datetime.fromisoformat(last_nav_date).date()
                nav_gap_days = (latest_nav_dt - last_nav_dt).days
            except ValueError:
                nav_gap_days = None

        if nav_count > 0 and not latest_nav_date and last_nav_date:
            issue_types.append("returns_missing")
            stats["returns_missing_total"] += 1

        if nav_count > 0 and nav_gap_days is not None:
            if nav_gap_days > 0:
                issue_types.append("nav_history_lag")
                stats["nav_history_lag_total"] += 1
            elif nav_gap_days < 0:
                issue_types.append("returns_lag")
                stats["returns_lag_total"] += 1

        nav_value_delta = None
        if (
            has_returns_latest_nav
            and has_nav_history_unit_nav
            and nav_count > 0
            and latest_nav_date
            and last_nav_date
            and latest_nav_date == last_nav_date
            and "missing_nav" not in issue_types
            and "truncated_history" not in issue_types
        ):
            if latest_nav_value is None or latest_history_nav_min is None or latest_history_nav_max is None:
                issue_types.append("nav_value_mismatch")
                stats["nav_value_mismatch_total"] += 1
            else:
                nav_value_delta = max(
                    abs(latest_nav_value - latest_history_nav_min),
                    abs(latest_nav_value - latest_history_nav_max),
                )
                if nav_value_delta > NAV_VALUE_MISMATCH_TOLERANCE:
                    issue_types.append("nav_value_mismatch")
                    stats["nav_value_mismatch_total"] += 1

        if (
            nav_count > 0
            and latest_nav_date
            and last_nav_date
            and latest_nav_date == last_nav_date
            and "missing_nav" not in issue_types
            and "truncated_history" not in issue_types
            and "returns_missing" not in issue_types
        ):
            if _is_recent_tail_lag_candidate(
                latest_nav_date=latest_nav_date,
                last_nav_date=last_nav_date,
                global_latest_nav_dt=global_latest_nav_dt,
                is_buyable=is_buyable,
                purchase_status=purchase_status,
                holding_days=holding_days,
            ):
                issue_types.append("recent_tail_lag")
                stats["recent_tail_lag_total"] += 1

        if not issue_types:
            continue

        candidates.append(
            {
                "fcode": code,
                "shortname": row[1],
                "ftype": row[2],
                "estab_date": estab_date,
                "latest_nav_date": latest_nav_date,
                "latest_nav": latest_nav_value,
                "nav_count": nav_count,
                "first_nav_date": first_nav_date,
                "last_nav_date": last_nav_date,
                "latest_history_unit_nav_min": latest_history_nav_min,
                "latest_history_unit_nav_max": latest_history_nav_max,
                "latest_nav_value_delta": nav_value_delta,
                "nav_gap_days": nav_gap_days,
                "estab_to_first_nav_gap_days": estab_gap_days,
                "is_buyable": is_buyable,
                "purchase_status": purchase_status,
                "holding_days": holding_days,
                "issue_types": issue_types,
            }
        )

    candidates.sort(
        key=lambda row: (
            min(ISSUE_PRIORITY.get(issue, 99) for issue in (row.get("issue_types") or ["unknown"])),
            -int(row.get("estab_to_first_nav_gap_days") or -1),
            row.get("fcode") or "",
        )
    )
    dominant_old_tail_nav_date = None
    if old_tail_rows:
        dominant_old_tail_nav_date = max(str(row.get("latest_nav_date") or "").strip() for row in old_tail_rows)
    dominant_old_tail_candidates = [
        row
        for row in old_tail_rows
        if str(row.get("latest_nav_date") or "").strip() == str(dominant_old_tail_nav_date or "").strip()
    ]
    dominant_old_tail_actionable_candidates: list[dict[str, Any]] = []
    dominant_old_tail_holding_blocked_candidates: list[dict[str, Any]] = []
    dominant_old_tail_name_restricted_candidates: list[dict[str, Any]] = []
    dominant_old_tail_closed_candidates: list[dict[str, Any]] = []
    dominant_old_tail_other_candidates: list[dict[str, Any]] = []
    for row in dominant_old_tail_candidates:
        purchase_status = str(row.get("purchase_status") or "").strip()
        shortname = str(row.get("shortname") or "").strip()
        is_open_or_buyable = bool(row.get("is_buyable")) or ("开放" in purchase_status)
        try:
            normalized_holding_days = int(row.get("holding_days") or 0)
        except (TypeError, ValueError):
            normalized_holding_days = 0
        has_name_restriction = any(keyword in shortname for keyword in OLD_TAIL_NAME_RESTRICTION_KEYWORDS)
        if is_open_or_buyable and normalized_holding_days <= RECENT_TAIL_LAG_HOLDING_PERIOD_THRESHOLD_DAYS and not has_name_restriction:
            dominant_old_tail_actionable_candidates.append(row)
        elif normalized_holding_days > RECENT_TAIL_LAG_HOLDING_PERIOD_THRESHOLD_DAYS:
            dominant_old_tail_holding_blocked_candidates.append(row)
        elif has_name_restriction:
            dominant_old_tail_name_restricted_candidates.append(row)
        elif not is_open_or_buyable:
            dominant_old_tail_closed_candidates.append(row)
        else:
            dominant_old_tail_other_candidates.append(row)
    stats.update(
        {
            "dominant_old_tail_nav_date": dominant_old_tail_nav_date,
            "dominant_old_tail_total": len(dominant_old_tail_candidates),
            "dominant_old_tail_actionable_total": len(dominant_old_tail_actionable_candidates),
            "dominant_old_tail_holding_blocked_total": len(dominant_old_tail_holding_blocked_candidates),
            "dominant_old_tail_name_restricted_total": len(dominant_old_tail_name_restricted_candidates),
            "dominant_old_tail_closed_total": len(dominant_old_tail_closed_candidates),
            "dominant_old_tail_other_total": len(dominant_old_tail_other_candidates),
            "dominant_old_tail_actionable_samples": dominant_old_tail_actionable_candidates[:AUDIT_SAMPLE_LIMIT],
            "dominant_old_tail_holding_blocked_samples": dominant_old_tail_holding_blocked_candidates[:AUDIT_SAMPLE_LIMIT],
            "dominant_old_tail_name_restricted_samples": dominant_old_tail_name_restricted_candidates[:AUDIT_SAMPLE_LIMIT],
            "dominant_old_tail_closed_samples": dominant_old_tail_closed_candidates[:AUDIT_SAMPLE_LIMIT],
            "dominant_old_tail_other_samples": dominant_old_tail_other_candidates[:AUDIT_SAMPLE_LIMIT],
        }
    )
    return candidates, stats


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1] or "").strip() for row in rows}


def _normalize_truncated_history_cooldown(payload: Any) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    if not isinstance(payload, dict):
        return normalized

    for raw_code, raw_record in payload.items():
        code = _normalize_code(raw_code)
        if not code or not isinstance(raw_record, dict):
            continue

        last_gap_days = raw_record.get("last_gap_days")
        try:
            normalized_gap_days = int(last_gap_days) if last_gap_days is not None else None
        except (TypeError, ValueError):
            normalized_gap_days = None

        normalized[code] = {
            "consecutive_no_progress_runs": max(
                0,
                int(raw_record.get("consecutive_no_progress_runs") or 0),
            ),
            "skip_runs_remaining": max(0, int(raw_record.get("skip_runs_remaining") or 0)),
            "last_attempted_at": str(raw_record.get("last_attempted_at") or "").strip() or None,
            "last_before_first_nav_date": str(raw_record.get("last_before_first_nav_date") or "").strip() or None,
            "last_after_first_nav_date": str(raw_record.get("last_after_first_nav_date") or "").strip() or None,
            "last_gap_days": normalized_gap_days,
            "last_inserted_records": max(0, int(raw_record.get("last_inserted_records") or 0)),
            "status": str(raw_record.get("status") or "").strip() or None,
        }

    return normalized


def _cooldown_runs_for_truncated_history(consecutive_no_progress_runs: int) -> int:
    normalized_runs = max(0, int(consecutive_no_progress_runs or 0))
    if normalized_runs < TRUNCATED_HISTORY_NO_PROGRESS_COOLDOWN_THRESHOLD:
        return 0
    exponential_step = normalized_runs - TRUNCATED_HISTORY_NO_PROGRESS_COOLDOWN_THRESHOLD
    return min(MAX_TRUNCATED_HISTORY_COOLDOWN_RUNS, max(1, 2**exponential_step))


def _decorate_candidate_with_truncated_history_cooldown(
    row: dict[str, Any],
    truncated_history_cooldown: dict[str, dict[str, Any]] | None,
    confirmed_upstream_limited_codes: set[str] | None = None,
) -> dict[str, Any]:
    decorated = dict(row)
    cooldown_state = _normalize_truncated_history_cooldown(truncated_history_cooldown)
    code = _normalize_code(row.get("fcode"))
    issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
    record = dict(cooldown_state.get(code) or {})
    has_truncated_history = TRUNCATED_HISTORY_ISSUE in issue_types
    confirmed_upstream_limited = bool(has_truncated_history and code and code in (confirmed_upstream_limited_codes or set()))
    consecutive_no_progress_runs = max(0, int(record.get("consecutive_no_progress_runs") or 0))
    skip_runs_remaining = max(0, int(record.get("skip_runs_remaining") or 0))
    tail_status = ""
    if confirmed_upstream_limited:
        tail_status = "confirmed_upstream_limited"
    elif has_truncated_history and skip_runs_remaining > 0:
        tail_status = "suspected_upstream_limited_tail"
    elif has_truncated_history and consecutive_no_progress_runs > 0:
        tail_status = "truncated_history_watch"

    decorated.update(
        {
            "truncated_history_tail_status": tail_status or None,
            "truncated_history_upstream_limited_confirmed": confirmed_upstream_limited,
            "truncated_history_tail_cooldown_active": bool(has_truncated_history and skip_runs_remaining > 0),
            "truncated_history_tail_cooldown_runs_remaining": skip_runs_remaining if has_truncated_history else 0,
            "truncated_history_tail_no_progress_runs": consecutive_no_progress_runs if has_truncated_history else 0,
        }
    )
    return decorated


def _build_candidate_snapshot(
    candidates: list[dict[str, Any]],
    stats: dict[str, int],
    *,
    truncated_history_cooldown: dict[str, dict[str, Any]] | None = None,
    confirmed_upstream_limited_codes: set[str] | None = None,
) -> dict[str, Any]:
    decorated_candidates = [
        _decorate_candidate_with_truncated_history_cooldown(
            row,
            truncated_history_cooldown,
            confirmed_upstream_limited_codes,
        )
        for row in candidates
    ]
    integrity_candidates = [
        row
        for row in decorated_candidates
        if {"missing_nav", TRUNCATED_HISTORY_ISSUE} & set(row.get("issue_types") or [])
    ]
    sync_candidates = [
        row for row in decorated_candidates if SYNC_ISSUE_TYPES & set(row.get("issue_types") or [])
    ]
    truncated_history_cooldown_candidates = [
        row
        for row in decorated_candidates
        if TRUNCATED_HISTORY_ISSUE in set(row.get("issue_types") or [])
        and bool(row.get("truncated_history_tail_cooldown_active"))
    ]
    truncated_history_upstream_limited_candidates = [
        row
        for row in decorated_candidates
        if TRUNCATED_HISTORY_ISSUE in set(row.get("issue_types") or [])
        and bool(row.get("truncated_history_upstream_limited_confirmed"))
    ]
    truncated_history_actionable_candidates = [
        row
        for row in decorated_candidates
        if TRUNCATED_HISTORY_ISSUE in set(row.get("issue_types") or [])
        and not bool(row.get("truncated_history_upstream_limited_confirmed"))
        and not bool(row.get("truncated_history_tail_cooldown_active"))
    ]
    return {
        **stats,
        "candidate_total": len(decorated_candidates),
        "integrity_candidate_total": len(integrity_candidates),
        "sync_candidate_total": len(sync_candidates),
        "truncated_history_actionable_total": len(truncated_history_actionable_candidates),
        "truncated_history_cooldown_total": len(truncated_history_cooldown_candidates),
        "truncated_history_upstream_limited_total": len(truncated_history_upstream_limited_candidates),
        "candidate_samples": decorated_candidates[:AUDIT_SAMPLE_LIMIT],
        "integrity_candidate_samples": integrity_candidates[:AUDIT_SAMPLE_LIMIT],
        "sync_candidate_samples": sync_candidates[:AUDIT_SAMPLE_LIMIT],
        "truncated_history_actionable_samples": truncated_history_actionable_candidates[:AUDIT_SAMPLE_LIMIT],
        "truncated_history_cooldown_samples": truncated_history_cooldown_candidates[:AUDIT_SAMPLE_LIMIT],
        "truncated_history_upstream_limited_samples": truncated_history_upstream_limited_candidates[:AUDIT_SAMPLE_LIMIT],
    }


def _rotate_candidates(
    candidates: list[dict[str, Any]],
    start_index: int,
    limit: int | None,
) -> tuple[list[dict[str, Any]], int]:
    if not candidates:
        return [], 0
    size = len(candidates)
    raw_limit = int(limit or 0)
    normalized_limit = size if raw_limit <= 0 else max(1, min(raw_limit, size))
    normalized_start = max(0, int(start_index or 0)) % size
    ordered = candidates[normalized_start:] + candidates[:normalized_start]
    selected = ordered[:normalized_limit]
    next_index = (normalized_start + normalized_limit) % size
    return selected, next_index


def _candidate_issue_priority(row: dict[str, Any]) -> int:
    issue_types = row.get("issue_types") or ["unknown"]
    return min(ISSUE_PRIORITY.get(str(issue or "").strip(), 99) for issue in issue_types)


def _candidate_gap_magnitude(row: dict[str, Any]) -> int:
    try:
        gap_value = int(row.get("nav_gap_days") or 0)
    except (TypeError, ValueError):
        return 10**9
    return abs(gap_value)


def _partition_preferred_sync_candidates(
    candidates: list[dict[str, Any]],
    *,
    preferred_nav_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_date = str(preferred_nav_date or "").strip()
    if not normalized_date:
        return [], list(candidates)

    preferred: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []
    for row in candidates:
        issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
        latest_nav_date = str(row.get("latest_nav_date") or "").strip()
        if latest_nav_date == normalized_date and issue_types.intersection(SYNC_ISSUE_TYPES):
            preferred.append(row)
        else:
            others.append(row)

    preferred.sort(
        key=lambda row: (
            _candidate_gap_magnitude(row),
            _candidate_issue_priority(row),
            str(row.get("fcode") or ""),
        )
    )
    return preferred, others


def _partition_candidates_by_truncated_history_cooldown(
    candidates: list[dict[str, Any]],
    truncated_history_cooldown: dict[str, dict[str, Any]],
    *,
    consume_skip: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    eligible_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    updated_cooldown: dict[str, dict[str, Any]] = {}

    for row in candidates:
        code = _normalize_code(row.get("fcode"))
        issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
        record = dict(truncated_history_cooldown.get(code) or {})
        if not code:
            continue
        if TRUNCATED_HISTORY_ISSUE not in issue_types or not record:
            eligible_candidates.append(row)
            continue

        skip_runs_before = max(0, int(record.get("skip_runs_remaining") or 0))
        if skip_runs_before > 0:
            skip_runs_after = max(0, skip_runs_before - 1) if consume_skip else skip_runs_before
            record["skip_runs_remaining"] = skip_runs_after
            updated_cooldown[code] = record
            skipped_candidates.append(
                {
                    **_decorate_candidate_with_truncated_history_cooldown(row, {code: record}),
                    "tail_cooldown_runs_remaining_before": skip_runs_before,
                    "tail_cooldown_runs_remaining_after": skip_runs_after,
                }
            )
            continue

        updated_cooldown[code] = record
        eligible_candidates.append(row)

    return eligible_candidates, skipped_candidates, updated_cooldown


def _partition_candidates_by_confirmed_upstream_limited(
    candidates: list[dict[str, Any]],
    *,
    confirmed_upstream_limited_codes: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not confirmed_upstream_limited_codes:
        return list(candidates), []

    eligible_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    for row in candidates:
        code = _normalize_code(row.get("fcode"))
        issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
        if code and code in confirmed_upstream_limited_codes and TRUNCATED_HISTORY_ISSUE in issue_types:
            skipped_candidates.append(
                _decorate_candidate_with_truncated_history_cooldown(
                    row,
                    None,
                    confirmed_upstream_limited_codes,
                )
            )
            continue
        eligible_candidates.append(row)
    return eligible_candidates, skipped_candidates


def _prune_truncated_history_cooldown(
    truncated_history_cooldown: dict[str, dict[str, Any]],
    *,
    candidate_codes: set[str],
) -> dict[str, dict[str, Any]]:
    return {
        code: dict(record)
        for code, record in _normalize_truncated_history_cooldown(truncated_history_cooldown).items()
        if code in candidate_codes
    }


def _summarize_maintenance_state(state: dict[str, Any]) -> dict[str, Any]:
    truncated_history_cooldown = _normalize_truncated_history_cooldown(state.get("truncated_history_cooldown"))
    return {
        "next_index": max(0, int(state.get("next_index") or 0)),
        "updated_at": state.get("updated_at"),
        "truncated_history_cooldown_total": len(truncated_history_cooldown),
        "truncated_history_cooldown_active_total": sum(
            1 for record in truncated_history_cooldown.values() if int(record.get("skip_runs_remaining") or 0) > 0
        ),
        "truncated_history_cooldown_samples": [
            {"fcode": code, **record}
            for code, record in sorted(truncated_history_cooldown.items())[:AUDIT_SAMPLE_LIMIT]
        ],
    }


def _truncated_history_issue_still_present(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
    return TRUNCATED_HISTORY_ISSUE in issue_types


def _truncated_history_frontier_progressed(before_row: dict[str, Any], after_row: dict[str, Any] | None) -> bool:
    if not after_row:
        return False
    before_first_nav_date = str(before_row.get("first_nav_date") or "").strip()
    after_first_nav_date = str(after_row.get("first_nav_date") or "").strip()
    if not before_first_nav_date or not after_first_nav_date:
        return False
    return after_first_nav_date < before_first_nav_date


def _update_truncated_history_cooldown_after_run(
    truncated_history_cooldown: dict[str, dict[str, Any]],
    *,
    selected_candidates: list[dict[str, Any]],
    post_candidate_map: dict[str, dict[str, Any]],
    attempted_inserted_by_code: dict[str, int],
    candidate_codes_after: set[str],
    attempted_at: str,
) -> dict[str, dict[str, Any]]:
    updated = _prune_truncated_history_cooldown(
        truncated_history_cooldown,
        candidate_codes=candidate_codes_after,
    )

    for row in selected_candidates:
        code = _normalize_code(row.get("fcode"))
        if not code:
            continue
        issue_types = {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
        if TRUNCATED_HISTORY_ISSUE not in issue_types:
            continue

        after_row = post_candidate_map.get(code)
        if code not in candidate_codes_after or not _truncated_history_issue_still_present(after_row):
            updated.pop(code, None)
            continue

        if code not in attempted_inserted_by_code:
            continue

        if _truncated_history_frontier_progressed(row, after_row):
            updated.pop(code, None)
            continue

        previous = dict(updated.get(code) or {})
        consecutive_no_progress_runs = max(0, int(previous.get("consecutive_no_progress_runs") or 0)) + 1
        updated[code] = {
            "consecutive_no_progress_runs": consecutive_no_progress_runs,
            "skip_runs_remaining": _cooldown_runs_for_truncated_history(consecutive_no_progress_runs),
            "last_attempted_at": attempted_at,
            "last_before_first_nav_date": str(row.get("first_nav_date") or "").strip() or None,
            "last_after_first_nav_date": str((after_row or {}).get("first_nav_date") or "").strip() or None,
            "last_gap_days": after_row.get("estab_to_first_nav_gap_days"),
            "last_inserted_records": max(0, int(attempted_inserted_by_code.get(code) or 0)),
            "status": (
                "suspected_upstream_limited_tail"
                if consecutive_no_progress_runs >= TRUNCATED_HISTORY_NO_PROGRESS_COOLDOWN_THRESHOLD
                else "truncated_history_watch"
            ),
        }

    return updated


def _is_candidate_issue_resolved(
    before_row: dict[str, Any],
    after_row: dict[str, Any] | None,
    *,
    allowed_issue_type_set: set[str],
) -> bool:
    before_issue_types = {
        str(issue or "").strip()
        for issue in (before_row.get("issue_types") or [])
        if str(issue or "").strip()
    }
    if not before_issue_types:
        return True

    target_issue_types = allowed_issue_type_set.intersection(before_issue_types) if allowed_issue_type_set else before_issue_types
    if not target_issue_types:
        target_issue_types = before_issue_types

    after_issue_types = {
        str(issue or "").strip()
        for issue in ((after_row or {}).get("issue_types") or [])
        if str(issue or "").strip()
    }
    return not bool(after_issue_types.intersection(target_issue_types))


def _resolve_preferred_sync_nav_date(settings: Settings) -> str:
    try:
        snapshot = load_effective_nav_snapshot(
            settings.database_path,
            coverage_ratio=getattr(settings, "effective_nav_coverage_ratio", 0.9),
            weekday_only=bool(getattr(settings, "effective_nav_weekday_only", True)),
        )
    except Exception:
        return ""
    return str(getattr(snapshot, "candidate_date", "") or "").strip()


def load_latest_nav_history_maintenance_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "nav_history_maintenance"
    if not artifact_dir.exists():
        return None, None
    files = sorted(glob_json_artifacts(artifact_dir, "nav_history_maintenance_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = read_task_audit(path)
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def build_nav_history_integrity_snapshot(settings: Settings) -> dict[str, Any]:
    candidates, stats = _read_candidates(settings.database_path)
    state = _load_maintenance_state(settings)
    confirmed_upstream_limited_codes = resolve_confirmed_upstream_limited_codes(settings)
    return _build_candidate_snapshot(
        candidates,
        stats,
        truncated_history_cooldown=state.get("truncated_history_cooldown"),
        confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
    )


def run_nav_history_maintenance(
    settings: Settings,
    *,
    log=lambda _message: None,
    target_codes: list[str] | None = None,
    limit: int | None = None,
    allowed_issue_types: list[str] | None = None,
    require_preferred_sync_candidates: bool = False,
) -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    maintenance_started = time.monotonic()
    log("开始执行债基净值历史完整性审计与回补")

    candidates, stats = _read_candidates(settings.database_path)
    log(f"[nav-history] 候选审计完成 candidates={len(candidates)} elapsed={time.monotonic() - maintenance_started:.1f}s")
    allowed_issue_type_set = {
        str(issue or "").strip()
        for issue in (allowed_issue_types or [])
        if str(issue or "").strip()
    }
    sync_only_scope = bool(allowed_issue_type_set) and allowed_issue_type_set.issubset(SYNC_ISSUE_TYPES)
    scoped_candidates = (
        [
            row
            for row in candidates
            if allowed_issue_type_set.intersection(set(row.get("issue_types") or []))
        ]
        if allowed_issue_type_set
        else list(candidates)
    )
    candidate_map = {row["fcode"]: row for row in scoped_candidates}
    state_before = _load_maintenance_state(settings)
    confirmed_upstream_limited_codes = (
        resolve_confirmed_upstream_limited_codes(settings)
        if not target_codes
        else set()
    )
    truncated_history_cooldown_before = _normalize_truncated_history_cooldown(
        state_before.get("truncated_history_cooldown")
    )
    enable_truncated_history_cooldown = not target_codes and any(
        TRUNCATED_HISTORY_ISSUE in {str(issue or "").strip() for issue in (row.get("issue_types") or [])}
        for row in scoped_candidates
    )
    effective_scoped_candidates = list(scoped_candidates)
    confirmed_upstream_limited_skipped_before: list[dict[str, Any]] = []
    if confirmed_upstream_limited_codes:
        effective_scoped_candidates, confirmed_upstream_limited_skipped_before = _partition_candidates_by_confirmed_upstream_limited(
            effective_scoped_candidates,
            confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
        )
    truncated_history_cooldown_skipped_before: list[dict[str, Any]] = []
    truncated_history_cooldown_working = truncated_history_cooldown_before
    if enable_truncated_history_cooldown:
        (
            effective_scoped_candidates,
            truncated_history_cooldown_skipped_before,
            truncated_history_cooldown_working,
        ) = _partition_candidates_by_truncated_history_cooldown(
            effective_scoped_candidates,
            truncated_history_cooldown_before,
            consume_skip=True,
        )
    selected_candidates = effective_scoped_candidates
    next_rotation_index = state_before.get("next_index") or 0
    preferred_nav_date = _resolve_preferred_sync_nav_date(settings) if effective_scoped_candidates else ""
    preferred_candidates, non_preferred_candidates = _partition_preferred_sync_candidates(
        effective_scoped_candidates,
        preferred_nav_date=preferred_nav_date,
    )
    selection_mode = "target_codes" if target_codes else "rotating_scope"
    rotation_scope_total = len(effective_scoped_candidates)
    if target_codes:
        normalized_targets = {_normalize_code(code) for code in target_codes if _normalize_code(code)}
        selected_candidates = [candidate_map[code] for code in sorted(normalized_targets) if code in candidate_map]
    else:
        effective_limit = int(limit or 0)
        if require_preferred_sync_candidates:
            if preferred_candidates:
                selected_candidates, next_rotation_index = _rotate_candidates(
                    preferred_candidates,
                    int(state_before.get("next_index") or 0),
                    effective_limit,
                )
                rotation_scope_total = len(preferred_candidates)
                selection_mode = "effective_nav_sync_priority_strict"
            else:
                selected_candidates = []
                next_rotation_index = int(state_before.get("next_index") or 0)
                rotation_scope_total = 0
                selection_mode = "effective_nav_sync_priority_strict_skipped"
        elif sync_only_scope and preferred_candidates:
            selected_candidates, next_rotation_index = _rotate_candidates(
                preferred_candidates,
                int(state_before.get("next_index") or 0),
                effective_limit,
            )
            rotation_scope_total = len(preferred_candidates)
            selection_mode = "effective_nav_sync_priority"
        else:
            ordered_candidates = preferred_candidates + non_preferred_candidates
            selected_candidates, next_rotation_index = _rotate_candidates(
                ordered_candidates,
                int(state_before.get("next_index") or 0),
                effective_limit,
            )
            rotation_scope_total = len(ordered_candidates)
            selection_mode = "rotating_scope_with_sync_priority" if preferred_candidates else "rotating_scope"

    log(
        f"[nav-history] selected={len(selected_candidates)} scope={len(scoped_candidates)} "
        f"mode={selection_mode} preferred_nav_date={preferred_nav_date or 'unknown'}"
    )
    ctx = _build_context(settings) if selected_candidates else None

    attempted_codes: list[str] = []
    failed_codes: list[dict[str, str]] = []
    nav_records_inserted = 0
    attempted_inserted_by_code: dict[str, int] = {}

    last_progress_at = time.monotonic()
    for index, row in enumerate(selected_candidates, 1):
        code = row["fcode"]
        if index == 1 or index % 25 == 0 or time.monotonic() - last_progress_at >= 30:
            log(
                f"[nav-history] processing={index}/{len(selected_candidates)} fcode={code} "
                f"issues={','.join(row.get('issue_types') or [])} "
                f"completed={len(attempted_codes)} failed={len(failed_codes)} inserted={nav_records_inserted} "
                f"elapsed={time.monotonic() - maintenance_started:.1f}s"
            )
            last_progress_at = time.monotonic()
        try:
            assert ctx is not None
            issue_types = set(row.get("issue_types") or [])
            if (
                "nav_value_mismatch" in issue_types
                and "missing_nav" not in issue_types
                and "truncated_history" not in issue_types
            ):
                inserted = int(ctx.nav_collector.collect_fund_history(code, years=50, force_update=True) or 0)
                _backfill_returns_from_nav_history(ctx, code)
            elif "returns_missing" in issue_types and "missing_nav" not in issue_types and "truncated_history" not in issue_types:
                inserted = 0
                _backfill_returns_from_nav_history(ctx, code)
            elif "returns_lag" in issue_types and "nav_history_lag" not in issue_types:
                inserted = 0
                _backfill_returns_from_nav_history(ctx, code)
            elif (
                issue_types.intersection({"nav_history_lag", "recent_tail_lag"})
                and "missing_nav" not in issue_types
                and "truncated_history" not in issue_types
            ):
                inserted = int(ctx.nav_collector.collect_fund_history(code, force_update=False) or 0)
                _backfill_returns_from_nav_history(ctx, code)
            else:
                inserted = int(ctx.nav_collector.collect_fund_history(code, years=50, force_update=True) or 0)
                _backfill_returns_from_nav_history(ctx, code)
            nav_records_inserted += inserted
            attempted_codes.append(code)
            attempted_inserted_by_code[code] = inserted
        except Exception as exc:
            failed_codes.append({"fcode": code, "error": str(exc)})
            log(f"净值历史回补失败: {code} {exc}")

    log(
        f"[nav-history] 回补请求结束 completed={len(attempted_codes)} failed={len(failed_codes)} "
        f"inserted={nav_records_inserted}; 开始重新核验问题是否消失"
    )
    post_candidates, post_stats = _read_candidates(settings.database_path)
    post_candidate_map = {row["fcode"]: row for row in post_candidates}
    post_scoped_candidates = (
        [
            row
            for row in post_candidates
            if allowed_issue_type_set.intersection(set(row.get("issue_types") or []))
        ]
        if allowed_issue_type_set
        else list(post_candidates)
    )
    effective_post_scoped_candidates = list(post_scoped_candidates)
    confirmed_upstream_limited_skipped_after: list[dict[str, Any]] = []
    if confirmed_upstream_limited_codes:
        effective_post_scoped_candidates, confirmed_upstream_limited_skipped_after = _partition_candidates_by_confirmed_upstream_limited(
            effective_post_scoped_candidates,
            confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
        )
    truncated_history_cooldown_after = _prune_truncated_history_cooldown(
        truncated_history_cooldown_working,
        candidate_codes={
            _normalize_code(row.get("fcode"))
            for row in post_candidates
            if _normalize_code(row.get("fcode"))
        },
    )
    truncated_history_cooldown_skipped_after: list[dict[str, Any]] = []
    if enable_truncated_history_cooldown:
        truncated_history_cooldown_after = _update_truncated_history_cooldown_after_run(
            truncated_history_cooldown_working,
            selected_candidates=selected_candidates,
            post_candidate_map=post_candidate_map,
            attempted_inserted_by_code=attempted_inserted_by_code,
            candidate_codes_after={
                _normalize_code(row.get("fcode"))
                for row in post_candidates
                if _normalize_code(row.get("fcode"))
            },
            attempted_at=generated_at,
        )
        (
            effective_post_scoped_candidates,
            truncated_history_cooldown_skipped_after,
            truncated_history_cooldown_after,
        ) = _partition_candidates_by_truncated_history_cooldown(
            effective_post_scoped_candidates,
            truncated_history_cooldown_after,
            consume_skip=False,
        )
    recovered_codes = [
        row["fcode"]
        for row in selected_candidates
        if row["fcode"] in attempted_codes
        and _is_candidate_issue_resolved(
            row,
            post_candidate_map.get(row["fcode"]),
            allowed_issue_type_set=allowed_issue_type_set,
        )
    ]
    if not target_codes:
        post_preferred_candidates, post_non_preferred_candidates = _partition_preferred_sync_candidates(
            effective_post_scoped_candidates,
            preferred_nav_date=preferred_nav_date,
        )
        if sync_only_scope and post_preferred_candidates:
            post_rotation_scope = post_preferred_candidates
        else:
            post_rotation_scope = post_preferred_candidates + post_non_preferred_candidates
        if post_rotation_scope:
            _save_maintenance_state(
                settings,
                next_index=next_rotation_index % len(post_rotation_scope),
                truncated_history_cooldown=truncated_history_cooldown_after,
            )
        else:
            _save_maintenance_state(
                settings,
                next_index=0,
                truncated_history_cooldown=truncated_history_cooldown_after,
            )

    summary = {
        "generated_at": generated_at,
        "database_path": str(settings.database_path),
        "selection": {
            "requested_target_codes": [_normalize_code(code) for code in (target_codes or []) if _normalize_code(code)],
            "limit": limit,
            "allowed_issue_types": sorted(allowed_issue_type_set),
            "require_preferred_sync_candidates": bool(require_preferred_sync_candidates),
            "scope_candidate_total": len(scoped_candidates),
            "effective_scope_candidate_total": len(effective_scoped_candidates),
            "rotation_scope_total": rotation_scope_total,
            "selected_candidate_total": len(selected_candidates),
            "selection_mode": selection_mode,
            "preferred_nav_date": preferred_nav_date or None,
            "priority_candidate_total": len(preferred_candidates),
            "confirmed_upstream_limited_total": len(confirmed_upstream_limited_codes),
            "confirmed_upstream_limited_skipped_total": len(confirmed_upstream_limited_skipped_before),
            "confirmed_upstream_limited_skipped_samples": confirmed_upstream_limited_skipped_before[:AUDIT_SAMPLE_LIMIT],
            "truncated_history_cooldown_enabled": bool(enable_truncated_history_cooldown),
            "truncated_history_cooldown_skipped_total": len(truncated_history_cooldown_skipped_before),
            "truncated_history_cooldown_skipped_samples": truncated_history_cooldown_skipped_before[:AUDIT_SAMPLE_LIMIT],
            "rotation_state_before": _summarize_maintenance_state(state_before),
            "rotation_state_after": (
                _summarize_maintenance_state(
                    {
                        "next_index": next_rotation_index,
                        "updated_at": generated_at,
                        "truncated_history_cooldown": truncated_history_cooldown_after,
                    }
                )
                if not target_codes
                else None
            ),
            "rotation_next_index": next_rotation_index if not target_codes else None,
        },
        "audit_before": _build_candidate_snapshot(
            candidates,
            stats,
            truncated_history_cooldown=truncated_history_cooldown_before,
            confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
        ),
        "maintenance_result": {
            "processed_total": len(selected_candidates),
            "attempted_total": len(attempted_codes),
            "recovered_total": len(recovered_codes),
            "failed_total": len(failed_codes),
            "nav_records_inserted": nav_records_inserted,
            "risk_refresh_target_total": len(attempted_codes),
            "confirmed_upstream_limited_skipped_total": len(confirmed_upstream_limited_skipped_before),
            "truncated_history_no_progress_total": sum(
                1
                for row in selected_candidates
                if row["fcode"] in attempted_inserted_by_code
                and TRUNCATED_HISTORY_ISSUE in set(row.get("issue_types") or [])
                and _truncated_history_issue_still_present(post_candidate_map.get(row["fcode"]))
                and not _truncated_history_frontier_progressed(row, post_candidate_map.get(row["fcode"]))
            ),
            "attempted_codes_sample": attempted_codes[:AUDIT_SAMPLE_LIMIT],
            "recovered_codes_sample": recovered_codes[:AUDIT_SAMPLE_LIMIT],
            "risk_refresh_target_codes_sample": attempted_codes[:AUDIT_SAMPLE_LIMIT],
            "failed_codes_sample": failed_codes[:AUDIT_SAMPLE_LIMIT],
        },
        "risk_refresh_target_fcodes": attempted_codes,
        "audit_after": _build_candidate_snapshot(
            post_candidates,
            post_stats,
            truncated_history_cooldown=truncated_history_cooldown_after,
            confirmed_upstream_limited_codes=confirmed_upstream_limited_codes,
        ),
    }

    artifact_dir = settings.reports_dir / "audit" / "nav_history_maintenance"
    artifact_file = build_audit_artifact_path(artifact_dir, "nav_history_maintenance", suffix=".json.gz")
    _write_json(artifact_file, summary)
    summary["artifact_path"] = str(artifact_file)
    log(f"债基净值历史完整性审计完成，审计产物: {artifact_file}")
    return summary


def _backfill_returns_from_nav_history(ctx: NavHistoryMaintenanceContext, fcode: str) -> None:
    normalized_code = _normalize_code(fcode)
    if not normalized_code:
        return

    conn = sqlite3.connect(str(ctx.settings.database_path))
    try:
        nav_history_columns = _table_columns(conn, "fund_nav_history")
        daily_growth_select = "daily_growth" if "daily_growth" in nav_history_columns else "NULL AS daily_growth"
        latest_row = conn.execute(
            f"""
            SELECT nav_date, unit_nav, {daily_growth_select}
            FROM fund_nav_history
            WHERE fcode = ?
            ORDER BY nav_date DESC
            LIMIT 1
            """,
            (normalized_code,),
        ).fetchone()
        if latest_row is None:
            return
        cursor = conn.execute(
            """
            UPDATE fund_returns
            SET latest_nav_date = ?,
                latest_nav = ?,
                daily_growth = ?,
                last_updated_at = ?
            WHERE fcode = ?
            """,
            (
                latest_row[0],
                latest_row[1],
                latest_row[2],
                datetime.now().isoformat(timespec="seconds"),
                normalized_code,
            ),
        )
        if int(getattr(cursor, "rowcount", 0) or 0) <= 0:
            conn.execute(
                """
                INSERT INTO fund_returns (
                    fcode,
                    latest_nav_date,
                    latest_nav,
                    daily_growth,
                    last_updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    normalized_code,
                    latest_row[0],
                    latest_row[1],
                    latest_row[2],
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
        conn.commit()
    finally:
        conn.close()
