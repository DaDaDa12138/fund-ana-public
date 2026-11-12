from __future__ import annotations

import csv
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from backend.config import Settings
from core.utils.buyability import normalize_buyability
from backend.final_selection_governance_queue import load_final_selection_governance_action_codes


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.autostock_client import AutoStockClient  # noqa: E402
from utils.project_runtime_config import load_runtime_config  # noqa: E402


AUTOSTOCK_SHADOW_TABLE = "autostock_nav_tail_fill"
AUTOSTOCK_SHADOW_HISTORY_TABLE = "autostock_nav_tail_fill_history"
RECENT_NAV_MAX_AGE_DAYS = 10
ARTIFACT_SAMPLE_LIMIT = 50
AUTOSTOCK_RETRY_BATCH_SIZE = 10
AUTOSTOCK_SINGLE_RETRY_LIMIT = 12
PERSISTENT_MISSING_RESPONSE_RETRY_WINDOW_DAYS = 7
LOW_PRIORITY_MISSING_RESPONSE_RETRY_WINDOW_DAYS = 4
AUTOSTOCK_ROTATING_PROBE_LIMIT = 10
AUTOSTOCK_ROTATING_LOW_PRIORITY_RESERVE = 3


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _to_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.split(" ", 1)[0].strip()
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _next_business_date(value: date | None) -> str | None:
    if value is None:
        return None
    cursor = value + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.isoformat()


def _missing_response_observation_bucket(missing_business_days: int) -> str:
    if missing_business_days >= 3:
        return "3d_plus"
    if missing_business_days >= 2:
        return "2d"
    return "1d"


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _ensure_shadow_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUTOSTOCK_SHADOW_TABLE} (
            fcode TEXT PRIMARY KEY,
            shortname TEXT,
            local_issue_type TEXT NOT NULL,
            local_latest_nav REAL,
            local_latest_nav_date TEXT,
            autostock_latest_nav REAL,
            autostock_latest_nav_date TEXT,
            autostock_estimated_nav REAL,
            autostock_estimated_change_pct REAL,
            autostock_valuation_date TEXT,
            autostock_found INTEGER NOT NULL DEFAULT 0,
            staleness_days INTEGER,
            review_status TEXT NOT NULL,
            review_reason TEXT,
            source_name TEXT NOT NULL,
            batch_run_id TEXT NOT NULL,
            reviewed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{AUTOSTOCK_SHADOW_TABLE}_status "
        f"ON {AUTOSTOCK_SHADOW_TABLE} (review_status)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{AUTOSTOCK_SHADOW_TABLE}_reviewed_at "
        f"ON {AUTOSTOCK_SHADOW_TABLE} (reviewed_at)"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUTOSTOCK_SHADOW_HISTORY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fcode TEXT NOT NULL,
            shortname TEXT,
            local_issue_type TEXT NOT NULL,
            local_latest_nav REAL,
            local_latest_nav_date TEXT,
            autostock_latest_nav REAL,
            autostock_latest_nav_date TEXT,
            autostock_estimated_nav REAL,
            autostock_estimated_change_pct REAL,
            autostock_valuation_date TEXT,
            autostock_found INTEGER NOT NULL DEFAULT 0,
            staleness_days INTEGER,
            review_status TEXT NOT NULL,
            review_reason TEXT,
            source_name TEXT NOT NULL,
            batch_run_id TEXT NOT NULL,
            reviewed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{AUTOSTOCK_SHADOW_HISTORY_TABLE}_fcode_reviewed_at "
        f"ON {AUTOSTOCK_SHADOW_HISTORY_TABLE} (fcode, reviewed_at)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{AUTOSTOCK_SHADOW_HISTORY_TABLE}_batch_status "
        f"ON {AUTOSTOCK_SHADOW_HISTORY_TABLE} (batch_run_id, review_status)"
    )


def _resolve_report_artifact_path(settings: Settings, path_value: Any) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = (settings.project_root / path).resolve()
    return path


def _load_latest_publish_code_sets(settings: Settings) -> dict[str, Any]:
    empty_payload = {
        "available": False,
        "run_id": None,
        "official_codes": set(),
        "portfolio_codes": set(),
        "reason": "latest_report_missing",
    }
    latest_report_file = getattr(settings, "latest_report_file", None)
    if not latest_report_file or not Path(latest_report_file).exists():
        return empty_payload

    try:
        payload = json.loads(Path(latest_report_file).read_text(encoding="utf-8"))
    except Exception:
        return {
            **empty_payload,
            "reason": "latest_report_unreadable",
        }

    artifacts = payload.get("artifacts") or {}
    filtered_csv_path = _resolve_report_artifact_path(settings, artifacts.get("filtered_csv"))
    official_codes: set[str] = set()
    if filtered_csv_path is not None and filtered_csv_path.exists():
        try:
            with filtered_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    code = _normalize_code((row or {}).get("基金代码"))
                    if code:
                        official_codes.add(code)
        except Exception:
            official_codes = set()

    portfolio_codes: set[str] = set()
    portfolio_groups = (
        (((payload.get("project_publish_summary") or {}).get("portfolio_suggestions") or {}).get("groups"))
        or []
    )
    for group in portfolio_groups:
        if not isinstance(group, dict):
            continue
        for fund in group.get("funds") or []:
            if not isinstance(fund, dict):
                continue
            code = _normalize_code(fund.get("基金代码"))
            if code:
                portfolio_codes.add(code)

    return {
        "available": bool(official_codes or portfolio_codes),
        "run_id": str(payload.get("run_id") or "").strip() or None,
        "official_codes": official_codes,
        "portfolio_codes": portfolio_codes,
        "reason": "ok",
    }


def _resolve_rotation_state_path(settings: Settings) -> Path:
    return settings.database_path.parent / "autostock_nav_tail_fill_rotation_state.json"


def _load_current_missing_response_observation_rows(
    conn: sqlite3.Connection,
    *,
    latest_batch_run_id: str | None,
) -> list[dict[str, Any]]:
    if not latest_batch_run_id or not _table_exists(conn, AUTOSTOCK_SHADOW_HISTORY_TABLE):
        return []
    latest_missing_dates = conn.execute(
        f"""
        SELECT
            fcode,
            SUBSTR(reviewed_at, 1, 10) AS review_date
        FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}
        WHERE review_status = 'missing_response'
          AND fcode IN (
              SELECT DISTINCT fcode
              FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}
              WHERE batch_run_id = ?
                AND review_status = 'missing_response'
          )
        GROUP BY fcode, SUBSTR(reviewed_at, 1, 10)
        ORDER BY fcode ASC, review_date DESC
        """,
        (latest_batch_run_id,),
    ).fetchall()
    by_code_dates: dict[str, list[date]] = {}
    for row in latest_missing_dates:
        code = _normalize_code(row[0])
        review_date = _parse_iso_date(row[1])
        if not code or review_date is None:
            continue
        by_code_dates.setdefault(code, []).append(review_date)

    rows: list[dict[str, Any]] = []
    for code, review_dates in by_code_dates.items():
        ordered_dates = sorted(set(review_dates), reverse=True)
        if not ordered_dates:
            continue
        streak_dates = [ordered_dates[0]]
        previous_date = ordered_dates[0]
        for current_date in ordered_dates[1:]:
            gap_days = (previous_date - current_date).days
            if gap_days <= 3:
                streak_dates.append(current_date)
                previous_date = current_date
                continue
            break
        missing_business_days = len(streak_dates)
        rows.append(
            {
                "fcode": code,
                "first_review_date": streak_dates[-1].isoformat(),
                "last_review_date": streak_dates[0].isoformat(),
                "missing_business_days": missing_business_days,
                "bucket": _missing_response_observation_bucket(missing_business_days),
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item.get("missing_business_days") or 0),
            str(item.get("fcode") or ""),
        )
    )
    return rows


def _load_latest_missing_response_observation_rows(
    db_path: Path,
) -> tuple[str | None, list[dict[str, Any]]]:
    conn = sqlite3.connect(str(db_path))
    try:
        if not _table_exists(conn, AUTOSTOCK_SHADOW_HISTORY_TABLE) or not _table_exists(
            conn,
            AUTOSTOCK_SHADOW_TABLE,
        ):
            return None, []
        latest_batch_row = conn.execute(
            f"""
            SELECT batch_run_id
            FROM {AUTOSTOCK_SHADOW_TABLE}
            ORDER BY reviewed_at DESC, batch_run_id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_batch_run_id = str((latest_batch_row or [None])[0] or "").strip() or None
        if not latest_batch_run_id:
            return None, []
        return latest_batch_run_id, _load_current_missing_response_observation_rows(
            conn,
            latest_batch_run_id=latest_batch_run_id,
        )
    finally:
        conn.close()


def _load_core_config(project_root: Path) -> dict[str, Any]:
    return load_runtime_config(project_root / "config" / "project.yaml")


def _build_autostock_client(settings: Settings) -> tuple[AutoStockClient, int]:
    core_config = _load_core_config(settings.project_root)
    autostock_cfg = ((core_config.get("api") or {}).get("autostock") or {})
    retry_cfg = autostock_cfg.get("retry") or {}
    # P0-04.2 / B15: 速率 / 批量 / 共享状态改为从 settings 读。
    batch_size = max(1, min(int(settings.autostock_batch_fund_default), 50))
    client = AutoStockClient(
        enabled=getattr(settings, "autostock_enabled", True),
        base_url=settings.autostock_base_url,
        min_interval=float(settings.autostock_rate_limit_min_interval_seconds),
        timeout=int(retry_cfg.get("timeout", 60)),
        max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour),
        shared_state_path=str(settings.autostock_rate_limit_shared_state_path),
    )
    return client, batch_size


def _read_same_day_missing_response_codes(
    db_path: Path,
    *,
    business_date: date,
) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        if not _table_exists(conn, AUTOSTOCK_SHADOW_TABLE):
            return {}
        rows = conn.execute(
            f"""
            SELECT
                fcode,
                MAX(reviewed_at) AS latest_reviewed_at,
                MAX(batch_run_id) AS latest_batch_run_id
            FROM {AUTOSTOCK_SHADOW_TABLE}
            WHERE review_status = 'missing_response'
              AND SUBSTR(reviewed_at, 1, 10) = ?
            GROUP BY fcode
            """,
            (business_date.isoformat(),),
        ).fetchall()
    finally:
        conn.close()

    payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        payload[code] = {
            "reviewed_at": str(row[1] or "").strip() or None,
            "batch_run_id": str(row[2] or "").strip() or None,
        }
    return payload


def _read_persistent_missing_response_retry_deferrals(
    db_path: Path,
    *,
    business_date: date,
) -> dict[str, dict[str, Any]]:
    _, observation_rows = _load_latest_missing_response_observation_rows(db_path)

    deferrals: dict[str, dict[str, Any]] = {}
    for row in observation_rows:
        code = _normalize_code(row.get("fcode"))
        missing_business_days = int(row.get("missing_business_days") or 0)
        last_review_date = _parse_iso_date(row.get("last_review_date"))
        if not code or missing_business_days < 3 or last_review_date is None:
            continue
        elapsed_days = max(0, (business_date - last_review_date).days)
        remaining_days = max(0, PERSISTENT_MISSING_RESPONSE_RETRY_WINDOW_DAYS - elapsed_days)
        if remaining_days <= 0:
            continue
        next_retry_date = last_review_date + timedelta(days=PERSISTENT_MISSING_RESPONSE_RETRY_WINDOW_DAYS)
        deferrals[code] = {
            "missing_business_days": missing_business_days,
            "last_review_date": last_review_date.isoformat(),
            "next_retry_date": next_retry_date.isoformat(),
            "remaining_calendar_days": remaining_days,
        }
    return deferrals


def _load_fund_candidate_metadata(
    db_path: Path,
    *,
    codes: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_codes = [_normalize_code(code) for code in codes if _normalize_code(code)]
    if not normalized_codes:
        return {}

    conn = sqlite3.connect(str(db_path))
    try:
        fund_columns = {
            str(row[1] or "").strip()
            for row in conn.execute("PRAGMA table_info(funds)").fetchall()
        }
        placeholders = ",".join("?" for _ in normalized_codes)
        rows = conn.execute(
            f"""
            SELECT
                fcode,
                COALESCE(shortname, '') AS shortname,
                {"is_buyable" if "is_buyable" in fund_columns else "NULL"} AS is_buyable,
                {"COALESCE(purchase_status, '')" if "purchase_status" in fund_columns else "''"} AS purchase_status,
                {"COALESCE(has_holding_period, 0)" if "has_holding_period" in fund_columns else "0"} AS has_holding_period,
                {"holding_days" if "holding_days" in fund_columns else "NULL"} AS holding_days
            FROM funds
            WHERE fcode IN ({placeholders})
            """,
            normalized_codes,
        ).fetchall()
    finally:
        conn.close()

    metadata_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        metadata_map[code] = {
            "fcode": code,
            "shortname": str(row[1] or "").strip() or None,
            "is_buyable": normalize_buyability(row[2]),
            "purchase_status": str(row[3] or "").strip() or None,
            "has_holding_period": int(row[4] or 0),
            "holding_days": row[5],
        }
    return metadata_map


def _purchase_status_looks_open(purchase_status: Any) -> bool:
    text = str(purchase_status or "").strip()
    if not text:
        return False
    return any(keyword in text for keyword in ("开放", "可申购", "限额申购"))


def _read_low_priority_missing_response_retry_deferrals(
    db_path: Path,
    *,
    business_date: date,
    publish_code_sets: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    publish_sets = dict(publish_code_sets or {})
    if not bool(publish_sets.get("available")):
        return {}
    _, observation_rows = _load_latest_missing_response_observation_rows(db_path)

    fund_metadata_map = _load_fund_candidate_metadata(
        db_path,
        codes=[str(row.get("fcode") or "") for row in observation_rows],
    )
    official_codes = {str(code or "").strip() for code in publish_sets.get("official_codes") or set()}
    portfolio_codes = {str(code or "").strip() for code in publish_sets.get("portfolio_codes") or set()}

    deferrals: dict[str, dict[str, Any]] = {}
    for row in observation_rows:
        code = _normalize_code(row.get("fcode"))
        missing_business_days = int(row.get("missing_business_days") or 0)
        last_review_date = _parse_iso_date(row.get("last_review_date"))
        metadata = fund_metadata_map.get(code) or {}
        if (
            not code
            or missing_business_days <= 0
            or missing_business_days >= 3
            or last_review_date is None
        ):
            continue
        if code in official_codes or code in portfolio_codes:
            continue
        if normalize_buyability(metadata.get("is_buyable")) is not False:
            continue
        if _purchase_status_looks_open(metadata.get("purchase_status")):
            continue
        elapsed_days = max(0, (business_date - last_review_date).days)
        remaining_days = max(0, LOW_PRIORITY_MISSING_RESPONSE_RETRY_WINDOW_DAYS - elapsed_days)
        if remaining_days <= 0:
            continue
        next_retry_date = last_review_date + timedelta(days=LOW_PRIORITY_MISSING_RESPONSE_RETRY_WINDOW_DAYS)
        deferrals[code] = {
            "missing_business_days": missing_business_days,
            "last_review_date": last_review_date.isoformat(),
            "next_retry_date": next_retry_date.isoformat(),
            "remaining_calendar_days": remaining_days,
            "reason": "non_buyable_unpublished_short_streak",
            "purchase_status": metadata.get("purchase_status"),
        }
    return deferrals


def _rotate_candidates(
    candidates: list[dict[str, Any]],
    start_index: int,
    limit: int | None,
) -> tuple[list[dict[str, Any]], int, int]:
    if not candidates:
        return [], 0, 0
    size = len(candidates)
    raw_limit = int(limit or 0)
    normalized_limit = size if raw_limit <= 0 else max(1, min(raw_limit, size))
    normalized_start = max(0, int(start_index or 0)) % size
    ordered = candidates[normalized_start:] + candidates[:normalized_start]
    selected = ordered[:normalized_limit]
    next_index = (normalized_start + normalized_limit) % size
    return selected, next_index, normalized_start


def _apply_candidate_rotation(
    candidates: list[dict[str, Any]],
    *,
    db_path: Path,
    publish_code_sets: dict[str, Any] | None,
    governance_priority_codes: set[str] | None = None,
    rotation_state_path: Path | None,
    rotation_limit: int | None,
    consume_rotation: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not candidates or rotation_state_path is None:
        return candidates, {
            "rotation_applied": False,
            "priority_candidate_total": 0,
            "rotating_low_priority_candidate_total": 0,
        }

    publish_sets = dict(publish_code_sets or {})
    _, observation_rows = _load_latest_missing_response_observation_rows(db_path)
    observation_by_code = {
        _normalize_code(row.get("fcode")): dict(row)
        for row in observation_rows
        if _normalize_code(row.get("fcode"))
    }
    fund_metadata_map = _load_fund_candidate_metadata(
        db_path,
        codes=[str(candidate.get("fcode") or "") for candidate in candidates],
    )
    official_codes = {str(code or "").strip() for code in publish_sets.get("official_codes") or set()}
    portfolio_codes = {str(code or "").strip() for code in publish_sets.get("portfolio_codes") or set()}

    priority_candidates: list[dict[str, Any]] = []
    rotating_low_priority_candidates: list[dict[str, Any]] = []
    passthrough_candidates: list[dict[str, Any]] = []
    governance_priority_candidates: list[dict[str, Any]] = []
    governance_priority_code_set = {
        _normalize_code(code)
        for code in (governance_priority_codes or set())
        if _normalize_code(code)
    }
    for candidate in candidates:
        code = _normalize_code(candidate.get("fcode"))
        metadata = fund_metadata_map.get(code) or {}
        if (
            code in official_codes
            or code in portfolio_codes
            or normalize_buyability(metadata.get("is_buyable")) is True
            or _purchase_status_looks_open(metadata.get("purchase_status"))
        ):
            priority_candidates.append(candidate)
        elif code in governance_priority_code_set:
            governance_priority_candidates.append(candidate)
        elif code in observation_by_code:
            rotating_low_priority_candidates.append(candidate)
        else:
            passthrough_candidates.append(candidate)

    rotation_state = _read_json_file(rotation_state_path)
    total_limit = max(0, int(rotation_limit or 0))
    remaining_capacity = max(total_limit - len(passthrough_candidates), 0) if total_limit > 0 else 0
    low_priority_reserved_capacity = 0
    if total_limit > 0 and (priority_candidates or governance_priority_candidates) and rotating_low_priority_candidates:
        low_priority_reserved_capacity = min(
            AUTOSTOCK_ROTATING_LOW_PRIORITY_RESERVE,
            max(remaining_capacity - 1, 0),
        )
    priority_capacity = (
        max(remaining_capacity - low_priority_reserved_capacity, 0)
        if total_limit > 0
        else 0
    )

    priority_next_index_before = max(0, int(rotation_state.get("priority_next_index") or 0))
    selected_priority: list[dict[str, Any]] = []
    priority_next_index_after = priority_next_index_before
    priority_rotation_start_index = 0
    if priority_candidates and (total_limit <= 0 or priority_capacity > 0):
        selected_priority, priority_next_index_after, priority_rotation_start_index = _rotate_candidates(
            priority_candidates,
            priority_next_index_before,
            priority_capacity if total_limit > 0 else None,
        )
    remaining_after_priority = (
        max(remaining_capacity - len(selected_priority), 0) if total_limit > 0 else 0
    )
    governance_priority_capacity = (
        max(remaining_after_priority - low_priority_reserved_capacity, 0)
        if total_limit > 0
        else len(governance_priority_candidates)
    )
    selected_governance_priority = (
        governance_priority_candidates[:governance_priority_capacity]
        if governance_priority_capacity > 0
        else []
    )
    remaining_after_governance_priority = (
        max(remaining_after_priority - len(selected_governance_priority), 0) if total_limit > 0 else 0
    )

    low_priority_next_index_before = max(0, int(rotation_state.get("low_priority_next_index") or 0))
    selected_low_priority: list[dict[str, Any]] = []
    low_priority_next_index_after = low_priority_next_index_before
    low_priority_rotation_start_index = 0
    if rotating_low_priority_candidates and (total_limit <= 0 or remaining_after_governance_priority > 0):
        selected_low_priority, low_priority_next_index_after, low_priority_rotation_start_index = _rotate_candidates(
            rotating_low_priority_candidates,
            low_priority_next_index_before,
            remaining_after_governance_priority if total_limit > 0 else None,
        )

    if consume_rotation:
        _write_json(
            rotation_state_path,
            {
                "priority_next_index": priority_next_index_after,
                "low_priority_next_index": low_priority_next_index_after,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "priority_rotation_scope_total": len(priority_candidates),
                "low_priority_rotation_scope_total": len(rotating_low_priority_candidates),
                "rotation_limit": int(rotation_limit or 0),
            },
        )

    if not priority_candidates and not rotating_low_priority_candidates and not governance_priority_candidates:
        selected_candidates = list(passthrough_candidates)
    else:
        selected_candidates = passthrough_candidates + selected_priority + selected_governance_priority + selected_low_priority
    return selected_candidates, {
        "rotation_applied": bool(priority_candidates or rotating_low_priority_candidates or governance_priority_candidates),
        "priority_candidate_total": len(priority_candidates),
        "governance_priority_candidate_total": len(governance_priority_candidates),
        "rotating_low_priority_candidate_total": len(rotating_low_priority_candidates),
        "passthrough_candidate_total": len(passthrough_candidates),
        "rotation_selected_total": len(selected_priority) + len(selected_low_priority) + len(selected_governance_priority),
        "rotation_start_index": low_priority_rotation_start_index,
        "rotation_next_index": low_priority_next_index_after,
        "rotation_limit": int(rotation_limit or 0),
        "low_priority_reserved_capacity": low_priority_reserved_capacity,
        "rotating_low_priority_codes_sample": [
            str(row.get("fcode") or "") for row in selected_low_priority[:ARTIFACT_SAMPLE_LIMIT]
        ],
        "priority_rotation_selected_total": len(selected_priority),
        "priority_rotation_start_index": priority_rotation_start_index,
        "priority_rotation_next_index": priority_next_index_after,
        "priority_rotation_codes_sample": [
            str(row.get("fcode") or "") for row in selected_priority[:ARTIFACT_SAMPLE_LIMIT]
        ],
        "governance_priority_selected_total": len(selected_governance_priority),
        "governance_priority_codes_sample": [
            str(row.get("fcode") or "") for row in selected_governance_priority[:ARTIFACT_SAMPLE_LIMIT]
        ],
        "low_priority_rotation_selected_total": len(selected_low_priority),
        "low_priority_rotation_start_index": low_priority_rotation_start_index,
        "low_priority_rotation_next_index": low_priority_next_index_after,
    }


def _read_missing_latest_nav_candidates(
    db_path: Path,
    *,
    business_date: date | None = None,
    publish_code_sets: dict[str, Any] | None = None,
    governance_priority_codes: set[str] | None = None,
    rotation_state_path: Path | None = None,
    rotation_limit: int | None = None,
    consume_rotation: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT
                f.fcode,
                COALESCE(f.shortname, ''),
                r.latest_nav,
                r.latest_nav_date
            FROM funds f
            LEFT JOIN fund_returns r ON r.fcode = f.fcode
            WHERE f.type_code IN ('31', '32')
              AND (r.latest_nav_date IS NULL OR TRIM(r.latest_nav_date) = '')
            ORDER BY f.fcode
            """
        ).fetchall()
    finally:
        conn.close()

    candidates: list[dict[str, Any]] = []
    raw_candidates: list[dict[str, Any]] = []
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        raw_candidates.append(
            {
                "fcode": code,
                "shortname": str(row[1] or "").strip(),
                "local_latest_nav": _to_float(row[2]),
                "local_latest_nav_date": str(row[3] or "").strip() or None,
                "local_issue_type": "missing_latest_nav_date",
            }
        )

    effective_business_date = business_date or datetime.now().date()
    cooldown_map = _read_same_day_missing_response_codes(
        db_path,
        business_date=effective_business_date,
    )
    persistent_deferral_map = _read_persistent_missing_response_retry_deferrals(
        db_path,
        business_date=effective_business_date,
    )
    low_priority_deferral_map = _read_low_priority_missing_response_retry_deferrals(
        db_path,
        business_date=effective_business_date,
        publish_code_sets=publish_code_sets,
    )
    cooldown_skipped_codes: list[str] = []
    persistent_retry_deferred_codes: list[str] = []
    low_priority_retry_deferred_codes: list[str] = []
    for candidate in raw_candidates:
        code = candidate["fcode"]
        cooldown_info = cooldown_map.get(code) or {}
        if cooldown_info:
            cooldown_skipped_codes.append(code)
            continue
        persistent_deferral_info = persistent_deferral_map.get(code) or {}
        if persistent_deferral_info:
            persistent_retry_deferred_codes.append(code)
            continue
        low_priority_deferral_info = low_priority_deferral_map.get(code) or {}
        if low_priority_deferral_info:
            low_priority_retry_deferred_codes.append(code)
            continue
        candidates.append(candidate)

    rotation_meta: dict[str, Any] = {}
    candidates, rotation_meta = _apply_candidate_rotation(
        candidates,
        db_path=db_path,
        publish_code_sets=publish_code_sets,
        governance_priority_codes=governance_priority_codes,
        rotation_state_path=rotation_state_path,
        rotation_limit=rotation_limit,
        consume_rotation=consume_rotation,
    )

    return candidates, {
        "raw_candidate_total": len(raw_candidates),
        "cooldown_skipped_total": len(cooldown_skipped_codes),
        "cooldown_skipped_codes_sample": cooldown_skipped_codes[:ARTIFACT_SAMPLE_LIMIT],
        "same_day_missing_response_cooldown_enabled": True,
        "persistent_retry_deferral_total": len(persistent_retry_deferred_codes),
        "persistent_retry_deferral_codes_sample": persistent_retry_deferred_codes[:ARTIFACT_SAMPLE_LIMIT],
        "persistent_retry_window_days": PERSISTENT_MISSING_RESPONSE_RETRY_WINDOW_DAYS,
        "persistent_missing_response_retry_deferral_enabled": True,
        "low_priority_retry_deferral_total": len(low_priority_retry_deferred_codes),
        "low_priority_retry_deferral_codes_sample": low_priority_retry_deferred_codes[:ARTIFACT_SAMPLE_LIMIT],
        "low_priority_retry_window_days": LOW_PRIORITY_MISSING_RESPONSE_RETRY_WINDOW_DAYS,
        "low_priority_missing_response_retry_deferral_enabled": bool(
            publish_code_sets and publish_code_sets.get("available")
        ),
        "rotation_applied": bool(rotation_meta.get("rotation_applied")),
        "priority_candidate_total": int(rotation_meta.get("priority_candidate_total") or 0),
        "governance_priority_candidate_total": int(rotation_meta.get("governance_priority_candidate_total") or 0),
        "rotating_low_priority_candidate_total": int(
            rotation_meta.get("rotating_low_priority_candidate_total") or 0
        ),
        "passthrough_candidate_total": int(rotation_meta.get("passthrough_candidate_total") or 0),
        "rotation_selected_total": int(rotation_meta.get("rotation_selected_total") or 0),
        "rotation_start_index": int(rotation_meta.get("rotation_start_index") or 0),
        "rotation_next_index": int(rotation_meta.get("rotation_next_index") or 0),
        "rotation_limit": int(rotation_meta.get("rotation_limit") or 0),
        "low_priority_reserved_capacity": int(rotation_meta.get("low_priority_reserved_capacity") or 0),
        "priority_rotation_selected_total": int(
            rotation_meta.get("priority_rotation_selected_total") or 0
        ),
        "priority_rotation_start_index": int(rotation_meta.get("priority_rotation_start_index") or 0),
        "priority_rotation_next_index": int(rotation_meta.get("priority_rotation_next_index") or 0),
        "priority_rotation_codes_sample": list(
            rotation_meta.get("priority_rotation_codes_sample") or []
        ),
        "governance_priority_selected_total": int(
            rotation_meta.get("governance_priority_selected_total") or 0
        ),
        "governance_priority_codes_sample": list(
            rotation_meta.get("governance_priority_codes_sample") or []
        ),
        "rotating_low_priority_codes_sample": list(
            rotation_meta.get("rotating_low_priority_codes_sample") or []
        ),
        "low_priority_rotation_selected_total": int(
            rotation_meta.get("low_priority_rotation_selected_total") or 0
        ),
        "low_priority_rotation_start_index": int(
            rotation_meta.get("low_priority_rotation_start_index") or 0
        ),
        "low_priority_rotation_next_index": int(
            rotation_meta.get("low_priority_rotation_next_index") or 0
        ),
        "business_date": effective_business_date.isoformat(),
    }


def _build_autostock_lookup(rows: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        code = _normalize_code(row.get("code"))
        if not code:
            continue
        lookup[code] = dict(row)
    return lookup


def _filter_lookup_by_requested_codes(
    lookup: dict[str, dict[str, Any]],
    requested_codes: list[str],
) -> dict[str, dict[str, Any]]:
    requested_set = {code for code in requested_codes if code}
    return {
        code: dict(row)
        for code, row in lookup.items()
        if code in requested_set
    }


def _missing_codes_from_lookup(
    requested_codes: list[str],
    lookup: dict[str, dict[str, Any]],
) -> list[str]:
    return [
        code
        for code in requested_codes
        if code and code not in lookup
    ]


def _merge_autostock_lookup(
    target_lookup: dict[str, dict[str, Any]],
    stage_source_map: dict[str, str],
    rows: list[dict[str, Any]] | None,
    *,
    source_name: str,
    requested_codes: list[str],
) -> int:
    merged = 0
    filtered_lookup = _filter_lookup_by_requested_codes(
        _build_autostock_lookup(rows),
        requested_codes,
    )
    for code, row in filtered_lookup.items():
        if code not in target_lookup:
            merged += 1
        target_lookup[code] = row
        stage_source_map[code] = source_name
    return merged


def _build_fallback_stage_meta(
    *,
    stage: str,
    source_name: str,
    requested_codes: list[str],
    batch_size: int,
    rows: list[dict[str, Any]] | None,
    batch_meta: dict[str, Any] | None,
    skipped: bool = False,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    payload = dict(batch_meta or {})
    payload["stage"] = stage
    payload["source_name"] = source_name
    payload["requested_total"] = len(requested_codes)
    payload["requested_codes_sample"] = requested_codes[:ARTIFACT_SAMPLE_LIMIT]
    payload["requested_batch_size"] = int(payload.get("requested_batch_size") or batch_size or 0)
    payload["effective_batch_size"] = int(payload.get("effective_batch_size") or batch_size or 0)
    payload["returned_total"] = int(payload.get("returned_total") or len(rows or []))
    payload["returned_unique_total"] = int(payload.get("returned_unique_total") or len(_build_autostock_lookup(rows)))
    payload["missing_count"] = int(
        payload.get("missing_count")
        or max(0, len(requested_codes) - payload["returned_unique_total"])
    )
    payload["quota_exhausted"] = bool(payload.get("quota_exhausted"))
    payload["skipped"] = bool(skipped)
    payload["skip_reason"] = str(skip_reason or "").strip() or None
    return payload


def _run_fund_batch_retry_stage(
    client: Any,
    requested_codes: list[str],
    *,
    batch_size: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows, batch_meta = client.get_fund_batch_with_meta(requested_codes, batch_size=batch_size)
    return list(rows or []), _build_fallback_stage_meta(
        stage="fund_batch_retry",
        source_name="autostock_fund_batch_retry",
        requested_codes=requested_codes,
        batch_size=batch_size,
        rows=list(rows or []),
        batch_meta=dict(batch_meta or {}),
    )


def _run_fund_detail_retry_stage(
    client: Any,
    requested_codes: list[str],
    *,
    batch_size: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    get_fund_detail_batch = getattr(client, "get_fund_detail_batch", None)
    if not callable(get_fund_detail_batch):
        return [], _build_fallback_stage_meta(
            stage="fund_detail_retry",
            source_name="autostock_fund_detail_batch_retry",
            requested_codes=requested_codes,
            batch_size=batch_size,
            rows=[],
            batch_meta={},
            skipped=True,
            skip_reason="client_missing_get_fund_detail_batch",
        )

    # This fallback only reads current NAV fields, not the fund's full historical arrays.
    history_end = date.today()
    history_start = history_end - timedelta(days=31)
    rows = list(get_fund_detail_batch(
        requested_codes, batch_size=batch_size,
        start_date=history_start.isoformat(), end_date=history_end.isoformat(),
    ) or [])
    get_last_meta = getattr(client, "get_last_fund_detail_batch_meta", None)
    batch_meta = dict(get_last_meta() or {}) if callable(get_last_meta) else {}
    batch_meta.update(history_start_date=history_start.isoformat(), history_end_date=history_end.isoformat())
    return rows, _build_fallback_stage_meta(
        stage="fund_detail_retry",
        source_name="autostock_fund_detail_batch_retry",
        requested_codes=requested_codes,
        batch_size=batch_size,
        rows=rows,
        batch_meta=batch_meta,
    )


def _run_single_code_retry_stage(
    client: Any,
    requested_codes: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    returned_codes: set[str] = set()
    quota_exhausted = False
    last_error: str | None = None
    failed_codes: list[str] = []

    for code in requested_codes:
        rows, batch_meta = client.get_fund_batch_with_meta([code], batch_size=1)
        row_list = list(rows or [])
        all_rows.extend(row_list)
        returned_codes.update(_build_autostock_lookup(row_list).keys())
        quota_exhausted = quota_exhausted or bool((batch_meta or {}).get("quota_exhausted"))
        last_error = str((batch_meta or {}).get("last_error") or last_error or "").strip() or None
        if code not in returned_codes:
            failed_codes.append(code)
        if quota_exhausted:
            break

    batch_meta = {
        "requested_unique_total": len(requested_codes),
        "returned_unique_total": len(returned_codes),
        "missing_count": max(0, len(requested_codes) - len(returned_codes)),
        "returned_total": len(all_rows),
        "quota_exhausted": quota_exhausted,
        "last_error": last_error,
        "failed_codes_preview": failed_codes[:ARTIFACT_SAMPLE_LIMIT],
    }
    return all_rows, _build_fallback_stage_meta(
        stage="single_code_retry",
        source_name="autostock_fund_single_retry",
        requested_codes=requested_codes,
        batch_size=1,
        rows=all_rows,
        batch_meta=batch_meta,
    )


def _fallback_stage_returned_zero(meta: dict[str, Any] | None) -> bool:
    payload = dict(meta or {})
    returned_total = (
        int(payload.get("returned_unique_total") or 0)
        if "returned_unique_total" in payload
        else int(payload.get("returned_total") or 0)
    )
    return (
        not bool(payload.get("skipped"))
        and int(payload.get("requested_total") or 0) > 0
        and returned_total == 0
        and not bool(payload.get("quota_exhausted"))
    )


def _should_open_upstream_empty_circuit(
    *,
    initial_lookup: dict[str, dict[str, Any]],
    fund_retry_meta: dict[str, Any] | None,
    detail_meta: dict[str, Any] | None,
) -> bool:
    return (
        not initial_lookup
        and _fallback_stage_returned_zero(fund_retry_meta)
        and _fallback_stage_returned_zero(detail_meta)
    )


def _run_autostock_fallback_recovery(
    *,
    client: Any,
    requested_codes: list[str],
    initial_lookup: dict[str, dict[str, Any]],
    source_name_map: dict[str, str],
    batch_size: int,
    settings: Settings,
    initial_quota_exhausted: bool,
    logger: Any,
) -> dict[str, Any]:
    initial_missing_codes = _missing_codes_from_lookup(requested_codes, initial_lookup)
    fallback_meta: dict[str, Any] = {
        "attempted": False,
        "initial_missing_count": len(initial_missing_codes),
        "initial_missing_codes_sample": initial_missing_codes[:ARTIFACT_SAMPLE_LIMIT],
        "recovered_total": 0,
        "remaining_missing_count": len(initial_missing_codes),
        "remaining_missing_codes_sample": initial_missing_codes[:ARTIFACT_SAMPLE_LIMIT],
        "upstream_empty_circuit_open": False,
        "empty_circuit_skipped_single_retry_count": 0,
        "stages": [],
    }
    if not initial_missing_codes:
        return fallback_meta
    if initial_quota_exhausted:
        fallback_meta["attempted"] = False
        fallback_meta["stages"].append(
            _build_fallback_stage_meta(
                stage="fund_batch_retry",
                source_name="autostock_fund_batch_retry",
                requested_codes=initial_missing_codes,
                batch_size=batch_size,
                rows=[],
                batch_meta={},
                skipped=True,
                skip_reason="initial_batch_quota_exhausted",
            )
        )
        return fallback_meta

    fallback_meta["attempted"] = True
    retry_batch_size = max(1, min(int(batch_size or 1), AUTOSTOCK_RETRY_BATCH_SIZE))

    logger(
        "AutoStock /fund 批量返回缺失，开始缩批补救: "
        f"missing_total={len(initial_missing_codes)}, retry_batch_size={retry_batch_size}"
    )
    fund_retry_rows, fund_retry_meta = _run_fund_batch_retry_stage(
        client,
        initial_missing_codes,
        batch_size=retry_batch_size,
    )
    recovered_total = _merge_autostock_lookup(
        initial_lookup,
        source_name_map,
        fund_retry_rows,
        source_name="autostock_fund_batch_retry",
        requested_codes=initial_missing_codes,
    )
    fund_retry_meta["recovered_total"] = recovered_total
    fallback_meta["stages"].append(fund_retry_meta)

    remaining_codes = _missing_codes_from_lookup(requested_codes, initial_lookup)
    detail_meta: dict[str, Any] | None = None
    if remaining_codes and not bool(fund_retry_meta.get("quota_exhausted")):
        logger(
            "AutoStock /fund 缩批后仍有缺失，开始 /fund/detail/list 补救: "
            f"missing_total={len(remaining_codes)}, retry_batch_size={retry_batch_size}"
        )
        detail_rows, detail_meta = _run_fund_detail_retry_stage(
            client,
            remaining_codes,
            batch_size=retry_batch_size,
        )
        recovered_total = _merge_autostock_lookup(
            initial_lookup,
            source_name_map,
            detail_rows,
            source_name="autostock_fund_detail_batch_retry",
            requested_codes=remaining_codes,
        )
        detail_meta["recovered_total"] = recovered_total
        fallback_meta["stages"].append(detail_meta)
        remaining_codes = _missing_codes_from_lookup(requested_codes, initial_lookup)

    max_calls_per_hour = int(getattr(settings, "autostock_rate_limit_max_calls_per_hour", 100) or 100)
    single_retry_limit = max(1, min(AUTOSTOCK_SINGLE_RETRY_LIMIT, max_calls_per_hour // 8))
    if remaining_codes:
        if _should_open_upstream_empty_circuit(
            initial_lookup=initial_lookup,
            fund_retry_meta=fund_retry_meta,
            detail_meta=detail_meta,
        ):
            logger(
                "AutoStock 批量、缩批与 detail/list 均为空返回，打开空返回熔断并跳过单码兜底: "
                f"missing_total={len(remaining_codes)}"
            )
            fallback_meta["upstream_empty_circuit_open"] = True
            fallback_meta["empty_circuit_skipped_single_retry_count"] = len(remaining_codes)
            single_meta = _build_fallback_stage_meta(
                stage="single_code_retry",
                source_name="autostock_fund_single_retry",
                requested_codes=[],
                batch_size=1,
                rows=[],
                batch_meta={},
                skipped=True,
                skip_reason="upstream_empty_circuit_open",
            )
            single_meta["upstream_empty_circuit_open"] = True
            single_meta["single_retry_limit"] = 0
            single_meta["recovered_total"] = 0
            single_meta["skipped_unprobed_count"] = len(remaining_codes)
            single_meta["skipped_unprobed_codes_sample"] = remaining_codes[:ARTIFACT_SAMPLE_LIMIT]
            fallback_meta["stages"].append(single_meta)
        else:
            single_retry_codes = remaining_codes[:single_retry_limit]
            single_retry_skipped_count = max(0, len(remaining_codes) - len(single_retry_codes))
            logger(
                "AutoStock 批量补救后仍有缺失，开始单码兜底探测: "
                f"missing_total={len(remaining_codes)}, single_retry_limit={single_retry_limit}"
            )
            single_rows, single_meta = _run_single_code_retry_stage(client, single_retry_codes)
            recovered_total = _merge_autostock_lookup(
                initial_lookup,
                source_name_map,
                single_rows,
                source_name="autostock_fund_single_retry",
                requested_codes=single_retry_codes,
            )
            single_meta["recovered_total"] = recovered_total
            single_meta["limited_probe"] = bool(single_retry_skipped_count > 0)
            single_meta["single_retry_limit"] = single_retry_limit
            single_meta["skipped_unprobed_count"] = single_retry_skipped_count
            single_meta["skipped_unprobed_codes_sample"] = remaining_codes[
                len(single_retry_codes):len(single_retry_codes) + ARTIFACT_SAMPLE_LIMIT
            ]
            fallback_meta["stages"].append(single_meta)
            remaining_codes = _missing_codes_from_lookup(requested_codes, initial_lookup)

    fallback_meta["recovered_total"] = len(initial_missing_codes) - len(remaining_codes)
    fallback_meta["remaining_missing_count"] = len(remaining_codes)
    fallback_meta["remaining_missing_codes_sample"] = remaining_codes[:ARTIFACT_SAMPLE_LIMIT]
    return fallback_meta


def _upsert_shadow_row(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        f"""
        INSERT INTO {AUTOSTOCK_SHADOW_TABLE} (
            fcode,
            shortname,
            local_issue_type,
            local_latest_nav,
            local_latest_nav_date,
            autostock_latest_nav,
            autostock_latest_nav_date,
            autostock_estimated_nav,
            autostock_estimated_change_pct,
            autostock_valuation_date,
            autostock_found,
            staleness_days,
            review_status,
            review_reason,
            source_name,
            batch_run_id,
            reviewed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fcode) DO UPDATE SET
            shortname = excluded.shortname,
            local_issue_type = excluded.local_issue_type,
            local_latest_nav = excluded.local_latest_nav,
            local_latest_nav_date = excluded.local_latest_nav_date,
            autostock_latest_nav = excluded.autostock_latest_nav,
            autostock_latest_nav_date = excluded.autostock_latest_nav_date,
            autostock_estimated_nav = excluded.autostock_estimated_nav,
            autostock_estimated_change_pct = excluded.autostock_estimated_change_pct,
            autostock_valuation_date = excluded.autostock_valuation_date,
            autostock_found = excluded.autostock_found,
            staleness_days = excluded.staleness_days,
            review_status = excluded.review_status,
            review_reason = excluded.review_reason,
            source_name = excluded.source_name,
            batch_run_id = excluded.batch_run_id,
            reviewed_at = excluded.reviewed_at,
            updated_at = excluded.updated_at
        """,
        (
            row.get("fcode"),
            row.get("shortname"),
            row.get("local_issue_type"),
            row.get("local_latest_nav"),
            row.get("local_latest_nav_date"),
            row.get("autostock_latest_nav"),
            row.get("autostock_latest_nav_date"),
            row.get("autostock_estimated_nav"),
            row.get("autostock_estimated_change_pct"),
            row.get("autostock_valuation_date"),
            int(bool(row.get("autostock_found"))),
            row.get("staleness_days"),
            row.get("review_status"),
            row.get("review_reason"),
            row.get("source_name"),
            row.get("batch_run_id"),
            row.get("reviewed_at"),
            row.get("updated_at"),
        ),
    )


def _insert_shadow_history_row(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        f"""
        INSERT INTO {AUTOSTOCK_SHADOW_HISTORY_TABLE} (
            fcode,
            shortname,
            local_issue_type,
            local_latest_nav,
            local_latest_nav_date,
            autostock_latest_nav,
            autostock_latest_nav_date,
            autostock_estimated_nav,
            autostock_estimated_change_pct,
            autostock_valuation_date,
            autostock_found,
            staleness_days,
            review_status,
            review_reason,
            source_name,
            batch_run_id,
            reviewed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row.get("fcode"),
            row.get("shortname"),
            row.get("local_issue_type"),
            row.get("local_latest_nav"),
            row.get("local_latest_nav_date"),
            row.get("autostock_latest_nav"),
            row.get("autostock_latest_nav_date"),
            row.get("autostock_estimated_nav"),
            row.get("autostock_estimated_change_pct"),
            row.get("autostock_valuation_date"),
            int(bool(row.get("autostock_found"))),
            row.get("staleness_days"),
            row.get("review_status"),
            row.get("review_reason"),
            row.get("source_name"),
            row.get("batch_run_id"),
            row.get("reviewed_at"),
            row.get("updated_at"),
        ),
    )


def _bootstrap_history_table_from_shadow_table(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, AUTOSTOCK_SHADOW_TABLE):
        return {"attempted": False, "inserted_total": 0, "reason": "shadow_table_missing"}
    if not _table_exists(conn, AUTOSTOCK_SHADOW_HISTORY_TABLE):
        return {"attempted": False, "inserted_total": 0, "reason": "history_table_missing"}

    history_total = int(
        conn.execute(f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}").fetchone()[0] or 0
    )
    if history_total > 0:
        return {"attempted": False, "inserted_total": 0, "reason": "history_already_initialized"}

    shadow_total = int(
        conn.execute(f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE}").fetchone()[0] or 0
    )
    if shadow_total <= 0:
        return {"attempted": False, "inserted_total": 0, "reason": "shadow_table_empty"}

    conn.execute(
        f"""
        INSERT INTO {AUTOSTOCK_SHADOW_HISTORY_TABLE} (
            fcode,
            shortname,
            local_issue_type,
            local_latest_nav,
            local_latest_nav_date,
            autostock_latest_nav,
            autostock_latest_nav_date,
            autostock_estimated_nav,
            autostock_estimated_change_pct,
            autostock_valuation_date,
            autostock_found,
            staleness_days,
            review_status,
            review_reason,
            source_name,
            batch_run_id,
            reviewed_at,
            updated_at
        )
        SELECT
            fcode,
            shortname,
            local_issue_type,
            local_latest_nav,
            local_latest_nav_date,
            autostock_latest_nav,
            autostock_latest_nav_date,
            autostock_estimated_nav,
            autostock_estimated_change_pct,
            autostock_valuation_date,
            autostock_found,
            staleness_days,
            review_status,
            review_reason,
            source_name,
            batch_run_id,
            reviewed_at,
            updated_at
        FROM {AUTOSTOCK_SHADOW_TABLE}
        """
    )
    return {
        "attempted": True,
        "inserted_total": shadow_total,
        "reason": "bootstrapped_from_shadow_current_state",
    }


def load_autostock_nav_tail_fill_snapshot(settings: Settings) -> dict[str, Any]:
    snapshot = {
        "table_exists": False,
        "history_table_exists": False,
        "history_reviewed_total": 0,
        "history_coverage_start_date": None,
        "history_coverage_business_days": 0,
        "persistent_missing_response_tracking_ready": False,
        "history_bootstrap_attempted": False,
        "history_bootstrap_inserted_total": 0,
        "history_bootstrap_reason": None,
        "latest_batch_run_id": None,
        "reviewed_total": 0,
        "shadow_fill_ready_total": 0,
        "stale_nav_rejected_total": 0,
        "empty_nav_rejected_total": 0,
        "missing_response_total": 0,
        "last_reviewed_at": None,
        "lifetime_reviewed_total": 0,
        "lifetime_shadow_fill_ready_total": 0,
        "lifetime_stale_nav_rejected_total": 0,
        "lifetime_empty_nav_rejected_total": 0,
        "lifetime_missing_response_total": 0,
        "cooldown_active_today": False,
        "next_retry_business_date": None,
        "first_day_missing_response_total": 0,
        "persistent_missing_response_total": 0,
        "severe_persistent_missing_response_total": 0,
        "persistent_missing_response_max_days": 0,
        "missing_response_observation_bucket_summary": [],
        "missing_response_observation_codes_sample": [],
        "first_day_missing_response_codes_sample": [],
        "persistent_missing_response_codes_sample": [],
        "severe_persistent_missing_response_codes_sample": [],
    }
    db_path = getattr(settings, "database_path", None)
    if not db_path or not Path(db_path).exists():
        return snapshot

    conn = sqlite3.connect(str(db_path))
    try:
        if not _table_exists(conn, AUTOSTOCK_SHADOW_TABLE):
            return snapshot
        _ensure_shadow_table(conn)
        bootstrap_meta = _bootstrap_history_table_from_shadow_table(conn)
        snapshot["history_bootstrap_attempted"] = bool(bootstrap_meta.get("attempted"))
        snapshot["history_bootstrap_inserted_total"] = int(bootstrap_meta.get("inserted_total") or 0)
        snapshot["history_bootstrap_reason"] = str(bootstrap_meta.get("reason") or "").strip() or None
        if snapshot["history_bootstrap_attempted"]:
            conn.commit()
        snapshot["table_exists"] = True
        history_table_exists = _table_exists(conn, AUTOSTOCK_SHADOW_HISTORY_TABLE)
        snapshot["history_table_exists"] = history_table_exists
        if history_table_exists:
            snapshot["history_reviewed_total"] = int(
                conn.execute(f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}").fetchone()[0] or 0
            )
            snapshot["history_coverage_start_date"] = conn.execute(
                f"SELECT MIN(SUBSTR(reviewed_at, 1, 10)) FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}"
            ).fetchone()[0]
            snapshot["history_coverage_business_days"] = int(
                conn.execute(
                    f"SELECT COUNT(DISTINCT SUBSTR(reviewed_at, 1, 10)) FROM {AUTOSTOCK_SHADOW_HISTORY_TABLE}"
                ).fetchone()[0]
                or 0
            )
            snapshot["persistent_missing_response_tracking_ready"] = bool(
                snapshot["history_reviewed_total"] > 0
            )
        latest_batch_run_id = conn.execute(
            f"SELECT batch_run_id FROM {AUTOSTOCK_SHADOW_TABLE} ORDER BY reviewed_at DESC, batch_run_id DESC LIMIT 1"
        ).fetchone()
        latest_batch_run_id_value = str((latest_batch_run_id or [None])[0] or "").strip() or None
        snapshot["latest_batch_run_id"] = latest_batch_run_id_value

        snapshot["lifetime_reviewed_total"] = int(
            conn.execute(f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE}").fetchone()[0] or 0
        )
        snapshot["lifetime_shadow_fill_ready_total"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE review_status = 'shadow_fill_ready'"
            ).fetchone()[0]
            or 0
        )
        snapshot["lifetime_stale_nav_rejected_total"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE review_status = 'stale_nav_rejected'"
            ).fetchone()[0]
            or 0
        )
        snapshot["lifetime_empty_nav_rejected_total"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE review_status = 'empty_nav_rejected'"
            ).fetchone()[0]
            or 0
        )
        snapshot["lifetime_missing_response_total"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE review_status = 'missing_response'"
            ).fetchone()[0]
            or 0
        )
        snapshot["last_reviewed_at"] = conn.execute(
            f"SELECT MAX(reviewed_at) FROM {AUTOSTOCK_SHADOW_TABLE}"
        ).fetchone()[0]

        if latest_batch_run_id_value:
            snapshot["reviewed_total"] = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE batch_run_id = ?",
                    (latest_batch_run_id_value,),
                ).fetchone()[0]
                or 0
            )
            snapshot["shadow_fill_ready_total"] = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE batch_run_id = ? AND review_status = 'shadow_fill_ready'",
                    (latest_batch_run_id_value,),
                ).fetchone()[0]
                or 0
            )
            snapshot["stale_nav_rejected_total"] = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE batch_run_id = ? AND review_status = 'stale_nav_rejected'",
                    (latest_batch_run_id_value,),
                ).fetchone()[0]
                or 0
            )
            snapshot["empty_nav_rejected_total"] = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE batch_run_id = ? AND review_status = 'empty_nav_rejected'",
                    (latest_batch_run_id_value,),
                ).fetchone()[0]
                or 0
            )
            snapshot["missing_response_total"] = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {AUTOSTOCK_SHADOW_TABLE} WHERE batch_run_id = ? AND review_status = 'missing_response'",
                    (latest_batch_run_id_value,),
                ).fetchone()[0]
                or 0
            )
            latest_missing_rows: list[tuple[Any, Any, Any, Any]] = []
            if history_table_exists:
                latest_missing_rows = [
                    (
                        str(item.get("fcode") or "").strip(),
                        str(item.get("first_review_date") or "").strip(),
                        str(item.get("last_review_date") or "").strip(),
                        int(item.get("missing_business_days") or 0),
                    )
                    for item in _load_current_missing_response_observation_rows(
                        conn,
                        latest_batch_run_id=latest_batch_run_id_value,
                    )
                ]
            persistent_rows: list[dict[str, Any]] = []
            first_day_rows: list[dict[str, Any]] = []
            severe_rows: list[dict[str, Any]] = []
            all_observation_rows: list[dict[str, Any]] = []
            severe_total = 0
            first_day_total = 0
            persistent_total = 0
            persistent_max_days = 0
            bucket_counts = {
                "1d": 0,
                "2d": 0,
                "3d_plus": 0,
            }
            for row in latest_missing_rows:
                code = _normalize_code(row[0])
                missing_business_days = int(row[3] or 0)
                bucket_key = _missing_response_observation_bucket(missing_business_days)
                persistent_max_days = max(persistent_max_days, missing_business_days)
                bucket_counts[bucket_key] += 1
                observation_row = {
                    "fcode": code,
                    "first_review_date": str(row[1] or "").strip() or None,
                    "last_review_date": str(row[2] or "").strip() or None,
                    "missing_business_days": missing_business_days,
                    "bucket": bucket_key,
                }
                all_observation_rows.append(observation_row)
                if missing_business_days == 1:
                    first_day_total += 1
                    first_day_rows.append(dict(observation_row))
                if missing_business_days >= 2:
                    persistent_total += 1
                    persistent_rows.append(dict(observation_row))
                if missing_business_days >= 3:
                    severe_total += 1
                    severe_rows.append(dict(observation_row))
            snapshot["first_day_missing_response_total"] = first_day_total
            snapshot["persistent_missing_response_total"] = persistent_total
            snapshot["severe_persistent_missing_response_total"] = severe_total
            snapshot["persistent_missing_response_max_days"] = persistent_max_days
            snapshot["missing_response_observation_bucket_summary"] = [
                {"bucket": "1d", "count": int(bucket_counts["1d"])},
                {"bucket": "2d", "count": int(bucket_counts["2d"])},
                {"bucket": "3d_plus", "count": int(bucket_counts["3d_plus"])},
            ]
            snapshot["missing_response_observation_codes_sample"] = all_observation_rows[:ARTIFACT_SAMPLE_LIMIT]
            snapshot["first_day_missing_response_codes_sample"] = first_day_rows[:ARTIFACT_SAMPLE_LIMIT]
            snapshot["persistent_missing_response_codes_sample"] = persistent_rows[:ARTIFACT_SAMPLE_LIMIT]
            snapshot["severe_persistent_missing_response_codes_sample"] = severe_rows[:ARTIFACT_SAMPLE_LIMIT]

        last_review_date = _parse_iso_date(snapshot.get("last_reviewed_at"))
        today = datetime.now().date()
        snapshot["cooldown_active_today"] = bool(
            snapshot["missing_response_total"] > 0
            and last_review_date is not None
            and last_review_date == today
        )
        snapshot["next_retry_business_date"] = _next_business_date(last_review_date)
    finally:
        conn.close()
    return snapshot


def build_missing_response_publish_impact_summary(
    settings: Settings,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "available": False,
        "latest_report_run_id": None,
        "persistent_missing_response_related_publish_count": 0,
        "persistent_missing_response_official_count": 0,
        "persistent_missing_response_portfolio_count": 0,
        "severe_persistent_missing_response_related_publish_count": 0,
        "severe_persistent_missing_response_official_count": 0,
        "severe_persistent_missing_response_portfolio_count": 0,
        "persistent_missing_response_official_codes_sample": [],
        "persistent_missing_response_portfolio_codes_sample": [],
        "severe_persistent_missing_response_official_codes_sample": [],
        "severe_persistent_missing_response_portfolio_codes_sample": [],
        "reason": "unavailable",
    }
    db_path = getattr(settings, "database_path", None)
    if not db_path or not Path(db_path).exists():
        summary["reason"] = "database_missing"
        return summary

    publish_code_sets = _load_latest_publish_code_sets(settings)
    summary["latest_report_run_id"] = publish_code_sets.get("run_id")
    if not publish_code_sets.get("available"):
        summary["reason"] = str(publish_code_sets.get("reason") or "latest_publish_codes_unavailable")
        return summary

    snapshot_payload = snapshot or load_autostock_nav_tail_fill_snapshot(settings)
    latest_batch_run_id = str(snapshot_payload.get("latest_batch_run_id") or "").strip() or None
    conn = sqlite3.connect(str(db_path))
    try:
        all_rows = _load_current_missing_response_observation_rows(
            conn,
            latest_batch_run_id=latest_batch_run_id,
        )
    finally:
        conn.close()

    official_codes = set(publish_code_sets.get("official_codes") or set())
    portfolio_codes = set(publish_code_sets.get("portfolio_codes") or set())
    persistent_codes = {
        str(item.get("fcode") or "").strip()
        for item in all_rows
        if int(item.get("missing_business_days") or 0) >= 2
    }
    severe_codes = {
        str(item.get("fcode") or "").strip()
        for item in all_rows
        if int(item.get("missing_business_days") or 0) >= 3
    }
    persistent_official = sorted(persistent_codes & official_codes)
    persistent_portfolio = sorted(persistent_codes & portfolio_codes)
    severe_official = sorted(severe_codes & official_codes)
    severe_portfolio = sorted(severe_codes & portfolio_codes)

    summary.update(
        {
            "available": True,
            "persistent_missing_response_related_publish_count": len(
                (persistent_codes & official_codes) | (persistent_codes & portfolio_codes)
            ),
            "persistent_missing_response_official_count": len(persistent_official),
            "persistent_missing_response_portfolio_count": len(persistent_portfolio),
            "severe_persistent_missing_response_related_publish_count": len(
                (severe_codes & official_codes) | (severe_codes & portfolio_codes)
            ),
            "severe_persistent_missing_response_official_count": len(severe_official),
            "severe_persistent_missing_response_portfolio_count": len(severe_portfolio),
            "persistent_missing_response_official_codes_sample": persistent_official[:ARTIFACT_SAMPLE_LIMIT],
            "persistent_missing_response_portfolio_codes_sample": persistent_portfolio[:ARTIFACT_SAMPLE_LIMIT],
            "severe_persistent_missing_response_official_codes_sample": severe_official[:ARTIFACT_SAMPLE_LIMIT],
            "severe_persistent_missing_response_portfolio_codes_sample": severe_portfolio[:ARTIFACT_SAMPLE_LIMIT],
            "reason": "ok",
        }
    )
    return summary


def build_missing_response_observation_payload(settings: Settings) -> dict[str, Any]:
    snapshot = load_autostock_nav_tail_fill_snapshot(settings)
    publish_impact_summary = build_missing_response_publish_impact_summary(
        settings,
        snapshot=snapshot,
    )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "latest_batch_run_id": snapshot.get("latest_batch_run_id"),
        "tracking_ready": bool(snapshot.get("persistent_missing_response_tracking_ready")),
        "cooldown_active_today": bool(snapshot.get("cooldown_active_today")),
        "next_retry_business_date": snapshot.get("next_retry_business_date"),
        "history_table_exists": bool(snapshot.get("history_table_exists")),
        "history_reviewed_total": int(snapshot.get("history_reviewed_total") or 0),
        "history_coverage_start_date": snapshot.get("history_coverage_start_date"),
        "history_coverage_business_days": int(snapshot.get("history_coverage_business_days") or 0),
        "latest_missing_response_total": int(snapshot.get("missing_response_total") or 0),
        "first_day_missing_response_total": int(snapshot.get("first_day_missing_response_total") or 0),
        "persistent_missing_response_total": int(snapshot.get("persistent_missing_response_total") or 0),
        "severe_persistent_missing_response_total": int(snapshot.get("severe_persistent_missing_response_total") or 0),
        "persistent_missing_response_max_days": int(snapshot.get("persistent_missing_response_max_days") or 0),
        "bucket_summary": list(snapshot.get("missing_response_observation_bucket_summary") or []),
        "observation_rows": list(snapshot.get("missing_response_observation_codes_sample") or []),
        "first_day_rows": list(snapshot.get("first_day_missing_response_codes_sample") or []),
        "persistent_rows": list(snapshot.get("persistent_missing_response_codes_sample") or []),
        "severe_rows": list(snapshot.get("severe_persistent_missing_response_codes_sample") or []),
        "publish_impact_summary": publish_impact_summary,
    }


def write_missing_response_observation_payload(settings: Settings) -> dict[str, Any]:
    payload = build_missing_response_observation_payload(settings)
    artifact_file = settings.reports_dir / "audit" / "autostock_missing_response_observation" / (
        f"autostock_missing_response_observation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    _write_json(artifact_file, payload)
    payload["artifact_path"] = str(artifact_file)
    return payload


def run_autostock_nav_tail_fill(
    settings: Settings,
    *,
    log: Any | None = None,
) -> dict[str, Any]:
    logger = log or (lambda message: None)
    if not getattr(settings, "autostock_enabled", True):
        from backend.nav_history_maintenance import run_nav_history_maintenance
        logger("AutoStock 已停用，净值尾部补数改用天天基金历史净值；不写入 AutoStock 独立来源证据")
        replacement = run_nav_history_maintenance(
            settings, log=logger, limit=max(1, int(settings.scheduler_daily_nav_history_sync_limit or 100)),
            allowed_issue_types=["missing_nav", "nav_value_mismatch", "returns_missing",
                                 "nav_history_lag", "returns_lag", "recent_tail_lag"],
        )
        if int((replacement.get("maintenance_result") or {}).get("failed_total") or 0):
            raise RuntimeError("tiantian_nav_history_replacement_partial_failure")
        return {"status": "replaced", "source_name": "tiantian_nav_history",
                "autostock_status": "disabled", "independent_source": False,
                "replacement": replacement}
    batch_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    reviewed_at = datetime.now().isoformat(timespec="seconds")
    business_date = datetime.now().date()
    publish_code_sets = _load_latest_publish_code_sets(settings)
    governance_priority_codes = load_final_selection_governance_action_codes(
        settings,
        "resolve_autostock_unknown",
    )
    rotation_state_path = _resolve_rotation_state_path(settings)
    candidates, selection_meta = _read_missing_latest_nav_candidates(
        settings.database_path,
        business_date=business_date,
        publish_code_sets=publish_code_sets,
        governance_priority_codes=governance_priority_codes,
        rotation_state_path=rotation_state_path,
        rotation_limit=AUTOSTOCK_ROTATING_PROBE_LIMIT,
        consume_rotation=True,
    )
    summary: dict[str, Any] = {
        "generated_at": reviewed_at,
        "batch_run_id": batch_run_id,
        "status": "success",
        "source_name": "autostock_fund_batch",
        "selection": {
            "candidate_total": len(candidates),
            "raw_candidate_total": int(selection_meta.get("raw_candidate_total") or len(candidates)),
            "cooldown_skipped_total": int(selection_meta.get("cooldown_skipped_total") or 0),
            "cooldown_skipped_codes_sample": list(selection_meta.get("cooldown_skipped_codes_sample") or []),
            "same_day_missing_response_cooldown_enabled": bool(
                selection_meta.get("same_day_missing_response_cooldown_enabled")
            ),
            "persistent_retry_deferral_total": int(
                selection_meta.get("persistent_retry_deferral_total") or 0
            ),
            "persistent_retry_deferral_codes_sample": list(
                selection_meta.get("persistent_retry_deferral_codes_sample") or []
            ),
            "persistent_retry_window_days": int(
                selection_meta.get("persistent_retry_window_days")
                or PERSISTENT_MISSING_RESPONSE_RETRY_WINDOW_DAYS
            ),
            "persistent_missing_response_retry_deferral_enabled": bool(
                selection_meta.get("persistent_missing_response_retry_deferral_enabled")
            ),
            "low_priority_retry_deferral_total": int(
                selection_meta.get("low_priority_retry_deferral_total") or 0
            ),
            "low_priority_retry_deferral_codes_sample": list(
                selection_meta.get("low_priority_retry_deferral_codes_sample") or []
            ),
            "low_priority_retry_window_days": int(
                selection_meta.get("low_priority_retry_window_days")
                or LOW_PRIORITY_MISSING_RESPONSE_RETRY_WINDOW_DAYS
            ),
            "low_priority_missing_response_retry_deferral_enabled": bool(
                selection_meta.get("low_priority_missing_response_retry_deferral_enabled")
            ),
            "rotation_applied": bool(selection_meta.get("rotation_applied")),
            "priority_candidate_total": int(selection_meta.get("priority_candidate_total") or 0),
            "governance_priority_candidate_total": int(
                selection_meta.get("governance_priority_candidate_total") or 0
            ),
            "rotating_low_priority_candidate_total": int(
                selection_meta.get("rotating_low_priority_candidate_total") or 0
            ),
            "passthrough_candidate_total": int(selection_meta.get("passthrough_candidate_total") or 0),
            "rotation_selected_total": int(selection_meta.get("rotation_selected_total") or 0),
            "rotation_start_index": int(selection_meta.get("rotation_start_index") or 0),
            "rotation_next_index": int(selection_meta.get("rotation_next_index") or 0),
            "rotation_limit": int(
                selection_meta.get("rotation_limit") or AUTOSTOCK_ROTATING_PROBE_LIMIT
            ),
            "priority_rotation_selected_total": int(
                selection_meta.get("priority_rotation_selected_total") or 0
            ),
            "priority_rotation_start_index": int(
                selection_meta.get("priority_rotation_start_index") or 0
            ),
            "priority_rotation_next_index": int(
                selection_meta.get("priority_rotation_next_index") or 0
            ),
            "priority_rotation_codes_sample": list(
                selection_meta.get("priority_rotation_codes_sample") or []
            ),
            "governance_priority_selected_total": int(
                selection_meta.get("governance_priority_selected_total") or 0
            ),
            "governance_priority_codes_sample": list(
                selection_meta.get("governance_priority_codes_sample") or []
            ),
            "rotating_low_priority_codes_sample": list(
                selection_meta.get("rotating_low_priority_codes_sample") or []
            ),
            "low_priority_rotation_selected_total": int(
                selection_meta.get("low_priority_rotation_selected_total") or 0
            ),
            "low_priority_rotation_start_index": int(
                selection_meta.get("low_priority_rotation_start_index") or 0
            ),
            "low_priority_rotation_next_index": int(
                selection_meta.get("low_priority_rotation_next_index") or 0
            ),
            "business_date": str(selection_meta.get("business_date") or business_date.isoformat()),
            "issue_type": "missing_latest_nav_date",
            "recent_nav_max_age_days": RECENT_NAV_MAX_AGE_DAYS,
            "candidate_codes_sample": [row["fcode"] for row in candidates[:ARTIFACT_SAMPLE_LIMIT]],
        },
        "batch_meta": {},
        "maintenance_result": {
            "processed_total": len(candidates),
            "returned_total": 0,
            "shadow_fill_ready_total": 0,
            "stale_nav_rejected_total": 0,
            "empty_nav_rejected_total": 0,
            "missing_response_total": 0,
            "fallback_recovered_total": 0,
            "fallback_remaining_missing_total": 0,
            "upstream_empty_circuit_open": False,
            "empty_circuit_skipped_single_retry_count": 0,
            "failed_total": 0,
            "quota_exhausted": False,
            "shadow_fill_ready_codes_sample": [],
            "stale_nav_rejected_codes_sample": [],
            "missing_response_codes_sample": [],
        },
        "fallback_recovery": {
            "attempted": False,
            "initial_missing_count": 0,
            "initial_missing_codes_sample": [],
            "recovered_total": 0,
            "remaining_missing_count": 0,
            "remaining_missing_codes_sample": [],
            "upstream_empty_circuit_open": False,
            "empty_circuit_skipped_single_retry_count": 0,
            "stages": [],
        },
        "observation_snapshot": {},
    }

    conn = sqlite3.connect(str(settings.database_path))
    try:
        _ensure_shadow_table(conn)
        conn.commit()
    finally:
        conn.close()

    if not candidates:
        summary["observation_snapshot"] = load_autostock_nav_tail_fill_snapshot(settings)
        observation_payload = write_missing_response_observation_payload(settings)
        summary["observation_artifact_path"] = observation_payload.get("artifact_path")
        artifact_file = settings.reports_dir / "audit" / "autostock_nav_tail_fill" / (
            f"autostock_nav_tail_fill_{batch_run_id}.json"
        )
        _write_json(artifact_file, summary)
        summary["artifact_path"] = str(artifact_file)
        logger(
            "AutoStock 净值尾差复查完成：当前无可执行候选，"
            f"raw_candidate_total={summary['selection']['raw_candidate_total']}, "
            f"cooldown_skipped_total={summary['selection']['cooldown_skipped_total']}, "
            f"persistent_retry_deferral_total={summary['selection']['persistent_retry_deferral_total']}, "
            f"low_priority_retry_deferral_total={summary['selection']['low_priority_retry_deferral_total']}, "
            f"rotation_selected_total={summary['selection']['rotation_selected_total']}"
        )
        return summary

    client, batch_size = _build_autostock_client(settings)
    codes = [row["fcode"] for row in candidates]
    logger(f"开始 AutoStock 批量复查缺失最新净值日期债基: candidate_total={len(codes)}")
    rows, batch_meta = client.get_fund_batch_with_meta(codes, batch_size=batch_size)
    summary["batch_meta"] = dict(batch_meta or {})
    summary["maintenance_result"]["quota_exhausted"] = bool((batch_meta or {}).get("quota_exhausted"))
    autostock_lookup = _filter_lookup_by_requested_codes(_build_autostock_lookup(rows), codes)
    source_name_map: dict[str, str] = {code: "autostock_fund_batch" for code in autostock_lookup}

    fallback_recovery = _run_autostock_fallback_recovery(
        client=client,
        requested_codes=codes,
        initial_lookup=autostock_lookup,
        source_name_map=source_name_map,
        batch_size=batch_size,
        settings=settings,
        initial_quota_exhausted=bool((batch_meta or {}).get("quota_exhausted")),
        logger=logger,
    )
    summary["fallback_recovery"] = fallback_recovery
    summary["maintenance_result"]["returned_total"] = len(autostock_lookup)
    summary["maintenance_result"]["fallback_recovered_total"] = int(fallback_recovery.get("recovered_total") or 0)
    summary["maintenance_result"]["fallback_remaining_missing_total"] = int(
        fallback_recovery.get("remaining_missing_count") or 0
    )
    summary["maintenance_result"]["upstream_empty_circuit_open"] = bool(
        fallback_recovery.get("upstream_empty_circuit_open")
    )
    summary["maintenance_result"]["empty_circuit_skipped_single_retry_count"] = int(
        fallback_recovery.get("empty_circuit_skipped_single_retry_count") or 0
    )

    today = datetime.now().date()

    conn = sqlite3.connect(str(settings.database_path))
    try:
        _ensure_shadow_table(conn)
        for candidate in candidates:
            code = candidate["fcode"]
            autostock_row = autostock_lookup.get(code) or {}
            source_name = source_name_map.get(code) or "autostock_fund_batch"
            latest_nav = _to_float(autostock_row.get("netWorth"))
            latest_nav_date_text = str(autostock_row.get("netWorthDate") or "").strip() or None
            latest_nav_date = _parse_iso_date(latest_nav_date_text)
            estimated_nav = _to_float(autostock_row.get("expectWorth"))
            estimated_change_pct = _to_float(autostock_row.get("expectGrowth"))
            valuation_date_text = str(
                autostock_row.get("expectWorthDate") or autostock_row.get("expectDate") or ""
            ).strip() or None
            staleness_days = (today - latest_nav_date).days if latest_nav_date else None

            if not autostock_row:
                review_status = "missing_response"
                review_reason = "code_missing_in_autostock_batch_response"
                summary["maintenance_result"]["missing_response_total"] += 1
                summary["maintenance_result"]["missing_response_codes_sample"].append(code)
            elif latest_nav is None or latest_nav_date_text is None:
                review_status = "empty_nav_rejected"
                review_reason = "autostock_latest_nav_or_date_empty"
                summary["maintenance_result"]["empty_nav_rejected_total"] += 1
            elif staleness_days is not None and staleness_days > RECENT_NAV_MAX_AGE_DAYS:
                review_status = "stale_nav_rejected"
                review_reason = f"autostock_latest_nav_too_old_{staleness_days}d"
                summary["maintenance_result"]["stale_nav_rejected_total"] += 1
                summary["maintenance_result"]["stale_nav_rejected_codes_sample"].append(code)
            else:
                review_status = "shadow_fill_ready"
                review_reason = "autostock_latest_nav_available_recent"
                summary["maintenance_result"]["shadow_fill_ready_total"] += 1
                summary["maintenance_result"]["shadow_fill_ready_codes_sample"].append(code)

            row_payload = {
                "fcode": code,
                "shortname": candidate.get("shortname"),
                "local_issue_type": candidate.get("local_issue_type"),
                "local_latest_nav": candidate.get("local_latest_nav"),
                "local_latest_nav_date": candidate.get("local_latest_nav_date"),
                "autostock_latest_nav": latest_nav,
                "autostock_latest_nav_date": latest_nav_date_text,
                "autostock_estimated_nav": estimated_nav,
                "autostock_estimated_change_pct": estimated_change_pct,
                "autostock_valuation_date": valuation_date_text,
                "autostock_found": bool(autostock_row),
                "staleness_days": staleness_days,
                "review_status": review_status,
                "review_reason": review_reason,
                "source_name": source_name,
                "batch_run_id": batch_run_id,
                "reviewed_at": reviewed_at,
                "updated_at": reviewed_at,
            }
            _upsert_shadow_row(conn, row_payload)
            _insert_shadow_history_row(conn, row_payload)
        conn.commit()
    finally:
        conn.close()

    for key in (
        "shadow_fill_ready_codes_sample",
        "stale_nav_rejected_codes_sample",
        "missing_response_codes_sample",
    ):
        summary["maintenance_result"][key] = summary["maintenance_result"][key][:ARTIFACT_SAMPLE_LIMIT]

    summary["observation_snapshot"] = load_autostock_nav_tail_fill_snapshot(settings)
    observation_payload = write_missing_response_observation_payload(settings)
    summary["observation_artifact_path"] = observation_payload.get("artifact_path")
    artifact_file = settings.reports_dir / "audit" / "autostock_nav_tail_fill" / (
        f"autostock_nav_tail_fill_{batch_run_id}.json"
    )
    _write_json(artifact_file, summary)
    summary["artifact_path"] = str(artifact_file)
    logger(
        "AutoStock 净值尾差复查完成: "
        f"candidate_total={len(candidates)}, "
        f"shadow_fill_ready_total={summary['maintenance_result']['shadow_fill_ready_total']}, "
        f"stale_nav_rejected_total={summary['maintenance_result']['stale_nav_rejected_total']}, "
        f"missing_response_total={summary['maintenance_result']['missing_response_total']}, "
        f"fallback_recovered_total={summary['maintenance_result']['fallback_recovered_total']}"
    )
    return summary
