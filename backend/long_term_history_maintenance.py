from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from core.utils.buyability import normalize_buyability
from core.utils.holding_period_policy import combine_holding_period_names, holding_period_gate_reason

from backend.config import Settings
from backend.effective_nav import (
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    load_effective_nav_snapshot,
)
from backend.final_selection_governance_queue import load_final_selection_governance_queue


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.client import APIClient  # noqa: E402
from collectors.annual_return_collector import AnnualReturnCollector  # noqa: E402
from collectors.rank_collector import RankCollector  # noqa: E402
from database.connection import get_db_manager  # noqa: E402
from database.nav_history_queries import (  # noqa: E402
    attach_nav_history_archive_if_available,
    build_nav_history_union_source_sql,
    detach_nav_history_archive,
    load_latest_nav_history_archive_binding,
    nav_history_archive_binding_matches_database,
)
from database.rank_history_queries import (  # noqa: E402
    begin_rank_history_read_snapshot,
    attach_rank_history_archive_if_available,
    build_rank_history_union_source_sql,
    detach_rank_history_archive,
    load_latest_rank_history_archive_binding,
    rank_history_archive_binding_matches_database,
    read_latest_rank_dates,
)
from utils.fund_exclusion_rules import (  # noqa: E402
    DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH,
    clear_fund_exclusion_rules_cache,
    get_runtime_excluded_fund_codes,
)
from utils.project_runtime_config import load_runtime_config  # noqa: E402
from scripts.audit_runtime_utils import build_audit_artifact_path  # noqa: E402
from backend.task_audit_storage import glob_json_artifacts, read_task_audit, write_task_audit


AUDIT_SAMPLE_LIMIT = 50
DEFAULT_BATCH_SIZE = 20
NAV_FRESHNESS_DAYS = 10
LONG_TERM_HISTORY_MIN_AGE_DAYS = 365
MAX_FAILURE_COOLDOWN_RUNS = 3
RECENT_DISCOVERY_WITHOUT_NAV_GRACE_DAYS = 45
ISSUE_PRIORITY = {
    "missing_annual_returns": 0,
    "missing_rank_history": 1,
    "missing_both_histories": 2,
    "missing_annual_and_stale_rank": 3,
    "missing_rank_and_stale_annual": 4,
    "stale_both_histories": 5,
    "stale_annual_returns": 6,
    "stale_rank_history": 7,
}
DEFAULT_EMPTY_PAYLOAD_RETRY_DAYS = 7
DEFAULT_EMPTY_PAYLOAD_UPSTREAM_LIMITED_FAILURES = 3
DEFAULT_STALE_REFRESH_UPSTREAM_LIMITED_FAILURES = 2
DEFAULT_HIGH_PRIORITY_STALE_BACKLOG_TRIGGER = 400
DEFAULT_HIGH_PRIORITY_STALE_LIMIT = 160
AUTO_SCOPE_SAMPLE_LIMIT = 10
STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION = 2
# 对 upstream-limited 观察池而言，净值超过约 1 个季度仍未恢复，
# 就不应再与“仍在活跃更新中的样本”混在同一个主观察池。
LONG_TERM_UPSTREAM_STALE_NAV_DAYS = 90
LONG_TERM_UPSTREAM_TERMINAL_STALE_NAV_DAYS = 365
LOCAL_ANNUAL_RETURN_BACKFILL_MIN_POINT_COUNT = 80
LOCAL_ANNUAL_RETURN_BACKFILL_START_DAY_MAX = 20
LOCAL_ANNUAL_RETURN_BACKFILL_END_DAY_MIN = 345
# buyable_active_priority 且仅 rank stale 的本地缺口，若净值滞后仍处于中度窗口内，
# 继续放在 scope_not_ready 会让高价值样本长期饿死。允许 <=120 天直接回到自动补刷池，
# 但仍排除 deep/terminal stale。
BUYABLE_ACTIVE_STALE_RANK_RETRY_MAX_NAV_STALENESS_DAYS = 120


def _classify_priority_scope_not_ready_nav_bucket(nav_staleness_days: Any) -> str:
    if nav_staleness_days in (None, ""):
        return "unknown"
    try:
        days = int(float(nav_staleness_days))
    except (TypeError, ValueError):
        return "unknown"
    if days < 0:
        return "unknown"
    if days > 365:
        return "terminal_gt_365d"
    if days > 120:
        return "deep_121_365d"
    if days > 60:
        return "medium_61_120d"
    if days > 30:
        return "short_31_60d"
    return "fresh_0_30d"


def _build_stale_scope_recoverability_summary(
    counts_by_nav_bucket: dict[str, int],
) -> dict[str, Any]:
    fresh_recoverable_total = int(counts_by_nav_bucket.get("fresh_0_30d") or 0)
    short_recoverable_total = int(counts_by_nav_bucket.get("short_31_60d") or 0)
    medium_watch_total = int(counts_by_nav_bucket.get("medium_61_120d") or 0)
    deep_watch_total = int(counts_by_nav_bucket.get("deep_121_365d") or 0)
    terminal_candidate_total = int(counts_by_nav_bucket.get("terminal_gt_365d") or 0)
    unknown_nav_staleness_total = int(counts_by_nav_bucket.get("unknown") or 0)
    recoverable_total = fresh_recoverable_total + short_recoverable_total
    return {
        "nav_bucket_schema_version": STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION,
        "fresh_recoverable_total": fresh_recoverable_total,
        "short_recoverable_total": short_recoverable_total,
        "recoverable_total": recoverable_total,
        "medium_watch_total": medium_watch_total,
        "deep_watch_total": deep_watch_total,
        "terminal_candidate_total": terminal_candidate_total,
        "unknown_nav_staleness_total": unknown_nav_staleness_total,
        "recoverability_judgement": (
            "mixed_recoverability"
            if recoverable_total > 0
            else "explanation_only"
        ),
    }


def _classify_priority_scope_not_ready_stale_mode(sample: dict[str, Any]) -> str:
    stale_annual = bool(sample.get("stale_annual_returns"))
    stale_rank = bool(sample.get("stale_rank_history"))
    if stale_annual and stale_rank:
        return "annual_and_rank_stale"
    if stale_annual:
        return "annual_only_stale"
    if stale_rank:
        return "rank_only_stale"
    return "other_stale"


@dataclass(slots=True)
class LongTermHistoryMaintenanceContext:
    settings: Settings
    core_config: dict[str, Any]
    db_manager: Any
    api_client: APIClient
    annual_collector: AnnualReturnCollector
    rank_collector: RankCollector


def _load_core_config(project_root: Path) -> dict[str, Any]:
    return load_runtime_config(project_root / "config" / "project.yaml")


def _resolve_empty_payload_retry_days(settings: Any) -> int:
    try:
        return max(1, int(getattr(settings, "scheduler_long_term_history_empty_payload_retry_days", DEFAULT_EMPTY_PAYLOAD_RETRY_DAYS) or DEFAULT_EMPTY_PAYLOAD_RETRY_DAYS))
    except (TypeError, ValueError):
        return DEFAULT_EMPTY_PAYLOAD_RETRY_DAYS


def _resolve_empty_payload_upstream_limited_failures(settings: Any) -> int:
    try:
        return max(
            2,
            int(
                getattr(
                    settings,
                    "scheduler_long_term_history_empty_payload_upstream_limited_failures",
                    DEFAULT_EMPTY_PAYLOAD_UPSTREAM_LIMITED_FAILURES,
                )
                or DEFAULT_EMPTY_PAYLOAD_UPSTREAM_LIMITED_FAILURES
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_EMPTY_PAYLOAD_UPSTREAM_LIMITED_FAILURES


def _resolve_stale_refresh_upstream_limited_failures(settings: Any) -> int:
    try:
        return max(
            2,
            int(
                getattr(
                    settings,
                    "scheduler_long_term_history_stale_refresh_upstream_limited_failures",
                    DEFAULT_STALE_REFRESH_UPSTREAM_LIMITED_FAILURES,
                )
                or DEFAULT_STALE_REFRESH_UPSTREAM_LIMITED_FAILURES
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_STALE_REFRESH_UPSTREAM_LIMITED_FAILURES


def _resolve_rank_stale_days(settings: Any) -> int:
    try:
        return max(1, int(getattr(settings, "scheduler_long_term_history_rank_stale_days", 30) or 30))
    except (TypeError, ValueError):
        return 30


def _resolve_stale_refresh_retry_days(settings: Any) -> int:
    try:
        return max(1, int(getattr(settings, "scheduler_long_term_history_stale_refresh_retry_days", 28) or 28))
    except (TypeError, ValueError):
        return 28


def _resolve_high_priority_stale_backlog_trigger(settings: Any) -> int:
    try:
        return max(
            1,
            int(
                getattr(
                    settings,
                    "scheduler_long_term_history_high_priority_stale_backlog_trigger",
                    DEFAULT_HIGH_PRIORITY_STALE_BACKLOG_TRIGGER,
                )
                or DEFAULT_HIGH_PRIORITY_STALE_BACKLOG_TRIGGER
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_HIGH_PRIORITY_STALE_BACKLOG_TRIGGER


def _resolve_high_priority_stale_limit(settings: Any) -> int:
    try:
        return max(
            1,
            int(
                getattr(
                    settings,
                    "scheduler_long_term_history_high_priority_stale_limit",
                    DEFAULT_HIGH_PRIORITY_STALE_LIMIT,
                )
                or DEFAULT_HIGH_PRIORITY_STALE_LIMIT
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_HIGH_PRIORITY_STALE_LIMIT


def _table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.OperationalError:
        return False
    normalized_column = str(column_name or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == normalized_column for row in rows)


def _annual_feature_min_years() -> int:
    # 维护侧与报表侧统一按 5 个非空年度收益视为年度长期特征就绪。
    return 5


def _current_complete_calendar_years(*, min_years: int) -> list[int]:
    year_count = max(1, int(min_years or 0))
    last_complete_year = datetime.now().date().year - 1
    first_year = last_complete_year - year_count + 1
    return list(range(first_year, last_complete_year + 1))


def _extract_contiguous_recent_tail_years(
    target_years: list[int],
    present_years: list[int] | None,
) -> list[int]:
    normalized_target_years = [
        int(year)
        for year in (target_years or [])
        if str(year).strip().isdigit()
    ]
    if not normalized_target_years:
        return []
    present_set = {
        int(year)
        for year in (present_years or [])
        if str(year).strip().isdigit()
        and int(year) in normalized_target_years
    }
    if not present_set:
        return []
    tail_years: list[int] = []
    for year in reversed(normalized_target_years):
        if year not in present_set:
            break
        tail_years.append(year)
    return sorted(tail_years)


def _is_recent_share_class_tail_window(
    target_years: list[int],
    present_years: list[int] | None,
) -> bool:
    normalized_present_years = sorted(
        {
            int(year)
            for year in (present_years or [])
            if str(year).strip().isdigit()
            and int(year) in set(target_years or [])
        }
    )
    if not normalized_present_years:
        return False
    if len(normalized_present_years) >= len(target_years or []):
        return False
    tail_years = _extract_contiguous_recent_tail_years(target_years, normalized_present_years)
    return bool(tail_years) and tail_years == normalized_present_years


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
    return settings.scheduler_state_file.with_name("long_term_history_maintenance_state.json")


def _load_maintenance_state(settings: Settings) -> dict[str, Any]:
    path = _maintenance_state_path(settings)
    if not path.exists():
        return {"next_index": 0, "updated_at": None, "failure_backoff": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"next_index": 0, "updated_at": None, "failure_backoff": {}}
    if not isinstance(payload, dict):
        return {"next_index": 0, "updated_at": None, "failure_backoff": {}}
    return {
        "next_index": max(0, int(payload.get("next_index") or 0)),
        "updated_at": payload.get("updated_at"),
        "failure_backoff": _normalize_failure_backoff(payload.get("failure_backoff")),
    }


def _save_maintenance_state(
    settings: Settings,
    *,
    next_index: int,
    failure_backoff: dict[str, dict[str, Any]] | None = None,
) -> None:
    path = _maintenance_state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "next_index": max(0, int(next_index or 0)),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "failure_backoff": _normalize_failure_backoff(failure_backoff),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_context(settings: Settings) -> LongTermHistoryMaintenanceContext:
    core_config = _load_core_config(settings.project_root)
    core_config.setdefault("api", {})
    core_config["api"].setdefault("tiantian", {})
    core_config["api"]["tiantian"]["base_url"] = settings.tiantian_base_url
    core_config["api"]["base_url"] = settings.tiantian_base_url
    core_config.setdefault("database", {})
    core_config["database"]["path"] = str(settings.database_path)
    db_manager = get_db_manager(db_path=str(settings.database_path))
    api_client = APIClient(core_config)
    annual_collector = AnnualReturnCollector(config=core_config, db_manager=db_manager, api_client=api_client)
    rank_collector = RankCollector(config=core_config, db_manager=db_manager, api_client=api_client)
    return LongTermHistoryMaintenanceContext(
        settings=settings,
        core_config=core_config,
        db_manager=db_manager,
        api_client=api_client,
        annual_collector=annual_collector,
        rank_collector=rank_collector,
    )


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {
        str(row[1] or "").strip()
        for row in rows
        if len(row) > 1 and str(row[1] or "").strip()
    }


def _resolve_estab_age_days(raw_value: Any) -> int | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        estab_date = datetime.fromisoformat(text).date()
    except ValueError:
        return None
    return max(0, (datetime.now().date() - estab_date).days)


def _resolve_completed_calendar_years(raw_estab_date: Any) -> int | None:
    text = str(raw_estab_date or "").strip()
    if not text:
        return None
    try:
        estab_date = datetime.fromisoformat(text).date()
    except ValueError:
        return None
    last_complete_year = datetime.now().year - 1
    completed_years = max(0, last_complete_year - estab_date.year)
    if estab_date.month == 1 and estab_date.day == 1 and estab_date.year <= last_complete_year:
        completed_years += 1
    return completed_years


def _resolve_annual_feature_reference_date(
    raw_estab_date: Any,
    *,
    earliest_nav_date: Any = None,
) -> str | None:
    estab_text = str(raw_estab_date or "").strip()
    nav_text = str(earliest_nav_date or "").strip()
    estab_date = None
    nav_date = None
    if estab_text:
        try:
            estab_date = datetime.fromisoformat(estab_text).date()
        except ValueError:
            estab_date = None
    if nav_text:
        try:
            nav_date = datetime.fromisoformat(nav_text).date()
        except ValueError:
            nav_date = None
    if estab_date and nav_date:
        return max(estab_date, nav_date).isoformat()
    if estab_date:
        return estab_date.isoformat()
    if nav_date:
        return nav_date.isoformat()
    return None


def _history_mature_enough(raw_estab_date: Any, *, min_age_days: int = LONG_TERM_HISTORY_MIN_AGE_DAYS) -> bool:
    age_days = _resolve_estab_age_days(raw_estab_date)
    if age_days is None:
        return True
    return age_days >= max(0, int(min_age_days or 0))


def _annual_feature_window_mature_enough(
    raw_estab_date: Any,
    *,
    earliest_nav_date: Any = None,
    current_window_nav_present_years: list[int] | None = None,
    min_years: int = 5,
) -> bool:
    target_years = _current_complete_calendar_years(min_years=max(1, int(min_years or 0)))
    if _is_recent_share_class_tail_window(target_years, current_window_nav_present_years):
        return False
    reference_date = _resolve_annual_feature_reference_date(
        raw_estab_date,
        earliest_nav_date=earliest_nav_date,
    )
    completed_years = _resolve_completed_calendar_years(reference_date)
    if completed_years is None:
        return True
    return completed_years >= max(0, int(min_years or 0))


def _load_annual_history_window_stats(
    conn: sqlite3.Connection,
    *,
    codes: list[str],
    min_years: int | None = None,
) -> dict[str, dict[str, Any]]:
    normalized_codes = [code for code in (_normalize_code(code) for code in codes) if code]
    if not normalized_codes or not _table_has_column(conn, "fund_annual_returns", "year"):
        return {}

    target_years = _current_complete_calendar_years(min_years=max(1, int(min_years or _annual_feature_min_years())))
    placeholders = ",".join("?" for _ in normalized_codes)
    year_placeholders = ",".join("?" for _ in target_years)
    annual_has_return = _table_has_column(conn, "fund_annual_returns", "annual_return")
    if annual_has_return:
        rows = conn.execute(
            f"""
            SELECT
                fcode,
                COUNT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS row_count,
                COUNT(DISTINCT CASE WHEN year IN ({year_placeholders}) AND annual_return IS NOT NULL THEN year END) AS nonnull_count,
                GROUP_CONCAT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS present_years,
                GROUP_CONCAT(DISTINCT CASE WHEN year IN ({year_placeholders}) AND annual_return IS NOT NULL THEN year END) AS nonnull_years
            FROM fund_annual_returns
            WHERE fcode IN ({placeholders})
            GROUP BY fcode
            """,
            tuple(target_years) + tuple(target_years) + tuple(target_years) + tuple(target_years) + tuple(normalized_codes),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT
                fcode,
                COUNT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS row_count,
                COUNT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS nonnull_count,
                GROUP_CONCAT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS present_years,
                GROUP_CONCAT(DISTINCT CASE WHEN year IN ({year_placeholders}) THEN year END) AS nonnull_years
            FROM fund_annual_returns
            WHERE fcode IN ({placeholders})
            GROUP BY fcode
            """,
            tuple(target_years) + tuple(target_years) + tuple(target_years) + tuple(target_years) + tuple(normalized_codes),
        ).fetchall()

    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        present_years = sorted(
            {
                int(part)
                for part in str(row[3] or "").split(",")
                if str(part or "").strip().isdigit()
            }
        )
        nonnull_years = sorted(
            {
                int(part)
                for part in str(row[4] or "").split(",")
                if str(part or "").strip().isdigit()
            }
        )
        stats[code] = {
            "window_years": list(target_years),
            "row_count": int(row[1] or 0),
            "nonnull_count": int(row[2] or 0),
            "present_years": present_years,
            "nonnull_years": nonnull_years,
        }
    return stats


def _load_nav_history_window_present_years(
    conn: sqlite3.Connection,
    *,
    codes: list[str],
    min_years: int | None = None,
    archive_attach: dict | None = None,
) -> dict[str, list[int]]:
    normalized_codes = [code for code in (_normalize_code(code) for code in codes) if code]
    if not normalized_codes:
        return {}

    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'fund_nav_history'
        """
    ).fetchone()
    if not table_exists:
        return {}

    target_years = _current_complete_calendar_years(min_years=max(1, int(min_years or _annual_feature_min_years())))
    if not target_years:
        return {}
    start_date = f"{target_years[0]:04d}-01-01"
    end_date = f"{target_years[-1]:04d}-12-31"
    nav_source_sql = build_nav_history_union_source_sql(
        archive_attach,
        columns=("fcode", "nav_date"),
    )

    stats: dict[str, set[int]] = {}
    chunk_size = 500
    for start in range(0, len(normalized_codes), chunk_size):
        code_batch = normalized_codes[start:start + chunk_size]
        placeholders = ",".join("?" for _ in code_batch)
        rows = conn.execute(
            f"""
            SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year
            FROM ({nav_source_sql}) AS nav_history
            WHERE fcode IN ({placeholders})
              AND nav_date BETWEEN ? AND ?
            GROUP BY fcode, substr(nav_date, 1, 4)
            """,
            tuple(code_batch) + (start_date, end_date),
        ).fetchall()
        for row in rows:
            code = _normalize_code(row[0])
            year = row[1]
            if not code or year is None:
                continue
            try:
                normalized_year = int(year)
            except (TypeError, ValueError):
                continue
            if normalized_year not in target_years:
                continue
            stats.setdefault(code, set()).add(normalized_year)
    return {
        code: sorted(years)
        for code, years in stats.items()
    }


def _load_nav_history_window_first_nav_dates_by_year(
    conn: sqlite3.Connection,
    *,
    codes: list[str],
    min_years: int | None = None,
    archive_attach: dict | None = None,
) -> dict[str, dict[int, str]]:
    normalized_codes = [code for code in (_normalize_code(code) for code in codes) if code]
    if not normalized_codes:
        return {}

    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'fund_nav_history'
        """
    ).fetchone()
    if not table_exists:
        return {}

    target_years = _current_complete_calendar_years(min_years=max(1, int(min_years or _annual_feature_min_years())))
    if not target_years:
        return {}
    start_date = f"{target_years[0]:04d}-01-01"
    end_date = f"{target_years[-1]:04d}-12-31"
    nav_source_sql = build_nav_history_union_source_sql(
        archive_attach,
        columns=("fcode", "nav_date"),
    )

    first_nav_dates_by_code: dict[str, dict[int, str]] = {}
    chunk_size = 500
    for start in range(0, len(normalized_codes), chunk_size):
        code_batch = normalized_codes[start:start + chunk_size]
        placeholders = ",".join("?" for _ in code_batch)
        rows = conn.execute(
            f"""
            SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year, MIN(nav_date) AS first_nav_date
            FROM ({nav_source_sql}) AS nav_history
            WHERE fcode IN ({placeholders})
              AND nav_date BETWEEN ? AND ?
            GROUP BY fcode, substr(nav_date, 1, 4)
            """,
            tuple(code_batch) + (start_date, end_date),
        ).fetchall()
        for row in rows:
            code = _normalize_code(row[0])
            year = row[1]
            first_nav_date = str(row[2] or "").strip()
            if not code or year is None or not first_nav_date:
                continue
            try:
                normalized_year = int(year)
            except (TypeError, ValueError):
                continue
            if normalized_year not in target_years:
                continue
            first_nav_dates_by_code.setdefault(code, {})[normalized_year] = first_nav_date
    return first_nav_dates_by_code


def _annual_feature_window_has_leading_partial_null_year(
    target_years: list[int],
    present_years: list[int] | None,
    nonnull_years: list[int] | None,
    *,
    leading_present_year_first_nav_date: Any = None,
) -> bool:
    normalized_target_years = [
        int(year)
        for year in (target_years or [])
        if str(year).strip().isdigit()
    ]
    if not normalized_target_years:
        return False

    target_year_set = set(normalized_target_years)
    normalized_present_years = sorted(
        {
            int(year)
            for year in (present_years or [])
            if str(year).strip().isdigit()
            and int(year) in target_year_set
        }
    )
    normalized_nonnull_years = {
        int(year)
        for year in (nonnull_years or [])
        if str(year).strip().isdigit()
        and int(year) in target_year_set
    }
    if not normalized_present_years:
        return False

    leading_present_year = normalized_present_years[0]
    if leading_present_year != normalized_target_years[0]:
        return False
    if leading_present_year in normalized_nonnull_years:
        return False

    nav_text = str(leading_present_year_first_nav_date or "").strip()
    if not nav_text:
        return False
    try:
        nav_dt = datetime.fromisoformat(nav_text[:10]).date()
    except ValueError:
        return False
    return nav_dt.year == leading_present_year and (nav_dt.month > 1 or nav_dt.day > 1)


def _annual_feature_window_has_semantic_null_gap(row: dict[str, Any]) -> bool:
    window_row_count = int(row.get("annual_feature_window_row_count") or 0)
    window_nonnull_count = int(row.get("annual_feature_window_nonnull_count") or 0)
    target_year_total = len(row.get("annual_feature_window_years") or [])
    return bool(
        row.get("needs_annual_returns")
        and row.get("annual_feature_window_mature_enough")
        and target_year_total > 0
        and window_row_count >= target_year_total
        and window_nonnull_count < target_year_total
        and window_nonnull_count > 0
    )


def _load_earliest_nav_dates(
    conn: sqlite3.Connection,
    *,
    codes: list[str],
    archive_attach: dict | None = None,
) -> dict[str, str]:
    normalized_codes = [code for code in (_normalize_code(code) for code in codes) if code]
    if not normalized_codes:
        return {}
    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'fund_nav_history'
        """
    ).fetchone()
    if not table_exists:
        return {}
    nav_source_sql = build_nav_history_union_source_sql(
        archive_attach,
        columns=("fcode", "nav_date"),
    )

    earliest_nav_dates: dict[str, str] = {}
    chunk_size = 500
    for start in range(0, len(normalized_codes), chunk_size):
        code_batch = normalized_codes[start:start + chunk_size]
        placeholders = ",".join("?" for _ in code_batch)
        rows = conn.execute(
            f"""
            SELECT fcode, MIN(nav_date) AS earliest_nav_date
            FROM ({nav_source_sql}) AS nav_history
            WHERE fcode IN ({placeholders})
            GROUP BY fcode
            """,
            tuple(code_batch),
        ).fetchall()
        for row in rows:
            code = _normalize_code(row[0])
            earliest_nav_date = str(row[1] or "").strip()
            if code and earliest_nav_date:
                earliest_nav_dates[code] = earliest_nav_date
    return earliest_nav_dates


def _resolve_history_maturity(
    raw_estab_date: Any,
    *,
    latest_nav_known: bool,
    first_collected_at: Any = None,
    earliest_nav_date: Any = None,
    min_age_days: int = LONG_TERM_HISTORY_MIN_AGE_DAYS,
) -> tuple[bool, str]:
    threshold_days = max(0, int(min_age_days or 0))
    earliest_nav_age_days = _resolve_estab_age_days(earliest_nav_date)
    estab_age_days = _resolve_estab_age_days(raw_estab_date)
    if estab_age_days is not None:
        if estab_age_days >= threshold_days:
            # 对后续新增份额/新 share class 而言，基金成立日可能沿用母基金历史，
            # 但本份额真实可用净值窗口仍明显不足 1 年。此时若继续按 estab_date
            # 放行，会把样本错误送入 annual/rank 长周期补采。
            if earliest_nav_age_days is not None and earliest_nav_age_days < threshold_days:
                return False, "nav_history_lower_bound_pending"
            return True, "estab_date_mature"
        return False, "estab_date_pending"

    if earliest_nav_age_days is not None and earliest_nav_age_days >= threshold_days:
        return True, "nav_history_lower_bound_mature"

    if not latest_nav_known:
        first_collected_age_days = _resolve_estab_age_days(first_collected_at)
        if (
            first_collected_age_days is not None
            and first_collected_age_days < RECENT_DISCOVERY_WITHOUT_NAV_GRACE_DAYS
        ):
            return False, "recent_discovery_waiting_initial_nav"
        return False, "metadata_missing_no_latest_nav"

    if earliest_nav_age_days is not None:
        return False, "nav_history_lower_bound_pending"
    return True, "metadata_unknown_latest_nav_known"


def _resolve_nav_staleness_days(latest_nav_date: Any) -> int | None:
    text = str(latest_nav_date or "").strip()
    if not text:
        return None
    try:
        nav_date = datetime.fromisoformat(text).date()
    except ValueError:
        return None
    return max(0, (datetime.now().date() - nav_date).days)


def _has_reliable_accum_nav_window(nav_df: pd.DataFrame, *, regression_tolerance: float = 0.01) -> bool:
    if nav_df is None or nav_df.empty or "accum_nav" not in nav_df.columns:
        return False

    accum = pd.to_numeric(nav_df["accum_nav"], errors="coerce")
    if accum.notna().sum() < 2:
        return False
    if (accum.dropna() <= 0).any():
        return False

    if "unit_nav" not in nav_df.columns:
        return False
    unit = pd.to_numeric(nav_df["unit_nav"], errors="coerce")
    scale_df = pd.DataFrame({"unit_nav": unit, "accum_nav": accum}).dropna()
    if scale_df.empty:
        return False
    scale_df = scale_df[(scale_df["unit_nav"] > 0) & (scale_df["accum_nav"] > 0)]
    if scale_df.empty:
        return False

    ratio_series = scale_df["unit_nav"] / scale_df["accum_nav"]
    ratio_series = ratio_series.replace([float("inf"), float("-inf")], pd.NA).dropna()
    if ratio_series.empty:
        return False
    median_ratio = float(ratio_series.median())
    if median_ratio <= 0:
        return False
    max_ratio_dev = float((ratio_series - median_ratio).abs().max())
    if max_ratio_dev > 0.05:
        return False

    gap_series = (accum - unit).dropna()
    if len(gap_series) < 2:
        return True
    gap_diff = gap_series.diff().dropna()
    if gap_diff.empty:
        return True
    max_regression = float((-gap_diff.clip(upper=0)).max())
    return bool(max_regression <= regression_tolerance)


def _compute_calendar_year_return_from_nav(nav_df: pd.DataFrame) -> float | None:
    if nav_df is None or nav_df.empty or len(nav_df) < 2:
        return None

    period_df = nav_df.sort_values("nav_date").reset_index(drop=True)
    if len(period_df) < LOCAL_ANNUAL_RETURN_BACKFILL_MIN_POINT_COUNT:
        return None

    first_date = period_df.iloc[0]["nav_date"]
    last_date = period_df.iloc[-1]["nav_date"]
    if not isinstance(first_date, pd.Timestamp):
        first_date = pd.Timestamp(first_date)
    if not isinstance(last_date, pd.Timestamp):
        last_date = pd.Timestamp(last_date)

    if int(first_date.dayofyear) > LOCAL_ANNUAL_RETURN_BACKFILL_START_DAY_MAX:
        return None
    if int(last_date.dayofyear) < LOCAL_ANNUAL_RETURN_BACKFILL_END_DAY_MIN:
        return None

    if "daily_growth" in period_df.columns:
        dg = pd.to_numeric(period_df["daily_growth"], errors="coerce").iloc[1:].dropna()
        if len(dg) >= max(2, len(period_df) // 3):
            cumulative = float((1 + dg / 100.0).prod() - 1.0)
            return round(cumulative * 100.0, 4)

    if "accum_nav" in period_df.columns and _has_reliable_accum_nav_window(period_df):
        start_accum = pd.to_numeric(pd.Series([period_df.iloc[0].get("accum_nav")]), errors="coerce").iloc[0]
        end_accum = pd.to_numeric(pd.Series([period_df.iloc[-1].get("accum_nav")]), errors="coerce").iloc[0]
        if pd.notna(start_accum) and pd.notna(end_accum) and float(start_accum) > 0:
            return round(((float(end_accum) / float(start_accum)) - 1.0) * 100.0, 4)

    start_nav = pd.to_numeric(pd.Series([period_df.iloc[0].get("unit_nav")]), errors="coerce").iloc[0]
    end_nav = pd.to_numeric(pd.Series([period_df.iloc[-1].get("unit_nav")]), errors="coerce").iloc[0]
    if pd.isna(start_nav) or pd.isna(end_nav) or float(start_nav) <= 0:
        return None
    return round(((float(end_nav) / float(start_nav)) - 1.0) * 100.0, 4)


def _load_nav_rows_for_calendar_year(
    conn: sqlite3.Connection,
    *,
    code: str,
    year: int,
    accum_select: str,
    daily_growth_select: str,
) -> pd.DataFrame:
    nav_rows = conn.execute(
        f"""
        SELECT nav_date, unit_nav, {accum_select}, {daily_growth_select}
        FROM fund_nav_history
        WHERE fcode = ?
          AND nav_date BETWEEN ? AND ?
        ORDER BY nav_date
        """,
        (code, f"{year:04d}-01-01", f"{year:04d}-12-31"),
    ).fetchall()
    if not nav_rows:
        return pd.DataFrame()

    nav_df = pd.DataFrame([dict(item) for item in nav_rows])
    nav_df["nav_date"] = pd.to_datetime(nav_df["nav_date"], errors="coerce")
    return nav_df.dropna(subset=["nav_date"]).sort_values("nav_date").reset_index(drop=True)


def _describe_backfill_nav_coverage(nav_df: pd.DataFrame) -> dict[str, Any]:
    if nav_df is None or nav_df.empty:
        return {
            "nav_point_count": 0,
            "first_nav_date": None,
            "last_nav_date": None,
        }
    return {
        "nav_point_count": int(len(nav_df)),
        "first_nav_date": str(nav_df.iloc[0]["nav_date"].date()),
        "last_nav_date": str(nav_df.iloc[-1]["nav_date"].date()),
    }


def _backfill_semantic_null_annual_returns_from_nav(
    settings: Settings,
    *,
    eligible_codes: set[str] | list[str] | None = None,
    target_codes: list[str] | None = None,
    eligible_missing_window_years_by_code: dict[str, list[int]] | None = None,
    log=lambda _message: None,
) -> dict[str, Any]:
    eligible_code_set = {
        _normalize_code(code)
        for code in (eligible_codes or [])
        if _normalize_code(code)
    }
    normalized_targets = {
        _normalize_code(code)
        for code in (target_codes or [])
        if _normalize_code(code)
    }
    conn = sqlite3.connect(str(settings.database_path))
    conn.row_factory = sqlite3.Row
    try:
        annual_columns = _table_columns(conn, "fund_annual_returns")
        nav_columns = _table_columns(conn, "fund_nav_history")
        if not {"fcode", "year", "annual_return"}.issubset(annual_columns):
            return {
                "scanned_null_year_rows": 0,
                "eligible_null_year_rows": 0,
                "scanned_missing_window_year_rows": 0,
                "eligible_missing_window_year_rows": 0,
                "backfilled_missing_window_year_row_count": 0,
                "backfilled_row_count": 0,
                "backfilled_codes": [],
                "sample_rows": [],
                "skipped_rows": [],
            }
        if "fcode" not in nav_columns or "nav_date" not in nav_columns or "unit_nav" not in nav_columns:
            return {
                "scanned_null_year_rows": 0,
                "eligible_null_year_rows": 0,
                "scanned_missing_window_year_rows": 0,
                "eligible_missing_window_year_rows": 0,
                "backfilled_missing_window_year_row_count": 0,
                "backfilled_row_count": 0,
                "backfilled_codes": [],
                "sample_rows": [],
                "skipped_rows": [],
            }

        null_year_rows = conn.execute(
            """
            SELECT ar.fcode, ar.year, COALESCE(f.shortname, '') AS shortname
            FROM fund_annual_returns ar
            LEFT JOIN funds f ON f.fcode = ar.fcode
            WHERE ar.annual_return IS NULL
            ORDER BY ar.fcode, ar.year
            """
        ).fetchall()
        scanned_rows = 0
        eligible_rows = 0
        scanned_missing_window_year_rows = 0
        eligible_missing_window_year_rows = 0
        backfilled_rows: list[dict[str, Any]] = []
        skipped_rows: list[dict[str, Any]] = []
        accum_select = "accum_nav" if "accum_nav" in nav_columns else "NULL AS accum_nav"
        daily_growth_select = "daily_growth" if "daily_growth" in nav_columns else "NULL AS daily_growth"

        for row in null_year_rows:
            code = _normalize_code(row["fcode"])
            if not code:
                continue
            if eligible_code_set and code not in eligible_code_set:
                continue
            if normalized_targets and code not in normalized_targets:
                continue
            scanned_rows += 1
            year = int(row["year"])
            nav_df = _load_nav_rows_for_calendar_year(
                conn,
                code=code,
                year=year,
                accum_select=accum_select,
                daily_growth_select=daily_growth_select,
            )
            if nav_df.empty:
                skipped_rows.append(
                    {
                        "fcode": code,
                        "year": year,
                        "shortname": row["shortname"],
                        "reason": "no_nav_rows",
                    }
                )
                continue

            annual_return = _compute_calendar_year_return_from_nav(nav_df)
            if annual_return is None:
                skipped_rows.append(
                    {
                        "fcode": code,
                        "year": year,
                        "shortname": row["shortname"],
                        "reason": "insufficient_calendar_year_nav_coverage",
                        **_describe_backfill_nav_coverage(nav_df),
                    }
                )
                continue

            eligible_rows += 1
            conn.execute(
                """
                UPDATE fund_annual_returns
                SET annual_return = ?
                WHERE fcode = ? AND year = ? AND annual_return IS NULL
                """,
                (annual_return, code, year),
            )
            backfilled_rows.append(
                {
                    "fcode": code,
                    "year": year,
                    "shortname": row["shortname"],
                    "backfill_mode": "update_null_row",
                    "annual_return": annual_return,
                    **_describe_backfill_nav_coverage(nav_df),
                }
            )

        missing_window_years = {
            _normalize_code(code): sorted({int(year) for year in (years or [])})
            for code, years in (eligible_missing_window_years_by_code or {}).items()
            if _normalize_code(code)
        }
        for code, years in sorted(missing_window_years.items()):
            if eligible_code_set and code not in eligible_code_set:
                continue
            if normalized_targets and code not in normalized_targets:
                continue
            shortname_row = conn.execute(
                "SELECT COALESCE(shortname, '') FROM funds WHERE fcode = ?",
                (code,),
            ).fetchone()
            shortname = str(shortname_row[0] or "").strip() if shortname_row else ""
            for year in years:
                scanned_missing_window_year_rows += 1
                nav_df = _load_nav_rows_for_calendar_year(
                    conn,
                    code=code,
                    year=year,
                    accum_select=accum_select,
                    daily_growth_select=daily_growth_select,
                )
                if nav_df.empty:
                    skipped_rows.append(
                        {
                            "fcode": code,
                            "year": year,
                            "shortname": shortname,
                            "reason": "no_nav_rows_for_missing_window_year",
                        }
                    )
                    continue
                annual_return = _compute_calendar_year_return_from_nav(nav_df)
                if annual_return is None:
                    skipped_rows.append(
                        {
                            "fcode": code,
                            "year": year,
                            "shortname": shortname,
                            "reason": "insufficient_calendar_year_nav_coverage_for_missing_window_year",
                            **_describe_backfill_nav_coverage(nav_df),
                        }
                    )
                    continue

                eligible_missing_window_year_rows += 1
                conn.execute(
                    """
                    INSERT INTO fund_annual_returns (fcode, year, annual_return)
                    VALUES (?, ?, ?)
                    """,
                    (code, year, annual_return),
                )
                backfilled_rows.append(
                    {
                        "fcode": code,
                        "year": year,
                        "shortname": shortname,
                        "backfill_mode": "insert_missing_window_year_row",
                        "annual_return": annual_return,
                        **_describe_backfill_nav_coverage(nav_df),
                    }
                )

        if backfilled_rows:
            conn.commit()
            for item in backfilled_rows[:AUDIT_SAMPLE_LIMIT]:
                log(
                    "本地NAV补算年度涨幅成功: "
                    f"{item['fcode']} {item['year']} return={item['annual_return']} mode={item.get('backfill_mode')}"
                )

        return {
            "scanned_null_year_rows": int(scanned_rows),
            "eligible_null_year_rows": int(eligible_rows),
            "scanned_missing_window_year_rows": int(scanned_missing_window_year_rows),
            "eligible_missing_window_year_rows": int(eligible_missing_window_year_rows),
            "backfilled_missing_window_year_row_count": int(
                sum(1 for item in backfilled_rows if item.get("backfill_mode") == "insert_missing_window_year_row")
            ),
            "backfilled_row_count": int(len(backfilled_rows)),
            "backfilled_codes": sorted({item["fcode"] for item in backfilled_rows}),
            "sample_rows": backfilled_rows[:AUDIT_SAMPLE_LIMIT],
            "skipped_rows": skipped_rows[:AUDIT_SAMPLE_LIMIT],
        }
    finally:
        conn.close()


def _normalize_failure_backoff(payload: Any) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    if not isinstance(payload, dict):
        return normalized

    for raw_code, raw_record in payload.items():
        code = _normalize_code(raw_code)
        if not code or not isinstance(raw_record, dict):
            continue
        stages_raw = raw_record.get("last_failed_stages")
        if not isinstance(stages_raw, list):
            stages_raw = [raw_record.get("last_stage")] if raw_record.get("last_stage") else []
        stages = sorted({str(stage).strip() for stage in stages_raw if str(stage).strip()})
        normalized[code] = {
            "consecutive_failures": max(0, int(raw_record.get("consecutive_failures") or 0)),
            "skip_runs_remaining": max(0, int(raw_record.get("skip_runs_remaining") or 0)),
            "last_failed_at": str(raw_record.get("last_failed_at") or "").strip() or None,
            "next_retry_after": str(raw_record.get("next_retry_after") or "").strip() or None,
            "last_failed_stages": stages,
            "last_error": str(raw_record.get("last_error") or "").strip(),
            "upstream_limited": bool(raw_record.get("upstream_limited")),
        }
    return normalized


def _cooldown_runs_for_failure_count(consecutive_failures: int) -> int:
    return min(MAX_FAILURE_COOLDOWN_RUNS, max(1, int(consecutive_failures or 0)))


def _is_stable_empty_payload_failure(failed_stages: list[str], failed_errors: list[str]) -> bool:
    normalized_stages = {str(stage or "").strip() for stage in failed_stages if str(stage or "").strip()}
    normalized_errors = {str(error or "").strip() for error in failed_errors if str(error or "").strip()}
    return (
        bool(normalized_errors)
        and normalized_errors.issubset({"empty_period_records", "empty_rank_records"})
        and bool(normalized_stages)
        and normalized_stages.issubset({"annual_returns", "rank_history"})
    )


def _all_failures_map_to_governance_action(
    failed_errors: list[str],
    governance_action: str,
) -> bool:
    normalized_action = str(governance_action or "").strip()
    normalized_errors = [_normalize_failure_reason(error) for error in failed_errors if str(error or "").strip()]
    if not normalized_action or not normalized_errors:
        return False
    return all(_classify_failure_reason_governance_action(error) == normalized_action for error in normalized_errors)


def _cooldown_runs_for_failure_detail(
    consecutive_failures: int,
    failed_stages: list[str],
    failed_errors: list[str],
) -> int:
    if _is_stable_empty_payload_failure(failed_stages, failed_errors):
        return 0
    if any(error in {"stale_annual_refresh_no_progress", "stale_rank_refresh_no_progress"} for error in failed_errors):
        return 0
    if _all_failures_map_to_governance_action(failed_errors, "observe_upstream_source"):
        return 0
    return _cooldown_runs_for_failure_count(consecutive_failures)


def _is_stale_refresh_no_progress_failure(failed_errors: list[str]) -> bool:
    return any(error in {"stale_annual_refresh_no_progress", "stale_rank_refresh_no_progress"} for error in failed_errors)


def _resolve_next_retry_after(
    failed_at: str,
    *,
    failed_stages: list[str],
    failed_errors: list[str],
    empty_payload_retry_days: int,
    stale_refresh_retry_days: int | None = None,
    consecutive_failures: int = 1,
) -> str | None:
    try:
        failed_ts = datetime.fromisoformat(str(failed_at or "").strip())
    except ValueError:
        return None
    if _is_stable_empty_payload_failure(failed_stages, failed_errors):
        retry_days = max(1, int(empty_payload_retry_days or 0))
    elif any(error in {"stale_annual_refresh_no_progress", "stale_rank_refresh_no_progress"} for error in failed_errors):
        retry_days = max(1, int(stale_refresh_retry_days or 0))
    elif _all_failures_map_to_governance_action(failed_errors, "observe_upstream_source"):
        retry_days = max(1, int(empty_payload_retry_days or 0))
    else:
        return None
    retry_days *= max(1, int(consecutive_failures or 1))
    return (failed_ts.replace(microsecond=0) + timedelta(days=retry_days)).isoformat(timespec="seconds")


def _partition_candidates_by_failure_backoff(
    candidates: list[dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]],
    *,
    consume_skip: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    eligible_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    updated_backoff: dict[str, dict[str, Any]] = {}
    now_ts = datetime.now()

    for row in candidates:
        code = _normalize_code(row.get("fcode"))
        record = dict(failure_backoff.get(code) or {})
        if not code:
            continue
        if not record:
            eligible_candidates.append(row)
            continue

        next_retry_after_text = str(record.get("next_retry_after") or "").strip()
        next_retry_active = False
        next_retry_after = None
        if next_retry_after_text:
            try:
                next_retry_after = datetime.fromisoformat(next_retry_after_text)
            except ValueError:
                next_retry_after = None
            next_retry_active = bool(next_retry_after and now_ts < next_retry_after)

        skip_runs_before = max(0, int(record.get("skip_runs_remaining") or 0))
        if next_retry_active or skip_runs_before > 0:
            skip_runs_after = max(0, skip_runs_before - 1) if consume_skip else skip_runs_before
            record["skip_runs_remaining"] = skip_runs_after
            if next_retry_after and now_ts >= next_retry_after:
                record["next_retry_after"] = None
            updated_backoff[code] = record
            skipped_candidates.append(
                {
                    **row,
                    "cooldown_runs_remaining_before": skip_runs_before,
                    "cooldown_runs_remaining_after": skip_runs_after,
                    "cooldown_next_retry_after": next_retry_after_text or None,
                    "cooldown_next_retry_window_active": bool(next_retry_active),
                    "cooldown_last_failed_at": record.get("last_failed_at"),
                    "cooldown_last_failed_stages": list(record.get("last_failed_stages") or []),
                    "cooldown_last_error": str(record.get("last_error") or "").strip(),
                    "cooldown_upstream_limited": bool(record.get("upstream_limited")),
                }
            )
            continue

        if record.get("next_retry_after"):
            record["next_retry_after"] = None
        updated_backoff[code] = record
        eligible_candidates.append(row)

    return eligible_candidates, skipped_candidates, updated_backoff


def _has_active_retry_window(failure_record: dict[str, Any] | None, *, now_ts: datetime | None = None) -> bool:
    record = dict(failure_record or {})
    next_retry_after_text = str(record.get("next_retry_after") or "").strip()
    if not next_retry_after_text:
        return False
    try:
        next_retry_after = datetime.fromisoformat(next_retry_after_text)
    except ValueError:
        return False
    return bool((now_ts or datetime.now()) < next_retry_after)


def _prune_failure_backoff(
    failure_backoff: dict[str, dict[str, Any]],
    *,
    candidate_codes: set[str],
) -> dict[str, dict[str, Any]]:
    return {
        code: dict(record)
        for code, record in _normalize_failure_backoff(failure_backoff).items()
        if code in candidate_codes
    }


def _merge_targeted_failure_backoff_update(
    failure_backoff: dict[str, dict[str, Any]],
    *,
    target_codes: set[str],
    selected_candidates: list[dict[str, Any]],
    failed_items: list[dict[str, str]],
    candidate_codes_after: set[str],
    failed_at: str,
    empty_payload_retry_days: int,
    stale_refresh_retry_days: int,
    empty_payload_upstream_limited_failures: int,
    stale_refresh_upstream_limited_failures: int,
) -> dict[str, dict[str, Any]]:
    normalized_failure_backoff = _normalize_failure_backoff(failure_backoff)
    normalized_target_codes = {
        _normalize_code(code)
        for code in target_codes
        if _normalize_code(code)
    }
    if not normalized_target_codes:
        return normalized_failure_backoff

    targeted_before = {
        code: dict(record)
        for code, record in normalized_failure_backoff.items()
        if code in normalized_target_codes
    }
    targeted_after = _update_failure_backoff_after_run(
        targeted_before,
        selected_candidates=selected_candidates,
        failed_items=failed_items,
        candidate_codes_after={
            code
            for code in candidate_codes_after
            if code in normalized_target_codes
        },
        failed_at=failed_at,
        empty_payload_retry_days=empty_payload_retry_days,
        stale_refresh_retry_days=stale_refresh_retry_days,
        empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
        stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
    )

    merged = {
        code: dict(record)
        for code, record in normalized_failure_backoff.items()
        if code not in normalized_target_codes
    }
    merged.update(targeted_after)
    return merged


def _summarize_maintenance_state(state: dict[str, Any]) -> dict[str, Any]:
    failure_backoff = _normalize_failure_backoff(state.get("failure_backoff"))
    return {
        "next_index": max(0, int(state.get("next_index") or 0)),
        "updated_at": state.get("updated_at"),
        "failure_backoff_total": len(failure_backoff),
        "failure_backoff_samples": [
            {"fcode": code, **record}
            for code, record in sorted(failure_backoff.items())[:AUDIT_SAMPLE_LIMIT]
        ],
    }


def _group_failed_items_by_code(failed_items: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for item in failed_items:
        code = _normalize_code(item.get("fcode"))
        if not code:
            continue
        grouped.setdefault(code, []).append(item)
    return grouped


def _reconcile_stale_refresh_progress(
    *,
    selected_candidates: list[dict[str, Any]],
    annual_success_codes: list[str],
    rank_success_codes: list[str],
    failed_items: list[dict[str, str]],
    post_universe_map: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    annual_success_set = set(annual_success_codes)
    rank_success_set = set(rank_success_codes)
    extra_failed_items: list[dict[str, str]] = []

    for row in selected_candidates:
        code = _normalize_code(row.get("fcode"))
        if not code:
            continue
        post_row = post_universe_map.get(code) or {}

        if code in annual_success_set:
            if bool(row.get("needs_annual_returns")) and bool(post_row.get("needs_annual_returns")):
                annual_success_set.discard(code)
                extra_failed_items.append(
                    {"fcode": code, "stage": "annual_returns", "error": "annual_history_gap_no_progress"}
                )
            elif bool(row.get("stale_annual_returns")):
                before_year = row.get("latest_annual_year")
                after_year = post_row.get("latest_annual_year")
                if after_year is None or (before_year is not None and int(after_year) <= int(before_year)):
                    annual_success_set.discard(code)
                    extra_failed_items.append(
                        {"fcode": code, "stage": "annual_returns", "error": "stale_annual_refresh_no_progress"}
                    )

        if code in rank_success_set:
            if bool(row.get("needs_rank_history")) and bool(post_row.get("needs_rank_history")):
                rank_success_set.discard(code)
                extra_failed_items.append(
                    {"fcode": code, "stage": "rank_history", "error": "rank_history_gap_no_progress"}
                )
            elif bool(row.get("stale_rank_history")):
                before_rank_date = str(row.get("latest_rank_date") or "").strip()
                after_rank_date = str(post_row.get("latest_rank_date") or "").strip()
                if not after_rank_date or (before_rank_date and after_rank_date <= before_rank_date):
                    rank_success_set.discard(code)
                    extra_failed_items.append(
                        {"fcode": code, "stage": "rank_history", "error": "stale_rank_refresh_no_progress"}
                    )

    if extra_failed_items:
        failed_items = failed_items + extra_failed_items

    return sorted(annual_success_set), sorted(rank_success_set), failed_items


def _split_failure_error_tokens(last_error: Any) -> list[str]:
    return [
        token
        for token in (str(part or "").strip() for part in str(last_error or "").split("|"))
        if token
    ]


def _build_auto_scope_sample(
    row: dict[str, Any],
    *,
    source_category: str,
    blocker_category: str,
    action_category: str,
    failure_record: dict[str, Any],
) -> dict[str, Any]:
    return {
        "fcode": _normalize_code(row.get("fcode")),
        "shortname": row.get("shortname"),
        "priority_label": str(row.get("priority_label") or "").strip() or "unknown",
        "publishable_priority": bool(row.get("publishable_priority")),
        "screenable_universe": bool(row.get("screenable_universe")),
        "buyable": normalize_buyability(row.get("buyable")),
        "active_universe": bool(row.get("active_universe")),
        "fresh_nav": bool(row.get("fresh_nav")),
        "risk_ready": bool(row.get("risk_ready")),
        "holding_period_blocked": bool(row.get("holding_period_blocked")),
        "source_category": source_category,
        "blocker_category": blocker_category,
        "recommended_action": action_category,
        "cooldown_consecutive_failures": int(failure_record.get("consecutive_failures") or 0),
        "cooldown_next_retry_after": str(failure_record.get("next_retry_after") or "").strip() or None,
        "cooldown_upstream_limited": bool(failure_record.get("upstream_limited")),
        "cooldown_last_error": str(failure_record.get("last_error") or "").strip() or None,
        "latest_nav_date": str(row.get("latest_nav_date") or "").strip() or None,
        "nav_staleness_days": _resolve_nav_staleness_days(row.get("latest_nav_date")),
        "stale_annual_returns": bool(row.get("stale_annual_returns")),
        "stale_rank_history": bool(row.get("stale_rank_history")),
    }


def _build_priority_blocked_summary(
    universe: list[dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = list(universe or [])
    normalized_failure_backoff = _normalize_failure_backoff(failure_backoff or {})
    samples_by_bucket: dict[str, list[dict[str, Any]]] = {}
    counts_by_bucket: dict[str, int] = {}
    counts_by_priority: dict[str, int] = {}
    source_counts_by_bucket: dict[str, dict[str, int]] = {}
    stale_scope_counts_by_nav_bucket: dict[str, int] = {}
    stale_scope_counts_by_stale_mode: dict[str, int] = {}
    stale_scope_counts_by_priority: dict[str, int] = {}
    stale_scope_terminal_candidate_total = 0
    stale_scope_deep_stale_candidate_total = 0

    def add_bucket_sample(bucket: str, sample: dict[str, Any]) -> None:
        counts_by_bucket[bucket] = int(counts_by_bucket.get(bucket) or 0) + 1
        source_category = str(sample.get("source_category") or "").strip() or "unknown"
        source_counts = source_counts_by_bucket.setdefault(bucket, {})
        source_counts[source_category] = int(source_counts.get(source_category) or 0) + 1
        samples = samples_by_bucket.setdefault(bucket, [])
        sample_limit = AUDIT_SAMPLE_LIMIT if bucket.endswith("scope_not_ready") else AUTO_SCOPE_SAMPLE_LIMIT
        if len(samples) < sample_limit:
            samples.append(sample)

    for row in rows:
        if not _row_has_long_term_history_gap(row):
            continue
        code = _normalize_code(row.get("fcode"))
        failure_record = dict(normalized_failure_backoff.get(code) or {})
        source_category = _classify_long_term_gap_source(row, failure_record)
        blocker_category = _classify_long_term_gap_blocker(row)
        action_category = _classify_long_term_gap_action(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
        )
        priority_label = str(row.get("priority_label") or "").strip() or "unknown"
        sample = _build_auto_scope_sample(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
            action_category=action_category,
            failure_record=failure_record,
        )
        sample["history_maturity_reason"] = row.get("history_maturity_reason")

        if action_category == "await_metadata_completion":
            bucket = (
                "buyable_active_metadata_blocked"
                if priority_label == "buyable_active_priority"
                else "other_metadata_blocked"
            )
            add_bucket_sample(bucket, sample)
            counts_by_priority[priority_label] = int(counts_by_priority.get(priority_label) or 0) + 1
            continue

        if action_category == "defer_until_scope_recovers":
            bucket = (
                "buyable_active_scope_not_ready"
                if priority_label == "buyable_active_priority"
                else "other_scope_not_ready"
            )
            add_bucket_sample(bucket, sample)
            counts_by_priority[priority_label] = int(counts_by_priority.get(priority_label) or 0) + 1
            if source_category == "local_stale_history_gap":
                nav_bucket = _classify_priority_scope_not_ready_nav_bucket(
                    sample.get("nav_staleness_days")
                )
                stale_mode = _classify_priority_scope_not_ready_stale_mode(sample)
                stale_scope_counts_by_nav_bucket[nav_bucket] = int(
                    stale_scope_counts_by_nav_bucket.get(nav_bucket) or 0
                ) + 1
                stale_scope_counts_by_stale_mode[stale_mode] = int(
                    stale_scope_counts_by_stale_mode.get(stale_mode) or 0
                ) + 1
                stale_scope_counts_by_priority[priority_label] = int(
                    stale_scope_counts_by_priority.get(priority_label) or 0
                ) + 1
                if nav_bucket == "terminal_gt_365d":
                    stale_scope_terminal_candidate_total += 1
                elif nav_bucket in {"deep_121_365d", "medium_61_120d"}:
                    stale_scope_deep_stale_candidate_total += 1

    total = int(sum(counts_by_bucket.values()))
    stale_buyable_active_scope_not_ready_total = int(
        (source_counts_by_bucket.get("buyable_active_scope_not_ready") or {}).get("local_stale_history_gap") or 0
    )
    stale_other_scope_not_ready_total = int(
        (source_counts_by_bucket.get("other_scope_not_ready") or {}).get("local_stale_history_gap") or 0
    )
    stale_scope_not_ready_total = stale_buyable_active_scope_not_ready_total + stale_other_scope_not_ready_total
    stale_scope_recoverability_summary = _build_stale_scope_recoverability_summary(
        stale_scope_counts_by_nav_bucket
    )
    return {
        "total": total,
        "buyable_active_metadata_blocked_total": int(counts_by_bucket.get("buyable_active_metadata_blocked") or 0),
        "buyable_active_scope_not_ready_total": int(counts_by_bucket.get("buyable_active_scope_not_ready") or 0),
        "other_metadata_blocked_total": int(counts_by_bucket.get("other_metadata_blocked") or 0),
        "other_scope_not_ready_total": int(counts_by_bucket.get("other_scope_not_ready") or 0),
        "scope_not_ready_total": int(counts_by_bucket.get("buyable_active_scope_not_ready") or 0)
        + int(counts_by_bucket.get("other_scope_not_ready") or 0),
        "stale_buyable_active_scope_not_ready_total": stale_buyable_active_scope_not_ready_total,
        "stale_other_scope_not_ready_total": stale_other_scope_not_ready_total,
        "stale_scope_not_ready_total": stale_scope_not_ready_total,
        "total_excluding_stale_scope_not_ready": max(0, total - stale_scope_not_ready_total),
        "counts_by_bucket": counts_by_bucket,
        "counts_by_priority": counts_by_priority,
        "source_counts_by_bucket": source_counts_by_bucket,
        "stale_scope_not_ready_full_summary": {
            "total": stale_scope_not_ready_total,
            "counts_by_nav_bucket": stale_scope_counts_by_nav_bucket,
            "counts_by_stale_mode": stale_scope_counts_by_stale_mode,
            "counts_by_priority": stale_scope_counts_by_priority,
            "terminal_candidate_total": stale_scope_terminal_candidate_total,
            "deep_stale_candidate_total": stale_scope_deep_stale_candidate_total,
            **stale_scope_recoverability_summary,
        },
        "samples_by_bucket": samples_by_bucket,
    }


def _build_scope_filtered_no_retry_summary(
    universe: list[dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = list(universe or [])
    normalized_failure_backoff = _normalize_failure_backoff(failure_backoff or {})
    counts_by_source: dict[str, int] = {}
    counts_by_blocker: dict[str, int] = {}
    counts_by_maturity_reason: dict[str, int] = {}
    counts_by_stale_mode: dict[str, int] = {}
    structure_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []

    def add_structure(label: str) -> None:
        normalized_label = str(label or "").strip() or "unknown"
        structure_counts[normalized_label] = int(structure_counts.get(normalized_label) or 0) + 1

    for row in rows:
        if not _row_has_long_term_history_gap(row):
            continue
        code = _normalize_code(row.get("fcode"))
        failure_record = dict(normalized_failure_backoff.get(code) or {})
        source_category = _classify_long_term_gap_source(row, failure_record)
        blocker_category = _classify_long_term_gap_blocker(row)
        action_category = _classify_long_term_gap_action(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
        )
        if action_category != "scope_filtered_no_retry":
            continue
        sample = _build_auto_scope_sample(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
            action_category=action_category,
            failure_record=failure_record,
        )
        history_maturity_reason = str(row.get("history_maturity_reason") or "").strip() or "unknown"
        sample["history_maturity_reason"] = history_maturity_reason
        counts_by_source[source_category] = int(counts_by_source.get(source_category) or 0) + 1
        counts_by_blocker[blocker_category] = int(counts_by_blocker.get(blocker_category) or 0) + 1
        counts_by_maturity_reason[history_maturity_reason] = int(
            counts_by_maturity_reason.get(history_maturity_reason) or 0
        ) + 1
        stale_mode = _classify_priority_scope_not_ready_stale_mode(sample)
        counts_by_stale_mode[stale_mode] = int(counts_by_stale_mode.get(stale_mode) or 0) + 1

        if blocker_category == "holding_period_blocked":
            add_structure("holding_period_blocked")
            if normalize_buyability(row.get("buyable")) is None:
                add_structure("unknown_buyability_holding_period_blocked")
            elif bool(row.get("buyable")):
                add_structure("buyable_holding_period_blocked")
            else:
                add_structure("non_buyable_holding_period_blocked")
        elif blocker_category == "runtime_excluded":
            add_structure("runtime_excluded")
            if normalize_buyability(row.get("buyable")) is None:
                add_structure("unknown_buyability_runtime_excluded")
            elif bool(row.get("buyable")):
                add_structure("buyable_runtime_excluded")
            else:
                add_structure("non_buyable_runtime_excluded")
        else:
            add_structure("other")

        if bool(row.get("stale_rank_history")) and not bool(row.get("stale_annual_returns")):
            add_structure("rank_only_stale_tail")
        if bool(row.get("needs_annual_returns")) or bool(row.get("needs_rank_history")):
            add_structure("missing_history_tail")
        if not bool(row.get("fresh_nav")):
            add_structure("nav_not_fresh_tail")

        if len(samples) < AUDIT_SAMPLE_LIMIT:
            samples.append(sample)

    return {
        "total": int(sum(counts_by_source.values())),
        "counts_by_source": counts_by_source,
        "counts_by_blocker": counts_by_blocker,
        "counts_by_maturity_reason": counts_by_maturity_reason,
        "counts_by_stale_mode": counts_by_stale_mode,
        "structure_counts": structure_counts,
        "samples": samples,
    }


def _narrow_automatic_selection_pool(
    candidates: list[dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible_candidates: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    samples_by_reason: dict[str, list[dict[str, Any]]] = {}

    def add_reason(reason: str, sample: dict[str, Any]) -> None:
        normalized_reason = str(reason or "").strip() or "other"
        reason_counts[normalized_reason] = int(reason_counts.get(normalized_reason) or 0) + 1
        samples = samples_by_reason.setdefault(normalized_reason, [])
        if len(samples) < AUTO_SCOPE_SAMPLE_LIMIT:
            samples.append(sample)

    for row in candidates:
        code = _normalize_code(row.get("fcode"))
        failure_record = dict(failure_backoff.get(code) or {})
        source_category = _classify_long_term_gap_source(row, failure_record)
        blocker_category = _classify_long_term_gap_blocker(row)
        action_category = _classify_long_term_gap_action(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
        )
        sample = _build_auto_scope_sample(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
            action_category=action_category,
            failure_record=failure_record,
        )

        if action_category == "retry_locally_now":
            eligible_candidates.append(row)
            add_reason("retry_locally_now", sample)
            continue
        if action_category == "retry_locally_later":
            add_reason("retry_later", sample)
            continue
        if action_category == "await_history_maturity":
            add_reason("await_history_maturity", sample)
            continue
        if action_category == "await_metadata_completion":
            add_reason("await_metadata_completion", sample)
            continue
        if action_category == "scope_filtered_no_retry":
            add_reason("scope_filtered_no_retry", sample)
            continue
        if action_category == "defer_until_scope_recovers":
            add_reason("defer_until_scope_recovers", sample)
            continue
        if action_category == "observe_upstream_source":
            add_reason("upstream_limited", sample)
            continue
        if source_category.startswith("upstream_limited"):
            add_reason("upstream_limited", sample)
            continue
        add_reason("other", sample)

    excluded_total = max(0, len(candidates) - len(eligible_candidates))
    non_upstream_excluded_total = max(0, excluded_total - int(reason_counts.get("upstream_limited") or 0))
    return eligible_candidates, {
        "applied": True,
        "candidate_total": len(candidates),
        "retry_locally_now_total": len(eligible_candidates),
        "excluded_total": excluded_total,
        "excluded_upstream_limited_total": int(reason_counts.get("upstream_limited") or 0),
        "excluded_retry_later_total": int(reason_counts.get("retry_later") or 0),
        "excluded_scope_filtered_total": int(reason_counts.get("scope_filtered_no_retry") or 0),
        "excluded_scope_not_ready_total": int(reason_counts.get("defer_until_scope_recovers") or 0),
        "excluded_pending_maturity_total": int(reason_counts.get("await_history_maturity") or 0),
        "excluded_metadata_blocked_total": int(reason_counts.get("await_metadata_completion") or 0),
        "excluded_other_total": int(reason_counts.get("other") or 0),
        "non_upstream_excluded_total": non_upstream_excluded_total,
        "reason_counts": reason_counts,
        "samples_by_reason": samples_by_reason,
    }


def _hydrate_failure_backoff_policy(
    failure_backoff: dict[str, dict[str, Any]],
    *,
    empty_payload_retry_days: int,
    stale_refresh_retry_days: int,
    empty_payload_upstream_limited_failures: int,
    stale_refresh_upstream_limited_failures: int,
) -> dict[str, dict[str, Any]]:
    hydrated = _normalize_failure_backoff(failure_backoff)
    for code, record in hydrated.items():
        failed_stages = list(record.get("last_failed_stages") or [])
        failed_errors = _split_failure_error_tokens(record.get("last_error"))
        is_stable_empty_payload = _is_stable_empty_payload_failure(failed_stages, failed_errors)
        is_stale_no_progress = _is_stale_refresh_no_progress_failure(failed_errors)
        is_observe_upstream_source = _all_failures_map_to_governance_action(
            failed_errors,
            "observe_upstream_source",
        )
        if not is_stable_empty_payload and not is_stale_no_progress and not is_observe_upstream_source:
            continue
        if is_stable_empty_payload:
            record["upstream_limited"] = bool(
                record.get("upstream_limited")
                or max(0, int(record.get("consecutive_failures") or 0)) >= max(2, int(empty_payload_upstream_limited_failures or 0))
            )
        elif is_stale_no_progress:
            record["upstream_limited"] = bool(
                record.get("upstream_limited")
                or max(0, int(record.get("consecutive_failures") or 0)) >= max(2, int(stale_refresh_upstream_limited_failures or 0))
            )
        elif is_observe_upstream_source:
            record["upstream_limited"] = bool(
                record.get("upstream_limited")
                or max(0, int(record.get("consecutive_failures") or 0)) >= max(2, int(empty_payload_upstream_limited_failures or 0))
            )
        if not record.get("next_retry_after"):
            record["next_retry_after"] = _resolve_next_retry_after(
                str(record.get("last_failed_at") or ""),
                failed_stages=failed_stages,
                failed_errors=failed_errors,
                empty_payload_retry_days=empty_payload_retry_days,
                stale_refresh_retry_days=stale_refresh_retry_days,
                consecutive_failures=max(1, int(record.get("consecutive_failures") or 1)),
            )
        record["skip_runs_remaining"] = 0
        hydrated[code] = record
    return hydrated


def _row_has_long_term_history_gap(row: dict[str, Any]) -> bool:
    return bool(
        row.get("needs_annual_returns")
        or row.get("needs_rank_history")
        or row.get("annual_history_missing_raw")
        or row.get("rank_history_missing_raw")
        or row.get("stale_annual_returns")
        or row.get("stale_rank_history")
    )


def _is_upstream_semantic_annual_null_year_gap(row: dict[str, Any]) -> bool:
    return bool(
        not row.get("annual_history_missing_raw")
        and not row.get("stale_annual_returns")
        and row.get("annual_history_mature_enough")
        and _annual_feature_window_has_semantic_null_gap(row)
    )


def _is_leading_partial_annual_null_year_gap(row: dict[str, Any]) -> bool:
    return bool(
        row.get("annual_feature_window_leading_partial_null_year_gap")
        and row.get("annual_history_mature_enough")
        and row.get("needs_annual_returns")
    )


def _classify_long_term_gap_source(
    row: dict[str, Any],
    failure_record: dict[str, Any] | None,
) -> str:
    if not _row_has_long_term_history_gap(row):
        return "no_gap"
    if _is_leading_partial_annual_null_year_gap(row):
        return "leading_partial_null_year_gap"
    if _is_upstream_semantic_annual_null_year_gap(row):
        return "upstream_limited_semantic_null_year_gap"

    record = dict(failure_record or {})
    failed_stages = list(record.get("last_failed_stages") or [])
    failed_errors = _split_failure_error_tokens(record.get("last_error"))
    if bool(record.get("upstream_limited")):
        if _is_stable_empty_payload_failure(failed_stages, failed_errors):
            return "upstream_limited_empty_payload"
        if _is_stale_refresh_no_progress_failure(failed_errors):
            return "upstream_limited_stale_no_progress"
        return "upstream_limited_other"

    if record:
        if _has_active_retry_window(record):
            return "local_retry_window_pending"
        if int(record.get("skip_runs_remaining") or 0) > 0:
            return "local_retry_cooldown_pending"
        if int(record.get("consecutive_failures") or 0) > 0:
            return "local_retry_pending"
        return "local_previous_failure_pending_recheck"

    if bool(row.get("stale_annual_returns") or row.get("stale_rank_history")):
        return "local_stale_history_gap"
    return "local_missing_history_gap"


def _classify_long_term_gap_blocker(row: dict[str, Any]) -> str:
    if (
        _is_leading_partial_annual_null_year_gap(row)
        and not bool(row.get("needs_rank_history"))
        and not bool(row.get("stale_rank_history"))
        and not bool(row.get("rank_history_missing_raw"))
    ):
        return "history_pending_maturity"
    history_maturity_reason = str(row.get("history_maturity_reason") or "").strip()
    if history_maturity_reason in {
        "estab_date_pending",
        "nav_history_lower_bound_pending",
        "recent_discovery_waiting_initial_nav",
    }:
        return "history_pending_maturity"
    if history_maturity_reason == "metadata_missing_no_latest_nav":
        return "metadata_insufficient"
    if bool(row.get("holding_period_blocked")):
        return "holding_period_blocked"
    if not bool(row.get("active_universe")):
        return "runtime_excluded"
    if not bool(row.get("fresh_nav")) or not bool(row.get("risk_ready")):
        return "nav_or_risk_not_ready"
    if normalize_buyability(row.get("buyable")) is None:
        return "buyability_unknown"
    if not bool(row.get("buyable")):
        return "not_buyable"
    if not bool(row.get("screenable_universe")):
        return "non_screenable_other"
    return "none"


def _classify_long_term_gap_action(
    row: dict[str, Any],
    *,
    source_category: str,
    blocker_category: str,
) -> str:
    if not _row_has_long_term_history_gap(row):
        return "no_action_needed"
    if blocker_category == "history_pending_maturity":
        return "await_history_maturity"
    if blocker_category == "metadata_insufficient":
        return "await_metadata_completion"
    if blocker_category in {"holding_period_blocked", "runtime_excluded"}:
        return "scope_filtered_no_retry"
    if source_category.startswith("upstream_limited"):
        return "observe_upstream_source"
    if source_category in {
        "local_retry_window_pending",
        "local_retry_cooldown_pending",
    }:
        return "retry_locally_later"
    # 本地失败在 cooldown / retry window 已经结束后，应该重新回到可执行队列。
    # 若仍继续标记为 retry_later，会让样本在自动 scope 中永久饿死。
    if source_category in {
        "local_retry_pending",
        "local_previous_failure_pending_recheck",
    }:
        return "retry_locally_now"
    nav_staleness_days = _resolve_nav_staleness_days(row.get("latest_nav_date"))
    if (
        blocker_category == "nav_or_risk_not_ready"
        and source_category == "local_stale_history_gap"
        and str(row.get("priority_label") or "").strip() == "buyable_active_priority"
        and bool(row.get("buyable"))
        and bool(row.get("active_universe"))
        and not bool(row.get("holding_period_blocked"))
        and not bool(row.get("needs_annual_returns"))
        and not bool(row.get("needs_rank_history"))
        and bool(row.get("stale_rank_history"))
        and not bool(row.get("stale_annual_returns"))
        and nav_staleness_days is not None
        and nav_staleness_days <= BUYABLE_ACTIVE_STALE_RANK_RETRY_MAX_NAV_STALENESS_DAYS
    ):
        return "retry_locally_now"
    # 本地缺失/过期的长期历史并不依赖当前 1y 风险指标是否已就绪。
    # 若基金仍在活跃宇宙且净值新鲜，继续等待 `risk_ready` 恢复会让
    # 自动维护长期空转，因此允许优先回补这类本地缺口。
    if (
        blocker_category == "nav_or_risk_not_ready"
        and source_category in {"local_missing_history_gap", "local_stale_history_gap"}
        and bool(row.get("active_universe"))
        and bool(row.get("fresh_nav"))
        and not bool(row.get("holding_period_blocked"))
    ):
        return "retry_locally_now"
    # screenable 但暂不可买的基金不会进入正式发布池，但其长期排名特征仍会影响
    # screenable universe 覆盖诊断。若 NAV/风险均就绪且只是 rank history stale，
    # 继续等待 buyable 恢复会让可恢复的长期特征 backlog 长期空转。
    if (
        blocker_category in {"not_buyable", "buyability_unknown"}
        and source_category == "local_stale_history_gap"
        and str(row.get("priority_label") or "").strip() == "screenable_priority"
        and bool(row.get("screenable_universe"))
        and bool(row.get("active_universe"))
        and bool(row.get("fresh_nav"))
        and bool(row.get("risk_ready"))
        and not bool(row.get("holding_period_blocked"))
        and not bool(row.get("needs_annual_returns"))
        and not bool(row.get("needs_rank_history"))
        and bool(row.get("stale_rank_history"))
        and not bool(row.get("stale_annual_returns"))
    ):
        return "retry_locally_now"
    if blocker_category in {"nav_or_risk_not_ready", "not_buyable", "buyability_unknown", "non_screenable_other"}:
        return "defer_until_scope_recovers"
    return "retry_locally_now"


def build_long_term_history_gap_audit(
    settings: Settings,
    *,
    include_stale_candidates: bool = True,
    universe: list[dict[str, Any]] | None = None,
    candidates: list[dict[str, Any]] | None = None,
    stats: dict[str, int] | None = None,
    failure_backoff: dict[str, dict[str, Any]] | None = None,
    write_artifact: bool = True,
) -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    annual_enabled = True
    rank_enabled = True

    if universe is None or candidates is None or stats is None:
        loaded_universe, loaded_candidates, loaded_stats = _read_bond_universe(
            settings,
            settings.database_path,
            annual_enabled=annual_enabled,
            rank_enabled=rank_enabled,
            include_stale_candidates=include_stale_candidates,
        )
        universe = loaded_universe
        candidates = loaded_candidates
        stats = loaded_stats
    else:
        universe = list(universe)
        candidates = list(candidates)
        stats = dict(stats)

    if failure_backoff is None:
        state_payload = _load_maintenance_state(settings)
        failure_backoff = _hydrate_failure_backoff_policy(
            state_payload.get("failure_backoff") or {},
            empty_payload_retry_days=_resolve_empty_payload_retry_days(settings),
            stale_refresh_retry_days=_resolve_stale_refresh_retry_days(settings),
            empty_payload_upstream_limited_failures=_resolve_empty_payload_upstream_limited_failures(settings),
            stale_refresh_upstream_limited_failures=_resolve_stale_refresh_upstream_limited_failures(settings),
        )
    else:
        failure_backoff = _normalize_failure_backoff(failure_backoff)

    source_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}
    action_scope_matrix: dict[str, dict[str, int]] = {}
    samples_by_action: dict[str, list[dict[str, Any]]] = {}
    samples_by_blocker: dict[str, list[dict[str, Any]]] = {}
    samples_by_source: dict[str, list[dict[str, Any]]] = {}

    issue_counts = {
        "annual_missing_raw_total": 0,
        "rank_missing_raw_total": 0,
        "annual_actionable_missing_total": 0,
        "rank_actionable_missing_total": 0,
        "annual_stale_total": 0,
        "rank_stale_total": 0,
        "both_missing_raw_total": 0,
        "both_actionable_missing_total": 0,
    }

    for row in universe:
        if not _row_has_long_term_history_gap(row):
            continue
        code = _normalize_code(row.get("fcode"))
        failure_record = dict(failure_backoff.get(code) or {})
        source_category = _classify_long_term_gap_source(row, failure_record)
        blocker_category = _classify_long_term_gap_blocker(row)
        action_category = _classify_long_term_gap_action(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
        )
        scope_category = str(row.get("priority_label") or "unknown").strip() or "unknown"

        source_counts[source_category] = int(source_counts.get(source_category) or 0) + 1
        blocker_counts[blocker_category] = int(blocker_counts.get(blocker_category) or 0) + 1
        action_counts[action_category] = int(action_counts.get(action_category) or 0) + 1
        scope_counts[scope_category] = int(scope_counts.get(scope_category) or 0) + 1
        action_scope_matrix.setdefault(action_category, {})
        action_scope_matrix[action_category][scope_category] = (
            int(action_scope_matrix[action_category].get(scope_category) or 0) + 1
        )

        annual_missing_raw = bool(row.get("annual_history_missing_raw"))
        rank_missing_raw = bool(row.get("rank_history_missing_raw"))
        annual_actionable_missing = bool(row.get("needs_annual_returns"))
        rank_actionable_missing = bool(row.get("needs_rank_history"))
        annual_stale = bool(row.get("stale_annual_returns"))
        rank_stale = bool(row.get("stale_rank_history"))
        if annual_missing_raw:
            issue_counts["annual_missing_raw_total"] += 1
        if rank_missing_raw:
            issue_counts["rank_missing_raw_total"] += 1
        if annual_actionable_missing:
            issue_counts["annual_actionable_missing_total"] += 1
        if rank_actionable_missing:
            issue_counts["rank_actionable_missing_total"] += 1
        if annual_stale:
            issue_counts["annual_stale_total"] += 1
        if rank_stale:
            issue_counts["rank_stale_total"] += 1
        if annual_missing_raw and rank_missing_raw:
            issue_counts["both_missing_raw_total"] += 1
        if annual_actionable_missing and rank_actionable_missing:
            issue_counts["both_actionable_missing_total"] += 1

        sample = {
            "fcode": code,
            "shortname": row.get("shortname"),
            "priority_label": scope_category,
            "publishable_priority": bool(row.get("publishable_priority")),
            "screenable_universe": bool(row.get("screenable_universe")),
            "buyable": normalize_buyability(row.get("buyable")),
            "active_universe": bool(row.get("active_universe")),
            "fresh_nav": bool(row.get("fresh_nav")),
            "risk_ready": bool(row.get("risk_ready")),
            "holding_period_blocked": bool(row.get("holding_period_blocked")),
            "history_maturity_reason": row.get("history_maturity_reason"),
            "annual_history_missing_raw": annual_missing_raw,
            "rank_history_missing_raw": rank_missing_raw,
            "needs_annual_returns": annual_actionable_missing,
            "needs_rank_history": rank_actionable_missing,
            "stale_annual_returns": annual_stale,
            "stale_rank_history": rank_stale,
            "source_category": source_category,
            "blocker_category": blocker_category,
            "recommended_action": action_category,
            "cooldown_consecutive_failures": int(failure_record.get("consecutive_failures") or 0),
            "cooldown_next_retry_after": str(failure_record.get("next_retry_after") or "").strip() or None,
            "cooldown_upstream_limited": bool(failure_record.get("upstream_limited")),
            "cooldown_last_failed_stages": list(failure_record.get("last_failed_stages") or []),
            "cooldown_last_error": str(failure_record.get("last_error") or "").strip() or None,
        }

        samples_by_action.setdefault(action_category, [])
        if len(samples_by_action[action_category]) < 10:
            samples_by_action[action_category].append(sample)

        samples_by_blocker.setdefault(blocker_category, [])
        if len(samples_by_blocker[blocker_category]) < 10:
            samples_by_blocker[blocker_category].append(sample)

        samples_by_source.setdefault(source_category, [])
        if len(samples_by_source[source_category]) < 10:
            samples_by_source[source_category].append(sample)

    payload = {
        "generated_at": generated_at,
        "include_stale_candidates": bool(include_stale_candidates),
        "total_gap_fund_count": int(sum(source_counts.values())),
        "candidate_total": int(len(candidates)),
        "screenable_candidate_total": int(stats.get("screenable_candidate_total") or 0),
        "buyable_candidate_total": int(stats.get("buyable_candidate_total") or 0),
        "history_pending_maturity_total": int(
            stats.get("annual_history_pending_maturity_total") or 0
        ),
        "metadata_blocked_total": int(stats.get("history_metadata_blocked_total") or 0),
        "issue_counts": issue_counts,
        "source_counts": source_counts,
        "blocker_counts": blocker_counts,
        "action_counts": action_counts,
        "scope_counts": scope_counts,
        "action_scope_matrix": action_scope_matrix,
        "priority_blocked_summary": _build_priority_blocked_summary(
            universe,
            failure_backoff,
        ),
        "scope_filtered_no_retry_summary": _build_scope_filtered_no_retry_summary(
            universe,
            failure_backoff,
        ),
        "upstream_observation_summary": _build_upstream_observation_summary(
            {str(row.get("fcode") or ""): row for row in universe if str(row.get("fcode") or "")},
            failure_backoff,
        ),
        "samples_by_action": samples_by_action,
        "samples_by_blocker": samples_by_blocker,
        "samples_by_source": samples_by_source,
    }

    if write_artifact:
        artifact_dir = settings.reports_dir / "audit" / "long_term_history_gap"
        artifact_file = build_audit_artifact_path(artifact_dir, "long_term_history_gap", suffix=".json.gz")
        _write_json(artifact_file, payload)
        payload["artifact_path"] = str(artifact_file)
    else:
        payload["artifact_path"] = None
    return payload


def _update_failure_backoff_after_run(
    failure_backoff: dict[str, dict[str, Any]],
    *,
    selected_candidates: list[dict[str, Any]],
    failed_items: list[dict[str, str]],
    candidate_codes_after: set[str],
    failed_at: str,
    empty_payload_retry_days: int,
    stale_refresh_retry_days: int,
    empty_payload_upstream_limited_failures: int,
    stale_refresh_upstream_limited_failures: int,
) -> dict[str, dict[str, Any]]:
    updated = _prune_failure_backoff(failure_backoff, candidate_codes=candidate_codes_after)
    failed_items_by_code = _group_failed_items_by_code(failed_items)

    for row in selected_candidates:
        code = _normalize_code(row.get("fcode"))
        if not code:
            continue
        if code not in candidate_codes_after:
            updated.pop(code, None)
            continue

        failures = failed_items_by_code.get(code) or []
        if not failures:
            updated.pop(code, None)
            continue

        previous = dict(updated.get(code) or {})
        consecutive_failures = max(0, int(previous.get("consecutive_failures") or 0)) + 1
        failed_stages = sorted(
            {
                str(item.get("stage") or "").strip()
                for item in failures
                if str(item.get("stage") or "").strip()
            }
        )
        failed_errors = [
            str(item.get("error") or "").strip()
            for item in failures
            if str(item.get("error") or "").strip()
        ]
        is_stable_empty_payload = _is_stable_empty_payload_failure(failed_stages, failed_errors)
        is_stale_no_progress = _is_stale_refresh_no_progress_failure(failed_errors)
        is_observe_upstream_source = _all_failures_map_to_governance_action(
            failed_errors,
            "observe_upstream_source",
        )
        updated[code] = {
            "consecutive_failures": consecutive_failures,
            "skip_runs_remaining": _cooldown_runs_for_failure_detail(
                consecutive_failures,
                failed_stages,
                failed_errors,
            ),
            "last_failed_at": failed_at,
            "next_retry_after": _resolve_next_retry_after(
                failed_at,
                failed_stages=failed_stages,
                failed_errors=failed_errors,
                empty_payload_retry_days=empty_payload_retry_days,
                stale_refresh_retry_days=stale_refresh_retry_days,
                consecutive_failures=consecutive_failures,
            ),
            "last_failed_stages": failed_stages,
            "last_error": " | ".join(failed_errors)[:500],
            "upstream_limited": bool(
                (is_stable_empty_payload and consecutive_failures >= max(2, int(empty_payload_upstream_limited_failures or 0)))
                or (is_stale_no_progress and consecutive_failures >= max(2, int(stale_refresh_upstream_limited_failures or 0)))
                or (is_observe_upstream_source and consecutive_failures >= max(2, int(empty_payload_upstream_limited_failures or 0)))
            ),
        }

    return updated


def _load_runtime_excluded_fund_codes_lightweight() -> set[str]:
    return set(get_runtime_excluded_fund_codes())


def _candidate_priority(row: dict[str, Any]) -> tuple[int, str]:
    issue_types = list(row.get("issue_types") or [])
    issue_type_set = set(issue_types)
    if {"missing_annual_returns", "missing_rank_history"} <= issue_type_set:
        issue_key = "missing_both_histories"
    elif "missing_annual_returns" in issue_type_set and "stale_rank_history" in issue_type_set:
        issue_key = "missing_annual_and_stale_rank"
    elif "missing_rank_history" in issue_type_set and "stale_annual_returns" in issue_type_set:
        issue_key = "missing_rank_and_stale_annual"
    elif "missing_annual_returns" in issue_type_set:
        issue_key = "missing_annual_returns"
    elif "missing_rank_history" in issue_type_set:
        issue_key = "missing_rank_history"
    elif {"stale_annual_returns", "stale_rank_history"} <= issue_type_set:
        issue_key = "stale_both_histories"
    elif "stale_annual_returns" in issue_type_set:
        issue_key = "stale_annual_returns"
    else:
        issue_key = "stale_rank_history"
    priority_band_raw = row.get("priority_band")
    priority_band = 99 if priority_band_raw is None else int(priority_band_raw)
    latest_rank_date = str(row.get("latest_rank_date") or "").strip()
    latest_annual_year_raw = row.get("latest_annual_year")
    try:
        latest_annual_year = int(latest_annual_year_raw) if latest_annual_year_raw not in (None, "") else 0
    except (TypeError, ValueError):
        latest_annual_year = 0
    stale_rank_sort_key = latest_rank_date or "9999-12-31"
    stale_annual_sort_key = -latest_annual_year if latest_annual_year > 0 else 0
    return (
        priority_band,
        ISSUE_PRIORITY.get(issue_key, 99),
        stale_rank_sort_key,
        stale_annual_sort_key,
        str(row.get("fcode") or ""),
    )


def _candidate_priority_with_governance_queue(
    row: dict[str, Any],
    governance_priority_codes: set[str],
) -> tuple[Any, ...]:
    base_priority = _candidate_priority(row)
    code = _normalize_code(row.get("fcode"))
    governance_rank = 0 if code and code in governance_priority_codes else 1
    return (
        base_priority[0],
        governance_rank,
        *base_priority[1:],
    )


def _candidate_priority_with_current_published_and_governance(
    row: dict[str, Any],
    current_published_codes: set[str],
    governance_priority_codes: set[str],
) -> tuple[Any, ...]:
    base_priority = _candidate_priority(row)
    code = _normalize_code(row.get("fcode"))
    current_published_rank = 0 if code and code in current_published_codes else 1
    governance_rank = 0 if code and code in governance_priority_codes else 1
    return (
        base_priority[0],
        current_published_rank,
        governance_rank,
        *base_priority[1:],
    )


def _prioritize_current_published_and_governance_candidates(
    candidates: list[dict[str, Any]],
    current_published_codes: set[str],
    governance_priority_codes: set[str],
) -> list[dict[str, Any]]:
    if not candidates or (not current_published_codes and not governance_priority_codes):
        return list(candidates)
    return sorted(
        candidates,
        key=lambda row: _candidate_priority_with_current_published_and_governance(
            row,
            current_published_codes,
            governance_priority_codes,
        ),
    )


def _prioritize_governance_queue_candidates(
    candidates: list[dict[str, Any]],
    governance_priority_codes: set[str],
) -> list[dict[str, Any]]:
    if not candidates or not governance_priority_codes:
        return list(candidates)
    return sorted(
        candidates,
        key=lambda row: _candidate_priority_with_governance_queue(row, governance_priority_codes),
    )


def _safe_load_json_dict(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_project_artifact_path(settings: Any, raw_path: Any) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    candidate = Path(text)
    if candidate.is_absolute():
        if candidate.exists():
            return candidate
        project_root = Path(getattr(settings, "project_root", PROJECT_ROOT) or PROJECT_ROOT)
        parts = candidate.parts
        for index, part in enumerate(parts):
            if part in {"app", "workspace"} and index + 1 < len(parts):
                rebased = project_root / Path(*parts[index + 1:])
                if rebased.exists():
                    return rebased
        if len(parts) > 1:
            rebased = project_root / Path(*parts[1:])
            if rebased.exists():
                return rebased
        return None
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT) or PROJECT_ROOT)
    rebased = project_root / candidate
    if rebased.exists():
        return rebased
    return candidate if candidate.exists() else None


def _load_current_published_formal_codes(settings: Any) -> tuple[set[str], dict[str, Any]]:
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT) or PROJECT_ROOT)
    reports_dir = Path(getattr(settings, "reports_dir", project_root / "reports") or project_root / "reports")
    registry_path = Path(getattr(settings, "publication_registry_file", None) or reports_dir / "publication_registry.json")
    latest_report_path = Path(getattr(settings, "latest_report_file", None) or reports_dir / "latest.json")
    registry = _safe_load_json_dict(registry_path)
    latest_report = _safe_load_json_dict(latest_report_path)
    current_run_id = (
        str(registry.get("current_published_run_id") or "").strip()
        or str(latest_report.get("run_id") or "").strip()
    )
    report_path = _resolve_project_artifact_path(settings, registry.get("current_published_report_path"))
    report = _safe_load_json_dict(report_path)
    if not report and current_run_id and str(latest_report.get("run_id") or "").strip() == current_run_id:
        report = latest_report
        report_path = latest_report_path if latest_report_path.exists() else None

    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_artifact_path(settings, artifacts.get("filtered_csv"))
    summary = {
        "available": False,
        "truth_source": "publication_registry_current_published_report_filtered_csv",
        "current_published_run_id": current_run_id or None,
        "publication_registry_path": str(registry_path),
        "report_path": str(report_path) if report_path else None,
        "filtered_csv_path": str(filtered_csv_path) if filtered_csv_path else None,
        "code_count": 0,
        "reason": None,
    }
    if not current_run_id:
        summary["reason"] = "current_published_run_id_missing"
        return set(), summary
    if not report:
        summary["reason"] = "current_published_report_unavailable"
        return set(), summary
    if str(report.get("run_id") or "").strip() not in {"", current_run_id}:
        summary["reason"] = "current_published_report_run_id_mismatch"
        summary["report_run_id"] = str(report.get("run_id") or "").strip() or None
        return set(), summary
    if not filtered_csv_path:
        summary["reason"] = "filtered_csv_artifact_unavailable"
        return set(), summary

    try:
        frame = pd.read_csv(filtered_csv_path, dtype=str)
    except Exception as exc:
        summary["reason"] = "filtered_csv_read_failed"
        summary["error"] = str(exc)[:300]
        return set(), summary
    code_column = "基金代码" if "基金代码" in frame.columns else (str(frame.columns[0]) if len(frame.columns) else "")
    if not code_column:
        summary["reason"] = "filtered_csv_code_column_missing"
        return set(), summary
    codes = {
        _normalize_code(value)
        for value in frame[code_column].tolist()
        if _normalize_code(value)
    }
    summary["available"] = bool(codes)
    summary["code_count"] = len(codes)
    summary["code_column"] = code_column
    summary["reason"] = None if codes else "filtered_csv_empty"
    return codes, summary


def _build_current_published_priority_selection_audit(
    *,
    current_published_summary: dict[str, Any],
    current_published_codes: set[str],
    effective_selection_pool: list[dict[str, Any]],
    selected_candidates: list[dict[str, Any]],
    annual_target_codes: list[str],
    rank_target_codes: list[str],
    annual_success_codes: list[str],
    rank_success_codes: list[str],
    failed_items: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_current_codes = {
        _normalize_code(code)
        for code in current_published_codes or set()
        if _normalize_code(code)
    }

    def code_set(rows: list[dict[str, Any]]) -> set[str]:
        return {
            _normalize_code(row.get("fcode"))
            for row in rows or []
            if _normalize_code(row.get("fcode"))
        }

    effective_codes = code_set(effective_selection_pool)
    selected_codes = code_set(selected_candidates)
    annual_target_set = {
        _normalize_code(code)
        for code in annual_target_codes or []
        if _normalize_code(code)
    }
    rank_target_set = {
        _normalize_code(code)
        for code in rank_target_codes or []
        if _normalize_code(code)
    }
    annual_success_set = {
        _normalize_code(code)
        for code in annual_success_codes or []
        if _normalize_code(code)
    }
    rank_success_set = {
        _normalize_code(code)
        for code in rank_success_codes or []
        if _normalize_code(code)
    }
    failed_set = {
        _normalize_code(item.get("fcode"))
        for item in failed_items or []
        if _normalize_code(item.get("fcode"))
    }
    effective_hits = normalized_current_codes & effective_codes
    selected_hits = effective_hits & selected_codes
    return {
        **dict(current_published_summary or {}),
        "priority_enabled": bool((current_published_summary or {}).get("available") and normalized_current_codes),
        "effective_pool_hit_total": len(effective_hits),
        "selected_hit_total": len(selected_hits),
        "remaining_effective_hit_total": len(effective_hits - selected_hits),
        "selected_stage_counts": {
            "annual_target": len(selected_hits & annual_target_set),
            "rank_target": len(selected_hits & rank_target_set),
            "annual_success": len(selected_hits & annual_success_set),
            "rank_success": len(selected_hits & rank_success_set),
            "failed": len(selected_hits & failed_set),
        },
        "effective_codes_sample": sorted(effective_hits)[:AUDIT_SAMPLE_LIMIT],
        "selected_codes_sample": sorted(selected_hits)[:AUDIT_SAMPLE_LIMIT],
        "remaining_effective_codes_sample": sorted(effective_hits - selected_hits)[:AUDIT_SAMPLE_LIMIT],
    }


def _build_governance_queue_consumption_audit(
    *,
    queue_payload: dict[str, Any] | None,
    governance_priority_codes: set[str],
    selection_pool: list[dict[str, Any]],
    effective_selection_pool: list[dict[str, Any]],
    selected_candidates: list[dict[str, Any]],
    annual_target_codes: list[str],
    rank_target_codes: list[str],
    annual_success_codes: list[str],
    rank_success_codes: list[str],
    failed_items: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_priority_codes = {
        _normalize_code(code)
        for code in governance_priority_codes
        if _normalize_code(code)
    }
    queue = dict(queue_payload or {})
    action_codes = {
        _normalize_code(code)
        for code in ((queue.get("codes_by_action") or {}).get("backfill_long_term_features") or [])
        if _normalize_code(code)
    }
    if not normalized_priority_codes and action_codes:
        normalized_priority_codes = set(action_codes)

    def code_set(rows: list[dict[str, Any]]) -> set[str]:
        return {
            _normalize_code(row.get("fcode"))
            for row in rows or []
            if _normalize_code(row.get("fcode"))
        }

    selection_pool_codes = code_set(selection_pool)
    effective_pool_codes = code_set(effective_selection_pool)
    selected_codes = code_set(selected_candidates)
    annual_target_set = {
        _normalize_code(code)
        for code in annual_target_codes or []
        if _normalize_code(code)
    }
    rank_target_set = {
        _normalize_code(code)
        for code in rank_target_codes or []
        if _normalize_code(code)
    }
    annual_success_set = {
        _normalize_code(code)
        for code in annual_success_codes or []
        if _normalize_code(code)
    }
    rank_success_set = {
        _normalize_code(code)
        for code in rank_success_codes or []
        if _normalize_code(code)
    }

    failed_by_code: dict[str, set[str]] = {}
    for item in failed_items or []:
        code = _normalize_code(item.get("fcode"))
        if not code:
            continue
        stages = failed_by_code.setdefault(code, set())
        stage = str(item.get("stage") or "").strip() or "unknown"
        stages.add(stage)

    candidate_hits = normalized_priority_codes & selection_pool_codes
    effective_hits = normalized_priority_codes & effective_pool_codes
    selected_hits = normalized_priority_codes & selected_codes
    annual_target_hits = selected_hits & annual_target_set
    rank_target_hits = selected_hits & rank_target_set
    annual_success_hits = selected_hits & annual_success_set
    rank_success_hits = selected_hits & rank_success_set
    failed_hits = {
        code
        for code in selected_hits
        if code in failed_by_code
    }
    remaining_effective_hits = effective_hits - selected_hits

    selected_priority_counts: dict[str, int] = {}
    selected_stage_counts = {
        "annual_target": len(annual_target_hits),
        "rank_target": len(rank_target_hits),
        "annual_success": len(annual_success_hits),
        "rank_success": len(rank_success_hits),
        "failed": len(failed_hits),
    }
    for row in selected_candidates or []:
        code = _normalize_code(row.get("fcode"))
        if code not in selected_hits:
            continue
        priority_label = str(row.get("priority_label") or "").strip() or "unknown"
        selected_priority_counts[priority_label] = int(selected_priority_counts.get(priority_label) or 0) + 1

    return {
        "available": bool(queue.get("available")),
        "reason": queue.get("reason"),
        "artifact_project_path": queue.get("artifact_project_path"),
        "run_id": queue.get("run_id"),
        "source_report_run_id": queue.get("source_report_run_id"),
        "target_report_run_id": queue.get("target_report_run_id"),
        "target_report_run_source": queue.get("target_report_run_source"),
        "current_report_run_id": queue.get("current_report_run_id"),
        "action_key": "backfill_long_term_features",
        "queue_total": int(queue.get("queue_total") or 0),
        "action_code_total": len(normalized_priority_codes),
        "candidate_hit_total": len(candidate_hits),
        "effective_pool_hit_total": len(effective_hits),
        "selected_hit_total": len(selected_hits),
        "remaining_effective_hit_total": len(remaining_effective_hits),
        "selected_stage_counts": selected_stage_counts,
        "selected_priority_counts": selected_priority_counts,
        "candidate_hit_codes_sample": sorted(candidate_hits)[:AUDIT_SAMPLE_LIMIT],
        "selected_codes_sample": sorted(selected_hits)[:AUDIT_SAMPLE_LIMIT],
        "remaining_effective_codes_sample": sorted(remaining_effective_hits)[:AUDIT_SAMPLE_LIMIT],
        "failed_codes_sample": sorted(failed_hits)[:AUDIT_SAMPLE_LIMIT],
    }


def _resolve_adaptive_high_priority_stale_limit(
    settings: Any,
    *,
    configured_limit: int,
    refresh_stale: bool,
    automatic_scope_applied: bool,
    candidates: list[dict[str, Any]],
) -> tuple[int, dict[str, Any]]:
    normalized_limit = int(configured_limit or 0)
    publishable_priority_stale_total = int(
        sum(
            1
            for row in candidates
            if str(row.get("priority_label") or "").strip() == "publishable_priority"
            and (bool(row.get("stale_annual_returns")) or bool(row.get("stale_rank_history")))
        )
    )
    screenable_priority_stale_total = int(
        sum(
            1
            for row in candidates
            if str(row.get("priority_label") or "").strip() == "screenable_priority"
            and (bool(row.get("stale_annual_returns")) or bool(row.get("stale_rank_history")))
        )
    )
    high_priority_stale_total = publishable_priority_stale_total + screenable_priority_stale_total
    backlog_trigger = _resolve_high_priority_stale_backlog_trigger(settings)
    adaptive_limit = _resolve_high_priority_stale_limit(settings)
    summary = {
        "applied": False,
        "reason": "not_applicable",
        "configured_limit": normalized_limit,
        "effective_limit": normalized_limit,
        "adaptive_limit": adaptive_limit,
        "backlog_trigger": backlog_trigger,
        "high_priority_stale_candidate_total": high_priority_stale_total,
        "publishable_priority_stale_candidate_total": publishable_priority_stale_total,
        "screenable_priority_stale_candidate_total": screenable_priority_stale_total,
    }
    if normalized_limit <= 0:
        summary["reason"] = "configured_limit_non_positive"
        return normalized_limit, summary
    if not refresh_stale:
        summary["reason"] = "refresh_stale_disabled"
        return normalized_limit, summary
    if not automatic_scope_applied:
        summary["reason"] = "automatic_scope_not_applied"
        return normalized_limit, summary
    if high_priority_stale_total < backlog_trigger:
        summary["reason"] = "backlog_below_trigger"
        return normalized_limit, summary
    effective_limit = min(len(candidates), max(normalized_limit, adaptive_limit))
    summary["effective_limit"] = effective_limit
    if effective_limit <= normalized_limit:
        summary["reason"] = "no_additional_headroom"
        return normalized_limit, summary
    summary["applied"] = True
    summary["reason"] = "high_priority_stale_backlog"
    return effective_limit, summary


def _read_bond_universe(
    settings: Settings,
    db_path: Path,
    *,
    annual_enabled: bool = True,
    rank_enabled: bool = True,
    include_stale_candidates: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    runtime_excluded_codes = _load_runtime_excluded_fund_codes_lightweight()
    effective_nav_snapshot = load_effective_nav_snapshot(
        db_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    )
    risk_end_date = str(
        effective_nav_snapshot.risk_1y_effective_end_date
        or effective_nav_snapshot.risk_1y_latest_end_date
        or ""
    ).strip()
    conn = sqlite3.connect(str(db_path))
    nav_archive_binding = load_latest_nav_history_archive_binding()
    if not nav_history_archive_binding_matches_database(nav_archive_binding, db_path):
        nav_archive_binding = {}
    nav_archive_attach = attach_nav_history_archive_if_available(
        conn,
        archive_path=nav_archive_binding.get("archive_path"),
        archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
        archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
    )
    begin_rank_history_read_snapshot(conn)
    rank_archive_binding = load_latest_rank_history_archive_binding()
    if not rank_history_archive_binding_matches_database(rank_archive_binding, db_path):
        rank_archive_binding = {}
    rank_archive_attach = attach_rank_history_archive_if_available(
        conn,
        archive_path=rank_archive_binding.get("archive_path"),
        archive_table_name=rank_archive_binding.get("archive_table_name") or "fund_rank_history_archive",
        archive_cutoff_date=rank_archive_binding.get("archive_cutoff_date") or "",
    )
    try:
        has_first_collected_at = _table_has_column(conn, "funds", "first_collected_at")
        first_collected_select = "COALESCE(first_collected_at, '')" if has_first_collected_at else "''"
        fullname_select = "fullname" if _table_has_column(conn, "funds", "fullname") else "NULL"
        holding_source_select = (
            "holding_period_source" if _table_has_column(conn, "funds", "holding_period_source") else "NULL"
        )
        holding_keywords_select = (
            "holding_keywords" if _table_has_column(conn, "funds", "holding_keywords") else "NULL"
        )
        annual_has_year = _table_has_column(conn, "fund_annual_returns", "year")
        annual_has_return = _table_has_column(conn, "fund_annual_returns", "annual_return")
        fund_rows = conn.execute(
            f"""
            SELECT DISTINCT
                fcode,
                COALESCE(shortname, ''),
                COALESCE(ftype, ''),
                COALESCE(type_code, ''),
                COALESCE(estab_date, ''),
                {first_collected_select},
                has_holding_period,
                holding_days,
                is_buyable,
                {fullname_select},
                {holding_source_select},
                {holding_keywords_select}
            FROM funds
            WHERE COALESCE(type_code, '') IN ('31', '32')
               OR COALESCE(ftype, '') LIKE '债券型%'
            ORDER BY fcode
            """
        ).fetchall()
        fund_codes = [_normalize_code(row[0]) for row in fund_rows if _normalize_code(row[0])]
        annual_existing = {
            _normalize_code(row[0])
            for row in conn.execute("SELECT DISTINCT fcode FROM fund_annual_returns").fetchall()
            if _normalize_code(row[0])
        }
        annual_row_counts: dict[str, int] = {}
        annual_nonnull_counts: dict[str, int] = {}
        if annual_has_return:
            annual_row_counts = {
                _normalize_code(row[0]): int(row[1] or 0)
                for row in conn.execute(
                    """
                    SELECT fcode, COUNT(*) AS row_total
                    FROM fund_annual_returns
                    GROUP BY fcode
                    """
                ).fetchall()
                if _normalize_code(row[0])
            }
            annual_nonnull_counts = {
                _normalize_code(row[0]): int(row[1] or 0)
                for row in conn.execute(
                    """
                    SELECT fcode, SUM(CASE WHEN annual_return IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_total
                    FROM fund_annual_returns
                    GROUP BY fcode
                    """
                ).fetchall()
                if _normalize_code(row[0])
            }
        elif annual_has_year:
            annual_row_counts = {
                _normalize_code(row[0]): int(row[1] or 0)
                for row in conn.execute(
                    """
                    SELECT fcode, COUNT(*) AS row_total
                    FROM fund_annual_returns
                    GROUP BY fcode
                    """
                ).fetchall()
                if _normalize_code(row[0])
            }
            annual_nonnull_counts = {
                _normalize_code(row[0]): int(row[1] or 0)
                for row in conn.execute(
                    """
                    SELECT fcode, COUNT(*) AS row_total
                    FROM fund_annual_returns
                    GROUP BY fcode
                    """
                ).fetchall()
                if _normalize_code(row[0])
            }
        annual_latest_year: dict[str, int] = {}
        if include_stale_candidates and annual_has_year:
            annual_latest_year = {
                _normalize_code(row[0]): int(row[1])
                for row in conn.execute("SELECT fcode, MAX(year) FROM fund_annual_returns GROUP BY fcode").fetchall()
                if _normalize_code(row[0]) and row[1] is not None
            }
        annual_window_stats: dict[str, dict[str, Any]] = {}
        if annual_has_year:
            annual_window_stats = _load_annual_history_window_stats(
                conn,
                codes=fund_codes,
                min_years=_annual_feature_min_years(),
            )
        nav_window_first_nav_dates_by_code = _load_nav_history_window_first_nav_dates_by_year(
            conn,
            codes=fund_codes,
            min_years=_annual_feature_min_years(),
            archive_attach=nav_archive_attach,
        )
        # Both window queries group the same nonempty dates by fund/year.
        # Reuse the first-date groups instead of scanning the NAV union twice.
        nav_window_present_years_by_code = {
            code: sorted(first_dates)
            for code, first_dates in nav_window_first_nav_dates_by_code.items()
        }
        rank_has_date = _table_has_column(conn, "fund_rank_history", "rank_date")
        rank_latest_date: dict[str, str] = {}
        if rank_has_date:
            latest_rank_rows = read_latest_rank_dates(conn, rank_archive_attach)
            rank_existing = {_normalize_code(code) for code in latest_rank_rows if _normalize_code(code)}
            if include_stale_candidates:
                rank_latest_date = {
                    _normalize_code(code): str(value).strip()
                    for code, value in latest_rank_rows.items()
                    if _normalize_code(code) and value is not None and str(value).strip()
                }
        else:
            # Old minimal schemas without a date column still support
            # presence-only maintenance diagnostics.
            rank_source_sql = build_rank_history_union_source_sql(rank_archive_attach, columns=("fcode",))
            rank_existing = {
                _normalize_code(row[0])
                for row in conn.execute(
                    f"SELECT DISTINCT fcode FROM ({rank_source_sql})"
                ).fetchall()
                if _normalize_code(row[0])
            }
        return_rows = conn.execute(
            """
            SELECT fcode, latest_nav_date
            FROM fund_returns
            """
        ).fetchall()
        earliest_nav_dates = _load_earliest_nav_dates(
            conn,
            codes=fund_codes,
            archive_attach=nav_archive_attach,
        )
        if risk_end_date:
            risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
            latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
            risk_rows = conn.execute(
                f"""
                SELECT DISTINCT fcode
                FROM fund_risk_metrics risk
                WHERE risk.period = '1y'
                  AND risk.end_date = ?
                  AND {risk_ready_predicate}
                  AND {latest_rowid_predicate}
                """,
                (risk_end_date,),
            ).fetchall()
        else:
            risk_rows = []
    finally:
        if rank_archive_attach.get("attached"):
            detach_rank_history_archive(conn, alias=str(rank_archive_attach.get("alias") or "rank_archive_db"))
        if nav_archive_attach.get("attached"):
            detach_nav_history_archive(conn, alias=str(nav_archive_attach.get("alias") or "nav_archive_db"))
        conn.close()

    now_ts = datetime.now()
    fresh_nav_codes: set[str] = set()
    latest_nav_known_codes: set[str] = set()
    latest_nav_dates: dict[str, str] = {}
    for row in return_rows:
        code = _normalize_code(row[0])
        date_text = str(row[1] or "").strip()
        if not code:
            continue
        if date_text:
            latest_nav_known_codes.add(code)
            latest_nav_dates[code] = date_text
        if not date_text:
            continue
        try:
            nav_ts = datetime.fromisoformat(date_text)
        except ValueError:
            continue
        if (now_ts - nav_ts).days <= NAV_FRESHNESS_DAYS:
            fresh_nav_codes.add(code)

    risk_ready_codes = {
        _normalize_code(row[0])
        for row in risk_rows
        if _normalize_code(row[0])
    }

    universe: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    last_complete_year = datetime.now().date().year - 1
    rank_stale_days = _resolve_rank_stale_days(settings)
    stats = {
        "stored_bond_total": 0,
        "active_bond_total": 0,
        "buyable_bond_total": 0,
        "screenable_bond_total": 0,
        "annual_history_ready_total": 0,
        "annual_history_missing_total": 0,
        "annual_history_actionable_missing_total": 0,
        "annual_history_pending_maturity_total": 0,
        "annual_history_coverage_insufficient_total": 0,
        "rank_history_ready_total": 0,
        "rank_history_missing_total": 0,
        "rank_history_actionable_missing_total": 0,
        "rank_history_pending_maturity_total": 0,
        "both_history_missing_total": 0,
        "screenable_candidate_total": 0,
        "buyable_candidate_total": 0,
        "history_maturity_inferred_by_nav_total": 0,
        "history_metadata_unknown_allowed_total": 0,
        "history_metadata_blocked_total": 0,
        "annual_history_stale_total": 0,
        "rank_history_stale_total": 0,
    }

    for row in fund_rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        stats["stored_bond_total"] += 1
        estab_date = str(row[4] or "").strip() or None
        first_collected_at = str(row[5] or "").strip() or None
        is_buyable = normalize_buyability(row[8])
        runtime_excluded = code in runtime_excluded_codes
        active_universe = not runtime_excluded
        holding_period_blocked = holding_period_gate_reason(
            has_holding_period=row[6],
            holding_days=row[7],
            fund_name=combine_holding_period_names(row[1], row[9]),
            holding_period_source=row[10],
            holding_keywords=row[11],
            threshold_days=settings.exclude_holding_days_gt,
        ) is not None
        fresh_nav = code in fresh_nav_codes
        risk_ready = code in risk_ready_codes
        buyable = is_buyable
        screenable_universe = active_universe and fresh_nav and risk_ready and not holding_period_blocked
        publishable_priority = screenable_universe and buyable is True
        if active_universe:
            stats["active_bond_total"] += 1
        if buyable:
            stats["buyable_bond_total"] += 1
        if screenable_universe:
            stats["screenable_bond_total"] += 1
        annual_missing_raw = bool(annual_enabled and code not in annual_existing)
        annual_row_count = int(annual_row_counts.get(code) or 0)
        annual_nonnull_count = int(annual_nonnull_counts.get(code) or 0)
        latest_nav_known = code in latest_nav_known_codes
        earliest_nav_date = earliest_nav_dates.get(code)
        annual_feature_window_nav_present_years = list(nav_window_present_years_by_code.get(code) or [])
        annual_feature_window_mature = _annual_feature_window_mature_enough(
            estab_date,
            earliest_nav_date=earliest_nav_date,
            current_window_nav_present_years=annual_feature_window_nav_present_years,
            min_years=_annual_feature_min_years(),
        )
        annual_window_stat = annual_window_stats.get(code) or {}
        annual_feature_window_row_count = int(annual_window_stat.get("row_count") or 0)
        annual_feature_window_nonnull_count = int(annual_window_stat.get("nonnull_count") or 0)
        annual_feature_window_years = list(annual_window_stat.get("window_years") or [])
        if not annual_feature_window_years:
            annual_feature_window_years = _current_complete_calendar_years(min_years=_annual_feature_min_years())
        annual_feature_window_present_years = list(annual_window_stat.get("present_years") or [])
        annual_feature_window_nonnull_years = list(annual_window_stat.get("nonnull_years") or [])
        annual_feature_window_missing_years = [
            year for year in annual_feature_window_years if year not in annual_feature_window_present_years
        ]
        annual_feature_window_null_years = [
            year for year in annual_feature_window_present_years if year not in annual_feature_window_nonnull_years
        ]
        leading_present_year = annual_feature_window_present_years[0] if annual_feature_window_present_years else None
        annual_feature_window_leading_partial_null_year_gap = _annual_feature_window_has_leading_partial_null_year(
            annual_feature_window_years,
            annual_feature_window_present_years,
            annual_feature_window_nonnull_years,
            leading_present_year_first_nav_date=(
                (nav_window_first_nav_dates_by_code.get(code) or {}).get(int(leading_present_year))
                if leading_present_year is not None
                else None
            ),
        )
        annual_feature_window_recent_nav_tail_only = _is_recent_share_class_tail_window(
            annual_feature_window_years,
            annual_feature_window_nav_present_years,
        )
        annual_coverage_insufficient_raw = bool(
            annual_enabled
            and annual_has_return
            and not annual_missing_raw
            and annual_feature_window_mature
            and not annual_feature_window_leading_partial_null_year_gap
            and annual_feature_window_nonnull_count < _annual_feature_min_years()
        )
        rank_missing_raw = bool(rank_enabled and code not in rank_existing)
        history_mature, history_maturity_reason = _resolve_history_maturity(
            estab_date,
            latest_nav_known=latest_nav_known,
            first_collected_at=first_collected_at,
            earliest_nav_date=earliest_nav_date,
        )
        annual_history_mature = history_mature
        rank_history_mature = history_mature
        if history_maturity_reason == "nav_history_lower_bound_mature":
            stats["history_maturity_inferred_by_nav_total"] += 1
        elif history_maturity_reason in ("metadata_unknown_nav_history_too_short", "metadata_unknown_latest_nav_known"):
            stats["history_metadata_unknown_allowed_total"] += 1
        elif history_maturity_reason == "metadata_missing_no_latest_nav":
            stats["history_metadata_blocked_total"] += 1
        needs_annual = bool((annual_missing_raw or annual_coverage_insufficient_raw) and annual_history_mature)
        needs_rank = bool(rank_missing_raw and rank_history_mature)
        latest_annual_year = annual_latest_year.get(code)
        latest_rank_date = rank_latest_date.get(code)
        annual_stale = bool(
            include_stale_candidates
            and annual_enabled
            and annual_history_mature
            and latest_annual_year is not None
            and latest_annual_year < last_complete_year
        )
        rank_stale = False
        if include_stale_candidates and rank_enabled and rank_history_mature and latest_rank_date:
            try:
                rank_age_days = (datetime.now().date() - datetime.fromisoformat(latest_rank_date).date()).days
            except ValueError:
                rank_age_days = None
            rank_stale = bool(rank_age_days is not None and rank_age_days > rank_stale_days)
        if annual_enabled:
            if annual_missing_raw or annual_coverage_insufficient_raw or annual_feature_window_leading_partial_null_year_gap:
                stats["annual_history_missing_total"] += 1
                if annual_coverage_insufficient_raw:
                    stats["annual_history_coverage_insufficient_total"] += 1
                if needs_annual:
                    stats["annual_history_actionable_missing_total"] += 1
                elif history_maturity_reason in {
                    "estab_date_pending",
                    "nav_history_lower_bound_pending",
                    "recent_discovery_waiting_initial_nav",
                } or annual_feature_window_recent_nav_tail_only or annual_feature_window_leading_partial_null_year_gap:
                    stats["annual_history_pending_maturity_total"] += 1
            else:
                stats["annual_history_ready_total"] += 1
                if annual_stale:
                    stats["annual_history_stale_total"] += 1
        if rank_enabled:
            if rank_missing_raw:
                stats["rank_history_missing_total"] += 1
                if needs_rank:
                    stats["rank_history_actionable_missing_total"] += 1
                elif history_maturity_reason in {
                    "estab_date_pending",
                    "nav_history_lower_bound_pending",
                    "recent_discovery_waiting_initial_nav",
                }:
                    stats["rank_history_pending_maturity_total"] += 1
            else:
                stats["rank_history_ready_total"] += 1
                if rank_stale:
                    stats["rank_history_stale_total"] += 1

        issue_types: list[str] = []
        if needs_annual:
            issue_types.append("missing_annual_returns")
        if needs_rank:
            issue_types.append("missing_rank_history")
        if annual_stale:
            issue_types.append("stale_annual_returns")
        if rank_stale:
            issue_types.append("stale_rank_history")
        if needs_annual and needs_rank:
            stats["both_history_missing_total"] += 1

        if publishable_priority:
            priority_band = 0
            priority_label = "publishable_priority"
        elif screenable_universe:
            priority_band = 1
            priority_label = "screenable_priority"
        elif active_universe and buyable and not holding_period_blocked:
            priority_band = 2
            priority_label = "buyable_active_priority"
        elif active_universe and not holding_period_blocked:
            priority_band = 3
            priority_label = "active_priority"
        else:
            priority_band = 4
            priority_label = "deferred_priority"

        record = {
            "fcode": code,
            "shortname": row[1],
            "ftype": row[2],
            "type_code": row[3],
            "estab_date": estab_date,
            "first_collected_at": first_collected_at,
            "latest_nav_known": latest_nav_known,
            "latest_nav_date": latest_nav_dates.get(code),
            "earliest_nav_date": earliest_nav_date,
            "annual_history_mature_enough": annual_history_mature,
            "annual_feature_window_mature_enough": annual_feature_window_mature,
            "annual_feature_window_years": annual_feature_window_years,
            "annual_feature_window_row_count": annual_feature_window_row_count,
            "annual_feature_window_nonnull_count": annual_feature_window_nonnull_count,
            "annual_feature_window_present_years": annual_feature_window_present_years,
            "annual_feature_window_nonnull_years": annual_feature_window_nonnull_years,
            "annual_feature_window_missing_years": annual_feature_window_missing_years,
            "annual_feature_window_null_years": annual_feature_window_null_years,
            "annual_feature_window_nav_present_years": annual_feature_window_nav_present_years,
            "annual_feature_window_leading_partial_null_year_gap": annual_feature_window_leading_partial_null_year_gap,
            "annual_feature_window_recent_nav_tail_only": annual_feature_window_recent_nav_tail_only,
            "rank_history_mature_enough": rank_history_mature,
            "history_maturity_reason": history_maturity_reason,
            "latest_annual_year": latest_annual_year,
            "annual_row_count": annual_row_count,
            "annual_nonnull_count": annual_nonnull_count,
            "annual_history_coverage_insufficient_raw": annual_coverage_insufficient_raw,
            "latest_rank_date": latest_rank_date,
            "stale_annual_returns": annual_stale,
            "stale_rank_history": rank_stale,
            "active_universe": active_universe,
            "buyable": buyable,
            "fresh_nav": fresh_nav,
            "risk_ready": risk_ready,
            "holding_period_blocked": holding_period_blocked,
            "screenable_universe": screenable_universe,
            "publishable_priority": publishable_priority,
            "priority_band": priority_band,
            "priority_label": priority_label,
            "annual_history_missing_raw": annual_missing_raw,
            "rank_history_missing_raw": rank_missing_raw,
            "needs_annual_returns": needs_annual,
            "needs_rank_history": needs_rank,
            "issue_types": issue_types,
        }
        universe.append(record)
        if issue_types:
            if screenable_universe:
                stats["screenable_candidate_total"] += 1
            if buyable:
                stats["buyable_candidate_total"] += 1
            candidates.append(record)

    candidates.sort(key=_candidate_priority)
    universe.sort(key=lambda row: str(row.get("fcode") or ""))
    return universe, candidates, stats


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
    return ordered[:normalized_limit], normalized_start


def _resolve_next_rotation_index(
    before_candidates: list[dict[str, Any]],
    after_candidates: list[dict[str, Any]],
    *,
    start_index: int,
    limit: int,
) -> int:
    if not after_candidates:
        return 0
    if not before_candidates:
        return 0

    before_codes = [str(row.get("fcode") or "") for row in before_candidates if str(row.get("fcode") or "")]
    if not before_codes:
        return 0

    after_index_by_code = {
        str(row.get("fcode") or ""): index
        for index, row in enumerate(after_candidates)
        if str(row.get("fcode") or "")
    }
    normalized_start = max(0, int(start_index or 0)) % len(before_codes)
    raw_limit = int(limit or 0)
    normalized_limit = len(before_codes) if raw_limit <= 0 else max(1, min(raw_limit, len(before_codes)))
    anchor_index = (normalized_start + normalized_limit) % len(before_codes)

    for offset in range(len(before_codes)):
        code = before_codes[(anchor_index + offset) % len(before_codes)]
        if code in after_index_by_code:
            return int(after_index_by_code[code])
    return 0


def _resolve_stage_workers(collector: Any, workers_override: int | None = None) -> int:
    try:
        workers = int(workers_override if workers_override is not None else getattr(collector, "default_workers", 1) or 1)
    except (TypeError, ValueError):
        workers = 1
    return max(1, workers)


def _run_stage_concurrent(
    *,
    collector: Any,
    codes: list[str],
    collect_func: Any,
    stage: str,
    workers: int | None = None,
    log=lambda _message: None,
) -> tuple[list[str], list[dict[str, str]]]:
    if not codes:
        return [], []

    workers = _resolve_stage_workers(collector, workers)
    try:
        submit_batch_size = int(collector._resolve_submit_batch_size(workers, len(codes)))
    except Exception:
        submit_batch_size = min(len(codes), max(workers * 4, 200))
    submit_batch_size = max(1, submit_batch_size)

    success_codes: list[str] = []
    failed_items: list[dict[str, str]] = []
    log_collection = getattr(collector, "log_collection", None)

    def _log_failure(code: str, error_message: str) -> None:
        if not callable(log_collection):
            return
        try:
            log_collection(
                task_type=f"long_term_history_{stage}",
                fcode=code,
                status="failed",
                message=str(error_message or "")[:500],
            )
        except Exception:
            return

    def _consume_collector_outcome(code: str) -> dict[str, Any] | None:
        if not hasattr(collector, "consume_recent_outcome"):
            return None
        try:
            outcome = collector.consume_recent_outcome(code)
        except Exception:
            return None
        return outcome if isinstance(outcome, dict) else None

    for start in range(0, len(codes), submit_batch_size):
        code_batch = codes[start:start + submit_batch_size]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(collect_func, code): code for code in code_batch}
            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_codes.append(code)
                    else:
                        outcome = _consume_collector_outcome(code) or {}
                        error_message = str(outcome.get("status") or "collector_returned_false").strip() or "collector_returned_false"
                        failed_items.append(
                            {"fcode": code, "stage": stage, "error": error_message}
                        )
                        _log_failure(code, outcome.get("message") or error_message)
                        log(f"{'年度涨幅' if stage == 'annual_returns' else '同类排名'}历史回补未成功: {code}")
                except Exception as exc:
                    failed_items.append({"fcode": code, "stage": stage, "error": str(exc)})
                    _log_failure(code, str(exc))
                    log(f"{'年度涨幅' if stage == 'annual_returns' else '同类排名'}历史回补失败: {code} {exc}")

    return success_codes, failed_items


def _reset_collector_api_source_evidence(collector: Any, **context: Any) -> None:
    reset_func = getattr(collector, "reset_api_source_evidence", None)
    if not callable(reset_func):
        return
    try:
        reset_func(**context)
    except Exception:
        return


def _update_collector_api_source_evidence_context(collector: Any, **context: Any) -> None:
    update_func = getattr(collector, "update_api_source_evidence_context", None)
    if not callable(update_func):
        return
    try:
        update_func(**context)
    except Exception:
        return


def _collector_api_source_evidence_snapshot(collector: Any, **overrides: Any) -> dict[str, Any]:
    snapshot_func = getattr(collector, "get_api_source_evidence", None)
    if not callable(snapshot_func):
        return {}
    try:
        payload = snapshot_func(**overrides)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_long_term_history_api_source_evidence(
    *,
    annual_collector: Any,
    rank_collector: Any,
    annual_target_total: int,
    rank_target_total: int,
    annual_success_total: int,
    rank_success_total: int,
    annual_failed_total: int,
    rank_failed_total: int,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize real upstream API source evidence for long-term history maintenance."""
    annual_evidence = _collector_api_source_evidence_snapshot(
        annual_collector,
        target_count=int(annual_target_total or 0),
        success_count=int(annual_success_total or 0),
        failed_count=int(annual_failed_total or 0),
    )
    rank_evidence = _collector_api_source_evidence_snapshot(
        rank_collector,
        target_count=int(rank_target_total or 0),
        success_count=int(rank_success_total or 0),
        failed_count=int(rank_failed_total or 0),
    )
    source_entries = [
        item
        for item in (annual_evidence, rank_evidence)
        if isinstance(item, dict) and item
    ]
    total_request_count = sum(int(item.get("request_count") or 0) for item in source_entries)
    total_failed_response_count = sum(int(item.get("failed_response_count") or 0) for item in source_entries)
    total_transport_exception_count = sum(int(item.get("transport_exception_count") or 0) for item in source_entries)
    total_save_failed_count = sum(int(item.get("save_failed_count") or 0) for item in source_entries)
    return {
        "schema_version": 1,
        "task": "long-term-history-maintenance",
        "context": dict(context or {}),
        "annual_target_total": int(annual_target_total or 0),
        "rank_target_total": int(rank_target_total or 0),
        "annual_success_total": int(annual_success_total or 0),
        "rank_success_total": int(rank_success_total or 0),
        "annual_failed_total": int(annual_failed_total or 0),
        "rank_failed_total": int(rank_failed_total or 0),
        "total_request_count": total_request_count,
        "total_failed_response_count": total_failed_response_count,
        "total_transport_exception_count": total_transport_exception_count,
        "total_save_failed_count": total_save_failed_count,
        "sources": source_entries,
    }


def _normalize_failure_reason(error_value: Any) -> str:
    text = str(error_value or "").strip().lower()
    if not text:
        return "unknown_failure"
    if "invalid_datas_type" in text:
        return "invalid_datas_type"
    if "empty_period_records" in text:
        return "empty_period_records"
    if "empty_rank_records" in text:
        return "empty_rank_records"
    if "no_effective_annual_records" in text:
        return "no_effective_annual_records"
    if "no_effective_rank_records" in text:
        return "no_effective_rank_records"
    if "save_exception" in text:
        return "save_exception"
    if "request_failed" in text:
        return "request_failed"
    if "stale_annual_refresh_no_progress" in text:
        return "stale_annual_refresh_no_progress"
    if "stale_rank_refresh_no_progress" in text:
        return "stale_rank_refresh_no_progress"
    if "annual_history_gap_no_progress" in text:
        return "annual_history_gap_no_progress"
    if "rank_history_gap_no_progress" in text:
        return "rank_history_gap_no_progress"
    if "collector_returned_false" in text:
        return "collector_returned_false"
    return text.replace(" ", "_")


def _summarize_failed_items_by_stage(failed_items: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for item in failed_items or []:
        stage = str(item.get("stage") or "unknown").strip() or "unknown"
        reason = _normalize_failure_reason(item.get("error"))
        stage_bucket = summary.setdefault(
            stage,
            {"failed_total": 0, "reason_counts": {}, "samples_by_reason": {}},
        )
        stage_bucket["failed_total"] = int(stage_bucket.get("failed_total") or 0) + 1
        reason_counts = stage_bucket.setdefault("reason_counts", {})
        reason_counts[reason] = int(reason_counts.get(reason) or 0) + 1
        samples = stage_bucket.setdefault("samples_by_reason", {}).setdefault(reason, [])
        if len(samples) < AUDIT_SAMPLE_LIMIT:
            samples.append(
                {
                    "fcode": _normalize_code(item.get("fcode")),
                    "stage": stage,
                    "error": str(item.get("error") or "").strip(),
                }
            )
    return summary


def _classify_failure_reason_governance_action(reason: str) -> str:
    normalized = _normalize_failure_reason(reason)
    if normalized in {
        "empty_period_records",
        "empty_rank_records",
        "invalid_datas_type",
        "stale_annual_refresh_no_progress",
        "stale_rank_refresh_no_progress",
        "annual_history_gap_no_progress",
        "rank_history_gap_no_progress",
    }:
        return "observe_upstream_source"
    if normalized in {
        "request_failed",
    }:
        return "retry_locally_later"
    return "retry_locally_now"


def _build_failed_reason_governance_summary(failed_reason_summary: dict[str, Any]) -> dict[str, Any]:
    action_totals: dict[str, int] = {}
    stage_reason_actions: dict[str, Any] = {}
    for stage, stage_payload in (failed_reason_summary or {}).items():
        if not isinstance(stage_payload, dict):
            continue
        reason_counts = dict(stage_payload.get("reason_counts") or {})
        reason_actions: dict[str, Any] = {}
        for raw_reason, raw_total in reason_counts.items():
            reason = _normalize_failure_reason(raw_reason)
            failed_total = max(0, int(raw_total or 0))
            action = _classify_failure_reason_governance_action(reason)
            action_totals[action] = int(action_totals.get(action) or 0) + failed_total
            reason_actions[reason] = {
                "governance_action": action,
                "failed_total": failed_total,
            }
        if reason_actions:
            stage_reason_actions[str(stage)] = reason_actions
    return {
        "action_totals": action_totals,
        "stage_reason_actions": stage_reason_actions,
    }


def _classify_upstream_observation_subtype(failed_errors: list[str]) -> str:
    normalized_errors = {
        _normalize_failure_reason(error)
        for error in failed_errors
        if str(error or "").strip()
    }
    if not normalized_errors:
        return "unknown_observation_issue"
    if "invalid_datas_type" in normalized_errors:
        return "payload_shape_anomaly"
    if normalized_errors == {"empty_rank_records"}:
        return "rank_payload_empty"
    if normalized_errors == {"empty_period_records"}:
        return "annual_payload_empty"
    if normalized_errors.issubset({"empty_period_records", "empty_rank_records"}):
        return "dual_empty_payload"
    if normalized_errors.issubset({"stale_annual_refresh_no_progress", "stale_rank_refresh_no_progress"}):
        return "stale_refresh_no_progress"
    if normalized_errors.issubset({"annual_history_gap_no_progress", "rank_history_gap_no_progress"}):
        return "history_gap_no_progress"
    if normalized_errors == {"request_failed"}:
        return "request_failed"
    return "other_observe_upstream"


def _build_upstream_observation_summary(
    universe_map: dict[str, dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    pending_recovery_total = 0
    upstream_limited_total = 0
    upstream_limited_watchlist_total = 0
    stale_upstream_limited_watchlist_total = 0
    terminal_upstream_limited_watchlist_total = 0
    scope_filtered_upstream_limited_total = 0
    pending_reason_counts: dict[str, int] = {}
    limited_reason_counts: dict[str, int] = {}
    stale_limited_reason_counts: dict[str, int] = {}
    terminal_limited_reason_counts: dict[str, int] = {}
    scope_filtered_limited_reason_counts: dict[str, int] = {}
    pending_subtype_counts: dict[str, int] = {}
    upstream_limited_subtype_counts: dict[str, int] = {}
    stale_upstream_limited_subtype_counts: dict[str, int] = {}
    terminal_upstream_limited_subtype_counts: dict[str, int] = {}
    scope_filtered_upstream_limited_subtype_counts: dict[str, int] = {}
    pending_blocker_counts: dict[str, int] = {}
    upstream_limited_blocker_counts: dict[str, int] = {}
    stale_upstream_limited_blocker_counts: dict[str, int] = {}
    terminal_upstream_limited_blocker_counts: dict[str, int] = {}
    scope_filtered_upstream_limited_blocker_counts: dict[str, int] = {}
    pending_recovery_samples: list[dict[str, Any]] = []
    pending_retry_window_total = 0
    pending_retry_cooldown_total = 0
    pending_retry_ready_total = 0
    pending_next_retry_after_values: list[str] = []
    upstream_limited_watchlist_samples: list[dict[str, Any]] = []
    stale_upstream_limited_watchlist_samples: list[dict[str, Any]] = []
    terminal_upstream_limited_watchlist_samples: list[dict[str, Any]] = []
    scope_filtered_upstream_limited_samples: list[dict[str, Any]] = []
    pending_samples_by_subtype: dict[str, list[dict[str, Any]]] = {}
    upstream_limited_samples_by_subtype: dict[str, list[dict[str, Any]]] = {}
    stale_upstream_limited_samples_by_subtype: dict[str, list[dict[str, Any]]] = {}
    terminal_upstream_limited_samples_by_subtype: dict[str, list[dict[str, Any]]] = {}
    scope_filtered_upstream_limited_samples_by_subtype: dict[str, list[dict[str, Any]]] = {}

    for code, raw_record in sorted(_normalize_failure_backoff(failure_backoff).items()):
        row = dict(universe_map.get(code) or {})
        if not row:
            continue
        failed_errors = _split_failure_error_tokens(raw_record.get("last_error"))
        if not _all_failures_map_to_governance_action(failed_errors, "observe_upstream_source"):
            continue
        source_category = _classify_long_term_gap_source(row, raw_record)
        blocker_category = _classify_long_term_gap_blocker(row)
        action_category = _classify_long_term_gap_action(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
        )
        subtype = _classify_upstream_observation_subtype(failed_errors)
        sample = _build_auto_scope_sample(
            row,
            source_category=source_category,
            blocker_category=blocker_category,
            action_category=action_category,
            failure_record=raw_record,
        )
        nav_staleness_days = _resolve_nav_staleness_days(row.get("latest_nav_date"))
        is_terminal_stale_upstream_observation = bool(
            not row.get("fresh_nav")
            and not row.get("risk_ready")
            and (nav_staleness_days is None or nav_staleness_days > LONG_TERM_UPSTREAM_TERMINAL_STALE_NAV_DAYS)
        )
        is_stale_upstream_observation = bool(
            not row.get("fresh_nav")
            and not row.get("risk_ready")
            and nav_staleness_days is not None
            and nav_staleness_days > LONG_TERM_UPSTREAM_STALE_NAV_DAYS
            and not is_terminal_stale_upstream_observation
        )
        if bool(raw_record.get("upstream_limited")):
            upstream_limited_total += 1
            reason_key = str(source_category or "upstream_limited_other").strip() or "upstream_limited_other"
            if action_category == "scope_filtered_no_retry":
                scope_filtered_upstream_limited_total += 1
                scope_filtered_limited_reason_counts[reason_key] = int(
                    scope_filtered_limited_reason_counts.get(reason_key) or 0
                ) + 1
                scope_filtered_upstream_limited_subtype_counts[subtype] = int(
                    scope_filtered_upstream_limited_subtype_counts.get(subtype) or 0
                ) + 1
                scope_filtered_upstream_limited_blocker_counts[blocker_category] = int(
                    scope_filtered_upstream_limited_blocker_counts.get(blocker_category) or 0
                ) + 1
                if len(scope_filtered_upstream_limited_samples) < AUDIT_SAMPLE_LIMIT:
                    scope_filtered_upstream_limited_samples.append(sample)
                subtype_samples = scope_filtered_upstream_limited_samples_by_subtype.setdefault(subtype, [])
                if len(subtype_samples) < AUDIT_SAMPLE_LIMIT:
                    subtype_samples.append(sample)
            elif is_terminal_stale_upstream_observation:
                terminal_upstream_limited_watchlist_total += 1
                terminal_limited_reason_counts[reason_key] = int(
                    terminal_limited_reason_counts.get(reason_key) or 0
                ) + 1
                terminal_upstream_limited_subtype_counts[subtype] = int(
                    terminal_upstream_limited_subtype_counts.get(subtype) or 0
                ) + 1
                terminal_upstream_limited_blocker_counts[blocker_category] = int(
                    terminal_upstream_limited_blocker_counts.get(blocker_category) or 0
                ) + 1
                if len(terminal_upstream_limited_watchlist_samples) < AUDIT_SAMPLE_LIMIT:
                    terminal_upstream_limited_watchlist_samples.append(sample)
                subtype_samples = terminal_upstream_limited_samples_by_subtype.setdefault(subtype, [])
                if len(subtype_samples) < AUDIT_SAMPLE_LIMIT:
                    subtype_samples.append(sample)
            elif is_stale_upstream_observation:
                stale_upstream_limited_watchlist_total += 1
                stale_limited_reason_counts[reason_key] = int(
                    stale_limited_reason_counts.get(reason_key) or 0
                ) + 1
                stale_upstream_limited_subtype_counts[subtype] = int(
                    stale_upstream_limited_subtype_counts.get(subtype) or 0
                ) + 1
                stale_upstream_limited_blocker_counts[blocker_category] = int(
                    stale_upstream_limited_blocker_counts.get(blocker_category) or 0
                ) + 1
                if len(stale_upstream_limited_watchlist_samples) < AUDIT_SAMPLE_LIMIT:
                    stale_upstream_limited_watchlist_samples.append(sample)
                subtype_samples = stale_upstream_limited_samples_by_subtype.setdefault(subtype, [])
                if len(subtype_samples) < AUDIT_SAMPLE_LIMIT:
                    subtype_samples.append(sample)
            else:
                upstream_limited_watchlist_total += 1
                limited_reason_counts[reason_key] = int(limited_reason_counts.get(reason_key) or 0) + 1
                upstream_limited_subtype_counts[subtype] = int(upstream_limited_subtype_counts.get(subtype) or 0) + 1
                upstream_limited_blocker_counts[blocker_category] = int(
                    upstream_limited_blocker_counts.get(blocker_category) or 0
                ) + 1
                if len(upstream_limited_watchlist_samples) < AUDIT_SAMPLE_LIMIT:
                    upstream_limited_watchlist_samples.append(sample)
                subtype_samples = upstream_limited_samples_by_subtype.setdefault(subtype, [])
                if len(subtype_samples) < AUDIT_SAMPLE_LIMIT:
                    subtype_samples.append(sample)
        elif (
            action_category in {"retry_locally_now", "retry_locally_later"}
            and subtype in {"history_gap_no_progress", "stale_refresh_no_progress"}
        ):
            pending_recovery_total += 1
            reason_key = str(source_category or "pending_recovery_other").strip() or "pending_recovery_other"
            pending_reason_counts[reason_key] = int(pending_reason_counts.get(reason_key) or 0) + 1
            if source_category == "local_retry_window_pending":
                pending_retry_window_total += 1
            elif source_category == "local_retry_cooldown_pending":
                pending_retry_cooldown_total += 1
            elif source_category in {"local_retry_pending", "local_previous_failure_pending_recheck"}:
                pending_retry_ready_total += 1
            pending_subtype_counts[subtype] = int(pending_subtype_counts.get(subtype) or 0) + 1
            pending_blocker_counts[blocker_category] = int(pending_blocker_counts.get(blocker_category) or 0) + 1
            next_retry_after_text = str(sample.get("cooldown_next_retry_after") or "").strip()
            if next_retry_after_text:
                pending_next_retry_after_values.append(next_retry_after_text)
            if len(pending_recovery_samples) < AUDIT_SAMPLE_LIMIT:
                pending_recovery_samples.append(sample)
            subtype_samples = pending_samples_by_subtype.setdefault(subtype, [])
            if len(subtype_samples) < AUDIT_SAMPLE_LIMIT:
                subtype_samples.append(sample)

    pending_next_retry_after_values = sorted(
        value for value in pending_next_retry_after_values if value
    )

    return {
        "pending_recovery_total": pending_recovery_total,
        "pending_retry_window_total": pending_retry_window_total,
        "pending_retry_cooldown_total": pending_retry_cooldown_total,
        "pending_retry_ready_total": pending_retry_ready_total,
        "pending_next_retry_after_min": (
            pending_next_retry_after_values[0] if pending_next_retry_after_values else None
        ),
        "pending_next_retry_after_max": (
            pending_next_retry_after_values[-1] if pending_next_retry_after_values else None
        ),
        "upstream_limited_total": upstream_limited_total,
        "upstream_limited_watchlist_total": upstream_limited_watchlist_total,
        "stale_upstream_limited_watchlist_total": stale_upstream_limited_watchlist_total,
        "terminal_upstream_limited_watchlist_total": terminal_upstream_limited_watchlist_total,
        "scope_filtered_upstream_limited_total": scope_filtered_upstream_limited_total,
        "pending_reason_counts": pending_reason_counts,
        "upstream_limited_reason_counts": limited_reason_counts,
        "stale_upstream_limited_reason_counts": stale_limited_reason_counts,
        "terminal_upstream_limited_reason_counts": terminal_limited_reason_counts,
        "scope_filtered_upstream_limited_reason_counts": scope_filtered_limited_reason_counts,
        "pending_subtype_counts": pending_subtype_counts,
        "upstream_limited_subtype_counts": upstream_limited_subtype_counts,
        "stale_upstream_limited_subtype_counts": stale_upstream_limited_subtype_counts,
        "terminal_upstream_limited_subtype_counts": terminal_upstream_limited_subtype_counts,
        "scope_filtered_upstream_limited_subtype_counts": scope_filtered_upstream_limited_subtype_counts,
        "pending_blocker_counts": pending_blocker_counts,
        "upstream_limited_blocker_counts": upstream_limited_blocker_counts,
        "stale_upstream_limited_blocker_counts": stale_upstream_limited_blocker_counts,
        "terminal_upstream_limited_blocker_counts": terminal_upstream_limited_blocker_counts,
        "scope_filtered_upstream_limited_blocker_counts": scope_filtered_upstream_limited_blocker_counts,
        "pending_recovery_samples": pending_recovery_samples,
        "upstream_limited_watchlist_samples": upstream_limited_watchlist_samples,
        "stale_upstream_limited_watchlist_samples": stale_upstream_limited_watchlist_samples,
        "terminal_upstream_limited_watchlist_samples": terminal_upstream_limited_watchlist_samples,
        "scope_filtered_upstream_limited_samples": scope_filtered_upstream_limited_samples,
        "pending_samples_by_subtype": pending_samples_by_subtype,
        "upstream_limited_samples_by_subtype": upstream_limited_samples_by_subtype,
        "stale_upstream_limited_samples_by_subtype": stale_upstream_limited_samples_by_subtype,
        "terminal_upstream_limited_samples_by_subtype": terminal_upstream_limited_samples_by_subtype,
        "scope_filtered_upstream_limited_samples_by_subtype": scope_filtered_upstream_limited_samples_by_subtype,
    }


def _is_terminal_upstream_limited_runtime_exclusion_candidate(
    row: dict[str, Any],
    failure_record: dict[str, Any],
) -> bool:
    failed_errors = _split_failure_error_tokens(failure_record.get("last_error"))
    if not bool(failure_record.get("upstream_limited")):
        return False
    if not _all_failures_map_to_governance_action(failed_errors, "observe_upstream_source"):
        return False
    if bool(row.get("holding_period_blocked")):
        return False
    nav_staleness_days = _resolve_nav_staleness_days(row.get("latest_nav_date"))
    return bool(
        not row.get("fresh_nav")
        and not row.get("risk_ready")
        and (nav_staleness_days is None or nav_staleness_days > LONG_TERM_UPSTREAM_TERMINAL_STALE_NAV_DAYS)
    )


def _build_terminal_upstream_limited_runtime_exclusion_entries(
    universe_map: dict[str, dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for code, raw_record in sorted(_normalize_failure_backoff(failure_backoff).items()):
        row = dict(universe_map.get(code) or {})
        if not row:
            continue
        if not _is_terminal_upstream_limited_runtime_exclusion_candidate(row, raw_record):
            continue
        nav_staleness_days = _resolve_nav_staleness_days(row.get("latest_nav_date"))
        latest_nav_date = str(row.get("latest_nav_date") or "").strip() or None
        last_error = " | ".join(_split_failure_error_tokens(raw_record.get("last_error")))
        if nav_staleness_days is None:
            reason = (
                "长周期历史补源连续受上游限制，且 latest_nav_date 缺失，已进入终态停更排除"
            )
        else:
            reason = (
                f"长周期历史补源连续受上游限制，且最新净值已停更 {nav_staleness_days} 天，"
                "已进入终态停更排除"
            )
        if last_error:
            reason = f"{reason}；最近失败={last_error}"
        entries.append({
            "fcode": code,
            "shortname": str(row.get("shortname") or "").strip() or None,
            "enabled": True,
            "reason": reason,
            "source": "long_term_history_maintenance",
            "latest_nav_date": latest_nav_date,
            "nav_staleness_days": nav_staleness_days,
        })
    return entries


def _sync_terminal_upstream_limited_runtime_exclusions(
    universe_map: dict[str, dict[str, Any]],
    failure_backoff: dict[str, dict[str, Any]],
    *,
    allow_persist: bool = True,
) -> dict[str, Any]:
    entries = _build_terminal_upstream_limited_runtime_exclusion_entries(
        universe_map,
        failure_backoff,
    )
    if not allow_persist:
        return {
            "rules_path": str(DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH),
            "excluded_total": len(entries),
            "samples": entries[:AUDIT_SAMPLE_LIMIT],
            "persist_skipped": True,
            "persist_skip_reason": "non_canonical_runtime_context",
        }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "rule_group": "terminal_upstream_limited_high_confidence",
        "total_count": len(entries),
        "entries": entries,
    }
    _write_json(DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH, payload)
    clear_fund_exclusion_rules_cache()
    return {
        "rules_path": str(DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH),
        "excluded_total": len(entries),
        "samples": entries[:AUDIT_SAMPLE_LIMIT],
    }


def _build_long_term_coverage_progress(
    before_stats: dict[str, Any],
    after_stats: dict[str, Any],
) -> dict[str, Any]:
    before = dict(before_stats or {})
    after = dict(after_stats or {})

    def _delta(key: str) -> int:
        return int(after.get(key) or 0) - int(before.get(key) or 0)

    def _ratio(numerator_key: str, denominator_key: str, source: dict[str, Any]) -> float:
        denominator = int(source.get(denominator_key) or 0)
        if denominator <= 0:
            return 0.0
        return round((int(source.get(numerator_key) or 0) / denominator), 6)

    return {
        "annual_history_ready_delta": _delta("annual_history_ready_total"),
        "rank_history_ready_delta": _delta("rank_history_ready_total"),
        "annual_actionable_missing_delta": _delta("annual_history_actionable_missing_total"),
        "rank_actionable_missing_delta": _delta("rank_history_actionable_missing_total"),
        "screenable_candidate_delta": _delta("screenable_candidate_total"),
        "buyable_candidate_delta": _delta("buyable_candidate_total"),
        "before": {
            "annual_history_ready_total": int(before.get("annual_history_ready_total") or 0),
            "rank_history_ready_total": int(before.get("rank_history_ready_total") or 0),
            "annual_history_actionable_missing_total": int(before.get("annual_history_actionable_missing_total") or 0),
            "rank_history_actionable_missing_total": int(before.get("rank_history_actionable_missing_total") or 0),
            "screenable_candidate_total": int(before.get("screenable_candidate_total") or 0),
            "buyable_candidate_total": int(before.get("buyable_candidate_total") or 0),
            "annual_history_ready_ratio": _ratio("annual_history_ready_total", "stored_bond_total", before),
            "rank_history_ready_ratio": _ratio("rank_history_ready_total", "stored_bond_total", before),
        },
        "after": {
            "annual_history_ready_total": int(after.get("annual_history_ready_total") or 0),
            "rank_history_ready_total": int(after.get("rank_history_ready_total") or 0),
            "annual_history_actionable_missing_total": int(after.get("annual_history_actionable_missing_total") or 0),
            "rank_history_actionable_missing_total": int(after.get("rank_history_actionable_missing_total") or 0),
            "screenable_candidate_total": int(after.get("screenable_candidate_total") or 0),
            "buyable_candidate_total": int(after.get("buyable_candidate_total") or 0),
            "annual_history_ready_ratio": _ratio("annual_history_ready_total", "stored_bond_total", after),
            "rank_history_ready_ratio": _ratio("rank_history_ready_total", "stored_bond_total", after),
        },
    }


def load_latest_long_term_history_maintenance_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "long_term_history_maintenance"
    if not artifact_dir.exists():
        return None, None
    files = sorted(glob_json_artifacts(artifact_dir, "long_term_history_maintenance_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = read_task_audit(path)
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def run_long_term_history_maintenance(
    settings: Settings,
    *,
    log=lambda _message: None,
    target_codes: list[str] | None = None,
    limit: int | None = None,
    annual_only: bool = False,
    rank_only: bool = False,
    force_update: bool = False,
    refresh_stale: bool = False,
) -> dict[str, Any]:
    if annual_only and rank_only:
        raise ValueError("annual_only 和 rank_only 不能同时为 true")

    annual_enabled = not rank_only
    rank_enabled = not annual_only
    ctx = _build_context(settings)
    generated_at = datetime.now().isoformat(timespec="seconds")
    log("开始执行债基长期稳定性历史审计与回补")

    local_nav_annual_backfill_summary = {
        "scanned_null_year_rows": 0,
        "eligible_null_year_rows": 0,
        "scanned_missing_window_year_rows": 0,
        "eligible_missing_window_year_rows": 0,
        "backfilled_missing_window_year_row_count": 0,
        "backfilled_row_count": 0,
        "backfilled_codes": [],
        "sample_rows": [],
        "skipped_rows": [],
    }

    universe, candidates, stats = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=bool(refresh_stale),
    )
    semantic_null_year_codes = {
        _normalize_code(row.get("fcode"))
        for row in universe
        if _normalize_code(row.get("fcode")) and _is_upstream_semantic_annual_null_year_gap(row)
    }
    missing_window_years_by_code = {
        _normalize_code(row.get("fcode")): list(row.get("annual_feature_window_missing_years") or [])
        for row in universe
        if _normalize_code(row.get("fcode"))
        and bool(row.get("needs_annual_returns"))
        and not _is_upstream_semantic_annual_null_year_gap(row)
        and list(row.get("annual_feature_window_missing_years") or [])
    }
    if annual_enabled and (semantic_null_year_codes or missing_window_years_by_code):
        local_nav_backfill_eligible_codes = set(semantic_null_year_codes) | {
            code for code in missing_window_years_by_code if _normalize_code(code)
        }
        local_nav_annual_backfill_summary = _backfill_semantic_null_annual_returns_from_nav(
            settings,
            eligible_codes=local_nav_backfill_eligible_codes,
            target_codes=target_codes,
            eligible_missing_window_years_by_code=missing_window_years_by_code,
            log=log,
        )
        universe, candidates, stats = _read_bond_universe(
            settings,
            settings.database_path,
            annual_enabled=annual_enabled,
            rank_enabled=rank_enabled,
            include_stale_candidates=bool(refresh_stale),
        )
    universe_map = {row["fcode"]: row for row in universe}
    candidate_map = {row["fcode"]: row for row in candidates}
    selection_pool = universe if force_update else candidates
    state_before = _load_maintenance_state(settings)
    empty_payload_retry_days = _resolve_empty_payload_retry_days(settings)
    empty_payload_upstream_limited_failures = _resolve_empty_payload_upstream_limited_failures(settings)
    stale_refresh_retry_days = _resolve_stale_refresh_retry_days(settings)
    stale_refresh_upstream_limited_failures = _resolve_stale_refresh_upstream_limited_failures(settings)
    failure_backoff_before = _hydrate_failure_backoff_policy(
        state_before.get("failure_backoff") or {},
        empty_payload_retry_days=empty_payload_retry_days,
        stale_refresh_retry_days=stale_refresh_retry_days,
        empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
        stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
    )
    enable_failure_backoff = not force_update and annual_enabled and rank_enabled
    apply_failure_backoff_selection = bool(enable_failure_backoff and not target_codes)
    automatic_scope_applied = bool(enable_failure_backoff and not force_update and not target_codes)
    effective_selection_pool = selection_pool
    automatic_scope_candidate_codes: set[str] = set()
    auto_scope_summary: dict[str, Any] = {
        "applied": False,
        "candidate_total": len(selection_pool),
        "retry_locally_now_total": len(selection_pool),
        "excluded_total": 0,
        "excluded_upstream_limited_total": 0,
        "excluded_retry_later_total": 0,
        "excluded_scope_filtered_total": 0,
        "excluded_scope_not_ready_total": 0,
        "excluded_pending_maturity_total": 0,
        "excluded_metadata_blocked_total": 0,
        "excluded_other_total": 0,
        "non_upstream_excluded_total": 0,
        "reason_counts": {},
        "samples_by_reason": {},
    }
    if automatic_scope_applied:
        automatic_scope_candidates, auto_scope_summary = _narrow_automatic_selection_pool(
            selection_pool,
            failure_backoff_before,
        )
        automatic_scope_candidate_codes = {
            _normalize_code(row.get("fcode"))
            for row in automatic_scope_candidates
            if _normalize_code(row.get("fcode"))
        }
    governance_queue_payload = (
        load_final_selection_governance_queue(settings)
        if automatic_scope_applied
        else {
            "available": False,
            "reason": "automatic_scope_not_applied",
            "codes_by_action": {},
        }
    )
    governance_priority_codes = (
        {
            _normalize_code(code)
            for code in (
                (governance_queue_payload.get("codes_by_action") or {}).get("backfill_long_term_features")
                or []
            )
            if _normalize_code(code)
        }
        if automatic_scope_applied
        else set()
    )
    current_published_priority_codes: set[str] = set()
    current_published_priority_summary: dict[str, Any] = {
        "available": False,
        "truth_source": "publication_registry_current_published_report_filtered_csv",
        "reason": "automatic_scope_not_applied",
        "code_count": 0,
    }
    if automatic_scope_applied:
        current_published_priority_codes, current_published_priority_summary = _load_current_published_formal_codes(
            settings
        )
    cooldown_skipped_before: list[dict[str, Any]] = []
    failure_backoff_working = failure_backoff_before
    if apply_failure_backoff_selection:
        effective_selection_pool, cooldown_skipped_before, failure_backoff_working = _partition_candidates_by_failure_backoff(
            selection_pool,
            failure_backoff_before,
            consume_skip=True,
        )
    if automatic_scope_applied:
        effective_selection_pool = [
            row
            for row in effective_selection_pool
            if _normalize_code(row.get("fcode")) in automatic_scope_candidate_codes
        ]
        effective_selection_pool = _prioritize_current_published_and_governance_candidates(
            effective_selection_pool,
            current_published_priority_codes,
            governance_priority_codes,
        )
    selected_candidates = selection_pool
    rotation_start_index = int(state_before.get("next_index") or 0)
    configured_limit = int(limit or 0)
    effective_limit = configured_limit
    adaptive_limit_summary: dict[str, Any] = {
        "applied": False,
        "reason": "not_evaluated",
        "configured_limit": configured_limit,
        "effective_limit": configured_limit,
        "adaptive_limit": _resolve_high_priority_stale_limit(settings),
        "backlog_trigger": _resolve_high_priority_stale_backlog_trigger(settings),
        "high_priority_stale_candidate_total": 0,
        "publishable_priority_stale_candidate_total": 0,
        "screenable_priority_stale_candidate_total": 0,
    }

    if target_codes:
        normalized_targets = [_normalize_code(code) for code in (target_codes or []) if _normalize_code(code)]
        selected_candidates = []
        for code in normalized_targets:
            row = universe_map.get(code) if force_update else candidate_map.get(code)
            if row:
                selected_candidates.append(row)
    elif effective_selection_pool:
        effective_limit, adaptive_limit_summary = _resolve_adaptive_high_priority_stale_limit(
            settings,
            configured_limit=configured_limit,
            refresh_stale=bool(refresh_stale),
            automatic_scope_applied=bool(automatic_scope_applied),
            candidates=effective_selection_pool,
        )
        selected_candidates, rotation_start_index = _rotate_candidates(
            effective_selection_pool,
            rotation_start_index,
            effective_limit,
        )
    else:
        selected_candidates = []

    annual_target_codes = [
        row["fcode"]
        for row in selected_candidates
        if annual_enabled and bool(force_update or row.get("needs_annual_returns") or (refresh_stale and row.get("stale_annual_returns")))
    ]
    rank_target_codes = [
        row["fcode"]
        for row in selected_candidates
        if rank_enabled and bool(force_update or row.get("needs_rank_history") or (refresh_stale and row.get("stale_rank_history")))
    ]

    annual_target_total = len(annual_target_codes)
    rank_target_total = len(rank_target_codes)
    evidence_context = {
        "collection_mode": "long_term_history_maintenance",
        "force_update": bool(force_update),
        "refresh_stale": bool(refresh_stale),
        "automatic_scope_applied": bool(automatic_scope_applied),
    }
    _reset_collector_api_source_evidence(
        ctx.annual_collector,
        target_count=annual_target_total,
        **evidence_context,
    )
    _reset_collector_api_source_evidence(
        ctx.rank_collector,
        target_count=rank_target_total,
        **evidence_context,
    )

    annual_success_codes, annual_failed_items = _run_stage_concurrent(
        collector=ctx.annual_collector,
        codes=annual_target_codes,
        collect_func=ctx.annual_collector.collect_fund_annual_returns,
        stage="annual_returns",
        log=log,
    )
    rank_success_codes, rank_failed_items = _run_stage_concurrent(
        collector=ctx.rank_collector,
        codes=rank_target_codes,
        collect_func=ctx.rank_collector.collect_fund_rank_history,
        stage="rank_history",
        log=log,
    )
    failed_items: list[dict[str, str]] = annual_failed_items + rank_failed_items

    post_universe, post_candidates, post_stats = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=bool(refresh_stale),
    )
    post_universe_map = {row["fcode"]: row for row in post_universe}
    annual_success_codes, rank_success_codes, failed_items = _reconcile_stale_refresh_progress(
        selected_candidates=selected_candidates,
        annual_success_codes=annual_success_codes,
        rank_success_codes=rank_success_codes,
        failed_items=failed_items,
        post_universe_map=post_universe_map,
    )
    failed_reason_summary = _summarize_failed_items_by_stage(failed_items)
    failed_reason_governance_summary = _build_failed_reason_governance_summary(failed_reason_summary)
    coverage_progress = _build_long_term_coverage_progress(stats, post_stats)
    annual_failed_total = max(0, annual_target_total - len(annual_success_codes))
    rank_failed_total = max(0, rank_target_total - len(rank_success_codes))
    _update_collector_api_source_evidence_context(
        ctx.annual_collector,
        success_count=len(annual_success_codes),
        failed_count=annual_failed_total,
    )
    _update_collector_api_source_evidence_context(
        ctx.rank_collector,
        success_count=len(rank_success_codes),
        failed_count=rank_failed_total,
    )
    api_source_evidence = build_long_term_history_api_source_evidence(
        annual_collector=ctx.annual_collector,
        rank_collector=ctx.rank_collector,
        annual_target_total=annual_target_total,
        rank_target_total=rank_target_total,
        annual_success_total=len(annual_success_codes),
        rank_success_total=len(rank_success_codes),
        annual_failed_total=annual_failed_total,
        rank_failed_total=rank_failed_total,
        context=evidence_context,
    )
    governance_queue_consumption_audit = _build_governance_queue_consumption_audit(
        queue_payload=governance_queue_payload,
        governance_priority_codes=governance_priority_codes,
        selection_pool=selection_pool,
        effective_selection_pool=effective_selection_pool,
        selected_candidates=selected_candidates,
        annual_target_codes=annual_target_codes,
        rank_target_codes=rank_target_codes,
        annual_success_codes=annual_success_codes,
        rank_success_codes=rank_success_codes,
        failed_items=failed_items,
    )
    current_published_priority_audit = _build_current_published_priority_selection_audit(
        current_published_summary=current_published_priority_summary,
        current_published_codes=current_published_priority_codes,
        effective_selection_pool=effective_selection_pool,
        selected_candidates=selected_candidates,
        annual_target_codes=annual_target_codes,
        rank_target_codes=rank_target_codes,
        annual_success_codes=annual_success_codes,
        rank_success_codes=rank_success_codes,
        failed_items=failed_items,
    )

    next_rotation_index = None
    post_candidate_codes = {
        _normalize_code(row.get("fcode"))
        for row in post_candidates
        if _normalize_code(row.get("fcode"))
    }
    normalized_target_codes = {
        _normalize_code(code)
        for code in (target_codes or [])
        if _normalize_code(code)
    }
    if target_codes:
        failure_backoff_after = _normalize_failure_backoff(failure_backoff_working)
    else:
        failure_backoff_after = _prune_failure_backoff(
            failure_backoff_working,
            candidate_codes=post_candidate_codes,
        )
    effective_post_selection_pool = post_universe if force_update else post_candidates
    cooldown_skipped_after: list[dict[str, Any]] = []
    if enable_failure_backoff:
        if target_codes:
            failure_backoff_after = _merge_targeted_failure_backoff_update(
                failure_backoff_working,
                target_codes=normalized_target_codes,
                selected_candidates=selected_candidates,
                failed_items=failed_items,
                candidate_codes_after=post_candidate_codes,
                failed_at=generated_at,
                empty_payload_retry_days=empty_payload_retry_days,
                stale_refresh_retry_days=stale_refresh_retry_days,
                empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
                stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
            )
        else:
            failure_backoff_after = _update_failure_backoff_after_run(
                failure_backoff_working,
                selected_candidates=selected_candidates,
                failed_items=failed_items,
                candidate_codes_after=post_candidate_codes,
                failed_at=generated_at,
                empty_payload_retry_days=empty_payload_retry_days,
                stale_refresh_retry_days=stale_refresh_retry_days,
                empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
                stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
            )
    if not target_codes:
        if enable_failure_backoff:
            effective_post_selection_pool, cooldown_skipped_after, failure_backoff_after = _partition_candidates_by_failure_backoff(
                post_candidates,
                failure_backoff_after,
                consume_skip=False,
            )
            if automatic_scope_applied and not force_update:
                automatic_scope_post_candidates, _ = _narrow_automatic_selection_pool(
                    post_candidates,
                    failure_backoff_after,
                )
                automatic_scope_post_codes = {
                    _normalize_code(row.get("fcode"))
                    for row in automatic_scope_post_candidates
                    if _normalize_code(row.get("fcode"))
                }
                effective_post_selection_pool = [
                    row
                    for row in effective_post_selection_pool
                    if _normalize_code(row.get("fcode")) in automatic_scope_post_codes
                ]
                effective_post_selection_pool = _prioritize_current_published_and_governance_candidates(
                    effective_post_selection_pool,
                    current_published_priority_codes,
                    governance_priority_codes,
                )
        if force_update:
            next_rotation_index = (
                (rotation_start_index + len(selected_candidates)) % len(effective_post_selection_pool)
                if effective_post_selection_pool
                else 0
            )
        else:
            next_rotation_index = _resolve_next_rotation_index(
                effective_selection_pool,
                effective_post_selection_pool,
                start_index=rotation_start_index,
                limit=effective_limit,
            )
        _save_maintenance_state(
            settings,
            next_index=int(next_rotation_index or 0),
            failure_backoff=failure_backoff_after,
        )
    elif enable_failure_backoff:
        _save_maintenance_state(
            settings,
            next_index=int(state_before.get("next_index") or 0),
            failure_backoff=failure_backoff_after,
        )

    upstream_observation_summary = _build_upstream_observation_summary(
        post_universe_map,
        failure_backoff_after,
    )
    priority_blocked_summary = _build_priority_blocked_summary(
        post_universe,
        failure_backoff_after,
    )
    canonical_database_path = None
    try:
        canonical_database_path = Path(settings.project_root) / "backend" / "data" / "bond_funds.db"
    except Exception:
        canonical_database_path = None
    runtime_db_path = None
    try:
        runtime_db_path = Path(settings.database_path)
    except Exception:
        runtime_db_path = None
    allow_terminal_exclusion_persist = bool(
        canonical_database_path is not None
        and runtime_db_path is not None
        and runtime_db_path.resolve() == canonical_database_path.resolve()
    )
    terminal_upstream_runtime_exclusion = _sync_terminal_upstream_limited_runtime_exclusions(
        post_universe_map,
        failure_backoff_after,
        allow_persist=allow_terminal_exclusion_persist,
    )

    summary = {
        "generated_at": generated_at,
        "status": "success",
        "database_path": str(settings.database_path),
        "candidate_total": len(effective_selection_pool),
        "selection": {
            "requested_target_codes": [_normalize_code(code) for code in (target_codes or []) if _normalize_code(code)],
            "limit": limit,
            "effective_limit": effective_limit,
            "force_update": bool(force_update),
            "refresh_stale": bool(refresh_stale),
            "annual_only": bool(annual_only),
            "rank_only": bool(rank_only),
            "failure_backoff_enabled": bool(enable_failure_backoff),
            "candidate_total": len(effective_selection_pool),
            "selection_pool_total": len(selection_pool),
            "effective_selection_pool_total": len(effective_selection_pool),
            "governance_queue_priority_enabled": bool(governance_priority_codes),
            "governance_queue_available": bool(governance_queue_payload.get("available")),
            "governance_queue_reason": str(governance_queue_payload.get("reason") or "").strip() or None,
            "governance_queue_artifact_project_path": (
                str(governance_queue_payload.get("artifact_project_path") or "").strip() or None
            ),
            "current_published_priority_enabled": bool(
                current_published_priority_audit.get("priority_enabled")
            ),
            "current_published_priority_code_total": int(
                current_published_priority_audit.get("code_count") or 0
            ),
            "current_published_priority_effective_candidate_total": int(
                current_published_priority_audit.get("effective_pool_hit_total") or 0
            ),
            "current_published_priority_selected_total": int(
                current_published_priority_audit.get("selected_hit_total") or 0
            ),
            "current_published_priority_reason": (
                str(current_published_priority_audit.get("reason") or "").strip() or None
            ),
            "governance_queue_priority_candidate_total": int(
                sum(
                    1
                    for row in effective_selection_pool
                    if _normalize_code(row.get("fcode")) in governance_priority_codes
                )
            ),
            "governance_queue_priority_codes_sample": [
                _normalize_code(row.get("fcode"))
                for row in effective_selection_pool
                if _normalize_code(row.get("fcode")) in governance_priority_codes
            ][:AUDIT_SAMPLE_LIMIT],
            "automatic_scope_applied": bool(automatic_scope_applied),
            "automatic_scope_candidate_total": int(auto_scope_summary.get("candidate_total") or 0),
            "automatic_scope_retry_locally_now_total": int(auto_scope_summary.get("retry_locally_now_total") or 0),
            "high_priority_stale_candidate_total": int(
                adaptive_limit_summary.get("high_priority_stale_candidate_total") or 0
            ),
            "publishable_priority_stale_candidate_total": int(
                adaptive_limit_summary.get("publishable_priority_stale_candidate_total") or 0
            ),
            "screenable_priority_stale_candidate_total": int(
                adaptive_limit_summary.get("screenable_priority_stale_candidate_total") or 0
            ),
            "adaptive_limit_applied": bool(adaptive_limit_summary.get("applied")),
            "adaptive_limit_reason": str(adaptive_limit_summary.get("reason") or "").strip() or None,
            "adaptive_limit_backlog_trigger": int(adaptive_limit_summary.get("backlog_trigger") or 0),
            "adaptive_limit_target": int(adaptive_limit_summary.get("adaptive_limit") or 0),
            "automatic_scope_excluded_total": int(auto_scope_summary.get("excluded_total") or 0),
            "automatic_scope_excluded_upstream_limited_total": int(auto_scope_summary.get("excluded_upstream_limited_total") or 0),
            "automatic_scope_excluded_retry_later_total": int(auto_scope_summary.get("excluded_retry_later_total") or 0),
            "automatic_scope_excluded_scope_filtered_total": int(auto_scope_summary.get("excluded_scope_filtered_total") or 0),
            "automatic_scope_excluded_scope_not_ready_total": int(auto_scope_summary.get("excluded_scope_not_ready_total") or 0),
            "automatic_scope_excluded_pending_maturity_total": int(auto_scope_summary.get("excluded_pending_maturity_total") or 0),
            "automatic_scope_excluded_metadata_blocked_total": int(auto_scope_summary.get("excluded_metadata_blocked_total") or 0),
            "automatic_scope_excluded_other_total": int(auto_scope_summary.get("excluded_other_total") or 0),
            "automatic_scope_non_upstream_excluded_total": int(auto_scope_summary.get("non_upstream_excluded_total") or 0),
            "automatic_scope_reason_counts": dict(auto_scope_summary.get("reason_counts") or {}),
            "automatic_scope_samples_by_reason": dict(auto_scope_summary.get("samples_by_reason") or {}),
            "selected_candidate_total": len(selected_candidates),
            "cooldown_skipped_candidate_total": len(cooldown_skipped_before),
            "cooldown_empty_payload_candidate_total": int(
                sum(
                    1
                    for row in cooldown_skipped_before
                    if bool(row.get("cooldown_next_retry_window_active"))
                    and str(row.get("cooldown_last_error") or "").strip() in {
                        "empty_period_records",
                        "empty_rank_records",
                        "empty_period_records | empty_rank_records",
                    }
                )
            ),
            "cooldown_upstream_limited_candidate_total": int(
                sum(1 for row in cooldown_skipped_before if bool(row.get("cooldown_upstream_limited")))
            ),
            "cooldown_stale_no_progress_upstream_limited_candidate_total": int(
                sum(
                    1
                    for row in cooldown_skipped_before
                    if bool(row.get("cooldown_upstream_limited"))
                    and _is_stale_refresh_no_progress_failure(
                        _split_failure_error_tokens(row.get("cooldown_last_error"))
                    )
                )
            ),
            "cooldown_skipped_samples": cooldown_skipped_before[:AUDIT_SAMPLE_LIMIT],
            "rotation_state_before": _summarize_maintenance_state(state_before),
            "rotation_state_after": _summarize_maintenance_state(
                {
                    "next_index": next_rotation_index,
                    "updated_at": generated_at,
                    "failure_backoff": failure_backoff_after,
                }
            )
            if not target_codes
            else None,
            "rotation_next_index": next_rotation_index,
        },
        "audit_before": {
            **stats,
            "candidate_total": len(candidates),
            "candidate_samples": candidates[:AUDIT_SAMPLE_LIMIT],
        },
        "maintenance_result": {
            "candidate_total": len(effective_selection_pool),
            "processed_total": len(selected_candidates),
            "selected_publishable_priority_total": int(sum(1 for row in selected_candidates if bool(row.get("publishable_priority")))),
            "selected_screenable_total": int(sum(1 for row in selected_candidates if bool(row.get("screenable_universe")))),
            "local_nav_annual_backfill_summary": local_nav_annual_backfill_summary,
            "annual_target_total": annual_target_total,
            "rank_target_total": rank_target_total,
            "annual_success_total": len(annual_success_codes),
            "rank_success_total": len(rank_success_codes),
            "annual_failed_total": annual_failed_total,
            "rank_failed_total": rank_failed_total,
            "api_source_evidence": api_source_evidence,
            "cooldown_active_after_total": len(cooldown_skipped_after),
            "cooldown_empty_payload_active_after_total": int(
                sum(
                    1
                    for row in cooldown_skipped_after
                    if bool(row.get("cooldown_next_retry_window_active"))
                    and str(row.get("cooldown_last_error") or "").strip() in {
                        "empty_period_records",
                        "empty_rank_records",
                        "empty_period_records | empty_rank_records",
                    }
                )
            ),
            "cooldown_upstream_limited_active_after_total": int(
                sum(1 for row in cooldown_skipped_after if bool(row.get("cooldown_upstream_limited")))
            ),
            "cooldown_stale_no_progress_upstream_limited_active_after_total": int(
                sum(
                    1
                    for row in cooldown_skipped_after
                    if bool(row.get("cooldown_upstream_limited"))
                    and _is_stale_refresh_no_progress_failure(
                        _split_failure_error_tokens(row.get("cooldown_last_error"))
                    )
                )
            ),
            "stale_refresh_no_progress_failed_total": int(
                sum(
                    1
                    for item in failed_items
                    if str(item.get("error") or "").strip() in {
                        "stale_annual_refresh_no_progress",
                        "stale_rank_refresh_no_progress",
                    }
                )
            ),
            "annual_success_codes_sample": annual_success_codes[:AUDIT_SAMPLE_LIMIT],
            "rank_success_codes_sample": rank_success_codes[:AUDIT_SAMPLE_LIMIT],
            "failed_items_sample": failed_items[:AUDIT_SAMPLE_LIMIT],
            "failed_reason_summary": failed_reason_summary,
            "failed_reason_governance_summary": failed_reason_governance_summary,
            "upstream_observation_summary": upstream_observation_summary,
            "priority_blocked_summary": priority_blocked_summary,
            "current_published_priority_audit": current_published_priority_audit,
            "governance_queue_consumption_audit": governance_queue_consumption_audit,
            "terminal_upstream_limited_runtime_exclusion": terminal_upstream_runtime_exclusion,
            "coverage_progress": coverage_progress,
            "cooldown_active_after_samples": cooldown_skipped_after[:AUDIT_SAMPLE_LIMIT],
            "upstream_limited_selected_total": int(
                sum(
                    1
                    for row in selected_candidates
                    if bool((failure_backoff_before.get(_normalize_code(row.get("fcode"))) or {}).get("upstream_limited"))
                )
            ),
            "upstream_limited_selected_samples": [
                _build_auto_scope_sample(
                    row,
                    source_category=_classify_long_term_gap_source(
                        row,
                        dict(failure_backoff_before.get(_normalize_code(row.get("fcode"))) or {}),
                    ),
                    blocker_category=_classify_long_term_gap_blocker(row),
                    action_category=_classify_long_term_gap_action(
                        row,
                        source_category=_classify_long_term_gap_source(
                            row,
                            dict(failure_backoff_before.get(_normalize_code(row.get("fcode"))) or {}),
                        ),
                        blocker_category=_classify_long_term_gap_blocker(row),
                    ),
                    failure_record=dict(failure_backoff_before.get(_normalize_code(row.get("fcode"))) or {}),
                )
                for row in selected_candidates
                if bool((failure_backoff_before.get(_normalize_code(row.get("fcode"))) or {}).get("upstream_limited"))
            ][:AUDIT_SAMPLE_LIMIT],
        },
        "audit_after": {
            **post_stats,
            "candidate_total": len(post_candidates),
            "candidate_samples": post_candidates[:AUDIT_SAMPLE_LIMIT],
        },
    }

    long_term_gap_audit = build_long_term_history_gap_audit(
        settings,
        include_stale_candidates=bool(refresh_stale),
        universe=post_universe,
        candidates=post_candidates,
        stats=post_stats,
        failure_backoff=failure_backoff_after,
        write_artifact=True,
    )
    summary["long_term_gap_audit"] = long_term_gap_audit

    maintenance_result = dict(summary.get("maintenance_result") or {})
    summary.update(
        {
            "processed_total": maintenance_result.get("processed_total"),
            "changed_total": maintenance_result.get("changed_total"),
            "annual_return_success_total": maintenance_result.get("annual_success_total"),
            "rank_success_total": maintenance_result.get("rank_success_total"),
            "annual_failed_total": maintenance_result.get("annual_failed_total"),
            "rank_failed_total": maintenance_result.get("rank_failed_total"),
        }
    )

    artifact_dir = settings.reports_dir / "audit" / "long_term_history_maintenance"
    artifact_file = build_audit_artifact_path(artifact_dir, "long_term_history_maintenance", suffix=".json.gz")
    _write_json(artifact_file, summary)
    summary["artifact_path"] = str(artifact_file)
    log(f"债基长期稳定性历史审计完成，审计产物: {artifact_file}")
    return summary
