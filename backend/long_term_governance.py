from __future__ import annotations

from collections import Counter
from copy import deepcopy
import os
from pathlib import Path
from typing import Any
import sqlite3
import threading
import time


STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION = 2
SEMANTIC_NULL_SNAPSHOT_AUDIT_CACHE_TTL_SECONDS = 300.0
_SEMANTIC_NULL_SNAPSHOT_AUDIT_CACHE_SCHEMA_VERSION = 1
_semantic_null_snapshot_audit_cache_lock = threading.Lock()
_semantic_null_snapshot_audit_cache: dict[tuple[Any, ...], dict[str, Any]] = {}


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _classify_stale_scope_not_ready_nav_bucket(nav_staleness_days: Any) -> str:
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
    fresh_recoverable_total = _to_int(counts_by_nav_bucket.get("fresh_0_30d"))
    short_recoverable_total = _to_int(counts_by_nav_bucket.get("short_31_60d"))
    medium_watch_total = _to_int(counts_by_nav_bucket.get("medium_61_120d"))
    deep_watch_total = _to_int(counts_by_nav_bucket.get("deep_121_365d"))
    terminal_candidate_total = _to_int(counts_by_nav_bucket.get("terminal_gt_365d"))
    unknown_nav_staleness_total = _to_int(counts_by_nav_bucket.get("unknown"))
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


def _classify_stale_scope_not_ready_stale_mode(sample: dict[str, Any]) -> str:
    stale_annual = bool(sample.get("stale_annual_returns"))
    stale_rank = bool(sample.get("stale_rank_history"))
    if stale_annual and stale_rank:
        return "annual_and_rank_stale"
    if stale_annual:
        return "annual_only_stale"
    if stale_rank:
        return "rank_only_stale"
    return "other_stale"


def _summarize_stale_scope_not_ready_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    counts_by_nav_bucket: dict[str, int] = {}
    counts_by_stale_mode: dict[str, int] = {}
    counts_by_priority: dict[str, int] = {}
    terminal_candidate_total = 0
    deep_stale_candidate_total = 0

    for sample in samples:
        nav_bucket = _classify_stale_scope_not_ready_nav_bucket(sample.get("nav_staleness_days"))
        stale_mode = _classify_stale_scope_not_ready_stale_mode(sample)
        priority_label = str(sample.get("priority_label") or "").strip() or "unknown"
        counts_by_nav_bucket[nav_bucket] = int(counts_by_nav_bucket.get(nav_bucket) or 0) + 1
        counts_by_stale_mode[stale_mode] = int(counts_by_stale_mode.get(stale_mode) or 0) + 1
        counts_by_priority[priority_label] = int(counts_by_priority.get(priority_label) or 0) + 1
        if nav_bucket == "terminal_gt_365d":
            terminal_candidate_total += 1
        elif nav_bucket in {"deep_121_365d", "medium_61_120d"}:
            deep_stale_candidate_total += 1

    recoverability_summary = _build_stale_scope_recoverability_summary(counts_by_nav_bucket)
    return {
        "total": len(samples),
        "counts_by_nav_bucket": counts_by_nav_bucket,
        "counts_by_stale_mode": counts_by_stale_mode,
        "counts_by_priority": counts_by_priority,
        "terminal_candidate_total": terminal_candidate_total,
        "deep_stale_candidate_total": deep_stale_candidate_total,
        **recoverability_summary,
        "samples": list(samples[:8]),
    }


def _summarize_scope_filtered_no_retry(
    live_action_scope_matrix: dict[str, Any],
    scope_filtered_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    counts_by_priority = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(live_action_scope_matrix or {}).items()
        if _to_int(value) > 0
    }
    total = int(sum(counts_by_priority.values()))
    summary_payload = dict(scope_filtered_summary or {})
    counts_by_source = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(summary_payload.get("counts_by_source") or {}).items()
        if _to_int(value) > 0
    }
    counts_by_blocker = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(summary_payload.get("counts_by_blocker") or {}).items()
        if _to_int(value) > 0
    }
    counts_by_maturity_reason = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(summary_payload.get("counts_by_maturity_reason") or {}).items()
        if _to_int(value) > 0
    }
    structure_counts = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(summary_payload.get("structure_counts") or {}).items()
        if _to_int(value) > 0
    }
    stale_mode_counts = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(summary_payload.get("counts_by_stale_mode") or {}).items()
        if _to_int(value) > 0
    }
    return {
        "total": total,
        "counts_by_priority": counts_by_priority,
        "counts_by_source": counts_by_source,
        "counts_by_blocker": counts_by_blocker,
        "counts_by_maturity_reason": counts_by_maturity_reason,
        "structure_counts": structure_counts,
        "counts_by_stale_mode": stale_mode_counts,
        "deferred_priority_total": _to_int(counts_by_priority.get("deferred_priority")),
        "screenable_priority_total": _to_int(counts_by_priority.get("screenable_priority")),
        "buyable_active_priority_total": _to_int(counts_by_priority.get("buyable_active_priority")),
        "active_priority_total": _to_int(counts_by_priority.get("active_priority")),
        "holding_period_blocked_total": _to_int(structure_counts.get("holding_period_blocked")),
        "runtime_excluded_total": _to_int(structure_counts.get("runtime_excluded")),
        "buyable_holding_period_blocked_total": _to_int(
            structure_counts.get("buyable_holding_period_blocked")
        ),
        "non_buyable_holding_period_blocked_total": _to_int(
            structure_counts.get("non_buyable_holding_period_blocked")
        ),
        "stale_rank_only_total": _to_int(stale_mode_counts.get("rank_only_stale")),
        "upstream_empty_payload_total": _to_int(
            counts_by_source.get("upstream_limited_empty_payload")
        ),
        "upstream_semantic_null_year_total": _to_int(
            counts_by_source.get("upstream_limited_semantic_null_year_gap")
        ),
        "samples": list(summary_payload.get("samples") or []),
    }


def _sample_buyable_active_stale_scope_rows(
    priority_blocked_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    samples = list(
        (priority_blocked_summary.get("samples_by_bucket") or {}).get("buyable_active_scope_not_ready") or []
    )
    return [
        dict(sample or {})
        for sample in samples
        if str((sample or {}).get("source_category") or "").strip() == "local_stale_history_gap"
    ]


def _normalize_positive_count_map(payload: dict[str, Any] | None) -> dict[str, int]:
    return {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(payload or {}).items()
        if _to_int(value) > 0
    }


def _summarize_upstream_limited_structure(
    upstream_observation_summary: dict[str, Any],
    live_action_counts: dict[str, Any] | None = None,
    live_action_scope_matrix: dict[str, Any] | None = None,
    live_source_counts: dict[str, Any] | None = None,
    scope_filtered_no_retry_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_watchlist_total = _to_int(upstream_observation_summary.get("upstream_limited_watchlist_total"))
    stale_watchlist_total = _to_int(
        upstream_observation_summary.get("stale_upstream_limited_watchlist_total")
    )
    terminal_watchlist_total = _to_int(
        upstream_observation_summary.get("terminal_upstream_limited_watchlist_total")
    )
    scope_filtered_total = _to_int(
        upstream_observation_summary.get("scope_filtered_upstream_limited_total")
    )
    current_watchlist_total = (
        active_watchlist_total + stale_watchlist_total + terminal_watchlist_total
    )
    observe_scope_counts = _normalize_positive_count_map(
        dict((live_action_scope_matrix or {}).get("observe_upstream_source") or {})
    )
    observe_action_total = _to_int((live_action_counts or {}).get("observe_upstream_source"))
    if observe_action_total <= 0:
        observe_action_total = int(sum(observe_scope_counts.values()))
    observe_action_total = max(observe_action_total, current_watchlist_total)

    active_reason_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("upstream_limited_reason_counts") or {}
    )
    stale_reason_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("stale_upstream_limited_reason_counts") or {}
    )
    terminal_reason_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("terminal_upstream_limited_reason_counts") or {}
    )
    scope_filtered_reason_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("scope_filtered_upstream_limited_reason_counts") or {}
    )
    active_subtype_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("upstream_limited_subtype_counts") or {}
    )
    stale_subtype_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("stale_upstream_limited_subtype_counts") or {}
    )
    terminal_subtype_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("terminal_upstream_limited_subtype_counts") or {}
    )
    scope_filtered_subtype_counts = _normalize_positive_count_map(
        upstream_observation_summary.get("scope_filtered_upstream_limited_subtype_counts") or {}
    )

    watchlist_empty_payload_total = sum(
        _to_int(reason_counts.get("upstream_limited_empty_payload"))
        for reason_counts in (
            active_reason_counts,
            stale_reason_counts,
            terminal_reason_counts,
        )
    )
    watchlist_semantic_null_total = sum(
        _to_int(reason_counts.get("upstream_limited_semantic_null_year_gap"))
        for reason_counts in (
            active_reason_counts,
            stale_reason_counts,
            terminal_reason_counts,
        )
    )
    watchlist_stale_no_progress_total = sum(
        _to_int(reason_counts.get("upstream_limited_stale_no_progress"))
        for reason_counts in (
            active_reason_counts,
            stale_reason_counts,
            terminal_reason_counts,
        )
    )
    non_watchlist_observe_total = max(0, observe_action_total - current_watchlist_total)
    scope_filtered_empty_payload_total = _to_int(
        (scope_filtered_no_retry_summary or {}).get("upstream_empty_payload_total")
    ) or _to_int(scope_filtered_reason_counts.get("upstream_limited_empty_payload"))
    scope_filtered_semantic_null_total = _to_int(
        (scope_filtered_no_retry_summary or {}).get("upstream_semantic_null_year_total")
    ) or _to_int(scope_filtered_reason_counts.get("upstream_limited_semantic_null_year_gap"))
    derived_semantic_null_total = max(
        0,
        _to_int((live_source_counts or {}).get("upstream_limited_semantic_null_year_gap"))
        - scope_filtered_semantic_null_total,
    )
    inferred_watchlist_semantic_null_total = watchlist_semantic_null_total
    if (
        inferred_watchlist_semantic_null_total <= 0
        and current_watchlist_total > 0
        and derived_semantic_null_total > 0
        and non_watchlist_observe_total <= 0
    ):
        inferred_watchlist_semantic_null_total = min(current_watchlist_total, derived_semantic_null_total)
    remaining_semantic_null_total = max(
        0,
        derived_semantic_null_total - inferred_watchlist_semantic_null_total,
    )
    non_watchlist_semantic_null_total = min(
        non_watchlist_observe_total,
        remaining_semantic_null_total,
    )
    semantic_null_observe_total = (
        inferred_watchlist_semantic_null_total + non_watchlist_semantic_null_total
    )
    unclassified_non_watchlist_observe_total = max(
        0,
        non_watchlist_observe_total - non_watchlist_semantic_null_total,
    )
    empty_payload_total = sum(
        _to_int(reason_counts.get("upstream_limited_empty_payload"))
        for reason_counts in (
            active_reason_counts,
            stale_reason_counts,
            terminal_reason_counts,
            scope_filtered_reason_counts,
        )
    )

    return {
        "observe_action_total": observe_action_total,
        "current_watchlist_total": current_watchlist_total,
        "active_watchlist_total": active_watchlist_total,
        "stale_watchlist_total": stale_watchlist_total,
        "terminal_watchlist_total": terminal_watchlist_total,
        "scope_filtered_total": scope_filtered_total,
        "non_watchlist_observe_total": non_watchlist_observe_total,
        "semantic_null_observe_total": semantic_null_observe_total,
        "watchlist_semantic_null_total": watchlist_semantic_null_total,
        "non_watchlist_semantic_null_total": non_watchlist_semantic_null_total,
        "unclassified_non_watchlist_observe_total": unclassified_non_watchlist_observe_total,
        "stale_no_progress_watchlist_total": watchlist_stale_no_progress_total,
        "empty_payload_watchlist_total": watchlist_empty_payload_total,
        "empty_payload_total": empty_payload_total,
        "scope_filtered_empty_payload_total": scope_filtered_empty_payload_total,
        "scope_filtered_semantic_null_total": scope_filtered_semantic_null_total,
        "buyable_active_total": _to_int(observe_scope_counts.get("buyable_active_priority")),
        "publishable_total": _to_int(observe_scope_counts.get("publishable_priority")),
        "screenable_total": _to_int(observe_scope_counts.get("screenable_priority")),
        "active_reason_counts": active_reason_counts,
        "stale_reason_counts": stale_reason_counts,
        "terminal_reason_counts": terminal_reason_counts,
        "scope_filtered_reason_counts": scope_filtered_reason_counts,
        "active_subtype_counts": active_subtype_counts,
        "stale_subtype_counts": stale_subtype_counts,
        "terminal_subtype_counts": terminal_subtype_counts,
        "scope_filtered_subtype_counts": scope_filtered_subtype_counts,
        "counts_by_priority": observe_scope_counts,
    }


def _summarize_priority_scope_not_ready_structure(
    priority_blocked_summary: dict[str, Any],
    live_scope_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    live_counts_by_priority = {
        str(key).strip() or "unknown": _to_int(value)
        for key, value in dict(live_scope_matrix or {}).items()
        if _to_int(value) > 0
    }
    counts_by_priority = live_counts_by_priority or dict(priority_blocked_summary.get("counts_by_priority") or {})
    source_counts_by_bucket = dict(priority_blocked_summary.get("source_counts_by_bucket") or {})
    buyable_scope_sources = dict(source_counts_by_bucket.get("buyable_active_scope_not_ready") or {})
    other_scope_sources = dict(source_counts_by_bucket.get("other_scope_not_ready") or {})
    live_scope_not_ready_total = int(sum(counts_by_priority.values()))
    buyable_active_scope_not_ready_total = _to_int(
        counts_by_priority.get("buyable_active_priority")
    ) or _to_int(priority_blocked_summary.get("buyable_active_scope_not_ready_total"))
    other_scope_not_ready_total = (
        max(0, live_scope_not_ready_total - buyable_active_scope_not_ready_total)
        if live_counts_by_priority
        else _to_int(priority_blocked_summary.get("other_scope_not_ready_total"))
    )
    return {
        "scope_not_ready_total": (
            live_scope_not_ready_total
            if live_scope_not_ready_total > 0
            else _to_int(priority_blocked_summary.get("scope_not_ready_total"))
        ),
        "stale_scope_not_ready_total": _to_int(priority_blocked_summary.get("stale_scope_not_ready_total")),
        "buyable_active_scope_not_ready_total": buyable_active_scope_not_ready_total,
        "other_scope_not_ready_total": other_scope_not_ready_total,
        "screenable_priority_total": _to_int(counts_by_priority.get("screenable_priority")),
        "buyable_active_priority_total": _to_int(counts_by_priority.get("buyable_active_priority")),
        "active_priority_total": _to_int(counts_by_priority.get("active_priority")),
        "deferred_priority_total": _to_int(counts_by_priority.get("deferred_priority")),
        "buyable_active_scope_not_ready_stale_total": _to_int(
            buyable_scope_sources.get("local_stale_history_gap")
        ),
        "other_scope_not_ready_stale_total": _to_int(other_scope_sources.get("local_stale_history_gap")),
    }


def _summarize_annual_semantic_null_bridge(
    annual_feature_upstream_semantic_null_year_gap_fund_count: int,
    annual_feature_pending_maturity_excluding_semantic_null_fund_count: int,
    upstream_limited_structure_summary: dict[str, Any],
) -> dict[str, Any]:
    observe_total = _to_int(upstream_limited_structure_summary.get("semantic_null_observe_total"))
    scope_filtered_total = _to_int(
        upstream_limited_structure_summary.get("scope_filtered_semantic_null_total")
    )
    execution_total = observe_total
    remaining_total = max(
        0,
        annual_feature_upstream_semantic_null_year_gap_fund_count - execution_total,
    )
    execution_ratio = (
        execution_total / annual_feature_upstream_semantic_null_year_gap_fund_count
        if annual_feature_upstream_semantic_null_year_gap_fund_count > 0
        else 0.0
    )
    return {
        "snapshot_total": annual_feature_upstream_semantic_null_year_gap_fund_count,
        "pending_maturity_excluding_semantic_null_total": annual_feature_pending_maturity_excluding_semantic_null_fund_count,
        "observe_total": observe_total,
        "scope_filtered_tail_total": scope_filtered_total,
        "execution_total": execution_total,
        "remaining_total": remaining_total,
        "remaining_pending_maturity_layer_total": remaining_total,
        "execution_ratio": execution_ratio,
    }


def _path_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    try:
        resolved = Path(path).resolve()
        stat = resolved.stat()
    except OSError:
        return f"{Path(path)}:missing"
    return f"{resolved}:{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _build_semantic_null_snapshot_audit_cache_signature(
    settings: Any,
    rb: Any,
    nav_archive_binding: dict[str, Any],
) -> tuple[Any, ...]:
    db_path = Path(settings.database_path)
    archive_path = _clean_text(nav_archive_binding.get("archive_path"))
    source_artifact_path = (
        _clean_text(nav_archive_binding.get("source_apply_runner_artifact"))
        or _clean_text(nav_archive_binding.get("source_readiness_artifact"))
    )
    try:
        target_years = tuple(
            rb._current_complete_calendar_year_window(rb.LONG_TERM_STABILITY_MIN_YEARS_5)
        )
    except Exception:
        target_years = ()
    return (
        _SEMANTIC_NULL_SNAPSHOT_AUDIT_CACHE_SCHEMA_VERSION,
        os.environ.get("PYTEST_CURRENT_TEST", "").strip(),
        str(db_path.resolve()),
        _path_cache_token(db_path),
        _path_cache_token(f"{db_path}-wal"),
        _path_cache_token(f"{db_path}-shm"),
        target_years,
        _clean_text(nav_archive_binding.get("archive_table_name")),
        _clean_text(nav_archive_binding.get("archive_cutoff_date")),
        _path_cache_token(archive_path),
        _path_cache_token(source_artifact_path),
    )


def _load_matching_nav_archive_binding(settings: Any, rb: Any) -> dict[str, Any]:
    try:
        nav_archive_binding = rb.load_latest_nav_history_archive_binding()
        if not rb.nav_history_archive_binding_matches_database(
            nav_archive_binding,
            settings.database_path,
        ):
            return {}
        return dict(nav_archive_binding or {})
    except Exception:
        return {}


def _build_semantic_null_snapshot_audit_summary() -> dict[str, Any]:
    try:
        from backend.config import load_settings
        from backend import report_builder as rb
    except Exception:
        return {}

    settings = load_settings()
    nav_archive_binding = _load_matching_nav_archive_binding(settings, rb)
    cache_signature = _build_semantic_null_snapshot_audit_cache_signature(
        settings,
        rb,
        nav_archive_binding,
    )
    now = time.time()
    with _semantic_null_snapshot_audit_cache_lock:
        cached = _semantic_null_snapshot_audit_cache.get(cache_signature)
        if cached and now - float(cached.get("stored_at") or 0.0) <= SEMANTIC_NULL_SNAPSHOT_AUDIT_CACHE_TTL_SECONDS:
            return deepcopy(cached.get("value") or {})

    value = _build_semantic_null_snapshot_audit_summary_uncached(
        settings,
        rb,
        nav_archive_binding,
    )
    with _semantic_null_snapshot_audit_cache_lock:
        _semantic_null_snapshot_audit_cache[cache_signature] = {
            "stored_at": time.time(),
            "value": deepcopy(value),
        }
        if len(_semantic_null_snapshot_audit_cache) > 8:
            for key, payload in list(_semantic_null_snapshot_audit_cache.items()):
                if time.time() - float(payload.get("stored_at") or 0.0) > SEMANTIC_NULL_SNAPSHOT_AUDIT_CACHE_TTL_SECONDS:
                    _semantic_null_snapshot_audit_cache.pop(key, None)
            while len(_semantic_null_snapshot_audit_cache) > 8:
                oldest_key = min(
                    _semantic_null_snapshot_audit_cache,
                    key=lambda item: float(
                        _semantic_null_snapshot_audit_cache[item].get("stored_at") or 0.0
                    ),
                )
                _semantic_null_snapshot_audit_cache.pop(oldest_key, None)
    return deepcopy(value)


def _build_semantic_null_snapshot_audit_summary_uncached(
    settings: Any,
    rb: Any,
    nav_archive_binding: dict[str, Any],
) -> dict[str, Any]:
    metadata_df = rb._read_metadata(settings.database_path)
    if metadata_df.empty or "fcode" not in metadata_df.columns:
        return {}
    returns_df = rb._read_return_snapshot(settings.database_path)
    risk_df = rb._read_risk_snapshot(settings.database_path)
    code_sets = rb._build_bond_universe_code_sets(
        settings=settings,
        metadata_df=metadata_df.copy(),
        returns_df=returns_df,
        risk_df=risk_df,
    )
    screenable_metadata_df = metadata_df.copy()
    screenable_metadata_df["fcode"] = screenable_metadata_df.get("fcode").map(rb._normalize_code)
    screenable_metadata_df = screenable_metadata_df[
        screenable_metadata_df["fcode"].isin(code_sets.get("screenable_codes") or set())
    ].copy()
    if screenable_metadata_df.empty:
        return {}

    candidate_codes = {
        str(code)
        for code in screenable_metadata_df.get("fcode", []).astype(str).tolist()
        if str(code).strip()
    }
    estab_date_map = {
        str(code): str(estab_date or "").strip()
        for code, estab_date in zip(
            screenable_metadata_df.get("fcode").astype(str).tolist(),
            screenable_metadata_df.get("estab_date").tolist(),
            strict=False,
        )
        if str(code).strip()
    }
    conn = sqlite3.connect(str(settings.database_path))
    try:
        target_years = rb._current_complete_calendar_year_window(rb.LONG_TERM_STABILITY_MIN_YEARS_5)
        placeholders = ",".join("?" for _ in target_years)
        nav_archive_attach = rb.attach_nav_history_archive_if_available(
            conn,
            archive_path=nav_archive_binding.get("archive_path"),
            archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
        )
        nav_source_sql = rb.build_nav_history_union_source_sql(
            nav_archive_attach,
            columns=("fcode", "nav_date"),
        )
        annual_ready_codes = {
            rb._normalize_code(row[0])
            for row in conn.execute(
                f"""
                SELECT fcode
                FROM fund_annual_returns
                WHERE year IN ({placeholders})
                GROUP BY fcode
                HAVING COUNT(DISTINCT CASE WHEN annual_return IS NOT NULL THEN year END) >= ?
                """,
                tuple(target_years) + (rb.LONG_TERM_STABILITY_MIN_YEARS_5,),
            ).fetchall()
            if rb._normalize_code(row[0])
        }
        annual_gap_codes = candidate_codes - annual_ready_codes
        annual_stats_by_code = {
            rb._normalize_code(row[0]): {
                "row_count": int(row[1] or 0),
                "nonnull_count": int(row[2] or 0),
                "present_years": sorted(
                    {int(year) for year in str(row[3] or "").split(",") if str(year).strip().isdigit()}
                ),
                "nonnull_years": sorted(
                    {int(year) for year in str(row[4] or "").split(",") if str(year).strip().isdigit()}
                ),
            }
            for row in conn.execute(
                f"""
                SELECT
                    fcode,
                    COUNT(*) AS row_count,
                    SUM(CASE WHEN annual_return IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_count,
                    GROUP_CONCAT(DISTINCT year) AS present_years,
                    GROUP_CONCAT(DISTINCT CASE WHEN annual_return IS NOT NULL THEN year END) AS nonnull_years
                FROM fund_annual_returns
                WHERE year IN ({placeholders})
                GROUP BY fcode
                """,
                tuple(target_years),
            ).fetchall()
            if rb._normalize_code(row[0])
        }
        earliest_nav_date_map = {
            rb._normalize_code(row[0]): str(row[1] or "").strip()
            for row in conn.execute(
                f"""
                SELECT fcode, MIN(nav_date) AS earliest_nav_date
                FROM ({nav_source_sql}) AS nav_history
                GROUP BY fcode
                """
            ).fetchall()
            if rb._normalize_code(row[0]) and str(row[1] or "").strip()
        }
        nav_window_present_years_map: dict[str, list[int]] = {}
        nav_window_first_nav_date_by_code: dict[str, dict[int, str]] = {}
        normalized_candidate_codes = sorted(candidate_codes)
        start_date = f"{target_years[0]:04d}-01-01"
        end_date = f"{target_years[-1]:04d}-12-31"
        for start in range(0, len(normalized_candidate_codes), 500):
            batch = normalized_candidate_codes[start:start + 500]
            batch_placeholders = ",".join("?" for _ in batch)
            rows = conn.execute(
                f"""
                SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year, MIN(nav_date) AS first_nav_date
                FROM ({nav_source_sql}) AS nav_history
                WHERE fcode IN ({batch_placeholders})
                  AND nav_date BETWEEN ? AND ?
                GROUP BY fcode, substr(nav_date, 1, 4)
                """,
                tuple(batch) + (start_date, end_date),
            ).fetchall()
            for row in rows:
                code = rb._normalize_code(row[0])
                year = row[1]
                if not code or year is None:
                    continue
                normalized_year = int(year)
                if normalized_year not in target_years:
                    continue
                nav_window_present_years_map.setdefault(code, set()).add(normalized_year)
                first_nav_date = str(row[2] or "").strip()
                if first_nav_date:
                    nav_window_first_nav_date_by_code.setdefault(code, {})[normalized_year] = first_nav_date
        nav_window_present_years_map = {
            code: sorted(years)
            for code, years in nav_window_present_years_map.items()
        }

        mature_counts: Counter[str] = Counter()
        present_shape_counts: Counter[str] = Counter()
        nonnull_shape_counts: Counter[str] = Counter()
        row_count_counts: Counter[int] = Counter()
        nonnull_count_counts: Counter[int] = Counter()
        leading_month_counts: Counter[str] = Counter()
        semantic_total = 0

        for code in annual_gap_codes:
            estab_date_text = str(estab_date_map.get(code) or "").strip()
            earliest_nav_date_text = str(earliest_nav_date_map.get(code) or "").strip()
            annual_stats = annual_stats_by_code.get(code) or {}
            present_years = list(annual_stats.get("present_years") or [])
            nonnull_years = list(annual_stats.get("nonnull_years") or [])
            leading_present_year = present_years[0] if present_years else None
            leading_partial = rb._annual_feature_window_has_leading_partial_null_year_for_report(
                target_years,
                present_years,
                nonnull_years,
                leading_present_year_first_nav_date=(
                    (nav_window_first_nav_date_by_code.get(code) or {}).get(int(leading_present_year))
                    if leading_present_year is not None
                    else None
                ),
            )
            if not leading_partial:
                continue
            semantic_total += 1
            mature = rb._annual_feature_window_mature_enough_for_report(
                estab_date_text,
                earliest_nav_date=earliest_nav_date_text,
                current_window_nav_present_years=nav_window_present_years_map.get(code),
                min_years=rb.LONG_TERM_STABILITY_MIN_YEARS_5,
            )
            if mature is True:
                mature_counts["mature_true"] += 1
            elif mature is False:
                mature_counts["mature_false"] += 1
            else:
                mature_counts["mature_unknown"] += 1
            present_shape_counts[str(tuple(present_years))] += 1
            nonnull_shape_counts[str(tuple(nonnull_years))] += 1
            row_count_counts[int(annual_stats.get("row_count") or 0)] += 1
            nonnull_count_counts[int(annual_stats.get("nonnull_count") or 0)] += 1
            first_nav_date = (
                ((nav_window_first_nav_date_by_code.get(code) or {}).get(int(leading_present_year)) if leading_present_year is not None else "")
                or earliest_nav_date_text
            )
            if first_nav_date:
                leading_month_counts[first_nav_date[:7]] += 1

        return {
            "available": semantic_total > 0,
            "semantic_total": semantic_total,
            "mature_counts": dict(mature_counts),
            "present_shape_top": [
                {"shape": key, "count": value}
                for key, value in present_shape_counts.most_common(6)
            ],
            "nonnull_shape_top": [
                {"shape": key, "count": value}
                for key, value in nonnull_shape_counts.most_common(6)
            ],
            "row_count_distribution": {
                str(key): value for key, value in row_count_counts.most_common()
            },
            "nonnull_count_distribution": {
                str(key): value for key, value in nonnull_count_counts.most_common()
            },
            "leading_month_top": [
                {"month": key, "count": value}
                for key, value in leading_month_counts.most_common(12)
            ],
        }
    except Exception:
        return {}
    finally:
        if "nav_archive_attach" in locals() and nav_archive_attach.get("attached"):
            rb.detach_nav_history_archive(
                conn,
                alias=str(nav_archive_attach.get("alias") or "nav_archive_db"),
            )
        conn.close()


def _summarize_buyable_active_stale_scope_recoverability(
    priority_blocked_summary: dict[str, Any],
) -> dict[str, Any]:
    samples = list(
        (priority_blocked_summary.get("samples_by_bucket") or {}).get("buyable_active_scope_not_ready") or []
    )
    stale_samples = [
        dict(sample or {})
        for sample in samples
        if str((sample or {}).get("source_category") or "").strip() == "local_stale_history_gap"
    ]
    counts_by_nav_bucket: dict[str, int] = {}
    counts_by_stale_mode: dict[str, int] = {}
    counts_by_maturity_reason: dict[str, int] = {}

    for sample in stale_samples:
        nav_bucket = _classify_stale_scope_not_ready_nav_bucket(sample.get("nav_staleness_days"))
        stale_mode = _classify_stale_scope_not_ready_stale_mode(sample)
        maturity_reason = str(sample.get("history_maturity_reason") or "").strip() or "unknown"
        counts_by_nav_bucket[nav_bucket] = int(counts_by_nav_bucket.get(nav_bucket) or 0) + 1
        counts_by_stale_mode[stale_mode] = int(counts_by_stale_mode.get(stale_mode) or 0) + 1
        counts_by_maturity_reason[maturity_reason] = int(
            counts_by_maturity_reason.get(maturity_reason) or 0
        ) + 1

    recoverability_summary = _build_stale_scope_recoverability_summary(counts_by_nav_bucket)
    return {
        "total": len(stale_samples),
        "counts_by_nav_bucket": counts_by_nav_bucket,
        "counts_by_stale_mode": counts_by_stale_mode,
        "counts_by_maturity_reason": counts_by_maturity_reason,
        **recoverability_summary,
        "samples": stale_samples[:18],
    }


def build_long_term_maintenance_detail(
    scheduler_overview: dict[str, Any] | None,
) -> dict[str, Any]:
    overview = dict(scheduler_overview or {})
    maintenance_summary = overview.get("maintenance_summary") or {}
    weekly_closure = ((overview.get("task_closures") or {}).get("weekly_maintenance") or {})
    weekly_child = ((weekly_closure.get("child_tasks") or {}).get("long_term_history_maintenance") or {})

    return {
        "status": _clean_text(
            weekly_child.get("status")
            or maintenance_summary.get("long_term_status")
        ),
        "no_action_category": _clean_text(
            weekly_child.get("no_action_category")
            or maintenance_summary.get("long_term_no_action_category")
        ),
        "idle_reason": _clean_text(weekly_child.get("idle_reason")),
        "processed_total": _to_int(
            weekly_child.get("processed_total")
            or maintenance_summary.get("long_term_processed_total")
        ),
        "changed_total": _to_int(
            weekly_child.get("changed_total")
            or maintenance_summary.get("long_term_changed_total")
        ),
        "coverage_progress": (
            maintenance_summary.get("long_term_coverage_progress")
            or weekly_child.get("coverage_progress")
            or {}
        ),
        "upstream_observation_summary": (
            maintenance_summary.get("long_term_upstream_observation_summary")
            or weekly_child.get("upstream_observation_summary")
            or {}
        ),
        "priority_blocked_summary": (
            maintenance_summary.get("long_term_priority_blocked_summary")
            or weekly_child.get("priority_blocked_summary")
            or {}
        ),
        "failed_reason_governance_summary": (
            maintenance_summary.get("long_term_failed_reason_governance_summary")
            or weekly_child.get("failed_reason_governance_summary")
            or {}
        ),
        "failed_reason_summary": (
            maintenance_summary.get("long_term_failed_reason_summary")
            or weekly_child.get("failed_reason_summary")
            or {}
        ),
    }


def build_long_term_governance_summary(
    long_term_snapshot: dict[str, Any] | None,
    maintenance_detail: dict[str, Any] | None,
    *,
    include_semantic_null_snapshot_audit: bool = True,
    semantic_null_snapshot_audit_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = dict(long_term_snapshot or {})
    detail = dict(maintenance_detail or {})
    coverage_progress = detail.get("coverage_progress") or {}
    upstream_observation_summary = detail.get("upstream_observation_summary") or {}
    raw_priority_blocked_summary = dict(detail.get("priority_blocked_summary") or {})
    failed_reason_governance_summary = detail.get("failed_reason_governance_summary") or {}
    failed_reason_summary = detail.get("failed_reason_summary") or {}
    live_gap_audit = dict(detail.get("live_gap_audit") or {})
    live_action_counts = dict(live_gap_audit.get("action_counts") or {})
    live_upstream_observation_summary = dict(live_gap_audit.get("upstream_observation_summary") or {})
    live_action_scope_matrix = dict(live_gap_audit.get("action_scope_matrix") or {})
    live_source_counts = dict(live_gap_audit.get("source_counts") or {})
    live_scope_filtered_no_retry_summary = dict(
        live_gap_audit.get("scope_filtered_no_retry_summary") or {}
    )
    effective_upstream_observation_summary = dict(upstream_observation_summary or {})
    if live_upstream_observation_summary:
        effective_upstream_observation_summary = live_upstream_observation_summary
    upstream_observation_summary = effective_upstream_observation_summary

    pending_reason_counts = upstream_observation_summary.get("pending_reason_counts") or {}
    pending_recovery_samples = upstream_observation_summary.get("pending_recovery_samples") or []
    await_history_maturity_count = sum(
        _to_int(pending_reason_counts.get(key))
        for key in (
            "await_history_maturity",
            "history_window_not_mature",
            "history_maturity_pending",
            "insufficient_history_window",
            "minimum_history_not_reached",
        )
    )
    if await_history_maturity_count <= 0:
        await_history_maturity_count = _to_int(live_action_counts.get("await_history_maturity"))
    pending_recovery_total = _to_int(upstream_observation_summary.get("pending_recovery_total"))
    pending_retry_window_total = _to_int(upstream_observation_summary.get("pending_retry_window_total")) or _to_int(
        pending_reason_counts.get("local_retry_window_pending")
    )
    pending_retry_cooldown_total = _to_int(upstream_observation_summary.get("pending_retry_cooldown_total")) or _to_int(
        pending_reason_counts.get("local_retry_cooldown_pending")
    )
    pending_retry_ready_total = _to_int(upstream_observation_summary.get("pending_retry_ready_total"))
    if pending_retry_ready_total <= 0:
        pending_retry_ready_total = _to_int(pending_reason_counts.get("local_retry_pending")) + _to_int(
            pending_reason_counts.get("local_previous_failure_pending_recheck")
        )
    live_retry_later_total = _to_int(live_action_counts.get("retry_locally_later"))
    live_retry_locally_now_total = _to_int(live_action_counts.get("retry_locally_now"))
    local_retry_ready_count = pending_retry_ready_total + live_retry_locally_now_total
    local_retry_pool_count = pending_retry_window_total + pending_retry_cooldown_total + local_retry_ready_count
    local_retry_other_pending_count = 0
    live_retry_pool_total = live_retry_later_total + live_retry_locally_now_total
    if live_retry_pool_total > local_retry_pool_count:
        local_retry_other_pending_count = live_retry_pool_total - local_retry_pool_count
        local_retry_pool_count = live_retry_pool_total
    if local_retry_pool_count <= 0 and pending_recovery_total > await_history_maturity_count:
        local_retry_pool_count = max(0, pending_recovery_total - await_history_maturity_count)
    fallback_next_retry_values = sorted(
        str((item or {}).get("cooldown_next_retry_after") or "").strip()
        for item in pending_recovery_samples
        if str((item or {}).get("cooldown_next_retry_after") or "").strip()
    )
    pending_recovery_samples = list(upstream_observation_summary.get("pending_recovery_samples") or [])
    live_retry_later_samples = list((live_gap_audit.get("samples_by_action") or {}).get("retry_locally_later") or [])
    live_retry_ready_samples = list((live_gap_audit.get("samples_by_action") or {}).get("retry_locally_now") or [])
    local_retry_samples: list[dict[str, Any]] = []
    seen_local_retry_codes: set[str] = set()

    def _append_local_retry_samples(rows: list[dict[str, Any]]) -> None:
        for row in rows:
            sample = dict(row or {})
            code = str(sample.get("fcode") or "").strip()
            dedupe_key = code or repr(sorted(sample.items()))
            if dedupe_key in seen_local_retry_codes:
                continue
            seen_local_retry_codes.add(dedupe_key)
            local_retry_samples.append(sample)

    # 优先展示 live action 样本，再补齐 pending recovery 样本，避免只看到低信号的截断样本。
    _append_local_retry_samples(live_retry_ready_samples)
    _append_local_retry_samples(live_retry_later_samples)
    _append_local_retry_samples(pending_recovery_samples)

    counts_by_bucket = raw_priority_blocked_summary.get("counts_by_bucket") or {}
    buyable_active_metadata_blocked_total = _to_int(
        raw_priority_blocked_summary.get("buyable_active_metadata_blocked_total")
    ) or _to_int(counts_by_bucket.get("buyable_active_metadata_blocked"))
    buyable_active_scope_not_ready_total = _to_int(
        raw_priority_blocked_summary.get("buyable_active_scope_not_ready_total")
    ) or _to_int(counts_by_bucket.get("buyable_active_scope_not_ready"))
    other_metadata_blocked_total = _to_int(
        raw_priority_blocked_summary.get("other_metadata_blocked_total")
    ) or _to_int(counts_by_bucket.get("other_metadata_blocked"))
    other_scope_not_ready_total = _to_int(
        raw_priority_blocked_summary.get("other_scope_not_ready_total")
    ) or _to_int(counts_by_bucket.get("other_scope_not_ready"))
    priority_blocked_total_raw = _to_int(
        raw_priority_blocked_summary.get("total_raw")
    ) or _to_int(raw_priority_blocked_summary.get("total"))
    if priority_blocked_total_raw <= 0:
        priority_blocked_total_raw = (
            buyable_active_metadata_blocked_total
            + buyable_active_scope_not_ready_total
            + other_metadata_blocked_total
            + other_scope_not_ready_total
        )
    scope_not_ready_total = _to_int(raw_priority_blocked_summary.get("scope_not_ready_total"))
    if scope_not_ready_total <= 0:
        scope_not_ready_total = buyable_active_scope_not_ready_total + other_scope_not_ready_total
    stale_scope_not_ready_total = _to_int(raw_priority_blocked_summary.get("stale_scope_not_ready_total"))
    if stale_scope_not_ready_total > scope_not_ready_total:
        stale_scope_not_ready_total = scope_not_ready_total
    stale_buyable_active_scope_not_ready_total = _to_int(
        raw_priority_blocked_summary.get("stale_buyable_active_scope_not_ready_total")
    )
    stale_other_scope_not_ready_total = _to_int(
        raw_priority_blocked_summary.get("stale_other_scope_not_ready_total")
    )
    if stale_buyable_active_scope_not_ready_total <= 0 or stale_other_scope_not_ready_total <= 0:
        stale_buyable_active_scope_not_ready_samples = [
            sample
            for sample in list((raw_priority_blocked_summary.get("samples_by_bucket") or {}).get("buyable_active_scope_not_ready") or [])
            if str(sample.get("source_category") or "").strip() == "local_stale_history_gap"
        ]
        stale_other_scope_not_ready_samples = [
            sample
            for sample in list((raw_priority_blocked_summary.get("samples_by_bucket") or {}).get("other_scope_not_ready") or [])
            if str(sample.get("source_category") or "").strip() == "local_stale_history_gap"
        ]
        if stale_buyable_active_scope_not_ready_total <= 0:
            stale_buyable_active_scope_not_ready_total = len(stale_buyable_active_scope_not_ready_samples)
        if stale_other_scope_not_ready_total <= 0:
            stale_other_scope_not_ready_total = len(stale_other_scope_not_ready_samples)
    if stale_buyable_active_scope_not_ready_total + stale_other_scope_not_ready_total > stale_scope_not_ready_total:
        stale_scope_not_ready_total = stale_buyable_active_scope_not_ready_total + stale_other_scope_not_ready_total
    priority_blocked_total_excluding_stale_scope_not_ready = _to_int(
        raw_priority_blocked_summary.get("total_excluding_stale_scope_not_ready")
    )
    if (
        priority_blocked_total_excluding_stale_scope_not_ready <= 0
        and priority_blocked_total_raw > 0
    ):
        priority_blocked_total_excluding_stale_scope_not_ready = max(
            0,
            priority_blocked_total_raw - stale_scope_not_ready_total,
        )
    high_priority_scope_not_ready_total = max(
        0,
        buyable_active_scope_not_ready_total - min(stale_buyable_active_scope_not_ready_total, buyable_active_scope_not_ready_total),
    )
    priority_blocked_total = buyable_active_metadata_blocked_total + high_priority_scope_not_ready_total
    if priority_blocked_total <= 0:
        priority_blocked_total = max(
            0,
            buyable_active_metadata_blocked_total + buyable_active_scope_not_ready_total - stale_scope_not_ready_total,
        )

    priority_blocked_summary = dict(raw_priority_blocked_summary)
    priority_blocked_summary.update(
        {
            "total": priority_blocked_total,
            "total_raw": priority_blocked_total_raw,
            "buyable_active_metadata_blocked_total": buyable_active_metadata_blocked_total,
            "buyable_active_scope_not_ready_total": buyable_active_scope_not_ready_total,
            "other_metadata_blocked_total": other_metadata_blocked_total,
            "other_scope_not_ready_total": other_scope_not_ready_total,
            "scope_not_ready_total": scope_not_ready_total,
            "stale_buyable_active_scope_not_ready_total": stale_buyable_active_scope_not_ready_total,
            "stale_other_scope_not_ready_total": stale_other_scope_not_ready_total,
            "stale_scope_not_ready_total": stale_scope_not_ready_total,
            "total_excluding_stale_scope_not_ready": priority_blocked_total_excluding_stale_scope_not_ready,
            "high_priority_scope_not_ready_total": high_priority_scope_not_ready_total,
        }
    )

    raw_stale_scope_not_ready_samples = [
        *list((priority_blocked_summary.get("samples_by_bucket") or {}).get("buyable_active_scope_not_ready") or []),
        *list((priority_blocked_summary.get("samples_by_bucket") or {}).get("other_scope_not_ready") or []),
    ]
    stale_scope_not_ready_samples = [
        sample
        for sample in raw_stale_scope_not_ready_samples
        if str(sample.get("source_category") or "").strip() == "local_stale_history_gap"
    ]
    stale_scope_not_ready_all_samples = list(stale_scope_not_ready_samples)
    if not stale_scope_not_ready_all_samples and stale_scope_not_ready_total > 0:
        stale_scope_not_ready_all_samples = raw_stale_scope_not_ready_samples
    stale_scope_not_ready_summary = _summarize_stale_scope_not_ready_samples(
        stale_scope_not_ready_all_samples
    )
    full_stale_scope_not_ready_summary = dict(
        priority_blocked_summary.get("stale_scope_not_ready_full_summary") or {}
    )
    if (
        _to_int(full_stale_scope_not_ready_summary.get("total")) > 0
        and _to_int(full_stale_scope_not_ready_summary.get("nav_bucket_schema_version"))
        >= STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION
    ):
        full_counts_by_nav_bucket = dict(
            full_stale_scope_not_ready_summary.get("counts_by_nav_bucket") or {}
        )
        full_recoverability_summary = _build_stale_scope_recoverability_summary(
            full_counts_by_nav_bucket
        )
        stale_scope_not_ready_summary.update(
            {
                "total": _to_int(full_stale_scope_not_ready_summary.get("total")),
                "counts_by_nav_bucket": full_counts_by_nav_bucket,
                "counts_by_stale_mode": dict(
                    full_stale_scope_not_ready_summary.get("counts_by_stale_mode") or {}
                ),
                "counts_by_priority": dict(
                    full_stale_scope_not_ready_summary.get("counts_by_priority") or {}
                ),
                "terminal_candidate_total": _to_int(
                    full_stale_scope_not_ready_summary.get("terminal_candidate_total")
                ),
                "deep_stale_candidate_total": _to_int(
                    full_stale_scope_not_ready_summary.get("deep_stale_candidate_total")
                ),
                **full_recoverability_summary,
            }
        )
    scope_filtered_no_retry_summary = _summarize_scope_filtered_no_retry(
        live_action_scope_matrix.get("scope_filtered_no_retry") or {},
        live_scope_filtered_no_retry_summary,
    )
    upstream_limited_structure_summary = _summarize_upstream_limited_structure(
        upstream_observation_summary,
        live_action_counts,
        live_action_scope_matrix,
        live_source_counts,
        scope_filtered_no_retry_summary,
    )
    upstream_limited_watchlist_count = _to_int(
        upstream_limited_structure_summary.get("observe_action_total")
    )
    upstream_limited_empty_payload_total = _to_int(
        upstream_limited_structure_summary.get("empty_payload_total")
    )
    priority_scope_not_ready_summary = _summarize_priority_scope_not_ready_structure(
        priority_blocked_summary,
        live_action_scope_matrix.get("defer_until_scope_recovers") or {},
    )
    buyable_active_stale_scope_recoverability_summary = _summarize_buyable_active_stale_scope_recoverability(
        priority_blocked_summary
    )
    buyable_active_stale_scope_samples = _sample_buyable_active_stale_scope_rows(
        priority_blocked_summary
    )
    priority_blocked_samples = [
        *list((priority_blocked_summary.get("samples_by_bucket") or {}).get("buyable_active_metadata_blocked") or []),
    ]
    upstream_limited_watchlist_samples = [
        *(upstream_observation_summary.get("upstream_limited_watchlist_samples") or []),
        *(upstream_observation_summary.get("stale_upstream_limited_watchlist_samples") or []),
        *(upstream_observation_summary.get("terminal_upstream_limited_watchlist_samples") or []),
    ]
    live_observe_upstream_samples = list(
        (live_gap_audit.get("samples_by_action") or {}).get("observe_upstream_source") or []
    )
    if live_observe_upstream_samples:
        upstream_limited_watchlist_samples = live_observe_upstream_samples
    elif not upstream_limited_watchlist_samples:
        upstream_limited_watchlist_samples = list(
            live_observe_upstream_samples
        )

    annual_history_missing_total = _to_int(snapshot.get("annual_feature_gap_fund_count")) or max(
        0,
        _to_int(snapshot.get("candidate_fund_count"))
        - _to_int(snapshot.get("annual_feature_ready_fund_count")),
    )
    annual_feature_pending_maturity_fund_count = _to_int(
        snapshot.get("annual_feature_pending_maturity_fund_count")
    )
    annual_feature_actionable_gap_fund_count = _to_int(
        snapshot.get("annual_feature_actionable_gap_fund_count")
    )
    annual_feature_metadata_unknown_fund_count = _to_int(
        snapshot.get("annual_feature_metadata_unknown_fund_count")
    )
    annual_feature_actionable_missing_history_fund_count = _to_int(
        snapshot.get("annual_feature_actionable_missing_history_fund_count")
    )
    annual_feature_actionable_sparse_year_row_gap_fund_count = _to_int(
        snapshot.get("annual_feature_actionable_sparse_year_row_gap_fund_count")
    )
    annual_feature_actionable_null_year_gap_fund_count = _to_int(
        snapshot.get("annual_feature_actionable_null_year_gap_fund_count")
    )
    annual_feature_upstream_semantic_null_year_gap_fund_count = _to_int(
        snapshot.get("annual_feature_upstream_semantic_null_year_gap_fund_count")
    )
    annual_feature_upstream_limited_gap_fund_count = _to_int(
        snapshot.get("annual_feature_upstream_limited_gap_fund_count")
    )
    annual_feature_upstream_limited_missing_history_fund_count = _to_int(
        snapshot.get("annual_feature_upstream_limited_missing_history_fund_count")
    )
    annual_feature_upstream_limited_sparse_year_row_gap_fund_count = _to_int(
        snapshot.get("annual_feature_upstream_limited_sparse_year_row_gap_fund_count")
    )
    annual_feature_upstream_limited_null_year_gap_fund_count = _to_int(
        snapshot.get("annual_feature_upstream_limited_null_year_gap_fund_count")
    )
    annual_feature_upstream_limited_gap_samples = list(
        snapshot.get("annual_feature_upstream_limited_gap_samples") or []
    )
    annual_feature_pending_maturity_excluding_semantic_null_fund_count = max(
        0,
        annual_feature_pending_maturity_fund_count - annual_feature_upstream_semantic_null_year_gap_fund_count,
    )
    annual_semantic_null_bridge_summary = _summarize_annual_semantic_null_bridge(
        annual_feature_upstream_semantic_null_year_gap_fund_count,
        annual_feature_pending_maturity_excluding_semantic_null_fund_count,
        upstream_limited_structure_summary,
    )
    if semantic_null_snapshot_audit_summary is not None:
        semantic_null_snapshot_audit_summary = dict(semantic_null_snapshot_audit_summary or {})
    elif include_semantic_null_snapshot_audit:
        semantic_null_snapshot_audit_summary = _build_semantic_null_snapshot_audit_summary()
    else:
        semantic_null_snapshot_audit_summary = {}
    rank_history_missing_total = _to_int(snapshot.get("rank_feature_gap_fund_count")) or max(
        0,
        _to_int(snapshot.get("candidate_fund_count"))
        - _to_int(snapshot.get("rank_feature_ready_fund_count")),
    )
    annual_feature_stale_fund_count = _to_int(snapshot.get("annual_feature_stale_fund_count"))
    rank_feature_stale_fund_count = _to_int(snapshot.get("rank_feature_stale_fund_count"))
    full_feature_stale_fund_count = _to_int(snapshot.get("full_feature_stale_fund_count"))
    maintenance_gap_fund_count = _to_int(live_gap_audit.get("total_gap_fund_count"))
    maintenance_candidate_total = _to_int(live_gap_audit.get("candidate_total"))
    maintenance_screenable_candidate_total = _to_int(live_gap_audit.get("screenable_candidate_total"))
    maintenance_buyable_candidate_total = _to_int(live_gap_audit.get("buyable_candidate_total"))
    maintenance_scope_filtered_no_retry_total = _to_int(live_action_counts.get("scope_filtered_no_retry"))
    maintenance_actionable_governance_pool_total = (
        await_history_maturity_count
        + local_retry_pool_count
        + upstream_limited_watchlist_count
        + priority_blocked_total
        + stale_scope_not_ready_total
    )
    snapshot_scope_label = "长期快照全集统计"
    maintenance_scope_label = "当前维护治理池统计"
    snapshot_scope_note = (
        f"分母为长期快照候选全集 {_to_int(snapshot.get('candidate_fund_count'))} 只，"
        "用于衡量长周期特征覆盖，不与当前维护治理池统计直接相加。"
    )
    maintenance_scope_note_parts: list[str] = []
    if maintenance_candidate_total > 0:
        maintenance_scope_note_parts.append(f"维护候选集 {maintenance_candidate_total} 只")
    if maintenance_screenable_candidate_total > 0 or maintenance_buyable_candidate_total > 0:
        maintenance_scope_note_parts.append(
            f"其中可筛选 {maintenance_screenable_candidate_total} 只，可买入 {maintenance_buyable_candidate_total} 只"
        )
    if maintenance_gap_fund_count > 0:
        maintenance_scope_note_parts.append(f"缺口总量 {maintenance_gap_fund_count} 只")
    if maintenance_actionable_governance_pool_total > 0:
        maintenance_scope_note_parts.append(f"当前显式治理池 {maintenance_actionable_governance_pool_total} 只")
    if maintenance_scope_filtered_no_retry_total > 0:
        maintenance_scope_note_parts.append(
            f"另有 scope_filtered_no_retry {maintenance_scope_filtered_no_retry_total} 只未进入当前治理池"
        )
    maintenance_scope_note_parts.append("与长期快照全集统计分母不同，不直接相加。")
    maintenance_scope_note = "；".join(maintenance_scope_note_parts)
    annual_ready_ratio_after = _to_float(
        (coverage_progress.get("after") or {}).get("annual_history_ready_ratio")
    )
    rank_ready_ratio_after = _to_float(
        (coverage_progress.get("after") or {}).get("rank_history_ready_ratio")
    )
    blocking_reason = _clean_text(snapshot.get("blocking_reason"))

    return {
        "candidate_fund_count": _to_int(snapshot.get("candidate_fund_count")),
        "annual_feature_ready_fund_count": _to_int(snapshot.get("annual_feature_ready_fund_count")),
        "rank_feature_ready_fund_count": _to_int(snapshot.get("rank_feature_ready_fund_count")),
        "full_feature_ready_fund_count": _to_int(snapshot.get("full_feature_ready_fund_count")),
        "annual_feature_gap_fund_count": _to_int(snapshot.get("annual_feature_gap_fund_count")),
        "rank_feature_gap_fund_count": _to_int(snapshot.get("rank_feature_gap_fund_count")),
        "full_feature_gap_fund_count": _to_int(snapshot.get("full_feature_gap_fund_count")),
        "annual_history_missing_total": annual_history_missing_total,
        "annual_feature_pending_maturity_fund_count": annual_feature_pending_maturity_fund_count,
        "annual_feature_actionable_gap_fund_count": annual_feature_actionable_gap_fund_count,
        "annual_feature_metadata_unknown_fund_count": annual_feature_metadata_unknown_fund_count,
        "annual_feature_actionable_missing_history_fund_count": annual_feature_actionable_missing_history_fund_count,
        "annual_feature_actionable_sparse_year_row_gap_fund_count": annual_feature_actionable_sparse_year_row_gap_fund_count,
        "annual_feature_actionable_null_year_gap_fund_count": annual_feature_actionable_null_year_gap_fund_count,
        "annual_feature_upstream_semantic_null_year_gap_fund_count": annual_feature_upstream_semantic_null_year_gap_fund_count,
        "annual_feature_upstream_limited_gap_fund_count": annual_feature_upstream_limited_gap_fund_count,
        "annual_feature_upstream_limited_missing_history_fund_count": annual_feature_upstream_limited_missing_history_fund_count,
        "annual_feature_upstream_limited_sparse_year_row_gap_fund_count": annual_feature_upstream_limited_sparse_year_row_gap_fund_count,
        "annual_feature_upstream_limited_null_year_gap_fund_count": annual_feature_upstream_limited_null_year_gap_fund_count,
        "annual_feature_upstream_limited_gap_samples": annual_feature_upstream_limited_gap_samples[:8],
        "annual_semantic_null_bridge_summary": annual_semantic_null_bridge_summary,
        "semantic_null_snapshot_audit_summary": semantic_null_snapshot_audit_summary,
        "annual_feature_pending_maturity_excluding_semantic_null_fund_count": (
            annual_feature_pending_maturity_excluding_semantic_null_fund_count
        ),
        "rank_history_missing_total": rank_history_missing_total,
        "annual_feature_stale_fund_count": annual_feature_stale_fund_count,
        "rank_feature_stale_fund_count": rank_feature_stale_fund_count,
        "full_feature_stale_fund_count": full_feature_stale_fund_count,
        "neutralized_fund_count": _to_int(snapshot.get("neutralized_fund_count")),
        "neutralized_coverage_ratio": _to_float(snapshot.get("neutralized_coverage_ratio")),
        "annual_feature_ready_coverage_ratio": _to_float(
            snapshot.get("annual_feature_ready_coverage_ratio")
        ),
        "rank_feature_ready_coverage_ratio": _to_float(
            snapshot.get("rank_feature_ready_coverage_ratio")
        ),
        "full_feature_ready_coverage_ratio": _to_float(
            snapshot.get("full_feature_ready_coverage_ratio")
        ),
        "ready_for_scoring": bool(snapshot.get("ready_for_scoring")),
        "blocking_reason": blocking_reason,
        "coverage_progress": coverage_progress,
        "upstream_observation_summary": upstream_observation_summary,
        "priority_blocked_summary": priority_blocked_summary,
        "failed_reason_governance_summary": failed_reason_governance_summary,
        "failed_reason_summary": failed_reason_summary,
        "maintenance_status": detail.get("status"),
        "maintenance_no_action_category": detail.get("no_action_category"),
        "maintenance_idle_reason": detail.get("idle_reason"),
        "maintenance_processed_total": _to_int(detail.get("processed_total")),
        "maintenance_changed_total": _to_int(detail.get("changed_total")),
        "snapshot_scope_label": snapshot_scope_label,
        "snapshot_scope_note": snapshot_scope_note,
        "maintenance_scope_label": maintenance_scope_label,
        "maintenance_scope_note": maintenance_scope_note,
        "maintenance_gap_fund_count": maintenance_gap_fund_count,
        "maintenance_candidate_total": maintenance_candidate_total,
        "maintenance_screenable_candidate_total": maintenance_screenable_candidate_total,
        "maintenance_buyable_candidate_total": maintenance_buyable_candidate_total,
        "maintenance_scope_filtered_no_retry_total": maintenance_scope_filtered_no_retry_total,
        "maintenance_actionable_governance_pool_total": maintenance_actionable_governance_pool_total,
        "scope_filtered_no_retry_summary": scope_filtered_no_retry_summary,
        "upstream_limited_structure_summary": upstream_limited_structure_summary,
        "priority_scope_not_ready_summary": priority_scope_not_ready_summary,
        "buyable_active_stale_scope_recoverability_summary": (
            buyable_active_stale_scope_recoverability_summary
        ),
        "buyable_active_stale_scope_samples": buyable_active_stale_scope_samples[:8],
        "await_history_maturity_count": await_history_maturity_count,
        "local_retry_pool_count": local_retry_pool_count,
        "local_retry_window_pending_count": pending_retry_window_total,
        "local_retry_cooldown_pending_count": pending_retry_cooldown_total,
        "local_retry_ready_count": local_retry_ready_count,
        "local_retry_other_pending_count": local_retry_other_pending_count,
        "local_retry_next_retry_after_min": _clean_text(
            upstream_observation_summary.get("pending_next_retry_after_min")
            or (fallback_next_retry_values[0] if fallback_next_retry_values else None)
        ),
        "local_retry_next_retry_after_max": _clean_text(
            upstream_observation_summary.get("pending_next_retry_after_max")
            or (fallback_next_retry_values[-1] if fallback_next_retry_values else None)
        ),
        "upstream_limited_watchlist_count": upstream_limited_watchlist_count,
        "priority_blocked_total": priority_blocked_total,
        "priority_blocked_total_raw": priority_blocked_total_raw,
        "priority_blocked_total_excluding_stale_scope_not_ready": (
            priority_blocked_total_excluding_stale_scope_not_ready
        ),
        "upstream_limited_current_watchlist_total": _to_int(
            upstream_limited_structure_summary.get("current_watchlist_total")
        ),
        "upstream_limited_non_watchlist_observe_total": _to_int(
            upstream_limited_structure_summary.get("non_watchlist_observe_total")
        ),
        "upstream_limited_semantic_null_observe_total": _to_int(
            upstream_limited_structure_summary.get("semantic_null_observe_total")
        ),
        "upstream_limited_non_watchlist_semantic_null_total": _to_int(
            upstream_limited_structure_summary.get("non_watchlist_semantic_null_total")
        ),
        "upstream_limited_stale_no_progress_watchlist_total": _to_int(
            upstream_limited_structure_summary.get("stale_no_progress_watchlist_total")
        ),
        "upstream_limited_empty_payload_watchlist_total": _to_int(
            upstream_limited_structure_summary.get("empty_payload_watchlist_total")
        ),
        "upstream_limited_buyable_active_total": _to_int(
            upstream_limited_structure_summary.get("buyable_active_total")
        ),
        "upstream_limited_publishable_total": _to_int(
            upstream_limited_structure_summary.get("publishable_total")
        ),
        "upstream_limited_screenable_total": _to_int(
            upstream_limited_structure_summary.get("screenable_total")
        ),
        "buyable_active_metadata_blocked_total": buyable_active_metadata_blocked_total,
        "buyable_active_scope_not_ready_total": buyable_active_scope_not_ready_total,
        "high_priority_scope_not_ready_total": high_priority_scope_not_ready_total,
        "scope_not_ready_total": scope_not_ready_total,
        "stale_scope_not_ready_total": stale_scope_not_ready_total,
        "stale_upstream_limited_watchlist_count": _to_int(
            upstream_observation_summary.get("stale_upstream_limited_watchlist_total")
        ),
        "terminal_upstream_limited_watchlist_count": _to_int(
            upstream_observation_summary.get("terminal_upstream_limited_watchlist_total")
        ),
        "scope_filtered_upstream_limited_total": _to_int(
            upstream_observation_summary.get("scope_filtered_upstream_limited_total")
        ),
        "upstream_limited_empty_payload_total": upstream_limited_empty_payload_total,
        "governance_pool_summary": [
            {
                "pool_key": "await_history_maturity",
                "label": "等待历史成熟",
                "count": await_history_maturity_count,
                "note": (
                    "maintenance 观察池中的历史窗口未成熟样本；"
                    f"年度 5 年特征自然未成熟总量 {annual_feature_pending_maturity_fund_count} 只另行展示。"
                ),
            },
            {
                "pool_key": "local_retry_pool",
                "label": "本地重试池",
                "count": local_retry_pool_count,
                "note": "具备继续本地补采价值，按冷却窗口重试。",
            },
            {
                "pool_key": "upstream_limited_watchlist",
                "label": "上游受限观察池",
                "count": upstream_limited_watchlist_count,
                "note": (
                    f"当前执行观察 {upstream_limited_watchlist_count} 只中，"
                    f"当前观察池 {_to_int(upstream_limited_structure_summary.get('current_watchlist_total'))} 只，"
                    f"语义空值观察 {_to_int(upstream_limited_structure_summary.get('semantic_null_observe_total'))} 只；"
                    f"快照年度上游受限缺口 {annual_feature_upstream_limited_gap_fund_count} 只；"
                    f"scope_filtered 上游受限尾部 "
                    f"{_to_int(upstream_limited_structure_summary.get('scope_filtered_total'))} 只另行展示。"
                ),
            },
            {
                "pool_key": "priority_blocked",
                "label": "高优先级阻塞池",
                "count": priority_blocked_total,
                "note": (
                    "买入活跃样本因元数据缺口或范围状态未恢复而暂未进入执行池；"
                    f"当前正式口径 {priority_blocked_total} 只；"
                    f"另有 stale scope_not_ready {stale_scope_not_ready_total} 只。"
                ),
            },
            {
                "pool_key": "stale_scope_not_ready",
                "label": "Stale 范围未恢复池",
                "count": stale_scope_not_ready_total,
                "note": (
                    "已降级为恢复观察的问题样本；"
                    f"其中深度停更 {stale_scope_not_ready_summary.get('deep_stale_candidate_total') or 0} 只，"
                    f"接近终态停更 {stale_scope_not_ready_summary.get('terminal_candidate_total') or 0} 只。"
                ),
            },
        ],
        "local_retry_samples": local_retry_samples[:5],
        "priority_blocked_samples": priority_blocked_samples[:8],
        "stale_scope_not_ready_summary": stale_scope_not_ready_summary,
        "stale_scope_not_ready_samples": stale_scope_not_ready_summary.get("samples") or [],
        "scope_filtered_no_retry_samples": list(scope_filtered_no_retry_summary.get("samples") or [])[:8],
        "upstream_limited_watchlist_samples": upstream_limited_watchlist_samples[:8],
        "headline": (
            f"长期快照全集 {_to_int(snapshot.get('candidate_fund_count'))} 只基金中，"
            f"长周期全特征就绪率 {_to_float(snapshot.get('full_feature_ready_coverage_ratio')) * 100:.2f}%，"
            f"仍有 {_to_int(snapshot.get('neutralized_fund_count'))} 只基金以中性化方式进入后续链路；"
            f"其中年度 5 年特征自然未成熟 {annual_feature_pending_maturity_fund_count} 只，"
            f"其中上游语义空值子集 {annual_feature_upstream_semantic_null_year_gap_fund_count} 只，"
            f"当前执行观察 {annual_semantic_null_bridge_summary.get('execution_total') or 0} 只，"
            f"其余待成熟解释层 {annual_semantic_null_bridge_summary.get('remaining_pending_maturity_layer_total') or 0} 只，"
            f"快照外 scope-filtered 尾部 {annual_semantic_null_bridge_summary.get('scope_filtered_tail_total') or 0} 只，"
            f"年度特征可执行缺口 {annual_feature_actionable_gap_fund_count} 只，"
            f"年度上游受限缺口 {annual_feature_upstream_limited_gap_fund_count} 只，"
            f"排名历史 stale {rank_feature_stale_fund_count} 只；"
            f"当前维护治理池口径仅覆盖维护候选集 {maintenance_candidate_total} 只，"
            f"其中等待历史成熟 {await_history_maturity_count} 只，本地重试池 {local_retry_pool_count} 只，"
            f"上游受限执行观察 {upstream_limited_watchlist_count} 只"
            f"（当前观察池 {_to_int(upstream_limited_structure_summary.get('current_watchlist_total'))} 只，"
            f"语义空值观察 {_to_int(upstream_limited_structure_summary.get('semantic_null_observe_total'))} 只），"
            f"高优先级阻塞池 {priority_blocked_total} 只，"
            f"stale scope_not_ready {stale_scope_not_ready_total} 只；"
            f"另有 scope_filtered_no_retry {maintenance_scope_filtered_no_retry_total} 只。"
            "两组统计分母不同，不直接相加。"
        ),
        "coverage_snapshot": {
            "annual_history_ready_ratio_after": annual_ready_ratio_after,
            "rank_history_ready_ratio_after": rank_ready_ratio_after,
        },
        "explanation": (
            f"长期快照全集统计分母为 {_to_int(snapshot.get('candidate_fund_count'))} 只，"
            f"当前维护治理池统计分母为维护候选集 {maintenance_candidate_total} 只；"
            f"当前阻塞原因为 {blocking_reason or 'none'}；"
            f"年度历史缺口 {annual_history_missing_total} 只，排名历史缺口 {rank_history_missing_total} 只；"
            f"其中年度历史 stale {annual_feature_stale_fund_count} 只，排名历史 stale {rank_feature_stale_fund_count} 只；"
            f"其中年度 5 年特征自然未成熟 {annual_feature_pending_maturity_fund_count} 只，"
            f"其中上游语义空值子集 {annual_feature_upstream_semantic_null_year_gap_fund_count} 只，"
            f"其中当前执行观察 {annual_semantic_null_bridge_summary.get('execution_total') or 0} 只，"
            f"剩余未进入执行池 {annual_semantic_null_bridge_summary.get('remaining_total') or 0} 只，"
            f"快照外 scope-filtered 尾部 {annual_semantic_null_bridge_summary.get('scope_filtered_tail_total') or 0} 只，"
            f"当前纯待成熟非语义空值 {annual_feature_pending_maturity_excluding_semantic_null_fund_count} 只；"
            f"年度特征可执行缺口 {annual_feature_actionable_gap_fund_count} 只，"
            f"成立年限未知或无法归类 {annual_feature_metadata_unknown_fund_count} 只；"
            f"当前可执行缺口中，原始年度行缺失 {annual_feature_actionable_missing_history_fund_count} 只，"
            f"年度行数不足 5 条 {annual_feature_actionable_sparse_year_row_gap_fund_count} 只，"
            f"已有 5 年行但存在空值缺口 {annual_feature_actionable_null_year_gap_fund_count} 只；"
            f"年度上游受限缺口 {annual_feature_upstream_limited_gap_fund_count} 只，"
            f"其中原始年度行缺失 {annual_feature_upstream_limited_missing_history_fund_count} 只，"
            f"年度行数不足 5 条 {annual_feature_upstream_limited_sparse_year_row_gap_fund_count} 只，"
            f"已有 5 年行但存在空值缺口 {annual_feature_upstream_limited_null_year_gap_fund_count} 只；"
            f"当前本地重试池 {local_retry_pool_count} 只"
            f"（窗口等待 {pending_retry_window_total} 只，冷却中 {pending_retry_cooldown_total} 只，"
            f"可立即重试 {local_retry_ready_count} 只，其它待复核 {local_retry_other_pending_count} 只）；"
            f"上游受限观察池 {upstream_limited_watchlist_count} 只，"
            f"其中上游受限执行观察 {upstream_limited_watchlist_count} 只"
            f"（当前观察池 {_to_int(upstream_limited_structure_summary.get('current_watchlist_total'))} 只，"
            f"其中 stale_no_progress {_to_int(upstream_limited_structure_summary.get('stale_no_progress_watchlist_total'))} 只，"
            f"空载荷 {_to_int(upstream_limited_structure_summary.get('empty_payload_watchlist_total'))} 只，"
            f"terminal {_to_int(upstream_limited_structure_summary.get('terminal_watchlist_total'))} 只；"
            f"语义空值观察 {_to_int(upstream_limited_structure_summary.get('semantic_null_observe_total'))} 只；"
            f"buyable-active {_to_int(upstream_limited_structure_summary.get('buyable_active_total'))} 只，"
            f"publishable {_to_int(upstream_limited_structure_summary.get('publishable_total'))} 只，"
            f"screenable {_to_int(upstream_limited_structure_summary.get('screenable_total'))} 只）；"
            f"scope_filtered 上游受限尾部 {_to_int(upstream_limited_structure_summary.get('scope_filtered_total'))} 只，"
            f"其中空载荷 {_to_int(upstream_limited_structure_summary.get('scope_filtered_empty_payload_total'))} 只，"
            f"语义空值 {_to_int(upstream_limited_structure_summary.get('scope_filtered_semantic_null_total'))} 只；"
            f"累计空载荷观测 {upstream_limited_empty_payload_total} 只；"
            f"买入活跃但元数据阻塞 {buyable_active_metadata_blocked_total} 只，"
            f"买入活跃但范围未恢复 {buyable_active_scope_not_ready_total} 只；"
            f"全部 scope_not_ready {scope_not_ready_total} 只，其中 stale scope_not_ready {stale_scope_not_ready_total} 只；"
            f"其中 scope_filtered_no_retry {maintenance_scope_filtered_no_retry_total} 只当前全部落在 deferred_priority；"
            f"其中 holding_period_blocked { _to_int(scope_filtered_no_retry_summary.get('holding_period_blocked_total')) } 只，"
            f"runtime_excluded { _to_int(scope_filtered_no_retry_summary.get('runtime_excluded_total')) } 只；"
            f"stale scope_not_ready 中 screenable_priority "
            f"{_to_int(priority_scope_not_ready_summary.get('screenable_priority_total'))} 只，"
            f"buyable_active_priority {_to_int(priority_scope_not_ready_summary.get('buyable_active_priority_total'))} 只；"
            f"其中 buyable stale recoverable "
            f"{_to_int(buyable_active_stale_scope_recoverability_summary.get('recoverable_total'))} 只"
            f"（fresh_0_30d "
            f"{_to_int(buyable_active_stale_scope_recoverability_summary.get('fresh_recoverable_total'))}，"
            f"short_31_60d "
            f"{_to_int(buyable_active_stale_scope_recoverability_summary.get('short_recoverable_total'))}）。"
        ),
    }
