#!/usr/bin/env python3
"""Read-only completion audit for database hot/cold optimization.

The existing database governance health audit answers whether the current
maintenance evidence is healthy. This module answers the next question: whether
the broader database optimization target is fully complete, still under
controlled observation, or blocked.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple

from utils.database_governance_health_audit import (
    DEFAULT_OUTPUT_DIR as DATABASE_GOVERNANCE_HEALTH_OUTPUT_DIR,
    load_latest_database_governance_health_audit_payload,
)
from utils.fund_code_alias_audit import load_latest_json_artifact


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/database_governance_health"
DATABASE_OPTIMIZATION_COMPLETION_AUDIT_JSON_PATTERN = (
    "database_optimization_completion_audit_*.json"
)
REQUIRED_HOT_TABLES = (
    "fund_nav_history",
    "fund_rank_history",
    "screening_decisions",
    "screening_picks",
)


def load_latest_database_optimization_completion_audit_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        DATABASE_OPTIMIZATION_COMPLETION_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _round_mb(size_bytes: int | float | None) -> float:
    try:
        return round(float(size_bytes or 0) / (1024 * 1024), 2)
    except (TypeError, ValueError):
        return 0.0


def _resolve_path(value: str | Path | None, *, project_root: Path) -> Path | None:
    if value is None:
        return None
    text = _normalize_text(value)
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def _resolve_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _file_snapshot(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"path": "", "exists": False, "size_bytes": 0, "size_mb": 0.0}
    exists = path.exists()
    size_bytes = path.stat().st_size if exists else 0
    return {
        "path": str(path),
        "exists": exists,
        "size_bytes": int(size_bytes),
        "size_mb": _round_mb(size_bytes),
        "mtime": (
            datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
            if exists
            else ""
        ),
    }


def _latest_file_snapshot(directory: Path, pattern: str) -> dict[str, Any]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return {
            "path": "",
            "exists": False,
            "size_bytes": 0,
            "size_mb": 0.0,
            "pattern": pattern,
        }
    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    payload = _file_snapshot(latest)
    payload["pattern"] = pattern
    return payload


def _component_status(items: list[dict[str, Any]]) -> str:
    if any(item.get("status") == "blocked" for item in items):
        return "blocked"
    if any(item.get("status") == "review" for item in items):
        return "review"
    return "ok"


def _append_item(
    items: list[dict[str, Any]],
    *,
    key: str,
    status: str,
    label: str,
    detail: str,
    severity: str = "info",
    action_hint: str = "",
    evidence: dict[str, Any] | None = None,
) -> None:
    items.append(
        {
            "key": key,
            "status": status,
            "severity": severity,
            "label": label,
            "detail": detail,
            "action_hint": action_hint,
            "evidence": evidence or {},
        }
    )


def _read_sqlite_runtime_profile(db_path: Path | None) -> dict[str, Any]:
    if db_path is None:
        return {
            "status": "blocked",
            "database_path": "",
            "exists": False,
            "error": "database_path_not_configured",
        }
    if not db_path.exists():
        return {
            "status": "blocked",
            "database_path": str(db_path),
            "exists": False,
            "error": "database_missing",
        }
    size_bytes = db_path.stat().st_size
    if size_bytes <= 0:
        return {
            "status": "blocked",
            "database_path": str(db_path),
            "exists": True,
            "size_bytes": int(size_bytes),
            "size_mb": _round_mb(size_bytes),
            "error": "database_zero_byte",
        }

    profile: dict[str, Any] = {
        "status": "ok",
        "database_path": str(db_path),
        "exists": True,
        "size_bytes": int(size_bytes),
        "size_mb": _round_mb(size_bytes),
        "wal_exists": db_path.with_name(db_path.name + "-wal").exists(),
        "shm_exists": db_path.with_name(db_path.name + "-shm").exists(),
    }
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=15)
        conn.row_factory = sqlite3.Row
        with conn:
            page_size = _normalize_int(conn.execute("PRAGMA page_size").fetchone()[0])
            page_count = _normalize_int(conn.execute("PRAGMA page_count").fetchone()[0])
            freelist_count = _normalize_int(
                conn.execute("PRAGMA freelist_count").fetchone()[0]
            )
            journal_mode = _normalize_text(
                conn.execute("PRAGMA journal_mode").fetchone()[0]
            )
            table_rows = conn.execute(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
            table_names = [str(row["name"]) for row in table_rows]
            missing_required_tables = [
                table for table in REQUIRED_HOT_TABLES if table not in set(table_names)
            ]
            key_table_indexes: dict[str, list[str]] = {}
            for table_name in REQUIRED_HOT_TABLES:
                if table_name not in set(table_names):
                    continue
                index_rows = conn.execute(f"PRAGMA index_list({table_name})").fetchall()
                key_table_indexes[table_name] = [
                    str(row["name"]) for row in index_rows if row["name"]
                ]
    except Exception as exc:
        profile.update({"status": "blocked", "error": f"{type(exc).__name__}: {exc}"})
        return profile
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass

    freelist_bytes = page_size * freelist_count
    profile.update(
        {
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "freelist_bytes": freelist_bytes,
            "freelist_mb": _round_mb(freelist_bytes),
            "freelist_ratio": round(
                freelist_count / page_count, 6
            )
            if page_count > 0
            else 0.0,
            "journal_mode": journal_mode,
            "table_count": len(table_names),
            "required_hot_tables": list(REQUIRED_HOT_TABLES),
            "missing_required_hot_tables": missing_required_tables,
            "key_table_indexes": key_table_indexes,
        }
    )
    if missing_required_tables:
        profile["status"] = "blocked"
        profile["error"] = "required_hot_tables_missing"
    return profile


def _latest_health_payload(
    explicit_payload: dict[str, Any] | None,
    *,
    output_dir: Path,
) -> tuple[str, dict[str, Any]]:
    if isinstance(explicit_payload, dict) and explicit_payload:
        return "", explicit_payload
    try:
        path, payload = load_latest_database_governance_health_audit_payload(
            output_dir=str(output_dir)
        )
        return str(path), payload
    except Exception:
        return "", {}


def _find_health_component(health_payload: dict[str, Any], component_name: str) -> dict:
    for component in health_payload.get("components") or []:
        if _normalize_text((component or {}).get("component")) == component_name:
            return dict(component or {})
    return {}


def _find_health_item(component: dict[str, Any], key: str) -> dict:
    for item in component.get("items") or []:
        if _normalize_text((item or {}).get("key")) == key:
            return dict(item or {})
    return {}


def _build_runtime_component(runtime_profile: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    if runtime_profile.get("status") == "blocked":
        _append_item(
            items,
            key="sqlite_runtime_blocked",
            status="blocked",
            label="SQLite runtime unavailable",
            detail=_normalize_text(runtime_profile.get("error")) or "database unavailable",
            severity="error",
            evidence=runtime_profile,
        )
    else:
        _append_item(
            items,
            key="sqlite_runtime_readable",
            status="ok",
            label="SQLite runtime readable",
            detail=(
                f"size_mb={runtime_profile.get('size_mb')}; "
                f"journal_mode={runtime_profile.get('journal_mode')}; "
                f"freelist_mb={runtime_profile.get('freelist_mb')}"
            ),
            evidence=runtime_profile,
        )
        freelist_ratio = float(runtime_profile.get("freelist_ratio") or 0.0)
        if freelist_ratio >= 0.15:
            _append_item(
                items,
                key="sqlite_freelist_high",
                status="review",
                label="SQLite freelist is high",
                detail=f"freelist_ratio={freelist_ratio}; vacuum governance should reclaim space.",
                severity="warning",
                evidence={
                    "freelist_mb": runtime_profile.get("freelist_mb"),
                    "freelist_ratio": freelist_ratio,
                },
            )
        if runtime_profile.get("missing_required_hot_tables"):
            _append_item(
                items,
                key="required_hot_tables_missing",
                status="blocked",
                label="Required hot tables missing",
                detail=", ".join(runtime_profile.get("missing_required_hot_tables") or []),
                severity="error",
            )

    return {
        "component": "sqlite_runtime",
        "status": _component_status(items),
        "items": items,
    }


def _build_health_component(
    health_payload: dict[str, Any],
    *,
    health_artifact_path: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    summary = dict(health_payload.get("summary") or {})
    health_status = _normalize_text(summary.get("status"))
    if not health_payload:
        _append_item(
            items,
            key="database_governance_health_missing",
            status="review",
            label="Database governance health evidence missing",
            detail="No database_governance_health_audit artifact was available.",
            severity="warning",
            action_hint="Keep the weekly database governance health audit enabled.",
        )
    elif health_status == "blocked":
        _append_item(
            items,
            key="database_governance_health_blocked",
            status="blocked",
            label="Database governance health blocked",
            detail=(
                f"blocked_item_count={summary.get('blocked_item_count')}; "
                f"review_item_count={summary.get('review_item_count')}"
            ),
            severity="error",
            evidence={"artifact_path": health_artifact_path, "summary": summary},
        )
    elif health_status == "review":
        _append_item(
            items,
            key="database_governance_health_review",
            status="review",
            label="Database governance health has review items",
            detail=(
                f"blocked_item_count={summary.get('blocked_item_count')}; "
                f"review_item_count={summary.get('review_item_count')}"
            ),
            severity="warning",
            evidence={"artifact_path": health_artifact_path, "summary": summary},
        )
    else:
        _append_item(
            items,
            key="database_governance_health_ok",
            status="ok",
            label="Database governance health ok",
            detail=(
                f"component_count={summary.get('component_count')}; "
                f"blocked_item_count={summary.get('blocked_item_count')}; "
                f"review_item_count={summary.get('review_item_count')}"
            ),
            evidence={"artifact_path": health_artifact_path, "summary": summary},
        )

    return {
        "component": "governance_health_rollup",
        "status": _component_status(items),
        "items": items,
    }


def _build_archive_file_component(
    *,
    archive_dir: Path,
    screening_archive_path: Path,
) -> dict[str, Any]:
    nav_archive = _latest_file_snapshot(archive_dir, "fund_nav_history_before_*.db")
    rank_archive = _latest_file_snapshot(archive_dir, "fund_rank_history_before_*.db")
    screening_archive = _file_snapshot(screening_archive_path)
    items: list[dict[str, Any]] = []
    for key, label, snapshot in (
        ("nav_cold_archive_file", "NAV cold archive file", nav_archive),
        ("rank_cold_archive_file", "rank cold archive file", rank_archive),
        ("screening_cold_archive_file", "screening cold archive file", screening_archive),
    ):
        if snapshot.get("exists"):
            _append_item(
                items,
                key=key,
                status="ok",
                label=label,
                detail=f"size_mb={snapshot.get('size_mb')}; path={snapshot.get('path')}",
                evidence=snapshot,
            )
        else:
            _append_item(
                items,
                key=key,
                status="review",
                label=f"{label} missing",
                detail=f"No matching archive file found; pattern={snapshot.get('pattern') or snapshot.get('path')}",
                severity="warning",
                action_hint="Confirm whether the archive has no scope or the cold export has not run yet.",
                evidence=snapshot,
            )

    return {
        "component": "cold_archive_files",
        "status": _component_status(items),
        "items": items,
    }


def _build_screening_cleanup_completion_component(
    health_payload: dict[str, Any],
    scheduler_config: dict[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    component = _find_health_component(health_payload, "screening_history_hot_cold_cleanup")
    dry_run_item = _find_health_item(component, "screening_cleanup_dry_run")
    apply_item = _find_health_item(component, "screening_cleanup_apply_guarded")
    cleanup_mode = _normalize_text(scheduler_config.get("screening_cleanup_mode"))
    cleanup_enabled = _normalize_bool(
        scheduler_config.get("screening_cleanup_enabled"),
        default=True,
    )

    if not cleanup_enabled:
        _append_item(
            items,
            key="screening_cleanup_disabled_for_completion",
            status="review",
            label="screening cleanup disabled",
            detail="screening_history_cleanup_execution is disabled by config.",
            severity="warning",
        )
    elif apply_item:
        item_status = _normalize_text(apply_item.get("status"))
        _append_item(
            items,
            key="screening_cleanup_apply_completed",
            status="ok" if item_status == "ok" else "blocked",
            label="screening cleanup apply completed",
            detail=_normalize_text(apply_item.get("detail")) or f"status={item_status}",
            severity="info" if item_status == "ok" else "error",
            evidence=apply_item,
        )
    elif dry_run_item:
        item_status = _normalize_text(dry_run_item.get("status"))
        if item_status == "blocked":
            _append_item(
                items,
                key="screening_cleanup_apply_readiness_blocked",
                status="blocked",
                label="screening cleanup apply readiness blocked",
                detail=_normalize_text(dry_run_item.get("detail")),
                severity="error",
                evidence=dry_run_item,
            )
        else:
            _append_item(
                items,
                key="screening_cleanup_apply_pending_approval",
                status="review",
                label="screening cleanup apply pending approval",
                detail=(
                    f"configured_mode={cleanup_mode or 'unknown'}; "
                    f"{_normalize_text(dry_run_item.get('detail'))}"
                ),
                severity="warning",
                action_hint=(
                    "Keep dry-run in production until explicit approval is given for "
                    "the first guarded apply window."
                ),
                evidence=dry_run_item,
            )
    else:
        _append_item(
            items,
            key="screening_cleanup_completion_evidence_missing",
            status="review",
            label="screening cleanup completion evidence missing",
            detail="No screening cleanup dry-run/apply item was found in governance health.",
            severity="warning",
        )

    return {
        "component": "screening_cleanup_completion",
        "status": _component_status(items),
        "items": items,
    }


def _build_scheduler_config_component(scheduler_config: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    required_flags = {
        "nav_archive_enabled": "NAV archive automation",
        "nav_vacuum_enabled": "NAV vacuum automation",
        "rank_archive_enabled": "rank archive automation",
        "rank_vacuum_enabled": "rank vacuum automation",
        "screening_archive_execution_enabled": "screening archive automation",
        "screening_cleanup_enabled": "screening cleanup automation",
        "database_backup_cleanup_execution_enabled": "database backup cleanup automation",
        "database_backup_compression_execution_enabled": "database backup compression automation",
        "database_governance_health_audit_enabled": "database governance health audit",
        "database_optimization_completion_audit_enabled": "database optimization completion audit",
    }
    if not scheduler_config:
        _append_item(
            items,
            key="scheduler_database_config_missing",
            status="review",
            label="scheduler database config missing",
            detail="No scheduler config snapshot was supplied.",
            severity="warning",
        )
    for key, label in required_flags.items():
        enabled = _normalize_bool(scheduler_config.get(key), default=False)
        _append_item(
            items,
            key=key,
            status="ok" if enabled else "review",
            label=label,
            detail=f"enabled={enabled}; value={scheduler_config.get(key)}",
            severity="info" if enabled else "warning",
            action_hint="" if enabled else "Enable this task for fully automatic governance.",
        )

    cleanup_mode = _normalize_text(scheduler_config.get("screening_cleanup_mode"))
    if cleanup_mode and cleanup_mode != "apply":
        _append_item(
            items,
            key="screening_cleanup_mode_not_apply",
            status="review",
            label="screening cleanup mode not apply",
            detail=f"configured_mode={cleanup_mode}",
            severity="warning",
            action_hint="This is intentional until explicit approval for guarded apply.",
        )

    return {
        "component": "scheduler_automation_config",
        "status": _component_status(items),
        "items": items,
    }


QUERY_SURFACE_STATIC_CHECKS = (
    {
        "key": "nav_query_union_helper",
        "path": "core/database/nav_history_queries.py",
        "tokens": (
            "def build_nav_history_union_source_sql",
            "UNION ALL",
            "archive_cutoff_date",
        ),
    },
    {
        "key": "rank_query_union_helper",
        "path": "core/database/rank_history_queries.py",
        "tokens": (
            "def build_rank_history_union_source_sql",
            "UNION ALL",
            "archive_cutoff_date",
        ),
    },
    {
        "key": "report_builder_hot_cold_and_pick_summary",
        "path": "backend/report_builder.py",
        "tokens": (
            "build_nav_history_union_source_sql",
            "build_rank_history_union_source_sql",
            "screening_pick_metric_snapshots",
        ),
    },
    {
        "key": "long_term_maintenance_hot_cold",
        "path": "backend/long_term_history_maintenance.py",
        "tokens": (
            "build_nav_history_union_source_sql",
            "build_rank_history_union_source_sql",
            "attach_nav_history_archive_if_available",
            "attach_rank_history_archive_if_available",
        ),
    },
    {
        "key": "final_screening_latest_nav_hot_cold",
        "path": "core/bond_fund_screening/tasks/final_screening_database_helpers.py",
        "tokens": (
            "build_nav_history_union_source_sql",
            "attach_nav_history_archive_if_available",
        ),
    },
    {
        "key": "screening_cleanup_materialized_totals",
        "path": "core/utils/screening_history_cleanup_preview.py",
        "tokens": (
            "fetch_materialized_totals",
            "screening_run_storage_stats",
        ),
    },
    {
        "key": "screening_preflight_materialized_scope",
        "path": "core/utils/screening_history_cleanup_preflight.py",
        "tokens": (
            "fetch_materialized_counts",
            "count_detail_rows_for_run_ids",
        ),
    },
    {
        "key": "screening_growth_summary_tables",
        "path": "core/utils/screening_storage_growth_audit.py",
        "tokens": (
            "screening_run_latest_activity",
            "screening_run_storage_stats",
        ),
    },
)


def _build_query_surface_component(*, project_root: Path) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    check_results: list[dict[str, Any]] = []
    for check in QUERY_SURFACE_STATIC_CHECKS:
        relative_path = Path(str(check.get("path") or ""))
        source_path = (project_root / relative_path).resolve()
        exists = source_path.exists() and source_path.is_file()
        source_text = ""
        if exists:
            try:
                source_text = source_path.read_text(encoding="utf-8")
            except OSError:
                source_text = ""
        missing_tokens = [
            token
            for token in check.get("tokens", ())
            if str(token or "") and str(token) not in source_text
        ]
        check_results.append(
            {
                "key": check.get("key"),
                "path": str(relative_path),
                "exists": exists,
                "missing_tokens": missing_tokens,
                "ok": bool(exists and not missing_tokens),
            }
        )

    missing_checks = [item for item in check_results if not item.get("ok")]
    if missing_checks:
        _append_item(
            items,
            key="query_surface_full_scan_review_pending",
            status="review",
            label="query surface review pending",
            detail=f"{len(missing_checks)} query-surface compatibility check(s) lack static evidence.",
            severity="warning",
            action_hint=(
                "Keep report, long-term, final-screening and screening-history audit "
                "read paths on hot/cold-aware helpers or materialized summaries."
            ),
            evidence={"checks": check_results},
        )
    else:
        _append_item(
            items,
            key="query_surface_hot_cold_summary_paths_verified",
            status="ok",
            label="query surface hot/cold and summary paths verified",
            detail=f"{len(check_results)} static query-surface compatibility checks passed.",
            evidence={"checks": check_results},
        )
    return {
        "component": "query_surface_followup",
        "status": _component_status(items),
        "items": items,
    }


def build_database_optimization_completion_audit_payload(
    *,
    db_path: str | Path,
    project_root: str | Path | None = None,
    reports_dir: str | Path = "reports",
    generated_at: str | None = None,
    database_governance_health_audit: dict[str, Any] | None = None,
    scheduler_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_project_root = Path(project_root or PROJECT_ROOT).resolve()
    resolved_db_path = _resolve_path(db_path, project_root=resolved_project_root)
    resolved_reports_dir = _resolve_path(reports_dir, project_root=resolved_project_root)
    resolved_health_output_dir = (
        (resolved_reports_dir or (resolved_project_root / "reports"))
        / "audit"
        / "database_governance_health"
    )
    archive_dir = resolved_project_root / "backend" / "data" / "archives"
    screening_archive_path = archive_dir / "screening_history_details_archive.db"
    health_artifact_path, health_payload = _latest_health_payload(
        database_governance_health_audit,
        output_dir=resolved_health_output_dir,
    )
    runtime_profile = _read_sqlite_runtime_profile(resolved_db_path)
    config_payload = dict(scheduler_config or {})

    components = [
        _build_runtime_component(runtime_profile),
        _build_health_component(health_payload, health_artifact_path=health_artifact_path),
        _build_archive_file_component(
            archive_dir=archive_dir,
            screening_archive_path=screening_archive_path,
        ),
        _build_screening_cleanup_completion_component(health_payload, config_payload),
        _build_scheduler_config_component(config_payload),
        _build_query_surface_component(project_root=resolved_project_root),
    ]
    blocked_items = [
        item
        for component in components
        for item in component.get("items", [])
        if item.get("status") == "blocked"
    ]
    review_items = [
        item
        for component in components
        for item in component.get("items", [])
        if item.get("status") == "review"
    ]
    component_status_counts: dict[str, int] = {}
    for component in components:
        status = _normalize_text(component.get("status")) or "unknown"
        component_status_counts[status] = component_status_counts.get(status, 0) + 1

    status = "complete"
    if blocked_items:
        status = "blocked"
    elif review_items:
        status = "review"

    findings: list[str] = []
    if blocked_items:
        findings.append(
            f"Database optimization completion is blocked by {len(blocked_items)} item(s)."
        )
        findings.extend(_normalize_text(item.get("detail") or item.get("label")) for item in blocked_items[:5])
    elif review_items:
        findings.append(
            f"Database optimization is operational but not final-complete; {len(review_items)} review item(s) remain."
        )
        findings.extend(_normalize_text(item.get("detail") or item.get("label")) for item in review_items[:5])
    else:
        findings.append("Database hot/cold optimization target is complete by current automated checks.")

    return {
        "generated_at": generated_at or datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "status": status,
            "component_count": len(components),
            "component_status_counts": component_status_counts,
            "blocked_item_count": len(blocked_items),
            "review_item_count": len(review_items),
            "final_completion_ready": status == "complete",
            "automatic_hot_cold_governance_operational": status != "blocked",
            "database_size_bytes": runtime_profile.get("size_bytes", 0),
            "database_size_mb": runtime_profile.get("size_mb", 0.0),
            "freelist_mb": runtime_profile.get("freelist_mb", 0.0),
            "reports_dir": str(resolved_reports_dir or ""),
        },
        "components": components,
        "blocked_items": blocked_items,
        "review_items": review_items,
        "findings": findings,
        "source_artifacts": {
            "database_governance_health_audit": health_artifact_path,
        },
        "runtime_profile": runtime_profile,
        "scheduler_config": config_payload,
    }


def render_database_optimization_completion_audit_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# database optimization completion audit",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- status: {summary.get('status')}",
        f"- final_completion_ready: {summary.get('final_completion_ready')}",
        f"- automatic_hot_cold_governance_operational: {summary.get('automatic_hot_cold_governance_operational')}",
        f"- database_size_mb: {summary.get('database_size_mb')}",
        f"- freelist_mb: {summary.get('freelist_mb')}",
        f"- blocked_item_count: {summary.get('blocked_item_count', 0)}",
        f"- review_item_count: {summary.get('review_item_count', 0)}",
        "",
        "## findings",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## components"])
    for component in payload.get("components") or []:
        lines.append(f"- {component.get('component')}: {component.get('status')}")
        for item in component.get("items") or []:
            lines.append(
                "  - "
                + json.dumps(
                    {
                        "key": item.get("key"),
                        "status": item.get("status"),
                        "severity": item.get("severity"),
                        "detail": item.get("detail"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
    return "\n".join(lines).strip() + "\n"


def write_database_optimization_completion_audit_artifacts(
    payload: dict[str, Any],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: str | None = None,
) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = _resolve_output_dir(output_dir)
    json_path = artifact_dir / f"database_optimization_completion_audit_{ts}.json"
    md_path = artifact_dir / f"database_optimization_completion_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_database_optimization_completion_audit_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
