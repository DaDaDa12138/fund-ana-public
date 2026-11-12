#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 冷归档查询兼容审计。

该审计只读运行，用于证明真实归档前哪些生产/分析路径会跨越
archive cutoff 依赖 fund_nav_history 冷窗口。它不执行归档、删除或 VACUUM。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_maintenance_readiness_audit import (
    load_latest_nav_history_archive_maintenance_readiness_audit_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NAV_HISTORY_ARCHIVE_COLD_QUERY_COMPAT_AUDIT_JSON_PATTERN = (
    "nav_history_archive_cold_query_compat_audit_*.json"
)
DEFAULT_OUTPUT_DIR = "reports/audit/nav_history_archive"
DEFAULT_RISK_PERIODS = ("1y", "3y", "5y")
DEFAULT_FORWARD_WINDOWS = (
    ("1个月", 30),
    ("3个月", 90),
    ("6个月", 180),
    ("12个月", 365),
)
HOT_COLD_COMPAT_COVERED_PATHS = {
    "long_term_annual_feature_nav_year_window",
    "latest_nav_record_lookup",
}


def load_latest_nav_history_archive_cold_query_compat_audit_payload(
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_COLD_QUERY_COMPAT_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _resolve_project_path(value: str | Path, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        resolved = path.resolve()
        if resolved.exists():
            return resolved
        parts = resolved.parts
        for marker in ("backend", "reports", "config", "core", "scripts", "frontend"):
            if marker in parts:
                return (project_root / Path(*parts[parts.index(marker):])).resolve()
        return resolved
    return (project_root / path).resolve()


def _project_relative(path: str | Path, *, project_root: Path = PROJECT_ROOT) -> str:
    resolved = _resolve_project_path(path, project_root=project_root)
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _parse_date(value: Any) -> Optional[date]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def _period_days(period: str) -> Optional[int]:
    return {"1y": 365, "3y": 365 * 3, "5y": 365 * 5}.get(str(period or "").strip())


def _dedupe_dates(values: list[date]) -> list[date]:
    return sorted(set(values))


def _fetchone_dict(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict[str, Any]:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def _fetchall_dicts(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    return [{key: row[key] for key in row.keys()} for row in conn.execute(sql, params).fetchall()]


def _count_nav_rows(conn: sqlite3.Connection, start_date: str, end_date: str) -> dict[str, int]:
    row = _fetchone_dict(
        conn,
        """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT fcode) AS fund_count
        FROM fund_nav_history
        WHERE nav_date >= ? AND nav_date <= ?
        """,
        (start_date, end_date),
    )
    return {
        "row_count": int(row.get("row_count") or 0),
        "fund_count": int(row.get("fund_count") or 0),
    }


def _count_cold_overlap(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    cutoff_date: str,
) -> dict[str, int]:
    row = _fetchone_dict(
        conn,
        """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT fcode) AS fund_count
        FROM fund_nav_history
        WHERE nav_date >= ? AND nav_date <= ? AND nav_date < ?
        """,
        (start_date, end_date, cutoff_date),
    )
    return {
        "row_count": int(row.get("row_count") or 0),
        "fund_count": int(row.get("fund_count") or 0),
    }


def _build_risk_window_dependencies(
    conn: sqlite3.Connection,
    *,
    latest_nav_date: date,
    cutoff_date: date,
    risk_periods: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = []
    for period in risk_periods:
        days = _period_days(period)
        if not days:
            continue
        start = latest_nav_date - timedelta(days=days)
        end = latest_nav_date
        window_counts = _count_nav_rows(conn, start.isoformat(), end.isoformat())
        cold_overlap = _count_cold_overlap(
            conn,
            start.isoformat(),
            end.isoformat(),
            cutoff_date.isoformat(),
        )
        crosses_cutoff = start < cutoff_date
        rows.append(
            {
                "path_key": f"risk_metrics_{period}",
                "category": "production_risk_metrics",
                "period": period,
                "window_start_date": start.isoformat(),
                "window_end_date": end.isoformat(),
                "crosses_archive_cutoff": crosses_cutoff,
                "cold_overlap_row_count": cold_overlap["row_count"],
                "cold_overlap_fund_count": cold_overlap["fund_count"],
                "window_row_count": window_counts["row_count"],
                "window_fund_count": window_counts["fund_count"],
                "requires_cold_query_after_archive": crosses_cutoff
                and cold_overlap["row_count"] > 0,
                "hot_cold_query_compat_covered": (
                    f"risk_metrics_{period}" in HOT_COLD_COMPAT_COVERED_PATHS
                ),
                "default_hot_path_safe_after_archive": not (
                    crosses_cutoff and cold_overlap["row_count"] > 0
                ),
                "decision": (
                    "cold_query_compat_required"
                    if crosses_cutoff and cold_overlap["row_count"] > 0
                    else "hot_path_sufficient"
                ),
            }
        )
    return rows


def _build_long_term_annual_dependency(
    conn: sqlite3.Connection,
    *,
    cutoff_date: date,
    min_years: int = 5,
) -> dict[str, Any]:
    last_complete_year = datetime.now().date().year - 1
    first_year = last_complete_year - max(1, int(min_years or 5)) + 1
    start = date(first_year, 1, 1)
    end = date(last_complete_year, 12, 31)
    window_counts = _count_nav_rows(conn, start.isoformat(), end.isoformat())
    cold_overlap = _count_cold_overlap(
        conn,
        start.isoformat(),
        end.isoformat(),
        cutoff_date.isoformat(),
    )
    requires_cold = start < cutoff_date and cold_overlap["row_count"] > 0
    return {
        "path_key": "long_term_annual_feature_nav_year_window",
        "category": "production_long_term_maintenance",
        "window_years": list(range(first_year, last_complete_year + 1)),
        "window_start_date": start.isoformat(),
        "window_end_date": end.isoformat(),
        "crosses_archive_cutoff": start < cutoff_date,
        "cold_overlap_row_count": cold_overlap["row_count"],
        "cold_overlap_fund_count": cold_overlap["fund_count"],
        "window_row_count": window_counts["row_count"],
        "window_fund_count": window_counts["fund_count"],
        "requires_cold_query_after_archive": requires_cold,
        "hot_cold_query_compat_covered": (
            "long_term_annual_feature_nav_year_window" in HOT_COLD_COMPAT_COVERED_PATHS
        ),
        "default_hot_path_safe_after_archive": not requires_cold,
        "decision": "cold_query_compat_required" if requires_cold else "hot_path_sufficient",
    }


def _load_snapshot_dates(project_root: Path) -> list[date]:
    dates: list[date] = []
    latest_path = project_root / "reports/latest.json"
    report_paths: list[Path] = [latest_path] if latest_path.exists() else []
    for base_dir in (
        project_root / "reports/runs",
        project_root / "reports/archive/superseded_runs",
    ):
        if base_dir.exists():
            report_paths.extend(sorted(base_dir.glob("*/bond_analysis_report_*.json")))

    for report_path in report_paths:
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        snapshot_date = _parse_date(
            payload.get("snapshot_date")
            or (
                _normalize_dict(
                    _normalize_dict(payload.get("database_freshness")).get(
                        "effective_nav_snapshot"
                    )
                ).get("candidate_date")
            )
        )
        generated_at = _parse_date(payload.get("generated_at"))
        resolved_date = snapshot_date or generated_at
        if resolved_date is not None:
            dates.append(resolved_date)
    return _dedupe_dates(dates)


def _build_forward_validation_dependency(
    conn: sqlite3.Connection,
    *,
    project_root: Path,
    cutoff_date: date,
) -> dict[str, Any]:
    snapshot_dates = _load_snapshot_dates(project_root)
    if not snapshot_dates:
        latest_row = _fetchone_dict(conn, "SELECT MAX(nav_date) AS latest_nav_date FROM fund_nav_history")
        fallback_date = _parse_date(latest_row.get("latest_nav_date")) or datetime.now().date()
        snapshot_dates = [fallback_date]
    max_forward_days = max(days for _label, days in DEFAULT_FORWARD_WINDOWS)
    snapshot_dependencies = []
    for snapshot_date in snapshot_dates:
        start = snapshot_date - timedelta(days=7)
        end = snapshot_date + timedelta(days=max_forward_days + 10)
        window_counts = _count_nav_rows(conn, start.isoformat(), end.isoformat())
        cold_overlap = _count_cold_overlap(
            conn,
            start.isoformat(),
            end.isoformat(),
            cutoff_date.isoformat(),
        )
        snapshot_dependencies.append(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "window_start_date": start.isoformat(),
                "window_end_date": end.isoformat(),
                "crosses_archive_cutoff": start < cutoff_date,
                "cold_overlap_row_count": cold_overlap["row_count"],
                "cold_overlap_fund_count": cold_overlap["fund_count"],
                "window_row_count": window_counts["row_count"],
                "window_fund_count": window_counts["fund_count"],
                "requires_cold_query_after_archive": (
                    start < cutoff_date and cold_overlap["row_count"] > 0
                ),
            }
        )
    cold_required_snapshots = [
        item for item in snapshot_dependencies if item["requires_cold_query_after_archive"]
    ]
    earliest_window_start = min(item["window_start_date"] for item in snapshot_dependencies)
    latest_window_end = max(item["window_end_date"] for item in snapshot_dependencies)
    union_window_counts = _count_nav_rows(conn, earliest_window_start, latest_window_end)
    union_cold_overlap = _count_cold_overlap(
        conn,
        earliest_window_start,
        latest_window_end,
        cutoff_date.isoformat(),
    )
    requires_cold = bool(cold_required_snapshots)
    return {
        "path_key": "forward_validation_nav_window",
        "category": "analysis_forward_validation",
        "snapshot_count": len(snapshot_dates),
        "earliest_snapshot_date": min(snapshot_dates).isoformat(),
        "latest_snapshot_date": max(snapshot_dates).isoformat(),
        "window_start_date": earliest_window_start,
        "window_end_date": latest_window_end,
        "crosses_archive_cutoff": any(
            item["crosses_archive_cutoff"] for item in snapshot_dependencies
        ),
        "cold_required_snapshot_count": len(cold_required_snapshots),
        "cold_required_snapshot_samples": cold_required_snapshots[:10],
        "cold_overlap_row_count": union_cold_overlap["row_count"],
        "cold_overlap_fund_count": union_cold_overlap["fund_count"],
        "window_row_count": union_window_counts["row_count"],
        "window_fund_count": union_window_counts["fund_count"],
        "requires_cold_query_after_archive": requires_cold,
        "hot_cold_query_compat_covered": (
            "forward_validation_nav_window" in HOT_COLD_COMPAT_COVERED_PATHS
        ),
        "default_hot_path_safe_after_archive": not requires_cold,
        "decision": "cold_query_compat_required" if requires_cold else "hot_path_sufficient",
    }


def _build_latest_nav_dependency(
    conn: sqlite3.Connection,
    *,
    cutoff_date: date,
) -> dict[str, Any]:
    row = _fetchone_dict(
        conn,
        """
        SELECT COUNT(*) AS fund_count
        FROM (
            SELECT fcode, MAX(nav_date) AS latest_nav_date
            FROM fund_nav_history
            GROUP BY fcode
        )
        WHERE latest_nav_date < ?
        """,
        (cutoff_date.isoformat(),),
    )
    stale_latest_fund_count = int(row.get("fund_count") or 0)
    return {
        "path_key": "latest_nav_record_lookup",
        "category": "production_latest_snapshot",
        "window_start_date": "",
        "window_end_date": "",
        "crosses_archive_cutoff": False,
        "cold_overlap_row_count": 0,
        "cold_overlap_fund_count": stale_latest_fund_count,
        "requires_cold_query_after_archive": stale_latest_fund_count > 0,
        "hot_cold_query_compat_covered": (
            "latest_nav_record_lookup" in HOT_COLD_COMPAT_COVERED_PATHS
        ),
        "default_hot_path_safe_after_archive": stale_latest_fund_count == 0,
        "decision": (
            "stale_funds_need_explicit_cold_lookup"
            if stale_latest_fund_count > 0
            else "hot_path_sufficient"
        ),
    }


def build_nav_history_archive_cold_query_compat_audit_payload(
    *,
    db_path: str | Path,
    readiness_payload: Optional[dict] = None,
    source_readiness_artifact: Optional[str | Path] = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    project_root: str | Path | None = PROJECT_ROOT,
) -> dict:
    root = Path(project_root).resolve() if project_root else PROJECT_ROOT
    out_dir = _resolve_project_path(output_dir, project_root=root)
    if readiness_payload is None:
        if source_readiness_artifact:
            readiness_path = _resolve_project_path(source_readiness_artifact, project_root=root)
            readiness_payload = json.loads(readiness_path.read_text(encoding="utf-8"))
            source_readiness_artifact = _project_relative(readiness_path, project_root=root)
        else:
            readiness_path, readiness_payload = load_latest_nav_history_archive_maintenance_readiness_audit_payload(
                output_dir=str(out_dir)
            )
            source_readiness_artifact = _project_relative(readiness_path, project_root=root)
    readiness_payload = dict(readiness_payload or {})
    readiness_summary = _normalize_dict(readiness_payload.get("summary"))
    cutoff = _parse_date(readiness_summary.get("archive_cutoff_date"))
    if cutoff is None:
        raise ValueError("archive_cutoff_date is required for cold query compatibility audit")

    resolved_db_path = _resolve_project_path(db_path, project_root=root)
    conn = sqlite3.connect(f"file:{resolved_db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        nav_summary = _fetchone_dict(
            conn,
            """
            SELECT
                COUNT(*) AS total_row_count,
                COUNT(DISTINCT fcode) AS total_fund_count,
                MIN(nav_date) AS min_nav_date,
                MAX(nav_date) AS max_nav_date,
                SUM(CASE WHEN nav_date < ? THEN 1 ELSE 0 END) AS cold_candidate_row_count,
                COUNT(DISTINCT CASE WHEN nav_date < ? THEN fcode END) AS cold_candidate_fund_count
            FROM fund_nav_history
            """,
            (cutoff.isoformat(), cutoff.isoformat()),
        )
        latest_nav_date = _parse_date(nav_summary.get("max_nav_date"))
        if latest_nav_date is None:
            raise ValueError("fund_nav_history has no max nav_date")

        dependencies = []
        dependencies.extend(
            _build_risk_window_dependencies(
                conn,
                latest_nav_date=latest_nav_date,
                cutoff_date=cutoff,
                risk_periods=DEFAULT_RISK_PERIODS,
            )
        )
        dependencies.append(
            _build_long_term_annual_dependency(
                conn,
                cutoff_date=cutoff,
                min_years=5,
            )
        )
        dependencies.append(
            _build_forward_validation_dependency(
                conn,
                project_root=root,
                cutoff_date=cutoff,
            )
        )
        dependencies.append(_build_latest_nav_dependency(conn, cutoff_date=cutoff))
    finally:
        conn.close()

    production_dependencies = [
        item for item in dependencies if str(item.get("category") or "").startswith("production")
    ]
    production_cold_required = [
        item for item in production_dependencies if item.get("requires_cold_query_after_archive")
    ]
    production_uncovered_cold_required = [
        item
        for item in production_cold_required
        if not item.get("hot_cold_query_compat_covered")
    ]
    production_covered_cold_required = [
        item for item in production_cold_required if item.get("hot_cold_query_compat_covered")
    ]
    analysis_cold_required = [
        item for item in dependencies
        if not str(item.get("category") or "").startswith("production")
        and item.get("requires_cold_query_after_archive")
    ]
    query_compat_safe = len(production_uncovered_cold_required) == 0
    readiness_ready = bool(readiness_summary.get("ready_for_manual_window_execution"))
    safe_to_apply_archive_now = query_compat_safe and readiness_ready
    if not query_compat_safe:
        status = "blocked_by_hot_path_cold_dependency"
    elif not readiness_ready:
        status = "blocked_by_readiness_not_ready"
    else:
        status = "ready_for_archive_apply_querywise"
    if analysis_cold_required and safe_to_apply_archive_now:
        status = "ready_for_archive_apply_with_explicit_analysis_cold_path"

    summary = {
        "status": status,
        "query_compat_safe": query_compat_safe,
        "safe_to_apply_archive_now": safe_to_apply_archive_now,
        "archive_cutoff_date": cutoff.isoformat(),
        "readiness_status": readiness_summary.get("readiness_status") or "",
        "readiness_ready_for_manual_window_execution": bool(
            readiness_summary.get("ready_for_manual_window_execution")
        ),
        "total_nav_row_count": int(nav_summary.get("total_row_count") or 0),
        "total_nav_fund_count": int(nav_summary.get("total_fund_count") or 0),
        "min_nav_date": _normalize_text(nav_summary.get("min_nav_date")),
        "max_nav_date": _normalize_text(nav_summary.get("max_nav_date")),
        "cold_candidate_row_count": int(nav_summary.get("cold_candidate_row_count") or 0),
        "cold_candidate_fund_count": int(nav_summary.get("cold_candidate_fund_count") or 0),
        "dependency_count": len(dependencies),
        "production_dependency_count": len(production_dependencies),
        "production_cold_dependency_count": len(production_cold_required),
        "production_covered_cold_dependency_count": len(production_covered_cold_required),
        "production_uncovered_cold_dependency_count": len(production_uncovered_cold_required),
        "analysis_cold_dependency_count": len(analysis_cold_required),
        "page_apply_supported": False,
        "execution_mode": "manual_window_only",
    }
    blockers = []
    if production_uncovered_cold_required:
        blockers.append(
            "生产热路径仍存在 cutoff 前 NAV 依赖，真实归档前必须先实现 hot+cold 查询兼容层。"
        )
    if not summary["readiness_ready_for_manual_window_execution"]:
        blockers.append("维护窗口 readiness 尚未通过，不能进入真实归档 apply。")
    findings = []
    if blockers:
        findings.extend(blockers)
    elif production_covered_cold_required:
        findings.append(
            "生产路径仍存在 cutoff 前 NAV 依赖，但当前审计覆盖的生产依赖已具备 hot+cold 查询兼容层。"
        )
    else:
        findings.append("生产路径未发现 cutoff 前 NAV 依赖；分析路径若需冷数据必须显式走冷库。")
    for item in production_uncovered_cold_required[:5]:
        findings.append(
            f"{item['path_key']} 需要冷数据：{item.get('cold_overlap_row_count', 0)} 行 / "
            f"{item.get('cold_overlap_fund_count', 0)} 只基金。"
        )
    for item in production_covered_cold_required[:5]:
        findings.append(
            f"{item['path_key']} 仍需要冷数据，但已标记为 hot+cold 查询兼容覆盖。"
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(resolved_db_path),
        "source_nav_history_archive_maintenance_readiness_audit_artifact": (
            _normalize_text(source_readiness_artifact)
        ),
        "summary": summary,
        "dependencies": dependencies,
        "production_cold_required_dependencies": production_cold_required,
        "production_uncovered_cold_required_dependencies": production_uncovered_cold_required,
        "analysis_cold_required_dependencies": analysis_cold_required,
        "blockers": blockers,
        "findings": findings,
    }


def render_nav_history_archive_cold_query_compat_audit_markdown(payload: dict) -> str:
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive cold query compatibility audit",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- status: {summary.get('status', '')}",
        f"- query_compat_safe: {summary.get('query_compat_safe', False)}",
        f"- safe_to_apply_archive_now: {summary.get('safe_to_apply_archive_now', False)}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- production_cold_dependency_count: {summary.get('production_cold_dependency_count', 0)}",
        f"- production_covered_cold_dependency_count: {summary.get('production_covered_cold_dependency_count', 0)}",
        f"- production_uncovered_cold_dependency_count: {summary.get('production_uncovered_cold_dependency_count', 0)}",
        f"- analysis_cold_dependency_count: {summary.get('analysis_cold_dependency_count', 0)}",
        f"- page_apply_supported: {summary.get('page_apply_supported', False)}",
        f"- execution_mode: {summary.get('execution_mode', '')}",
        "",
        "## dependencies",
    ]
    for item in payload.get("dependencies") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")
    lines.extend(["", "## blockers"])
    for item in payload.get("blockers") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_cold_query_compat_audit_artifacts(
    payload: dict,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = _resolve_project_path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"nav_history_archive_cold_query_compat_audit_{ts}.json"
    md_path = out_dir / f"nav_history_archive_cold_query_compat_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        render_nav_history_archive_cold_query_compat_audit_markdown(payload),
        encoding="utf-8",
    )
    return json_path, md_path
