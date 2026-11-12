#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档执行前置核对（只读）。

目标：
1. 核对当前 live DB 与最新归档 runbook 是否仍一致；
2. 在真实归档 / 裁剪前识别冷数据行数、基金数和 cutoff 漂移；
3. 输出可审计的 JSON / Markdown 产物，不执行任何归档或删除。
"""

from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy import func

from database.connection import get_db_manager, resolve_db_path
from database.models import FundNavHistory
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_runbook import (
    load_latest_nav_history_archive_runbook_payload,
)
from utils.nav_history_archive_analysis import (
    _build_nav_history_aggregate_snapshot,
    build_nav_history_archive_strategy_preview,
    build_nav_history_retention_preview,
    build_nav_history_storage_profile,
    build_primary_database_hotspots,
)


NAV_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN = "nav_history_archive_preflight_*.json"
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_latest_nav_history_archive_preflight_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档 preflight 产物。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_cutoff_date(value: str):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _format_date_value(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    return str(value).strip()


def _build_findings(*, summary: dict, checks: dict) -> list[str]:
    findings = []

    if checks.get("ready_for_manual_signoff"):
        findings.append(
            "当前 live NAV 冷数据范围与 runbook 一致，可进入人工签字阶段；真实归档前仍需先做主库备份与归档目标校验。"
        )
    else:
        findings.append(
            "当前 live NAV 冷数据范围与 runbook 未完全对齐，进入真实归档前应先重新生成 runbook 或重新评估 cutoff。"
        )

    if not checks.get("live_counts_match_runbook_summary"):
        findings.append(
            f"runbook 预计归档 {summary.get('runbook_estimated_archive_row_count', 0)} 行 / {summary.get('runbook_estimated_archive_fund_count', 0)} 只基金，"
            f"当前 live 冷数据为 {summary.get('live_cold_row_count', 0)} 行 / {summary.get('live_cold_fund_count', 0)} 只基金。"
        )

    if not checks.get("strategy_matches_runbook_summary"):
        findings.append(
            f"当前策略推荐窗口或 cutoff 与 runbook 不一致：runbook={summary.get('runbook_recommended_retention_years')}年/{summary.get('runbook_archive_cutoff_date')}，"
            f"live strategy={summary.get('strategy_recommended_retention_years')}年/{summary.get('strategy_archive_cutoff_date')}。"
        )

    if not checks.get("cold_data_exists_for_archive"):
        findings.append("当前 live 主库中已无可归档冷数据，说明 runbook 可能已过时或主库已发生外部变更。")

    return findings


def build_nav_history_archive_preflight_payload(
    *,
    db_manager=None,
    db_path: Optional[str] = None,
    runbook_payload: Optional[dict] = None,
    source_runbook_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """构建 NAV 历史归档前置核对载荷。"""
    if runbook_payload is None:
        runbook_path, runbook_payload = load_latest_nav_history_archive_runbook_payload(output_dir=output_dir)
        source_runbook_artifact = str(runbook_path)

    runbook_payload = dict(runbook_payload or {})
    runbook_summary = _normalize_dict(runbook_payload.get("summary"))
    runbook_status = str(runbook_payload.get("status") or "").strip()
    archive_cutoff_date = str(runbook_summary.get("archive_cutoff_date") or "").strip()
    cutoff_date = _parse_cutoff_date(archive_cutoff_date)

    manager = db_manager or get_db_manager(db_path=db_path)
    resolved_db_path = Path(
        getattr(
            manager,
            "db_path",
            resolve_db_path(db_path or "database/bond_funds.db"),
        )
    )

    nav_history_aggregate_snapshot = _build_nav_history_aggregate_snapshot(manager)
    database_hotspots = build_primary_database_hotspots(manager)
    storage_profile = build_nav_history_storage_profile(
        manager,
        aggregate_snapshot=nav_history_aggregate_snapshot,
    )
    retention_preview = build_nav_history_retention_preview(
        manager,
        database_hotspots=database_hotspots,
        aggregate_snapshot=nav_history_aggregate_snapshot,
    )
    strategy_preview = build_nav_history_archive_strategy_preview(
        storage_profile,
        retention_preview,
    )
    strategy_summary = _normalize_dict(strategy_preview)

    with manager.get_session(task_name="nav_history_archive_preflight", readonly=True) as session:
        base_query = session.query(FundNavHistory)
        if cutoff_date is not None:
            base_query = base_query.filter(FundNavHistory.nav_date < cutoff_date)

        live_cold_row_count = int(base_query.with_entities(func.count(FundNavHistory.id)).scalar() or 0)
        live_cold_fund_count = int(
            base_query.with_entities(func.count(func.distinct(FundNavHistory.fcode))).scalar() or 0
        )
        live_min_nav_date = base_query.with_entities(func.min(FundNavHistory.nav_date)).scalar()
        live_max_nav_date = base_query.with_entities(func.max(FundNavHistory.nav_date)).scalar()

    strategy_recommended_retention_years = _normalize_int(strategy_summary.get("recommended_retention_years"))
    strategy_archive_cutoff_date = str(strategy_summary.get("archive_cutoff_date") or "").strip()
    strategy_estimated_archive_row_count = _normalize_int(strategy_summary.get("estimated_archive_row_count"))
    strategy_estimated_archive_fund_count = _normalize_int(strategy_summary.get("estimated_archive_fund_count"))

    runbook_estimated_archive_row_count = _normalize_int(runbook_summary.get("estimated_archive_row_count"))
    runbook_estimated_archive_fund_count = _normalize_int(runbook_summary.get("estimated_archive_fund_count"))
    runbook_recommended_retention_years = _normalize_int(runbook_summary.get("recommended_retention_years"))

    checks = {
        "runbook_available": bool(runbook_payload.get("available", True)),
        "runbook_status_supported": runbook_status in {"archive_candidate", "dense_fund_review", "observe"},
        "cold_data_exists_for_archive": live_cold_row_count > 0,
        "live_counts_match_runbook_summary": (
            live_cold_row_count == runbook_estimated_archive_row_count
            and live_cold_fund_count == runbook_estimated_archive_fund_count
        ),
        "strategy_matches_runbook_summary": (
            strategy_recommended_retention_years == runbook_recommended_retention_years
            and strategy_archive_cutoff_date == archive_cutoff_date
            and strategy_estimated_archive_row_count == runbook_estimated_archive_row_count
            and strategy_estimated_archive_fund_count == runbook_estimated_archive_fund_count
        ),
        "cutoff_is_valid": cutoff_date is not None,
    }
    checks["ready_for_manual_signoff"] = all(checks.values())

    summary = {
        "runbook_recommended_retention_years": runbook_recommended_retention_years,
        "runbook_archive_cutoff_date": archive_cutoff_date,
        "runbook_estimated_archive_row_count": runbook_estimated_archive_row_count,
        "runbook_estimated_archive_fund_count": runbook_estimated_archive_fund_count,
        "runbook_estimated_archive_size_mb": _normalize_float(runbook_summary.get("estimated_archive_size_mb")),
        "strategy_recommended_retention_years": strategy_recommended_retention_years,
        "strategy_archive_cutoff_date": strategy_archive_cutoff_date,
        "strategy_estimated_archive_row_count": strategy_estimated_archive_row_count,
        "strategy_estimated_archive_fund_count": strategy_estimated_archive_fund_count,
        "live_cold_row_count": live_cold_row_count,
        "live_cold_fund_count": live_cold_fund_count,
        "live_min_nav_date": _format_date_value(live_min_nav_date),
        "live_max_nav_date": _format_date_value(live_max_nav_date),
        "ready_for_manual_signoff": checks["ready_for_manual_signoff"],
    }

    return {
        "generated_at": datetime.now().isoformat(),
        "db_path": str(resolved_db_path),
        "source_nav_history_archive_runbook_artifact": source_runbook_artifact or "",
        "source_nav_history_archive_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "source_nav_history_archive_runbook_summary": runbook_summary,
        "current_nav_history_storage_profile_summary": storage_profile,
        "current_nav_history_retention_preview_summary": retention_preview,
        "current_nav_history_archive_strategy_preview_summary": strategy_preview,
        "summary": summary,
        "checks": checks,
        "findings": _build_findings(summary=summary, checks=checks),
    }


def render_nav_history_archive_preflight_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档 preflight Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    checks = _normalize_dict(payload.get("checks"))

    lines = [
        "# nav history archive preflight",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- db_path: {payload.get('db_path')}",
        f"- source_runbook_artifact: {payload.get('source_nav_history_archive_runbook_artifact')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff')}",
        f"- runbook_archive_cutoff_date: {summary.get('runbook_archive_cutoff_date', '')}",
        f"- runbook_estimated_archive_row_count: {summary.get('runbook_estimated_archive_row_count', 0)}",
        f"- live_cold_row_count: {summary.get('live_cold_row_count', 0)}",
        f"- live_cold_fund_count: {summary.get('live_cold_fund_count', 0)}",
        "",
        "## checks",
    ]
    for key, value in checks.items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_preflight_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档 preflight JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    if not out_dir.is_absolute():
        out_dir = (PROJECT_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"nav_history_archive_preflight_{ts}.json"
    md_path = out_dir / f"nav_history_archive_preflight_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_nav_history_archive_preflight_markdown(payload), encoding="utf-8")
    return json_path, md_path
