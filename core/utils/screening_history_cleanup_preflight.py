#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选历史清理执行前置核对（只读）。

目标：
1. 核对当前 live DB 与最新 preview / runbook 是否一致；
2. 在真实删除前识别 preview 过期、批次计划漂移、明细行数变化；
3. 输出可审计的 JSON / Markdown 产物，不执行任何删除。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

from sqlalchemy import func

from database.connection import get_db_manager, resolve_db_path
from database.models import ScreeningRun
from database.screening_history_maintenance import build_recent_duplicate_success_cleanup_plan
from database.screening_run_storage_stats import (
    count_detail_rows_for_run_ids,
    fetch_materialized_counts,
)
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_history_cleanup_preview import (
    load_latest_screening_history_cleanup_preview_payload,
)
from utils.screening_history_cleanup_runbook import (
    load_latest_screening_history_cleanup_runbook_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCREENING_HISTORY_CLEANUP_PREFLIGHT_JSON_PATTERN = "screening_history_cleanup_preflight_*.json"


def load_latest_screening_history_cleanup_preflight_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选历史 cleanup preflight 产物。"""
    return load_latest_json_artifact(
        SCREENING_HISTORY_CLEANUP_PREFLIGHT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_run_ids(run_ids: Optional[Sequence[str]]) -> list[str]:
    normalized = []
    seen = set()
    for item in run_ids or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _normalize_delete_batch_run_limit(value) -> int:
    try:
        parsed = int(value or 100)
    except (TypeError, ValueError):
        parsed = 100
    return max(1, parsed)


def _resolve_artifact_path(path_value: str) -> Path:
    path = Path(str(path_value or "").strip()).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def _build_findings(*, summary: dict, checks: dict, added_run_ids: Sequence[str], missing_run_ids: Sequence[str]) -> list[str]:
    findings = []

    if checks.get("ready_for_manual_signoff"):
        findings.append(
            "当前 live DB 与 preview / runbook 范围一致，可进入人工签字阶段；执行前仍需先做单文件备份。"
        )
    else:
        findings.append(
            "当前 live DB 与 preview / runbook 未完全对齐，进入真实删除前应先重新生成 preview / runbook 或重新做范围复核。"
        )

    if not checks.get("current_plan_matches_preview_scope"):
        findings.append(
            f"当前 live 近窗重复成功 run 范围与 preview 不一致：新增 {summary.get('scope_added_run_count', 0)} 个，缺失 {summary.get('scope_missing_run_count', 0)} 个。"
        )

    if not checks.get("preview_scope_rows_match_preview_summary"):
        findings.append(
            f"preview 记录的待删行数与当前 live scope 不一致：decision 预期 {summary.get('preview_decisions_to_delete', 0)} / 当前 {summary.get('live_scope_decisions_to_delete', 0)}，"
            f"pick 预期 {summary.get('preview_picks_to_delete', 0)} / 当前 {summary.get('live_scope_picks_to_delete', 0)}。"
        )

    if not checks.get("runbook_batch_plan_matches_preview_scope"):
        findings.append(
            f"runbook 拆批计划与 preview scope 不一致：runbook 批次 {summary.get('runbook_batch_count', 0)}，按上限 {summary.get('runbook_delete_batch_run_limit', 0)} 应为 {summary.get('expected_batch_count_from_preview_scope', 0)}。"
        )

    if added_run_ids:
        findings.append(
            "live 范围新增 run_id 样本: " + ", ".join(added_run_ids[:10])
        )
    if missing_run_ids:
        findings.append(
            "preview 中已缺失的 run_id 样本: " + ", ".join(missing_run_ids[:10])
        )

    return findings


def _count_preview_scope_rows(session, run_ids: Sequence[str]) -> dict:
    """统计 preview scope 待删明细行数，优先使用 run 级物化统计。"""
    normalized_run_ids = _normalize_run_ids(run_ids)
    if not normalized_run_ids:
        return {
            "decision_count": 0,
            "pick_count": 0,
            "count_source": "empty_scope",
            "storage_stats_covered_run_count": 0,
            "storage_stats_missing_run_count": 0,
            "storage_stats_missing_run_samples": [],
        }

    materialized_decisions, materialized_picks, covered_count, missing_run_ids = fetch_materialized_counts(
        session,
        normalized_run_ids,
    )
    detail_decisions = 0
    detail_picks = 0
    if missing_run_ids:
        detail_decisions, detail_picks = count_detail_rows_for_run_ids(session, missing_run_ids)

    if not missing_run_ids:
        count_source = "materialized"
    elif covered_count > 0:
        count_source = "mixed"
    else:
        count_source = "detail_scan_fallback"

    return {
        "decision_count": int(materialized_decisions + detail_decisions),
        "pick_count": int(materialized_picks + detail_picks),
        "count_source": count_source,
        "storage_stats_covered_run_count": int(covered_count),
        "storage_stats_missing_run_count": int(len(missing_run_ids)),
        "storage_stats_missing_run_samples": list(missing_run_ids[:20]),
    }


def build_screening_history_cleanup_preflight_payload(
    *,
    db_manager=None,
    db_path: Optional[str] = None,
    preview_payload: Optional[dict] = None,
    runbook_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    source_runbook_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> dict:
    """构建筛选历史 cleanup 执行前置核对载荷。"""
    if preview_payload is None:
        if source_preview_artifact:
            preview_path = _resolve_artifact_path(source_preview_artifact)
            preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
            source_preview_artifact = str(preview_path)
        else:
            preview_path, preview_payload = load_latest_screening_history_cleanup_preview_payload(output_dir=output_dir)
            source_preview_artifact = str(preview_path)
    if runbook_payload is None:
        if source_runbook_artifact:
            runbook_path = _resolve_artifact_path(source_runbook_artifact)
            runbook_payload = json.loads(runbook_path.read_text(encoding="utf-8"))
            source_runbook_artifact = str(runbook_path)
        else:
            runbook_path, runbook_payload = load_latest_screening_history_cleanup_runbook_payload(output_dir=output_dir)
            source_runbook_artifact = str(runbook_path)

    preview_payload = dict(preview_payload or {})
    runbook_payload = dict(runbook_payload or {})
    preview_summary = dict(preview_payload.get("summary") or {})
    preview_parameters = dict(preview_payload.get("parameters") or {})
    preview_execution_plan = dict(preview_payload.get("execution_plan") or {})
    runbook_summary = dict(runbook_payload.get("summary") or {})
    runbook_batches = list(runbook_payload.get("batches") or [])

    preview_scope_run_ids = _normalize_run_ids(preview_execution_plan.get("scope_run_ids") or [])
    recent_days = max(1, int(preview_parameters.get("recent_days") or 30))
    fund_types = [
        str(item).strip()
        for item in (preview_parameters.get("fund_types") or [])
        if str(item).strip()
    ] or None

    manager = db_manager or get_db_manager(db_path=db_path)
    resolved_db_path = Path(getattr(manager, "db_path", resolve_db_path(db_path or preview_payload.get("db_path") or "database/bond_funds.db")))

    with manager.get_session(task_name="screening_history_cleanup_preflight", readonly=True) as session:
        current_plan = build_recent_duplicate_success_cleanup_plan(
            session,
            recent_days=recent_days,
            fund_types=fund_types,
            max_sample_runs=20,
        )
        current_candidate_run_ids = _normalize_run_ids(current_plan.pop("_candidate_run_ids", []) or [])

        live_scope_existing_run_count = 0
        live_scope_decisions_to_delete = 0
        live_scope_picks_to_delete = 0
        live_scope_count_result = _count_preview_scope_rows(session, preview_scope_run_ids)
        if preview_scope_run_ids:
            live_scope_existing_run_count = int(session.query(func.count(ScreeningRun.id)).filter(
                ScreeningRun.run_id.in_(preview_scope_run_ids)
            ).scalar() or 0)
            live_scope_decisions_to_delete = int(live_scope_count_result.get("decision_count") or 0)
            live_scope_picks_to_delete = int(live_scope_count_result.get("pick_count") or 0)

    preview_scope_set = set(preview_scope_run_ids)
    current_scope_set = set(current_candidate_run_ids)
    added_run_ids = sorted(current_scope_set - preview_scope_set)
    missing_run_ids = sorted(preview_scope_set - current_scope_set)

    batch_limit = _normalize_delete_batch_run_limit(runbook_summary.get("delete_batch_run_limit") or 100)
    expected_batch_count = (
        (len(preview_scope_run_ids) + batch_limit - 1) // batch_limit
        if preview_scope_run_ids else
        0
    )
    runbook_batch_row_total = sum(int(batch.get("batch_run_count") or 0) for batch in runbook_batches)

    checks = {
        "runbook_references_preview_artifact": bool(source_preview_artifact) and (
            str(runbook_payload.get("source_screening_history_cleanup_preview_artifact") or "").strip()
            == str(source_preview_artifact).strip()
        ),
        "preview_scope_run_ids_exist_in_db": live_scope_existing_run_count == len(preview_scope_run_ids),
        "preview_scope_rows_match_preview_summary": (
            int(preview_summary.get("decisions_to_delete") or 0) == live_scope_decisions_to_delete
            and int(preview_summary.get("picks_to_delete") or 0) == live_scope_picks_to_delete
        ),
        "current_plan_matches_preview_scope": (
            len(current_candidate_run_ids) == len(preview_scope_run_ids)
            and current_scope_set == preview_scope_set
        ),
        "current_plan_counts_match_preview_summary": (
            int(current_plan.get("candidate_run_count") or 0) == int(preview_summary.get("candidate_run_count") or 0)
            and int(current_plan.get("decisions_to_delete") or 0) == int(preview_summary.get("decisions_to_delete") or 0)
            and int(current_plan.get("picks_to_delete") or 0) == int(preview_summary.get("picks_to_delete") or 0)
        ),
        "runbook_batch_plan_matches_preview_scope": (
            int(runbook_summary.get("delete_batch_count") or 0) == expected_batch_count
            and len(runbook_batches) == expected_batch_count
            and runbook_batch_row_total == len(preview_scope_run_ids)
            and all(int(batch.get("batch_run_count") or 0) <= batch_limit for batch in runbook_batches)
        ),
    }
    checks["ready_for_manual_signoff"] = all(checks.values())

    summary = {
        "recent_days": recent_days,
        "fund_types": fund_types or [],
        "preview_candidate_run_count": int(preview_summary.get("candidate_run_count") or 0),
        "preview_decisions_to_delete": int(preview_summary.get("decisions_to_delete") or 0),
        "preview_picks_to_delete": int(preview_summary.get("picks_to_delete") or 0),
        "preview_scope_run_id_total": len(preview_scope_run_ids),
        "live_scope_existing_run_count": live_scope_existing_run_count,
        "live_scope_decisions_to_delete": live_scope_decisions_to_delete,
        "live_scope_picks_to_delete": live_scope_picks_to_delete,
        "live_scope_count_source": live_scope_count_result.get("count_source") or "unknown",
        "live_scope_storage_stats_covered_run_count": int(
            live_scope_count_result.get("storage_stats_covered_run_count") or 0
        ),
        "live_scope_storage_stats_missing_run_count": int(
            live_scope_count_result.get("storage_stats_missing_run_count") or 0
        ),
        "live_scope_storage_stats_missing_run_samples": list(
            live_scope_count_result.get("storage_stats_missing_run_samples") or []
        ),
        "current_candidate_run_count": int(current_plan.get("candidate_run_count") or 0),
        "current_decisions_to_delete": int(current_plan.get("decisions_to_delete") or 0),
        "current_picks_to_delete": int(current_plan.get("picks_to_delete") or 0),
        "scope_added_run_count": len(added_run_ids),
        "scope_missing_run_count": len(missing_run_ids),
        "runbook_batch_count": len(runbook_batches),
        "runbook_batch_row_total": runbook_batch_row_total,
        "runbook_delete_batch_run_limit": batch_limit,
        "expected_batch_count_from_preview_scope": expected_batch_count,
        "ready_for_manual_signoff": checks["ready_for_manual_signoff"],
    }

    return {
        "generated_at": datetime.now().isoformat(),
        "db_path": str(resolved_db_path),
        "source_screening_history_cleanup_preview_artifact": source_preview_artifact or "",
        "source_screening_history_cleanup_runbook_artifact": source_runbook_artifact or "",
        "source_screening_history_cleanup_preview_generated_at": preview_payload.get("generated_at") or "",
        "source_screening_history_cleanup_runbook_generated_at": runbook_payload.get("generated_at") or "",
        "summary": summary,
        "checks": checks,
        "drift": {
            "added_run_ids_sample": added_run_ids[:20],
            "missing_run_ids_sample": missing_run_ids[:20],
        },
        "findings": _build_findings(
            summary=summary,
            checks=checks,
            added_run_ids=added_run_ids,
            missing_run_ids=missing_run_ids,
        ),
    }


def render_screening_history_cleanup_preflight_markdown(payload: dict) -> str:
    """渲染 Markdown 前置核对报告。"""
    summary = payload.get("summary") or {}
    checks = payload.get("checks") or {}
    drift = payload.get("drift") or {}

    lines = [
        "# screening history cleanup preflight",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- db_path: {payload.get('db_path')}",
        f"- source_preview_artifact: {payload.get('source_screening_history_cleanup_preview_artifact')}",
        f"- source_runbook_artifact: {payload.get('source_screening_history_cleanup_runbook_artifact')}",
        f"- ready_for_manual_signoff: {summary.get('ready_for_manual_signoff')}",
        f"- preview_candidate_run_count: {summary.get('preview_candidate_run_count', 0)}",
        f"- current_candidate_run_count: {summary.get('current_candidate_run_count', 0)}",
        f"- preview_decisions_to_delete: {summary.get('preview_decisions_to_delete', 0)}",
        f"- live_scope_decisions_to_delete: {summary.get('live_scope_decisions_to_delete', 0)}",
        f"- preview_picks_to_delete: {summary.get('preview_picks_to_delete', 0)}",
        f"- live_scope_picks_to_delete: {summary.get('live_scope_picks_to_delete', 0)}",
        f"- live_scope_count_source: {summary.get('live_scope_count_source')}",
        f"- live_scope_storage_stats_covered_run_count: {summary.get('live_scope_storage_stats_covered_run_count', 0)}",
        f"- live_scope_storage_stats_missing_run_count: {summary.get('live_scope_storage_stats_missing_run_count', 0)}",
        f"- runbook_batch_count: {summary.get('runbook_batch_count', 0)}",
        f"- expected_batch_count_from_preview_scope: {summary.get('expected_batch_count_from_preview_scope', 0)}",
        "",
        "## checks",
    ]
    for key, value in checks.items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## drift"])
    lines.append(f"- added_run_ids_sample: {json.dumps(drift.get('added_run_ids_sample') or [], ensure_ascii=False)}")
    lines.append(f"- missing_run_ids_sample: {json.dumps(drift.get('missing_run_ids_sample') or [], ensure_ascii=False)}")
    return "\n".join(lines).strip() + "\n"


def write_screening_history_cleanup_preflight_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出筛选历史 cleanup preflight JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    if not out_dir.is_absolute():
        out_dir = (Path.cwd() / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"screening_history_cleanup_preflight_{ts}.json"
    md_path = out_dir / f"screening_history_cleanup_preflight_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_history_cleanup_preflight_markdown(payload), encoding="utf-8")
    return json_path, md_path
