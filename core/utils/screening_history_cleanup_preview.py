#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选历史近窗重复成功 run 的 dry-run 预演产物。

目标：
1. 固化当前数据库中可裁剪的重复成功 run 范围；
2. 输出 exact run_id 范围 + 表影响 + 执行前后核对要点；
3. 全程只读，不执行任何数据库删除。
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

from sqlalchemy import func

from database.connection import get_db_manager, resolve_db_path
from database.models import ScreeningDecision, ScreeningPick, ScreeningRun
from database.screening_history_maintenance import (
    build_recent_duplicate_success_cleanup_plan,
)
from database.screening_run_storage_stats import fetch_materialized_totals
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact


SCREENING_HISTORY_CLEANUP_PREVIEW_JSON_PATTERN = "screening_history_cleanup_preview_*.json"


def load_latest_screening_history_cleanup_preview_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选历史 cleanup dry-run 预演产物。"""
    return load_latest_json_artifact(
        SCREENING_HISTORY_CLEANUP_PREVIEW_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_fund_types(fund_types: Optional[Sequence[str]]) -> list[str]:
    normalized = []
    seen = set()
    for item in fund_types or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _format_period_label(fund_type: str, period_years) -> str:
    fund_type_labels = {
        "bond": "债券",
        "stock": "股票",
        "hybrid": "混合",
        "lof": "LOF",
        "qdii": "QDII",
    }
    fund_label = fund_type_labels.get(
        str(fund_type or "").strip().lower(),
        str(fund_type or "未知").strip() or "未知",
    )

    try:
        numeric = float(period_years)
    except (TypeError, ValueError):
        period_label = str(period_years or "未知周期").strip() or "未知周期"
    else:
        period_label = f"{int(numeric)}年" if numeric.is_integer() else f"{numeric:g}年"

    return f"{fund_label} {period_label}"


def _serialize_run(row: ScreeningRun) -> dict:
    return {
        "run_id": row.run_id,
        "status": row.status,
        "fund_type": row.fund_type,
        "period_years": row.period_years,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "fund_count_in": row.fund_count_in,
        "fund_count_pass": row.fund_count_pass,
        "fund_count_fail": row.fund_count_fail,
    }


def _build_candidate_groups(candidate_rows: Sequence[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for row in candidate_rows or []:
        key = (str(row.get("fund_type") or "unknown"), row.get("period_years"))
        grouped[key].append(row)

    group_rows = []
    for (fund_type, period_years), rows in grouped.items():
        sorted_rows = sorted(
            rows,
            key=lambda item: (
                str(item.get("started_at") or ""),
                str(item.get("run_id") or ""),
            ),
            reverse=True,
        )
        run_ids = [str(item.get("run_id") or "").strip() for item in sorted_rows if str(item.get("run_id") or "").strip()]
        started_values = [str(item.get("started_at") or "").strip() for item in sorted_rows if str(item.get("started_at") or "").strip()]
        group_rows.append({
            "period_key": f"{fund_type}:{period_years}",
            "fund_type": fund_type,
            "period_years": period_years,
            "label": _format_period_label(fund_type, period_years),
            "candidate_run_count": len(sorted_rows),
            "latest_started_at": started_values[0] if started_values else None,
            "oldest_started_at": started_values[-1] if started_values else None,
            "run_ids": run_ids,
            "sample_run_ids": run_ids[:10],
        })

    return sorted(
        group_rows,
        key=lambda item: (
            -int(item.get("candidate_run_count") or 0),
            str(item.get("label") or ""),
        ),
    )


def _build_findings(summary: dict, candidate_groups: Sequence[dict]) -> list[str]:
    findings = []
    recent_success_run_count = int(summary.get("recent_success_run_count") or 0)
    kept_latest_run_count = int(summary.get("kept_latest_run_count") or 0)
    candidate_run_count = int(summary.get("candidate_run_count") or 0)
    zero_detail_candidate_run_count = int(summary.get("zero_detail_candidate_run_count") or 0)
    decisions_to_delete = int(summary.get("decisions_to_delete") or 0)
    picks_to_delete = int(summary.get("picks_to_delete") or 0)
    decision_share_pct = summary.get("candidate_decision_share_pct")
    pick_share_pct = summary.get("candidate_pick_share_pct")

    findings.append(
        f"最近 {summary.get('recent_days', 0)} 天成功 run {recent_success_run_count} 次，"
        f"按 `(fund_type, period_years)` 仅保留最新一次后，重复候选仍有 {candidate_run_count} 次，"
        f"实际只需保留 {kept_latest_run_count} 次。"
    )

    findings.append(
        f"若执行清理，仅会影响 `screening_decisions` {decisions_to_delete} 行与 "
        f"`screening_picks` {picks_to_delete} 行。"
        + (
            f" 其中 decision 约占全表 {decision_share_pct:.2f}%，pick 约占全表 {pick_share_pct:.2f}%。"
            if decision_share_pct is not None and pick_share_pct is not None
            else ""
        )
    )

    if candidate_groups:
        top_group = candidate_groups[0]
        findings.append(
            f"重复最集中的周期桶是 {top_group.get('label')}，"
            f"候选 run {int(top_group.get('candidate_run_count') or 0)} 次。"
        )
    else:
        findings.append("当前未发现近窗重复成功 run 候选。")

    if zero_detail_candidate_run_count:
        findings.append(
            f"另有 {zero_detail_candidate_run_count} 个重复成功 run 已无待删明细，"
            "本轮自动 cleanup scope 已跳过这些幂等 no-op run。"
        )

    return findings


def _fetch_screening_table_totals(session) -> dict:
    """读取筛选表总量，优先使用 run 级物化统计表。"""
    screening_runs_total = int(session.query(func.count(ScreeningRun.id)).scalar() or 0)
    materialized_decisions, materialized_picks, stats_run_count, stats_available = fetch_materialized_totals(session)
    if stats_available and stats_run_count >= screening_runs_total:
        return {
            "screening_runs_total": screening_runs_total,
            "screening_decisions_total": int(materialized_decisions),
            "screening_picks_total": int(materialized_picks),
            "table_totals_source": "materialized",
            "table_totals_storage_stats_run_count": int(stats_run_count),
        }

    return {
        "screening_runs_total": screening_runs_total,
        "screening_decisions_total": int(session.query(func.count(ScreeningDecision.id)).scalar() or 0),
        "screening_picks_total": int(session.query(func.count(ScreeningPick.id)).scalar() or 0),
        "table_totals_source": "detail_scan",
        "table_totals_storage_stats_run_count": int(stats_run_count),
    }


def build_screening_history_cleanup_preview_payload(
    *,
    db_manager=None,
    db_path: Optional[str] = None,
    recent_days: int = 30,
    fund_types: Optional[Sequence[str]] = None,
    delete_runs: bool = False,
    max_sample_runs: int = 20,
) -> dict:
    """构建筛选历史 cleanup dry-run 预演载荷。"""
    manager = db_manager or get_db_manager(db_path=db_path)
    normalized_fund_types = _normalize_fund_types(fund_types)

    with manager.get_session(task_name="preview_screening_history_cleanup_bundle", readonly=True) as session:
        plan = build_recent_duplicate_success_cleanup_plan(
            session,
            recent_days=max(1, int(recent_days)),
            fund_types=normalized_fund_types or None,
            max_sample_runs=max_sample_runs,
        )
        candidate_run_ids = list(plan.pop("_candidate_run_ids", []) or [])
        run_scope_order = {run_id: index for index, run_id in enumerate(candidate_run_ids)}

        candidate_rows = []
        if candidate_run_ids:
            candidate_scope_rows = session.query(ScreeningRun).filter(
                ScreeningRun.run_id.in_(candidate_run_ids)
            ).all()
            candidate_scope_rows.sort(key=lambda row: run_scope_order.get(row.run_id, 10**9))
            candidate_rows = [_serialize_run(row) for row in candidate_scope_rows]

        table_totals = _fetch_screening_table_totals(session)

    candidate_groups = _build_candidate_groups(candidate_rows)
    resolved_db_path = Path(getattr(manager, "db_path", resolve_db_path(db_path or "database/bond_funds.db")))
    db_file_size_bytes = resolved_db_path.stat().st_size if resolved_db_path.exists() else 0

    decisions_to_delete = int(plan.get("decisions_to_delete") or 0)
    picks_to_delete = int(plan.get("picks_to_delete") or 0)
    summary = {
        "recent_days": max(1, int(recent_days)),
        "fund_types": normalized_fund_types,
        "screening_runs_total": int(table_totals.get("screening_runs_total") or 0),
        "screening_decisions_total": int(table_totals.get("screening_decisions_total") or 0),
        "screening_picks_total": int(table_totals.get("screening_picks_total") or 0),
        "recent_success_run_count": int(plan.get("recent_success_run_count") or 0),
        "kept_latest_run_count": int(plan.get("kept_latest_run_count") or 0),
        "candidate_run_count": int(plan.get("candidate_run_count") or 0),
        "raw_candidate_run_count": int(plan.get("raw_candidate_run_count") or 0),
        "zero_detail_candidate_run_count": int(plan.get("zero_detail_candidate_run_count") or 0),
        "zero_detail_candidate_run_samples": list(plan.get("zero_detail_candidate_run_samples") or []),
        "candidate_period_count": len(candidate_groups),
        "candidate_day_count": len(plan.get("candidate_day_counts") or {}),
        "decisions_to_delete": decisions_to_delete,
        "picks_to_delete": picks_to_delete,
        "runs_to_delete": int(plan.get("runs_to_delete") or 0),
        "estimated_rows_to_delete": int(plan.get("estimated_rows_to_delete") or 0),
        "candidate_decision_share_pct": round((decisions_to_delete / int(table_totals.get("screening_decisions_total") or 0)) * 100, 4)
        if int(table_totals.get("screening_decisions_total") or 0) > 0 else None,
        "candidate_pick_share_pct": round((picks_to_delete / int(table_totals.get("screening_picks_total") or 0)) * 100, 4)
        if int(table_totals.get("screening_picks_total") or 0) > 0 else None,
        "table_totals_source": table_totals.get("table_totals_source") or "unknown",
        "table_totals_storage_stats_run_count": int(table_totals.get("table_totals_storage_stats_run_count") or 0),
        "count_source": plan.get("count_source") or "unknown",
        "storage_stats_covered_run_count": int(plan.get("storage_stats_covered_run_count") or 0),
        "storage_stats_missing_run_count": int(plan.get("storage_stats_missing_run_count") or 0),
        "storage_stats_missing_run_samples": list(plan.get("storage_stats_missing_run_samples") or []),
        "vacuum_recommended": bool(decisions_to_delete or picks_to_delete),
    }

    execution_plan = {
        "dry_run": True,
        "delete_runs": bool(delete_runs),
        "target_tables": [
            "screening_decisions",
            "screening_picks",
            *(["screening_runs"] if delete_runs else []),
        ],
        "scope_run_id_total": len(candidate_run_ids),
        "scope_run_ids": candidate_run_ids,
        "precheck_sql_templates": [
            "SELECT COUNT(*) FROM screening_runs WHERE run_id IN (:candidate_run_ids);",
            "SELECT COUNT(*) FROM screening_decisions WHERE run_id IN (:candidate_run_ids);",
            "SELECT COUNT(*) FROM screening_picks WHERE run_id IN (:candidate_run_ids);",
        ],
        "apply_sql_templates": [
            "DELETE FROM screening_decisions WHERE run_id IN (:candidate_run_ids);",
            "DELETE FROM screening_picks WHERE run_id IN (:candidate_run_ids);",
            *(
                ["DELETE FROM screening_runs WHERE run_id IN (:candidate_run_ids);"]
                if delete_runs else
                []
            ),
        ],
        "postcheck_sql_templates": [
            "SELECT COUNT(*) FROM screening_decisions WHERE run_id IN (:candidate_run_ids);",
            "SELECT COUNT(*) FROM screening_picks WHERE run_id IN (:candidate_run_ids);",
        ],
        "guardrails": [
            "仅纳入最近窗口内的 success run。",
            "每个 `(fund_type, period_years)` 必保留最新一条 success run。",
            "默认只删除 `screening_decisions` 与 `screening_picks`，不删除 `screening_runs` 汇总。",
            "本预演不包含 `VACUUM`，删除后数据库文件不会立刻缩小。",
        ],
        "vacuum_note": (
            "SQLite 删除行后不会自动回收磁盘文件体积；若要真正缩小文件，"
            "需要在维护窗口单独执行 VACUUM。"
        ),
    }

    payload = {
        "generated_at": datetime.now().isoformat(),
        "dry_run": True,
        "db_path": str(resolved_db_path),
        "db_file_size_bytes": db_file_size_bytes,
        "parameters": {
            "recent_days": max(1, int(recent_days)),
            "fund_types": normalized_fund_types,
            "delete_runs": bool(delete_runs),
            "max_sample_runs": max_sample_runs,
        },
        "summary": summary,
        "findings": _build_findings(summary, candidate_groups),
        "candidate_groups": candidate_groups,
        "candidate_run_samples": list(plan.get("candidate_run_samples") or []),
        "kept_run_samples": list(plan.get("kept_run_samples") or []),
        "execution_plan": execution_plan,
    }
    return payload


def render_screening_history_cleanup_preview_markdown(payload: dict) -> str:
    """渲染 Markdown 预演报告。"""
    summary = payload.get("summary") or {}
    parameters = payload.get("parameters") or {}
    candidate_groups = payload.get("candidate_groups") or []
    execution_plan = payload.get("execution_plan") or {}
    findings = payload.get("findings") or []

    lines = [
        "# screening history cleanup preview",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- db_path: {payload.get('db_path')}",
        f"- db_file_size_bytes: {payload.get('db_file_size_bytes', 0)}",
        f"- recent_days: {parameters.get('recent_days')}",
        f"- fund_types: {', '.join(parameters.get('fund_types') or ['all'])}",
        f"- delete_runs: {parameters.get('delete_runs')}",
        "",
        "## summary",
        f"- screening_runs_total: {summary.get('screening_runs_total', 0)}",
        f"- screening_decisions_total: {summary.get('screening_decisions_total', 0)}",
        f"- screening_picks_total: {summary.get('screening_picks_total', 0)}",
        f"- recent_success_run_count: {summary.get('recent_success_run_count', 0)}",
        f"- kept_latest_run_count: {summary.get('kept_latest_run_count', 0)}",
        f"- candidate_run_count: {summary.get('candidate_run_count', 0)}",
        f"- raw_candidate_run_count: {summary.get('raw_candidate_run_count', 0)}",
        f"- zero_detail_candidate_run_count: {summary.get('zero_detail_candidate_run_count', 0)}",
        f"- decisions_to_delete: {summary.get('decisions_to_delete', 0)}",
        f"- picks_to_delete: {summary.get('picks_to_delete', 0)}",
        f"- estimated_rows_to_delete: {summary.get('estimated_rows_to_delete', 0)}",
        f"- candidate_decision_share_pct: {summary.get('candidate_decision_share_pct')}",
        f"- candidate_pick_share_pct: {summary.get('candidate_pick_share_pct')}",
        f"- table_totals_source: {summary.get('table_totals_source')}",
        f"- table_totals_storage_stats_run_count: {summary.get('table_totals_storage_stats_run_count', 0)}",
        f"- count_source: {summary.get('count_source')}",
        f"- storage_stats_covered_run_count: {summary.get('storage_stats_covered_run_count', 0)}",
        f"- storage_stats_missing_run_count: {summary.get('storage_stats_missing_run_count', 0)}",
        f"- vacuum_recommended: {summary.get('vacuum_recommended')}",
        "",
        "## findings",
    ]

    for item in findings:
        lines.append(f"- {item}")

    lines.extend(["", "## candidate_groups"])
    if not candidate_groups:
        lines.append("- 无重复候选")
    else:
        for item in candidate_groups[:20]:
            lines.append(
                "- "
                + json.dumps(
                    {
                        "label": item.get("label"),
                        "candidate_run_count": item.get("candidate_run_count"),
                        "latest_started_at": item.get("latest_started_at"),
                        "oldest_started_at": item.get("oldest_started_at"),
                        "sample_run_ids": item.get("sample_run_ids"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

    lines.extend(["", "## execution_plan"])
    lines.append(f"- target_tables: {json.dumps(execution_plan.get('target_tables', []), ensure_ascii=False)}")
    lines.append(f"- scope_run_id_total: {execution_plan.get('scope_run_id_total', 0)}")
    lines.append(f"- vacuum_note: {execution_plan.get('vacuum_note', '')}")
    lines.append("- guardrails:")
    for item in execution_plan.get("guardrails") or []:
        lines.append(f"  - {item}")

    return "\n".join(lines) + "\n"


def write_screening_history_cleanup_preview_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 JSON + Markdown 产物。"""
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (Path.cwd() / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = artifact_dir / f"screening_history_cleanup_preview_{ts}.json"
    markdown_path = artifact_dir / f"screening_history_cleanup_preview_{ts}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_screening_history_cleanup_preview_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path
