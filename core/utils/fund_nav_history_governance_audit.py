#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fund_nav_history 只读治理审计。

目标：
1. 解释主库体积为什么主要由 NAV 历史驱动；
2. 收敛归档收益、索引风险与下一步优化方向；
3. 输出可复用的 JSON / Markdown 审计产物，不执行任何真实清理。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from database.connection import get_db_manager, resolve_db_path
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_analysis import (
    _get_sqlite_storage_metrics,
    build_nav_history_archive_strategy_preview,
    build_nav_history_index_audit,
    build_nav_history_retention_preview,
    build_nav_history_storage_profile,
    build_primary_database_hotspots,
)


FUND_NAV_HISTORY_GOVERNANCE_AUDIT_JSON_PATTERN = "fund_nav_history_governance_audit_*.json"


def load_latest_fund_nav_history_governance_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 fund_nav_history 治理审计产物。"""
    return load_latest_json_artifact(
        FUND_NAV_HISTORY_GOVERNANCE_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _find_nav_history_table(database_hotspots: dict) -> dict:
    hotspots = _normalize_dict(database_hotspots)
    largest_table = _normalize_dict(hotspots.get("largest_table"))
    if str(largest_table.get("name") or "").strip() == "fund_nav_history":
        return largest_table
    for item in _normalize_list(hotspots.get("top_tables")):
        row = _normalize_dict(item)
        if str(row.get("name") or "").strip() == "fund_nav_history":
            return row
    return {}


def _collect_nav_history_indexes(database_hotspots: dict) -> list[dict]:
    rows = []
    for item in _normalize_list(_normalize_dict(database_hotspots).get("top_indexes")):
        row = _normalize_dict(item)
        if str(row.get("table_name") or "").strip() != "fund_nav_history":
            continue
        rows.append(row)
    return rows


def _pick_retention_option(retention_preview: dict, retention_years: int) -> dict:
    for item in _normalize_list(_normalize_dict(retention_preview).get("retention_options")):
        row = _normalize_dict(item)
        if _normalize_int(row.get("retention_years")) == int(retention_years):
            return row
    return {}


def _build_summary(
    *,
    storage_metrics: dict,
    database_hotspots: dict,
    nav_history_storage_profile: dict,
    nav_history_retention_preview: dict,
    nav_history_archive_strategy_preview: dict,
    nav_history_index_audit: dict,
) -> dict:
    metrics = _normalize_dict(storage_metrics)
    hotspots = _normalize_dict(database_hotspots)
    profile = _normalize_dict(nav_history_storage_profile)
    retention = _normalize_dict(nav_history_retention_preview)
    strategy = _normalize_dict(nav_history_archive_strategy_preview)
    index_audit = _normalize_dict(nav_history_index_audit)

    database_size_mb = _normalize_float(
        metrics.get("database_live_size_mb") or metrics.get("database_size_mb")
    )
    nav_table = _find_nav_history_table(hotspots)
    nav_table_size_mb = _normalize_float(nav_table.get("size_mb"))
    nav_indexes = _collect_nav_history_indexes(hotspots)
    nav_index_total_size_mb = round(
        sum(_normalize_float(item.get("size_mb")) for item in nav_indexes),
        2,
    )
    nav_hot_zone_size_mb = round(nav_table_size_mb + nav_index_total_size_mb, 2)
    nav_hot_zone_share_of_db_pct = round(
        (nav_hot_zone_size_mb / database_size_mb) * 100,
        1,
    ) if database_size_mb > 0 and nav_hot_zone_size_mb > 0 else 0.0

    recommended_retention_years = _normalize_int(
        strategy.get("recommended_retention_years") or retention.get("active_recommended_years")
    )
    strategy_option = _pick_retention_option(retention, recommended_retention_years)
    estimated_archive_row_count = _normalize_int(strategy.get("estimated_archive_row_count"))
    estimated_archive_fund_count = _normalize_int(strategy.get("estimated_archive_fund_count"))
    estimated_archive_size_mb = _normalize_float(strategy.get("estimated_archive_size_mb"))
    estimated_archive_share_pct = _normalize_float(strategy.get("estimated_archive_share_pct"))
    estimated_archive_share_of_db_pct = round(
        (estimated_archive_size_mb / database_size_mb) * 100,
        1,
    ) if database_size_mb > 0 and estimated_archive_size_mb > 0 else 0.0
    estimated_archive_share_of_nav_hot_zone_pct = round(
        (estimated_archive_size_mb / nav_hot_zone_size_mb) * 100,
        1,
    ) if nav_hot_zone_size_mb > 0 and estimated_archive_size_mb > 0 else 0.0

    profile_status = str(profile.get("status") or "").strip()
    strategy_status = str(strategy.get("status") or "").strip()
    index_status = str(index_audit.get("status") or "").strip()
    runtime_drop_candidate = bool(index_audit.get("runtime_drop_candidate"))
    legacy_index_hits = _normalize_int(index_audit.get("legacy_used_by_query_count"))

    status = "observe"
    recommended_direction_key = "observe"
    recommended_direction_label = "继续观察"
    headline = "fund_nav_history 当前进入观察窗口"
    message = "当前尚未识别出明确优先级最高的 fund_nav_history 专项治理动作。"
    action_hint = "继续观察主库热点、年度增量和查询链路；只有证据充分时再进入专项维护准备。"

    if strategy_status == "archive_candidate" and estimated_archive_size_mb > 0:
        status = "archive_preparation_priority"
        recommended_direction_key = "nav_history_archive_preparation"
        recommended_direction_label = "NAV 冷数据归档准备"
        headline = "下一优化方向应优先转向 NAV 冷数据归档准备"
        message = (
            f"当前主库约 {database_size_mb} MB，其中 fund_nav_history 表及其主要索引约 {nav_hot_zone_size_mb} MB "
            f"（{nav_hot_zone_share_of_db_pct}%）；若只保留近 {recommended_retention_years} 年，"
            f"理论上可移出约 {estimated_archive_row_count} 行、约 {estimated_archive_size_mb} MB 冷数据。"
        )
        action_hint = (
            "优先补齐 runbook、preflight、control pack 和 readiness audit，"
            "先把 NAV 归档维护链路收口到可签字状态，再决定是否进入真实维护窗口。"
        )
    elif profile_status in {"long_history_accumulation", "dense_fund_outliers"}:
        status = "nav_history_profile_priority"
        recommended_direction_key = "nav_history_profile_followup"
        recommended_direction_label = "NAV 历史剖面复核"
        headline = "下一优化方向应继续深挖 fund_nav_history 剖面"
        message = (
            f"当前 fund_nav_history 已解释主库主要体积来源，但仍需围绕高密度基金、年度累积与保留窗口继续复核。"
        )
        action_hint = "先补齐年度分布与高密度基金抽样证据，再决定归档窗口或索引策略。"
    elif index_status in {"keep", "review"} and (legacy_index_hits > 0 or not runtime_drop_candidate):
        status = "index_audit_followup"
        recommended_direction_key = "nav_index_query_audit"
        recommended_direction_label = "NAV 查询链路索引审计"
        headline = "下一优化方向应继续做 NAV 查询链路索引审计"
        message = "当前仍有 Explain 证据表明部分 NAV 查询或遗留路径依赖旧索引，不宜贸然动索引。"
        action_hint = "继续清点 DISTINCT / COUNT / 分页总数路径，确认完全脱离旧索引后再评估索引窗口。"

    return {
        "status": status,
        "recommended_direction_key": recommended_direction_key,
        "recommended_direction_label": recommended_direction_label,
        "headline": headline,
        "message": message,
        "action_hint": action_hint,
        "database_size_mb": database_size_mb,
        "database_path": str(metrics.get("database_path") or "").strip(),
        "nav_history_table_size_mb": nav_table_size_mb,
        "nav_history_index_total_size_mb": nav_index_total_size_mb,
        "nav_history_hot_zone_size_mb": nav_hot_zone_size_mb,
        "nav_history_hot_zone_share_of_db_pct": nav_hot_zone_share_of_db_pct,
        "recommended_retention_years": recommended_retention_years,
        "archive_cutoff_date": str(strategy.get("archive_cutoff_date") or "").strip(),
        "estimated_archive_row_count": estimated_archive_row_count,
        "estimated_archive_fund_count": estimated_archive_fund_count,
        "estimated_archive_size_mb": estimated_archive_size_mb,
        "estimated_archive_share_pct": estimated_archive_share_pct,
        "estimated_archive_share_of_db_pct": estimated_archive_share_of_db_pct,
        "estimated_archive_share_of_nav_hot_zone_pct": estimated_archive_share_of_nav_hot_zone_pct,
        "retention_option": strategy_option,
        "profile_status": profile_status,
        "strategy_status": strategy_status,
        "index_status": index_status,
        "index_runtime_drop_candidate": runtime_drop_candidate,
        "index_legacy_hit_count": legacy_index_hits,
    }


def _build_findings(
    *,
    summary: dict,
    nav_history_storage_profile: dict,
    nav_history_archive_strategy_preview: dict,
    nav_history_index_audit: dict,
    nav_indexes: list[dict],
) -> list[str]:
    profile = _normalize_dict(nav_history_storage_profile)
    strategy = _normalize_dict(nav_history_archive_strategy_preview)
    index_audit = _normalize_dict(nav_history_index_audit)
    findings = []

    database_size_mb = _normalize_float(summary.get("database_size_mb"))
    nav_hot_zone_size_mb = _normalize_float(summary.get("nav_history_hot_zone_size_mb"))
    nav_hot_zone_share_of_db_pct = _normalize_float(summary.get("nav_history_hot_zone_share_of_db_pct"))
    if nav_hot_zone_size_mb > 0:
        findings.append(
            f"当前主库约 {database_size_mb} MB，其中 fund_nav_history 表和主要索引约 {nav_hot_zone_size_mb} MB，"
            f"占主库约 {nav_hot_zone_share_of_db_pct}%。"
        )

    row_count = _normalize_int(profile.get("nav_history_row_count"))
    fund_count = _normalize_int(profile.get("nav_history_fund_count"))
    earliest_nav_date = str(profile.get("earliest_nav_date") or "").strip()
    latest_nav_date = str(profile.get("latest_nav_date") or "").strip()
    avg_rows_per_fund = _normalize_float(profile.get("avg_rows_per_fund"))
    if row_count > 0 and fund_count > 0:
        findings.append(
            f"当前 NAV 历史约 {row_count} 行，覆盖 {fund_count} 只基金，日期跨度 {earliest_nav_date or '—'} ~ {latest_nav_date or '—'}，"
            f"均值每只基金 {avg_rows_per_fund} 行。"
        )

    estimated_archive_size_mb = _normalize_float(summary.get("estimated_archive_size_mb"))
    if estimated_archive_size_mb > 0:
        findings.append(
            f"按近 {summary.get('recommended_retention_years') or '—'} 年保留窗口测算，"
            f"理论可移出约 {summary.get('estimated_archive_row_count') or 0} 行、"
            f"{summary.get('estimated_archive_fund_count') or 0} 只基金、"
            f"约 {estimated_archive_size_mb} MB 冷数据，约占主库 {summary.get('estimated_archive_share_of_db_pct') or 0}% 。"
        )

    if nav_indexes:
        index_brief = " / ".join(
            f"{str(item.get('name') or '').strip()} {item.get('size_mb')} MB"
            for item in nav_indexes[:3]
        )
        findings.append(f"当前 fund_nav_history 相关索引热点：{index_brief}。")

    index_name = str(index_audit.get("index_name") or "idx_nav_fcode").strip()
    if str(index_audit.get("status") or "").strip() == "missing":
        findings.append(f"{index_name} 当前已不存在，说明 NAV 索引治理重点已不在该旧索引本身。")
    elif bool(index_audit.get("runtime_drop_candidate")):
        findings.append(
            f"{index_name} 已脱离当前运行链路，但在真实删索引前仍应继续清点遗留查询形态。"
        )
    elif _normalize_int(index_audit.get("used_by_query_count")) > 0:
        findings.append(
            f"{index_name} 仍被当前运行路径命中 {_normalize_int(index_audit.get('used_by_query_count'))} 类查询，当前不宜继续推进删索引窗口。"
        )

    strategy_message = str(strategy.get("message") or "").strip()
    if strategy_message:
        findings.append(strategy_message)

    return findings


def _build_recommendations(summary: dict) -> list[str]:
    if str(summary.get("recommended_direction_key") or "").strip() == "nav_history_archive_preparation":
        return [
            "先刷新 NAV 归档 runbook / preflight，确认 cutoff、冷数据行数和归档目标路径仍与 live 主库一致。",
            "在不做真实删除的前提下，继续补齐 control pack、maintenance window pack、execution checklist 和 readiness audit。",
            "索引层当前只做只读 Explain 审计，不直接碰 fund_nav_history 的唯一索引和日期索引。",
        ]
    if str(summary.get("recommended_direction_key") or "").strip() == "nav_index_query_audit":
        return [
            "优先清点 DISTINCT / COUNT / 分页总数等 NAV 查询路径，确认是否仍依赖旧索引。",
            "补充 Explain 样本和运行链路证据，再决定是否进入删索引复核窗口。",
            "在索引证据不足前，不把删索引当成主库优化主方向。",
        ]
    return [
        "继续观察 fund_nav_history 的年度分布、高密度基金样本和主库热点变化。",
        "若后续 archive_candidate 结论稳定，再切换到 NAV 冷数据归档准备方向。",
    ]


def build_fund_nav_history_governance_audit_payload(
    *,
    db_manager=None,
    db_path: Optional[str] = None,
    storage_metrics: Optional[dict] = None,
    database_hotspots: Optional[dict] = None,
    nav_history_storage_profile: Optional[dict] = None,
    nav_history_retention_preview: Optional[dict] = None,
    nav_history_archive_strategy_preview: Optional[dict] = None,
    nav_history_index_audit: Optional[dict] = None,
) -> dict:
    """构建 fund_nav_history 治理审计结果。"""
    manager = db_manager or get_db_manager(db_path=db_path)
    resolved_db_path = str(
        Path(getattr(manager, "db_path", resolve_db_path(db_path or "database/bond_funds.db"))).resolve()
    )

    metrics = _normalize_dict(storage_metrics) or _get_sqlite_storage_metrics(manager)
    hotspots = _normalize_dict(database_hotspots) or build_primary_database_hotspots(manager)
    profile = _normalize_dict(nav_history_storage_profile) or build_nav_history_storage_profile(manager)
    retention = _normalize_dict(nav_history_retention_preview) or build_nav_history_retention_preview(
        manager,
        database_hotspots=hotspots,
    )
    strategy = _normalize_dict(nav_history_archive_strategy_preview) or build_nav_history_archive_strategy_preview(
        profile,
        retention,
    )
    index_audit = _normalize_dict(nav_history_index_audit) or build_nav_history_index_audit(manager)

    nav_indexes = _collect_nav_history_indexes(hotspots)
    summary = _build_summary(
        storage_metrics=metrics,
        database_hotspots=hotspots,
        nav_history_storage_profile=profile,
        nav_history_retention_preview=retention,
        nav_history_archive_strategy_preview=strategy,
        nav_history_index_audit=index_audit,
    )
    findings = _build_findings(
        summary=summary,
        nav_history_storage_profile=profile,
        nav_history_archive_strategy_preview=strategy,
        nav_history_index_audit=index_audit,
        nav_indexes=nav_indexes,
    )
    recommendations = _build_recommendations(summary)

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database_path": resolved_db_path,
        "summary": summary,
        "findings": findings,
        "recommendations": recommendations,
        "storage_metrics": metrics,
        "database_hotspots": hotspots,
        "nav_history_storage_profile": profile,
        "nav_history_retention_preview": retention,
        "nav_history_archive_strategy_preview": strategy,
        "nav_history_index_audit": index_audit,
    }


def render_fund_nav_history_governance_audit_markdown(payload: dict) -> str:
    """渲染 fund_nav_history 治理审计 Markdown。"""
    summary = _normalize_dict(payload.get("summary"))
    hotspots = _normalize_dict(payload.get("database_hotspots"))
    lines = [
        "# fund_nav_history governance audit",
        "",
        f"- 生成时间: {payload.get('generated_at', '')}",
        f"- 主库路径: {payload.get('database_path', '')}",
        f"- 状态: {summary.get('status', '')}",
        f"- 推荐方向: {summary.get('recommended_direction_label', '')}",
        f"- headline: {summary.get('headline', '')}",
        "",
        "## 核心结论",
        "",
        f"- {summary.get('message', '')}",
        f"- {summary.get('action_hint', '')}",
        "",
        "## Findings",
        "",
    ]
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")

    lines.extend([
        "",
        "## Next Steps",
        "",
    ])
    for item in payload.get("recommendations") or []:
        lines.append(f"- {item}")

    nav_table = _find_nav_history_table(hotspots)
    nav_indexes = _collect_nav_history_indexes(hotspots)
    if nav_table:
        lines.extend([
            "",
            "## 热点证据",
            "",
            f"- fund_nav_history 表体积: {nav_table.get('size_mb', 0)} MB",
            f"- 主库热点占比: {summary.get('nav_history_hot_zone_share_of_db_pct', 0)}%",
        ])
        for item in nav_indexes[:5]:
            lines.append(
                f"- NAV 相关索引: {item.get('name', '')} {item.get('size_mb', 0)} MB"
            )

    return "\n".join(lines).strip() + "\n"


def write_fund_nav_history_governance_audit_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 fund_nav_history 治理审计 JSON / Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"fund_nav_history_governance_audit_{ts}.json"
    md_path = artifact_dir / f"fund_nav_history_governance_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_fund_nav_history_governance_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    payload = build_fund_nav_history_governance_audit_payload()
    json_path, md_path = write_fund_nav_history_governance_audit_artifacts(payload)
    print(json.dumps({
        "json_path": str(json_path),
        "md_path": str(md_path),
        "summary": payload.get("summary"),
    }, ensure_ascii=False, indent=2))
