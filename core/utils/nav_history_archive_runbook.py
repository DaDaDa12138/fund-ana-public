#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档执行手册（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.nav_history_archive_maintenance_defaults import (
    build_nav_history_archive_target_path,
    load_nav_history_archive_maintenance_defaults,
    normalize_nav_history_archive_maintenance_defaults,
)


NAV_HISTORY_ARCHIVE_RUNBOOK_JSON_PATTERN = "nav_history_archive_runbook_*.json"
DEFAULT_ARCHIVE_TABLE_NAME = "fund_nav_history_archive"
DEFAULT_ARCHIVE_DATABASE_ALIAS = "archive_db"


def load_latest_nav_history_archive_runbook_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 NAV 历史归档 runbook。"""
    return load_latest_json_artifact(
        NAV_HISTORY_ARCHIVE_RUNBOOK_JSON_PATTERN,
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


def _quote_sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _build_archive_attach_sql(archive_target_path: str, archive_db_alias: str = DEFAULT_ARCHIVE_DATABASE_ALIAS) -> str:
    archive_literal = _quote_sql_literal(archive_target_path)
    return f"ATTACH DATABASE {archive_literal} AS {archive_db_alias};"


def _build_archive_detach_sql(archive_db_alias: str = DEFAULT_ARCHIVE_DATABASE_ALIAS) -> str:
    return f"DETACH DATABASE {archive_db_alias};"


def _build_archive_create_sql(
    *,
    archive_table_name: str,
    archive_db_alias: str = DEFAULT_ARCHIVE_DATABASE_ALIAS,
) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {archive_db_alias}.{archive_table_name} ("
        "fcode TEXT NOT NULL, nav_date TEXT NOT NULL, unit_nav REAL NOT NULL, accum_nav REAL, daily_growth REAL, "
        "PRIMARY KEY (fcode, nav_date)"
        ");"
    )


def _build_archive_insert_sql(
    *,
    archive_table_name: str,
    archive_cutoff_date: str,
    archive_db_alias: str = DEFAULT_ARCHIVE_DATABASE_ALIAS,
) -> str:
    cutoff_literal = _quote_sql_literal(archive_cutoff_date)
    return "\n".join([
        f"INSERT INTO {archive_db_alias}.{archive_table_name} (fcode, nav_date, unit_nav, accum_nav, daily_growth)",
        "SELECT fcode, nav_date, unit_nav, accum_nav, daily_growth",
        "FROM main.fund_nav_history",
        f"WHERE nav_date < {cutoff_literal};",
    ])


def _build_archive_delete_sql(archive_cutoff_date: str) -> str:
    cutoff_literal = _quote_sql_literal(archive_cutoff_date)
    return "\n".join([
        "DELETE FROM fund_nav_history",
        f"WHERE nav_date < {cutoff_literal};",
    ])


def build_nav_history_archive_runbook_payload(
    *,
    nav_history_storage_profile: Optional[dict] = None,
    nav_history_retention_preview: Optional[dict] = None,
    nav_history_archive_strategy_preview: Optional[dict] = None,
    maintenance_defaults: Optional[dict] = None,
) -> dict:
    """基于 NAV 历史只读预演结果，收敛为最终归档 runbook。"""
    profile = _normalize_dict(nav_history_storage_profile)
    retention = _normalize_dict(nav_history_retention_preview)
    strategy = _normalize_dict(nav_history_archive_strategy_preview)
    maintenance_defaults = normalize_nav_history_archive_maintenance_defaults(
        maintenance_defaults or load_nav_history_archive_maintenance_defaults()
    )

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    recommended_retention_years = _normalize_int(strategy.get("recommended_retention_years"))
    archive_cutoff_date = str(strategy.get("archive_cutoff_date") or "").strip()
    estimated_archive_row_count = _normalize_int(strategy.get("estimated_archive_row_count"))
    estimated_archive_size_mb = _normalize_float(strategy.get("estimated_archive_size_mb"))
    archive_table_name = str(
        strategy.get("archive_table_name")
        or maintenance_defaults.get("archive_table_name")
        or DEFAULT_ARCHIVE_TABLE_NAME
    ).strip() or DEFAULT_ARCHIVE_TABLE_NAME
    archive_target_path = str(strategy.get("archive_target_path") or "").strip() or (
        build_nav_history_archive_target_path(
            archive_cutoff_date,
            maintenance_defaults=maintenance_defaults,
        )
    )
    estimated_archive_fund_count = _normalize_int(strategy.get("estimated_archive_fund_count"))
    estimated_archive_share_pct = _normalize_float(strategy.get("estimated_archive_share_pct"))
    latest_nav_date = str(strategy.get("latest_nav_date") or retention.get("latest_nav_date") or "").strip()
    earliest_nav_date = str(retention.get("earliest_nav_date") or "").strip()

    summary = {
        "recommended_retention_years": recommended_retention_years if recommended_retention_years > 0 else None,
        "archive_cutoff_date": archive_cutoff_date,
        "estimated_archive_row_count": estimated_archive_row_count,
        "estimated_archive_size_mb": estimated_archive_size_mb,
        "estimated_archive_fund_count": estimated_archive_fund_count,
        "estimated_archive_share_pct": estimated_archive_share_pct,
        "archive_table_name": archive_table_name,
        "archive_target_path": archive_target_path,
        "latest_nav_date": latest_nav_date,
        "earliest_nav_date": earliest_nav_date,
    }

    if strategy.get("available") is False:
        return {
            "generated_at": generated_at,
            "source_nav_history_storage_profile_summary": profile,
            "source_nav_history_retention_preview_summary": retention,
            "source_nav_history_archive_strategy_preview_summary": strategy,
            "summary": summary,
            "available": False,
            "status": "unavailable",
            "message": str(strategy.get("message") or "当前缺少足够的 NAV 历史只读预演结论，暂无法生成归档手册。"),
            "preflight_checklist": [],
            "execution_sequence": [],
            "rollback_strategy": [],
            "sample_validation_queries": [],
            "archive_sql_preview": [],
            "delete_sql_preview": [],
            "postcheck_sql_preview": [],
        }

    sample_validation_queries = [
        (
            "SELECT COUNT(*) AS cold_row_count FROM fund_nav_history "
            f"WHERE nav_date < {_quote_sql_literal(archive_cutoff_date)};"
        ),
        (
            "SELECT COUNT(DISTINCT fcode) AS cold_fund_count FROM fund_nav_history "
            f"WHERE nav_date < {_quote_sql_literal(archive_cutoff_date)};"
        ),
        (
            "SELECT fcode, MIN(nav_date) AS first_nav_date, MAX(nav_date) AS last_nav_date, COUNT(*) AS row_count "
            "FROM fund_nav_history "
            f"WHERE nav_date < {_quote_sql_literal(archive_cutoff_date)} "
            "GROUP BY fcode ORDER BY row_count DESC LIMIT 20;"
        ),
    ]

    archive_sql_preview = [
        _build_archive_attach_sql(archive_target_path),
        _build_archive_create_sql(
            archive_table_name=archive_table_name,
        ),
        _build_archive_insert_sql(
            archive_table_name=archive_table_name,
            archive_cutoff_date=archive_cutoff_date,
        ),
        _build_archive_detach_sql(),
    ]
    delete_sql_preview = [_build_archive_delete_sql(archive_cutoff_date)]
    postcheck_sql_preview = [
        _build_archive_attach_sql(archive_target_path),
        (
            f"SELECT COUNT(*) AS archive_row_count FROM {DEFAULT_ARCHIVE_DATABASE_ALIAS}.{archive_table_name} "
            f"WHERE nav_date < {_quote_sql_literal(archive_cutoff_date)};"
        ),
        _build_archive_detach_sql(),
        (
            "SELECT COUNT(*) AS remaining_cold_rows FROM fund_nav_history "
            f"WHERE nav_date < {_quote_sql_literal(archive_cutoff_date)};"
        ),
        "PRAGMA freelist_count;",
    ]

    status = str(strategy.get("status") or "archive_candidate").strip() or "archive_candidate"
    message = str(strategy.get("message") or "").strip()
    if not message:
        message = "已根据 NAV 历史剖面与保留预演生成只读归档执行手册。"

    preflight_checklist = [
        "确认当前 runbook 基于最新主库状态；若 NAV 历史在此期间继续写入，先重新生成剖面、保留预演与归档方案预演。",
        f"先在维护窗口抽样核验 {earliest_nav_date or '最早日期'} 至 {archive_cutoff_date or 'cutoff'} 的冷数据仍可离线保存，不再要求常驻主库。",
        f"确认归档目标文件 {archive_target_path or 'backend/data/archives/...'} 与归档表 {archive_table_name} 已预留并可读写。",
        "真实执行前必须先完成备份、抽样核对、回滚路径演练与批次删除策略审批。",
        "本手册只提供 SQL 预览与核对清单，不直接执行导出、删除或 VACUUM。",
    ]

    execution_sequence = [
        f"1. 先执行 sample_validation_queries，确认 {archive_cutoff_date or 'cutoff'} 之前的冷数据行数、基金数与抽样范围符合预演结论。",
        f"2. 在归档库中创建 {archive_table_name}，再执行 archive_sql_preview 导出冷数据。",
        f"3. 对比归档表与主库冷数据计数一致后，另开维护窗口执行 delete_sql_preview。",
        "4. 删除后立即执行 postcheck_sql_preview，确认 remaining_cold_rows = 0，再评估是否需要 VACUUM。",
        "5. 最终记录归档文件路径、归档批次、抽样证据和回滚点，供后续复核与审批留档。",
    ]

    rollback_strategy = [
        "若抽样核验发现冷数据仍被主流程依赖，停止执行并重新评估保留窗口。",
        "若归档计数与主库计数不一致，不进入删除阶段。",
        "若删除中断或 postcheck 返回 remaining_cold_rows 非 0，立即停止后续批次并回到备份恢复方案。",
        "真实恢复动作应以主库备份和归档文件双重校验为前提；本 runbook 不直接执行恢复。",
    ]

    return {
        "generated_at": generated_at,
        "available": True,
        "status": status,
        "message": message,
        "source_nav_history_storage_profile_summary": profile,
        "source_nav_history_retention_preview_summary": retention,
        "source_nav_history_archive_strategy_preview_summary": strategy,
        "summary": summary,
        "preflight_checklist": preflight_checklist,
        "execution_sequence": execution_sequence,
        "rollback_strategy": rollback_strategy,
        "sample_validation_queries": sample_validation_queries,
        "archive_sql_preview": archive_sql_preview,
        "delete_sql_preview": delete_sql_preview,
        "postcheck_sql_preview": postcheck_sql_preview,
    }


def render_nav_history_archive_runbook_markdown(payload: dict) -> str:
    """渲染 NAV 历史归档 Markdown 手册。"""
    summary = _normalize_dict(payload.get("summary"))
    lines = [
        "# nav history archive runbook",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- available: {payload.get('available', False)}",
        f"- status: {payload.get('status', '')}",
        f"- message: {payload.get('message', '')}",
        f"- recommended_retention_years: {summary.get('recommended_retention_years', '')}",
        f"- archive_cutoff_date: {summary.get('archive_cutoff_date', '')}",
        f"- estimated_archive_row_count: {summary.get('estimated_archive_row_count', 0)}",
        f"- estimated_archive_size_mb: {summary.get('estimated_archive_size_mb', 0.0)}",
        f"- archive_table_name: {summary.get('archive_table_name', '')}",
        f"- archive_target_path: {summary.get('archive_target_path', '')}",
        "",
        "## preflight_checklist",
    ]
    for item in payload.get("preflight_checklist") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_sequence"])
    for item in payload.get("execution_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## sample_validation_queries"])
    for item in payload.get("sample_validation_queries") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## archive_sql_preview"])
    for item in payload.get("archive_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## delete_sql_preview"])
    for item in payload.get("delete_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## postcheck_sql_preview"])
    for item in payload.get("postcheck_sql_preview") or []:
        lines.append(f"- {json.dumps(item, ensure_ascii=False)}")

    lines.extend(["", "## rollback_strategy"])
    for item in payload.get("rollback_strategy") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_nav_history_archive_runbook_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出 NAV 历史归档 runbook JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"nav_history_archive_runbook_{ts}.json"
    md_path = artifact_dir / f"nav_history_archive_runbook_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_nav_history_archive_runbook_markdown(payload), encoding="utf-8")
    return json_path, md_path
