#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选历史近窗重复成功 run 的最终执行手册（只读）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_history_cleanup_preview import (
    load_latest_screening_history_cleanup_preview_payload,
)


SCREENING_HISTORY_CLEANUP_RUNBOOK_JSON_PATTERN = "screening_history_cleanup_runbook_*.json"


def load_latest_screening_history_cleanup_runbook_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选历史 cleanup runbook。"""
    return load_latest_json_artifact(
        SCREENING_HISTORY_CLEANUP_RUNBOOK_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_delete_batch_run_limit(delete_batch_run_limit: Optional[int]) -> int:
    try:
        value = int(delete_batch_run_limit or 100)
    except (TypeError, ValueError):
        value = 100
    return max(1, value)


def _iter_run_id_batches(run_ids: Sequence[str], batch_run_limit: int):
    batch_limit = _normalize_delete_batch_run_limit(batch_run_limit)
    for index in range(0, len(run_ids), batch_limit):
        yield list(run_ids[index:index + batch_limit])


def _quote_sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _build_run_id_in_clause(run_ids: Sequence[str]) -> str:
    quoted = [_quote_sql_literal(item) for item in run_ids if str(item or "").strip()]
    return ", ".join(quoted) if quoted else "''"


def _build_backup_path(db_path: str, generated_at: str) -> str:
    resolved_db_path = Path(str(db_path or "database/bond_funds.db"))
    backup_dir = resolved_db_path.parent / "backups"
    safe_ts = generated_at.replace("-", "").replace(":", "").replace(" ", "_")
    return str((backup_dir / f"bond_funds_pre_screening_cleanup_{safe_ts}.db").as_posix())


def _resolve_artifact_path(path_value: str) -> Path:
    path = Path(str(path_value or "").strip()).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def build_screening_history_cleanup_runbook_batch(
    *,
    batch_index: int,
    total_batches: int,
    batch_run_ids: Sequence[str],
    delete_runs: bool,
) -> dict:
    """为单个 run_id 批次构建执行手册。"""
    run_ids = [str(item).strip() for item in (batch_run_ids or []) if str(item or "").strip()]
    run_id_clause = _build_run_id_in_clause(run_ids)
    batch_id = f"screening-history-batch-{batch_index:03d}"

    precheck_sql = [
        f"SELECT COUNT(*) AS run_count FROM screening_runs WHERE run_id IN ({run_id_clause});",
        f"SELECT COUNT(*) AS decision_count FROM screening_decisions WHERE run_id IN ({run_id_clause});",
        f"SELECT COUNT(*) AS pick_count FROM screening_picks WHERE run_id IN ({run_id_clause});",
    ]
    delete_sql = [
        f"DELETE FROM screening_decisions WHERE run_id IN ({run_id_clause});",
        f"DELETE FROM screening_picks WHERE run_id IN ({run_id_clause});",
    ]
    if delete_runs:
        delete_sql.append(f"DELETE FROM screening_runs WHERE run_id IN ({run_id_clause});")

    postcheck_sql = [
        f"SELECT COUNT(*) AS remaining_decisions FROM screening_decisions WHERE run_id IN ({run_id_clause});",
        f"SELECT COUNT(*) AS remaining_picks FROM screening_picks WHERE run_id IN ({run_id_clause});",
    ]
    if delete_runs:
        postcheck_sql.append(
            f"SELECT COUNT(*) AS remaining_runs FROM screening_runs WHERE run_id IN ({run_id_clause});"
        )

    return {
        "execution_order": batch_index,
        "batch_id": batch_id,
        "batch_run_count": len(run_ids),
        "total_batches": total_batches,
        "run_ids": run_ids,
        "sample_run_ids": run_ids[:10],
        "precheck_sql": precheck_sql,
        "delete_sql": delete_sql,
        "postcheck_sql": postcheck_sql,
        "rollback_note": "若任一 precheck/postcheck 不符合预期，不继续后续批次，改走备份恢复。",
    }


def build_screening_history_cleanup_runbook_payload(
    *,
    preview_payload: Optional[dict] = None,
    source_preview_artifact: Optional[str] = None,
    output_dir: str = DEFAULT_AUDIT_DIR,
    delete_batch_run_limit: int = 100,
) -> dict:
    """基于最新 cleanup preview 产物构建最终执行手册。"""
    if preview_payload is None:
        if source_preview_artifact:
            preview_path = _resolve_artifact_path(source_preview_artifact)
            preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
            source_preview_artifact = str(preview_path)
        else:
            preview_path, preview_payload = load_latest_screening_history_cleanup_preview_payload(output_dir=output_dir)
            source_preview_artifact = str(preview_path)

    payload = dict(preview_payload or {})
    summary = dict(payload.get("summary") or {})
    execution_plan = dict(payload.get("execution_plan") or {})
    parameters = dict(payload.get("parameters") or {})
    findings = list(payload.get("findings") or [])

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch_run_limit = _normalize_delete_batch_run_limit(delete_batch_run_limit)
    candidate_run_ids = [
        str(item).strip()
        for item in (execution_plan.get("scope_run_ids") or [])
        if str(item or "").strip()
    ]
    delete_runs = bool(parameters.get("delete_runs") or execution_plan.get("delete_runs"))
    batches = [
        build_screening_history_cleanup_runbook_batch(
            batch_index=index,
            total_batches=(len(candidate_run_ids) + batch_run_limit - 1) // batch_run_limit if candidate_run_ids else 0,
            batch_run_ids=batch_run_ids,
            delete_runs=delete_runs,
        )
        for index, batch_run_ids in enumerate(
            _iter_run_id_batches(candidate_run_ids, batch_run_limit),
            start=1,
        )
    ]

    db_path = str(payload.get("db_path") or "")
    backup_path = _build_backup_path(db_path, generated_at)
    backup_command = (
        f'sqlite3 "{db_path}" ".backup {backup_path}"'
        if db_path else
        'sqlite3 "database/bond_funds.db" ".backup database/backups/bond_funds_pre_screening_cleanup_<timestamp>.db"'
    )

    return {
        "generated_at": generated_at,
        "source_screening_history_cleanup_preview_artifact": source_preview_artifact or "",
        "source_screening_history_cleanup_preview_generated_at": payload.get("generated_at") or "",
        "source_screening_history_cleanup_preview_summary": summary,
        "db_path": db_path,
        "db_file_size_bytes": int(payload.get("db_file_size_bytes") or 0),
        "backup_path": backup_path,
        "backup_command": backup_command,
        "summary": {
            "fund_types": list(summary.get("fund_types") or []),
            "candidate_run_count": int(summary.get("candidate_run_count") or 0),
            "candidate_period_count": int(summary.get("candidate_period_count") or 0),
            "decisions_to_delete": int(summary.get("decisions_to_delete") or 0),
            "picks_to_delete": int(summary.get("picks_to_delete") or 0),
            "estimated_rows_to_delete": int(summary.get("estimated_rows_to_delete") or 0),
            "delete_runs": delete_runs,
            "delete_batch_run_limit": batch_run_limit,
            "delete_batch_count": len(batches),
            "vacuum_recommended": bool(summary.get("vacuum_recommended")),
        },
        "findings": findings,
        "preflight_checklist": [
            "确认当前产物仍对应最新数据库状态；若运行期间新增了筛选任务，先重新生成 preview。",
            "进入维护窗口，暂停新的筛选写入，避免 precheck 与实际删除范围漂移。",
            "先执行备份命令，拿到可恢复的单文件快照后再进入 delete_sql。",
            "先跑全局 precheck，再按批次执行 precheck_sql / delete_sql / postcheck_sql。",
            "本轮默认只删除 screening_decisions 与 screening_picks；除非已单独签字，不删除 screening_runs。",
        ],
        "execution_sequence": [
            "1. 执行 backup_command 生成单文件备份。",
            "2. 逐批执行 precheck_sql，确认当前 run_count / decision_count / pick_count 与预期一致。",
            "3. 在同一维护窗口内执行 delete_sql。",
            "4. 每批立即执行 postcheck_sql，确认 remaining_* = 0。",
            "5. 所有批次完成后，再单独评估是否需要另开窗口执行 VACUUM。",
        ],
        "rollback_strategy": [
            "任一批次 delete_sql 报错或中断，立即停止后续批次。",
            "任一 postcheck_sql 返回 remaining_* 非 0，立即停止后续批次。",
            "若抽样复核发现 run_id 范围与 preview 不一致，放弃继续执行，重新生成 preview + runbook。",
            f"如需恢复，停止写入后，用备份文件 {backup_path} 覆盖主库，再重新启动服务。",
        ],
        "batches": batches,
    }


def render_screening_history_cleanup_runbook_markdown(payload: dict) -> str:
    """渲染 Markdown 手册。"""
    summary = payload.get("summary") or {}
    lines = [
        "# screening history cleanup runbook",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- source_preview_artifact: {payload.get('source_screening_history_cleanup_preview_artifact', '')}",
        f"- source_preview_generated_at: {payload.get('source_screening_history_cleanup_preview_generated_at', '')}",
        f"- db_path: {payload.get('db_path', '')}",
        f"- backup_path: {payload.get('backup_path', '')}",
        f"- candidate_run_count: {summary.get('candidate_run_count', 0)}",
        f"- decisions_to_delete: {summary.get('decisions_to_delete', 0)}",
        f"- picks_to_delete: {summary.get('picks_to_delete', 0)}",
        f"- delete_batch_run_limit: {summary.get('delete_batch_run_limit', 0)}",
        f"- delete_batch_count: {summary.get('delete_batch_count', 0)}",
        "",
        "## preflight_checklist",
    ]
    for item in payload.get("preflight_checklist") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## execution_sequence"])
    for item in payload.get("execution_sequence") or []:
        lines.append(f"- {item}")

    lines.extend(["", "## batches"])
    for batch in payload.get("batches") or []:
        lines.extend([
            f"- {json.dumps({'batch_id': batch.get('batch_id'), 'batch_run_count': batch.get('batch_run_count'), 'sample_run_ids': batch.get('sample_run_ids')}, ensure_ascii=False, sort_keys=True)}",
        ])

    lines.extend(["", "## rollback_strategy"])
    for item in payload.get("rollback_strategy") or []:
        lines.append(f"- {item}")

    return "\n".join(lines).strip() + "\n"


def write_screening_history_cleanup_runbook_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出筛选历史 cleanup runbook JSON/Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"screening_history_cleanup_runbook_{ts}.json"
    md_path = artifact_dir / f"screening_history_cleanup_runbook_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_history_cleanup_runbook_markdown(payload), encoding="utf-8")
    return json_path, md_path
