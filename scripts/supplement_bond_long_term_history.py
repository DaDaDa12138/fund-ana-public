#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

CORE_ROOT = PROJECT_ROOT / "core"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.long_term_history_maintenance import (  # noqa: E402
    _hydrate_failure_backoff_policy,
    _load_current_published_formal_codes,
    _load_maintenance_state,
    _prioritize_current_published_and_governance_candidates,
    _read_bond_universe,
    _reconcile_stale_refresh_progress,
    _resolve_empty_payload_retry_days,
    _resolve_empty_payload_upstream_limited_failures,
    _resolve_stale_refresh_retry_days,
    _resolve_stale_refresh_upstream_limited_failures,
    _run_stage_concurrent,
    _save_maintenance_state,
    _update_failure_backoff_after_run,
    _reset_collector_api_source_evidence,
    _update_collector_api_source_evidence_context,
    build_long_term_history_api_source_evidence,
)
from api.client import APIClient  # type: ignore # noqa: E402
from collectors.annual_return_collector import AnnualReturnCollector  # type: ignore # noqa: E402
from collectors.rank_collector import RankCollector  # type: ignore # noqa: E402
from database.connection import get_db_manager  # type: ignore # noqa: E402
from utils.project_runtime_config import load_runtime_config  # type: ignore # noqa: E402


def _distinct_history_counts(database_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(database_path))
    try:
        annual_count = int(conn.execute("SELECT COUNT(DISTINCT fcode) FROM fund_annual_returns").fetchone()[0] or 0)
        rank_count = int(conn.execute("SELECT COUNT(DISTINCT fcode) FROM fund_rank_history").fetchone()[0] or 0)
    finally:
        conn.close()
    return {
        "annual_history_fund_count": annual_count,
        "rank_history_fund_count": rank_count,
    }


def _normalize_code(value: str) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _load_codes_from_file(path: Path) -> list[str]:
    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        return []
    if raw_text.startswith("["):
        payload = json.loads(raw_text)
        return [_normalize_code(item) for item in payload if _normalize_code(item)]
    return [_normalize_code(line) for line in raw_text.splitlines() if _normalize_code(line)]


def _count_failed_items_by_error(failed_items: list[dict[str, str]], error_code: str) -> int:
    normalized_error = str(error_code or "").strip()
    if not normalized_error:
        return 0
    return sum(1 for item in failed_items if str(item.get("error") or "").strip() == normalized_error)


def _reconcile_run_results(
    settings,
    *,
    annual_enabled: bool,
    rank_enabled: bool,
    include_stale_candidates: bool,
    selected_candidates: list[dict[str, object]],
    annual_success_codes: list[str],
    rank_success_codes: list[str],
    failed_items: list[dict[str, str]],
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    if not selected_candidates:
        return annual_success_codes, rank_success_codes, failed_items
    if not annual_success_codes and not rank_success_codes:
        return annual_success_codes, rank_success_codes, failed_items

    post_universe, _, _ = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=include_stale_candidates,
    )
    post_universe_map = {
        str(row.get("fcode") or "").strip(): row
        for row in post_universe
        if str(row.get("fcode") or "").strip()
    }
    return _reconcile_stale_refresh_progress(
        selected_candidates=selected_candidates,
        annual_success_codes=annual_success_codes,
        rank_success_codes=rank_success_codes,
        failed_items=failed_items,
        post_universe_map=post_universe_map,
    )


def _sync_targeted_failure_backoff_state(
    settings,
    *,
    annual_enabled: bool,
    rank_enabled: bool,
    include_stale_candidates: bool,
    selected_candidates: list[dict[str, object]],
    failed_items: list[dict[str, str]],
) -> bool:
    scheduler_state_file = getattr(settings, "scheduler_state_file", None)
    if not scheduler_state_file or not selected_candidates:
        return False

    target_codes = {
        _normalize_code(str(row.get("fcode") or "").strip())
        for row in selected_candidates
        if _normalize_code(str(row.get("fcode") or "").strip())
    }
    if not target_codes:
        return False

    _, post_candidates, _ = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=include_stale_candidates,
    )
    post_candidate_codes = {
        _normalize_code(str(row.get("fcode") or "").strip())
        for row in post_candidates
        if _normalize_code(str(row.get("fcode") or "").strip())
    }

    state_before = _load_maintenance_state(settings)
    empty_payload_retry_days = _resolve_empty_payload_retry_days(settings)
    stale_refresh_retry_days = _resolve_stale_refresh_retry_days(settings)
    empty_payload_upstream_limited_failures = _resolve_empty_payload_upstream_limited_failures(settings)
    stale_refresh_upstream_limited_failures = _resolve_stale_refresh_upstream_limited_failures(settings)
    failure_backoff_before = _hydrate_failure_backoff_policy(
        state_before.get("failure_backoff") or {},
        empty_payload_retry_days=empty_payload_retry_days,
        stale_refresh_retry_days=stale_refresh_retry_days,
        empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
        stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
    )

    failed_codes = {
        _normalize_code(str(item.get("fcode") or "").strip())
        for item in failed_items
        if _normalize_code(str(item.get("fcode") or "").strip())
    }
    targeted_backoff = {
        code: dict(record)
        for code, record in failure_backoff_before.items()
        if code in target_codes
    }
    for code in list(targeted_backoff):
        if code not in post_candidate_codes:
            targeted_backoff.pop(code, None)

    if failed_codes:
        failed_candidates = [
            row
            for row in selected_candidates
            if _normalize_code(str(row.get("fcode") or "").strip()) in failed_codes
        ]
        failed_items_filtered = [
            item
            for item in failed_items
            if _normalize_code(str(item.get("fcode") or "").strip()) in failed_codes
        ]
        failed_backoff_after = _update_failure_backoff_after_run(
            {
                code: dict(record)
                for code, record in targeted_backoff.items()
                if code in failed_codes
            },
            selected_candidates=failed_candidates,
            failed_items=failed_items_filtered,
            candidate_codes_after={
                code for code in post_candidate_codes if code in failed_codes
            },
            failed_at=datetime.now().isoformat(timespec="seconds"),
            empty_payload_retry_days=empty_payload_retry_days,
            stale_refresh_retry_days=stale_refresh_retry_days,
            empty_payload_upstream_limited_failures=empty_payload_upstream_limited_failures,
            stale_refresh_upstream_limited_failures=stale_refresh_upstream_limited_failures,
        )
        targeted_backoff.update(failed_backoff_after)

    merged_backoff = {
        code: dict(record)
        for code, record in failure_backoff_before.items()
        if code not in target_codes
    }
    merged_backoff.update(targeted_backoff)
    _save_maintenance_state(
        settings,
        next_index=int(state_before.get("next_index") or 0),
        failure_backoff=merged_backoff,
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="回补债券基金长期稳定性历史数据（年度涨幅 + 同类排名历史）")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 只债券基金，0 表示全部")
    parser.add_argument("--offset", type=int, default=0, help="从候选列表第 N 个样本开始处理，默认 0")
    parser.add_argument("--annual-only", action="store_true", help="只回补 fund_annual_returns")
    parser.add_argument("--rank-only", action="store_true", help="只回补 fund_rank_history")
    parser.add_argument("--force-update", action="store_true", help="强制重采已存在历史的基金")
    parser.add_argument(
        "--refresh-stale",
        action="store_true",
        help="纳入长期历史已存在但已陈旧的样本（例如 stale annual/rank history）",
    )
    parser.add_argument("--workers", type=int, default=4, help="并发数，默认 4")
    parser.add_argument("--codes-file", type=str, default="", help="只处理文件中的基金代码，支持 JSON 数组或每行一个代码")
    parser.add_argument("--failed-codes-output", type=str, default="", help="将失败代码写入指定 JSON 文件")
    args = parser.parse_args()

    if args.annual_only and args.rank_only:
        raise SystemExit("--annual-only 和 --rank-only 不能同时使用")

    settings = load_settings()
    core_config = load_runtime_config(settings.config_path)
    db_manager = get_db_manager(db_path=str(settings.database_path))
    api_client = APIClient(core_config)

    annual_collector = AnnualReturnCollector(config=core_config, db_manager=db_manager, api_client=api_client)
    rank_collector = RankCollector(config=core_config, db_manager=db_manager, api_client=api_client)
    annual_enabled = not args.rank_only
    rank_enabled = not args.annual_only
    include_stale_candidates = bool(args.force_update or args.refresh_stale)
    universe, candidates, audit_stats = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=include_stale_candidates,
    )

    selection_pool = universe if args.force_update else candidates
    codes_file = str(args.codes_file or "").strip()
    current_published_priority_summary = {
        "available": False,
        "reason": "targeted_or_force_update_mode",
        "code_count": 0,
    }
    current_published_priority_codes: set[str] = set()
    if not args.force_update and not codes_file:
        current_published_priority_codes, current_published_priority_summary = _load_current_published_formal_codes(
            settings
        )
        selection_pool = _prioritize_current_published_and_governance_candidates(
            selection_pool,
            current_published_priority_codes,
            set(),
        )
    if codes_file:
        code_set = set(_load_codes_from_file(Path(codes_file)))
        selection_pool = [
            row for row in selection_pool
            if _normalize_code(str(row.get("fcode") or "").strip()) in code_set
        ]
    normalized_offset = max(0, int(args.offset or 0))
    if normalized_offset:
        selection_pool = selection_pool[normalized_offset:]
    if args.limit and args.limit > 0:
        selection_pool = selection_pool[: args.limit]

    selected_candidates = list(selection_pool)
    selected_codes = [str(row.get("fcode") or "").strip() for row in selected_candidates if str(row.get("fcode") or "").strip()]
    annual_candidates = (
        []
        if args.rank_only
        else [
            row["fcode"]
            for row in selected_candidates
            if args.force_update or bool(row.get("needs_annual_returns")) or bool(args.refresh_stale and row.get("stale_annual_returns"))
        ]
    )
    rank_candidates = (
        []
        if args.annual_only
        else [
            row["fcode"]
            for row in selected_candidates
            if args.force_update or bool(row.get("needs_rank_history")) or bool(args.refresh_stale and row.get("stale_rank_history"))
        ]
    )

    history_counts_before = _distinct_history_counts(settings.database_path)
    started_at = time.time()
    evidence_context = {
        "collection_mode": "supplement_bond_long_term_history",
        "force_update": bool(args.force_update),
        "refresh_stale": bool(args.refresh_stale),
        "annual_only": bool(args.annual_only),
        "rank_only": bool(args.rank_only),
    }
    _reset_collector_api_source_evidence(
        annual_collector,
        target_count=len(annual_candidates),
        **evidence_context,
    )
    _reset_collector_api_source_evidence(
        rank_collector,
        target_count=len(rank_candidates),
        **evidence_context,
    )

    annual_success_codes: list[str] = []
    rank_success_codes: list[str] = []
    annual_failed_items: list[dict[str, str]] = []
    rank_failed_items: list[dict[str, str]] = []
    normalized_workers = max(1, int(args.workers or 1))

    if annual_candidates:
        annual_success_codes, annual_failed_items = _run_stage_concurrent(
            collector=annual_collector,
            codes=annual_candidates,
            collect_func=annual_collector.collect_fund_annual_returns,
            stage="annual_returns",
            workers=normalized_workers,
            log=print,
        )

    if rank_candidates:
        rank_success_codes, rank_failed_items = _run_stage_concurrent(
            collector=rank_collector,
            codes=rank_candidates,
            collect_func=rank_collector.collect_fund_rank_history,
            stage="rank_history",
            workers=normalized_workers,
            log=print,
        )

    failed_items = annual_failed_items + rank_failed_items
    annual_success_codes, rank_success_codes, failed_items = _reconcile_run_results(
        settings,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=include_stale_candidates,
        selected_candidates=selected_candidates,
        annual_success_codes=annual_success_codes,
        rank_success_codes=rank_success_codes,
        failed_items=failed_items,
    )

    annual_success = len(annual_success_codes)
    rank_success = len(rank_success_codes)
    annual_failed_count = max(0, len(annual_candidates) - annual_success)
    rank_failed_count = max(0, len(rank_candidates) - rank_success)
    _update_collector_api_source_evidence_context(
        annual_collector,
        success_count=annual_success,
        failed_count=annual_failed_count,
    )
    _update_collector_api_source_evidence_context(
        rank_collector,
        success_count=rank_success,
        failed_count=rank_failed_count,
    )
    api_source_evidence = build_long_term_history_api_source_evidence(
        annual_collector=annual_collector,
        rank_collector=rank_collector,
        annual_target_total=len(annual_candidates),
        rank_target_total=len(rank_candidates),
        annual_success_total=annual_success,
        rank_success_total=rank_success,
        annual_failed_total=annual_failed_count,
        rank_failed_total=rank_failed_count,
        context=evidence_context,
    )
    annual_no_progress_count = (
        _count_failed_items_by_error(failed_items, "stale_annual_refresh_no_progress")
        + _count_failed_items_by_error(failed_items, "annual_history_gap_no_progress")
    )
    rank_no_progress_count = (
        _count_failed_items_by_error(failed_items, "stale_rank_refresh_no_progress")
        + _count_failed_items_by_error(failed_items, "rank_history_gap_no_progress")
    )

    history_counts_after = _distinct_history_counts(settings.database_path)
    elapsed_seconds = round(time.time() - started_at, 2)
    maintenance_state_synced = _sync_targeted_failure_backoff_state(
        settings,
        annual_enabled=annual_enabled,
        rank_enabled=rank_enabled,
        include_stale_candidates=include_stale_candidates,
        selected_candidates=selected_candidates,
        failed_items=failed_items,
    )
    failed_codes = sorted({
        _normalize_code(str(item.get("fcode") or "").strip())
        for item in failed_items
        if _normalize_code(str(item.get("fcode") or "").strip())
    })
    failed_codes_output = str(args.failed_codes_output or "").strip()
    failed_codes_artifact = ""
    if failed_codes_output:
        failed_path = Path(failed_codes_output)
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        failed_path.write_text(
            json.dumps(failed_codes, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        failed_codes_artifact = str(failed_path)

    summary = {
        "processed_fund_count": len(selected_codes),
        "annual_only": bool(args.annual_only),
        "rank_only": bool(args.rank_only),
        "force_update": bool(args.force_update),
        "refresh_stale": bool(args.refresh_stale),
        "offset": normalized_offset,
        "workers": normalized_workers,
        "selection_pool_total": len(candidates if not args.force_update else universe),
        "selected_codes_sample": selected_codes[:20],
        "current_published_priority": {
            **current_published_priority_summary,
            "selected_hit_count": sum(
                1
                for code in selected_codes
                if code
                in {
                    _normalize_code(str(item or "").strip())
                    for item in current_published_priority_codes
                }
            )
            if not args.force_update and not codes_file
            else 0,
        },
        "annual_candidate_count": len(annual_candidates),
        "rank_candidate_count": len(rank_candidates),
        "annual_success_count": annual_success,
        "rank_success_count": rank_success,
        "annual_no_progress_count": annual_no_progress_count,
        "rank_no_progress_count": rank_no_progress_count,
        "stale_refresh_no_progress_total": annual_no_progress_count + rank_no_progress_count,
        "annual_failed_count": annual_failed_count,
        "rank_failed_count": rank_failed_count,
        "api_source_evidence": api_source_evidence,
        "failed_codes_total": len(failed_codes),
        "failed_codes_sample": failed_codes[:50],
        "failed_items_sample": failed_items[:50],
        "failed_codes_artifact": failed_codes_artifact or None,
        "history_counts_before": history_counts_before,
        "history_counts_after": history_counts_after,
        "audit_stats": audit_stats,
        "elapsed_seconds": elapsed_seconds,
        "database_path": str(settings.database_path),
        "maintenance_state_synced": maintenance_state_synced,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
