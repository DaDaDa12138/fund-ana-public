#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.core_runner import resolve_core_python
from backend.effective_nav import load_effective_nav_snapshot
from backend.nav_history_maintenance import (
    _partition_preferred_sync_candidates,
    _read_candidates,
    run_nav_history_maintenance,
)


class BackfillLock:
    def __init__(self, path: Path):
        self.path = path
        self._handle = None

    def __enter__(self) -> "BackfillLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            holder = handle.read().strip()
            handle.close()
            raise RuntimeError(
                f"数据库补完任务已有实例运行中，锁文件: {self.path}"
                + (f"，当前持有信息: {holder}" if holder else "")
            ) from exc

        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                    "lock_path": str(self.path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        handle.flush()
        self._handle = handle
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            self._handle.seek(0)
            self._handle.truncate()
            self._handle.flush()
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def _build_progress_snapshot(settings) -> dict[str, Any]:
    snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    )
    candidates, stats = _read_candidates(settings.database_path)
    sync_issue_types = {"nav_value_mismatch", "nav_history_lag", "returns_lag"}
    sync_candidates = [
        row for row in candidates if sync_issue_types & set(row.get("issue_types") or [])
    ]
    preferred, others = _partition_preferred_sync_candidates(
        sync_candidates,
        preferred_nav_date=snapshot.candidate_date,
    )

    conn = sqlite3.connect(str(settings.database_path))
    try:
        cursor = conn.cursor()
        candidate_date = str(snapshot.candidate_date or "").strip()
        risk_counts = {}
        for period in ("1y", "3y", "5y"):
            risk_counts[period] = int(
                cursor.execute(
                    "SELECT COUNT(DISTINCT fcode) FROM fund_risk_metrics WHERE period = ? AND end_date = ?",
                    (period, candidate_date),
                ).fetchone()[0]
                or 0
            )
        nav_history_same_date = int(
            cursor.execute(
                "SELECT COUNT(DISTINCT fcode) FROM fund_nav_history WHERE nav_date = ?",
                (candidate_date,),
            ).fetchone()[0]
            or 0
        )
        latest_nav_funds = int(
            cursor.execute(
                "SELECT COUNT(*) FROM fund_returns WHERE latest_nav_date = ?",
                (candidate_date,),
            ).fetchone()[0]
            or 0
        )
    finally:
        conn.close()

    return {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "effective_candidate_date": snapshot.candidate_date,
        "latest_nav_funds": latest_nav_funds,
        "nav_history_same_date_funds": nav_history_same_date,
        "risk_same_date_funds": risk_counts,
        "missing_nav_total": int(stats.get("missing_nav_total") or 0),
        "truncated_history_total": int(stats.get("truncated_history_total") or 0),
        "nav_value_mismatch_total": int(stats.get("nav_value_mismatch_total") or 0),
        "nav_history_lag_total": int(stats.get("nav_history_lag_total") or 0),
        "returns_lag_total": int(stats.get("returns_lag_total") or 0),
        "sync_candidate_total": len(sync_candidates),
        "preferred_sync_candidate_total": len(preferred),
        "non_preferred_sync_candidate_total": len(others),
    }


def _print_progress(label: str, payload: dict[str, Any]) -> None:
    risk = payload.get("risk_same_date_funds") or {}
    print(
        json.dumps(
            {
                "label": label,
                "captured_at": payload.get("captured_at"),
                "effective_candidate_date": payload.get("effective_candidate_date"),
                "latest_nav_funds": payload.get("latest_nav_funds"),
                "nav_history_same_date_funds": payload.get("nav_history_same_date_funds"),
                "risk_1y_same_date_funds": risk.get("1y"),
                "risk_3y_same_date_funds": risk.get("3y"),
                "risk_5y_same_date_funds": risk.get("5y"),
                "missing_nav_total": payload.get("missing_nav_total"),
                "truncated_history_total": payload.get("truncated_history_total"),
                "nav_value_mismatch_total": payload.get("nav_value_mismatch_total"),
                "nav_history_lag_total": payload.get("nav_history_lag_total"),
                "sync_candidate_total": payload.get("sync_candidate_total"),
                "preferred_sync_candidate_total": payload.get("preferred_sync_candidate_total"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _run_changed_nav_risk_refresh(settings) -> None:
    python_bin = resolve_core_python(settings)
    command = [
        python_bin,
        "main.py",
        "calculate-risk",
        "--yes",
        "--workers",
        str(settings.risk_workers),
        "--changed-nav-only",
        "--force",
    ]
    subprocess.run(
        command,
        cwd=str(settings.core_root),
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="分批执行数据库补完收敛")
    parser.add_argument("--rounds", type=int, default=1, help="执行轮数")
    parser.add_argument("--nav-limit", type=int, default=0, help="每轮净值历史补齐上限，0 表示使用配置值")
    parser.add_argument(
        "--issue-types",
        type=str,
        default="nav_value_mismatch,nav_history_lag,returns_lag",
        help="逗号分隔的问题类型，如 missing_nav,truncated_history 或 nav_value_mismatch,nav_history_lag,returns_lag",
    )
    parser.add_argument("--skip-risk-refresh", action="store_true", help="仅补齐净值历史，不执行 changed-nav 风险同步")
    parser.add_argument(
        "--progress-file",
        type=str,
        default="reports/audit/backfill_convergence/latest_progress.json",
        help="进度快照输出路径",
    )
    args = parser.parse_args()

    settings = load_settings()
    progress_path = (PROJECT_ROOT / args.progress_file).resolve()
    progress_path.parent.mkdir(parents=True, exist_ok=True)

    lock_path = progress_path.with_name("run.lock")
    nav_limit = max(0, int(args.nav_limit or 0)) or int(settings.scheduler_daily_nav_history_sync_limit)
    issue_types = [item.strip() for item in str(args.issue_types or "").split(",") if item.strip()]
    if not issue_types:
        issue_types = ["nav_value_mismatch", "nav_history_lag", "returns_lag"]

    with BackfillLock(lock_path):
        initial = _build_progress_snapshot(settings)
        _print_progress("before", initial)

        history: list[dict[str, Any]] = [{"label": "before", "snapshot": initial}]

        for round_index in range(1, max(1, int(args.rounds or 1)) + 1):
            print(f"\n[backfill] round={round_index} nav_limit={nav_limit} issue_types={issue_types}")
            nav_summary = run_nav_history_maintenance(
                settings,
                limit=nav_limit,
                allowed_issue_types=issue_types,
                log=lambda message: print(f"[nav_history] {message}"),
            )
            if not args.skip_risk_refresh:
                _run_changed_nav_risk_refresh(settings)

            current = _build_progress_snapshot(settings)
            _print_progress(f"after_round_{round_index}", current)
            history.append(
                {
                    "label": f"after_round_{round_index}",
                    "snapshot": current,
                    "nav_history_summary": nav_summary,
                }
            )

        progress_path.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "rounds": max(1, int(args.rounds or 1)),
                    "nav_limit": nav_limit,
                    "issue_types": issue_types,
                    "skip_risk_refresh": bool(args.skip_risk_refresh),
                    "lock_path": str(lock_path),
                    "history": history,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\n[backfill] progress written to {progress_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
