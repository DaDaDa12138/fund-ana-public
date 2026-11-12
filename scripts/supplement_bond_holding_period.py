#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


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
for path in [PROJECT_ROOT, CORE_ROOT]:
    path_text = str(path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from collectors.supplement_holding_period import HoldingPeriodCollector  # noqa: E402
from database.connection import DatabaseManager  # noqa: E402
from utils.holding_period_parser import (  # noqa: E402
    HoldingMatch,
    build_holding_update_fields,
    parse_holding_detail_struct,
    parse_holding_match,
    parse_holding_with_priority,
)
from utils.project_runtime_config import load_runtime_config  # noqa: E402


def _load_collector() -> HoldingPeriodCollector:
    config_path = PROJECT_ROOT / "config" / "project.yaml"
    config = load_runtime_config(config_path)
    db_path = str((config.get("database") or {}).get("path") or "").strip()
    db_manager = DatabaseManager(db_path=db_path) if db_path else None
    return HoldingPeriodCollector(config=config, db_manager=db_manager)


def main() -> int:
    parser = argparse.ArgumentParser(description="补强债券基金持有期识别")
    parser.add_argument("--dry-run", action="store_true", help="仅输出审计结果，不写入数据库")
    parser.add_argument("--limit", type=int, default=20, help="示例输出数量")
    parser.add_argument("--workers", type=int, default=10, help="并发数")
    parser.add_argument("--force-update", action="store_true", help="强制扫描全部基金")
    args = parser.parse_args()

    collector = _load_collector()
    funds = collector._get_funds_to_update(force_update=bool(args.force_update))
    updates: list[dict[str, object]] = []
    samples: list[dict[str, object]] = []
    propagated_updates: list[dict[str, object]] = []

    for fund in funds:
        payload = collector._build_holding_update(fund)
        if not payload:
            continue
        updates.append(payload)
        if len(samples) < args.limit:
            samples.append(
                {
                    "fcode": fund.get("fcode"),
                    "shortname": fund.get("shortname"),
                    "old_has_holding_period": int(fund.get("has_holding_period") or 0),
                    "old_holding_days": fund.get("holding_days"),
                    "old_holding_keywords": fund.get("holding_keywords"),
                    "old_holding_period_source": fund.get("holding_period_source"),
                    "new_has_holding_period": payload.get("has_holding_period", fund.get("has_holding_period")),
                    "new_holding_days": payload.get("holding_days", fund.get("holding_days")),
                    "new_holding_keywords": payload.get("holding_keywords", fund.get("holding_keywords")),
                    "new_holding_period_source": payload.get("holding_period_source"),
                }
            )

    if updates:
        propagated_updates = collector._apply_share_family_propagation(funds, updates)

    updated_count = len(updates) + len(propagated_updates)
    run_summary: dict[str, int] = {
        "total": len(funds),
        "updated": updated_count,
        "skipped": max(0, len(funds) - updated_count),
        "failed": 0,
    }
    if not args.dry_run:
        updated_count = int(
            collector.supplement_all(workers=max(1, int(args.workers or 10)), force_update=bool(args.force_update))
        )
        if isinstance(collector.last_run_summary, dict):
            run_summary = {
                "total": int(collector.last_run_summary.get("total") or 0),
                "updated": int(collector.last_run_summary.get("updated") or 0),
                "skipped": int(collector.last_run_summary.get("skipped") or 0),
                "failed": int(collector.last_run_summary.get("failed") or 0),
            }
        else:
            run_summary = {
                "total": len(funds),
                "updated": updated_count,
                "skipped": max(0, len(funds) - updated_count),
                "failed": 0,
            }

    total_count = max(0, int(run_summary.get("total") or 0))
    success_count = max(0, int(run_summary.get("updated") or 0))
    skipped_count = max(0, int(run_summary.get("skipped") or 0))
    failed_count = max(0, int(run_summary.get("failed") or 0))
    processed_count = min(total_count, success_count + skipped_count + failed_count)
    success_rate = (success_count / total_count * 100.0) if total_count else 100.0

    # 与其它采集脚本共享 completion contract，供调度器和真实性审计解析。
    print(f"总任务数:    {total_count:,}")
    print(f"已处理:      {processed_count:,}")
    print(f"成功:        {success_count:,}")
    print(f"跳过:        {skipped_count:,}")
    print(f"失败:        {failed_count:,}")
    print(f"成功率:      {success_rate:.2f}%")

    summary = {
        "database_path": str(collector.db_manager.db_path),
        "dry_run": bool(args.dry_run),
        "force_update": bool(args.force_update),
        "bond_fund_count": len(funds),
        "updated_count": updated_count,
        "direct_updated_count": len(updates),
        "share_family_propagated_count": len(propagated_updates),
        "completion_stats": {
            "total": total_count,
            "processed": processed_count,
            "success": success_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "success_rate": round(success_rate, 2),
        },
        "samples": samples,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


__all__ = [
    "HoldingMatch",
    "HoldingPeriodCollector",
    "build_holding_update_fields",
    "main",
    "parse_holding_detail_struct",
    "parse_holding_match",
    "parse_holding_with_priority",
]


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
