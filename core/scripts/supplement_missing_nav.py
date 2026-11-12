#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.nav_history_maintenance import run_nav_history_maintenance  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="补充缺失或历史截断的债基净值数据")
    parser.add_argument("--execute", action="store_true", help="执行实际回补")
    parser.add_argument("--limit", type=int, default=50, help="最多处理多少只基金")
    parser.add_argument("--workers", type=int, default=8, help="兼容旧调度参数，占位不使用")
    args = parser.parse_args()

    if not args.execute:
        print("未指定 --execute，跳过执行")
        return 0

    settings = load_settings(PROJECT_ROOT / "config" / "project.yaml")
    summary = run_nav_history_maintenance(settings, log=print, limit=max(int(args.limit or 0), 0) or None)
    processed_total = int(((summary.get("maintenance_result") or {}).get("processed_total")) or 0)
    failed_total = int(((summary.get("maintenance_result") or {}).get("failed_total")) or 0)
    candidate_total_after = int(((summary.get("audit_after") or {}).get("candidate_total")) or 0)

    print(
        f"净值历史维护完成: processed={processed_total}, "
        f"failed={failed_total}, remaining_candidates={candidate_total_after}"
    )
    if processed_total <= 0 and candidate_total_after <= 0:
        return 2
    return 0 if failed_total == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
