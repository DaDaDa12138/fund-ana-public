#!/usr/bin/env python3
"""Preview, apply an exact bounded plan, or restore a historical global audit CSV attachment."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.legacy_audit_csv_compression import (
    apply_legacy_audit_csv_plan, plan_legacy_audit_csv_compression, restore_legacy_audit_csv,
)
from backend.storage import WorkflowStore
from backend.task_audit_storage import read_task_audit, write_task_audit
from backend.shared_audit_csv_compression import apply_shared_audit_csv_plan, plan_shared_audit_csv_compression


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--max-files", type=int, default=32)
    parser.add_argument("--shared", action="store_true", help="处理所有硬链接均在审计范围内的完整CSV组，支持中断续跑")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--restore", type=Path)
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if args.restore:
        if not args.destination or args.apply or args.plan:
            parser.error("--restore requires --destination; cannot combine with --apply/--plan")
        print(json.dumps(restore_legacy_audit_csv(args.restore, args.destination), ensure_ascii=False))
        return 0
    if args.destination or bool(args.apply) != bool(args.plan):
        parser.error("--apply requires --plan; --destination requires --restore")
    settings = load_settings(args.config)
    output = args.output or (settings.reports_dir / "audit/legacy_audit_csv_compression"
                            / f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json.gz")
    if output.exists() or output.is_symlink():
        parser.error("output already exists")
    if args.apply:
        plan = read_task_audit(args.plan)
        store = WorkflowStore(settings.state_file)
        try:
            with store._lock:
                store._acquire_run_lock_locked("compress_legacy_audit_csvs")
            apply_plan = apply_shared_audit_csv_plan if args.shared else apply_legacy_audit_csv_plan
            result = apply_plan(settings.project_root, plan)
        finally:
            store.close()
        result["plan_path"] = str(args.plan)
    else:
        planner = plan_shared_audit_csv_compression if args.shared else plan_legacy_audit_csv_compression
        result = planner(settings.project_root, max_files=args.max_files)
    write_task_audit(output, result)
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {"files", "candidates", "retained"}} | {"artifact": str(output)},
                     ensure_ascii=False))
    return 2 if (result.get("failed_count") or result.get("unprocessed_count") or result.get("errors")
                 or result.get("unprocessed_group_count") or result.get("scan_complete") is False) else 0


if __name__ == "__main__":
    raise SystemExit(main())
