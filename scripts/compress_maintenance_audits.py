#!/usr/bin/env python3
"""Preview, apply an exact bounded plan, or restore a global maintenance audit."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.maintenance_audit_compression import (
    apply_maintenance_audit_plan, plan_maintenance_audit_compression, restore_maintenance_audit,
)
from backend.storage import WorkflowStore
from backend.task_audit_storage import read_task_audit, write_task_audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--max-files", type=int, default=32)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--restore", type=Path)
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if args.restore:
        if not args.destination or args.apply or args.plan:
            parser.error("--restore requires --destination; cannot combine with --apply/--plan")
        print(json.dumps(restore_maintenance_audit(args.restore, args.destination), ensure_ascii=False))
        return 0
    if args.destination or bool(args.apply) != bool(args.plan):
        parser.error("--apply requires --plan; --destination requires --restore")
    settings = load_settings(args.config)
    output = args.output or (settings.reports_dir / "audit/maintenance_audit_compression"
                            / f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json.gz")
    if output.exists() or output.is_symlink():
        parser.error("output already exists")
    if args.apply:
        plan = read_task_audit(args.plan)
        store = WorkflowStore(settings.state_file)
        try:
            with store._lock:
                store._acquire_run_lock_locked("compress_maintenance_audits")
            result = apply_maintenance_audit_plan(settings.project_root, plan)
        finally:
            store.close()
        result["plan_path"] = str(args.plan)
    else:
        result = plan_maintenance_audit_compression(settings.project_root, max_files=args.max_files)
    write_task_audit(output, result)
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {"files", "candidates", "retained"}} | {"artifact": str(output)},
                     ensure_ascii=False))
    return 2 if result.get("failed_count") or result.get("unprocessed_count") else 0


if __name__ == "__main__":
    raise SystemExit(main())
