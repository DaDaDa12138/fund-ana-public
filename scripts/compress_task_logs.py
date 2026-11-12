"""Preview/apply bounded task-log compression or independently restore one log."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import load_settings
from backend.task_log_compression import restore_task_log, run_task_log_compression


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--restore-source", type=Path)
    parser.add_argument("--restore-target", type=Path)
    args = parser.parse_args()
    if bool(args.restore_source) != bool(args.restore_target) or (args.apply and args.restore_source):
        parser.error("restore requires both paths and cannot be combined with --apply")
    if args.restore_source:
        result = restore_task_log(args.restore_source, args.restore_target)
    else:
        settings = load_settings()
        result = run_task_log_compression(
            settings.project_root, keep_days=settings.scheduler_weekly_disk_cleanup_logs_keep_days,
            apply=args.apply,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 2 if result.get("errors") or result.get("scan_complete") is False else 0


if __name__ == "__main__":
    raise SystemExit(main())
