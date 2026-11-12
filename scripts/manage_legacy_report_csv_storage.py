#!/usr/bin/env python3
"""Preview or losslessly compress old legacy holding-exclusion CSVs in bounded batches."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.legacy_report_csv_compression import run_legacy_report_csv_compression, restore_legacy_report_csv
from backend.task_audit_storage import write_task_audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-files", type=int, default=32)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--restore", type=Path)
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        parser.error("output already exists")
    if bool(args.restore) != bool(args.destination) or (args.restore and not args.apply):
        parser.error("--restore requires --destination and --apply")
    settings = load_settings()
    result = (restore_legacy_report_csv(settings, args.restore, args.destination) if args.restore
              else run_legacy_report_csv_compression(settings, apply=args.apply, max_files=args.max_files))
    write_task_audit(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key not in {"candidates", "outcomes"}},
                     ensure_ascii=False))
    return 2 if result.get("errors") or result.get("status") == "deferred" else 0


if __name__ == "__main__":
    raise SystemExit(main())
