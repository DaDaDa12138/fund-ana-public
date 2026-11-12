#!/usr/bin/env python3
"""Preview or migrate eligible historical successful-test source packages."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.test_source_migration import migrate_legacy_sources


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit verified packages and retire the old representation")
    parser.add_argument("--run-dir", action="append", type=Path, help="Limit to an exact original run directory; may repeat")
    parser.add_argument("--limit", type=int, default=8, help="Maximum packages per invocation (1–16; default 8)")
    parser.add_argument("--min-age-hours", type=float, default=24, help="Protect recently written original records (minimum 1, default 24)")
    args = parser.parse_args()
    result = migrate_legacy_sources(ROOT, apply=args.apply, run_dirs=args.run_dir,
                                    max_runs=args.limit, min_age_hours=args.min_age_hours)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["errors"] or not result["scan_complete"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
