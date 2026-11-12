#!/usr/bin/env python3
"""Preview verified maintenance-backup retention; --apply executes eligible removals."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.maintenance_backup_retention import run_maintenance_backup_retention


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = run_maintenance_backup_retention(load_settings(args.config), mode="apply" if args.apply else "dry-run")
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0 if result["status"] in {"applied", "dry-run", "skipped"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
