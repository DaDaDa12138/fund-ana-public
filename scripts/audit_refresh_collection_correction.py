#!/usr/bin/env python3
"""Create or verify an immutable explanation of one historical collection task."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.config import load_settings
from backend.refresh_collection_correction import build_correction, load_verified_correction, write_correction


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", type=int, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preview", action="store_true", help="Read sources and reproduce without writing")
    mode.add_argument("--create", action="store_true", help="Create once; never replace the original history or explanation")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.preview:
            print(json.dumps(build_correction(settings, args.task_id), ensure_ascii=False, indent=2, allow_nan=False))
            return 0
        if args.create:
            write_correction(settings, build_correction(settings, args.task_id))
        result = load_verified_correction(settings, args.task_id)
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
        result = {"status": "invalid", "verified": False, "reason": str(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if result.get("verified") else 2


if __name__ == "__main__":
    raise SystemExit(main())
