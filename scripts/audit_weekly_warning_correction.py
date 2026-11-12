#!/usr/bin/env python3
"""Create or verify a historical warning explanation without editing history."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.config import load_settings
from backend.weekly_warning_correction import build_correction, load_verified_correction, write_correction


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--create", action="store_true", help="Write a new append-only explanation; never overwrite one")
    args = parser.parse_args()
    settings = load_settings()
    if args.create:
        write_correction(settings, build_correction(settings, args.task_id))
    result = load_verified_correction(settings, args.task_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("verified") else 2


if __name__ == "__main__":
    raise SystemExit(main())
