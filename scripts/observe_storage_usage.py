#!/usr/bin/env python3
"""Read runtime filesystem metadata; print a snapshot without inserting an automatic window."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.config import load_settings
from backend.storage_observation import collect_storage_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--max-seconds", type=float, default=40)
    args = parser.parse_args()
    if not 1 <= args.max_seconds <= 60:
        parser.error("--max-seconds must be between 1 and 60")
    result = collect_storage_snapshot(load_settings(args.config), max_seconds=args.max_seconds)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
