#!/usr/bin/env python3
"""Collect one auditable, read-only snapshot of fund trading status."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.fund_status_api import ENDPOINT, fetch_status_snapshot, write_snapshot_artifact  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codes", nargs="*", default=[], help="Optional six-digit fund codes to project from the full table")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    settings = load_settings()
    snapshot = fetch_status_snapshot(
        codes=args.codes,
        endpoint=getattr(settings, "fund_status_url", ENDPOINT),
        timeout=(5.0, max(1.0, args.timeout)),
    )
    path = write_snapshot_artifact(snapshot, settings.reports_dir)
    print(f"status={snapshot.get('status')} rows={len(snapshot.get('rows') or [])} missing={len(snapshot.get('missing_codes') or [])}")
    print(f"artifact={path}")
    if snapshot.get("error"):
        print(f"error={snapshot['error']}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
