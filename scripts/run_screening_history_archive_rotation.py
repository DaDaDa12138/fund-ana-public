#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from utils.screening_history_archive_rotation import (  # noqa: E402
    DEFAULT_MAX_ACTIVE_SIZE_BYTES,
    build_screening_history_archive_rotation_payload,
    write_screening_history_archive_rotation_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit or rotate the writable screening-history SQLite archive. "
            "The default is a read-only dry-run."
        )
    )
    parser.add_argument(
        "--active-archive-path",
        default="backend/data/archives/screening_history_details_archive.db",
    )
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--output-dir", default="reports/audit/screening_history_archive_rotation")
    parser.add_argument(
        "--mode",
        choices=("dry-run", "auto", "rotate"),
        default="dry-run",
    )
    parser.add_argument(
        "--max-active-size-gb",
        type=float,
        default=DEFAULT_MAX_ACTIVE_SIZE_BYTES / 1024 / 1024 / 1024,
    )
    parser.add_argument("--min-active-run-count", type=int, default=1)
    parser.add_argument("--confirm-rotate", action="store_true")
    parser.add_argument("--skip-sha256", action="store_true")
    parser.add_argument("--timestamp", default="")
    args = parser.parse_args()

    payload = build_screening_history_archive_rotation_payload(
        active_archive_path=args.active_archive_path,
        manifest_path=(args.manifest_path or None),
        project_root=PROJECT_ROOT,
        mode=args.mode,
        max_active_size_bytes=max(1, int(args.max_active_size_gb * 1024**3)),
        min_active_run_count=max(1, args.min_active_run_count),
        confirm_rotate=bool(args.confirm_rotate),
        compute_sha256=not bool(args.skip_sha256),
        timestamp=(args.timestamp or None),
    )
    json_path, md_path = write_screening_history_archive_rotation_artifacts(
        payload,
        output_dir=args.output_dir,
        timestamp=(args.timestamp or None),
    )
    output = {
        "status": payload.get("status"),
        "artifact_path": str(json_path),
        "markdown_path": str(md_path),
        "summary": payload.get("summary") or {},
        "blockers": payload.get("blockers") or [],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 2 if payload.get("status") == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
