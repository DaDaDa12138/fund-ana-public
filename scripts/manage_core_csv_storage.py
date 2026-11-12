#!/usr/bin/env python3
"""Preview/archive closed core CSV, or restore verified ordinary CSV bytes."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.core_csv_compression import MAX_BATCH_FILES, restore_core_csv, run_core_csv_compression


def main() -> int:
    parser = argparse.ArgumentParser(description="历史核心 CSV 无损压缩与完整恢复（默认仅预演）")
    parser.add_argument("--apply", action="store_true", help="执行有界压缩，或实际恢复")
    parser.add_argument("--max-files", type=int, default=MAX_BATCH_FILES, help="本批最多文件数，不超过256")
    parser.add_argument("--restore", type=Path, help="原 CSV 逻辑路径（不是 .zst 路径）")
    parser.add_argument("--destination", type=Path, help="独立恢复目标；省略时恢复原路径，不覆盖文件")
    args = parser.parse_args()
    settings = load_settings()
    if args.destination and not args.restore:
        parser.error("--destination 需要 --restore")
    if args.restore:
        if not args.apply:
            parser.error("实际恢复需要 --apply")
        result = restore_core_csv(settings, args.restore, args.destination)
    else:
        result = run_core_csv_compression(
            settings, keep_days=settings.scheduler_weekly_disk_cleanup_core_csv_keep_days,
            apply=args.apply, max_files=args.max_files,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
