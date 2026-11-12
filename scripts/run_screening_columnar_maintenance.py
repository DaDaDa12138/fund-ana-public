#!/usr/bin/env python3
"""Preview or convert one immutable screening shard with a workflow owner lock."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'core')]

from backend.config import load_settings
from utils.screening_columnar_maintenance import maintain_columnar_archive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['preview', 'apply'], default='preview')
    parser.add_argument('--shard-id')
    parser.add_argument('--keep-source', action='store_true', help='Switch reads but retain the redundant original for deployment validation')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'reports/audit/screening_columnar_maintenance')
    args = parser.parse_args()
    settings = load_settings()
    bindings_path = ROOT / 'config/screening_parquet_bindings.json'
    copies = json.loads(bindings_path.read_text()).get('shadow_copies', []) if bindings_path.exists() else []
    result = maintain_columnar_archive(
        project_root=ROOT, active_archive_path=Path(settings.scheduler_weekly_screening_history_archive_execution_archive_target_path),
        hot_db_path=settings.database_path, output_dir=args.output_dir, mode=args.mode,
        shard_id=args.shard_id, keep_source=args.keep_source, existing_copies=copies,
        workflow_state_file=settings.state_file, log=lambda message: print(message, flush=True),
    )
    print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)
    return 2 if result['status'] in {'failed', 'deferred'} else 0


if __name__ == '__main__':
    raise SystemExit(main())
