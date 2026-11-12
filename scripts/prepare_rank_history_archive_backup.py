#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings  # noqa: E402
from utils.rank_history_archive_backup import (  # noqa: E402
    DEFAULT_BACKUP_PREFIX,
    DEFAULT_OUTPUT_DIR,
    create_rank_history_archive_backup,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "生成 rank 热库删除前的主库备份证据。默认 dry-run；"
            "不会执行归档、删除或 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="rank archive audit 输出目录。")
    parser.add_argument("--backup-dir", default="", help="备份输出目录，默认 backend/data/backups。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument("--backup-prefix", default=DEFAULT_BACKUP_PREFIX, help="备份文件名前缀。")
    parser.add_argument("--create-backup", action="store_true", help="实际生成 SQLite 备份。")
    parser.add_argument("--no-compress", action="store_true", help="不使用 zstd 压缩备份。")
    parser.add_argument("--keep-uncompressed", action="store_true", help="压缩后保留未压缩 .db。")
    parser.add_argument("--skip-sha256", action="store_true", help="省略汇总 SHA 字段；完整恢复校验仍会计算 SHA。")
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    backup_dir = Path(args.backup_dir) if args.backup_dir else settings.database_path.parent / "backups"
    timestamp = str(args.timestamp or "").strip() or None
    backup_json_path, backup_md_path, backup_payload = create_rank_history_archive_backup(
        source_db_path=settings.database_path,
        backup_dir=backup_dir,
        output_dir=args.output_dir,
        project_root=settings.project_root,
        backup_prefix=args.backup_prefix,
        timestamp=timestamp,
        compress=not args.no_compress,
        keep_uncompressed=args.keep_uncompressed,
        compute_sha256=not args.skip_sha256,
        dry_run=not args.create_backup,
    )
    result = {
        "status": backup_payload['status'],
        "mode": "create" if args.create_backup else "dry-run",
        "backup_evidence_artifact": str(backup_json_path),
        "backup_evidence_markdown": str(backup_md_path),
        "backup_summary": backup_payload.get("summary") or {},
        "backup_findings": backup_payload.get("findings") or [],
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if backup_payload['status'] in {'success', 'dry-run'} else 2


if __name__ == "__main__":
    raise SystemExit(main())
