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
from utils.nav_history_archive_backup import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    bind_nav_history_archive_backup_to_checklist,
    create_nav_history_archive_backup,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "生成 NAV 归档专用主库备份证据，并可绑定到 execution checklist。"
            "默认 dry-run，不执行归档、删除或 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="NAV archive audit 输出目录。")
    parser.add_argument("--backup-dir", default="", help="备份输出目录，默认 backend/data/backups。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument("--backup-prefix", default="bond_funds_pre_nav_archive", help="备份文件名前缀。")
    parser.add_argument("--create-backup", action="store_true", help="实际生成 SQLite 备份。")
    parser.add_argument("--bind-checklist", action="store_true", help="把生成/指定备份绑定到新的 execution checklist。")
    parser.add_argument("--backup-artifact", default="", help="跳过生成，直接绑定已有备份文件。")
    parser.add_argument("--source-execution-checklist-artifact", default="", help="指定源 execution checklist。")
    parser.add_argument("--no-compress", action="store_true", help="不使用 zstd 压缩备份。")
    parser.add_argument("--keep-uncompressed", action="store_true", help="压缩后保留未压缩 .db。")
    parser.add_argument("--skip-sha256", action="store_true", help="省略汇总 SHA 字段；完整恢复校验仍会计算 SHA。")
    args = parser.parse_args()
    if args.bind_checklist and not args.create_backup and not args.backup_artifact:
        parser.error("--bind-checklist requires --create-backup or --backup-artifact")

    settings = load_settings(Path(args.config) if args.config else None)
    backup_dir = Path(args.backup_dir) if args.backup_dir else settings.database_path.parent / "backups"
    timestamp = str(args.timestamp or "").strip() or None

    if args.backup_artifact:
        backup_artifact_path = Path(args.backup_artifact)
        backup_json_path = None
        backup_md_path = None
        backup_payload = {
            "status": "external_artifact",
            "summary": {
                "backup_artifact_path": str(backup_artifact_path),
                "created": False,
            },
            "findings": ["Using caller-provided backup artifact; no backup was created."],
        }
    else:
        backup_json_path, backup_md_path, backup_payload = create_nav_history_archive_backup(
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
        backup_artifact_path = Path(
            str((backup_payload.get("summary") or {}).get("backup_artifact_path") or "")
        )

    bind_result = None
    if args.bind_checklist and backup_payload['status'] in {'success', 'external_artifact'}:
        (
            checklist_json_path,
            checklist_md_path,
            readiness_json_path,
            readiness_md_path,
            bind_result,
        ) = bind_nav_history_archive_backup_to_checklist(
            backup_artifact_path=backup_artifact_path,
            output_dir=args.output_dir,
            project_root=settings.project_root,
            backup_evidence_artifact=backup_json_path,
            source_execution_checklist_artifact=(
                args.source_execution_checklist_artifact or None
            ),
            timestamp=timestamp,
        )
        bind_result = {
            **bind_result,
            "execution_checklist_markdown": str(checklist_md_path),
            "readiness_markdown": str(readiness_md_path),
            "execution_checklist_artifact_absolute": str(checklist_json_path),
            "readiness_artifact_absolute": str(readiness_json_path),
        }

    result = {
        "status": backup_payload['status'],
        "mode": "create" if args.create_backup else "dry-run",
        "backup_evidence_artifact": str(backup_json_path or ""),
        "backup_evidence_markdown": str(backup_md_path or ""),
        "backup_summary": backup_payload.get("summary") or {},
        "backup_findings": backup_payload.get("findings") or [],
        "bind_result": bind_result,
        "safety": {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if backup_payload['status'] in {'success', 'dry-run', 'external_artifact'} else 2


if __name__ == "__main__":
    raise SystemExit(main())
