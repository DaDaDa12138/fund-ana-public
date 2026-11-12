#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings  # noqa: E402
from utils.task_history_cleanup_execution import (  # noqa: E402
    DEFAULT_ARCHIVE_DIR,
    DEFAULT_BACKUP_PREFIX,
    DEFAULT_ELIGIBLE_STATUSES,
    DEFAULT_OUTPUT_DIR,
    REQUIRED_APPLY_CONFIRMATION,
    execute_task_history_cleanup,
)


def _split_statuses(raw: str) -> tuple[str, ...]:
    statuses = tuple(
        item.strip().lower()
        for item in str(raw or "").split(",")
        if item.strip()
    )
    return statuses or tuple(DEFAULT_ELIGIBLE_STATUSES)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "运行 task_history guarded cleanup；默认 dry-run 且不修改数据库。"
            "apply 必须指定 dry-run artifact，并传入确认串 "
            f"{REQUIRED_APPLY_CONFIRMATION}。执行器会生成备份、archive 和 post consistency 证据，且不会 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="审计产物输出目录。")
    parser.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR, help="apply 时归档 rows/summary 的目录。")
    parser.add_argument("--backup-dir", default="backend/data/backups", help="apply 时自动备份输出目录。")
    parser.add_argument("--backup-file", default="", help="apply 时绑定已有备份文件；为空则自动创建。")
    parser.add_argument("--backup-prefix", default=DEFAULT_BACKUP_PREFIX, help="自动备份文件前缀。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument(
        "--mode",
        default="dry-run",
        choices=("dry-run", "apply"),
        help="执行模式。dry-run 只生成计划；apply 会归档并删除候选行。",
    )
    parser.add_argument(
        "--source-plan-artifact",
        default="",
        help="apply 必填：前置 dry-run 生成的 JSON artifact。",
    )
    parser.add_argument(
        "--confirm-apply",
        default="",
        help=f"apply 必填确认串：{REQUIRED_APPLY_CONFIRMATION}",
    )
    parser.add_argument("--retention-days", type=int, default=14, help="保留最近 N 天记录。")
    parser.add_argument(
        "--recent-keep-per-task-status",
        type=int,
        default=20,
        help="每个 task/status 额外保留最近 N 条。",
    )
    parser.add_argument(
        "--eligible-statuses",
        default=",".join(DEFAULT_ELIGIBLE_STATUSES),
        help="允许清理的 status，逗号分隔；默认 success,skipped。",
    )
    parser.add_argument("--max-sample-rows", type=int, default=20, help="artifact 中保留的样本行数。")
    parser.add_argument(
        "--no-compress-backup",
        action="store_true",
        help="apply 自动备份时不压缩；主要用于本地测试。",
    )
    parser.add_argument(
        "--compact-json",
        action="store_true",
        help="以单行 JSON 输出，便于调度链读取。",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    mode = str(args.mode or "dry-run").strip().lower()
    source_plan_artifact = str(args.source_plan_artifact or "").strip() or None
    if mode == "apply" and not source_plan_artifact:
        parser.error("apply requires --source-plan-artifact")

    json_path, md_path, payload = execute_task_history_cleanup(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        source_plan_artifact=source_plan_artifact,
        output_dir=args.output_dir,
        archive_dir=args.archive_dir,
        mode=mode,
        confirm_apply=str(args.confirm_apply or "").strip() or None,
        retention_days=args.retention_days,
        recent_keep_per_task_status=args.recent_keep_per_task_status,
        eligible_statuses=_split_statuses(args.eligible_statuses),
        max_sample_rows=args.max_sample_rows,
        backup_file=str(args.backup_file or "").strip() or None,
        backup_dir=args.backup_dir,
        backup_prefix=args.backup_prefix,
        compress_backup=not bool(args.no_compress_backup),
        timestamp=(str(args.timestamp or "").strip() or None),
    )
    result = {
        "status": payload.get("status"),
        "mode": payload.get("mode"),
        "artifact_path": str(json_path),
        "markdown_path": str(md_path),
        "summary": payload.get("summary") or {},
        "plan_fingerprint": payload.get("plan_fingerprint"),
        "source_plan_artifact": payload.get("source_plan_artifact"),
        "backup": payload.get("backup") or {},
        "archive": payload.get("archive") or {},
        "post_apply_consistency": payload.get("post_apply_consistency") or {},
        "guard_errors": payload.get("guard_errors") or [],
        "findings": payload.get("findings") or [],
    }
    if args.compact_json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
