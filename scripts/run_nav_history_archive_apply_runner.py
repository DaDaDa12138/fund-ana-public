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
from utils.nav_history_archive_apply_runner import (  # noqa: E402
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    DEFAULT_OUTPUT_DIR,
    build_nav_history_archive_apply_runner_payload,
    write_nav_history_archive_apply_runner_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "运行 NAV 热冷库归档治理 runner；默认 dry_run 只读。"
            "mode=archive_export 只向独立归档库分页写入冷数据，不删除热库或执行 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="NAV archive audit 输出目录。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument(
        "--mode",
        default="dry_run",
        choices=("dry_run", "archive_export"),
        help="执行模式；archive_export 仅写独立归档库，不删除热库或执行 VACUUM。",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=DEFAULT_ARCHIVE_PAGE_LIMIT,
        help="dry_run 抽样行数上限；archive_export 单页写入行数上限。",
    )
    parser.add_argument(
        "--source-readiness-artifact",
        default="",
        help="指定 readiness JSON；默认读取 output-dir 下最新 readiness 产物。",
    )
    parser.add_argument(
        "--source-cold-query-compat-artifact",
        default="",
        help="指定 cold query compat JSON；默认读取 output-dir 下最新产物。",
    )
    parser.add_argument(
        "--source-runbook-artifact",
        default="",
        help="指定 runbook JSON；默认读取 output-dir 下最新 runbook 产物。",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    payload = build_nav_history_archive_apply_runner_payload(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        project_root=settings.project_root,
        source_readiness_artifact=args.source_readiness_artifact or None,
        source_cold_query_compat_artifact=args.source_cold_query_compat_artifact or None,
        source_runbook_artifact=args.source_runbook_artifact or None,
        output_dir=args.output_dir,
        mode=args.mode,
        page_limit=args.page_limit,
    )
    json_path, md_path = write_nav_history_archive_apply_runner_artifacts(
        payload,
        output_dir=args.output_dir,
        timestamp=(str(args.timestamp or "").strip() or None),
    )

    result = {
        "status": "success",
        "artifact_path": str(json_path),
        "markdown_path": str(md_path),
        "summary": payload.get("summary") or {},
        "blockers": payload.get("blockers") or [],
        "findings": payload.get("findings") or [],
        "safety": payload.get("safety")
        or {
            "archive_sql_executed": False,
            "delete_sql_executed": False,
            "vacuum_executed": False,
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
