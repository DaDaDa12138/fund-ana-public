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
from utils.rank_history_archive_apply_runner import (  # noqa: E402
    DEFAULT_ARCHIVE_PAGE_LIMIT,
    DEFAULT_OUTPUT_DIR,
    build_rank_history_archive_apply_runner_payload,
    write_rank_history_archive_apply_runner_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="执行或预演 fund_rank_history 热冷归档；删除热库前必须满足精确匹配门禁。"
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="runner 产物输出目录。")
    parser.add_argument(
        "--source-preflight-artifact",
        default="",
        help="指定 rank preflight JSON；默认读取 output-dir 下最新 preflight。",
    )
    parser.add_argument(
        "--backup-artifact-path",
        default="",
        help="hot_delete 模式的回滚备份路径；archive_export/dry_run 不需要。",
    )
    parser.add_argument(
        "--mode",
        choices=["dry_run", "archive_export", "hot_delete", "auto"],
        default="dry_run",
        help="默认 dry_run；auto 会先归档，归档完整且有备份后才删除。",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=DEFAULT_ARCHIVE_PAGE_LIMIT,
        help="单页归档或删除行数上限。",
    )
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    payload = build_rank_history_archive_apply_runner_payload(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        project_root=settings.project_root,
        output_dir=args.output_dir,
        source_preflight_artifact=(str(args.source_preflight_artifact or "").strip() or None),
        backup_artifact_path=(str(args.backup_artifact_path or "").strip() or None),
        mode=args.mode,
        page_limit=args.page_limit,
    )
    json_path, md_path = write_rank_history_archive_apply_runner_artifacts(
        payload,
        output_dir=args.output_dir,
        timestamp=(str(args.timestamp or "").strip() or None),
    )
    print(
        json.dumps(
            {
                "status": "success",
                "artifact_path": str(json_path),
                "markdown_path": str(md_path),
                "summary": payload.get("summary") or {},
                "blockers": payload.get("blockers") or [],
                "safety": payload.get("safety") or {},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
