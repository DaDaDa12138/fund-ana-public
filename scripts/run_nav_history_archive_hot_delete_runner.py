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
    write_nav_history_archive_apply_runner_artifacts,
)
from utils.nav_history_archive_hot_delete_runner import (  # noqa: E402
    build_nav_history_archive_hot_delete_runner_payload,
    write_nav_history_archive_hot_delete_runner_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "运行 NAV 热库冷数据 page-limited hot delete runner；默认 dry_run。"
            "hot_delete 模式只删除已完整归档且通过 delete gate 的 cutoff 前热库行，不执行 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="NAV archive audit 输出目录。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument(
        "--source-delete-gate-artifact",
        default="",
        help="指定 delete gate audit JSON；默认读取 output-dir 下最新产物。",
    )
    parser.add_argument(
        "--mode",
        default="dry_run",
        choices=("dry_run", "hot_delete"),
        help="执行模式；hot_delete 会分页删除热库 cutoff 前行，但不执行 VACUUM。",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=DEFAULT_ARCHIVE_PAGE_LIMIT,
        help="hot_delete 单页删除行数上限。",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    payload = build_nav_history_archive_hot_delete_runner_payload(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        project_root=settings.project_root,
        source_delete_gate_artifact=args.source_delete_gate_artifact or None,
        output_dir=args.output_dir,
        mode=args.mode,
        page_limit=args.page_limit,
    )
    json_path, md_path = write_nav_history_archive_hot_delete_runner_artifacts(
        payload,
        output_dir=args.output_dir,
        timestamp=(str(args.timestamp or "").strip() or None),
    )
    post_apply_runner_artifact = ""
    post_apply_runner_markdown = ""
    post_apply_runner_payload = payload.get("post_apply_runner_payload")
    if isinstance(post_apply_runner_payload, dict) and post_apply_runner_payload:
        post_json_path, post_md_path = write_nav_history_archive_apply_runner_artifacts(
            post_apply_runner_payload,
            output_dir=args.output_dir,
        )
        post_apply_runner_artifact = str(post_json_path)
        post_apply_runner_markdown = str(post_md_path)
    result = {
        "status": "success",
        "artifact_path": str(json_path),
        "markdown_path": str(md_path),
        "post_apply_runner_artifact": post_apply_runner_artifact,
        "post_apply_runner_markdown": post_apply_runner_markdown,
        "summary": payload.get("summary") or {},
        "blockers": payload.get("blockers") or [],
        "review_items": payload.get("review_items") or [],
        "safety": payload.get("safety") or {},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
