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
from utils.nav_history_archive_vacuum_runner import (  # noqa: E402
    DEFAULT_MIN_FREE_SPACE_MULTIPLIER,
    DEFAULT_ROLLBACK_DIR,
)
from utils.rank_history_archive_apply_runner import DEFAULT_OUTPUT_DIR  # noqa: E402
from utils.rank_history_archive_vacuum_runner import (  # noqa: E402
    DEFAULT_CANDIDATE_DIR,
    DEFAULT_ROLLBACK_PREFIX,
    build_rank_history_archive_vacuum_runner_payload,
    write_rank_history_archive_vacuum_runner_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "运行 rank 热冷库治理的 SQLite VACUUM INTO runner；默认 dry_run。"
            "auto 会先生成候选库；替换必须同时显式确认 replace 和服务已停止。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="rank archive audit 输出目录。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument(
        "--mode",
        default="dry_run",
        choices=("dry_run", "vacuum_into", "replace", "post_replace_audit", "auto"),
        help="执行模式。",
    )
    parser.add_argument("--candidate-path", default="", help="指定 VACUUM INTO 候选库路径。")
    parser.add_argument(
        "--candidate-dir",
        default=DEFAULT_CANDIDATE_DIR,
        help="未指定 candidate-path 时的候选库目录。",
    )
    parser.add_argument("--rollback-path", default="", help="指定 replace 前活动库回滚路径。")
    parser.add_argument(
        "--rollback-dir",
        default=DEFAULT_ROLLBACK_DIR,
        help="未指定 rollback-path 时的回滚目录。",
    )
    parser.add_argument(
        "--rollback-prefix",
        default=DEFAULT_ROLLBACK_PREFIX,
        help="未指定 rollback-path 时的回滚文件前缀。",
    )
    parser.add_argument(
        "--min-free-space-multiplier",
        type=float,
        default=DEFAULT_MIN_FREE_SPACE_MULTIPLIER,
        help="候选库生成/替换前要求的可用空间倍数。",
    )
    parser.add_argument(
        "--confirm-replace",
        action="store_true",
        help="replace/auto 替换活动库时必须显式提供。",
    )
    parser.add_argument(
        "--confirm-services-stopped",
        action="store_true",
        help="确认 web/scheduler 容器均已停止；replace/auto 替换活动库时必须显式提供。",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    payload = build_rank_history_archive_vacuum_runner_payload(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        project_root=settings.project_root,
        output_dir=args.output_dir,
        mode=args.mode,
        candidate_path=args.candidate_path or None,
        candidate_dir=args.candidate_dir,
        rollback_path=args.rollback_path or None,
        rollback_dir=args.rollback_dir,
        rollback_prefix=args.rollback_prefix,
        timestamp=(str(args.timestamp or "").strip() or None),
        min_free_space_multiplier=args.min_free_space_multiplier,
        confirm_replace=bool(args.confirm_replace),
        confirm_services_stopped=bool(args.confirm_services_stopped),
    )
    json_path, md_path = write_rank_history_archive_vacuum_runner_artifacts(
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
        "review_items": payload.get("review_items") or [],
        "safety": payload.get("safety") or {},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
