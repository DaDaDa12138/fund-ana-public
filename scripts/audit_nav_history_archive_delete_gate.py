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
from utils.nav_history_archive_apply_runner import DEFAULT_OUTPUT_DIR  # noqa: E402
from utils.nav_history_archive_delete_gate_audit import (  # noqa: E402
    build_nav_history_archive_delete_gate_audit_payload,
    write_nav_history_archive_delete_gate_audit_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "审计 NAV 热库冷数据删除门禁；只读验证冷热库计数、样本和 digest，"
            "不执行 hot delete 或 VACUUM。"
        )
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="NAV archive audit 输出目录。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    parser.add_argument(
        "--source-apply-runner-artifact",
        default="",
        help="指定 apply runner JSON；默认读取 output-dir 下最新 apply runner 产物。",
    )
    parser.add_argument("--sample-limit", type=int, default=20, help="冷热库头尾样本行数。")
    parser.add_argument(
        "--skip-full-digest",
        action="store_true",
        help="跳过全量 digest；会使门禁保持 review_required。",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    payload = build_nav_history_archive_delete_gate_audit_payload(
        db_path=Path(args.db_path) if args.db_path else settings.database_path,
        project_root=settings.project_root,
        source_apply_runner_artifact=args.source_apply_runner_artifact or None,
        output_dir=args.output_dir,
        sample_limit=args.sample_limit,
        compute_full_digest=not args.skip_full_digest,
    )
    json_path, md_path = write_nav_history_archive_delete_gate_audit_artifacts(
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
