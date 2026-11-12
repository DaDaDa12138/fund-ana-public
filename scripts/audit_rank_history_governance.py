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
from utils.rank_history_governance_audit import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    build_rank_history_governance_audit_payload,
    write_rank_history_governance_audit_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="只读审计 fund_rank_history 热库规模、冷数据候选和归档前阻断项。"
    )
    parser.add_argument("--config", default="", help="项目配置文件路径，默认 config/project.yaml。")
    parser.add_argument("--db-path", default="", help="SQLite 主库路径，默认读取项目配置。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="审计产物输出目录。")
    parser.add_argument("--timestamp", default="", help="指定产物时间戳，默认当前时间。")
    args = parser.parse_args()

    settings = load_settings(Path(args.config) if args.config else None)
    db_path = Path(args.db_path) if args.db_path else settings.database_path
    payload = build_rank_history_governance_audit_payload(
        db_path=db_path,
        project_root=settings.project_root,
    )
    json_path, md_path = write_rank_history_governance_audit_artifacts(
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
