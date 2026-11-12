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

from backend.config import load_settings  # noqa: E402
from backend.plain_open_tail_follow_up import (  # noqa: E402
    _normalize_code,
    load_codes_from_latest_audit,
    run_plain_open_tail_follow_up,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="对普通开放型旧尾差样本执行定向净值追尾与风险刷新")
    parser.add_argument(
        "--category",
        default="plain_open_tail_follow_up",
        help="从最新 risk_coverage_gap_audit 中读取的尾差分类，默认 plain_open_tail_follow_up",
    )
    parser.add_argument(
        "--codes",
        default="",
        help="逗号分隔手动指定基金代码；提供后优先于最新审计产物",
    )
    parser.add_argument("--skip-risk-refresh", action="store_true", help="仅做定向净值追尾，不执行 changed-nav 风险刷新")
    args = parser.parse_args()

    settings = load_settings()
    if args.codes.strip():
        codes = []
        seen: set[str] = set()
        for raw in args.codes.split(","):
            code = _normalize_code(raw)
            if not code or code in seen:
                continue
            seen.add(code)
            codes.append(code)
    else:
        codes = load_codes_from_latest_audit(settings, str(args.category or "").strip())

    payload = run_plain_open_tail_follow_up(
        settings,
        category=str(args.category or "").strip(),
        codes=codes,
        skip_risk_refresh=args.skip_risk_refresh,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not payload.get("error_total") else 1


if __name__ == "__main__":
    raise SystemExit(main())
