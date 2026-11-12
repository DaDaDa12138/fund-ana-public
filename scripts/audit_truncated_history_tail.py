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

from backend.config import load_settings
from backend.truncated_history_tail_audit import build_truncated_history_tail_audit
from scripts.audit_runtime_utils import resolve_run_payload as _resolve_run_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="审计 truncated_history 尾部样本是否受上游历史限制")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    parser.add_argument("--limit", type=int, default=10, help="审计样本数")
    parser.add_argument("--codes", type=str, default="", help="逗号分隔的基金代码，非空时只审计这些基金")
    parser.add_argument("--page-size", type=int, default=100, help="分页大小")
    args = parser.parse_args()

    target_codes = [item.strip() for item in str(args.codes or "").split(",") if item.strip()]
    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    try:
        latest_run_id, _latest_payload, _artifacts, _latest_replay_precision = _resolve_run_payload(
            settings,
            project_root=project_root,
        )
        run_id, _payload, _artifacts, replay_precision = _resolve_run_payload(
            settings,
            run_id=args.run_id,
            project_root=project_root,
        )
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": str(exc),
                    "latest_report_file": str(settings.latest_report_file),
                    "requested_run_id": str(args.run_id or "").strip(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    payload = build_truncated_history_tail_audit(
        settings,
        limit=max(0, int(args.limit or 0)),
        target_codes=target_codes or None,
        page_size=max(1, int(args.page_size or 100)),
        log=lambda message: print(f"[tail-audit] {message}"),
    )
    payload["run_id"] = run_id
    payload["latest_canonical_run_id"] = latest_run_id
    payload["replay_precision"] = replay_precision
    payload["replay_precision_status"] = str(replay_precision.get("status") or "").strip()
    payload["replay_precision_status_label"] = str(replay_precision.get("status_label") or "").strip()
    payload["auxiliary_scope"] = (
        "same_run_current_upstream_probe"
        if run_id == latest_run_id
        else "cross_run_current_upstream_probe"
    )
    payload["auxiliary_scope_label"] = (
        "latest + 当前上游探针"
        if run_id == latest_run_id
        else "历史 run + 当前上游探针"
    )
    payload["scope_detail"] = (
        "当前审计会实时探测上游最老净值页，适合 latest 同期排查。"
        if run_id == latest_run_id
        else "当前审计会实时探测上游最老净值页；即使指定历史 run，也只能做跨期近似诊断，不能视为历史精确复盘。"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
