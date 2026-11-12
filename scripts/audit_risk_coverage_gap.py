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

from backend.config import load_settings  # noqa: E402
from backend.risk_coverage_gap_audit import build_risk_coverage_gap_audit  # noqa: E402
from scripts.audit_runtime_utils import resolve_run_payload as _resolve_run_payload  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="审计活跃债基 1 年风险覆盖缺口的真实原因分层")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    parser.add_argument("--sample-limit", type=int, default=8, help="每类输出样本上限")
    args = parser.parse_args()

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

    payload = build_risk_coverage_gap_audit(
        settings,
        sample_limit=max(1, int(args.sample_limit or 1)),
    )
    payload["run_id"] = run_id
    payload["latest_canonical_run_id"] = latest_run_id
    payload["replay_precision"] = replay_precision
    payload["replay_precision_status"] = str(replay_precision.get("status") or "").strip()
    payload["replay_precision_status_label"] = str(replay_precision.get("status_label") or "").strip()
    payload["auxiliary_scope"] = (
        "same_run_current_risk_state" if run_id == latest_run_id else "cross_run_current_risk_state"
    )
    payload["auxiliary_scope_label"] = (
        "latest + 当前风险状态" if run_id == latest_run_id else "历史 run + 当前风险状态"
    )
    payload["scope_detail"] = (
        "当前审计基于当前数据库、当前有效净值快照与当前风险覆盖状态生成，适合 latest 同期排查。"
        if run_id == latest_run_id
        else "当前审计基于当前数据库、当前有效净值快照与当前风险覆盖状态生成；即使指定历史 run，也只能做跨期近似诊断，不能视为历史精确复盘。"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
