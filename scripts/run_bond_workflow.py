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

sys.path.insert(0, str(PROJECT_ROOT))

from backend.workflow import WorkflowManager  # noqa: E402


parser = argparse.ArgumentParser(description="运行债券基金筛选工作流")
parser.add_argument("--reuse-existing-outputs", action="store_true", help="跳过核心筛选执行，直接使用已有产物生成过滤后报告")
parser.add_argument("--skip-collection", action="store_true", help="跳过数据采集步骤")
parser.add_argument("--skip-screening", action="store_true", help="跳过核心筛选步骤")
parser.add_argument("--collection-profile", default="", help="显式指定 collection_profile")
parser.add_argument("--force-calculate-risk", action="store_true", help="强制先执行风险指标重算")
parser.add_argument("--risk-refresh-changed-nav-only", action="store_true", help="风险指标重算仅针对净值变更样本")
parser.add_argument("--risk-refresh-target-fcodes-file", default="", help="changed-nav 风险重算额外目标基金代码文件")
parser.add_argument("--skip-risk-calculation", action="store_true", help="跳过风险指标重算")
parser.add_argument(
    "--skip-annual-return-gate-shadow-audit",
    action="store_true",
    help="跳过年度收益门禁 shadow audit 及其后的二次报告重建",
)
parser.add_argument("--compact-json", action="store_true", help="以单行 JSON 输出结果，供调度链复用")
parser.add_argument("--stream-collection-evidence", action="store_true", help="实时输出采集来源证据，报告 JSON 保留在末尾")
parser.add_argument("--collection-calculation-capture", action="append", type=json.loads, default=[],
                    help="关联此前采集计算的完整来源描述符；不替代本次工作流输入")
args = parser.parse_args()

manager = WorkflowManager(
    collection_event_callback=(lambda line: print(line, flush=True)) if args.stream_collection_evidence else None,
)
report = manager.run_sync(
    {
        "run_collection": not args.skip_collection,
        "run_screening": not args.skip_screening,
        "reuse_existing_outputs": args.reuse_existing_outputs,
        "collection_profile": args.collection_profile.strip() if args.collection_profile else None,
        "force_calculate_risk": bool(args.force_calculate_risk),
        "risk_refresh_changed_nav_only": bool(args.risk_refresh_changed_nav_only),
        "risk_refresh_target_fcodes_file": args.risk_refresh_target_fcodes_file.strip(),
        "skip_risk_calculation": bool(args.skip_risk_calculation),
        "skip_annual_return_gate_shadow_audit": bool(args.skip_annual_return_gate_shadow_audit),
        "collection_calculation_captures": args.collection_calculation_capture,
    }
)
if args.compact_json:
    print(json.dumps(report, ensure_ascii=False))
else:
    print(json.dumps(report, ensure_ascii=False, indent=2))
