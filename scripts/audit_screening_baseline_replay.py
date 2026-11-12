"""Verify the report selection baseline using real frozen inputs only."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))
from backend.config import load_settings
from backend.screening_baseline_replay import verify_frozen_baseline, write_baseline_verification
from scripts.audit_runtime_utils import resolve_run_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="从冻结输入核对完整报告筛选基线；不读取后续数据库重建历史。")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--report", type=Path, help="显式的真实报告 JSON，包括隔离诊断候选")
    args = parser.parse_args()
    settings = load_settings()
    if args.report:
        payload = json.loads(args.report.read_text())
    else:
        _, payload, _, _ = resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    summary, _ = verify_frozen_baseline(settings, payload)
    summary.setdefault("run_id", payload["run_id"])
    path = write_baseline_verification(settings, summary)
    print(json.dumps({"artifact": str(path), "status": summary["status"],
                      "baseline_verified": summary["baseline_verified"],
                      "failed_checks": [check for check in summary.get("checks", []) if not check["aligned"]]}, ensure_ascii=False), flush=True)
    return 0 if summary["baseline_verified"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
