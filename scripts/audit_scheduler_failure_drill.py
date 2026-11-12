#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.scheduler_failure_drill import (
    build_scheduler_failure_drill_payload,
    write_scheduler_failure_drill_report,
)
from scripts.audit_runtime_utils import resolve_run_payload as _resolve_run_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="合成调度失败演练审计。")
    parser.add_argument("--run-id", default="", help="指定 run_id；仅作为输出上下文元信息。")
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

    payload = build_scheduler_failure_drill_payload(settings)
    payload["run_id"] = run_id
    payload["latest_canonical_run_id"] = latest_run_id
    payload["replay_precision"] = replay_precision
    payload["replay_precision_status"] = str(replay_precision.get("status") or "").strip()
    payload["replay_precision_status_label"] = str(replay_precision.get("status_label") or "").strip()
    payload["auxiliary_scope"] = "synthetic_scheduler_drill"
    payload["auxiliary_scope_label"] = "合成调度演练"
    payload["scope_detail"] = "该脚本输出的是合成调度失败演练，不复盘指定 run 的历史调度现场；run_id 仅用于和当前发布上下文并列展示。"
    output_path = write_scheduler_failure_drill_report(
        payload,
        reports_dir=settings.reports_dir,
    )
    response = {
        "passed": payload.get("passed"),
        "run_id": run_id,
        "scenario": payload.get("scenario"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "summary": payload.get("summary") or {},
        "artifact_path": str(output_path),
        "artifact_project_path": str(output_path.relative_to(settings.project_root)),
    }
    print(json.dumps(response, ensure_ascii=False, indent=2))
    return 0 if payload.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
