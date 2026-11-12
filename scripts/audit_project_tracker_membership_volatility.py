#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from scripts.audit_runtime_utils import (
    build_audit_artifact_path as _build_audit_artifact_path,
    resolve_run_payload as _resolve_run_payload,
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="审计项目主榜成员波动是否偏离滚动基线。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    latest_canonical_payload = _load_json(settings.latest_report_file)
    latest_canonical_run_id = str(latest_canonical_payload.get("run_id") or "").strip()
    run_id, latest_payload, _artifacts, replay_precision = _resolve_run_payload(
        settings,
        run_id=args.run_id,
        project_root=project_root,
    )
    tracker_meta_path = settings.reports_dir / "最终优质基金累积跟踪_report_meta.json"
    tracker_meta_payload = _load_json(tracker_meta_path)

    audit_dir = settings.reports_dir / "audit" / "project_tracker_membership_volatility"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "project_tracker_membership_volatility",
        run_id=run_id,
    )

    if run_id != latest_canonical_run_id:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id,
            "latest_report_file": str(settings.latest_report_file.relative_to(project_root)),
            "tracker_meta_file": str(tracker_meta_path.relative_to(project_root)),
            "available": False,
            "status": "latest_tracker_meta_only",
            "status_label": "仅支持 latest",
            "detail": "成员波动审计当前依赖全局 tracker meta 文件，尚未具备 run-local 历史快照，不能对非 latest run 做精确比较。",
            "replay_precision": replay_precision,
            "replay_precision_status": replay_precision.get("status"),
            "replay_precision_status_label": replay_precision.get("status_label"),
        }
        artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "passed": True,
                    "available": False,
                    "run_id": run_id,
                    "artifact_path": str(artifact_path),
                    "artifact_project_path": str(artifact_path.relative_to(project_root)),
                    "status": payload["status"],
                    "status_label": payload["status_label"],
                    "replay_precision_status": payload["replay_precision_status"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    tracker_summary = (
        ((latest_payload.get("project_publish_summary") or {}).get("project_tracker_summary")) or {}
    )
    meta_summary = (tracker_meta_payload.get("accumulation_summary") or {})

    latest_membership = tracker_summary.get("latest_membership_change_summary") or {}
    rolling = latest_membership.get("rolling_change_stability_summary") or {}
    meta_rolling = ((meta_summary.get("latest_membership_change_summary") or {}).get("rolling_change_stability_summary")) or {}

    aligned = rolling == meta_rolling
    alert_codes = list(rolling.get("rolling_alert_codes") or [])
    assessment = "stable_within_recent_baseline"
    if alert_codes:
        assessment = "elevated_membership_volatility_observed"
    elif not bool(rolling.get("available")):
        assessment = "insufficient_snapshot_history"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_report_file": str(settings.latest_report_file.relative_to(project_root)),
        "tracker_meta_file": str(tracker_meta_path.relative_to(project_root)),
        "available": True,
        "replay_precision": replay_precision,
        "replay_precision_status": replay_precision.get("status"),
        "replay_precision_status_label": replay_precision.get("status_label"),
        "aligned_between_latest_and_meta": aligned,
        "assessment": assessment,
        "latest_membership_change_summary": {
            "current_count": latest_membership.get("current_count"),
            "previous_count": latest_membership.get("previous_count"),
            "new_entry_count": latest_membership.get("new_entry_count"),
            "returned_count": latest_membership.get("returned_count"),
            "dropped_count": latest_membership.get("dropped_count"),
            "current_change_rate_pct": latest_membership.get("current_change_rate_pct"),
            "dropped_rate_pct": latest_membership.get("dropped_rate_pct"),
        },
        "rolling_change_stability_summary": {
            "available": rolling.get("available"),
            "window_size": rolling.get("window_size"),
            "comparison_count": rolling.get("comparison_count"),
            "comparison_periods": rolling.get("comparison_periods") or [],
            "mean_current_change_rate_pct": rolling.get("mean_current_change_rate_pct"),
            "median_current_change_rate_pct": rolling.get("median_current_change_rate_pct"),
            "max_current_change_rate_pct": rolling.get("max_current_change_rate_pct"),
            "mean_dropped_rate_pct": rolling.get("mean_dropped_rate_pct"),
            "median_dropped_rate_pct": rolling.get("median_dropped_rate_pct"),
            "max_dropped_rate_pct": rolling.get("max_dropped_rate_pct"),
            "current_vs_rolling_change_gap_pct": rolling.get("current_vs_rolling_change_gap_pct"),
            "current_vs_rolling_drop_gap_pct": rolling.get("current_vs_rolling_drop_gap_pct"),
            "rolling_alert_codes": alert_codes,
        },
        "judgement": {
            "strategy_change_recommended_now": bool(alert_codes),
            "reason": (
                "连续滚动基线已显著偏离，建议进入风格内稳健阈值复审"
                if alert_codes
                else "当前正式主榜成员波动未高于近5期滚动基线，不建议直接修改主链筛选策略"
            ),
        },
    }

    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "passed": aligned,
                "assessment": assessment,
                "run_id": run_id,
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(project_root)),
                "replay_precision_status": payload["replay_precision_status"],
                "rolling_alert_codes": alert_codes,
                "strategy_change_recommended_now": payload["judgement"]["strategy_change_recommended_now"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if aligned else 1


if __name__ == "__main__":
    raise SystemExit(main())
