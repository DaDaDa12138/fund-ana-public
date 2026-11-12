#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.risk_coverage_gap_audit import load_latest_risk_coverage_gap_audit_payload  # noqa: E402
from backend.risk_lag_audit import load_latest_risk_lag_audit_payload  # noqa: E402
from backend.risk_residual_warnings import build_risk_residual_warning_summary  # noqa: E402
from backend.risk_sync_audit import load_latest_risk_sync_audit_payload  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    load_json as _load_json,
    resolve_run_payload as _resolve_run_payload,
)


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def main() -> int:
    parser = argparse.ArgumentParser(description="汇总风险滞后、风险同步与覆盖缺口后的残余告警。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    latest_canonical_payload = _load_json(settings.latest_report_file)
    latest_canonical_run_id = str(latest_canonical_payload.get("run_id") or "").strip()
    try:
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

    audit_dir = settings.reports_dir / "audit" / "risk_residual_warnings"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "risk_residual_warnings",
        run_id=run_id,
    )

    if run_id != latest_canonical_run_id:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id,
            "latest_canonical_run_id": latest_canonical_run_id,
            "available": False,
            "status": "latest_audit_inputs_only",
            "status_label": "仅支持 latest 审计输入",
            "detail": "risk_residual_warnings 当前依赖 latest 风险滞后/同步/覆盖缺口审计产物，尚未保留 run-local 历史快照，不能对非 latest run 做精确汇总。",
            "replay_precision": replay_precision,
            "replay_precision_status": replay_precision.get("status"),
            "replay_precision_status_label": replay_precision.get("status_label"),
            "auxiliary_scope": "cross_run_latest_audit_only",
            "auxiliary_scope_label": "历史 run + latest 风险审计",
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
                    "auxiliary_scope": payload["auxiliary_scope"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    risk_lag_artifact_path, risk_lag_payload = load_latest_risk_lag_audit_payload(settings)
    risk_sync_artifact_path, risk_sync_payload = load_latest_risk_sync_audit_payload(settings)
    risk_coverage_gap_artifact_path, risk_coverage_gap_payload = (
        load_latest_risk_coverage_gap_audit_payload(settings)
    )

    summary = build_risk_residual_warning_summary(
        risk_lag_payload or {},
        risk_sync_payload or {},
        risk_coverage_gap_payload or {},
    )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_canonical_run_id": latest_canonical_run_id,
        "risk_lag_artifact_path": (
            str(risk_lag_artifact_path.relative_to(settings.project_root))
            if risk_lag_artifact_path and risk_lag_artifact_path.exists()
            else None
        ),
        "risk_sync_artifact_path": (
            str(risk_sync_artifact_path.relative_to(settings.project_root))
            if risk_sync_artifact_path and risk_sync_artifact_path.exists()
            else None
        ),
        "risk_coverage_gap_artifact_path": (
            str(risk_coverage_gap_artifact_path.relative_to(settings.project_root))
            if risk_coverage_gap_artifact_path and risk_coverage_gap_artifact_path.exists()
            else None
        ),
        "replay_precision": replay_precision,
        "replay_precision_status": replay_precision.get("status"),
        "replay_precision_status_label": replay_precision.get("status_label"),
        "auxiliary_scope": "same_run_latest_audit_only",
        "auxiliary_scope_label": "latest run + latest 风险审计",
        "risk_residual_warning_summary": summary,
    }

    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    judgement = summary.get("judgement") or {}
    print(
        json.dumps(
            {
                "passed": bool(summary.get("available")),
                "run_id": run_id,
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(project_root)),
                "replay_precision_status": payload.get("replay_precision_status"),
                "replay_precision_status_label": payload.get("replay_precision_status_label"),
                "auxiliary_scope": payload.get("auxiliary_scope"),
                "status": summary.get("status"),
                "blocking_codes": summary.get("blocking_codes"),
                "warning_codes": summary.get("warning_codes"),
                "recommended_action": judgement.get("recommended_action"),
                "direct_threshold_relaxation_recommended_now": judgement.get(
                    "direct_threshold_relaxation_recommended_now"
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if summary.get("available") else 1


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
