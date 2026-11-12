#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.risk_adjusted_metric_stack import (  # noqa: E402
    load_risk_adjusted_metric_stack_review_from_workbook,
)
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    clean_text as _clean_text,
    resolve_artifact_project_path as _resolve_artifact_project_path,
    resolve_run_payload as _resolve_run_payload,
)


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def main() -> int:
    parser = argparse.ArgumentParser(description="审计风险调整收益指标栈。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    try:
        run_id, payload, artifacts, replay_precision = _resolve_run_payload(
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

    screening_audit_excel, workbook_path = _resolve_artifact_project_path(
        artifacts,
        "screening_audit_excel",
        project_root=project_root,
    )
    if not screening_audit_excel:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "screening_audit_excel_missing",
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    if workbook_path is None or not workbook_path.exists():
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "screening_audit_excel_not_found",
                    "screening_audit_excel": screening_audit_excel,
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    project_publish_summary = payload.get("project_publish_summary") or {}
    audit = load_risk_adjusted_metric_stack_review_from_workbook(
        workbook_path,
        positive_ratio_semantics=project_publish_summary.get("positive_ratio_semantics") or {},
        low_vol_positive_ratio_shadow_watch=project_publish_summary.get(
            "low_vol_positive_ratio_shadow_watch"
        )
        or {},
    )
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_report_file": str(settings.latest_report_file.relative_to(settings.project_root)),
        "source_report_run_id": _clean_text(payload.get("run_id")),
        "screening_audit_excel": str(workbook_path.relative_to(settings.project_root)),
        "replay_precision": replay_precision,
        "risk_adjusted_metric_stack_review": audit,
    }

    audit_dir = settings.reports_dir / "audit" / "risk_adjusted_metric_stack"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "risk_adjusted_metric_stack",
        run_id=run_id,
    )
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    judgement = audit.get("judgement") or {}
    dominant_pair = judgement.get("dominant_intra_cluster_pair") or {}
    positive_ratio_governance = audit.get("positive_ratio_governance") or {}
    print(
        json.dumps(
            {
                "passed": bool(audit.get("available")),
                "run_id": run_id,
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(settings.project_root)),
                "replay_precision_status": replay_precision.get("status"),
                "replay_precision_status_label": replay_precision.get("status_label"),
                "status": audit.get("status"),
                "focus_scope_total": audit.get("focus_scope_total"),
                "high_overlap_pair_total": judgement.get("high_overlap_pair_total"),
                "dominant_pair": "+".join(
                    [
                        str(item)
                        for item in (
                            dominant_pair.get("factor_a_label"),
                            dominant_pair.get("factor_b_label"),
                        )
                        if _clean_text(item)
                    ]
                ),
                "dominant_pair_overlap_ratio": dominant_pair.get("overlap_vs_smaller_ratio"),
                "sharpe_review_priority": judgement.get("sharpe_review_priority"),
                "positive_ratio_strategy_materiality_status": positive_ratio_governance.get(
                    "strategy_materiality_status"
                ),
                "recommended_action": judgement.get("recommended_action"),
                "direct_threshold_relaxation_recommended_now": judgement.get(
                    "direct_threshold_relaxation_recommended_now"
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if audit.get("available") else 1


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
