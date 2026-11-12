#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from core.utils.core_csv_storage import artifact_is_file, read_csv_artifact
from backend.report_builder import _build_score_cluster_explainability_audit  # noqa: E402
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
    parser = argparse.ArgumentParser(description="审计得分簇解释性。")
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

    stage_b_reason_csv, ledger_path = _resolve_artifact_project_path(
        artifacts,
        "stage_b_reason_csv",
        project_root=project_root,
    )
    if not stage_b_reason_csv:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "stage_b_reason_csv_missing",
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    if ledger_path is None or not artifact_is_file(ledger_path):
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "stage_b_reason_csv_not_found",
                    "stage_b_reason_csv": stage_b_reason_csv,
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    ledger_df = read_csv_artifact(ledger_path)
    audit = _build_score_cluster_explainability_audit(ledger_df)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_report_file": str(settings.latest_report_file.relative_to(settings.project_root)),
        "source_report_run_id": _clean_text(payload.get("run_id")),
        "stage_b_reason_csv": str(ledger_path.relative_to(settings.project_root)),
        "replay_precision": replay_precision,
        "score_cluster_explainability": audit,
    }

    audit_dir = settings.reports_dir / "audit" / "score_cluster_explainability"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "score_cluster_explainability",
        run_id=run_id,
    )
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    judgement = audit.get("judgement") or {}
    print(
        json.dumps(
            {
                "passed": bool(audit.get("available")),
                "run_id": run_id,
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(settings.project_root)),
                "replay_precision_status": replay_precision.get("status"),
                "replay_precision_status_label": replay_precision.get("status_label"),
                "score_cluster_hit_total": audit.get("score_cluster_hit_total"),
                "score_cluster_hit_share_pct": audit.get("score_cluster_hit_share_pct"),
                "suspected_redundancy_present": judgement.get("suspected_redundancy_present"),
                "direct_threshold_relaxation_recommended_now": judgement.get(
                    "direct_threshold_relaxation_recommended_now"
                ),
                "recommended_action": judgement.get("recommended_action"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if audit.get("available") else 1


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
