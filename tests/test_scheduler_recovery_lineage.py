"""Task recovery diagnostics may not borrow a shared workflow observation."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.scheduler_diagnostics import build_recent_scheduler_failure_recovery_summary


@pytest.mark.parametrize("task_bound", [False, True])
def test_recovery_audit_requires_task_bound_run(tmp_path: Path, task_bound: bool) -> None:
    reports = tmp_path / "reports"
    directory = reports / ".audit/scheduler_task_runs/20261028"
    directory.mkdir(parents=True)
    for index, status in enumerate(("failed", "success")):
        started = f"2026-10-28T0{7 + index}:30:00"
        finished = f"2026-10-28T0{7 + index}:31:00"
        result = {"status": status, "started_at": started, "finished_at": finished,
                  "screening_triggered": True}
        if task_bound:
            result["screening_run_id"] = f"bound-{index}"
        audit = {"audit_version": 2, "generated_at": finished,
                 "task": {"task_name": "scheduled_daily_bond_refresh", "task_key": "daily_refresh",
                          "status": status, "started_at": started, "finished_at": finished},
                 "task_result": result,
                 "workflow_summary": {"run_id": "unrelated-manual-run",
                                      "observed_run_id": "unrelated-manual-run"}}
        (directory / f"{index}.json").write_text(json.dumps(audit))
    summary = build_recent_scheduler_failure_recovery_summary(
        SimpleNamespace(project_root=tmp_path, reports_dir=reports),
        now=datetime(2026, 10, 28, 10, 0),
    )
    recovered = summary["task_map"]["daily_refresh"]["latest_recovered_sequence"]
    assert recovered["recovery_screening_run_id"] == ("bound-1" if task_bound else None)
    assert recovered["failure_workflow_run_ids"] == (["bound-0"] if task_bound else [])
    assert "unrelated-manual-run" not in json.dumps(recovered)
