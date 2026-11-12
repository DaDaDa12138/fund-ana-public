"""Task audit lineage must not inherit the shared workflow status snapshot."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from backend import scheduler as scheduling
from backend.scheduler import ProjectScheduler
from backend.scheduler_diagnostics import (
    build_recent_scheduler_failure_recovery_summary,
    build_task_run_artifacts_summary,
)
from backend.task_audit_storage import read_task_audit


def _minimal_scheduler(tmp_path: Path) -> ProjectScheduler:
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        project_root=tmp_path,
        reports_dir=tmp_path / "reports",
        publication_registry_file=tmp_path / "publication_registry.json",
    )
    scheduler.workflow_manager = SimpleNamespace(
        status_summary=lambda: {
            "status": "completed",
            "run_id": "old-shared-run",
            "started_at": "2026-10-21T23:00:00",
            "finished_at": "2026-10-21T23:05:00",
        }
    )
    return scheduler


def test_task_audit_keeps_shared_run_as_observation_but_returns_no_task_lineage(tmp_path, monkeypatch):
    scheduler = _minimal_scheduler(tmp_path)
    scheduler._build_scheduler_status_payload_for_task_audit = lambda *args, **kwargs: {
        "task_closures": {},
        "daily_run": {},
    }
    monkeypatch.setattr(scheduling, "load_scheduler_status", lambda *args, **kwargs: {})
    monkeypatch.setattr(scheduling, "load_publication_registry", lambda settings: {})

    audit_ref = scheduler._write_task_run_audit(
        task_info={"id": 7, "started_at": "2026-10-22T07:30:00"},
        task_name="scheduled_daily_bond_refresh",
        result={"status": "failed", "started_at": "2026-10-22T07:30:00"},
    )

    audit = read_task_audit(audit_ref["artifact_path"])
    assert audit_ref["workflow_run_id"] is None
    assert audit["workflow_summary"]["run_id"] == "old-shared-run"
    assert audit["workflow_summary"]["observed_run_id"] == "old-shared-run"
    assert audit["task_result"].get("workflow_run_id") is None


def test_finalize_trigger_without_task_run_id_does_not_promote_shared_audit_id(tmp_path, monkeypatch):
    scheduler = _minimal_scheduler(tmp_path)
    scheduler.state_store = SimpleNamespace(
        finish_task=Mock(),
        stage_task_result=Mock(),
        set_jobs=Mock(),
    )
    scheduler.task_recorder = SimpleNamespace(
        finish_task=Mock(
            side_effect=lambda task_info, **kwargs: kwargs["completion_result_factory"](datetime.now())
        )
    )
    scheduler._serialize_jobs = lambda: []
    scheduler._refresh_runtime_status_summary = lambda: None
    scheduler._write_task_run_audit = lambda **kwargs: {
        "artifact_path": str(tmp_path / "task-audit.json"),
        "workflow_run_id": "old-shared-run",
    }
    monkeypatch.setattr(scheduling, "seal_task_run_audit", lambda *args: None)
    monkeypatch.setattr(
        scheduling,
        "load_publication_registry",
        lambda settings: {
            "current_published_run_id": "old-shared-run",
            "automation_layers": {
                "latest_successful_run": {"run_id": "old-shared-run", "published": True}
            },
        },
    )

    result = ProjectScheduler._finalize_task(
        scheduler,
        task_info={"id": 8, "started_at": "2026-10-22T07:30:00"},
        task_name="scheduled_daily_bond_refresh",
        result={
            "status": "success",
            "screening_triggered": True,
            "started_at": "2026-10-22T07:30:00",
            "finished_at": "2026-10-22T07:31:00",
        },
    )

    assert result.get("workflow_run_id") is None
    assert "publication_result" not in result


def test_finalize_preserves_explicit_task_run_id_for_publication_lookup(tmp_path, monkeypatch):
    scheduler = _minimal_scheduler(tmp_path)
    scheduler.state_store = SimpleNamespace(
        finish_task=Mock(),
        stage_task_result=Mock(),
        set_jobs=Mock(),
    )
    scheduler.task_recorder = SimpleNamespace(
        finish_task=Mock(
            side_effect=lambda task_info, **kwargs: kwargs["completion_result_factory"](datetime.now())
        )
    )
    scheduler._serialize_jobs = lambda: []
    scheduler._refresh_runtime_status_summary = lambda: None
    scheduler._write_task_run_audit = lambda **kwargs: {
        "artifact_path": str(tmp_path / "task-audit.json"),
        "workflow_run_id": None,
    }
    monkeypatch.setattr(scheduling, "seal_task_run_audit", lambda *args: None)
    monkeypatch.setattr(
        scheduling,
        "load_publication_registry",
        lambda settings: {
            "current_published_run_id": "current-screening-run",
            "automation_layers": {
                "latest_successful_run": {
                    "run_id": "current-screening-run",
                    "published": True,
                    "publishable": True,
                }
            },
        },
    )

    result = ProjectScheduler._finalize_task(
        scheduler,
        task_info={"id": 9, "started_at": "2026-10-22T07:30:00"},
        task_name="scheduled_daily_bond_refresh",
        result={
            "status": "success",
            "screening_triggered": True,
            "screening_run_id": "current-screening-run",
            "started_at": "2026-10-22T07:30:00",
            "finished_at": "2026-10-22T07:31:00",
        },
    )

    assert result["workflow_run_id"] == "current-screening-run"
    assert result["publication_result"]["status"] in {"published", "success"}


def test_diagnostics_does_not_use_stale_audit_workflow_run_id():
    summary = build_task_run_artifacts_summary(
        {
            "last_runs": {
                "scheduled_daily_bond_refresh": {
                    "status": "failed",
                    "task_run_audit": {
                        "artifact_path": "/tmp/task-audit.json",
                        "workflow_run_id": "old-shared-run",
                    },
                }
            }
        }
    )

    assert summary["task_map"]["daily_refresh"]["workflow_run_id"] is None


def test_failure_diagnostics_does_not_promote_shared_workflow_snapshot(tmp_path):
    reports_dir = tmp_path / "reports"
    audit_dir = reports_dir / ".audit" / "scheduler_task_runs" / "20261022"
    audit_dir.mkdir(parents=True)
    (audit_dir / "daily.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-10-22T08:00:00",
                "task": {
                    "task_name": "scheduled_daily_bond_refresh",
                    "task_key": "daily_refresh",
                    "started_at": "2026-10-22T07:30:00",
                    "finished_at": "2026-10-22T07:31:00",
                    "status": "failed",
                },
                "task_result": {
                    "status": "failed",
                    "screening_triggered": True,
                    "screening_run_id": None,
                },
                "workflow_summary": {
                    "status": "completed",
                    "run_id": "old-shared-run",
                    "started_at": "2026-10-21T23:00:00",
                    "finished_at": "2026-10-21T23:05:00",
                },
            }
        ),
        encoding="utf-8",
    )

    result = build_recent_scheduler_failure_recovery_summary(
        SimpleNamespace(
            project_root=tmp_path,
            reports_dir=reports_dir,
            database_path=tmp_path / "missing.db",
        ),
        now=datetime(2026, 10, 22, 12, 0, 0),
    )

    latest = result["task_map"]["daily_refresh"]["open_failure_sequence"]
    assert latest["failure_workflow_run_ids"] == []
    assert latest["latest_failure_workflow_run_id"] is None

