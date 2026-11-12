"""Weekly outcomes against injected workflow failures, without fund data or APIs."""
from dataclasses import fields
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend import scheduler as scheduling
from backend.config import Settings
from backend.scheduler_diagnostics import summarize_scheduler_run_for_api


@pytest.fixture
def weekly_scheduler(tmp_path, monkeypatch):
    settings = SimpleNamespace(
        project_root=tmp_path,
        reports_dir=tmp_path / "reports",
        database_path=tmp_path / "unused.sqlite",
        publication_registry_file=tmp_path / "publication_registry.json",
    )
    settings.reports_dir.mkdir()
    for field in fields(Settings):
        if field.name.startswith("scheduler_weekly_") and field.name.endswith("_enabled"):
            setattr(settings, field.name, False)
    scheduler = scheduling.ProjectScheduler.__new__(scheduling.ProjectScheduler)
    scheduler.settings = settings
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None
    scheduler._build_empty_detail_payload_audit = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._build_data_quality_audits_compat = lambda: {}
    scheduler._refresh_database_freshness_summary_sidecar = lambda: {}
    scheduler._build_maintenance_data_change_summary = lambda **kwargs: {"changed": True}
    scheduler._rescreen_report_after_maintenance = Mock(return_value={"run_id": "fixture_published"})
    scheduler._workflow_busy_skip_reason = Mock(return_value="workflow_busy_locked")
    scheduler._attempt_refresh_risk_free_rate = lambda: {
        "succeeded": True,
        "report": {"applied": True, "current_value": 0.02, "post_value": 0.019},
    }
    scheduler._build_risk_free_rate_drift_audit = lambda: {"alert_triggered": False}
    # A known nonblocking collection gap must remain visible, but cannot hide
    # a failed mandatory risk recalculation later in this same maintenance run.
    health = scheduling.ProjectScheduler._classify_log_severity([])
    health.update(
        degraded=True,
        degraded_stage="collect-period-returns",
        reason="period_return_unavailable_without_returns",
        period_return_collection={"status": "degraded"},
    )
    scheduler._classify_log_severity = lambda logs: health
    import scripts.cleanup_disk_usage as disk_cleanup

    monkeypatch.setattr(disk_cleanup, "run_cleanup", lambda **kwargs: SimpleNamespace(to_dict=lambda: {}))
    return scheduler


@pytest.mark.parametrize(
    "outcome,recalculation_status,warning,run_id,skip_reason",
    [
        (RuntimeError("workflow_subprocess_failed exit=1"), "failed", "risk_parameter_rescreen_failed", "fixture_published", None),
        (ValueError("fixture calculation failure"), "failed", "risk_parameter_rescreen_failed", "fixture_published", None),
        (RuntimeError("workflow_subprocess_busy"), "skipped", "risk_parameter_rescreen_incomplete", "fixture_published", "workflow_busy_locked"),
        ({"status": "skipped", "skip_reason": "workflow_executor_unavailable"}, "skipped", "risk_parameter_rescreen_incomplete", "fixture_published", "workflow_executor_unavailable"),
        ({"run_id": "fixture_recalculated"}, "triggered", "period_return_unavailable_without_returns", "fixture_recalculated", None),
    ],
)
def test_weekly_preserves_publication_and_exposes_required_recalculation_failure(
    weekly_scheduler, monkeypatch, outcome, recalculation_status, warning, run_id, skip_reason,
):
    scheduler = weekly_scheduler
    scheduler._rescreen_report_after_risk_parameter_change = (
        Mock(side_effect=outcome) if isinstance(outcome, Exception) else Mock(return_value=outcome)
    )
    result = scheduler._execute_weekly_maintenance(collection_enabled=False, bond_universe_sync_enabled=False)

    audit = result["risk_free_rate_audit"]
    assert audit["risk_metrics_full_recalculation_required"] is True
    assert audit["risk_metrics_full_recalculation_status"] == recalculation_status
    assert result["status"] == "degraded"
    assert result["warning_reason"] == warning
    assert result["screening_run_id"] == run_id
    assert result["report_refresh_run_id"] == run_id
    assert result["maintenance_rescreen_skipped_reason"] == skip_reason
    assert result["collection_warning_reason"] == "period_return_unavailable_without_returns"
    assert result["collection_degraded_stage"] == "collect-period-returns"
    if recalculation_status == "triggered":
        assert result["degraded_stage"] == "collect-period-returns"
        assert result["screening_reason"] == "risk_free_rate_changed_full_risk_rescreen"
    else:
        assert result["degraded_stage"] == "risk-parameter-rescreen"
        assert result["screening_reason"] == "maintenance_changed_rescreen"
        api_summary = summarize_scheduler_run_for_api(result, project_root=scheduler.settings.project_root)
        assert api_summary["degraded_stage"] == "risk-parameter-rescreen"
        assert api_summary["degradation_summary"]["category"] == "workflow_refresh_failed"
    if isinstance(outcome, RuntimeError) and str(outcome) != "workflow_subprocess_busy":
        scheduler._workflow_busy_skip_reason.assert_not_called()
        assert result["report_refresh_skipped_reason"] is None
        assert audit["risk_metrics_full_recalculation_error"] == str(outcome)

    scheduler.state_store = SimpleNamespace(stage_task_result=Mock(), set_jobs=Mock(), finish_task=Mock())
    scheduler.task_recorder = SimpleNamespace(finish_task=Mock(
        side_effect=lambda task_info, **kwargs: kwargs["completion_result_factory"](datetime.now())))
    scheduler._serialize_jobs = lambda: []
    scheduler._refresh_runtime_status_summary = lambda: {}
    scheduler._write_task_run_audit = lambda **kwargs: {"artifact_path": "/tmp/fixture_audit.json"}
    monkeypatch.setattr(scheduling, "seal_task_run_audit", lambda *args: None)
    monkeypatch.setattr(scheduling, "load_publication_registry", lambda settings: {
        "current_published_run_id": run_id,
        "automation_layers": {
            "latest_successful_is_currently_published": True,
            "latest_successful_run": {"run_id": run_id, "publishable": True},
        },
    })
    finalized = scheduler._finalize_task(
        task_info={"id": 1, "started_at": result["started_at"]},
        task_name=result["task_name"],
        result=result,
    )
    assert finalized["workflow_run_id"] == run_id
    assert finalized["publication_result"]["status"] == "published"
    assert finalized["status"] == "degraded"
    assert finalized["warning_reason"] == warning
    assert finalized["status_detail"] == (
        "success_and_published" if recalculation_status == "triggered" else "degraded_and_published"
    )
    assert scheduler.state_store.finish_task.call_args.args[1]["status_detail"] == finalized["status_detail"]


@pytest.mark.parametrize("task_status", ["success", "degraded"])
def test_ordinary_published_status_detail_remains_compatible(task_status):
    assert scheduling._derive_status_detail_for_task_result(
        task_status, {"status": "published"}, task_result={},
    ) == "success_and_published"


def test_required_recalculation_skipped_without_exception_is_degraded(weekly_scheduler):
    scheduler = weekly_scheduler
    health = scheduling.ProjectScheduler._classify_log_severity([])
    scheduler._classify_log_severity = lambda logs: health
    scheduler._rescreen_report_after_risk_parameter_change = Mock(return_value={
        "status": "skipped", "skip_reason": "workflow_executor_unavailable",
    })
    result = scheduler._execute_weekly_maintenance(collection_enabled=False, bond_universe_sync_enabled=False)
    assert result["status"] == "degraded"
    assert result["degraded"] is True
    assert result["degraded_stage"] == "risk-parameter-rescreen"
    assert result["warning_reason"] == "risk_parameter_rescreen_incomplete"
