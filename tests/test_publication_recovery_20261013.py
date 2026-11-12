"""Window boundaries and publication dependency recovery, in isolation."""
from datetime import date, datetime
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import gzip
import json

import pandas as pd
import pytest

from bond_fund_screening.core.filtering_engine import FilteringEngine
from bond_fund_screening.core.nav_observation_quality import assess_nav_observation_quality
from backend import scheduler as module
from backend.publication_dependencies import configured_publication_blocker
from backend.standalone_runner import run_project_pipeline


REAL_SAMPLES = json.loads(gzip.decompress(
    (Path(__file__).parent / "fixtures/nav_boundary_real_20261013.json.gz").read_bytes()))


@pytest.mark.parametrize("enabled", [False, True])
def test_public_scheduler_summary_and_detail_preserve_known_dependency(monkeypatch, enabled):
    import backend.app as web

    # Exercise the real compact builder and HTTP projection. Business inputs are
    # isolated; a missing blocker must never be interpreted as source health.
    for name in ("_build_scheduler_overview_summary", "_load_latest_report_cached",
                 "_load_latest_report_snapshot_cached", "_load_default_risk_lag_summary",
                 "_load_risk_sync_summary", "_load_risk_coverage_gap_summary",
                 "_load_current_final_selection_governance_queue_summary_cached",
                 "_scheduler_summary_common_file_cache_basis", "_build_automation_autonomy_summary"):
        monkeypatch.setattr(web, name, lambda *args, **kwargs: {})
    runtime_settings = replace(web.settings, autostock_enabled=enabled, publication_validation_provider="autostock")
    monkeypatch.setattr(web, "_runtime_scheduler_settings", lambda: runtime_settings)
    monkeypatch.setattr(web, "_resolve_lightweight_scheduler_runtime_digest_sources", lambda *args, **kwargs: ({}, {}))
    monkeypatch.setattr(web, "_load_scheduler_summary_fragment_cached", lambda name, basis, builder: (builder(), {}))
    monkeypatch.setattr(web.dashboard_payloads, "_build_nav_risk_governance_summary", lambda *args, **kwargs: {})
    payload = web._build_scheduler_status_summary_payload({"last_runs": {}})
    expected = configured_publication_blocker(SimpleNamespace(autostock_enabled=enabled))
    assert "publication_dependency" in payload and payload["publication_dependency"] == expected
    monkeypatch.setattr(web, "ensure_embedded_scheduler", lambda: None)
    monkeypatch.setattr(web, "_build_scheduler_status_summary_cached", lambda: payload)
    for endpoint in ("/api/scheduler/status", "/api/scheduler/status/detail"):
        response = web.app.test_client().get(endpoint)
        assert response.status_code == 200
        assert "publication_dependency" in response.json
        assert response.json["publication_dependency"] == expected


def test_dashboard_cached_overview_refreshes_dependency_after_configuration_changes(monkeypatch):
    import backend.app as web

    runtime = SimpleNamespace(autostock_enabled=False)
    monkeypatch.setattr(web, "_runtime_scheduler_settings", lambda: runtime)
    monkeypatch.setattr(web, "_load_scheduler_state_snapshot_cached", lambda: {})
    summary = web._build_scheduler_overview_api_summary({})
    compact = web.dashboard_payloads._reduce_scheduler_overview_for_overview(summary)
    expected = configured_publication_blocker(runtime)
    assert compact["publication_dependency"] == expected
    stale_cache = {"scheduler_overview": {"publication_dependency": None}}
    refreshed = web._refresh_dashboard_overview_runtime_fields(stale_cache)
    assert refreshed["scheduler_overview"]["publication_dependency"] == expected
    assert stale_cache["scheduler_overview"]["publication_dependency"] is None
    runtime.autostock_enabled = True
    assert web._refresh_dashboard_overview_runtime_fields(refreshed)["scheduler_overview"]["publication_dependency"] is None


@pytest.mark.parametrize("sample", REAL_SAMPLES, ids=lambda value: value["fcode"])
def test_real_failed_window_reaches_risk_calculation_with_same_nav_values(sample):
    frame = pd.DataFrame(sample["nav"])
    frame["nav_date"] = pd.to_datetime(frame["nav_date"])
    engine = FilteringEngine(period_years=sample["period_years"], data_loader=SimpleNamespace(),
                             enable_tracking=False, verbose=False)
    _, metrics, meta = engine._calculate_layer6_single_fund(
        sample["fcode"], frame, ["sharpe_ratio", "max_drawdown", "volatility"],
        window_end_date=date.fromisoformat(sample["window_end_date"]))
    assert sample["before"]["status"] == "NAV_DATA_GAP"
    assert meta["status"] == "OK" and metrics is not None
    assert meta["nav_count"] == len(frame) == 4
    assert meta["required_nav_points"] == sample["before"]["required_nav_points"] == 4
    assert meta["max_gap_threshold_days"] == sample["before"]["max_gap_threshold_days"] == 7
    assert meta["max_gap_days"] == 8 and meta["continuity_max_gap_days"] == 3
    assert metrics["volatility"] is not None
    selected, _ = engine._prepare_layer6_nav_window(frame, date.fromisoformat(sample["window_end_date"]))
    pd.testing.assert_frame_equal(selected, frame)


def quality(dates, start="2026-10-06", end="2026-10-13"):
    return assess_nav_observation_quality(
        dates, period_days=7, required_observation_points=4,
        max_gap_threshold_days=7, window_start=start, window_end=end,
        max_boundary_age_days=7,
    )


def test_pre_window_context_does_not_create_a_false_window_gap():
    result = quality(["2026-09-30", "2026-10-08", "2026-10-09", "2026-10-12"])
    assert result["max_gap_days"] == 8  # Preserve the actual source gap.
    assert result["continuity_max_gap_days"] == 3
    assert result["boundary_age_days"] == 6
    assert result["continuity_sufficient"] is True
    assert result["observation_density_sufficient"] is True


def test_old_boundary_is_rejected_even_when_window_gaps_are_small():
    result = quality(["2026-09-01", "2026-10-08", "2026-10-09", "2026-10-12"])
    assert result["continuity_max_gap_days"] == 3
    assert result["boundary_sufficient"] is False
    assert result["continuity_sufficient"] is False


@pytest.mark.parametrize("dates", [
    ["2026-09-30", "2026-10-08"],  # The earlier holiday sample stays low frequency.
    ["2026-09-30", "2026-10-09", "2026-10-12"],  # A genuinely missing point.
])
def test_missing_observations_are_not_synthesized(dates):
    result = quality(dates)
    assert result["observation_density_sufficient"] is False
    assert result["observation_count"] == len(dates)


@pytest.mark.parametrize("dates", [
    ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-20", "2026-01-31"],
    ["2026-01-01", "2026-01-02", "2026-01-03"],  # Unobserved tail is also a gap.
])
def test_real_internal_and_trailing_gaps_remain_blocked(dates):
    result = quality(dates, start="2026-01-01", end="2026-02-01")
    assert result["continuity_max_gap_days"] > 7
    assert result["continuity_sufficient"] is False


def test_layer6_rejects_old_predecessor_even_from_an_unbounded_loader():
    engine = FilteringEngine(period_years=7 / 365, data_loader=SimpleNamespace(),
                             enable_tracking=False, verbose=False)
    engine.risk_calculator = Mock()
    frame = pd.DataFrame({"nav_date": pd.to_datetime([
        "2026-09-01", "2026-10-08", "2026-10-09", "2026-10-12"]),
        "unit_nav": [1, 1, 1, 1]})
    _, metrics, meta = engine._calculate_layer6_single_fund(
        "isolated", frame, ["volatility"], window_end_date=date(2026, 10, 13))
    assert metrics is None and meta["status"] == "NAV_DATA_GAP"
    engine.risk_calculator.calculate_all_metrics.assert_not_called()


@pytest.mark.parametrize("window", ["daily", "intraday"])
def test_disabled_dependency_keeps_collection_and_blocks_screening(monkeypatch, tmp_path, window):
    scheduler = module.ProjectScheduler.__new__(module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        autostock_enabled=False, project_root=tmp_path, database_path=tmp_path / "unused.db",
        latest_report_file=tmp_path / "latest.json", effective_nav_coverage_ratio=.9,
        effective_nav_weekday_only=True, run_collection_by_default=True, run_screening_by_default=True,
    )
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._log = Mock()
    scheduler._build_host_runtime_snapshot = lambda: {}
    scheduler._read_holding_period_snapshot = lambda settings: {}
    scheduler._daily_refresh_lineage_rescreen_required = lambda **kw: {"required": False}
    scheduler._build_maintenance_data_change_summary = lambda **kw: {"changed": False}
    scheduler._run_workflow_in_subprocess = Mock(side_effect=AssertionError("must not screen"))
    snapshot = SimpleNamespace(candidate_date="2026-10-12", signature="isolated",
                               to_dict=lambda: {"candidate_date": "2026-10-12", "signature": "isolated"})
    monkeypatch.setattr(module, "load_effective_nav_snapshot", lambda *a, **kw: snapshot)
    monkeypatch.setattr(module, "load_latest_screened_db_snapshot", lambda *a: {})
    monkeypatch.setattr(module, "screening_snapshot_changed", lambda *a: (True, "snapshot_advanced"))
    monkeypatch.setattr(module, "run_autostock_nav_tail_fill", Mock(return_value={"status": "skipped"}))
    collection = Mock(return_value={})
    monkeypatch.setattr(module, "run_project_pipeline", collection)
    result = scheduler._execute_scheduled_refresh(
        task_name="scheduled_daily_bond_refresh", refresh_window=window,
        nav_history_sync_enabled=False, nav_history_sync_limit=0,
        long_term_history_enabled=False, long_term_history_limit=0)
    collection.assert_called_once()
    assert collection.call_args.args[1]["run_collection"] is True
    assert collection.call_args.args[1]["run_screening"] is False
    scheduler._run_workflow_in_subprocess.assert_not_called()
    assert result["status"] == "degraded"
    assert result["publication_result"]["status"] == "blocked"
    assert result["publication_result"]["run_id"] is None
    assert result["collection_completed"] is True
    assert result["screening_triggered"] is False


def test_manual_formal_preflight_stops_before_any_expensive_pipeline(tmp_path, monkeypatch):
    from backend import standalone_runner
    core = tmp_path / "core"
    (core / "bond_fund_screening/tasks").mkdir(parents=True)
    (core / "main.py").touch()
    (core / "bond_fund_screening/tasks/daily_screening_master.py").touch()
    settings = SimpleNamespace(core_root=core, autostock_enabled=False,
                               run_collection_by_default=True, run_screening_by_default=True)
    pipeline = Mock()
    monkeypatch.setattr(standalone_runner, "run_core_pipeline", pipeline)
    with pytest.raises(RuntimeError, match="independent_source_unavailable"):
        run_project_pipeline(settings, {"run_collection": False, "run_screening": True}, lambda text: None)
    pipeline.assert_not_called()
    settings.autostock_enabled = True
    run_project_pipeline(settings, {"run_collection": False, "run_screening": True}, lambda text: None)
    pipeline.assert_called_once()
    assert configured_publication_blocker(settings) is None  # Configured != verified healthy.


def test_watchdog_dependency_block_resumes_when_provider_reenabled(monkeypatch, tmp_path):
    scheduler = module.ProjectScheduler.__new__(module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(autostock_enabled=False, database_path=tmp_path / "unused.db",
                                        latest_report_file=tmp_path / "latest.json")
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._last_refresh_processed_current_snapshot = lambda *a, **kw: False
    scheduler._blocked_snapshot_retry_cooldown = lambda *a, **kw: {}
    scheduler._failed_screening_retry_cooldown = lambda *a, **kw: {}
    scheduler._execute_scheduled_refresh = Mock(return_value={"status": "success"})
    snapshot = SimpleNamespace(candidate_date="2026-10-12", signature="isolated")
    monkeypatch.setattr(module, "load_effective_nav_snapshot", lambda *a, **kw: snapshot)
    monkeypatch.setattr(module, "load_latest_screened_db_snapshot", lambda *a: {})
    monkeypatch.setattr(module, "screening_snapshot_changed", lambda *a: (True, "snapshot_advanced"))
    blocked = scheduler._execute_snapshot_watchdog()
    assert blocked["publication_result"]["status"] == "blocked"
    assert blocked["collection_triggered"] is False
    scheduler._execute_scheduled_refresh.assert_not_called()
    scheduler.settings.autostock_enabled = True
    scheduler._execute_snapshot_watchdog()
    scheduler._execute_scheduled_refresh.assert_called_once()


def test_catch_up_blocks_only_completed_collection_slot(monkeypatch):
    scheduler = module.ProjectScheduler.__new__(module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        autostock_enabled=False, scheduler_daily_hour=7, scheduler_daily_minute=30,
        scheduler_intraday_refresh_enabled=True, scheduler_intraday_hour=13, scheduler_intraday_minute=45,
        scheduler_weekly_day_of_week="sun", scheduler_weekly_hour=3, scheduler_weekly_minute=0)
    scheduler._last_run_at = lambda *a, **kw: datetime(2026, 10, 13, 7)
    scheduler._workflow_busy_retry_cooldown = lambda **kw: {}
    last_runs = {"scheduled_daily_bond_refresh": {
        "collection_completed": True, "warning_reason": "independent_source_unavailable",
        "finished_at": "2026-10-13T08:00:00", "status": "degraded"}}
    scheduler.state_store = SimpleNamespace(snapshot=lambda: {"last_runs": last_runs})
    assert scheduler._resolve_startup_catch_up_jobs(datetime(2026, 10, 13, 9)) == []
    # The next collection slot must never be suppressed by the morning blocker.
    assert scheduler._resolve_startup_catch_up_jobs(datetime(2026, 10, 13, 14)) == ["intraday_refresh"]
    scheduler.settings.autostock_enabled = True
    assert scheduler._resolve_startup_catch_up_jobs(datetime(2026, 10, 13, 9)) == ["daily_refresh"]
    scheduler.settings.autostock_enabled = False
    last_runs["scheduled_daily_bond_refresh"]["collection_completed"] = False
    assert scheduler._resolve_startup_catch_up_jobs(datetime(2026, 10, 13, 9)) == ["daily_refresh"]
