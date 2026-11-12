"""Provider migration keeps missing evidence and historical state explicit."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from backend.publication_dependencies import configured_publication_blocker
from backend import scheduler as module
from backend.prospective_autostock import capture_strategy_observations
from bond_fund_screening.tasks.final_screening_validation_display import apply_validation_display


def new_settings(**kwargs):
    return SimpleNamespace(autostock_enabled=False, publication_validation_provider="eastmoney_cross_endpoint",
        tiantian_valuation_primary_url="https://fundcomapi.tiantianfunds.com/mm/newCore/FundValuationLast",
        tiantian_valuation_fallback_url="https://fundcomapi.eastmoney.com/mm/newCore/FundValuationLast",
        eastmoney_fund_info_url="https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo", **kwargs)


def test_configured_new_provider_allows_real_validation_without_claiming_health():
    settings = new_settings()
    assert configured_publication_blocker(settings) is None
    settings.eastmoney_fund_info_url = ""
    assert configured_publication_blocker(settings)["code"] == "validation_source_unconfigured"
    settings.publication_validation_provider = "unknown"
    assert configured_publication_blocker(settings)["code"] == "validation_source_unconfigured"
    settings.publication_validation_provider = "autostock"
    assert configured_publication_blocker(settings)["code"] == "independent_source_unavailable"


def test_publication_cannot_omit_new_provider_evidence_gate():
    from backend.publication import _resolve_publish_blockers
    payload = {"data_sources": {"publication_validation_provider": "eastmoney_cross_endpoint"}}
    assert "publication_data_validation_unverified" in {item['code'] for item in _resolve_publish_blockers(payload)}
    payload['data_validation_audit'] = {'required': True, 'snapshot': {'path': 'isolated'}}
    assert "publication_data_validation_unverified" in {item['code'] for item in _resolve_publish_blockers(payload)}
    payload['internal_audit'] = {'artifact_consistency': {'checks': [{'name': 'data_validation_snapshot_bound', 'aligned': True}]}}
    assert "publication_data_validation_unverified" not in {item['code'] for item in _resolve_publish_blockers(payload)}
    payload['data_validation_audit']['unavailable'] = {'reason': 'isolated_failure'}
    assert "publication_data_validation_unverified" in {item['code'] for item in _resolve_publish_blockers(payload)}


def test_watchdog_resumes_actual_refresh_after_provider_switch(monkeypatch, tmp_path):
    scheduler = module.ProjectScheduler.__new__(module.ProjectScheduler)
    scheduler.settings = new_settings(database_path=tmp_path / "unused.db", latest_report_file=tmp_path / "latest.json")
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler._last_refresh_processed_current_snapshot = lambda *a, **kw: False
    scheduler._blocked_snapshot_retry_cooldown = lambda *a, **kw: {}
    scheduler._failed_screening_retry_cooldown = lambda *a, **kw: {}
    scheduler._execute_scheduled_refresh = Mock(return_value={"status": "success"})
    monkeypatch.setattr(module, "load_effective_nav_snapshot", lambda *a, **kw: SimpleNamespace(candidate_date="2026-10-12", signature="isolated"))
    monkeypatch.setattr(module, "load_latest_screened_db_snapshot", lambda *a: {})
    monkeypatch.setattr(module, "screening_snapshot_changed", lambda *a: (True, "snapshot_advanced"))
    scheduler._execute_snapshot_watchdog()
    scheduler._execute_scheduled_refresh.assert_called_once()


def test_unknown_export_status_remains_unchecked_instead_of_passed():
    frame = pd.DataFrame({"基金代码": ["000191", "100066"]})
    statuses = {"000191": {"validation_source": "eastmoney_cross_endpoint", "validation_status": "strict_match"}}
    result = apply_validation_display(frame, statuses, lambda value: value)
    assert result["数据校验状态"].tolist() == ["通过", "未校验"]
    assert result["数据校验原始状态"].tolist() == ["strict_match", "not_checked"]
    assert not any(column.startswith("AutoStock") for column in result)


def test_disabled_autostock_experiment_never_queries_old_or_new_source(monkeypatch, tmp_path):
    from backend import prospective_autostock
    plan = Mock(side_effect=AssertionError("disabled experiments must not query"))
    monkeypatch.setattr(prospective_autostock, "build_observation_plan", plan)
    settings = new_settings(project_root=Path(__file__).resolve().parents[1])
    result = capture_strategy_observations(settings, run_id="isolated", revision="isolated", generated_at="2026-10-13",
                                         universe=pd.DataFrame(), filters={}, report_inputs={})
    plan.assert_not_called()
    assert result["status"] == "disabled" and result["target_count"] is None
    assert result["records"] == [] and result["observed_statuses"] == {}
