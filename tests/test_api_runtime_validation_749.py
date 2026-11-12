"""Runtime source evidence distinguishes real matches, drift and unknown fields."""
from pathlib import Path
from types import SimpleNamespace

import pytest


VALIDATION = {
    "autostock_enabled": False,
    "publication_validation_provider": "eastmoney_cross_endpoint",
    "tiantian_valuation_primary_url": "https://fundcomapi.tiantianfunds.com/mm/newCore/FundValuationLast",
    "tiantian_valuation_fallback_url": "https://fundcomapi.eastmoney.com/mm/newCore/FundValuationLast",
    "eastmoney_fund_info_url": "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo",
}


@pytest.fixture
def runtime_payload(monkeypatch, tmp_path):
    import backend.app as app

    values = {
        **VALIDATION,
        "config_path": tmp_path / "project.yaml",
        "scheduler_host_mode": "external_process",
        "tiantian_base_url": "http://tiantian-api:3000",
        "autostock_base_url": "https://api.autostock.cn/v1",
    }
    configured = SimpleNamespace(**values)
    runtime = SimpleNamespace(**values)
    state = {"runtime_api_config": {**values, "captured_at": "isolated-state-fixture"}}
    monkeypatch.setattr(app, "settings", runtime)
    monkeypatch.setattr(app, "load_settings", lambda *args, **kwargs: configured)
    monkeypatch.setattr(app, "_scheduler_state_with_runtime_evidence", lambda *args: state)
    return app._build_api_settings_payload, runtime, state


def test_actual_web_and_scheduler_source_settings_match(runtime_payload):
    build, _, _ = runtime_payload
    payload = build()
    for key, value in VALIDATION.items():
        assert payload["configured"][key] == value
        assert payload["runtime"][key] == value
        assert payload["runtime_matches_config"][key] is True
        assert payload["scheduler"]["runtime"][key] == value
        assert payload["scheduler"]["runtime_matches_config"][key] is True
    assert payload["runtime"]["autostock_enabled"] is False
    assert payload["scheduler"]["runtime"]["autostock_enabled"] is False


@pytest.mark.parametrize("key", VALIDATION)
def test_web_source_drift_is_explicit(runtime_payload, key):
    build, runtime, _ = runtime_payload
    changed = True if key == "autostock_enabled" else "isolated-changed-setting"
    setattr(runtime, key, changed)
    payload = build()
    assert payload["runtime"][key] == changed
    assert payload["runtime_matches_config"][key] is False
    assert payload["scheduler"]["runtime_matches_config"][key] is True


@pytest.mark.parametrize("key", VALIDATION)
def test_scheduler_source_drift_is_explicit(runtime_payload, key):
    build, _, state = runtime_payload
    changed = True if key == "autostock_enabled" else "isolated-changed-setting"
    state["runtime_api_config"][key] = changed
    payload = build()
    assert payload["scheduler"]["runtime"][key] == changed
    assert payload["scheduler"]["runtime_matches_config"][key] is False
    assert payload["runtime_matches_config"][key] is True


@pytest.mark.parametrize("key", VALIDATION)
def test_missing_scheduler_source_evidence_stays_unknown(runtime_payload, key):
    build, _, state = runtime_payload
    del state["runtime_api_config"][key]
    payload = build()
    assert payload["scheduler"]["runtime"][key] is None
    assert payload["scheduler"]["runtime_matches_config"][key] is None


@pytest.mark.parametrize("unknown", [None, "", "false", 0])
def test_untyped_scheduler_disabled_flag_is_not_a_match(runtime_payload, unknown):
    build, _, state = runtime_payload
    state["runtime_api_config"]["autostock_enabled"] = unknown
    assert build()["scheduler"]["runtime_matches_config"]["autostock_enabled"] is None


def test_scheduler_records_the_actual_primary_and_backup_urls(monkeypatch, tmp_path):
    from backend.scheduler import ProjectScheduler

    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.settings = SimpleNamespace(**VALIDATION, scheduler_host_mode="external_process")
    monkeypatch.setattr(scheduler, "_ensure_scheduler_instance_id", lambda: "isolated-state-fixture")
    monkeypatch.setattr(scheduler, "_scheduler_lock_path", lambda *args: tmp_path / "scheduler.lock")
    monkeypatch.setattr(scheduler, "_project_relative_path", lambda *args: "config/project.yaml")
    evidence = scheduler._runtime_api_config()
    for key, value in VALIDATION.items():
        assert evidence[key] == value
    assert evidence["autostock_enabled"] is False
    assert evidence["validation_supplier_independent"] is False
