"""Signed disabled observation captures remain explicit absence of coverage.

These contract fixtures contain no fabricated fund NAV or return values.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend import prospective_autostock as production
from backend import report_builder

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "isolated_disabled_capture"
REVISION = "isolated_revision"
REPORT_GENERATED_AT = "2020-01-01T00:00:00"


def _settings():
    return SimpleNamespace(project_root=ROOT, autostock_enabled=False,
                           publication_validation_provider="autostock")


def _resign(capture):
    capture["content_sha256"] = production.content_sha256(
        {key: value for key, value in capture.items() if key != "content_sha256"}
    )
    return capture


def _verify(capture, *, settings=None, report=None):
    return production.validate_strategy_observations(
        settings or _settings(),
        frozen={"generated_at": REPORT_GENERATED_AT, "inputs": {"prospective_autostock_capture": capture}},
        report=report or {"run_id": RUN_ID, "publication_control": {"revision": REVISION}},
        universe=pd.DataFrame(), production_statuses={},
    )


@pytest.fixture
def disabled_capture(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("disabled capture must never query or initialize the legacy client")

    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report", forbidden)
    monkeypatch.setattr(report_builder, "_supplement_official_autostock_statuses", forbidden)
    monkeypatch.setattr(production, "build_observation_plan", forbidden)
    return production.capture_strategy_observations(
        _settings(), run_id=RUN_ID, revision=REVISION,
        generated_at=REPORT_GENERATED_AT,
        universe=pd.DataFrame(), filters={}, report_inputs={},
    )


def test_disabled_writer_and_reader_preserve_unavailable_coverage(disabled_capture):
    check, observed = _verify(disabled_capture)
    assert check == {"available": False, "integrity_verified": True, "status": "disabled",
                     "reason": "autostock_disabled_or_replaced", "target_count": None,
                     "compared_count": 0}
    assert observed == {}
    assert disabled_capture["plan"] is None
    assert disabled_capture["target_count"] is None
    assert disabled_capture["records"] == [] and disabled_capture["observed_statuses"] == {}


def test_replaced_provider_also_captures_disabled_without_legacy_client(disabled_capture):
    settings = SimpleNamespace(project_root=ROOT, autostock_enabled=True,
                               publication_validation_provider="eastmoney_cross_endpoint")
    replaced = production.capture_strategy_observations(
        settings, run_id=RUN_ID, revision=REVISION,
        generated_at=disabled_capture["report_generated_at"],
        universe=pd.DataFrame(), filters={}, report_inputs={},
    )
    check, observed = _verify(replaced, settings=settings)
    assert check["status"] == "disabled" and check["available"] is False
    assert check["integrity_verified"] is True and observed == {}


def test_historical_disabled_evidence_is_not_reinterpreted_by_current_settings(disabled_capture):
    settings = SimpleNamespace(project_root=ROOT, autostock_enabled=True,
                               publication_validation_provider="autostock")
    check, observed = _verify(disabled_capture, settings=settings)
    assert check["available"] is False and observed == {}
    production_statuses = {"000001": {"AutoStock原始校验状态": "hard_block"}}
    assert {**production_statuses, **observed} == production_statuses


@pytest.mark.parametrize(("field", "value"), [
    ("reason", "source_available"), ("reason", None),
    ("plan", {}), ("records", [{}]), ("records", None), ("records", {}),
    ("observed_statuses", {"000001": {"AutoStock原始校验状态": "strict_match"}}),
    ("observed_statuses", None), ("observed_statuses", []),
    ("target_count", 0), ("target_count", 1),
    ("compared_count", None), ("compared_count", False),
    ("compared_count", "0"), ("compared_count", 0.0), ("compared_count", 1),
])
def test_signed_disabled_capture_cannot_claim_any_observation_or_known_target_count(
    disabled_capture, field, value,
):
    invalid = deepcopy(disabled_capture)
    invalid[field] = value
    _resign(invalid)
    with pytest.raises(ValueError, match="prospective_disabled_capture_invalid"):
        _verify(invalid)


@pytest.mark.parametrize("field", ["plan", "target_count", "records", "observed_statuses", "compared_count", "reason"])
def test_disabled_contract_fields_must_be_recorded(disabled_capture, field):
    invalid = deepcopy(disabled_capture)
    invalid.pop(field)
    _resign(invalid)
    with pytest.raises(ValueError, match="prospective_disabled_capture_invalid"):
        _verify(invalid)


@pytest.mark.parametrize("field", ["run_id", "revision", "report_generated_at"])
def test_signed_disabled_capture_keeps_report_binding(disabled_capture, field):
    invalid = deepcopy(disabled_capture)
    invalid[field] = "different_report"
    _resign(invalid)
    with pytest.raises(ValueError, match="prospective_capture_binding_invalid"):
        _verify(invalid)


def test_disabled_capture_still_rejects_unsigned_changes(disabled_capture):
    invalid = deepcopy(disabled_capture)
    invalid["compared_count"] = 1
    with pytest.raises(ValueError, match="prospective_capture_binding_invalid"):
        _verify(invalid)


@pytest.mark.parametrize("mutation", ["before_report", "negative_duration"])
def test_signed_disabled_capture_keeps_observed_clock_checks(disabled_capture, mutation):
    invalid = deepcopy(disabled_capture)
    if mutation == "before_report":
        invalid["started_at"] = (
            datetime.fromisoformat(invalid["report_generated_at"]) - timedelta(seconds=1)
        ).isoformat()
    else:
        invalid["finished_at"] = (
            datetime.fromisoformat(invalid["started_at"]) - timedelta(seconds=1)
        ).isoformat()
    _resign(invalid)
    with pytest.raises(ValueError, match="prospective_capture_time_invalid"):
        _verify(invalid)


def test_existing_capture_failed_without_plan_keeps_its_original_contract(disabled_capture):
    failed = deepcopy(disabled_capture)
    failed.update(status="capture_failed", error="isolated planning failure")
    failed.pop("reason")
    check, observed = _verify(_resign(failed))
    assert check == {"available": True, "integrity_verified": True,
                     "status": "capture_failed", "compared_count": 0}
    assert observed == {}


def test_captured_status_without_a_plan_does_not_gain_disabled_exemption(disabled_capture):
    invalid = deepcopy(disabled_capture)
    invalid["status"] = "captured"
    _resign(invalid)
    with pytest.raises(ValueError, match="prospective_capture_missing_plan"):
        _verify(invalid)
