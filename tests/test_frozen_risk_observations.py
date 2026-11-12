"""Synthetic fixtures exercise provenance; they are not fund performance data."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))
from utils import calculation_capture as capture
from bond_fund_screening.core.standard_risk_metrics import calculate_standard_risk_metrics
from backend import frozen_risk_observations as observations


@pytest.fixture
def journal(tmp_path, monkeypatch):
    directory = tmp_path / "capture"
    directory.mkdir()
    monkeypatch.setenv(capture.ENV_DIRECTORY, str(directory))
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", "synthetic_risk")
    monkeypatch.setattr(capture, "_ACTIVE", None)
    writer = capture.CaptureWriter
    monkeypatch.setattr(capture, "CaptureWriter", lambda directory, kind, context:
                        writer(directory, kind, context, code_paths=[ROOT / "core/utils/calculation_capture.py"]))
    return directory


def captured_risk(journal, *, returns=None, mar=0., wrong_period_value=False, duplicate_period=False):
    """Use the real typed writer and metric core, with explicitly synthetic rows."""
    values = np.array(returns if returns is not None else [.001, .002, .003, .004])
    calculated_at = datetime(2026, 9, 14, 12, 0, 0, 123456)

    @capture.capture_call("database_risk.period")
    def period_result(period):
        result = calculate_standard_risk_metrics(values, np.r_[1., np.cumprod(1 + values)],
            annual_return=.05, periods_per_year=252., risk_free_rate=.015, mar=mar)
        result.update(annual_return=5., data_points=len(values),
                      start_date=pd.Timestamp("2025-09-11"), end_date=pd.Timestamp("2026-09-11"))
        if wrong_period_value:
            result["raw_sortino_ratio"] = 1.0
        return result

    @capture.capture_call("database_risk.write_record")
    def write_record(fcode, period, metrics, calculated_at):
        return {**metrics, "fcode": fcode, "period": period, "calculated_at": calculated_at,
                "nav_snapshot_signature": "synthetic_signature"}

    @capture.capture_call("database_risk.fund")
    def fund(fcode):
        metrics = period_result("1y")
        if duplicate_period:
            period_result("1y")
        return write_record(fcode, "1y", metrics, calculated_at)

    @capture.capture_session("database_risk")
    def run():
        return fund("000001")

    result = run()
    manifest_path = next(journal.glob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_text())
    session = {**manifest, "manifest": manifest_path.name,
               "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}
    return session, result


@pytest.mark.parametrize("values,mar,undefined", [
    ([.001, .002, .003, .004], 0., True),
    ([.001, .002, -.003, .004], 0., False),
    ([.001, .002, .003, .004], .8, False),
])
def test_actual_capture_binds_operands_period_output_and_write(journal, tmp_path, values, mar, undefined):
    session, original = captured_risk(journal, returns=values, mar=mar)
    result = observations._extract_session(journal, session, run_id="synthetic_risk", scratch_dir=tmp_path, log=lambda _: None)
    assert len(result) == 1
    row = result[0]
    assert row["fcode"] == "000001" and row["annual_mar"] == mar
    assert row["production_fallback_triggered"] is False
    assert row["undefined_downside"] is undefined
    assert row["observation_count"] == len(values)
    assert row["risk_record"]["raw_sortino_ratio"] == original["raw_sortino_ratio"]
    assert row["risk_record"]["calculated_at"] == original["calculated_at"].isoformat()
    assert len({row["standard_call_id"], row["period_call_id"], row["write_call_id"]}) == 3


def test_period_cannot_claim_different_child_metrics(journal, tmp_path):
    session, _ = captured_risk(journal, wrong_period_value=True)
    with pytest.raises(observations.CalculationCaptureError, match="operands_disagree"):
        observations._extract_session(journal, session, run_id="synthetic_risk", scratch_dir=tmp_path, log=lambda _: None)


def test_repeated_identical_period_results_do_not_prove_which_input_was_used(journal, tmp_path):
    session, _ = captured_risk(journal, duplicate_period=True)
    with pytest.raises(observations.CalculationCaptureError, match="ambiguous_risk_period"):
        observations._extract_session(journal, session, run_id="synthetic_risk", scratch_dir=tmp_path, log=lambda _: None)


def test_corrupt_journal_cannot_produce_a_partial_success(journal, tmp_path):
    session, _ = captured_risk(journal)
    path = journal / session["file"]
    path.write_bytes(path.read_bytes()[:-7])
    with pytest.raises(observations.CalculationCaptureError, match="physical_hash"):
        observations._extract_session(journal, session, run_id="synthetic_risk", scratch_dir=tmp_path, log=lambda _: None)


def fixture_record(code="000001"):
    record = {field: 1. for field in observations.RISK_FIELDS}
    record.update(fcode=code, period="1y", start_date="2025-09-11", end_date="2026-09-11",
                  calculated_at="2026-09-14T12:00:00.123456", nav_snapshot_signature="synthetic_signature",
                  sharpe_ratio_cap_hit=False, sortino_ratio_cap_hit=True)
    return record


def fixture_evidence(record):
    return {"observations": [{"fcode": record["fcode"], "risk_record": record.copy(),
        "standard_input_sha256": "a" * 64, "annual_mar": 0., "zero_downside": True,
        "production_fallback_triggered": False, "positive_excess_return": True}], "sources": []}


def test_exact_report_source_keeps_real_zero_and_missing(journal):
    record = fixture_record()
    record.update(raw_sortino_ratio=None, sortino_ratio=None, sortino_ratio_cap_hit=None, max_drawdown=0.)
    frame = pd.DataFrame([record])
    actual = observations.bind_report_risk_observations(frame, fixture_evidence(record), generated_at="2026-09-14T13:00:00")
    assert actual["complete"] and actual["matched_risk_rows"] == 1
    assert actual["rows"][0]["stored_raw_sortino_ratio"] is None
    assert actual["zero_downside_count"] == 1


@pytest.mark.parametrize("field,value", [
    ("calculated_at", "2026-09-14T12:00:01"),
    ("start_date", "2025-09-12"),
    ("end_date", "2026-09-10"),
    ("nav_snapshot_signature", "different_snapshot"),
    ("raw_sortino_ratio", 1.001),
    ("data_points", 2),
    ("sortino_ratio_cap_hit", None),
])
def test_equal_code_and_nav_day_cannot_hide_a_different_risk_row(field, value):
    record = fixture_record()
    frame = pd.DataFrame([{**record, field: value}])
    actual = observations.bind_report_risk_observations(frame, fixture_evidence(record), generated_at="2026-09-14T13:00:00")
    assert not actual["complete"] and actual["matched_risk_rows"] == 0
    assert actual["source_risk_row_mismatch_codes"] == ["000001"]
    assert actual["rows"][0]["mismatched_fields"] == field


def test_uncomputed_funds_stay_uncomputed_and_missing_sources_block_the_full_universe():
    record = fixture_record()
    unknown = {field: None for field in observations.RISK_FIELDS} | {"fcode": "000003"}
    frame = pd.DataFrame([record, fixture_record("000002"), unknown])
    before = frame.copy(deep=True)
    actual = observations.bind_report_risk_observations(frame, fixture_evidence(record), generated_at="2026-09-14T13:00:00")
    assert not actual["complete"] and actual["matched_risk_rows"] == 1
    assert actual["missing_risk_input_codes"] == ["000002"] and actual["report_rows_without_risk"] == 1
    pd.testing.assert_frame_equal(frame, before)


def test_a_future_calculation_cannot_be_used_as_the_original_report_input():
    record = fixture_record()
    with pytest.raises(observations.CalculationCaptureError, match="not_observed_before_report"):
        observations.bind_report_risk_observations(pd.DataFrame([record]), fixture_evidence(record),
                                                   generated_at="2026-09-14T11:59:59")


def test_identical_written_numbers_with_different_original_operands_are_ambiguous():
    record = fixture_record()
    evidence = fixture_evidence(record)
    evidence["observations"].append({**evidence["observations"][0], "standard_input_sha256": "b" * 64})
    with pytest.raises(observations.CalculationCaptureError, match="operands_ambiguous"):
        observations.bind_report_risk_observations(pd.DataFrame([record]), evidence, generated_at="2026-09-14T13:00:00")


def test_missing_identity_columns_do_not_turn_into_an_empty_success():
    actual = observations.bind_report_risk_observations(pd.DataFrame([{"fcode": "000001"}]),
                                                       {"observations": []}, generated_at="2026-09-14T13:00:00")
    assert actual["complete"] is False and "calculated_at" in actual["missing_risk_columns"]


def test_missing_source_field_is_not_an_observed_null():
    record = fixture_record()
    record["max_drawdown_duration"] = None
    evidence = fixture_evidence(record)
    del evidence["observations"][0]["risk_record"]["max_drawdown_duration"]
    actual = observations.bind_report_risk_observations(pd.DataFrame([record]), evidence,
                                                       generated_at="2026-09-14T13:00:00")
    assert not actual["complete"] and actual["source_risk_row_mismatch_codes"] == ["000001"]
    assert actual["rows"][0]["mismatched_fields"] == "max_drawdown_duration"


def test_partially_present_risk_row_is_not_classified_as_no_risk_record():
    record = {field: None for field in observations.RISK_FIELDS} | {"fcode": "000001", "annual_return": 1.}
    actual = observations.bind_report_risk_observations(pd.DataFrame([record]), {"observations": []},
                                                       generated_at="2026-09-14T13:00:00")
    assert not actual["complete"] and actual["report_rows_without_risk"] == 0
    assert actual["missing_risk_input_codes"] == ["000001"]


def test_capture_header_must_match_the_index_source_identity(journal, tmp_path):
    session, _ = captured_risk(journal)
    with pytest.raises(observations.CalculationCaptureError, match="source_index_disagrees"):
        observations._extract_session(journal, session, run_id="synthetic_risk", scratch_dir=tmp_path,
                                      log=lambda _: None, expected_code_sha256={"different.py": "a" * 64})


def test_source_mutation_after_replay_discards_its_evidence(tmp_path):
    path = tmp_path / "reports/source.json"
    path.parent.mkdir()
    path.write_text("original synthetic source")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    evidence = {"sources": [{"files": {"reports/source.json": digest}}]}
    path.write_text("changed synthetic source")
    with pytest.raises(observations.CalculationCaptureError, match="source_changed"):
        observations.verify_risk_observation_sources(evidence, project_root=tmp_path)


def test_failed_outer_child_cannot_be_read_as_a_success(tmp_path):
    path = tmp_path / "reports/meta.json"
    path.parent.mkdir()
    metadata = {"success": False, "return_code": 1, "timed_out": False}
    path.write_text(json.dumps(metadata))
    session = {"kind": "database_risk", "subprocess": {"meta_path": "reports/meta.json",
        "meta_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "metadata": metadata}}
    index = tmp_path / "reports/index.json"
    index.write_text("synthetic index")
    with pytest.raises(observations.CalculationCaptureError, match="child_not_verified_success"):
        observations._source_files(index, {"sessions": [session]}, tmp_path)
