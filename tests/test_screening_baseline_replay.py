"""Replay truthfulness tests; all small data below are synthetic test fixtures."""
from __future__ import annotations
from dataclasses import replace
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.config import load_settings
from backend import replay_evidence
from backend.runtime_replay import build_replay_precision_summary, refresh_registry_replay_precision
from backend.screening_baseline_replay import compare_frame, verify_frozen_baseline


def test_full_membership_nulls_and_real_zero_are_compared_separately():
    expected = pd.DataFrame({"基金代码": ["000001", "000002"], "score": [0.0, None]})
    actual = pd.DataFrame({"基金代码": ["000001", "000002"], "score": [None, 0.0]})
    result = compare_frame("fixture", expected, actual, ["score"])
    assert result["aligned"] is False
    assert result["value_difference_count"] == 2
    assert result["added_codes"] == result["removed_codes"] == []


def test_duplicate_or_missing_members_do_not_pass_value_check():
    expected = pd.DataFrame({"基金代码": ["000001", "000002"], "score": [63.02, 60.0]})
    duplicate = expected.copy()
    duplicate.loc[1, "基金代码"] = "000001"
    result = compare_frame("fixture", expected, duplicate, ["score"])
    assert result["duplicate_or_empty_codes"] is True and result["aligned"] is False
    empty = expected.iloc[:0]
    assert compare_frame("empty", empty, empty, ["score"])["aligned"] is True


def test_absent_snapshot_does_not_reconstruct_from_later_database(tmp_path, monkeypatch):
    from backend import report_builder
    monkeypatch.setattr(report_builder, "_read_metadata", lambda *a: pytest.fail("A later database was read"))
    result, context = verify_frozen_baseline(replace(load_settings(), project_root=tmp_path), {"run_id": "fixture", "artifacts": {}})
    assert result["status"] == "inputs_missing"
    assert result["baseline_verified"] is False and not context


def test_complete_legacy_input_files_are_not_proof_of_reproduction(tmp_path):
    artifacts = {}
    for key in ("long_csv", "short_csv", "final_screening_decision_ledger_file", "bond_mainlist_quality_audit_json_file", "filtered_csv"):
        path = tmp_path / key
        path.write_text("fixture")
        artifacts[key] = str(path)
    snapshot = tmp_path / "publish.json"
    snapshot.write_text(json.dumps({"dynamic_post_review_records": [{"基金代码": "000001"}]}))
    artifacts["publish_chain_snapshot_json"] = str(snapshot)
    result = build_replay_precision_summary(artifacts, project_root=tmp_path)
    assert result["exact_replay_inputs_available"] is True
    assert result["publish_gate_inputs_available"] is True
    assert result["status"] == "reference_only"
    assert result["baseline_reproduction_verified"] is False
    assert result["publish_gate_exact_replay_available"] is False


def test_replay_verification_is_bound_to_snapshot_sources_and_all_checks(tmp_path):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic signature fixture")
    report = tmp_path / "report.json"
    report.write_text('{"fixture":true}')
    proof = {"snapshot_sha256": replay_evidence.content_hash(snapshot), "baseline_verified": True,
             "status": "matched", "scope": "report_selection_from_complete_scored_universe",
             "later_database_reads": False, "missing_history_imputation": False,
             "source_files": {key: {"path": "report.json", "sha256": replay_evidence.content_hash(report)}
                              for key in replay_evidence.REQUIRED_BASELINE_SOURCES},
             "checks": [{"name": name, "aligned": True} for name in replay_evidence.REQUIRED_BASELINE_CHECKS]}
    replay_evidence.write_evidence(tmp_path, proof)
    assert replay_evidence.read_evidence(tmp_path, snapshot)["baseline_verified"] is True
    proof["checks"].pop()
    replay_evidence.write_evidence(tmp_path, proof)
    assert replay_evidence.read_evidence(tmp_path, snapshot)["baseline_verified"] is False
    report.write_text('{"fixture":false}')
    assert replay_evidence.read_evidence(tmp_path, snapshot)["status"] == "invalid_evidence"


def test_cached_publication_observes_new_proof_and_its_invalidation(tmp_path):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic signature fixture")
    report = tmp_path / "report.json"
    report.write_text('{"fixture":true}')
    registry = {
        "commit_generation": "fixture-generation",
        "canonical_runs": [{"run_id": "fixture", "artifacts": {"screening_input_snapshot_json_gz": "snapshot.gz"},
                            "replay_precision": {"status": "exact"}}],
        "automation_layers": {"current_published_run": {"run_id": "fixture", "replay_precision": {"status": "exact"}}},
    }
    before = json.dumps(registry, sort_keys=True)
    first = refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert first["automation_layers"]["current_published_run"]["replay_precision"]["status"] == "inputs_frozen"
    proof = {"snapshot_sha256": replay_evidence.content_hash(snapshot), "baseline_verified": True,
             "status": "matched", "scope": replay_evidence.REPORT_BASELINE_SCOPE,
             "later_database_reads": False, "missing_history_imputation": False,
             "source_files": {key: {"path": "report.json", "sha256": replay_evidence.content_hash(report)}
                              for key in replay_evidence.REQUIRED_BASELINE_SOURCES},
             "checks": [{"name": name, "aligned": True} for name in replay_evidence.REQUIRED_BASELINE_CHECKS]}
    replay_evidence.write_evidence(tmp_path, proof)
    second = refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert second["commit_generation"] == first["commit_generation"]
    assert second["automation_layers"]["current_published_run"]["replay_precision"]["status"] == "report_baseline_verified"
    report.write_text('{"fixture":false}')
    third = refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert third["automation_layers"]["current_published_run"]["replay_precision"]["baseline_reproduction_verified"] is False
    assert json.dumps(registry, sort_keys=True) == before
