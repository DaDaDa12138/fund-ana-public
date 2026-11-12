"""Synthetic fixtures test audit routing; real fund evidence is kept outside tests."""
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import pytest

from backend.config import load_settings
from backend.project_formal_groups import FORMAL_GROUP_LABEL, FORMAL_SCORE_LABEL
from scripts import audit_core_publish_overlap as cli


@pytest.fixture
def audit_source(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                       database_path=tmp_path / "forbidden_current_database.db")
    directory = settings.reports_dir / "runs" / "fixture" / "revisions" / "revision_a"
    directory.mkdir(parents=True)
    files = {key: directory / filename for key, filename in (
        ("report_json", "report.json"), ("screening_input_snapshot_json_gz", "snapshot.json.gz"),
        ("filtered_csv", "formal.csv"), ("watch_csv", "watch.csv"),
        ("dynamic_filtered_csv", "dynamic.csv"), ("dynamic_watch_csv", "dynamic_watch.csv"),
    )}
    report = {"run_id": "fixture", "publication_control": {"revision": "revision_a", "mode": "diagnostic_only"},
              "artifacts": {key: str(path.relative_to(tmp_path)) for key, path in files.items()}}
    for key, path in files.items():
        path.write_text(json.dumps(report) if key == "report_json" else "synthetic input")
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["audit_core_publish_overlap.py", "--report", str(files["report_json"])])
    monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: pytest.fail("audit opened a current database"))
    return settings, report, files


def read_result(settings):
    files = list((settings.reports_dir / "audit/core_publish_overlap").glob("*.json"))
    assert len(files) == 1
    return json.loads(files[0].read_text())


def test_missing_typed_input_returns_failure_and_no_fake_zero_or_empty_exports(audit_source):
    settings, _, paths = audit_source
    paths["screening_input_snapshot_json_gz"].unlink()
    assert cli.main() == 2
    result = read_result(settings)
    assert result["passed"] is False and result["core_selected_total"] is None
    assert result["final_official_count"] is None and result["stage_rows"] is None
    assert result["artifacts"] == {}
    assert not list((settings.reports_dir / "audit/core_publish_overlap").glob("*.csv*"))


def test_corrupt_snapshot_signature_does_not_fall_back_to_current_data(audit_source):
    settings, _, paths = audit_source
    paths["screening_input_snapshot_json_gz"].write_bytes(gzip.compress(json.dumps({
        "schema_version": 1, "run_id": "fixture", "inputs": {}, "inputs_sha256": "bad signature",
    }).encode()))
    assert cli.main() == 2
    assert "signature_mismatch" in read_result(settings)["detail"]


def test_same_run_other_revision_is_not_accepted_as_current_input(audit_source):
    settings, report, paths = audit_source
    other = paths["report_json"].parent.parent / "revision_b"
    other.mkdir()
    (other / "snapshot.json.gz").write_bytes(paths["screening_input_snapshot_json_gz"].read_bytes())
    report["artifacts"]["screening_input_snapshot_json_gz"] = str((other / "snapshot.json.gz").relative_to(settings.project_root))
    paths["report_json"].write_text(json.dumps(report))
    assert cli.main() == 2
    assert "revision_mismatch" in read_result(settings)["detail"]


def test_failed_baseline_never_produces_overlap_counts(audit_source, monkeypatch):
    settings, _, _ = audit_source
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *args: ({"baseline_verified": False, "status": "mismatch"}, {}))
    assert cli.main() == 2
    result = read_result(settings)
    assert result["final_official_count"] is None and result["baseline_verification"]["status"] == "mismatch"


def test_source_change_during_replay_invalidates_result(audit_source, monkeypatch):
    settings, _, paths = audit_source

    def verifier(*args):
        paths["watch_csv"].write_text("changed synthetic input")
        return {"baseline_verified": True}, {}

    monkeypatch.setattr(cli, "verify_frozen_baseline", verifier)
    assert cli.main() == 2
    assert "source_changed" in read_result(settings)["detail"]


@pytest.mark.parametrize("flag", [None, "False", "True", 0, 1])
def test_unknown_or_untyped_core_membership_is_not_a_false_or_true_vote(flag):
    with pytest.raises(ValueError, match="selection_flag"):
        cli._load_core_selected_records({"000001": {"fcode": "000001", "final_mainlist_selected": flag}})


def test_known_zero_core_selection_is_valid_but_zero_denominator_ratio_is_missing():
    frame = cli._load_core_selected_records({"000001": {"fcode": "000001", "final_mainlist_selected": False}})
    assert frame.empty and "基金代码" in frame
    rows = cli._build_stage_rows(**{key: set() for key in (
        "total_core_codes", "in_project_universe_codes", "after_holding_gate_codes", "base_gate_pass_codes",
        "dynamic_pass_codes", "publish_formal_codes", "final_official_codes",
    )})
    assert all(row["passed_count"] == 0 and row["retained_ratio_vs_prev"] is None for row in rows)


@pytest.mark.parametrize("selected", [True, False])
def test_cli_uses_only_verified_frozen_frames_for_complete_stage_membership(audit_source, monkeypatch, selected):
    settings, report, _ = audit_source
    all_codes = [f"{index:06d}" for index in range(1, 9)]
    decisions = {code: {"fcode": code, "fund_name": f"Synthetic {code}", "fund_category": "fixture",
                        "final_mainlist_selected": selected} for code in all_codes}
    universe = pd.DataFrame({"基金代码": [code for code in all_codes if code != "000002"]})
    rows = []
    for code in all_codes:
        if code in {"000002", "000003"}:
            continue
        rows.append({"基金代码": code, "基础门禁通过": code != "000004",
                     "dynamic_threshold_verdict": "正式推荐" if code not in {"000004", "000005"} else "自动诊断",
                     "dynamic_threshold_reason": "synthetic", "基础推荐理由": "synthetic",
                     FORMAL_SCORE_LABEL: None if code == "000004" else 0.0})
    candidate = pd.DataFrame(rows)
    track = candidate.assign(项目推荐结论=lambda frame: frame["基金代码"].map(
        lambda code: "正式推荐" if code in {"000001", "000007", "000008"} else "自动诊断"), 推荐理由="synthetic")
    official = pd.DataFrame({"基金代码": ["000001", "000008"], FORMAL_GROUP_LABEL: ["fixture", "fixture"]})
    context = {"snapshot": {"inputs": {"decision_map": decisions}},
               "frames": {"scored_universe_before_holding_gate": universe},
               "result": {"candidate": candidate, "publish_track": track, "official": official}}
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *args: ({"baseline_verified": True, "status": "matched"}, context))
    assert cli.main() == 0
    result = read_result(settings)
    expected_counts = [8, 7, 6, 5, 4, 3, 2] if selected else [0] * 7
    assert result["passed"] and [row["passed_count"] for row in result["stage_rows"]] == expected_counts
    assert result["upstream_collection_and_core_risk_recalculation_verified"] is False
    assert result["source_publication_control"] == report["publication_control"]
    for record in result["artifacts"].values():
        path = settings.project_root / record["path"]
        assert path.name.endswith(".csv.gz") and record["format"] == "csv_gzip"
        raw = gzip.decompress(path.read_bytes())
        assert hashlib.sha256(raw).hexdigest() == record["uncompressed_sha256"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]
        assert len(raw) == record["uncompressed_bytes"] and path.stat().st_size == record["bytes"]
        restored = pd.read_csv(path, dtype={"基金代码": str})
        assert restored.shape == (record["rows"], record["columns"])
    compared = pd.read_csv(settings.project_root / result["artifacts"]["compare"]["path"], dtype={"基金代码": str})
    assert len(compared) == (8 if selected else 0)
    if selected:
        assert compared.loc[compared["基金代码"] == "000004", FORMAL_SCORE_LABEL].isna().all()
        assert compared.loc[compared["基金代码"] == "000001", FORMAL_SCORE_LABEL].iloc[0] == 0.0
    else:
        assert all(row["retained_ratio_vs_prev"] is None for row in result["stage_rows"])


def test_inconsistent_stage_membership_fails_without_exporting_counts(audit_source, monkeypatch):
    settings, _, _ = audit_source
    candidate = pd.DataFrame({"基金代码": ["000001"], "基础门禁通过": [False],
                              "dynamic_threshold_verdict": ["正式推荐"], FORMAL_SCORE_LABEL: [0.0]})
    context = {
        "snapshot": {"inputs": {"decision_map": {"000001": {"fcode": "000001", "final_mainlist_selected": True}}}},
        "frames": {"scored_universe_before_holding_gate": candidate},
        "result": {"candidate": candidate, "publish_track": candidate.assign(项目推荐结论="正式推荐"),
                   "official": candidate},
    }
    monkeypatch.setattr(cli, "verify_frozen_baseline", lambda *args: ({"baseline_verified": True}, context))
    assert cli.main() == 2
    result = read_result(settings)
    assert "stage_not_nested" in result["detail"]
    assert result["final_official_count"] is None and not result["passed"]
    assert not list((settings.reports_dir / "audit/core_publish_overlap").glob("*.csv*"))


def test_partial_final_stage_metadata_preserves_unknown_counts():
    summary = cli._extract_final_selection_stage_summary({"project_publish_summary": {
        "publish_reason_summary": {"final_stage": {"stage_key": "final_selection", "removed_count": 2,
            "top_reasons": [{"code": "final_selection_stability_cap", "count": None}]}}
    }})
    assert summary["input_count"] is None and summary["passed_count"] is None
    assert summary["removed_count"] == 2 and summary["stability_cap_count"] is None
    assert summary["stability_cap_removed_share"] is None and summary["stability_cap_dominant"] is None
