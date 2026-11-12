"""Synthetic membership fixtures verify capture/replay; no fund business claims."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from backend import project_tracker, report_builder, screening_baseline_replay as replay
from backend import screening_replay_snapshot as snapshots
from backend.config import load_settings
from backend.selection_tracker_history import replay_selection_tracker_features


@pytest.fixture
def source(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                       runs_dir=tmp_path / "reports/runs")
    records = []

    def add(run_id, generated_at, registry_date, actual_date, codes, *, publishable=True):
        directory = settings.runs_dir / run_id
        directory.mkdir(parents=True)
        workbook = directory / "formal.xlsx"
        pd.DataFrame({"基金代码": codes, "最新净值日期": [actual_date] * len(codes)}).to_excel(workbook, index=False)
        report_path = directory / "report.json"
        report_path.write_text(json.dumps({"run_id": run_id, "generated_at": generated_at,
            "counts": {"final_count": len(codes)}, "artifacts": {"filtered_excel": str(workbook.relative_to(tmp_path))}}))
        record = {"run_id": run_id, "publishable": publishable, "report_path": str(report_path),
                  "snapshot": {"candidate_date": registry_date}}
        records.append(record)
        return workbook, report_path, record

    add("first", "2026-05-19T10:00:00", "2026-05-19", "2026-05-18", ["000001", "000002"])
    add("revised", "2026-05-20T10:00:00", "2026-05-20", "2026-05-18", ["000002", "000003"])
    add("same_nav", "2026-05-21T10:00:00", "2026-05-21", "2026-05-20", ["000003"])
    monkeypatch.setattr(report_builder, "build_publication_registry", lambda *a: {"canonical_runs": records})
    current = pd.DataFrame({"基金代码": ["000001", "000002"], "最新净值日期": ["2026-05-20"] * 2})
    identity = {"current_run_id": "current", "current_generated_at": "2026-05-22T11:00:00",
                "current_nav_snapshot_date": "2026-05-22"}

    def capture(frame=None):
        basis = {}
        frame = current if frame is None else frame
        features = report_builder._build_final_selection_tracker_features(
            settings, **identity, current_df=frame, history_capture=basis)
        frozen = {"run_id": identity["current_run_id"], "generated_at": identity["current_generated_at"],
                  "snapshot_date": identity["current_nav_snapshot_date"], "inputs": {"selection_tracker_history": basis}}
        # The writer attaches the actual summary timestamp after selection.
        basis["report_generated_at"] = frozen["generated_at"]
        frames = {"post_review": frame, "final_selection_tracker_features": features}
        return basis, frozen, frames

    return settings, records, add, capture


def test_captures_actual_sources_and_matches_existing_features_before_expansion(source, monkeypatch):
    settings, _, _, capture = source
    basis, frozen, frames = capture()
    original = report_builder._build_final_selection_tracker_features(settings, current_run_id=frozen["run_id"],
        current_generated_at=frozen["generated_at"], current_nav_snapshot_date=frozen["snapshot_date"],
        current_df=frames["post_review"])
    pd.testing.assert_frame_equal(frames["final_selection_tracker_features"], original, check_exact=True)
    assert basis["status"] == "complete" and len(basis["observations"]) == 3
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"] is True
    before = deepcopy(basis)
    # A replay must not require current files or a newer registry/API/database.
    for row in basis["observations"]:
        Path(row["source_path"]).unlink()
    monkeypatch.setattr(report_builder, "_load_project_report_history", lambda *a, **k: pytest.fail("read later history"))
    entrant = pd.DataFrame({"基金代码": ["000002", "000003", "000004"], "最新净值日期": ["2026-05-20"] * 3})
    actual = replay_selection_tracker_features(basis, entrant).set_index("基金代码")
    assert actual["跟踪总期数"].tolist() == [2, 2, 2]
    assert actual["出现次数"].tolist() == [2, 2, 1]
    assert actual["出现频率(%)"].tolist() == [100., 100., 50.]
    assert actual["当前是否在榜"].tolist() == ["是"] * 3
    assert basis == before
    assert set(basis["current_observation"]["member_codes"]) == {"000001", "000002"}


def test_registry_exclusions_and_snapshot_supersession_are_recorded(source):
    _, _, add, capture = source
    add("older_same_registry_date", "2026-05-21T09:00:00", "2026-05-21", "2026-05-19", ["000005"])
    add("future", "2026-05-23T10:00:00", "2026-05-23", "2026-05-22", ["000006"])
    add("diagnostic", "2026-05-21T09:00:00", "2026-05-19", "2026-05-18", ["000007"], publishable=False)
    basis, frozen, frames = capture()
    statuses = {row["run_id"]: row["outcome"] for row in basis["selection"]["records"]}
    assert statuses["older_same_registry_date"] == "superseded_snapshot"
    assert statuses["future"] == "after_current_run"
    assert statuses["diagnostic"] == "not_publishable"
    assert basis["selection"]["selected_history_run_ids"] == ["first", "revised", "same_nav"]
    assert replay.verify_selection_tracker_capture(frozen, frames)["status"] == "matched"


@pytest.mark.parametrize("fault", ["missing_report", "invalid_report", "missing_workbook", "missing_date", "run_mismatch"])
def test_unavailable_registry_evidence_cannot_be_called_complete_history(source, fault):
    settings, records, _, capture = source
    report = Path(records[0]["report_path"])
    if fault == "missing_report":
        report.unlink()
    elif fault == "invalid_report":
        report.write_text("not json")
    elif fault == "missing_workbook":
        (settings.runs_dir / "first/formal.xlsx").unlink()
    elif fault == "missing_date":
        records[0]["snapshot"]["candidate_date"] = ""
    else:
        payload = json.loads(report.read_text())
        payload["run_id"] = "different"
        report.write_text(json.dumps(payload))
    basis, frozen, frames = capture()
    check = replay.verify_selection_tracker_capture(frozen, frames)
    assert basis["status"] == check["status"] == "incomplete"
    assert check["available"] is False and check["blocks_baseline"] is False
    assert check["source_issues"]
    with pytest.raises(ValueError, match="incomplete"):
        replay_selection_tracker_features(basis, frames["post_review"])


def test_workbook_changes_between_read_and_hash_are_not_silently_certified(source, monkeypatch):
    _, _, _, capture = source
    reader = report_builder._load_tracker_source_frame

    def change(path):
        value = reader(path)
        with path.open("ab") as stream:
            stream.write(b"synthetic concurrent change")
        return value

    monkeypatch.setattr(report_builder, "_load_tracker_source_frame", change)
    basis, frozen, frames = capture()
    assert basis["status"] == "incomplete"
    assert all(row["source_verified_unchanged"] is False for row in basis["observations"])
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"] is False


@pytest.mark.parametrize("fault", ["wrong_run", "wrong_time", "wrong_report_time", "wrong_snapshot", "missing_observation", "wrong_order",
    "unbound_path", "invalid_sha", "duplicate_member", "wrong_current", "wrong_feature", "changed_membership"])
def test_malformed_or_misaligned_capture_blocks_baseline_verification(source, fault):
    _, _, _, capture = source
    basis, frozen, frames = capture()
    if fault == "wrong_run":
        basis["run_id"] = "other"
    elif fault == "wrong_time":
        basis["generated_at"] = "2026-05-23T11:00:00"
    elif fault == "wrong_report_time":
        basis["report_generated_at"] = "2026-05-23T11:00:00"
    elif fault == "wrong_snapshot":
        basis["registry_nav_snapshot_date"] = "2026-05-23"
    elif fault == "missing_observation":
        basis["observations"].pop()
    elif fault == "wrong_order":
        basis["observations"].reverse()
    elif fault == "unbound_path":
        basis["observations"][0]["source_path"] = "other.xlsx"
    elif fault == "invalid_sha":
        basis["observations"][0]["source_sha256"] = ""
    elif fault == "duplicate_member":
        basis["observations"][0]["member_codes"].append("000001")
    elif fault == "wrong_current":
        basis["current_observation"]["member_codes"].append("000004")
    elif fault == "wrong_feature":
        frames["final_selection_tracker_features"].loc[0, "出现次数"] = None
    else:
        basis["observations"][1]["member_codes"].append("000001")
    result = replay.verify_selection_tracker_capture(frozen, frames)
    assert result["status"] == "mismatch" and result["blocks_baseline"] is True
    assert result["available"] is False


def test_no_past_publication_is_distinct_from_history_not_collected(source):
    _, records, _, capture = source
    records.clear()
    basis, frozen, frames = capture()
    assert basis["status"] == "complete" and basis["observations"] == []
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"] is True
    assert frames["final_selection_tracker_features"]["出现次数"].tolist() == [1, 1]
    basis, frozen, frames = capture(pd.DataFrame(columns=["基金代码", "最新净值日期"]))
    assert basis["status"] == "not_collected"
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"] is False


def test_selection_cutoff_and_later_summary_time_are_preserved_separately(source):
    _, _, _, capture = source
    basis, frozen, frames = capture()
    frozen["generated_at"] = "2026-05-22T11:04:00"
    basis["report_generated_at"] = frozen["generated_at"]
    check = replay.verify_selection_tracker_capture(frozen, frames)
    assert check["available"] is True
    assert check["selection_generated_at"] == "2026-05-22T11:00:00"
    assert check["report_generated_at"] == "2026-05-22T11:04:00"
    basis["generated_at"] = None
    assert replay.verify_selection_tracker_capture(frozen, frames)["blocks_baseline"] is True


def test_old_snapshots_keep_missing_inputs_and_real_zero_features(source):
    _, _, _, capture = source
    _, frozen, frames = capture()
    del frozen["inputs"]["selection_tracker_history"]
    frames["final_selection_tracker_features"].loc[0, "出现次数"] = 0
    frames["final_selection_tracker_features"].loc[0, "出现频率(%)"] = 0.
    check = replay.verify_selection_tracker_capture(frozen, frames)
    assert check == {"status": "not_recorded", "available": False, "blocks_baseline": False}
    entrants = pd.DataFrame({"基金代码": ["000001", "000003"]})
    old = replay.scenario_tracker_features({"frames": frames}, entrants)
    assert old.loc[0, "出现次数"] == 0
    assert replay.missing_tracker_history_codes(entrants, old) == ["000003"]


def test_missing_nav_uses_scenario_content_not_an_invented_observation_date(source):
    settings, records, add, capture = source
    records.clear()
    add("undated", "2026-05-19T10:00:00", "2026-05-19", "", ["000001"])
    frame = pd.DataFrame({"基金代码": ["000001"], "最新净值日期": [None]})
    basis, frozen, frames = capture(frame)
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"] is True
    assert frames["final_selection_tracker_features"].iloc[0]["跟踪总期数"] == 1
    expanded = pd.DataFrame({"基金代码": ["000001", "000002"], "最新净值日期": [None, None]})
    actual = replay_selection_tracker_features(basis, expanded)
    expected = report_builder._build_final_selection_tracker_features(settings, current_run_id=frozen["run_id"],
        current_generated_at=frozen["generated_at"], current_nav_snapshot_date=frozen["snapshot_date"], current_df=expanded)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert actual["出现次数"].tolist() == [2, 1] and actual["跟踪总期数"].tolist() == [2, 2]
    assert basis["current_observation"]["nav_date"] == ""


def test_snapshot_hash_covers_history_membership_and_source_provenance(source, monkeypatch):
    settings, _, _, capture = source
    basis, frozen, frames = capture()
    directory = settings.runs_dir / frozen["run_id"]
    directory.mkdir()
    current = frames["post_review"]
    filters = {key: current for key in ("candidate_df", "dynamic_publish_official_df", "final_selection_contraction_df", "dynamic_official_df")}
    filters.update(final_selection_tracker_features_df=frames["final_selection_tracker_features"],
                   selection_tracker_history=basis, style_thresholds={}, global_thresholds={}, style_meta={},
                   effective_nav_candidate_date=frozen["snapshot_date"])
    monkeypatch.setattr(snapshots, "source_code_hashes", lambda root: {"synthetic": "fixture"})
    del basis["report_generated_at"]
    summary = {"run_id": frozen["run_id"], "generated_at": "2026-05-22T11:04:00",
               "artifacts": {"report_json": str((directory / "report.json").relative_to(settings.project_root))}}
    path = snapshots.write_screening_input_snapshot(settings, summary, universe=current, filters=filters,
        materialized={"official_df": current}, report_inputs={"decision_map": {}, "soft_warning_map": {}})
    payload = snapshots.load_screening_input_snapshot(path)
    assert payload["inputs"]["selection_tracker_history"] == {**basis, "report_generated_at": summary["generated_at"]}
    assert "report_generated_at" not in basis
    payload["inputs"]["selection_tracker_history"]["observations"][0]["member_codes"].append("000004")
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, allow_nan=False)
    with pytest.raises(ValueError, match="signature_mismatch"):
        snapshots.load_screening_input_snapshot(path)


@pytest.mark.parametrize("status", [None, "unknown", "not_checked"])
def test_shared_selection_uses_verified_history_but_keeps_autostock_gaps(source, monkeypatch, status):
    settings, _, _, capture = source
    # This test exercises the legacy provider contract explicitly. The live
    # project provider is Eastmoney cross-endpoint validation.
    settings = replace(settings, autostock_enabled=True, publication_validation_provider="autostock")
    basis, frozen, frames = capture()
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"]
    current = pd.DataFrame({"基金代码": ["000001", "000003"], "最新净值日期": ["2026-05-20"] * 2,
        "has_holding_period": [0, 0], "holding_days": [0, 0], "基础门禁通过": [True, True],
        "项目推荐结论": ["正式推荐"] * 2})
    for name in ("_append_project_decision_columns", "_apply_fixed_quality_thresholds", "_apply_dynamic_quality_thresholds",
                 "_apply_enhanced_supplemental_gate", "_materialize_track_frame", "_apply_publish_post_filters"):
        monkeypatch.setattr(report_builder, name, lambda frame, **kw: frame.copy())
    monkeypatch.setattr(report_builder, "_compute_dynamic_style_thresholds", lambda frame: ({}, {}, {}))
    seen = []
    monkeypatch.setattr(report_builder, "_apply_final_official_selection",
        lambda frame, **kw: seen.append(kw["tracker_features_df"].copy()) or frame.copy())
    monkeypatch.setattr(report_builder, "_annotate_final_selection_removed_rows", lambda frame: frame.copy())
    result = replay.run_report_selection(settings=settings, scored_universe=current, decision_map={}, soft_warning_map={},
        tracker_features=frames["final_selection_tracker_features"],
        autostock_status_map={code: {"AutoStock原始校验状态": status} for code in ("000001", "000003")},
        evaluation_times={"decision": datetime(2026, 5, 22), "dynamic_post_review": datetime(2026, 5, 22)},
        selection_tracker_history=basis, observed_unvalidated_autostock_codes={"000001"})
    assert result["status"] == "missing_autostock_observations"
    assert result["missing_autostock_codes"] == (["000001", "000003"] if status is None else ["000003"])
    assert "official" not in result
    assert seen[0].set_index("基金代码").loc["000003", "出现次数"] == 2


@pytest.mark.parametrize("status,original_result", [("strict_match", False), ("unknown", False),
    ("not_checked", False), ("unknown", True), ("not_checked", True)])
def test_annual_materializer_returns_the_entrant_features_for_downstream_analysis(source, monkeypatch, status, original_result):
    from scripts import audit_annual_return_gate_shadow as annual

    settings, _, _, capture = source
    settings = replace(settings, autostock_enabled=True, publication_validation_provider="autostock")
    basis, frozen, frames = capture()
    assert replay.verify_selection_tracker_capture(frozen, frames)["available"]
    candidate = pd.DataFrame([{"基金代码": "000003", "基金名称": "Synthetic Bond A", "债基风格标签": "综合纯债",
        "最新净值日期": "2026-05-20",
        "dynamic_threshold_reason_codes": "below_annual_floor", "dynamic_threshold_verdict": "自动诊断",
        "dynamic_threshold_watch_bucket": "动态质量诊断", "dynamic_threshold_reason": "年化收益不足",
        "项目推荐结论": "正式推荐", "推荐理由": "synthetic", "项目推荐原因代码": "", "项目质量信心": "高信心",
        "项目准入方式": "动态阈值准入"}])
    for name in ("apply_enhanced_supplemental_gate", "_materialize_track_frame", "_apply_publish_post_filters"):
        monkeypatch.setattr(annual, name, lambda frame, **kw: frame.copy())
    seen = []
    monkeypatch.setattr(annual, "_apply_final_official_selection",
        lambda frame, **kw: seen.append(kw["tracker_features_df"].copy()) or frame.copy())
    monkeypatch.setattr(annual, "_build_final_selection_tracker_features", lambda *a, **kw: pytest.fail("read current Tracker"))
    context = {"frames": frames, "selection_tracker_history": basis,
               "observed_unvalidated_autostock_codes": {"000003"} if original_result else set(),
               "evaluation_times": {"dynamic_post_review": datetime(2026, 5, 22)}}
    arguments = dict(
        candidate_df=candidate, style_thresholds={}, global_thresholds={}, run_id=frozen["run_id"],
        generated_at=frozen["generated_at"], snapshot_date=frozen["snapshot_date"],
        autostock_status_map={"000003": {"AutoStock原始校验状态": status}},
        supplement_runtime_autostock=False, frozen_context=context)
    if status != "strict_match" and not original_result:
        with pytest.raises(annual.FrozenAnnualScenarioIncomplete) as error:
            annual._materialize_shadow_publish_result(settings, **arguments)
        assert error.value.status == "missing_autostock_observations"
        assert error.value.missing_codes == ["000003"]
        return
    official, _, _, _, features = annual._materialize_shadow_publish_result(settings, **arguments)
    assert official["基金代码"].tolist() == features["基金代码"].tolist() == ["000003"]
    assert features.iloc[0]["出现次数"] == 2 and features.iloc[0]["出现频率(%)"] == 100.
    pd.testing.assert_frame_equal(features, seen[0], check_exact=True)
