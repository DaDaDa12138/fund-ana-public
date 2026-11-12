"""Synthetic fixtures for source capture and unchanged incremental safety rules."""
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend import forward_validation as forward, report_builder as production
from backend import incremental_review_replay as incremental
from backend import screening_baseline_replay as baseline, screening_replay_snapshot as snapshot
from backend.config import load_settings
from backend.project_formal_groups import ENHANCED_BOND_GROUP_LABEL, FORMAL_GROUP_LABEL, FORMAL_SCORE_LABEL


def official_rows(count=1):
    return pd.DataFrame([{
        "基金代码": f"{index + 1:06d}", "基金名称": f"Synthetic fixture {index} C",
        FORMAL_GROUP_LABEL: ENHANCED_BOND_GROUP_LABEL, "债基风格标签": "二级债基",
        FORMAL_SCORE_LABEL: 50. + index, "1年-年化收益(%)": 1. + index,
        "1年-最大回撤(%)": .1 + index, "1年-波动率": .1 + index,
        "持有期状态": "无持有期", "项目准入方式": "增强债基补充准入", "项目质量信心": "中信心",
        "费率合计(%)": 1.2, "项目推荐结论": "正式推荐", "项目推荐原因代码": "",
        "推荐理由": "synthetic", "主原因分组": "", "原因分组汇总": "",
    } for index in range(count)])


def test_empty_export_aliases_remain_unique_through_review_and_frozen_snapshot():
    # A zero-row official selection can retain both database and display names.
    # The Oct14 failure occurred after those aliases were renamed and projected
    # into the incremental review capture, before a report could be committed.
    frame = pd.DataFrame({
        "fcode": pd.Series(dtype="string"), "基金代码": pd.Series(dtype="string"),
        "company_name": pd.Series(dtype="string"), "基金公司": pd.Series(dtype="string"),
        FORMAL_SCORE_LABEL: pd.Series(dtype="Float64"),
        "1年-年化收益(%)": pd.Series(dtype="Float64"),
    })
    original = frame.copy(deep=True)
    review = incremental.review_export_input(frame)
    assert review.empty and review.columns.is_unique
    assert list(review.columns).count("基金代码") == 1
    assert review["基金代码"].dtype == frame["fcode"].dtype
    assert review[FORMAL_SCORE_LABEL].dtype == frame[FORMAL_SCORE_LABEL].dtype
    pd.testing.assert_frame_equal(snapshot.restore_frame(snapshot.freeze_frame(review)), review, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)


def test_empty_schema_normalization_preserves_first_column_dtypes_and_index():
    frame = pd.DataFrame({"first": pd.Series(dtype="string"), "second": pd.Series(dtype="Int64"),
                          "score": pd.Series(dtype="Float64")})
    frame.columns = ["基金代码", "基金代码", "score"]
    frame.index = pd.Index([], name="source_row", dtype="int64")
    result = production._drop_duplicate_columns(frame)
    pd.testing.assert_frame_equal(result, frame.iloc[:, [0, 2]], check_exact=True)
    result["extra"] = pd.Series(dtype="boolean")
    assert "extra" not in frame


@pytest.fixture
def sources(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                       runs_dir=tmp_path / "reports/runs")
    records = []
    monkeypatch.setattr(forward, "visible_report_paths", lambda settings: list(records))
    monkeypatch.setattr(forward, "_resolve_publish_blockers", lambda value: value.get("fixture_blockers", []))

    def add(run="previous", generated="2026-09-11T08:00:00", nav="2026-09-10", codes=None, *, blocked=False):
        folder = settings.runs_dir / run
        folder.mkdir(parents=True)
        workbook = folder / "formal.xlsx"
        frame = pd.DataFrame({"基金代码": ["999001"] if codes is None else codes,
                              "基金名称": ["Synthetic old"] * (1 if codes is None else len(codes))})
        frame.to_excel(workbook, index=False)
        frame.to_csv(workbook.with_suffix(".csv"), index=False)
        report = folder / "report.json"
        report.write_text(json.dumps({"run_id": run, "generated_at": generated,
            "database_freshness": {"effective_nav_snapshot": {"candidate_date": nav}},
            "artifacts": {"filtered_excel": str(workbook.relative_to(tmp_path))},
            "publication_control": {"revision": "fixture-" + run},
            "counts": {"final_count": len(frame)}, "fixture_blockers": ["synthetic"] if blocked else []}))
        records.append(report)
        return workbook, report

    def capture(official=None):
        official = official_rows() if official is None else official
        current = incremental.review_export_input(official)
        options = dict(run_id="current", generated_at="2026-09-12T08:00:00", snapshot_date="2026-09-11",
                       filtered_excel=settings.runs_dir / "current/formal.xlsx", watch_excel=None,
                       final_count=len(current), stable_final_count=0, enhanced_final_count=len(current),
                       current_formal_df=current)
        captured = {}
        review = forward.build_incremental_review_frame(settings, **options, input_capture=captured)
        original = forward.build_incremental_review_frame(settings, **options)
        pd.testing.assert_frame_equal(review, original, check_exact=True)
        captured["demoted_codes"] = sorted(incremental.review_demoted_codes(official, review))
        frames = {name: snapshot.restore_frame(snapshot.freeze_frame(captured[key]))
                  for key, name in incremental.FRAME_NAMES.items() if key in captured}
        contract = {key: deepcopy(value) for key, value in captured.items() if key not in incremental.FRAME_NAMES}
        contract["report_generated_at"] = "2026-09-12T08:01:00"
        frozen = {"run_id": "current", "generated_at": contract["report_generated_at"], "snapshot_date": "2026-09-11",
                  "inputs": {"incremental_review_capture": contract}}
        return captured, frozen, frames, official

    return settings, records, add, capture


def test_capture_binds_actual_csv_bytes_and_previous_members_without_copying_workbook(sources, monkeypatch):
    _, _, add, capture = sources
    workbook, report = add()
    csv = workbook.with_suffix(".csv")
    expected_csv = csv.read_bytes()
    captured, frozen, frames, official = capture()
    assert captured["status"] == "captured" and captured["previous_member_count"] == 1
    assert captured["previous_frame_read"]["effective_source"]["sha256"] == hashlib.sha256(expected_csv).hexdigest()
    assert captured["previous_frame_read"]["effective_source"]["path"] == str(csv)
    assert captured["previous_snapshot"]["report_source"]["sha256"] == hashlib.sha256(report.read_bytes()).hexdigest()
    assert list(frames[incremental.FRAME_NAMES["previous_members"]].columns) == ["基金代码"]
    assert frames[incremental.FRAME_NAMES["previous_members"]]["基金代码"].tolist() == ["999001"]
    for path in (csv, workbook, report):
        path.unlink()
    monkeypatch.setattr(forward, "_load_snapshot_records", lambda *a, **kw: pytest.fail("read later snapshots"))
    monkeypatch.setattr(forward, "_load_formal_frame", lambda *a, **kw: pytest.fail("read later file"))
    check, history = incremental.verify_incremental_capture(frozen, frames, official)
    assert check["status"] == "matched" and check["available"] is True
    assert history["previous_codes"] == ["999001"]


def test_same_nav_latest_selection_and_future_blockers_remain_unchanged(sources):
    _, _, add, capture = sources
    add("older", "2026-09-10T08:00:00", codes=["999002"])
    add("previous", "2026-09-11T08:00:00")
    add("same_nav_as_current", "2026-09-12T07:00:00", "2026-09-11", ["999003"])
    add("future_time", "2026-09-13T08:00:00", "2026-09-10", ["999004"])
    add("future_nav", "2026-09-11T08:00:00", "2026-09-13", ["999005"])
    add("blocked", "2026-09-11T09:00:00", codes=["999006"], blocked=True)
    captured, frozen, frames, official = capture()
    assert [row["run_id"] for row in captured["selection"]["selected_snapshots"]] == ["current", "previous"]
    assert captured["previous_snapshot"]["revision"] == "fixture-previous"
    assert len(captured["selection"]["observed_report_sources"]) == 6
    assert incremental.verify_incremental_capture(frozen, frames, official)[0]["available"]


def test_capture_does_not_require_legacy_revision_metadata_that_the_reader_never_required(sources):
    _, _, add, capture = sources
    _, report = add()
    payload = json.loads(report.read_text())
    payload["publication_control"] = "synthetic legacy value"
    report.write_text(json.dumps(payload))
    captured, frozen, frames, official = capture()
    assert captured["previous_snapshot"]["revision"] is None
    assert incremental.verify_incremental_capture(frozen, frames, official)[0]["available"]


def test_no_previous_snapshot_is_explicit_and_not_an_invented_empty_history(sources):
    _, _, _, capture = sources
    captured, frozen, frames, official = capture(official_rows(5))
    assert captured["status"] == "no_previous_snapshot" and captured["previous_member_count"] is None
    assert incremental.FRAME_NAMES["previous_members"] not in frames
    check, history = incremental.verify_incremental_capture(frozen, frames, official)
    assert check["available"] and history["status"] == "no_previous_snapshot"
    output = incremental.apply_scenario_incremental_review({"incremental_review_history": history},
                                                          {"status": "computed", "official": official})
    assert output["incremental_review"].empty and len(output["official"]) == 5
    assert output["incremental_review_removed_codes"] == []


@pytest.mark.parametrize("fault", ["report", "members", "workbook"])
def test_unreadable_previous_input_is_distinct_from_no_previous_snapshot(sources, fault):
    _, _, add, capture = sources
    workbook, report = add()
    if fault == "report":
        report.write_text("invalid synthetic JSON")
    elif fault == "workbook":
        workbook.unlink()
    else:
        workbook.with_suffix(".csv").write_text("not_a_code\nsynthetic\n")
    captured, frozen, frames, official = capture()
    assert captured["status"] == "incomplete"
    check, history = incremental.verify_incremental_capture(frozen, frames, official)
    assert check["status"] == "incomplete" and not check["available"] and history is None
    output = incremental.apply_scenario_incremental_review({}, {"status": "computed", "official": official})
    assert output["status"] == "missing_frozen_incremental_review" and "official" not in output


def test_excel_fallback_retains_real_source_and_failed_csv_attempt(sources):
    _, _, add, capture = sources
    workbook, _ = add()
    workbook.with_suffix(".csv").write_bytes(b'code\n"unterminated')
    captured, frozen, frames, official = capture()
    observed = captured["previous_frame_read"]
    assert observed["sources"][0]["status"] == "unreadable"
    assert observed["effective_source"]["role"] == "formal_excel_main"
    assert observed["effective_source"]["sha256"] == hashlib.sha256(workbook.read_bytes()).hexdigest()
    assert incremental.verify_incremental_capture(frozen, frames, official)[0]["available"]


def test_empty_previous_membership_is_a_real_zero_with_a_bound_source(sources):
    _, _, add, capture = sources
    add(codes=[])
    captured, frozen, frames, official = capture()
    assert captured["status"] == "captured" and captured["previous_member_count"] == 0
    assert frames[incremental.FRAME_NAMES["previous_members"]].empty
    check, history = incremental.verify_incremental_capture(frozen, frames, official)
    assert check["available"] and history["previous_codes"] == []
    assert len(frames[incremental.FRAME_NAMES["review_result"]]) == 1


def test_source_hash_describes_consumed_bytes_even_if_path_changes_during_parse(sources, monkeypatch):
    _, _, add, _ = sources
    workbook, _ = add()
    path = workbook.with_suffix(".csv")
    original = path.read_bytes()
    reader = pd.read_csv

    def changed(source, **kwargs):
        result = reader(source, **kwargs)
        path.write_text("基金代码,基金名称\n999002,Synthetic replacement\n")
        return result

    monkeypatch.setattr(pd, "read_csv", changed)
    observed = {}
    frame = forward._load_formal_frame(workbook, read_capture=observed)
    assert frame["基金代码"].tolist() == ["999001"]
    assert observed["effective_source"]["sha256"] == hashlib.sha256(original).hexdigest()
    assert hashlib.sha256(path.read_bytes()).hexdigest() != observed["effective_source"]["sha256"]


def test_each_scenario_recomputes_its_own_percentiles_and_applies_the_existing_gate(sources, monkeypatch):
    _, _, add, capture = sources
    add()
    _, frozen, frames, official = capture()
    _, history = incremental.verify_incremental_capture(frozen, frames, official)
    original = frames[incremental.FRAME_NAMES["review_result"]]
    assert original.iloc[0]["评分分位(%)"] == 100.
    expanded = official_rows(5)
    before = expanded.copy(deep=True)
    monkeypatch.setattr(forward, "_load_snapshot_records", lambda *a, **kw: pytest.fail("read later cohort"))
    output = incremental.apply_scenario_incremental_review({"incremental_review_history": history},
        {"status": "computed", "official": expanded, "publish_track": expanded.copy()})
    row = output["incremental_review"].set_index("基金代码").loc["000001"]
    assert row["评分分位(%)"] == row["年化分位(%)"] == 20.
    assert row["即时体检结论"] == "需持续跟踪"
    assert output["incremental_review_removed_codes"] == ["000001"]
    assert output["official"]["基金代码"].tolist() == ["000002", "000003", "000004", "000005"]
    demoted = output["publish_track"].set_index("基金代码").loc["000001"]
    assert demoted["项目推荐结论"] != "正式推荐" and "incremental_safety_watch" in demoted["项目推荐原因代码"]
    pd.testing.assert_frame_equal(expanded, before, check_exact=True)
    assert original.iloc[0]["评分分位(%)"] == 100.


def test_original_members_are_not_misclassified_as_new_and_null_differs_from_zero(sources):
    _, _, add, capture = sources
    add(codes=["000001"])
    official = official_rows(5)
    official.loc[1, "1年-年化收益(%)"] = None
    official.loc[2, "1年-年化收益(%)"] = 0.
    _, frozen, frames, original = capture(official)
    check, history = incremental.verify_incremental_capture(frozen, frames, original)
    assert check["available"]
    result = incremental.replay_review(incremental.review_export_input(official), history).set_index("基金代码")
    assert "000001" not in result.index
    assert pd.isna(result.loc["000002", "年化分位(%)"])
    assert result.loc["000003", "1年-年化收益(%)"] == 0.
    assert pd.notna(result.loc["000003", "年化分位(%)"])


@pytest.mark.parametrize("fault", ["run", "report_time", "selection_time", "nav", "selected_order", "source_hash",
    "current_score", "review_value", "previous_duplicate", "previous_count", "demoted"])
def test_misaligned_capture_blocks_baseline_despite_available_input_files(sources, fault):
    _, _, add, capture = sources
    add()
    _, frozen, frames, official = capture()
    value = frozen["inputs"]["incremental_review_capture"]
    if fault == "run":
        value["run_id"] = "other"
    elif fault == "report_time":
        value["report_generated_at"] = "2026-09-13T08:00:00"
    elif fault == "selection_time":
        value["selection_generated_at"] = "2026-09-13T08:00:00"
    elif fault == "nav":
        value["snapshot_date"] = "2026-09-12"
    elif fault == "selected_order":
        value["selection"]["selected_snapshots"].reverse()
    elif fault == "source_hash":
        value["previous_snapshot"]["report_source"]["sha256"] = ""
    elif fault == "current_score":
        frames[incremental.FRAME_NAMES["current_input"]].loc[0, FORMAL_SCORE_LABEL] += 1
    elif fault == "review_value":
        frames[incremental.FRAME_NAMES["review_result"]].loc[0, "评分分位(%)"] = 0.
    elif fault == "previous_duplicate":
        key = incremental.FRAME_NAMES["previous_members"]
        frames[key] = pd.concat([frames[key], frames[key]], ignore_index=True)
        value["previous_member_count"] = 2
    elif fault == "previous_count":
        value["previous_member_count"] = 0
    else:
        value["demoted_codes"] = ["000001"]
    check, history = incremental.verify_incremental_capture(frozen, frames, official)
    assert check["status"] == "mismatch" and check["blocks_baseline"] and not check["available"]
    assert history is None


@pytest.mark.parametrize('ledger', [False, True])
def test_snapshot_writer_preserves_capture_types_values_and_report_binding(sources, monkeypatch, ledger):
    settings, _, add, capture = sources
    workbook, _ = add()
    if ledger:
        from backend import report_history
        from core.utils import report_history_store as store

        (settings.project_root / 'config').mkdir()
        (settings.project_root / 'config/project.yaml').write_text('report_history:\n  read_mode: ledger\n')
        settings = replace(settings, report_history_database_path=settings.project_root / 'backend/data/report_history.sqlite')
        report_history.import_source(settings, workbook, {'formal'})
    captured, _, _, official = capture(official_rows(5))
    revision = settings.runs_dir / "current/revisions/one"
    revision.mkdir(parents=True)
    summary = {"run_id": "current", "generated_at": "2026-09-12T08:01:00",
               "artifacts": {"report_json": str((revision / "report.json").relative_to(settings.project_root))}}
    filters = {key: official for key in ("candidate_df", "dynamic_publish_official_df", "final_selection_tracker_features_df",
                                       "final_selection_contraction_df", "dynamic_official_df")}
    filters.update(style_thresholds={}, global_thresholds={}, style_meta={}, effective_nav_candidate_date="2026-09-11")
    monkeypatch.setattr(snapshot, "source_code_hashes", lambda root: {"synthetic.py": "fixture"})
    path = snapshot.write_screening_input_snapshot(settings, summary, universe=official, filters=filters,
        materialized={"official_df": official, "incremental_review_capture": captured},
        report_inputs={"decision_map": {}, "soft_warning_map": {}})
    frozen = snapshot.load_screening_input_snapshot(path, expected_run_id="current")
    frames = {name: snapshot.restore_frame(value) for name, value in frozen["inputs"]["frames"].items()}
    for key, name in incremental.FRAME_NAMES.items():
        pd.testing.assert_frame_equal(frames[name], captured[key], check_exact=True)
    contract = frozen["inputs"]["incremental_review_capture"]
    assert not contract["previous_snapshot"]["report_source"]["path"].startswith("/")
    if ledger:
        proof = contract['previous_frame_read']['effective_source']['history_record']
        store.verify_embedded(proof)
        workbook.unlink()
        workbook.with_suffix('.csv').unlink()
        report_history.database(settings).unlink()
    assert incremental.verify_incremental_capture(frozen, frames, official)[0]["available"]
    frozen["inputs"]["frames"][incremental.FRAME_NAMES["previous_members"]]["rows"][0][0] = "000001"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(frozen, stream, allow_nan=False)
    with pytest.raises(ValueError, match="signature_mismatch"):
        snapshot.load_screening_input_snapshot(path)


def test_capture_verification_is_additional_to_all_nine_original_baseline_checks(sources, monkeypatch):
    settings, _, add, capture = sources
    add()
    _, frozen, frames, official = capture()
    frozen["inputs"].update(configuration={}, decision_map={}, soft_warning_map={}, style_thresholds={}, global_thresholds={}, style_meta={})
    frames.update({key: official.copy() for key in ("scored_universe_before_holding_gate", "candidate_after_thresholds", "post_review",
        "formal_after_all_filters", "dynamic_reference", "final_selection_tracker_features", "final_selection_contraction")})
    frozen["inputs"]["frames"] = {key: snapshot.freeze_frame(value) for key, value in frames.items()}
    frozen.update(inputs_sha256="synthetic", code_sha256={})
    path = settings.project_root / "synthetic.gz"
    path.write_bytes(b"synthetic fixture; loader is replaced")
    report = {"run_id": frozen["run_id"], "generated_at": frozen["generated_at"], "artifacts": {"screening_input_snapshot_json_gz": str(path)}}
    monkeypatch.setattr(baseline, "load_screening_input_snapshot", lambda *a, **kw: frozen)
    monkeypatch.setattr(baseline, "source_code_hashes", lambda root: {})
    monkeypatch.setattr(baseline, "load_actual_autostock_status_map", lambda *a, **kw: {})
    monkeypatch.setattr(production, "_assign_project_scores", lambda frame: frame.copy())
    monkeypatch.setattr(baseline, "run_report_selection", lambda **kw: {
        "status": "computed", **{key: official.copy() for key in ("candidate", "post_review", "official", "dynamic", "contraction")},
        "style_thresholds": {}, "global_thresholds": {}, "style_meta": {}})
    result, context = baseline.verify_frozen_baseline(settings, report)
    assert len(result["checks"]) == 9 and all(row["aligned"] for row in result["checks"])
    assert result["baseline_verified"] and context["incremental_review_history"]
    frozen["inputs"]["incremental_review_capture"]["demoted_codes"] = ["000001"]
    result, context = baseline.verify_frozen_baseline(settings, report)
    assert len(result["checks"]) == 9 and all(row["aligned"] for row in result["checks"])
    assert not result["baseline_verified"] and result["failed_check_count"] == 1
    assert context["incremental_review_history"] is None


@pytest.mark.parametrize("entrypoint", ["strategy", "holding"])
def test_shared_scenario_entrypoints_apply_verified_incremental_history(sources, monkeypatch, entrypoint):
    from backend import frozen_strategy_audit as strategy, holding_period_gate_shadow as holding

    settings, _, add, capture = sources
    add()
    _, frozen, frames, official = capture()
    _, history = incremental.verify_incremental_capture(frozen, frames, official)
    expanded = official_rows(5)
    context = {"snapshot": {"snapshot_date": frozen["snapshot_date"], "inputs": {"decision_map": {}, "soft_warning_map": {}}},
               "settings": settings, "autostock_status_map": {}, "evaluation_times": {},
               "incremental_review_history": history,
               "frames": {"scored_universe_before_holding_gate": expanded, "final_selection_tracker_features": pd.DataFrame(),
                          "formal_after_all_filters": official, "dynamic_reference": official}}
    selected = {"status": "computed", "official": expanded, "publish_track": expanded.copy(), "candidate": expanded,
                "post_review": expanded, "dynamic": expanded, "style_thresholds": {}, "global_thresholds": {}}
    if entrypoint == "strategy":
        monkeypatch.setattr(strategy, "run_report_selection", lambda **kwargs: dict(selected))
        result = strategy.run_frozen_strategy_scenario(context, scored_universe=expanded)
        assert result["status"] == "computed" and result["incremental_review_removed_codes"] == ["000001"]
        assert len(result["official"]) == 4
    else:
        proof = {"status": "matched", "baseline_verified": True, "evaluation_times": {}, "time_reference_source": "synthetic"}
        monkeypatch.setattr(holding, "verify_frozen_baseline", lambda *args: (proof, context))
        monkeypatch.setattr(holding, "run_report_selection", lambda **kwargs: dict(selected))
        monkeypatch.setattr(production, "_assign_project_scores", lambda frame: frame.copy())
        result, exports = holding.build_holding_period_gate_shadow(settings, {"run_id": "synthetic"})
        assert result["passed"] and result["scenario_without_gate"]["final_count"] == 4
        assert result["scenario_without_gate"]["incremental_review_removed_codes"] == ["000001"]
        assert len(exports["scenario_incremental_review"]) == 5


def test_ledger_capture_is_independent_and_rejects_embedded_corruption(sources,monkeypatch):
    from backend import report_history
    from core.utils import report_history_store as store
    settings,_,add,capture=sources
    workbook,_=add()
    (settings.project_root/'config').mkdir()
    (settings.project_root/'config/project.yaml').write_text('report_history:\n  read_mode: ledger\n')
    settings=replace(settings,report_history_database_path=settings.project_root/'backend/data/report_history.sqlite')
    report_history.import_source(settings,workbook,{'formal'})
    captured,frozen,frames,official=capture()
    proof=frozen['inputs']['incremental_review_capture']['previous_frame_read']['effective_source']['history_record']
    assert proof['contract']==store.CONTRACT
    workbook.unlink();workbook.with_suffix('.csv').unlink();report_history.database(settings).unlink()
    assert incremental.verify_incremental_capture(frozen,frames,official)[0]['available']
    proof['payload']['rows'][0][0]='000000'
    assert incremental.verify_incremental_capture(frozen,frames,official)[0]['status']=='mismatch'
