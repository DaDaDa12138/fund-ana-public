"""Synthetic files exercise evidence validation, never fund performance."""
from copy import deepcopy
from dataclasses import replace
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest
import requests

from backend import low_vol_overlap_audit as reader
from backend.api_payloads.positive_ratio import normalize_low_vol_positive_ratio_shadow_watch
from backend.config import load_settings
from scripts.audit_low_vol_annual_positive_ratio_overlap import LOW_VOL_RELEASE_SCENARIOS, LOW_VOL_STYLE_SCOPES


def digest(value):
    return hashlib.sha256(value).hexdigest()


def write_csv(path, rows, fields):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    raw = stream.getvalue().encode("utf-8-sig")
    path.write_bytes(gzip.compress(raw, mtime=0))
    return {"sha256": digest(path.read_bytes()), "bytes": path.stat().st_size,
            "uncompressed_sha256": digest(raw), "uncompressed_bytes": len(raw), "rows": len(rows), "columns": len(fields)}


def export_audit(settings, path, audit):
    artifacts = {}
    def export(name, rows, fields):
        target = path.with_name(f"{path.stem}_{name}.csv.gz")
        artifacts[name] = {"path": str(target.relative_to(settings.project_root)), **write_csv(target, rows, fields)}
    if audit["status"] == "baseline_verified":
        export("baseline_members", [{"基金代码": "000001"}], ["基金代码"])
    else:
        scenarios = audit["scenario_impacts"]
        fields = sorted({key for row in scenarios for key in row})
        export("scenario_impacts", scenarios, fields)
        seen = set()
        for row in scenarios:
            if row["release_id"] in seen:
                continue
            seen.add(row["release_id"])
            if row["status"] == "computed":
                codes = ({"000001"} - set(row["removed_codes"])) | set(row["added_codes"])
                export(f"release_{row['release_id']}_members", [{"基金代码": code} for code in sorted(codes)], ["基金代码"])
            else:
                codes = row[reader.MISSING_INPUTS[row["status"]]]
                export(f"release_{row['release_id']}_missing_inputs", [{"基金代码": code, "缺失阶段": "Tracker历史"} for code in codes],
                       ["基金代码", "缺失阶段"])
    audit["artifacts"] = artifacts
    path.write_text(json.dumps(audit, ensure_ascii=False))


@pytest.fixture
def evidence(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports")
    directory = settings.reports_dir / "runs/fixture/revisions/a"
    directory.mkdir(parents=True)
    for name in (*reader.AUDIT_CODE_FILES, "backend/replay_fixture.py"):
        file = tmp_path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("# Synthetic audit-code fixture.\n")
    files = {key: directory / (key + (".json" if key == "report_json" else ".gz" if "snapshot" in key else ".csv"))
             for key in reader.SOURCE_KEYS}
    report = {"run_id": "fixture", "publication_control": {"revision": "a"},
              "artifacts": {key: str(file.relative_to(tmp_path)) for key, file in files.items()},
              "project_publish_summary": {"low_vol_positive_ratio_shadow_watch": {
                  "available": True, "focus_sample_count": 3, "monthly_shadow_ready_count": 3}}}
    for key, file in files.items():
        file.write_text(json.dumps(report, ensure_ascii=False) if key == "report_json" else "基金代码\n000001\n")
    sources = {key: {"path": str(file.relative_to(tmp_path)), "sha256": digest(file.read_bytes())} for key, file in files.items()}
    baseline = {"run_id": "fixture", "status": "matched", "baseline_verified": True, "failed_check_count": 0,
                "checks": [{"name": name, "aligned": True} for name in sorted(reader.BASELINE_CHECKS)],
                "snapshot_path": sources["screening_input_snapshot_json_gz"]["path"],
                "snapshot_sha256": sources["screening_input_snapshot_json_gz"]["sha256"],
                "source_files": {key: value for key, value in sources.items() if "snapshot" not in key},
                "replay_code_sha256": {"backend/replay_fixture.py": digest((tmp_path / "backend/replay_fixture.py").read_bytes())},
                "inputs_sha256": digest(b"synthetic frozen input"),
                "later_database_reads": False, "missing_history_imputation": False, "wall_clock_used_for_replayed_gates": False}
    definition = reader.build_low_vol_audit_definition(
        settings, baseline=baseline, configuration={"exclude_holding_days_gt": 90},
        style_scopes=LOW_VOL_STYLE_SCOPES, scenarios=LOW_VOL_RELEASE_SCENARIOS,
    )
    targets_by_scenario = {
        "all_dynamic_blocked_in_scope": ["000002", "000003", "000004"],
        "positive_ratio_only": ["000002"], "annual_return_plus_score": ["000003"],
        "contains_annual_return_without_positive_ratio": ["000003"],
        "contains_positive_ratio_without_annual_return": ["000002", "000004"],
    }
    rows, scopes = [], []
    for scope in reader.SCOPE_KEYS:
        nonempty = scope in {"style_mid_short", "style_low_vol_focus"}
        scopes.append({"scope_key": scope, "dynamic_blocked_count": 3 if nonempty else 0,
                       "annual_return_hit_count": 1 if nonempty else 0, "positive_ratio_hit_count": 2 if nonempty else 0})
        for scenario in reader.SCENARIO_KEYS:
            targets = targets_by_scenario.get(scenario, []) if nonempty else []
            rows.append({"scope_key": scope, "scenario_key": scenario, "status": "computed",
                         "targeted_count": len(targets), "target_codes": targets,
                         "release_id": digest(json.dumps(targets, separators=(",", ":")).encode())[:20],
                         "formal_count": 1 + len(targets), "direct_final_impact_count": len(targets),
                         "added_official_count": len(targets), "removed_official_count": 0,
                         "net_final_count_delta": len(targets), "added_codes": targets,
                         "removed_codes": [], "direct_final_codes": targets})
    audit = {"schema_version": 3, "input_contract": "frozen-report-strategy-2026-09-11.1",
             "generated_at": "2026-01-01T00:00:00", "run_id": "fixture",
             "source_publication_control": {"revision": "a"}, "status": "computed", "passed": True,
             "source_binding": {"run_id": "fixture", "revision": "a", "files": sources},
             "source_verified_after_replay": True, "baseline_verification": baseline,
             "baseline_final_count": 1, "focus_style_dynamic_blocked_count": 3,
             "later_database_reads": False, "current_upstream_queries": False,
             "wall_clock_used_for_replayed_gates": False, "production_formulas_changed": False,
             "strategy_comparison_allowed": True, "scenario_impacts": rows, "focus_scope_summary": scopes,
             "requested_scenario_count": 44, "completed_scenario_count": 44, "audit_definition": definition}
    audit_dir = settings.reports_dir / "audit" / reader.AUDIT_NAME
    audit_dir.mkdir(parents=True)
    path = audit_dir / f"{reader.AUDIT_NAME}_fixture_20260101_000000.json"
    export_audit(settings, path, audit)
    for module in (sqlite3, sqlite3.dbapi2):
        monkeypatch.setattr(module, "connect", lambda *a, **kw: pytest.fail("reader queried SQL"))
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("reader requested an upstream API"))
    return settings, report, audit, path, files


def test_verified_reader_preserves_real_zero_and_does_not_add_overlapping_experiments(evidence):
    settings, report, _, path, files = evidence
    before = {str(file): file.read_bytes() for file in (*files.values(), path)}
    watch = normalize_low_vol_positive_ratio_shadow_watch(report["project_publish_summary"], report_identity=report, settings=settings)
    overlap = watch["low_vol_overlap_summary"]
    assert overlap["available"] and overlap["report_binding"]["status"] == "matched"
    assert overlap["annual_positive_direct_final_impact_count"] == 0
    assert watch["semantics"]["positive_ratio_independent_direct_final_impact_count"] == 2
    assert watch["semantics"]["positive_ratio_only_direct_final_impact_count"] == 1
    assert overlap["scenario_coverage"]["completed_count"] == 44
    assert overlap["strategy_effectiveness_verified"] is False
    assert before == {name: Path(name).read_bytes() for name in before}


def make_partial(audit):
    for row in audit["scenario_impacts"]:
        if row["target_codes"]:
            row.update(status="missing_tracker_history", missing_tracker_history_codes=list(row["target_codes"]))
            row.update(dict.fromkeys(reader.OUTCOME_FIELDS))
    audit.update(status="scenario_inputs_incomplete", passed=False, strategy_comparison_allowed=False,
                 completed_scenario_count=sum(row["status"] == "computed" for row in audit["scenario_impacts"]))


def test_partial_and_baseline_only_are_not_completed_strategy_results(evidence):
    settings, report, audit, path, _ = evidence
    make_partial(audit)
    export_audit(settings, path, audit)
    watch = normalize_low_vol_positive_ratio_shadow_watch(report["project_publish_summary"], report_identity=report, settings=settings)
    overlap = watch["low_vol_overlap_summary"]
    assert overlap["status"] == "scenario_inputs_incomplete", overlap
    assert overlap["report_binding"]["status"] == "matched" and not overlap["available"]
    assert overlap["scenario_coverage"]["completed_nonempty_target_count"] == 0
    assert all(overlap[key] is None for key in reader.OVERLAP_COUNT_FIELDS)
    assert "34/44" in watch["headline"] and watch["semantics"]["production_gate_change_required"] is None
    audit.update(status="baseline_verified", passed=True, scenario_impacts=[], requested_scenario_count=0, completed_scenario_count=0)
    export_audit(settings, path, audit)
    result = reader.load_low_vol_overlap_for_report(settings, report)
    assert result["status"] == "baseline_only" and not result["available"]


@pytest.mark.parametrize("mode", ["summary", "detail"])
@pytest.mark.parametrize("partial", [False, True])
def test_recommendation_payloads_read_the_configured_revision_evidence(evidence, mode, partial):
    from backend.api_payloads import recommendations

    settings, report, audit, path, files = evidence
    if partial:
        make_partial(audit)
        export_audit(settings, path, audit)
    expected = normalize_low_vol_positive_ratio_shadow_watch(
        report["project_publish_summary"], report_identity=report, settings=settings,
    )
    original_report = deepcopy(report)
    original_files = {str(file): file.read_bytes() for file in (*files.values(), path)}
    kwargs = {
        "settings": settings,
        "normalize_portfolio_groups": lambda value: value,
        "build_portfolio_group_brief": lambda value: {},
        "build_publish_funnel_summary": lambda value: {},
    }
    if mode == "summary":
        result = recommendations.build_recommendations_summary_response(report, **kwargs)
        assert result["payload_scope"]["mode"] == "summary"
        assert "funds" not in result and "rows" not in result
    else:
        result = recommendations.build_recommendations_response(
            report, **kwargs,
            load_tracker_occurrence_map_fn=lambda value: {},
            overlay_portfolio_occurrence_metrics_fn=lambda value, occurrences: value,
            apply_since_inception_annualized_return_fn=lambda value: value,
        )
    actual = result["low_vol_positive_ratio_shadow_watch"]
    for key in ("available", "status", "source_revision", "report_binding", "scenario_coverage"):
        assert actual["low_vol_overlap_summary"][key] == expected["low_vol_overlap_summary"][key]
    assert actual["low_vol_overlap_summary"]["report_binding"]["status"] == "matched"
    for key in reader.OVERLAP_COUNT_FIELDS:
        assert actual["low_vol_overlap_summary"][key] == expected["low_vol_overlap_summary"][key]
    semantics = result["positive_ratio_semantics"]
    for key in ("strategy_materiality_status", "production_gate_change_required",
                "positive_ratio_independent_direct_final_impact_count"):
        assert semantics[key] == expected["semantics"][key]
    assert report == original_report
    assert original_files == {name: Path(name).read_bytes() for name in original_files}


@pytest.mark.parametrize("damage", [
    "report_sha", "snapshot_sha", "missing_source", "cross_revision_source", "baseline_flag", "baseline_check",
    "audit_definition", "code_changed", "replay_code_changed", "config_missing", "input_binding", "schema", "policy", "input_contract",
    "matrix_missing", "matrix_duplicate", "target_duplicate", "target_count", "outcome_count", "partition",
    "terminal", "partial_fake_success", "export_bytes", "export_logical_bytes", "export_path", "summary_vs_export",
])
def test_bad_evidence_fails_closed_without_old_embedded_fallback(evidence, damage):
    settings, report, audit, path, files = evidence
    if damage == "report_sha": audit["source_binding"]["files"]["report_json"]["sha256"] = "0" * 64
    elif damage == "snapshot_sha": files["screening_input_snapshot_json_gz"].write_bytes(b"changed")
    elif damage == "missing_source": files["watch_csv"].unlink()
    elif damage == "cross_revision_source":
        audit["source_binding"]["files"]["watch_csv"]["path"] = "../outside.csv"
    elif damage == "baseline_flag": audit["baseline_verification"]["baseline_verified"] = False
    elif damage == "baseline_check": audit["baseline_verification"]["checks"].pop()
    elif damage == "audit_definition": audit["audit_definition"]["sha256"] = "0" * 64
    elif damage == "code_changed": (settings.project_root / reader.AUDIT_CODE_FILES[0]).write_text("# changed\n")
    elif damage == "replay_code_changed": (settings.project_root / "backend/replay_fixture.py").write_text("# changed\n")
    elif damage in {"config_missing", "input_binding"}:
        definition = audit["audit_definition"]["definition"]
        definition["frozen_configuration" if damage == "config_missing" else "frozen_inputs_sha256"] = {} if damage == "config_missing" else "0" * 64
        audit["audit_definition"]["sha256"] = reader._digest(definition)
    elif damage == "schema": audit["schema_version"] = 2
    elif damage == "input_contract": audit["input_contract"] = "unknown"
    elif damage == "policy":
        audit["audit_definition"]["definition"]["scenarios"][0]["mode"] = "unverified_policy"
        audit["audit_definition"]["sha256"] = reader._digest(audit["audit_definition"]["definition"])
    elif damage == "matrix_missing": audit["scenario_impacts"].pop()
    elif damage == "matrix_duplicate": audit["scenario_impacts"][-1] = deepcopy(audit["scenario_impacts"][0])
    elif damage == "target_duplicate": audit["scenario_impacts"][0]["target_codes"].append("000002")
    elif damage == "target_count": audit["scenario_impacts"][0]["targeted_count"] = True
    elif damage == "outcome_count": audit["scenario_impacts"][0]["formal_count"] += 1
    elif damage == "partition": audit["focus_scope_summary"][0]["positive_ratio_hit_count"] = 0
    elif damage == "terminal": audit["scenario_impacts"][0]["status"] = "success"
    elif damage == "partial_fake_success":
        make_partial(audit); audit.update(status="computed", passed=True, completed_scenario_count=44)
    elif damage == "export_bytes": (settings.project_root / audit["artifacts"]["scenario_impacts"]["path"]).write_bytes(b"changed")
    elif damage == "export_logical_bytes": audit["artifacts"]["scenario_impacts"]["uncompressed_sha256"] = "0" * 64
    elif damage == "export_path": audit["artifacts"]["scenario_impacts"]["path"] = report["artifacts"]["watch_csv"]
    elif damage == "summary_vs_export":
        for row in audit["scenario_impacts"]:
            row.update(formal_count=1, direct_final_impact_count=0, added_official_count=0, net_final_count_delta=0,
                       direct_final_codes=[], added_codes=[])
    path.write_text(json.dumps(audit, ensure_ascii=False))
    report["project_publish_summary"]["low_vol_positive_ratio_shadow_watch"]["low_vol_overlap_summary"] = {
        "available": True, "passed": True, **{key: 99 for key in reader.OVERLAP_COUNT_FIELDS}}
    result = normalize_low_vol_positive_ratio_shadow_watch(report["project_publish_summary"], report_identity=report, settings=settings)
    overlap = result["low_vol_overlap_summary"]
    assert overlap["status"] == "invalid_audit_evidence" and not overlap["available"], overlap
    assert all(overlap[key] is None for key in reader.OVERLAP_COUNT_FIELDS)
    assert result["semantics"]["strategy_materiality_status"] == "unknown"


def test_newer_other_revision_does_not_displace_matching_audit_but_corruption_does(evidence):
    settings, report, audit, path, _ = evidence
    later = path.with_name(path.name.replace("000000", "000001"))
    other = deepcopy(audit)
    other["source_binding"]["revision"] = "b"
    other["source_publication_control"]["revision"] = "b"
    later.write_text(json.dumps(other))
    assert reader.load_low_vol_overlap_for_report(settings, report)["available"]
    later.write_text("{")
    assert reader.load_low_vol_overlap_for_report(settings, report)["status"] == "invalid_audit_evidence"
    make_partial(audit)
    export_audit(settings, later, audit)
    result = reader.load_low_vol_overlap_for_report(settings, report)
    assert result["status"] == "scenario_inputs_incomplete" and not result["available"]
    later.unlink()
    path.unlink()
    result = reader.load_low_vol_overlap_for_report(settings, report)
    assert result["status"] == "no_same_revision_audit" and not result["available"]


def test_unrelated_runs_do_not_invalidate_current_report_cache(evidence):
    settings, _, _, path, _ = evidence
    token = reader.low_vol_overlap_cache_token(settings, "fixture")
    unrelated = path.with_name(path.name.replace("_fixture_", "_another_"))
    unrelated.write_text("{}")
    assert reader.low_vol_overlap_cache_token(settings, "fixture") == token


@pytest.mark.parametrize("target", ["audit", "source", "export", "code"])
def test_cache_token_changes_on_content_mutation_and_removal(evidence, target):
    settings, _, audit, path, files = evidence
    file = {"audit": path, "source": files["watch_csv"], "code": settings.project_root / reader.AUDIT_CODE_FILES[0],
            "export": settings.project_root / audit["artifacts"]["scenario_impacts"]["path"]}[target]
    original = reader.low_vol_overlap_cache_token(settings, "fixture")
    original_stat = Path.stat
    frozen_stat = file.stat()
    raw = file.read_bytes()
    file.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    # A same-size rewrite can retain the observed timestamps. Content must
    # invalidate the cache even when every stat field appears unchanged.
    with patch.object(Path, "stat", lambda path, *a, **kw: frozen_stat if path == file else original_stat(path, *a, **kw)):
        changed = reader.low_vol_overlap_cache_token(settings, "fixture")
    assert original != changed
    file.unlink()
    assert reader.low_vol_overlap_cache_token(settings, "fixture") != changed


@pytest.mark.parametrize("mode", ["compact", "scoped"])
def test_dashboard_cache_sees_later_audit_without_publication_change(evidence, mode):
    import backend.app as app
    settings, report, audit, path, _ = evidence
    cache = {}
    def cache_response(key, ttl, builder):
        if key not in cache:
            cache[key] = builder()
        return cache[key]
    def payload_response(payload, *args, **kwargs):
        return normalize_low_vol_positive_ratio_shadow_watch(
            payload["project_publish_summary"], report_identity=payload, settings=kwargs["settings"])
    with (
        patch.object(app, "settings", settings),
        patch.object(app, "_runtime_scheduler_settings", return_value=settings),
        patch.object(app, "_load_current_published_report_cached", return_value=report),
        patch.object(app, "_load_publication_registry_snapshot_cached", return_value={}),
        patch.object(app, "_load_publication_registry_cached", return_value={}),
        patch.object(app, "_load_scheduler_state_snapshot_cached", return_value={}),
        patch.object(app, "_scheduler_state_semantic_cache_token", return_value="unchanged"),
        patch.object(app, "_final_selection_contraction_shadow_cache_token", return_value="unchanged"),
        patch.object(app, "_cached_payload", side_effect=cache_response),
        patch.object(app.dashboard_payloads, "build_latest_dashboard_response", side_effect=payload_response),
    ):
        build = app._build_latest_dashboard_compact_cached if mode == "compact" else lambda: app._build_latest_dashboard_scoped_detail_cached("reports")
        assert build()["low_vol_overlap_summary"]["available"]
        make_partial(audit)
        export_audit(settings, path, audit)
        result = build()["low_vol_overlap_summary"]
        assert result["status"] == "scenario_inputs_incomplete" and not result["available"]
        assert len(cache) == 2
