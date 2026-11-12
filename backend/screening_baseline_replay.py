"""Replay report selection from frozen inputs without querying a later database.

This verifies the report selection layer. It does not claim to reproduce the
upstream collection, NAV repair, risk calculation or the nine core screenings.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from utils.core_csv_storage import open_original_artifact
from backend import report_builder as production
from backend.incremental_review_replay import apply_scenario_incremental_review, verify_incremental_capture
from backend.publish_pipeline import apply_exported_autostock_status_map
from backend.report_inputs import load_actual_autostock_status_map, resolve_runtime_artifact_path
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS, validation_status_value
from backend.screening_replay_snapshot import load_screening_input_snapshot, restore_frame, source_code_hashes
from backend.selection_tracker_history import (
    CONTRACT as TRACKER_CAPTURE_CONTRACT, FEATURE_COLUMNS as TRACKER_FEATURE_COLUMNS,
    replay_selection_tracker_features, validate_selection_history,
)

SCORE_FIELDS = ("综合加权得分", "近期得分", "长期得分", "综合评分", "正式投资评分")
DECISION_FIELDS = (
    "基础门禁通过", "fixed_threshold_verdict", "fixed_threshold_reason",
    "dynamic_threshold_verdict", "dynamic_threshold_reason", "dynamic_threshold_watch_bucket",
    "项目质量信心", "项目准入方式", "项目推荐原因代码",
)
TRACKER_HISTORY_FIELDS = ("出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数")


def missing_tracker_history_codes(candidates: pd.DataFrame, features: pd.DataFrame) -> list[str]:
    """A row without its observed values is not frozen history, including nulls."""
    required = _codes(candidates)
    if not required:
        return []
    if "基金代码" not in features or not features.columns.is_unique:
        return sorted(required)
    current = features.assign(_code=features["基金代码"].map(production._normalize_code))
    duplicate = set(current.loc[current["_code"].duplicated(keep=False), "_code"])
    known = {}
    for row in current.to_dict(orient="records"):
        code = row.pop("_code")
        if code not in required or code in duplicate:
            continue
        if any(_missing(row.get(field)) or (isinstance(row.get(field), str) and not row[field].strip())
               for field in TRACKER_HISTORY_FIELDS):
            continue
        numeric = pd.to_numeric(pd.Series([row[field] for field in TRACKER_HISTORY_FIELDS if field != "当前是否在榜"]), errors="coerce")
        if not all(math.isfinite(value) for value in numeric):
            continue
        known[code] = row
    return sorted(required - set(known))


def _validation_provider(settings: Any | None) -> str:
    if settings is None:
        return "autostock"
    provider = str(getattr(settings, "publication_validation_provider", "autostock") or "autostock").strip()
    if getattr(settings, "autostock_enabled", True) is False and provider == "autostock":
        return "disabled"
    return provider or "autostock"


def missing_validation_observation_codes(
    codes: set[str], statuses: dict[str, dict[str, str]], *,
    observed_unvalidated_codes: set[str] | None = None,
    settings: Any | None = None,
) -> list[str]:
    """Return missing comparisons for the active publication provider.

    AutoStock is a legacy provider and is not consulted when disabled or
    replaced.  A current replacement provider still needs a complete frozen
    observation before a new counterfactual candidate can be called validated.

    A watch-only unknown/not_checked row has no report supplement outcome.
    Production would try to validate it if a counterfactual selected it. The
    original official/dynamic results may still reproduce their recorded
    degraded states without claiming that those states were successful checks.
    """
    provider = _validation_provider(settings)
    if provider == "disabled":
        return []
    compared_statuses = {"strict_match", "soft_pass_with_warnings", "hard_block"}
    observed = observed_unvalidated_codes or set()
    missing = []
    for code in codes:
        observation = statuses.get(code) or {}
        if provider != "autostock":
            source = str(observation.get("数据校验来源") or "").strip()
            if source != provider:
                missing.append(code)
                continue
            observed_status = production._normalize_autostock_status(observation.get("数据校验原始状态"))
            if observed_status in compared_statuses:
                continue
            if code in observed and observed_status in {"not_checked", "unknown"}:
                continue
            missing.append(code)
            continue
        neutral = any(column in observation for column in DATA_VALIDATION_COLUMNS)
        if neutral and (not observation.get("数据校验来源") or not observation.get("数据校验契约版本")
                        or not observation.get("数据校验原始状态")):
            missing.append(code)
            continue
        observed_status = validation_status_value(observation)
        status = "unknown" if observed_status == "unknown" else production._normalize_autostock_status(observed_status)
        if status in compared_statuses:
            continue
        if code in observed and status in {"not_checked", "unknown"}:
            continue
        missing.append(code)
    return sorted(missing)


def missing_autostock_observation_codes(
    codes: set[str], statuses: dict[str, dict[str, str]], *,
    observed_unvalidated_codes: set[str] | None = None,
    settings: Any | None = None,
) -> list[str]:
    """Backward-compatible name for callers still using the legacy helper."""
    return missing_validation_observation_codes(
        codes,
        statuses,
        observed_unvalidated_codes=observed_unvalidated_codes,
        settings=settings,
    )


def resolve_evaluation_times(frozen: dict[str, Any]) -> tuple[dict[str, datetime], str]:
    """Use the report's observed clock, never the time at which replay is run."""
    inputs = frozen.get("inputs") or {}
    stages = ("decision", "fixed_post_review", "dynamic_post_review")
    if "evaluation_times" in inputs:
        raw = inputs["evaluation_times"]
        if not isinstance(raw, dict) or any(stage not in raw for stage in stages):
            raise ValueError("replay_evaluation_times_incomplete")
        source = "frozen_stage_timestamps"
    else:
        raw = {stage: frozen.get("generated_at") for stage in stages}
        source = "legacy_snapshot_generated_at"
    result = {}
    for stage in stages:
        value = raw[stage]
        if not isinstance(value, str) or "T" not in value:
            raise ValueError("replay_evaluation_time_missing_or_invalid")
        try:
            timestamp = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("replay_evaluation_time_missing_or_invalid") from exc
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
        result[stage] = timestamp
    return result, source


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open_original_artifact(path) as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _codes(frame: pd.DataFrame) -> set[str]:
    return set(frame.get("基金代码", pd.Series(dtype=str)).map(production._normalize_code)) - {""}


def _missing(value: Any) -> bool:
    return value is None or bool(pd.isna(value))


def _equal(left: Any, right: Any, *, tolerance: float = 1e-10) -> bool:
    if _missing(left) or _missing(right):
        return _missing(left) and _missing(right)
    if isinstance(left, (float, int)) and isinstance(right, (float, int)):
        return math.isclose(left, right, rel_tol=tolerance, abs_tol=tolerance)
    return bool(left == right)


def compare_frame(name: str, expected: pd.DataFrame, actual: pd.DataFrame, fields: list[str] | tuple[str, ...]) -> dict[str, Any]:
    expected_codes, actual_codes = _codes(expected), _codes(actual)
    missing_fields = [field for field in fields if field in expected and field not in actual]
    fields = [field for field in fields if field in expected and field in actual and field != "基金代码"]
    duplicate = len(expected_codes) != len(expected.index) or len(actual_codes) != len(actual.index)
    differences: list[dict[str, Any]] = []
    difference_count = 0
    max_numeric_delta = 0.0
    if not duplicate and expected_codes and actual_codes:
        left = expected.assign(_code=expected["基金代码"].map(production._normalize_code)).set_index("_code")
        right = actual.assign(_code=actual["基金代码"].map(production._normalize_code)).set_index("_code")
        for code in sorted(expected_codes & actual_codes):
            for field in fields:
                a, b = left.at[code, field], right.at[code, field]
                if not _equal(a, b):
                    difference_count += 1
                    if len(differences) < 20:
                        differences.append({"fcode": code, "field": field,
                                            "expected": None if _missing(a) else str(a),
                                            "actual": None if _missing(b) else str(b)})
                if isinstance(a, (float, int)) and isinstance(b, (float, int)) and math.isfinite(a) and math.isfinite(b):
                    max_numeric_delta = max(max_numeric_delta, abs(float(a) - float(b)))
    return {"name": name, "aligned": not (duplicate or expected_codes != actual_codes or missing_fields or difference_count),
            "expected_count": len(expected.index), "actual_count": len(actual.index),
            "added_codes": sorted(actual_codes - expected_codes), "removed_codes": sorted(expected_codes - actual_codes),
            "duplicate_or_empty_codes": duplicate, "missing_fields": missing_fields,
            "compared_fields": list(fields), "value_difference_count": difference_count,
            "max_numeric_delta": max_numeric_delta, "difference_samples": differences}


def _compare_mapping(name: str, expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    # Production already persists threshold rounding; preserve it exactly.
    return {"name": name, "aligned": expected == actual,
            "expected": expected, "actual": actual}


def verify_selection_tracker_capture(frozen: dict[str, Any], frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Keep original feature replay separate from coverage for new candidates."""
    inputs = frozen["inputs"]
    if "selection_tracker_history" not in inputs:
        return {"status": "not_recorded", "available": False, "blocks_baseline": False}
    history = inputs["selection_tracker_history"]
    try:
        if (not isinstance(history, dict) or history.get("contract") != TRACKER_CAPTURE_CONTRACT
                or (history.get("run_id"), history.get("report_generated_at"), history.get("registry_nav_snapshot_date"))
                != (frozen["run_id"], frozen["generated_at"], frozen["snapshot_date"])):
            raise ValueError("selection_tracker_history_report_identity_mismatch")
        selection_clock, _ = resolve_evaluation_times({"generated_at": history.get("generated_at"), "inputs": {}})
        report_clock, _ = resolve_evaluation_times({"generated_at": frozen["generated_at"], "inputs": {}})
        if selection_clock["decision"] > report_clock["decision"]:
            raise ValueError("selection_tracker_history_selection_after_report")
        if "evaluation_times" in inputs:
            stage_clocks, _ = resolve_evaluation_times(frozen)
            if selection_clock["decision"] > min(stage_clocks.values()):
                raise ValueError("selection_tracker_history_selection_after_gate")
        if history.get("status") in {"incomplete", "not_collected"}:
            return {"status": history["status"], "available": False, "blocks_baseline": False,
                    "reason": "Captured sources are incomplete; only original saved feature rows remain usable",
                    "source_issues": (history.get("selection") or {}).get("issues", [])}
        validate_selection_history(history, run_id=frozen["run_id"], generated_at=history["generated_at"],
                                   snapshot_date=frozen["snapshot_date"], original_current=frames["post_review"])
        rebuilt = replay_selection_tracker_features(history, frames["post_review"])
        original = frames["final_selection_tracker_features"]
        if any(column not in original for column in TRACKER_FEATURE_COLUMNS):
            raise ValueError("selection_tracker_history_original_features_missing")
        comparison = compare_frame("selection_tracker_history_features", original, rebuilt, TRACKER_FEATURE_COLUMNS)
        return {"status": "matched" if comparison["aligned"] else "mismatch", "available": comparison["aligned"],
                "blocks_baseline": not comparison["aligned"], "feature_comparison": comparison,
                "historical_source_count": len(history["observations"]),
                "selection_generated_at": history["generated_at"], "report_generated_at": frozen["generated_at"],
                "current_membership_scope": history["current_observation"]["membership_scope"],
                "later_source_reads": False, "missing_history_imputation": False}
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        return {"status": "mismatch", "available": False, "blocks_baseline": True, "reason": str(exc)}


def scenario_tracker_features(context: dict[str, Any], current_df: pd.DataFrame) -> pd.DataFrame:
    history = context.get("selection_tracker_history")
    if history is not None:
        return replay_selection_tracker_features(history, current_df)
    return context["frames"]["final_selection_tracker_features"].copy()


def prepare_selection_candidates(
    *, settings: Any, scored_universe: pd.DataFrame, decision_map: dict[str, Any],
    soft_warning_map: dict[str, Any], evaluation_times: dict[str, datetime],
    apply_holding_gate: bool = True,
    dynamic_threshold_override: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None,
    dynamic_release_codes: set[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Shared preselection for replay and prospective observation planning.

    These candidates have not passed publication or AutoStock gates. They are
    never a final list, including when used to plan a counterfactual experiment.
    """
    universe = scored_universe.copy()
    from core.utils.holding_period_policy import HOLDING_PERIOD_LIMIT, holding_period_gate_reason

    universe["需排除长持有期产品"] = universe.apply(
        lambda row: holding_period_gate_reason(
            has_holding_period=row.get("has_holding_period"), holding_days=row.get("holding_days"),
            holding_period_source=row.get("holding_period_source"), holding_keywords=row.get("holding_keywords"),
            fund_name=production.combine_holding_period_names(
                row.get("shortname"), row.get("fullname"), row.get("基金名称"), row.get("fund_name")),
            threshold_days=min(90, settings.exclude_holding_days_gt),
        ) == HOLDING_PERIOD_LIMIT, axis=1,
    )
    candidate = (universe.loc[~universe["需排除长持有期产品"]] if apply_holding_gate else universe).copy()
    candidate = production._append_project_decision_columns(
        candidate, decision_map=decision_map, soft_warning_map=soft_warning_map,
        evaluated_at=evaluation_times["decision"],
    )
    candidate = production._apply_fixed_quality_thresholds(candidate)
    if dynamic_threshold_override is None:
        styles, global_thresholds, style_meta = production._compute_dynamic_style_thresholds(candidate.loc[candidate["基础门禁通过"]].copy())
    else:
        # Only the threshold research caller supplies this explicit alternative.
        # Eligibility, as-of clocks and final selection still use the same path.
        styles, global_thresholds, style_meta = dynamic_threshold_override
    candidate = production._apply_dynamic_quality_thresholds(candidate, style_thresholds=styles,
                                                            global_thresholds=global_thresholds, style_meta=style_meta)
    if dynamic_release_codes:
        # This counterfactual releases only the dynamic thresholds. Base,
        # holding-period, enhanced, post-review and final safety gates remain.
        targets = {production._normalize_code(code) for code in dynamic_release_codes}
        eligible = candidate["基础门禁通过"]
        eligible_codes = _codes(candidate.loc[eligible])
        if not targets <= eligible_codes or "" in targets:
            raise ValueError("dynamic_release_targets_not_base_eligible")
        release = candidate["基金代码"].map(production._normalize_code).isin(targets)
        candidate.loc[release, "dynamic_threshold_verdict"] = "正式推荐"
        for column in ("dynamic_threshold_watch_bucket", "dynamic_threshold_reason", "dynamic_threshold_reason_codes"):
            candidate.loc[release, column] = ""
    candidate = production._apply_enhanced_supplemental_gate(candidate)
    return candidate, styles, global_thresholds, style_meta


def run_report_selection(
    *, settings: Any, scored_universe: pd.DataFrame, decision_map: dict[str, Any],
    soft_warning_map: dict[str, Any], tracker_features: pd.DataFrame,
    autostock_status_map: dict[str, dict[str, str]],
    evaluation_times: dict[str, datetime],
    apply_holding_gate: bool = True,
    dynamic_threshold_override: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None,
    dynamic_release_codes: set[str] | None = None,
    selection_tracker_history: dict[str, Any] | None = None,
    observed_unvalidated_autostock_codes: set[str] | None = None,
    observed_unvalidated_validation_codes: set[str] | None = None,
) -> dict[str, Any]:
    candidate, styles, global_thresholds, style_meta = prepare_selection_candidates(
        settings=settings, scored_universe=scored_universe, decision_map=decision_map,
        soft_warning_map=soft_warning_map, evaluation_times=evaluation_times,
        apply_holding_gate=apply_holding_gate, dynamic_threshold_override=dynamic_threshold_override,
        dynamic_release_codes=dynamic_release_codes,
    )
    track = production._materialize_track_frame(candidate, verdict_column="dynamic_threshold_verdict",
                                               watch_bucket_column="dynamic_threshold_watch_bucket", reason_column="dynamic_threshold_reason")
    dynamic = track.loc[track["项目推荐结论"].eq("正式推荐")].copy()
    publish_track = production._apply_publish_post_filters(track, threshold_mode="dynamic", style_thresholds=styles,
                                                           global_thresholds=global_thresholds,
                                                           evaluated_at=evaluation_times["dynamic_post_review"])
    post_review = publish_track.loc[publish_track["项目推荐结论"].eq("正式推荐")].copy()
    if selection_tracker_history is not None:
        tracker_features = replay_selection_tracker_features(selection_tracker_history, post_review)
    missing_history = missing_tracker_history_codes(post_review, tracker_features)
    if missing_history:
        return {"status": "missing_tracker_history", "missing_tracker_history_codes": sorted(missing_history),
                "candidate": candidate, "post_review": post_review, "tracker_features": tracker_features, "style_thresholds": styles,
                "global_thresholds": global_thresholds, "style_meta": style_meta}
    official = production._apply_final_official_selection(post_review, settings=settings, tracker_features_df=tracker_features)
    removed = post_review.loc[~post_review["基金代码"].map(production._normalize_code).isin(_codes(official))].copy()
    removed = production._annotate_final_selection_removed_rows(removed)
    missing_status = missing_validation_observation_codes(
        _codes(official) | _codes(dynamic), autostock_status_map,
        observed_unvalidated_codes=(observed_unvalidated_validation_codes
                                     if observed_unvalidated_validation_codes is not None
                                     else observed_unvalidated_autostock_codes),
        settings=settings,
    )
    if missing_status:
        provider = _validation_provider(settings)
        missing_status_name = "missing_autostock_observations" if provider == "autostock" else "missing_validation_observations"
        return {"status": missing_status_name, "missing_validation_codes": sorted(missing_status),
                "missing_autostock_codes": sorted(missing_status) if provider == "autostock" else [],
                "candidate": candidate, "post_review": post_review, "tracker_features": tracker_features, "style_thresholds": styles,
                "global_thresholds": global_thresholds, "style_meta": style_meta}
    official = apply_exported_autostock_status_map(official, autostock_status_map=autostock_status_map)
    allow_legacy_autostock = _validation_provider(settings) == "autostock"
    official, blocked = production._demote_autostock_hard_block_rows(
        official, allow_legacy_autostock=allow_legacy_autostock,
    )
    dynamic = apply_exported_autostock_status_map(dynamic, autostock_status_map=autostock_status_map)
    dynamic, _ = production._demote_autostock_hard_block_rows(
        dynamic, allow_legacy_autostock=allow_legacy_autostock,
    )
    if not blocked.empty:
        removed = pd.concat([removed, blocked], ignore_index=True)
    official = production._refresh_recommendation_notes(official)
    contraction = production._build_final_selection_contraction_explainability(
        post_review, official_df=official, removed_after_final_selection_df=removed,
        tracker_features_df=tracker_features, settings=settings,
    )
    return {"status": "computed", "candidate": candidate, "publish_track": publish_track, "post_review": post_review,
            "official": official, "dynamic": dynamic, "contraction": contraction,
            "tracker_features": tracker_features,
            "style_thresholds": styles, "global_thresholds": global_thresholds, "style_meta": style_meta}


def verify_frozen_baseline(settings: Any, report: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    artifacts = report.get("artifacts") or {}
    path = resolve_runtime_artifact_path(artifacts.get("screening_input_snapshot_json_gz"), project_root=settings.project_root)
    if path is None or not path.is_file():
        return {"status": "inputs_missing", "baseline_verified": False,
                "reason": "No typed as-of snapshot; later database reads cannot establish historical replay"}, {}
    frozen = load_screening_input_snapshot(path, expected_run_id=report["run_id"])
    try:
        evaluation_times, time_reference_source = resolve_evaluation_times(frozen)
    except ValueError as exc:
        return {"status": "evaluation_time_missing_or_invalid", "baseline_verified": False,
                "failed_check_count": 1, "reason": str(exc), "run_id": report["run_id"]}, {}
    inputs = frozen["inputs"]
    frames = {key: restore_frame(value) for key, value in inputs["frames"].items()}
    tracker_capture_check = verify_selection_tracker_capture(frozen, frames)
    replay_settings = replace(settings, **inputs["configuration"])
    observed_universe = frames["scored_universe_before_holding_gate"]
    # Rank/score the complete observed universe before the holding-period gate.
    scored = production._assign_project_scores(observed_universe)
    checks = [compare_frame("complete_scoring_universe", observed_universe, scored, SCORE_FIELDS)]
    statuses = load_actual_autostock_status_map(artifacts, project_root=settings.project_root)
    observed_unvalidated_codes = {
        code for code in _codes(frames["formal_after_all_filters"]) | _codes(frames["dynamic_reference"])
        if validation_status_value(statuses.get(code) or {}) in {"not_checked", "unknown"}
    }
    result = run_report_selection(settings=replay_settings, scored_universe=scored, decision_map=inputs["decision_map"],
                                  soft_warning_map=inputs["soft_warning_map"], tracker_features=frames["final_selection_tracker_features"],
                                  autostock_status_map=statuses, evaluation_times=evaluation_times,
                                  observed_unvalidated_validation_codes=observed_unvalidated_codes)
    incremental_capture_check, incremental_history = verify_incremental_capture(
        frozen, frames, result.get("official") if result["status"] == "computed" else None,
    )
    if incremental_history is not None:
        result = apply_scenario_incremental_review({"incremental_review_history": incremental_history}, result)
    for key in ("style_thresholds", "global_thresholds", "style_meta"):
        checks.append(_compare_mapping(key, inputs[key], result[key]))
    checks.append(compare_frame("candidate_decisions_and_reasons", frames["candidate_after_thresholds"], result["candidate"],
                                (*SCORE_FIELDS, *DECISION_FIELDS)))
    checks.append(compare_frame("post_review_members_scores_reasons", frames["post_review"], result["post_review"],
                                (*SCORE_FIELDS, "项目推荐结论", "项目推荐原因代码", "项目质量信心", "项目准入方式")))
    if result["status"] == "computed":
        neutral_formal = any(column in frames["formal_after_all_filters"] for column in DATA_VALIDATION_COLUMNS)
        neutral_dynamic = any(column in frames["dynamic_reference"] for column in DATA_VALIDATION_COLUMNS)
        checks.extend([
            compare_frame("formal_members_and_scores", frames["formal_after_all_filters"], result["official"],
                          (*SCORE_FIELDS, "正式榜分组", "项目推荐结论",
                           *(DATA_VALIDATION_COLUMNS if neutral_formal else ("AutoStock原始校验状态",)))),
            compare_frame("dynamic_members_and_scores", frames["dynamic_reference"], result["dynamic"],
                          (*SCORE_FIELDS, *(DATA_VALIDATION_COLUMNS if neutral_dynamic else ()))),
            compare_frame("all_final_selection_rejection_reasons", frames["final_selection_contraction"], result["contraction"],
                          list(frames["final_selection_contraction"].columns)),
        ])
    else:
        checks.append({"name": "frozen_input_coverage", "aligned": False, "reason": result["status"],
                       "missing_tracker_history_codes": result.get("missing_tracker_history_codes"),
                       "missing_validation_codes": result.get("missing_validation_codes"),
                       "missing_autostock_codes": result.get("missing_autostock_codes")})
    passed = (all(check["aligned"] for check in checks) and not tracker_capture_check["blocks_baseline"]
              and not incremental_capture_check["blocks_baseline"])
    from backend.prospective_autostock import validate_strategy_observations

    prospective_check, prospective_statuses = validate_strategy_observations(
        settings, frozen=frozen, report=report, universe=observed_universe, production_statuses=statuses,
    )
    source_files = {}
    for key in ("report_json", "filtered_csv", "watch_csv", "dynamic_filtered_csv", "dynamic_watch_csv"):
        source = resolve_runtime_artifact_path(artifacts.get(key), project_root=settings.project_root)
        if source is not None and source.is_file():
            source_files[key] = {"path": str(source.relative_to(settings.project_root)), "sha256": file_sha256(source)}
    validation_provider = _validation_provider(replay_settings)
    summary = {
        "schema_version": 1, "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": report["run_id"],
        "report_generated_at": report["generated_at"], "snapshot_generated_at": frozen["generated_at"],
        "status": "matched" if passed else "mismatch", "baseline_verified": passed,
        "scope": "report_selection_from_complete_scored_universe",
        "upstream_collection_and_core_risk_recalculation_verified": False,
        "later_database_reads": False, "missing_history_imputation": False,
        "evaluation_times": {stage: value.isoformat() for stage, value in evaluation_times.items()},
        "time_reference_source": time_reference_source,
        "wall_clock_used_for_replayed_gates": False,
        "snapshot_path": str(path.relative_to(settings.project_root)), "snapshot_sha256": file_sha256(path),
        "inputs_sha256": frozen["inputs_sha256"], "source_files": source_files,
        "source_code_sha256": frozen["code_sha256"], "replay_code_sha256": source_code_hashes(settings.project_root),
        "checks": checks, "failed_check_count": sum(not check["aligned"] for check in checks)
            + int(tracker_capture_check["blocks_baseline"]) + int(incremental_capture_check["blocks_baseline"]),
        "selection_tracker_history": tracker_capture_check,
        "incremental_review_capture": incremental_capture_check,
        "prospective_autostock_capture": prospective_check,
        "validation_provider": validation_provider,
        "observed_unvalidated_autostock_codes": sorted(observed_unvalidated_codes),
        "observed_unvalidated_validation_codes": sorted(observed_unvalidated_codes),
        "validation_observation_policy": (
            f"{validation_provider}:new_entrants_require_complete_observation;"
            " original_results_may_replay_recorded_degradation"
        ),
        # Keep the legacy field for readers of schema 1 artifacts. It is an
        # informational compatibility marker and is not a current safety gate.
        "autostock_observation_policy": (
            "legacy_provider_new_entrants_require_comparison;"
            " original_results_may_replay_recorded_degradation"
            if validation_provider == "autostock" else "legacy_provider_disabled_or_replaced"
        ),
        "strategy_comparison_allowed": passed, "production_formula_change_authorized_by_this_check": False,
    }
    return summary, {"snapshot": frozen, "frames": frames, "result": result, "settings": replay_settings,
                     # The baseline above uses only its original exported
                     # outcomes. New observations apply only to new entrants.
                     "autostock_status_map": {**statuses, **prospective_statuses} if passed else statuses,
                     "baseline_autostock_status_map": statuses, "evaluation_times": evaluation_times,
                     "observed_unvalidated_autostock_codes": observed_unvalidated_codes if passed else set(),
                     "observed_unvalidated_validation_codes": observed_unvalidated_codes if passed else set(),
                     "validation_provider": validation_provider,
                     "selection_tracker_history": (inputs["selection_tracker_history"]
                         if passed and tracker_capture_check["available"] else None),
                     "incremental_review_history": incremental_history if passed else None}


def write_baseline_verification(settings: Any, summary: dict[str, Any]) -> Path:
    directory = Path(settings.reports_dir) / "audit" / "screening_baseline_replay"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = directory / f"{summary.get('run_id', 'unknown')}_{stamp}.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    from backend.replay_evidence import write_evidence

    write_evidence(Path(settings.project_root), summary)
    return path
