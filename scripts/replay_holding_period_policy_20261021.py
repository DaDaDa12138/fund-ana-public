from __future__ import annotations

"""Replay the 2026-10-21 holding-period policy on its frozen selection inputs.

This is a selection-layer shadow replay. It does not collect data, query APIs,
read later database state, change publication state, or create a formal report.
"""

import argparse
import gzip
import hashlib
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend import report_builder as production
from backend.config import load_settings
from backend.incremental_review_replay import apply_scenario_incremental_review
from backend.project_formal_groups import ENHANCED_BOND_GROUP_LABEL, FORMAL_GROUP_LABEL, STABLE_BOND_GROUP_LABEL
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS
from backend.publish_pipeline import (
    apply_exported_autostock_status_map,
    materialize_track_frame,
)
from backend.report_inputs import load_actual_autostock_status_map
from backend.runtime_replay import load_runtime_payload, merge_runtime_artifacts, resolve_runtime_artifact_path
from backend.screening_baseline_replay import (
    _codes,
    file_sha256,
    missing_tracker_history_codes,
    missing_validation_observation_codes,
    resolve_evaluation_times,
    verify_incremental_capture,
    verify_selection_tracker_capture,
)
from backend.screening_replay_snapshot import load_screening_input_snapshot, restore_frame, source_code_hashes
from backend.selection_tracker_history import replay_selection_tracker_features
from core.utils.holding_period_policy import (
    HOLDING_DAYS_UNKNOWN,
    combine_holding_period_names,
    holding_period_gate_reason,
    is_reviewed_no_holding_signal,
    resolve_holding_period_days,
)

RUN_ID = "20261021_074144_sync"
SNAPSHOT_EXPECTED_SHA256 = "be9a7e43cd724ef85de35b92ec7068b63e7c3b0817a454b82aac50c0856f1705"
INPUTS_EXPECTED_SHA256 = "2010203cbcb8bd293c36102d58189de4a41d0df4278d69bb0778df8d5a196cf7"
BASELINE_PROOF = Path("reports/audit/screening_baseline_replay/20261021_074144_sync_20261021_081706_505999.json")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _count_groups(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or FORMAL_GROUP_LABEL not in frame:
        return {STABLE_BOND_GROUP_LABEL: 0, ENHANCED_BOND_GROUP_LABEL: 0}
    counts = frame[FORMAL_GROUP_LABEL].fillna("").astype(str).value_counts().to_dict()
    return {STABLE_BOND_GROUP_LABEL: int(counts.get(STABLE_BOND_GROUP_LABEL, 0)),
            ENHANCED_BOND_GROUP_LABEL: int(counts.get(ENHANCED_BOND_GROUP_LABEL, 0))}


def _counts_by_value(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in frame:
        return {}
    values = frame[column].where(frame[column].notna(), "<NULL>").astype(str)
    return {key: int(value) for key, value in values.value_counts(dropna=False).items()}


def _read_baseline_proof(settings: Any, snapshot_path: Path, snapshot: dict[str, Any]) -> dict[str, Any]:
    proof_path = settings.project_root / BASELINE_PROOF
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    proof_sha = file_sha256(proof_path)
    if (proof.get("run_id") != RUN_ID or proof.get("status") != "matched"
            or proof.get("baseline_verified") is not True
            or proof.get("snapshot_sha256") != SNAPSHOT_EXPECTED_SHA256
            or proof.get("inputs_sha256") != INPUTS_EXPECTED_SHA256
            or proof.get("snapshot_path") != str(snapshot_path.relative_to(settings.project_root))):
        raise ValueError("historical_baseline_proof_does_not_match_frozen_inputs")
    current_sha = file_sha256(snapshot_path)
    if current_sha != SNAPSHOT_EXPECTED_SHA256 or snapshot.get("inputs_sha256") != INPUTS_EXPECTED_SHA256:
        raise ValueError("frozen_snapshot_digest_mismatch")
    for artifact_key, source in (proof.get("source_files") or {}).items():
        path = settings.project_root / source["path"]
        if not path.is_file() or file_sha256(path) != source.get("sha256"):
            raise ValueError(f"baseline_source_artifact_changed:{artifact_key}")
    return {"path": str(proof_path.relative_to(settings.project_root)), "sha256": proof_sha,
            "status": proof["status"], "snapshot_sha256": proof["snapshot_sha256"],
            "inputs_sha256": proof["inputs_sha256"], "historical_baseline_verified": True,
            "note": "Verification predates this policy change and proves only the old selection baseline."}


def _build_result(settings: Any) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    run = load_runtime_payload(settings, RUN_ID)
    artifacts = merge_runtime_artifacts(run.get("artifacts") or {})
    snapshot_value = artifacts.get("screening_input_snapshot_json_gz")
    snapshot_path = resolve_runtime_artifact_path(snapshot_value, project_root=settings.project_root)
    if snapshot_path is None or not snapshot_path.is_file():
        raise ValueError("frozen_screening_input_snapshot_missing")
    frozen = load_screening_input_snapshot(snapshot_path, expected_run_id=RUN_ID)
    proof = _read_baseline_proof(settings, snapshot_path, frozen)

    registry = json.loads((settings.reports_dir / "publication_registry.json").read_text(encoding="utf-8"))
    publication = next((item for section in ("canonical_runs", "superseded_runs")
                        for item in registry.get(section, []) if item.get("run_id") == RUN_ID), None)
    if (publication is None or publication.get("publication_committed") is not True):
        raise ValueError("frozen_run_is_not_the_verified_generation_66_publication")

    inputs = frozen["inputs"]
    frames = {key: restore_frame(value) for key, value in inputs["frames"].items()}
    universe = frames["scored_universe_before_holding_gate"]
    original_candidate = frames["candidate_after_thresholds"]
    evaluation_times, time_source = resolve_evaluation_times(frozen)
    replay_settings = replace(settings, **inputs["configuration"])

    # The score inputs are frozen. Re-run the production score assignment over
    # all 8,184 rows before recalculating the new policy's dynamic thresholds.
    scored = production._assign_project_scores(universe)
    from backend.screening_baseline_replay import compare_frame

    score_check = compare_frame(
        "frozen_score_assignment", universe, scored,
        ("综合加权得分", "近期得分", "长期得分", "综合评分", "正式投资评分"),
    )
    if not score_check["aligned"]:
        raise ValueError("frozen_scoring_inputs_do_not_reproduce")

    tracker_check = verify_selection_tracker_capture(frozen, frames)
    if tracker_check.get("blocks_baseline"):
        raise ValueError("selection_tracker_history_not_verified")

    # Holding gate, fixed gates, dynamic quantiles and all report post-filters
    # are recalculated with current code. No threshold from the old run is reused.
    from backend.screening_baseline_replay import prepare_selection_candidates

    candidate, styles, global_thresholds, style_meta = prepare_selection_candidates(
        settings=replay_settings, scored_universe=scored,
        decision_map=inputs["decision_map"], soft_warning_map=inputs["soft_warning_map"],
        evaluation_times=evaluation_times, apply_holding_gate=True,
    )
    track = materialize_track_frame(
        candidate, verdict_column="dynamic_threshold_verdict",
        watch_bucket_column="dynamic_threshold_watch_bucket",
        reason_column="dynamic_threshold_reason",
    )
    dynamic_pre_review = track.loc[track["项目推荐结论"].eq("正式推荐")].copy()
    publish_track = production._apply_publish_post_filters(
        track, threshold_mode="dynamic", style_thresholds=styles,
        global_thresholds=global_thresholds,
        evaluated_at=evaluation_times["dynamic_post_review"],
    )
    post_review = publish_track.loc[publish_track["项目推荐结论"].eq("正式推荐")].copy()
    history = inputs.get("selection_tracker_history")
    tracker_features = (replay_selection_tracker_features(history, post_review) if history is not None
                        else frames["final_selection_tracker_features"].copy())
    tracker_gaps = missing_tracker_history_codes(post_review, tracker_features)
    if tracker_gaps:
        raise ValueError(f"selection_tracker_history_missing_for_counterfactual:{len(tracker_gaps)}")
    selected_before_incremental = production._apply_final_official_selection(
        post_review, settings=replay_settings, tracker_features_df=tracker_features,
    )
    result: dict[str, Any] = {"status": "computed", "candidate": candidate,
                              "publish_track": publish_track, "post_review": post_review,
                              "dynamic": dynamic_pre_review, "official": selected_before_incremental,
                              "style_thresholds": styles, "global_thresholds": global_thresholds,
                              "style_meta": style_meta}

    incremental_check, incremental_history = verify_incremental_capture(
        frozen, frames, frames["formal_after_all_filters"],
    )
    if incremental_check.get("blocks_baseline") or not incremental_check.get("available"):
        raise ValueError("historical_incremental_review_capture_not_verified")
    if incremental_history is not None:
        result = apply_scenario_incremental_review(
            {"incremental_review_history": incremental_history}, result,
        )
    if result.get("status") != "computed":
        raise ValueError(f"incremental_review_counterfactual_incomplete:{result.get('status')}")

    statuses = load_actual_autostock_status_map(artifacts, project_root=settings.project_root)
    selected = result["official"].copy()
    selected = apply_exported_autostock_status_map(selected, autostock_status_map=statuses)
    selected, hard_blocked = production._demote_autostock_hard_block_rows(
        selected, allow_legacy_autostock=False,
    )
    dynamic_with_status = apply_exported_autostock_status_map(result["dynamic"], autostock_status_map=statuses)
    dynamic_with_status, dynamic_hard_blocked = production._demote_autostock_hard_block_rows(
        dynamic_with_status, allow_legacy_autostock=False,
    )

    baseline_proof_payload = json.loads((settings.project_root / BASELINE_PROOF).read_text(encoding="utf-8"))
    observed_unvalidated = set(baseline_proof_payload.get("observed_unvalidated_validation_codes") or [])
    missing_validation = missing_validation_observation_codes(
        _codes(selected) | _codes(dynamic_with_status), statuses,
        observed_unvalidated_codes=observed_unvalidated, settings=replay_settings,
    )

    original_official = frames["formal_after_all_filters"].copy()
    original_dynamic = frames["dynamic_reference"].copy()
    original_codes = _codes(original_official)
    selected_codes = _codes(selected)
    added = selected_codes - original_codes
    removed = original_codes - selected_codes

    # Explicitly count policy states from the frozen full universe. No NULL is
    # replaced with zero in the exported business columns.
    source = universe.get("holding_period_source", pd.Series(index=universe.index, dtype=object)).fillna("").astype(str)
    keywords = universe.get("holding_keywords", pd.Series(index=universe.index, dtype=object)).fillna("").astype(str)
    has_period = pd.to_numeric(universe.get("has_holding_period"), errors="coerce")
    days = pd.to_numeric(universe.get("holding_days"), errors="coerce")
    shortname = universe.get("shortname", universe.get("基金名称", pd.Series("", index=universe.index)))
    fullname = universe.get("fullname", pd.Series("", index=universe.index))
    names = pd.Series(
        [combine_holding_period_names(short, full) for short, full in zip(shortname, fullname)],
        index=universe.index,
    )
    effective_days = pd.Series(
        [resolve_holding_period_days(
            holding_days=raw, fund_name=name,
            holding_period_source=src, holding_keywords=keyword,
        ) for raw, name, src, keyword in zip(days, names, source, keywords)],
        index=universe.index,
    )
    confirmed_over = effective_days.gt(replay_settings.exclude_holding_days_gt)
    gate_reasons = universe.apply(lambda row: holding_period_gate_reason(
        has_holding_period=row.get("has_holding_period"), holding_days=row.get("holding_days"),
        holding_period_source=row.get("holding_period_source"),
        holding_keywords=row.get("holding_keywords"),
        threshold_days=replay_settings.exclude_holding_days_gt,
        fund_name=combine_holding_period_names(
            row.get("shortname"), row.get("基金名称"), row.get("fullname")),
    ) or "eligible", axis=1)
    confirmed_eligible = effective_days.notna() & gate_reasons.eq("eligible")
    name_derived = days.isna() & effective_days.notna()
    ordinary_no_signal = effective_days.isna() & gate_reasons.eq("eligible")
    ordinary_null = days.isna() & ordinary_no_signal
    reviewed_no_signal = pd.Series([
        is_reviewed_no_holding_signal(
            has_holding_period=flag, holding_days=raw, holding_period_source=src,
        ) for flag, raw, src in zip(has_period, days, source)
    ], index=universe.index) & ordinary_no_signal
    generic_placeholder_ordinary = days.eq(999) & ordinary_no_signal
    unknown_or_invalid = gate_reasons.eq(HOLDING_DAYS_UNKNOWN)

    summary = {
        "schema_version": 2,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": RUN_ID,
        "source_generation": 66,
        "source_run_generated_at": frozen["generated_at"],
        "snapshot_date": frozen["snapshot_date"],
        "scope": "frozen_selection_layer_policy_replay",
        "is_shadow_replay": True,
        "is_new_formal_publication": False,
        "upstream_collection_replayed": False,
        "risk_recalculation_replayed": False,
        "later_database_reads": False,
        "api_requests": 0,
        "production_publication_state_modified": False,
        "threshold_reused_from_baseline": False,
        "holding_period_policy": {
            "no_signal_rule": "holding_days=NULL with a non-empty ordinary name (or no_signal_reviewed) is eligible; preserve database NULL",
            "name_derived_rule": "a concrete term in shortname/fullname is evaluated at the parsed day count without writing holding_days",
            "explicit_limit_rule": f"confirmed holding_days > {replay_settings.exclude_holding_days_gt} remains blocked",
            "other_unknown_rule": "missing usable name and days remain blocked; positive holding flag with zero days remains invalid",
        },
        "baseline_evidence": proof,
        "snapshot": {"path": str(snapshot_path.relative_to(settings.project_root)),
                     "sha256": file_sha256(snapshot_path), "inputs_sha256": frozen["inputs_sha256"]},
        "configuration": inputs["configuration"],
        "evaluation_times": {key: value.isoformat() for key, value in evaluation_times.items()},
        "evaluation_time_source": time_source,
        "verification": {"frozen_score_assignment": score_check,
                         "selection_tracker_history": tracker_check,
                         "incremental_review_capture": incremental_check,
                         "all_selection_stages_recomputed": True,
                         "dynamic_thresholds_recomputed_from_policy_eligible_base": True},
        "population": {
            "frozen_scored_universe_count": int(len(universe)),
            "old_snapshot_candidate_count_after_gt90_gate": int(len(original_candidate)),
            "ordinary_no_signal_count": int(ordinary_no_signal.sum()),
            "ordinary_no_signal_null_count": int(ordinary_null.sum()),
            "reviewed_no_signal_null_count": int(reviewed_no_signal.sum()),
            "name_default_ordinary_null_count": int((ordinary_null & ~reviewed_no_signal).sum()),
            "generic_placeholder_ordinary_count": int(generic_placeholder_ordinary.sum()),
            "name_derived_holding_days_count": int(name_derived.sum()),
            "confirmed_holding_days_over_limit_count": int(confirmed_over.sum()),
            "confirmed_eligible_days_count": int(confirmed_eligible.sum()),
            "unknown_or_invalid_count": int(unknown_or_invalid.sum()),
            "gate_reason_counts": _counts_by_value(pd.DataFrame({"reason": gate_reasons}), "reason"),
        },
        "stage_counts": {
            "policy_candidate_count": int(len(candidate)),
            "base_gate_pass_count": int(candidate["基础门禁通过"].sum()) if "基础门禁通过" in candidate else 0,
            "dynamic_formal_before_post_review_count": int(len(dynamic_pre_review)),
            "post_review_formal_count": int(len(post_review)),
            "final_selection_before_incremental_review_count": int(len(selected_before_incremental)),
            "incremental_review_demoted_count": len(result.get("incremental_review_removed_codes", [])),
            "selected_after_incremental_and_observed_hard_blocks_count": int(len(selected)),
            "dynamic_after_observed_hard_blocks_count": int(len(dynamic_with_status)),
            "observed_hard_block_count": int(len(hard_blocked)),
            "dynamic_observed_hard_block_count": int(len(dynamic_hard_blocked)),
            "missing_active_provider_observation_count": len(missing_validation),
            "missing_active_provider_observation_codes": missing_validation,
            "provider_validation_complete_for_counterfactual": not missing_validation,
            "selected_group_counts_before_external_validation": _count_groups(selected),
            "dynamic_group_counts_before_external_validation": _count_groups(dynamic_with_status),
            "original_formal_count": int(len(original_official)),
            "original_dynamic_count": int(len(original_dynamic)),
            "final_count_delta_before_external_validation": int(len(selected) - len(original_official)),
            "added_codes_before_external_validation": sorted(added),
            "removed_codes_before_external_validation": sorted(removed),
        },
        "dynamic_thresholds": {"style": styles, "global": global_thresholds, "style_meta": style_meta},
        "external_validation": {
            "provider": getattr(replay_settings, "publication_validation_provider", ""),
            "captured_status_count": len(statuses),
            "new_api_observation_requests_made": False,
            "missing_observations_block_full_publication_claim": bool(missing_validation),
        },
        "source_code_sha256": source_code_hashes(settings.project_root),
        "policy_helper_sha256": file_sha256(settings.project_root / "core/utils/holding_period_policy.py"),
        "replay_script_sha256": file_sha256(Path(__file__).resolve()),
    }
    exports = {
        "policy_candidate_decisions": candidate,
        "dynamic_formal_before_post_review": dynamic_pre_review,
        "post_review_formal_before_final_selection": post_review,
        "selected_after_frozen_observations": selected,
        "observed_hard_blocked_after_selection": hard_blocked,
        "dynamic_after_observed_hard_blocks": dynamic_with_status,
        "original_formal_members": original_official,
        "original_dynamic_members": original_dynamic,
    }
    if not selected.empty:
        selected_validation = selected.copy()
        selected_validation["是否缺少本次校验观测"] = selected_validation["基金代码"].map(
            lambda code: production._normalize_code(code) in set(missing_validation)
        )
        exports["selected_validation_coverage"] = selected_validation
    return summary, exports


def _write_csv(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = frame.to_csv(index=False).encode("utf-8-sig")
    with path.open("xb") as file_obj:
        with gzip.GzipFile(fileobj=file_obj, mode="wb", mtime=0) as zipped:
            zipped.write(raw)
    return {"path": str(path.relative_to(PROJECT_ROOT)), "rows": int(len(frame)),
            "sha256": file_sha256(path), "bytes": path.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description="重放冻结正式 run 的持有期新策略（仅选择层影子分析）")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/audit/holding_policy_20261021"))
    args = parser.parse_args()
    settings = load_settings()
    summary, exports = _build_result(settings)
    output_dir = args.output_dir if args.output_dir.is_absolute() else settings.project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FileExistsError(f"holding_policy_replay_output_not_empty:{output_dir}")
    summary["artifacts"] = {
        name: _write_csv(output_dir / f"{name}.csv.gz", frame)
        for name, frame in exports.items()
    }
    summary_path = output_dir / "holding_policy_replay_20261021.json"
    summary_bytes = (json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    summary_path.write_bytes(summary_bytes)
    lines = [
        "# 持有期策略冻结选择层影子重放",
        "",
        f"- 源正式 run：`{RUN_ID}`；源发布 generation：`66`；净值日期：`{summary['snapshot_date']}`",
        "- 结果属性：仅基于冻结选择层输入的影子重放；不是重新采集、不是新正式发布，也没有修改发布状态或数据库。",
        "- 旧正式发布基线：正式榜 50 只，增强债弹性榜 0 只；动态正式榜 52 只。",
        "- 新策略：从名称解析明确期限；名称无期限的 NULL 按普通基金准入并保留 NULL；明确 >90 天仍拦截；名称与天数均缺失仍拦截。",
        "- 本轮从完整冻结全集重新计算动态门槛，没有复用旧阈值。",
        "",
        "## 母体和筛选阶段",
        "",
        f"- 冻结打分母体：{summary['population']['frozen_scored_universe_count']} 只。",
        f"- 按普通基金处理且无明确期限：{summary['population']['ordinary_no_signal_count']} 只；其中原始天数为 NULL：{summary['population']['ordinary_no_signal_null_count']} 只。",
        f"- NULL 中实际已有扫描无信号记录：{summary['population']['reviewed_no_signal_null_count']} 只；依名称无期限默认普通基金：{summary['population']['name_default_ordinary_null_count']} 只。",
        f"- 旧 999 泛化占位且名称无明确期限：{summary['population']['generic_placeholder_ordinary_count']} 只；不把占位值计为已确认长期限制。",
        f"- 原始天数 NULL、由名称解析出具体期限：{summary['population']['name_derived_holding_days_count']} 只。",
        f"- 明确超过门槛：{summary['population']['confirmed_holding_days_over_limit_count']} 只；继续排除。",
        f"- 仍无法确定（名称及有效天数均缺失，或标志与零天数冲突）：{summary['population']['unknown_or_invalid_count']} 只；继续拦截。",
        f"- 新规则下基础门禁通过：{summary['stage_counts']['base_gate_pass_count']} 只。",
        f"- 动态门槛后、发布复核前：{summary['stage_counts']['dynamic_formal_before_post_review_count']} 只。",
        f"- 发布复核后：{summary['stage_counts']['post_review_formal_count']} 只。",
        f"- 稳定性精选后、冻结校验映射中的硬拦截处理后：{summary['stage_counts']['selected_after_incremental_and_observed_hard_blocks_count']} 只。",
        f"- 其中稳定债：{summary['stage_counts']['selected_group_counts_before_external_validation'][STABLE_BOND_GROUP_LABEL]} 只；增强债弹性：{summary['stage_counts']['selected_group_counts_before_external_validation'][ENHANCED_BOND_GROUP_LABEL]} 只。",
        f"- 相对旧正式榜净变化：{summary['stage_counts']['final_count_delta_before_external_validation']} 只；新增 {len(summary['stage_counts']['added_codes_before_external_validation'])} 只，移出 {len(summary['stage_counts']['removed_codes_before_external_validation'])} 只。",
        "",
        "## 发布前置证据限制",
        "",
        f"- 活跃校验来源：`{summary['external_validation']['provider']}`；冻结校验映射共 {summary['external_validation']['captured_status_count']} 条。",
        f"- 新策略候选中缺少本次来源校验观测：{summary['stage_counts']['missing_active_provider_observation_count']} 只。",
        f"- 因此若缺失数大于 0，表中最终精选列表只表示校验前候选，不可称为可正式发布榜单。",
        "- 所有 CSV 产物明确标为策略重放阶段数据；正式榜及动态门槛均不写回项目发布目录。",
        "",
        f"机器可读摘要：`{summary_path.name}`。",
    ]
    md_path = output_dir / "README.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        "summary": str(summary_path.relative_to(settings.project_root)),
        "summary_sha256": _sha256_bytes(summary_bytes),
        "readme": str(md_path.relative_to(settings.project_root)),
        "source_generation": 66,
        "universe_count": summary["population"]["frozen_scored_universe_count"],
        "reviewed_no_signal_count": summary["population"]["reviewed_no_signal_null_count"],
        "ordinary_no_signal_count": summary["population"]["ordinary_no_signal_count"],
        "base_gate_pass_count": summary["stage_counts"]["base_gate_pass_count"],
        "pre_external_validation_final_count": summary["stage_counts"]["selected_after_incremental_and_observed_hard_blocks_count"],
        "enhanced_count": summary["stage_counts"]["selected_group_counts_before_external_validation"][ENHANCED_BOND_GROUP_LABEL],
        "missing_active_provider_observations": summary["stage_counts"]["missing_active_provider_observation_count"],
        "formal_publication": False,
    }, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
