"""Holding-period research using verified report inputs, with explicit gaps."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from backend import report_builder as production
from backend.incremental_review_replay import apply_scenario_incremental_review
from backend.incremental_review_replay import incremental_review_candidates as _incremental_review_candidates
from backend.screening_baseline_replay import run_report_selection, verify_frozen_baseline


def _codes(frame: pd.DataFrame) -> set[str]:
    return set(frame.get("基金代码", pd.Series(dtype=str)).map(production._normalize_code)) - {""}


def build_holding_period_gate_shadow(
    settings: Any, report: dict[str, Any], *, baseline_only: bool = False,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    """Never rebuild a historical universe, tracker or API status from live data."""
    try:
        baseline, context = verify_frozen_baseline(settings, report)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        baseline, context = {"status": "invalid_frozen_inputs", "baseline_verified": False,
                             "reason": f"{type(exc).__name__}: {exc}"}, {}
    summary = {
        "schema_version": 2, "run_id": report.get("run_id"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_report": (report.get("artifacts") or {}).get("report_json"),
        "source_publication_control": report.get("publication_control"),
        "baseline_verification": baseline,
        "baseline": {"status": baseline["status"], "final_count": None},
        "status": "baseline_not_verified", "passed": False,
        "scope": "report_selection_from_frozen_complete_universe",
        "later_database_reads": False, "runtime_upstream_requests": 0,
        "wall_clock_used_for_replayed_gates": False, "missing_business_value_imputation": False,
        "production_holding_gate_changed": False, "production_switch_recommended": False,
        "scenario_without_gate": {"status": "not_evaluated", "final_count": None,
                                  "strategy_comparison_allowed": False},
    }
    exports: dict[str, pd.DataFrame] = {}
    if not baseline["baseline_verified"]:
        return summary, exports

    frames = context["frames"]
    frozen = context["snapshot"]
    universe = frames["scored_universe_before_holding_gate"]
    official = frames["formal_after_all_filters"]
    summary["baseline"].update(final_count=len(official), universe_count=len(universe),
                               dynamic_reference_count=len(frames["dynamic_reference"]))
    summary["holding_period_threshold_days"] = context["settings"].exclude_holding_days_gt
    summary["snapshot_date"] = frozen["snapshot_date"]
    summary["evaluation_times"] = baseline["evaluation_times"]
    summary["time_reference_source"] = baseline["time_reference_source"]
    exports["baseline_members"] = official.copy()
    if baseline_only:
        summary.update(status="baseline_verified", passed=True)
        summary["scenario_without_gate"]["status"] = "not_requested"
        return summary, exports

    # Re-score the complete population before changing the eligibility gate.
    # The counterfactual does not alter settings, holding_days, or saved frames.
    scored = production._assign_project_scores(universe)
    inputs = frozen["inputs"]
    result = run_report_selection(
        settings=context["settings"], scored_universe=scored,
        decision_map=inputs["decision_map"], soft_warning_map=inputs["soft_warning_map"],
        tracker_features=frames["final_selection_tracker_features"],
        autostock_status_map=context["autostock_status_map"], evaluation_times=context["evaluation_times"],
        apply_holding_gate=False,
        selection_tracker_history=context.get("selection_tracker_history"),
        observed_unvalidated_validation_codes=context.get(
            "observed_unvalidated_validation_codes",
            context.get("observed_unvalidated_autostock_codes"),
        ),
    )
    result = apply_scenario_incremental_review(context, result)
    scenario = summary["scenario_without_gate"]
    scenario.update(status=result["status"], candidate_count=len(result["candidate"]),
                    post_review_count=len(result["post_review"]),
                    style_thresholds=result["style_thresholds"], global_thresholds=result["global_thresholds"])
    # These are stage outputs, never a substitute final fund list.
    exports["scenario_candidate_decisions"] = result["candidate"].copy()
    exports["scenario_post_review"] = result["post_review"].copy()
    for key in ("missing_tracker_history_codes", "missing_validation_codes", "missing_autostock_codes", "missing_incremental_review_codes",
                "incremental_review_status", "incremental_review_removed_codes"):
        if key in result:
            scenario[key] = result[key]
    if "incremental_review" in result:
        exports["scenario_incremental_review"] = result["incremental_review"].copy()
    gap_rows = [{"基金代码": code, "缺失阶段": stage}
                for key, stage in (("missing_tracker_history_codes", "Tracker历史"),
                                   ("missing_validation_codes", "当前校验证据"),
                                   ("missing_autostock_codes", "AutoStock观测（兼容旧 provider）"),
                                   ("missing_incremental_review_codes", "新增样本安全复核"))
                for code in scenario.get(key, [])]
    if gap_rows:
        exports["missing_inputs"] = pd.DataFrame(gap_rows)
    if scenario["status"] != "computed":
        summary.update(status="scenario_inputs_incomplete", passed=False)
        scenario["not_evaluated_stages"] = {
            "missing_tracker_history": ["final_selection", "autostock_gate", "incremental_safety"],
            "missing_autostock_observations": ["autostock_gate", "incremental_safety"],
            "missing_validation_observations": ["validation_gate", "incremental_safety"],
            "missing_frozen_incremental_review": ["incremental_safety"],
        }.get(scenario["status"], [])
        return summary, exports

    actual_codes, scenario_codes = _codes(official), _codes(result["official"])
    scenario.update(final_count=len(result["official"]), dynamic_reference_count=len(result["dynamic"]),
                    strategy_comparison_allowed=True)
    summary["impact"] = {"added_codes": sorted(scenario_codes - actual_codes),
                         "removed_codes": sorted(actual_codes - scenario_codes),
                         "final_count_delta": len(scenario_codes) - len(actual_codes)}
    summary.update(status="computed", passed=True)
    exports["scenario_members"] = result["official"].copy()
    return summary, exports


def render_holding_period_gate_shadow(summary: dict[str, Any]) -> str:
    baseline, scenario = summary["baseline"], summary["scenario_without_gate"]
    lines = ["# 持有期冻结输入审计", "", f"- run：`{summary.get('run_id')}`",
             f"- 审计状态：`{summary['status']}`", f"- 基线核验：`{baseline['status']}`",
             f"- 冻结正式成员数：{baseline['final_count'] if baseline['final_count'] is not None else '未核验'}",
             f"- 情景状态：`{scenario['status']}`",
             f"- 情景完整成员数：{scenario['final_count'] if scenario['final_count'] is not None else '未完成计算'}",
             "", "仅使用报告冻结的候选全集、配置和各阶段时间。缺少历史证据时停止相应阶段，保留缺失，生产持有期门禁不变。"]
    for key, label in (("missing_tracker_history_codes", "缺Tracker历史"), ("missing_validation_codes", "缺当前校验证据"), ("missing_autostock_codes", "缺AutoStock观测（兼容旧 provider）"),
                       ("missing_incremental_review_codes", "缺新增样本安全复核输入")):
        if key in scenario:
            lines.extend(["", f"{label}（{len(scenario[key])}只）：" + "、".join(scenario[key])])
    if summary.get("time_reference_source") == "legacy_snapshot_generated_at":
        lines.extend(["", "旧快照使用原生成时间，各门禁的精确原始时刻未记录。"])
    return "\n".join(lines) + "\n"
