"""Read-only counterfactual: treat reviewed name-no-signal rows as zero days.

This audit deliberately changes only an in-memory frozen-universe copy.  It
never updates ``funds.holding_days`` or any publication artifact used by the
live application.  The scenario is a research comparison, not a release
recommendation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.holding_period_gate_shadow import _codes
from backend.incremental_review_replay import apply_scenario_incremental_review
from backend.screening_baseline_replay import run_report_selection, verify_frozen_baseline
from backend import report_builder as production
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload
from backend.audit_support import write_compressed_audit_csv


NO_SIGNAL_SOURCE = "no_signal_reviewed"


def _scenario_universe(universe: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    scenario = universe.copy(deep=True)
    source = scenario.get("holding_period_source", pd.Series("", index=scenario.index))
    source = source.fillna("").astype(str).str.strip()
    days = pd.to_numeric(scenario.get("holding_days"), errors="coerce")
    mask = source.eq(NO_SIGNAL_SOURCE) & days.isna()
    scenario.loc[mask, "holding_days"] = 0
    scenario.loc[mask, "has_holding_period"] = 0
    return scenario, mask


def _summary_frame(frame: pd.DataFrame) -> dict[str, int]:
    groups = frame.get("正式榜分组", pd.Series(dtype=object)).fillna("").astype(str)
    return {
        "total": int(len(frame.index)),
        "stable": int(groups.eq("稳健债基").sum()),
        "enhanced": int(groups.eq("增强债弹性宝").sum()),
    }


def _reason_counts(frame: pd.DataFrame) -> dict[str, int]:
    values = frame.get("基础推荐原因代码", pd.Series(dtype=object)).fillna("").astype(str)
    counts: dict[str, int] = {}
    for value in values:
        for reason in (item.strip() for item in value.replace("；", "|").split("|") if item.strip()):
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _stage_counts(frame: pd.DataFrame) -> dict[str, int]:
    result = {"candidate": int(len(frame.index))}
    if "基础门禁通过" in frame.columns:
        result["base_pass"] = int(frame["基础门禁通过"].fillna(False).astype(bool).sum())
    for column, label in (("fixed_threshold_verdict", "fixed_pass"), ("dynamic_threshold_verdict", "dynamic_pass")):
        if column in frame.columns:
            result[label] = int(frame[column].astype(str).eq("正式推荐").sum())
    if "enhanced_supplemental_pass" in frame.columns:
        result["enhanced_pass"] = int(frame["enhanced_supplemental_pass"].fillna(False).astype(bool).sum())
    return result


def _name_signal_counts(frame: pd.DataFrame) -> dict[str, int]:
    names = frame.get("shortname", pd.Series("", index=frame.index)).fillna("").astype(str)
    patterns = {
        "contains_持有": "持有",
        "contains_滚动": "滚动",
        "contains_定开": "定开",
        "contains_封闭": "封闭",
        "contains_定期开放": "定期开放",
        "contains_开放": "开放",
    }
    return {label: int(names.str.contains(pattern, regex=False).sum()) for label, pattern in patterns.items()}


def _enhanced_block_counts(frame: pd.DataFrame) -> dict[str, int]:
    if "enhanced_supplemental_reason" not in frame.columns:
        return {}
    values = frame["enhanced_supplemental_reason"].fillna("").astype(str).str.strip()
    values = values[values.ne("")]
    return {str(key): int(value) for key, value in values.value_counts().head(12).to_dict().items()}


def _write_markdown(summary: dict[str, Any], path: Path) -> None:
    baseline = summary["baseline"]
    scenario = summary["scenario"]
    impact = summary["impact"]
    lines = [
        "# 名称无持有期信号视为0天：只读影子分析",
        "",
        f"- 生成时间：`{summary['generated_at']}`",
        f"- 冻结运行：`{summary['run_id']}`",
        f"- 输入报告 SHA-256：`{summary['source_report_sha256']}`",
        "- 生产状态：未修改数据库、配置、正式报告或发布指针。",
        "",
        "## 影子假设",
        "",
        f"仅对 `holding_period_source={NO_SIGNAL_SOURCE}` 且 `holding_days=NULL` 的冻结记录，在内存副本中设置 `holding_days=0`；已确认天数、结构化未知、策略文本和长持有期记录均保持原值。",
        "该假设不代表合同事实，只用于衡量名称负向推断对筛选结果的影响。",
        "",
        "## 结果",
        "",
        f"- 基线正式榜：`{baseline['total']}`（稳健 `{baseline['stable']}`，增强 `{baseline['enhanced']}`）",
        f"- 影子正式榜：`{scenario['total'] if scenario['total'] is not None else '未完成'}`（稳健 `{scenario['stable'] if scenario['stable'] is not None else '未完成'}`，增强 `{scenario['enhanced'] if scenario['enhanced'] is not None else '未完成'}`）",
        f"- 影子新增正式成员：`{impact['added_count'] if impact['added_count'] is not None else '未计算'}`，被挤出成员：`{impact['removed_count'] if impact['removed_count'] is not None else '未计算'}`",
        f"- 可模拟为0天的记录：`{summary['shadow_zero_count']}`",
        f"- 影子候选规模：`{scenario['candidate_count']}`（基线 `{baseline['candidate_count']}`）",
        f"- 基线阶段计数：`{baseline['stage_counts']}`",
        f"- 影子阶段计数：`{scenario['stage_counts']}`",
        f"- 被模拟记录的名称信号分布：`{summary['shadow_name_signal_counts']}`",
        f"- 影子增强补充门禁评估样本原因：`{scenario.get('enhanced_block_counts', {})}`",
        f"- 影子重放状态：`{summary.get('scenario_status')}`；当前校验证据缺口：`{len((summary.get('scenario_missing_inputs') or {}).get('missing_validation_codes', []))}` 条",
        "",
        "## 门禁变化",
        "",
        "### 基线基础门禁原因",
        "",
        "\n".join(f"- `{key}`：{value}" for key, value in baseline["reason_counts"].items()) or "- 无",
        "",
        "### 影子基础门禁原因",
        "",
        "\n".join(f"- `{key}`：{value}" for key, value in scenario["reason_counts"].items()) or "- 无",
        "",
        "## 判断",
        "",
        "该结果只说明将名称无信号当作0天后，项目候选和榜单会如何变化；它不能证明这些基金合同确实没有最短持有期。影子新增样本必须逐份复核合同、招募说明书或公告后，才可进入正式事实字段。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(settings: Any, report: dict[str, Any]) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    source_report = Path(str((report.get("artifacts") or {}).get("report_json") or ""))
    source_sha = hashlib.sha256(source_report.read_bytes()).hexdigest() if source_report.is_file() else ""
    baseline_check, context = verify_frozen_baseline(settings, report)
    summary: dict[str, Any] = {
        "schema_version": 1,
        "run_id": report.get("run_id"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_report": str(source_report),
        "source_report_sha256": source_sha,
        "status": "baseline_not_verified",
        "passed": False,
        "read_only": True,
        "database_written": False,
        "publication_changed": False,
        "assumption": {
            "source": NO_SIGNAL_SOURCE,
            "requires_holding_days_null": True,
            "shadow_value": 0,
        },
        "baseline_verification": baseline_check,
    }
    exports: dict[str, pd.DataFrame] = {}
    if not baseline_check.get("baseline_verified"):
        return summary, exports

    frames = context["frames"]
    frozen = context["snapshot"]
    universe = frames["scored_universe_before_holding_gate"]
    scenario_universe, shadow_mask = _scenario_universe(universe)
    shadow_rows = scenario_universe.loc[shadow_mask].copy()
    shadow_rows["影子处理"] = "no_signal_reviewed -> 0天（仅内存）"
    exports["shadow_zero_rows"] = shadow_rows

    inputs = frozen["inputs"]
    baseline_official = frames["formal_after_all_filters"].copy()
    baseline_result = run_report_selection(
        settings=context["settings"],
        scored_universe=production._assign_project_scores(universe),
        decision_map=inputs["decision_map"],
        soft_warning_map=inputs["soft_warning_map"],
        tracker_features=frames["final_selection_tracker_features"],
        autostock_status_map=context["autostock_status_map"],
        evaluation_times=context["evaluation_times"],
        apply_holding_gate=True,
        selection_tracker_history=context.get("selection_tracker_history"),
        observed_unvalidated_validation_codes=context.get(
            "observed_unvalidated_validation_codes",
            context.get("observed_unvalidated_autostock_codes"),
        ),
    )
    baseline_candidate = baseline_result.get("candidate", pd.DataFrame())

    result = run_report_selection(
        settings=context["settings"],
        scored_universe=production._assign_project_scores(scenario_universe),
        decision_map=inputs["decision_map"],
        soft_warning_map=inputs["soft_warning_map"],
        tracker_features=frames["final_selection_tracker_features"],
        autostock_status_map=context["autostock_status_map"],
        evaluation_times=context["evaluation_times"],
        apply_holding_gate=True,
        selection_tracker_history=context.get("selection_tracker_history"),
        observed_unvalidated_validation_codes=context.get(
            "observed_unvalidated_validation_codes",
            context.get("observed_unvalidated_autostock_codes"),
        ),
    )
    result = apply_scenario_incremental_review(context, result)
    scenario_candidate = result.get("candidate", pd.DataFrame())
    scenario_official = result.get("official", pd.DataFrame())
    scenario_complete = result.get("status") == "computed"
    baseline_codes = _codes(baseline_official)
    scenario_codes = _codes(scenario_official)
    added = sorted(scenario_codes - baseline_codes)
    removed = sorted(baseline_codes - scenario_codes)

    baseline_summary = _summary_frame(baseline_official)
    baseline_summary.update({
        "candidate_count": int(len(baseline_candidate.index)),
        "reason_counts": _reason_counts(baseline_candidate),
        "stage_counts": _stage_counts(baseline_candidate),
    })
    scenario_summary = _summary_frame(scenario_official) if scenario_complete else {
        "total": None,
        "stable": None,
        "enhanced": None,
    }
    scenario_summary.update({
        "candidate_count": int(len(scenario_candidate.index)),
        "reason_counts": _reason_counts(scenario_candidate),
        "stage_counts": _stage_counts(scenario_candidate),
        "enhanced_block_counts": _enhanced_block_counts(scenario_candidate),
    })
    summary.update({
        "status": "computed" if result.get("status") == "computed" else "scenario_inputs_incomplete",
        "passed": result.get("status") == "computed",
        "shadow_zero_count": int(shadow_mask.sum()),
        "shadow_name_signal_counts": _name_signal_counts(shadow_rows),
        "baseline": baseline_summary,
        "scenario": scenario_summary,
        "impact": {
            "added_count": len(added) if scenario_complete else None,
            "removed_count": len(removed) if scenario_complete else None,
            "added_codes": added if scenario_complete else [],
            "removed_codes": removed if scenario_complete else [],
        },
        "scenario_status": result.get("status"),
        "scenario_missing_inputs": {key: result.get(key, []) for key in ("missing_tracker_history_codes", "missing_validation_codes", "missing_autostock_codes", "missing_incremental_review_codes") if result.get(key)},
        "snapshot_date": frozen.get("snapshot_date"),
    })
    exports["baseline_members"] = baseline_official
    exports["scenario_members"] = scenario_official
    exports["scenario_candidate"] = scenario_candidate
    compare = pd.DataFrame({"基金代码": sorted(baseline_codes | scenario_codes)})
    compare["基线正式榜"] = compare["基金代码"].isin(baseline_codes)
    compare["影子正式榜"] = compare["基金代码"].isin(scenario_codes)
    compare["变化"] = compare.apply(lambda row: "新增" if row["影子正式榜"] and not row["基线正式榜"] else "移除" if row["基线正式榜"] and not row["影子正式榜"] else "保持" if row["基线正式榜"] else "未入榜", axis=1)
    exports["membership_compare"] = compare
    return summary, exports


def main() -> int:
    parser = argparse.ArgumentParser(description="只读审计：名称无持有期信号临时视为0天")
    parser.add_argument("--run-id", default="", help="指定冻结正式运行，默认最新正式报告")
    parser.add_argument("--report", type=Path, help="指定报告 JSON")
    args = parser.parse_args()
    settings = load_settings()
    if args.report:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    else:
        _, report, _, _ = resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    summary, exports = run(settings, report)
    audit_dir = settings.reports_dir / "audit/holding_no_signal_zero_shadow"
    summary_path = build_audit_artifact_path(audit_dir, "holding_no_signal_zero_shadow", run_id=str(report.get("run_id") or "unknown"))
    artifacts: dict[str, Any] = {}
    for name, frame in exports.items():
        path = summary_path.with_name(f"{summary_path.stem}_{name}.csv.gz")
        artifacts[name] = {"path": str(path), **write_compressed_audit_csv(path, frame)}
    summary["artifacts"] = artifacts
    with summary_path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(summary, summary_path.with_suffix(".md"))
    print(json.dumps({"artifact": str(summary_path), "status": summary["status"], "passed": summary["passed"], "shadow_zero_count": summary.get("shadow_zero_count"), "baseline": summary.get("baseline"), "scenario": summary.get("scenario"), "impact": summary.get("impact")}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
