from __future__ import annotations

import argparse
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
from backend.audit_support import write_compressed_audit_csv
from backend.frozen_strategy_audit import (
    FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources, missing_scenario_inputs,
    run_frozen_strategy_scenario, scenario_gap_frame,
)
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL, FORMAL_GROUP_LABEL, FORMAL_SCORE_LABEL, STABLE_BOND_GROUP_LABEL,
)
from backend.project_quality_thresholds import (
    DYNAMIC_THRESHOLD_CONFIG, PROJECT_ABSOLUTE_QUALITY_THRESHOLDS, style_relative_thresholds,
)
from backend.publish_pipeline import normalize_code as _normalize_code
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload as _resolve_run_payload


SCENARIOS: list[dict[str, Any]] = [
    {
        "name": "clip_p05_p95",
        "label": "风格内 5%-95% 截尾",
        "lower_quantile": 0.05,
        "upper_quantile": 0.95,
        "min_style_sample_size": int(DYNAMIC_THRESHOLD_CONFIG["min_style_sample_size"]),
        "clip_columns": [
            "1年-年化收益(%)",
            "1年-夏普比率",
            "1年-索提诺比率",
            "1年-卡玛比率",
            "1年-正收益比率",
            "费率合计(%)",
            "1年-最大回撤(%)",
            "1年-波动率",
        ],
    },
    {
        "name": "clip_p05_p95_min50",
        "label": "风格内 5%-95% 截尾 + 样本数 < 50 回退全局门槛",
        "lower_quantile": 0.05,
        "upper_quantile": 0.95,
        "min_style_sample_size": 50,
        "clip_columns": [
            "1年-年化收益(%)",
            "1年-夏普比率",
            "1年-索提诺比率",
            "1年-卡玛比率",
            "1年-正收益比率",
            "费率合计(%)",
            "1年-最大回撤(%)",
            "1年-波动率",
        ],
    },
]

THRESHOLD_METRICS = [
    "weighted_score_min",
    "short_score_min",
    "long_score_min",
    "annual_return_min",
    "sharpe_min",
    "sortino_min",
    "calmar_min",
    "positive_ratio_min",
    "fee_total_max",
    "max_drawdown_max",
    "volatility_max",
    "scale_billion_min",
]

COMPARE_COLUMNS = [
    "基金代码",
    "基金名称",
    "债基风格标签",
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-索提诺比率",
    "1年-卡玛比率",
    "1年-最大回撤(%)",
    "1年-波动率",
    "基金规模(亿)",
]


def _extract_artifact_maps(latest_payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    artifacts = latest_payload.get("artifacts") or {}
    if not isinstance(artifacts, dict):
        artifacts = {}
    core_artifacts = artifacts.get("core") or {}
    if not isinstance(core_artifacts, dict):
        core_artifacts = {}
    return artifacts, core_artifacts


def _safe_count_by_group(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or FORMAL_GROUP_LABEL not in df.columns:
        return {
            STABLE_BOND_GROUP_LABEL: 0,
            ENHANCED_BOND_GROUP_LABEL: 0,
        }
    counts = df[FORMAL_GROUP_LABEL].fillna("").astype(str).value_counts().to_dict()
    return {
        STABLE_BOND_GROUP_LABEL: int(counts.get(STABLE_BOND_GROUP_LABEL, 0)),
        ENHANCED_BOND_GROUP_LABEL: int(counts.get(ENHANCED_BOND_GROUP_LABEL, 0)),
    }


def _winsorize_series(series: pd.Series, lower_quantile: float, upper_quantile: float) -> tuple[pd.Series, dict[str, Any]]:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return numeric, {"lower_bound": None, "upper_bound": None, "clipped_count": 0}
    lower_bound = float(valid.quantile(lower_quantile))
    upper_bound = float(valid.quantile(upper_quantile))
    if lower_bound > upper_bound:
        lower_bound, upper_bound = upper_bound, lower_bound
    clipped = numeric.clip(lower=lower_bound, upper=upper_bound)
    clipped_count = int(((numeric < lower_bound) | (numeric > upper_bound)).fillna(False).sum())
    return clipped, {
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "clipped_count": clipped_count,
    }


def _prepare_threshold_group(group: pd.DataFrame, scenario: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    adjusted = group.copy()
    clip_meta: dict[str, dict[str, Any]] = {}
    for column in scenario["clip_columns"]:
        clipped, meta = _winsorize_series(adjusted.get(column, pd.Series(index=adjusted.index, dtype=float)), scenario["lower_quantile"], scenario["upper_quantile"])
        adjusted[column] = clipped
        clip_meta[column] = meta
    return adjusted, clip_meta


def _compute_robust_dynamic_style_thresholds(
    df: pd.DataFrame,
    *,
    scenario: dict[str, Any],
) -> tuple[
    dict[str, dict[str, float]],
    dict[str, float],
    dict[str, dict[str, Any]],
    dict[str, dict[str, dict[str, Any]]],
]:
    style_thresholds: dict[str, dict[str, float]] = {}
    style_meta: dict[str, dict[str, Any]] = {}
    clip_meta_by_style: dict[str, dict[str, dict[str, Any]]] = {}
    if df.empty:
        return style_thresholds, PROJECT_ABSOLUTE_QUALITY_THRESHOLDS.copy(), style_meta, clip_meta_by_style

    adjusted_global_df, global_clip_meta = _prepare_threshold_group(df, scenario)
    global_thresholds = style_relative_thresholds(adjusted_global_df, "DEFAULT")
    clip_meta_by_style["__global__"] = global_clip_meta

    grouped = df.copy()
    grouped["债基风格标签"] = grouped.get("债基风格标签", "").map(lambda value: str(value or "").strip() or "未知")
    for style, group in grouped.groupby("债基风格标签"):
        style_key = str(style)
        sample_size = int(len(group.index))
        if sample_size < int(scenario["min_style_sample_size"]):
            style_thresholds[style_key] = global_thresholds.copy()
            style_meta[style_key] = {
                "style": style_key,
                "sample_size": sample_size,
                "threshold_source": "global_fallback",
                "robust_scenario": scenario["name"],
                **global_thresholds,
            }
            clip_meta_by_style[style_key] = {}
            continue

        adjusted_group, clip_meta = _prepare_threshold_group(group, scenario)
        thresholds = style_relative_thresholds(adjusted_group, style_key)
        style_thresholds[style_key] = thresholds
        style_meta[style_key] = {
            "style": style_key,
            "sample_size": sample_size,
            "threshold_source": "style_quantile_robust",
            "robust_scenario": scenario["name"],
            **thresholds,
        }
        clip_meta_by_style[style_key] = clip_meta
    return style_thresholds, global_thresholds, style_meta, clip_meta_by_style


def _run_publish_pipeline_with_thresholds(
    *, context: dict[str, Any], universe_df: pd.DataFrame,
    style_thresholds: dict[str, dict[str, float]], global_thresholds: dict[str, float],
    style_meta: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    result = run_frozen_strategy_scenario(context, scored_universe=universe_df,
        dynamic_threshold_override=(style_thresholds, global_thresholds, style_meta))
    return {**result, "candidate_df": result["candidate"],
            "publish_track_df": result.get("publish_track"), "official_df": result.get("official")}


def _codes(df: pd.DataFrame) -> set[str]:
    if df.empty:
        return set()
    return {
        _normalize_code(code)
        for code in df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }


def _build_threshold_delta_rows(
    *,
    baseline_style_thresholds: dict[str, dict[str, float]],
    baseline_style_meta: dict[str, dict[str, Any]],
    scenario_name: str,
    scenario_style_thresholds: dict[str, dict[str, float]],
    scenario_style_meta: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for style in sorted(set(baseline_style_thresholds) | set(scenario_style_thresholds)):
        baseline = baseline_style_thresholds.get(style) or {}
        scenario = scenario_style_thresholds.get(style) or {}
        baseline_meta = baseline_style_meta.get(style) or {}
        scenario_meta = scenario_style_meta.get(style) or {}
        row: dict[str, Any] = {
            "scenario": scenario_name,
            "style": style,
            "baseline_sample_size": baseline_meta.get("sample_size"),
            "scenario_sample_size": scenario_meta.get("sample_size"),
            "baseline_threshold_source": baseline_meta.get("threshold_source"),
            "scenario_threshold_source": scenario_meta.get("threshold_source"),
        }
        for metric in THRESHOLD_METRICS:
            baseline_value = baseline.get(metric)
            scenario_value = scenario.get(metric)
            row[f"{metric}_baseline"] = baseline_value
            row[f"{metric}_scenario"] = scenario_value
            if baseline_value is None or scenario_value is None:
                row[f"{metric}_delta"] = None
            else:
                row[f"{metric}_delta"] = round(float(scenario_value) - float(baseline_value), 6)
        rows.append(row)
    return rows


def _build_membership_compare_frame(
    *,
    baseline_official_df: pd.DataFrame,
    scenarios: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    baseline = baseline_official_df.copy()
    baseline["基金代码"] = baseline["基金代码"].map(_normalize_code)
    baseline["baseline_in"] = True
    baseline["baseline_score"] = pd.to_numeric(baseline.get(FORMAL_SCORE_LABEL), errors="coerce")
    baseline = baseline.rename(
        columns={
            "基金名称": "baseline_基金名称",
            FORMAL_GROUP_LABEL: "baseline_正式榜分组",
        }
    )
    # The shared frozen candidate frame supplies the style once. Keeping another
    # copy here would produce _x/_y columns and break the final sort.
    keep_columns = ["基金代码", "baseline_基金名称", "baseline_正式榜分组", "baseline_score"]
    baseline = baseline[[column for column in keep_columns if column in baseline.columns] + ["baseline_in"]]

    compare = baseline.copy()
    for scenario_name, scenario_result in scenarios.items():
        scenario_df = scenario_result["official_df"].copy()
        scenario_df["基金代码"] = scenario_df["基金代码"].map(_normalize_code)
        scenario_df[f"{scenario_name}_in"] = True
        scenario_df[f"{scenario_name}_score"] = pd.to_numeric(scenario_df.get(FORMAL_SCORE_LABEL), errors="coerce")
        rename_map = {
            "基金名称": f"{scenario_name}_基金名称",
            FORMAL_GROUP_LABEL: f"{scenario_name}_正式榜分组",
        }
        scenario_df = scenario_df.rename(columns=rename_map)
        scenario_keep = [
            "基金代码",
            f"{scenario_name}_基金名称",
            f"{scenario_name}_正式榜分组",
            f"{scenario_name}_score",
            f"{scenario_name}_in",
        ]
        compare = compare.merge(
            scenario_df[[column for column in scenario_keep if column in scenario_df.columns]],
            on="基金代码",
            how="outer",
        )

    candidate_df = next(iter(scenarios.values()))["candidate_df"].copy()
    candidate_df["基金代码"] = candidate_df["基金代码"].map(_normalize_code)
    shared_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-索提诺比率",
        "1年-卡玛比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "基金规模(亿)",
    ]
    candidate_view = candidate_df[[column for column in shared_columns if column in candidate_df.columns]].drop_duplicates(subset=["基金代码"])
    compare = candidate_view.merge(compare, on="基金代码", how="outer")

    baseline_in_series = compare.get("baseline_in")
    if baseline_in_series is None:
        compare["baseline_in"] = False
    else:
        compare["baseline_in"] = baseline_in_series.where(baseline_in_series.notna(), False).infer_objects(copy=False).astype(bool)
    for scenario_name in scenarios:
        scenario_in_series = compare.get(f"{scenario_name}_in")
        if scenario_in_series is None:
            compare[f"{scenario_name}_in"] = False
        else:
            compare[f"{scenario_name}_in"] = scenario_in_series.where(scenario_in_series.notna(), False).infer_objects(copy=False).astype(bool)
        compare[f"{scenario_name}_change"] = compare.apply(
            lambda row, key=scenario_name: (
                "仍在正式榜"
                if bool(row["baseline_in"]) and bool(row[f"{key}_in"])
                else "稳健场景新增入榜"
                if (not bool(row["baseline_in"])) and bool(row[f"{key}_in"])
                else "稳健场景移出正式榜"
                if bool(row["baseline_in"]) and (not bool(row[f"{key}_in"]))
                else "均未入榜"
            ),
            axis=1,
        )
        compare[f"{scenario_name}_score_delta"] = (
            pd.to_numeric(compare.get(f"{scenario_name}_score"), errors="coerce")
            - pd.to_numeric(compare.get("baseline_score"), errors="coerce")
        ).round(4)

    compare = compare.sort_values(
        by=["债基风格标签", "基金代码"],
        ascending=[True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return compare


def _build_scenario_summary(
    *,
    scenario: dict[str, Any],
    baseline_official_df: pd.DataFrame,
    baseline_result: dict[str, Any],
    scenario_result: dict[str, Any],
    clip_meta_by_style: dict[str, dict[str, dict[str, Any]]],
    baseline_style_thresholds: dict[str, dict[str, float]],
    baseline_style_meta: dict[str, dict[str, Any]],
    scenario_style_thresholds: dict[str, dict[str, float]],
    scenario_style_meta: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_codes = _codes(baseline_official_df)
    scenario_codes = _codes(scenario_result["official_df"])
    added_codes = sorted(scenario_codes - baseline_codes)
    removed_codes = sorted(baseline_codes - scenario_codes)

    dynamic_pass_count = int((scenario_result["candidate_df"].get("dynamic_threshold_verdict", pd.Series(dtype=object)) == "正式推荐").sum())
    publish_post_review_count = int((scenario_result["publish_track_df"].get("项目推荐结论", pd.Series(dtype=object)) == "正式推荐").sum())
    final_count = int(len(scenario_result["official_df"].index))
    baseline_final_count = int(len(baseline_official_df.index))

    changed_style_distribution = (
        scenario_result["official_df"][
            scenario_result["official_df"].get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code).isin(added_codes)
        ]
        .get("债基风格标签", pd.Series(dtype=object))
        .fillna("未知")
        .astype(str)
        .value_counts()
        .to_dict()
    )

    clip_summary_rows: list[dict[str, Any]] = []
    total_clipped_points = 0
    for style, metric_meta in clip_meta_by_style.items():
        for metric, meta in metric_meta.items():
            clipped_count = int(meta.get("clipped_count") or 0)
            total_clipped_points += clipped_count
            clip_summary_rows.append(
                {
                    "style": style,
                    "metric": metric,
                    "clipped_count": clipped_count,
                    "lower_bound": meta.get("lower_bound"),
                    "upper_bound": meta.get("upper_bound"),
                }
            )

    changed_threshold_styles: list[str] = []
    for row in _build_threshold_delta_rows(
        baseline_style_thresholds=baseline_style_thresholds,
        baseline_style_meta=baseline_style_meta,
        scenario_name=scenario["name"],
        scenario_style_thresholds=scenario_style_thresholds,
        scenario_style_meta=scenario_style_meta,
    ):
        if any(abs(float(row.get(f"{metric}_delta") or 0.0)) > 0 for metric in THRESHOLD_METRICS):
            changed_threshold_styles.append(str(row["style"]))

    return {
        "name": scenario["name"],
        "label": scenario["label"],
        "lower_quantile": scenario["lower_quantile"],
        "upper_quantile": scenario["upper_quantile"],
        "min_style_sample_size": scenario["min_style_sample_size"],
        "baseline_final_count": baseline_final_count,
        "scenario_final_count": final_count,
        "final_count_delta": final_count - baseline_final_count,
        "baseline_publish_post_review_count": int((baseline_result["publish_track_df"].get("项目推荐结论", pd.Series(dtype=object)) == "正式推荐").sum()),
        "scenario_publish_post_review_count": publish_post_review_count,
        "scenario_dynamic_threshold_pass_count": dynamic_pass_count,
        "added_count": len(added_codes),
        "removed_count": len(removed_codes),
        "changed_code_count": len(added_codes) + len(removed_codes),
        "added_codes": added_codes,
        "removed_codes": removed_codes,
        "final_group_counts": _safe_count_by_group(scenario_result["official_df"]),
        "changed_style_distribution": changed_style_distribution,
        "threshold_changed_style_count": len(set(changed_threshold_styles)),
        "threshold_changed_styles": sorted(set(changed_threshold_styles)),
        "clip_summary": sorted(
            clip_summary_rows,
            key=lambda item: (-int(item["clipped_count"]), str(item["style"]), str(item["metric"])),
        )[:80],
        "total_clipped_points": total_clipped_points,
    }


def _build_recommendation(summaries: list[dict[str, Any]]) -> list[str]:
    lines = []
    for item in summaries:
        if item.get("status") == "computed":
            lines.append(f"{item['label']}在本次冻结输入下新增{item['added_count']}只、移出{item['removed_count']}只；这是成员敏感性结果。")
        else:
            lines.append(f"{item['label']}尚缺完整历史输入（{item.get('status')}），不能给出最终成员变化结论。")
    lines.append("单次成员变化不能证明策略有效性；生产阈值保持不变，后续仍需独立快照、费用后收益与同风格基准验证。")
    return lines


def _write_markdown(
    *,
    run_id: str,
    baseline_actual_count: int | None,
    baseline_recomputed_count: int | None,
    reproduction_delta_count: int | None,
    scenario_summaries: list[dict[str, Any]],
    output_path: Path,
    audit_status: str = "unknown",
    recommendations: list[str] | None = None,
) -> None:
    lines = [
        "# 动态阈值稳健性影子审计",
        "",
        f"- 生成时间：`{datetime.now().isoformat(timespec='seconds')}`",
        f"- 基线批次：`{run_id}`",
        f"- 审计状态：`{audit_status}`",
        f"- 源报告正式层数量：`{baseline_actual_count if baseline_actual_count is not None else '未核验'}`",
        f"- 影子复算正式主榜数量：`{baseline_recomputed_count}`",
        f"- 影子复算与真实主榜差异数：`{reproduction_delta_count}`",
        "",
        "## 场景结论",
        "",
    ]
    for summary in scenario_summaries:
        if summary.get("status") != "computed":
            lines.extend([f"### {summary['label']}", "", f"- 状态：`{summary.get('status')}`",
                          "- 完整名单及入退榜数量：未完成计算，保留缺失。", ""])
            for key, codes in missing_scenario_inputs(summary).items():
                lines.extend([f"- `{key}`：{len(codes)}只。", ""])
            continue
        lines.extend(
            [
                f"### {summary['label']}",
                "",
                f"- 最终正式榜：`{summary['baseline_final_count']} -> {summary['scenario_final_count']}`",
                f"- 最终正式榜净变化：`{summary['final_count_delta']}`",
                f"- 入榜/移出总变动：`{summary['changed_code_count']}`（新增 `+{summary['added_count']}` / 移出 `-{summary['removed_count']}`）",
                f"- 门槛发生变化的风格数：`{summary['threshold_changed_style_count']}`",
                f"- 截尾数据点数：`{summary['total_clipped_points']}`",
                "",
            ]
        )
        if summary["threshold_changed_styles"]:
            lines.append(f"- 受影响风格：`{', '.join(summary['threshold_changed_styles'])}`")
            lines.append("")
        if summary["added_codes"]:
            lines.append(f"- 新增入榜代码样本：`{', '.join(summary['added_codes'][:10])}`")
            lines.append("")
        if summary["removed_codes"]:
            lines.append(f"- 移出主榜代码样本：`{', '.join(summary['removed_codes'][:10])}`")
            lines.append("")

    lines.extend(
        [
            "## 审计建议",
            "",
        ]
    )
    for item in recommendations if recommendations is not None else _build_recommendation(scenario_summaries):
        lines.append(f"- {item}")
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="使用同一冻结报告的输入审计动态阈值，缺历史证据时停止情景。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定 run；默认读取当前正式报告。")
    source.add_argument("--report", type=Path, help="显式报告 JSON，保留正式或诊断身份。")
    parser.add_argument("--baseline-only", action="store_true", help="仅核验原门槛基线。")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            report = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(report, dict) or not report.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = report["run_id"]
        else:
            run_id, report, _, _ = _resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    summary = {"schema_version": 2, "input_contract": FROZEN_STRATEGY_CONTRACT,
        "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
        "source_publication_control": report.get("publication_control"),
        "status": "baseline_not_verified", "passed": False, "baseline_verification": {},
        "scope": "report_selection_from_frozen_complete_universe", "later_database_reads": False,
        "runtime_upstream_requests": 0, "wall_clock_used_for_replayed_gates": False,
        "missing_business_value_imputation": False, "production_thresholds_changed": False,
        "production_switch_recommended": False, "baseline_actual_final_count": None,
        "baseline_recomputed_final_count": None, "baseline_reproduction_delta_count": None,
        "dynamic_base_count": None, "scenarios": [], "recommendation": ["冻结基线尚未通过核验，不能比较策略。"]}
    exports: dict[str, pd.DataFrame] = {}
    try:
        binding = bind_frozen_report_sources(settings, report)
        summary["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, report)
        summary["baseline_verification"] = baseline
        summary["replay_precision"] = {"status": baseline["status"], "baseline_reproduction_verified": baseline["baseline_verified"]}
        summary["replay_precision_status"] = baseline["status"]
        if baseline["baseline_verified"]:
            frames = context["frames"]
            actual = frames["formal_after_all_filters"].copy()
            verified = context["result"]
            baseline_result = {"candidate_df": verified["candidate"], "publish_track_df": verified["publish_track"],
                               "official_df": verified["official"]}
            dynamic_base = verified["candidate"].loc[verified["candidate"]["基础门禁通过"]].copy()
            summary.update(baseline_actual_final_count=len(actual), baseline_recomputed_final_count=len(verified["official"]),
                baseline_reproduction_delta_count=len(_codes(actual) ^ _codes(verified["official"])),
                dynamic_base_count=len(dynamic_base), evaluation_times=baseline["evaluation_times"],
                snapshot_date=context["snapshot"]["snapshot_date"])
            if args.baseline_only:
                summary.update(status="baseline_verified", passed=True, recommendation=["冻结基线已核验，未请求情景实验。"])
                exports["baseline_members"] = actual
            else:
                complete_scenarios = {}
                threshold_rows = []
                for scenario in SCENARIOS:
                    styles, global_thresholds, style_meta, clip_meta = _compute_robust_dynamic_style_thresholds(dynamic_base, scenario=scenario)
                    result = _run_publish_pipeline_with_thresholds(context=context,
                        universe_df=frames["scored_universe_before_holding_gate"].copy(), style_thresholds=styles,
                        global_thresholds=global_thresholds, style_meta=style_meta)
                    name = scenario["name"]
                    threshold_rows.extend(_build_threshold_delta_rows(baseline_style_thresholds=verified["style_thresholds"],
                        baseline_style_meta=verified["style_meta"], scenario_name=name,
                        scenario_style_thresholds=styles, scenario_style_meta=style_meta))
                    exports[f"{name}_candidate_decisions"] = result["candidate"]
                    exports[f"{name}_post_review"] = result["post_review"]
                    if result["status"] == "computed":
                        item = _build_scenario_summary(scenario=scenario, baseline_official_df=actual,
                            baseline_result=baseline_result, scenario_result=result, clip_meta_by_style=clip_meta,
                            baseline_style_thresholds=verified["style_thresholds"], baseline_style_meta=verified["style_meta"],
                            scenario_style_thresholds=styles, scenario_style_meta=style_meta)
                        item.update(status="computed", strategy_comparison_allowed=True)
                        complete_scenarios[name] = result
                        exports[f"{name}_scenario_members"] = result["official"]
                    else:
                        item = {"name": name, "label": scenario["label"], "status": result["status"],
                            "strategy_comparison_allowed": False, "baseline_final_count": len(actual),
                            "scenario_final_count": None, "final_count_delta": None,
                            "changed_code_count": None, "added_count": None, "removed_count": None,
                            "added_codes": None, "removed_codes": None,
                            "scenario_publish_post_review_count": len(result["post_review"]),
                            "clip_summary_by_style": clip_meta, **missing_scenario_inputs(result)}
                        gaps = scenario_gap_frame(result)
                        if not gaps.empty:
                            exports[f"{name}_missing_inputs"] = gaps
                    summary["scenarios"].append(item)
                if complete_scenarios:
                    exports["membership_compare"] = _build_membership_compare_frame(baseline_official_df=actual, scenarios=complete_scenarios)
                exports["threshold_deltas"] = pd.DataFrame(threshold_rows)
                complete = len(complete_scenarios) == len(SCENARIOS)
                summary.update(status="computed" if complete else "scenario_inputs_incomplete", passed=complete,
                    recommendation=_build_recommendation(summary["scenarios"]))
        if binding != bind_frozen_report_sources(settings, report):
            raise ValueError("frozen_source_changed_during_replay")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="frozen_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}",
            baseline_actual_final_count=None, baseline_recomputed_final_count=None, baseline_reproduction_delta_count=None,
            dynamic_base_count=None, scenarios=[], recommendation=["冻结输入或实验核验失败，未生成策略比较结论。"])
        exports = {}

    audit_dir = settings.reports_dir / "audit" / "dynamic_threshold_robustness"
    summary_path = build_audit_artifact_path(audit_dir, "dynamic_threshold_robustness", run_id=run_id)
    summary["artifacts"] = {}
    for name, frame in exports.items():
        path = summary_path.with_name(f"{summary_path.stem}_{name}.csv.gz")
        metadata = write_compressed_audit_csv(path, frame)
        summary["artifacts"][name] = {"path": str(path.relative_to(settings.project_root)), **metadata}
    with summary_path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(run_id=run_id, baseline_actual_count=summary["baseline_actual_final_count"],
        baseline_recomputed_count=summary["baseline_recomputed_final_count"],
        reproduction_delta_count=summary["baseline_reproduction_delta_count"], scenario_summaries=summary["scenarios"],
        output_path=summary_path.with_suffix(".md"), audit_status=summary["status"], recommendations=summary["recommendation"])
    print(json.dumps({"artifact_path": str(summary_path), "passed": summary["passed"], "status": summary["status"],
        "run_id": run_id, "baseline_reproduction_delta_count": summary["baseline_reproduction_delta_count"],
        "scenarios": [{key: item.get(key) for key in ("name", "status", "scenario_final_count", "added_count", "removed_count")}
                      for item in summary["scenarios"]]}, ensure_ascii=False))
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
