from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
import sys
import tempfile
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.audit_support import write_compressed_audit_csv
from backend.frozen_strategy_audit import FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources
from backend.low_vol_overlap_audit import (
    LOW_VOL_RELEASE_SCENARIOS, LOW_VOL_STYLE_SCOPES, build_low_vol_audit_definition,
)
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline, write_baseline_verification
from backend.publish_pipeline import (
    normalize_code as _normalize_code,
)
from scripts.audit_core_dynamic_overlap import (
    FOCUS_STYLE_BUCKETS,
    METRIC_ORDER,
    _code_set,
    _reason_metric_flags_from_row,
    _run_release_shadow,
    _release_exports,
)
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload as _resolve_run_payload


def _ordered_metric_tuple(metrics: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    metric_set = {str(metric).strip() for metric in metrics if str(metric).strip()}
    return tuple(metric for metric in METRIC_ORDER if metric in metric_set)


def _prepare_dynamic_blocked_df(candidate_df: pd.DataFrame) -> pd.DataFrame:
    current = candidate_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    current["债基风格标签"] = current.get("债基风格标签", pd.Series(dtype=object)).fillna("").astype(str)
    current["基础门禁通过"] = (
        current.get("基础门禁通过", pd.Series(False, index=current.index))
        .fillna(False)
        .astype(bool)
    )
    current["dynamic_threshold_verdict"] = (
        current.get("dynamic_threshold_verdict", pd.Series("", index=current.index, dtype=object))
        .fillna("")
        .astype(str)
    )
    current = current[current["基础门禁通过"]].copy()
    current = current[current["dynamic_threshold_verdict"] != "正式推荐"].copy()
    if current.empty:
        return current

    reason_flags = current.apply(_reason_metric_flags_from_row, axis=1)
    for metric in METRIC_ORDER:
        current[f"reason_has_{metric}"] = reason_flags.map(lambda item, metric_name=metric: bool(item.get(metric_name)))
    current["metric_combo_tuple"] = reason_flags.map(
        lambda item: tuple(metric for metric in METRIC_ORDER if bool(item.get(metric)))
    )
    current["metric_combo_label"] = current["metric_combo_tuple"].map(lambda value: " + ".join(value) if value else "unknown")
    return current.reset_index(drop=True)


def _scenario_mask(
    df: pd.DataFrame,
    *,
    required_metrics: tuple[str, ...] = (),
    exclude_metrics: tuple[str, ...] = (),
    exact: bool = False,
) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    if exact:
        return df["metric_combo_tuple"] == _ordered_metric_tuple(required_metrics)

    mask = pd.Series(True, index=df.index, dtype=bool)
    for metric in required_metrics:
        column = f"reason_has_{metric}"
        mask &= df.get(column, pd.Series(False, index=df.index)).fillna(False).astype(bool)
    for metric in exclude_metrics:
        column = f"reason_has_{metric}"
        mask &= ~df.get(column, pd.Series(False, index=df.index)).fillna(False).astype(bool)
    return mask


def _build_scope_summary_rows(dynamic_blocked_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scope_key, scope_label, styles in LOW_VOL_STYLE_SCOPES:
        scope_df = dynamic_blocked_df[dynamic_blocked_df["债基风格标签"].isin(styles)].copy()
        row = {
            "scope_key": scope_key,
            "scope_label": scope_label,
            "style_count": len(styles),
            "dynamic_blocked_count": int(len(scope_df.index)),
            "annual_return_hit_count": int(scope_df.get("reason_has_annual_return", pd.Series(dtype=bool)).sum()),
            "positive_ratio_hit_count": int(scope_df.get("reason_has_positive_ratio", pd.Series(dtype=bool)).sum()),
            "score_hit_count": int(scope_df.get("reason_has_score", pd.Series(dtype=bool)).sum()),
            "sharpe_hit_count": int(scope_df.get("reason_has_sharpe", pd.Series(dtype=bool)).sum()),
            "top_metric_combos": [],
        }
        if not scope_df.empty:
            combo_counts = scope_df["metric_combo_label"].value_counts().head(8)
            row["top_metric_combos"] = [
                {
                    "metric_combo_label": str(label),
                    "count": int(count),
                    "share_pct": round(float(count) / len(scope_df.index) * 100, 2),
                }
                for label, count in combo_counts.items()
            ]
        rows.append(row)
    return rows


def _build_scenario_impact_rows(
    *,
    dynamic_blocked_df: pd.DataFrame,
    frozen_context: dict[str, Any],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []

    for scope_key, scope_label, styles in LOW_VOL_STYLE_SCOPES:
        scope_df = dynamic_blocked_df[dynamic_blocked_df["债基风格标签"].isin(styles)].copy()
        for scenario in LOW_VOL_RELEASE_SCENARIOS:
            scenario_key = str(scenario.get("scenario_key") or "").strip()
            if scenario.get("mode") == "all":
                scenario_df = scope_df.copy()
            else:
                mask = _scenario_mask(
                    scope_df,
                    required_metrics=tuple(scenario.get("required_metrics") or ()),
                    exclude_metrics=tuple(scenario.get("exclude_metrics") or ()),
                    exact=bool(scenario.get("exact")),
                )
                scenario_df = scope_df[mask].copy()

            impact = _run_release_shadow(
                frozen_context=frozen_context,
                target_codes=_code_set(scenario_df),
            )

            rows.append(
                {
                    "scope_key": scope_key,
                    "scope_label": scope_label,
                    "scenario_key": scenario_key,
                    "scenario_label": str(scenario.get("label") or ""),
                    "exact_metric_combo": bool(scenario.get("exact")),
                    "targeted_count": int(len(scenario_df.index)),
                    "style_distribution": (
                        scenario_df.get("债基风格标签", pd.Series(dtype=object)).fillna("未知").astype(str).value_counts().to_dict()
                        if not scenario_df.empty
                        else {}
                    ),
                    "median_formal_score": (
                        round(float(pd.to_numeric(scenario_df.get("正式投资评分"), errors="coerce").dropna().median()), 4)
                        if not scenario_df.empty and not pd.to_numeric(scenario_df.get("正式投资评分"), errors="coerce").dropna().empty
                        else None
                    ),
                    **impact,
                }
            )

            if scenario_df.empty:
                continue
            preview_df = scenario_df.copy()
            preview_df["scope_key"] = scope_key
            preview_df["scope_label"] = scope_label
            preview_df["scenario_key"] = scenario_key
            preview_df["scenario_label"] = str(scenario.get("label") or "")
            preview_df["scenario_status"] = impact["status"]
            keep_columns = [
                "scope_key",
                "scope_label",
                "scenario_key",
                "scenario_label",
                "scenario_status",
                "基金代码",
                "基金名称",
                "债基风格标签",
                "metric_combo_label",
                "dynamic_threshold_reason",
                "dynamic_threshold_reason_codes",
                "正式投资评分",
                "综合加权得分",
                "1年-年化收益(%)",
                "1年-夏普比率",
                "1年-正收益比率",
                "1年-最大回撤(%)",
                "1年-波动率",
            ]
            detail_rows.extend(preview_df[[column for column in keep_columns if column in preview_df.columns]].to_dict(orient="records"))

    impact_rows = sorted(
        rows,
        key=lambda item: (
            str(item.get("scope_key") or ""),
            item["status"] != "computed",
            -int(item.get("direct_final_impact_count") or 0),
            -int(item.get("targeted_count") or 0),
            str(item.get("scenario_key") or ""),
        ),
    )
    return impact_rows, pd.DataFrame(detail_rows)


def _build_recommendation(
    *, focus_scope_rows: list[dict[str, Any]], scenario_rows: list[dict[str, Any]],
) -> list[str]:
    completed = sum(row.get("status") == "computed" for row in scenario_rows)
    rows = [f"已完成 {completed}/{len(scenario_rows)} 个冻结输入情景；缺输入的情景不能解释为零影响。",
            "本实验只释放指定候选的动态阈值，其余发布门禁继续适用。各情景可能包含相同基金，影响数量不能相加。"]
    for item in scenario_rows:
        if item.get("scope_key") == "style_low_vol_focus" and item.get("scenario_key") in {
            "contains_annual_return_and_positive_ratio", "contains_annual_return_without_positive_ratio",
            "contains_positive_ratio_without_annual_return",
        }:
            value = item.get("direct_final_impact_count")
            rows.append(f"{item['scenario_label']}：" + (f"直达正式 {value} 只。" if item.get("status") == "computed"
                                                    else f"未验证（{item.get('status')}）。"))
    rows.append("成员变化不证明策略有效性；生产阈值调整仍需费用后收益、回撤和稳定性的成熟前瞻比较。")
    return rows


def _write_markdown(*, run_id: str, summary: dict[str, Any], output_path: Path) -> None:
    display = lambda value: "未计算" if value is None else str(value)
    lines = ["# 低波风格年化收益／正收益比率重叠审计", "", f"- 基线批次：`{run_id}`",
             f"- 状态：`{summary['status']}`",
             f"- 已完成情景：{summary['completed_scenario_count']} / {summary['requested_scenario_count']}",
             f"- 冻结正式主榜：{display(summary['baseline_final_count'])}", "",
             "| 风格范围 | 情景 | 状态 | 候选 | 直达正式 | 新增 | 移出 |", "| --- | --- | --- | ---: | ---: | ---: | ---: |"]
    for item in summary.get("scenario_impacts") or []:
        lines.append(f"| {item['scope_label']} | {item['scenario_label']} | {item['status']} | {item['targeted_count']} | "
                     f"{display(item['direct_final_impact_count'])} | {display(item['added_official_count'])} | {display(item['removed_official_count'])} |")
    lines.extend(["", *summary.get("recommendation", []), ""])
    if summary.get("error"):
        lines.append(summary["error"])
    with output_path.open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="从冻结报告审计低波风格 annual_return / positive_ratio 重叠。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="")
    source.add_argument("--report", type=Path)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            payload = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not payload.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = payload["run_id"]
        else:
            run_id, payload, _, _ = _resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2
    summary = {"schema_version": 3, "input_contract": FROZEN_STRATEGY_CONTRACT,
               "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
               "source_publication_control": payload.get("publication_control"),
               "status": "baseline_not_verified", "passed": False, "baseline_verification": {},
               "scope": "report_selection_from_frozen_complete_universe", "source_verified_after_replay": False,
               "later_database_reads": False, "current_upstream_queries": False,
               "wall_clock_used_for_replayed_gates": False, "production_formulas_changed": False,
               "upstream_collection_and_core_risk_recalculation_verified": False,
               "baseline_final_count": None, "dynamic_blocked_total_count": None, "focus_style_dynamic_blocked_count": None,
               "focus_style_buckets": list(FOCUS_STYLE_BUCKETS), "focus_scope_summary": [],
               "scenario_impacts": [], "requested_scenario_count": 0 if args.baseline_only else len(LOW_VOL_STYLE_SCOPES) * len(LOW_VOL_RELEASE_SCENARIOS),
               "completed_scenario_count": 0, "strategy_comparison_allowed": False, "recommendation": []}
    exports = {}
    try:
        binding = bind_frozen_report_sources(settings, payload)
        summary["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, payload)
        summary["baseline_verification"] = baseline
        if not baseline.get("baseline_verified"):
            raise ValueError(f"frozen_baseline_not_verified:{baseline.get('status')}")
        definition_args = {
            "baseline": baseline, "configuration": context["snapshot"]["inputs"]["configuration"],
            "style_scopes": LOW_VOL_STYLE_SCOPES, "scenarios": LOW_VOL_RELEASE_SCENARIOS,
        }
        summary["audit_definition"] = build_low_vol_audit_definition(settings, **definition_args)
        summary["baseline_final_count"] = len(context["frames"]["formal_after_all_filters"])
        if args.baseline_only:
            summary.update(status="baseline_verified", passed=True)
            exports["baseline_members"] = context["frames"]["formal_after_all_filters"]
        else:
            blocked = _prepare_dynamic_blocked_df(context["result"]["candidate"])
            scopes = _build_scope_summary_rows(blocked)
            scenarios, details = _build_scenario_impact_rows(dynamic_blocked_df=blocked, frozen_context=context)
            completed = sum(item["status"] == "computed" for item in scenarios)
            complete = completed == summary["requested_scenario_count"]
            summary.update(status="computed" if complete else "scenario_inputs_incomplete", passed=complete,
                           strategy_comparison_allowed=complete, completed_scenario_count=completed,
                           dynamic_blocked_total_count=len(blocked),
                           focus_style_dynamic_blocked_count=int(blocked["债基风格标签"].isin(FOCUS_STYLE_BUCKETS).sum()),
                           focus_scope_summary=scopes, scenario_impacts=scenarios, scenario_impacts_top=scenarios[:16],
                           unique_release_input_count=len(context["release_results"]),
                           recommendation=_build_recommendation(focus_scope_rows=scopes, scenario_rows=scenarios))
            exports.update(scenario_impacts=pd.DataFrame(scenarios), samples=details)
            exports.update(_release_exports(context))
        if binding != bind_frozen_report_sources(settings, payload):
            raise ValueError("frozen_source_changed_during_replay")
        if summary["audit_definition"] != build_low_vol_audit_definition(settings, **definition_args):
            raise ValueError("audit_definition_changed_during_replay")
        summary["source_verified_after_replay"] = True
        summary["baseline_verification_artifact"] = str(write_baseline_verification(settings, baseline))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="frozen_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}",
                       source_verified_after_replay=False, strategy_comparison_allowed=False,
                       baseline_final_count=None, dynamic_blocked_total_count=None, focus_style_dynamic_blocked_count=None,
                       focus_scope_summary=[], scenario_impacts=[], scenario_impacts_top=[], completed_scenario_count=0)
        exports = {}
    path = build_audit_artifact_path(settings.reports_dir / "audit/low_vol_annual_positive_ratio_overlap",
                                     "low_vol_annual_positive_ratio_overlap", run_id=run_id)
    summary["artifacts"] = {}
    for name, frame in exports.items():
        output = path.with_name(f"{path.stem}_{name}.csv.gz")
        summary["artifacts"][name] = {"path": str(output.relative_to(settings.project_root)),
                                      **write_compressed_audit_csv(output, frame)}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    _write_markdown(run_id=run_id, summary=summary, output_path=path.with_suffix(".md"))
    print(json.dumps({"artifact_path": str(path), "artifact_sha256": file_sha256(path), "run_id": run_id,
                      "passed": summary["passed"], "status": summary["status"],
                      "completed_scenario_count": summary["completed_scenario_count"],
                      "requested_scenario_count": summary["requested_scenario_count"]}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
