from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
# Exposing the nested screening directory would make its `core` package shadow
# the project's `core.utils` namespace in a fresh Python process.
for source_root in (CORE_ROOT, PROJECT_ROOT):
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))

from backend import report_builder
from backend.audit_support import write_compressed_audit_csv
from backend.config import load_settings
from backend.frozen_strategy_audit import (
    FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources, missing_scenario_inputs,
    run_frozen_strategy_scenario, scenario_gap_frame,
)
from backend.frozen_risk_observations import (
    bind_report_risk_observations, read_frozen_risk_observations, verify_risk_observation_sources,
)
from backend.project_formal_groups import FORMAL_SCORE_LABEL
from backend.report_inputs import normalize_code
from backend.risk_metric_formula_shadow import apply_metric_variant, membership_delta
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline, write_baseline_verification
from bond_fund_screening.config import CALMAR_MIN_DD_BY_STYLE, RISK_FREE_RATE
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload


VARIANTS: tuple[tuple[str, str], ...] = (
    ("production_capped_score_raw_display", "生产基线：评分封顶、展示 raw"),
    ("raw_sharpe_sortino_score", "Sharpe/Sortino 全链 raw"),
    ("consistent_capped_sharpe_sortino", "Sharpe/Sortino 全链封顶"),
    ("standard_calmar_recomputed", "标准 Calmar 重算对照"),
    ("style_floor_calmar", "Calmar 风格回撤下限、不扣无风险利率"),
    ("layer6_adjusted_calmar", "Layer6 调整后 Calmar"),
    ("sortino_zero_downside_missing", "Sortino 零下行保持缺失"),
    ("sortino_zero_downside_bounded", "Sortino 零下行正收益样本取上界"),
)


def _code_set(frame: pd.DataFrame) -> set[str]:
    return {
        normalize_code(value)
        for value in frame.get("基金代码", pd.Series(dtype=object)).tolist()
        if normalize_code(value)
    }


def _score_delta_summary(baseline: pd.DataFrame, scenario: pd.DataFrame) -> dict[str, Any]:
    left = baseline[["基金代码", FORMAL_SCORE_LABEL]].copy()
    right = scenario[["基金代码", FORMAL_SCORE_LABEL]].copy()
    left["基金代码"] = left["基金代码"].map(normalize_code)
    right["基金代码"] = right["基金代码"].map(normalize_code)
    merged = left.merge(right, on="基金代码", suffixes=("_baseline", "_scenario"))
    delta = (
        pd.to_numeric(merged[f"{FORMAL_SCORE_LABEL}_scenario"], errors="coerce")
        - pd.to_numeric(merged[f"{FORMAL_SCORE_LABEL}_baseline"], errors="coerce")
    ).dropna()
    changed = delta[delta.abs().gt(0.00005)]
    return {
        "comparable_count": int(len(delta.index)),
        "changed_count": int(len(changed.index)),
        "median_delta": round(float(delta.median()), 6) if not delta.empty else None,
        "p95_abs_delta": round(float(delta.abs().quantile(0.95)), 6) if not delta.empty else None,
        "max_abs_delta": round(float(delta.abs().max()), 6) if not delta.empty else None,
    }


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise argparse.ArgumentTypeError("The risk-free assumption must be finite")
    return result


def _formula_contract(risk_free_rate: float) -> dict[str, Any]:
    """Persist experimental assumptions separately from the frozen business inputs."""
    files = ("backend/risk_metric_formula_shadow.py", "backend/frozen_strategy_audit.py",
             "backend/frozen_risk_observations.py", "backend/calculation_input_audit.py",
             "backend/calculation_input_capture.py", "core/utils/calculation_capture.py",
             "scripts/audit_risk_metric_formula_shadow.py", "core/bond_fund_screening/config.py")
    return {
        "contract_version": "risk-metric-formula-shadow-2026-09-14.1",
        "risk_free_rate": risk_free_rate,
        "risk_free_rate_source": "declared_counterfactual_parameter_not_historical_fact",
        "min_drawdown_by_style": dict(CALMAR_MIN_DD_BY_STYLE),
        "drawdown_floor_source": "declared_audit_defaults_not_historical_configuration",
        "annual_return_unit": "percent", "max_drawdown_unit": "fraction",
        "standard_calmar": "annual_return / 100 / abs(max_drawdown); zero or missing drawdown is undefined",
        "adjusted_calmar_abs_cap": 20.0,
        "missing_or_nonfinite_drawdown": "undefined; never replaced with a style floor",
        "observed_zero_drawdown": "standard undefined; adjusted variants use the declared style floor",
        "sortino_zero_downside_positive_bound": 10.0,
        "historical_nav_window_available": False,
        "sortino_input_policy": "Recorded standard-metric operands linked through period and write calls; all projected risk fields must match the frozen report",
        "sortino_daily_mar": "Original captured annual MAR / original captured periods per year",
        "sortino_fallback_boundary": "Original full-sample downside second moment < 1e-20; exact zero is counted separately",
        "sortino_bounded_variant_is_investment_score_hypothesis": True,
        "code_sha256": {name: file_sha256(PROJECT_ROOT / name) for name in files},
    }


def _risk_input_paths(settings: Any, context: dict[str, Any], explicit: list[Path]) -> list[Path]:
    """Use the report's own references plus explicitly named earlier captures."""
    from backend.calculation_input_capture import capture_index_reference
    from backend.collection_calculation_capture import validate_collection_capture_reference

    paths = list(explicit)
    inputs = context["snapshot"]["inputs"]
    reference = inputs.get("calculation_input_capture")
    if reference:
        actual = capture_index_reference(settings, reference["path"])
        for field in ("sha256", "source_workflow_run_id", "capture_complete", "scopes", "period_days"):
            if field not in reference or actual.get(field) != reference[field]:
                raise ValueError("frozen_risk_index_reference_changed")
        # Before collection captures existed, workflow references identified
        # their run through source_workflow_run_id. The original index SHA and
        # full source identity still have to match; no historical input is added.
        scope = reference.get("capture_scope", "workflow")
        run_id = reference.get("capture_run_id", reference["source_workflow_run_id"])
        if actual["capture_scope"] != scope or actual["capture_run_id"] != run_id:
            raise ValueError("frozen_risk_index_reference_changed")
        paths.append(Path(settings.project_root) / actual["path"])
    for reference in inputs.get("collection_calculation_input_captures", []):
        actual = validate_collection_capture_reference(settings, reference)
        paths.append(Path(settings.project_root) / actual["index_path"])
    return paths


def _write_markdown(summary: dict[str, Any], path: Path) -> None:
    display = lambda value: "未计算" if value is None else str(value)
    lines = ["# 风险公式冻结输入审计", "", f"- run：`{summary['run_id']}`",
             f"- 审计状态：`{summary['status']}`",
             f"- 报告筛选基线：`{summary['baseline_verification'].get('status', '未核验')}`",
             f"- 完成情景：{summary['completed_scenario_count']} / {summary['requested_scenario_count']}",
             "- 回放范围：报告候选全集评分、动态阈值、发布筛选；不声称重跑了上游采集及风险计算。", "",
             "| 场景 | 状态 | 正式成员 | 新增 | 移出 |", "| --- | --- | ---: | ---: | ---: |"]
    for item in summary["scenarios"]:
        delta = item.get("formal_membership_vs_actual") or {}
        lines.append(f"| {item['label']} | {item['status']} | {display(item['formal_count'])} | "
                     f"{display(delta.get('added_count'))} | {display(delta.get('removed_count'))} |")
    lines.extend(["", "缺少当时的 Tracker、AutoStock、新增样本安全复核或 NAV 窗口时，完整名单和成员变化保持缺失；部分情景完成不代表整体审计通过。",
                  "无风险利率、风格回撤下限和封顶值是本次明确保存的实验假设，不冒充历史真实配置。",
                  "本次只比较计算定义对筛选的影响；改变生产策略仍需成熟前瞻结果。", ""])
    risk_inputs = summary.get("frozen_risk_inputs")
    if risk_inputs:
        lines.extend([f"原风险输入绑定：{risk_inputs['status']}；匹配 {display(risk_inputs.get('matched_risk_rows'))} 条风险行。",
                      "基金、周期、计算时间、来源签名及全部风险投影值必须一致；未计算风险的基金保持缺失。",
                      "零下行分类使用原调用的 MAR 和频率；上界情景仅是评分政策假设，不是有限标准 Sortino 的估计值。", ""])
    with path.open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="先验证冻结输入基线，再比较风险指标公式；禁止用后续数据库猜测历史。")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--report", type=Path, help="显式真实报告，包括标明诊断身份的候选")
    parser.add_argument("--baseline-only", action="store_true", help="仅核验冻结生产基线，不进行公式实验。")
    parser.add_argument("--risk-free-rate", type=_finite_float, default=float(RISK_FREE_RATE),
                        help="反事实公式的年化无风险利率假设，不冒充历史实际配置")
    parser.add_argument("--risk-input-index", action="append", type=Path, default=[],
                        help="报表复用的原风险计算索引，可重复指定；必须逐字段匹配当时报表，不读取当前数据库")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            payload = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not payload.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = payload["run_id"]
        else:
            run_id, payload, _, _ = resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    summary = {
        "schema_version": 4, "input_contract": FROZEN_STRATEGY_CONTRACT,
        "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
        "source_report": (payload.get("artifacts") or {}).get("report_json"),
        "source_publication_control": payload.get("publication_control"),
        "status": "baseline_not_verified", "passed": False, "baseline_verification": {},
        "scope": "report_selection_from_frozen_complete_universe",
        "baseline_verification_artifact": None, "source_verified_after_replay": False,
        "formula_contract": _formula_contract(args.risk_free_rate),
        "risk_free_rate": args.risk_free_rate,
        "risk_free_rate_source": "declared_counterfactual_parameter_not_historical_fact",
        "scoring_universe_count": None, "baseline_actual_formal_count": None,
        "scenarios": [], "requested_scenario_count": 0 if args.baseline_only else len(VARIANTS),
        "completed_scenario_count": 0, "strategy_comparison_allowed": False,
        "production_switch_recommended": False, "observation_only": True,
        "production_formulas_changed": False, "later_database_reads": False,
        "frozen_risk_inputs": None, "risk_input_sources": [], "risk_input_sources_verified_after_replay": False,
        "runtime_upstream_requests": 0, "wall_clock_used_for_replayed_gates": False,
        "missing_business_value_imputation": False,
        "external_references": {
            "calmar": "https://raw.githubusercontent.com/braverock/PerformanceAnalytics/master/R/CalmarRatio.R",
            "sortino": "https://raw.githubusercontent.com/braverock/PerformanceAnalytics/master/R/SortinoRatio.R",
            "downside_deviation": "https://raw.githubusercontent.com/braverock/PerformanceAnalytics/master/R/DownsideDeviation.R"},
    }
    exports: dict[str, pd.DataFrame] = {}
    risk_evidence = None
    try:
        binding = bind_frozen_report_sources(settings, payload)
        summary["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, payload)
        baseline.setdefault("run_id", run_id)
        summary["baseline_verification"] = baseline
        if baseline["baseline_verified"]:
            universe = context["frames"]["scored_universe_before_holding_gate"]
            actual = context["frames"]["formal_after_all_filters"]
            actual_codes = _code_set(actual)
            summary.update(scoring_universe_count=len(universe), baseline_actual_formal_count=len(actual_codes),
                           evaluation_times=baseline["evaluation_times"], snapshot_date=context["snapshot"]["snapshot_date"])
            if args.baseline_only:
                summary.update(status="baseline_verified", passed=True)
                exports["baseline_members"] = actual
            else:
                memberships: list[dict[str, Any]] = []
                risk_binding = {"complete": False}
                if any(variant.startswith("sortino_zero_downside") for variant, _ in VARIANTS):
                    input_paths = _risk_input_paths(settings, context, args.risk_input_index)
                    scratch_parent = settings.reports_dir / "audit/risk_metric_formula_shadow"
                    scratch_parent.mkdir(parents=True, exist_ok=True)
                    with tempfile.TemporaryDirectory(prefix=".risk-inputs-", dir=scratch_parent) as scratch:
                        risk_evidence = read_frozen_risk_observations(input_paths,
                            project_root=settings.project_root, scratch_dir=Path(scratch),
                            log=lambda message: print(message, flush=True))
                    risk_binding = bind_report_risk_observations(universe, risk_evidence,
                                                                 generated_at=payload["generated_at"])
                    summary["frozen_risk_inputs"] = {key: value for key, value in risk_binding.items() if key != "rows"}
                    summary["risk_input_sources"] = risk_evidence["sources"]
                    summary["formula_contract"]["historical_nav_window_available"] = bool(
                        risk_binding["complete"] and risk_binding.get("matched_risk_rows"))
                    if risk_binding["rows"]:
                        exports["frozen_sortino_observations"] = pd.DataFrame(risk_binding["rows"])
                for variant, label in VARIANTS:
                    item = {"variant": variant, "label": label, "status": "not_evaluated",
                            "strategy_comparison_allowed": False, "formal_count": None,
                            "dynamic_reference_count": None, "post_review_count": None,
                            "formal_membership_vs_actual": None, "score_delta_vs_production": None}
                    if variant.startswith("sortino_zero_downside") and not risk_binding["complete"]:
                        item.update(status="missing_frozen_nav_window",
                                    reason="The complete report risk universe must match its original captured operands",
                                    missing_risk_input_codes=risk_binding.get("missing_risk_input_codes", []),
                                    source_risk_row_mismatch_codes=risk_binding.get("source_risk_row_mismatch_codes", []))
                    else:
                        changed_universe = apply_metric_variant(
                            universe, variant=variant, risk_free_rate=args.risk_free_rate,
                            min_drawdown_by_style=summary["formula_contract"]["min_drawdown_by_style"],
                            zero_downside_codes=set(risk_binding.get("zero_downside_codes", [])),
                            positive_zero_downside_codes=set(risk_binding.get("positive_zero_downside_codes", [])),
                        )
                        # Score the full style population before the unchanged holding-period gate.
                        scored = report_builder._assign_project_scores(changed_universe)
                        result = run_frozen_strategy_scenario(context, scored_universe=scored)
                        item.update(status=result["status"], score_delta_vs_production=_score_delta_summary(universe, scored),
                                    **missing_scenario_inputs(result))
                        for key, frame_key in (("dynamic_reference_count", "dynamic"), ("post_review_count", "post_review")):
                            if frame_key in result:
                                item[key] = len(result[frame_key])
                        if result["status"] == "computed":
                            codes = _code_set(result["official"])
                            item.update(strategy_comparison_allowed=True, formal_count=len(codes),
                                        formal_membership_vs_actual=membership_delta(actual_codes, codes))
                            exports[f"{variant}_scenario_members"] = result["official"]
                            for code in sorted(actual_codes | codes):
                                memberships.append({"variant": variant, "基金代码": code, "actual_formal": code in actual_codes,
                                                    "scenario_formal": code in codes})
                        else:
                            gaps = scenario_gap_frame(result)
                            if not gaps.empty:
                                exports[f"{variant}_missing_inputs"] = gaps
                    summary["scenarios"].append(item)
                completed = sum(item["status"] == "computed" for item in summary["scenarios"])
                complete = completed == len(VARIANTS)
                summary.update(status="computed" if complete else "scenario_inputs_incomplete", passed=complete,
                               completed_scenario_count=completed, strategy_comparison_allowed=complete)
                if completed:
                    exports["membership"] = pd.DataFrame(memberships, columns=["variant", "基金代码", "actual_formal", "scenario_formal"])
        if binding != bind_frozen_report_sources(settings, payload):
            raise ValueError("frozen_source_changed_during_replay")
        if risk_evidence is not None:
            verify_risk_observation_sources(risk_evidence, project_root=settings.project_root)
            summary["risk_input_sources_verified_after_replay"] = bool(risk_evidence["sources"])
        summary["source_verified_after_replay"] = True
        summary["baseline_verification_artifact"] = str(write_baseline_verification(settings, baseline))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="frozen_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}",
                       source_verified_after_replay=False, strategy_comparison_allowed=False,
                       scoring_universe_count=None, baseline_actual_formal_count=None,
                       scenarios=[], completed_scenario_count=0, frozen_risk_inputs=None,
                       risk_input_sources_verified_after_replay=False)
        summary["formula_contract"]["historical_nav_window_available"] = False
        exports = {}

    audit_dir = settings.reports_dir / "audit/risk_metric_formula_shadow"
    path = build_audit_artifact_path(audit_dir, "risk_metric_formula_shadow", run_id=run_id)
    summary["artifacts"] = {}
    for name, frame in exports.items():
        output = path.with_name(f"{path.stem}_{name}.csv.gz")
        metadata = write_compressed_audit_csv(output, frame)
        summary["artifacts"][name] = {"path": str(output.relative_to(settings.project_root)), **metadata}
    with path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(summary, path.with_suffix(".md"))
    print(json.dumps({"artifact": str(path), "artifact_path": str(path), "artifact_sha256": file_sha256(path),
                      "run_id": run_id, "passed": summary["passed"], "status": summary["status"],
                      "baseline_status": summary["baseline_verification"].get("status"),
                      "scenarios": [{key: item[key] for key in ("variant", "status", "formal_count", "strategy_comparison_allowed")}
                                    for item in summary["scenarios"]]}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
