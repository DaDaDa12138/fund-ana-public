"""Audit scale-field consistency for the enhanced-bond gates.

This is a read-only shadow audit.  It replays one frozen report and evaluates
the enhanced candidates under the former mixed policy and two unified policies:

* ``legacy_mixed``: dynamic gate uses merged/family scale while the
  supplemental gate uses individual share scale (the former production rule);
* ``unified_aggregate``: both gates use the merged/family resolver;
* ``unified_share``: both gates use the individual share field.

The script deliberately does not mutate production thresholds, the database,
or publication pointers.  Counts are gate-level diagnostics, not publication
membership.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from backend.frozen_strategy_audit import FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources
from backend.project_publish_decisions import ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS
from backend.project_quality_thresholds import (
    resolve_scale_for_thresholds,
    _safe_float,
    apply_dynamic_quality_thresholds,
    apply_enhanced_supplemental_gate,
)
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload


ENHANCED_STYLES = frozenset(ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS)
SCALE_POLICIES = ("legacy_mixed", "unified_aggregate", "unified_share")
GATE_BASELINE_CHECKS = (
    "complete_scoring_universe",
    "style_thresholds",
    "global_thresholds",
    "style_meta",
    "candidate_decisions_and_reasons",
)
SUPPLEMENTAL_REASON_CODES = {
    "年化收益不足": "annual_return_below_floor",
    "夏普不足": "sharpe_below_floor",
    "卡玛不足": "calmar_below_floor",
    "回撤偏高": "drawdown_above_cap",
    "正收益率不足": "positive_ratio_below_floor",
    "费率数据缺失": "fee_data_missing",
    "费率偏高": "fee_above_cap",
    "规模偏小": "scale_below_floor",
}


def _normalise_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def _scale_value(value: Any) -> float | None:
    return _safe_float(value)


def _share_scale(row: dict[str, Any]) -> float | None:
    # Keep the production supplemental precedence: individual share size,
    # then its legacy alias.  Missing remains missing; callers apply the gate
    # floor without turning it into a positive value.
    for key in ("基金规模(亿)", "scale_billion"):
        value = _scale_value(row.get(key))
        if value is not None:
            return value
    return None


def _aggregate_scale(row: dict[str, Any]) -> float | None:
    # This delegates to the exact production resolver used by the dynamic gate.
    return resolve_scale_for_thresholds(row)


def _scale_for_policy(row: dict[str, Any], policy: str, *, dynamic: bool) -> float | None:
    if policy == "legacy_mixed":
        return _aggregate_scale(row) if dynamic else _share_scale(row)
    if policy == "unified_aggregate":
        return _aggregate_scale(row)
    if policy == "unified_share":
        return _share_scale(row)
    raise ValueError(f"unknown_scale_policy:{policy}")


def _gate_scope_verification(baseline: dict[str, Any]) -> dict[str, Any]:
    checks = {
        str(item.get("name") or ""): bool(item.get("aligned"))
        for item in baseline.get("checks", [])
    }
    missing = [name for name in GATE_BASELINE_CHECKS if name not in checks]
    failed = [name for name in GATE_BASELINE_CHECKS if name in checks and not checks[name]]
    return {
        "gate_inputs_verified": not missing and not failed,
        "full_report_baseline_verified": bool(baseline.get("baseline_verified")),
        "required_checks": list(GATE_BASELINE_CHECKS),
        "missing_checks": missing,
        "failed_checks": failed,
        "scope_note": (
            "Gate-only shadow may proceed when the full report replay differs only in post-gate confidence/final-selection fields; "
            "this does not validate final publication ranking."
        ),
    }


def _apply_dynamic_scale(frame: pd.DataFrame, values: list[float | None]) -> pd.DataFrame:
    current = frame.copy()
    for column in ("合并规模(亿)", "effective_scale_billion", "family_scale_billion", "基金规模(亿)", "scale_billion"):
        current[column] = None
    # The production resolver's first field now carries the policy-selected
    # value. Missing values remain NULL and continue to fail the scale floor.
    current["合并规模(亿)"] = values
    return current


def _apply_supplemental_scale(frame: pd.DataFrame, values: list[float | None]) -> pd.DataFrame:
    current = frame.copy()
    for column in ("合并规模(亿)", "effective_scale_billion", "family_scale_billion", "基金规模(亿)", "scale_billion"):
        current[column] = None
    current["合并规模(亿)"] = values
    current["基金规模(亿)"] = values
    current["scale_billion"] = values
    return current


def _count_reasons(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(row.get(key) or [])
    return dict(sorted(counts.items()))


def _evaluate_candidates(candidate: pd.DataFrame, result: dict[str, Any]) -> tuple[dict[str, Any], pd.DataFrame]:
    source_rows = candidate.loc[
        candidate["基础门禁通过"].astype(bool)
        & candidate["债基风格标签"].astype(str).isin(ENHANCED_STYLES)
    ].copy()
    records: list[dict[str, Any]] = []
    evaluated_by_policy: dict[str, pd.DataFrame] = {}
    for policy in SCALE_POLICIES:
        dynamic_values = [
            _scale_for_policy(row, policy, dynamic=True)
            for row in source_rows.to_dict(orient="records")
        ]
        dynamic_input = _apply_dynamic_scale(source_rows, dynamic_values)
        dynamic_result = apply_dynamic_quality_thresholds(
            dynamic_input,
            style_thresholds=result["style_thresholds"],
            global_thresholds=result["global_thresholds"],
            style_meta=result["style_meta"],
        )
        supplemental_values = [
            _scale_for_policy(row, policy, dynamic=False)
            for row in source_rows.to_dict(orient="records")
        ]
        supplemental_input = _apply_supplemental_scale(dynamic_result, supplemental_values)
        evaluated_by_policy[policy] = apply_enhanced_supplemental_gate(supplemental_input)

    for index, (_, original) in enumerate(source_rows.iterrows()):
        row = original.to_dict()
        entry: dict[str, Any] = {
            "基金代码": _normalise_code(row.get("基金代码")),
            "基金名称": str(row.get("基金名称") or ""),
            "债基风格标签": str(row.get("债基风格标签") or "未知"),
            "基金规模(亿)": _scale_value(row.get("基金规模(亿)")),
            "合并规模(亿)": _scale_value(row.get("合并规模(亿)")),
            "family_scale_billion": _scale_value(row.get("family_scale_billion")),
            "effective_scale_billion": _scale_value(row.get("effective_scale_billion")),
            "关联份额数": _scale_value(row.get("关联份额数")),
        }
        policy_results: dict[str, Any] = {}
        for policy in SCALE_POLICIES:
            evaluated = evaluated_by_policy[policy].iloc[index]
            dynamic_reasons = [
                item for item in str(evaluated.get("dynamic_threshold_reason_codes") or "").split("|") if item
            ]
            supplemental_reasons = [
                SUPPLEMENTAL_REASON_CODES[item]
                for item in str(evaluated.get("enhanced_supplemental_reason") or "").split("；")
                if item in SUPPLEMENTAL_REASON_CODES
            ]
            dynamic_pass = str(evaluated.get("dynamic_threshold_verdict") or "") == "正式推荐"
            supplemental_pass = bool(evaluated.get("enhanced_supplemental_pass"))
            policy_results[policy] = {
                "dynamic_scale_billion": _scale_for_policy(row, policy, dynamic=True),
                "supplemental_scale_billion": _scale_for_policy(row, policy, dynamic=False),
                "dynamic_pass": dynamic_pass,
                "dynamic_reason_codes": dynamic_reasons,
                "supplemental_pass": supplemental_pass,
                "supplemental_reason_codes": supplemental_reasons,
                "formal_gate_pass": dynamic_pass or supplemental_pass,
            }
        entry["policies"] = policy_results
        records.append(entry)

    summary: dict[str, Any] = {
        "candidate_count": int(len(source_rows.index)),
        "candidate_by_style": {
            str(style): int(count)
            for style, count in source_rows["债基风格标签"].astype(str).value_counts().sort_index().items()
        },
        "policies": {},
    }
    for policy in SCALE_POLICIES:
        policy_rows = [row["policies"][policy] for row in records]
        summary["policies"][policy] = {
            "dynamic_pass_count": int(sum(item["dynamic_pass"] for item in policy_rows)),
            "supplemental_pass_count": int(sum(item["supplemental_pass"] for item in policy_rows)),
            "formal_gate_pass_count": int(sum(item["formal_gate_pass"] for item in policy_rows)),
            "dynamic_reason_hits": _count_reasons(policy_rows, "dynamic_reason_codes"),
            "supplemental_reason_hits": _count_reasons(policy_rows, "supplemental_reason_codes"),
            "dynamic_pass_by_style": {
                style: int(sum(
                    item["policies"][policy]["dynamic_pass"]
                    for item in records
                    if item["债基风格标签"] == style
                ))
                for style in sorted(ENHANCED_STYLES)
            },
            "supplemental_pass_by_style": {
                style: int(sum(
                    item["policies"][policy]["supplemental_pass"]
                    for item in records
                    if item["债基风格标签"] == style
                ))
                for style in sorted(ENHANCED_STYLES)
            },
        }

    baseline_dynamic = source_rows["dynamic_threshold_verdict"].astype(str).eq("正式推荐").tolist()
    baseline_supplemental = source_rows["enhanced_supplemental_pass"].astype(bool).tolist()
    current_dynamic = [row["policies"]["unified_aggregate"]["dynamic_pass"] for row in records]
    current_supplemental = [row["policies"]["unified_aggregate"]["supplemental_pass"] for row in records]
    summary["current_production_parity"] = {
        "dynamic_match_count": int(sum(left == right for left, right in zip(baseline_dynamic, current_dynamic))),
        "supplemental_match_count": int(sum(left == right for left, right in zip(baseline_supplemental, current_supplemental))),
        "candidate_count": int(len(source_rows.index)),
        "matched": baseline_dynamic == current_dynamic and baseline_supplemental == current_supplemental,
    }
    if not summary["current_production_parity"]["matched"]:
        raise ValueError("current_scale_policy_does_not_reproduce_frozen_candidate_gate")

    view_rows: list[dict[str, Any]] = []
    for row in records:
        view: dict[str, Any] = {
            key: row[key]
            for key in (
                "基金代码", "基金名称", "债基风格标签", "基金规模(亿)",
                "合并规模(亿)", "family_scale_billion", "effective_scale_billion", "关联份额数",
            )
        }
        for policy in SCALE_POLICIES:
            payload = row["policies"][policy]
            prefix = policy
            view[f"{prefix}_dynamic_scale_billion"] = payload["dynamic_scale_billion"]
            view[f"{prefix}_supplemental_scale_billion"] = payload["supplemental_scale_billion"]
            view[f"{prefix}_dynamic_pass"] = payload["dynamic_pass"]
            view[f"{prefix}_dynamic_reason_codes"] = "|".join(payload["dynamic_reason_codes"])
            view[f"{prefix}_supplemental_pass"] = payload["supplemental_pass"]
            view[f"{prefix}_supplemental_reason_codes"] = "|".join(payload["supplemental_reason_codes"])
            view[f"{prefix}_formal_gate_pass"] = payload["formal_gate_pass"]
        view_rows.append(view)
    return summary, pd.DataFrame(view_rows)


def _write_markdown(summary: dict[str, Any], path: Path) -> None:
    policies = summary["shadow"]["policies"]
    lines = [
        "# 增强债规模口径统一影子审计",
        "",
        f"- 生成时间：`{summary['generated_at']}`",
        f"- 冻结批次：`{summary['run_id']}`",
        f"- 修订：`{summary['revision']}`",
        f"- 输入契约：`{summary['input_contract']}`",
        f"- 状态：`{summary['status']}`",
        "",
        "本报告只复算门禁层，不修改生产阈值、数据库、正式报告或发布指针。所有计数均为增强风格且已通过基础门禁的候选（本批120只），不能直接视为正式发布成员。",
        "",
        "## 口径对照",
        "",
        "| 影子口径 | 动态门禁规模 | 补充门禁规模 | 动态通过 | 补充通过 | 合并通过 |",
        "|---|---|---|---:|---:|---:|",
    ]
    labels = {
        "legacy_mixed": "旧版混用口径",
        "unified_aggregate": "统一合并/家族规模",
        "unified_share": "统一单份额规模",
    }
    for policy in SCALE_POLICIES:
        row = policies[policy]
        lines.append(
            f"| {labels[policy]} | {'单份额' if policy == 'unified_share' else '合并/家族'} | "
            f"{'单份额' if policy == 'legacy_mixed' else ('合并/家族' if policy == 'unified_aggregate' else '单份额')} | "
            f"{row['dynamic_pass_count']} | {row['supplemental_pass_count']} | {row['formal_gate_pass_count']} |"
        )
    lines.extend([
        "",
        "## 当前口径与统一口径差异",
        "",
        "- 当前实现统一使用合并/家族规模；旧版补充门禁使用单份额规模，本影子将旧逻辑保留为历史对照。",
        f"- 当前统一合并/家族与旧版混用的正式门禁层通过数差异：{policies['unified_aggregate']['formal_gate_pass_count'] - policies['legacy_mixed']['formal_gate_pass_count']}。",
        f"- 统一单份额与旧版混用的正式门禁层通过数差异：{policies['unified_share']['formal_gate_pass_count'] - policies['legacy_mixed']['formal_gate_pass_count']}。",
        "- 若口径变化使候选通过，仍需重新经过经理、规模时效、前瞻验证、母基金去重和最终发布门禁；本影子不授权生产切换。",
        "",
        "## 复算边界",
        "",
        "- 规模缺失保持缺失并按门禁失败处理，不将缺失转换为真实零值或通过。",
        "- 近规模门槛缓释规则随所选规模口径复算；其他收益、风险、费率与基础门禁规则保持冻结版本。",
        "- 需要在多个独立冻结快照、母基金去重和费后前瞻结果中验证后，才能形成生产决策。",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="冻结报告上的增强债规模口径影子审计")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定冻结 run；默认读取当前正式报告")
    source.add_argument("--report", type=Path, help="指定冻结报告 JSON")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            report_path = args.report.resolve()
            report = json.loads(report_path.read_text(encoding="utf-8"))
            if not isinstance(report, dict) or not report.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = str(report["run_id"])
        else:
            run_id, report, _, _ = resolve_run_payload(
                settings, run_id=args.run_id, project_root=settings.project_root
            )
            report_path = Path(report.get("artifacts", {}).get("report_json") or "")
            if not report_path.is_absolute():
                report_path = settings.project_root / report_path
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    revision = str((report.get("publication_control") or {}).get("revision") or "")
    summary: dict[str, Any] = {
        "schema_version": 1,
        "input_contract": FROZEN_STRATEGY_CONTRACT,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "revision": revision,
        "status": "baseline_not_verified",
        "passed": False,
        "production_thresholds_changed": False,
        "production_data_modified": False,
        "publication_pointer_modified": False,
        "source_report": {
            "path": str(report_path),
            "sha256": file_sha256(report_path) if report_path.is_file() else None,
        },
    }
    try:
        binding = bind_frozen_report_sources(settings, report)
        baseline, context = verify_frozen_baseline(settings, report)
        summary["baseline_verification"] = baseline
        summary["source_binding"] = binding
        gate_scope = _gate_scope_verification(baseline)
        summary["gate_scope_verification"] = gate_scope
        if not gate_scope["gate_inputs_verified"]:
            summary.update(status="gate_inputs_not_verified", recommendation="冻结门禁输入未通过，停止规模比较")
        else:
            result = context["result"]
            candidate = result["candidate"]
            shadow, detail = _evaluate_candidates(candidate, result)
            summary["status"] = "computed"
            summary["passed"] = True
            summary["shadow"] = shadow
            summary["detail"] = detail
            summary["input"] = {
                "candidate_count_after_base_gate": shadow["candidate_count"],
                "candidate_by_style": shadow["candidate_by_style"],
                "candidate_frame_columns": [
                    "基金代码", "债基风格标签", "基金规模(亿)", "合并规模(亿)",
                    "effective_scale_billion", "family_scale_billion", "关联份额数",
                ],
                "dynamic_threshold_source": "frozen result style_thresholds/global_thresholds",
                "supplemental_threshold_source": "backend.project_publish_decisions.ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS",
            }
            summary["recommendation"] = (
                "当前统一采用合并/家族规模以代表底层基金承载规模；单份额规模继续作为原始字段和诊断信息。"
                "维持所有规模绝对下限与近门槛规则；在多快照、份额级流动性研究和前瞻验证前，不据此放宽正式门禁。"
            )
        if binding != bind_frozen_report_sources(settings, report):
            raise ValueError("frozen_source_changed_during_replay")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="shadow_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}")

    audit_dir = settings.reports_dir / "audit" / "enhanced_bond_shadow"
    audit_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = audit_dir / f"{run_id}_scale_gate_shadow.json"
    if artifact_path.exists():
        artifact_path = build_audit_artifact_path(audit_dir, "scale_gate_shadow", run_id=run_id)
    detail = summary.pop("detail", None)
    artifact_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    md_path = artifact_path.with_suffix(".md")
    if summary.get("status") == "computed":
        _write_markdown(summary, md_path)
        if detail is not None:
            detail.to_csv(artifact_path.with_name(f"{artifact_path.stem}_detail.csv"), index=False)
    print(json.dumps({
        "artifact_path": str(artifact_path),
        "markdown_path": str(md_path) if md_path.exists() else None,
        "passed": summary["passed"],
        "status": summary["status"],
        "run_id": run_id,
        "revision": revision,
        "policies": (summary.get("shadow") or {}).get("policies"),
    }, ensure_ascii=False))
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
