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
from backend.diagnosis_contract import diagnosis_categories
from backend.project_publish_decisions import AUTO_DIAGNOSIS_CATEGORY_COLUMN
from backend.report_inputs import resolve_runtime_artifact_path as _resolve_runtime_artifact_path
from backend.project_formal_groups import (
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
)
from backend.publish_pipeline import normalize_code as _normalize_code
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload as _resolve_run_payload


def _extract_artifact_maps(latest_payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    artifacts = latest_payload.get("artifacts") or {}
    if not isinstance(artifacts, dict):
        artifacts = {}
    core_artifacts = artifacts.get("core") or {}
    if not isinstance(core_artifacts, dict):
        core_artifacts = {}
    return artifacts, core_artifacts


def _extract_final_selection_stage_summary(latest_payload: dict[str, Any]) -> dict[str, Any]:
    def optional_count(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("invalid_frozen_final_stage_count")
        return value

    project_publish_summary = latest_payload.get("project_publish_summary") or {}
    if not isinstance(project_publish_summary, dict):
        project_publish_summary = {}
    publish_reason_summary = project_publish_summary.get("publish_reason_summary") or {}
    if not isinstance(publish_reason_summary, dict):
        publish_reason_summary = {}
    final_stage = publish_reason_summary.get("final_stage") or {}
    if not isinstance(final_stage, dict) or not final_stage:
        return {
            "available": False,
            "stage_key": "final_selection",
            "input_count": None,
            "passed_count": None,
            "removed_count": None,
            "lead_reason": {},
            "top_reasons": [],
            "stability_cap_count": None,
            "stability_cap_removed_share": None,
            "stability_cap_dominant": None,
            "explanation": "",
        }

    top_reasons = final_stage.get("top_reasons") or []
    if not isinstance(top_reasons, list):
        top_reasons = []
    normalized_top_reasons = [
        {
            "code": str(item.get("code") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "display_reason": str(item.get("display_reason") or "").strip(),
            "count": optional_count(item.get("count")),
        }
        for item in top_reasons
        if isinstance(item, dict)
    ]
    lead_reason = final_stage.get("lead_reason") or {}
    if not isinstance(lead_reason, dict):
        lead_reason = {}
    removed_count = optional_count(final_stage.get("removed_count"))
    stability_cap_count = next(
        (
            item["count"]
            for item in normalized_top_reasons
            if item["code"] == "final_selection_stability_cap"
        ),
        0 if removed_count == 0 else None,
    )
    stability_cap_removed_share = (
        round(stability_cap_count / removed_count, 6)
        if removed_count is not None and removed_count > 0 and stability_cap_count is not None else None
    )
    lead_reason_code = str(lead_reason.get("code") or "").strip()
    stability_cap_dominant = (
        False if removed_count == 0 else None
        if stability_cap_removed_share is None or not lead_reason_code else
        lead_reason_code == "final_selection_stability_cap" and stability_cap_removed_share >= 0.8
    )
    return {
        "available": True,
        "stage_key": str(final_stage.get("stage_key") or "final_selection").strip() or "final_selection",
        "stage_label": str(final_stage.get("stage_label") or "最终精选").strip() or "最终精选",
        "input_count": optional_count(final_stage.get("input_count")),
        "passed_count": optional_count(final_stage.get("passed_count")),
        "removed_count": removed_count,
        "lead_reason": {
            "code": lead_reason_code,
            "reason": str(lead_reason.get("reason") or "").strip(),
            "display_reason": str(lead_reason.get("display_reason") or "").strip(),
            "count": optional_count(lead_reason.get("count")),
        },
        "top_reasons": normalized_top_reasons,
        "stability_cap_count": stability_cap_count,
        "stability_cap_removed_share": stability_cap_removed_share,
        "stability_cap_dominant": stability_cap_dominant,
        "explanation": str(final_stage.get("explanation") or "").strip(),
    }


def _load_core_selected_records(
    decision_map: dict[str, Any],
) -> pd.DataFrame:
    if not isinstance(decision_map, dict) or not decision_map:
        raise ValueError("missing_frozen_core_decisions")
    for code, record in decision_map.items():
        if not code or not isinstance(record, dict) or _normalize_code(record.get("fcode")) != code:
            raise ValueError("invalid_frozen_core_decision_identity")
        if not isinstance(record.get("final_mainlist_selected"), bool):
            raise ValueError("missing_or_invalid_frozen_core_selection_flag")
    records = list(decision_map.values())
    df = pd.DataFrame(records)
    df = df[df["final_mainlist_selected"]].copy()
    df["基金代码"] = df.get("fcode", "").astype(str).map(_normalize_code)
    df["核心类别"] = df.get("fund_category", pd.Series(index=df.index, dtype=object)).fillna("未知").astype(str)
    df["核心推荐状态"] = df.get("recommendation_status", pd.Series(index=df.index, dtype=object)).fillna("未知").astype(str)
    df["核心加权得分"] = pd.to_numeric(df.get("weighted_score"), errors="coerce")
    keep_columns = [
        "基金代码",
        "fund_name",
        "核心类别",
        "candidate_stage",
        "selected_pool_status",
        "核心推荐状态",
        "autostock_validation_status",
        "cross_period_support_count",
        "watch_only_reason_code",
        "soft_intersection_reason",
        "核心加权得分",
    ]
    keep_columns = [column for column in keep_columns if column in df.columns]
    return df[keep_columns].rename(columns={"fund_name": "基金名称"})


def _count_by_reason(series: pd.Series, limit: int = 20) -> list[dict[str, Any]]:
    values = series.fillna("未知").astype(str).map(lambda text: text.strip() or "未知").value_counts().head(limit)
    return [{"reason": str(index), "count": int(value)} for index, value in values.items()]


def _codes_from_df(df: pd.DataFrame) -> set[str]:
    if df.empty or "基金代码" not in df.columns:
        return set()
    return {
        _normalize_code(code)
        for code in df["基金代码"].astype(str).tolist()
        if _normalize_code(code)
    }


def _build_stage_rows(
    *,
    total_core_codes: set[str],
    in_project_universe_codes: set[str],
    after_holding_gate_codes: set[str],
    base_gate_pass_codes: set[str],
    dynamic_pass_codes: set[str],
    publish_formal_codes: set[str],
    final_official_codes: set[str],
) -> list[dict[str, Any]]:
    ordered = [
        ("核心主榜入选", total_core_codes),
        ("进入项目候选宇宙", in_project_universe_codes),
        ("通过持有期硬门槛", after_holding_gate_codes),
        ("通过基础门禁", base_gate_pass_codes),
        ("通过动态阈值", dynamic_pass_codes),
        ("通过发布后复核", publish_formal_codes),
        ("进入最终正式主榜", final_official_codes),
    ]
    rows: list[dict[str, Any]] = []
    previous_codes: set[str] | None = None
    for stage_name, stage_codes in ordered:
        current_codes = set(stage_codes)
        if previous_codes is not None and not current_codes.issubset(previous_codes):
            raise ValueError(f"core_overlap_stage_not_nested:{stage_name}")
        passed_count = int(len(current_codes))
        if previous_codes is None:
            dropped_count = 0
            retained_ratio = 1.0 if current_codes else None
        else:
            dropped_count = int(len(previous_codes - current_codes))
            retained_ratio = round(passed_count / len(previous_codes), 6) if previous_codes else None
        rows.append(
            {
                "stage": stage_name,
                "passed_count": passed_count,
                "dropped_count_vs_prev": dropped_count,
                "retained_ratio_vs_prev": retained_ratio,
            }
        )
        previous_codes = current_codes
    return rows


def _build_compare_frame(
    *,
    core_selected_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    baseline_pool_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    publish_track_df: pd.DataFrame,
    official_df: pd.DataFrame,
) -> pd.DataFrame:
    compare = core_selected_df.copy()
    compare["基金代码"] = compare["基金代码"].map(_normalize_code)

    universe_view = universe_df.copy()
    universe_view["基金代码"] = universe_view["基金代码"].map(_normalize_code)
    universe_view["在项目候选宇宙"] = True
    universe_view = universe_view[["基金代码", "在项目候选宇宙"]].drop_duplicates(subset=["基金代码"])

    baseline_view = baseline_pool_df.copy()
    baseline_view["基金代码"] = baseline_view["基金代码"].map(_normalize_code)
    baseline_view["通过持有期硬门槛"] = True
    baseline_view = baseline_view[["基金代码", "通过持有期硬门槛"]].drop_duplicates(subset=["基金代码"])

    candidate_view = candidate_df.copy()
    candidate_view["基金代码"] = candidate_view["基金代码"].map(_normalize_code)
    candidate_keep = [
        "基金代码",
        "基金名称",
        "基础门禁通过",
        "基础观察分类",
        "基础推荐理由",
        "dynamic_threshold_verdict",
        "dynamic_threshold_watch_bucket",
        "dynamic_threshold_reason",
        FORMAL_SCORE_LABEL,
        "债基风格标签",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
    ]
    candidate_view = candidate_view[[column for column in candidate_keep if column in candidate_view.columns]].drop_duplicates(subset=["基金代码"])

    publish_view = publish_track_df.copy()
    publish_view[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = diagnosis_categories(publish_view)
    publish_view["基金代码"] = publish_view["基金代码"].map(_normalize_code)
    publish_keep = [
        "基金代码",
        "项目推荐结论",
        AUTO_DIAGNOSIS_CATEGORY_COLUMN,
        "推荐理由",
    ]
    publish_view = publish_view[[column for column in publish_keep if column in publish_view.columns]].drop_duplicates(subset=["基金代码"])

    official_view = official_df.copy()
    official_view["基金代码"] = official_view["基金代码"].map(_normalize_code)
    official_view["进入最终正式主榜"] = True
    official_keep = [
        "基金代码",
        "进入最终正式主榜",
        FORMAL_GROUP_LABEL,
    ]
    official_view = official_view[[column for column in official_keep if column in official_view.columns]].drop_duplicates(subset=["基金代码"])

    compare = compare.merge(universe_view, on="基金代码", how="left")
    compare = compare.merge(baseline_view, on="基金代码", how="left")
    compare = compare.merge(candidate_view, on="基金代码", how="left")
    compare = compare.merge(publish_view, on="基金代码", how="left")
    compare = compare.merge(official_view, on="基金代码", how="left")

    for column in ["在项目候选宇宙", "通过持有期硬门槛", "基础门禁通过", "进入最终正式主榜"]:
        if column in compare.columns:
            series = compare[column]
            compare[column] = series.where(series.notna(), False).infer_objects(copy=False).astype(bool)

    compare["dynamic_threshold_verdict"] = compare.get("dynamic_threshold_verdict", "").fillna("").astype(str)
    compare["项目推荐结论"] = compare.get("项目推荐结论", "").fillna("").astype(str)
    compare["核心到发布状态"] = compare.apply(
        lambda row: (
            "未进入项目候选宇宙"
            if not bool(row.get("在项目候选宇宙"))
            else "被持有期硬门槛拦截"
            if not bool(row.get("通过持有期硬门槛"))
            else "被基础门禁拦截"
            if not bool(row.get("基础门禁通过"))
            else "被动态阈值拦截"
            if str(row.get("dynamic_threshold_verdict") or "").strip() != "正式推荐"
            else "被发布后复核拦截"
            if str(row.get("项目推荐结论") or "").strip() != "正式推荐"
            else "进入最终正式主榜"
            if bool(row.get("进入最终正式主榜"))
            else "通过发布后复核但未入最终主榜"
        ),
        axis=1,
    ) if not compare.empty else pd.Series(index=compare.index, dtype=object)
    compare = compare.sort_values(
        by=["核心到发布状态", "核心类别", FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[True, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return compare


def _write_markdown(
    *,
    run_id: str,
    summary: dict[str, Any],
    output_path: Path,
) -> None:
    if not bool(summary.get("available", True)):
        lines = [
            "# 核心主榜与项目发布链重叠审计",
            "",
            f"- 生成时间：`{summary.get('generated_at', '')}`",
            f"- 基线批次：`{run_id}`",
            f"- 状态：`{summary.get('status_label') or '不可用'}`",
            f"- 原因：{summary.get('detail') or summary.get('error') or '历史输入不足'}",
            "",
            "## 回放精确性",
            "",
            f"- 状态：`{summary.get('replay_precision_status_label') or '未知'}`",
            f"- 说明：{((summary.get('replay_precision') or {}).get('detail')) or '无'}",
            "",
        ]
        output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        return

    lines = [
        "# 核心主榜与项目发布链重叠审计",
        "",
        f"- 生成时间：`{summary['generated_at']}`",
        f"- 基线批次：`{run_id}`",
        f"- 报告版本：`{(summary.get('source_publication_control') or {}).get('revision') or '未知'}`",
        f"- 报告用途：`{(summary.get('source_publication_control') or {}).get('mode') or '未知'}`",
        "- 范围：冻结核心决策与报告选择层；未复算上游采集、风险和九周期筛选。漏斗只统计核心入选与各发布阶段的交集。",
        "",
        "## 核心漏斗",
        "",
    ]
    for row in summary["stage_rows"]:
        ratio_text = "未知" if row["retained_ratio_vs_prev"] is None else str(row["retained_ratio_vs_prev"])
        lines.append(
            f"- {row['stage']}：`{row['passed_count']}`，较上一层淘汰 `-{row['dropped_count_vs_prev']}`，留存率 `{ratio_text}`"
        )
    lines.extend(
        [
            "",
            "## 关键结论",
            "",
            f"- 核心主榜代码总数：`{summary['core_selected_total']}`",
            f"- 能映射到项目候选宇宙的代码：`{summary['matched_project_universe_count']}`",
            f"- 被持有期硬门槛挡下：`{summary['holding_gate_blocked_count']}`",
            f"- 通过动态阈值的核心样本：`{summary['dynamic_pass_count']}`",
            f"- 进入项目正式推荐层的核心样本：`{summary['publish_formal_count']}`",
            f"- 最终仍进入正式主榜的核心样本：`{summary['final_official_count']}`",
            "",
            "## 主要淘汰原因",
            "",
        ]
    )
    for title, rows in [
        ("基础门禁", summary["base_gate_top_reasons"]),
        ("动态阈值", summary["dynamic_top_reasons"]),
        ("发布后复核", summary["publish_post_filter_top_reasons"]),
        ("最终精选", summary["final_selection_top_reasons"]),
    ]:
        lines.append(f"### {title}")
        lines.append("")
        if not rows:
            lines.append("- _无数据_")
        else:
            for item in rows:
                lines.append(f"- {item['reason']}：`{item['count']}`")
        lines.append("")
    final_selection_stage = summary.get("final_selection_stage_summary") or {}
    if final_selection_stage.get("available"):
        cap_share = final_selection_stage.get("stability_cap_removed_share")
        cap_share_text = "未知" if cap_share is None else f"{cap_share:.2%}"
        lines.extend(
            [
                "## 最终精选判断",
                "",
                f"- 全局最终精选输入：`{final_selection_stage['input_count']}`，保留：`{final_selection_stage['passed_count']}`，淘汰：`{final_selection_stage['removed_count']}`",
                (
                    f"- `final_selection_stability_cap` 占最终精选淘汰的比例："
                    f"`{cap_share_text}`"
                ),
                (
                    "- 缺少完整最终精选原因计数，无法判断稳定化收敛是否主导"
                    if final_selection_stage.get("stability_cap_dominant") is None else
                    "- 当前最终精选差异主要表现为发布稳定化收敛，不建议把这一层直接当作高优先级放宽阈值对象"
                    if final_selection_stage.get("stability_cap_dominant")
                    else "- 当前最终精选差异并非单一由稳定化收敛主导，仍需逐项拆解原因"
                ),
                "",
            ]
        )
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _bind_frozen_sources(settings, report: dict[str, Any]) -> dict[str, Any]:
    """Bind the report and its own revision's saved inputs before replay."""
    artifacts = report.get("artifacts") or {}
    report_path = _resolve_runtime_artifact_path(artifacts.get("report_json"), project_root=settings.project_root)
    if report_path is None or not report_path.is_file():
        raise ValueError("source_report_missing")
    if json.loads(report_path.read_text(encoding="utf-8")) != report:
        raise ValueError("source_report_payload_mismatch")
    files = {}
    for key in ("report_json", "screening_input_snapshot_json_gz", "filtered_csv", "watch_csv",
                "dynamic_filtered_csv", "dynamic_watch_csv"):
        path = _resolve_runtime_artifact_path(artifacts.get(key), project_root=settings.project_root)
        if path is None or not path.is_file():
            raise ValueError(f"frozen_source_missing:{key}")
        if path.parent.resolve() != report_path.parent.resolve():
            raise ValueError(f"frozen_source_revision_mismatch:{key}")
        files[key] = {"path": str(path.relative_to(settings.project_root)),
                      "sha256": file_sha256(path)}
    return {"run_id": report["run_id"], "revision": (report.get("publication_control") or {}).get("revision"),
            "files": files}


def main() -> int:
    parser = argparse.ArgumentParser(description="先核验冻结报告基线，再审计核心主榜与发布链的重叠。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定 run_id；默认读取当前正式报告。")
    source.add_argument("--report", type=Path, help="显式冻结报告JSON，可用于诊断候选。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    try:
        if args.report:
            latest_payload = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(latest_payload, dict) or not latest_payload.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = latest_payload["run_id"]
        else:
            run_id, latest_payload, _, _ = _resolve_run_payload(settings, run_id=args.run_id, project_root=project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2
    audit_dir = settings.reports_dir / "audit" / "core_publish_overlap"
    summary_path = build_audit_artifact_path(audit_dir, "core_publish_overlap", run_id=run_id)
    compare_path = summary_path.with_name(f"{summary_path.stem}_compare.csv.gz")
    category_path = summary_path.with_name(f"{summary_path.stem}_category_status.csv.gz")
    markdown_path = summary_path.with_suffix(".md")
    baseline: dict[str, Any] = {}
    binding: dict[str, Any] = {}
    try:
        binding = _bind_frozen_sources(settings, latest_payload)
        baseline, context = verify_frozen_baseline(settings, latest_payload)
        if not baseline.get("baseline_verified"):
            raise ValueError(f"frozen_baseline_not_verified:{baseline.get('status', 'unknown')}")
        if binding != _bind_frozen_sources(settings, latest_payload):
            raise ValueError("frozen_source_changed_during_replay")
        core_selected_df = _load_core_selected_records(context["snapshot"]["inputs"]["decision_map"])
        universe_df = context["frames"]["scored_universe_before_holding_gate"].copy()
        candidate_df = context["result"]["candidate"].copy()
        publish_track_df = context["result"]["publish_track"].copy()
        official_df = context["result"]["official"].copy()
        for frame in (universe_df, candidate_df, publish_track_df, official_df):
            frame["基金代码"] = frame["基金代码"].map(_normalize_code)
        baseline_pool_df = universe_df[universe_df["基金代码"].isin(_codes_from_df(candidate_df))].copy()
        core_selected_codes = _codes_from_df(core_selected_df)

        universe_codes = _codes_from_df(universe_df)
        baseline_pool_codes = _codes_from_df(baseline_pool_df)
        candidate_codes = _codes_from_df(candidate_df)
        base_gate_pass_codes = _codes_from_df(candidate_df[candidate_df["基础门禁通过"]])
        dynamic_pass_codes = _codes_from_df(candidate_df[candidate_df["dynamic_threshold_verdict"] == "正式推荐"])
        publish_formal_codes = _codes_from_df(publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"])
        final_official_codes = _codes_from_df(official_df)

        compare_df = _build_compare_frame(
            core_selected_df=core_selected_df,
            universe_df=universe_df,
            baseline_pool_df=baseline_pool_df,
            candidate_df=candidate_df,
            publish_track_df=publish_track_df,
            official_df=official_df,
        )

        core_in_universe_df = compare_df[compare_df["在项目候选宇宙"]].copy()
        core_after_holding_df = compare_df[compare_df["通过持有期硬门槛"]].copy()
        core_base_gate_blocked_df = compare_df[compare_df["通过持有期硬门槛"] & ~compare_df["基础门禁通过"]].copy()
        core_dynamic_blocked_df = compare_df[
            compare_df["基础门禁通过"] & (compare_df["dynamic_threshold_verdict"] != "正式推荐")
        ].copy()
        core_publish_blocked_df = compare_df[
            (compare_df["dynamic_threshold_verdict"] == "正式推荐")
            & (compare_df["项目推荐结论"] != "正式推荐")
        ].copy()
        core_final_selection_removed_df = compare_df[
            (compare_df["dynamic_threshold_verdict"] == "正式推荐")
            & compare_df["项目推荐结论"].eq("正式推荐")
            & ~compare_df["进入最终正式主榜"]
        ].copy()
        core_final_official_df = compare_df[compare_df["进入最终正式主榜"]].copy()
        final_selection_stage_summary = _extract_final_selection_stage_summary(latest_payload)

        category_summary = (
            compare_df.groupby(["核心类别", "核心到发布状态"])
            .size()
            .reset_index(name="count")
            .sort_values(by=["核心类别", "count"], ascending=[True, False], kind="mergesort")
        )

        stage_rows = _build_stage_rows(
            total_core_codes=core_selected_codes,
            in_project_universe_codes=core_selected_codes & universe_codes,
            after_holding_gate_codes=core_selected_codes & baseline_pool_codes,
            base_gate_pass_codes=core_selected_codes & base_gate_pass_codes,
            dynamic_pass_codes=core_selected_codes & dynamic_pass_codes,
            publish_formal_codes=core_selected_codes & publish_formal_codes,
            final_official_codes=core_selected_codes & final_official_codes,
        )

        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id, "available": True, "passed": True, "status": "matched",
            "status_label": "冻结报告基线一致",
            "baseline_verification": baseline, "source_binding": binding,
            "source_publication_control": latest_payload.get("publication_control"),
            "replay_precision_status": "report_baseline_verified",
            "replay_precision_status_label": "报告基线已核验",
            "scope": "core_decisions_and_report_selection_from_frozen_inputs",
            "core_decision_source": "screening_input_snapshot.inputs.decision_map",
            "later_database_reads": False, "current_upstream_queries": False,
            "wall_clock_used_for_replayed_gates": False,
            "upstream_collection_and_core_risk_recalculation_verified": False,
            "core_selected_total": int(len(core_selected_codes)),
            "matched_project_universe_count": int(len(core_selected_codes & universe_codes)),
            "holding_gate_blocked_count": int(len((core_selected_codes & universe_codes) - baseline_pool_codes)),
            "not_in_project_universe_count": int(len(core_selected_codes - universe_codes)),
            "base_gate_pass_count": int(len(core_selected_codes & base_gate_pass_codes)),
            "dynamic_pass_count": int(len(core_selected_codes & dynamic_pass_codes)),
            "publish_formal_count": int(len(core_selected_codes & publish_formal_codes)),
            "final_official_count": int(len(core_selected_codes & final_official_codes)),
            "stage_rows": stage_rows,
            "status_counts": [
                {"status": str(index), "count": int(value)}
                for index, value in compare_df["核心到发布状态"].value_counts().items()
            ],
            "category_status_summary": category_summary.to_dict(orient="records"),
            "base_gate_top_reasons": _count_by_reason(core_base_gate_blocked_df.get("基础推荐理由", pd.Series(dtype=object))),
            "dynamic_top_reasons": _count_by_reason(core_dynamic_blocked_df.get("dynamic_threshold_reason", pd.Series(dtype=object))),
            "publish_post_filter_top_reasons": _count_by_reason(core_publish_blocked_df.get("推荐理由", pd.Series(dtype=object))),
            "core_final_selection_removed_count": int(len(core_final_selection_removed_df.index)),
            "core_final_selection_removed_sample_codes": core_final_selection_removed_df["基金代码"].astype(str).head(50).tolist(),
            "final_selection_stage_summary": final_selection_stage_summary,
            "final_selection_top_reasons": final_selection_stage_summary.get("top_reasons") or [],
            "final_selection_is_stability_cap_dominant": final_selection_stage_summary.get("stability_cap_dominant"),
            "sample_not_in_project_universe_codes": sorted(core_selected_codes - universe_codes)[:50],
            "sample_holding_gate_blocked_codes": core_in_universe_df[
                ~core_in_universe_df["通过持有期硬门槛"]
            ]["基金代码"].astype(str).head(50).tolist(),
            "sample_dynamic_blocked_codes": core_dynamic_blocked_df["基金代码"].astype(str).head(50).tolist(),
            "sample_final_official_codes": core_final_official_df["基金代码"].astype(str).head(50).tolist(),
        }

    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id, "available": False, "passed": False,
            "status": "frozen_inputs_unverified", "status_label": "冻结输入或基线未通过核验",
            "detail": str(exc), "error": type(exc).__name__,
            "baseline_verification": baseline, "source_binding": binding,
            "source_publication_control": latest_payload.get("publication_control"),
            "replay_precision_status": baseline.get("status", "not_verified"),
            "replay_precision_status_label": "未核验",
            "core_selected_total": None, "matched_project_universe_count": None,
            "holding_gate_blocked_count": None, "not_in_project_universe_count": None,
            "base_gate_pass_count": None, "dynamic_pass_count": None, "publish_formal_count": None,
            "final_official_count": None, "stage_rows": None, "artifacts": {},
            "upstream_collection_and_core_risk_recalculation_verified": False,
        }
        with summary_path.open("x", encoding="utf-8") as stream:
            json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        _write_markdown(run_id=run_id, summary=summary, output_path=markdown_path)
        print(json.dumps({"artifact": str(summary_path), "passed": False, "status": summary["status"]}, ensure_ascii=False))
        return 2
    summary["artifacts"] = {name: {"path": str(path.relative_to(project_root)),
                                      **write_compressed_audit_csv(path, frame)}
                            for name, path, frame in (("compare", compare_path, compare_df),
                                                      ("category_status", category_path, category_summary))}
    with summary_path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
    _write_markdown(run_id=run_id, summary=summary, output_path=markdown_path)

    print(json.dumps({"artifact": str(summary_path), "passed": True, "status": summary["status"],
                      "core_selected_total": summary["core_selected_total"], "final_official_count": summary["final_official_count"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
