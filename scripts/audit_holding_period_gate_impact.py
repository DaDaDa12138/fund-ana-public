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

from backend.project_publish_decisions import (
    AUTO_DIAGNOSIS_CATEGORY_COLUMN, AUTO_DIAGNOSIS_VERDICT,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
)
from backend.audit_support import (
    resolve_latest_run_payload as resolve_latest_run_payload_impl,
    write_compressed_audit_csv,
)
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL,
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL,
)
from backend.publish_pipeline import (
    append_formal_group_column as append_formal_group_column_impl,
    append_project_decision_columns as append_project_decision_columns_impl,
    apply_dynamic_quality_thresholds as apply_dynamic_quality_thresholds_impl,
    apply_exported_autostock_status_map as apply_exported_autostock_status_map_impl,
    apply_final_official_selection as apply_final_official_selection_impl,
    apply_fixed_quality_thresholds as apply_fixed_quality_thresholds_impl,
    apply_incremental_safety_watch_alignment as apply_incremental_safety_watch_alignment_impl,
    apply_publish_post_filters as apply_publish_post_filters_impl,
    apply_enhanced_supplemental_gate as apply_enhanced_supplemental_gate_impl,
    build_final_selection_tracker_features as build_final_selection_tracker_features_impl,
    compute_dynamic_style_thresholds as compute_dynamic_style_thresholds_impl,
    demote_autostock_hard_block_rows as demote_autostock_hard_block_rows_impl,
)
from backend.report_inputs import (
    build_candidate_universe as build_candidate_universe_impl,
    load_actual_autostock_status_map as load_actual_autostock_status_map_impl,
    load_actual_official_df as load_actual_official_df_impl,
    normalize_code as _normalize_code,
    resolve_runtime_artifact_path as _resolve_runtime_artifact_path,
)
from backend.publish_pipeline import (
    load_decision_record_map as _load_decision_record_map,
    load_soft_warning_map as _load_soft_warning_map,
)
from scripts.audit_runtime_utils import resolve_run_payload as _resolve_run_payload


def _resolve_latest_run_payload(settings) -> tuple[str, dict[str, Any]]:
    return resolve_latest_run_payload_impl(settings)


def _build_candidate_universe(settings, artifacts: dict[str, Any]) -> pd.DataFrame:
    return build_candidate_universe_impl(settings, artifacts)


def _load_actual_autostock_status_map(
    artifacts: dict[str, Any],
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, dict[str, str]]:
    return load_actual_autostock_status_map_impl(
        artifacts,
        project_root=project_root,
    )


def _load_actual_official_df(
    artifacts: dict[str, Any],
    *,
    project_root: Path = PROJECT_ROOT,
) -> pd.DataFrame:
    return load_actual_official_df_impl(
        artifacts,
        project_root=project_root,
    )


def _apply_exported_autostock_status_map(
    df: pd.DataFrame,
    *,
    autostock_status_map: dict[str, dict[str, str]],
) -> pd.DataFrame:
    return apply_exported_autostock_status_map_impl(
        df,
        autostock_status_map=autostock_status_map,
    )


def _apply_incremental_safety_watch_alignment(
    *,
    settings,
    publish_track_df: pd.DataFrame,
    official_df: pd.DataFrame,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    return apply_incremental_safety_watch_alignment_impl(
        settings=settings,
        publish_track_df=publish_track_df,
        official_df=official_df,
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
    )


def _append_project_decision_columns(
    df: pd.DataFrame,
    *,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    return append_project_decision_columns_impl(
        df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
    )


def _apply_fixed_quality_thresholds(df: pd.DataFrame) -> pd.DataFrame:
    return apply_fixed_quality_thresholds_impl(df)


def _compute_dynamic_style_thresholds(
    df: pd.DataFrame,
) -> tuple[dict[str, dict[str, float]], dict[str, float], dict[str, dict[str, Any]]]:
    return compute_dynamic_style_thresholds_impl(df)


def _apply_dynamic_quality_thresholds(
    df: pd.DataFrame,
    *,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    style_meta: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    return apply_dynamic_quality_thresholds_impl(
        df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )


def _apply_enhanced_supplemental_gate(df: pd.DataFrame) -> pd.DataFrame:
    return apply_enhanced_supplemental_gate_impl(df)


def _apply_publish_post_filters(
    df: pd.DataFrame,
    *,
    threshold_mode: str,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
) -> pd.DataFrame:
    return apply_publish_post_filters_impl(
        df,
        threshold_mode=threshold_mode,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
    )


def _build_final_selection_tracker_features(
    settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    return build_final_selection_tracker_features_impl(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_nav_snapshot_date=current_nav_snapshot_date,
        current_df=current_df,
    )


def _apply_final_official_selection(
    df: pd.DataFrame,
    *,
    settings=None,
    tracker_features_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    return apply_final_official_selection_impl(
        df,
        settings=settings,
        tracker_features_df=tracker_features_df,
    )


def _call_apply_final_official_selection(
    df: pd.DataFrame,
    *,
    settings=None,
    tracker_features_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    try:
        return _apply_final_official_selection(
            df,
            settings=settings,
            tracker_features_df=tracker_features_df,
        )
    except TypeError:
        return _apply_final_official_selection(df)


def _demote_autostock_hard_block_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return demote_autostock_hard_block_rows_impl(df)


def _append_formal_group_column(df: pd.DataFrame) -> pd.DataFrame:
    return append_formal_group_column_impl(df)


def _run_publish_pipeline(
    *,
    universe_df: pd.DataFrame,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    autostock_status_map: dict[str, dict[str, str]] | None = None,
    settings=None,
    run_id: str = "",
    generated_at: str = "",
    snapshot_date: str = "",
) -> dict[str, pd.DataFrame | dict[str, dict[str, float]]]:
    candidate_df = _append_project_decision_columns(
        universe_df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
    )
    candidate_df = _apply_fixed_quality_thresholds(candidate_df)
    dynamic_base_df = candidate_df[candidate_df["基础门禁通过"]].copy()
    style_thresholds, global_thresholds, style_meta = _compute_dynamic_style_thresholds(dynamic_base_df)
    candidate_df = _apply_dynamic_quality_thresholds(
        candidate_df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )
    candidate_df = _apply_enhanced_supplemental_gate(candidate_df)
    publish_track_df = _apply_publish_post_filters(
        candidate_df,
        threshold_mode="dynamic",
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
    )
    publish_official_df = publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    final_selection_tracker_features_df = _build_final_selection_tracker_features(
        settings,
        current_run_id=str(run_id or "").strip(),
        current_generated_at=str(generated_at or "").strip(),
        current_nav_snapshot_date=str(snapshot_date or "").strip(),
        current_df=publish_official_df,
    )
    official_df = _call_apply_final_official_selection(
        publish_official_df,
        settings=settings,
        tracker_features_df=final_selection_tracker_features_df,
    )
    official_df = _apply_exported_autostock_status_map(
        official_df,
        autostock_status_map=autostock_status_map or {},
    )
    official_df, official_autostock_blocked_df = _demote_autostock_hard_block_rows(official_df)
    official_df = _append_formal_group_column(official_df)
    publish_track_df, official_df, _ = _apply_incremental_safety_watch_alignment(
        settings=settings,
        publish_track_df=publish_track_df,
        official_df=official_df,
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
    )
    publish_official_df = publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    official_df = _append_formal_group_column(official_df)
    official_codes = {
        _normalize_code(code)
        for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    removed_df = publish_official_df[
        ~publish_official_df.get("基金代码", pd.Series(dtype=object))
        .astype(str)
        .map(_normalize_code)
        .isin(official_codes)
    ].copy()
    if not removed_df.empty:
        removed_df["项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
        removed_df[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS
        removed_df["推荐理由"] = removed_df.get("推荐理由", "").astype(str).map(
            lambda text: "；".join(
                [item for item in [str(text).strip(), "通过发布门禁，但未进入最终精选名单"] if item]
            )
        )
    internal_watch_df = pd.concat(
        [
            publish_track_df[publish_track_df["项目推荐结论"] != "正式推荐"].copy(),
            removed_df,
            official_autostock_blocked_df,
        ],
        ignore_index=True,
    )
    return {
        "candidate_df": candidate_df,
        "publish_track_df": publish_track_df,
        "official_df": official_df,
        "internal_watch_df": internal_watch_df,
        "style_thresholds": style_thresholds,
        "global_thresholds": global_thresholds,
    }


def _count_by_group(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or FORMAL_GROUP_LABEL not in df.columns:
        return {
            STABLE_BOND_GROUP_LABEL: 0,
            ENHANCED_BOND_GROUP_LABEL: 0,
        }
    counts = (
        df[FORMAL_GROUP_LABEL]
        .fillna("")
        .astype(str)
        .value_counts()
        .to_dict()
    )
    return {
        STABLE_BOND_GROUP_LABEL: int(counts.get(STABLE_BOND_GROUP_LABEL, 0)),
        ENHANCED_BOND_GROUP_LABEL: int(counts.get(ENHANCED_BOND_GROUP_LABEL, 0)),
    }


def _build_summary(
    *,
    baseline_official_df: pd.DataFrame,
    scenario_official_df: pd.DataFrame,
    blocked_df: pd.DataFrame,
    latest_payload: dict[str, Any],
    threshold_days: int,
    scenario_screenable_count: int,
    blocked_ready_buyable_count: int,
) -> dict[str, Any]:
    baseline_codes = {
        _normalize_code(code)
        for code in baseline_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    scenario_codes = {
        _normalize_code(code)
        for code in scenario_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    added_codes = scenario_codes - baseline_codes
    removed_codes = baseline_codes - scenario_codes

    blocked_buyable = blocked_df[pd.to_numeric(blocked_df.get("is_buyable"), errors="coerce").fillna(0).astype(int) == 1].copy()
    blocked_ready = blocked_df[
        blocked_df["最新净值日期"].notna()
        & blocked_df["1年-年化收益(%)"].notna()
        & blocked_df["1年-夏普比率"].notna()
        & blocked_df["1年-最大回撤(%)"].notna()
    ].copy()

    baseline_group_counts = _count_by_group(baseline_official_df)
    scenario_group_counts = _count_by_group(scenario_official_df)
    baseline_final_count = int(len(baseline_official_df.index))
    scenario_final_count = int(len(scenario_official_df.index))
    baseline_screenable = int((latest_payload.get("counts") or {}).get("screenable_universe_count") or 0)

    top_added_df = scenario_official_df[
        scenario_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code).isin(added_codes)
    ].copy()
    top_added_df = top_added_df.sort_values(
        by=[FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[False, True],
        kind="mergesort",
    )

    top_removed_df = baseline_official_df[
        baseline_official_df.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code).isin(removed_codes)
    ].copy()
    top_removed_df = top_removed_df.sort_values(
        by=[FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[False, True],
        kind="mergesort",
    )

    blocked_current = blocked_df.copy()
    blocked_current["holding_days_num"] = pd.to_numeric(blocked_current.get("holding_days"), errors="coerce").fillna(0)
    blocked_current["holding_period_source_text"] = blocked_current.get("holding_period_source", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    blocked_current["holding_period_source_label"] = blocked_current["holding_period_source_text"].map(lambda value: value if value else "empty")
    blocked_current["holding_period_block_reason"] = blocked_current.apply(
        lambda row: (
            "通用占位持有期"
            if int(row.get("holding_days_num") or 0) == 999
            else "已确认>90天"
            if int(row.get("holding_days_num") or 0) > threshold_days
            else "其他"
        ),
        axis=1,
    )
    blocked_current["formal_score_num"] = pd.to_numeric(blocked_current.get(FORMAL_SCORE_LABEL), errors="coerce")
    high_score_blocked_df = blocked_current[blocked_current["formal_score_num"] >= 80].copy()
    high_score_blocked_preview = (
        high_score_blocked_df.sort_values(
            by=["formal_score_num", "基金代码"],
            ascending=[False, True],
            kind="mergesort",
        )[
            [
                column
                for column in [
                    "基金代码",
                    "基金名称",
                    "债基风格标签",
                    "holding_period_block_reason",
                    "holding_period_source_label",
                    "holding_days",
                    FORMAL_SCORE_LABEL,
                    "1年-年化收益(%)",
                    "1年-夏普比率",
                    "1年-最大回撤(%)",
                    "1年-波动率",
                    "is_buyable",
                ]
                if column in high_score_blocked_df.columns
            ]
        ]
        .fillna("")
        .astype(str)
        .head(20)
        .to_dict(orient="records")
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "threshold_removed": True,
        "holding_period_threshold_days": threshold_days,
        "baseline": {
            "screenable_universe_count": baseline_screenable,
            "final_count": baseline_final_count,
            "stable_bond_final_count": baseline_group_counts[STABLE_BOND_GROUP_LABEL],
            "enhanced_bond_final_count": baseline_group_counts[ENHANCED_BOND_GROUP_LABEL],
        },
        "scenario_without_gate": {
            "screenable_universe_count": int(scenario_screenable_count),
            "final_count": scenario_final_count,
            "stable_bond_final_count": scenario_group_counts[STABLE_BOND_GROUP_LABEL],
            "enhanced_bond_final_count": scenario_group_counts[ENHANCED_BOND_GROUP_LABEL],
        },
        "impact": {
            "screenable_universe_delta": int(scenario_screenable_count) - baseline_screenable,
            "final_count_delta": scenario_final_count - baseline_final_count,
            "added_official_count": int(len(added_codes)),
            "removed_official_count": int(len(removed_codes)),
            "blocked_total": int(len(blocked_df.index)),
            "blocked_buyable_count": int(len(blocked_buyable.index)),
            "blocked_metrics_ready_count": int(len(blocked_ready.index)),
            "blocked_metrics_ready_buyable_count": int(blocked_ready_buyable_count),
            "blocked_with_formal_score_ge_70": int((pd.to_numeric(blocked_df.get(FORMAL_SCORE_LABEL), errors="coerce") >= 70).sum()),
            "blocked_with_formal_score_ge_80": int((pd.to_numeric(blocked_df.get(FORMAL_SCORE_LABEL), errors="coerce") >= 80).sum()),
        },
        "blocked_breakdown": {
            "block_reason_counts": {
                str(index): int(value)
                for index, value in blocked_current["holding_period_block_reason"].value_counts().to_dict().items()
            },
            "holding_period_source_counts": {
                str(index): int(value)
                for index, value in blocked_current["holding_period_source_label"].value_counts().to_dict().items()
            },
            "high_score_ge_80_block_reason_counts": {
                str(index): int(value)
                for index, value in high_score_blocked_df["holding_period_block_reason"].value_counts().to_dict().items()
            },
            "high_score_ge_80_holding_period_source_counts": {
                str(index): int(value)
                for index, value in high_score_blocked_df["holding_period_source_label"].value_counts().to_dict().items()
            },
        },
        "top_added_codes": top_added_df.get("基金代码", pd.Series(dtype=object)).astype(str).head(20).tolist(),
        "top_removed_codes": top_removed_df.get("基金代码", pd.Series(dtype=object)).astype(str).head(20).tolist(),
        "high_score_blocked_preview": high_score_blocked_preview,
    }


def _build_compare_table(
    *,
    baseline_official_df: pd.DataFrame,
    scenario_official_df: pd.DataFrame,
) -> pd.DataFrame:
    baseline = baseline_official_df.copy()
    baseline["基金代码"] = baseline["基金代码"].map(_normalize_code)
    baseline["_baseline_in"] = True
    baseline["_baseline_score"] = pd.to_numeric(baseline.get(FORMAL_SCORE_LABEL), errors="coerce")
    baseline = baseline[["基金代码", "基金名称", FORMAL_GROUP_LABEL, "_baseline_in", "_baseline_score"]].rename(
        columns={
            "基金名称": "baseline_基金名称",
            FORMAL_GROUP_LABEL: "baseline_正式榜分组",
        }
    )

    scenario = scenario_official_df.copy()
    scenario["基金代码"] = scenario["基金代码"].map(_normalize_code)
    scenario["_scenario_in"] = True
    scenario["_scenario_score"] = pd.to_numeric(scenario.get(FORMAL_SCORE_LABEL), errors="coerce")
    holding_days_col = "持有期天数" if "持有期天数" in scenario.columns else "holding_days"
    scenario = scenario[["基金代码", "基金名称", FORMAL_GROUP_LABEL, "_scenario_in", "_scenario_score", "持有期状态", holding_days_col, "债基风格标签", "1年-年化收益(%)", "1年-夏普比率", "1年-最大回撤(%)", "1年-波动率"]].rename(
        columns={
            "基金名称": "scenario_基金名称",
            FORMAL_GROUP_LABEL: "scenario_正式榜分组",
            holding_days_col: "持有期天数",
        }
    )

    compare = baseline.merge(scenario, on="基金代码", how="outer")
    compare["_baseline_in"] = compare["_baseline_in"].where(compare["_baseline_in"].notna(), False)
    compare["_scenario_in"] = compare["_scenario_in"].where(compare["_scenario_in"].notna(), False)
    compare["_baseline_in"] = compare["_baseline_in"].infer_objects(copy=False).astype(bool)
    compare["_scenario_in"] = compare["_scenario_in"].infer_objects(copy=False).astype(bool)

    def _resolve_change(row: pd.Series) -> str:
        if bool(row["_baseline_in"]) and bool(row["_scenario_in"]):
            return "仍在正式榜"
        if (not bool(row["_baseline_in"])) and bool(row["_scenario_in"]):
            return "取消持有期硬门槛后新增入榜"
        if bool(row["_baseline_in"]) and (not bool(row["_scenario_in"])):
            return "被新入榜样本挤出正式榜"
        return "未入榜"

    compare["变化类型"] = compare.apply(_resolve_change, axis=1)
    compare["正式投资评分变化"] = (
        pd.to_numeric(compare["_scenario_score"], errors="coerce")
        - pd.to_numeric(compare["_baseline_score"], errors="coerce")
    ).round(4)
    compare = compare.sort_values(
        by=["变化类型", "_scenario_score", "_baseline_score", "基金代码"],
        ascending=[True, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return compare


def _write_markdown(
    *,
    summary: dict[str, Any],
    compare_df: pd.DataFrame,
    output_path: Path,
) -> None:
    impact = summary["impact"]
    baseline = summary["baseline"]
    scenario = summary["scenario_without_gate"]
    blocked_breakdown = summary.get("blocked_breakdown") or {}
    added_df = compare_df[compare_df["变化类型"] == "取消持有期硬门槛后新增入榜"].head(20).copy()
    removed_df = compare_df[compare_df["变化类型"] == "被新入榜样本挤出正式榜"].head(20).copy()
    high_score_blocked_preview_df = pd.DataFrame(summary.get("high_score_blocked_preview") or [])

    def _table(df: pd.DataFrame, columns: list[str]) -> str:
        if df.empty:
            return "_无数据_"
        view = df[[col for col in columns if col in df.columns]].fillna("").astype(str)
        header = "| " + " | ".join(view.columns.tolist()) + " |"
        sep = "| " + " | ".join(["---"] * len(view.columns.tolist())) + " |"
        rows = ["| " + " | ".join(row) + " |" for row in view.values.tolist()]
        return "\n".join([header, sep, *rows])

    lines = [
        "# 持有期硬门槛影响影子审计",
        "",
        f"- 生成时间：`{summary['generated_at']}`",
        f"- 当前门槛：`holding_days > {summary['holding_period_threshold_days']}` 硬排除",
        "",
        "## 核心结论",
        "",
        f"- 基线正式榜数量：`{baseline['final_count']}`",
        f"- 去除硬门槛后的正式榜数量：`{scenario['final_count']}`",
        f"- 正式榜净变化：`{impact['final_count_delta']}`",
        f"- 当前可筛选宇宙：`{baseline['screenable_universe_count']}`",
        f"- 去除硬门槛后的可筛选宇宙：`{scenario['screenable_universe_count']}`",
        f"- 可筛选宇宙增量：`{impact['screenable_universe_delta']}`",
        f"- 当前被 `>90天` 挡掉样本：`{impact['blocked_total']}`",
        f"- 其中可申购样本：`{impact['blocked_buyable_count']}`",
        f"- 其中指标已齐备样本：`{impact['blocked_metrics_ready_count']}`",
        f"- 其中正式投资评分 `>=70`：`{impact['blocked_with_formal_score_ge_70']}`",
        f"- 其中正式投资评分 `>=80`：`{impact['blocked_with_formal_score_ge_80']}`",
        f"- 被挡样本中 `已确认>90天`：`{(blocked_breakdown.get('block_reason_counts') or {}).get('已确认>90天', 0)}`",
        f"- 被挡样本中 `通用占位持有期`：`{(blocked_breakdown.get('block_reason_counts') or {}).get('通用占位持有期', 0)}`",
        "",
        "## 榜单变化",
        "",
        f"- 新增入榜数量：`{impact['added_official_count']}`",
        f"- 被挤出正式榜数量：`{impact['removed_official_count']}`",
        "",
        "### 新增入榜样本",
        "",
        _table(
            added_df,
            ["基金代码", "scenario_基金名称", "scenario_正式榜分组", "债基风格标签", "持有期天数", "_scenario_score", "1年-年化收益(%)", "1年-夏普比率", "1年-最大回撤(%)", "1年-波动率"],
        ),
        "",
        "### 被挤出样本",
        "",
        _table(
            removed_df,
            ["基金代码", "baseline_基金名称", "baseline_正式榜分组", "_baseline_score"],
        ),
        "",
        "## 高分被挡样本预览",
        "",
        _table(
            high_score_blocked_preview_df,
            [
                "基金代码",
                "基金名称",
                "债基风格标签",
                "holding_period_block_reason",
                "holding_period_source_label",
                "holding_days",
                FORMAL_SCORE_LABEL,
                "1年-年化收益(%)",
                "1年-夏普比率",
                "1年-最大回撤(%)",
                "1年-波动率",
                "is_buyable",
            ],
        ),
    ]
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def main() -> int:
    from backend.holding_period_gate_shadow import (
        build_holding_period_gate_shadow, render_holding_period_gate_shadow,
    )
    from scripts.audit_runtime_utils import build_audit_artifact_path
    import hashlib

    parser = argparse.ArgumentParser(description="先复现冻结报告基线，再审计持有期门槛；缺历史证据时停止相应阶段。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定正式 run；默认使用当前报告。")
    source.add_argument("--report", type=Path, help="显式报告 JSON，可用于标明身份的诊断候选。")
    parser.add_argument("--baseline-only", action="store_true", help="只核验原门槛基线，不运行反事实情景。")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            report = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(report, dict) or not report.get("run_id"):
                raise ValueError("report_identity_missing")
        else:
            _, report, _, _ = _resolve_run_payload(
                settings, run_id=args.run_id, project_root=settings.project_root,
            )
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    summary, exports = build_holding_period_gate_shadow(settings, report, baseline_only=args.baseline_only)
    if args.report:
        summary["requested_report"] = str(args.report)
        summary["requested_report_sha256"] = hashlib.sha256(args.report.read_bytes()).hexdigest()
    audit_dir = settings.reports_dir / "audit/holding_period_gate_impact"
    summary_path = build_audit_artifact_path(audit_dir, "holding_period_gate_impact", run_id=report["run_id"])
    artifacts = {}
    for name, frame in exports.items():
        path = summary_path.with_name(f"{summary_path.stem}_{name}.csv.gz")
        artifacts[name] = {"path": str(path), **write_compressed_audit_csv(path, frame)}
    summary["artifacts"] = artifacts
    with summary_path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    summary_path.with_suffix(".md").write_text(render_holding_period_gate_shadow(summary), encoding="utf-8")
    print(json.dumps({"artifact": str(summary_path), "status": summary["status"], "passed": summary["passed"],
                      "baseline_status": summary["baseline"]["status"],
                      "scenario_status": summary["scenario_without_gate"]["status"],
                      "scenario_final_count": summary["scenario_without_gate"]["final_count"]}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
