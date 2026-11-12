from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from backend.project_publish_decisions import (
    AUTO_DIAGNOSIS_CATEGORY_COLUMN, AUTO_DIAGNOSIS_VERDICT,
    DATA_VALIDATION_COLUMNS,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
)

from backend import report_builder as _report_builder
from backend.forward_validation import build_incremental_review_frame
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL,
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL,
)


def normalize_code(value: Any) -> str:
    return _report_builder._normalize_code(value)


def load_decision_record_map(path_value: str | None) -> dict[str, dict[str, Any]]:
    return _report_builder._load_decision_record_map(path_value)


def load_soft_warning_map(path_value: str | None) -> dict[str, dict[str, Any]]:
    return _report_builder._load_soft_warning_map(path_value)


def append_formal_group_column(df: pd.DataFrame) -> pd.DataFrame:
    return _report_builder._append_formal_group_column(df)


def append_project_decision_columns(
    df: pd.DataFrame,
    *,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    return _report_builder._append_project_decision_columns(
        df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        **({"evaluated_at": evaluated_at} if evaluated_at is not None else {}),
    )


def apply_fixed_quality_thresholds(df: pd.DataFrame) -> pd.DataFrame:
    return _report_builder._apply_fixed_quality_thresholds(df)


def compute_dynamic_style_thresholds(
    df: pd.DataFrame,
) -> tuple[dict[str, dict[str, float]], dict[str, float], dict[str, dict[str, Any]]]:
    return _report_builder._compute_dynamic_style_thresholds(df)


def apply_dynamic_quality_thresholds(
    df: pd.DataFrame,
    *,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    style_meta: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    return _report_builder._apply_dynamic_quality_thresholds(
        df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )


def apply_enhanced_supplemental_gate(df: pd.DataFrame) -> pd.DataFrame:
    return _report_builder._apply_enhanced_supplemental_gate(df)


def apply_publish_post_filters(
    df: pd.DataFrame,
    *,
    threshold_mode: str,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    return _report_builder._apply_publish_post_filters(
        df,
        threshold_mode=threshold_mode,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        **({"evaluated_at": evaluated_at} if evaluated_at is not None else {}),
    )


def build_final_selection_tracker_features(
    settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    return _report_builder._build_final_selection_tracker_features(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_nav_snapshot_date=current_nav_snapshot_date,
        current_df=current_df,
    )


def apply_final_official_selection(
    df: pd.DataFrame,
    *,
    settings,
    tracker_features_df: pd.DataFrame,
) -> pd.DataFrame:
    return _report_builder._apply_final_official_selection(
        df,
        settings=settings,
        tracker_features_df=tracker_features_df,
    )


def demote_autostock_hard_block_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _report_builder._demote_autostock_hard_block_rows(df)


def build_incremental_safety_watch_seed_df(
    current_official_df: pd.DataFrame,
    incremental_review_df: pd.DataFrame,
) -> pd.DataFrame:
    return _report_builder._build_incremental_safety_watch_seed_df(
        current_official_df,
        incremental_review_df,
    )


def is_incremental_safety_watch_candidate(row: pd.Series | dict[str, Any]) -> bool:
    return _report_builder._is_incremental_safety_watch_candidate(row)


def demote_incremental_safety_watch_codes(
    df: pd.DataFrame,
    *,
    target_codes: set[str],
) -> pd.DataFrame:
    return _report_builder._demote_incremental_safety_watch_codes(df, target_codes=target_codes)


def build_priority_pool_tracker_features(
    settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    return _report_builder._build_priority_pool_tracker_features(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_nav_snapshot_date=current_nav_snapshot_date,
        current_df=current_df,
    )


def build_portfolio_suggestions(
    df: pd.DataFrame,
    *,
    tracker_features_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    return _report_builder._build_portfolio_suggestions(
        df,
        tracker_features_df=tracker_features_df,
    )


def materialize_track_frame(
    df: pd.DataFrame,
    *,
    verdict_column: str,
    watch_bucket_column: str,
    reason_column: str,
) -> pd.DataFrame:
    return _report_builder._materialize_track_frame(
        df,
        verdict_column=verdict_column,
        watch_bucket_column=watch_bucket_column,
        reason_column=reason_column,
    )


def overlay_autostock_statuses(
    target_df: pd.DataFrame,
    source_df: pd.DataFrame,
) -> pd.DataFrame:
    return _report_builder._overlay_autostock_statuses(target_df, source_df)


def refresh_recommendation_notes(df: pd.DataFrame) -> pd.DataFrame:
    return _report_builder._refresh_recommendation_notes(df)


def supplement_official_autostock_statuses(
    df: pd.DataFrame,
    *,
    settings,
    status_context: dict[str, Any] | None = None,
) -> pd.DataFrame:
    return _report_builder._supplement_official_autostock_statuses(
        df,
        settings=settings,
        status_context=status_context,
    )


def compute_low_vol_monthly_shadow_metrics(daily_returns: pd.Series) -> dict[str, Any]:
    return _report_builder._compute_low_vol_monthly_shadow_metrics(daily_returns)


def apply_exported_autostock_status_map(
    df: pd.DataFrame,
    *,
    autostock_status_map: dict[str, dict[str, str]],
) -> pd.DataFrame:
    current = df.copy()
    if current.empty or not autostock_status_map or "基金代码" not in current.columns:
        return current

    current["_merge_fcode"] = current["基金代码"].map(normalize_code)
    legacy_columns = ("AutoStock原始校验状态", "AutoStock告警原因", "AutoStock校验状态", "AutoStock校验备注")
    neutral_codes = {code for code, record in autostock_status_map.items()
                     if any(column in record for column in DATA_VALIDATION_COLUMNS)}
    has_neutral_schema = bool(neutral_codes) or any(column in current for column in DATA_VALIDATION_COLUMNS)
    columns = (*legacy_columns, *DATA_VALIDATION_COLUMNS) if has_neutral_schema else legacy_columns
    for column in columns:
        if column not in current and not any(column in record for record in autostock_status_map.values()):
            continue
        values = []
        for code in current["_merge_fcode"]:
            record = autostock_status_map.get(code, {})
            if code in neutral_codes:
                # Do not relabel this provider's result as an AutoStock result
                # or inherit a prior pass where this observation is missing.
                values.append(record.get(column, "") if column in DATA_VALIDATION_COLUMNS else "")
            elif column in legacy_columns:
                values.append(record.get(column, ""))
            else:
                values.append("")
        current[column] = values
    return current.drop(columns=["_merge_fcode"], errors="ignore")


def apply_incremental_safety_watch_alignment(
    *,
    settings,
    publish_track_df: pd.DataFrame,
    official_df: pd.DataFrame,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    current_publish_track_df = publish_track_df.copy()
    current_official_df = official_df.copy()
    if (
        current_official_df.empty
        or settings is None
        or not str(run_id or "").strip()
        or not str(generated_at or "").strip()
        or not str(snapshot_date or "").strip()
    ):
        return current_publish_track_df, current_official_df, set()

    current_official_df = append_formal_group_column(current_official_df)
    incremental_review_df = build_incremental_review_frame(
        settings,
        run_id=str(run_id or "").strip(),
        generated_at=str(generated_at or "").strip(),
        snapshot_date=str(snapshot_date or "").strip(),
        filtered_excel=settings.runs_dir / str(run_id or "").strip() / "__audit_shadow_filtered__.xlsx",
        watch_excel=None,
        final_count=int(len(current_official_df.index)),
        stable_final_count=int(
            current_official_df.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object))
            .astype(str)
            .eq(STABLE_BOND_GROUP_LABEL)
            .sum()
        ),
        enhanced_final_count=int(
            current_official_df.get(FORMAL_GROUP_LABEL, pd.Series(dtype=object))
            .astype(str)
            .eq(ENHANCED_BOND_GROUP_LABEL)
            .sum()
        ),
        current_formal_df=current_official_df.copy(),
    )
    incremental_review_seed_df = build_incremental_safety_watch_seed_df(
        current_official_df,
        incremental_review_df,
    )
    incremental_safety_watch_codes = {
        normalize_code(row.get("基金代码"))
        for row in incremental_review_seed_df.to_dict(orient="records")
        if is_incremental_safety_watch_candidate(row)
    }
    if not incremental_safety_watch_codes:
        return current_publish_track_df, current_official_df, set()

    current_publish_track_df = demote_incremental_safety_watch_codes(
        current_publish_track_df,
        target_codes=incremental_safety_watch_codes,
    )
    current_official_df = current_official_df[
        ~current_official_df.get("基金代码", pd.Series(dtype=object))
        .map(normalize_code)
        .isin(incremental_safety_watch_codes)
    ].copy()
    return current_publish_track_df, current_official_df, incremental_safety_watch_codes


def run_publish_pipeline(
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
    candidate_df = append_project_decision_columns(
        universe_df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
    )
    candidate_df = apply_fixed_quality_thresholds(candidate_df)
    dynamic_base_df = candidate_df[candidate_df["基础门禁通过"]].copy()
    style_thresholds, global_thresholds, style_meta = compute_dynamic_style_thresholds(dynamic_base_df)
    candidate_df = apply_dynamic_quality_thresholds(
        candidate_df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )
    candidate_df = apply_enhanced_supplemental_gate(candidate_df)
    publish_track_df = apply_publish_post_filters(
        candidate_df,
        threshold_mode="dynamic",
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
    )
    publish_official_df = publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    final_selection_tracker_features_df = build_final_selection_tracker_features(
        settings,
        current_run_id=str(run_id or "").strip(),
        current_generated_at=str(generated_at or "").strip(),
        current_nav_snapshot_date=str(snapshot_date or "").strip(),
        current_df=publish_official_df,
    )
    official_df = apply_final_official_selection(
        publish_official_df,
        settings=settings,
        tracker_features_df=final_selection_tracker_features_df,
    )
    official_df = apply_exported_autostock_status_map(
        official_df,
        autostock_status_map=autostock_status_map or {},
    )
    official_df, official_autostock_blocked_df = demote_autostock_hard_block_rows(official_df)
    official_df = append_formal_group_column(official_df)
    publish_track_df, official_df, _ = apply_incremental_safety_watch_alignment(
        settings=settings,
        publish_track_df=publish_track_df,
        official_df=official_df,
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
    )
    publish_official_df = publish_track_df[publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    official_df = append_formal_group_column(official_df)
    official_codes = {
        normalize_code(code)
        for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if normalize_code(code)
    }
    removed_df = publish_official_df[
        ~publish_official_df.get("基金代码", pd.Series(dtype=object))
        .astype(str)
        .map(normalize_code)
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
