from __future__ import annotations

from typing import Any


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def build_portfolio_display_scope(
    counts: dict[str, Any] | None,
    portfolio_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    count_payload = counts or {}
    portfolio = portfolio_summary or {}
    display_pool_count = int(portfolio.get("fund_count") or 0)
    official_publish_count = int(count_payload.get("final_count") or 0)
    group_count = int(portfolio.get("group_count") or 0)
    compressed_from_official_publish = (
        official_publish_count > 0 and display_pool_count < official_publish_count
    )
    display_pool_share_pct = (
        round(display_pool_count / official_publish_count * 100, 2)
        if official_publish_count > 0
        else None
    )

    return {
        "available": True,
        "display_pool_count": display_pool_count,
        "official_publish_count": official_publish_count,
        "group_count": group_count,
        "compressed_from_official_publish": compressed_from_official_publish,
        "display_pool_share_pct": display_pool_share_pct,
        "display_entry_endpoint": "/api/recommendations/latest.portfolio_suggestions",
        "full_official_endpoint": "/api/dashboard/latest.final_preview",
        "download_artifact_hint": "正式推荐债券基金.xlsx",
        "frontend_policy": "保留当前前端入口，仅展示最终优质基金，不展示淘汰基金。",
        "headline": (
            f"当前推荐页展示 {display_pool_count} 只基金，来自正式主榜 {official_publish_count} 只的压缩组合。"
        ),
        "explanation": (
            "推荐页只保留最终优质基金的压缩展示池，不展示淘汰基金；"
            "全量正式主榜继续保留在 dashboard 最新快照与正式推荐导出文件中。"
        ),
    }


def build_publish_layer_semantics_summary(
    counts: dict[str, Any] | None,
    publish_funnel_summary: dict[str, Any] | None,
    portfolio_display_scope: dict[str, Any] | None,
    portfolio_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    count_payload = counts or {}
    funnel = publish_funnel_summary or {}
    display_scope = portfolio_display_scope or {}
    portfolio = portfolio_summary or {}

    official_publish_count = _to_int(
        funnel.get("official_publish_count") or count_payload.get("final_count")
    )
    dynamic_reference_count = _to_int(
        funnel.get("dynamic_reference_export_count")
        or funnel.get("dynamic_reference_count")
        or count_payload.get("dynamic_final_count")
    )
    automated_watch_count = _to_int(count_payload.get("internal_watch_count"))
    display_pool_count = _to_int(
        display_scope.get("display_pool_count") or portfolio.get("fund_count")
    )
    screenable_universe_count = _to_int(
        funnel.get("screenable_universe_count") or count_payload.get("screenable_universe_count")
    )
    governance_candidate_pool_count = _to_int(
        funnel.get("governance_candidate_pool_count")
        or count_payload.get("stage_b_candidate_pool_count")
        or count_payload.get("publishable_pool_count")
    )
    final_selection_removed_count = _to_int(funnel.get("final_selection_removed_count"))
    dynamic_baseline_after_hard_block_count = _to_int(
        funnel.get("dynamic_baseline_after_hard_block_count")
    )
    display_pool_share_of_official = (
        round(display_pool_count / official_publish_count, 6)
        if official_publish_count > 0
        else 0.0
    )

    return {
        "available": True,
        "screenable_universe_count": screenable_universe_count,
        "governance_candidate_pool_count": governance_candidate_pool_count,
        "official_publish_count": official_publish_count,
        "dynamic_reference_count": dynamic_reference_count,
        "automated_watch_count": automated_watch_count,
        "display_pool_count": display_pool_count,
        "dynamic_baseline_after_hard_block_count": dynamic_baseline_after_hard_block_count,
        "final_selection_removed_count": final_selection_removed_count,
        "display_pool_share_of_official": display_pool_share_of_official,
        "layer_definitions": [
            {
                "layer_key": "official_publish",
                "label": "正式主榜",
                "count": official_publish_count,
                "source_field": "counts.final_count",
                "semantic_role": "当前正式发布全集，代表完整正式推荐结果，不等于前端压缩展示池。",
            },
            {
                "layer_key": "dynamic_reference",
                "label": "动态参考榜",
                "count": dynamic_reference_count,
                "source_field": "counts.dynamic_final_count",
                "semantic_role": "动态阈值参考导出，用于对照审计，不替代正式主榜。",
            },
            {
                "layer_key": "automated_watch",
                "label": "自动诊断池",
                "count": automated_watch_count,
                "source_field": "counts.internal_watch_count",
                "semantic_role": "高分但未进入正式主榜的研究/诊断池，不面向终端投资展示。",
            },
            {
                "layer_key": "display_pool",
                "label": "前端展示池",
                "count": display_pool_count,
                "source_field": "portfolio_suggestions.fund_count",
                "semantic_role": "推荐页从正式主榜压缩出的组合展示池，用于有限数量的前端展示，不代表筛选只产出这些基金。",
            },
        ],
        "consistency_flags": {
            "display_pool_is_subset_of_official": bool(
                official_publish_count > 0 and 0 <= display_pool_count <= official_publish_count
            ),
            "dynamic_reference_not_smaller_than_official": bool(
                dynamic_reference_count >= official_publish_count
            ),
            "watch_pool_is_not_official_publish": bool(
                automated_watch_count >= 0 and automated_watch_count != official_publish_count
            ),
        },
        "headline": (
            f"正式主榜 {official_publish_count} 只，动态参考榜 {dynamic_reference_count} 只，"
            f"自动诊断池 {automated_watch_count} 只，前端展示池 {display_pool_count} 只。"
        ),
        "explanation": (
            "正式主榜是完整正式推荐结果；动态参考榜是动态阈值对照导出；"
            "自动诊断池承接高分但未正式发布的样本；"
            f"前端展示池仅是正式主榜压缩展示，当前占正式主榜 {display_pool_share_of_official * 100:.2f}%。"
        ),
    }
