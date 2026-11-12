from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import time
from typing import Any

from backend import long_term_governance
from backend.long_term_governance import (
    build_long_term_governance_summary,
    build_long_term_maintenance_detail,
)
from backend.publication_status import build_publication_status_summary, build_publication_summary
from backend.scheduler_diagnostics import build_scheduler_path_governance_summary
from backend.scheduler_maintenance_metrics import optional_count
from backend.api_payloads.portfolio_scope import (
    build_portfolio_display_scope,
    build_publish_layer_semantics_summary,
)
from backend.api_payloads.positive_ratio import (
    build_positive_ratio_semantics_summary,
    normalize_low_vol_positive_ratio_shadow_watch,
)
from backend.publish_reason_summary import enrich_publish_reason_summary
from backend.risk_residual_warnings import build_risk_residual_warning_summary
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics
from utils.nav_history_consistency_audit import load_latest_nav_history_consistency_audit_payload

DEFAULT_DASHBOARD_SAMPLE_LIMIT = 0
MAX_DASHBOARD_SAMPLE_LIMIT = 50
DASHBOARD_BUILD_PROFILE_SCHEMA_VERSION = 1


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


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _start_dashboard_build_profile(
    detail_scope: str,
    *,
    compact: bool = False,
) -> tuple[float, dict[str, Any]]:
    started_at = time.perf_counter()
    scope = str(detail_scope or "").strip() or ("compact" if compact else "diagnostic_summary")
    return started_at, {
        "schema_version": DASHBOARD_BUILD_PROFILE_SCHEMA_VERSION,
        "detail_scope": scope,
        "compact": bool(compact),
        "timing_unit": "seconds",
        "steps": [],
    }


def _record_dashboard_build_step(
    profile: dict[str, Any] | None,
    name: str,
    started_at: float,
    *,
    skipped: bool = False,
) -> None:
    if profile is None:
        return
    elapsed = 0.0 if skipped else max(0.0, time.perf_counter() - started_at)
    step = {
        "name": str(name or "").strip() or "unknown",
        "seconds": round(elapsed, 6),
    }
    if skipped:
        step["skipped"] = True
    profile.setdefault("steps", []).append(step)


def _record_dashboard_build_skip(profile: dict[str, Any] | None, name: str) -> None:
    _record_dashboard_build_step(profile, name, time.perf_counter(), skipped=True)


def _finish_dashboard_build_profile(
    profile: dict[str, Any],
    started_at: float,
) -> dict[str, Any]:
    steps = list(profile.get("steps") or [])
    profile["total_seconds"] = round(max(0.0, time.perf_counter() - started_at), 6)
    profile["step_count"] = len(steps)
    profile["slowest_steps"] = [
        {
            "name": step.get("name"),
            "seconds": step.get("seconds"),
        }
        for step in sorted(
            (step for step in steps if not step.get("skipped")),
            key=lambda item: float(item.get("seconds") or 0.0),
            reverse=True,
        )[:5]
    ]
    return profile


def normalize_dashboard_sample_limit(
    sample_limit: Any,
    *,
    default: int = DEFAULT_DASHBOARD_SAMPLE_LIMIT,
    max_limit: int = MAX_DASHBOARD_SAMPLE_LIMIT,
) -> int:
    try:
        parsed = int(sample_limit)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(0, min(int(max_limit), parsed))


def reduce_dashboard_sample_payload(
    payload: dict[str, Any] | None,
    *,
    sample_limit: Any = DEFAULT_DASHBOARD_SAMPLE_LIMIT,
    include_full_samples: bool = False,
) -> dict[str, Any]:
    normalized_limit = normalize_dashboard_sample_limit(sample_limit)
    if include_full_samples:
        full_payload = deepcopy(payload or {})
        full_payload["dashboard_sample_policy"] = {
            "mode": "full",
            "include_full_samples": True,
            "sample_limit": None,
            "max_sample_limit": MAX_DASHBOARD_SAMPLE_LIMIT,
        }
        return full_payload

    def _is_sample_container_key(key: Any) -> bool:
        normalized = str(key or "").strip().lower()
        return bool(
            "sample" in normalized
            or normalized == "preview"
            or normalized.endswith("_preview")
            or normalized in {"top_high_score_excluded"}
        )

    def _reduce(value: Any, *, in_sample_container: bool = False) -> Any:
        if isinstance(value, dict):
            compact: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key_is_sample_container = _is_sample_container_key(raw_key)
                if key_is_sample_container and isinstance(raw_value, (list, dict)) and normalized_limit <= 0:
                    continue
                compact[raw_key] = _reduce(
                    raw_value,
                    in_sample_container=in_sample_container or key_is_sample_container,
                )
            return compact
        if isinstance(value, list):
            items = value[:normalized_limit] if in_sample_container else value
            return [_reduce(item, in_sample_container=False) for item in items]
        return value

    reduced = _reduce(payload or {})
    reduced["dashboard_sample_policy"] = {
        "mode": "sample_limited" if normalized_limit > 0 else "sample_omitted",
        "include_full_samples": False,
        "sample_limit": normalized_limit,
        "max_sample_limit": MAX_DASHBOARD_SAMPLE_LIMIT,
    }
    return reduced


def _build_dashboard_scope_contract(
    detail_scope: str,
    *,
    compact: bool = False,
    live_diagnostics_merged: bool | None = None,
) -> dict[str, Any]:
    scope_key = str(detail_scope or "").strip() or ("compact" if compact else "diagnostic_summary")
    configs: dict[str, dict[str, Any]] = {
        "compact": {
            "scope_label": "首页轻量概览",
            "truth_boundary": "用于首页快速展示正式发布状态；长期治理摘要读取调度器与报告快照，不重新验证其当前时效，实时诊断由明细接口提供。",
            "live_diagnostics_merged": False,
            "report_snapshot_scope": True,
            "recommended_use": "首页摘要、健康检查和正式主榜入口。",
        },
        "overview": {
            "scope_label": "首页概览",
            "truth_boundary": "由当前发布报告快照叠加当前自动化治理摘要构成。",
            "live_diagnostics_merged": True,
            "report_snapshot_scope": True,
            "recommended_use": "首页摘要和自动化状态概览。",
        },
        "diagnostic_summary": {
            "scope_label": "实时诊断摘要",
            "truth_boundary": "以当前发布报告为主线，合并当前数据库、调度和长周期维护诊断。",
            "live_diagnostics_merged": True,
            "report_snapshot_scope": False,
            "recommended_use": "运维诊断、自动化状态排查和发布前后健康判断。",
        },
        "reports": {
            "scope_label": "报告中心快照",
            "truth_boundary": "同 run 报告快照与聚合解释口径，不合并当前 live 长周期维护优先阻塞。",
            "live_diagnostics_merged": False,
            "report_snapshot_scope": True,
            "recommended_use": "报告归档、下载入口和发布漏斗复盘。",
        },
        "task_summary": {
            "scope_label": "任务摘要快照",
            "truth_boundary": "同 run 任务阶段摘要与筛选解释口径，不合并当前 live 长周期维护优先阻塞。",
            "live_diagnostics_merged": False,
            "report_snapshot_scope": True,
            "recommended_use": "筛选阶段复盘、主因解释和任务结果阅读。",
        },
        "audit_full": {
            "scope_label": "完整审计诊断",
            "truth_boundary": "包含完整报告快照、当前诊断和审计侧车字段，响应体较大。",
            "live_diagnostics_merged": True,
            "report_snapshot_scope": False,
            "recommended_use": "深度排查和开发审计。",
        },
        "full": {
            "scope_label": "完整审计诊断",
            "truth_boundary": "包含完整报告快照、当前诊断和审计侧车字段，响应体较大。",
            "live_diagnostics_merged": True,
            "report_snapshot_scope": False,
            "recommended_use": "深度排查和开发审计。",
        },
    }
    config = configs.get(scope_key, configs["diagnostic_summary"])
    effective_live_diagnostics_merged = (
        bool(config["live_diagnostics_merged"])
        if live_diagnostics_merged is None
        else bool(live_diagnostics_merged)
    )
    return {
        "detail_scope": scope_key,
        "compact": bool(compact),
        "scope_label": config["scope_label"],
        "truth_boundary": config["truth_boundary"],
        "official_publish_truth": "publication registry + current published report",
        "automation_truth": "scheduler status/detail and live scheduler state",
        "live_diagnostics_merged": effective_live_diagnostics_merged,
        "report_snapshot_scope": bool(config["report_snapshot_scope"]),
        "recommended_use": config["recommended_use"],
    }


_DASHBOARD_FILE_KEYS = (
    "project_tracker_excel",
    "filtered_excel",
    "filtered_csv",
    "dynamic_filtered_excel",
    "dynamic_filtered_csv",
    "watch_excel",
    "watch_csv",
    "dynamic_watch_excel",
    "dynamic_watch_csv",
    "excluded_excel",
    "excluded_csv",
    "threshold_compare_excel",
    "screening_audit_excel",
    "publish_chain_snapshot_json",
    "forward_validation_excel",
    "forward_validation_json",
    "forward_validation_markdown",
    "stage_b_reason_excel",
    "report_markdown",
    "report_json",
)

_DASHBOARD_ARTIFACT_KEYS = (
    "project_tracker_excel",
    "project_tracker_excel_meta",
    "filtered_excel",
    "filtered_csv",
    "filtered_excel_meta",
    "dynamic_filtered_excel",
    "dynamic_filtered_csv",
    "dynamic_filtered_excel_meta",
    "watch_excel",
    "watch_csv",
    "watch_excel_meta",
    "dynamic_watch_excel",
    "dynamic_watch_csv",
    "dynamic_watch_excel_meta",
    "excluded_excel",
    "excluded_csv",
    "excluded_excel_meta",
    "threshold_compare_excel",
    "threshold_compare_excel_meta",
    "screening_audit_excel",
    "screening_audit_excel_meta",
    "publish_chain_snapshot_json",
    "forward_validation_excel",
    "forward_validation_json",
    "forward_validation_markdown",
    "stage_b_reason_excel",
    "stage_b_reason_excel_meta",
    "report_markdown",
    "report_json",
)


def _project_artifact_values(
    artifacts: dict[str, Any] | None,
    keys: tuple[str, ...],
) -> dict[str, Any]:
    current = artifacts if isinstance(artifacts, dict) else {}
    return {key: current.get(key) for key in keys}


def _build_dashboard_files_payload(
    artifacts: dict[str, Any] | None,
    *,
    keys: tuple[str, ...],
    include_recommendations_page: bool = False,
) -> dict[str, Any]:
    payload = _project_artifact_values(artifacts, keys)
    if include_recommendations_page:
        return {
            "recommendations_page": "/recommendations",
            **payload,
        }
    return payload


def _build_database_freshness_runtime_scope(
    freshness: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(freshness or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_snapshot" if normalized_run_id else "latest_report_snapshot")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告快照" if normalized_run_id else "latest 报告快照")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要来自当前报告 payload 内的 database_freshness 快照，用于描述该 run 的数据就绪度，不属于独立历史回放诊断。",
    }


def _build_coverage_summary(
    *,
    bond_universe_snapshot: dict[str, Any] | None,
    long_term_snapshot: dict[str, Any] | None,
    counts: dict[str, Any] | None,
    run_id: str | None = None,
) -> dict[str, Any]:
    universe = dict(bond_universe_snapshot or {})
    long_term = dict(long_term_snapshot or {})
    count_payload = dict(counts or {})
    normalized_run_id = _clean_text(run_id) or _clean_text(universe.get("run_id"))
    return {
        "stored_universe_count": int(universe.get("stored_universe_count") or 0),
        "active_universe_count": int(universe.get("active_universe_count") or 0),
        "screenable_universe_count": int(universe.get("screenable_universe_count") or 0),
        "stage_b_candidate_pool_count": int(
            count_payload.get("stage_b_candidate_pool_count") or count_payload.get("publishable_pool_count") or 0
        ),
        "governance_candidate_pool_count": int(
            count_payload.get("stage_b_candidate_pool_count") or count_payload.get("publishable_pool_count") or 0
        ),
        "publishable_pool_count": int(count_payload.get("publishable_pool_count") or 0),
        "has_latest_nav_count": int(universe.get("has_latest_nav_count") or 0),
        "has_1y_risk_metrics_count": int(universe.get("has_1y_risk_metrics_count") or 0),
        "annual_return_history_fund_count": int(long_term.get("annual_return_history_fund_count") or 0),
        "rank_history_fund_count": int(long_term.get("rank_history_fund_count") or 0),
        "full_feature_ready_fund_count": int(long_term.get("full_feature_ready_fund_count") or 0),
        "full_feature_gap_fund_count": int(long_term.get("full_feature_gap_fund_count") or 0),
        "neutralized_long_term_feature_fund_count": int(long_term.get("neutralized_fund_count") or 0),
        "run_id": normalized_run_id,
        "latest_canonical_run_id": normalized_run_id,
        "replay_precision_status": "missing",
        "replay_precision_status_label": "非精确回放",
        "auxiliary_scope": (
            "same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate"
        ),
        "auxiliary_scope_label": (
            "同 run 报告聚合" if normalized_run_id else "latest 报告聚合"
        ),
        "scope_detail": "当前摘要由同一报告中的 bond_universe_snapshot、long_term_history_snapshot 与 counts 聚合得出，用于说明该 run 的覆盖口径，不属于独立历史回放诊断。",
    }


def _build_coverage_layers_runtime_scope(
    layers: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(layers or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 bond_universe_snapshot、long_term_history_snapshot 与 counts 归一化/拼装得出，用于解释该 run 的覆盖分层口径，不属于独立历史回放诊断。",
    }


def _build_portfolio_display_scope_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 counts 与 portfolio_suggestions 聚合得出，用于解释推荐页压缩展示池与正式主榜的关系，不属于独立历史回放诊断。",
    }


def _build_publish_layer_semantics_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 counts、publish_funnel_summary、portfolio_display_scope 与 portfolio_suggestions 聚合得出，用于解释正式主榜、动态参考榜、自动诊断池与前端展示池之间的层级语义，不属于独立历史回放诊断。",
    }


def _build_positive_ratio_semantics_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 positive_ratio 解释块与 low_vol_positive_ratio_shadow_watch 聚合得出，用于描述项目内正收益稳定性指标的治理语义，不属于独立历史回放诊断。",
    }


def _build_portfolio_group_brief_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 portfolio_suggestions 分组结果聚合得出，用于描述前端组合分组的风格、持有期与风险标签概览，不属于独立历史回放诊断。",
    }


def _build_report_timing_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_snapshot" if normalized_run_id else "latest_report_snapshot")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告快照" if normalized_run_id else "latest 报告快照")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要来自当前 run 报告 payload 内的 report_timing_summary 快照，用于描述该次报告构建耗时，不属于独立历史回放诊断。",
    }


def _build_low_vol_positive_ratio_shadow_watch_runtime_scope(
    watch: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(watch or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的低波风格正收益 shadow 解释块归一化得出，用于描述项目内 shadow 观察样本、差异告警与伴随原因，不属于独立历史回放诊断。",
    }


def _build_long_term_governance_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 long_term_history_snapshot 与 scheduler maintenance 明细聚合得出，用于解释长周期历史覆盖、中性化来源与治理候选池，不属于独立历史回放诊断。",
    }


def _build_tracker_summary_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_snapshot" if normalized_run_id else "latest_report_snapshot")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告快照" if normalized_run_id else "latest 报告快照")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要来自当前 run 报告 payload 内的 project_tracker_summary 快照，用于描述累计跟踪结果与本期变动，不属于独立历史回放诊断。",
    }


def _build_publication_status_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    latest_canonical_run_id = (
        payload.get("latest_canonical_run_id")
        or normalized_run_id
        or _clean_text(payload.get("latest_successful_run_id"))
        or _clean_text(payload.get("current_published_run_id"))
    )
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id or _clean_text(payload.get("latest_successful_run_id")),
        "latest_canonical_run_id": latest_canonical_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": payload.get("auxiliary_scope") or "cross_run_current_publication_state",
        "auxiliary_scope_label": payload.get("auxiliary_scope_label") or "当前发布治理状态",
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由 current publication registry、latest successful/publishable/current published 指针与 tracker 变化提示共同汇总得出，属于跨期当前发布治理诊断，不提供精确历史回放。",
    }


def _build_publication_summary_for_response(
    publication: dict[str, Any] | None,
    *,
    tracker_summary: dict[str, Any] | None = None,
    publication_status_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = build_publication_summary(
        publication,
        tracker_summary=tracker_summary,
    )
    if publication_status_summary is not None:
        summary["status_summary"] = publication_status_summary
    return summary


def _build_automation_autonomy_runtime_scope(
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        **payload,
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": payload.get("auxiliary_scope") or "cross_run_current_automation_state",
        "auxiliary_scope_label": payload.get("auxiliary_scope_label") or "当前自动化治理状态",
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由 scheduler_overview、scheduler_chain_audit 与当前数据库自动化观测状态共同汇总得出，属于跨期当前自动化治理诊断，不提供精确历史回放。",
    }


def _build_publish_funnel_runtime_scope(
    summary: dict[str, Any] | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = dict(summary or {})
    normalized_run_id = _clean_text(run_id)
    return {
        **payload,
        "run_id": payload.get("run_id") or normalized_run_id,
        "latest_canonical_run_id": payload.get("latest_canonical_run_id") or normalized_run_id,
        "replay_precision_status": payload.get("replay_precision_status") or "missing",
        "replay_precision_status_label": payload.get("replay_precision_status_label") or "非精确回放",
        "auxiliary_scope": (
            payload.get("auxiliary_scope")
            or ("same_run_report_aggregate" if normalized_run_id else "latest_report_aggregate")
        ),
        "auxiliary_scope_label": (
            payload.get("auxiliary_scope_label")
            or ("同 run 报告聚合" if normalized_run_id else "latest 报告聚合")
        ),
        "scope_detail": payload.get("scope_detail")
        or "当前摘要由同一报告中的 counts、coverage_layers 与 project_publish_summary 聚合得出，用于描述该 run 的发布漏斗口径，不属于独立历史回放诊断。",
    }


def _load_screening_relevant_current_nav_blocking_counts(
    weekly_nav_history_consistency_audit: dict[str, Any] | None,
    weekly_nav_history_consistency_summary: dict[str, Any] | None,
) -> tuple[int, int]:
    summary = dict(weekly_nav_history_consistency_summary or {})
    explicit_fund_total = summary.get("screening_relevant_current_screening_blocking_fund_total")
    explicit_window_total = summary.get("screening_relevant_current_screening_blocking_window_total")
    if explicit_fund_total is not None or explicit_window_total is not None:
        return (_to_int(explicit_fund_total), _to_int(explicit_window_total))

    audit_payload = dict(weekly_nav_history_consistency_audit or {})
    artifact_payload: dict[str, Any] = {}
    artifact_path = _clean_text(audit_payload.get("artifact_path"))
    if artifact_path:
        try:
            artifact_payload = json.loads(Path(artifact_path).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            artifact_payload = {}
    if not artifact_payload:
        try:
            _latest_path, latest_payload = load_latest_nav_history_consistency_audit_payload()
            artifact_payload = dict(latest_payload or {})
        except Exception:
            artifact_payload = {}

    rows = artifact_payload.get("top_current_screening_blocking_review_rows") or []
    if rows:
        screening_relevant_rows = [row for row in rows if bool((row or {}).get("screening_relevant"))]
        return (
            int(len(screening_relevant_rows)),
            int(
                sum(
                    _to_int((row or {}).get("current_screening_blocking_window_count"))
                    for row in screening_relevant_rows
                )
            ),
        )

    raw_fund_total = _to_int(summary.get("funds_with_current_screening_blocking_issue"))
    raw_window_total = _to_int(summary.get("windows_with_current_screening_blocking_issue"))
    screening_relevant_actionable_fund_total = _to_int(
        summary.get("funds_with_screening_relevant_actionable_issue")
    )
    derived_fund_total = min(raw_fund_total, screening_relevant_actionable_fund_total)
    derived_window_total = raw_window_total if derived_fund_total > 0 else 0
    return (derived_fund_total, derived_window_total)


def _build_nav_risk_governance_summary(
    payload: dict[str, Any],
    publication: dict[str, Any],
    scheduler_overview_api: dict[str, Any],
    scheduler_chain_audit: dict[str, Any],
    risk_lag_summary: dict[str, Any],
    risk_sync_summary: dict[str, Any],
    risk_coverage_gap_summary: dict[str, Any],
) -> dict[str, Any]:
    freshness = payload.get("database_freshness") or {}
    effective_nav_snapshot = freshness.get("effective_nav_snapshot") or {}
    risk_freshness = freshness.get("risk_1y_freshness") or {}
    publication_layers = publication.get("automation_layers") or {}
    latest_successful_run = publication_layers.get("latest_successful_run") or {}
    publish_governance_summary = latest_successful_run.get("publish_governance_summary") or {}
    publish_blockers = list(latest_successful_run.get("publish_blockers") or [])
    weekly_run = scheduler_overview_api.get("weekly_run") or {}
    weekly_nav_history_consistency_audit = weekly_run.get("nav_history_consistency_audit") or {}
    weekly_nav_history_consistency_summary = (
        weekly_nav_history_consistency_audit.get("summary") or {}
        if isinstance(weekly_nav_history_consistency_audit, dict)
        else {}
    )

    nav_candidate_date = _clean_text(effective_nav_snapshot.get("candidate_date"))
    nav_ready = bool(effective_nav_snapshot.get("ready", True))
    nav_readiness_reason = _clean_text(effective_nav_snapshot.get("readiness_reason")) or (
        "ready" if nav_ready else "effective_nav_not_ready"
    )
    risk_candidate_date = _clean_text(risk_freshness.get("candidate_date")) or _clean_text(
        risk_lag_summary.get("candidate_date")
    )
    risk_effective_end_date = _clean_text(risk_freshness.get("risk_1y_effective_end_date")) or _clean_text(
        risk_lag_summary.get("risk_1y_effective_end_date")
    )
    dates_aligned = bool(
        risk_freshness.get("dates_aligned")
        if risk_freshness.get("dates_aligned") is not None
        else risk_lag_summary.get("dates_aligned")
    )
    coverage_sufficient = bool(
        risk_freshness.get("coverage_sufficient")
        if risk_freshness.get("coverage_sufficient") is not None
        else risk_lag_summary.get("coverage_sufficient")
    )
    coverage_ratio = _to_float(
        risk_freshness.get("coverage_ratio_vs_candidate")
        if risk_freshness.get("coverage_ratio_vs_candidate") is not None
        else risk_lag_summary.get("coverage_ratio_vs_candidate")
    )
    minimum_coverage_ratio = _to_float(
        risk_freshness.get("minimum_coverage_ratio")
        if risk_freshness.get("minimum_coverage_ratio") is not None
        else risk_lag_summary.get("minimum_coverage_ratio")
    )
    threshold_semantics = build_risk_coverage_threshold_semantics(
        {
            "coverage_ratio_vs_candidate": coverage_ratio,
            "minimum_coverage_ratio": minimum_coverage_ratio,
            "publish_minimum_coverage_ratio": (
                risk_freshness.get("publish_minimum_coverage_ratio")
                if risk_freshness.get("publish_minimum_coverage_ratio") is not None
                else risk_lag_summary.get("publish_minimum_coverage_ratio")
            ),
            "observation_coverage_ratio": (
                risk_freshness.get("observation_coverage_ratio")
                if risk_freshness.get("observation_coverage_ratio") is not None
                else risk_lag_summary.get("observation_coverage_ratio")
            ),
        },
        coverage_sufficient=coverage_sufficient,
    )
    minimum_coverage_ratio = _to_float(threshold_semantics.get("minimum_coverage_ratio"))
    nav_risk_aligned = bool(nav_ready and dates_aligned and coverage_sufficient)

    severity_rank = {"blocking": 3, "warning": 2, "info": 1, "none": 0}

    issues: list[dict[str, Any]] = []
    residual_warning_lag_summary = {
        "candidate_date": risk_candidate_date,
        "risk_1y_effective_end_date": risk_effective_end_date,
        "dates_aligned": dates_aligned,
        "coverage_sufficient": coverage_sufficient,
        "coverage_ratio_vs_candidate": coverage_ratio,
        "minimum_coverage_ratio": minimum_coverage_ratio,
        "publish_minimum_coverage_ratio": threshold_semantics["publish_minimum_coverage_ratio"],
        "observation_coverage_ratio": threshold_semantics["observation_coverage_ratio"],
        "coverage_observation_line_met": threshold_semantics["coverage_observation_line_met"],
        "coverage_gate_status": threshold_semantics["coverage_gate_status"],
        "coverage_gate_label": threshold_semantics["coverage_gate_label"],
        "coverage_gate_explanation": threshold_semantics["coverage_gate_explanation"],
        "effective_risk_uncovered_candidate_total": _to_int(
            risk_lag_summary.get("effective_risk_uncovered_candidate_total")
        ),
    }
    residual_warning_summary = build_risk_residual_warning_summary(
        residual_warning_lag_summary,
        risk_sync_summary,
        risk_coverage_gap_summary,
    )
    scope_rank = {"report_baseline_verified": 5, "snapshot_frozen": 4, "inputs_frozen": 3,
                  "reference_only": 3, "exact": 3, "partial": 3, "latest_only": 2, "missing": 1, "unknown": 0}
    residual_scope_source = max(
        (
            dict(risk_coverage_gap_summary or {}),
            dict(risk_lag_summary or {}),
            dict(risk_sync_summary or {}),
        ),
        key=lambda item: scope_rank.get(str(item.get("replay_precision_status") or "").strip(), 0),
        default={},
    )
    residual_warning_summary = {
        **dict(residual_warning_summary or {}),
        "run_id": residual_scope_source.get("run_id"),
        "latest_canonical_run_id": residual_scope_source.get("latest_canonical_run_id"),
        "replay_precision_status": residual_scope_source.get("replay_precision_status") or "latest_only",
        "replay_precision_status_label": residual_scope_source.get("replay_precision_status_label") or "仅 latest 审计",
        "auxiliary_scope": residual_scope_source.get("auxiliary_scope") or "latest_audit_only",
        "auxiliary_scope_label": residual_scope_source.get("auxiliary_scope_label") or "当前最新审计",
        "scope_detail": (
            residual_scope_source.get("scope_detail")
            or "当前摘要基于最新 risk lag / risk sync / risk coverage gap 审计即时合成，不提供历史 run 级回放。"
        ),
    }

    def add_issue(
        code: str,
        label: str,
        detail: str,
        *,
        severity: str,
        action_path: str | None,
        source: str,
        governance_stage: str | None = None,
    ) -> None:
        issues.append(
            {
                "code": code,
                "label": label,
                "detail": detail,
                "severity": severity,
                "action_path": action_path,
                "source": source,
                "governance_stage": governance_stage,
            }
        )

    if not nav_ready:
        add_issue(
            "nav_snapshot_not_ready",
            "净值快照未就绪",
            nav_readiness_reason,
            severity="blocking",
            action_path="collection",
            source="effective_nav_snapshot",
            governance_stage="nav_snapshot",
        )
    if nav_ready and not dates_aligned:
        add_issue(
            "risk_snapshot_not_aligned",
            "风险快照未对齐",
            f"candidate_date={risk_candidate_date or '-'}, risk_end_date={risk_effective_end_date or '-'}",
            severity="blocking",
            action_path="risk",
            source="risk_lag_summary",
            governance_stage="risk_snapshot",
        )
    if nav_ready and dates_aligned and not coverage_sufficient:
        add_issue(
            "risk_coverage_insufficient",
            "风险覆盖不足",
            f"{coverage_ratio:.6f} < {minimum_coverage_ratio:.6f}",
            severity="blocking",
            action_path="risk",
            source="risk_lag_summary",
            governance_stage="risk_snapshot",
        )
    if _to_int(risk_coverage_gap_summary.get("current_publish_blocker_total")) > 0:
        add_issue(
            "risk_publish_blockers_in_formal_pool",
            "正式池存在风险发布阻断",
            (
                f"current_publish_blocker_total="
                f"{_to_int(risk_coverage_gap_summary.get('current_publish_blocker_total'))}"
            ),
            severity="blocking",
            action_path="risk",
            source="risk_coverage_gap_audit",
            governance_stage="risk_snapshot",
        )
    if _to_int(risk_coverage_gap_summary.get("actionable_gap_total")) > 0:
        add_issue(
            "risk_actionable_gap_pending",
            "存在可立即补算的风险缺口",
            f"actionable_gap_total={_to_int(risk_coverage_gap_summary.get('actionable_gap_total'))}",
            severity="warning",
            action_path="risk",
            source="risk_coverage_gap_audit",
            governance_stage="risk_snapshot",
        )
    if _to_int(risk_sync_summary.get("actionable_candidate_total")) > 0:
        add_issue(
            "nav_risk_sync_candidates_pending",
            "存在 NAV 变更后待同步风险样本",
            f"actionable_candidate_total={_to_int(risk_sync_summary.get('actionable_candidate_total'))}",
            severity="info",
            action_path="risk",
            source="risk_sync_audit",
            governance_stage="risk_sync",
        )
    nav_current_screening_blocking_fund_total = _to_int(
        weekly_nav_history_consistency_summary.get("funds_with_current_screening_blocking_issue")
    )
    nav_current_screening_blocking_window_total = _to_int(
        weekly_nav_history_consistency_summary.get("windows_with_current_screening_blocking_issue")
    )
    nav_screening_relevant_actionable_fund_total = _to_int(
        weekly_nav_history_consistency_summary.get("funds_with_screening_relevant_actionable_issue")
    )
    (
        nav_current_screening_relevant_blocking_fund_total,
        nav_current_screening_relevant_blocking_window_total,
    ) = _load_screening_relevant_current_nav_blocking_counts(
        weekly_nav_history_consistency_audit,
        weekly_nav_history_consistency_summary,
    )
    if nav_current_screening_relevant_blocking_fund_total > 0:
        add_issue(
            "nav_history_current_screening_blockers_present",
            "周维护发现当前筛选阻断级 NAV 质量问题",
            (
                "screening_relevant_current_screening_blocking_fund_total="
                f"{nav_current_screening_relevant_blocking_fund_total}; "
                "screening_relevant_current_screening_blocking_window_total="
                f"{nav_current_screening_relevant_blocking_window_total}"
            ),
            severity="warning",
            action_path="collection",
            source="nav_history_consistency_audit",
            governance_stage="nav_history_consistency",
        )

    for blocker in publish_blockers:
        code = _clean_text(blocker.get("code"))
        if not code:
            continue
        if any(existing.get("code") == code for existing in issues):
            continue
        add_issue(
            code,
            _clean_text(blocker.get("label")) or code,
            _clean_text(blocker.get("detail")) or code,
            severity=_clean_text(blocker.get("severity")) or "blocking",
            action_path=_clean_text(blocker.get("action_path")),
            source="publication",
            governance_stage=_clean_text(blocker.get("governance_stage")),
        )

    highest_issue = max(
        issues,
        key=lambda item: (
            severity_rank.get(str(item.get("severity") or "none"), 0),
            str(item.get("code") or ""),
        ),
        default={},
    )
    highest_severity = str(highest_issue.get("severity") or "none")

    automation_actions: list[dict[str, Any]] = []
    for run_key, task_label in (
        ("snapshot_watchdog_run", "快照看门狗"),
        ("intraday_run", "盘中补刷"),
        ("daily_run", "日更调度"),
        ("weekly_run", "周维护"),
    ):
        run = scheduler_overview_api.get(run_key) or {}
        activity_at = _clean_text(run.get("finished_at")) or _clean_text(run.get("started_at"))
        if not activity_at:
            continue
        if run.get("screening_triggered"):
            action_path = "screening"
        elif _clean_text(run.get("action_category")) == "maintenance_rescreen_triggered":
            action_path = "screening"
        elif run.get("report_refresh_triggered") or _clean_text(run.get("action_category")) == "report_refresh_triggered":
            action_path = "report_refresh"
        elif _clean_text(run.get("action_category")) == "screening_triggered":
            action_path = "screening"
        elif _clean_text(run.get("warning_reason")):
            action_path = "collection"
        else:
            action_path = "none"
        automation_actions.append(
            {
                "task_key": run_key.replace("_run", ""),
                "task_label": task_label,
                "status": _clean_text(run.get("status")),
                "action_category": _clean_text(run.get("action_category")),
                "screening_reason": _clean_text(run.get("screening_reason")),
                "warning_reason": _clean_text(run.get("warning_reason")),
                "skip_reason": _clean_text(run.get("skip_reason")),
                "action_path": action_path,
                "activity_at": activity_at,
            }
        )
    automation_actions.sort(
        key=lambda item: (
            str(item.get("activity_at") or ""),
            1 if str(item.get("action_path") or "") != "none" else 0,
        ),
        reverse=True,
    )
    latest_automation_action = next(
        (
            item for item in automation_actions
            if str(item.get("action_path") or "") != "none"
        ),
        automation_actions[0] if automation_actions else {},
    )

    blocker_total = sum(1 for item in issues if str(item.get("severity") or "") == "blocking")
    warning_total = sum(1 for item in issues if str(item.get("severity") or "") == "warning")
    info_total = sum(1 for item in issues if str(item.get("severity") or "") == "info")
    readiness_status = (
        "blocking"
        if blocker_total > 0
        else ("warning" if warning_total > 0 else "ready")
    )
    primary_issue = highest_issue or {}
    if blocker_total > 0 or warning_total > 0:
        recommended_action_path = _clean_text(primary_issue.get("action_path")) or "none"
    elif info_total > 0 and _clean_text(primary_issue.get("action_path")):
        recommended_action_path = _clean_text(primary_issue.get("action_path")) or "none"
    elif bool(scheduler_chain_audit.get("screening_backlog_due")):
        recommended_action_path = "screening"
    else:
        recommended_action_path = "none"

    headline = (
        f"NAV 快照 {nav_candidate_date or '-'}，"
        f"风险 1Y 截止 {risk_effective_end_date or '-'}；"
        f"发布态 {publish_governance_summary.get('status') or ('publishable' if nav_risk_aligned else 'blocked')}；"
        f"建议动作 {recommended_action_path}。"
    )
    explanation_parts = [
        f"nav_ready={nav_ready}",
        f"dates_aligned={dates_aligned}",
        f"coverage_sufficient={coverage_sufficient}",
        (
            f"coverage_ratio={coverage_ratio:.6f}/{minimum_coverage_ratio:.6f}"
            if minimum_coverage_ratio > 0
            else f"coverage_ratio={coverage_ratio:.6f}"
        ),
        (
            "coverage_observation="
            f"{coverage_ratio:.6f}/{threshold_semantics['observation_coverage_ratio']:.6f};"
            f"{threshold_semantics['coverage_gate_status']}"
        ),
        (
            f"risk_actionable_gap_total={_to_int(risk_coverage_gap_summary.get('actionable_gap_total'))}"
        ),
        (
            f"risk_sync_actionable_candidate_total={_to_int(risk_sync_summary.get('actionable_candidate_total'))}"
        ),
    ]
    if primary_issue:
        explanation_parts.append(
            f"primary_issue={primary_issue.get('code')}:{primary_issue.get('detail')}"
        )

    return {
        "available": True,
        "readiness_status": readiness_status,
        "nav_ready": nav_ready,
        "nav_candidate_date": nav_candidate_date,
        "nav_readiness_reason": nav_readiness_reason,
        "risk_candidate_date": risk_candidate_date,
        "risk_effective_end_date": risk_effective_end_date,
        "dates_aligned": dates_aligned,
        "coverage_sufficient": coverage_sufficient,
        "coverage_ratio_vs_candidate": coverage_ratio,
        "minimum_coverage_ratio": minimum_coverage_ratio,
        "publish_minimum_coverage_ratio": threshold_semantics["publish_minimum_coverage_ratio"],
        "observation_coverage_ratio": threshold_semantics["observation_coverage_ratio"],
        "coverage_observation_line_met": threshold_semantics["coverage_observation_line_met"],
        "coverage_gate_status": threshold_semantics["coverage_gate_status"],
        "coverage_gate_label": threshold_semantics["coverage_gate_label"],
        "coverage_gate_explanation": threshold_semantics["coverage_gate_explanation"],
        "nav_risk_aligned": nav_risk_aligned,
        "publication_publishable": bool(latest_successful_run.get("publishable")),
        "publication_risk_publish_ready": bool(latest_successful_run.get("risk_publish_ready", True)),
        "publication_governance_summary": publish_governance_summary,
        "risk_sync_summary": {
            "actionable_candidate_total": _to_int(risk_sync_summary.get("actionable_candidate_total")),
            "period_eligible_gap_candidate_total": _to_int(
                risk_sync_summary.get("period_eligible_gap_candidate_total")
            ),
            "blocked_by_nav_history_total": _to_int(risk_sync_summary.get("blocked_by_nav_history_total")),
            "blocked_by_nav_history_lag_total": _to_int(
                risk_sync_summary.get("blocked_by_nav_history_lag_total")
            ),
            "blocked_by_returns_lag_total": _to_int(risk_sync_summary.get("blocked_by_returns_lag_total")),
        },
        "risk_coverage_gap_summary": {
            "actionable_gap_total": _to_int(risk_coverage_gap_summary.get("actionable_gap_total")),
            "current_publish_blocker_total": _to_int(
                risk_coverage_gap_summary.get("current_publish_blocker_total")
            ),
            "formal_pool_blocking_total": _to_int(
                risk_coverage_gap_summary.get("formal_pool_blocking_total")
            ),
            "formal_pool_natural_short_history_total": _to_int(
                risk_coverage_gap_summary.get("formal_pool_natural_short_history_total")
            ),
            "run_id": risk_coverage_gap_summary.get("run_id"),
            "latest_canonical_run_id": risk_coverage_gap_summary.get("latest_canonical_run_id"),
            "replay_precision_status": risk_coverage_gap_summary.get("replay_precision_status"),
            "replay_precision_status_label": risk_coverage_gap_summary.get("replay_precision_status_label"),
            "auxiliary_scope": risk_coverage_gap_summary.get("auxiliary_scope"),
            "auxiliary_scope_label": risk_coverage_gap_summary.get("auxiliary_scope_label"),
        },
        "risk_residual_warning_summary": residual_warning_summary,
        "nav_history_consistency_summary": {
            "status": _clean_text(weekly_nav_history_consistency_audit.get("status")),
            "funds_with_current_screening_blocking_issue": nav_current_screening_blocking_fund_total,
            "windows_with_current_screening_blocking_issue": nav_current_screening_blocking_window_total,
            "screening_relevant_current_screening_blocking_fund_total": (
                nav_current_screening_relevant_blocking_fund_total
            ),
            "screening_relevant_current_screening_blocking_window_total": (
                nav_current_screening_relevant_blocking_window_total
            ),
            "funds_with_actionable_issue": _to_int(
                weekly_nav_history_consistency_summary.get("funds_with_actionable_issue")
            ),
            "funds_with_screening_relevant_actionable_issue": nav_screening_relevant_actionable_fund_total,
            "run_id": None,
            "latest_canonical_run_id": None,
            "replay_precision_status": "latest_only",
            "replay_precision_status_label": "仅 latest 审计",
            "auxiliary_scope": "latest_audit_only",
            "auxiliary_scope_label": "当前最新审计",
            "scope_detail": "当前摘要基于最新 nav history consistency 周审计产物推导，不提供历史 run 级别回放。",
        },
        "scheduler_backlog_due": bool(scheduler_chain_audit.get("screening_backlog_due")),
        "scheduler_backlog_reason": _clean_text(scheduler_chain_audit.get("screening_backlog_reason")),
        "latest_automation_action": latest_automation_action,
        "recommended_action_path": recommended_action_path,
        "issue_total": len(issues),
        "blocking_total": blocker_total,
        "warning_total": warning_total,
        "info_total": info_total,
        "primary_issue_code": _clean_text(primary_issue.get("code")),
        "primary_issue_label": _clean_text(primary_issue.get("label")),
        "primary_issue_detail": _clean_text(primary_issue.get("detail")),
        "primary_issue_action_path": _clean_text(primary_issue.get("action_path")),
        "issues": issues,
        "headline": headline,
        "explanation": "；".join(explanation_parts),
        "run_id": _clean_text(payload.get("run_id")),
        "latest_canonical_run_id": (
            _clean_text(publication.get("latest_canonical_run_id"))
            or _clean_text(latest_successful_run.get("run_id"))
            or _clean_text(payload.get("run_id"))
        ),
        "replay_precision_status": "missing",
        "replay_precision_status_label": "非精确回放",
        "auxiliary_scope": "cross_run_current_risk_state",
        "auxiliary_scope_label": "当前风险治理状态",
        "scope_detail": (
            "当前摘要由当前 run 报告中的 database_freshness 与当前 latest risk lag / risk sync / "
            "risk coverage gap 审计、当前 scheduler 自动化状态、当前 publication 治理状态共同汇总得出，"
            "属于跨期当前治理诊断，不提供精确历史回放。"
        ),
    }


def _classify_publish_focus_metric(reason: dict[str, Any]) -> str | None:
    code = str(reason.get("code") or "").strip().lower()
    reason_text = str(reason.get("reason") or "")
    display_text = str(reason.get("display_reason") or "")
    combined_text = f"{reason_text} {display_text}"

    if "annual" in code or "年化" in combined_text:
        return "annual_return"
    if "sharpe" in code or "夏普" in combined_text:
        return "sharpe"
    if "positive_ratio" in code or "正收益" in combined_text:
        return "positive_ratio"
    if code.startswith("manager_") or "经理" in combined_text:
        return "manager_stability"
    if (
        code in {"below_weighted_floor", "below_short_floor", "below_long_floor", "score_low"}
        or "得分" in combined_text
        or "评分" in combined_text
    ):
        return "score"
    return None


def _enrich_publish_reason_summary(
    summary: dict[str, Any],
    exclusion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = enrich_publish_reason_summary(summary, exclusion_summary)
    scope_source = dict(exclusion_summary or {})
    if scope_source:
        payload.setdefault("run_id", scope_source.get("run_id"))
        payload.setdefault("latest_canonical_run_id", scope_source.get("latest_canonical_run_id"))
        payload.setdefault("replay_precision_status", scope_source.get("replay_precision_status"))
        payload.setdefault(
            "replay_precision_status_label",
            scope_source.get("replay_precision_status_label"),
        )
        payload.setdefault("auxiliary_scope", scope_source.get("auxiliary_scope"))
        payload.setdefault("auxiliary_scope_label", scope_source.get("auxiliary_scope_label"))
        payload.setdefault("scope_detail", scope_source.get("scope_detail"))
    else:
        payload.setdefault("run_id", None)
        payload.setdefault("latest_canonical_run_id", None)
        payload.setdefault("replay_precision_status", None)
        payload.setdefault("replay_precision_status_label", None)
        payload.setdefault("auxiliary_scope", None)
        payload.setdefault("auxiliary_scope_label", None)
        payload.setdefault("scope_detail", None)
    return payload


def _build_dashboard_shared_context(
    payload: dict[str, Any],
    publication: dict[str, Any],
    scheduler_state: dict[str, Any],
    *,
    settings: Any,
    manual_operation_mode: bool,
    normalize_portfolio_groups: Any,
    build_portfolio_group_brief: Any,
    build_database_source_audit: Any,
    load_database_overview: Any,
    load_database_integrity_summary: Any,
    load_nav_lag_summary: Any,
    load_risk_lag_summary: Any,
    load_risk_sync_summary: Any,
    load_risk_coverage_gap_summary: Any,
    load_bond_universe_snapshot: Any,
    load_exclusion_reason_summary: Any,
    normalize_coverage_layers: Any,
    build_coverage_layers_from_snapshots: Any,
    build_publish_funnel_summary: Any,
    build_scheduler_overview_summary: Any,
    build_scheduler_overview_api_summary: Any,
    build_scheduler_chain_audit: Any,
    build_automation_autonomy_summary: Any,
    merge_live_long_term_priority_blocked_summary: Any | None = None,
    detail_scope: str = "diagnostic_summary",
) -> dict[str, Any]:
    profile_started_at, build_profile = _start_dashboard_build_profile(detail_scope)
    step_started_at = time.perf_counter()
    counts = payload.get("counts") or {}
    publish_summary = payload.get("project_publish_summary") or {}
    tracker_summary = publish_summary.get("project_tracker_summary") or {}
    portfolio_summary = normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    artifacts = payload.get("artifacts") or {}
    report_timing_summary = publish_summary.get("report_timing_summary") or {}
    low_vol_positive_ratio_shadow_watch = normalize_low_vol_positive_ratio_shadow_watch(
        publish_summary, report_identity=payload, settings=settings,
    )
    effective_nav_snapshot = scheduler_state.get("effective_nav_snapshot") or {}
    latest_screened_snapshot = scheduler_state.get("latest_screened_snapshot") or {}
    include_database_diagnostics = detail_scope in {
        "overview",
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_database_source_audit = detail_scope in {
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_scheduler_overview_api = detail_scope in {
        "overview",
        "task_summary",
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_scheduler_diagnostics = detail_scope in {
        "overview",
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_portfolio_semantics = detail_scope in {
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_positive_ratio_semantics = detail_scope in {
        "reports",
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_live_long_term_maintenance = detail_scope in {
        "overview",
        "diagnostic_summary",
        "full",
        "audit_full",
    }
    include_long_term_semantic_null_snapshot_audit = detail_scope in {
        "full",
        "audit_full",
    }
    _record_dashboard_build_step(build_profile, "report_core_fields", step_started_at)

    portfolio_display_scope: dict[str, Any] = {}
    portfolio_group_brief: dict[str, Any] = {}
    if include_portfolio_semantics:
        step_started_at = time.perf_counter()
        portfolio_display_scope = build_portfolio_display_scope(counts, portfolio_summary)
        portfolio_group_brief = build_portfolio_group_brief(portfolio_summary)
        _record_dashboard_build_step(build_profile, "portfolio_semantics", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "portfolio_semantics")

    database_overview = {}
    database_integrity = {}
    nav_lag_summary = {}
    risk_lag_summary = {}
    risk_sync_summary = {}
    risk_coverage_gap_summary = {}
    if include_database_diagnostics:
        database_diagnostics_started_at = time.perf_counter()
        step_started_at = time.perf_counter()
        database_overview = load_database_overview(effective_nav_snapshot)
        _record_dashboard_build_step(build_profile, "database_overview", step_started_at)

        step_started_at = time.perf_counter()
        database_integrity = load_database_integrity_summary()
        _record_dashboard_build_step(build_profile, "database_integrity", step_started_at)

        step_started_at = time.perf_counter()
        nav_lag_summary = load_nav_lag_summary()
        _record_dashboard_build_step(build_profile, "nav_lag_summary", step_started_at)

        step_started_at = time.perf_counter()
        risk_lag_summary = load_risk_lag_summary()
        _record_dashboard_build_step(build_profile, "risk_lag_summary", step_started_at)

        step_started_at = time.perf_counter()
        risk_sync_summary = load_risk_sync_summary()
        _record_dashboard_build_step(build_profile, "risk_sync_summary", step_started_at)

        step_started_at = time.perf_counter()
        risk_coverage_gap_summary = load_risk_coverage_gap_summary()
        _record_dashboard_build_step(build_profile, "risk_coverage_gap_summary", step_started_at)

        _record_dashboard_build_step(
            build_profile,
            "database_diagnostics",
            database_diagnostics_started_at,
        )
    else:
        _record_dashboard_build_skip(build_profile, "database_diagnostics")

    step_started_at = time.perf_counter()
    bond_universe_snapshot = load_bond_universe_snapshot(payload)
    long_term_snapshot = ((payload.get("database_freshness") or {}).get("long_term_history_snapshot") or {})
    coverage_layers = normalize_coverage_layers(
        payload.get("coverage_layers")
        or build_coverage_layers_from_snapshots(
            bond_universe_snapshot,
            long_term_snapshot,
            counts,
        ),
        counts=counts,
    )
    publish_funnel_summary = build_publish_funnel_summary(payload, coverage_layers)
    _record_dashboard_build_step(build_profile, "coverage_and_publish_funnel", step_started_at)

    step_started_at = time.perf_counter()
    exclusion_summary = load_exclusion_reason_summary(payload)
    publish_reason_summary = _enrich_publish_reason_summary(
        publish_summary.get("publish_reason_summary") or {},
        exclusion_summary=exclusion_summary,
    )
    publish_run_id = str(payload.get("run_id") or "").strip() or None
    if not publish_reason_summary.get("run_id"):
        publish_reason_summary["run_id"] = publish_run_id
    if not publish_reason_summary.get("latest_canonical_run_id"):
        publish_reason_summary["latest_canonical_run_id"] = publish_run_id
    if publish_run_id:
        if not publish_reason_summary.get("replay_precision_status"):
            publish_reason_summary["replay_precision_status"] = "missing"
        if not publish_reason_summary.get("replay_precision_status_label"):
            publish_reason_summary["replay_precision_status_label"] = "非精确回放"
        if not publish_reason_summary.get("auxiliary_scope"):
            publish_reason_summary["auxiliary_scope"] = "same_run_report_artifact_rebuild"
        if not publish_reason_summary.get("auxiliary_scope_label"):
            publish_reason_summary["auxiliary_scope_label"] = "同 run 报告 + 侧车补建"
        if not publish_reason_summary.get("scope_detail"):
            publish_reason_summary["scope_detail"] = (
                "当前摘要优先复用当前 run 报告内的筛选解释块；缺失时会回退到同批报告产物侧车结果补建，"
                "不属于独立历史回放结果。"
            )
    _record_dashboard_build_step(build_profile, "exclusion_and_publish_reason", step_started_at)

    scheduler_overview = {}
    scheduler_overview_api = {}
    if include_scheduler_overview_api or include_live_long_term_maintenance:
        step_started_at = time.perf_counter()
        scheduler_overview = build_scheduler_overview_summary(scheduler_state)
        if include_scheduler_overview_api:
            scheduler_overview_api = build_scheduler_overview_api_summary(scheduler_overview)
        _record_dashboard_build_step(build_profile, "scheduler_overview", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "scheduler_overview")

    scheduler_path_governance = {}
    scheduler_chain_audit = {}
    automation_autonomy = {}
    if include_scheduler_diagnostics:
        step_started_at = time.perf_counter()
        scheduler_path_governance = (
            scheduler_overview_api.get("scheduler_path_governance")
            or build_scheduler_path_governance_summary(scheduler_overview)
        )
        scheduler_chain_audit = build_scheduler_chain_audit(scheduler_overview)
        automation_autonomy = build_automation_autonomy_summary(
            scheduler_overview,
            scheduler_chain_audit,
            manual_operations_enabled=manual_operation_mode,
        )
        _record_dashboard_build_step(build_profile, "scheduler_diagnostics", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "scheduler_diagnostics")

    long_term_started_at = time.perf_counter()
    step_started_at = time.perf_counter()
    long_term_maintenance_detail = {}
    if include_live_long_term_maintenance:
        long_term_maintenance_detail = build_long_term_maintenance_detail(scheduler_overview)
        _record_dashboard_build_step(build_profile, "long_term_maintenance_detail", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "long_term_maintenance_detail")

    step_started_at = time.perf_counter()
    if include_live_long_term_maintenance and merge_live_long_term_priority_blocked_summary is not None:
        long_term_maintenance_detail = merge_live_long_term_priority_blocked_summary(
            long_term_maintenance_detail
        )
        _record_dashboard_build_step(build_profile, "long_term_live_priority_merge", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "long_term_live_priority_merge")

    step_started_at = time.perf_counter()
    semantic_null_snapshot_audit_summary = {}
    if include_long_term_semantic_null_snapshot_audit:
        semantic_null_snapshot_audit_summary = (
            long_term_governance._build_semantic_null_snapshot_audit_summary()
        )
        _record_dashboard_build_step(
            build_profile,
            "long_term_semantic_null_snapshot_audit",
            step_started_at,
        )
    else:
        _record_dashboard_build_skip(build_profile, "long_term_semantic_null_snapshot_audit")

    step_started_at = time.perf_counter()
    long_term_governance_summary = build_long_term_governance_summary(
        long_term_snapshot,
        long_term_maintenance_detail,
        include_semantic_null_snapshot_audit=False,
        semantic_null_snapshot_audit_summary=semantic_null_snapshot_audit_summary,
    )
    long_term_governance_summary = _build_long_term_governance_runtime_scope(
        long_term_governance_summary,
        run_id=publish_run_id,
    )
    _record_dashboard_build_step(build_profile, "long_term_summary_reduce", step_started_at)
    _record_dashboard_build_step(build_profile, "long_term_governance", long_term_started_at)

    nav_risk_governance_summary = {}
    if include_database_diagnostics:
        step_started_at = time.perf_counter()
        nav_risk_governance_summary = _build_nav_risk_governance_summary(
            payload,
            publication,
            scheduler_overview_api,
            scheduler_chain_audit,
            risk_lag_summary,
            risk_sync_summary,
            risk_coverage_gap_summary,
        )
        _record_dashboard_build_step(build_profile, "nav_risk_governance", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "nav_risk_governance")

    publish_layer_semantics_summary = {}
    if include_portfolio_semantics:
        step_started_at = time.perf_counter()
        publish_layer_semantics_summary = build_publish_layer_semantics_summary(
            counts,
            publish_funnel_summary,
            portfolio_display_scope,
            portfolio_summary,
        )
        portfolio_display_scope = _build_portfolio_display_scope_runtime_scope(
            portfolio_display_scope,
            run_id=publish_run_id,
        )
        publish_layer_semantics_summary = _build_publish_layer_semantics_runtime_scope(
            publish_layer_semantics_summary,
            run_id=publish_run_id,
        )
        portfolio_group_brief = _build_portfolio_group_brief_runtime_scope(
            portfolio_group_brief,
            run_id=publish_run_id,
        )
        _record_dashboard_build_step(build_profile, "publish_layer_semantics", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "publish_layer_semantics")

    positive_ratio_semantics = {}
    if include_positive_ratio_semantics:
        step_started_at = time.perf_counter()
        positive_ratio_semantics = build_positive_ratio_semantics_summary(
            publish_summary,
            low_vol_positive_ratio_shadow_watch,
        )
        positive_ratio_semantics = _build_positive_ratio_semantics_runtime_scope(
            positive_ratio_semantics,
            run_id=publish_run_id,
        )
        _record_dashboard_build_step(build_profile, "positive_ratio_semantics", step_started_at)
    else:
        _record_dashboard_build_skip(build_profile, "positive_ratio_semantics")

    step_started_at = time.perf_counter()
    tracker_summary = _build_tracker_summary_runtime_scope(
        tracker_summary,
        run_id=publish_run_id,
    )
    publication_status_summary = _build_publication_status_runtime_scope(
        build_publication_status_summary(
            publication,
            tracker_summary=tracker_summary,
        ),
        run_id=publish_run_id,
    )
    report_timing_summary = _build_report_timing_runtime_scope(
        report_timing_summary,
        run_id=publish_run_id,
    )
    low_vol_positive_ratio_shadow_watch = _build_low_vol_positive_ratio_shadow_watch_runtime_scope(
        low_vol_positive_ratio_shadow_watch,
        run_id=publish_run_id,
    )
    automation_autonomy = _build_automation_autonomy_runtime_scope(
        automation_autonomy
    )
    database_source_audit = {}
    if include_database_source_audit:
        database_source_audit = build_database_source_audit(settings)
    _record_dashboard_build_step(build_profile, "runtime_scopes_and_publication", step_started_at)
    return {
        "settings": settings,
        "payload": payload,
        "publication": publication,
        "scheduler_state": scheduler_state,
        "counts": counts,
        "publish_summary": publish_summary,
        "tracker_summary": tracker_summary,
        "portfolio_summary": portfolio_summary,
        "portfolio_display_scope": portfolio_display_scope,
        "portfolio_group_brief": portfolio_group_brief,
        "artifacts": artifacts,
        "report_timing_summary": report_timing_summary,
        "low_vol_positive_ratio_shadow_watch": low_vol_positive_ratio_shadow_watch,
        "publish_reason_summary": publish_reason_summary,
        "effective_nav_snapshot": effective_nav_snapshot,
        "latest_screened_snapshot": latest_screened_snapshot,
        "database_overview": database_overview,
        "database_integrity": database_integrity,
        "database_source_audit": database_source_audit,
        "nav_lag_summary": nav_lag_summary,
        "risk_lag_summary": risk_lag_summary,
        "risk_sync_summary": risk_sync_summary,
        "risk_coverage_gap_summary": risk_coverage_gap_summary,
        "bond_universe_snapshot": bond_universe_snapshot,
        "long_term_snapshot": long_term_snapshot,
        "coverage_layers": coverage_layers,
        "publish_funnel_summary": publish_funnel_summary,
        "exclusion_summary": exclusion_summary,
        "scheduler_overview": scheduler_overview,
        "scheduler_overview_api": scheduler_overview_api,
        "publication_status_summary": publication_status_summary,
        "scheduler_path_governance": scheduler_path_governance,
        "scheduler_chain_audit": scheduler_chain_audit,
        "automation_autonomy": automation_autonomy,
        "long_term_governance_summary": long_term_governance_summary,
        "nav_risk_governance_summary": nav_risk_governance_summary,
        "publish_layer_semantics_summary": publish_layer_semantics_summary,
        "positive_ratio_semantics": positive_ratio_semantics,
        "dashboard_build_profile": _finish_dashboard_build_profile(
            build_profile,
            profile_started_at,
        ),
    }


def _build_compact_dashboard_counts(counts: dict[str, Any]) -> dict[str, int]:
    return {
        "final_count": int(counts.get("final_count") or 0),
        "internal_watch_count": int(counts.get("internal_watch_count") or 0),
        "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
        "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
        "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
        "dynamic_internal_watch_count": int(counts.get("dynamic_internal_watch_count") or 0),
        "manager_stability_watch_count": int(counts.get("manager_stability_watch_count") or 0),
        "publish_post_review_watch_count": int(counts.get("publish_post_review_watch_count") or 0),
        "project_universe_count": int(counts.get("project_universe_count") or 0),
        "excluded_count": int(counts.get("excluded_count") or 0),
        "annual_return_history_fund_count": int(counts.get("annual_return_history_fund_count") or 0),
        "rank_history_fund_count": int(counts.get("rank_history_fund_count") or 0),
        "neutralized_long_term_feature_fund_count": int(
            counts.get("neutralized_long_term_feature_fund_count") or 0
        ),
        "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
        "project_tracker_fund_count": int(counts.get("project_tracker_fund_count") or 0),
        "project_tracker_repeated_fund_count": int(
            counts.get("project_tracker_repeated_fund_count") or 0
        ),
        "project_tracker_current_active_count": int(
            counts.get("project_tracker_current_active_count") or 0
        ),
    }


def _reduce_automation_layers_for_compact(layers: dict[str, Any]) -> dict[str, Any]:
    payload = dict(layers or {})
    latest_database_snapshot = dict(payload.get("latest_database_snapshot") or {})
    latest_successful_run = dict(payload.get("latest_successful_run") or {})
    latest_publishable_run = dict(payload.get("latest_publishable_run") or {})
    current_published_run = dict(payload.get("current_published_run") or {})
    blockers = list(latest_successful_run.get("publish_blockers") or [])
    reduced_blockers = [
        {
            "code": _clean_text(item.get("code")),
            "label": _clean_text(item.get("label")),
            "detail": _clean_text(item.get("detail")),
            "severity": _clean_text(item.get("severity")),
        }
        for item in blockers[:3]
    ]
    return {
        "latest_database_snapshot": {
            "candidate_date": latest_database_snapshot.get("candidate_date"),
            "ready": bool(latest_database_snapshot.get("ready")),
            "readiness_reason": latest_database_snapshot.get("readiness_reason"),
        },
        "latest_successful_run": {
            "run_id": latest_successful_run.get("run_id"),
            "generated_at": latest_successful_run.get("generated_at"),
            "publishable": bool(latest_successful_run.get("publishable")),
            "publish_blockers": reduced_blockers,
            "replay_precision": {
                "status": ((latest_successful_run.get("replay_precision") or {}).get("status")),
                "status_label": ((latest_successful_run.get("replay_precision") or {}).get("status_label")),
                "exact_replay_inputs_available": bool(
                    ((latest_successful_run.get("replay_precision") or {}).get("exact_replay_inputs_available"))
                ),
                "publish_gate_exact_replay_available": bool(
                    ((latest_successful_run.get("replay_precision") or {}).get("publish_gate_exact_replay_available"))
                ),
                "final_layer_exact_replay_available": bool(
                    ((latest_successful_run.get("replay_precision") or {}).get("final_layer_exact_replay_available"))
                ),
            },
        },
        "latest_publishable_run": {
            "run_id": latest_publishable_run.get("run_id"),
            "generated_at": latest_publishable_run.get("generated_at"),
        },
        "current_published_run": {
            "run_id": current_published_run.get("run_id"),
            "generated_at": current_published_run.get("generated_at"),
        },
        "latest_successful_is_currently_published": bool(
            payload.get("latest_successful_is_currently_published")
        ),
    }


def _reduce_integrity_summary_for_compact(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    blockers = list(payload.get("blockers") or [])
    return {
        "aligned": bool(payload.get("aligned")),
        "check_count": int(payload.get("check_count") or 0),
        "blocker_count": int(payload.get("blocker_count") or 0),
        "blockers": [
            {
                "name": _clean_text(item.get("name")),
                "code": _clean_text(item.get("code")),
                "detail": _clean_text(item.get("detail")),
            }
            for item in blockers[:3]
        ],
    }


def _reduce_regression_smoke_summary_for_compact(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    group_results = list(payload.get("group_results") or [])
    return {
        "overall_status": payload.get("overall_status"),
        "status": payload.get("status"),
        "groups": list(payload.get("groups") or []),
        "passed_group_total": int(payload.get("passed_group_total") or 0),
        "failed_group_total": int(payload.get("failed_group_total") or 0),
        "issue_classification_status": payload.get("issue_classification_status"),
        "issue_classification_label": payload.get("issue_classification_label"),
        "issue_category_counts": dict(payload.get("issue_category_counts") or {}),
        "failed_command_total": int(payload.get("failed_command_total") or 0),
        "failed_group_classifications": [
            {
                "group": item.get("group"),
                "status": item.get("status"),
                "failed_command_total": item.get("failed_command_total"),
                "issue_classification_status": item.get("issue_classification_status"),
                "issue_classification_label": item.get("issue_classification_label"),
                "evidence_snippets": list(item.get("evidence_snippets") or [])[:3],
            }
            for item in list(payload.get("failed_group_classifications") or [])[:4]
        ],
        "expected_warning_total": int(payload.get("expected_warning_total") or 0),
        "test_expectation_mismatch_total": int(payload.get("test_expectation_mismatch_total") or 0),
        "production_degradation_total": int(payload.get("production_degradation_total") or 0),
        "environment_issue_total": int(payload.get("environment_issue_total") or 0),
        "unknown_failure_total": int(payload.get("unknown_failure_total") or 0),
        "summary_path": payload.get("summary_path"),
        "artifact_dir": payload.get("artifact_dir"),
        "source": payload.get("source"),
        "generated_at": payload.get("generated_at"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "group_results": [
            {
                "group": item.get("group"),
                "status": item.get("status"),
            }
            for item in group_results[:8]
        ],
    }


def _reduce_regression_smoke_task_for_compact(task: dict[str, Any]) -> dict[str, Any]:
    payload = dict(task or {})
    return {
        "status": payload.get("status"),
        "summary_path": payload.get("summary_path"),
        "artifact_dir": payload.get("artifact_dir"),
        "source": payload.get("source"),
        "generated_at": payload.get("generated_at"),
        "processed_total": int(payload.get("processed_total") or 0),
        "candidate_total": int(payload.get("candidate_total") or 0),
        "issue_classification_status": payload.get("issue_classification_status"),
        "issue_classification_label": payload.get("issue_classification_label"),
        "issue_category_counts": dict(payload.get("issue_category_counts") or {}),
        "failed_command_total": int(payload.get("failed_command_total") or 0),
        "failed_group_classifications": [
            {
                "group": item.get("group"),
                "status": item.get("status"),
                "failed_command_total": item.get("failed_command_total"),
                "issue_classification_status": item.get("issue_classification_status"),
                "issue_classification_label": item.get("issue_classification_label"),
                "evidence_snippets": list(item.get("evidence_snippets") or [])[:3],
            }
            for item in list(payload.get("failed_group_classifications") or [])[:4]
        ],
        "expected_warning_total": int(payload.get("expected_warning_total") or 0),
        "test_expectation_mismatch_total": int(payload.get("test_expectation_mismatch_total") or 0),
        "production_degradation_total": int(payload.get("production_degradation_total") or 0),
        "environment_issue_total": int(payload.get("environment_issue_total") or 0),
        "unknown_failure_total": int(payload.get("unknown_failure_total") or 0),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_scheduler_chain_audit_for_compact(audit: dict[str, Any]) -> dict[str, Any]:
    payload = dict(audit or {})
    weekly_run = ((payload.get("runs") or {}).get("weekly_maintenance") or {})
    open_issues = list(payload.get("open_issues") or [])
    return {
        "issue_count": int(payload.get("issue_count") or 0),
        "startup_catch_up_pending": bool(payload.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": list(payload.get("startup_catch_up_due_jobs") or []),
        "failed_screening_retry_cooldown": payload.get("failed_screening_retry_cooldown") or {},
        "screening_backlog_due": bool(payload.get("screening_backlog_due")),
        "screening_backlog_reason": payload.get("screening_backlog_reason"),
        "runs": {
            "weekly_maintenance": {
                "status": weekly_run.get("status"),
                "warning_reason": weekly_run.get("warning_reason"),
                "degraded_stage": weekly_run.get("degraded_stage"),
                "suppressed_historical_false_positive": bool(
                    weekly_run.get("suppressed_historical_false_positive")
                ),
            }
        },
        "open_issues": [
            {
                "label": item.get("label"),
                "code": item.get("code"),
                "severity": item.get("severity"),
                "detail": item.get("detail"),
            }
            for item in open_issues[:5]
        ],
    }


def _reduce_selection_exclusion_summary_for_compact(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "available": bool(payload.get("available")),
        "high_score_candidate_total": int(payload.get("high_score_candidate_total") or 0),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "annual_return_gate_shadow": payload.get("annual_return_gate_shadow") or {},
        "high_score_friction_gate_review": payload.get("high_score_friction_gate_review") or {},
        "risk_adjusted_metric_stack_review": payload.get("risk_adjusted_metric_stack_review") or {},
        "high_score_bucket_counts": [
            {
                "label": item.get("label"),
                "count": int(item.get("count") or 0),
            }
            for item in list(payload.get("high_score_bucket_counts") or [])[:4]
        ],
        "top_reasons": [
            {
                "reason": item.get("reason"),
                "count": int(item.get("count") or 0),
            }
            for item in list(payload.get("top_reasons") or [])[:6]
        ],
        "top_high_score_excluded": [
            {
                "fcode": item.get("fcode"),
                "fund_name": item.get("fund_name"),
                "weighted_score": item.get("weighted_score"),
                "bucket_label": item.get("bucket_label"),
                "excluded_reason": item.get("excluded_reason"),
            }
            for item in list(payload.get("top_high_score_excluded") or [])[:8]
        ],
    }


def _reduce_long_term_governance_summary_for_compact(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    governance_pool_summary = list(payload.get("governance_pool_summary") or [])

    def _compact_stale_scope_summary(raw_summary: dict[str, Any] | None) -> dict[str, Any]:
        source = dict(raw_summary or {})
        return {
            "total": _to_int(source.get("total")),
            "nav_bucket_schema_version": _to_int(source.get("nav_bucket_schema_version")),
            "counts_by_nav_bucket": dict(source.get("counts_by_nav_bucket") or {}),
            "counts_by_stale_mode": dict(source.get("counts_by_stale_mode") or {}),
            "counts_by_priority": dict(source.get("counts_by_priority") or {}),
            "fresh_recoverable_total": _to_int(source.get("fresh_recoverable_total")),
            "short_recoverable_total": _to_int(source.get("short_recoverable_total")),
            "recoverable_total": _to_int(source.get("recoverable_total")),
            "medium_watch_total": _to_int(source.get("medium_watch_total")),
            "deep_watch_total": _to_int(source.get("deep_watch_total")),
            "terminal_candidate_total": _to_int(source.get("terminal_candidate_total")),
            "deep_stale_candidate_total": _to_int(source.get("deep_stale_candidate_total")),
            "unknown_nav_staleness_total": _to_int(source.get("unknown_nav_staleness_total")),
            "recoverability_judgement": source.get("recoverability_judgement"),
        }

    def _sample_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "fcode": item.get("fcode"),
                "shortname": item.get("shortname"),
                "recommended_action": (
                    item.get("recommended_action")
                    or ("observe_upstream_source" if item.get("gap_shape") else None)
                ),
                "blocker_category": (
                    item.get("blocker_category")
                    or ("upstream_limited_annual_gap" if item.get("gap_shape") else None)
                ),
                "gap_shape": item.get("gap_shape"),
                "missing_years": list(item.get("missing_years") or []),
                "null_years": list(item.get("null_years") or []),
                "cooldown_last_error": item.get("cooldown_last_error"),
                "cooldown_next_retry_after": item.get("cooldown_next_retry_after"),
            }
            for item in rows[:8]
        ]

    return {
        "available": bool(payload),
        "headline": payload.get("headline"),
        "explanation": payload.get("explanation"),
        "blocking_reason": payload.get("blocking_reason"),
        "maintenance_status": payload.get("maintenance_status"),
        "maintenance_no_action_category": payload.get("maintenance_no_action_category"),
        "snapshot_scope_label": payload.get("snapshot_scope_label"),
        "snapshot_scope_note": payload.get("snapshot_scope_note"),
        "maintenance_scope_label": payload.get("maintenance_scope_label"),
        "maintenance_scope_note": payload.get("maintenance_scope_note"),
        "candidate_fund_count": int(payload.get("candidate_fund_count") or 0),
        "neutralized_fund_count": int(payload.get("neutralized_fund_count") or 0),
        "full_feature_ready_coverage_ratio": _to_float(
            payload.get("full_feature_ready_coverage_ratio")
        ),
        "annual_history_missing_total": int(payload.get("annual_history_missing_total") or 0),
        "rank_history_missing_total": int(payload.get("rank_history_missing_total") or 0),
        "await_history_maturity_count": int(payload.get("await_history_maturity_count") or 0),
        "annual_feature_pending_maturity_fund_count": int(
            payload.get("annual_feature_pending_maturity_fund_count") or 0
        ),
        "annual_feature_actionable_gap_fund_count": int(
            payload.get("annual_feature_actionable_gap_fund_count") or 0
        ),
        "annual_feature_actionable_missing_history_fund_count": int(
            payload.get("annual_feature_actionable_missing_history_fund_count") or 0
        ),
        "annual_feature_actionable_sparse_year_row_gap_fund_count": int(
            payload.get("annual_feature_actionable_sparse_year_row_gap_fund_count") or 0
        ),
        "annual_feature_actionable_null_year_gap_fund_count": int(
            payload.get("annual_feature_actionable_null_year_gap_fund_count") or 0
        ),
        "annual_feature_upstream_semantic_null_year_gap_fund_count": int(
            payload.get("annual_feature_upstream_semantic_null_year_gap_fund_count") or 0
        ),
        "annual_feature_upstream_limited_gap_fund_count": int(
            payload.get("annual_feature_upstream_limited_gap_fund_count") or 0
        ),
        "annual_feature_upstream_limited_missing_history_fund_count": int(
            payload.get("annual_feature_upstream_limited_missing_history_fund_count") or 0
        ),
        "annual_feature_upstream_limited_sparse_year_row_gap_fund_count": int(
            payload.get("annual_feature_upstream_limited_sparse_year_row_gap_fund_count") or 0
        ),
        "annual_feature_upstream_limited_null_year_gap_fund_count": int(
            payload.get("annual_feature_upstream_limited_null_year_gap_fund_count") or 0
        ),
        "annual_semantic_null_bridge_summary": payload.get("annual_semantic_null_bridge_summary") or {},
        "semantic_null_snapshot_audit_summary": payload.get("semantic_null_snapshot_audit_summary") or {},
        "annual_feature_pending_maturity_excluding_semantic_null_fund_count": int(
            payload.get("annual_feature_pending_maturity_excluding_semantic_null_fund_count") or 0
        ),
        "maintenance_gap_fund_count": int(payload.get("maintenance_gap_fund_count") or 0),
        "maintenance_candidate_total": int(payload.get("maintenance_candidate_total") or 0),
        "maintenance_screenable_candidate_total": int(
            payload.get("maintenance_screenable_candidate_total") or 0
        ),
        "maintenance_buyable_candidate_total": int(
            payload.get("maintenance_buyable_candidate_total") or 0
        ),
        "maintenance_scope_filtered_no_retry_total": int(
            payload.get("maintenance_scope_filtered_no_retry_total") or 0
        ),
        "maintenance_actionable_governance_pool_total": int(
            payload.get("maintenance_actionable_governance_pool_total") or 0
        ),
        "scope_filtered_no_retry_summary": payload.get("scope_filtered_no_retry_summary") or {},
        "annual_semantic_null_bridge_summary": payload.get("annual_semantic_null_bridge_summary") or {},
        "semantic_null_snapshot_audit_summary": payload.get("semantic_null_snapshot_audit_summary") or {},
        "upstream_limited_structure_summary": payload.get("upstream_limited_structure_summary") or {},
        "priority_scope_not_ready_summary": payload.get("priority_scope_not_ready_summary") or {},
        "stale_scope_not_ready_summary": _compact_stale_scope_summary(
            payload.get("stale_scope_not_ready_summary") or {}
        ),
        "buyable_active_stale_scope_recoverability_summary": (
            payload.get("buyable_active_stale_scope_recoverability_summary") or {}
        ),
        "local_retry_pool_count": int(payload.get("local_retry_pool_count") or 0),
        "local_retry_window_pending_count": int(payload.get("local_retry_window_pending_count") or 0),
        "local_retry_cooldown_pending_count": int(
            payload.get("local_retry_cooldown_pending_count") or 0
        ),
        "local_retry_ready_count": int(payload.get("local_retry_ready_count") or 0),
        "local_retry_other_pending_count": int(payload.get("local_retry_other_pending_count") or 0),
        "priority_blocked_total": int(payload.get("priority_blocked_total") or 0),
        "upstream_limited_watchlist_count": int(
            payload.get("upstream_limited_watchlist_count") or 0
        ),
        "upstream_limited_current_watchlist_total": int(
            payload.get("upstream_limited_current_watchlist_total") or 0
        ),
        "upstream_limited_non_watchlist_observe_total": int(
            payload.get("upstream_limited_non_watchlist_observe_total") or 0
        ),
        "upstream_limited_semantic_null_observe_total": int(
            payload.get("upstream_limited_semantic_null_observe_total") or 0
        ),
        "upstream_limited_non_watchlist_semantic_null_total": int(
            payload.get("upstream_limited_non_watchlist_semantic_null_total") or 0
        ),
        "upstream_limited_stale_no_progress_watchlist_total": int(
            payload.get("upstream_limited_stale_no_progress_watchlist_total") or 0
        ),
        "upstream_limited_empty_payload_watchlist_total": int(
            payload.get("upstream_limited_empty_payload_watchlist_total") or 0
        ),
        "upstream_limited_buyable_active_total": int(
            payload.get("upstream_limited_buyable_active_total") or 0
        ),
        "upstream_limited_publishable_total": int(
            payload.get("upstream_limited_publishable_total") or 0
        ),
        "upstream_limited_screenable_total": int(
            payload.get("upstream_limited_screenable_total") or 0
        ),
        "buyable_active_metadata_blocked_total": int(
            payload.get("buyable_active_metadata_blocked_total") or 0
        ),
        "buyable_active_scope_not_ready_total": int(
            payload.get("buyable_active_scope_not_ready_total") or 0
        ),
        "stale_scope_not_ready_total": int(payload.get("stale_scope_not_ready_total") or 0),
        "upstream_limited_empty_payload_total": int(
            payload.get("upstream_limited_empty_payload_total") or 0
        ),
        "terminal_upstream_limited_watchlist_count": int(
            payload.get("terminal_upstream_limited_watchlist_count") or 0
        ),
        "stale_upstream_limited_watchlist_count": int(
            payload.get("stale_upstream_limited_watchlist_count") or 0
        ),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "scope_filtered_upstream_limited_total": int(
            payload.get("scope_filtered_upstream_limited_total") or 0
        ),
        "governance_pool_summary": [
            {
                "pool_key": item.get("pool_key"),
                "label": item.get("label"),
                "count": int(item.get("count") or 0),
                "note": item.get("note"),
            }
            for item in governance_pool_summary
        ],
        "local_retry_samples": _sample_rows(list(payload.get("local_retry_samples") or [])),
        "priority_blocked_samples": _sample_rows(list(payload.get("priority_blocked_samples") or [])),
        "annual_feature_upstream_limited_gap_samples": _sample_rows(
            list(payload.get("annual_feature_upstream_limited_gap_samples") or [])
        ),
        "upstream_limited_watchlist_samples": _sample_rows(
            list(payload.get("upstream_limited_watchlist_samples") or [])
        ),
    }


def _reduce_long_term_governance_summary_for_detail(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    reduced = dict(payload)

    for key in (
        "annual_feature_pending_maturity_fund_count",
        "annual_feature_actionable_gap_fund_count",
        "annual_feature_actionable_missing_history_fund_count",
        "annual_feature_actionable_sparse_year_row_gap_fund_count",
        "annual_feature_actionable_null_year_gap_fund_count",
        "annual_feature_upstream_semantic_null_year_gap_fund_count",
        "annual_feature_upstream_limited_gap_fund_count",
        "annual_feature_upstream_limited_missing_history_fund_count",
        "annual_feature_upstream_limited_sparse_year_row_gap_fund_count",
        "annual_feature_upstream_limited_null_year_gap_fund_count",
        "annual_feature_pending_maturity_excluding_semantic_null_fund_count",
        "maintenance_gap_fund_count",
        "maintenance_candidate_total",
        "maintenance_screenable_candidate_total",
        "maintenance_buyable_candidate_total",
        "maintenance_scope_filtered_no_retry_total",
        "maintenance_actionable_governance_pool_total",
        "local_retry_pool_count",
        "local_retry_window_pending_count",
        "local_retry_cooldown_pending_count",
        "local_retry_ready_count",
        "local_retry_other_pending_count",
        "upstream_limited_current_watchlist_total",
        "upstream_limited_non_watchlist_observe_total",
        "upstream_limited_semantic_null_observe_total",
        "upstream_limited_non_watchlist_semantic_null_total",
        "upstream_limited_stale_no_progress_watchlist_total",
        "upstream_limited_empty_payload_watchlist_total",
        "upstream_limited_buyable_active_total",
        "upstream_limited_publishable_total",
        "upstream_limited_screenable_total",
    ):
        reduced[key] = _to_int(payload.get(key))

    def _sample_rows(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
        return [
            {
                "fcode": item.get("fcode"),
                "shortname": item.get("shortname"),
                "recommended_action": (
                    item.get("recommended_action")
                    or ("observe_upstream_source" if item.get("gap_shape") else None)
                ),
                "blocker_category": (
                    item.get("blocker_category")
                    or ("upstream_limited_annual_gap" if item.get("gap_shape") else None)
                ),
                "gap_shape": item.get("gap_shape"),
                "missing_years": list(item.get("missing_years") or []),
                "null_years": list(item.get("null_years") or []),
                "cooldown_last_error": item.get("cooldown_last_error"),
                "cooldown_next_retry_after": item.get("cooldown_next_retry_after"),
            }
            for item in rows[:limit]
        ]

    coverage_progress = dict(payload.get("coverage_progress") or {})
    coverage_before = dict(coverage_progress.get("before") or {})
    coverage_after = dict(coverage_progress.get("after") or {})
    reduced["coverage_progress"] = {
        "annual_history_ready_delta": _to_int(coverage_progress.get("annual_history_ready_delta")),
        "rank_history_ready_delta": _to_int(coverage_progress.get("rank_history_ready_delta")),
        "before": {
            "annual_history_ready_ratio": _to_float(coverage_before.get("annual_history_ready_ratio")),
            "rank_history_ready_ratio": _to_float(coverage_before.get("rank_history_ready_ratio")),
            "buyable_candidate_total": _to_int(coverage_before.get("buyable_candidate_total")),
            "annual_history_ready_total": _to_int(coverage_before.get("annual_history_ready_total")),
            "rank_history_ready_total": _to_int(coverage_before.get("rank_history_ready_total")),
        },
        "after": {
            "annual_history_ready_ratio": _to_float(coverage_after.get("annual_history_ready_ratio")),
            "rank_history_ready_ratio": _to_float(coverage_after.get("rank_history_ready_ratio")),
            "buyable_candidate_total": _to_int(coverage_after.get("buyable_candidate_total")),
            "annual_history_ready_total": _to_int(coverage_after.get("annual_history_ready_total")),
            "rank_history_ready_total": _to_int(coverage_after.get("rank_history_ready_total")),
        },
    }

    upstream_observation_summary = dict(payload.get("upstream_observation_summary") or {})
    reduced["upstream_observation_summary"] = {
        "pending_recovery_total": _to_int(upstream_observation_summary.get("pending_recovery_total")),
        "pending_retry_window_total": _to_int(
            upstream_observation_summary.get("pending_retry_window_total")
        ),
        "pending_retry_cooldown_total": _to_int(
            upstream_observation_summary.get("pending_retry_cooldown_total")
        ),
        "pending_retry_ready_total": _to_int(
            upstream_observation_summary.get("pending_retry_ready_total")
        ),
        "pending_next_retry_after_min": upstream_observation_summary.get(
            "pending_next_retry_after_min"
        ),
        "pending_next_retry_after_max": upstream_observation_summary.get(
            "pending_next_retry_after_max"
        ),
        "upstream_limited_watchlist_total": _to_int(
            upstream_observation_summary.get("upstream_limited_watchlist_total")
        ),
        "stale_upstream_limited_watchlist_total": _to_int(
            upstream_observation_summary.get("stale_upstream_limited_watchlist_total")
        ),
        "terminal_upstream_limited_watchlist_total": _to_int(
            upstream_observation_summary.get("terminal_upstream_limited_watchlist_total")
        ),
        "scope_filtered_upstream_limited_total": _to_int(
            upstream_observation_summary.get("scope_filtered_upstream_limited_total")
        ),
        "pending_reason_counts": dict(upstream_observation_summary.get("pending_reason_counts") or {}),
        "upstream_limited_reason_counts": dict(
            upstream_observation_summary.get("upstream_limited_reason_counts") or {}
        ),
        "stale_upstream_limited_reason_counts": dict(
            upstream_observation_summary.get("stale_upstream_limited_reason_counts") or {}
        ),
        "terminal_upstream_limited_reason_counts": dict(
            upstream_observation_summary.get("terminal_upstream_limited_reason_counts") or {}
        ),
        "scope_filtered_upstream_limited_reason_counts": dict(
            upstream_observation_summary.get("scope_filtered_upstream_limited_reason_counts") or {}
        ),
        "pending_subtype_counts": dict(upstream_observation_summary.get("pending_subtype_counts") or {}),
        "upstream_limited_subtype_counts": dict(
            upstream_observation_summary.get("upstream_limited_subtype_counts") or {}
        ),
        "stale_upstream_limited_subtype_counts": dict(
            upstream_observation_summary.get("stale_upstream_limited_subtype_counts") or {}
        ),
        "terminal_upstream_limited_subtype_counts": dict(
            upstream_observation_summary.get("terminal_upstream_limited_subtype_counts") or {}
        ),
        "scope_filtered_upstream_limited_subtype_counts": dict(
            upstream_observation_summary.get("scope_filtered_upstream_limited_subtype_counts") or {}
        ),
    }
    reduced["upstream_limited_structure_summary"] = dict(
        payload.get("upstream_limited_structure_summary") or {}
    )
    reduced["annual_semantic_null_bridge_summary"] = dict(
        payload.get("annual_semantic_null_bridge_summary") or {}
    )
    reduced["semantic_null_snapshot_audit_summary"] = dict(
        payload.get("semantic_null_snapshot_audit_summary") or {}
    )

    priority_blocked_summary = dict(payload.get("priority_blocked_summary") or {})
    counts_by_bucket = dict(priority_blocked_summary.get("counts_by_bucket") or {})
    reduced["priority_blocked_summary"] = {
        "total": _to_int(priority_blocked_summary.get("total")),
        "total_raw": _to_int(priority_blocked_summary.get("total_raw")),
        "total_excluding_stale_scope_not_ready": _to_int(
            priority_blocked_summary.get("total_excluding_stale_scope_not_ready")
        ),
        "buyable_active_metadata_blocked_total": _to_int(
            priority_blocked_summary.get("buyable_active_metadata_blocked_total")
        ),
        "buyable_active_scope_not_ready_total": _to_int(
            priority_blocked_summary.get("buyable_active_scope_not_ready_total")
        ),
        "other_metadata_blocked_total": _to_int(
            priority_blocked_summary.get("other_metadata_blocked_total")
        ),
        "other_scope_not_ready_total": _to_int(
            priority_blocked_summary.get("other_scope_not_ready_total")
        ),
        "scope_not_ready_total": _to_int(priority_blocked_summary.get("scope_not_ready_total")),
        "stale_scope_not_ready_total": _to_int(
            priority_blocked_summary.get("stale_scope_not_ready_total")
        ),
        "high_priority_scope_not_ready_total": _to_int(
            priority_blocked_summary.get("high_priority_scope_not_ready_total")
        ),
        "counts_by_bucket": {
            "buyable_active_metadata_blocked": _to_int(
                counts_by_bucket.get("buyable_active_metadata_blocked")
            ),
            "buyable_active_scope_not_ready": _to_int(
                counts_by_bucket.get("buyable_active_scope_not_ready")
            ),
            "other_metadata_blocked": _to_int(counts_by_bucket.get("other_metadata_blocked")),
            "other_scope_not_ready": _to_int(counts_by_bucket.get("other_scope_not_ready")),
        },
    }
    scope_filtered_no_retry_summary = dict(payload.get("scope_filtered_no_retry_summary") or {})
    reduced["scope_filtered_no_retry_summary"] = {
        "total": _to_int(scope_filtered_no_retry_summary.get("total")),
        "deferred_priority_total": _to_int(scope_filtered_no_retry_summary.get("deferred_priority_total")),
        "screenable_priority_total": _to_int(scope_filtered_no_retry_summary.get("screenable_priority_total")),
        "buyable_active_priority_total": _to_int(
            scope_filtered_no_retry_summary.get("buyable_active_priority_total")
        ),
        "active_priority_total": _to_int(scope_filtered_no_retry_summary.get("active_priority_total")),
        "counts_by_priority": dict(scope_filtered_no_retry_summary.get("counts_by_priority") or {}),
        "counts_by_source": dict(scope_filtered_no_retry_summary.get("counts_by_source") or {}),
        "counts_by_blocker": dict(scope_filtered_no_retry_summary.get("counts_by_blocker") or {}),
        "counts_by_maturity_reason": dict(
            scope_filtered_no_retry_summary.get("counts_by_maturity_reason") or {}
        ),
        "structure_counts": dict(scope_filtered_no_retry_summary.get("structure_counts") or {}),
        "counts_by_stale_mode": dict(scope_filtered_no_retry_summary.get("counts_by_stale_mode") or {}),
        "holding_period_blocked_total": _to_int(
            scope_filtered_no_retry_summary.get("holding_period_blocked_total")
        ),
        "runtime_excluded_total": _to_int(
            scope_filtered_no_retry_summary.get("runtime_excluded_total")
        ),
        "buyable_holding_period_blocked_total": _to_int(
            scope_filtered_no_retry_summary.get("buyable_holding_period_blocked_total")
        ),
        "non_buyable_holding_period_blocked_total": _to_int(
            scope_filtered_no_retry_summary.get("non_buyable_holding_period_blocked_total")
        ),
        "stale_rank_only_total": _to_int(scope_filtered_no_retry_summary.get("stale_rank_only_total")),
        "upstream_empty_payload_total": _to_int(
            scope_filtered_no_retry_summary.get("upstream_empty_payload_total")
        ),
        "upstream_semantic_null_year_total": _to_int(
            scope_filtered_no_retry_summary.get("upstream_semantic_null_year_total")
        ),
    }
    priority_scope_not_ready_summary = dict(payload.get("priority_scope_not_ready_summary") or {})
    reduced["priority_scope_not_ready_summary"] = {
        "scope_not_ready_total": _to_int(priority_scope_not_ready_summary.get("scope_not_ready_total")),
        "stale_scope_not_ready_total": _to_int(
            priority_scope_not_ready_summary.get("stale_scope_not_ready_total")
        ),
        "buyable_active_scope_not_ready_total": _to_int(
            priority_scope_not_ready_summary.get("buyable_active_scope_not_ready_total")
        ),
        "other_scope_not_ready_total": _to_int(
            priority_scope_not_ready_summary.get("other_scope_not_ready_total")
        ),
        "screenable_priority_total": _to_int(
            priority_scope_not_ready_summary.get("screenable_priority_total")
        ),
        "buyable_active_priority_total": _to_int(
            priority_scope_not_ready_summary.get("buyable_active_priority_total")
        ),
        "active_priority_total": _to_int(priority_scope_not_ready_summary.get("active_priority_total")),
        "deferred_priority_total": _to_int(
            priority_scope_not_ready_summary.get("deferred_priority_total")
        ),
        "buyable_active_scope_not_ready_stale_total": _to_int(
            priority_scope_not_ready_summary.get("buyable_active_scope_not_ready_stale_total")
        ),
        "other_scope_not_ready_stale_total": _to_int(
            priority_scope_not_ready_summary.get("other_scope_not_ready_stale_total")
        ),
    }
    buyable_active_stale_scope_recoverability_summary = dict(
        payload.get("buyable_active_stale_scope_recoverability_summary") or {}
    )
    buyable_fresh_recoverable_total = _to_int(
        buyable_active_stale_scope_recoverability_summary.get("fresh_recoverable_total")
    )
    buyable_short_recoverable_total = _to_int(
        buyable_active_stale_scope_recoverability_summary.get("short_recoverable_total")
    )
    buyable_recoverable_total = _to_int(
        buyable_active_stale_scope_recoverability_summary.get("recoverable_total")
    )
    if buyable_recoverable_total <= 0 and (
        buyable_fresh_recoverable_total > 0 or buyable_short_recoverable_total > 0
    ):
        buyable_recoverable_total = (
            buyable_fresh_recoverable_total + buyable_short_recoverable_total
        )
    reduced["buyable_active_stale_scope_recoverability_summary"] = {
        "total": _to_int(buyable_active_stale_scope_recoverability_summary.get("total")),
        "counts_by_nav_bucket": dict(
            buyable_active_stale_scope_recoverability_summary.get("counts_by_nav_bucket") or {}
        ),
        "counts_by_stale_mode": dict(
            buyable_active_stale_scope_recoverability_summary.get("counts_by_stale_mode") or {}
        ),
        "counts_by_maturity_reason": dict(
            buyable_active_stale_scope_recoverability_summary.get("counts_by_maturity_reason") or {}
        ),
        "short_recoverable_total": buyable_short_recoverable_total,
        "fresh_recoverable_total": buyable_fresh_recoverable_total,
        "recoverable_total": buyable_recoverable_total,
        "medium_watch_total": _to_int(
            buyable_active_stale_scope_recoverability_summary.get("medium_watch_total")
        ),
        "deep_watch_total": _to_int(
            buyable_active_stale_scope_recoverability_summary.get("deep_watch_total")
        ),
        "terminal_candidate_total": _to_int(
            buyable_active_stale_scope_recoverability_summary.get("terminal_candidate_total")
        ),
        "unknown_nav_staleness_total": _to_int(
            buyable_active_stale_scope_recoverability_summary.get("unknown_nav_staleness_total")
        ),
        "recoverability_judgement": (
            buyable_active_stale_scope_recoverability_summary.get("recoverability_judgement")
        ),
    }

    failed_reason_governance_summary = dict(payload.get("failed_reason_governance_summary") or {})
    reduced["failed_reason_governance_summary"] = {
        "action_totals": {
            str(key): _to_int(value)
            for key, value in dict(failed_reason_governance_summary.get("action_totals") or {}).items()
        },
        "stage_count": len(dict(failed_reason_governance_summary.get("stage_reason_actions") or {})),
    }

    reduced["local_retry_samples"] = _sample_rows(list(payload.get("local_retry_samples") or []), limit=4)
    reduced["priority_blocked_samples"] = _sample_rows(
        list(payload.get("priority_blocked_samples") or []),
        limit=4,
    )
    reduced["annual_feature_upstream_limited_gap_samples"] = _sample_rows(
        list(payload.get("annual_feature_upstream_limited_gap_samples") or []),
        limit=8,
    )
    reduced["stale_scope_not_ready_samples"] = _sample_rows(
        list(payload.get("stale_scope_not_ready_samples") or []),
        limit=4,
    )
    reduced["buyable_active_stale_scope_samples"] = _sample_rows(
        list(payload.get("buyable_active_stale_scope_samples") or []),
        limit=8,
    )
    reduced["scope_filtered_no_retry_samples"] = _sample_rows(
        list(payload.get("scope_filtered_no_retry_samples") or []),
        limit=8,
    )
    reduced["upstream_limited_watchlist_samples"] = _sample_rows(
        list(payload.get("upstream_limited_watchlist_samples") or []),
        limit=4,
    )
    reduced["run_id"] = payload.get("run_id")
    reduced["latest_canonical_run_id"] = payload.get("latest_canonical_run_id")
    reduced["replay_precision_status"] = payload.get("replay_precision_status")
    reduced["replay_precision_status_label"] = payload.get("replay_precision_status_label")
    reduced["auxiliary_scope"] = payload.get("auxiliary_scope")
    reduced["auxiliary_scope_label"] = payload.get("auxiliary_scope_label")
    reduced["scope_detail"] = payload.get("scope_detail")
    return reduced


def _reduce_database_overview_for_overview(overview: dict[str, Any]) -> dict[str, Any]:
    payload = dict(overview or {})
    return {
        "database_file_updated_at": payload.get("database_file_updated_at"),
        "latest_nav_business_date": payload.get("latest_nav_business_date"),
        "risk_1y_business_date": payload.get("risk_1y_business_date"),
        "risk_1y_raw_business_date": payload.get("risk_1y_raw_business_date"),
        "nav_data_updated_at": payload.get("nav_data_updated_at"),
        "risk_data_updated_at": payload.get("risk_data_updated_at"),
        "nav_history_record_count": int(payload.get("nav_history_record_count") or 0),
        "nav_history_hot_record_count": int(payload.get("nav_history_hot_record_count") or 0),
        "nav_history_archive_record_count": int(payload.get("nav_history_archive_record_count") or 0),
        "nav_history_effective_record_count": int(
            payload.get("nav_history_effective_record_count")
            or payload.get("nav_history_record_count")
            or 0
        ),
        "nav_history_archive_bound": bool(payload.get("nav_history_archive_bound")),
        "nav_history_archive_cutoff_date": payload.get("nav_history_archive_cutoff_date"),
        "effective_nav_fund_count": int(payload.get("effective_nav_fund_count") or 0),
        "risk_1y_coverage_ratio_vs_candidate": _to_float(
            payload.get("risk_1y_coverage_ratio_vs_candidate")
        ),
        "risk_1y_publish_minimum_coverage_ratio": _to_float(
            payload.get("risk_1y_publish_minimum_coverage_ratio")
        ),
        "risk_1y_observation_coverage_ratio": _to_float(
            payload.get("risk_1y_observation_coverage_ratio")
        ),
        "risk_1y_coverage_gate_status": payload.get("risk_1y_coverage_gate_status"),
        "risk_1y_coverage_gate_label": payload.get("risk_1y_coverage_gate_label"),
        "risk_1y_coverage_gate_explanation": payload.get("risk_1y_coverage_gate_explanation"),
        "risk_1y_coverage_observation_line_met": bool(
            payload.get("risk_1y_coverage_observation_line_met")
        ),
        "risk_coverage_semantics": {
            "coverage_ratio_vs_candidate": _to_float(payload.get("risk_1y_coverage_ratio_vs_candidate")),
            "publish_minimum_coverage_ratio": _to_float(
                payload.get("risk_1y_publish_minimum_coverage_ratio")
            ),
            "observation_coverage_ratio": _to_float(
                payload.get("risk_1y_observation_coverage_ratio")
            ),
            "coverage_gate_status": payload.get("risk_1y_coverage_gate_status"),
            "coverage_gate_label": payload.get("risk_1y_coverage_gate_label"),
            "coverage_gate_explanation": payload.get("risk_1y_coverage_gate_explanation"),
            "coverage_observation_line_met": bool(
                payload.get("risk_1y_coverage_observation_line_met")
            ),
        },
        "database_freshness_summary": payload.get("database_freshness_summary") or {},
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_database_integrity_for_overview(integrity: dict[str, Any]) -> dict[str, Any]:
    payload = dict(integrity or {})
    return {
        "candidate_total": int(payload.get("candidate_total") or 0),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_report_timing_for_overview(summary: dict[str, Any]) -> dict[str, Any]:
    payload = _build_report_timing_runtime_scope(summary)
    stages = dict(payload.get("stages") or {})
    return {
        "available": bool(payload.get("available")),
        "generated_at": payload.get("generated_at"),
        "total_seconds": _to_float(payload.get("total_seconds")),
        "stages": {
            "materialize_summary_payload": stages.get("materialize_summary_payload"),
        },
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_scheduler_snapshot_for_overview(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot or {})
    return {
        "candidate_date": payload.get("candidate_date"),
        "candidate_fund_count": int(payload.get("candidate_fund_count") or 0),
        "ready": payload.get("ready"),
        "readiness_reason": payload.get("readiness_reason"),
    }


def _reduce_scheduler_run_for_overview(run: dict[str, Any]) -> dict[str, Any]:
    payload = dict(run or {})
    return {
        "task_name": payload.get("task_name"),
        "screening_run_id": payload.get("screening_run_id"),
        "task_run_audit_json_project_path": payload.get("task_run_audit_json_project_path"),
        "status": payload.get("status"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        "screening_triggered": bool(payload.get("screening_triggered")),
        "maintenance_rescreen_triggered": bool(payload.get("maintenance_rescreen_triggered")),
        "screening_reason": payload.get("screening_reason"),
        "skip_reason": payload.get("skip_reason"),
        "warning_reason": payload.get("warning_reason"),
        "action_category": payload.get("action_category"),
        "no_action_category": payload.get("no_action_category"),
    }


def _reduce_scheduler_job_for_overview(job: dict[str, Any]) -> dict[str, Any]:
    payload = dict(job or {})
    return {
        "id": payload.get("id"),
        "next_run_time": payload.get("next_run_time"),
    }


def _reduce_scheduler_child_task_digest_for_overview(digest: dict[str, Any]) -> dict[str, Any]:
    payload = dict(digest or {})
    return {
        "items": [
            {
                "task_key": item.get("task_key"),
                "task_label": item.get("task_label"),
                "attention_needed": bool(item.get("attention_needed")),
                "changed_total": optional_count(item.get("changed_total")),
                "processed_total": optional_count(item.get("processed_total")),
                "candidate_total": optional_count(item.get("candidate_total")),
                "activity_observed": item.get("activity_observed"),
                "skip_reason": item.get("skip_reason"),
                "no_action_category": item.get("no_action_category"),
            }
            for item in list(payload.get("items") or [])[:8]
        ]
    }


def _reduce_automation_closure_digest_for_overview(digest: dict[str, Any]) -> dict[str, Any]:
    payload = dict(digest or {})
    return {
        "summary": payload.get("summary") or {},
        "alerts": list(payload.get("alerts") or [])[:4],
        "ordered_tasks": [
            {
                "task_key": item.get("task_key"),
                "task_label": item.get("task_label"),
                "status": item.get("status"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "attention_needed": bool(item.get("attention_needed")),
                "action_category": item.get("action_category"),
                "no_action_category": item.get("no_action_category"),
                "screening_triggered": bool(item.get("screening_triggered")),
                "maintenance_rescreen_triggered": bool(item.get("maintenance_rescreen_triggered")),
                "screening_run_id": item.get("screening_run_id"),
                "maintenance_rescreen_run_id": item.get("maintenance_rescreen_run_id"),
                "report_refresh_triggered": bool(item.get("report_refresh_triggered")),
                "report_refresh_run_id": item.get("report_refresh_run_id"),
                "screening_reason": item.get("screening_reason"),
                "skip_reason": item.get("skip_reason"),
                "warning_reason": item.get("warning_reason"),
                "snapshot_changed_during_task": bool(item.get("snapshot_changed_during_task")),
                "snapshot_advanced_without_screening": bool(item.get("snapshot_advanced_without_screening")),
                "effective_snapshot_ahead_of_screened": bool(item.get("effective_snapshot_ahead_of_screened")),
                "maintenance_change_items": list(item.get("maintenance_change_items") or [])[:4],
                "child_task_digest": _reduce_scheduler_child_task_digest_for_overview(
                    item.get("child_task_digest") or {}
                ),
                "task_log_file_project_path": item.get("task_log_file_project_path"),
                "task_run_audit_json_project_path": item.get("task_run_audit_json_project_path"),
            }
            for item in list(payload.get("ordered_tasks") or [])[:4]
        ],
    }


def _reduce_maintenance_summary_for_overview(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "metrics_source": deepcopy(payload.get("metrics_source") or {}),
        "nav_processed_total": optional_count(payload.get("nav_processed_total")),
        "nav_attempted_total": optional_count(payload.get("nav_attempted_total")),
        "nav_recovered_total": optional_count(payload.get("nav_recovered_total")),
        "nav_records_inserted": optional_count(payload.get("nav_records_inserted")),
        "nav_failed_total": optional_count(payload.get("nav_failed_total")),
        "long_term_processed_total": optional_count(payload.get("long_term_processed_total")),
        "long_term_changed_total": optional_count(payload.get("long_term_changed_total")),
        "long_term_annual_success_total": optional_count(payload.get("long_term_annual_success_total")),
        "long_term_rank_success_total": optional_count(payload.get("long_term_rank_success_total")),
        "regression_smoke_summary": _reduce_regression_smoke_summary_for_compact(
            payload.get("regression_smoke_summary") or {}
        ),
    }


def _reduce_weekly_run_for_overview(run: dict[str, Any]) -> dict[str, Any]:
    payload = _reduce_scheduler_run_for_overview(run)
    payload["task_history_id"] = (run or {}).get("task_history_id")
    payload["maintenance_metrics_source"] = deepcopy((run or {}).get("maintenance_metrics_source") or {})
    child_tasks = dict((run or {}).get("child_tasks") or {})
    if "regression_smoke" in child_tasks:
        payload["child_tasks"] = {
            "regression_smoke": _reduce_regression_smoke_task_for_compact(
                child_tasks.get("regression_smoke") or {}
            )
        }
    return payload


def _reduce_scheduler_overview_for_overview(overview: dict[str, Any]) -> dict[str, Any]:
    payload = dict(overview or {})
    maintenance_summary = _reduce_maintenance_summary_for_overview(
        payload.get("maintenance_summary") or {}
    )
    return {
        "publication_dependency": deepcopy(payload.get("publication_dependency")),
        "enabled": bool(payload.get("enabled")),
        "scheduler_running": bool(payload.get("scheduler_running")),
        "automation_host_mode": payload.get("automation_host_mode"),
        "started_at": payload.get("started_at"),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "last_error": payload.get("last_error"),
        "screening_backlog_due": bool(payload.get("screening_backlog_due")),
        "screening_backlog_reason": payload.get("screening_backlog_reason"),
        "failed_screening_retry_cooldown": payload.get(
            "failed_screening_retry_cooldown"
        )
        or {},
        "startup_catch_up_pending": bool(payload.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": list(payload.get("startup_catch_up_due_jobs") or []),
        "effective_nav_snapshot": _reduce_scheduler_snapshot_for_overview(
            payload.get("effective_nav_snapshot") or {}
        ),
        "latest_screened_snapshot": _reduce_scheduler_snapshot_for_overview(
            payload.get("latest_screened_snapshot") or {}
        ),
        "nav_freshness": payload.get("nav_freshness") or {},
        "current_task": payload.get("current_task") or {},
        "daily_job": _reduce_scheduler_job_for_overview(payload.get("daily_job") or {}),
        "intraday_job": _reduce_scheduler_job_for_overview(payload.get("intraday_job") or {}),
        "weekly_job": _reduce_scheduler_job_for_overview(payload.get("weekly_job") or {}),
        "daily_run": _reduce_scheduler_run_for_overview(payload.get("daily_run") or {}),
        "intraday_run": _reduce_scheduler_run_for_overview(payload.get("intraday_run") or {}),
        "weekly_run": _reduce_weekly_run_for_overview(payload.get("weekly_run") or {}),
        "maintenance_summary": maintenance_summary,
        "automation_closure_digest": _reduce_automation_closure_digest_for_overview(
            payload.get("automation_closure_digest") or {}
        ),
    }


def _reduce_publish_funnel_for_overview(summary: dict[str, Any]) -> dict[str, Any]:
    payload = _build_publish_funnel_runtime_scope(summary)
    return {
        "screenable_universe_count": int(payload.get("screenable_universe_count") or 0),
        "governance_candidate_pool_count": int(payload.get("governance_candidate_pool_count") or 0),
        "official_publish_count": int(payload.get("official_publish_count") or 0),
        "dynamic_reference_count": int(payload.get("dynamic_reference_count") or 0),
        "dynamic_reference_export_count": int(payload.get("dynamic_reference_export_count") or 0),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_tracker_summary_for_overview(summary: dict[str, Any]) -> dict[str, Any]:
    payload = _build_tracker_summary_runtime_scope(summary)
    membership_summary = dict(payload.get("latest_membership_change_summary") or {})
    membership_summary.pop("rolling_change_stability_summary", None)
    return {
        "current_active_count": int(payload.get("current_active_count") or 0),
        "latest_nav_snapshot_date": payload.get("latest_nav_snapshot_date"),
        "latest_run_id": payload.get("latest_run_id"),
        "latest_run_is_new_snapshot": bool(payload.get("latest_run_is_new_snapshot", True)),
        "latest_transition_counts": payload.get("latest_transition_counts") or [],
        "latest_changes": payload.get("latest_changes"),
        "latest_membership_change_summary": membership_summary,
        "narrative_summary": payload.get("narrative_summary") or {},
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_low_vol_positive_ratio_shadow_watch_for_compact(
    watch: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(watch or {})
    overlap_summary = dict(payload.get("low_vol_overlap_summary") or {})
    semantics = dict(payload.get("semantics") or {})
    return {
        "available": bool(payload.get("available")),
        "headline": payload.get("headline"),
        "explanation": payload.get("explanation"),
        "focus_sample_count": int(payload.get("focus_sample_count") or 0),
        "positive_ratio_gate_hit_count": int(payload.get("positive_ratio_gate_hit_count") or 0),
        "positive_ratio_gap_alert_count": int(payload.get("positive_ratio_gap_alert_count") or 0),
        "monthly_shadow_ready_count": int(payload.get("monthly_shadow_ready_count") or 0),
        "style_summary": [
            {
                "债基风格标签": item.get("债基风格标签"),
                "自然月正收益差>=15pct样本数": item.get("自然月正收益差>=15pct样本数"),
            }
            for item in list(payload.get("style_summary") or [])[:3]
        ],
        "style_gap_reason_summary": [
            {
                "债基风格标签": item.get("债基风格标签"),
                "主伴随原因": item.get("主伴随原因"),
            }
            for item in list(payload.get("style_gap_reason_summary") or [])[:3]
        ],
        "gap_companion_reason_summary": [
            {
                "原因标签": item.get("原因标签"),
                "样本数": item.get("样本数"),
            }
            for item in list(payload.get("gap_companion_reason_summary") or [])[:3]
        ],
        "low_vol_overlap_summary": {
            "available": bool(overlap_summary.get("available")),
            "status": overlap_summary.get("status"),
            "run_id": overlap_summary.get("run_id"),
            "source_revision": overlap_summary.get("source_revision"),
            "report_binding": dict(overlap_summary.get("report_binding") or {}),
            "scenario_coverage": dict(overlap_summary.get("scenario_coverage") or {}),
            "audit_definition_sha256": overlap_summary.get("audit_definition_sha256"),
            "strategy_effectiveness_verified": overlap_summary.get("strategy_effectiveness_verified"),
            "annual_positive_targeted_count": overlap_summary.get("annual_positive_targeted_count"),
            "annual_positive_direct_final_impact_count": overlap_summary.get("annual_positive_direct_final_impact_count"),
            "annual_without_positive_targeted_count": overlap_summary.get("annual_without_positive_targeted_count"),
            "annual_without_positive_direct_final_impact_count": overlap_summary.get("annual_without_positive_direct_final_impact_count"),
            "positive_without_annual_targeted_count": overlap_summary.get("positive_without_annual_targeted_count"),
            "positive_without_annual_direct_final_impact_count": overlap_summary.get("positive_without_annual_direct_final_impact_count"),
            "recommendation": list(overlap_summary.get("recommendation") or [])[:3],
        },
        "semantics": {
            "definition": semantics.get("definition"),
            "industry_standard": semantics.get("industry_standard"),
        },
        "preview": [
            {
                "基金代码": item.get("基金代码"),
                "基金名称": item.get("基金名称"),
                "债基风格标签": item.get("债基风格标签"),
                "正式投资评分": item.get("正式投资评分"),
                "1年-正收益比率": item.get("1年-正收益比率"),
                "positive_month_ratio_calendar_pct": item.get("positive_month_ratio_calendar_pct"),
                "positive_ratio_daily_vs_calendar_gap_pct": item.get(
                    "positive_ratio_daily_vs_calendar_gap_pct"
                ),
                "dynamic_threshold_reason_codes": item.get("dynamic_threshold_reason_codes"),
            }
            for item in list(payload.get("preview") or [])[:4]
        ],
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_portfolio_suggestions_for_detail(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    groups = []
    for item in list(payload.get("groups") or []):
        group = dict(item or {})
        groups.append(
            {
                "name": group.get("name"),
                "description": group.get("description"),
                "count": int(group.get("count") or len(group.get("funds") or [])),
                "downgrade_info": group.get("downgrade_info") or {},
            }
        )
    return {
        "available": bool(payload.get("available")),
        "source_candidate_count": int(payload.get("source_candidate_count") or 0),
        "candidate_count": int(payload.get("candidate_count") or 0),
        "target_count": int(payload.get("target_count") or 0),
        "fund_count": int(payload.get("fund_count") or 0),
        "group_count": int(payload.get("group_count") or 0),
        "company_cap": int(payload.get("company_cap") or 0),
        "share_class_unique_count": int(payload.get("share_class_unique_count") or 0),
        "selection_policy": payload.get("selection_policy"),
        "quality_gate": payload.get("quality_gate") or {},
        "core_quality_gate": payload.get("core_quality_gate") or {},
        "target_policy": payload.get("target_policy") or {},
        "compression_audit": payload.get("compression_audit") or {},
        "groups": groups,
    }


def _reduce_tracker_summary_for_scoped_pages(summary: dict[str, Any]) -> dict[str, Any]:
    payload = _build_tracker_summary_runtime_scope(summary)
    return {
        "current_active_count": int(payload.get("current_active_count") or 0),
        "latest_nav_snapshot_date": payload.get("latest_nav_snapshot_date"),
        "latest_run_id": payload.get("latest_run_id"),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_stage_reason_item(item: dict[str, Any]) -> dict[str, Any]:
    payload = dict(item or {})
    return {
        "code": payload.get("code"),
        "reason": payload.get("reason"),
        "display_reason": payload.get("display_reason"),
        "count": int(payload.get("count") or 0),
    }


def _reduce_publish_stage_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    return {
        "stage_key": payload.get("stage_key"),
        "stage_label": payload.get("stage_label"),
        "scope": payload.get("scope"),
        "input_count": int(payload.get("input_count") or 0),
        "passed_count": int(payload.get("passed_count") or 0),
        "removed_count": int(payload.get("removed_count") or 0),
        "removed_rate_pct": _to_float(payload.get("removed_rate_pct")),
        "lead_reason": _reduce_stage_reason_item(payload.get("lead_reason") or {}),
        "bottleneck_reason": _reduce_stage_reason_item(payload.get("bottleneck_reason") or {}),
        "top_reasons": [
            _reduce_stage_reason_item(item)
            for item in list(payload.get("top_reasons") or [])[:2]
        ],
        "note": payload.get("note"),
        "explanation": payload.get("explanation"),
    }


def _build_strategy_shadow_truth_contract(
    *,
    shadow_key: str,
    official_count_field: str = "counts.final_count",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "shadow_key": str(shadow_key or "").strip(),
        "truth_scope": "strategy_shadow_observation",
        "publish_truth": False,
        "official_count_field": official_count_field,
        "shadow_count_field": "shadow_final_count",
        "business_value_synthesis": "forbidden",
        "threshold_change_policy": "shadow_evidence_only_requires_exact_replay_and_followup_review",
        "direct_threshold_relaxation_recommended_now": False,
        "missing_value_policy": "preserve_missing_semantics",
        "source_policy": "run_local_audit_artifact_or_report_embedded_shadow_only",
    }


def _with_annual_return_shadow_truth_contract(
    annual_return_gate_shadow: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(annual_return_gate_shadow or {})
    if not payload:
        return {}
    existing_contract = (
        payload.get("truth_contract")
        if isinstance(payload.get("truth_contract"), dict)
        else {}
    )
    payload["truth_contract"] = {
        **_build_strategy_shadow_truth_contract(
            shadow_key="annual_return_gate_shadow",
        ),
        **existing_contract,
    }
    if payload.get("report_binding"):
        payload["truth_contract"]["source_policy"] = "exact_report_revision_and_sha256_bound_audit"
    decision_summary = (
        payload.get("decision_summary")
        if isinstance(payload.get("decision_summary"), dict)
        else {}
    )
    if decision_summary:
        payload["truth_contract"]["decision_recommended_action"] = decision_summary.get(
            "recommended_action"
        )
    return payload


def _reduce_publish_reason_summary_for_scoped_pages(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    annual_return_gate_shadow = _with_annual_return_shadow_truth_contract(
        dict(payload.get("annual_return_gate_shadow") or {})
    )
    annual_return_gate_shadow_decision_summary = dict(
        annual_return_gate_shadow.get("decision_summary") or {}
    )
    return {
        "available": bool(payload.get("available")),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "headline_highlights": list(payload.get("headline_highlights") or [])[:4],
        "narrative_summary": {
            "overview": ((payload.get("narrative_summary") or {}).get("overview")),
            "main_chain": ((payload.get("narrative_summary") or {}).get("main_chain")),
        },
        "responsibility_chain_headline": payload.get("responsibility_chain_headline"),
        "primary_bottleneck": _reduce_publish_stage_row(payload.get("primary_bottleneck") or {}),
        "main_chain": [
            _reduce_publish_stage_row(item)
            for item in list(payload.get("main_chain") or [])[:6]
        ],
        "audit_branch": [
            _reduce_publish_stage_row(item)
            for item in list(payload.get("audit_branch") or [])[:4]
        ],
        "focus_metric_summary": [
            {
                "metric_key": item.get("metric_key"),
                "metric_label": item.get("metric_label") or item.get("label"),
                "intercepted_count": int(item.get("intercepted_count") or 0),
                "count_basis": item.get("count_basis"),
                "stage_labels": list(item.get("stage_labels") or [])[:3],
                "local_boundary_review_recommended_now": bool(
                    item.get("local_boundary_review_recommended_now")
                ),
                "local_boundary_review_scope": item.get("local_boundary_review_scope"),
                "high_priority_local_review_style_total": int(
                    item.get("high_priority_local_review_style_total") or 0
                ),
                "dominant_style_band_label": item.get("dominant_style_band_label"),
                "dominant_style_band_count": int(item.get("dominant_style_band_count") or 0),
                "dominant_style_band_share_pct": item.get("dominant_style_band_share_pct"),
                "dominant_local_boundary_shadow_review_recommended_now": bool(
                    item.get("dominant_local_boundary_shadow_review_recommended_now")
                ),
                "dominant_local_boundary_sample_count": int(
                    item.get("dominant_local_boundary_sample_count") or 0
                ),
                "dominant_local_boundary_weighted_margin_median": item.get(
                    "dominant_local_boundary_weighted_margin_median"
                ),
                "dominant_local_boundary_annual_gap_median_pct": item.get(
                    "dominant_local_boundary_annual_gap_median_pct"
                ),
                "direct_relieved_final_count": int(item.get("direct_relieved_final_count") or 0),
                "shadow_added_count": int(item.get("shadow_added_count") or 0),
                "shadow_removed_count": int(item.get("shadow_removed_count") or 0),
                "net_final_count_delta": int(item.get("net_final_count_delta") or 0),
                "local_style_band_shadow_recommended_action": item.get(
                    "local_style_band_shadow_recommended_action"
                ),
                "local_style_band_shadow_direct_relief_supported_scenario_total": int(
                    item.get("local_style_band_shadow_direct_relief_supported_scenario_total") or 0
                ),
                "local_style_band_shadow_selection_side_effect_only_scenario_total": int(
                    item.get("local_style_band_shadow_selection_side_effect_only_scenario_total") or 0
                ),
                "top_local_shadow_scenarios": [
                    dict(scenario or {})
                    for scenario in list(item.get("top_local_shadow_scenarios") or [])[:3]
                ],
            }
            for item in list(payload.get("focus_metric_summary") or [])[:8]
        ],
        "annual_return_score_sharpe_coupling": payload.get("annual_return_score_sharpe_coupling")
        or {},
        "annual_return_gate_shadow": {
            **annual_return_gate_shadow,
            "decision_summary": {
                **annual_return_gate_shadow_decision_summary,
                "top_local_shadow_scenarios": [
                    dict(scenario or {})
                    for scenario in list(
                        annual_return_gate_shadow_decision_summary.get(
                            "top_local_shadow_scenarios"
                        )
                        or []
                    )[:3]
                ],
                "top_local_shadow_scenario": dict(
                    annual_return_gate_shadow_decision_summary.get(
                        "top_local_shadow_scenario"
                    )
                    or {}
                ),
            },
        },
        "score_cluster_explainability": payload.get("score_cluster_explainability") or {},
        "high_score_friction_gate_review": payload.get("high_score_friction_gate_review") or {},
        "risk_adjusted_metric_stack_review": payload.get("risk_adjusted_metric_stack_review") or {},
    }


def _reduce_publish_stage_audit_for_task_summary(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "headline_highlights": list(payload.get("headline_highlights") or [])[:4],
        "main_chain_stage_keys": list(payload.get("main_chain_stage_keys") or [])[:6],
        "audit_stage_keys": list(payload.get("audit_stage_keys") or [])[:6],
        "stage_rows": [
            _reduce_publish_stage_row(item)
            for item in list(payload.get("stage_rows") or [])[:8]
        ],
    }


def _reduce_selection_exclusion_summary_for_scoped_pages(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    annual_return_gate_shadow = _with_annual_return_shadow_truth_contract(
        dict(payload.get("annual_return_gate_shadow") or {})
    )
    annual_return_gate_shadow_decision_summary = dict(
        annual_return_gate_shadow.get("decision_summary") or {}
    )
    return {
        "available": bool(payload.get("available")),
        "high_score_candidate_total": int(payload.get("high_score_candidate_total") or 0),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "high_score_friction_count": int(payload.get("high_score_friction_count") or 0),
        "top_reasons": [
            {
                "reason": item.get("reason"),
                "count": int(item.get("count") or 0),
            }
            for item in list(payload.get("top_reasons") or [])[:3]
        ],
        "reason_group_preview": list(payload.get("reason_group_preview") or [])[:3],
        "quality_factor_group_preview": list(payload.get("quality_factor_group_preview") or [])[:3],
        "quality_factor_detail_preview": list(payload.get("quality_factor_detail_preview") or [])[:4],
        "quality_overlap_combo_preview": list(payload.get("quality_overlap_combo_preview") or [])[:3],
        "annual_return_score_sharpe_coupling": payload.get("annual_return_score_sharpe_coupling") or {},
        "annual_return_gate_shadow": {
            **annual_return_gate_shadow,
            "decision_summary": {
                **annual_return_gate_shadow_decision_summary,
                "top_local_shadow_scenarios": [
                    dict(scenario or {})
                    for scenario in list(
                        annual_return_gate_shadow_decision_summary.get(
                            "top_local_shadow_scenarios"
                        )
                        or []
                    )[:3]
                ],
                "top_local_shadow_scenario": dict(
                    annual_return_gate_shadow_decision_summary.get(
                        "top_local_shadow_scenario"
                    )
                    or {}
                ),
            },
        },
        "score_cluster_explainability": payload.get("score_cluster_explainability") or {},
        "high_score_friction_gate_review": payload.get("high_score_friction_gate_review") or {},
        "risk_adjusted_metric_stack_review": payload.get("risk_adjusted_metric_stack_review") or {},
        "high_score_friction_reason_preview": list(
            payload.get("high_score_friction_reason_preview") or []
        )[:3],
        "status_split_preview": list(payload.get("status_split_preview") or [])[:4],
    }


def _reduce_low_vol_positive_ratio_shadow_watch_for_reports_scope(
    watch: dict[str, Any],
) -> dict[str, Any]:
    payload = _build_low_vol_positive_ratio_shadow_watch_runtime_scope(watch)
    return {
        "available": bool(payload.get("available")),
        "headline": payload.get("headline"),
        "explanation": payload.get("explanation"),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
    }


def _reduce_database_freshness_for_diagnostic_detail(
    freshness: dict[str, Any],
) -> dict[str, Any]:
    payload = _build_database_freshness_runtime_scope(freshness)
    effective_nav_snapshot = dict(payload.get("effective_nav_snapshot") or {})
    risk_1y_freshness = dict(payload.get("risk_1y_freshness") or {})
    long_term_history_snapshot = dict(payload.get("long_term_history_snapshot") or {})
    return {
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "effective_nav_snapshot": {
            "candidate_date": effective_nav_snapshot.get("candidate_date"),
            "ready": effective_nav_snapshot.get("ready"),
            "readiness_reason": effective_nav_snapshot.get("readiness_reason"),
            "effective_date": effective_nav_snapshot.get("effective_date"),
            "latest_nav_date": effective_nav_snapshot.get("latest_nav_date"),
            "candidate_fund_count": _to_int(effective_nav_snapshot.get("candidate_fund_count")),
        },
        "risk_1y_freshness": {
            "candidate_date": risk_1y_freshness.get("candidate_date"),
            "risk_1y_effective_end_date": risk_1y_freshness.get("risk_1y_effective_end_date"),
            "dates_aligned": risk_1y_freshness.get("dates_aligned"),
            "coverage_sufficient": risk_1y_freshness.get("coverage_sufficient"),
            "coverage_ratio_vs_candidate": _to_float(
                risk_1y_freshness.get("coverage_ratio_vs_candidate")
            ),
            "minimum_coverage_ratio": _to_float(
                risk_1y_freshness.get("minimum_coverage_ratio")
            ),
            "publish_minimum_coverage_ratio": _to_float(
                risk_1y_freshness.get("publish_minimum_coverage_ratio")
            ),
            "observation_coverage_ratio": _to_float(
                risk_1y_freshness.get("observation_coverage_ratio")
            ),
            "coverage_observation_line_met": bool(
                risk_1y_freshness.get("coverage_observation_line_met")
            ),
            "coverage_gate_status": risk_1y_freshness.get("coverage_gate_status"),
            "coverage_gate_label": risk_1y_freshness.get("coverage_gate_label"),
            "coverage_gate_explanation": risk_1y_freshness.get("coverage_gate_explanation"),
        },
        "long_term_history_snapshot": {
            "candidate_fund_count": _to_int(long_term_history_snapshot.get("candidate_fund_count")),
            "annual_return_history_fund_count": _to_int(
                long_term_history_snapshot.get("annual_return_history_fund_count")
            ),
            "rank_history_fund_count": _to_int(
                long_term_history_snapshot.get("rank_history_fund_count")
            ),
            "annual_feature_stale_fund_count": _to_int(
                long_term_history_snapshot.get("annual_feature_stale_fund_count")
            ),
            "rank_feature_stale_fund_count": _to_int(
                long_term_history_snapshot.get("rank_feature_stale_fund_count")
            ),
            "full_feature_ready_fund_count": _to_int(
                long_term_history_snapshot.get("full_feature_ready_fund_count")
            ),
            "full_feature_gap_fund_count": _to_int(
                long_term_history_snapshot.get("full_feature_gap_fund_count")
            ),
            "neutralized_fund_count": _to_int(long_term_history_snapshot.get("neutralized_fund_count")),
            "blocking_reason": long_term_history_snapshot.get("blocking_reason"),
            "full_feature_ready_coverage_ratio": _to_float(
                long_term_history_snapshot.get("full_feature_ready_coverage_ratio")
            ),
        },
    }


def _reduce_risk_sync_summary_for_diagnostic_detail(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "latest_generated_at": payload.get("latest_generated_at"),
        "latest_artifact_path": payload.get("latest_artifact_path"),
        "configured_periods": list(payload.get("configured_periods") or []),
        "period_coverage_grace_days": _to_int(payload.get("period_coverage_grace_days")),
        "changed_nav_candidate_total": _to_int(payload.get("changed_nav_candidate_total")),
        "actionable_candidate_total": _to_int(payload.get("actionable_candidate_total")),
        "blocked_by_returns_lag_total": _to_int(payload.get("blocked_by_returns_lag_total")),
        "blocked_by_nav_history_total": _to_int(payload.get("blocked_by_nav_history_total")),
        "blocked_by_nav_history_lag_total": _to_int(
            payload.get("blocked_by_nav_history_lag_total")
        ),
        "period_eligible_gap_candidate_total": _to_int(
            payload.get("period_eligible_gap_candidate_total")
        ),
        "ftype_counts": dict(payload.get("ftype_counts") or {}),
        "nav_span_bucket_counts": dict(payload.get("nav_span_bucket_counts") or {}),
        "period_gap_counts": dict(payload.get("period_gap_counts") or {}),
        "blocked_by_nav_history_ftype_counts": dict(
            payload.get("blocked_by_nav_history_ftype_counts") or {}
        ),
    }


def _reduce_forward_validation_summary_for_diagnostic_detail(
    summary: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "available": bool(payload.get("available")),
        "generated_at": payload.get("generated_at"),
        "latest_snapshot_date": payload.get("latest_snapshot_date"),
        "latest_snapshot_run_id": payload.get("latest_snapshot_run_id"),
        "latest_final_count": _to_int(payload.get("latest_final_count")),
        "latest_stable_final_count": _to_int(payload.get("latest_stable_final_count")),
        "latest_enhanced_final_count": _to_int(payload.get("latest_enhanced_final_count")),
        "snapshot_total": _to_int(payload.get("snapshot_total")),
        "status_label": payload.get("status_label"),
        "blocking_reason": payload.get("blocking_reason"),
        "candidate_pool_definition": payload.get("candidate_pool_definition"),
        "reused_from_generated_at": payload.get("reused_from_generated_at"),
        "reused_from_run_id": payload.get("reused_from_run_id"),
        "incremental_review": payload.get("incremental_review") or {},
        "window_summaries": list(payload.get("window_summaries") or [])[:3],
    }


def _reduce_publish_reason_summary_for_diagnostic_detail(
    summary: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(summary or {})
    annual_return_gate_shadow = _with_annual_return_shadow_truth_contract(
        dict(payload.get("annual_return_gate_shadow") or {})
    )
    annual_return_gate_shadow_decision_summary = dict(
        annual_return_gate_shadow.get("decision_summary") or {}
    )
    return {
        "available": bool(payload.get("available")),
        "run_id": payload.get("run_id"),
        "latest_canonical_run_id": payload.get("latest_canonical_run_id"),
        "replay_precision_status": payload.get("replay_precision_status"),
        "replay_precision_status_label": payload.get("replay_precision_status_label"),
        "auxiliary_scope": payload.get("auxiliary_scope"),
        "auxiliary_scope_label": payload.get("auxiliary_scope_label"),
        "scope_detail": payload.get("scope_detail"),
        "headline_highlights": list(payload.get("headline_highlights") or [])[:4],
        "narrative_summary": {
            "overview": ((payload.get("narrative_summary") or {}).get("overview")),
            "main_chain": ((payload.get("narrative_summary") or {}).get("main_chain")),
        },
        "responsibility_chain_headline": payload.get("responsibility_chain_headline"),
        "primary_bottleneck": _reduce_publish_stage_row(payload.get("primary_bottleneck") or {}),
        "main_chain": [
            _reduce_publish_stage_row(item)
            for item in list(payload.get("main_chain") or [])[:6]
        ],
        "audit_branch": [
            _reduce_publish_stage_row(item)
            for item in list(payload.get("audit_branch") or [])[:4]
        ],
        "stage_family_summary": [
            dict(item or {})
            for item in list(payload.get("stage_family_summary") or [])[:6]
        ],
        "focus_metric_summary": [
            {
                "metric_key": item.get("metric_key"),
                "metric_label": item.get("metric_label") or item.get("label"),
                "intercepted_count": int(item.get("intercepted_count") or 0),
                "count_basis": item.get("count_basis"),
                "stage_labels": list(item.get("stage_labels") or [])[:3],
                "suspected_redundancy_present": bool(item.get("suspected_redundancy_present")),
                "weighted_anchor_coverage_ratio": item.get("weighted_anchor_coverage_ratio"),
                "short_without_weighted_total": int(item.get("short_without_weighted_total") or 0),
                "long_without_weighted_total": int(item.get("long_without_weighted_total") or 0),
                "annual_return_coupled_share_pct": item.get("annual_return_coupled_share_pct"),
                "score_requires_annual_return_share_pct": item.get(
                    "score_requires_annual_return_share_pct"
                ),
                "sharpe_requires_annual_return_share_pct": item.get(
                    "sharpe_requires_annual_return_share_pct"
                ),
                "annual_return_independent_hit_total": int(
                    item.get("annual_return_independent_hit_total") or 0
                ),
                "local_boundary_review_recommended_now": bool(
                    item.get("local_boundary_review_recommended_now")
                ),
                "local_boundary_review_scope": item.get("local_boundary_review_scope"),
                "high_priority_local_review_style_total": int(
                    item.get("high_priority_local_review_style_total") or 0
                ),
                "dominant_style_band_label": item.get("dominant_style_band_label"),
                "dominant_style_band_count": int(item.get("dominant_style_band_count") or 0),
                "dominant_style_band_share_pct": item.get("dominant_style_band_share_pct"),
                "dominant_local_boundary_shadow_review_recommended_now": bool(
                    item.get("dominant_local_boundary_shadow_review_recommended_now")
                ),
                "dominant_local_boundary_sample_count": int(
                    item.get("dominant_local_boundary_sample_count") or 0
                ),
                "dominant_local_boundary_weighted_margin_median": item.get(
                    "dominant_local_boundary_weighted_margin_median"
                ),
                "dominant_local_boundary_annual_gap_median_pct": item.get(
                    "dominant_local_boundary_annual_gap_median_pct"
                ),
                "direct_relieved_final_count": int(item.get("direct_relieved_final_count") or 0),
                "shadow_added_count": int(item.get("shadow_added_count") or 0),
                "shadow_removed_count": int(item.get("shadow_removed_count") or 0),
                "net_final_count_delta": int(item.get("net_final_count_delta") or 0),
                "portfolio_direct_relieved_count": int(
                    item.get("portfolio_direct_relieved_count") or 0
                ),
                "annual_only_score_ge_80_count": int(item.get("annual_only_score_ge_80_count") or 0),
                "local_style_band_shadow_recommended_action": item.get(
                    "local_style_band_shadow_recommended_action"
                ),
                "local_style_band_shadow_direct_relief_supported_scenario_total": int(
                    item.get("local_style_band_shadow_direct_relief_supported_scenario_total") or 0
                ),
                "local_style_band_shadow_selection_side_effect_only_scenario_total": int(
                    item.get("local_style_band_shadow_selection_side_effect_only_scenario_total") or 0
                ),
                "top_local_shadow_scenarios": [
                    dict(scenario or {})
                    for scenario in list(item.get("top_local_shadow_scenarios") or [])[:3]
                ],
                "actionable_threshold_review_total": int(
                    item.get("actionable_threshold_review_total") or 0
                ),
                "pending_data_verify_total": int(
                    item.get("pending_data_verify_total") or 0
                ),
                "dominant_friction_gate": item.get("dominant_friction_gate"),
                "high_overlap_pair_total": int(item.get("high_overlap_pair_total") or 0),
                "sharpe_review_priority": item.get("sharpe_review_priority"),
                "positive_ratio_strategy_materiality_status": item.get(
                    "positive_ratio_strategy_materiality_status"
                ),
                "dominant_pair": item.get("dominant_pair"),
                "dominant_pair_overlap_ratio": item.get("dominant_pair_overlap_ratio"),
            }
            for item in list(payload.get("focus_metric_summary") or [])[:10]
        ],
        "annual_return_score_sharpe_coupling": payload.get("annual_return_score_sharpe_coupling")
        or {},
        "annual_return_gate_shadow": {
            **annual_return_gate_shadow,
            "decision_summary": {
                **annual_return_gate_shadow_decision_summary,
                "top_local_shadow_scenarios": [
                    dict(scenario or {})
                    for scenario in list(
                        annual_return_gate_shadow_decision_summary.get(
                            "top_local_shadow_scenarios"
                        )
                        or []
                    )[:3]
                ],
                "top_local_shadow_scenario": dict(
                    annual_return_gate_shadow_decision_summary.get(
                        "top_local_shadow_scenario"
                    )
                    or {}
                ),
            },
        },
        "score_cluster_explainability": payload.get("score_cluster_explainability") or {},
        "high_score_friction_gate_review": payload.get("high_score_friction_gate_review") or {},
        "risk_adjusted_metric_stack_review": payload.get("risk_adjusted_metric_stack_review") or {},
    }


def _reduce_scheduler_chain_audit_for_diagnostic_detail(
    summary: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(summary or {})
    return {
        "aligned": bool(payload.get("aligned")),
        "issue_count": _to_int(payload.get("issue_count")),
        "screening_backlog_due": bool(payload.get("screening_backlog_due")),
        "screening_backlog_reason": payload.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(payload.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": list(payload.get("startup_catch_up_due_jobs") or [])[:4],
        "schedule_stability_summary": payload.get("schedule_stability_summary") or {},
        "open_issues": [
            {
                "label": item.get("label"),
                "code": item.get("code"),
                "severity": item.get("severity"),
                "detail": item.get("detail"),
            }
            for item in list(payload.get("open_issues") or [])[:4]
        ],
    }


def _build_latest_dashboard_full_audit_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    artifacts = context["artifacts"]
    counts = context["counts"]
    tracker_file = artifacts.get("project_tracker_excel")
    normalized_run_id = _clean_text(payload.get("run_id"))
    database_freshness = _build_database_freshness_runtime_scope(
        payload.get("database_freshness") or {},
        run_id=normalized_run_id,
    )
    coverage_summary = _build_coverage_summary(
        bond_universe_snapshot=context["bond_universe_snapshot"],
        long_term_snapshot=context["long_term_snapshot"],
        counts=counts,
        run_id=normalized_run_id,
    )
    coverage_layers = _build_coverage_layers_runtime_scope(
        context["coverage_layers"],
        run_id=normalized_run_id,
    )
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        context["publish_funnel_summary"],
        run_id=normalized_run_id,
    )
    publication_summary = _build_publication_summary_for_response(
        publication,
        tracker_summary=context["publish_summary"].get("project_tracker_summary") or {},
        publication_status_summary=context["publication_status_summary"],
    )
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "compact": False,
        "detail_scope": "audit_full",
        "dashboard_build_profile": context["dashboard_build_profile"],
        "scope_contract": _build_dashboard_scope_contract("audit_full"),
        "data_sources": payload.get("data_sources") or {},
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "internal_watch_count": int(counts.get("internal_watch_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "dynamic_internal_watch_count": int(counts.get("dynamic_internal_watch_count") or 0),
            "manager_stability_watch_count": int(counts.get("manager_stability_watch_count") or 0),
            "publish_post_review_watch_count": int(counts.get("publish_post_review_watch_count") or 0),
            "project_universe_count": int(counts.get("project_universe_count") or 0),
            "excluded_count": int(counts.get("excluded_count") or 0),
            "annual_return_history_fund_count": int(counts.get("annual_return_history_fund_count") or 0),
            "rank_history_fund_count": int(counts.get("rank_history_fund_count") or 0),
            "neutralized_long_term_feature_fund_count": int(counts.get("neutralized_long_term_feature_fund_count") or 0),
            "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
            "project_tracker_fund_count": int(counts.get("project_tracker_fund_count") or 0),
            "project_tracker_repeated_fund_count": int(counts.get("project_tracker_repeated_fund_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
        },
        "tracker_summary": context["tracker_summary"],
        "portfolio_suggestions": _reduce_portfolio_suggestions_for_detail(
            context["portfolio_summary"]
        ),
        "portfolio_display_scope": context["portfolio_display_scope"],
        "portfolio_group_brief": context["portfolio_group_brief"],
        "tracker_file": tracker_file,
        "tracker_meta_file": artifacts.get("project_tracker_excel_meta"),
        "filtered_excel": artifacts.get("filtered_excel"),
        "filtered_csv": artifacts.get("filtered_csv"),
        "dynamic_filtered_excel": artifacts.get("dynamic_filtered_excel"),
        "dynamic_filtered_csv": artifacts.get("dynamic_filtered_csv"),
        "watch_excel": artifacts.get("watch_excel"),
        "watch_csv": artifacts.get("watch_csv"),
        "dynamic_watch_excel": artifacts.get("dynamic_watch_excel"),
        "dynamic_watch_csv": artifacts.get("dynamic_watch_csv"),
        "excluded_excel": artifacts.get("excluded_excel"),
        "excluded_csv": artifacts.get("excluded_csv"),
        "report_json": artifacts.get("report_json"),
        "report_markdown": artifacts.get("report_markdown"),
        "database_freshness": database_freshness,
        "database_overview": context["database_overview"],
        "database_source_audit": context["database_source_audit"],
        "database_integrity": context["database_integrity"],
        "nav_lag_summary": context["nav_lag_summary"],
        "risk_lag_summary": context["risk_lag_summary"],
        "risk_sync_summary": context["risk_sync_summary"],
        "risk_coverage_gap_summary": context["risk_coverage_gap_summary"],
        "bond_universe_snapshot": context["bond_universe_snapshot"],
        "coverage_layers": coverage_layers,
        "coverage_summary": coverage_summary,
        "long_term_governance_summary": _reduce_long_term_governance_summary_for_detail(
            context["long_term_governance_summary"]
        ),
        "nav_risk_governance_summary": context["nav_risk_governance_summary"],
        "positive_ratio_semantics": context["positive_ratio_semantics"],
        "publish_funnel_summary": publish_funnel_summary,
        "publish_layer_semantics_summary": context["publish_layer_semantics_summary"],
        "publish_reason_summary": context["publish_reason_summary"],
        "selection_exclusion_summary": context["exclusion_summary"],
        "automation_layers": publication.get("automation_layers") or {},
        "publication_integrity": publication.get("publication_integrity") or {},
        "artifact_consistency": ((payload.get("internal_audit") or {}).get("artifact_consistency") or {}),
        "publication_summary": publication_summary,
        "publication_status_summary": context["publication_status_summary"],
        "report_timing_summary": context["report_timing_summary"],
        "scheduler_overview": context["scheduler_overview_api"],
        "maintenance_summary": (context["scheduler_overview_api"].get("maintenance_summary") or {}),
        "weekly_run": (context["scheduler_overview_api"].get("weekly_run") or {}),
        "scheduler_path_governance": context["scheduler_path_governance"],
        "scheduler_chain_audit": context["scheduler_chain_audit"],
        "automation_autonomy": context["automation_autonomy"],
        "forward_validation_summary": (payload.get("project_publish_summary") or {}).get("forward_validation_summary") or {},
        "publish_stage_audit": (context["publish_summary"].get("publish_stage_audit") or {}),
        "low_vol_positive_ratio_shadow_watch": context["low_vol_positive_ratio_shadow_watch"],
        "formal_group_distribution": context["publish_summary"].get("formal_group_distribution") or [],
        "stable_style_distribution": context["publish_summary"].get("stable_official_style_distribution") or [],
        "enhanced_style_distribution": context["publish_summary"].get("enhanced_official_style_distribution") or [],
        "style_distribution": context["publish_summary"].get("official_style_distribution") or [],
        "company_distribution": context["publish_summary"].get("official_company_distribution") or [],
        "artifacts": {
            "filtered_excel": artifacts.get("filtered_excel"),
            "filtered_csv": artifacts.get("filtered_csv"),
            "dynamic_filtered_excel": artifacts.get("dynamic_filtered_excel"),
            "dynamic_filtered_csv": artifacts.get("dynamic_filtered_csv"),
            "dynamic_filtered_excel_meta": artifacts.get("dynamic_filtered_excel_meta"),
            "project_tracker_excel": artifacts.get("project_tracker_excel"),
            "project_tracker_excel_meta": artifacts.get("project_tracker_excel_meta"),
            "watch_excel": artifacts.get("watch_excel"),
            "watch_csv": artifacts.get("watch_csv"),
            "watch_excel_meta": artifacts.get("watch_excel_meta"),
            "dynamic_watch_excel": artifacts.get("dynamic_watch_excel"),
            "dynamic_watch_csv": artifacts.get("dynamic_watch_csv"),
            "dynamic_watch_excel_meta": artifacts.get("dynamic_watch_excel_meta"),
            "excluded_excel": artifacts.get("excluded_excel"),
            "excluded_csv": artifacts.get("excluded_csv"),
            "excluded_excel_meta": artifacts.get("excluded_excel_meta"),
            "report_markdown": artifacts.get("report_markdown"),
            "report_json": artifacts.get("report_json"),
            "forward_validation_excel": artifacts.get("forward_validation_excel"),
            "forward_validation_json": artifacts.get("forward_validation_json"),
            "forward_validation_markdown": artifacts.get("forward_validation_markdown"),
        },
        "gate_rules": (payload.get("project_publish_summary") or {}).get("gate_rules") or [],
    }


def _build_latest_dashboard_diagnostic_detail_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    counts = context["counts"]
    artifacts = context["artifacts"]
    scheduler_overview_api = context["scheduler_overview_api"]
    normalized_run_id = _clean_text(payload.get("run_id"))
    database_freshness = _build_database_freshness_runtime_scope(
        payload.get("database_freshness") or {},
        run_id=normalized_run_id,
    )
    coverage_summary = _build_coverage_summary(
        bond_universe_snapshot=context["bond_universe_snapshot"],
        long_term_snapshot=context["long_term_snapshot"],
        counts=counts,
        run_id=normalized_run_id,
    )
    coverage_layers = _build_coverage_layers_runtime_scope(
        context["coverage_layers"],
        run_id=normalized_run_id,
    )
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        context["publish_funnel_summary"],
        run_id=normalized_run_id,
    )
    publication_summary = _build_publication_summary_for_response(
        publication,
        tracker_summary=context["publish_summary"].get("project_tracker_summary") or {},
        publication_status_summary=context["publication_status_summary"],
    )
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "compact": False,
        "detail_scope": "diagnostic_summary",
        "dashboard_build_profile": context["dashboard_build_profile"],
        "scope_contract": _build_dashboard_scope_contract("diagnostic_summary"),
        "data_sources": payload.get("data_sources") or {},
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "internal_watch_count": int(counts.get("internal_watch_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "dynamic_internal_watch_count": int(counts.get("dynamic_internal_watch_count") or 0),
            "manager_stability_watch_count": int(counts.get("manager_stability_watch_count") or 0),
            "publish_post_review_watch_count": int(counts.get("publish_post_review_watch_count") or 0),
            "project_universe_count": int(counts.get("project_universe_count") or 0),
            "excluded_count": int(counts.get("excluded_count") or 0),
            "annual_return_history_fund_count": int(counts.get("annual_return_history_fund_count") or 0),
            "rank_history_fund_count": int(counts.get("rank_history_fund_count") or 0),
            "neutralized_long_term_feature_fund_count": int(counts.get("neutralized_long_term_feature_fund_count") or 0),
            "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
        },
        "tracker_summary": _reduce_tracker_summary_for_overview(context["tracker_summary"]),
        "portfolio_suggestions": _reduce_portfolio_suggestions_for_detail(context["portfolio_summary"]),
        "portfolio_display_scope": context["portfolio_display_scope"],
        "portfolio_group_brief": context["portfolio_group_brief"],
        "database_freshness": _reduce_database_freshness_for_diagnostic_detail(
            database_freshness
        ),
        "database_overview": _reduce_database_overview_for_overview(context["database_overview"]),
        "database_source_audit": context["database_source_audit"],
        "database_integrity": _reduce_database_integrity_for_overview(context["database_integrity"]),
        "nav_lag_summary": context["nav_lag_summary"],
        "risk_lag_summary": context["risk_lag_summary"],
        "risk_sync_summary": _reduce_risk_sync_summary_for_diagnostic_detail(
            context["risk_sync_summary"]
        ),
        "risk_coverage_gap_summary": context["risk_coverage_gap_summary"],
        "coverage_layers": coverage_layers,
        "coverage_summary": coverage_summary,
        "long_term_governance_summary": _reduce_long_term_governance_summary_for_detail(
            context["long_term_governance_summary"]
        ),
        "nav_risk_governance_summary": context["nav_risk_governance_summary"],
        "positive_ratio_semantics": context["positive_ratio_semantics"],
        "publish_funnel_summary": publish_funnel_summary,
        "publish_layer_semantics_summary": context["publish_layer_semantics_summary"],
        "publish_reason_summary": _reduce_publish_reason_summary_for_diagnostic_detail(
            context["publish_reason_summary"]
        ),
        "selection_exclusion_summary": _reduce_selection_exclusion_summary_for_scoped_pages(
            context["exclusion_summary"]
        ),
        "automation_layers": publication.get("automation_layers") or {},
        "publication_integrity": publication.get("publication_integrity") or {},
        "artifact_consistency": ((payload.get("internal_audit") or {}).get("artifact_consistency") or {}),
        "publication_summary": publication_summary,
        "publication_status_summary": context["publication_status_summary"],
        "report_timing_summary": _reduce_report_timing_for_overview(context["report_timing_summary"]),
        "scheduler_overview": _reduce_scheduler_overview_for_overview(scheduler_overview_api),
        "maintenance_summary": _reduce_maintenance_summary_for_overview(
            scheduler_overview_api.get("maintenance_summary") or {}
        ),
        "weekly_run": _reduce_weekly_run_for_overview(
            scheduler_overview_api.get("weekly_run") or {}
        ),
        "scheduler_path_governance": context["scheduler_path_governance"],
        "scheduler_chain_audit": _reduce_scheduler_chain_audit_for_diagnostic_detail(
            context["scheduler_chain_audit"]
        ),
        "automation_autonomy": context["automation_autonomy"],
        "forward_validation_summary": _reduce_forward_validation_summary_for_diagnostic_detail(
            (payload.get("project_publish_summary") or {}).get("forward_validation_summary") or {}
        ),
        "publish_stage_audit": _reduce_publish_stage_audit_for_task_summary(
            (context["publish_summary"].get("publish_stage_audit") or {})
        ),
        "low_vol_positive_ratio_shadow_watch": _reduce_low_vol_positive_ratio_shadow_watch_for_compact(
            context["low_vol_positive_ratio_shadow_watch"]
        ),
        "files": _build_dashboard_files_payload(
            artifacts,
            keys=(
                "project_tracker_excel",
                "filtered_excel",
                "filtered_csv",
                "dynamic_filtered_excel",
                "dynamic_filtered_csv",
                "watch_excel",
                "watch_csv",
                "dynamic_watch_excel",
                "dynamic_watch_csv",
                "excluded_excel",
                "excluded_csv",
                "report_markdown",
                "report_json",
            ),
        ),
        "artifacts": _project_artifact_values(
            artifacts,
            (
                "project_tracker_excel",
                "project_tracker_excel_meta",
                "filtered_excel",
                "filtered_csv",
                "dynamic_filtered_excel",
                "dynamic_filtered_csv",
                "dynamic_filtered_excel_meta",
                "watch_excel",
                "watch_csv",
                "watch_excel_meta",
                "dynamic_watch_excel",
                "dynamic_watch_csv",
                "dynamic_watch_excel_meta",
                "excluded_excel",
                "excluded_csv",
                "excluded_excel_meta",
                "report_markdown",
                "report_json",
                "forward_validation_excel",
                "forward_validation_json",
                "forward_validation_markdown",
            ),
        ),
    }


def build_dashboard_overview_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    scheduler_state: dict[str, Any],
    *,
    settings: Any,
    manual_operation_mode: bool,
    normalize_portfolio_groups: Any,
    build_portfolio_group_brief: Any,
    build_database_source_audit: Any,
    load_database_overview: Any,
    load_database_integrity_summary: Any,
    load_nav_lag_summary: Any,
    load_risk_lag_summary: Any,
    load_risk_sync_summary: Any,
    load_risk_coverage_gap_summary: Any,
    load_bond_universe_snapshot: Any,
    load_exclusion_reason_summary: Any,
    normalize_coverage_layers: Any,
    build_coverage_layers_from_snapshots: Any,
    build_publish_funnel_summary: Any,
    build_scheduler_overview_summary: Any,
    build_scheduler_overview_api_summary: Any,
    build_scheduler_chain_audit: Any,
    build_automation_autonomy_summary: Any,
    merge_live_long_term_priority_blocked_summary: Any | None = None,
) -> dict[str, Any]:
    context = _build_dashboard_shared_context(
        payload,
        publication,
        scheduler_state,
        settings=settings,
        manual_operation_mode=manual_operation_mode,
        normalize_portfolio_groups=normalize_portfolio_groups,
        build_portfolio_group_brief=build_portfolio_group_brief,
        build_database_source_audit=build_database_source_audit,
        load_database_overview=load_database_overview,
        load_database_integrity_summary=load_database_integrity_summary,
        load_nav_lag_summary=load_nav_lag_summary,
        load_risk_lag_summary=load_risk_lag_summary,
        load_risk_sync_summary=load_risk_sync_summary,
        load_risk_coverage_gap_summary=load_risk_coverage_gap_summary,
        load_bond_universe_snapshot=load_bond_universe_snapshot,
        load_exclusion_reason_summary=load_exclusion_reason_summary,
        normalize_coverage_layers=normalize_coverage_layers,
        build_coverage_layers_from_snapshots=build_coverage_layers_from_snapshots,
        build_publish_funnel_summary=build_publish_funnel_summary,
        build_scheduler_overview_summary=build_scheduler_overview_summary,
        build_scheduler_overview_api_summary=build_scheduler_overview_api_summary,
        build_scheduler_chain_audit=build_scheduler_chain_audit,
        build_automation_autonomy_summary=build_automation_autonomy_summary,
        merge_live_long_term_priority_blocked_summary=merge_live_long_term_priority_blocked_summary,
        detail_scope="overview",
    )
    counts = context["counts"]
    bond_universe_snapshot = context["bond_universe_snapshot"]
    long_term_snapshot = context["long_term_snapshot"]
    exclusion_summary = context["exclusion_summary"]
    database_freshness = _build_database_freshness_runtime_scope(
        {"effective_nav_snapshot": context["effective_nav_snapshot"]},
        run_id=_clean_text(payload.get("run_id")),
    )
    coverage_summary = _build_coverage_summary(
        bond_universe_snapshot=bond_universe_snapshot,
        long_term_snapshot=long_term_snapshot,
        counts=counts,
        run_id=_clean_text(payload.get("run_id")),
    )
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        context["publish_funnel_summary"],
        run_id=_clean_text(payload.get("run_id")),
    )
    exclusion_summary_reduced = {
        "available": bool(exclusion_summary.get("available")),
        "ledger_path": exclusion_summary.get("ledger_path"),
        "excluded_candidate_total": int(exclusion_summary.get("excluded_candidate_total") or 0),
        "high_score_candidate_total": int(exclusion_summary.get("high_score_candidate_total") or 0),
        "high_score_bucket_counts": exclusion_summary.get("high_score_bucket_counts") or [],
        "top_reasons": exclusion_summary.get("top_reasons") or [],
        "reason_group_preview": exclusion_summary.get("reason_group_preview") or [],
        "quality_factor_group_preview": exclusion_summary.get("quality_factor_group_preview") or [],
        "quality_factor_detail_preview": exclusion_summary.get("quality_factor_detail_preview") or [],
        "quality_overlap_combo_preview": exclusion_summary.get("quality_overlap_combo_preview") or [],
        "high_score_friction_count": int(exclusion_summary.get("high_score_friction_count") or 0),
        "high_score_friction_reason_preview": exclusion_summary.get("high_score_friction_reason_preview") or [],
        "high_score_friction_style_preview": exclusion_summary.get("high_score_friction_style_preview") or [],
        "risk_adjusted_metric_stack_review": exclusion_summary.get("risk_adjusted_metric_stack_review") or {},
        "status_split_preview": exclusion_summary.get("status_split_preview") or [],
    }
    publication_summary = _build_publication_summary_for_response(
        publication,
        tracker_summary=context["publish_summary"].get("project_tracker_summary") or {},
        publication_status_summary=context["publication_status_summary"],
    )
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "dashboard_build_profile": context["dashboard_build_profile"],
        "scope_contract": _build_dashboard_scope_contract("overview"),
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "internal_watch_count": int(counts.get("internal_watch_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "dynamic_internal_watch_count": int(counts.get("dynamic_internal_watch_count") or 0),
            "publish_post_review_watch_count": int(counts.get("publish_post_review_watch_count") or 0),
            "autostock_passed_count": int(counts.get("autostock_passed_count") or 0),
            "autostock_filtered_count": int(counts.get("autostock_filtered_count") or 0),
            "stage_b_candidate_pool_count": int(counts.get("stage_b_candidate_pool_count") or counts.get("publishable_pool_count") or 0),
            "neutralized_long_term_feature_fund_count": int(counts.get("neutralized_long_term_feature_fund_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
            "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
        },
        "tracker_summary": _reduce_tracker_summary_for_overview(context["tracker_summary"]),
        "database_overview": _reduce_database_overview_for_overview(context["database_overview"]),
        "database_integrity": _reduce_database_integrity_for_overview(context["database_integrity"]),
        "database_freshness": database_freshness,
        "coverage_summary": coverage_summary,
        "publish_funnel_summary": _reduce_publish_funnel_for_overview(publish_funnel_summary),
        "scheduler_overview": _reduce_scheduler_overview_for_overview(context["scheduler_overview_api"]),
        "maintenance_summary": _reduce_maintenance_summary_for_overview(
            context["scheduler_overview_api"].get("maintenance_summary") or {}
        ),
        "weekly_run": _reduce_weekly_run_for_overview(
            context["scheduler_overview_api"].get("weekly_run") or {}
        ),
        "automation_autonomy": context["automation_autonomy"],
        "publication_summary": publication_summary,
        "publication_status_summary": context["publication_status_summary"],
        "report_timing_summary": _reduce_report_timing_for_overview(context["report_timing_summary"]),
        "tracker_file": context["artifacts"].get("project_tracker_excel"),
        "filtered_excel": context["artifacts"].get("filtered_excel"),
        "filtered_csv": context["artifacts"].get("filtered_csv"),
        "dynamic_filtered_excel": context["artifacts"].get("dynamic_filtered_excel"),
        "dynamic_filtered_csv": context["artifacts"].get("dynamic_filtered_csv"),
        "watch_excel": context["artifacts"].get("watch_excel"),
        "watch_csv": context["artifacts"].get("watch_csv"),
        "dynamic_watch_excel": context["artifacts"].get("dynamic_watch_excel"),
        "dynamic_watch_csv": context["artifacts"].get("dynamic_watch_csv"),
        "excluded_excel": context["artifacts"].get("excluded_excel"),
        "excluded_csv": context["artifacts"].get("excluded_csv"),
        "report_markdown": context["artifacts"].get("report_markdown"),
        "report_json": context["artifacts"].get("report_json"),
        "files": {
            "recommendations_page": "/recommendations",
            "tracker_file": context["artifacts"].get("project_tracker_excel"),
            "filtered_excel": context["artifacts"].get("filtered_excel"),
            "filtered_csv": context["artifacts"].get("filtered_csv"),
            "dynamic_filtered_excel": context["artifacts"].get("dynamic_filtered_excel"),
            "dynamic_filtered_csv": context["artifacts"].get("dynamic_filtered_csv"),
            "watch_excel": context["artifacts"].get("watch_excel"),
            "watch_csv": context["artifacts"].get("watch_csv"),
            "dynamic_watch_excel": context["artifacts"].get("dynamic_watch_excel"),
            "dynamic_watch_csv": context["artifacts"].get("dynamic_watch_csv"),
            "excluded_excel": context["artifacts"].get("excluded_excel"),
            "excluded_csv": context["artifacts"].get("excluded_csv"),
            "report_markdown": context["artifacts"].get("report_markdown"),
            "report_json": context["artifacts"].get("report_json"),
        },
        "artifacts": {
            "project_tracker_excel": context["artifacts"].get("project_tracker_excel"),
            "project_tracker_excel_meta": context["artifacts"].get("project_tracker_excel_meta"),
            "filtered_excel": context["artifacts"].get("filtered_excel"),
            "filtered_csv": context["artifacts"].get("filtered_csv"),
            "dynamic_filtered_excel": context["artifacts"].get("dynamic_filtered_excel"),
            "dynamic_filtered_csv": context["artifacts"].get("dynamic_filtered_csv"),
            "dynamic_filtered_excel_meta": context["artifacts"].get("dynamic_filtered_excel_meta"),
            "watch_excel": context["artifacts"].get("watch_excel"),
            "watch_csv": context["artifacts"].get("watch_csv"),
            "watch_excel_meta": context["artifacts"].get("watch_excel_meta"),
            "dynamic_watch_excel": context["artifacts"].get("dynamic_watch_excel"),
            "dynamic_watch_csv": context["artifacts"].get("dynamic_watch_csv"),
            "dynamic_watch_excel_meta": context["artifacts"].get("dynamic_watch_excel_meta"),
            "excluded_excel": context["artifacts"].get("excluded_excel"),
            "excluded_csv": context["artifacts"].get("excluded_csv"),
            "excluded_excel_meta": context["artifacts"].get("excluded_excel_meta"),
            "report_markdown": context["artifacts"].get("report_markdown"),
            "report_json": context["artifacts"].get("report_json"),
            "threshold_compare_excel": context["artifacts"].get("threshold_compare_excel"),
            "forward_validation_excel": context["artifacts"].get("forward_validation_excel"),
            "forward_validation_json": context["artifacts"].get("forward_validation_json"),
            "forward_validation_markdown": context["artifacts"].get("forward_validation_markdown"),
            "stage_b_reason_excel": context["artifacts"].get("stage_b_reason_excel"),
        },
    }


def build_latest_dashboard_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    scheduler_state: dict[str, Any],
    *,
    settings: Any,
    manual_operation_mode: bool,
    normalize_portfolio_groups: Any,
    build_portfolio_group_brief: Any,
    build_database_source_audit: Any,
    load_database_overview: Any,
    load_database_integrity_summary: Any,
    load_nav_lag_summary: Any,
    load_risk_lag_summary: Any,
    load_risk_sync_summary: Any,
    load_risk_coverage_gap_summary: Any,
    load_bond_universe_snapshot: Any,
    load_exclusion_reason_summary: Any,
    normalize_coverage_layers: Any,
    build_coverage_layers_from_snapshots: Any,
    build_publish_funnel_summary: Any,
    build_scheduler_overview_summary: Any,
    build_scheduler_overview_api_summary: Any,
    build_scheduler_chain_audit: Any,
    build_automation_autonomy_summary: Any,
    merge_live_long_term_priority_blocked_summary: Any | None = None,
    read_excel_cached: Any,
    read_tracker_workbook_sheet: Any,
    compact: bool = False,
    detail_scope: str = "diagnostic_summary",
) -> dict[str, Any]:
    if compact:
        return _build_latest_dashboard_compact_response(
            payload,
            publication,
            scheduler_state,
            settings=settings,
            manual_operation_mode=manual_operation_mode,
            normalize_portfolio_groups=normalize_portfolio_groups,
            build_portfolio_group_brief=build_portfolio_group_brief,
            build_database_source_audit=build_database_source_audit,
            load_database_overview=load_database_overview,
            load_database_integrity_summary=load_database_integrity_summary,
            load_bond_universe_snapshot=load_bond_universe_snapshot,
            load_exclusion_reason_summary=load_exclusion_reason_summary,
            load_risk_lag_summary=load_risk_lag_summary,
            load_risk_sync_summary=load_risk_sync_summary,
            load_risk_coverage_gap_summary=load_risk_coverage_gap_summary,
            build_publish_funnel_summary=build_publish_funnel_summary,
            normalize_coverage_layers=normalize_coverage_layers,
            build_coverage_layers_from_snapshots=build_coverage_layers_from_snapshots,
            build_scheduler_overview_summary=build_scheduler_overview_summary,
            build_scheduler_overview_api_summary=build_scheduler_overview_api_summary,
            build_scheduler_chain_audit=build_scheduler_chain_audit,
            build_automation_autonomy_summary=build_automation_autonomy_summary,
            merge_live_long_term_priority_blocked_summary=merge_live_long_term_priority_blocked_summary,
        )

    context = _build_dashboard_shared_context(
        payload,
        publication,
        scheduler_state,
        settings=settings,
        manual_operation_mode=manual_operation_mode,
        normalize_portfolio_groups=normalize_portfolio_groups,
        build_portfolio_group_brief=build_portfolio_group_brief,
        build_database_source_audit=build_database_source_audit,
        load_database_overview=load_database_overview,
        load_database_integrity_summary=load_database_integrity_summary,
        load_nav_lag_summary=load_nav_lag_summary,
        load_risk_lag_summary=load_risk_lag_summary,
        load_risk_sync_summary=load_risk_sync_summary,
        load_risk_coverage_gap_summary=load_risk_coverage_gap_summary,
        load_bond_universe_snapshot=load_bond_universe_snapshot,
        load_exclusion_reason_summary=load_exclusion_reason_summary,
        normalize_coverage_layers=normalize_coverage_layers,
        build_coverage_layers_from_snapshots=build_coverage_layers_from_snapshots,
        build_publish_funnel_summary=build_publish_funnel_summary,
        build_scheduler_overview_summary=build_scheduler_overview_summary,
        build_scheduler_overview_api_summary=build_scheduler_overview_api_summary,
        build_scheduler_chain_audit=build_scheduler_chain_audit,
        build_automation_autonomy_summary=build_automation_autonomy_summary,
        merge_live_long_term_priority_blocked_summary=merge_live_long_term_priority_blocked_summary,
        detail_scope=detail_scope,
    )
    if detail_scope == "reports":
        return _build_latest_dashboard_reports_response(
            payload,
            publication,
            context=context,
        )
    if detail_scope == "task_summary":
        return _build_latest_dashboard_task_summary_response(
            payload,
            publication,
            context=context,
        )
    if detail_scope in {"full", "audit_full"}:
        return _build_latest_dashboard_full_audit_response(
            payload,
            publication,
            context=context,
        )

    return _build_latest_dashboard_diagnostic_detail_response(
        payload,
        publication,
        context=context,
    )


def _build_latest_dashboard_reports_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    counts = context["counts"]
    artifacts = context["artifacts"]
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        context["publish_funnel_summary"],
        run_id=_clean_text(payload.get("run_id")),
    )
    publication_summary = _build_publication_summary_for_response(
        publication,
        tracker_summary=context["publish_summary"].get("project_tracker_summary") or {},
        publication_status_summary=context["publication_status_summary"],
    )
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "compact": False,
        "detail_scope": "reports",
        "dashboard_build_profile": context["dashboard_build_profile"],
        "scope_contract": _build_dashboard_scope_contract("reports"),
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
        },
        "tracker_summary": _reduce_tracker_summary_for_scoped_pages(context["tracker_summary"]),
        "portfolio_suggestions": _reduce_portfolio_suggestions_for_detail(context["portfolio_summary"]),
        "publish_funnel_summary": publish_funnel_summary,
        "publish_reason_summary": _reduce_publish_reason_summary_for_scoped_pages(
            context["publish_reason_summary"]
        ),
        "selection_exclusion_summary": _reduce_selection_exclusion_summary_for_scoped_pages(
            context["exclusion_summary"]
        ),
        "long_term_governance_summary": _reduce_long_term_governance_summary_for_compact(
            context["long_term_governance_summary"]
        ),
        "positive_ratio_semantics": context["positive_ratio_semantics"],
        "low_vol_positive_ratio_shadow_watch": _reduce_low_vol_positive_ratio_shadow_watch_for_reports_scope(
            context["low_vol_positive_ratio_shadow_watch"]
        ),
        "report_timing_summary": context["report_timing_summary"],
        "publication_summary": publication_summary,
        "publication_status_summary": context["publication_status_summary"],
        "files": _build_dashboard_files_payload(
            artifacts,
            keys=(
                "project_tracker_excel",
                "filtered_excel",
                "filtered_csv",
                "dynamic_filtered_excel",
                "dynamic_filtered_csv",
                "report_markdown",
                "report_json",
            ),
        ),
        "artifacts": _project_artifact_values(
            artifacts,
            (
                "project_tracker_excel",
                "project_tracker_excel_meta",
                "filtered_excel",
                "filtered_csv",
                "dynamic_filtered_excel",
                "dynamic_filtered_csv",
                "dynamic_filtered_excel_meta",
                "report_markdown",
                "report_json",
            ),
        ),
    }


def _build_latest_dashboard_task_summary_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    counts = context["counts"]
    scheduler_overview_api = context["scheduler_overview_api"]
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        context["publish_funnel_summary"],
        run_id=_clean_text(payload.get("run_id")),
    )
    publication_summary = _build_publication_summary_for_response(
        publication,
        tracker_summary=context["publish_summary"].get("project_tracker_summary") or {},
        publication_status_summary=context["publication_status_summary"],
    )
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "compact": False,
        "detail_scope": "task_summary",
        "dashboard_build_profile": context["dashboard_build_profile"],
        "scope_contract": _build_dashboard_scope_contract("task_summary"),
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
        },
        "tracker_summary": _reduce_tracker_summary_for_scoped_pages(context["tracker_summary"]),
        "scheduler_overview": {
            "latest_screened_snapshot": scheduler_overview_api.get("latest_screened_snapshot") or {},
        },
        "portfolio_suggestions": _reduce_portfolio_suggestions_for_detail(context["portfolio_summary"]),
        "publish_funnel_summary": publish_funnel_summary,
        "publish_stage_audit": _reduce_publish_stage_audit_for_task_summary(
            context["publish_summary"].get("publish_stage_audit") or {}
        ),
        "publish_reason_summary": _reduce_publish_reason_summary_for_scoped_pages(
            context["publish_reason_summary"]
        ),
        "selection_exclusion_summary": _reduce_selection_exclusion_summary_for_scoped_pages(
            context["exclusion_summary"]
        ),
        "long_term_governance_summary": _reduce_long_term_governance_summary_for_compact(
            context["long_term_governance_summary"]
        ),
        "publication_summary": publication_summary,
        "publication_status_summary": context["publication_status_summary"],
    }


def _build_latest_dashboard_compact_response(
    payload: dict[str, Any],
    publication: dict[str, Any],
    scheduler_state: dict[str, Any],
    *,
    settings: Any,
    manual_operation_mode: bool,
    normalize_portfolio_groups: Any,
    build_portfolio_group_brief: Any,
    build_database_source_audit: Any,
    load_database_overview: Any,
    load_database_integrity_summary: Any,
    load_bond_universe_snapshot: Any,
    load_exclusion_reason_summary: Any,
    load_risk_lag_summary: Any,
    load_risk_sync_summary: Any,
    load_risk_coverage_gap_summary: Any,
    build_publish_funnel_summary: Any,
    normalize_coverage_layers: Any,
    build_coverage_layers_from_snapshots: Any,
    build_scheduler_overview_summary: Any,
    build_scheduler_overview_api_summary: Any,
    build_scheduler_chain_audit: Any,
    build_automation_autonomy_summary: Any,
    merge_live_long_term_priority_blocked_summary: Any | None = None,
) -> dict[str, Any]:
    profile_started_at, build_profile = _start_dashboard_build_profile("compact", compact=True)
    step_started_at = time.perf_counter()
    publish_summary = payload.get("project_publish_summary") or {}
    artifacts = payload.get("artifacts") or {}
    normalized_run_id = _clean_text(payload.get("run_id"))
    counts = dict(payload.get("counts") or {})
    _record_dashboard_build_step(build_profile, "compact_report_core_fields", step_started_at)

    step_started_at = time.perf_counter()
    low_vol_positive_ratio_shadow_watch = normalize_low_vol_positive_ratio_shadow_watch(
        publish_summary, report_identity=payload, settings=settings,
    )
    low_vol_positive_ratio_shadow_watch = _build_low_vol_positive_ratio_shadow_watch_runtime_scope(
        low_vol_positive_ratio_shadow_watch,
        run_id=normalized_run_id,
    )
    _record_dashboard_build_step(build_profile, "compact_low_vol_shadow_watch", step_started_at)

    step_started_at = time.perf_counter()
    exclusion_summary = load_exclusion_reason_summary(payload)
    _record_dashboard_build_step(build_profile, "compact_exclusion_summary", step_started_at)

    step_started_at = time.perf_counter()
    scheduler_overview = build_scheduler_overview_summary(scheduler_state)
    _record_dashboard_build_step(build_profile, "compact_scheduler_overview", step_started_at)

    step_started_at = time.perf_counter()
    long_term_snapshot = (
        (payload.get("database_freshness") or {}).get("long_term_history_snapshot") or {}
    )
    long_term_maintenance_detail = build_long_term_maintenance_detail(scheduler_overview)
    if merge_live_long_term_priority_blocked_summary is not None:
        long_term_maintenance_detail = merge_live_long_term_priority_blocked_summary(
            long_term_maintenance_detail
        )
    long_term_governance_summary = build_long_term_governance_summary(
        long_term_snapshot,
        long_term_maintenance_detail,
        include_semantic_null_snapshot_audit=False,
    )
    long_term_governance_summary = _build_long_term_governance_runtime_scope(
        long_term_governance_summary,
        run_id=normalized_run_id,
    )
    _record_dashboard_build_step(build_profile, "compact_long_term_governance", step_started_at)

    step_started_at = time.perf_counter()
    publication_status_summary = build_publication_status_summary(
        publication,
        tracker_summary=publish_summary.get("project_tracker_summary") or {},
    )
    publication_status_summary = _build_publication_status_runtime_scope(
        publication_status_summary,
        run_id=normalized_run_id,
    )
    publication_summary = build_publication_summary(
        publication,
        tracker_summary=publish_summary.get("project_tracker_summary") or {},
    )
    publication_summary["status_summary"] = publication_status_summary
    publication_summary["latest_canonical_run_id"] = (
        publication_summary.get("latest_canonical_run_id") or publication.get("latest_canonical_run_id")
    )
    publication_summary["latest_canonical_report_path"] = publication.get("latest_canonical_report_path")
    _record_dashboard_build_step(build_profile, "compact_publication_summary", step_started_at)

    step_started_at = time.perf_counter()
    compact_publish_funnel_summary = _reduce_publish_funnel_for_overview(
        _build_publish_funnel_runtime_scope(
            build_publish_funnel_summary(
                payload,
                normalize_coverage_layers(
                    payload.get("coverage_layers")
                    or build_coverage_layers_from_snapshots(
                        load_bond_universe_snapshot(payload),
                        (payload.get("database_freshness") or {}).get("long_term_history_snapshot") or {},
                        counts,
                    ),
                    counts=counts,
                ),
            ),
            run_id=normalized_run_id,
        )
    )
    _record_dashboard_build_step(build_profile, "compact_publish_funnel", step_started_at)

    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "compact": True,
        "detail_scope": "compact",
        "dashboard_build_profile": _finish_dashboard_build_profile(
            build_profile,
            profile_started_at,
        ),
        "scope_contract": _build_dashboard_scope_contract(
            "compact",
            compact=True,
            live_diagnostics_merged=merge_live_long_term_priority_blocked_summary is not None,
        ),
        "counts": _build_compact_dashboard_counts(counts),
        "automation_layers": _reduce_automation_layers_for_compact(
            publication.get("automation_layers") or {}
        ),
        "publication_status_summary": publication_status_summary,
        "publication_summary": publication_summary,
        "publication_integrity": _reduce_integrity_summary_for_compact(
            publication.get("publication_integrity") or {}
        ),
        "artifact_consistency": _reduce_integrity_summary_for_compact(
            ((payload.get("internal_audit") or {}).get("artifact_consistency") or {})
        ),
        "long_term_governance_summary": _reduce_long_term_governance_summary_for_compact(
            long_term_governance_summary
        ),
        "selection_exclusion_summary": _reduce_selection_exclusion_summary_for_compact(
            exclusion_summary
        ),
        "low_vol_positive_ratio_shadow_watch": _reduce_low_vol_positive_ratio_shadow_watch_for_compact(
            low_vol_positive_ratio_shadow_watch
        ),
        "tracker_file": artifacts.get("project_tracker_excel"),
        "publish_funnel_summary": compact_publish_funnel_summary,
        **_project_artifact_values(
            artifacts,
            (
                "filtered_excel",
                "filtered_csv",
                "dynamic_filtered_excel",
                "dynamic_filtered_csv",
                "watch_excel",
                "watch_csv",
                "dynamic_watch_excel",
                "dynamic_watch_csv",
                "excluded_excel",
                "excluded_csv",
                "threshold_compare_excel",
                "screening_audit_excel",
                "publish_chain_snapshot_json",
                "forward_validation_excel",
                "forward_validation_json",
                "forward_validation_markdown",
                "stage_b_reason_excel",
                "report_markdown",
                "report_json",
            ),
        ),
        "files": _build_dashboard_files_payload(
            artifacts,
            keys=_DASHBOARD_FILE_KEYS,
            include_recommendations_page=True,
        ),
        "artifacts": _project_artifact_values(artifacts, _DASHBOARD_ARTIFACT_KEYS),
    }
